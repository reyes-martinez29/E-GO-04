// Producer publica eventos_hotsale.jsonl al stream HOTSALE de JetStream, en el
// orden del archivo.
//
// Modelo (de E-Go03): un solo lector secuencial + PublishMsgAsync con un
// límite de mensajes en vuelo. No hay worker pool ni json.Unmarshal: el
// productor publica los bytes crudos de cada línea, el orden del archivo se
// preserva y la RAM queda acotada a maxPending mensajes sin importar el tamaño
// del archivo. Un goroutine aparte confirma los PubAck en orden y reintenta los
// fallidos con el mismo Nats-Msg-Id, para que el broker descarte el reenvío si
// el original sí había llegado.
//
// E-Go04 agrega pacing por fases (ver pacing.go): ritmo normal sostenido, una
// ventana de spike a -spike-mult veces ese ritmo y vuelta al ritmo normal. El
// productor escribe una línea de tiempo por segundo (CSV) con la tasa real
// publicada, y al final un resumen por fase.
package main

import (
	"bufio"
	"bytes"
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime/pprof"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"hotsale/internal/broker"
)

type pending struct {
	paf  jetstream.PubAckFuture
	msg  *nats.Msg
	line uint64
}

type ackStats struct {
	acked, brokerDup, retried uint64
}

func main() {
	file := flag.String("file", "eventos_hotsale.jsonl", "archivo JSONL de eventos")
	url := flag.String("url", nats.DefaultURL, "URL de NATS")
	rate := flag.Int("rate", 2000, "ritmo normal en eventos/s (0 = sin límite, como E-Go03; ver decisions.md §4)")
	spikeMult := flag.Float64("spike-mult", 10, "multiplicador del ritmo durante el spike")
	spikeAfter := flag.Duration("spike-after", time.Minute, "cuándo empieza el spike, desde la primera publicación")
	spikeFor := flag.Duration("spike-for", 2*time.Minute, "duración del spike (0 = sin spike)")
	outDir := flag.String("out", "results", "carpeta para la línea de tiempo CSV")
	limit := flag.Uint64("limit", 0, "publicar solo las primeras N líneas (0 = todas)")
	// El Nats-Msg-Id debe ser único POR CORRIDA: con solo el número de línea,
	// una segunda corrida dentro de la ventana Duplicates del broker se
	// descartaba entera (ver decisions.md, hallazgo 5).
	runID := flag.String("run-id", strconv.FormatInt(time.Now().UnixNano(), 36), "prefijo del Nats-Msg-Id de esta corrida")
	// 1024: mismo throughput que 4096 con un tercio de la latencia; 16384 provoca
	// 429 del broker (ver decisions.md, barrido de max-pending).
	maxPending := flag.Int("max-pending", 1024, "máximo de publicaciones async sin PubAck (contrapresión)")
	cpuProfile := flag.String("cpuprofile", "", "escribir un CPU profile en este archivo")
	flag.Parse()

	if *cpuProfile != "" {
		pf, err := os.Create(*cpuProfile)
		if err != nil {
			log.Fatal(err)
		}
		if err := pprof.StartCPUProfile(pf); err != nil {
			log.Fatal(err)
		}
		defer pprof.StopCPUProfile()
	}

	nc, err := nats.Connect(*url, nats.Name("hotsale-producer"))
	if err != nil {
		log.Fatalf("conectar a NATS: %v", err)
	}
	defer nc.Close()
	js, err := jetstream.New(nc, jetstream.WithPublishAsyncMaxPending(*maxPending))
	if err != nil {
		log.Fatal(err)
	}
	ctx := context.Background()
	if _, err := broker.EnsureStream(ctx, js); err != nil {
		log.Fatalf("crear stream: %v", err)
	}

	f, err := os.Open(*file)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	sched := newSchedule(*rate, *spikeMult, *spikeAfter, *spikeFor)
	if *rate > 0 && *spikeFor > 0 && *spikeFor < 2*time.Minute {
		log.Printf("ATENCIÓN: spike de %v; la guía pide al menos 2 minutos continuos", *spikeFor)
	}
	log.Printf("plan de publicación: %s", sched)
	_ = os.MkdirAll(*outDir, 0o755)
	tlPath := filepath.Join(*outDir, "producer-"+time.Now().Format("20060102-150405")+".csv")
	var published atomic.Uint64

	futures := make(chan pending, *maxPending)
	var st ackStats
	var wg sync.WaitGroup
	wg.Go(func() { confirmAcks(ctx, js, futures, &st) })

	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64<<10), 1<<20)
	var line uint64
	start := time.Now()
	sched.start(start)
	stopTL := timeline(tlPath, start, sched, &published)
	lastLog := start
	for sc.Scan() {
		if *limit > 0 && line >= *limit {
			break
		}
		line++
		if ph, changed := sched.pace(line); changed {
			log.Printf("fase %s: %s", ph.name, ph.describe())
		}
		msg := nats.NewMsg(broker.EventsSubject)
		msg.Data = bytes.Clone(sc.Bytes()) // Scanner reutiliza su buffer; el future retiene el *Msg
		msg.Header.Set(jetstream.MsgIDHeader, *runID+"-"+strconv.FormatUint(line, 10))
		msg.Header.Set(broker.HeaderPubTs, strconv.FormatInt(time.Now().UnixNano(), 10))
		futures <- pending{paf: publishAsync(js, msg), msg: msg, line: line}
		published.Store(line)
		sched.count(time.Now())

		if now := time.Now(); now.Sub(lastLog) >= 5*time.Second {
			lastLog = now
			log.Printf("publicadas %d líneas, fase %s (%.0f ev/s promedio)", line, sched.current().name, float64(line)/now.Sub(start).Seconds())
		}
	}
	stopTL()
	if err := sc.Err(); err != nil {
		log.Fatalf("leer %s: %v", *file, err)
	}

	// Marcador de fin: el consumidor sabe cuántas líneas esperar y cuándo reportar.
	eof := nats.NewMsg(broker.EOFSubject)
	eof.Header.Set(broker.HeaderTotalLines, strconv.FormatUint(line, 10))
	eof.Header.Set(broker.HeaderPubTs, strconv.FormatInt(time.Now().UnixNano(), 10))
	futures <- pending{paf: publishAsync(js, eof), msg: eof}
	close(futures)
	wg.Wait()
	elapsed := time.Since(start)

	fmt.Printf("\n=== PRODUCTOR ===\n")
	fmt.Printf("líneas publicadas:      %d\n", line)
	fmt.Printf("PubAck confirmados:     %d (incluye EOF)\n", st.acked)
	fmt.Printf("reintentos del prod.:   %d\n", st.retried)
	fmt.Printf("descartados por broker: %d (Nats-Msg-Id repetido = reintento del productor)\n", st.brokerDup)
	fmt.Printf("tiempo:                 %v\n", elapsed.Round(time.Millisecond))
	fmt.Printf("throughput publicación: %.0f ev/s\n", float64(line)/elapsed.Seconds())
	sched.printSummary()
	fmt.Printf("línea de tiempo:        %s\n", tlPath)
	if st.brokerDup > 0 {
		log.Printf("ATENCIÓN: el broker descartó %d mensajes por Nats-Msg-Id repetido; si no hubo reintentos del productor, ¿se reutilizó -run-id?", st.brokerDup)
	}
}

// publishAsync bloquea (con contrapresión) mientras haya maxPending mensajes
// sin confirmar, en lugar de fallar con ErrTooManyStalledMsgs.
func publishAsync(js jetstream.JetStream, msg *nats.Msg) jetstream.PubAckFuture {
	for {
		paf, err := js.PublishMsgAsync(msg, jetstream.WithStallWait(time.Second))
		if err == nil {
			return paf
		}
		if err == jetstream.ErrTooManyStalledMsgs {
			continue
		}
		log.Fatalf("publicar: %v", err)
	}
}

// confirmAcks espera los PubAck en orden de publicación. Un fallo se
// reintenta de forma síncrona con el mismo Nats-Msg-Id, sin límite de
// intentos y con espera creciente (tope 5 s): con el stream lleno
// (DiscardNew) el broker rechaza y el productor espera en vez de perder el
// evento. Mientras reintenta no confirma los siguientes, así que
// publishAsync se bloquea al llegar a maxPending: el productor se frena.
func confirmAcks(ctx context.Context, js jetstream.JetStream, futures <-chan pending, st *ackStats) {
	for p := range futures {
		select {
		case ack := <-p.paf.Ok():
			st.acked++
			if ack.Duplicate {
				st.brokerDup++
			}
		case err := <-p.paf.Err():
			if st.retried < 5 {
				log.Printf("línea %d: PubAck falló (%v); reintentando (se omiten los siguientes avisos)", p.line, err)
			}
			// Mensaje nuevo: el async dejó su propio inbox en Reply.
			retry := &nats.Msg{Subject: p.msg.Subject, Data: p.msg.Data, Header: p.msg.Header}
			for attempt := 1; ; attempt++ {
				st.retried++
				rctx, cancel := context.WithTimeout(ctx, 5*time.Second)
				ack, err := js.PublishMsg(rctx, retry)
				cancel()
				if err == nil {
					st.acked++
					if ack.Duplicate {
						st.brokerDup++
					}
					break
				}
				if attempt%10 == 0 {
					log.Printf("línea %d: %d reintentos sin PubAck (%v); se sigue intentando", p.line, attempt, err)
				}
				time.Sleep(min(time.Duration(attempt)*200*time.Millisecond, 5*time.Second))
			}
		}
	}
}
