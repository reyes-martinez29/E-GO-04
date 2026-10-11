// Consumer consume el stream HOTSALE, deduplica por event_id y mantiene los
// agregados por país y tipo de evento.
//
// Pipeline (sin locks sobre el estado de negocio):
//
//	fetcher (1) ──▶ shards (M: json decode + dedup + agregado) ──▶ merge final
//	      shard = hash(event_id) % M, con event_id extraído de los bytes crudos
//
// Cada shard es el único dueño de su ventana de dedup y de su Agg, y un mismo
// event_id siempre cae en el mismo shard: el "¿ya lo vi? → márcalo → súmalo"
// es atómico por construcción, sin sync.Mutex ni RWMutex. El fetcher es
// secuencial y cada shard es FIFO, así que el orden por event_id se preserva
// (un pool de decoders intermedio lo rompía; E-GO03, hallazgo H2). El json decode
// sigue siendo paralelo: ocurre dentro de los M shards. El ack se envía solo
// después de aplicar el evento, por lotes y en orden (ver commit.go).
//
// E-Go04: cada inventory_reserved único se confirma contra el servicio de
// inventario (inventory.go). El shard lo deja en el buffer de backpressure y
// sigue; la confirmación ocurre en workers aparte, detrás de un limitador
// adaptativo y un circuit breaker. Al recibir el EOF el consumidor no termina:
// drena el buffer hasta confirmar cada evento, y solo entonces reporta.
//
// El agregado y el buffer viven en memoria/disco local (decisión explícita, ver
// decisions.md): si el proceso se cae, al reiniciar se re-consume el stream
// desde el inicio y ventana, agregado y verificaciones pendientes se
// reconstruyen juntos (verificar inventario es idempotente).
package main

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"hotsale/internal/agg"
	"hotsale/internal/broker"
	"hotsale/internal/dedup"
	"hotsale/internal/event"
	"hotsale/internal/metrics"
)

type config struct {
	URL          string        `json:"-"`
	Shards       int           `json:"shards"`
	Window       time.Duration `json:"dedup_window"`
	Generations  int           `json:"dedup_generations"`
	MinRetention time.Duration `json:"dedup_min_retention"`
	MaxPerGen    int           `json:"dedup_max_per_generation"`
	PullBatch    int           `json:"pull_max_messages"`
	MaxAckPend   int           `json:"max_ack_pending"`
	Purge        bool          `json:"purge_stream_on_start"`
	AckBatch     int           `json:"ack_batch"`
	AckEvery     time.Duration `json:"ack_max_interval"`
	AckWait      time.Duration `json:"ack_wait"`
	Label        string        `json:"label"`
	Inventory    invConfig     `json:"inventory"`
}

type shard struct {
	in      chan work
	win     *dedup.Window
	agg     agg.Agg
	invalid *atomic.Int64
	inv     *inventory

	applied        int64 // eventos únicos aplicados al agregado
	dups           int64 // duplicados del origen (reintentos del gateway) descartados
	redelivered    int64 // re-entregas del broker (NumDelivered > 1)
	redeliveredDup int64 // re-entregas que la ventana reconoció y descartó
	delays         metrics.RetryDelays
	lat            []int64
	minPubTs       int64
	lastApply      int64

	processed atomic.Int64 // para el log de progreso
	winLen    atomic.Int64
	latSum    atomic.Int64 // latencia e2e acumulada (ns), para la línea de tiempo
	latN      atomic.Int64
	latMax    atomic.Int64 // máximo desde la última muestra (el monitor lo reinicia)
}

func main() {
	var cfg config
	flag.StringVar(&cfg.URL, "url", nats.DefaultURL, "URL de NATS")
	flag.IntVar(&cfg.Shards, "shards", 8, "shards de agregación (cada uno con su ventana de dedup)")
	flag.DurationVar(&cfg.Window, "dedup-window", 5*time.Minute, "retención nominal de la ventana de dedup")
	flag.IntVar(&cfg.Generations, "dedup-gens", 10, "generaciones del anillo de dedup")
	flag.DurationVar(&cfg.MinRetention, "dedup-min", 3*time.Minute, "retención mínima garantizada (retraso máximo de reintento)")
	flag.IntVar(&cfg.MaxPerGen, "dedup-max-per-gen", 0, "tope de llaves por generación y shard (0 = sin tope)")
	flag.IntVar(&cfg.PullBatch, "pull", 4096, "mensajes a pedir por pull al broker")
	flag.IntVar(&cfg.MaxAckPend, "max-ack-pending", 65536, "mensajes entregados sin ack permitidos por el broker")
	flag.BoolVar(&cfg.Purge, "purge", true, "vaciar el stream al arrancar (corrida nueva); false = re-consumir lo que haya (reinicio)")
	flag.IntVar(&cfg.AckBatch, "ack-batch", 1000, "mensajes por lote de ack (AckAll)")
	flag.DurationVar(&cfg.AckEvery, "ack-every", 10*time.Millisecond, "intervalo máximo para cerrar un lote de ack incompleto")
	// AckWait cubre el peor caso del nivel BLOCKED: los mensajes entregados
	// esperan a que el buffer libere espacio (decisions.md §1).
	flag.DurationVar(&cfg.AckWait, "ack-wait", 5*time.Minute, "tiempo que el broker espera un ack antes de re-entregar")
	flag.StringVar(&cfg.Label, "label", "", "etiqueta de la corrida para el reporte")

	ic := &cfg.Inventory
	flag.StringVar(&ic.URL, "inventory-url", "http://127.0.0.1:8090", "URL base del servicio de inventario (127.0.0.1, no localhost: ver decisions.md H1)")
	flag.IntVar(&ic.Workers, "inv-workers", 32, "llamadas en vuelo como máximo (bulkhead)")
	flag.DurationVar(&ic.Timeout, "inv-timeout", time.Second, "timeout por llamada a /check-inventory")
	flag.IntVar(&ic.BreakerFailures, "breaker-failures", 5, "fallos consecutivos que abren el breaker")
	flag.DurationVar(&ic.BreakerCooldown, "breaker-cooldown", 2*time.Second, "tiempo en OPEN antes de pasar a HALF-OPEN")
	flag.IntVar(&ic.BreakerProbes, "breaker-probes", 3, "llamadas de prueba en HALF-OPEN (todas OK para cerrar)")
	flag.BoolVar(&ic.NoLimiter, "no-limiter", false, "ABLACIÓN: sin limitador adaptativo (solo breaker + workers)")
	flag.Float64Var(&ic.LimInitial, "lim-initial", 10, "tasa inicial de llamadas/s del limitador")
	flag.Float64Var(&ic.LimMin, "lim-min", 2, "tasa mínima de llamadas/s")
	flag.Float64Var(&ic.LimMax, "lim-max", 1000, "tasa máxima de llamadas/s")
	flag.Float64Var(&ic.LimIncrease, "lim-increase", 2, "+llamadas/s por ventana sana")
	flag.Float64Var(&ic.LimDecrease, "lim-decrease", 0.75, "factor de reducción por ventana con fallos o lenta")
	flag.DurationVar(&ic.LimWindow, "lim-window", time.Second, "ventana de evaluación del limitador")
	flag.Float64Var(&ic.LimLatFactor, "lim-latency-factor", 4, "ventana lenta si latencia media > factor × base")
	flag.IntVar(&ic.MemCap, "buffer-mem", 50_000, "verificaciones pendientes en memoria")
	flag.IntVar(&ic.SpillCap, "buffer-disk", 4_000_000, "verificaciones pendientes en disco (al llenarse, se bloquea la ingesta)")
	flag.StringVar(&ic.SpillPath, "buffer-file", filepath.Join(os.TempDir(), "hotsale-inventory-spill.bin"), "archivo de desborde (fuera de OneDrive)")

	pprofAddr := flag.String("pprof", "localhost:6060", "dirección de net/http/pprof (vacío = desactivado)")
	outDir := flag.String("out", "results", "carpeta para el reporte JSON y la línea de tiempo CSV")
	flag.Parse()

	if *pprofAddr != "" {
		go func() { log.Println(http.ListenAndServe(*pprofAddr, nil)) }()
	}

	nc, err := nats.Connect(cfg.URL, nats.Name("hotsale-consumer"))
	if err != nil {
		log.Fatalf("conectar a NATS: %v", err)
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		log.Fatal(err)
	}
	ctx := context.Background()
	stream, err := broker.EnsureStream(ctx, js)
	if err != nil {
		log.Fatalf("crear stream: %v", err)
	}
	if cfg.Purge {
		if err := stream.Purge(ctx); err != nil {
			log.Fatalf("purgar stream: %v", err)
		}
	}
	// Consumer nuevo en cada arranque con DeliverAll: sin agregado persistido,
	// reiniciar significa reconstruir desde el primer mensaje retenido.
	if err := js.DeleteConsumer(ctx, broker.StreamName, broker.ConsumerName); err != nil && !errors.Is(err, jetstream.ErrConsumerNotFound) {
		log.Fatalf("borrar consumer: %v", err)
	}
	cons, err := js.CreateOrUpdateConsumer(ctx, broker.StreamName, jetstream.ConsumerConfig{
		Durable:       broker.ConsumerName,
		AckPolicy:     jetstream.AckAllPolicy, // ack de S confirma todo <= S; ver commit.go
		DeliverPolicy: jetstream.DeliverAllPolicy,
		AckWait:       cfg.AckWait,
		MaxAckPending: cfg.MaxAckPend,
	})
	if err != nil {
		log.Fatalf("crear consumer: %v", err)
	}
	it, err := cons.Messages(jetstream.PullMaxMessages(cfg.PullBatch))
	if err != nil {
		log.Fatal(err)
	}

	inv, err := newInventory(cfg.Inventory)
	if err != nil {
		log.Fatal(err)
	}
	inv.start()

	var invalid atomic.Int64
	shards := make([]*shard, cfg.Shards)
	for i := range shards {
		w, err := dedup.New(dedup.Config{Window: cfg.Window, Generations: cfg.Generations, MinRetention: cfg.MinRetention, MaxPerGen: cfg.MaxPerGen})
		if err != nil {
			log.Fatal(err)
		}
		shards[i] = &shard{in: make(chan work, 1024), win: w, invalid: &invalid, inv: inv, minPubTs: 1<<63 - 1}
	}
	var shardWG sync.WaitGroup
	for _, s := range shards {
		shardWG.Go(s.run)
	}

	_ = os.MkdirAll(*outDir, 0o755)
	stamp := time.Now().Format("20060102-150405")
	tlPath := filepath.Join(*outDir, "timeline-"+stamp+".csv")
	var received atomic.Int64
	var peakHeap atomic.Uint64
	stopMon := monitor(tlPath, shards, inv, &received, &invalid, &peakHeap)

	log.Printf("consumidor listo (shards=%d, ventana=%v/%d gen, retención garantizada=%v); esperando eventos...",
		cfg.Shards, cfg.Window, cfg.Generations, shards[0].win.GuaranteedRetention())
	log.Printf("inventario: %s, %d workers, timeout %v, breaker %d fallos/%v/%d pruebas, limitador=%v, buffer mem=%d disco=%d",
		cfg.Inventory.URL, cfg.Inventory.Workers, cfg.Inventory.Timeout, cfg.Inventory.BreakerFailures,
		cfg.Inventory.BreakerCooldown, cfg.Inventory.BreakerProbes, !cfg.Inventory.NoLimiter, cfg.Inventory.MemCap, cfg.Inventory.SpillCap)

	// it.Next() bloquea; un goroutine lector permite cerrar lotes por tiempo
	// cuando el tráfico es bajo.
	msgs := make(chan jetstream.Msg, cfg.PullBatch)
	go func() {
		for {
			msg, err := it.Next()
			if err != nil {
				if !errors.Is(err, jetstream.ErrMsgIteratorClosed) {
					log.Fatalf("leer del stream: %v", err)
				}
				return
			}
			msgs <- msg
		}
	}()
	batches := make(chan *batch, 1024)
	var acks atomic.Int64
	var commitWG sync.WaitGroup
	commitWG.Go(func() { committer(batches, &acks) })

	var firstRecv time.Time
	var totalLines int64 = -1
	var eofMsg jetstream.Msg
	cur := newBatch()
	sealTick := time.NewTicker(cfg.AckEvery)
	seal := func() {
		if cur.size == 0 {
			return
		}
		cur.seal()
		batches <- cur
		cur = newBatch()
	}
	for eofMsg == nil {
		var msg jetstream.Msg
		select {
		case msg = <-msgs:
		case <-sealTick.C:
			seal()
			continue
		}
		if firstRecv.IsZero() {
			firstRecv = time.Now()
		}
		if msg.Subject() == broker.EOFSubject {
			totalLines, _ = strconv.ParseInt(msg.Headers().Get(broker.HeaderTotalLines), 10, 64)
			eofMsg = msg
			break
		}
		received.Add(1)
		id, err := event.ExtractID(msg.Data())
		if err != nil {
			reject(msg, err, &invalid)
			continue
		}
		cur.add(msg)
		// El UUID v4 es aleatorio: sus primeros 8 bytes ya son un buen hash.
		shards[binary.LittleEndian.Uint64(id[:8])%uint64(len(shards))].in <- work{msg: msg, b: cur}
		if cur.size >= cfg.AckBatch {
			seal()
		}
	}
	sealTick.Stop()
	seal()
	close(batches)
	commitWG.Wait()
	_ = eofMsg.Ack() // con AckAll confirma también todo lo anterior
	acks.Add(1)
	it.Stop()
	for _, s := range shards {
		close(s.in)
	}
	shardWG.Wait()
	if err := nc.Flush(); err != nil { // asegura que los últimos acks salgan
		log.Printf("flush de acks: %v", err)
	}

	// El stream ya se aplicó completo; falta confirmar el inventario pendiente.
	inv.pipelineDone.Store(time.Now().UnixNano())
	log.Printf("pipeline principal terminado: %d eventos aplicados; drenando %d verificaciones pendientes de inventario",
		sumProcessed(shards), inv.q.Snapshot().Depth)
	inv.drain()
	stopMon()
	log.Printf("buffer drenado en %v", inv.drainEnd.Sub(inv.drainStart).Round(time.Second))

	rep := buildReport(cfg, shards, inv, firstRecv, received.Load(), invalid.Load(), totalLines, peakHeap.Load())
	rep.Timeline = tlPath
	rep.Counts.Acks = acks.Load()
	printReport(rep)
	path := filepath.Join(*outDir, "consumer-"+stamp+".json")
	b, _ := json.MarshalIndent(rep, "", "  ")
	if err := os.WriteFile(path, b, 0o644); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("\nreporte: %s\nlínea de tiempo: %s\n", path, tlPath)
	runtime.KeepAlive(shards)
}

func sumProcessed(shards []*shard) int64 {
	var n int64
	for _, s := range shards {
		n += s.processed.Load()
	}
	return n
}

func reject(msg jetstream.Msg, err error, invalid *atomic.Int64) {
	if invalid.Add(1) <= 5 {
		log.Printf("evento inválido descartado: %v: %.120s", err, msg.Data())
	}
	_ = msg.Term() // no reintentar: nunca va a parsear
}

func (s *shard) run() {
	for w := range s.in {
		s.handle(w.msg)
		w.b.finish()
		if n := s.processed.Add(1); n&1023 == 0 {
			s.winLen.Store(int64(s.win.Stats().Len))
		}
	}
	s.winLen.Store(int64(s.win.Stats().Len))
}

// handle aplica un mensaje: parseo, dedup y agregado. Sin ack: lo hace el
// committer cuando todo el lote está aplicado.
func (s *shard) handle(msg jetstream.Msg) {
	ev, err := event.Parse(msg.Data())
	if err != nil {
		reject(msg, err, s.invalid)
		return
	}
	pubTs, _ := strconv.ParseInt(msg.Headers().Get(broker.HeaderPubTs), 10, 64)
	redelivered := false
	if md, err := msg.Metadata(); err == nil && md.NumDelivered > 1 {
		redelivered = true
	}

	dup, origTS := s.win.SeenOrAdd(ev.ID, ev.TS)
	apply := false
	switch {
	case redelivered:
		s.redelivered++
		if dup {
			s.redeliveredDup++
		} else { // la entrega original nunca llegó a aplicarse
			apply = true
		}
	case dup:
		s.dups++
		s.delays.Add(ev.TS - origTS)
	default:
		apply = true
	}
	if apply {
		s.agg.Apply(&ev)
		s.applied++
		// Solo los únicos van a verificación: la dedup ocurre ANTES de llamar
		// al servicio externo (un reintento del gateway no cuesta una llamada).
		if ev.Type == event.InventoryReserved {
			s.inv.enqueue(&ev, pubTs)
		}
	}
	now := time.Now().UnixNano()
	if !redelivered && pubTs > 0 {
		l := now - pubTs
		s.lat = append(s.lat, l)
		s.minPubTs = min(s.minPubTs, pubTs)
		s.latSum.Add(l)
		s.latN.Add(1)
		if l > s.latMax.Load() {
			s.latMax.Store(l) // carrera benigna con el Swap(0) del monitor: a lo más se pierde un máximo de 1 s
		}
	}
	s.lastApply = now
}
