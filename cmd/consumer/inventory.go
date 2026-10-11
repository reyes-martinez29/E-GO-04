package main

// Verificación de inventario (E-Go04).
//
//	shards ──Push──▶ backpressure.Queue  (memoria → disco → bloqueo; nunca descarta)
//	                       │ Pop
//	                       ▼
//	               W workers: breaker.Allow → limiter.Wait → POST /check-inventory
//	                 200: evento confirmado (procesado)
//	                 503 / timeout / error / breaker abierto: el worker CONSERVA
//	                 el ítem y reintenta con espera; jamás lo suelta sin confirmar
//
// Lo que se atrasa es solo la confirmación de inventario: el agregado de
// órdenes y pagos sigue al ritmo del stream. El orden de las protecciones:
//   - limitador (AIMD): mantiene la tasa de llamadas donde el servicio
//     responde rápido y sin 503; reacciona a degradación (latencia, fallos).
//   - breaker: corta por completo cuando el servicio falla de forma
//     consecutiva (saturado o caído) y prueba la recuperación en half-open.
//   - W workers: tope duro de llamadas en vuelo (goroutines y conexiones que
//     un servicio colgado puede retener), cada una con timeout.

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand/v2"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"hotsale/internal/backpressure"
	"hotsale/internal/breaker"
	"hotsale/internal/event"
)

type invConfig struct {
	URL             string        `json:"url"`
	Workers         int           `json:"workers"`
	Timeout         time.Duration `json:"call_timeout"`
	BreakerFailures int           `json:"breaker_consecutive_failures"`
	BreakerCooldown time.Duration `json:"breaker_cooldown"`
	BreakerProbes   int           `json:"breaker_half_open_probes"`
	NoLimiter       bool          `json:"no_limiter"`
	LimInitial      float64       `json:"limiter_initial_rps"`
	LimMin          float64       `json:"limiter_min_rps"`
	LimMax          float64       `json:"limiter_max_rps"`
	LimIncrease     float64       `json:"limiter_increase_rps"`
	LimDecrease     float64       `json:"limiter_decrease_factor"`
	LimWindow       time.Duration `json:"limiter_window"`
	LimLatFactor    float64       `json:"limiter_latency_factor"`
	MemCap          int           `json:"buffer_mem_cap"`
	SpillCap        int           `json:"buffer_spill_cap"`
	SpillPath       string        `json:"buffer_spill_path"`
}

type callResult int

const (
	resOK callResult = iota
	res503
	resTimeout
	resConnErr
	resOtherStatus
)

type levelChange struct {
	At    time.Time `json:"at"`
	From  string    `json:"from"`
	To    string    `json:"to"`
	Depth int       `json:"depth"`
	Mem   int       `json:"mem"`
	Disk  int       `json:"disk"`
}

type inventory struct {
	cfg      invConfig
	q        *backpressure.Queue
	br       *breaker.Breaker
	lim      *backpressure.Limiter // nil con -no-limiter (ablación)
	client   *http.Client
	endpoint string

	enqueued, confirmed  atomic.Int64
	calls, ok, s503      atomic.Int64
	failed               atomic.Int64 // 503 + timeouts + conexión + otros
	timeouts, connErrs   atomic.Int64
	otherStatus          atomic.Int64
	rejected             atomic.Int64 // intentos que el breaker no dejó salir
	itemsRetried         atomic.Int64 // ítems que necesitaron más de una llamada
	breakerState         atomic.Int32
	stateMu              sync.Mutex
	transitions          []breaker.Transition
	levels               []levelChange
	workers              []*invWorker
	wg                   sync.WaitGroup
	drainStart, drainEnd time.Time
	pipelineDone         atomic.Int64 // UnixNano en que terminó el pipeline principal (0 = no ha terminado)
}

type invWorker struct {
	callLat []int64 // latencia de cada llamada HTTP
	waitLat []int64 // entrada al buffer → confirmado
}

func newInventory(cfg invConfig) (*inventory, error) {
	inv := &inventory{cfg: cfg, endpoint: cfg.URL + "/check-inventory"}
	if !cfg.NoLimiter {
		inv.lim = backpressure.NewLimiter(backpressure.LimiterConfig{
			Initial: cfg.LimInitial, Min: cfg.LimMin, Max: cfg.LimMax,
			Increase: cfg.LimIncrease, Decrease: cfg.LimDecrease,
			Window: cfg.LimWindow, LatencyFactor: cfg.LimLatFactor,
		})
	}
	br, err := breaker.New(breaker.Config{
		FailureThreshold: cfg.BreakerFailures,
		OpenTimeout:      cfg.BreakerCooldown,
		HalfOpenProbes:   cfg.BreakerProbes,
		OnTransition:     inv.onTransition,
	})
	if err != nil {
		return nil, err
	}
	inv.br = br
	q, err := backpressure.NewQueue(backpressure.Config{
		MemCap: cfg.MemCap, SpillCap: cfg.SpillCap, SpillPath: cfg.SpillPath,
		OnLevel: inv.onLevel,
	})
	if err != nil {
		return nil, err
	}
	inv.q = q
	inv.client = &http.Client{
		Timeout: cfg.Timeout,
		Transport: &http.Transport{
			MaxIdleConnsPerHost: cfg.Workers,
			MaxConnsPerHost:     cfg.Workers,
			DialContext:         (&net.Dialer{Timeout: cfg.Timeout}).DialContext,
		},
	}
	return inv, nil
}

// onTransition corre con el lock del breaker tomado: solo registra y avisa.
func (inv *inventory) onTransition(tr breaker.Transition) {
	inv.breakerState.Store(int32(tr.To))
	log.Printf("BREAKER %s → %s (%s)", tr.From, tr.To, tr.Reason)
	inv.stateMu.Lock()
	inv.transitions = append(inv.transitions, tr)
	inv.stateMu.Unlock()
	// Recortar solo al abrir desde CLOSED: una reapertura desde HALF-OPEN no
	// dice nada nuevo sobre la tasa (en OPEN no salió ninguna llamada). Ojo:
	// cada prueba fallida en HALF-OPEN igual cuenta como ventana con fallo en
	// el limitador, así que tras una caída larga la tasa queda en el mínimo y
	// vuelve a la capacidad sana en ~25 s (+2/s). Mejora pendiente: restaurar
	// la tasa previa a la apertura al cerrar (decisions.md §6).
	if tr.To == breaker.Open && tr.From == breaker.Closed && inv.lim != nil {
		inv.lim.Backoff()
	}
}

func (inv *inventory) onLevel(from, to backpressure.Level, s backpressure.Snapshot) {
	log.Printf("BUFFER %s → %s (profundidad=%d memoria=%d disco=%d)", from, to, s.Depth, s.Mem, s.Disk)
	inv.stateMu.Lock()
	inv.levels = append(inv.levels, levelChange{At: time.Now(), From: from.String(), To: to.String(), Depth: s.Depth, Mem: s.Mem, Disk: s.Disk})
	inv.stateMu.Unlock()
}

func (inv *inventory) start() {
	for range inv.cfg.Workers {
		w := &invWorker{}
		inv.workers = append(inv.workers, w)
		inv.wg.Go(func() { inv.work(w) })
	}
}

// enqueue lo llama un shard para un inventory_reserved ya deduplicado y
// aplicado. Puede bloquear (nivel BLOCKED): esa es la señal que frena al
// consumidor y deja los mensajes esperando en el broker.
func (inv *inventory) enqueue(ev *event.Event, pubTs int64) {
	it := backpressure.Item{EventID: ev.ID, OrderID: ev.OrderID, Country: uint8(ev.Country), PubTs: pubTs, Enqueued: time.Now().UnixNano()}
	if err := inv.q.Push(it); err != nil {
		log.Fatalf("buffer de inventario: %v", err) // falla de disco: detenerse antes que perder
	}
	inv.enqueued.Add(1)
}

// drain cierra el buffer (ya no llegan eventos) y espera a que se confirme todo.
func (inv *inventory) drain() {
	inv.drainStart = time.Now()
	inv.q.Close()
	inv.wg.Wait()
	inv.drainEnd = time.Now()
	if err := inv.q.Remove(); err != nil {
		log.Printf("borrar archivo de desborde: %v", err)
	}
}

func (inv *inventory) work(w *invWorker) {
	ctx := context.Background()
	body := make([]byte, 0, 96)
	for {
		it, ok, err := inv.q.Pop()
		if err != nil {
			log.Fatalf("buffer de inventario: %v", err)
		}
		if !ok {
			return
		}
		body = appendBody(body[:0], &it)
		for attempt := 0; ; attempt++ {
			tk, err := inv.br.Allow()
			if err != nil {
				inv.rejected.Add(1)
				var oe *breaker.OpenError
				if errors.As(err, &oe) {
					time.Sleep(jitter(oe.RetryAfter, 0.1))
				}
				continue
			}
			if inv.lim != nil {
				_ = inv.lim.Wait(ctx)
			}
			t0 := time.Now()
			res := inv.call(ctx, body)
			lat := time.Since(t0)
			tk.Done(res == resOK)
			if res != resOK {
				inv.failed.Add(1)
			}
			if inv.lim != nil {
				inv.lim.Observe(t0, lat, res != resOK)
			}
			w.callLat = append(w.callLat, int64(lat))
			if res == resOK {
				break
			}
			if attempt == 0 {
				inv.itemsRetried.Add(1)
			}
			// Espera exponencial con jitter por ítem: evita que un worker
			// reintente en lazo cerrado; el limitador y el breaker regulan el total.
			time.Sleep(jitter(min(50*time.Millisecond<<min(attempt, 6), 2*time.Second), 0.5))
		}
		w.waitLat = append(w.waitLat, time.Now().UnixNano()-it.Enqueued)
		inv.confirmed.Add(1)
	}
}

func (inv *inventory) call(ctx context.Context, body []byte) callResult {
	inv.calls.Add(1)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, inv.endpoint, bytes.NewReader(body))
	if err != nil {
		log.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := inv.client.Do(req)
	if err != nil {
		var ne net.Error
		if errors.As(err, &ne) && ne.Timeout() {
			inv.timeouts.Add(1)
			return resTimeout
		}
		inv.connErrs.Add(1)
		return resConnErr
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusOK:
		inv.ok.Add(1)
		return resOK
	case http.StatusServiceUnavailable:
		inv.s503.Add(1)
		return res503
	default:
		inv.otherStatus.Add(1)
		return resOtherStatus
	}
}

// appendBody arma {"order_id":"<uuid>","country":"MX"} sin encoding/json.
func appendBody(b []byte, it *backpressure.Item) []byte {
	b = append(b, `{"order_id":"`...)
	b = append(b, event.ID(it.OrderID).String()...)
	b = append(b, `","country":"`...)
	b = append(b, event.CountryNames[it.Country]...)
	return append(b, `"}`...)
}

// jitter devuelve d ± frac·d, para que los workers no se sincronicen.
func jitter(d time.Duration, frac float64) time.Duration {
	if d <= 0 {
		return 0
	}
	return time.Duration(float64(d) * (1 - frac + 2*frac*rand.Float64()))
}

func (inv *inventory) limiterRate() float64 {
	if inv.lim == nil {
		return 0
	}
	return inv.lim.Rate()
}

func (inv *inventory) progress() string {
	s := inv.q.Snapshot()
	return fmt.Sprintf("inventario: confirmados=%d/%d buffer=%d (mem=%d disco=%d, nivel=%s, más viejo=%.1fs) breaker=%s límite=%.1f llamadas/s",
		inv.confirmed.Load(), inv.enqueued.Load(), s.Depth, s.Mem, s.Disk, s.Level, s.Oldest.Seconds(),
		breaker.State(inv.breakerState.Load()), inv.limiterRate())
}
