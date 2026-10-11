package main

import (
	"fmt"
	"runtime"
	"sort"
	"time"

	"hotsale/internal/agg"
	"hotsale/internal/backpressure"
	"hotsale/internal/breaker"
	"hotsale/internal/metrics"
)

type counts struct {
	ExpectedLines   int64 `json:"expected_lines"`   // según el EOF del productor
	Received        int64 `json:"received"`         // todas las entregas de eventos
	FirstDeliveries int64 `json:"first_deliveries"` // NumDelivered == 1
	UniqueApplied   int64 `json:"unique_applied"`
	DuplicatesSrc   int64 `json:"duplicates_discarded"` // reintentos del gateway detectados
	Redelivered     int64 `json:"broker_redeliveries"`
	RedeliveredDup  int64 `json:"broker_redeliveries_discarded"`
	Invalid         int64 `json:"invalid"`
	Acks            int64 `json:"acks_sent"` // con AckAll: uno por lote
	// Procesado = aplicado al agregado y, si es inventory_reserved, además
	// confirmado por el servicio de inventario.
	Processed int64 `json:"processed"`
}

type report struct {
	Label       string              `json:"label"`
	Timestamp   time.Time           `json:"timestamp"`
	Config      config              `json:"config"`
	Counts      counts              `json:"counts"`
	Checks      map[string]bool     `json:"checks"`
	Throughput  throughput          `json:"throughput"`
	Latency     metrics.Latency     `json:"latency_end_to_end"`
	RetryDelays metrics.RetryDelays `json:"retry_delays"`
	Dedup       dedupReport         `json:"dedup"`
	Memory      memory              `json:"memory"`
	Inventory   invReport           `json:"inventory"`
	Aggregate   agg.Report          `json:"aggregate"`
	Timeline    string              `json:"timeline_csv"`
}

type throughput struct {
	ConsumerSec   float64 `json:"consumer_seconds"` // primer mensaje recibido -> último aplicado
	ConsumerEvSec float64 `json:"consumer_events_per_sec"`
	EndToEndSec   float64 `json:"end_to_end_seconds"` // primer Pub-Ts -> último aplicado
	EndToEndEvSec float64 `json:"end_to_end_events_per_sec"`
}

type dedupReport struct {
	GuaranteedRetention string `json:"guaranteed_retention"`
	FinalKeys           int    `json:"final_keys"`
	Rotations           uint64 `json:"rotations"`
	PrematureEvictions  uint64 `json:"premature_evictions"`
}

type memory struct {
	PeakHeapInuseMiB  uint64 `json:"peak_heap_inuse_mib"`
	FinalHeapInuseMiB uint64 `json:"final_heap_inuse_mib"`
}

type invReport struct {
	Enqueued     int64 `json:"enqueued"`  // inventory_reserved únicos que entraron al buffer
	Confirmed    int64 `json:"confirmed"` // confirmados con 200
	FinalDepth   int   `json:"final_buffer_depth"`
	Calls        int64 `json:"calls"`
	OK           int64 `json:"ok_200"`
	HTTP503      int64 `json:"overloaded_503"`
	Timeouts     int64 `json:"timeouts"`
	ConnErrors   int64 `json:"connection_errors"`
	OtherStatus  int64 `json:"other_status"`
	Rejected     int64 `json:"rejected_by_breaker"`
	ItemsRetried int64 `json:"items_needing_retry"`

	CallLatency metrics.Latency `json:"call_latency"`
	WaitLatency metrics.Latency `json:"wait_enqueue_to_confirmed"`

	Breaker          breaker.Stats              `json:"breaker"`
	Transitions      []breaker.Transition       `json:"breaker_transitions"` // las primeras 500
	TransitionsTotal int                        `json:"breaker_transitions_total"`
	Buffer           backpressure.Stats         `json:"buffer"`
	BufferLevels     []levelChange              `json:"buffer_level_changes"` // los primeros 500
	Limiter          *backpressure.LimiterStats `json:"limiter,omitempty"`

	PipelineSec float64 `json:"main_pipeline_seconds"` // primer recibido -> último evento aplicado
	DrainSec    float64 `json:"drain_seconds"`         // EOF aplicado -> buffer vacío
	TotalSec    float64 `json:"total_seconds"`         // primer recibido -> última confirmación
}

func buildReport(cfg config, shards []*shard, inv *inventory, firstRecv time.Time, received, invalid, totalLines int64, peakHeap uint64) report {
	var total agg.Agg
	var c counts
	var lat []int64
	var delays metrics.RetryDelays
	var d dedupReport
	minPub, lastApply := int64(1<<63-1), int64(0)
	for _, s := range shards {
		total.Merge(&s.agg)
		c.UniqueApplied += s.applied
		c.DuplicatesSrc += s.dups
		c.Redelivered += s.redelivered
		c.RedeliveredDup += s.redeliveredDup
		lat = append(lat, s.lat...)
		s.lat = nil
		delays.Merge(s.delays)
		st := s.win.Stats()
		d.FinalKeys += st.Len
		d.Rotations += st.Rotations
		d.PrematureEvictions += st.Premature
		d.GuaranteedRetention = s.win.GuaranteedRetention().String()
		minPub = min(minPub, s.minPubTs)
		lastApply = max(lastApply, s.lastApply)
	}
	delays.Finish()
	c.ExpectedLines = totalLines
	c.Received = received
	c.Invalid = invalid
	c.FirstDeliveries = received - c.Redelivered
	aggRep := total.Report()

	ir := buildInvReport(inv, firstRecv, lastApply)
	c.Processed = c.UniqueApplied - ir.Enqueued + ir.Confirmed

	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)

	r := report{
		Label: cfg.Label, Timestamp: time.Now(), Config: cfg, Counts: c,
		Latency: metrics.Summarize(lat), RetryDelays: delays, Dedup: d, Aggregate: aggRep, Inventory: ir,
		Memory: memory{PeakHeapInuseMiB: peakHeap >> 20, FinalHeapInuseMiB: ms.HeapInuse >> 20},
	}
	if lastApply > 0 {
		r.Throughput.ConsumerSec = time.Duration(lastApply - firstRecv.UnixNano()).Seconds()
		r.Throughput.ConsumerEvSec = float64(c.FirstDeliveries) / r.Throughput.ConsumerSec
		r.Throughput.EndToEndSec = time.Duration(lastApply - minPub).Seconds()
		r.Throughput.EndToEndEvSec = float64(c.FirstDeliveries) / r.Throughput.EndToEndSec
	}
	r.Checks = map[string]bool{
		// De E-Go03: ninguna línea publicada se perdió y la dedup cuadra.
		"no_loss: first_deliveries == expected_lines":                   c.FirstDeliveries == totalLines,
		"accounting: unique + duplicates + invalid == first_deliveries": c.UniqueApplied+c.DuplicatesSrc+c.Invalid == c.FirstDeliveries,
		"aggregate_total == unique_applied":                             aggRep.Total.UniqueEventsCounted == c.UniqueApplied,
		"no_premature_evictions":                                        d.PrematureEvictions == 0,
		// E-Go04: cada inventory_reserved único entró al buffer y salió confirmado.
		"inventory: enqueued == aggregate inventory_reserved": ir.Enqueued == aggRep.Total.InventoryReserved,
		"inventory: confirmed == enqueued (nada se descartó)": ir.Confirmed == ir.Enqueued,
		"buffer: final depth == 0":                            ir.FinalDepth == 0,
		"processed == unique_applied":                         c.Processed == c.UniqueApplied,
	}
	return r
}

func buildInvReport(inv *inventory, firstRecv time.Time, lastApply int64) invReport {
	r := invReport{
		Enqueued: inv.enqueued.Load(), Confirmed: inv.confirmed.Load(), FinalDepth: inv.q.Snapshot().Depth,
		Calls: inv.calls.Load(), OK: inv.ok.Load(), HTTP503: inv.s503.Load(), Timeouts: inv.timeouts.Load(),
		ConnErrors: inv.connErrs.Load(), OtherStatus: inv.otherStatus.Load(), Rejected: int64(inv.br.Stats().Rejected),
		ItemsRetried: inv.itemsRetried.Load(),
		Breaker:      inv.br.Stats(), Buffer: inv.q.Stats(),
	}
	var callLat, waitLat []int64
	for _, w := range inv.workers {
		callLat = append(callLat, w.callLat...)
		waitLat = append(waitLat, w.waitLat...)
		w.callLat, w.waitLat = nil, nil
	}
	r.CallLatency = metrics.Summarize(callLat)
	r.WaitLatency = metrics.Summarize(waitLat)
	inv.stateMu.Lock()
	r.TransitionsTotal = len(inv.transitions)
	r.Transitions = inv.transitions[:min(len(inv.transitions), 500)]
	r.BufferLevels = inv.levels[:min(len(inv.levels), 500)]
	inv.stateMu.Unlock()
	if inv.lim != nil {
		ls := inv.lim.Stats()
		r.Limiter = &ls
	}
	if lastApply > 0 {
		r.PipelineSec = time.Duration(lastApply - firstRecv.UnixNano()).Seconds()
	}
	r.DrainSec = inv.drainEnd.Sub(inv.drainStart).Seconds()
	r.TotalSec = inv.drainEnd.Sub(firstRecv).Seconds()
	return r
}

func printReport(r report) {
	c := r.Counts
	fmt.Printf("\n=== CONSUMIDOR: AGREGADO FINAL ===\n")
	fmt.Printf("%-6s %10s %16s %12s %16s %12s\n", "país", "órdenes", "órdenes USD", "pagos", "pagos USD", "inventario")
	for _, cn := range []string{"MX", "AR", "CO", "CL", "PE"} {
		l := r.Aggregate.ByCountry[cn]
		fmt.Printf("%-6s %10d %16s %12d %16s %12d\n", cn, l.Orders, l.OrdersUSD, l.PaymentsConfirmed, l.PaymentsUSD, l.InventoryReserved)
	}
	t := r.Aggregate.Total
	fmt.Printf("%-6s %10d %16s %12d %16s %12d\n", "TOTAL", t.Orders, t.OrdersUSD, t.PaymentsConfirmed, t.PaymentsUSD, t.InventoryReserved)

	fmt.Printf("\n=== CONTEO ===\n")
	fmt.Printf("líneas esperadas (EOF):     %d\n", c.ExpectedLines)
	fmt.Printf("primeras entregas:          %d\n", c.FirstDeliveries)
	fmt.Printf("eventos únicos aplicados:   %d\n", c.UniqueApplied)
	fmt.Printf("duplicados descartados:     %d\n", c.DuplicatesSrc)
	fmt.Printf("re-entregas del broker:     %d (descartadas por la ventana: %d)\n", c.Redelivered, c.RedeliveredDup)
	fmt.Printf("inválidos:                  %d\n", c.Invalid)
	fmt.Printf("acks enviados (AckAll):     %d\n", c.Acks)
	fmt.Printf("EVENTOS PROCESADOS:         %d (esperados: %d únicos)\n", c.Processed, c.UniqueApplied)

	i := r.Inventory
	fmt.Printf("\n=== INVENTARIO ===\n")
	fmt.Printf("encolados / confirmados:    %d / %d\n", i.Enqueued, i.Confirmed)
	fmt.Printf("llamadas:                   %d (200=%d 503=%d timeout=%d conexión=%d otros=%d)\n", i.Calls, i.OK, i.HTTP503, i.Timeouts, i.ConnErrors, i.OtherStatus)
	fmt.Printf("rechazadas por el breaker:  %d; ítems que necesitaron reintento: %d\n", i.Rejected, i.ItemsRetried)
	fmt.Printf("latencia por llamada:       p50=%.1fms p95=%.1fms p99=%.1fms max=%.1fms\n", i.CallLatency.P50Ms, i.CallLatency.P95Ms, i.CallLatency.P99Ms, i.CallLatency.MaxMs)
	fmt.Printf("espera en buffer:           p50=%s p95=%s max=%s\n", dur(i.WaitLatency.P50Ms), dur(i.WaitLatency.P95Ms), dur(i.WaitLatency.MaxMs))
	if i.Limiter != nil {
		fmt.Printf("limitador:                  tasa min=%.1f max=%.1f llamadas/s, base=%.1fms, +%d / -%d (fallos %d, latencia %d), backoffs por breaker %d\n",
			i.Limiter.MinRate, i.Limiter.MaxRate, i.Limiter.BaselineMs, i.Limiter.Increases, i.Limiter.Decreases,
			i.Limiter.DecreasesFail, i.Limiter.DecreasesSlow, i.Limiter.Backoffs)
	}
	fmt.Printf("pipeline principal:         %.1fs; drenado del buffer: %s; total: %s\n", i.PipelineSec, dur(i.DrainSec*1000), dur(i.TotalSec*1000))

	b := i.Breaker
	fmt.Printf("\n=== CIRCUIT BREAKER ===\n")
	fmt.Printf("veces que abrió:            %d (por fallos consecutivos: %d, por prueba fallida en half-open: %d)\n", b.Opens, b.OpensFromClosed, b.ReopensFromHalfOpen)
	fmt.Printf("cerró tras half-open:       %d\n", b.ClosesFromHalfOpen)
	fmt.Printf("tiempo total abierto:       %s (half-open: %s)\n", dur(b.OpenSeconds*1000), dur(b.HalfOpenSeconds*1000))
	fmt.Printf("racha máx. de fallos:       %d\n", b.MaxConsecutiveFails)

	bf := i.Buffer
	fmt.Printf("\n=== BUFFER DE ESPERA (backpressure) ===\n")
	fmt.Printf("profundidad máxima:         %d (memoria máx. %d, disco máx. %d)\n", bf.MaxDepth, bf.MaxMem, bf.MaxDisk)
	fmt.Printf("ítems que pasaron por disco: %d; Push bloqueados: %d\n", bf.Spilled, bf.BlockedPushes)
	levels := make([]string, 0, len(bf.TimeInLevel))
	for l := range bf.TimeInLevel {
		levels = append(levels, l)
	}
	sort.Strings(levels)
	for _, l := range levels {
		fmt.Printf("tiempo en nivel %-9s   %s\n", l+":", dur(bf.TimeInLevel[l]*1000))
	}

	fmt.Printf("\n=== RENDIMIENTO DEL PIPELINE PRINCIPAL ===\n")
	fmt.Printf("throughput consumidor:      %.0f ev/s (%.2fs, primer recibido -> último aplicado)\n", r.Throughput.ConsumerEvSec, r.Throughput.ConsumerSec)
	l := r.Latency
	fmt.Printf("latencia e2e (publicado -> aplicado): p50=%.2fms p95=%.2fms p99=%.2fms max=%.2fms (n=%d)\n", l.P50Ms, l.P95Ms, l.P99Ms, l.MaxMs, l.Samples)
	fmt.Printf("heap pico: %d MiB; ventana final: %d llaves, %d expulsiones prematuras\n", r.Memory.PeakHeapInuseMiB, r.Dedup.FinalKeys, r.Dedup.PrematureEvictions)

	fmt.Printf("\n=== CHEQUEOS ===\n")
	keys := make([]string, 0, len(r.Checks))
	for k := range r.Checks {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		fmt.Printf("[%s] %s\n", map[bool]string{true: "OK", false: "FALLA"}[r.Checks[k]], k)
	}
}

func dur(ms float64) string {
	return (time.Duration(ms * float64(time.Millisecond))).Round(100 * time.Millisecond).String()
}
