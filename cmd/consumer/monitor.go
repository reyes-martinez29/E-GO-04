package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"hotsale/internal/breaker"
)

// monitor muestrea cada segundo y escribe una fila en la línea de tiempo CSV:
// es la evidencia de cómo se comportó el sistema durante el spike (ritmo de
// ingesta, latencia, profundidad del buffer, estado del breaker, tasa del
// limitador). Además imprime progreso cada 5 s (cada 30 s durante el drenado,
// que puede durar horas) y registra el heap pico.
func monitor(path string, shards []*shard, inv *inventory, received, invalid *atomic.Int64, peakHeap *atomic.Uint64) (stop func()) {
	f, err := os.Create(path)
	if err != nil {
		log.Fatalf("línea de tiempo: %v", err)
	}
	w := bufio.NewWriter(f)
	fmt.Fprintln(w, "t_s,received_per_s,applied_per_s,e2e_mean_ms,e2e_max_ms,"+
		"inv_enqueued_per_s,inv_confirmed_per_s,inv_confirmed_total,buffer_depth,buffer_mem,buffer_disk,buffer_level,oldest_wait_s,"+
		"breaker_state,limiter_rps,calls_per_s,ok_per_s,fail_per_s,rejected_per_s,heap_mib")

	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Go(func() {
		t0 := time.Now()
		tick := time.NewTicker(time.Second)
		defer tick.Stop()
		var ms runtime.MemStats
		var last struct{ recv, applied, latSum, latN, enq, conf, calls, ok, fail, rej int64 }
		var lastLogApplied, lastLogConf int64
		lastLog := t0
		for {
			select {
			case <-done:
				return
			case <-tick.C:
			}
			runtime.ReadMemStats(&ms)
			if ms.HeapInuse > peakHeap.Load() {
				peakHeap.Store(ms.HeapInuse)
			}
			var applied, latSum, latN, latMax int64
			for _, s := range shards {
				applied += s.processed.Load()
				latSum += s.latSum.Load()
				latN += s.latN.Load()
				latMax = max(latMax, s.latMax.Swap(0))
			}
			recv := received.Load()
			enq, conf := inv.enqueued.Load(), inv.confirmed.Load()
			calls, ok, fail, rej := inv.calls.Load(), inv.ok.Load(), inv.failed.Load(), int64(inv.br.Stats().Rejected)
			var latMean float64
			if dn := latN - last.latN; dn > 0 {
				latMean = float64(latSum-last.latSum) / float64(dn) / 1e6
			}
			snap := inv.q.Snapshot()
			st := breaker.State(inv.breakerState.Load())
			fmt.Fprintf(w, "%.0f,%d,%d,%.2f,%.2f,%d,%d,%d,%d,%d,%d,%s,%.1f,%s,%.1f,%d,%d,%d,%d,%d\n",
				time.Since(t0).Seconds(), recv-last.recv, applied-last.applied, latMean, float64(latMax)/1e6,
				enq-last.enq, conf-last.conf, conf, snap.Depth, snap.Mem, snap.Disk, snap.Level, snap.Oldest.Seconds(),
				st, inv.limiterRate(), calls-last.calls, ok-last.ok, fail-last.fail, rej-last.rej, ms.HeapInuse>>20)
			last.recv, last.applied, last.latSum, last.latN = recv, applied, latSum, latN
			last.enq, last.conf, last.calls, last.ok, last.fail, last.rej = enq, conf, calls, ok, fail, rej

			every := 5 * time.Second
			if inv.pipelineDone.Load() != 0 {
				every = 30 * time.Second
				_ = w.Flush() // el drenado es largo: que el CSV se pueda seguir en vivo
			}
			if el := time.Since(lastLog); el >= every {
				if inv.pipelineDone.Load() == 0 {
					var winLen int64
					for _, s := range shards {
						winLen += s.winLen.Load()
					}
					log.Printf("recibidos=%d aplicados=%d (%.0f ev/s) ventana=%d llaves heap=%d MiB inválidos=%d | %s",
						recv, applied, float64(applied-lastLogApplied)/el.Seconds(), winLen, ms.HeapInuse>>20, invalid.Load(), inv.progress())
				} else {
					r := float64(conf-lastLogConf) / el.Seconds()
					eta := "-"
					if r > 0 {
						eta = (time.Duration(float64(snap.Depth)/r) * time.Second).Round(time.Second).String()
					}
					log.Printf("drenando %.1f confirmaciones/s, ETA %s | %s", r, eta, inv.progress())
				}
				lastLog, lastLogApplied, lastLogConf = time.Now(), applied, conf
			}
		}
	})
	return func() {
		close(done)
		wg.Wait()
		_ = w.Flush()
		_ = f.Close()
	}
}
