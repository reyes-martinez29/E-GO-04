// Invprobe caracteriza el servicio de inventario: para cada tasa de la lista
// envía llamadas a ritmo fijo (lazo abierto) durante -dur y reporta latencia
// y tasa de 503/timeouts. Al final mide cuánto tarda el servicio en "olvidar"
// una saturación (GET /stats hasta current_rps == 0).
//
// No es parte del sistema: es el experimento del que salen los parámetros del
// breaker y del limitador (decisions.md §3). El consumidor no usa estas cifras.
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"hotsale/internal/metrics"
)

func main() {
	base := flag.String("url", "http://127.0.0.1:8090", "URL base del servicio")
	rates := flag.String("rates", "20,40,60,80,120,180,250,400", "tasas a probar (llamadas/s)")
	dur := flag.Duration("dur", 5*time.Second, "duración de cada escalón")
	timeout := flag.Duration("timeout", 3*time.Second, "timeout por llamada")
	flag.Parse()

	client := &http.Client{Timeout: *timeout}
	fmt.Printf("%8s %8s %8s %8s %8s %8s %8s %8s\n", "rps", "llamadas", "ok", "503", "timeout", "p50 ms", "p95 ms", "max ms")
	for _, s := range strings.Split(*rates, ",") {
		rate, err := strconv.Atoi(strings.TrimSpace(s))
		if err != nil {
			log.Fatal(err)
		}
		post(client, *base+"/reset")
		time.Sleep(1500 * time.Millisecond) // vaciar la ventana de 1 s del servicio
		var mu sync.Mutex
		var lat []int64
		var ok, s503, tmo, other int
		var wg sync.WaitGroup
		start := time.Now()
		n := int(dur.Seconds() * float64(rate))
		for i := range n {
			time.Sleep(time.Until(start.Add(time.Duration(i) * time.Second / time.Duration(rate))))
			wg.Go(func() {
				t0 := time.Now()
				code, err := check(client, *base)
				d := time.Since(t0)
				mu.Lock()
				defer mu.Unlock()
				switch {
				case err != nil && strings.Contains(err.Error(), "Timeout"):
					tmo++
				case err != nil:
					other++
				case code == 200:
					ok++
					lat = append(lat, int64(d))
				case code == 503:
					s503++
					lat = append(lat, int64(d))
				default:
					other++
				}
			})
		}
		wg.Wait()
		l := metrics.Summarize(lat)
		fmt.Printf("%8d %8d %7.1f%% %7.1f%% %7.1f%% %8.1f %8.1f %8.1f\n", rate, n,
			pct(ok, n), pct(s503, n), pct(tmo, n), l.P50Ms, l.P95Ms, l.MaxMs)
		if other > 0 {
			fmt.Printf("         otros errores: %d\n", other)
		}
	}

	// Recuperación: saturar 3 s y medir cuándo el servicio vuelve a ver 0 rps.
	fmt.Printf("\nrecuperación tras saturar a 400 rps durante 3 s:\n")
	start := time.Now()
	for i := range 1200 {
		time.Sleep(time.Until(start.Add(time.Duration(i) * time.Second / 400)))
		go check(client, *base)
	}
	stop := time.Now()
	for {
		rps := stats(client, *base)
		if rps == 0 {
			fmt.Printf("current_rps llegó a 0 a los %v de dejar de llamar\n", time.Since(stop).Round(10*time.Millisecond))
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	t0 := time.Now()
	code, err := check(client, *base)
	fmt.Printf("primera llamada después: status=%d err=%v latencia=%v\n", code, err, time.Since(t0).Round(time.Millisecond))
}

func check(c *http.Client, base string) (int, error) {
	resp, err := c.Post(base+"/check-inventory", "application/json", bytes.NewReader([]byte(`{"order_id":"probe","country":"MX"}`)))
	if err != nil {
		return 0, err
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	return resp.StatusCode, nil
}

func post(c *http.Client, url string) {
	resp, err := c.Post(url, "application/json", nil)
	if err != nil {
		log.Fatalf("%s: %v", url, err)
	}
	resp.Body.Close()
}

func stats(c *http.Client, base string) int {
	resp, err := c.Get(base + "/stats")
	if err != nil {
		log.Fatal(err)
	}
	defer resp.Body.Close()
	var s struct {
		CurrentRPS int `json:"current_rps"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&s)
	return s.CurrentRPS
}

func pct(a, n int) float64 { return 100 * float64(a) / float64(max(n, 1)) }
