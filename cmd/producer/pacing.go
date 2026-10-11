package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Pacing por fases. Cada fase tiene una tasa fija y la línea n de la fase sale
// en inicio_de_fase + (n - n0)/tasa: se programa contra el reloj absoluto (no
// un sleep fijo por mensaje), así los retrasos no se acumulan. Las fronteras
// de fase también son instantes absolutos (t0 + offset), así que el spike
// empieza y termina a la hora planeada aunque una fase anterior se haya
// atrasado.

type phase struct {
	name     string
	from, to time.Duration // offset desde t0; to == 0: hasta agotar el archivo
	rate     float64       // eventos/s; 0 = sin límite

	// medido
	startedAt time.Time
	lines     uint64
	last      time.Time
}

func (p *phase) describe() string {
	r := "sin límite"
	if p.rate > 0 {
		r = fmt.Sprintf("%.0f ev/s", p.rate)
	}
	if p.to == 0 {
		return r + " hasta agotar el archivo"
	}
	return fmt.Sprintf("%s durante %v", r, p.to-p.from)
}

type schedule struct {
	mu         sync.Mutex // current() se lee desde el goroutine de la línea de tiempo
	phases     []*phase
	cur        int
	t0         time.Time
	phaseStart time.Time
	n0         uint64
}

func newSchedule(rate int, mult float64, after, dur time.Duration) *schedule {
	r := float64(rate)
	switch {
	case rate <= 0:
		return &schedule{phases: []*phase{{name: "sin-limite"}}}
	case dur <= 0:
		return &schedule{phases: []*phase{{name: "normal", rate: r}}}
	}
	var ps []*phase
	if after > 0 {
		ps = append(ps, &phase{name: "normal", from: 0, to: after, rate: r})
	}
	ps = append(ps,
		&phase{name: "spike", from: after, to: after + dur, rate: r * mult},
		&phase{name: "normal-post", from: after + dur, rate: r})
	return &schedule{phases: ps}
}

func (s *schedule) String() string {
	var b strings.Builder
	for i, p := range s.phases {
		if i > 0 {
			b.WriteString(" → ")
		}
		fmt.Fprintf(&b, "%s (%s)", p.name, p.describe())
	}
	return b.String()
}

func (s *schedule) start(t time.Time) {
	s.t0, s.phaseStart = t, t
	s.phases[0].startedAt = t
}

// pace duerme hasta que le toca salir a la línea n (1-based). Devuelve la
// fase en la que cae y si es la primera línea de una fase nueva.
func (s *schedule) pace(n uint64) (*phase, bool) {
	p := s.phases[s.cur]
	changed := false
	for {
		if p.rate <= 0 {
			return p, changed
		}
		target := s.phaseStart.Add(time.Duration(float64(n-s.n0) / p.rate * float64(time.Second)))
		if p.to > 0 && s.cur < len(s.phases)-1 && !target.Before(s.t0.Add(p.to)) {
			// La línea n ya cae en la fase siguiente.
			s.mu.Lock()
			s.cur++
			s.mu.Unlock()
			s.phaseStart = s.t0.Add(p.to)
			s.n0 = n - 1
			p = s.phases[s.cur]
			p.startedAt = s.phaseStart
			changed = true
			continue
		}
		if d := time.Until(target); d > time.Millisecond {
			time.Sleep(d)
		}
		return p, changed
	}
}

// count registra que la línea actual ya se publicó.
func (s *schedule) count(now time.Time) {
	p := s.phases[s.cur]
	p.lines++
	p.last = now
}

func (s *schedule) current() *phase {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.phases[s.cur]
}

// printSummary: tasa real de cada fase = líneas / (última línea - inicio de fase).
func (s *schedule) printSummary() {
	fmt.Printf("\n=== FASES (tasa real medida) ===\n")
	fmt.Printf("%-12s %12s %12s %10s %12s\n", "fase", "objetivo", "líneas", "duración", "real ev/s")
	var normal, spike float64
	var spikeDur time.Duration
	for _, p := range s.phases {
		if p.lines == 0 {
			fmt.Printf("%-12s %12.0f %12d %10s %12s\n", p.name, p.rate, 0, "-", "-")
			continue
		}
		d := p.last.Sub(p.startedAt)
		real := float64(p.lines) / d.Seconds()
		fmt.Printf("%-12s %12.0f %12d %10v %12.0f\n", p.name, p.rate, p.lines, d.Round(100*time.Millisecond), real)
		switch p.name {
		case "normal":
			normal = real
		case "spike":
			spike, spikeDur = real, d
		}
	}
	if normal > 0 && spike > 0 {
		fmt.Printf("spike / normal = %.2fx sostenido durante %v\n", spike/normal, spikeDur.Round(100*time.Millisecond))
	}
}

// timeline escribe una fila por segundo con lo publicado en ese segundo.
func timeline(path string, t0 time.Time, s *schedule, published *atomic.Uint64) (stop func()) {
	f, err := os.Create(path)
	if err != nil {
		log.Printf("línea de tiempo: %v", err)
		return func() {}
	}
	w := bufio.NewWriter(f)
	fmt.Fprintln(w, "t_s,phase,target_rate,published_in_second,published_total")
	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Go(func() {
		tick := time.NewTicker(time.Second)
		defer tick.Stop()
		var last uint64
		for {
			select {
			case <-done:
				return
			case now := <-tick.C:
				n := published.Load()
				p := s.current()
				fmt.Fprintf(w, "%.0f,%s,%.0f,%d,%d\n", now.Sub(t0).Seconds(), p.name, p.rate, n-last, n)
				last = n
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
