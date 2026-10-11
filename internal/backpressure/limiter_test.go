package backpressure

import (
	"context"
	"testing"
	"time"
)

type clock struct{ t time.Time }

func (c *clock) now() time.Time { return c.t }

func newTestLimiter() (*Limiter, *clock) {
	c := &clock{t: time.Unix(1_700_000_000, 0)}
	l := NewLimiter(LimiterConfig{
		Initial: 20, Min: 2, Max: 1000, Increase: 3, Decrease: 0.75,
		Window: time.Second, LatencyFactor: 4, Now: c.now,
		Sleep: func(_ context.Context, d time.Duration) error { c.t = c.t.Add(d); return nil },
	})
	return l, c
}

// window simula una ventana de 1 s con n llamadas de latencia lat y fails fallos.
func window(l *Limiter, c *clock, n, fails int, lat time.Duration) {
	start := c.t
	for i := range n {
		c.t = start.Add(time.Duration(i+1) * time.Second / time.Duration(n)) // la última cae justo en 1 s
		l.Observe(c.t.Add(-lat/100), lat, i < fails)                         // salió hace poco: dentro de la ventana
	}
}

func TestIncreasesWhileHealthyAndUsed(t *testing.T) {
	l, c := newTestLimiter()
	window(l, c, 20, 0, 20*time.Millisecond)
	if r := l.Rate(); r != 23 {
		t.Fatalf("tasa tras ventana sana y aprovechada = %v, se esperaba 23", r)
	}
	// Poca demanda (5 llamadas con tasa 23): sin evidencia, no crece.
	window(l, c, 5, 0, 20*time.Millisecond)
	if r := l.Rate(); r != 23 {
		t.Fatalf("con la tasa sin usar no debe crecer: %v", r)
	}
}

func TestDecreasesOnFailureOrLatency(t *testing.T) {
	l, c := newTestLimiter()
	window(l, c, 20, 0, 20*time.Millisecond) // base = 20 ms, tasa 23
	window(l, c, 23, 1, 20*time.Millisecond) // un 503
	if r := l.Rate(); r != 23*0.75 {
		t.Fatalf("tras un fallo la tasa debe bajar a 17.25: %v", r)
	}
	r0 := l.Rate()
	window(l, c, 17, 0, 300*time.Millisecond) // sin fallos pero 15× más lento
	if r := l.Rate(); r != r0*0.75 {
		t.Fatalf("latencia > 4× base debe bajar la tasa: %v", r)
	}
	st := l.Stats()
	if st.DecreasesFail != 1 || st.DecreasesSlow != 1 || st.BaselineMs != 20 {
		t.Fatalf("stats: %+v", st)
	}
}

// Los fallos de llamadas que salieron antes de un recorte no provocan un
// segundo recorte: reflejan la tasa vieja.
func TestIgnoresResultsFromBeforeTheLastCut(t *testing.T) {
	l, c := newTestLimiter()
	window(l, c, 20, 0, 20*time.Millisecond) // tasa 23
	issuedBefore := c.t
	window(l, c, 23, 1, 20*time.Millisecond) // recorte → 17.25
	r := l.Rate()
	// Llegan fallos de llamadas emitidas antes del recorte, durante más de una ventana.
	for i := range 40 {
		c.t = c.t.Add(50 * time.Millisecond)
		l.Observe(issuedBefore, 300*time.Millisecond, i%2 == 0)
	}
	if got := l.Rate(); got != r {
		t.Fatalf("resultados viejos recortaron otra vez: %v → %v", r, got)
	}
	if l.Stats().Stale != 40 {
		t.Fatalf("stale=%d, se esperaban 40", l.Stats().Stale)
	}
}

func TestRateStaysWithinBounds(t *testing.T) {
	l, c := newTestLimiter()
	for range 50 {
		window(l, c, 10, 10, time.Second)
	}
	if r := l.Rate(); r != 2 {
		t.Fatalf("la tasa no debe bajar del mínimo: %v", r)
	}
	l.Backoff()
	if r := l.Rate(); r != 2 {
		t.Fatalf("Backoff no debe bajar del mínimo: %v", r)
	}
}

// Wait espacia las llamadas a 1/tasa contra el reloj y no acumula ráfagas
// tras un periodo sin llamadas.
func TestWaitPacesWithoutBursts(t *testing.T) {
	l, c := newTestLimiter() // 20/s = 50 ms
	start := c.t
	for range 10 {
		_ = l.Wait(context.Background())
	}
	if got := c.t.Sub(start); got != 450*time.Millisecond {
		t.Fatalf("10 llamadas a 20/s: %v, se esperaban 450ms", got)
	}
	c.t = c.t.Add(10 * time.Second) // inactividad
	before := c.t
	_ = l.Wait(context.Background())
	_ = l.Wait(context.Background())
	if got := c.t.Sub(before); got != 50*time.Millisecond {
		t.Fatalf("tras inactividad la 2ª llamada debe esperar 50ms, esperó %v", got)
	}
}
