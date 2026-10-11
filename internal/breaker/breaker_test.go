package breaker

import (
	"errors"
	"sync"
	"testing"
	"time"
)

type fakeClock struct{ t time.Time }

func (c *fakeClock) Now() time.Time          { return c.t }
func (c *fakeClock) Advance(d time.Duration) { c.t = c.t.Add(d) }

func newTest(t *testing.T) (*Breaker, *fakeClock, *[]Transition) {
	t.Helper()
	clk := &fakeClock{t: time.Unix(1_700_000_000, 0)}
	var trs []Transition
	b, err := New(Config{
		FailureThreshold: 5,
		OpenTimeout:      2 * time.Second,
		HalfOpenProbes:   3,
		Now:              clk.Now,
		OnTransition:     func(tr Transition) { trs = append(trs, tr) },
	})
	if err != nil {
		t.Fatal(err)
	}
	return b, clk, &trs
}

func call(t *testing.T, b *Breaker, success bool) {
	t.Helper()
	tk, err := b.Allow()
	if err != nil {
		t.Fatalf("Allow rechazó con el breaker en %v: %v", b.State(), err)
	}
	tk.Done(success)
}

func fail(t *testing.T, b *Breaker, n int) {
	t.Helper()
	for range n {
		call(t, b, false)
	}
}

func TestOpensOnlyAfterConsecutiveFailures(t *testing.T) {
	b, _, _ := newTest(t)
	// 4 fallos, un éxito, 4 fallos: nunca 5 seguidos.
	fail(t, b, 4)
	call(t, b, true)
	fail(t, b, 4)
	if s := b.State(); s != Closed {
		t.Fatalf("fallos no consecutivos abrieron el breaker: %v", s)
	}
	fail(t, b, 1)
	if s := b.State(); s != Open {
		t.Fatalf("5 fallos consecutivos: estado %v, se esperaba open", s)
	}
}

func TestOpenRejectsForTheWholeCooldown(t *testing.T) {
	b, clk, _ := newTest(t)
	fail(t, b, 5)
	start := clk.Now()
	for elapsed := time.Duration(0); elapsed < 2*time.Second; elapsed += 250 * time.Millisecond {
		clk.t = start.Add(elapsed)
		_, err := b.Allow()
		var oe *OpenError
		if !errors.As(err, &oe) || oe.State != Open {
			t.Fatalf("a %v del cooldown la llamada no fue rechazada: %v", elapsed, err)
		}
		if want := 2*time.Second - elapsed; oe.RetryAfter != want {
			t.Fatalf("RetryAfter=%v, se esperaba %v", oe.RetryAfter, want)
		}
	}
	clk.t = start.Add(2 * time.Second)
	if s := b.State(); s != HalfOpen {
		t.Fatalf("al cumplirse el cooldown: estado %v, se esperaba half-open", s)
	}
}

func TestHalfOpenClosesAfterAllProbesSucceed(t *testing.T) {
	b, clk, trs := newTest(t)
	fail(t, b, 5)
	clk.Advance(2 * time.Second)

	// Solo 3 pruebas pueden estar en vuelo; la cuarta se rechaza.
	t1, err1 := b.Allow()
	t2, err2 := b.Allow()
	t3, err3 := b.Allow()
	if err1 != nil || err2 != nil || err3 != nil {
		t.Fatalf("half-open debe permitir 3 pruebas: %v %v %v", err1, err2, err3)
	}
	var oe *OpenError
	if _, err := b.Allow(); !errors.As(err, &oe) || oe.State != HalfOpen {
		t.Fatalf("una 4ª llamada en half-open debe rechazarse, err=%v", err)
	}
	t1.Done(true)
	t2.Done(true)
	if s := b.State(); s != HalfOpen {
		t.Fatalf("con 2/3 pruebas OK debe seguir en half-open, está %v", s)
	}
	t3.Done(true)
	if s := b.State(); s != Closed {
		t.Fatalf("3/3 pruebas OK: estado %v, se esperaba closed", s)
	}

	want := []State{Open, HalfOpen, Closed}
	if len(*trs) != len(want) {
		t.Fatalf("transiciones: %+v", *trs)
	}
	for i, tr := range *trs {
		if tr.To != want[i] {
			t.Fatalf("transición %d: %v, se esperaba %v", i, tr.To, want[i])
		}
	}
	st := b.Stats()
	if st.Opens != 1 || st.ClosesFromHalfOpen != 1 || st.OpenTime != 2*time.Second {
		t.Fatalf("stats: %+v", st)
	}
}

func TestHalfOpenFailureReopensWithFullCooldown(t *testing.T) {
	b, clk, _ := newTest(t)
	fail(t, b, 5)
	clk.Advance(2 * time.Second)
	call(t, b, true)
	call(t, b, false) // la segunda prueba falla
	if s := b.State(); s != Open {
		t.Fatalf("prueba fallida en half-open: estado %v, se esperaba open", s)
	}
	clk.Advance(1999 * time.Millisecond)
	if s := b.State(); s != Open {
		t.Fatalf("el cooldown se reinicia completo tras reabrir; estado %v", s)
	}
	clk.Advance(time.Millisecond)
	if s := b.State(); s != HalfOpen {
		t.Fatalf("estado %v, se esperaba half-open", s)
	}
	st := b.Stats()
	if st.Opens != 2 || st.ReopensFromHalfOpen != 1 || st.OpenTime != 4*time.Second {
		t.Fatalf("stats: %+v", st)
	}
}

func TestStaleResultsDoNotChangeState(t *testing.T) {
	b, clk, _ := newTest(t)
	// Una llamada sale con el breaker cerrado y tarda.
	slow, err := b.Allow()
	if err != nil {
		t.Fatal(err)
	}
	fail(t, b, 5) // mientras tanto el breaker abre
	clk.Advance(2 * time.Second)
	if s := b.State(); s != HalfOpen {
		t.Fatalf("estado %v", s)
	}
	// La llamada lenta regresa con éxito: no debe contar como prueba de half-open.
	slow.Done(true)
	if s := b.State(); s != HalfOpen {
		t.Fatalf("un resultado viejo cambió el estado a %v", s)
	}
	if b.Stats().StaleResults != 1 {
		t.Fatal("el resultado viejo debe contarse como stale")
	}
}

func TestOpenTimeAccumulatesWhileOpen(t *testing.T) {
	b, clk, _ := newTest(t)
	fail(t, b, 5)
	clk.Advance(1500 * time.Millisecond)
	if got := b.Stats().OpenTime; got != 1500*time.Millisecond {
		t.Fatalf("tiempo abierto parcial=%v", got)
	}
	// Nadie llama durante 10 s: el tiempo en OPEN sigue siendo el cooldown, y
	// el resto se cuenta en HALF-OPEN.
	clk.Advance(10 * time.Second)
	st := b.Stats()
	if st.OpenTime != 2*time.Second || st.HalfOpenTime != 9500*time.Millisecond {
		t.Fatalf("open=%v half-open=%v", st.OpenTime, st.HalfOpenTime)
	}
}

// Con muchas goroutines reportando a la vez, el breaker nunca deja más de
// HalfOpenProbes pruebas en vuelo, y sus contadores cuadran. Se corre con -race.
func TestConcurrentUse(t *testing.T) {
	b, err := New(Config{FailureThreshold: 5, OpenTimeout: time.Millisecond, HalfOpenProbes: 3})
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	var mu sync.Mutex
	allowed := 0
	for g := range 16 {
		wg.Go(func() {
			for i := range 2000 {
				tk, err := b.Allow()
				if err != nil {
					continue
				}
				mu.Lock()
				allowed++
				mu.Unlock()
				tk.Done((i+g)%3 != 0)
			}
		})
	}
	wg.Wait()
	st := b.Stats()
	if int(st.Successes+st.Failures) != allowed {
		t.Fatalf("resultados=%d, llamadas permitidas=%d", st.Successes+st.Failures, allowed)
	}
	if allowed+int(st.Rejected) != 16*2000 {
		t.Fatalf("permitidas+rechazadas=%d", allowed+int(st.Rejected))
	}
}
