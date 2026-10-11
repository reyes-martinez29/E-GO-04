package backpressure

import (
	"context"
	"sync"
	"time"
)

// Limiter decide qué tan seguido se llama al servicio externo: es la forma
// explícita de "degradar la frecuencia de llamadas" en vez de llamarlo sin
// control. Es un AIMD (como el control de congestión de TCP) sobre la tasa de
// llamadas por segundo, y NO necesita conocer los umbrales del servicio:
//
//   - Cada Window se evalúan las llamadas que terminaron en esa ventana.
//   - Ventana "mala" (algún fallo, o latencia media > LatencyFactor × la
//     latencia base observada) → tasa × Decrease.
//   - Ventana "sana" y aprovechada (se usó al menos el 80 % de la tasa) →
//     tasa + Increase.
//   - Cuando el breaker abre → tasa × 0.5 (Backoff).
//   - Los resultados de llamadas que salieron ANTES del último recorte se
//     ignoran: reflejan la tasa vieja, y contarlos recortaría dos veces por la
//     misma sobrecarga (se observó en la primera corrida: 62 → 46 → 35).
//
// La profundidad del buffer NO sube la tasa: tener más trabajo pendiente no
// hace que el servicio aguante más. Lo que se atrasa es la confirmación de
// inventario; no se le traslada la presión al servicio.
//
// El ritmo se programa contra el reloj (cada llamada reserva el siguiente
// hueco de 1/tasa), sin cubeta de tokens: después de un rato sin llamadas no
// sale una ráfaga.
type Limiter struct {
	mu  sync.Mutex
	cfg LimiterConfig

	rate float64
	next time.Time

	winStart time.Time
	winN     int
	winFail  int
	winLat   time.Duration
	baseline time.Duration // latencia media más baja vista en una ventana sana
	lastCut  time.Time     // último recorte (Decrease o Backoff)

	st LimiterStats
}

type LimiterConfig struct {
	Initial, Min, Max float64 // llamadas/s
	Increase          float64 // +llamadas/s por ventana sana
	Decrease          float64 // factor multiplicativo en ventana mala (0 < x < 1)
	Window            time.Duration
	LatencyFactor     float64 // "lento" = latencia media > factor × base
	Now               func() time.Time
	Sleep             func(context.Context, time.Duration) error
}

type LimiterStats struct {
	Increases     int64   `json:"increases"`
	Decreases     int64   `json:"decreases"`
	DecreasesSlow int64   `json:"decreases_by_latency"`
	DecreasesFail int64   `json:"decreases_by_failures"`
	Backoffs      int64   `json:"backoffs_by_breaker"`
	Stale         int64   `json:"stale_results_ignored"`
	MinRate       float64 `json:"min_rate"`
	MaxRate       float64 `json:"max_rate"`
	BaselineMs    float64 `json:"baseline_latency_ms"`
}

func NewLimiter(cfg LimiterConfig) *Limiter {
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.Sleep == nil {
		cfg.Sleep = sleepCtx
	}
	now := cfg.Now()
	return &Limiter{cfg: cfg, rate: cfg.Initial, next: now, winStart: now,
		st: LimiterStats{MinRate: cfg.Initial, MaxRate: cfg.Initial}}
}

// Wait espera el turno de la siguiente llamada.
func (l *Limiter) Wait(ctx context.Context) error {
	l.mu.Lock()
	now := l.cfg.Now()
	t := l.next
	if t.Before(now) {
		t = now
	}
	l.next = t.Add(time.Duration(float64(time.Second) / l.rate))
	l.mu.Unlock()
	if d := t.Sub(now); d > 0 {
		return l.cfg.Sleep(ctx, d)
	}
	return nil
}

// Observe registra el resultado de una llamada que salió en start.
func (l *Limiter) Observe(start time.Time, lat time.Duration, failed bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if start.Before(l.lastCut) {
		l.st.Stale++
		return
	}
	l.winN++
	l.winLat += lat
	if failed {
		l.winFail++
	}
	now := l.cfg.Now()
	if el := now.Sub(l.winStart); el >= l.cfg.Window {
		l.evaluate(now, el)
		l.winStart, l.winN, l.winFail, l.winLat = now, 0, 0, 0
	}
}

func (l *Limiter) evaluate(now time.Time, elapsed time.Duration) {
	mean := l.winLat / time.Duration(l.winN)
	slow := l.baseline > 0 && float64(mean) > l.cfg.LatencyFactor*float64(l.baseline)
	switch {
	case l.winFail > 0 || slow:
		l.st.Decreases++
		if l.winFail > 0 {
			l.st.DecreasesFail++
		} else {
			l.st.DecreasesSlow++
		}
		l.setRate(l.rate * l.cfg.Decrease)
		l.lastCut = now
	default:
		if l.winN >= 5 && (l.baseline == 0 || mean < l.baseline) {
			l.baseline = mean
		}
		// Solo crecer si la tasa actual se está usando: con poca demanda no
		// hay evidencia de que el servicio aguante más.
		if float64(l.winN) >= 0.8*l.rate*elapsed.Seconds() {
			l.st.Increases++
			l.setRate(l.rate + l.cfg.Increase)
		}
	}
}

// Backoff reduce la tasa a la mitad (lo llama el consumidor cuando el breaker abre).
func (l *Limiter) Backoff() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.st.Backoffs++
	l.setRate(l.rate * 0.5)
	l.lastCut = l.cfg.Now()
}

func (l *Limiter) setRate(r float64) {
	l.rate = min(max(r, l.cfg.Min), l.cfg.Max)
	l.st.MinRate = min(l.st.MinRate, l.rate)
	l.st.MaxRate = max(l.st.MaxRate, l.rate)
}

func (l *Limiter) Rate() float64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.rate
}

func (l *Limiter) Stats() LimiterStats {
	l.mu.Lock()
	defer l.mu.Unlock()
	st := l.st
	st.BaselineMs = float64(l.baseline) / float64(time.Millisecond)
	return st
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
