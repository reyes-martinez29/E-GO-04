// Package breaker implementa un circuit breaker de tres estados para envolver
// las llamadas a un servicio externo (en este proyecto, POST /check-inventory).
//
//	CLOSED ──(FailureThreshold fallos consecutivos)──▶ OPEN
//	OPEN ──(pasa OpenTimeout)──▶ HALF-OPEN
//	HALF-OPEN ──(HalfOpenProbes pruebas seguidas OK)──▶ CLOSED
//	HALF-OPEN ──(falla una prueba)──▶ OPEN (cooldown completo de nuevo)
//
// En OPEN no sale ninguna llamada: Allow devuelve *OpenError con el tiempo que
// falta para half-open, y quien llama decide qué hacer con su trabajo (aquí:
// esperar y conservarlo; nunca descartarlo). En HALF-OPEN salen como máximo
// HalfOpenProbes llamadas de prueba a la vez; las demás se rechazan igual que
// en OPEN.
//
// Cada cambio de estado incrementa una generación. Una llamada que empezó en
// una generación anterior (por ejemplo, salió con el breaker cerrado y
// regresó cuando ya estaba abierto) no cambia el estado al terminar: se cuenta
// en las estadísticas, pero no decide sobre un estado que no la autorizó.
package breaker

import (
	"fmt"
	"sync"
	"time"
)

type State int32

const (
	Closed State = iota
	Open
	HalfOpen
)

func (s State) String() string {
	switch s {
	case Closed:
		return "closed"
	case Open:
		return "open"
	case HalfOpen:
		return "half-open"
	}
	return fmt.Sprintf("State(%d)", int32(s))
}

type Config struct {
	FailureThreshold int           // fallos consecutivos que abren el breaker
	OpenTimeout      time.Duration // cooldown: cuánto dura OPEN antes de pasar a HALF-OPEN
	HalfOpenProbes   int           // pruebas en HALF-OPEN; todas deben salir bien para cerrar
	Now              func() time.Time
	// OnTransition se llama con el lock tomado, en el orden exacto de los
	// cambios de estado. Debe ser rápido y no llamar de vuelta al Breaker.
	OnTransition func(Transition)
}

type Transition struct {
	At     time.Time `json:"at"`
	From   State     `json:"-"`
	To     State     `json:"-"`
	Reason string    `json:"reason"`
	// Nombres legibles para el reporte JSON.
	FromName string `json:"from"`
	ToName   string `json:"to"`
}

// OpenError indica que el breaker no permite la llamada ahora.
type OpenError struct {
	State      State
	RetryAfter time.Duration
}

func (e *OpenError) Error() string {
	return fmt.Sprintf("breaker %s: reintentar en %v", e.State, e.RetryAfter)
}

type Stats struct {
	Opens               uint64        `json:"opens"`                  // veces que pasó a OPEN (desde CLOSED o HALF-OPEN)
	OpensFromClosed     uint64        `json:"opens_from_closed"`      // por fallos consecutivos
	ReopensFromHalfOpen uint64        `json:"reopens_from_half_open"` // una prueba falló
	ClosesFromHalfOpen  uint64        `json:"closes_from_half_open"`  // recuperación confirmada
	OpenTime            time.Duration `json:"-"`                      // tiempo total en OPEN
	HalfOpenTime        time.Duration `json:"-"`                      // tiempo total en HALF-OPEN
	OpenSeconds         float64       `json:"open_seconds"`
	HalfOpenSeconds     float64       `json:"half_open_seconds"`
	Rejected            uint64        `json:"rejected_calls"`  // llamadas que el breaker no dejó salir
	Successes           uint64        `json:"successes"`       // resultados reportados con Done(true)
	Failures            uint64        `json:"failures"`        // resultados reportados con Done(false)
	StaleResults        uint64        `json:"stale_results"`   // resultados de una generación anterior
	MaxConsecutiveFails int           `json:"max_consecutive"` // racha máxima de fallos vista en CLOSED
}

type Breaker struct {
	mu  sync.Mutex
	cfg Config

	state       State
	gen         uint64
	consecutive int       // fallos consecutivos en CLOSED
	since       time.Time // inicio del estado actual
	openUntil   time.Time // fin del cooldown si state == Open
	probesOut   int       // pruebas en vuelo en HALF-OPEN
	probesOK    int       // pruebas exitosas en HALF-OPEN

	st Stats
}

func New(cfg Config) (*Breaker, error) {
	if cfg.FailureThreshold < 1 || cfg.HalfOpenProbes < 1 || cfg.OpenTimeout <= 0 {
		return nil, fmt.Errorf("breaker: configuración inválida %+v", cfg)
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	return &Breaker{cfg: cfg, since: cfg.Now()}, nil
}

// Ticket autoriza una llamada. Done debe llamarse exactamente una vez.
type Ticket struct {
	b     *Breaker
	gen   uint64
	probe bool
}

// Allow pide permiso para una llamada. Si el breaker está OPEN (o HALF-OPEN
// con todas las pruebas ya en vuelo) devuelve *OpenError.
func (b *Breaker) Allow() (Ticket, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	now := b.cfg.Now()
	b.refresh(now)
	switch b.state {
	case Open:
		b.st.Rejected++
		return Ticket{}, &OpenError{State: Open, RetryAfter: b.openUntil.Sub(now)}
	case HalfOpen:
		if b.probesOut+b.probesOK >= b.cfg.HalfOpenProbes {
			b.st.Rejected++
			// Las pruebas en vuelo deciden pronto; reintentar en una fracción del cooldown.
			return Ticket{}, &OpenError{State: HalfOpen, RetryAfter: max(b.cfg.OpenTimeout/20, time.Millisecond)}
		}
		b.probesOut++
		return Ticket{b: b, gen: b.gen, probe: true}, nil
	}
	return Ticket{b: b, gen: b.gen}, nil
}

// Done reporta el resultado de la llamada autorizada por el ticket.
func (t Ticket) Done(success bool) {
	b := t.b
	if b == nil {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	now := b.cfg.Now()
	if success {
		b.st.Successes++
	} else {
		b.st.Failures++
	}
	b.refresh(now)
	if t.gen != b.gen {
		b.st.StaleResults++
		return
	}
	switch b.state {
	case Closed:
		if success {
			b.consecutive = 0
			return
		}
		b.consecutive++
		b.st.MaxConsecutiveFails = max(b.st.MaxConsecutiveFails, b.consecutive)
		if b.consecutive >= b.cfg.FailureThreshold {
			b.st.OpensFromClosed++
			b.toOpen(now, fmt.Sprintf("%d fallos consecutivos", b.consecutive))
		}
	case HalfOpen:
		b.probesOut--
		if !success {
			b.st.ReopensFromHalfOpen++
			b.toOpen(now, fmt.Sprintf("prueba %d/%d en half-open falló", b.probesOK+1, b.cfg.HalfOpenProbes))
			return
		}
		b.probesOK++
		if b.probesOK >= b.cfg.HalfOpenProbes {
			b.st.ClosesFromHalfOpen++
			b.transition(now, Closed, fmt.Sprintf("%d/%d pruebas OK en half-open", b.probesOK, b.cfg.HalfOpenProbes))
		}
	}
}

// refresh aplica la transición OPEN → HALF-OPEN si el cooldown ya venció. La
// transición se fecha en openUntil (no en el momento en que alguien la
// observa), así el tiempo en OPEN es exactamente el cooldown.
func (b *Breaker) refresh(now time.Time) {
	if b.state == Open && !now.Before(b.openUntil) {
		b.transition(b.openUntil, HalfOpen, fmt.Sprintf("cooldown de %v cumplido", b.cfg.OpenTimeout))
	}
}

func (b *Breaker) toOpen(now time.Time, reason string) {
	b.st.Opens++
	b.openUntil = now.Add(b.cfg.OpenTimeout)
	b.transition(now, Open, reason)
}

func (b *Breaker) transition(at time.Time, to State, reason string) {
	from := b.state
	switch from {
	case Open:
		b.st.OpenTime += at.Sub(b.since)
	case HalfOpen:
		b.st.HalfOpenTime += at.Sub(b.since)
	}
	b.state = to
	b.since = at
	b.gen++
	b.consecutive = 0
	b.probesOut = 0
	b.probesOK = 0
	if b.cfg.OnTransition != nil {
		b.cfg.OnTransition(Transition{At: at, From: from, To: to, Reason: reason, FromName: from.String(), ToName: to.String()})
	}
}

// State devuelve el estado actual (aplicando el vencimiento del cooldown).
func (b *Breaker) State() State {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.refresh(b.cfg.Now())
	return b.state
}

// Stats incluye el tiempo parcial del estado actual si es OPEN o HALF-OPEN.
func (b *Breaker) Stats() Stats {
	b.mu.Lock()
	defer b.mu.Unlock()
	now := b.cfg.Now()
	b.refresh(now)
	st := b.st
	switch b.state {
	case Open:
		st.OpenTime += now.Sub(b.since)
	case HalfOpen:
		st.HalfOpenTime += now.Sub(b.since)
	}
	st.OpenSeconds = st.OpenTime.Seconds()
	st.HalfOpenSeconds = st.HalfOpenTime.Seconds()
	return st
}
