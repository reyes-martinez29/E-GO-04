// Package backpressure contiene el buffer de espera de las verificaciones de
// inventario, la señal que mide cuánto trabajo se está acumulando, y el
// limitador adaptativo que decide qué tan seguido se llama al servicio.
//
// Definición operativa de "no perder datos" (ver decisions.md §1): un evento
// inventory_reserved que ya pasó la dedup NUNCA se descarta. Mientras espera
// su verificación vive en este Queue, en tres niveles explícitos según la
// profundidad (la señal de backpressure):
//
//	MEMORY   profundidad <= MemCap        → anillo en memoria (RAM acotada)
//	SPILLING memoria llena                → los nuevos van a un archivo en disco
//	BLOCKED  memoria y disco llenos       → Push bloquea: los shards dejan de
//	                                        aplicar, el consumidor deja de pedir
//	                                        al broker, y los mensajes esperan en
//	                                        JetStream (disco del broker)
//
// El orden es FIFO global: mientras haya algo en disco, lo nuevo también va a
// disco (si no, lo nuevo en memoria se adelantaría a lo viejo en disco).
package backpressure

import (
	"fmt"
	"sync"
	"time"
)

// Item es una verificación de inventario pendiente.
type Item struct {
	EventID  [16]byte
	OrderID  [16]byte
	Country  uint8
	PubTs    int64 // UnixNano en que el productor publicó el evento
	Enqueued int64 // UnixNano en que entró al buffer
}

type Level int32

const (
	LevelMemory Level = iota
	LevelSpilling
	LevelBlocked
	numLevels
)

func (l Level) String() string {
	return [...]string{"memory", "spilling", "blocked"}[l]
}

type Config struct {
	MemCap    int    // ítems en memoria
	SpillPath string // archivo de desborde ("" = sin disco: al llenarse la memoria, Push bloquea)
	SpillCap  int    // ítems máximos en disco
	Now       func() time.Time
	// OnLevel se llama (con el lock tomado) en cada cambio de nivel.
	OnLevel func(from, to Level, s Snapshot)
}

type Snapshot struct {
	Mem      int           `json:"mem"`
	Disk     int           `json:"disk"`
	Depth    int           `json:"depth"`
	Level    Level         `json:"-"`
	OldestMs float64       `json:"oldest_wait_ms"` // antigüedad del ítem más viejo
	Oldest   time.Duration `json:"-"`
}

type Stats struct {
	Pushed        int64              `json:"pushed"`
	Popped        int64              `json:"popped"`
	Spilled       int64              `json:"spilled_to_disk"` // ítems que pasaron por disco
	MaxDepth      int                `json:"max_depth"`
	MaxMem        int                `json:"max_mem"`
	MaxDisk       int                `json:"max_disk"`
	BlockedPushes int64              `json:"blocked_pushes"` // Push que tuvieron que esperar
	LevelChanges  int64              `json:"level_changes"`
	TimeInLevel   map[string]float64 `json:"seconds_in_level"`
}

type Queue struct {
	mu       sync.Mutex
	notEmpty *sync.Cond
	notFull  *sync.Cond
	cfg      Config

	ring       []Item
	head, size int
	spill      *spillFile
	closed     bool

	level      Level
	levelSince time.Time
	blocked    int // Push esperando ahora mismo

	st          Stats
	timeInLevel [numLevels]time.Duration
}

func NewQueue(cfg Config) (*Queue, error) {
	if cfg.MemCap < 1 {
		return nil, fmt.Errorf("backpressure: MemCap debe ser >= 1")
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	q := &Queue{cfg: cfg, ring: make([]Item, cfg.MemCap), levelSince: cfg.Now()}
	if cfg.SpillPath != "" && cfg.SpillCap > 0 {
		sf, err := openSpill(cfg.SpillPath)
		if err != nil {
			return nil, err
		}
		q.spill = sf
	}
	q.notEmpty = sync.NewCond(&q.mu)
	q.notFull = sync.NewCond(&q.mu)
	return q, nil
}

// Push agrega un ítem. Nunca descarta: si memoria y disco están llenos,
// bloquea hasta que haya espacio. Devuelve error solo si el disco falla.
func (q *Queue) Push(it Item) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return fmt.Errorf("backpressure: Push sobre una cola cerrada")
	}
	waited := false
	for {
		diskLen := q.diskLen()
		switch {
		case diskLen == 0 && q.size < len(q.ring):
			q.ring[(q.head+q.size)%len(q.ring)] = it
			q.size++
		case q.spill != nil && diskLen < q.cfg.SpillCap:
			if err := q.spill.append(it); err != nil {
				return err
			}
			q.st.Spilled++
		default:
			if !waited {
				waited = true
				q.st.BlockedPushes++
			}
			q.blocked++
			q.updateLevel()
			q.notFull.Wait()
			q.blocked--
			continue
		}
		break
	}
	q.st.Pushed++
	q.updateLevel()
	d := q.size + q.diskLen()
	q.st.MaxDepth = max(q.st.MaxDepth, d)
	q.st.MaxMem = max(q.st.MaxMem, q.size)
	q.st.MaxDisk = max(q.st.MaxDisk, q.diskLen())
	q.notEmpty.Signal()
	return nil
}

// Pop saca el ítem más viejo. Bloquea mientras la cola esté vacía; devuelve
// ok=false cuando la cola está cerrada y vacía.
func (q *Queue) Pop() (Item, bool, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for q.size == 0 {
		if q.diskLen() > 0 {
			if err := q.refill(); err != nil {
				return Item{}, false, err
			}
			continue
		}
		if q.closed {
			return Item{}, false, nil
		}
		q.notEmpty.Wait()
	}
	it := q.ring[q.head]
	q.head = (q.head + 1) % len(q.ring)
	q.size--
	q.st.Popped++
	q.updateLevel()
	q.notFull.Broadcast()
	return it, true, nil
}

// refill pasa a memoria el bloque más viejo del disco (memoria vacía).
func (q *Queue) refill() error {
	n := min(q.diskLen(), len(q.ring))
	q.head = 0
	got, err := q.spill.read(q.ring[:n])
	if err != nil {
		return err
	}
	q.size = got
	q.notFull.Broadcast() // se liberó espacio en disco
	return nil
}

// Close indica que no habrá más Push. Pop sigue entregando lo que quede.
func (q *Queue) Close() {
	q.mu.Lock()
	q.closed = true
	q.mu.Unlock()
	q.notEmpty.Broadcast()
}

// Remove borra el archivo de desborde. Llamar solo cuando la cola está vacía.
func (q *Queue) Remove() error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.spill == nil {
		return nil
	}
	return q.spill.remove()
}

func (q *Queue) diskLen() int {
	if q.spill == nil {
		return 0
	}
	return q.spill.len()
}

// updateLevel recalcula el nivel de backpressure a partir de la ocupación.
func (q *Queue) updateLevel() {
	var lv Level
	switch {
	case q.blocked > 0:
		lv = LevelBlocked
	case q.diskLen() > 0:
		lv = LevelSpilling
	default:
		lv = LevelMemory
	}
	if lv == q.level {
		return
	}
	now := q.cfg.Now()
	q.timeInLevel[q.level] += now.Sub(q.levelSince)
	from := q.level
	q.level, q.levelSince = lv, now
	q.st.LevelChanges++
	if q.cfg.OnLevel != nil {
		q.cfg.OnLevel(from, lv, q.snapshot(now))
	}
}

func (q *Queue) snapshot(now time.Time) Snapshot {
	s := Snapshot{Mem: q.size, Disk: q.diskLen(), Level: q.level}
	s.Depth = s.Mem + s.Disk
	if q.size > 0 { // FIFO: la cabeza de memoria es el ítem más viejo de toda la cola
		s.Oldest = now.Sub(time.Unix(0, q.ring[q.head].Enqueued))
	} else if s.Disk > 0 {
		s.Oldest = now.Sub(time.Unix(0, q.spill.oldestEnqueued()))
	}
	s.OldestMs = float64(s.Oldest) / float64(time.Millisecond)
	return s
}

// Snapshot es la señal de backpressure en este instante.
func (q *Queue) Snapshot() Snapshot {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.snapshot(q.cfg.Now())
}

func (q *Queue) Stats() Stats {
	q.mu.Lock()
	defer q.mu.Unlock()
	st := q.st
	tl := q.timeInLevel
	tl[q.level] += q.cfg.Now().Sub(q.levelSince)
	st.TimeInLevel = map[string]float64{}
	for l := range numLevels {
		st.TimeInLevel[l.String()] = tl[l].Seconds()
	}
	return st
}
