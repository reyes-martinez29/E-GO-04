// Package dedup implementa la ventana de deduplicación por event_id.
//
// La ventana es un anillo de G "generaciones", cada una un map que cubre
// Window/G de tiempo de procesamiento. Insertar va a la generación actual;
// buscar recorre las G generaciones (O(G), G constante => O(1)). Expirar es
// descartar la generación más vieja completa, en vez de hacer delete llave por
// llave: los maps de Go no devuelven memoria al hacer delete, así que un map
// único con expiración por llave crecería sin control (E-GO03, decisions.md §2).
//
// Garantía: una llave vive como mínimo (G-1)/G * Window. Con Window=5m y G=10
// eso son 4.5 min, por encima de los 3 min máximos de reintento del dataset.
//
// Window NO es thread-safe: el consumidor le da a cada shard su propia ventana
// y enruta cada event_id siempre al mismo shard.
package dedup

import (
	"fmt"
	"time"
)

type Key = [16]byte

type Config struct {
	Window       time.Duration    // retención nominal total (p. ej. 5m)
	Generations  int              // número de generaciones del anillo (p. ej. 10)
	MinRetention time.Duration    // retención mínima que la ventana debe garantizar (p. ej. 3m)
	MaxPerGen    int              // tope de llaves por generación; 0 = sin tope
	Now          func() time.Time // reloj inyectable (tests); nil = time.Now
}

type generation struct {
	m     map[Key]uint32 // event_id -> timestamp del evento original (segundos Unix)
	start time.Time
}

type Window struct {
	cfg       Config
	genDur    time.Duration
	gens      []generation
	cur       int
	size      int
	rotations uint64
	premature uint64 // llaves expulsadas antes de MinRetention (solo por MaxPerGen)
}

func New(cfg Config) (*Window, error) {
	if cfg.Generations < 2 {
		return nil, fmt.Errorf("dedup: se necesitan al menos 2 generaciones")
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	genDur := cfg.Window / time.Duration(cfg.Generations)
	if guaranteed := genDur * time.Duration(cfg.Generations-1); guaranteed < cfg.MinRetention {
		return nil, fmt.Errorf("dedup: la ventana garantiza %v de retención, menos que el mínimo exigido %v", guaranteed, cfg.MinRetention)
	}
	w := &Window{cfg: cfg, genDur: genDur, gens: make([]generation, cfg.Generations)}
	now := cfg.Now()
	for i := range w.gens {
		w.gens[i] = generation{m: make(map[Key]uint32), start: now}
	}
	return w, nil
}

// SeenOrAdd devuelve (true, tsOriginal) si key ya estaba en la ventana, es
// decir, si el evento es un duplicado. Si no estaba, la registra con ts y
// devuelve false.
func (w *Window) SeenOrAdd(key Key, ts int64) (dup bool, origTS int64) {
	now := w.cfg.Now()
	w.rotateByTime(now)
	n := len(w.gens)
	for i := 0; i < n; i++ { // de la más nueva a la más vieja: el reintento suele estar cerca
		if o, ok := w.gens[(w.cur-i+n)%n].m[key]; ok {
			return true, int64(o)
		}
	}
	if w.cfg.MaxPerGen > 0 && len(w.gens[w.cur].m) >= w.cfg.MaxPerGen {
		w.rotate(now) // rotación forzada por memoria: puede expulsar antes de tiempo
	}
	w.gens[w.cur].m[key] = uint32(ts)
	w.size++
	return false, 0
}

func (w *Window) rotateByTime(now time.Time) {
	elapsed := now.Sub(w.gens[w.cur].start)
	if elapsed < w.genDur {
		return
	}
	if elapsed >= w.cfg.Window { // inactividad larga: todo expiró, no rotar una a una
		for i := range w.gens {
			w.gens[i] = generation{m: make(map[Key]uint32), start: now}
		}
		w.size = 0
		w.rotations += uint64(len(w.gens))
		return
	}
	for now.Sub(w.gens[w.cur].start) >= w.genDur {
		w.rotate(w.gens[w.cur].start.Add(w.genDur))
	}
}

// rotate avanza a la siguiente generación, descartando la más vieja.
func (w *Window) rotate(start time.Time) {
	n := len(w.gens)
	next := (w.cur + 1) % n
	old := w.gens[next]
	if len(old.m) > 0 {
		// La llave más joven de la generación descartada se insertó antes de
		// que empezara la generación que le sigue.
		youngestAge := w.cfg.Now().Sub(w.gens[(next+1)%n].start)
		if youngestAge < w.cfg.MinRetention {
			w.premature += uint64(len(old.m))
		}
	}
	w.size -= len(old.m)
	w.gens[next] = generation{m: make(map[Key]uint32, len(w.gens[w.cur].m)), start: start}
	w.cur = next
	w.rotations++
}

type Stats struct {
	Len       int    `json:"len"`
	Rotations uint64 `json:"rotations"`
	Premature uint64 `json:"premature_evictions"`
}

func (w *Window) Stats() Stats {
	return Stats{Len: w.size, Rotations: w.rotations, Premature: w.premature}
}

// GuaranteedRetention es el tiempo mínimo que cualquier llave permanece en la
// ventana (sin rotaciones forzadas por MaxPerGen).
func (w *Window) GuaranteedRetention() time.Duration {
	return w.genDur * time.Duration(len(w.gens)-1)
}
