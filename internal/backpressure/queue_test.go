package backpressure

import (
	"encoding/binary"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func item(n uint64) Item {
	var it Item
	binary.LittleEndian.PutUint64(it.EventID[:], n)
	binary.LittleEndian.PutUint64(it.OrderID[8:], ^n)
	it.Country = uint8(n % 5)
	it.PubTs = int64(n) * 3
	it.Enqueued = int64(n)*7 + 1
	return it
}

func newQ(t *testing.T, memCap, spillCap int) (*Queue, *[]string) {
	t.Helper()
	var levels []string
	q, err := NewQueue(Config{
		MemCap: memCap, SpillCap: spillCap,
		SpillPath: filepath.Join(t.TempDir(), "spill.bin"),
		OnLevel:   func(_, to Level, _ Snapshot) { levels = append(levels, to.String()) },
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = q.Remove() })
	return q, &levels
}

func pop(t *testing.T, q *Queue) Item {
	t.Helper()
	it, ok, err := q.Pop()
	if err != nil || !ok {
		t.Fatalf("Pop: ok=%v err=%v", ok, err)
	}
	return it
}

// Lo que no cabe en memoria pasa por disco y sale en el mismo orden, con
// todos sus campos intactos.
func TestSpillPreservesFIFOAndContent(t *testing.T) {
	q, levels := newQ(t, 10, 1000)
	for i := range uint64(250) {
		if err := q.Push(item(i)); err != nil {
			t.Fatal(err)
		}
	}
	s := q.Snapshot()
	if s.Mem != 10 || s.Disk != 240 || s.Depth != 250 || s.Level != LevelSpilling {
		t.Fatalf("snapshot tras 250 Push: %+v", s)
	}
	// Mezclar Pop y Push: lo nuevo no puede adelantarse a lo que está en disco.
	for i := range uint64(100) {
		if got := pop(t, q); got != item(i) {
			t.Fatalf("Pop %d: %+v != %+v", i, got, item(i))
		}
	}
	for i := uint64(250); i < 300; i++ {
		if err := q.Push(item(i)); err != nil {
			t.Fatal(err)
		}
	}
	for i := uint64(100); i < 300; i++ {
		if got := pop(t, q); got != item(i) {
			t.Fatalf("Pop %d: %+v != %+v", i, got, item(i))
		}
	}
	st := q.Stats()
	if st.Pushed != 300 || st.Popped != 300 || st.MaxDepth != 250 || st.Spilled == 0 {
		t.Fatalf("stats: %+v", st)
	}
	if q.Snapshot().Level != LevelMemory {
		t.Fatal("al vaciarse debe volver a nivel memory")
	}
	if len(*levels) != 2 || (*levels)[0] != "spilling" || (*levels)[1] != "memory" {
		t.Fatalf("cambios de nivel: %v", *levels)
	}
}

// Con memoria y disco llenos Push bloquea (nivel blocked) en vez de
// descartar, y se libera en cuanto hay espacio.
func TestFullQueueBlocksInsteadOfDropping(t *testing.T) {
	q, levels := newQ(t, 4, 4)
	for i := range uint64(8) {
		if err := q.Push(item(i)); err != nil {
			t.Fatal(err)
		}
	}
	done := make(chan struct{})
	go func() {
		_ = q.Push(item(8)) // no cabe: debe bloquear
		close(done)
	}()
	deadline := time.Now().Add(2 * time.Second)
	for q.Snapshot().Level != LevelBlocked {
		if time.Now().After(deadline) {
			t.Fatal("Push con la cola llena no quedó bloqueado")
		}
		time.Sleep(time.Millisecond)
	}
	select {
	case <-done:
		t.Fatal("Push no debía completar con la cola llena")
	default:
	}
	// Vaciar la memoria provoca un refill desde disco y libera espacio.
	for i := range uint64(5) {
		if got := pop(t, q); got != item(i) {
			t.Fatalf("Pop %d fuera de orden", i)
		}
	}
	<-done
	for i := uint64(5); i < 9; i++ {
		if got := pop(t, q); got != item(i) {
			t.Fatalf("Pop %d fuera de orden: %+v", i, got)
		}
	}
	if st := q.Stats(); st.BlockedPushes != 1 || st.Pushed != 9 || st.Popped != 9 {
		t.Fatalf("stats: %+v", st)
	}
	found := false
	for _, l := range *levels {
		found = found || l == "blocked"
	}
	if !found {
		t.Fatalf("el nivel blocked no se registró: %v", *levels)
	}
}

// Regresión: al vaciarse el disco el archivo se trunca; un segundo episodio
// de desborde debe escribir desde el inicio del archivo (antes se escribía en
// el offset viejo y se leían ceros).
func TestSecondSpillEpisodeAfterTruncate(t *testing.T) {
	q, _ := newQ(t, 3, 100)
	n := uint64(0)
	for episode := range 3 {
		first := n
		for range 20 {
			_ = q.Push(item(n))
			n++
		}
		for i := first; i < n; i++ {
			if got := pop(t, q); got != item(i) {
				t.Fatalf("episodio %d, Pop %d: %+v", episode, i, got)
			}
		}
	}
}

func TestCloseDrainsThenEnds(t *testing.T) {
	q, _ := newQ(t, 2, 10)
	for i := range uint64(5) {
		_ = q.Push(item(i))
	}
	q.Close()
	for i := range uint64(5) {
		if got := pop(t, q); got != item(i) {
			t.Fatalf("Pop %d", i)
		}
	}
	if _, ok, _ := q.Pop(); ok {
		t.Fatal("cola cerrada y vacía debe devolver ok=false")
	}
}

// Muchos productores y consumidores a la vez, con memoria y disco chicos para
// forzar desborde y bloqueo: cada ítem sale exactamente una vez. Se corre
// con -race.
func TestConcurrentNoLossNoDup(t *testing.T) {
	q, _ := newQ(t, 64, 256)
	const producers, perProducer, consumers = 8, 5000, 4
	var pw sync.WaitGroup
	for p := range uint64(producers) {
		pw.Go(func() {
			for i := range uint64(perProducer) {
				if err := q.Push(item(p*perProducer + i)); err != nil {
					t.Error(err)
					return
				}
			}
		})
	}
	seen := make([][]uint64, consumers)
	var cw sync.WaitGroup
	for c := range consumers {
		cw.Go(func() {
			for {
				it, ok, err := q.Pop()
				if err != nil {
					t.Error(err)
					return
				}
				if !ok {
					return
				}
				seen[c] = append(seen[c], binary.LittleEndian.Uint64(it.EventID[:]))
			}
		})
	}
	pw.Wait()
	q.Close()
	cw.Wait()
	count := make(map[uint64]int, producers*perProducer)
	for _, s := range seen {
		for _, id := range s {
			count[id]++
		}
	}
	if len(count) != producers*perProducer {
		t.Fatalf("salieron %d ítems distintos, se esperaban %d", len(count), producers*perProducer)
	}
	for id, n := range count {
		if n != 1 {
			t.Fatalf("ítem %d salió %d veces", id, n)
		}
	}
	if st := q.Stats(); st.Spilled == 0 || st.BlockedPushes == 0 {
		t.Fatalf("la prueba debía ejercitar desborde y bloqueo: %+v", st)
	}
}

func TestEncodeDecodeRoundTrip(t *testing.T) {
	var b [recordSize]byte
	for i := range uint64(100) {
		it := item(i * 0x9E3779B97F4A7C15)
		encode(b[:], it)
		if got := decode(b[:]); got != it {
			t.Fatalf("%+v != %+v", got, it)
		}
	}
}
