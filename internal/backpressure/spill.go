package backpressure

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
)

// spillFile es una cola FIFO de registros de tamaño fijo en un archivo:
// se escribe al final y se lee desde rOff. Cuando el lector alcanza al
// escritor, el archivo se trunca a 0 para no crecer sin límite entre
// episodios de desborde.
//
// El archivo es temporal (se crea vacío al arrancar y se borra al final):
// su propósito es acotar la RAM durante una corrida, no sobrevivir a una
// caída. Tras una caída el consumidor re-consume el stream desde el inicio y
// reconstruye agregado, ventana y buffer juntos (ver decisions.md §1).
type spillFile struct {
	f          *os.File
	w          *bufio.Writer
	path       string
	rOff, wOff int64 // en registros
	head       Item  // copia del registro en rOff (para medir antigüedad)
	buf        [recordSize]byte
}

// 16 (event_id) + 16 (order_id) + 1 (country) + 8 (pub_ts) + 8 (enqueued)
const recordSize = 49

func openSpill(path string) (*spillFile, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, fmt.Errorf("backpressure: abrir archivo de desborde: %w", err)
	}
	return &spillFile{f: f, w: bufio.NewWriterSize(f, 256<<10), path: path}, nil
}

func (s *spillFile) len() int { return int(s.wOff - s.rOff) }

func (s *spillFile) append(it Item) error {
	if s.len() == 0 {
		s.head = it
	}
	encode(s.buf[:], it)
	if _, err := s.w.Write(s.buf[:]); err != nil {
		return fmt.Errorf("backpressure: escribir desborde: %w", err)
	}
	s.wOff++
	return nil
}

// read llena dst con los registros más viejos y devuelve cuántos leyó.
func (s *spillFile) read(dst []Item) (int, error) {
	if s.w.Buffered() > 0 {
		if err := s.w.Flush(); err != nil {
			return 0, fmt.Errorf("backpressure: flush desborde: %w", err)
		}
	}
	n := min(len(dst), s.len())
	raw := make([]byte, n*recordSize)
	if _, err := s.f.ReadAt(raw, s.rOff*recordSize); err != nil && err != io.EOF {
		return 0, fmt.Errorf("backpressure: leer desborde: %w", err)
	}
	for i := range n {
		dst[i] = decode(raw[i*recordSize:])
	}
	s.rOff += int64(n)
	if s.len() == 0 {
		// Todo se leyó: reiniciar el archivo. Truncate no mueve la posición de
		// escritura; sin el Seek lo siguiente se escribiría en el offset viejo
		// (con un hueco de ceros delante) y el lector lo buscaría en 0.
		if err := s.f.Truncate(0); err != nil {
			return 0, fmt.Errorf("backpressure: truncar desborde: %w", err)
		}
		if _, err := s.f.Seek(0, io.SeekStart); err != nil {
			return 0, fmt.Errorf("backpressure: rebobinar desborde: %w", err)
		}
		s.rOff, s.wOff = 0, 0
	} else if err := s.loadHead(); err != nil {
		return 0, err
	}
	return n, nil
}

func (s *spillFile) loadHead() error {
	if _, err := s.f.ReadAt(s.buf[:], s.rOff*recordSize); err != nil {
		return fmt.Errorf("backpressure: leer cabeza del desborde: %w", err)
	}
	s.head = decode(s.buf[:])
	return nil
}

func (s *spillFile) oldestEnqueued() int64 { return s.head.Enqueued }

func (s *spillFile) remove() error {
	_ = s.f.Close()
	return os.Remove(s.path)
}

func encode(b []byte, it Item) {
	copy(b[0:16], it.EventID[:])
	copy(b[16:32], it.OrderID[:])
	b[32] = it.Country
	binary.LittleEndian.PutUint64(b[33:41], uint64(it.PubTs))
	binary.LittleEndian.PutUint64(b[41:49], uint64(it.Enqueued))
}

func decode(b []byte) Item {
	var it Item
	copy(it.EventID[:], b[0:16])
	copy(it.OrderID[:], b[16:32])
	it.Country = b[32]
	it.PubTs = int64(binary.LittleEndian.Uint64(b[33:41]))
	it.Enqueued = int64(binary.LittleEndian.Uint64(b[41:49]))
	return it
}
