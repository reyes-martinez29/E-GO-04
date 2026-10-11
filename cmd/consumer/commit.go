package main

import (
	"sync/atomic"

	"github.com/nats-io/nats.go/jetstream"
)

// Ack por lotes con AckAllPolicy.
//
// Hacer Ack() de cada mensaje costaba ~40% del CPU del consumidor en syscalls
// (un PUB diminuto por evento; E-GO03, hallazgo H3). Con AckAllPolicy, hacer ack
// del mensaje con secuencia S confirma todos los <= S. Pero los shards
// terminan en desorden entre sí, así que no basta con hacer ack del último
// mensaje aplicado: el piso de ack solo puede avanzar hasta el final de un
// lote cuando ese lote Y todos los anteriores están aplicados. Por eso el
// commit es un solo goroutine que espera los lotes en orden de llegada.

type batch struct {
	last    jetstream.Msg
	size    int
	pending atomic.Int64 // mensajes sin aplicar + 1 mientras el lote está abierto
	done    chan struct{}
}

func newBatch() *batch {
	b := &batch{done: make(chan struct{})}
	b.pending.Store(1) // token de "lote abierto": se libera en seal()
	return b
}

func (b *batch) add(msg jetstream.Msg) {
	b.pending.Add(1)
	b.last = msg
	b.size++
}

// finish marca un mensaje del lote como aplicado.
func (b *batch) finish() {
	if b.pending.Add(-1) == 0 {
		close(b.done)
	}
}

// seal cierra el lote: ya no entran más mensajes.
func (b *batch) seal() { b.finish() }

type work struct {
	msg jetstream.Msg
	b   *batch
}

// committer hace ack de cada lote, en orden, cuando todos sus mensajes se
// aplicaron. Devuelve cuántos acks envió.
func committer(batches <-chan *batch, acks *atomic.Int64) {
	for b := range batches {
		<-b.done
		if b.last != nil {
			_ = b.last.Ack()
			acks.Add(1)
		}
	}
}
