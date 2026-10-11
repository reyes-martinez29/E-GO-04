// Package broker concentra la configuración del stream de JetStream para que
// productor y consumidor la declaren igual (cualquiera de los dos puede
// arrancar primero).
package broker

import (
	"context"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

const (
	StreamName    = "HOTSALE"
	EventsSubject = "hotsale.events"
	EOFSubject    = "hotsale.control.eof"
	ConsumerName  = "hotsale-agg"

	// Headers propios.
	HeaderPubTs      = "Pub-Ts"      // UnixNano en el momento de publicar (latencia end-to-end)
	HeaderTotalLines = "Total-Lines" // en el mensaje EOF: líneas publicadas
)

// DuplicateWindow es la ventana de deduplicación del BROKER, que actúa sobre
// Nats-Msg-Id = número de línea. Solo absorbe reintentos del propio productor
// (p. ej. reenvío tras un timeout de PubAck), que ocurren en segundos. NO
// deduplica reintentos del gateway: esos son líneas distintas con el mismo
// event_id y deben llegar al consumidor para que él los detecte y cuente.
const DuplicateWindow = time.Minute

// EnsureStream declara el stream. El stream es el último eslabón de la cadena
// de backpressure (decisions.md §1): si el consumidor deja de pedir mensajes
// porque su buffer está lleno, los mensajes esperan aquí, en el disco del
// broker. Por eso Discard es DiscardNew: con el stream lleno (MaxBytes) el
// broker RECHAZA la publicación nueva y el productor reintenta. Con
// DiscardOld (lo que tenía E-Go03) borraría en silencio los mensajes más
// viejos, aunque nadie los haya consumido: pérdida de datos.
func EnsureStream(ctx context.Context, js jetstream.JetStream) (jetstream.Stream, error) {
	return js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:       StreamName,
		Subjects:   []string{"hotsale.>"},
		Storage:    jetstream.FileStorage,
		Retention:  jetstream.LimitsPolicy, // no WorkQueue: permite re-consumir desde el inicio tras un reinicio
		Duplicates: DuplicateWindow,
		MaxBytes:   4 << 30,
		Discard:    jetstream.DiscardNew,
	})
}
