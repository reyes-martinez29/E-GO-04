// Package event define el modelo de un evento del Hot Sale y su parseo desde
// una línea JSONL. El modelo usa índices y enteros (no strings ni float64) para
// que el consumidor pueda agregar sin allocations y sin error de redondeo.
package event

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

type Type uint8

const (
	OrderPlaced Type = iota
	PaymentConfirmed
	InventoryReserved
	NumTypes
)

var TypeNames = [NumTypes]string{"order_placed", "payment_confirmed", "inventory_reserved"}

type Country uint8

const (
	MX Country = iota
	AR
	CO
	CL
	PE
	NumCountries
)

var CountryNames = [NumCountries]string{"MX", "AR", "CO", "CL", "PE"}

// ID es el event_id (UUID) en su forma binaria de 16 bytes: la mitad de
// memoria que el string de 36 caracteres y sin allocation al usarlo como llave.
type ID [16]byte

type Event struct {
	ID      ID
	OrderID ID // order_id: lo necesita la verificación de inventario (E-Go04)
	Type    Type
	Country Country
	Cents   int64 // amount_usd en centavos; 0 si amount_usd es null
	TS      int64 // timestamp del evento, segundos Unix (UTC)
}

type raw struct {
	EventID   string       `json:"event_id"`
	OrderID   string       `json:"order_id"`
	EventType string       `json:"event_type"`
	Country   string       `json:"country"`
	Amount    *json.Number `json:"amount_usd"`
	Timestamp string       `json:"timestamp"`
}

const tsLayout = "2006-01-02T15:04:05"

// Parse convierte una línea JSONL en un Event.
func Parse(line []byte) (Event, error) {
	var r raw
	if err := json.Unmarshal(line, &r); err != nil {
		return Event{}, fmt.Errorf("json: %w", err)
	}
	var e Event
	var err error
	if e.ID, err = ParseID(r.EventID); err != nil {
		return Event{}, err
	}
	if r.OrderID != "" {
		if e.OrderID, err = ParseID(r.OrderID); err != nil {
			return Event{}, fmt.Errorf("order_id: %w", err)
		}
	}
	if e.Type, err = parseType(r.EventType); err != nil {
		return Event{}, err
	}
	if e.Country, err = parseCountry(r.Country); err != nil {
		return Event{}, err
	}
	if r.Amount != nil {
		if e.Cents, err = ParseCents(string(*r.Amount)); err != nil {
			return Event{}, err
		}
	}
	ts, err := time.Parse(tsLayout, r.Timestamp)
	if err != nil {
		return Event{}, fmt.Errorf("timestamp: %w", err)
	}
	e.TS = ts.Unix()
	return e, nil
}

var errBadUUID = errors.New("event_id no es un UUID válido")

var eventIDKey = []byte(`"event_id"`)

// ExtractID lee solo el event_id de una línea JSON cruda, sin decodificarla
// completa. Lo usa el fetcher del consumidor para enrutar al shard correcto
// antes del json.Unmarshal (que ocurre ya dentro del shard, en orden).
func ExtractID(line []byte) (ID, error) {
	i := bytes.Index(line, eventIDKey)
	if i < 0 {
		return ID{}, errBadUUID
	}
	rest := line[i+len(eventIDKey):]
	start := bytes.IndexByte(rest, '"')
	if start < 0 || len(rest) < start+38 || rest[start+37] != '"' {
		return ID{}, errBadUUID
	}
	return ParseID(string(rest[start+1 : start+37]))
}

// ParseID decodifica un UUID canónico (8-4-4-4-12) a 16 bytes.
func ParseID(s string) (ID, error) {
	var id ID
	if len(s) != 36 || s[8] != '-' || s[13] != '-' || s[18] != '-' || s[23] != '-' {
		return id, errBadUUID
	}
	j := 0
	for _, seg := range [5][2]int{{0, 8}, {9, 13}, {14, 18}, {19, 23}, {24, 36}} {
		n, err := hex.Decode(id[j:], []byte(s[seg[0]:seg[1]]))
		if err != nil {
			return id, errBadUUID
		}
		j += n
	}
	return id, nil
}

func (id ID) String() string {
	h := hex.EncodeToString(id[:])
	return h[0:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:32]
}

func parseType(s string) (Type, error) {
	for i, n := range TypeNames {
		if s == n {
			return Type(i), nil
		}
	}
	return 0, fmt.Errorf("event_type desconocido: %q", s)
}

func parseCountry(s string) (Country, error) {
	for i, n := range CountryNames {
		if s == n {
			return Country(i), nil
		}
	}
	return 0, fmt.Errorf("country desconocido: %q", s)
}

// ParseCents convierte un decimal como "29.61" o "30.0" a centavos de forma
// exacta (sin pasar por float64).
func ParseCents(s string) (int64, error) {
	intPart, frac, _ := strings.Cut(s, ".")
	if len(frac) > 2 {
		return 0, fmt.Errorf("amount_usd con más de 2 decimales: %q", s)
	}
	units, err := strconv.ParseInt(intPart, 10, 64)
	if err != nil || units < 0 {
		return 0, fmt.Errorf("amount_usd inválido: %q", s)
	}
	var cents int64
	if frac != "" {
		frac += strings.Repeat("0", 2-len(frac))
		if cents, err = strconv.ParseInt(frac, 10, 64); err != nil {
			return 0, fmt.Errorf("amount_usd inválido: %q", s)
		}
	}
	return units*100 + cents, nil
}
