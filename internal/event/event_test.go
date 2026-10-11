package event

import "testing"

func TestParse(t *testing.T) {
	line := []byte(`{"event_id": "61bada38-c6d6-401f-a2c5-0758faa57fb9", "event_type": "payment_confirmed", "order_id": "17003331-77ac-47cd-8213-a0f26c940490", "country": "MX", "amount_usd": 84.87, "timestamp": "2026-11-14T01:30:41"}`)
	e, err := Parse(line)
	if err != nil {
		t.Fatal(err)
	}
	if e.ID.String() != "61bada38-c6d6-401f-a2c5-0758faa57fb9" {
		t.Errorf("id = %s", e.ID)
	}
	if e.Type != PaymentConfirmed || e.Country != MX || e.Cents != 8487 {
		t.Errorf("evento mal parseado: %+v", e)
	}
	if e.TS != 1794619841 {
		t.Errorf("ts = %d", e.TS)
	}
	if e.OrderID.String() != "17003331-77ac-47cd-8213-a0f26c940490" {
		t.Errorf("order_id = %s", e.OrderID)
	}
	bad := []byte(`{"event_id": "61bada38-c6d6-401f-a2c5-0758faa57fb9", "event_type": "inventory_reserved", "order_id": "x", "country": "MX", "amount_usd": null, "timestamp": "2026-11-14T01:30:41"}`)
	if _, err := Parse(bad); err == nil {
		t.Error("un order_id que no es UUID debe rechazarse")
	}
}

func TestExtractIDMatchesParse(t *testing.T) {
	line := []byte(`{"event_id": "61bada38-c6d6-401f-a2c5-0758faa57fb9", "event_type": "payment_confirmed", "country": "MX", "amount_usd": 1.0, "timestamp": "2026-11-14T01:30:41"}`)
	id, err := ExtractID(line)
	if err != nil {
		t.Fatal(err)
	}
	e, _ := Parse(line)
	if id != e.ID {
		t.Fatalf("ExtractID=%s Parse=%s", id, e.ID)
	}
	compact := []byte(`{"event_type":"order_placed","event_id":"61bada38-c6d6-401f-a2c5-0758faa57fb9"}`)
	if id2, err := ExtractID(compact); err != nil || id2 != id {
		t.Fatalf("formato compacto: %s, %v", id2, err)
	}
	for _, bad := range []string{`{}`, `{"event_id": 5}`, `{"event_id": "short"}`} {
		if _, err := ExtractID([]byte(bad)); err == nil {
			t.Errorf("ExtractID(%s) debería fallar", bad)
		}
	}
}

func TestParseNullAmount(t *testing.T) {
	line := []byte(`{"event_id": "5d2d6a41-391f-4fb6-bb2f-55029c58beeb", "event_type": "inventory_reserved", "order_id": "8e1f6a2b-7c3d-4e5f-9a0b-1c2d3e4f5a6b", "country": "PE", "amount_usd": null, "timestamp": "2026-11-14T01:30:33"}`)
	e, err := Parse(line)
	if err != nil {
		t.Fatal(err)
	}
	if e.Cents != 0 || e.Type != InventoryReserved || e.Country != PE {
		t.Errorf("evento mal parseado: %+v", e)
	}
}

func TestParseCents(t *testing.T) {
	cases := map[string]int64{"29.61": 2961, "30.0": 3000, "8": 800, "8.5": 850, "900.0": 90000, "0.07": 7}
	for in, want := range cases {
		got, err := ParseCents(in)
		if err != nil || got != want {
			t.Errorf("ParseCents(%q) = %d, %v; want %d", in, got, err, want)
		}
	}
	for _, bad := range []string{"1.234", "-1", "abc", "1e5"} {
		if _, err := ParseCents(bad); err == nil {
			t.Errorf("ParseCents(%q) debería fallar", bad)
		}
	}
}

func TestParseIDRejectsGarbage(t *testing.T) {
	for _, bad := range []string{"", "not-a-uuid", "61bada38c6d6401fa2c50758faa57fb9zzzz", "61bada38-c6d6-401f-a2c5-0758faa57fbZ"} {
		if _, err := ParseID(bad); err == nil {
			t.Errorf("ParseID(%q) debería fallar", bad)
		}
	}
}

func BenchmarkParse(b *testing.B) {
	line := []byte(`{"event_id": "61bada38-c6d6-401f-a2c5-0758faa57fb9", "event_type": "payment_confirmed", "order_id": "17003331-77ac-47cd-8213-a0f26c940490", "country": "MX", "amount_usd": 84.87, "timestamp": "2026-11-14T01:30:41"}`)
	b.ReportAllocs()
	for b.Loop() {
		if _, err := Parse(line); err != nil {
			b.Fatal(err)
		}
	}
}
