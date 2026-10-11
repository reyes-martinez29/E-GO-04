// Verify es el oráculo: lee el archivo directo (sin broker), deduplica con un
// map SIN ventana (recuerda todos los event_id) y calcula el agregado
// verdadero. Con -report compara contra el JSON que escribió el consumidor:
// duplicados, agregado al centavo y (E-Go04) que cada inventory_reserved
// único quedó confirmado y que los eventos procesados son exactamente los
// únicos del archivo. Con -limit N calcula la verdad de las primeras N
// líneas (para corridas parciales con producer -limit N).
package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"

	"hotsale/internal/agg"
	"hotsale/internal/event"
)

func main() {
	file := flag.String("file", "eventos_hotsale.jsonl", "archivo JSONL de eventos")
	reportPath := flag.String("report", "", "reporte JSON del consumidor a comparar (opcional)")
	limit := flag.Int64("limit", 0, "considerar solo las primeras N líneas (0 = todas)")
	flag.Parse()

	f, err := os.Open(*file)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	seen := make(map[event.ID]int64, 3_100_000) // event_id -> línea del original
	var a agg.Agg
	var lines, dups, maxDist int64
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64<<10), 1<<20)
	for sc.Scan() {
		if *limit > 0 && lines >= *limit {
			break
		}
		lines++
		e, err := event.Parse(sc.Bytes())
		if err != nil {
			log.Fatalf("línea %d: %v", lines, err)
		}
		if orig, ok := seen[e.ID]; ok {
			dups++
			maxDist = max(maxDist, lines-orig)
			continue
		}
		seen[e.ID] = lines
		a.Apply(&e)
	}
	if err := sc.Err(); err != nil {
		log.Fatal(err)
	}
	truth := a.Report()

	fmt.Printf("=== VERDAD (archivo, sin ventana) ===\n")
	fmt.Printf("líneas: %d  únicos: %d  duplicados: %d  distancia máx. original->reintento: %d línea(s)\n",
		lines, truth.Total.UniqueEventsCounted, dups, maxDist)
	fmt.Printf("por tipo: %v\n", truth.ByType)
	for _, c := range event.CountryNames {
		l := truth.ByCountry[c]
		fmt.Printf("%s órdenes=%d (%s USD) pagos=%d (%s USD) inventario=%d\n", c, l.Orders, l.OrdersUSD, l.PaymentsConfirmed, l.PaymentsUSD, l.InventoryReserved)
	}
	t := truth.Total
	fmt.Printf("TOTAL órdenes=%d (%s USD) pagos=%d (%s USD) inventario=%d\n", t.Orders, t.OrdersUSD, t.PaymentsConfirmed, t.PaymentsUSD, t.InventoryReserved)

	if *reportPath == "" {
		return
	}
	b, err := os.ReadFile(*reportPath)
	if err != nil {
		log.Fatal(err)
	}
	var rep struct {
		Counts struct {
			Expected   int64 `json:"expected_lines"`
			Duplicates int64 `json:"duplicates_discarded"`
			Processed  int64 `json:"processed"`
		} `json:"counts"`
		Inventory struct {
			Confirmed int64 `json:"confirmed"`
		} `json:"inventory"`
		Aggregate agg.Report `json:"aggregate"`
	}
	if err := json.Unmarshal(b, &rep); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("\n=== COMPARACIÓN CON %s ===\n", *reportPath)
	ok := true
	if rep.Counts.Duplicates != dups {
		ok = false
		fmt.Printf("FALLA duplicados: consumidor=%d verdad=%d\n", rep.Counts.Duplicates, dups)
	} else {
		fmt.Printf("OK duplicados: %d == %d\n", rep.Counts.Duplicates, dups)
	}
	if rep.Counts.Expected != lines {
		ok = false
		fmt.Printf("FALLA el consumidor esperaba %d líneas y el oráculo leyó %d (¿falta -limit?)\n", rep.Counts.Expected, lines)
	}
	cmp := func(name string, got, want int64) {
		if got != want {
			ok = false
			fmt.Printf("FALLA %s: consumidor=%d verdad=%d\n", name, got, want)
		} else {
			fmt.Printf("OK %s: %d == %d\n", name, got, want)
		}
	}
	cmp("inventario confirmado", rep.Inventory.Confirmed, t.InventoryReserved)
	cmp("eventos procesados", rep.Counts.Processed, t.UniqueEventsCounted)
	if diffs := agg.Diff(rep.Aggregate, truth); len(diffs) > 0 {
		ok = false
		for _, d := range diffs {
			fmt.Println("FALLA", d)
		}
	} else {
		fmt.Println("OK agregado idéntico al centavo en los 5 países y el total")
	}
	if !ok {
		os.Exit(1)
	}
}
