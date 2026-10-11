# E-GO04 — Hot Sale LATAM: sistema resiliente con backpressure

Extiende el productor/consumidor exactly-once de E-GO03 (NATS JetStream, dedup
por `event_id`, agregados por país) para sobrevivir al pico del Hot Sale:

- el **productor** publica con pacing real: ritmo normal → **spike a 10×
  durante 2 min** → ritmo normal;
- el **consumidor** confirma cada `inventory_reserved` único contra
  `inventory_service.py` (un servicio externo que se degrada con la carga),
  detrás de un **limitador adaptativo**, un **circuit breaker** y un **buffer
  de espera acotado** (memoria → disco → bloqueo) que nunca descarta.

Las decisiones, parámetros y la evidencia (conteos finales contra el oráculo)
están en [decisions.md](decisions.md).

## Estructura

| Ruta | Qué hace |
| :--- | :--- |
| `cmd/producer/` | De Go03: lector secuencial + `PublishMsgAsync`. **Go04:** pacing por fases (`pacing.go`), línea de tiempo CSV por segundo, resumen de tasa real por fase, reintentos de publicación sin límite. |
| `cmd/consumer/` | De Go03: pull consumer → shards (dedup + agregado sin locks) → ack por lotes. **Go04:** `inventory.go` (buffer → workers → limitador → breaker → HTTP), `monitor.go` (línea de tiempo CSV), drenado del buffer antes de reportar. |
| `cmd/verify/` | Oráculo: verdad del archivo sin broker. **Go04:** compara también inventario confirmado y eventos procesados; `-limit` para corridas parciales. |
| `cmd/invprobe/` | Caracteriza el servicio de inventario (latencia y 503 por tasa). Experimento, no parte del sistema. |
| `internal/breaker/` | Circuit breaker CLOSED / OPEN / HALF-OPEN con generaciones y estadísticas. |
| `internal/backpressure/` | Buffer de espera (`queue.go` + `spill.go`), señal de profundidad con niveles, limitador AIMD (`limiter.go`). |
| `internal/dedup/`, `event/`, `agg/`, `broker/`, `metrics/` | De Go03 (`event` agrega `order_id`; `broker` pasa a `DiscardNew`). |
| `scripts/run.sh` | Corrida completa reproducible: servicio + consumidor + productor + caída opcional + verificación. |
| `inventory_service.py` | Servicio simulado del curso, sin modificar. |
| `Dockerfile`, `docker-compose.e2e.yml` | Corrida end-to-end completa en contenedores (NATS aislado + binarios + servicio + `run.sh`). |
| `results/go04/<corrida>/` | Evidencia: logs, reporte JSON, líneas de tiempo CSV, `verify.txt`. |
| `results/go03-baseline/` | Corrida de Go03 de donde sale el throughput de referencia. |

## Requisitos

- Go 1.25+ (probado con 1.26.5)
- Docker Desktop (probado con 28.5)
- Python 3 (probado con 3.13); `numpy` solo para generar el dataset
- Git Bash para `scripts/run.sh` (en Windows)

## Comandos

Todo se ejecuta desde la raíz del proyecto. Los ejemplos usan Git Bash; en
PowerShell son iguales cambiando `./bin/x.exe` por `.\bin\x.exe`.

### 1. Dataset (el mismo de E-GO03)

`eventos_hotsale.jsonl` y su generador no están en el repositorio (material
del curso, ~666 MB). Copia `generate_events_go03.py` a la raíz y ejecútalo, o
copia el JSONL que ya generaste en Go03:

```bash
python generate_events_go03.py     # escribe eventos_hotsale.jsonl
go run ./cmd/verify                # líneas: 3126750  únicos: 3035188  duplicados: 91562 ...
```

### 2. Broker y servicio de inventario

```bash
docker compose up -d --wait        # NATS 2.11 + JetStream en disco (monitoreo: http://localhost:8222/jsz)
python inventory_service.py        # en otra terminal; escucha en http://localhost:8090
curl -s http://127.0.0.1:8090/stats
```

`scripts/run.sh` levanta el servicio solo si no está corriendo, y siempre lo
resetea (`POST /reset`) antes de cada corrida.

> En Windows usa `127.0.0.1`, no `localhost`: el servicio solo escucha en
> IPv4 y `localhost` intenta IPv6 primero, lo que suma ~200 ms a cada
> conexión (decisions.md, H1). El consumidor ya usa `127.0.0.1` por defecto.

### 3. Compilar y probar

```bash
go test ./...
go build -o bin/ ./cmd/...
```

Race detector (en Windows sin gcc de 64 bits, dentro de un contenedor Linux):

```bash
MSYS_NO_PATHCONV=1 docker run --rm -v "$PWD":/src -w /src -e GOFLAGS=-buildvcs=false golang:1.26 go test -race -count=1 ./...
```

Pipeline completo bajo `-race` (todo en contenedores, red `go-04_default`).
El servicio se importa sin modificarlo, pero escuchando en `0.0.0.0` para que
el consumidor lo alcance desde otro contenedor:

```bash
export MSYS_NO_PATHCONV=1
out=results/go04/race-pipeline; mkdir -p $out
docker run --rm -v "$PWD":/src -w /src -e GOFLAGS=-buildvcs=false golang:1.26 \
  sh -c 'go build -race -o bin/consumer-race ./cmd/consumer && go build -race -o bin/producer-race ./cmd/producer'
docker run -d --name race-svc --network go-04_default -v "$PWD":/src -w /src python:3.13-alpine \
  sh -c 'python -c "import inventory_service as m; m.ThreadingHTTPServer((\"0.0.0.0\", 8090), m.Handler).serve_forever()"'
docker run -d --name race-consumer --network go-04_default -v "$PWD":/src -w /src golang:1.26 \
  sh -c "./bin/consumer-race -url nats://nats:4222 -inventory-url http://race-svc:8090 -buffer-file /tmp/spill.bin -pprof '' -out $out > $out/consumer.log 2>&1"
until grep -q "esperando eventos" $out/consumer.log; do sleep 0.5; done
docker run --rm --network go-04_default -v "$PWD":/src -w /src golang:1.26 \
  sh -c "./bin/producer-race -url nats://nats:4222 -limit 40000 -rate 500 -spike-after 10s -spike-for 30s -out $out > $out/producer.log 2>&1"
docker stop -t 0 race-svc; sleep 20; docker start race-svc     # caída opcional
docker wait race-consumer; docker rm -f race-svc race-consumer
grep -c "WARNING: DATA RACE" $out/*.log                         # esperado: 0 en ambos
```

### 4. Corrida completa (la de decisions.md)

```bash
OUTAGE_AT=100 OUTAGE_FOR=45 scripts/run.sh full
```

- Ritmo normal 2,000 ev/s; spike a 20,000 ev/s del segundo 60 al 180;
  después ritmo normal hasta agotar el archivo (~8 min de publicación).
- A los 100 s (en pleno spike) el script detiene `inventory_service.py`
  durante 45 s y lo vuelve a levantar.
- El consumidor aplica el stream al ritmo del productor y después **drena el
  buffer de inventario al ritmo que el servicio aguanta (~50 confirmaciones/s:
  varias horas para los 815,590 eventos)**. Al final imprime el reporte y
  `run.sh` corre `verify`.

**La misma corrida, toda en Docker** (no hace falta Go ni Python en el host;
queda detached y sobrevive a cerrar la terminal). Usa su propio NATS en una
red aislada, así que no choca con `docker-compose.yml` ni con una corrida
local en curso:

```bash
docker compose -f docker-compose.e2e.yml up -d --build      # results/go04/full-docker/
docker compose -f docker-compose.e2e.yml logs -f runner     # progreso (Ctrl+C no la detiene)
docker compose -f docker-compose.e2e.yml down -v            # al terminar
# corrida corta para probar el stack (~5 min):
RUN_LABEL=smoke-docker RUN_LIMIT=40000 RATE=500 SPIKE_AFTER=10s SPIKE_FOR=30s OUTAGE_AT=25 OUTAGE_FOR=20 \
  docker compose -f docker-compose.e2e.yml up -d --build
```

Variables: `RATE`, `SPIKE_MULT`, `SPIKE_AFTER`, `SPIKE_FOR`, `OUTAGE_AT`,
`OUTAGE_FOR`, `CONSUMER_ARGS`, `OUT`. Segundo argumento: límite de líneas.

Corridas cortas para probar (minutos, no horas):

```bash
RATE=500 SPIKE_AFTER=10s SPIKE_FOR=30s OUTAGE_AT=25 OUTAGE_FOR=20 scripts/run.sh outage-smoke 40000
CONSUMER_ARGS="-no-limiter" RATE=500 SPIKE_AFTER=10s SPIKE_FOR=30s scripts/run.sh ablation-no-limiter 40000
CONSUMER_ARGS="-buffer-mem 500 -buffer-disk 2000" RATE=500 SPIKE_AFTER=10s SPIKE_FOR=30s scripts/run.sh blocked-buffer 40000
```

### 5. Corrida manual (sin el script)

```bash
# terminal 1
python inventory_service.py
# terminal 2
./bin/consumer.exe -out results/manual
# terminal 3, cuando el consumidor diga "esperando eventos..."
./bin/producer.exe -out results/manual                  # 2000 ev/s, spike 10x 60s→180s
./bin/verify.exe -report results/manual/consumer-<fecha>.json
```

### 6. Qué mirar durante la corrida

- Consumidor: líneas `BREAKER closed → open ...`, `BUFFER memory → spilling ...`
  y el progreso cada 5 s (cada 30 s durante el drenado, con ETA).
- `results/<corrida>/timeline-*.csv`: una fila por segundo con ingesta,
  latencia e2e, profundidad del buffer (memoria/disco/nivel), antigüedad del
  más viejo, estado del breaker, tasa del limitador y llamadas OK/fallidas.
- `results/<corrida>/producer-*.csv`: tasa publicada por segundo y fase.

## Flags principales

| Binario | Flag | Default | Uso |
| :--- | :--- | :--- | :--- |
| producer | `-rate` | 2000 | ritmo normal ev/s (0 = sin límite, como Go03) |
| producer | `-spike-mult` / `-spike-after` / `-spike-for` | 10 / 1m / 2m | ventana de spike |
| producer | `-limit` | 0 | publicar solo N líneas |
| consumer | `-inventory-url` | http://127.0.0.1:8090 | servicio de inventario |
| consumer | `-inv-workers` / `-inv-timeout` | 32 / 1s | llamadas en vuelo y timeout |
| consumer | `-breaker-failures` / `-breaker-cooldown` / `-breaker-probes` | 5 / 2s / 3 | circuit breaker |
| consumer | `-lim-*` | ver `-h` | limitador AIMD (inicial 10, +2, ×0.75, ventana 1s, latencia 4× base) |
| consumer | `-no-limiter` | false | **ablación**: solo breaker + workers |
| consumer | `-buffer-mem` / `-buffer-disk` / `-buffer-file` | 50000 / 4000000 / `%TEMP%` | buffer de espera |
| consumer | `-ack-wait` | 5m | espera de ack del broker (nivel BLOCKED) |
| consumer | `-purge` | true | vaciar el stream al arrancar |

## Apagar

```bash
docker compose down        # conserva el volumen
docker compose down -v     # borra también el stream
```
