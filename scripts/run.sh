#!/usr/bin/env bash
# Corrida completa reproducible de E-Go04:
#   1. levanta inventory_service.py si no está corriendo (y lo resetea)
#   2. arranca el consumidor (purga el stream) y espera a que esté listo
#   3. publica con el productor: ritmo normal → spike → ritmo normal
#   4. (opcional) tira el servicio de inventario OUTAGE_FOR segundos a partir
#      de OUTAGE_AT segundos, para ver al breaker abrir y recuperarse
#   5. espera a que el consumidor drene el buffer y compara contra el oráculo
#
# Uso: scripts/run.sh <label> [limit líneas, 0 = todas]
# Variables (con su default):
#   RATE=2000 SPIKE_MULT=10 SPIKE_AFTER=60s SPIKE_FOR=2m
#   OUTAGE_AT= OUTAGE_FOR=      (segundos; vacío = sin caída)
#   CONSUMER_ARGS=              (flags extra del consumidor, p. ej. "-no-limiter")
#   OUT=results/go04
set -euo pipefail
cd "$(dirname "$0")/.."

label=${1:?label}
limit=${2:-0}
out=${OUT:-results/go04}/$label
mkdir -p "$out"
ext=""; [[ "${OS:-}" == "Windows_NT" ]] && ext=".exe"
svc=http://127.0.0.1:8090
# En Windows "python3" suele ser el alias de la Microsoft Store, que no
# ejecuta nada: se elige el primer intérprete que realmente corre.
py=""
for cand in python3 python; do
  if command -v "$cand" >/dev/null && "$cand" -c "import sys" >/dev/null 2>&1; then py=$cand; break; fi
done

svc_pid=""
start_service() {
  [[ -n "$py" ]] || { echo "no se encontró un intérprete de Python que funcione" >&2; exit 1; }
  "$py" inventory_service.py >>"$out/inventory-service.log" 2>&1 &
  svc_pid=$!
  for _ in $(seq 50); do
    curl -s -o /dev/null "$svc/stats" && return 0
    sleep 0.2
  done
  echo "inventory_service.py no respondió en 10 s; ver $out/inventory-service.log" >&2
  exit 1
}
stop_service() {
  [[ -n "$svc_pid" ]] && kill "$svc_pid" 2>/dev/null && wait "$svc_pid" 2>/dev/null || true
  svc_pid=""
}
trap stop_service EXIT

if curl -s -o /dev/null "$svc/stats"; then
  if [[ -n "${OUTAGE_AT:-}" ]]; then
    echo "OUTAGE_AT requiere que el script controle el servicio: detén el inventory_service.py que ya corre" >&2
    exit 1
  fi
  echo "usando el inventory_service.py que ya corre en $svc"
else
  echo "levantando inventory_service.py"
  start_service
fi
curl -s -X POST "$svc/reset" >/dev/null

clog="$out/consumer.log"
./bin/consumer$ext -label "$label" -out "$out" -pprof "" ${CONSUMER_ARGS:-} >"$clog" 2>&1 &
cpid=$!
until grep -q "esperando eventos" "$clog"; do
  kill -0 "$cpid" 2>/dev/null || { cat "$clog"; exit 1; }
  sleep 0.2
done

./bin/producer$ext -limit "$limit" -out "$out" \
  -rate "${RATE:-2000}" -spike-mult "${SPIKE_MULT:-10}" \
  -spike-after "${SPIKE_AFTER:-60s}" -spike-for "${SPIKE_FOR:-2m}" >"$out/producer.log" 2>&1 &
ppid=$!

if [[ -n "${OUTAGE_AT:-}" ]]; then
  sleep "$OUTAGE_AT"
  echo "$(date +%T) CAÍDA: deteniendo inventory_service.py por ${OUTAGE_FOR:-30}s" | tee -a "$clog.outage"
  stop_service
  sleep "${OUTAGE_FOR:-30}"
  echo "$(date +%T) RECUPERACIÓN: levantando inventory_service.py" | tee -a "$clog.outage"
  start_service
fi

wait "$ppid"
echo "productor terminó; esperando a que el consumidor drene el buffer de inventario..."
wait "$cpid"
curl -s "$svc/stats" >"$out/inventory-stats-final.json" || true

echo "== $label (rate=${RATE:-2000} spike=${SPIKE_MULT:-10}x limit=$limit)"
sed -n '/=== FASES/,/spike \/ normal/p' "$out/producer.log"
sed -n '/=== CONTEO/,$p' "$clog"
report=$(grep -E "^reporte:" "$clog" | awk '{print $2}')
./bin/verify$ext -limit "$limit" -report "$report" | sed -n '/=== COMPARACIÓN/,$p' | tee "$out/verify.txt"
