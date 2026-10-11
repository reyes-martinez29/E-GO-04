# decisions.md — E-GO04 Sistema resiliente con backpressure

En este archivo registro las decisiones que tomé en la semana 20 y la
evidencia que respalda cada una. Todas las cifras las obtuve de corridas
reales en mi máquina (Windows 11, 20 hilos, Docker Desktop 28.5 con NATS 2.11,
Go 1.26.5, Python 3.13). Guardé los reportes JSON, los logs y las líneas de
tiempo por segundo en `results/go04/`.

Partí del sistema que construí en E-GO03 (NATS JetStream, dedup por
`event_id` con ventana de 5 min, shards sin locks, ack por lotes) y lo
**extendí sin reescribirlo**: dejé intacto el pipeline de Go03 y le agregué el
pacing del productor, la verificación de inventario y la cadena de
backpressure.

## Resumen

| Pregunta (§6 de la guía) | Mi respuesta corta |
| :--- | :--- |
| Definición operativa de "no perder datos" | Mi sistema **nunca descarta** un `inventory_reserved` único. Cada uno espera su verificación en un buffer de 3 niveles que diseñé: **memoria (50,000) → disco (hasta 4M) → bloqueo**. Al bloquear, el consumidor deja de pedir mensajes y estos esperan en JetStream, al que configuré para **rechazar** en vez de borrar (`DiscardNew`). §1 |
| Señal de backpressure | Mido la **profundidad del buffer de espera** (memoria + disco) y su antigüedad, cada segundo. Elegí esta señal porque mide directamente "llega más trabajo del que sale", sin importar la causa. La latencia y los errores del servicio los uso como señales de *salud de la dependencia*: alimentan al limitador y al breaker. §2 |
| Circuit breaker | Elegí **5 fallos consecutivos** para abrir, **2 s** de cooldown y **3 pruebas** en half-open (las 3 deben salir bien). Derivé cada número de la curva que medí del servicio. §3 |
| Evidencia sin pérdida en el spike 10× | Corrí el archivo completo con un spike de **20,000 ev/s durante 2 min 0 s (10.00×)** y con el servicio caído 45 s en medio. Obtuve **3,035,188 procesados = 3,035,188 esperados** (el mismo total que en Go03), **815,590 = 815,590** confirmados de inventario y 91,562 = 91,562 duplicados; `verify` dio OK al centavo y los 8/8 chequeos salieron OK. Repetí la corrida en Docker y obtuve exactamente lo mismo. §4 |

---

## 0. Ritmo normal: por qué elegí 2,000 ev/s y no 20,000

La guía pide usar el throughput de Go03 como ritmo normal y un spike de 10×
durante al menos 2 minutos. Mi baseline de Go03 fue **20,000 ev/s**
(`results/go03-baseline/`). Cuando hice las cuentas, comprobé que tomarlo
literal es imposible:

| Requisito literal | Lo que haría falta | Lo que tengo |
| :--- | :--- | :--- |
| Spike de 200,000 ev/s × 120 s | 24,000,000 eventos | El archivo tiene **3,126,750**: a ese ritmo se acaba en 15.6 s |
| Publicar a 200,000 ev/s | un broker que aguante 200k ev/s | En Go03 medí un techo de **66–77k ev/s** (NATS en Docker Desktop) |

**Decisión:** fijé el ritmo normal en **R = 2,000 ev/s** y el spike en
**10R = 20,000 ev/s durante 120 s**. Con esto respeto las dos partes del
requisito que sí puedo medir (10× y ≥ 2 min), y además **el pico cae
exactamente en el throughput que demostré sostener en Go03** (20k ev/s con
p95 = 1.13 ms sobre el archivo completo). Con R = 2,000 el archivo me alcanza
para 60 s de ritmo normal, 120 s de spike (2.4M eventos) y ~5 min de ritmo
normal después, así que puedo mostrar el antes, el durante y el después.

Al analizar el problema, concluí que lo que realmente limita al sistema es la
dependencia y no el broker: el servicio de inventario está sano solo hasta
~60 llamadas/s (§3). Como el 26 % de los eventos son `inventory_reserved`, eso
equivale a ~230 ev/s, **100× menos que mi throughput de Go03**. Cualquier
ritmo normal que derive de Go03 satura la dependencia; por eso entendí el
ejercicio como decidir qué hacer con esa saturación.

---

## 1. Mi definición operativa de "no perder datos"

**Definición:** considero que un evento está *procesado* cuando lo apliqué al
agregado y, si es `inventory_reserved`, cuando además el servicio lo
**confirmó con un 200** en `/check-inventory`. Para mí, "no perder datos"
significa que **al terminar la corrida, los procesados son iguales a los
eventos únicos del archivo**, y que ningún componente descarta un evento para
aliviar carga. Mi sistema puede *atrasar* eventos, pero nunca los *descarta*.

### Qué decidí atrasar y qué no

- **No atraso** la dedup ni el agregado de órdenes, pagos e inventario
  reservado. Los mantengo al ritmo del stream porque no quería que el
  dashboard de negocio se congelara por culpa de una dependencia lenta.
- **Atraso solo la confirmación** de inventario, porque es lo único que
  depende del servicio externo.
- Descarto los duplicados **antes** de llamar al servicio: así un reintento
  del gateway no me cuesta una llamada (en el archivo completo me ahorré
  24,757 llamadas).

### Dónde vive lo que atraso: tres niveles explícitos

```
shard ──Push──▶ [ MEMORIA 50,000 ] ──llena──▶ [ DISCO hasta 4,000,000 ] ──lleno──▶ BLOQUEO
                                                                                     │
       consumidor deja de pedir mensajes ◀── shards bloqueados ◀───────────────────┘
                     │
                     ▼
       mensajes esperan en JetStream (disco del broker, MaxBytes 4 GiB)
                     │ lleno
                     ▼
       broker RECHAZA la publicación (DiscardNew) ──▶ productor reintenta sin límite y se frena
```

| Nivel | Cuándo ocurre | Qué hace mi sistema | Por qué elegí este tamaño |
| :--- | :--- | :--- | :--- |
| `memory` | profundidad ≤ 50,000 | guarda los ítems en un anillo en RAM (~2.8 MB) | Cubre ~10 s de demanda de inventario en pleno spike (5,200/s), o ~15 min de capacidad del servicio. Lo dimensioné para absorber ráfagas; considero que lo que excede eso ya es backlog estructural, no jitter. |
| `spilling` | memoria llena | escribe los nuevos ítems en un archivo de registros fijos de 49 B en `%TEMP%` (lo puse fuera de OneDrive a propósito) | Acota la RAM sin importar el tamaño del pico. Elegí un cupo de 4M (~196 MB), 5× todos los `inventory_reserved` del archivo, para que en este escenario nunca se llene. |
| `blocked` | memoria y disco llenos | `Push` bloquea al shard → el ack se detiene → el consumidor deja de pedir → los mensajes esperan en el broker | Lo diseñé como último recurso: prefiero que la presión suba hasta la fuente antes que soltar eventos. |

Para que esto fuera una decisión y no un efecto secundario, cuidé estos
detalles:

- **FIFO global:** mientras quede algo en disco, mando lo nuevo también a
  disco. Si no lo hiciera, lo nuevo en memoria se adelantaría a lo viejo en
  disco.
- **Cambié el stream a `DiscardNew`** (en Go03 tenía `DiscardOld`). Al diseñar
  la cadena de backpressure noté que, con `DiscardOld`, un stream lleno borra
  en silencio los mensajes más viejos aunque nadie los haya consumido. Era una
  pérdida latente que en Go03 nunca se disparó (666 MB < 4 GiB), pero que esta
  semana sí importa.
- **Quité el límite de reintentos del productor** (en Go03 hacía 5 intentos y
  luego daba el mensaje por perdido). Ahora reintenta con espera creciente
  hasta 5 s; mientras reintenta no confirma los siguientes, así que se
  bloquea al llegar a `max-pending` y se frena solo.
- **Mis workers nunca sueltan un ítem sin confirmar:** ante un 503, un
  timeout, un error de conexión o el breaker abierto, esperan (con espera
  exponencial y jitter, tope de 2 s, o el `RetryAfter` del breaker) y
  reintentan el mismo ítem.
- **Subí `AckWait` a 5 min** (en Go03 era de 60 s). En el nivel `blocked`, los
  mensajes ya entregados esperan a que el buffer libere espacio, y con 60 s el
  broker los re-entregaría. La ventana de dedup igual cubriría esas
  re-entregas (las cuento aparte), pero así evito el ruido.

### Evidencia de cada nivel

| Corrida | Nivel que ejercité | Resultado |
| :--- | :--- | :--- |
| `full` (archivo completo) | memory → **spilling** | La profundidad máxima llegó a **796,655** (memoria 50,000 + disco 763,035) al terminar el stream (s 484); **763,035 ítems pasaron por disco**; el buffer pasó 5 h 27 min en `spilling` y **no perdí ninguno** (815,590/815,590 confirmados). En Docker: 797,673 (disco 763,351). |
| `blocked-buffer` (40k líneas, `-buffer-mem 500 -buffer-disk 2000`) | **blocked** | Forcé **2 min 49 s en `blocked`**, con 126 Push bloqueados y 9,891 ítems por disco. Durante el bloqueo capturé el estado del broker: JetStream tenía **13,366 mensajes sin entregar** y 16,211 entregados sin ack (`jsz-during-block.txt`). Obtuve **0 re-entregas**, 10,467/10,467 confirmados y `verify` OK. |

### Qué pasa si el proceso se cae

Decidí que el buffer sea local y que no sobreviva a una caída. Lo hice a
propósito para mantener el criterio que fijé en Go03 (§5 de aquel
decisions.md): **el estado se reconstruye completo o no se reconstruye**. El
stream retiene todo (retención `Limits`), y si reinicio el consumidor con
`-purge=false`, este re-consume desde el primer mensaje y reconstruye a la vez
la ventana, el agregado **y** las verificaciones pendientes. Verificar
inventario es idempotente (es una consulta), así que re-verificar lo ya
confirmado no cambia nada. Acepté como costo que, tras una caída, el sistema
repita llamadas al servicio. Si ese costo no fuera aceptable, mi siguiente
paso sería persistir el conjunto de confirmados junto con la secuencia del
stream.

---

## 2. La señal de backpressure

### Qué mido

Mido **la profundidad del buffer de espera** (`memoria + disco`) y la
**antigüedad del ítem más viejo**, y las leo del propio `Queue` cada segundo
(columnas `buffer_depth`, `buffer_mem`, `buffer_disk`, `buffer_level` y
`oldest_wait_s` de `timeline-*.csv`). Definí el nivel
(`memory`/`spilling`/`blocked`) como una función explícita de esa
profundidad, y registro cada cambio de nivel en el log
(`BUFFER memory → spilling ...`) y en el reporte (`buffer_level_changes`).

### Por qué elegí esta señal y no otra

- **Mide exactamente el fenómeno:** la profundidad es la integral de
  (llegadas − salidas). Si sube, el trabajo se acumula más rápido de lo que lo
  proceso, sea cual sea la causa: servicio lento, breaker abierto, limitador
  conservador o spike del productor. Con una sola señal cubro todas las
  causas.
- **Descarté la latencia de procesamiento** del pipeline principal porque no
  detecta el problema: durante el spike del archivo completo, la latencia e2e
  media por segundo nunca pasó de 6 ms, mientras el buffer crecía de 28k a
  650k. El atraso está en la confirmación, no en el stream.
- **Descarté la tasa de error del servicio** porque describe a la
  dependencia, no a mi backlog. Con el limitador funcionando, la tasa de error
  es baja (~1.4 %) justo cuando el backlog es máximo; si la usara como señal
  de backpressure, me diría "todo bien" en el peor momento. Por eso la uso
  para otros dos mecanismos: el **limitador** (degradación) y el **breaker**
  (falla).
- **Agregué la antigüedad del más viejo** para complementar la profundidad:
  traduce el backlog a tiempo de negocio ("la confirmación más atrasada lleva
  X minutos").

### Qué decisión dispara

| Señal | Acción de mi sistema |
| :--- | :--- |
| profundidad > memoria | desborda a disco (en lugar de hacer crecer la RAM) |
| profundidad > memoria + disco | bloquea la ingesta (en lugar de descartar) |
| profundidad alta | **no** sube la tasa de llamadas. Considero que tener más trabajo pendiente no hace que el servicio aguante más; subirla sería trasladarle mi presión a una dependencia que ya está saturada. |

### Cómo decido la tasa de llamadas: limitador AIMD

Decidí degradar de forma explícita la frecuencia con que llamo al servicio
mediante un limitador AIMD (como el control de congestión de TCP), y lo
diseñé para que funcione **sin conocer los umbrales del servicio**:

- Evalúa ventanas de 1 s. Si una ventana tiene algún fallo, o su latencia
  media supera 4× la base observada, multiplica la tasa por 0.75. Si la
  ventana está sana y aprovechada (≥ 80 % de la tasa), le suma 2/s. Cuando el
  breaker abre desde CLOSED, la multiplica por 0.5.
- Ignora los resultados de llamadas que salieron **antes** del último recorte
  (la misma idea que las generaciones del breaker). Lo agregué después de
  notar que en la primera corrida el limitador recortaba dos veces por la
  misma sobrecarga (62 → 46 → 35, H3).
- Programa el ritmo contra el reloj, sin cubeta de tokens: después de un rato
  sin llamadas no suelta una ráfaga.
- Converge solo a la capacidad sana: en todas mis corridas osciló entre ~47 y
  ~63 llamadas/s, y descubrió una latencia base de ~18 ms.

### La ablación: ¿hace falta el limitador?

Para comprobar si el limitador se justificaba, corrí la misma carga (40k
líneas, 500 → 5,000 ev/s) con y sin él:

| | Con limitador (`blocked-buffer`) | Sin limitador (`ablation-no-limiter`) |
| :--- | ---: | ---: |
| Confirmaciones/s | **45.2** | **77.7** (1.7×) |
| Respuestas 503 | **1.4 %** | **19.3 %** |
| Latencia por llamada p50 / p95 | **22 / 207 ms** | **318 / 328 ms** |
| Aperturas del breaker | 0 | 0 |
| Confirmados / esperados | 10,467 / 10,467 | 10,467 / 10,467 |

El resultado me sorprendió: sin limitador drené más rápido. Al analizarlo,
entendí que los 32 workers en lazo cerrado se autorregulan (por la ley de
Little: 32 en vuelo ÷ ~0.33 s ≈ 100 llamadas/s) y dejan al servicio **en su
zona degradada todo el tiempo**, con 15× más latencia y 1 de cada 5 llamadas
fallando. Saca más confirmaciones por segundo porque el servicio, aun
degradado, devuelve más éxitos totales.

**Decisión: mantuve el limitador.** El servicio de inventario es compartido
(la guía dice que falla "porque también está saturado"), y operarlo en su
zona degradada es exactamente la cascada que el ejercicio pide evitar. La
guía lo dice de forma explícita: la respuesta correcta "no es procesar más
rápido a cualquier costo". Lo que pago es atraso en una confirmación que
puede esperar. Dejé la alternativa a un flag (`-no-limiter`) por si el
negocio prefiriera drenar más rápido a costa del servicio.

---

## 3. Circuit breaker

### Cómo lo implementé (`internal/breaker`)

```
CLOSED ──5 fallos consecutivos──▶ OPEN ──2 s──▶ HALF-OPEN ──3/3 OK──▶ CLOSED
                                    ▲                │
                                    └──1 prueba falla┘  (cooldown completo de nuevo)
```

- Envuelve **solo** las llamadas a `/check-inventory`. Cuento como fallo un
  503, un timeout (1 s) o un error de conexión.
- En OPEN no deja salir ninguna llamada: `Allow` devuelve `*OpenError` con el
  `RetryAfter`, y el worker espera ese tiempo **conservando su ítem**.
- En HALF-OPEN deja salir como máximo 3 pruebas a la vez y rechaza las demás.
- **Generaciones:** las agregué para que una llamada que salió con el breaker
  cerrado y regresa cuando ya está abierto no decida sobre un estado que no la
  autorizó (`stale_results`).
- Fecho la transición OPEN → HALF-OPEN al vencer el cooldown, no cuando
  alguien la observa. Así el "tiempo abierto" que reporto es exacto.
- Lo probé de forma aislada con un reloj falso (`breaker_test.go`): verifiqué
  que abre solo con fallos *consecutivos*, que rechaza durante todo el
  cooldown con el `RetryAfter` correcto, que 3/3 lo cierra, que una prueba
  fallida lo reabre con el cooldown completo, que los resultados viejos no
  cambian el estado, y que soporta 16 goroutines concurrentes bajo `-race`.

### La curva del servicio (de donde saqué los números)

Antes de fijar los parámetros, caractericé el servicio con
`go run ./cmd/invprobe`, que manda llamadas a ritmo fijo durante 5 s por
escalón (`results/go04/invprobe.txt`):

| llamadas/s | OK | 503 | p50 | p95 |
| ---: | ---: | ---: | ---: | ---: |
| 20 | 100 % | 0 % | 22 ms | 31 ms |
| 40 | 100 % | 0 % | 20 ms | 30 ms |
| 60 | 92.3 % | 7.7 % | 27 ms | 205 ms |
| 80 | 84.8 % | 15.2 % | 265 ms | 268 ms |
| 120 | 81.8 % | 18.2 % | 391 ms | 394 ms |
| 180 | 73.9 % | 26.1 % | 580 ms | 583 ms |
| 250 | 50.9 % | 49.1 % | 801 ms | 1,596 ms |
| 400 | 36.0 % | 64.0 % | 21 ms | 1,836 ms |

También medí que, después de saturarlo a 400/s, el servicio vuelve a ver
0 rps **1.02 s** después de la última llamada, y que la siguiente llamada
responde 200 en 18 ms.

Mi sistema **no usa** estas cifras: el limitador descubre la capacidad por sí
solo. Las usé únicamente para justificar los parámetros.

### Parámetros y justificación

| Parámetro | Valor | Por qué lo elegí |
| :--- | :--- | :--- |
| Fallos consecutivos para abrir | **5** | Quería separar "degradado" de "saturado o caído". Con 503 independientes, la probabilidad de 5 seguidos es 0.15⁵ ≈ 0.008 % en la zona degradada leve (80 rps), 0.26⁵ ≈ 0.1 % a 180 rps, y 0.64⁵ ≈ 11 % saturado (400 rps). Con el servicio caído, abre a la 5ª llamada (~0.1 s). Un umbral más bajo (3) lo haría abrir en degradación leve, que le corresponde al limitador; uno más alto (10) dejaría pasar más llamadas inútiles a un servicio caído. En las corridas cortas observé una racha máxima de 3–4; las rachas de 5 solo aparecieron con la caída real o tras horas operando pegado a la capacidad (H4). |
| Cooldown (OPEN) | **2 s** | Medí que el servicio olvida una saturación en ~1 s, así que elegí 2× ese tiempo para que la prueba lo encuentre ya sin mi carga. Con ≤ 1 s probaría antes de que se recupere y casi seguro reabriría; con 10 s perdería hasta ~500 confirmaciones por apertura con el servicio ya sano. Un servicio caído recibe 1 prueba cada 2 s: una carga despreciable. |
| Pruebas en HALF-OPEN | **3, todas OK** | Una sola prueba OK puede ser suerte: a 120 rps el 82 % de las llamadas sale bien. Con 3 de 3, la probabilidad de cerrar por suerte con el servicio saturado (400 rps) es 0.36³ ≈ 5 %; con el servicio sano es ~100 %. Además, 3 llamadas no vuelven a saturar a nadie. |
| Timeout por llamada | **1 s** | El p95 sano es de 31 ms y el p95 degradado no pasa de 583 ms. Decidí tratar como fallo un éxito de 1–2 s (zona saturada), porque mantiene ocupado a un worker 30–60× más que uno sano. |
| Workers (llamadas en vuelo) | **32** | Los usé como tope duro de goroutines y conexiones que un servicio colgado puede retener: como mucho 32 durante 1 s. |

### Capas: quién responde a qué

Diseñé tres capas con responsabilidades separadas:

- **Degradación** (latencia ×10, 5–20 % de 503): el **limitador** baja la
  tasa. El breaker no se entera, porque no se juntan 5 fallos seguidos.
- **Falla** (servicio caído o saturado): el **breaker** corta del todo y
  prueba cada 2 s.
- **Recursos**: los **32 workers** con timeout acotan lo que la dependencia
  puede retener.

En mis corridas cortas (minutos) sin caída, el breaker **nunca abrió**: el
limitador mantiene al servicio sano y no se juntan 5 fallos seguidos. En las
corridas largas (horas operando pegado a la capacidad) **sí abrió solo**: con
rachas de 5 fallos en la frontera de las ~60 llamadas/s, y siempre se
recuperó en un solo ciclo de 2 s (H4). Considero que así las capas quedan bien
separadas: el limitador absorbe la degradación y el breaker solo corta ante
rachas reales. Para ejercitarlo también contra una falla dura, programé
`scripts/run.sh` para que detenga `inventory_service.py` (`OUTAGE_AT`,
`OUTAGE_FOR`).

### Evidencia de las transiciones

En la corrida del archivo completo detuve el servicio 45 s en pleno spike (del
s 100 al 145). Este es el extracto de `results/go04/full/consumer.log`:

```
21:38:08 BREAKER closed → open (5 fallos consecutivos)
21:38:10 BREAKER open → half-open (cooldown de 2s cumplido)
21:38:10 BREAKER half-open → open (prueba 1/3 en half-open falló)
   ... (un ciclo cada 2 s mientras el servicio está caído) ...
21:38:53 RECUPERACIÓN: levantando inventory_service.py         (consumer.log.outage)
21:38:55 BREAKER half-open → closed (3/3 pruebas OK en half-open)
```

En la línea de tiempo del mismo tramo (`timeline-*.csv`) comprobé que,
mientras el breaker estaba abierto, solo salieron **0–3 llamadas por segundo**
(las pruebas) y que rechazó ~60 intentos por ciclo. El buffer en memoria se
congeló (48,319) mientras el disco seguía creciendo con el spike, sin perder
nada:

```
t_s recibidos/s confirmados/s profundidad  memoria disco   breaker   límite llamadas/s
102   20000        0          243,884      48,319  195,565 open      15.5     10
103   19996        0          249,092      48,319  200,773 open      11.6      3
146   19995        0          473,378      48,319  425,059 open       2.0      2
147   19991        1          478,599      48,318  430,281 half-open  2.0      1
148   20004        2          483,847      48,316  435,531 closed     4.0      2
```

| Métrica del breaker (corrida `full`) | Valor |
| :--- | ---: |
| Veces que abrió | **30** (por 5 fallos consecutivos: 8; reaperturas por prueba fallida en half-open: 22, todas durante la caída de 45 s) |
| Cierres tras half-open | **8** (cada apertura desde CLOSED terminó en recuperación) |
| Tiempo total abierto | **1 min 0 s** (half-open: 1.8 s) en 5 h 33 min de corrida |
| Llamadas rechazadas por el breaker | 2,290 (intentos que no salieron; ningún ítem se soltó) |
| Racha máxima de fallos | 5 (nunca más: el breaker corta justo en el umbral) |

Clasifiqué las 8 aperturas desde CLOSED: **1** fue por la caída que inyecté
(21:38:08), **1** ocurrió al reanudar tras la suspensión del equipo
(23:23:45, H8) y **6 fueron orgánicas**, sin ninguna intervención (22:15:04,
00:24:30, 00:25:59, 01:11:54, 01:13:52, 01:23:19): rachas reales de 5 fallos
mientras el limitador operaba pegado a la capacidad. Todas cerraron en el
primer half-open, a los 2 s.

También repetí una caída de 20 s bajo el race detector: obtuve 8 aperturas,
1 cierre, 16 s abierto y **0 data races** (`results/go04/race-pipeline/`).

---

## 4. El spike de 10× sin pérdida: evidencia

### Corrida

```bash
OUTAGE_AT=100 OUTAGE_FOR=45 scripts/run.sh full              # local:  results/go04/full/
docker compose -f docker-compose.e2e.yml up -d --build      # Docker: results/go04/full-docker/
```

Las cifras de esta sección vienen de la corrida local (binarios de Windows,
servicio en el host). Lancé en paralelo la misma corrida en Docker (todo en
contenedores Linux, con su propio NATS aislado) como **reproducción
independiente**, y obtuve exactamente lo mismo:

| | Local (`full`) | Docker (`full-docker`) |
| :--- | ---: | ---: |
| Spike real | 20,000 ev/s × 2 min 0 s (10.00×) | 20,000 ev/s × 2 min 0 s (10.00×) |
| Eventos procesados (esperado 3,035,188) | **3,035,188** | **3,035,188** |
| Inventario confirmado (esperado 815,590) | **815,590** | **815,590** |
| Duplicados (esperado 91,562) | 91,562 | 91,562 |
| Agregado según `verify` | idéntico al centavo | idéntico al centavo |
| Chequeos del reporte | 8/8 OK | 8/8 OK |
| Profundidad máx. del buffer / pasaron por disco | 796,669 / 763,035 | 797,700 / 763,351 |
| Breaker: aperturas (desde CLOSED) / cierres / tiempo abierto | 30 (8) / 8 / 1 min 0 s | 29 (7) / 7 / 58 s |
| Llamadas con 503 | 12,689 (1.5 %) | 12,715 (1.5 %) |

En Docker, de las 7 aperturas desde CLOSED, 1 fue por la caída que inyecté y
6 fueron orgánicas. Las dos corridas sobrevivieron suspensiones del equipo
(la de Docker, además, una de 4 h 16 min, de ~03:45 a 08:01); por eso su
espera máxima en el buffer es de 9 h 42 min de reloj de pared. Guardé la
evidencia en `results/go04/full-docker/` (incluido `verify.txt`).

### Pacing del productor (tasa real medida, `producer.log`)

| Fase | Objetivo | Líneas | Duración | Real ev/s |
| :--- | ---: | ---: | ---: | ---: |
| normal | 2,000 | 119,999 | 1 min 0 s | **2,000** |
| **spike** | 20,000 | 2,399,999 | **2 min 0 s** | **20,000** |
| normal-post | 2,000 | 606,752 | 5 min 3 s | **2,000** |

Logré un **spike de 10.00× sostenido durante 2 min 0 s**. El productor
publicó 3,126,750 líneas, sin reintentos y sin descartes del broker. En
`producer-*.csv` dejé la tasa publicada segundo a segundo.

### El sistema durante y después del spike (`timeline-*.csv`)

| Fase | Recibidos/s (media) | Latencia e2e media | Máx. e2e en 1 s | Buffer máx. | Heap máx. |
| :--- | ---: | ---: | ---: | ---: | ---: |
| normal (s 1–60) | 1,987 | 0.50 ms | 11.7 ms | 28,835 | 18 MiB |
| **spike (s 61–180)** | **19,942** | **0.82 ms** | 76.6 ms | 650,672 | 217 MiB |
| normal-post (s 181–484) | 2,022 | 0.59 ms | 102.5 ms | 796,655 | 297 MiB |
| drenado (s 485–19,988) | — | — | — | baja a 0 | 138 MiB |

- El **pipeline principal** absorbió el spike a su ritmo: aplicó los
  3,126,750 eventos en 8 min, a la par del productor, y su latencia e2e media
  apenas pasó de 0.50 a 0.82 ms. La caída del servicio de inventario **no lo
  afectó en nada**, porque el buffer los mantiene desacoplados.
- Atrasé la **confirmación de inventario**, tal como lo decidí: el buffer
  creció hasta **796,655** (s 484, al terminar el stream), desbordó a disco a
  los 4 s de empezar el spike (21:37:32) y bajó a 0 al ritmo que aguanta el
  servicio (**47.6 confirmaciones/s** sostenidas, sin contar la suspensión de
  H8).
- Verifiqué que el heap creció por la ventana de dedup de Go03 (3M llaves ×
  ~51 B ≈ 156 MB) y no por el buffer: su parte en memoria es fija
  (50,000 × 56 B ≈ 2.8 MB) y el resto vivió en disco.

### Conteo final contra lo esperado

| | Esperado (oráculo / Go03) | Obtenido | |
| :--- | ---: | ---: | :--- |
| Líneas publicadas → primeras entregas | 3,126,750 | **3,126,750** | ✓ |
| Duplicados descartados | 91,562 | **91,562** | ✓ |
| Eventos únicos aplicados | 3,035,188 | **3,035,188** | ✓ |
| `inventory_reserved` encolados | 815,590 | **815,590** (local y Docker) | ✓ |
| `inventory_reserved` **confirmados** | 815,590 | **815,590** | ✓ |
| **Eventos procesados** | **3,035,188** | **3,035,188** | ✓ |
| Profundidad final del buffer | 0 | **0** | ✓ |
| Re-entregas del broker | 0 | 0 | ✓ |
| Agregado por país (`verify`) | idéntico al centavo | **idéntico al centavo** en los 5 países y el total | ✓ |

Los 8 chequeos automáticos del reporte salieron **8/8 OK**. Esta es la salida
de `verify` (`results/go04/full/verify.txt`):

```
OK duplicados: 91562 == 91562
OK inventario confirmado: 815590 == 815590
OK eventos procesados: 3035188 == 3035188
OK agregado idéntico al centavo en los 5 países y el total
```

En total hice 828,401 llamadas al servicio (815,590 con 200; 12,689 con 503,
un 1.5 %; 122 errores de conexión durante la caída; 0 timeouts). 11,649 ítems
necesitaron más de un intento, y todos terminaron confirmados.

**Qué prueba qué.** Los chequeos del reporte son contabilidad *interna* de mi
consumidor: `processed == unique_applied` equivale por construcción a
`confirmed == enqueued`, es decir, prueba que el buffer no soltó nada. La
prueba *independiente* es `cmd/verify`: lo escribí para que recalcule la
verdad desde el archivo, sin broker ni ventana, y la compare contra el
reporte. Que el inventario confirmado sea igual a 815,590 **según el
oráculo**, y que el agregado coincida al centavo, es lo que demuestra que no
hubo pérdida ni doble conteo.

### Lo que me costó (y decidí pagar)

- La espera en el buffer fue de **3 h 8 min** en la mediana, **5 h 12 min** en
  el p95 y **5 h 25 min** como máximo (incluye los 46 min de la suspensión del
  equipo, H8). Considero que es el precio inevitable de un servicio que
  aguanta ~60 llamadas/s frente a una demanda de 5,200/s en el spike: **el
  backlog drena en backlog ÷ capacidad, y ningún diseño lo baja sin operar la
  dependencia en su zona degradada** (§2, ablación).
- El drenado total tardó **5 h 25 min** (4 h 39 min sin la suspensión) para
  las 796,669 verificaciones pendientes al terminar el stream, y la corrida
  completa, 5 h 33 min. El pipeline principal, en cambio, terminó en 8 min
  (483 s), con una latencia e2e de p50 0.56 ms / p95 1.12 ms / p99 2.11 ms
  sobre los 3,126,750 mensajes.

---

## 5. Bitácora de hallazgos

### H1: `localhost` suma 200 ms a cada llamada en Windows

Al probar el servicio con `curl`, noté que `localhost:8090` tardaba ~230 ms y
`127.0.0.1:8090` ~25 ms. Investigué y descubrí que el servicio escucha solo en
IPv4, mientras que `localhost` intenta `::1` primero. Como el servicio
responde con HTTP/1.0 (cierra la conexión en cada respuesta), habría pagado
ese costo **en cada llamada**: 10× la latencia sana del servicio. Lo resolví
haciendo que `-inventory-url` use `http://127.0.0.1:8090` por defecto.

### H2: el desborde a disco perdía datos tras el primer vaciado

Escribí `TestConcurrentNoLossNoDup` (8 productores, 4 consumidores, buffer
chico) y la prueba entregó solo **8,153 de 40,000** ítems. Al depurarlo,
encontré la causa: cuando el disco se vaciaba, truncaba el archivo a 0, pero
`Truncate` **no mueve la posición de escritura**, así que lo siguiente se
escribía en el offset viejo (con un hueco de ceros delante) mientras el lector
lo buscaba en 0. Mi prueba secuencial no lo había detectado porque nunca
vaciaba el disco y volvía a desbordar. Lo resolví con un `Seek(0)` después de
truncar y agregué una prueba de regresión
(`TestSecondSpillEpisodeAfterTruncate`). Sin la prueba concurrente, este bug
habría aparecido como una "pérdida marginal" en el archivo completo.

### H3: el limitador recortaba dos veces por la misma sobrecarga

En la primera corrida observé que la tasa caía 62 → 46.5 → 34.9 en dos
ventanas seguidas (un serrucho de 35 a 62). Analicé la línea de tiempo y
entendí que los 503 de llamadas que salieron con la tasa vieja llegaban en la
ventana siguiente y provocaban un segundo recorte. Lo resolví haciendo que el
limitador ignore los resultados de llamadas emitidas antes del último recorte
(`TestIgnoresResultsFromBeforeTheLastCut`).

### H4: el breaker abre solo, pero rara vez, y considero que es correcto

En las corridas cortas, con el limitador activo, la racha máxima de fallos fue
de 3–4 y el breaker nunca abrió. Consideré hacerlo más sensible (3 fallos, o
contar las llamadas lentas como fallo), pero lo descarté porque eso lo haría
abrir en degradación leve, donde lo correcto es bajar la tasa y no cortar.
**Decidí mantener el umbral en 5.** Las corridas largas confirmaron el
diseño: tras horas de AIMD pegado a la capacidad aparecieron rachas de 5
fallos reales, y el breaker abrió sin ninguna caída inyectada (6 veces en cada
corrida completa; la primera, en `full`, a las 22:15:04 hora local, y en
`full-docker` a las 04:23:10 UTC, en pleno spike). Siempre cerró en el primer
half-open (3/3 OK a los 2 s). En resumen: abre poco, ante rachas reales, y se
recupera enseguida. Con la caída inyectada (`OUTAGE_AT`) cubro el caso de
falla dura.

### H5: sin limitador drené más rápido

Lo detallo en el §2 (ablación): obtuve 1.7× más confirmaciones/s a cambio de
19 % de 503 y 15× la latencia del servicio. Lo documento como una elección
consciente, no como un error.

### H6: `python3` en Windows es un alias que no ejecuta nada

En una de mis primeras pruebas, `scripts/run.sh` se quedó esperando para
siempre a un servicio que nunca arrancó. Descubrí que `command -v python3`
encuentra el alias de la Microsoft Store, que no ejecuta nada. Lo resolví
haciendo que el script elija el primer intérprete que realmente ejecuta
`import sys`, y le agregué un timeout de 10 s a la espera del servicio.

### H7: `DiscardOld` era una pérdida latente que heredé de Go03

Al diseñar la cadena de backpressure, me di cuenta de que si el consumidor
deja de pedir mensajes y el stream se llena, `DiscardOld` borra mensajes no
consumidos. Lo resolví cambiando a `DiscardNew` y quitando el límite de
reintentos del productor (§1).

### H8: mis dos corridas sobrevivieron suspensiones del equipo

No lo planeé. Durante el drenado, mi máquina se suspendió de 22:37 a 23:23
(hora local) y las dos corridas se congelaron: el log de cada una tiene un
hueco de ~2,775 s. Al reanudar, la corrida local registró esto:

```
23:23:45 drenando 0.5 confirmaciones/s ...  confirmados=171349/815590  breaker=closed
23:23:45 BREAKER closed → open (5 fallos consecutivos)       # llamadas en vuelo rotas por la suspensión
23:23:47 BREAKER open → half-open (cooldown de 2s cumplido)
23:23:47 BREAKER half-open → closed (3/3 pruebas OK en half-open)
23:24:45 drenando 47.7 confirmaciones/s ...  confirmados=173845/815590
```

El breaker trató las llamadas que la suspensión dejó rotas como una falla real
y se recuperó en un ciclo; en un minuto, el drenado volvió a la capacidad
sana. El buffer en disco quedó intacto (613,035 antes y después). La corrida
de Docker reanudó sin abrir el breaker, y más tarde sobrevivió una segunda
suspensión de 4 h 16 min (de ~03:45 a 08:01) con el mismo resultado: siguió
drenando y terminó con el conteo exacto. **Efecto en las cifras:** los tiempos
totales y la espera máxima en el buffer incluyen esos periodos.

---

## 6. Limitaciones y lo que haría en una siguiente iteración

- **Recuperación del limitador tras una caída larga:** cada prueba fallida en
  half-open cuenta como ventana con fallo, así que la tasa queda en el mínimo
  (2/s) y tarda ~25 s en volver a ~55/s (+2/s). Calculo que cuesta del orden
  de cientos de confirmaciones de atraso por caída. En una siguiente
  iteración, restauraría la tasa previa a la apertura al cerrar el breaker.
- **El buffer no sobrevive a una caída del consumidor** (§1): lo reconstruyo
  re-consumiendo el stream. Para streams sin fin, persistiría el conjunto de
  confirmados junto con la secuencia.
- **El servicio de inventario es el límite real:** ~50 confirmaciones/s
  sostenidas. Considero que, para ir más rápido sin dañarlo, habría que
  cambiar el contrato del servicio (verificación por lotes, o caché con TTL
  por SKU si los eventos lo tuvieran), no el cliente.
- **La espera que mido no incluye el tiempo en el broker.**
  `wait_enqueue_to_confirmed` cuenta desde que el ítem entra al buffer. En el
  nivel `blocked`, el evento ya esperó antes en JetStream, y no sumo ese
  tiempo (por eso `blocked-buffer` reporta una espera máxima de solo 58 s).
  La espera de punta a punta real sería `Pub-Ts → confirmado`; como el
  `PubTs` ya viaja en cada ítem, agregarla me resultaría directo.
- **Reintento del productor fuera de la ventana del broker.** Si un PubAck
  fallara y el reintento llegara después de `Duplicates` = 1 min, el broker lo
  aceptaría y mi consumidor lo contaría como reintento del gateway (inflaría
  los 91,562). No ocurrió (0 reintentos en todas mis corridas). Para cerrarlo,
  alargaría `Duplicates` o marcaría los reintentos del productor con un
  header.
- **Ritmo normal de 2,000 ev/s** en vez de 20,000 (§0): lo limitan el tamaño
  del archivo y el techo del broker en mi máquina. Con un dataset más grande y
  un broker escalado, repetiría la prueba con el ritmo literal.
