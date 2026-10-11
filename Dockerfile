# Imagen de la corrida end-to-end (docker-compose.e2e.yml): binarios de Go +
# inventory_service.py + scripts/run.sh en un solo contenedor. El servicio
# corre junto al consumidor para que run.sh pueda detenerlo y relanzarlo
# (OUTAGE_AT / OUTAGE_FOR), igual que en la corrida local.
FROM golang:1.26 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd ./cmd
COPY internal ./internal
RUN CGO_ENABLED=0 go build -o /out/ ./cmd/...

FROM python:3.13-alpine
RUN apk add --no-cache bash curl
WORKDIR /app
COPY --from=build /out/ ./bin/
COPY scripts ./scripts
COPY inventory_service.py ./
# Por si el repo se clonó en Windows con autocrlf.
RUN sed -i 's/\r$//' scripts/run.sh
ENTRYPOINT ["bash", "scripts/run.sh"]
