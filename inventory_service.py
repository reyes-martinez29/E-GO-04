"""
E-Go04 — Servicio simulado de verificacion de inventario
===========================================================

Este NO es codigo que resuelve el ejercicio -- es el ambiente contra el que
tu sistema se prueba. Simula un servicio externo real: rapido bajo carga
normal, degradado bajo carga alta, y con fallas duras cuando se satura.
Su comportamiento se mide sobre su PROPIA tasa de requests entrantes, asi
que es identico para todo el cohort sin importar que tan rapido sea el
codigo Go de cada quien.

Uso: python3 inventory_service.py
Escucha en: http://localhost:8090

Endpoints:
  POST /check-inventory   body: {"order_id": "...", "country": "..."}
                           -> 200 {"available": true} | 503 {"error": "overloaded"}
  GET  /stats              -> estado actual (rps ventana 1s, totales, fallas)
  POST /reset               -> limpia contadores internos (util entre corridas de prueba)

No hace falta conocer los umbrales exactos para disenar un sistema resiliente
-- estan documentados en los comentarios de abajo si los quieres consultar,
pero el ejercicio espera que tu backpressure/circuit breaker funcione sin
asumir un numero especifico.
"""

import json
import random
import threading
import time
from collections import deque
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

# Umbrales del servicio simulado (ventana deslizante de 1 segundo)
NORMAL_THRESHOLD_RPS = 60     # por debajo de esto: rapido, siempre exitoso
MAX_THRESHOLD_RPS = 250       # por encima de esto: mayoria de fallas, fast-fail

_lock = threading.Lock()
_timestamps = deque()
_total_requests = 0
_total_failures = 0


def _current_rps():
    now = time.monotonic()
    with _lock:
        while _timestamps and now - _timestamps[0] > 1.0:
            _timestamps.popleft()
        return len(_timestamps)


def _register_request():
    global _total_requests
    with _lock:
        _timestamps.append(time.monotonic())
        _total_requests += 1


def _handle_check(rps):
    """Devuelve (status_code, delay_seconds, body_dict)."""
    global _total_failures
    if rps <= NORMAL_THRESHOLD_RPS:
        return 200, random.uniform(0.01, 0.03), {"available": True}

    if rps <= MAX_THRESHOLD_RPS:
        overage = (rps - NORMAL_THRESHOLD_RPS) / (MAX_THRESHOLD_RPS - NORMAL_THRESHOLD_RPS)
        delay = 0.2 + overage * 0.6
        if random.random() < 0.15 + overage * 0.25:
            with _lock:
                _total_failures += 1
            return 503, delay, {"error": "overloaded"}
        return 200, delay, {"available": True}

    # por encima del umbral maximo: fast-fail la mayoria, exito lento el resto
    if random.random() < 0.70:
        with _lock:
            _total_failures += 1
        return 503, 0.02, {"error": "overloaded"}
    return 200, random.uniform(1.0, 2.0), {"available": True}


class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        pass  # silenciar logs default -- /stats ya da visibilidad

    def _send_json(self, status, payload):
        body = json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self):
        if self.path == "/check-inventory":
            length = int(self.headers.get("Content-Length", 0))
            _ = self.rfile.read(length)  # no se valida el payload, solo se mide carga
            _register_request()
            rps = _current_rps()
            status, delay, body = _handle_check(rps)
            time.sleep(delay)
            self._send_json(status, body)
        elif self.path == "/reset":
            global _total_requests, _total_failures
            with _lock:
                _timestamps.clear()
                _total_requests = 0
                _total_failures = 0
            self._send_json(200, {"reset": True})
        else:
            self._send_json(404, {"error": "not found"})

    def do_GET(self):
        if self.path == "/stats":
            self._send_json(200, {
                "current_rps": _current_rps(),
                "total_requests": _total_requests,
                "total_failures": _total_failures,
            })
        else:
            self._send_json(404, {"error": "not found"})


if __name__ == "__main__":
    server = ThreadingHTTPServer(("localhost", 8090), Handler)
    print("Servicio de inventario simulado corriendo en http://localhost:8090")
    print("POST /check-inventory · GET /stats · POST /reset")
    server.serve_forever()
