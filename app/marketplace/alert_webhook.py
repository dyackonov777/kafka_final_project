"""Приёмник вебхуков Alertmanager: печатает алерты в stdout.

Позволяет проверить цепочку Prometheus -> Alertmanager -> получатель без внешних сервисов:
    docker logs -f alert-webhook
"""
import json
import logging
import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from marketplace.config import setup_logging

log = logging.getLogger("alert-webhook")


class Handler(BaseHTTPRequestHandler):
    def do_POST(self) -> None:  # noqa: N802
        length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(length)
        try:
            payload = json.loads(body or b"{}")
            for alert in payload.get("alerts", []):
                labels = alert.get("labels", {})
                annotations = alert.get("annotations", {})
                log.warning("[%s] %s | severity=%s | instance=%s | %s",
                            alert.get("status", "?").upper(), labels.get("alertname"),
                            labels.get("severity"), labels.get("instance", "-"),
                            annotations.get("summary") or annotations.get("description") or "")
        except ValueError:
            log.error("Некорректный JSON от Alertmanager: %r", body[:200])
        self.send_response(200)
        self.end_headers()

    def do_GET(self) -> None:  # noqa: N802 — healthcheck
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"ok")

    def log_message(self, fmt, *args) -> None:  # тише стандартного access-лога
        pass


def main() -> None:
    setup_logging()
    port = int(os.getenv("PORT", "5001"))
    log.info("Жду алерты Alertmanager на :%d", port)
    ThreadingHTTPServer(("0.0.0.0", port), Handler).serve_forever()


if __name__ == "__main__":
    main()
