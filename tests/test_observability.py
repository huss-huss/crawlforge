import json
import logging
import socket
import urllib.request

from prometheus_client.parser import text_string_to_metric_families

from crawlforge.logs import JsonFormatter
from crawlforge.metrics import Metrics


def samples(metrics):
    out = {}
    for family in text_string_to_metric_families(metrics.render().decode()):
        for s in family.samples:
            out[(s.name, tuple(sorted(s.labels.items())))] = s.value
    return out


def test_metrics_are_isolated_per_instance():
    a, b = Metrics(), Metrics()
    a.requests.labels("x.test").inc()
    assert samples(a)[("crawlforge_requests_total", (("domain", "x.test"),))] == 1
    assert ("crawlforge_requests_total", (("domain", "x.test"),)) not in samples(b)


def test_metric_names_exposed():
    m = Metrics()
    m.responses.labels("x.test", "200").inc(3)
    m.latency.labels("x.test").observe(0.3)
    m.breaker_state.labels("x.test").set(2)
    s = samples(m)
    assert s[("crawlforge_responses_total", (("domain", "x.test"), ("status", "200")))] == 3
    assert s[("crawlforge_request_latency_seconds_count", (("domain", "x.test"),))] == 1
    assert s[("crawlforge_circuit_breaker_state", (("domain", "x.test"),))] == 2


def test_metrics_http_endpoint():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    m = Metrics()
    m.items.labels("books").inc()
    m.serve(port, addr="127.0.0.1")
    body = urllib.request.urlopen(f"http://127.0.0.1:{port}/metrics", timeout=5).read().decode()
    assert 'crawlforge_items_total{spider="books"} 1.0' in body


def test_json_formatter_includes_extra_fields():
    record = logging.makeLogRecord(
        {"name": "crawlforge.engine", "levelname": "INFO", "msg": "fetched %s", "args": ("u",)}
    )
    record.status = 200
    record.url = "http://x.test/"
    payload = json.loads(JsonFormatter().format(record))
    assert payload["msg"] == "fetched u"
    assert payload["status"] == 200 and payload["url"] == "http://x.test/"
    assert payload["level"] == "info" and "ts" in payload
