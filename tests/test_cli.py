"""CLI tests against a throwaway HTTP server on 127.0.0.1 (no internet)."""

import json
import socket
import textwrap
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest
import typer
from typer.testing import CliRunner

from crawlforge.cli import app, load_spider
from crawlforge.metrics import Metrics

PAGES = {
    "/robots.txt": "User-agent: *\nDisallow: /admin/\n",
    "/": "<a href='/item/1'>1</a><a href='/item/2'>2</a><a href='/admin/x'>admin</a>",
    "/item/1": "<h1>Alpha</h1>",
    "/item/2": "<h1>Beta</h1>",
}


class _Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        body = PAGES.get(self.path)
        self.send_response(200 if body is not None else 404)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.end_headers()
        self.wfile.write((body or "not found").encode())

    def log_message(self, *args):
        pass


@pytest.fixture
def local_site():
    server = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield f"http://127.0.0.1:{server.server_address[1]}"
    server.shutdown()
    server.server_close()


SPIDER = """
from pydantic import BaseModel
from crawlforge import Spider


class Entry(BaseModel):
    url: str
    title: str


class LocalSpider(Spider):
    name = "local"
    start_urls = ["{base}/"]
    item_model = Entry

    def parse(self, response):
        for href in response.css("a::attr(href)").getall():
            yield response.follow(href, callback=self.parse_item)

    def parse_item(self, response):
        yield {{"url": response.url, "title": response.css("h1::text").get()}}
"""


def write_spider(tmp_path, base):
    path = tmp_path / "local_spider.py"
    path.write_text(textwrap.dedent(SPIDER.format(base=base)))
    return path


def test_run_end_to_end_with_jsonl_and_csv(tmp_path, local_site):
    spider = write_spider(tmp_path, local_site)
    out, csv_out, state = tmp_path / "out.jsonl", tmp_path / "out.csv", tmp_path / "state"
    result = CliRunner().invoke(
        app,
        [
            "run", str(spider),
            "--sink", f"jsonl:{out}", "--sink", f"csv:{csv_out}",
            "--concurrency", "4", "--rate", "50", "--output-dir", str(state),
            "--set", "burst=10", "--log-level", "WARNING",
        ],
    )  # fmt: skip
    assert result.exit_code == 0, result.output
    titles = sorted(json.loads(line)["title"] for line in out.read_text().splitlines())
    assert titles == ["Alpha", "Beta"]
    assert csv_out.read_text().splitlines()[0] == "url,title"
    stats = json.loads((state / "stats.json").read_text())
    assert stats["items"] == 2 and stats["robots_blocked"] == 1
    assert stats["finish_reason"] == "finished"

    shown = CliRunner().invoke(app, ["stats", "--file", str(state / "stats.json")])
    assert shown.exit_code == 0
    assert "robots_blocked" in shown.output and "finished" in shown.output


def test_stats_from_live_metrics_endpoint():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    metrics = Metrics()
    metrics.items.labels("books").inc(5)
    metrics.serve(port, addr="127.0.0.1")
    result = CliRunner().invoke(app, ["stats", "--metrics-url", f"http://127.0.0.1:{port}/metrics"])
    assert result.exit_code == 0, result.output
    assert "crawlforge_items_total{spider=books}" in result.output
    assert " 5" in result.output


def test_stats_without_file_fails(tmp_path):
    result = CliRunner().invoke(app, ["stats", "--file", str(tmp_path / "missing.json")])
    assert result.exit_code == 1


def test_load_spider_selection(tmp_path):
    path = tmp_path / "two.py"
    path.write_text(
        "from crawlforge import Spider\n"
        "class A(Spider):\n    name = 'a'\n"
        "class B(Spider):\n    name = 'b'\n"
    )
    assert load_spider(path, "b").name == "b"
    assert load_spider(path, "A").name == "a"
    with pytest.raises(typer.BadParameter, match="several spiders"):
        load_spider(path)
    with pytest.raises(typer.BadParameter, match="no Spider"):
        load_spider(path, "zzz")


def test_version_command():
    result = CliRunner().invoke(app, ["version"])
    assert result.exit_code == 0 and result.output.strip() == "0.1.0"
