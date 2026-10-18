import csv
import json
import os
import uuid

import pytest

from crawlforge.deadletter import DeadLetterQueue, RejectsSink
from crawlforge.http import Request
from crawlforge.sinks import CsvSink, JsonlSink, sink_from_spec


async def test_jsonl_writes_and_appends(tmp_path):
    path = tmp_path / "out" / "items.jsonl"
    async with JsonlSink(path) as sink:
        await sink.write({"title": "Ünïcode", "tags": ["a"]})
        await sink.write({"title": "two"})
    async with JsonlSink(path, append=True) as sink:
        await sink.write({"title": "three"})
    rows = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
    assert [r["title"] for r in rows] == ["Ünïcode", "two", "three"]


async def test_jsonl_without_append_truncates(tmp_path):
    path = tmp_path / "items.jsonl"
    path.write_text('{"old": true}\n')
    async with JsonlSink(path) as sink:
        await sink.write({"new": True})
    assert path.read_text().splitlines() == ['{"new": true}']


async def test_csv_header_nested_values_and_resume(tmp_path):
    path = tmp_path / "items.csv"
    async with CsvSink(path) as sink:
        await sink.write({"title": "a", "price": 1.5, "tags": ["x", "y"]})
        await sink.write({"title": "b", "price": 2, "extra": "dropped"})
    async with CsvSink(path, append=True) as sink:
        await sink.write({"price": 3, "title": "c"})
    with path.open(newline="") as fh:
        rows = list(csv.DictReader(fh))
    assert [r["title"] for r in rows] == ["a", "b", "c"]
    assert json.loads(rows[0]["tags"]) == ["x", "y"]
    assert "extra" not in rows[1]
    assert path.read_text().count("title,price,tags") == 1


async def test_dead_letter_and_rejects_records(tmp_path):
    dlq_path, rej_path = tmp_path / "dlq.jsonl", tmp_path / "rejects.jsonl"
    async with JsonlSink(dlq_path) as dlq_sink, JsonlSink(rej_path) as rej_sink:
        dlq = DeadLetterQueue(dlq_sink)
        req = Request("http://x.test/p", callback="parse_page", retries=3)
        await dlq.put(req, "max retries exceeded", status=503)
        rejects = RejectsSink(rej_sink)
        await rejects.put(
            {"price": "abc"},
            [{"loc": ["price"], "msg": "bad"}],
            url=req.url,
            spider="s",
            schema="Book",
        )
    entry = json.loads(dlq_path.read_text())
    assert entry["reason"] == "max retries exceeded"
    assert entry["status"] == 503 and entry["attempts"] == 4
    assert Request.from_dict(entry["request"]) == req
    rej = json.loads(rej_path.read_text())
    assert rej["schema"] == "Book" and rej["item"] == {"price": "abc"}


def test_sink_from_spec(tmp_path):
    assert isinstance(sink_from_spec(f"jsonl:{tmp_path}/a.jsonl"), JsonlSink)
    assert isinstance(sink_from_spec(f"csv:{tmp_path}/a.csv"), CsvSink)
    pg = sink_from_spec("postgresql://u:p@db:5432/crawl?table=books&key=upc&sslmode=disable")
    assert pg.table == "books" and pg.key == "upc"
    assert pg.dsn == "postgresql://u:p@db:5432/crawl?sslmode=disable"
    for bad in ("parquet:x", "jsonl", "nope"):
        with pytest.raises(ValueError):
            sink_from_spec(bad)
    with pytest.raises(ValueError):
        sink_from_spec("postgresql://h/db?table=bad;drop")


@pytest.mark.skipif(not os.environ.get("TEST_DATABASE_URL"), reason="TEST_DATABASE_URL not set")
async def test_postgres_upsert_on_key():
    import asyncpg

    from crawlforge.sinks.postgres import PostgresSink

    dsn = os.environ["TEST_DATABASE_URL"]
    table = f"cf_test_{uuid.uuid4().hex[:8]}"
    sink = PostgresSink(dsn, table=table, key="upc", batch_size=2)
    try:
        async with sink:
            await sink.write({"upc": "A", "price": 1})
            await sink.write({"upc": "B", "price": 2})  # triggers a batch flush
            await sink.write({"upc": "A", "price": 10})  # upsert of an existing row
        conn = await asyncpg.connect(dsn)
        try:
            rows = await conn.fetch(
                f"SELECT key, data->>'price' AS price FROM {table} ORDER BY key"
            )
            assert [(r["key"], r["price"]) for r in rows] == [("A", "10"), ("B", "2")]
        finally:
            await conn.execute(f"DROP TABLE IF EXISTS {table}")
            await conn.close()
    finally:
        await sink.close()


async def test_postgres_rejects_item_without_key():
    from crawlforge.sinks.postgres import PostgresSink

    sink = PostgresSink("postgresql://unused/db", key="upc")
    with pytest.raises(KeyError):
        await sink.write({"title": "no key"})
