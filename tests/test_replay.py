"""`crawlforge replay-dlq`: filtering, dry run, idempotency and end-to-end replays (no network)."""

import asyncio
import json
import re
from collections import Counter
from datetime import UTC, datetime, timedelta

import fakeredis
import pytest
from typer.testing import CliRunner

from crawlforge import cli
from crawlforge.checkpoint import Checkpoint
from crawlforge.http import Request
from crawlforge.queues.redis import RedisQueue
from crawlforge.replay import DlqFilter, parse_since, plan_replay, read_dlq
from test_engine import CatalogSpider, catalog_handler, html, read_jsonl

NOW = datetime(2026, 10, 19, 12, 0, tzinfo=UTC)


def entry(url, reason, *, ts=NOW, retries=2, **extra):
    request = Request(url, callback="parse_product", depth=1, retries=retries).to_dict()
    return {"ts": ts.isoformat(), "reason": reason, "status": None, "request": request, **extra}


def write_dlq(path, entries, *, raw=()):
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [json.dumps(e) for e in entries] + list(raw)
    path.write_text("\n".join(lines) + "\n")


def squashed(output):
    """Error text with ANSI codes, Rich box borders and line wrapping removed."""
    return re.sub(r"\x1b\[[0-9;]*m|[\s│╭╮╰╯─]", "", output)


def replay(*args):
    return CliRunner().invoke(cli.app, ["replay-dlq", *map(str, args)])


SAMPLE = [
    entry("http://shop.test/p/1", "max_retries_exceeded:http_503", ts=NOW - timedelta(hours=5)),
    entry("http://shop.test/p/2", "http_404", ts=NOW - timedelta(hours=1)),
    entry("http://cdn.shop.test:8080/a", "fetch_error:ConnectError", ts=NOW - timedelta(minutes=5)),
    entry("http://other.test/x", "callback_error:KeyError", ts=NOW - timedelta(minutes=1)),
]


def selected_urls(tmp_path, flt, **kwargs):
    path = tmp_path / "dlq.jsonl"
    write_dlq(path, SAMPLE)
    plan = plan_replay(read_dlq(path), flt, **kwargs)
    return [line.request.url for line in plan.selected]


# ------------------------------------------------------------------ filtering


def test_filter_by_reason_substring_and_regex(tmp_path):
    assert selected_urls(tmp_path, DlqFilter(reason="http_")) == [
        "http://shop.test/p/1",
        "http://shop.test/p/2",
    ]
    assert selected_urls(tmp_path, DlqFilter(reason=r"^(http_4\d\d|callback_)", regex=True)) == [
        "http://shop.test/p/2",
        "http://other.test/x",
    ]
    # Without --regex the pattern is taken literally.
    assert selected_urls(tmp_path, DlqFilter(reason=r"^http_4\d\d")) == []


def test_filter_by_domain_includes_subdomains_and_ignores_port(tmp_path):
    urls = selected_urls(tmp_path, DlqFilter(domains=["shop.test"]))
    assert urls == ["http://shop.test/p/1", "http://shop.test/p/2", "http://cdn.shop.test:8080/a"]
    assert selected_urls(tmp_path, DlqFilter(domains=["CDN.shop.test", "other.test"])) == [
        "http://cdn.shop.test:8080/a",
        "http://other.test/x",
    ]


def test_filter_since_limit_and_combined_criteria(tmp_path):
    since = parse_since("2h", now=NOW)
    assert selected_urls(tmp_path, DlqFilter(since=since)) == [
        "http://shop.test/p/2",
        "http://cdn.shop.test:8080/a",
        "http://other.test/x",
    ]
    assert selected_urls(tmp_path, DlqFilter(since=since), limit=1) == ["http://shop.test/p/2"]
    combined = DlqFilter(reason="Error", domains=["shop.test"], since=since)
    assert selected_urls(tmp_path, combined) == ["http://cdn.shop.test:8080/a"]


def test_parse_since_formats():
    assert parse_since("30m", now=NOW) == NOW - timedelta(minutes=30)
    assert parse_since("1.5d", now=NOW) == NOW - timedelta(hours=36)
    assert parse_since("2026-10-18T08:00:00") == datetime(2026, 10, 18, 8, tzinfo=UTC)
    assert parse_since("2026-10-18T08:00:00+02:00").utcoffset() == timedelta(hours=2)
    with pytest.raises(ValueError, match="invalid --since"):
        parse_since("yesterday")


def test_plan_skips_replayed_queued_duplicate_and_unreadable(tmp_path):
    path = tmp_path / "dlq.jsonl"
    write_dlq(
        path,
        [
            entry("http://a.test/1", "http_500", replayed_at="2026-10-18T00:00:00+00:00"),
            entry("http://a.test/2", "http_500"),
            entry("http://a.test/2", "http_500"),  # same request dead-lettered twice
            entry("http://a.test/3", "http_500"),
            entry("http://a.test/4", "http_500"),
        ],
        raw=["{not json", '{"reason": "http_500"}'],
    )
    queued = {Request("http://a.test/3").fingerprint}
    plan = plan_replay(read_dlq(path), DlqFilter(reason="500"), queued=queued)
    assert [line.request.url for line in plan.selected] == ["http://a.test/2", "http://a.test/4"]
    assert plan.total == 7 and plan.unreadable == 2 and len(plan.matched) == 5
    assert plan.skipped == {"already replayed": 1, "duplicate entry": 1, "already queued": 1}


# ------------------------------------------------------------------ CLI: dry run, idempotency


def test_dry_run_changes_nothing(tmp_path):
    dlq, ckpt = tmp_path / "dlq.jsonl", tmp_path / "c.json"
    write_dlq(dlq, SAMPLE)
    before = dlq.read_bytes()
    result = replay(
        "--dlq", dlq, "--checkpoint", ckpt, "--spider", "catalog", "--reason", "http_", "--dry-run"
    )
    assert result.exit_code == 0, result.output
    assert "would replay  http_404  http://shop.test/p/2" in result.output
    assert "matched   2" in result.output and "to replay 2" in result.output
    assert "dry run: nothing was changed" in result.output
    assert dlq.read_bytes() == before and not ckpt.exists()


def test_replay_twice_is_idempotent(tmp_path):
    dlq, ckpt = tmp_path / "dlq.jsonl", tmp_path / "c.json"
    write_dlq(dlq, SAMPLE, raw=["garbage line"])
    args = ["--dlq", dlq, "--checkpoint", ckpt, "--spider", "catalog", "--domain", "shop.test"]

    first = replay(*args)
    assert first.exit_code == 0, first.output
    assert "replayed  3 -> checkpoint:" in first.output and "skipped   0" in first.output
    data = Checkpoint(ckpt).load(spider="catalog")
    pending = data["frontier"]["pending"]
    assert [p["url"] for p in pending] == [e["request"]["url"] for e in SAMPLE[:3]]
    # Fresh retry budget, everything else (callback, depth) preserved.
    assert {p["retries"] for p in pending} == {0}
    assert {p["callback"] for p in pending} == {"parse_product"}
    assert set(data["frontier"]["seen"]) == {Request(p["url"]).fingerprint for p in pending}

    lines = dlq.read_text().splitlines()
    assert "garbage line" in lines  # unreadable lines are kept verbatim
    marked = [json.loads(line) for line in lines if line.startswith("{")]
    assert [bool(e.get("replayed_at")) for e in marked] == [True, True, True, False]
    assert marked[0]["replayed_to"] == f"checkpoint:{ckpt}"

    second = replay(*args)
    assert second.exit_code == 0, second.output
    assert "matched   3" in second.output and "replayed  0" in second.output
    assert "skipped   3 (already replayed: 3)" in second.output
    assert Checkpoint(ckpt).load(spider="catalog")["frontier"]["pending"] == pending


def test_replay_appends_to_existing_checkpoint_of_same_spider(tmp_path):
    dlq, ckpt = tmp_path / "dlq.jsonl", tmp_path / "c.json"
    write_dlq(dlq, SAMPLE[:2])
    stopped = Request("http://shop.test/page/9").to_dict()
    Checkpoint(ckpt).save(
        spider="catalog",
        frontier={"pending": [stopped], "seen": ["abc"]},
        stats={"requests": 7},
    )
    assert replay("--dlq", dlq, "--checkpoint", ckpt, "--spider", "other").exit_code == 2

    result = replay("--dlq", dlq, "--checkpoint", ckpt)  # spider taken from the checkpoint
    assert result.exit_code == 0, result.output
    data = Checkpoint(ckpt).load(spider="catalog")
    urls = [p["url"] for p in data["frontier"]["pending"]]
    assert urls == ["http://shop.test/page/9", "http://shop.test/p/1", "http://shop.test/p/2"]
    assert "abc" in data["frontier"]["seen"] and data["stats"] == {"requests": 7}


def test_replay_requires_a_persistent_target(tmp_path):
    dlq = tmp_path / "dlq.jsonl"
    write_dlq(dlq, SAMPLE)
    result = replay("--dlq", dlq, "--spider", "catalog")
    assert result.exit_code == 2 and "thememoryqueue" in squashed(result.output)
    no_spider = replay("--dlq", dlq, "--checkpoint", tmp_path / "new.json")
    assert no_spider.exit_code == 2 and "--spiderisrequired" in squashed(no_spider.output)
    assert replay("--dlq", tmp_path / "missing.jsonl").exit_code == 1
    bad = replay("--dlq", dlq, "--checkpoint", tmp_path / "c.json", "-s", "x", "--since", "soon")
    assert bad.exit_code == 2 and "invalid--since" in squashed(bad.output)


# ------------------------------------------------------------------ end to end


def flaky_catalog(hits, broken):
    """Catalog site where paths in ``broken`` answer 503 until removed from the set."""
    ok = catalog_handler(hits)

    def handler(request):
        if request.url.path in broken:
            hits[request.url.path] += 1
            return html("maintenance", 503)
        return ok(request)

    return handler


async def test_end_to_end_replay_through_checkpoint(make_crawler, tmp_path):
    hits, broken = Counter(), {"/p/2"}
    ckpt = tmp_path / "catalog.ckpt.json"
    first = make_crawler(
        CatalogSpider(), flaky_catalog(hits, broken), checkpoint_path=ckpt, max_retries=1
    )
    stats = await first.run()
    assert stats.finish_reason == "finished" and stats.dead_letters == 1
    assert not ckpt.exists()  # a finished crawl removes its checkpoint
    dlq = first.settings.resolved("dlq")
    assert read_jsonl(dlq)[0]["reason"] == "max_retries_exceeded:http_503"

    result = replay("--dlq", dlq, "--checkpoint", ckpt, "--spider", "catalog", "--reason", "503")
    assert result.exit_code == 0, result.output
    assert "replayed  1" in result.output

    broken.clear()  # "the fix"
    hits.clear()
    second = make_crawler(
        CatalogSpider(), flaky_catalog(hits, broken), checkpoint_path=ckpt, resume=True
    )
    stats = await second.run()
    assert stats.resumed and stats.finish_reason == "finished" and stats.dead_letters == 0
    assert hits == {"/p/2": 1}  # only the replayed page is fetched again
    # The sink is appended to on resume: the first run's items plus the replayed one.
    assert sorted(i["name"] for i in read_jsonl(second.test_sink_path)) == ["One", "Three", "Two"]
    assert not ckpt.exists()
    # The old entry stays in the DLQ, marked; replaying again does nothing.
    assert read_jsonl(dlq)[0]["replayed_at"]
    again = replay("--dlq", dlq, "--checkpoint", ckpt, "--spider", "catalog")
    assert "replayed  0" in again.output and not ckpt.exists()


def test_end_to_end_replay_through_redis(make_crawler, tmp_path, monkeypatch):
    # Sync test: the CLI runs its own event loop for the Redis push.
    server = fakeredis.FakeServer()
    created = []

    def queue(spec="redis://fake", prefix="crawlforge:catalog"):
        created.append((spec, prefix))
        return RedisQueue(fakeredis.FakeAsyncRedis(server=server), prefix=prefix)

    def size():
        return asyncio.run(queue().size())

    hits, broken = Counter(), {"/p/1", "/p/3"}
    first = make_crawler(CatalogSpider(), flaky_catalog(hits, broken), queue=queue(), max_retries=0)
    stats = asyncio.run(first.run())
    assert stats.dead_letters == 2 and size() == 0
    dlq = first.settings.resolved("dlq")

    monkeypatch.setattr(cli, "queue_from_spec", queue)
    args = ["--dlq", dlq, "--queue", "redis://fake", "--spider", "catalog"]
    result = replay(*args, "--limit", "1")
    assert result.exit_code == 0, result.output
    assert "replayed  1 -> queue:redis://fake" in result.output
    assert "skipped   1 (over --limit: 1)" in result.output
    assert created[-1] == ("redis://fake", "crawlforge:catalog")
    assert size() == 1
    result = replay(*args)
    assert "replayed  1" in result.output and "already replayed: 1" in result.output
    assert size() == 2
    assert "replayed  0" in replay(*args).output and size() == 2

    broken.clear()
    hits.clear()
    second = make_crawler(CatalogSpider(), flaky_catalog(hits, broken), queue=queue(), resume=True)
    stats = asyncio.run(second.run())
    assert stats.resumed and stats.dead_letters == 0
    assert hits == {"/p/1": 1, "/p/3": 1}
    assert sorted(i["name"] for i in read_jsonl(second.test_sink_path)) == ["One", "Three", "Two"]
