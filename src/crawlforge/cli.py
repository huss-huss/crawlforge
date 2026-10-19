"""Command-line interface: ``crawlforge run``, ``stats`` and ``replay-dlq``."""

from __future__ import annotations

import asyncio
import contextlib
import importlib.util
import json
import signal
import sys
from pathlib import Path
from typing import Annotated, Any

import typer

from crawlforge import __version__
from crawlforge.engine import Crawler
from crawlforge.logs import configure_logging
from crawlforge.queues import queue_from_spec
from crawlforge.settings import Settings
from crawlforge.sinks import sink_from_spec
from crawlforge.spider import Spider

app = typer.Typer(
    add_completion=False,
    no_args_is_help=True,
    help="CrawlForge: a resilient, observable data extraction framework.",
)


def load_spider(path: Path, name: str | None = None) -> type[Spider]:
    """Import ``path`` and return the Spider subclass defined in it."""
    if not path.exists():
        raise typer.BadParameter(f"{path} does not exist")
    module_name = f"crawlforge_user_{path.stem}"
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise typer.BadParameter(f"cannot import {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    spiders = [
        obj
        for obj in vars(module).values()
        if isinstance(obj, type)
        and issubclass(obj, Spider)
        and obj is not Spider
        and obj.__module__ == module_name
    ]
    if name is not None:
        spiders = [s for s in spiders if name in (s.name, s.__name__)]
    if not spiders:
        raise typer.BadParameter(
            f"no Spider subclass{f' named {name!r}' if name else ''} in {path}"
        )
    if len(spiders) > 1:
        names = ", ".join(sorted(s.name for s in spiders))
        raise typer.BadParameter(
            f"{path} defines several spiders ({names}); pick one with --spider"
        )
    return spiders[0]


def _parse_set(pairs: list[str]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for pair in pairs:
        key, sep, raw = pair.partition("=")
        if not sep:
            raise typer.BadParameter(f"--set expects key=value, got {pair!r}")
        try:
            out[key.strip()] = json.loads(raw)
        except json.JSONDecodeError:
            out[key.strip()] = raw
    return out


async def _run_crawler(crawler: Crawler) -> None:
    loop = asyncio.get_running_loop()
    main = asyncio.current_task()
    presses = 0

    def on_signal() -> None:
        nonlocal presses
        presses += 1
        if presses == 1:
            typer.echo("stopping gracefully (Ctrl-C again to abort)...", err=True)
            crawler.stop("interrupted")
        elif main is not None:
            main.cancel()

    for sig in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(NotImplementedError, RuntimeError):  # non-POSIX loops
            loop.add_signal_handler(sig, on_signal)
    try:
        await crawler.run()
    finally:
        for sig in (signal.SIGINT, signal.SIGTERM):
            with contextlib.suppress(NotImplementedError, RuntimeError):
                loop.remove_signal_handler(sig)


@app.command()
def run(
    spider_file: Annotated[Path, typer.Argument(help="Python file defining a Spider subclass.")],
    spider: Annotated[str | None, typer.Option("--spider", "-s", help="Spider name.")] = None,
    sink: Annotated[
        list[str] | None,
        typer.Option(help="jsonl:PATH, csv:PATH or postgresql://...?table=T&key=K (repeatable)."),
    ] = None,
    concurrency: Annotated[int | None, typer.Option(help="Global concurrent requests.")] = None,
    per_domain_concurrency: Annotated[int | None, typer.Option()] = None,
    rate: Annotated[float | None, typer.Option(help="Requests/second per domain.")] = None,
    max_depth: Annotated[int | None, typer.Option()] = None,
    max_requests: Annotated[int | None, typer.Option()] = None,
    queue: Annotated[str, typer.Option(help="'memory' or a redis:// URL.")] = "memory",
    checkpoint: Annotated[Path | None, typer.Option(help="Checkpoint file path.")] = None,
    resume: Annotated[bool, typer.Option(help="Resume from checkpoint / Redis state.")] = False,
    metrics_port: Annotated[int | None, typer.Option(help="Serve Prometheus metrics.")] = None,
    output_dir: Annotated[Path | None, typer.Option(help="DLQ, rejects and stats dir.")] = None,
    user_agent: Annotated[str | None, typer.Option()] = None,
    ignore_robots: Annotated[
        bool, typer.Option(help="Do not fetch/obey robots.txt. Only for sites you own.")
    ] = False,
    set_: Annotated[
        list[str] | None, typer.Option("--set", help="Any setting as key=value (repeatable).")
    ] = None,
    log_level: Annotated[str, typer.Option()] = "INFO",
    plain_logs: Annotated[bool, typer.Option(help="Human-readable logs instead of JSON.")] = False,
) -> None:
    """Run a spider."""
    configure_logging(log_level, json_logs=not plain_logs)
    spider_cls = load_spider(spider_file, spider)
    overrides: dict[str, Any] = {
        "concurrency": concurrency,
        "per_domain_concurrency": per_domain_concurrency,
        "rate_per_domain": rate,
        "max_depth": max_depth,
        "max_requests": max_requests,
        "checkpoint_path": checkpoint,
        "resume": resume or None,
        "metrics_port": metrics_port,
        "output_dir": output_dir,
        "user_agent": user_agent,
        "obey_robots": False if ignore_robots else None,
    }
    settings = Settings.build(spider_cls.custom_settings, overrides, _parse_set(set_ or []))
    sinks = [sink_from_spec(spec, append=settings.resume) for spec in sink or []]
    if not sinks:
        typer.echo("warning: no --sink given; items are counted but not stored", err=True)
    crawler = Crawler(
        spider_cls(),
        settings,
        sinks=sinks,
        queue=queue_from_spec(queue, prefix=f"crawlforge:{spider_cls.name}"),
    )
    try:
        asyncio.run(_run_crawler(crawler))
    except (KeyboardInterrupt, asyncio.CancelledError):
        typer.echo("aborted; state saved for --resume" if settings.checkpoint_path else "aborted")
        raise typer.Exit(130) from None
    typer.echo(json.dumps(crawler.stats.to_dict(), indent=2))
    if (crawler.stats.finish_reason or "").startswith("error"):
        raise typer.Exit(1)


def _metrics_summary(url: str) -> dict[str, float]:
    import httpx
    from prometheus_client.parser import text_string_to_metric_families

    body = httpx.get(url, timeout=10).text
    totals: dict[str, float] = {}
    for family in text_string_to_metric_families(body):
        if not family.name.startswith("crawlforge_"):
            continue
        for sample in family.samples:
            if sample.name.endswith(("_bucket", "_created")):
                continue
            labels = ",".join(f"{k}={v}" for k, v in sorted(sample.labels.items()))
            key = f"{sample.name}{{{labels}}}" if labels else sample.name
            totals[key] = sample.value
    return totals


@app.command()
def stats(
    file: Annotated[Path, typer.Option(help="Stats file written by a run.")] = Path(
        ".crawlforge/stats.json"
    ),
    metrics_url: Annotated[
        str | None,
        typer.Option(help="Read live metrics instead, e.g. http://localhost:9410/metrics"),
    ] = None,
) -> None:
    """Show stats of the last run, or of a running crawl via its metrics endpoint."""
    if metrics_url:
        for key, value in sorted(_metrics_summary(metrics_url).items()):
            typer.echo(f"{key:<80} {value:g}")
        return
    if not file.exists():
        typer.echo(f"no stats file at {file}; run a crawl first or pass --file", err=True)
        raise typer.Exit(1)
    data = json.loads(file.read_text())
    width = max(len(k) for k in data)
    for key, value in data.items():
        if isinstance(value, dict):
            value = ", ".join(f"{k}: {v}" for k, v in value.items()) or "-"
        typer.echo(f"{key:<{width}}  {value}")


async def _replay_to_queue(spec: str, spider: str, requests: list[Any]) -> None:
    from crawlforge.replay import replay_into_queue

    backend = queue_from_spec(spec, prefix=f"crawlforge:{spider}")
    try:
        await replay_into_queue(backend, requests)
    finally:
        await backend.close()


async def _queue_pending(spec: str, spider: str) -> set[str]:
    from crawlforge.replay import queue_pending

    backend = queue_from_spec(spec, prefix=f"crawlforge:{spider}")
    try:
        return await queue_pending(backend)
    finally:
        await backend.close()


@app.command("replay-dlq")
def replay_dlq(
    dlq: Annotated[Path, typer.Option(help="Dead-letter file written by a run.")] = Path(
        ".crawlforge/dlq.jsonl"
    ),
    checkpoint: Annotated[
        Path | None, typer.Option(help="Checkpoint to re-queue into (created if missing).")
    ] = None,
    queue: Annotated[
        str, typer.Option(help="redis:// URL to re-queue into (when no checkpoint file).")
    ] = "memory",
    spider: Annotated[
        str | None,
        typer.Option(
            "--spider", "-s", help="Spider name (Redis namespace / new checkpoint owner)."
        ),
    ] = None,
    reason: Annotated[
        str | None, typer.Option(help="Only entries whose reason contains this text.")
    ] = None,
    regex: Annotated[bool, typer.Option(help="Treat --reason as a regular expression.")] = False,
    domain: Annotated[
        list[str] | None, typer.Option(help="Only this domain and its subdomains (repeatable).")
    ] = None,
    since: Annotated[
        str | None, typer.Option(help="Only entries since an ISO time or a duration (2h, 7d).")
    ] = None,
    limit: Annotated[int | None, typer.Option(min=1, help="Replay at most N entries.")] = None,
    dry_run: Annotated[bool, typer.Option(help="Show what would be replayed; change nothing.")] = (
        False
    ),
) -> None:
    """Re-queue dead letters so the next `run --resume` fetches them again."""
    import re

    from crawlforge.checkpoint import Checkpoint
    from crawlforge.replay import (
        DlqFilter,
        checkpoint_pending,
        mark_replayed,
        parse_since,
        plan_replay,
        read_dlq,
        replay_into_checkpoint,
        replay_request,
        write_dlq,
    )

    if not dlq.exists():
        typer.echo(f"no dead-letter file at {dlq}; pass --dlq", err=True)
        raise typer.Exit(1)
    try:
        flt = DlqFilter(
            reason=reason,
            regex=regex,
            domains=domain or [],
            since=parse_since(since) if since else None,
        )
    except re.error as exc:
        raise typer.BadParameter(f"invalid --reason regex: {exc}") from None
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from None

    # Same precedence as `run --resume`: an existing checkpoint file wins over Redis state.
    ckpt = Checkpoint(checkpoint) if checkpoint else None
    use_checkpoint = ckpt is not None and (ckpt.exists() or queue in ("", "memory"))
    if not use_checkpoint and queue in ("", "memory"):
        raise typer.BadParameter(
            "the memory queue does not outlive a run; pass --checkpoint or a redis:// --queue"
        )
    if use_checkpoint:
        assert ckpt is not None
        if ckpt.exists():
            owner = json.loads(ckpt.path.read_text(encoding="utf-8")).get("spider")
            if spider is not None and spider != owner:
                raise typer.BadParameter(
                    f"checkpoint {ckpt.path} belongs to spider {owner!r}, not {spider!r}"
                )
            spider = owner
        target = f"checkpoint:{ckpt.path}"
    else:
        target = f"queue:{queue}"
    if not spider:
        raise typer.BadParameter("--spider is required to create a checkpoint or use Redis")

    lines = read_dlq(dlq)
    try:
        if use_checkpoint:
            queued = checkpoint_pending(ckpt, spider) if ckpt else set()
        else:
            queued = asyncio.run(_queue_pending(queue, spider))
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from None
    plan = plan_replay(lines, flt, limit=limit, queued=queued)

    if dry_run:
        for line in plan.selected:
            assert line.entry is not None and line.request is not None
            typer.echo(f"would replay  {line.entry.get('reason')}  {line.request.url}")
    else:
        requests = [replay_request(line) for line in plan.selected]
        if requests:
            if use_checkpoint:
                assert ckpt is not None
                replay_into_checkpoint(ckpt, spider, requests)
            else:
                asyncio.run(_replay_to_queue(queue, spider, requests))
        if plan.selected or plan.covered:
            mark_replayed([*plan.selected, *plan.covered], target)
            write_dlq(dlq, lines)

    verb = "to replay" if dry_run else "replayed"
    skipped = sum(plan.skipped.values())
    detail = ", ".join(f"{k}: {v}" for k, v in sorted(plan.skipped.items()))
    typer.echo(
        f"entries   {plan.total}" + (f" ({plan.unreadable} unreadable)" if plan.unreadable else "")
    )
    typer.echo(f"matched   {len(plan.matched)}")
    typer.echo(f"{verb:<9} {len(plan.selected)} -> {target}")
    typer.echo(f"skipped   {skipped}" + (f" ({detail})" if detail else ""))
    if dry_run:
        typer.echo("dry run: nothing was changed")
    elif plan.selected:
        where = f"--checkpoint {ckpt.path}" if use_checkpoint and ckpt else f"--queue {queue}"
        typer.echo(f"next: crawlforge run <spider.py> {where} --resume")


@app.command()
def version() -> None:
    """Print the CrawlForge version."""
    typer.echo(__version__)
