from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from crawlforge.sinks.base import Sink
from crawlforge.sinks.files import CsvSink, JsonlSink


def sink_from_spec(spec: str, *, append: bool = False) -> Sink:
    """Build a sink from a CLI spec.

    - ``jsonl:path/to/out.jsonl``
    - ``csv:path/to/out.csv``
    - ``postgresql://user:pass@host/db?table=books&key=url`` (``postgres://`` also works);
      ``table`` and ``key`` are consumed here, other query params go to asyncpg.
    """
    if spec.startswith(("postgres://", "postgresql://")):
        from crawlforge.sinks.postgres import PostgresSink

        parts = urlsplit(spec)
        params = dict(parse_qsl(parts.query))
        table = params.pop("table", "items")
        key = params.pop("key", "url")
        dsn = urlunsplit(parts._replace(query=urlencode(params)))
        return PostgresSink(dsn, table=table, key=key)
    kind, sep, target = spec.partition(":")
    if not sep or not target:
        raise ValueError(f"invalid sink spec {spec!r}; expected kind:target")
    if kind == "jsonl":
        return JsonlSink(target, append=append)
    if kind == "csv":
        return CsvSink(target, append=append)
    raise ValueError(f"unknown sink type {kind!r} (jsonl, csv, postgresql://...)")


__all__ = ["CsvSink", "JsonlSink", "Sink", "sink_from_spec"]
