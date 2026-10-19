# CrawlForge

**A resilient, observable data extraction framework for async Python.**

CrawlForge is a small crawling framework built on `asyncio` + `httpx`. You write a spider; the
framework takes care of scheduling, de-duplication, politeness, retries, circuit breaking,
validation, output, metrics and resumable state.

```python
from pydantic import BaseModel
from crawlforge import Spider

class Quote(BaseModel):
    text: str
    author: str
    tags: list[str]

class QuotesSpider(Spider):
    name = "quotes"
    start_urls = ["https://quotes.toscrape.com/"]
    item_model = Quote

    def parse(self, response):
        for q in response.css("div.quote"):
            yield {
                "text": q.css("span.text::text").get(),
                "author": q.css("small.author::text").get(),
                "tags": q.css("a.tag::text").getall(),
            }
        if next_page := response.css("li.next a::attr(href)").get():
            yield response.follow(next_page)
```

```bash
crawlforge run quotes.py --sink jsonl:out/quotes.jsonl --concurrency 8
```

## Why it exists

Writing the code that pulls data out of one page is the easy part. Production crawls usually fail
somewhere else:

- a site starts returning 503s or 429s and the crawler either hammers it or gives up;
- a selector silently breaks and garbage flows into the database;
- the process dies at 80% and starts over from zero;
- nobody can tell whether the crawl is healthy until it's over.

CrawlForge handles each of these explicitly: retries with backoff that honor `Retry-After`, a
circuit breaker per domain, a dead-letter queue, schema validation with a rejects sink,
checkpoints, and Prometheus metrics. It's small enough to read in an afternoon (about 2.5k
lines of library code).

## Architecture

```
                 +----------------------------------------------------------------+
  Spider         |                         Crawler (engine)                       |
  start_requests +--> Frontier ------------------------------------------+        |
  parse()  <-----+    | dedupe (normalized URL fingerprint), max depth   |        |
    |            |    | one queue per domain + index of domains w/ work  |        |
    |            |    | QueueBackend: MemoryQueue | RedisQueue (leases)  |        |
    |            |    +------------------------+-------------------------+        |
    |            |                             | next request of a READY domain   |
    |            |                             | (N workers; throttled, capped    |
    |            |                             |  and breaker-open ones skipped)  |
    |            |                             v                                  |
    |            |   robots.txt cache --> circuit breaker --> per-domain slot     |
    |            |   (RFC 9309, per          (per domain:     (semaphore +        |
    |            |    origin, crawl-delay)    closed/open/     token bucket)      |
    |            |                            half-open)            |             |
    |            |                                                  v             |
    |            |                          Fetcher: httpx  |  PlaywrightRenderer |
    |            |                                    (render=True, lazy import)  |
    |            |                                                  |             |
    |            |          +---------------------------------------+             |
    |            |          v                                                     |
    |            |   classify: 2xx/3xx -> callback                                |
    |            |             408/425/429/5xx, timeouts, conn errors -> retry    |
    |            |               (full-jitter backoff or Retry-After, deferred    |
    |            |                on a timer, never blocking a worker)            |
    |            |             retries exhausted / 4xx / callback error -> DLQ    |
    |            |                                                                |
    +-- yields --+--> Request ---------------------------------------> Frontier   |
                 |    dict / BaseModel --> pydantic validate --+--> Sinks         |
                 |                                             |    (JSONL, CSV,  |
                 |                                             |     Postgres     |
                 |                                             |     upsert)      |
                 |                                             +--> rejects.jsonl |
                 |                                                                |
                 |  Checkpoint (queue + in-flight + deferred + seen) <- periodic, |
                 |  on stop, on cancel.   Metrics -> /metrics (Prometheus)       |
                 |  JSON logs -> stderr.  stats.json at the end of every run.     |
                 +----------------------------------------------------------------+
```

Source layout (`src/crawlforge/`):

| Module | Responsibility |
| --- | --- |
| `engine.py` | Worker loop, ready-domain scheduling, request pipeline, deferral, shutdown, checkpoint capture |
| `spider.py`, `http.py` | Public API: `Spider`, `Request`, `Response` (parsel selectors) |
| `frontier.py`, `queues/` | Dedupe + depth limits; memory and Redis (leasing) per-domain priority queues |
| `url.py` | URL normalization and request fingerprints |
| `ratelimit.py` | Token bucket, per-domain throttle, robots crawl-delay |
| `retry.py`, `breaker.py` | Retry classification/backoff, circuit breaker state machine |
| `robots.py` | robots.txt cache with RFC 9309 failure semantics |
| `fetch.py`, `render.py` | httpx fetcher, optional Playwright renderer |
| `sinks/`, `deadletter.py` | JSONL / CSV / Postgres sinks, DLQ and rejects writers |
| `checkpoint.py`, `stats.py` | Atomic checkpoint files, run statistics |
| `metrics.py`, `logs.py` | Prometheus registry, JSON log formatter |
| `replay.py` | DLQ filtering and re-queueing for `crawlforge replay-dlq` |
| `cli.py` | `crawlforge run`, `crawlforge stats`, `crawlforge replay-dlq` |

## Scheduling

The frontier keeps one priority queue per domain (host, plus the port if it isn't the
default) and an index of the domains that have queued work. A worker doesn't take "the next
request"; it takes the next request of a domain it can send to right now. A domain is
skipped while:

- its token bucket has no token for one more request. Requests already handed out but not yet
  through their slot count against the bucket, so a burst of 1 means one request at a time is
  scheduled, not four that then queue up on the bucket;
- it already has `per_domain_concurrency` requests in flight in this process;
- its circuit breaker is open, or half-open with its single probe already out.

Skipped domains keep their requests and their place in line. Among the domains that are
ready, the one whose next request has the highest `priority` goes first; domains tied on
priority take turns (round-robin: a domain that was just served goes to the back of its
priority band). Within a domain, higher priority goes first and ties are FIFO, as before.
So a crawl that doesn't use priorities interleaves its domains evenly, and one that does gets
the same priority order as before, just without waiting on domains that aren't ready.

When every domain with queued work is blocked, workers don't spin or each sleep: one timer on
the crawler's clock wakes them when the first blocked domain becomes ready (next token, or the
end of a breaker's recovery window), and a finished request wakes them when it frees a
concurrency slot or a probe. An open breaker's requests are never popped just to be put back.

On Redis the same rules run inside the lease scripts. Every domain has its own sorted set
(`<prefix>:domains:<domain>`) and `<prefix>:domains` is the ready-domain index (domain ->
priority band of its best request + its turn). `receive` walks the index, skips the domains
the process passed in, and leases the head of the first other one, in one script. Pushes,
retries, releases and reclaimed leases update the queue and the index in the same script, so
two processes never see them disagree. Which domains are blocked is each process's own view
(see Limitations).

With the fake clock, a crawl of 20 pages on a domain with a 1 s crawl-delay plus 20 pages on
an unthrottled domain, seeded slow-first, with 4 workers
(`tests/test_scheduling.py::test_throttled_domain_no_longer_blocks_the_others`):

| scheduler | last fast.test page | fast.test throughput | whole crawl |
| --- | --- | --- | --- |
| strict FIFO (the old single queue) | 19.016 s | ~1.05 req/s | 19.016 s |
| ready domain first | 0.019 s | ~1050 req/s | 19.000 s |

Under FIFO every worker ends up waiting in the slow domain's token bucket and the fast domain
only starts at 16 s. With per-domain scheduling it finishes before the slow domain's second
token. The whole crawl takes the same time because the slow domain's crawl-delay bounds it.

## Design decisions and trade-offs

- **Retries are deferred, not slept.** A request that needs to back off is handed to a timer
  task that re-queues it later. Workers never sit idle inside a `sleep`, so one slow domain
  can't use up the worker pool with backoff waits.
- **Full-jitter exponential backoff** (`uniform(0, min(cap, base * 2^n))`) spreads retries out
  so they don't arrive at the server in synchronized bursts. When the server sends
  `Retry-After` (seconds or HTTP-date), that value wins, capped at `max_retry_after`.
- **Circuit breaker per domain**, counting *consecutive* failures (retryable statuses and
  transport errors). When it's open, nothing is sent to that domain. After
  `breaker_recovery` seconds, one half-open probe decides whether the breaker closes again.
  4xx responses count as a success for the breaker, since the server is up and answering.
  Errors that aren't the server's fault (bad URL, missing renderer) release the probe slot
  without judging the domain.
- **Callbacks are stored by name** (`Request.callback == "parse_book"`). That keeps requests
  JSON-serializable, so they can go into Redis and into checkpoints.
- **Dedupe at enqueue time** on a SHA-1 of method + normalized URL + body. Normalization
  lower-cases the scheme and host, drops default ports and fragments, sorts query params and
  resolves dot segments. Retries bypass dedupe by design. `dont_filter=True` opts out.
- **Checkpoint consistency.** Frontier mutations (pop+register, push) and checkpoint capture
  share one lock. Without it, a snapshot could miss a request that is moving between the
  queue, a worker and a timer. In-flight requests are included in checkpoints. A graceful stop
  drains them first (each completed page is processed once). A hard kill or cancel re-fetches
  them on resume (at-least-once for those pages). The Postgres upsert sink makes that
  idempotent.
- **One registry per crawler** for Prometheus metrics: no global state, and tests stay
  isolated.
- **Robots.txt follows RFC 9309:** 4xx means "no rules", 5xx or a network error means
  "disallow everything". Error results are re-checked after `error_ttl` instead of being
  cached for an hour. `Crawl-delay` can only slow a domain down, never speed it up.
- **File sinks use plain buffered writes** from the event loop. That's simple and fast enough
  for JSONL/CSV volumes. A crawl producing very large items at very high rates would want a
  writer thread.
- **Leases, not destructive pops, on Redis.** A Redis pop moves the request into an in-flight
  set with a deadline (one Lua script), and the worker acks it only after its follow-ups are
  queued and its items written. A process that dies mid-request therefore loses nothing: its
  leases expire and another process re-queues them. The price is at-least-once delivery for
  requests whose holder died, the same trade-off as checkpoints.
- **Readiness is checked before the pop, not after.** Workers used to pop the next request and
  then wait for that domain's slot, so one throttled domain could hold every worker
  (head-of-line blocking). The engine now passes the domains it can't send to into the pop
  (see [Scheduling](#scheduling)). The token is still taken at fetch time, so the rate limit
  and `Crawl-delay` are enforced exactly as before; the check before the pop only decides
  which domain to serve.

## Quick start

```bash
git clone <this repo> crawlforge && cd crawlforge
uv venv .venv --python 3.12
uv pip install --python .venv/bin/python -e ".[dev]"          # core + test tooling
uv pip install --python .venv/bin/python -e ".[browser]"      # optional: Playwright
.venv/bin/playwright install chromium                          # optional: browser binary

.venv/bin/crawlforge run examples/books.py --sink jsonl:out/books.jsonl --concurrency 8
.venv/bin/crawlforge stats --file .crawlforge/stats.json
```

With the full local stack (Redis frontier, Postgres sink, Prometheus):

```bash
docker compose up -d
crawlforge run examples/books.py \
  --queue redis://localhost:6379/0 \
  --sink "postgresql://crawlforge:crawlforge@localhost:5432/crawlforge?table=books&key=upc" \
  --metrics-port 9410
# http://localhost:9090  ->  rate(crawlforge_requests_total[1m])
crawlforge stats --metrics-url http://localhost:9410/metrics      # live view from the CLI
```

Stop with Ctrl-C and resume later:

```bash
crawlforge run examples/books.py --sink jsonl:out/books.jsonl --checkpoint .crawlforge/books.ckpt.json
^C   # graceful: in-flight pages finish, the rest is checkpointed
crawlforge run examples/books.py --sink jsonl:out/books.jsonl --checkpoint .crawlforge/books.ckpt.json --resume
```

On resume, sinks open in append mode and the stats total both runs. The checkpoint is deleted
once the crawl finishes. Redis-backed crawls can also resume straight from Redis
(`--queue redis://... --resume`) without a checkpoint file.

### Several processes on one Redis frontier

Any number of crawler processes (on one machine or several) can share a Redis frontier.
Start the first one normally, then join more with `--resume`:

```bash
crawlforge run examples/books.py --queue redis://localhost:6379/0 --sink "postgresql://..."
crawlforge run examples/books.py --queue redis://localhost:6379/0 --sink "postgresql://..." --resume
```

How the work is shared:

- **Leased pops.** A pop atomically moves the head of a ready domain's queue
  (`crawlforge:<spider>:domains:<domain>`) into `:leases` (lease id -> deadline) and
  `:inflight` (lease id -> queue entry), and updates the `:domains` index. No two processes
  ever get the same entry.
- **Ack on completion.** The lease is acked once the request is done: callback run (items
  written, follow-ups queued) or dead-lettered. A retry swaps the lease for the re-queued
  request in one step when its backoff timer fires; until then the lease is held.
- **Extensions.** Every `lease_ttl / 3` seconds a process extends all leases it holds
  (in-flight and waiting on backoff), so a slow page or a long `Retry-After` keeps its lease.
- **Reclaim.** The same loop, and any idle process waiting for work, moves expired leases of
  *any* process back to their domain's queue with their original priority. A crashed or frozen worker's
  requests are fetched elsewhere after at most `lease_ttl` seconds.
- **Delivery limit.** `:deliveries` counts how often an entry was handed out. An entry whose
  lease keeps expiring (a page that crashes or hangs every worker that takes it) goes to the
  DLQ as `lease_expired_too_often` when it would be handed out more than `max_deliveries`
  times.
- **Shared seen set.** Fingerprints are added to `:seen` atomically when a request is
  queued, so two processes never enqueue the same URL twice.
- **Graceful shutdown** (Ctrl-C, `max_requests`, cancel) releases this process's leases at
  once. A released request goes back to its place in the queue and doesn't count as a
  delivery; a request waiting on a retry goes back with its updated retry count.
- **Finishing.** A process stops when the queue is empty *and* no process holds a lease, so
  the last workers don't quit while another one may still yield follow-ups.

Tune with `--set lease_ttl=60` (seconds, default 60; keep it above your slowest request plus
a margin, since a lease is only extended every third of it) and `--set max_deliveries=3`.
Every process writes its own `dlq.jsonl`, `rejects.jsonl` and `stats.json`, so give each one
its own `--output-dir`, and use a sink that can take concurrent writers (Postgres) or one file
per process. Lease deadlines use each client's wall clock, so keep the machines' clocks in
sync (NTP).

### Replaying dead letters

Requests that failed permanently land in `dlq.jsonl` with their reason (`http_404`,
`max_retries_exceeded:http_503`, `fetch_error:ConnectError`, `callback_error:KeyError`, ...).
After fixing the cause, put them back into the frontier and resume:

```bash
crawlforge replay-dlq --checkpoint .crawlforge/books.ckpt.json --spider books \
  --reason http_503 --dry-run                                   # list what would be replayed
crawlforge replay-dlq --checkpoint .crawlforge/books.ckpt.json --spider books --reason http_503
crawlforge run examples/books.py --sink jsonl:out/books.jsonl \
  --checkpoint .crawlforge/books.ckpt.json --resume
```

```
entries   12
matched   9
replayed  9 -> checkpoint:.crawlforge/books.ckpt.json
skipped   0
next: crawlforge run <spider.py> --checkpoint .crawlforge/books.ckpt.json --resume
```

| Option | Meaning |
| --- | --- |
| `--dlq PATH` | Dead-letter file (default `.crawlforge/dlq.jsonl`) |
| `--reason TEXT` | Reason contains `TEXT`; with `--regex`, `TEXT` is a regular expression (`re.search`) |
| `--domain HOST` | Request host is `HOST` or a subdomain of it, port ignored (repeatable) |
| `--since WHEN` | Dead-lettered at or after an ISO 8601 time (UTC if naive) or a duration ago (`30m`, `2h`, `7d`) |
| `--limit N` | Replay at most `N` entries (oldest first) |
| `--dry-run` | Print the entries that would be replayed and change nothing |
| `--checkpoint PATH` | Append to this checkpoint's pending requests; it is created if the crawl already finished and removed it |
| `--queue redis://...` | Push onto the spider's Redis queue instead (used when no checkpoint file exists, same precedence as `run --resume`); requests queued or leased by a running process count as already queued |
| `--spider NAME` | The spider's `name`; needed for Redis keys and for new checkpoints (an existing checkpoint names its own spider) |

All filters must match. Replayed requests keep their URL, callback, meta, priority and depth
and get a fresh retry budget. They go in as pending requests, so they skip the seen-set check
that would otherwise drop them as duplicates. Their fingerprints are still added to the seen
set, so links back to them stay de-duplicated, and every other URL is filtered as usual.

Replayed entries stay in the DLQ, marked with `replayed_at` and `replayed_to` (the file is
rewritten atomically). Running the same command again matches them but replays nothing
(`skipped 9 (already replayed: 9)`), and entries whose request is already pending in the
checkpoint are skipped too. If a replayed request fails again, the resumed run appends a new,
unmarked entry that can be replayed later. Run `replay-dlq` while the crawl is stopped: a
running crawl rewrites its checkpoint and would drop the replayed requests. When the
checkpoint is created from scratch (the crawl had finished), it holds only the replayed
requests, so links they yield to pages crawled before are fetched again.

### Real run (books.toscrape.com)

Measured on the development machine on 2026-10-18 with the command above (`--concurrency 8`,
the spider's defaults of 5 req/s and 4 concurrent requests per domain, about 1.1 s
round-trip to the site from where it ran):

| requests | items | rejects | retries | dead letters | wall time |
| --- | --- | --- | --- | --- | --- |
| 1050 | 1000 | 0 | 0 | 0 | 275 s |

All 1000 books came out with unique UPCs across 50 categories. A re-run on 2026-10-19 with
per-domain scheduling (same command and settings) gave 1050 requests, 1000 items with unique
UPCs, no retries or dead letters, in 218 s. It's a single-domain crawl, so the scheduler
has nothing to interleave there; that run checks for regressions, and the gap to 275 s is
mostly network variance between the two days (round-trip time wasn't controlled). That throughput (~3.8 req/s)
is limited by per-domain concurrency × latency, not by the rate limit. It's a deliberate,
polite default.

`examples/quotes.py`: 10 requests, 100 quotes, about 5 s. `examples/quotes_js.py` through
headless Chromium: 10 rendered pages, 100 quotes, about 37 s at 1 req/s. A SIGINT followed by
`--resume` on the quotes spider ended with exactly 100 unique quotes across the two runs.

## Writing a spider

```python
from crawlforge import Request, Response, Spider

class MySpider(Spider):
    name = "my_spider"                     # also the Redis key namespace
    start_urls = ["https://example.org/"]  # or override start_requests()
    item_model = MyPydanticModel           # optional; dict items are validated against it
    custom_settings = {"rate_per_domain": 1.0, "max_depth": 3}

    def start_requests(self):              # may be a sync or async generator
        yield Request("https://example.org/", callback=self.parse, priority=5)

    def parse(self, response: Response):   # sync/async function or (async) generator
        yield {"title": response.css("title::text").get()}            # item (dict)
        yield MyPydanticModel(...)                                      # item (already valid)
        yield response.follow("/next", callback=self.parse_detail)     # follow-up request
        yield Request(url, render=True, meta={"wait_for": ".loaded"})  # Playwright page
```

- `Response` has `.css()`, `.xpath()` (parsel), `.text`, `.json()`, `.urljoin()`, `.follow()`,
  `.status`, `.headers`, `.rendered` and `.request` (with `.depth` and `.meta`).
- Higher `priority` is fetched first among the domains that are ready to be sent to; equal
  priorities are FIFO within a domain and round-robin across domains. Depth is set by the
  engine (parent + 1).
- Items that fail validation go to `rejects.jsonl` with the pydantic error list. A callback
  that raises sends its request to the DLQ with `callback_error:<Type>`, and items it yielded
  before the error are kept.
- Non-2xx/3xx responses go to the DLQ (`http_404`, ...) unless they're listed in
  `handle_statuses`.

## Configuration

Precedence: defaults < `Spider.custom_settings` < CLI flags / `--set key=value`.

| Setting | Default | Meaning |
| --- | --- | --- |
| `user_agent` | `CrawlForge/0.1.0 (+https://github.com/huss-huss/crawlforge)` | Sent on every request and used for robots matching. Put your own contact URL here. |
| `obey_robots` | `true` | Fetch and obey robots.txt (`--ignore-robots` only for sites you own) |
| `concurrency` | `8` | Global workers |
| `per_domain_concurrency` | `2` | Simultaneous requests per domain |
| `rate_per_domain` / `burst` | `2.0` / `2.0` | Token bucket per domain (req/s, bucket size) |
| `timeout` | `30` | Total seconds per attempt |
| `max_retries` | `3` | Retries after the first attempt |
| `backoff_base` / `backoff_cap` | `0.5` / `30` | Full-jitter exponential backoff window |
| `max_retry_after` | `300` | Upper bound for honoring `Retry-After` |
| `breaker_threshold` / `breaker_recovery` | `5` / `30` | Consecutive failures to open; seconds until the probe |
| `max_depth` / `max_requests` | unset | Scope limits (`max_requests` stops gracefully and checkpoints) |
| `lease_ttl` / `max_deliveries` | `60` / `3` | Leasing queues (Redis): seconds a lease lasts without an extension; times a request may be handed out before it is dead-lettered (`lease_expired_too_often`) |
| `handle_statuses` | `[]` | Error statuses passed to callbacks instead of the DLQ |
| `render_concurrency` | `2` | Concurrent browser pages |
| `output_dir` | `.crawlforge` | Default location of `dlq.jsonl`, `rejects.jsonl`, `stats.json` |
| `checkpoint_path` / `checkpoint_interval` | unset / `30` | Checkpoint file and periodic save interval |
| `metrics_port` / `metrics_addr` | unset / `0.0.0.0` | Prometheus endpoint |

Sinks: `--sink jsonl:PATH`, `--sink csv:PATH`,
`--sink "postgresql://user:pass@host/db?table=T&key=FIELD"` (repeatable). The Postgres sink
creates `T(key TEXT PRIMARY KEY, data JSONB, scraped_at TIMESTAMPTZ)` and upserts in batches,
so a re-crawl updates rows instead of duplicating them.

Queues: `--queue memory` (default) or `--queue redis://host:6379/0`. Keys are namespaced
`crawlforge:<spider>:{domains,domains:<domain>,queued,turn,seen,seq,leases,inflight,deliveries}`.
A frontier written by an earlier version (a single `:queue` sorted set) is moved into the
per-domain queues the first time a process uses it. A fresh (non-`--resume`)
run clears its own namespace, so only the first process of a shared crawl runs without
`--resume`. A `--resume` run joins when the queue is non-empty or any lease is outstanding.

## Metrics

Exposed at `http://<host>:<metrics_port>/metrics`:

| Metric | Type | Labels |
| --- | --- | --- |
| `crawlforge_requests_total` | counter | `domain` (each attempt) |
| `crawlforge_responses_total` | counter | `domain`, `status` |
| `crawlforge_fetch_errors_total` | counter | `domain`, `error` |
| `crawlforge_retries_total` | counter | `domain`, `reason` |
| `crawlforge_items_total` | counter | `spider` |
| `crawlforge_item_rejects_total` | counter | `spider` |
| `crawlforge_dead_letters_total` | counter | `domain`, `reason` |
| `crawlforge_requests_dropped_total` | counter | `reason` (`duplicate`, `too_deep`, `robots`) |
| `crawlforge_request_latency_seconds` | histogram | `domain` |
| `crawlforge_circuit_breaker_state` | gauge | `domain` (0 closed, 1 half-open, 2 open) |
| `crawlforge_circuit_breaker_trips_total` | counter | `domain` |
| `crawlforge_frontier_size` | gauge | |
| `crawlforge_in_flight` | gauge | |
| `crawlforge_deferred` | gauge | requests waiting on backoff (or on a breaker that opened after they were scheduled) |
| `crawlforge_leases_reclaimed_total` | counter | expired leases this process put back in a shared queue |

Logs are JSON lines on stderr (`--plain-logs` for human-readable output), with structured
fields such as `url`, `status`, `reason`, `attempt` and `delay`. A `progress` line is logged
every 10 s.

## Responsible scraping

CrawlForge's defaults are meant to be polite: robots.txt is obeyed (including `Crawl-delay`),
each domain gets at most 2 concurrent requests at 2 req/s, failing servers are backed off from
rather than retried harder, and the user agent identifies the tool. Before you crawl anything:

- read the site's terms of service, and don't collect personal data you have no lawful basis
  to process;
- set `user_agent` to something that identifies you and gives a way to reach you;
- prefer official APIs or data dumps when they exist;
- keep rates low, crawl off-peak, and cache instead of re-fetching;
- use `--ignore-robots` only on sites you own or have permission to crawl.

The bundled examples only target [books.toscrape.com](https://books.toscrape.com) and
[quotes.toscrape.com](https://quotes.toscrape.com), sandboxes published specifically for
scraping practice.

## Testing

```bash
.venv/bin/ruff check . && .venv/bin/ruff format --check .
.venv/bin/pytest -q
TEST_DATABASE_URL=postgresql://user:pass@localhost:5432/db .venv/bin/pytest -q   # + Postgres sink
```

The suite never touches the internet. Engine tests use `httpx.MockTransport` and a
`FakeClock` whose `sleep` advances virtual time, so backoff, `Retry-After`, breaker recovery
windows, rate-limiter timing and the scheduler's throughput gain are asserted exactly and run in
milliseconds. CLI tests use a
throwaway HTTP server on `127.0.0.1`. Redis behavior, including the lease and scheduling Lua scripts, is tested with `fakeredis` (with
its `lupa` Lua runtime); the lease and scheduling tests also run against a real Redis when
`TEST_REDIS_URL` is set, and a randomized test checks that the Lua scheduler pops in exactly
the same order as `MemoryQueue`. The real
browser test runs only where Playwright and Chromium are installed. CI (GitHub Actions) runs
ruff and pytest on Python 3.12 with the pinned `requirements-ci.txt` and Postgres and Redis
services, so the Postgres sink test and the real-Redis lease and scheduling tests run there too.

## Limitations (v0.1)

- Multi-process crawls need the Redis backend. Politeness is per process: rate limits,
  per-domain concurrency, circuit breakers and robots.txt caches are not shared, so N
  processes send up to N times the configured rate to a domain. Divide `rate_per_domain` by
  the number of processes.
- Requests that are in flight when a process is killed hard are fetched again: on resume with
  checkpoints, or by another process once their lease expires (up to `lease_ttl` later). File
  sinks may then contain duplicates for those pages; the Postgres sink deduplicates by key.
  A request whose lease expired while its holder was merely slow can be fetched twice too.
- `max_requests` and `stats.json` are per process; DLQ and rejects files are too.
- Scheduling decisions are per process too. On a shared Redis frontier each process skips
  the domains *it* has throttled, capped or broken, and the Lua script walks the domain index
  past them. A process that has blocked thousands of domains passes them all on each pop, so
  very wide crawls (many thousands of hot domains per process) pay for that per pop.
- Priority is respected across domains, so a domain that keeps yielding high-priority
  requests is served before lower-priority work elsewhere (round-robin only applies among
  equal priorities). Give requests the same priority if you want strict fairness.
- A worker counts a request against its domain's concurrency from the pop until it is done,
  including its robots.txt check, so the first requests of a domain may start slightly later
  than the semaphore alone would allow.
- Lease deadlines come from client wall clocks, and the scripts build per-domain key names
  themselves (and the keys of one spider aren't hash-tagged), so Redis Cluster isn't
  supported.
- Upgrading a shared Redis frontier from the single-queue layout: stop the old processes
  gracefully first (they release their leases). Queued entries are migrated with their
  domains; a lease left behind by an old process that died is reclaimed into a shared
  catch-all queue that is scheduled without per-domain readiness (the request is still
  rate-limited when it is fetched).
- No HTTP cache, cookie/session management helpers, proxy rotation, or sitemap support.
- `item_model` is one schema per spider. Yield pydantic instances directly if you need several.
- Checkpoint files hold the whole frontier and seen set in JSON. That's fine for hundreds of
  thousands of URLs, not for hundreds of millions (use Redis plus a Bloom filter there).
- The Playwright renderer uses a fresh browser context per page (isolated but slower). Its
  timeouts are mapped to retryable errors, but it has no request interception or resource
  blocking yet.
- `docker-compose.yml` is checked with `docker compose config`, but the stack wasn't brought up
  during development (no Docker daemon was available). Locally the Redis queue was tested
  against fakeredis and the Postgres sink against a real PostgreSQL 17; CI runs the lease
  tests against a real Redis 7 service as well.

## Roadmap

- Adaptive (AutoThrottle-style) rate control, feeding latency into each domain's bucket and
  so into the scheduler
- Shared (Redis-backed) per-domain rate limits and breakers for multi-process crawls;
  Bloom-filter seen set
- Sitemap and RSS discovery, HTTP caching (ETag/Last-Modified), proxy pools
- Grafana dashboard JSON next to the Prometheus config
- Parquet and S3 sinks; item pipelines (transform/enrich) before sinks

## License

MIT, see [LICENSE](LICENSE). Copyright (c) 2026 Hussen Mohammed.
