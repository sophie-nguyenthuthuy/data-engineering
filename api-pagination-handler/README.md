# api-pagination-handler

A generic pagination framework for REST APIs. Four pagination
strategies (offset, cursor, page-token, RFC-5988 `Link` header) plus a
configurable exponential-backoff retry policy that any of them can use.

[![Python](https://img.shields.io/badge/python-3.10%20%7C%203.11%20%7C%203.12-blue.svg)](#)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

## Why

Every REST API has invented its own pagination wire format. A
data-ingestion job that hits five APIs ends up with five hand-rolled
loops, each with its own bugs around end-of-stream detection,
infinite-loop protection, and retry-on-5xx. This package gives you one
loop that takes a `Paginator` strategy and a `Transport` callable, and
yields records.

## Architecture

```
   ┌──────────────┐
   │  Transport   │   (url, headers) → Response
   └──────┬───────┘
          │
          ▼
   ┌──────────────────┐    ┌──────────────────┐
   │  RetryPolicy     │ ── │  Paginator       │
   │  exp backoff +   │    │  - first()       │
   │  full jitter     │    │  - next(req,r)   │
   └──────┬───────────┘    │  - records(r)    │
          │                └──────────────────┘
          ▼
   ┌──────────────────┐
   │ PaginatedClient  │   iter_pages / iter_records / fetch_all
   │  + max_pages     │   circuit-breaker
   └──────────────────┘
```

## Install

```bash
pip install -e ".[dev]"
```

Python 3.10+. **Zero runtime dependencies** — stdlib only.

## CLI

```bash
aphctl info
aphctl demo --base-url https://example.com/items --limit 5
```

## Library

```python
from aph.client import PaginatedClient
from aph.paginators.offset       import OffsetPaginator
from aph.paginators.cursor       import CursorPaginator
from aph.paginators.token        import TokenPaginator
from aph.paginators.link_header  import LinkHeaderPaginator
from aph.retry                   import RetryPolicy
from aph.transport               import Response

# Offset / limit
records = PaginatedClient(
    transport=my_http_get,
    paginator=OffsetPaginator(limit=200, records_path="data"),
    retry=RetryPolicy(max_attempts=5, base=0.1, multiplier=2.0, cap=30.0),
).fetch_all("https://api.example.com/v1/orders")

# Cursor
PaginatedClient(
    transport=my_http_get,
    paginator=CursorPaginator(cursor_param="cursor", cursor_path="next_cursor"),
).fetch_all("https://api.example.com/v1/events")

# Google-style page token
PaginatedClient(
    transport=my_http_get,
    paginator=TokenPaginator(token_param="pageToken", next_token_path="nextPageToken"),
).fetch_all("https://api.example.com/v1/projects")

# GitHub-style Link header
PaginatedClient(
    transport=my_http_get,
    paginator=LinkHeaderPaginator(),
).fetch_all("https://api.github.com/orgs/x/repos")
```

`my_http_get` is any callable matching `(url: str, headers: Mapping[str,
str]) → Response`. The default-construction `Response` has
`status`/`body`/`headers`/`url` and convenience methods `is_success()`,
`is_retryable()` (recognises 408/425/429/500/502/503/504), and a
case-insensitive `header(name)` lookup.

## Components

| Module                       | Role                                                                |
| ---------------------------- | ------------------------------------------------------------------- |
| `aph.transport`              | `Response`, `Transport` type alias                                  |
| `aph.retry`                  | `RetryPolicy` (exp backoff + full jitter), `RetryError`             |
| `aph.paginators.base`        | `Paginator` ABC, `PageRequest`                                      |
| `aph.paginators.offset`      | `OffsetPaginator` (offset + limit)                                  |
| `aph.paginators.cursor`      | `CursorPaginator` (opaque cursor, echo-loop detection)              |
| `aph.paginators.token`       | `TokenPaginator` (Google `pageToken`)                              |
| `aph.paginators.link_header` | `LinkHeaderPaginator` (RFC 5988)                                    |
| `aph.client`                 | `PaginatedClient` with `iter_pages` / `iter_records` / `fetch_all`  |
| `aph.cli`                    | `aphctl info | demo`                                               |

## Retry semantics

- `delay(k) = min(base · multiplier^(k-1), cap)`, optionally with full
  random jitter (`uniform(0, delay)`), Amazon Builders' Library recipe.
- `run(fn, is_failure=…)` retries on any exception in `retry_on` or
  whenever `is_failure(result)` is True.
- `run_response(fn)` is shorthand for HTTP calls — retries any
  `Response` where `is_retryable()` returns True.

## Safety rails

- `OffsetPaginator` stops on the first short page.
- `CursorPaginator` stops when `next_cursor` is missing **or** when
  the server echoes the same cursor back (a common bug).
- `LinkHeaderPaginator` stops when the `next` link equals the current
  URL.
- `PaginatedClient(max_pages=N)` is a hard circuit-breaker against any
  paginator that produces an infinite stream.

## Quality

```bash
make lint        # ruff
make format
make type        # mypy --strict
make test        # 50+ tests
```

- **53 tests**, 0 failing; includes 1 Hypothesis property (offset
  paginator iterates every record back across random page sizes).
- `mypy --strict` clean over 10 source files.
- Python 3.10 / 3.11 / 3.12 CI matrix + Docker build smoke step.
- Multi-stage slim Docker image, non-root `aph` user.
- **Zero runtime dependencies** — stdlib only.

## License

MIT — see [LICENSE](LICENSE).
