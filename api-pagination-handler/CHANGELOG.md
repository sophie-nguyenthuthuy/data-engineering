# Changelog

## [0.1.0] — 2026-05-13

### Added
- **Transport** (`aph.transport`) — `Response(status, body, headers,
  url)` + case-insensitive header lookup + `is_retryable()` recognising
  408/425/429/500/502/503/504.
- **Retry policy** (`aph.retry`) — exponential backoff with full
  jitter (Amazon Builders' Library), `run(fn, is_failure=)` and
  `run_response(fn)` convenience wrapper.
- **Paginators**:
  * `OffsetPaginator(limit, offset_param, limit_param, records_path)`
    — stops on first short page.
  * `CursorPaginator(cursor_param, cursor_path, records_path)` —
    rejects echoed-cursor infinite-loop traps.
  * `TokenPaginator(token_param, next_token_path, records_path)` —
    Google-style.
  * `LinkHeaderPaginator(rel, records_path)` — RFC 5988; refuses to
    follow a `next` link equal to the current URL.
- **Client** (`aph.client.PaginatedClient`) — `iter_pages`,
  `iter_records`, `fetch_all`, with `max_pages` circuit breaker.
- **CLI** (`aphctl info | demo`).
- **48 tests** including 1 Hypothesis property (offset paginator
  returns every record for any (page-size, page-count) split).
- mypy `--strict` clean, multi-stage Docker, GHA matrix 3.10/3.11/3.12.
- Zero runtime dependencies.

### Notes
- `mypy --strict` rejected a lambda-with-default that captured the
  for-loop variable; replaced with `functools.partial(self.transport,
  request.url, dict(request.headers))` to side-step ruff B023 and
  mypy's lambda-inference limitation at the same time.
- `RetryPolicy.run` accepts both exception-based and predicate-based
  failure semantics; `run_response` is shorthand for the common
  HTTP-retry path.
