# Production hardening — TODO

This scaffold is a working skeleton. The following are stubbed or deferred.

## Observability
- [ ] OpenTelemetry: instrument `ingest_github` and `enrich_batch` with
      spans + structured logs. Export to local Jaeger / Honeycomb via OTLP.
- [ ] Prometheus metrics: counters for `enrichment_ok`, `enrichment_failed`,
      histogram for Ollama latency, gauge for backlog size.
- [ ] Prefect artifacts: attach the top-20 digest as a markdown artifact
      each run so flow-run URLs are immediately useful.

## Testing
- [ ] Integration test that spins up a real DuckDB + mocked GitHub API
      (respx) and asserts the merge/dedupe semantics.
- [ ] Prompt-regression: pin a snapshot of `build_user_prompt` output per fixture
      and diff in CI so prompt changes are always reviewed.
- [ ] Property-based test for `_coerce` with hypothesis (never crash,
      always produce a clamped priority).

## CI
- [ ] `.github/workflows/ci.yml`: ruff + mypy + pytest on 3.11 / 3.12.
- [ ] Cache `uv` installs; run Ollama in a service container with a tiny
      model (phi3:mini) to gate eval regressions.

## Retrieval-Augmented enrichment (RAG)
- [ ] Embed each notification body (bge-small via Ollama) and store
      vectors in DuckDB's `vss` extension.
- [ ] On enrich, retrieve the 3 most similar prior notifications from
      the same repo and include them as context — improves priority
      calibration ("this is the 4th CI failure this week" should bump
      priority higher than a one-off).

## AI layer
- [ ] Thread-awareness: when the subject is a PR, pull review comments
      and decision state, not just the top-level body.
- [ ] Auto-draft replies for review-requested PRs (held behind a flag
      until eval coverage is solid).
- [ ] Model-switching: route "security" or "production" reasons to a
      larger model (e.g. llama3.1:70b via remote Ollama) and keep the
      small model for noise.

## Sources
- [ ] Implement `gmail.py`, `slack.py`, `linear.py` (plans in each module's
      docstring).
- [ ] Canonicalize source output into a shared `Event` dataclass so the
      enrichment layer is source-agnostic.

## Deployment
- [ ] `prefect deploy` config + a cron schedule (every 10 min, workdays).
- [ ] Dockerfile that bundles the package + a local Ollama sidecar for
      one-command self-hosting.
