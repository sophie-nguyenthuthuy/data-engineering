# CI (Phase 6)

GitHub Actions split into three workflows, each with a narrow
responsibility and its own concurrency group so pushes don't pile up.

| Workflow           | Trigger                                    | Gates merge? | Typical runtime |
|--------------------|--------------------------------------------|--------------|-----------------|
| `ci.yml`           | every push / PR                            | yes          | ~3 min          |
| `containers.yml`   | every push / PR + nightly cron             | no (report)  | ~6 min          |
| `smoke.yml`        | push to `main`, PRs with `smoke` label     | yes when it runs | ~8 min      |

## `ci.yml` — lint, type, test

Matrix over `{producer, api, orchestrator} × {ruff, mypy, pytest}`, so
9 jobs minus one excluded cell:

- `orchestrator × mypy` is skipped. Dagster's generated types + PySpark
  stubs drift between minor releases; chasing the errors costs more
  than it catches. Ruff + pytest still run.

Two extra jobs round it out:
- `compose-config` — parses both compose files with `--env-file .env.example`.
  Catches variable/YAML regressions without running anything.
- `grafana-dashboards` — loads each dashboard JSON via `json.load` so a
  trailing comma can't silently break the Pipeline Overview board.

## `containers.yml` — Dockerfile + image hygiene

- **Hadolint** runs per Dockerfile (`services/{producer,api,orchestrator}`,
  `infra/kafka-connect`). SARIF uploaded to the Security tab.
- **Trivy filesystem** scan of the repo root for pinned-dependency CVEs.
- **Trivy image** scan of each built image, cached via `type=gha` so
  rebuilds on unchanged layers are near-instant.

All three are `exit-code: "0"` / `no-fail: true` — they surface findings
as triage input, not merge blockers. Tighten severity thresholds once a
baseline is clean.

Scheduled daily at 03:17 UTC (off-the-hour to miss the scheduler
thundering herd) to catch base-image CVEs that land after the repo is
quiet.

## `smoke.yml` — end-to-end compose test

Brings up the critical-path subset (Kafka + Connect + Schema Registry +
ClickHouse + producer + API + dashboard), registers the sink connector,
and asserts:

1. `GET /healthz` returns `"status":"ok"` within 120s, and
2. `SELECT count() FROM user_interactions` > 100 within 180s.

Skips the observability and orchestration stacks to keep the runner
under 7 GB of RAM; those are covered by `ci.yml`'s compose-config pass.

**Why it's label-gated on PRs:** It's ~8 minutes and CI minutes aren't
free. Adding the `smoke` label to a PR (or pushing to `main`) runs it.

## Dependabot

Weekly updates, grouped per service:

- **OTel cross-package release** collapses into one PR per service
  (pattern match on `opentelemetry-*`).
- **Dagster ecosystem** bumps together (`dagster*`).
- **Dev dependencies** (ruff, mypy, pytest) separate from prod deps so
  they can be merged on a quicker cadence.

GitHub Actions and Docker base images are also covered.

## Pre-commit

Install with `pre-commit install`. Scope is deliberately narrow:

- `trailing-whitespace`, `end-of-file-fixer`, `check-yaml`, `check-json`
- `ruff` + `ruff-format` (matches the CI pin at 0.6.9)
- `shellcheck` for the scripts under `scripts/` and `infra/kafka/`
- `detect-secrets` with a baseline file — blocks new high-entropy
  strings or credential-looking values. The `secrets/` directory is
  `.gitignored` so bootstrap-generated dev creds never reach staging.

Mypy and pytest are **not** pre-commit hooks — they live in CI so that
commits stay quick. Run them locally via `make test` / `make lint`
before pushing.

## What's deliberately out of scope

- **CodeQL**. Python repositories with this much compiled-C-dep
  surface (librdkafka, PySpark) get noisy results. Revisit when there's
  a clear threat model.
- **Publishing images to GHCR / a registry**. The smoke test builds
  locally; no deploy target consumes them yet. Phase 7 (Terraform) is
  where ECR gets wired up.
- **Required-status-check enforcement**. The `ci.yml` jobs are safe to
  mark required on the branch-protection screen; `smoke.yml` is not,
  because it doesn't run on every PR by design.
