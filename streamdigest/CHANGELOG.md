# Changelog

All notable changes to this project will be documented in this file.
The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- GitHub Actions CI (ruff, mypy, pytest on Python 3.11 and 3.12).
- `CONTRIBUTING.md`, `CODE_OF_CONDUCT.md`, `SECURITY.md`.
- Issue and pull request templates.
- `Dockerfile` + `docker-compose.yml` bundling Ollama as a sidecar service.
- `Makefile` with `test` / `lint` / `run` / `evals` / `doctor` targets.
- `pre-commit` hooks: ruff, ruff-format, end-of-file-fixer, trailing-whitespace.
- Dependabot config for GitHub Actions + pip.
- MIT license.

## [0.1.0] — 2026-04-20

### Added
- Initial scaffold.
- `dlt` source for GitHub notifications with incremental loading on `updated_at`,
  writing to DuckDB under the `github` schema.
- Ollama-backed single-call enrichment: summary + action items + priority
  (1–5) + sentiment, with defensive coercion of model output.
- DuckDB `enriched.notifications` table and `enriched.digest` view joining
  raw + enriched rows.
- Prefect 3 flow orchestrating ingest → prepare → enrich.
- Eval harness with 4 fixtures (security, review-requested, noise, CI failure).
- Typer CLI: `doctor`, `init`, `run`, `digest`, `evals`.
- Smoke tests covering coercion, prompt assembly, and the storage round-trip.
- Stubs for Gmail, Slack, and Linear sources with implementation plans in their
  module docstrings.
- `TODO.md` tracking production hardening (OTel, RAG, integration tests,
  deployment).

[Unreleased]: https://github.com/sophie-nguyenthuthuy/streamdigest/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/sophie-nguyenthuthuy/streamdigest/releases/tag/v0.1.0
