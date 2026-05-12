# Contributing to streamdigest

Thanks for considering a contribution. This project is small and opinionated —
the goal is a local-first, easy-to-hack-on reference for orchestrated
AI-enriched ingestion. PRs that keep it that way are very welcome.

## Dev setup

```bash
git clone <your-fork>
cd streamdigest
python3.11 -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
pre-commit install
```

You also need a local Ollama daemon for the AI layer and the eval harness:

```bash
# macOS
brew install ollama
ollama serve &
ollama pull llama3.2:3b
```

Then copy `.env.example` to `.env` and set `GITHUB_TOKEN`.

## Running things

```bash
make test          # pytest
make lint          # ruff + mypy
make evals         # run the eval harness (requires Ollama)
make run           # ingest + enrich end-to-end (requires Ollama + GITHUB_TOKEN)
```

## Coding standards

- **Formatting + linting:** `ruff` (config in `pyproject.toml`). CI blocks on lint.
- **Types:** `mypy` on the `streamdigest/` package. Add type hints on all
  public functions.
- **Tests:** `pytest`. Smoke tests must not require network or Ollama — if a
  test needs either, mark it with `@pytest.mark.integration` and skip in CI's
  default matrix.
- **Line length:** 100.
- **Python versions:** 3.11 and 3.12.

## What makes a good PR

- **Scoped:** one thing at a time. A new source is one PR. A prompt tweak is
  another. Don't bundle.
- **Evals attached:** if you change the prompt or the enrichment coercion,
  add a fixture to `streamdigest/evals/fixtures.json` that would have caught
  the regression you're fixing or the behavior you're adding.
- **README updated** when the CLI surface or the dependency list changes.
- **CHANGELOG updated** under `[Unreleased]`.

## Adding a new source

1. Drop a module in `streamdigest/sources/<name>.py`.
2. Emit rows with this canonical shape so the enrichment layer stays
   source-agnostic:
   ```python
   {
       "id":              str,           # stable unique id
       "repo_full_name":  str | None,    # team/project/channel/etc.
       "subject_type":    str,           # "Issue" / "Email" / "SlackMessage" / ...
       "subject_title":   str,
       "reason":          str,           # why this landed in the user's inbox
       "body":            str,
       "updated_at":      str,           # ISO-8601
   }
   ```
3. Add config fields to `Settings` in `streamdigest/config.py`.
4. Add a CLI flag (or a new Prefect flow) to invoke it.
5. Stubs for the other sources already exist in `streamdigest/sources/` —
   follow their docstring plans.

## Reporting issues

Use the [issue templates](.github/ISSUE_TEMPLATE/). For security issues, see
[SECURITY.md](./SECURITY.md) — please do not open a public issue.
