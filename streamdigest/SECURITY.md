# Security Policy

## Supported versions

Only the latest release of `streamdigest` is supported with security updates.
The project is pre-1.0 — breaking changes can land in any minor version.

## Reporting a vulnerability

**Do not open a public GitHub issue for security reports.**

Instead, use GitHub's private vulnerability reporting feature:
**Repo → Security → Report a vulnerability**.

Please include:

- A description of the issue and its impact.
- Reproduction steps or a proof-of-concept.
- Affected version / commit SHA.
- Your preferred contact for follow-up.

You can expect an initial acknowledgement within 5 business days and a status
update within 14 days. Once the issue is confirmed and fixed, we'll coordinate
a disclosure date with you and credit you in the release notes if you wish.

## Scope

In scope:

- The `streamdigest` package itself (ingestion, storage, AI layer, flows,
  CLI, evals).
- The Dockerfile / docker-compose.

Out of scope:

- Vulnerabilities in upstream dependencies (`dlt`, `prefect`, `duckdb`,
  `ollama`, etc.) — please report those to the upstream project and we'll
  bump the pin once a fix is available.
- Local-only attacks that already require code execution on the user's machine.
