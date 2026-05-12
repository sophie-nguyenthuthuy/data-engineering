"""Prompt templates for enrichment.

Single-pass design: one LLM call per notification producing summary,
actions, priority, and sentiment in one JSON object. Cheaper than four
round-trips and lets the model reason jointly (a PR review request with
a failing CI is higher priority than either signal alone).
"""

ENRICH_SYSTEM = """\
You triage developer notifications. For each notification, return a compact
JSON object with exactly these keys:

  summary:         one sentence, <= 25 words, plain English, no markdown
  actions:         array of 0-3 short imperative strings (what the user should do)
  priority:        integer 1-5 where
                     5 = production-critical / blocking / security
                     4 = review requested of me, or CI broken on my PR
                     3 = mention / assigned / decision awaited
                     2 = FYI in a repo I own
                     1 = pure noise
  priority_reason: one short phrase explaining the priority
  sentiment:       one of "positive", "neutral", "negative", "mixed"

Return ONLY the JSON object. No prose, no code fences.
"""

def build_user_prompt(
    *,
    repo: str | None,
    subject_type: str | None,
    title: str | None,
    reason: str | None,
    body: str | None,
    body_char_limit: int = 2000,
) -> str:
    body = (body or "").strip()
    if len(body) > body_char_limit:
        body = body[:body_char_limit] + "…[truncated]"

    return (
        f"Repository: {repo or '(unknown)'}\n"
        f"Kind: {subject_type or '(unknown)'}\n"
        f"Reason: {reason or '(unknown)'}\n"
        f"Title: {title or '(no title)'}\n"
        f"Body:\n{body or '(empty)'}\n"
    )
