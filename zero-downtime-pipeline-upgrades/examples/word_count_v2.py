"""
Example pipeline v2 — improved word-count with proper punctuation stripping
and unicode-aware tokenisation.  Produces the same output schema as v1 so the
comparator can diff field-by-field.
"""

import re
from typing import Any, Dict

from pipeline_deployer import BasePipeline

# Matches any sequence of unicode word characters
_TOKEN_RE = re.compile(r"\w+", re.UNICODE)


class WordCountV2(BasePipeline):
    """
    Counts words per document using a regex tokeniser that correctly handles
    punctuation, hyphenation, and unicode text.

    The running totals mirror v1 exactly when inputs are clean ASCII; they
    diverge on punctuation-heavy text — which is precisely the signal the
    shadow runner detects.
    """

    @property
    def version(self) -> str:
        return "v2.0.0"

    def setup(self) -> None:
        self._total_words = 0
        self._doc_count = 0

    def process(self, record: Dict[str, Any]) -> Dict[str, Any]:
        text: str = record.get("text", "")
        tokens = _TOKEN_RE.findall(text)
        word_count = len(tokens)

        self._total_words += word_count
        self._doc_count += 1

        return {
            "doc_id": record.get("doc_id"),
            "word_count": word_count,
            "running_total": self._total_words,
            "avg_words_per_doc": self._total_words / self._doc_count,
        }

    def snapshot_state(self) -> Dict[str, Any]:
        return {"total_words": self._total_words, "doc_count": self._doc_count}

    def restore_state(self, snapshot: Dict[str, Any]) -> None:
        self._total_words = snapshot.get("total_words", 0)
        self._doc_count = snapshot.get("doc_count", 0)
