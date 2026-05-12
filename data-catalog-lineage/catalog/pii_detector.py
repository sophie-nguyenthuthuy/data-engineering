import re
from typing import Any

# Column name patterns → PII category
_NAME_PATTERNS: list[tuple[re.Pattern, str]] = [
    (re.compile(r"\b(first[_\s]?name|last[_\s]?name|full[_\s]?name|middle[_\s]?name|surname|given[_\s]?name)\b", re.I), "NAME"),
    (re.compile(r"\b(email|e[_\s]?mail|email[_\s]?address)\b", re.I), "EMAIL"),
    (re.compile(r"\b(phone|phone[_\s]?number|mobile|cell|telephone|tel)\b", re.I), "PHONE"),
    (re.compile(r"\b(ssn|social[_\s]?security|social[_\s]?security[_\s]?number|sin)\b", re.I), "SSN"),
    (re.compile(r"(?:^|[_\s])(credit[_\s]?card|card[_\s]?number|cc[_\s]?number|pan)(?:[_\s]|$)", re.I), "CREDIT_CARD"),
    (re.compile(r"\b(address|street|addr|mailing[_\s]?address|billing[_\s]?address)\b", re.I), "ADDRESS"),
    (re.compile(r"\b(zip|zip[_\s]?code|postal[_\s]?code|postcode)\b", re.I), "ZIP_CODE"),
    (re.compile(r"\b(dob|date[_\s]?of[_\s]?birth|birth[_\s]?date|birthday)\b", re.I), "DATE_OF_BIRTH"),
    (re.compile(r"\b(passport|passport[_\s]?number)\b", re.I), "PASSPORT"),
    (re.compile(r"\b(driver[_\s]?license|drivers[_\s]?license|dl[_\s]?number|license[_\s]?number)\b", re.I), "DRIVERS_LICENSE"),
    (re.compile(r"\b(ip[_\s]?address|ip[_\s]?addr)\b", re.I), "IP_ADDRESS"),
    (re.compile(r"\b(gender|sex)\b", re.I), "GENDER"),
    (re.compile(r"\b(race|ethnicity|nationality)\b", re.I), "SENSITIVE_DEMOGRAPHIC"),
    (re.compile(r"\b(salary|income|wage|compensation|earnings)\b", re.I), "FINANCIAL"),
    (re.compile(r"\b(account[_\s]?number|bank[_\s]?account|routing[_\s]?number|iban)\b", re.I), "BANK_ACCOUNT"),
    (re.compile(r"(?:^|[_\s])(password|passwd|pwd|secret|api[_\s]?key|token|auth[_\s]?token)(?:[_\s]|$|hash|salt)", re.I), "CREDENTIAL"),
    (re.compile(r"\b(medical|diagnosis|prescription|health|disease|condition)\b", re.I), "HEALTH"),
    (re.compile(r"\b(location|latitude|longitude|lat|lng|geo)\b", re.I), "GEO_LOCATION"),
    (re.compile(r"\b(user[_\s]?id|userid|uid|customer[_\s]?id|person[_\s]?id)\b", re.I), "USER_ID"),
]

# Value-level regex patterns for sampling
_VALUE_PATTERNS: list[tuple[re.Pattern, str]] = [
    (re.compile(r"^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$"), "EMAIL"),
    (re.compile(r"^\d{3}[-.\s]?\d{2}[-.\s]?\d{4}$"), "SSN"),
    (re.compile(r"^\+?1?\s?\(?\d{3}\)?[\s.\-]?\d{3}[\s.\-]?\d{4}$"), "PHONE"),
    (re.compile(r"^\d{13,19}$"), "CREDIT_CARD"),
    (re.compile(r"^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$"), "IP_ADDRESS"),
    (re.compile(r"^\d{5}(-\d{4})?$"), "ZIP_CODE"),
]


def detect_pii_from_name(column_name: str) -> list[str]:
    tags: list[str] = []
    for pattern, label in _NAME_PATTERNS:
        if pattern.search(column_name):
            tags.append(label)
    return tags


def detect_pii_from_values(values: list[Any]) -> list[str]:
    found: set[str] = set()
    str_values = [str(v) for v in values if v is not None]
    for val in str_values[:50]:  # check first 50 samples
        for pattern, label in _VALUE_PATTERNS:
            if pattern.match(val.strip()):
                found.add(label)
    return list(found)


def detect_pii(column_name: str, sample_values: list[Any] | None = None) -> list[str]:
    tags_set: set[str] = set(detect_pii_from_name(column_name))
    if sample_values:
        tags_set.update(detect_pii_from_values(sample_values))
    tags = sorted(tags_set)
    if tags:
        tags = list(dict.fromkeys(["PII"] + tags))  # prepend generic PII tag
    return tags
