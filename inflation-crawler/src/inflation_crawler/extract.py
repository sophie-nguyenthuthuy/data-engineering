"""Structured product/price extraction.

Strategy (highest-signal first):
  1. JSON-LD `Product` schema — Google/Bing require it, so most retailers ship it.
  2. Microdata (schema.org/Product) via extruct.
  3. OpenGraph product tags.
  4. Heuristic fallback: meta tags + price-parser on visible text.

The LLM fallback (Anthropic) is optional and used only when the structured
paths yield nothing, since it costs money. Set IC_ANTHROPIC_API_KEY to enable.
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import extruct
from price_parser import Price
from selectolax.parser import HTMLParser

from .config import settings
from .logging import get_logger

log = get_logger(__name__)


@dataclass(slots=True)
class Product:
    product_id: str
    url: str
    title: str
    brand: str | None
    price: float
    currency: str
    category: str | None
    fetch_time: datetime
    source: str  # jsonld | microdata | opengraph | heuristic | llm


_CURRENCY_SYMBOLS = {"$": "USD", "£": "GBP", "€": "EUR", "¥": "JPY"}


def _stable_id(url: str, title: str) -> str:
    # Canonical ID: strip query strings so the same item across visits collapses.
    canonical = re.sub(r"\?.*$", "", url.lower())
    h = hashlib.sha1(f"{canonical}|{title.lower().strip()}".encode()).hexdigest()
    return h[:16]


def _parse_fetch_time(s: str) -> datetime:
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except ValueError:
        return datetime.utcnow()


def _normalize_price(value: Any, currency_hint: str | None = None) -> tuple[float, str] | None:
    if value is None:
        return None
    parsed = Price.fromstring(str(value))
    if parsed.amount is None:
        return None
    currency = parsed.currency or currency_hint or "USD"
    if currency in _CURRENCY_SYMBOLS:
        currency = _CURRENCY_SYMBOLS[currency]
    return float(parsed.amount), currency


def _from_jsonld(items: list[dict[str, Any]]) -> dict[str, Any] | None:
    for item in items:
        graph = item.get("@graph", [item]) if isinstance(item, dict) else []
        for node in graph:
            if not isinstance(node, dict):
                continue
            t = node.get("@type")
            if t == "Product" or (isinstance(t, list) and "Product" in t):
                return node
    return None


def _price_from_offers(offers: Any) -> tuple[float, str] | None:
    if isinstance(offers, list):
        offers = offers[0] if offers else None
    if not isinstance(offers, dict):
        return None
    # Schema.org: Offer.price + priceCurrency, or AggregateOffer.lowPrice.
    price = offers.get("price") or offers.get("lowPrice")
    currency = offers.get("priceCurrency")
    return _normalize_price(price, currency)


def _extract_jsonld(html: str) -> dict[str, Any] | None:
    data = extruct.extract(html, syntaxes=["json-ld"], uniform=True).get("json-ld", [])
    product = _from_jsonld(data)
    if not product:
        return None
    priced = _price_from_offers(product.get("offers"))
    if not priced:
        return None
    return {
        "title": str(product.get("name") or "").strip(),
        "brand": (product.get("brand", {}).get("name")
                  if isinstance(product.get("brand"), dict) else product.get("brand")),
        "price": priced[0],
        "currency": priced[1],
        "category": product.get("category"),
        "source": "jsonld",
    }


def _extract_microdata(html: str) -> dict[str, Any] | None:
    data = extruct.extract(html, syntaxes=["microdata"], uniform=True).get("microdata", [])
    for item in data:
        if "Product" not in str(item.get("@type", "")):
            continue
        props = item.get("properties", item)
        priced = _price_from_offers(props.get("offers"))
        if not priced:
            continue
        return {
            "title": str(props.get("name") or "").strip(),
            "brand": props.get("brand") if isinstance(props.get("brand"), str) else None,
            "price": priced[0],
            "currency": priced[1],
            "category": props.get("category"),
            "source": "microdata",
        }
    return None


def _extract_opengraph(html: str) -> dict[str, Any] | None:
    tree = HTMLParser(html)
    def meta(prop: str) -> str | None:
        node = tree.css_first(f'meta[property="{prop}"]')
        return node.attributes.get("content") if node else None

    price_val = meta("product:price:amount") or meta("og:price:amount")
    currency = meta("product:price:currency") or meta("og:price:currency") or "USD"
    title = meta("og:title")
    if not (price_val and title):
        return None
    priced = _normalize_price(price_val, currency)
    if not priced:
        return None
    return {
        "title": title.strip(),
        "brand": meta("product:brand"),
        "price": priced[0],
        "currency": priced[1],
        "category": None,
        "source": "opengraph",
    }


_PRICE_NEAR_BUY = re.compile(r"[\$£€][\s]?\d+(?:[.,]\d{2})?")


def _extract_heuristic(html: str) -> dict[str, Any] | None:
    tree = HTMLParser(html)
    title_node = tree.css_first("h1") or tree.css_first("title")
    if not title_node:
        return None
    title = title_node.text(strip=True)

    # Scan elements that look like prices (common itemprop or class hints first).
    for selector in ('[itemprop="price"]', '[class*="price"]', '[data-price]'):
        node = tree.css_first(selector)
        if not node:
            continue
        text = node.attributes.get("content") or node.text(strip=True)
        priced = _normalize_price(text)
        if priced:
            return {
                "title": title,
                "brand": None,
                "price": priced[0],
                "currency": priced[1],
                "category": None,
                "source": "heuristic",
            }

    # Last resort: regex scan of body text.
    body = tree.body.text() if tree.body else ""
    match = _PRICE_NEAR_BUY.search(body)
    if match:
        priced = _normalize_price(match.group(0))
        if priced:
            return {
                "title": title,
                "brand": None,
                "price": priced[0],
                "currency": priced[1],
                "category": None,
                "source": "heuristic",
            }
    return None


def extract_product(html: str, url: str, fetch_time: str) -> Product | None:
    for extractor in (_extract_jsonld, _extract_microdata, _extract_opengraph, _extract_heuristic):
        try:
            result = extractor(html)
        except Exception as exc:  # noqa: BLE001 - extractors are best-effort
            log.debug("extract.error", extractor=extractor.__name__, error=str(exc))
            continue
        if result and result["title"] and result["price"] > 0:
            return Product(
                product_id=_stable_id(url, result["title"]),
                url=url,
                title=result["title"][:500],
                brand=result.get("brand"),
                price=result["price"],
                currency=result["currency"],
                category=result.get("category"),
                fetch_time=_parse_fetch_time(fetch_time),
                source=result["source"],
            )
    return None
