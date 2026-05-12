"""
Techcombank (TCB) scraper.

TCB exposes rates at:
  https://www.techcombank.com.vn/api/v1/interest-rates/saving?currency=VND
(discovered via Network tab — may require a specific Referer header)
"""
import logging
from datetime import datetime
from bs4 import BeautifulSoup

from .base import BaseScraper, ScraperError
from savings_engine.models.schemas import RateEntry

logger = logging.getLogger(__name__)

_API_URL = "https://www.techcombank.com.vn/api/v1/interest-rates/saving"
_RATES_URL = "https://www.techcombank.com.vn/tai-chinh-ca-nhan/tiet-kiem"


class TechcombankScraper(BaseScraper):
    bank_code = "TCB"
    bank_name = "Techcombank"

    def _fetch_rates(self) -> list[RateEntry]:
        # Try the JSON API first
        try:
            return self._fetch_api()
        except Exception:
            pass
        return self._fetch_html()

    def _fetch_api(self) -> list[RateEntry]:
        resp = self._get(_API_URL, params={"currency": "VND"}, headers={"Referer": _RATES_URL})
        data = resp.json()
        entries: list[RateEntry] = []
        for row in data.get("items", data.get("data", [])):
            term = row.get("term") or row.get("termName", "")
            rate = float(row.get("rate") or row.get("interestRate", 0))
            rate_type = "online" if row.get("isOnline") else "standard"
            entries.append(RateEntry(
                bank_code=self.bank_code,
                term_label=str(term),
                rate_pa=rate,
                rate_type=rate_type,
                scraped_at=datetime.utcnow(),
            ))
        if not entries:
            raise ScraperError("TCB API returned no entries")
        return entries

    def _fetch_html(self) -> list[RateEntry]:
        resp = self._get(_RATES_URL)
        soup = BeautifulSoup(resp.text, "lxml")
        entries: list[RateEntry] = []

        for table in soup.select("table"):
            rows = table.select("tr")
            header_text = " ".join(c.get_text(strip=True).lower() for c in rows[0].select("th,td")) if rows else ""
            if "kỳ hạn" not in header_text and "lãi suất" not in header_text:
                continue
            for row in rows[1:]:
                cells = [td.get_text(strip=True) for td in row.select("td")]
                if len(cells) < 2:
                    continue
                rate_str = cells[1].replace("%", "").replace(",", ".").strip()
                try:
                    entries.append(RateEntry(
                        bank_code=self.bank_code, term_label=cells[0],
                        rate_pa=float(rate_str), scraped_at=datetime.utcnow(),
                    ))
                except ValueError:
                    continue

        if not entries:
            raise ScraperError("TCB HTML parsing produced no entries")
        return entries

    def _mock_rates(self) -> list[RateEntry]:
        now = datetime.utcnow()
        data = [
            ("Không kỳ hạn", 0.10, "standard"),
            ("1 tháng",      5.00, "standard"),
            ("3 tháng",      5.20, "standard"),
            ("6 tháng",      5.50, "standard"),
            ("12 tháng",     6.00, "standard"),
            ("18 tháng",     6.00, "standard"),
            ("24 tháng",     6.00, "standard"),
            ("1 tháng",      5.10, "online"),
            ("3 tháng",      5.30, "online"),
            ("6 tháng",      5.60, "online"),
            ("12 tháng",     6.10, "online"),
        ]
        return [
            RateEntry(bank_code=self.bank_code, term_label=t, rate_pa=r, rate_type=rt, scraped_at=now)
            for t, r, rt in data
        ]
