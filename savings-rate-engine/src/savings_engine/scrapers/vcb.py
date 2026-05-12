"""
Vietcombank (VCB) scraper.

Rate page: https://www.vietcombank.com.vn/vi-VN/KHCN/TienGuiTietKiem
The page loads rates via an internal API endpoint discovered via devtools:
  POST https://www.vietcombank.com.vn/api/v1/saving-interest-rates
Response is JSON with a `data` list.  Falls back gracefully if the endpoint changes.
"""
import logging
from datetime import datetime
from bs4 import BeautifulSoup

from .base import BaseScraper, ScraperError
from savings_engine.models.schemas import RateEntry

logger = logging.getLogger(__name__)

_RATES_URL = "https://www.vietcombank.com.vn/vi-VN/KHCN/TienGuiTietKiem"
_API_URL = "https://www.vietcombank.com.vn/api/v1/saving-interest-rates"


class VCBScraper(BaseScraper):
    bank_code = "VCB"
    bank_name = "Vietcombank"

    def _fetch_rates(self) -> list[RateEntry]:
        # Try JSON API first
        try:
            return self._fetch_from_api()
        except Exception:
            pass
        # Fallback to HTML parsing
        return self._fetch_from_html()

    def _fetch_from_api(self) -> list[RateEntry]:
        resp = self._post(_API_URL, json={"currency": "VND"})
        data = resp.json()
        entries: list[RateEntry] = []
        for row in data.get("data", []):
            entries.append(RateEntry(
                bank_code=self.bank_code,
                term_label=row.get("termName", ""),
                rate_pa=float(row.get("interestRate", 0)),
                rate_type="online" if row.get("channel") == "online" else "standard",
                scraped_at=datetime.utcnow(),
            ))
        if not entries:
            raise ScraperError("VCB API returned no entries")
        return entries

    def _fetch_from_html(self) -> list[RateEntry]:
        resp = self._get(_RATES_URL)
        soup = BeautifulSoup(resp.text, "lxml")
        entries: list[RateEntry] = []

        for table in soup.select("table"):
            headers = [th.get_text(strip=True).lower() for th in table.select("th")]
            if not any("kỳ hạn" in h or "lãi suất" in h for h in headers):
                continue
            for row in table.select("tr")[1:]:
                cells = [td.get_text(strip=True) for td in row.select("td")]
                if len(cells) < 2:
                    continue
                term_label, rate_str = cells[0], cells[1].replace("%", "").replace(",", ".")
                try:
                    rate = float(rate_str)
                except ValueError:
                    continue
                entries.append(RateEntry(
                    bank_code=self.bank_code,
                    term_label=term_label,
                    rate_pa=rate,
                    scraped_at=datetime.utcnow(),
                ))

        if not entries:
            raise ScraperError("VCB HTML parsing produced no entries")
        return entries

    def _mock_rates(self) -> list[RateEntry]:
        now = datetime.utcnow()
        data = [
            ("Không kỳ hạn", 0.10, "standard"),
            ("1 tháng",      4.70, "standard"),
            ("2 tháng",      4.70, "standard"),
            ("3 tháng",      4.80, "standard"),
            ("6 tháng",      5.00, "standard"),
            ("9 tháng",      5.00, "standard"),
            ("12 tháng",     5.60, "standard"),
            ("18 tháng",     5.60, "standard"),
            ("24 tháng",     5.60, "standard"),
            ("36 tháng",     5.60, "standard"),
            ("1 tháng",      4.80, "online"),
            ("3 tháng",      4.90, "online"),
            ("6 tháng",      5.10, "online"),
            ("12 tháng",     5.70, "online"),
        ]
        return [
            RateEntry(bank_code=self.bank_code, term_label=t, rate_pa=r, rate_type=rt, scraped_at=now)
            for t, r, rt in data
        ]
