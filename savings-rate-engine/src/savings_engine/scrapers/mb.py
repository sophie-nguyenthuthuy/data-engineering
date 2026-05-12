"""
MB Bank (MBB) scraper.

Rate page: https://www.mbbank.com.vn/san-pham/tiet-kiem
"""
import logging
from datetime import datetime
from bs4 import BeautifulSoup

from .base import BaseScraper, ScraperError
from savings_engine.models.schemas import RateEntry

logger = logging.getLogger(__name__)

_RATES_URL = "https://www.mbbank.com.vn/san-pham/tiet-kiem"


class MBBankScraper(BaseScraper):
    bank_code = "MBB"
    bank_name = "MB Bank"

    def _fetch_rates(self) -> list[RateEntry]:
        resp = self._get(_RATES_URL)
        soup = BeautifulSoup(resp.text, "lxml")
        entries: list[RateEntry] = []

        for table in soup.select("table"):
            rows = table.select("tr")
            if not rows:
                continue
            header_text = " ".join(c.get_text(strip=True).lower() for c in rows[0].select("th,td"))
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
            raise ScraperError("MBB: no entries parsed")
        return entries

    def _mock_rates(self) -> list[RateEntry]:
        now = datetime.utcnow()
        data = [
            ("Không kỳ hạn", 0.10, "standard"),
            ("1 tháng",      4.90, "standard"),
            ("3 tháng",      5.10, "standard"),
            ("6 tháng",      5.40, "standard"),
            ("9 tháng",      5.40, "standard"),
            ("12 tháng",     5.90, "standard"),
            ("18 tháng",     5.90, "standard"),
            ("24 tháng",     5.90, "standard"),
            ("36 tháng",     5.90, "standard"),
            ("1 tháng",      5.00, "online"),
            ("3 tháng",      5.20, "online"),
            ("6 tháng",      5.50, "online"),
            ("12 tháng",     6.00, "online"),
        ]
        return [
            RateEntry(bank_code=self.bank_code, term_label=t, rate_pa=r, rate_type=rt, scraped_at=now)
            for t, r, rt in data
        ]
