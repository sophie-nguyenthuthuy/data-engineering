"""
BIDV scraper.

Rate page: https://www.bidv.com.vn/vi/san-pham-dich-vu/ngan-hang-ban-le/tien-gui
Attempts HTML table parsing.  Most banks in this tier use server-rendered tables
for regulatory transparency, so lxml works well here.
"""
import logging
from datetime import datetime
from bs4 import BeautifulSoup

from .base import BaseScraper, ScraperError
from savings_engine.models.schemas import RateEntry

logger = logging.getLogger(__name__)

_RATES_URL = "https://www.bidv.com.vn/vi/san-pham-dich-vu/ngan-hang-ban-le/tien-gui"


class BIDVScraper(BaseScraper):
    bank_code = "BIDV"
    bank_name = "BIDV"

    def _fetch_rates(self) -> list[RateEntry]:
        resp = self._get(_RATES_URL)
        soup = BeautifulSoup(resp.text, "lxml")
        entries: list[RateEntry] = []

        for table in soup.select("table.interest-rate, table.rate-table, table"):
            rows = table.select("tr")
            if len(rows) < 3:
                continue
            header_text = " ".join(th.get_text(strip=True).lower() for th in rows[0].select("th, td"))
            if "kỳ hạn" not in header_text and "lãi suất" not in header_text:
                continue

            for row in rows[1:]:
                cells = [td.get_text(strip=True) for td in row.select("td")]
                if len(cells) < 2:
                    continue
                term_label = cells[0]
                rate_str = cells[1].replace("%", "").replace(",", ".").strip()
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
            raise ScraperError("BIDV: no rate rows parsed")
        return entries

    def _mock_rates(self) -> list[RateEntry]:
        now = datetime.utcnow()
        data = [
            ("Không kỳ hạn", 0.10, "standard"),
            ("1 tháng",      4.70, "standard"),
            ("2 tháng",      4.70, "standard"),
            ("3 tháng",      5.00, "standard"),
            ("6 tháng",      5.30, "standard"),
            ("9 tháng",      5.30, "standard"),
            ("12 tháng",     5.80, "standard"),
            ("18 tháng",     5.80, "standard"),
            ("24 tháng",     5.80, "standard"),
            ("36 tháng",     5.80, "standard"),
        ]
        return [
            RateEntry(bank_code=self.bank_code, term_label=t, rate_pa=r, rate_type=rt, scraped_at=now)
            for t, r, rt in data
        ]
