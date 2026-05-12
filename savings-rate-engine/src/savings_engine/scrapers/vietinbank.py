"""
VietinBank (CTG) scraper.

Rate page: https://www.vietinbank.vn/web/home/vn/products/saving-deposit
"""
import logging
from datetime import datetime
from bs4 import BeautifulSoup

from .base import BaseScraper, ScraperError
from savings_engine.models.schemas import RateEntry

logger = logging.getLogger(__name__)

_RATES_URL = "https://www.vietinbank.vn/web/home/vn/products/saving-deposit"


class VietinBankScraper(BaseScraper):
    bank_code = "CTG"
    bank_name = "VietinBank"

    def _fetch_rates(self) -> list[RateEntry]:
        resp = self._get(_RATES_URL)
        soup = BeautifulSoup(resp.text, "lxml")
        entries: list[RateEntry] = []

        for table in soup.select("table"):
            rows = table.select("tr")
            if len(rows) < 2:
                continue
            header_cells = rows[0].select("th, td")
            header_text = " ".join(c.get_text(strip=True).lower() for c in header_cells)
            if "kỳ hạn" not in header_text and "term" not in header_text:
                continue

            rate_type = "online" if "online" in header_text else "standard"
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
                    rate_type=rate_type,
                    scraped_at=datetime.utcnow(),
                ))

        if not entries:
            raise ScraperError("VietinBank: no entries parsed")
        return entries

    def _mock_rates(self) -> list[RateEntry]:
        now = datetime.utcnow()
        data = [
            ("Không kỳ hạn", 0.10, "standard"),
            ("1 tháng",      4.70, "standard"),
            ("3 tháng",      4.80, "standard"),
            ("6 tháng",      5.10, "standard"),
            ("9 tháng",      5.10, "standard"),
            ("12 tháng",     5.60, "standard"),
            ("24 tháng",     5.60, "standard"),
            ("36 tháng",     5.60, "standard"),
        ]
        return [
            RateEntry(bank_code=self.bank_code, term_label=t, rate_pa=r, rate_type=rt, scraped_at=now)
            for t, r, rt in data
        ]
