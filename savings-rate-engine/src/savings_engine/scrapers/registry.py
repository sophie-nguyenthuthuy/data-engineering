from .vcb import VCBScraper
from .bidv import BIDVScraper
from .vietinbank import VietinBankScraper
from .techcombank import TechcombankScraper
from .mb import MBBankScraper
from .acb import ACBScraper
from .vpbank import VPBankScraper
from .base import BaseScraper

# Map bank_code → scraper class. Add new scrapers here.
SCRAPER_REGISTRY: dict[str, type[BaseScraper]] = {
    "VCB":  VCBScraper,
    "BIDV": BIDVScraper,
    "CTG":  VietinBankScraper,
    "TCB":  TechcombankScraper,
    "MBB":  MBBankScraper,
    "ACB":  ACBScraper,
    "VPB":  VPBankScraper,
}


def get_scraper(bank_code: str) -> BaseScraper:
    cls = SCRAPER_REGISTRY.get(bank_code.upper())
    if cls is None:
        raise KeyError(f"No scraper registered for bank code '{bank_code}'")
    return cls()
