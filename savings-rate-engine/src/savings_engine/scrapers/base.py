import logging
from abc import ABC, abstractmethod

import requests
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from savings_engine.config import settings
from savings_engine.models.schemas import RateEntry

logger = logging.getLogger(__name__)


class ScraperError(Exception):
    pass


class BaseScraper(ABC):
    bank_code: str
    bank_name: str

    def __init__(self) -> None:
        self._session = requests.Session()
        self._session.headers.update({
            "User-Agent": settings.user_agent,
            "Accept-Language": "vi-VN,vi;q=0.9,en-US;q=0.8",
        })

    @abstractmethod
    def _fetch_rates(self) -> list[RateEntry]:
        """Fetch and parse rates from the bank website. Raise ScraperError on failure."""

    @abstractmethod
    def _mock_rates(self) -> list[RateEntry]:
        """Return realistic mock data for testing / fallback."""

    def scrape(self) -> list[RateEntry]:
        if settings.use_mock_data:
            logger.debug("%s: using mock data", self.bank_code)
            return self._mock_rates()
        try:
            return self._fetch_with_retry()
        except Exception as exc:
            logger.warning("%s: live scrape failed (%s), falling back to mock", self.bank_code, exc)
            raise ScraperError(str(exc)) from exc

    @retry(
        retry=retry_if_exception_type(requests.RequestException),
        stop=stop_after_attempt(settings.request_retry_attempts),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        reraise=True,
    )
    def _fetch_with_retry(self) -> list[RateEntry]:
        return self._fetch_rates()

    def _get(self, url: str, **kwargs) -> requests.Response:
        resp = self._session.get(url, timeout=settings.request_timeout_seconds, **kwargs)
        resp.raise_for_status()
        return resp

    def _post(self, url: str, **kwargs) -> requests.Response:
        resp = self._session.post(url, timeout=settings.request_timeout_seconds, **kwargs)
        resp.raise_for_status()
        return resp
