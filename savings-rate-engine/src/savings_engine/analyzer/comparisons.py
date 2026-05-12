from savings_engine.models.db_models import RateRecord, Bank
from savings_engine.models.schemas import BankComparison
from savings_engine.storage.repository import RateRepository


def compare_banks(
    repo: RateRepository,
    term_days: int,
    rate_type: str = "standard",
    top_n: int = 10,
) -> list[BankComparison]:
    """Return ranked BankComparison list for a given term and rate type."""
    records: list[RateRecord] = [
        r for r in repo.get_best_rates(term_days, top_n=50)
        if r.rate_type == rate_type
    ]
    records.sort(key=lambda r: r.rate_pa, reverse=True)
    records = records[:top_n]

    results: list[BankComparison] = []
    for rank, rec in enumerate(records, start=1):
        bank = repo.get_bank(rec.bank_code)
        results.append(BankComparison(
            term_days=term_days,
            bank_code=rec.bank_code,
            bank_name_vi=bank.name_vi if bank else rec.bank_code,
            rate_pa=rec.rate_pa,
            rate_type=rec.rate_type,
            rank=rank,
            scraped_at=rec.snapshot.scraped_at if rec.snapshot else None,
        ))
    return results


def best_rates_table(repo: RateRepository, terms: list[int] | None = None) -> dict[int, list[BankComparison]]:
    """Return best-rate comparison for each term in `terms` (defaults to common terms)."""
    if terms is None:
        terms = [30, 90, 180, 365, 730]
    return {t: compare_banks(repo, t) for t in terms}
