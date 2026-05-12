from dataclasses import dataclass


@dataclass(frozen=True)
class TierLimits:
    storage_bytes: int           # -1 = unlimited
    requests_per_minute: int     # token-bucket refill rate
    burst_requests: int          # token-bucket capacity
    concurrent_jobs: int
    max_rows: int                # -1 = unlimited


TIERS: dict[str, TierLimits] = {
    "free": TierLimits(
        storage_bytes=1 * 1024**3,       # 1 GB
        requests_per_minute=10,
        burst_requests=20,
        concurrent_jobs=1,
        max_rows=100_000,
    ),
    "starter": TierLimits(
        storage_bytes=10 * 1024**3,      # 10 GB
        requests_per_minute=60,
        burst_requests=120,
        concurrent_jobs=3,
        max_rows=5_000_000,
    ),
    "pro": TierLimits(
        storage_bytes=100 * 1024**3,     # 100 GB
        requests_per_minute=300,
        burst_requests=600,
        concurrent_jobs=10,
        max_rows=50_000_000,
    ),
    "enterprise": TierLimits(
        storage_bytes=-1,
        requests_per_minute=10_000,
        burst_requests=20_000,
        concurrent_jobs=100,
        max_rows=-1,
    ),
}
