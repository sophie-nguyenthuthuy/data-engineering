"""Redis-Lua atomic-take script.

This module **does not depend on redis-py** — we just emit the Lua
source. Callers wire it up by passing the script string to their
Redis client's `EVAL` (or `register_script` for cached SHA-based
calls).

The script implements exactly the same algorithm as
:class:`InMemoryStorage.atomic_take` so the two backends are
behaviourally interchangeable from the bucket's point of view.

KEYS[1] = bucket key
ARGV[1] = capacity
ARGV[2] = refill_per_second
ARGV[3] = requested
ARGV[4] = now (unix seconds, may have fractional digits)
"""

from __future__ import annotations

import textwrap

REDIS_TOKEN_BUCKET_LUA = textwrap.dedent("""\
    -- Atomic refill + (maybe) deduct, mirroring InMemoryStorage.atomic_take.
    local key       = KEYS[1]
    local capacity  = tonumber(ARGV[1])
    local refill    = tonumber(ARGV[2])
    local requested = tonumber(ARGV[3])
    local now       = tonumber(ARGV[4])

    local data = redis.call('HMGET', key, 'tokens', 'last')
    local tokens
    local last
    if data[1] == false then
      tokens = capacity
      last = now
    else
      local prev_tokens = tonumber(data[1])
      local prev_last   = tonumber(data[2])
      local elapsed = now - prev_last
      if elapsed < 0 then elapsed = 0 end
      tokens = math.min(capacity, prev_tokens + elapsed * refill)
      last = now
    end

    local took = 0
    if requested <= tokens then
      tokens = tokens - requested
      took = 1
    end

    redis.call('HMSET', key, 'tokens', tokens, 'last', last)
    -- Expire after one full refill window so stale keys don't grow.
    redis.call('PEXPIRE', key, math.ceil((capacity / refill) * 2 * 1000))

    return {took, tostring(tokens), tostring(last)}
    """).strip()


def render_redis_lua() -> str:
    """Return the Lua source. Kept as a function so the consumer pattern
    matches the future ``render_redis_lua(prefix=...)`` extension."""
    return REDIS_TOKEN_BUCKET_LUA


__all__ = ["REDIS_TOKEN_BUCKET_LUA", "render_redis_lua"]
