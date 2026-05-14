"""Tests for the Redis Lua-script emitter."""

from __future__ import annotations

from arlo.storage.redis_lua import REDIS_TOKEN_BUCKET_LUA, render_redis_lua


def test_lua_script_has_required_redis_calls():
    src = render_redis_lua()
    # Atomic refill + take must use HMGET + HMSET + PEXPIRE.
    assert "HMGET" in src
    assert "HMSET" in src
    assert "PEXPIRE" in src


def test_lua_script_uses_argv_for_capacity_refill_requested_now():
    src = render_redis_lua()
    assert "ARGV[1]" in src and "capacity" in src
    assert "ARGV[2]" in src and "refill" in src
    assert "ARGV[3]" in src and "requested" in src
    assert "ARGV[4]" in src and "now" in src


def test_lua_script_returns_triple():
    src = render_redis_lua()
    assert "return {took" in src


def test_lua_script_is_deterministic():
    assert render_redis_lua() == render_redis_lua()
    assert render_redis_lua() == REDIS_TOKEN_BUCKET_LUA


def test_lua_script_caps_at_capacity():
    src = render_redis_lua()
    assert "math.min(capacity," in src


def test_lua_script_clamps_negative_elapsed_to_zero():
    src = render_redis_lua()
    assert "elapsed < 0 then elapsed = 0" in src
