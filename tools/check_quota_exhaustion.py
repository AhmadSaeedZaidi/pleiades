#!/usr/bin/env python3
"""Check exhaustion status of all YouTube API keys and suggest pool rebalancing."""

import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "atlas" / "src"))

# Load env manually (don't trigger Settings which needs all vars)
env_path = Path(__file__).parent.parent / ".env"
env = {}
with open(env_path) as f:
    for line in f:
        line = line.strip()
        if line and not line.startswith('#') and '=' in line:
            key, value = line.split('=', 1)
            env[key] = value.strip().strip('"').strip("'")

api_keys: list[str] = json.loads(env.get('YOUTUBE_API_KEY_POOL_JSON', '[]'))
ARCHEOLOGY_SIZE = int(env.get('KEY_POOL_ARCHEOLOGY_SIZE', '1'))
TRACKING_SIZE = int(env.get('KEY_POOL_TRACKING_SIZE', '1'))

# Replicate config.py::key_rings logic
def get_key_rings(keys: list[str]) -> dict[str, list[str]]:
    total = len(keys)
    reserved = ARCHEOLOGY_SIZE + TRACKING_SIZE
    if total <= reserved:
        return {"hunting": keys, "tracking": keys, "archeology": keys}
    archeology_keys = keys[-ARCHEOLOGY_SIZE:]
    remaining = keys[:-ARCHEOLOGY_SIZE]
    tracking_keys = remaining[-TRACKING_SIZE:]
    hunting_keys = remaining[:-TRACKING_SIZE]
    return {
        "hunting": hunting_keys,
        "tracking": tracking_keys,
        "archeology": archeology_keys,
    }

def get_ring_for_key(rings: dict[str, list[str]], key: str) -> str:
    for ring_name, ring_keys in rings.items():
        if key in ring_keys:
            return ring_name
    return "unknown"

def test_key(api_key: str) -> tuple[bool, str]:
    """Test a key against YouTube Data API. Returns (is_working, detail)."""
    params = urllib.parse.urlencode({
        "part": "snippet",
        "q": "test",
        "type": "video",
        "maxResults": 1,
        "key": api_key,
    })
    url = f"https://www.googleapis.com/youtube/v3/search?{params}"
    try:
        with urllib.request.urlopen(url, timeout=10) as resp:
            data = json.loads(resp.read())
            items = data.get("items", [])
            return (True, f"OK ({len(items)} results)")
    except urllib.error.HTTPError as e:
        body = e.read().decode()
        reason = "unknown"
        try:
            err = json.loads(body)
            reason = err.get("error", {}).get("errors", [{}])[0].get("reason", "unknown")
        except json.JSONDecodeError:
            pass
        if reason in ("quotaExceeded", "dailyLimitExceeded"):
            return (False, "EXHAUSTED")
        return (False, f"ERROR {e.code}: {reason}")
    except Exception as e:
        return (False, f"EXCEPTION: {e}")

print("=" * 70)
print("PLEIADES YOUTUBE API KEY EXHAUSTION CHECK")
print(f"Keys to test: {len(api_keys)}")
print(f"Pool config: Archeology={ARCHEOLOGY_SIZE}, Tracking={TRACKING_SIZE}")
print("=" * 70)

rings = get_key_rings(api_keys)
print(f"\nCurrent key ring allocation:")
for ring_name, ring_keys in rings.items():
    print(f"  {ring_name}: {len(ring_keys)} keys (indices {[api_keys.index(k) for k in ring_keys]})")

results = {}
for i, key in enumerate(api_keys):
    ring = get_ring_for_key(rings, key)
    mask = key[:15] + "***" + key[-3:]
    is_working, detail = test_key(key)
    results[key] = {"ring": ring, "working": is_working, "detail": detail}
    status_icon = "OK" if is_working else "EXHAUSTED"
    print(f"\n  Key {i+1:2d}: {mask:30s} | {ring:12s} | {status_icon}")

working = [k for k, v in results.items() if v["working"]]
exhausted = [k for k, v in results.items() if not v["working"]]

print("\n" + "=" * 70)
print(f"SUMMARY:  {len(working)} working, {len(exhausted)} exhausted out of {len(api_keys)}")
print("=" * 70)

if working:
    available_for_reassign = [k for k in working if results[k]["ring"] != "archeology"]
    print(f"\nKeys NOT exhausted that could be reassigned to archeology pool:")
    for key in available_for_reassign:
        mask = key[:15] + "***" + key[-3:]
        print(f"  {mask:30s} | currently in: {results[key]['ring']}")
else:
    print("\nNo working keys available for reassignment.")

print("\n--- Three sample unexhausted keys (masked) for review ---")
sample_working = available_for_reassign[:3] if available_for_reassign else []
for key in sample_working:
    mask = key[:15] + "***" + key[-3:]
    print(f"  Key: {mask}")
    print(f"  Current ring: {results[key]['ring']}")
    print()
