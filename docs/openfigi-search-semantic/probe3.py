#!/usr/bin/env python3
"""Battery 3: date expressions in /search queries + a showcase 'works great' example."""
import json
import os
import time
import urllib.request
import urllib.error

KEY = os.environ["OPENFIGI_API_KEY"]
OUT = "corpus3.jsonl"
SPACING = 3.5

PROBES = [
    # date expressions in free text — same bond, different human formats
    ("date_slash_full",   "search", {"query": "apple 05/01/1995"}),
    ("date_month_name",   "search", {"query": "apple may 1995"}),
    ("date_month_slash",  "search", {"query": "apple 05/1995"}),
    ("date_year_only",    "search", {"query": "apple 1995"}),
    # corp bond style: AAPL coupon maturity
    ("corp_slash",        "search", {"query": "AAPL 05/2026"}),
    ("corp_month_name",   "search", {"query": "AAPL May 2026"}),
    # the structured way: maturity interval filter
    ("maturity_interval", "search", {"query": "AAPL", "maturity": ["2026-01-01", "2026-12-31"]}),
    # showcase: precise intent, instant exact answer
    ("showcase_ticker_exch", "search", {"query": "AAPL", "exchCode": "US"}),
]


def call(endpoint, body):
    req = urllib.request.Request(
        f"https://api.openfigi.com/v3/{endpoint}",
        data=json.dumps(body).encode(),
        headers={"Content-Type": "application/json", "X-OPENFIGI-APIKEY": KEY},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return r.status, json.loads(r.read().decode())
    except urllib.error.HTTPError as e:
        raw = e.read().decode()
        try:
            return e.code, json.loads(raw)
        except json.JSONDecodeError:
            return e.code, raw[:300]


with open(OUT, "w") as f:
    for name, endpoint, body in PROBES:
        status, resp = call(endpoint, body)
        rec = {"probe": name, "endpoint": endpoint, "request": body, "status": status, "response": resp}
        f.write(json.dumps(rec) + "\n")
        f.flush()
        if isinstance(resp, dict):
            d = resp.get("data")
            dlen = len(d) if isinstance(d, list) else None
            firsts = [(x.get("ticker"), x.get("name"), x.get("securityType"), x.get("maturity")) for x in (d or [])[:3]]
            print(f"{name:20s} -> {status} len={dlen} err={resp.get('error')} first3={firsts}")
        else:
            print(f"{name:20s} -> {status} {str(resp)[:120]}")
        time.sleep(SPACING)
