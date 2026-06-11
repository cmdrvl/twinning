#!/usr/bin/env python3
"""Battery 2: determinism, enum validation, narrowing recipes, /filter asymmetry."""
import json
import os
import sys
import time
import urllib.request
import urllib.error

KEY = os.environ["OPENFIGI_API_KEY"]
OUT = "corpus2.jsonl"
SPACING = 3.5

PROBES = [
    ("search", "determinism_run_a", {"query": "apple"}),
    ("search", "determinism_run_b", {"query": "apple"}),
    ("search", "invalid_enum_sectype", {"query": "apple", "securityType": "NotARealType"}),
    ("search", "name_plus_marketsec", {"query": "apple", "marketSecDes": "Equity"}),
    ("search", "isin_as_text", {"query": "US0378331005"}),
    ("search", "capitalized", {"query": "Apple"}),
    ("filter", "filter_same_query", {"query": "apple"}),
    ("filter", "filter_equity_only", {"query": "apple", "marketSecDes": "Equity"}),
]


def call(endpoint, body):
    data = json.dumps(body).encode()
    req = urllib.request.Request(
        f"https://api.openfigi.com/v3/{endpoint}",
        data=data,
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


def main():
    with open(OUT, "w") as f:
        for endpoint, name, body in PROBES:
            status, resp = call(endpoint, body)
            rec = {"probe": name, "endpoint": endpoint, "request": body,
                   "status": status, "response": resp}
            f.write(json.dumps(rec) + "\n")
            f.flush()
            if isinstance(resp, dict):
                keys = sorted(resp.keys())
                dlen = len(resp.get("data", [])) if isinstance(resp.get("data"), list) else None
                first = None
                if dlen:
                    x = resp["data"][0]
                    first = (x.get("ticker"), x.get("name"), x.get("securityType"))
                print(f"{name:24s} [{endpoint}] -> {status} keys={keys} len={dlen} "
                      f"total={resp.get('total')} err={resp.get('error')} first={first}")
            else:
                print(f"{name:24s} [{endpoint}] -> {status} {str(resp)[:120]}")
            time.sleep(SPACING)

    # determinism check
    runs = [json.loads(l) for l in open(OUT)]
    a = next(r for r in runs if r["probe"] == "determinism_run_a")
    b = next(r for r in runs if r["probe"] == "determinism_run_b")
    fa = [x["figi"] for x in a["response"]["data"]]
    fb = [x["figi"] for x in b["response"]["data"]]
    print("determinism: identical page-1 FIGI sequence:", fa == fb)
    print("next token identical:", a["response"].get("next") == b["response"].get("next"))


if __name__ == "__main__":
    main()
