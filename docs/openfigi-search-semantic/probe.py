#!/usr/bin/env python3
"""Controlled semantic probe battery for OpenFIGI POST /v3/search.

Records every (request, response) pair to a JSONL corpus. Spaced to stay
well inside published rate limits (search: 20 req/min with key).
"""
import json
import os
import sys
import time
import urllib.request
import urllib.error

API = "https://api.openfigi.com/v3/search"
KEY = os.environ["OPENFIGI_API_KEY"]
OUT = sys.argv[1] if len(sys.argv) > 1 else "corpus.jsonl"
SPACING = 3.5  # seconds between probes

PROBES = [
    ("happy_name", {"query": "apple"}),
    ("ticker_text", {"query": "AAPL"}),
    ("cusip_as_text", {"query": "037833100"}),
    ("figi_as_text", {"query": "BBG000B9XRY4"}),
    ("empty_query", {"query": ""}),
    ("empty_object", {}),
    ("name_plus_exch", {"query": "apple", "exchCode": "US"}),
    ("mixed_case", {"query": "aPpLe"}),
    ("multi_word", {"query": "apple inc"}),
    ("prefix_partial", {"query": "appl"}),
    ("unicode", {"query": "苹果"}),
    ("single_char", {"query": "a"}),
    ("no_match", {"query": "zzzzqqqqxxxxwwww"}),
    ("undeclared_field", {"query": "apple", "definitelyNotAField": "x"}),
    ("wrong_type_query", {"query": 123}),
    ("filter_only_no_query", {"securityType2": "Option", "strike": [100, 200]}),
    ("garbage_start_token", {"query": "apple", "start": "not-a-real-token"}),
]


def call(body):
    data = json.dumps(body).encode()
    req = urllib.request.Request(
        API,
        data=data,
        headers={
            "Content-Type": "application/json",
            "X-OPENFIGI-APIKEY": KEY,
        },
        method="POST",
    )
    t0 = time.time()
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            raw = r.read().decode()
            status = r.status
            headers = dict(r.headers)
    except urllib.error.HTTPError as e:
        raw = e.read().decode()
        status = e.code
        headers = dict(e.headers)
    elapsed = round((time.time() - t0) * 1000)
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        parsed = None
    return status, headers, raw, parsed, elapsed


def summarize(parsed, raw):
    if parsed is None:
        return {"body_kind": "non-json", "raw_prefix": raw[:200]}
    kind = type(parsed).__name__
    s = {"body_kind": kind}
    if isinstance(parsed, dict):
        s["keys"] = sorted(parsed.keys())
        if isinstance(parsed.get("data"), list):
            s["data_len"] = len(parsed["data"])
            if parsed["data"]:
                s["first_keys"] = sorted(parsed["data"][0].keys())
        if "next" in parsed:
            s["has_next"] = bool(parsed.get("next"))
        if "error" in parsed:
            s["error"] = parsed["error"]
    elif isinstance(parsed, list):
        s["len"] = len(parsed)
    return s


def main():
    results = []
    with open(OUT, "w") as f:
        for name, body in PROBES:
            status, headers, raw, parsed, elapsed = call(body)
            rl = {k: v for k, v in headers.items() if "ratelimit" in k.lower() or "retry" in k.lower()}
            rec = {
                "probe": name,
                "request": body,
                "status": status,
                "elapsed_ms": elapsed,
                "ratelimit_headers": rl,
                "content_type": headers.get("Content-Type"),
                "summary": summarize(parsed, raw),
                "response": parsed if parsed is not None else raw[:500],
            }
            f.write(json.dumps(rec) + "\n")
            f.flush()
            print(f"{name:24s} -> {status} {json.dumps(rec['summary'])[:160]}")
            results.append(rec)
            time.sleep(SPACING)

    # pagination follow-up: page 2 from the happy-path next token
    happy = next((r for r in results if r["probe"] == "happy_name"), None)
    if happy and isinstance(happy["response"], dict) and happy["response"].get("next"):
        tok = happy["response"]["next"]
        status, headers, raw, parsed, elapsed = call({"query": "apple", "start": tok})
        rec = {
            "probe": "page_2_via_next",
            "request": {"query": "apple", "start": tok[:20] + "..."},
            "status": status,
            "elapsed_ms": elapsed,
            "summary": summarize(parsed, raw),
            "response": parsed if parsed is not None else raw[:500],
        }
        with open(OUT, "a") as f:
            f.write(json.dumps(rec) + "\n")
        print(f"{'page_2_via_next':24s} -> {status} {json.dumps(rec['summary'])[:160]}")


if __name__ == "__main__":
    main()
