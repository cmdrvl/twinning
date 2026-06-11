# OpenFIGI /search Semantic Twin Packet

**Status:** measured 2026-06-10 against the live API. Companion artifacts in
[docs/openfigi-search-semantic/](./openfigi-search-semantic/).

This packet documents the gap between what an OpenAPI document declares
(structural contract) and what a provider actually does (semantic behavior),
using OpenFIGI `POST /v3/search` as the measured case. It closes the
documentation half of bd-24xn and motivates the behavioral-parity controls in
bd-3r96.

Verified contract snapshot (same as the v2/v3 packet):

- Source URL: `https://api.openfigi.com/schema`
- SHA-256: `d83fbc4ad3053c23684ec9c9b24e667d61ef1022e1d98456252f8cba3159d520`
- Probed: 2026-06-10, authenticated tier (`ratelimit-policy: 20;w=60` on
  `/search`)

---

## 1. The gap: structural spec vs semantic behavior

An OpenAPI document pins request/response **envelope shape**. It does not — and
mostly cannot — encode **provider behavior**: what a miss looks like, which
errors arrive at which status codes, how relevance ranks results, which inputs
silently match nothing, or how auth tiers change limits.

Twinning's REST kernel can synthesize structurally valid responses from the
schema alone, but a structurally valid response is not a live-like response.
The original `/mapping` observations (bd-24xn):

- Live `POST /v3/mapping` with an unknown CUSIP returns HTTP 200 with
  `[{"warning":"No identifier found."}]` — a per-item miss payload.
- An unstubbed but schema-valid CUSIP falls through to schema synthesis and
  returns `{"data":null,"error":null,"warning":null}` — valid-looking, never
  produced by the live API.
- The fixture marks `ApiKeyAuth` required, so the twin returns 401 without the
  header; live OpenFIGI allows unauthenticated calls at a lower rate-limit
  tier.

**Warning:** unstubbed schema fallback can hide provider-semantic gaps. A test
that passes against schema synthesis proves envelope conformance only. Do not
treat it as live parity.

## 2. The measured case: what `/search` actually does

`/search` is the endpoint where this gap is widest, because its core contract —
*which instruments come back, in what order, for what query* — is entirely
semantic. The published schema declares almost none of the behavior below.
Every row was measured live (full request/response pairs in
[corpus.trimmed.jsonl](./openfigi-search-semantic/corpus.trimmed.jsonl);
regenerate with [probe.py](./openfigi-search-semantic/probe.py),
[probe2.py](./openfigi-search-semantic/probe2.py), and
[probe3.py](./openfigi-search-semantic/probe3.py)).

| # | Measured behavior | What the spec says | Gap class |
|---|-------------------|--------------------|-----------|
| 1 | Top level of a `/search` response is a single object `{data, next}` / `{data}` / `{error}` | `SearchResponse` is `array<SearchResponseElement>` | shape drift |
| 2 | **All** request-validation failures return HTTP **200** with `{"error": "..."}`: empty query, empty object, unknown key, wrong type, bad `start` token | 400 "Invalid request (body)" with `text/html` body | status-code drift |
| 3 | Unknown request **keys** error (`"Invalid key 'x'."` at 200); unknown **enum values** silently return `{"data": []}` | 400 `invalid_enum` example | silent no-match trap |
| 4 | CUSIP or ISIN as `query` text → 0 results; a FIGI as `query` text → exactly 1 result | nothing | identifier-routing semantics |
| 5 | `{"query": "apple"}` page 1 is Apple Valley **municipal bonds**, not Apple Inc | nothing | relevance semantics |
| 6 | `{"query": "AAPL"}` → Apple Inc common stock first; `{"query": "apple", "exchCode": "US"}` → Apple Inc first | nothing | relevance recipes |
| 7 | Page size fixed at 100, opaque `next` cursor, no `total` on `/search`; the same query on `/filter` returns `total` (`"apple"` → 1,350,538 instruments) | `start`/`next` described, `total` only on filter response | cross-endpoint asymmetry |
| 8 | Identical query → identical page-1 ordering and identical `next` token across runs | nothing | determinism (twin-friendly) |
| 9 | `query` is optional when another filter property is present (`securityType2` + `strike` alone works) | "At least one property must be populated" | confirmed |
| 10 | Free-text dates match the descriptor dialect, not the calendar: `"apple may 1995"` → 0 results; `"apple 05/1995"` and `"apple 05/01/1995"` → exactly the 2 Apple Valley bonds maturing 1995-05-01. Structured `maturity` interval filters work as documented | nothing | date-dialect semantics |

Rows 2–4 are the agent killers. A client that branches on HTTP status never
sees an OpenFIGI validation error. A client that typos a `securityType` value
concludes the instrument universe is empty. A client that feeds CUSIPs to
`/search` instead of `/mapping` concludes OpenFIGI has no coverage. All three
failures are invisible in the published contract, and all three pass a
schema-synthesis twin.

Row 5 is the relevance problem in one probe: the FIGI corpus is dominated by
fixed income and derivatives (every muni, tranche, and option contract has a
FIGI), so bare name queries drown. Rows 6 and 10 show the measured recipes that rescue intent — including the
date dialect: descriptors carry `MM/DD/YYYY` / `MM/YY` dates, so
`"may 1995"` finds nothing while `"05/1995"` is surgical. Row 8 is the good
news: the behavior is deterministic, which means it twins faithfully.

## 3. The method: spec → probes → corpus → overlay → twin

The repeatable loop, runnable against any provider with a published spec and a
live key:

1. **Pin the spec.** Download, hash, record (`/schema`,
   `sha256:d83fbc4a…`).
2. **Derive the probe battery.** Two probe families:
   - *Schema-derived:* for each request property — valid value, invalid type,
     invalid enum value, empty value, undeclared sibling key; plus pagination
     follow/corrupt-token cases.
   - *Domain-derived:* inputs an agent will actually send — identifier-shaped
     strings (CUSIP, ISIN, FIGI, ticker), names, multi-word, unicode,
     filter-combination recipes.
3. **Capture the corpus.** Every (request, status, headers, body) pair to
   JSONL, spaced inside the published rate limit. The corpus is the evidence;
   findings are derived, never asserted from memory.
4. **Classify divergences** with the spec-accuracy taxonomy
   ([PLAN_TWINNING_REST_STRATEGY.md §3](./PLAN_TWINNING_REST_STRATEGY.md)):
   over-promising, under-specifying, shape lying, status-code drift, auth
   drift.
5. **Encode as an `x-twinning` overlay.** Corpus records become
   `x-twinning.response-stubs` keyed on canonical request bodies; one stub per
   semantic use case, not per endpoint. See
   [openfigi-search-semantic-twin.yaml](./openfigi-search-semantic/openfigi-search-semantic-twin.yaml)
   (13 stubs: identifier routing, relevance recipes, error envelopes, the
   silent-enum trap, no-match shape, a deterministic two-page pagination
   chain).
6. **Replay to prove parity.** Serve the overlay with `twinning rest`, replay
   the client battery, compare against the corpus:

```bash
twinning --json rest \
  --spec docs/openfigi-search-semantic/openfigi-search-semantic-twin.yaml \
  --server-variable basePath=v3 \
  --auth-mode shape \
  --run 'TWIN_BASE_URL=http://127.0.0.1:<port> ./docs/openfigi-search-semantic/replay_client.sh'
```

Replay result on 2026-06-10: all 10 battery cases reproduce the live
semantics — error envelopes at 200, FIGI exact-hit, CUSIP zero-hit, muni-first
relevance page, silent enum no-match, working `next` token. (Checked-in stub
fixtures trim `data` arrays to 5 rows for readability; the untrimmed corpus
captures full 100-row pages.)

## 4. Stub-authoring guidance

For OpenFIGI and any future provider twin:

- **Capture, don't invent.** Agents with live API access should record
  representative request/response pairs and transcribe them into
  `x-twinning.response-stubs`. A stub that was never observed live is a guess
  wearing a contract's clothes.
- **One stub per semantic use case.** Success, miss, per-item warning, each
  distinct error envelope, the pagination chain, and each relevance recipe are
  separate cases. Separate spec variants/overlays per scenario beat runtime
  scenario state (see the v2/v3 packet).
- **Stub the misses first.** The miss/error shapes are where schema synthesis
  lies most convincingly (`{"data":null,...}` vs
  `[{"warning":"No identifier found."}]`, 400-vs-200).
- **Point routine tests at stubs, not at live OpenFIGI.** Canon/OpenFIGI tests
  should validate against the checked-in overlay; live calls are for
  re-measurement (refreshing the corpus when the spec hash changes), not for
  CI.
- **Record provenance in the overlay.** Spec source URL, spec hash, probe
  date, auth tier, and rate-limit policy belong in the overlay's `info`
  block. A behavioral claim without a measurement date is stale on arrival.

## 5. What this does not solve yet (bd-3r96 territory)

- **Unstubbed fallback is still silent.** A schema-valid request with no
  matching stub falls through to schema synthesis. A per-route `x-twinning`
  no-stub policy (`refuse` / `error` / `empty` / `schema` / `stub-required`)
  would let provider twins fail loudly where behavior has not been captured.
- **Exact-body matching only.** Stubs match canonical JSON request bodies.
  Relevance *recipes* ("name + exchCode → composite equities first")
  generalize beyond any finite body set; encoding them needs either templated
  matching or a seeded-corpus read path.
- **Optional-auth tiers.** Live OpenFIGI serves unauthenticated requests at a
  lower rate tier; `--auth-mode shape` cannot express "allowed without key,
  different limits."
- **Semantic annotations as first-class `x-twinning` surface.** The findings
  table in §2 is prose. A machine-readable
  `x-twinning.semantics` block (identifier-routing hints, error-channel
  declaration, relevance recipes) would let agents read the behavioral
  contract the same way they read the structural one. That is the natural
  next extension and the substance of the public write-up.

---

If this packet and the corpus disagree, the corpus wins. If the corpus and the
live API disagree, the API moved — re-run the probes and re-stamp the date.
