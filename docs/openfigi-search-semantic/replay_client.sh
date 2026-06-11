#!/bin/bash
# Replays the semantic battery against whatever base URL is in $TWIN_BASE_URL
B="$TWIN_BASE_URL/v3/search"
H1="Content-Type: application/json"
H2="X-OPENFIGI-APIKEY: test-token"
post() { curl -s -o /tmp/figi_probe/replay_out.json -w "%{http_code}" -X POST "$B" -H "$H1" -H "$H2" -d "$1"; }
check() {
  local name="$1" body="$2" expect="$3"
  local code; code=$(post "$body")
  local got; got=$(python3 -c "
import json
r=json.load(open('/tmp/figi_probe/replay_out.json'))
if isinstance(r,dict):
    if r.get('error'): print('error:'+r['error'][:40])
    elif isinstance(r.get('data'),list):
        d=r['data']
        first=(d[0].get('ticker') or d[0].get('name')) if d else ''
        print(f'data[{len(d)}] first={first} next={bool(r.get(chr(34)+chr(110)+chr(101)+chr(120)+chr(116)+chr(34)) if False else r.get(\"next\"))}')
else: print(type(r).__name__)
")
  printf '%-28s -> %s %s\n' "$name" "$code" "$got"
}
check figi_exact_hit        '{"query":"BBG000B9XRY4"}'
check cusip_no_match        '{"query":"037833100"}'
check bare_name_drowns      '{"query":"apple"}'
check ticker_works          '{"query":"AAPL"}'
check name_exch_rescues     '{"query":"apple","exchCode":"US"}'
check empty_query_err       '{"query":""}'
check empty_object_err      '{}'
check unknown_key_err       '{"query":"apple","definitelyNotAField":"x"}'
check invalid_enum_silent   '{"query":"apple","securityType":"NotARealType"}'
check no_match_empty_data   '{"query":"zzzzqqqqxxxxwwww"}'
