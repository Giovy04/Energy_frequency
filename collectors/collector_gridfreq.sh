#!/usr/bin/env bash
set -euo pipefail

URL="${URL:-https://dat.netzfrequenzmessung.de:9080/386f.xml?c=86118}"
SLEEP_SEC="${SLEEP_SEC:-3}"

ua='Mozilla/5.0'

xml_get() {
  curl -sS --connect-timeout 5 --max-time 10 -H "User-Agent: $ua" "$URL" || return 1
}

xpath() {
  echo "$1" | xmllint --xpath "$2" - 2>/dev/null || true
}

json_num_or_null() {
  local v="$1"
  v="${v//,/\.}"
  if [[ -z "${v}" ]]; then
    echo "null"
  else
    if [[ "$v" =~ ^-?[0-9]+(\.[0-9]+)?$ ]]; then
      echo "$v"
    else
      echo "null"
    fi
  fi
}

json_str() {
  local s="$1"
  s="${s//\\/\\\\}"
  s="${s//\"/\\\"}"
  echo "$s"
}

while true; do
  XML="$(xml_get)" || { sleep "$SLEEP_SEC"; continue; }

  XML_FIXED="$(echo "$XML" | sed 's/<f<f_864>/<f_864>/g')"

  f1=$(echo "$XML_FIXED" | xmllint --xpath 'string(/r/f1)' - 2>/dev/null || true)
  f2=$(echo "$XML_FIXED" | xmllint --xpath 'string(/r/f2)' - 2>/dev/null || true)
  p=$(echo "$XML_FIXED"  | xmllint --xpath 'string(/r/p)'  - 2>/dev/null || true)
  z=$(echo "$XML_FIXED"  | xmllint --xpath 'string(/r/z)'  - 2>/dev/null || true)
  dt=$(echo "$XML_FIXED" | xmllint --xpath 'string(/r/dt)' - 2>/dev/null || true)
  n=$(echo "$XML_FIXED"  | xmllint --xpath 'string(/r/n)'  - 2>/dev/null || true)
  d=$(echo "$XML_FIXED"  | xmllint --xpath 'string(/r/d)'  - 2>/dev/null || true)
  f864=$(echo "$XML_FIXED"| xmllint --xpath 'string(/r/f_864)' - 2>/dev/null || true)

  ts_utc="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

  printf '{"ts_utc":"%s","z":"%s","n":"%s","d":"%s","f1":%s,"f2":%s,"phase_deg":%s,"dt":%s,"f_864":%s}\n' \
    "$ts_utc" "$(json_str "$z")" "$(json_str "$n")" "$(json_str "$d")" \
    "$(json_num_or_null "$f1")" "$(json_num_or_null "$f2")" "$(json_num_or_null "$p")" "$(json_num_or_null "$dt")" "$(json_num_or_null "$f864")"

  sleep "$SLEEP_SEC"
done
