#!/usr/bin/env bash
set -euo pipefail

base="${GRIDFREQ_URL:-https://dat.netzfrequenzmessung.de:9080}"
base="${base%$'\r'}"
base="${base#\"}"; base="${base%\"}"

src="$(curl -4 -fsS --connect-timeout 5 --max-time 20 --http1.1 \
  -A "Mozilla/5.0" \
  "${base}/source.xml?c=$(date +%s)" )" || { echo "__CURL_FAIL__"; exit 0; }

f="$(echo "$src" | sed -n 's:.*<f>\(.*\)</f>.*:\1:p')"
t="$(echo "$src" | sed -n 's:.*<t>\(.*\)</t>.*:\1:p')"

if [[ -z "${f}" || -z "${t}" ]]; then
  echo "__CURL_FAIL__"
  exit 0
fi

curl -4 -fsS --connect-timeout 5 --max-time 20 --http1.1 \
  -A "Mozilla/5.0" \
  "${base}/${f}?c=$(date +%s)" || echo "__CURL_FAIL__"
