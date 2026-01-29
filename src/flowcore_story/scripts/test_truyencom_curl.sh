#!/usr/bin/env bash
# Quick curl tester for truyencom using current crawling proxy.
# Usage:
#   PROXY=http://user:pass@host:port scripts/test_truyencom_curl.sh https://truyencom.com/dau-la-chi-lang-thanh/chuong-82.html
#   scripts/test_truyencom_curl.sh https://truyencom.com/api/chapters/11492/2/50 http://user:pass@host:port
set -euo pipefail

if [[ $# -lt 1 || $# -gt 2 ]]; then
  echo "Usage: $0 <url> [proxy]" >&2
  exit 1
fi

URL="$1"
PROXY="${2:-${PROXY:-}}"
OUT_DIR="${TMPDIR:-/tmp}/truyencom_curl_test"
mkdir -p "$OUT_DIR"

UA="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
HEADER_FILE="$OUT_DIR/headers.txt"
BODY_FILE="$OUT_DIR/body.html"
META_FILE="$OUT_DIR/meta.txt"

echo "[*] Target: $URL"
if [[ -n "$PROXY" ]]; then
  echo "[*] Using proxy: $PROXY"
  PROXY_ARG=(--proxy "$PROXY")
else
  echo "[*] No proxy set (will hit directly)"
  PROXY_ARG=()
fi

echo "[*] Curling..."
curl -sSL \
  "${PROXY_ARG[@]}" \
  -D "$HEADER_FILE" \
  -o "$BODY_FILE" \
  -H "User-Agent: $UA" \
  --compressed \
  --max-time 60 \
  -w $'status:%{http_code}\ntime_total:%{time_total}\nsize_download:%{size_download}\n' \
  "$URL" >"$META_FILE"

STATUS=$(grep -E '^status:' "$META_FILE" | cut -d: -f2)
TIME_TOTAL=$(grep -E '^time_total:' "$META_FILE" | cut -d: -f2)
SIZE_DOWNLOAD=$(grep -E '^size_download:' "$META_FILE" | cut -d: -f2)

echo "[*] status=$STATUS time_total=${TIME_TOTAL}s size_download=${SIZE_DOWNLOAD} bytes"
echo "[*] Headers saved to $HEADER_FILE"

LOWER_BODY=$(tr '[:upper:]' '[:lower:]' <"$BODY_FILE")
contains_phrase() {
  local phrase="$1"
  if grep -qF "$phrase" <<<"$LOWER_BODY"; then
    echo "yes"
  else
    echo "no"
  fi
}

PHRASES=(
  "enable javascript and cookies to continue"
  "just a moment"
  "checking if the site connection is secure"
  "cf_chl_opt"
  "cloudflare"
)

echo "[*] Anti-bot markers:"
for p in "${PHRASES[@]}"; do
  echo "    - '$p': $(contains_phrase "$p")"
done

echo "[*] Body length: $(wc -c <"$BODY_FILE") bytes"
echo "[*] Body preview (first 40 lines):"
head -n 40 "$BODY_FILE" | sed 's/\r$//'

echo "[*] Files:"
echo "    $BODY_FILE"
echo "    $HEADER_FILE"
echo "    $META_FILE"
