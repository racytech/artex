#!/bin/bash
# Re-download era files that are missing, empty, or truncated.
#
# Bad files found by gap scanner:
#   629  (403MB, truncated — missing 169 blocks at end)
#   630  (507MB, truncated — missing 1149 blocks at end)
#   664  (466MB, truncated — missing 114 blocks at end)
#   783  (0MB, empty)
#   787  (0MB, empty)
#   1283 (1MB, incomplete)
#   1363 (15MB, incomplete)
#   1553 (11MB, incomplete)
#   1561 (missing entirely)

ERA_DIR="$(dirname "$0")/era"
BASE_URL="https://mainnet.era.nimbus.team"

BAD_FILES=(629 630 664 783 787 1283 1363 1553 1561)

echo "Fetching file listing from $BASE_URL ..."
LISTING=$(curl -s "$BASE_URL/")
if [ -z "$LISTING" ]; then
    echo "ERROR: couldn't fetch listing"
    exit 1
fi

FAILED=0
for i in "${BAD_FILES[@]}"; do
    NUM=$(printf "%05d" $i)
    FNAME=$(echo "$LISTING" | grep -o "mainnet-${NUM}-[a-f0-9]*\.era" | head -1)
    if [ -z "$FNAME" ]; then
        echo "SKIP $NUM: not found in remote listing"
        continue
    fi

    # Delete existing bad file
    EXISTING=$(ls "$ERA_DIR"/mainnet-${NUM}-*.era 2>/dev/null)
    if [ -n "$EXISTING" ]; then
        echo "Removing: $(basename $EXISTING)"
        rm -f "$EXISTING"
    fi

    echo -n "GET  $FNAME ... "
    curl -s -o "$ERA_DIR/$FNAME" "$BASE_URL/$FNAME"
    SIZE=$(stat -c%s "$ERA_DIR/$FNAME" 2>/dev/null || echo 0)
    if [ "$SIZE" -lt 10000000 ]; then
        echo "FAILED (${SIZE}B)"
        rm -f "$ERA_DIR/$FNAME"
        FAILED=$((FAILED + 1))
    else
        echo "OK ($(( SIZE / 1024 / 1024 ))MB)"
    fi
done

if [ "$FAILED" -gt 0 ]; then
    echo "WARNING: $FAILED files failed to download"
    exit 1
fi
echo "All files downloaded. Re-run the gap scanner to verify."
