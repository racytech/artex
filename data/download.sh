#!/bin/bash
#
# Download era1 archive files for Ethereum mainnet chain replay.
# Each era1 file contains 8192 blocks.
#
# Usage: ./data/download.sh <target_block>
#   Example: ./data/download.sh 100000
#
# Only downloads files not already present in data/era1/.
# Fetches the file index from ethpandaops on first run and caches it.

set -e

BASE_URL="https://data.ethpandaops.io/era1/mainnet"
DIR="$(dirname "$0")/era1"
INDEX="$DIR/.index"
BLOCKS_PER_FILE=8192

if [ $# -lt 1 ]; then
    echo "Usage: $0 <target_block>"
    echo "  Downloads era1 files needed to replay up to <target_block>."
    exit 1
fi

TARGET=$1
mkdir -p "$DIR"

# Fetch or refresh the file index
if [ ! -f "$INDEX" ]; then
    echo "Fetching era1 file index..."
    curl -s "$BASE_URL/" \
        | grep -oP 'mainnet-\d{5}-[a-f0-9]{8}\.era1' \
        | sort -u > "$INDEX"
    echo "  $(wc -l < "$INDEX") files available"
fi

TOTAL_FILES=$(wc -l < "$INDEX")
MAX_BLOCK=$(( TOTAL_FILES * BLOCKS_PER_FILE - 1 ))

if [ "$TARGET" -gt "$MAX_BLOCK" ]; then
    echo "Error: target block $TARGET exceeds max available $MAX_BLOCK"
    exit 1
fi

# Calculate how many files we need
FILES_NEEDED=$(( (TARGET / BLOCKS_PER_FILE) + 1 ))

downloaded=0
skipped=0

for (( i=0; i<FILES_NEEDED; i++ )); do
    f=$(sed -n "$((i + 1))p" "$INDEX")
    if [ -z "$f" ]; then
        echo "Error: no entry for index $i in file index"
        exit 1
    fi
    if [ -f "$DIR/$f" ]; then
        skipped=$((skipped + 1))
    else
        block_start=$((i * BLOCKS_PER_FILE))
        block_end=$(( (i + 1) * BLOCKS_PER_FILE - 1 ))
        echo "GET  $f  (blocks $block_start-$block_end)"
        wget -q --show-progress -O "$DIR/$f" "$BASE_URL/$f" || {
            echo "FAIL $f"
            rm -f "$DIR/$f"
            exit 1
        }
        downloaded=$((downloaded + 1))
    fi
done

echo "Done. downloaded=$downloaded skipped=$skipped (covers blocks 0-$((FILES_NEEDED * BLOCKS_PER_FILE - 1)))"
