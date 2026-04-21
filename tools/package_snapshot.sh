#!/usr/bin/env bash
#
# package_snapshot.sh — bundle a snapshot package into a compressed archive
# ready for upload (R2, BitTorrent seed, mirror host, etc.).
#
# Produces three files in the output directory:
#
#   state_<N>.tar.zst           # tar + zstd archive of the four-file package
#   state_<N>.tar.zst.sha256    # sha256 of the archive (for integrity check)
#   state_<N>.manifest.txt      # per-file sha256 of the unpacked contents
#
# Usage:
#   tools/package_snapshot.sh <block_number> [--src DIR] [--out DIR] [--level N]
#
# Defaults:
#   --src    ~/.artex
#   --out    ~/snapshot-publish
#   --level  9
#
# Example (uses all defaults):
#   tools/package_snapshot.sh 24864361
#
# Example (custom paths + lighter compression):
#   tools/package_snapshot.sh 24864361 \
#       --src /mnt/nvme/.artex \
#       --out /home/me/another-dir \
#       --level 6

set -euo pipefail

# -------- defaults --------
SRC_DIR="${HOME}/.artex"
OUT_DIR="${HOME}/snapshot-publish"
ZSTD_LEVEL=9

# -------- parse args --------
if [[ $# -lt 1 ]]; then
    echo "usage: $0 <block_number> [--src DIR] [--out DIR] [--level N]" >&2
    exit 2
fi

BLOCK="$1"; shift
if ! [[ "$BLOCK" =~ ^[0-9]+$ ]]; then
    echo "error: block_number must be a non-negative integer (got '$BLOCK')" >&2
    exit 2
fi

while [[ $# -gt 0 ]]; do
    case "$1" in
        --src)   SRC_DIR="$2"; shift 2 ;;
        --out)   OUT_DIR="$2"; shift 2 ;;
        --level) ZSTD_LEVEL="$2"; shift 2 ;;
        *) echo "error: unknown flag '$1'" >&2; exit 2 ;;
    esac
done

# -------- check deps --------
for cmd in tar zstd sha256sum; do
    if ! command -v "$cmd" >/dev/null; then
        echo "error: '$cmd' not found in PATH" >&2
        exit 1
    fi
done

# -------- validate inputs --------
STATE_BIN="state_${BLOCK}.bin"
STATE_HASHES="state_${BLOCK}.bin.hashes"
CODE_DAT="chain_replay_code.dat"
CODE_IDX="chain_replay_code.idx"

declare -a FILES=("$STATE_BIN" "$STATE_HASHES" "$CODE_DAT" "$CODE_IDX")

missing=0
for f in "${FILES[@]}"; do
    if [[ ! -f "$SRC_DIR/$f" ]]; then
        echo "error: missing required file: $SRC_DIR/$f" >&2
        missing=1
    fi
done
if [[ $missing -ne 0 ]]; then
    exit 1
fi

mkdir -p "$OUT_DIR"

# -------- report --------
ARCHIVE="$OUT_DIR/state_${BLOCK}.tar.zst"
SHA_FILE="${ARCHIVE}.sha256"
MANIFEST="$OUT_DIR/state_${BLOCK}.manifest.txt"

echo "=== package_snapshot ==="
echo "  src:     $SRC_DIR"
echo "  out:     $OUT_DIR"
echo "  block:   $BLOCK"
echo "  level:   $ZSTD_LEVEL (zstd)"
echo "  archive: $ARCHIVE"
echo

raw_total=0
for f in "${FILES[@]}"; do
    sz=$(stat -c %s "$SRC_DIR/$f")
    raw_total=$((raw_total + sz))
    printf "  %-40s  %12d bytes\n" "$f" "$sz"
done
printf "  %-40s  %12d bytes (%.1f GiB)\n" "[total raw]" "$raw_total" \
    "$(awk "BEGIN {print $raw_total/1024/1024/1024}")"
echo

# -------- per-file manifest (unpacked checksums) --------
echo "[1/3] computing per-file sha256 (manifest) ..."
t0=$(date +%s)
( cd "$SRC_DIR" && sha256sum "${FILES[@]}" ) > "$MANIFEST"
echo "      -> $MANIFEST   ($(($(date +%s) - t0))s)"
echo

# -------- tar + zstd --------
echo "[2/3] tar + zstd -T0 -${ZSTD_LEVEL} --long=27 ..."
t0=$(date +%s)
( cd "$SRC_DIR" && tar --sort=name -cf - "${FILES[@]}" ) \
    | zstd -T0 "-${ZSTD_LEVEL}" --long=27 -o "$ARCHIVE"
arch_sz=$(stat -c %s "$ARCHIVE")
printf "      -> %s   %d bytes (%.1f GiB, %.1f%% of raw)   (%ds)\n" \
    "$ARCHIVE" "$arch_sz" \
    "$(awk "BEGIN {print $arch_sz/1024/1024/1024}")" \
    "$(awk "BEGIN {print 100*$arch_sz/$raw_total}")" \
    "$(($(date +%s) - t0))"
echo

# -------- sha256 of archive --------
echo "[3/3] sha256 of archive ..."
t0=$(date +%s)
( cd "$OUT_DIR" && sha256sum "$(basename "$ARCHIVE")" ) > "$SHA_FILE"
echo "      -> $SHA_FILE   ($(($(date +%s) - t0))s)"
echo

# -------- summary --------
echo "=== done ==="
echo "  archive:  $ARCHIVE"
echo "  sha256:   $SHA_FILE"
echo "  manifest: $MANIFEST"
echo
echo "Publish all three files together. Users verify with:"
echo
echo "  sha256sum -c state_${BLOCK}.tar.zst.sha256"
echo "  zstd -d --long=27 -c state_${BLOCK}.tar.zst | tar -xf - -C ~/.artex"
echo "  ( cd ~/.artex && sha256sum -c /path/to/state_${BLOCK}.manifest.txt )"
