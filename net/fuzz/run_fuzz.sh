#!/bin/bash
#
# Build and run a libFuzzer fuzz target.
#
# Usage: ./net/fuzz/run_fuzz.sh <target> [duration_seconds]
# Example: ./net/fuzz/run_fuzz.sh rlp 60
#
# Targets: rlp, portal_wire, enr, discv5_msg, utp, discv5_header, ssz, history
#
# Run from the project root directory.

set -e

TARGET=${1:?"Usage: $0 <target> [seconds]"}
DURATION=${2:-60}

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
FUZZ_DIR="$ROOT/net/fuzz"
BUILD_DIR="$ROOT/build-fuzz"
CORPUS_DIR="$FUZZ_DIR/corpus/$TARGET"

mkdir -p "$BUILD_DIR" "$CORPUS_DIR"

CC=clang
CFLAGS="-g -O1 -fsanitize=fuzzer,address -fno-omit-frame-pointer"
INCLUDES="-I$ROOT/net/include -I$ROOT/common/include -I$ROOT/third_party/blst/bindings -I$ROOT/third_party/secp256k1/include"
BLST_LIB="$ROOT/third_party/blst/libblst.a"
SECP_LIB="$ROOT/third_party/secp256k1/build/lib/libsecp256k1.a"

echo "=== Building fuzz_${TARGET} ==="

case "$TARGET" in
    rlp)
        $CC $CFLAGS $INCLUDES \
            "$FUZZ_DIR/fuzz_rlp.c" \
            "$ROOT/common/src/rlp.c" \
            "$ROOT/common/src/bytes.c" \
            -o "$BUILD_DIR/fuzz_rlp"
        ;;

    portal_wire)
        $CC $CFLAGS $INCLUDES \
            "$FUZZ_DIR/fuzz_portal_wire.c" \
            "$ROOT/net/src/portal_wire.c" \
            "$ROOT/net/src/ssz.c" \
            -o "$BUILD_DIR/fuzz_portal_wire"
        ;;

    enr)
        $CC $CFLAGS $INCLUDES \
            "$FUZZ_DIR/fuzz_enr.c" \
            "$ROOT/net/src/enr.c" \
            "$ROOT/common/src/rlp.c" \
            "$ROOT/common/src/bytes.c" \
            "$ROOT/common/src/hash.c" \
            "$ROOT/common/src/keccak256.c" \
            "$ROOT/net/src/secp256k1_wrap.c" \
            $SECP_LIB \
            -o "$BUILD_DIR/fuzz_enr"
        ;;

    discv5_msg)
        $CC $CFLAGS $INCLUDES \
            "$FUZZ_DIR/fuzz_discv5_msg.c" \
            "$ROOT/net/src/discv5.c" \
            "$ROOT/net/src/discv5_codec.c" \
            "$ROOT/net/src/discv5_session.c" \
            "$ROOT/net/src/discv5_table.c" \
            "$ROOT/net/src/enr.c" \
            "$ROOT/common/src/rlp.c" \
            "$ROOT/common/src/bytes.c" \
            "$ROOT/common/src/hash.c" \
            "$ROOT/common/src/keccak256.c" \
            "$ROOT/net/src/secp256k1_wrap.c" \
            "$ROOT/net/src/hkdf.c" \
            "$ROOT/net/src/aes.c" \
            $BLST_LIB $SECP_LIB \
            -maes \
            -o "$BUILD_DIR/fuzz_discv5_msg"
        ;;

    utp)
        $CC $CFLAGS $INCLUDES \
            "$FUZZ_DIR/fuzz_utp.c" \
            "$ROOT/net/src/utp.c" \
            -o "$BUILD_DIR/fuzz_utp"
        ;;

    discv5_header)
        $CC $CFLAGS $INCLUDES \
            "$FUZZ_DIR/fuzz_discv5_header.c" \
            "$ROOT/net/src/discv5_codec.c" \
            "$ROOT/net/src/hkdf.c" \
            "$ROOT/net/src/aes.c" \
            "$ROOT/net/src/secp256k1_wrap.c" \
            $BLST_LIB $SECP_LIB \
            -maes \
            -o "$BUILD_DIR/fuzz_discv5_header"
        ;;

    ssz)
        $CC $CFLAGS $INCLUDES \
            "$FUZZ_DIR/fuzz_ssz.c" \
            "$ROOT/net/src/ssz.c" \
            -o "$BUILD_DIR/fuzz_ssz"
        ;;

    history)
        $CC $CFLAGS $INCLUDES \
            "$FUZZ_DIR/fuzz_history.c" \
            "$ROOT/net/src/history.c" \
            -o "$BUILD_DIR/fuzz_history"
        ;;

    all)
        echo "Building all targets..."
        for t in rlp portal_wire enr discv5_msg utp discv5_header ssz history; do
            "$0" "$t" 0  # build only (0 = skip run)
        done
        echo "=== All targets built ==="
        exit 0
        ;;

    *)
        echo "Unknown target: $TARGET"
        echo "Targets: rlp portal_wire enr discv5_msg utp discv5_header ssz history all"
        exit 1
        ;;
esac

echo "=== Built: $BUILD_DIR/fuzz_${TARGET} ==="

if [ "$DURATION" -eq 0 ] 2>/dev/null; then
    echo "(build only, skipping run)"
    exit 0
fi

echo "=== Running fuzz_${TARGET} for ${DURATION}s ==="
echo "    corpus: $CORPUS_DIR"
echo ""

"$BUILD_DIR/fuzz_${TARGET}" "$CORPUS_DIR" \
    -max_total_time="$DURATION" \
    -max_len=4096 \
    -print_final_stats=1
