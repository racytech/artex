#!/usr/bin/env python3
"""
EVM Differential Fuzzer

Generates random EVM state transitions and compares results between
art's evm_t8n and geth's evm t8n. Saves mismatches for reproduction.

Usage:
    python3 diff_fuzz.py --art ./build/evm_t8n --geth /path/to/evm \
        --fork Cancun --iterations 10000 --work-dir /tmp/fuzz_work
"""

import argparse
import hashlib
import json
import os
import random
import shutil
import subprocess
import struct
import sys
import time

# Well-known test private key and corresponding address
DEFAULT_SECRET_KEY = "0x45a915e4d060149eb4365960e6a7a45f334393093061116b197e3240065ff2d8"
DEFAULT_SENDER = "0xa94f5374fce5edbc8e2a8697c15331677e6ebf0b"

# Interesting EVM opcodes to bias toward
INTERESTING_OPS = [
    0x00,  # STOP
    0x01,  # ADD
    0x02,  # MUL
    0x03,  # SUB
    0x04,  # DIV
    0x06,  # MOD
    0x10,  # LT
    0x11,  # GT
    0x14,  # EQ
    0x15,  # ISZERO
    0x16,  # AND
    0x17,  # OR
    0x18,  # XOR
    0x19,  # NOT
    0x20,  # KECCAK256
    0x30,  # ADDRESS
    0x31,  # BALANCE
    0x32,  # ORIGIN
    0x33,  # CALLER
    0x34,  # CALLVALUE
    0x35,  # CALLDATALOAD
    0x36,  # CALLDATASIZE
    0x3B,  # EXTCODESIZE
    0x40,  # BLOCKHASH
    0x41,  # COINBASE
    0x42,  # TIMESTAMP
    0x43,  # NUMBER
    0x44,  # PREVRANDAO
    0x45,  # GASLIMIT
    0x46,  # CHAINID
    0x47,  # SELFBALANCE
    0x48,  # BASEFEE
    0x50,  # POP
    0x51,  # MLOAD
    0x52,  # MSTORE
    0x54,  # SLOAD
    0x55,  # SSTORE
    0x56,  # JUMP
    0x57,  # JUMPI
    0x58,  # PC
    0x59,  # MSIZE
    0x5A,  # GAS
    0x5B,  # JUMPDEST
    0x5F,  # PUSH0
    0x60,  # PUSH1
    0x61,  # PUSH2
    0x7F,  # PUSH32
    0x80,  # DUP1
    0x90,  # SWAP1
    0xA0,  # LOG0
    0xA1,  # LOG1
    0xF0,  # CREATE
    0xF1,  # CALL
    0xF3,  # RETURN
    0xF5,  # CREATE2
    0xFA,  # STATICCALL
    0xFD,  # REVERT
    0xFE,  # INVALID
    0xFF,  # SELFDESTRUCT
]


def random_hex(n_bytes):
    """Generate random hex string of n_bytes."""
    return "0x" + os.urandom(n_bytes).hex()


def hex_padded(val):
    """Format integer as even-length 0x-prefixed hex (geth requires even length)."""
    h = hex(val)
    # Remove 0x prefix, pad to even length
    digits = h[2:]
    if len(digits) % 2 == 1:
        digits = "0" + digits
    return "0x" + digits


def random_address():
    """Generate a random address (avoiding precompile range 0x01-0x09)."""
    addr = bytearray(os.urandom(20))
    # Ensure not in precompile range
    if all(b == 0 for b in addr[:19]) and addr[19] <= 9:
        addr[0] = random.randint(0x10, 0xff)
    return "0x" + addr.hex()


def random_bytecode(min_len=1, max_len=256, structured=True):
    """Generate random EVM bytecode."""
    length = random.randint(min_len, max_len)

    if not structured:
        return "0x" + os.urandom(length).hex()

    code = bytearray()
    while len(code) < length:
        if random.random() < 0.7:
            # Use an interesting opcode
            op = random.choice(INTERESTING_OPS)
        else:
            # Random byte
            op = random.randint(0, 255)

        code.append(op)

        # If it's a PUSH, add the push data
        if 0x60 <= op <= 0x7F:
            push_bytes = op - 0x5F
            for _ in range(push_bytes):
                if len(code) >= length:
                    break
                code.append(random.randint(0, 255))

    return "0x" + bytes(code[:length]).hex()


def generate_test_case(fork="Cancun"):
    """Generate a random differential test case."""
    # Random contract addresses
    n_contracts = random.randint(1, 3)
    contracts = [random_address() for _ in range(n_contracts)]

    # Build alloc
    alloc = {
        DEFAULT_SENDER: {
            "balance": hex(10**20),  # 100 ETH
            "nonce": "0x0",
        }
    }

    # Add contracts with random bytecode and state
    for addr in contracts:
        account = {
            "balance": hex(random.randint(0, 10**18)),
            "nonce": hex(random.randint(0, 5)),
            "code": random_bytecode(1, 128),
        }

        # Random storage
        n_slots = random.randint(0, 3)
        if n_slots > 0:
            storage = {}
            for _ in range(n_slots):
                key = hex_padded(random.randint(0, 2**16))
                val = hex_padded(random.randint(1, 2**64))
                storage[key] = val
            account["storage"] = storage

        alloc[addr] = account

    # Coinbase
    coinbase = random_address()
    alloc[coinbase] = {"balance": "0x0"}

    # Build env
    env = {
        "currentCoinbase": coinbase,
        "currentNumber": hex(random.randint(1, 1000000)),
        "currentTimestamp": hex(random.randint(1000, 2000000000)),
        "currentGasLimit": "0x1000000",
        "currentBaseFee": hex(random.randint(1, 10**10)),
    }

    # Pre-Paris forks: use currentDifficulty, NOT currentRandom
    # (geth overrides DIFFICULTY opcode with currentRandom when present)
    PRE_PARIS_FORKS = ("Frontier", "Homestead", "Tangerine Whistle", "Spurious Dragon",
                       "Byzantium", "Constantinople", "Istanbul", "Berlin", "London",
                       "Arrow Glacier", "Gray Glacier")
    if fork in PRE_PARIS_FORKS:
        env["currentDifficulty"] = hex(random.randint(1, 2**20))
    else:
        env["currentRandom"] = random_hex(32)

    # Shanghai+ needs withdrawals in env
    SHANGHAI_PLUS = ("Shanghai", "Cancun", "Prague")
    if fork in SHANGHAI_PLUS:
        env["withdrawals"] = []

    # Cancun+ needs parentBeaconBlockRoot and excessBlobGas
    if fork in ("Cancun", "Prague"):
        env["currentExcessBlobGas"] = hex(random.randint(0, 10**8))
        env["parentBeaconBlockRoot"] = random_hex(32)

    # Build transactions (1-2 txs)
    n_txs = random.randint(1, 2)
    txs = []
    for ti in range(n_txs):
        target = random.choice(contracts + [None])  # None = CREATE
        gas = random.randint(21000, 500000)

        tx = {
            "secretKey": DEFAULT_SECRET_KEY,
            "nonce": hex(ti),
            "gas": hex(gas),
            "value": hex(random.choice([0, 0, 0, random.randint(1, 10**16)])),
            "input": "0x" + os.urandom(random.randint(0, 64)).hex(),
            "v": "0x0",
            "r": "0x0",
            "s": "0x0",
        }

        tx["gasPrice"] = hex(int(env["currentBaseFee"], 16))
        if target:
            tx["to"] = target
        else:
            # Contract creation — omit 'to' field entirely for geth compat
            tx["input"] = random_bytecode(1, 64)

        txs.append(tx)

    return alloc, env, txs


def write_test_case(directory, alloc, env, txs):
    """Write test case files to directory."""
    os.makedirs(directory, exist_ok=True)
    with open(os.path.join(directory, "alloc.json"), "w") as f:
        json.dump(alloc, f)
    with open(os.path.join(directory, "env.json"), "w") as f:
        json.dump(env, f)
    with open(os.path.join(directory, "txs.json"), "w") as f:
        json.dump(txs, f)


def run_t8n(binary, alloc_path, env_path, txs_path, fork, is_geth=False):
    """Run a t8n tool and return the parsed result JSON."""
    if is_geth:
        cmd = [
            binary, "t8n",
            "--input.alloc", alloc_path,
            "--input.env", env_path,
            "--input.txs", txs_path,
            "--state.fork", fork,
            "--output.result", "stdout",
            "--output.alloc", "/dev/null",
        ]
    else:
        cmd = [
            binary,
            "--input.alloc", alloc_path,
            "--input.env", env_path,
            "--input.txs", txs_path,
            "--state.fork", fork,
            "--output.result", "stdout",
        ]

    try:
        proc = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=30,
        )
        if proc.returncode != 0:
            return None, proc.stderr.strip()[:200]

        # Parse stdout as JSON
        stdout = proc.stdout.strip()
        if not stdout:
            return None, "empty output"

        parsed = json.loads(stdout)

        # geth wraps the result in {"result": {...}}
        if is_geth and "result" in parsed:
            parsed = parsed["result"]

        return parsed, None

    except subprocess.TimeoutExpired:
        return None, "timeout"
    except json.JSONDecodeError as e:
        return None, f"json parse error: {e}"
    except Exception as e:
        return None, str(e)


def compare_results(art_result, geth_result):
    """Compare two t8n results. Returns list of differences."""
    diffs = []

    if art_result.get("stateRoot") != geth_result.get("stateRoot"):
        diffs.append(
            f"stateRoot: art={art_result.get('stateRoot')} "
            f"geth={geth_result.get('stateRoot')}"
        )

    if art_result.get("gasUsed") != geth_result.get("gasUsed"):
        diffs.append(
            f"gasUsed: art={art_result.get('gasUsed')} "
            f"geth={geth_result.get('gasUsed')}"
        )

    # Compare receipt statuses
    art_receipts = art_result.get("receipts", [])
    geth_receipts = geth_result.get("receipts", [])
    if len(art_receipts) != len(geth_receipts):
        diffs.append(
            f"receipt count: art={len(art_receipts)} geth={len(geth_receipts)}"
        )
    else:
        for i, (ar, gr) in enumerate(zip(art_receipts, geth_receipts)):
            if ar.get("status") != gr.get("status"):
                diffs.append(
                    f"receipt[{i}].status: art={ar.get('status')} "
                    f"geth={gr.get('status')}"
                )

    # Compare rejected transaction count
    art_rejected = art_result.get("rejected", [])
    geth_rejected = geth_result.get("rejected", [])
    if len(art_rejected) != len(geth_rejected):
        diffs.append(
            f"rejected count: art={len(art_rejected)} "
            f"geth={len(geth_rejected)}"
        )

    return diffs


def save_failing_case(work_dir, alloc, env, txs, art_result, geth_result,
                      diffs, art_error, geth_error):
    """Save a failing test case for later analysis."""
    ts = int(time.time() * 1000)
    fail_dir = os.path.join(work_dir, "failing", str(ts))
    os.makedirs(fail_dir, exist_ok=True)

    write_test_case(fail_dir, alloc, env, txs)

    if art_result:
        with open(os.path.join(fail_dir, "art_result.json"), "w") as f:
            json.dump(art_result, f, indent=2)
    if geth_result:
        with open(os.path.join(fail_dir, "geth_result.json"), "w") as f:
            json.dump(geth_result, f, indent=2)

    with open(os.path.join(fail_dir, "diff.txt"), "w") as f:
        if art_error:
            f.write(f"art error: {art_error}\n")
        if geth_error:
            f.write(f"geth error: {geth_error}\n")
        for d in diffs:
            f.write(d + "\n")

    return fail_dir


def main():
    parser = argparse.ArgumentParser(description="EVM Differential Fuzzer")
    parser.add_argument("--art", required=True, help="Path to art's evm_t8n binary")
    parser.add_argument("--geth", required=True, help="Path to geth's evm binary")
    parser.add_argument("--fork", default="Cancun", help="Fork name (default: Cancun)")
    parser.add_argument("--iterations", type=int, default=1000, help="Number of iterations (0 = infinite)")
    parser.add_argument("--work-dir", default="/tmp/fuzz_work", help="Working directory")
    parser.add_argument("--seed", type=int, default=None, help="Random seed")
    parser.add_argument("--fail-fast", action="store_true", help="Stop on first mismatch")
    parser.add_argument("--verbose", action="store_true", help="Verbose output")
    args = parser.parse_args()

    if args.seed is not None:
        random.seed(args.seed)

    # Verify binaries exist
    if not os.path.isfile(args.art):
        print(f"Error: art binary not found: {args.art}", file=sys.stderr)
        return 1
    if not os.path.isfile(args.geth):
        print(f"Error: geth binary not found: {args.geth}", file=sys.stderr)
        return 1

    # Setup working directory
    current_dir = os.path.join(args.work_dir, "current")
    os.makedirs(current_dir, exist_ok=True)
    os.makedirs(os.path.join(args.work_dir, "failing"), exist_ok=True)

    alloc_path = os.path.join(current_dir, "alloc.json")
    env_path = os.path.join(current_dir, "env.json")
    txs_path = os.path.join(current_dir, "txs.json")

    # Stats
    total = 0
    matches = 0
    mismatches = 0
    art_errors = 0
    geth_errors = 0
    start_time = time.time()

    print(f"EVM Differential Fuzzer")
    print(f"  art:    {args.art}")
    print(f"  geth:   {args.geth}")
    print(f"  fork:   {args.fork}")
    print(f"  iters:  {'infinite' if args.iterations == 0 else args.iterations}")
    print(f"  seed:   {args.seed}")
    if args.fail_fast:
        print(f"  mode:   fail-fast (stop on first mismatch)")
    print(flush=True)

    i = 0
    while args.iterations == 0 or i < args.iterations:
        total += 1

        # Generate and write test case
        alloc, env, txs = generate_test_case(args.fork)
        write_test_case(current_dir, alloc, env, txs)

        # Run both tools
        art_result, art_error = run_t8n(
            args.art, alloc_path, env_path, txs_path, args.fork, is_geth=False
        )
        geth_result, geth_error = run_t8n(
            args.geth, alloc_path, env_path, txs_path, args.fork, is_geth=True
        )

        # Handle errors
        if art_error and not geth_error:
            art_errors += 1
            if args.verbose:
                print(f"[{i}] art error: {art_error}")
            fail_dir = save_failing_case(
                args.work_dir, alloc, env, txs,
                art_result, geth_result, [], art_error, geth_error
            )
            if args.verbose:
                print(f"  saved: {fail_dir}")
            if args.fail_fast:
                print(f"\nFail-fast: art error at iteration {i}")
                break
            i += 1
            continue

        if geth_error and not art_error:
            geth_errors += 1
            if args.verbose:
                print(f"[{i}] geth error: {geth_error}")
            i += 1
            continue

        if art_error and geth_error:
            # Both errored — skip (likely invalid test case)
            if args.verbose:
                print(f"[{i}] both errored")
            i += 1
            continue

        # Compare results
        diffs = compare_results(art_result, geth_result)
        if diffs:
            mismatches += 1
            print(f"\n[{i}] MISMATCH:")
            for d in diffs:
                print(f"  {d}")

            fail_dir = save_failing_case(
                args.work_dir, alloc, env, txs,
                art_result, geth_result, diffs, None, None
            )
            print(f"  saved: {fail_dir}")
            if args.fail_fast:
                break
        else:
            matches += 1
            iters_str = "inf" if args.iterations == 0 else str(args.iterations)
            if args.verbose or (i + 1) % 100 == 0:
                elapsed = time.time() - start_time
                rate = total / elapsed if elapsed > 0 else 0
                print(
                    f"[{i+1}/{iters_str}] "
                    f"match={matches} mismatch={mismatches} "
                    f"art_err={art_errors} geth_err={geth_errors} "
                    f"({rate:.1f}/s)",
                    flush=True
                )

        i += 1

    # Final summary
    elapsed = time.time() - start_time
    print(f"\n{'='*60}")
    print(f"Fuzzing complete: {total} iterations in {elapsed:.1f}s")
    print(f"  Matches:    {matches}")
    print(f"  Mismatches: {mismatches}")
    print(f"  Art errors: {art_errors}")
    print(f"  Geth errors: {geth_errors}")
    if mismatches > 0:
        print(f"  Failing cases saved to: {os.path.join(args.work_dir, 'failing')}")
    print(f"{'='*60}")

    # Save stats
    stats = {
        "total": total,
        "matches": matches,
        "mismatches": mismatches,
        "art_errors": art_errors,
        "geth_errors": geth_errors,
        "elapsed_seconds": elapsed,
        "fork": args.fork,
        "seed": args.seed,
    }
    with open(os.path.join(args.work_dir, "stats.json"), "w") as f:
        json.dump(stats, f, indent=2)

    return 1 if mismatches > 0 else 0


if __name__ == "__main__":
    sys.exit(main())
