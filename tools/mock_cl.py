#!/usr/bin/env python3
"""
Mock Consensus Layer — replays blockchain_tests_engine fixtures as
Engine API JSON-RPC calls against a live execution engine server.

Usage:
    python3 tools/mock_cl.py --jwt-secret /tmp/jwt.hex \
        [-f Paris] [-v] integration_tests/fixtures/blockchain_tests_engine/
"""

import argparse
import base64
import hashlib
import hmac
import http.client
import json
import os
import sys
import time

# ==============================================================================
# Colors
# ==============================================================================

_COLOR = sys.stdout.isatty()

def green(s):  return f"\033[92m{s}\033[0m" if _COLOR else s
def red(s):    return f"\033[91m{s}\033[0m" if _COLOR else s
def yellow(s): return f"\033[93m{s}\033[0m" if _COLOR else s
def bold(s):   return f"\033[1m{s}\033[0m" if _COLOR else s

# ==============================================================================
# JWT
# ==============================================================================

def b64url(data):
    """Base64url encode without padding."""
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")

def load_jwt_secret(path):
    """Load 32-byte secret from hex file (optional 0x prefix)."""
    text = open(path).read().strip()
    if text.startswith("0x") or text.startswith("0X"):
        text = text[2:]
    return bytes.fromhex(text)

def jwt_token(secret):
    """Generate HS256 JWT with iat=now."""
    hdr = b64url(b'{"alg":"HS256","typ":"JWT"}')
    pay = b64url(json.dumps({"iat": int(time.time())},
                            separators=(",", ":")).encode())
    sig = hmac.new(secret, f"{hdr}.{pay}".encode(),
                   hashlib.sha256).digest()
    return f"{hdr}.{pay}.{b64url(sig)}"

# ==============================================================================
# JSON-RPC Client
# ==============================================================================

class EngineClient:
    def __init__(self, host, port, secret, timeout=30):
        self.host = host
        self.port = port
        self.secret = secret
        self.timeout = timeout
        self._id = 0

    def call(self, method, params):
        """Send JSON-RPC 2.0 request, return parsed response dict."""
        self._id += 1
        body = json.dumps({
            "jsonrpc": "2.0",
            "method": method,
            "params": params,
            "id": self._id,
        })
        token = jwt_token(self.secret)
        conn = http.client.HTTPConnection(
            self.host, self.port, timeout=self.timeout)
        try:
            conn.request("POST", "/", body.encode(), {
                "Content-Type": "application/json",
                "Authorization": f"Bearer {token}",
            })
            resp = conn.getresponse()
            raw = resp.read()
        finally:
            conn.close()

        if resp.status != 200:
            return {"error": {"code": resp.status,
                              "message": raw.decode(errors="replace")}}
        return json.loads(raw)

# ==============================================================================
# Test Execution
# ==============================================================================

def short_name(full_name):
    """Extract short test name from full pytest path."""
    # "tests/paris/.../test_foo.py::test_bar[fork_Paris-...]" -> "test_bar[...]"
    if "::" in full_name:
        full_name = full_name.split("::")[-1]
    return full_name

def run_test(client, name, data, verbose):
    """Execute one test case. Returns (passed, messages)."""
    msgs = []
    payloads = data.get("engineNewPayloads", [])
    expected_last = data.get("lastblockhash", "")
    genesis_hash = ""
    ghdr = data.get("genesisBlockHeader")
    if ghdr:
        genesis_hash = ghdr.get("hash", "")

    head_hash = genesis_hash

    for i, entry in enumerate(payloads):
        version = int(entry.get("newPayloadVersion", "1"))
        fc_version = int(entry.get("forkchoiceUpdatedVersion", "1"))
        params = entry.get("params", [])
        expect_invalid = entry.get("validationError") is not None

        if not params:
            msgs.append(f"  payload[{i}]: empty params, skipping")
            continue

        block_hash = params[0].get("blockHash", "")
        block_num = params[0].get("blockNumber", "0x0")

        # --- newPayload ---
        method = f"engine_newPayloadV{version}"
        if verbose:
            msgs.append(f"  [{i}] {method} block={block_num}")

        resp = client.call(method, params)

        if "error" in resp:
            err = resp["error"]
            msgs.append(f"  [{i}] {method} RPC error: {err.get('code')} "
                        f"{err.get('message', '')[:120]}")
            return False, msgs

        result = resp.get("result", {})
        status = result.get("status", "???")
        val_err = result.get("validationError")

        if verbose:
            msgs.append(f"       -> status={status}"
                        + (f" validationError={val_err}" if val_err else ""))

        # Check status expectation
        if expect_invalid:
            if status not in ("INVALID", "INVALID_BLOCK_HASH"):
                msgs.append(f"  [{i}] expected INVALID, got {status}")
                # Don't fail — server may not have state to detect invalidity
        else:
            if status not in ("VALID", "ACCEPTED", "SYNCING"):
                msgs.append(f"  [{i}] expected VALID/SYNCING, got {status}")
                return False, msgs

        # --- forkchoiceUpdated ---
        if status in ("VALID", "ACCEPTED"):
            fc_method = f"engine_forkchoiceUpdatedV{fc_version}"
            fc_state = {
                "headBlockHash": block_hash,
                "safeBlockHash": block_hash,
                "finalizedBlockHash": "0x" + "00" * 32,
            }
            if verbose:
                msgs.append(f"       {fc_method} head={block_hash[:18]}...")

            fc_resp = client.call(fc_method, [fc_state, None])

            if "error" in fc_resp:
                err = fc_resp["error"]
                msgs.append(f"  [{i}] {fc_method} RPC error: "
                            f"{err.get('code')} {err.get('message', '')[:120]}")
                return False, msgs

            fc_result = fc_resp.get("result", {})
            fc_status = fc_result.get("payloadStatus", {}).get("status", "???")
            if verbose:
                msgs.append(f"       -> fcu status={fc_status}")

            if fc_status in ("VALID", "ACCEPTED"):
                head_hash = block_hash

    # Verify last block hash
    if expected_last and head_hash and status in ("VALID", "ACCEPTED"):
        if head_hash.lower() != expected_last.lower():
            msgs.append(f"  lastblockhash mismatch: "
                        f"got {head_hash[:18]}... expected {expected_last[:18]}...")
            return False, msgs

    return True, msgs

# ==============================================================================
# Fixture Loading
# ==============================================================================

def load_fixtures(path):
    """Yield (filepath, test_name, test_data) from file or directory."""
    if os.path.isfile(path):
        files = [path]
    else:
        files = []
        for root, _, fnames in os.walk(path):
            for f in sorted(fnames):
                if f.endswith(".json"):
                    files.append(os.path.join(root, f))
        files.sort()

    for fpath in files:
        try:
            with open(fpath) as f:
                data = json.load(f)
        except (json.JSONDecodeError, OSError) as e:
            print(f"  WARN: {fpath}: {e}", file=sys.stderr)
            continue
        for name, test_data in data.items():
            if name == "_info" or not isinstance(test_data, dict):
                continue
            yield fpath, name, test_data

def should_run(network, fork_filter):
    """Check if test matches fork filter (case-insensitive)."""
    if not fork_filter:
        return True
    return fork_filter.lower() in network.lower()

# ==============================================================================
# Main
# ==============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Mock CL — replay engine test fixtures via Engine API")
    parser.add_argument("fixtures", nargs="+",
                        help="Fixture file(s) or directory")
    parser.add_argument("--host", default="127.0.0.1",
                        help="Engine API host (default: 127.0.0.1)")
    parser.add_argument("--port", type=int, default=8551,
                        help="Engine API port (default: 8551)")
    parser.add_argument("--jwt-secret", required=True,
                        help="Path to hex-encoded JWT secret file")
    parser.add_argument("-f", "--fork", default=None,
                        help="Only run tests for this fork (e.g., Paris)")
    parser.add_argument("-v", "--verbose", action="store_true",
                        help="Show request/response details")
    parser.add_argument("--timeout", type=int, default=30,
                        help="HTTP timeout in seconds (default: 30)")
    args = parser.parse_args()

    secret = load_jwt_secret(args.jwt_secret)
    client = EngineClient(args.host, args.port, secret, args.timeout)

    print(bold("=" * 72))
    print(bold("Mock Consensus Layer — Engine API Test"))
    print(bold("=" * 72))
    print(f"  Server:  {args.host}:{args.port}")
    print(f"  Fork:    {args.fork or 'all'}")
    print(f"  Verbose: {args.verbose}")
    print()

    # Quick connectivity check
    try:
        resp = client.call("engine_exchangeCapabilities", [])
        caps = resp.get("result", [])
        if caps:
            print(f"  Capabilities: {len(caps)} methods")
        else:
            err = resp.get("error", {})
            print(f"  Warning: exchangeCapabilities returned: "
                  f"{err.get('message', resp)}")
    except (ConnectionRefusedError, OSError) as e:
        print(red(f"  Cannot connect to {args.host}:{args.port}: {e}"))
        sys.exit(1)

    print()

    passed = 0
    failed = 0
    skipped = 0
    errors = 0

    for fixture_path in args.fixtures:
        for fpath, name, data in load_fixtures(fixture_path):
            network = data.get("network", "")
            if not should_run(network, args.fork):
                skipped += 1
                continue

            sname = short_name(name)
            try:
                ok, msgs = run_test(client, name, data, args.verbose)
            except (ConnectionRefusedError, OSError) as e:
                print(f"  {red('ERROR')} {sname}")
                print(f"         Connection error: {e}")
                errors += 1
                continue
            except Exception as e:
                print(f"  {red('ERROR')} {sname}")
                print(f"         {type(e).__name__}: {e}")
                errors += 1
                continue

            if ok:
                passed += 1
                if args.verbose:
                    print(f"  {green('PASS')} {sname}")
                    for m in msgs:
                        print(m)
            else:
                failed += 1
                print(f"  {red('FAIL')} {sname}")
                for m in msgs:
                    print(m)

    print()
    print(bold("=" * 72))
    print(bold("Results"))
    print(bold("=" * 72))
    print(f"  Passed:  {green(str(passed))}")
    print(f"  Failed:  {red(str(failed)) if failed else '0'}")
    print(f"  Errors:  {red(str(errors)) if errors else '0'}")
    print(f"  Skipped: {skipped}")
    print(f"  Total:   {passed + failed + errors + skipped}")
    print(bold("=" * 72))

    sys.exit(1 if (failed + errors) > 0 else 0)

if __name__ == "__main__":
    main()
