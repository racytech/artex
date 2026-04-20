#!/usr/bin/env python3
"""
artex_cli.py — interactive REPL for the artex execution engine.

A small exploratory shell that keeps one engine alive across
commands. Load state, query accounts, execute blocks from era files,
compute roots, save snapshots — all interactively.

Example session:

    $ python3 artex_cli.py
    artex> version
    libartex 0.1.0
    artex> load-state ~/.artex/state_24864361.bin
    loaded state at block 24864361
    artex> root
    0x7a8f...
    artex> balance 0xdAC17F958D2ee523a2206206994597C13D831ec7
    0 ETH  (USDT contract)
    artex> execute data/era --to 24864500 --every 64
    block 24864361 → 24864500 (140 blocks) ...
    artex> save-state /tmp/snap.bin
    artex> quit
"""

from __future__ import annotations

import argparse
import cmd
import os
import shlex
import sys
import time

from artex import Engine, Config, adaptive_interval, version, ArtexError


# =============================================================================
# Helpers
# =============================================================================

def parse_addr(s: str) -> str:
    """Strip 0x prefix, validate. Returns hex string for .balance() etc."""
    if not s.startswith(("0x", "0X")):
        raise ValueError(f"address must start with 0x: {s!r}")
    if len(s) != 42:
        raise ValueError(f"address must be 42 hex chars (incl 0x): {s!r}")
    bytes.fromhex(s[2:])  # validate
    return s


def parse_int(s: str) -> int:
    return int(s, 16) if s.startswith(("0x", "0X")) else int(s)


def fmt_wei(wei: int) -> str:
    if wei == 0:
        return "0 ETH"
    eth = wei / 1e18
    if eth >= 0.0001:
        return f"{eth:.6f} ETH"
    return f"{wei:,} wei"


# =============================================================================
# Shell
# =============================================================================

class ArtexShell(cmd.Cmd):
    intro = (
        "\n  artex interactive CLI — type 'help' or '?' for commands, 'quit' to exit.\n"
        "  load a snapshot or genesis first, then query / execute / save.\n"
    )
    prompt = "artex> "

    def __init__(
        self,
        initial_state: str | None = None,
        data_dir: str | None = None,
    ):
        super().__init__()
        # If --data-dir not given but --state is, infer data_dir from the
        # snapshot's directory (so chain_replay_code.{dat,idx} alongside it
        # are picked up automatically — standard snapshot-package layout).
        if data_dir is None and initial_state:
            data_dir = os.path.dirname(os.path.abspath(os.path.expanduser(initial_state)))

        self.data_dir = data_dir
        self.engine = Engine(Config(data_dir=data_dir))
        self.initial_state = initial_state
        print(f"libartex version: {version()}")
        if data_dir:
            print(f"data dir: {data_dir}")

    # ------------------------------------------------------------------
    # Cleanup
    # ------------------------------------------------------------------

    def postloop(self):
        try:
            self.engine.close()
        except Exception:
            pass

    def preloop(self):
        if self.initial_state:
            self._safe("load-state " + self.initial_state, self.do_load_state)

    # ------------------------------------------------------------------
    # Error wrapper: show errors, never bubble to Cmd
    # ------------------------------------------------------------------

    def _safe(self, _line_echoed, fn, arg: str = ""):
        try:
            fn(arg)
        except ArtexError as e:
            print(f"  error: {e}")
        except ValueError as e:
            print(f"  value error: {e}")
        except FileNotFoundError as e:
            print(f"  not found: {e}")
        except Exception as e:
            print(f"  {type(e).__name__}: {e}")

    # ------------------------------------------------------------------
    # Meta
    # ------------------------------------------------------------------

    def do_version(self, _):
        """version                   print libartex version"""
        print(f"libartex {version()}")

    def do_block(self, _):
        """block                     current block number"""
        print(self.engine.block_number())

    def do_root(self, _):
        """root                      compute + print current state root"""
        r = self.engine.state_root()
        print("0x" + r.hex())

    def do_quit(self, _):
        """quit                      exit"""
        print("bye")
        return True

    do_exit = do_quit
    do_EOF = lambda self, _: self.do_quit(_)

    # ------------------------------------------------------------------
    # Load / save
    # ------------------------------------------------------------------

    def do_load_state(self, arg):
        """load-state <path>         load a state snapshot .bin"""
        path = arg.strip()
        if not path:
            print("usage: load-state <path>")
            return
        self._safe(arg, lambda _: self._load_state(path))

    def _load_state(self, path):
        path = os.path.expanduser(path)

        # If the engine was created without a data_dir, infer it from this
        # snapshot's directory (so chain_replay_code.{dat,idx} alongside
        # it get picked up). If it differs from what we already have, warn.
        inferred = os.path.dirname(os.path.abspath(path))
        if self.data_dir is None:
            self.engine.close()
            self.data_dir = inferred
            self.engine = Engine(Config(data_dir=inferred))
            print(f"  data dir set to: {inferred}")
        elif self.data_dir != inferred:
            print(f"  warning: snapshot is in {inferred} but engine uses "
                  f"data dir {self.data_dir} — code_store lookups may miss")

        t0 = time.time()
        print(f"  loading {path} ...")
        self.engine.load_state(path)
        dt = time.time() - t0
        print(f"  loaded state at block {self.engine.block_number()} ({dt:.1f}s)")

    def do_load_genesis(self, arg):
        """load-genesis <path>       load genesis JSON (alternative to load-state)"""
        path = arg.strip()
        if not path:
            print("usage: load-genesis <path>")
            return
        self._safe(arg, lambda _: self._load_genesis(path))

    def _load_genesis(self, path):
        path = os.path.expanduser(path)
        self.engine.load_genesis(path)
        print(f"  loaded genesis (block {self.engine.block_number()})")

    def do_save_state(self, arg):
        """save-state [name]        write state snapshot to the data dir.
           Default name: state_<block>.bin in the load-time data dir.
           A matching .hashes sidecar is written automatically by the
           library so the output is a complete snapshot package
           (pair with the existing chain_replay_code.{dat,idx} that
           already live there)."""
        name = arg.strip()
        self._safe(arg, lambda _: self._save_state(name))

    def _save_state(self, name):
        if not self.data_dir:
            print("  cannot save: no data dir set (load a snapshot first)")
            return

        if not name:
            name = f"state_{self.engine.block_number()}.bin"

        # Relative name → resolve inside data_dir. Absolute → warn if
        # outside data_dir (code_store files won't travel with it).
        if os.path.isabs(name):
            path = name
            if os.path.dirname(os.path.abspath(path)) != os.path.abspath(self.data_dir):
                print(f"  warning: writing outside data dir; "
                      f"chain_replay_code.{{dat,idx}} stay at {self.data_dir}")
        else:
            path = os.path.join(self.data_dir, name)

        t0 = time.time()
        print(f"  saving to {path} ...")
        self.engine.save_state(path)
        dt = time.time() - t0
        print(f"  saved ({dt:.1f}s)")
        # rx_engine_save_state also wrote <path>.hashes
        print(f"  wrote sidecar: {path}.hashes")

    # ------------------------------------------------------------------
    # Queries
    # ------------------------------------------------------------------

    def do_balance(self, arg):
        """balance <addr>            get ETH balance"""
        self._safe(arg, lambda a: print(fmt_wei(self.engine.balance(parse_addr(a.strip())))))

    def do_nonce(self, arg):
        """nonce <addr>              get transaction count"""
        self._safe(arg, lambda a: print(self.engine.nonce(parse_addr(a.strip()))))

    def do_code_size(self, arg):
        """code-size <addr>          get bytecode size in bytes"""
        self._safe(arg, lambda a: print(self.engine.code_size(parse_addr(a.strip())), "bytes"))

    def do_code_hash(self, arg):
        """code-hash <addr>          get keccak256(code)"""
        self._safe(arg, lambda a: print("0x" + self.engine.code_hash(parse_addr(a.strip())).hex()))

    def do_exists(self, arg):
        """exists <addr>             check if account exists"""
        self._safe(arg, lambda a: print(self.engine.exists(parse_addr(a.strip()))))

    def do_storage(self, arg):
        """storage <addr> <slot>     read storage slot (hex or decimal)"""
        parts = shlex.split(arg)
        if len(parts) != 2:
            print("usage: storage <addr> <slot>")
            return
        self._safe(arg, lambda _: self._storage(parts[0], parts[1]))

    def _storage(self, addr_s, slot_s):
        addr = parse_addr(addr_s)
        slot = parse_int(slot_s)
        val = self.engine.storage(addr, slot)
        print(f"  0x{val:064x}")
        if val != 0:
            print(f"  = {val} (decimal)")

    # ------------------------------------------------------------------
    # Execute from era
    # ------------------------------------------------------------------

    def do_execute(self, arg):
        """execute <era-dir> [--to N] [--every N] [--adaptive]
           Replay blocks from post-merge era files. --every fixes the
           validation interval; --adaptive uses adaptive_interval (shrinks
           as we approach --to)."""
        ap = argparse.ArgumentParser(prog="execute", add_help=False)
        ap.add_argument("era_dir")
        ap.add_argument("--to", type=int, default=0, help="stop at this block")
        ap.add_argument("--every", type=int, default=256, help="validate root every N blocks (fixed)")
        ap.add_argument("--adaptive", action="store_true", help="use adaptive_interval")
        try:
            opts = ap.parse_args(shlex.split(arg))
        except SystemExit:
            return

        self._safe(arg, lambda _: self._execute(opts))

    def _execute(self, opts):
        from era import iter_era_blocks, ep_to_header_body

        start_block = self.engine.block_number() + 1
        target = opts.to if opts.to > 0 else 10**18
        print(f"  replaying from {start_block} → {target if opts.to else '∞'} "
              f"({'adaptive' if opts.adaptive else f'every {opts.every}'} validation)")

        # Block hash ring was populated by rx_engine_load_state from the
        # mandatory .hashes sidecar. No pre-populate from era files needed —
        # if load_state succeeded, the ring is already correct.

        blocks = 0
        gas_total = 0
        t0 = time.time()
        t_window = t0
        window_blocks = 0
        window_gas = 0
        validated = 0

        try:
            for b in iter_era_blocks(opts.era_dir, start_block):
                bn = b.number
                if bn < start_block:
                    continue
                if opts.to and bn > opts.to:
                    break

                # parent_beacon_root is required for Deneb+ (EIP-4788)
                header, body, block_hash = ep_to_header_body(
                    b.ep, parent_beacon_root=b.parent_beacon_root)
                self.engine.set_block_hash(bn, block_hash)

                if opts.adaptive:
                    interval = adaptive_interval(bn, target)
                else:
                    interval = opts.every
                validate = (bn % interval == 0) or (opts.to and bn == opts.to)

                result = self.engine.execute_block(
                    header, body, block_hash, compute_root=validate)

                if result.gas_used != header.gas_used:
                    print(f"  ✗ gas mismatch at {bn}: "
                          f"expected {header.gas_used} got {result.gas_used}")
                    self.engine.revert_block()
                    break

                if validate:
                    expected = bytes(header.state_root.bytes)
                    if result.state_root != expected:
                        print(f"  ✗ root mismatch at {bn}")
                        print(f"    expected: 0x{expected.hex()}")
                        print(f"    got:      0x{result.state_root.hex()}")
                        self.engine.revert_block()
                        break
                    validated += 1

                self.engine.commit_block()
                blocks += 1
                gas_total += header.gas_used
                window_blocks += 1
                window_gas += header.gas_used

                now = time.time()
                if now - t_window >= 5.0:
                    dt = now - t_window
                    bps = window_blocks / dt if dt else 0
                    mgps = window_gas / dt / 1e6 if dt else 0
                    print(f"  block {bn}  |  {bps:6.1f} blk/s  |  "
                          f"{mgps:6.0f} Mgas/s  |  interval={interval}  |  validated={validated}")
                    t_window = now
                    window_blocks = 0
                    window_gas = 0
        except KeyboardInterrupt:
            print("\n  interrupted")

        dt = time.time() - t0
        print()
        print(f"  blocks    : {blocks}")
        print(f"  total gas : {gas_total:,}")
        print(f"  time      : {dt:.1f}s")
        if dt > 0:
            print(f"  avg       : {blocks/dt:.1f} blk/s, {gas_total/dt/1e6:.0f} Mgas/s")
        print(f"  validated : {validated}")

    # ------------------------------------------------------------------
    # cmd.Cmd boilerplate
    # ------------------------------------------------------------------

    def default(self, line):
        if line.strip():
            print(f"  unknown command: {line.split()[0]!r} (type 'help' for list)")

    def emptyline(self):
        pass  # don't repeat previous command on bare Enter


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[1])
    ap.add_argument(
        "--state", required=True,
        help="path to the state snapshot .bin. The directory containing it "
             "must also hold chain_replay_code.{dat,idx} and the "
             "<snapshot>.hashes sidecar.",
    )
    ap.add_argument(
        "--data-dir",
        help="override the directory holding chain_replay_code.{dat,idx}. "
             "Defaults to the --state file's directory.",
    )
    args = ap.parse_args()

    # Validate the snapshot package upfront so the user gets a clear error
    # before the engine starts a 90 s load.
    state_path = os.path.abspath(os.path.expanduser(args.state))
    data_dir = args.data_dir or os.path.dirname(state_path)

    required = [
        (state_path, "state snapshot"),
        (state_path + ".hashes",
         "block-hash sidecar (run tools/make_hashes.py if missing)"),
        (os.path.join(data_dir, "chain_replay_code.dat"), "code store data"),
        (os.path.join(data_dir, "chain_replay_code.idx"), "code store index"),
    ]
    missing = [(p, label) for p, label in required if not os.path.isfile(p)]
    if missing:
        print("ERROR: snapshot package is incomplete — missing files:")
        for p, label in missing:
            print(f"  - {p}")
            print(f"      ({label})")
        print()
        print("All four files must live in the same directory. See the"
              " README under 'Snapshot files' for details.")
        return 1

    sh = ArtexShell(initial_state=state_path, data_dir=data_dir)
    try:
        sh.cmdloop()
    except KeyboardInterrupt:
        print("\n(ctrl-c)")
        sh.postloop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
