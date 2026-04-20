"""
artex.py — high-level Python wrapper around libartex.

The artex library exposes a stable C API (rx_* functions). This module
wraps it in a Pythonic Engine class with context-manager support,
exceptions instead of bool returns, and int/bytes types instead of
ctypes structs on the public surface.

Low-level ctypes structs are still exported (RxHash, RxAddress, etc.)
for advanced users who need to drop down to the raw API.

Typical usage:

    from artex import Engine, Config

    with Engine(Config(chain_id=1, data_dir="./state")) as engine:
        engine.load_state("state_24864361.bin")
        print("block:", engine.block_number())
        print("root:", engine.state_root().hex())

        # execute a block (raw RLP)
        result = engine.execute_block_rlp(header_rlp, body_rlp, block_hash)
        print("tx count:", result.tx_count, "gas:", result.gas_used)
        engine.commit_block()
"""

from __future__ import annotations

import ctypes
import ctypes.util
import os
from dataclasses import dataclass
from pathlib import Path


# =============================================================================
# Locate libartex
# =============================================================================

def _find_libartex() -> str:
    """Try common locations for libartex.so. Override with ARTEX_LIB env var."""
    env = os.environ.get("ARTEX_LIB")
    if env and os.path.isfile(env):
        return env

    here = Path(__file__).resolve().parent
    # examples/python/ → repo root → build/libartex.so
    candidates = [
        here.parent.parent / "build" / "libartex.so",
        here.parent.parent / "build" / "libartex.so.0",
        Path("/usr/local/lib/libartex.so"),
    ]
    for p in candidates:
        if p.is_file():
            return str(p)

    so = ctypes.util.find_library("artex")
    if so:
        return so

    raise RuntimeError(
        "libartex.so not found — set ARTEX_LIB env var or build in ./build"
    )


# =============================================================================
# Low-level ctypes structs (mirror include/artex.h)
# =============================================================================

class RxHash(ctypes.Structure):
    _fields_ = [("bytes", ctypes.c_uint8 * 32)]

class RxAddress(ctypes.Structure):
    _fields_ = [("bytes", ctypes.c_uint8 * 20)]

class RxUint256(ctypes.Structure):
    _fields_ = [("bytes", ctypes.c_uint8 * 32)]

class RxConfig(ctypes.Structure):
    _fields_ = [
        ("chain_id", ctypes.c_int),
        ("data_dir", ctypes.c_char_p),
    ]

class RxReceipt(ctypes.Structure):
    _fields_ = [
        ("status", ctypes.c_uint8),
        ("tx_type", ctypes.c_uint8),
        ("gas_used", ctypes.c_uint64),
        ("cumulative_gas", ctypes.c_uint64),
        ("logs_bloom", ctypes.c_uint8 * 256),
        ("logs", ctypes.c_void_p),
        ("log_count", ctypes.c_size_t),
        ("contract_created", ctypes.c_bool),
        ("contract_addr", RxAddress),
    ]

class RxBlockResult(ctypes.Structure):
    _fields_ = [
        ("ok", ctypes.c_bool),
        ("gas_used", ctypes.c_uint64),
        ("tx_count", ctypes.c_size_t),
        ("state_root", RxHash),
        ("receipt_root", RxHash),
        ("logs_bloom", ctypes.c_uint8 * 256),
        ("receipts", ctypes.POINTER(RxReceipt)),
    ]


class RxBlockHeader(ctypes.Structure):
    _fields_ = [
        ("parent_hash", RxHash),
        ("uncle_hash", RxHash),
        ("coinbase", RxAddress),
        ("state_root", RxHash),
        ("tx_root", RxHash),
        ("receipt_root", RxHash),
        ("logs_bloom", ctypes.c_uint8 * 256),
        ("difficulty", RxUint256),
        ("number", ctypes.c_uint64),
        ("gas_limit", ctypes.c_uint64),
        ("gas_used", ctypes.c_uint64),
        ("timestamp", ctypes.c_uint64),
        ("extra_data", ctypes.c_uint8 * 32),
        ("extra_data_len", ctypes.c_size_t),
        ("mix_hash", RxHash),
        ("nonce", ctypes.c_uint64),
        ("has_base_fee", ctypes.c_bool),
        ("base_fee", RxUint256),
        ("has_withdrawals_root", ctypes.c_bool),
        ("withdrawals_root", RxHash),
        ("has_blob_gas", ctypes.c_bool),
        ("blob_gas_used", ctypes.c_uint64),
        ("excess_blob_gas", ctypes.c_uint64),
        ("has_parent_beacon_root", ctypes.c_bool),
        ("parent_beacon_root", RxHash),
        ("has_requests_hash", ctypes.c_bool),
        ("requests_hash", RxHash),
    ]


class RxWithdrawal(ctypes.Structure):
    _fields_ = [
        ("index", ctypes.c_uint64),
        ("validator_index", ctypes.c_uint64),
        ("address", RxAddress),
        ("amount_gwei", ctypes.c_uint64),
    ]


class RxBlockBody(ctypes.Structure):
    _fields_ = [
        ("transactions", ctypes.POINTER(ctypes.POINTER(ctypes.c_uint8))),
        ("tx_lengths", ctypes.POINTER(ctypes.c_size_t)),
        ("tx_count", ctypes.c_size_t),
        ("withdrawals", ctypes.POINTER(RxWithdrawal)),
        ("withdrawal_count", ctypes.c_size_t),
    ]


# =============================================================================
# Error handling
# =============================================================================

# rx_error_t enum
class RxError:
    OK = 0
    NULL_ARG = 1
    INVALID_CONFIG = 2
    OUT_OF_MEMORY = 3
    ALREADY_INIT = 4
    NOT_INIT = 5
    FILE_IO = 6
    PARSE = 7
    DECODE = 8
    EXECUTION = 9
    BLOCK_NOT_FOUND = 10


class ArtexError(Exception):
    """Raised when an rx_* call fails. Contains code + message from engine."""

    def __init__(self, code: int, message: str):
        self.code = code
        self.message = message
        super().__init__(f"[code={code}] {message}")


# =============================================================================
# Load + bind library symbols
# =============================================================================

_LIB: ctypes.CDLL | None = None


def _lib() -> ctypes.CDLL:
    global _LIB
    if _LIB is not None:
        return _LIB
    lib = ctypes.CDLL(_find_libartex())

    lib.rx_version.restype = ctypes.c_char_p
    lib.rx_version.argtypes = []

    lib.rx_error_string.restype = ctypes.c_char_p
    lib.rx_error_string.argtypes = [ctypes.c_int]

    lib.rx_engine_create.restype = ctypes.c_void_p
    lib.rx_engine_create.argtypes = [ctypes.POINTER(RxConfig)]

    lib.rx_engine_destroy.restype = None
    lib.rx_engine_destroy.argtypes = [ctypes.c_void_p]

    lib.rx_engine_last_error.restype = ctypes.c_int
    lib.rx_engine_last_error.argtypes = [ctypes.c_void_p]

    lib.rx_engine_last_error_msg.restype = ctypes.c_char_p
    lib.rx_engine_last_error_msg.argtypes = [ctypes.c_void_p]

    lib.rx_engine_load_genesis.restype = ctypes.c_bool
    lib.rx_engine_load_genesis.argtypes = [
        ctypes.c_void_p, ctypes.c_char_p, ctypes.POINTER(RxHash)]

    lib.rx_engine_load_state.restype = ctypes.c_bool
    lib.rx_engine_load_state.argtypes = [ctypes.c_void_p, ctypes.c_char_p]

    lib.rx_engine_save_state.restype = ctypes.c_bool
    lib.rx_engine_save_state.argtypes = [ctypes.c_void_p, ctypes.c_char_p]

    lib.rx_execute_block_rlp.restype = ctypes.c_bool
    lib.rx_execute_block_rlp.argtypes = [
        ctypes.c_void_p,
        ctypes.POINTER(ctypes.c_uint8), ctypes.c_size_t,
        ctypes.POINTER(ctypes.c_uint8), ctypes.c_size_t,
        ctypes.POINTER(RxHash),
        ctypes.c_bool,
        ctypes.POINTER(RxBlockResult),
    ]

    lib.rx_execute_block.restype = ctypes.c_bool
    lib.rx_execute_block.argtypes = [
        ctypes.c_void_p,
        ctypes.POINTER(RxBlockHeader),
        ctypes.POINTER(RxBlockBody),
        ctypes.POINTER(RxHash),
        ctypes.c_bool,
        ctypes.POINTER(RxBlockResult),
    ]

    lib.rx_commit_block.restype = ctypes.c_bool
    lib.rx_commit_block.argtypes = [ctypes.c_void_p]

    lib.rx_revert_block.restype = ctypes.c_bool
    lib.rx_revert_block.argtypes = [ctypes.c_void_p]

    lib.rx_block_result_free.restype = None
    lib.rx_block_result_free.argtypes = [ctypes.POINTER(RxBlockResult)]

    lib.rx_compute_state_root.restype = RxHash
    lib.rx_compute_state_root.argtypes = [ctypes.c_void_p]

    lib.rx_get_block_number.restype = ctypes.c_uint64
    lib.rx_get_block_number.argtypes = [ctypes.c_void_p]

    lib.rx_set_block_hash.restype = None
    lib.rx_set_block_hash.argtypes = [
        ctypes.c_void_p, ctypes.c_uint64, ctypes.POINTER(RxHash)]

    lib.rx_engine_get_state.restype = ctypes.c_void_p
    lib.rx_engine_get_state.argtypes = [ctypes.c_void_p]

    lib.rx_account_exists.restype = ctypes.c_bool
    lib.rx_account_exists.argtypes = [ctypes.c_void_p, ctypes.POINTER(RxAddress)]

    lib.rx_get_nonce.restype = ctypes.c_uint64
    lib.rx_get_nonce.argtypes = [ctypes.c_void_p, ctypes.POINTER(RxAddress)]

    lib.rx_get_balance.restype = RxUint256
    lib.rx_get_balance.argtypes = [ctypes.c_void_p, ctypes.POINTER(RxAddress)]

    lib.rx_get_code_hash.restype = RxHash
    lib.rx_get_code_hash.argtypes = [ctypes.c_void_p, ctypes.POINTER(RxAddress)]

    lib.rx_get_code_size.restype = ctypes.c_uint32
    lib.rx_get_code_size.argtypes = [ctypes.c_void_p, ctypes.POINTER(RxAddress)]

    lib.rx_get_storage.restype = RxUint256
    lib.rx_get_storage.argtypes = [
        ctypes.c_void_p,
        ctypes.POINTER(RxAddress),
        ctypes.POINTER(RxUint256),
    ]

    _LIB = lib
    return lib


# =============================================================================
# Pythonic helpers
# =============================================================================

def _addr(a: bytes | str) -> RxAddress:
    if isinstance(a, str):
        s = a[2:] if a.startswith(("0x", "0X")) else a
        a = bytes.fromhex(s)
    if len(a) != 20:
        raise ValueError(f"address must be 20 bytes, got {len(a)}")
    r = RxAddress()
    ctypes.memmove(r.bytes, a, 20)
    return r


def _u256(v: bytes | int) -> RxUint256:
    if isinstance(v, int):
        if v < 0 or v >= (1 << 256):
            raise ValueError("uint256 out of range")
        v = v.to_bytes(32, "big")
    if len(v) != 32:
        raise ValueError(f"uint256 must be 32 bytes, got {len(v)}")
    r = RxUint256()
    ctypes.memmove(r.bytes, v, 32)
    return r


def _u256_to_int(u: RxUint256) -> int:
    return int.from_bytes(bytes(u.bytes), "big")


# =============================================================================
# High-level wrappers
# =============================================================================

@dataclass
class Config:
    """Engine configuration. Only mainnet supported today."""
    chain_id: int = 1           # RX_CHAIN_MAINNET
    data_dir: str | None = None  # None = in-memory only

    def _to_rx(self) -> RxConfig:
        c = RxConfig()
        c.chain_id = self.chain_id
        c.data_dir = self.data_dir.encode() if self.data_dir else None
        return c


@dataclass
class BlockResult:
    """Pythonic view of an rx_block_result_t (receipts not exposed here)."""
    ok: bool
    gas_used: int
    tx_count: int
    state_root: bytes   # 32 bytes; zeroed if compute_root=False
    receipt_root: bytes # 32 bytes
    logs_bloom: bytes   # 256 bytes

    @classmethod
    def _from_rx(cls, r: RxBlockResult) -> "BlockResult":
        return cls(
            ok=r.ok,
            gas_used=r.gas_used,
            tx_count=r.tx_count,
            state_root=bytes(r.state_root.bytes),
            receipt_root=bytes(r.receipt_root.bytes),
            logs_bloom=bytes(r.logs_bloom),
        )


class Engine:
    """High-level wrapper around rx_engine_t. Use as a context manager
    to guarantee cleanup, or call close() explicitly."""

    def __init__(self, config: Config | None = None):
        lib = _lib()
        rx_cfg = (config or Config())._to_rx()
        self._lib = lib
        self._p = lib.rx_engine_create(ctypes.byref(rx_cfg))
        if not self._p:
            raise ArtexError(
                RxError.OUT_OF_MEMORY,
                "rx_engine_create returned NULL",
            )
        self._state_p = lib.rx_engine_get_state(self._p)

    # --- Lifecycle -----------------------------------------------------

    def close(self) -> None:
        if getattr(self, "_p", None):
            self._lib.rx_engine_destroy(self._p)
            self._p = None
            self._state_p = None

    def __enter__(self) -> "Engine":
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    def __del__(self):
        try:
            self.close()
        except Exception:
            pass

    # --- Error helpers -------------------------------------------------

    def _raise_last(self, fallback: str = "unknown error") -> None:
        code = self._lib.rx_engine_last_error(self._p)
        msg = self._lib.rx_engine_last_error_msg(self._p)
        raise ArtexError(code, (msg.decode() if msg else fallback))

    # --- Snapshot + genesis --------------------------------------------

    def load_state(self, path: str) -> None:
        if not self._lib.rx_engine_load_state(self._p, str(path).encode()):
            self._raise_last(f"load_state({path}) failed")

    def load_genesis(self, path: str, genesis_hash: bytes | None = None) -> None:
        gh_ptr = None
        if genesis_hash is not None:
            if len(genesis_hash) != 32:
                raise ValueError("genesis_hash must be 32 bytes")
            gh = RxHash()
            ctypes.memmove(gh.bytes, genesis_hash, 32)
            gh_ptr = ctypes.byref(gh)
        if not self._lib.rx_engine_load_genesis(self._p, str(path).encode(), gh_ptr):
            self._raise_last(f"load_genesis({path}) failed")

    def save_state(self, path: str) -> None:
        if not self._lib.rx_engine_save_state(self._p, str(path).encode()):
            self._raise_last(f"save_state({path}) failed")

    # --- Execution -----------------------------------------------------

    def execute_block_rlp(
        self,
        header_rlp: bytes,
        body_rlp: bytes,
        block_hash: bytes,
        *,
        compute_root: bool = True,
    ) -> BlockResult:
        """Execute a block from RLP-encoded header + body.
        Caller is responsible for calling commit_block() after a successful
        execution or revert_block() to discard. State queries are only
        valid BETWEEN blocks (after commit or before next execute).
        """
        if len(block_hash) != 32:
            raise ValueError("block_hash must be 32 bytes")

        bh = RxHash()
        ctypes.memmove(bh.bytes, block_hash, 32)

        hdr_buf = (ctypes.c_uint8 * len(header_rlp)).from_buffer_copy(header_rlp)
        body_buf = (ctypes.c_uint8 * len(body_rlp)).from_buffer_copy(body_rlp)
        result = RxBlockResult()

        ok = self._lib.rx_execute_block_rlp(
            self._p,
            hdr_buf, len(header_rlp),
            body_buf, len(body_rlp),
            ctypes.byref(bh),
            ctypes.c_bool(compute_root),
            ctypes.byref(result),
        )
        if not ok:
            self._raise_last("rx_execute_block_rlp failed")

        py_result = BlockResult._from_rx(result)
        self._lib.rx_block_result_free(ctypes.byref(result))
        return py_result

    def execute_block(
        self,
        header: "RxBlockHeader",
        body: "RxBlockBody",
        block_hash: bytes,
        *,
        compute_root: bool = True,
    ) -> BlockResult:
        """Execute a block from pre-decoded RxBlockHeader + RxBlockBody
        structs (skip the RLP decode step). Useful when your input is
        already in a decoded form — e.g., SSZ ExecutionPayload from
        era files. See era.ep_to_header_body for the construction helper.
        """
        if len(block_hash) != 32:
            raise ValueError("block_hash must be 32 bytes")
        bh = RxHash()
        ctypes.memmove(bh.bytes, block_hash, 32)

        result = RxBlockResult()
        ok = self._lib.rx_execute_block(
            self._p,
            ctypes.byref(header),
            ctypes.byref(body),
            ctypes.byref(bh),
            ctypes.c_bool(compute_root),
            ctypes.byref(result),
        )
        if not ok:
            self._raise_last("rx_execute_block failed")

        py_result = BlockResult._from_rx(result)
        self._lib.rx_block_result_free(ctypes.byref(result))
        return py_result

    def commit_block(self) -> None:
        if not self._lib.rx_commit_block(self._p):
            self._raise_last("rx_commit_block failed")

    def revert_block(self) -> None:
        if not self._lib.rx_revert_block(self._p):
            self._raise_last("rx_revert_block failed")

    # --- Root + block number ------------------------------------------

    def state_root(self) -> bytes:
        return bytes(self._lib.rx_compute_state_root(self._p).bytes)

    def block_number(self) -> int:
        return int(self._lib.rx_get_block_number(self._p))

    def set_block_hash(self, block_num: int, h: bytes) -> None:
        """Populate the BLOCKHASH opcode ring buffer."""
        if len(h) != 32:
            raise ValueError("block hash must be 32 bytes")
        rh = RxHash()
        ctypes.memmove(rh.bytes, h, 32)
        self._lib.rx_set_block_hash(self._p, block_num, ctypes.byref(rh))

    # --- State queries -------------------------------------------------

    def exists(self, addr: bytes | str) -> bool:
        return bool(self._lib.rx_account_exists(self._state_p, ctypes.byref(_addr(addr))))

    def nonce(self, addr: bytes | str) -> int:
        return int(self._lib.rx_get_nonce(self._state_p, ctypes.byref(_addr(addr))))

    def balance(self, addr: bytes | str) -> int:
        return _u256_to_int(self._lib.rx_get_balance(self._state_p, ctypes.byref(_addr(addr))))

    def code_hash(self, addr: bytes | str) -> bytes:
        return bytes(self._lib.rx_get_code_hash(self._state_p, ctypes.byref(_addr(addr))).bytes)

    def code_size(self, addr: bytes | str) -> int:
        return int(self._lib.rx_get_code_size(self._state_p, ctypes.byref(_addr(addr))))

    def storage(self, addr: bytes | str, key: bytes | int) -> int:
        return _u256_to_int(self._lib.rx_get_storage(
            self._state_p,
            ctypes.byref(_addr(addr)),
            ctypes.byref(_u256(key)),
        ))


# =============================================================================
# Adaptive checkpoint interval
# =============================================================================

def adaptive_interval(current: int, tip: int) -> int:
    """Pick validation cadence based on distance from tip.
    Far from tip → validate rarely (bulk sync throughput).
    Near tip → validate often (catch errors early).
    """
    behind = tip - current
    if behind > 100_000: return 1024
    if behind > 10_000:  return 256
    if behind > 1_000:   return 64
    if behind > 100:     return 16
    return 1


# =============================================================================
# Module metadata
# =============================================================================

def version() -> str:
    """Return libartex version string (e.g. '0.1.0')."""
    v = _lib().rx_version()
    return v.decode() if v else "unknown"


__all__ = [
    "Config", "Engine", "BlockResult", "ArtexError", "RxError",
    "adaptive_interval", "version",
    # low-level re-exports
    "RxHash", "RxAddress", "RxUint256", "RxConfig", "RxReceipt", "RxBlockResult",
    "RxBlockHeader", "RxBlockBody", "RxWithdrawal",
]
