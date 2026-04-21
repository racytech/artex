"""
era.py — reader for post-merge Ethereum era files.

Era files are an e2store archive of consensus-layer beacon blocks. Each
beacon block embeds an ExecutionPayload — the data execution clients
replay. This module parses era files, extracts ExecutionPayload bytes,
and converts them into the RxBlockHeader + RxBlockBody structs the
artex library consumes via rx_execute_block.

Adapted from tools/py_chain_replay.py. Supports Shanghai (Capella) and
Cancun (Deneb) era layouts. Earlier forks aren't post-merge, so they
don't exist in .era format — use .era1 for pre-merge blocks.

Dependencies:
    python-snappy   (snappy framed decoder for compressed blocks)

Example:

    from era import iter_era_blocks, ep_to_header_body

    for block_num, ep in iter_era_blocks("data/era", start_block=18_000_000):
        header, body, block_hash = ep_to_header_body(ep)
        engine.execute_block(header, body, block_hash, compute_root=True)
"""

from __future__ import annotations

import ctypes
import glob
import os
import struct
from dataclasses import dataclass

import hashlib

import snappy

from artex import (
    RxAddress, RxBlockBody, RxBlockHeader, RxBuildHeader, RxHash,
    RxUint256, RxWithdrawal,
)


@dataclass
class EraBlock:
    """One block extracted from an era file.

    `parent_beacon_root` is non-None for Deneb+ blocks (EIP-4788); it
    lives in the BeaconBlock header (NOT the ExecutionPayload) at
    offset 16 within the BeaconBlock (= slot(8) + proposer(8)).

    `requests_hash` is non-None for Electra/Prague+ blocks (EIP-7685);
    computed from the BeaconBlockBody.execution_requests field.
    """
    number: int
    ep: bytes
    parent_beacon_root: bytes | None = None   # 32 B if Deneb+
    requests_hash: bytes | None = None        # 32 B if Electra/Prague+


# =============================================================================
# e2store entry types
# =============================================================================

ENTRY_HEADER_SIZE = 8
TYPE_VERSION = 0x3265
TYPE_COMPRESSED_BLOCK = 0x0001
TYPE_SLOT_INDEX = 0x3269

# Execution payload field offsets (post-merge beacon block body)
EP_PARENT_HASH    = 0
EP_FEE_RECIPIENT  = 32
EP_STATE_ROOT     = 52
EP_RECEIPTS_ROOT  = 84
EP_LOGS_BLOOM     = 116
EP_PREV_RANDAO    = 372
EP_BLOCK_NUMBER   = 404
EP_GAS_LIMIT      = 412
EP_GAS_USED       = 420
EP_TIMESTAMP      = 428
EP_EXTRA_DATA_OFF = 436
EP_BASE_FEE       = 440
EP_BLOCK_HASH     = 472


# =============================================================================
# Byte helpers
# =============================================================================

def _le16(data, off): return struct.unpack_from('<H', data, off)[0]
def _le32(data, off): return struct.unpack_from('<I', data, off)[0]
def _le64(data, off): return struct.unpack_from('<Q', data, off)[0]
def _le256(data, off):
    return int.from_bytes(data[off:off+32], 'little')


def _snappy_frame_decode(data: bytes) -> bytes:
    """Decode snappy framed format (what era files use)."""
    if len(data) < 10 or data[0] != 0xFF or data[4:10] != b'sNaPpY':
        raise ValueError("invalid snappy framing header")
    pos = 10
    out = bytearray()
    while pos + 4 <= len(data):
        chunk_type = data[pos]
        chunk_len = data[pos + 1] | (data[pos + 2] << 8) | (data[pos + 3] << 16)
        pos += 4
        if pos + chunk_len > len(data):
            break
        if chunk_type == 0x00:
            out.extend(snappy.decompress(data[pos + 4:pos + chunk_len]))
        elif chunk_type == 0x01:
            out.extend(data[pos + 4:pos + chunk_len])
        pos += chunk_len
    return bytes(out)


# =============================================================================
# SSZ — locate ExecutionPayload inside a beacon block
# =============================================================================

def _find_execution_payload(ssz: bytes) -> bytes | None:
    """Navigate the SSZ offsets to extract ExecutionPayload bytes."""
    if len(ssz) < 104:
        return None
    msg_off = _le32(ssz, 0)
    if msg_off + 84 > len(ssz):
        return None
    bb = ssz[msg_off:]
    bb_len = len(ssz) - msg_off
    if bb_len < 84:
        return None
    body_off = _le32(bb, 80)
    if body_off > bb_len:
        return None
    body = bb[body_off:]
    body_len = bb_len - body_off
    if body_len < 384:
        return None
    ep_off = _le32(body, 380)
    if ep_off >= body_len:
        return None
    ep_end = body_len
    body_fixed = _le32(body, 200)
    if body_fixed > 384 and body_len >= body_fixed:
        next_off = _le32(body, 384)
        if next_off > ep_off and next_off <= body_len:
            ep_end = next_off
    return body[ep_off:ep_end]


def _probe_first_block(data: bytes, file_size: int) -> int | None:
    """Read the first block number from an era file without full scan."""
    if file_size < 16:
        return None
    pos = 0
    if file_size >= ENTRY_HEADER_SIZE:
        typ = _le16(data, 0)
        elen = _le32(data, 2)
        if typ == TYPE_VERSION:
            pos = ENTRY_HEADER_SIZE + elen

    while pos + ENTRY_HEADER_SIZE <= file_size:
        typ = _le16(data, pos)
        elen = _le32(data, pos + 2)
        value_start = pos + ENTRY_HEADER_SIZE
        if typ == TYPE_COMPRESSED_BLOCK and elen > 0:
            try:
                ssz = _snappy_frame_decode(data[value_start:value_start + elen])
                ep = _find_execution_payload(ssz)
                if ep and len(ep) >= 508:
                    bn = _le64(ep, EP_BLOCK_NUMBER)
                    if bn > 0:
                        return bn
            except Exception:
                pass
        pos = value_start + elen
    return None


# =============================================================================
# Public API
# =============================================================================

def _probe_file_first_block(path: str) -> int | None:
    """Fast probe: read only the first few MB to find the file's first
    block number, instead of loading the entire era file (can be 1 GB+)."""
    # 4 MB is more than enough for VERSION entry + one compressed block header.
    # Post-merge blocks with max blobs can be ~500 KB compressed; 4 MB covers it.
    PROBE_BYTES = 4 * 1024 * 1024
    try:
        with open(path, "rb") as f:
            data = f.read(PROBE_BYTES)
    except IOError:
        return None
    return _probe_first_block(data, len(data))


def iter_era_blocks(era_dir: str, start_block: int = 0):
    """Yield (block_number, execution_payload_bytes) from era files in order.

    era_dir: directory containing mainnet-*.era files
    start_block: skip blocks before this number
    """
    pattern = os.path.join(era_dir, "mainnet-*.era")
    files = sorted(glob.glob(pattern))
    if not files:
        raise FileNotFoundError(f"no mainnet-*.era files in {era_dir}")

    # Skip files whose blocks are entirely before start_block. Probe each
    # file's FIRST block only (4 MB read), not the whole file.
    start_idx = 0
    if start_block > 0:
        # Binary search over files: we want the largest i with
        # first_block(files[i]) <= start_block.
        lo, hi = 0, len(files) - 1
        start_idx = 0
        while lo <= hi:
            mid = (lo + hi) // 2
            bn = _probe_file_first_block(files[mid])
            if bn is None:
                # Treat unreadable files conservatively — advance past them
                lo = mid + 1
                continue
            if bn <= start_block:
                start_idx = mid  # candidate; keep trying further
                lo = mid + 1
            else:
                hi = mid - 1
        files = files[start_idx:]

    for path in files:
        try:
            with open(path, 'rb') as f:
                data = f.read()
        except IOError:
            continue

        file_size = len(data)
        if file_size < 16:
            continue

        try:
            count = _le64(data, file_size - 8)
            if count == 0 or count > 100000:
                continue
        except struct.error:
            continue

        pos = 0
        if file_size >= ENTRY_HEADER_SIZE:
            typ = _le16(data, 0)
            elen = _le32(data, 2)
            if typ == TYPE_VERSION:
                pos = ENTRY_HEADER_SIZE + elen

        index_size = 8 + count * 8 + 8
        index_start = file_size - ENTRY_HEADER_SIZE - index_size

        while pos + ENTRY_HEADER_SIZE <= index_start:
            typ = _le16(data, pos)
            elen = _le32(data, pos + 2)
            value_start = pos + ENTRY_HEADER_SIZE

            if typ == TYPE_COMPRESSED_BLOCK and elen > 0:
                try:
                    compressed = data[value_start:value_start + elen]
                    ssz = _snappy_frame_decode(compressed)
                    ep = _find_execution_payload(ssz)
                    if ep and len(ep) >= 508:
                        bn = _le64(ep, EP_BLOCK_NUMBER)
                        if bn == 0:
                            # Pre-merge Bellatrix blocks have all-zero EP;
                            # skip (matches C era_iter_next behavior).
                            pass
                        elif bn >= start_block:
                            # Deneb+ (EIP-4788): pull parent_beacon_root from
                            # the BeaconBlock itself. Layout inside BeaconBlock:
                            #   slot(8) + proposer(8) + parent_root(32) ...
                            # parent_beacon_root is at offset 16 relative to BB.
                            pbr = None
                            rh = None
                            extra_data_off = _le32(ep, EP_EXTRA_DATA_OFF)
                            is_deneb = (extra_data_off >= 528)
                            if is_deneb:
                                bb_off = _le32(ssz, 0)
                                if bb_off + 48 <= len(ssz):
                                    pbr = bytes(ssz[bb_off + 16:bb_off + 48])
                                # Electra+ (EIP-7685): BeaconBlockBody has
                                # execution_requests as its last variable
                                # field. Compute requests_hash per spec.
                                rh = _compute_requests_hash(ssz, bb_off)
                            yield EraBlock(
                                number=bn,
                                ep=ep,
                                parent_beacon_root=pbr,
                                requests_hash=rh,
                            )
                except Exception:
                    pass

            pos = value_start + elen


def _compute_requests_hash(ssz: bytes, bb_off: int) -> bytes | None:
    """EIP-7685 requests_hash from a Prague+ BeaconBlockBody.

    Layout within BeaconBlockBody (Electra/Prague):
      randao_reveal(96) + eth1_data(72) + graffiti(32)
      + 5 × variable-offset(4)       → bytes 200..220
      + sync_aggregate(160)          → bytes 220..380
      + execution_payload_offset(4)  → bytes 380..384
      + bls_to_execution_changes_off(4)  → 384..388  [Capella+]
      + blob_kzg_commitments_off(4)      → 388..392  [Deneb+]
      + execution_requests_off(4)        → 392..396  [Electra+]

    execution_requests is an SSZ Container of three lists (deposits,
    withdrawals, consolidations). EIP-7685 hashes each type's raw bytes:

        requests_hash = sha256(sha256(r0) || sha256(r1) || sha256(r2))

    where r_i = type_byte || concatenated_request_data_of_type_i.

    Returns None if the beacon body is pre-Electra (no execution_requests
    field), or on any parse error.
    """
    # BeaconBlockBody starts at ssz[bb_off + 80 (body_offset within BB)]
    bb_body_off_loc = bb_off + 80
    if bb_body_off_loc + 4 > len(ssz):
        return None
    body_rel = _le32(ssz, bb_body_off_loc)
    body_start = bb_off + body_rel
    if body_start + 396 > len(ssz):
        # Pre-Electra body fixed part is smaller than 396
        return None

    # proposer_slashings offset = fixed-part size of the body.
    body_fixed = _le32(ssz, body_start + 200)
    if body_fixed < 396:
        return None  # body layout doesn't include execution_requests

    exec_requests_off = _le32(ssz, body_start + 392)
    # Execution requests region extends to end of body; we need the upper
    # bound. The body ends where the next outer structure begins. We use
    # len(ssz) as a conservative upper bound (no siblings after body in
    # SignedBeaconBlock — the signature is BEFORE the message pointer).
    body_end = len(ssz) - bb_off
    if exec_requests_off >= body_end:
        return None

    er_start = body_start + exec_requests_off
    er_end = bb_off + body_end  # bb_off + body_end = len(ssz) for our case
    er_bytes = ssz[er_start:er_end]

    # ExecutionRequests SSZ Container with three lists. Fixed part is
    # 3 × offset(4) = 12 bytes, followed by the three list blobs.
    if len(er_bytes) < 12:
        return None
    off0 = _le32(er_bytes, 0)
    off1 = _le32(er_bytes, 4)
    off2 = _le32(er_bytes, 8)
    if not (12 <= off0 <= off1 <= off2 <= len(er_bytes)):
        return None

    deposits       = er_bytes[off0:off1]
    withdrawals_r  = er_bytes[off1:off2]
    consolidations = er_bytes[off2:]

    # Per EIP-7685, each request r = type_byte || data; the outer hash
    # SKIPS types with empty data (len(r) == 1). We only append the
    # sha256 of r when there's payload after the type byte.
    m = hashlib.sha256()
    for type_byte, data in ((0x00, deposits),
                            (0x01, withdrawals_r),
                            (0x02, consolidations)):
        if len(data) == 0:
            continue
        m.update(hashlib.sha256(bytes([type_byte]) + data).digest())
    return m.digest()


def ep_to_build_header(block: "EraBlock") -> RxBuildHeader:
    """Populate an RxBuildHeader from an EraBlock's ExecutionPayload +
    per-block metadata. Includes all fork-gated fields (base_fee,
    blob_gas, parent_beacon_root, requests_hash) where applicable."""
    ep = block.ep
    hdr = RxBuildHeader()
    ctypes.memset(ctypes.addressof(hdr), 0, ctypes.sizeof(hdr))

    ctypes.memmove(hdr.parent_hash.bytes, ep[EP_PARENT_HASH:EP_PARENT_HASH + 32], 32)
    ctypes.memmove(hdr.coinbase.bytes, ep[EP_FEE_RECIPIENT:EP_FEE_RECIPIENT + 20], 20)
    hdr.number    = _le64(ep, EP_BLOCK_NUMBER)
    hdr.gas_limit = _le64(ep, EP_GAS_LIMIT)
    hdr.timestamp = _le64(ep, EP_TIMESTAMP)

    extra_data_off = _le32(ep, EP_EXTRA_DATA_OFF)
    tx_off = _le32(ep, 504)
    ed_end = tx_off if tx_off > extra_data_off else len(ep)
    ed_len = min(ed_end - extra_data_off, 32)
    if ed_len > 0 and extra_data_off + ed_len <= len(ep):
        ctypes.memmove(hdr.extra_data, ep[extra_data_off:extra_data_off + ed_len], ed_len)
        hdr.extra_data_len = ed_len

    ctypes.memmove(hdr.prev_randao.bytes, ep[EP_PREV_RANDAO:EP_PREV_RANDAO + 32], 32)

    # Base fee (always present post-merge, stored LE in EP, BE in rx_uint256_t)
    base_fee_le = _le256(ep, EP_BASE_FEE)
    hdr.has_base_fee = True
    hdr.base_fee.bytes[:] = base_fee_le.to_bytes(32, 'big')

    is_deneb = (extra_data_off >= 528)
    if is_deneb:
        hdr.has_blob_gas = True
        hdr.blob_gas_used = _le64(ep, 512)
        hdr.excess_blob_gas = _le64(ep, 520)

    if block.parent_beacon_root is not None:
        if len(block.parent_beacon_root) != 32:
            raise ValueError("parent_beacon_root must be 32 bytes")
        hdr.has_parent_beacon_root = True
        ctypes.memmove(hdr.parent_beacon_root.bytes, block.parent_beacon_root, 32)

    if block.requests_hash is not None:
        if len(block.requests_hash) != 32:
            raise ValueError("requests_hash must be 32 bytes")
        hdr.has_requests_hash = True
        ctypes.memmove(hdr.requests_hash.bytes, block.requests_hash, 32)

    return hdr


def ep_extract_txs(ep: bytes) -> list[bytes]:
    """Return each transaction's raw bytes from the EP tx list."""
    extra_data_off = _le32(ep, EP_EXTRA_DATA_OFF)
    tx_off = _le32(ep, 504)
    is_deneb = (extra_data_off >= 528)
    is_capella = (not is_deneb and extra_data_off >= 512)
    wd_off = 0
    if is_capella or is_deneb:
        wd_off = _le32(ep, 508)

    txs = []
    if 0 < tx_off < len(ep):
        tx_end = wd_off if wd_off > tx_off else len(ep)
        tx_region = tx_end - tx_off
        if tx_region >= 4:
            first_off = _le32(ep, tx_off)
            tx_count = first_off // 4
            if 0 < tx_count < 100_000 and first_off <= tx_region:
                for t in range(tx_count):
                    t_start = _le32(ep, tx_off + t * 4)
                    t_end = (_le32(ep, tx_off + (t + 1) * 4)
                             if t + 1 < tx_count else tx_region)
                    if t_start < tx_region and t_end <= tx_region and t_end > t_start:
                        txs.append(bytes(ep[tx_off + t_start:tx_off + t_end]))
    return txs


def ep_extract_withdrawals(ep: bytes) -> list[RxWithdrawal]:
    """Return RxWithdrawal structs for each Shanghai+ withdrawal in EP."""
    extra_data_off = _le32(ep, EP_EXTRA_DATA_OFF)
    is_deneb = (extra_data_off >= 528)
    is_capella = (not is_deneb and extra_data_off >= 512)
    if not (is_capella or is_deneb):
        return []

    wd_off = _le32(ep, 508)
    if not (0 < wd_off < len(ep)):
        return []

    wd_region = len(ep) - wd_off
    wd_count = wd_region // 44
    out = []
    for w in range(wd_count):
        wb = ep[wd_off + w * 44:]
        wd = RxWithdrawal()
        wd.index = _le64(wb, 0)
        wd.validator_index = _le64(wb, 8)
        ctypes.memmove(wd.address.bytes, wb[16:36], 20)
        wd.amount_gwei = _le64(wb, 36)
        out.append(wd)
    return out


def ep_to_header_body(ep: bytes, parent_beacon_root: bytes | None = None):
    """Convert an ExecutionPayload byte string into filled ctypes structs
    ready for rx_execute_block.

    Returns (RxBlockHeader, RxBlockBody, block_hash_bytes).

    `parent_beacon_root` (32 B) must be provided for Deneb+ (Cancun+)
    blocks — it's stored in the BeaconBlock, not the ExecutionPayload.
    See EraBlock.parent_beacon_root and EIP-4788.
    """
    hdr = RxBlockHeader()

    ctypes.memmove(hdr.parent_hash.bytes, ep[EP_PARENT_HASH:EP_PARENT_HASH + 32], 32)
    ctypes.memmove(hdr.coinbase.bytes, ep[EP_FEE_RECIPIENT:EP_FEE_RECIPIENT + 20], 20)
    ctypes.memmove(hdr.state_root.bytes, ep[EP_STATE_ROOT:EP_STATE_ROOT + 32], 32)
    ctypes.memmove(hdr.receipt_root.bytes, ep[EP_RECEIPTS_ROOT:EP_RECEIPTS_ROOT + 32], 32)
    ctypes.memmove(hdr.logs_bloom, ep[EP_LOGS_BLOOM:EP_LOGS_BLOOM + 256], 256)
    ctypes.memmove(hdr.mix_hash.bytes, ep[EP_PREV_RANDAO:EP_PREV_RANDAO + 32], 32)

    hdr.number = _le64(ep, EP_BLOCK_NUMBER)
    hdr.gas_limit = _le64(ep, EP_GAS_LIMIT)
    hdr.gas_used = _le64(ep, EP_GAS_USED)
    hdr.timestamp = _le64(ep, EP_TIMESTAMP)
    hdr.nonce = 0

    base_fee = _le256(ep, EP_BASE_FEE)
    hdr.has_base_fee = True
    hdr.base_fee.bytes[:] = base_fee.to_bytes(32, 'big')

    extra_data_off = _le32(ep, EP_EXTRA_DATA_OFF)
    tx_off = _le32(ep, 504)
    ed_end = tx_off if tx_off > extra_data_off else len(ep)
    ed_len = min(ed_end - extra_data_off, 32)
    if ed_len > 0 and extra_data_off + ed_len <= len(ep):
        ctypes.memmove(hdr.extra_data, ep[extra_data_off:extra_data_off + ed_len], ed_len)
        hdr.extra_data_len = ed_len

    is_deneb = (extra_data_off >= 528)
    is_capella = (not is_deneb and extra_data_off >= 512)

    # Capella+: withdrawals_root flag on header (value recomputed from body).
    if is_capella or is_deneb:
        hdr.has_withdrawals_root = True

    if is_deneb:
        hdr.has_blob_gas = True
        hdr.blob_gas_used = _le64(ep, 512)
        hdr.excess_blob_gas = _le64(ep, 520)
        # EIP-4788: parent_beacon_root lives in the BeaconBlock, not the EP.
        # Caller must supply it (iter_era_blocks yields EraBlock with it).
        if parent_beacon_root is not None:
            if len(parent_beacon_root) != 32:
                raise ValueError(
                    f"parent_beacon_root must be 32 B, got {len(parent_beacon_root)}")
            hdr.has_parent_beacon_root = True
            ctypes.memmove(hdr.parent_beacon_root.bytes, parent_beacon_root, 32)

    # Block hash (post-execution, embedded in ExecutionPayload)
    block_hash = bytes(ep[EP_BLOCK_HASH:EP_BLOCK_HASH + 32])

    # Transactions
    wd_off = 0
    if is_capella or is_deneb:
        wd_off = _le32(ep, 508)

    txs_raw = []
    if 0 < tx_off < len(ep):
        tx_end = wd_off if wd_off > tx_off else len(ep)
        tx_region = tx_end - tx_off
        if tx_region >= 4:
            first_off = _le32(ep, tx_off)
            tx_count = first_off // 4
            if 0 < tx_count < 100_000 and first_off <= tx_region:
                for t in range(tx_count):
                    t_start = _le32(ep, tx_off + t * 4)
                    t_end = _le32(ep, tx_off + (t + 1) * 4) if t + 1 < tx_count else tx_region
                    if t_start < tx_region and t_end <= tx_region and t_end > t_start:
                        txs_raw.append(bytes(ep[tx_off + t_start:tx_off + t_end]))

    body = RxBlockBody()
    body.tx_count = len(txs_raw)

    # Allocate C-array-of-pointers for txs
    if txs_raw:
        TxPtrArray = (ctypes.POINTER(ctypes.c_uint8) * len(txs_raw))
        LenArray = (ctypes.c_size_t * len(txs_raw))
        tx_ptrs = TxPtrArray()
        tx_lens = LenArray()
        tx_bufs = []
        for i, raw in enumerate(txs_raw):
            buf = (ctypes.c_uint8 * len(raw))(*raw)
            tx_bufs.append(buf)
            tx_ptrs[i] = ctypes.cast(buf, ctypes.POINTER(ctypes.c_uint8))
            tx_lens[i] = len(raw)
        body.transactions = ctypes.cast(tx_ptrs, ctypes.POINTER(ctypes.POINTER(ctypes.c_uint8)))
        body.tx_lengths = ctypes.cast(tx_lens, ctypes.POINTER(ctypes.c_size_t))
        body._tx_bufs = tx_bufs  # keep refs alive
        body._tx_ptrs = tx_ptrs
        body._tx_lens = tx_lens

    # Withdrawals (Shanghai+)
    withdrawals = []
    if (is_capella or is_deneb) and 0 < wd_off < len(ep):
        wd_region = len(ep) - wd_off
        wd_count = wd_region // 44
        for w in range(wd_count):
            wb = ep[wd_off + w * 44:]
            wd = RxWithdrawal()
            wd.index = _le64(wb, 0)
            wd.validator_index = _le64(wb, 8)
            ctypes.memmove(wd.address.bytes, wb[16:36], 20)
            wd.amount_gwei = _le64(wb, 36)
            withdrawals.append(wd)

    if withdrawals:
        WdArray = (RxWithdrawal * len(withdrawals))
        wd_arr = WdArray(*withdrawals)
        body.withdrawals = ctypes.cast(wd_arr, ctypes.POINTER(RxWithdrawal))
        body.withdrawal_count = len(withdrawals)
        body._wd_arr = wd_arr  # keep ref

    return hdr, body, block_hash


__all__ = [
    "iter_era_blocks", "ep_to_header_body", "EraBlock",
    "ep_to_build_header", "ep_extract_txs", "ep_extract_withdrawals",
    "EP_BLOCK_HASH", "EP_STATE_ROOT", "EP_RECEIPTS_ROOT",
    "EP_LOGS_BLOOM", "EP_GAS_USED",
]
