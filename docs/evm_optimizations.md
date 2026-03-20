# EVM & State Layer Optimization Opportunities

Identified by comparing artex with evmone and geth. Ranked by expected
impact on mainnet chain replay throughput.

## EVM Interpreter

### 1. Merge warm check into ensure_slot [HIGH — 5-10%]

SLOAD/SSTORE do two ART lookups with the same 52-byte key:
1. `evm_is_storage_warm()` → `make_slot_key` + `mem_art_contains`
2. `evm_state_get_storage()` → `ensure_slot` → `make_slot_key` + `mem_art_get_mut`

Fix: add `bool warm` to `cached_slot_t`. First access marks warm and charges
cold gas. Subsequent accesses see `cs->warm == true`. One ART lookup instead
of two. This fires on every SLOAD/SSTORE — the most frequent state ops.

Files: `evm/src/opcodes/storage.c`, `evm/src/evm_state.c`

### 2. Cache JUMPDEST bitmap by code_hash [HIGH — 3-8%]

Every `evm_interpret` call does `calloc` + linear scan + `free` for the
JUMPDEST bitmap. Nested CALLs to the same contract (Uniswap router, WETH)
rebuild the same bitmap repeatedly.

Fix: add a `jumpdest_cache` (hash map: code_hash → bitmap) to evm_t or
evm_state. Evmone caches analysis per code_hash. Geth caches JumpdestMap
similarly. For hot contracts called hundreds of times per block, this
avoids O(code_size) work per nested call.

Files: `evm/src/interpreter.c`, `evm/src/opcodes/control.c`

### 3. Short-circuit CALL to EOA [HIGH — 2-5%]

When CALL targets an account with no code and it's not a precompile,
we still set up a full evm_interpret context (allocate jumpdest bitmap,
etc.) only for it to return immediately. Geth skips the EVM entirely
for codeless targets.

Fix: after gas charging in op_call, if `code_size == 0 && !is_precompile`,
do the value transfer and return success directly. Skip `evm_interpret`.
Common for simple ETH transfers within contract execution.

Files: `evm/src/opcodes/call.c`

### 4. Remove verkle branch from DISPATCH [MED — 1-3%]

The DISPATCH macro always tests `verkle_chunk_mode` on every instruction
fetch, even when verkle is disabled. While marked `__builtin_expect(0)`,
it's still a branch in the hottest loop.

Fix: compile two versions of `evm_interpret` — one with verkle gas, one
without — selected at call time. Or use `#ifdef ENABLE_VERKLE` to compile
out the check entirely for non-verkle builds.

Files: `evm/src/interpreter.c` (DISPATCH macro, lines 115-137)

### 5. Resolve fork gas constants once per block [MED — 1-2%]

SLOAD/SSTORE evaluate a 5-branch if-else chain on `evm->fork` on every
execution to select the gas cost. Fork is constant for the entire block.

Fix: resolve gas constants (sload_gas, cold_sload_cost, etc.) once in
`evm_set_block_env` or `sync_execute_block`, store in evm_t. Opcode
handlers read the pre-resolved value directly.

Files: `evm/src/opcodes/storage.c`, `evm/include/gas.h`

### 6. SIMD Keccak (XKCP AVX2) [MED — 1-3%]

Current keccak is portable C. Geth uses assembly keccak-f[1600] for amd64.
XKCP provides AVX2 lane-interleaved implementations that are 2-3x faster
on Zen3+.

Files: `common/src/keccak256.c`

## State Access Layer

### 7. Partial cache eviction [HIGH — 10-20%]

`evm_state_evict_cache` destroys both ART trees completely every 256
blocks. The next 256 blocks start fully cold — every account and storage
slot must be re-loaded from MPT (5-10 disk_hash lookups per access).

Geth keeps accounts in an LRU across blocks. Only evicts least-recently-used
entries when memory pressure exceeds a threshold.

Fix: instead of destroying the ARTs, iterate and remove entries not accessed
in the last checkpoint window. Or keep the top K accounts/slots by access
count. Even keeping the top 1000 hot contracts (WETH, Uniswap, major tokens)
across checkpoints would eliminate most re-loads.

Files: `evm/src/evm_state.c` (evm_state_evict_cache), `sync/src/sync.c`

### 8. Skip rwlock in disk_hash [HIGH — 3-5%]

`disk_hash_get` takes `pthread_rwlock_rdlock` on every call. Chain replay
is single-threaded — the lock overhead is pure waste. Each acquire/release
is ~20-40ns; at 5 nodes per trie walk and millions of state accesses per
checkpoint window, this adds up.

Fix: add `disk_hash_set_single_threaded(dh)` that sets a flag bypassing
rwlock. Or provide `disk_hash_get_nolock` / `disk_hash_put_nolock` variants.

Files: `database/src/disk_hash_mmap.c`

### 9. Hot node cache in mpt_store [HIGH — 5-10%]

Every `mpt_store_get` walks 5-10 trie nodes via `load_node_rlp`, each
doing a `disk_hash_get` (rwlock + mmap page access). The root node and
top 2-3 branch levels are accessed on every single state lookup.

Geth uses `fastcache` + `triedb.cleans` LRU for hot trie nodes. Our
code relies entirely on OS page cache, which is less deterministic.

Fix: add a small in-memory LRU (8-16 MB, ~64K entries) in mpt_store for
recently accessed nodes. Would eliminate ~60% of disk_hash lookups since
the top trie levels are shared across all paths.

Files: `database/src/mpt_store.c`

### 10. Bloom filter for non-existent storage slots [MED — 2-5%]

Many SLOAD calls read slots that don't exist (return zero). This triggers
a full 5-10 node trie walk that finds nothing. A bloom filter on the
storage trie could short-circuit these.

The bloom filter already exists in mpt_store but is only used during writes
(dedup in `write_node`). Repurpose it for read-path filtering: if bloom
says "definitely not present", skip the trie walk.

Files: `database/src/mpt_store.c`

### 11. Batch/prefetch disk_hash during trie walks [MED — 2-3%]

`mpt_store_get` does 5-10 sequential `disk_hash_get` calls. The trie path
is deterministic from the key hash. Could prefetch likely bucket pages or
use `disk_hash_batch_get` (already implemented in disk_hash_mmap.c).

Files: `database/src/mpt_store.c`, `database/src/disk_hash_mmap.c`

## AVX-512 / SIMD Optimizations

Target: AMD Ryzen 9 9950X (Zen5) — full AVX-512 including IFMA.

### 12. Keccak-256 AVX-512 [HIGH — 2-4x hashing speedup]

Current keccak is portable C. Keccak-f[1600] state is 200 bytes — fits
in 4 zmm registers. XKCP provides ready-to-use AVX-512 implementation
with 4-way lane-interleaved permutation.

Fires on every: SLOAD key derivation (`keccak(addr)`, `keccak(slot)`),
MPT node hash, SHA3 opcode, CREATE2 address, account RLP hashing during
`compute_mpt_root`.

Can also use AVX2 fallback for older hardware. Runtime CPUID detection
to select the best path.

Files: `common/src/keccak256.c`, `common/include/keccak256.h`

### 13. Pedersen/Banderwagon with AVX-512 IFMA [HIGH — Verkle only]

`VPMADD52LUQ`/`VPMADD52HUQ` instructions do 52-bit integer multiply-
accumulate — exactly what field multiplication needs. The Banderwagon
prime field (253-bit) maps to 5x52-bit limbs.

Benefits:
- Field mul: single IFMA chain replaces 10+ scalar MUL+ADD
- Multi-scalar multiplication (MSM) for Verkle commits: 2-4x speedup
- IPA proof computation: dominated by field ops

Not needed until Verkle goes live, but the 9950X has `avx512ifma` ready.

Files: `verkle/src/banderwagon.c`, `verkle/src/pedersen.c`

### 14. ART node scanning with AVX-512 [LOW — minor]

`mem_art` Node16 already uses SSE2 (16-byte key compare). AVX-512 could
scan Node48/Node256 (48-256 byte key arrays) in 1-4 loads instead of
loops. Minor win since ART lookups are already fast.

Files: `common/src/mem_art.c`

### Notes on uint256

Current `uint256_t` = two `__uint128_t`. The compiler already generates
optimal `add`+`adc` for addition and schoolbook multiplication with
4 partial 64x64→128 products. AVX-512 wouldn't help here — scalar
carry-chain arithmetic is inherently sequential. Not worth vectorizing
unless profiling shows uint256 as a bottleneck.

## Quick Reference: Implementation Order

For maximum impact with minimum risk during active chain replay:

1. Skip rwlock (LOW effort, safe, 3-5%)
2. Short-circuit CALL to EOA (LOW effort, safe, 2-5%)
3. Merge warm check into ensure_slot (MEDIUM effort, 5-10%)
4. Partial cache eviction (MEDIUM effort, 10-20%)
5. JUMPDEST cache (MEDIUM effort, 3-8%)
6. Everything else after profiling
