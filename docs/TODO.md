# Optimization TODO

Bigger changes that need discussion before implementation.

## 1. ~~Free List Cap (502 slots per size class)~~ — DONE

Overflow free offsets now persisted to `.free` sidecar file. No more
silent slot loss. Compaction cleans up `.free` after reclaiming.
Committed: `5f34d2d`

---

## 2. ~~Block-Level Account Prefetch~~ — DONE

Lookahead prefetch: decode next tx and prefetch sender/receiver ART
paths while current tx executes. Added `mem_art_prefetch` and
`evm_state_prefetch_account`. Committed: `e4ea4f5`

---

## 3. ~~Journal Arena Allocation~~ — SKIPPED

Journal is already a flat dynamic array (`journal_entry_t *journal`,
grown via `realloc`). Each `journal_push` copies a stack-local struct
into the array — no per-entry malloc. Essentially already an arena pattern.

---

## 4. ~~mmap for Read-Through~~ — SKIPPED → Depth-Pinned Cache Eviction (DONE)

mmap degrades badly when .dat files exceed RAM (~100+ GB at scale).
LRU cache + pread is the right architecture — but plain LRU lets leaf
bursts evict hot upper-branch nodes.

**Replaced with:** depth-pinned eviction — nodes at trie depth ≤ 4 are
never evicted from the LRU cache. Pinned set is ~70K nodes (~70 MB),
negligible vs 2 GB cache. Upper branch nodes stay hot across blocks.

---

## 5. io_uring for Flush

**Problem:** Flush does N individual `pwrite()` syscalls (now sorted by
offset, but still one syscall each). Each syscall has ~200ns overhead.

**Approach:** Batch all writes into a single io_uring submission queue.
Submit once, reap completions. Already partially implemented
(`USE_IO_URING` flag exists).

**Impact:** Medium — ~200ns × 1000 nodes = ~200μs saved per flush.
More impactful at tip where flush count and node count are higher.

**Complexity:** Medium — io_uring API, error handling, fallback for
older kernels. Ring setup/teardown lifecycle.

---

## 6. Batch Keccak

**Problem:** Each trie node is hashed individually. Keccak's internal
state setup is amortized poorly for small inputs (32-64 byte nodes).

**Approach:** Collect multiple nodes to hash, process in pipeline.
The fused permutation with precomputed round constants (already done)
is the baseline — batching adds input-level parallelism.

**Impact:** Low-Medium — keccak is already well-optimized. Batching
helps most during commit_batch where thousands of nodes are hashed.

**Complexity:** Medium — requires restructuring commit_batch to
collect nodes before hashing rather than hash-as-you-go.

---

## 7. Opcode Fusion (Super-Instructions)

**Problem:** Even with computed gotos, each opcode dispatch has overhead:
load PC, load opcode, jump. Common sequences like PUSH1+ADD execute as
two separate dispatches.

**Approach:** During jump analysis pass (already done for JUMPDEST bitmap),
detect common opcode pairs/triples and replace with fused super-instructions:
- PUSH1 + ADD → PUSH1_ADD
- PUSH1 + MSTORE → PUSH1_MSTORE
- DUP1 + PUSH1 + EQ → DUP1_PUSH1_EQ
- PUSH1 + SLOAD → PUSH1_SLOAD

**Impact:** Medium — eliminates dispatch overhead for ~30-40% of opcode
sequences (based on mainnet bytecode analysis). Most valuable for tight
loops in hot contracts.

**Complexity:** High — requires bytecode rewriting pass, extended dispatch
table, careful gas accounting per fused instruction.

---

## 8. Stack Caching (Register Allocation)

**Problem:** All opcodes read/write stack values through memory
(`evm->stack->data[sp]`). Most opcodes only touch the top 1-3 elements.

**Approach:** Keep top 2-3 stack values in local variables (registers).
Spill to memory only when stack depth changes beyond the cached window.
Computed goto dispatch already keeps these in registers across jumps.

**Impact:** Medium — eliminates memory load/store for top-of-stack
operations. Most impactful for arithmetic-heavy code (ADD, MUL, LT, GT).

**Complexity:** High — every opcode handler needs to manage the register
cache. Stack underflow/overflow checks become more complex.
