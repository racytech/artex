# Execution Flow — Chain Sync

## Overview

During chain sync, blocks are executed in batches of 256 (CHECKPOINT_INTERVAL).
State roots are validated against block headers, and checkpoints persist
progress to disk for crash recovery.

## Current Flow

```
chain_replay(era1_dir, genesis, start, end)
│
├── Load genesis (8893 accounts)
│   ├── evm_state_create(vs, mpt_path)
│   ├── for each genesis account:
│   │   └── create_account + set_nonce + set_balance + set_code + set_storage
│   ├── evm_state_commit()          ← sets existed=true, clears dirty
│   ├── evm_state_finalize()        ← flush to verkle (or no-op)
│   └── evm_state_compute_state_root_ex()  ← clear block_dirty
│
├── Resume from checkpoint (if exists)
│   └── restore block_hashes ring buffer, counters
│
└── for each block [start..end]:
    │
    ├── 1. READ: era1_read_block → header RLP + body RLP
    │
    ├── 2. DECODE: header + body (txs, uncles, withdrawals)
    │
    ├── 3. EXECUTE: block_execute(evm, header, body, block_hashes)
    │   │
    │   ├── evm_state_begin_block(state, block_num)
    │   │   ├── [VERKLE] verkle_state_begin_block()
    │   │   └── [VERKLE] witness_gas_reset()
    │   │
    │   ├── for each transaction:
    │   │   ├── decode tx (legacy / EIP-2930 / EIP-1559 / EIP-4844 / EIP-7702)
    │   │   ├── validate signature, nonce, balance
    │   │   ├── EVM execution
    │   │   │   └── opcodes modify state via evm_state_*()
    │   │   │       each setter: dirty=true, block_dirty=true, mpt_dirty=true
    │   │   │       storage setters also: storage_dirty=true
    │   │   └── evm_state_commit_tx()
    │   │       ├── process self-destructs (zero account, clear storage)
    │   │       ├── promote non-empty dirty accounts to existed=true
    │   │       ├── clear journal, warm sets, transient storage
    │   │       └── [VERKLE] witness_gas_reset()
    │   │       (mpt_dirty, block_dirty, storage_dirty SURVIVE commit_tx)
    │   │
    │   ├── block rewards (PoW only, pre-Merge)
    │   │   ├── coinbase += base_reward + uncle_bonus
    │   │   └── uncle miners += uncle_reward
    │   │   (NO commit_tx after rewards — dirty flags set but not committed)
    │   │
    │   ├── withdrawals (Shanghai+, EIP-4895)
    │   │   └── add_balance per withdrawal
    │   │
    │   ├── system calls (Prague+)
    │   │   ├── EIP-7002 withdrawal requests
    │   │   └── EIP-7251 consolidation requests
    │   │
    │   ├── evm_state_finalize()
    │   │   ├── [VERKLE] flush dirty accounts/storage to verkle_state
    │   │   └── [!VERKLE] no-op (return true)
    │   │
    │   └── evm_state_compute_state_root_ex(state, prune_empty)
    │       ├── [VERKLE] flush block_dirty → verkle_state
    │       │   verkle_state_commit_block()
    │       │   verkle_state_root_hash() → verkle root
    │       ├── [!VERKLE] promote_block_dirty_cb → set existed=true
    │       └── clear block_dirty on all accounts + slots
    │
    ├── 4. VALIDATE GAS: result.gas_used == header.gas_used
    │
    ├── 5. VALIDATE STATE ROOT (conditional):
    │   │  check_root = (bn % 256 == 0 || has_txs || has_uncles || last_block)
    │   │
    │   └── [MPT] evm_state_compute_mpt_root(state, prune_empty)
    │       ├── compute_all_storage_roots()  ← incremental per dirty account
    │       ├── update account trie           ← incremental for mpt_dirty accounts
    │       ├── mpt_store_root() → state_root
    │       ├── verify: state_root == header.state_root
    │       └── clear mpt_dirty flags
    │
    ├── 6. ON FAILURE:
    │   └── print error, increment blocks_fail, continue executing (!)
    │       (state is now wrong for all subsequent blocks)
    │
    └── 7. CHECKPOINT (every 256 blocks, if blocks_fail == 0):
        ├── [VERKLE] verkle_state_sync()
        └── checkpoint_save(block_num, hashes, counters)
```

## Dirty Flag Lifecycle

Three independent sets of dirty flags, consumed by different paths:

```
                 set by              survives       cleared by
                 mutation ops        commit_tx?
─────────────────────────────────────────────────────────────────
block_dirty      any field change    YES            compute_state_root_ex
block_code_dirty code change         YES            compute_state_root_ex
mpt_dirty        any field change    YES            compute_mpt_root
storage_dirty    storage/create      YES            compute_mpt_root (after storage roots)
dirty            any field change    NO             commit_tx
code_dirty       code change         NO             commit_tx
created          create_account      NO             commit_tx
```

- `dirty` / `code_dirty` / `created` — per-transaction scope, cleared by commit_tx
- `block_dirty` / `block_code_dirty` — per-block scope, consumed by verkle flush
- `mpt_dirty` / `storage_dirty` — cross-block scope, consumed by MPT root computation

## Backend Separation (ENABLE_MPT / ENABLE_VERKLE)

```
Build Flag         Default    Controls
─────────────────────────────────────────────────────────────────
ENABLE_MPT         ON         mpt_store linking, compute_mpt_root(),
                              mpt_dirty/storage_dirty consumption
ENABLE_VERKLE      OFF        verkle_state linking, finalize flush,
                              compute_state_root_ex verkle path,
                              witness gas, begin_block verkle init
```

Both can be ON. Both OFF → CMake fatal error.
If ENABLE_VERKLE=OFF and fork reaches Verkle → planned FATAL at runtime.

Always linked regardless of flags: verkle_key, pedersen, banderwagon
(needed by EVM opcodes for runtime fork >= FORK_VERKLE checks).

## Known Issues

1. **Root validation is conditional** — only checks on blocks with txs/uncles
   or every 256 blocks. With incremental MPT this could be every block.

2. **No rollback on failure** — if a block fails validation, execution
   continues with corrupted state. Should revert to last checkpoint.

3. **Two separate root computations** — compute_state_root_ex (verkle, every
   block inside block_execute) and compute_mpt_root (MPT, conditional in
   chain_replay). These are independent paths with independent dirty flags.

4. **Rewards not committed** — miner/uncle rewards and withdrawals happen
   after the last commit_tx, so their dirty/created flags are only picked up
   by the block-end root computation, not by commit_tx's existed promotion.
   Fixed by promote_block_dirty_cb (non-VERKLE) and flush_all_accounts_cb
   (VERKLE).

## Disk Layout

```
/tmp/chain_replay_mpt.idx          account trie index (disk_hash)
/tmp/chain_replay_mpt.dat          account trie nodes (append-only RLP)
/tmp/chain_replay_mpt_storage.idx  storage tries index (shared, all accounts)
/tmp/chain_replay_mpt_storage.dat  storage tries nodes (shared, append-only)
/tmp/chain_replay.ckpt             checkpoint file (block num + hashes + counters)
```
