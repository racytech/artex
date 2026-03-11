# Engine API — Production Readiness TODO

## P0: Correctness (blocks will be rejected or wrongly accepted without these)

### Log Capture Pipeline
- [x] Implement log capture in LOG0-LOG4 opcodes (`evm/src/opcodes/logging.c`)
  - `evm_log_t` struct (address, topics[4], topic_count, data, data_len) in `evm/include/evm.h`
  - Log accumulator on `evm_t` (logs, log_count, log_cap)
  - Copy contract address, memory data, and topics into log entry
  - On REVERT: truncate logs since snapshot (subcalls + CREATE/CREATE2)
- [x] Add `evm_log_t *logs` + `log_count` fields to `transaction_result_t` (`evm/include/transaction.h`)
  - Zero-copy ownership transfer from EVM accumulator after `transaction_execute()`
- [x] Extend `tx_receipt_t` (`executor/include/block_executor.h`)
  - `tx_type` (uint8_t) for typed receipt RLP envelope (EIP-2718)
  - `status_code` (uint8_t) — 0=fail, 1=success (post-Byzantium)
  - `logs_bloom[256]` — per-tx 2048-bit bloom filter (big-endian)
  - `evm_log_t *logs` + `log_count`
  - `receipt_encode_rlp()` in `executor/src/block_executor.c`
- [x] Add computed fields to `block_result_t` (`executor/include/block_executor.h`)
  - `receipt_root` (hash_t) — MPT root of RLP-encoded receipts
  - `logs_bloom[256]` — aggregate bloom = OR of all per-tx blooms
- [x] Fix `mpt_compute_root_unsecured()` leaf_hash overflow for large receipt values (`database/src/mem_mpt.c`)

### Payload Validation
- [x] Validate `receiptsRoot` from payload against computed receipt root (engine test runner)
- [x] Validate `logsBloom` from payload against computed block bloom (engine test runner)
- [x] Wire receipt_root/logs_bloom validation into `newPayload` handler (`engine/src/engine_handlers.c`)
- [x] Validate `expectedBlobVersionedHashes` (V3+) from params[1] against blob tx hashes (`engine/src/engine_handlers.c`)
  - Decode type-3 txs from payload, collect blob hashes, compare with params[1]
- [x] Check `engine_store_put()` return value at all 4 call sites (`engine/src/engine_handlers.c`)
  - 3 sites return error, 1 (invalid payload cache) logs warning

### Block Executor
- [x] Validate uncle depth before reward arithmetic (`executor/src/block_executor.c`)
  - Skip uncles where `uncle_hdr.number >= header->number` or depth > 7

## P1: Sync Mode (receive blocks from CL)

### Engine Configuration
- [ ] Add `chain_id`, `fork_schedule`, `genesis_hash` to `engine_config_t` (`engine/include/engine.h`)
  - Fork schedule determines which Engine API version rules apply per block
  - Genesis hash needed to seed canonical chain in the store

### State Persistence
- [ ] Persist fork choice state (head/safe/finalized hashes) across restarts
  - Currently all state is in-memory (`engine/src/engine_store.c`); lost on process exit
  - On restart, engine responds SYNCING to all forkchoiceUpdated until CL re-submits
- [ ] Persist blockhash ring buffer or reconstruct from block DB on startup

## P1.5: Block Production (propose blocks as validator — future work)

### Transaction Pool
- [ ] Implement txpool module (`txpool/`)
  - `eth_sendRawTransaction` JSON-RPC handler to accept transactions
  - Per-sender nonce queue (reject gaps, buffer future nonces)
  - Price-sorted index (effective priority fee, descending)
  - Eviction policy (drop lowest-paying txs when pool is full)
  - No formal spec — this is an implementation detail, not consensus-critical
- [ ] Transaction selection: `txpool_select(gas_limit)` → ordered list of txs
  - Sort by effective priority fee (descending)
  - Respect per-sender nonce ordering (nonce N before nonce N+1)
  - Greedy packing up to block gas limit
  - Skip txs that exceed remaining gas

### Payload Building (getPayload)
- [ ] Implement block building in `forkchoiceUpdated` when `payloadAttributes` is non-null
  - Select transactions from txpool
  - Execute them, compute all roots, assemble `execution_payload_t`
  - Call `engine_store_set_pending()` with the built payload
- [ ] Currently `engine_store_set_pending()` is never called — `getPayload` always returns `-38001 unknown payload`
- [ ] Compute correct `blockValue` in `getPayload` response (`engine/src/engine_handlers.c:634`)
  - Must be sum of `effective_gas_price * gas_used` for each tx (tip to fee recipient)
  - Currently hardcoded to `"0x0"`
- [ ] Populate `executionRequests` in `getPayloadV4` response (`engine/src/engine_handlers.c:647`)
  - Capture return data from EIP-6110/EIP-7002/EIP-7251 system calls in `block_executor.c`
  - Currently the system call results are discarded (`evm_result_free()` immediately)

### Payload ID Generation
- [ ] Replace `timestamp ^ 0xDEADBEEF` with proper unique ID (`engine/src/engine_handlers.c:545`)
  - Should incorporate: timestamp, prevRandao, fee_recipient, withdrawals hash
  - Two payloads with same timestamp but different attributes currently collide

## P2: Robustness (won't crash a production node but will cause issues under load)

### HTTP Server
- [x] Add concurrency support to HTTP server (`engine/src/engine_http.c`)
  - Worker thread dispatches handler calls, accept loop stays responsive
  - 30-second timeout on worker — returns 503 on timeout
- [x] Check `write()` return values (`engine/src/engine_http.c`)
  - `write_all()` helper retries short writes, handles EPIPE/ECONNRESET
- [x] Add connection timeout
  - SO_RCVTIMEO/SO_SNDTIMEO set to 30s after accept()
- [ ] Add request rate limiting or max concurrent connections

### Store Capacity
- [x] Handle store-full condition gracefully (`engine/src/engine_store.c`)
  - `evict_oldest()` removes oldest non-protected (non head/safe/finalized) block
  - `engine_store_put()` auto-evicts on full, logs if all blocks protected
- [x] Check `engine_store_put()` return value at all 4 call sites (`engine/src/engine_handlers.c`)
  - On failure, return INTERNAL_ERROR JSON-RPC response

### Memory Management
- [x] Audit payload ownership model (`engine/src/engine_handlers.c`)
  - `engine_store_put()` now deep-copies via `execution_payload_deep_copy()`
  - Callers always free their own payload — no ownership transfer ambiguity

### JWT Authentication
- [x] Replace `memmem()` with proper JSON parsing (`engine/src/engine_jwt.c`)
  - C99-portable `json_find_key()` scans for top-level keys, skips nested strings
- [x] Increase JWT buffers (`engine/src/engine_jwt.c`)
  - Header: 256 → 512, Payload: 512 → 1024

## P3: Nice to Have

- [ ] Structured logging with levels (currently fprintf to stderr)
- [ ] Metrics (block execution time, payload count, validation failures)
- [ ] Health check endpoint (GET /)
- [ ] Graceful shutdown (drain in-flight requests before closing)
- [ ] EIP-7685 request validation in `newPayloadV4` (validate request type ordering, etc.)
