# Engine API — Production Readiness TODO

## P0: Correctness (blocks will be rejected or wrongly accepted without these)

### Log Capture Pipeline
- [ ] Implement log capture in LOG0-LOG4 opcodes (`evm/src/opcodes/logging.c`)
  - Define `log_t` struct (address, topics[0..4], topic_count, data, data_len)
  - Add log accumulator to `evm_t` or transaction context
  - Copy contract address, memory data, and topics into log entry
  - On REVERT: discard logs since snapshot (depth-aware)
- [ ] Add `log_t *logs` + `log_count` fields to `transaction_result_t` (`evm/include/transaction.h`)
  - Populate from EVM log accumulator after each `transaction_execute()`
- [ ] Extend `tx_receipt_t` (`executor/include/block_executor.h`)
  - Add `tx_type` (uint8_t) for typed receipt RLP envelope (EIP-2718)
  - Add `status_code` (uint8_t) — 0=fail, 1=success (post-Byzantium)
  - Add `logs_bloom[256]` — per-tx 2048-bit bloom filter
  - Add `log_t *logs` + `log_count`
  - Add `receipt_encode_rlp()` function
- [ ] Add computed fields to `block_result_t` (`executor/include/block_executor.h`)
  - `receipt_root` (hash_t) — MPT root of RLP-encoded receipts
  - `logs_bloom[256]` — aggregate bloom = OR of all per-tx blooms

### Payload Validation
- [ ] Validate `receiptsRoot` from payload against computed receipt root (`engine/src/engine_handlers.c`)
- [ ] Validate `logsBloom` from payload against computed block bloom (`engine/src/engine_handlers.c`)
- [ ] Validate `expectedBlobVersionedHashes` (V3+) from params[1] against blob tx hashes (`engine/src/engine_handlers.c:333`)
- [ ] Check `engine_store_put()` return value at all 4 call sites (`engine/src/engine_handlers.c:343,434,445,453`)
  - On failure (store full), return appropriate error instead of silently dropping the block

### Block Executor
- [ ] Validate uncle depth before reward arithmetic (`executor/src/block_executor.c:326`)
  - `uncle_hdr.number + 8 - header->number` can underflow if uncle is stale beyond 7 generations
  - Add check: `uncle_hdr.number + 7 >= header->number` before computing reward

## P1: Functional Gaps (features that must work for a real node)

### Payload Building (getPayload)
- [ ] Implement block building in `forkchoiceUpdated` when `payloadAttributes` is non-null
  - Select transactions from mempool (requires mempool/txpool module)
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

### Engine Configuration
- [ ] Add `chain_id`, `fork_schedule`, `genesis_hash` to `engine_config_t` (`engine/include/engine.h`)
  - Fork schedule determines which Engine API version rules apply per block
  - Genesis hash needed to seed canonical chain in the store

### State Persistence
- [ ] Persist fork choice state (head/safe/finalized hashes) across restarts
  - Currently all state is in-memory (`engine/src/engine_store.c`); lost on process exit
  - On restart, engine responds SYNCING to all forkchoiceUpdated until CL re-submits
- [ ] Persist blockhash ring buffer or reconstruct from block DB on startup

## P2: Robustness (won't crash a production node but will cause issues under load)

### HTTP Server
- [ ] Add concurrency support to HTTP server (`engine/src/engine_http.c`)
  - Currently single-threaded, one-request-at-a-time
  - Slow block execution blocks all other Engine API calls (CL will timeout)
  - Options: thread pool, async I/O, or at minimum a worker thread for execution
- [ ] Check `write()` return values (`engine/src/engine_http.c:156-158`)
  - Partial writes or broken pipe silently truncate responses
  - CL receives malformed JSON-RPC and disconnects
- [ ] Add connection timeout
  - A slow/malicious client holding the connection blocks the entire server
- [ ] Add request rate limiting or max concurrent connections

### Store Capacity
- [ ] Handle store-full condition gracefully (`engine/src/engine_store.c:66-74`)
  - Currently `find_free_slot()` returns -1 and block is silently dropped
  - Consider: evict oldest non-finalized block, or grow the store dynamically
  - `ENGINE_STORE_MAX_BLOCKS` is hardcoded to 1024

### Memory Management
- [ ] Audit payload ownership model (`engine/src/engine_handlers.c`)
  - `engine_store_put()` does shallow copy — takes ownership of heap pointers
  - Fragile: any new error path risks double-free or leak
  - Consider: deep copy in store_put so caller always frees, or explicit ownership transfer

### JWT Authentication
- [ ] Replace `memmem()` with proper JSON parsing (`engine/src/engine_jwt.c:208,226`)
  - `memmem()` is a GNU extension (not C99/C11), breaks on non-glibc systems
  - Naive pattern matching could match inside string values
- [ ] Increase JWT header buffer from 256 bytes (`engine/src/engine_jwt.c:286`)
  - JWTs with extra claims or whitespace will be rejected

## P3: Nice to Have

- [ ] Structured logging with levels (currently fprintf to stderr)
- [ ] Metrics (block execution time, payload count, validation failures)
- [ ] Health check endpoint (GET /)
- [ ] Graceful shutdown (drain in-flight requests before closing)
- [ ] EIP-7685 request validation in `newPayloadV4` (validate request type ordering, etc.)
