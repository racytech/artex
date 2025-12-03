# ART Database Development Plan

## Overview

Building a memory-efficient, high-performance Adaptive Radix Trie (ART) as the primary database for Ethereum state management. The ART serves as both a general-purpose ordered key-value store and the foundation for Ethereum's state trie structure.

## Goals

- **Memory Efficiency**: Adaptive node sizes (4/16/48/256) minimize pointer overhead
- **High Performance**: O(k) operations where k is key length (constant 32 bytes for Ethereum)
- **Ethereum-Native**: Direct mapping to trie structure, no impedance mismatch
- **General Purpose**: Design for potential standalone library use beyond Ethereum

## Architecture

```
Core: ART (general-purpose ordered key-value store)
  ↓
Persistence Layer: Snapshots + WAL
  ↓
Ethereum Layer: StateDB using ART for tries
  ↓
MPT Hash Layer: Transient, for state root computation only
```

## Development Phases

### Phase 1: Core In-Memory ART ⏳
**Files**: `database/include/art.h`, `database/src/art.c`

**Implementation**:
- Node types: Node4, Node16, Node48, Node256
- Basic operations: insert, get, delete
- Iterator support for ordered traversal
- Memory management and cleanup

**Requirements**:
- Pure in-memory, no persistence
- Support arbitrary-length keys
- Maintain sorted order for iteration
- Efficient memory usage with adaptive nodes

### Phase 2: ART Unit Tests ⏳
**Files**: `database/tests/test_art.c`

**Test Coverage**:
- Basic operations (insert, get, delete)
- Edge cases (empty tree, single node, large datasets)
- Iteration order correctness
- Memory leak detection
- Node type transitions (4→16→48→256)

**Validation**:
- All operations correct
- No memory leaks (valgrind)
- Performance baselines established

### Phase 3: Hash-Only MPT ⏳
**Files**: `state/include/mpt.h`, `state/src/mpt.c`

**Implementation**:
- Transient MPT structure for state root calculation
- Node types: Branch (16 children), Extension (path compression), Leaf (value)
- RLP encoding + Keccak-256 hashing
- Build from ART iteration, discard after computing hash

**Requirements**:
- Ethereum-compatible MPT structure
- Correct RLP encoding per Yellow Paper
- Keccak-256 (NOT SHA3-256)
- Memory cleanup after use

### Phase 4: MPT Test Vector Validation ✅ CRITICAL
**Files**: `state/tests/test_mpt_vectors.c`

**Implementation**:
- Load test vectors from `tools/state_test_vectors.json`
- Build MPT from test account data
- Calculate state roots
- Verify against expected values

**Test Vectors** (11 scenarios):
- Empty state
- Single accounts (no storage)
- Accounts with storage (4-5 slots)
- Multiple accounts (2-5 accounts)
- Account with bytecode

**Success Criteria**:
- All 11 test vectors pass
- State roots match Python HexaryTrie output
- Proves Ethereum compatibility

### Phase 5: StateDB with ART Tries ⏳
**Files**: `state/include/statedb.h`, `state/src/statedb.c`

**Implementation**:
- Account trie using ART
- Per-account storage tries using ART
- State root calculation via transient MPT
- Dirty tracking for commit optimization

**Operations**:
- Get/set account balance, nonce, code
- Get/set storage slots
- Commit: build MPT, calculate state root
- Rollback support

### Phase 6: StateDB Integration Tests ⏳
**Files**: `state/tests/test_statedb.c`

**Test Coverage**:
- Account creation and updates
- Storage slot operations
- State root calculation with full test vectors
- Transaction simulation (balance transfers, storage changes)
- Commit/rollback functionality

**Validation**:
- End-to-end state management
- State roots match expected values
- Journal/cache mechanics correct

### Phase 7: Snapshot Serialization ⏳
**Files**: `database/include/snapshot.h`, `database/src/snapshot.c`

**Implementation**:
- Serialize ART to disk format
- Deserialize on load/restart
- Incremental snapshots (delta from previous)
- Compression support

**Format Considerations**:
- Efficient on-disk representation
- Fast load times
- Versioning for upgrades
- Integrity verification (checksums)

### Phase 8: Write-Ahead Log (WAL) ⏳
**Files**: `database/include/wal.h`, `database/src/wal.c`

**Implementation**:
- Log operations before applying to ART
- Crash recovery replay
- Log rotation and cleanup
- Fsync policies

**Durability**:
- Atomicity guarantees
- Recovery from crashes
- Configurable sync modes (performance vs safety)

### Phase 9: Memory Eviction Strategy ⏳
**Files**: `database/art.c` (extension)

**Implementation**:
- LRU or clock eviction policy
- Flush cold state to snapshots
- Keep hot state in memory
- Configurable memory bounds

**Goals**:
- Bounded memory usage
- Graceful degradation under pressure
- Minimal impact on hot path

### Phase 10: Performance Optimizations ⏳
**Files**: Various

**Optimizations**:
- SIMD for node search (Node4/Node16)
- Prefetching hints
- Concurrent access (RW locks or lock-free)
- Cache-line alignment for nodes
- Custom allocators for node pools

**Benchmarking**:
- Operations per second
- Memory usage patterns
- Latency percentiles
- Comparison to RocksDB, LMDB, MDBX

## Success Metrics

### Correctness
- ✅ All test vectors pass (Phase 4)
- ✅ State roots match Ethereum reference
- ✅ No memory leaks or corruption

### Performance Targets
- **Insert/Get/Delete**: < 100ns per operation (in-memory)
- **Iteration**: > 1M keys/second
- **Memory overhead**: < 20 bytes per key-value pair
- **State root calculation**: < 10ms for 10K accounts

### Usability
- Clean API for general-purpose use
- Well-documented
- Comprehensive test coverage
- Potential for standalone library

## Current Status

**Phase**: Planning Complete ✅  
**Next Action**: Implement Phase 1 - Core In-Memory ART

## References

- [ART Paper](https://db.in.tum.de/~leis/papers/ART.pdf) - "The Adaptive Radix Tree: ARTful Indexing for Main-Memory Databases"
- [Ethereum Yellow Paper](https://ethereum.github.io/yellowpaper/paper.pdf) - MPT specification
- [libart Implementation](https://github.com/armon/libart) - Reference implementation
- Test Vectors: `tools/state_test_vectors.json` (11 scenarios, Python HexaryTrie validated)
