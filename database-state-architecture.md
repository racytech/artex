# Database ↔ StateDB Architecture

## Overview

The Ethereum execution layer uses a layered architecture to balance **performance** (fast state access) with **correctness** (consensus compliance). This document describes the relationship between the persistent database layer and the state management layer.

---

## Architecture Layers

```
┌─────────────────────────────────────────┐
│              EVM                        │  ← Execution engine
│  (reads/writes accounts & storage)      │
└────────────────┬────────────────────────┘
                 │
┌────────────────▼────────────────────────┐
│           Journal/Cache                 │  ← Dirty state tracking
│  (uncommitted changes, snapshots)       │
└────────────────┬────────────────────────┘
                 │
┌────────────────▼────────────────────────┐
│            StateDB                      │  ← State management
│  (accounts, storage, state roots)       │
└────────────────┬────────────────────────┘
                 │
┌────────────────▼────────────────────────┐
│       Persistent Database               │  ← Durable storage
│  (RocksDB, Pebble, MDBX, etc.)         │
└─────────────────────────────────────────┘
```

---

## Component Roles

### 1. **Persistent Database** (Bottom Layer)

**Purpose:** Durable, crash-safe storage for all blockchain data.

**Responsibilities:**
- Store all state data on disk
- Provide ACID guarantees
- Survive crashes and restarts
- Enable database snapshots and backups
- Serve as source of truth for committed state

**What it stores:**

```
Table/Column Family: accounts
├─ Key:   address (20 bytes)
└─ Value: RLP(Account{nonce, balance, storageRoot, codeHash})

Table/Column Family: storage
├─ Key:   address + storage_key (52 bytes)
└─ Value: storage_value (32 bytes)

Table/Column Family: code
├─ Key:   code_hash (32 bytes)
└─ Value: contract bytecode

Table/Column Family: state_trie
├─ Key:   node_hash (32 bytes)
└─ Value: RLP(MPT node)

Table/Column Family: metadata
├─ Block headers
├─ State roots per block
└─ Chain configuration
```

**Operations:**
```c
typedef struct Database Database;
typedef struct Batch Batch;
typedef struct Snapshot Snapshot;

// Database operations
uint8_t* db_get(Database* db, const uint8_t* key, size_t key_len, size_t* value_len);
int db_put(Database* db, const uint8_t* key, size_t key_len, 
           const uint8_t* value, size_t value_len);
int db_delete(Database* db, const uint8_t* key, size_t key_len);

Batch* db_batch(Database* db);              // Atomic writes
Snapshot* db_snapshot(Database* db);        // Point-in-time view
int db_commit(Database* db, Batch* batch);  // Atomic commit
```

**Key characteristics:**
- **Persistent** - survives process restarts
- **Slow** - disk I/O latency (even with SSD)
- **Authoritative** - final source of truth
- **Append-mostly** - new blocks add data, rarely delete

---

### 2. **StateDB** (Middle Layer)

**Purpose:** Manage world state and provide efficient access for transaction execution.

**Responsibilities:**
- Cache frequently accessed accounts and storage
- Manage Merkle Patricia Trie (MPT) for state roots
- Provide clean API for account operations
- Coordinate between cache/journal and persistent DB
- Generate state roots for block headers
- Create Merkle proofs for state verification

**What it manages:**

```c
typedef struct StateDB {
    // Persistent storage
    Database* db;
    
    // In-memory caches
    LRUCache* account_cache;     // LRU<Address, Account>
    LRUCache* storage_cache;     // LRU<(Address, U256), U256>
    LRUCache* code_cache;        // LRU<Hash, Bytecode>
    
    // State trie for root calculation
    MPT* state_trie;
    HashMap* storage_tries;      // HashMap<Address, MPT*>
    
    // Pending changes (not yet in DB)
    Journal* journal;
    
    // Current block context
    uint64_t block_number;
    uint8_t state_root[32];      // Hash (32 bytes)
} StateDB;
```

**Core operations:**

```c
// Account operations
Account* state_db_get_account(StateDB* state, const uint8_t addr[20]);
int state_db_set_account(StateDB* state, const uint8_t addr[20], const Account* acc);
int state_db_delete_account(StateDB* state, const uint8_t addr[20]);

// Storage operations
void state_db_get_storage(StateDB* state, const uint8_t addr[20], 
                          const uint8_t key[32], uint8_t value[32]);
int state_db_set_storage(StateDB* state, const uint8_t addr[20],
                         const uint8_t key[32], const uint8_t value[32]);

// Code operations
Bytecode* state_db_get_code(StateDB* state, const uint8_t addr[20]);
int state_db_set_code(StateDB* state, const uint8_t addr[20], const Bytecode* code);

// Snapshot/revert for EVM
uint32_t state_db_snapshot(StateDB* state);
int state_db_revert_to_snapshot(StateDB* state, uint32_t snapshot_id);

// State root calculation
int state_db_compute_state_root(StateDB* state, uint8_t root[32]);

// Commit to database
int state_db_commit(StateDB* state);
```

**Access pattern:**

```
EVM requests account
        ↓
Check journal (dirty data) ───→ Found? Return
        ↓ Not found
Check cache ──────────────────→ Found? Return
        ↓ Not found
Query database ───────────────→ Load & cache
        ↓
Return to EVM
```

**Key characteristics:**
- **Fast** - memory caching reduces DB hits
- **Stateful** - tracks uncommitted changes
- **Trie-aware** - maintains MPT structure
- **Transactional** - supports snapshots/reverts

---

### 3. **Journal/Cache** (Top Layer)

**Purpose:** Track uncommitted state changes during transaction/block execution.

**Responsibilities:**
- Record all state modifications
- Support EVM snapshot/revert operations
- Enable rollback on transaction failure
- Batch changes for efficient database commits
- Provide instant access to dirty (modified) data

**Structure:**

```c
typedef struct Journal {
    // Dirty accounts (modified but not committed)
    HashMap* dirty_accounts;         // HashMap<Address, Account>
    
    // Dirty storage
    HashMap* dirty_storage;          // HashMap<Address, HashMap<U256, U256>>
    
    // Deleted accounts
    HashSet* deleted_accounts;       // HashSet<Address>
    
    // Snapshot stack for EVM REVERT
    Snapshot** snapshots;
    size_t snapshot_count;
    size_t snapshot_capacity;
    
    // Access tracking (for gas accounting)
    HashSet* accessed_accounts;      // HashSet<Address>
    HashSet* accessed_storage;       // HashSet<(Address, U256)>
} Journal;

typedef struct Snapshot {
    size_t journal_len;
    AccountChange* account_changes;
    size_t account_changes_count;
    StorageChange* storage_changes;
    size_t storage_changes_count;
} Snapshot;
```

**Operations:**

```c
// Record changes
void journal_set_account(Journal* j, const uint8_t addr[20], const Account* acc) {
    hashmap_insert(j->dirty_accounts, addr, 20, acc, sizeof(Account));
}

void journal_set_storage(Journal* j, const uint8_t addr[20], 
                         const uint8_t key[32], const uint8_t val[32]) {
    HashMap* storage = hashmap_get(j->dirty_storage, addr, 20);
    if (!storage) {
        storage = hashmap_create();
        hashmap_insert(j->dirty_storage, addr, 20, &storage, sizeof(HashMap*));
    }
    hashmap_insert(storage, key, 32, val, 32);
}

// EVM snapshot/revert
uint32_t journal_snapshot(Journal* j) {
    uint32_t id = j->snapshot_count;
    if (j->snapshot_count >= j->snapshot_capacity) {
        j->snapshot_capacity *= 2;
        j->snapshots = realloc(j->snapshots, j->snapshot_capacity * sizeof(Snapshot*));
    }
    j->snapshots[j->snapshot_count++] = journal_capture_state(j);
    return id;
}

int journal_revert(Journal* j, uint32_t id) {
    if (id >= j->snapshot_count) return -1;
    // Discard changes after snapshot
    journal_restore_from_snapshot(j, id);
    return 0;
}

// Flush to StateDB
int journal_commit(Journal* j, StateDB* state_db) {
    // Iterate dirty accounts
    HashMapIterator it = hashmap_iterator(j->dirty_accounts);
    while (hashmap_next(&it)) {
        const uint8_t* addr = it.key;
        const Account* acc = it.value;
        state_db_update_account(state_db, addr, acc);
    }
    
    // Iterate dirty storage
    it = hashmap_iterator(j->dirty_storage);
    while (hashmap_next(&it)) {
        const uint8_t* addr = it.key;
        HashMap* storage = *(HashMap**)it.value;
        
        HashMapIterator storage_it = hashmap_iterator(storage);
        while (hashmap_next(&storage_it)) {
            const uint8_t* key = storage_it.key;
            const uint8_t* val = storage_it.value;
            state_db_update_storage(state_db, addr, key, val);
        }
    }
    
    // Clear dirty sets
    hashmap_clear(j->dirty_accounts);
    hashmap_clear(j->dirty_storage);
    
    return 0;
}
```

**Key characteristics:**
- **Ephemeral** - exists only during execution
- **Mutable** - changes constantly during execution
- **Stackable** - supports nested snapshots
- **Fast** - pure in-memory operations

---

## Data Flow Examples

### Example 1: Reading Account Balance

```
┌─────────────────────────────────────────────────┐
│ EVM: balance = state.get_balance(0x123...abc)  │
└──────────────────┬──────────────────────────────┘
                   │
┌──────────────────▼──────────────────────────────┐
│ Journal: check dirty_accounts[0x123...abc]     │
│ Status: NOT FOUND (not modified yet)           │
└──────────────────┬──────────────────────────────┘
                   │
┌──────────────────▼──────────────────────────────┐
│ StateDB: check account_cache[0x123...abc]      │
│ Status: NOT FOUND (cache miss)                 │
└──────────────────┬──────────────────────────────┘
                   │
┌──────────────────▼──────────────────────────────┐
│ Database: query accounts[0x123...abc]          │
│ Status: FOUND → Account{balance: 10 ETH, ...}  │
└──────────────────┬──────────────────────────────┘
                   │
┌──────────────────▼──────────────────────────────┐
│ StateDB: cache result in account_cache         │
└──────────────────┬──────────────────────────────┘
                   │
┌──────────────────▼──────────────────────────────┐
│ EVM: receives balance = 10 ETH                 │
└─────────────────────────────────────────────────┘
```

### Example 2: Modifying Account Balance

```
┌─────────────────────────────────────────────────┐
│ EVM: state.set_balance(0x123...abc, 5 ETH)    │
└──────────────────┬──────────────────────────────┘
                   │
┌──────────────────▼──────────────────────────────┐
│ Journal: dirty_accounts[0x123...abc] = 5 ETH   │
│ (stored in memory, not yet persisted)          │
└─────────────────────────────────────────────────┘

Database is NOT touched yet - change is pending!
```

### Example 3: Transaction Execution with Revert

```
Transaction starts
    ↓
┌───────────────────────────────────┐
│ Journal: snapshot_0 = snapshot()  │
└────────────┬──────────────────────┘
             │
EVM executes: transfer 10 ETH
    ↓
┌────────────────────────────────────────┐
│ Journal: dirty_accounts[sender] -= 10  │
│ Journal: dirty_accounts[receiver] += 10│
└────────────┬───────────────────────────┘
             │
EVM encounters: REVERT opcode
    ↓
┌───────────────────────────────────────┐
│ Journal: revert_to_snapshot(snapshot_0)│
│ (discards all changes after snapshot) │
└────────────┬──────────────────────────┘
             │
Transaction fails - no changes persisted
StateDB and Database remain unchanged
```

### Example 4: Block Finalization

```
All transactions in block processed
    ↓
┌──────────────────────────────────────────┐
│ Journal: contains all dirty state        │
│ - 1,000 modified accounts                │
│ - 5,000 modified storage slots           │
└──────────────┬───────────────────────────┘
               │
┌──────────────▼───────────────────────────┐
│ StateDB: journal.commit(state_db)        │
│ - Updates account_cache                  │
│ - Updates storage_cache                  │
│ - Marks accounts as dirty in MPT         │
└──────────────┬───────────────────────────┘
               │
┌──────────────▼───────────────────────────┐
│ StateDB: compute_state_root()            │
│ - Update MPT for dirty accounts only     │
│ - Hash modified trie paths               │
│ - Result: new_root = 0xabcd...1234       │
└──────────────┬───────────────────────────┘
               │
┌──────────────▼───────────────────────────┐
│ Database: batch commit                   │
│ batch.put(accounts[addr1], account1)     │
│ batch.put(accounts[addr2], account2)     │
│ ...                                      │
│ batch.put(state_trie[hash1], node1)      │
│ batch.put(state_trie[hash2], node2)      │
│ ...                                      │
│ batch.commit() ← ATOMIC WRITE            │
└──────────────┬───────────────────────────┘
               │
State is now persistent and committed
New block header includes state_root
```

---

## Performance Optimization

### Caching Strategy

```
Hot Data (accessed frequently):
├─ StateDB.account_cache (LRU, ~100K accounts)
├─ StateDB.storage_cache (LRU, ~1M slots)
└─ StateDB.code_cache (LRU, ~10K contracts)

Warm Data (recently accessed):
├─ Database built-in cache (RocksDB block cache)
└─ OS page cache

Cold Data (rarely accessed):
└─ Disk storage
```

### Write Optimization

```
During execution:
├─ All writes go to Journal (memory)
├─ Zero disk I/O
└─ Maximum throughput

At commit:
├─ Batch all writes together
├─ Single atomic DB transaction
└─ Minimize fsync() calls
```

### Read Optimization

```
Account lookup:
1. Check Journal: O(1) hashmap
2. Check StateDB cache: O(1) LRU
3. Query Database: O(log n) LSM tree
4. Cache result for future hits
```

---

## Crash Recovery

### Scenario: Crash during block execution

```
Status before crash:
├─ Database: committed up to block N
├─ StateDB: processing block N+1
└─ Journal: contains uncommitted changes

After crash and restart:
├─ Journal: lost (in-memory only)
├─ StateDB: rebuilt from Database
├─ Database: consistent at block N
└─ Re-execute block N+1 from scratch
```

### Scenario: Crash during commit

```
Database uses write-ahead log (WAL):
├─ Changes written to WAL first
├─ Then applied to main database
└─ On restart: replay WAL if needed

Result: Either block N+1 fully committed or fully rolled back
No partial state corruption
```

---

## Component Comparison

| Aspect | Database | StateDB | Journal |
|--------|----------|---------|---------|
| **Storage** | Disk (persistent) | Memory + Disk | Memory only |
| **Speed** | Slow (milliseconds) | Fast (microseconds) | Fastest (nanoseconds) |
| **Scope** | All blockchain data | Current state + cache | Uncommitted changes |
| **Mutability** | Append-mostly | Read-heavy | Write-heavy |
| **Lifetime** | Permanent | Process lifetime | Block/tx lifetime |
| **Crash safety** | ACID guaranteed | Rebuilt on restart | Lost on crash |
| **Size** | Terabytes | Gigabytes | Megabytes |

---

## Key Takeaways

1. **Database** = Durable source of truth, slow but safe
2. **StateDB** = Performance layer with caching and trie management
3. **Journal** = Execution workspace for dirty state tracking

4. **Reads** flow: Journal → StateDB cache → Database
5. **Writes** flow: Journal → StateDB → Database (batched)

6. **Journal** makes execution fast (no DB writes during execution)
7. **StateDB** makes reads fast (caching + smart indexing)
8. **Database** makes it safe (persistence + crash recovery)

This layered architecture achieves both **speed** (for transaction execution) and **safety** (for consensus compliance).
