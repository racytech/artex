# MVCC Version Chains - Design Document

**Status**: Planning / Not Implemented  
**Date**: February 20, 2026  
**Context**: Extension to existing MVCC snapshot isolation system

---

## Problem Statement

Current MVCC implementation supports:
- ✅ Snapshot isolation for concurrent inserts
- ✅ Logical deletion (snapshots see deleted data)
- ❌ **Updates**: Delete+Insert physically replaces old version
- ❌ **Multiple snapshots cannot see different versions of same key**

**Test that fails**: `test_multiple_snapshots()` in `test_snapshot_isolation.c`

```c
// Scenario:
INSERT key="AA" value="version1"  (txn=1)
SNAPSHOT1 created
UPDATE key="AA" value="version2"  (txn=2) ← Physically replaces v1
SNAPSHOT1 tries to read key="AA" → finds v2 leaf (xmin=2) → NOT VISIBLE
// Expected: "version1"
// Actual: NULL (visibility check fails)
```

---

## Current Implementation Limitations

### 1. Physical Replacement on Update
```
Tree before update:          Tree after update:
   Path "AA"                    Path "AA"
      ↓                            ↓
   Leaf(xmin=1, xmax=0)         Leaf(xmin=2, xmax=0)
   value="version1"              value="version2"
                                 ← v1 is GONE
```

### 2. Logical Deletion Works
```
Tree before delete:          Tree after delete:
   Path "AA"                    Path "AA"
      ↓                            ↓
   Leaf(xmin=1, xmax=0)         Leaf(xmin=1, xmax=2) ← Still in tree!
   value="data"                  value="data"
```
**Why it works**: Leaf stays at same path, xmax marks deletion time

### 3. Why Updates Don't Work
- Delete sets `xmax=2` on old leaf
- Insert creates NEW leaf at SAME path (replaces node_ref)
- Old leaf becomes unreachable from tree
- Snapshot cannot find it via tree traversal

---

## Proposed Solution: Version Chains

### Architecture

**Add `prev_version` field to leaf nodes:**

```c
typedef struct data_art_leaf {
    node_type_t type;           // NODE_LEAF
    uint8_t flags;              // LEAF_FLAG_*
    uint16_t key_len;
    uint32_t value_len;
    uint64_t overflow_page;
    uint16_t inline_data_len;
    
    // MVCC fields
    uint64_t xmin;              // Creating transaction ID
    uint64_t xmax;              // Deleting transaction ID (0 = not deleted)
    node_ref_t prev_version;    // ← NEW: Points to older version
    
    uint8_t inline_data[];
} __attribute__((packed)) data_art_leaf_t;
```

**Size impact**: +8 bytes per leaf (node_ref_t = page_id:offset)

### Version Chain Structure

```
Tree path "AA" always points to LATEST version:

   Path "AA"
      ↓
   [Latest] Leaf(xmin=3, xmax=0, value="v3", prev → page_ref_2)
               ↓
   [Middle] Leaf(xmin=2, xmax=3, value="v2", prev → page_ref_1)
               ↓
   [Oldest] Leaf(xmin=1, xmax=2, value="v1", prev → NULL)
```

### Read Algorithm with Version Chains

```c
const void* data_art_get_versioned(tree, key, snapshot) {
    // 1. Tree traversal finds LATEST version
    node_ref_t current_ref = tree_lookup(tree, key);
    
    while (current_ref != NULL_NODE_REF) {
        data_art_leaf_t *leaf = get_leaf(current_ref);
        
        // 2. Check visibility
        if (mvcc_is_visible(leaf->xmin, leaf->xmax, snapshot)) {
            return leaf->data;  // Found visible version
        }
        
        // 3. Walk to older version
        current_ref = leaf->prev_version;
    }
    
    return NULL;  // No visible version
}
```

**Visibility examples:**

| Snapshot xmax | Leaf xmin | Leaf xmax | Visible? | Reason |
|--------------|-----------|-----------|----------|---------|
| 1 | 1 | 0 | ✅ | Created before snapshot, not deleted |
| 1 | 2 | 0 | ❌ | Created after snapshot |
| 2 | 1 | 3 | ✅ | Created before, deleted after snapshot |
| 2 | 1 | 2 | ❌ | Deleted before/at snapshot |

### Update Algorithm with Version Chains

**Current (broken):**
```c
bool data_art_update(tree, key, new_value) {
    data_art_delete(tree, key);        // Sets xmax or physically removes
    data_art_insert(tree, key, value); // Creates new leaf, replaces node_ref
    // ← Old version lost!
}
```

**With version chains:**
```c
bool data_art_update_versioned(tree, key, new_value) {
    // 1. Find current version
    node_ref_t old_ref = tree_lookup(tree, key);
    data_art_leaf_t *old_leaf = get_leaf(old_ref);
    
    // 2. Mark as superseded
    old_leaf->xmax = current_txn_id;
    
    // 3. Create new version
    node_ref_t new_ref = create_leaf(key, new_value);
    data_art_leaf_t *new_leaf = get_leaf(new_ref);
    new_leaf->xmin = current_txn_id;
    new_leaf->xmax = 0;
    new_leaf->prev_version = old_ref;  // ← Link to old version
    
    // 4. Replace tree pointer (only latest changes)
    tree_replace(tree, key, new_ref);
    
    // Chain preserved: new_ref → old_ref → older_ref → ...
}
```

---

## Implementation Phases

### Phase 1: Data Structure Changes
- [ ] Add `prev_version` field to `data_art_leaf_t`
- [ ] Update leaf creation to initialize `prev_version = NULL_NODE_REF`
- [ ] Verify page layout still fits in PAGE_SIZE
- [ ] Update serialization/deserialization

### Phase 2: Update Path
- [ ] Modify `data_art_delete()` to NOT physically remove during updates
- [ ] Create `data_art_update()` function:
  - Set xmax on old version
  - Create new version with prev_version link
  - Replace tree node_ref
- [ ] Distinguish between DELETE (xmax, prev=NULL) vs UPDATE (xmax, prev≠NULL)

### Phase 3: Read Path with Chain Walking
- [ ] Modify `data_art_get()` to walk version chains
- [ ] Add chain length tracking (prevent infinite loops)
- [ ] Add performance metrics (avg chain length)

### Phase 4: Testing
- [ ] Enable `test_multiple_snapshots()`
- [ ] Add chain walking tests
- [ ] Add long chain tests (100+ versions)
- [ ] Verify visibility rules across chains

### Phase 5: Garbage Collection (CRITICAL)
- [ ] Track oldest active snapshot
- [ ] Background GC thread:
  - Find versions with `xmax < oldest_snapshot.xmin`
  - Check if any snapshot needs them
  - Physically delete + update prev_version links
- [ ] Add GC metrics (versions collected, disk freed)
- [ ] Configurable retention policy

---

## Performance Considerations

### Space Overhead

**Per-leaf overhead**: +8 bytes (prev_version)

**Example: 1M keys with 10 updates each**
```
Without chains: 1M leaves × 500 bytes = 500 MB
With chains:    10M leaves × 508 bytes = 5.08 GB (10.2x increase)
With GC:        Depends on retention policy
```

### Read Performance

**Best case**: O(1) - latest version visible
**Worst case**: O(chain_length) - need oldest version
**Typical**: O(2-3) - most reads hit recent versions

**Mitigation strategies:**
1. Keep chains short via GC (max 10-20 versions)
2. Compact long chains (keep v1, v10, v20, v30... delete middle versions)
3. Version sampling for old snapshots

### Write Performance

**Update cost:**
- Current: O(delete) + O(insert) = 2 × tree operations
- With chains: O(lookup) + O(create_leaf) + O(update_ref) = similar
- **No significant slowdown** (just link manipulation)

**GC overhead:**
- Background thread (doesn't block transactions)
- Scans for old versions periodically
- Only touches pages with xmax < oldest_snapshot

---

## Ethereum Use Case Analysis

### Archive Node Requirements

**Query pattern:**
```solidity
// Historical balance at specific block
eth_getBalance(address, blockNumber)

// Implementation:
snapshot = create_snapshot_at_block(blockNumber)
balance = data_art_get(address, snapshot)
release_snapshot(snapshot)
```

**Storage implications:**
- Ethereum state: ~40M accounts
- Average updates per account: ~100 over 5 years
- Total versions: **4 billion**
- With GC (keep last 128 blocks): ~5M versions (manageable)

### Full Node vs Archive Node

| Node Type | Retention | Storage | GC Strategy |
|-----------|-----------|---------|-------------|
| Light | 0 blocks | ~1 GB | Aggressive - keep nothing |
| Full | 128 blocks | ~100 GB | Time-based - delete >128 blocks old |
| Archive | All blocks | ~12 TB | None or minimal (keep landmarks) |

### Block Number Mapping

**Challenge**: MVCC uses sequential txn_id, Ethereum uses block_number

**Solution 1: Direct mapping**
```c
// txn_id = block_number × 1000 + txn_index
// Example: Block 18000000, txn 5 → txn_id = 18000000005
// Snapshot at block X → snapshot.xmax = X × 1000 + 999
```

**Solution 2: Lookup table**
```c
// block_to_txn_range[18000000] = {start: 500000000, end: 500000156}
// Snapshot at block X → snapshot.xmax = block_to_txn_range[X].end
```

**Solution 3: Hybrid (recommended)**
```c
typedef struct {
    uint64_t block_number : 40;  // Up to 1 trillion blocks
    uint64_t txn_index : 24;     // Up to 16M txns per block
} ethereum_txn_id_t;
```

---

## Alternative Approaches Considered

### 1. Copy-on-Write Tree Snapshots
**Idea**: Clone entire tree structure on snapshot
- ❌ **Rejected**: Too expensive (full tree copy)
- ❌ Memory overhead: O(tree_size) per snapshot
- ✅ Would provide perfect isolation

### 2. Separate Version Table
**Idea**: Keep versions in separate table, tree points to version_id
- ❌ **Rejected**: Extra indirection on every read
- ❌ Breaks locality (versions not near key)
- ✅ Could enable cross-key version coordination

### 3. Time-Travel Index
**Idea**: B-tree indexed by (key, timestamp)
- ❌ **Rejected**: Not adaptive radix tree anymore
- ❌ Range queries become complex
- ✅ Would make historical queries efficient

### 4. Delta Encoding
**Idea**: Store diffs between versions instead of full values
- 🤔 **Maybe later**: Reduces storage for similar versions
- ❌ Increases read cost (reconstruct from deltas)
- ✅ Great for Ethereum storage slots (often small changes)

---

## Open Questions

1. **Chain Length Limits**: Hard cap at 1000? Compact long chains?
2. **GC Scheduling**: Time-based (every 10s)? Space-based (when disk >80%)?
3. **Concurrent GC**: How to handle snapshots created during GC?
4. **Crash Recovery**: What if GC is interrupted? (Need WAL for GC operations?)
5. **Version Chain Compaction**: Keep every Nth version for old snapshots?
6. **Cross-key Transactions**: How to handle multi-key updates atomically?
7. **Overflow Pages**: Do version chains span overflow pages? (Large values)

---

## Testing Strategy

### Unit Tests
- [ ] Chain creation and walking
- [ ] Visibility across chain (8 MVCC rules × multiple versions)
- [ ] GC correctness (preserve needed versions)
- [ ] Edge cases (empty chains, single version, very long chains)

### Integration Tests
- [ ] `test_multiple_snapshots()` - the original failing test
- [ ] Concurrent readers on different snapshots
- [ ] Update storm (many updates to same key)
- [ ] Snapshot at different points in chain

### Stress Tests
- [ ] 1M keys × 100 updates each
- [ ] 100 concurrent snapshots
- [ ] GC under load
- [ ] Measure: throughput, latency, memory, disk growth

### Performance Tests
- [ ] Baseline: current implementation (no chains)
- [ ] With chains, no GC (worst case)
- [ ] With chains + GC (realistic)
- [ ] Metrics: ops/sec, P50/P99 latency, space amplification

---

## Migration Path

**Option 1: Breaking change**
- Bump database format version
- Require full rebuild from scratch
- **Pro**: Clean implementation
- **Con**: Users lose data

**Option 2: Backward compatible**
- Detect old format (prev_version field missing/zero)
- Initialize chains on first update
- **Pro**: No data loss
- **Con**: Complex transition code

**Option 3: Optional feature flag**
```c
data_art_config_t config = {
    .enable_version_chains = true,  // Default: false
    .max_chain_length = 100,
    .gc_enabled = true,
};
```

---

## References

- PostgreSQL MVCC: https://www.postgresql.org/docs/current/mvcc.html
- CockroachDB Time-Travel: https://www.cockroachlabs.com/docs/stable/as-of-system-time.html
- Our MVCC implementation: `/home/racytech/workspace/art/database/src/mvcc.c`
- Failing test: `/home/racytech/workspace/art/database/tests/test_snapshot_isolation.c:237`

---

## Decision Log

**2026-02-20**: Document created - version chains needed for update isolation
- Current system: Works for inserts and deletes
- Gap: Cannot see old versions after updates
- Solution: Version chains with prev_version links
- Next step: Decide if/when to implement

---

## Estimated Effort

| Phase | Complexity | Time Estimate |
|-------|-----------|---------------|
| Data structure changes | Low | 2-4 hours |
| Update path | Medium | 8-12 hours |
| Read path with chains | Medium | 6-10 hours |
| Testing | High | 12-16 hours |
| Garbage collection | Very High | 20-30 hours |
| **Total** | | **48-72 hours** |

**Recommendation**: Worth implementing if:
1. You need true historical queries (archive node)
2. Multiple concurrent snapshots are required
3. Storage budget allows for version retention

**Skip if**:
1. Only need transaction-level isolation (current system works)
2. Storage is constrained
3. Can afford to block snapshots during updates
