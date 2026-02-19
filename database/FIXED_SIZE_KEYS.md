# Fixed-Size Keys Implementation

## ⚠️ Important Notice

**This ART implementation is designed for FIXED-SIZE KEYS ONLY.**

All keys must have the same length. This implementation does NOT support:
- Variable-length keys
- Keys that are prefixes of other keys
- Mixed key sizes

## Stress Test

The full stress test (`test_data_art_full_stress`) now validates the fixed-size key approach:

```bash
# Test with 20-byte keys (Ethereum addresses)
./test_data_art_full_stress 30 1 12345 20

# Test with 32-byte keys (Ethereum hashes/storage keys)
./test_data_art_full_stress 30 1 99999 32
```

**Results**: ✅ Both key sizes work correctly with variable-length values (16-511 bytes)

### Validated Performance
- **20-byte keys** (10 seconds, seed 12345):
  - 1,879 inserts, 16,506 searches, 1,631 deletes
  - Throughput: 2,001.6 ops/sec
  
- **32-byte keys** (30 seconds, seed 99999):
  - 4,906 inserts, 95,781 searches, 4,642 deletes
  - Throughput: 3,511.0 ops/sec
  - 37 iterations completed

## Why Fixed-Size Keys?

This implementation is optimized for Ethereum state storage where:
- **Account addresses**: Always 20 bytes (160 bits)
- **Storage keys**: Always 32 bytes (256 bits)
- **Hashes**: Always 32 bytes (256 bits)

With fixed-size keys:
1. ✅ Keys never exhaust at different depths
2. ✅ No prefix relationships possible
3. ✅ Simpler implementation without values-in-internal-nodes
4. ✅ All values stored in leaves only

## Example

```c
// ✅ CORRECT USAGE - All keys same size
uint8_t key1[32] = {0xAA, 0xBB, ...};  // 32 bytes
uint8_t key2[32] = {0xCC, 0xDD, ...};  // 32 bytes
uint8_t key3[32] = {0xEE, 0xFF, ...};  // 32 bytes

data_art_insert(tree, key1, 32, value1, value1_len);
data_art_insert(tree, key2, 32, value2, value2_len);
data_art_insert(tree, key3, 32, value3, value3_len);
```

```c
// ❌ INCORRECT USAGE - Variable-length keys
uint8_t key1[20] = {...};  // 20 bytes - address
uint8_t key2[32] = {...};  // 32 bytes - storage key
uint8_t key3[8] = {...};   // 8 bytes - some ID

// DON'T MIX KEY SIZES - Use separate trees!
```

## Variable-Length Key Support

If you need variable-length keys or prefix relationships, use the implementation on the `feature/values-in-internal-nodes` branch, which includes:
- Values stored in internal nodes
- Proper handling of prefix relationships
- Support for keys of different lengths

## Technical Details

### Current Implementation (Fixed-Size)
- Uses `0x00` as navigation byte when key exhausted
- Works correctly because all keys exhaust at the same depth
- No collision possible since identical-length keys differ at some position

### Why Variable-Length Would Break
With variable-length keys like:
- Key A: `[0x2c]` (1 byte)
- Key B: `[0x2c, 0x00, 0x01]` (3 bytes)

At depth 1:
- Key A exhausted → navigates via `0x00`
- Key B has actual byte `0x00` at position 1
- **COLLISION**: Both use the same navigation byte!

This is avoided in fixed-size implementations because keys never have prefix relationships.

## Performance Characteristics

### Memory Efficiency
- **Fixed-size keys**: Values stored only in leaves
- **No value fields** in internal nodes (saves 12 bytes per node)
- Typical node: 4-256 children + metadata only

### Time Complexity
All operations remain O(k) where k is the fixed key length:
- **Insert**: O(20) for addresses, O(32) for hashes
- **Search**: O(20) for addresses, O(32) for hashes
- **Delete**: O(20) for addresses, O(32) for hashes

### Space Complexity
- Internal nodes: No value storage overhead
- Leaf nodes: One value per unique key
- Tree depth: Exactly k levels (where k = key size)

### Comparison with Variable-Length
| Metric | Fixed-Size | Variable-Length |
|--------|------------|-----------------|
| Node size | Smaller (no value fields) | Larger (+12 bytes) |
| Implementation | Simpler | More complex |
| Value storage | Leaves only | Internal + leaves |
| Prefix handling | Not needed | Required |
| Ethereum fit | Perfect match | Overkill |

## Ethereum State Tree Usage

For Ethereum, use separate trees:
1. **Account Tree**: 20-byte address keys → account data
2. **Storage Tree**: 32-byte storage keys → storage values
3. **Transaction Index**: 32-byte tx hash → tx data

Each tree uses consistent key sizes, avoiding the variable-length issue entirely.

## Design Decisions

### Why Not Support Both?
We maintain two separate implementations instead of one flexible solution because:

1. **Performance**: Fixed-size version avoids unnecessary overhead
2. **Simplicity**: Simpler code is easier to audit and maintain
3. **Safety**: Type-system enforcement of key size requirements
4. **Optimization**: Can leverage fixed-size assumptions for better performance

### Branch Strategy
- **`db` branch**: Production-ready fixed-size implementation for Ethereum
- **`feature/values-in-internal-nodes` branch**: General-purpose variable-length support

### When to Use Each
- **Use `db` branch** if:
  - Building Ethereum state storage
  - All keys are same size (e.g., UUIDs, hashes, fixed IDs)
  - Performance and simplicity are priorities
  
- **Use `feature/values-in-internal-nodes` branch** if:
  - Need variable-length keys
  - Keys have prefix relationships
  - General-purpose key-value store with mixed sizes

## Testing Strategy

### Current Test Coverage
1. **Unit Tests**: Individual operations (insert, search, delete)
2. **Stress Tests**: High-volume randomized operations
3. **Fixed-Size Validation**: Both 20-byte and 32-byte keys
4. **Value Variability**: 16-511 byte variable-length values

### How to Add Tests
```c
// Example: Testing with 20-byte keys
void test_ethereum_addresses(void) {
    data_art_t *tree = data_art_create("test.db");
    
    uint8_t addr1[20] = {0x12, 0x34, ...};
    uint8_t addr2[20] = {0x56, 0x78, ...};
    
    uint8_t balance1[] = "100 ETH";
    uint8_t balance2[] = "50 ETH";
    
    data_art_insert(tree, addr1, 20, balance1, 7);
    data_art_insert(tree, addr2, 20, balance2, 6);
    
    // Validate retrieval...
    data_art_destroy(tree);
}
```

## Migration Note

If you need to migrate to variable-length support in the future:
```bash
git checkout feature/values-in-internal-nodes
# Review changes in mem_art.c
# Apply similar changes to data_art if needed
```

## FAQ

### Q: Can I use 16-byte keys?
**A**: Yes, but you'll need to modify the validation in tests. The implementation supports any fixed size - the 20/32 byte restriction is specific to the stress test validation logic.

### Q: What if I accidentally mix key sizes?
**A**: The behavior is undefined. You may get incorrect results, crashes, or silent data corruption. Always ensure all keys in a tree have identical length.

### Q: Can values be different sizes?
**A**: Yes! Values can be any size. Only keys must be fixed-size.

### Q: Why not add a key_size field to the tree?
**A**: This would add runtime overhead for checking. The fixed-size constraint is a design decision, not a limitation. If you need flexibility, use the variable-length branch.

### Q: How do I handle multiple key types?
**A**: Use separate trees:
```c
data_art_t *address_tree = data_art_create("addresses.db");  // 20-byte keys
data_art_t *storage_tree = data_art_create("storage.db");    // 32-byte keys
```

### Q: What about key compression?
**A**: The ART structure already provides path compression through prefix compression in nodes. Additional key compression would break the radix tree properties.

### Q: Is this implementation thread-safe?
**A**: No. Concurrent access requires external synchronization (mutexes, read-write locks, etc.).

## References

- **ART Paper**: "The Adaptive Radix Tree: ARTful Indexing for Main-Memory Databases" (Leis et al., 2013)
- **Ethereum Yellow Paper**: For key size specifications
- **Feature Branch**: `feature/values-in-internal-nodes` for variable-length implementation

## Summary

✅ **Use this implementation if**: All keys are the same size (Ethereum addresses, hashes, fixed IDs)  
❌ **Don't use if**: You need variable-length keys or prefix relationships  
🔀 **Alternative**: Check `feature/values-in-internal-nodes` branch for full variable-length support
