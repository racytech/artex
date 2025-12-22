# EIP-4844 Implementation TODO

## Overview
EIP-4844 (Proto-Danksharding) introduces blob transactions with separate fee market for data availability.
Currently, blob transaction tests are passing but we're **not actually validating blob-specific fields**.

## Missing Data Structures

### 1. `transaction_t` (evm/include/transaction.h)
Add blob-specific fields:
```c
// EIP-4844: Blob transactions
uint256_t max_fee_per_blob_gas;           // Maximum fee per blob gas unit
hash_t *blob_versioned_hashes;            // Array of blob commitment hashes (type 0x01)
size_t blob_versioned_hashes_count;       // Number of blob hashes (max 6 per tx)
```

### 2. `block_env_t` (evm/include/transaction.h)
Add blob gas tracking:
```c
// EIP-4844: Blob gas
uint64_t excess_blob_gas;                 // Excess blob gas from previous block
```

### 3. `test_transaction_t` (integration_tests/include/test_fixtures.h)
Add EIP-1559 and EIP-4844 fields:
```c
// EIP-1559 fields
uint256_t max_priority_fee_per_gas;       // Miner tip
uint256_t max_fee_per_gas;                // Maximum total fee per gas

// EIP-4844 fields
uint256_t max_fee_per_blob_gas;           // Maximum fee per blob gas
hash_t *blob_versioned_hashes;            // Array of blob hashes
size_t blob_versioned_hashes_count;       // Number of blob hashes
```

## Missing Parser Implementation

### `parse_transaction()` (integration_tests/src/test_parser_blocks.c)
Add parsing for:
- `maxPriorityFeePerGas` (EIP-1559)
- `maxFeePerGas` (EIP-1559)
- `maxFeePerBlobGas` (EIP-4844)
- `blobVersionedHashes` (EIP-4844) - array of 32-byte hashes

### `parse_block_header()` (integration_tests/src/test_parser_blocks.c)
Parse from env:
- `currentExcessBlobGas` → `excess_blob_gas`

## Missing Validation Logic

### 1. Blob Gas Price Calculation (evm/src/transaction.c)
Implement `calculate_blob_gas_price()`:
```c
// Blob base fee formula: fake_exponential(MIN_BLOB_GASPRICE, excess_blob_gas, BLOB_GASPRICE_UPDATE_FRACTION)
// MIN_BLOB_GASPRICE = 1
// BLOB_GASPRICE_UPDATE_FRACTION = 3338477
uint256_t calculate_blob_gas_price(uint64_t excess_blob_gas);
```

### 2. Transaction Validation (evm/src/transaction.c)
Add to `transaction_validate()`:
- Validate `blob_versioned_hashes_count` ≤ 6 (MAX_BLOB_GAS_PER_BLOCK / GAS_PER_BLOB)
- Validate all blob hashes have version 0x01 (first byte)
- Check balance covers: `value + (gas_limit * max_fee_per_gas) + (blob_gas * max_fee_per_blob_gas)`
- Verify `max_fee_per_blob_gas >= blob_base_fee`

### 3. Blob Gas Charging (evm/src/transaction.c)
In `transaction_execute()`:
- Calculate actual blob gas: `blob_versioned_hashes_count * GAS_PER_BLOB` (131072 per blob)
- Charge blob gas: `blob_gas * blob_base_fee`
- Burn blob gas payment (not paid to coinbase)
- Update excess blob gas for next block

## Missing Opcode Support

### BLOBHASH (0x49) - evm/src/opcodes/block.c
```c
evm_status_t op_blobhash(evm_t *evm) {
    // Pop index from stack
    // If index < tx.blob_versioned_hashes_count:
    //   Push tx.blob_versioned_hashes[index]
    // Else:
    //   Push 0
}
```

### BLOBBASEFEE (0x4A) - evm/src/opcodes/block.c
```c
evm_status_t op_blobbasefee(evm_t *evm) {
    // Calculate blob_base_fee = calculate_blob_gas_price(block.excess_blob_gas)
    // Push blob_base_fee to stack
}
```

## Constants to Define

In `evm/include/gas.h` or `evm/include/fork.h`:
```c
#define GAS_PER_BLOB 131072                    // Gas per blob (128 KB)
#define MAX_BLOB_GAS_PER_BLOCK 786432          // 6 blobs per block
#define TARGET_BLOB_GAS_PER_BLOCK 393216       // 3 blobs target
#define MIN_BLOB_GASPRICE 1                    // Minimum blob base fee
#define BLOB_GASPRICE_UPDATE_FRACTION 3338477  // Price update denominator
```

## Test Runner Updates

### `test_runner_state.c`
- Extract blob fields from `test_transaction_t`
- Copy to `transaction_t` when building transaction
- Set `excess_blob_gas` in block environment from test header
- Handle TX_TYPE_EIP4844 transactions

## Implementation Priority

1. **High**: Add data structures (blocks BLOBHASH/BLOBBASEFEE opcodes)
2. **High**: Implement parser for blob fields (needed to read test data)
3. **High**: Implement BLOBHASH and BLOBBASEFEE opcodes
4. **Medium**: Blob gas price calculation
5. **Medium**: Transaction validation for blobs
6. **Low**: Blob gas charging and burning (most tests don't validate this)

## Current Status

✅ Transaction type `TX_TYPE_EIP4844` defined
✅ Fork detection `fork_has_blob_opcodes()` implemented
❌ No blob-specific fields in data structures
❌ No blob field parsing
❌ No blob validation
❌ BLOBHASH opcode not implemented
❌ BLOBBASEFEE opcode not implemented

## Why Tests Currently Pass

Tests pass because:
1. We don't validate blob-specific fields (they're ignored)
2. We don't check blob gas prices
3. We don't verify blob hashes
4. BLOBHASH/BLOBBASEFEE opcodes return 0 (stack underflow or unimplemented)
5. Balance checks only consider regular gas, not blob gas

**This is a false positive!** We need proper implementation to truly support EIP-4844.

## References

- EIP-4844: https://eips.ethereum.org/EIPS/eip-4844
- Blob base fee formula: https://github.com/ethereum/execution-specs/blob/master/src/ethereum/cancun/fork.py
