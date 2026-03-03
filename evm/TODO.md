# EVM — Spec Test Fix Roadmap

## Current Status (evm-dev-0)

**Total state tests: 15,591 across 13 forks**

| Fork | Passed | Total | Rate | Failures |
|------|--------|-------|------|----------|
| frontier | 4,682 | 5,625 | 83.2% | 943 |
| homestead | 10 | 82 | 12.2% | 72 |
| byzantium | 3 | 405 | 0.7% | 402 |
| constantinople | 168 | 192 | 87.5% | 24 |
| istanbul | 19 | 841 | 2.3% | 822 |
| berlin | 2,957 | 2,957 | **100%** | 0 |
| london | 11 | 11 | **100%** | 0 |
| paris | 108 | 180 | 60.0% | 72 |
| shanghai | 288 | 288 | **100%** | 0 |
| cancun | 764 | 2,990 | 25.6% | 2,226 |
| prague | 377 | 2,019 | 18.7% | 1,642 |
| **TOTAL** | **9,387** | **15,591** | **60.2%** | **6,204** |

## Failure Root Cause Analysis

### 1. Missing Precompiles — 3,197 failures (51.5%)

No precompile support exists. Calls to addresses 0x01–0x09 (and 0x0a+ for Prague)
fall through to "no code at address" → wrong state root.

| Precompile | Address | Failures | EIP | Crypto dependency |
|------------|---------|----------|-----|-------------------|
| ECRECOVER | 0x01 | ~200 | — | secp256k1 |
| SHA-256 | 0x02 | ~200 | — | OpenSSL / custom |
| RIPEMD-160 | 0x03 | ~200 | — | OpenSSL / custom |
| IDENTITY | 0x04 | ~350 | — | None (memcpy) |
| MODEXP | 0x05 | 348 | EIP-198 | bignum (GMP or custom) |
| BN256 ADD | 0x06 | 36 | EIP-196 | bn256 curve lib |
| BN256 MUL | 0x07 | part of above | EIP-196 | bn256 curve lib |
| BN256 PAIRING | 0x08 | 18 | EIP-197 | bn256 curve lib |
| BLAKE2F | 0x09 | 819 | EIP-152 | BLAKE2 ref impl |
| BLS12-381 (0x0a-0x12) | 0x0a+ | 975 | EIP-2537 | BLS lib (blst) |

### 2. Missing Blob TX Support (Type 3) — ~2,144 failures (34.6%)

EIP-4844 blob transactions not parsed/validated. Includes:
- Blob gas accounting (excess_blob_gas, blob_gas_used)
- Type 3 transaction validation
- BLOBHASH opcode (0x49) — may already exist
- BLOBBASEFEE opcode (0x4A) — may already exist
- Point evaluation precompile (0x0A, Cancun)

### 3. Missing EIP-7702 Set Code TX (Type 4) — 527 failures (8.5%)

Prague-only. Authorization lists, delegation designators.

### 4. Easy Fixes — 168 failures (2.7%)

| Issue | Failures | Effort |
|-------|----------|--------|
| EIP-7623 calldata gas floor | 140 | Trivial — gas formula tweak |
| EIP-145 bitwise shift bug | 24 | Debug — SHL/SHR/SAR edge case |
| EIP-7610 CREATE collision | 72 | Small — check code_hash before CREATE |
| SELFDESTRUCT edge cases | 82 | Debug — EIP-6780 Cancun rules |

## Incremental Fix Plan (ordered by impact / effort)

### Step 1: Identity Precompile (0x04) + Precompile Framework
**Impact: ~350 failures fixed + framework for all precompiles**
- Add `is_precompile(address, fork)` check
- Add precompile dispatch in `evm_execute()` before code loading
- Implement IDENTITY (trivial: memcpy input → output)
- Gas: 15 base + 3 per word

### Step 2: SHA-256 (0x02) + RIPEMD-160 (0x03)
**Impact: ~400 failures fixed**
- Use OpenSSL or standalone C implementations
- SHA-256: 60 base + 12 per word
- RIPEMD-160: 600 base + 120 per word, output left-padded to 32 bytes

### Step 3: ECRECOVER (0x01)
**Impact: ~200 failures fixed**
- Needs secp256k1 library (libsecp256k1 or custom)
- Input: hash, v, r, s → output: recovered address
- Gas: 3000 flat

### Step 4: EIP-145 Bitwise Shift Bug
**Impact: 24 failures fixed**
- Debug SHL/SHR/SAR — likely an edge case with shift >= 256

### Step 5: EIP-7623 Calldata Gas Floor
**Impact: 140 failures fixed**
- Prague gas rule: `floor_gas = TOTAL_COST_FLOOR_PER_TOKEN * tokens_in_calldata`
- `intrinsic_gas = max(standard_intrinsic, floor_gas)`

### Step 6: EIP-7610 CREATE Collision
**Impact: 72 failures fixed**
- Before CREATE: check if target has code or nonce > 0 → fail

### Step 7: EIP-6780 SELFDESTRUCT (Cancun)
**Impact: 82 failures fixed**
- SELFDESTRUCT only deletes account if created in same tx
- Otherwise just transfers balance

### Step 8: BLAKE2F Precompile (0x09)
**Impact: 819 failures fixed**
- BLAKE2b compression function (reference C implementation available)
- 12 rounds parameter from input, gas = rounds

### Step 9: MODEXP Precompile (0x05)
**Impact: 348 failures fixed**
- Arbitrary-precision modular exponentiation
- Needs bignum library (mini-gmp or similar)
- Complex gas formula based on operand sizes

### Step 10: BN256 Precompiles (0x06, 0x07, 0x08)
**Impact: 54 failures fixed**
- Elliptic curve operations on alt_bn128
- Needs dedicated EC library

### Step 11: Blob TX Support (EIP-4844)
**Impact: ~2,144 failures fixed**
- Type 3 transaction parsing
- Blob gas model
- Point evaluation precompile

### Step 12: EIP-7702 Set Code TX
**Impact: 527 failures fixed**
- Type 4 transaction parsing
- Authorization list processing

### Step 13: BLS12-381 Precompiles (EIP-2537)
**Impact: 975 failures fixed**
- 9 precompile addresses for BLS operations
- Needs blst or similar library

## Implementation Order Summary

| Step | What | Failures Fixed | Cumulative | Rate |
|------|------|---------------|------------|------|
| 1 | Identity precompile + framework | ~350 | 9,737 | 62.5% |
| 2 | SHA-256 + RIPEMD-160 | ~400 | 10,137 | 65.0% |
| 3 | ECRECOVER | ~200 | 10,337 | 66.3% |
| 4 | Bitwise shift bug | 24 | 10,361 | 66.5% |
| 5 | EIP-7623 calldata gas | 140 | 10,501 | 67.4% |
| 6 | EIP-7610 CREATE collision | 72 | 10,573 | 67.8% |
| 7 | EIP-6780 SELFDESTRUCT | 82 | 10,655 | 68.4% |
| 8 | BLAKE2F | 819 | 11,474 | 73.6% |
| 9 | MODEXP | 348 | 11,822 | 75.8% |
| 10 | BN256 (EC add/mul/pairing) | 54 | 11,876 | 76.2% |
| 11 | Blob TX (EIP-4844) | ~2,144 | 14,020 | 89.9% |
| 12 | EIP-7702 set code | 527 | 14,547 | 93.3% |
| 13 | BLS12-381 | 975 | 15,522 | 99.6% |

## Files to Create/Modify

- `evm/src/precompile.c` — precompile dispatch + implementations
- `evm/include/precompile.h` — precompile interface
- `evm/src/evm.c` — hook precompile check into evm_execute()
- `evm/src/transaction.c` — blob tx, 7702 tx support
- `evm/src/gas.c` — EIP-7623 calldata floor
- `evm/src/opcodes/create.c` — EIP-7610 collision check
- `CMakeLists.txt` — add precompile.c, link crypto libs

## External Dependencies Needed

| Library | For | Install |
|---------|-----|---------|
| OpenSSL | SHA-256, RIPEMD-160 | `apt install libssl-dev` |
| libsecp256k1 | ECRECOVER | `apt install libsecp256k1-dev` |
| mini-gmp or GMP | MODEXP | `apt install libgmp-dev` |
| BLAKE2 ref | BLAKE2F | vendored (small, public domain) |
| bn256 / mcl | BN256 precompiles | vendored or system |
| blst | BLS12-381 | vendored |
