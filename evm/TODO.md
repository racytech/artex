# EVM — Spec Test Fix Roadmap

## Current Status (evm-dev-0)

**Total state tests: 15,590 across 11 forks**

| Fork | Passed | Total | Rate | Failures | Notes |
|------|--------|-------|------|----------|-------|
| frontier | 5,478 | 5,625 | **97.4%** | 147 | precompiles(144), opcodes(3) |
| homestead | 82 | 82 | **100%** | 0 | |
| byzantium | 117 | 405 | 28.9% | 288 | MODEXP(261), EC add/mul(18), EC pairing(9) |
| constantinople | 192 | 192 | **100%** | 0 | |
| istanbul | 390 | 841 | 46.4% | 451 | BLAKE2(448), chainid(3) |
| berlin | 2,957 | 2,957 | **100%** | 0 | |
| london | 11 | 11 | **100%** | 0 | |
| paris | 144 | 180 | 80.0% | 36 | create collision(36) |
| shanghai | 288 | 288 | **100%** | 0 | |
| cancun | 998 | 2,990 | 33.4% | 1,992 | blobs(1918), selfdestruct(74) |
| prague | 769 | 2,019 | 38.1% | 1,250 | BLS12-381(585), 7702(525), calldata gas(140) |
| **TOTAL** | **11,426** | **15,590** | **73.3%** | **4,164** | |

### Completed Fixes (Steps 1-4 + partial 6)

- [x] **Step 1**: Precompile framework + IDENTITY (0x04)
- [x] **Step 2**: SHA-256 (0x02) + RIPEMD-160 (0x03) with fork-gated gas costs
- [x] **Step 3**: ECRECOVER (0x01) via libsecp256k1
- [x] **Step 4**: SHL/SHR/SAR shift overflow fix (Constantinople 100%)
- [x] **EIP-2929**: Warm precompile addresses at transaction start (Berlin+)
- [x] **EIP-7610**: Fork-gate storage collision check (Prague only)
- [x] **CREATE collision**: Consume forwarded gas on collision (63/64 rule)

## Remaining Failure Breakdown

### 1. Missing Precompiles — 1,461 failures

| Precompile | Address | Failures | Fork | Crypto dependency |
|------------|---------|----------|------|-------------------|
| Frontier precompiles | various | 144 | frontier | Edge cases in existing impls |
| MODEXP | 0x05 | 261 | byzantium | GMP bignum |
| BN256 ADD/MUL | 0x06-0x07 | 18 | byzantium | bn256 curve lib |
| BN256 PAIRING | 0x08 | 9 | byzantium | bn256 curve lib |
| BLAKE2F | 0x09 | 448 | istanbul | BLAKE2 ref impl |
| BLS12-381 | 0x0a-0x12 | 585 | prague | blst library |

### 2. Missing Blob TX (EIP-4844) — 1,918 failures

Cancun-only. Type 3 transaction parsing, blob gas model, point evaluation precompile.

### 3. Missing EIP-7702 Set Code TX — 525 failures

Prague-only. Type 4 transaction parsing, authorization lists.

### 4. Remaining Easy Fixes — 253 failures

| Issue | Failures | Fork | Effort |
|-------|----------|------|--------|
| EIP-7623 calldata gas floor | 140 | prague | Trivial — gas formula tweak |
| EIP-6780 SELFDESTRUCT Cancun | 74 | cancun | Debug — same-tx deletion rule |
| EIP-7610 CREATE collision | 36 | paris | Debug — remaining edge cases |
| EIP-1344 CHAINID | 3 | istanbul | Debug — likely config issue |
| Frontier opcodes | 3 | frontier | Debug |

## Next Steps (ordered by impact / effort)

| Step | What | Remaining Failures | Effort |
|------|------|--------------------|--------|
| 5 | EIP-7623 calldata gas floor | 140 | Trivial |
| 6 | EIP-6780 SELFDESTRUCT (Cancun) | 74 | Small |
| 7 | EIP-1344 CHAINID fix | 3 | Trivial |
| 8 | BLAKE2F precompile (0x09) | 448 | Medium — ref C impl |
| 9 | MODEXP precompile (0x05) | 261 | Medium — needs GMP |
| 10 | BN256 precompiles (0x06-0x08) | 27 | Medium — needs EC lib |
| 11 | Blob TX (EIP-4844) | 1,918 | Large — new TX type |
| 12 | EIP-7702 set code TX | 525 | Large — new TX type |
| 13 | BLS12-381 precompiles (EIP-2537) | 585 | Large — needs blst |

## External Dependencies

| Library | For | Install | Status |
|---------|-----|---------|--------|
| OpenSSL | SHA-256, RIPEMD-160 | `apt install libssl-dev` | **Installed** |
| libsecp256k1 | ECRECOVER | `apt install libsecp256k1-dev` | **Installed** |
| GMP | MODEXP | `apt install libgmp-dev` | Needed |
| BLAKE2 ref | BLAKE2F | vendored (small, public domain) | Needed |
| bn256 / mcl | BN256 precompiles | vendored or system | Needed |
| blst | BLS12-381 | vendored | Needed |
