# EVM — Spec Test Fix Roadmap

## Current Status (evm-dev-0)

**Total state tests: 15,590 across 11 forks**

| Fork | Passed | Total | Rate | Failures | Notes |
|------|--------|-------|------|----------|-------|
| frontier | 5,625 | 5,625 | **100%** | 0 | |
| homestead | 82 | 82 | **100%** | 0 | |
| byzantium | 402 | 405 | **99.3%** | 3 | BN256 EC precompiles(3) |
| constantinople | 192 | 192 | **100%** | 0 | |
| istanbul | 841 | 841 | **100%** | 0 | |
| berlin | 2,957 | 2,957 | **100%** | 0 | |
| london | 11 | 11 | **100%** | 0 | |
| paris | 180 | 180 | **100%** | 0 | |
| shanghai | 288 | 288 | **100%** | 0 | |
| cancun | 2,925 | 2,990 | **97.8%** | 65 | KZG point evaluation(65) |
| prague | 831 | 2,019 | 41.2% | 1,188 | BLS12-381(585), 7702(523), calldata gas blocked(80) |
| **TOTAL** | **14,334** | **15,590** | **92.0%** | **1,256** | |

### Completed Fixes

- [x] **Step 1**: Precompile framework + IDENTITY (0x04)
- [x] **Step 2**: SHA-256 (0x02) + RIPEMD-160 (0x03) with fork-gated gas costs
- [x] **Step 3**: ECRECOVER (0x01) via libsecp256k1
- [x] **Step 4**: SHL/SHR/SAR shift overflow fix (Constantinople 100%)
- [x] **EIP-2929**: Warm precompile addresses at transaction start (Berlin+)
- [x] **EIP-7610**: CREATE/CREATE2 storage collision check (retroactive, all forks)
- [x] **CREATE collision**: Consume forwarded gas on collision (63/64 rule)
- [x] **EIP-6780**: SELFDESTRUCT Cancun same-tx deletion + reentrancy/revert edge cases
- [x] **SELFDESTRUCT gas**: Fork-dependent gas (EIP-150/161/2929/3529), Exist vs Empty fix
- [x] **CALL gas**: Account existence check (Empty not Exist for Spurious Dragon+)
- [x] **EXTCODEHASH**: Fork gate for Constantinople+ (EIP-1052)
- [x] **CREATE2 EIP-2929**: Mark target address warm before collision check
- [x] **MODEXP**: Precompile (0x05) via mini-gmp (EIP-198/EIP-2565 gas)
- [x] **BLAKE2F**: Precompile (0x09) (Istanbul 100%)
- [x] **EIP-4844**: Blob TX type 3 — parsing, gas, BLOBHASH, BLOBBASEFEE, fake_exponential
- [x] **EIP-7623**: Calldata gas floor (Prague)

## Remaining Failure Breakdown

### 1. Missing Precompiles — 653 failures

| Precompile | Address | Failures | Fork | Crypto dependency |
|------------|---------|----------|------|-------------------|
| BN256 ADD/MUL/PAIRING | 0x06-0x08 | 3 | byzantium | bn256 curve lib (3-5 day project) |
| KZG Point Evaluation | 0x0a | 65 | cancun | KZG commitment lib |
| BLS12-381 | 0x0b-0x12 | 585 | prague | blst library |

### 2. Missing EIP-7702 Set Code TX — 523 failures

Prague-only. Type 4 transaction parsing, authorization lists.

### 3. EIP-7623 Blocked by Missing TX Types — 80 failures

Prague EIP-7623 calldata gas tests blocked by missing EIP-7702/blob TX support.

## Next Steps (ordered by impact / effort)

| Step | What | Remaining Failures | Effort |
|------|------|--------------------|--------|
| 1 | BN256 precompiles (0x06-0x08) | 3 | Large — 3-5 day EC math project |
| 2 | KZG point evaluation (0x0a) | 65 | Large — needs KZG library |
| 3 | EIP-7702 set code TX | 523+80 | Large — new TX type |
| 4 | BLS12-381 precompiles (EIP-2537) | 585 | Large — needs blst |

## External Dependencies

| Library | For | Install | Status |
|---------|-----|---------|--------|
| OpenSSL | SHA-256, RIPEMD-160 | `apt install libssl-dev` | **Installed** |
| libsecp256k1 | ECRECOVER | `apt install libsecp256k1-dev` | **Installed** |
| mini-gmp | MODEXP | vendored in evm/vendor/ | **Installed** |
| bn256 / mcl | BN256 precompiles | port from go-ethereum | Needed |
| c-kzg-4844 | KZG point evaluation | vendored | Needed |
| blst | BLS12-381 | vendored | Needed |
