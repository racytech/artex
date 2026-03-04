# EVM — Spec Test Fix Roadmap

## Current Status (evm-dev-0)

**Total non-static state tests: 15,590 across 11 forks**

| Fork | Passed | Total | Rate | Failures | Notes |
|------|--------|-------|------|----------|-------|
| frontier | 5,621 | 5,625 | **99.9%** | 4 | Prague precompile absence (0x12) |
| homestead | 82 | 82 | **100%** | 0 | |
| byzantium | 378 | 405 | **93.3%** | 27 | BN256 EC ADD/MUL (0x06, 0x07) |
| constantinople | 192 | 192 | **100%** | 0 | |
| istanbul | 841 | 841 | **100%** | 0 | |
| berlin | 2,957 | 2,957 | **100%** | 0 | |
| london | 11 | 11 | **100%** | 0 | |
| paris | 180 | 180 | **100%** | 0 | |
| shanghai | 288 | 288 | **100%** | 0 | |
| cancun | 2,493 | 2,990 | **83.4%** | 497 | KZG point evaluation (0x0a) |
| prague | 1,431 | 2,019 | **70.9%** | 588 | BLS12-381(585), BN256 EIP-7702(3) |
| **TOTAL** | **14,474** | **15,590** | **92.8%** | **1,116** | |

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
- [x] **EIP-7702**: Set code TX type 4 — authorization processing, delegation resolution, CALL/EXTCODE* delegation, intrinsic gas, auth refund (545/548)
- [x] **EIP-2200**: SSTORE sentry check (gas_left <= 2300 guard)
- [x] **Call depth**: Fix off-by-one (allow depth 1024)

## Remaining Failures — All Precompiles

Every remaining non-static failure is a missing precompile crypto implementation.

| Precompile | Address | Failures | Forks Affected | Crypto dependency |
|------------|---------|----------|----------------|-------------------|
| BN256 ADD | 0x06 | ~15 | byzantium+ | bn256 curve lib |
| BN256 MUL | 0x07 | ~15 | byzantium+ | bn256 curve lib |
| BN256 PAIRING | 0x08 | — | byzantium+ | bn256 curve lib |
| KZG Point Evaluation | 0x0a | 497 | cancun+ | c-kzg-4844 library |
| BLS12-381 (9 precompiles) | 0x0b–0x13 | 585 | prague | blst library |

**Note**: Frontier 4 failures are Prague-fork precompile absence tests (address 0x12).

## Next Steps (ordered by impact)

| Step | What | Fixes | Effort |
|------|------|-------|--------|
| 1 | KZG point evaluation (0x0a) | ~497 | Medium — c-kzg-4844 library integration |
| 2 | BLS12-381 precompiles (0x0b-0x13) | ~585 | Large — blst library, 9 precompile impls |
| 3 | BN256 precompiles (0x06-0x08) | ~30 | Medium — bn256 curve library |

## External Dependencies

| Library | For | Install | Status |
|---------|-----|---------|--------|
| OpenSSL | SHA-256, RIPEMD-160 | `apt install libssl-dev` | **Installed** |
| libsecp256k1 | ECRECOVER | `apt install libsecp256k1-dev` | **Installed** |
| mini-gmp | MODEXP | vendored in evm/vendor/ | **Installed** |
| c-kzg-4844 | KZG point evaluation | vendored | Needed |
| blst | BLS12-381 | vendored | Needed |
| bn256 / mcl | BN256 precompiles | port from go-ethereum | Needed |
