# EVM — Spec Test Fix Roadmap

## Current Status (evm-dev-0)

**Total non-static state tests: 15,590 across 11 forks — ALL PASSING**

| Fork | Passed | Total | Rate |
|------|--------|-------|------|
| frontier | 5,625 | 5,625 | **100%** |
| homestead | 82 | 82 | **100%** |
| byzantium | 405 | 405 | **100%** |
| constantinople | 192 | 192 | **100%** |
| istanbul | 841 | 841 | **100%** |
| berlin | 2,957 | 2,957 | **100%** |
| london | 11 | 11 | **100%** |
| paris | 180 | 180 | **100%** |
| shanghai | 288 | 288 | **100%** |
| cancun | 2,990 | 2,990 | **100%** |
| prague | 2,019 | 2,019 | **100%** |
| **TOTAL** | **15,590** | **15,590** | **100%** |

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
- [x] **EIP-7702**: Set code TX type 4 — authorization processing, delegation resolution, CALL/EXTCODE* delegation, intrinsic gas, auth refund
- [x] **EIP-2200**: SSTORE sentry check (gas_left <= 2300 guard)
- [x] **Call depth**: Fix off-by-one (allow depth 1024)
- [x] **BN256 precompiles**: ADD (0x06), MUL (0x07), PAIRING (0x08) — custom implementation on mini-gmp (EIP-196/197, EIP-1108 gas)

## External Dependencies

| Library | For | Install | Status |
|---------|-----|---------|--------|
| OpenSSL | SHA-256, RIPEMD-160 | `apt install libssl-dev` | **Installed** |
| libsecp256k1 | ECRECOVER | `apt install libsecp256k1-dev` | **Installed** |
| mini-gmp | MODEXP, BN256 | vendored in evm/vendor/ | **Installed** |
