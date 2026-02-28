# EVM Implementation TODO

## Phase 1: Core Foundation ✓ (Ready to start)
- [ ] EVM context structures (evm.h)
  - [ ] Execution context (caller, callee, value, gas, input data)
  - [ ] Block context (number, timestamp, coinbase, difficulty, gas limit)
  - [ ] Transaction context (origin, gas price)
  - [ ] Return data handling
- [ ] Stack implementation (evm_stack.h/c)
  - [ ] 256-bit word stack (max 1024 items)
  - [ ] Push/pop operations with overflow checks
  - [ ] Dup operations (DUP1-DUP16)
  - [ ] Swap operations (SWAP1-SWAP16)
- [ ] Memory implementation (evm_memory.h/c)
  - [ ] Expandable byte array
  - [ ] Gas cost calculation for expansion
  - [ ] MLOAD, MSTORE, MSTORE8 operations
  - [ ] MSIZE opcode

## Phase 2: Basic Opcodes
- [ ] Arithmetic opcodes (evm_arithmetic.c)
  - [ ] ADD, SUB, MUL, DIV, SDIV, MOD, SMOD
  - [ ] ADDMOD, MULMOD
  - [ ] EXP (exponentiation)
  - [ ] SIGNEXTEND
- [ ] Comparison & bitwise opcodes (evm_comparison.c)
  - [ ] LT, GT, SLT, SGT, EQ, ISZERO
  - [ ] AND, OR, XOR, NOT
  - [ ] BYTE, SHL, SHR, SAR
- [ ] Stack manipulation (evm_stack.c)
  - [ ] POP
  - [ ] PUSH1-PUSH32
  - [ ] DUP1-DUP16
  - [ ] SWAP1-SWAP16

## Phase 3: Control Flow & Program Counter
- [ ] Control flow opcodes (evm_control.c)
  - [ ] PC (program counter)
  - [ ] JUMP, JUMPI
  - [ ] JUMPDEST validation
  - [ ] Jump destination analysis (build valid jump map)
  - [ ] STOP, RETURN, REVERT opcodes

## Phase 4: StateDB Integration
- [ ] Storage opcodes (evm_storage.c)
  - [ ] SLOAD (load from storage)
  - [ ] SSTORE (store to storage)
  - [ ] Gas cost for SSTORE (EIP-2200 net gas metering)
  - [ ] Warm/cold storage access tracking
- [ ] Account opcodes (evm_account.c)
  - [ ] BALANCE (get account balance)
  - [ ] SELFBALANCE (get current contract balance)
  - [ ] EXTCODESIZE (get code size)
  - [ ] EXTCODECOPY (copy external code)
  - [ ] EXTCODEHASH (get code hash)

## Phase 5: Blockchain Context Opcodes
- [ ] Block context opcodes (evm_block.c)
  - [ ] BLOCKHASH (get block hash by number)
  - [ ] COINBASE (get block beneficiary)
  - [ ] TIMESTAMP (get block timestamp)
  - [ ] NUMBER (get block number)
  - [ ] DIFFICULTY / PREVRANDAO (post-merge)
  - [ ] GASLIMIT (get block gas limit)
  - [ ] CHAINID (get chain ID)
  - [ ] BASEFEE (EIP-1559)
- [ ] Transaction context opcodes
  - [ ] ORIGIN (transaction origin)
  - [ ] GASPRICE (transaction gas price)
  - [ ] CALLER (message sender)
  - [ ] CALLVALUE (message value)
  - [ ] CALLDATALOAD, CALLDATASIZE, CALLDATACOPY
  - [ ] CODESIZE, CODECOPY
  - [ ] RETURNDATASIZE, RETURNDATACOPY

## Phase 6: Gas Calculation
- [ ] Gas cost tables (evm_gas.h)
  - [ ] Base costs for all opcodes
  - [ ] Dynamic costs (memory expansion, SSTORE, CALL, etc.)
  - [ ] EIP-2929 access list costs (warm/cold)
- [ ] Gas tracking (evm_gas.c)
  - [ ] Deduct gas before each operation
  - [ ] Memory expansion cost calculation
  - [ ] Gas refunds (SSTORE, SELFDESTRUCT)
  - [ ] Out-of-gas handling

## Phase 7: Message Calls (Complex)
- [ ] Call operations (evm_call.c)
  - [ ] CALL (external call with value transfer)
  - [ ] CALLCODE (deprecated, call with caller context)
  - [ ] DELEGATECALL (call with caller context, no value)
  - [ ] STATICCALL (call without state modification)
  - [ ] Gas stipend calculation (2300 gas for value transfer)
  - [ ] Call depth limit (1024)
  - [ ] Call stack management
  - [ ] Return data handling
- [ ] Contract creation (evm_create.c)
  - [ ] CREATE (create contract at computed address)
  - [ ] CREATE2 (create contract at deterministic address)
  - [ ] Constructor execution
  - [ ] Init code vs runtime code
  - [ ] Contract address calculation
  - [ ] Deployment gas costs

## Phase 8: Advanced Features
- [ ] Logs and events (evm_log.c)
  - [ ] LOG0, LOG1, LOG2, LOG3, LOG4
  - [ ] Topic handling
  - [ ] Log data storage
- [ ] Precompiled contracts (evm_precompiled.c)
  - [ ] 0x01: ECRECOVER (signature recovery)
  - [ ] 0x02: SHA256
  - [ ] 0x03: RIPEMD160
  - [ ] 0x04: IDENTITY (data copy)
  - [ ] 0x05: MODEXP (modular exponentiation)
  - [ ] 0x06: ECADD (elliptic curve addition)
  - [ ] 0x07: ECMUL (elliptic curve multiplication)
  - [ ] 0x08: ECPAIRING (elliptic curve pairing)
  - [ ] 0x09: BLAKE2F
- [ ] SELFDESTRUCT opcode
  - [ ] Account deletion
  - [ ] Beneficiary balance transfer
  - [ ] Gas refund (deprecated in newer EIPs)

## Phase 9: EIP Implementations
- [ ] EIP-1559: Fee market (BASEFEE opcode)
- [ ] EIP-2929: Gas cost increases for state access
- [ ] EIP-2930: Access lists
- [ ] EIP-3198: BASEFEE opcode
- [ ] EIP-3529: Reduced refunds
- [ ] EIP-3541: Reject code starting with 0xEF
- [ ] EIP-3855: PUSH0 opcode
- [ ] EIP-3860: Limit and meter initcode
- [ ] EIP-4399: PREVRANDAO (replacing DIFFICULTY)

## Phase 10: Interpreter & Testing
- [ ] Main interpreter loop (evm_interpreter.c)
  - [ ] Opcode dispatch
  - [ ] Execution loop
  - [ ] Exception handling
  - [ ] Result return (success/revert/error)
- [ ] Comprehensive test suite
  - [ ] Unit tests for each opcode
  - [ ] Integration tests with StateDB
  - [ ] Ethereum test vectors (ethereum/tests repository)
  - [ ] Gas cost validation
  - [ ] Edge case testing (stack overflow, out-of-gas, etc.)

## Phase 11: Optimizations (Future)
- [ ] Jump destination caching
- [ ] Hot path optimization
- [ ] Stack depth analysis
- [ ] Inline common opcode sequences
- [ ] SIMD optimizations for arithmetic

## Phase 12: Debugging & Tooling (Future)
- [ ] Execution tracer
- [ ] Step-by-step debugger
- [ ] Gas profiler
- [ ] Opcode disassembler
- [ ] Contract ABI decoder

---

## Implementation Strategy

**Start Simple, Build Up:**
1. Phase 1-3: Get basic execution working (stack, memory, arithmetic, control flow)
2. Phase 4: Integrate with StateDB (storage operations)
3. Phase 5-6: Add context opcodes and gas metering
4. Phase 7: Implement CALL family (most complex)
5. Phase 8-9: Add advanced features and EIP compliance
6. Phase 10: Polish and comprehensive testing

**Current State:**
- ✅ StateDB fully implemented and tested
- ✅ MPT for state root computation working
- ✅ State cache and journal ready
- ⏸️ Ready to begin EVM Phase 1

**Next Immediate Step:**
Create `evm/include/evm.h` with core context structures and begin stack implementation.
