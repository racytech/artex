# VM Design — EOF-Native Virtual Machine on Verkle State

## Overview

A stack-based virtual machine designed from scratch for a Verkle-native blockchain.
Takes EOF (EVM Object Format, EIP-3540/7692) as the base instruction set, removes all
legacy EVM baggage, and integrates witness-based gas metering for Verkle state access.

Same 256-bit word size and general execution model as the EVM, but with structured
bytecode, deploy-time validation, and a flat state tree.

```
┌──────────────────────────────────────────────────┐
│                  Transaction                      │
└──────────┬───────────────────────────────────────┘
           │
           ▼
┌──────────────────────────────────────────────────┐
│              VM Interpreter                       │
│  ┌──────────┐  ┌──────────┐  ┌────────────────┐ │
│  │  Stack    │  │  Memory  │  │  Return Stack  │ │
│  │ (1024)   │  │ (linear) │  │  (CALLF/RETF)  │ │
│  └──────────┘  └──────────┘  └────────────────┘ │
└──────────┬───────────────────────────────────────┘
           │
           ▼
┌──────────────────────────────────────────────────┐
│            Witness Manager                        │
│  Tracks (stem, leaf) access for gas charging      │
│  Replaces EIP-2929 warm/cold address/slot logic   │
└──────────┬───────────────────────────────────────┘
           │
           ▼
┌──────────────────────────────────────────────────┐
│           State (Verkle Tree)                     │
│  Flat key-value: pedersen(addr, idx) || sub_idx   │
│  Accounts, storage, code chunks all in one tree   │
└──────────────────────────────────────────────────┘
```

---

## 1. EOF Container Format

Every deployed contract is a validated EOF container. No raw bytecode.

```
┌─────────────────────────────────────────┐
│ Magic: 0xEF00                           │  2 bytes
│ Version: 0x01                           │  1 byte
├─────────────────────────────────────────┤
│ Header                                  │
│  ├─ type_section_size                   │  kind=1, 2 bytes
│  ├─ code_section_count                  │  kind=2, 2 bytes
│  ├─ code_section_sizes[count]           │  2 bytes each
│  ├─ container_section_count (optional)  │  kind=3, 2 bytes
│  ├─ container_section_sizes[] (opt)     │  2 bytes each
│  ├─ data_section_size                   │  kind=4, 2 bytes
│  └─ terminator: 0x00                    │  1 byte
├─────────────────────────────────────────┤
│ Type Section                            │
│  For each code section:                 │
│  ├─ inputs:  uint8   (stack items in)   │
│  ├─ outputs: uint8   (stack items out)  │
│  ├─ max_stack_height: uint16            │
│  (function 0: inputs=0, outputs=0x80)   │
├─────────────────────────────────────────┤
│ Code Section 0  (entry point)           │
│ Code Section 1  (function 1)            │
│ ...                                     │
│ Code Section N  (function N)            │
├─────────────────────────────────────────┤
│ Container Section 0 (optional)          │
│  (nested EOF for CREATE)                │
├─────────────────────────────────────────┤
│ Data Section                            │
│  (constants, constructor args, etc.)    │
└─────────────────────────────────────────┘
```

### Validation (at deploy time, once)

1. Magic + version check
2. Header well-formedness (section counts, sizes, no overflow)
3. For each code section:
   - All opcodes are known and valid
   - All RJUMP/RJUMPI/RJUMPV targets land on opcode boundaries
   - Stack height is statically determinable on every path
   - Stack underflow/overflow impossible (proven via abstract interpretation)
   - Every code path terminates (STOP, RETURN, REVERT, RETF, or JUMPF)
   - No unreachable code
4. CALLF/JUMPF function indices are in bounds
5. Type section matches actual stack behavior
6. No legacy opcodes present

If validation passes, the contract is deployed. The VM trusts the validation
and skips runtime stack/jump checks entirely.

---

## 2. Instruction Set

### 2.1 Removed (vs EVM)

| Opcode       | Reason                                          |
|--------------|-------------------------------------------------|
| JUMP         | Replaced by RJUMP (static, validated)           |
| JUMPI        | Replaced by RJUMPI                              |
| JUMPDEST     | Not needed — no dynamic jumps                   |
| PC           | Not meaningful with relative jumps              |
| CODECOPY     | Code is not data. Use DATALOAD/DATACOPY         |
| CODESIZE     | Use DATASIZE for data section length             |
| EXTCODECOPY  | Security hazard, use EXTCODEHASH                |
| EXTCODESIZE  | Removed — don't branch on code size             |
| SELFDESTRUCT | Not included                                    |
| CALLCODE     | Replaced by DELEGATECALL long ago               |
| GAS          | Prevents gas introspection games                |
| CREATE       | Use EOFCREATE (deploys validated containers)    |
| CREATE2      | Use EOFCREATE with salt                         |

### 2.2 New / Modified Opcodes

#### Control Flow

| Opcode       | Byte | Stack         | Description                         |
|--------------|------|---------------|-------------------------------------|
| RJUMP        | 0xE0 | => (none)     | PC += imm16 (signed relative jump)  |
| RJUMPI       | 0xE1 | cond =>       | If cond != 0: PC += imm16           |
| RJUMPV       | 0xE2 | idx =>        | Jump table: PC += table[idx]        |
| CALLF        | 0xE3 | [in] => [out] | Call function #imm16                |
| RETF         | 0xE4 | [out] =>      | Return from function                |
| JUMPF        | 0xE5 | [in] => *     | Tail-call function #imm16           |

#### Data Access

| Opcode       | Byte | Stack             | Description                       |
|--------------|------|-------------------|-----------------------------------|
| DATALOAD     | 0xD0 | offset => word     | Load 32 bytes from data section   |
| DATALOADN    | 0xD1 | => word            | Load 32 bytes at imm16 offset     |
| DATASIZE     | 0xD2 | => size            | Push data section size             |
| DATACOPY     | 0xD3 | dst src len =>     | Copy from data section to memory  |

#### Contract Creation

| Opcode       | Byte | Stack                          | Description                    |
|--------------|------|--------------------------------|--------------------------------|
| EOFCREATE    | 0xEC | val salt din dsz idx => addr   | Deploy container[idx] with EOF |
| RETURNCONTRACT | 0xEE | offset size =>              | Return from init code          |

#### Call

| Opcode         | Byte | Stack                               | Description          |
|----------------|------|-------------------------------------|----------------------|
| EXTCALL        | 0xF8 | addr val din dsz => status          | External call        |
| EXTDELEGATECALL| 0xF9 | addr din dsz => status              | Delegate call        |
| EXTSTATICCALL  | 0xFB | addr din dsz => status              | Static call          |

Return values from EXT*CALL:
- 0 = success
- 1 = revert
- 2 = failure (out of gas, etc.)

No gas parameter — caller forwards all available gas minus retention (for post-call ops).
RETURNDATA* opcodes used to retrieve output.

#### Stack Manipulation

| Opcode       | Byte | Stack            | Description                        |
|--------------|------|------------------|------------------------------------|
| DUPN         | 0xE6 | => stack[N]      | Dup item at depth imm8             |
| SWAPN        | 0xE7 | (swap top, N+1)  | Swap top with item at depth imm8+1 |
| EXCHANGE     | 0xE8 | (swap N, M)      | Swap two non-top items             |

Replace DUP1-DUP16 and SWAP1-SWAP16 with unbounded (up to 256 depth) versions.

#### Kept from EVM (unchanged semantics)

- Arithmetic: ADD, MUL, SUB, DIV, SDIV, MOD, SMOD, ADDMOD, MULMOD, EXP, SIGNEXTEND
- Comparison: LT, GT, SLT, SGT, EQ, ISZERO
- Bitwise: AND, OR, XOR, NOT, BYTE, SHL, SHR, SAR
- Keccak: KECCAK256
- Environmental: ADDRESS, BALANCE, ORIGIN, CALLER, CALLVALUE, CALLDATALOAD,
  CALLDATASIZE, CALLDATACOPY, RETURNDATASIZE, RETURNDATACOPY, RETURNDATALOAD,
  EXTCODEHASH, BLOCKHASH, COINBASE, TIMESTAMP, NUMBER, PREVRANDAO, GASLIMIT,
  CHAINID, SELFBALANCE, BASEFEE, BLOBHASH, BLOBBASEFEE
- Memory: MLOAD, MSTORE, MSTORE8, MSIZE, MCOPY
- Storage: SLOAD, SSTORE, TLOAD, TSTORE
- Logging: LOG0-LOG4
- Control: STOP, RETURN, REVERT, INVALID

---

## 3. Gas Model — Witness-Based (Verkle)

### 3.1 Core Constants

```
WITNESS_BRANCH_COST     1900    First access to a new stem
WITNESS_CHUNK_COST       200    First access to a new leaf within a stem

GAS_VERY_LOW               3    Base arithmetic tier
GAS_LOW                    5
GAS_MID                    8
GAS_MEMORY_WORD            3    Per-word memory expansion
GAS_MEMORY_QUAD          512    Quadratic divisor
GAS_COPY_WORD              3    Per-word copy (CALLDATACOPY, MCOPY, etc.)
GAS_LOG_TOPIC            375
GAS_LOG_DATA               8    Per byte

GAS_SSTORE_SET         20000    Storage: zero -> non-zero
GAS_SSTORE_RESET        5000    Storage: non-zero -> non-zero
GAS_SSTORE_CLEAR_REFUND 4800    Refund: non-zero -> zero

GAS_CALL_STIPEND        2300    Minimum gas passed to callee
GAS_CALL_VALUE          9000    Sending non-zero value
GAS_NEW_ACCOUNT        25000    Creating a new account via value transfer
```

### 3.2 Witness Charging Rules

Every state access resolves to a `(stem[31], sub_index)` pair in the Verkle tree.
The witness manager tracks what has been accessed in the current transaction:

```c
typedef struct {
    bool stem_accessed;     // branch cost already charged
    bool leaf_accessed[256]; // per-leaf chunk cost
} witness_stem_t;
```

Gas for any state read/write:

```
cost = 0
if (!stem_accessed[stem])  { cost += WITNESS_BRANCH_COST; mark_stem(stem); }
if (!leaf_accessed[stem][sub]) { cost += WITNESS_CHUNK_COST;  mark_leaf(stem, sub); }
// + operation-specific cost (SSTORE set/reset, etc.)
```

### 3.3 Verkle Key Layout

```
Account header:  stem = pedersen(address, 0)
  sub 0:  version          (reserved, currently 0)
  sub 1:  balance
  sub 2:  nonce
  sub 3:  code_hash
  sub 4:  code_size

Code chunks:     stem = pedersen(address, 0),  sub 128..255 → first 128 chunks
                 stem = pedersen(address, 1),  sub 0..255   → next 256 chunks
                 stem = pedersen(address, 2),  sub 0..255   → next 256 chunks
                 ...
                 Each chunk = 31 bytes of code + 1 byte prefix

Storage:         stem = pedersen(address, 64 + slot/256)
                 sub  = slot % 256
                 (slots 0-255 in header stem at sub 64..319 won't fit,
                  so slots 0-63 map to header stem sub 64..127)
```

### 3.4 Code Chunk Witnessing

The interpreter charges for code chunks as execution reaches them:

```
Before fetching each instruction:
  chunk_id = pc / 31
  if chunk_id changed since last instruction:
      stem, sub = code_chunk_key(address, chunk_id)
      charge witness cost for (stem, sub)
```

Combined with EOF function sections, a CALLF only witnesses the target function's
chunks, not the entire contract.

### 3.5 Access List Pre-warming

Transactions can include an access list of `(address, [storage_keys])` pairs.
These are pre-witnessed (branch + chunk costs charged upfront at a discount)
before execution begins, so subsequent accesses are free.

---

## 4. Execution Model

### 4.1 VM Context

```c
typedef struct vm vm_t;

typedef struct {
    // Container
    eof_container_t *container;     // validated EOF bytecode

    // Execution state
    uint32_t         pc;            // program counter (within current code section)
    uint16_t         current_func;  // active code section index
    bool             stopped;       // halt flag

    // Stack
    uint256_t       *stack;         // operand stack (pre-allocated to max_stack_height)
    uint16_t         sp;            // stack pointer

    // Return stack (for CALLF/RETF)
    return_frame_t  *return_stack;  // function return addresses
    uint16_t         rsp;           // return stack pointer

    // Memory
    vm_memory_t     *memory;        // linear byte-addressable memory

    // Gas
    uint64_t         gas_left;
    uint64_t         gas_refund;

    // Message (call context)
    vm_message_t     msg;           // caller, value, calldata, etc.

    // State access
    vm_state_t      *state;         // Verkle state interface
    witness_t       *witness;       // witness tracking for gas

    // Return data from last EXT*CALL
    uint8_t         *return_data;
    size_t           return_data_size;

    // Call depth
    uint16_t         depth;
} vm_t;
```

### 4.2 Return Stack Frame

```c
typedef struct {
    uint16_t func_id;       // code section to return to
    uint32_t pc;            // return address within that section
    uint16_t stack_height;  // caller's stack height (for validation in debug builds)
} return_frame_t;
```

### 4.3 Interpreter Loop

The interpreter is a simple fetch-decode-execute loop. Because EOF validation
guarantees no stack underflow/overflow and no invalid jumps, the hot path is
clean:

```c
vm_result_t vm_interpret(vm_t *vm) {
    const uint8_t *code = vm->container->sections[vm->current_func].code;

    for (;;) {
        // Code chunk witness charge (if new chunk boundary crossed)
        witness_charge_code_chunk(vm);

        uint8_t op = code[vm->pc++];

        switch (op) {
        case OP_STOP:
            return result_success(vm);

        case OP_ADD: {
            uint256_t *a = &vm->stack[vm->sp];
            uint256_t *b = &vm->stack[vm->sp - 1];
            uint256_add(a, b, b);  // b = a + b
            vm->sp--;
            break;
        }

        case OP_RJUMP: {
            int16_t offset = read_i16(code, vm->pc);
            vm->pc += 2 + offset;
            break;
        }

        case OP_RJUMPI: {
            int16_t offset = read_i16(code, vm->pc);
            vm->pc += 2;
            if (!uint256_is_zero(&vm->stack[vm->sp--])) {
                vm->pc += offset;
            }
            break;
        }

        case OP_CALLF: {
            uint16_t func_id = read_u16(code, vm->pc);
            vm->pc += 2;
            vm->return_stack[vm->rsp++] = (return_frame_t){
                .func_id = vm->current_func,
                .pc = vm->pc,
                .stack_height = vm->sp,
            };
            vm->current_func = func_id;
            code = vm->container->sections[func_id].code;
            vm->pc = 0;
            break;
        }

        case OP_RETF: {
            return_frame_t frame = vm->return_stack[--vm->rsp];
            vm->current_func = frame.func_id;
            code = vm->container->sections[frame.func_id].code;
            vm->pc = frame.pc;
            break;
        }

        case OP_SLOAD: {
            uint256_t *key = &vm->stack[vm->sp];
            uint64_t gas = witness_storage_cost(vm, &vm->msg.recipient, key);
            if (!vm_use_gas(vm, gas)) return result_oog(vm);
            *key = vm_state_get_storage(vm->state, &vm->msg.recipient, key);
            break;
        }

        case OP_EXTCALL: {
            // ... pop addr, value, input_offset, input_size
            // Forward all gas minus retention
            // Push 0 (success), 1 (revert), or 2 (failure)
            break;
        }

        // ... remaining opcodes
        }
    }
}
```

### 4.4 No Runtime Safety Checks Needed

Because EOF validation is mandatory at deploy time:

| Check                    | EVM (runtime) | VM (deploy-time) |
|--------------------------|---------------|------------------|
| Stack underflow          | Every opcode  | Validated once   |
| Stack overflow           | Every opcode  | Validated once   |
| Valid jump target        | Every JUMP    | All targets static, validated |
| Opcode exists            | Every fetch   | Validated once   |
| Code bounds              | Every PC++    | Validated once   |
| Unreachable code         | Never checked | Rejected at deploy |

The only runtime checks that remain:
- Gas metering (unavoidable)
- Memory expansion bounds
- Call depth limit (1024)
- Integer overflow in memory offset calculations

---

## 5. State Interface

The VM talks to state through an opaque interface, similar to the current
`evm_state.h` but adapted for Verkle:

```c
// vm_state.h — Verkle-native state interface

typedef struct vm_state vm_state_t;

// Account fields
uint256_t vm_state_get_balance(vm_state_t *s, const address_t *addr);
void      vm_state_set_balance(vm_state_t *s, const address_t *addr, const uint256_t *bal);
uint64_t  vm_state_get_nonce(vm_state_t *s, const address_t *addr);
void      vm_state_set_nonce(vm_state_t *s, const address_t *addr, uint64_t nonce);
hash_t    vm_state_get_code_hash(vm_state_t *s, const address_t *addr);

// Storage (same API, different key derivation underneath)
uint256_t vm_state_get_storage(vm_state_t *s, const address_t *addr, const uint256_t *key);
void      vm_state_set_storage(vm_state_t *s, const address_t *addr,
                                const uint256_t *key, const uint256_t *val);

// Transient storage (transaction-scoped, no witness cost)
uint256_t vm_state_tload(vm_state_t *s, const address_t *addr, const uint256_t *key);
void      vm_state_tstore(vm_state_t *s, const address_t *addr,
                           const uint256_t *key, const uint256_t *val);

// Code access (chunk-based for Verkle)
bool      vm_state_get_code_chunk(vm_state_t *s, const address_t *addr,
                                   uint32_t chunk_id, uint8_t out[31], uint8_t *out_len);

// EOF container loading (validated at deploy, cached)
const eof_container_t *vm_state_get_container(vm_state_t *s, const address_t *addr);

// Snapshot / Revert
uint32_t vm_state_snapshot(vm_state_t *s);
void     vm_state_revert(vm_state_t *s, uint32_t snap_id);
```

Key difference from `evm_state.h`: no `get_code_ptr` that returns the full
bytecode blob. Code is accessed chunk-by-chunk (31 bytes at a time), matching
how it lives in the Verkle tree. The `eof_container_t` is assembled from
chunks and cached in memory on first access.

---

## 6. Witness Manager

Replaces the EIP-2929 access list tracking:

```c
// witness.h

typedef struct witness witness_t;

witness_t *witness_create(void);
void       witness_destroy(witness_t *w);
void       witness_clear(witness_t *w);  // reset between transactions

// Core API: charge for accessing a (stem, sub_index) pair
// Returns the gas cost (0 if already witnessed)
uint64_t witness_charge(witness_t *w, const uint8_t stem[31], uint8_t sub_index);

// Convenience: derive stem+sub from address and field, then charge
uint64_t witness_charge_account_field(witness_t *w, const address_t *addr, uint8_t field);
uint64_t witness_charge_storage_slot(witness_t *w, const address_t *addr, const uint256_t *slot);
uint64_t witness_charge_code_chunk_id(witness_t *w, const address_t *addr, uint32_t chunk_id);

// Pre-warm from access list (transaction-level)
void witness_prewarm_account(witness_t *w, const address_t *addr);
void witness_prewarm_slot(witness_t *w, const address_t *addr, const uint256_t *slot);

// Snapshot/revert for witness state (parallel to state snapshots)
uint32_t witness_snapshot(witness_t *w);
void     witness_revert(witness_t *w, uint32_t snap_id);
```

Internally, the witness manager uses an ART (or hash map) keyed by `stem[31]`,
where each entry is a 256-bit bitmap of accessed leaves.

---

## 7. EOF Validation

Deploy-time validation is the foundation that allows the runtime to skip checks.
Validation is performed once and is not part of the hot path.

```c
// eof.h

typedef struct {
    uint8_t  inputs;
    uint8_t  outputs;         // 0x80 = non-returning (entry point)
    uint16_t max_stack_height;
    uint8_t *code;
    uint32_t code_size;
} eof_func_t;

typedef struct {
    uint8_t      version;
    uint16_t     num_functions;
    eof_func_t  *functions;
    uint8_t     *data;
    uint32_t     data_size;
    // Nested containers (for EOFCREATE)
    uint16_t     num_containers;
    eof_container_t **containers;
} eof_container_t;

typedef enum {
    EOF_VALID = 0,
    EOF_INVALID_MAGIC,
    EOF_INVALID_VERSION,
    EOF_INVALID_HEADER,
    EOF_INVALID_TYPE_SECTION,
    EOF_TRUNCATED_INSTRUCTION,
    EOF_INVALID_RJUMP_TARGET,
    EOF_STACK_HEIGHT_MISMATCH,
    EOF_STACK_UNDERFLOW,
    EOF_STACK_OVERFLOW,
    EOF_UNREACHABLE_CODE,
    EOF_INVALID_FUNCTION_INDEX,
    EOF_INVALID_TERMINATION,
    EOF_UNKNOWN_OPCODE,
} eof_validation_error_t;

// Validate raw bytes, return parsed container or error
eof_validation_error_t eof_validate(const uint8_t *bytecode, size_t len,
                                     eof_container_t **out);

void eof_container_free(eof_container_t *c);
```

### Stack Validation Algorithm

For each code section, perform abstract interpretation:
1. Track stack height at every instruction offset
2. At branches (RJUMPI), both paths must agree on stack height at join points
3. At CALLF, consume inputs and push outputs per type section
4. At section end, verify max_stack_height matches declared value
5. If any path allows underflow or exceeds 1024, reject

This is a single forward pass with a work queue for branch targets — O(n) in
code size.

---

## 8. Directory Structure

```
vm/
├── DESIGN.md                   ← this document
├── include/
│   ├── vm.h                    ← core VM types, context, result
│   ├── vm_memory.h             ← linear memory
│   ├── vm_state.h              ← Verkle state interface
│   ├── eof.h                   ← EOF container, validation
│   ├── witness.h               ← witness manager
│   ├── gas.h                   ← gas constants and helpers
│   ├── interpreter.h           ← interpreter entry point
│   └── opcodes/
│       ├── arithmetic.h
│       ├── comparison.h
│       ├── control.h           ← RJUMP, RJUMPI, RJUMPV, CALLF, RETF, JUMPF
│       ├── memory.h            ← MLOAD, MSTORE, MCOPY, etc.
│       ├── storage.h           ← SLOAD, SSTORE, TLOAD, TSTORE
│       ├── environmental.h     ← ADDRESS, BALANCE, CALLER, CALLVALUE, etc.
│       ├── data.h              ← DATALOAD, DATALOADN, DATASIZE, DATACOPY
│       ├── call.h              ← EXTCALL, EXTDELEGATECALL, EXTSTATICCALL
│       ├── create.h            ← EOFCREATE, RETURNCONTRACT
│       ├── crypto.h            ← KECCAK256
│       ├── block.h             ← BLOCKHASH, COINBASE, TIMESTAMP, etc.
│       ├── logging.h           ← LOG0-LOG4
│       └── stack.h             ← DUPN, SWAPN, EXCHANGE, PUSHn, POP
├── src/
│   ├── vm.c                    ← VM lifecycle, call/create
│   ├── vm_memory.c
│   ├── eof.c                   ← parsing + validation
│   ├── witness.c               ← witness tracking
│   ├── gas.c
│   ├── interpreter.c           ← main dispatch loop
│   └── opcodes/
│       ├── arithmetic.c
│       ├── comparison.c
│       ├── control.c
│       ├── memory.c
│       ├── storage.c
│       ├── environmental.c
│       ├── data.c
│       ├── call.c
│       ├── create.c
│       ├── crypto.c
│       ├── block.c
│       ├── logging.c
│       └── stack.c
└── tests/
    ├── test_eof_validation.c   ← container parsing + validation edge cases
    ├── test_interpreter.c      ← opcode execution
    ├── test_witness.c          ← witness charging correctness
    ├── test_memory.c
    └── test_gas.c
```

---

## 9. Key Differences from EVM Summary

| Aspect              | EVM                           | VM (this design)                |
|---------------------|-------------------------------|----------------------------------|
| Bytecode format     | Flat, unstructured            | EOF containers, validated        |
| Jump targets        | Dynamic (JUMP/JUMPI)          | Static (RJUMP/RJUMPI/RJUMPV)    |
| Functions           | Convention (JUMP-based)       | First-class (CALLF/RETF/JUMPF)  |
| Stack safety        | Runtime checks every opcode   | Deploy-time validation           |
| DUP/SWAP            | DUP1-16, SWAP1-16             | DUPN/SWAPN/EXCHANGE (up to 256)  |
| Code access         | CODECOPY (code is data)       | DATALOAD/DATACOPY (separate)     |
| External calls      | CALL(gas, addr, val, ...)     | EXTCALL(addr, val, din, dsz)     |
| Gas for calls       | Caller specifies gas          | Forward all minus retention      |
| Contract creation   | CREATE/CREATE2 (raw init)     | EOFCREATE (validated containers) |
| State commitment    | MPT (keccak + RLP)            | Verkle (pedersen + IPA)          |
| State access gas    | EIP-2929 warm/cold            | Witness-based (stem/leaf)        |
| Code storage        | Single blob by hash           | 31-byte chunks in Verkle tree    |
| GAS opcode          | Available                     | Removed                          |
| SELFDESTRUCT        | Available (crippled)          | Not included                     |

---

## 10. EOF-Enabled Optimization Opportunities

EOF's deploy-time validation gives the VM a complete static picture of every
contract before execution begins. In legacy EVM, nothing can be analyzed until
you run it. With EOF, you can analyze everything at deploy time and build
execution plans. Five concrete optimizations follow.

### 10.1 Verkle Witness Prefetching

Static analysis of an EOF container can identify which storage keys and account
fields a function *will* touch on a given code path. The VM can pre-fetch Verkle
proofs and tree nodes before execution begins, turning serial state lookups into
a single batched I/O operation.

```
Deploy-time analysis:
  code section 0 → SLOAD key X, SLOAD key Y, BALANCE addr Z
  code section 1 → SSTORE key W (via CALLF from section 0)

Before executing:
  prefetch(stem(addr, X), stem(addr, Y), stem(Z, balance), stem(addr, W))
```

The static CFG lets us build a conservative over-approximation of accessed keys.
For conditional branches (RJUMPI), both sides' accesses are included. The cost
of over-fetching is low (unused proofs are discarded), while the latency
reduction from batching is significant.

### 10.2 Transaction-Level Parallelism

With the full CFG and data flow known, static analysis can compute a
conservative set of state keys each contract may access (read-set and
write-set). The block builder or executor can then:

1. Analyze each transaction's target contract(s)
2. Build per-transaction read/write sets (addresses, storage slots)
3. Schedule non-conflicting transactions for parallel execution
4. Serialize only those with overlapping write-sets

```
tx1: contract A reads  [slot 1, 2],  writes [slot 3]
tx2: contract B reads  [slot 5],     writes [slot 6]
tx3: contract A reads  [slot 3],     writes [slot 7]

→ tx1 and tx2 execute in parallel (no overlap)
→ tx3 waits for tx1 (reads slot 3 which tx1 writes)
```

This is impractical with legacy EVM because the access set is unknowable until
execution. EOF makes it statically derivable (with over-approximation for
branches and dynamic storage keys).

### 10.3 JIT Compilation

EOF provides exactly the guarantees a JIT compiler needs:

- **Static stack heights** at every instruction → direct register/local mapping
- **No dynamic jumps** → the CFG is known, basic blocks are fixed
- **Structured functions** (CALLF/RETF) → standard calling conventions
- **No JUMPDEST scanning** → code can be compiled section-by-section
- **Max stack height declared** → stack frame size known at compile time

A straightforward compilation pipeline:

```
EOF bytecode
  → basic block identification (from RJUMP/RJUMPI/RJUMPV targets)
  → SSA conversion (stack → virtual registers)
  → native code generation (x86-64 / aarch64)
  → cache compiled code per code_hash
```

Hot contracts (DEX routers, token transfers) would execute at near-native speed
after the first invocation. Cold contracts fall back to the interpreter.

### 10.4 Speculative State Access

At conditional branches (RJUMPI, RJUMPV), the static CFG reveals both paths'
state accesses. The VM can speculatively initiate state reads for both sides
while the branch condition is being computed:

```
instruction stream:
  SLOAD key_A        ← both paths need this
  PUSH condition
  RJUMPI +offset
    SLOAD key_B      ← taken path
    ...
  else:
    SLOAD key_C      ← fallthrough path
    ...

At RJUMPI: speculatively prefetch both key_B and key_C.
One fetch will be wasted, but the other saves a full tree traversal.
```

For Verkle trees where each traversal involves IPA proof verification, the
latency savings from speculative prefetching compound across nested branches.

### 10.5 Batched Verkle Operations

Instead of paying the full tree-traversal cost for each state access
individually, the VM can batch all accesses within a basic block (or even an
entire function call) into a single tree operation:

```
Without batching (sequential):
  SLOAD key_1  →  traverse tree  →  result_1
  SLOAD key_2  →  traverse tree  →  result_2
  SLOAD key_3  →  traverse tree  →  result_3
  Total: 3 × tree traversal latency

With batching (parallel):
  Collect {key_1, key_2, key_3} from static analysis
  →  single multi-get on tree  →  {result_1, result_2, result_3}
  Total: 1 × tree traversal latency (amortized)
```

This applies to both reads (SLOAD, BALANCE) and witness charging. The witness
manager can pre-warm all stems for a function in a single pass rather than
charging one-by-one as execution hits each opcode.

Combined with 10.1 (prefetching), the VM can overlap tree I/O with computation:
execute arithmetic while the next batch of state accesses is in flight.

---

## 11. Implementation Order

Phase 1 — Foundation (complete):
  1. `eof.c` — EOF parsing and validation (most critical, everything depends on it)
  2. `vm.h` / `vm.c` — core types, context creation, lifecycle
  3. `vm_memory.c` — linear memory (same as EVM, can reuse)
  4. `gas.c` — constants and helpers

Phase 2 — Interpreter:
  5. `interpreter.c` — dispatch loop
  6. `opcodes/arithmetic.c` — can reuse uint256 ops from EVM
  7. `opcodes/comparison.c` — same
  8. `opcodes/stack.c` — DUPN/SWAPN/EXCHANGE + PUSHn/POP
  9. `opcodes/control.c` — RJUMP/RJUMPI/RJUMPV/CALLF/RETF/JUMPF
  10. `opcodes/memory.c` — MLOAD/MSTORE/MCOPY/MSIZE
  11. `opcodes/data.c` — DATALOAD/DATALOADN/DATASIZE/DATACOPY

Phase 3 — State:
  12. `vm_state.h` — Verkle state interface
  13. `witness.c` — witness tracking + gas charging
  14. `opcodes/storage.c` — SLOAD/SSTORE/TLOAD/TSTORE
  15. `opcodes/environmental.c` — ADDRESS/BALANCE/CALLER/etc.
  16. `opcodes/block.c` — BLOCKHASH/COINBASE/TIMESTAMP/etc.

Phase 4 — Calls & Creation:
  17. `opcodes/call.c` — EXTCALL/EXTDELEGATECALL/EXTSTATICCALL
  18. `opcodes/create.c` — EOFCREATE/RETURNCONTRACT
  19. `opcodes/logging.c` — LOG0-LOG4
  20. `opcodes/crypto.c` — KECCAK256
