/**
 * EVM Tracer — EIP-3155 compliant execution trace output
 *
 * Outputs JSON Lines (one JSON object per opcode) to a FILE*.
 * Enable at compile time with -DENABLE_EVM_TRACE=1.
 *
 * Spec: https://eips.ethereum.org/EIPS/eip-3155
 */

#ifndef EVM_TRACER_H
#define EVM_TRACER_H

#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Forward declarations */
struct evm_t;

#ifdef ENABLE_EVM_TRACE

#include <stdio.h>

/* Maximum stack items to snapshot (EVM max is 1024) */
#define TRACE_MAX_STACK 1024

/**
 * Pending trace state — captures pre-execution snapshot of one opcode.
 * Emitted at the start of the NEXT opcode (so we know gasCost = gas_before - gas_now).
 */
typedef struct {
    bool     active;
    uint64_t pc;
    uint8_t  op;
    uint64_t gas;
    int      depth;      /* 1-based */
    int64_t  refund;
    size_t   mem_size;   /* in bytes */
    /* Stack snapshot (bottom-to-top order, matching geth output) */
    uint8_t  stack[TRACE_MAX_STACK][32]; /* uint256 as 32 big-endian bytes */
    int      stack_count;
} evm_trace_pending_t;

/**
 * Global tracer instance.  Kept as a global to avoid polluting evm_t with
 * trace fields that would affect every non-trace build.
 */
typedef struct {
    FILE    *out;       /* output stream (stderr, or a file) */
    bool     enabled;   /* master switch (can be toggled at runtime) */
} evm_tracer_t;

extern evm_tracer_t g_evm_tracer;

/* Per-tx trace filter: when >= 0, only trace this tx index within the block.
 * Set by --trace-tx N.  -1 = trace all txs (default when --trace-block used). */
extern int g_trace_tx_index;

/**
 * Initialise the tracer.  Call once before any EVM execution.
 * @param out   Output stream (e.g. stderr, or fopen'd file)
 */
void evm_tracer_init(FILE *out);

/**
 * Capture pre-execution state of the current opcode and emit the PREVIOUS
 * opcode's trace line (if any).
 *
 * Must be called at the top of DISPATCH(), before the opcode executes.
 */
void evm_tracer_on_dispatch(struct evm_t *evm);

/**
 * Emit the final trace line for the last opcode before an interpreter exit
 * (STOP, RETURN, REVERT, SELFDESTRUCT, OOG, error).
 *
 * @param evm   EVM context
 * @param error Error string, or NULL if no error
 */
void evm_tracer_on_exit(struct evm_t *evm, const char *error);

/**
 * Emit the previous opcode's trace + a synthetic STOP trace at evm->pc.
 * Called when pc >= code_size (implicit STOP at end of code).
 */
void evm_tracer_on_implicit_stop(struct evm_t *evm);

/**
 * Emit the per-call-frame summary line: {"output":"<hex>","gasUsed":"0x<hex>"}
 * Called when a CALL/CREATE/SELFDESTRUCT frame returns to its parent.
 *
 * @param output      Return data bytes (may be NULL)
 * @param output_len  Length of return data
 * @param gas_used    Gas consumed by this frame
 * @param error       Error string, or NULL
 */
void evm_tracer_on_return(const uint8_t *output, size_t output_len,
                          uint64_t gas_used, const char *error);

/**
 * Emit the final transaction summary: {"stateRoot":"0x...","output":"...","gasUsed":"0x..."}
 */
void evm_tracer_tx_summary(const uint8_t *state_root_32,
                           const uint8_t *output, size_t output_len,
                           uint64_t gas_used, bool pass);

/* Convenience macros for the interpreter hot path */
#define EVM_TRACE_DISPATCH(evm)  \
    do { if (g_evm_tracer.enabled) evm_tracer_on_dispatch(evm); } while (0)

#define EVM_TRACE_EXIT(evm, err) \
    do { if (g_evm_tracer.enabled) evm_tracer_on_exit(evm, err); } while (0)

#define EVM_TRACE_IMPLICIT_STOP(evm) \
    do { if (g_evm_tracer.enabled) evm_tracer_on_implicit_stop(evm); } while (0)

#define EVM_TRACE_RETURN(out, len, used, err) \
    do { if (g_evm_tracer.enabled) evm_tracer_on_return(out, len, used, err); } while (0)

#else /* ENABLE_EVM_TRACE not defined */

#define EVM_TRACE_DISPATCH(evm)               ((void)0)
#define EVM_TRACE_EXIT(evm, err)              ((void)0)
#define EVM_TRACE_IMPLICIT_STOP(evm)          ((void)0)
#define EVM_TRACE_RETURN(out, len, used, err) ((void)0)

static inline void evm_tracer_init(void *out) { (void)out; }
static inline void evm_tracer_tx_summary(const uint8_t *r, const uint8_t *o,
                                          size_t ol, uint64_t g, bool p)
{ (void)r; (void)o; (void)ol; (void)g; (void)p; }

#endif /* ENABLE_EVM_TRACE */

#ifdef __cplusplus
}
#endif

#endif /* EVM_TRACER_H */
