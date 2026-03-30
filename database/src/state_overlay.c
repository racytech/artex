/**
 * State Overlay — unified in-memory state backed by flat_store.
 *
 * Architecture:
 *   - Accounts: flat_store with 32-byte keys (addr_hash)
 *     Overlay entries hold: nonce, balance, code_hash, storage_root, flags
 *   - Storage: flat_store with 64-byte keys (addr_hash || slot_hash)
 *     Overlay entries hold: current value, original value (EIP-2200)
 *   - Journal: undo log for snapshot/revert
 *   - Access lists: mem_art sets (EIP-2929 warm/cold)
 *   - Transient storage: mem_art (EIP-1153)
 *
 * Replaces: mem_art cache + flat_state + all flush/evict logic in evm_state.c
 */

#include "state_overlay.h"
#include "flat_state.h"
#include "flat_store.h"
#include "code_store.h"
#include "compact_art.h"
#include "storage_trie.h"
#include "account_trie.h"
#include "keccak256.h"
#include "mem_art.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <time.h>

/* =========================================================================
 * Constants
 * ========================================================================= */

#define JOURNAL_INIT_CAP  256
#define MAX_CODE_SIZE     (24 * 1024 + 1)

/* =========================================================================
 * Account entry — in-memory representation
 *
 * Stored as the "record" inside flat_store's overlay entry.
 * Serialized to/from the compressed format used by flat_state.
 * ========================================================================= */

typedef struct {
    uint64_t   nonce;
    uint256_t  balance;
    hash_t     code_hash;
    hash_t     storage_root;
    hash_t     addr_hash;       /* cached keccak256(addr) */
    address_t  addr;            /* original address */
    uint8_t   *code;            /* loaded bytecode (NULL until needed) */
    uint32_t   code_size;

    /* Flags */
    bool dirty;                 /* modified in current tx */
    bool block_dirty;           /* modified in current block */
    bool existed;               /* exists in the trie */
    bool created;               /* created via CREATE/CREATE2 in current tx */
    bool self_destructed;       /* SELFDESTRUCT called in current tx */
    bool has_code;
    bool code_dirty;
    bool block_code_dirty;
    bool storage_dirty;         /* any storage slot modified */
    bool storage_cleared;       /* storage wiped (CREATE/self-destruct) */
    bool mpt_dirty;             /* needs update in MPT */

#ifdef ENABLE_HISTORY
    uint64_t   original_nonce;
    uint256_t  original_balance;
    hash_t     original_code_hash;
    bool       block_self_destructed;
    bool       block_created;
    bool       block_accessed;
#endif
} account_entry_t;

/* =========================================================================
 * Storage entry — in-memory representation
 * ========================================================================= */

typedef struct {
    uint256_t  current;
    uint256_t  original;        /* pre-tx value (EIP-2200) */
    hash_t     slot_hash;       /* cached keccak256(slot) */
    address_t  addr;            /* owning account */
    bool       dirty;
    bool       block_dirty;
    bool       mpt_dirty;

#ifdef ENABLE_HISTORY
    uint256_t  block_original;
#endif
} storage_entry_t;

/* =========================================================================
 * Journal
 * ========================================================================= */

typedef enum {
    JOURNAL_NONCE,
    JOURNAL_BALANCE,
    JOURNAL_CODE,
    JOURNAL_STORAGE,
    JOURNAL_ACCOUNT_CREATE,
    JOURNAL_SELF_DESTRUCT,
    JOURNAL_WARM_ADDR,
    JOURNAL_WARM_SLOT,
    JOURNAL_TRANSIENT_STORAGE,
} journal_type_t;

typedef struct {
    journal_type_t type;
    address_t addr;
    union {
        struct { uint64_t val; bool dirty; bool block_dirty; } nonce;
        struct { uint256_t val; bool dirty; bool block_dirty; } balance;
        struct { hash_t old_hash; bool old_has_code; uint8_t *old_code;
                 uint32_t old_code_size; } code;
        struct { uint256_t slot; uint256_t old_value; bool old_mpt_dirty; } storage;
        uint256_t slot;  /* WARM_SLOT */
        struct {
            uint64_t old_nonce; uint256_t old_balance;
            hash_t old_code_hash; bool old_has_code;
            uint8_t *old_code; uint32_t old_code_size;
            bool old_dirty; bool old_code_dirty;
            bool old_block_dirty; bool old_block_code_dirty;
            bool old_created; bool old_existed; bool old_self_destructed;
            hash_t old_storage_root;
            bool old_storage_dirty; bool old_storage_cleared;
#ifdef ENABLE_HISTORY
            bool old_block_created;
#endif
        } create;
        bool old_self_destructed;
        uint256_t old_transient_value;
    } data;
} journal_entry_t;

/* =========================================================================
 * Main struct
 * ========================================================================= */

struct state_overlay {
    flat_state_t     *flat_state;    /* backing store (not owned) */
    code_store_t     *code_store;    /* bytecode store (not owned) */
    account_trie_t   *account_trie;  /* MPT root from flat_state */
    storage_trie_t   *storage_trie;  /* per-account storage roots */

    /* In-memory account cache — keyed by address (20 bytes).
     * This is the ONLY source of truth during execution.
     * TODO: replace with flat_store overlay entries directly. */
    mem_art_t         accounts;      /* addr[20] → account_entry_t */
    mem_art_t         storage;       /* addr[20]||slot[32] → storage_entry_t */

    /* Journal */
    journal_entry_t  *journal;
    uint32_t          journal_len;
    uint32_t          journal_cap;

    /* Access lists (EIP-2929) */
    mem_art_t         warm_addrs;
    mem_art_t         warm_slots;

    /* Transient storage (EIP-1153) */
    mem_art_t         transient;

    /* EIP-161 */
    bool              prune_empty;

    /* Root computation timing */
    double            last_root_stor_ms;
    double            last_root_acct_ms;
    size_t            last_root_dirty_count;
};

/* =========================================================================
 * Lifecycle
 * ========================================================================= */

state_overlay_t *state_overlay_create(flat_state_t *fs, code_store_t *cs) {
    state_overlay_t *so = calloc(1, sizeof(*so));
    if (!so) return NULL;

    so->flat_state = fs;
    so->code_store = cs;

    mem_art_init(&so->accounts);
    mem_art_init(&so->storage);
    mem_art_init(&so->warm_addrs);
    mem_art_init(&so->warm_slots);
    mem_art_init(&so->transient);

    so->journal = malloc(JOURNAL_INIT_CAP * sizeof(journal_entry_t));
    if (!so->journal) {
        free(so);
        return NULL;
    }
    so->journal_cap = JOURNAL_INIT_CAP;

    /* Create tries if flat_state available */
    if (fs) {
        compact_art_t *s_art = flat_state_storage_art(fs);
        flat_store_t  *s_store = flat_state_storage_store(fs);
        if (s_art && s_store)
            so->storage_trie = storage_trie_create(s_art, s_store);

        compact_art_t *a_art = flat_state_account_art(fs);
        flat_store_t  *a_store = flat_state_account_store(fs);
        if (a_art && a_store)
            so->account_trie = account_trie_create(a_art, a_store);
    }

    return so;
}

void state_overlay_destroy(state_overlay_t *so) {
    if (!so) return;

    /* Free code pointers in cached accounts */
    /* TODO: iterate accounts and free code */

    if (so->storage_trie) storage_trie_destroy(so->storage_trie);
    if (so->account_trie) account_trie_destroy(so->account_trie);

    mem_art_destroy(&so->accounts);
    mem_art_destroy(&so->storage);
    mem_art_destroy(&so->warm_addrs);
    mem_art_destroy(&so->warm_slots);
    mem_art_destroy(&so->transient);

    /* Free journal code pointers */
    for (uint32_t i = 0; i < so->journal_len; i++) {
        if (so->journal[i].type == JOURNAL_CODE)
            free(so->journal[i].data.code.old_code);
        else if (so->journal[i].type == JOURNAL_ACCOUNT_CREATE)
            free(so->journal[i].data.create.old_code);
    }
    free(so->journal);
    free(so);
}

/* =========================================================================
 * TODO: implement all API functions
 *
 * Phase 1: Wire up using mem_art (same as current evm_state.c internals)
 * Phase 2: Replace mem_art with direct flat_store overlay access
 * ========================================================================= */
