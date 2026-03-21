#include "witness_gas.h"

void witness_gas_init(witness_gas_t *wg)
{
    mem_art_init(&wg->accessed_subtrees);
    mem_art_init(&wg->accessed_leaves);
    mem_art_init(&wg->edited_subtrees);
    mem_art_init(&wg->edited_leaves);
}

void witness_gas_destroy(witness_gas_t *wg)
{
    mem_art_destroy(&wg->accessed_subtrees);
    mem_art_destroy(&wg->accessed_leaves);
    mem_art_destroy(&wg->edited_subtrees);
    mem_art_destroy(&wg->edited_leaves);
}

void witness_gas_reset(witness_gas_t *wg)
{
    witness_gas_destroy(wg);
    witness_gas_init(wg);
}

uint64_t witness_gas_access_event(witness_gas_t *wg,
                                   const uint8_t key[32],
                                   bool is_write,
                                   bool value_was_empty)
{
    (void)value_was_empty;
    uint64_t gas = 0;

    /* Stem = key[0:31]. Check/add to accessed_subtrees. */
    if (!mem_art_contains(&wg->accessed_subtrees, key, 31)) {
        gas += WITNESS_BRANCH_COST;
        mem_art_insert(&wg->accessed_subtrees, key, 31, NULL, 0);
    }

    /* Full key. Check/add to accessed_leaves. */
    if (!mem_art_contains(&wg->accessed_leaves, key, 32)) {
        gas += WITNESS_CHUNK_COST;
        mem_art_insert(&wg->accessed_leaves, key, 32, NULL, 0);
    }

    if (is_write) {
        /* Stem write tracking. */
        if (!mem_art_contains(&wg->edited_subtrees, key, 31)) {
            gas += SUBTREE_EDIT_COST;
            mem_art_insert(&wg->edited_subtrees, key, 31, NULL, 0);
        }

        /* Leaf write tracking. */
        if (!mem_art_contains(&wg->edited_leaves, key, 32)) {
            gas += CHUNK_EDIT_COST;
            mem_art_insert(&wg->edited_leaves, key, 32, NULL, 0);
            /* Note: CHUNK_FILL_COST is not charged in go-ethereum's current
             * implementation (chunkFill is declared but never set to true). */
        }
    }

    return gas;
}
