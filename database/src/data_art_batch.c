/**
 * Persistent ART - Batch Operations
 *
 * Atomic multi-key insert and mixed insert/delete batches.
 * Wraps the transaction API (begin → buffer → commit) for convenience.
 */

#include "data_art.h"
#include "db_error.h"

bool data_art_insert_batch(data_art_tree_t *tree,
                           const uint8_t **keys, const size_t *key_lens,
                           const void **values, const size_t *value_lens,
                           size_t count) {
    if (!tree) {
        DB_ERROR(DB_ERROR_INVALID_ARG, "tree is NULL");
        return false;
    }
    if (count == 0) {
        return true;  // Empty batch is a no-op
    }
    if (!keys || !key_lens || !values || !value_lens) {
        DB_ERROR(DB_ERROR_INVALID_ARG, "NULL array argument");
        return false;
    }

    uint64_t txn_id;
    if (!data_art_begin_txn(tree, &txn_id)) {
        DB_ERROR(DB_ERROR_IO, "failed to begin batch transaction");
        return false;
    }

    for (size_t i = 0; i < count; i++) {
        if (!data_art_insert(tree, keys[i], key_lens[i], values[i], value_lens[i])) {
            DB_ERROR(DB_ERROR_IO, "batch insert failed at index %zu", i);
            data_art_abort_txn(tree);
            return false;
        }
    }

    if (!data_art_commit_txn(tree)) {
        DB_ERROR(DB_ERROR_IO, "batch commit failed");
        return false;
    }

    return true;
}

bool data_art_batch(data_art_tree_t *tree,
                    const data_art_batch_op_t *ops, size_t count) {
    if (!tree) {
        DB_ERROR(DB_ERROR_INVALID_ARG, "tree is NULL");
        return false;
    }
    if (count == 0) {
        return true;  // Empty batch is a no-op
    }
    if (!ops) {
        DB_ERROR(DB_ERROR_INVALID_ARG, "NULL ops array");
        return false;
    }

    uint64_t txn_id;
    if (!data_art_begin_txn(tree, &txn_id)) {
        DB_ERROR(DB_ERROR_IO, "failed to begin batch transaction");
        return false;
    }

    for (size_t i = 0; i < count; i++) {
        bool success;
        if (ops[i].type == BATCH_OP_INSERT) {
            success = data_art_insert(tree, ops[i].key, ops[i].key_len,
                                      ops[i].value, ops[i].value_len);
        } else {
            success = data_art_delete(tree, ops[i].key, ops[i].key_len);
        }
        if (!success) {
            DB_ERROR(DB_ERROR_IO, "batch operation failed at index %zu", i);
            data_art_abort_txn(tree);
            return false;
        }
    }

    if (!data_art_commit_txn(tree)) {
        DB_ERROR(DB_ERROR_IO, "batch commit failed");
        return false;
    }

    return true;
}
