#include "verkle_journal.h"
#include "verkle_snapshot.h"
#include "verkle_state.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>

/* =========================================================================
 * Test Infrastructure
 * ========================================================================= */

static int tests_passed = 0;
static int tests_failed = 0;

#define ASSERT(cond, msg)                                              \
    do {                                                               \
        if (!(cond)) {                                                 \
            printf("  FAIL: %s (line %d)\n", msg, __LINE__);          \
            tests_failed++;                                            \
        } else {                                                       \
            tests_passed++;                                            \
        }                                                              \
    } while (0)

#define FWD_PATH "/tmp/test_verkle_journal.fwd"
#define SNAP_PATH "/tmp/test_verkle_journal.snap"

static void cleanup(void) {
    remove(FWD_PATH);
    remove(SNAP_PATH);
}

/* Helper: make a key with given stem byte and suffix */
static void make_key(uint8_t key[32], uint8_t stem_byte, uint8_t suffix)
{
    memset(key, stem_byte, 31);
    key[31] = suffix;
}

/* Helper: make a value filled with a byte */
static void make_val(uint8_t val[32], uint8_t fill)
{
    memset(val, fill, 32);
}

/* =========================================================================
 * Phase 1: verkle_unset
 * ========================================================================= */

static void test_unset(void)
{
    printf("Phase 1: verkle_unset\n");

    verkle_tree_t *vt = verkle_create();

    /* Set a key */
    uint8_t key[32], val[32], got[32];
    make_key(key, 0xAA, 5);
    make_val(val, 0x42);
    verkle_set(vt, key, val);

    /* Capture root hash with key set */
    uint8_t hash_with[32];
    verkle_root_hash(vt, hash_with);

    /* Unset it */
    bool cleared = verkle_unset(vt, key);
    ASSERT(cleared, "unset returns true");

    /* verkle_get should return false */
    ASSERT(!verkle_get(vt, key, got), "get returns false after unset");

    /* Unset again should return false (already cleared) */
    ASSERT(!verkle_unset(vt, key), "second unset returns false");

    /* Set two keys on the same stem, unset one */
    uint8_t key2[32], val2[32];
    make_key(key, 0xBB, 3);
    make_key(key2, 0xBB, 7);
    make_val(val, 0x11);
    make_val(val2, 0x22);
    verkle_set(vt, key, val);
    verkle_set(vt, key2, val2);

    uint8_t hash_before[32];
    verkle_root_hash(vt, hash_before);

    verkle_unset(vt, key);
    ASSERT(!verkle_get(vt, key, got), "unset key not found");
    ASSERT(verkle_get(vt, key2, got), "other key still found");
    ASSERT(memcmp(got, val2, 32) == 0, "other key value intact");

    /* Re-set the key, hash should match what it was before */
    verkle_set(vt, key, val);
    uint8_t hash_after[32];
    verkle_root_hash(vt, hash_after);
    ASSERT(memcmp(hash_before, hash_after, 32) == 0,
           "root hash restored after set-unset-set");

    verkle_destroy(vt);
    printf("  OK\n\n");
}

/* =========================================================================
 * Phase 2: Empty block
 * ========================================================================= */

static void test_empty_block(void)
{
    printf("Phase 2: Empty block begin/commit\n");

    verkle_tree_t *vt = verkle_create();
    verkle_journal_t *j = verkle_journal_create(vt);
    ASSERT(j != NULL, "journal created");

    bool ok = verkle_journal_begin_block(j, 1);
    ASSERT(ok, "begin_block succeeds");

    ok = verkle_journal_commit_block(j);
    ASSERT(ok, "commit empty block succeeds");

    /* Double begin without commit should fail */
    verkle_journal_begin_block(j, 2);
    ASSERT(!verkle_journal_begin_block(j, 3),
           "double begin_block fails");
    verkle_journal_commit_block(j);

    verkle_journal_destroy(j);
    verkle_destroy(vt);
    printf("  OK\n\n");
}

/* =========================================================================
 * Phase 3: Single key revert
 * ========================================================================= */

static void test_single_key_revert(void)
{
    printf("Phase 3: Single key set + revert\n");

    verkle_tree_t *vt = verkle_create();

    /* Pre-set a value */
    uint8_t key[32], orig_val[32], new_val[32], got[32];
    make_key(key, 0xCC, 0);
    make_val(orig_val, 0x11);
    make_val(new_val, 0x22);
    verkle_set(vt, key, orig_val);

    uint8_t hash_before[32];
    verkle_root_hash(vt, hash_before);

    /* Journal: overwrite the key */
    verkle_journal_t *j = verkle_journal_create(vt);
    verkle_journal_begin_block(j, 1);
    verkle_journal_set(j, key, new_val);

    /* Verify new value is in tree */
    ASSERT(verkle_get(vt, key, got), "new value present");
    ASSERT(memcmp(got, new_val, 32) == 0, "new value matches");

    verkle_journal_commit_block(j);

    /* Revert */
    bool ok = verkle_journal_revert_block(j);
    ASSERT(ok, "revert succeeds");

    /* Original value restored */
    ASSERT(verkle_get(vt, key, got), "original value present");
    ASSERT(memcmp(got, orig_val, 32) == 0, "original value matches");

    /* Root hash matches */
    uint8_t hash_after[32];
    verkle_root_hash(vt, hash_after);
    ASSERT(memcmp(hash_before, hash_after, 32) == 0,
           "root hash restored");

    verkle_journal_destroy(j);
    verkle_destroy(vt);
    printf("  OK\n\n");
}

/* =========================================================================
 * Phase 4: Multi-key revert
 * ========================================================================= */

static void test_multi_key_revert(void)
{
    printf("Phase 4: Multi-key set + revert\n");

    verkle_tree_t *vt = verkle_create();

    /* Pre-set 5 keys */
    uint8_t keys[5][32], orig_vals[5][32], new_vals[5][32];
    for (int i = 0; i < 5; i++) {
        make_key(keys[i], (uint8_t)(0x10 * (i + 1)), 0);
        make_val(orig_vals[i], (uint8_t)(i + 1));
        make_val(new_vals[i], (uint8_t)(i + 100));
        verkle_set(vt, keys[i], orig_vals[i]);
    }

    uint8_t hash_before[32];
    verkle_root_hash(vt, hash_before);

    /* Journal: overwrite all 5 */
    verkle_journal_t *j = verkle_journal_create(vt);
    verkle_journal_begin_block(j, 1);
    for (int i = 0; i < 5; i++)
        verkle_journal_set(j, keys[i], new_vals[i]);
    verkle_journal_commit_block(j);

    /* Revert */
    verkle_journal_revert_block(j);

    /* All originals restored */
    uint8_t got[32];
    for (int i = 0; i < 5; i++) {
        ASSERT(verkle_get(vt, keys[i], got) &&
               memcmp(got, orig_vals[i], 32) == 0,
               "key value restored");
    }

    uint8_t hash_after[32];
    verkle_root_hash(vt, hash_after);
    ASSERT(memcmp(hash_before, hash_after, 32) == 0,
           "root hash restored");

    verkle_journal_destroy(j);
    verkle_destroy(vt);
    printf("  OK\n\n");
}

/* =========================================================================
 * Phase 5: New key revert (key didn't exist before)
 * ========================================================================= */

static void test_new_key_revert(void)
{
    printf("Phase 5: New key revert (unset on revert)\n");

    verkle_tree_t *vt = verkle_create();

    uint8_t hash_before[32];
    verkle_root_hash(vt, hash_before);

    /* Journal: set a brand new key */
    verkle_journal_t *j = verkle_journal_create(vt);
    verkle_journal_begin_block(j, 1);

    uint8_t key[32], val[32], got[32];
    make_key(key, 0xDD, 10);
    make_val(val, 0xFF);
    verkle_journal_set(j, key, val);

    /* Key exists now */
    ASSERT(verkle_get(vt, key, got), "new key present after set");

    verkle_journal_commit_block(j);

    /* Revert — key should not exist */
    verkle_journal_revert_block(j);
    ASSERT(!verkle_get(vt, key, got), "new key gone after revert");

    /* Root hash matches original empty tree */
    uint8_t hash_after[32];
    verkle_root_hash(vt, hash_after);
    ASSERT(memcmp(hash_before, hash_after, 32) == 0,
           "root hash matches empty tree");

    verkle_journal_destroy(j);
    verkle_destroy(vt);
    printf("  OK\n\n");
}

/* =========================================================================
 * Phase 6: Multi-block, revert only latest
 * ========================================================================= */

static void test_multi_block_revert(void)
{
    printf("Phase 6: Multi-block, revert only latest\n");

    verkle_tree_t *vt = verkle_create();
    verkle_journal_t *j = verkle_journal_create(vt);

    /* Block 1: set key A */
    uint8_t key_a[32], val_a[32];
    make_key(key_a, 0xAA, 0);
    make_val(val_a, 0x11);

    verkle_journal_begin_block(j, 1);
    verkle_journal_set(j, key_a, val_a);
    verkle_journal_commit_block(j);

    uint8_t hash_after_block1[32];
    verkle_root_hash(vt, hash_after_block1);

    /* Block 2: set key B + overwrite key A */
    uint8_t key_b[32], val_b[32], val_a2[32];
    make_key(key_b, 0xBB, 0);
    make_val(val_b, 0x22);
    make_val(val_a2, 0x33);

    verkle_journal_begin_block(j, 2);
    verkle_journal_set(j, key_b, val_b);
    verkle_journal_set(j, key_a, val_a2);
    verkle_journal_commit_block(j);

    /* Revert block 2 only */
    verkle_journal_revert_block(j);

    /* Key A should have block 1's value */
    uint8_t got[32];
    ASSERT(verkle_get(vt, key_a, got) &&
           memcmp(got, val_a, 32) == 0,
           "key A has block 1 value");

    /* Key B should not exist (was new in block 2) */
    ASSERT(!verkle_get(vt, key_b, got),
           "key B gone after revert");

    /* Hash matches state after block 1 */
    uint8_t hash_after_revert[32];
    verkle_root_hash(vt, hash_after_revert);
    ASSERT(memcmp(hash_after_block1, hash_after_revert, 32) == 0,
           "root hash matches block 1 state");

    verkle_journal_destroy(j);
    verkle_destroy(vt);
    printf("  OK\n\n");
}

/* =========================================================================
 * Phase 7: Root hash consistency after revert
 * ========================================================================= */

static void test_root_hash_consistency(void)
{
    printf("Phase 7: Root hash consistency through multiple ops\n");

    verkle_tree_t *vt = verkle_create();
    verkle_journal_t *j = verkle_journal_create(vt);

    /* Build initial state with 10 keys */
    for (int i = 0; i < 10; i++) {
        uint8_t key[32], val[32];
        make_key(key, (uint8_t)(i * 17), (uint8_t)i);
        make_val(val, (uint8_t)(i + 1));
        verkle_set(vt, key, val);
    }

    uint8_t hash_initial[32];
    verkle_root_hash(vt, hash_initial);

    /* Block: modify all 10 keys */
    verkle_journal_begin_block(j, 1);
    for (int i = 0; i < 10; i++) {
        uint8_t key[32], val[32];
        make_key(key, (uint8_t)(i * 17), (uint8_t)i);
        make_val(val, (uint8_t)(i + 50));
        verkle_journal_set(j, key, val);
    }
    verkle_journal_commit_block(j);

    /* Verify hash changed */
    uint8_t hash_modified[32];
    verkle_root_hash(vt, hash_modified);
    ASSERT(memcmp(hash_initial, hash_modified, 32) != 0,
           "hash changed after modification");

    /* Revert */
    verkle_journal_revert_block(j);

    uint8_t hash_reverted[32];
    verkle_root_hash(vt, hash_reverted);
    ASSERT(memcmp(hash_initial, hash_reverted, 32) == 0,
           "hash matches initial state after revert");

    verkle_journal_destroy(j);
    verkle_destroy(vt);
    printf("  OK\n\n");
}

/* =========================================================================
 * Phase 8: Forward journal write + replay
 * ========================================================================= */

static void test_fwd_journal_replay(void)
{
    printf("Phase 8: Forward journal write + replay\n");
    cleanup();

    verkle_tree_t *vt = verkle_create();
    verkle_journal_t *j = verkle_journal_create(vt);

    bool ok = verkle_journal_enable_fwd(j, FWD_PATH, 0);
    ASSERT(ok, "enable fwd journal");

    /* Block 1: set 3 keys */
    uint8_t keys[3][32], vals[3][32];
    for (int i = 0; i < 3; i++) {
        make_key(keys[i], (uint8_t)(0x10 + i), 0);
        make_val(vals[i], (uint8_t)(i + 1));
    }

    verkle_journal_begin_block(j, 1);
    for (int i = 0; i < 3; i++)
        verkle_journal_set(j, keys[i], vals[i]);
    verkle_journal_commit_block(j);

    /* Block 2: set 2 more keys */
    uint8_t keys2[2][32], vals2[2][32];
    for (int i = 0; i < 2; i++) {
        make_key(keys2[i], (uint8_t)(0x50 + i), 0);
        make_val(vals2[i], (uint8_t)(i + 10));
    }

    verkle_journal_begin_block(j, 2);
    for (int i = 0; i < 2; i++)
        verkle_journal_set(j, keys2[i], vals2[i]);
    verkle_journal_commit_block(j);

    /* Capture original root hash */
    uint8_t orig_hash[32];
    verkle_root_hash(vt, orig_hash);

    /* Replay onto a fresh tree */
    verkle_tree_t *fresh = verkle_create();
    uint64_t last_block = 0;
    ok = verkle_journal_replay_fwd(FWD_PATH, fresh, &last_block);
    ASSERT(ok, "replay succeeds");
    ASSERT(last_block == 2, "last block is 2");

    /* Verify all keys */
    uint8_t got[32];
    for (int i = 0; i < 3; i++) {
        ASSERT(verkle_get(fresh, keys[i], got) &&
               memcmp(got, vals[i], 32) == 0,
               "block 1 key replayed correctly");
    }
    for (int i = 0; i < 2; i++) {
        ASSERT(verkle_get(fresh, keys2[i], got) &&
               memcmp(got, vals2[i], 32) == 0,
               "block 2 key replayed correctly");
    }

    /* Root hash matches */
    uint8_t replayed_hash[32];
    verkle_root_hash(fresh, replayed_hash);
    ASSERT(memcmp(orig_hash, replayed_hash, 32) == 0,
           "replayed root hash matches original");

    verkle_journal_destroy(j);
    verkle_destroy(vt);
    verkle_destroy(fresh);
    printf("  OK\n\n");
    cleanup();
}

/* =========================================================================
 * Phase 9: Incomplete forward block discarded
 * ========================================================================= */

static void test_incomplete_fwd_block(void)
{
    printf("Phase 9: Incomplete forward block discarded\n");
    cleanup();

    verkle_tree_t *vt = verkle_create();
    verkle_journal_t *j = verkle_journal_create(vt);
    verkle_journal_enable_fwd(j, FWD_PATH, 0);

    /* Block 1: complete */
    uint8_t key1[32], val1[32];
    make_key(key1, 0xAA, 0);
    make_val(val1, 0x11);

    verkle_journal_begin_block(j, 1);
    verkle_journal_set(j, key1, val1);
    verkle_journal_commit_block(j);

    /* Block 2: write entries but no commit tag (simulate crash) */
    uint8_t key2[32], val2[32];
    make_key(key2, 0xBB, 0);
    make_val(val2, 0x22);

    verkle_journal_begin_block(j, 2);
    verkle_journal_set(j, key2, val2);
    /* Don't commit — write block begin + entries manually then stop */

    /* Close the journal fd so the incomplete block stays on disk */
    /* The begin_block entries haven't been written to fwd yet (only on commit).
     * So we need to write an incomplete block manually. */
    verkle_journal_destroy(j);

    /* Append an incomplete block to the file */
    {
        FILE *f = fopen(FWD_PATH, "ab");
        uint8_t tag = 0xBB; /* BLOCK_BEGIN */
        uint64_t bn = 2;
        uint32_t cnt = 1;
        fwrite(&tag, 1, 1, f);
        fwrite(&bn, 8, 1, f);
        fwrite(&cnt, 4, 1, f);
        fwrite(key2, 32, 1, f);
        fwrite(val2, 32, 1, f);
        /* No BLOCK_COMMIT tag */
        fclose(f);
    }

    /* Replay — should only get block 1 */
    verkle_tree_t *fresh = verkle_create();
    uint64_t last_block = 0;
    verkle_journal_replay_fwd(FWD_PATH, fresh, &last_block);
    ASSERT(last_block == 1, "only block 1 replayed");

    uint8_t got[32];
    ASSERT(verkle_get(fresh, key1, got), "block 1 key present");
    ASSERT(!verkle_get(fresh, key2, got), "block 2 key NOT present");

    verkle_destroy(vt);
    verkle_destroy(fresh);
    printf("  OK\n\n");
    cleanup();
}

/* =========================================================================
 * Phase 10: Checkpoint fork
 * ========================================================================= */

static void test_checkpoint_fork(void)
{
    printf("Phase 10: Checkpoint via fork()\n");
    cleanup();

    verkle_tree_t *vt = verkle_create();
    verkle_journal_t *j = verkle_journal_create(vt);

    /* Set some state */
    uint8_t keys[5][32], vals[5][32];
    for (int i = 0; i < 5; i++) {
        make_key(keys[i], (uint8_t)(0x10 + i), 0);
        make_val(vals[i], (uint8_t)(i + 1));
    }

    verkle_journal_begin_block(j, 1);
    for (int i = 0; i < 5; i++)
        verkle_journal_set(j, keys[i], vals[i]);
    verkle_journal_commit_block(j);

    uint8_t orig_hash[32];
    verkle_root_hash(vt, orig_hash);

    /* Start checkpoint */
    bool ok = verkle_journal_checkpoint_start(j, SNAP_PATH);
    ASSERT(ok, "checkpoint start succeeds");

    /* Second start should fail */
    ASSERT(!verkle_journal_checkpoint_start(j, SNAP_PATH),
           "double checkpoint start fails");

    /* Wait for completion */
    ok = verkle_journal_checkpoint_wait(j);
    ASSERT(ok, "checkpoint wait returns true");

    /* Verify snapshot exists and loads correctly */
    verkle_tree_t *loaded = verkle_snapshot_load(SNAP_PATH);
    ASSERT(loaded != NULL, "snapshot loads");

    uint8_t loaded_hash[32];
    verkle_root_hash(loaded, loaded_hash);
    ASSERT(memcmp(orig_hash, loaded_hash, 32) == 0,
           "snapshot root hash matches");

    /* Verify all keys in loaded tree */
    uint8_t got[32];
    for (int i = 0; i < 5; i++) {
        ASSERT(verkle_get(loaded, keys[i], got) &&
               memcmp(got, vals[i], 32) == 0,
               "checkpoint key value matches");
    }

    verkle_journal_destroy(j);
    verkle_destroy(vt);
    verkle_destroy(loaded);
    printf("  OK\n\n");
    cleanup();
}

/* =========================================================================
 * Main
 * ========================================================================= */

int main(void)
{
    printf("=== Verkle Journal Tests ===\n\n");

    test_unset();
    test_empty_block();
    test_single_key_revert();
    test_multi_key_revert();
    test_new_key_revert();
    test_multi_block_revert();
    test_root_hash_consistency();
    test_fwd_journal_replay();
    test_incomplete_fwd_block();
    test_checkpoint_fork();

    printf("=== Results: %d passed, %d failed ===\n",
           tests_passed, tests_failed);
    return tests_failed > 0 ? 1 : 0;
}
