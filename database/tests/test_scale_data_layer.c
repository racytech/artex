/*
 * Data Layer Scale Test — Realistic Ethereum Block Simulation
 *
 * Simulates realistic block processing at scale:
 *   - 5K-50K state ops per block (70% insert, 20% update, 10% delete)
 *   - 1-10 contract deploys per block
 *   - Merge after every block
 *   - Checkpoint every 128 blocks
 *   - Verification: checkpoint recovery + state sampling + code verification
 *
 * Designed to find bugs at scale before production use.
 * Uses bitset tracking (1 bit per key_id) for O(1) live/dead status.
 *
 * Usage: ./test_scale_data_layer [target_millions]
 *   Default: 10M keys
 */

#include "../include/data_layer.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <stdbool.h>
#include <inttypes.h>
#include <time.h>
#include <unistd.h>
#include <sys/stat.h>

// ============================================================================
// Constants
// ============================================================================

#define KEY_SIZE            32
#define VALUE_LEN_MIN       32
#define VALUE_LEN_MAX       120
#define CODE_SIZE_MIN       100
#define CODE_SIZE_MAX       24576   // EIP-170 limit

#define OPS_MIN             5000
#define OPS_MAX             50000
#define CHECKPOINT_INTERVAL 128
#define STATS_INTERVAL      128

#define STATE_PATH  "/tmp/art_scale_state.dat"
#define CODE_PATH   "/tmp/art_scale_code.dat"
#define INDEX_PATH  "/tmp/art_scale_index.dat"

#define MASTER_SEED 0x5343414C45544553ULL

// Separate key namespaces
#define STATE_KEY_SEED (MASTER_SEED ^ 0x5354415445000000ULL)
#define CODE_KEY_SEED  (MASTER_SEED ^ 0xC0DE000000000000ULL)

// ============================================================================
// Fail-fast
// ============================================================================

#define ASSERT_MSG(cond, fmt, ...) do { \
    if (!(cond)) { \
        fprintf(stderr, "FAIL %s:%d: " fmt "\n", \
                __FILE__, __LINE__, ##__VA_ARGS__); \
        abort(); \
    } \
} while(0)

// ============================================================================
// RNG (SplitMix64)
// ============================================================================

typedef struct { uint64_t state; } rng_t;

static inline uint64_t rng_next(rng_t *rng) {
    uint64_t z = (rng->state += 0x9e3779b97f4a7c15ULL);
    z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9ULL;
    z = (z ^ (z >> 27)) * 0x94d049bb133111ebULL;
    return z ^ (z >> 31);
}

static inline rng_t rng_create(uint64_t seed) {
    rng_t r = { .state = seed };
    rng_next(&r);
    return r;
}

// ============================================================================
// Key/Value Generation
// ============================================================================

static void generate_key(uint8_t key[KEY_SIZE], uint64_t seed, uint64_t index) {
    rng_t rng = rng_create(seed ^ (index * 0x517cc1b727220a95ULL));
    uint64_t r0 = rng_next(&rng), r1 = rng_next(&rng);
    uint64_t r2 = rng_next(&rng), r3 = rng_next(&rng);
    memcpy(key,      &r0, 8); memcpy(key + 8,  &r1, 8);
    memcpy(key + 16, &r2, 8); memcpy(key + 24, &r3, 8);
}

static uint16_t generate_value(uint8_t *buf, uint64_t seed, uint64_t index,
                               uint16_t len) {
    rng_t rng = rng_create(seed ^ (index * 0x9c2f0b3a71d8e6f5ULL));
    for (uint16_t i = 0; i < len; i += 8) {
        uint64_t r = rng_next(&rng);
        uint16_t remain = len - i;
        memcpy(buf + i, &r, remain < 8 ? remain : 8);
    }
    return len;
}

static uint32_t generate_bytecode(uint8_t *buf, uint64_t seed, uint64_t index,
                                  uint32_t len) {
    rng_t rng = rng_create(seed ^ (index * 0xC0DE1E4F00000001ULL));
    for (uint32_t i = 0; i < len; i += 8) {
        uint64_t r = rng_next(&rng);
        uint32_t remain = len - i;
        memcpy(buf + i, &r, remain < 8 ? remain : 8);
    }
    return len;
}

// ============================================================================
// Utilities
// ============================================================================

static double now_sec(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec + ts.tv_nsec / 1e9;
}

static size_t get_rss_mb(void) {
    FILE *f = fopen("/proc/self/status", "r");
    if (!f) return 0;
    char line[256];
    size_t rss_kb = 0;
    while (fgets(line, sizeof(line), f)) {
        if (strncmp(line, "VmRSS:", 6) == 0) {
            sscanf(line + 6, "%zu", &rss_kb);
            break;
        }
    }
    fclose(f);
    return rss_kb / 1024;
}

static size_t get_file_size_path(const char *path) {
    struct stat st;
    if (stat(path, &st) != 0) return 0;
    return (size_t)st.st_size;
}

// ============================================================================
// Bitset — 1 bit per key_id for tracking live keys
// ============================================================================

typedef struct {
    uint64_t *bits;
    uint64_t capacity;  // number of bits
} bitset_t;

static bool bitset_init(bitset_t *bs, uint64_t initial_bits) {
    uint64_t words = (initial_bits + 63) / 64;
    if (words == 0) words = 1024;
    bs->bits = calloc(words, sizeof(uint64_t));
    if (!bs->bits) return false;
    bs->capacity = words * 64;
    return true;
}

static void bitset_grow(bitset_t *bs, uint64_t needed) {
    if (needed < bs->capacity) return;
    uint64_t new_cap = bs->capacity;
    while (new_cap <= needed) new_cap *= 2;
    uint64_t old_words = bs->capacity / 64;
    uint64_t new_words = new_cap / 64;
    bs->bits = realloc(bs->bits, new_words * sizeof(uint64_t));
    memset(bs->bits + old_words, 0, (new_words - old_words) * sizeof(uint64_t));
    bs->capacity = new_cap;
}

static void bitset_set(bitset_t *bs, uint64_t id) {
    bitset_grow(bs, id + 1);
    bs->bits[id / 64] |= (1ULL << (id % 64));
}

static void bitset_clear(bitset_t *bs, uint64_t id) {
    if (id >= bs->capacity) return;
    bs->bits[id / 64] &= ~(1ULL << (id % 64));
}

static bool bitset_test(const bitset_t *bs, uint64_t id) {
    if (id >= bs->capacity) return false;
    return (bs->bits[id / 64] & (1ULL << (id % 64))) != 0;
}

static void bitset_destroy(bitset_t *bs) {
    free(bs->bits);
    bs->bits = NULL;
    bs->capacity = 0;
}

// ============================================================================
// Value seed — changes per block so updates produce different values
// ============================================================================

static uint64_t value_seed(uint64_t block) {
    return MASTER_SEED ^ (block * 0x56414C5545000000ULL);
}

// Random value length in [VALUE_LEN_MIN, VALUE_LEN_MAX]
// But capped at 62 (STATE_STORE_MAX_VALUE)
static uint16_t random_value_len(rng_t *rng) {
    uint16_t range = VALUE_LEN_MAX - VALUE_LEN_MIN;
    uint16_t len = VALUE_LEN_MIN + (uint16_t)(rng_next(rng) % (range + 1));
    if (len > 62) len = 62;  // state_store max value
    return len;
}

// Random code length in [CODE_SIZE_MIN, CODE_SIZE_MAX]
static uint32_t random_code_len(rng_t *rng) {
    uint32_t range = CODE_SIZE_MAX - CODE_SIZE_MIN;
    return CODE_SIZE_MIN + (uint32_t)(rng_next(rng) % (range + 1));
}

// ============================================================================
// Block Simulation
// ============================================================================

typedef struct {
    uint64_t next_state_id;
    uint64_t next_code_id;
    uint64_t total_blocks;
    uint64_t total_inserts;
    uint64_t total_updates;
    uint64_t total_deletes;
    uint64_t total_code_deploys;
    double   total_merge_time;
    double   total_checkpoint_time;
    uint64_t last_checkpoint_block;
    bitset_t state_live;
    // Per-block value lengths — track last written length per key for verification
    // (we regenerate deterministically, so we just need the seed+index+len)
    uint16_t *value_lens;       // array indexed by key_id
    uint64_t value_lens_cap;
    uint64_t *value_versions;   // block number at which key was last written
    uint64_t value_versions_cap;
} sim_state_t;

static void sim_track_value(sim_state_t *sim, uint64_t key_id,
                            uint16_t len, uint64_t block) {
    if (key_id >= sim->value_lens_cap) {
        uint64_t new_cap = sim->value_lens_cap ? sim->value_lens_cap : 1024;
        while (new_cap <= key_id) new_cap *= 2;
        sim->value_lens = realloc(sim->value_lens,
                                  new_cap * sizeof(uint16_t));
        memset(sim->value_lens + sim->value_lens_cap, 0,
               (new_cap - sim->value_lens_cap) * sizeof(uint16_t));
        sim->value_lens_cap = new_cap;
    }
    if (key_id >= sim->value_versions_cap) {
        uint64_t new_cap = sim->value_versions_cap ? sim->value_versions_cap : 1024;
        while (new_cap <= key_id) new_cap *= 2;
        sim->value_versions = realloc(sim->value_versions,
                                      new_cap * sizeof(uint64_t));
        memset(sim->value_versions + sim->value_versions_cap, 0,
               (new_cap - sim->value_versions_cap) * sizeof(uint64_t));
        sim->value_versions_cap = new_cap;
    }
    sim->value_lens[key_id] = len;
    sim->value_versions[key_id] = block;
}

static void run_block(data_layer_t *dl, sim_state_t *sim, uint64_t block_num) {
    rng_t brng = rng_create(MASTER_SEED ^ (block_num * 0x426C6F636B000000ULL));

    // Determine ops count for this block
    uint64_t ops_count = OPS_MIN + (rng_next(&brng) % (OPS_MAX - OPS_MIN + 1));

    uint64_t n_inserts = (ops_count * 70) / 100;
    uint64_t n_updates = (ops_count * 20) / 100;
    uint64_t n_deletes = ops_count - n_inserts - n_updates;

    uint8_t key[KEY_SIZE];
    uint8_t val_buf[128];  // max 62 actually used

    // --- 70% new inserts ---
    for (uint64_t i = 0; i < n_inserts; i++) {
        uint64_t key_id = sim->next_state_id++;
        generate_key(key, STATE_KEY_SEED, key_id);
        uint16_t vlen = random_value_len(&brng);
        generate_value(val_buf, value_seed(block_num), key_id, vlen);
        bool ok = dl_put(dl, key, val_buf, vlen);
        ASSERT_MSG(ok, "dl_put(insert) failed block %" PRIu64 " key_id %" PRIu64,
                   block_num, key_id);
        bitset_set(&sim->state_live, key_id);
        sim_track_value(sim, key_id, vlen, block_num);
    }
    sim->total_inserts += n_inserts;

    // --- 20% updates (random existing key) ---
    for (uint64_t i = 0; i < n_updates; i++) {
        if (sim->next_state_id == 0) continue;
        uint64_t key_id = rng_next(&brng) % sim->next_state_id;
        generate_key(key, STATE_KEY_SEED, key_id);
        uint16_t vlen = random_value_len(&brng);
        generate_value(val_buf, value_seed(block_num), key_id, vlen);
        bool ok = dl_put(dl, key, val_buf, vlen);
        ASSERT_MSG(ok, "dl_put(update) failed block %" PRIu64, block_num);
        bitset_set(&sim->state_live, key_id);
        sim_track_value(sim, key_id, vlen, block_num);
    }
    sim->total_updates += n_updates;

    // --- 10% deletes (random existing key) ---
    for (uint64_t i = 0; i < n_deletes; i++) {
        if (sim->next_state_id == 0) continue;
        uint64_t key_id = rng_next(&brng) % sim->next_state_id;
        generate_key(key, STATE_KEY_SEED, key_id);
        bool ok = dl_delete(dl, key);
        ASSERT_MSG(ok, "dl_delete failed block %" PRIu64, block_num);
        bitset_clear(&sim->state_live, key_id);
    }
    sim->total_deletes += n_deletes;

    // --- 1-10 code deployments ---
    uint64_t code_count = 1 + (rng_next(&brng) % 10);
    uint8_t *code_buf = malloc(CODE_SIZE_MAX);
    for (uint64_t i = 0; i < code_count; i++) {
        uint64_t code_id = sim->next_code_id++;
        generate_key(key, CODE_KEY_SEED, code_id);
        uint32_t code_len = random_code_len(&brng);
        generate_bytecode(code_buf, CODE_KEY_SEED, code_id, code_len);
        bool ok = dl_put_code(dl, key, code_buf, code_len);
        ASSERT_MSG(ok, "dl_put_code failed block %" PRIu64 " code_id %" PRIu64,
                   block_num, code_id);
    }
    free(code_buf);
    sim->total_code_deploys += code_count;

    // --- Merge ---
    double t0 = now_sec();
    uint64_t merged = dl_merge(dl);
    double t1 = now_sec();
    (void)merged;
    sim->total_merge_time += (t1 - t0);
    sim->total_blocks++;

    // --- Checkpoint every 128 blocks ---
    if (sim->total_blocks % CHECKPOINT_INTERVAL == 0) {
        double ct0 = now_sec();
        bool ok = dl_checkpoint(dl, INDEX_PATH, sim->total_blocks);
        double ct1 = now_sec();
        ASSERT_MSG(ok, "dl_checkpoint failed at block %" PRIu64, sim->total_blocks);
        sim->total_checkpoint_time += (ct1 - ct0);
        sim->last_checkpoint_block = sim->total_blocks;
    }
}

// ============================================================================
// Main
// ============================================================================

int main(int argc, char *argv[]) {
    uint64_t target_millions = 10;
    if (argc >= 2) {
        target_millions = (uint64_t)atoll(argv[1]);
        if (target_millions == 0) target_millions = 10;
    }

    uint64_t target_keys = target_millions * 1000000ULL;

    printf("============================================\n");
    printf("  Data Layer Scale Test\n");
    printf("============================================\n");
    printf("  target:     %" PRIu64 "M keys\n", target_millions);
    printf("  ops/block:  %d-%d (70/20/10 ins/upd/del)\n", OPS_MIN, OPS_MAX);
    printf("  checkpoint: every %d blocks\n", CHECKPOINT_INTERVAL);
    printf("  code:       1-10 deploys/block (%d-%d bytes)\n",
           CODE_SIZE_MIN, CODE_SIZE_MAX);
    printf("  seed:       0x%016" PRIx64 "\n", (uint64_t)MASTER_SEED);
    printf("============================================\n\n");

    // Clean up from prior runs
    unlink(STATE_PATH);
    unlink(CODE_PATH);
    unlink(INDEX_PATH);

    // Create data layer
    data_layer_t *dl = dl_create(STATE_PATH, CODE_PATH, KEY_SIZE, 4);
    ASSERT_MSG(dl != NULL, "dl_create failed");

    // Init simulation state
    sim_state_t sim;
    memset(&sim, 0, sizeof(sim));
    ASSERT_MSG(bitset_init(&sim.state_live, target_keys),
               "bitset_init failed");

    double t_start = now_sec();
    (void)0;

    // ========================================================================
    // Block simulation loop
    // ========================================================================
    printf("--- Block Simulation ---\n\n");

    while (sim.next_state_id < target_keys) {
        uint64_t block_num = sim.total_blocks + 1;
        run_block(dl, &sim, block_num);

        // Stats every STATS_INTERVAL blocks (or every 10 if total < 128)
        uint64_t print_every = (target_keys < 5000000) ? 10 : STATS_INTERVAL;
        bool is_last = (sim.next_state_id >= target_keys);
        if (sim.total_blocks % print_every == 0 || is_last) {
            double now = now_sec();
            double total_elapsed = now - t_start;

            dl_stats_t st = dl_stats(dl);
            size_t state_mb = get_file_size_path(STATE_PATH) / (1024 * 1024);
            size_t code_mb = get_file_size_path(CODE_PATH) / (1024 * 1024);

            double avg_merge_ms = (sim.total_merge_time / sim.total_blocks) * 1000.0;
            double throughput = sim.next_state_id / total_elapsed / 1000.0;

            // Last checkpoint time (or 0 if none this interval)
            double last_ckpt_ms = 0;
            if (sim.last_checkpoint_block == sim.total_blocks) {
                last_ckpt_ms = sim.total_checkpoint_time * 1000.0 /
                    (sim.total_blocks / CHECKPOINT_INTERVAL);
            }

            printf("block %5" PRIu64 " | index %6.2fM | "
                   "merge %5.1fms | ckpt %5.0fms | "
                   "state %4zuMB | code %4zuMB | RSS %4zuMB | "
                   "%6.0fK k/s\n",
                   sim.total_blocks,
                   st.index_keys / 1e6,
                   avg_merge_ms,
                   last_ckpt_ms,
                   state_mb, code_mb, get_rss_mb(),
                   throughput);
            fflush(stdout);
        }
    }

    double block_elapsed = now_sec() - t_start;

    dl_stats_t final_stats = dl_stats(dl);
    printf("\n--- Block Phase Complete ---\n");
    printf("  blocks:       %" PRIu64 "\n", sim.total_blocks);
    printf("  inserts:      %" PRIu64 "\n", sim.total_inserts);
    printf("  updates:      %" PRIu64 "\n", sim.total_updates);
    printf("  deletes:      %" PRIu64 "\n", sim.total_deletes);
    printf("  code deploys: %" PRIu64 "\n", sim.total_code_deploys);
    printf("  index keys:   %" PRIu64 "\n", final_stats.index_keys);
    printf("  code entries: %u\n", final_stats.code_count);
    printf("  total merged: %" PRIu64 "\n", final_stats.total_merged);
    printf("  free slots:   %u\n", final_stats.free_slots);
    printf("  avg merge:    %.2fms\n",
           (sim.total_merge_time / sim.total_blocks) * 1000.0);
    printf("  time:         %.1fs\n", block_elapsed);
    printf("  throughput:   %.0fK keys/s\n",
           sim.next_state_id / block_elapsed / 1000.0);
    printf("  RSS:          %zuMB\n\n", get_rss_mb());

    // ========================================================================
    // Phase A: Checkpoint recovery
    // ========================================================================
    printf("--- Phase A: Checkpoint Recovery ---\n");

    uint64_t ckpt_index_keys = final_stats.index_keys;
    uint32_t ckpt_code_count = final_stats.code_count;

    // Need a final checkpoint if the last block wasn't one
    if (sim.last_checkpoint_block != sim.total_blocks) {
        printf("  writing final checkpoint at block %" PRIu64 "...\n",
               sim.total_blocks);
        bool ok = dl_checkpoint(dl, INDEX_PATH, sim.total_blocks);
        ASSERT_MSG(ok, "final checkpoint failed");
        sim.last_checkpoint_block = sim.total_blocks;
        // Refresh stats after checkpoint
        final_stats = dl_stats(dl);
        ckpt_index_keys = final_stats.index_keys;
        ckpt_code_count = final_stats.code_count;
    }

    // Destroy
    dl_destroy(dl);
    dl = NULL;

    // Recover
    double t_recover_start = now_sec();
    uint64_t recovered_block = 0;
    dl = dl_open(STATE_PATH, CODE_PATH, INDEX_PATH,
                 KEY_SIZE, 4, &recovered_block);
    double t_recover_end = now_sec();

    ASSERT_MSG(dl != NULL, "dl_open failed");
    ASSERT_MSG(recovered_block == sim.last_checkpoint_block,
               "recovered block %" PRIu64 " != expected %" PRIu64,
               recovered_block, sim.last_checkpoint_block);

    dl_stats_t recovered_stats = dl_stats(dl);
    ASSERT_MSG(recovered_stats.index_keys == ckpt_index_keys,
               "index_keys mismatch: got %" PRIu64 " expected %" PRIu64,
               recovered_stats.index_keys, ckpt_index_keys);
    ASSERT_MSG(recovered_stats.code_count == ckpt_code_count,
               "code_count mismatch: got %u expected %u",
               recovered_stats.code_count, ckpt_code_count);

    printf("  dl_open:        %.3fs\n", t_recover_end - t_recover_start);
    printf("  recovered blk:  %" PRIu64 "\n", recovered_block);
    printf("  index_keys:     %" PRIu64 " (expected %" PRIu64 ")\n",
           recovered_stats.index_keys, ckpt_index_keys);
    printf("  code_count:     %u (expected %u)\n",
           recovered_stats.code_count, ckpt_code_count);
    printf("  checkpoint recovery: OK\n\n");

    // ========================================================================
    // Phase B: State key sampling verification
    // ========================================================================
    printf("--- Phase B: State Key Verification ---\n");

    uint64_t sample_count = 100000;
    if (sample_count > sim.next_state_id) sample_count = sim.next_state_id;

    rng_t vrng = rng_create(MASTER_SEED ^ 0x5645524946590000ULL);
    uint64_t verify_ok = 0;
    uint64_t verify_found = 0;
    uint64_t verify_absent = 0;
    uint8_t key[KEY_SIZE];
    uint8_t val_buf[128];
    uint8_t got_buf[128];

    for (uint64_t i = 0; i < sample_count; i++) {
        uint64_t key_id = rng_next(&vrng) % sim.next_state_id;
        generate_key(key, STATE_KEY_SEED, key_id);

        uint16_t got_len = 0;
        bool found = dl_get(dl, key, got_buf, &got_len);

        if (bitset_test(&sim.state_live, key_id)) {
            // Should be found — verify value content
            ASSERT_MSG(found,
                       "state key %" PRIu64 " expected present but dl_get returned false",
                       key_id);

            // Regenerate expected value
            uint16_t expected_len = sim.value_lens[key_id];
            uint64_t expected_block = sim.value_versions[key_id];
            generate_value(val_buf, value_seed(expected_block), key_id, expected_len);

            ASSERT_MSG(got_len == expected_len,
                       "state key %" PRIu64 " len mismatch: got %u expected %u",
                       key_id, got_len, expected_len);
            ASSERT_MSG(memcmp(got_buf, val_buf, expected_len) == 0,
                       "state key %" PRIu64 " data mismatch", key_id);
            verify_found++;
        } else {
            // Should NOT be found
            ASSERT_MSG(!found,
                       "state key %" PRIu64 " expected absent but dl_get returned true",
                       key_id);
            verify_absent++;
        }
        verify_ok++;
    }

    printf("  sampled:  %" PRIu64 " keys\n", sample_count);
    printf("  found:    %" PRIu64 "\n", verify_found);
    printf("  absent:   %" PRIu64 "\n", verify_absent);
    printf("  state verification: %" PRIu64 "/%" PRIu64 " OK\n\n",
           verify_ok, sample_count);

    // ========================================================================
    // Phase C: Code verification
    // ========================================================================
    printf("--- Phase C: Code Verification ---\n");

    uint64_t code_sample = 1000;
    if (code_sample > sim.next_code_id) code_sample = sim.next_code_id;

    rng_t crng = rng_create(MASTER_SEED ^ 0xC0DE5EED00000000ULL);
    uint64_t code_ok = 0;
    uint8_t *code_expected = malloc(CODE_SIZE_MAX);
    uint8_t *code_got = malloc(CODE_SIZE_MAX);

    for (uint64_t i = 0; i < code_sample; i++) {
        uint64_t code_id = rng_next(&crng) % sim.next_code_id;
        generate_key(key, CODE_KEY_SEED, code_id);

        // Regenerate expected bytecode — we need the original brng state
        // to know the code length. Replay the block's brng to find it.
        // Simpler: regenerate with same seed used in generate_bytecode.
        // The code_len was generated from random_code_len with block rng,
        // but we need to recover it. Since code is content-addressed and
        // we stored it once, we can verify by checking dl_code_length.
        uint32_t actual_len = dl_code_length(dl, key);
        ASSERT_MSG(actual_len > 0,
                   "code key %" PRIu64 " dl_code_length returned 0", code_id);
        ASSERT_MSG(actual_len >= CODE_SIZE_MIN && actual_len <= CODE_SIZE_MAX,
                   "code key %" PRIu64 " length %u out of range", code_id, actual_len);

        // Read code
        uint32_t got_code_len = 0;
        bool found = dl_get_code(dl, key, code_got, &got_code_len);
        ASSERT_MSG(found, "code key %" PRIu64 " dl_get_code returned false", code_id);
        ASSERT_MSG(got_code_len == actual_len,
                   "code key %" PRIu64 " length mismatch: get=%u length=%u",
                   code_id, got_code_len, actual_len);

        // Verify content
        generate_bytecode(code_expected, CODE_KEY_SEED, code_id, actual_len);
        ASSERT_MSG(memcmp(code_got, code_expected, actual_len) == 0,
                   "code key %" PRIu64 " data mismatch", code_id);

        code_ok++;
    }

    free(code_expected);
    free(code_got);

    printf("  sampled:  %" PRIu64 " code entries\n", code_sample);
    printf("  code verification: %" PRIu64 "/%" PRIu64 " OK\n\n",
           code_ok, code_sample);

    // ========================================================================
    // Cleanup
    // ========================================================================

    double total_time = now_sec() - t_start;
    size_t peak_rss = get_rss_mb();

    dl_destroy(dl);
    bitset_destroy(&sim.state_live);
    free(sim.value_lens);
    free(sim.value_versions);
    unlink(STATE_PATH);
    unlink(CODE_PATH);
    unlink(INDEX_PATH);

    printf("============================================\n");
    printf("  ALL PHASES PASSED\n");
    printf("============================================\n");
    printf("  total time:    %.1fs\n", total_time);
    printf("  total keys:    %" PRIu64 "M\n", sim.next_state_id / 1000000);
    printf("  total blocks:  %" PRIu64 "\n", sim.total_blocks);
    printf("  total code:    %" PRIu64 "\n", sim.next_code_id);
    printf("  throughput:    %.0fK keys/s\n",
           sim.next_state_id / total_time / 1000.0);
    printf("  peak RSS:      %zuMB\n", peak_rss);
    printf("============================================\n");

    return 0;
}
