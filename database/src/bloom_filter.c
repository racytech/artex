#include "bloom_filter.h"
#include <stdlib.h>
#include <string.h>
#include <math.h>

/* =========================================================================
 * MurmurHash3 64-bit finalizer (used for double hashing)
 * ========================================================================= */

static inline uint64_t murmur_mix(uint64_t k) {
    k ^= k >> 33;
    k *= 0xff51afd7ed558ccdULL;
    k ^= k >> 33;
    k *= 0xc4ceb9fe1a85ec53ULL;
    k ^= k >> 33;
    return k;
}

/* Derive two independent hashes from the key bytes.
 * Reads first 8 and next 8 bytes (zero-padded if shorter). */
static void hash_key(const uint8_t *key, size_t key_len,
                     uint64_t *h1_out, uint64_t *h2_out) {
    uint64_t a = 0, b = 0;
    if (key_len >= 8)
        memcpy(&a, key, 8);
    else
        memcpy(&a, key, key_len);
    if (key_len >= 16)
        memcpy(&b, key + 8, 8);
    else if (key_len > 8)
        memcpy(&b, key + 8, key_len - 8);

    *h1_out = murmur_mix(a);
    *h2_out = murmur_mix(b ^ 0x9e3779b97f4a7c15ULL); /* golden ratio seed */
}

/* =========================================================================
 * Bloom filter internals
 * ========================================================================= */

#define NUM_HASHES 7

struct bloom_filter {
    uint64_t *bits;       /* bit array as uint64_t words */
    uint64_t  num_bits;   /* total bits (multiple of 64) */
    uint64_t  num_words;  /* num_bits / 64 */
};

bloom_filter_t *bloom_filter_create(uint64_t capacity, double fpr) {
    if (capacity == 0) capacity = 1;
    if (fpr <= 0.0) fpr = 0.001;
    if (fpr >= 1.0) fpr = 0.5;

    /* Optimal bits: m = -n * ln(p) / (ln(2)^2) */
    double m = -(double)capacity * log(fpr) / (log(2.0) * log(2.0));
    uint64_t num_bits = (uint64_t)ceil(m);

    /* Round up to multiple of 64 */
    num_bits = (num_bits + 63) & ~63ULL;
    if (num_bits < 64) num_bits = 64;

    uint64_t num_words = num_bits / 64;

    bloom_filter_t *bf = malloc(sizeof(*bf));
    if (!bf) return NULL;

    bf->bits = calloc(num_words, sizeof(uint64_t));
    if (!bf->bits) { free(bf); return NULL; }

    bf->num_bits  = num_bits;
    bf->num_words = num_words;
    return bf;
}

void bloom_filter_destroy(bloom_filter_t *bf) {
    if (!bf) return;
    free(bf->bits);
    free(bf);
}

void bloom_filter_add(bloom_filter_t *bf, const uint8_t *key, size_t key_len) {
    uint64_t h1, h2;
    hash_key(key, key_len, &h1, &h2);

    for (int i = 0; i < NUM_HASHES; i++) {
        uint64_t pos = (h1 + (uint64_t)i * h2) % bf->num_bits;
        bf->bits[pos / 64] |= (1ULL << (pos % 64));
    }
}

bool bloom_filter_maybe_contains(const bloom_filter_t *bf,
                                  const uint8_t *key, size_t key_len) {
    uint64_t h1, h2;
    hash_key(key, key_len, &h1, &h2);

    for (int i = 0; i < NUM_HASHES; i++) {
        uint64_t pos = (h1 + (uint64_t)i * h2) % bf->num_bits;
        if (!(bf->bits[pos / 64] & (1ULL << (pos % 64))))
            return false;
    }
    return true;
}

void bloom_filter_clear(bloom_filter_t *bf) {
    if (!bf) return;
    memset(bf->bits, 0, bf->num_words * sizeof(uint64_t));
}

uint64_t bloom_filter_popcount(const bloom_filter_t *bf) {
    if (!bf) return 0;
    uint64_t count = 0;
    for (uint64_t i = 0; i < bf->num_words; i++)
        count += (uint64_t)__builtin_popcountll(bf->bits[i]);
    return count;
}

size_t bloom_filter_memory(const bloom_filter_t *bf) {
    if (!bf) return 0;
    return sizeof(*bf) + bf->num_words * sizeof(uint64_t);
}
