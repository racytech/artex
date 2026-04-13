/**
 * Benchmark: serial vs parallel tx decode + ecrecover.
 *
 * Creates N signed legacy transactions, then:
 *   1. Decodes all serially, measures wall time
 *   2. Decodes all in parallel (N_THREADS), measures wall time
 *   3. Verifies all senders match
 */
#include "tx_decoder.h"
#include "secp256k1_wrap.h"
#include "hash.h"
#include "rlp.h"
#include "uint256.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <pthread.h>

#define N_TXS       512
#define N_THREADS   8

/* Known private key for test signing */
static const uint8_t TEST_KEY[32] = {
    0x01,0x02,0x03,0x04,0x05,0x06,0x07,0x08,
    0x09,0x0a,0x0b,0x0c,0x0d,0x0e,0x0f,0x10,
    0x11,0x12,0x13,0x14,0x15,0x16,0x17,0x18,
    0x19,0x1a,0x1b,0x1c,0x1d,0x1e,0x1f,0x20,
};

/* Build a signed legacy tx RLP bytes */
static bool build_signed_tx(uint64_t nonce, uint8_t **out, size_t *out_len) {
    uint8_t to[20] = {0xde, 0xad, 0xbe, 0xef};

    /* Build unsigned tx items: [nonce, gasPrice, gasLimit, to, value, data, chainId, 0, 0] */
    rlp_item_t *unsigned_list = rlp_list_new();

    /* nonce */
    uint8_t nonce_be[8]; int nlen = 0;
    { uint64_t n = nonce; if (n == 0) { nonce_be[0] = 0; nlen = 0; }
      else { while (n) { nonce_be[7 - nlen++] = n & 0xff; n >>= 8; } } }
    rlp_list_append(unsigned_list, rlp_string(nlen ? nonce_be + 8 - nlen : NULL, nlen));

    /* gasPrice = 20 gwei */
    uint8_t gp[] = {0x04, 0xa8, 0x17, 0xc8, 0x00};
    rlp_list_append(unsigned_list, rlp_string(gp, 5));

    /* gasLimit = 21000 */
    uint8_t gl[] = {0x52, 0x08};
    rlp_list_append(unsigned_list, rlp_string(gl, 2));

    /* to */
    rlp_list_append(unsigned_list, rlp_string(to, 20));

    /* value = 1 ETH */
    uint8_t val[] = {0x0d, 0xe0, 0xb6, 0xb3, 0xa7, 0x64, 0x00, 0x00};
    rlp_list_append(unsigned_list, rlp_string(val, 8));

    /* data = empty */
    rlp_list_append(unsigned_list, rlp_string(NULL, 0));

    /* EIP-155: chainId=1, 0, 0 */
    uint8_t one = 1;
    rlp_list_append(unsigned_list, rlp_string(&one, 1));
    rlp_list_append(unsigned_list, rlp_string(NULL, 0));
    rlp_list_append(unsigned_list, rlp_string(NULL, 0));

    bytes_t unsigned_enc = rlp_encode(unsigned_list);

    /* Hash for signing */
    hash_t sig_hash = hash_keccak256(unsigned_enc.data, unsigned_enc.len);

    /* Sign */
    uint8_t sig[64];
    int recid;
    if (!secp256k1_wrap_sign(sig, &recid, sig_hash.bytes, TEST_KEY)) {
        rlp_item_free(unsigned_list);
        free(unsigned_enc.data);
        return false;
    }

    /* Build signed tx: [nonce, gasPrice, gasLimit, to, value, data, v, r, s] */
    rlp_item_t *signed_list = rlp_list_new();

    /* Reuse first 6 items */
    rlp_list_append(signed_list, rlp_string(nlen ? nonce_be + 8 - nlen : NULL, nlen));
    rlp_list_append(signed_list, rlp_string(gp, 5));
    rlp_list_append(signed_list, rlp_string(gl, 2));
    rlp_list_append(signed_list, rlp_string(to, 20));
    rlp_list_append(signed_list, rlp_string(val, 8));
    rlp_list_append(signed_list, rlp_string(NULL, 0));

    /* v = recid + 37 (EIP-155, chain_id=1) */
    uint8_t v = (uint8_t)(recid + 37);
    rlp_list_append(signed_list, rlp_string(&v, 1));

    /* r (strip leading zeros) */
    int r_start = 0;
    while (r_start < 31 && sig[r_start] == 0) r_start++;
    rlp_list_append(signed_list, rlp_string(sig + r_start, 32 - r_start));

    /* s (strip leading zeros) */
    int s_start = 0;
    while (s_start < 31 && sig[32 + s_start] == 0) s_start++;
    rlp_list_append(signed_list, rlp_string(sig + 32 + s_start, 32 - s_start));

    bytes_t signed_enc = rlp_encode(signed_list);
    *out = signed_enc.data;
    *out_len = signed_enc.len;

    rlp_item_free(unsigned_list);
    rlp_item_free(signed_list);
    free(unsigned_enc.data);
    return true;
}

/* Parallel decode context */
typedef struct {
    const uint8_t **txs;
    const size_t   *lens;
    transaction_t  *results;
    bool           *valid;
    uint64_t        chain_id;
    size_t          start, end;
} decode_work_t;

static void *decode_worker(void *arg) {
    decode_work_t *w = (decode_work_t *)arg;
    for (size_t i = w->start; i < w->end; i++)
        w->valid[i] = tx_decode_raw(&w->results[i], w->txs[i], w->lens[i], w->chain_id);
    return NULL;
}

static double now_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec * 1000.0 + ts.tv_nsec / 1e6;
}

int main(void) {
    secp256k1_wrap_init();

    printf("Building %d signed transactions...\n", N_TXS);
    uint8_t *txs[N_TXS];
    size_t lens[N_TXS];
    for (int i = 0; i < N_TXS; i++) {
        if (!build_signed_tx(i, &txs[i], &lens[i])) {
            fprintf(stderr, "Failed to build tx %d\n", i);
            return 1;
        }
    }
    printf("  avg tx size: %zu bytes\n", lens[0]);

    /* === Serial decode === */
    transaction_t serial_res[N_TXS];
    bool serial_ok[N_TXS];

    double t0 = now_ms();
    for (int i = 0; i < N_TXS; i++)
        serial_ok[i] = tx_decode_raw(&serial_res[i], txs[i], lens[i], 1);
    double t1 = now_ms();

    int serial_valid = 0;
    for (int i = 0; i < N_TXS; i++) if (serial_ok[i]) serial_valid++;
    printf("\nSerial:   %d/%d valid, %.1fms (%.0f us/tx)\n",
           serial_valid, N_TXS, t1 - t0, (t1 - t0) * 1000.0 / N_TXS);

    /* === Parallel decode === */
    transaction_t par_res[N_TXS];
    bool par_ok[N_TXS];

    decode_work_t workers[N_THREADS];
    pthread_t tids[N_THREADS];
    size_t chunk = N_TXS / N_THREADS;

    double t2 = now_ms();
    for (int t = 0; t < N_THREADS; t++) {
        workers[t] = (decode_work_t){
            .txs = (const uint8_t **)txs,
            .lens = lens,
            .results = par_res,
            .valid = par_ok,
            .chain_id = 1,
            .start = t * chunk,
            .end = (t == N_THREADS - 1) ? N_TXS : (t + 1) * chunk,
        };
        pthread_create(&tids[t], NULL, decode_worker, &workers[t]);
    }
    for (int t = 0; t < N_THREADS; t++)
        pthread_join(tids[t], NULL);
    double t3 = now_ms();

    int par_valid = 0;
    for (int i = 0; i < N_TXS; i++) if (par_ok[i]) par_valid++;
    printf("Parallel: %d/%d valid, %.1fms (%.0f us/tx), %d threads\n",
           par_valid, N_TXS, t3 - t2, (t3 - t2) * 1000.0 / N_TXS, N_THREADS);
    printf("Speedup:  %.1fx\n", (t1 - t0) / (t3 - t2));

    /* === Verify senders match === */
    int mismatches = 0;
    for (int i = 0; i < N_TXS; i++) {
        if (!serial_ok[i] || !par_ok[i]) continue;
        if (memcmp(serial_res[i].sender.bytes, par_res[i].sender.bytes, 20) != 0) {
            fprintf(stderr, "MISMATCH tx %d\n", i);
            mismatches++;
        }
    }

    /* Cleanup */
    for (int i = 0; i < N_TXS; i++) {
        if (serial_ok[i]) tx_decoded_free(&serial_res[i]);
        if (par_ok[i]) tx_decoded_free(&par_res[i]);
        free(txs[i]);
    }

    printf("\n=== %s ===\n", mismatches ? "FAIL" : "PASS");
    return mismatches ? 1 : 0;
}
