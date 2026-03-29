/*
 * Account Trie — MPT root hash from flat_state's account compact_art.
 *
 * The encode callback reads compressed account records from flat_store's
 * mmap'd data file, decodes to {nonce, balance, code_hash, storage_root},
 * then produces the Ethereum account RLP.
 */

#include "account_trie.h"
#include "art_mpt.h"
#include "flat_store.h"
#include "flat_state.h"

#include <stdlib.h>
#include <string.h>

struct account_trie {
    art_mpt_t    *mpt;
    flat_store_t *store;   /* non-owning — for reading account records */
};

/* =========================================================================
 * RLP helpers (same encoding as evm_state.c mpt_rlp_account)
 * ========================================================================= */

static const uint8_t EMPTY_CODE_HASH[32] = {
    0xc5, 0xd2, 0x46, 0x01, 0x86, 0xf7, 0x23, 0x3c,
    0x92, 0x7e, 0x7d, 0xb2, 0xdc, 0xc7, 0x03, 0xc0,
    0xe5, 0x00, 0xb6, 0x53, 0xca, 0x82, 0x27, 0x3b,
    0x7b, 0xfa, 0xd8, 0x04, 0x5d, 0x85, 0xa4, 0x70,
};

static const uint8_t EMPTY_STORAGE_ROOT[32] = {
    0x56, 0xe8, 0x1f, 0x17, 0x1b, 0xcc, 0x55, 0xa6,
    0xff, 0x83, 0x45, 0xe6, 0x92, 0xc0, 0xf8, 0x6e,
    0x5b, 0x48, 0xe0, 0x1b, 0x99, 0x6c, 0xad, 0xc0,
    0x01, 0x62, 0x2f, 0xb5, 0xe3, 0x63, 0xb4, 0x21,
};

/* RLP-encode a uint64 as big-endian integer */
static size_t rlp_u64(uint64_t v, uint8_t *out) {
    if (v == 0) { out[0] = 0x80; return 1; }
    if (v < 0x80) { out[0] = (uint8_t)v; return 1; }

    uint8_t be[8];
    int len = 0;
    uint64_t tmp = v;
    while (tmp > 0) { be[7 - len++] = (uint8_t)(tmp & 0xFF); tmp >>= 8; }
    out[0] = 0x80 + (uint8_t)len;
    memcpy(out + 1, be + 8 - len, len);
    return 1 + len;
}

/* RLP-encode a big-endian byte string (strip leading zeros) */
static size_t rlp_be(const uint8_t *be, size_t be_len, uint8_t *out) {
    size_t i = 0;
    while (i < be_len && be[i] == 0) i++;
    size_t len = be_len - i;

    if (len == 0)             { out[0] = 0x80;          return 1; }
    if (len == 1 && be[i] < 0x80) { out[0] = be[i];    return 1; }
    out[0] = 0x80 + (uint8_t)len;
    memcpy(out + 1, be + i, len);
    return 1 + len;
}

/* =========================================================================
 * Decode flat_state compressed account
 * ========================================================================= */

#define ACCT_FLAG_HAS_NONCE      0x01
#define ACCT_FLAG_HAS_BALANCE    0x02
#define ACCT_FLAG_BALANCE_FULL   0x04
#define ACCT_FLAG_HAS_CODE       0x08
#define ACCT_FLAG_HAS_STORAGE    0x10
#define ACCT_FLAG_NONCE_BIG      0x20

static void decode_account(const uint8_t *buf, uint32_t len,
                            flat_account_record_t *out) {
    memset(out, 0, sizeof(*out));
    if (len == 0) return;

    uint8_t flags = buf[0];
    uint32_t pos = 1;

    if (flags & ACCT_FLAG_HAS_NONCE) {
        if (flags & ACCT_FLAG_NONCE_BIG) {
            memcpy(&out->nonce, buf + pos, 8); pos += 8;
        } else {
            uint32_t n; memcpy(&n, buf + pos, 4); out->nonce = n; pos += 4;
        }
    }
    if (flags & ACCT_FLAG_HAS_BALANCE) {
        if (flags & ACCT_FLAG_BALANCE_FULL) {
            memcpy(out->balance, buf + pos, 32); pos += 32;
        } else {
            memcpy(out->balance + 24, buf + pos, 8); pos += 8;
        }
    }
    if (flags & ACCT_FLAG_HAS_CODE) {
        memcpy(out->code_hash, buf + pos, 32); pos += 32;
    } else {
        memcpy(out->code_hash, EMPTY_CODE_HASH, 32);
    }
    if (flags & ACCT_FLAG_HAS_STORAGE) {
        memcpy(out->storage_root, buf + pos, 32); pos += 32;
    } else {
        memcpy(out->storage_root, EMPTY_STORAGE_ROOT, 32);
    }
    (void)len;
}

/* =========================================================================
 * Encode callback: flat_store leaf → account RLP
 * ========================================================================= */

static uint32_t account_value_encode(const uint8_t *key,
                                      const void *leaf_val,
                                      uint32_t val_size,
                                      uint8_t *rlp_out,
                                      void *user_ctx) {
    (void)key;
    (void)val_size;
    account_trie_t *at = user_ctx;

    /* Read compressed account from flat_store */
    uint8_t raw[128];
    uint32_t raw_len = flat_store_read_leaf_record(at->store, leaf_val,
                                                    raw, sizeof(raw));
    if (raw_len == 0) return 0;

    /* Decode compressed → flat_account_record_t */
    flat_account_record_t rec;
    decode_account(raw, raw_len, &rec);

    /* Build account RLP: [nonce, balance, storage_root, code_hash] */
    uint8_t payload[120];
    size_t pos = 0;
    pos += rlp_u64(rec.nonce, payload + pos);
    pos += rlp_be(rec.balance, 32, payload + pos);
    payload[pos++] = 0xa0; memcpy(payload + pos, rec.storage_root, 32); pos += 32;
    payload[pos++] = 0xa0; memcpy(payload + pos, rec.code_hash, 32);   pos += 32;

    /* List wrapper */
    if (pos <= 55) {
        rlp_out[0] = 0xc0 + (uint8_t)pos;
        memcpy(rlp_out + 1, payload, pos);
        return 1 + (uint32_t)pos;
    } else {
        rlp_out[0] = 0xf8;
        rlp_out[1] = (uint8_t)pos;
        memcpy(rlp_out + 2, payload, pos);
        return 2 + (uint32_t)pos;
    }
}

/* =========================================================================
 * Public API
 * ========================================================================= */

account_trie_t *account_trie_create(compact_art_t *account_art,
                                      flat_store_t *account_store) {
    if (!account_art || !account_store) return NULL;

    account_trie_t *at = calloc(1, sizeof(*at));
    if (!at) return NULL;

    at->store = account_store;
    at->mpt = art_mpt_create(account_art, account_value_encode, at);
    if (!at->mpt) {
        free(at);
        return NULL;
    }

    return at;
}

void account_trie_destroy(account_trie_t *at) {
    if (!at) return;
    art_mpt_destroy(at->mpt);
    free(at);
}

void account_trie_root(account_trie_t *at, uint8_t out[32]) {
    if (!at) return;
    art_mpt_root_hash(at->mpt, out);
}

void account_trie_invalidate_all(account_trie_t *at) {
    if (!at) return;
    art_mpt_invalidate_all(at->mpt);
}
