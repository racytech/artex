#include "../include/account.h"

#include <string.h>

// ============================================================================
// Helpers
// ============================================================================

// Returns number of significant bytes in a uint64 (0 if value is 0)
static uint8_t uint64_byte_len(uint64_t v) {
    if (v == 0) return 0;
    uint8_t n = 0;
    uint64_t tmp = v;
    while (tmp > 0) { n++; tmp >>= 8; }
    return n;
}

// Returns number of significant bytes in a big-endian byte array
static uint8_t bignum_byte_len(const uint8_t *buf, uint8_t max_len) {
    for (uint8_t i = 0; i < max_len; i++) {
        if (buf[i] != 0) return max_len - i;
    }
    return 0;
}

// Encode uint64 as big-endian, no leading zeros. Returns bytes written.
static uint8_t encode_uint64_be(uint64_t v, uint8_t *out) {
    uint8_t n = uint64_byte_len(v);
    for (uint8_t i = 0; i < n; i++) {
        out[n - 1 - i] = (uint8_t)(v & 0xFF);
        v >>= 8;
    }
    return n;
}

// Decode big-endian bytes into uint64.
static uint64_t decode_uint64_be(const uint8_t *buf, uint8_t len) {
    uint64_t v = 0;
    for (uint8_t i = 0; i < len; i++) {
        v = (v << 8) | buf[i];
    }
    return v;
}

// ============================================================================
// Public API
// ============================================================================

account_t account_empty(void) {
    account_t acct;
    memset(&acct, 0, sizeof(acct));
    acct.balance = UINT256_ZERO_INIT;
    acct.code_hash = hash_zero();
    acct.has_code = false;
    acct.nonce = 0;
    return acct;
}

uint16_t account_encode(const account_t *acct, uint8_t *out) {
    if (!acct || !out) return 0;

    // Convert balance to 32-byte big-endian
    uint8_t bal_bytes[32];
    uint256_to_bytes(&acct->balance, bal_bytes);

    uint8_t nonce_len = uint64_byte_len(acct->nonce);
    uint8_t bal_len = bignum_byte_len(bal_bytes, 32);

    if (nonce_len > 8 || bal_len > 32) return 0;

    // Flags byte
    uint8_t flags = 0;
    if (acct->has_code) flags |= 0x01;
    flags |= (nonce_len & 0x0F) << 1;

    out[0] = flags;
    out[1] = bal_len;

    uint16_t pos = 2;

    // Nonce (big-endian, no leading zeros)
    encode_uint64_be(acct->nonce, out + pos);
    pos += nonce_len;

    // Balance (big-endian, no leading zeros)
    if (bal_len > 0) {
        memcpy(out + pos, bal_bytes + (32 - bal_len), bal_len);
        pos += bal_len;
    }

    // Code hash
    if (acct->has_code) {
        memcpy(out + pos, acct->code_hash.bytes, HASH_SIZE);
        pos += HASH_SIZE;
    }

    return pos;
}

bool account_decode(const uint8_t *buf, uint16_t len, account_t *out) {
    if (!buf || len < 2) return false;

    uint8_t flags = buf[0];
    uint8_t bal_len = buf[1];

    bool has_code = (flags & 0x01) != 0;
    uint8_t nonce_len = (flags >> 1) & 0x0F;

    if (nonce_len > 8) return false;
    if (bal_len > 32) return false;

    uint16_t expected = 2 + nonce_len + bal_len + (has_code ? HASH_SIZE : 0);
    if (len != expected) return false;

    uint16_t pos = 2;

    // Nonce
    uint64_t nonce = decode_uint64_be(buf + pos, nonce_len);
    pos += nonce_len;

    // Balance (expand to 32-byte big-endian, then convert to uint256_t)
    uint8_t bal_bytes[32];
    memset(bal_bytes, 0, 32);
    if (bal_len > 0) {
        memcpy(bal_bytes + (32 - bal_len), buf + pos, bal_len);
    }
    pos += bal_len;

    // Code hash
    hash_t code_hash = hash_zero();
    if (has_code) {
        code_hash = hash_from_bytes(buf + pos);
    }

    if (out) {
        out->nonce = nonce;
        out->balance = uint256_from_bytes(bal_bytes, 32);
        out->code_hash = code_hash;
        out->has_code = has_code;
    }

    return true;
}
