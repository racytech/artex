/*
 * Account Encoding PoC Test
 *
 * Compact variable-length encoding for Ethereum accounts.
 * No storage_root — derived by MPT hash tree from flat storage slots.
 *
 * Layout:
 *   [1B flags] [1B balance_len] [nonce bytes] [balance bytes] [code_hash]
 *
 *   flags byte:
 *     bit 0:    has_code (code_hash follows at end, 32 bytes)
 *     bits 1-4: nonce_len (0 = nonce is zero, 1-8 = N bytes follow)
 *     bits 5-7: reserved
 *
 *   balance_len byte: 0-32 (0 = balance is zero)
 *
 *   nonce: big-endian, no leading zeros
 *   balance: big-endian, no leading zeros
 *   code_hash: 32 bytes, only if has_code
 *
 * Max encoded size: 2 + 8 + 32 + 32 = 74 bytes (theoretical)
 * Max realistic:    2 + 8 + 12 + 32 = 54 bytes (fits in 62-byte slot)
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <stdbool.h>
#include <inttypes.h>

// ============================================================================
// Constants
// ============================================================================

#define ACCOUNT_MAX_ENCODED  74   // theoretical max
#define ACCOUNT_MAX_BALANCE  32   // uint256
#define CODE_HASH_SIZE       32
#define SLOT_MAX_VALUE       62   // state_store limit

// ============================================================================
// Encoding
// ============================================================================

// Returns number of significant bytes in a uint64 (0 if value is 0)
static uint8_t uint64_byte_len(uint64_t v) {
    if (v == 0) return 0;
    uint8_t n = 0;
    uint64_t tmp = v;
    while (tmp > 0) { n++; tmp >>= 8; }
    return n;
}

// Returns number of significant bytes in a big-endian byte array (0 if all zeros)
static uint8_t bignum_byte_len(const uint8_t *buf, uint8_t max_len) {
    for (uint8_t i = 0; i < max_len; i++) {
        if (buf[i] != 0) return max_len - i;
    }
    return 0;
}

// Encode uint64 as big-endian, no leading zeros, into out. Returns bytes written.
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

/**
 * Encode an Ethereum account.
 *
 * @param nonce      Account nonce
 * @param balance    Balance as big-endian bytes (32 bytes, may have leading zeros)
 * @param code_hash  Code hash (32 bytes), or NULL for EOA
 * @param out        Output buffer (must be >= ACCOUNT_MAX_ENCODED bytes)
 * @return           Encoded length, or 0 on error
 */
static uint16_t account_encode(uint64_t nonce,
                                const uint8_t balance[ACCOUNT_MAX_BALANCE],
                                const uint8_t *code_hash,
                                uint8_t *out) {
    uint8_t nonce_len = uint64_byte_len(nonce);
    uint8_t bal_len = balance ? bignum_byte_len(balance, ACCOUNT_MAX_BALANCE) : 0;

    if (nonce_len > 8 || bal_len > ACCOUNT_MAX_BALANCE) return 0;

    // Flags byte
    uint8_t flags = 0;
    if (code_hash) flags |= 0x01;          // bit 0: has_code
    flags |= (nonce_len & 0x0F) << 1;      // bits 1-4: nonce_len

    out[0] = flags;
    out[1] = bal_len;

    uint16_t pos = 2;

    // Nonce (big-endian, no leading zeros)
    encode_uint64_be(nonce, out + pos);
    pos += nonce_len;

    // Balance (big-endian, no leading zeros — strip from the 32-byte input)
    if (bal_len > 0 && balance) {
        memcpy(out + pos, balance + (ACCOUNT_MAX_BALANCE - bal_len), bal_len);
        pos += bal_len;
    }

    // Code hash
    if (code_hash) {
        memcpy(out + pos, code_hash, CODE_HASH_SIZE);
        pos += CODE_HASH_SIZE;
    }

    return pos;
}

/**
 * Decode an Ethereum account.
 *
 * @param buf            Encoded bytes
 * @param len            Length of encoded bytes
 * @param out_nonce      Output nonce
 * @param out_balance    Output balance as 32-byte big-endian (zero-padded)
 * @param out_code_hash  Output code hash (32 bytes), zeroed if no code
 * @param out_has_code   Output: true if account has code
 * @return               true on success, false on malformed data
 */
static bool account_decode(const uint8_t *buf, uint16_t len,
                            uint64_t *out_nonce,
                            uint8_t out_balance[ACCOUNT_MAX_BALANCE],
                            uint8_t out_code_hash[CODE_HASH_SIZE],
                            bool *out_has_code) {
    if (len < 2) return false;

    uint8_t flags = buf[0];
    uint8_t bal_len = buf[1];

    bool has_code = (flags & 0x01) != 0;
    uint8_t nonce_len = (flags >> 1) & 0x0F;

    if (nonce_len > 8) return false;
    if (bal_len > ACCOUNT_MAX_BALANCE) return false;

    uint16_t expected = 2 + nonce_len + bal_len + (has_code ? CODE_HASH_SIZE : 0);
    if (len != expected) return false;

    uint16_t pos = 2;

    // Nonce
    if (out_nonce) {
        *out_nonce = decode_uint64_be(buf + pos, nonce_len);
    }
    pos += nonce_len;

    // Balance (expand to 32-byte big-endian)
    if (out_balance) {
        memset(out_balance, 0, ACCOUNT_MAX_BALANCE);
        if (bal_len > 0) {
            memcpy(out_balance + (ACCOUNT_MAX_BALANCE - bal_len), buf + pos, bal_len);
        }
    }
    pos += bal_len;

    // Code hash
    if (out_has_code) *out_has_code = has_code;
    if (out_code_hash) {
        if (has_code) {
            memcpy(out_code_hash, buf + pos, CODE_HASH_SIZE);
        } else {
            memset(out_code_hash, 0, CODE_HASH_SIZE);
        }
    }

    return true;
}

// ============================================================================
// Test helpers
// ============================================================================

static int tests_passed = 0;
static int tests_failed = 0;

#define CHECK(cond, fmt, ...) do { \
    if (!(cond)) { \
        printf("  FAIL [%s:%d]: " fmt "\n", __func__, __LINE__, ##__VA_ARGS__); \
        tests_failed++; \
        return; \
    } \
} while(0)

#define PASS() do { tests_passed++; } while(0)

// Create a balance from a uint64 (for convenience)
static void balance_from_u64(uint64_t v, uint8_t out[ACCOUNT_MAX_BALANCE]) {
    memset(out, 0, ACCOUNT_MAX_BALANCE);
    for (int i = 0; i < 8; i++) {
        out[ACCOUNT_MAX_BALANCE - 1 - i] = (uint8_t)(v & 0xFF);
        v >>= 8;
    }
}

// Create a balance from a hex string (for large values)
static void balance_from_hex(const char *hex, uint8_t out[ACCOUNT_MAX_BALANCE]) {
    memset(out, 0, ACCOUNT_MAX_BALANCE);
    size_t hex_len = strlen(hex);
    size_t byte_len = (hex_len + 1) / 2;
    if (byte_len > ACCOUNT_MAX_BALANCE) byte_len = ACCOUNT_MAX_BALANCE;

    size_t offset = ACCOUNT_MAX_BALANCE - byte_len;
    for (size_t i = 0; i < hex_len; i += 2) {
        unsigned int byte;
        sscanf(hex + i, "%2x", &byte);
        out[offset + i / 2] = (uint8_t)byte;
    }
}

// Fill code_hash with a pattern
static void make_code_hash(uint8_t out[CODE_HASH_SIZE], uint8_t pattern) {
    memset(out, pattern, CODE_HASH_SIZE);
}

// ============================================================================
// Tests
// ============================================================================

static void test_empty_eoa(void) {
    uint8_t buf[ACCOUNT_MAX_ENCODED];
    uint8_t balance[ACCOUNT_MAX_BALANCE] = {0};

    uint16_t len = account_encode(0, balance, NULL, buf);
    CHECK(len == 2, "expected 2 bytes, got %u", len);
    CHECK(len <= SLOT_MAX_VALUE, "doesn't fit in slot: %u > %u", len, SLOT_MAX_VALUE);

    uint64_t nonce;
    uint8_t bal[ACCOUNT_MAX_BALANCE], ch[CODE_HASH_SIZE];
    bool has_code;
    CHECK(account_decode(buf, len, &nonce, bal, ch, &has_code), "decode failed");
    CHECK(nonce == 0, "nonce = %" PRIu64, nonce);
    CHECK(!has_code, "should not have code");

    uint8_t zero_bal[ACCOUNT_MAX_BALANCE] = {0};
    CHECK(memcmp(bal, zero_bal, ACCOUNT_MAX_BALANCE) == 0, "balance not zero");

    printf("  empty EOA:             %2u bytes  OK\n", len);
    PASS();
}

static void test_typical_eoa(void) {
    uint8_t buf[ACCOUNT_MAX_ENCODED];
    uint8_t balance[ACCOUNT_MAX_BALANCE];
    // 1.5 ETH = 1.5e18 wei = 0x14D1120D7B160000
    balance_from_u64(0x14D1120D7B160000ULL, balance);

    uint16_t len = account_encode(42, balance, NULL, buf);
    // nonce 42 = 1 byte, balance = 8 bytes → 2 + 1 + 8 = 11
    CHECK(len == 11, "expected 11 bytes, got %u", len);
    CHECK(len <= SLOT_MAX_VALUE, "doesn't fit in slot");

    uint64_t nonce;
    uint8_t bal[ACCOUNT_MAX_BALANCE];
    bool has_code;
    CHECK(account_decode(buf, len, &nonce, bal, NULL, &has_code), "decode failed");
    CHECK(nonce == 42, "nonce = %" PRIu64, nonce);
    CHECK(!has_code, "should not have code");
    CHECK(memcmp(bal, balance, ACCOUNT_MAX_BALANCE) == 0, "balance mismatch");

    printf("  typical EOA:           %2u bytes  OK\n", len);
    PASS();
}

static void test_nonce_boundaries(void) {
    uint8_t buf[ACCOUNT_MAX_ENCODED];
    uint8_t balance[ACCOUNT_MAX_BALANCE] = {0};

    struct { uint64_t nonce; uint8_t expected_nonce_len; } cases[] = {
        { 0,                    0 },
        { 1,                    1 },
        { 0xFF,                 1 },
        { 0x100,                2 },
        { 0xFFFF,               2 },
        { 0x10000,              3 },
        { 0xFFFFFF,             3 },
        { 0x1000000,            4 },
        { 0xFFFFFFFF,           4 },
        { 0x100000000ULL,       5 },
        { 0xFFFFFFFFFFULL,      5 },
        { 0x10000000000ULL,     6 },
        { 0xFFFFFFFFFFFFULL,    6 },
        { 0x1000000000000ULL,   7 },
        { 0xFFFFFFFFFFFFFFULL,  7 },
        { 0x100000000000000ULL, 8 },
        { UINT64_MAX,           8 },
    };
    int n = sizeof(cases) / sizeof(cases[0]);

    for (int i = 0; i < n; i++) {
        uint16_t len = account_encode(cases[i].nonce, balance, NULL, buf);
        uint16_t expected = 2 + cases[i].expected_nonce_len;
        CHECK(len == expected,
              "nonce=0x%" PRIx64 ": expected %u bytes, got %u",
              cases[i].nonce, expected, len);

        uint64_t got_nonce;
        CHECK(account_decode(buf, len, &got_nonce, NULL, NULL, NULL), "decode failed");
        CHECK(got_nonce == cases[i].nonce,
              "nonce roundtrip: expected 0x%" PRIx64 ", got 0x%" PRIx64,
              cases[i].nonce, got_nonce);
    }

    printf("  nonce boundaries:      %2d cases  OK\n", n);
    PASS();
}

static void test_balance_boundaries(void) {
    uint8_t buf[ACCOUNT_MAX_ENCODED];

    struct { const char *hex; uint8_t expected_bal_len; const char *label; } cases[] = {
        { NULL,                                                              0, "zero" },
        { "01",                                                              1, "1 wei" },
        { "ff",                                                              1, "255 wei" },
        { "0100",                                                            2, "256 wei" },
        { "ffff",                                                            2, "65535 wei" },
        { "0de0b6b3a7640000",                                                8, "1 ETH" },
        { "14d1120d7b160000",                                                8, "1.5 ETH" },
        { "6f05b59d3b200000",                                                8, "8 ETH" },
        { "d3c21bcecceda1000000",                                           10, "1M ETH" },
        // ~120M ETH total supply in wei: ~1.2e26 = 0x5f5e100 * 1e18
        { "639187a38c1b7af4200000",                                         11, "120M ETH" },
    };
    int n = sizeof(cases) / sizeof(cases[0]);

    for (int i = 0; i < n; i++) {
        uint8_t balance[ACCOUNT_MAX_BALANCE] = {0};
        if (cases[i].hex) {
            balance_from_hex(cases[i].hex, balance);
        }

        uint16_t len = account_encode(0, balance, NULL, buf);
        uint16_t expected = 2 + cases[i].expected_bal_len;
        CHECK(len == expected,
              "%s: expected %u bytes, got %u",
              cases[i].label, expected, len);

        uint8_t got_bal[ACCOUNT_MAX_BALANCE];
        CHECK(account_decode(buf, len, NULL, got_bal, NULL, NULL), "decode failed");
        CHECK(memcmp(got_bal, balance, ACCOUNT_MAX_BALANCE) == 0,
              "%s: balance roundtrip failed", cases[i].label);
    }

    printf("  balance boundaries:    %2d cases  OK\n", n);
    PASS();
}

static void test_eoa_with_max_nonce(void) {
    uint8_t buf[ACCOUNT_MAX_ENCODED];
    uint8_t balance[ACCOUNT_MAX_BALANCE];
    balance_from_hex("639187a38c1b7af4200000", balance);  // 120M ETH

    uint16_t len = account_encode(UINT64_MAX, balance, NULL, buf);
    // 2 + 8(nonce) + 11(balance) = 21
    CHECK(len == 21, "expected 21 bytes, got %u", len);
    CHECK(len <= SLOT_MAX_VALUE, "doesn't fit in slot");

    uint64_t nonce;
    uint8_t bal[ACCOUNT_MAX_BALANCE];
    bool has_code;
    CHECK(account_decode(buf, len, &nonce, bal, NULL, &has_code), "decode failed");
    CHECK(nonce == UINT64_MAX, "nonce mismatch");
    CHECK(!has_code, "should not have code");
    CHECK(memcmp(bal, balance, ACCOUNT_MAX_BALANCE) == 0, "balance mismatch");

    printf("  max nonce+balance EOA: %2u bytes  OK\n", len);
    PASS();
}

static void test_contract_no_balance(void) {
    uint8_t buf[ACCOUNT_MAX_ENCODED];
    uint8_t balance[ACCOUNT_MAX_BALANCE] = {0};
    uint8_t code_hash[CODE_HASH_SIZE];
    make_code_hash(code_hash, 0xAB);

    uint16_t len = account_encode(1, balance, code_hash, buf);
    // 2 + 1(nonce) + 0(balance) + 32(code_hash) = 35
    CHECK(len == 35, "expected 35 bytes, got %u", len);
    CHECK(len <= SLOT_MAX_VALUE, "doesn't fit in slot");

    uint64_t nonce;
    uint8_t bal[ACCOUNT_MAX_BALANCE], ch[CODE_HASH_SIZE];
    bool has_code;
    CHECK(account_decode(buf, len, &nonce, bal, ch, &has_code), "decode failed");
    CHECK(nonce == 1, "nonce = %" PRIu64, nonce);
    CHECK(has_code, "should have code");
    CHECK(memcmp(ch, code_hash, CODE_HASH_SIZE) == 0, "code_hash mismatch");

    uint8_t zero_bal[ACCOUNT_MAX_BALANCE] = {0};
    CHECK(memcmp(bal, zero_bal, ACCOUNT_MAX_BALANCE) == 0, "balance not zero");

    printf("  contract (no balance): %2u bytes  OK\n", len);
    PASS();
}

static void test_contract_with_balance(void) {
    uint8_t buf[ACCOUNT_MAX_ENCODED];
    uint8_t balance[ACCOUNT_MAX_BALANCE];
    balance_from_u64(0x6F05B59D3B200000ULL, balance);  // 8 ETH
    uint8_t code_hash[CODE_HASH_SIZE];
    make_code_hash(code_hash, 0xCD);

    uint16_t len = account_encode(100, balance, code_hash, buf);
    // 2 + 1(nonce) + 8(balance) + 32(code_hash) = 43
    CHECK(len == 43, "expected 43 bytes, got %u", len);
    CHECK(len <= SLOT_MAX_VALUE, "doesn't fit in slot");

    uint64_t nonce;
    uint8_t bal[ACCOUNT_MAX_BALANCE], ch[CODE_HASH_SIZE];
    bool has_code;
    CHECK(account_decode(buf, len, &nonce, bal, ch, &has_code), "decode failed");
    CHECK(nonce == 100, "nonce = %" PRIu64, nonce);
    CHECK(has_code, "should have code");
    CHECK(memcmp(bal, balance, ACCOUNT_MAX_BALANCE) == 0, "balance mismatch");
    CHECK(memcmp(ch, code_hash, CODE_HASH_SIZE) == 0, "code_hash mismatch");

    printf("  contract + balance:    %2u bytes  OK\n", len);
    PASS();
}

static void test_worst_realistic(void) {
    uint8_t buf[ACCOUNT_MAX_ENCODED];
    uint8_t balance[ACCOUNT_MAX_BALANCE];
    // 12-byte balance (slightly beyond total ETH supply)
    balance_from_hex("ffffffffffffffffffffffff", balance);  // 12 bytes

    uint8_t code_hash[CODE_HASH_SIZE];
    make_code_hash(code_hash, 0xFF);

    uint16_t len = account_encode(UINT64_MAX, balance, code_hash, buf);
    // 2 + 8(nonce) + 12(balance) + 32(code_hash) = 54
    CHECK(len == 54, "expected 54 bytes, got %u", len);
    CHECK(len <= SLOT_MAX_VALUE, "CRITICAL: worst realistic doesn't fit in %u-byte slot!",
          SLOT_MAX_VALUE);

    uint64_t nonce;
    uint8_t bal[ACCOUNT_MAX_BALANCE], ch[CODE_HASH_SIZE];
    bool has_code;
    CHECK(account_decode(buf, len, &nonce, bal, ch, &has_code), "decode failed");
    CHECK(nonce == UINT64_MAX, "nonce mismatch");
    CHECK(has_code, "should have code");
    CHECK(memcmp(bal, balance, ACCOUNT_MAX_BALANCE) == 0, "balance mismatch");
    CHECK(memcmp(ch, code_hash, CODE_HASH_SIZE) == 0, "code_hash mismatch");

    printf("  worst realistic:       %2u bytes  OK  (<= %u slot limit)\n",
           len, SLOT_MAX_VALUE);
    PASS();
}

static void test_theoretical_max(void) {
    uint8_t buf[ACCOUNT_MAX_ENCODED];

    // 32-byte balance (all 0xFF — not physically possible on Ethereum)
    uint8_t balance[ACCOUNT_MAX_BALANCE];
    memset(balance, 0xFF, ACCOUNT_MAX_BALANCE);

    uint8_t code_hash[CODE_HASH_SIZE];
    make_code_hash(code_hash, 0xEE);

    uint16_t len = account_encode(UINT64_MAX, balance, code_hash, buf);
    // 2 + 8(nonce) + 32(balance) + 32(code_hash) = 74
    CHECK(len == 74, "expected 74 bytes, got %u", len);

    bool fits = len <= SLOT_MAX_VALUE;
    printf("  theoretical max:       %2u bytes  %s  (slot limit = %u)\n",
           len, fits ? "FITS" : "EXCEEDS", SLOT_MAX_VALUE);

    // Verify roundtrip still works
    uint64_t nonce;
    uint8_t bal[ACCOUNT_MAX_BALANCE], ch[CODE_HASH_SIZE];
    bool has_code;
    CHECK(account_decode(buf, len, &nonce, bal, ch, &has_code), "decode failed");
    CHECK(nonce == UINT64_MAX, "nonce mismatch");
    CHECK(has_code, "should have code");
    CHECK(memcmp(bal, balance, ACCOUNT_MAX_BALANCE) == 0, "balance mismatch");
    CHECK(memcmp(ch, code_hash, CODE_HASH_SIZE) == 0, "code_hash mismatch");

    printf("  theoretical roundtrip:            OK\n");
    PASS();
}

static void test_decode_rejects_bad_input(void) {
    int rejected = 0;

    // Too short
    uint8_t buf1[] = { 0x00 };
    CHECK(!account_decode(buf1, 1, NULL, NULL, NULL, NULL), "should reject 1-byte input");
    rejected++;

    // nonce_len > 8
    uint8_t buf2[] = { 0x12, 0x00 };  // nonce_len = (0x12 >> 1) & 0x0F = 9
    CHECK(!account_decode(buf2, 2, NULL, NULL, NULL, NULL), "should reject nonce_len=9");
    rejected++;

    // balance_len > 32
    uint8_t buf3[] = { 0x00, 33 };
    CHECK(!account_decode(buf3, 2, NULL, NULL, NULL, NULL), "should reject balance_len=33");
    rejected++;

    // Length mismatch: claims nonce_len=1 but only 2 bytes total
    uint8_t buf4[] = { 0x02, 0x00 };  // nonce_len=1, bal_len=0 → expected 3 bytes
    CHECK(!account_decode(buf4, 2, NULL, NULL, NULL, NULL), "should reject length mismatch");
    rejected++;

    // has_code but buffer too short for code_hash
    uint8_t buf5[] = { 0x01, 0x00 };  // has_code=1, nonce=0, bal=0 → expected 2+32=34 bytes
    CHECK(!account_decode(buf5, 2, NULL, NULL, NULL, NULL), "should reject missing code_hash");
    rejected++;

    printf("  bad input rejection:   %2d cases  OK\n", rejected);
    PASS();
}

static void test_size_distribution(void) {
    printf("\n  ---- Size Distribution Table ----\n");
    printf("  %-30s  %s  %s\n", "Account Type", "Size", "Fits?");
    printf("  %-30s  %s  %s\n", "------------", "----", "-----");

    uint8_t buf[ACCOUNT_MAX_ENCODED];
    uint8_t balance[ACCOUNT_MAX_BALANCE];
    uint8_t code_hash[CODE_HASH_SIZE];
    make_code_hash(code_hash, 0xAA);

    struct {
        const char *label;
        uint64_t nonce;
        const char *bal_hex;  // NULL = zero
        bool has_code;
    } cases[] = {
        { "empty EOA",                   0,          NULL,                         false },
        { "new EOA (1 tx)",              1,          NULL,                         false },
        { "EOA + 0.001 ETH",            5,          "038d7ea4c68000",             false },
        { "active EOA + 1 ETH",         150,        "0de0b6b3a7640000",           false },
        { "power user + 100 ETH",       50000,      "056bc75e2d63100000",         false },
        { "whale + 10K ETH",            200,        "021e19e0c9bab2400000",       false },
        { "empty contract",             1,          NULL,                         true  },
        { "contract + 0.1 ETH",         1,          "016345785d8a0000",           true  },
        { "Uniswap-like (active)",       1,          "6f05b59d3b200000",           true  },
        { "rich contract + 100K ETH",   100,        "152d02c7e14af6800000",       true  },
        { "worst realistic",            UINT64_MAX, "ffffffffffffffffffffffff", true },
    };
    int n = sizeof(cases) / sizeof(cases[0]);

    int all_fit = 1;
    for (int i = 0; i < n; i++) {
        memset(balance, 0, ACCOUNT_MAX_BALANCE);
        if (cases[i].bal_hex) {
            balance_from_hex(cases[i].bal_hex, balance);
        }

        uint16_t len = account_encode(
            cases[i].nonce, balance,
            cases[i].has_code ? code_hash : NULL,
            buf);

        bool fits = len <= SLOT_MAX_VALUE;
        if (!fits) all_fit = 0;

        printf("  %-30s  %2u B   %s\n", cases[i].label, len,
               fits ? "yes" : "NO!");

        // Verify roundtrip
        uint64_t got_nonce;
        uint8_t got_bal[ACCOUNT_MAX_BALANCE], got_ch[CODE_HASH_SIZE];
        bool got_has_code;
        if (!account_decode(buf, len, &got_nonce, got_bal, got_ch, &got_has_code)) {
            printf("    FAIL: roundtrip decode failed\n");
            tests_failed++;
            return;
        }
        if (got_nonce != cases[i].nonce) {
            printf("    FAIL: nonce roundtrip\n");
            tests_failed++;
            return;
        }
    }

    printf("\n  all realistic accounts fit in %u-byte slot: %s\n",
           SLOT_MAX_VALUE, all_fit ? "YES" : "NO");
    PASS();
}

static void test_null_balance(void) {
    // Ensure passing NULL balance works (treated as zero)
    uint8_t buf[ACCOUNT_MAX_ENCODED];
    uint16_t len = account_encode(5, NULL, NULL, buf);
    CHECK(len == 3, "expected 3 bytes, got %u", len);  // 2 + 1(nonce)

    uint64_t nonce;
    uint8_t bal[ACCOUNT_MAX_BALANCE];
    CHECK(account_decode(buf, len, &nonce, bal, NULL, NULL), "decode failed");
    CHECK(nonce == 5, "nonce = %" PRIu64, nonce);

    uint8_t zero[ACCOUNT_MAX_BALANCE] = {0};
    CHECK(memcmp(bal, zero, ACCOUNT_MAX_BALANCE) == 0, "balance not zero");

    printf("  NULL balance:           3 bytes  OK\n");
    PASS();
}

// ============================================================================
// Main
// ============================================================================

int main(void) {
    printf("============================================\n");
    printf("Account Encoding PoC Test\n");
    printf("============================================\n");
    printf("slot limit: %u bytes\n", SLOT_MAX_VALUE);

    printf("\n--- Roundtrip Tests ---\n");
    test_empty_eoa();
    test_typical_eoa();
    test_contract_no_balance();
    test_contract_with_balance();
    test_eoa_with_max_nonce();
    test_worst_realistic();
    test_theoretical_max();
    test_null_balance();

    printf("\n--- Boundary Tests ---\n");
    test_nonce_boundaries();
    test_balance_boundaries();

    printf("\n--- Validation Tests ---\n");
    test_decode_rejects_bad_input();

    printf("\n--- Size Analysis ---\n");
    test_size_distribution();

    printf("\n============================================\n");
    printf("Results: %d passed, %d failed\n", tests_passed, tests_failed);
    printf("============================================\n");

    return tests_failed > 0 ? 1 : 0;
}
