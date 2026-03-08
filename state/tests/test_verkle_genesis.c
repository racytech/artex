/**
 * Debug test: reproduce genesis state root for verkle test fixtures.
 * Sets up the 4 accounts from extcodesize_warm.json and computes root.
 */
#include "verkle_state.h"
#include "verkle_key.h"
#include "keccak256.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>

static void print_hex(const char *label, const uint8_t *data, int len) {
    printf("  %s: 0x", label);
    for (int i = 0; i < len; i++) printf("%02x", data[i]);
    printf("\n");
}

static void hex_to_bytes(const char *hex, uint8_t *out, int len) {
    for (int i = 0; i < len; i++) {
        unsigned int val;
        sscanf(hex + i * 2, "%2x", &val);
        out[i] = (uint8_t)val;
    }
}

int main(void) {
    printf("=== Verkle Genesis Root Debug ===\n\n");

    verkle_state_t *vs = verkle_state_create();

    /* Account 1: 0xfffffffffffffffffffffffffffffffffffffffe
     * nonce=1, balance=0, code=54 bytes */
    uint8_t addr1[20];
    memset(addr1, 0xFF, 20);
    addr1[19] = 0xFE;

    printf("Account 1: ");
    for (int i = 0; i < 20; i++) printf("%02x", addr1[i]);
    printf("\n");

    /* Set nonce=1 */
    verkle_state_set_nonce(vs, addr1, 1);

    /* Code from fixture: system contract deposit bytecode */
    /* 0x60003560e01c...  (54 bytes) — we'll use placeholder */
    uint8_t code1[] = {
        0x60, 0x00, 0x35, 0x60, 0xe0, 0x1c, 0x80, 0x63, 0x48, 0xb3, 0x8a, 0x84,
        0x14, 0x60, 0x1c, 0x57, 0x80, 0x63, 0x4f, 0x5f, 0x43, 0x78, 0x14, 0x60,
        0x26, 0x57, 0x60, 0x00, 0x5f, 0xfd, 0x5b, 0x60, 0x04, 0x35, 0x60, 0x08,
        0x1c, 0x63, 0xff, 0xff, 0xff, 0xff, 0x16, 0x40, 0x60, 0x00, 0x52, 0x60,
        0x20, 0x60, 0x00, 0xf3, 0x5b, 0x40
    };
    printf("  code_len=%zu\n", sizeof(code1));

    /* Actually, I need the real code from the fixture. Let me just use a generic approach. */
    /* For now, let me read it from the fixture JSON. */
    /* But let me first check: is the issue in code chunking or basic data packing? */

    /* Let me test with a minimal account to isolate the issue. */
    /* Start fresh with just one account: 0xa94f... with balance only */

    verkle_state_destroy(vs);
    vs = verkle_state_create();

    /* Account 2: 0xa94f5374fce5edbc8e2a8697c15331677e6ebf0b
     * nonce=0, balance=0x3635c9adc5dea00000 (1000 ETH), no code */
    uint8_t addr2[20];
    hex_to_bytes("a94f5374fce5edbc8e2a8697c15331677e6ebf0b", addr2, 20);

    printf("\nAccount 2 (balance only): ");
    for (int i = 0; i < 20; i++) printf("%02x", addr2[i]);
    printf("\n");

    /* Balance = 0x3635c9adc5dea00000 = 1000 ETH in wei
     * As uint128: 0x3635c9adc5dea00000 = 9 bytes
     * In LE 32-byte: bytes[0..8] = 00 00 a0 de c5 ad c9 35 36, rest zeros */
    uint8_t balance_le[32] = {0};
    /* 0x3635c9adc5dea00000 in LE: */
    balance_le[0] = 0x00;
    balance_le[1] = 0x00;
    balance_le[2] = 0xa0;
    balance_le[3] = 0xde;
    balance_le[4] = 0xc5;
    balance_le[5] = 0xad;
    balance_le[6] = 0xc9;
    balance_le[7] = 0x35;
    balance_le[8] = 0x36;

    verkle_state_set_nonce(vs, addr2, 0);
    verkle_state_set_balance(vs, addr2, balance_le);

    /* Verify what's stored in the tree */
    uint8_t basic_data_key[32];
    verkle_account_basic_data_key(basic_data_key, addr2);
    print_hex("basic_data_key", basic_data_key, 32);

    /* Read back the basic data from tree */
    uint8_t basic_data_val[32];
    verkle_tree_t *tree = verkle_state_get_tree(vs);
    if (verkle_get(tree, basic_data_key, basic_data_val)) {
        print_hex("basic_data_val", basic_data_val, 32);
    } else {
        printf("  basic_data: NOT FOUND!\n");
    }

    /* Expected packed basic data per EIP-6800:
     * byte[0] = 0 (version)
     * bytes[1..4] = 0 (reserved)
     * bytes[5..7] = 0 (code_size, 3 bytes BE)
     * bytes[8..15] = 0 (nonce, 8 bytes BE)
     * bytes[16..31] = balance, 16 bytes BE
     *
     * Balance 0x3635c9adc5dea00000 as 16-byte BE:
     * 00 00 00 00 00 00 00 36 35 c9 ad c5 de a0 00 00
     *
     * So full packed data:
     * 00 00000000 000000 0000000000000000 00000000000000363c9adc5dea00000
     */
    printf("  Expected packed: 0x000000000000000000000000000000000000000000000036 35c9adc5dea00000\n");

    /* Code hash key */
    uint8_t code_hash_key[32];
    verkle_account_code_hash_key(code_hash_key, addr2);
    print_hex("code_hash_key", code_hash_key, 32);

    uint8_t code_hash_val[32];
    if (verkle_get(tree, code_hash_key, code_hash_val)) {
        print_hex("code_hash_val", code_hash_val, 32);
    } else {
        printf("  code_hash: NOT FOUND (expected for no-code account)\n");
    }

    /* Root with just this one account */
    uint8_t root[32];
    verkle_state_root_hash(vs, root);
    print_hex("root (1 account)", root, 32);

    printf("\n=== Expected genesis root: 0x724a6b342ed5ac925cd707bfaf7f57eed4f54a5ff9bdba58d5355bc58362691e ===\n");

    verkle_state_destroy(vs);
    return 0;
}
