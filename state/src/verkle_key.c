#include "verkle_key.h"
#include "pedersen.h"
#include <string.h>

/* =========================================================================
 * Core Derivation
 * ========================================================================= */

void verkle_derive_stem(uint8_t stem[31],
                        const uint8_t address[20],
                        const uint8_t tree_index[32])
{
    uint8_t scalars[4][32];
    memset(scalars, 0, sizeof(scalars));

    /* scalar[0]: domain separator = 2 */
    scalars[0][0] = VERKLE_KEY_DOMAIN;

    /* scalar[1]: address (20 bytes LE, zero-padded to 32) */
    memcpy(scalars[1], address, 20);

    /* scalar[2]: tree_index lower 16 bytes */
    memcpy(scalars[2], tree_index, 16);

    /* scalar[3]: tree_index upper 16 bytes */
    memcpy(scalars[3], tree_index + 16, 16);

    banderwagon_point_t commitment;
    pedersen_commit(&commitment, scalars, 4);

    uint8_t field[32];
    banderwagon_map_to_field(field, &commitment);
    memcpy(stem, field, 31);
}

void verkle_derive_key(uint8_t key[32],
                       const uint8_t address[20],
                       const uint8_t tree_index[32],
                       uint8_t sub_index)
{
    verkle_derive_stem(key, address, tree_index);
    key[31] = sub_index;
}

/* =========================================================================
 * Account Header Convenience
 * ========================================================================= */

/** Header tree_index is zero (32 zero bytes). */
static const uint8_t HEADER_TREE_INDEX[32] = {0};

void verkle_account_version_key(uint8_t key[32], const uint8_t address[20]) {
    verkle_derive_key(key, address, HEADER_TREE_INDEX, VERKLE_VERSION_SUFFIX);
}

void verkle_account_balance_key(uint8_t key[32], const uint8_t address[20]) {
    verkle_derive_key(key, address, HEADER_TREE_INDEX, VERKLE_BALANCE_SUFFIX);
}

void verkle_account_nonce_key(uint8_t key[32], const uint8_t address[20]) {
    verkle_derive_key(key, address, HEADER_TREE_INDEX, VERKLE_NONCE_SUFFIX);
}

void verkle_account_code_hash_key(uint8_t key[32], const uint8_t address[20]) {
    verkle_derive_key(key, address, HEADER_TREE_INDEX, VERKLE_CODE_HASH_SUFFIX);
}

void verkle_account_code_size_key(uint8_t key[32], const uint8_t address[20]) {
    verkle_derive_key(key, address, HEADER_TREE_INDEX, VERKLE_CODE_SIZE_SUFFIX);
}

/* =========================================================================
 * Storage Slot Convenience
 * ========================================================================= */

void verkle_storage_key(uint8_t key[32],
                        const uint8_t address[20],
                        const uint8_t slot[32])
{
    /* sub_index = slot & 0xFF (lowest LE byte) */
    uint8_t sub_index = slot[0];

    /* tree_index = (slot >> 8) + 1
     *
     * Right-shift slot by 8 bits (1 byte):
     *   tree_index[i] = slot[i + 1]  for i = 0..30
     *   tree_index[31] = 0
     * Then add 1.
     */
    uint8_t tree_index[32];
    memcpy(tree_index, slot + 1, 31);
    tree_index[31] = 0;

    /* Add 1 to tree_index (LE byte array) */
    uint16_t carry = 1;
    for (int i = 0; i < 32 && carry; i++) {
        carry += tree_index[i];
        tree_index[i] = (uint8_t)carry;
        carry >>= 8;
    }

    verkle_derive_key(key, address, tree_index, sub_index);
}
