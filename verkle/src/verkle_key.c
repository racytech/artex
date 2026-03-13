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
    uint8_t scalars[5][32];
    memset(scalars, 0, sizeof(scalars));

    /* scalar[0] = 2 + VERKLE_NODE_WIDTH * 64 = 16386 = 0x4002 (LE) */
    scalars[0][0] = 0x02;
    scalars[0][1] = 0x40;

    /* Address32 = 12 zero-bytes (left-pad) + 20-byte address.
     * Split into two 16-byte LE chunks:
     *   scalar[1] = Address32[0:16]  = zeros[0:12] + addr[0:4]
     *   scalar[2] = Address32[16:32] = addr[4:20]              */
    memcpy(scalars[1] + 12, address, 4);
    memcpy(scalars[2], address + 4, 16);

    /* scalar[3] = tree_index[0:16] (LE) */
    memcpy(scalars[3], tree_index, 16);

    /* scalar[4] = tree_index[16:32] (LE) */
    memcpy(scalars[4], tree_index + 16, 16);

    banderwagon_point_t commitment;
    pedersen_commit(&commitment, scalars, 5);

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

static const uint8_t HEADER_TREE_INDEX[32] = {0};

void verkle_account_basic_data_key(uint8_t key[32], const uint8_t address[20]) {
    verkle_derive_key(key, address, HEADER_TREE_INDEX, VERKLE_BASIC_DATA_SUFFIX);
}

void verkle_account_code_hash_key(uint8_t key[32], const uint8_t address[20]) {
    verkle_derive_key(key, address, HEADER_TREE_INDEX, VERKLE_CODE_HASH_SUFFIX);
}

/* =========================================================================
 * Storage Slot Convenience (EIP-6800)
 * ========================================================================= */

void verkle_storage_key(uint8_t key[32],
                        const uint8_t address[20],
                        const uint8_t slot[32])
{
    /* Check if slot < 64 (CODE_OFFSET - HEADER_STORAGE_OFFSET) */
    bool is_header_slot = (slot[0] < 64);
    for (int i = 1; i < 32 && is_header_slot; i++) {
        if (slot[i] != 0) is_header_slot = false;
    }

    if (is_header_slot) {
        /* Header storage: same stem as account header, suffix = 64 + slot */
        verkle_derive_key(key, address, HEADER_TREE_INDEX,
                          VERKLE_HEADER_STORAGE_OFFSET + slot[0]);
    } else {
        /* Main storage: pos = MAIN_STORAGE_OFFSET + slot
         *
         * MAIN_STORAGE_OFFSET = 256^31 = 2^248.
         * In LE: byte[31] = 1, all others 0.
         *
         * tree_index = pos / 256 = (2^248 + slot) >> 8
         *   = (slot >> 8) with byte[30] += 1  (adding 2^240)
         * sub_index = pos % 256 = slot[0]   (2^248 is aligned to 256)
         */
        uint8_t tree_index[32];
        memcpy(tree_index, slot + 1, 31);   /* slot >> 8 */
        tree_index[31] = 0;

        /* Add 2^240: byte[30] += 1 (with carry for correctness) */
        uint16_t carry = 1;
        for (int i = 30; i < 32 && carry; i++) {
            carry += tree_index[i];
            tree_index[i] = (uint8_t)carry;
            carry >>= 8;
        }

        verkle_derive_key(key, address, tree_index, slot[0]);
    }
}

/* =========================================================================
 * Code Chunk Convenience (EIP-6800)
 * ========================================================================= */

void verkle_code_chunk_key(uint8_t key[32],
                           const uint8_t address[20],
                           uint32_t chunk_id)
{
    /* pos = CODE_OFFSET + chunk_id = 128 + chunk_id
     * tree_index = pos / 256
     * sub_index  = pos % 256
     * Uses same domain (2) as all other keys.
     */
    uint32_t pos = VERKLE_CODE_OFFSET + chunk_id;

    uint8_t tree_index[32];
    memset(tree_index, 0, 32);
    uint32_t ti = pos / VERKLE_NODE_WIDTH;
    memcpy(tree_index, &ti, sizeof(ti));  /* LE */

    uint8_t sub_index = (uint8_t)(pos % VERKLE_NODE_WIDTH);

    verkle_derive_key(key, address, tree_index, sub_index);
}
