#ifndef VERKLE_KEY_H
#define VERKLE_KEY_H

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Verkle Key Derivation — Pedersen Hash (EIP-6800)
 *
 * Maps (address, tree_index, sub_index) to a 32-byte verkle tree key.
 *
 * Key = [31-byte stem][1-byte sub_index]
 *
 * Stem is derived via Pedersen commitment over 4 scalars:
 *   scalar[0] = 2               (domain separator)
 *   scalar[1] = address          (20 bytes LE, 160 bits)
 *   scalar[2] = tree_index_lo    (lower 16 bytes LE, 128 bits)
 *   scalar[3] = tree_index_hi    (upper 16 bytes LE, 128 bits)
 *
 *   stem = map_to_field(pedersen_commit(scalars, CRS))[0:31]
 *
 * Account header layout (tree_index=0, all same stem):
 *   Suffix 0:       Basic data (packed: version|reserved|code_size|nonce|balance)
 *   Suffix 1:       Code hash (32-byte keccak256)
 *   Suffixes 64-127:  Header storage (slots 0-63)
 *   Suffixes 128-255: Code chunks 0-127 (31-byte with PUSHDATA prefix)
 */

/* =========================================================================
 * Layout Constants (EIP-6800)
 * ========================================================================= */

/** Domain separator for all key derivation. */
#define VERKLE_KEY_DOMAIN              2

/** Suffix for packed basic data (version + code_size + nonce + balance). */
#define VERKLE_BASIC_DATA_SUFFIX       0

/** Suffix for code hash (keccak256). */
#define VERKLE_CODE_HASH_SUFFIX        1

/** Offset for header storage slots 0-63 (suffixes 64-127). */
#define VERKLE_HEADER_STORAGE_OFFSET   64

/** Offset for code chunks (suffixes 128-255 in first stem). */
#define VERKLE_CODE_OFFSET             128

/** Verkle node width (256 children per internal node). */
#define VERKLE_NODE_WIDTH              256

/**
 * Basic data layout within the 32-byte value at suffix 0 (big-endian):
 *   [0]      version     (1 byte)
 *   [1..4]   reserved    (4 bytes, zero)
 *   [5..7]   code_size   (3 bytes, BE uint24)
 *   [8..15]  nonce       (8 bytes, BE uint64)
 *   [16..31] balance     (16 bytes, BE uint128)
 */
#define VERKLE_BASIC_DATA_VERSION_OFFSET     0
#define VERKLE_BASIC_DATA_CODE_SIZE_OFFSET   5
#define VERKLE_BASIC_DATA_NONCE_OFFSET       8
#define VERKLE_BASIC_DATA_BALANCE_OFFSET     16
#define VERKLE_BASIC_DATA_BALANCE_SIZE       16

/* =========================================================================
 * Core Key Derivation
 * ========================================================================= */

/**
 * Derive a 32-byte verkle tree key.
 *
 * @param key        Output: 32-byte key [31-byte stem || sub_index]
 * @param address    20-byte address
 * @param tree_index 32-byte tree index (uint256, little-endian)
 * @param sub_index  Suffix byte (0-255)
 */
void verkle_derive_key(uint8_t key[32],
                       const uint8_t address[20],
                       const uint8_t tree_index[32],
                       uint8_t sub_index);

/**
 * Derive just the 31-byte stem (shared across all sub_indices for a given
 * address + tree_index). Useful when setting multiple values in one leaf.
 *
 * @param stem       Output: 31-byte stem
 * @param address    20-byte address
 * @param tree_index 32-byte tree index (uint256, little-endian)
 */
void verkle_derive_stem(uint8_t stem[31],
                        const uint8_t address[20],
                        const uint8_t tree_index[32]);

/* =========================================================================
 * Account Header Convenience
 * ========================================================================= */

/** Derive key for packed basic data (tree_index=0, suffix=0). */
void verkle_account_basic_data_key(uint8_t key[32], const uint8_t address[20]);

/** Derive key for code hash (tree_index=0, suffix=1). */
void verkle_account_code_hash_key(uint8_t key[32], const uint8_t address[20]);

/* =========================================================================
 * Storage Slot Convenience
 * ========================================================================= */

/**
 * Derive key for a storage slot (EIP-6800).
 *
 * Slots 0-63:  header storage at suffix (64 + slot) in header stem.
 * Slots >= 64: main storage at MAIN_STORAGE_OFFSET + slot.
 *   MAIN_STORAGE_OFFSET = 256^31 = 2^248.
 *   tree_index = (MAIN_STORAGE_OFFSET + slot) / 256
 *   sub_index  = (MAIN_STORAGE_OFFSET + slot) % 256
 *
 * @param key     Output: 32-byte key
 * @param address 20-byte address
 * @param slot    32-byte storage slot (uint256, little-endian)
 */
void verkle_storage_key(uint8_t key[32],
                        const uint8_t address[20],
                        const uint8_t slot[32]);

/* =========================================================================
 * Code Chunk Convenience
 * ========================================================================= */

/**
 * Derive key for code chunk #chunk_id (EIP-6800).
 *
 * pos = CODE_OFFSET + chunk_id = 128 + chunk_id.
 * tree_index = pos / 256, sub_index = pos % 256.
 * Chunks 0-127 share the header stem (tree_index=0, suffixes 128-255).
 *
 * @param key       Output: 32-byte key
 * @param address   20-byte address
 * @param chunk_id  Code chunk index (0-based)
 */
void verkle_code_chunk_key(uint8_t key[32],
                           const uint8_t address[20],
                           uint32_t chunk_id);

#ifdef __cplusplus
}
#endif

#endif /* VERKLE_KEY_H */
