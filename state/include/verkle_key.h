#ifndef VERKLE_KEY_H
#define VERKLE_KEY_H

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Verkle Key Derivation — Pedersen Hash
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
 */

/* =========================================================================
 * Account Layout Constants
 * ========================================================================= */

/** Domain separator for state key derivation (leaf commitment uses 1). */
#define VERKLE_KEY_DOMAIN          2

/** Domain separator for code key derivation (separate from state). */
#define VERKLE_CODE_DOMAIN         3

/** Account header fields (tree_index = 0). */
#define VERKLE_VERSION_SUFFIX      0
#define VERKLE_BALANCE_SUFFIX      1
#define VERKLE_NONCE_SUFFIX        2
#define VERKLE_CODE_HASH_SUFFIX    3
#define VERKLE_CODE_SIZE_SUFFIX    4

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

/** Derive key for account version (tree_index=0, suffix=0). */
void verkle_account_version_key(uint8_t key[32], const uint8_t address[20]);

/** Derive key for account balance (tree_index=0, suffix=1). */
void verkle_account_balance_key(uint8_t key[32], const uint8_t address[20]);

/** Derive key for account nonce (tree_index=0, suffix=2). */
void verkle_account_nonce_key(uint8_t key[32], const uint8_t address[20]);

/** Derive key for account code hash (tree_index=0, suffix=3). */
void verkle_account_code_hash_key(uint8_t key[32], const uint8_t address[20]);

/** Derive key for account code size (tree_index=0, suffix=4). */
void verkle_account_code_size_key(uint8_t key[32], const uint8_t address[20]);

/* =========================================================================
 * Storage Slot Convenience
 * ========================================================================= */

/**
 * Derive key for a storage slot.
 *
 * Mapping: tree_index = (slot >> 8) + 1, sub_index = slot & 0xFF.
 * tree_index=0 is reserved for account header.
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
 * Derive key for code chunk #chunk_id.
 *
 * Uses domain separator 3 (separate from state domain 2).
 * Mapping: tree_index = chunk_id / 256, sub_index = chunk_id % 256.
 * Each group of 256 chunks shares a stem (one leaf node, 8192 bytes of code).
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
