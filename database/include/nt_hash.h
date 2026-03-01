#ifndef NT_HASH_H
#define NT_HASH_H

#include "nibble_trie.h"
#include "hash.h"

/**
 * Compute Ethereum MPT root hash by walking the nibble_trie structure.
 *
 * The trie already matches Ethereum's hex-prefix trie (branch/extension/leaf).
 * This function walks nodes recursively, RLP-encodes each one, and produces
 * the standard keccak256 root hash.
 *
 * Returns HASH_EMPTY_STORAGE for empty tries.
 */
hash_t nt_root_hash(const nibble_trie_t *t);

#endif /* NT_HASH_H */
