# Project build configuration
# Edit this file instead of passing -D flags to cmake.
# Values here override the defaults in CMakeLists.txt.

set(CMAKE_BUILD_TYPE Release)

# State backends (at least one must be ON)
set(ENABLE_MPT    ON)
set(ENABLE_VERKLE OFF)

# MPT store LRU cache sizes (MB)
set(MPT_ACCOUNT_CACHE_MB  2048)   # account trie node cache (default: 2048 MB)
set(MPT_STORAGE_CACHE_MB  2048)   # storage trie node cache (default: 2048 MB)

# MPT disk_hash capacity hints (pre-allocated hash table slots)
set(MPT_ACCOUNT_CAPACITY  500000000)  # account trie (~500M for mainnet)
set(MPT_STORAGE_CAPACITY  500000000)  # storage trie (~500M for mainnet)

# Checkpoint interval (blocks between auto-saves)
set(CHECKPOINT_INTERVAL 256)
