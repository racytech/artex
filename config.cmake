# Project build configuration
# Edit this file instead of passing -D flags to cmake.
# Values here override the defaults in CMakeLists.txt.

set(CMAKE_BUILD_TYPE Release)

# State backends (at least one must be ON)
set(ENABLE_MPT    ON)
set(ENABLE_VERKLE OFF)

# Per-block state diff history (required for verkle_build tool)
set(ENABLE_HISTORY ON)

# Build verkle libraries for background state building (pre-fork)
set(ENABLE_VERKLE_BUILD OFF)

# MPT disk_table capacity hints (pre-allocated hash table slots)
set(MPT_ACCOUNT_CAPACITY  300000000)  # ~20GB idx, overflows ~block 15-18M
set(MPT_STORAGE_CAPACITY  2000000000)  # ~130GB idx, overflows ~block 18-20M

# MPT node cache sizes (LRU, in bytes)
set(MPT_ACCOUNT_CACHE_BYTES  0)           # disabled — hit rate too low at scale
set(MPT_STORAGE_CACHE_BYTES  8589934592)  # 8 GB

# Checkpoint interval (blocks between auto-saves)
set(CHECKPOINT_INTERVAL 256)

# Debug instrumentation (trace, prestate dump, etc.)
set(ENABLE_DEBUG OFF)
set(ENABLE_EVM_TRACE ON)
