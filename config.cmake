# Project build configuration
# Edit this file instead of passing -D flags to cmake.
# Values here override the defaults in CMakeLists.txt.

set(CMAKE_BUILD_TYPE Release)

# State backend
set(ENABLE_MPT ON)

# Per-block state diff history
set(ENABLE_HISTORY ON)

# Checkpoint interval (blocks between auto-saves)
set(CHECKPOINT_INTERVAL 256)

# Debug instrumentation (trace, prestate dump, etc.)
set(ENABLE_DEBUG OFF)
set(ENABLE_EVM_TRACE ON)
