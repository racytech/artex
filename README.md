# ART - Ethereum Execution Engine

A C implementation of the Ethereum execution layer with RocksDB-backed state management.

## Project Structure

```
art/
├── common/          # Shared utilities (logging, uint256, etc.)
├── database/        # RocksDB wrapper
├── state/           # State management (StateDB, Journal, MPT)
├── evm/             # EVM execution engine
├── third_party/     # External dependencies (RocksDB)
├── build/           # Build output
└── docs/            # Documentation
```

## Dependencies

- CMake 3.15+
- C11 compiler (GCC/Clang)
- C++17 compiler (for RocksDB)
- Snappy, Zstd, LZ4 (optional, for compression)

### Install system dependencies:

**Ubuntu/Debian:**
```bash
sudo apt-get install cmake build-essential libsnappy-dev libzstd-dev liblz4-dev
```

**Fedora/RHEL:**
```bash
sudo dnf install cmake gcc gcc-c++ snappy-devel libzstd-devel lz4-devel
```

**macOS:**
```bash
brew install cmake snappy zstd lz4
```

## Building

```bash
# Clone repository
git clone <your-repo-url>
cd art

# RocksDB is already vendored in third_party/

# Create build directory
mkdir build && cd build

# Configure
cmake ..

# Build (use -j for parallel build)
cmake --build . -j$(nproc)

# Run tests (optional)
ctest
```

## Build Options

```bash
# Debug build
cmake -DCMAKE_BUILD_TYPE=Debug ..

# Release build (default)
cmake -DCMAKE_BUILD_TYPE=Release ..

# Disable tests
cmake -DBUILD_TESTS=OFF ..
```

## Quick Start

```c
#include "database.h"

// Open database
database_options_t opts = database_default_options();
opts.path = "/tmp/testdb";

db_error_t err;
database_t* db = database_open(&opts, &err);

// Write data
uint8_t key[20] = {0x12, 0x34, ...};
uint8_t value[32] = {...};
database_put(db, COLUMN_ACCOUNTS, key, 20, value, 32);

// Read data
uint8_t* data;
size_t len;
database_get(db, COLUMN_ACCOUNTS, key, 20, &data, &len);
free(data);

// Close
database_close(db);
```

## Components

### Database Layer
- RocksDB wrapper with column families
- Atomic batch operations
- Snapshot support

### State Layer
- Account and storage management
- Merkle Patricia Trie
- Journal for dirty state tracking

### EVM Layer
- Opcode execution
- Gas metering
- Contract calls

## License

TBD
