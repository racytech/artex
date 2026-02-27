/*
 * Logger example/test
 */

#include "logger.h"

int main(void) {
    /* Initialize with INFO level */
    log_init(LOG_LEVEL_TRACE, NULL);
    
    /* Test database logging */
    LOG_DB_TRACE("Opening database at path: %s", "/tmp/test.db");
    LOG_DB_DEBUG("Column families initialized: %d", 5);
    LOG_DB_INFO("Database opened successfully");
    LOG_DB_WARN("Write buffer is %d%% full", 75);
    LOG_DB_ERROR("Failed to compact range: %s", "IO error");
    
    /* Test state logging */
    LOG_STATE_TRACE("Reading account: 0x1234...abcd");
    LOG_STATE_DEBUG("Cache hit for account: 0x1234...abcd");
    LOG_STATE_INFO("State root computed: 0xabcd...1234");
    LOG_STATE_WARN("Journal contains %d uncommitted changes", 150);
    LOG_STATE_ERROR("MPT corruption detected at depth %d", 7);
    
    /* Test EVM logging */
    LOG_EVM_TRACE("Executing opcode: SLOAD at PC=%d", 42);
    LOG_EVM_DEBUG("Gas remaining: %llu", 21000ULL);
    LOG_EVM_INFO("Contract deployed at: 0x5678...ef01");
    LOG_EVM_WARN("Stack depth approaching limit: %d/1024", 900);
    LOG_EVM_ERROR("Out of gas at PC=%d", 100);
    LOG_EVM_FATAL("Invalid opcode: 0x%02x", 0xFF);
    
    /* Test common logging */
    LOG_INFO("ART execution engine started");
    LOG_WARN("Memory usage: %lu MB", 1024UL);
    
    return 0;
}
