## Next Steps (Production Readiness)

### 1. Crash Recovery Tests (1-2 days) 🔴 CRITICAL
   - Test: Kill process mid-write, verify data on restart
   - Test: Corrupt WAL segment, verify graceful failure
   - Test: Power loss simulation (truncated writes)
   - Validates: Durability guarantee actually works

### 2. Checkpointing Implementation (2-3 days) 🔴 CRITICAL
   - Background checkpoint thread
   - Test: Verify checkpoint reduces recovery time
   - Test: Concurrent checkpoint + writes don't corrupt
   - Prevents: Unbounded WAL growth

### 3. Iterator Support (3-4 days) 🟡 IMPORTANT
   - Implement persistent iterator with page pinning
   - Test: Iterate 1M entries without running out of memory
   - Test: Iterator + concurrent writes maintain consistency
   - Required for: Ethereum state scanning

### 4. Error Handling & Return Codes (1-2 days) 🟡 IMPORTANT
   - Define db_error_t enum (replace bool/-1)
   - Add get_last_error() API for detailed messages
   - Distinguish: "disk full" vs "corruption" vs "I/O error"

### 5. Fsync Retry Logic (1 day) 🟡 IMPORTANT
   - Retry fsync failures 3x with exponential backoff
   - Add health monitoring (db_health_t)
   - Test: Inject ENOSPC, verify graceful degradation

### 6. Page Corruption Handling (1 day)
   - Fail-stop on checksum mismatch
   - Log corruption details
   - Test: Corrupt page on disk, verify detection

### 7. Snapshot Timeout Enforcement (1 day)
   - Add 5-minute timeout per snapshot
   - Force-close expired snapshots
   - Test: Create 100 snapshots, verify old ones auto-close

Total: ~2 weeks to production-ready 🎯