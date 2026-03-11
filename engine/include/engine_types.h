#ifndef ENGINE_TYPES_H
#define ENGINE_TYPES_H

#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>
#include <cjson/cJSON.h>

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Engine Types — Data structures for the Engine API (V1-V4).
 *
 * Maps the execution-apis spec types to C structs.
 * Supports all versions: Paris (V1), Shanghai (V2), Cancun (V3), Prague (V4).
 * Single unified structs with version-aware JSON parsing.
 */

/* =========================================================================
 * Version Enum
 * ========================================================================= */

typedef enum {
    ENGINE_V1 = 1,  /* Paris (The Merge) */
    ENGINE_V2 = 2,  /* Shanghai */
    ENGINE_V3 = 3,  /* Cancun */
    ENGINE_V4 = 4,  /* Prague */
} engine_version_t;

/* =========================================================================
 * Execution Payload V4 (Prague)
 * ========================================================================= */

/** Withdrawal (EIP-4895) */
typedef struct {
    uint64_t index;
    uint64_t validator_index;
    uint8_t  address[20];
    uint64_t amount;       /* in Gwei */
} engine_withdrawal_t;

/** Execution Payload V4 */
typedef struct {
    uint8_t  parent_hash[32];
    uint8_t  fee_recipient[20];
    uint8_t  state_root[32];
    uint8_t  receipts_root[32];
    uint8_t  logs_bloom[256];
    uint8_t  prev_randao[32];
    uint64_t block_number;
    uint64_t gas_limit;
    uint64_t gas_used;
    uint64_t timestamp;
    uint8_t  extra_data[32];
    size_t   extra_data_len;
    uint8_t  base_fee_per_gas[32];  /* uint256, big-endian */
    uint8_t  block_hash[32];

    /* Transactions (raw RLP bytes) */
    uint8_t **transactions;     /* array of raw tx bytes */
    size_t   *tx_lengths;       /* length of each transaction */
    size_t    tx_count;

    /* Withdrawals (EIP-4895) */
    engine_withdrawal_t *withdrawals;
    size_t               withdrawal_count;

    /* EIP-4844: blob gas */
    uint64_t blob_gas_used;
    uint64_t excess_blob_gas;

    /* EIP-7685: execution layer requests (Prague) */
    uint8_t **requests;         /* array of request bytes */
    size_t   *request_lengths;
    size_t    request_count;
} execution_payload_t;

/* =========================================================================
 * Payload Status
 * ========================================================================= */

typedef enum {
    PAYLOAD_VALID,
    PAYLOAD_INVALID,
    PAYLOAD_SYNCING,
    PAYLOAD_ACCEPTED,
    PAYLOAD_INVALID_BLOCK_HASH,
} payload_status_enum_t;

typedef struct {
    payload_status_enum_t status;
    uint8_t  latest_valid_hash[32];
    bool     has_latest_valid_hash;
    const char *validation_error;   /* NULL if no error */
} payload_status_t;

/* =========================================================================
 * Forkchoice State
 * ========================================================================= */

typedef struct {
    uint8_t head_block_hash[32];
    uint8_t safe_block_hash[32];
    uint8_t finalized_block_hash[32];
} forkchoice_state_t;

/* =========================================================================
 * Payload Attributes V3
 * ========================================================================= */

typedef struct {
    uint64_t timestamp;
    uint8_t  prev_randao[32];
    uint8_t  suggested_fee_recipient[20];
    engine_withdrawal_t *withdrawals;
    size_t               withdrawal_count;
    uint8_t  parent_beacon_block_root[32];
} payload_attributes_t;

/* =========================================================================
 * JSON Conversion — Versioned
 * ========================================================================= */

/** Parse ExecutionPayload from JSON (version-aware).
 *  V1: core 14 fields. V2: +withdrawals. V3/V4: +blobGas. */
bool execution_payload_from_json_v(const cJSON *json, execution_payload_t *out,
                                    engine_version_t version);

/** Convert ExecutionPayload to JSON (version-aware). */
cJSON *execution_payload_to_json_v(const execution_payload_t *payload,
                                    engine_version_t version);

/** Parse PayloadAttributes from JSON (version-aware).
 *  V1: 3 fields. V2: +withdrawals. V3: +parentBeaconBlockRoot. */
bool payload_attributes_from_json_v(const cJSON *json, payload_attributes_t *out,
                                     engine_version_t version);

/* =========================================================================
 * JSON Conversion — Unversioned (default to latest)
 * ========================================================================= */

/** Parse ExecutionPayload (defaults to V4). */
bool execution_payload_from_json(const cJSON *json, execution_payload_t *out);

/** Convert ExecutionPayload to JSON (defaults to V4). */
cJSON *execution_payload_to_json(const execution_payload_t *payload);

/** Convert PayloadStatus to JSON. */
cJSON *payload_status_to_json(const payload_status_t *ps);

/** Parse ForkchoiceStateV1 from JSON (same across all versions). */
bool forkchoice_state_from_json(const cJSON *json, forkchoice_state_t *out);

/** Parse PayloadAttributes (defaults to V3). */
bool payload_attributes_from_json(const cJSON *json, payload_attributes_t *out);

/** Deep copy an execution payload (all dynamic fields are cloned). */
void execution_payload_deep_copy(execution_payload_t *dst,
                                 const execution_payload_t *src);

/** Free dynamically allocated fields in execution_payload_t. */
void execution_payload_free(execution_payload_t *payload);

/** Free dynamically allocated fields in payload_attributes_t. */
void payload_attributes_free(payload_attributes_t *attrs);

/** Convert payload status enum to string. */
const char *payload_status_str(payload_status_enum_t status);

#ifdef __cplusplus
}
#endif

#endif /* ENGINE_TYPES_H */
