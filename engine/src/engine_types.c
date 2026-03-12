/*
 * Engine Types — JSON ↔ C struct conversion for Engine API.
 *
 * All quantities are hex-encoded per execution-apis spec.
 * Hashes and data are 0x-prefixed hex strings.
 */

#include "engine_types.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>

/* =========================================================================
 * Hex Helpers
 * ========================================================================= */

static int hex_val(char c) {
    if (c >= '0' && c <= '9') return c - '0';
    if (c >= 'a' && c <= 'f') return c - 'a' + 10;
    if (c >= 'A' && c <= 'F') return c - 'A' + 10;
    return -1;
}

/** Decode hex string (with optional 0x prefix) into bytes. Returns decoded length. */
static int decode_hex(const char *hex, uint8_t *out, size_t max_out) {
    if (!hex) return -1;
    if (hex[0] == '0' && (hex[1] == 'x' || hex[1] == 'X'))
        hex += 2;

    size_t slen = strlen(hex);
    if (slen % 2 != 0) return -1;
    size_t byte_len = slen / 2;
    if (byte_len > max_out) return -1;

    for (size_t i = 0; i < byte_len; i++) {
        int hi = hex_val(hex[i * 2]);
        int lo = hex_val(hex[i * 2 + 1]);
        if (hi < 0 || lo < 0) return -1;
        out[i] = (uint8_t)((hi << 4) | lo);
    }
    return (int)byte_len;
}

/** Decode fixed-size hex field (exactly n bytes). */
static bool decode_hex_fixed(const char *hex, uint8_t *out, size_t n) {
    return decode_hex(hex, out, n) == (int)n;
}

/** Decode quantity (hex number) to uint64. */
static bool decode_quantity(const char *hex, uint64_t *out) {
    if (!hex) return false;
    if (hex[0] == '0' && (hex[1] == 'x' || hex[1] == 'X'))
        hex += 2;
    if (*hex == '\0') return false;

    char *endp;
    *out = strtoull(hex, &endp, 16);
    return *endp == '\0';
}

/** Encode bytes to 0x-prefixed hex string. buf must be >= 2*len+3. */
static void encode_hex(const uint8_t *data, size_t len, char *buf) {
    buf[0] = '0';
    buf[1] = 'x';
    static const char hex_chars[] = "0123456789abcdef";
    for (size_t i = 0; i < len; i++) {
        buf[2 + i * 2]     = hex_chars[data[i] >> 4];
        buf[2 + i * 2 + 1] = hex_chars[data[i] & 0x0f];
    }
    buf[2 + len * 2] = '\0';
}

/** Encode uint64 as 0x hex quantity. */
static void encode_quantity(uint64_t val, char *buf, size_t buf_size) {
    if (val == 0) {
        snprintf(buf, buf_size, "0x0");
    } else {
        snprintf(buf, buf_size, "0x%llx", (unsigned long long)val);
    }
}

/* =========================================================================
 * JSON → C: Get string field and decode
 * ========================================================================= */

static bool json_get_hash(const cJSON *obj, const char *key, uint8_t out[32]) {
    cJSON *item = cJSON_GetObjectItem(obj, key);
    if (!item || !cJSON_IsString(item)) return false;
    return decode_hex_fixed(item->valuestring, out, 32);
}

static bool json_get_address(const cJSON *obj, const char *key, uint8_t out[20]) {
    cJSON *item = cJSON_GetObjectItem(obj, key);
    if (!item || !cJSON_IsString(item)) return false;
    return decode_hex_fixed(item->valuestring, out, 20);
}

static bool json_get_quantity(const cJSON *obj, const char *key, uint64_t *out) {
    cJSON *item = cJSON_GetObjectItem(obj, key);
    if (!item || !cJSON_IsString(item)) return false;
    return decode_quantity(item->valuestring, out);
}

static bool json_get_bloom(const cJSON *obj, const char *key, uint8_t out[256]) {
    cJSON *item = cJSON_GetObjectItem(obj, key);
    if (!item || !cJSON_IsString(item)) return false;
    return decode_hex_fixed(item->valuestring, out, 256);
}

/* =========================================================================
 * Internal: Parse transactions array
 * ========================================================================= */

static bool parse_transactions(const cJSON *json, execution_payload_t *out) {
    cJSON *txs = cJSON_GetObjectItem(json, "transactions");
    if (txs && cJSON_IsArray(txs)) {
        out->tx_count = (size_t)cJSON_GetArraySize(txs);
        if (out->tx_count > 0) {
            out->transactions = calloc(out->tx_count, sizeof(uint8_t *));
            out->tx_lengths = calloc(out->tx_count, sizeof(size_t));
            if (!out->transactions || !out->tx_lengths) return false;

            size_t idx = 0;
            cJSON *tx;
            cJSON_ArrayForEach(tx, txs) {
                if (!cJSON_IsString(tx)) return false;
                const char *hex = tx->valuestring;
                if (hex[0] == '0' && (hex[1] == 'x' || hex[1] == 'X'))
                    hex += 2;
                size_t hex_len = strlen(hex);
                size_t byte_len = hex_len / 2;
                out->transactions[idx] = malloc(byte_len > 0 ? byte_len : 1);
                if (!out->transactions[idx]) return false;
                if (byte_len > 0) {
                    if (decode_hex(tx->valuestring, out->transactions[idx], byte_len) < 0)
                        return false;
                }
                out->tx_lengths[idx] = byte_len;
                idx++;
            }
        }
    }
    return true;
}

/* =========================================================================
 * Internal: Parse withdrawals array
 * ========================================================================= */

static bool parse_withdrawals(const cJSON *json, execution_payload_t *out) {
    cJSON *wds = cJSON_GetObjectItem(json, "withdrawals");
    if (wds && cJSON_IsArray(wds)) {
        out->withdrawal_count = (size_t)cJSON_GetArraySize(wds);
        if (out->withdrawal_count > 0) {
            out->withdrawals = calloc(out->withdrawal_count, sizeof(engine_withdrawal_t));
            if (!out->withdrawals) return false;

            size_t idx = 0;
            cJSON *wd;
            cJSON_ArrayForEach(wd, wds) {
                if (!json_get_quantity(wd, "index", &out->withdrawals[idx].index)) return false;
                if (!json_get_quantity(wd, "validatorIndex", &out->withdrawals[idx].validator_index)) return false;
                if (!json_get_address(wd, "address", out->withdrawals[idx].address)) return false;
                if (!json_get_quantity(wd, "amount", &out->withdrawals[idx].amount)) return false;
                idx++;
            }
        }
    }
    return true;
}

/* =========================================================================
 * Internal: Parse execution requests array (EIP-7685)
 * ========================================================================= */

static bool parse_requests(const cJSON *json, execution_payload_t *out) {
    cJSON *reqs = cJSON_GetObjectItem(json, "executionRequests");
    if (reqs && cJSON_IsArray(reqs)) {
        out->request_count = (size_t)cJSON_GetArraySize(reqs);
        if (out->request_count > 0) {
            out->requests = calloc(out->request_count, sizeof(uint8_t *));
            out->request_lengths = calloc(out->request_count, sizeof(size_t));
            if (!out->requests || !out->request_lengths) return false;

            size_t idx = 0;
            cJSON *req;
            cJSON_ArrayForEach(req, reqs) {
                if (!cJSON_IsString(req)) return false;
                const char *hex = req->valuestring;
                if (hex[0] == '0' && (hex[1] == 'x' || hex[1] == 'X'))
                    hex += 2;
                size_t hex_len = strlen(hex);
                size_t byte_len = hex_len / 2;
                out->requests[idx] = malloc(byte_len > 0 ? byte_len : 1);
                if (!out->requests[idx]) return false;
                if (byte_len > 0) {
                    if (decode_hex(req->valuestring, out->requests[idx], byte_len) < 0)
                        return false;
                }
                out->request_lengths[idx] = byte_len;
                idx++;
            }
        }
    }
    return true;
}

/* =========================================================================
 * Internal: Parse baseFeePerGas (quantity → big-endian 32 bytes)
 * ========================================================================= */

static bool parse_base_fee(const cJSON *json, execution_payload_t *out) {
    cJSON *base_fee = cJSON_GetObjectItem(json, "baseFeePerGas");
    if (base_fee && cJSON_IsString(base_fee)) {
        const char *bf_hex = base_fee->valuestring;
        if (bf_hex[0] == '0' && (bf_hex[1] == 'x' || bf_hex[1] == 'X'))
            bf_hex += 2;
        size_t bf_slen = strlen(bf_hex);

        memset(out->base_fee_per_gas, 0, 32);
        if (bf_slen > 0) {
            size_t padded_len = bf_slen + (bf_slen % 2);
            size_t byte_len = padded_len / 2;
            if (byte_len > 32) return false;

            uint8_t temp[32];
            memset(temp, 0, 32);
            size_t offset = (bf_slen % 2);
            for (size_t i = 0; i < bf_slen; i++) {
                int v = hex_val(bf_hex[i]);
                if (v < 0) return false;
                size_t bi = (offset + i) / 2;
                if ((offset + i) % 2 == 0)
                    temp[bi] = (uint8_t)(v << 4);
                else
                    temp[bi] |= (uint8_t)v;
            }
            memcpy(out->base_fee_per_gas + (32 - byte_len), temp, byte_len);
        }
    }
    return true;
}

/* =========================================================================
 * Execution Payload — Versioned Parse
 *
 * V1 (Paris):    14 core fields + transactions
 * V2 (Shanghai): +withdrawals
 * V3 (Cancun):   +blobGasUsed, +excessBlobGas
 * V4 (Prague):   same as V3 (requests come from method params)
 * ========================================================================= */

bool execution_payload_from_json_v(const cJSON *json, execution_payload_t *out,
                                    engine_version_t version) {
    if (!json || !out) return false;
    memset(out, 0, sizeof(*out));

    /* Core fields (all versions) */
    if (!json_get_hash(json, "parentHash", out->parent_hash)) return false;
    if (!json_get_address(json, "feeRecipient", out->fee_recipient)) return false;
    if (!json_get_hash(json, "stateRoot", out->state_root)) return false;
    if (!json_get_hash(json, "receiptsRoot", out->receipts_root)) return false;
    if (!json_get_bloom(json, "logsBloom", out->logs_bloom)) return false;
    if (!json_get_hash(json, "prevRandao", out->prev_randao)) return false;
    if (!json_get_quantity(json, "blockNumber", &out->block_number)) return false;
    if (!json_get_quantity(json, "gasLimit", &out->gas_limit)) return false;
    if (!json_get_quantity(json, "gasUsed", &out->gas_used)) return false;
    if (!json_get_quantity(json, "timestamp", &out->timestamp)) return false;
    if (!json_get_hash(json, "blockHash", out->block_hash)) return false;

    cJSON *extra = cJSON_GetObjectItem(json, "extraData");
    if (extra && cJSON_IsString(extra)) {
        int elen = decode_hex(extra->valuestring, out->extra_data, 32);
        if (elen < 0) return false;
        out->extra_data_len = (size_t)elen;
    }

    if (!parse_base_fee(json, out)) return false;
    if (!parse_transactions(json, out)) return false;

    /* V2+: withdrawals */
    if (version >= ENGINE_V2) {
        if (!parse_withdrawals(json, out)) return false;
    }

    /* V3+: blob gas fields */
    if (version >= ENGINE_V3) {
        json_get_quantity(json, "blobGasUsed", &out->blob_gas_used);
        json_get_quantity(json, "excessBlobGas", &out->excess_blob_gas);
    }

    /* V4: execution requests (in payload JSON for V4, but spec says
     * they come as separate method param — parse if present) */
    if (version >= ENGINE_V4) {
        if (!parse_requests(json, out)) return false;
    }

    return true;
}

bool execution_payload_from_json(const cJSON *json, execution_payload_t *out) {
    return execution_payload_from_json_v(json, out, ENGINE_V4);
}

/* =========================================================================
 * Execution Payload — Versioned Emit
 * ========================================================================= */

static void emit_withdrawals(cJSON *obj, const execution_payload_t *payload) {
    char hex_buf[64];
    cJSON *wds = cJSON_AddArrayToObject(obj, "withdrawals");
    for (size_t i = 0; i < payload->withdrawal_count; i++) {
        cJSON *wd = cJSON_CreateObject();
        encode_quantity(payload->withdrawals[i].index, hex_buf, sizeof(hex_buf));
        cJSON_AddStringToObject(wd, "index", hex_buf);
        encode_quantity(payload->withdrawals[i].validator_index, hex_buf, sizeof(hex_buf));
        cJSON_AddStringToObject(wd, "validatorIndex", hex_buf);
        encode_hex(payload->withdrawals[i].address, 20, hex_buf);
        cJSON_AddStringToObject(wd, "address", hex_buf);
        encode_quantity(payload->withdrawals[i].amount, hex_buf, sizeof(hex_buf));
        cJSON_AddStringToObject(wd, "amount", hex_buf);
        cJSON_AddItemToArray(wds, wd);
    }
}

cJSON *execution_payload_to_json_v(const execution_payload_t *payload,
                                    engine_version_t version) {
    if (!payload) return NULL;

    cJSON *obj = cJSON_CreateObject();
    char hex_buf[520];

    /* Core fields (all versions) */
    encode_hex(payload->parent_hash, 32, hex_buf);
    cJSON_AddStringToObject(obj, "parentHash", hex_buf);

    encode_hex(payload->fee_recipient, 20, hex_buf);
    cJSON_AddStringToObject(obj, "feeRecipient", hex_buf);

    encode_hex(payload->state_root, 32, hex_buf);
    cJSON_AddStringToObject(obj, "stateRoot", hex_buf);

    encode_hex(payload->receipts_root, 32, hex_buf);
    cJSON_AddStringToObject(obj, "receiptsRoot", hex_buf);

    encode_hex(payload->logs_bloom, 256, hex_buf);
    cJSON_AddStringToObject(obj, "logsBloom", hex_buf);

    encode_hex(payload->prev_randao, 32, hex_buf);
    cJSON_AddStringToObject(obj, "prevRandao", hex_buf);

    encode_quantity(payload->block_number, hex_buf, sizeof(hex_buf));
    cJSON_AddStringToObject(obj, "blockNumber", hex_buf);

    encode_quantity(payload->gas_limit, hex_buf, sizeof(hex_buf));
    cJSON_AddStringToObject(obj, "gasLimit", hex_buf);

    encode_quantity(payload->gas_used, hex_buf, sizeof(hex_buf));
    cJSON_AddStringToObject(obj, "gasUsed", hex_buf);

    encode_quantity(payload->timestamp, hex_buf, sizeof(hex_buf));
    cJSON_AddStringToObject(obj, "timestamp", hex_buf);

    encode_hex(payload->extra_data, payload->extra_data_len, hex_buf);
    cJSON_AddStringToObject(obj, "extraData", hex_buf);

    int first = 0;
    while (first < 31 && payload->base_fee_per_gas[first] == 0) first++;
    encode_hex(payload->base_fee_per_gas + first, (size_t)(32 - first), hex_buf);
    cJSON_AddStringToObject(obj, "baseFeePerGas", hex_buf);

    encode_hex(payload->block_hash, 32, hex_buf);
    cJSON_AddStringToObject(obj, "blockHash", hex_buf);

    /* Transactions (all versions) */
    cJSON *txs = cJSON_AddArrayToObject(obj, "transactions");
    for (size_t i = 0; i < payload->tx_count; i++) {
        size_t tx_hex_len = 2 + payload->tx_lengths[i] * 2 + 1;
        char *tx_hex = malloc(tx_hex_len);
        if (tx_hex) {
            encode_hex(payload->transactions[i], payload->tx_lengths[i], tx_hex);
            cJSON_AddItemToArray(txs, cJSON_CreateString(tx_hex));
            free(tx_hex);
        }
    }

    /* V2+: withdrawals */
    if (version >= ENGINE_V2) {
        emit_withdrawals(obj, payload);
    }

    /* V3+: blob gas */
    if (version >= ENGINE_V3) {
        encode_quantity(payload->blob_gas_used, hex_buf, sizeof(hex_buf));
        cJSON_AddStringToObject(obj, "blobGasUsed", hex_buf);

        encode_quantity(payload->excess_blob_gas, hex_buf, sizeof(hex_buf));
        cJSON_AddStringToObject(obj, "excessBlobGas", hex_buf);
    }

    /* V4: execution requests in payload JSON */
    if (version >= ENGINE_V4) {
        cJSON *reqs = cJSON_AddArrayToObject(obj, "executionRequests");
        for (size_t i = 0; i < payload->request_count; i++) {
            size_t req_hex_len = 2 + payload->request_lengths[i] * 2 + 1;
            char *req_hex = malloc(req_hex_len);
            if (req_hex) {
                encode_hex(payload->requests[i], payload->request_lengths[i], req_hex);
                cJSON_AddItemToArray(reqs, cJSON_CreateString(req_hex));
                free(req_hex);
            }
        }
    }

    return obj;
}

cJSON *execution_payload_to_json(const execution_payload_t *payload) {
    return execution_payload_to_json_v(payload, ENGINE_V4);
}

void execution_payload_deep_copy(execution_payload_t *dst,
                                 const execution_payload_t *src) {
    if (!dst || !src) return;

    /* Copy all fixed fields */
    *dst = *src;

    /* Deep copy transactions */
    dst->transactions = NULL;
    dst->tx_lengths = NULL;
    if (src->tx_count > 0) {
        dst->transactions = calloc(src->tx_count, sizeof(uint8_t *));
        dst->tx_lengths = calloc(src->tx_count, sizeof(size_t));
        for (size_t i = 0; i < src->tx_count; i++) {
            dst->tx_lengths[i] = src->tx_lengths[i];
            if (src->tx_lengths[i] > 0) {
                dst->transactions[i] = malloc(src->tx_lengths[i]);
                memcpy(dst->transactions[i], src->transactions[i], src->tx_lengths[i]);
            }
        }
    }

    /* Deep copy withdrawals */
    dst->withdrawals = NULL;
    if (src->withdrawal_count > 0) {
        dst->withdrawals = calloc(src->withdrawal_count, sizeof(engine_withdrawal_t));
        memcpy(dst->withdrawals, src->withdrawals,
               src->withdrawal_count * sizeof(engine_withdrawal_t));
    }

    /* Deep copy requests */
    dst->requests = NULL;
    dst->request_lengths = NULL;
    if (src->request_count > 0) {
        dst->requests = calloc(src->request_count, sizeof(uint8_t *));
        dst->request_lengths = calloc(src->request_count, sizeof(size_t));
        for (size_t i = 0; i < src->request_count; i++) {
            dst->request_lengths[i] = src->request_lengths[i];
            if (src->request_lengths[i] > 0) {
                dst->requests[i] = malloc(src->request_lengths[i]);
                memcpy(dst->requests[i], src->requests[i], src->request_lengths[i]);
            }
        }
    }
}

void execution_payload_free(execution_payload_t *payload) {
    if (!payload) return;
    for (size_t i = 0; i < payload->tx_count; i++)
        free(payload->transactions[i]);
    free(payload->transactions);
    free(payload->tx_lengths);
    free(payload->withdrawals);
    for (size_t i = 0; i < payload->request_count; i++)
        free(payload->requests[i]);
    free(payload->requests);
    free(payload->request_lengths);
    memset(payload, 0, sizeof(*payload));
}

/* =========================================================================
 * Payload Status
 * ========================================================================= */

const char *payload_status_str(payload_status_enum_t status) {
    switch (status) {
        case PAYLOAD_VALID:              return "VALID";
        case PAYLOAD_INVALID:            return "INVALID";
        case PAYLOAD_SYNCING:            return "SYNCING";
        case PAYLOAD_ACCEPTED:           return "ACCEPTED";
        case PAYLOAD_INVALID_BLOCK_HASH: return "INVALID_BLOCK_HASH";
        default:                         return "UNKNOWN";
    }
}

cJSON *payload_status_to_json(const payload_status_t *ps) {
    if (!ps) return NULL;

    cJSON *obj = cJSON_CreateObject();
    cJSON_AddStringToObject(obj, "status", payload_status_str(ps->status));

    if (ps->has_latest_valid_hash) {
        char hex[67];
        encode_hex(ps->latest_valid_hash, 32, hex);
        cJSON_AddStringToObject(obj, "latestValidHash", hex);
    } else {
        cJSON_AddNullToObject(obj, "latestValidHash");
    }

    if (ps->validation_error)
        cJSON_AddStringToObject(obj, "validationError", ps->validation_error);
    else
        cJSON_AddNullToObject(obj, "validationError");

    return obj;
}

/* =========================================================================
 * Forkchoice State
 * ========================================================================= */

bool forkchoice_state_from_json(const cJSON *json, forkchoice_state_t *out) {
    if (!json || !out) return false;
    memset(out, 0, sizeof(*out));

    if (!json_get_hash(json, "headBlockHash", out->head_block_hash)) return false;
    if (!json_get_hash(json, "safeBlockHash", out->safe_block_hash)) return false;
    if (!json_get_hash(json, "finalizedBlockHash", out->finalized_block_hash)) return false;
    return true;
}

/* =========================================================================
 * Payload Attributes — Versioned Parse
 *
 * V1 (Paris):    timestamp, prevRandao, suggestedFeeRecipient
 * V2 (Shanghai): +withdrawals
 * V3 (Cancun):   +parentBeaconBlockRoot
 * ========================================================================= */

bool payload_attributes_from_json_v(const cJSON *json, payload_attributes_t *out,
                                     engine_version_t version) {
    if (!json || !out) return false;
    memset(out, 0, sizeof(*out));

    /* Core fields (all versions) */
    if (!json_get_quantity(json, "timestamp", &out->timestamp)) return false;
    if (!json_get_hash(json, "prevRandao", out->prev_randao)) return false;
    if (!json_get_address(json, "suggestedFeeRecipient", out->suggested_fee_recipient)) return false;

    /* V2+: withdrawals */
    if (version >= ENGINE_V2) {
        cJSON *wds = cJSON_GetObjectItem(json, "withdrawals");
        if (wds && cJSON_IsArray(wds)) {
            out->withdrawal_count = (size_t)cJSON_GetArraySize(wds);
            if (out->withdrawal_count > 0) {
                out->withdrawals = calloc(out->withdrawal_count, sizeof(engine_withdrawal_t));
                if (!out->withdrawals) return false;

                size_t idx = 0;
                cJSON *wd;
                cJSON_ArrayForEach(wd, wds) {
                    if (!json_get_quantity(wd, "index", &out->withdrawals[idx].index)) return false;
                    if (!json_get_quantity(wd, "validatorIndex", &out->withdrawals[idx].validator_index)) return false;
                    if (!json_get_address(wd, "address", out->withdrawals[idx].address)) return false;
                    if (!json_get_quantity(wd, "amount", &out->withdrawals[idx].amount)) return false;
                    idx++;
                }
            }
        }
    }

    /* V3+: parentBeaconBlockRoot */
    if (version >= ENGINE_V3) {
        json_get_hash(json, "parentBeaconBlockRoot", out->parent_beacon_block_root);
    }

    return true;
}

bool payload_attributes_from_json(const cJSON *json, payload_attributes_t *out) {
    return payload_attributes_from_json_v(json, out, ENGINE_V3);
}

void payload_attributes_free(payload_attributes_t *attrs) {
    if (!attrs) return;
    free(attrs->withdrawals);
    memset(attrs, 0, sizeof(*attrs));
}
