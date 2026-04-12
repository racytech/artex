/*
 * Era file reader — post-merge beacon chain archives (Bellatrix+).
 *
 * Reads e2store files containing snappy-compressed SSZ beacon blocks.
 * Extracts ExecutionPayload fields into block_header_t + block_body_t.
 *
 * SSZ layout (Bellatrix):
 *   SignedBeaconBlock { message_offset(4), signature(96) }
 *   BeaconBlock { slot(8), proposer(8), parent_root(32), state_root(32), body_offset(4) }
 *   BeaconBlockBody {
 *     randao(96), eth1_data(72), graffiti(32),
 *     5 × list_offset(4),
 *     sync_aggregate(160),
 *     execution_payload_offset(4)
 *   }
 *   ExecutionPayload {
 *     parent_hash(32), fee_recipient(20), state_root(32), receipts_root(32),
 *     logs_bloom(256), prev_randao(32), block_number(8), gas_limit(8),
 *     gas_used(8), timestamp(8), extra_data_offset(4), base_fee(32),
 *     block_hash(32), transactions_offset(4)
 *     [Capella+] withdrawals_offset(4)
 *     [Deneb+]   blob_gas_used(8), excess_blob_gas(8)
 *   }
 */

#include "era.h"
#include "snappy_decode.h"
#include "rlp.h"
#include "hash.h"

#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

/* E2Store entry types */
#define TYPE_VERSION              0x3265
#define TYPE_COMPRESSED_BLOCK     0x0001
#define ENTRY_HEADER_SIZE         8
#define MAX_DECOMPRESSED_SIZE     (2 * 1024 * 1024)

/* =========================================================================
 * Helpers
 * ========================================================================= */

static inline uint16_t read_le16(const uint8_t *p) {
    return (uint16_t)p[0] | ((uint16_t)p[1] << 8);
}
static inline uint32_t read_le32(const uint8_t *p) {
    return (uint32_t)p[0] | ((uint32_t)p[1] << 8) |
           ((uint32_t)p[2] << 16) | ((uint32_t)p[3] << 24);
}
static inline uint64_t read_le64(const uint8_t *p) {
    return (uint64_t)read_le32(p) | ((uint64_t)read_le32(p + 4) << 32);
}

static inline uint256_t read_le256(const uint8_t *p) {
    uint8_t be[32];
    for (int i = 0; i < 32; i++) be[i] = p[31 - i];
    return uint256_from_bytes(be, 32);
}

/* =========================================================================
 * Lifecycle
 * ========================================================================= */

bool era_open(era_t *era, const char *path) {
    if (!era || !path) return false;
    memset(era, 0, sizeof(*era));

    int fd = open(path, O_RDONLY);
    if (fd < 0) return false;

    struct stat st;
    if (fstat(fd, &st) < 0) { close(fd); return false; }

    era->file_size = (size_t)st.st_size;
    if (era->file_size < 16) { close(fd); return false; }

    era->data = mmap(NULL, era->file_size, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);
    if (era->data == MAP_FAILED) { era->data = NULL; return false; }

    /* Read start_slot from SlotIndex at end of file.
     * SlotIndex value: start_slot(8) + offsets... + count(8)
     * We just need start_slot — skip backwards past the index entry. */
    size_t tail_off = era->file_size;

    /* Walk backwards: last 8 bytes = count */
    uint64_t count = read_le64(era->data + tail_off - 8);
    /* Index value size = 8 + count*8 + 8 */
    size_t idx_val_size = 8 + count * 8 + 8;
    size_t idx_entry_start = tail_off - ENTRY_HEADER_SIZE - idx_val_size;

    if (idx_entry_start < era->file_size) {
        size_t val_start = idx_entry_start + ENTRY_HEADER_SIZE;
        era->start_slot = read_le64(era->data + val_start);
    }

    return true;
}

void era_close(era_t *era) {
    if (era && era->data) {
        munmap(era->data, era->file_size);
        era->data = NULL;
    }
}

/* =========================================================================
 * Iterator
 * ========================================================================= */

era_iter_t era_iter(const era_t *era) {
    era_iter_t it = { .era = era, .pos = 0 };
    /* Skip version entry */
    if (era && era->data && era->file_size >= ENTRY_HEADER_SIZE) {
        uint16_t typ = read_le16(era->data);
        uint32_t len = read_le32(era->data + 2);
        if (typ == TYPE_VERSION)
            it.pos = ENTRY_HEADER_SIZE + len;
    }
    return it;
}

/* =========================================================================
 * SSZ navigation: SignedBeaconBlock → ExecutionPayload
 * ========================================================================= */

/* EP field offsets */
#define EP_PARENT_HASH       0
#define EP_FEE_RECIPIENT    32
#define EP_STATE_ROOT       52
#define EP_RECEIPTS_ROOT    84
#define EP_LOGS_BLOOM      116
#define EP_PREV_RANDAO     372
#define EP_BLOCK_NUMBER    404
#define EP_GAS_LIMIT       412
#define EP_GAS_USED        420
#define EP_TIMESTAMP       428
#define EP_EXTRA_DATA_OFF  436
#define EP_BASE_FEE        440
#define EP_BLOCK_HASH      472

static const uint8_t *find_execution_payload(const uint8_t *ssz, size_t ssz_len,
                                              size_t *ep_len_out) {
    if (ssz_len < 104) return NULL;

    /* SignedBeaconBlock: message_offset(4) + signature(96) */
    uint32_t msg_off = read_le32(ssz);
    if (msg_off + 84 > ssz_len) return NULL;

    /* BeaconBlock: slot(8) + proposer(8) + parent_root(32) + state_root(32) + body_offset(4) */
    const uint8_t *bb = ssz + msg_off;
    size_t bb_len = ssz_len - msg_off;
    if (bb_len < 84) return NULL;

    uint32_t body_off = read_le32(bb + 80);
    if (body_off > bb_len) return NULL;

    const uint8_t *body = bb + body_off;
    size_t body_len = bb_len - body_off;

    /* BeaconBlockBody fixed part: randao(96) + eth1(72) + graffiti(32) +
     * 5×offset(20) + sync_aggregate(160) + ep_offset(4) = 384 min */
    if (body_len < 384) return NULL;

    uint32_t ep_off = read_le32(body + 380);
    if (ep_off >= body_len) return NULL;

    /* EP extends to next variable field offset or end of body.
     * Bellatrix: body fixed = 384, EP is last variable field → ep_end = body_len.
     * Capella+:  body fixed ≥ 388, bls_to_execution_changes offset at 384. */
    size_t ep_end = body_len;
    uint32_t body_fixed = read_le32(body + 200); /* proposer_slashings offset = body fixed size */
    if (body_fixed > 384 && body_len >= body_fixed) {
        uint32_t next_off = read_le32(body + 384);
        if (next_off > ep_off && next_off <= body_len)
            ep_end = next_off;
    }

    *ep_len_out = ep_end - ep_off;
    return body + ep_off;
}

static uint64_t ssz_get_slot(const uint8_t *ssz, size_t ssz_len) {
    if (ssz_len < 108) return 0;
    uint32_t msg_off = read_le32(ssz);
    if (msg_off + 8 > ssz_len) return 0;
    return read_le64(ssz + msg_off);
}

/* =========================================================================
 * Parse ExecutionPayload → block_header_t + block_body_t
 * ========================================================================= */

static bool parse_execution_payload(const uint8_t *ep, size_t ep_len,
                                     block_header_t *hdr, block_body_t *body,
                                     uint8_t block_hash_out[32]) {
    if (ep_len < 508) return false;

    memset(hdr, 0, sizeof(*hdr));
    memset(body, 0, sizeof(*body));

    /* Header fields */
    memcpy(hdr->parent_hash.bytes, ep + EP_PARENT_HASH, 32);
    memcpy(hdr->coinbase.bytes, ep + EP_FEE_RECIPIENT, 20);
    memcpy(hdr->state_root.bytes, ep + EP_STATE_ROOT, 32);
    memcpy(hdr->receipt_root.bytes, ep + EP_RECEIPTS_ROOT, 32);
    memcpy(hdr->logs_bloom, ep + EP_LOGS_BLOOM, 256);
    memcpy(hdr->mix_hash.bytes, ep + EP_PREV_RANDAO, 32);

    hdr->number    = read_le64(ep + EP_BLOCK_NUMBER);
    hdr->gas_limit = read_le64(ep + EP_GAS_LIMIT);
    hdr->gas_used  = read_le64(ep + EP_GAS_USED);
    hdr->timestamp = read_le64(ep + EP_TIMESTAMP);

    hdr->has_base_fee = true;
    hdr->base_fee = read_le256(ep + EP_BASE_FEE);

    memcpy(block_hash_out, ep + EP_BLOCK_HASH, 32);

    /* Difficulty = 0 post-merge */
    hdr->difficulty = UINT256_ZERO;
    hdr->nonce = 0;

    /* Detect fork from extra_data offset (first variable field) */
    uint32_t extra_data_off = read_le32(ep + EP_EXTRA_DATA_OFF);

    /* Detect fork from fixed part size (= extra_data_off value).
     * Bellatrix: 508, Capella: 512 (+withdrawals_offset), Deneb: 528 (+blob gas fields) */
    bool is_deneb   = (extra_data_off >= 528);
    bool is_capella = (!is_deneb && extra_data_off >= 512);

    /* Transaction offset is always at byte 504 (all forks) */
    uint32_t tx_off = read_le32(ep + 504);
    uint32_t wd_off = 0;

    if (is_capella || is_deneb) {
        /* Withdrawals offset at byte 508 (Capella+) */
        wd_off = read_le32(ep + 508);
        hdr->has_withdrawals_root = true;
    }

    if (is_deneb) {
        /* blob_gas_used at 512, excess_blob_gas at 520 */
        hdr->has_blob_gas = true;
        hdr->blob_gas_used   = read_le64(ep + 512);
        hdr->excess_blob_gas = read_le64(ep + 520);
    }

    /* SSZ variable fields appear in declaration order after the fixed part:
     * extra_data, then transactions, then withdrawals (Capella+).
     * extra_data runs from extra_data_off to the next variable field start. */
    size_t ed_len = (tx_off > extra_data_off) ? tx_off - extra_data_off : 0;
    if (ed_len > 32) ed_len = 32;
    if (extra_data_off + ed_len <= ep_len) {
        memcpy(hdr->extra_data, ep + extra_data_off, ed_len);
        hdr->extra_data_len = ed_len;
    }

    /* === Transactions === */
    if (tx_off > 0 && tx_off < ep_len) {
        const uint8_t *tx_data = ep + tx_off;
        /* tx region ends at withdrawals or EP end */
        size_t tx_end = ep_len;
        if (wd_off > tx_off) tx_end = wd_off;
        size_t tx_region = tx_end - tx_off;

        if (tx_region >= 4) {
            uint32_t first_off = read_le32(tx_data);
            size_t tx_count = first_off / 4;

            if (tx_count > 0 && tx_count < 100000 && first_off <= tx_region) {
                /* Build RLP list of raw transaction byte strings */
                rlp_item_t *list = calloc(1, sizeof(rlp_item_t));
                if (!list) return false;
                list->type = RLP_TYPE_LIST;
                list->data.list.items = calloc(tx_count, sizeof(rlp_item_t *));
                list->data.list.count = tx_count;
                if (!list->data.list.items) { free(list); return false; }

                bool ok = true;
                for (size_t t = 0; t < tx_count && ok; t++) {
                    uint32_t t_start = read_le32(tx_data + t * 4);
                    uint32_t t_end = (t + 1 < tx_count)
                        ? read_le32(tx_data + (t + 1) * 4)
                        : (uint32_t)tx_region;

                    if (t_start >= tx_region || t_end > tx_region || t_end <= t_start)
                        { ok = false; break; }

                    list->data.list.items[t] = rlp_string(tx_data + t_start, t_end - t_start);
                    if (!list->data.list.items[t]) { ok = false; break; }
                }

                if (!ok) { rlp_item_free(list); return false; }

                /* Wrap in outer list so block_body_tx(body, i) works */
                rlp_item_t *outer = calloc(1, sizeof(rlp_item_t));
                if (!outer) { rlp_item_free(list); return false; }
                outer->type = RLP_TYPE_LIST;
                outer->data.list.items = calloc(1, sizeof(rlp_item_t *));
                outer->data.list.count = 1;
                outer->data.list.items[0] = list;

                body->_rlp = outer;
                body->_tx_list_idx = 0;
                body->tx_count = tx_count;
            }
        }
    }

    /* === Withdrawals (Capella+) === */
    if ((is_capella || is_deneb) && wd_off > 0 && wd_off < ep_len) {
        size_t wd_region = ep_len - wd_off;

        /* Withdrawal is fixed 44 bytes: index(8) + validator(8) + addr(20) + amount(8) */
        if (wd_region >= 44) {
            size_t wd_count = wd_region / 44;
            withdrawal_t *wds = calloc(wd_count, sizeof(withdrawal_t));
            if (wds) {
                for (size_t w = 0; w < wd_count; w++) {
                    const uint8_t *wd = ep + wd_off + w * 44;
                    wds[w].index = read_le64(wd);
                    wds[w].validator_index = read_le64(wd + 8);
                    memcpy(wds[w].address.bytes, wd + 16, 20);
                    wds[w].amount_gwei = read_le64(wd + 36);
                }
                body->withdrawals = wds;
                body->withdrawal_count = wd_count;
            }
        }
    }

    return true;
}

/* =========================================================================
 * Iterator: era_iter_next
 * ========================================================================= */

bool era_iter_next(era_iter_t *it,
                   block_header_t *hdr, block_body_t *body,
                   uint8_t block_hash[32], uint64_t *slot_out) {
    if (!it || !it->era || !it->era->data) return false;

    const uint8_t *fdata = it->era->data;
    size_t fsize = it->era->file_size;

    while (it->pos + ENTRY_HEADER_SIZE <= fsize) {
        uint16_t typ = read_le16(fdata + it->pos);
        uint32_t length = read_le32(fdata + it->pos + 2);
        size_t value_start = it->pos + ENTRY_HEADER_SIZE;

        it->pos = value_start + length;  /* advance past this entry */

        if (typ != TYPE_COMPRESSED_BLOCK)
            continue;  /* skip version, state, index entries */

        if (value_start + length > fsize)
            return false;

        /* Decompress SSZ beacon block */
        size_t cap = MAX_DECOMPRESSED_SIZE;
        uint8_t *ssz = malloc(cap);
        if (!ssz) return false;

        size_t ssz_len = snappy_frame_decode(fdata + value_start, length, ssz, cap);
        if (ssz_len == 0) { free(ssz); continue; }

        /* Get slot number */
        uint64_t slot = ssz_get_slot(ssz, ssz_len);

        /* Find execution payload */
        size_t ep_len;
        const uint8_t *ep = find_execution_payload(ssz, ssz_len, &ep_len);

        if (!ep || ep_len < 508) {
            free(ssz);
            continue;
        }

        /* Check block_number > 0 — pre-merge Bellatrix blocks have an
         * all-zero execution payload that parses but has no real data */
        uint64_t bn = read_le64(ep + EP_BLOCK_NUMBER);
        if (bn == 0) {
            free(ssz);
            continue;
        }

        /* Parse into header + body */
        bool ok = parse_execution_payload(ep, ep_len, hdr, body, block_hash);
        free(ssz);

        if (!ok) continue;

        if (slot_out) *slot_out = slot;
        return true;
    }

    return false;  /* no more entries */
}
