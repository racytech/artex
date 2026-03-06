/*
 * Discv5 Packet Codec — wire protocol encoding/decoding.
 *
 * Packet structure:  masking-iv(16) || masked-header || message
 * Header masking:    AES-128-CTR(key=dest-id[0:16], iv=masking-iv)
 * Message encryption: AES-128-GCM(key=session-key, nonce=header.nonce,
 *                                  aad=masking-iv||header)
 */

#include "../include/discv5_codec.h"
#include "../include/hkdf.h"
#include "../include/aes.h"
#include "../include/secp256k1_wrap.h"
#include <blst.h>
#include <string.h>

/* =========================================================================
 * Internal helpers
 * ========================================================================= */

static const char PROTOCOL_ID[6] = { 'd','i','s','c','v','5' };
static const char KDF_INFO_PREFIX[] = "discovery v5 key agreement";
#define KDF_INFO_PREFIX_LEN 26

static const char ID_SIGN_PREFIX[] = "discovery v5 identity proof";
#define ID_SIGN_PREFIX_LEN 27

/* Write big-endian uint16 */
static void put_be16(uint8_t *p, uint16_t v) {
    p[0] = (uint8_t)(v >> 8);
    p[1] = (uint8_t)(v);
}

/* Read big-endian uint16 */
static uint16_t get_be16(const uint8_t *p) {
    return ((uint16_t)p[0] << 8) | p[1];
}

/* Write big-endian uint64 */
static void put_be64(uint8_t *p, uint64_t v) {
    for (int i = 7; i >= 0; i--) {
        p[i] = (uint8_t)(v & 0xFF);
        v >>= 8;
    }
}

/* Read big-endian uint64 */
static uint64_t get_be64(const uint8_t *p) {
    uint64_t v = 0;
    for (int i = 0; i < 8; i++)
        v = (v << 8) | p[i];
    return v;
}

/*
 * Build the static header into buf.
 * Returns the number of bytes written (always DISCV5_STATIC_HDR_SIZE = 23).
 */
static size_t build_static_header(uint8_t *buf, uint8_t flag,
                                   const uint8_t nonce[12],
                                   uint16_t authdata_size) {
    memcpy(buf, PROTOCOL_ID, 6);
    put_be16(buf + 6, DISCV5_VERSION);
    buf[8] = flag;
    memcpy(buf + 9, nonce, 12);
    put_be16(buf + 21, authdata_size);
    return DISCV5_STATIC_HDR_SIZE;
}

/*
 * Mask/unmask header using AES-128-CTR.
 * masking-key = dest-id[0:16], iv = masking-iv.
 */
static void mask_header(uint8_t *out, const uint8_t *header, size_t hdr_len,
                         const uint8_t dest_id[32],
                         const uint8_t masking_iv[16]) {
    aes128_ctr(out, header, hdr_len, dest_id, masking_iv);
}

/* =========================================================================
 * Key derivation
 * ========================================================================= */

bool discv5_derive_keys_from_secret(discv5_keys_t *keys,
                                     const uint8_t shared_secret[33],
                                     const uint8_t *challenge_data, size_t cd_len,
                                     const uint8_t node_id_a[32],
                                     const uint8_t node_id_b[32]) {
    /* PRK = HKDF-Extract(salt=challenge_data, IKM=shared_secret)
     * Note: The spec pseudocode uses HKDF-Extract(secret, challenge-data)
     * which maps to RFC 5869 HMAC(salt=challenge-data, IKM=secret). */
    uint8_t prk[32];
    hkdf_extract(prk, challenge_data, cd_len, shared_secret, 33);

    /* info = "discovery v5 key agreement" || node-id-A || node-id-B */
    uint8_t info[KDF_INFO_PREFIX_LEN + 64];
    memcpy(info, KDF_INFO_PREFIX, KDF_INFO_PREFIX_LEN);
    memcpy(info + KDF_INFO_PREFIX_LEN, node_id_a, 32);
    memcpy(info + KDF_INFO_PREFIX_LEN + 32, node_id_b, 32);

    /* key_data = HKDF-Expand(prk, info, 32) */
    uint8_t key_data[32];
    hkdf_expand(key_data, 32, prk, info, sizeof(info));

    memcpy(keys->initiator_key, key_data, 16);
    memcpy(keys->recipient_key, key_data + 16, 16);
    return true;
}

bool discv5_derive_keys(discv5_keys_t *keys,
                        const uint8_t eph_priv[32],
                        const uint8_t dest_pub[64],
                        const uint8_t *challenge_data, size_t cd_len,
                        const uint8_t node_id_a[32],
                        const uint8_t node_id_b[32]) {
    /* ECDH → 33-byte compressed shared point */
    uint8_t shared[33];
    if (!secp256k1_wrap_ecdh_compressed(shared, eph_priv, dest_pub))
        return false;

    return discv5_derive_keys_from_secret(keys, shared,
                                           challenge_data, cd_len,
                                           node_id_a, node_id_b);
}

/* =========================================================================
 * ID nonce signing
 * ========================================================================= */

bool discv5_sign_id_nonce(uint8_t sig[64],
                           const uint8_t *challenge_data, size_t cd_len,
                           const uint8_t eph_pubkey[33],
                           const uint8_t node_id_b[32],
                           const uint8_t static_key[32]) {
    /*
     * id-signature-input = "discovery v5 identity proof" || challenge_data
     *                      || eph_pubkey || node_id_b
     * hash = SHA256(id-signature-input)
     * sig = secp256k1_sign(hash, static_key)
     */
    size_t input_len = ID_SIGN_PREFIX_LEN + cd_len + 33 + 32;
    uint8_t input_stack[512];
    uint8_t *input = (input_len <= sizeof(input_stack))
                      ? input_stack : NULL;
    /* For Discv5, input_len is typically 27+63+33+32 = 155 bytes, fits on stack */

    memcpy(input, ID_SIGN_PREFIX, ID_SIGN_PREFIX_LEN);
    memcpy(input + ID_SIGN_PREFIX_LEN, challenge_data, cd_len);
    memcpy(input + ID_SIGN_PREFIX_LEN + cd_len, eph_pubkey, 33);
    memcpy(input + ID_SIGN_PREFIX_LEN + cd_len + 33, node_id_b, 32);

    /* SHA-256 hash */
    uint8_t hash[32];
    blst_sha256(hash, input, input_len);

    /* ECDSA sign */
    int recid;
    return secp256k1_wrap_sign(sig, &recid, hash, static_key);
}

/* =========================================================================
 * Encode: Ordinary packet (flag=0)
 * ========================================================================= */

size_t discv5_encode_ordinary(uint8_t *out, size_t out_cap,
                               const uint8_t masking_iv[16],
                               const uint8_t src_id[32],
                               const uint8_t dest_id[32],
                               const uint8_t nonce[12],
                               const uint8_t *pt, size_t pt_len,
                               const uint8_t session_key[16]) {
    uint16_t authdata_size = 32;
    size_t header_len = DISCV5_STATIC_HDR_SIZE + authdata_size;
    size_t msg_len = pt_len + DISCV5_GCM_TAG_SIZE;
    size_t total = DISCV5_MASKING_IV_SIZE + header_len + msg_len;

    if (total > out_cap) return 0;

    /* Build unmasked header */
    uint8_t header[DISCV5_STATIC_HDR_SIZE + 32];
    build_static_header(header, DISCV5_FLAG_ORDINARY, nonce, authdata_size);
    memcpy(header + DISCV5_STATIC_HDR_SIZE, src_id, 32);

    /* Encrypt message: AES-GCM(key, nonce, pt, aad=masking-iv||header) */
    uint8_t aad[DISCV5_MASKING_IV_SIZE + DISCV5_STATIC_HDR_SIZE + 32];
    memcpy(aad, masking_iv, DISCV5_MASKING_IV_SIZE);
    memcpy(aad + DISCV5_MASKING_IV_SIZE, header, header_len);

    uint8_t *ct_start = out + DISCV5_MASKING_IV_SIZE + header_len;
    uint8_t tag[16];
    aes128_gcm_encrypt(ct_start, tag,
                        pt, pt_len,
                        aad, DISCV5_MASKING_IV_SIZE + header_len,
                        session_key, nonce);
    memcpy(ct_start + pt_len, tag, 16);

    /* Output masking-iv */
    memcpy(out, masking_iv, DISCV5_MASKING_IV_SIZE);

    /* Mask header */
    mask_header(out + DISCV5_MASKING_IV_SIZE, header, header_len,
                dest_id, masking_iv);

    return total;
}

/* =========================================================================
 * Encode: WHOAREYOU packet (flag=1)
 * ========================================================================= */

size_t discv5_encode_whoareyou(uint8_t *out, size_t out_cap,
                                const uint8_t masking_iv[16],
                                const uint8_t dest_id[32],
                                const uint8_t request_nonce[12],
                                const uint8_t id_nonce[16],
                                uint64_t enr_seq) {
    uint16_t authdata_size = DISCV5_ID_NONCE_SIZE + 8;  /* 24 */
    size_t header_len = DISCV5_STATIC_HDR_SIZE + authdata_size;
    size_t total = DISCV5_MASKING_IV_SIZE + header_len;  /* no message body */

    if (total > out_cap) return 0;

    /* Build unmasked header */
    uint8_t header[DISCV5_STATIC_HDR_SIZE + 24];
    build_static_header(header, DISCV5_FLAG_WHOAREYOU, request_nonce, authdata_size);
    memcpy(header + DISCV5_STATIC_HDR_SIZE, id_nonce, 16);
    put_be64(header + DISCV5_STATIC_HDR_SIZE + 16, enr_seq);

    /* Output masking-iv */
    memcpy(out, masking_iv, DISCV5_MASKING_IV_SIZE);

    /* Mask header */
    mask_header(out + DISCV5_MASKING_IV_SIZE, header, header_len,
                dest_id, masking_iv);

    return total;
}

/* =========================================================================
 * Encode: Handshake packet (flag=2)
 * ========================================================================= */

size_t discv5_encode_handshake(uint8_t *out, size_t out_cap,
                                const uint8_t masking_iv[16],
                                const uint8_t src_id[32],
                                const uint8_t dest_id[32],
                                const uint8_t nonce[12],
                                const uint8_t id_sig[64],
                                const uint8_t eph_pubkey[33],
                                const uint8_t *enr_record, size_t enr_len,
                                const uint8_t *pt, size_t pt_len,
                                const uint8_t session_key[16]) {
    /* authdata = src-id(32) + sig-size(1) + eph-key-size(1)
     *            + id-sig(64) + eph-pubkey(33) + enr(variable) */
    uint16_t authdata_size = 32 + 1 + 1 + 64 + 33 + (uint16_t)enr_len;
    size_t header_len = DISCV5_STATIC_HDR_SIZE + authdata_size;
    size_t msg_len = pt_len + DISCV5_GCM_TAG_SIZE;
    size_t total = DISCV5_MASKING_IV_SIZE + header_len + msg_len;

    if (total > out_cap) return 0;

    /* Build unmasked header */
    uint8_t header[1024];  /* large enough for header + ENR */
    if (header_len > sizeof(header)) return 0;

    build_static_header(header, DISCV5_FLAG_HANDSHAKE, nonce, authdata_size);
    size_t pos = DISCV5_STATIC_HDR_SIZE;
    memcpy(header + pos, src_id, 32); pos += 32;
    header[pos++] = 64;   /* sig-size */
    header[pos++] = 33;   /* eph-key-size */
    memcpy(header + pos, id_sig, 64); pos += 64;
    memcpy(header + pos, eph_pubkey, 33); pos += 33;
    if (enr_len > 0 && enr_record) {
        memcpy(header + pos, enr_record, enr_len);
        pos += enr_len;
    }

    /* Encrypt message */
    size_t aad_len = DISCV5_MASKING_IV_SIZE + header_len;
    uint8_t aad[1024 + DISCV5_MASKING_IV_SIZE];
    if (aad_len > sizeof(aad)) return 0;

    memcpy(aad, masking_iv, DISCV5_MASKING_IV_SIZE);
    memcpy(aad + DISCV5_MASKING_IV_SIZE, header, header_len);

    uint8_t *ct_start = out + DISCV5_MASKING_IV_SIZE + header_len;
    uint8_t tag[16];
    aes128_gcm_encrypt(ct_start, tag,
                        pt, pt_len,
                        aad, aad_len,
                        session_key, nonce);
    memcpy(ct_start + pt_len, tag, 16);

    /* Output masking-iv */
    memcpy(out, masking_iv, DISCV5_MASKING_IV_SIZE);

    /* Mask header */
    mask_header(out + DISCV5_MASKING_IV_SIZE, header, header_len,
                dest_id, masking_iv);

    return total;
}

/* =========================================================================
 * Decode header
 * ========================================================================= */

bool discv5_decode_header(discv5_header_t *hdr,
                           uint8_t *header_buf,
                           const uint8_t *packet, size_t pkt_len,
                           const uint8_t local_id[32]) {
    if (pkt_len < DISCV5_MASKING_IV_SIZE + DISCV5_STATIC_HDR_SIZE)
        return false;

    /* Copy masking-iv */
    memcpy(hdr->masking_iv, packet, DISCV5_MASKING_IV_SIZE);
    const uint8_t *masked = packet + DISCV5_MASKING_IV_SIZE;
    size_t masked_len = pkt_len - DISCV5_MASKING_IV_SIZE;

    /* Unmask static header (first 23 bytes) */
    uint8_t static_hdr[DISCV5_STATIC_HDR_SIZE];
    aes128_ctr(static_hdr, masked, DISCV5_STATIC_HDR_SIZE,
               local_id, hdr->masking_iv);

    /* Verify protocol-id */
    if (memcmp(static_hdr, PROTOCOL_ID, 6) != 0)
        return false;

    /* Parse static header fields */
    hdr->flag = static_hdr[8];
    memcpy(hdr->nonce, static_hdr + 9, 12);
    hdr->authdata_size = get_be16(static_hdr + 21);

    /* Reject unreasonable authdata sizes.
     * Max valid: handshake with ENR ≈ 34 + 64 + 33 + 300 = 431.
     * Use 512 as a generous upper bound. */
    if (hdr->authdata_size > 512) return false;

    size_t header_len = DISCV5_STATIC_HDR_SIZE + hdr->authdata_size;
    if (header_len > masked_len) return false;

    hdr->header_len = header_len;
    hdr->total_hdr_len = DISCV5_MASKING_IV_SIZE + header_len;

    /* Unmask full header (re-decrypt from start — CTR is deterministic) */
    aes128_ctr(header_buf, masked, header_len,
               local_id, hdr->masking_iv);

    /* Parse authdata based on flag */
    const uint8_t *authdata = header_buf + DISCV5_STATIC_HDR_SIZE;

    switch (hdr->flag) {
    case DISCV5_FLAG_ORDINARY:
        if (hdr->authdata_size != 32) return false;
        memcpy(hdr->auth.ordinary.src_id, authdata, 32);
        break;

    case DISCV5_FLAG_WHOAREYOU:
        if (hdr->authdata_size != 24) return false;
        memcpy(hdr->auth.whoareyou.id_nonce, authdata, 16);
        hdr->auth.whoareyou.enr_seq = get_be64(authdata + 16);
        break;

    case DISCV5_FLAG_HANDSHAKE: {
        if (hdr->authdata_size < 34) return false;
        memcpy(hdr->auth.handshake.src_id, authdata, 32);
        hdr->auth.handshake.sig_size = authdata[32];
        hdr->auth.handshake.eph_key_size = authdata[33];

        /* Validate sizes fit in destination buffers */
        if (hdr->auth.handshake.sig_size > 64) return false;
        if (hdr->auth.handshake.eph_key_size > 33) return false;

        size_t expected = 34 + hdr->auth.handshake.sig_size
                             + hdr->auth.handshake.eph_key_size;
        if (expected > hdr->authdata_size) return false;

        memcpy(hdr->auth.handshake.id_sig, authdata + 34,
               hdr->auth.handshake.sig_size);
        memcpy(hdr->auth.handshake.eph_pubkey,
               authdata + 34 + hdr->auth.handshake.sig_size,
               hdr->auth.handshake.eph_key_size);

        /* ENR record (optional, remainder of authdata) */
        size_t enr_offset = expected;
        if (enr_offset < hdr->authdata_size) {
            hdr->auth.handshake.enr_data = authdata + enr_offset;
            hdr->auth.handshake.enr_len = hdr->authdata_size - enr_offset;
        } else {
            hdr->auth.handshake.enr_data = NULL;
            hdr->auth.handshake.enr_len = 0;
        }
        break;
    }
    default:
        return false;
    }

    return true;
}

/* =========================================================================
 * Decrypt message
 * ========================================================================= */

bool discv5_decrypt_message(uint8_t *pt, size_t *pt_len,
                             const uint8_t *packet, size_t pkt_len,
                             const discv5_header_t *hdr,
                             const uint8_t *header_buf,
                             const uint8_t session_key[16]) {
    size_t msg_start = hdr->total_hdr_len;
    if (msg_start + DISCV5_GCM_TAG_SIZE > pkt_len)
        return false;

    size_t msg_total = pkt_len - msg_start;
    if (msg_total < DISCV5_GCM_TAG_SIZE)
        return false;

    size_t ct_len = msg_total - DISCV5_GCM_TAG_SIZE;
    const uint8_t *ct = packet + msg_start;
    const uint8_t *tag = ct + ct_len;

    /* AAD = masking-iv || unmasked-header */
    size_t aad_len = DISCV5_MASKING_IV_SIZE + hdr->header_len;
    uint8_t aad[1024 + DISCV5_MASKING_IV_SIZE];
    if (aad_len > sizeof(aad)) return false;
    memcpy(aad, hdr->masking_iv, DISCV5_MASKING_IV_SIZE);
    memcpy(aad + DISCV5_MASKING_IV_SIZE, header_buf, hdr->header_len);

    bool ok = aes128_gcm_decrypt(pt, ct, ct_len, tag,
                                  aad, aad_len,
                                  session_key, hdr->nonce);
    if (ok) *pt_len = ct_len;
    return ok;
}
