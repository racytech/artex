#ifndef ART_NET_DISCV5_CODEC_H
#define ART_NET_DISCV5_CODEC_H

#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Discv5 Packet Codec — wire protocol encoding/decoding.
 *
 * Three packet types:
 *   flag=0 (Ordinary):  src-id || AES-GCM encrypted message
 *   flag=1 (WHOAREYOU): id-nonce || enr-seq, no message body
 *   flag=2 (Handshake): src-id || sig-size || eph-key-size || id-sig || eph-pubkey [|| enr] || AES-GCM message
 *
 * Header format: protocol-id(6) || version(2) || flag(1) || nonce(12) || authdata-size(2) || authdata
 * Header is masked with AES-128-CTR(key=dest-id[0:16], iv=masking-iv).
 * Message is encrypted with AES-128-GCM(key=session-key, nonce=nonce, aad=masking-iv||header).
 */

#define DISCV5_PROTOCOL_ID     "discv5"
#define DISCV5_VERSION         1
#define DISCV5_MASKING_IV_SIZE 16
#define DISCV5_NONCE_SIZE      12
#define DISCV5_ID_NONCE_SIZE   16
#define DISCV5_STATIC_HDR_SIZE 23   /* protocol-id(6) + version(2) + flag(1) + nonce(12) + authdata-size(2) */
#define DISCV5_GCM_TAG_SIZE    16

#define DISCV5_FLAG_ORDINARY   0
#define DISCV5_FLAG_WHOAREYOU  1
#define DISCV5_FLAG_HANDSHAKE  2

/* =========================================================================
 * Session keys
 * ========================================================================= */

typedef struct {
    uint8_t initiator_key[16];
    uint8_t recipient_key[16];
} discv5_keys_t;

/**
 * Derive session keys from ECDH shared secret + challenge data.
 *
 * Steps:
 *   1. shared_secret = ECDH-compressed(eph_priv, dest_pub) → 33 bytes
 *   2. prk = HKDF-Extract(salt=shared_secret, IKM=challenge_data)
 *   3. info = "discovery v5 key agreement" || node_id_a || node_id_b
 *   4. key_data = HKDF-Expand(prk, info, 32)
 *   5. initiator_key = key_data[0:16], recipient_key = key_data[16:32]
 *
 * @param keys            Output: derived session keys
 * @param eph_priv        32-byte ephemeral private key
 * @param dest_pub        64-byte destination public key (uncompressed, no prefix)
 * @param challenge_data  WHOAREYOU challenge data (masking-iv || header)
 * @param cd_len          Challenge data length (typically 63)
 * @param node_id_a       32-byte initiator node ID
 * @param node_id_b       32-byte recipient node ID
 */
bool discv5_derive_keys(discv5_keys_t *keys,
                        const uint8_t eph_priv[32],
                        const uint8_t dest_pub[64],
                        const uint8_t *challenge_data, size_t cd_len,
                        const uint8_t node_id_a[32],
                        const uint8_t node_id_b[32]);

/**
 * Derive session keys from a pre-computed ECDH shared secret.
 * Use this when you already have the 33-byte compressed ECDH point.
 */
bool discv5_derive_keys_from_secret(discv5_keys_t *keys,
                                     const uint8_t shared_secret[33],
                                     const uint8_t *challenge_data, size_t cd_len,
                                     const uint8_t node_id_a[32],
                                     const uint8_t node_id_b[32]);

/* =========================================================================
 * ID nonce signing
 * ========================================================================= */

/**
 * Sign the ID nonce for handshake authentication.
 *
 * Signs: SHA256("discovery v5 identity proof" || challenge_data || eph_pubkey || node_id_b)
 *
 * @param sig             Output: 64-byte signature (r || s)
 * @param challenge_data  WHOAREYOU challenge data
 * @param cd_len          Challenge data length
 * @param eph_pubkey      33-byte compressed ephemeral public key
 * @param node_id_b       32-byte recipient node ID
 * @param static_key      32-byte static private key
 */
bool discv5_sign_id_nonce(uint8_t sig[64],
                           const uint8_t *challenge_data, size_t cd_len,
                           const uint8_t eph_pubkey[33],
                           const uint8_t node_id_b[32],
                           const uint8_t static_key[32]);

/* =========================================================================
 * Packet encoding
 * ========================================================================= */

/**
 * Encode an ordinary message packet (flag=0).
 *
 * @return  Encoded packet length, or 0 on error
 */
size_t discv5_encode_ordinary(uint8_t *out, size_t out_cap,
                               const uint8_t masking_iv[16],
                               const uint8_t src_id[32],
                               const uint8_t dest_id[32],
                               const uint8_t nonce[12],
                               const uint8_t *pt, size_t pt_len,
                               const uint8_t session_key[16]);

/**
 * Encode a WHOAREYOU packet (flag=1).
 *
 * @return  Encoded packet length, or 0 on error
 */
size_t discv5_encode_whoareyou(uint8_t *out, size_t out_cap,
                                const uint8_t masking_iv[16],
                                const uint8_t dest_id[32],
                                const uint8_t request_nonce[12],
                                const uint8_t id_nonce[16],
                                uint64_t enr_seq);

/**
 * Encode a handshake message packet (flag=2).
 *
 * @param enr_record  RLP-encoded ENR (NULL if omitted)
 * @param enr_len     ENR length (0 if omitted)
 * @return            Encoded packet length, or 0 on error
 */
size_t discv5_encode_handshake(uint8_t *out, size_t out_cap,
                                const uint8_t masking_iv[16],
                                const uint8_t src_id[32],
                                const uint8_t dest_id[32],
                                const uint8_t nonce[12],
                                const uint8_t id_sig[64],
                                const uint8_t eph_pubkey[33],
                                const uint8_t *enr_record, size_t enr_len,
                                const uint8_t *pt, size_t pt_len,
                                const uint8_t session_key[16]);

/* =========================================================================
 * Packet decoding
 * ========================================================================= */

/** Decoded packet header. */
typedef struct {
    uint8_t  masking_iv[16];
    uint8_t  flag;
    uint8_t  nonce[12];
    uint16_t authdata_size;
    size_t   header_len;     /* total header bytes (static + authdata) */
    size_t   total_hdr_len;  /* masking-iv + header */

    union {
        struct {
            uint8_t src_id[32];
        } ordinary;
        struct {
            uint8_t  id_nonce[16];
            uint64_t enr_seq;
        } whoareyou;
        struct {
            uint8_t  src_id[32];
            uint8_t  sig_size;
            uint8_t  eph_key_size;
            uint8_t  id_sig[64];
            uint8_t  eph_pubkey[33];
            const uint8_t *enr_data;   /* points into decoded header buffer */
            size_t   enr_len;
        } handshake;
    } auth;
} discv5_header_t;

/**
 * Decode (unmask) a packet header.
 *
 * @param hdr           Output: parsed header
 * @param header_buf    Output: unmasked header bytes (for use as GCM AAD).
 *                      Must be large enough (packet length - 16).
 * @param packet        Raw packet data
 * @param pkt_len       Packet length
 * @param local_id      32-byte local node ID (masking key = local_id[0:16])
 * @return              true on success
 */
bool discv5_decode_header(discv5_header_t *hdr,
                           uint8_t *header_buf,
                           const uint8_t *packet, size_t pkt_len,
                           const uint8_t local_id[32]);

/**
 * Decrypt a message from a decoded packet.
 *
 * @param pt            Output: decrypted plaintext
 * @param pt_len        Output: plaintext length
 * @param packet        Raw packet data
 * @param pkt_len       Packet length
 * @param hdr           Parsed header (from discv5_decode_header)
 * @param header_buf    Unmasked header bytes (from discv5_decode_header)
 * @param session_key   16-byte session key
 * @return              true on success (GCM tag valid)
 */
bool discv5_decrypt_message(uint8_t *pt, size_t *pt_len,
                             const uint8_t *packet, size_t pkt_len,
                             const discv5_header_t *hdr,
                             const uint8_t *header_buf,
                             const uint8_t session_key[16]);

#ifdef __cplusplus
}
#endif

#endif /* ART_NET_DISCV5_CODEC_H */
