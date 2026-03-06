#ifndef ART_NET_PORTAL_H
#define ART_NET_PORTAL_H

#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>
#include "portal_table.h"
#include "portal_wire.h"
#include "discv5.h"
#include "utp.h"

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Portal Overlay Handler — generic integration layer for Portal sub-protocols.
 *
 * Each sub-protocol (history, state, beacon) creates one overlay instance.
 * The overlay:
 *   1. Registers a TALKREQ handler with Discv5 for its protocol ID
 *   2. Decodes incoming messages using portal_wire
 *   3. Dispatches to per-message-type handlers
 *   4. Manages a portal_table_t routing table
 *   5. Bridges large content transfers via uTP
 *
 * Sub-protocols provide content storage through callbacks.
 */

/* Maximum response buffer size */
#define PORTAL_RESP_BUF_SIZE  2048

/* =========================================================================
 * Content store callbacks
 * ========================================================================= */

/**
 * Content storage interface — provided by the sub-protocol.
 *
 * The overlay uses these callbacks to look up, store, and derive content IDs.
 */
typedef struct {
    /**
     * Compute content_id from content_key.
     * @return true on success, false if key is invalid
     */
    bool (*content_id_fn)(uint8_t out[32],
                          const uint8_t *key, size_t key_len,
                          void *user_data);

    /**
     * Retrieve stored content by content_key.
     * @return content length, or 0 if not found.
     *         If out is NULL, just check existence.
     */
    size_t (*get_fn)(const uint8_t *key, size_t key_len,
                     uint8_t *out, size_t cap,
                     void *user_data);

    /**
     * Store content. Returns true on success.
     */
    bool (*put_fn)(const uint8_t *key, size_t key_len,
                   const uint8_t *data, size_t data_len,
                   void *user_data);

    void *user_data;
} portal_content_store_t;

/* =========================================================================
 * Overlay handler
 * ========================================================================= */

typedef struct portal_overlay portal_overlay_t;

/**
 * Create a portal overlay for a sub-protocol.
 *
 * @param engine       Discv5 engine (for sending TALKREQ/TALKRESP)
 * @param utp          uTP context (for streaming large content), may be NULL
 * @param protocol_id  Protocol identifier string (e.g. "\x50\x00" for history)
 * @param proto_len    Length of protocol_id
 * @param local_id     32-byte local node ID
 * @param data_radius  32-byte initial data_radius (little-endian U256)
 * @param store        Content storage callbacks
 * @return             Overlay instance, or NULL on error
 */
portal_overlay_t *portal_overlay_create(
    discv5_engine_t *engine,
    utp_ctx_t *utp,
    const char *protocol_id,
    size_t proto_len,
    const uint8_t local_id[32],
    const uint8_t data_radius[32],
    const portal_content_store_t *store);

/** Destroy overlay and release resources. */
void portal_overlay_destroy(portal_overlay_t *overlay);

/** Get the overlay's routing table (for inspection or direct manipulation). */
portal_table_t *portal_overlay_table(portal_overlay_t *overlay);

/* =========================================================================
 * Incoming message handler
 * ========================================================================= */

/**
 * Process an incoming TALKREQ for this overlay.
 *
 * Called by the Discv5 TALKREQ handler when the protocol string matches.
 * Decodes the message, dispatches to the appropriate handler, and sends
 * back a TALKRESP with the encoded response.
 *
 * @param overlay   Overlay instance
 * @param peer_id   32-byte peer node ID
 * @param req_id    TALKREQ request ID (for sending TALKRESP)
 * @param data      Raw message bytes (msg_id + SSZ body)
 * @param len       Message length
 */
void portal_overlay_on_request(portal_overlay_t *overlay,
                               const uint8_t peer_id[32],
                               uint32_t req_id,
                               const uint8_t *data, size_t len);

/* =========================================================================
 * Outgoing requests
 * ========================================================================= */

/** Send PING to a peer. */
void portal_overlay_ping(portal_overlay_t *overlay,
                         const uint8_t peer_id[32]);

/** Send FIND_NODES to a peer. */
void portal_overlay_find_nodes(portal_overlay_t *overlay,
                               const uint8_t peer_id[32],
                               const uint16_t *distances, size_t count);

/** Send FIND_CONTENT to a peer. */
void portal_overlay_find_content(portal_overlay_t *overlay,
                                 const uint8_t peer_id[32],
                                 const uint8_t *key, size_t key_len);

/** Send OFFER to a peer. */
void portal_overlay_offer(portal_overlay_t *overlay,
                          const uint8_t peer_id[32],
                          const uint8_t *const *keys,
                          const size_t *key_lens,
                          size_t key_count);

#ifdef __cplusplus
}
#endif

#endif /* ART_NET_PORTAL_H */
