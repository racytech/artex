/*
 * Tests for Portal Overlay Handler.
 *
 * Tests use NULL Discv5 engine (no network I/O). The overlay handler logic
 * is tested by feeding encoded messages to portal_overlay_on_request() and
 * verifying routing table state and content store callbacks.
 */

#include "../include/portal.h"
#include "../include/portal_wire.h"
#include "../include/portal_table.h"
#include "../include/history.h"
#include <stdio.h>
#include <string.h>
#include <assert.h>

static int tests_run = 0;
static int tests_passed = 0;

#define RUN_TEST(fn) do { \
    tests_run++; \
    printf("  [%d] %-50s", tests_run, #fn); \
    fn(); \
    tests_passed++; \
    printf(" OK\n"); \
} while(0)

/* =========================================================================
 * Mock content store
 * ========================================================================= */

/* Simple in-memory content store for testing */
#define MOCK_MAX_ITEMS  16
#define MOCK_MAX_VALUE  256

typedef struct {
    uint8_t key[32];
    size_t  key_len;
    uint8_t value[MOCK_MAX_VALUE];
    size_t  value_len;
} mock_item_t;

typedef struct {
    mock_item_t items[MOCK_MAX_ITEMS];
    int count;
    int get_calls;
    int put_calls;
    int content_id_calls;
} mock_store_t;

static void mock_store_init(mock_store_t *s) {
    memset(s, 0, sizeof(*s));
}

static void mock_store_add(mock_store_t *s,
                            const uint8_t *key, size_t key_len,
                            const uint8_t *value, size_t value_len) {
    assert(s->count < MOCK_MAX_ITEMS);
    assert(key_len <= 32);
    assert(value_len <= MOCK_MAX_VALUE);
    memcpy(s->items[s->count].key, key, key_len);
    s->items[s->count].key_len = key_len;
    memcpy(s->items[s->count].value, value, value_len);
    s->items[s->count].value_len = value_len;
    s->count++;
}

static bool mock_content_id(uint8_t out[32],
                             const uint8_t *key, size_t key_len,
                             void *user_data) {
    mock_store_t *s = (mock_store_t *)user_data;
    s->content_id_calls++;
    return history_content_id_from_key(out, key, key_len);
}

static size_t mock_get(const uint8_t *key, size_t key_len,
                        uint8_t *out, size_t cap,
                        void *user_data) {
    mock_store_t *s = (mock_store_t *)user_data;
    s->get_calls++;

    for (int i = 0; i < s->count; i++) {
        if (s->items[i].key_len == key_len &&
            memcmp(s->items[i].key, key, key_len) == 0) {
            if (out && cap >= s->items[i].value_len)
                memcpy(out, s->items[i].value, s->items[i].value_len);
            return s->items[i].value_len;
        }
    }
    return 0;
}

static bool mock_put(const uint8_t *key, size_t key_len,
                      const uint8_t *data, size_t data_len,
                      void *user_data) {
    mock_store_t *s = (mock_store_t *)user_data;
    s->put_calls++;
    mock_store_add(s, key, key_len, data, data_len);
    return true;
}

/* =========================================================================
 * Test helpers
 * ========================================================================= */

static const uint8_t LOCAL_ID[32] = {
    0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
};

static const uint8_t PEER_ID[32] = {
    0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
};

/* Max radius — all 0xFF bytes (LE) */
static const uint8_t MAX_RADIUS[32] = {
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
};

static portal_overlay_t *create_test_overlay(mock_store_t *store) {
    portal_content_store_t cs = {
        .content_id_fn = mock_content_id,
        .get_fn = mock_get,
        .put_fn = mock_put,
        .user_data = store,
    };

    return portal_overlay_create(NULL, NULL,
                                  HISTORY_PROTOCOL_ID,
                                  HISTORY_PROTOCOL_ID_LEN,
                                  LOCAL_ID, MAX_RADIUS,
                                  &cs);
}

/* =========================================================================
 * Test: Overlay create and destroy
 * ========================================================================= */

static void test_overlay_create_destroy(void) {
    mock_store_t store;
    mock_store_init(&store);

    portal_overlay_t *ov = create_test_overlay(&store);
    assert(ov != NULL);

    portal_table_t *t = portal_overlay_table(ov);
    assert(t != NULL);
    assert(portal_table_size(t) == 0);

    portal_overlay_destroy(ov);
}

/* =========================================================================
 * Test: PING updates routing table
 * ========================================================================= */

static void test_ping_updates_table(void) {
    mock_store_t store;
    mock_store_init(&store);

    portal_overlay_t *ov = create_test_overlay(&store);
    portal_table_t *t = portal_overlay_table(ov);

    /* Encode a PING with Type 1 payload (data_radius only) */
    uint8_t peer_radius[32];
    memset(peer_radius, 0, 32);
    peer_radius[0] = 0x42;  /* small radius (LE) */

    uint8_t payload[32];
    size_t payload_len = portal_encode_payload_type1(payload, sizeof(payload),
                                                      peer_radius);
    assert(payload_len == 32);

    uint8_t msg[128];
    size_t msg_len = portal_encode_ping(msg, sizeof(msg),
                                         1, /* enr_seq */
                                         PORTAL_PAYLOAD_TYPE1,
                                         payload, payload_len);
    assert(msg_len > 0);

    /* Process the PING */
    portal_overlay_on_request(ov, PEER_ID, 1, msg, msg_len);

    /* Verify peer was added to routing table */
    assert(portal_table_size(t) == 1);
    const portal_node_t *found = portal_table_find(t, PEER_ID);
    assert(found != NULL);
    assert(memcmp(found->node_id, PEER_ID, 32) == 0);

    portal_overlay_destroy(ov);
}

/* =========================================================================
 * Test: Multiple PINGs update the same peer
 * ========================================================================= */

static void test_ping_updates_existing(void) {
    mock_store_t store;
    mock_store_init(&store);

    portal_overlay_t *ov = create_test_overlay(&store);
    portal_table_t *t = portal_overlay_table(ov);

    /* First PING */
    uint8_t radius1[32] = {0};
    radius1[0] = 0x10;

    uint8_t payload[32];
    portal_encode_payload_type1(payload, sizeof(payload), radius1);

    uint8_t msg[128];
    size_t msg_len = portal_encode_ping(msg, sizeof(msg), 1,
                                         PORTAL_PAYLOAD_TYPE1, payload, 32);
    portal_overlay_on_request(ov, PEER_ID, 1, msg, msg_len);
    assert(portal_table_size(t) == 1);

    /* Second PING — same peer, different radius */
    uint8_t radius2[32] = {0};
    radius2[0] = 0x20;
    portal_encode_payload_type1(payload, sizeof(payload), radius2);

    msg_len = portal_encode_ping(msg, sizeof(msg), 2,
                                  PORTAL_PAYLOAD_TYPE1, payload, 32);
    portal_overlay_on_request(ov, PEER_ID, 2, msg, msg_len);

    /* Still only 1 node */
    assert(portal_table_size(t) == 1);

    portal_overlay_destroy(ov);
}

/* =========================================================================
 * Test: FIND_CONTENT for stored content triggers get callback
 * ========================================================================= */

static void test_find_content_stored(void) {
    mock_store_t store;
    mock_store_init(&store);

    /* Store content for block 100, body */
    uint8_t key[9];
    history_encode_content_key(key, HISTORY_SELECTOR_BODY, 100);
    uint8_t value[] = { 0xDE, 0xAD, 0xBE, 0xEF };
    mock_store_add(&store, key, 9, value, 4);

    portal_overlay_t *ov = create_test_overlay(&store);

    /* Encode FIND_CONTENT request */
    uint8_t msg[128];
    size_t msg_len = portal_encode_find_content(msg, sizeof(msg), key, 9);
    assert(msg_len > 0);

    /* Process request */
    int calls_before = store.get_calls;
    portal_overlay_on_request(ov, PEER_ID, 1, msg, msg_len);

    /* get_fn should have been called */
    assert(store.get_calls > calls_before);

    portal_overlay_destroy(ov);
}

/* =========================================================================
 * Test: FIND_CONTENT for missing content triggers get callback
 * ========================================================================= */

static void test_find_content_missing(void) {
    mock_store_t store;
    mock_store_init(&store);

    portal_overlay_t *ov = create_test_overlay(&store);

    /* Content key for block 999 (not stored) */
    uint8_t key[9];
    history_encode_content_key(key, HISTORY_SELECTOR_BODY, 999);

    uint8_t msg[128];
    size_t msg_len = portal_encode_find_content(msg, sizeof(msg), key, 9);
    assert(msg_len > 0);

    int calls_before = store.get_calls;
    portal_overlay_on_request(ov, PEER_ID, 1, msg, msg_len);

    /* get_fn should have been called but returned 0 */
    assert(store.get_calls > calls_before);

    portal_overlay_destroy(ov);
}

/* =========================================================================
 * Test: OFFER with content in radius
 * ========================================================================= */

static void test_offer_in_radius(void) {
    mock_store_t store;
    mock_store_init(&store);

    /* Overlay with max radius — everything is in radius */
    portal_overlay_t *ov = create_test_overlay(&store);

    /* Create OFFER with two content keys */
    uint8_t key1[9], key2[9];
    history_encode_content_key(key1, HISTORY_SELECTOR_BODY, 100);
    history_encode_content_key(key2, HISTORY_SELECTOR_RECEIPTS, 200);

    const uint8_t *keys[] = { key1, key2 };
    size_t key_lens[] = { 9, 9 };

    uint8_t msg[256];
    size_t msg_len = portal_encode_offer(msg, sizeof(msg),
                                          keys, key_lens, 2);
    assert(msg_len > 0);

    /* Process OFFER */
    portal_overlay_on_request(ov, PEER_ID, 1, msg, msg_len);

    /* content_id_fn should have been called for each key */
    assert(store.content_id_calls == 2);

    /* get_fn should have been called to check existence */
    assert(store.get_calls == 2);

    portal_overlay_destroy(ov);
}

/* =========================================================================
 * Test: OFFER with already-stored content
 * ========================================================================= */

static void test_offer_already_stored(void) {
    mock_store_t store;
    mock_store_init(&store);

    /* Pre-store content for block 100 */
    uint8_t key[9];
    history_encode_content_key(key, HISTORY_SELECTOR_BODY, 100);
    uint8_t value[] = { 0xAA, 0xBB };
    mock_store_add(&store, key, 9, value, 2);

    portal_overlay_t *ov = create_test_overlay(&store);

    /* OFFER the already-stored key */
    const uint8_t *keys[] = { key };
    size_t key_lens[] = { 9 };

    uint8_t msg[256];
    size_t msg_len = portal_encode_offer(msg, sizeof(msg),
                                          keys, key_lens, 1);
    assert(msg_len > 0);

    portal_overlay_on_request(ov, PEER_ID, 1, msg, msg_len);

    /* content_id should have been derived, and get should find it stored */
    assert(store.content_id_calls == 1);
    assert(store.get_calls >= 1);

    portal_overlay_destroy(ov);
}

/* =========================================================================
 * Test: FIND_NODES with empty table
 * ========================================================================= */

static void test_find_nodes_empty(void) {
    mock_store_t store;
    mock_store_init(&store);

    portal_overlay_t *ov = create_test_overlay(&store);

    /* FIND_NODES for distance 255 */
    uint16_t distances[] = { 255 };
    uint8_t msg[128];
    size_t msg_len = portal_encode_find_nodes(msg, sizeof(msg),
                                               distances, 1);
    assert(msg_len > 0);

    /* Should not crash with empty table */
    portal_overlay_on_request(ov, PEER_ID, 1, msg, msg_len);

    portal_overlay_destroy(ov);
}

/* =========================================================================
 * Test: Invalid message doesn't crash
 * ========================================================================= */

static void test_invalid_message(void) {
    mock_store_t store;
    mock_store_init(&store);

    portal_overlay_t *ov = create_test_overlay(&store);

    /* Feed garbage data */
    uint8_t garbage[] = { 0xFF, 0x01, 0x02 };
    portal_overlay_on_request(ov, PEER_ID, 1, garbage, sizeof(garbage));

    /* Empty data */
    portal_overlay_on_request(ov, PEER_ID, 2, NULL, 0);

    /* Should not crash */
    portal_overlay_destroy(ov);
}

/* =========================================================================
 * Test: Overlay without content store
 * ========================================================================= */

static void test_overlay_no_store(void) {
    portal_overlay_t *ov = portal_overlay_create(NULL, NULL,
                                                  HISTORY_PROTOCOL_ID,
                                                  HISTORY_PROTOCOL_ID_LEN,
                                                  LOCAL_ID, MAX_RADIUS,
                                                  NULL);
    assert(ov != NULL);

    /* FIND_CONTENT should not crash without store */
    uint8_t key[9];
    history_encode_content_key(key, HISTORY_SELECTOR_BODY, 100);

    uint8_t msg[128];
    size_t msg_len = portal_encode_find_content(msg, sizeof(msg), key, 9);
    portal_overlay_on_request(ov, PEER_ID, 1, msg, msg_len);

    /* OFFER should not crash without store */
    const uint8_t *keys[] = { key };
    size_t key_lens[] = { 9 };
    msg_len = portal_encode_offer(msg, sizeof(msg), keys, key_lens, 1);
    portal_overlay_on_request(ov, PEER_ID, 2, msg, msg_len);

    portal_overlay_destroy(ov);
}

/* =========================================================================
 * Test: Multiple peers in routing table
 * ========================================================================= */

static void test_multiple_peers(void) {
    mock_store_t store;
    mock_store_init(&store);

    portal_overlay_t *ov = create_test_overlay(&store);
    portal_table_t *t = portal_overlay_table(ov);

    /* PING from peer 1 */
    uint8_t radius[32] = {0};
    radius[0] = 0xFF;
    uint8_t payload[32];
    portal_encode_payload_type1(payload, sizeof(payload), radius);

    uint8_t msg[128];
    size_t msg_len = portal_encode_ping(msg, sizeof(msg), 1,
                                         PORTAL_PAYLOAD_TYPE1, payload, 32);
    portal_overlay_on_request(ov, PEER_ID, 1, msg, msg_len);

    /* PING from peer 2 */
    uint8_t peer2[32] = {0};
    peer2[0] = 0x03;
    portal_overlay_on_request(ov, peer2, 2, msg, msg_len);

    /* PING from peer 3 */
    uint8_t peer3[32] = {0};
    peer3[0] = 0x04;
    portal_overlay_on_request(ov, peer3, 3, msg, msg_len);

    assert(portal_table_size(t) == 3);

    portal_overlay_destroy(ov);
}

/* =========================================================================
 * Main
 * ========================================================================= */

int main(void) {
    printf("=== test_portal ===\n");

    RUN_TEST(test_overlay_create_destroy);
    RUN_TEST(test_ping_updates_table);
    RUN_TEST(test_ping_updates_existing);
    RUN_TEST(test_find_content_stored);
    RUN_TEST(test_find_content_missing);
    RUN_TEST(test_offer_in_radius);
    RUN_TEST(test_offer_already_stored);
    RUN_TEST(test_find_nodes_empty);
    RUN_TEST(test_invalid_message);
    RUN_TEST(test_overlay_no_store);
    RUN_TEST(test_multiple_peers);

    printf("\n  %d/%d tests passed\n\n", tests_passed, tests_run);
    return (tests_passed == tests_run) ? 0 : 1;
}
