/*
 * Portal Overlay Routing Table — test suite.
 *
 * Tests: insert/find, bucket indexing, closest nodes, radius filtering,
 * content-aware lookups, SHA-256 content ID, replacement promotion.
 */

#include "../include/portal_table.h"
#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <blst.h>

/* =========================================================================
 * Helpers
 * ========================================================================= */

/* Create a node with a specific ID byte pattern */
static portal_node_t make_node(uint8_t id_byte, uint16_t port) {
    portal_node_t n;
    memset(&n, 0, sizeof(n));
    memset(n.node_id, id_byte, 32);
    n.udp_port = port;
    /* Default: max radius (all 0xFF LE = max U256) */
    memset(n.data_radius, 0xFF, 32);
    return n;
}

/* Create a node with a specific first byte (rest zeros) */
static portal_node_t make_node_first(uint8_t first_byte) {
    portal_node_t n;
    memset(&n, 0, sizeof(n));
    n.node_id[0] = first_byte;
    memset(n.data_radius, 0xFF, 32);
    return n;
}

/* Set radius to a single-byte value in MSB (big-endian distance threshold).
 * Since radius is little-endian, byte 31 = MSB. */
static void set_radius_msb(portal_node_t *n, uint8_t msb) {
    memset(n->data_radius, 0, 32);
    n->data_radius[31] = msb;
}

/* Max radius (all 0xFF) = covers entire keyspace */
static const uint8_t MAX_RADIUS[32] = {
    0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,
    0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,
    0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,
    0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,
};

/* Zero radius = covers nothing */
static const uint8_t ZERO_RADIUS[32] = {0};

/* =========================================================================
 * Test: Insert and find
 * ========================================================================= */

static void test_insert_find(void) {
    printf("  test_insert_find... ");

    uint8_t local_id[32] = {0};
    portal_table_t table;
    portal_table_init(&table, local_id, MAX_RADIUS);

    /* Insert a node */
    portal_node_t n = make_node(0x80, 9000);
    portal_table_result_t r = portal_table_insert(&table, &n);
    assert(r == PORTAL_TABLE_ADDED);
    assert(portal_table_size(&table) == 1);

    /* Find it */
    const portal_node_t *found = portal_table_find(&table, n.node_id);
    assert(found != NULL);
    assert(found->udp_port == 9000);

    /* Update it */
    n.udp_port = 9001;
    r = portal_table_insert(&table, &n);
    assert(r == PORTAL_TABLE_UPDATED);
    assert(portal_table_size(&table) == 1);
    found = portal_table_find(&table, n.node_id);
    assert(found->udp_port == 9001);

    /* Reject self */
    portal_node_t self;
    memset(&self, 0, sizeof(self)); /* same as local_id */
    r = portal_table_insert(&table, &self);
    assert(r == PORTAL_TABLE_SELF);

    /* Remove */
    assert(portal_table_remove(&table, n.node_id));
    assert(portal_table_size(&table) == 0);
    assert(portal_table_find(&table, n.node_id) == NULL);

    printf("OK\n");
}

/* =========================================================================
 * Test: Bucket index (log-distance)
 * ========================================================================= */

static void test_bucket_index(void) {
    printf("  test_bucket_index... ");

    uint8_t local[32] = {0};
    uint8_t node[32] = {0};

    /* Same ID → -1 */
    assert(portal_table_bucket_index(local, node) == -1);

    /* Differ in last bit → bucket 0 */
    node[31] = 0x01;
    assert(portal_table_bucket_index(local, node) == 0);

    /* Differ in MSB → bucket 255 */
    memset(node, 0, 32);
    node[0] = 0x80;
    assert(portal_table_bucket_index(local, node) == 255);

    /* Differ in second bit → bucket 254 */
    memset(node, 0, 32);
    node[0] = 0x40;
    assert(portal_table_bucket_index(local, node) == 254);

    /* Differ at byte 16, bit 0 → bucket 127 */
    memset(node, 0, 32);
    node[16] = 0x80;
    assert(portal_table_bucket_index(local, node) == 127);

    printf("OK\n");
}

/* =========================================================================
 * Test: Closest nodes
 * ========================================================================= */

static void test_closest(void) {
    printf("  test_closest... ");

    uint8_t local_id[32] = {0};
    portal_table_t table;
    portal_table_init(&table, local_id, MAX_RADIUS);

    /* Insert nodes at varying distances */
    portal_node_t n1 = make_node_first(0x80);  /* distance = 0x80... (far) */
    portal_node_t n2 = make_node_first(0x01);  /* distance = 0x01... (close) */
    portal_node_t n3 = make_node_first(0x40);  /* distance = 0x40... (mid) */
    portal_node_t n4 = make_node_first(0x02);  /* distance = 0x02... (close) */

    portal_table_insert(&table, &n1);
    portal_table_insert(&table, &n2);
    portal_table_insert(&table, &n3);
    portal_table_insert(&table, &n4);

    /* Find 3 closest to local_id (all zeros → distance = node_id) */
    portal_node_t out[4];
    uint8_t target[32] = {0};
    size_t count = portal_table_closest(&table, target, out, 3);
    assert(count == 3);
    assert(out[0].node_id[0] == 0x01); /* closest */
    assert(out[1].node_id[0] == 0x02);
    assert(out[2].node_id[0] == 0x40);

    printf("OK\n");
}

/* =========================================================================
 * Test: Radius update
 * ========================================================================= */

static void test_radius_update(void) {
    printf("  test_radius_update... ");

    uint8_t local_id[32] = {0};
    portal_table_t table;
    portal_table_init(&table, local_id, MAX_RADIUS);

    portal_node_t n = make_node(0x80, 9000);
    portal_table_insert(&table, &n);

    /* Update radius */
    uint8_t new_radius[32];
    memset(new_radius, 0, 32);
    new_radius[31] = 0x40; /* small radius: only 0x40... in MSB */

    assert(portal_table_update_radius(&table, n.node_id, new_radius));

    const portal_node_t *found = portal_table_find(&table, n.node_id);
    assert(found != NULL);
    assert(memcmp(found->data_radius, new_radius, 32) == 0);

    /* Non-existent node */
    uint8_t fake_id[32] = {0x99};
    assert(!portal_table_update_radius(&table, fake_id, new_radius));

    printf("OK\n");
}

/* =========================================================================
 * Test: Content in radius (local)
 * ========================================================================= */

static void test_content_in_radius(void) {
    printf("  test_content_in_radius... ");

    uint8_t local_id[32] = {0};
    portal_table_t table;

    /* Small radius: only content with distance <= 0x10 (MSB) */
    uint8_t radius[32] = {0};
    radius[31] = 0x10;  /* LE: MSB = 0x10 */
    portal_table_init(&table, local_id, radius);

    /* Content close to us (distance byte 0 = 0x05, within 0x10) */
    uint8_t close_content[32] = {0};
    close_content[0] = 0x05;
    assert(portal_table_content_in_radius(&table, close_content));

    /* Content far from us (distance byte 0 = 0x20, outside 0x10) */
    uint8_t far_content[32] = {0};
    far_content[0] = 0x20;
    assert(!portal_table_content_in_radius(&table, far_content));

    /* Exact boundary (distance = radius) → in radius */
    uint8_t boundary[32] = {0};
    boundary[0] = 0x10;
    assert(portal_table_content_in_radius(&table, boundary));

    /* Max radius → everything in radius */
    portal_table_set_radius(&table, MAX_RADIUS);
    assert(portal_table_content_in_radius(&table, far_content));

    /* Zero radius → nothing (except distance=0) */
    portal_table_set_radius(&table, ZERO_RADIUS);
    assert(portal_table_content_in_radius(&table, local_id)); /* dist=0 */
    assert(!portal_table_content_in_radius(&table, close_content));

    printf("OK\n");
}

/* =========================================================================
 * Test: Node content in radius
 * ========================================================================= */

static void test_node_content_in_radius(void) {
    printf("  test_node_content_in_radius... ");

    portal_node_t node;
    memset(&node, 0, sizeof(node));
    node.node_id[0] = 0x10;

    /* Radius = 0x20 (MSB) → covers distance up to 0x20 */
    set_radius_msb(&node, 0x20);

    /* Content: id[0]=0x00 → distance[0] = 0x10^0x00 = 0x10, within 0x20 */
    uint8_t c1[32] = {0};
    assert(portal_node_content_in_radius(&node, c1));

    /* Content: id[0]=0x40 → distance[0] = 0x10^0x40 = 0x50, outside 0x20 */
    uint8_t c2[32] = {0};
    c2[0] = 0x40;
    assert(!portal_node_content_in_radius(&node, c2));

    /* Content: id[0]=0x30 → distance[0] = 0x10^0x30 = 0x20, exactly at boundary */
    uint8_t c3[32] = {0};
    c3[0] = 0x30;
    assert(portal_node_content_in_radius(&node, c3));

    printf("OK\n");
}

/* =========================================================================
 * Test: Closest to content (radius-filtered)
 * ========================================================================= */

static void test_closest_to_content(void) {
    printf("  test_closest_to_content... ");

    uint8_t local_id[32] = {0};
    portal_table_t table;
    portal_table_init(&table, local_id, MAX_RADIUS);

    /* Node A: close to content, big radius → should be returned */
    portal_node_t a = make_node_first(0x01);
    memset(a.data_radius, 0xFF, 32); /* max radius */
    portal_table_insert(&table, &a);

    /* Node B: close to content, tiny radius → should be filtered out */
    portal_node_t b = make_node_first(0x02);
    set_radius_msb(&b, 0x01); /* radius = 0x01 MSB, very small */
    portal_table_insert(&table, &b);

    /* Node C: far from content, big radius → should be returned */
    portal_node_t c = make_node_first(0x80);
    memset(c.data_radius, 0xFF, 32);
    portal_table_insert(&table, &c);

    /* Content at 0x00... → distances are just the node IDs */
    uint8_t content[32] = {0};
    portal_node_t out[4];
    size_t count = portal_table_closest_to_content(&table, content, out, 4);

    /* B has radius 0x01 MSB but distance to content is 0x02 MSB → filtered out */
    assert(count == 2);
    assert(out[0].node_id[0] == 0x01); /* A: closest, in radius */
    assert(out[1].node_id[0] == 0x80); /* C: farther, but max radius */

    printf("OK\n");
}

/* =========================================================================
 * Test: Content ID derivation (SHA-256)
 * ========================================================================= */

static void test_content_id(void) {
    printf("  test_content_id... ");

    /* SHA-256("") = e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855 */
    uint8_t out[32];
    portal_content_id(out, (const uint8_t *)"", 0);

    uint8_t expected[32] = {
        0xe3, 0xb0, 0xc4, 0x42, 0x98, 0xfc, 0x1c, 0x14,
        0x9a, 0xfb, 0xf4, 0xc8, 0x99, 0x6f, 0xb9, 0x24,
        0x27, 0xae, 0x41, 0xe4, 0x64, 0x9b, 0x93, 0x4c,
        0xa4, 0x95, 0x99, 0x1b, 0x78, 0x52, 0xb8, 0x55,
    };
    assert(memcmp(out, expected, 32) == 0);

    /* SHA-256("abc") = ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad */
    portal_content_id(out, (const uint8_t *)"abc", 3);
    uint8_t expected2[32] = {
        0xba, 0x78, 0x16, 0xbf, 0x8f, 0x01, 0xcf, 0xea,
        0x41, 0x41, 0x40, 0xde, 0x5d, 0xae, 0x22, 0x23,
        0xb0, 0x03, 0x61, 0xa3, 0x96, 0x17, 0x7a, 0x9c,
        0xb4, 0x10, 0xff, 0x61, 0xf2, 0x00, 0x15, 0xad,
    };
    assert(memcmp(out, expected2, 32) == 0);

    printf("OK\n");
}

/* =========================================================================
 * Test: Full bucket + replacement
 * ========================================================================= */

static void test_bucket_full(void) {
    printf("  test_bucket_full... ");

    uint8_t local_id[32] = {0};
    portal_table_t table;
    portal_table_init(&table, local_id, MAX_RADIUS);

    /* Fill bucket 255 (nodes with MSB set, different lower bytes) */
    for (int i = 0; i < PORTAL_BUCKET_SIZE; i++) {
        portal_node_t n;
        memset(&n, 0, sizeof(n));
        n.node_id[0] = 0x80;
        n.node_id[1] = (uint8_t)i;
        memset(n.data_radius, 0xFF, 32);
        n.udp_port = (uint16_t)(1000 + i);
        portal_table_result_t r = portal_table_insert(&table, &n);
        assert(r == PORTAL_TABLE_ADDED);
    }
    assert(portal_table_size(&table) == PORTAL_BUCKET_SIZE);

    /* 17th node goes to replacement cache */
    portal_node_t extra;
    memset(&extra, 0, sizeof(extra));
    extra.node_id[0] = 0x80;
    extra.node_id[1] = 0xFF;
    memset(extra.data_radius, 0xFF, 32);
    extra.udp_port = 2000;
    portal_table_result_t r = portal_table_insert(&table, &extra);
    assert(r == PORTAL_TABLE_REPLACEMENT);
    assert(portal_table_size(&table) == PORTAL_BUCKET_SIZE); /* still 16 in bucket */

    printf("OK\n");
}

/* =========================================================================
 * Test: Mark alive / dead with replacement promotion
 * ========================================================================= */

static void test_mark_alive_dead(void) {
    printf("  test_mark_alive_dead... ");

    uint8_t local_id[32] = {0};
    portal_table_t table;
    portal_table_init(&table, local_id, MAX_RADIUS);

    /* Fill bucket */
    for (int i = 0; i < PORTAL_BUCKET_SIZE; i++) {
        portal_node_t n;
        memset(&n, 0, sizeof(n));
        n.node_id[0] = 0x80;
        n.node_id[1] = (uint8_t)i;
        memset(n.data_radius, 0xFF, 32);
        portal_table_insert(&table, &n);
    }

    /* Add replacement */
    portal_node_t repl;
    memset(&repl, 0, sizeof(repl));
    repl.node_id[0] = 0x80;
    repl.node_id[1] = 0xAA;
    memset(repl.data_radius, 0xFF, 32);
    repl.udp_port = 5555;
    portal_table_insert(&table, &repl);

    /* Mark first node alive — should move to tail */
    uint8_t first_id[32] = {0};
    first_id[0] = 0x80;
    first_id[1] = 0x00;
    portal_table_mark_alive(&table, first_id);
    const portal_node_t *found = portal_table_find(&table, first_id);
    assert(found != NULL);
    assert(found->checks == 1);

    /* Mark second node dead — replacement should be promoted */
    uint8_t dead_id[32] = {0};
    dead_id[0] = 0x80;
    dead_id[1] = 0x01;
    bool promoted = portal_table_mark_dead(&table, dead_id);
    assert(promoted);
    assert(portal_table_size(&table) == PORTAL_BUCKET_SIZE);

    /* Dead node gone, replacement present */
    assert(portal_table_find(&table, dead_id) == NULL);
    found = portal_table_find(&table, repl.node_id);
    assert(found != NULL);
    assert(found->udp_port == 5555);

    printf("OK\n");
}

/* =========================================================================
 * Test: Set radius
 * ========================================================================= */

static void test_set_radius(void) {
    printf("  test_set_radius... ");

    uint8_t local_id[32] = {0};
    portal_table_t table;
    portal_table_init(&table, local_id, ZERO_RADIUS);

    /* Initially zero radius → nothing in range */
    uint8_t content[32] = {0};
    content[0] = 0x01;
    assert(!portal_table_content_in_radius(&table, content));

    /* Expand radius */
    portal_table_set_radius(&table, MAX_RADIUS);
    assert(portal_table_content_in_radius(&table, content));

    printf("OK\n");
}

/* =========================================================================
 * Main
 * ========================================================================= */

int main(void) {
    printf("test_portal_table (10 tests)\n");

    test_insert_find();             /* 1 */
    test_bucket_index();            /* 2 */
    test_closest();                 /* 3 */
    test_radius_update();           /* 4 */
    test_content_in_radius();       /* 5 */
    test_node_content_in_radius();  /* 6 */
    test_closest_to_content();      /* 7 */
    test_content_id();              /* 8 */
    test_bucket_full();             /* 9 */
    test_mark_alive_dead();         /* 10 */
    test_set_radius();              /* 11 */

    printf("\nAll 11 tests passed.\n");
    return 0;
}
