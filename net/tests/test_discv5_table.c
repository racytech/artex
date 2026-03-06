/*
 * Test: Discv5 Kademlia Routing Table
 *
 * Tests:
 *  1. Bucket index — XOR log-distance calculation
 *  2. Insert and find — basic operations
 *  3. LRU ordering — existing nodes move to tail
 *  4. Bucket full — overflow to replacement cache
 *  5. Mark dead — eviction + replacement promotion
 *  6. Mark alive — updates last_seen and moves to tail
 *  7. Closest nodes — XOR distance sorted
 *  8. Nodes at distance — FINDNODE response
 *  9. Remove — from bucket and replacement cache
 * 10. Table size — total node count
 */

#include "../include/discv5_table.h"
#include <stdio.h>
#include <string.h>

static int tests_passed = 0;
static int tests_failed = 0;

#define TEST(name) do { printf("  [%s] ", name); } while(0)
#define PASS() do { printf("PASS\n"); tests_passed++; return; } while(0)
#define FAIL(msg) do { printf("FAIL: %s\n", msg); tests_failed++; return; } while(0)
#define ASSERT(cond, msg) do { if (!(cond)) FAIL(msg); } while(0)

/* Make a node with a specific ID (first byte set, rest zero) */
static discv5_node_t make_node(uint8_t first_byte) {
    discv5_node_t n;
    memset(&n, 0, sizeof(n));
    n.node_id[0] = first_byte;
    return n;
}

/* Make a node with a specific ID at byte position */
static discv5_node_t make_node_at(int byte_pos, uint8_t val) {
    discv5_node_t n;
    memset(&n, 0, sizeof(n));
    n.node_id[byte_pos] = val;
    return n;
}

/* =========================================================================
 * Test 1: Bucket index
 * ========================================================================= */

static void test_bucket_index(void) {
    TEST("bucket index");

    uint8_t local[32], other[32];
    memset(local, 0, 32);
    memset(other, 0, 32);

    /* Same ID → -1 */
    ASSERT(discv5_table_bucket_index(local, other) == -1, "same ID");

    /* Differ in last bit → bucket 0 */
    other[31] = 0x01;
    ASSERT(discv5_table_bucket_index(local, other) == 0, "last bit → 0");

    /* Differ in second-to-last bit → bucket 1 */
    other[31] = 0x02;
    ASSERT(discv5_table_bucket_index(local, other) == 1, "bit 1 → 1");

    /* Differ in high bit of last byte → bucket 7 */
    other[31] = 0x80;
    ASSERT(discv5_table_bucket_index(local, other) == 7, "bit 7 → 7");

    /* Differ in first byte, high bit → bucket 255 */
    other[31] = 0;
    other[0] = 0x80;
    ASSERT(discv5_table_bucket_index(local, other) == 255, "first bit → 255");

    /* Differ in first byte, low bit → bucket 248 */
    other[0] = 0x01;
    ASSERT(discv5_table_bucket_index(local, other) == 248, "byte0 bit0 → 248");

    PASS();
}

/* =========================================================================
 * Test 2: Insert and find
 * ========================================================================= */

static void test_insert_find(void) {
    TEST("insert and find");

    discv5_table_t table;
    uint8_t local[32];
    memset(local, 0, 32);
    discv5_table_init(&table, local);

    /* Insert node with ID byte[0]=0x80 → bucket 255 */
    discv5_node_t n = make_node(0x80);
    n.udp_port = 30303;
    ASSERT(discv5_table_insert(&table, &n) == DISCV5_TABLE_ADDED, "insert");

    /* Find it */
    const discv5_node_t *found = discv5_table_find(&table, n.node_id);
    ASSERT(found != NULL, "found");
    ASSERT(found->udp_port == 30303, "port matches");

    /* Not found */
    uint8_t unknown[32] = {0xFF};
    ASSERT(discv5_table_find(&table, unknown) == NULL, "unknown not found");

    /* Reject self */
    discv5_node_t self;
    memset(&self, 0, sizeof(self));  /* same as local_id */
    ASSERT(discv5_table_insert(&table, &self) == DISCV5_TABLE_SELF, "reject self");

    ASSERT(discv5_table_size(&table) == 1, "size = 1");

    PASS();
}

/* =========================================================================
 * Test 3: LRU ordering
 * ========================================================================= */

static void test_lru_ordering(void) {
    TEST("LRU ordering");

    discv5_table_t table;
    uint8_t local[32];
    memset(local, 0, 32);
    discv5_table_init(&table, local);

    /* Insert 3 nodes in same bucket (all differ in byte[0] high bit) */
    discv5_node_t n1 = make_node(0x81);
    discv5_node_t n2 = make_node(0x82);
    discv5_node_t n3 = make_node(0x83);

    discv5_table_insert(&table, &n1);  /* oldest */
    discv5_table_insert(&table, &n2);
    discv5_table_insert(&table, &n3);  /* newest */

    /* Re-insert n1 → should move to tail */
    ASSERT(discv5_table_insert(&table, &n1) == DISCV5_TABLE_UPDATED, "re-insert");

    /* Check order: n2 (oldest), n3, n1 (newest) */
    discv5_bucket_t *b = &table.buckets[255];
    ASSERT(b->count == 3, "count = 3");
    ASSERT(b->entries[0].node_id[0] == 0x82, "n2 at head");
    ASSERT(b->entries[1].node_id[0] == 0x83, "n3 in middle");
    ASSERT(b->entries[2].node_id[0] == 0x81, "n1 at tail");

    PASS();
}

/* =========================================================================
 * Test 4: Bucket full → replacement cache
 * ========================================================================= */

static void test_replacement_cache(void) {
    TEST("replacement cache");

    discv5_table_t table;
    uint8_t local[32];
    memset(local, 0, 32);
    discv5_table_init(&table, local);

    /* Fill bucket 255 with 16 nodes (0x80..0x8F) */
    for (int i = 0; i < 16; i++) {
        discv5_node_t n = make_node(0x80 + i);
        ASSERT(discv5_table_insert(&table, &n) == DISCV5_TABLE_ADDED, "fill bucket");
    }

    discv5_bucket_t *b = &table.buckets[255];
    ASSERT(b->count == 16, "bucket full");

    /* 17th node → replacement cache */
    discv5_node_t extra = make_node(0x90);
    ASSERT(discv5_table_insert(&table, &extra) == DISCV5_TABLE_REPLACEMENT, "goes to replacement");
    ASSERT(b->repl_count == 1, "repl_count = 1");
    ASSERT(memcmp(b->replacements[0].node_id, extra.node_id, 32) == 0, "replacement stored");

    /* Table size only counts bucket entries */
    ASSERT(discv5_table_size(&table) == 16, "size = 16 (excludes replacements)");

    PASS();
}

/* =========================================================================
 * Test 5: Mark dead — eviction + replacement promotion
 * ========================================================================= */

static void test_mark_dead(void) {
    TEST("mark dead + promotion");

    discv5_table_t table;
    uint8_t local[32];
    memset(local, 0, 32);
    discv5_table_init(&table, local);

    /* Fill bucket with 16 nodes */
    for (int i = 0; i < 16; i++) {
        discv5_node_t n = make_node(0x80 + i);
        discv5_table_insert(&table, &n);
    }

    /* Add 2 replacements */
    discv5_node_t r1 = make_node(0x90);
    discv5_node_t r2 = make_node(0x91);
    discv5_table_insert(&table, &r1);
    discv5_table_insert(&table, &r2);

    discv5_bucket_t *b = &table.buckets[255];
    ASSERT(b->count == 16, "bucket full");
    ASSERT(b->repl_count == 2, "2 replacements");

    /* Mark node 0x80 as dead → should promote most recent replacement (r2) */
    uint8_t dead_id[32];
    memset(dead_id, 0, 32);
    dead_id[0] = 0x80;
    ASSERT(discv5_table_mark_dead(&table, dead_id), "promotion happened");

    /* Bucket should still be full (15 original + 1 promoted) */
    ASSERT(b->count == 16, "still 16");
    ASSERT(b->repl_count == 1, "1 replacement left");

    /* Dead node should not be found */
    ASSERT(discv5_table_find(&table, dead_id) == NULL, "dead removed");

    /* r2 (most recently seen replacement) should now be in bucket */
    ASSERT(discv5_table_find(&table, r2.node_id) != NULL, "r2 promoted");

    PASS();
}

/* =========================================================================
 * Test 6: Mark alive
 * ========================================================================= */

static void test_mark_alive(void) {
    TEST("mark alive");

    discv5_table_t table;
    uint8_t local[32];
    memset(local, 0, 32);
    discv5_table_init(&table, local);

    discv5_node_t n1 = make_node(0x81);
    discv5_node_t n2 = make_node(0x82);
    discv5_table_insert(&table, &n1);
    discv5_table_insert(&table, &n2);

    /* n1 is at position 0 (oldest), n2 at position 1 */
    discv5_bucket_t *b = &table.buckets[255];
    ASSERT(b->entries[0].node_id[0] == 0x81, "n1 oldest");

    /* Mark n1 alive → should move to tail */
    discv5_table_mark_alive(&table, n1.node_id);
    ASSERT(b->entries[0].node_id[0] == 0x82, "n2 now oldest");
    ASSERT(b->entries[1].node_id[0] == 0x81, "n1 now newest");

    /* Check counter incremented */
    ASSERT(b->entries[1].checks == 1, "checks = 1");

    PASS();
}

/* =========================================================================
 * Test 7: Closest nodes
 * ========================================================================= */

static void test_closest_nodes(void) {
    TEST("closest nodes");

    discv5_table_t table;
    uint8_t local[32];
    memset(local, 0, 32);
    discv5_table_init(&table, local);

    /* Add nodes at various distances */
    discv5_node_t n1 = make_node_at(31, 0x01);  /* dist = 0x01 (bucket 0) */
    discv5_node_t n2 = make_node_at(31, 0x02);  /* dist = 0x02 (bucket 1) */
    discv5_node_t n3 = make_node_at(31, 0x10);  /* dist = 0x10 (bucket 4) */
    discv5_node_t n4 = make_node_at(0, 0x80);   /* dist = 0x80<<248 (bucket 255) */

    discv5_table_insert(&table, &n1);
    discv5_table_insert(&table, &n2);
    discv5_table_insert(&table, &n3);
    discv5_table_insert(&table, &n4);

    /* Target = 0x00...00 (same as local), closest should be n1 (dist 0x01) */
    uint8_t target[32];
    memset(target, 0, 32);

    discv5_node_t results[16];
    size_t count = discv5_table_closest(&table, target, results, 16);
    ASSERT(count == 4, "found 4 nodes");

    /* Should be sorted by distance: n1, n2, n3, n4 */
    ASSERT(results[0].node_id[31] == 0x01, "closest = n1");
    ASSERT(results[1].node_id[31] == 0x02, "second = n2");
    ASSERT(results[2].node_id[31] == 0x10, "third = n3");
    ASSERT(results[3].node_id[0]  == 0x80, "fourth = n4");

    /* Limit results */
    count = discv5_table_closest(&table, target, results, 2);
    ASSERT(count == 2, "limited to 2");
    ASSERT(results[0].node_id[31] == 0x01, "still closest");

    PASS();
}

/* =========================================================================
 * Test 8: Nodes at distance
 * ========================================================================= */

static void test_nodes_at_distance(void) {
    TEST("nodes at distance");

    discv5_table_t table;
    uint8_t local[32];
    memset(local, 0, 32);
    discv5_table_init(&table, local);

    /* Add 3 nodes at bucket 255 and 1 at bucket 0 */
    discv5_node_t n1 = make_node(0x80);
    discv5_node_t n2 = make_node(0x90);
    discv5_node_t n3 = make_node(0xA0);
    discv5_node_t n4 = make_node_at(31, 0x01);

    discv5_table_insert(&table, &n1);
    discv5_table_insert(&table, &n2);
    discv5_table_insert(&table, &n3);
    discv5_table_insert(&table, &n4);

    discv5_node_t out[16];

    /* Bucket 255 should have 3 nodes */
    size_t count = discv5_table_nodes_at_distance(&table, 255, out);
    ASSERT(count == 3, "3 at dist 255");

    /* Bucket 0 should have 1 node */
    count = discv5_table_nodes_at_distance(&table, 0, out);
    ASSERT(count == 1, "1 at dist 0");
    ASSERT(out[0].node_id[31] == 0x01, "correct node");

    /* Empty bucket */
    count = discv5_table_nodes_at_distance(&table, 128, out);
    ASSERT(count == 0, "empty bucket");

    /* Invalid distances */
    ASSERT(discv5_table_nodes_at_distance(&table, -1, out) == 0, "negative dist");
    ASSERT(discv5_table_nodes_at_distance(&table, 256, out) == 0, "dist 256");

    PASS();
}

/* =========================================================================
 * Test 9: Remove
 * ========================================================================= */

static void test_remove(void) {
    TEST("remove");

    discv5_table_t table;
    uint8_t local[32];
    memset(local, 0, 32);
    discv5_table_init(&table, local);

    discv5_node_t n1 = make_node(0x80);
    discv5_node_t n2 = make_node(0x81);
    discv5_table_insert(&table, &n1);
    discv5_table_insert(&table, &n2);

    ASSERT(discv5_table_size(&table) == 2, "size = 2");
    ASSERT(discv5_table_remove(&table, n1.node_id), "remove n1");
    ASSERT(discv5_table_size(&table) == 1, "size = 1");
    ASSERT(discv5_table_find(&table, n1.node_id) == NULL, "n1 gone");
    ASSERT(discv5_table_find(&table, n2.node_id) != NULL, "n2 present");

    /* Remove from replacement cache */
    for (int i = 0; i < 16; i++) {
        discv5_node_t n = make_node(0xA0 + i);
        discv5_table_insert(&table, &n);
    }
    /* Bucket 255 now has: n2 + 15 nodes = 16 (full) */
    /* Actually, n2 is 0x81 which is bucket 255 (differs in high bit of byte 0).
     * Let's add one more that goes to replacement */
    discv5_node_t repl = make_node(0xB0);
    discv5_table_insert(&table, &repl);  /* goes to replacement */

    discv5_bucket_t *b = &table.buckets[255];
    ASSERT(b->repl_count >= 1, "has replacement");
    ASSERT(discv5_table_remove(&table, repl.node_id), "remove from replacement");
    ASSERT(discv5_table_find(&table, repl.node_id) == NULL, "repl gone");

    /* Remove non-existent */
    uint8_t fake[32] = {0xFF, 0xFF};
    ASSERT(!discv5_table_remove(&table, fake), "remove non-existent fails");

    PASS();
}

/* =========================================================================
 * Test 10: Table size
 * ========================================================================= */

static void test_table_size(void) {
    TEST("table size");

    discv5_table_t table;
    uint8_t local[32];
    memset(local, 0, 32);
    discv5_table_init(&table, local);

    ASSERT(discv5_table_size(&table) == 0, "empty");

    /* Add nodes to different buckets */
    discv5_node_t n1 = make_node(0x80);           /* bucket 255 */
    discv5_node_t n2 = make_node_at(31, 0x01);    /* bucket 0 */
    discv5_node_t n3 = make_node_at(15, 0x01);    /* bucket 136 */

    discv5_table_insert(&table, &n1);
    discv5_table_insert(&table, &n2);
    discv5_table_insert(&table, &n3);

    ASSERT(discv5_table_size(&table) == 3, "3 across buckets");

    discv5_table_remove(&table, n2.node_id);
    ASSERT(discv5_table_size(&table) == 2, "2 after remove");

    PASS();
}

/* =========================================================================
 * Main
 * ========================================================================= */

int main(void) {
    printf("Discv5 Kademlia Table\n");
    printf("======================\n");

    test_bucket_index();
    test_insert_find();
    test_lru_ordering();
    test_replacement_cache();
    test_mark_dead();
    test_mark_alive();
    test_closest_nodes();
    test_nodes_at_distance();
    test_remove();
    test_table_size();

    printf("----------------------\n");
    printf("Results: %d passed, %d failed\n", tests_passed, tests_failed);

    return tests_failed > 0 ? 1 : 0;
}
