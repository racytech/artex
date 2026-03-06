/*
 * Test: Discv5 Session Management
 *
 * Tests:
 *  1. ID nonce verify — verify signature from spec test vector
 *  2. ID nonce roundtrip — sign then verify
 *  3. Cache basic ops — create, find, remove
 *  4. Cache LRU eviction — evicts least recently used
 *  5. Session lifecycle — NONE → WHOAREYOU_SENT → ESTABLISHED
 *  6. Encrypt/decrypt key direction
 */

#include "../include/discv5_session.h"
#include "../include/discv5_codec.h"
#include "../include/secp256k1_wrap.h"
#include <stdio.h>
#include <string.h>

static int tests_passed = 0;
static int tests_failed = 0;

#define TEST(name) do { printf("  [%s] ", name); } while(0)
#define PASS() do { printf("PASS\n"); tests_passed++; return; } while(0)
#define FAIL(msg) do { printf("FAIL: %s\n", msg); tests_failed++; return; } while(0)
#define ASSERT(cond, msg) do { if (!(cond)) FAIL(msg); } while(0)

static void hex_to_bytes(const char *hex, uint8_t *out, size_t len) {
    for (size_t i = 0; i < len; i++) {
        unsigned int b;
        sscanf(hex + 2 * i, "%02x", &b);
        out[i] = (uint8_t)b;
    }
}

static const char *NODE_A_KEY = "eef77acb6c6a6eebc5b363a475ac583ec7eccdb42b6481424c60f59aa326547f";
static const char *NODE_A_ID  = "aaaa8419e9f49d0083561b48287df592939a8d19947d8c0ef88f2a4856a69fbb";
static const char *NODE_B_ID  = "bbbb9d047f0488c0b5a93c1c3f2d8bafc7c8ff337024a55434a0d0555de64db9";

/* =========================================================================
 * Test 1: ID nonce verify — spec test vector
 * ========================================================================= */

static void test_id_nonce_verify(void) {
    TEST("ID nonce verify");

    /* Sign with Node A's key (reusing the codec test vector) */
    uint8_t static_key[32];
    hex_to_bytes("fb757dc581730490a1d7a00deea65e9b1936924caaea8f44d476014856b68736", static_key, 32);

    uint8_t challenge_data[63];
    hex_to_bytes("000000000000000000000000000000006469736376350001010102030405060708090a0b0c00180102030405060708090a0b0c0d0e0f100000000000000000", challenge_data, 63);

    uint8_t eph_pubkey[33];
    hex_to_bytes("039961e4c2356d61bedb83052c115d311acb3a96f5777296dcf297351130266231", eph_pubkey, 33);

    uint8_t node_id_b[32];
    hex_to_bytes(NODE_B_ID, node_id_b, 32);

    /* Sign */
    uint8_t sig[64];
    ASSERT(discv5_sign_id_nonce(sig, challenge_data, 63, eph_pubkey,
                                 node_id_b, static_key), "sign");

    /* Derive the signer's public key */
    uint8_t signer_pub[64];
    ASSERT(secp256k1_wrap_pubkey_create(signer_pub, static_key), "pubkey");

    /* Verify */
    ASSERT(discv5_verify_id_nonce(sig, challenge_data, 63, eph_pubkey,
                                   node_id_b, signer_pub), "verify");

    PASS();
}

/* =========================================================================
 * Test 2: ID nonce roundtrip — sign then verify, tamper fails
 * ========================================================================= */

static void test_id_nonce_roundtrip(void) {
    TEST("ID nonce roundtrip");

    uint8_t priv[32];
    hex_to_bytes(NODE_A_KEY, priv, 32);

    uint8_t pub[64];
    ASSERT(secp256k1_wrap_pubkey_create(pub, priv), "pubkey");

    uint8_t challenge[63];
    memset(challenge, 0xAB, 63);

    uint8_t eph[33];
    memset(eph, 0xCD, 33);

    uint8_t nid[32];
    hex_to_bytes(NODE_B_ID, nid, 32);

    uint8_t sig[64];
    ASSERT(discv5_sign_id_nonce(sig, challenge, 63, eph, nid, priv), "sign");
    ASSERT(discv5_verify_id_nonce(sig, challenge, 63, eph, nid, pub), "verify");

    /* Tamper with sig — should fail */
    sig[0] ^= 0xFF;
    ASSERT(!discv5_verify_id_nonce(sig, challenge, 63, eph, nid, pub), "tampered sig fails");

    PASS();
}

/* =========================================================================
 * Test 3: Cache basic operations
 * ========================================================================= */

static void test_cache_basic(void) {
    TEST("cache basic ops");

    discv5_session_cache_t cache;
    ASSERT(discv5_session_cache_init(&cache, 16), "init");

    uint8_t id_a[32], id_b[32];
    hex_to_bytes(NODE_A_ID, id_a, 32);
    hex_to_bytes(NODE_B_ID, id_b, 32);

    /* Find should return NULL for unknown node */
    ASSERT(discv5_session_find(&cache, id_a) == NULL, "not found initially");

    /* Create session */
    discv5_session_t *sa = discv5_session_get_or_create(&cache, id_a);
    ASSERT(sa != NULL, "create A");
    ASSERT(memcmp(sa->node_id, id_a, 32) == 0, "A node_id");
    ASSERT(sa->state == DISCV5_SESS_NONE, "A state = NONE");
    ASSERT(cache.count == 1, "count = 1");

    /* Find should now succeed */
    discv5_session_t *found = discv5_session_find(&cache, id_a);
    ASSERT(found == sa, "find A");

    /* Create second session */
    discv5_session_t *sb = discv5_session_get_or_create(&cache, id_b);
    ASSERT(sb != NULL, "create B");
    ASSERT(cache.count == 2, "count = 2");

    /* Get-or-create for existing returns same */
    discv5_session_t *sa2 = discv5_session_get_or_create(&cache, id_a);
    ASSERT(sa2 == sa, "get-or-create returns existing");
    ASSERT(cache.count == 2, "count still 2");

    /* Remove */
    ASSERT(discv5_session_remove(&cache, id_a), "remove A");
    ASSERT(cache.count == 1, "count = 1 after remove");
    ASSERT(discv5_session_find(&cache, id_a) == NULL, "A removed");
    ASSERT(discv5_session_find(&cache, id_b) != NULL, "B still present");

    /* Remove non-existent */
    ASSERT(!discv5_session_remove(&cache, id_a), "remove non-existent fails");

    discv5_session_cache_destroy(&cache);
    PASS();
}

/* =========================================================================
 * Test 4: Cache LRU eviction
 * ========================================================================= */

static void test_cache_lru(void) {
    TEST("cache LRU eviction");

    discv5_session_cache_t cache;
    ASSERT(discv5_session_cache_init(&cache, 4), "init cap=4");

    /* Fill cache with 4 nodes */
    uint8_t ids[5][32];
    for (int i = 0; i < 5; i++) {
        memset(ids[i], 0, 32);
        ids[i][0] = (uint8_t)(i + 1);
    }

    for (int i = 0; i < 4; i++) {
        discv5_session_t *s = discv5_session_get_or_create(&cache, ids[i]);
        ASSERT(s != NULL, "create");
    }
    ASSERT(cache.count == 4, "full");

    /* Touch node 0 (oldest) to make it recently used */
    discv5_session_find(&cache, ids[0]);

    /* Add node 4 — should evict node 1 (now the LRU) */
    discv5_session_t *s4 = discv5_session_get_or_create(&cache, ids[4]);
    ASSERT(s4 != NULL, "create 5th");
    ASSERT(cache.count == 4, "still cap=4");

    /* Node 1 should be evicted, others present */
    ASSERT(discv5_session_find(&cache, ids[0]) != NULL, "node 0 present");
    ASSERT(discv5_session_find(&cache, ids[1]) == NULL, "node 1 evicted");
    ASSERT(discv5_session_find(&cache, ids[2]) != NULL, "node 2 present");
    ASSERT(discv5_session_find(&cache, ids[3]) != NULL, "node 3 present");
    ASSERT(discv5_session_find(&cache, ids[4]) != NULL, "node 4 present");

    discv5_session_cache_destroy(&cache);
    PASS();
}

/* =========================================================================
 * Test 5: Session lifecycle
 * ========================================================================= */

static void test_session_lifecycle(void) {
    TEST("session lifecycle");

    discv5_session_cache_t cache;
    ASSERT(discv5_session_cache_init(&cache, 16), "init");

    uint8_t id[32];
    hex_to_bytes(NODE_A_ID, id, 32);

    discv5_session_t *s = discv5_session_get_or_create(&cache, id);
    ASSERT(s->state == DISCV5_SESS_NONE, "initial state = NONE");

    /* Transition to WHOAREYOU_SENT */
    uint8_t cd[63];
    memset(cd, 0x42, 63);
    discv5_session_set_whoareyou(s, cd, 63);
    ASSERT(s->state == DISCV5_SESS_WHOAREYOU_SENT, "state = WHOAREYOU_SENT");
    ASSERT(s->challenge_data_len == 63, "challenge_data_len");
    ASSERT(memcmp(s->challenge_data, cd, 63) == 0, "challenge_data stored");

    /* Transition to ESTABLISHED */
    discv5_keys_t keys;
    memset(keys.initiator_key, 0xAA, 16);
    memset(keys.recipient_key, 0xBB, 16);
    discv5_session_establish(s, &keys, false);
    ASSERT(s->state == DISCV5_SESS_ESTABLISHED, "state = ESTABLISHED");
    ASSERT(!s->is_initiator, "is_initiator = false");
    ASSERT(memcmp(s->keys.initiator_key, keys.initiator_key, 16) == 0, "initiator_key stored");
    ASSERT(memcmp(s->keys.recipient_key, keys.recipient_key, 16) == 0, "recipient_key stored");

    discv5_session_cache_destroy(&cache);
    PASS();
}

/* =========================================================================
 * Test 6: Encrypt/decrypt key direction
 * ========================================================================= */

static void test_key_direction(void) {
    TEST("key direction");

    discv5_session_t s;
    memset(&s, 0, sizeof(s));
    memset(s.keys.initiator_key, 0xAA, 16);
    memset(s.keys.recipient_key, 0xBB, 16);

    /* As initiator: encrypt with initiator_key, decrypt with recipient_key */
    s.is_initiator = true;
    ASSERT(discv5_session_encrypt_key(&s) == s.keys.initiator_key, "initiator encrypts with init_key");
    ASSERT(discv5_session_decrypt_key(&s) == s.keys.recipient_key, "initiator decrypts with recv_key");

    /* As recipient: encrypt with recipient_key, decrypt with initiator_key */
    s.is_initiator = false;
    ASSERT(discv5_session_encrypt_key(&s) == s.keys.recipient_key, "recipient encrypts with recv_key");
    ASSERT(discv5_session_decrypt_key(&s) == s.keys.initiator_key, "recipient decrypts with init_key");

    PASS();
}

/* =========================================================================
 * Main
 * ========================================================================= */

int main(void) {
    secp256k1_wrap_init();

    printf("Discv5 Session Management\n");
    printf("==========================\n");

    test_id_nonce_verify();
    test_id_nonce_roundtrip();
    test_cache_basic();
    test_cache_lru();
    test_session_lifecycle();
    test_key_direction();

    printf("--------------------------\n");
    printf("Results: %d passed, %d failed\n", tests_passed, tests_failed);

    secp256k1_wrap_destroy();
    return tests_failed > 0 ? 1 : 0;
}
