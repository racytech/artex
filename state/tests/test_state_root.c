/**
 * State root test — reproduces exact test_runner flow:
 *   setup_state → commit → compute_root → clear_prestate_dirty → modify → commit_tx → compute_root
 */
#include "state.h"
#include "evm_state.h"
#include "keccak256.h"
#include <stdio.h>
#include <string.h>

static void print_hash(const char *label, const uint8_t *h) {
    printf("  %s: ", label);
    for (int i = 0; i < 32; i++) printf("%02x", h[i]);
    printf("\n");
}

int main(void) {
    evm_state_t *es = evm_state_create(NULL);
    if (!es) { fprintf(stderr, "FAIL: evm_state_create\n"); return 1; }

    address_t a1, a2;
    memcpy(a1.bytes, (uint8_t[]){0x12,0x34,0x56,0x78,0x90,0xab,0xcd,0xef,
                                  0x12,0x34,0x56,0x78,0x90,0xab,0xcd,0xef,
                                  0x12,0x34,0x56,0x78}, 20);
    memcpy(a2.bytes, (uint8_t[]){0xaa,0xbb,0xcc,0xdd,0xee,0xff,0x00,0x11,
                                  0x22,0x33,0x44,0x55,0x66,0x77,0x88,0x99,
                                  0xaa,0xbb,0xcc,0xdd}, 20);

    /* === Reproduce test_runner_state.c flow exactly === */

    /* 1. Setup pre-state: a1 nonce=1 */
    printf("Step 1: setup prestate (a1 nonce=1)\n");
    evm_state_set_nonce(es, &a1, 1);
    evm_state_mark_existed(es, &a1);

    /* 2. Commit pre-state */
    printf("Step 2: evm_state_commit\n");
    evm_state_commit(es);

    /* 3. Compute genesis root */
    printf("Step 3: compute genesis root\n");
    hash_t genesis = evm_state_compute_mpt_root(es, false);
    print_hash("genesis", genesis.bytes);

    /* 4. Clear prestate dirty */
    printf("Step 4: clear_prestate_dirty\n");
    evm_state_clear_prestate_dirty(es);

    /* 5. Modify: a1 nonce=2, add a2 balance=1000 */
    printf("Step 5: modify (a1 nonce=2, a2 bal=1000)\n");
    evm_state_set_nonce(es, &a1, 2);
    uint256_t bal = uint256_from_uint64(1000);
    evm_state_set_balance(es, &a2, &bal);

    /* 6. Commit tx */
    printf("Step 6: commit_tx\n");
    evm_state_commit_tx(es);

    /* 7. Compute post-tx root */
    printf("Step 7: compute post-tx root\n");
    hash_t post = evm_state_compute_mpt_root(es, false);
    print_hash("post-tx", post.bytes);

    /* Expected: r3 from Python = 2 accounts (a1 n=2, a2 bal=1000) */
    printf("\n  expected: 95ebc01bf138e315ce28fbf29cbbaca1c10b832ac3a3cc8d36fefd5969304923\n");
    printf("  match:    %s\n",
        memcmp(post.bytes, (uint8_t[]){
            0x95,0xeb,0xc0,0x1b,0xf1,0x38,0xe3,0x15,
            0xce,0x28,0xfb,0xf2,0x9c,0xbb,0xac,0xa1,
            0xc1,0x0b,0x83,0x2a,0xc3,0xa3,0xcc,0x8d,
            0x36,0xfe,0xfd,0x59,0x69,0x30,0x49,0x23
        }, 32) == 0 ? "YES" : "NO");

    evm_state_destroy(es);
    return 0;
}
