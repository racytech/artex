#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include "mem_art.h"

int main() {
    printf("Testing prefix scenarios...\n");
    
    art_tree_t tree;
    if (!art_tree_init(&tree)) {
        fprintf(stderr, "Failed to initialize tree\n");
        return 1;
    }
    
    // Test case 1: Insert longer key first, then prefix
    printf("\n1. Insert longer key, then its prefix...\n");
    uint8_t key1[] = {0xAA, 0xBB, 0xCC};
    uint8_t key2[] = {0xAA, 0xBB};
    uint64_t val1 = 111;
    uint64_t val2 = 222;
    
    if (!art_insert(&tree, key1, 3, &val1, sizeof(val1))) {
        fprintf(stderr, "  Failed to insert key1\n");
        return 1;
    }
    printf("  Inserted [AA BB CC] = 111\n");
    
    if (!art_insert(&tree, key2, 2, &val2, sizeof(val2))) {
        fprintf(stderr, "  Failed to insert key2\n");
        return 1;
    }
    printf("  Inserted [AA BB] = 222\n");
    
    // Verify both
    size_t vlen;
    const void *v = art_get(&tree, key1, 3, &vlen);
    if (!v || *(uint64_t*)v != 111) {
        fprintf(stderr, "  Failed to retrieve [AA BB CC]\n");
        return 1;
    }
    printf("  ✓ Retrieved [AA BB CC] = 111\n");
    
    v = art_get(&tree, key2, 2, &vlen);
    if (!v || *(uint64_t*)v != 222) {
        fprintf(stderr, "  Failed to retrieve [AA BB]\n");
        if (!v) fprintf(stderr, "    (key not found)\n");
        else fprintf(stderr, "    (wrong value: %lu)\n", *(uint64_t*)v);
        return 1;
    }
    printf("  ✓ Retrieved [AA BB] = 222\n");
    
    // Test case 2: Insert prefix first, then longer key
    printf("\n2. Insert prefix, then longer key...\n");
    uint8_t key3[] = {0xDD};
    uint8_t key4[] = {0xDD, 0xEE, 0xFF};
    uint64_t val3 = 333;
    uint64_t val4 = 444;
    
    if (!art_insert(&tree, key3, 1, &val3, sizeof(val3))) {
        fprintf(stderr, "  Failed to insert key3\n");
        return 1;
    }
    printf("  Inserted [DD] = 333\n");
    
    if (!art_insert(&tree, key4, 3, &val4, sizeof(val4))) {
        fprintf(stderr, "  Failed to insert key4\n");
        return 1;
    }
    printf("  Inserted [DD EE FF] = 444\n");
    
    v = art_get(&tree, key3, 1, &vlen);
    if (!v || *(uint64_t*)v != 333) {
        fprintf(stderr, "  Failed to retrieve [DD]\n");
        if (!v) fprintf(stderr, "    (key not found)\n");
        else fprintf(stderr, "    (wrong value: %lu)\n", *(uint64_t*)v);
        return 1;
    }
    printf("  ✓ Retrieved [DD] = 333\n");
    
    v = art_get(&tree, key4, 3, &vlen);
    if (!v || *(uint64_t*)v != 444) {
        fprintf(stderr, "  Failed to retrieve [DD EE FF]\n");
        return 1;
    }
    printf("  ✓ Retrieved [DD EE FF] = 444\n");
    
    // Test case 3: Three keys where one is prefix of another
    printf("\n3. Three keys with prefix relationship...\n");
    uint8_t key5[] = {0x11, 0x22};
    uint8_t key6[] = {0x11, 0x22, 0x33};
    uint8_t key7[] = {0x11, 0x22, 0x33, 0x44};
    uint64_t val5 = 555;
    uint64_t val6 = 666;
    uint64_t val7 = 777;
    
    if (!art_insert(&tree, key6, 3, &val6, sizeof(val6))) {
        fprintf(stderr, "  Failed to insert key6\n");
        return 1;
    }
    printf("  Inserted [11 22 33] = 666\n");
    
    if (!art_insert(&tree, key5, 2, &val5, sizeof(val5))) {
        fprintf(stderr, "  Failed to insert key5\n");
        return 1;
    }
    printf("  Inserted [11 22] = 555\n");
    
    if (!art_insert(&tree, key7, 4, &val7, sizeof(val7))) {
        fprintf(stderr, "  Failed to insert key7\n");
        return 1;
    }
    printf("  Inserted [11 22 33 44] = 777\n");
    
    v = art_get(&tree, key5, 2, &vlen);
    if (!v || *(uint64_t*)v != 555) {
        fprintf(stderr, "  Failed to retrieve [11 22]\n");
        if (!v) fprintf(stderr, "    (key not found)\n");
        else fprintf(stderr, "    (wrong value: %lu)\n", *(uint64_t*)v);
        return 1;
    }
    printf("  ✓ Retrieved [11 22] = 555\n");
    
    v = art_get(&tree, key6, 3, &vlen);
    if (!v || *(uint64_t*)v != 666) {
        fprintf(stderr, "  Failed to retrieve [11 22 33]\n");
        if (!v) fprintf(stderr, "    (key not found)\n");
        else fprintf(stderr, "    (wrong value: %lu)\n", *(uint64_t*)v);
        return 1;
    }
    printf("  ✓ Retrieved [11 22 33] = 666\n");
    
    v = art_get(&tree, key7, 4, &vlen);
    if (!v || *(uint64_t*)v != 777) {
        fprintf(stderr, "  Failed to retrieve [11 22 33 44]\n");
        return 1;
    }
    printf("  ✓ Retrieved [11 22 33 44] = 777\n");
    
    printf("\n✓ All prefix tests passed!\n");
    
    art_tree_destroy(&tree);
    return 0;
}
