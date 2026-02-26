/*
 * Online File Compaction
 *
 * Relocates live nodes from tail pages to front pages, then truncates
 * the file.  Requires exclusive write_lock and no active MVCC snapshots.
 *
 * Algorithm:
 *   1. Drain pending frees (safe — no snapshots).
 *   2. Build live-page bitmap via DFS from root.
 *   3. Compute frontier = live_page_count.
 *      Pages [0, frontier) will hold all live data after compaction.
 *      Dead pages below frontier provide exactly enough space for
 *      relocating live pages above frontier.
 *   4. Populate the reuse pool with dead pages below frontier.
 *   5. DFS bottom-up: for every node on a page >= frontier,
 *      allocate a new slot/page below frontier (from reuse pool),
 *      copy the node, and update the parent's child reference in-place.
 *   6. Update root, set next_page_id = frontier, checkpoint, ftruncate.
 */

#include "data_art.h"
#include "mvcc.h"
#include "logger.h"

#include <stdlib.h>
#include <string.h>
#include <time.h>

/* Externs from data_art_core.c */
extern size_t get_node_size(data_art_node_type_t type);
extern void data_art_publish_root(data_art_tree_t *tree);
extern void drain_pending_frees(data_art_tree_t *tree);

/* ── Bitmap helpers ──────────────────────────────────────────────── */

static inline void bm_set(uint64_t *bm, uint64_t id) {
    bm[id / 64] |= (1ULL << (id % 64));
}

static inline bool bm_test(const uint64_t *bm, uint64_t id) {
    return (bm[id / 64] >> (id % 64)) & 1;
}

/* ── Phase 1: Build live-page bitmap + max-page-id per page ──────── */

/* Read-only walk — no writes, so direct mmap pointer reads are safe
 * (no aliasing concern).  Avoids costly stack memcpy of every node.
 *
 * Returns the highest page_id reachable from this subtree.  Stored in
 * max_pg[page_id] (per-page max, conservative for multi-node pages).
 * compact_node uses this to skip subtrees entirely below frontier. */

static uint64_t walk_live(data_art_tree_t *tree, node_ref_t ref,
                          uint64_t *bm, uint64_t *count,
                          uint64_t *max_pg) {
    if (node_ref_is_null(ref)) return 0;

    uint64_t pg = node_ref_page_id(ref);

    if (!bm_test(bm, pg)) {
        bm_set(bm, pg);
        (*count)++;
    }

    uint64_t subtree_max = pg;

    page_t *page = mmap_storage_get_page(tree->mmap_storage, pg);
    const void *node = page->data + node_ref_offset(ref);

    uint8_t type = *(const uint8_t *)node;

    switch (type) {
    case DATA_NODE_4: {
        const data_art_node4_t *n = node;
        for (int i = 0; i < n->num_children; i++) {
            uint64_t cm = walk_live(tree, n->children[i], bm, count, max_pg);
            if (cm > subtree_max) subtree_max = cm;
        }
        break;
    }
    case DATA_NODE_16: {
        const data_art_node16_t *n = node;
        for (int i = 0; i < n->num_children; i++) {
            uint64_t cm = walk_live(tree, n->children[i], bm, count, max_pg);
            if (cm > subtree_max) subtree_max = cm;
        }
        break;
    }
    case DATA_NODE_48: {
        const data_art_node48_t *n = node;
        for (int i = 0; i < 256; i++) {
            if (n->keys[i] != NODE48_EMPTY) {
                uint64_t cm = walk_live(tree, n->children[n->keys[i]], bm, count, max_pg);
                if (cm > subtree_max) subtree_max = cm;
            }
        }
        break;
    }
    case DATA_NODE_256: {
        const data_art_node256_t *n = node;
        for (int i = 0; i < 256; i++) {
            if (n->children[i] != NULL_NODE_REF) {
                uint64_t cm = walk_live(tree, n->children[i], bm, count, max_pg);
                if (cm > subtree_max) subtree_max = cm;
            }
        }
        break;
    }
    case DATA_NODE_LEAF: {
        const data_art_leaf_t *leaf = node;
        if (leaf->flags & LEAF_FLAG_OVERFLOW) {
            uint64_t op = leaf_overflow_page(leaf);
            while (op != 0) {
                if (op > subtree_max) subtree_max = op;
                if (!bm_test(bm, op)) {
                    bm_set(bm, op);
                    (*count)++;
                }
                page_t *p = mmap_storage_get_page(tree->mmap_storage, op);
                const data_art_overflow_t *ov = (const data_art_overflow_t *)p->data;
                op = ov->next_page;
            }
        }
        break;
    }
    }

    /* Store max for this page (conservative: take max with existing value
     * since multiple nodes may share a page via slot packing) */
    if (subtree_max > max_pg[pg])
        max_pg[pg] = subtree_max;

    return subtree_max;
}

/* ── Phase 2: Relocate overflow page chain ───────────────────────── */

static void compact_overflow(data_art_tree_t *tree, node_ref_t leaf_ref,
                             uint64_t frontier, uint64_t *relocated) {
    mmap_storage_t *ms = tree->mmap_storage;
    page_t *lp = mmap_storage_get_page(ms, node_ref_page_id(leaf_ref));
    const data_art_leaf_t *leaf = (const data_art_leaf_t *)(lp->data + node_ref_offset(leaf_ref));
    if (!(leaf->flags & LEAF_FLAG_OVERFLOW)) return;

    uint64_t prev_page = 0;   /* 0 = head stored in leaf */
    uint64_t curr = leaf_overflow_page(leaf);

    while (curr != 0) {
        page_t *cp = mmap_storage_get_page(tree->mmap_storage, curr);
        data_art_overflow_t ov;
        memcpy(&ov, cp->data, sizeof(ov));
        uint64_t next = ov.next_page;

        if (curr >= frontier) {
            /* Allocate new page below frontier (reuse pool) */
            node_ref_t new_ref = data_art_alloc_node(tree, sizeof(data_art_overflow_t));
            uint64_t new_pg = node_ref_page_id(new_ref);

            /* Copy overflow data */
            data_art_copy_node(tree, new_ref,
                               node_ref_make(curr, 0),
                               sizeof(data_art_overflow_t));

            /* Update link: prev → new_pg */
            if (prev_page == 0) {
                /* Update leaf's overflow_page_id (first 8 bytes of data[]) */
                data_art_write_partial(tree, leaf_ref,
                    offsetof(data_art_leaf_t, data), &new_pg, sizeof(uint64_t));
            } else {
                /* Update previous overflow page's next_page */
                data_art_write_partial(tree, node_ref_make(prev_page, 0),
                    offsetof(data_art_overflow_t, next_page),
                    &new_pg, sizeof(uint64_t));
            }

            (*relocated)++;
            prev_page = new_pg;
        } else {
            prev_page = curr;
        }

        curr = next;
    }
}

/* ── Phase 3: Recursive bottom-up node relocation ────────────────── */

/* max_pg[] (computed in walk_live) stores the highest page_id reachable
 * from any node on a given page.  If max_pg[page] < frontier, the entire
 * subtree rooted at that page is already below frontier — skip it. */

static node_ref_t compact_node(data_art_tree_t *tree, node_ref_t ref,
                                uint64_t frontier, uint64_t *relocated,
                                const uint64_t *max_pg) {
    if (node_ref_is_null(ref)) return ref;

    uint64_t pg = node_ref_page_id(ref);

    /* ── Subtree skip: max reachable page < frontier → nothing to do ── */
    if (pg < frontier && max_pg[pg] < frontier)
        return ref;

    mmap_storage_t *ms = tree->mmap_storage;
    page_t *page = mmap_storage_get_page(ms, pg);
    const void *node = page->data + node_ref_offset(ref);

    uint8_t type = *(const uint8_t *)node;

    /* ── Leaf ── */
    if (type == DATA_NODE_LEAF) {
        const data_art_leaf_t *leaf = node;

        if (pg < frontier) {
            /* Leaf below frontier: only overflow pages may need relocation */
            if (leaf->flags & LEAF_FLAG_OVERFLOW)
                compact_overflow(tree, ref, frontier, relocated);
            return ref;
        }

        /* Leaf above frontier: relocate overflow first, then leaf itself */
        if (leaf->flags & LEAF_FLAG_OVERFLOW)
            compact_overflow(tree, ref, frontier, relocated);

        /* Re-load: compact_overflow may have updated overflow_page_id */
        leaf = (const data_art_leaf_t *)(page->data + node_ref_offset(ref));
        size_t sz = leaf_total_size(leaf, tree->key_size);
        node_ref_t new_ref = data_art_alloc_node(tree, sz);
        if (node_ref_is_null(new_ref)) return ref;
        data_art_copy_node(tree, new_ref, ref, sz);
        (*relocated)++;
        return new_ref;
    }

    /* ── Inner node: recurse into children, batch-write updates ──
     *
     * We memcpy the node to the stack, recurse into each child updating
     * the stack copy in-place, then:
     *   - Below frontier: single write-back of the node if any child changed
     *   - Above frontier: write updated stack copy directly to new page
     * This replaces N separate write_partial + mark_dirty calls with one. */
    size_t node_size = get_node_size(type);
    bool dirty = false;

    switch (type) {
    case DATA_NODE_4: {
        data_art_node4_t n;
        memcpy(&n, node, sizeof(n));
        for (int i = 0; i < n.num_children; i++) {
            node_ref_t new_child = compact_node(tree, n.children[i], frontier, relocated, max_pg);
            if (!node_ref_equals(new_child, n.children[i])) {
                n.children[i] = new_child;
                dirty = true;
            }
        }
        if (pg >= frontier) {
            node_ref_t new_ref = data_art_alloc_node(tree, node_size);
            if (node_ref_is_null(new_ref)) return ref;
            page_t *dp = mmap_storage_get_page(ms, node_ref_page_id(new_ref));
            memcpy(dp->data + node_ref_offset(new_ref), &n, node_size);
            mmap_storage_mark_dirty(ms, node_ref_page_id(new_ref));
            (*relocated)++;
            return new_ref;
        }
        if (dirty) {
            memcpy(page->data + node_ref_offset(ref), &n, node_size);
            mmap_storage_mark_dirty(ms, pg);
        }
        break;
    }
    case DATA_NODE_16: {
        data_art_node16_t n;
        memcpy(&n, node, sizeof(n));
        for (int i = 0; i < n.num_children; i++) {
            node_ref_t new_child = compact_node(tree, n.children[i], frontier, relocated, max_pg);
            if (!node_ref_equals(new_child, n.children[i])) {
                n.children[i] = new_child;
                dirty = true;
            }
        }
        if (pg >= frontier) {
            node_ref_t new_ref = data_art_alloc_node(tree, node_size);
            if (node_ref_is_null(new_ref)) return ref;
            page_t *dp = mmap_storage_get_page(ms, node_ref_page_id(new_ref));
            memcpy(dp->data + node_ref_offset(new_ref), &n, node_size);
            mmap_storage_mark_dirty(ms, node_ref_page_id(new_ref));
            (*relocated)++;
            return new_ref;
        }
        if (dirty) {
            memcpy(page->data + node_ref_offset(ref), &n, node_size);
            mmap_storage_mark_dirty(ms, pg);
        }
        break;
    }
    case DATA_NODE_48: {
        data_art_node48_t n;
        memcpy(&n, node, sizeof(n));
        for (int i = 0; i < 256; i++) {
            if (n.keys[i] == NODE48_EMPTY) continue;
            uint8_t slot = n.keys[i];
            node_ref_t new_child = compact_node(tree, n.children[slot], frontier, relocated, max_pg);
            if (!node_ref_equals(new_child, n.children[slot])) {
                n.children[slot] = new_child;
                dirty = true;
            }
        }
        if (pg >= frontier) {
            node_ref_t new_ref = data_art_alloc_node(tree, node_size);
            if (node_ref_is_null(new_ref)) return ref;
            page_t *dp = mmap_storage_get_page(ms, node_ref_page_id(new_ref));
            memcpy(dp->data + node_ref_offset(new_ref), &n, node_size);
            mmap_storage_mark_dirty(ms, node_ref_page_id(new_ref));
            (*relocated)++;
            return new_ref;
        }
        if (dirty) {
            memcpy(page->data + node_ref_offset(ref), &n, node_size);
            mmap_storage_mark_dirty(ms, pg);
        }
        break;
    }
    case DATA_NODE_256: {
        data_art_node256_t n;
        memcpy(&n, node, sizeof(n));
        for (int i = 0; i < 256; i++) {
            if (n.children[i] == NULL_NODE_REF) continue;
            node_ref_t new_child = compact_node(tree, n.children[i], frontier, relocated, max_pg);
            if (!node_ref_equals(new_child, n.children[i])) {
                n.children[i] = new_child;
                dirty = true;
            }
        }
        if (pg >= frontier) {
            node_ref_t new_ref = data_art_alloc_node(tree, node_size);
            if (node_ref_is_null(new_ref)) return ref;
            page_t *dp = mmap_storage_get_page(ms, node_ref_page_id(new_ref));
            memcpy(dp->data + node_ref_offset(new_ref), &n, node_size);
            mmap_storage_mark_dirty(ms, node_ref_page_id(new_ref));
            (*relocated)++;
            return new_ref;
        }
        if (dirty) {
            memcpy(page->data + node_ref_offset(ref), &n, node_size);
            mmap_storage_mark_dirty(ms, pg);
        }
        break;
    }
    }

    return ref;
}

/* ── Public API ──────────────────────────────────────────────────── */

bool data_art_compact(data_art_tree_t *tree, data_art_compact_result_t *result) {
    if (!tree) return false;

    struct timespec t0;
    clock_gettime(CLOCK_MONOTONIC, &t0);

    /* ── Acquire exclusive write access ── */
    pthread_rwlock_wrlock(&tree->write_lock);

    /* Require no active snapshots */
    if (tree->mvcc_manager && mvcc_has_active_snapshots(tree->mvcc_manager)) {
        pthread_rwlock_unlock(&tree->write_lock);
        LOG_ERROR("data_art_compact: active snapshots exist — cannot compact");
        return false;
    }

    mmap_storage_t *ms = tree->mmap_storage;
    uint64_t old_pages = ms->next_page_id;

    /* ── Handle empty tree ── */
    if (node_ref_is_null(tree->root)) {
        pthread_rwlock_unlock(&tree->write_lock);
        if (result) {
            memset(result, 0, sizeof(*result));
            result->pages_before = old_pages;
            result->pages_after = old_pages;
        }
        return true;
    }

    /* ── Step 0: Drain pending frees into reuse pool ── */
    drain_pending_frees(tree);

    /* ── Step 1: Build live-page bitmap + max-page-id array ── */
    size_t bm_words = (old_pages + 63) / 64;
    uint64_t *live_bm = calloc(bm_words, sizeof(uint64_t));
    uint64_t *max_pg = calloc(old_pages, sizeof(uint64_t));
    if (!live_bm || !max_pg) {
        free(live_bm);
        free(max_pg);
        pthread_rwlock_unlock(&tree->write_lock);
        return false;
    }

    /* Page 0 (header) is always live */
    bm_set(live_bm, 0);
    uint64_t live_count = 1;

    walk_live(tree, tree->root, live_bm, &live_count, max_pg);

    /* ── Step 2: Compute frontier ──
     * frontier = live_count  →  pages [0, frontier) hold all live data.
     * Dead pages below frontier = frontier - live_below = live_above.
     * Exactly enough holes for relocation. */
    uint64_t frontier = live_count;

    if (frontier >= old_pages) {
        /* No dead pages — nothing to compact */
        free(live_bm);
        free(max_pg);
        pthread_rwlock_unlock(&tree->write_lock);
        if (result) {
            memset(result, 0, sizeof(*result));
            result->pages_before = old_pages;
            result->pages_after = old_pages;
            result->live_pages = live_count;
        }
        return true;
    }

    LOG_INFO("compact: %lu total pages, %lu live, frontier=%lu, reclaimable=%lu",
             old_pages, live_count, frontier, old_pages - frontier);

    /* ── Step 3: Rebuild reuse pool with dead pages below frontier ── */
    tree->reuse_pool_count = 0;

    /* Count dead pages below frontier for capacity pre-alloc */
    uint64_t dead_below = 0;
    for (uint64_t pg = 1; pg < frontier; pg++) {
        if (!bm_test(live_bm, pg))
            dead_below++;
    }

    /* Ensure reuse pool capacity */
    if (dead_below > tree->reuse_pool_capacity) {
        uint64_t *new_pool = realloc(tree->reuse_pool, dead_below * sizeof(uint64_t));
        if (!new_pool) {
            free(live_bm);
            free(max_pg);
            pthread_rwlock_unlock(&tree->write_lock);
            return false;
        }
        tree->reuse_pool = new_pool;
        tree->reuse_pool_capacity = dead_below;
    }

    /* Populate: push dead pages below frontier (reverse order so low IDs come first) */
    for (uint64_t pg = frontier; pg-- > 1; ) {
        if (!bm_test(live_bm, pg)) {
            tree->reuse_pool[tree->reuse_pool_count++] = pg;
        }
    }

    free(live_bm);

    /* Reset slot allocator current pages (force fresh allocation from reuse pool) */
    for (int i = 0; i < NUM_SLOT_CLASSES; i++) {
        tree->slot_classes[i].current_page_id = 0;
    }

    /* ── Step 4: DFS bottom-up relocation ── */
    uint64_t nodes_relocated = 0;
    node_ref_t new_root = compact_node(tree, tree->root, frontier, &nodes_relocated, max_pg);

    free(max_pg);

    /* Update tree root */
    tree->root = new_root;
    data_art_publish_root(tree);

    /* ── Step 5: Reset allocator state ── */
    tree->reuse_pool_count = 0;
    tree->pending_free_count = 0;
    tree->pending_slot_free_count = 0;

    for (int i = 0; i < NUM_SLOT_CLASSES; i++) {
        /* Keep current_page_id if it's below frontier; clear otherwise */
        if (tree->slot_classes[i].current_page_id >= frontier)
            tree->slot_classes[i].current_page_id = 0;
    }

    /* ── Step 6: Checkpoint with new root, then truncate ── */
    ms->next_page_id = frontier;

    mmap_storage_checkpoint(ms,
                             node_ref_page_id(tree->root),
                             node_ref_offset(tree->root),
                             tree->size, tree->key_size);

    mmap_storage_truncate(ms, frontier);

    pthread_rwlock_unlock(&tree->write_lock);

    /* ── Stats ── */
    struct timespec t1;
    clock_gettime(CLOCK_MONOTONIC, &t1);
    double elapsed = (t1.tv_sec - t0.tv_sec) + (t1.tv_nsec - t0.tv_nsec) / 1e9;

    LOG_INFO("compact: %lu → %lu pages (freed %lu, relocated %lu nodes, %.1fms)",
             old_pages, frontier, old_pages - frontier, nodes_relocated,
             elapsed * 1000.0);

    if (result) {
        result->pages_before = old_pages;
        result->pages_after = frontier;
        result->pages_freed = old_pages - frontier;
        result->nodes_relocated = nodes_relocated;
        result->live_pages = live_count;
    }

    return true;
}
