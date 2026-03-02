/**
 * EOF Osaka Test Runner
 *
 * Runs official Ethereum EOF validation test vectors from:
 *   tests/osaka-eof/eof_tests/osaka/eip7692_eof_v1/
 *
 * Format: JSON files with hex-encoded EOF containers and expected
 * validation results (valid/invalid with exception type).
 *
 * Usage:
 *   test_eof_osaka [options] [path]
 *     path    Directory or file (default: ../vm/tests/fixtures/eof_tests)
 *     -v      Verbose: print each failing vector
 *     -s      Stop on first failure
 */

#include "eof.h"
#include <cjson/cJSON.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <dirent.h>
#include <sys/stat.h>

//==============================================================================
// Hex Decode
//==============================================================================

static inline uint8_t nibble(char c)
{
    if (c >= '0' && c <= '9') return (uint8_t)(c - '0');
    if (c >= 'a' && c <= 'f') return (uint8_t)(c - 'a' + 10);
    if (c >= 'A' && c <= 'F') return (uint8_t)(c - 'A' + 10);
    return 0;
}

static uint8_t *hex_decode(const char *hex, size_t *out_len)
{
    if (!hex) { *out_len = 0; return NULL; }
    if (hex[0] == '0' && (hex[1] == 'x' || hex[1] == 'X'))
        hex += 2;

    size_t slen = strlen(hex);
    if (slen == 0) { *out_len = 0; return NULL; }

    size_t blen = slen / 2;
    uint8_t *buf = (uint8_t *)malloc(blen);
    if (!buf) { *out_len = 0; return NULL; }

    for (size_t i = 0; i < blen; i++)
        buf[i] = (nibble(hex[2 * i]) << 4) | nibble(hex[2 * i + 1]);

    *out_len = blen;
    return buf;
}

//==============================================================================
// File I/O
//==============================================================================

static char *read_file(const char *path, size_t *out_len)
{
    FILE *f = fopen(path, "rb");
    if (!f) return NULL;

    fseek(f, 0, SEEK_END);
    long len = ftell(f);
    fseek(f, 0, SEEK_SET);

    if (len <= 0) { fclose(f); return NULL; }

    char *buf = (char *)malloc((size_t)len + 1);
    if (!buf) { fclose(f); return NULL; }

    size_t read = fread(buf, 1, (size_t)len, f);
    fclose(f);

    buf[read] = '\0';
    if (out_len) *out_len = read;
    return buf;
}

//==============================================================================
// Directory Recursion
//==============================================================================

#define MAX_FILES 2048

typedef struct {
    char **paths;
    int    count;
    int    capacity;
} file_list_t;

static void file_list_init(file_list_t *fl)
{
    fl->capacity = 256;
    fl->count = 0;
    fl->paths = (char **)malloc(fl->capacity * sizeof(char *));
}

static void file_list_add(file_list_t *fl, const char *path)
{
    if (fl->count >= fl->capacity) {
        fl->capacity *= 2;
        fl->paths = (char **)realloc(fl->paths, fl->capacity * sizeof(char *));
    }
    fl->paths[fl->count++] = strdup(path);
}

static void file_list_free(file_list_t *fl)
{
    for (int i = 0; i < fl->count; i++)
        free(fl->paths[i]);
    free(fl->paths);
}

static int cmp_str(const void *a, const void *b)
{
    return strcmp(*(const char **)a, *(const char **)b);
}

static void find_json_files(const char *dir, file_list_t *fl)
{
    DIR *d = opendir(dir);
    if (!d) return;

    struct dirent *ent;
    while ((ent = readdir(d)) != NULL) {
        if (ent->d_name[0] == '.') continue;

        char path[4096];
        snprintf(path, sizeof(path), "%s/%s", dir, ent->d_name);

        struct stat st;
        if (stat(path, &st) != 0) continue;

        if (S_ISDIR(st.st_mode)) {
            find_json_files(path, fl);
        } else if (S_ISREG(st.st_mode)) {
            size_t nlen = strlen(ent->d_name);
            if (nlen > 5 && strcmp(ent->d_name + nlen - 5, ".json") == 0) {
                file_list_add(fl, path);
            }
        }
    }
    closedir(d);
}

//==============================================================================
// Stats
//==============================================================================

typedef struct {
    int total;
    int passed;
    int failed;
    int errors;  // JSON parse errors, etc.
} stats_t;

//==============================================================================
// Extract EIP directory name from path for per-category reporting
//==============================================================================

static const char *extract_eip_dir(const char *path)
{
    // Look for "eip" in the path, return the eip directory component
    const char *p = path;
    const char *last_eip = NULL;
    while ((p = strstr(p, "eip")) != NULL) {
        // Find the start of this path component
        const char *start = p;
        while (start > path && *(start - 1) != '/') start--;
        last_eip = start;
        p++;
    }
    if (!last_eip) return "unknown";

    // Find the end of this component
    static char buf[128];
    const char *end = strchr(last_eip, '/');
    if (!end) end = last_eip + strlen(last_eip);
    size_t len = (size_t)(end - last_eip);
    if (len >= sizeof(buf)) len = sizeof(buf) - 1;
    memcpy(buf, last_eip, len);
    buf[len] = '\0';
    return buf;
}

//==============================================================================
// Per-Category Stats
//==============================================================================

#define MAX_CATEGORIES 32

typedef struct {
    char name[128];
    stats_t stats;
} category_t;

static category_t categories[MAX_CATEGORIES];
static int num_categories = 0;

static stats_t *get_category(const char *name)
{
    for (int i = 0; i < num_categories; i++) {
        if (strcmp(categories[i].name, name) == 0)
            return &categories[i].stats;
    }
    if (num_categories < MAX_CATEGORIES) {
        strncpy(categories[num_categories].name, name,
                sizeof(categories[num_categories].name) - 1);
        memset(&categories[num_categories].stats, 0, sizeof(stats_t));
        return &categories[num_categories++].stats;
    }
    return NULL;
}

//==============================================================================
// Process One JSON File
//==============================================================================

static void process_file(const char *path, stats_t *global, bool verbose,
                         bool stop_on_fail, bool *stopped)
{
    if (*stopped) return;

    size_t file_len;
    char *content = read_file(path, &file_len);
    if (!content) {
        if (verbose)
            fprintf(stderr, "  ERROR: cannot read %s\n", path);
        global->errors++;
        return;
    }

    cJSON *root = cJSON_Parse(content);
    free(content);
    if (!root) {
        if (verbose)
            fprintf(stderr, "  ERROR: JSON parse failed: %s\n", path);
        global->errors++;
        return;
    }

    const char *eip_dir = extract_eip_dir(path);
    stats_t *cat = get_category(eip_dir);

    // Iterate test cases (top-level keys)
    cJSON *test_case = NULL;
    cJSON_ArrayForEach(test_case, root)
    {
        if (*stopped) break;

        const char *test_name = test_case->string;

        // Skip _info at test level (shouldn't exist at top level, but just in case)
        if (test_name && strcmp(test_name, "_info") == 0) continue;

        cJSON *vectors = cJSON_GetObjectItemCaseSensitive(test_case, "vectors");
        if (!vectors) continue;

        // Iterate vectors
        cJSON *vector = NULL;
        cJSON_ArrayForEach(vector, vectors)
        {
            if (*stopped) break;

            cJSON *code_json = cJSON_GetObjectItemCaseSensitive(vector, "code");
            if (!code_json || !cJSON_IsString(code_json)) continue;

            cJSON *results = cJSON_GetObjectItemCaseSensitive(vector, "results");
            if (!results) continue;

            cJSON *osaka = cJSON_GetObjectItemCaseSensitive(results, "Osaka");
            if (!osaka) continue;

            cJSON *result_json = cJSON_GetObjectItemCaseSensitive(osaka, "result");
            if (!result_json) continue;

            bool expected_valid = cJSON_IsTrue(result_json);
            cJSON *exception_json = cJSON_GetObjectItemCaseSensitive(osaka, "exception");
            const char *expected_exception = (exception_json && cJSON_IsString(exception_json))
                                              ? exception_json->valuestring : NULL;

            // Read containerKind
            cJSON *kind_json = cJSON_GetObjectItemCaseSensitive(vector, "containerKind");
            int container_kind = EOF_CONTAINER_RUNTIME;
            if (kind_json && cJSON_IsString(kind_json) &&
                strcmp(kind_json->valuestring, "INITCODE") == 0) {
                container_kind = EOF_CONTAINER_INITCODE;
            }

            // Decode hex
            size_t code_len;
            uint8_t *code_bytes = hex_decode(code_json->valuestring, &code_len);
            if (!code_bytes && code_len > 0) {
                global->errors++;
                if (cat) cat->errors++;
                continue;
            }

            // Validate with container kind
            eof_container_t *container = NULL;
            eof_validation_error_t err = EOF_INVALID_HEADER;
            if (code_bytes) {
                err = eof_validate_kind(code_bytes, code_len,
                                         container_kind, &container);
            }

            bool actual_valid = (err == EOF_VALID);
            bool pass = (actual_valid == expected_valid);

            global->total++;
            if (cat) cat->total++;

            if (pass) {
                global->passed++;
                if (cat) cat->passed++;
            } else {
                global->failed++;
                if (cat) cat->failed++;

                if (verbose) {
                    // Extract short test name
                    const char *short_name = test_name;
                    const char *last_colon = strrchr(test_name, ':');
                    if (last_colon && last_colon > test_name + 1)
                        short_name = last_colon + 1;

                    if (expected_valid) {
                        fprintf(stderr, "  FAIL: %s — expected VALID, got %s\n",
                                short_name, eof_error_string(err));
                    } else {
                        fprintf(stderr, "  FAIL: %s — expected INVALID (%s), got VALID\n",
                                short_name,
                                expected_exception ? expected_exception : "?");
                    }
                }

                if (stop_on_fail) *stopped = true;
            }

            if (container) eof_container_free(container);
            free(code_bytes);
        }
    }

    cJSON_Delete(root);
}

//==============================================================================
// Main
//==============================================================================

int main(int argc, char *argv[])
{
    bool verbose = false;
    bool stop_on_fail = false;
    const char *test_path = NULL;

    // Parse arguments
    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "-v") == 0) {
            verbose = true;
        } else if (strcmp(argv[i], "-s") == 0) {
            stop_on_fail = true;
        } else if (strcmp(argv[i], "-h") == 0 || strcmp(argv[i], "--help") == 0) {
            printf("Usage: %s [options] [path]\n", argv[0]);
            printf("  path    Directory or JSON file (default: ../vm/tests/fixtures/eof_tests)\n");
            printf("  -v      Verbose: print each failing vector\n");
            printf("  -s      Stop on first failure\n");
            return 0;
        } else {
            test_path = argv[i];
        }
    }

    if (!test_path)
        test_path = "../vm/tests/fixtures/eof_tests";

    printf("\n=== EOF Osaka Validation Tests ===\n");
    printf("Path: %s\n\n", test_path);

    // Collect JSON files
    file_list_t files;
    file_list_init(&files);

    struct stat st;
    if (stat(test_path, &st) != 0) {
        fprintf(stderr, "ERROR: cannot access %s\n", test_path);
        return 1;
    }

    if (S_ISDIR(st.st_mode)) {
        find_json_files(test_path, &files);
    } else {
        file_list_add(&files, test_path);
    }

    // Sort for deterministic order
    qsort(files.paths, (size_t)files.count, sizeof(char *), cmp_str);

    printf("Found %d JSON files\n\n", files.count);

    // Process all files
    stats_t global = {0};
    bool stopped = false;

    for (int i = 0; i < files.count && !stopped; i++) {
        process_file(files.paths[i], &global, verbose, stop_on_fail, &stopped);
    }

    // Print per-category results
    printf("%-45s %8s %8s %8s\n", "Category", "Passed", "Total", "Rate");
    printf("%-45s %8s %8s %8s\n",
           "---------------------------------------------",
           "--------", "--------", "--------");

    for (int i = 0; i < num_categories; i++) {
        stats_t *s = &categories[i].stats;
        double rate = s->total > 0 ? 100.0 * s->passed / s->total : 0.0;
        printf("%-45s %8d %8d %7.1f%%", categories[i].name,
               s->passed, s->total, rate);
        if (s->failed > 0)
            printf("  [%d FAIL]", s->failed);
        printf("\n");
    }

    // Print global summary
    printf("\n");
    double global_rate = global.total > 0 ? 100.0 * global.passed / global.total : 0.0;
    printf("=== Total: %d/%d passed (%.1f%%) ===\n",
           global.passed, global.total, global_rate);
    if (global.failed > 0)
        printf("    %d failed\n", global.failed);
    if (global.errors > 0)
        printf("    %d errors\n", global.errors);
    printf("\n");

    file_list_free(&files);

    return (global.failed == 0 && global.errors == 0) ? 0 : 1;
}
