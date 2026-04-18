/**
 * Fork Configuration Implementation
 */

#include "fork.h"
#include <stdlib.h>
#include <string.h>
#include <stdint.h>

//==============================================================================
// Fork Names
//==============================================================================

static const char *fork_names[] = {
    [FORK_FRONTIER] = "Frontier",
    [FORK_HOMESTEAD] = "Homestead",
    [FORK_TANGERINE_WHISTLE] = "Tangerine Whistle",
    [FORK_SPURIOUS_DRAGON] = "Spurious Dragon",
    [FORK_BYZANTIUM] = "Byzantium",
    [FORK_CONSTANTINOPLE] = "Constantinople",
    [FORK_PETERSBURG] = "Petersburg",
    [FORK_ISTANBUL] = "Istanbul",
    [FORK_MUIR_GLACIER] = "Muir Glacier",
    [FORK_BERLIN] = "Berlin",
    [FORK_LONDON] = "London",
    [FORK_ARROW_GLACIER] = "Arrow Glacier",
    [FORK_GRAY_GLACIER] = "Gray Glacier",
    [FORK_PARIS] = "Paris (The Merge)",
    [FORK_SHANGHAI] = "Shanghai",
    [FORK_CANCUN] = "Cancun",
    [FORK_PRAGUE] = "Prague",
    [FORK_OSAKA] = "Osaka",
    [FORK_VERKLE] = "Verkle",
    [FORK_LATEST] = "Latest",
};

const char *fork_get_name(evm_fork_t fork)
{
    if (fork >= 0 && fork < sizeof(fork_names) / sizeof(fork_names[0]))
    {
        return fork_names[fork];
    }
    return "Unknown";
}

//==============================================================================
// Mainnet Configuration
//==============================================================================

static const chain_config_t mainnet_config = {
    .chain_id = 1,
    .name = "mainnet",
    .fork_blocks = {
        .frontier = 0,
        .homestead = 1150000,
        .tangerine_whistle = 2463000, // EIP-150
        .spurious_dragon = 2675000,   // EIP-155, EIP-160
        .byzantium = 4370000,
        .constantinople = 7280000,
        .petersburg = 7280000, // Same as Constantinople
        .istanbul = 9069000,
        .muir_glacier = 9200000,
        .berlin = 12244000,
        .london = 12965000,
        .arrow_glacier = 13773000,
        .gray_glacier = 15050000,
        .paris = 15537394,      // The Merge (TTD-based, approx block)
        .shanghai = 1681338455, // Timestamp: April 12, 2023
        .cancun = 1710338135,   // Timestamp: March 13, 2024
        .prague = 1746612311,   // Timestamp: May 7, 2025 (Pectra)
        .osaka = 1764798551,    // Timestamp: Dec 3, 2025 (Fusaka)
        .verkle = UINT64_MAX,
    },
    .blob_cancun = { .target = 3, .max = 6, .update_fraction = 3338477, .timestamp = 0 },
    .blob_prague = { .target = 6, .max = 9, .update_fraction = 5007716, .timestamp = 0 },
    .blob_bpo = {
        { .target = 10, .max = 15, .update_fraction = 8346193,  .timestamp = 1765290071 }, // BPO1
        { .target = 14, .max = 21, .update_fraction = 11684671, .timestamp = 1767747671 }, // BPO2
    },
    .blob_bpo_count = 2,
};

const chain_config_t *chain_config_mainnet(void)
{
    return &mainnet_config;
}

//==============================================================================
// Sepolia Testnet Configuration
//==============================================================================

static const chain_config_t sepolia_config = {
    .chain_id = 11155111,
    .name = "sepolia",
    .fork_blocks = {
        .frontier = 0,
        .homestead = 0,
        .tangerine_whistle = 0,
        .spurious_dragon = 0,
        .byzantium = 0,
        .constantinople = 0,
        .petersburg = 0,
        .istanbul = 0,
        .muir_glacier = 0,
        .berlin = 0,
        .london = 0,
        .arrow_glacier = 0,
        .gray_glacier = 0,
        .paris = 1735371,       // The Merge (block number)
        .shanghai = 1677557088, // Timestamp: Feb 28, 2023
        .cancun = 1706655072,   // Timestamp: Jan 30, 2024
        .prague = UINT64_MAX,
        .osaka = UINT64_MAX,
        .verkle = UINT64_MAX,
    }};

const chain_config_t *chain_config_sepolia(void)
{
    return &sepolia_config;
}

//==============================================================================
// Goerli Testnet Configuration (deprecated)
//==============================================================================

static const chain_config_t goerli_config = {
    .chain_id = 5,
    .name = "goerli",
    .fork_blocks = {
        .frontier = 0,
        .homestead = 0,
        .tangerine_whistle = 0,
        .spurious_dragon = 0,
        .byzantium = 0,
        .constantinople = 0,
        .petersburg = 0,
        .istanbul = 1561651,
        .muir_glacier = 0,
        .berlin = 4460644,
        .london = 5062605,
        .arrow_glacier = 0,
        .gray_glacier = 0,
        .paris = 7382818,
        .shanghai = 1678832736, // Timestamp: March 14, 2023
        .cancun = 1705473120,   // Timestamp: Jan 17, 2024
        .prague = UINT64_MAX,
        .osaka = UINT64_MAX,
        .verkle = UINT64_MAX,
    }};

const chain_config_t *chain_config_goerli(void)
{
    return &goerli_config;
}

//==============================================================================
// Holesky Testnet Configuration
//==============================================================================

static const chain_config_t holesky_config = {
    .chain_id = 17000,
    .name = "holesky",
    .fork_blocks = {
        .frontier = 0,
        .homestead = 0,
        .tangerine_whistle = 0,
        .spurious_dragon = 0,
        .byzantium = 0,
        .constantinople = 0,
        .petersburg = 0,
        .istanbul = 0,
        .muir_glacier = 0,
        .berlin = 0,
        .london = 0,
        .arrow_glacier = 0,
        .gray_glacier = 0,
        .paris = 0,             // Launched post-merge
        .shanghai = 1696000704, // Timestamp: Sept 29, 2023
        .cancun = 1707305664,   // Timestamp: Feb 7, 2024
        .prague = UINT64_MAX,
        .osaka = UINT64_MAX,
        .verkle = UINT64_MAX,
    }};

const chain_config_t *chain_config_holesky(void)
{
    return &holesky_config;
}

//==============================================================================
// Custom Configuration
//==============================================================================

chain_config_t *chain_config_create(uint64_t chain_id, const char *name)
{
    chain_config_t *config = malloc(sizeof(chain_config_t));
    if (!config)
    {
        return NULL;
    }

    config->chain_id = chain_id;
    config->name = name ? strdup(name) : NULL;

    // Initialize all forks to UINT64_MAX (never activate)
    config->fork_blocks.frontier = UINT64_MAX;
    config->fork_blocks.homestead = UINT64_MAX;
    config->fork_blocks.tangerine_whistle = UINT64_MAX;
    config->fork_blocks.spurious_dragon = UINT64_MAX;
    config->fork_blocks.byzantium = UINT64_MAX;
    config->fork_blocks.constantinople = UINT64_MAX;
    config->fork_blocks.petersburg = UINT64_MAX;
    config->fork_blocks.istanbul = UINT64_MAX;
    config->fork_blocks.muir_glacier = UINT64_MAX;
    config->fork_blocks.berlin = UINT64_MAX;
    config->fork_blocks.london = UINT64_MAX;
    config->fork_blocks.arrow_glacier = UINT64_MAX;
    config->fork_blocks.gray_glacier = UINT64_MAX;
    config->fork_blocks.paris = UINT64_MAX;
    config->fork_blocks.shanghai = UINT64_MAX;
    config->fork_blocks.cancun = UINT64_MAX;
    config->fork_blocks.prague = UINT64_MAX;
    config->fork_blocks.osaka = UINT64_MAX;
    config->fork_blocks.verkle = UINT64_MAX;

    /* Initialize blob configs with defaults (no BPO overrides for test chains) */
    config->blob_cancun = (blob_config_t){ .target = 3, .max = 6, .update_fraction = 3338477, .timestamp = 0 };
    config->blob_prague = (blob_config_t){ .target = 6, .max = 9, .update_fraction = 5007716, .timestamp = 0 };
    config->blob_bpo_count = 0;

    return config;
}

void chain_config_free(chain_config_t *config)
{
    if (config && config->name)
    {
        free((void *)config->name);
    }
    free(config);
}

//==============================================================================
// Fork Detection
//==============================================================================

evm_fork_t fork_get_active(uint64_t block_number, uint64_t timestamp, const chain_config_t *config)
{
    if (!config)
    {
        return FORK_FRONTIER;
    }

    const fork_schedule_t *forks = &config->fork_blocks;

    // Post-merge forks: check timestamps (Shanghai+)
    // UINT64_MAX means "not activated" — never match, even if block timestamp equals UINT64_MAX
    if (forks->verkle != UINT64_MAX && timestamp >= forks->verkle)
        return FORK_VERKLE;
    if (forks->osaka != UINT64_MAX && timestamp >= forks->osaka)
        return FORK_OSAKA;
    if (forks->prague != UINT64_MAX && timestamp >= forks->prague)
        return FORK_PRAGUE;
    if (forks->cancun != UINT64_MAX && timestamp >= forks->cancun)
        return FORK_CANCUN;
    if (forks->shanghai != UINT64_MAX && timestamp >= forks->shanghai)
        return FORK_SHANGHAI;

    // Pre-merge forks: check block numbers (Frontier-Paris)
    if (forks->paris != UINT64_MAX && block_number >= forks->paris)
        return FORK_PARIS;
    if (forks->gray_glacier != UINT64_MAX && block_number >= forks->gray_glacier)
        return FORK_GRAY_GLACIER;
    if (forks->arrow_glacier != UINT64_MAX && block_number >= forks->arrow_glacier)
        return FORK_ARROW_GLACIER;
    if (forks->london != UINT64_MAX && block_number >= forks->london)
        return FORK_LONDON;
    if (forks->berlin != UINT64_MAX && block_number >= forks->berlin)
        return FORK_BERLIN;
    if (forks->muir_glacier != UINT64_MAX && block_number >= forks->muir_glacier)
        return FORK_MUIR_GLACIER;
    if (forks->istanbul != UINT64_MAX && block_number >= forks->istanbul)
        return FORK_ISTANBUL;
    if (forks->petersburg != UINT64_MAX && block_number >= forks->petersburg)
        return FORK_PETERSBURG;
    if (forks->constantinople != UINT64_MAX && block_number >= forks->constantinople)
        return FORK_CONSTANTINOPLE;
    if (forks->byzantium != UINT64_MAX && block_number >= forks->byzantium)
        return FORK_BYZANTIUM;
    if (forks->spurious_dragon != UINT64_MAX && block_number >= forks->spurious_dragon)
        return FORK_SPURIOUS_DRAGON;
    if (forks->tangerine_whistle != UINT64_MAX && block_number >= forks->tangerine_whistle)
        return FORK_TANGERINE_WHISTLE;
    if (forks->homestead != UINT64_MAX && block_number >= forks->homestead)
        return FORK_HOMESTEAD;

    return FORK_FRONTIER;
}

bool fork_is_active(uint64_t block_number, uint64_t timestamp, const chain_config_t *config, evm_fork_t fork)
{
    evm_fork_t active = fork_get_active(block_number, timestamp, config);
    return active >= fork;
}

uint64_t fork_get_activation_block(const chain_config_t *config, evm_fork_t fork)
{
    if (!config)
    {
        return UINT64_MAX;
    }

    const fork_schedule_t *forks = &config->fork_blocks;

    switch (fork)
    {
    case FORK_FRONTIER:
        return forks->frontier;
    case FORK_HOMESTEAD:
        return forks->homestead;
    case FORK_TANGERINE_WHISTLE:
        return forks->tangerine_whistle;
    case FORK_SPURIOUS_DRAGON:
        return forks->spurious_dragon;
    case FORK_BYZANTIUM:
        return forks->byzantium;
    case FORK_CONSTANTINOPLE:
        return forks->constantinople;
    case FORK_PETERSBURG:
        return forks->petersburg;
    case FORK_ISTANBUL:
        return forks->istanbul;
    case FORK_MUIR_GLACIER:
        return forks->muir_glacier;
    case FORK_BERLIN:
        return forks->berlin;
    case FORK_LONDON:
        return forks->london;
    case FORK_ARROW_GLACIER:
        return forks->arrow_glacier;
    case FORK_GRAY_GLACIER:
        return forks->gray_glacier;
    case FORK_PARIS:
        return forks->paris;
    case FORK_SHANGHAI:
        return forks->shanghai;
    case FORK_CANCUN:
        return forks->cancun;
    case FORK_PRAGUE:
        return forks->prague;
    case FORK_OSAKA:
        return forks->osaka;
    case FORK_VERKLE:
        return forks->verkle;
    default:
        return UINT64_MAX;
    }
}

//==============================================================================
// Fork Feature Queries
//==============================================================================

bool fork_has_delegatecall(evm_fork_t fork)
{
    return fork >= FORK_BYZANTIUM;
}

bool fork_has_staticcall(evm_fork_t fork)
{
    return fork >= FORK_BYZANTIUM;
}

bool fork_has_create2(evm_fork_t fork)
{
    return fork >= FORK_CONSTANTINOPLE;
}

bool fork_has_extcodehash(evm_fork_t fork)
{
    return fork >= FORK_CONSTANTINOPLE;
}

bool fork_has_chainid(evm_fork_t fork)
{
    return fork >= FORK_ISTANBUL;
}

bool fork_has_selfbalance(evm_fork_t fork)
{
    return fork >= FORK_ISTANBUL;
}

bool fork_has_basefee(evm_fork_t fork)
{
    return fork >= FORK_LONDON;
}

bool fork_has_prevrandao(evm_fork_t fork)
{
    return fork >= FORK_PARIS;
}

bool fork_has_push0(evm_fork_t fork)
{
    return fork >= FORK_SHANGHAI;
}

bool fork_has_transient_storage(evm_fork_t fork)
{
    return fork >= FORK_CANCUN;
}

bool fork_has_blob_opcodes(evm_fork_t fork)
{
    return fork >= FORK_CANCUN;
}

bool fork_has_mcopy(evm_fork_t fork)
{
    return fork >= FORK_CANCUN;
}

bool fork_has_access_lists(evm_fork_t fork)
{
    return fork >= FORK_BERLIN;
}

const blob_config_t *blob_config_active(uint64_t timestamp, const chain_config_t *config) {
    if (!config) return NULL;

    /* Check BPO overrides in reverse order (latest first) */
    for (int i = config->blob_bpo_count - 1; i >= 0; i--) {
        if (config->blob_bpo[i].timestamp != UINT64_MAX &&
            config->blob_bpo[i].update_fraction != 0 &&
            timestamp >= config->blob_bpo[i].timestamp) {
            return &config->blob_bpo[i];
        }
    }

    /* Fall back to Prague or Cancun defaults (only if initialized) */
    if (config->blob_prague.update_fraction != 0 &&
        config->fork_blocks.prague != UINT64_MAX &&
        timestamp >= config->fork_blocks.prague)
        return &config->blob_prague;

    if (config->blob_cancun.update_fraction != 0)
        return &config->blob_cancun;

    return NULL; /* no blob config — caller uses fork-based defaults */
}
