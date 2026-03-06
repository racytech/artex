/**
 * Fork Configuration Tests
 */

#include "fork.h"
#include <assert.h>
#include <stdio.h>
#include <string.h>

void test_fork_names(void)
{
    printf("Testing fork names...\n");
    
    assert(strcmp(fork_get_name(FORK_FRONTIER), "Frontier") == 0);
    assert(strcmp(fork_get_name(FORK_HOMESTEAD), "Homestead") == 0);
    assert(strcmp(fork_get_name(FORK_BYZANTIUM), "Byzantium") == 0);
    assert(strcmp(fork_get_name(FORK_CONSTANTINOPLE), "Constantinople") == 0);
    assert(strcmp(fork_get_name(FORK_ISTANBUL), "Istanbul") == 0);
    assert(strcmp(fork_get_name(FORK_BERLIN), "Berlin") == 0);
    assert(strcmp(fork_get_name(FORK_LONDON), "London") == 0);
    assert(strcmp(fork_get_name(FORK_PARIS), "Paris (The Merge)") == 0);
    assert(strcmp(fork_get_name(FORK_SHANGHAI), "Shanghai") == 0);
    assert(strcmp(fork_get_name(FORK_CANCUN), "Cancun") == 0);
    
    printf("  ✓ All fork names correct\n");
}

void test_mainnet_fork_detection(void)
{
    printf("Testing mainnet fork detection...\n");

    const chain_config_t *mainnet = chain_config_mainnet();

    // Pre-merge: block number matters, timestamp=0 (irrelevant)
    assert(fork_get_active(0, 0, mainnet) == FORK_FRONTIER);
    assert(fork_get_active(1000000, 0, mainnet) == FORK_FRONTIER);
    assert(fork_get_active(1150000, 0, mainnet) == FORK_HOMESTEAD);
    assert(fork_get_active(2463000, 0, mainnet) == FORK_TANGERINE_WHISTLE);
    assert(fork_get_active(2675000, 0, mainnet) == FORK_SPURIOUS_DRAGON);
    assert(fork_get_active(4370000, 0, mainnet) == FORK_BYZANTIUM);
    assert(fork_get_active(7280000, 0, mainnet) == FORK_CONSTANTINOPLE);
    assert(fork_get_active(9069000, 0, mainnet) == FORK_ISTANBUL);
    assert(fork_get_active(12244000, 0, mainnet) == FORK_BERLIN);
    assert(fork_get_active(12965000, 0, mainnet) == FORK_LONDON);
    assert(fork_get_active(15537394, 0, mainnet) == FORK_PARIS);

    // Post-merge: timestamp matters
    assert(fork_get_active(17034870, 1681338455, mainnet) == FORK_SHANGHAI);
    assert(fork_get_active(19426587, 1710338135, mainnet) == FORK_CANCUN);
    assert(fork_get_active(20000000, 1710338135, mainnet) == FORK_CANCUN);

    printf("  ✓ Mainnet fork detection correct\n");
}

void test_sepolia_fork_detection(void)
{
    printf("Testing Sepolia fork detection...\n");

    const chain_config_t *sepolia = chain_config_sepolia();

    // Sepolia: all pre-merge forks at block 0, Paris at 1735371
    // With timestamp=0, pre-merge blocks return Gray Glacier (last pre-merge fork at block 0)
    assert(fork_get_active(0, 0, sepolia) == FORK_GRAY_GLACIER);
    assert(fork_get_active(1735371, 0, sepolia) == FORK_PARIS);

    // Post-merge: timestamp-based
    assert(fork_get_active(2990908, 1677557088, sepolia) == FORK_SHANGHAI);
    assert(fork_get_active(5187023, 1706655072, sepolia) == FORK_CANCUN);
    assert(fork_get_active(6000000, 1706655072, sepolia) == FORK_CANCUN);

    printf("  ✓ Sepolia fork detection correct\n");
}

void test_fork_is_active(void)
{
    printf("Testing fork_is_active...\n");

    const chain_config_t *mainnet = chain_config_mainnet();

    // At block 9069000 (Istanbul activation), timestamp=0 (pre-merge)
    assert(fork_is_active(9069000, 0, mainnet, FORK_FRONTIER) == true);
    assert(fork_is_active(9069000, 0, mainnet, FORK_HOMESTEAD) == true);
    assert(fork_is_active(9069000, 0, mainnet, FORK_BYZANTIUM) == true);
    assert(fork_is_active(9069000, 0, mainnet, FORK_ISTANBUL) == true);
    assert(fork_is_active(9069000, 0, mainnet, FORK_BERLIN) == false);
    assert(fork_is_active(9069000, 0, mainnet, FORK_LONDON) == false);
    assert(fork_is_active(9069000, 0, mainnet, FORK_PARIS) == false);

    // At block 15537394 (Paris/Merge activation), timestamp before Shanghai
    assert(fork_is_active(15537394, 0, mainnet, FORK_LONDON) == true);
    assert(fork_is_active(15537394, 0, mainnet, FORK_PARIS) == true);
    assert(fork_is_active(15537394, 0, mainnet, FORK_SHANGHAI) == false);

    printf("  ✓ fork_is_active works correctly\n");
}

void test_fork_features(void)
{
    printf("Testing fork feature queries...\n");
    
    // DELEGATECALL (Byzantium+)
    assert(fork_has_delegatecall(FORK_FRONTIER) == false);
    assert(fork_has_delegatecall(FORK_HOMESTEAD) == false);
    assert(fork_has_delegatecall(FORK_BYZANTIUM) == true);
    assert(fork_has_delegatecall(FORK_ISTANBUL) == true);
    
    // STATICCALL (Byzantium+)
    assert(fork_has_staticcall(FORK_HOMESTEAD) == false);
    assert(fork_has_staticcall(FORK_BYZANTIUM) == true);
    
    // CREATE2 (Constantinople+)
    assert(fork_has_create2(FORK_BYZANTIUM) == false);
    assert(fork_has_create2(FORK_CONSTANTINOPLE) == true);
    assert(fork_has_create2(FORK_ISTANBUL) == true);
    
    // EXTCODEHASH (Constantinople+)
    assert(fork_has_extcodehash(FORK_BYZANTIUM) == false);
    assert(fork_has_extcodehash(FORK_CONSTANTINOPLE) == true);
    
    // CHAINID (Istanbul+)
    assert(fork_has_chainid(FORK_CONSTANTINOPLE) == false);
    assert(fork_has_chainid(FORK_ISTANBUL) == true);
    assert(fork_has_chainid(FORK_BERLIN) == true);
    
    // SELFBALANCE (Istanbul+)
    assert(fork_has_selfbalance(FORK_BYZANTIUM) == false);
    assert(fork_has_selfbalance(FORK_ISTANBUL) == true);
    
    // BASEFEE (London+)
    assert(fork_has_basefee(FORK_BERLIN) == false);
    assert(fork_has_basefee(FORK_LONDON) == true);
    assert(fork_has_basefee(FORK_PARIS) == true);
    
    // PREVRANDAO (Paris+)
    assert(fork_has_prevrandao(FORK_LONDON) == false);
    assert(fork_has_prevrandao(FORK_PARIS) == true);
    assert(fork_has_prevrandao(FORK_SHANGHAI) == true);
    
    // PUSH0 (Shanghai+)
    assert(fork_has_push0(FORK_PARIS) == false);
    assert(fork_has_push0(FORK_SHANGHAI) == true);
    assert(fork_has_push0(FORK_CANCUN) == true);
    
    // Transient storage - TSTORE/TLOAD (Cancun+)
    assert(fork_has_transient_storage(FORK_SHANGHAI) == false);
    assert(fork_has_transient_storage(FORK_CANCUN) == true);
    
    // MCOPY (Cancun+)
    assert(fork_has_mcopy(FORK_SHANGHAI) == false);
    assert(fork_has_mcopy(FORK_CANCUN) == true);
    
    // Access lists (Berlin+)
    assert(fork_has_access_lists(FORK_ISTANBUL) == false);
    assert(fork_has_access_lists(FORK_BERLIN) == true);
    assert(fork_has_access_lists(FORK_LONDON) == true);
    
    printf("  ✓ All fork feature queries correct\n");
}

void test_custom_chain_config(void)
{
    printf("Testing custom chain configuration...\n");
    
    chain_config_t *custom = chain_config_create(999, "testchain");
    assert(custom != NULL);
    assert(custom->chain_id == 999);
    assert(strcmp(custom->name, "testchain") == 0);
    
    // All forks should be disabled (UINT64_MAX)
    assert(fork_get_activation_block(custom, FORK_HOMESTEAD) == UINT64_MAX);
    assert(fork_get_activation_block(custom, FORK_BYZANTIUM) == UINT64_MAX);

    // Any block should return FRONTIER
    assert(fork_get_active(0, 0, custom) == FORK_FRONTIER);
    assert(fork_get_active(1000000, 0, custom) == FORK_FRONTIER);
    
    chain_config_free(custom);
    
    printf("  ✓ Custom chain configuration works\n");
}

void test_chain_ids(void)
{
    printf("Testing chain IDs...\n");
    
    assert(chain_config_mainnet()->chain_id == 1);
    assert(chain_config_sepolia()->chain_id == 11155111);
    assert(chain_config_goerli()->chain_id == 5);
    assert(chain_config_holesky()->chain_id == 17000);
    
    printf("  ✓ Chain IDs correct\n");
}

void test_fork_activation_blocks(void)
{
    printf("Testing fork activation block queries...\n");
    
    const chain_config_t *mainnet = chain_config_mainnet();
    
    assert(fork_get_activation_block(mainnet, FORK_FRONTIER) == 0);
    assert(fork_get_activation_block(mainnet, FORK_HOMESTEAD) == 1150000);
    assert(fork_get_activation_block(mainnet, FORK_BYZANTIUM) == 4370000);
    assert(fork_get_activation_block(mainnet, FORK_CONSTANTINOPLE) == 7280000);
    assert(fork_get_activation_block(mainnet, FORK_ISTANBUL) == 9069000);
    assert(fork_get_activation_block(mainnet, FORK_BERLIN) == 12244000);
    assert(fork_get_activation_block(mainnet, FORK_LONDON) == 12965000);
    assert(fork_get_activation_block(mainnet, FORK_PARIS) == 15537394);
    // Post-merge: activation values are timestamps
    assert(fork_get_activation_block(mainnet, FORK_SHANGHAI) == 1681338455);
    assert(fork_get_activation_block(mainnet, FORK_CANCUN) == 1710338135);
    
    printf("  ✓ Fork activation blocks correct\n");
}

int main(void)
{
    printf("\n=== Fork Configuration Tests ===\n\n");
    
    test_fork_names();
    test_mainnet_fork_detection();
    test_sepolia_fork_detection();
    test_fork_is_active();
    test_fork_features();
    test_custom_chain_config();
    test_chain_ids();
    test_fork_activation_blocks();
    
    printf("\n=== All Fork Tests Passed ===\n\n");
    return 0;
}
