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
    
    // Test various block numbers
    assert(fork_get_active(0, mainnet) == FORK_FRONTIER);
    assert(fork_get_active(1000000, mainnet) == FORK_FRONTIER);
    assert(fork_get_active(1150000, mainnet) == FORK_HOMESTEAD);
    assert(fork_get_active(2463000, mainnet) == FORK_TANGERINE_WHISTLE);
    assert(fork_get_active(2675000, mainnet) == FORK_SPURIOUS_DRAGON);
    assert(fork_get_active(4370000, mainnet) == FORK_BYZANTIUM);
    assert(fork_get_active(7280000, mainnet) == FORK_CONSTANTINOPLE);
    assert(fork_get_active(9069000, mainnet) == FORK_ISTANBUL);
    assert(fork_get_active(12244000, mainnet) == FORK_BERLIN);
    assert(fork_get_active(12965000, mainnet) == FORK_LONDON);
    assert(fork_get_active(15537394, mainnet) == FORK_PARIS);
    assert(fork_get_active(17034870, mainnet) == FORK_SHANGHAI);
    assert(fork_get_active(19426587, mainnet) == FORK_CANCUN);
    assert(fork_get_active(20000000, mainnet) == FORK_CANCUN);  // Current latest
    
    printf("  ✓ Mainnet fork detection correct\n");
}

void test_sepolia_fork_detection(void)
{
    printf("Testing Sepolia fork detection...\n");
    
    const chain_config_t *sepolia = chain_config_sepolia();
    
    // Sepolia started with all pre-merge forks active
    assert(fork_get_active(0, sepolia) == FORK_FRONTIER);
    assert(fork_get_active(1735371, sepolia) == FORK_PARIS);
    assert(fork_get_active(2990908, sepolia) == FORK_SHANGHAI);
    assert(fork_get_active(5187023, sepolia) == FORK_CANCUN);
    assert(fork_get_active(6000000, sepolia) == FORK_CANCUN);
    
    printf("  ✓ Sepolia fork detection correct\n");
}

void test_fork_is_active(void)
{
    printf("Testing fork_is_active...\n");
    
    const chain_config_t *mainnet = chain_config_mainnet();
    
    // At block 9069000 (Istanbul activation)
    assert(fork_is_active(9069000, mainnet, FORK_FRONTIER) == true);
    assert(fork_is_active(9069000, mainnet, FORK_HOMESTEAD) == true);
    assert(fork_is_active(9069000, mainnet, FORK_BYZANTIUM) == true);
    assert(fork_is_active(9069000, mainnet, FORK_ISTANBUL) == true);
    assert(fork_is_active(9069000, mainnet, FORK_BERLIN) == false);
    assert(fork_is_active(9069000, mainnet, FORK_LONDON) == false);
    assert(fork_is_active(9069000, mainnet, FORK_PARIS) == false);
    
    // At block 15537394 (Paris/Merge activation)
    assert(fork_is_active(15537394, mainnet, FORK_LONDON) == true);
    assert(fork_is_active(15537394, mainnet, FORK_PARIS) == true);
    assert(fork_is_active(15537394, mainnet, FORK_SHANGHAI) == false);
    
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
    assert(fork_get_active(0, custom) == FORK_FRONTIER);
    assert(fork_get_active(1000000, custom) == FORK_FRONTIER);
    
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
    assert(fork_get_activation_block(mainnet, FORK_SHANGHAI) == 17034870);
    assert(fork_get_activation_block(mainnet, FORK_CANCUN) == 19426587);
    
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
