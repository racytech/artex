#!/usr/bin/env python3
"""
Compare expected post-state from test fixture with actual post-state from our implementation.
Helps identify specific differences in account balances, nonces, code, and storage.
"""

import json
import sys
from typing import Dict, Any, Set

def normalize_hex(value: str) -> str:
    """Normalize hex string by removing 0x prefix and converting to lowercase."""
    if value.startswith('0x') or value.startswith('0X'):
        value = value[2:]
    return value.lower()

def parse_uint256(value: str) -> int:
    """Parse a hex string as uint256 integer."""
    normalized = normalize_hex(value)
    if not normalized:
        return 0
    return int(normalized, 16)

def format_wei(value: int) -> str:
    """Format wei value with separators for readability."""
    if value == 0:
        return "0"
    # Convert to string and add separators
    s = str(value)
    parts = []
    for i in range(len(s), 0, -3):
        parts.append(s[max(0, i-3):i])
    return "_".join(reversed(parts))

def compare_accounts(expected: Dict[str, Any], actual: Dict[str, Any]) -> Dict[str, Any]:
    """Compare two account objects and return differences."""
    diff = {}
    
    # Compare balance
    exp_balance = parse_uint256(expected.get('balance', '0x0'))
    act_balance = parse_uint256(actual.get('balance', '0x0'))
    if exp_balance != act_balance:
        diff['balance'] = {
            'expected': expected.get('balance', '0x0'),
            'actual': actual.get('balance', '0x0'),
            'expected_dec': format_wei(exp_balance),
            'actual_dec': format_wei(act_balance),
            'difference': act_balance - exp_balance
        }
    
    # Compare nonce
    exp_nonce = parse_uint256(expected.get('nonce', '0x0'))
    act_nonce = parse_uint256(actual.get('nonce', '0x0'))
    if exp_nonce != act_nonce:
        diff['nonce'] = {
            'expected': expected.get('nonce', '0x0'),
            'actual': actual.get('nonce', '0x0'),
            'expected_dec': exp_nonce,
            'actual_dec': act_nonce
        }
    
    # Compare code
    exp_code = normalize_hex(expected.get('code', '0x'))
    act_code = normalize_hex(actual.get('code', '0x'))
    if exp_code != act_code:
        diff['code'] = {
            'expected': expected.get('code', '0x'),
            'actual': actual.get('code', '0x'),
            'expected_len': len(exp_code) // 2,
            'actual_len': len(act_code) // 2
        }
    
    # Compare storage
    exp_storage = expected.get('storage', {})
    act_storage = actual.get('storage', {})
    
    # Get all storage keys
    all_keys = set(exp_storage.keys()) | set(act_storage.keys())
    storage_diff = {}
    
    for key in all_keys:
        exp_val = exp_storage.get(key, '0x0')
        act_val = act_storage.get(key, '0x0')
        
        exp_int = parse_uint256(exp_val)
        act_int = parse_uint256(act_val)
        
        if exp_int != act_int:
            storage_diff[key] = {
                'expected': exp_val,
                'actual': act_val,
                'expected_dec': exp_int,
                'actual_dec': act_int
            }
    
    if storage_diff:
        diff['storage'] = storage_diff
    
    return diff

def main():
    if len(sys.argv) < 3:
        print("Usage: compare_post_states.py <expected_test.json> <actual_post_state.json> [--ignore-coinbase]")
        print("\nExample:")
        print("  python3 tools/compare_post_states.py build/failing_test.json build/post_state.json")
        print("  python3 tools/compare_post_states.py build/failing_test.json build/post_state.json --ignore-coinbase")
        sys.exit(1)
    
    expected_file = sys.argv[1]
    actual_file = sys.argv[2]
    ignore_coinbase = '--ignore-coinbase' in sys.argv[3:] if len(sys.argv) > 3 else True  # Default to ignoring coinbase
    
    # Load expected test fixture
    with open(expected_file, 'r') as f:
        test_data = json.load(f)
    
    # Extract post-state from test (it's nested under the test name -> post -> fork -> state)
    # Structure: { "test_name": { "post": { "Fork": [{ "state": {...} }] } } }
    test_name = list(test_data.keys())[0]
    test = test_data[test_name]
    
    # Get coinbase address if we need to ignore it
    coinbase_addr = None
    if ignore_coinbase:
        env = test.get('env', {})
        coinbase = env.get('currentCoinbase', '')
        if coinbase:
            coinbase_addr = normalize_hex(coinbase)
    
    # Get post conditions for the first fork
    post = test.get('post', {})
    fork_name = list(post.keys())[0]
    post_condition = post[fork_name][0]
    
    # Get the expected state - could be "state" or "expect"
    expected_state = post_condition.get('state', post_condition.get('expect', []))
    
    # If it's a list (expect format), convert to dict
    if isinstance(expected_state, list):
        state_dict = {}
        for account in expected_state:
            addr = account.get('address', '')
            if addr:
                state_dict[normalize_hex(addr)] = account
        expected_state = state_dict
    else:
        # If it's already a dict, normalize the keys
        normalized = {}
        for addr, account in expected_state.items():
            normalized[normalize_hex(addr)] = account
        expected_state = normalized
    
    # Load actual post-state
    with open(actual_file, 'r') as f:
        actual_state = json.load(f)
    
    # Normalize actual state addresses
    actual_state_normalized = {}
    for addr, account in actual_state.items():
        actual_state_normalized[normalize_hex(addr)] = account
    
    # Get all addresses
    all_addresses = set(expected_state.keys()) | set(actual_state_normalized.keys())
    
    print("=" * 80)
    print("POST-STATE COMPARISON")
    print("=" * 80)
    print(f"Expected file: {expected_file}")
    print(f"Actual file:   {actual_file}")
    print(f"Fork:          {fork_name}")
    print(f"Total expected accounts: {len(expected_state)}")
    print(f"Total actual accounts:   {len(actual_state_normalized)}")
    print()
    
    has_differences = False
    
    # Compare each account
    for addr in sorted(all_addresses):
        # Skip coinbase if requested
        if coinbase_addr and addr == coinbase_addr:
            continue
        
        exp_acc = expected_state.get(addr, {})
        act_acc = actual_state_normalized.get(addr, {})
        
        if not exp_acc:
            print(f"❌ EXTRA ACCOUNT: 0x{addr}")
            print(f"   This account exists in actual state but not in expected state")
            print()
            has_differences = True
            continue
        
        if not act_acc:
            print(f"❌ MISSING ACCOUNT: 0x{addr}")
            print(f"   This account exists in expected state but not in actual state")
            print()
            has_differences = True
            continue
        
        diff = compare_accounts(exp_acc, act_acc)
        
        if diff:
            has_differences = True
            print(f"❌ DIFFERENCE: 0x{addr}")
            
            if 'balance' in diff:
                b = diff['balance']
                print(f"   Balance:")
                print(f"     Expected: {b['expected']} ({b['expected_dec']} wei)")
                print(f"     Actual:   {b['actual']} ({b['actual_dec']} wei)")
                print(f"     Diff:     {b['difference']:+d} wei")
            
            if 'nonce' in diff:
                n = diff['nonce']
                print(f"   Nonce:")
                print(f"     Expected: {n['expected']} ({n['expected_dec']})")
                print(f"     Actual:   {n['actual']} ({n['actual_dec']})")
            
            if 'code' in diff:
                c = diff['code']
                print(f"   Code:")
                print(f"     Expected: {c['expected'][:66]}{'...' if len(c['expected']) > 66 else ''} ({c['expected_len']} bytes)")
                print(f"     Actual:   {c['actual'][:66]}{'...' if len(c['actual']) > 66 else ''} ({c['actual_len']} bytes)")
            
            if 'storage' in diff:
                print(f"   Storage differences: {len(diff['storage'])} slot(s)")
                for slot_key, slot_diff in diff['storage'].items():
                    print(f"     Slot {slot_key}:")
                    print(f"       Expected: {slot_diff['expected']} ({slot_diff['expected_dec']})")
                    print(f"       Actual:   {slot_diff['actual']} ({slot_diff['actual_dec']})")
            
            print()
    
    print("=" * 80)
    if has_differences:
        print("❌ DIFFERENCES FOUND")
        sys.exit(1)
    else:
        print("✅ ALL ACCOUNTS MATCH")
        sys.exit(0)

if __name__ == '__main__':
    main()
