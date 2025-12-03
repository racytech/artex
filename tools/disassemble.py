#!/usr/bin/env python3
"""
EVM Bytecode Disassembler

Parses and displays EVM bytecode in human-readable format.
Usage: python disassemble.py <hex_bytecode>
       python disassemble.py -f <file.json> -a <address>
"""

import sys
import json
import argparse

# EVM Opcode definitions
OPCODES = {
    0x00: ('STOP', 0),
    0x01: ('ADD', 0),
    0x02: ('MUL', 0),
    0x03: ('SUB', 0),
    0x04: ('DIV', 0),
    0x05: ('SDIV', 0),
    0x06: ('MOD', 0),
    0x07: ('SMOD', 0),
    0x08: ('ADDMOD', 0),
    0x09: ('MULMOD', 0),
    0x0a: ('EXP', 0),
    0x0b: ('SIGNEXTEND', 0),
    
    0x10: ('LT', 0),
    0x11: ('GT', 0),
    0x12: ('SLT', 0),
    0x13: ('SGT', 0),
    0x14: ('EQ', 0),
    0x15: ('ISZERO', 0),
    0x16: ('AND', 0),
    0x17: ('OR', 0),
    0x18: ('XOR', 0),
    0x19: ('NOT', 0),
    0x1a: ('BYTE', 0),
    0x1b: ('SHL', 0),
    0x1c: ('SHR', 0),
    0x1d: ('SAR', 0),
    
    0x20: ('KECCAK256', 0),
    
    0x30: ('ADDRESS', 0),
    0x31: ('BALANCE', 0),
    0x32: ('ORIGIN', 0),
    0x33: ('CALLER', 0),
    0x34: ('CALLVALUE', 0),
    0x35: ('CALLDATALOAD', 0),
    0x36: ('CALLDATASIZE', 0),
    0x37: ('CALLDATACOPY', 0),
    0x38: ('CODESIZE', 0),
    0x39: ('CODECOPY', 0),
    0x3a: ('GASPRICE', 0),
    0x3b: ('EXTCODESIZE', 0),
    0x3c: ('EXTCODECOPY', 0),
    0x3d: ('RETURNDATASIZE', 0),
    0x3e: ('RETURNDATACOPY', 0),
    0x3f: ('EXTCODEHASH', 0),
    
    0x40: ('BLOCKHASH', 0),
    0x41: ('COINBASE', 0),
    0x42: ('TIMESTAMP', 0),
    0x43: ('NUMBER', 0),
    0x44: ('DIFFICULTY', 0),
    0x45: ('GASLIMIT', 0),
    0x46: ('CHAINID', 0),
    0x47: ('SELFBALANCE', 0),
    0x48: ('BASEFEE', 0),
    
    0x50: ('POP', 0),
    0x51: ('MLOAD', 0),
    0x52: ('MSTORE', 0),
    0x53: ('MSTORE8', 0),
    0x54: ('SLOAD', 0),
    0x55: ('SSTORE', 0),
    0x56: ('JUMP', 0),
    0x57: ('JUMPI', 0),
    0x58: ('PC', 0),
    0x59: ('MSIZE', 0),
    0x5a: ('GAS', 0),
    0x5b: ('JUMPDEST', 0),
    
    0xa0: ('LOG0', 0),
    0xa1: ('LOG1', 0),
    0xa2: ('LOG2', 0),
    0xa3: ('LOG3', 0),
    0xa4: ('LOG4', 0),
    
    0xf0: ('CREATE', 0),
    0xf1: ('CALL', 0),
    0xf2: ('CALLCODE', 0),
    0xf3: ('RETURN', 0),
    0xf4: ('DELEGATECALL', 0),
    0xf5: ('CREATE2', 0),
    0xfa: ('STATICCALL', 0),
    0xfd: ('REVERT', 0),
    0xfe: ('INVALID', 0),
    0xff: ('SELFDESTRUCT', 0),
}

# PUSH opcodes (0x60-0x7f)
for i in range(1, 33):
    OPCODES[0x5f + i] = (f'PUSH{i}', i)

# DUP opcodes (0x80-0x8f)
for i in range(1, 17):
    OPCODES[0x7f + i] = (f'DUP{i}', 0)

# SWAP opcodes (0x90-0x9f)
for i in range(1, 17):
    OPCODES[0x8f + i] = (f'SWAP{i}', 0)


def disassemble(bytecode_hex):
    """
    Disassemble EVM bytecode into human-readable format.
    
    Args:
        bytecode_hex: Hex string of bytecode (with or without 0x prefix)
    
    Returns:
        List of (pc, opcode_name, operand_hex) tuples
    """
    # Remove 0x prefix if present
    if bytecode_hex.startswith('0x'):
        bytecode_hex = bytecode_hex[2:]
    
    # Convert to bytes
    try:
        bytecode = bytes.fromhex(bytecode_hex)
    except ValueError as e:
        print(f"Error: Invalid hex string: {e}", file=sys.stderr)
        return []
    
    instructions = []
    pc = 0
    
    while pc < len(bytecode):
        opcode = bytecode[pc]
        
        if opcode in OPCODES:
            name, operand_size = OPCODES[opcode]
            
            # Extract operand for PUSH instructions
            if operand_size > 0:
                operand_bytes = bytecode[pc+1:pc+1+operand_size]
                # Pad with zeros if at end of bytecode
                if len(operand_bytes) < operand_size:
                    operand_bytes += b'\x00' * (operand_size - len(operand_bytes))
                operand_hex = operand_bytes.hex()
                instructions.append((pc, name, operand_hex))
                pc += 1 + operand_size
            else:
                instructions.append((pc, name, None))
                pc += 1
        else:
            # Unknown opcode
            instructions.append((pc, f'UNKNOWN(0x{opcode:02x})', None))
            pc += 1
    
    return instructions


def format_disassembly(instructions, show_pc=True, show_hex=True):
    """Format disassembled instructions for display."""
    lines = []
    
    for pc, name, operand in instructions:
        parts = []
        
        if show_pc:
            parts.append(f'{pc:04d}')
        
        if show_hex:
            if operand:
                hex_str = OPCODES.get(next(k for k, v in OPCODES.items() if v[0] == name), (None, 0))[0]
                parts.append(f'{name:16s}')
            else:
                parts.append(f'{name:16s}')
        else:
            parts.append(name)
        
        if operand:
            parts.append(f'0x{operand}')
        
        lines.append('  '.join(parts))
    
    return '\n'.join(lines)


def load_from_json(filepath, address):
    """Load bytecode from a test JSON file."""
    try:
        with open(filepath, 'r') as f:
            data = json.load(f)
        
        # Handle different JSON structures
        # Try to find the address in pre-state
        for test_name, test_data in data.items():
            if test_name == '_info':
                continue
            
            if 'pre' in test_data:
                pre = test_data['pre']
                if address in pre:
                    code = pre[address].get('code', '0x')
                    return code
        
        print(f"Error: Address {address} not found in {filepath}", file=sys.stderr)
        return None
        
    except FileNotFoundError:
        print(f"Error: File {filepath} not found", file=sys.stderr)
        return None
    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON: {e}", file=sys.stderr)
        return None


def main():
    parser = argparse.ArgumentParser(
        description='Disassemble EVM bytecode',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Examples:
  %(prog)s 6000600055
  %(prog)s 0x6000600055
  %(prog)s -f test.json -a 0x1234...
  %(prog)s --no-pc 0x6000600055
        '''
    )
    
    parser.add_argument('bytecode', nargs='?', help='Hex bytecode to disassemble')
    parser.add_argument('-f', '--file', help='Load bytecode from JSON file')
    parser.add_argument('-a', '--address', help='Contract address (when using -f)')
    parser.add_argument('--no-pc', action='store_true', help='Hide program counter')
    parser.add_argument('--no-hex', action='store_true', help='Hide hex values')
    
    args = parser.parse_args()
    
    # Get bytecode
    bytecode = None
    if args.file and args.address:
        bytecode = load_from_json(args.file, args.address)
    elif args.bytecode:
        bytecode = args.bytecode
    else:
        parser.print_help()
        sys.exit(1)
    
    if not bytecode:
        sys.exit(1)
    
    # Disassemble
    instructions = disassemble(bytecode)
    
    if not instructions:
        print("No instructions found", file=sys.stderr)
        sys.exit(1)
    
    # Print results
    print(f"Bytecode: {bytecode[:80]}{'...' if len(bytecode) > 80 else ''}")
    print(f"Size: {len(bytecode.replace('0x', '')) // 2} bytes")
    print()
    print("Disassembly:")
    print("-" * 60)
    print(format_disassembly(instructions, 
                            show_pc=not args.no_pc,
                            show_hex=not args.no_hex))
    print("-" * 60)
    print(f"Total instructions: {len(instructions)}")


if __name__ == '__main__':
    main()
