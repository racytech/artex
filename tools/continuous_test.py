#!/usr/bin/env python3
"""
Continuous MPT test runner - generates random test vectors and validates them
Runs in infinite loop until Ctrl+C is pressed
Fails fast on first error
"""

import subprocess
import sys
import os
import signal
from pathlib import Path

# Handle Ctrl+C gracefully
def signal_handler(sig, frame):
    print('\n\n=== Interrupted by user (Ctrl+C) ===')
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)

# Get paths
TOOLS_DIR = Path(__file__).parent
WORKSPACE_DIR = TOOLS_DIR.parent
BUILD_DIR = WORKSPACE_DIR / "build"
GENERATOR_SCRIPT = TOOLS_DIR / "generate_state_tests.py"
TEST_BINARY = BUILD_DIR / "test_mpt_vectors"
OUTPUT_FILE = TOOLS_DIR / "mpt_test_vectors.json"

def run_command(cmd, cwd=None, description=""):
    """Run command and fail fast on error"""
    print(f"\n{'='*60}")
    print(f"{description}")
    print(f"Command: {' '.join(str(c) for c in cmd)}")
    print(f"{'='*60}")
    
    result = subprocess.run(
        cmd,
        cwd=cwd,
        capture_output=True,
        text=True
    )
    
    # Print output
    if result.stdout:
        print(result.stdout, end='')
    if result.stderr:
        print(result.stderr, end='', file=sys.stderr)
    
    # Fail fast on error
    if result.returncode != 0:
        print(f"\n❌ FAILED with exit code {result.returncode}")
        print(f"\n=== TEST FAILED - STOPPING ===")
        sys.exit(1)
    
    return result

def main():
    print("="*60)
    print("Continuous MPT Test Runner")
    print("="*60)
    print(f"Generator: {GENERATOR_SCRIPT}")
    print(f"Test binary: {TEST_BINARY}")
    print(f"Output: {OUTPUT_FILE}")
    print("="*60)
    print("\nPress Ctrl+C to stop\n")
    
    # Verify files exist
    if not GENERATOR_SCRIPT.exists():
        print(f"❌ Generator script not found: {GENERATOR_SCRIPT}")
        sys.exit(1)
    
    if not TEST_BINARY.exists():
        print(f"❌ Test binary not found: {TEST_BINARY}")
        print("Run 'make' in the build directory first")
        sys.exit(1)
    
    iteration = 0
    
    while True:
        iteration += 1
        print(f"\n{'#'*60}")
        print(f"# ITERATION {iteration}")
        print(f"{'#'*60}")
        
        # Step 1: Generate test vectors
        run_command(
            [sys.executable, str(GENERATOR_SCRIPT)],
            cwd=TOOLS_DIR,
            description=f"[{iteration}] Generating random test vectors..."
        )
        
        # Step 2: Run tests
        run_command(
            [str(TEST_BINARY)],
            cwd=BUILD_DIR,
            description=f"[{iteration}] Running MPT vector tests..."
        )
        
        print(f"\n✅ Iteration {iteration} PASSED")

if __name__ == "__main__":
    main()
