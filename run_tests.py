#!/usr/bin/env python3
"""
Test runner script for Audiobook Tracker
Runs all tests with proper configuration and warning handling
"""

import subprocess
import sys
import os
from pathlib import Path

def run_tests():
    """Run all tests with optimized settings"""

    # Change to project root directory
    project_root = Path(__file__).parent
    os.chdir(project_root)
    os.environ.setdefault(
        "PYTHONWARNINGS",
        "ignore:datetime\\.datetime\\.utcnow\\(\\) is deprecated:DeprecationWarning",
    )

    # Test directories
    test_dirs = [
        "tests/integration",
        "tests/operations"
    ]

    # Common pytest arguments for clean output and warning suppression
    pytest_args = [
        "python", "-m", "pytest",
        "--tb=short",  # Shorter traceback format
        "--disable-warnings",  # Disable warnings by default
        "--strict-markers",  # Strict marker validation
        "--strict-config",  # Strict config validation
        "-q",  # Quiet mode
    ]

    # Add test directories
    pytest_args.extend(test_dirs)

    print("Running Audiobook Tracker Tests")
    print("=" * 50)

    try:
        # Run pytest
        result = subprocess.run(pytest_args, capture_output=True, text=True)

        # Print output
        if result.stdout:
            print(result.stdout)

        if result.stderr:
            print("STDERR:", result.stderr, file=sys.stderr)

        # Check result
        if result.returncode == 0:
            print("\n✅ All tests passed!")
            return True
        else:
            print(f"\n❌ Tests failed with exit code {result.returncode}")
            return False

    except Exception as e:
        print(f"❌ Error running tests: {e}")
        return False

def run_tests_verbose():
    """Run tests with verbose output (shows warnings)"""

    project_root = Path(__file__).parent
    os.chdir(project_root)
    os.environ.setdefault(
        "PYTHONWARNINGS",
        "ignore:datetime\\.datetime\\.utcnow\\(\\) is deprecated:DeprecationWarning",
    )

    test_dirs = [
        "tests/integration",
        "tests/operations"
    ]

    pytest_args = [
        "python", "-m", "pytest",
        "--tb=long",  # Full traceback
        "-v",  # Verbose output
        "-s",  # Don't capture output
    ]

    pytest_args.extend(test_dirs)

    print("🔍 Running tests with verbose output (showing warnings)")
    print("=" * 60)

    try:
        result = subprocess.run(pytest_args)
        return result.returncode == 0
    except Exception as e:
        print(f"❌ Error running tests: {e}")
        return False

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--verbose":
        success = run_tests_verbose()
    else:
        success = run_tests()

    sys.exit(0 if success else 1)