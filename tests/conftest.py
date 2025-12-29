import os
import sys

# Ensure project root is on sys.path so tests can import the 'tracker' package
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

# Optional sanity import (no-op but useful when debugging import issues)
try:
    import tracker  # type: ignore
except Exception:
    # Let the original import error surface in tests if tracker truly fails to import
    pass
