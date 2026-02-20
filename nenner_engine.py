"""
Nenner Signal Engine - Shim
============================
Thin wrapper that preserves backward compatibility.

    python nenner_engine.py --status       (still works)
    python -m nenner_engine --status       (new canonical way)

All logic lives in the nenner_engine/ package.
This file re-exports everything so existing imports and tests work unchanged.
"""

# Re-export the full public API from the package so that
#   from nenner_engine import parse_price, init_db, ...
# continues to work whether someone imports the file or the package.
from nenner_engine import *  # noqa: F401,F403
from nenner_engine.cli import main, setup_logging

# When run directly, behave exactly like the old monolith
if __name__ == "__main__":
    main()
