#!/usr/bin/env python3
"""
Container health check.

Returns exit code 0 if the application can import its core modules
and required directories exist.
Used by Docker HEALTHCHECK directive.
"""

import sys
from pathlib import Path

# Ensure project root is on sys.path (needed outside Docker too)
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


def main() -> int:
    try:
        import agents.base  # noqa: F401
        import agents.orchestrator  # noqa: F401
        import contracts.validator  # noqa: F401
        import middleware.policy  # noqa: F401
        import state.machine  # noqa: F401

        # Verify required directories exist
        root = Path(__file__).resolve().parent.parent
        for d in ["config", "data"]:
            if not (root / d).is_dir():
                print(f"Health check failed: {d}/ directory missing", file=sys.stderr)
                return 1

        return 0
    except Exception as e:
        print(f"Health check failed: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
