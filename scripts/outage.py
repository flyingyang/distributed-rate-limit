"""Verify automatic single-primary failover and fail-closed full-store outage."""

import sys
import subprocess

if __package__ in (None, ""):
    from ha import verify
else:
    from .ha import verify


if __name__ == "__main__":
    try:
        verify()
    except (RuntimeError, ValueError, OSError, subprocess.SubprocessError) as error:
        print(f"error: {error}", file=sys.stderr)
        sys.exit(1)
