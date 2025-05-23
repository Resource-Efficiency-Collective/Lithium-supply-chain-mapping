from __future__ import annotations

import shlex
import subprocess
import sys
from pathlib import Path

# ------------------------------------------------------------------
# List the modules to run, in the desired order
# ------------------------------------------------------------------
MODULES: list[str] = [
    "preprocessing.benchmark_to_graphElements",
    "preprocessing.benchmark_formatting",
    "preprocessing.create_partnership_matches",
    "preprocessing.edges",
]

ROOT = Path(__file__).resolve().parent
print(f"[run_preprocessing] project root = {ROOT}")

for idx, mod in enumerate(MODULES, start=1):
    cmd = [sys.executable, "-m", mod]
    print(f"\n==> Step {idx}/{len(MODULES)}: {' '.join(shlex.quote(p) for p in cmd)}")

    result = subprocess.run(cmd)
    if result.returncode != 0:
        sys.exit(
            f"\n[ABORTED] {mod} exited with status {result.returncode}. "
            "Fix the error and re-run."
        )

print("\n✓✓ All preprocessing modules finished successfully!")
