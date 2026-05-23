from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path


PROJECT_DIR = Path(__file__).resolve().parent
REQUIREMENTS_FILE = PROJECT_DIR / "requirements.txt"


def build_command(upgrade: bool) -> list[str]:
    command = [sys.executable, "-m", "pip", "install", "-r", str(REQUIREMENTS_FILE)]
    if upgrade:
        command.insert(4, "--upgrade")
    return command


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Install the Python requirements for the Himawari-9 processor."
    )
    parser.add_argument(
        "--upgrade",
        action="store_true",
        help="Upgrade installed packages to satisfy requirements.",
    )
    args = parser.parse_args()

    if not REQUIREMENTS_FILE.exists():
        print(f"Missing requirements file: {REQUIREMENTS_FILE}", file=sys.stderr)
        return 1

    command = build_command(args.upgrade)
    print("Running:", " ".join(f'"{part}"' if " " in part else part for part in command))
    return subprocess.call(command, cwd=PROJECT_DIR)


if __name__ == "__main__":
    raise SystemExit(main())
