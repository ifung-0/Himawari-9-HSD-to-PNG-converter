from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

import check_environment


PROJECT_DIR = Path(__file__).resolve().parent
REQUIREMENTS_FILE = check_environment.REQUIREMENTS_FILE


def build_command(upgrade: bool) -> list[str]:
    return check_environment.pip_install_command(upgrade=upgrade)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Install the Python requirements for the Himawari-9 processor."
    )
    parser.add_argument(
        "--upgrade",
        action="store_true",
        help="Upgrade installed packages to satisfy requirements.",
    )
    parser.add_argument(
        "--open-overlays",
        action="store_true",
        help="Open the overlays folder after ensuring/installing overlay data on Windows.",
    )
    parser.add_argument(
        "--skip-overlay-data",
        action="store_true",
        help="Install Python packages but do not download/install GSHHS/WDBII overlay data.",
    )
    parser.add_argument(
        "--force-overlay-data",
        action="store_true",
        help="Re-download/reinstall GSHHS/WDBII overlay data even if required files already exist.",
    )
    args = parser.parse_args()

    if not REQUIREMENTS_FILE.exists():
        print(f"Missing requirements file: {REQUIREMENTS_FILE}", file=sys.stderr)
        return 1

    command = build_command(args.upgrade)
    pip_result = check_environment.check_pip_available()
    if not pip_result.ok:
        print(f"Cannot install requirements: {pip_result.detail}", file=sys.stderr)
        return 1

    print("Running:", check_environment.command_text(command))
    result = subprocess.call(command, cwd=PROJECT_DIR)
    if result != 0:
        return result

    if args.skip_overlay_data:
        check_environment.ensure_overlay_folder(open_folder=args.open_overlays)
        return 0

    overlay_result = check_environment.install_overlay_data(
        open_folder=args.open_overlays,
        force=args.force_overlay_data,
    )
    return 0 if overlay_result.ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
