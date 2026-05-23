from __future__ import annotations

import argparse
import importlib.metadata as metadata
import importlib.util
import os
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path


PROJECT_DIR = Path(__file__).resolve().parent
REQUIREMENTS_FILE = PROJECT_DIR / "requirements.txt"
VENV_DIR = PROJECT_DIR / ".venv"
MIN_SATPY_VERSION = (0, 60)
SUPPORTED_PYTHON_MAJOR = 3
SUPPORTED_PYTHON_MINOR_MIN = 12
SUPPORTED_PYTHON_MINOR_MAX = 13
TRUE_COLOR_REPRODUCTION = "true_color_reproduction"


@dataclass(frozen=True)
class PackageCheck:
    import_name: str
    distribution_name: str
    purpose: str
    critical: bool = True


@dataclass(frozen=True)
class CheckResult:
    name: str
    ok: bool
    detail: str
    critical: bool = True


PACKAGE_CHECKS = (
    PackageCheck("satpy", "satpy", "Satpy scene processing"),
    PackageCheck("dask", "dask", "lazy array processing"),
    PackageCheck("xarray", "xarray", "lazy labelled arrays"),
    PackageCheck("pyresample", "pyresample", "native/geographic resampling"),
    PackageCheck("requests", "requests", "NOAA AWS downloads"),
    PackageCheck("imageio", "imageio", "GIF/MP4 assembly"),
    PackageCheck("PIL", "pillow", "PNG image writing"),
    PackageCheck("numpy", "numpy", "array metadata and dtype handling"),
    PackageCheck("pyspectral", "pyspectral", "official true color correction"),
    PackageCheck("rasterio", "rasterio", "chunked GeoTIFF output"),
    PackageCheck("psutil", "psutil", "memory reporting", critical=False),
    PackageCheck("pycoast", "pycoast", "coastline and border overlays", critical=False),
    PackageCheck("aggdraw", "aggdraw", "pycoast overlay drawing", critical=False),
    PackageCheck("imageio_ffmpeg", "imageio-ffmpeg", "MP4 timelapse writing", critical=False),
)


def version_tuple(version: str) -> tuple[int, ...]:
    parts: list[int] = []
    for piece in version.replace("-", ".").split("."):
        digits = ""
        for ch in piece:
            if not ch.isdigit():
                break
            digits += ch
        if not digits:
            break
        parts.append(int(digits))
    return tuple(parts)


def package_version(distribution_name: str) -> str | None:
    try:
        return metadata.version(distribution_name)
    except metadata.PackageNotFoundError:
        return None


def module_available(import_name: str) -> bool:
    return importlib.util.find_spec(import_name) is not None


def supported_python_version(version_info: tuple[int, int] | None = None) -> bool:
    if version_info is None:
        version_info = (sys.version_info.major, sys.version_info.minor)
    major, minor = version_info
    return (
        major == SUPPORTED_PYTHON_MAJOR
        and SUPPORTED_PYTHON_MINOR_MIN <= minor <= SUPPORTED_PYTHON_MINOR_MAX
    )


def venv_python_path(venv_dir: Path = VENV_DIR) -> Path:
    if os.name == "nt":
        return venv_dir / "Scripts" / "python.exe"
    return venv_dir / "bin" / "python"


def command_text(command: list[str]) -> str:
    return " ".join(f'"{part}"' if " " in part else part for part in command)


def satpy_package_dir() -> Path | None:
    spec = importlib.util.find_spec("satpy")
    if spec is None or spec.origin is None:
        return None
    return Path(spec.origin).resolve().parent


def satpy_config_file(*parts: str) -> Path | None:
    package_dir = satpy_package_dir()
    if package_dir is None:
        return None
    return package_dir.joinpath("etc", *parts)


def pip_install_command(upgrade: bool = True, python_executable: str | Path | None = None) -> list[str]:
    python_executable = str(python_executable or sys.executable)
    command = [python_executable, "-m", "pip", "install", "-r", str(REQUIREMENTS_FILE)]
    if upgrade:
        command.insert(4, "--upgrade")
    return command


def launcher_exists() -> bool:
    try:
        completed = subprocess.run(
            ["py", "-0p"],
            cwd=PROJECT_DIR,
            capture_output=True,
            text=True,
            check=False,
        )
    except FileNotFoundError:
        return False
    return completed.returncode == 0


def launcher_python_command() -> list[str] | None:
    if not launcher_exists():
        return None
    for minor in range(SUPPORTED_PYTHON_MINOR_MAX, SUPPORTED_PYTHON_MINOR_MIN - 1, -1):
        command = ["py", f"-{SUPPORTED_PYTHON_MAJOR}.{minor}"]
        completed = subprocess.run(
            command + ["-c", "import sys; print(sys.executable)"],
            cwd=PROJECT_DIR,
            capture_output=True,
            text=True,
            check=False,
        )
        if completed.returncode == 0:
            return command
    return None


def create_venv(venv_dir: Path = VENV_DIR) -> Path | None:
    launcher_command = launcher_python_command()
    if launcher_command is None:
        print(
            "Could not find Python 3.12 or 3.13 with the Windows 'py' launcher. "
            "Install Python 3.12/3.13, then rerun this script with --auto.",
            file=sys.stderr,
        )
        return None
    command = launcher_command + ["-m", "venv", str(venv_dir)]
    print("Creating virtual environment:", command_text(command))
    result = subprocess.call(command, cwd=PROJECT_DIR)
    if result != 0:
        return None
    python_path = venv_python_path(venv_dir)
    if not python_path.exists():
        print(f"Virtual environment was created but Python was not found at {python_path}", file=sys.stderr)
        return None
    return python_path


def check_packages() -> list[CheckResult]:
    results = []
    for package in PACKAGE_CHECKS:
        version = package_version(package.distribution_name)
        if module_available(package.import_name):
            shown_version = version or "version unknown"
            results.append(
                CheckResult(
                    package.import_name,
                    True,
                    f"{shown_version} ({package.purpose})",
                    critical=package.critical,
                )
            )
        else:
            results.append(
                CheckResult(
                    package.import_name,
                    False,
                    f"missing; needed for {package.purpose}",
                    critical=package.critical,
                )
            )
    return results


def check_satpy_version() -> CheckResult:
    version = package_version("satpy")
    if version is None:
        return CheckResult("satpy version", False, "Satpy is not installed.")
    if version_tuple(version) >= MIN_SATPY_VERSION:
        return CheckResult("satpy version", True, f"{version} >= {'.'.join(map(str, MIN_SATPY_VERSION))}")
    return CheckResult(
        "satpy version",
        False,
        f"{version} is older than {'.'.join(map(str, MIN_SATPY_VERSION))}; upgrade Satpy.",
    )


def check_satpy_ahi_configs() -> list[CheckResult]:
    results = []
    reader_config = satpy_config_file("readers", "ahi_hsd.yaml")
    composite_config = satpy_config_file("composites", "ahi.yaml")

    if reader_config is None:
        return [CheckResult("satpy config path", False, "Satpy package path could not be found.")]

    results.append(
        CheckResult(
            "ahi_hsd reader config",
            bool(reader_config and reader_config.exists()),
            str(reader_config) if reader_config and reader_config.exists() else f"missing: {reader_config}",
        )
    )

    if not composite_config or not composite_config.exists():
        results.append(
            CheckResult(
                "AHI composite config",
                False,
                f"missing: {composite_config}",
            )
        )
        return results

    text = composite_config.read_text(encoding="utf-8", errors="replace")
    has_true_color = "true_color:" in text
    has_reproduction = f"{TRUE_COLOR_REPRODUCTION}:" in text
    results.append(CheckResult("AHI true_color composite", has_true_color, str(composite_config)))
    results.append(
        CheckResult(
            "AHI true_color_reproduction composite",
            has_reproduction,
            (
                str(composite_config)
                if has_reproduction
                else (
                    f"{TRUE_COLOR_REPRODUCTION!r} not found in {composite_config}; "
                    "run --auto/--fix to repair Satpy. The app can use a custom low-RAM fallback."
                )
            ),
            critical=False,
        )
    )
    return results


def check_project_import() -> CheckResult:
    try:
        import himawari_lowram_processor as app
    except Exception as exc:
        return CheckResult("project import", False, f"failed to import himawari_lowram_processor: {exc}")

    mapped_name = app.SATPY_COMPOSITE_NAMES.get("True Color Reproduction Image")
    has_fallback = "True Color Reproduction Image" in app.CUSTOM_DATASET_NAMES
    if mapped_name == TRUE_COLOR_REPRODUCTION and has_fallback:
        return CheckResult(
            "project true color mapping",
            True,
            f"maps to {mapped_name!r}; custom fallback available",
        )
    return CheckResult(
        "project true color mapping",
        False,
        f"expected {TRUE_COLOR_REPRODUCTION!r} with custom fallback, found {mapped_name!r}",
    )


def run_checks() -> list[CheckResult]:
    python_ok = supported_python_version()
    results = [
        CheckResult("python executable", True, sys.executable, critical=False),
        CheckResult(
            "python version",
            python_ok,
            (
                f"{sys.version.split()[0]} supported"
                if python_ok
                else f"{sys.version.split()[0]} is not recommended; use Python 3.12 or 3.13"
            ),
            critical=True,
        ),
        CheckResult("requirements file", REQUIREMENTS_FILE.exists(), str(REQUIREMENTS_FILE)),
    ]
    results.extend(check_packages())
    results.append(check_satpy_version())
    results.extend(check_satpy_ahi_configs())
    results.append(check_project_import())
    return results


def has_critical_failures(results: list[CheckResult]) -> bool:
    return any(not result.ok and result.critical for result in results)


def print_results(results: list[CheckResult]) -> None:
    for result in results:
        label = "OK" if result.ok else ("FAIL" if result.critical else "WARN")
        print(f"[{label}] {result.name}: {result.detail}")


def has_failures(results: list[CheckResult]) -> bool:
    return any(not result.ok for result in results)


def run_fix(python_executable: str | Path | None = None) -> int:
    if not REQUIREMENTS_FILE.exists():
        print(f"Cannot repair: missing requirements file: {REQUIREMENTS_FILE}", file=sys.stderr)
        return 1
    command = pip_install_command(upgrade=True, python_executable=python_executable)
    print("Running:", command_text(command))
    return subprocess.call(command, cwd=PROJECT_DIR)


def run_environment_check_with(python_executable: str | Path) -> int:
    command = [str(python_executable), str(Path(__file__).resolve())]
    print("Checking repaired environment:", command_text(command))
    return subprocess.call(command, cwd=PROJECT_DIR)


def run_auto_fix(results: list[CheckResult]) -> int:
    python_result = next((result for result in results if result.name == "python version"), None)
    if python_result is not None and not python_result.ok:
        python_path = venv_python_path()
        if not python_path.exists():
            created_path = create_venv()
            if created_path is None:
                return 1
            python_path = created_path
        fix_code = run_fix(python_path)
        if fix_code != 0:
            return fix_code
        print()
        print("A supported virtual environment is ready.")
        print("Launch the GUI with:")
        print(command_text([str(python_path), str(PROJECT_DIR / "himawari_lowram_processor.py")]))
        print("Check it with:")
        print(command_text([str(python_path), str(PROJECT_DIR / "check_environment.py")]))
        print()
        return run_environment_check_with(python_path)

    return run_fix()


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Diagnose and repair the Python environment used by the Himawari-9 processor."
    )
    parser.add_argument(
        "--fix",
        action="store_true",
        help="Run pip install --upgrade -r requirements.txt with this Python, then re-check.",
    )
    parser.add_argument(
        "--auto",
        action="store_true",
        help=(
            "Automatically repair the environment. If Python is unsupported, create/use .venv "
            "with Python 3.12/3.13 from the Windows py launcher."
        ),
    )
    args = parser.parse_args()

    print("Himawari-9 processor environment check")
    print(f"Project: {PROJECT_DIR}")
    print()

    results = run_checks()
    print_results(results)

    if args.auto and has_failures(results):
        print()
        fix_code = run_auto_fix(results)
        if fix_code != 0:
            return fix_code
        print()
        if supported_python_version():
            print("Re-checking after repair...")
            results = run_checks()
            print_results(results)
    elif args.fix and has_failures(results):
        print()
        fix_code = run_fix()
        if fix_code != 0:
            return fix_code
        print()
        print("Re-checking after repair...")
        results = run_checks()
        print_results(results)

    if has_critical_failures(results):
        print()
        print("One or more critical checks failed. Try automatic repair with:")
        print(command_text([sys.executable, str(Path(__file__).resolve()), "--auto"]))
        print("Or install requirements with:")
        print(command_text(pip_install_command(upgrade=True)))
        return 1

    if has_failures(results):
        print()
        print("Only optional checks failed. To install optional helpers too, run:")
        print(command_text([sys.executable, str(Path(__file__).resolve()), "--fix"]))
        return 0

    print()
    print("Environment looks ready for true color reproduction and low-RAM processing.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
