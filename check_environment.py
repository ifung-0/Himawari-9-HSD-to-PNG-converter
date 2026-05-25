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
TRUE_COLOR = "true_color"
TRUE_COLOR_UI = "True Color RGB (Enhanced)"
TRUE_COLOR_REPRODUCTION = "true_color_reproduction"
TRUE_COLOR_REPRODUCTION_UI = "True Color Reproduction Image"
GROUP_ORDER = ("Python", "Packages", "Satpy", "Project")


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


def satpy_config_environment_detail() -> str:
    values = []
    for name in ("SATPY_CONFIG_PATH", "PPP_CONFIG_DIR"):
        value = os.environ.get(name)
        if value:
            values.append(f"{name}={value}")
    return "; ".join(values) if values else "no custom Satpy config environment variables"


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


def satpy_compositor_names_for_sensor(sensor: str = "ahi") -> set[str]:
    from satpy.composites.config_loader import load_compositor_configs_for_sensors

    composites, _modifiers = load_compositor_configs_for_sensors({sensor})
    sensor_composites = composites.get(sensor, {})
    names = set()
    for data_id in sensor_composites:
        try:
            names.add(data_id["name"])
        except Exception:
            names.add(getattr(data_id, "name", str(data_id)))
    return names


def check_satpy_true_color_registry() -> CheckResult:
    try:
        names = satpy_compositor_names_for_sensor("ahi")
    except Exception as exc:
        return CheckResult(
            "Satpy parsed true_color_reproduction",
            False,
            f"could not parse active Satpy AHI compositor configs: {exc}; {satpy_config_environment_detail()}",
            critical=False,
        )

    if TRUE_COLOR_REPRODUCTION in names:
        return CheckResult(
            "Satpy parsed true_color_reproduction",
            True,
            f"available in active Satpy compositor registry; {satpy_config_environment_detail()}",
            critical=False,
        )
    matching = ", ".join(sorted(name for name in names if "true_color" in name)) or "none"
    return CheckResult(
        "Satpy parsed true_color_reproduction",
        False,
        (
            f"{TRUE_COLOR_REPRODUCTION!r} was not found in Satpy's parsed AHI compositor registry. "
            f"Other true_color entries: {matching}. {satpy_config_environment_detail()}. "
            "The app will use its custom low-RAM fallback for this product."
        ),
        critical=False,
    )


def check_project_true_color_fallback_runtime() -> CheckResult:
    try:
        import himawari_lowram_processor as app
    except Exception as exc:
        return CheckResult("project true color fallback runtime", False, f"project import failed: {exc}")

    products = (
        (TRUE_COLOR_UI, TRUE_COLOR),
        (TRUE_COLOR_REPRODUCTION_UI, TRUE_COLOR_REPRODUCTION),
    )
    missing_routes = []
    for ui_name, satpy_name in products:
        missing_exc = KeyError(f"\"No dataset matching 'DataQuery(name='{satpy_name}')' found\"")
        mapped_name = app.SATPY_COMPOSITE_NAMES.get(ui_name)
        has_custom_name = ui_name in app.CUSTOM_DATASET_NAMES
        detects_missing = bool(
            mapped_name
            and has_custom_name
            and app.use_custom_satpy_missing_dataset_fallback(
                missing_exc,
                ui_name,
                mapped_name,
            )
        )
        if not detects_missing:
            missing_routes.append(f"{ui_name} -> {satpy_name}")

    if not missing_routes:
        return CheckResult(
            "project true color fallback runtime",
            True,
            "missing Satpy true_color and true_color_reproduction errors are routed to custom low-RAM fallbacks",
        )
    return CheckResult(
        "project true color fallback runtime",
        False,
        "missing Satpy dataset errors would not be caught by the app fallback for: "
        + ", ".join(missing_routes),
    )


def check_project_import() -> CheckResult:
    try:
        import himawari_lowram_processor as app
    except Exception as exc:
        return CheckResult("project import", False, f"failed to import himawari_lowram_processor: {exc}")

    expected = {
        TRUE_COLOR_UI: TRUE_COLOR,
        TRUE_COLOR_REPRODUCTION_UI: TRUE_COLOR_REPRODUCTION,
    }
    mismatches = [
        f"{ui_name}: expected {satpy_name!r}, found {app.SATPY_COMPOSITE_NAMES.get(ui_name)!r}"
        for ui_name, satpy_name in expected.items()
        if app.SATPY_COMPOSITE_NAMES.get(ui_name) != satpy_name
    ]
    missing_fallbacks = [
        ui_name
        for ui_name in expected
        if ui_name not in app.CUSTOM_DATASET_NAMES
        or ui_name not in app.CUSTOM_SATPY_MISSING_DATASET_FALLBACKS
    ]
    if not mismatches and not missing_fallbacks:
        return CheckResult(
            "project true color mapping",
            True,
            "true_color and true_color_reproduction mappings have custom fallbacks available",
        )
    details = mismatches + [f"missing custom fallback for {name}" for name in missing_fallbacks]
    return CheckResult(
        "project true color mapping",
        False,
        "; ".join(details),
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
    results.append(check_satpy_true_color_registry())
    results.append(check_project_import())
    results.append(check_project_true_color_fallback_runtime())
    return results


def has_critical_failures(results: list[CheckResult]) -> bool:
    return any(not result.ok and result.critical for result in results)


def result_group(result: CheckResult) -> str:
    package_names = {package.import_name for package in PACKAGE_CHECKS}
    if result.name in {"python executable", "python version", "requirements file"}:
        return "Python"
    if result.name in package_names:
        return "Packages"
    if result.name.startswith("satpy") or result.name.startswith("Satpy") or "AHI" in result.name:
        return "Satpy"
    return "Project"


def result_counts(results: list[CheckResult]) -> tuple[int, int, int]:
    ok = sum(1 for result in results if result.ok)
    warnings = sum(1 for result in results if not result.ok and not result.critical)
    failures = sum(1 for result in results if not result.ok and result.critical)
    return ok, warnings, failures


def status_label(result: CheckResult) -> str:
    if result.ok:
        return "OK"
    return "FAIL" if result.critical else "WARN"


def print_banner() -> None:
    print("Himawari-9 processor environment check")
    print(f"Project: {PROJECT_DIR}")
    print(f"Python:  {sys.executable}")
    print()


def print_summary(results: list[CheckResult]) -> None:
    ok, warnings, failures = result_counts(results)
    print()
    print(f"Summary: {ok} OK, {warnings} warning(s), {failures} critical failure(s)")


def print_results(results: list[CheckResult], grouped: bool = True) -> None:
    if not grouped:
        for result in results:
            print(f"[{status_label(result)}] {result.name}: {result.detail}")
        print_summary(results)
        return

    grouped_results: dict[str, list[CheckResult]] = {group: [] for group in GROUP_ORDER}
    for result in results:
        grouped_results.setdefault(result_group(result), []).append(result)

    for group in GROUP_ORDER:
        items = grouped_results.get(group, [])
        if not items:
            continue
        name_width = max(len(result.name) for result in items)
        print(f"{group}:")
        for result in items:
            print(f"  [{status_label(result):4}] {result.name:<{name_width}}  {result.detail}")
        print()
    print_summary(results)


def print_next_steps(results: list[CheckResult]) -> None:
    if has_critical_failures(results):
        failed = [result.name for result in results if not result.ok and result.critical]
        print()
        print("Next steps:")
        print("  Critical checks failed: " + ", ".join(failed))
        print("  1. Try automatic repair:")
        print("     " + command_text([sys.executable, str(Path(__file__).resolve()), "--auto"]))
        print("  2. If that fails, install requirements with:")
        print("     " + command_text(pip_install_command(upgrade=True)))
        print("  3. Re-run this checker, then launch the GUI or CLI with the same Python shown above.")
        return

    if has_failures(results):
        warnings = [result.name for result in results if not result.ok and not result.critical]
        print()
        print("Next steps:")
        print("  Optional checks need attention: " + ", ".join(warnings))
        print("  Core processing can still run. Features such as memory reporting,")
        print("  border overlays, MP4 writing, or official Satpy true color may be limited.")
        print("  To repair optional helpers, run:")
        print("     " + command_text([sys.executable, str(Path(__file__).resolve()), "--fix"]))
        print("  You can also use the custom low-RAM true color fallback from the GUI or CLI.")
        return

    print()
    print("Environment looks ready for true color reproduction and low-RAM processing.")
    print("Use run_gui.bat for the desktop app or run_cli.bat for the terminal interface.")


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
    parser.add_argument(
        "--plain",
        action="store_true",
        help="Print compact one-line-per-check output.",
    )
    args = parser.parse_args()

    print_banner()

    results = run_checks()
    print_results(results, grouped=not args.plain)

    if args.auto and has_failures(results):
        print()
        fix_code = run_auto_fix(results)
        if fix_code != 0:
            return fix_code
        print()
        if supported_python_version():
            print("Re-checking after repair...")
            results = run_checks()
            print_results(results, grouped=not args.plain)
    elif args.fix and has_failures(results):
        print()
        fix_code = run_fix()
        if fix_code != 0:
            return fix_code
        print()
        print("Re-checking after repair...")
        results = run_checks()
        print_results(results, grouped=not args.plain)

    if has_critical_failures(results):
        print_next_steps(results)
        return 1

    if has_failures(results):
        print_next_steps(results)
        return 0

    print_next_steps(results)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
