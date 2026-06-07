from __future__ import annotations

import argparse
import importlib
import importlib.metadata as metadata
import importlib.util
import json
import os
import shutil
import subprocess
import sys
import tempfile
import urllib.error
import urllib.request
import warnings
import zipfile
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path


PROJECT_DIR = Path(__file__).resolve().parent
REQUIREMENTS_FILE = PROJECT_DIR / "requirements.txt"
GPU_REQUIREMENTS_FILE = PROJECT_DIR / "requirements-gpu.txt"
VENV_DIR = PROJECT_DIR / ".venv"
LOCAL_APP_DATA_DIR = Path(os.environ.get("LOCALAPPDATA") or (Path.home() / "AppData" / "Local"))
APP_DATA_DIR = LOCAL_APP_DATA_DIR / "Himawari9LowRamProcessor"
APP_IMPORT_NAME = "himawari_lowram_processor"
MIN_SATPY_VERSION = (0, 60)
OVERLAY_RESOLUTION = "l"
OVERLAY_LEVEL = 1
GSHHG_SHAPEFILE_ARCHIVE = "gshhg-shp-2.3.7.zip"
GSHHG_ARCHIVE_URLS = (
    "https://www.soest.hawaii.edu/pwessel/gshhg/gshhg-shp-2.3.7.zip",
    "https://ftp.soest.hawaii.edu/gshhg/gshhg-shp-2.3.7.zip",
    "http://www.soest.hawaii.edu/pwessel/gshhg/gshhg-shp-2.3.7.zip",
)
OVERLAY_CACHE_DIR = APP_DATA_DIR / "cache" / "overlays"
SHAPEFILE_SIDECAR_SUFFIXES = (".shp", ".dbf", ".shx", ".prj", ".cpg")
SUPPORTED_PYTHON_MAJOR = 3
SUPPORTED_PYTHON_MINOR_MIN = 12
SUPPORTED_PYTHON_MINOR_MAX = 13
TRUE_COLOR = "true_color"
TRUE_COLOR_UI = "True Color RGB (Enhanced)"
TRUE_COLOR_REPRODUCTION = "true_color_reproduction"
TRUE_COLOR_REPRODUCTION_UI = "True Color Reproduction Image"
GROUP_ORDER = ("Python", "Packages", "GPU", "Satpy", "Project", "Project Cleanup", "Overlays", "Paths")
SATPY_CONFIG_ENV_VARS = ("SATPY_CONFIG_PATH", "PPP_CONFIG_DIR")
CLOUD_SYNC_PREFIXES = ("onedrive", "dropbox", "google drive", "icloud")
CLOUD_SYNC_EXACT = ("box",)
LAUNCHER_HELPERS = {
    "run_gui.bat": "himawari_lowram_processor.py",
    "run_cli.bat": "himawari_cli.py",
    "runcli.bat": "run_cli.bat",
    "check_environment.bat": "check_environment.py",
    "checkenv.bat": "check_environment.bat",
}
RUNTIME_JSON_FILES = (
    "himawari_gui_settings.json",
    "himawari_recent_runs.json",
    "himawari_custom_presets.json",
)
CORE_ROOT_FILES = {
    ".gitignore",
    "check_environment.bat",
    "check_environment.py",
    "checkenv.bat",
    "himawari_cli.py",
    "himawari_lowram_processor.py",
    "install_requirements.py",
    "LICENSE",
    "README.md",
    "requirements-gpu.txt",
    "requirements.txt",
    "run_cli.bat",
    "runcli.bat",
    "run_gui.bat",
    *RUNTIME_JSON_FILES,
}
ROOT_CLEANUP_EXTENSIONS = {".py", ".bat", ".ipynb"}
KNOWN_UNUSED_ROOT_FILES = {
    "HW_9_to_png_tiff 2.(old version)ipynb": "old notebook replaced by the low-RAM processor",
    "tiff_to_png_converter.py": "standalone TIFF converter is not part of the supported app workflow",
    "himawari_dashboard.py": "removed dashboard entrypoint is out of scope for the HSD processor",
    "himawari_l2_analytics.py": "removed L2 analytics entrypoint is out of scope for the HSD processor",
    "run_dashboard.bat": "removed dashboard launcher is out of scope for the HSD processor",
}
PROTECTED_ROOT_DIRS = {
    ".git",
    ".venv",
    "venv",
    "__pycache__",
    ".cache",
    ".pytest_cache",
    ".mypy_cache",
    ".ruff_cache",
    "cleanup_archive",
    "outputs",
    "overlays",
    "temp",
    "tests",
}


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


@dataclass(frozen=True)
class OverlayInstallResult:
    ok: bool
    installed: bool
    detail: str
    attempted_urls: tuple[str, ...] = ()
    missing_paths: tuple[Path, ...] = ()


@dataclass(frozen=True)
class CleanupCandidate:
    path: Path
    reason: str


PACKAGE_CHECKS = (
    PackageCheck("satpy", "satpy", "Satpy scene processing"),
    PackageCheck("dask", "dask", "lazy array processing"),
    PackageCheck("xarray", "xarray", "lazy labelled arrays"),
    PackageCheck("pyresample", "pyresample", "native/geographic resampling"),
    PackageCheck("pyproj", "pyproj", "Web Mercator flat-map projection"),
    PackageCheck("requests", "requests", "NOAA AWS downloads"),
    PackageCheck("imageio", "imageio", "GIF/MP4 assembly"),
    PackageCheck("PIL", "pillow", "PNG image writing"),
    PackageCheck("numpy", "numpy", "array metadata and dtype handling"),
    PackageCheck("scipy", "scipy", "Dask array helpers and scientific routines"),
    PackageCheck("pyspectral", "pyspectral", "official true color correction"),
    PackageCheck("rasterio", "rasterio", "chunked GeoTIFF output"),
    PackageCheck("psutil", "psutil", "memory reporting", critical=False),
    PackageCheck("pycoast", "pycoast", "coastline and border overlays", critical=False),
    PackageCheck("aggdraw", "aggdraw", "pycoast overlay drawing", critical=False),
    PackageCheck("imageio_ffmpeg", "imageio-ffmpeg", "MP4 timelapse writing", critical=False),
    PackageCheck("tkinterdnd2", "tkinterdnd2", "local file drag/drop import", critical=False),
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


def module_import_result(import_name: str) -> tuple[bool, str]:
    try:
        if importlib.util.find_spec(import_name) is None:
            return False, "module is not importable"
    except Exception as exc:
        return False, f"module probe failed: {exc.__class__.__name__}: {exc}"

    try:
        with suppressed_known_environment_warnings():
            importlib.import_module(import_name)
    except Exception as exc:
        return False, f"import failed: {exc.__class__.__name__}: {exc}"
    return True, "importable"


@contextmanager
def suppressed_known_environment_warnings():
    with warnings.catch_warnings():
        configure_known_warning_filters()
        yield


def configure_known_warning_filters() -> None:
    warnings.filterwarnings(
        "ignore",
        message="CUDA path could not be detected.*",
        category=UserWarning,
        module=r"cupy\._environment",
    )


def module_available(import_name: str) -> bool:
    return module_import_result(import_name)[0]


def normalize_distribution_name(name: str) -> str:
    return name.lower().replace("_", "-")


def minimum_versions_from_requirements(requirements_file: Path = REQUIREMENTS_FILE) -> dict[str, str]:
    if not requirements_file.exists():
        return {}

    minimums: dict[str, str] = {}
    for raw_line in requirements_file.read_text(encoding="utf-8", errors="replace").splitlines():
        line = raw_line.split("#", 1)[0].strip()
        if not line or line.startswith("-"):
            continue
        line = line.split(";", 1)[0].strip()
        if ">=" not in line:
            continue
        name, version = line.split(">=", 1)
        name = name.split("[", 1)[0].strip()
        version = version.split(",", 1)[0].strip()
        if name and version:
            minimums[normalize_distribution_name(name)] = version
    return minimums


def minimum_version_for(package: PackageCheck, minimums: dict[str, str]) -> str | None:
    return minimums.get(normalize_distribution_name(package.distribution_name))


def version_meets_minimum(version: str, minimum: str) -> bool:
    return version_tuple(version) >= version_tuple(minimum)


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
    for name in SATPY_CONFIG_ENV_VARS:
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


def gpu_pip_install_command(upgrade: bool = True, python_executable: str | Path | None = None) -> list[str]:
    python_executable = str(python_executable or sys.executable)
    command = [python_executable, "-m", "pip", "install", "-r", str(GPU_REQUIREMENTS_FILE)]
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


def check_package(package: PackageCheck, minimums: dict[str, str]) -> CheckResult:
    version = package_version(package.distribution_name)
    minimum = minimum_version_for(package, minimums)

    if version is not None and minimum is not None and not version_meets_minimum(version, minimum):
        return CheckResult(
            package.import_name,
            False,
            f"{version} is older than required {minimum}; needed for {package.purpose}",
            critical=package.critical,
        )

    import_ok, import_detail = module_import_result(package.import_name)

    if not import_ok:
        installed = f"; installed distribution version {version}" if version else ""
        return CheckResult(
            package.import_name,
            False,
            f"{import_detail}; needed for {package.purpose}{installed}",
            critical=package.critical,
        )

    if version is None:
        return CheckResult(
            package.import_name,
            False,
            (
                f"importable, but {package.distribution_name} package metadata was not found; "
                "reinstall requirements with this Python"
            ),
            critical=package.critical,
        )

    shown_version = f"{version} >= {minimum}" if minimum else version
    return CheckResult(
        package.import_name,
        True,
        f"{shown_version} ({package.purpose})",
        critical=package.critical,
    )


def check_packages() -> list[CheckResult]:
    minimums = minimum_versions_from_requirements()
    return [check_package(package, minimums) for package in PACKAGE_CHECKS]


def check_gpu_support() -> list[CheckResult]:
    results = [
        CheckResult("GPU requirements file", GPU_REQUIREMENTS_FILE.exists(), str(GPU_REQUIREMENTS_FILE), critical=False)
    ]
    if importlib.util.find_spec("cupy") is None:
        results.append(
            CheckResult(
                "CuPy GPU package",
                False,
                "CuPy is not installed; run --gpu-fix to install optional NVIDIA/CUDA GPU support",
                critical=False,
            )
        )
        return results
    try:
        import cupy as cp  # type: ignore
    except Exception as exc:
        results.append(CheckResult("CuPy GPU package", False, f"import failed: {exc}", critical=False))
        return results

    version = str(getattr(cp, "__version__", "unknown"))
    results.append(CheckResult("CuPy GPU package", True, f"CuPy {version} importable", critical=False))
    try:
        device_count = int(cp.cuda.runtime.getDeviceCount())
    except Exception as exc:
        results.append(CheckResult("CUDA GPU device", False, f"device check failed: {exc}", critical=False))
        return results
    if device_count <= 0:
        results.append(CheckResult("CUDA GPU device", False, "no CUDA device found", critical=False))
        return results
    try:
        props = cp.cuda.runtime.getDeviceProperties(0)
        raw_name = props.get("name", b"")
        device_name = raw_name.decode("utf-8", errors="replace") if isinstance(raw_name, bytes) else str(raw_name)
    except Exception:
        device_name = "CUDA device 0"
    results.append(CheckResult("CUDA GPU device", True, f"{device_name} ({device_count} device(s))", critical=False))
    try:
        test = cp.asarray([1], dtype=cp.float32)
        test = (test + cp.float32(1.0)).astype(cp.float32)
        cp.cuda.Stream.null.synchronize()
        if float(cp.asnumpy(test)[0]) != 2.0:
            raise RuntimeError("unexpected CUDA test result")
        del test
        cp.get_default_memory_pool().free_all_blocks()
    except Exception as exc:
        results.append(
            CheckResult(
                "CUDA kernel test",
                False,
                f"kernel test failed: {exc}; run --gpu-fix to install CuPy with CUDA toolkit headers",
                critical=False,
            )
        )
    else:
        results.append(CheckResult("CUDA kernel test", True, "small CuPy kernel operation succeeded", critical=False))
    return results


def overlay_data_required_paths(project_dir: Path = PROJECT_DIR) -> tuple[Path, ...]:
    overlays_dir = project_dir / "overlays"
    return (
        overlays_dir / "GSHHS_shp" / OVERLAY_RESOLUTION / f"GSHHS_{OVERLAY_RESOLUTION}_L{OVERLAY_LEVEL}.shp",
        overlays_dir / "GSHHS_shp" / OVERLAY_RESOLUTION / f"GSHHS_{OVERLAY_RESOLUTION}_L{OVERLAY_LEVEL}.dbf",
        overlays_dir
        / "WDBII_shp"
        / OVERLAY_RESOLUTION
        / f"WDBII_border_{OVERLAY_RESOLUTION}_L{OVERLAY_LEVEL}.shp",
        overlays_dir
        / "WDBII_shp"
        / OVERLAY_RESOLUTION
        / f"WDBII_border_{OVERLAY_RESOLUTION}_L{OVERLAY_LEVEL}.dbf",
    )


def overlay_required_base_paths(project_dir: Path = PROJECT_DIR) -> tuple[Path, ...]:
    overlays_dir = project_dir / "overlays"
    return (
        overlays_dir / "GSHHS_shp" / OVERLAY_RESOLUTION / f"GSHHS_{OVERLAY_RESOLUTION}_L{OVERLAY_LEVEL}",
        overlays_dir / "WDBII_shp" / OVERLAY_RESOLUTION / f"WDBII_border_{OVERLAY_RESOLUTION}_L{OVERLAY_LEVEL}",
    )


def overlay_archive_member_suffixes() -> tuple[str, ...]:
    suffixes = []
    for base in (
        f"GSHHS_shp/{OVERLAY_RESOLUTION}/GSHHS_{OVERLAY_RESOLUTION}_L{OVERLAY_LEVEL}",
        f"WDBII_shp/{OVERLAY_RESOLUTION}/WDBII_border_{OVERLAY_RESOLUTION}_L{OVERLAY_LEVEL}",
    ):
        suffixes.extend(f"{base}{suffix}" for suffix in SHAPEFILE_SIDECAR_SUFFIXES)
    return tuple(suffixes)


def overlay_data_layout_text(project_dir: Path = PROJECT_DIR) -> str:
    sidecars = tuple(base.with_suffix(".shx") for base in overlay_required_base_paths(project_dir))
    return "\n".join(str(path) for path in (*overlay_data_required_paths(project_dir), *sidecars))


def missing_overlay_data_paths(project_dir: Path = PROJECT_DIR) -> tuple[Path, ...]:
    return tuple(path for path in overlay_data_required_paths(project_dir) if not path.exists() or path.stat().st_size <= 0)


def missing_overlay_sidecar_paths(project_dir: Path = PROJECT_DIR) -> tuple[Path, ...]:
    missing = []
    for base in overlay_required_base_paths(project_dir):
        sidecar = base.with_suffix(".shx")
        if not sidecar.exists() or sidecar.stat().st_size <= 0:
            missing.append(sidecar)
    return tuple(missing)


def check_overlay_data(project_dir: Path = PROJECT_DIR) -> CheckResult:
    overlays_dir = project_dir / "overlays"
    missing = missing_overlay_data_paths(project_dir)
    missing_sidecars = missing_overlay_sidecar_paths(project_dir)
    if missing or missing_sidecars:
        folder_note = "overlays/ folder is missing; " if not overlays_dir.exists() else ""
        missing_text = "; ".join(str(path) for path in missing)
        if missing_sidecars:
            sidecar_text = "; ".join(str(path) for path in missing_sidecars)
            missing_text = (missing_text + "; " if missing_text else "") + "missing/empty sidecar file(s): " + sidecar_text
        return CheckResult(
            "overlay data files",
            False,
            (
                folder_note
                + "border lines require pycoast-compatible GSHHS/WDBII files at: "
                + missing_text
                + "; Quick Fix can download and install the needed low-resolution overlay files"
            ),
            critical=False,
        )
    return CheckResult(
        "overlay data files",
        True,
        f"required low-resolution GSHHS/WDBII files found under {overlays_dir}",
        critical=False,
    )


def check_pycoast_overlay_runtime(project_dir: Path = PROJECT_DIR) -> CheckResult:
    missing = (*missing_overlay_data_paths(project_dir), *missing_overlay_sidecar_paths(project_dir))
    if missing:
        return CheckResult(
            "pycoast overlay runtime",
            False,
            "skipped until overlay data files are installed",
            critical=False,
        )
    missing_packages = [
        module
        for module in ("pycoast", "aggdraw")
        if importlib.util.find_spec(module) is None
    ]
    if missing_packages:
        return CheckResult(
            "pycoast overlay runtime",
            False,
            "missing overlay package(s): " + ", ".join(missing_packages),
            critical=False,
        )
    try:
        import aggdraw  # noqa: F401
        import pycoast  # noqa: F401
    except Exception as exc:
        return CheckResult(
            "pycoast overlay runtime",
            False,
            f"overlay imports failed: {exc}",
            critical=False,
        )
    return CheckResult(
        "pycoast overlay runtime",
        True,
        "pycoast and aggdraw import successfully with required overlay data present",
        critical=False,
    )


def ensure_overlay_folder(project_dir: Path = PROJECT_DIR, open_folder: bool = False) -> Path:
    overlays_dir = project_dir / "overlays"
    overlays_dir.mkdir(parents=True, exist_ok=True)
    print()
    print(f"Overlay folder is ready: {overlays_dir}")
    print("Border overlays also need pycoast-compatible GSHHS/WDBII data at:")
    for path in (*overlay_data_required_paths(project_dir), *missing_overlay_sidecar_paths(project_dir)):
        print(f"  {path}")
    print("Quick Fix can download and install the needed low-resolution shapefiles automatically.")
    if open_folder:
        open_path_in_file_manager(overlays_dir)
    return overlays_dir


def open_path_in_file_manager(path: Path) -> None:
    if os.name != "nt":
        return
    try:
        os.startfile(str(path))
    except OSError as exc:
        print(f"Could not open overlay folder: {exc}", file=sys.stderr)


def open_overlay_folder(project_dir: Path = PROJECT_DIR) -> None:
    overlays_dir = project_dir / "overlays"
    open_path_in_file_manager(overlays_dir)


def download_file(urls: tuple[str, ...], destination: Path, timeout: int = 60) -> tuple[bool, str]:
    destination.parent.mkdir(parents=True, exist_ok=True)
    errors = []
    for url in urls:
        print(f"Downloading overlay data: {url}")
        tmp_path = destination.with_suffix(destination.suffix + ".part")
        try:
            with urllib.request.urlopen(url, timeout=timeout) as response:
                total_text = response.headers.get("Content-Length")
                total = int(total_text) if total_text and total_text.isdigit() else None
                downloaded = 0
                with tmp_path.open("wb") as handle:
                    while True:
                        chunk = response.read(1024 * 1024)
                        if not chunk:
                            break
                        handle.write(chunk)
                        downloaded += len(chunk)
                        if total:
                            print(f"  {downloaded / total:.0%}", end="\r")
                if total:
                    print("  100%")
            tmp_path.replace(destination)
            return True, url
        except (OSError, urllib.error.URLError, TimeoutError) as exc:
            errors.append(f"{url}: {exc}")
            try:
                tmp_path.unlink(missing_ok=True)
            except OSError:
                pass
    return False, "; ".join(errors)


def archive_members_by_suffix(zip_file: zipfile.ZipFile, suffixes: tuple[str, ...]) -> dict[str, str]:
    normalized_suffixes = {suffix.replace("\\", "/").lower(): suffix for suffix in suffixes}
    matches: dict[str, str] = {}
    for member in zip_file.namelist():
        normalized_member = member.replace("\\", "/").lower()
        for normalized_suffix, original_suffix in normalized_suffixes.items():
            if normalized_member.endswith(normalized_suffix):
                matches[original_suffix] = member
    return matches


def extract_overlay_archive(archive_path: Path, project_dir: Path = PROJECT_DIR, force: bool = False) -> tuple[int, tuple[Path, ...]]:
    extracted = 0
    required_suffixes = overlay_archive_member_suffixes()
    overlays_dir = project_dir / "overlays"
    try:
        with zipfile.ZipFile(archive_path) as archive:
            matches = archive_members_by_suffix(archive, required_suffixes)
            missing_required = [
                suffix
                for suffix in required_suffixes
                if suffix.endswith((".shp", ".dbf")) and suffix not in matches
            ]
            if missing_required:
                missing_text = ", ".join(missing_required)
                raise RuntimeError(f"archive is missing required overlay members: {missing_text}")

            for suffix, member in matches.items():
                destination = overlays_dir / Path(suffix)
                if destination.exists() and not force:
                    continue
                destination.parent.mkdir(parents=True, exist_ok=True)
                with archive.open(member) as source, destination.open("wb") as target:
                    target.write(source.read())
                extracted += 1
    except zipfile.BadZipFile as exc:
        raise RuntimeError(f"overlay archive is not a valid ZIP file: {archive_path}") from exc

    return extracted, (*missing_overlay_data_paths(project_dir), *missing_overlay_sidecar_paths(project_dir))


def install_overlay_data(
    project_dir: Path = PROJECT_DIR,
    open_folder: bool = True,
    force: bool = False,
    archive_path: Path | None = None,
    urls: tuple[str, ...] = GSHHG_ARCHIVE_URLS,
) -> OverlayInstallResult:
    overlays_dir = ensure_overlay_folder(project_dir, open_folder=False)
    if not force and not missing_overlay_data_paths(project_dir) and not missing_overlay_sidecar_paths(project_dir):
        detail = f"Overlay data already installed under {overlays_dir}"
        print(detail)
        if open_folder:
            open_overlay_folder(project_dir)
        return OverlayInstallResult(True, False, detail)

    archive_path = archive_path or (OVERLAY_CACHE_DIR / GSHHG_SHAPEFILE_ARCHIVE)
    attempted_urls: tuple[str, ...] = ()
    if force or not archive_path.exists():
        ok, detail = download_file(urls, archive_path)
        attempted_urls = urls
        if not ok:
            missing = (*missing_overlay_data_paths(project_dir), *missing_overlay_sidecar_paths(project_dir))
            message = (
                f"Could not download required overlay data archive {GSHHG_SHAPEFILE_ARCHIVE}. "
                "Every attempted mirror failed: "
                + "; ".join(urls)
                + f". Destination folder: {overlays_dir}. Missing files: "
                + "; ".join(str(path) for path in missing)
                + f". Manual fallback: download {GSHHG_SHAPEFILE_ARCHIVE} and extract the listed "
                + "GSHHS/WDBII files under overlays/."
                + f" Details: {detail}"
            )
            print(message, file=sys.stderr)
            return OverlayInstallResult(False, False, message, attempted_urls, missing)
    else:
        print(f"Using cached overlay archive: {archive_path}")

    try:
        extracted, missing = extract_overlay_archive(archive_path, project_dir, force=force)
    except RuntimeError as exc:
        missing = (*missing_overlay_data_paths(project_dir), *missing_overlay_sidecar_paths(project_dir))
        message = (
            f"Could not install overlay data from {archive_path}: {exc}. "
            f"Destination folder: {overlays_dir}. Missing files: "
            + "; ".join(str(path) for path in missing)
        )
        print(message, file=sys.stderr)
        return OverlayInstallResult(False, False, message, attempted_urls, missing)

    if missing:
        message = "Overlay archive was processed but required files are still missing: " + "; ".join(
            str(path) for path in missing
        )
        print(message, file=sys.stderr)
        return OverlayInstallResult(False, extracted > 0, message, attempted_urls, missing)

    detail = f"Overlay data installed under {overlays_dir} ({extracted} file(s) extracted)."
    print(detail)
    if open_folder:
        open_overlay_folder(project_dir)
    return OverlayInstallResult(True, extracted > 0, detail, attempted_urls)


def satpy_config_env_paths() -> list[tuple[str, Path]]:
    paths = []
    for name in SATPY_CONFIG_ENV_VARS:
        value = os.environ.get(name)
        if not value:
            continue
        for piece in value.split(os.pathsep):
            if piece.strip():
                paths.append((name, Path(piece).expanduser()))
    return paths


def check_satpy_config_environment() -> CheckResult:
    configured = satpy_config_env_paths()
    if not configured:
        return CheckResult(
            "Satpy config environment",
            True,
            "no custom Satpy config environment variables",
            critical=False,
        )

    invalid = [
        f"{name}={path}"
        for name, path in configured
        if not path.exists() or not path.is_dir()
    ]
    if invalid:
        return CheckResult(
            "Satpy config environment",
            False,
            "missing or not folders: " + ", ".join(invalid),
            critical=False,
        )

    return CheckResult(
        "Satpy config environment",
        True,
        "configured paths exist: " + ", ".join(f"{name}={path}" for name, path in configured),
        critical=False,
    )


def check_launcher_helpers(project_dir: Path = PROJECT_DIR) -> CheckResult:
    problems = []
    for helper, expected_script in LAUNCHER_HELPERS.items():
        path = project_dir / helper
        if not path.exists():
            problems.append(f"missing {helper}")
            continue
        try:
            text = path.read_text(encoding="utf-8", errors="replace").lower()
        except Exception as exc:
            problems.append(f"could not read {helper}: {exc}")
            continue
        if expected_script.lower() not in text:
            problems.append(f"{helper} does not reference {expected_script}")

    if problems:
        return CheckResult(
            "launcher helpers",
            False,
            "; ".join(problems),
            critical=False,
        )

    return CheckResult(
        "launcher helpers",
        True,
        ", ".join(LAUNCHER_HELPERS) + " found and point to expected scripts",
        critical=False,
    )


def nearest_existing_parent(path: Path) -> Path | None:
    current = path if path.exists() else path.parent
    while not current.exists():
        parent = current.parent
        if parent == current:
            return None
        current = parent
    return current


def check_writable_folder_target(name: str, path: Path) -> CheckResult:
    path = Path(path).expanduser()
    display_path = path.resolve(strict=False)

    if path.exists() and not path.is_dir():
        return CheckResult(name, False, f"{display_path} exists but is not a folder")

    probe_dir = path if path.is_dir() else nearest_existing_parent(path)
    if probe_dir is None:
        return CheckResult(name, False, f"no existing parent folder for {display_path}")
    if not probe_dir.is_dir():
        return CheckResult(name, False, f"nearest existing path is not a folder: {probe_dir}")

    probe_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(prefix=".himawari_env_check_", dir=probe_dir, delete=False) as probe:
            probe_path = Path(probe.name)
            probe.write(b"ok")
    except Exception as exc:
        return CheckResult(
            name,
            False,
            f"{display_path} is not writable through {probe_dir}: {exc.__class__.__name__}: {exc}",
        )
    finally:
        if probe_path is not None:
            try:
                probe_path.unlink(missing_ok=True)
            except Exception:
                pass

    if path.exists():
        detail = f"writable: {display_path}"
    else:
        detail = f"{display_path} can be created; nearest existing parent is writable: {probe_dir}"
    return CheckResult(name, True, detail)


def check_default_path_writability() -> list[CheckResult]:
    try:
        import himawari_lowram_processor as app
    except Exception as exc:
        detail = f"project import failed: {exc}"
        return [
            CheckResult("default output folder", False, detail, critical=False),
            CheckResult("default temp folder", False, detail, critical=False),
        ]

    results = []
    results.append(check_writable_folder_target("default output folder", Path(app.OUTPUT_DIR)))
    results.append(check_writable_folder_target("default temp folder", Path(app.TEMP_DIR)))
    return results


def cloud_sync_marker(path: Path) -> str | None:
    for part in path.expanduser().resolve(strict=False).parts:
        normalized = part.lower()
        if normalized in CLOUD_SYNC_EXACT:
            return part
        if any(normalized.startswith(prefix) for prefix in CLOUD_SYNC_PREFIXES):
            return part
    return None


def check_cloud_sync_locations(paths: list[Path] | None = None) -> CheckResult:
    if paths is None:
        try:
            import himawari_lowram_processor as app

            paths = [
                Path(app.OUTPUT_DIR),
                Path(app.TEMP_DIR),
                OVERLAY_CACHE_DIR,
            ]
        except Exception:
            paths = [APP_DATA_DIR, OVERLAY_CACHE_DIR]

    matches = []
    for path in paths:
        marker = cloud_sync_marker(path)
        if marker:
            matches.append(f"{path} ({marker})")

    if matches:
        return CheckResult(
            "cloud sync location",
            False,
            (
                "path is under a cloud-sync folder: "
                + "; ".join(matches)
                + "; large GeoTIFF/temp writes are safer in local non-synced folders such as "
                r"C:\Himawari\outputs and C:\Himawari\temp"
            ),
            critical=False,
        )

    return CheckResult(
        "cloud sync location",
        True,
        "output/temp/cache paths are not under a known cloud-sync folder",
        critical=False,
    )


def root_cleanup_candidates(project_dir: Path = PROJECT_DIR) -> list[CleanupCandidate]:
    if not project_dir.exists():
        return []

    candidates: list[CleanupCandidate] = []
    for path in sorted(project_dir.iterdir(), key=lambda item: item.name.lower()):
        name = path.name
        if path.is_dir() or name in CORE_ROOT_FILES:
            continue
        reason = KNOWN_UNUSED_ROOT_FILES.get(name)
        if reason is not None:
            candidates.append(CleanupCandidate(path, reason))
            continue
        if path.suffix.lower() not in ROOT_CLEANUP_EXTENSIONS:
            continue
        if reason is None:
            reason = "extra root-level program file is not part of the supported app entrypoints"
        candidates.append(CleanupCandidate(path, reason))
    return candidates


def check_project_cleanup(project_dir: Path = PROJECT_DIR) -> CheckResult:
    candidates = root_cleanup_candidates(project_dir)
    if not candidates:
        return CheckResult(
            "unused root programs",
            True,
            "no obsolete or extra root-level program files found",
            critical=False,
        )
    details = "; ".join(f"{candidate.path.name} ({candidate.reason})" for candidate in candidates)
    return CheckResult(
        "unused root programs",
        False,
        "archive recommended for: " + details,
        critical=False,
    )


def runtime_json_paths(project_dir: Path = PROJECT_DIR) -> tuple[Path, ...]:
    return tuple(project_dir / name for name in RUNTIME_JSON_FILES)


def check_runtime_json_files(project_dir: Path = PROJECT_DIR) -> CheckResult:
    invalid = []
    present = 0
    for path in runtime_json_paths(project_dir):
        if not path.exists():
            continue
        present += 1
        try:
            json.loads(path.read_text(encoding="utf-8"))
        except Exception as exc:
            invalid.append(f"{path.name}: {exc.__class__.__name__}: {exc}")

    if invalid:
        return CheckResult("runtime settings JSON", False, "; ".join(invalid), critical=False)
    if present:
        return CheckResult(
            "runtime settings JSON",
            True,
            f"{present} local settings/history file(s) parse as JSON",
            critical=False,
        )
    return CheckResult("runtime settings JSON", True, "no local settings/history JSON files found", critical=False)


def cleanup_archive_dir(project_dir: Path = PROJECT_DIR, timestamp: str | None = None) -> Path:
    timestamp = timestamp or datetime.now().strftime("%Y%m%d_%H%M%S")
    return project_dir / "cleanup_archive" / timestamp


def safe_relative_to_project(path: Path, project_dir: Path) -> Path:
    return path.resolve().relative_to(project_dir.resolve())


def protected_cleanup_candidate(path: Path, project_dir: Path = PROJECT_DIR) -> bool:
    try:
        relative = safe_relative_to_project(path, project_dir)
    except ValueError:
        return True
    if len(relative.parts) != 1:
        return True
    name = relative.parts[0]
    if name in CORE_ROOT_FILES or name in PROTECTED_ROOT_DIRS:
        return True
    if name in KNOWN_UNUSED_ROOT_FILES:
        return False
    return path.suffix.lower() not in ROOT_CLEANUP_EXTENSIONS


def archive_unused_programs(project_dir: Path = PROJECT_DIR, timestamp: str | None = None) -> CheckResult:
    candidates = root_cleanup_candidates(project_dir)
    if not candidates:
        return CheckResult("archive unused programs", True, "no cleanup candidates to archive", critical=False)

    archive_dir = cleanup_archive_dir(project_dir, timestamp)
    manifest: list[dict[str, object]] = []
    failures = []
    moved = 0
    archive_dir.mkdir(parents=True, exist_ok=True)

    for candidate in candidates:
        source = candidate.path
        if protected_cleanup_candidate(source, project_dir):
            failures.append(f"{source.name}: protected path was skipped")
            continue
        if not source.exists():
            failures.append(f"{source.name}: file disappeared before archiving")
            continue
        destination = archive_dir / source.name
        try:
            stat = source.stat()
            shutil.move(str(source), str(destination))
        except Exception as exc:
            failures.append(f"{source.name}: {exc.__class__.__name__}: {exc}")
            continue
        moved += 1
        manifest.append(
            {
                "original_path": str(source),
                "archived_path": str(destination),
                "size": stat.st_size,
                "modified_time": datetime.fromtimestamp(stat.st_mtime).isoformat(timespec="seconds"),
                "reason": candidate.reason,
            }
        )

    if manifest:
        (archive_dir / "manifest.json").write_text(
            json.dumps(manifest, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )

    if failures:
        return CheckResult(
            "archive unused programs",
            False,
            f"archived {moved} file(s) to {archive_dir}; issues: " + "; ".join(failures),
            critical=False,
        )
    return CheckResult(
        "archive unused programs",
        True,
        f"archived {moved} file(s) to {archive_dir}",
        critical=False,
    )


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


def check_pip_available(python_executable: str | Path | None = None) -> CheckResult:
    python_executable = str(python_executable or sys.executable)
    try:
        completed = subprocess.run(
            [python_executable, "-m", "pip", "--version"],
            cwd=PROJECT_DIR,
            capture_output=True,
            text=True,
            check=False,
        )
    except OSError as exc:
        return CheckResult("pip repair tool", False, f"could not run pip: {exc}")
    detail = (completed.stdout or completed.stderr).strip()
    if completed.returncode != 0:
        return CheckResult("pip repair tool", False, f"pip is unavailable: {detail}")
    return CheckResult("pip repair tool", True, detail or "pip is available", critical=False)


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


def check_app_version() -> CheckResult:
    try:
        import himawari_lowram_processor as app
    except Exception as exc:
        return CheckResult("app version", False, f"failed to import {APP_IMPORT_NAME}: {exc}")
    version = getattr(app, "APP_VERSION", None)
    if version:
        return CheckResult("app version", True, f"{app.app_version_label()}")
    return CheckResult("app version", False, "APP_VERSION is missing")


def run_checks(include_gpu: bool = False) -> list[CheckResult]:
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
    results.append(check_pip_available())
    package_results = check_packages()
    results.extend(package_results)
    results.append(check_satpy_version())
    results.append(check_satpy_config_environment())
    if has_critical_failures(package_results):
        results.extend(
            [
                CheckResult(
                    "AHI config and project import checks",
                    False,
                    "skipped until critical package checks pass; run --fix first",
                    critical=False,
                ),
                CheckResult(
                    "app version",
                    False,
                    "skipped until critical package checks pass; run --fix first",
                    critical=False,
                ),
            ]
        )
    else:
        results.extend(check_satpy_ahi_configs())
        results.append(check_satpy_true_color_registry())
        results.append(check_app_version())
        results.append(check_project_import())
        results.append(check_project_true_color_fallback_runtime())
    results.append(check_overlay_data())
    results.append(check_pycoast_overlay_runtime())
    if include_gpu:
        results.extend(check_gpu_support())
    results.append(check_project_cleanup())
    results.append(check_runtime_json_files())
    results.append(check_launcher_helpers())
    results.extend(check_default_path_writability())
    results.append(check_cloud_sync_locations())
    return results


def has_critical_failures(results: list[CheckResult]) -> bool:
    return any(not result.ok and result.critical for result in results)


def result_group(result: CheckResult) -> str:
    package_names = {package.import_name for package in PACKAGE_CHECKS}
    if result.name in {"python executable", "python version", "requirements file", "pip repair tool"}:
        return "Python"
    if result.name.startswith("GPU") or result.name.startswith("CuPy") or result.name.startswith("CUDA"):
        return "GPU"
    if result.name in package_names:
        return "Packages"
    if result.name in {"unused root programs", "runtime settings JSON", "archive unused programs"}:
        return "Project Cleanup"
    if result.name.startswith("satpy") or result.name.startswith("Satpy") or "AHI" in result.name:
        return "Satpy"
    if result.name in {"overlay data files", "pycoast overlay runtime"}:
        return "Overlays"
    if result.name in {
        "launcher helpers",
        "default output folder",
        "default temp folder",
        "cloud sync location",
    }:
        return "Paths"
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
    try:
        import himawari_lowram_processor as app

        print(f"App:     {app.app_version_label()}")
    except Exception:
        print("App:     unavailable")
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
        if any(result_group(result) == "GPU" for result in results if not result.ok and not result.critical):
            print("  To repair optional GPU acceleration support, run:")
            print("     " + command_text([sys.executable, str(Path(__file__).resolve()), "--gpu-fix"]))
        print("  For border overlays, Quick Fix downloads the needed low-resolution GSHHS/WDBII shapefiles.")
        print("  For path warnings, choose local non-synced output/temp folders such as C:\\Himawari\\outputs.")
        print("  You can also use the custom low-RAM true color fallback from the GUI or CLI.")
        return

    print()
    print("Environment looks ready for true color reproduction and low-RAM processing.")
    print("Use run_gui.bat for the desktop app or run_cli.bat/runcli.bat for the terminal interface.")


def has_failures(results: list[CheckResult]) -> bool:
    return any(not result.ok for result in results)


def final_status_code(results: list[CheckResult]) -> int:
    return 1 if has_critical_failures(results) else 0


def recheck_after_repair(grouped: bool = True) -> list[CheckResult]:
    print()
    print("Re-checking after repair...")
    results = run_checks()
    print_results(results, grouped=grouped)
    if has_failures(results):
        unresolved = [result.name for result in results if not result.ok]
        print("Remaining issue(s): " + ", ".join(unresolved))
    else:
        print("Final status: all checks passed.")
    return results


def recheck_after_repair_fresh(grouped: bool = True, python_executable: str | Path | None = None) -> list[CheckResult]:
    python_executable = str(python_executable or sys.executable)
    command = [python_executable, str(Path(__file__).resolve())]
    if not grouped:
        command.append("--plain")
    print()
    print("Re-checking after repair in a fresh Python process...")
    print(command_text(command))
    code = subprocess.call(command, cwd=PROJECT_DIR)
    if code == 0:
        return [CheckResult("fresh recheck", True, "fresh process reported no critical failures", critical=False)]
    return [CheckResult("fresh recheck", False, f"fresh process exited with status {code}", critical=True)]


def run_fix(
    python_executable: str | Path | None = None,
    open_overlays: bool = True,
    install_overlays: bool = True,
    force_overlay_data: bool = False,
    archive_unused: bool = True,
) -> int:
    if not REQUIREMENTS_FILE.exists():
        print(f"Cannot repair: missing requirements file: {REQUIREMENTS_FILE}", file=sys.stderr)
        return 1
    pip_result = check_pip_available(python_executable)
    if not pip_result.ok:
        print(f"Cannot repair: {pip_result.detail}", file=sys.stderr)
        return 1
    command = pip_install_command(upgrade=True, python_executable=python_executable)
    print(f"Current Python: {sys.executable}")
    print(f"Target Python:  {python_executable or sys.executable}")
    print("Running:", command_text(command))
    result = subprocess.call(command, cwd=PROJECT_DIR)
    if result != 0:
        return result
    if install_overlays:
        overlay_result = install_overlay_data(open_folder=open_overlays, force=force_overlay_data)
        if not overlay_result.ok:
            return 1
    else:
        ensure_overlay_folder(open_folder=open_overlays)
    if archive_unused:
        cleanup_result = archive_unused_programs(PROJECT_DIR)
        print(f"{status_label(cleanup_result)}: {cleanup_result.name}: {cleanup_result.detail}")
    return result


def run_gpu_fix(python_executable: str | Path | None = None) -> int:
    if not GPU_REQUIREMENTS_FILE.exists():
        print(f"Cannot repair GPU support: missing requirements file: {GPU_REQUIREMENTS_FILE}", file=sys.stderr)
        return 1
    pip_result = check_pip_available(python_executable)
    if not pip_result.ok:
        print(f"Cannot repair GPU support: {pip_result.detail}", file=sys.stderr)
        return 1
    command = gpu_pip_install_command(upgrade=True, python_executable=python_executable)
    print(f"Current Python: {sys.executable}")
    print(f"Target Python:  {python_executable or sys.executable}")
    print("Running:", command_text(command))
    return subprocess.call(command, cwd=PROJECT_DIR)


def run_environment_check_with(python_executable: str | Path) -> int:
    command = [str(python_executable), str(Path(__file__).resolve())]
    print("Checking repaired environment:", command_text(command))
    return subprocess.call(command, cwd=PROJECT_DIR)


def run_auto_fix(
    results: list[CheckResult],
    install_overlays: bool = True,
    force_overlay_data: bool = False,
    archive_unused: bool = True,
) -> int:
    python_result = next((result for result in results if result.name == "python version"), None)
    if python_result is not None and not python_result.ok:
        python_path = venv_python_path()
        if not python_path.exists():
            created_path = create_venv()
            if created_path is None:
                return 1
            python_path = created_path
        fix_code = run_fix(
            python_path,
            install_overlays=install_overlays,
            force_overlay_data=force_overlay_data,
            archive_unused=archive_unused,
        )
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

    return run_fix(
        install_overlays=install_overlays,
        force_overlay_data=force_overlay_data,
        archive_unused=archive_unused,
    )


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
    parser.add_argument(
        "--gpu",
        action="store_true",
        help="Include optional NVIDIA/CuPy GPU acceleration diagnostics.",
    )
    parser.add_argument(
        "--gpu-fix",
        action="store_true",
        help="Install/upgrade optional GPU packages from requirements-gpu.txt, then re-check GPU support.",
    )
    parser.add_argument(
        "--skip-overlay-data",
        action="store_true",
        help="Repair Python packages but do not download/install GSHHS/WDBII overlay data.",
    )
    parser.add_argument(
        "--force-overlay-data",
        action="store_true",
        help="Re-download/reinstall GSHHS/WDBII overlay data even if required files already exist.",
    )
    parser.add_argument(
        "--archive-unused",
        action="store_true",
        help="Archive obsolete/extra root-level program files without reinstalling packages.",
    )
    parser.add_argument(
        "--no-archive-unused",
        action="store_true",
        help="Skip cleanup archiving during --fix or --auto.",
    )
    args = parser.parse_args()

    configure_known_warning_filters()
    print_banner()

    include_gpu = args.gpu or args.gpu_fix
    results = run_checks(include_gpu=include_gpu)
    print_results(results, grouped=not args.plain)

    archive_unused = not args.no_archive_unused

    if args.archive_unused:
        print()
        cleanup_result = archive_unused_programs(PROJECT_DIR)
        print(f"{status_label(cleanup_result)}: {cleanup_result.name}: {cleanup_result.detail}")
        results = run_checks(include_gpu=include_gpu)
        print_results(results, grouped=not args.plain)
    elif args.gpu_fix:
        print()
        fix_code = run_gpu_fix()
        if fix_code != 0:
            return fix_code
        results = run_checks(include_gpu=True)
        print_results(results, grouped=not args.plain)
    elif args.auto:
        print()
        python_result = next((result for result in results if result.name == "python version"), None)
        current_python_supported = python_result is None or python_result.ok
        fix_code = run_auto_fix(
            results,
            install_overlays=not args.skip_overlay_data,
            force_overlay_data=args.force_overlay_data,
            archive_unused=archive_unused,
        )
        if fix_code != 0:
            return fix_code
        if current_python_supported:
            results = recheck_after_repair_fresh(grouped=not args.plain)
        else:
            return 0
    elif args.fix:
        print()
        fix_code = run_fix(
            install_overlays=not args.skip_overlay_data,
            force_overlay_data=args.force_overlay_data,
            archive_unused=archive_unused,
        )
        if fix_code != 0:
            return fix_code
        results = recheck_after_repair_fresh(grouped=not args.plain)

    if has_critical_failures(results):
        print_next_steps(results)
        return final_status_code(results)

    if has_failures(results):
        print_next_steps(results)
        return 0

    print_next_steps(results)
    return final_status_code(results)


if __name__ == "__main__":
    raise SystemExit(main())
