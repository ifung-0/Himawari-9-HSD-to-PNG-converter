from __future__ import annotations

import bz2
import concurrent.futures
import gc
import hashlib
import importlib.util
import json
import logging
import os
import queue
import re
import shutil
import subprocess
import sys
import threading
import tkinter as tk
import xml.etree.ElementTree as ET
from dataclasses import dataclass, fields
from datetime import UTC, datetime, timedelta
from pathlib import Path
from tkinter import colorchooser, filedialog, messagebox, ttk
from typing import Callable, Iterable

import dask
import dask.array as da
import numpy as np
import requests
import xarray as xr
from pyresample.geometry import AreaDefinition
from satpy import Scene


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
APP_VERSION = "2026.05.25.1"
APP_DISPLAY_NAME = "Himawari-9 Low-RAM Processor"
USER_URL = "https://noaa-himawari9.s3.amazonaws.com/AHI-L1b-FLDK/2024/07/25/0400/HS_H09_20240725_0400_B01_FLDK_R10_S0110.DAT.bz2"
MODE = "Single Image"  # "Single Image" or "Timelapse"
COMPOSITE_CHOICE = "True Color Reproduction Image"

HOURS_BACK = 72
INTERVAL_MINUTES = 20
FPS = 10
AUTO_DOWNLOAD = True
USE_NIGHT_FALLBACK = True
DOWNLOAD_WORKERS = 4
TIMELAPSE_FORMAT = "gif"  # "gif" or "mp4"
DELETE_TIMELAPSE_FRAMES = True
IMAGE_FORMAT = "png"  # Use "tif" for chunked GeoTIFF writes on very large outputs.
RESAMPLER = "native"  # "native" is required for full-disk low-RAM processing.
ADD_BORDER_LINES = False
BORDER_LINE_COLOR = "green"
BORDER_LINE_WIDTH = 1.0
MAX_SAFE_PNG_PIXELS = 120_000_000

RAM_LIMIT_GB = 10.0
DASK_CHUNK_SIZE = "64MiB"  # Use "128MiB" only if the machine has headroom.
DASK_NUM_WORKERS = 1  # Keep at 1 or 2.
NIGHT_CHECK_SAMPLE_PIXELS = 512
MAX_NATIVE_COMPATIBILITY_CROP_PIXELS = 32
NATIVE_GRID_INDEX_TOLERANCE = 1e-7

PROJECT_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = PROJECT_DIR / "outputs"
TEMP_DIR = PROJECT_DIR / "temp"
CLOUD_SYNC_PREFIXES = ("onedrive", "dropbox", "google drive", "icloud")
CLOUD_SYNC_EXACT = ("box",)
GUI_SETTINGS_FILE = PROJECT_DIR / "himawari_gui_settings.json"
GUI_SETTINGS_SCHEMA_VERSION = 1
TIMELAPSE_MANIFEST_SCHEMA_VERSION = 1
NOAA_HIMAWARI9_BUCKET = "https://noaa-himawari9.s3.amazonaws.com"


# ---------------------------------------------------------------------------
# Composite definitions
# ---------------------------------------------------------------------------
BAND_NAMES = tuple(f"B{i:02d}" for i in range(1, 17))

BAND_RESOLUTION = {
    "B01": "R10",
    "B02": "R10",
    "B03": "R05",
    "B04": "R10",
    "B05": "R20",
    "B06": "R20",
    "B07": "R20",
    "B08": "R20",
    "B09": "R20",
    "B10": "R20",
    "B11": "R20",
    "B12": "R20",
    "B13": "R20",
    "B14": "R20",
    "B15": "R20",
    "B16": "R20",
}

BAND_PIXEL_SIZE_M = {
    "B01": 1000,
    "B02": 1000,
    "B03": 500,
    "B04": 1000,
    "B05": 2000,
    "B06": 2000,
    "B07": 2000,
    "B08": 2000,
    "B09": 2000,
    "B10": 2000,
    "B11": 2000,
    "B12": 2000,
    "B13": 2000,
    "B14": 2000,
    "B15": 2000,
    "B16": 2000,
}

IR_BANDS = {
    "B07",
    "B08",
    "B09",
    "B10",
    "B11",
    "B12",
    "B13",
    "B14",
    "B15",
    "B16",
}
REFLECTANCE_BANDS = set(BAND_NAMES) - IR_BANDS

SINGLE_BAND_LABELS = {
    "B01 (Blue Visible)": "B01",
    "B02 (Green Visible)": "B02",
    "B03 (Red Visible)": "B03",
    "B04 (Near-IR)": "B04",
    "B05 (Snow/Ice Channel)": "B05",
    "B06 (Cloud Particle Size)": "B06",
    "B07 (Short Wave Infrared)": "B07",
    "B08 (Water Vapor)": "B08",
    "B09 (Mid-Level Water Vapor)": "B09",
    "B10 (Lower-Level Water Vapor)": "B10",
    "B11 (SO2 Channel)": "B11",
    "B12 (Ozone Channel)": "B12",
    "B13 (Infrared Window)": "B13",
    "B14 (Clean IR Window)": "B14",
    "B15 (Dirty IR Window)": "B15",
    "B16 (CO2 Channel)": "B16",
}

SATPY_COMPOSITE_NAMES = {
    "True Color RGB (Enhanced)": "true_color",
    "True Color Reproduction Image": "true_color_reproduction",
    "Natural Color RGB": "natural_color",
    "Day Microphysics RGB": "day_microphysics_ahi",
    "Night Microphysics RGB": "night_microphysics",
    "Dust RGB": "dust",
    "Airmass RGB": "airmass",
    "Day Convective Storm RGB": "day_severe_storms",
}

SATPY_OPTIONAL_DEP_FALLBACKS = {
    "True Color RGB (Enhanced)": "true_color_nocorr",
}

QUALITY_CRITICAL_COMPOSITES = {
    "True Color RGB (Enhanced)",
    "True Color Reproduction Image",
}

CUSTOM_COMPOSITE_BANDS = {
    "Day Snow-Fog RGB": ("B03", "B05", "B13"),
    "Sandwich (B03 + B13)": ("B03", "B13"),
    "B03 and B13 at night": ("B03", "B13"),
    "Heavy Rainfall Potential": ("B08", "B13", "B15"),
}

COMPOSITE_BANDS = {
    "True Color RGB (Enhanced)": ("B01", "B02", "B03", "B04"),
    "True Color Reproduction Image": ("B01", "B02", "B03", "B04"),
    "Natural Color RGB": ("B01", "B02", "B03", "B04", "B05"),
    "Day Microphysics RGB": ("B04", "B06", "B13"),
    "Night Microphysics RGB": ("B07", "B13", "B14", "B15"),
    "Dust RGB": ("B11", "B13", "B14", "B15"),
    "Airmass RGB": ("B08", "B10", "B12", "B14"),
    "Day Convective Storm RGB": ("B03", "B05", "B07", "B08", "B10", "B13"),
    **{label: (band,) for label, band in SINGLE_BAND_LABELS.items()},
    **CUSTOM_COMPOSITE_BANDS,
}

DAY_ONLY_COMPOSITES = {
    "True Color RGB (Enhanced)",
    "True Color Reproduction Image",
    "Natural Color RGB",
    "Day Microphysics RGB",
    "Day Snow-Fog RGB",
    "Day Convective Storm RGB",
    "B01 (Blue Visible)",
    "B02 (Green Visible)",
    "B03 (Red Visible)",
    "B04 (Near-IR)",
    "B05 (Snow/Ice Channel)",
    "B06 (Cloud Particle Size)",
}

NIGHT_FALLBACK_MAP = {
    "True Color RGB (Enhanced)": "B13 (Infrared Window)",
    "True Color Reproduction Image": "B13 (Infrared Window)",
    "Natural Color RGB": "B13 (Infrared Window)",
    "Day Microphysics RGB": "Night Microphysics RGB",
    "Day Snow-Fog RGB": "Night Microphysics RGB",
    "Day Convective Storm RGB": "Airmass RGB",
    "B01 (Blue Visible)": "B13 (Infrared Window)",
    "B02 (Green Visible)": "B13 (Infrared Window)",
    "B03 (Red Visible)": "B13 (Infrared Window)",
    "B04 (Near-IR)": "B13 (Infrared Window)",
    "B05 (Snow/Ice Channel)": "B13 (Infrared Window)",
    "B06 (Cloud Particle Size)": "B13 (Infrared Window)",
}

CUSTOM_DATASET_NAMES = {
    "True Color RGB (Enhanced)": "custom_true_color_rgb",
    "True Color Reproduction Image": "custom_true_color_reproduction_rgb",
    "Sandwich (B03 + B13)": "custom_sandwich_b03_b13",
    "B03 and B13 at night": "custom_b03_b13_night",
    "Heavy Rainfall Potential": "custom_heavy_rainfall_potential",
}

CUSTOM_SATPY_MISSING_DATASET_FALLBACKS = {
    "True Color RGB (Enhanced)",
    "True Color Reproduction Image",
}

LOG = logging.getLogger("himawari_lowram")


@dataclass(frozen=True)
class UrlInfo:
    root: str
    sat_id: str
    timestamp: str
    area: str
    total_segments: int


@dataclass(frozen=True)
class DownloadTask:
    url: str
    destination: Path


@dataclass
class ProcessorConfig:
    user_url: str = USER_URL
    mode: str = MODE
    composite_choice: str = COMPOSITE_CHOICE
    hours_back: int = HOURS_BACK
    interval_minutes: int = INTERVAL_MINUTES
    fps: int = FPS
    auto_download: bool = AUTO_DOWNLOAD
    use_night_fallback: bool = USE_NIGHT_FALLBACK
    download_workers: int = DOWNLOAD_WORKERS
    timelapse_format: str = TIMELAPSE_FORMAT
    delete_timelapse_frames: bool = DELETE_TIMELAPSE_FRAMES
    image_format: str = IMAGE_FORMAT
    resampler: str = RESAMPLER
    allow_quality_fallback: bool = False
    add_border_lines: bool = ADD_BORDER_LINES
    border_line_color: str = BORDER_LINE_COLOR
    border_line_width: float = BORDER_LINE_WIDTH
    max_safe_png_pixels: int = MAX_SAFE_PNG_PIXELS
    ram_limit_gb: float = RAM_LIMIT_GB
    dask_chunk_size: str = DASK_CHUNK_SIZE
    dask_num_workers: int = DASK_NUM_WORKERS


ProgressCallback = Callable[[str, int | None, int | None], None]


@dataclass(frozen=True)
class OverlayStatus:
    ok: bool
    details: tuple[str, ...]
    missing_packages: tuple[str, ...]
    missing_data: tuple[str, ...]

    def display_text(self) -> str:
        lines = list(self.details)
        if self.missing_packages:
            lines.append("Missing package(s): " + ", ".join(self.missing_packages))
        if self.missing_data:
            lines.extend(self.missing_data)
        if self.ok:
            lines.append("Overlay setup looks ready.")
        else:
            lines.append(
                "Install requirements with Quick Fix and place pycoast-compatible "
                "GSHHS/WDBII data under overlays/."
            )
        return "\n".join(lines)


@dataclass(frozen=True)
class RunSummary:
    source: str
    product: str
    frames: int
    bands: tuple[str, ...]
    total_segments: int
    output: str
    warnings: tuple[str, ...]

    def display_text(self) -> str:
        frame_word = "frame" if self.frames == 1 else "frames"
        segment_word = "segment" if self.total_segments == 1 else "segments"
        lines = [
            f"Source: {self.source}",
            f"Product: {self.product}",
            f"Frames: {self.frames} {frame_word}",
            f"Bands: {', '.join(self.bands)}",
            f"Downloads: {self.total_segments} {segment_word}",
            f"Output: {self.output}",
        ]
        if self.warnings:
            lines.append("")
            lines.append("Warnings:")
            lines.extend(f"- {warning}" for warning in self.warnings)
        return "\n".join(lines)


@dataclass(frozen=True)
class PreflightResult:
    ok: bool
    summary: RunSummary | None
    warnings: tuple[str, ...]
    errors: tuple[str, ...]

    def display_text(self) -> str:
        lines: list[str] = []
        if self.summary:
            lines.append(self.summary.display_text())
        if self.errors:
            if lines:
                lines.append("")
            lines.append("Fix before starting:")
            lines.extend(f"- {error}" for error in self.errors)
        if self.warnings:
            if lines:
                lines.append("")
            lines.append("Check before long runs:")
            lines.extend(f"- {warning}" for warning in self.warnings)
        return "\n".join(lines)


class ProcessingCancelled(RuntimeError):
    """Raised when the user requests cancellation from the GUI."""


def check_cancel(cancel_event: threading.Event | None) -> None:
    if cancel_event is not None and cancel_event.is_set():
        raise ProcessingCancelled("Processing canceled by user.")


def default_config() -> ProcessorConfig:
    return ProcessorConfig()


def processor_config_field_names() -> set[str]:
    return {field.name for field in fields(ProcessorConfig)}


def app_version_label() -> str:
    return f"{APP_DISPLAY_NAME} {APP_VERSION}"


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
    )


def configure_dask(config: ProcessorConfig | None = None) -> None:
    config = config or default_config()
    workers = clamp_dask_workers(config.dask_num_workers)
    dask.config.set(
        {
            "array.chunk-size": config.dask_chunk_size,
            "array.slicing.split_large_chunks": True,
            "optimization.fuse.active": False,
            "scheduler": "threads",
            "num_workers": workers,
        }
    )
    LOG.info("Dask configured: chunk-size=%s, threaded workers=%s", config.dask_chunk_size, workers)


def clamp_download_workers(value: int) -> int:
    return max(1, min(int(value), 4))


def clamp_dask_workers(value: int) -> int:
    return max(1, min(int(value), 2))


def memory_gb() -> float | None:
    try:
        import psutil

        return psutil.Process(os.getpid()).memory_info().rss / (1024**3)
    except Exception:
        pass

    if os.name == "nt":
        try:
            import ctypes
            from ctypes import wintypes

            class ProcessMemoryCounters(ctypes.Structure):
                _fields_ = [
                    ("cb", wintypes.DWORD),
                    ("PageFaultCount", wintypes.DWORD),
                    ("PeakWorkingSetSize", ctypes.c_size_t),
                    ("WorkingSetSize", ctypes.c_size_t),
                    ("QuotaPeakPagedPoolUsage", ctypes.c_size_t),
                    ("QuotaPagedPoolUsage", ctypes.c_size_t),
                    ("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t),
                    ("QuotaNonPagedPoolUsage", ctypes.c_size_t),
                    ("PagefileUsage", ctypes.c_size_t),
                    ("PeakPagefileUsage", ctypes.c_size_t),
                ]

            counters = ProcessMemoryCounters()
            counters.cb = ctypes.sizeof(ProcessMemoryCounters)
            handle = ctypes.windll.kernel32.GetCurrentProcess()
            ok = ctypes.windll.psapi.GetProcessMemoryInfo(
                handle, ctypes.byref(counters), counters.cb
            )
            if ok:
                return counters.WorkingSetSize / (1024**3)
        except Exception:
            return None

    try:
        import resource

        usage = resource.getrusage(resource.RUSAGE_SELF)
        scale = 1024**2 if sys.platform == "darwin" else 1024
        return usage.ru_maxrss / scale / 1024
    except Exception:
        return None


def log_memory(stage: str, config: ProcessorConfig | None = None) -> None:
    config = config or default_config()
    used = memory_gb()
    if used is None:
        LOG.info("[mem] %-22s unavailable", stage)
        return
    state = "OK" if used < config.ram_limit_gb else "OVER LIMIT"
    LOG.info("[mem] %-22s %.2f GiB / %.1f GiB %s", stage, used, config.ram_limit_gb, state)


def parse_url(url: str) -> UrlInfo:
    clean_url = url.strip().strip("[]()")
    object_pattern = (
        r"(https?://.*?/AHI-L1b-(?:Target|FLDK|Japan)/)"
        r"(\d{4}/\d{2}/\d{2}/\d{4}/)"
        r"(HS_H0[89])_(\d{8}_\d{4})_B\d{2}_([A-Z0-9]+)_R\d{2}_S\d{2}(\d{2})"
        r"\.DAT(?:\.bz2)?"
    )
    match = re.search(object_pattern, clean_url)
    if match:
        return UrlInfo(
            root=match.group(1),
            sat_id=match.group(3),
            timestamp=match.group(4),
            area=match.group(5),
            total_segments=int(match.group(6)),
        )

    index_pattern = (
        r"https?://(?P<host>[^/]+)/index\.html#"
        r"AHI-L1b-(?P<area_type>Target|FLDK|Japan)/"
        r"(?P<year>\d{4})/(?P<month>\d{2})/(?P<day>\d{2})/(?P<hhmm>\d{4})/?"
    )
    match = re.search(index_pattern, clean_url)
    if match:
        area = match.group("area_type")
        timestamp = f"{match.group('year')}{match.group('month')}{match.group('day')}_{match.group('hhmm')}"
        return UrlInfo(
            root=f"https://{match.group('host')}/AHI-L1b-{area}/",
            sat_id="HS_H09",
            timestamp=timestamp,
            area=area,
            total_segments=10,
        )

    raise ValueError(f"URL not recognised: {url}")


def noaa_scan_prefix(dt: datetime, area: str = "FLDK") -> str:
    return f"AHI-L1b-{area}/{dt:%Y/%m/%d/%H%M}/"


def parse_s3_listing_keys(xml_text: str) -> list[str]:
    root = ET.fromstring(xml_text)
    keys: list[str] = []
    for element in root.iter():
        if element.tag.rsplit("}", 1)[-1] == "Key" and element.text:
            keys.append(element.text)
    return keys


def object_url_from_s3_key(key: str) -> str:
    return f"{NOAA_HIMAWARI9_BUCKET}/{key}"


def latest_fldk_url_from_listing(xml_text: str) -> str | None:
    keys = parse_s3_listing_keys(xml_text)
    candidates = [
        key
        for key in keys
        if key.endswith(".DAT.bz2") and re.search(r"_B01_FLDK_R10_S01\d{2}\.DAT\.bz2$", key)
    ]
    if not candidates:
        return None
    return object_url_from_s3_key(sorted(candidates)[0])


def fetch_s3_prefix_listing(prefix: str, timeout: int = 20) -> str:
    response = requests.get(
        NOAA_HIMAWARI9_BUCKET,
        params={"list-type": "2", "prefix": prefix, "max-keys": "25"},
        timeout=timeout,
    )
    response.raise_for_status()
    return response.text


def recent_himawari_scan_datetimes(
    now: datetime | None = None,
    lookback_hours: int = 48,
    interval_minutes: int = 10,
) -> list[datetime]:
    current = now or datetime.now(UTC).replace(tzinfo=None)
    minute = (current.minute // interval_minutes) * interval_minutes
    aligned = current.replace(minute=minute, second=0, microsecond=0)
    count = max(1, int(lookback_hours * 60 / interval_minutes) + 1)
    return [aligned - timedelta(minutes=interval_minutes * idx) for idx in range(count)]


def find_latest_fldk_url(
    now: datetime | None = None,
    lookback_hours: int = 48,
    fetch_listing: Callable[[str], str] | None = None,
) -> str:
    fetch_listing = fetch_listing or fetch_s3_prefix_listing
    last_error: Exception | None = None
    for dt in recent_himawari_scan_datetimes(now=now, lookback_hours=lookback_hours):
        prefix = noaa_scan_prefix(dt, "FLDK")
        try:
            url = latest_fldk_url_from_listing(fetch_listing(prefix))
        except Exception as exc:
            last_error = exc
            continue
        if url:
            return url
    if last_error is not None:
        raise RuntimeError(f"Could not find a recent FLDK scan: {last_error}") from last_error
    raise RuntimeError(f"No FLDK scans found in the last {lookback_hours} hour(s).")


def frame_datetimes(start: datetime, mode: str, hours_back: int, interval_minutes: int) -> list[datetime]:
    if mode == "Single Image":
        return [start]
    if interval_minutes <= 0:
        raise ValueError("INTERVAL_MINUTES must be positive.")
    count = max(1, int(hours_back * 60 / interval_minutes))
    steps = [start - timedelta(minutes=i * interval_minutes) for i in range(count)]
    steps.reverse()
    return steps


def timestamp_path(dt: datetime) -> str:
    return dt.strftime("%Y/%m/%d/%H%M/")


def segment_filename(info: UrlInfo, dt: datetime, band: str, segment: int) -> str:
    return (
        f"{info.sat_id}_{dt:%Y%m%d_%H%M}_{band}_{info.area}_"
        f"{BAND_RESOLUTION[band]}_S{segment:02d}{info.total_segments:02d}.DAT"
    )


def make_download_tasks(info: UrlInfo, dt: datetime, bands: Iterable[str], temp_dir: Path) -> list[DownloadTask]:
    tasks = []
    d_path = timestamp_path(dt)
    frame_dir = temp_dir / dt.strftime("%Y%m%d_%H%M")
    frame_dir.mkdir(parents=True, exist_ok=True)
    for band in bands:
        for segment in range(1, info.total_segments + 1):
            filename = segment_filename(info, dt, band, segment)
            tasks.append(
                DownloadTask(
                    url=f"{info.root}{d_path}{filename}.bz2",
                    destination=frame_dir / filename,
                )
            )
    return tasks


def emit_progress(
    progress: ProgressCallback | None,
    message: str,
    current: int | None = None,
    total: int | None = None,
) -> None:
    if progress is not None:
        progress(message, current, total)


def decompress_bz2_chunk(
    decompressor: bz2.BZ2Decompressor,
    chunk: bytes,
) -> tuple[bz2.BZ2Decompressor, bytes]:
    output = bytearray()
    data = chunk
    while data:
        try:
            output.extend(decompressor.decompress(data))
            data = decompressor.unused_data
            if data:
                decompressor = bz2.BZ2Decompressor()
        except EOFError:
            decompressor = bz2.BZ2Decompressor()
    return decompressor, bytes(output)


def download_task_label(task: DownloadTask) -> str:
    match = re.search(r"_(B\d{2})_[A-Z0-9]+_R\d{2}_S(\d{2})(\d{2})\.DAT$", task.destination.name)
    if match:
        return f"{match.group(1)} S{int(match.group(2)):02d}/{int(match.group(3)):02d}"
    return task.destination.name


def download_workload_summary(tasks: list[DownloadTask], worker_count: int) -> str:
    if not tasks:
        worker_word = "worker" if worker_count == 1 else "workers"
        return f"Downloading 0 segments ({worker_count} {worker_word})"

    bands: set[str] = set()
    scan_counts: set[int] = set()
    areas: set[str] = set()
    for task in tasks:
        match = re.search(r"_(B\d{2})_([A-Z0-9]+)_R\d{2}_S\d{2}(\d{2})\.DAT$", task.destination.name)
        if not match:
            continue
        bands.add(match.group(1))
        areas.add(match.group(2))
        scan_counts.add(int(match.group(3)))

    if len(areas) == 1 and len(scan_counts) == 1 and bands:
        area = next(iter(areas))
        scans = next(iter(scan_counts))
        band_word = "band" if len(bands) == 1 else "bands"
        worker_word = "worker" if worker_count == 1 else "workers"
        return (
            f"Downloading {len(tasks)} segments "
            f"({len(bands)} {band_word} x {scans} {area} scans, {worker_count} {worker_word})"
        )
    worker_word = "worker" if worker_count == 1 else "workers"
    return f"Downloading {len(tasks)} segments ({worker_count} {worker_word})"


def remove_partial_download(path: Path) -> bool:
    try:
        path.unlink(missing_ok=True)
        return True
    except Exception as exc:
        LOG.warning("Could not remove partial download %s: %s", path, exc)
        return False


def stream_download_and_extract(
    task: DownloadTask,
    timeout: int = 60,
    cancel_event: threading.Event | None = None,
) -> Path | None:
    check_cancel(cancel_event)
    if task.destination.exists() and task.destination.stat().st_size > 0:
        return task.destination

    task.destination.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = task.destination.with_suffix(task.destination.suffix + ".part")
    try:
        if not remove_partial_download(tmp_path):
            return None
        with requests.get(task.url, timeout=timeout, stream=True) as response:
            check_cancel(cancel_event)
            if response.status_code != 200:
                LOG.warning("Missing %s: HTTP %s", task.url, response.status_code)
                return None
            decompressor = bz2.BZ2Decompressor()
            with tmp_path.open("wb") as out_file:
                for chunk in response.iter_content(chunk_size=1024 * 1024):
                    check_cancel(cancel_event)
                    if not chunk:
                        continue
                    decompressor, data = decompress_bz2_chunk(decompressor, chunk)
                    if data:
                        out_file.write(data)
                if not decompressor.eof:
                    raise EOFError("Compressed stream ended before bz2 EOF marker.")
        tmp_path.replace(task.destination)
        return task.destination
    except ProcessingCancelled:
        remove_partial_download(tmp_path)
        raise
    except Exception as exc:
        LOG.warning("Download failed for %s: %s", task.url, exc)
        remove_partial_download(tmp_path)
        return None


def download_segments(
    tasks: list[DownloadTask],
    workers: int,
    auto_download: bool = AUTO_DOWNLOAD,
    progress: ProgressCallback | None = None,
    cancel_event: threading.Event | None = None,
) -> list[Path]:
    check_cancel(cancel_event)
    total = len(tasks)
    completed = 0
    if not auto_download:
        existing = [task.destination for task in tasks if task.destination.exists()]
        emit_progress(progress, f"Found {len(existing)}/{total} existing files", len(existing), total)
        return existing

    worker_count = clamp_download_workers(workers)
    LOG.info("Downloading %s segments with %s worker(s)", len(tasks), worker_count)
    emit_progress(progress, download_workload_summary(tasks, worker_count), 0, total)
    pool = concurrent.futures.ThreadPoolExecutor(max_workers=worker_count)
    future_map: dict[concurrent.futures.Future[Path | None], DownloadTask] = {}
    results = []

    def download_one(task: DownloadTask) -> Path | None:
        emit_progress(progress, f"Downloading {download_task_label(task)}", completed, total)
        return stream_download_and_extract(task, cancel_event=cancel_event)

    try:
        for task in tasks:
            check_cancel(cancel_event)
            future = pool.submit(download_one, task)
            future_map[future] = task

        pending = set(future_map)
        while pending:
            check_cancel(cancel_event)
            done, pending = concurrent.futures.wait(
                pending,
                timeout=0.2,
                return_when=concurrent.futures.FIRST_COMPLETED,
            )
            for future in done:
                completed += 1
                result = future.result()
                if result is not None:
                    results.append(result)
                label = download_task_label(future_map[future])
                emit_progress(progress, f"Downloaded {label} ({completed}/{total})", completed, total)
    except ProcessingCancelled:
        for future in future_map:
            future.cancel()
        emit_progress(progress, "Download canceled", completed, total)
        raise
    finally:
        pool.shutdown(wait=True, cancel_futures=True)
    return results


def required_bands(
    composite_choice: str,
    include_night_fallback: bool = True,
    use_night_fallback: bool | None = None,
) -> tuple[str, ...]:
    if use_night_fallback is None:
        use_night_fallback = USE_NIGHT_FALLBACK
    if composite_choice not in COMPOSITE_BANDS:
        raise KeyError(f"Unknown composite: {composite_choice}")
    bands = list(COMPOSITE_BANDS[composite_choice])
    if include_night_fallback and composite_choice in DAY_ONLY_COMPOSITES and use_night_fallback:
        fallback = NIGHT_FALLBACK_MAP.get(composite_choice)
        if fallback:
            bands.extend(b for b in COMPOSITE_BANDS[fallback] if b not in bands)
    return tuple(dict.fromkeys(bands))


def area_compatibility_bands(composite_choice: str) -> tuple[str, ...]:
    """Bands that must align for the requested daytime product area."""
    return required_bands(composite_choice, include_night_fallback=False)


def select_active_composite(
    composite_choice: str,
    is_night: bool,
    use_night_fallback: bool | None = None,
) -> str:
    if use_night_fallback is None:
        use_night_fallback = USE_NIGHT_FALLBACK
    if is_night and use_night_fallback and composite_choice in DAY_ONLY_COMPOSITES:
        return NIGHT_FALLBACK_MAP.get(composite_choice, "B13 (Infrared Window)")
    return composite_choice


def target_pixel_size_m(composite_choice: str, use_night_fallback: bool | None = None) -> int:
    """Choose the finest native pixel size needed by the requested product."""
    bands = required_bands(composite_choice, use_night_fallback=use_night_fallback)
    return min(BAND_PIXEL_SIZE_M[band] for band in bands)


def area_reference_band(composite_choice: str) -> str:
    """Choose the finest day-product band to define the native target grid."""
    bands = COMPOSITE_BANDS[composite_choice]
    finest = min(BAND_PIXEL_SIZE_M[band] for band in bands)
    for band in bands:
        if BAND_PIXEL_SIZE_M[band] == finest:
            return band
    raise KeyError(f"No area reference band for {composite_choice}")


def coarse_sample_area(area: AreaDefinition, max_pixels: int = NIGHT_CHECK_SAMPLE_PIXELS) -> AreaDefinition:
    if area.width <= max_pixels and area.height <= max_pixels:
        return area
    scale = max(area.width / max_pixels, area.height / max_pixels)
    width = max(1, int(np.ceil(area.width / scale)))
    height = max(1, int(np.ceil(area.height / scale)))
    return AreaDefinition(
        f"{area.area_id}_sample",
        f"{area.description} Sample",
        area.proj_id,
        area.crs,
        width,
        height,
        area.area_extent,
    )


def is_visible_dark(scene: Scene, area: AreaDefinition) -> bool:
    try:
        scene.load(["B03"], calibration="reflectance")
        sample_area = coarse_sample_area(area)
        LOG.info(
            "Night check sampling B03 at %sx%s px instead of %sx%s full target",
            sample_area.width,
            sample_area.height,
            area.width,
            area.height,
        )
        sampled = scene.resample(sample_area, resampler="nearest", radius_of_influence=10000)
        max_value = sampled["B03"].max(skipna=True).compute()
        result = float(max_value) < 2.0
        LOG.info("Night check: B03 max reflectance %.4f -> %s", float(max_value), result)
        return result
    except Exception as exc:
        LOG.warning("Night check failed, keeping requested composite: %s", exc)
        return False


def calibration_for_band(band: str) -> str:
    return "brightness_temperature" if band in IR_BANDS else "reflectance"


def xr_clip(data: xr.DataArray, low: float, high: float) -> xr.DataArray:
    return data.clip(min=low, max=high)


def scale_reflectance(data: xr.DataArray, max_value: float = 100.0, gamma: float = 1.0) -> xr.DataArray:
    scaled = xr_clip(data.fillna(0.0), 0.0, max_value) / max_value
    if gamma != 1.0:
        scaled = scaled ** (1.0 / gamma)
    return xr_clip(scaled, 0.0, 1.0)


def scale_ir_temperature(
    data: xr.DataArray,
    warm_k: float = 300.0,
    cold_k: float = 190.0,
    gamma: float = 1.0,
) -> xr.DataArray:
    scaled = (warm_k - data.fillna(warm_k)) / (warm_k - cold_k)
    scaled = xr_clip(scaled, 0.0, 1.0)
    if gamma != 1.0:
        scaled = scaled ** (1.0 / gamma)
    return xr_clip(scaled, 0.0, 1.0)


def apply_black_point(data: xr.DataArray, black: float = 0.015) -> xr.DataArray:
    corrected = (data - black) / (1.0 - black)
    return xr_clip(corrected, 0.0, 1.0)


def apply_contrast(data: xr.DataArray, contrast: float = 1.12, midpoint: float = 0.42) -> xr.DataArray:
    return xr_clip((data - midpoint) * contrast + midpoint, 0.0, 1.0)


def apply_saturation(
    red: xr.DataArray,
    green: xr.DataArray,
    blue: xr.DataArray,
    saturation: float = 1.18,
) -> tuple[xr.DataArray, xr.DataArray, xr.DataArray]:
    luma = red * 0.2126 + green * 0.7152 + blue * 0.0722
    return (
        xr_clip(luma + (red - luma) * saturation, 0.0, 1.0),
        xr_clip(luma + (green - luma) * saturation, 0.0, 1.0),
        xr_clip(luma + (blue - luma) * saturation, 0.0, 1.0),
    )


def rgb_dataarray(
    red: xr.DataArray,
    green: xr.DataArray,
    blue: xr.DataArray,
    name: str,
    standard_name: str,
) -> xr.DataArray:
    rgb = xr.concat([red, green, blue], dim="bands")
    rgb = (xr_clip(rgb, 0.0, 1.0) * 255.0).round().astype(np.uint8)
    rgb = rgb.assign_coords(bands=["R", "G", "B"])
    attrs = red.attrs.copy()
    attrs.update(
        {
            "name": name,
            "standard_name": standard_name,
            "mode": "RGB",
            "sensor": "ahi",
        }
    )
    attrs["_FillValue"] = np.uint8(0)
    attrs.pop("calibration", None)
    attrs.pop("wavelength", None)
    attrs.pop("units", None)
    rgb.attrs = attrs
    return rgb


def single_band_to_rgb(data: xr.DataArray, band: str, name: str) -> xr.DataArray:
    if band in IR_BANDS:
        base = scale_ir_temperature(data, warm_k=305.0, cold_k=190.0, gamma=1.15)
        red = base
        green = xr_clip(base * 0.95 + 0.03, 0.0, 1.0)
        blue = xr_clip(base * 0.85 + 0.06, 0.0, 1.0)
        standard_name = "custom_ir_band_rgb"
    else:
        base = scale_reflectance(data, max_value=100.0, gamma=1.3)
        red = green = blue = base
        standard_name = "custom_visible_band_rgb"
    return rgb_dataarray(red, green, blue, name=name, standard_name=standard_name)


def create_sandwich_composite(b03: xr.DataArray, b13: xr.DataArray) -> xr.DataArray:
    vis = scale_reflectance(b03, max_value=100.0, gamma=1.2)
    ir = scale_ir_temperature(b13, warm_k=305.0, cold_k=190.0, gamma=1.05)

    # Lazy local contrast approximation. This avoids SciPy Gaussian filtering,
    # global percentiles, and full-array materialization.
    shifted_x = vis.shift(x=1, fill_value=0)
    shifted_y = vis.shift(y=1, fill_value=0)
    edges = xr_clip(abs(vis - shifted_x) + abs(vis - shifted_y), 0.0, 1.0)
    highlight = xr_clip(ir + edges * 0.45, 0.0, 1.0)

    red = highlight
    green = xr_clip(ir * 0.68 + highlight * 0.22, 0.0, 1.0)
    blue = xr_clip(ir * 0.28 + edges * 0.2, 0.0, 1.0)
    return rgb_dataarray(
        red,
        green,
        blue,
        name=CUSTOM_DATASET_NAMES["Sandwich (B03 + B13)"],
        standard_name="custom_sandwich_b03_b13",
    )


def create_b03_b13_night(b03: xr.DataArray, b13: xr.DataArray, is_night: bool) -> xr.DataArray:
    if is_night:
        return single_band_to_rgb(b13, "B13", CUSTOM_DATASET_NAMES["B03 and B13 at night"])
    return single_band_to_rgb(b03, "B03", CUSTOM_DATASET_NAMES["B03 and B13 at night"])


def create_heavy_rainfall_rgb(b08: xr.DataArray, b13: xr.DataArray, b15: xr.DataArray) -> xr.DataArray:
    water_vapor = xr_clip((b08.fillna(250.0) - 190.0) / 60.0, 0.0, 1.0)
    split_window = xr_clip(0.5 + (b13.fillna(300.0) - b15.fillna(300.0)) * 2.0, 0.0, 1.0)
    cold_cloud = scale_ir_temperature(b13, warm_k=300.0, cold_k=180.0, gamma=1.0)
    return rgb_dataarray(
        water_vapor,
        split_window,
        cold_cloud,
        name=CUSTOM_DATASET_NAMES["Heavy Rainfall Potential"],
        standard_name="custom_heavy_rainfall_potential",
    )


def create_day_snow_fog_rgb(b03: xr.DataArray, b05: xr.DataArray, b13: xr.DataArray) -> xr.DataArray:
    red = scale_reflectance(b03, max_value=100.0, gamma=1.15)
    green = scale_reflectance(b05, max_value=100.0, gamma=1.05)
    blue = scale_ir_temperature(b13, warm_k=300.0, cold_k=230.0, gamma=1.0)
    return rgb_dataarray(
        red,
        green,
        blue,
        name="custom_day_snow_fog_rgb",
        standard_name="custom_day_snow_fog_rgb",
    )


def create_true_color_reproduction_fallback(
    b01: xr.DataArray,
    b02: xr.DataArray,
    b03: xr.DataArray,
    b04: xr.DataArray,
    name: str | None = None,
    standard_name: str = "custom_true_color_reproduction_rgb",
) -> xr.DataArray:
    red = apply_black_point(scale_reflectance(b03, max_value=100.0, gamma=1.22), black=0.018)
    green = xr_clip(
        apply_black_point(scale_reflectance(b02, max_value=100.0, gamma=1.15), black=0.015) * 0.56
        + red * 0.32
        + apply_black_point(scale_reflectance(b04, max_value=100.0, gamma=1.1), black=0.012) * 0.12,
        0.0,
        1.0,
    )
    blue = apply_black_point(scale_reflectance(b01, max_value=100.0, gamma=1.18), black=0.008)
    red = apply_contrast(xr_clip(red * 1.05 + 0.01, 0.0, 1.0), contrast=1.12, midpoint=0.42)
    green = apply_contrast(xr_clip(green * 1.03 + 0.004, 0.0, 1.0), contrast=1.10, midpoint=0.42)
    blue = apply_contrast(xr_clip(blue * 0.94, 0.0, 1.0), contrast=1.08, midpoint=0.40)
    red, green, blue = apply_saturation(red, green, blue, saturation=1.18)
    return rgb_dataarray(
        red,
        green,
        blue,
        name=name or CUSTOM_DATASET_NAMES["True Color Reproduction Image"],
        standard_name=standard_name,
    )


def build_custom_composite(scene: Scene, composite_choice: str, is_night: bool) -> tuple[str, xr.DataArray]:
    if composite_choice in SINGLE_BAND_LABELS:
        band = SINGLE_BAND_LABELS[composite_choice]
        dataset_name = f"custom_{band.lower()}_rgb"
        return dataset_name, single_band_to_rgb(scene[band], band, dataset_name)

    if composite_choice in {"True Color RGB (Enhanced)", "True Color Reproduction Image"}:
        dataset = create_true_color_reproduction_fallback(
            scene["B01"],
            scene["B02"],
            scene["B03"],
            scene["B04"],
            name=CUSTOM_DATASET_NAMES[composite_choice],
            standard_name=CUSTOM_DATASET_NAMES[composite_choice],
        )
        return dataset.attrs["name"], dataset

    if composite_choice == "Sandwich (B03 + B13)":
        dataset = create_sandwich_composite(scene["B03"], scene["B13"])
        return dataset.attrs["name"], dataset

    if composite_choice == "B03 and B13 at night":
        dataset = create_b03_b13_night(scene["B03"], scene["B13"], is_night)
        return dataset.attrs["name"], dataset

    if composite_choice == "Heavy Rainfall Potential":
        dataset = create_heavy_rainfall_rgb(scene["B08"], scene["B13"], scene["B15"])
        return dataset.attrs["name"], dataset

    if composite_choice == "Day Snow-Fog RGB":
        dataset = create_day_snow_fog_rgb(scene["B03"], scene["B05"], scene["B13"])
        return dataset.attrs["name"], dataset

    raise KeyError(f"Composite is not custom: {composite_choice}")


def output_stem(info: UrlInfo, dt: datetime, composite_choice: str) -> str:
    safe_composite = re.sub(r"[^A-Za-z0-9]+", "_", composite_choice).strip("_")
    return f"Himawari_{info.area}_{dt:%Y%m%d_%H%M}_{safe_composite}"


def output_filename(
    info: UrlInfo,
    dt: datetime,
    composite_choice: str,
    mode: str,
    frame_idx: int,
    image_format: str | None = None,
    frame_dir: Path | None = None,
) -> Path:
    image_format = image_format or IMAGE_FORMAT
    suffix = ".tif" if image_format.lower() in {"tif", "tiff", "geotiff"} else ".png"
    if mode == "Single Image":
        return OUTPUT_DIR / f"{output_stem(info, dt, composite_choice)}{suffix}"
    base_dir = frame_dir or OUTPUT_DIR
    return base_dir / f"frame_{frame_idx:04d}{suffix}"


def enforce_safe_output_format(path: Path, area: AreaDefinition, config: ProcessorConfig) -> Path:
    pixel_count = int(area.width) * int(area.height)
    if path.suffix.lower() == ".png" and pixel_count > config.max_safe_png_pixels:
        safe_path = path.with_suffix(".tif")
        LOG.warning(
            "PNG output requested for %s pixels. Switching to GeoTIFF for low-RAM writing: %s",
            f"{pixel_count:,}",
            safe_path.name,
        )
        return safe_path
    return path


def output_behavior_for_config(config: ProcessorConfig, info: UrlInfo, start: datetime) -> str:
    suffix = ".tif" if config.image_format.lower() in {"tif", "tiff", "geotiff"} else ".png"
    if (
        info.area == "FLDK"
        and suffix == ".png"
        and target_pixel_size_m(config.composite_choice, config.use_night_fallback) <= 500
    ):
        suffix = ".tif"
        note = "auto-switch likely for full-disk low-RAM writing"
    else:
        note = "requested format"
    if config.mode == "Timelapse":
        return f"{config.timelapse_format.lower()} movie from frame images ({note}: {suffix})"
    return f"single image ({note}: {suffix})"


def writer_for_output(path: Path) -> str:
    return "geotiff" if path.suffix.lower() in {".tif", ".tiff"} else "simple_image"


def has_module(module_name: str) -> bool:
    return importlib.util.find_spec(module_name) is not None


def overlay_status(
    project_dir: Path = PROJECT_DIR,
    module_checker: Callable[[str], bool] = has_module,
) -> OverlayStatus:
    missing_packages = tuple(module for module in ("pycoast", "aggdraw") if not module_checker(module))
    overlays_dir = project_dir / "overlays"
    missing_data: list[str] = []
    details = [f"Overlay folder: {overlays_dir}"]
    if not overlays_dir.exists():
        missing_data.append("overlays/ folder not found.")
    elif not any(overlays_dir.rglob("*.shp")):
        missing_data.append("No shapefile data found under overlays/.")
    return OverlayStatus(
        ok=not missing_packages and not missing_data,
        details=tuple(details),
        missing_packages=missing_packages,
        missing_data=tuple(missing_data),
    )


def require_module(module_name: str, purpose: str) -> None:
    if not has_module(module_name):
        raise RuntimeError(
            f"Missing Python package '{module_name}' needed for {purpose}.\n\n"
            f"Install requirements with this Python:\n{sys.executable} -m pip install -r "
            f"\"{PROJECT_DIR / 'requirements.txt'}\""
        )


NAMED_COLORS = {
    "black": (0, 0, 0),
    "white": (255, 255, 255),
    "red": (255, 0, 0),
    "green": (0, 255, 0),
    "blue": (0, 0, 255),
    "yellow": (255, 255, 0),
    "cyan": (0, 255, 255),
    "magenta": (255, 0, 255),
    "orange": (255, 165, 0),
}


def parse_rgb_color(value: str) -> tuple[int, int, int]:
    text = value.strip().lower()
    if text in NAMED_COLORS:
        return NAMED_COLORS[text]
    if re.fullmatch(r"#[0-9a-f]{6}", text):
        return tuple(int(text[i : i + 2], 16) for i in (1, 3, 5))
    parts = [part.strip() for part in text.split(",")]
    if len(parts) == 3:
        try:
            rgb = tuple(int(part) for part in parts)
        except ValueError as exc:
            raise ValueError(f"Invalid RGB color: {value}") from exc
        if all(0 <= channel <= 255 for channel in rgb):
            return rgb
    raise ValueError("Border color must be a name, #RRGGBB, or R,G,B values.")


def build_overlay_options(config: ProcessorConfig) -> dict | None:
    if not config.add_border_lines:
        return None
    color = parse_rgb_color(config.border_line_color)
    return {
        "coast_dir": str(PROJECT_DIR / "overlays"),
        "color": color,
        "width": config.border_line_width,
        "level_coast": 1,
        "level_borders": 1,
        "resolution": "l",
    }


def cleanup_paths(paths: Iterable[Path]) -> None:
    for path in paths:
        try:
            if path.is_dir():
                shutil.rmtree(path)
            else:
                path.unlink(missing_ok=True)
        except Exception as exc:
            LOG.warning("Cleanup failed for %s: %s", path, exc)


def cleanup_partial_downloads(frame_dir: Path) -> None:
    if frame_dir.exists():
        cleanup_paths(list(frame_dir.glob("*.part")))


def stable_run_id(config: ProcessorConfig, info: UrlInfo, steps: list[datetime]) -> str:
    payload = {
        "schema": TIMELAPSE_MANIFEST_SCHEMA_VERSION,
        "url": config.user_url,
        "mode": config.mode,
        "composite": config.composite_choice,
        "image_format": config.image_format,
        "timelapse_format": config.timelapse_format,
        "night_fallback": config.use_night_fallback,
        "area": info.area,
        "timestamp": info.timestamp,
        "steps": [dt.strftime("%Y%m%d_%H%M") for dt in steps],
    }
    text = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha1(text.encode("utf-8")).hexdigest()[:12]


def timelapse_frame_dir(run_id: str, output_dir: Path = OUTPUT_DIR) -> Path:
    return output_dir / "frames" / run_id


def timelapse_manifest_path(run_id: str, output_dir: Path = OUTPUT_DIR) -> Path:
    return output_dir / "manifests" / f"{run_id}.json"


def frame_manifest_records(steps: list[datetime], frame_dir: Path, image_format: str) -> list[dict[str, object]]:
    suffix = ".tif" if image_format.lower() in {"tif", "tiff", "geotiff"} else ".png"
    return [
        {
            "index": idx,
            "timestamp": dt.strftime("%Y%m%d_%H%M"),
            "path": str(frame_dir / f"frame_{idx:04d}{suffix}"),
            "status": "pending",
        }
        for idx, dt in enumerate(steps)
    ]


def load_json_file(path: Path) -> dict | None:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def write_json_file(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def build_timelapse_manifest(
    run_id: str,
    config: ProcessorConfig,
    info: UrlInfo,
    steps: list[datetime],
    frame_dir: Path,
) -> dict:
    return {
        "schema_version": TIMELAPSE_MANIFEST_SCHEMA_VERSION,
        "run_id": run_id,
        "app_version": APP_VERSION,
        "created_at_utc": datetime.now(UTC).replace(tzinfo=None).isoformat(timespec="seconds") + "Z",
        "mode": config.mode,
        "composite_choice": config.composite_choice,
        "image_format": config.image_format,
        "timelapse_format": config.timelapse_format,
        "source_url": config.user_url,
        "source_area": info.area,
        "source_timestamp": info.timestamp,
        "frame_dir": str(frame_dir),
        "frames": frame_manifest_records(steps, frame_dir, config.image_format),
        "movie": None,
    }


def save_timelapse_manifest(path: Path, manifest: dict) -> None:
    write_json_file(path, manifest)


def manifest_frames(manifest: dict) -> list[dict[str, object]]:
    frames = manifest.get("frames", [])
    return frames if isinstance(frames, list) else []


def frame_output_from_manifest(manifest: dict, frame_idx: int) -> Path | None:
    for frame in manifest_frames(manifest):
        if frame.get("index") == frame_idx and frame.get("path"):
            return Path(str(frame["path"]))
    return None


def update_manifest_frame(manifest: dict, frame_idx: int, path: Path | None, status: str) -> None:
    for frame in manifest_frames(manifest):
        if frame.get("index") == frame_idx:
            frame["status"] = status
            if path is not None:
                frame["path"] = str(path)
            return


def resume_frame_path(manifest: dict, frame_idx: int) -> Path | None:
    path = frame_output_from_manifest(manifest, frame_idx)
    if path and path.exists() and path.stat().st_size > 0:
        return path
    return None


def load_or_create_timelapse_manifest(
    path: Path,
    run_id: str,
    config: ProcessorConfig,
    info: UrlInfo,
    steps: list[datetime],
    frame_dir: Path,
    resume: bool = True,
) -> dict:
    if resume and path.exists():
        existing = load_json_file(path)
        if (
            existing
            and existing.get("schema_version") == TIMELAPSE_MANIFEST_SCHEMA_VERSION
            and existing.get("run_id") == run_id
            and len(manifest_frames(existing)) == len(steps)
        ):
            return existing
    manifest = build_timelapse_manifest(run_id, config, info, steps, frame_dir)
    save_timelapse_manifest(path, manifest)
    return manifest


def intersect_extents(areas: Iterable[AreaDefinition]) -> tuple[float, float, float, float]:
    area_list = list(areas)
    if not area_list:
        raise RuntimeError("Could not determine geographic area from B13 scan segments.")

    x_min = max(area.area_extent[0] for area in area_list)
    y_min = max(area.area_extent[1] for area in area_list)
    x_max = min(area.area_extent[2] for area in area_list)
    y_max = min(area.area_extent[3] for area in area_list)
    if x_min >= x_max or y_min >= y_max:
        raise RuntimeError("No common geographic area across selected frames.")
    return x_min, y_min, x_max, y_max


def area_slice(area: AreaDefinition, target: AreaDefinition) -> tuple[slice, slice]:
    x_slice, y_slice = area.get_area_slices(target)
    return y_slice, x_slice


def area_slice_shape(area: AreaDefinition, target: AreaDefinition) -> tuple[int, int]:
    y_slice, x_slice = area_slice(area, target)
    return y_slice.stop - y_slice.start, x_slice.stop - x_slice.start


def same_area_slice_shape(areas: Iterable[AreaDefinition], target: AreaDefinition) -> tuple[int, int] | None:
    shapes = {area_slice_shape(area, target) for area in areas}
    if len(shapes) == 1:
        return shapes.pop()
    return None


def make_native_area(
    template_area: AreaDefinition,
    x_min: float,
    y_min: float,
    x_max: float,
    y_max: float,
    pixel_size_m: int,
) -> AreaDefinition | None:
    width = int(round((x_max - x_min) / pixel_size_m))
    height = int(round((y_max - y_min) / pixel_size_m))
    if width <= 0 or height <= 0:
        return None
    return AreaDefinition(
        "stabilized_b13_common",
        "Stabilized B13 Common Area",
        "stabilized_b13_common",
        template_area.crs,
        width,
        height,
        (x_min, y_min, x_max, y_max),
    )


def crop_native_area(
    template_area: AreaDefinition,
    left_crop: int,
    bottom_crop: int,
    right_crop: int,
    top_crop: int,
    pixel_size_m: int,
) -> AreaDefinition | None:
    width = template_area.width - left_crop - right_crop
    height = template_area.height - bottom_crop - top_crop
    if width <= 0 or height <= 0:
        return None
    x_min, y_min, x_max, y_max = template_area.area_extent
    return AreaDefinition(
        template_area.area_id,
        template_area.description,
        template_area.proj_id,
        template_area.crs,
        width,
        height,
        (
            x_min + left_crop * pixel_size_m,
            y_min + bottom_crop * pixel_size_m,
            x_max - right_crop * pixel_size_m,
            y_max - top_crop * pixel_size_m,
        ),
    )


def native_ratio_is_integer(source_size: int, target_size: int) -> bool:
    larger = max(source_size, target_size)
    smaller = min(source_size, target_size)
    return smaller > 0 and larger % smaller == 0


def native_area_compatibility_error(
    band_areas: dict[str, AreaDefinition],
    target_area: AreaDefinition,
) -> str | None:
    problems = []
    for band, area in band_areas.items():
        try:
            height, width = area_slice_shape(area, target_area)
        except Exception as exc:
            problems.append(f"{band}: could not slice source area ({exc})")
            continue
        if not (
            native_ratio_is_integer(height, target_area.height)
            and native_ratio_is_integer(width, target_area.width)
        ):
            problems.append(
                f"{band}: source slice {width}x{height} cannot resample natively "
                f"to target {target_area.width}x{target_area.height}"
            )
    if not problems:
        return None
    return (
        "Native target area is not integer-compatible with all requested bands. "
        + "; ".join(problems)
    )


def snap_extent_to_template_grid(
    template_area: AreaDefinition,
    extent: tuple[float, float, float, float],
    pixel_size_m: int,
) -> tuple[float, float, float, float]:
    x_min, y_min, x_max, y_max = extent
    base_x_min, base_y_min, base_x_max, base_y_max = template_area.area_extent
    tol = NATIVE_GRID_INDEX_TOLERANCE
    left_index = max(0, int(np.ceil((x_min - base_x_min) / pixel_size_m - tol)))
    bottom_index = max(0, int(np.ceil((y_min - base_y_min) / pixel_size_m - tol)))
    right_index = min(template_area.width, int(np.floor((x_max - base_x_min) / pixel_size_m + tol)))
    top_index = min(template_area.height, int(np.floor((y_max - base_y_min) / pixel_size_m + tol)))
    if left_index >= right_index or bottom_index >= top_index:
        raise RuntimeError("No common geographic area across selected frames.")
    return (
        base_x_min + left_index * pixel_size_m,
        base_y_min + bottom_index * pixel_size_m,
        base_x_min + right_index * pixel_size_m,
        base_y_min + top_index * pixel_size_m,
    )


def find_native_common_area(
    areas: list[AreaDefinition],
    extent: tuple[float, float, float, float],
    source_pixel_size_m: int,
) -> tuple[AreaDefinition, tuple[int, int]]:
    template_area = areas[0]
    x_min, y_min, x_max, y_max = snap_extent_to_template_grid(template_area, extent, source_pixel_size_m)
    max_inset = min(
        int(round((x_max - x_min) / source_pixel_size_m)),
        int(round((y_max - y_min) / source_pixel_size_m)),
    )
    for inset in range(max_inset):
        for left_extra in (0, 1):
            for bottom_extra in (0, 1):
                candidate = make_native_area(
                    template_area,
                    x_min + (inset + left_extra) * source_pixel_size_m,
                    y_min + (inset + bottom_extra) * source_pixel_size_m,
                    x_max - inset * source_pixel_size_m,
                    y_max - inset * source_pixel_size_m,
                    source_pixel_size_m,
                )
                if candidate is None:
                    continue
                shape = same_area_slice_shape(areas, candidate)
                if shape is not None:
                    return candidate, shape
    raise RuntimeError("Could not snap common area to the native source grid for all frames.")


def compatibility_area_labels(compatibility_areas: dict[str, AreaDefinition]) -> str:
    bands = sorted({label.split(":", 1)[-1] for label in compatibility_areas})
    return "/".join(bands) if bands else "requested bands"


def native_dimension_compatible(
    compatibility_areas: dict[str, AreaDefinition],
    target_area: AreaDefinition,
    dimension: str,
) -> bool:
    for area in compatibility_areas.values():
        try:
            height, width = area_slice_shape(area, target_area)
        except Exception:
            return False
        if dimension == "x":
            if not native_ratio_is_integer(width, target_area.width):
                return False
        elif dimension == "y":
            if not native_ratio_is_integer(height, target_area.height):
                return False
        else:
            raise ValueError(f"Unsupported dimension: {dimension}")
    return True


def find_dimension_crop(
    target_area: AreaDefinition,
    compatibility_areas: dict[str, AreaDefinition],
    target_pixel_size_m: int,
    dimension: str,
    max_crop_pixels: int,
) -> tuple[int, int]:
    if native_dimension_compatible(compatibility_areas, target_area, dimension):
        return 0, 0
    for total_crop in range(1, max_crop_pixels + 1):
        for lower_crop in range(total_crop + 1):
            upper_crop = total_crop - lower_crop
            if dimension == "x":
                candidate = crop_native_area(target_area, lower_crop, 0, upper_crop, 0, target_pixel_size_m)
            elif dimension == "y":
                candidate = crop_native_area(target_area, 0, lower_crop, 0, upper_crop, target_pixel_size_m)
            else:
                raise ValueError(f"Unsupported dimension: {dimension}")
            if candidate is not None and native_dimension_compatible(compatibility_areas, candidate, dimension):
                return lower_crop, upper_crop
    raise RuntimeError(f"Could not find native-compatible {dimension}-axis crop.")


def refine_native_compatible_target_area(
    target_area: AreaDefinition,
    compatibility_areas: dict[str, AreaDefinition],
    target_pixel_size_m: int,
    max_crop_pixels: int = MAX_NATIVE_COMPATIBILITY_CROP_PIXELS,
) -> AreaDefinition:
    if not compatibility_areas:
        return target_area
    if native_area_compatibility_error(compatibility_areas, target_area) is None:
        return target_area

    try:
        left_crop, right_crop = find_dimension_crop(
            target_area,
            compatibility_areas,
            target_pixel_size_m,
            "x",
            max_crop_pixels,
        )
        x_refined = crop_native_area(target_area, left_crop, 0, right_crop, 0, target_pixel_size_m)
        if x_refined is None:
            raise RuntimeError("Horizontal native-compatible crop removed the full target area.")
        bottom_crop, top_crop = find_dimension_crop(
            x_refined,
            compatibility_areas,
            target_pixel_size_m,
            "y",
            max_crop_pixels,
        )
        candidate = crop_native_area(
            target_area,
            left_crop,
            bottom_crop,
            right_crop,
            top_crop,
            target_pixel_size_m,
        )
        if candidate is not None and native_area_compatibility_error(compatibility_areas, candidate) is None:
            labels = compatibility_area_labels(compatibility_areas)
            LOG.info(
                "Adjusted target area by %s/%s/%s/%s px for %s native compatibility",
                left_crop,
                bottom_crop,
                right_crop,
                top_crop,
                labels,
            )
            return candidate
    except RuntimeError:
        pass

    error = native_area_compatibility_error(compatibility_areas, target_area)
    raise RuntimeError(
        "Selected frames cannot be aligned to a native low-RAM target area across all required bands. "
        "Try a shorter timelapse range, use Single Image, or choose a coarser product such as B13. "
        f"Last compatibility check: {error}"
    )


def native_compatible_common_area(
    areas: Iterable[AreaDefinition],
    target_pixel_size_m: int,
    source_pixel_size_m: int = BAND_PIXEL_SIZE_M["B13"],
    compatibility_areas: dict[str, AreaDefinition] | None = None,
) -> AreaDefinition:
    area_list = list(areas)
    if not area_list:
        raise RuntimeError("Could not determine geographic area from scan segments.")
    if source_pixel_size_m % target_pixel_size_m != 0:
        raise RuntimeError(
            f"Target pixel size {target_pixel_size_m} m is not an integer native factor "
            f"of {source_pixel_size_m} m source scan data."
        )

    template_area = area_list[0]
    all_areas = list(compatibility_areas.values()) if compatibility_areas else area_list
    common, source_shape = find_native_common_area(area_list, intersect_extents(all_areas), source_pixel_size_m)
    source_height, source_width = source_shape
    if source_height <= 0 or source_width <= 0:
        raise RuntimeError("No common geographic area across selected frames.")

    target_factor = source_pixel_size_m // target_pixel_size_m
    target_area = AreaDefinition(
        "stabilized_target",
        "Stabilized Target Area",
        "stabilized_target",
        template_area.crs,
        source_width * target_factor,
        source_height * target_factor,
        common.area_extent,
    )
    return refine_native_compatible_target_area(
        target_area,
        compatibility_areas or {},
        target_pixel_size_m,
    )


def common_area_from_frames(
    info: UrlInfo,
    steps: list[datetime],
    pixel_size_m: int,
    area_band: str,
    compatibility_bands: Iterable[str] | None = None,
    config: ProcessorConfig | None = None,
    progress: ProgressCallback | None = None,
    cancel_event: threading.Event | None = None,
) -> AreaDefinition:
    config = config or default_config()
    LOG.info("Scanning frames for common native area")
    scan_bands = tuple(dict.fromkeys(compatibility_bands or (area_band,)))
    if area_band not in scan_bands:
        scan_bands = (area_band, *scan_bands)
    emit_progress(progress, f"Downloading {', '.join(scan_bands)} scan segments for common area", 0, len(steps))
    areas = []
    compatibility_areas: dict[str, AreaDefinition] = {}
    source_pixel_size_m = BAND_PIXEL_SIZE_M[area_band]

    for idx, dt in enumerate(steps, start=1):
        check_cancel(cancel_event)
        tasks = make_download_tasks(info, dt, scan_bands, TEMP_DIR)
        results = download_segments(
            tasks,
            config.download_workers,
            auto_download=config.auto_download,
            progress=progress,
            cancel_event=cancel_event,
        )
        if len(results) != len(tasks):
            LOG.warning("Could not scan %s; segment unavailable", dt.strftime("%Y%m%d_%H%M"))
            emit_progress(progress, f"Area scan skipped {idx}/{len(steps)}", idx, len(steps))
            continue
        try:
            scene = Scene(filenames=[str(path) for path in results], reader="ahi_hsd")
            for band in scan_bands:
                scene.load([band], calibration=calibration_for_band(band))
                compatibility_areas[f"{dt:%Y%m%d_%H%M}:{band}"] = scene[band].attrs["area"]
            area = scene[area_band].attrs["area"]
            areas.append(area)
            LOG.info("Scanned %s", dt.strftime("%Y%m%d_%H%M"))
            emit_progress(progress, f"Area scanned {idx}/{len(steps)}", idx, len(steps))
        except Exception as exc:
            LOG.warning("Area scan failed for %s: %s", dt.strftime("%Y%m%d_%H%M"), exc)
        finally:
            gc.collect()

    log_memory("area scan complete", config)

    target_area = native_compatible_common_area(
        areas,
        pixel_size_m,
        source_pixel_size_m=source_pixel_size_m,
        compatibility_areas=compatibility_areas,
    )
    LOG.info(
        "Area locked: %sx%s px at %s m native target using %s grid",
        target_area.width,
        target_area.height,
        pixel_size_m,
        area_band,
    )
    return target_area


def load_bands(scene: Scene, bands: Iterable[str]) -> None:
    for band in bands:
        scene.load([band], calibration=calibration_for_band(band))


def resample_scene_low_ram(
    scene: Scene,
    target_area: AreaDefinition,
    config: ProcessorConfig,
    datasets: Iterable[str] | None = None,
) -> Scene:
    resampler = config.resampler.lower()
    if resampler == "native":
        return scene.resample(target_area, datasets=datasets, resampler="native")
    if resampler == "nearest":
        return scene.resample(
            target_area,
            datasets=datasets,
            resampler="nearest",
            radius_of_influence=10000,
        )
    if resampler == "bilinear":
        raise ValueError(
            "Bilinear resampling is disabled for this low-RAM full-disk workflow. "
            "Use the native resampler."
        )
    raise ValueError(f"Unsupported resampler: {config.resampler}")


def satpy_resample_datasets_for_composite(active: str, satpy_name: str) -> list[str] | None:
    # Satpy builds true_color_reproduction during resampling from loaded prerequisites.
    # Filtering to that final dataset before resampling prevents Satpy from creating it.
    if active == "True Color Reproduction Image":
        return None
    return [satpy_name]


def missing_optional_dependency(exc: BaseException, module_name: str) -> bool:
    current: BaseException | None = exc
    while current is not None:
        if isinstance(current, ModuleNotFoundError) and current.name == module_name:
            return True
        current = current.__cause__ or current.__context__
    return False


def missing_satpy_dataset(exc: BaseException, dataset_name: str) -> bool:
    dataset_pattern = re.compile(rf"(?<![A-Za-z0-9_]){re.escape(dataset_name)}(?![A-Za-z0-9_])")
    current: BaseException | None = exc
    while current is not None:
        message = str(current)
        if (
            dataset_pattern.search(message)
            and (
                "No dataset matching" in message
                or "unknown dataset" in message.lower()
                or "could not find" in message.lower()
                or "not found" in message.lower()
            )
        ):
            return True
        current = current.__cause__ or current.__context__
    return False


def use_custom_satpy_missing_dataset_fallback(exc: BaseException, active: str, satpy_name: str) -> bool:
    return active in CUSTOM_SATPY_MISSING_DATASET_FALLBACKS and missing_satpy_dataset(exc, satpy_name)


def use_true_color_reproduction_fallback(exc: BaseException, active: str, satpy_name: str) -> bool:
    return use_custom_satpy_missing_dataset_fallback(exc, active, satpy_name)


def satpy_missing_dataset_fallback_message(active: str, satpy_name: str) -> str:
    if active == "True Color Reproduction Image":
        return (
            "Satpy true_color_reproduction unavailable; using custom low-RAM fallback approximation "
            "(not official Satpy/JMA true color reproduction)"
        )
    return f"Satpy {satpy_name} unavailable; using custom low-RAM fallback"


def missing_pyspectral_message(composite_name: str) -> str:
    return (
        f"{composite_name} needs the optional package pyspectral for the official-looking "
        "Rayleigh/JMA true color processing. Install requirements with the same Python "
        f"running this app:\n\n{sys.executable} -m pip install -r \"{PROJECT_DIR / 'requirements.txt'}\""
    )


def save_dataset_with_optional_overlay(
    scene: Scene,
    dataset_name: str,
    output_path: Path,
    writer: str,
    enhance: bool,
    overlay: dict | None,
    fill_value: int | float | None = None,
) -> Path:
    save_kwargs = {
        "filename": str(output_path),
        "writer": writer,
        "enhance": enhance,
    }
    if fill_value is not None:
        save_kwargs["fill_value"] = fill_value
    if overlay is not None:
        try:
            scene.save_dataset(dataset_name, overlay=overlay, **save_kwargs)
            return output_path
        except Exception as exc:
            LOG.warning(
                "Border overlay failed (%s). Saving image without border lines.",
                exc,
            )
    try:
        scene.save_dataset(dataset_name, **save_kwargs)
    except ValueError as exc:
        if output_path.suffix.lower() == ".png" and "empty image" in str(exc).lower():
            fallback_path = output_path.with_suffix(".tif")
            LOG.warning(
                "PNG writer failed with '%s'. Retrying as chunked GeoTIFF: %s",
                exc,
                fallback_path.name,
            )
            save_kwargs["filename"] = str(fallback_path)
            save_kwargs["writer"] = "geotiff"
            scene.save_dataset(dataset_name, **save_kwargs)
            return fallback_path
        raise
    return output_path


def save_custom_composite_output(
    scene: Scene,
    active: str,
    master_area: AreaDefinition,
    output_path: Path,
    config: ProcessorConfig,
    is_night: bool,
    overlay_options: dict | None,
    progress: ProgressCallback | None = None,
    cancel_event: threading.Event | None = None,
) -> Path:
    custom_bands = COMPOSITE_BANDS[active]
    emit_progress(progress, f"Loading {active}", None, None)
    load_bands(scene, custom_bands)
    log_memory("after load", config)
    check_cancel(cancel_event)
    band_areas = {band: scene[band].attrs["area"] for band in custom_bands}
    compatibility_error = native_area_compatibility_error(band_areas, master_area)
    if compatibility_error:
        if is_night:
            emit_progress(
                progress,
                f"Skipping night fallback frame; {active} is not native-compatible with the locked target area",
                None,
                None,
            )
        else:
            emit_progress(progress, f"Skipping frame; {active} is not native-compatible with target area", None, None)
        raise RuntimeError(compatibility_error)
    emit_progress(progress, "Resampling", None, None)
    resampled = resample_scene_low_ram(scene, master_area, config, datasets=custom_bands)
    log_memory("after resample", config)
    check_cancel(cancel_event)
    emit_progress(progress, f"Building {active}", None, None)
    dataset_name, dataset = build_custom_composite(resampled, active, is_night)
    resampled[dataset_name] = dataset
    check_cancel(cancel_event)
    emit_progress(progress, f"Saving {output_path.name}", None, None)
    output_path = save_dataset_with_optional_overlay(
        resampled,
        dataset_name,
        output_path,
        writer_for_output(output_path),
        enhance=False,
        fill_value=0,
        overlay=overlay_options,
    )
    log_memory("after save", config)
    return output_path


def save_custom_satpy_missing_dataset_fallback(
    local_files: list[Path],
    active: str,
    satpy_name: str,
    master_area: AreaDefinition,
    output_path: Path,
    config: ProcessorConfig,
    is_night: bool,
    overlay_options: dict | None,
    progress: ProgressCallback | None = None,
    cancel_event: threading.Event | None = None,
) -> Path:
    message = satpy_missing_dataset_fallback_message(active, satpy_name)
    LOG.warning("%s.", message)
    emit_progress(progress, message, None, None)
    scene = Scene(filenames=[str(path) for path in local_files], reader="ahi_hsd")
    return save_custom_composite_output(
        scene,
        active,
        master_area,
        output_path,
        config,
        is_night,
        overlay_options,
        progress=progress,
        cancel_event=cancel_event,
    )


def save_true_color_reproduction_fallback(
    local_files: list[Path],
    master_area: AreaDefinition,
    output_path: Path,
    config: ProcessorConfig,
    is_night: bool,
    overlay_options: dict | None,
    progress: ProgressCallback | None = None,
    cancel_event: threading.Event | None = None,
) -> Path:
    return save_custom_satpy_missing_dataset_fallback(
        local_files,
        "True Color Reproduction Image",
        "true_color_reproduction",
        master_area,
        output_path,
        config,
        is_night,
        overlay_options,
        progress=progress,
        cancel_event=cancel_event,
    )


def process_frame(
    dt: datetime,
    info: UrlInfo,
    master_area: AreaDefinition,
    frame_idx: int,
    total_frames: int,
    config: ProcessorConfig | None = None,
    progress: ProgressCallback | None = None,
    cancel_event: threading.Event | None = None,
    frame_output_dir: Path | None = None,
) -> Path | None:
    config = config or default_config()
    check_cancel(cancel_event)
    requested = config.composite_choice
    bands = required_bands(requested, use_night_fallback=config.use_night_fallback)
    tasks = make_download_tasks(info, dt, bands, TEMP_DIR)
    frame_dir = tasks[0].destination.parent if tasks else TEMP_DIR
    local_files: list[Path] = []
    scene: Scene | None = None
    frame_succeeded = False

    LOG.info("[%s/%s] Processing %s", frame_idx + 1, total_frames, dt.strftime("%Y%m%d_%H%M"))
    emit_progress(progress, f"Frame {frame_idx + 1}/{total_frames}: starting", 0, max(1, len(tasks)))
    log_memory("frame start", config)

    try:
        local_files = download_segments(
            tasks,
            config.download_workers,
            auto_download=config.auto_download,
            progress=progress,
            cancel_event=cancel_event,
        )
        log_memory("after download", config)
        check_cancel(cancel_event)
        if len(local_files) != len(tasks):
            LOG.warning("Skipping frame; got %s/%s segments", len(local_files), len(tasks))
            return None

        is_night = False
        if (
            config.use_night_fallback
            and (requested in DAY_ONLY_COMPOSITES or requested == "B03 and B13 at night")
        ):
            emit_progress(progress, "Checking day/night fallback", None, None)
            night_scene = Scene(filenames=[str(path) for path in local_files], reader="ahi_hsd")
            try:
                is_night = is_visible_dark(night_scene, master_area)
            finally:
                night_scene = None
                gc.collect()
            log_memory("night check", config)
            check_cancel(cancel_event)

        active = select_active_composite(requested, is_night, config.use_night_fallback)
        if active != requested:
            LOG.info("Night fallback: %s -> %s", requested, active)

        scene = Scene(filenames=[str(path) for path in local_files], reader="ahi_hsd")
        log_memory("scene created", config)
        check_cancel(cancel_event)

        output_path = output_filename(
            info,
            dt,
            active,
            config.mode,
            frame_idx,
            config.image_format,
            frame_dir=frame_output_dir,
        )
        output_path = enforce_safe_output_format(output_path, master_area, config)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        overlay_options = build_overlay_options(config)

        if active in SATPY_COMPOSITE_NAMES:
            satpy_name = SATPY_COMPOSITE_NAMES[active]
            emit_progress(progress, f"Loading {active}", None, None)
            try:
                scene.load([satpy_name])
            except Exception as exc:
                if use_custom_satpy_missing_dataset_fallback(exc, active, satpy_name):
                    output_path = save_custom_satpy_missing_dataset_fallback(
                        local_files,
                        active,
                        satpy_name,
                        master_area,
                        output_path,
                        config,
                        is_night,
                        overlay_options,
                        progress=progress,
                        cancel_event=cancel_event,
                    )
                    frame_succeeded = True
                    return output_path
                if (
                    active in QUALITY_CRITICAL_COMPOSITES
                    and not config.allow_quality_fallback
                    and missing_optional_dependency(exc, "pyspectral")
                ):
                    raise RuntimeError(missing_pyspectral_message(active)) from exc
                fallback_name = SATPY_OPTIONAL_DEP_FALLBACKS.get(active)
                if not fallback_name or not missing_optional_dependency(exc, "pyspectral"):
                    raise
                LOG.warning(
                    "%s requires pyspectral, which is not installed for this Python. "
                    "Falling back to Satpy composite %s.",
                    satpy_name,
                    fallback_name,
                )
                emit_progress(progress, f"pyspectral missing; using {fallback_name}", None, None)
                scene = Scene(filenames=[str(path) for path in local_files], reader="ahi_hsd")
                satpy_name = fallback_name
                scene.load([satpy_name])
            log_memory("after load", config)
            check_cancel(cancel_event)
            emit_progress(progress, "Resampling", None, None)
            try:
                resample_datasets = satpy_resample_datasets_for_composite(active, satpy_name)
                resampled = resample_scene_low_ram(scene, master_area, config, datasets=resample_datasets)
                log_memory("after resample", config)
                check_cancel(cancel_event)
                emit_progress(progress, f"Saving {output_path.name}", None, None)
                output_path = save_dataset_with_optional_overlay(
                    resampled,
                    satpy_name,
                    output_path,
                    writer_for_output(output_path),
                    enhance=True,
                    overlay=overlay_options,
                )
            except Exception as exc:
                if use_custom_satpy_missing_dataset_fallback(exc, active, satpy_name):
                    output_path = save_custom_satpy_missing_dataset_fallback(
                        local_files,
                        active,
                        satpy_name,
                        master_area,
                        output_path,
                        config,
                        is_night,
                        overlay_options,
                        progress=progress,
                        cancel_event=cancel_event,
                    )
                else:
                    raise
            log_memory("after save", config)
            frame_succeeded = True
            return output_path

        output_path = save_custom_composite_output(
            scene,
            active,
            master_area,
            output_path,
            config,
            is_night,
            overlay_options,
            progress=progress,
            cancel_event=cancel_event,
        )
        frame_succeeded = True
        return output_path
    except ProcessingCancelled:
        LOG.info("Frame canceled.")
        raise
    except Exception as exc:
        LOG.exception("Frame failed: %s", exc)
        return None
    finally:
        scene = None
        cleanup_partial_downloads(frame_dir)
        gc.collect()
        log_memory("frame cleanup", config)


def assemble_timelapse(
    paths: list[Path],
    info: UrlInfo,
    config: ProcessorConfig | None = None,
    progress: ProgressCallback | None = None,
    cancel_event: threading.Event | None = None,
) -> Path | None:
    config = config or default_config()
    check_cancel(cancel_event)
    if not paths:
        return None

    safe_composite = re.sub(r"[^A-Za-z0-9]+", "_", config.composite_choice).strip("_")
    fmt = config.timelapse_format.lower()
    if fmt == "mp4" and not has_module("imageio_ffmpeg"):
        LOG.warning("imageio-ffmpeg is unavailable; falling back to GIF")
        fmt = "gif"

    import imageio.v2 as imageio

    output = OUTPUT_DIR / f"Himawari_{info.area}_{info.timestamp}_{safe_composite}.{fmt}"
    LOG.info("Assembling %s from %s frame(s)", output.name, len(paths))
    emit_progress(progress, f"Assembling {output.name}", 0, len(paths))
    log_memory("timelapse start", config)

    if fmt == "mp4":
        with imageio.get_writer(output, fps=config.fps, codec="libx264", quality=8) as writer:
            for idx, path in enumerate(paths, start=1):
                check_cancel(cancel_event)
                writer.append_data(imageio.imread(path))
                emit_progress(progress, f"Added frame {idx}/{len(paths)}", idx, len(paths))
    else:
        with imageio.get_writer(output, mode="I", duration=int(1000 / config.fps), loop=0) as writer:
            for idx, path in enumerate(paths, start=1):
                check_cancel(cancel_event)
                writer.append_data(imageio.imread(path))
                emit_progress(progress, f"Added frame {idx}/{len(paths)}", idx, len(paths))

    log_memory("timelapse saved", config)
    if config.delete_timelapse_frames:
        cleanup_paths(paths)
    return output


def validate_configuration(config: ProcessorConfig | None = None) -> None:
    config = config or default_config()
    if not config.user_url.strip():
        raise ValueError("Himawari URL is required.")
    if config.mode not in {"Single Image", "Timelapse"}:
        raise ValueError('MODE must be "Single Image" or "Timelapse".')
    if config.composite_choice not in COMPOSITE_BANDS:
        supported = ", ".join(sorted(COMPOSITE_BANDS))
        raise ValueError(f"Unsupported COMPOSITE_CHOICE {config.composite_choice!r}. Supported: {supported}")
    if BAND_NAMES != tuple(BAND_RESOLUTION):
        raise ValueError("BAND_RESOLUTION must define B01 through B16 in order.")
    if BAND_NAMES != tuple(BAND_PIXEL_SIZE_M):
        raise ValueError("BAND_PIXEL_SIZE_M must define B01 through B16 in order.")
    if config.interval_minutes <= 0:
        raise ValueError("INTERVAL_MINUTES must be positive.")
    if config.hours_back <= 0:
        raise ValueError("HOURS_BACK must be positive.")
    if config.fps <= 0:
        raise ValueError("FPS must be positive.")
    if config.download_workers <= 0:
        raise ValueError("Download workers must be positive.")
    if config.dask_num_workers <= 0:
        raise ValueError("Dask workers must be positive.")
    if config.ram_limit_gb <= 0:
        raise ValueError("RAM limit must be positive.")
    if config.download_workers > 4:
        LOG.warning("DOWNLOAD_WORKERS=%s requested; capped to 4.", config.download_workers)
    if config.image_format.lower() not in {"png", "tif", "tiff", "geotiff"}:
        raise ValueError('IMAGE_FORMAT must be "png" or "tif".')
    if config.timelapse_format.lower() not in {"gif", "mp4"}:
        raise ValueError('TIMELAPSE_FORMAT must be "gif" or "mp4".')
    if config.resampler.lower() not in {"native", "nearest"}:
        raise ValueError('RESAMPLER must be "native" or "nearest" for low-RAM processing.')
    if config.add_border_lines:
        parse_rgb_color(config.border_line_color)
        if config.border_line_width <= 0:
            raise ValueError("Border line width must be positive.")
        require_module("pycoast", "coastline and border overlays")


def validate_runtime_dependencies(config: ProcessorConfig, info: UrlInfo, start: datetime, area: AreaDefinition) -> None:
    preview_name = output_filename(
        info,
        start,
        config.composite_choice,
        config.mode,
        0,
        config.image_format,
    )
    safe_name = enforce_safe_output_format(preview_name, area, config)
    if config.mode == "Timelapse" and writer_for_output(safe_name) == "geotiff":
        raise RuntimeError(
            "Timelapse frames would be too large for low-RAM GIF/MP4 assembly and would need GeoTIFF output. "
            "Use Single Image, choose a coarser product such as B13, or use a smaller Himawari area."
        )
    if writer_for_output(safe_name) == "geotiff":
        require_module("rasterio", "GeoTIFF output")


def build_run_summary(config: ProcessorConfig) -> RunSummary:
    info = parse_url(config.user_url)
    start = datetime.strptime(info.timestamp, "%Y%m%d_%H%M")
    steps = frame_datetimes(start, config.mode, config.hours_back, config.interval_minutes)
    bands = required_bands(config.composite_choice, use_night_fallback=config.use_night_fallback)
    total_segments = len(steps) * len(bands) * info.total_segments
    warnings: list[str] = []
    if config.mode == "Timelapse" and config.composite_choice in QUALITY_CRITICAL_COMPOSITES:
        warnings.append("True color timelapses can be slow and memory-sensitive; IR products are safer.")
    if config.download_workers > 4:
        warnings.append("Download workers will be capped to 4.")
    if config.dask_num_workers > 2:
        warnings.append("Dask workers will be capped to 2.")
    return RunSummary(
        source=f"{info.area} {info.timestamp}",
        product=config.composite_choice,
        frames=len(steps),
        bands=bands,
        total_segments=total_segments,
        output=output_behavior_for_config(config, info, start),
        warnings=tuple(warnings),
    )


def preflight_run(config: ProcessorConfig, output_dir: Path = OUTPUT_DIR, temp_dir: Path = TEMP_DIR) -> PreflightResult:
    warnings: list[str] = []
    errors = setup_configuration_errors(config)
    summary: RunSummary | None = None
    try:
        if not errors:
            validate_configuration(config)
        summary = build_run_summary(config)
    except Exception as exc:
        errors.append(str(exc))

    setup_status = build_setup_status(config, output_dir, temp_dir)
    warnings.extend(setup_status.warnings)
    warnings.extend(summary.warnings if summary else ())
    for error in setup_status.errors:
        if error not in errors:
            errors.append(error)

    if config.add_border_lines:
        status = overlay_status()
        if not status.ok:
            warnings.append(status.display_text())

    return PreflightResult(
        ok=not errors,
        summary=summary,
        warnings=tuple(dict.fromkeys(warnings)),
        errors=tuple(dict.fromkeys(errors)),
    )


def preset_config(name: str, base: ProcessorConfig | None = None) -> ProcessorConfig:
    values = (base or default_config()).__dict__.copy()
    if name == "Balanced Single":
        values.update(
            mode="Single Image",
            composite_choice="True Color Reproduction Image",
            image_format="png",
            auto_download=True,
            use_night_fallback=True,
            download_workers=4,
            dask_num_workers=1,
            dask_chunk_size="64MiB",
            ram_limit_gb=10.0,
            resampler="native",
        )
    elif name == "Fast IR Check":
        values.update(
            mode="Single Image",
            composite_choice="B13 (Infrared Window)",
            image_format="png",
            auto_download=True,
            use_night_fallback=False,
            download_workers=2,
            dask_num_workers=1,
            dask_chunk_size="64MiB",
            ram_limit_gb=6.0,
            resampler="native",
        )
    elif name == "Low-RAM Timelapse":
        values.update(
            mode="Timelapse",
            composite_choice="B13 (Infrared Window)",
            hours_back=2,
            interval_minutes=20,
            fps=8,
            image_format="png",
            timelapse_format="gif",
            delete_timelapse_frames=False,
            auto_download=True,
            use_night_fallback=False,
            download_workers=2,
            dask_num_workers=1,
            dask_chunk_size="32MiB",
            ram_limit_gb=8.0,
            resampler="native",
        )
    else:
        raise KeyError(f"Unknown preset: {name}")
    return ProcessorConfig(**values)


def serialize_gui_settings(config: ProcessorConfig, output_dir: Path, temp_dir: Path) -> dict:
    return {
        "schema_version": GUI_SETTINGS_SCHEMA_VERSION,
        "app_version": APP_VERSION,
        "config": config.__dict__.copy(),
        "output_dir": str(output_dir),
        "temp_dir": str(temp_dir),
    }


def save_gui_settings(
    config: ProcessorConfig,
    output_dir: Path = OUTPUT_DIR,
    temp_dir: Path = TEMP_DIR,
    settings_path: Path = GUI_SETTINGS_FILE,
) -> None:
    write_json_file(settings_path, serialize_gui_settings(config, output_dir, temp_dir))


def load_gui_settings(settings_path: Path = GUI_SETTINGS_FILE) -> tuple[ProcessorConfig, Path, Path] | None:
    data = load_json_file(settings_path)
    if not data or data.get("schema_version") != GUI_SETTINGS_SCHEMA_VERSION:
        return None
    raw_config = data.get("config", {})
    if not isinstance(raw_config, dict):
        return None
    values = default_config().__dict__.copy()
    for key, value in raw_config.items():
        if key in processor_config_field_names():
            values[key] = value
    try:
        config = ProcessorConfig(**values)
        if setup_configuration_errors(config):
            return None
    except Exception:
        return None
    output_dir = Path(str(data.get("output_dir") or OUTPUT_DIR)).expanduser().resolve()
    temp_dir = Path(str(data.get("temp_dir") or TEMP_DIR)).expanduser().resolve()
    return config, output_dir, temp_dir


def format_environment_results(results: list[object]) -> str:
    lines: list[str] = []
    for result in results:
        ok = bool(getattr(result, "ok", False))
        critical = bool(getattr(result, "critical", True))
        label = "OK" if ok else ("FAIL" if critical else "WARN")
        lines.append(f"[{label}] {getattr(result, 'name', 'check')}: {getattr(result, 'detail', '')}")
    return "\n".join(lines)


def build_error_report(
    error_message: str,
    config: ProcessorConfig | None = None,
    log_text: str = "",
    outputs: Iterable[Path] | None = None,
) -> str:
    lines = [
        app_version_label(),
        f"Python: {sys.executable}",
        f"Project: {PROJECT_DIR}",
        "",
        "Error:",
        error_message,
    ]
    if config is not None:
        lines.extend(["", "Settings:"])
        for key, value in config.__dict__.items():
            lines.append(f"{key}: {value}")
    if outputs:
        lines.extend(["", "Outputs:"])
        lines.extend(str(path) for path in outputs)
    if log_text:
        lines.extend(["", "Recent log:", log_text.strip()])
    return "\n".join(lines).strip() + "\n"


def run(
    config: ProcessorConfig | None = None,
    progress: ProgressCallback | None = None,
    cancel_event: threading.Event | None = None,
    resume_timelapse: bool = True,
) -> list[Path]:
    config = config or default_config()
    configure_logging()
    validate_configuration(config)
    check_cancel(cancel_event)
    LOG.info("App version: %s", APP_VERSION)
    configure_dask(config)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    TEMP_DIR.mkdir(parents=True, exist_ok=True)

    info = parse_url(config.user_url)
    start = datetime.strptime(info.timestamp, "%Y%m%d_%H%M")
    steps = frame_datetimes(start, config.mode, config.hours_back, config.interval_minutes)
    run_id: str | None = None
    frame_dir: Path | None = None
    manifest_path: Path | None = None
    manifest: dict | None = None
    if config.mode == "Timelapse":
        run_id = stable_run_id(config, info, steps)
        frame_dir = timelapse_frame_dir(run_id, OUTPUT_DIR)
        manifest_path = timelapse_manifest_path(run_id, OUTPUT_DIR)
        frame_dir.mkdir(parents=True, exist_ok=True)
        manifest = load_or_create_timelapse_manifest(
            manifest_path,
            run_id,
            config,
            info,
            steps,
            frame_dir,
            resume=resume_timelapse,
        )
        LOG.info("Timelapse run id: %s", run_id)
        LOG.info("Timelapse manifest: %s", manifest_path)
    area_band = area_reference_band(config.composite_choice)
    check_cancel(cancel_event)
    log_memory("startup", config)
    master_area = common_area_from_frames(
        info,
        steps,
        target_pixel_size_m(config.composite_choice, config.use_night_fallback),
        area_band,
        compatibility_bands=area_compatibility_bands(config.composite_choice),
        config=config,
        progress=progress,
        cancel_event=cancel_event,
    )
    check_cancel(cancel_event)
    validate_runtime_dependencies(config, info, start, master_area)

    outputs: list[Path] = []
    for idx, dt in enumerate(steps):
        check_cancel(cancel_event)
        if manifest is not None and resume_timelapse:
            resumed = resume_frame_path(manifest, idx)
            if resumed is not None:
                LOG.info("Reusing existing timelapse frame %s", resumed)
                emit_progress(progress, f"Reusing frame {idx + 1}/{len(steps)}", idx + 1, len(steps))
                outputs.append(resumed)
                update_manifest_frame(manifest, idx, resumed, "reused")
                if manifest_path is not None:
                    save_timelapse_manifest(manifest_path, manifest)
                continue
        result = process_frame(
            dt,
            info,
            master_area,
            idx,
            len(steps),
            config=config,
            progress=progress,
            cancel_event=cancel_event,
            frame_output_dir=frame_dir,
        )
        if result:
            outputs.append(result)
            if manifest is not None:
                update_manifest_frame(manifest, idx, result, "complete")
        elif manifest is not None:
            update_manifest_frame(manifest, idx, None, "failed")
        if manifest is not None and manifest_path is not None:
            save_timelapse_manifest(manifest_path, manifest)

    if config.mode == "Timelapse" and outputs:
        if len(outputs) != len(steps):
            LOG.warning(
                "Timelapse assembled from %s/%s successful frame(s). Check earlier frame errors.",
                len(outputs),
                len(steps),
            )
            emit_progress(
                progress,
                f"Timelapse using {len(outputs)}/{len(steps)} successful frames",
                len(outputs),
                len(steps),
            )
        movie = assemble_timelapse(outputs, info, config=config, progress=progress, cancel_event=cancel_event)
        if manifest is not None and manifest_path is not None:
            manifest["movie"] = str(movie) if movie else None
            manifest["completed_at_utc"] = datetime.now(UTC).replace(tzinfo=None).isoformat(timespec="seconds") + "Z"
            save_timelapse_manifest(manifest_path, manifest)
        return [movie] if movie else []
    if config.mode == "Timelapse":
        raise RuntimeError("Timelapse failed: no frames were processed successfully, so no GIF/MP4 was created.")

    if config.mode == "Single Image" and outputs:
        LOG.info("Saved: %s", outputs[0])
        return outputs
    if config.mode == "Single Image":
        raise RuntimeError("Single image failed: no output was created. Check the log for the frame error.")
    return outputs


class QueueLogHandler(logging.Handler):
    def __init__(self, messages: queue.Queue[tuple[str, object]]) -> None:
        super().__init__()
        self.messages = messages

    def emit(self, record: logging.LogRecord) -> None:
        self.messages.put(("log", self.format(record)))


@dataclass(frozen=True)
class SetupStatus:
    ok: bool
    details: tuple[str, ...]
    warnings: tuple[str, ...]
    errors: tuple[str, ...]

    def display_text(self) -> str:
        lines: list[str] = []
        if self.errors:
            lines.append("Fix before starting:")
            lines.extend(f"- {error}" for error in self.errors)
        else:
            lines.append("Ready to start.")
        if self.details:
            lines.append("")
            lines.extend(self.details)
        if self.warnings:
            lines.append("")
            lines.append("Check before long runs:")
            lines.extend(f"- {warning}" for warning in self.warnings)
        return "\n".join(lines)


def cloud_sync_marker(path: Path) -> str | None:
    for part in path.expanduser().resolve(strict=False).parts:
        normalized = part.lower()
        if normalized in CLOUD_SYNC_EXACT:
            return part
        if any(normalized.startswith(prefix) for prefix in CLOUD_SYNC_PREFIXES):
            return part
    return None


def setup_configuration_errors(config: ProcessorConfig) -> list[str]:
    errors: list[str] = []
    if not config.user_url.strip():
        errors.append("Himawari URL is required.")
    if config.mode not in {"Single Image", "Timelapse"}:
        errors.append('Mode must be "Single Image" or "Timelapse".')
    if config.composite_choice not in COMPOSITE_BANDS:
        errors.append(f"Unsupported product: {config.composite_choice}")
    if config.interval_minutes <= 0:
        errors.append("Interval minutes must be positive.")
    if config.hours_back <= 0:
        errors.append("Hours back must be positive.")
    if config.fps <= 0:
        errors.append("FPS must be positive.")
    if config.download_workers <= 0:
        errors.append("Download workers must be positive.")
    if config.dask_num_workers <= 0:
        errors.append("Dask workers must be positive.")
    if config.ram_limit_gb <= 0:
        errors.append("RAM limit must be positive.")
    if config.image_format.lower() not in {"png", "tif", "tiff", "geotiff"}:
        errors.append('Image format must be "png" or "tif".')
    if config.timelapse_format.lower() not in {"gif", "mp4"}:
        errors.append('Timelapse format must be "gif" or "mp4".')
    if config.resampler.lower() not in {"native", "nearest"}:
        errors.append('Resampler must be "native" or "nearest".')
    if config.add_border_lines:
        try:
            parse_rgb_color(config.border_line_color)
        except ValueError as exc:
            errors.append(str(exc))
        if config.border_line_width <= 0:
            errors.append("Border line width must be positive.")
    return errors


def build_setup_status(
    config: ProcessorConfig,
    output_dir: Path = OUTPUT_DIR,
    temp_dir: Path = TEMP_DIR,
) -> SetupStatus:
    details: list[str] = []
    warnings: list[str] = []
    errors = setup_configuration_errors(config)
    info: UrlInfo | None = None
    start: datetime | None = None

    if config.user_url.strip():
        try:
            info = parse_url(config.user_url)
            start = datetime.strptime(info.timestamp, "%Y%m%d_%H%M")
            details.append(f"Source: {info.area} {info.timestamp}, {info.total_segments} segments per band")
        except Exception as exc:
            errors.append(str(exc))

    if config.composite_choice in COMPOSITE_BANDS:
        bands = required_bands(config.composite_choice, use_night_fallback=config.use_night_fallback)
        band_list = ", ".join(bands)
        details.append(f"Product: {config.composite_choice} ({len(bands)} band(s): {band_list})")
        if info is not None and start is not None:
            try:
                frames = len(frame_datetimes(start, config.mode, config.hours_back, config.interval_minutes))
                total_segments = frames * len(bands) * info.total_segments
                frame_word = "frame" if frames == 1 else "frames"
                details.append(f"Download estimate: {total_segments} segment file(s) across {frames} {frame_word}")
            except Exception as exc:
                errors.append(str(exc))
            if (
                info.area == "FLDK"
                and config.image_format.lower() == "png"
                and target_pixel_size_m(config.composite_choice) <= 500
            ):
                warnings.append(
                    "Full-disk 500 m PNG jobs may auto-switch to GeoTIFF for low-RAM writing."
                )

    if config.add_border_lines:
        overlay_notes = ["pycoast", "aggdraw", "GSHHS/WDBII shapefiles under overlays/"]
        missing_packages = [module for module in ("pycoast", "aggdraw") if not has_module(module)]
        if missing_packages:
            overlay_notes.append("missing package(s): " + ", ".join(missing_packages))
        if not (PROJECT_DIR / "overlays").exists():
            overlay_notes.append("overlays/ folder not found")
        warnings.append("Border lines require " + "; ".join(overlay_notes) + ".")

    cloud_matches = []
    for label, path in (("project", PROJECT_DIR), ("output", output_dir), ("temp", temp_dir)):
        marker = cloud_sync_marker(path)
        if marker:
            cloud_matches.append(f"{label}: {path} ({marker})")
    if cloud_matches:
        warnings.append(
            "Cloud-sync path detected; large output/temp writes are safer in local folders. "
            + "; ".join(cloud_matches)
        )

    return SetupStatus(
        ok=not errors,
        details=tuple(details),
        warnings=tuple(warnings),
        errors=tuple(errors),
    )


class HimawariProcessorApp:
    def __init__(self, root: tk.Tk) -> None:
        loaded_settings = load_gui_settings()
        initial_config = loaded_settings[0] if loaded_settings else default_config()
        if loaded_settings:
            global OUTPUT_DIR, TEMP_DIR
            OUTPUT_DIR = loaded_settings[1]
            TEMP_DIR = loaded_settings[2]

        self.root = root
        self.root.title(f"{APP_DISPLAY_NAME} v{APP_VERSION}")
        self.root.geometry("1060x800")
        self.root.minsize(820, 520)
        self.messages: queue.Queue[tuple[str, object]] = queue.Queue()
        self.worker: threading.Thread | None = None
        self.is_running = False
        self.cancel_event = threading.Event()
        self.last_outputs: list[Path] = []
        self.last_config: ProcessorConfig | None = None
        self.last_error_report = ""

        self.url_var = tk.StringVar(value=initial_config.user_url)
        self.mode_var = tk.StringVar(value=initial_config.mode)
        self.composite_var = tk.StringVar(value=initial_config.composite_choice)
        self.hours_var = tk.StringVar(value=str(initial_config.hours_back))
        self.interval_var = tk.StringVar(value=str(initial_config.interval_minutes))
        self.fps_var = tk.StringVar(value=str(initial_config.fps))
        self.download_workers_var = tk.StringVar(value=str(initial_config.download_workers))
        self.dask_workers_var = tk.StringVar(value=str(initial_config.dask_num_workers))
        self.chunk_var = tk.StringVar(value=initial_config.dask_chunk_size)
        self.ram_limit_var = tk.StringVar(value=str(initial_config.ram_limit_gb))
        self.image_format_var = tk.StringVar(value=initial_config.image_format)
        self.resampler_var = tk.StringVar(value=initial_config.resampler)
        self.timelapse_format_var = tk.StringVar(value=initial_config.timelapse_format)
        self.auto_download_var = tk.BooleanVar(value=initial_config.auto_download)
        self.night_fallback_var = tk.BooleanVar(value=initial_config.use_night_fallback)
        self.delete_frames_var = tk.BooleanVar(value=initial_config.delete_timelapse_frames)
        self.quality_fallback_var = tk.BooleanVar(value=initial_config.allow_quality_fallback)
        self.border_lines_var = tk.BooleanVar(value=initial_config.add_border_lines)
        self.border_color_var = tk.StringVar(value=initial_config.border_line_color)
        self.border_width_var = tk.StringVar(value=str(initial_config.border_line_width))
        self.status_var = tk.StringVar(value="Ready")
        self.progress_var = tk.DoubleVar(value=0)
        self.setup_status_var = tk.StringVar(value="")
        self.output_dir_var = tk.StringVar(value=str(OUTPUT_DIR))
        self.temp_dir_var = tk.StringVar(value=str(TEMP_DIR))

        self._build_ui()
        self._install_log_handler()
        self._install_setup_watchers()
        self._update_setup_status()
        self._set_running(False)
        self.root.after(100, self._poll_messages)

    def _install_log_handler(self) -> None:
        configure_logging()
        handler = QueueLogHandler(self.messages)
        handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s", "%H:%M:%S"))
        LOG.addHandler(handler)

    @staticmethod
    def _mouse_wheel_units(event: tk.Event) -> int:
        delta = getattr(event, "delta", 0)
        if delta:
            return -1 * int(delta / 120)
        button = getattr(event, "num", None)
        if button == 4:
            return -1
        if button == 5:
            return 1
        return 0

    def _create_scrollable_tab(self, notebook: ttk.Notebook, label: str) -> ttk.Frame:
        tab = ttk.Frame(notebook)
        tab.columnconfigure(0, weight=1)
        tab.rowconfigure(0, weight=1)
        notebook.add(tab, text=label)

        canvas = tk.Canvas(tab, highlightthickness=0)
        scrollbar = ttk.Scrollbar(tab, orient="vertical", command=canvas.yview)
        inner = ttk.Frame(canvas, padding=(12, 10))
        window_id = canvas.create_window((0, 0), window=inner, anchor="nw")
        canvas.configure(yscrollcommand=scrollbar.set)

        canvas.grid(row=0, column=0, sticky="nsew")
        scrollbar.grid(row=0, column=1, sticky="ns")

        def refresh_scroll_region(_event: tk.Event | None = None) -> None:
            canvas.configure(scrollregion=canvas.bbox("all"))

        def resize_inner(event: tk.Event) -> None:
            canvas.itemconfigure(window_id, width=event.width)

        def on_mouse_wheel(event: tk.Event) -> str:
            units = self._mouse_wheel_units(event)
            if units:
                canvas.yview_scroll(units, "units")
            return "break"

        def bind_mouse_wheel(_event: tk.Event) -> None:
            canvas.bind_all("<MouseWheel>", on_mouse_wheel)
            canvas.bind_all("<Button-4>", on_mouse_wheel)
            canvas.bind_all("<Button-5>", on_mouse_wheel)

        def unbind_mouse_wheel(_event: tk.Event) -> None:
            canvas.unbind_all("<MouseWheel>")
            canvas.unbind_all("<Button-4>")
            canvas.unbind_all("<Button-5>")

        inner.bind("<Configure>", refresh_scroll_region)
        canvas.bind("<Configure>", resize_inner)
        canvas.bind("<Enter>", bind_mouse_wheel)
        canvas.bind("<Leave>", unbind_mouse_wheel)
        inner.bind("<Enter>", bind_mouse_wheel)
        inner.bind("<Leave>", unbind_mouse_wheel)
        return inner

    @staticmethod
    def _initial_split_position(total_height: int, ratio: float = 0.45) -> int:
        if total_height <= 0:
            return 220
        minimum_pane_height = 160
        if total_height <= minimum_pane_height * 2:
            return max(80, total_height // 2)
        desired = int(total_height * ratio)
        return max(minimum_pane_height, min(desired, total_height - minimum_pane_height))

    def _configure_main_split(self) -> None:
        def set_initial_sash() -> None:
            height = self.main_pane.winfo_height()
            self.main_pane.sashpos(0, self._initial_split_position(height))

        self.root.after(100, set_initial_sash)

    def _build_ui(self) -> None:
        style = ttk.Style(self.root)
        try:
            style.theme_use("clam")
        except tk.TclError:
            pass
        style.configure("Title.TLabel", font=("Segoe UI", 17, "bold"))
        style.configure("Section.TLabelframe", padding=12)
        style.configure("Section.TLabelframe.Label", font=("Segoe UI", 10, "bold"))
        style.configure("Primary.TButton", font=("Segoe UI", 10, "bold"))
        style.configure("Status.TLabel", justify="left")

        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(1, weight=1)

        title = ttk.Label(
            self.root,
            text=f"{APP_DISPLAY_NAME} v{APP_VERSION}",
            style="Title.TLabel",
        )
        title.grid(row=0, column=0, sticky="w", padx=16, pady=(14, 6))

        self.main_pane = ttk.PanedWindow(self.root, orient="vertical")
        self.main_pane.grid(row=1, column=0, sticky="nsew", padx=16, pady=8)

        notebook_pane = ttk.Frame(self.main_pane)
        notebook_pane.columnconfigure(0, weight=1)
        notebook_pane.rowconfigure(0, weight=1)
        self.main_pane.add(notebook_pane, weight=1)

        self.notebook = ttk.Notebook(notebook_pane)
        self.notebook.grid(row=0, column=0, sticky="nsew")

        settings = self._create_scrollable_tab(self.notebook, "Run Setup")
        advanced = self._create_scrollable_tab(self.notebook, "Advanced")

        for col in (0, 1):
            settings.columnconfigure(col, weight=1)
            advanced.columnconfigure(col, weight=1)

        source_frame = ttk.LabelFrame(settings, text="Source", style="Section.TLabelframe")
        source_frame.grid(row=0, column=0, columnspan=2, sticky="ew", pady=(0, 10))
        source_frame.columnconfigure(0, weight=1)
        ttk.Label(source_frame, text="NOAA AWS Himawari URL").grid(row=0, column=0, sticky="w")
        ttk.Entry(source_frame, textvariable=self.url_var).grid(row=1, column=0, sticky="ew", pady=(2, 0))
        self.latest_url_button = ttk.Button(source_frame, text="Latest FLDK", command=self._fill_latest_fldk_url)
        self.latest_url_button.grid(row=1, column=1, sticky="ew", padx=(8, 0), pady=(2, 0))

        product_frame = ttk.LabelFrame(settings, text="Product", style="Section.TLabelframe")
        product_frame.grid(row=1, column=0, sticky="nsew", padx=(0, 8), pady=(0, 10))
        product_frame.columnconfigure(0, weight=1)
        product_frame.columnconfigure(1, weight=1)
        ttk.Label(product_frame, text="Safe Preset").grid(row=0, column=0, sticky="w")
        preset_box = ttk.Combobox(
            product_frame,
            values=("Balanced Single", "Fast IR Check", "Low-RAM Timelapse"),
            state="readonly",
        )
        preset_box.grid(row=1, column=0, columnspan=2, sticky="ew", pady=(2, 10))
        preset_box.bind("<<ComboboxSelected>>", lambda _event: self._apply_preset(preset_box.get()))
        mode_box = ttk.Combobox(
            product_frame,
            textvariable=self.mode_var,
            values=("Single Image", "Timelapse"),
            state="readonly",
        )
        ttk.Label(product_frame, text="Output Mode").grid(row=2, column=0, sticky="w")
        mode_box.grid(row=3, column=0, sticky="ew", pady=(2, 10), padx=(0, 8))
        mode_box.bind("<<ComboboxSelected>>", lambda _event: self._refresh_mode_state())

        ttk.Label(product_frame, text="Image Format").grid(row=2, column=1, sticky="w")
        ttk.Combobox(
            product_frame,
            textvariable=self.image_format_var,
            values=("png", "tif"),
            state="readonly",
        ).grid(row=3, column=1, sticky="ew", pady=(2, 10))

        ttk.Label(product_frame, text="Composite / Band").grid(row=4, column=0, columnspan=2, sticky="w")
        ttk.Combobox(
            product_frame,
            textvariable=self.composite_var,
            values=tuple(sorted(COMPOSITE_BANDS)),
            state="readonly",
        ).grid(row=5, column=0, columnspan=2, sticky="ew", pady=(2, 0))

        timing_frame = ttk.LabelFrame(settings, text="Timelapse", style="Section.TLabelframe")
        timing_frame.grid(row=1, column=1, sticky="nsew", padx=(8, 0), pady=(0, 10))
        for col in (0, 1, 2):
            timing_frame.columnconfigure(col, weight=1)
        ttk.Label(timing_frame, text="Hours Back").grid(row=0, column=0, sticky="w")
        self.hours_spin = ttk.Spinbox(timing_frame, from_=1, to=240, textvariable=self.hours_var, width=8)
        self.hours_spin.grid(row=1, column=0, sticky="ew", padx=(0, 8), pady=(2, 10))

        ttk.Label(timing_frame, text="Interval Minutes").grid(row=0, column=1, sticky="w")
        self.interval_spin = ttk.Spinbox(timing_frame, from_=1, to=120, textvariable=self.interval_var, width=8)
        self.interval_spin.grid(row=1, column=1, sticky="ew", padx=(0, 8), pady=(2, 10))

        ttk.Label(timing_frame, text="FPS").grid(row=0, column=2, sticky="w")
        self.fps_spin = ttk.Spinbox(timing_frame, from_=1, to=60, textvariable=self.fps_var, width=8)
        self.fps_spin.grid(row=1, column=2, sticky="ew", pady=(2, 10))

        ttk.Label(timing_frame, text="Format").grid(row=2, column=0, sticky="w")
        ttk.Combobox(
            timing_frame,
            textvariable=self.timelapse_format_var,
            values=("gif", "mp4"),
            state="readonly",
        ).grid(row=3, column=0, sticky="ew", padx=(0, 8), pady=(2, 0))
        ttk.Checkbutton(
            timing_frame,
            text="Delete frame images after assembly",
            variable=self.delete_frames_var,
        ).grid(row=3, column=1, columnspan=2, sticky="w", pady=(2, 0))

        options_frame = ttk.LabelFrame(settings, text="Options", style="Section.TLabelframe")
        options_frame.grid(row=2, column=0, columnspan=2, sticky="ew")
        options_frame.columnconfigure(0, weight=1)
        options_frame.columnconfigure(1, weight=1)
        ttk.Checkbutton(
            options_frame,
            text="Auto-download missing satellite files",
            variable=self.auto_download_var,
        ).grid(row=0, column=0, sticky="w", pady=(2, 4))
        ttk.Checkbutton(
            options_frame,
            text="Use night fallback for day-only products",
            variable=self.night_fallback_var,
        ).grid(row=0, column=1, sticky="w", pady=(2, 4))
        ttk.Checkbutton(
            options_frame,
            text="Allow lower-quality fallback if true color dependencies are missing",
            variable=self.quality_fallback_var,
        ).grid(row=1, column=0, columnspan=2, sticky="w", pady=(4, 4))
        ttk.Checkbutton(
            options_frame,
            text="Draw coastline and country border lines",
            variable=self.border_lines_var,
        ).grid(row=2, column=0, sticky="w", pady=(4, 4))
        self.overlay_check_button = ttk.Button(options_frame, text="Check Overlay Setup", command=self._check_overlays)
        self.overlay_check_button.grid(row=2, column=1, sticky="e", padx=(0, 8), pady=(4, 4))
        ttk.Label(options_frame, text="Border Color").grid(row=3, column=1, sticky="w", padx=(0, 8))
        color_row = ttk.Frame(options_frame)
        color_row.grid(row=4, column=1, sticky="ew")
        color_row.columnconfigure(0, weight=1)
        ttk.Entry(color_row, textvariable=self.border_color_var, width=14).grid(row=0, column=0, sticky="ew")
        ttk.Button(color_row, text="Pick", command=self._choose_border_color).grid(row=0, column=1, padx=(8, 0))
        ttk.Label(options_frame, text="Border Width").grid(row=3, column=0, sticky="w", pady=(4, 4))
        ttk.Spinbox(
            options_frame,
            from_=0.25,
            to=5.0,
            increment=0.25,
            textvariable=self.border_width_var,
            width=8,
        ).grid(row=4, column=0, sticky="w", pady=(2, 0))

        status_frame = ttk.LabelFrame(settings, text="Setup Status", style="Section.TLabelframe")
        status_frame.grid(row=3, column=0, columnspan=2, sticky="ew", pady=(10, 0))
        status_frame.columnconfigure(0, weight=1)
        ttk.Label(
            status_frame,
            textvariable=self.setup_status_var,
            style="Status.TLabel",
            wraplength=960,
        ).grid(row=0, column=0, sticky="ew")

        performance_frame = ttk.LabelFrame(advanced, text="Performance", style="Section.TLabelframe")
        performance_frame.grid(row=0, column=0, sticky="nsew", padx=(0, 8), pady=(0, 10))
        for col in (0, 1):
            performance_frame.columnconfigure(col, weight=1)
        ttk.Label(performance_frame, text="Download Workers").grid(row=0, column=0, sticky="w")
        ttk.Spinbox(performance_frame, from_=1, to=4, textvariable=self.download_workers_var, width=8).grid(
            row=1, column=0, sticky="ew", padx=(0, 8), pady=(2, 10)
        )
        ttk.Label(performance_frame, text="Dask Workers").grid(row=0, column=1, sticky="w")
        ttk.Spinbox(performance_frame, from_=1, to=2, textvariable=self.dask_workers_var, width=8).grid(
            row=1, column=1, sticky="ew", pady=(2, 10)
        )
        ttk.Label(performance_frame, text="Dask Chunk Size").grid(row=2, column=0, sticky="w")
        ttk.Combobox(
            performance_frame,
            textvariable=self.chunk_var,
            values=("32MiB", "64MiB", "128MiB"),
            state="readonly",
        ).grid(row=3, column=0, sticky="ew", padx=(0, 8), pady=(2, 0))
        ttk.Label(performance_frame, text="RAM Limit GiB").grid(row=2, column=1, sticky="w")
        ttk.Spinbox(performance_frame, from_=1, to=64, increment=0.5, textvariable=self.ram_limit_var, width=8).grid(
            row=3, column=1, sticky="ew", pady=(2, 0)
        )

        paths_frame = ttk.LabelFrame(advanced, text="Paths and Resampling", style="Section.TLabelframe")
        paths_frame.grid(row=0, column=1, sticky="nsew", padx=(8, 0), pady=(0, 10))
        paths_frame.columnconfigure(0, weight=1)
        paths_frame.columnconfigure(1, weight=1)
        ttk.Label(paths_frame, text="Resampler").grid(row=0, column=0, sticky="w")
        ttk.Combobox(
            paths_frame,
            textvariable=self.resampler_var,
            values=("native", "nearest"),
            state="readonly",
        ).grid(row=1, column=0, columnspan=2, sticky="ew", pady=(2, 10))

        ttk.Label(paths_frame, text="Output Folder").grid(row=2, column=0, columnspan=2, sticky="w")
        ttk.Entry(paths_frame, textvariable=self.output_dir_var, state="readonly").grid(
            row=3, column=0, columnspan=2, sticky="ew", pady=(2, 8)
        )
        ttk.Label(paths_frame, text="Temp Folder").grid(row=4, column=0, columnspan=2, sticky="w")
        ttk.Entry(paths_frame, textvariable=self.temp_dir_var, state="readonly").grid(
            row=5, column=0, columnspan=2, sticky="ew", pady=(2, 8)
        )
        self.choose_output_button = ttk.Button(
            paths_frame,
            text="Choose Output Folder",
            command=self._choose_output_dir,
        )
        self.choose_output_button.grid(row=6, column=0, sticky="ew", padx=(0, 8), pady=(2, 8))
        self.choose_temp_button = ttk.Button(
            paths_frame,
            text="Choose Temp Folder",
            command=self._choose_temp_dir,
        )
        self.choose_temp_button.grid(row=6, column=1, sticky="ew", pady=(2, 8))
        self.open_temp_button = ttk.Button(
            paths_frame,
            text="Open Temp Folder",
            command=self._open_temp_folder,
        )
        self.open_temp_button.grid(row=7, column=0, columnspan=2, sticky="ew", pady=(2, 0))
        self._refresh_path_fields()
        self._path_controls = (
            self.choose_output_button,
            self.choose_temp_button,
            self.open_temp_button,
        )
        self._refresh_mode_state()

        self.log_frame = ttk.Frame(self.main_pane, padding=(0, 4, 0, 0))
        self.log_frame.columnconfigure(0, weight=1)
        self.log_frame.rowconfigure(1, weight=1)
        self.main_pane.add(self.log_frame, weight=1)

        progress = ttk.Progressbar(self.log_frame, variable=self.progress_var, maximum=100)
        progress.grid(row=0, column=0, sticky="ew", pady=(0, 6))
        ttk.Label(self.log_frame, textvariable=self.status_var).grid(row=0, column=1, sticky="e", padx=(10, 0))

        self.log_text = tk.Text(self.log_frame, height=16, wrap="word", state="disabled")
        self.log_text.grid(row=1, column=0, columnspan=2, sticky="nsew")
        scroll = ttk.Scrollbar(self.log_frame, orient="vertical", command=self.log_text.yview)
        scroll.grid(row=1, column=2, sticky="ns")
        self.log_text.configure(yscrollcommand=scroll.set)

        buttons = ttk.Frame(self.root, padding=(16, 0, 16, 16))
        buttons.grid(row=2, column=0, sticky="ew")
        buttons.columnconfigure(0, weight=1)
        self.start_button = ttk.Button(buttons, text="Start Processing", command=self._start, style="Primary.TButton")
        self.start_button.grid(row=0, column=1, padx=(0, 8))
        self.stop_button = ttk.Button(buttons, text="Stop", command=self._stop_current_task)
        self.stop_button.grid(row=0, column=2, padx=(0, 8))
        self.open_output_button = ttk.Button(buttons, text="Open Outputs", command=self._open_output_folder)
        self.open_output_button.grid(
            row=0, column=3, padx=(0, 8)
        )
        self.check_env_button = ttk.Button(buttons, text="Check Env", command=self._open_environment_check)
        self.check_env_button.grid(
            row=0, column=4, padx=(0, 8)
        )
        self.quick_fix_button = ttk.Button(buttons, text="Quick Fix", command=self._open_environment_fix)
        self.quick_fix_button.grid(
            row=0, column=5, padx=(0, 8)
        )
        self.open_last_button = ttk.Button(buttons, text="Open Last", command=self._open_last_output)
        self.open_last_button.grid(row=0, column=6, padx=(0, 8))
        self.copy_paths_button = ttk.Button(buttons, text="Copy Paths", command=self._copy_output_paths)
        self.copy_paths_button.grid(row=0, column=7, padx=(0, 8))
        self.copy_error_button = ttk.Button(buttons, text="Copy Error", command=self._copy_error_report)
        self.copy_error_button.grid(row=0, column=8, padx=(0, 8))
        ttk.Button(buttons, text="Close", command=self._terminate_app).grid(row=0, column=9)
        self._configure_main_split()

    def _install_setup_watchers(self) -> None:
        watched_vars = (
            self.url_var,
            self.mode_var,
            self.composite_var,
            self.hours_var,
            self.interval_var,
            self.fps_var,
            self.download_workers_var,
            self.dask_workers_var,
            self.chunk_var,
            self.ram_limit_var,
            self.image_format_var,
            self.resampler_var,
            self.timelapse_format_var,
            self.auto_download_var,
            self.night_fallback_var,
            self.delete_frames_var,
            self.quality_fallback_var,
            self.border_lines_var,
            self.border_color_var,
            self.border_width_var,
        )
        for watched_var in watched_vars:
            watched_var.trace_add("write", lambda *_args: self._update_setup_status())

    def _update_setup_status(self) -> SetupStatus:
        try:
            config = self._read_config()
            setup_status = build_setup_status(config, OUTPUT_DIR, TEMP_DIR)
            self.setup_status_var.set(setup_status.display_text())
        except Exception as exc:
            setup_status = SetupStatus(False, (), (), (f"Invalid setup value: {exc}",))
            self.setup_status_var.set(setup_status.display_text())
        return setup_status

    def _refresh_path_fields(self) -> None:
        self.output_dir_var.set(str(OUTPUT_DIR))
        self.temp_dir_var.set(str(TEMP_DIR))

    def _write_current_settings(self) -> None:
        try:
            save_gui_settings(self._read_config(), OUTPUT_DIR, TEMP_DIR)
        except Exception as exc:
            self._append_log(f"Could not save settings: {exc}")

    def _set_config_vars(self, config: ProcessorConfig) -> None:
        self.url_var.set(config.user_url)
        self.mode_var.set(config.mode)
        self.composite_var.set(config.composite_choice)
        self.hours_var.set(str(config.hours_back))
        self.interval_var.set(str(config.interval_minutes))
        self.fps_var.set(str(config.fps))
        self.download_workers_var.set(str(config.download_workers))
        self.dask_workers_var.set(str(config.dask_num_workers))
        self.chunk_var.set(config.dask_chunk_size)
        self.ram_limit_var.set(str(config.ram_limit_gb))
        self.image_format_var.set(config.image_format)
        self.resampler_var.set(config.resampler)
        self.timelapse_format_var.set(config.timelapse_format)
        self.auto_download_var.set(config.auto_download)
        self.night_fallback_var.set(config.use_night_fallback)
        self.delete_frames_var.set(config.delete_timelapse_frames)
        self.quality_fallback_var.set(config.allow_quality_fallback)
        self.border_lines_var.set(config.add_border_lines)
        self.border_color_var.set(config.border_line_color)
        self.border_width_var.set(str(config.border_line_width))
        self._refresh_mode_state()
        self._update_setup_status()

    def _apply_preset(self, name: str) -> None:
        try:
            config = preset_config(name, self._read_config())
        except Exception as exc:
            messagebox.showerror("Preset failed", str(exc))
            return
        self._set_config_vars(config)
        self._write_current_settings()
        self._append_log(f"Applied preset: {name}")

    def _refresh_mode_state(self) -> None:
        is_timelapse = self.mode_var.get() == "Timelapse"
        state = "normal" if is_timelapse and not self.is_running else "disabled"
        for widget in (self.hours_spin, self.interval_spin, self.fps_spin):
            widget.configure(state=state)

    def _set_running(self, running: bool) -> None:
        self.is_running = running
        self.start_button.configure(state="disabled" if running else "normal")
        self.stop_button.configure(state="normal" if running else "disabled")
        mutable_state = "disabled" if running else "normal"
        for widget in getattr(self, "_path_controls", ()):
            widget.configure(state=mutable_state)
        for widget in (
            getattr(self, "open_output_button", None),
            getattr(self, "check_env_button", None),
            getattr(self, "quick_fix_button", None),
            getattr(self, "latest_url_button", None),
            getattr(self, "overlay_check_button", None),
            getattr(self, "open_last_button", None),
            getattr(self, "copy_paths_button", None),
            getattr(self, "copy_error_button", None),
        ):
            if widget is not None:
                widget.configure(state=mutable_state)
        self._refresh_mode_state()

    def _choose_output_dir(self) -> None:
        global OUTPUT_DIR
        selected = filedialog.askdirectory(initialdir=OUTPUT_DIR)
        if selected:
            OUTPUT_DIR = Path(selected).expanduser().resolve()
            self._refresh_path_fields()
            self._update_setup_status()
            self._write_current_settings()
            self._append_log(f"Output folder set to {OUTPUT_DIR}")

    def _choose_temp_dir(self) -> None:
        global TEMP_DIR
        selected = filedialog.askdirectory(initialdir=TEMP_DIR)
        if selected:
            TEMP_DIR = Path(selected).expanduser().resolve()
            self._refresh_path_fields()
            self._update_setup_status()
            self._write_current_settings()
            self._append_log(f"Temp folder set to {TEMP_DIR}")

    def _open_output_folder(self) -> None:
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        if os.name == "nt":
            os.startfile(str(OUTPUT_DIR))
        else:
            messagebox.showinfo("Output folder", str(OUTPUT_DIR))

    def _open_temp_folder(self) -> None:
        TEMP_DIR.mkdir(parents=True, exist_ok=True)
        if os.name == "nt":
            os.startfile(str(TEMP_DIR))
        else:
            messagebox.showinfo("Temp folder", str(TEMP_DIR))

    def _open_path(self, path: Path) -> None:
        target = path.parent if path.is_file() else path
        if os.name == "nt":
            os.startfile(str(target))
        else:
            messagebox.showinfo("Path", str(target))

    def _open_last_output(self) -> None:
        if not self.last_outputs:
            messagebox.showinfo("Open last output", "No completed output is available yet.")
            return
        self._open_path(self.last_outputs[-1])

    def _copy_to_clipboard(self, text: str) -> None:
        self.root.clipboard_clear()
        self.root.clipboard_append(text)
        self.root.update_idletasks()

    def _copy_output_paths(self) -> None:
        if not self.last_outputs:
            messagebox.showinfo("Copy paths", "No completed output paths are available yet.")
            return
        text = "\n".join(str(path) for path in self.last_outputs)
        self._copy_to_clipboard(text)
        self._append_log("Output path(s) copied to clipboard.")

    def _copy_error_report(self) -> None:
        if not self.last_error_report:
            self.last_error_report = build_error_report(
                "No processing error has been recorded.",
                self.last_config,
                self.log_text.get("1.0", "end"),
                self.last_outputs,
            )
        self._copy_to_clipboard(self.last_error_report)
        self._append_log("Error report copied to clipboard.")

    def _open_environment_command(self, args: list[str], label: str) -> None:
        command = [sys.executable, str(PROJECT_DIR / "check_environment.py"), *args]
        try:
            if os.name == "nt":
                subprocess.Popen(
                    ["cmd", "/k", *command],
                    cwd=PROJECT_DIR,
                    creationflags=subprocess.CREATE_NEW_CONSOLE,
                )
            else:
                subprocess.Popen(command, cwd=PROJECT_DIR)
            self._append_log(f"{label} started in a separate console.")
        except Exception as exc:
            messagebox.showerror(f"{label} failed", str(exc))

    def _open_environment_check(self) -> None:
        self.status_var.set("Checking environment")
        self._append_log("Running environment check inline.")

        def worker() -> None:
            try:
                import check_environment

                results = check_environment.run_checks()
                self.messages.put(("env_results", results))
            except Exception as exc:
                self.messages.put(("env_error", str(exc)))

        threading.Thread(target=worker, daemon=True).start()

    def _open_environment_fix(self) -> None:
        self._open_environment_command(["--fix"], "Environment quick fix")

    def _check_overlays(self) -> None:
        status = overlay_status()
        self._append_log(status.display_text())
        if status.ok:
            messagebox.showinfo("Overlay setup", status.display_text())
        else:
            messagebox.showwarning("Overlay setup", status.display_text())

    def _fill_latest_fldk_url(self) -> None:
        self.status_var.set("Finding latest FLDK")
        self.latest_url_button.configure(state="disabled")
        self._append_log("Looking for latest FLDK scan on NOAA AWS.")

        def worker() -> None:
            try:
                self.messages.put(("latest_url", find_latest_fldk_url()))
            except Exception as exc:
                self.messages.put(("latest_url_error", str(exc)))

        threading.Thread(target=worker, daemon=True).start()

    def _choose_border_color(self) -> None:
        initial = self.border_color_var.get()
        try:
            rgb = parse_rgb_color(initial)
            initial_color = "#{:02x}{:02x}{:02x}".format(*rgb)
        except ValueError:
            initial_color = "#00ff00"
        selected, hex_value = colorchooser.askcolor(color=initial_color, title="Choose border line color")
        if hex_value:
            self.border_color_var.set(hex_value)

    def _read_config(self) -> ProcessorConfig:
        return ProcessorConfig(
            user_url=self.url_var.get().strip(),
            mode=self.mode_var.get(),
            composite_choice=self.composite_var.get(),
            hours_back=int(self.hours_var.get()),
            interval_minutes=int(self.interval_var.get()),
            fps=int(self.fps_var.get()),
            auto_download=self.auto_download_var.get(),
            use_night_fallback=self.night_fallback_var.get(),
            download_workers=int(self.download_workers_var.get()),
            timelapse_format=self.timelapse_format_var.get(),
            delete_timelapse_frames=self.delete_frames_var.get(),
            image_format=self.image_format_var.get(),
            resampler=self.resampler_var.get(),
            allow_quality_fallback=self.quality_fallback_var.get(),
            add_border_lines=self.border_lines_var.get(),
            border_line_color=self.border_color_var.get(),
            border_line_width=float(self.border_width_var.get()),
            ram_limit_gb=float(self.ram_limit_var.get()),
            dask_chunk_size=self.chunk_var.get(),
            dask_num_workers=int(self.dask_workers_var.get()),
        )

    def _start(self) -> None:
        try:
            config = self._read_config()
            validate_configuration(config)
            setup_status = self._update_setup_status()
            if not setup_status.ok:
                raise ValueError("\n".join(setup_status.errors))
            preflight = preflight_run(config, OUTPUT_DIR, TEMP_DIR)
            if not preflight.ok:
                raise ValueError("\n".join(preflight.errors))
        except Exception as exc:
            self._append_log(f"Invalid settings: {exc}")
            messagebox.showerror("Invalid settings", str(exc))
            return

        if not messagebox.askokcancel("Run summary", preflight.display_text() + "\n\nStart processing?"):
            self._append_log("Run canceled before processing.")
            return

        self.progress_var.set(0)
        self.status_var.set("Starting")
        self.cancel_event.clear()
        self._set_running(True)
        self.last_config = config
        self.last_error_report = ""
        self._write_current_settings()
        self._append_log(f"Starting processing - version {APP_VERSION}")
        self._append_log(preflight.display_text())

        self.worker = threading.Thread(target=self._run_worker, args=(config,), daemon=True)
        self.worker.start()

    def _stop_current_task(self) -> None:
        if not self.is_running:
            return
        self.cancel_event.set()
        self.status_var.set("Canceling")
        self.stop_button.configure(state="disabled")
        self._append_log("Stop requested; canceling active downloads and pending processing.")

    def _terminate_app(self) -> None:
        self.cancel_event.set()
        self._append_log("Terminate requested.")
        self.root.after(100, self.root.destroy)

    def _run_worker(self, config: ProcessorConfig) -> None:
        def progress(message: str, current: int | None, total: int | None) -> None:
            self.messages.put(("progress", (message, current, total)))

        try:
            outputs = run(config, progress=progress, cancel_event=self.cancel_event)
            self.messages.put(("done", outputs))
        except ProcessingCancelled as exc:
            LOG.info("%s", exc)
            self.messages.put(("canceled", str(exc)))
        except Exception as exc:
            LOG.exception("Processing failed")
            self.messages.put(("error", str(exc)))

    def _poll_messages(self) -> None:
        while True:
            try:
                kind, payload = self.messages.get_nowait()
            except queue.Empty:
                break

            if kind == "log":
                self._append_log(str(payload))
            elif kind == "progress":
                message, current, total = payload  # type: ignore[misc]
                self.status_var.set(str(message))
                if current is not None and total:
                    self.progress_var.set(min(100.0, (float(current) / float(total)) * 100.0))
            elif kind == "done":
                outputs = payload
                self.last_outputs = list(outputs)
                self._set_running(False)
                self.progress_var.set(100)
                self.status_var.set("Done")
                output_lines = "\n".join(str(path) for path in outputs) if outputs else "No output paths returned."
                self._append_log("Finished. Outputs:\n" + output_lines)
                messagebox.showinfo("Done", f"Processing finished.\n\nOutputs:\n{output_lines}")
            elif kind == "env_results":
                results = payload
                text = format_environment_results(results)
                self.status_var.set("Environment checked")
                self._append_log("Environment check results:\n" + text)
                messagebox.showinfo("Environment check", text)
            elif kind == "env_error":
                self.status_var.set("Environment check failed")
                self._append_log(f"Environment check failed: {payload}")
                messagebox.showerror("Environment check failed", str(payload))
            elif kind == "latest_url":
                self.latest_url_button.configure(state="normal" if not self.is_running else "disabled")
                self.url_var.set(str(payload))
                self.status_var.set("Latest FLDK selected")
                self._update_setup_status()
                self._write_current_settings()
                self._append_log(f"Latest FLDK URL set: {payload}")
            elif kind == "latest_url_error":
                self.latest_url_button.configure(state="normal" if not self.is_running else "disabled")
                self.status_var.set("Latest FLDK failed")
                self._append_log(f"Latest FLDK lookup failed: {payload}")
                messagebox.showerror("Latest FLDK failed", str(payload))
            elif kind == "canceled":
                self._set_running(False)
                self.status_var.set("Canceled")
                self._append_log(str(payload))
                messagebox.showinfo("Canceled", str(payload))
            elif kind == "error":
                self._set_running(False)
                self.status_var.set("Failed")
                self.last_error_report = build_error_report(
                    str(payload),
                    self.last_config,
                    self.log_text.get("1.0", "end"),
                    self.last_outputs,
                )
                messagebox.showerror("Processing failed", str(payload))

        self.root.after(100, self._poll_messages)

    def _append_log(self, message: str) -> None:
        self.log_text.configure(state="normal")
        self.log_text.insert("end", message + "\n")
        self.log_text.see("end")
        self.log_text.configure(state="disabled")


def launch_gui() -> None:
    root = tk.Tk()
    HimawariProcessorApp(root)
    root.mainloop()


if __name__ == "__main__":
    try:
        launch_gui()
    except KeyboardInterrupt:
        print("Interrupted", file=sys.stderr)
        raise SystemExit(130)
