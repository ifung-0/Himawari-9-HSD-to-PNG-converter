from __future__ import annotations

import bz2
import concurrent.futures
import gc
import hashlib
import importlib.util
import json
import logging
import math
import os
import queue
import re
import shutil
import string
import subprocess
import sys
import threading
import time
import tkinter as tk
import warnings
import xml.etree.ElementTree as ET
from dataclasses import dataclass, fields, replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from tkinter import colorchooser, filedialog, messagebox, ttk
from typing import Callable, Iterable

def configure_known_warning_filters() -> None:
    warnings.filterwarnings(
        "ignore",
        message="CUDA path could not be detected.*",
        category=UserWarning,
        module=r"cupy\._environment",
    )
    warnings.filterwarnings(
        "ignore",
        message="invalid value encountered in (cos|sin)",
        category=RuntimeWarning,
        module=r"dask\._task_spec",
    )


configure_known_warning_filters()

if __name__ == "__main__":
    print("Loading scientific packages; first startup can take a few seconds...", flush=True)
try:
    import dask
    import dask.array as da
    import numpy as np
    import requests
    import xarray as xr
    from pyresample.geometry import AreaDefinition
    from satpy import Scene
except KeyboardInterrupt:
    print(
        "\nStartup was canceled while loading scientific packages. "
        "Run the app again and wait for the GUI to open.",
        file=sys.stderr,
    )
    raise SystemExit(130) from None


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
APP_VERSION = "2026.06.17.01"
APP_DISPLAY_NAME = "Himawari-8/9 Low-RAM Processor"
USER_URL = "https://noaa-himawari9.s3.amazonaws.com/AHI-L1b-FLDK/2024/07/25/0400/HS_H09_20240725_0400_B01_FLDK_R10_S0110.DAT.bz2"
MODE = "Single Image"  # "Single Image" or "Timelapse"
COMPOSITE_CHOICE = "True Color Reproduction Image"
SATELLITE_LAYER_MODE = "standard"
SATELLITE_LAYER_MODES = ("standard", "live", "hd")

HOURS_BACK = 72
INTERVAL_MINUTES = 20
FPS = 10
AUTO_DOWNLOAD = True
GPU_ACCELERATION = False
USE_NIGHT_FALLBACK = True
NIGHT_FALLBACK_MODE = "hybrid"
DOWNLOAD_WORKERS = 2
DOWNLOAD_STREAM_CHUNK_BYTES = 256 * 1024
DOWNLOAD_MAX_RETRIES = 3  # extra passes over still-missing segments before giving up
DOWNLOAD_RETRY_BACKOFF_SECONDS = 6.0  # pause between retry passes (transient DNS/network)
TIMELAPSE_FORMAT = "gif"  # "gif" or "mp4"
DELETE_TIMELAPSE_FRAMES = True
IMAGE_FORMAT = "png"  # Use "tif" for chunked GeoTIFF writes on very large outputs.
OUTPUT_TEMPLATE = "Himawari_{area}_{scan_time}_{product}"
RESAMPLER = "native"  # "native" is required for full-disk low-RAM processing.
ADD_BORDER_LINES = False
BORDER_LINE_COLOR = "green"
BORDER_LINE_WIDTH = 1.0
ADD_MAP_LABELS = False
MAP_LABEL_SIZE = 14
MAP_LABEL_SIZE_MIN = 6
MAP_LABEL_SIZE_MAX = 96
ADD_NIGHT_BOUNDARY = False
ADD_CROSSHAIR = False
CROSSHAIR_TYPE = "target"
CROSSHAIR_COLOR = "#7c3cff"
ZOOM_EARTH_STYLE = False
FLAT_MAP_INVALID_FILL = (12, 26, 44)
FLAT_MAP_SOURCE_VALID_MIN = 1.0e-6
FLAT_MAP_BASEMAP_OCEAN = (15, 32, 52)
FLAT_MAP_BASEMAP_LAND = (60, 72, 60)
FLAT_MAP_BASEMAP_COAST = (122, 134, 126)
FLAT_MAP_LIMB_FADE_FRACTION = 0.05
FLAT_MAP_LIMB_FADE_MIN_PX = 8
FLAT_MAP_LIMB_FADE_MAX_PX = 240
FLAT_MAP_ZOOM_OVERLAY_OPACITY = 200
FLAT_MAP_ZOOM_CROSSHAIR_OPACITY_SCALE = 0.78
SATELLITE_LAYER_BORDER_COLOR = "#d8dee8"
FLAT_MAP_DIRECT_SAMPLE_CHUNK = 256
GPU_CUSTOM_COMPOSITE_MAX_CHUNK_EDGE = 512
GPU_CUSTOM_COMPOSITE_MAX_CHUNK_PIXELS = 262_144
CROSSHAIR_TYPES = ("target", "dot", "plus", "ring")
UNSUPPORTED_MAP_OVERLAYS = (
    "Radar",
    "Wind Animation",
    "Heat Spots",
    "Active Fires",
    "Tropical Systems",
    "Temperatures",
)
ZOOM_EARTH_LABEL_POINTS = (
    ("RUSSIA", 61.0, 110.0, "land"),
    ("KAZAKHSTAN", 48.0, 68.0, "land"),
    ("MONGOLIA", 46.0, 104.0, "land"),
    ("CHINA", 35.0, 104.0, "land"),
    ("JAPAN", 37.5, 138.0, "land"),
    ("SOUTH KOREA", 36.3, 128.0, "land"),
    ("PHILIPPINES", 12.5, 122.0, "land"),
    ("INDONESIA", -2.0, 118.0, "land"),
    ("PAPUA NEW\nGUINEA", -6.0, 145.0, "land"),
    ("AUSTRALIA", -25.0, 134.0, "land"),
    ("NEW\nZEALAND", -42.0, 172.5, "land"),
    ("INDIA", 22.0, 78.0, "land"),
    ("North Pacific\nOcean", 29.0, 173.0, "water"),
    ("West Pacific\nOcean", 10.0, 150.0, "water"),
    ("Indian\nOcean", -27.0, 82.0, "water"),
    ("Philippine Sea", 18.0, 135.0, "water"),
)
MAX_SAFE_PNG_PIXELS = 40_000_000
MAX_SAFE_TIMELAPSE_FRAME_PIXELS = 40_000_000
MAP_VIEW = "native"
FLAT_MIN_LAT = -60.0
FLAT_MAX_LAT = 60.0
FLAT_MIN_LON = 80.0
FLAT_MAX_LON = 200.0
FLAT_RESOLUTION_DEG = 0.05
MAX_FLAT_MAP_PIXELS = 30_000_000
WEB_MERCATOR_MAX_LAT = 85.05112878
WEB_MERCATOR_PROJ4 = (
    "+proj=merc +a=6378137 +b=6378137 +lat_ts=0 +lon_0=0 "
    "+x_0=0 +y_0=0 +units=m +over +no_defs"
)
WEB_MERCATOR_METERS_PER_DEGREE = (2.0 * math.pi * 6378137.0) / 360.0

# ---------------------------------------------------------------------------
# Segment-aware regional downloads.
# Himawari FLDK scans are split into equal-height horizontal segments. For a
# regional crop only the segments whose latitude band touches the requested
# bounds need to be downloaded. The geostationary navigation below converts
# segment pixel rows into latitude bands so the unneeded segments are skipped.
# ---------------------------------------------------------------------------
SEGMENT_AWARE_DOWNLOADS = True
# CGMS/LRIT geostationary navigation constants for the Himawari AHI full disk.
# These are fixed by the Himawari Standard Data specification. They are used
# only to estimate each segment's latitude band, so the 2 km reference grid is
# enough (segment latitude bands are the same fraction of the disk at every
# band resolution).
AHI_SUB_SATELLITE_LON_DEG = 140.7
AHI_EARTH_EQUATORIAL_RADIUS_M = 6378137.0
AHI_EARTH_POLAR_RADIUS_M = 6356752.3
AHI_SATELLITE_DISTANCE_M = 42164000.0
AHI_FULL_DISK_2KM_LINES = 5500
AHI_FULL_DISK_2KM_COFF = 2750.5
AHI_FULL_DISK_2KM_LOFF = 2750.5
AHI_FULL_DISK_2KM_CFAC = 20466275.0
AHI_FULL_DISK_2KM_LFAC = 20466275.0
# Extra segments kept on each side of the intersecting band as a safety margin
# so a slightly off latitude estimate can never drop a needed segment.
SEGMENT_DOWNLOAD_MARGIN = 1

# ---------------------------------------------------------------------------
# Live preview (quicklook). A coarse, fast render used to check framing and
# look before committing to a full-resolution run.
# ---------------------------------------------------------------------------
PREVIEW_FLAT_RESOLUTION_DEG = 0.16   # ~2 km per pixel at the equator
PREVIEW_MAX_DIMENSION_PX = 900       # downscale the preview image for display
PREVIEW_FLAT_FALLBACK_BOUNDS = (-60.0, 60.0, 80.0, 200.0)

# ---------------------------------------------------------------------------
# Rough timing model for the pre-run wall-clock estimate shown in the run
# summary. These are deliberately conservative averages for a typical broadband
# connection and a low-RAM machine; the live ETA during a run quickly replaces
# them with measured speed, so they only need to be in the right ballpark.
# ---------------------------------------------------------------------------
ESTIMATE_SECONDS_PER_SEGMENT_DOWNLOAD = 3.0    # download + bz2 decompress, per segment
ESTIMATE_SECONDS_PER_MEGAPIXEL_RENDER = 1.1    # load/resample/render/save, per output megapixel
ESTIMATE_FRAME_FIXED_OVERHEAD = 4.0            # scene setup, night check and file I/O per frame
ESTIMATE_TIMELAPSE_ASSEMBLY_PER_FRAME = 0.6    # GIF/MP4 assembly per frame
ESTIMATE_MAX_RENDER_MEGAPIXELS = 520.0         # cap so a full-disk estimate stays sane

# ---------------------------------------------------------------------------
# Overlay colour themes. Each theme is a ready-made palette for the border
# lines, labels, night boundary and crosshair so users can pick a look instead
# of hand-entering hex colours. "Custom" keeps whatever colours are set.
# Keys: border, label, night_boundary, crosshair.
# ---------------------------------------------------------------------------
OVERLAY_THEME_CUSTOM = "Custom (keep my colors)"
OVERLAY_THEMES: dict[str, dict[str, str]] = {
    "Subtle Light": {
        "border": "#d8dee8",
        "label": "#ebeef2",
        "night_boundary": "#91b4d7",
        "crosshair": "#7c3cff",
    },
    "High-Contrast Night": {
        "border": "#ffd23f",
        "label": "#ffffff",
        "night_boundary": "#ff5d5d",
        "crosshair": "#00e5ff",
    },
    "Classic Green": {
        "border": "#37c837",
        "label": "#eaffea",
        "night_boundary": "#9fe0ff",
        "crosshair": "#ff2f7b",
    },
    "Warm Amber": {
        "border": "#f6a13c",
        "label": "#fff1dd",
        "night_boundary": "#ffb27a",
        "crosshair": "#ff4d4d",
    },
    "Cool Cyan": {
        "border": "#39d3e6",
        "label": "#e6fbff",
        "night_boundary": "#7ad0ff",
        "crosshair": "#ff7ad0",
    },
    "Monochrome Ink": {
        "border": "#101418",
        "label": "#f5f7fa",
        "night_boundary": "#101418",
        "crosshair": "#101418",
    },
}
OVERLAY_THEME_ORDER = (OVERLAY_THEME_CUSTOM, *OVERLAY_THEMES.keys())
OVERLAY_THEME = OVERLAY_THEME_CUSTOM
MAP_LABEL_COLOR = "#ebeef2"
NIGHT_BOUNDARY_COLOR = "#91b4d7"
WRITE_METADATA_SIDECAR = True

# ---------------------------------------------------------------------------
# Output-area presets (lat/lon bounding boxes inside the Himawari-8/9 view).
# Each regional preset switches the app to the flat (Web Mercator) map and
# fills the four bounds. "Custom" leaves the bounds untouched; "Full Disk"
# selects the native full-disk view (the whole round Earth).
# Bounds are (min_lat, max_lat, min_lon, max_lon) in degrees.
# Coverage note: Himawari-9 sits at 140.7 deg E, so areas near 80 deg E
# (Central/South Asia) sit close to the western limb and can show edge falloff.
# ---------------------------------------------------------------------------
AREA_PRESET_CUSTOM = "Custom (manual bounds)"
AREA_PRESET_FULL_DISK = "Full Disk (native)"
AREA_PRESET_FULL_DISK_FLAT = "Full Disk (flat map)"
AREA_PRESETS: dict[str, tuple[float, float, float, float]] = {
    AREA_PRESET_FULL_DISK_FLAT: (-60.0, 60.0, 80.0, 200.0),
    "Australia": (-45.0, -9.0, 110.0, 156.0),
    "Central Asia": (33.0, 53.0, 80.0, 112.0),
    "New Zealand": (-48.0, -33.0, 165.0, 179.5),
    "Pacific Islands 1": (-10.0, 10.0, 150.0, 175.0),
    "Pacific Islands 2": (-10.0, 10.0, 175.0, 200.0),
    "Pacific Islands 3": (0.0, 20.0, 145.0, 170.0),
    "Pacific Islands 4": (0.0, 20.0, 170.0, 195.0),
    "Pacific Islands 5": (-25.0, -5.0, 150.0, 175.0),
    "Pacific Islands 6": (-25.0, -5.0, 175.0, 200.0),
    "Pacific Islands 7": (5.0, 25.0, 130.0, 155.0),
    "Pacific Islands 8": (-20.0, 0.0, 130.0, 155.0),
    "Pacific Islands 9": (-35.0, -15.0, 165.0, 190.0),
    "Pacific Islands 10": (10.0, 30.0, 130.0, 155.0),
    "Southeast Asia 1": (-12.0, 8.0, 95.0, 120.0),
    "Southeast Asia 2": (5.0, 25.0, 95.0, 115.0),
    "Southeast Asia 3": (0.0, 20.0, 115.0, 130.0),
    "South Asia": (5.0, 32.0, 80.0, 100.0),
}
# Order shown in the GUI dropdown.
AREA_PRESET_ORDER = (
    AREA_PRESET_CUSTOM,
    AREA_PRESET_FULL_DISK_FLAT,
    AREA_PRESET_FULL_DISK,
    *[name for name in AREA_PRESETS if name != AREA_PRESET_FULL_DISK_FLAT],
)

# ---------------------------------------------------------------------------
# In-app updater. Pulls the default branch of the public GitHub repository,
# verifies the download, backs up the current files, then replaces them.
# ---------------------------------------------------------------------------
GITHUB_REPO = "ifung-0/Himawari-9-HSD-to-PNG-converter"
GITHUB_API_BASE = "https://api.github.com"
GITHUB_UPDATE_TIMEOUT_SECONDS = 60
# Files the updater is allowed to replace if they exist in the downloaded branch.
SELF_UPDATE_FILES = (
    "himawari_lowram_processor.py",
    "check_environment.py",
    "run_gui.bat",
    "README.md",
)

RAM_LIMIT_GB = 10.0
DASK_CHUNK_CHOICES = ("16MiB", "32MiB", "64MiB", "128MiB")
DASK_CHUNK_SIZE = "32MiB"  # Use larger chunks only if the machine has headroom.
DASK_NUM_WORKERS = 1  # Keep at 1 or 2.
NIGHT_CHECK_SAMPLE_PIXELS = 512
NIGHT_CHECK_CENTER_FRACTION = 0.65
NIGHT_CHECK_BRIGHT_REFLECTANCE = 2.0
NIGHT_CHECK_BRIGHT_FRACTION = 0.03
HYBRID_NIGHT_DARK_REFLECTANCE = 0.3
HYBRID_NIGHT_DAY_REFLECTANCE = 1.5
MAX_NATIVE_COMPATIBILITY_CROP_PIXELS = 32
NATIVE_GRID_INDEX_TOLERANCE = 1e-7

PROJECT_DIR = Path(__file__).resolve().parent
CLOUD_SYNC_PREFIXES = ("onedrive", "dropbox", "google drive", "icloud")
CLOUD_SYNC_EXACT = ("box",)
LOCAL_APP_DATA_DIR = Path(os.environ.get("LOCALAPPDATA") or (Path.home() / "AppData" / "Local"))
APP_DATA_DIR = LOCAL_APP_DATA_DIR / "Himawari9LowRamProcessor"
OUTPUT_DIR = APP_DATA_DIR / "outputs"
TEMP_DIR = APP_DATA_DIR / "temp"
LOG_DIR = APP_DATA_DIR / "logs"
GUI_SETTINGS_FILE = PROJECT_DIR / "himawari_gui_settings.json"
RECENT_RUNS_FILE = PROJECT_DIR / "himawari_recent_runs.json"
CUSTOM_PRESETS_FILE = PROJECT_DIR / "himawari_custom_presets.json"
GUI_SETTINGS_SCHEMA_VERSION = 1
RECENT_RUNS_SCHEMA_VERSION = 2
CUSTOM_PRESETS_SCHEMA_VERSION = 1
TIMELAPSE_MANIFEST_SCHEMA_VERSION = 1
NOAA_HIMAWARI8_BUCKET = "https://noaa-himawari8.s3.amazonaws.com"
NOAA_HIMAWARI9_BUCKET = "https://noaa-himawari9.s3.amazonaws.com"
HIMAWARI_BUCKET_BY_SAT_ID = {
    "HS_H08": NOAA_HIMAWARI8_BUCKET,
    "HS_H09": NOAA_HIMAWARI9_BUCKET,
}
RECENT_RUN_LIMIT = 50
CUSTOM_PRESET_LIMIT = 25
PREVIEW_MAX_BYTES = 100 * 1024 * 1024
OVERLAY_RESOLUTION = "l"
OVERLAY_LEVEL = 1
BUILT_IN_PRESETS = ("Balanced Single", "Fast IR Check", "Low-RAM Timelapse")
ALLOWED_TEMPLATE_TOKENS = {"scan_time", "area", "product", "mode", "band", "format"}
WINDOWS_RESERVED_FILENAME_CHARS = '<>:"/\\|?*'
WINDOWS_RESERVED_DEVICE_NAMES = {
    "CON",
    "PRN",
    "AUX",
    "NUL",
    *(f"COM{index}" for index in range(1, 10)),
    *(f"LPT{index}" for index in range(1, 10)),
}


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


@dataclass(frozen=True)
class LocalSegmentInfo:
    source_path: Path
    sat_id: str
    timestamp: str
    band: str
    area: str
    resolution: str
    segment: int
    total_segments: int
    compressed: bool


@dataclass(frozen=True)
class LocalImportResult:
    sat_id: str
    timestamp: str
    area: str
    total_segments: int
    imported_paths: tuple[Path, ...]
    reused_paths: tuple[Path, ...]
    bands: tuple[str, ...]

    @property
    def synthetic_url(self) -> str:
        return offline_source_url_for_import(self)


@dataclass(frozen=True)
class SystemPerformanceProfile:
    total_ram_gb: float | None
    available_ram_gb: float | None
    cpu_count: int
    cpu_percent: float | None


@dataclass(frozen=True)
class PerformanceRecommendation:
    mode: str
    download_workers: int
    dask_num_workers: int
    dask_chunk_size: str
    ram_limit_gb: float
    summary: str


@dataclass(frozen=True)
class GpuSupportStatus:
    ok: bool
    detail: str
    device_name: str = ""
    device_count: int = 0
    package_version: str = ""


@dataclass(frozen=True)
class ConnectivityResult:
    host: str
    dns_ok: bool
    dns_ms: float | None
    resolved_ips: tuple[str, ...]
    http_ok: bool
    http_status: int | None
    http_ms: float | None
    error: str = ""

    @property
    def reachable(self) -> bool:
        # Any HTTP response (even 403/405 from S3) means the network path works.
        return self.dns_ok and self.http_ok

    def display_text(self) -> str:
        lines = [f"Data host: {self.host}"]
        if self.dns_ok:
            ip_text = ", ".join(self.resolved_ips[:4]) if self.resolved_ips else "resolved"
            ms = f"{self.dns_ms:.0f} ms" if self.dns_ms is not None else "ok"
            lines.append(f"DNS lookup: OK ({ms}) -> {ip_text}")
        else:
            lines.append("DNS lookup: FAILED - the host name could not be resolved.")
        if self.http_ok:
            ms = f"{self.http_ms:.0f} ms" if self.http_ms is not None else "ok"
            status = self.http_status if self.http_status is not None else "response"
            lines.append(f"HTTP HEAD: OK (status {status}, {ms})")
        else:
            lines.append("HTTP HEAD: FAILED - no HTTP response from the host.")
        if self.reachable:
            lines.append("")
            lines.append("The data host is reachable. A failed run is unlikely to be the network.")
        else:
            lines.append("")
            lines.append(
                "The data host is NOT reachable from here. This points to a network/DNS "
                "problem (VPN, firewall, proxy, or an outage) rather than the app itself."
            )
        if self.error:
            lines.append("")
            lines.append(f"Detail: {self.error}")
        return "\n".join(lines)


@dataclass
class ProcessorConfig:
    user_url: str = USER_URL
    mode: str = MODE
    composite_choice: str = COMPOSITE_CHOICE
    satellite_layer_mode: str = SATELLITE_LAYER_MODE
    hours_back: int = HOURS_BACK
    interval_minutes: int = INTERVAL_MINUTES
    fps: int = FPS
    auto_download: bool = AUTO_DOWNLOAD
    gpu_acceleration: bool = GPU_ACCELERATION
    use_night_fallback: bool = USE_NIGHT_FALLBACK
    night_fallback_mode: str = NIGHT_FALLBACK_MODE
    download_workers: int = DOWNLOAD_WORKERS
    timelapse_format: str = TIMELAPSE_FORMAT
    delete_timelapse_frames: bool = DELETE_TIMELAPSE_FRAMES
    image_format: str = IMAGE_FORMAT
    output_template: str = OUTPUT_TEMPLATE
    resampler: str = RESAMPLER
    allow_quality_fallback: bool = False
    add_border_lines: bool = ADD_BORDER_LINES
    border_line_color: str = BORDER_LINE_COLOR
    border_line_width: float = BORDER_LINE_WIDTH
    add_map_labels: bool = ADD_MAP_LABELS
    map_label_size: int = MAP_LABEL_SIZE
    add_night_boundary: bool = ADD_NIGHT_BOUNDARY
    add_crosshair: bool = ADD_CROSSHAIR
    crosshair_type: str = CROSSHAIR_TYPE
    crosshair_color: str = CROSSHAIR_COLOR
    zoom_earth_style: bool = ZOOM_EARTH_STYLE
    map_view: str = MAP_VIEW
    flat_min_lat: float = FLAT_MIN_LAT
    flat_max_lat: float = FLAT_MAX_LAT
    flat_min_lon: float = FLAT_MIN_LON
    flat_max_lon: float = FLAT_MAX_LON
    flat_resolution_deg: float = FLAT_RESOLUTION_DEG
    max_safe_png_pixels: int = MAX_SAFE_PNG_PIXELS
    ram_limit_gb: float = RAM_LIMIT_GB
    dask_chunk_size: str = DASK_CHUNK_SIZE
    dask_num_workers: int = DASK_NUM_WORKERS
    segment_aware_downloads: bool = SEGMENT_AWARE_DOWNLOADS
    write_metadata_sidecar: bool = WRITE_METADATA_SIDECAR
    overlay_theme: str = OVERLAY_THEME
    map_label_color: str = MAP_LABEL_COLOR
    night_boundary_color: str = NIGHT_BOUNDARY_COLOR


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
                "Run Quick Fix to install pycoast-compatible GSHHS/WDBII overlay data, "
                "or disable border lines."
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
    estimated_seconds: float | None = None
    segments_per_frame: int | None = None
    all_segments_per_frame: int | None = None

    def display_text(self) -> str:
        frame_word = "frame" if self.frames == 1 else "frames"
        segment_word = "segment" if self.total_segments == 1 else "segments"
        download_line = f"Downloads: {self.total_segments} {segment_word}"
        if (
            self.segments_per_frame is not None
            and self.all_segments_per_frame is not None
            and self.segments_per_frame < self.all_segments_per_frame
        ):
            saved = self.all_segments_per_frame - self.segments_per_frame
            download_line += (
                f" (segment-aware: {self.segments_per_frame} of {self.all_segments_per_frame} "
                f"per band, skipping {saved})"
            )
        lines = [
            f"Source: {self.source}",
            f"Product: {self.product}",
            f"Frames: {self.frames} {frame_word}",
            f"Bands: {', '.join(self.bands)}",
            download_line,
            f"Output: {self.output}",
        ]
        if self.estimated_seconds is not None:
            lines.append(f"Estimated time: ~{format_estimated_duration(self.estimated_seconds)} (rough)")
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


@dataclass(frozen=True)
class RecentScanChoice:
    timestamp: str
    band: str
    url: str
    label: str


@dataclass(frozen=True)
class OutputPreview:
    path: Path
    exists: bool
    supported_preview: bool
    reason: str
    size_bytes: int | None = None
    dimensions: tuple[int, int] | None = None

    def display_text(self) -> str:
        lines = [str(self.path)]
        lines.append("Exists: yes" if self.exists else "Exists: no")
        if self.size_bytes is not None:
            lines.append(f"Size: {format_file_size(self.size_bytes)}")
        if self.dimensions:
            lines.append(f"Dimensions: {self.dimensions[0]} x {self.dimensions[1]}")
        if self.reason:
            lines.append(self.reason)
        return "\n".join(lines)


@dataclass(frozen=True)
class RecentRunRecord:
    run_id: str
    status: str
    started_at_utc: str
    completed_at_utc: str
    app_version: str
    mode: str
    product: str
    source: str
    outputs: tuple[str, ...]
    manifest_path: str
    frame_dir: str
    log_path: str
    error: str
    config: dict[str, object]

    @property
    def main_output(self) -> str:
        return self.outputs[-1] if self.outputs else ""


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
    logging.captureWarnings(True)


def configure_dask(config: ProcessorConfig | None = None) -> None:
    config = config or default_config()
    validate_dask_chunk_size(config.dask_chunk_size)
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


def validate_dask_chunk_size(value: str) -> None:
    if value not in DASK_CHUNK_CHOICES:
        choices = ", ".join(DASK_CHUNK_CHOICES)
        raise ValueError(f"Dask chunk size must be one of: {choices}.")


def system_performance_profile() -> SystemPerformanceProfile:
    cpu_count = os.cpu_count() or 1
    total_ram_gb: float | None = None
    available_ram_gb: float | None = None
    cpu_percent: float | None = None
    try:
        import psutil

        memory = psutil.virtual_memory()
        total_ram_gb = memory.total / (1024**3)
        available_ram_gb = memory.available / (1024**3)
        cpu_percent = float(psutil.cpu_percent(interval=0.1))
    except Exception:
        pass
    return SystemPerformanceProfile(
        total_ram_gb=total_ram_gb,
        available_ram_gb=available_ram_gb,
        cpu_count=cpu_count,
        cpu_percent=cpu_percent,
    )


def recommend_performance_settings(
    profile: SystemPerformanceProfile,
    mode: str,
) -> PerformanceRecommendation:
    normalized = mode.strip().lower().replace("-", "_").replace(" ", "_")
    if normalized not in {"safe", "best_performance"}:
        raise ValueError('Performance mode must be "safe" or "best_performance".')

    available = profile.available_ram_gb if profile.available_ram_gb is not None else 4.0
    total = profile.total_ram_gb if profile.total_ram_gb is not None else max(available, 4.0)
    cpu_count = max(1, int(profile.cpu_count or 1))
    cpu_busy = profile.cpu_percent is not None and profile.cpu_percent >= 75.0

    if normalized == "safe":
        download_workers = 1 if cpu_busy or available < 8 else 2
        dask_workers = 1
        chunk_size = "16MiB" if available < 8 else "32MiB"
        ram_limit = max(2.0, min(8.0, available * 0.55, total * 0.5))
        label = "Safe Mode"
    else:
        download_workers = 4 if cpu_count >= 8 and available >= 12 and not cpu_busy else 2
        dask_workers = 2 if cpu_count >= 6 and available >= 12 and not cpu_busy else 1
        if available >= 24 and cpu_count >= 8 and not cpu_busy:
            chunk_size = "128MiB"
        elif available >= 12:
            chunk_size = "64MiB"
        else:
            chunk_size = "32MiB"
        ram_limit = max(4.0, min(16.0, available * 0.7, total * 0.65))
        label = "Best Performance"

    summary = (
        f"{label}: CPU cores {cpu_count}, "
        f"available RAM {available:.1f} GiB"
        + (f", CPU load {profile.cpu_percent:.0f}%" if profile.cpu_percent is not None else "")
    )
    return PerformanceRecommendation(
        mode=normalized,
        download_workers=clamp_download_workers(download_workers),
        dask_num_workers=clamp_dask_workers(dask_workers),
        dask_chunk_size=chunk_size,
        ram_limit_gb=round(ram_limit, 1),
        summary=summary,
    )


def gpu_support_status() -> GpuSupportStatus:
    if importlib.util.find_spec("cupy") is None:
        return GpuSupportStatus(
            False,
            "CuPy is not installed. Run GPU Fix to install optional NVIDIA/CUDA GPU support.",
        )
    try:
        import cupy as cp  # type: ignore
    except Exception as exc:
        return GpuSupportStatus(False, f"CuPy import failed: {exc.__class__.__name__}: {exc}")

    version = str(getattr(cp, "__version__", "unknown"))
    try:
        device_count = int(cp.cuda.runtime.getDeviceCount())
    except Exception as exc:
        return GpuSupportStatus(False, f"CUDA device check failed: {exc.__class__.__name__}: {exc}", package_version=version)
    if device_count <= 0:
        return GpuSupportStatus(False, "CuPy is installed, but no CUDA GPU device was found.", package_version=version)

    device_name = ""
    try:
        raw_name = cp.cuda.runtime.getDeviceProperties(0).get("name", b"")
        device_name = raw_name.decode("utf-8", errors="replace") if isinstance(raw_name, bytes) else str(raw_name)
        test = cp.asarray([1], dtype=cp.float32)
        test = (test + cp.float32(1.0)).astype(cp.float32)
        cp.cuda.Stream.null.synchronize()
        if float(cp.asnumpy(test)[0]) != 2.0:
            raise RuntimeError("unexpected CUDA test result")
        del test
        cp.get_default_memory_pool().free_all_blocks()
    except Exception as exc:
        return GpuSupportStatus(
            False,
            (
                f"CUDA kernel test failed: {exc.__class__.__name__}: {exc}. "
                "Run GPU Fix to install CuPy with CUDA toolkit headers."
            ),
            device_name=device_name,
            device_count=device_count,
            package_version=version,
        )
    return GpuSupportStatus(
        True,
        f"CuPy {version} ready on {device_name or 'CUDA device 0'} ({device_count} device(s)).",
        device_name=device_name,
        device_count=device_count,
        package_version=version,
    )


def require_gpu_ready() -> GpuSupportStatus:
    status = gpu_support_status()
    if not status.ok:
        raise RuntimeError("GPU acceleration is enabled, but GPU support is not ready. " + status.detail)
    return status


def dask_array_to_gpu_chunks(array: da.Array) -> da.Array:
    import cupy as cp  # type: ignore

    return array.map_blocks(cp.asarray, dtype=array.dtype)


def dask_array_to_cpu_chunks(array: da.Array) -> da.Array:
    def to_cpu(block):
        if not is_cupy_array_like(block):
            return block
        import cupy as cp  # type: ignore

        return cp.asnumpy(block)

    return array.map_blocks(to_cpu, dtype=array.dtype)


def is_cupy_array_like(value: object) -> bool:
    return value.__class__.__module__.split(".", 1)[0] == "cupy"


def dataarray_to_gpu_chunks(data: xr.DataArray) -> xr.DataArray:
    if not isinstance(data.data, da.Array):
        return data
    return data.copy(deep=False, data=dask_array_to_gpu_chunks(data.data))


def dataarray_to_cpu_chunks(data: xr.DataArray) -> xr.DataArray:
    if not isinstance(data.data, da.Array):
        return data
    return data.copy(deep=False, data=dask_array_to_cpu_chunks(data.data))


def scene_missing_bands(scene: Scene, bands: Iterable[str]) -> tuple[str, ...]:
    missing: list[str] = []
    for band in bands:
        try:
            scene[band]
        except KeyError:
            missing.append(band)
    return tuple(missing)


def maybe_gpu_scene_for_custom_composite(scene: Scene, bands: Iterable[str], config: ProcessorConfig) -> Scene:
    if not config.gpu_acceleration:
        return scene
    require_gpu_ready()
    for band in bands:
        scene[band] = dataarray_to_gpu_chunks(scene[band])
    return scene


def maybe_cpu_scene_after_gpu(scene: Scene, dataset_names: Iterable[str], config: ProcessorConfig) -> Scene:
    if not config.gpu_acceleration:
        return scene
    for name in dataset_names:
        try:
            scene[name] = dataarray_to_cpu_chunks(scene[name])
        except KeyError:
            continue
    return scene


def maybe_cpu_dataset_after_gpu(dataset: xr.DataArray, config: ProcessorConfig) -> xr.DataArray:
    if not config.gpu_acceleration:
        return dataset
    return dataarray_to_cpu_chunks(dataset)


def chunk_sizes_for_length(length: int, chunk_size: int) -> tuple[int, ...]:
    length = int(length)
    chunk_size = max(1, int(chunk_size))
    if length <= 0:
        return ()
    chunks: list[int] = []
    remaining = length
    while remaining > 0:
        size = min(chunk_size, remaining)
        chunks.append(size)
        remaining -= size
    return tuple(chunks)


def bounded_2d_chunk_grid(
    shape: tuple[int, int],
    max_edge: int = GPU_CUSTOM_COMPOSITE_MAX_CHUNK_EDGE,
    max_pixels: int = GPU_CUSTOM_COMPOSITE_MAX_CHUNK_PIXELS,
) -> tuple[tuple[int, ...], tuple[int, ...]]:
    height, width = int(shape[0]), int(shape[1])
    if height <= 0 or width <= 0:
        raise ValueError(f"Chunked 2D data requires a positive shape, got {shape}.")
    y_chunk = min(height, max(1, int(max_edge)))
    x_limit_from_pixels = max(1, int(max_pixels) // max(1, y_chunk))
    x_chunk = min(width, max(1, int(max_edge)), x_limit_from_pixels)
    return chunk_sizes_for_length(height, y_chunk), chunk_sizes_for_length(width, x_chunk)


def rechunk_2d_array_to_grid(array: da.Array, chunks: tuple[tuple[int, ...], tuple[int, ...]]) -> da.Array:
    if array.ndim != 2:
        raise ValueError(f"Expected a 2D Dask array, got {array.ndim} dimensions.")
    if array.chunks == chunks:
        return array
    return array.rechunk(chunks)


def gpu_scale_reflectance_block(cp, data, max_value: float = 100.0, gamma: float = 1.0):
    scaled = cp.clip(cp.nan_to_num(data, nan=0.0), 0.0, max_value) / cp.float32(max_value)
    if gamma != 1.0:
        scaled = scaled ** cp.float32(1.0 / gamma)
    return cp.clip(scaled, 0.0, 1.0)


def gpu_scale_ir_temperature_block(cp, data, warm_k: float = 300.0, cold_k: float = 190.0, gamma: float = 1.0):
    filled = cp.nan_to_num(data, nan=warm_k)
    scaled = (cp.float32(warm_k) - filled) / cp.float32(warm_k - cold_k)
    scaled = cp.clip(scaled, 0.0, 1.0)
    if gamma != 1.0:
        scaled = scaled ** cp.float32(1.0 / gamma)
    return cp.clip(scaled, 0.0, 1.0)


def gpu_black_point_block(cp, data, black: float):
    return cp.clip((data - cp.float32(black)) / cp.float32(1.0 - black), 0.0, 1.0)


def gpu_contrast_block(cp, data, contrast: float, midpoint: float):
    return cp.clip((data - cp.float32(midpoint)) * cp.float32(contrast) + cp.float32(midpoint), 0.0, 1.0)


def gpu_saturation_block(cp, red, green, blue, saturation: float):
    luma = red * cp.float32(0.2126) + green * cp.float32(0.7152) + blue * cp.float32(0.0722)
    saturation = cp.float32(saturation)
    return (
        cp.clip(luma + (red - luma) * saturation, 0.0, 1.0),
        cp.clip(luma + (green - luma) * saturation, 0.0, 1.0),
        cp.clip(luma + (blue - luma) * saturation, 0.0, 1.0),
    )


def gpu_fill_visible_reflectance_gaps_block(cp, *arrays):
    finite = [cp.isfinite(array) & (cp.abs(array) > cp.float32(FLAT_MAP_SOURCE_VALID_MIN)) for array in arrays]
    count = cp.zeros_like(arrays[0], dtype=cp.float32)
    total = cp.zeros_like(arrays[0], dtype=cp.float32)
    for array, valid in zip(arrays, finite, strict=False):
        count += valid.astype(cp.float32)
        total += cp.where(valid, array, cp.float32(0.0))
    fallback = cp.where(count > 0, total / cp.maximum(count, cp.float32(1.0)), cp.float32(0.0))
    return tuple(cp.where(valid, array, fallback) for array, valid in zip(arrays, finite, strict=False))


def gpu_true_color_reproduction_block(
    b01,
    b02,
    b03,
    b04,
    b13=None,
    use_hybrid: bool = False,
) -> np.ndarray:
    import cupy as cp  # type: ignore

    input_shapes = {tuple(np.shape(block)) for block in (b01, b02, b03, b04)}
    if len(input_shapes) != 1:
        raise ValueError(f"GPU true color block received mismatched visible band shapes: {sorted(input_shapes)}")
    if use_hybrid and b13 is not None and tuple(np.shape(b13)) != tuple(np.shape(b03)):
        raise ValueError(
            "GPU hybrid true color block received a B13 chunk that does not match the visible band chunk shape."
        )

    b01_gpu = cp.asarray(b01, dtype=cp.float32)
    b02_gpu = cp.asarray(b02, dtype=cp.float32)
    b03_gpu = cp.asarray(b03, dtype=cp.float32)
    b04_gpu = cp.asarray(b04, dtype=cp.float32)
    b01_gpu, b02_gpu, b03_gpu, b04_gpu = gpu_fill_visible_reflectance_gaps_block(
        cp,
        b01_gpu,
        b02_gpu,
        b03_gpu,
        b04_gpu,
    )

    red = gpu_black_point_block(cp, gpu_scale_reflectance_block(cp, b03_gpu, max_value=100.0, gamma=1.05), 0.006)
    green = cp.clip(
        gpu_black_point_block(cp, gpu_scale_reflectance_block(cp, b02_gpu, max_value=100.0, gamma=1.04), 0.006)
        * cp.float32(0.56)
        + red * cp.float32(0.34)
        + gpu_black_point_block(cp, gpu_scale_reflectance_block(cp, b04_gpu, max_value=100.0, gamma=1.02), 0.004)
        * cp.float32(0.10),
        0.0,
        1.0,
    )
    blue = gpu_black_point_block(cp, gpu_scale_reflectance_block(cp, b01_gpu, max_value=100.0, gamma=1.03), 0.004)
    red = gpu_contrast_block(cp, cp.clip(red * cp.float32(1.12) + cp.float32(0.014), 0.0, 1.0), 1.13, 0.42)
    green = gpu_contrast_block(cp, cp.clip(green * cp.float32(1.07) + cp.float32(0.010), 0.0, 1.0), 1.10, 0.42)
    blue = gpu_contrast_block(cp, cp.clip(blue * cp.float32(0.88) + cp.float32(0.002), 0.0, 1.0), 1.04, 0.42)
    red, green, blue = gpu_saturation_block(cp, red, green, blue, 1.18)
    rgb = cp.stack([red, green, blue], axis=0)

    if use_hybrid:
        if b13 is None:
            raise ValueError("GPU hybrid custom composite requires B13.")
        b13_gpu = cp.asarray(b13, dtype=cp.float32)
        weight = cp.clip(
            (cp.float32(HYBRID_NIGHT_DAY_REFLECTANCE) - cp.nan_to_num(b03_gpu, nan=0.0))
            / cp.float32(HYBRID_NIGHT_DAY_REFLECTANCE - HYBRID_NIGHT_DARK_REFLECTANCE),
            0.0,
            1.0,
        )
        night = gpu_scale_ir_temperature_block(cp, b13_gpu, warm_k=305.0, cold_k=190.0, gamma=1.15)
        night_rgb = cp.stack(
            [
                night,
                cp.clip(night * cp.float32(0.95) + cp.float32(0.03), 0.0, 1.0),
                cp.clip(night * cp.float32(0.85) + cp.float32(0.06), 0.0, 1.0),
            ],
            axis=0,
        )
        rgb = rgb * (cp.float32(1.0) - weight[None, :, :]) + night_rgb * weight[None, :, :]

    out = cp.rint(cp.clip(rgb, 0.0, 1.0) * cp.float32(255.0)).astype(cp.uint8)
    result = cp.asnumpy(out)
    return result


def can_build_gpu_custom_composite(composite_choice: str) -> bool:
    return composite_choice in {"True Color RGB (Enhanced)", "True Color Reproduction Image"}


def build_gpu_custom_composite(scene: Scene, composite_choice: str, config: ProcessorConfig) -> tuple[str, xr.DataArray]:
    if not can_build_gpu_custom_composite(composite_choice):
        raise KeyError(f"GPU custom composite is not implemented for {composite_choice}.")
    require_gpu_ready()
    use_hybrid = config.use_night_fallback and uses_hybrid_night_fallback(
        composite_choice,
        config.night_fallback_mode,
    )
    bands = ["B01", "B02", "B03", "B04"] + (["B13"] if use_hybrid else [])
    missing = scene_missing_bands(scene, bands)
    if missing:
        raise RuntimeError(
            "GPU custom composite is missing required resampled band(s): "
            + ", ".join(missing)
            + ". Disable GPU acceleration and retry, or choose a product whose required bands are available."
        )
    reference = scene["B03"]
    y_dim, x_dim = reference.dims[-2], reference.dims[-1]
    expected_shape = (int(reference.sizes[y_dim]), int(reference.sizes[x_dim]))
    arrays = []
    for band in bands:
        source = scene[band]
        if len(source.dims) < 2:
            raise RuntimeError(f"GPU custom composite band {band} is not two-dimensional after resampling.")
        band_shape = (int(source.sizes[source.dims[-2]]), int(source.sizes[source.dims[-1]]))
        if band_shape != expected_shape:
            raise RuntimeError(
                f"GPU custom composite band {band} has shape {band_shape}, expected {expected_shape}. "
                "This usually means the source bands did not resample to the same target grid."
            )
        arrays.append(source.data if isinstance(source.data, da.Array) else da.from_array(source.data, chunks=source.shape))
    spatial_chunks = bounded_2d_chunk_grid(expected_shape)
    arrays = [rechunk_2d_array_to_grid(array, spatial_chunks) for array in arrays]
    chunks = ((3,), spatial_chunks[0], spatial_chunks[1])
    LOG.info(
        "GPU custom composite chunk grid: y=%s x=%s",
        max(spatial_chunks[0]),
        max(spatial_chunks[1]),
    )
    try:
        rgb_data = da.map_blocks(
            gpu_true_color_reproduction_block,
            *arrays,
            dtype=np.uint8,
            chunks=chunks,
            new_axis=[0],
            use_hybrid=use_hybrid,
        )
    except Exception as exc:
        raise RuntimeError(
            "GPU custom composite graph setup failed. Disable GPU acceleration and retry this run. "
            f"{exc.__class__.__name__}: {exc}"
        ) from exc
    name = hybrid_dataset_name(composite_choice) if use_hybrid else CUSTOM_DATASET_NAMES[composite_choice]
    attrs = scene["B03"].attrs.copy()
    attrs.update(
        {
            "name": name,
            "standard_name": name,
            "mode": "RGB",
            "sensor": "ahi",
        }
    )
    if use_hybrid:
        attrs["night_fallback_mode"] = "hybrid"
    attrs["_FillValue"] = np.uint8(0)
    attrs.pop("calibration", None)
    attrs.pop("wavelength", None)
    attrs.pop("units", None)
    dataset = xr.DataArray(
        rgb_data,
        dims=("bands", y_dim, x_dim),
        coords={"bands": ["R", "G", "B"]},
        attrs=attrs,
    )
    return name, dataset


def flat_map_validity_mask_from_scene(scene: Scene, bands: Iterable[str], area: AreaDefinition) -> np.ndarray | None:
    masks: list[da.Array] = []
    for band in bands:
        try:
            source = scene[band]
        except KeyError:
            continue
        if len(source.dims) < 2:
            continue
        y_dim, x_dim = source.dims[-2], source.dims[-1]
        if int(source.sizes[y_dim]) != int(area.height) or int(source.sizes[x_dim]) != int(area.width):
            continue
        data = source.data if isinstance(source.data, da.Array) else da.from_array(source.data, chunks=source.shape)
        if data.dtype.kind not in {"f", "i", "u"}:
            continue
        finite = da.isfinite(data)
        signal = da.where(finite, data, 0.0)
        has_signal = da.fabs(signal) > FLAT_MAP_SOURCE_VALID_MIN
        masks.append(finite & has_signal)
    if not masks:
        return None
    combined = masks[0]
    for mask in masks[1:]:
        combined = combined | mask
    return np.asarray(combined.compute(), dtype=bool)


def combine_validity_masks(*masks: np.ndarray | None) -> np.ndarray | None:
    combined: np.ndarray | None = None
    for mask in masks:
        if mask is None:
            continue
        current = np.asarray(mask, dtype=bool)
        if combined is None:
            combined = current.copy()
            continue
        if current.shape != combined.shape:
            raise ValueError(f"Validity mask shape {current.shape} does not match {combined.shape}.")
        combined &= current
    return combined


def flat_map_geometry_valid_mask_tile(
    source_area: AreaDefinition,
    target_area: AreaDefinition,
    y_offset: int,
    height: int,
    x_offset: int,
    width: int,
) -> np.ndarray:
    lon, lat = flat_map_tile_lonlat(target_area, y_offset, height, x_offset, width)
    cols, rows = source_area.get_array_coordinates_from_lonlat(lon, lat)
    rows_float = np.broadcast_to(np.asarray(rows, dtype=np.float64), (height, width))
    cols_float = np.broadcast_to(np.asarray(cols, dtype=np.float64), (height, width))
    finite = np.isfinite(lon) & np.isfinite(lat) & np.isfinite(rows_float) & np.isfinite(cols_float)
    rounded_rows = np.rint(np.where(finite, rows_float, -1.0)).astype(np.int64)
    rounded_cols = np.rint(np.where(finite, cols_float, -1.0)).astype(np.int64)
    return (
        finite
        & (rounded_rows >= 0)
        & (rounded_rows < int(source_area.height))
        & (rounded_cols >= 0)
        & (rounded_cols < int(source_area.width))
    )


def flat_map_geometry_validity_mask(
    source_area: AreaDefinition,
    target_area: AreaDefinition,
    chunk_size: int = FLAT_MAP_DIRECT_SAMPLE_CHUNK,
) -> np.ndarray:
    mask = np.zeros((int(target_area.height), int(target_area.width)), dtype=bool)
    y_chunks, x_chunks = bounded_2d_chunk_grid(
        mask.shape,
        max_edge=max(1, int(chunk_size)),
        max_pixels=max(1, int(chunk_size)) * max(1, int(chunk_size)),
    )
    for y_offset, y_size in chunk_offsets(y_chunks):
        for x_offset, x_size in chunk_offsets(x_chunks):
            mask[y_offset : y_offset + y_size, x_offset : x_offset + x_size] = flat_map_geometry_valid_mask_tile(
                source_area,
                target_area,
                y_offset,
                y_size,
                x_offset,
                x_size,
            )
    return mask


def flat_map_geometry_validity_mask_from_scene(
    scene: Scene,
    bands: Iterable[str],
    target_area: AreaDefinition,
    chunk_size: int = FLAT_MAP_DIRECT_SAMPLE_CHUNK,
) -> np.ndarray | None:
    for band in bands:
        try:
            source = scene[band]
        except KeyError:
            continue
        source_area = source.attrs.get("area")
        if source_area is not None:
            return flat_map_geometry_validity_mask(source_area, target_area, chunk_size=chunk_size)
    return None


def direct_flat_map_validity_mask_from_scenes(
    source_scene: Scene,
    sampled_scene: Scene,
    bands: Iterable[str],
    target_area: AreaDefinition,
) -> np.ndarray | None:
    geometry_mask = flat_map_geometry_validity_mask_from_scene(source_scene, bands, target_area)
    sampled_signal_mask = flat_map_validity_mask_from_scene(sampled_scene, bands, target_area)
    return combine_validity_masks(geometry_mask, sampled_signal_mask)


def flat_map_tile_lonlat(area: AreaDefinition, y_offset: int, height: int, x_offset: int, width: int) -> tuple[np.ndarray, np.ndarray]:
    left, bottom, right, top = area.area_extent
    pixel_width = (right - left) / float(area.width)
    pixel_height = (top - bottom) / float(area.height)
    x_values = left + (np.arange(x_offset, x_offset + width, dtype=np.float64) + 0.5) * pixel_width
    y_values = top - (np.arange(y_offset, y_offset + height, dtype=np.float64) + 0.5) * pixel_height
    x_grid, y_grid = np.meshgrid(x_values, y_values)
    lon, lat = area.get_lonlat_from_projection_coordinates(x_grid, y_grid)
    return np.asarray(lon, dtype=np.float64), np.asarray(lat, dtype=np.float64)


def sample_source_band_tile(
    source_data,
    source_area: AreaDefinition,
    target_area: AreaDefinition,
    y_offset: int,
    height: int,
    x_offset: int,
    width: int,
) -> np.ndarray:
    lon, lat = flat_map_tile_lonlat(target_area, y_offset, height, x_offset, width)
    cols, rows = source_area.get_array_coordinates_from_lonlat(lon, lat)
    rows = np.broadcast_to(np.asarray(np.rint(rows), dtype=np.int64), (height, width))
    cols = np.broadcast_to(np.asarray(np.rint(cols), dtype=np.int64), (height, width))
    valid = (
        np.isfinite(lon)
        & np.isfinite(lat)
        & (rows >= 0)
        & (rows < int(source_area.height))
        & (cols >= 0)
        & (cols < int(source_area.width))
    )
    output = np.full((height, width), np.nan, dtype=np.float32)
    if not np.any(valid):
        return output

    valid_rows = rows[valid]
    valid_cols = cols[valid]
    row_min, row_max = int(valid_rows.min()), int(valid_rows.max()) + 1
    col_min, col_max = int(valid_cols.min()), int(valid_cols.max()) + 1
    source_slice = source_data[row_min:row_max, col_min:col_max]
    if isinstance(source_slice, da.Array):
        source_slice = source_slice.compute(scheduler="synchronous")
    source_block = np.asarray(source_slice, dtype=np.float32)
    sampled = source_block[valid_rows - row_min, valid_cols - col_min]
    output[valid] = sampled
    return output


def direct_flat_map_sample_band(source: xr.DataArray, target_area: AreaDefinition, chunk_size: int = FLAT_MAP_DIRECT_SAMPLE_CHUNK) -> xr.DataArray:
    if len(source.dims) < 2:
        raise RuntimeError("Direct flat-map sampling requires a two-dimensional source band.")
    source_area = source.attrs.get("area")
    if source_area is None:
        raise RuntimeError("Direct flat-map sampling requires source bands with area metadata.")
    y_dim, x_dim = source.dims[-2], source.dims[-1]
    source_data = source.data if isinstance(source.data, da.Array) else da.from_array(source.data, chunks=source.shape)
    y_chunks, x_chunks = bounded_2d_chunk_grid(
        (int(target_area.height), int(target_area.width)),
        max_edge=max(1, int(chunk_size)),
        max_pixels=max(1, int(chunk_size)) * max(1, int(chunk_size)),
    )
    rows: list[list[da.Array]] = []
    def sample_tile(y_offset: int, y_size: int, x_offset: int, x_size: int) -> np.ndarray:
        return sample_source_band_tile(source_data, source_area, target_area, y_offset, y_size, x_offset, x_size)

    for y_offset, y_size in chunk_offsets(y_chunks):
        row_blocks: list[da.Array] = []
        for x_offset, x_size in chunk_offsets(x_chunks):
            delayed_tile = dask.delayed(sample_tile, pure=False)(y_offset, y_size, x_offset, x_size)
            row_blocks.append(da.from_delayed(delayed_tile, shape=(y_size, x_size), dtype=np.float32))
        rows.append(row_blocks)
    data = da.block(rows)
    attrs = source.attrs.copy()
    attrs["area"] = target_area
    attrs.pop("_FillValue", None)
    return xr.DataArray(data, dims=(y_dim, x_dim), attrs=attrs)


def direct_flat_map_sample_scene(scene: Scene, bands: Iterable[str], target_area: AreaDefinition) -> dict[str, xr.DataArray]:
    sampled: dict[str, xr.DataArray] = {}
    for band in bands:
        sampled[band] = direct_flat_map_sample_band(scene[band], target_area)
    return sampled


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
        root = f"https://{match.group('host')}/AHI-L1b-{area}/"
        return UrlInfo(
            root=root,
            sat_id=sat_id_from_himawari_bucket_url(root),
            timestamp=timestamp,
            area=area,
            total_segments=10,
        )

    raise ValueError(f"URL not recognised: {url}")


def normalized_himawari_sat_id(sat_id: str | None) -> str:
    normalized = (sat_id or "HS_H09").strip().upper()
    if normalized not in HIMAWARI_BUCKET_BY_SAT_ID:
        raise ValueError(f"Unsupported Himawari satellite id: {sat_id}")
    return normalized


def himawari_bucket_for_sat_id(sat_id: str | None = None) -> str:
    return HIMAWARI_BUCKET_BY_SAT_ID[normalized_himawari_sat_id(sat_id)]


def sat_id_from_himawari_bucket_url(url: str) -> str:
    host = re.search(r"https?://([^/]+)", url.strip(), flags=re.IGNORECASE)
    hostname = host.group(1).lower() if host else ""
    if "himawari8" in hostname:
        return "HS_H08"
    if "himawari9" in hostname:
        return "HS_H09"
    return "HS_H09"


def sat_id_from_himawari_source(value: str | None) -> str:
    text = (value or "").strip()
    if not text:
        return "HS_H09"
    try:
        return parse_url(text).sat_id
    except ValueError:
        try:
            return parse_local_hsd_segment(text).sat_id
        except ValueError:
            return sat_id_from_himawari_bucket_url(text)


def noaa_scan_prefix(dt: datetime, area: str = "FLDK") -> str:
    return f"AHI-L1b-{area}/{dt:%Y/%m/%d/%H%M}/"


def parse_s3_listing_keys(xml_text: str) -> list[str]:
    root = ET.fromstring(xml_text)
    keys: list[str] = []
    for element in root.iter():
        if element.tag.rsplit("}", 1)[-1] == "Key" and element.text:
            keys.append(element.text)
    return keys


def object_url_from_s3_key(key: str, sat_id: str | None = None) -> str:
    return f"{himawari_bucket_for_sat_id(sat_id)}/{key}"


def latest_fldk_url_from_listing(xml_text: str, sat_id: str | None = None) -> str | None:
    sat_id = normalized_himawari_sat_id(sat_id)
    keys = parse_s3_listing_keys(xml_text)
    candidates = [
        key
        for key in keys
        if key.endswith(".DAT.bz2") and re.search(rf"{re.escape(sat_id)}_\d{{8}}_\d{{4}}_B01_FLDK_R10_S01\d{{2}}\.DAT\.bz2$", key)
    ]
    if not candidates:
        return None
    return object_url_from_s3_key(sorted(candidates)[0], sat_id=sat_id)


def fldk_scan_choices_from_listing(xml_text: str, sat_id: str | None = None) -> list[RecentScanChoice]:
    sat_id = normalized_himawari_sat_id(sat_id)
    keys = parse_s3_listing_keys(xml_text)
    choices: list[RecentScanChoice] = []
    for key in sorted(keys):
        match = re.search(rf"{re.escape(sat_id)}_(\d{{8}}_\d{{4}})_(B\d{{2}})_FLDK_R\d{{2}}_S01\d{{2}}\.DAT\.bz2$", key)
        if not match:
            continue
        timestamp, band = match.groups()
        choices.append(
            RecentScanChoice(
                timestamp=timestamp,
                band=band,
                url=object_url_from_s3_key(key, sat_id=sat_id),
                label=f"{timestamp} {band}",
            )
        )
    return choices


def fetch_s3_prefix_listing(prefix: str, timeout: int = 20, sat_id: str | None = None) -> str:
    response = requests.get(
        himawari_bucket_for_sat_id(sat_id),
        params={"list-type": "2", "prefix": prefix, "max-keys": "25"},
        timeout=timeout,
    )
    response.raise_for_status()
    return response.text


def data_host_for_source(source: str | None) -> str:
    """Return the bucket URL whose host should be tested for a given source.

    Falls back to the Himawari-9 bucket when the source cannot be parsed.
    """
    try:
        return himawari_bucket_for_sat_id(sat_id_from_himawari_source(source))
    except Exception:
        return NOAA_HIMAWARI9_BUCKET


def check_data_host_connectivity(source: str | None = None, timeout: float = 8.0) -> ConnectivityResult:
    """Test whether the satellite data host is reachable (DNS + HTTP HEAD).

    This isolates "is it the network?" from "is it the app?" when a run fails. It
    resolves the host name (DNS) and issues a lightweight HEAD request to the
    bucket, timing both. Any HTTP status (including S3's 403/405 on the root)
    counts as reachable because it proves the request reached the host.
    """
    import socket
    from urllib.parse import urlparse

    bucket_url = data_host_for_source(source)
    host = urlparse(bucket_url).netloc or "noaa-himawari9.s3.amazonaws.com"

    dns_ok = False
    dns_ms: float | None = None
    resolved: tuple[str, ...] = ()
    http_ok = False
    http_status: int | None = None
    http_ms: float | None = None
    error = ""

    start = time.perf_counter()
    try:
        infos = socket.getaddrinfo(host, 443, proto=socket.IPPROTO_TCP)
        dns_ms = (time.perf_counter() - start) * 1000.0
        dns_ok = True
        ips: list[str] = []
        for info in infos:
            ip = info[4][0]
            if ip not in ips:
                ips.append(ip)
        resolved = tuple(ips)
    except Exception as exc:
        dns_ms = (time.perf_counter() - start) * 1000.0
        error = f"DNS resolution failed: {exc}"

    if dns_ok:
        start = time.perf_counter()
        try:
            response = requests.head(
                bucket_url,
                timeout=timeout,
                allow_redirects=True,
                headers={"User-Agent": f"{APP_DISPLAY_NAME}/{APP_VERSION}"},
            )
            http_ms = (time.perf_counter() - start) * 1000.0
            http_status = int(response.status_code)
            http_ok = True
        except Exception as exc:
            http_ms = (time.perf_counter() - start) * 1000.0
            error = error or f"HTTP HEAD request failed: {exc}"

    return ConnectivityResult(
        host=host,
        dns_ok=dns_ok,
        dns_ms=dns_ms,
        resolved_ips=resolved,
        http_ok=http_ok,
        http_status=http_status,
        http_ms=http_ms,
        error=error,
    )


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
    sat_id: str | None = None,
) -> str:
    sat_id = normalized_himawari_sat_id(sat_id)
    fetch_listing = fetch_listing or (lambda prefix: fetch_s3_prefix_listing(prefix, sat_id=sat_id))
    last_error: Exception | None = None
    for dt in recent_himawari_scan_datetimes(now=now, lookback_hours=lookback_hours):
        prefix = noaa_scan_prefix(dt, "FLDK")
        try:
            url = latest_fldk_url_from_listing(fetch_listing(prefix), sat_id=sat_id)
        except Exception as exc:
            last_error = exc
            continue
        if url:
            return url
    if last_error is not None:
        raise RuntimeError(f"Could not find a recent FLDK scan: {last_error}") from last_error
    raise RuntimeError(f"No FLDK scans found in the last {lookback_hours} hour(s).")


def find_recent_fldk_scan_choices(
    now: datetime | None = None,
    lookback_hours: int = 6,
    fetch_listing: Callable[[str], str] | None = None,
    limit: int = 24,
    sat_id: str | None = None,
) -> list[RecentScanChoice]:
    sat_id = normalized_himawari_sat_id(sat_id)
    fetch_listing = fetch_listing or (lambda prefix: fetch_s3_prefix_listing(prefix, sat_id=sat_id))
    choices: list[RecentScanChoice] = []
    seen: set[tuple[str, str]] = set()
    last_error: Exception | None = None
    for dt in recent_himawari_scan_datetimes(now=now, lookback_hours=lookback_hours):
        try:
            listing_choices = fldk_scan_choices_from_listing(fetch_listing(noaa_scan_prefix(dt, "FLDK")), sat_id=sat_id)
        except Exception as exc:
            last_error = exc
            continue
        for choice in listing_choices:
            key = (choice.timestamp, choice.band)
            if key in seen:
                continue
            seen.add(key)
            choices.append(choice)
            if len(choices) >= limit:
                return choices
    if choices:
        return choices
    if last_error is not None:
        raise RuntimeError(f"Could not list recent FLDK scans: {last_error}") from last_error
    raise RuntimeError(f"No recent FLDK scans found in the last {lookback_hours} hour(s).")


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


def local_segment_destination(info: LocalSegmentInfo, temp_dir: Path) -> Path:
    frame_dir = temp_dir / info.timestamp
    filename = (
        f"{info.sat_id}_{info.timestamp}_{info.band}_{info.area}_"
        f"{info.resolution}_S{info.segment:02d}{info.total_segments:02d}.DAT"
    )
    return frame_dir / filename


def parse_local_hsd_segment(path: str | Path) -> LocalSegmentInfo:
    source_path = Path(path).expanduser()
    name = source_path.name
    match = re.fullmatch(
        r"(HS_H0[89])_(\d{8}_\d{4})_(B\d{2})_([A-Z0-9]+)_(R\d{2})_S(\d{2})(\d{2})\.DAT(?:\.bz2)?",
        name,
        flags=re.IGNORECASE,
    )
    if not match:
        raise ValueError(
            "Local HSD file name must look like "
            "HS_H08_YYYYMMDD_HHMM_B13_FLDK_R20_S0110.DAT "
            "or HS_H09_YYYYMMDD_HHMM_B13_FLDK_R20_S0110.DAT, "
            "with optional .bz2."
        )
    sat_id, timestamp, band, area, resolution, segment, total_segments = match.groups()
    band = band.upper()
    resolution = resolution.upper()
    expected_resolution = BAND_RESOLUTION.get(band)
    if expected_resolution is None:
        raise ValueError(f"Unsupported local HSD band: {band}")
    if resolution != expected_resolution:
        raise ValueError(f"{band} files must use {expected_resolution}; got {resolution}.")
    try:
        datetime.strptime(timestamp, "%Y%m%d_%H%M")
    except ValueError as exc:
        raise ValueError(f"Invalid local HSD timestamp in {name}: {timestamp}") from exc
    segment_number = int(segment)
    segment_total = int(total_segments)
    if segment_number < 1 or segment_total < 1 or segment_number > segment_total:
        raise ValueError(f"Invalid segment number in {name}: S{segment}{total_segments}")
    return LocalSegmentInfo(
        source_path=source_path,
        sat_id=sat_id.upper(),
        timestamp=timestamp,
        band=band,
        area=area.upper(),
        resolution=resolution,
        segment=segment_number,
        total_segments=segment_total,
        compressed=name.lower().endswith(".dat.bz2"),
    )


def sorted_local_segments(paths: Iterable[str | Path]) -> list[LocalSegmentInfo]:
    infos = [parse_local_hsd_segment(path) for path in paths]
    if not infos:
        raise ValueError("Choose at least one local Himawari .DAT or .DAT.bz2 file.")

    first = infos[0]
    scan_key = (first.sat_id, first.timestamp, first.area, first.total_segments)
    seen: dict[tuple[str, int], LocalSegmentInfo] = {}
    for info in infos:
        current_key = (info.sat_id, info.timestamp, info.area, info.total_segments)
        if current_key != scan_key:
            raise ValueError(
                "Local import supports one scan at a time. "
                f"Expected {first.sat_id} {first.timestamp} {first.area} Sxx{first.total_segments:02d}; "
                f"got {info.sat_id} {info.timestamp} {info.area} Sxx{info.total_segments:02d}."
            )
        duplicate_key = (info.band, info.segment)
        if duplicate_key in seen:
            raise ValueError(
                f"Duplicate local segment for {info.band} S{info.segment:02d}/{info.total_segments:02d}: "
                f"{seen[duplicate_key].source_path.name} and {info.source_path.name}"
            )
        seen[duplicate_key] = info
    return sorted(infos, key=lambda item: (item.band, item.segment, item.source_path.name.lower()))


def copy_local_dat_file(info: LocalSegmentInfo, destination: Path) -> bool:
    if destination.exists() and destination.stat().st_size > 0:
        return False
    destination.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = destination.with_suffix(destination.suffix + ".part")
    try:
        remove_partial_download(tmp_path)
        shutil.copyfile(info.source_path, tmp_path)
        tmp_path.replace(destination)
        return True
    except Exception:
        remove_partial_download(tmp_path)
        raise


def decompress_local_bz2_file(
    info: LocalSegmentInfo,
    destination: Path,
    cancel_event: threading.Event | None = None,
) -> bool:
    if destination.exists() and destination.stat().st_size > 0:
        return False
    destination.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = destination.with_suffix(destination.suffix + ".part")
    try:
        remove_partial_download(tmp_path)
        decompressor = bz2.BZ2Decompressor()
        with info.source_path.open("rb") as in_file, tmp_path.open("wb") as out_file:
            while True:
                check_cancel(cancel_event)
                chunk = in_file.read(DOWNLOAD_STREAM_CHUNK_BYTES)
                if not chunk:
                    break
                decompressor = write_decompressed_bz2_chunk(decompressor, chunk, out_file)
            if not decompressor.eof:
                raise EOFError("Compressed local file ended before bz2 EOF marker.")
        tmp_path.replace(destination)
        return True
    except ProcessingCancelled:
        remove_partial_download(tmp_path)
        raise
    except Exception:
        remove_partial_download(tmp_path)
        raise


def import_local_hsd_segments(
    paths: Iterable[str | Path],
    temp_dir: Path,
    progress: ProgressCallback | None = None,
    cancel_event: threading.Event | None = None,
) -> LocalImportResult:
    infos = sorted_local_segments(paths)
    invalid_sources: list[Path] = []
    for info in infos:
        try:
            if not info.source_path.is_file() or info.source_path.stat().st_size <= 0:
                invalid_sources.append(info.source_path)
        except OSError:
            invalid_sources.append(info.source_path)
    if invalid_sources:
        shown = ", ".join(str(path) for path in invalid_sources[:5])
        if len(invalid_sources) > 5:
            shown += f", ... plus {len(invalid_sources) - 5} more"
        raise FileNotFoundError(f"Local HSD import file(s) not found, empty, or not readable: {shown}")
    total = len(infos)
    imported_paths: list[Path] = []
    reused_paths: list[Path] = []
    for idx, info in enumerate(infos, start=1):
        check_cancel(cancel_event)
        destination = local_segment_destination(info, temp_dir)
        emit_progress(progress, f"Importing {info.band} S{info.segment:02d}/{info.total_segments:02d}", idx - 1, total)
        if info.compressed:
            imported = decompress_local_bz2_file(info, destination, cancel_event=cancel_event)
        else:
            imported = copy_local_dat_file(info, destination)
        if imported:
            imported_paths.append(destination)
        else:
            reused_paths.append(destination)
        emit_progress(progress, f"Imported {idx}/{total} local file(s)", idx, total)

    bands = tuple(sorted({info.band for info in infos}))
    first = infos[0]
    return LocalImportResult(
        sat_id=first.sat_id,
        timestamp=first.timestamp,
        area=first.area,
        total_segments=first.total_segments,
        imported_paths=tuple(imported_paths),
        reused_paths=tuple(reused_paths),
        bands=bands,
    )


def ahi_l1b_folder_for_area(area: str) -> str:
    normalized = area.upper()
    if normalized == "FLDK":
        return "FLDK"
    if normalized == "JAPAN":
        return "Japan"
    return "Target"


def offline_source_url_for_import(result: LocalImportResult) -> str:
    timestamp = datetime.strptime(result.timestamp, "%Y%m%d_%H%M")
    band = result.bands[0] if result.bands else "B01"
    filename = (
        f"{result.sat_id}_{result.timestamp}_{band}_{result.area}_"
        f"{BAND_RESOLUTION[band]}_S01{result.total_segments:02d}.DAT.bz2"
    )
    return f"{himawari_bucket_for_sat_id(result.sat_id)}/AHI-L1b-{ahi_l1b_folder_for_area(result.area)}/{timestamp_path(timestamp)}{filename}"


def _geos_pixel_to_lonlat(col: np.ndarray, line: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    """Convert AHI full-disk pixel (column, line) to (lon, lat) in degrees.

    Implements the standard CGMS/LRIT geostationary inverse navigation using the
    fixed Himawari AHI full-disk constants. Pixels that fall off the Earth disk
    return NaN. Only used to estimate each download segment's latitude band, so
    the 2 km reference grid is sufficient.
    """
    req = AHI_EARTH_EQUATORIAL_RADIUS_M
    rpol = AHI_EARTH_POLAR_RADIUS_M
    h = AHI_SATELLITE_DISTANCE_M
    ratio = (req / rpol) ** 2
    x = np.radians((col - AHI_FULL_DISK_2KM_COFF) / ((2.0 ** -16) * AHI_FULL_DISK_2KM_CFAC))
    y = np.radians((line - AHI_FULL_DISK_2KM_LOFF) / ((2.0 ** -16) * AHI_FULL_DISK_2KM_LFAC))
    cosx = np.cos(x)
    sinx = np.sin(x)
    cosy = np.cos(y)
    siny = np.sin(y)
    denom = cosy ** 2 + ratio * siny ** 2
    disc = (h * cosx * cosy) ** 2 - denom * (h ** 2 - req ** 2)
    with np.errstate(invalid="ignore"):
        disc = np.where(disc < 0.0, np.nan, disc)
        sd = np.sqrt(disc)
        sn = (h * cosx * cosy - sd) / denom
        s1 = h - sn * cosx * cosy
        s2 = sn * sinx * cosy
        s3 = -sn * siny
        lon = np.degrees(np.arctan2(s2, s1)) + AHI_SUB_SATELLITE_LON_DEG
        lat = np.degrees(np.arctan(ratio * s3 / np.sqrt(s1 ** 2 + s2 ** 2)))
    return lon, lat


def fldk_segment_latitude_bounds(total_segments: int) -> list[tuple[float, float] | None]:
    """Return the (min_lat, max_lat) latitude band each FLDK segment covers.

    Himawari full-disk scans are split into ``total_segments`` equal-height
    horizontal strips. Segment k covers the same fraction of the disk at every
    band resolution, so the 2 km reference grid gives the latitude band that
    applies to every band. A segment that is entirely off-disk returns None.
    """
    total = int(total_segments)
    if total <= 0:
        return []
    lines_total = float(AHI_FULL_DISK_2KM_LINES)
    seg_height = lines_total / total
    cols = np.linspace(0.0, lines_total - 1.0, 48)
    bands: list[tuple[float, float] | None] = []
    for k in range(1, total + 1):
        row0 = (k - 1) * seg_height
        row1 = k * seg_height - 1.0
        rows = np.linspace(row0, row1, 20)
        grid_cols, grid_rows = np.meshgrid(cols, rows)
        _lon, lat = _geos_pixel_to_lonlat(grid_cols, grid_rows)
        finite = lat[np.isfinite(lat)]
        if finite.size == 0:
            bands.append(None)
        else:
            bands.append((float(finite.min()), float(finite.max())))
    return bands


def segments_intersecting_lat_bounds(
    total_segments: int,
    min_lat: float,
    max_lat: float,
    margin: int = SEGMENT_DOWNLOAD_MARGIN,
) -> list[int]:
    """Contiguous list of segment numbers whose latitude band touches the bounds.

    ``margin`` keeps a few extra segments on each side so a slightly imperfect
    latitude estimate can never drop a needed segment. If nothing intersects (or
    the geometry cannot be evaluated) every segment is returned, so the result is
    always a safe superset of what the crop needs.
    """
    total = int(total_segments)
    if total <= 0:
        return []
    try:
        lo = float(min(min_lat, max_lat))
        hi = float(max(min_lat, max_lat))
        bands = fldk_segment_latitude_bounds(total)
        hits: list[int] = []
        for index, band in enumerate(bands, start=1):
            if band is None:
                # Polar/off-disk segment: keep it rather than risk dropping data.
                hits.append(index)
                continue
            band_lo, band_hi = band
            if band_hi >= lo and band_lo <= hi:
                hits.append(index)
        if not hits:
            return list(range(1, total + 1))
        margin = max(0, int(margin))
        lo_seg = max(1, min(hits) - margin)
        hi_seg = min(total, max(hits) + margin)
        return list(range(lo_seg, hi_seg + 1))
    except Exception as exc:  # pragma: no cover - safety net
        LOG.debug("Segment latitude estimate failed (%s); downloading all segments.", exc)
        return list(range(1, total + 1))


def segments_for_flat_bounds(info: UrlInfo, config: ProcessorConfig) -> list[int] | None:
    """Segments needed for the configured flat-map crop, or None to use all.

    Returns None (meaning "download every segment") when segment-aware downloads
    are disabled, when the source is not a full disk, or when the crop already
    spans effectively the whole disk so trimming would save nothing.
    """
    if not getattr(config, "segment_aware_downloads", True):
        return None
    if not SEGMENT_AWARE_DOWNLOADS:
        return None
    if str(info.area).upper() != "FLDK":
        return None
    if not is_flat_map(config):
        return None
    total = int(info.total_segments)
    if total <= 1:
        return None
    selected = segments_intersecting_lat_bounds(
        total, config.flat_min_lat, config.flat_max_lat, margin=SEGMENT_DOWNLOAD_MARGIN
    )
    if not selected or len(selected) >= total:
        return None
    return selected


def make_download_tasks(
    info: UrlInfo,
    dt: datetime,
    bands: Iterable[str],
    temp_dir: Path,
    segment_numbers: Iterable[int] | None = None,
) -> list[DownloadTask]:
    tasks = []
    d_path = timestamp_path(dt)
    frame_dir = temp_dir / dt.strftime("%Y%m%d_%H%M")
    frame_dir.mkdir(parents=True, exist_ok=True)
    if segment_numbers is None:
        segments = list(range(1, info.total_segments + 1))
    else:
        # Keep only valid, in-range, sorted, de-duplicated segment numbers.
        segments = sorted({int(s) for s in segment_numbers if 1 <= int(s) <= info.total_segments})
        if not segments:
            segments = list(range(1, info.total_segments + 1))
    for band in bands:
        for segment in segments:
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


def phase_from_progress_message(message: str) -> str:
    normalized = message.strip().lower()
    if not normalized:
        return "Working"
    if "cancel" in normalized:
        return "Canceled" if "canceled" in normalized else "Canceling"
    if normalized.startswith("checking") or "preflight" in normalized:
        return "Checking"
    if normalized.startswith("download") or " existing files" in normalized:
        return "Downloading"
    if normalized.startswith("loading"):
        return "Loading"
    if normalized.startswith("area scan") or normalized.startswith("scanning"):
        return "Scanning"
    if normalized.startswith("resampling"):
        return "Resampling"
    if normalized.startswith("building"):
        return "Building"
    if normalized.startswith("saving"):
        return "Saving"
    if normalized.startswith("assembling") or normalized.startswith("added frame"):
        return "Assembling"
    if normalized.startswith("reusing"):
        return "Resuming"
    if normalized.startswith("frame"):
        return "Processing"
    if normalized.startswith("timelapse"):
        return "Assembling"
    return "Working"


def format_eta_duration(seconds: float | int) -> str:
    seconds_int = max(0, int(round(float(seconds))))
    if seconds_int < 60:
        return f"{seconds_int}s"
    minutes, seconds_int = divmod(seconds_int, 60)
    if minutes < 60:
        return f"{minutes}m {seconds_int:02d}s"
    hours, minutes = divmod(minutes, 60)
    if hours < 24:
        return f"{hours}h {minutes:02d}m"
    days, hours = divmod(hours, 24)
    return f"{days}d {hours}h"


def format_estimated_duration(seconds: float | int) -> str:
    """Friendly, intentionally coarse duration for a pre-run estimate.

    The numbers are rounded into easy-to-read buckets so the value reads as a
    ballpark ("about 3 min") rather than a false-precision figure.
    """
    total = max(0.0, float(seconds))
    if total < 45.0:
        return "under 1 min"
    if total < 90.0:
        return "about 1 min"
    minutes = total / 60.0
    if minutes < 10.0:
        return f"about {int(round(minutes))} min"
    if minutes < 60.0:
        rounded = int(round(minutes / 5.0)) * 5
        return f"about {rounded} min"
    hours = int(total // 3600)
    remainder_minutes = int(round((total - hours * 3600) / 60.0))
    if remainder_minutes >= 60:
        hours += 1
        remainder_minutes = 0
    if remainder_minutes == 0:
        return f"about {hours}h"
    return f"about {hours}h {remainder_minutes:02d}m"


def estimate_run_seconds(
    config: ProcessorConfig,
    frame_count: int,
    band_count: int,
    segments_per_frame: int,
    output_megapixels: float,
) -> float:
    """Rough wall-clock estimate for a run, in seconds.

    Combines three terms: parallel segment downloads, per-frame rendering that
    scales with the output size, and (for timelapses) animation assembly. This
    is a planning aid only; the in-run ETA measures real speed.
    """
    workers = max(1, min(4, int(config.download_workers)))
    segments_total = max(0, int(frame_count)) * max(1, int(band_count)) * max(0, int(segments_per_frame))
    download_seconds = math.ceil(segments_total / workers) * ESTIMATE_SECONDS_PER_SEGMENT_DOWNLOAD
    megapixels = max(0.0, min(float(output_megapixels), ESTIMATE_MAX_RENDER_MEGAPIXELS))
    render_seconds = frame_count * (
        ESTIMATE_FRAME_FIXED_OVERHEAD + megapixels * ESTIMATE_SECONDS_PER_MEGAPIXEL_RENDER
    )
    assembly_seconds = 0.0
    if config.mode == "Timelapse" and frame_count > 1:
        assembly_seconds = frame_count * ESTIMATE_TIMELAPSE_ASSEMBLY_PER_FRAME
    return float(download_seconds + render_seconds + assembly_seconds)


class ProgressEtaEstimator:
    def __init__(self, clock: Callable[[], float] | None = None, min_elapsed_seconds: float = 1.0) -> None:
        self.clock = clock or time.monotonic
        self.min_elapsed_seconds = min_elapsed_seconds
        self._started_at: float | None = None
        self._last_current: int | None = None
        self._last_total: int | None = None
        self._last_phase: str | None = None
        self._smoothed_rate: float | None = None

    def reset(self) -> None:
        self._started_at = None
        self._last_current = None
        self._last_total = None
        self._last_phase = None
        self._smoothed_rate = None

    def update(self, message: str, current: int | None = None, total: int | None = None) -> str | None:
        if current is None or total is None or total <= 0:
            return None
        phase = phase_from_progress_message(message)
        now = self.clock()
        current_int = max(0, int(current))
        total_int = max(1, int(total))
        if (
            self._started_at is None
            or self._last_total != total_int
            or self._last_phase != phase
            or (self._last_current is not None and current_int < self._last_current)
        ):
            self._started_at = now
            self._smoothed_rate = None
        self._last_current = current_int
        self._last_total = total_int
        self._last_phase = phase
        if current_int <= 0 or current_int >= total_int or self._started_at is None:
            return None
        elapsed = max(0.0, now - self._started_at)
        if elapsed < self.min_elapsed_seconds:
            return None
        seconds_per_unit = elapsed / float(current_int)
        # Exponentially smooth the per-unit rate so the displayed ETA settles
        # instead of jumping on every progress tick.
        if self._smoothed_rate is None:
            self._smoothed_rate = seconds_per_unit
        else:
            self._smoothed_rate = 0.6 * self._smoothed_rate + 0.4 * seconds_per_unit
        remaining_seconds = self._smoothed_rate * float(total_int - current_int)
        if not math.isfinite(remaining_seconds) or remaining_seconds < 1.0:
            return None
        return f"ETA {format_eta_duration(remaining_seconds)}"


def format_phase_status(
    message: str,
    current: int | None = None,
    total: int | None = None,
    eta_text: str | None = None,
) -> str:
    phase = phase_from_progress_message(message)
    if current is not None and total:
        text = f"{phase}: {current}/{total} - {message}"
    else:
        text = f"{phase}: {message}"
    if eta_text:
        text = f"{text} ({eta_text})"
    return text


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


def write_decompressed_bz2_chunk(
    decompressor: bz2.BZ2Decompressor,
    chunk: bytes,
    out_file,
) -> bz2.BZ2Decompressor:
    data = chunk
    while data:
        try:
            output = decompressor.decompress(data)
            if output:
                out_file.write(output)
            data = decompressor.unused_data
            if data:
                decompressor = bz2.BZ2Decompressor()
        except EOFError:
            decompressor = bz2.BZ2Decompressor()
    return decompressor


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
                for chunk in response.iter_content(chunk_size=DOWNLOAD_STREAM_CHUNK_BYTES):
                    check_cancel(cancel_event)
                    if not chunk:
                        continue
                    decompressor = write_decompressed_bz2_chunk(decompressor, chunk, out_file)
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


def interruptible_sleep(seconds: float, cancel_event: threading.Event | None = None) -> None:
    """Sleep in small slices so a cancel request is honored promptly."""
    deadline = time.monotonic() + max(0.0, float(seconds))
    while time.monotonic() < deadline:
        check_cancel(cancel_event)
        time.sleep(min(0.25, deadline - time.monotonic()))


def _download_segment_pass(
    pass_tasks: list[DownloadTask],
    worker_count: int,
    progress: ProgressCallback | None,
    cancel_event: threading.Event | None,
    grand_total: int,
    already_done: int,
) -> None:
    pool = concurrent.futures.ThreadPoolExecutor(max_workers=worker_count)
    future_map: dict[concurrent.futures.Future[Path | None], DownloadTask] = {}
    completed = already_done

    def download_one(task: DownloadTask) -> Path | None:
        emit_progress(progress, f"Downloading {download_task_label(task)}", completed, grand_total)
        return stream_download_and_extract(task, cancel_event=cancel_event)

    try:
        for task in pass_tasks:
            check_cancel(cancel_event)
            future = pool.submit(download_one, task)
            future_map[future] = task
        pending = set(future_map)
        while pending:
            check_cancel(cancel_event)
            done, pending = concurrent.futures.wait(
                pending, timeout=0.2, return_when=concurrent.futures.FIRST_COMPLETED
            )
            for future in done:
                completed += 1
                future.result()
                label = download_task_label(future_map[future])
                emit_progress(progress, f"Downloaded {label} ({completed}/{grand_total})", completed, grand_total)
    except ProcessingCancelled:
        for future in future_map:
            future.cancel()
        emit_progress(progress, "Download canceled", completed, grand_total)
        raise
    finally:
        pool.shutdown(wait=True, cancel_futures=True)


def download_segments(
    tasks: list[DownloadTask],
    workers: int,
    auto_download: bool = AUTO_DOWNLOAD,
    progress: ProgressCallback | None = None,
    cancel_event: threading.Event | None = None,
    max_retries: int = DOWNLOAD_MAX_RETRIES,
    retry_backoff: float = DOWNLOAD_RETRY_BACKOFF_SECONDS,
) -> list[Path]:
    check_cancel(cancel_event)
    total = len(tasks)
    if not auto_download:
        existing = [task.destination for task in tasks if task.destination.exists() and task.destination.stat().st_size > 0]
        emit_progress(progress, f"Found {len(existing)}/{total} existing files", len(existing), total)
        return existing

    def have(task: DownloadTask) -> bool:
        return task.destination.exists() and task.destination.stat().st_size > 0

    worker_count = clamp_download_workers(workers)
    LOG.info("Downloading %s segments with %s worker(s)", total, worker_count)
    emit_progress(progress, download_workload_summary(tasks, worker_count), 0, total)

    attempt = 0
    while True:
        check_cancel(cancel_event)
        pending_tasks = [task for task in tasks if not have(task)]
        if not pending_tasks:
            break
        done_count = total - len(pending_tasks)
        _download_segment_pass(pending_tasks, worker_count, progress, cancel_event, total, done_count)
        remaining = [task for task in tasks if not have(task)]
        if not remaining or attempt >= max_retries:
            break
        attempt += 1
        LOG.warning(
            "Retrying %s missing segment(s): attempt %s/%s after %.0fs "
            "(usually a transient DNS/network blip).",
            len(remaining), attempt, max_retries, retry_backoff,
        )
        emit_progress(
            progress,
            f"Retry {attempt}/{max_retries}: {len(remaining)} segment(s) still missing",
            total - len(remaining), total,
        )
        interruptible_sleep(retry_backoff, cancel_event)

    downloaded = [task.destination for task in tasks if have(task)]
    emit_progress(progress, f"Downloaded {len(downloaded)}/{total} segments", len(downloaded), total)
    return downloaded


def missing_cached_segments(
    config: ProcessorConfig,
    info: UrlInfo,
    steps: list[datetime],
    bands: Iterable[str],
    temp_dir: Path,
) -> list[Path]:
    missing: list[Path] = []
    for dt in steps:
        for task in make_download_tasks(info, dt, bands, temp_dir):
            if not task.destination.exists() or task.destination.stat().st_size <= 0:
                missing.append(task.destination)
    return missing


def offline_cache_summary(missing: list[Path], total_expected: int) -> str:
    if not missing:
        return f"Offline cache: ready ({total_expected} required segment file(s) found)"
    shown = ", ".join(path.name for path in missing[:5])
    if len(missing) > 5:
        shown += f", ... plus {len(missing) - 5} more"
    return f"Offline cache is missing {len(missing)}/{total_expected} required segment file(s): {shown}"


def required_bands(
    composite_choice: str,
    include_night_fallback: bool = True,
    use_night_fallback: bool | None = None,
    night_fallback_mode: str | None = None,
) -> tuple[str, ...]:
    if use_night_fallback is None:
        use_night_fallback = USE_NIGHT_FALLBACK
    if night_fallback_mode is None:
        night_fallback_mode = NIGHT_FALLBACK_MODE
    if composite_choice not in COMPOSITE_BANDS:
        raise KeyError(f"Unknown composite: {composite_choice}")
    bands = list(COMPOSITE_BANDS[composite_choice])
    if include_night_fallback and composite_choice in DAY_ONLY_COMPOSITES and use_night_fallback:
        if uses_hybrid_night_fallback(composite_choice, night_fallback_mode):
            for band in ("B03", "B13"):
                if band not in bands:
                    bands.append(band)
        else:
            fallback = NIGHT_FALLBACK_MAP.get(composite_choice)
            if fallback:
                bands.extend(b for b in COMPOSITE_BANDS[fallback] if b not in bands)
    return tuple(dict.fromkeys(bands))


def area_compatibility_bands(
    composite_choice: str,
    use_night_fallback: bool = False,
    night_fallback_mode: str | None = None,
) -> tuple[str, ...]:
    """Bands that must align for the requested daytime product area."""
    if use_night_fallback and uses_hybrid_night_fallback(composite_choice, night_fallback_mode):
        return required_bands(
            composite_choice,
            include_night_fallback=True,
            use_night_fallback=True,
            night_fallback_mode=night_fallback_mode,
        )
    return required_bands(composite_choice, include_night_fallback=False)


def normalized_night_fallback_mode(mode: str | None) -> str:
    normalized = (mode or NIGHT_FALLBACK_MODE).strip().lower().replace("-", "_").replace(" ", "_")
    aliases = {
        "auto": "hybrid",
        "blend": "hybrid",
        "day_ir": "hybrid",
        "infrared": "whole_frame_ir",
        "ir": "whole_frame_ir",
        "whole": "whole_frame_ir",
        "whole_frame": "whole_frame_ir",
    }
    return aliases.get(normalized, normalized)


def uses_hybrid_night_fallback(composite_choice: str, mode: str | None = None) -> bool:
    return (
        normalized_night_fallback_mode(mode) == "hybrid"
        and composite_choice in {"True Color RGB (Enhanced)", "True Color Reproduction Image"}
    )


def select_active_composite(
    composite_choice: str,
    is_night: bool,
    use_night_fallback: bool | None = None,
    night_fallback_mode: str | None = None,
) -> str:
    if use_night_fallback is None:
        use_night_fallback = USE_NIGHT_FALLBACK
    if uses_hybrid_night_fallback(composite_choice, night_fallback_mode):
        return composite_choice
    if is_night and use_night_fallback and composite_choice in DAY_ONLY_COMPOSITES:
        return NIGHT_FALLBACK_MAP.get(composite_choice, "B13 (Infrared Window)")
    return composite_choice


def night_check_band_for_bands(bands: Iterable[str]) -> str | None:
    band_list = list(bands)
    if "B03" in band_list:
        return "B03"
    return next((band for band in band_list if band in REFLECTANCE_BANDS), None)


def target_pixel_size_m(
    composite_choice: str,
    use_night_fallback: bool | None = None,
    night_fallback_mode: str | None = None,
) -> int:
    """Choose the finest native pixel size needed by the requested product."""
    bands = required_bands(
        composite_choice,
        use_night_fallback=use_night_fallback,
        night_fallback_mode=night_fallback_mode,
    )
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


def centered_slice(length: int, fraction: float) -> slice:
    if length <= 0:
        return slice(0, 0)
    fraction = min(1.0, max(0.05, fraction))
    size = max(1, int(round(length * fraction)))
    start = max(0, (length - size) // 2)
    return slice(start, start + size)


def center_crop_sample(data: xr.DataArray, fraction: float = NIGHT_CHECK_CENTER_FRACTION) -> xr.DataArray:
    if len(data.dims) < 2:
        return data
    y_dim, x_dim = data.dims[-2], data.dims[-1]
    return data.isel(
        {
            y_dim: centered_slice(data.sizes[y_dim], fraction),
            x_dim: centered_slice(data.sizes[x_dim], fraction),
        }
    )


def visible_sample_is_dark(
    b03_sample: xr.DataArray,
    bright_reflectance: float = NIGHT_CHECK_BRIGHT_REFLECTANCE,
    bright_fraction_threshold: float = NIGHT_CHECK_BRIGHT_FRACTION,
) -> tuple[bool, float, float, int]:
    """Classify a small visible/near-IR reflectance sample without materializing a full disk.

    Full-disk scenes can have a bright limb even when the central view is night.
    A max-only test is therefore too optimistic and can leave true-color output
    mostly black. Use the center crop and the fraction of visibly bright pixels
    so a few sunlit edge pixels do not prevent the infrared night fallback.
    """
    center = center_crop_sample(b03_sample)
    valid = center.notnull()
    bright = (center > bright_reflectance) & valid
    stats = xr.Dataset(
        {
            "max_reflectance": center.max(skipna=True),
            "valid_pixels": valid.sum(),
            "bright_pixels": bright.sum(),
        }
    ).compute()
    valid_pixels = int(stats["valid_pixels"].item())
    bright_pixels = int(stats["bright_pixels"].item())
    max_reflectance = float(stats["max_reflectance"].item()) if valid_pixels else float("nan")
    bright_fraction = bright_pixels / valid_pixels if valid_pixels else 0.0
    return bright_fraction < bright_fraction_threshold, max_reflectance, bright_fraction, valid_pixels


def is_visible_dark(
    scene: Scene,
    area: AreaDefinition,
    band: str = "B03",
    direct_flat_map: bool = False,
) -> bool:
    try:
        scene.load([band], calibration="reflectance")
        sample_area = coarse_sample_area(area)
        LOG.info(
            "Night check sampling %s at %sx%s px instead of %sx%s full target%s",
            band,
            sample_area.width,
            sample_area.height,
            area.width,
            area.height,
            " using direct flat-map sampling" if direct_flat_map else "",
        )
        if direct_flat_map:
            sampled_band = direct_flat_map_sample_band(scene[band], sample_area)
        else:
            sampled = scene.resample(sample_area, resampler="nearest", radius_of_influence=10000)
            sampled_band = sampled[band]
        result, max_value, bright_fraction, valid_pixels = visible_sample_is_dark(sampled_band)
        LOG.info(
            "Night check: center %s max reflectance %.4f, bright %.2f%% of %s valid px -> %s",
            band,
            max_value,
            bright_fraction * 100.0,
            valid_pixels,
            result,
        )
        return result
    except Exception as exc:
        if direct_flat_map:
            LOG.warning(
                "Flat-map night check failed; continuing with pixel-level hybrid day/night blending "
                "instead of whole-frame night fallback: %s",
                exc,
            )
            return False
        LOG.warning("Night check failed; using night fallback to avoid black visible output: %s", exc)
        return True


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


def soften_repaired_true_color_channels(
    red: xr.DataArray,
    green: xr.DataArray,
    blue: xr.DataArray,
    valid_visible_count: xr.DataArray,
) -> tuple[xr.DataArray, xr.DataArray, xr.DataArray]:
    repaired = valid_visible_count <= 1
    luma = red * 0.2126 + green * 0.7152 + blue * 0.0722
    mix = xr.where(repaired, 0.45, 0.0)
    return (
        xr_clip(red * (1.0 - mix) + luma * mix, 0.0, 1.0),
        xr_clip(green * (1.0 - mix) + luma * mix, 0.0, 1.0),
        xr_clip(blue * (1.0 - mix) + luma * mix, 0.0, 1.0),
    )


def fill_visible_reflectance_gaps(*bands: xr.DataArray) -> tuple[xr.DataArray, ...]:
    cleaned = [band.where(np.isfinite(band) & (abs(band) > FLAT_MAP_SOURCE_VALID_MIN)) for band in bands]
    fallback = xr.concat(cleaned, dim="_visible_reflectance_band").mean(
        dim="_visible_reflectance_band",
        skipna=True,
    ).fillna(0.0)
    return tuple(band.fillna(fallback) for band in cleaned)


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


def create_b13_night_rgb(b13: xr.DataArray, name: str = "custom_b13_night_rgb") -> xr.DataArray:
    return single_band_to_rgb(b13, "B13", name)


def visible_dark_weight(
    b03: xr.DataArray,
    dark_reflectance: float = HYBRID_NIGHT_DARK_REFLECTANCE,
    day_reflectance: float = HYBRID_NIGHT_DAY_REFLECTANCE,
) -> xr.DataArray:
    if day_reflectance <= dark_reflectance:
        raise ValueError("day_reflectance must be greater than dark_reflectance.")
    reflectance = b03.fillna(0.0)
    weight = (day_reflectance - reflectance) / (day_reflectance - dark_reflectance)
    return xr_clip(weight, 0.0, 1.0)


def rgb_to_unit_float(rgb: xr.DataArray) -> xr.DataArray:
    data = rgb.fillna(0.0)
    if np.issubdtype(data.dtype, np.integer):
        return xr_clip(data.astype(np.float32) / 255.0, 0.0, 1.0)
    return xr_clip(xr.where(data > 1.0, data / 255.0, data), 0.0, 1.0)


def blend_day_night_rgb(
    day_rgb: xr.DataArray,
    night_rgb: xr.DataArray,
    b03: xr.DataArray,
    name: str,
    standard_name: str = "custom_hybrid_day_night_rgb",
) -> xr.DataArray:
    weight = visible_dark_weight(b03)
    day = rgb_to_unit_float(day_rgb)
    night = rgb_to_unit_float(night_rgb)
    blended = day * (1.0 - weight) + night * weight
    blended = (xr_clip(blended, 0.0, 1.0) * 255.0).round().astype(np.uint8)
    if "bands" in day_rgb.coords:
        blended = blended.assign_coords(bands=day_rgb.coords["bands"])
    attrs = day_rgb.attrs.copy()
    attrs.update(
        {
            "name": name,
            "standard_name": standard_name,
            "mode": "RGB",
            "sensor": "ahi",
            "night_fallback_mode": "hybrid",
        }
    )
    attrs["_FillValue"] = np.uint8(0)
    attrs.pop("calibration", None)
    attrs.pop("wavelength", None)
    attrs.pop("units", None)
    blended.attrs = attrs
    return blended


def hybrid_dataset_name(composite_choice: str) -> str:
    return f"hybrid_{safe_filename_component(composite_choice).lower()}_rgb"


def create_hybrid_day_night_rgb(
    day_rgb: xr.DataArray,
    b03: xr.DataArray,
    b13: xr.DataArray,
    composite_choice: str,
) -> xr.DataArray:
    dataset_name = hybrid_dataset_name(composite_choice)
    night_rgb = create_b13_night_rgb(b13, name=f"{dataset_name}_ir")
    return blend_day_night_rgb(day_rgb, night_rgb, b03, name=dataset_name)


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
    source_valid = [np.isfinite(band) & (abs(band) > FLAT_MAP_SOURCE_VALID_MIN) for band in (b01, b02, b03, b04)]
    valid_visible_count = sum(mask.astype(np.uint8) for mask in source_valid)
    b01, b02, b03, b04 = fill_visible_reflectance_gaps(b01, b02, b03, b04)
    red = apply_black_point(scale_reflectance(b03, max_value=100.0, gamma=1.05), black=0.006)
    green = xr_clip(
        apply_black_point(scale_reflectance(b02, max_value=100.0, gamma=1.04), black=0.006) * 0.56
        + red * 0.34
        + apply_black_point(scale_reflectance(b04, max_value=100.0, gamma=1.02), black=0.004) * 0.10,
        0.0,
        1.0,
    )
    blue = apply_black_point(scale_reflectance(b01, max_value=100.0, gamma=1.03), black=0.004)
    red = apply_contrast(xr_clip(red * 1.12 + 0.014, 0.0, 1.0), contrast=1.13, midpoint=0.42)
    green = apply_contrast(xr_clip(green * 1.07 + 0.010, 0.0, 1.0), contrast=1.10, midpoint=0.42)
    blue = apply_contrast(xr_clip(blue * 0.88 + 0.002, 0.0, 1.0), contrast=1.04, midpoint=0.42)
    red, green, blue = apply_saturation(red, green, blue, saturation=1.18)
    red, green, blue = soften_repaired_true_color_channels(red, green, blue, valid_visible_count)
    return rgb_dataarray(
        red,
        green,
        blue,
        name=name or CUSTOM_DATASET_NAMES["True Color Reproduction Image"],
        standard_name=standard_name,
    )


def apply_hybrid_night_if_needed(
    dataset: xr.DataArray,
    scene: Scene,
    composite_choice: str,
    use_night_fallback: bool,
    night_fallback_mode: str,
) -> xr.DataArray:
    if not use_night_fallback or not uses_hybrid_night_fallback(composite_choice, night_fallback_mode):
        return dataset
    if "B03" not in scene or "B13" not in scene:
        raise KeyError("Hybrid day/night fallback requires B03 and B13 to be loaded and resampled.")
    return create_hybrid_day_night_rgb(dataset, scene["B03"], scene["B13"], composite_choice)


def build_custom_composite(
    scene: Scene,
    composite_choice: str,
    is_night: bool,
    config: ProcessorConfig | None = None,
) -> tuple[str, xr.DataArray]:
    config = config or default_config()
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
        dataset = apply_hybrid_night_if_needed(
            dataset,
            scene,
            composite_choice,
            config.use_night_fallback,
            config.night_fallback_mode,
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


def safe_filename_component(value: object) -> str:
    text = re.sub(r"[^A-Za-z0-9._-]+", "_", str(value)).strip("._-")
    return text or "output"


def validate_windows_filename_stem(stem: str, label: str = "Output filename") -> None:
    if not stem.strip(" ."):
        raise ValueError(f"{label} must not be empty.")
    normalized = stem.rstrip(" .").upper()
    if normalized in WINDOWS_RESERVED_DEVICE_NAMES:
        raise ValueError(f"{label} must not use a Windows reserved device name: {stem}")


def first_required_band(composite_choice: str, use_night_fallback: bool = True) -> str:
    bands = required_bands(composite_choice, use_night_fallback=use_night_fallback)
    return bands[0] if bands else "B00"


def validate_output_template(template: str) -> None:
    if not template or not template.strip():
        raise ValueError("Output filename template is required.")
    if any(char in template for char in "/\\"):
        raise ValueError("Output filename template must not contain path separators.")
    formatter = string.Formatter()
    try:
        fields = [field_name for _literal, field_name, _format_spec, _conversion in formatter.parse(template) if field_name]
    except ValueError as exc:
        raise ValueError(f"Output filename template is invalid: {exc}") from exc
    unknown = sorted({field for field in fields if field not in ALLOWED_TEMPLATE_TOKENS})
    if unknown:
        allowed = ", ".join("{" + token + "}" for token in sorted(ALLOWED_TEMPLATE_TOKENS))
        raise ValueError(f"Unknown output template token(s): {', '.join(unknown)}. Allowed: {allowed}.")
    sample_values = {
        "scan_time": "20240725_0400",
        "area": "FLDK",
        "product": "B13_Infrared_Window",
        "mode": "Single_Image",
        "band": "B13",
        "format": "png",
    }
    try:
        rendered = template.format(**sample_values)
    except Exception as exc:
        raise ValueError(f"Output filename template is invalid: {exc}") from exc
    if any(char in rendered for char in WINDOWS_RESERVED_FILENAME_CHARS):
        raise ValueError("Output filename template renders reserved filename characters.")
    rendered_stem = safe_filename_component(rendered)
    validate_windows_filename_stem(rendered_stem, "Output filename template")
    if len(rendered) > 180:
        raise ValueError("Output filename template is too long.")


def output_stem_from_template(
    config: ProcessorConfig,
    info: UrlInfo,
    dt: datetime,
    image_format: str,
    composite_choice: str | None = None,
) -> str:
    validate_output_template(config.output_template)
    product = composite_choice or config.composite_choice
    values = {
        "scan_time": dt.strftime("%Y%m%d_%H%M"),
        "area": safe_filename_component(info.area),
        "product": safe_filename_component(product),
        "mode": safe_filename_component(config.mode),
        "band": safe_filename_component(first_required_band(product, config.use_night_fallback)),
        "format": safe_filename_component(image_format.lower().lstrip(".")),
    }
    rendered = config.output_template.format(**values)
    stem = safe_filename_component(rendered)
    validate_windows_filename_stem(stem)
    if len(stem) > 180:
        raise ValueError("Output filename template renders a name that is too long.")
    return stem


def output_filename(
    info: UrlInfo,
    dt: datetime,
    composite_choice: str,
    mode: str,
    frame_idx: int,
    image_format: str | None = None,
    frame_dir: Path | None = None,
    config: ProcessorConfig | None = None,
) -> Path:
    image_format = image_format or IMAGE_FORMAT
    suffix = ".tif" if image_format.lower() in {"tif", "tiff", "geotiff"} else ".png"
    if mode == "Single Image":
        stem = (
            output_stem_from_template(config, info, dt, image_format, composite_choice)
            if config
            else output_stem(info, dt, composite_choice)
        )
        if config and is_flat_map(config):
            stem = f"{stem}_Flat_Map"
        return OUTPUT_DIR / f"{stem}{suffix}"
    base_dir = frame_dir or OUTPUT_DIR
    return base_dir / f"frame_{frame_idx:04d}{suffix}"


def normalized_map_view(value: str | None) -> str:
    raw_value = MAP_VIEW if value is None else str(value)
    normalized = raw_value.strip().lower().replace("-", "_").replace(" ", "_")
    aliases = {
        "disk": "native",
        "native_disk": "native",
        "geostationary": "native",
        "latlon": "flat",
        "lat_lon": "flat",
        "equirectangular": "flat",
        "flat_map": "flat",
    }
    return aliases.get(normalized, normalized)


def normalized_satellite_layer_mode(value: str | None) -> str:
    raw_value = SATELLITE_LAYER_MODE if value is None else str(value)
    normalized = raw_value.strip().lower().replace("-", "_").replace(" ", "_")
    aliases = {
        "normal": "standard",
        "default": "standard",
        "realtime": "live",
        "real_time": "live",
        "latest": "live",
        "latest_himawari": "live",
        "high_definition": "hd",
        "high_def": "hd",
        "zoom_earth": "hd",
        "zoom_earth_style": "hd",
    }
    return aliases.get(normalized, normalized)


def is_live_satellite_layer(config: ProcessorConfig) -> bool:
    return normalized_satellite_layer_mode(config.satellite_layer_mode) == "live"


def is_hd_satellite_layer(config: ProcessorConfig) -> bool:
    return normalized_satellite_layer_mode(config.satellite_layer_mode) == "hd"


def is_enhanced_satellite_layer(config: ProcessorConfig) -> bool:
    return normalized_satellite_layer_mode(config.satellite_layer_mode) in {"live", "hd"}


def satellite_layer_border_width(value: object) -> float:
    try:
        return min(positive_finite_float(value, "Border line width"), 1.0)
    except ValueError:
        return 1.0


def validated_map_label_size(value: object) -> int:
    numeric = positive_integer(value, "Map label size")
    if numeric < MAP_LABEL_SIZE_MIN or numeric > MAP_LABEL_SIZE_MAX:
        raise ValueError(f"Map label size must be between {MAP_LABEL_SIZE_MIN} and {MAP_LABEL_SIZE_MAX}.")
    return numeric


def satellite_layer_map_label_size(value: object) -> int:
    try:
        return validated_map_label_size(value)
    except ValueError:
        return MAP_LABEL_SIZE


def layer_defaults_config(config: ProcessorConfig) -> ProcessorConfig:
    mode = normalized_satellite_layer_mode(config.satellite_layer_mode)
    if mode == "standard":
        if config.satellite_layer_mode == mode:
            return config
        return replace(config, satellite_layer_mode=mode)
    if mode not in SATELLITE_LAYER_MODES:
        return config
    return replace(
        config,
        satellite_layer_mode=mode,
        composite_choice="True Color Reproduction Image",
        map_view="flat",
        zoom_earth_style=True,
        use_night_fallback=True,
        night_fallback_mode="hybrid",
        add_border_lines=True,
        add_map_labels=True,
        map_label_size=satellite_layer_map_label_size(config.map_label_size),
        add_crosshair=False,
        border_line_color=SATELLITE_LAYER_BORDER_COLOR,
        border_line_width=satellite_layer_border_width(config.border_line_width),
    )


def build_preview_config(config: ProcessorConfig) -> ProcessorConfig:
    """Derive a fast, coarse flat-map "quicklook" config from the user's settings.

    The preview always renders a single coarse flat-map PNG so framing and the
    overall look can be checked in a few seconds before committing to a full-
    resolution (and possibly multi-frame) run. The product and overlay choices
    are kept so the preview resembles the final image; only speed-related fields
    are overridden.
    """
    base = layer_defaults_config(config)
    if normalized_map_view(base.map_view) == "flat":
        min_lat, max_lat = base.flat_min_lat, base.flat_max_lat
        min_lon, max_lon = base.flat_min_lon, base.flat_max_lon
    else:
        min_lat, max_lat, min_lon, max_lon = PREVIEW_FLAT_FALLBACK_BOUNDS
    resolution = max(float(base.flat_resolution_deg), PREVIEW_FLAT_RESOLUTION_DEG)
    return replace(
        base,
        mode="Single Image",
        map_view="flat",
        flat_min_lat=min_lat,
        flat_max_lat=max_lat,
        flat_min_lon=min_lon,
        flat_max_lon=max_lon,
        flat_resolution_deg=resolution,
        image_format="png",
        segment_aware_downloads=True,
        allow_quality_fallback=True,
        write_metadata_sidecar=False,
        gpu_acceleration=False,
    )


def resolve_live_satellite_url(config: ProcessorConfig) -> ProcessorConfig:
    if not is_live_satellite_layer(config):
        return config
    sat_id = sat_id_from_himawari_source(config.user_url)
    LOG.info("Resolving latest %s FLDK scan for live satellite layer.", sat_id)
    resolved_url = find_latest_fldk_url(sat_id=sat_id)
    LOG.info("Live satellite layer resolved to %s", resolved_url)
    return replace(config, user_url=resolved_url)


def runtime_config(config: ProcessorConfig) -> ProcessorConfig:
    return resolve_live_satellite_url(layer_defaults_config(config))


def area_preset_names() -> tuple[str, ...]:
    """Ordered names shown in the GUI area-preset dropdown."""
    return AREA_PRESET_ORDER


def area_preset_bounds(name: str) -> tuple[float, float, float, float] | None:
    """Return (min_lat, max_lat, min_lon, max_lon) for a regional preset, else None."""
    return AREA_PRESETS.get(str(name).strip())


def apply_area_preset_to_config(config: ProcessorConfig, name: str) -> ProcessorConfig:
    """Apply an output-area preset to a config.

    "Full Disk (native)" switches to the native full-disk view. A regional
    preset switches to the flat map and fills the four bounds. "Custom" or any
    unknown name leaves the config unchanged so the user keeps manual control.
    """
    cleaned = str(name).strip()
    if cleaned == AREA_PRESET_FULL_DISK:
        # Native full disk cannot use Zoom Earth flat styling; turn it off so the run
        # is valid. Labels/borders/night boundary/crosshair still render on native.
        return replace(config, map_view="native", zoom_earth_style=False)
    bounds = area_preset_bounds(cleaned)
    if bounds is None:
        return config
    min_lat, max_lat, min_lon, max_lon = bounds
    return replace(
        config,
        map_view="flat",
        flat_min_lat=float(min_lat),
        flat_max_lat=float(max_lat),
        flat_min_lon=float(min_lon),
        flat_max_lon=float(max_lon),
    )


def is_flat_map(config: ProcessorConfig) -> bool:
    return normalized_map_view(config.map_view) == "flat"


def finite_float(value: object, label: str) -> float:
    if isinstance(value, bool):
        raise ValueError(f"{label} must be a finite number.")
    try:
        numeric = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{label} must be a finite number.") from exc
    if not np.isfinite(numeric):
        raise ValueError(f"{label} must be finite.")
    return numeric


def positive_finite_float(value: object, label: str) -> float:
    numeric = finite_float(value, label)
    if numeric <= 0:
        raise ValueError(f"{label} must be positive.")
    return numeric


def positive_integer(value: object, label: str) -> int:
    if isinstance(value, bool):
        raise ValueError(f"{label} must be positive.")
    if isinstance(value, int):
        numeric = value
    else:
        text = str(value).strip()
        if not re.fullmatch(r"[+-]?\d+", text):
            raise ValueError(f"{label} must be positive.")
        numeric = int(text)
    if numeric <= 0:
        raise ValueError(f"{label} must be positive.")
    return numeric


def flat_map_shape(
    min_lat: float,
    max_lat: float,
    min_lon: float,
    max_lon: float,
    resolution_deg: float,
) -> tuple[int, int]:
    resolution_deg = finite_float(resolution_deg, "Flat map resolution")
    if resolution_deg <= 0:
        raise ValueError("Flat map resolution must be positive.")
    left, bottom, right, top = web_mercator_extent(min_lat, max_lat, min_lon, max_lon)
    resolution_m = flat_map_resolution_meters(resolution_deg)
    width = int(round((right - left) / resolution_m))
    height = int(round((top - bottom) / resolution_m))
    return height, width


def flat_map_resolution_meters(resolution_deg: float) -> float:
    return finite_float(resolution_deg, "Flat map resolution") * WEB_MERCATOR_METERS_PER_DEGREE


def web_mercator_extent(min_lat: float, max_lat: float, min_lon: float, max_lon: float) -> tuple[float, float, float, float]:
    min_lat = finite_float(min_lat, "Flat map min latitude")
    max_lat = finite_float(max_lat, "Flat map max latitude")
    min_lon = finite_float(min_lon, "Flat map min longitude")
    max_lon = finite_float(max_lon, "Flat map max longitude")
    from pyproj import Transformer

    transformer = Transformer.from_crs("EPSG:4326", WEB_MERCATOR_PROJ4, always_xy=True)
    left, bottom = transformer.transform(min_lon, min_lat)
    right, top = transformer.transform(max_lon, max_lat)
    return float(left), float(bottom), float(right), float(top)


def checked_flat_map_parameters(config: ProcessorConfig) -> tuple[float, float, float, float, float, int, int]:
    min_lat = finite_float(config.flat_min_lat, "Flat map min latitude")
    max_lat = finite_float(config.flat_max_lat, "Flat map max latitude")
    min_lon = finite_float(config.flat_min_lon, "Flat map min longitude")
    max_lon = finite_float(config.flat_max_lon, "Flat map max longitude")
    resolution_deg = finite_float(config.flat_resolution_deg, "Flat map resolution")
    if resolution_deg <= 0:
        raise ValueError("Flat map resolution must be positive.")
    if min_lat >= max_lat:
        raise ValueError("Flat map min latitude must be less than max latitude.")
    if min_lon >= max_lon:
        raise ValueError("Flat map min longitude must be less than max longitude.")
    if not (-WEB_MERCATOR_MAX_LAT <= min_lat <= WEB_MERCATOR_MAX_LAT and -WEB_MERCATOR_MAX_LAT <= max_lat <= WEB_MERCATOR_MAX_LAT):
        raise ValueError(
            "Flat map latitude bounds must be between "
            f"-{WEB_MERCATOR_MAX_LAT:g} and {WEB_MERCATOR_MAX_LAT:g} for Web Mercator."
        )
    if not (-360.0 <= min_lon <= 360.0 and -360.0 <= max_lon <= 360.0):
        raise ValueError("Flat map longitude bounds must be between -360 and 360.")
    height, width = flat_map_shape(min_lat, max_lat, min_lon, max_lon, resolution_deg)
    if width <= 0 or height <= 0:
        raise ValueError("Flat map bounds/resolution produce an empty output.")
    if width * height > MAX_FLAT_MAP_PIXELS:
        raise ValueError(
            f"Flat map would be {width:,}x{height:,} pixels. "
            f"Increase resolution degrees or shrink bounds to stay under {MAX_FLAT_MAP_PIXELS:,} pixels."
        )
    return min_lat, max_lat, min_lon, max_lon, resolution_deg, height, width


def validate_flat_map_settings(config: ProcessorConfig) -> None:
    if normalized_map_view(config.map_view) not in {"native", "flat"}:
        raise ValueError('Map view must be "native" or "flat".')
    if not is_flat_map(config):
        return
    checked_flat_map_parameters(config)


def flat_map_area(config: ProcessorConfig) -> AreaDefinition:
    min_lat, max_lat, min_lon, max_lon, _resolution_deg, height, width = checked_flat_map_parameters(config)
    extent = web_mercator_extent(min_lat, max_lat, min_lon, max_lon)
    return AreaDefinition(
        "himawari_flat_map",
        "Himawari Web Mercator Flat Map",
        "webmerc",
        WEB_MERCATOR_PROJ4,
        width,
        height,
        extent,
    )


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


def validate_timelapse_frame_size(area: AreaDefinition, config: ProcessorConfig) -> None:
    if config.mode != "Timelapse":
        return
    pixel_count = int(area.width) * int(area.height)
    if pixel_count > MAX_SAFE_TIMELAPSE_FRAME_PIXELS:
        raise RuntimeError(
            f"Timelapse frame target would be {area.width:,}x{area.height:,} pixels "
            f"({pixel_count:,} pixels). GIF/MP4 assembly has to read each frame into memory. "
            "Use Single Image, a smaller Himawari target area, or coarser flat-map settings "
            f"under {MAX_SAFE_TIMELAPSE_FRAME_PIXELS:,} pixels."
        )


def output_behavior_for_config(config: ProcessorConfig, info: UrlInfo, start: datetime) -> str:
    suffix = ".tif" if config.image_format.lower() in {"tif", "tiff", "geotiff"} else ".png"
    if is_flat_map(config):
        area = flat_map_area(config)
        preview_name = output_filename(
            info,
            start,
            config.composite_choice,
            config.mode,
            0,
            config.image_format,
            config=config,
        )
        safe_name = enforce_safe_output_format(preview_name, area, config)
        suffix = ".tif" if writer_for_output(safe_name) == "geotiff" else ".png"
        pixel_count = int(area.width) * int(area.height)
        note = f"flat target {area.width}x{area.height} px, {pixel_count:,} pixels"
    elif (
        info.area == "FLDK"
        and suffix == ".png"
        and target_pixel_size_m(
            config.composite_choice,
            config.use_night_fallback,
            config.night_fallback_mode,
        )
        <= 500
    ):
        suffix = ".tif"
        note = "auto-switch likely for full-disk low-RAM writing"
    else:
        note = "requested format"
    if config.mode == "Timelapse":
        frame_count = len(frame_datetimes(start, config.mode, config.hours_back, config.interval_minutes))
        if frame_count < 2:
            return f"single timelapse frame image ({note}: {suffix}); increase Hours Back or reduce Interval for animation"
        return f"{config.timelapse_format.lower()} animation; frame images use {suffix} ({note})"
    return f"single image ({note}: {suffix})"


def writer_for_output(path: Path) -> str:
    return "geotiff" if path.suffix.lower() in {".tif", ".tiff"} else "simple_image"


def has_module(module_name: str) -> bool:
    return importlib.util.find_spec(module_name) is not None


def overlay_data_required_paths(
    project_dir: Path = PROJECT_DIR,
    resolution: str = OVERLAY_RESOLUTION,
    level: int = OVERLAY_LEVEL,
) -> tuple[Path, ...]:
    overlays_dir = project_dir / "overlays"
    return (
        overlays_dir / "GSHHS_shp" / resolution / f"GSHHS_{resolution}_L{level}.shp",
        overlays_dir / "GSHHS_shp" / resolution / f"GSHHS_{resolution}_L{level}.dbf",
        overlays_dir / "WDBII_shp" / resolution / f"WDBII_border_{resolution}_L{level}.shp",
        overlays_dir / "WDBII_shp" / resolution / f"WDBII_border_{resolution}_L{level}.dbf",
    )


def overlay_required_base_paths(
    project_dir: Path = PROJECT_DIR,
    resolution: str = OVERLAY_RESOLUTION,
    level: int = OVERLAY_LEVEL,
) -> tuple[Path, ...]:
    overlays_dir = project_dir / "overlays"
    return (
        overlays_dir / "GSHHS_shp" / resolution / f"GSHHS_{resolution}_L{level}",
        overlays_dir / "WDBII_shp" / resolution / f"WDBII_border_{resolution}_L{level}",
    )


def overlay_data_layout_text(project_dir: Path = PROJECT_DIR) -> str:
    paths = overlay_data_required_paths(project_dir)
    sidecars = tuple(base.with_suffix(".shx") for base in overlay_required_base_paths(project_dir))
    return "Expected overlay data files:\n" + "\n".join(f"- {path}" for path in (*paths, *sidecars))


def missing_overlay_data_paths(project_dir: Path = PROJECT_DIR) -> tuple[Path, ...]:
    return tuple(path for path in overlay_data_required_paths(project_dir) if not path.exists() or path.stat().st_size <= 0)


def missing_overlay_sidecar_paths(project_dir: Path = PROJECT_DIR) -> tuple[Path, ...]:
    missing = []
    for base in overlay_required_base_paths(project_dir):
        sidecar = base.with_suffix(".shx")
        if not sidecar.exists() or sidecar.stat().st_size <= 0:
            missing.append(sidecar)
    return tuple(missing)


def overlay_status(
    project_dir: Path = PROJECT_DIR,
    module_checker: Callable[[str], bool] = has_module,
) -> OverlayStatus:
    missing_packages = tuple(module for module in ("pycoast", "aggdraw") if not module_checker(module))
    overlays_dir = project_dir / "overlays"
    missing_data: list[str] = []
    details = [
        f"Overlay folder: {overlays_dir}",
        overlay_data_layout_text(project_dir),
    ]
    if not overlays_dir.exists():
        missing_data.append("overlays/ folder not found.")
    missing_paths = missing_overlay_data_paths(project_dir)
    if missing_paths:
        missing_data.append("Missing required pycoast overlay data file(s):")
        missing_data.extend(str(path) for path in missing_paths)
    missing_sidecars = missing_overlay_sidecar_paths(project_dir)
    if missing_sidecars:
        missing_data.append("Missing required shapefile sidecar file(s):")
        missing_data.extend(str(path) for path in missing_sidecars)
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


def ensure_directory_writable(path: Path, label: str) -> None:
    try:
        if path.exists() and not path.is_dir():
            raise RuntimeError(f"{label} folder path exists but is not a folder: {path}")
        path.mkdir(parents=True, exist_ok=True)
        probe = path / f".himawari_write_test_{os.getpid()}.tmp"
        with open(probe, "wb") as handle:
            handle.write(b"ok")
        probe.unlink(missing_ok=True)
    except Exception as exc:
        raise RuntimeError(f"{label} folder is not writable: {path} ({exc.__class__.__name__}: {exc})") from exc


def setup_directory_writability(path: Path, label: str) -> tuple[str | None, str | None]:
    try:
        if path.exists():
            ensure_directory_writable(path, label)
            return f"{label} folder: writable ({path})", None
        parent = path.parent
        while not parent.exists() and parent != parent.parent:
            parent = parent.parent
        if not parent.exists() or not parent.is_dir():
            return None, f"{label} folder parent does not exist and cannot be checked: {path}"
        probe = parent / f".himawari_write_test_{os.getpid()}.tmp"
        with open(probe, "wb") as handle:
            handle.write(b"ok")
        probe.unlink(missing_ok=True)
        return f"{label} folder: parent writable; folder will be created when processing starts ({path})", None
    except Exception as exc:
        return None, f"{label} folder is not writable: {path} ({exc.__class__.__name__}: {exc})"


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


def functional_map_overlay_enabled(config: ProcessorConfig) -> bool:
    return bool(config.add_border_lines or config.add_map_labels or config.add_night_boundary or config.add_crosshair)


def flat_map_visual_style_enabled(config: ProcessorConfig) -> bool:
    return bool(config.zoom_earth_style or functional_map_overlay_enabled(config))


def overlay_theme_colors(theme_name: str | None) -> dict[str, str] | None:
    """Return the {border, label, night_boundary, crosshair} colours for a theme.

    Returns None for the "Custom" theme (or any unknown name), meaning the user's
    hand-picked colours should be left untouched.
    """
    if not theme_name:
        return None
    name = str(theme_name).strip()
    if name == OVERLAY_THEME_CUSTOM:
        return None
    palette = OVERLAY_THEMES.get(name)
    if palette is None:
        return None
    return dict(palette)


def should_style_output(config: ProcessorConfig, product: str) -> bool:
    """Whether the saved image should be post-processed with overlays/styling.

    Flat maps get true-color styling and/or overlays. Native (full-disk) output
    is left as the raw satellite product, but now still receives any requested
    map overlays (labels, crosshair, night boundary, coastlines) so labels are
    no longer flat-map-only.
    """
    if is_flat_map(config):
        return flat_map_visual_style_enabled(config) or true_color_product(product)
    return functional_map_overlay_enabled(config)


def true_color_product(composite_choice: str) -> bool:
    return composite_choice in {"True Color RGB (Enhanced)", "True Color Reproduction Image"}


def should_force_low_ram_flat_true_color(config: ProcessorConfig, active: str) -> bool:
    return is_flat_map(config) and true_color_product(active)


def projected_point_to_pixel(area: AreaDefinition, x: float, y: float) -> tuple[float, float] | None:
    left, bottom, right, top = area.area_extent
    if right == left or top == bottom:
        return None
    px = (float(x) - left) / (right - left) * float(area.width)
    py = (top - float(y)) / (top - bottom) * float(area.height)
    if px < -1 or py < -1 or px > float(area.width) + 1 or py > float(area.height) + 1:
        return None
    return px, py


def lonlat_to_area_pixel(area: AreaDefinition, lon: float, lat: float) -> tuple[float, float] | None:
    from pyproj import Transformer

    transformer = Transformer.from_crs("EPSG:4326", area.crs, always_xy=True)
    x, y = transformer.transform(float(lon), float(lat))
    return projected_point_to_pixel(area, float(x), float(y))


def draw_text_with_halo(
    draw,
    position: tuple[float, float],
    text: str,
    font,
    fill,
    halo=(0, 0, 0, 180),
    halo_width: int = 1,
) -> None:
    x, y = position
    spacing = max(1, halo_width)
    try:
        draw.multiline_text(
            (x, y),
            text,
            font=font,
            fill=fill,
            align="center",
            anchor="mm",
            spacing=spacing,
            stroke_width=halo_width,
            stroke_fill=halo,
        )
        return
    except TypeError:
        pass
    offsets = []
    for dy in range(-halo_width, halo_width + 1):
        for dx in range(-halo_width, halo_width + 1):
            if dx or dy:
                offsets.append((dx, dy))
    for dx, dy in offsets:
        draw.multiline_text((x + dx, y + dy), text, font=font, fill=halo, align="center", anchor="mm", spacing=spacing)
    draw.multiline_text((x, y), text, font=font, fill=fill, align="center", anchor="mm", spacing=spacing)


def map_label_font(size: int):
    from PIL import ImageFont

    font_size = max(MAP_LABEL_SIZE_MIN, min(MAP_LABEL_SIZE_MAX, int(size)))
    font_candidates = (
        "arial.ttf",
        "Arial.ttf",
        "DejaVuSans.ttf",
        "DejaVuSans-Bold.ttf",
        str(Path("C:/Windows/Fonts/arial.ttf")),
        str(Path("C:/Windows/Fonts/segoeui.ttf")),
    )
    for candidate in font_candidates:
        try:
            return ImageFont.truetype(candidate, font_size)
        except Exception:
            continue
    try:
        return ImageFont.load_default(size=font_size)
    except TypeError:
        return ImageFont.load_default()


def draw_zoom_earth_labels(
    image,
    area: AreaDefinition,
    label_size: int = MAP_LABEL_SIZE,
    label_color: tuple[int, int, int] | None = None,
) -> None:
    from PIL import ImageDraw, ImageFont

    draw = ImageDraw.Draw(image, "RGBA")
    try:
        size = validated_map_label_size(label_size)
    except ValueError:
        size = MAP_LABEL_SIZE
    font = map_label_font(size)
    halo_width = max(1, int(round(size / 7.0)))
    for label, lat, lon, kind in ZOOM_EARTH_LABEL_POINTS:
        point = lonlat_to_area_pixel(area, lon, lat)
        if point is None:
            continue
        if label_color is None:
            fill = (235, 238, 242, 235) if kind == "land" else (205, 220, 238, 210)
        else:
            # Keep the land/water opacity difference but use the themed colour.
            fill = (*label_color, 235) if kind == "land" else (*label_color, 210)
        draw_text_with_halo(draw, point, label, font, fill, halo_width=halo_width)


def solar_declination_and_subsolar_lon(scan_time: datetime) -> tuple[float, float]:
    if scan_time.tzinfo is None:
        scan_time = scan_time.replace(tzinfo=UTC)
    utc_time = scan_time.astimezone(UTC)
    day = utc_time.timetuple().tm_yday
    hour = utc_time.hour + utc_time.minute / 60.0 + utc_time.second / 3600.0
    gamma = 2.0 * math.pi / 365.0 * (day - 1 + (hour - 12.0) / 24.0)
    declination = (
        0.006918
        - 0.399912 * math.cos(gamma)
        + 0.070257 * math.sin(gamma)
        - 0.006758 * math.cos(2.0 * gamma)
        + 0.000907 * math.sin(2.0 * gamma)
        - 0.002697 * math.cos(3.0 * gamma)
        + 0.00148 * math.sin(3.0 * gamma)
    )
    equation_minutes = 229.18 * (
        0.000075
        + 0.001868 * math.cos(gamma)
        - 0.032077 * math.sin(gamma)
        - 0.014615 * math.cos(2.0 * gamma)
        - 0.040849 * math.sin(2.0 * gamma)
    )
    subsolar_lon = 180.0 - (hour * 15.0 + equation_minutes / 4.0)
    while subsolar_lon < -180.0:
        subsolar_lon += 360.0
    while subsolar_lon > 180.0:
        subsolar_lon -= 360.0
    return math.degrees(declination), subsolar_lon


def night_boundary_points(scan_time: datetime, step_deg: float = 1.0) -> list[tuple[float, float]]:
    declination_deg, subsolar_lon = solar_declination_and_subsolar_lon(scan_time)
    declination = math.radians(declination_deg)
    points: list[tuple[float, float]] = []
    cos_dec = math.cos(declination)
    if abs(cos_dec) < 1e-6:
        return points
    tan_dec = math.tan(declination)
    count = int(round(360.0 / step_deg)) + 1
    for index in range(count):
        lon = -180.0 + index * step_deg
        hour_angle = math.radians(lon - subsolar_lon)
        lat = math.degrees(math.atan(-math.cos(hour_angle) / tan_dec)) if abs(tan_dec) >= 1e-6 else 0.0
        lat = max(-WEB_MERCATOR_MAX_LAT, min(WEB_MERCATOR_MAX_LAT, lat))
        points.append((lon, lat))
    return points


def visible_polyline_segments(points: Iterable[tuple[float, float] | None], max_jump_px: float) -> list[list[tuple[float, float]]]:
    segments: list[list[tuple[float, float]]] = []
    current: list[tuple[float, float]] = []
    previous: tuple[float, float] | None = None
    for point in points:
        if point is None:
            if len(current) >= 2:
                segments.append(current)
            current = []
            previous = None
            continue
        if previous is not None and math.dist(previous, point) > max_jump_px:
            if len(current) >= 2:
                segments.append(current)
            current = []
        current.append(point)
        previous = point
    if len(current) >= 2:
        segments.append(current)
    return segments


def draw_night_boundary(
    image,
    area: AreaDefinition,
    scan_time: datetime,
    boundary_color: tuple[int, int, int] | None = None,
) -> None:
    from PIL import ImageDraw

    draw = ImageDraw.Draw(image, "RGBA")
    pixels = [lonlat_to_area_pixel(area, lon, lat) for lon, lat in night_boundary_points(scan_time, step_deg=0.5)]
    max_jump_px = max(24.0, min(float(area.width), float(area.height)) * 0.08)
    line_color = (145, 180, 215) if boundary_color is None else boundary_color
    for segment in visible_polyline_segments(pixels, max_jump_px):
        draw.line(segment, fill=(0, 0, 0, 225), width=9)
        draw.line(segment, fill=(255, 255, 255, 235), width=5)
        draw.line(segment, fill=(*line_color, 210), width=2)


def normalized_crosshair_type(value: str | None) -> str:
    normalized = (CROSSHAIR_TYPE if value is None else str(value)).strip().lower().replace("-", "_").replace(" ", "_")
    aliases = {
        "circle": "ring",
        "bullseye": "target",
        "target_dot": "target",
        "cross": "plus",
        "cross_hair": "plus",
        "crosshair": "plus",
        "marker": "dot",
    }
    normalized = aliases.get(normalized, normalized)
    if normalized not in CROSSHAIR_TYPES:
        raise ValueError(f"Crosshair type must be one of: {', '.join(CROSSHAIR_TYPES)}.")
    return normalized


def scaled_alpha(color: tuple[int, int, int] | tuple[int, int, int, int], opacity_scale: float) -> tuple[int, int, int, int]:
    if len(color) == 3:
        base = (*color, 255)
    else:
        base = color
    alpha = max(0, min(255, int(round(base[3] * opacity_scale))))
    return (base[0], base[1], base[2], alpha)


def draw_crosshair(
    image,
    area: AreaDefinition,
    crosshair_type: str = CROSSHAIR_TYPE,
    color: str = CROSSHAIR_COLOR,
    opacity_scale: float = 1.0,
) -> None:
    from PIL import ImageDraw

    draw = ImageDraw.Draw(image, "RGBA")
    x = int(area.width) / 2.0
    y = int(area.height) / 2.0
    radius = max(8, min(int(area.width), int(area.height)) // 120)
    marker = parse_rgb_color(color)
    opacity_scale = max(0.0, min(1.0, float(opacity_scale)))
    marker_fill = scaled_alpha((*marker, 235), opacity_scale)
    halo = scaled_alpha((255, 255, 255, 225), max(0.85, opacity_scale))
    shadow = scaled_alpha((0, 0, 0, 150), opacity_scale)
    marker_type = normalized_crosshair_type(crosshair_type)

    def line(points, fill, width):
        draw.line(points, fill=fill, width=width)

    if marker_type in {"target", "ring"}:
        draw.ellipse((x - radius - 2, y - radius - 2, x + radius + 2, y + radius + 2), outline=shadow, width=5)
        draw.ellipse((x - radius, y - radius, x + radius, y + radius), outline=halo, width=3)
        draw.ellipse((x - radius + 3, y - radius + 3, x + radius - 3, y + radius - 3), outline=marker_fill, width=3)
    if marker_type in {"target", "dot"}:
        dot_radius = max(4, radius // 2)
        draw.ellipse((x - dot_radius - 2, y - dot_radius - 2, x + dot_radius + 2, y + dot_radius + 2), fill=halo)
        draw.ellipse((x - dot_radius, y - dot_radius, x + dot_radius, y + dot_radius), fill=marker_fill)
    if marker_type in {"target", "plus"}:
        length = radius * 2.4
        gap = radius + 4 if marker_type == "target" else max(3, radius // 3)
        for fill, width in ((shadow, 6), (halo, 4), (marker_fill, 2)):
            line((x - length, y, x - gap, y), fill, width)
            line((x + gap, y, x + length, y), fill, width)
            line((x, y - length, x, y - gap), fill, width)
            line((x, y + gap, x, y + length), fill, width)


def lift_true_color_shadows(image, strength: float = 0.55):
    """Highlight-preserving shadow/midtone lift for true-color imagery.

    Himawari True Color Reproduction is genuinely dark over ocean, which made
    flat maps look almost entirely dark blue. This raises shadows and midtones
    (so ocean reads as a brighter, legible blue and faint cloud/land detail
    becomes visible) while leaving near-black and near-white essentially
    untouched, so cloud highlights are not blown out. RGB is scaled by a single
    per-pixel luminance ratio, which preserves hue.
    """
    from PIL import Image

    if strength <= 0:
        return image
    rgb = np.asarray(image.convert("RGB"), dtype=np.float32) / 255.0
    lum = 0.2126 * rgb[:, :, 0] + 0.7152 * rgb[:, :, 1] + 0.0722 * rgb[:, :, 2]
    # Concave boost: zero at black and white, peaks across shadows/midtones.
    boost = float(strength) * np.clip(1.0 - lum, 0.0, 1.0) * np.sqrt(np.clip(lum, 0.0, 1.0))
    target = np.clip(lum + boost, 0.0, 1.0)
    scale = np.ones_like(lum)
    valid = lum > 1.0e-4
    scale[valid] = np.minimum(target[valid] / lum[valid], 3.0)
    lifted = np.clip(rgb * scale[:, :, None], 0.0, 1.0)
    return Image.fromarray((lifted * 255.0 + 0.5).astype(np.uint8), mode="RGB")


def robust_true_color_stretch(image, lower_percentile: float = 0.6, upper_percentile: float = 99.7, blend: float = 0.70):
    from PIL import Image

    original = np.asarray(image.convert("RGB"), dtype=np.float32)
    stretched = original.copy()
    for channel in range(3):
        values = original[:, :, channel]
        low, high = np.percentile(values, (lower_percentile, upper_percentile))
        if not np.isfinite(low) or not np.isfinite(high) or high <= low + 8.0:
            continue
        channel_stretch = np.clip((values - low) * (255.0 / (high - low)), 0.0, 255.0)
        stretched[:, :, channel] = np.maximum(channel_stretch, values)

    luminance = 0.2126 * original[:, :, 0] + 0.7152 * original[:, :, 1] + 0.0722 * original[:, :, 2]
    highlight = np.clip((luminance - 178.0) / 77.0, 0.0, 1.0)
    mix = np.clip(float(blend) * (1.0 - 0.48 * highlight), 0.0, 1.0)
    output = np.clip((original * (1.0 - mix[:, :, None])) + (stretched * mix[:, :, None]), 0.0, 255.0).astype(np.uint8)
    return Image.fromarray(output, mode="RGB")


def compress_true_color_highlights(image, knee: float = 206.0, ceiling: float = 246.0):
    """Soft highlight roll-off so bright clouds keep texture instead of clipping.

    Luminance at or below ``knee`` is untouched; above it a smooth, monotonic
    Reinhard-style shoulder maps the range toward ``ceiling`` (< 255) so the
    brightest cloud tops stop slamming into flat white. RGB is scaled by the
    per-pixel luminance ratio, preserving hue.
    """
    from PIL import Image

    knee = float(max(0.0, min(254.0, knee)))
    ceiling = float(max(knee + 1.0, min(255.0, ceiling)))
    rgb = np.asarray(image.convert("RGB"), dtype=np.float32)
    lum = 0.2126 * rgb[:, :, 0] + 0.7152 * rgb[:, :, 1] + 0.0722 * rgb[:, :, 2]
    span = 255.0 - knee
    over = np.clip((lum - knee) / span, 0.0, 1.0)
    # Reinhard-style shoulder calibrated so lum=255 maps exactly to `ceiling`.
    k = span / max(1.0e-3, (ceiling - knee))
    compressed = knee + span * (over / (1.0 + (k - 1.0) * over))
    target = np.where(lum > knee, compressed, lum)
    scale = np.ones_like(lum)
    valid = lum > 1.0e-4
    scale[valid] = np.minimum(target[valid] / lum[valid], 1.0)
    out = np.clip(rgb * scale[:, :, None], 0.0, 255.0)
    return Image.fromarray((out + 0.5).astype(np.uint8), mode="RGB")


def apply_zoom_earth_true_color_enhancement(image, hd: bool = False):
    from PIL import Image, ImageEnhance

    alpha = image.getchannel("A") if "A" in image.getbands() else None
    original_rgb = np.asarray(image.convert("RGB"), dtype=np.float32)
    working = image.convert("RGB")
    # Lift dark ocean/midtones first so the image is legible. HD also runs a
    # percentile stretch below, so it needs a smaller lift to avoid washing out.
    working = lift_true_color_shadows(working, strength=0.30 if hd else 0.55)
    if hd:
        working = robust_true_color_stretch(working)
    gamma = 0.76 if hd else 0.84
    gamma_lut = [int(round((value / 255.0) ** gamma * 255.0)) for value in range(256)]
    working = working.point(gamma_lut * 3)
    # HD saturation/contrast were too punchy (this is what made live/hd look
    # "weird"); these are dialled back to a natural-but-crisp look.
    working = ImageEnhance.Color(working).enhance(1.30 if hd else 1.20)
    working = ImageEnhance.Contrast(working).enhance(1.10 if hd else 1.06)
    working = ImageEnhance.Brightness(working).enhance(1.06 if hd else 1.05)
    working = ImageEnhance.Sharpness(working).enhance(1.12 if hd else 1.08)
    if hd:
        enhanced = np.asarray(working.convert("RGB"), dtype=np.float32)
        luminance = 0.2126 * original_rgb[:, :, 0] + 0.7152 * original_rgb[:, :, 1] + 0.0722 * original_rgb[:, :, 2]
        protect = np.clip((luminance - 198.0) / 57.0, 0.0, 0.38)
        protected = np.clip((enhanced * (1.0 - protect[:, :, None])) + (original_rgb * protect[:, :, None]), 0, 255)
        working = Image.fromarray(protected.astype(np.uint8), mode="RGB")
    balanced = np.asarray(working.convert("RGB"), dtype=np.float32)
    luminance = 0.2126 * balanced[:, :, 0] + 0.7152 * balanced[:, :, 1] + 0.0722 * balanced[:, :, 2]
    maximum = balanced.max(axis=2)
    minimum = balanced.min(axis=2)
    chroma = maximum - minimum
    cloud_protect = np.clip((luminance - 196.0) / 58.0, 0.0, 0.55) * np.clip(1.0 - chroma / 92.0, 0.0, 1.0)
    warm_mix = 1.0 - cloud_protect
    warm = balanced.copy()
    warm[:, :, 0] *= 1.07 if hd else 1.05
    warm[:, :, 1] *= 1.03 if hd else 1.02
    warm[:, :, 2] *= 0.88 if hd else 0.91
    blue_excess = np.maximum(0.0, warm[:, :, 2] - np.maximum(warm[:, :, 0], warm[:, :, 1]))
    warm[:, :, 2] -= blue_excess * (0.30 if hd else 0.24)
    balanced = np.clip((balanced * (1.0 - warm_mix[:, :, None])) + (warm * warm_mix[:, :, None]), 0, 255)
    working = Image.fromarray(balanced.astype(np.uint8), mode="RGB")
    working = ImageEnhance.Color(working).enhance(1.12 if hd else 1.22)
    if alpha is not None:
        working.putalpha(alpha)
    return working


def chroma_speckle_candidate_mask(rgb: np.ndarray) -> np.ndarray:
    values = np.asarray(rgb, dtype=np.int16)
    red = values[:, :, 0]
    green = values[:, :, 1]
    blue = values[:, :, 2]
    high_red = (red > 145) & (red > green + 55) & (red > blue + 55)
    high_green = (green > 145) & (green > red + 55) & (green > blue + 55)
    high_magenta = (red > 130) & (blue > 120) & (np.minimum(red, blue) > green + 55)
    return high_red | high_green | high_magenta


def impossible_true_color_chroma_mask(rgb: np.ndarray) -> np.ndarray:
    values = np.asarray(rgb, dtype=np.int16)
    red = values[:, :, 0]
    green = values[:, :, 1]
    blue = values[:, :, 2]
    impossible_red = (red >= 245) & (green <= 45) & (blue <= 45)
    impossible_green = (green >= 245) & (red <= 45) & (blue <= 45)
    impossible_magenta = (red >= 220) & (blue >= 220) & (green <= 65)
    return impossible_red | impossible_green | impossible_magenta


def aggressive_true_color_artifact_mask(rgb: np.ndarray) -> np.ndarray:
    values = np.asarray(rgb, dtype=np.int16)
    red = values[:, :, 0]
    green = values[:, :, 1]
    blue = values[:, :, 2]
    luminance = 0.2126 * red + 0.7152 * green + 0.0722 * blue
    maximum = values.max(axis=2)
    minimum = values.min(axis=2)
    chroma = maximum - minimum

    saturated_red = (red > 118) & (red > green + 38) & (red > blue + 28) & (chroma > 48)
    dark_red_fleck = (red > 72) & (red > green + 42) & (red > blue + 34) & (green < 76) & (blue < 92)
    magenta_fleck = (red > 112) & (blue > 82) & (red > green + 34) & (blue > green + 22) & (chroma > 46)
    # Vivid green specks are a true-color-reproduction artifact (real vegetation is
    # a muted olive where red is not far below green). Require green to clearly
    # dominate both other channels with high chroma so genuine land is spared.
    saturated_green = (green > 116) & (green > red + 40) & (green > blue + 38) & (chroma > 50)
    dark_green_fleck = (green > 70) & (green > red + 44) & (green > blue + 38) & (red < 96) & (blue < 104)

    natural_brown = (
        (red > green)
        & (green > blue)
        & (green >= (red * 0.42))
        & (blue >= (red * 0.18))
        & (luminance > 45.0)
        & (chroma < 125)
    )
    natural_warm_cloud = (luminance > 188.0) & (green > 115) & (blue > 92) & (chroma < 92)
    # Muted vegetation: green leads but red is substantial (not a vivid artifact green).
    natural_vegetation = (
        (green > red)
        & (green > blue)
        & (red >= (green * 0.52))
        & (luminance > 38.0)
        & (chroma < 118)
    )
    artifacts = saturated_red | dark_red_fleck | magenta_fleck | saturated_green | dark_green_fleck
    return artifacts & ~natural_brown & ~natural_warm_cloud & ~natural_vegetation


def cleanup_true_color_chroma_speckles(
    image,
    max_component_pixels: int = 24,
    aggressive: bool = False,
    aggressive_component_pixels: int = 192,
) -> None:
    from PIL import Image

    rgb = np.asarray(image.convert("RGB"), dtype=np.uint8)
    chroma = chroma_speckle_candidate_mask(rgb)
    impossible = impossible_true_color_chroma_mask(rgb)
    aggressive_candidates = aggressive_true_color_artifact_mask(rgb) if aggressive else np.zeros(chroma.shape, dtype=bool)
    if not bool(chroma.any() or impossible.any() or aggressive_candidates.any()):
        return
    try:
        from scipy import ndimage
    except Exception:
        return

    median = np.empty_like(rgb)
    for channel in range(3):
        median[:, :, channel] = ndimage.median_filter(rgb[:, :, channel], size=3, mode="nearest")
    local_chroma = chroma_speckle_candidate_mask(median)
    local_impossible = impossible_true_color_chroma_mask(median)
    isolated = chroma & ~local_chroma
    if bool(isolated.any()):
        labels, label_count = ndimage.label(isolated)
        if label_count:
            sizes = np.bincount(labels.ravel())
            removable = sizes <= max_component_pixels
            removable[0] = False
            isolated = removable[labels]
    aggressive_clusters = np.zeros(chroma.shape, dtype=bool)
    if bool(aggressive_candidates.any()):
        labels, label_count = ndimage.label(aggressive_candidates)
        if label_count:
            sizes = np.bincount(labels.ravel())
            removable = sizes <= max(1, int(aggressive_component_pixels))
            removable[0] = False
            aggressive_clusters = removable[labels]
    replace_mask = isolated | impossible | aggressive_clusters
    if not bool(replace_mask.any()):
        return

    cleaned = rgb.copy()
    if bool((~replace_mask).any()):
        nearest_y, nearest_x = ndimage.distance_transform_edt(replace_mask, return_distances=False, return_indices=True)
        cleaned[replace_mask] = cleaned[nearest_y[replace_mask], nearest_x[replace_mask]]
    else:
        cleaned[replace_mask] = median[replace_mask]
    if "A" in image.getbands():
        output = Image.fromarray(cleaned, mode="RGB").convert("RGBA")
        output.putalpha(image.getchannel("A"))
    else:
        output = Image.fromarray(cleaned, mode="RGB").convert(image.mode)
    image.paste(output)


def finish_zoom_earth_true_color_quality(image, hd: bool = False):
    from PIL import ImageEnhance

    alpha = image.getchannel("A") if "A" in image.getbands() else None
    working = image.convert("RGB")
    working = ImageEnhance.Contrast(working).enhance(1.045 if hd else 1.035)
    working = ImageEnhance.Color(working).enhance(1.055 if hd else 1.04)
    working = ImageEnhance.Sharpness(working).enhance(1.14 if hd else 1.10)
    # Final highlight roll-off: pull the brightest cloud tops just below pure
    # white so they read as bright clouds with shape instead of blown-out white.
    working = compress_true_color_highlights(working, knee=204.0, ceiling=240.0)
    if alpha is not None:
        working = working.convert("RGBA")
        working.putalpha(alpha)
    return working


def apply_faithful_true_color_enhancement(image):
    """Gentle, faithful tone curve for standard (non-cosmetic) true-color flat maps.

    The Zoom Earth / HD enhancement (``apply_zoom_earth_true_color_enhancement``)
    is a deliberately punchy, stylised look: it warms clouds, lifts ocean toward a
    vivid blue and boosts saturation. On a plain flat-map true-color export that
    styling made the picture look washed out and yellow-tinted compared with the
    native True Color Reproduction image (clouds went cream, ocean went muddy
    teal). This routine instead applies only a light, hue-preserving correction so
    a standard flat map closely matches the native look:

      * a small shadow lift so deep ocean is legible without going bright,
      * very mild contrast/saturation/brightness/sharpness for a clean image,
      * a gentle highlight roll-off so the brightest cloud tops keep texture.

    Clouds stay neutral white and ocean stays a deep, natural blue. Alpha is
    preserved so the off-disk / out-of-bounds transparency is untouched.
    """
    from PIL import ImageEnhance

    alpha = image.getchannel("A") if "A" in image.getbands() else None
    working = image.convert("RGB")
    working = lift_true_color_shadows(working, strength=0.22)
    working = ImageEnhance.Contrast(working).enhance(1.05)
    working = ImageEnhance.Color(working).enhance(1.07)
    working = ImageEnhance.Brightness(working).enhance(1.02)
    working = ImageEnhance.Sharpness(working).enhance(1.06)
    working = compress_true_color_highlights(working, knee=210.0, ceiling=246.0)
    if alpha is not None:
        working = working.convert("RGBA")
        working.putalpha(alpha)
    return working


def build_overlay_options(config: ProcessorConfig) -> dict | None:
    if not config.add_border_lines:
        return None
    color = parse_rgb_color(config.border_line_color)
    return {
        "coast_dir": str(PROJECT_DIR / "overlays"),
        "color": color,
        "width": config.border_line_width,
        "level_coast": OVERLAY_LEVEL,
        "level_borders": OVERLAY_LEVEL,
        "resolution": OVERLAY_RESOLUTION,
    }


def direct_overlay_writer(coast_dir: str):
    from pycoast import ContourWriterAGG

    return ContourWriterAGG(coast_dir)


def apply_direct_overlay_to_image_file(output_path: Path, area: AreaDefinition, overlay: dict | None) -> Path:
    if overlay is None:
        return output_path
    if output_path.suffix.lower() not in {".png", ".jpg", ".jpeg"}:
        LOG.warning(
            "Border overlay is not applied to direct %s output because preserving georeferencing is prioritized.",
            output_path.suffix or "image",
        )
        return output_path
    try:
        from PIL import Image

        writer = direct_overlay_writer(str(overlay["coast_dir"]))
        tmp_path = temporary_output_path(output_path)
        try:
            tmp_path.unlink(missing_ok=True)
            with Image.open(output_path) as image:
                original_mode = image.mode
                working = image.convert("RGBA")
                color = tuple(overlay["color"])
                width = float(overlay["width"])
                resolution = str(overlay["resolution"])
                writer.add_coastlines(
                    working,
                    area,
                    resolution=resolution,
                    level=int(overlay["level_coast"]),
                    outline=color,
                    width=width,
                )
                writer.add_borders(
                    working,
                    area,
                    resolution=resolution,
                    level=int(overlay["level_borders"]),
                    outline=color,
                    width=width,
                )
                if original_mode != "RGBA":
                    working = working.convert(original_mode)
                working.save(tmp_path)
            tmp_path.replace(output_path)
        except Exception:
            try:
                tmp_path.unlink(missing_ok=True)
            except Exception:
                pass
            raise
    except Exception as exc:
        LOG.warning(
            "Border overlay failed for direct RGB output (%s). Saving image without border lines.",
            exc,
        )
    return output_path


def apply_alpha_scale_and_mask(layer, opacity: int, valid_mask: np.ndarray | None) -> None:
    if opacity >= 255 and valid_mask is None:
        return
    from PIL import Image

    alpha = np.asarray(layer.getchannel("A"), dtype=np.uint16).copy()
    if opacity < 255:
        alpha = (alpha * max(0, min(255, int(opacity))) + 127) // 255
    mask = normalize_validity_mask(valid_mask, layer.width, layer.height)
    if mask is not None:
        alpha[~mask] = 0
    layer.putalpha(Image.fromarray(alpha.astype(np.uint8), mode="L"))


def direct_overlay_to_image(
    image,
    area: AreaDefinition,
    overlay: dict | None,
    opacity: int = 255,
    valid_mask: np.ndarray | None = None,
) -> None:
    if overlay is None:
        return
    from PIL import Image

    writer = direct_overlay_writer(str(overlay["coast_dir"]))
    color = tuple(overlay["color"])
    width = float(overlay["width"])
    resolution = str(overlay["resolution"])
    target = image
    if opacity < 255 or valid_mask is not None:
        target = Image.new("RGBA", image.size, (0, 0, 0, 0))
    writer.add_coastlines(
        target,
        area,
        resolution=resolution,
        level=int(overlay["level_coast"]),
        outline=color,
        width=width,
    )
    writer.add_borders(
        target,
        area,
        resolution=resolution,
        level=int(overlay["level_borders"]),
        outline=color,
        width=width,
    )
    if target is image:
        return
    apply_alpha_scale_and_mask(target, opacity, valid_mask)
    if image.mode != "RGBA":
        working = image.convert("RGBA")
        working.alpha_composite(target)
        image.paste(working.convert(image.mode))
    else:
        image.alpha_composite(target)


def normalize_validity_mask(mask: np.ndarray | None, width: int, height: int) -> np.ndarray | None:
    if mask is None:
        return None
    array = np.asarray(mask)
    if array.shape != (height, width):
        raise ValueError(f"Validity mask shape {array.shape} does not match image shape {(height, width)}.")
    return array.astype(bool, copy=False)


def image_alpha_validity_mask(image) -> np.ndarray | None:
    if "A" not in image.getbands():
        return None
    return np.asarray(image.getchannel("A")) > 0


def geotiff_validity_mask_from_dataset(dataset) -> np.ndarray | None:
    try:
        import rasterio

        for band_index, color_interp in zip(dataset.indexes, dataset.colorinterp, strict=False):
            if color_interp == rasterio.enums.ColorInterp.alpha:
                alpha_valid = np.asarray(dataset.read(band_index)) > 0
                if alpha_valid.size and not bool(alpha_valid.all()):
                    return alpha_valid
    except Exception:
        pass
    try:
        if int(dataset.count) >= 4:
            alpha = np.asarray(dataset.read(4))
            unique = np.unique(alpha.astype(np.uint8, copy=False))
            unique_values = {int(value) for value in unique.tolist()}
            looks_like_alpha = (
                unique.size <= 4
                and int(alpha.min()) >= 0
                and int(alpha.max()) <= 255
                and 0 in unique_values
                and 255 in unique_values
            )
            if looks_like_alpha:
                alpha_valid = alpha > 0
                if alpha_valid.size and not bool(alpha_valid.all()):
                    return alpha_valid
    except Exception:
        pass
    try:
        mask = dataset.dataset_mask()
    except Exception:
        return None
    if mask is None:
        return None
    valid = np.asarray(mask) > 0
    if valid.size == 0 or bool(valid.all()):
        return None
    return valid


def fill_invalid_flat_map_pixels(image, valid_mask: np.ndarray | None) -> None:
    mask = normalize_validity_mask(valid_mask, image.width, image.height)
    if mask is None:
        return
    from PIL import Image

    rgb = np.asarray(image.convert("RGB")).copy()
    rgb[~mask] = np.asarray(FLAT_MAP_INVALID_FILL, dtype=np.uint8)
    image.paste(Image.fromarray(rgb, mode="RGB").convert(image.mode))


def flat_map_pixel_center_vectors(area: AreaDefinition) -> tuple[np.ndarray, np.ndarray]:
    left, bottom, right, top = area.area_extent
    width = int(area.width)
    height = int(area.height)
    x_step = (float(right) - float(left)) / max(width, 1)
    y_step = (float(top) - float(bottom)) / max(height, 1)
    xs = float(left) + (np.arange(width, dtype=np.float64) + 0.5) * x_step
    ys = float(top) - (np.arange(height, dtype=np.float64) + 0.5) * y_step
    return xs, ys


def flat_map_lonlat_vectors(area: AreaDefinition) -> tuple[np.ndarray, np.ndarray]:
    from pyproj import Transformer

    xs, ys = flat_map_pixel_center_vectors(area)
    left, bottom, right, top = area.area_extent
    transformer = Transformer.from_crs(area.crs, "EPSG:4326", always_xy=True)
    lon, _lat_sample = transformer.transform(xs, np.full(xs.shape, (float(bottom) + float(top)) * 0.5))
    _lon_sample, lat = transformer.transform(np.full(ys.shape, (float(left) + float(right)) * 0.5), ys)
    return np.asarray(lon, dtype=np.float32), np.asarray(lat, dtype=np.float32)


def generated_flat_map_ocean_image(area: AreaDefinition):
    from PIL import Image

    width = int(area.width)
    height = int(area.height)
    lon, lat = flat_map_lonlat_vectors(area)
    lat_norm = (1.0 - np.clip(np.abs(lat) / WEB_MERCATOR_MAX_LAT, 0.0, 1.0))[:, None]
    y_grad = np.linspace(0.0, 1.0, height, dtype=np.float32)[:, None]
    lon_wave = ((np.sin(np.deg2rad(lon * 1.7)) + 1.0) * 0.5)[None, :]
    rgb = np.empty((height, width, 3), dtype=np.uint8)
    rgb[:, :, 0] = np.clip(FLAT_MAP_BASEMAP_OCEAN[0] + 5.0 * lat_norm + 2.0 * lon_wave, 0, 255).astype(np.uint8)
    rgb[:, :, 1] = np.clip(FLAT_MAP_BASEMAP_OCEAN[1] + 18.0 * lat_norm + 5.0 * lon_wave, 0, 255).astype(np.uint8)
    rgb[:, :, 2] = np.clip(
        FLAT_MAP_BASEMAP_OCEAN[2] + 34.0 * lat_norm + 9.0 * lon_wave + 8.0 * y_grad,
        0,
        255,
    ).astype(np.uint8)
    return Image.fromarray(rgb, mode="RGB").convert("RGBA")


def draw_flat_map_basemap_land(image, area: AreaDefinition) -> None:
    try:
        writer = direct_overlay_writer(str(PROJECT_DIR / "overlays"))
        writer.add_coastlines(
            image,
            area,
            resolution=OVERLAY_RESOLUTION,
            level=OVERLAY_LEVEL,
            fill=FLAT_MAP_BASEMAP_LAND,
            fill_opacity=185,
            outline=FLAT_MAP_BASEMAP_COAST,
            width=0.65,
            outline_opacity=85,
        )
    except Exception as exc:
        LOG.debug("Flat-map generated basemap land layer skipped (%s).", exc)


def build_flat_map_basemap_image(area: AreaDefinition):
    image = generated_flat_map_ocean_image(area)
    draw_flat_map_basemap_land(image, area)
    return image


def edge_connected_mask(candidate: np.ndarray) -> np.ndarray | None:
    mask = np.asarray(candidate, dtype=bool)
    if mask.ndim != 2 or not bool(mask.any()):
        return None
    seed = np.zeros(mask.shape, dtype=bool)
    seed[0, :] = mask[0, :]
    seed[-1, :] = mask[-1, :]
    seed[:, 0] |= mask[:, 0]
    seed[:, -1] |= mask[:, -1]
    if not bool(seed.any()):
        return None
    try:
        from scipy import ndimage

        connected = ndimage.binary_propagation(seed, mask=mask)
    except Exception:
        from collections import deque

        connected = np.zeros(mask.shape, dtype=bool)
        queue: deque[tuple[int, int]] = deque()
        for y, x in np.argwhere(seed):
            connected[int(y), int(x)] = True
            queue.append((int(y), int(x)))
        height, width = mask.shape
        while queue:
            y, x = queue.popleft()
            for yy, xx in ((y - 1, x), (y + 1, x), (y, x - 1), (y, x + 1)):
                if 0 <= yy < height and 0 <= xx < width and mask[yy, xx] and not connected[yy, xx]:
                    connected[yy, xx] = True
                    queue.append((yy, xx))
    if not bool(connected.any()):
        return None
    return np.asarray(connected, dtype=bool)


def flat_map_edge_artifact_mask(image) -> np.ndarray | None:
    rgb = np.asarray(image.convert("RGB"), dtype=np.int16)
    height, width = rgb.shape[:2]
    luminance = 0.2126 * rgb[:, :, 0] + 0.7152 * rgb[:, :, 1] + 0.0722 * rgb[:, :, 2]
    maximum = rgb.max(axis=2)
    minimum = rgb.min(axis=2)
    chroma = maximum - minimum
    red = rgb[:, :, 0]
    green = rgb[:, :, 1]
    blue = rgb[:, :, 2]
    y_index, x_index = np.indices((height, width))
    edge_distance = np.minimum.reduce((y_index, x_index, height - 1 - y_index, width - 1 - x_index))
    near_edge = edge_distance <= max(4, int(round(min(width, height) * 0.22)))
    dark_fill = (luminance < 30.0) & (maximum < 68)
    blue_limb = (blue > red + 60) & (blue > green + 45) & (chroma > 70) & (luminance > 42.0)
    purple_limb = (red > green + 52) & (blue > green + 52) & (chroma > 70) & (luminance > 42.0)
    large_enough_for_limb_cleanup = min(width, height) >= 32
    washed_limb = large_enough_for_limb_cleanup & (luminance > 132.0) & (chroma < 78) & near_edge
    gray_limb = large_enough_for_limb_cleanup & (luminance > 92.0) & (luminance < 210.0) & (chroma < 36) & near_edge
    seed = edge_connected_mask(dark_fill | blue_limb | purple_limb | washed_limb | gray_limb)
    if seed is None:
        return None
    connected = np.zeros(dark_fill.shape, dtype=bool)
    candidate = blue_limb | purple_limb | washed_limb | gray_limb
    try:
        from scipy import ndimage

        connected |= ndimage.binary_propagation(seed, mask=dark_fill | candidate)
    except Exception:
        fringe_connected = edge_connected_mask(dark_fill | candidate)
        if fringe_connected is not None:
            connected |= fringe_connected
    return np.asarray(connected, dtype=bool) if connected is not None and bool(connected.any()) else None


def flat_map_limb_seed_mask(image, valid_mask: np.ndarray | None) -> np.ndarray | None:
    mask = normalize_validity_mask(valid_mask, image.width, image.height)
    seed = np.zeros((image.height, image.width), dtype=bool)
    edge_mask = flat_map_edge_artifact_mask(image)
    if edge_mask is not None:
        seed |= edge_mask
    if mask is not None:
        edge_invalid = edge_connected_mask(~mask) if not bool(mask.all()) else None
        if edge_invalid is not None:
            seed |= edge_invalid
        if not bool(seed.any()):
            return None
        return seed
    if not bool(seed.any()):
        return None
    return seed


def flat_map_limb_fade_width(width: int, height: int) -> int:
    scaled = int(round(min(width, height) * FLAT_MAP_LIMB_FADE_FRACTION))
    return max(1, min(FLAT_MAP_LIMB_FADE_MAX_PX, max(FLAT_MAP_LIMB_FADE_MIN_PX, scaled)))


def flat_map_basemap_blend_alpha(image, valid_mask: np.ndarray | None) -> np.ndarray | None:
    seed = flat_map_limb_seed_mask(image, valid_mask)
    if seed is None:
        return None
    fade_width = flat_map_limb_fade_width(image.width, image.height)
    try:
        from scipy import ndimage

        distance_from_seed = ndimage.distance_transform_edt(~seed)
    except Exception:
        distance_from_seed = np.full(seed.shape, fade_width + 1.0, dtype=np.float32)
        current = seed.copy()
        visited = seed.copy()
        for step in range(1, fade_width + 1):
            expanded = current.copy()
            expanded[1:, :] |= current[:-1, :]
            expanded[:-1, :] |= current[1:, :]
            expanded[:, 1:] |= current[:, :-1]
            expanded[:, :-1] |= current[:, 1:]
            ring = expanded & ~visited
            distance_from_seed[ring] = float(step)
            visited |= ring
            current = expanded

    alpha = np.clip(1.0 - (distance_from_seed / float(max(fade_width, 1))), 0.0, 1.0).astype(np.float32)
    alpha[seed] = 1.0
    if not bool((alpha > 0.0).any()):
        return None
    return alpha


def composite_flat_map_basemap(image, area: AreaDefinition, valid_mask: np.ndarray | None) -> None:
    from PIL import Image

    alpha = flat_map_basemap_blend_alpha(image, valid_mask)
    if alpha is None:
        return
    rgb = np.asarray(image.convert("RGB"), dtype=np.float32)
    background = np.asarray(build_flat_map_basemap_image(area).convert("RGB"), dtype=np.float32)
    blend = alpha[:, :, None]
    styled = np.clip((rgb * (1.0 - blend)) + (background * blend), 0, 255).astype(np.uint8)
    image.paste(Image.fromarray(styled, mode="RGB").convert(image.mode))


def apply_flat_map_style_to_image(
    image,
    area: AreaDefinition,
    config: ProcessorConfig,
    scan_time: datetime,
    product: str,
    overlay_options: dict | None = None,
    valid_mask: np.ndarray | None = None,
    native: bool = False,
):
    working = image.convert("RGBA")
    is_true_color = true_color_product(product)
    # Zoom Earth styling (synthetic basemap, translucent overlays) is a flat-map
    # concept only; native output is the raw product with overlays drawn on top.
    is_zoom_true_color = (not native) and config.zoom_earth_style and is_true_color
    if not native:
        fill_invalid_flat_map_pixels(working, valid_mask)
        if is_true_color:
            # Red/magenta artifact cleanup runs for every true-color flat map, then
            # the tone curve is chosen by mode. Both run before any overlay so green
            # borders, labels and the crosshair are drawn on top of the cleaned image
            # and are never removed by the speckle cleanup.
            #
            # Cosmetic modes (Zoom Earth style, or the enhanced "live"/"hd" satellite
            # layers) get the punchy stylised look. A plain standard flat map instead
            # gets a gentle, faithful correction so it matches the native True Color
            # Reproduction image (deep-blue ocean, neutral white clouds) rather than
            # the old washed-out, yellow-tinted result.
            hd = is_enhanced_satellite_layer(config)
            cosmetic_true_color = config.zoom_earth_style or hd
            if cosmetic_true_color:
                working = apply_zoom_earth_true_color_enhancement(working, hd=hd)
                cleanup_true_color_chroma_speckles(working, aggressive=True)
                working = finish_zoom_earth_true_color_quality(working, hd=hd)
            else:
                working = apply_faithful_true_color_enhancement(working)
                cleanup_true_color_chroma_speckles(working, aggressive=True)
        if is_zoom_true_color:
            composite_flat_map_basemap(working, area, valid_mask)
    try:
        direct_overlay_to_image(
            working,
            area,
            overlay_options,
            opacity=FLAT_MAP_ZOOM_OVERLAY_OPACITY if is_zoom_true_color else 255,
            valid_mask=valid_mask if is_zoom_true_color else None,
        )
    except Exception as exc:
        LOG.warning("Map border overlay failed (%s). Continuing without border lines.", exc)
    if config.add_night_boundary:
        try:
            night_color = parse_rgb_color(getattr(config, "night_boundary_color", NIGHT_BOUNDARY_COLOR))
        except Exception:
            night_color = None
        draw_night_boundary(working, area, scan_time, boundary_color=night_color)
    if config.add_map_labels:
        try:
            label_color = parse_rgb_color(getattr(config, "map_label_color", MAP_LABEL_COLOR))
        except Exception:
            label_color = None
        draw_zoom_earth_labels(working, area, config.map_label_size, label_color=label_color)
    if config.add_crosshair:
        draw_crosshair(
            working,
            area,
            config.crosshair_type,
            config.crosshair_color,
            opacity_scale=FLAT_MAP_ZOOM_CROSSHAIR_OPACITY_SCALE if is_zoom_true_color else 1.0,
        )
    return working


def style_flat_map_raster_file(
    output_path: Path,
    area: AreaDefinition,
    config: ProcessorConfig,
    scan_time: datetime,
    product: str,
    overlay_options: dict | None = None,
    valid_mask: np.ndarray | None = None,
    native: bool = False,
) -> Path:
    suffix = output_path.suffix.lower()
    if suffix in {".png", ".jpg", ".jpeg"}:
        from PIL import Image

        tmp_path = temporary_output_path(output_path)
        try:
            tmp_path.unlink(missing_ok=True)
            with Image.open(output_path) as image:
                original_mode = image.mode
                image_valid_mask = valid_mask if valid_mask is not None else image_alpha_validity_mask(image)
                working = apply_flat_map_style_to_image(
                    image,
                    area,
                    config,
                    scan_time,
                    product,
                    overlay_options=overlay_options,
                    valid_mask=image_valid_mask,
                    native=native,
                )
                if original_mode != "RGBA":
                    working = working.convert(original_mode)
                working.save(tmp_path)
            tmp_path.replace(output_path)
        except Exception:
            try:
                tmp_path.unlink(missing_ok=True)
            except Exception:
                pass
            raise
        return output_path

    if suffix in {".tif", ".tiff"}:
        require_module("rasterio", "styled flat-map GeoTIFF output")
        import rasterio
        from PIL import Image

        tmp_path = temporary_output_path(output_path)
        try:
            tmp_path.unlink(missing_ok=True)
            with rasterio.open(output_path) as src:
                profile = src.profile.copy()
                rgb = src.read((1, 2, 3))
                geotiff_valid_mask = valid_mask if valid_mask is not None else geotiff_validity_mask_from_dataset(src)
                rgb = np.moveaxis(rgb, 0, -1).astype(np.uint8, copy=False)
                working = apply_flat_map_style_to_image(
                    Image.fromarray(rgb, mode="RGB"),
                    area,
                    config,
                    scan_time,
                    product,
                    overlay_options=overlay_options,
                    valid_mask=geotiff_valid_mask,
                    native=native,
                ).convert("RGB")
                styled = np.moveaxis(np.asarray(working, dtype=np.uint8), -1, 0)
            profile.update(
                count=3,
                dtype="uint8",
                nodata=None,
                photometric="RGB",
                compress=profile.get("compress", "deflate"),
            )
            with rasterio.open(tmp_path, "w", **profile) as dst:
                dst.write(styled)
                dst.colorinterp = (
                    rasterio.enums.ColorInterp.red,
                    rasterio.enums.ColorInterp.green,
                    rasterio.enums.ColorInterp.blue,
                )
            tmp_path.replace(output_path)
        except Exception:
            try:
                tmp_path.unlink(missing_ok=True)
            except Exception:
                pass
            raise
        return output_path

    return output_path


def apply_flat_map_visual_overlays(
    output_path: Path,
    area: AreaDefinition,
    config: ProcessorConfig,
    scan_time: datetime,
    product: str,
    overlay_options: dict | None = None,
    valid_mask: np.ndarray | None = None,
) -> Path:
    if not should_style_output(config, product):
        return output_path
    if output_path.suffix.lower() not in {".png", ".jpg", ".jpeg", ".tif", ".tiff"}:
        return output_path

    native = not is_flat_map(config)
    try:
        return style_flat_map_raster_file(
            output_path,
            area,
            config,
            scan_time,
            product,
            # Borders/labels/night boundary/crosshair are all drawn here for both
            # flat and native output; the dataset writer is told not to draw the
            # coastline overlay (see save_satpy_dataset_output) so it is not doubled.
            overlay_options=overlay_options,
            valid_mask=valid_mask,
            native=native,
        )
    except Exception as exc:
        LOG.warning("Map overlay styling failed (%s). Keeping unstyled image.", exc)
    return output_path


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


def temporary_output_path(output_path: Path) -> Path:
    suffix = output_path.suffix or ".tmp"
    return output_path.with_name(f".{output_path.stem}.{os.getpid()}.{threading.get_ident()}.part{suffix}")


def stable_run_id(config: ProcessorConfig, info: UrlInfo, steps: list[datetime]) -> str:
    payload = {
        "schema": TIMELAPSE_MANIFEST_SCHEMA_VERSION,
        "app_version": APP_VERSION,
        "url": config.user_url,
        "mode": config.mode,
        "composite": config.composite_choice,
        "image_format": config.image_format,
        "timelapse_format": config.timelapse_format,
        "night_fallback": config.use_night_fallback,
        "night_fallback_mode": config.night_fallback_mode,
        "quality_fallback": config.allow_quality_fallback,
        "map_view": normalized_map_view(config.map_view),
        "flat_map": {
            "min_lat": config.flat_min_lat,
            "max_lat": config.flat_max_lat,
            "min_lon": config.flat_min_lon,
            "max_lon": config.flat_max_lon,
            "resolution_deg": config.flat_resolution_deg,
        },
        "resampler": config.resampler,
        "border_lines": {
            "enabled": config.add_border_lines,
            "color": config.border_line_color,
            "width": config.border_line_width,
        },
        "flat_overlays": {
            "map_labels": config.add_map_labels,
            "night_boundary": config.add_night_boundary,
            "crosshair": config.add_crosshair,
            "crosshair_type": normalized_crosshair_type(config.crosshair_type),
            "crosshair_color": config.crosshair_color,
            "zoom_earth_style": config.zoom_earth_style,
        },
        "gpu_acceleration": config.gpu_acceleration,
        "max_safe_png_pixels": config.max_safe_png_pixels,
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
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    return data if isinstance(data, dict) else None


def write_json_file(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(f".{path.name}.{os.getpid()}.{threading.get_ident()}.tmp")
    try:
        tmp_path.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        tmp_path.replace(path)
    except Exception:
        try:
            tmp_path.unlink(missing_ok=True)
        except Exception:
            pass
        raise


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
    if not isinstance(frames, list):
        return []
    return [frame for frame in frames if isinstance(frame, dict)]


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
    if path and valid_resume_frame(path):
        return path
    return None


def valid_resume_frame(path: Path) -> bool:
    try:
        if not path.exists() or path.stat().st_size <= 0:
            return False
    except OSError:
        return False
    suffix = path.suffix.lower()
    if suffix in {".png", ".jpg", ".jpeg", ".gif"}:
        try:
            from PIL import Image

            with Image.open(path) as image:
                image.verify()
            return True
        except Exception:
            return False
    if suffix in {".tif", ".tiff"}:
        try:
            import rasterio

            with rasterio.open(path) as src:
                return bool(src.width > 0 and src.height > 0 and src.count > 0)
        except Exception:
            return False
    return True


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
        "Try flat map output, a shorter timelapse range, or a coarser product such as B13. "
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
    # Lock the target to the reference band/timelapse frames first. Other
    # required bands validate the result below, but their tiny floating-point
    # extent differences must not shrink full-disk targets to 21999x21999.
    common, source_shape = find_native_common_area(area_list, intersect_extents(area_list), source_pixel_size_m)
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
    if is_flat_map(config):
        return scene.resample(
            target_area,
            datasets=datasets,
            resampler="nearest",
            radius_of_influence=10000,
        )
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


def satpy_resample_datasets(
    active: str,
    satpy_name: str,
    config: ProcessorConfig,
) -> list[str] | None:
    if config.use_night_fallback and uses_hybrid_night_fallback(active, config.night_fallback_mode):
        return [satpy_name, "B03", "B13"]
    if is_flat_map(config) and active == "True Color RGB (Enhanced)":
        return [satpy_name, *required_bands(active, include_night_fallback=False)]
    return satpy_resample_datasets_for_composite(active, satpy_name)


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


def is_rgb_dataarray(dataset: xr.DataArray) -> bool:
    return (
        len(dataset.dims) == 3
        and dataset.dims[0] == "bands"
        and int(dataset.sizes.get("bands", 0)) == 3
    )


def validate_rgb_dataset_for_direct_output(dataset: xr.DataArray, area: AreaDefinition | None = None) -> None:
    if not is_rgb_dataarray(dataset):
        raise ValueError("Direct RGB output requires a DataArray with dimensions ('bands', y, x) and exactly 3 bands.")
    y_dim, x_dim = dataset.dims[-2], dataset.dims[-1]
    height = int(dataset.sizes.get(y_dim, 0))
    width = int(dataset.sizes.get(x_dim, 0))
    if width <= 0 or height <= 0:
        raise ValueError("Direct RGB output requires non-empty image dimensions.")
    if area is not None and (int(area.width) != width or int(area.height) != height):
        raise ValueError(
            "Direct RGB output dimensions do not match the target map area: "
            f"dataset {width}x{height} px, area {int(area.width)}x{int(area.height)} px."
        )
    if dataset.dtype.kind in {"O", "S", "U", "V"}:
        raise ValueError(f"Direct RGB output requires numeric data, got dtype {dataset.dtype}.")
    if isinstance(dataset.data, da.Array) and len(dataset.data.chunks) != 3:
        raise ValueError("Direct RGB output requires a 3-dimensional Dask array.")


def area_transform(area: AreaDefinition):
    from rasterio.transform import from_bounds

    left, bottom, right, top = area.area_extent
    return from_bounds(left, bottom, right, top, int(area.width), int(area.height))


def write_rgb_geotiff_low_ram(dataset: xr.DataArray, output_path: Path, area: AreaDefinition) -> Path:
    require_module("rasterio", "direct RGB GeoTIFF output")
    import rasterio

    validate_rgb_dataset_for_direct_output(dataset, area)
    data = prepared_rgb_dask_array(dataset)
    dask_data = data.data
    delayed_blocks = dask_data.to_delayed().flatten()
    band_chunks, y_chunks, x_chunks = dask_data.chunks
    crs = area.crs
    output_path.parent.mkdir(parents=True, exist_ok=True)
    profile = {
        "driver": "GTiff",
        "width": int(area.width),
        "height": int(area.height),
        "count": 3,
        "dtype": "uint8",
        "crs": crs,
        "transform": area_transform(area),
        "nodata": 0,
        "tiled": True,
        "compress": "deflate",
        "photometric": "RGB",
    }
    tmp_path = temporary_output_path(output_path)
    try:
        tmp_path.unlink(missing_ok=True)
        with rasterio.open(tmp_path, "w", **profile) as dst:
            index = 0
            for band_index, band_height in enumerate(band_chunks):
                for y_offset, y_size in chunk_offsets(y_chunks):
                    for x_offset, x_size in chunk_offsets(x_chunks):
                        block = delayed_blocks[index].compute()
                        index += 1
                        if is_cupy_array_like(block):
                            import cupy as cp  # type: ignore

                            block = cp.asnumpy(block)
                        if band_height != 1:
                            raise ValueError("RGB GeoTIFF writer expects one band per block.")
                        dst.write(
                            block[0],
                            int(band_index) + 1,
                            window=rasterio.windows.Window(x_offset, y_offset, x_size, y_size),
                        )
            dst.colorinterp = (
                rasterio.enums.ColorInterp.red,
                rasterio.enums.ColorInterp.green,
                rasterio.enums.ColorInterp.blue,
            )
        tmp_path.replace(output_path)
    except Exception:
        try:
            tmp_path.unlink(missing_ok=True)
        except Exception:
            pass
        raise
    return output_path


def prepared_rgb_dask_array(dataset: xr.DataArray) -> xr.DataArray:
    validate_rgb_dataset_for_direct_output(dataset)
    data = dataset.astype(np.uint8)
    if not isinstance(data.data, da.Array):
        data = data.chunk({"bands": 1, data.dims[-2]: data.sizes[data.dims[-2]], data.dims[-1]: data.sizes[data.dims[-1]]})
    dask_data = data.data
    if len(dask_data.chunks[0]) != 3 or any(chunk != 1 for chunk in dask_data.chunks[0]):
        dask_data = dask_data.rechunk((1, dask_data.chunks[1], dask_data.chunks[2]))
        data = data.copy(deep=False, data=dask_data)
    return data


def write_rgb_png_low_ram(dataset: xr.DataArray, output_path: Path) -> Path:
    from PIL import Image

    validate_rgb_dataset_for_direct_output(dataset)
    data = prepared_rgb_dask_array(dataset)
    dask_data = data.data
    height = int(data.sizes[data.dims[-2]])
    width = int(data.sizes[data.dims[-1]])
    if len(dask_data.chunks[0]) != 3 or any(chunk != 1 for chunk in dask_data.chunks[0]):
        dask_data = dask_data.rechunk((1, dask_data.chunks[1], dask_data.chunks[2]))
    delayed_blocks = dask_data.to_delayed().flatten()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = temporary_output_path(output_path)
    work_path = tmp_path.with_suffix(tmp_path.suffix + ".rgb")
    try:
        tmp_path.unlink(missing_ok=True)
        work_path.unlink(missing_ok=True)
        rgb = np.memmap(work_path, dtype=np.uint8, mode="w+", shape=(height, width, 3))
        index = 0
        _band_chunks, y_chunks, x_chunks = dask_data.chunks
        for band_index, band_height in enumerate(dask_data.chunks[0]):
            if band_height != 1:
                raise ValueError("RGB PNG writer expects one band per block.")
            for y_offset, y_size in chunk_offsets(y_chunks):
                for x_offset, x_size in chunk_offsets(x_chunks):
                    block = delayed_blocks[index].compute()
                    index += 1
                    if is_cupy_array_like(block):
                        import cupy as cp  # type: ignore

                        block = cp.asnumpy(block)
                    rgb[y_offset : y_offset + y_size, x_offset : x_offset + x_size, band_index] = np.asarray(
                        block[0],
                        dtype=np.uint8,
                    )
        rgb.flush()
        image_array = np.asarray(rgb)
        Image.fromarray(image_array, mode="RGB").save(tmp_path)
        del image_array
        del rgb
        tmp_path.replace(output_path)
    except Exception:
        try:
            tmp_path.unlink(missing_ok=True)
        except Exception:
            pass
        try:
            work_path.unlink(missing_ok=True)
        except Exception:
            pass
        raise
    finally:
        try:
            work_path.unlink(missing_ok=True)
        except Exception:
            pass
    return output_path


def chunk_offsets(chunks: tuple[int, ...]) -> Iterable[tuple[int, int]]:
    offset = 0
    for size in chunks:
        yield offset, int(size)
        offset += int(size)


def rgb_geotiff_is_degenerate(path: Path, max_mean: float = 2.0) -> tuple[bool, str]:
    require_module("rasterio", "GeoTIFF output validation")
    import rasterio

    if not path.exists():
        return True, "file does not exist"
    if path.suffix.lower() not in {".tif", ".tiff"}:
        return False, "not a GeoTIFF"
    try:
        with rasterio.open(path) as src:
            if src.count < 3:
                return False, "not an RGB GeoTIFF"
            sample_height = min(int(src.height), 256)
            sample_width = min(int(src.width), 256)
            sample = src.read(
                indexes=(1, 2, 3),
                out_shape=(3, sample_height, sample_width),
                masked=True,
            )
    except Exception as exc:
        return True, f"could not validate GeoTIFF: {exc}"
    band_values = []
    for index in range(3):
        values = sample[index].compressed() if hasattr(sample[index], "compressed") else np.asarray(sample[index]).ravel()
        if values.size == 0:
            return True, "RGB bands contain only nodata values"
        band_values.append(values)
    ranges = [float(values.max()) - float(values.min()) for values in band_values]
    means = [float(values.mean()) for values in band_values]
    if all(value <= 0.0 for value in ranges) and max(means) <= max_mean:
        return True, "RGB bands are constant and near black"
    if all(float(values.max()) <= max_mean for values in band_values):
        return True, "RGB bands contain only near-black values"
    return False, "RGB GeoTIFF has visible data"


def save_dataset_with_optional_overlay(
    scene: Scene,
    dataset_name: str,
    output_path: Path,
    writer: str,
    enhance: bool,
    overlay: dict | None,
    fill_value: int | float | None = None,
) -> Path:
    def save_once(target_path: Path, target_writer: str, target_overlay: dict | None = None) -> Path:
        target_path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = temporary_output_path(target_path)
        save_kwargs = {
            "filename": str(tmp_path),
            "writer": target_writer,
            "enhance": enhance,
        }
        if fill_value is not None:
            save_kwargs["fill_value"] = fill_value
        try:
            tmp_path.unlink(missing_ok=True)
            if target_overlay is None:
                scene.save_dataset(dataset_name, **save_kwargs)
            else:
                scene.save_dataset(dataset_name, overlay=target_overlay, **save_kwargs)
            tmp_path.replace(target_path)
            return target_path
        except Exception:
            try:
                tmp_path.unlink(missing_ok=True)
            except Exception:
                pass
            raise

    if overlay is not None:
        try:
            return save_once(output_path, writer, overlay)
        except Exception as exc:
            LOG.warning(
                "Border overlay failed (%s). Saving image without border lines. "
                "Overlay preflight should normally catch missing pycoast data before processing.",
                exc,
            )
    try:
        return save_once(output_path, writer)
    except ValueError as exc:
        if output_path.suffix.lower() == ".png" and "empty image" in str(exc).lower():
            fallback_path = output_path.with_suffix(".tif")
            LOG.warning(
                "PNG writer failed with '%s'. Retrying as chunked GeoTIFF: %s",
                exc,
                fallback_path.name,
            )
            return save_once(fallback_path, "geotiff")
        raise


def save_satpy_dataset_output(
    resampled: Scene,
    active: str,
    satpy_name: str,
    output_path: Path,
    config: ProcessorConfig,
    overlay_options: dict | None,
    scan_time: datetime,
) -> Path:
    dataset_name = satpy_name
    enhance = True
    fill_value: int | float | None = None
    valid_mask = None
    if config.use_night_fallback and uses_hybrid_night_fallback(active, config.night_fallback_mode):
        dataset = apply_hybrid_night_if_needed(
            resampled[satpy_name],
            resampled,
            active,
            config.use_night_fallback,
            config.night_fallback_mode,
        )
        dataset_name = dataset.attrs["name"]
        resampled[dataset_name] = dataset
        enhance = False
        fill_value = 0
        LOG.info("Hybrid day/night fallback enabled for %s; filling dark side with B13 infrared.", active)
    if is_flat_map(config) and true_color_product(active):
        valid_mask = flat_map_validity_mask_from_scene(
            resampled,
            required_bands(
                active,
                use_night_fallback=config.use_night_fallback,
                night_fallback_mode=config.night_fallback_mode,
            ),
            resampled[dataset_name].attrs["area"],
        )
    saved_path = save_dataset_with_optional_overlay(
        resampled,
        dataset_name,
        output_path,
        writer_for_output(output_path),
        enhance=enhance,
        fill_value=fill_value,
        overlay=None,
    )
    if not should_style_output(config, active):
        return saved_path
    return apply_flat_map_visual_overlays(
        saved_path,
        resampled[dataset_name].attrs["area"],
        config,
        scan_time,
        active,
        overlay_options=overlay_options,
        valid_mask=valid_mask,
    )


def save_custom_composite_output(
    scene: Scene,
    active: str,
    master_area: AreaDefinition,
    output_path: Path,
    config: ProcessorConfig,
    is_night: bool,
    overlay_options: dict | None,
    scan_time: datetime,
    progress: ProgressCallback | None = None,
    cancel_event: threading.Event | None = None,
) -> Path:
    custom_bands = required_bands(
        active,
        use_night_fallback=config.use_night_fallback,
        night_fallback_mode=config.night_fallback_mode,
    )
    emit_progress(progress, f"Loading {active}", None, None)
    load_bands(scene, custom_bands)
    log_memory("after load", config)
    check_cancel(cancel_event)
    use_direct_flat_map = is_flat_map(config) and active in {"True Color RGB (Enhanced)", "True Color Reproduction Image"}
    if not is_flat_map(config):
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
    if use_direct_flat_map:
        emit_progress(progress, "Direct flat-map sampling", None, None)
        LOG.info(
            "Using direct flat-map source sampling for %s to avoid high-memory KD-tree resampling.",
            active,
        )
        resampled = direct_flat_map_sample_scene(scene, custom_bands, master_area)
        log_memory("after direct sample setup", config)
    else:
        emit_progress(progress, "Resampling", None, None)
        resampled = resample_scene_low_ram(scene, master_area, config, datasets=custom_bands)
        log_memory("after resample", config)
    check_cancel(cancel_event)
    emit_progress(progress, f"Building {active}", None, None)
    if config.gpu_acceleration and can_build_gpu_custom_composite(active) and not use_direct_flat_map:
        emit_progress(progress, "GPU acceleration enabled for custom composite math", None, None)
        LOG.info("GPU custom composite will return CPU chunks for saving.")
        try:
            dataset_name, dataset = build_gpu_custom_composite(resampled, active, config)
        except Exception as exc:
            raise RuntimeError(
                "GPU custom composite failed. Disable GPU acceleration and retry this run. "
                f"{exc.__class__.__name__}: {exc}"
            ) from exc
    else:
        if use_direct_flat_map and config.gpu_acceleration:
            LOG.info("GPU custom composite skipped for direct flat-map sampling; sampled chunks stay on the CPU path.")
        dataset_name, dataset = build_custom_composite(resampled, active, is_night, config)
        dataset = maybe_cpu_dataset_after_gpu(dataset, config)
    resampled[dataset_name] = dataset
    valid_mask = None
    if is_flat_map(config) and (flat_map_visual_style_enabled(config) or true_color_product(active)):
        if use_direct_flat_map:
            valid_mask = direct_flat_map_validity_mask_from_scenes(scene, resampled, custom_bands, master_area)
        else:
            valid_mask = flat_map_validity_mask_from_scene(resampled, custom_bands, master_area)
    check_cancel(cancel_event)
    emit_progress(progress, f"Saving {output_path.name}", None, None)
    if writer_for_output(output_path) == "geotiff" and is_rgb_dataarray(dataset):
        output_path = write_rgb_geotiff_low_ram(dataset, output_path, master_area)
        output_path = apply_flat_map_visual_overlays(
            output_path,
            master_area,
            config,
            scan_time,
            active,
            overlay_options=overlay_options,
            valid_mask=valid_mask,
        )
        degenerate, reason = rgb_geotiff_is_degenerate(output_path)
        if degenerate:
            raise RuntimeError(f"GeoTIFF output is invalid after direct RGB write: {reason}.")
    elif output_path.suffix.lower() == ".png" and is_rgb_dataarray(dataset):
        output_path = write_rgb_png_low_ram(dataset, output_path)
        if not is_flat_map(config):
            output_path = apply_direct_overlay_to_image_file(output_path, master_area, overlay_options)
        output_path = apply_flat_map_visual_overlays(
            output_path,
            master_area,
            config,
            scan_time,
            active,
            overlay_options=overlay_options,
            valid_mask=valid_mask,
        )
    else:
        output_path = save_dataset_with_optional_overlay(
            resampled,
            dataset_name,
            output_path,
            writer_for_output(output_path),
            enhance=False,
            fill_value=0,
            overlay=overlay_options,
        )
        if writer_for_output(output_path) == "geotiff" and is_rgb_dataarray(dataset):
            degenerate, reason = rgb_geotiff_is_degenerate(output_path)
            if degenerate:
                LOG.warning("GeoTIFF output looked invalid (%s). Retrying with direct RGB writer.", reason)
                output_path = write_rgb_geotiff_low_ram(dataset, output_path, master_area)
                degenerate, retry_reason = rgb_geotiff_is_degenerate(output_path)
                if degenerate:
                    raise RuntimeError(f"GeoTIFF output is invalid after retry: {retry_reason}.")
        output_path = apply_flat_map_visual_overlays(
            output_path,
            master_area,
            config,
            scan_time,
            active,
            overlay_options=overlay_options,
            valid_mask=valid_mask,
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
    scan_time: datetime,
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
        scan_time,
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
    scan_time: datetime,
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
        scan_time,
        progress=progress,
        cancel_event=cancel_event,
    )


def _world_file_path(output_path: Path) -> Path:
    """Return the conventional ESRI world-file path for a raster output."""
    suffix = output_path.suffix.lower()
    mapping = {
        ".png": ".pgw",
        ".tif": ".tfw",
        ".tiff": ".tfw",
        ".jpg": ".jgw",
        ".jpeg": ".jgw",
    }
    return output_path.with_suffix(mapping.get(suffix, ".wld"))


def _area_lonlat_corner_bounds(area: AreaDefinition) -> tuple[float, float, float, float] | None:
    """Best-effort (min_lon, min_lat, max_lon, max_lat) from an area's corners.

    Returns None when corners fall off the Earth disk (e.g. a full-disk
    geostationary area), in which case the caller omits lat/lon bounds.
    """
    try:
        height = int(area.height)
        width = int(area.width)
        cols = [0, width - 1, 0, width - 1]
        rows = [0, 0, height - 1, height - 1]
        lons: list[float] = []
        lats: list[float] = []
        for col, row in zip(cols, rows):
            lon, lat = area.get_lonlat(row, col)
            if np.isfinite(lon) and np.isfinite(lat):
                lons.append(float(lon))
                lats.append(float(lat))
        if len(lons) < 4:
            return None
        return (min(lons), min(lats), max(lons), max(lats))
    except Exception:
        return None


def write_output_sidecar(
    output_path: Path,
    config: ProcessorConfig,
    info: UrlInfo,
    scan_time: datetime,
    area: AreaDefinition,
    product: str,
) -> None:
    """Write a small JSON + world file (+ .prj) next to a rendered image.

    The sidecar makes each image self-documenting and re-loadable in GIS tools:
    it records the exact geographic bounds, the scan time, the projection and the
    settings that produced the picture. Any failure here is logged and swallowed
    so writing metadata can never break an otherwise successful run.
    """
    if not getattr(config, "write_metadata_sidecar", True):
        return
    try:
        suffix = output_path.suffix.lower()
        if suffix not in {".png", ".tif", ".tiff", ".jpg", ".jpeg"}:
            return
        width = int(area.width)
        height = int(area.height)
        x_min, y_min, x_max, y_max = (float(v) for v in area.area_extent)
        pixel_size_x = (x_max - x_min) / width if width else 0.0
        pixel_size_y = (y_max - y_min) / height if height else 0.0

        # Projection description (WKT preferred, proj4 as a fallback).
        proj4 = ""
        wkt = ""
        try:
            proj4 = area.crs.to_proj4() or ""
        except Exception:
            proj4 = getattr(area, "proj_str", "") or ""
        try:
            wkt = area.crs.to_wkt()
        except Exception:
            wkt = ""

        flat = is_flat_map(config)
        metadata: dict[str, object] = {
            "generator": f"{APP_DISPLAY_NAME}",
            "app_version": APP_VERSION,
            "image_file": output_path.name,
            "product": product,
            "requested_product": config.composite_choice,
            "scan_time_utc": scan_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "satellite_id": info.sat_id,
            "source_area": info.area,
            "view": "flat_map" if flat else "native",
            "pixels": {"width": width, "height": height},
            "projection": {
                "name": area.proj_id if hasattr(area, "proj_id") else "",
                "proj4": proj4,
                "extent_xy_min_max": [x_min, y_min, x_max, y_max],
                "pixel_size": [pixel_size_x, abs(pixel_size_y)],
                "units": "meters",
            },
            "settings": {
                "map_view": config.map_view,
                "image_format": config.image_format,
                "resampler": config.resampler,
                "zoom_earth_style": bool(config.zoom_earth_style),
                "overlay_theme": getattr(config, "overlay_theme", OVERLAY_THEME),
                "segment_aware_downloads": bool(getattr(config, "segment_aware_downloads", True)),
                "add_border_lines": bool(config.add_border_lines),
                "add_map_labels": bool(config.add_map_labels),
                "add_night_boundary": bool(config.add_night_boundary),
                "add_crosshair": bool(config.add_crosshair),
            },
        }

        if flat:
            metadata["geographic_bounds"] = {
                "min_lat": float(config.flat_min_lat),
                "max_lat": float(config.flat_max_lat),
                "min_lon": float(config.flat_min_lon),
                "max_lon": float(config.flat_max_lon),
                "resolution_deg": float(config.flat_resolution_deg),
                "note": "Requested crop in degrees; image grid is Web Mercator.",
            }
        else:
            corner_bounds = _area_lonlat_corner_bounds(area)
            if corner_bounds is not None:
                min_lon, min_lat, max_lon, max_lat = corner_bounds
                metadata["geographic_bounds"] = {
                    "min_lat": min_lat,
                    "max_lat": max_lat,
                    "min_lon": min_lon,
                    "max_lon": max_lon,
                    "note": "Approximate lat/lon of the image corners.",
                }
            else:
                metadata["geographic_bounds"] = {
                    "note": "Full-disk geostationary image; corners fall off the Earth.",
                }

        json_path = output_path.with_suffix(output_path.suffix + ".json")
        json_path.write_text(json.dumps(metadata, indent=2), encoding="utf-8")

        # ESRI world file: pixel size and the centre of the upper-left pixel.
        world_path = _world_file_path(output_path)
        world_lines = [
            repr(pixel_size_x),
            "0.0",
            "0.0",
            repr(-abs(pixel_size_y)),
            repr(x_min + pixel_size_x / 2.0),
            repr(y_max - abs(pixel_size_y) / 2.0),
        ]
        world_path.write_text("\n".join(world_lines) + "\n", encoding="utf-8")

        if wkt:
            prj_path = output_path.with_suffix(".prj")
            prj_path.write_text(wkt, encoding="utf-8")
    except Exception as exc:  # pragma: no cover - metadata must never break a run
        LOG.warning("Could not write metadata sidecar for %s (%s).", output_path.name, exc)


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
    setattr(process_frame, "last_error", "")
    check_cancel(cancel_event)
    requested = config.composite_choice
    bands = required_bands(
        requested,
        use_night_fallback=config.use_night_fallback,
        night_fallback_mode=config.night_fallback_mode,
    )
    segment_numbers = segments_for_flat_bounds(info, config)
    if segment_numbers is not None and len(segment_numbers) < info.total_segments:
        LOG.info(
            "Segment-aware download: fetching %s of %s FLDK segments for the regional crop "
            "(latitude %.1f..%.1f).",
            len(segment_numbers), info.total_segments, config.flat_min_lat, config.flat_max_lat,
        )
    tasks = make_download_tasks(info, dt, bands, TEMP_DIR, segment_numbers=segment_numbers)
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
            missing = len(tasks) - len(local_files)
            if config.allow_quality_fallback and local_files:
                LOG.warning(
                    "Proceeding with %s/%s segments (%s missing after retries); the image may "
                    "have empty stripes where segments are missing. Turn off 'Allow lower-quality "
                    "fallback' to require every segment instead.",
                    len(local_files), len(tasks), missing,
                )
            else:
                LOG.warning(
                    "Skipping frame; got %s/%s segments after retries. This is almost always a "
                    "network/DNS problem reaching the data host, not the imagery. Enable 'Allow "
                    "lower-quality fallback' to render with the segments that did download.",
                    len(local_files), len(tasks),
                )
                return None

        is_night = False
        if (
            config.use_night_fallback
            and (requested in DAY_ONLY_COMPOSITES or requested == "B03 and B13 at night")
        ):
            emit_progress(progress, "Checking day/night fallback", None, None)
            night_scene = Scene(filenames=[str(path) for path in local_files], reader="ahi_hsd")
            try:
                night_check_band = night_check_band_for_bands(bands)
                if night_check_band:
                    is_night = is_visible_dark(
                        night_scene,
                        master_area,
                        night_check_band,
                        direct_flat_map=is_flat_map(config),
                    )
                else:
                    LOG.warning(
                        "Night fallback requested but no reflectance band is available; using night fallback."
                    )
                    is_night = True
            finally:
                night_scene = None
                gc.collect()
            log_memory("night check", config)
            check_cancel(cancel_event)

        active = select_active_composite(
            requested,
            is_night,
            config.use_night_fallback,
            config.night_fallback_mode,
        )
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
            config=config,
        )
        output_path = enforce_safe_output_format(output_path, master_area, config)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        overlay_options = build_overlay_options(config)

        if should_force_low_ram_flat_true_color(config, active):
            LOG.info("Flat-map %s will use direct low-RAM RGB output instead of Satpy composite saving.", active)
            output_path = save_custom_composite_output(
                scene,
                active,
                master_area,
                output_path,
                config,
                is_night,
                overlay_options,
                dt,
                progress=progress,
                cancel_event=cancel_event,
            )
            log_memory("after save", config)
            frame_succeeded = True
            return output_path

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
                        dt,
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
            hybrid_inputs = (
                config.use_night_fallback
                and uses_hybrid_night_fallback(active, config.night_fallback_mode)
            )
            if hybrid_inputs:
                scene.load(["B03"], calibration="reflectance")
                scene.load(["B13"], calibration="brightness_temperature")
            emit_progress(progress, "Resampling", None, None)
            try:
                resample_datasets = satpy_resample_datasets(active, satpy_name, config)
                resampled = resample_scene_low_ram(scene, master_area, config, datasets=resample_datasets)
                log_memory("after resample", config)
                check_cancel(cancel_event)
                emit_progress(progress, f"Saving {output_path.name}", None, None)
                output_path = save_satpy_dataset_output(
                    resampled,
                    active,
                    satpy_name,
                    output_path,
                    config,
                    overlay_options,
                    dt,
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
                        dt,
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
            dt,
            progress=progress,
            cancel_event=cancel_event,
        )
        frame_succeeded = True
        return output_path
    except ProcessingCancelled:
        LOG.info("Frame canceled.")
        raise
    except Exception as exc:
        setattr(process_frame, "last_error", f"{exc.__class__.__name__}: {exc}")
        LOG.exception("Frame failed: %s", exc)
        return None
    finally:
        if frame_succeeded:
            try:
                if "output_path" in locals() and output_path is not None and "active" in locals():
                    write_output_sidecar(output_path, config, info, dt, master_area, active)
            except Exception as exc:
                LOG.debug("Sidecar metadata step skipped (%s).", exc)
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

    fmt = config.timelapse_format.lower()
    if fmt == "mp4" and not has_module("imageio_ffmpeg"):
        LOG.warning("imageio-ffmpeg is unavailable; falling back to GIF")
        fmt = "gif"

    import imageio.v2 as imageio

    start = datetime.strptime(info.timestamp, "%Y%m%d_%H%M")
    output = OUTPUT_DIR / f"{output_stem_from_template(config, info, start, fmt)}.{fmt}"
    output.parent.mkdir(parents=True, exist_ok=True)
    tmp_output = temporary_output_path(output)
    LOG.info("Assembling %s from %s frame(s)", output.name, len(paths))
    emit_progress(progress, f"Assembling {output.name}", 0, len(paths))
    log_memory("timelapse start", config)

    try:
        tmp_output.unlink(missing_ok=True)
        if fmt == "mp4":
            with imageio.get_writer(tmp_output, fps=config.fps, codec="libx264", quality=8) as writer:
                for idx, path in enumerate(paths, start=1):
                    check_cancel(cancel_event)
                    writer.append_data(imageio.imread(path))
                    emit_progress(progress, f"Added frame {idx}/{len(paths)}", idx, len(paths))
        else:
            with imageio.get_writer(tmp_output, mode="I", duration=int(1000 / config.fps), loop=0) as writer:
                for idx, path in enumerate(paths, start=1):
                    check_cancel(cancel_event)
                    writer.append_data(imageio.imread(path))
                    emit_progress(progress, f"Added frame {idx}/{len(paths)}", idx, len(paths))
        tmp_output.replace(output)
    except Exception:
        try:
            tmp_output.unlink(missing_ok=True)
        except Exception:
            pass
        raise

    log_memory("timelapse saved", config)
    if config.delete_timelapse_frames:
        cleanup_paths(paths)
    return output


def validate_configuration(config: ProcessorConfig | None = None) -> None:
    config = layer_defaults_config(config or default_config())
    if config.user_url is None or not str(config.user_url).strip():
        raise ValueError("Himawari URL is required.")
    if config.mode not in {"Single Image", "Timelapse"}:
        raise ValueError('MODE must be "Single Image" or "Timelapse".')
    if config.composite_choice not in COMPOSITE_BANDS:
        supported = ", ".join(sorted(COMPOSITE_BANDS))
        raise ValueError(f"Unsupported COMPOSITE_CHOICE {config.composite_choice!r}. Supported: {supported}")
    if normalized_satellite_layer_mode(config.satellite_layer_mode) not in SATELLITE_LAYER_MODES:
        raise ValueError(f"Satellite layer must be one of: {', '.join(SATELLITE_LAYER_MODES)}.")
    if BAND_NAMES != tuple(BAND_RESOLUTION):
        raise ValueError("BAND_RESOLUTION must define B01 through B16 in order.")
    if BAND_NAMES != tuple(BAND_PIXEL_SIZE_M):
        raise ValueError("BAND_PIXEL_SIZE_M must define B01 through B16 in order.")
    positive_integer(config.interval_minutes, "INTERVAL_MINUTES")
    positive_integer(config.hours_back, "HOURS_BACK")
    positive_integer(config.fps, "FPS")
    positive_integer(config.download_workers, "Download workers")
    positive_integer(config.dask_num_workers, "Dask workers")
    positive_finite_float(config.ram_limit_gb, "RAM limit")
    positive_integer(config.max_safe_png_pixels, "Max safe PNG pixels")
    validate_dask_chunk_size(config.dask_chunk_size)
    if config.download_workers > 4:
        LOG.warning("DOWNLOAD_WORKERS=%s requested; capped to 4.", config.download_workers)
    image_format = str(config.image_format).lower()
    if image_format not in {"png", "tif", "tiff", "geotiff"}:
        raise ValueError('IMAGE_FORMAT must be "png" or "tif".')
    validate_output_template(config.output_template)
    if str(config.timelapse_format).lower() not in {"gif", "mp4"}:
        raise ValueError('TIMELAPSE_FORMAT must be "gif" or "mp4".')
    if str(config.resampler).lower() not in {"native", "nearest"}:
        raise ValueError('RESAMPLER must be "native" or "nearest" for low-RAM processing.')
    if normalized_night_fallback_mode(config.night_fallback_mode) not in {"hybrid", "whole_frame_ir"}:
        raise ValueError('Night fallback mode must be "hybrid" or "whole_frame_ir".')
    validate_flat_map_settings(config)
    if config.gpu_acceleration:
        if not can_build_gpu_custom_composite(config.composite_choice):
            raise ValueError(
                "GPU acceleration is currently limited to True Color Reproduction Image and True Color RGB (Enhanced). "
                "Disable GPU acceleration or choose one of those products."
            )
        require_gpu_ready()
    if config.add_border_lines:
        parse_rgb_color(config.border_line_color)
        positive_finite_float(config.border_line_width, "Border line width")
        validate_overlay_ready_for_run(config)
    validated_map_label_size(config.map_label_size)
    normalized_crosshair_type(config.crosshair_type)
    try:
        parse_rgb_color(config.crosshair_color)
    except ValueError as exc:
        raise ValueError("Crosshair color must be a name, #RRGGBB, or R,G,B values.") from exc
    if config.add_map_labels:
        try:
            parse_rgb_color(getattr(config, "map_label_color", MAP_LABEL_COLOR))
        except ValueError as exc:
            raise ValueError("Map label color must be a name, #RRGGBB, or R,G,B values.") from exc
    if config.add_night_boundary:
        try:
            parse_rgb_color(getattr(config, "night_boundary_color", NIGHT_BOUNDARY_COLOR))
        except ValueError as exc:
            raise ValueError("Night boundary color must be a name, #RRGGBB, or R,G,B values.") from exc
    theme = getattr(config, "overlay_theme", OVERLAY_THEME)
    if theme not in OVERLAY_THEME_ORDER:
        raise ValueError(f"Overlay theme must be one of: {', '.join(OVERLAY_THEME_ORDER)}.")
    if config.zoom_earth_style and not is_flat_map(config):
        raise ValueError(
            "Zoom Earth-style flat-map styling requires flat map output. Turn it off to render the "
            "native full-disk view (labels, borders, night boundary and crosshair still work on native)."
        )


def validate_overlay_ready_for_run(config: ProcessorConfig, project_dir: Path = PROJECT_DIR) -> None:
    if not config.add_border_lines:
        return
    parse_rgb_color(config.border_line_color)
    positive_finite_float(config.border_line_width, "Border line width")
    status = overlay_status(project_dir)
    if status.ok:
        return
    details = []
    if status.missing_packages:
        details.append("Missing package(s): " + ", ".join(status.missing_packages))
    details.extend(status.missing_data)
    raise RuntimeError(
        "Border lines are enabled, but overlay setup is incomplete. "
        + " ".join(details)
        + " Run Quick Fix to install overlay data, or disable border lines."
    )


def validate_runtime_dependencies(config: ProcessorConfig, info: UrlInfo, start: datetime, area: AreaDefinition) -> None:
    validate_overlay_ready_for_run(config)
    ensure_directory_writable(OUTPUT_DIR, "Output")
    ensure_directory_writable(TEMP_DIR, "Temp/cache")
    validate_timelapse_frame_size(area, config)
    preview_name = output_filename(
        info,
        start,
        config.composite_choice,
        config.mode,
        0,
        config.image_format,
        config=config,
    )
    safe_name = enforce_safe_output_format(preview_name, area, config)
    if config.mode == "Timelapse" and writer_for_output(safe_name) == "geotiff":
        raise RuntimeError(
            "Timelapse frames would be too large for low-RAM GIF/MP4 assembly and would need GeoTIFF output. "
            "Use Single Image, choose a coarser product such as B13, or use a smaller Himawari area."
        )
    if writer_for_output(safe_name) == "geotiff":
        require_module("rasterio", "GeoTIFF output")
    if config.gpu_acceleration and config.composite_choice not in {"True Color RGB (Enhanced)", "True Color Reproduction Image"}:
        raise RuntimeError(
            "GPU acceleration is currently limited to True Color Reproduction Image and True Color RGB (Enhanced). "
            "Disable GPU acceleration or choose one of those products."
        )


def estimated_output_megapixels(config: ProcessorConfig, info: UrlInfo) -> float:
    """Approximate output size in megapixels, for the run-time estimate."""
    try:
        if is_flat_map(config):
            area = flat_map_area(config)
            return (int(area.width) * int(area.height)) / 1.0e6
        pixel_size = target_pixel_size_m(
            config.composite_choice,
            config.use_night_fallback,
            config.night_fallback_mode,
        )
        pixel_size = max(250, int(pixel_size))
        side = AHI_FULL_DISK_2KM_LINES * 2000.0 / pixel_size
        return (side * side) / 1.0e6
    except Exception:
        return 30.0


def build_run_summary(config: ProcessorConfig) -> RunSummary:
    config = layer_defaults_config(config)
    info = parse_url(config.user_url)
    start = datetime.strptime(info.timestamp, "%Y%m%d_%H%M")
    steps = frame_datetimes(start, config.mode, config.hours_back, config.interval_minutes)
    bands = required_bands(
        config.composite_choice,
        use_night_fallback=config.use_night_fallback,
        night_fallback_mode=config.night_fallback_mode,
    )
    all_segments_per_frame = int(info.total_segments)
    selected_segments = segments_for_flat_bounds(info, config)
    segments_per_frame = len(selected_segments) if selected_segments is not None else all_segments_per_frame
    total_segments = len(steps) * len(bands) * segments_per_frame
    output_megapixels = estimated_output_megapixels(config, info)
    estimated_seconds = estimate_run_seconds(
        config,
        frame_count=len(steps),
        band_count=len(bands),
        segments_per_frame=segments_per_frame,
        output_megapixels=output_megapixels,
    )
    warnings: list[str] = []
    if config.mode == "Timelapse" and config.composite_choice in QUALITY_CRITICAL_COMPOSITES:
        warnings.append("True color timelapses can be slow and memory-sensitive; IR products are safer.")
    if config.use_night_fallback and uses_hybrid_night_fallback(config.composite_choice, config.night_fallback_mode):
        warnings.append("Hybrid night fallback fills the dark side with B13 infrared while keeping sunlit true color.")
    if is_flat_map(config):
        warnings.append("Web Mercator flat map output uses nearest-neighbor reprojection and a bounded regional extent.")
        if selected_segments is not None and segments_per_frame < all_segments_per_frame:
            warnings.append(
                f"Segment-aware download will fetch {segments_per_frame} of {all_segments_per_frame} "
                "scan segments that cover the selected latitude band."
            )
        if config.zoom_earth_style:
            warnings.append("Zoom Earth-style flat maps keep the selected satellite product; no map tiles are added.")
        if config.add_map_labels or config.add_night_boundary or config.add_crosshair:
            warnings.append("Labels, night boundary, and crosshair are burned into PNG and GeoTIFF flat-map outputs.")
    elif config.add_map_labels or config.add_night_boundary or config.add_crosshair:
        warnings.append("Labels, night boundary, and crosshair are burned into the native full-disk output.")
    if config.gpu_acceleration:
        warnings.append("GPU acceleration is experimental and only applies to compatible custom composite math.")
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
        estimated_seconds=estimated_seconds,
        segments_per_frame=segments_per_frame,
        all_segments_per_frame=all_segments_per_frame,
    )


def preflight_run(config: ProcessorConfig, output_dir: Path = OUTPUT_DIR, temp_dir: Path = TEMP_DIR) -> PreflightResult:
    config = layer_defaults_config(config)
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

    if not config.auto_download:
        try:
            info = parse_url(config.user_url)
            start = datetime.strptime(info.timestamp, "%Y%m%d_%H%M")
            steps = frame_datetimes(start, config.mode, config.hours_back, config.interval_minutes)
            bands = required_bands(
                config.composite_choice,
                use_night_fallback=config.use_night_fallback,
                night_fallback_mode=config.night_fallback_mode,
            )
            total_expected = len(steps) * len(bands) * info.total_segments
            missing = missing_cached_segments(config, info, steps, bands, temp_dir)
            if missing:
                error = offline_cache_summary(missing, total_expected)
                if error not in errors:
                    errors.append(error)
        except Exception as exc:
            if str(exc) not in errors:
                errors.append(str(exc))

    if config.add_border_lines:
        status = overlay_status(PROJECT_DIR)
        if not status.ok:
            for error in status.missing_data:
                if error not in errors:
                    errors.append(error)
            for package in status.missing_packages:
                package_error = f"Missing package needed for border overlays: {package}"
                if package_error not in errors:
                    errors.append(package_error)

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
            night_fallback_mode="hybrid",
            download_workers=2,
            dask_num_workers=1,
            dask_chunk_size="32MiB",
            ram_limit_gb=10.0,
            resampler="native",
            add_border_lines=False,
            add_map_labels=False,
            add_night_boundary=False,
            add_crosshair=False,
            zoom_earth_style=False,
        )
    elif name == "Fast IR Check":
        values.update(
            mode="Single Image",
            composite_choice="B13 (Infrared Window)",
            image_format="png",
            auto_download=True,
            use_night_fallback=False,
            night_fallback_mode="whole_frame_ir",
            download_workers=1,
            dask_num_workers=1,
            dask_chunk_size="32MiB",
            ram_limit_gb=6.0,
            resampler="native",
            add_border_lines=False,
            add_map_labels=False,
            add_night_boundary=False,
            add_crosshair=False,
            zoom_earth_style=False,
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
            night_fallback_mode="whole_frame_ir",
            download_workers=1,
            dask_num_workers=1,
            dask_chunk_size="16MiB",
            ram_limit_gb=8.0,
            resampler="native",
            add_border_lines=False,
            add_map_labels=False,
            add_night_boundary=False,
            add_crosshair=False,
            zoom_earth_style=False,
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
    output_dir = migrate_default_cloud_sync_path(output_dir, "outputs", OUTPUT_DIR)
    temp_dir = migrate_default_cloud_sync_path(temp_dir, "temp", TEMP_DIR)
    return config, output_dir, temp_dir


def config_from_mapping(raw_config: object) -> ProcessorConfig | None:
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
    return config


def clean_custom_preset_name(name: str) -> str:
    cleaned = re.sub(r"\s+", " ", name.strip())
    if not cleaned:
        raise ValueError("Preset name is required.")
    if cleaned in BUILT_IN_PRESETS:
        raise ValueError("Built-in safe presets cannot be overwritten.")
    if len(cleaned) > 60:
        raise ValueError("Preset name is too long.")
    return cleaned


def serialize_custom_presets(presets: dict[str, ProcessorConfig]) -> dict:
    limited_items = list(presets.items())[:CUSTOM_PRESET_LIMIT]
    return {
        "schema_version": CUSTOM_PRESETS_SCHEMA_VERSION,
        "app_version": APP_VERSION,
        "presets": {name: config.__dict__.copy() for name, config in limited_items},
    }


def load_custom_presets(presets_path: Path = CUSTOM_PRESETS_FILE) -> dict[str, ProcessorConfig]:
    data = load_json_file(presets_path)
    if not data or data.get("schema_version") != CUSTOM_PRESETS_SCHEMA_VERSION:
        return {}
    raw_presets = data.get("presets", {})
    if not isinstance(raw_presets, dict):
        return {}
    presets: dict[str, ProcessorConfig] = {}
    for raw_name, raw_config in raw_presets.items():
        try:
            name = clean_custom_preset_name(str(raw_name))
        except ValueError:
            continue
        config = config_from_mapping(raw_config)
        if config is not None:
            presets[name] = config
        if len(presets) >= CUSTOM_PRESET_LIMIT:
            break
    return presets


def save_custom_presets(presets: dict[str, ProcessorConfig], presets_path: Path = CUSTOM_PRESETS_FILE) -> None:
    write_json_file(presets_path, serialize_custom_presets(presets))


def save_custom_preset(
    name: str,
    config: ProcessorConfig,
    presets_path: Path = CUSTOM_PRESETS_FILE,
) -> dict[str, ProcessorConfig]:
    cleaned = clean_custom_preset_name(name)
    presets = load_custom_presets(presets_path)
    ordered: dict[str, ProcessorConfig] = {cleaned: config}
    for existing_name, existing_config in presets.items():
        if existing_name != cleaned:
            ordered[existing_name] = existing_config
        if len(ordered) >= CUSTOM_PRESET_LIMIT:
            break
    save_custom_presets(ordered, presets_path)
    return ordered


def delete_custom_preset(name: str, presets_path: Path = CUSTOM_PRESETS_FILE) -> dict[str, ProcessorConfig]:
    cleaned = clean_custom_preset_name(name)
    presets = load_custom_presets(presets_path)
    presets.pop(cleaned, None)
    save_custom_presets(presets, presets_path)
    return presets


def utc_timestamp() -> str:
    return datetime.now(UTC).replace(tzinfo=None).isoformat(timespec="seconds") + "Z"


def run_record_id(started_at_utc: str, config: ProcessorConfig) -> str:
    digest = hashlib.sha1(
        json.dumps(
            {"started_at_utc": started_at_utc, "config": config.__dict__},
            sort_keys=True,
            default=str,
        ).encode("utf-8")
    ).hexdigest()
    return digest[:12]


def safe_log_timestamp(value: str) -> str:
    return re.sub(r"[^0-9A-Za-z]+", "_", value).strip("_") or "run"


def run_log_path(started_at_utc: str, config: ProcessorConfig, log_dir: Path = LOG_DIR) -> Path:
    return log_dir / f"{safe_log_timestamp(started_at_utc)}_{run_record_id(started_at_utc, config)}.log"


def add_run_file_log_handler(log_path: Path) -> logging.Handler:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    handler = logging.FileHandler(log_path, encoding="utf-8")
    handler.setLevel(logging.INFO)
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s", "%Y-%m-%d %H:%M:%S"))
    LOG.addHandler(handler)
    logging.getLogger("py.warnings").addHandler(handler)
    return handler


def remove_run_file_log_handler(handler: logging.Handler) -> None:
    for logger in (LOG, logging.getLogger("py.warnings")):
        try:
            logger.removeHandler(handler)
        except ValueError:
            pass
    handler.close()


def recent_run_source(config: ProcessorConfig) -> str:
    try:
        info = parse_url(config.user_url)
        return f"{info.area} {info.timestamp}"
    except Exception:
        return config.user_url.strip() or "Unknown source"


def recent_run_manifest_path(config: ProcessorConfig, output_dir: Path = OUTPUT_DIR) -> tuple[str, str]:
    if config.mode != "Timelapse":
        return "", ""
    try:
        info = parse_url(config.user_url)
        start = datetime.strptime(info.timestamp, "%Y%m%d_%H%M")
        steps = frame_datetimes(start, config.mode, config.hours_back, config.interval_minutes)
        run_id = stable_run_id(config, info, steps)
        return str(timelapse_manifest_path(run_id, output_dir)), str(timelapse_frame_dir(run_id, output_dir))
    except Exception:
        return "", ""


def build_recent_run_record(
    status: str,
    config: ProcessorConfig,
    started_at_utc: str,
    completed_at_utc: str | None = None,
    outputs: Iterable[Path] | None = None,
    error: str = "",
    log_path: Path | str | None = None,
    output_dir: Path = OUTPUT_DIR,
) -> RecentRunRecord:
    completed = completed_at_utc or utc_timestamp()
    manifest_path, frame_dir = recent_run_manifest_path(config, output_dir)
    return RecentRunRecord(
        run_id=run_record_id(started_at_utc, config),
        status=status,
        started_at_utc=started_at_utc,
        completed_at_utc=completed,
        app_version=APP_VERSION,
        mode=config.mode,
        product=config.composite_choice,
        source=recent_run_source(config),
        outputs=tuple(str(path) for path in (outputs or ())),
        manifest_path=manifest_path,
        frame_dir=frame_dir,
        log_path=str(log_path or ""),
        error=error,
        config=config.__dict__.copy(),
    )


def serialize_recent_run_record(record: RecentRunRecord) -> dict:
    return {
        "run_id": record.run_id,
        "status": record.status,
        "started_at_utc": record.started_at_utc,
        "completed_at_utc": record.completed_at_utc,
        "app_version": record.app_version,
        "mode": record.mode,
        "product": record.product,
        "source": record.source,
        "outputs": list(record.outputs),
        "manifest_path": record.manifest_path,
        "frame_dir": record.frame_dir,
        "log_path": record.log_path,
        "error": record.error,
        "config": dict(record.config),
    }


def recent_run_record_from_mapping(raw_record: object) -> RecentRunRecord | None:
    if not isinstance(raw_record, dict):
        return None
    raw_outputs = raw_record.get("outputs", [])
    if not isinstance(raw_outputs, list):
        raw_outputs = []
    raw_config = raw_record.get("config", {})
    config = config_from_mapping(raw_config)
    if config is None:
        raw_config = {}
    return RecentRunRecord(
        run_id=str(raw_record.get("run_id") or hashlib.sha1(json.dumps(raw_record, sort_keys=True, default=str).encode("utf-8")).hexdigest()[:12]),
        status=str(raw_record.get("status") or "unknown"),
        started_at_utc=str(raw_record.get("started_at_utc") or ""),
        completed_at_utc=str(raw_record.get("completed_at_utc") or ""),
        app_version=str(raw_record.get("app_version") or ""),
        mode=str(raw_record.get("mode") or getattr(config, "mode", "")),
        product=str(raw_record.get("product") or getattr(config, "composite_choice", "")),
        source=str(raw_record.get("source") or ""),
        outputs=tuple(str(path) for path in raw_outputs),
        manifest_path=str(raw_record.get("manifest_path") or ""),
        frame_dir=str(raw_record.get("frame_dir") or ""),
        log_path=str(raw_record.get("log_path") or ""),
        error=str(raw_record.get("error") or ""),
        config=dict(raw_config) if isinstance(raw_config, dict) else {},
    )


def load_recent_runs(history_path: Path = RECENT_RUNS_FILE) -> list[RecentRunRecord]:
    data = load_json_file(history_path)
    if not data or data.get("schema_version") not in {1, RECENT_RUNS_SCHEMA_VERSION}:
        return []
    raw_runs = data.get("runs", [])
    if not isinstance(raw_runs, list):
        return []
    records: list[RecentRunRecord] = []
    for raw_record in raw_runs:
        record = recent_run_record_from_mapping(raw_record)
        if record is not None:
            records.append(record)
        if len(records) >= RECENT_RUN_LIMIT:
            break
    return records


def save_recent_runs(records: list[RecentRunRecord], history_path: Path = RECENT_RUNS_FILE) -> None:
    write_json_file(
        history_path,
        {
            "schema_version": RECENT_RUNS_SCHEMA_VERSION,
            "app_version": APP_VERSION,
            "runs": [serialize_recent_run_record(record) for record in records[:RECENT_RUN_LIMIT]],
        },
    )


def append_recent_run(record: RecentRunRecord, history_path: Path = RECENT_RUNS_FILE) -> list[RecentRunRecord]:
    existing = [item for item in load_recent_runs(history_path) if item.run_id != record.run_id]
    records = [record] + existing
    save_recent_runs(records, history_path)
    return records[:RECENT_RUN_LIMIT]


def format_recent_run_summary(record: RecentRunRecord) -> str:
    lines = [
        f"Status: {record.status}",
        f"Started: {record.started_at_utc}",
        f"Completed: {record.completed_at_utc}",
        f"Source: {record.source}",
        f"Mode: {record.mode}",
        f"Product: {record.product}",
    ]
    if record.outputs:
        lines.extend(["", "Outputs:"])
        lines.extend(record.outputs)
    if record.manifest_path:
        lines.extend(["", f"Manifest: {record.manifest_path}"])
    if record.frame_dir:
        lines.append(f"Frames: {record.frame_dir}")
    if record.log_path:
        lines.append(f"Log: {record.log_path}")
    if record.error:
        lines.extend(["", "Error:", record.error])
    return "\n".join(lines)


def config_from_recent_run(record: RecentRunRecord) -> ProcessorConfig | None:
    return config_from_mapping(record.config)


def format_file_size(size_bytes: int) -> str:
    value = float(size_bytes)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if value < 1024 or unit == "TB":
            return f"{value:.1f} {unit}" if unit != "B" else f"{int(value)} B"
        value /= 1024
    return f"{size_bytes} B"


def safe_preview_metadata(path: Path, max_bytes: int = PREVIEW_MAX_BYTES) -> OutputPreview:
    if not path.exists():
        return OutputPreview(path, False, False, "File does not exist.")
    try:
        size = path.stat().st_size
    except OSError as exc:
        return OutputPreview(path, False, False, f"Could not read file: {exc}")
    suffix = path.suffix.lower()
    if suffix in {".tif", ".tiff"}:
        return OutputPreview(path, True, False, "GeoTIFF preview is metadata-only to avoid high memory use.", size)
    if suffix == ".mp4":
        return OutputPreview(path, True, False, "MP4 preview is metadata-only.", size)
    if suffix not in {".png", ".jpg", ".jpeg", ".gif"}:
        return OutputPreview(path, True, False, "Preview is not supported for this file type.", size)
    if size > max_bytes:
        return OutputPreview(path, True, False, "File is too large for safe preview.", size)
    try:
        from PIL import Image

        with Image.open(path) as image:
            dimensions = image.size
    except Exception as exc:
        return OutputPreview(path, True, False, f"Could not read preview metadata: {exc}", size)
    return OutputPreview(path, True, True, "Preview available.", size, dimensions)


def completion_message(outputs: Iterable[Path], config: ProcessorConfig, output_dir: Path = OUTPUT_DIR) -> str:
    output_list = list(outputs)
    lines = [f"Processing finished with {len(output_list)} output file(s)."]
    if output_list:
        main = output_list[-1]
        lines.append(f"Main output: {main}")
        if main.exists():
            try:
                lines.append(f"Main output size: {format_file_size(main.stat().st_size)}")
            except OSError:
                pass
    lines.append(f"Output folder: {output_dir}")
    manifest_path, frame_dir = recent_run_manifest_path(config, output_dir)
    if manifest_path:
        lines.append(f"Manifest: {manifest_path}")
    if frame_dir:
        lines.append(f"Frame folder: {frame_dir}")
    return "\n".join(lines)


def cancel_resume_message(config: ProcessorConfig, outputs: Iterable[Path], output_dir: Path = OUTPUT_DIR) -> str:
    output_list = list(outputs)
    if config.mode != "Timelapse":
        return "Processing canceled. No timelapse resume manifest is needed for single-image runs."
    manifest_path, frame_dir = recent_run_manifest_path(config, output_dir)
    frame_word = "frame" if len(output_list) == 1 else "frames"
    lines = [
        f"Processing canceled after {len(output_list)} completed {frame_word}.",
        "Retrying the same timelapse will reuse matching completed frames when resume is enabled.",
    ]
    if manifest_path:
        lines.append(f"Manifest: {manifest_path}")
    if frame_dir:
        lines.append(f"Frame folder: {frame_dir}")
    return "\n".join(lines)


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
    log_path: Path | str | None = None,
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
    if log_path:
        lines.extend(["", f"Log: {log_path}"])
    if log_text:
        lines.extend(["", "Recent log:", log_text.strip()])
    return "\n".join(lines).strip() + "\n"


def run(
    config: ProcessorConfig | None = None,
    progress: ProgressCallback | None = None,
    cancel_event: threading.Event | None = None,
    resume_timelapse: bool = True,
    started_at_utc: str | None = None,
) -> list[Path]:
    config = layer_defaults_config(config or default_config())
    configure_logging()
    started_at_utc = started_at_utc or utc_timestamp()
    current_log_path: Path | None = None
    log_handler: logging.Handler | None = None
    frame_failures: list[str] = []
    setattr(run, "last_log_path", None)
    setattr(run, "last_config", config)
    try:
        validate_configuration(config)
        check_cancel(cancel_event)
        config = resolve_live_satellite_url(config)
        setattr(run, "last_config", config)
        current_log_path = run_log_path(started_at_utc, config)
        log_handler = add_run_file_log_handler(current_log_path)
        setattr(run, "last_log_path", current_log_path)
        LOG.info("App version: %s", APP_VERSION)
        LOG.info("Run log: %s", current_log_path)
        LOG.info("Settings: %s", json.dumps(config.__dict__, sort_keys=True, default=str))
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
        check_cancel(cancel_event)
        log_memory("startup", config)
        if is_flat_map(config):
            min_lat, max_lat, min_lon, max_lon, resolution_deg, _height, _width = checked_flat_map_parameters(config)
            master_area = flat_map_area(config)
            LOG.info(
                "Web Mercator flat map area locked: %sx%s px, lat %.3f..%.3f lon %.3f..%.3f at %.4g deg/pixel at equator",
                master_area.width,
                master_area.height,
                min_lat,
                max_lat,
                min_lon,
                max_lon,
                resolution_deg,
            )
        else:
            area_band = area_reference_band(config.composite_choice)
            master_area = common_area_from_frames(
                info,
                steps,
                target_pixel_size_m(
                    config.composite_choice,
                    config.use_night_fallback,
                    config.night_fallback_mode,
                ),
                area_band,
                compatibility_bands=area_compatibility_bands(
                    config.composite_choice,
                    config.use_night_fallback,
                    config.night_fallback_mode,
                ),
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
            else:
                raw_failure = getattr(process_frame, "last_error", "")
                failure = (
                    raw_failure
                    if isinstance(raw_failure, str) and raw_failure
                    else f"Frame {idx + 1}/{len(steps)} did not create output."
                )
                frame_failures.append(failure)
                if manifest is not None:
                    update_manifest_frame(manifest, idx, None, "failed")
            if manifest is not None and manifest_path is not None:
                save_timelapse_manifest(manifest_path, manifest)

        if config.mode == "Timelapse" and outputs:
            if len(outputs) == 1:
                only_frame = outputs[0]
                if len(outputs) != len(steps):
                    LOG.warning(
                        "Timelapse produced only 1/%s successful frame(s); keeping the frame image instead of "
                        "assembling a one-frame %s animation. Check earlier frame errors.",
                        len(steps),
                        config.timelapse_format.lower(),
                    )
                    emit_progress(
                        progress,
                        f"Timelapse produced one frame; keeping {only_frame.suffix.lower() or 'frame'} image",
                        1,
                        len(steps),
                    )
                else:
                    LOG.info("Timelapse has one requested frame; keeping frame image %s.", only_frame)
                    emit_progress(progress, "Timelapse produced one frame; keeping frame image", 1, len(steps))
                if manifest is not None and manifest_path is not None:
                    manifest["movie"] = None
                    manifest["single_frame_output"] = str(only_frame)
                    manifest["completed_at_utc"] = datetime.now(UTC).replace(tzinfo=None).isoformat(timespec="seconds") + "Z"
                    save_timelapse_manifest(manifest_path, manifest)
                return [only_frame]
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
            detail = frame_failures[-1] if frame_failures else "no frames were processed successfully"
            raise RuntimeError(
                f"Timelapse failed: no frames were processed successfully; "
                f"{len(frame_failures)}/{len(steps)} frame(s) failed; {detail}. Log: {current_log_path}"
            )

        if config.mode == "Single Image" and outputs:
            LOG.info("Saved: %s", outputs[0])
            return outputs
        if config.mode == "Single Image":
            detail = frame_failures[-1] if frame_failures else "no output was created"
            raise RuntimeError(f"Single image failed: {detail}. Log: {current_log_path}")
        return outputs
    except ProcessingCancelled:
        LOG.info("Run canceled.")
        raise
    except Exception:
        LOG.exception("Run failed")
        raise
    finally:
        if log_handler is not None:
            remove_run_file_log_handler(log_handler)


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


def migrate_default_cloud_sync_path(path: Path, project_child_name: str, local_default: Path) -> Path:
    try:
        resolved = path.expanduser().resolve(strict=False)
        old_default = (PROJECT_DIR / project_child_name).resolve(strict=False)
    except Exception:
        return path
    if resolved == old_default and cloud_sync_marker(resolved):
        return local_default
    return path


def setup_configuration_errors(config: ProcessorConfig) -> list[str]:
    errors: list[str] = []
    if normalized_satellite_layer_mode(config.satellite_layer_mode) not in SATELLITE_LAYER_MODES:
        errors.append(f"Satellite layer must be one of: {', '.join(SATELLITE_LAYER_MODES)}.")
    config = layer_defaults_config(config)
    if config.user_url is None or not str(config.user_url).strip():
        errors.append("Himawari URL is required.")
    if config.mode not in {"Single Image", "Timelapse"}:
        errors.append('Mode must be "Single Image" or "Timelapse".')
    if config.composite_choice not in COMPOSITE_BANDS:
        errors.append(f"Unsupported product: {config.composite_choice}")
    for field_value, label in (
        (config.interval_minutes, "Interval minutes"),
        (config.hours_back, "Hours back"),
        (config.fps, "FPS"),
        (config.download_workers, "Download workers"),
        (config.dask_num_workers, "Dask workers"),
        (config.max_safe_png_pixels, "Max safe PNG pixels"),
    ):
        try:
            positive_integer(field_value, label)
        except ValueError as exc:
            errors.append(str(exc))
    try:
        positive_finite_float(config.ram_limit_gb, "RAM limit")
    except ValueError as exc:
        errors.append(str(exc))
    try:
        validate_dask_chunk_size(config.dask_chunk_size)
    except ValueError as exc:
        errors.append(str(exc))
    if str(config.image_format).lower() not in {"png", "tif", "tiff", "geotiff"}:
        errors.append('Image format must be "png" or "tif".')
    try:
        validate_output_template(config.output_template)
    except ValueError as exc:
        errors.append(str(exc))
    if str(config.timelapse_format).lower() not in {"gif", "mp4"}:
        errors.append('Timelapse format must be "gif" or "mp4".')
    if str(config.resampler).lower() not in {"native", "nearest"}:
        errors.append('Resampler must be "native" or "nearest".')
    if normalized_night_fallback_mode(config.night_fallback_mode) not in {"hybrid", "whole_frame_ir"}:
        errors.append('Night fallback mode must be "hybrid" or "whole_frame_ir".')
    try:
        validate_flat_map_settings(config)
    except ValueError as exc:
        errors.append(str(exc))
    if config.add_border_lines:
        try:
            parse_rgb_color(config.border_line_color)
        except ValueError as exc:
            errors.append(str(exc))
        try:
            positive_finite_float(config.border_line_width, "Border line width")
        except ValueError as exc:
            errors.append(str(exc))
    try:
        validated_map_label_size(config.map_label_size)
    except ValueError as exc:
        errors.append(str(exc))
    try:
        normalized_crosshair_type(config.crosshair_type)
    except ValueError as exc:
        errors.append(str(exc))
    try:
        parse_rgb_color(config.crosshair_color)
    except ValueError:
        errors.append("Crosshair color must be a name, #RRGGBB, or R,G,B values.")
    if config.zoom_earth_style and not is_flat_map(config):
        errors.append(
            "Zoom Earth-style flat-map styling requires flat map output. Turn it off to use the native "
            "full-disk view (labels/borders/night boundary/crosshair still work on native)."
        )
    if config.gpu_acceleration:
        status = gpu_support_status()
        if not status.ok:
            errors.append("GPU acceleration is enabled, but GPU support is not ready. " + status.detail)
        if config.composite_choice in COMPOSITE_BANDS and not can_build_gpu_custom_composite(config.composite_choice):
            errors.append(
                "GPU acceleration is currently limited to True Color Reproduction Image and True Color RGB (Enhanced). "
                "Disable GPU acceleration or choose one of those products."
            )
    return errors


def build_setup_status(
    config: ProcessorConfig,
    output_dir: Path = OUTPUT_DIR,
    temp_dir: Path = TEMP_DIR,
) -> SetupStatus:
    config = layer_defaults_config(config)
    details: list[str] = []
    warnings: list[str] = []
    errors = setup_configuration_errors(config)
    info: UrlInfo | None = None
    start: datetime | None = None
    user_url_text = "" if config.user_url is None else str(config.user_url).strip()

    if user_url_text:
        try:
            info = parse_url(user_url_text)
            start = datetime.strptime(info.timestamp, "%Y%m%d_%H%M")
            details.append(f"Source: {info.area} {info.timestamp}, {info.total_segments} segments per band")
        except Exception as exc:
            errors.append(str(exc))

    if config.composite_choice in COMPOSITE_BANDS:
        bands = required_bands(
            config.composite_choice,
            use_night_fallback=config.use_night_fallback,
            night_fallback_mode=config.night_fallback_mode,
        )
        band_list = ", ".join(bands)
        details.append(f"Product: {config.composite_choice} ({len(bands)} band(s): {band_list})")
        if info is not None and start is not None:
            try:
                frames = len(frame_datetimes(start, config.mode, config.hours_back, config.interval_minutes))
                total_segments = frames * len(bands) * info.total_segments
                frame_word = "frame" if frames == 1 else "frames"
                if config.auto_download:
                    details.append(f"Download estimate: {total_segments} segment file(s) across {frames} {frame_word}")
                else:
                    steps = frame_datetimes(start, config.mode, config.hours_back, config.interval_minutes)
                    missing = missing_cached_segments(config, info, steps, bands, temp_dir)
                    summary = offline_cache_summary(missing, total_segments)
                    if missing:
                        errors.append(summary)
                    else:
                        details.append(summary)
            except Exception as exc:
                errors.append(str(exc))
            if (
                info.area == "FLDK"
                and not is_flat_map(config)
                and config.image_format.lower() == "png"
                and target_pixel_size_m(
                    config.composite_choice,
                    config.use_night_fallback,
                    config.night_fallback_mode,
                )
                <= 500
            ):
                warnings.append(
                    "Full-disk 500 m PNG jobs may auto-switch to GeoTIFF for low-RAM writing."
                )

    if config.gpu_acceleration:
        status = gpu_support_status()
        if status.ok:
            details.append(f"GPU acceleration: experimental custom-composite math on {status.device_name or 'CUDA GPU'}")
            warnings.append("GPU mode keeps Satpy reading, reprojection, and image writing on the CPU path.")
            if config.dask_chunk_size == "16MiB":
                warnings.append("GPU mode works better with larger Dask chunks; use Best Performance if RAM headroom is available.")
            if config.ram_limit_gb < 4.0:
                warnings.append("The current RAM limit is low enough to bottleneck GPU jobs with CPU paging and disk writes.")
    else:
        details.append("GPU acceleration: off")

    if config.add_border_lines:
        status = overlay_status(PROJECT_DIR)
        if not status.ok:
            warnings.append("Border lines require pycoast, aggdraw, and GSHHS/WDBII shapefiles under overlays/.")
            errors.extend(status.missing_data)
            errors.extend(f"Missing package needed for border overlays: {package}" for package in status.missing_packages)
        else:
            details.append("Border overlays: ready")

    if is_flat_map(config):
        try:
            min_lat, max_lat, min_lon, max_lon, _resolution_deg, height, width = checked_flat_map_parameters(config)
            details.append(
                "Map view: Web Mercator flat map "
                f"{min_lat:g}..{max_lat:g} lat, "
                f"{min_lon:g}..{max_lon:g} lon, "
                f"{width}x{height} px"
            )
            warnings.append(
                "Web Mercator flat map output uses nearest-neighbor reprojection and may be slower than native disk output."
            )
            if config.zoom_earth_style:
                details.append("Flat-map style: Zoom Earth-like satellite styling enabled")
            if config.add_map_labels:
                details.append("Map labels: curated static labels enabled")
            if config.add_night_boundary:
                details.append("Night boundary: approximate solar terminator enabled")
            if config.add_crosshair:
                details.append(f"Crosshair: {normalized_crosshair_type(config.crosshair_type)} marker in {config.crosshair_color}")
            if config.add_map_labels or config.add_night_boundary or config.add_crosshair:
                warnings.append("Labels, night boundary, and crosshair are burned into PNG and GeoTIFF flat-map outputs.")
            try:
                validate_timelapse_frame_size(flat_map_area(config), config)
            except RuntimeError as exc:
                errors.append(str(exc))
        except ValueError as exc:
            if str(exc) not in errors:
                errors.append(str(exc))

    cloud_matches = []
    for label, path in (("output", output_dir), ("temp", temp_dir)):
        marker = cloud_sync_marker(path)
        if marker:
            cloud_matches.append(f"{label}: {path} ({marker})")
    if cloud_matches:
        warnings.append(
            "Cloud-sync path detected; large output/temp writes are safer in local folders. "
            + "; ".join(cloud_matches)
        )
    for label, path in (("Output", output_dir), ("Temp/cache", temp_dir)):
        detail, error = setup_directory_writability(path, label)
        if detail:
            details.append(detail)
        if error:
            errors.append(error)

    return SetupStatus(
        ok=not errors,
        details=tuple(details),
        warnings=tuple(warnings),
        errors=tuple(errors),
    )


# ---------------------------------------------------------------------------
# In-app self-update (download latest default branch from GitHub and replace
# the local files). Designed to be SAFE: HTTPS only, archive path-traversal
# guard, the downloaded code is compile-checked before anything is replaced,
# and every replaced file is backed up first so a bad update can be undone.
# ---------------------------------------------------------------------------
def github_api_request(url: str, timeout: int = GITHUB_UPDATE_TIMEOUT_SECONDS):
    import urllib.request

    request = urllib.request.Request(
        url,
        headers={
            "User-Agent": f"{APP_DISPLAY_NAME}/{APP_VERSION}",
            "Accept": "application/vnd.github+json",
        },
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return json.loads(response.read().decode("utf-8"))


def github_default_branch(repo: str = GITHUB_REPO, timeout: int = GITHUB_UPDATE_TIMEOUT_SECONDS) -> str:
    data = github_api_request(f"{GITHUB_API_BASE}/repos/{repo}", timeout=timeout)
    branch = data.get("default_branch") if isinstance(data, dict) else None
    if not branch:
        raise RuntimeError("Could not determine the repository's default branch.")
    return str(branch)


def download_github_branch_zip(
    repo: str,
    branch: str,
    dest_path: Path,
    timeout: int = GITHUB_UPDATE_TIMEOUT_SECONDS,
) -> Path:
    import urllib.request

    url = f"{GITHUB_API_BASE}/repos/{repo}/zipball/{branch}"
    request = urllib.request.Request(url, headers={"User-Agent": f"{APP_DISPLAY_NAME}/{APP_VERSION}"})
    with urllib.request.urlopen(request, timeout=timeout) as response, open(dest_path, "wb") as handle:
        shutil.copyfileobj(response, handle)
    return dest_path


def safe_extract_zip(zip_path: Path, extract_dir: Path) -> None:
    """Extract a zip while refusing any member that would escape extract_dir."""
    import zipfile

    extract_root = extract_dir.resolve()
    with zipfile.ZipFile(zip_path) as archive:
        for member in archive.namelist():
            target = (extract_dir / member).resolve()
            if target != extract_root and extract_root not in target.parents:
                raise RuntimeError(f"Refusing unsafe path in update archive: {member}")
        archive.extractall(extract_dir)


def find_repo_file(extract_dir: Path, filename: str) -> Path | None:
    matches = sorted(extract_dir.rglob(filename))
    return matches[0] if matches else None


def perform_self_update(
    project_dir: Path,
    log: Callable[[str], None] | None = None,
    repo: str = GITHUB_REPO,
) -> dict:
    """Download the latest default branch and replace local files in place.

    Returns a summary dict. Raises on any failure (with nothing replaced unless
    the compile-check already passed). Every replaced file is backed up under
    project_dir/backups/pre_update_<timestamp>/ first.
    """
    import py_compile
    import tempfile

    def emit(message: str) -> None:
        LOG.info(message)
        if log is not None:
            try:
                log(message)
            except Exception:
                pass

    emit(f"Checking github.com/{repo} for the latest version...")
    branch = github_default_branch(repo)
    emit(f"Default branch is '{branch}'. Downloading...")

    with tempfile.TemporaryDirectory(prefix="himawari_update_") as temp_name:
        temp_dir = Path(temp_name)
        zip_path = temp_dir / "update.zip"
        download_github_branch_zip(repo, branch, zip_path)
        emit(f"Downloaded {zip_path.stat().st_size:,} bytes. Extracting...")

        extract_dir = temp_dir / "extracted"
        extract_dir.mkdir()
        safe_extract_zip(zip_path, extract_dir)

        new_main = find_repo_file(extract_dir, "himawari_lowram_processor.py")
        if new_main is None:
            raise RuntimeError("Downloaded branch does not contain himawari_lowram_processor.py; update aborted.")

        # Verify the freshly downloaded code at least compiles before replacing anything.
        try:
            py_compile.compile(str(new_main), doraise=True)
        except py_compile.PyCompileError as exc:
            raise RuntimeError(f"Downloaded code failed to compile; update aborted. {exc.msg}") from exc

        timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
        backup_dir = project_dir / "backups" / f"pre_update_{timestamp}"
        backup_dir.mkdir(parents=True, exist_ok=True)
        running_file = Path(__file__).resolve()

        updated: list[str] = []
        for filename in SELF_UPDATE_FILES:
            source = find_repo_file(extract_dir, filename)
            if source is None:
                continue
            # The main script overwrites the actually-running file (handles a
            # locally renamed copy); auxiliary files land beside it.
            destination = running_file if filename == "himawari_lowram_processor.py" else (project_dir / filename)
            if destination.exists():
                shutil.copy2(destination, backup_dir / destination.name)
            shutil.copy2(source, destination)
            updated.append(destination.name)
            emit(f"Updated {destination.name}")

        if not updated:
            raise RuntimeError("No updatable files were found in the downloaded branch.")

        emit(f"Backed up replaced files to {backup_dir}")
        return {
            "branch": branch,
            "updated": updated,
            "backup_dir": str(backup_dir),
            "running_file": str(running_file),
        }


class _Tooltip:
    """Lightweight hover tooltip for any Tk widget (beginner help on hover)."""

    def __init__(self, widget, text: str, delay_ms: int = 450, wrap: int = 360):
        self.widget = widget
        self.text = text
        self.delay_ms = delay_ms
        self.wrap = wrap
        self._after_id = None
        self._tip = None
        widget.bind("<Enter>", self._schedule, add="+")
        widget.bind("<Leave>", self._hide, add="+")
        widget.bind("<ButtonPress>", self._hide, add="+")

    def _schedule(self, _event=None):
        self._cancel()
        self._after_id = self.widget.after(self.delay_ms, self._show)

    def _cancel(self):
        if self._after_id is not None:
            try:
                self.widget.after_cancel(self._after_id)
            except Exception:
                pass
            self._after_id = None

    def _show(self):
        if self._tip is not None or not self.text:
            return
        try:
            x = self.widget.winfo_rootx() + 18
            y = self.widget.winfo_rooty() + self.widget.winfo_height() + 6
            self._tip = tk.Toplevel(self.widget)
            self._tip.wm_overrideredirect(True)
            self._tip.wm_geometry(f"+{x}+{y}")
            label = tk.Label(
                self._tip,
                text=self.text,
                justify="left",
                background="#fffbe6",
                foreground="#1d1d1f",
                relief="solid",
                borderwidth=1,
                wraplength=self.wrap,
                padx=8,
                pady=5,
            )
            label.pack()
        except Exception:
            self._tip = None

    def _hide(self, _event=None):
        self._cancel()
        if self._tip is not None:
            try:
                self._tip.destroy()
            except Exception:
                pass
            self._tip = None


class RegionPickerDialog:
    """A simple clickable equirectangular mini-map for choosing flat-map bounds.

    The user drags a rectangle over a graticule that shows the Himawari full-disk
    view; the selection is converted to latitude/longitude and a live pixel and
    RAM estimate is shown. Confirming fills the main window's flat-map bounds and
    switches it to the flat map view. Everything is pure tkinter and maths - no
    external map data is needed.
    """

    LON_MIN = 60.0
    LON_MAX = 220.0
    LAT_MIN = -85.0
    LAT_MAX = 85.0
    CANVAS_W = 720
    CANVAS_H = 460

    def __init__(self, app: "HimawariProcessorApp") -> None:
        self.app = app
        self.root = app.root
        self.window = tk.Toplevel(self.root)
        self.window.title("Pick a region")
        self.window.transient(self.root)
        self._drag_start: tuple[float, float] | None = None
        self._box_id: int | None = None
        self._selection: tuple[float, float, float, float] | None = None
        self._build()
        self._draw_base_map()
        self._show_initial_bounds()

    # -- layout ---------------------------------------------------------
    def _build(self) -> None:
        ttk.Label(
            self.window,
            text="Drag a box over the map to set the flat-map crop. The dashed outline is the Himawari view.",
            wraplength=self.CANVAS_W,
        ).pack(padx=10, pady=(10, 6))
        self.canvas = tk.Canvas(
            self.window,
            width=self.CANVAS_W,
            height=self.CANVAS_H,
            bg="#0b1e3a",
            highlightthickness=1,
            highlightbackground="#3a4a60",
        )
        self.canvas.pack(padx=10)
        self.canvas.bind("<ButtonPress-1>", self._on_press)
        self.canvas.bind("<B1-Motion>", self._on_drag)
        self.canvas.bind("<ButtonRelease-1>", self._on_release)
        self.estimate_var = tk.StringVar(value="Drag to select a region.")
        ttk.Label(self.window, textvariable=self.estimate_var, style="Status.TLabel").pack(padx=10, pady=(6, 4))
        row = ttk.Frame(self.window)
        row.pack(pady=(4, 10))
        self.apply_button = ttk.Button(row, text="Use This Region", command=self._apply, state="disabled")
        self.apply_button.grid(row=0, column=0, padx=(0, 8))
        ttk.Button(row, text="Cancel", command=self.window.destroy).grid(row=0, column=1)

    # -- coordinate helpers --------------------------------------------
    def _lon_to_x(self, lon: float) -> float:
        return (lon - self.LON_MIN) / (self.LON_MAX - self.LON_MIN) * self.CANVAS_W

    def _lat_to_y(self, lat: float) -> float:
        return (self.LAT_MAX - lat) / (self.LAT_MAX - self.LAT_MIN) * self.CANVAS_H

    def _x_to_lon(self, x: float) -> float:
        clamped = max(0.0, min(self.CANVAS_W, x))
        return self.LON_MIN + (clamped / self.CANVAS_W) * (self.LON_MAX - self.LON_MIN)

    def _y_to_lat(self, y: float) -> float:
        clamped = max(0.0, min(self.CANVAS_H, y))
        return self.LAT_MAX - (clamped / self.CANVAS_H) * (self.LAT_MAX - self.LAT_MIN)

    @staticmethod
    def _lon_label(lon: float) -> str:
        value = lon
        if value > 180.0:
            value -= 360.0
        hemi = "E" if value >= 0 else "W"
        return f"{abs(value):g}{hemi}"

    # -- drawing --------------------------------------------------------
    def _draw_base_map(self) -> None:
        canvas = self.canvas
        for lon in range(60, 221, 20):
            x = self._lon_to_x(lon)
            canvas.create_line(x, 0, x, self.CANVAS_H, fill="#16304f")
            canvas.create_text(x, self.CANVAS_H - 8, text=self._lon_label(lon), fill="#7fa8d0", font=("TkDefaultFont", 7))
        for lat in range(-80, 81, 20):
            y = self._lat_to_y(lat)
            canvas.create_line(0, y, self.CANVAS_W, y, fill="#16304f")
            canvas.create_text(16, y, text=f"{lat}", fill="#7fa8d0", font=("TkDefaultFont", 7))
        equator_y = self._lat_to_y(0)
        canvas.create_line(0, equator_y, self.CANVAS_W, equator_y, fill="#274c79")
        x1 = self._lon_to_x(80)
        x2 = self._lon_to_x(200)
        y_top = self._lat_to_y(81)
        y_bottom = self._lat_to_y(-81)
        canvas.create_rectangle(x1, y_top, x2, y_bottom, outline="#5fd0ff", dash=(4, 3))
        canvas.create_text((x1 + x2) / 2, y_top + 12, text="Himawari full-disk view", fill="#5fd0ff", font=("TkDefaultFont", 8))
        sub_x = self._lon_to_x(AHI_SUB_SATELLITE_LON_DEG)
        canvas.create_line(sub_x, equator_y - 6, sub_x, equator_y + 6, fill="#ffd23f")
        canvas.create_line(sub_x - 6, equator_y, sub_x + 6, equator_y, fill="#ffd23f")

    def _show_initial_bounds(self) -> None:
        try:
            min_lat = float(self.app.flat_min_lat_var.get())
            max_lat = float(self.app.flat_max_lat_var.get())
            min_lon = float(self.app.flat_min_lon_var.get())
            max_lon = float(self.app.flat_max_lon_var.get())
        except Exception:
            return
        if min_lat >= max_lat or min_lon >= max_lon:
            return
        self._selection = (min_lat, max_lat, min_lon, max_lon)
        self._draw_selection_box()
        self._update_estimate()
        self.apply_button.configure(state="normal")

    def _draw_selection_box(self) -> None:
        if self._selection is None:
            return
        min_lat, max_lat, min_lon, max_lon = self._selection
        x1 = self._lon_to_x(min_lon)
        x2 = self._lon_to_x(max_lon)
        y1 = self._lat_to_y(max_lat)
        y2 = self._lat_to_y(min_lat)
        if self._box_id is not None:
            self.canvas.delete(self._box_id)
        self._box_id = self.canvas.create_rectangle(x1, y1, x2, y2, outline="#ff7ad0", width=2)

    # -- interaction ----------------------------------------------------
    def _on_press(self, event: tk.Event) -> None:
        self._drag_start = (event.x, event.y)
        if self._box_id is not None:
            self.canvas.delete(self._box_id)
            self._box_id = None

    def _on_drag(self, event: tk.Event) -> None:
        if self._drag_start is None:
            return
        x0, y0 = self._drag_start
        if self._box_id is not None:
            self.canvas.delete(self._box_id)
        self._box_id = self.canvas.create_rectangle(x0, y0, event.x, event.y, outline="#ff7ad0", width=2)

    def _on_release(self, event: tk.Event) -> None:
        if self._drag_start is None:
            return
        x0, y0 = self._drag_start
        self._drag_start = None
        if abs(event.x - x0) < 4 or abs(event.y - y0) < 4:
            self.estimate_var.set("Selection too small - drag a larger box.")
            return
        lon_a = self._x_to_lon(x0)
        lon_b = self._x_to_lon(event.x)
        lat_a = self._y_to_lat(y0)
        lat_b = self._y_to_lat(event.y)
        min_lon, max_lon = sorted((lon_a, lon_b))
        min_lat, max_lat = sorted((lat_a, lat_b))
        max_lat = min(max_lat, WEB_MERCATOR_MAX_LAT)
        min_lat = max(min_lat, -WEB_MERCATOR_MAX_LAT)
        self._selection = (round(min_lat, 3), round(max_lat, 3), round(min_lon, 3), round(max_lon, 3))
        self._draw_selection_box()
        self._update_estimate()
        self.apply_button.configure(state="normal")

    def _update_estimate(self) -> None:
        if self._selection is None:
            return
        min_lat, max_lat, min_lon, max_lon = self._selection
        try:
            resolution = float(self.app.flat_resolution_var.get())
        except Exception:
            resolution = FLAT_RESOLUTION_DEG
        text = (
            f"lat {min_lat:g}..{max_lat:g}, lon {min_lon:g}..{max_lon:g}"
        )
        try:
            height, width = flat_map_shape(min_lat, max_lat, min_lon, max_lon, resolution)
            pixels = width * height
            peak_mb = pixels * 16 / 1.0e6
            text += f"   ->  {width:,} x {height:,} px  (~{peak_mb:,.0f} MB peak, rough)"
            if pixels > MAX_FLAT_MAP_PIXELS:
                text += f"   TOO LARGE: over {MAX_FLAT_MAP_PIXELS:,} px; coarsen resolution or shrink the box."
        except Exception as exc:
            text += f"   (size estimate unavailable: {exc})"
        self.estimate_var.set(text)

    def _apply(self) -> None:
        if self._selection is None:
            self.window.destroy()
            return
        min_lat, max_lat, min_lon, max_lon = self._selection
        self.app.flat_min_lat_var.set(str(min_lat))
        self.app.flat_max_lat_var.set(str(max_lat))
        self.app.flat_min_lon_var.set(str(min_lon))
        self.app.flat_max_lon_var.set(str(max_lon))
        self.app.map_view_var.set("flat")
        self.app.area_preset_var.set(AREA_PRESET_CUSTOM)
        self.app._append_log(
            f"Region picker set flat map bounds: lat {min_lat:g}..{max_lat:g}, lon {min_lon:g}..{max_lon:g}."
        )
        self.app._update_setup_status()
        self.app._write_current_settings()
        self.window.destroy()


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
        self.last_log_path: Path | None = None
        self.last_error_report = ""
        self.run_started_at_utc = ""
        self.progress_eta_estimator = ProgressEtaEstimator()
        self.recent_runs = load_recent_runs()
        self.custom_presets = load_custom_presets()
        self.preview_image: object | None = None

        self.ui_mode_var = tk.StringVar(value="Advanced")
        self.url_var = tk.StringVar(value=initial_config.user_url)
        self.mode_var = tk.StringVar(value=initial_config.mode)
        self.composite_var = tk.StringVar(value=initial_config.composite_choice)
        self.satellite_layer_var = tk.StringVar(value=initial_config.satellite_layer_mode)
        self.hours_var = tk.StringVar(value=str(initial_config.hours_back))
        self.interval_var = tk.StringVar(value=str(initial_config.interval_minutes))
        self.fps_var = tk.StringVar(value=str(initial_config.fps))
        self.download_workers_var = tk.StringVar(value=str(initial_config.download_workers))
        self.dask_workers_var = tk.StringVar(value=str(initial_config.dask_num_workers))
        self.chunk_var = tk.StringVar(value=initial_config.dask_chunk_size)
        self.ram_limit_var = tk.StringVar(value=str(initial_config.ram_limit_gb))
        self.image_format_var = tk.StringVar(value=initial_config.image_format)
        self.output_template_var = tk.StringVar(value=initial_config.output_template)
        self.resampler_var = tk.StringVar(value=initial_config.resampler)
        self.night_fallback_mode_var = tk.StringVar(value=initial_config.night_fallback_mode)
        self.map_view_var = tk.StringVar(value=initial_config.map_view)
        self.area_preset_var = tk.StringVar(value=AREA_PRESET_CUSTOM)
        self.flat_min_lat_var = tk.StringVar(value=str(initial_config.flat_min_lat))
        self.flat_max_lat_var = tk.StringVar(value=str(initial_config.flat_max_lat))
        self.flat_min_lon_var = tk.StringVar(value=str(initial_config.flat_min_lon))
        self.flat_max_lon_var = tk.StringVar(value=str(initial_config.flat_max_lon))
        self.flat_resolution_var = tk.StringVar(value=str(initial_config.flat_resolution_deg))
        self.timelapse_format_var = tk.StringVar(value=initial_config.timelapse_format)
        self.auto_download_var = tk.BooleanVar(value=initial_config.auto_download)
        self.gpu_acceleration_var = tk.BooleanVar(value=initial_config.gpu_acceleration)
        self.night_fallback_var = tk.BooleanVar(value=initial_config.use_night_fallback)
        self.delete_frames_var = tk.BooleanVar(value=initial_config.delete_timelapse_frames)
        self.quality_fallback_var = tk.BooleanVar(value=initial_config.allow_quality_fallback)
        self.border_lines_var = tk.BooleanVar(value=initial_config.add_border_lines)
        self.border_color_var = tk.StringVar(value=initial_config.border_line_color)
        self.border_width_var = tk.StringVar(value=str(initial_config.border_line_width))
        self.map_labels_var = tk.BooleanVar(value=initial_config.add_map_labels)
        self.map_label_size_var = tk.StringVar(value=str(initial_config.map_label_size))
        self.night_boundary_var = tk.BooleanVar(value=initial_config.add_night_boundary)
        self.crosshair_var = tk.BooleanVar(value=initial_config.add_crosshair)
        self.crosshair_type_var = tk.StringVar(value=initial_config.crosshair_type)
        self.crosshair_color_var = tk.StringVar(value=initial_config.crosshair_color)
        self.zoom_earth_style_var = tk.BooleanVar(value=initial_config.zoom_earth_style)
        self.segment_aware_var = tk.BooleanVar(value=initial_config.segment_aware_downloads)
        self.write_sidecar_var = tk.BooleanVar(value=initial_config.write_metadata_sidecar)
        self.overlay_theme_var = tk.StringVar(value=initial_config.overlay_theme)
        self.map_label_color_var = tk.StringVar(value=initial_config.map_label_color)
        self.night_boundary_color_var = tk.StringVar(value=initial_config.night_boundary_color)
        self.status_var = tk.StringVar(value="Ready")
        self.progress_var = tk.DoubleVar(value=0)
        self.setup_status_var = tk.StringVar(value="")
        self.phase_var = tk.StringVar(value="Ready")
        self.run_summary_var = tk.StringVar(value="")
        self.selected_recent_run_id = ""
        self.output_dir_var = tk.StringVar(value=str(OUTPUT_DIR))
        self.temp_dir_var = tk.StringVar(value=str(TEMP_DIR))
        self.pending_log_messages: list[str] = []

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
    def _initial_split_position(total_height: int, ratio: float = 0.5) -> int:
        if total_height <= 0:
            return 220
        minimum_pane_height = 160
        if total_height <= minimum_pane_height * 2:
            return max(80, total_height // 2)
        desired = int(total_height * ratio)
        return max(minimum_pane_height, min(desired, total_height - minimum_pane_height))

    def _configure_main_split(self) -> None:
        def set_initial_sash(attempt: int = 0) -> None:
            height = self.main_pane.winfo_height()
            if height < 360 and attempt < 20:
                self.root.after(50, lambda: set_initial_sash(attempt + 1))
                return
            self.main_pane.sashpos(0, self._initial_split_position(height))

        self.root.after_idle(set_initial_sash)

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
        recent = self._create_scrollable_tab(self.notebook, "Recent Runs")
        self.advanced_tab_id = self.notebook.tabs()[1]

        for col in (0, 1):
            settings.columnconfigure(col, weight=1)
            advanced.columnconfigure(col, weight=1)
            recent.columnconfigure(col, weight=1)

        source_frame = ttk.LabelFrame(settings, text="Source", style="Section.TLabelframe")
        source_frame.grid(row=0, column=0, columnspan=2, sticky="ew", pady=(0, 10))
        source_frame.columnconfigure(0, weight=1)
        ttk.Label(source_frame, text="NOAA AWS Himawari URL").grid(row=0, column=0, sticky="w")
        ttk.Entry(source_frame, textvariable=self.url_var).grid(row=1, column=0, sticky="ew", pady=(2, 0))
        self.latest_url_button = ttk.Button(source_frame, text="Latest FLDK", command=self._fill_latest_fldk_url)
        self.latest_url_button.grid(row=1, column=1, sticky="ew", padx=(8, 0), pady=(2, 0))
        self.scan_browser_button = ttk.Button(source_frame, text="Choose Scan", command=self._open_scan_browser)
        self.scan_browser_button.grid(row=1, column=2, sticky="ew", padx=(8, 0), pady=(2, 0))
        self.local_files_button = ttk.Button(source_frame, text="Local Files...", command=self._choose_local_hsd_files)
        self.local_files_button.grid(row=1, column=3, sticky="ew", padx=(8, 0), pady=(2, 0))
        self.local_drop_label = ttk.Label(
            source_frame,
            text="Local offline import: choose or drop .DAT / .DAT.bz2 segment files.",
            style="Status.TLabel",
        )
        self.local_drop_label.grid(row=2, column=0, columnspan=4, sticky="ew", pady=(8, 0))
        ttk.Label(source_frame, text="Satellite Layer").grid(row=3, column=0, sticky="w", pady=(8, 0))
        self.satellite_layer_box = ttk.Combobox(
            source_frame,
            textvariable=self.satellite_layer_var,
            values=SATELLITE_LAYER_MODES,
            state="readonly",
        )
        self.satellite_layer_box.grid(row=4, column=0, sticky="ew", pady=(2, 0))
        self.satellite_layer_box.bind("<<ComboboxSelected>>", lambda _event: self._apply_satellite_layer_defaults())
        self._setup_optional_local_drop_target()

        product_frame = ttk.LabelFrame(settings, text="Product", style="Section.TLabelframe")
        product_frame.grid(row=1, column=0, sticky="nsew", padx=(0, 8), pady=(0, 10))
        product_frame.columnconfigure(0, weight=1)
        product_frame.columnconfigure(1, weight=1)
        ttk.Label(product_frame, text="Safe Preset").grid(row=0, column=0, sticky="w")
        preset_box = ttk.Combobox(
            product_frame,
            values=BUILT_IN_PRESETS,
            state="readonly",
        )
        self.preset_box = preset_box
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
        ttk.Label(options_frame, text="Night Fallback Mode").grid(row=1, column=1, sticky="w", pady=(4, 4))
        ttk.Combobox(
            options_frame,
            textvariable=self.night_fallback_mode_var,
            values=("hybrid", "whole_frame_ir"),
            state="readonly",
        ).grid(row=1, column=1, sticky="e", padx=(150, 8), pady=(4, 4))
        ttk.Checkbutton(
            options_frame,
            text="Allow lower-quality fallback if true color dependencies are missing",
            variable=self.quality_fallback_var,
        ).grid(row=1, column=0, sticky="w", pady=(4, 4))
        ttk.Checkbutton(
            options_frame,
            text="Draw coastline and country border lines",
            variable=self.border_lines_var,
        ).grid(row=2, column=0, sticky="w", pady=(4, 4))
        self.overlay_check_button = ttk.Button(options_frame, text="Check Overlay Setup", command=self._check_overlays)
        self.overlay_check_button.grid(row=2, column=1, sticky="e", padx=(0, 8), pady=(4, 4))
        ttk.Checkbutton(
            options_frame,
            text="Zoom Earth-style flat map",
            variable=self.zoom_earth_style_var,
            command=self._on_zoom_earth_toggle,
        ).grid(row=3, column=0, sticky="w", pady=(4, 4))
        labels_row = ttk.Frame(options_frame)
        labels_row.grid(row=3, column=1, sticky="ew", pady=(4, 4))
        labels_row.columnconfigure(2, weight=1)
        ttk.Checkbutton(
            labels_row,
            text="Labels",
            variable=self.map_labels_var,
        ).grid(row=0, column=0, sticky="w")
        ttk.Label(labels_row, text="Size").grid(row=0, column=1, sticky="w", padx=(14, 4))
        ttk.Spinbox(
            labels_row,
            from_=MAP_LABEL_SIZE_MIN,
            to=MAP_LABEL_SIZE_MAX,
            increment=1,
            textvariable=self.map_label_size_var,
            width=5,
        ).grid(row=0, column=2, sticky="w")
        ttk.Checkbutton(
            options_frame,
            text="Night Boundary",
            variable=self.night_boundary_var,
        ).grid(row=4, column=0, sticky="w", pady=(4, 4))
        ttk.Checkbutton(
            options_frame,
            text="Crosshair",
            variable=self.crosshair_var,
        ).grid(row=4, column=1, sticky="w", pady=(4, 4))
        ttk.Label(options_frame, text="Crosshair Type").grid(row=5, column=0, sticky="w", pady=(4, 4))
        ttk.Combobox(
            options_frame,
            textvariable=self.crosshair_type_var,
            values=CROSSHAIR_TYPES,
            state="readonly",
        ).grid(row=6, column=0, sticky="ew", pady=(2, 0), padx=(0, 8))
        ttk.Label(options_frame, text="Crosshair Color").grid(row=5, column=1, sticky="w", padx=(0, 8), pady=(4, 4))
        crosshair_color_row = ttk.Frame(options_frame)
        crosshair_color_row.grid(row=6, column=1, sticky="ew")
        crosshair_color_row.columnconfigure(0, weight=1)
        ttk.Entry(crosshair_color_row, textvariable=self.crosshair_color_var, width=14).grid(row=0, column=0, sticky="ew")
        ttk.Button(crosshair_color_row, text="Pick", command=self._choose_crosshair_color).grid(row=0, column=1, padx=(8, 0))
        ttk.Label(
            options_frame,
            text="Unavailable map feeds: " + ", ".join(UNSUPPORTED_MAP_OVERLAYS),
            style="Status.TLabel",
            wraplength=760,
        ).grid(row=7, column=0, columnspan=2, sticky="ew", pady=(4, 4))
        ttk.Label(options_frame, text="Border Color").grid(row=8, column=1, sticky="w", padx=(0, 8))
        color_row = ttk.Frame(options_frame)
        color_row.grid(row=9, column=1, sticky="ew")
        color_row.columnconfigure(0, weight=1)
        ttk.Entry(color_row, textvariable=self.border_color_var, width=14).grid(row=0, column=0, sticky="ew")
        ttk.Button(color_row, text="Pick", command=self._choose_border_color).grid(row=0, column=1, padx=(8, 0))
        ttk.Label(options_frame, text="Border Width").grid(row=8, column=0, sticky="w", pady=(4, 4))
        ttk.Spinbox(
            options_frame,
            from_=0.25,
            to=5.0,
            increment=0.25,
            textvariable=self.border_width_var,
            width=8,
        ).grid(row=9, column=0, sticky="w", pady=(2, 0))

        ttk.Separator(options_frame, orient="horizontal").grid(
            row=10, column=0, columnspan=2, sticky="ew", pady=(8, 6)
        )
        ttk.Label(options_frame, text="Overlay Theme").grid(row=11, column=0, sticky="w", pady=(2, 4))
        self.overlay_theme_box = ttk.Combobox(
            options_frame,
            textvariable=self.overlay_theme_var,
            values=list(OVERLAY_THEME_ORDER),
            state="readonly",
        )
        self.overlay_theme_box.grid(row=11, column=0, sticky="ew", padx=(110, 8), pady=(2, 4))
        self.overlay_theme_box.bind("<<ComboboxSelected>>", lambda _event: self._on_overlay_theme_change())
        ttk.Label(options_frame, text="Label Color").grid(row=11, column=1, sticky="w", padx=(0, 8), pady=(2, 4))
        label_color_row = ttk.Frame(options_frame)
        label_color_row.grid(row=12, column=1, sticky="ew", pady=(0, 4))
        label_color_row.columnconfigure(0, weight=1)
        ttk.Entry(label_color_row, textvariable=self.map_label_color_var, width=14).grid(row=0, column=0, sticky="ew")
        ttk.Button(
            label_color_row,
            text="Pick",
            command=lambda: self._choose_color(self.map_label_color_var, "Map label color", MAP_LABEL_COLOR),
        ).grid(row=0, column=1, padx=(8, 0))
        ttk.Label(options_frame, text="Night Line Color").grid(row=12, column=0, sticky="w", pady=(0, 4))
        night_color_row = ttk.Frame(options_frame)
        night_color_row.grid(row=13, column=0, sticky="ew", pady=(0, 4))
        night_color_row.columnconfigure(0, weight=1)
        ttk.Entry(night_color_row, textvariable=self.night_boundary_color_var, width=14).grid(row=0, column=0, sticky="ew")
        ttk.Button(
            night_color_row,
            text="Pick",
            command=lambda: self._choose_color(
                self.night_boundary_color_var, "Night boundary color", NIGHT_BOUNDARY_COLOR
            ),
        ).grid(row=0, column=1, padx=(8, 0))
        ttk.Checkbutton(
            options_frame,
            text="Segment-aware downloads (regional crops skip unused scan segments)",
            variable=self.segment_aware_var,
        ).grid(row=14, column=0, columnspan=2, sticky="w", pady=(6, 2))
        ttk.Checkbutton(
            options_frame,
            text="Write a metadata sidecar (.json + world file) next to each image",
            variable=self.write_sidecar_var,
        ).grid(row=15, column=0, columnspan=2, sticky="w", pady=(2, 2))

        simple_frame = ttk.LabelFrame(settings, text="Simple View", style="Section.TLabelframe")
        simple_frame.grid(row=3, column=0, columnspan=2, sticky="ew", pady=(10, 0))
        simple_frame.columnconfigure(0, weight=1)
        simple_frame.columnconfigure(1, weight=1)
        ttk.Label(simple_frame, text="View Mode").grid(row=0, column=0, sticky="w")
        self.ui_mode_box = ttk.Combobox(
            simple_frame,
            textvariable=self.ui_mode_var,
            values=("Simple", "Advanced"),
            state="readonly",
        )
        self.ui_mode_box.grid(row=1, column=0, sticky="ew", padx=(0, 8), pady=(2, 8))
        self.ui_mode_box.bind("<<ComboboxSelected>>", lambda _event: self._refresh_ui_mode())
        ttk.Label(simple_frame, text="Output Folder").grid(row=0, column=1, sticky="w")
        ttk.Entry(simple_frame, textvariable=self.output_dir_var, state="readonly").grid(
            row=1, column=1, sticky="ew", pady=(2, 8)
        )
        self.simple_output_button = ttk.Button(
            simple_frame,
            text="Choose Output Folder",
            command=self._choose_output_dir,
        )
        self.simple_output_button.grid(row=2, column=1, sticky="ew", pady=(2, 0))
        ttk.Label(simple_frame, text="Run Summary").grid(row=2, column=0, sticky="w", padx=(0, 8))
        ttk.Label(
            simple_frame,
            textvariable=self.run_summary_var,
            style="Status.TLabel",
            wraplength=560,
        ).grid(row=3, column=0, columnspan=2, sticky="ew", pady=(2, 0))

        status_frame = ttk.LabelFrame(settings, text="Setup Status", style="Section.TLabelframe")
        status_frame.grid(row=4, column=0, columnspan=2, sticky="ew", pady=(10, 0))
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
            values=DASK_CHUNK_CHOICES,
            state="readonly",
        ).grid(row=3, column=0, sticky="ew", padx=(0, 8), pady=(2, 0))
        ttk.Label(performance_frame, text="RAM Limit GiB").grid(row=2, column=1, sticky="w")
        ttk.Spinbox(performance_frame, from_=1, to=64, increment=0.5, textvariable=self.ram_limit_var, width=8).grid(
            row=3, column=1, sticky="ew", pady=(2, 0)
        )
        self.safe_perf_button = ttk.Button(
            performance_frame,
            text="Safe Mode",
            command=lambda: self._apply_performance_recommendation("safe"),
        )
        self.safe_perf_button.grid(row=4, column=0, sticky="ew", padx=(0, 8), pady=(10, 0))
        self.best_perf_button = ttk.Button(
            performance_frame,
            text="Best Performance",
            command=lambda: self._apply_performance_recommendation("best_performance"),
        )
        self.best_perf_button.grid(row=4, column=1, sticky="ew", pady=(10, 0))
        self.gpu_check = ttk.Checkbutton(
            performance_frame,
            text="Use GPU (Experimental)",
            variable=self.gpu_acceleration_var,
            command=self._toggle_gpu_acceleration,
        )
        self.gpu_check.grid(row=5, column=0, sticky="w", pady=(10, 0))
        self.gpu_fix_button = ttk.Button(
            performance_frame,
            text="GPU Fix",
            command=self._open_gpu_environment_fix,
        )
        self.gpu_fix_button.grid(row=5, column=1, sticky="ew", pady=(10, 0))

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

        ttk.Label(paths_frame, text="Map View").grid(row=2, column=0, sticky="w")
        self.map_view_box = ttk.Combobox(
            paths_frame,
            textvariable=self.map_view_var,
            values=("native", "flat"),
            state="readonly",
        )
        self.map_view_box.grid(row=3, column=0, sticky="ew", padx=(0, 8), pady=(2, 8))
        ttk.Label(paths_frame, text="Flat Resolution Deg").grid(row=2, column=1, sticky="w")
        ttk.Entry(paths_frame, textvariable=self.flat_resolution_var).grid(
            row=3, column=1, sticky="ew", pady=(2, 8)
        )

        ttk.Label(paths_frame, text="Flat Min/Max Lat").grid(row=4, column=0, sticky="w")
        flat_lat_row = ttk.Frame(paths_frame)
        flat_lat_row.grid(row=5, column=0, sticky="ew", padx=(0, 8), pady=(2, 8))
        flat_lat_row.columnconfigure(0, weight=1)
        flat_lat_row.columnconfigure(1, weight=1)
        ttk.Entry(flat_lat_row, textvariable=self.flat_min_lat_var, width=8).grid(row=0, column=0, sticky="ew")
        ttk.Entry(flat_lat_row, textvariable=self.flat_max_lat_var, width=8).grid(
            row=0, column=1, sticky="ew", padx=(8, 0)
        )
        ttk.Label(paths_frame, text="Flat Min/Max Lon").grid(row=4, column=1, sticky="w")
        flat_lon_row = ttk.Frame(paths_frame)
        flat_lon_row.grid(row=5, column=1, sticky="ew", pady=(2, 8))
        flat_lon_row.columnconfigure(0, weight=1)
        flat_lon_row.columnconfigure(1, weight=1)
        ttk.Entry(flat_lon_row, textvariable=self.flat_min_lon_var, width=8).grid(row=0, column=0, sticky="ew")
        ttk.Entry(flat_lon_row, textvariable=self.flat_max_lon_var, width=8).grid(
            row=0, column=1, sticky="ew", padx=(8, 0)
        )

        ttk.Label(paths_frame, text="Output Filename Template").grid(row=6, column=0, columnspan=2, sticky="w")
        ttk.Entry(paths_frame, textvariable=self.output_template_var).grid(
            row=7, column=0, columnspan=2, sticky="ew", pady=(2, 8)
        )

        ttk.Label(paths_frame, text="Output Folder").grid(row=8, column=0, columnspan=2, sticky="w")
        ttk.Entry(paths_frame, textvariable=self.output_dir_var, state="readonly").grid(
            row=9, column=0, columnspan=2, sticky="ew", pady=(2, 8)
        )
        ttk.Label(paths_frame, text="Temp Folder").grid(row=10, column=0, columnspan=2, sticky="w")
        ttk.Entry(paths_frame, textvariable=self.temp_dir_var, state="readonly").grid(
            row=11, column=0, columnspan=2, sticky="ew", pady=(2, 8)
        )
        self.choose_output_button = ttk.Button(
            paths_frame,
            text="Choose Output Folder",
            command=self._choose_output_dir,
        )
        self.choose_output_button.grid(row=12, column=0, sticky="ew", padx=(0, 8), pady=(2, 8))
        self.choose_temp_button = ttk.Button(
            paths_frame,
            text="Choose Temp Folder",
            command=self._choose_temp_dir,
        )
        self.choose_temp_button.grid(row=12, column=1, sticky="ew", pady=(2, 8))
        self.open_temp_button = ttk.Button(
            paths_frame,
            text="Open Temp Folder",
            command=self._open_temp_folder,
        )
        self.open_temp_button.grid(row=13, column=0, columnspan=2, sticky="ew", pady=(2, 0))

        preset_frame = ttk.LabelFrame(advanced, text="Custom Presets", style="Section.TLabelframe")
        preset_frame.grid(row=2, column=0, columnspan=2, sticky="ew", pady=(10, 0))
        preset_frame.columnconfigure(0, weight=1)
        self.custom_preset_var = tk.StringVar(value="")
        self.custom_preset_box = ttk.Combobox(
            preset_frame,
            textvariable=self.custom_preset_var,
            values=tuple(self.custom_presets),
        )
        self.custom_preset_box.grid(row=0, column=0, sticky="ew", padx=(0, 8))
        ttk.Button(preset_frame, text="Load", command=self._load_custom_preset).grid(row=0, column=1, padx=(0, 8))
        ttk.Button(preset_frame, text="Save Current", command=self._save_custom_preset).grid(
            row=0, column=2, padx=(0, 8)
        )
        ttk.Button(preset_frame, text="Delete", command=self._delete_custom_preset).grid(row=0, column=3)

        area_frame = ttk.LabelFrame(advanced, text="Output Region (Area Preset)", style="Section.TLabelframe")
        area_frame.grid(row=1, column=0, columnspan=2, sticky="ew")
        area_frame.columnconfigure(0, weight=1)
        ttk.Label(
            area_frame,
            text="Pick a region to frame the output. Regional presets and 'Full Disk (flat map)' "
            "switch to the flat map and fill the bounds; 'Full Disk (native)' uses the round-Earth "
            "view (and turns off Zoom Earth styling, which is flat-only).",
            style="Status.TLabel",
            wraplength=520,
            justify="left",
        ).grid(row=0, column=0, columnspan=2, sticky="ew", pady=(0, 4))
        self.area_preset_box = ttk.Combobox(
            area_frame,
            textvariable=self.area_preset_var,
            values=area_preset_names(),
            state="readonly",
        )
        self.area_preset_box.grid(row=1, column=0, sticky="ew", padx=(0, 8))
        self.area_preset_box.bind("<<ComboboxSelected>>", lambda _event: self._apply_area_preset())
        ttk.Button(area_frame, text="Apply Region", command=self._apply_area_preset).grid(row=1, column=1)
        self._refresh_path_fields()
        self._path_controls = (
            self.choose_output_button,
            self.choose_temp_button,
            self.open_temp_button,
            self.simple_output_button,
        )
        self._build_recent_runs_tab(recent)
        self._refresh_custom_preset_box()
        self._refresh_recent_runs()
        self._refresh_ui_mode()
        self.notebook.select(0)

        self.log_frame = ttk.Frame(self.main_pane, padding=(0, 4, 0, 0))
        self.log_frame.columnconfigure(0, weight=1)
        self.log_frame.rowconfigure(1, weight=1)
        self.main_pane.add(self.log_frame, weight=1)

        progress = ttk.Progressbar(self.log_frame, variable=self.progress_var, maximum=100)
        progress.grid(row=0, column=0, sticky="ew", pady=(0, 6))
        ttk.Label(self.log_frame, textvariable=self.status_var).grid(row=0, column=1, sticky="e", padx=(10, 0))
        ttk.Label(self.log_frame, textvariable=self.phase_var).grid(
            row=0,
            column=2,
            sticky="e",
            padx=(10, 0),
        )

        self.log_text = tk.Text(self.log_frame, height=16, wrap="word", state="disabled")
        self.log_text.grid(row=1, column=0, columnspan=3, sticky="nsew")
        scroll = ttk.Scrollbar(self.log_frame, orient="vertical", command=self.log_text.yview)
        scroll.grid(row=1, column=3, sticky="ns")
        self.log_text.configure(yscrollcommand=scroll.set)
        self._flush_pending_log_messages()

        buttons = ttk.Frame(self.root, padding=(16, 0, 16, 16))
        buttons.grid(row=2, column=0, sticky="ew")
        buttons.columnconfigure(0, weight=1)
        buttons.columnconfigure(8, weight=1)
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
            row=1, column=1, padx=(0, 8), pady=(8, 0)
        )
        self.quick_fix_button = ttk.Button(buttons, text="Quick Fix", command=self._open_environment_fix)
        self.quick_fix_button.grid(
            row=1, column=2, padx=(0, 8), pady=(8, 0)
        )
        self.auto_fix_button = ttk.Button(buttons, text="Auto Fix", command=self._open_environment_auto_fix)
        self.auto_fix_button.grid(row=1, column=3, padx=(0, 8), pady=(8, 0))
        self.open_last_button = ttk.Button(buttons, text="Open Last", command=self._open_last_output)
        self.open_last_button.grid(row=0, column=4, padx=(0, 8))
        self.copy_paths_button = ttk.Button(buttons, text="Copy Paths", command=self._copy_output_paths)
        self.copy_paths_button.grid(row=0, column=5, padx=(0, 8))
        self.copy_error_button = ttk.Button(buttons, text="Copy Error", command=self._copy_error_report)
        self.copy_error_button.grid(row=0, column=6, padx=(0, 8))
        self.copy_log_button = ttk.Button(buttons, text="Copy Log", command=self._copy_current_log)
        self.copy_log_button.grid(row=0, column=7, padx=(0, 8))
        ttk.Button(buttons, text="Close", command=self._terminate_app).grid(row=0, column=9)
        self.copy_settings_button = ttk.Button(buttons, text="Copy Settings", command=self._copy_last_settings)
        self.copy_settings_button.grid(row=1, column=4, padx=(0, 8), pady=(8, 0))
        self.update_app_button = ttk.Button(buttons, text="Update App", command=self._update_from_github)
        self.update_app_button.grid(row=1, column=5, padx=(0, 8), pady=(8, 0))
        self.help_button = ttk.Button(buttons, text="Help (?)", command=self._open_help_window)
        self.help_button.grid(row=1, column=6, padx=(0, 8), pady=(8, 0))
        self.preview_button = ttk.Button(buttons, text="Quick Look", command=self._start_preview)
        self.preview_button.grid(row=1, column=7, padx=(0, 8), pady=(8, 0))
        self.pick_region_button = ttk.Button(buttons, text="Pick Region", command=self._open_region_picker)
        self.pick_region_button.grid(row=2, column=1, padx=(0, 8), pady=(8, 0))
        self.test_host_button = ttk.Button(buttons, text="Test Data Host", command=self._start_connectivity_check)
        self.test_host_button.grid(row=2, column=2, padx=(0, 8), pady=(8, 0))
        self._install_tooltips()
        self._refresh_mode_state()
        self._configure_main_split()

    def _build_recent_runs_tab(self, recent: ttk.Frame) -> None:
        recent.rowconfigure(0, weight=1)
        recent.columnconfigure(0, weight=1)
        recent.columnconfigure(1, weight=1)

        list_frame = ttk.LabelFrame(recent, text="Run History", style="Section.TLabelframe")
        list_frame.grid(row=0, column=0, sticky="nsew", padx=(0, 8), pady=(0, 10))
        list_frame.rowconfigure(0, weight=1)
        list_frame.columnconfigure(0, weight=1)

        columns = ("started", "status", "mode", "product", "output")
        self.recent_tree = ttk.Treeview(list_frame, columns=columns, show="headings", height=10)
        headings = {
            "started": "Started",
            "status": "Status",
            "mode": "Mode",
            "product": "Product",
            "output": "Main Output",
        }
        widths = {"started": 140, "status": 80, "mode": 95, "product": 190, "output": 260}
        for column in columns:
            self.recent_tree.heading(column, text=headings[column])
            self.recent_tree.column(column, width=widths[column], anchor="w")
        self.recent_tree.grid(row=0, column=0, sticky="nsew")
        recent_scroll = ttk.Scrollbar(list_frame, orient="vertical", command=self.recent_tree.yview)
        recent_scroll.grid(row=0, column=1, sticky="ns")
        self.recent_tree.configure(yscrollcommand=recent_scroll.set)
        self.recent_tree.bind("<<TreeviewSelect>>", lambda _event: self._select_recent_run())

        actions = ttk.Frame(list_frame)
        actions.grid(row=1, column=0, columnspan=2, sticky="ew", pady=(8, 0))
        for column in range(4):
            actions.columnconfigure(column, weight=1)
        ttk.Button(actions, text="Open Output", command=self._open_selected_recent_output).grid(
            row=0, column=0, sticky="ew", padx=(0, 6), pady=(0, 6)
        )
        ttk.Button(actions, text="Open Folder", command=self._open_selected_recent_folder).grid(
            row=0, column=1, sticky="ew", padx=(0, 6), pady=(0, 6)
        )
        ttk.Button(actions, text="Copy Paths", command=self._copy_selected_recent_paths).grid(
            row=0, column=2, sticky="ew", padx=(0, 6), pady=(0, 6)
        )
        ttk.Button(actions, text="Copy Error", command=self._copy_selected_recent_error).grid(
            row=0, column=3, sticky="ew", pady=(0, 6)
        )
        ttk.Button(actions, text="Open Log", command=self._open_selected_recent_log).grid(
            row=1, column=0, sticky="ew", padx=(0, 6)
        )
        ttk.Button(actions, text="Re-run Settings", command=self._rerun_selected_recent_settings).grid(
            row=1, column=1, sticky="ew", padx=(0, 6)
        )
        ttk.Button(actions, text="Refresh", command=self._refresh_recent_runs).grid(
            row=1, column=2, sticky="ew", padx=(0, 6)
        )
        ttk.Button(actions, text="Clear History", command=self._clear_recent_runs).grid(row=1, column=3, sticky="ew")

        detail_frame = ttk.LabelFrame(recent, text="Details and Preview", style="Section.TLabelframe")
        detail_frame.grid(row=0, column=1, sticky="nsew", padx=(8, 0), pady=(0, 10))
        detail_frame.rowconfigure(0, weight=1)
        detail_frame.columnconfigure(0, weight=1)
        self.recent_detail_text = tk.Text(detail_frame, height=12, wrap="word", state="disabled")
        self.recent_detail_text.grid(row=0, column=0, sticky="nsew")
        detail_scroll = ttk.Scrollbar(detail_frame, orient="vertical", command=self.recent_detail_text.yview)
        detail_scroll.grid(row=0, column=1, sticky="ns")
        self.recent_detail_text.configure(yscrollcommand=detail_scroll.set)
        self.preview_label = ttk.Label(detail_frame, text="Select a recent run to preview output.")
        self.preview_label.grid(row=1, column=0, columnspan=2, sticky="ew", pady=(8, 0))

    def _set_text_widget(self, widget: tk.Text, text: str) -> None:
        widget.configure(state="normal")
        widget.delete("1.0", "end")
        widget.insert("end", text)
        widget.configure(state="disabled")

    def _refresh_recent_runs(self) -> None:
        self.recent_runs = load_recent_runs()
        if not hasattr(self, "recent_tree"):
            return
        for item in self.recent_tree.get_children():
            self.recent_tree.delete(item)
        for record in self.recent_runs:
            self.recent_tree.insert(
                "",
                "end",
                iid=record.run_id,
                values=(
                    record.started_at_utc,
                    record.status,
                    record.mode,
                    record.product,
                    Path(record.main_output).name if record.main_output else "",
                ),
            )
        if self.recent_runs:
            self.recent_tree.selection_set(self.recent_runs[0].run_id)
            self._select_recent_run()

    def _selected_recent_run(self) -> RecentRunRecord | None:
        if not hasattr(self, "recent_tree"):
            return None
        selection = self.recent_tree.selection()
        if not selection:
            return None
        run_id = str(selection[0])
        for record in self.recent_runs:
            if record.run_id == run_id:
                return record
        return None

    def _select_recent_run(self) -> None:
        record = self._selected_recent_run()
        if record is None:
            return
        self.selected_recent_run_id = record.run_id
        self._set_text_widget(self.recent_detail_text, format_recent_run_summary(record))
        self._update_recent_preview(record)

    def _update_recent_preview(self, record: RecentRunRecord) -> None:
        self.preview_image = None
        if not record.main_output:
            self.preview_label.configure(text="No output file is recorded for this run.", image="")
            return
        path = Path(record.main_output)
        preview = safe_preview_metadata(path)
        if not preview.supported_preview:
            self.preview_label.configure(text=preview.display_text(), image="")
            return
        try:
            from PIL import Image, ImageTk

            with Image.open(path) as image:
                image.thumbnail((360, 240))
                self.preview_image = ImageTk.PhotoImage(image.copy())
            self.preview_label.configure(text=preview.display_text(), image=self.preview_image, compound="top")
        except Exception as exc:
            self.preview_image = None
            self.preview_label.configure(text=f"{preview.display_text()}\nPreview failed: {exc}", image="")

    def _open_selected_recent_output(self) -> None:
        record = self._selected_recent_run()
        if record is None or not record.main_output:
            messagebox.showinfo("Open output", "No recent output is selected.")
            return
        self._open_path(Path(record.main_output))

    def _open_selected_recent_folder(self) -> None:
        record = self._selected_recent_run()
        if record is None:
            messagebox.showinfo("Open folder", "No recent run is selected.")
            return
        target = Path(record.main_output).parent if record.main_output else OUTPUT_DIR
        self._open_path(target)

    def _open_selected_recent_log(self) -> None:
        record = self._selected_recent_run()
        if record is None or not record.log_path:
            messagebox.showinfo("Open log", "No recent run log is recorded.")
            return
        self._open_existing_path(Path(record.log_path), "Log")

    def _copy_selected_recent_paths(self) -> None:
        record = self._selected_recent_run()
        if record is None:
            messagebox.showinfo("Copy paths", "No recent run is selected.")
            return
        paths = [*record.outputs]
        if record.log_path:
            paths.append(record.log_path)
        if not paths:
            messagebox.showinfo("Copy paths", "No recent output or log paths are selected.")
            return
        self._copy_to_clipboard("\n".join(paths))
        self._append_log("Recent output/log path(s) copied to clipboard.")

    def _copy_selected_recent_error(self) -> None:
        record = self._selected_recent_run()
        if record is None:
            messagebox.showinfo("Copy error", "No recent run is selected.")
            return
        config = config_from_recent_run(record)
        report = build_error_report(
            record.error or f"Recent run status: {record.status}",
            config,
            "",
            [Path(path) for path in record.outputs],
            record.log_path,
        )
        self._copy_to_clipboard(report)
        self._append_log("Recent run error report copied to clipboard.")

    def _rerun_selected_recent_settings(self) -> None:
        record = self._selected_recent_run()
        if record is None:
            messagebox.showinfo("Re-run settings", "No recent run is selected.")
            return
        config = config_from_recent_run(record)
        if config is None:
            messagebox.showerror("Re-run settings", "Saved settings for this run could not be loaded.")
            return
        self._set_config_vars(config)
        self.notebook.select(0)
        self._append_log(f"Loaded settings from recent run {record.run_id}.")

    def _clear_recent_runs(self) -> None:
        if not messagebox.askokcancel("Clear history", "Clear all recent run history?"):
            return
        self.recent_runs = []
        save_recent_runs([])
        self._refresh_recent_runs()
        self._set_text_widget(self.recent_detail_text, "")
        self.preview_label.configure(text="Recent run history is empty.", image="")

    def _record_recent_run(self, record: RecentRunRecord) -> None:
        self.recent_runs = append_recent_run(record)
        self._refresh_recent_runs()

    def _refresh_custom_preset_box(self) -> None:
        if hasattr(self, "custom_preset_box"):
            self.custom_preset_box.configure(values=tuple(self.custom_presets))

    def _load_custom_preset(self) -> None:
        name = self.custom_preset_var.get().strip()
        if not name:
            messagebox.showinfo("Load preset", "Choose a custom preset first.")
            return
        config = self.custom_presets.get(name)
        if config is None:
            messagebox.showerror("Load preset", f"Custom preset not found: {name}")
            return
        self._set_config_vars(config)
        self._write_current_settings()
        self._append_log(f"Loaded custom preset: {name}")

    def _save_custom_preset(self) -> None:
        name = self.custom_preset_var.get().strip()
        try:
            config = self._read_config()
            self.custom_presets = save_custom_preset(name, config)
        except Exception as exc:
            messagebox.showerror("Save preset", str(exc))
            return
        self.custom_preset_var.set(name.strip())
        self._refresh_custom_preset_box()
        self._append_log(f"Saved custom preset: {name.strip()}")

    def _delete_custom_preset(self) -> None:
        name = self.custom_preset_var.get().strip()
        try:
            self.custom_presets = delete_custom_preset(name)
        except Exception as exc:
            messagebox.showerror("Delete preset", str(exc))
            return
        self.custom_preset_var.set("")
        self._refresh_custom_preset_box()
        self._append_log(f"Deleted custom preset: {name}")

    def _refresh_ui_mode(self) -> None:
        if not hasattr(self, "advanced_tab_id"):
            return
        if self.ui_mode_var.get() == "Simple":
            self.notebook.hide(self.advanced_tab_id)
        else:
            self.notebook.add(self.advanced_tab_id, text="Advanced")
            self.notebook.select(self.advanced_tab_id)

    def _setup_optional_local_drop_target(self) -> None:
        try:
            import tkinterdnd2  # type: ignore
        except Exception:
            self.local_drop_label.configure(text="Use Local Files... to import .DAT / .DAT.bz2 segment files.")
            return
        if not hasattr(self.local_drop_label, "drop_target_register"):
            self.local_drop_label.configure(text="Use Local Files... to import .DAT / .DAT.bz2 segment files.")
            return
        try:
            self.local_drop_label.drop_target_register(tkinterdnd2.DND_FILES)
            self.local_drop_label.dnd_bind("<<Drop>>", self._handle_local_file_drop)
            self.local_drop_label.configure(text="Drop .DAT / .DAT.bz2 segment files here, or use Local Files...")
            self._append_log("Local drag/drop import is available.")
        except Exception as exc:
            self._append_log(f"Local file picker ready. Drag/drop setup failed: {exc}")

    def _split_drop_paths(self, raw_data: str) -> list[str]:
        try:
            parsed = self.root.tk.splitlist(raw_data)
        except Exception:
            parsed = raw_data.split()
        return [str(path) for path in parsed if str(path).strip()]

    def _handle_local_file_drop(self, event: tk.Event) -> str:
        paths = self._split_drop_paths(str(getattr(event, "data", "")))
        self._import_local_hsd_files(paths)
        return "break"

    def _choose_local_hsd_files(self) -> None:
        if self.is_running:
            return
        selected = filedialog.askopenfilenames(
            title="Choose local Himawari HSD segment files",
            filetypes=(
                ("Himawari HSD segments", "*.DAT *.DAT.bz2 *.dat *.dat.bz2"),
                ("All files", "*.*"),
            ),
        )
        self._import_local_hsd_files(selected)

    def _import_local_hsd_files(self, paths: Iterable[str | Path]) -> None:
        selected = [Path(path) for path in paths if str(path).strip()]
        if not selected:
            return
        try:
            result = import_local_hsd_segments(selected, TEMP_DIR)
        except Exception as exc:
            self.status_var.set("Local import failed")
            self._append_log(f"Local file import failed: {exc}")
            messagebox.showerror("Local import failed", str(exc))
            return
        self.url_var.set(result.synthetic_url)
        self.auto_download_var.set(False)
        self.mode_var.set("Single Image")
        self.status_var.set("Local files imported")
        setup_status = self._update_setup_status()
        self._write_current_settings()
        imported_count = len(result.imported_paths)
        reused_count = len(result.reused_paths)
        message = (
            f"Imported local scan {result.area} {result.timestamp}: "
            f"{imported_count} new, {reused_count} reused, bands {', '.join(result.bands)}."
        )
        self._append_log(message)
        if not setup_status.ok:
            message += "\n\n" + setup_status.display_text()
        messagebox.showinfo("Local files imported", message)

    def _open_scan_browser(self) -> None:
        if self.is_running:
            return
        sat_id = sat_id_from_himawari_source(self.url_var.get())
        self.scan_browser_button.configure(state="disabled")
        self.status_var.set("Loading recent scans")
        self._append_log(f"Loading recent {sat_id} FLDK scan choices from NOAA AWS.")

        def worker() -> None:
            try:
                self.messages.put(("scan_choices", find_recent_fldk_scan_choices(sat_id=sat_id)))
            except Exception as exc:
                self.messages.put(("scan_choices_error", str(exc)))

        threading.Thread(target=worker, daemon=True).start()

    def _show_scan_choice_dialog(self, choices: list[RecentScanChoice]) -> None:
        if not choices:
            messagebox.showinfo("Choose scan", "No recent FLDK scans were found.")
            return
        dialog = tk.Toplevel(self.root)
        dialog.title("Choose Recent FLDK Scan")
        dialog.geometry("520x360")
        dialog.columnconfigure(0, weight=1)
        dialog.rowconfigure(0, weight=1)
        listbox = tk.Listbox(dialog, height=12)
        listbox.grid(row=0, column=0, sticky="nsew", padx=12, pady=12)
        scrollbar = ttk.Scrollbar(dialog, orient="vertical", command=listbox.yview)
        scrollbar.grid(row=0, column=1, sticky="ns", pady=12)
        listbox.configure(yscrollcommand=scrollbar.set)
        for choice in choices:
            listbox.insert("end", choice.label)
        listbox.selection_set(0)

        def use_selected() -> None:
            selection = listbox.curselection()
            if not selection:
                return
            choice = choices[int(selection[0])]
            self.url_var.set(choice.url)
            self.status_var.set("FLDK scan selected")
            self._update_setup_status()
            self._write_current_settings()
            self._append_log(f"Selected FLDK scan URL: {choice.url}")
            dialog.destroy()

        button_row = ttk.Frame(dialog)
        button_row.grid(row=1, column=0, columnspan=2, sticky="ew", padx=12, pady=(0, 12))
        button_row.columnconfigure(0, weight=1)
        ttk.Button(button_row, text="Use Selected", command=use_selected).grid(row=0, column=1, padx=(0, 8))
        ttk.Button(button_row, text="Cancel", command=dialog.destroy).grid(row=0, column=2)
        listbox.bind("<Double-Button-1>", lambda _event: use_selected())

    def _install_setup_watchers(self) -> None:
        watched_vars = (
            self.url_var,
            self.mode_var,
            self.composite_var,
            self.satellite_layer_var,
            self.hours_var,
            self.interval_var,
            self.fps_var,
            self.download_workers_var,
            self.dask_workers_var,
            self.chunk_var,
            self.ram_limit_var,
            self.image_format_var,
            self.resampler_var,
            self.night_fallback_mode_var,
            self.map_view_var,
            self.flat_min_lat_var,
            self.flat_max_lat_var,
            self.flat_min_lon_var,
            self.flat_max_lon_var,
            self.flat_resolution_var,
            self.timelapse_format_var,
            self.auto_download_var,
            self.gpu_acceleration_var,
            self.night_fallback_var,
            self.delete_frames_var,
            self.quality_fallback_var,
            self.border_lines_var,
            self.border_color_var,
            self.border_width_var,
            self.map_labels_var,
            self.map_label_size_var,
            self.night_boundary_var,
            self.crosshair_var,
            self.crosshair_type_var,
            self.crosshair_color_var,
            self.zoom_earth_style_var,
            self.output_template_var,
            self.segment_aware_var,
            self.write_sidecar_var,
            self.overlay_theme_var,
            self.map_label_color_var,
            self.night_boundary_color_var,
        )
        for watched_var in watched_vars:
            watched_var.trace_add("write", lambda *_args: self._update_setup_status())

    def _update_setup_status(self) -> SetupStatus:
        try:
            config = self._read_config()
            setup_status = build_setup_status(config, OUTPUT_DIR, TEMP_DIR)
            self.setup_status_var.set(setup_status.display_text())
            try:
                summary = build_run_summary(config)
                self.run_summary_var.set(summary.display_text())
            except Exception:
                self.run_summary_var.set("")
        except Exception as exc:
            setup_status = SetupStatus(False, (), (), (f"Invalid setup value: {exc}",))
            self.setup_status_var.set(setup_status.display_text())
            self.run_summary_var.set("")
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
        self.satellite_layer_var.set(config.satellite_layer_mode)
        self.hours_var.set(str(config.hours_back))
        self.interval_var.set(str(config.interval_minutes))
        self.fps_var.set(str(config.fps))
        self.download_workers_var.set(str(config.download_workers))
        self.dask_workers_var.set(str(config.dask_num_workers))
        self.chunk_var.set(config.dask_chunk_size)
        self.ram_limit_var.set(str(config.ram_limit_gb))
        self.image_format_var.set(config.image_format)
        self.output_template_var.set(config.output_template)
        self.resampler_var.set(config.resampler)
        self.night_fallback_mode_var.set(config.night_fallback_mode)
        self.map_view_var.set(config.map_view)
        self.flat_min_lat_var.set(str(config.flat_min_lat))
        self.flat_max_lat_var.set(str(config.flat_max_lat))
        self.flat_min_lon_var.set(str(config.flat_min_lon))
        self.flat_max_lon_var.set(str(config.flat_max_lon))
        self.flat_resolution_var.set(str(config.flat_resolution_deg))
        self.timelapse_format_var.set(config.timelapse_format)
        self.auto_download_var.set(config.auto_download)
        self.gpu_acceleration_var.set(config.gpu_acceleration)
        self.night_fallback_var.set(config.use_night_fallback)
        self.delete_frames_var.set(config.delete_timelapse_frames)
        self.quality_fallback_var.set(config.allow_quality_fallback)
        self.border_lines_var.set(config.add_border_lines)
        self.border_color_var.set(config.border_line_color)
        self.border_width_var.set(str(config.border_line_width))
        self.map_labels_var.set(config.add_map_labels)
        self.map_label_size_var.set(str(config.map_label_size))
        self.night_boundary_var.set(config.add_night_boundary)
        self.crosshair_var.set(config.add_crosshair)
        self.crosshair_type_var.set(normalized_crosshair_type(config.crosshair_type))
        self.crosshair_color_var.set(config.crosshair_color)
        self.zoom_earth_style_var.set(config.zoom_earth_style)
        self.segment_aware_var.set(config.segment_aware_downloads)
        self.write_sidecar_var.set(config.write_metadata_sidecar)
        self.overlay_theme_var.set(config.overlay_theme)
        self.map_label_color_var.set(config.map_label_color)
        self.night_boundary_color_var.set(config.night_boundary_color)
        self._refresh_mode_state()
        self._update_setup_status()

    def _apply_satellite_layer_defaults(self) -> None:
        mode = normalized_satellite_layer_mode(self.satellite_layer_var.get())
        if self.satellite_layer_var.get() != mode:
            self.satellite_layer_var.set(mode)
        if mode in {"live", "hd"}:
            self.composite_var.set("True Color Reproduction Image")
            self.map_view_var.set("flat")
            self.zoom_earth_style_var.set(True)
            self.night_fallback_var.set(True)
            self.night_fallback_mode_var.set("hybrid")
            self.border_lines_var.set(True)
            self.map_labels_var.set(True)
            self.crosshair_var.set(False)
            self.border_color_var.set(SATELLITE_LAYER_BORDER_COLOR)
            self.border_width_var.set(str(satellite_layer_border_width(self.border_width_var.get())))
            self._append_log(f"Satellite layer set to {mode}; flat-map true-color defaults applied.")
        self._update_setup_status()
        self._write_current_settings()

    def _apply_area_preset(self) -> None:
        name = self.area_preset_var.get().strip()
        if name == AREA_PRESET_CUSTOM:
            self._append_log("Area preset: Custom (manual bounds kept).")
            return
        if name == AREA_PRESET_FULL_DISK:
            self.map_view_var.set("native")
            if self.zoom_earth_style_var.get():
                self.zoom_earth_style_var.set(False)
                self._append_log("Full Disk (native): turned off Zoom Earth style (it is flat-map only).")
            self._append_log("Area preset: Full Disk (native full-disk view); labels/borders still render.")
        else:
            bounds = area_preset_bounds(name)
            if bounds is None:
                self._append_log(f"Area preset '{name}' is not recognized; bounds unchanged.")
                return
            min_lat, max_lat, min_lon, max_lon = bounds
            self.map_view_var.set("flat")
            self.flat_min_lat_var.set(str(min_lat))
            self.flat_max_lat_var.set(str(max_lat))
            self.flat_min_lon_var.set(str(min_lon))
            self.flat_max_lon_var.set(str(max_lon))
            self._append_log(
                f"Area preset '{name}' applied: flat map lat {min_lat}..{max_lat}, lon {min_lon}..{max_lon}."
            )
        self._update_setup_status()
        self._write_current_settings()

    def _on_zoom_earth_toggle(self) -> None:
        if self.zoom_earth_style_var.get() and normalized_map_view(self.map_view_var.get()) != "flat":
            self.map_view_var.set("flat")
            self._append_log("Zoom Earth style enabled; switched map view to flat (it requires a flat map).")
        self._update_setup_status()
        self._write_current_settings()

    def _on_overlay_theme_change(self) -> None:
        theme = self.overlay_theme_var.get()
        colors = overlay_theme_colors(theme)
        if colors is None:
            self._append_log(f"Overlay theme set to '{theme}'; keeping your current colors.")
            self._update_setup_status()
            self._write_current_settings()
            return
        self.border_color_var.set(colors["border"])
        self.map_label_color_var.set(colors["label"])
        self.night_boundary_color_var.set(colors["night_boundary"])
        self.crosshair_color_var.set(colors["crosshair"])
        self._append_log(
            f"Overlay theme '{theme}' applied: border {colors['border']}, label {colors['label']}, "
            f"night line {colors['night_boundary']}, crosshair {colors['crosshair']}."
        )
        self._update_setup_status()
        self._write_current_settings()

    def _add_tooltip(self, widget, text: str) -> None:
        if widget is None:
            return
        if not hasattr(self, "_tooltips"):
            self._tooltips = []
        try:
            self._tooltips.append(_Tooltip(widget, text))
        except Exception:
            pass

    def _button_help_texts(self) -> dict[str, str]:
        return {
            "start_button": "Start Processing: download the satellite data and create the image or timelapse with the current settings.",
            "stop_button": "Stop: cancel the run that is currently in progress.",
            "open_output_button": "Open Outputs: open the folder where finished images and timelapses are saved.",
            "open_last_button": "Open Last: open the most recently created output file.",
            "copy_paths_button": "Copy Paths: copy the file paths of the last run's outputs to the clipboard.",
            "copy_error_button": "Copy Error: copy the last error report to the clipboard (useful for bug reports).",
            "copy_log_button": "Copy Log: copy the on-screen run log to the clipboard.",
            "copy_settings_button": "Copy Settings: copy the last run's settings as JSON, handy for sharing your exact setup with an AI agent or in a bug report.",
            "update_app_button": "Update App: download the latest version from GitHub and replace the local files (a backup is made first; restart afterwards).",
            "help_button": "Help: open a window explaining what every button does.",
            "check_env_button": "Check Env: run the environment checker to confirm Python and packages are installed correctly.",
            "quick_fix_button": "Quick Fix: try to repair the Python environment (upgrade packages, install overlay data).",
            "auto_fix_button": "Auto Fix: automatically repair the environment, creating a dedicated Python if needed.",
            "latest_url_button": "Latest FLDK: fill the URL with the most recent full-disk scan available.",
            "scan_browser_button": "Choose Scan: browse and pick a specific scan time to process.",
            "local_files_button": "Local Files: import already-downloaded .DAT/.DAT.bz2 segment files instead of downloading.",
            "simple_output_button": "Choose Output Folder: pick where finished images are saved.",
            "choose_output_button": "Choose Output Folder: pick where finished images are saved.",
            "choose_temp_button": "Choose Temp Folder: pick where temporary download files are stored.",
            "open_temp_button": "Open Temp Folder: open the temporary working folder.",
            "overlay_check_button": "Check Overlay Setup: verify the coastline/border overlay data is installed.",
            "preview_button": "Quick Look: render a fast, coarse flat-map preview so you can check framing and colour before a full run. It does not change your settings.",
            "pick_region_button": "Pick Region: open a clickable mini-map and drag a box to set the flat-map crop instead of typing latitude and longitude.",
            "test_host_button": "Test Data Host: check whether the satellite data server is reachable (DNS and a quick HTTP request). Use this to tell a failed run apart from a network problem.",
            "area_preset_box": "Output Region: pick a region to frame the image. Regional presets and 'Full Disk (flat map)' switch to the flat map and fill the bounds; 'Full Disk (native)' uses the round-Earth view.",
            "satellite_layer_box": "Satellite Layer: 'standard' is normal; 'live' grabs the latest scan; 'hd' applies stronger enhancement. live/hd also turn on flat-map true-color styling.",
            "map_view_box": "Map View: 'native' is the round full-disk Earth; 'flat' is a rectangular Web Mercator map you can crop with the bounds.",
        }

    def _install_tooltips(self) -> None:
        for attr, text in self._button_help_texts().items():
            self._add_tooltip(getattr(self, attr, None), text)

    def _open_help_window(self) -> None:
        window = tk.Toplevel(self.root)
        window.title("Help — what each button does")
        window.geometry("620x560")
        window.transient(self.root)
        container = ttk.Frame(window, padding=12)
        container.pack(fill="both", expand=True)
        ttk.Label(
            container,
            text="Hover any button in the main window to see this same help as a tooltip.",
            style="Status.TLabel",
            wraplength=580,
            justify="left",
        ).pack(anchor="w", pady=(0, 8))
        text_widget = tk.Text(container, wrap="word", height=24, borderwidth=1, relief="solid")
        scroll = ttk.Scrollbar(container, orient="vertical", command=text_widget.yview)
        text_widget.configure(yscrollcommand=scroll.set)
        text_widget.pack(side="left", fill="both", expand=True)
        scroll.pack(side="right", fill="y")
        sections = [
            ("Run", ["start_button", "stop_button", "open_output_button", "open_last_button"]),
            ("Tools", ["preview_button", "pick_region_button", "test_host_button", "overlay_check_button"]),
            ("Clipboard", ["copy_paths_button", "copy_error_button", "copy_log_button", "copy_settings_button"]),
            ("Maintenance", ["update_app_button", "check_env_button", "quick_fix_button", "auto_fix_button", "help_button"]),
            ("Source", ["latest_url_button", "scan_browser_button", "local_files_button"]),
            ("Choices", ["satellite_layer_box", "map_view_box", "area_preset_box"]),
        ]
        helps = self._button_help_texts()
        for title, keys in sections:
            text_widget.insert("end", f"{title}\n", ("h",))
            for key in keys:
                if key in helps:
                    label = helps[key].split(":", 1)
                    name = label[0]
                    desc = label[1].strip() if len(label) > 1 else ""
                    text_widget.insert("end", f"  • {name}: ", ("b",))
                    text_widget.insert("end", f"{desc}\n")
            text_widget.insert("end", "\n")
        text_widget.tag_configure("h", font=("Segoe UI", 11, "bold"), spacing1=6, spacing3=2)
        text_widget.tag_configure("b", font=("Segoe UI", 9, "bold"))
        text_widget.configure(state="disabled")
        ttk.Button(window, text="Close", command=window.destroy).pack(pady=(0, 10))

    def _copy_last_settings(self) -> None:
        """Copy the last run's settings (or current settings) as JSON for an AI agent."""
        payload: dict[str, object] = {
            "app": APP_DISPLAY_NAME,
            "app_version": APP_VERSION,
            "exported_at_utc": utc_timestamp(),
        }
        source_label = "current settings (no run recorded yet)"
        config_dict: dict[str, object] | None = None
        if getattr(self, "last_config", None) is not None:
            config_dict = self.last_config.__dict__.copy()
            source_label = "last started run (this session)"
        elif self.recent_runs:
            record = self.recent_runs[0]
            config_dict = dict(record.config)
            payload["last_run"] = {
                "run_id": record.run_id,
                "status": record.status,
                "started_at_utc": record.started_at_utc,
                "completed_at_utc": record.completed_at_utc,
                "product": record.product,
                "source": record.source,
                "main_output": record.main_output,
            }
            source_label = "most recent recorded run"
        else:
            try:
                config_dict = self._read_config().__dict__.copy()
            except Exception as exc:
                self._append_log(f"Could not read current settings to copy: {exc}")
                messagebox.showerror("Copy settings failed", str(exc))
                return
        payload["settings_source"] = source_label
        payload["config"] = config_dict
        try:
            payload["output_dir"] = self.output_dir_var.get()
            payload["temp_dir"] = self.temp_dir_var.get()
        except Exception:
            pass
        text = json.dumps(payload, indent=2, ensure_ascii=False, sort_keys=True)
        self._copy_to_clipboard(text)
        self._append_log(f"Copied settings to clipboard ({source_label}).")

    def _update_from_github(self) -> None:
        if self.is_running:
            messagebox.showinfo("Update App", "Please wait until the current run finishes before updating.")
            return
        message = (
            f"Download the latest version from github.com/{GITHUB_REPO} and replace the local "
            "program files?\n\n"
            "A backup of every replaced file is saved under the 'backups' folder first, and the "
            "downloaded code is checked before anything is replaced.\n\n"
            "You will need to restart the app afterwards."
        )
        if not messagebox.askokcancel("Update App", message):
            self._append_log("Update canceled.")
            return
        self.update_app_button.configure(state="disabled")
        self._append_log("Starting update from GitHub...")

        def log_from_thread(text: str) -> None:
            self.root.after(0, lambda: self._append_log(text))

        def worker() -> None:
            try:
                result = perform_self_update(PROJECT_DIR, log=log_from_thread)
            except Exception as exc:
                self.root.after(0, lambda: self._update_finished(False, str(exc)))
            else:
                self.root.after(0, lambda: self._update_finished(True, result))

        threading.Thread(target=worker, name="himawari-self-update", daemon=True).start()

    def _update_finished(self, ok: bool, result: object) -> None:
        self.update_app_button.configure(state="normal")
        if not ok:
            self._append_log(f"Update failed: {result}")
            messagebox.showerror("Update failed", str(result))
            return
        summary = result if isinstance(result, dict) else {}
        updated = ", ".join(summary.get("updated", []) or []) or "(none)"
        backup_dir = summary.get("backup_dir", "")
        self._append_log(f"Update complete. Updated: {updated}. Backup: {backup_dir}")
        messagebox.showinfo(
            "Update complete",
            "The latest version was installed.\n\n"
            f"Updated files: {updated}\n"
            f"Backup saved to:\n{backup_dir}\n\n"
            "Please close and relaunch the app to use the new version.",
        )

    def _apply_preset(self, name: str) -> None:
        try:
            config = preset_config(name, self._read_config())
        except Exception as exc:
            messagebox.showerror("Preset failed", str(exc))
            return
        self._set_config_vars(config)
        self._write_current_settings()
        self._append_log(f"Applied preset: {name}")

    def _apply_performance_recommendation(self, mode: str) -> None:
        try:
            recommendation = recommend_performance_settings(system_performance_profile(), mode)
        except Exception as exc:
            messagebox.showerror("Performance optimizer failed", str(exc))
            return
        self.download_workers_var.set(str(recommendation.download_workers))
        self.dask_workers_var.set(str(recommendation.dask_num_workers))
        self.chunk_var.set(recommendation.dask_chunk_size)
        self.ram_limit_var.set(str(recommendation.ram_limit_gb))
        self._update_setup_status()
        self._write_current_settings()
        self._append_log(
            f"{recommendation.summary}; "
            f"download workers={recommendation.download_workers}, "
            f"Dask workers={recommendation.dask_num_workers}, "
            f"chunk={recommendation.dask_chunk_size}, "
            f"RAM limit={recommendation.ram_limit_gb:g} GiB"
        )

    def _toggle_gpu_acceleration(self) -> None:
        if self.gpu_acceleration_var.get():
            status = gpu_support_status()
            if not status.ok:
                self.gpu_acceleration_var.set(False)
                self._update_setup_status()
                self._write_current_settings()
                self._append_log("GPU acceleration unavailable: " + status.detail)
                if messagebox.askyesno(
                    "GPU support is not ready",
                    status.detail + "\n\nRun GPU Fix to install optional CuPy support?",
                ):
                    self._open_gpu_environment_fix()
                return
            self._append_log("GPU acceleration enabled: " + status.detail)
            self._append_log("GPU mode accelerates custom true-color math only; reading, reprojection, overlays, and saving stay CPU/disk-bound.")
        else:
            self._append_log("GPU acceleration disabled; CPU processing path selected.")
        self._update_setup_status()
        self._write_current_settings()

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
            getattr(self, "auto_fix_button", None),
            getattr(self, "latest_url_button", None),
            getattr(self, "scan_browser_button", None),
            getattr(self, "local_files_button", None),
            getattr(self, "satellite_layer_box", None),
            getattr(self, "safe_perf_button", None),
            getattr(self, "best_perf_button", None),
            getattr(self, "gpu_check", None),
            getattr(self, "gpu_fix_button", None),
            getattr(self, "overlay_check_button", None),
            getattr(self, "open_last_button", None),
            getattr(self, "copy_paths_button", None),
            getattr(self, "copy_error_button", None),
            getattr(self, "custom_preset_box", None),
            getattr(self, "preview_button", None),
            getattr(self, "pick_region_button", None),
            getattr(self, "test_host_button", None),
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
        self._open_existing_path(OUTPUT_DIR, "Output folder")

    def _open_temp_folder(self) -> None:
        TEMP_DIR.mkdir(parents=True, exist_ok=True)
        self._open_existing_path(TEMP_DIR, "Temp folder")

    def _open_existing_path(self, path: Path, label: str = "Path") -> bool:
        if not path.exists():
            messagebox.showwarning(label, f"Location is not available:\n{path}")
            self._append_log(f"Could not open {label.lower()}; path does not exist: {path}")
            return False
        try:
            if os.name == "nt":
                os.startfile(str(path))
            else:
                messagebox.showinfo(label, str(path))
            return True
        except Exception as exc:
            messagebox.showwarning(label, f"Could not open location:\n{path}\n\n{exc}")
            self._append_log(f"Could not open {label.lower()}: {exc}")
            return False

    def _open_path(self, path: Path) -> None:
        target = path.parent if path.is_file() else path
        if target.exists():
            self._open_existing_path(target, "Path")
            return
        fallback = OUTPUT_DIR
        fallback.mkdir(parents=True, exist_ok=True)
        self._append_log(f"Requested path is unavailable: {target}. Opening output folder instead.")
        messagebox.showwarning(
            "Path unavailable",
            f"Location is not available:\n{target}\n\nOpening output folder instead.",
        )
        self._open_existing_path(fallback, "Output folder")

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
        paths = [str(path) for path in self.last_outputs]
        last_log_path = getattr(self, "last_log_path", None)
        if last_log_path:
            paths.append(str(last_log_path))
        if not paths:
            messagebox.showinfo("Copy paths", "No completed output or log paths are available yet.")
            return
        self._copy_to_clipboard("\n".join(paths))
        self._append_log("Output/log path(s) copied to clipboard.")

    def _copy_current_log(self) -> None:
        last_log_path = getattr(self, "last_log_path", None)
        if last_log_path:
            path = Path(last_log_path)
            if path.is_file():
                try:
                    text = path.read_text(encoding="utf-8", errors="replace").strip()
                except OSError as exc:
                    self._append_log(f"Could not read run log for copying: {exc}")
                    messagebox.showwarning(
                        "Copy log",
                        f"Could not read saved run log:\n{path}\n\nUsing the visible log panel instead.",
                    )
                else:
                    self._copy_to_clipboard(f"Log file: {path}\n\n{text}\n")
                    self._append_log("Run log copied to clipboard.")
                    return
            else:
                self._append_log(f"Saved run log is unavailable: {path}; copying visible log panel instead.")

        visible_log = ""
        if hasattr(self, "log_text"):
            visible_log = str(self.log_text.get("1.0", "end")).strip()
        if not visible_log:
            visible_log = "\n".join(str(message) for message in getattr(self, "pending_log_messages", ())).strip()
        if not visible_log:
            messagebox.showinfo("Copy log", "No run log or visible log text is available yet.")
            return
        if last_log_path:
            header = f"Visible log panel (saved log unavailable: {last_log_path})"
        else:
            header = "Visible log panel"
        self._copy_to_clipboard(f"{header}\n\n{visible_log}\n")
        self._append_log("Visible log copied to clipboard.")

    def _copy_error_report(self) -> None:
        if not self.last_error_report:
            self.last_error_report = build_error_report(
                "No processing error has been recorded.",
                self.last_config,
                self.log_text.get("1.0", "end"),
                self.last_outputs,
                getattr(self, "last_log_path", None),
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

    def _open_environment_auto_fix(self) -> None:
        self._open_environment_command(["--auto"], "Environment auto fix")

    def _open_gpu_environment_fix(self) -> None:
        self._open_environment_command(["--gpu-fix"], "GPU environment fix")

    def _check_overlays(self) -> None:
        overlays_dir = PROJECT_DIR / "overlays"
        overlays_dir.mkdir(parents=True, exist_ok=True)
        status = overlay_status(PROJECT_DIR)
        self._append_log(status.display_text())
        self._append_log(f"Overlay folder: {overlays_dir}")
        if status.ok:
            messagebox.showinfo("Overlay setup", status.display_text())
        else:
            messagebox.showwarning("Overlay setup", status.display_text())

    # ------------------------------------------------------------------
    # Connectivity check (Test Data Host)
    # ------------------------------------------------------------------
    def _start_connectivity_check(self) -> None:
        if self.is_running:
            return
        source = self.url_var.get().strip()
        self.status_var.set("Testing data host")
        self._append_log("Testing whether the satellite data host is reachable...")
        self.test_host_button.configure(state="disabled")

        def worker() -> None:
            try:
                result = check_data_host_connectivity(source or None)
                self.messages.put(("connectivity_result", result))
            except Exception as exc:
                self.messages.put(("connectivity_error", str(exc)))

        threading.Thread(target=worker, daemon=True).start()

    # ------------------------------------------------------------------
    # Live preview (Quick Look)
    # ------------------------------------------------------------------
    def _start_preview(self) -> None:
        if self.is_running:
            messagebox.showinfo("Quick Look", "A run is already in progress. Wait for it to finish first.")
            return
        try:
            config = self._read_config()
            preview_config = build_preview_config(config)
            validate_configuration(preview_config)
        except Exception as exc:
            self._append_log(f"Quick Look could not start: {exc}")
            messagebox.showerror("Quick Look", str(exc))
            return

        self.status_var.set("Building quick look")
        self.phase_var.set("Quick Look")
        self._append_log(
            "Quick Look: rendering a coarse flat-map preview "
            f"(~{preview_config.flat_resolution_deg:g} deg/pixel). This does not affect a full run."
        )
        self.preview_button.configure(state="disabled")
        preview_started_utc = utc_timestamp()

        def progress(message: str, current: int | None, total: int | None) -> None:
            self.messages.put(("progress", (f"Quick Look: {message}", current, total)))

        def worker() -> None:
            try:
                outputs = run(
                    preview_config,
                    progress=progress,
                    cancel_event=self.cancel_event,
                    started_at_utc=preview_started_utc,
                )
                self.messages.put(("preview_done", outputs))
            except ProcessingCancelled as exc:
                self.messages.put(("preview_error", f"Canceled: {exc}"))
            except Exception as exc:
                LOG.exception("Quick Look preview failed")
                self.messages.put(("preview_error", str(exc)))

        threading.Thread(target=worker, daemon=True).start()

    def _show_preview_window(self, image_path: Path) -> None:
        try:
            from PIL import Image, ImageTk
        except Exception as exc:
            messagebox.showinfo("Quick Look", f"Preview image saved to:\n{image_path}\n\n(Could not display inline: {exc})")
            return
        try:
            with Image.open(image_path) as opened:
                preview = opened.convert("RGBA")
                width, height = preview.size
                scale = min(1.0, PREVIEW_MAX_DIMENSION_PX / float(max(width, height)))
                if scale < 1.0:
                    preview = preview.resize(
                        (max(1, int(width * scale)), max(1, int(height * scale))),
                        Image.LANCZOS,
                    )
                photo = ImageTk.PhotoImage(preview)
        except Exception as exc:
            messagebox.showinfo("Quick Look", f"Preview image saved to:\n{image_path}\n\n(Could not open image: {exc})")
            return

        window = tk.Toplevel(self.root)
        window.title("Quick Look preview")
        window.transient(self.root)
        # Keep a reference so the image is not garbage-collected.
        self.preview_image = photo
        ttk.Label(window, text=f"{image_path.name}  ({width} x {height} px source)").pack(padx=10, pady=(10, 4))
        ttk.Label(window, image=photo).pack(padx=10, pady=(0, 6))
        ttk.Label(
            window,
            text="This is a coarse preview for framing and colour. The full run uses your real settings.",
            wraplength=max(360, photo.width()),
            style="Status.TLabel",
        ).pack(padx=10, pady=(0, 8))
        ttk.Button(window, text="Close", command=window.destroy).pack(pady=(0, 10))

    # ------------------------------------------------------------------
    # Visual region picker (Pick Region)
    # ------------------------------------------------------------------
    def _open_region_picker(self) -> None:
        RegionPickerDialog(self)

    def _fill_latest_fldk_url(self) -> None:
        sat_id = sat_id_from_himawari_source(self.url_var.get())
        self.status_var.set("Finding latest FLDK")
        self.latest_url_button.configure(state="disabled")
        self._append_log(f"Looking for latest {sat_id} FLDK scan on NOAA AWS.")

        def worker() -> None:
            try:
                self.messages.put(("latest_url", find_latest_fldk_url(sat_id=sat_id)))
            except Exception as exc:
                self.messages.put(("latest_url_error", str(exc)))

        threading.Thread(target=worker, daemon=True).start()

    def _choose_color(self, variable: tk.StringVar, title: str, fallback: str) -> None:
        initial = variable.get()
        try:
            rgb = parse_rgb_color(initial)
            initial_color = "#{:02x}{:02x}{:02x}".format(*rgb)
        except ValueError:
            initial_color = fallback
        _selected, hex_value = colorchooser.askcolor(color=initial_color, title=title)
        if hex_value:
            variable.set(hex_value)

    def _choose_border_color(self) -> None:
        self._choose_color(self.border_color_var, "Choose border line color", "#00ff00")

    def _choose_crosshair_color(self) -> None:
        self._choose_color(self.crosshair_color_var, "Choose crosshair color", CROSSHAIR_COLOR)

    def _read_float_var(self, variable: tk.StringVar, label: str) -> float:
        return finite_float(variable.get(), label)

    def _read_config(self) -> ProcessorConfig:
        return ProcessorConfig(
            user_url=self.url_var.get().strip(),
            mode=self.mode_var.get(),
            composite_choice=self.composite_var.get(),
            satellite_layer_mode=self.satellite_layer_var.get(),
            hours_back=int(self.hours_var.get()),
            interval_minutes=int(self.interval_var.get()),
            fps=int(self.fps_var.get()),
            auto_download=self.auto_download_var.get(),
            gpu_acceleration=self.gpu_acceleration_var.get(),
            use_night_fallback=self.night_fallback_var.get(),
            night_fallback_mode=self.night_fallback_mode_var.get(),
            download_workers=int(self.download_workers_var.get()),
            timelapse_format=self.timelapse_format_var.get(),
            delete_timelapse_frames=self.delete_frames_var.get(),
            image_format=self.image_format_var.get(),
            output_template=self.output_template_var.get(),
            resampler=self.resampler_var.get(),
            allow_quality_fallback=self.quality_fallback_var.get(),
            add_border_lines=self.border_lines_var.get(),
            border_line_color=self.border_color_var.get(),
            border_line_width=self._read_float_var(self.border_width_var, "Border line width"),
            add_map_labels=self.map_labels_var.get(),
            map_label_size=int(self.map_label_size_var.get()),
            add_night_boundary=self.night_boundary_var.get(),
            add_crosshair=self.crosshair_var.get(),
            crosshair_type=self.crosshair_type_var.get(),
            crosshair_color=self.crosshair_color_var.get(),
            zoom_earth_style=self.zoom_earth_style_var.get(),
            map_view=self.map_view_var.get(),
            flat_min_lat=self._read_float_var(self.flat_min_lat_var, "Flat map min latitude"),
            flat_max_lat=self._read_float_var(self.flat_max_lat_var, "Flat map max latitude"),
            flat_min_lon=self._read_float_var(self.flat_min_lon_var, "Flat map min longitude"),
            flat_max_lon=self._read_float_var(self.flat_max_lon_var, "Flat map max longitude"),
            flat_resolution_deg=self._read_float_var(self.flat_resolution_var, "Flat map resolution"),
            ram_limit_gb=self._read_float_var(self.ram_limit_var, "RAM limit"),
            dask_chunk_size=self.chunk_var.get(),
            dask_num_workers=int(self.dask_workers_var.get()),
            segment_aware_downloads=self.segment_aware_var.get(),
            write_metadata_sidecar=self.write_sidecar_var.get(),
            overlay_theme=self.overlay_theme_var.get(),
            map_label_color=self.map_label_color_var.get(),
            night_boundary_color=self.night_boundary_color_var.get(),
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
        self.phase_var.set("Checking")
        self.progress_eta_estimator.reset()
        self.cancel_event.clear()
        self._set_running(True)
        self.last_config = config
        self.last_error_report = ""
        self.run_started_at_utc = utc_timestamp()
        self.last_log_path = run_log_path(self.run_started_at_utc, config)
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
        self.phase_var.set("Canceling")
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
            outputs = run(
                config,
                progress=progress,
                cancel_event=self.cancel_event,
                started_at_utc=self.run_started_at_utc or None,
            )
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
                eta_text = self.progress_eta_estimator.update(str(message), current, total)
                self.status_var.set(str(message))
                self.phase_var.set(format_phase_status(str(message), current, total, eta_text=eta_text))
                if current is not None and total:
                    self.progress_var.set(min(100.0, (float(current) / float(total)) * 100.0))
            elif kind == "done":
                self._sync_runtime_run_state()
                outputs = payload
                self.last_outputs = list(outputs)
                self._set_running(False)
                self.progress_var.set(100)
                self.status_var.set("Done")
                self.phase_var.set("Done")
                self.progress_eta_estimator.reset()
                output_lines = "\n".join(str(path) for path in outputs) if outputs else "No output paths returned."
                self._append_log("Finished. Outputs:\n" + output_lines)
                if self.last_config is not None:
                    record = build_recent_run_record(
                        "complete",
                        self.last_config,
                        self.run_started_at_utc or utc_timestamp(),
                        outputs=self.last_outputs,
                        log_path=self.last_log_path,
                    )
                    self._record_recent_run(record)
                    done_text = completion_message(self.last_outputs, self.last_config, OUTPUT_DIR)
                else:
                    done_text = f"Processing finished.\n\nOutputs:\n{output_lines}"
                messagebox.showinfo("Done", done_text)
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
            elif kind == "connectivity_result":
                result = payload
                if not self.is_running:
                    self.test_host_button.configure(state="normal")
                text = result.display_text()  # type: ignore[union-attr]
                self.status_var.set("Data host reachable" if result.reachable else "Data host unreachable")  # type: ignore[union-attr]
                self._append_log("Data host connectivity:\n" + text)
                if result.reachable:  # type: ignore[union-attr]
                    messagebox.showinfo("Data host connectivity", text)
                else:
                    messagebox.showwarning("Data host connectivity", text)
            elif kind == "connectivity_error":
                if not self.is_running:
                    self.test_host_button.configure(state="normal")
                self.status_var.set("Connectivity test failed")
                self._append_log(f"Connectivity test failed: {payload}")
                messagebox.showerror("Data host connectivity", str(payload))
            elif kind == "preview_done":
                if not self.is_running:
                    self.preview_button.configure(state="normal")
                outputs = list(payload) if payload else []
                self.status_var.set("Quick Look ready")
                self.phase_var.set("Ready")
                if outputs:
                    preview_path = Path(outputs[0])
                    self._append_log(f"Quick Look preview saved: {preview_path}")
                    self._show_preview_window(preview_path)
                else:
                    self._append_log("Quick Look finished but produced no image.")
                    messagebox.showinfo("Quick Look", "The preview finished but produced no image.")
            elif kind == "preview_error":
                if not self.is_running:
                    self.preview_button.configure(state="normal")
                self.status_var.set("Quick Look failed")
                self.phase_var.set("Ready")
                self._append_log(f"Quick Look failed: {payload}")
                messagebox.showerror("Quick Look", str(payload))
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
            elif kind == "scan_choices":
                self.scan_browser_button.configure(state="normal" if not self.is_running else "disabled")
                self.status_var.set("Recent scans loaded")
                self._show_scan_choice_dialog(list(payload))
            elif kind == "scan_choices_error":
                self.scan_browser_button.configure(state="normal" if not self.is_running else "disabled")
                self.status_var.set("Scan lookup failed")
                self._append_log(f"Recent FLDK scan lookup failed: {payload}")
                messagebox.showerror("Scan lookup failed", str(payload))
            elif kind == "canceled":
                self._sync_runtime_run_state()
                self._set_running(False)
                self.status_var.set("Canceled")
                self.phase_var.set("Canceled")
                self.progress_eta_estimator.reset()
                cancel_text = str(payload)
                if self.last_config is not None:
                    cancel_text = cancel_resume_message(self.last_config, self.last_outputs, OUTPUT_DIR)
                    self._record_recent_run(
                        build_recent_run_record(
                            "canceled",
                            self.last_config,
                            self.run_started_at_utc or utc_timestamp(),
                            outputs=self.last_outputs,
                            error=str(payload),
                            log_path=self.last_log_path,
                        )
                    )
                self._append_log(cancel_text)
                messagebox.showinfo("Canceled", cancel_text)
            elif kind == "error":
                self._sync_runtime_run_state()
                self._set_running(False)
                self.status_var.set("Failed")
                self.phase_var.set("Failed")
                self.progress_eta_estimator.reset()
                self.last_error_report = build_error_report(
                    str(payload),
                    self.last_config,
                    self.log_text.get("1.0", "end"),
                    self.last_outputs,
                    self.last_log_path,
                )
                if self.last_config is not None:
                    self._record_recent_run(
                        build_recent_run_record(
                            "failed",
                            self.last_config,
                            self.run_started_at_utc or utc_timestamp(),
                            outputs=self.last_outputs,
                            error=str(payload),
                            log_path=self.last_log_path,
                        )
                    )
                messagebox.showerror("Processing failed", str(payload))

        self.root.after(100, self._poll_messages)

    def _sync_runtime_run_state(self) -> None:
        config = getattr(run, "last_config", None)
        if isinstance(config, ProcessorConfig):
            self.last_config = config
        log_path = getattr(run, "last_log_path", None)
        if log_path:
            self.last_log_path = Path(log_path)

    def _flush_pending_log_messages(self) -> None:
        pending = list(getattr(self, "pending_log_messages", ()))
        self.pending_log_messages = []
        for message in pending:
            self._append_log(message)

    def _append_log(self, message: str) -> None:
        if not hasattr(self, "log_text"):
            if not hasattr(self, "pending_log_messages"):
                self.pending_log_messages = []
            self.pending_log_messages.append(message)
            return
        self.log_text.configure(state="normal")
        self.log_text.insert("end", message + "\n")
        self.log_text.see("end")
        self.log_text.configure(state="disabled")


def launch_gui() -> None:
    try:
        from tkinterdnd2 import TkinterDnD  # type: ignore

        root = TkinterDnD.Tk()
    except Exception:
        root = tk.Tk()
    HimawariProcessorApp(root)
    root.mainloop()


if __name__ == "__main__":
    try:
        launch_gui()
    except KeyboardInterrupt:
        print("Interrupted", file=sys.stderr)
        raise SystemExit(130)
