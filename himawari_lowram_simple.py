#!/usr/bin/env python3
"""Himawari-8/9 Low-RAM Processor — Simple edition.

This is the simplest possible front-end to the same low-RAM processing engine.
It is meant for people who just want a picture: pick a scan, pick a product,
maybe turn labels or coastlines on, and click Start.

What's kept (vs the full GUI):
  * Source: URL, Latest FLDK, Choose Scan, Local Files.
  * Product (band/composite) and Timelapse timing (hours back / interval / fps).
  * Options: a single Map Style chooser (Native flat map / Zoom Earth style flat
    map), Labels with a colour chooser, Coastline & borders with a colour chooser.
  * All the bottom buttons (Quick Look, Pick Region, Test Data Host, Check Env,
    Quick Fix, Auto Fix, Open Outputs, Open Last, Copy Paths/Error/Log/Settings,
    Update App, Help).
  * Advanced tab: Performance only (download workers, Dask workers + chunk size,
    RAM limit, Max PNG Pixels, GPU toggle). Output/temp folders and the resampler
    are hidden — they use the safe defaults.

What's locked (forced, not shown):
  * Satellite layer = standard (no live/hd auto-switching).
  * Auto-download missing satellite files = ON (always).
  * Write metadata sidecar = OFF (always).
  * Night fallback, crosshair, night boundary, overlay theme, segment-aware
    downloads, quality fallback, delete timelapse frames, image format, output
    template — all use their defaults and are not shown.

The simple GUI saves its own settings file (himawari_simple_settings.json) so it
does not collide with the full GUI's settings.

Launch:
    python himawari_lowram_simple.py
"""

from __future__ import annotations

import os
import sys
import threading
import tkinter as tk
from pathlib import Path
from tkinter import colorchooser, filedialog, messagebox, ttk

# Locate the processor module (himawari_lowram_processor_claude.py). The simple
# GUI is just a thin front-end that imports the full engine, so the two files
# must sit together. Search a few likely locations so it still works if the
# user keeps the simple script next to the processor, in a subfolder, or runs
# it from the project directory.
_HERE = Path(__file__).resolve().parent
_PROCESSOR_FILENAME = "himawari_lowram_processor_claude.py"


def _find_processor_module() -> Path | None:
    """Search candidate folders for the processor .py and return its path."""
    candidates: list[Path] = []
    # 1. Same folder as this simple script.
    candidates.append(_HERE / _PROCESSOR_FILENAME)
    # 2. Parent folder (script in a subfolder).
    candidates.append(_HERE.parent / _PROCESSOR_FILENAME)
    # 3. Current working directory.
    candidates.append(Path.cwd() / _PROCESSOR_FILENAME)
    # 4. Common Windows locations (Downloads, Desktop, OneDrive Desktop).
    home = Path.home()
    candidates.extend(
        [
            home / "Downloads" / _PROCESSOR_FILENAME,
            home / "Desktop" / _PROCESSOR_FILENAME,
            home / "OneDrive" / "Desktop" / _PROCESSOR_FILENAME,
            home / "OneDrive" / "Desktop" / "coding projects" / _PROCESSOR_FILENAME,
            home / "Desktop" / "coding projects" / _PROCESSOR_FILENAME,
        ]
    )
    for candidate in candidates:
        try:
            if candidate.is_file():
                return candidate
        except OSError:
            continue
    return None


_processor_path = _find_processor_module()
if _processor_path is not None:
    _processor_dir = str(_processor_path.parent)
    if _processor_dir not in sys.path:
        sys.path.insert(0, _processor_dir)
else:
    print(
        "ERROR: could not find 'himawari_lowram_processor_claude.py'.\n"
        "The Simple GUI is a front-end that needs the main processor file next to it.\n"
        "Please copy both files into the same folder and run again. Looked in:\n"
        f"  - {_HERE}\n"
        f"  - {_HERE.parent}\n"
        f"  - {Path.cwd()}\n"
        f"  - {Path.home() / 'Downloads'}\n"
        f"  - {Path.home() / 'Desktop'}\n"
        f"  - {Path.home() / 'OneDrive' / 'Desktop'}\n",
        file=sys.stderr,
    )
    raise SystemExit(1)

import himawari_lowram_processor_claude as h  # noqa: E402

from himawari_lowram_processor_claude import (  # noqa: E402
    APP_DISPLAY_NAME,
    APP_VERSION,
    AREA_PRESET_CUSTOM,
    AREA_PRESET_FULL_DISK,
    COMPOSITE_BANDS,
    DASK_CHUNK_CHOICES,
    FLAT_MAX_LAT,
    FLAT_MAX_LON,
    FLAT_MIN_LAT,
    FLAT_MIN_LON,
    FLAT_RESOLUTION_DEG,
    MAP_LABEL_COLOR,
    MAP_LABEL_SIZE,
    OUTPUT_DIR,
    TEMP_DIR,
    ProcessorConfig,
    SATELLITE_LAYER_BORDER_COLOR,
    default_config,
    load_gui_settings,
    save_gui_settings,
)

# A separate settings file so the simple GUI never clobbers the full GUI's
# saved settings (the two have different option sets).
SIMPLE_SETTINGS_FILE = h.PROJECT_DIR / "himawari_simple_settings.json"
SIMPLE_SETTINGS_SCHEMA_VERSION = 1


def _simple_default_config() -> ProcessorConfig:
    """The locked defaults the simple GUI forces on top of whatever the user
    can see/edit."""
    cfg = default_config()
    cfg = h.replace(cfg, satellite_layer_mode="standard")
    cfg = h.replace(cfg, auto_download=True)
    cfg = h.replace(cfg, write_metadata_sidecar=False)
    cfg = h.replace(cfg, map_view="flat")
    cfg = h.replace(cfg, zoom_earth_style=False)
    return cfg


def _load_simple_settings() -> ProcessorConfig:
    import json

    try:
        text = SIMPLE_SETTINGS_FILE.read_text(encoding="utf-8")
        data = json.loads(text)
    except Exception:
        return _simple_default_config()
    raw = data.get("config", {}) if isinstance(data, dict) else {}
    if not isinstance(raw, dict):
        return _simple_default_config()
    base = _simple_default_config().__dict__.copy()
    # Only accept the handful of fields the simple GUI exposes.
    allowed = {
        "user_url",
        "mode",
        "composite_choice",
        "hours_back",
        "interval_minutes",
        "fps",
        "timelapse_format",
        "map_view",
        "zoom_earth_style",
        "add_map_labels",
        "map_label_color",
        "map_label_size",
        "add_border_lines",
        "border_line_color",
        "download_workers",
        "dask_num_workers",
        "dask_chunk_size",
        "ram_limit_gb",
        "max_safe_png_pixels",
        "gpu_acceleration",
        "flat_min_lat",
        "flat_max_lat",
        "flat_min_lon",
        "flat_max_lon",
    }
    for key, value in raw.items():
        if key in allowed:
            base[key] = value
    try:
        cfg = ProcessorConfig(**base)
        # Re-force the locked fields so a manually-edited file can't override them.
        cfg = h.replace(cfg, satellite_layer_mode="standard")
        cfg = h.replace(cfg, auto_download=True)
        cfg = h.replace(cfg, write_metadata_sidecar=False)
        return cfg
    except Exception:
        return _simple_default_config()


def _save_simple_settings(config: ProcessorConfig) -> None:
    import json

    payload = {
        "schema_version": SIMPLE_SETTINGS_SCHEMA_VERSION,
        "app_version": APP_VERSION,
        "config": config.__dict__.copy(),
    }
    try:
        SIMPLE_SETTINGS_FILE.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    except Exception:
        pass


class HimawariSimpleApp(h.HimawariProcessorApp):
    """A stripped-down front-end to the same low-RAM processing engine.

    Everything heavy (download, Satpy, true-color enhancement, basemap blend,
    timelapse assembly, progress polling, worker thread) is inherited unchanged
    from ``HimawariProcessorApp``. Only the interface and the config-read path
    are overridden.
    """

    # ---- settings persistence (separate file) -------------------------------
    def _write_current_settings(self) -> None:
        try:
            _save_simple_settings(self._read_config())
        except Exception as exc:
            self._append_log(f"Could not save settings: {exc}")

    # ---- config read: force the locked defaults -----------------------------
    def _read_config(self) -> ProcessorConfig:
        # Read the editable fields from the simple GUI's variables, then force
        # the locked defaults on top so the run is always "standard, auto-
        # download on, no sidecar".
        try:
            flat_min_lat = float(self.flat_min_lat_var.get())
            flat_max_lat = float(self.flat_max_lat_var.get())
            flat_min_lon = float(self.flat_min_lon_var.get())
            flat_max_lon = float(self.flat_max_lon_var.get())
        except Exception:
            flat_min_lat, flat_max_lat = FLAT_MIN_LAT, FLAT_MAX_LAT
            flat_min_lon, flat_max_lon = FLAT_MIN_LON, FLAT_MAX_LON
        try:
            map_label_size = int(self.map_label_size_var.get())
        except Exception:
            map_label_size = MAP_LABEL_SIZE
        try:
            download_workers = int(self.download_workers_var.get())
        except Exception:
            download_workers = h.DOWNLOAD_WORKERS
        try:
            dask_workers = int(self.dask_workers_var.get())
        except Exception:
            dask_workers = h.DASK_NUM_WORKERS
        try:
            ram_limit = float(self.ram_limit_var.get())
        except Exception:
            ram_limit = h.RAM_LIMIT_GB
        try:
            max_png = int(self.max_safe_png_pixels_var.get())
        except Exception:
            max_png = h.MAX_SAFE_PNG_PIXELS

        # Map style chooser: "native" = plain flat map, "zoom" = zoom-earth style.
        map_style = self._map_style_var.get()
        map_view = "flat"
        zoom_earth = map_style == "zoom"

        config = ProcessorConfig(
            user_url=self.url_var.get().strip(),
            mode=self.mode_var.get(),
            composite_choice=self.composite_var.get(),
            satellite_layer_mode="standard",  # locked
            hours_back=int(self.hours_var.get()),
            interval_minutes=int(self.interval_var.get()),
            fps=int(self.fps_var.get()),
            auto_download=True,  # locked on
            gpu_acceleration=self.gpu_acceleration_var.get(),
            use_night_fallback=True,  # sensible default, hidden
            night_fallback_mode="hybrid",
            download_workers=download_workers,
            timelapse_format=self.timelapse_format_var.get(),
            delete_timelapse_frames=False,  # default, hidden
            image_format="png",  # default, hidden
            output_template=h.OUTPUT_TEMPLATE,
            resampler="native",  # locked to the low-RAM default
            allow_quality_fallback=False,
            add_border_lines=self.border_lines_var.get(),
            border_line_color=self.border_color_var.get(),
            border_line_width=1.0,
            add_map_labels=self.map_labels_var.get(),
            map_label_size=map_label_size,
            add_night_boundary=False,
            add_crosshair=False,
            crosshair_type="target",
            crosshair_color=h.CROSSHAIR_COLOR,
            zoom_earth_style=zoom_earth,
            map_view=map_view,
            flat_min_lat=flat_min_lat,
            flat_max_lat=flat_max_lat,
            flat_min_lon=flat_min_lon,
            flat_max_lon=flat_max_lon,
            flat_resolution_deg=FLAT_RESOLUTION_DEG,
            ram_limit_gb=ram_limit,
            dask_chunk_size=self.chunk_var.get(),
            dask_num_workers=dask_workers,
            max_safe_png_pixels=max_png,
            segment_aware_downloads=True,  # default, hidden
            write_metadata_sidecar=False,  # locked off
            overlay_theme="Custom (keep my colors)",
            map_label_color=self.map_label_color_var.get(),
            night_boundary_color=h.NIGHT_BOUNDARY_COLOR,
        )
        return config

    # ---- build the simplified UI --------------------------------------------
    def _build_ui(self) -> None:
        self.root.title(f"{APP_DISPLAY_NAME} (Simple) v{APP_VERSION}")
        self.root.geometry("980x820")
        self.root.minsize(760, 660)

        # Pack the bottom elements FIRST (side="bottom") so they reserve their
        # space before the notebook + log expand to fill the rest. Packing them
        # last with the log using expand=True previously squeezed the button bar
        # below the visible window.
        self._build_bottom_buttons(bottom_side=True)
        self._build_status_and_log(bottom_side=True)

        # A single notebook with two tabs: Run Setup and Advanced.
        notebook = ttk.Notebook(self.root)
        notebook.pack(fill="both", expand=True, padx=8, pady=8)

        setup_tab = ttk.Frame(notebook)
        advanced_tab = ttk.Frame(notebook)
        notebook.add(setup_tab, text="Run Setup")
        notebook.add(advanced_tab, text="Advanced")

        self._build_setup_tab(setup_tab)
        self._build_advanced_tab(advanced_tab)

    # ---- Run Setup tab ------------------------------------------------------
    def _build_setup_tab(self, parent: ttk.Frame) -> None:
        # --- Source ---
        source_frame = ttk.LabelFrame(parent, text="Source")
        source_frame.pack(fill="x", padx=8, pady=(8, 4))

        ttk.Label(source_frame, text="Himawari scan URL:").grid(row=0, column=0, sticky="w", padx=4, pady=4)
        self.url_entry = ttk.Entry(source_frame, textvariable=self.url_var)
        self.url_entry.grid(row=0, column=1, sticky="ew", padx=4, pady=4)
        source_frame.columnconfigure(1, weight=1)

        self.latest_url_button = ttk.Button(source_frame, text="Latest FLDK", command=self._fill_latest_fldk_url)
        self.latest_url_button.grid(row=0, column=2, padx=4, pady=4)
        self.scan_browser_button = ttk.Button(source_frame, text="Choose Scan", command=self._open_scan_browser)
        self.scan_browser_button.grid(row=0, column=3, padx=4, pady=4)
        ttk.Button(source_frame, text="Local Files...", command=self._choose_local_hsd_files).grid(
            row=0, column=4, padx=4, pady=4
        )

        # --- Product ---
        product_frame = ttk.LabelFrame(parent, text="Product")
        product_frame.pack(fill="x", padx=8, pady=4)
        ttk.Label(product_frame, text="Product:").grid(row=0, column=0, sticky="w", padx=4, pady=4)
        self.product_combo = ttk.Combobox(
            product_frame,
            textvariable=self.composite_var,
            values=tuple(sorted(COMPOSITE_BANDS)),
            state="readonly",
            width=42,
        )
        self.product_combo.grid(row=0, column=1, sticky="w", padx=4, pady=4)

        # --- Output mode + timelapse timing ---
        mode_frame = ttk.LabelFrame(parent, text="Output")
        mode_frame.pack(fill="x", padx=8, pady=4)
        ttk.Label(mode_frame, text="Mode:").grid(row=0, column=0, sticky="w", padx=4, pady=4)
        ttk.Radiobutton(
            mode_frame, text="Single Image", variable=self.mode_var, value="Single Image"
        ).grid(row=0, column=1, padx=4, pady=4, sticky="w")
        ttk.Radiobutton(
            mode_frame, text="Timelapse", variable=self.mode_var, value="Timelapse"
        ).grid(row=0, column=2, padx=4, pady=4, sticky="w")

        ttk.Label(mode_frame, text="Hours back:").grid(row=1, column=0, sticky="w", padx=4, pady=4)
        ttk.Spinbox(mode_frame, from_=1, to=72, textvariable=self.hours_var, width=8).grid(
            row=1, column=1, padx=4, pady=4, sticky="w"
        )
        ttk.Label(mode_frame, text="Interval (min):").grid(row=1, column=2, sticky="w", padx=4, pady=4)
        ttk.Spinbox(mode_frame, from_=1, to=180, textvariable=self.interval_var, width=8).grid(
            row=1, column=3, padx=4, pady=4, sticky="w"
        )
        ttk.Label(mode_frame, text="FPS:").grid(row=2, column=0, sticky="w", padx=4, pady=4)
        ttk.Spinbox(mode_frame, from_=1, to=30, textvariable=self.fps_var, width=8).grid(
            row=2, column=1, padx=4, pady=4, sticky="w"
        )
        ttk.Label(mode_frame, text="Animation:").grid(row=2, column=2, sticky="w", padx=4, pady=4)
        ttk.Combobox(
            mode_frame,
            textvariable=self.timelapse_format_var,
            values=("gif", "mp4"),
            state="readonly",
            width=8,
        ).grid(row=2, column=3, padx=4, pady=4, sticky="w")

        # --- Options (the only exposed options) ---
        options_frame = ttk.LabelFrame(parent, text="Options")
        options_frame.pack(fill="x", padx=8, pady=4)

        # Map style: native flat map vs zoom earth style flat map.
        self._map_style_var = tk.StringVar(value="native")
        # If loaded settings had zoom_earth_style on, reflect it.
        if getattr(self, "_initial_zoom", False):
            self._map_style_var.set("zoom")
        ttk.Label(options_frame, text="Map style:").grid(row=0, column=0, sticky="w", padx=4, pady=4)
        ttk.Radiobutton(
            options_frame,
            text="Native flat map",
            variable=self._map_style_var,
            value="native",
        ).grid(row=0, column=1, padx=4, pady=4, sticky="w")
        ttk.Radiobutton(
            options_frame,
            text="Zoom Earth style flat map",
            variable=self._map_style_var,
            value="zoom",
        ).grid(row=0, column=2, padx=4, pady=4, sticky="w")

        # Labels + colour chooser.
        ttk.Checkbutton(options_frame, text="Labels", variable=self.map_labels_var).grid(
            row=1, column=0, padx=4, pady=4, sticky="w"
        )
        ttk.Label(options_frame, text="Label colour:").grid(row=1, column=1, sticky="w", padx=4, pady=4)
        self._label_color_entry = ttk.Entry(options_frame, textvariable=self.map_label_color_var, width=10)
        self._label_color_entry.grid(row=1, column=2, padx=4, pady=4, sticky="w")
        ttk.Button(options_frame, text="Choose...", command=self._choose_label_color).grid(
            row=1, column=3, padx=4, pady=4
        )
        ttk.Label(options_frame, text="Label size:").grid(row=1, column=4, sticky="w", padx=4, pady=4)
        ttk.Spinbox(
            options_frame, from_=6, to=96, textvariable=self.map_label_size_var, width=6
        ).grid(row=1, column=5, padx=4, pady=4, sticky="w")

        # Coastline & borders + colour chooser.
        ttk.Checkbutton(
            options_frame, text="Coastline & borders", variable=self.border_lines_var
        ).grid(row=2, column=0, padx=4, pady=4, sticky="w")
        ttk.Label(options_frame, text="Border colour:").grid(row=2, column=1, sticky="w", padx=4, pady=4)
        self._border_color_entry = ttk.Entry(options_frame, textvariable=self.border_color_var, width=10)
        self._border_color_entry.grid(row=2, column=2, padx=4, pady=4, sticky="w")
        ttk.Button(options_frame, text="Choose...", command=self._choose_border_color).grid(
            row=2, column=3, padx=4, pady=4
        )

        # Flat-map region (kept simple: just the four bound fields + Pick Region).
        region_frame = ttk.LabelFrame(parent, text="Flat-map region (latitude / longitude bounds)")
        region_frame.pack(fill="x", padx=8, pady=4)
        ttk.Label(region_frame, text="Min lat:").grid(row=0, column=0, padx=4, pady=4)
        ttk.Entry(region_frame, textvariable=self.flat_min_lat_var, width=8).grid(row=0, column=1, padx=4, pady=4)
        ttk.Label(region_frame, text="Max lat:").grid(row=0, column=2, padx=4, pady=4)
        ttk.Entry(region_frame, textvariable=self.flat_max_lat_var, width=8).grid(row=0, column=3, padx=4, pady=4)
        ttk.Label(region_frame, text="Min lon:").grid(row=0, column=4, padx=4, pady=4)
        ttk.Entry(region_frame, textvariable=self.flat_min_lon_var, width=8).grid(row=0, column=5, padx=4, pady=4)
        ttk.Label(region_frame, text="Max lon:").grid(row=0, column=6, padx=4, pady=4)
        ttk.Entry(region_frame, textvariable=self.flat_max_lon_var, width=8).grid(row=0, column=7, padx=4, pady=4)
        ttk.Button(region_frame, text="Pick Region (map)", command=self._open_region_picker).grid(
            row=0, column=8, padx=4, pady=4
        )

        # Setup status (Start/Stop live in the bottom button bar).
        status_frame = ttk.LabelFrame(parent, text="Status")
        status_frame.pack(fill="x", padx=8, pady=(4, 8))
        ttk.Label(status_frame, textvariable=self.setup_status_var, wraplength=900, justify="left").pack(
            anchor="w", padx=8, pady=4
        )

    # ---- Advanced tab: performance only -------------------------------------
    def _build_advanced_tab(self, parent: ttk.Frame) -> None:
        perf = ttk.LabelFrame(parent, text="Performance")
        perf.pack(fill="x", padx=8, pady=8)

        ttk.Label(perf, text="Download Workers").grid(row=0, column=0, sticky="w", padx=4, pady=4)
        ttk.Spinbox(perf, from_=1, to=4, textvariable=self.download_workers_var, width=8).grid(
            row=1, column=0, padx=4, pady=4
        )
        ttk.Label(perf, text="Dask Workers").grid(row=0, column=1, sticky="w", padx=4, pady=4)
        ttk.Spinbox(perf, from_=1, to=2, textvariable=self.dask_workers_var, width=8).grid(
            row=1, column=1, padx=4, pady=4
        )
        ttk.Label(perf, text="Dask Chunk Size").grid(row=0, column=2, sticky="w", padx=4, pady=4)
        ttk.Combobox(
            perf, textvariable=self.chunk_var, values=DASK_CHUNK_CHOICES, state="readonly"
        ).grid(row=1, column=2, padx=4, pady=4)
        ttk.Label(perf, text="RAM Limit GiB").grid(row=2, column=0, sticky="w", padx=4, pady=4)
        ttk.Spinbox(
            perf, from_=1, to=64, increment=0.5, textvariable=self.ram_limit_var, width=8
        ).grid(row=3, column=0, padx=4, pady=4)
        ttk.Label(perf, text="Max PNG Pixels").grid(row=2, column=1, sticky="w", padx=4, pady=4)
        ttk.Spinbox(
            perf,
            from_=1_000_000,
            to=200_000_000,
            increment=1_000_000,
            textvariable=self.max_safe_png_pixels_var,
            width=12,
        ).grid(row=3, column=1, padx=4, pady=4)

        self.safe_perf_button = ttk.Button(
            perf, text="Safe Mode", command=lambda: self._apply_performance_recommendation("safe")
        )
        self.safe_perf_button.grid(row=4, column=0, padx=4, pady=8, sticky="w")
        self.best_perf_button = ttk.Button(
            perf, text="Best Performance", command=lambda: self._apply_performance_recommendation("best_performance")
        )
        self.best_perf_button.grid(row=4, column=1, padx=4, pady=8, sticky="w")
        self.gpu_check = ttk.Checkbutton(
            perf, text="Use GPU (Experimental)", variable=self.gpu_acceleration_var, command=self._toggle_gpu_acceleration
        )
        self.gpu_check.grid(row=5, column=0, columnspan=2, padx=4, pady=4, sticky="w")

        note = ttk.Label(
            parent,
            text=(
                "Simple mode keeps satellite layer on 'standard', auto-downloads missing\n"
                "files, and writes no metadata sidecar. Output/temp folders and the\n"
                "resampler use safe defaults. Use the full GUI for advanced control."
            ),
            justify="left",
        )
        note.pack(anchor="w", padx=12, pady=(4, 8))

    # ---- status + log + bottom buttons --------------------------------------
    def _build_status_and_log(self, bottom_side: bool = False) -> None:
        # The log frame packs first (so it sits above the status strip), then
        # the status strip. When bottom_side is True they pack from the bottom
        # so the button bar (packed last from the bottom) ends up at the very
        # bottom and is never squeezed off-screen.
        log_frame = ttk.LabelFrame(self.root, text="Log")
        log_frame.pack(fill="both", expand=True, side=("bottom" if bottom_side else "top"), padx=8, pady=(0, 4))
        self.log_text = tk.Text(log_frame, height=8, state="disabled", wrap="word")
        self.log_text.pack(side="left", fill="both", expand=True)
        log_scroll = ttk.Scrollbar(log_frame, command=self.log_text.yview)
        log_scroll.pack(side="right", fill="y")
        self.log_text.configure(yscrollcommand=log_scroll.set)

        bottom = ttk.Frame(self.root)
        bottom.pack(fill="x", side=("bottom" if bottom_side else "top"), padx=8, pady=(0, 4))
        ttk.Label(bottom, text="Status:").pack(side="left")
        ttk.Label(bottom, textvariable=self.status_var).pack(side="left", padx=(4, 12))
        ttk.Label(bottom, textvariable=self.phase_var).pack(side="left")
        self.progress_bar = ttk.Progressbar(bottom, variable=self.progress_var, maximum=100)
        self.progress_bar.pack(fill="x", side="left", expand=True, padx=8)

    def _build_bottom_buttons(self, bottom_side: bool = False) -> None:
        # Button bar matches the requested layout (left to right):
        # Start Processing | Stop | Open Outputs | Open Last | Copy Paths |
        # Copy Error | Copy Log | Close | Check Env | Quick Fix | Auto Fix |
        # Copy Settings | Update App | Help (?) | Quick Look | Pick Region |
        # Test Data Host
        #
        # The 17 buttons are split across two wrapped rows so they all stay
        # fully visible without a scrollbar (the previous single-row + canvas
        # approach clipped the buttons). Tk's grid wrapping handles narrow
        # windows gracefully.
        outer = ttk.Frame(self.root)
        outer.pack(fill="x", side=("bottom" if bottom_side else "top"), padx=8, pady=(0, 8))

        # (label, command) in the exact requested order.
        button_specs = [
            ("Start Processing", lambda: self._start()),
            ("Stop", lambda: self._stop_current_task()),
            ("Open Outputs", lambda: self._open_output_folder()),
            ("Open Last", lambda: self._open_last_output()),
            ("Copy Paths", lambda: self._copy_output_paths()),
            ("Copy Error", lambda: self._copy_error_report()),
            ("Copy Log", lambda: self._copy_current_log()),
            ("Close", lambda: self._on_close_window()),
            ("Check Env", lambda: self._open_environment_check()),
            ("Quick Fix", lambda: self._open_environment_fix()),
            ("Auto Fix", lambda: self._open_environment_auto_fix()),
            ("Copy Settings", lambda: self._copy_last_settings()),
            ("Update App", lambda: self._update_from_github()),
            ("Help (?)", lambda: self._open_help_window()),
            ("Quick Look", lambda: self._start_preview()),
            ("Pick Region", lambda: self._open_region_picker()),
            ("Test Data Host", lambda: self._start_connectivity_check()),
        ]
        # Split into two rows: first 9 on row 1, remaining 8 on row 2.
        per_row = 9
        for index, (label, command) in enumerate(button_specs):
            r = index // per_row
            c = index % per_row
            btn = ttk.Button(outer, text=label, command=command)
            btn.grid(row=r, column=c, sticky="ew", padx=2, pady=2)
            # Keep references to the ones other methods need to toggle.
            if label == "Start Processing":
                self.start_button = btn
            elif label == "Stop":
                btn.configure(state="disabled")
                self.stop_button = btn
            elif label == "Quick Look":
                self.preview_button = btn
            elif label == "Test Data Host":
                self.test_host_button = btn
        # Make all columns equal-width so the bar looks even.
        total_cols = per_row
        for c in range(total_cols):
            outer.columnconfigure(c, weight=1)

    # ---- colour choosers (save on pick, like the full GUI) ------------------
    def _choose_label_color(self) -> None:
        self._choose_color(self.map_label_color_var, "Choose label colour", MAP_LABEL_COLOR)

    # _choose_border_color is inherited and already saves on pick.

    # ---- refresh mode state: keep start/stop buttons in sync ----------------
    def _refresh_mode_state(self) -> None:
        # Called by the inherited _set_running; keep our Start/Stop buttons wired.
        if self.is_running:
            self.start_button.configure(state="disabled")
            self.stop_button.configure(state="normal")
        else:
            self.start_button.configure(state="normal")
            self.stop_button.configure(state="disabled")


def launch_simple_gui() -> None:
    try:
        from tkinterdnd2 import TkinterDnD  # type: ignore

        root = TkinterDnD.Tk()
    except Exception:
        root = tk.Tk()
    HimawariSimpleApp(root)
    root.mainloop()


if __name__ == "__main__":
    try:
        print(f"Loading {APP_DISPLAY_NAME} (Simple) v{APP_VERSION}...", flush=True)
        launch_simple_gui()
    except KeyboardInterrupt:
        print("\nInterrupted.", file=sys.stderr)
        raise SystemExit(130)
