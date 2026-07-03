#!/usr/bin/env python3
"""Himawari-8/9 Low-RAM Processor - Text User Interface (TUI).

A terminal-based, keyboard-driven front-end to the same low-RAM processing
engine used by the GUI (``himawari_lowram_processor.py``) and the command-line
interface (``himawari_cli.py``). It is meant for systems with no graphical
display: a server over SSH, a minimal Linux install, or anywhere tkinter is not
available.

Highlights:
  * Browse and edit every processing setting from grouped screens.
  * Keyboard driven: arrow keys (or j/k) to move, Enter to select/toggle/edit,
    q or Esc to go back, ? for help.
  * Settings are shared with the GUI (the same ``himawari_gui_settings.json``),
    so a configuration made here also opens in the GUI and vice versa.
  * Check the environment, run a render, or update the program from GitHub,
    all without leaving the terminal.

Launch:
    python himawari_tui.py

If curses is not available (for example on a bare Windows Python without the
``windows-curses`` package) or there is no interactive terminal, the TUI falls
back to the simple text menu in ``himawari_cli.py``.
"""

from __future__ import annotations

import argparse
import importlib.util
import subprocess
import sys
from pathlib import Path
from typing import Any, Callable

# ---------------------------------------------------------------------------
# Locate the processor engine (the canonical file is
# ``himawari_lowram_processor.py``; some renamed installs ship it as
# ``himawari_lowram_processor_claude.py``). We only resolve the path here; the
# heavy import is deferred until launch so this module stays importable (and
# ``--help`` works) even before the scientific dependencies are installed.
# ---------------------------------------------------------------------------
_HERE = Path(__file__).resolve().parent
_PROCESSOR_IMPORT_NAME = "himawari_lowram_processor"
_PROCESSOR_FILENAMES = (
    "himawari_lowram_processor.py",
    "himawari_lowram_processor_claude.py",
)


def _candidate_dirs() -> list[Path]:
    home = Path.home()
    return [
        _HERE,
        _HERE.parent,
        Path.cwd(),
        home / "Downloads",
        home / "Desktop",
        home / "OneDrive" / "Desktop",
        home / "OneDrive" / "Desktop" / "coding projects",
        home / "Desktop" / "coding projects",
    ]


def _find_processor_path() -> Path | None:
    for directory in _candidate_dirs():
        for filename in _PROCESSOR_FILENAMES:
            candidate = directory / filename
            try:
                if candidate.is_file():
                    return candidate
            except OSError:
                continue
    return None


def load_processor() -> Any:
    """Import and return the processor engine module.

    Raises SystemExit with an actionable message if the file cannot be found or
    its dependencies are missing.
    """
    if _PROCESSOR_IMPORT_NAME in sys.modules:
        return sys.modules[_PROCESSOR_IMPORT_NAME]

    path = _find_processor_path()
    if path is None:
        searched = "\n".join(f"  - {d}" for d in _candidate_dirs())
        raise SystemExit(
            "ERROR: could not find the processor engine "
            f"('{_PROCESSOR_FILENAMES[0]}' or '{_PROCESSOR_FILENAMES[1]}').\n"
            "The text interface needs the main processor file next to it. Looked in:\n"
            f"{searched}"
        )

    processor_dir = str(path.parent)
    if processor_dir not in sys.path:
        sys.path.insert(0, processor_dir)

    try:
        spec = importlib.util.spec_from_file_location(_PROCESSOR_IMPORT_NAME, path)
        if spec is None or spec.loader is None:
            raise ImportError(f"could not create an import spec for {path}")
        module = importlib.util.module_from_spec(spec)
        sys.modules[_PROCESSOR_IMPORT_NAME] = module
        spec.loader.exec_module(module)
    except SystemExit:
        raise
    except Exception as exc:  # noqa: BLE001 - report any dependency/import problem clearly
        raise SystemExit(
            f"ERROR: could not load the processing engine: {exc}\n"
            "Make sure the required packages are installed:\n"
            "    python -m pip install -r requirements.txt\n"
            "or run the environment check:\n"
            "    python check_environment.py"
        ) from exc
    return module


# ---------------------------------------------------------------------------
# Field model (pure logic; no curses, no heavy dependencies). These functions
# are deliberately import-light so they can be unit tested directly.
# ---------------------------------------------------------------------------
class Field:
    """One editable setting in a screen.

    kind is one of: "text", "int", "float", "bool", "choice".
    For "choice", ``choices`` is the list of allowed values.
    """

    __slots__ = ("attr", "label", "kind", "choices", "help")

    def __init__(
        self,
        attr: str,
        label: str,
        kind: str,
        choices: list[str] | None = None,
        help: str = "",
    ) -> None:
        self.attr = attr
        self.label = label
        self.kind = kind
        self.choices = choices or []
        self.help = help


def format_value(kind: str, value: Any) -> str:
    """Render a setting value for display."""
    if kind == "bool":
        return "\u25cf ON " if bool(value) else "\u25cb off"
    if value is None:
        return ""
    text = str(value)
    if not text:
        return "(empty)"
    return text


def coerce_value(kind: str, raw: str, current: Any) -> tuple[bool, Any, str]:
    """Convert text typed by the user into the proper type.

    Returns ``(ok, value, error_message)``. On failure ``ok`` is False and
    ``value`` is the unchanged ``current`` value.
    """
    raw = raw.strip()
    if kind == "text":
        return True, raw, ""
    if kind == "int":
        try:
            return True, int(raw), ""
        except ValueError:
            return False, current, f"'{raw}' is not a whole number."
    if kind == "float":
        try:
            return True, float(raw), ""
        except ValueError:
            return False, current, f"'{raw}' is not a number."
    # bool / choice are not edited through a text prompt.
    return False, current, f"Cannot edit a {kind} field by typing."


def cycle_choice(choices: list[str], current: Any, direction: int) -> Any:
    """Return the next/previous value in ``choices`` (wrapping)."""
    if not choices:
        return current
    try:
        index = choices.index(current)
    except ValueError:
        index = 0
        return choices[0]
    return choices[(index + direction) % len(choices)]


def build_screens(processor: Any) -> list[tuple[str, list[Field]]]:
    """Build the grouped settings screens using the engine's live choice lists."""
    composites = sorted(processor.COMPOSITE_BANDS.keys())
    layer_modes = list(processor.SATELLITE_LAYER_MODES)
    chunk_choices = list(processor.DASK_CHUNK_CHOICES)
    crosshair_types = list(processor.CROSSHAIR_TYPES)
    overlay_themes = list(processor.OVERLAY_THEMES.keys())
    custom_theme = getattr(processor, "OVERLAY_THEME_CUSTOM", "Custom (keep my colors)")
    if custom_theme not in overlay_themes:
        overlay_themes = [custom_theme, *overlay_themes]

    source_output = (
        "Source & Output",
        [
            Field("user_url", "Source URL", "text", help="NOAA AWS Himawari AHI folder URL (blank = latest)."),
            Field("mode", "Mode", "choice", ["Single Image", "Timelapse"]),
            Field("composite_choice", "Product / band", "choice", composites),
            Field("satellite_layer_mode", "Satellite layer", "choice", layer_modes,
                  help="standard, live (latest scan), or hd (Zoom Earth style)."),
            Field("hours_back", "Timelapse hours back", "int"),
            Field("interval_minutes", "Timelapse interval (min)", "int"),
            Field("fps", "Timelapse fps", "int"),
            Field("timelapse_format", "Timelapse format", "choice", ["gif", "mp4"]),
            Field("image_format", "Image format", "choice", ["png", "tif"]),
            Field("output_template", "Output filename template", "text"),
        ],
    )

    map_overlays = (
        "Map & Overlays",
        [
            Field("map_view", "Map view", "choice", ["native", "flat"]),
            Field("zoom_earth_style", "Zoom Earth style flat map", "bool"),
            Field("flat_min_lat", "Flat map min latitude", "float"),
            Field("flat_max_lat", "Flat map max latitude", "float"),
            Field("flat_min_lon", "Flat map min longitude", "float"),
            Field("flat_max_lon", "Flat map max longitude", "float"),
            Field("flat_resolution_deg", "Flat map resolution (deg/px)", "float"),
            Field("add_border_lines", "Coastlines & borders", "bool"),
            Field("border_line_color", "Border colour", "text"),
            Field("border_line_width", "Border width", "float"),
            Field("add_map_labels", "Map labels", "bool"),
            Field("map_label_size", "Label size", "int"),
            Field("map_label_color", "Label colour", "text"),
            Field("add_night_boundary", "Night boundary line", "bool"),
            Field("night_boundary_color", "Night boundary colour", "text"),
            Field("add_crosshair", "Crosshair", "bool"),
            Field("crosshair_type", "Crosshair type", "choice", crosshair_types),
            Field("crosshair_color", "Crosshair colour", "text"),
            Field("add_typhoon_tracks", "Typhoon tracks (when data available)", "bool"),
            Field("typhoon_track_color", "Typhoon track colour", "text"),
            Field("typhoon_data_source", "Typhoon data (auto/file/URL)", "text"),
            Field("overlay_theme", "Overlay theme", "choice", overlay_themes),
        ],
    )

    processing = (
        "Processing & Performance",
        [
            Field("auto_download", "Auto-download missing files", "bool"),
            Field("gpu_acceleration", "GPU acceleration", "bool"),
            Field("use_night_fallback", "Night fallback", "bool"),
            Field("night_fallback_mode", "Night fallback mode", "choice", ["hybrid", "whole_frame_ir"]),
            Field("download_workers", "Download workers", "int"),
            Field("dask_num_workers", "Dask workers", "int"),
            Field("dask_chunk_size", "Dask chunk size", "choice", chunk_choices),
            Field("ram_limit_gb", "RAM limit (GiB)", "float"),
            Field("max_safe_png_pixels", "Max safe PNG pixels", "int"),
            Field("resampler", "Resampler", "choice", ["native", "nearest"]),
            Field("delete_timelapse_frames", "Delete timelapse frames", "bool"),
            Field("allow_quality_fallback", "Allow quality fallback", "bool"),
            Field("segment_aware_downloads", "Segment-aware downloads", "bool"),
            Field("write_metadata_sidecar", "Write metadata sidecar", "bool"),
        ],
    )

    return [source_output, map_overlays, processing]


# ---------------------------------------------------------------------------
# Application state (independent of curses).
# ---------------------------------------------------------------------------
class TuiState:
    def __init__(self, processor: Any) -> None:
        self.processor = processor
        loaded = processor.load_gui_settings()
        if loaded:
            config, output_dir, temp_dir = loaded
        else:
            config = processor.default_config()
            output_dir = processor.OUTPUT_DIR
            temp_dir = processor.TEMP_DIR
        # Keep an editable plain dict; rebuild a ProcessorConfig when needed.
        self.values: dict[str, Any] = dict(config.__dict__)
        self.output_dir = Path(output_dir)
        self.temp_dir = Path(temp_dir)
        self.screens = build_screens(processor)
        self.dirty = False

    def build_config(self) -> Any:
        return self.processor.ProcessorConfig(**self.values)

    def save(self) -> str:
        try:
            self.processor.save_gui_settings(self.build_config(), self.output_dir, self.temp_dir)
            self.dirty = False
            return f"Saved settings to {self.processor.GUI_SETTINGS_FILE}"
        except Exception as exc:  # noqa: BLE001
            return f"Could not save settings: {exc}"

    def apply_output_temp(self) -> None:
        """Point the engine at the chosen output/temp folders for this run."""
        self.processor.OUTPUT_DIR = self.output_dir
        self.processor.TEMP_DIR = self.temp_dir


# ---------------------------------------------------------------------------
# Shared "leave the screen, run something with normal stdout" helpers. Used for
# the environment check, a render, and the self-update.
# ---------------------------------------------------------------------------
def run_processing(state: TuiState) -> None:
    processor = state.processor
    state.apply_output_temp()
    try:
        config = state.build_config()
        processor.validate_configuration(config)
    except Exception as exc:  # noqa: BLE001
        print(f"Cannot start: {exc}")
        return
    eta = processor.ProgressEtaEstimator()

    def progress(message: str, current: int | None, total: int | None) -> None:
        text = eta.update(message, current, total)
        if current is not None and total:
            suffix = f" ({text})" if text else ""
            print(f"[{current}/{total}] {message}{suffix}")
        else:
            print(message)

    print("Starting render. Press Ctrl+C to cancel.\n")
    try:
        outputs = processor.run(config, progress=progress)
    except KeyboardInterrupt:
        print("\nCancelled.")
        return
    except Exception as exc:  # noqa: BLE001
        print(f"\nRender failed: {exc}")
        return
    print("\nFinished. Outputs:")
    for output in outputs:
        print(f"  {output}")


def run_true_color_styles(state: TuiState) -> None:
    """Render True Color Reproduction across all nine layer/style cells
    (standard/live/HD satellite layers x native/flat/Zoom Earth map styles)."""
    processor = state.processor
    state.apply_output_temp()
    try:
        config = state.build_config()
        base = processor.true_color_style_base_config(config)
        processor.validate_configuration(base)
    except Exception as exc:  # noqa: BLE001
        print(f"Cannot start: {exc}")
        return
    eta = processor.ProgressEtaEstimator()

    def progress(message: str, current: int | None, total: int | None) -> None:
        text = eta.update(message, current, total)
        if current is not None and total:
            suffix = f" ({text})" if text else ""
            print(f"[{current}/{total}] {message}{suffix}")
        else:
            print(message)

    print(
        f"Rendering True Color Reproduction in {len(processor.TRUE_COLOR_STYLE_SET)} "
        "cells: standard/live/HD satellite layers, each as native / flat / Zoom "
        "Earth-style.\nPress Ctrl+C to cancel.\n"
    )
    try:
        outputs = processor.run_true_color_style_set(config, progress=progress)
    except KeyboardInterrupt:
        print("\nCancelled.")
        return
    except Exception as exc:  # noqa: BLE001
        print(f"\nFailed: {exc}")
        return
    print("\nFinished. Outputs:")
    for output in outputs:
        print(f"  {output}")


def run_environment_check(state: TuiState) -> None:
    script = _HERE / "check_environment.py"
    if not script.is_file():
        # fall back to the directory the engine actually loaded from
        script = Path(state.processor.PROJECT_DIR) / "check_environment.py"
    print("Running environment check...\n")
    try:
        subprocess.call([sys.executable, str(script)], cwd=str(script.parent))
    except Exception as exc:  # noqa: BLE001
        print(f"Could not run the environment check: {exc}")


def run_self_update(state: TuiState) -> None:
    processor = state.processor
    branch = getattr(processor, "GITHUB_QUICK_FIX_BRANCH", "main")
    print(f"Updating from the '{branch}' branch of github.com/{processor.GITHUB_REPO} ...\n")
    try:
        summary = processor.perform_self_update(
            Path(processor.PROJECT_DIR), log=print, branch=branch
        )
    except Exception as exc:  # noqa: BLE001
        print(f"Update failed: {exc}")
        return
    info = summary if isinstance(summary, dict) else {}
    updated = ", ".join(info.get("updated", []) or []) or "(none)"
    backup_dir = info.get("backup_dir", "")
    print("\nUpdate complete.")
    print(f"  Updated files: {updated}")
    if backup_dir:
        print(f"  Backup saved to: {backup_dir}")
    print("  Please restart the text interface to use the new version.")


# ---------------------------------------------------------------------------
# curses front-end. All curses calls live below this line so the rest of the
# module can be imported and tested without a terminal.
# ---------------------------------------------------------------------------

# Color pair IDs (initialized in _init_colors)
_CP_TITLE = 1
_CP_HEADER = 2
_CP_HIGHLIGHT = 3
_CP_STATUS = 4
_CP_DIM = 5
_CP_ON = 6
_CP_OFF = 7
_CP_BORDER = 8
_CP_SECTION = 9
_CP_SELECTED = 10


def _init_colors() -> None:
    """Initialize the color palette. Safe to call even if the terminal has no
    color support — all drawing degrades to attributes gracefully."""
    import curses

    if not curses.has_colors():
        return
    curses.start_color()
    curses.use_default_colors()
    #            ID              FG              BG
    curses.init_pair(_CP_TITLE,     curses.COLOR_CYAN,    -1)
    curses.init_pair(_CP_HEADER,    curses.COLOR_WHITE,   curses.COLOR_BLUE)
    curses.init_pair(_CP_HIGHLIGHT, curses.COLOR_YELLOW,  -1)
    curses.init_pair(_CP_STATUS,    curses.COLOR_GREEN,   -1)
    curses.init_pair(_CP_DIM,       curses.COLOR_WHITE,   -1)
    curses.init_pair(_CP_ON,        curses.COLOR_GREEN,   -1)
    curses.init_pair(_CP_OFF,       curses.COLOR_WHITE,   -1)
    curses.init_pair(_CP_BORDER,    curses.COLOR_CYAN,    -1)
    curses.init_pair(_CP_SECTION,   curses.COLOR_MAGENTA, -1)
    curses.init_pair(_CP_SELECTED,  curses.COLOR_BLACK,   curses.COLOR_CYAN)


def _safe_addstr(win: Any, y: int, x: int, text: str, attr: int = 0) -> None:
    """addstr that never raises if the text would run past the window edge."""
    import curses

    try:
        max_y, max_x = win.getmaxyx()
        if y < 0 or y >= max_y or x < 0 or x >= max_x:
            return
        win.addstr(y, x, text[: max_x - x - 1], attr)
    except curses.error:
        pass


def _hline(stdscr: Any, y: int, width: int) -> None:
    """Draw a horizontal line across the screen."""
    import curses

    max_y, max_x = stdscr.getmaxyx()
    if y < 0 or y >= max_y:
        return
    line = "\u2500" * min(width, max_x)
    _safe_addstr(stdscr, y, 0, line, curses.color_pair(_CP_BORDER))


def _box(stdscr: Any, y: int, x: int, w: int, h: int) -> None:
    """Draw a box with rounded/Unicode corners."""
    import curses

    max_y, max_x = stdscr.getmaxyx()
    attr = curses.color_pair(_CP_BORDER)
    if y < 0 or y >= max_y or x < 0 or x >= max_x:
        return
    # top
    _safe_addstr(stdscr, y, x, "\u250c" + "\u2500" * (w - 2) + "\u2510", attr)
    # sides
    for row in range(1, h - 1):
        if y + row >= max_y:
            break
        _safe_addstr(stdscr, y + row, x, "\u2502", attr)
        if x + w - 1 < max_x:
            _safe_addstr(stdscr, y + row, x + w - 1, "\u2502", attr)
    # bottom
    if y + h - 1 < max_y:
        _safe_addstr(stdscr, y + h - 1, x, "\u2514" + "\u2500" * (w - 2) + "\u2518", attr)


def _draw_header(stdscr: Any, title: str, subtitle: str = "") -> int:
    """Draw a styled header bar. Returns the Y coordinate where content starts."""
    import curses

    max_y, max_x = stdscr.getmaxyx()
    # Title bar (full width, inverse video)
    _safe_addstr(stdscr, 0, 0, " " * max_x, curses.color_pair(_CP_HEADER))
    padded = f" {title} "
    start_x = max(0, (max_x - len(padded)) // 2)
    _safe_addstr(stdscr, 0, start_x, padded[:max_x - 1], curses.color_pair(_CP_HEADER) | curses.A_BOLD)
    if subtitle:
        _safe_addstr(stdscr, 1, 0, " " * max_x, curses.color_pair(_CP_HEADER))
        _safe_addstr(stdscr, 1, 0, f" {subtitle}"[:max_x - 1], curses.color_pair(_CP_HEADER) | curses.A_DIM)
        return 3
    return 2


def _draw_statusbar(stdscr: Any, text: str, help_text: str = "") -> None:
    """Draw a status bar at the bottom of the screen."""
    import curses

    max_y, max_x = stdscr.getmaxyx()
    if max_y < 2:
        return
    # Status line
    _safe_addstr(stdscr, max_y - 2, 0, " " * max_x, curses.color_pair(_CP_STATUS))
    _safe_addstr(stdscr, max_y - 2, 0, f" {text}"[:max_x - 1], curses.color_pair(_CP_STATUS) | curses.A_BOLD)
    # Help line
    if help_text:
        _safe_addstr(stdscr, max_y - 1, 0, " " * max_x, curses.color_pair(_CP_DIM))
        _safe_addstr(stdscr, max_y - 1, 0, f" {help_text}"[:max_x - 1], curses.color_pair(_CP_DIM))


def _menu(stdscr: Any, title: str, items: list[str], subtitle: str = "", start: int = 0) -> int:
    """Generic vertical menu. Returns the chosen index, or -1 to go back."""
    import curses

    pos = max(0, min(start, len(items) - 1))
    while True:
        stdscr.erase()
        max_y, max_x = stdscr.getmaxyx()

        top = _draw_header(stdscr, title, subtitle)

        # Calculate visible area
        visible = max(1, max_y - top - 3)  # leave room for header + status
        first = 0
        if pos >= visible:
            first = pos - visible + 1

        for offset in range(visible):
            index = first + offset
            if index >= len(items):
                break
            item = items[index]
            is_selected = index == pos

            if item == "-" or item == "\u2500" * len(item):
                _hline(stdscr, top + offset, max_x)
                continue

            if is_selected:
                attr = curses.color_pair(_CP_SELECTED) | curses.A_BOLD
                prefix = " \u25b6 "
            else:
                attr = 0
                prefix = "   "

            text = f"{prefix}{item}"
            _safe_addstr(stdscr, top + offset, 0, text[:max_x - 1], attr)

        _draw_statusbar(stdscr, "Enter select | q/Esc back | ? help")
        stdscr.refresh()

        key = stdscr.getch()
        if key in (curses.KEY_UP, ord("k")):
            pos = (pos - 1) % len(items)
        elif key in (curses.KEY_DOWN, ord("j")):
            pos = (pos + 1) % len(items)
        elif key in (curses.KEY_HOME,):
            pos = 0
        elif key in (curses.KEY_END,):
            pos = len(items) - 1
        elif key in (curses.KEY_ENTER, ord("\n"), ord("\r"), ord(" ")):
            return pos
        elif key in (27, ord("q")):  # Esc or q
            return -1
        elif key == ord("?"):
            _show_help(stdscr)


def _show_help(stdscr: Any) -> None:
    import curses

    lines = [
        ("Himawari TUI - Keyboard Reference", _CP_TITLE),
        ("", 0),
        ("Navigation", _CP_SECTION),
        ("  \u2191 / \u2193  or  k / j     move the selection", 0),
        ("  Home / End             jump to first / last", 0),
        ("  Enter or Space         select / toggle / edit", 0),
        ("  q or Esc               go back / cancel", 0),
        ("", 0),
        ("Editing a setting", _CP_SECTION),
        ("  \u25cf / \u25cb toggles      Enter toggles booleans", 0),
        ("  List settings          Left/Right cycle values", 0),
        ("  Text / number fields   Enter opens a prompt", 0),
        ("                         Backspace edits, Enter confirms", 0),
        ("", 0),
        ("Press any key to return.", _CP_DIM),
    ]
    stdscr.erase()
    max_y, max_x = stdscr.getmaxyx()
    for i, (line, attr_id) in enumerate(lines):
        if i >= max_y:
            break
        attr = curses.color_pair(attr_id) if attr_id else curses.A_BOLD if i == 0 else 0
        _safe_addstr(stdscr, i, 0, line[:max_x - 1], attr)
    stdscr.refresh()
    stdscr.getch()


def _prompt_line(stdscr: Any, prompt: str, initial: str = "") -> str | None:
    """Read a line of text at the bottom of the screen. Esc cancels (None)."""
    import curses

    buf = list(initial)
    max_y, max_x = stdscr.getmaxyx()
    y = max_y - 1
    curses.curs_set(1)
    try:
        while True:
            stdscr.move(y, 0)
            stdscr.clrtoeol()
            shown = (prompt + "".join(buf))[: max_x - 1]
            _safe_addstr(stdscr, y, 0, shown, curses.color_pair(_CP_HIGHLIGHT))
            cursor_x = min(len(prompt) + len(buf), max_x - 1)
            try:
                stdscr.move(y, cursor_x)
            except curses.error:
                pass
            stdscr.refresh()
            try:
                ch = stdscr.get_wch()
            except curses.error:
                continue
            if isinstance(ch, str):
                if ch in ("\n", "\r"):
                    return "".join(buf)
                if ch == "\x1b":  # Esc
                    return None
                if ch in ("\x7f", "\b"):
                    if buf:
                        buf.pop()
                    continue
                if ch.isprintable():
                    buf.append(ch)
            else:
                if ch in (curses.KEY_ENTER,):
                    return "".join(buf)
                if ch in (curses.KEY_BACKSPACE, 127, 8):
                    if buf:
                        buf.pop()
    finally:
        curses.curs_set(0)


def _flash(stdscr: Any, message: str) -> None:
    """Show a transient message on the second-to-last line."""
    import curses

    max_y, max_x = stdscr.getmaxyx()
    _safe_addstr(stdscr, max_y - 2, 0, " " * max_x, curses.color_pair(_CP_STATUS))
    _safe_addstr(stdscr, max_y - 2, 0, message[: max_x - 1], curses.color_pair(_CP_STATUS) | curses.A_BOLD)
    stdscr.refresh()


def _pick_choice(stdscr: Any, field: Field, current: Any) -> Any:
    start = field.choices.index(current) if current in field.choices else 0
    index = _menu(stdscr, f"Select: {field.label}", field.choices, start=start)
    if index < 0:
        return current
    return field.choices[index]


def _edit_field(stdscr: Any, state: TuiState, field: Field) -> str:
    """Edit one field in place. Returns a short status message."""
    current = state.values.get(field.attr)
    if field.kind == "bool":
        state.values[field.attr] = not bool(current)
        state.dirty = True
        val = format_value("bool", state.values[field.attr])
        return f"{field.label}: {val}"
    if field.kind == "choice":
        chosen = _pick_choice(stdscr, field, current)
        if chosen != current:
            state.values[field.attr] = chosen
            state.dirty = True
        return f"{field.label}: {chosen}"
    # text / int / float
    raw = _prompt_line(stdscr, f"{field.label} = ", "" if current is None else str(current))
    if raw is None:
        return "Cancelled."
    ok, value, error = coerce_value(field.kind, raw, current)
    if not ok:
        return error
    state.values[field.attr] = value
    state.dirty = True
    return f"{field.label}: {format_value(field.kind, value)}"


def _settings_screen(stdscr: Any, state: TuiState, title: str, fields: list[Field]) -> None:
    import curses

    pos = 0
    status = ""
    while True:
        stdscr.erase()
        max_y, max_x = stdscr.getmaxyx()

        top = _draw_header(stdscr, title, "Enter edit | Left/Right cycle | q/Esc back")

        visible = max(1, max_y - top - 3)
        first = 0
        if pos >= visible:
            first = pos - visible + 1

        # Calculate column widths
        label_width = min(34, max(len(f.label) for f in fields) + 2)
        for offset in range(visible):
            index = first + offset
            if index >= len(fields):
                break
            field = fields[index]
            raw_val = state.values.get(field.attr)
            value = format_value(field.kind, raw_val)
            is_selected = index == pos

            if field.kind == "bool":
                # Colorize boolean values
                val_attr = curses.color_pair(_CP_ON) if raw_val else curses.color_pair(_CP_OFF)
            elif field.kind == "choice":
                val_attr = curses.color_pair(_CP_HIGHLIGHT)
            else:
                val_attr = 0

            label_part = f"  {field.label:<{label_width}}"
            value_part = f"{value}"

            if is_selected:
                line = f"\u25b6 {field.label:<{label_width}} {value_part}"
                _safe_addstr(stdscr, top + offset, 0, f"\u25b6 ", curses.color_pair(_CP_SELECTED) | curses.A_BOLD)
                _safe_addstr(stdscr, top + offset, 2, f"{field.label:<{label_width}}", curses.color_pair(_CP_SELECTED) | curses.A_BOLD)
                _safe_addstr(stdscr, top + offset, label_width + 3, value_part, curses.color_pair(_CP_SELECTED) | val_attr)
            else:
                line = f"  {field.label:<{label_width}} {value_part}"
                _safe_addstr(stdscr, top + offset, 0, f"  {field.label:<{label_width}}", 0)
                _safe_addstr(stdscr, top + offset, label_width + 3, value_part, val_attr)

        if status:
            _draw_statusbar(stdscr, status, fields[pos].help or "q/Esc to go back")
        else:
            _draw_statusbar(stdscr, "", fields[pos].help or "q/Esc to go back")
        stdscr.refresh()

        key = stdscr.getch()
        if key in (curses.KEY_UP, ord("k")):
            pos = (pos - 1) % len(fields)
            status = ""
        elif key in (curses.KEY_DOWN, ord("j")):
            pos = (pos + 1) % len(fields)
            status = ""
        elif key in (curses.KEY_HOME,):
            pos = 0
        elif key in (curses.KEY_END,):
            pos = len(fields) - 1
        elif key in (curses.KEY_LEFT, curses.KEY_RIGHT):
            field = fields[pos]
            if field.kind == "bool":
                state.values[field.attr] = not bool(state.values.get(field.attr))
                state.dirty = True
                status = f"{field.label}: {format_value('bool', state.values[field.attr])}"
            elif field.kind == "choice":
                direction = 1 if key == curses.KEY_RIGHT else -1
                state.values[field.attr] = cycle_choice(field.choices, state.values.get(field.attr), direction)
                state.dirty = True
                status = f"{field.label}: {state.values[field.attr]}"
        elif key in (curses.KEY_ENTER, ord("\n"), ord("\r"), ord(" ")):
            status = _edit_field(stdscr, state, fields[pos])
        elif key in (27, ord("q")):
            return
        elif key == ord("?"):
            _show_help(stdscr)


def _folders_screen(stdscr: Any, state: TuiState) -> None:
    while True:
        items = [
            f"Output folder: {state.output_dir}",
            f"Temp folder:   {state.temp_dir}",
            "\u2500" * 40,
            "Back",
        ]
        index = _menu(stdscr, "Output & temp folders", items, "Enter to change a folder")
        if index in (-1, 3):
            return
        if index == 0:
            raw = _prompt_line(stdscr, "Output folder = ", str(state.output_dir))
            if raw is not None and raw.strip():
                state.output_dir = Path(raw.strip()).expanduser()
                state.dirty = True
        elif index == 1:
            raw = _prompt_line(stdscr, "Temp folder = ", str(state.temp_dir))
            if raw is not None and raw.strip():
                state.temp_dir = Path(raw.strip()).expanduser()
                state.dirty = True


def _review_screen(stdscr: Any, state: TuiState) -> None:
    import curses

    lines: list[str] = [
        f"  Output folder: {state.output_dir}",
        f"  Temp folder:   {state.temp_dir}",
        "",
    ]
    for key in sorted(state.values.keys()):
        val = state.values[key]
        if isinstance(val, bool):
            display = format_value("bool", val)
        else:
            display = str(val)
        lines.append(f"  {key} = {display}")
    pos = 0
    while True:
        stdscr.erase()
        max_y, max_x = stdscr.getmaxyx()

        top = _draw_header(stdscr, "All Settings", "Scroll with Up/Down | q/Esc back")

        body = max_y - top - 3
        for offset in range(body):
            index = pos + offset
            if index >= len(lines):
                break
            line = lines[index]
            # Colorize bool values in review
            if " = " in line:
                key_part, val_part = line.split(" = ", 1)
                if val_part.strip() in ("\u25cf ON ", "\u25cb off"):
                    attr = curses.color_pair(_CP_ON) if "\u25cf" in val_part else curses.color_pair(_CP_OFF)
                    _safe_addstr(stdscr, top + offset, 0, f"{key_part} = ", 0)
                    _safe_addstr(stdscr, top + offset, len(key_part) + 3, val_part[:max_x - len(key_part) - 4], attr)
                    continue
            _safe_addstr(stdscr, top + offset, 0, line[:max_x - 1])

        _draw_statusbar(stdscr, "Up/Down scroll | q/Esc back")
        stdscr.refresh()
        key = stdscr.getch()
        if key in (curses.KEY_UP, ord("k")):
            pos = max(0, pos - 1)
        elif key in (curses.KEY_DOWN, ord("j")):
            pos = min(max(0, len(lines) - 1), pos + 1)
        elif key in (curses.KEY_NPAGE,):
            pos = min(max(0, len(lines) - 1), pos + body)
        elif key in (curses.KEY_PPAGE,):
            pos = max(0, pos - body)
        elif key in (27, ord("q")):
            return


def _run_external(stdscr: Any, action: Callable[[], None]) -> None:
    """Leave curses, run a console action, then wait and resume curses."""
    import curses

    curses.def_prog_mode()
    curses.endwin()
    print("\n" + "=" * 60)
    try:
        action()
    except Exception as exc:  # noqa: BLE001 - never let an action kill the TUI
        print(f"\nError: {exc}")
    print("=" * 60)
    try:
        input("\nPress Enter to return to the menu...")
    except (EOFError, KeyboardInterrupt):
        pass
    curses.reset_prog_mode()
    stdscr.refresh()


def _confirm(stdscr: Any, question: str) -> bool:
    index = _menu(stdscr, question, ["Yes", "No"], start=1)
    return index == 0


def _main_loop(stdscr: Any, state: TuiState) -> None:
    import curses

    curses.curs_set(0)
    stdscr.keypad(True)
    _init_colors()

    title = state.processor.app_version_label()
    subtitle = "Text Interface"
    status = ""
    while True:
        unsaved = " \u2022 unsaved" if state.dirty else ""
        items = [
            "Source & Output settings",
            "Map & Overlay settings",
            "Processing & Performance settings",
            "\u2500" * 40,
            "Output & temp folders",
            "Review all settings",
            "Save settings",
            "\u2500" * 40,
            "Check environment",
            "Run render",
            "Render 9 true color styles",
            "Update program (GitHub)",
            "\u2500" * 40,
            "Quit",
        ]
        display_status = status or "Choose an action." + unsaved
        index = _menu(stdscr, title, items, display_status)
        status = ""
        if index in (-1, 14):  # Esc/q or Quit
            if state.dirty and _confirm(stdscr, "Save changes before quitting?"):
                status = state.save()
            return
        elif index == 0:
            _settings_screen(stdscr, state, "Source & Output", state.screens[0][1])
        elif index == 1:
            _settings_screen(stdscr, state, "Map & Overlays", state.screens[1][1])
        elif index == 2:
            _settings_screen(stdscr, state, "Processing & Performance", state.screens[2][1])
        elif index == 4:
            _folders_screen(stdscr, state)
        elif index == 5:
            _review_screen(stdscr, state)
        elif index == 6:
            status = state.save()
        elif index == 8:
            _run_external(stdscr, lambda: run_environment_check(state))
        elif index == 9:
            if state.dirty:
                state.save()
            _run_external(stdscr, lambda: run_processing(state))
        elif index == 10:
            if state.dirty:
                state.save()
            _run_external(stdscr, lambda: run_true_color_styles(state))
        elif index == 11:
            if _confirm(stdscr, "Download and install the latest code from main?"):
                _run_external(stdscr, lambda: run_self_update(state))


# ---------------------------------------------------------------------------
# Entry points and graceful fallback.
# ---------------------------------------------------------------------------
def _fallback_to_cli(processor: Any) -> int:
    print("The curses text interface is unavailable; using the simple text menu instead.")
    try:
        import himawari_cli  # noqa: WPS433 - intentional local import for fallback
    except Exception as exc:  # noqa: BLE001
        print(f"Could not load the command-line fallback: {exc}")
        print("You can run it directly with:  python himawari_cli.py --menu")
        return 1
    loaded = processor.load_gui_settings()
    config = loaded[0] if loaded else processor.default_config()
    return himawari_cli.interactive_menu(config)


def launch_tui(processor: Any) -> int:
    try:
        import curses
    except Exception:  # noqa: BLE001 - e.g. Windows without windows-curses
        return _fallback_to_cli(processor)

    if not (sys.stdin.isatty() and sys.stdout.isatty()):
        return _fallback_to_cli(processor)

    state = TuiState(processor)
    try:
        curses.wrapper(_main_loop, state)
    except Exception as exc:  # noqa: BLE001 - terminal too small, no color, etc.
        print(f"The text interface could not start ({exc}).")
        return _fallback_to_cli(processor)
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Terminal (text) interface for the Himawari-8/9 low-RAM processor.",
    )
    parser.add_argument(
        "--no-tui",
        action="store_true",
        help="Skip curses and use the simple command-line menu directly.",
    )
    args = parser.parse_args(argv)

    processor = load_processor()
    if args.no_tui:
        return _fallback_to_cli(processor)
    return launch_tui(processor)


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        print("\nInterrupted.", file=sys.stderr)
        raise SystemExit(130)
