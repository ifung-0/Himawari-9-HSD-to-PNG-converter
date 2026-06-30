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
        return "on" if bool(value) else "off"
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
    """Render True Color Reproduction in all three map styles (native, standard
    flat, Zoom Earth-style flat)."""
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
        "Rendering True Color Reproduction in 3 map styles: native, standard "
        "flat, Zoom Earth-style flat.\nPress Ctrl+C to cancel.\n"
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


def _menu(stdscr: Any, title: str, items: list[str], subtitle: str = "", start: int = 0) -> int:
    """Generic vertical menu. Returns the chosen index, or -1 to go back."""
    import curses

    pos = max(0, min(start, len(items) - 1))
    while True:
        stdscr.erase()
        max_y, max_x = stdscr.getmaxyx()
        _safe_addstr(stdscr, 0, 0, title, curses.A_BOLD)
        if subtitle:
            _safe_addstr(stdscr, 1, 0, subtitle, curses.A_DIM)
        top = 3
        # Scroll if the list is taller than the window.
        visible = max(1, max_y - top - 2)
        first = 0
        if pos >= visible:
            first = pos - visible + 1
        for offset in range(visible):
            index = first + offset
            if index >= len(items):
                break
            marker = "> " if index == pos else "  "
            attr = curses.A_REVERSE if index == pos else 0
            _safe_addstr(stdscr, top + offset, 0, f"{marker}{items[index]}", attr)
        _safe_addstr(
            stdscr,
            max_y - 1,
            0,
            "Up/Down move - Enter select - q/Esc back - ? help",
            curses.A_DIM,
        )
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
        "Himawari TUI - help",
        "",
        "Navigation",
        "  Up / Down or k / j   move the selection",
        "  Home / End           jump to first / last",
        "  Enter or Space       select / toggle / edit",
        "  q or Esc             go back / cancel",
        "",
        "Editing a setting",
        "  on/off settings      Enter toggles them",
        "  list settings        Left/Right cycle; Enter opens a picker",
        "  text/number settings Enter opens a prompt; type, Backspace edits,",
        "                       Enter confirms, Esc cancels",
        "",
        "Press any key to return.",
    ]
    stdscr.erase()
    for i, line in enumerate(lines):
        attr = curses.A_BOLD if i == 0 else 0
        _safe_addstr(stdscr, i, 0, line, attr)
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
            _safe_addstr(stdscr, y, 0, shown)
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

    max_y, _ = stdscr.getmaxyx()
    _safe_addstr(stdscr, max_y - 2, 0, message, curses.A_BOLD)
    stdscr.refresh()


def _pick_choice(stdscr: Any, field: Field, current: Any) -> Any:
    start = field.choices.index(current) if current in field.choices else 0
    index = _menu(stdscr, f"Choose: {field.label}", field.choices, start=start)
    if index < 0:
        return current
    return field.choices[index]


def _edit_field(stdscr: Any, state: TuiState, field: Field) -> str:
    """Edit one field in place. Returns a short status message."""
    current = state.values.get(field.attr)
    if field.kind == "bool":
        state.values[field.attr] = not bool(current)
        state.dirty = True
        return f"{field.label}: {format_value('bool', state.values[field.attr])}"
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
        _safe_addstr(stdscr, 0, 0, title, curses.A_BOLD)
        _safe_addstr(stdscr, 1, 0, "Enter edits - Left/Right cycle lists - q/Esc back", curses.A_DIM)
        top = 3
        visible = max(1, max_y - top - 2)
        first = 0
        if pos >= visible:
            first = pos - visible + 1
        label_width = min(36, max(len(f.label) for f in fields) + 2)
        for offset in range(visible):
            index = first + offset
            if index >= len(fields):
                break
            field = fields[index]
            value = format_value(field.kind, state.values.get(field.attr))
            line = f"{field.label:<{label_width}} {value}"
            attr = curses.A_REVERSE if index == pos else 0
            _safe_addstr(stdscr, top + offset, 0, line, attr)
        if status:
            _safe_addstr(stdscr, max_y - 2, 0, status[: max_x - 1], curses.A_BOLD)
        hint = fields[pos].help
        _safe_addstr(stdscr, max_y - 1, 0, (hint or "q/Esc to go back")[: max_x - 1], curses.A_DIM)
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
            "Back",
        ]
        index = _menu(stdscr, "Output & temp folders", items, "Enter to change a folder")
        if index in (-1, 2):
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

    lines: list[str] = [f"Output folder: {state.output_dir}", f"Temp folder:   {state.temp_dir}", ""]
    for key in sorted(state.values.keys()):
        lines.append(f"{key} = {state.values[key]}")
    pos = 0
    while True:
        stdscr.erase()
        max_y, max_x = stdscr.getmaxyx()
        _safe_addstr(stdscr, 0, 0, "Current settings", curses.A_BOLD)
        body = max_y - 3
        for offset in range(body):
            index = pos + offset
            if index >= len(lines):
                break
            _safe_addstr(stdscr, 2 + offset, 0, lines[index][: max_x - 1])
        _safe_addstr(stdscr, max_y - 1, 0, "Up/Down scroll - q/Esc back", curses.A_DIM)
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
    title = f"{state.processor.app_version_label()} - Text Interface"
    status = ""
    while True:
        unsaved = " (unsaved changes)" if state.dirty else ""
        items = [
            "Source & Output settings",
            "Map & Overlay settings",
            "Processing & Performance settings",
            "Output & temp folders",
            "Review all settings",
            "Save settings",
            "Check environment",
            "Run render",
            "Render 3 true color styles (native / flat / Zoom Earth)",
            "Update program (GitHub main branch)",
            "Quit",
        ]
        index = _menu(stdscr, title, items, (status or "Choose an action.") + unsaved)
        status = ""
        if index in (-1, 10):  # Esc/q or Quit
            if state.dirty and _confirm(stdscr, "Save changes before quitting?"):
                status = state.save()
            return
        elif index == 0:
            _settings_screen(stdscr, state, "Source & Output", state.screens[0][1])
        elif index == 1:
            _settings_screen(stdscr, state, "Map & Overlays", state.screens[1][1])
        elif index == 2:
            _settings_screen(stdscr, state, "Processing & Performance", state.screens[2][1])
        elif index == 3:
            _folders_screen(stdscr, state)
        elif index == 4:
            _review_screen(stdscr, state)
        elif index == 5:
            status = state.save()
        elif index == 6:
            _run_external(stdscr, lambda: run_environment_check(state))
        elif index == 7:
            if state.dirty:
                # Persist first so a crash mid-render doesn't lose edits.
                state.save()
            _run_external(stdscr, lambda: run_processing(state))
        elif index == 8:
            if state.dirty:
                state.save()
            _run_external(stdscr, lambda: run_true_color_styles(state))
        elif index == 9:
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
