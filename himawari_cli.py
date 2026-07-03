from __future__ import annotations

import argparse
import subprocess
import sys
from dataclasses import fields
from pathlib import Path
from typing import Any

import himawari_lowram_processor as processor


PROJECT_DIR = Path(__file__).resolve().parent


def yes_no(value: bool) -> str:
    return "yes" if value else "no"


def parse_bool(value: str) -> bool:
    normalized = value.strip().lower()
    if normalized in {"1", "true", "t", "yes", "y", "on"}:
        return True
    if normalized in {"0", "false", "f", "no", "n", "off"}:
        return False
    raise argparse.ArgumentTypeError(f"Expected yes/no, true/false, or 1/0; got {value!r}")


def config_field_names() -> set[str]:
    return {field.name for field in fields(processor.ProcessorConfig)}


def set_output_dir(path: str | None) -> None:
    if path:
        processor.OUTPUT_DIR = Path(path).expanduser().resolve()


def set_temp_dir(path: str | None) -> None:
    if path:
        processor.TEMP_DIR = Path(path).expanduser().resolve()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Command-line interface for the Himawari-8/9 low-RAM processor.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--menu", action="store_true", help="Open the interactive CLI menu.")
    parser.add_argument("--run", action="store_true", help="Run once with the provided options.")
    parser.add_argument(
        "--true-color-set",
        action="store_true",
        help=(
            "Render the True Color Reproduction product across all nine cells "
            "(standard/live/HD satellite layers x native/flat/Zoom Earth map "
            "styles), then exit."
        ),
    )
    parser.add_argument("--check-env", action="store_true", help="Run check_environment.py before any processing.")
    parser.add_argument(
        "--version",
        action="store_true",
        help="Show detailed app and CLI version information, then exit.",
    )
    parser.add_argument(
        "--update",
        action="store_true",
        help=(
            "Download the latest code from the project's main branch on GitHub, "
            "back up and replace the local program files, then exit."
        ),
    )
    parser.add_argument("--output-dir", help="Folder for final outputs.")
    parser.add_argument("--temp-dir", help="Folder for downloaded DAT files and temporary frame data.")

    parser.add_argument("--url", dest="user_url", default=None, help="NOAA AWS Himawari AHI URL.")
    parser.add_argument("--mode", metavar="MODE", default=None, help='Output mode: "Single Image" or "Timelapse".')
    parser.add_argument(
        "--composite",
        dest="composite_choice",
        metavar="NAME",
        default=None,
        help='Composite or band name, for example "True Color Reproduction Image" or "B13 (Infrared Window)".',
    )
    parser.add_argument(
        "--satellite-layer",
        dest="satellite_layer_mode",
        choices=processor.SATELLITE_LAYER_MODES,
        default=None,
        help="Satellite layer mode: standard keeps the configured URL, live resolves the latest NOAA scan, hd applies local Zoom Earth-style flat-map rendering.",
    )
    parser.add_argument("--hours-back", type=int, default=None)
    parser.add_argument("--interval-minutes", type=int, default=None)
    parser.add_argument("--fps", type=int, default=None)
    parser.add_argument("--download-workers", type=int, default=None)
    parser.add_argument("--dask-workers", dest="dask_num_workers", type=int, default=None)
    parser.add_argument("--chunk-size", dest="dask_chunk_size", choices=processor.DASK_CHUNK_CHOICES, default=None)
    parser.add_argument("--ram-limit-gb", type=float, default=None)
    parser.add_argument("--image-format", metavar="png|tif", default=None)
    parser.add_argument(
        "--output-template",
        default=None,
        help="Output filename template with tokens like {scan_time}, {area}, and {product}.",
    )
    parser.add_argument("--timelapse-format", metavar="gif|mp4", default=None)
    parser.add_argument("--resampler", metavar="native|nearest", default=None)
    parser.add_argument("--night-fallback-mode", metavar="hybrid|whole_frame_ir", default=None)
    parser.add_argument("--map-view", metavar="native|flat", default=None)
    parser.add_argument("--flat-min-lat", type=float, default=None)
    parser.add_argument("--flat-max-lat", type=float, default=None)
    parser.add_argument("--flat-min-lon", type=float, default=None)
    parser.add_argument("--flat-max-lon", type=float, default=None)
    parser.add_argument("--flat-resolution-deg", type=float, default=None)

    parser.add_argument("--auto-download", type=parse_bool, default=None)
    parser.add_argument("--gpu-acceleration", type=parse_bool, default=None)
    parser.add_argument("--night-fallback", dest="use_night_fallback", type=parse_bool, default=None)
    parser.add_argument("--delete-frames", dest="delete_timelapse_frames", type=parse_bool, default=None)
    parser.add_argument("--quality-fallback", dest="allow_quality_fallback", type=parse_bool, default=None)
    parser.add_argument("--border-lines", dest="add_border_lines", type=parse_bool, default=None)
    parser.add_argument("--border-color", dest="border_line_color", default=None)
    parser.add_argument("--border-width", dest="border_line_width", type=float, default=None)
    parser.add_argument("--map-labels", dest="add_map_labels", type=parse_bool, default=None)
    parser.add_argument("--map-label-size", type=int, default=None)
    parser.add_argument("--night-boundary", dest="add_night_boundary", type=parse_bool, default=None)
    parser.add_argument("--crosshair", dest="add_crosshair", type=parse_bool, default=None)
    parser.add_argument("--crosshair-type", choices=processor.CROSSHAIR_TYPES, default=None)
    parser.add_argument("--crosshair-color", default=None)
    parser.add_argument("--zoom-earth-style", dest="zoom_earth_style", type=parse_bool, default=None)
    parser.add_argument("--typhoon-tracks", dest="add_typhoon_tracks", type=parse_bool, default=None,
                        help="Draw typhoon/tropical-cyclone tracks when track data is available.")
    parser.add_argument("--typhoon-color", dest="typhoon_track_color", default=None,
                        help="Typhoon track/label color (name, #RRGGBB, or R,G,B).")
    parser.add_argument("--typhoon-source", dest="typhoon_data_source", default=None,
                        help="Typhoon data source: 'auto', a JSON file/folder path, or an http(s) URL.")
    return parser


def format_version_report() -> str:
    # The CLI is a thin front-end over the processor engine, so the single
    # source of truth for the version is processor.APP_VERSION. Reporting it
    # here (rather than a separate, drift-prone CLI constant) keeps the CLI and
    # GUI versions identical.
    return "\n".join(
        [
            processor.APP_DISPLAY_NAME,
            "-" * len(processor.APP_DISPLAY_NAME),
            f"App version: {processor.APP_VERSION}",
            "CLI:         himawari_cli.py (matches the app version above)",
            f"Engine:      himawari_lowram_processor.py",
            f"Python:      {sys.executable}",
            f"Project:     {PROJECT_DIR}",
        ]
    )


def run_self_update() -> int:
    """Update the local program files from the project's main branch on GitHub."""
    branch = getattr(processor, "GITHUB_QUICK_FIX_BRANCH", "main")
    print(f"Updating from the '{branch}' branch of github.com/{processor.GITHUB_REPO} ...")
    try:
        summary = processor.perform_self_update(PROJECT_DIR, log=print, branch=branch)
    except Exception as exc:  # noqa: BLE001 - surface any failure to the user
        print(f"Update failed: {exc}")
        return 1
    info = summary if isinstance(summary, dict) else {}
    updated = ", ".join(info.get("updated", []) or []) or "(none)"
    backup_dir = info.get("backup_dir", "")
    print()
    print("Update complete.")
    print(f"  Updated files: {updated}")
    if backup_dir:
        print(f"  Backup saved to: {backup_dir}")
    print("  Please re-run the program to use the new version.")
    return 0


def config_from_args(args: argparse.Namespace) -> processor.ProcessorConfig:
    values: dict[str, Any] = {}
    names = config_field_names()
    for name in names:
        value = getattr(args, name, None)
        if value is not None:
            values[name] = value
    config = processor.layer_defaults_config(processor.ProcessorConfig(**values))
    try:
        processor.validate_configuration(config)
        setup_errors = processor.setup_configuration_errors(config)
        if setup_errors:
            raise ValueError(setup_errors[0])
    except Exception as exc:
        raise argparse.ArgumentTypeError(str(exc)) from exc
    return config


def print_config(config: processor.ProcessorConfig) -> None:
    print()
    print("Current settings")
    print("----------------")
    print(f"URL:                {config.user_url}")
    print(f"Mode:               {config.mode}")
    print(f"Composite/Band:     {config.composite_choice}")
    print(f"Satellite layer:    {config.satellite_layer_mode}")
    print(f"Hours back:         {config.hours_back}")
    print(f"Interval minutes:   {config.interval_minutes}")
    print(f"FPS:                {config.fps}")
    print(f"Image format:       {config.image_format}")
    print(f"Timelapse format:   {config.timelapse_format}")
    print(f"Resampler:          {config.resampler}")
    print(f"Map view:           {config.map_view}")
    if processor.is_flat_map(config):
        print(
            "Flat bounds:        "
            f"Web Mercator, lat {config.flat_min_lat:g}..{config.flat_max_lat:g}, "
            f"lon {config.flat_min_lon:g}..{config.flat_max_lon:g}, "
            f"{config.flat_resolution_deg:g} deg/px at equator"
        )
    print(f"Auto-download:      {yes_no(config.auto_download)}")
    print(f"GPU acceleration:   {yes_no(config.gpu_acceleration)}")
    print(f"Night fallback:     {yes_no(config.use_night_fallback)}")
    print(f"Night mode:         {config.night_fallback_mode}")
    print(f"Delete frames:      {yes_no(config.delete_timelapse_frames)}")
    print(f"Quality fallback:   {yes_no(config.allow_quality_fallback)}")
    print(f"Border lines:       {yes_no(config.add_border_lines)}")
    print(f"Border color/width: {config.border_line_color} / {config.border_line_width}")
    print(f"Map labels:         {yes_no(config.add_map_labels)}")
    print(f"Map label size:     {config.map_label_size}")
    print(f"Night boundary:     {yes_no(config.add_night_boundary)}")
    print(f"Crosshair:          {yes_no(config.add_crosshair)}")
    print(f"Crosshair style:    {config.crosshair_type} / {config.crosshair_color}")
    print(f"Zoom Earth style:   {yes_no(config.zoom_earth_style)}")
    print(f"Typhoon tracks:     {yes_no(config.add_typhoon_tracks)}")
    print(f"Typhoon color/src:  {config.typhoon_track_color} / {config.typhoon_data_source}")
    print(f"Download workers:   {config.download_workers}")
    print(f"Dask workers:       {config.dask_num_workers}")
    print(f"Dask chunk size:    {config.dask_chunk_size}")
    print(f"RAM limit GiB:      {config.ram_limit_gb}")
    print(f"Output folder:      {processor.OUTPUT_DIR}")
    print(f"Temp folder:        {processor.TEMP_DIR}")


def prompt_text(label: str, current: str) -> str:
    value = input(f"{label} [{current}]: ").strip()
    return value or current


def prompt_choice(label: str, current: str, choices: tuple[str, ...] | list[str]) -> str:
    print(f"{label}:")
    for idx, choice in enumerate(choices, start=1):
        marker = " *" if choice == current else ""
        print(f"  {idx}. {choice}{marker}")
    raw = input(f"Choose 1-{len(choices)} or press Enter to keep [{current}]: ").strip()
    if not raw:
        return current
    try:
        index = int(raw)
    except ValueError:
        print("Invalid number; keeping current value.")
        return current
    if 1 <= index <= len(choices):
        return choices[index - 1]
    print("Choice out of range; keeping current value.")
    return current


def prompt_bool(label: str, current: bool) -> bool:
    raw = input(f"{label} [{yes_no(current)}]: ").strip()
    if not raw:
        return current
    try:
        return parse_bool(raw)
    except argparse.ArgumentTypeError as exc:
        print(f"{exc}; keeping current value.")
        return current


def prompt_int(label: str, current: int, minimum: int | None = None, maximum: int | None = None) -> int:
    raw = input(f"{label} [{current}]: ").strip()
    if not raw:
        return current
    try:
        value = int(raw)
    except ValueError:
        print("Invalid integer; keeping current value.")
        return current
    if minimum is not None and value < minimum:
        print(f"Value is below {minimum}; keeping current value.")
        return current
    if maximum is not None and value > maximum:
        print(f"Value is above {maximum}; keeping current value.")
        return current
    return value


def prompt_float(label: str, current: float, minimum: float | None = None) -> float:
    raw = input(f"{label} [{current}]: ").strip()
    if not raw:
        return current
    try:
        value = float(raw)
    except ValueError:
        print("Invalid number; keeping current value.")
        return current
    if minimum is not None and value < minimum:
        print(f"Value is below {minimum}; keeping current value.")
        return current
    return value


def edit_basic_settings(config: processor.ProcessorConfig) -> processor.ProcessorConfig:
    values = config.__dict__.copy()
    values["user_url"] = prompt_text("Himawari URL", config.user_url)
    values["mode"] = prompt_choice("Output mode", config.mode, ("Single Image", "Timelapse"))
    values["composite_choice"] = prompt_choice("Composite / band", config.composite_choice, sorted(processor.COMPOSITE_BANDS))
    values["satellite_layer_mode"] = prompt_choice("Satellite layer", config.satellite_layer_mode, processor.SATELLITE_LAYER_MODES)
    values["image_format"] = prompt_choice("Image format", config.image_format, ("png", "tif"))
    if values["mode"] == "Timelapse":
        values["hours_back"] = prompt_int("Hours back", config.hours_back, 1, 240)
        values["interval_minutes"] = prompt_int("Interval minutes", config.interval_minutes, 1, 240)
        values["fps"] = prompt_int("FPS", config.fps, 1, 60)
        values["timelapse_format"] = prompt_choice("Timelapse format", config.timelapse_format, ("gif", "mp4"))
        values["delete_timelapse_frames"] = prompt_bool("Delete frame images after assembly", config.delete_timelapse_frames)
    values["auto_download"] = prompt_bool("Auto-download missing files", config.auto_download)
    values["gpu_acceleration"] = prompt_bool("Use GPU acceleration (experimental)", config.gpu_acceleration)
    values["use_night_fallback"] = prompt_bool("Use night fallback for day-only products", config.use_night_fallback)
    values["night_fallback_mode"] = prompt_choice(
        "Night fallback mode",
        config.night_fallback_mode,
        ("hybrid", "whole_frame_ir"),
    )
    values["allow_quality_fallback"] = prompt_bool("Allow lower-quality true color fallback", config.allow_quality_fallback)
    return processor.ProcessorConfig(**values)


def edit_advanced_settings(config: processor.ProcessorConfig) -> processor.ProcessorConfig:
    values = config.__dict__.copy()
    values["download_workers"] = prompt_int("Download workers", config.download_workers, 1, 4)
    values["dask_num_workers"] = prompt_int("Dask workers", config.dask_num_workers, 1, 2)
    values["dask_chunk_size"] = prompt_choice("Dask chunk size", config.dask_chunk_size, processor.DASK_CHUNK_CHOICES)
    values["ram_limit_gb"] = prompt_float("RAM limit GiB", config.ram_limit_gb, 1.0)
    values["resampler"] = prompt_choice("Resampler", config.resampler, ("native", "nearest"))
    values["map_view"] = prompt_choice("Map view", config.map_view, ("native", "flat"))
    if processor.normalized_map_view(values["map_view"]) == "flat":
        values["flat_min_lat"] = prompt_float("Flat min latitude", config.flat_min_lat, -90.0)
        values["flat_max_lat"] = prompt_float("Flat max latitude", config.flat_max_lat, -90.0)
        values["flat_min_lon"] = prompt_float("Flat min longitude", config.flat_min_lon, -360.0)
        values["flat_max_lon"] = prompt_float("Flat max longitude", config.flat_max_lon, -360.0)
        values["flat_resolution_deg"] = prompt_float("Flat resolution degrees", config.flat_resolution_deg, 0.001)
    values["output_template"] = prompt_text("Output filename template", config.output_template)
    values["add_border_lines"] = prompt_bool("Draw coastline/country borders", config.add_border_lines)
    values["border_line_color"] = prompt_text("Border line color", config.border_line_color)
    values["border_line_width"] = prompt_float("Border line width", config.border_line_width, 0.25)
    values["add_map_labels"] = prompt_bool("Flat map labels", config.add_map_labels)
    values["map_label_size"] = prompt_int(
        "Flat map label size",
        config.map_label_size,
        processor.MAP_LABEL_SIZE_MIN,
        processor.MAP_LABEL_SIZE_MAX,
    )
    values["add_night_boundary"] = prompt_bool("Flat map night boundary", config.add_night_boundary)
    values["add_crosshair"] = prompt_bool("Flat map crosshair", config.add_crosshair)
    values["crosshair_type"] = prompt_choice("Crosshair type", config.crosshair_type, processor.CROSSHAIR_TYPES)
    values["crosshair_color"] = prompt_text("Crosshair color", config.crosshair_color)
    values["zoom_earth_style"] = prompt_bool("Zoom Earth-style flat map", config.zoom_earth_style)
    values["add_typhoon_tracks"] = prompt_bool("Draw typhoon tracks (when data available)", config.add_typhoon_tracks)
    if values["add_typhoon_tracks"]:
        values["typhoon_track_color"] = prompt_text("Typhoon track color", config.typhoon_track_color)
        values["typhoon_data_source"] = prompt_text(
            "Typhoon data source (auto / file / URL)", config.typhoon_data_source
        )
    output_dir = prompt_text("Output folder", str(processor.OUTPUT_DIR))
    temp_dir = prompt_text("Temp folder", str(processor.TEMP_DIR))
    # Build the config first so a validation error while constructing it never
    # leaves the global OUTPUT_DIR/TEMP_DIR half-updated (the previous order
    # mutated the globals before this could fail, so a failed edit changed the
    # output folders as a side effect).
    updated = processor.ProcessorConfig(**values)
    set_output_dir(output_dir)
    set_temp_dir(temp_dir)
    return updated


def run_environment_check() -> int:
    command = [sys.executable, str(PROJECT_DIR / "check_environment.py")]
    return subprocess.call(command, cwd=PROJECT_DIR)


def run_processor(config: processor.ProcessorConfig) -> list[Path]:
    processor.validate_configuration(config)
    eta_estimator = processor.ProgressEtaEstimator()

    def progress(message: str, current: int | None, total: int | None) -> None:
        eta_text = eta_estimator.update(message, current, total)
        if current is not None and total:
            suffix = f" ({eta_text})" if eta_text else ""
            print(f"[{current}/{total}] {message}{suffix}")
        else:
            print(message)

    outputs = processor.run(config, progress=progress)
    print()
    print("Finished.")
    for output in outputs:
        print(f"  {output}")
    return outputs


def run_true_color_styles(config: processor.ProcessorConfig) -> list[Path]:
    """Render True Color Reproduction across all nine layer/style cells
    (standard/live/HD satellite layers x native/flat/Zoom Earth map styles)."""
    eta_estimator = processor.ProgressEtaEstimator()

    def progress(message: str, current: int | None, total: int | None) -> None:
        eta_text = eta_estimator.update(message, current, total)
        if current is not None and total:
            suffix = f" ({eta_text})" if eta_text else ""
            print(f"[{current}/{total}] {message}{suffix}")
        else:
            print(message)

    count = len(processor.TRUE_COLOR_STYLE_SET)
    print(
        f"Rendering True Color Reproduction in {count} cells: the standard, live "
        "and HD satellite layers, each as native / flat / Zoom Earth-style.\n"
    )
    outputs = processor.run_true_color_style_set(config, progress=progress)
    print()
    print("Finished. Outputs:")
    for output in outputs:
        print(f"  {output}")
    return outputs


def interactive_menu(config: processor.ProcessorConfig) -> int:
    while True:
        print()
        print(f"{processor.app_version_label()} CLI")
        print("--------------------------------")
        print("1. Review current settings")
        print("2. Edit basic settings")
        print("3. Edit advanced settings")
        print("4. Check environment")
        print("5. Run processor")
        print("6. Render 9 true color styles (standard/live/HD x native/flat/Zoom Earth)")
        print("7. Update program (from GitHub main branch)")
        print("8. Exit")
        choice = input("Choose an option: ").strip()
        if choice == "1":
            print_config(config)
        elif choice == "2":
            config = edit_basic_settings(config)
        elif choice == "3":
            config = edit_advanced_settings(config)
        elif choice == "4":
            code = run_environment_check()
            if code != 0:
                print(f"Environment check exited with code {code}.")
        elif choice == "5":
            print_config(config)
            confirm = input("Run with these settings? [y/N]: ").strip().lower()
            if confirm in {"y", "yes"}:
                run_processor(config)
        elif choice == "6":
            run_true_color_styles(config)
        elif choice == "7":
            run_self_update()
        elif choice == "8":
            return 0
        else:
            print("Choose a number from 1 to 8.")


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.version:
        print(format_version_report())
        return 0

    if args.update:
        return run_self_update()

    set_output_dir(args.output_dir)
    set_temp_dir(args.temp_dir)
    config = config_from_args(args)

    if args.check_env:
        code = run_environment_check()
        if code != 0 and not args.run and not args.menu:
            return code

    if args.true_color_set:
        run_true_color_styles(config)
        return 0

    if args.run:
        run_processor(config)
        return 0

    provided_args = sys.argv[1:] if argv is None else argv
    if args.menu or not provided_args:
        return interactive_menu(config)

    print_config(config)
    print()
    print(
        "No processing started. Use --run to run once, --true-color-set for the "
        "9-cell (layer x style) true-color batch, or --menu for the guided CLI."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
