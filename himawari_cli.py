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
        description="Command-line interface for the Himawari-9 low-RAM processor.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--menu", action="store_true", help="Open the interactive CLI menu.")
    parser.add_argument("--run", action="store_true", help="Run once with the provided options.")
    parser.add_argument("--check-env", action="store_true", help="Run check_environment.py before any processing.")
    parser.add_argument(
        "--version",
        action="version",
        version=processor.app_version_label(),
        help="Show the app version and exit.",
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
    parser.add_argument("--hours-back", type=int, default=None)
    parser.add_argument("--interval-minutes", type=int, default=None)
    parser.add_argument("--fps", type=int, default=None)
    parser.add_argument("--download-workers", type=int, default=None)
    parser.add_argument("--dask-workers", dest="dask_num_workers", type=int, default=None)
    parser.add_argument("--chunk-size", dest="dask_chunk_size", choices=("32MiB", "64MiB", "128MiB"), default=None)
    parser.add_argument("--ram-limit-gb", type=float, default=None)
    parser.add_argument("--image-format", metavar="png|tif", default=None)
    parser.add_argument(
        "--output-template",
        default=None,
        help="Output filename template with tokens like {scan_time}, {area}, and {product}.",
    )
    parser.add_argument("--timelapse-format", metavar="gif|mp4", default=None)
    parser.add_argument("--resampler", metavar="native|nearest", default=None)

    parser.add_argument("--auto-download", type=parse_bool, default=None)
    parser.add_argument("--night-fallback", dest="use_night_fallback", type=parse_bool, default=None)
    parser.add_argument("--delete-frames", dest="delete_timelapse_frames", type=parse_bool, default=None)
    parser.add_argument("--quality-fallback", dest="allow_quality_fallback", type=parse_bool, default=None)
    parser.add_argument("--border-lines", dest="add_border_lines", type=parse_bool, default=None)
    parser.add_argument("--border-color", dest="border_line_color", default=None)
    parser.add_argument("--border-width", dest="border_line_width", type=float, default=None)
    return parser


def config_from_args(args: argparse.Namespace) -> processor.ProcessorConfig:
    values: dict[str, Any] = {}
    names = config_field_names()
    for name in names:
        value = getattr(args, name, None)
        if value is not None:
            values[name] = value
    config = processor.ProcessorConfig(**values)
    try:
        processor.validate_configuration(config)
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
    print(f"Hours back:         {config.hours_back}")
    print(f"Interval minutes:   {config.interval_minutes}")
    print(f"FPS:                {config.fps}")
    print(f"Image format:       {config.image_format}")
    print(f"Timelapse format:   {config.timelapse_format}")
    print(f"Resampler:          {config.resampler}")
    print(f"Auto-download:      {yes_no(config.auto_download)}")
    print(f"Night fallback:     {yes_no(config.use_night_fallback)}")
    print(f"Delete frames:      {yes_no(config.delete_timelapse_frames)}")
    print(f"Quality fallback:   {yes_no(config.allow_quality_fallback)}")
    print(f"Border lines:       {yes_no(config.add_border_lines)}")
    print(f"Border color/width: {config.border_line_color} / {config.border_line_width}")
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
    values["image_format"] = prompt_choice("Image format", config.image_format, ("png", "tif"))
    if values["mode"] == "Timelapse":
        values["hours_back"] = prompt_int("Hours back", config.hours_back, 1, 240)
        values["interval_minutes"] = prompt_int("Interval minutes", config.interval_minutes, 1, 240)
        values["fps"] = prompt_int("FPS", config.fps, 1, 60)
        values["timelapse_format"] = prompt_choice("Timelapse format", config.timelapse_format, ("gif", "mp4"))
        values["delete_timelapse_frames"] = prompt_bool("Delete frame images after assembly", config.delete_timelapse_frames)
    values["auto_download"] = prompt_bool("Auto-download missing files", config.auto_download)
    values["use_night_fallback"] = prompt_bool("Use night fallback for day-only products", config.use_night_fallback)
    values["allow_quality_fallback"] = prompt_bool("Allow lower-quality true color fallback", config.allow_quality_fallback)
    return processor.ProcessorConfig(**values)


def edit_advanced_settings(config: processor.ProcessorConfig) -> processor.ProcessorConfig:
    values = config.__dict__.copy()
    values["download_workers"] = prompt_int("Download workers", config.download_workers, 1, 4)
    values["dask_num_workers"] = prompt_int("Dask workers", config.dask_num_workers, 1, 2)
    values["dask_chunk_size"] = prompt_choice("Dask chunk size", config.dask_chunk_size, ("32MiB", "64MiB", "128MiB"))
    values["ram_limit_gb"] = prompt_float("RAM limit GiB", config.ram_limit_gb, 1.0)
    values["resampler"] = prompt_choice("Resampler", config.resampler, ("native", "nearest"))
    values["output_template"] = prompt_text("Output filename template", config.output_template)
    values["add_border_lines"] = prompt_bool("Draw coastline/country borders", config.add_border_lines)
    values["border_line_color"] = prompt_text("Border line color", config.border_line_color)
    values["border_line_width"] = prompt_float("Border line width", config.border_line_width, 0.25)
    output_dir = prompt_text("Output folder", str(processor.OUTPUT_DIR))
    temp_dir = prompt_text("Temp folder", str(processor.TEMP_DIR))
    set_output_dir(output_dir)
    set_temp_dir(temp_dir)
    return processor.ProcessorConfig(**values)


def run_environment_check() -> int:
    command = [sys.executable, str(PROJECT_DIR / "check_environment.py")]
    return subprocess.call(command, cwd=PROJECT_DIR)


def run_processor(config: processor.ProcessorConfig) -> list[Path]:
    processor.validate_configuration(config)

    def progress(message: str, current: int | None, total: int | None) -> None:
        if current is not None and total:
            print(f"[{current}/{total}] {message}")
        else:
            print(message)

    outputs = processor.run(config, progress=progress)
    print()
    print("Finished.")
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
        print("6. Exit")
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
            return 0
        else:
            print("Choose a number from 1 to 6.")


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    set_output_dir(args.output_dir)
    set_temp_dir(args.temp_dir)
    config = config_from_args(args)

    if args.check_env:
        code = run_environment_check()
        if code != 0 and not args.run and not args.menu:
            return code

    if args.run:
        run_processor(config)
        return 0

    provided_args = sys.argv[1:] if argv is None else argv
    if args.menu or not provided_args:
        return interactive_menu(config)

    print_config(config)
    print()
    print("No processing started. Use --run to run once, or --menu for the guided CLI.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
