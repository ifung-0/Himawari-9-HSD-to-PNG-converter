"""Tests for himawari_cli.py."""

from __future__ import annotations

import argparse
import io
import unittest
from pathlib import Path
from unittest import mock

import himawari_lowram_processor as h
import himawari_cli as cli


class FakeNamespace:
    """Minimal argparse.Namespace stand-in for testing config_from_args."""

    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)


class TestParseBool(unittest.TestCase):
    def test_yes_values_return_true(self):
        for value in ("1", "true", "True", "TRUE", "yes", "Yes", "YES", "y", "Y", "on", "On", "ON"):
            with self.subTest(value=value):
                self.assertTrue(cli.parse_bool(value))

    def test_no_values_return_false(self):
        for value in ("0", "false", "False", "FALSE", "no", "No", "NO", "n", "N", "off", "Off", "OFF"):
            with self.subTest(value=value):
                self.assertFalse(cli.parse_bool(value))

    def test_invalid_value_raises(self):
        with self.assertRaises(argparse.ArgumentTypeError):
            cli.parse_bool("maybe")


class TestConfigFieldNames(unittest.TestCase):
    def test_returns_expected_names(self):
        names = cli.config_field_names()
        expected = {
            "user_url",
            "mode",
            "composite_choice",
            "hours_back",
            "interval_minutes",
            "fps",
            "auto_download",
            "gpu_acceleration",
            "use_night_fallback",
            "night_fallback_mode",
            "download_workers",
            "timelapse_format",
            "delete_timelapse_frames",
            "image_format",
            "output_template",
            "resampler",
            "allow_quality_fallback",
            "add_border_lines",
            "border_line_color",
            "border_line_width",
            "add_map_labels",
            "add_night_boundary",
            "add_crosshair",
            "zoom_earth_style",
            "map_view",
            "flat_min_lat",
            "flat_max_lat",
            "flat_min_lon",
            "flat_max_lon",
            "flat_resolution_deg",
            "ram_limit_gb",
            "dask_chunk_size",
            "dask_num_workers",
            "max_safe_png_pixels",
        }
        self.assertEqual(names, expected)


class TestSetOutputDir(unittest.TestCase):
    def test_sets_output_dir_when_path_provided(self):
        original = h.OUTPUT_DIR
        try:
            cli.set_output_dir("/tmp/test_output")
            self.assertEqual(h.OUTPUT_DIR, Path("/tmp/test_output").resolve())
        finally:
            h.OUTPUT_DIR = original

    def test_does_nothing_when_none(self):
        original = h.OUTPUT_DIR
        try:
            cli.set_output_dir(None)
            self.assertEqual(h.OUTPUT_DIR, original)
        finally:
            h.OUTPUT_DIR = original


class TestSetTempDir(unittest.TestCase):
    def test_sets_temp_dir_when_path_provided(self):
        original = h.TEMP_DIR
        try:
            cli.set_temp_dir("/tmp/test_temp")
            self.assertEqual(h.TEMP_DIR, Path("/tmp/test_temp").resolve())
        finally:
            h.TEMP_DIR = original

    def test_does_nothing_when_none(self):
        original = h.TEMP_DIR
        try:
            cli.set_temp_dir(None)
            self.assertEqual(h.TEMP_DIR, original)
        finally:
            h.TEMP_DIR = original


class TestBuildParser(unittest.TestCase):
    def test_parser_has_expected_arguments(self):
        parser = cli.build_parser()
        actions = {action.dest for action in parser._actions}
        expected = {
            "help",
            "menu",
            "run",
            "check_env",
            "version",
            "output_dir",
            "temp_dir",
            "user_url",
            "mode",
            "composite_choice",
            "hours_back",
            "interval_minutes",
            "fps",
            "download_workers",
            "dask_num_workers",
            "dask_chunk_size",
            "ram_limit_gb",
            "image_format",
            "output_template",
            "timelapse_format",
            "resampler",
            "night_fallback_mode",
            "map_view",
            "flat_min_lat",
            "flat_max_lat",
            "flat_min_lon",
            "flat_max_lon",
            "flat_resolution_deg",
            "auto_download",
            "gpu_acceleration",
            "use_night_fallback",
            "delete_timelapse_frames",
            "allow_quality_fallback",
            "add_border_lines",
            "border_line_color",
            "border_line_width",
            "add_map_labels",
            "add_night_boundary",
            "add_crosshair",
            "zoom_earth_style",
        }
        self.assertTrue(expected.issubset(actions))

    def test_parser_defaults_are_none(self):
        parser = cli.build_parser()
        args = parser.parse_args([])
        self.assertFalse(args.version)
        self.assertIsNone(args.user_url)
        self.assertIsNone(args.mode)
        self.assertIsNone(args.composite_choice)
        self.assertIsNone(args.hours_back)
        self.assertIsNone(args.interval_minutes)
        self.assertIsNone(args.fps)
        self.assertIsNone(args.auto_download)
        self.assertIsNone(args.gpu_acceleration)
        self.assertIsNone(args.use_night_fallback)
        self.assertIsNone(args.night_fallback_mode)
        self.assertIsNone(args.download_workers)
        self.assertIsNone(args.timelapse_format)
        self.assertIsNone(args.delete_timelapse_frames)
        self.assertIsNone(args.image_format)
        self.assertIsNone(args.output_template)
        self.assertIsNone(args.resampler)
        self.assertIsNone(args.allow_quality_fallback)
        self.assertIsNone(args.add_border_lines)
        self.assertIsNone(args.border_line_color)
        self.assertIsNone(args.border_line_width)
        self.assertIsNone(args.add_map_labels)
        self.assertIsNone(args.add_night_boundary)
        self.assertIsNone(args.add_crosshair)
        self.assertIsNone(args.zoom_earth_style)
        self.assertIsNone(args.map_view)
        self.assertIsNone(args.flat_min_lat)
        self.assertIsNone(args.flat_max_lat)
        self.assertIsNone(args.flat_min_lon)
        self.assertIsNone(args.flat_max_lon)
        self.assertIsNone(args.flat_resolution_deg)
        self.assertIsNone(args.ram_limit_gb)
        self.assertIsNone(args.dask_chunk_size)
        self.assertIsNone(args.dask_num_workers)

    def test_parser_parses_bool_flags(self):
        parser = cli.build_parser()
        args = parser.parse_args(["--auto-download", "yes", "--night-fallback", "no"])
        self.assertTrue(args.auto_download)
        self.assertFalse(args.use_night_fallback)

        args = parser.parse_args(["--gpu-acceleration", "yes"])
        self.assertTrue(args.gpu_acceleration)

        args = parser.parse_args([
            "--map-labels", "yes",
            "--night-boundary", "yes",
            "--crosshair", "yes",
            "--zoom-earth-style", "yes",
        ])
        self.assertTrue(args.add_map_labels)
        self.assertTrue(args.add_night_boundary)
        self.assertTrue(args.add_crosshair)
        self.assertTrue(args.zoom_earth_style)

    def test_parser_parses_chunk_size(self):
        parser = cli.build_parser()
        args = parser.parse_args(["--chunk-size", "16MiB"])
        self.assertEqual(args.dask_chunk_size, "16MiB")

    def test_parser_rejects_invalid_chunk_size(self):
        parser = cli.build_parser()
        with self.assertRaises(SystemExit):
            parser.parse_args(["--chunk-size", "256MiB"])

    def test_parser_parses_all_numeric_args(self):
        parser = cli.build_parser()
        args = parser.parse_args([
            "--hours-back", "12",
            "--interval-minutes", "10",
            "--fps", "15",
            "--download-workers", "2",
            "--dask-workers", "1",
            "--ram-limit-gb", "4.5",
            "--flat-min-lat", "-60.0",
            "--flat-max-lat", "60.0",
            "--flat-min-lon", "-180.0",
            "--flat-max-lon", "180.0",
            "--flat-resolution-deg", "0.01",
            "--border-width", "1.5",
        ])
        self.assertEqual(args.hours_back, 12)
        self.assertEqual(args.interval_minutes, 10)
        self.assertEqual(args.fps, 15)
        self.assertEqual(args.download_workers, 2)
        self.assertEqual(args.dask_num_workers, 1)
        self.assertEqual(args.ram_limit_gb, 4.5)
        self.assertEqual(args.flat_min_lat, -60.0)
        self.assertEqual(args.flat_max_lat, 60.0)
        self.assertEqual(args.flat_min_lon, -180.0)
        self.assertEqual(args.flat_max_lon, 180.0)
        self.assertEqual(args.flat_resolution_deg, 0.01)
        self.assertEqual(args.border_line_width, 1.5)


class TestConfigFromArgs(unittest.TestCase):
    def test_builds_config_with_no_args(self):
        namespace = cli.build_parser().parse_args([])
        config = cli.config_from_args(namespace)
        self.assertIsInstance(config, h.ProcessorConfig)

    @mock.patch("himawari_cli.processor.gpu_support_status")
    def test_builds_config_with_custom_args(self, mock_gpu):
        mock_gpu.return_value = h.GpuSupportStatus(True, "ready", device_name="Test GPU", device_count=1)
        namespace = cli.build_parser().parse_args([
            "--url", "http://example.com/test.dat",
            "--mode", "Timelapse",
            "--composite", "True Color Reproduction Image",
            "--hours-back", "6",
            "--interval-minutes", "30",
            "--fps", "10",
            "--auto-download", "yes",
            "--gpu-acceleration", "yes",
            "--night-fallback", "no",
            "--download-workers", "2",
            "--timelapse-format", "mp4",
            "--delete-frames", "yes",
            "--image-format", "tif",
            "--resampler", "nearest",
            "--quality-fallback", "yes",
            "--border-lines", "yes",
            "--border-color", "#ff0000",
            "--border-width", "2.0",
            "--map-labels", "yes",
            "--night-boundary", "yes",
            "--crosshair", "yes",
            "--zoom-earth-style", "yes",
            "--map-view", "flat",
            "--flat-min-lat", "-50.0",
            "--flat-max-lat", "50.0",
            "--flat-min-lon", "-100.0",
            "--flat-max-lon", "100.0",
            "--flat-resolution-deg", "0.05",
            "--ram-limit-gb", "8.0",
            "--chunk-size", "128MiB",
            "--dask-workers", "2",
        ])
        config = cli.config_from_args(namespace)
        self.assertEqual(config.user_url, "http://example.com/test.dat")
        self.assertEqual(config.mode, "Timelapse")
        self.assertEqual(config.composite_choice, "True Color Reproduction Image")
        self.assertEqual(config.hours_back, 6)
        self.assertEqual(config.interval_minutes, 30)
        self.assertEqual(config.fps, 10)
        self.assertTrue(config.auto_download)
        self.assertTrue(config.gpu_acceleration)
        self.assertFalse(config.use_night_fallback)
        self.assertEqual(config.download_workers, 2)
        self.assertEqual(config.timelapse_format, "mp4")
        self.assertTrue(config.delete_timelapse_frames)
        self.assertEqual(config.image_format, "tif")
        self.assertEqual(config.resampler, "nearest")
        self.assertTrue(config.allow_quality_fallback)
        self.assertTrue(config.add_border_lines)
        self.assertEqual(config.border_line_color, "#ff0000")
        self.assertEqual(config.border_line_width, 2.0)
        self.assertTrue(config.add_map_labels)
        self.assertTrue(config.add_night_boundary)
        self.assertTrue(config.add_crosshair)
        self.assertTrue(config.zoom_earth_style)
        self.assertEqual(config.map_view, "flat")
        self.assertEqual(config.flat_min_lat, -50.0)
        self.assertEqual(config.flat_max_lat, 50.0)
        self.assertEqual(config.flat_min_lon, -100.0)
        self.assertEqual(config.flat_max_lon, 100.0)
        self.assertEqual(config.flat_resolution_deg, 0.05)
        self.assertEqual(config.ram_limit_gb, 8.0)
        self.assertEqual(config.dask_chunk_size, "128MiB")
        self.assertEqual(config.dask_num_workers, 2)

    @mock.patch("himawari_cli.processor.validate_configuration")
    def test_raises_on_invalid_config(self, mock_validate):
        mock_validate.side_effect = ValueError("Invalid configuration")
        namespace = cli.build_parser().parse_args([])
        with self.assertRaises(argparse.ArgumentTypeError):
            cli.config_from_args(namespace)

    @mock.patch("himawari_cli.processor.gpu_support_status")
    def test_rejects_gpu_for_unsupported_product(self, mock_gpu):
        mock_gpu.return_value = h.GpuSupportStatus(True, "ready", device_name="Test GPU", device_count=1)
        namespace = cli.build_parser().parse_args([
            "--gpu-acceleration", "yes",
            "--composite", "B13 (Infrared Window)",
        ])

        with self.assertRaisesRegex(argparse.ArgumentTypeError, "GPU acceleration is currently limited"):
            cli.config_from_args(namespace)


class TestPrintConfig(unittest.TestCase):
    def test_prints_expected_lines(self):
        config = h.default_config()
        captured = io.StringIO()
        with mock.patch("sys.stdout", captured):
            cli.print_config(config)
        output = captured.getvalue()
        self.assertIn("Current settings", output)
        self.assertIn("URL:", output)
        self.assertIn("Mode:", output)
        self.assertIn("Composite/Band:", output)
        self.assertIn("Hours back:", output)
        self.assertIn("Interval minutes:", output)
        self.assertIn("FPS:", output)
        self.assertIn("Image format:", output)
        self.assertIn("Timelapse format:", output)
        self.assertIn("Resampler:", output)
        self.assertIn("Map view:", output)
        self.assertIn("Auto-download:", output)
        self.assertIn("GPU acceleration:", output)
        self.assertIn("Night fallback:", output)
        self.assertIn("Night mode:", output)
        self.assertIn("Delete frames:", output)
        self.assertIn("Quality fallback:", output)
        self.assertIn("Border lines:", output)
        self.assertIn("Border color/width:", output)
        self.assertIn("Map labels:", output)
        self.assertIn("Night boundary:", output)
        self.assertIn("Crosshair:", output)
        self.assertIn("Zoom Earth style:", output)
        self.assertIn("Download workers:", output)
        self.assertIn("Dask workers:", output)
        self.assertIn("Dask chunk size:", output)
        self.assertIn("RAM limit GiB:", output)
        self.assertIn("Output folder:", output)
        self.assertIn("Temp folder:", output)

    def test_prints_flat_bounds_when_flat_map(self):
        config = h.default_config()
        config.map_view = "flat"
        captured = io.StringIO()
        with mock.patch("sys.stdout", captured):
            cli.print_config(config)
        output = captured.getvalue()
        self.assertIn("Flat bounds:", output)


class TestPromptText(unittest.TestCase):
    @mock.patch("builtins.input", return_value="new_value")
    def test_returns_new_value(self, mock_input):
        result = cli.prompt_text("Label", "current")
        self.assertEqual(result, "new_value")

    @mock.patch("builtins.input", return_value="")
    def test_returns_current_when_empty(self, mock_input):
        result = cli.prompt_text("Label", "current")
        self.assertEqual(result, "current")

    @mock.patch("builtins.input", return_value="  trimmed  ")
    def test_strips_whitespace(self, mock_input):
        result = cli.prompt_text("Label", "current")
        self.assertEqual(result, "trimmed")


class TestPromptChoice(unittest.TestCase):
    @mock.patch("builtins.input", return_value="2")
    def test_returns_choice_by_index(self, mock_input):
        result = cli.prompt_choice("Label", "a", ("a", "b", "c"))
        self.assertEqual(result, "b")

    @mock.patch("builtins.input", return_value="")
    def test_returns_current_when_empty(self, mock_input):
        result = cli.prompt_choice("Label", "a", ("a", "b", "c"))
        self.assertEqual(result, "a")

    @mock.patch("builtins.input", return_value="abc")
    def test_returns_current_on_invalid_number(self, mock_input):
        result = cli.prompt_choice("Label", "a", ("a", "b", "c"))
        self.assertEqual(result, "a")

    @mock.patch("builtins.input", return_value="5")
    def test_returns_current_on_out_of_range(self, mock_input):
        result = cli.prompt_choice("Label", "a", ("a", "b", "c"))
        self.assertEqual(result, "a")

    @mock.patch("builtins.input", return_value="0")
    def test_returns_current_on_zero_index(self, mock_input):
        result = cli.prompt_choice("Label", "a", ("a", "b", "c"))
        self.assertEqual(result, "a")


class TestPromptBool(unittest.TestCase):
    @mock.patch("builtins.input", return_value="yes")
    def test_returns_true_for_yes(self, mock_input):
        result = cli.prompt_bool("Label", False)
        self.assertTrue(result)

    @mock.patch("builtins.input", return_value="no")
    def test_returns_false_for_no(self, mock_input):
        result = cli.prompt_bool("Label", True)
        self.assertFalse(result)

    @mock.patch("builtins.input", return_value="")
    def test_returns_current_when_empty(self, mock_input):
        result = cli.prompt_bool("Label", True)
        self.assertTrue(result)

    @mock.patch("builtins.input", return_value="invalid")
    def test_returns_current_on_invalid_input(self, mock_input):
        result = cli.prompt_bool("Label", False)
        self.assertFalse(result)


class TestPromptInt(unittest.TestCase):
    @mock.patch("builtins.input", return_value="42")
    def test_returns_new_value(self, mock_input):
        result = cli.prompt_int("Label", 10)
        self.assertEqual(result, 42)

    @mock.patch("builtins.input", return_value="")
    def test_returns_current_when_empty(self, mock_input):
        result = cli.prompt_int("Label", 10)
        self.assertEqual(result, 10)

    @mock.patch("builtins.input", return_value="abc")
    def test_returns_current_on_invalid_input(self, mock_input):
        result = cli.prompt_int("Label", 10)
        self.assertEqual(result, 10)

    @mock.patch("builtins.input", return_value="5")
    def test_enforces_minimum(self, mock_input):
        result = cli.prompt_int("Label", 10, minimum=10)
        self.assertEqual(result, 10)

    @mock.patch("builtins.input", return_value="100")
    def test_enforces_maximum(self, mock_input):
        result = cli.prompt_int("Label", 10, maximum=50)
        self.assertEqual(result, 10)

    @mock.patch("builtins.input", return_value="25")
    def test_accepts_value_within_bounds(self, mock_input):
        result = cli.prompt_int("Label", 10, minimum=10, maximum=50)
        self.assertEqual(result, 25)


class TestPromptFloat(unittest.TestCase):
    @mock.patch("builtins.input", return_value="3.14")
    def test_returns_new_value(self, mock_input):
        result = cli.prompt_float("Label", 1.0)
        self.assertEqual(result, 3.14)

    @mock.patch("builtins.input", return_value="")
    def test_returns_current_when_empty(self, mock_input):
        result = cli.prompt_float("Label", 1.0)
        self.assertEqual(result, 1.0)

    @mock.patch("builtins.input", return_value="abc")
    def test_returns_current_on_invalid_input(self, mock_input):
        result = cli.prompt_float("Label", 1.0)
        self.assertEqual(result, 1.0)

    @mock.patch("builtins.input", return_value="0.5")
    def test_enforces_minimum(self, mock_input):
        result = cli.prompt_float("Label", 2.0, minimum=1.0)
        self.assertEqual(result, 2.0)

    @mock.patch("builtins.input", return_value="2.5")
    def test_accepts_value_above_minimum(self, mock_input):
        result = cli.prompt_float("Label", 1.0, minimum=1.0)
        self.assertEqual(result, 2.5)


class TestEditBasicSettings(unittest.TestCase):
    @mock.patch("himawari_cli.prompt_text", return_value="http://test.url")
    @mock.patch("himawari_cli.prompt_choice", side_effect=[
        "Single Image",
        "B13 (Infrared Window)",
        "png",
        "hybrid",
    ])
    @mock.patch("himawari_cli.prompt_bool", side_effect=[True, True, True, True, True])
    def test_edits_basic_settings_single_image(self, mock_bool, mock_choice, mock_text):
        config = h.default_config()
        result = cli.edit_basic_settings(config)
        self.assertEqual(result.user_url, "http://test.url")
        self.assertEqual(result.mode, "Single Image")
        self.assertEqual(result.composite_choice, "B13 (Infrared Window)")
        self.assertEqual(result.image_format, "png")

    @mock.patch("himawari_cli.prompt_text", return_value="http://test.url")
    @mock.patch("himawari_cli.prompt_choice", side_effect=[
        "Timelapse",
        "True Color Reproduction Image",
        "png",
        "gif",
        "hybrid",
    ])
    @mock.patch("himawari_cli.prompt_int", side_effect=[12, 30, 15])
    @mock.patch("himawari_cli.prompt_bool", side_effect=[True, True, True, True, True])
    def test_edits_basic_settings_timelapse(self, mock_bool, mock_int, mock_choice, mock_text):
        config = h.default_config()
        result = cli.edit_basic_settings(config)
        self.assertEqual(result.mode, "Timelapse")
        self.assertEqual(result.hours_back, 12)
        self.assertEqual(result.interval_minutes, 30)
        self.assertEqual(result.fps, 15)
        self.assertEqual(result.timelapse_format, "gif")


class TestEditAdvancedSettings(unittest.TestCase):
    @mock.patch("himawari_cli.prompt_int", side_effect=[2, 1])
    @mock.patch("himawari_cli.prompt_choice", side_effect=[
        "64MiB",
        "native",
        "native",
    ])
    @mock.patch("himawari_cli.prompt_float", side_effect=[4.0, 1.0])
    @mock.patch("himawari_cli.prompt_text", side_effect=["{scan_time}_{area}_{product}", "#00ff00", "/tmp/out", "/tmp/temp"])
    @mock.patch("himawari_cli.prompt_bool", return_value=True)
    def test_edits_advanced_settings(self, mock_bool, mock_text, mock_float, mock_choice, mock_int):
        config = h.default_config()
        result = cli.edit_advanced_settings(config)
        self.assertEqual(result.download_workers, 2)
        self.assertEqual(result.dask_num_workers, 1)
        self.assertEqual(result.dask_chunk_size, "64MiB")
        self.assertEqual(result.ram_limit_gb, 4.0)
        self.assertEqual(result.resampler, "native")
        self.assertEqual(result.map_view, "native")
        self.assertEqual(result.output_template, "{scan_time}_{area}_{product}")
        self.assertTrue(result.add_border_lines)
        self.assertEqual(result.border_line_color, "#00ff00")
        self.assertEqual(result.border_line_width, 1.0)


class TestRunEnvironmentCheck(unittest.TestCase):
    @mock.patch("subprocess.call", return_value=0)
    def test_returns_zero_on_success(self, mock_call):
        result = cli.run_environment_check()
        self.assertEqual(result, 0)
        mock_call.assert_called_once()

    @mock.patch("subprocess.call", return_value=1)
    def test_returns_non_zero_on_failure(self, mock_call):
        result = cli.run_environment_check()
        self.assertEqual(result, 1)


class TestRunProcessor(unittest.TestCase):
    @mock.patch("himawari_cli.processor.run", return_value=[Path("output.png")])
    @mock.patch("himawari_cli.processor.validate_configuration")
    def test_runs_processor_and_returns_outputs(self, mock_validate, mock_run):
        config = h.default_config()
        outputs = cli.run_processor(config)
        self.assertEqual(outputs, [Path("output.png")])
        mock_validate.assert_called_once_with(config)
        mock_run.assert_called_once()

    @mock.patch("himawari_cli.processor.run", return_value=[])
    @mock.patch("himawari_cli.processor.validate_configuration")
    def test_runs_processor_with_no_outputs(self, mock_validate, mock_run):
        config = h.default_config()
        outputs = cli.run_processor(config)
        self.assertEqual(outputs, [])


class TestInteractiveMenu(unittest.TestCase):
    @mock.patch("builtins.input", side_effect=["1", "6"])
    def test_review_settings_then_exit(self, mock_input):
        config = h.default_config()
        captured = io.StringIO()
        with mock.patch("sys.stdout", captured):
            result = cli.interactive_menu(config)
        self.assertEqual(result, 0)
        output = captured.getvalue()
        self.assertIn("Current settings", output)

    @mock.patch("builtins.input", side_effect=["2", "6"])
    @mock.patch("himawari_cli.edit_basic_settings")
    def test_edit_basic_settings_then_exit(self, mock_edit, mock_input):
        config = h.default_config()
        mock_edit.return_value = config
        result = cli.interactive_menu(config)
        self.assertEqual(result, 0)
        mock_edit.assert_called_once()

    @mock.patch("builtins.input", side_effect=["3", "6"])
    @mock.patch("himawari_cli.edit_advanced_settings")
    def test_edit_advanced_settings_then_exit(self, mock_edit, mock_input):
        config = h.default_config()
        mock_edit.return_value = config
        result = cli.interactive_menu(config)
        self.assertEqual(result, 0)
        mock_edit.assert_called_once()

    @mock.patch("builtins.input", side_effect=["4", "6"])
    @mock.patch("himawari_cli.run_environment_check", return_value=0)
    def test_check_environment_then_exit(self, mock_check, mock_input):
        config = h.default_config()
        result = cli.interactive_menu(config)
        self.assertEqual(result, 0)
        mock_check.assert_called_once()

    @mock.patch("builtins.input", side_effect=["4", "6"])
    @mock.patch("himawari_cli.run_environment_check", return_value=1)
    def test_check_environment_failure_prints_message(self, mock_check, mock_input):
        config = h.default_config()
        captured = io.StringIO()
        with mock.patch("sys.stdout", captured):
            result = cli.interactive_menu(config)
        self.assertEqual(result, 0)
        output = captured.getvalue()
        self.assertIn("Environment check exited with code", output)

    @mock.patch("builtins.input", side_effect=["5", "y", "6"])
    @mock.patch("himawari_cli.run_processor", return_value=[Path("output.png")])
    def test_run_processor_confirmed_then_exit(self, mock_run, mock_input):
        config = h.default_config()
        result = cli.interactive_menu(config)
        self.assertEqual(result, 0)
        mock_run.assert_called_once()

    @mock.patch("builtins.input", side_effect=["5", "n", "6"])
    @mock.patch("himawari_cli.run_processor")
    def test_run_processor_cancelled_then_exit(self, mock_run, mock_input):
        config = h.default_config()
        result = cli.interactive_menu(config)
        self.assertEqual(result, 0)
        mock_run.assert_not_called()

    @mock.patch("builtins.input", side_effect=["6"])
    def test_exit_immediately(self, mock_input):
        config = h.default_config()
        result = cli.interactive_menu(config)
        self.assertEqual(result, 0)

    @mock.patch("builtins.input", side_effect=["invalid", "6"])
    def test_invalid_choice_then_exit(self, mock_input):
        config = h.default_config()
        captured = io.StringIO()
        with mock.patch("sys.stdout", captured):
            result = cli.interactive_menu(config)
        self.assertEqual(result, 0)
        output = captured.getvalue()
        self.assertIn("Choose a number from 1 to 6", output)


class TestMain(unittest.TestCase):
    @mock.patch("himawari_cli.run_processor")
    @mock.patch("himawari_cli.interactive_menu", return_value=0)
    def test_no_args_opens_menu(self, mock_menu, mock_run):
        result = cli.main([])
        self.assertEqual(result, 0)
        mock_menu.assert_called_once()
        mock_run.assert_not_called()

    @mock.patch("himawari_cli.run_processor")
    def test_run_flag_executes_processor(self, mock_run):
        mock_run.return_value = [Path("output.png")]
        result = cli.main(["--run"])
        self.assertEqual(result, 0)
        mock_run.assert_called_once()

    @mock.patch("himawari_cli.run_processor")
    @mock.patch("himawari_cli.interactive_menu", return_value=0)
    def test_menu_flag_opens_menu(self, mock_menu, mock_run):
        result = cli.main(["--menu"])
        self.assertEqual(result, 0)
        mock_menu.assert_called_once()
        mock_run.assert_not_called()

    @mock.patch("himawari_cli.run_environment_check", return_value=0)
    @mock.patch("himawari_cli.run_processor")
    def test_check_env_flag_runs_check(self, mock_run, mock_check):
        result = cli.main(["--check-env"])
        self.assertEqual(result, 0)
        mock_check.assert_called_once()
        mock_run.assert_not_called()

    @mock.patch("himawari_cli.run_environment_check", return_value=1)
    @mock.patch("himawari_cli.run_processor")
    @mock.patch("himawari_cli.interactive_menu", return_value=0)
    def test_check_env_failure_with_menu_returns_menu_result(self, mock_menu, mock_run, mock_check):
        result = cli.main(["--check-env", "--menu"])
        self.assertEqual(result, 0)
        mock_menu.assert_called_once()

    @mock.patch("himawari_cli.run_environment_check", return_value=1)
    @mock.patch("himawari_cli.run_processor")
    def test_check_env_failure_without_run_or_menu_returns_code(self, mock_run, mock_check):
        result = cli.main(["--check-env"])
        self.assertEqual(result, 1)
        mock_run.assert_not_called()

    @mock.patch("himawari_cli.run_processor")
    def test_with_args_but_no_run_or_menu_prints_config(self, mock_run):
        captured = io.StringIO()
        with mock.patch("sys.stdout", captured):
            result = cli.main(["--url", "http://example.com/test.dat"])
        self.assertEqual(result, 0)
        output = captured.getvalue()
        self.assertIn("Current settings", output)
        self.assertIn("No processing started", output)
        mock_run.assert_not_called()

    @mock.patch("himawari_cli.run_processor")
    def test_sets_output_and_temp_dirs(self, mock_run):
        mock_run.return_value = [Path("output.png")]
        original_output = h.OUTPUT_DIR
        original_temp = h.TEMP_DIR
        try:
            result = cli.main([
                "--run",
                "--output-dir", "/tmp/test_output",
                "--temp-dir", "/tmp/test_temp",
            ])
            self.assertEqual(result, 0)
            self.assertEqual(h.OUTPUT_DIR, Path("/tmp/test_output").resolve())
            self.assertEqual(h.TEMP_DIR, Path("/tmp/test_temp").resolve())
        finally:
            h.OUTPUT_DIR = original_output
            h.TEMP_DIR = original_temp

    @mock.patch("himawari_cli.run_processor")
    def test_version_flag(self, mock_run):
        captured = io.StringIO()
        with mock.patch("sys.stdout", captured):
            result = cli.main(["--version"])
        self.assertEqual(result, 0)
        output = captured.getvalue()
        self.assertIn(h.APP_DISPLAY_NAME, output)
        self.assertIn(f"Version: {h.APP_VERSION}", output)
        self.assertIn("CLI:     himawari_cli.py", output)
        self.assertIn("Python:", output)
        self.assertIn("Project:", output)
        mock_run.assert_not_called()


class TestYesNo(unittest.TestCase):
    def test_returns_yes_for_true(self):
        self.assertEqual(cli.yes_no(True), "yes")

    def test_returns_no_for_false(self):
        self.assertEqual(cli.yes_no(False), "no")


if __name__ == "__main__":
    unittest.main()
