import argparse
import io
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import himawari_cli as cli
import himawari_lowram_processor as processor


class CliTests(unittest.TestCase):
    def test_parse_bool_accepts_common_values(self):
        self.assertTrue(cli.parse_bool("yes"))
        self.assertTrue(cli.parse_bool("ON"))
        self.assertFalse(cli.parse_bool("no"))
        self.assertFalse(cli.parse_bool("0"))

        with self.assertRaises(argparse.ArgumentTypeError):
            cli.parse_bool("maybe")

    def test_config_from_args_maps_cli_values(self):
        parser = cli.build_parser()
        args = parser.parse_args(
            [
                "--run",
                "--url",
                processor.USER_URL,
                "--mode",
                "Timelapse",
                "--composite",
                "B13 (Infrared Window)",
                "--hours-back",
                "6",
                "--interval-minutes",
                "30",
                "--fps",
                "8",
                "--night-fallback",
                "no",
                "--delete-frames",
                "false",
                "--download-workers",
                "2",
                "--dask-workers",
                "1",
                "--image-format",
                "tif",
                "--output-template",
                "{scan_time}_{product}",
            ]
        )

        config = cli.config_from_args(args)

        self.assertEqual(config.user_url, processor.USER_URL)
        self.assertEqual(config.mode, "Timelapse")
        self.assertEqual(config.composite_choice, "B13 (Infrared Window)")
        self.assertEqual(config.hours_back, 6)
        self.assertEqual(config.interval_minutes, 30)
        self.assertEqual(config.fps, 8)
        self.assertFalse(config.use_night_fallback)
        self.assertFalse(config.delete_timelapse_frames)
        self.assertEqual(config.download_workers, 2)
        self.assertEqual(config.dask_num_workers, 1)
        self.assertEqual(config.image_format, "tif")
        self.assertEqual(config.output_template, "{scan_time}_{product}")

    def test_set_output_and_temp_dirs_update_processor_paths(self):
        original_output = processor.OUTPUT_DIR
        original_temp = processor.TEMP_DIR
        try:
            with tempfile.TemporaryDirectory() as tmp_dir:
                output_dir = Path(tmp_dir) / "out"
                temp_dir = Path(tmp_dir) / "tmp"

                cli.set_output_dir(str(output_dir))
                cli.set_temp_dir(str(temp_dir))

                self.assertEqual(processor.OUTPUT_DIR, output_dir.resolve())
                self.assertEqual(processor.TEMP_DIR, temp_dir.resolve())
        finally:
            processor.OUTPUT_DIR = original_output
            processor.TEMP_DIR = original_temp

    @mock.patch("himawari_cli.processor.run", return_value=[Path("out.png")])
    @mock.patch("himawari_cli.processor.validate_configuration")
    def test_run_processor_uses_shared_processor_api(self, mock_validate, mock_run):
        config = processor.default_config()

        result = cli.run_processor(config)

        self.assertEqual(result, [Path("out.png")])
        mock_validate.assert_called_once_with(config)
        mock_run.assert_called_once()
        self.assertIs(mock_run.call_args.kwargs["progress"].__class__, type(lambda: None))

    @mock.patch("himawari_cli.run_processor")
    def test_main_run_executes_without_menu(self, mock_run_processor):
        mock_run_processor.return_value = [Path("out.png")]

        result = cli.main(["--run", "--composite", "B13 (Infrared Window)"])

        self.assertEqual(result, 0)
        mock_run_processor.assert_called_once()

    def test_main_version_prints_app_version(self):
        with mock.patch("sys.stdout", new_callable=io.StringIO) as stdout:
            with self.assertRaises(SystemExit) as raised:
                cli.main(["--version"])

        self.assertEqual(raised.exception.code, 0)
        self.assertIn(processor.APP_VERSION, stdout.getvalue())
        self.assertIn(processor.APP_DISPLAY_NAME, stdout.getvalue())


if __name__ == "__main__":
    unittest.main()
