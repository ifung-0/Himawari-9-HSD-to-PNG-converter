import unittest
import bz2
import tempfile
import threading
from unittest import mock

import dask.array as da
import xarray as xr
from pyresample.geometry import AreaDefinition

import himawari_lowram_processor as h


class ProcessorTests(unittest.TestCase):
    def test_parse_url(self):
        info = h.parse_url(h.USER_URL)
        self.assertEqual(info.root, "https://noaa-himawari9.s3.amazonaws.com/AHI-L1b-FLDK/")
        self.assertEqual(info.sat_id, "HS_H09")
        self.assertEqual(info.timestamp, "20240725_0400")
        self.assertEqual(info.area, "FLDK")
        self.assertEqual(info.total_segments, 10)

    def test_parse_index_folder_url(self):
        info = h.parse_url("https://noaa-himawari9.s3.amazonaws.com/index.html#AHI-L1b-FLDK/2025/07/11/0050/")
        self.assertEqual(info.root, "https://noaa-himawari9.s3.amazonaws.com/AHI-L1b-FLDK/")
        self.assertEqual(info.sat_id, "HS_H09")
        self.assertEqual(info.timestamp, "20250711_0050")
        self.assertEqual(info.area, "FLDK")
        self.assertEqual(info.total_segments, 10)

    def test_all_band_resolution_mapping(self):
        self.assertEqual(tuple(h.BAND_RESOLUTION), h.BAND_NAMES)
        self.assertEqual(tuple(h.BAND_PIXEL_SIZE_M), h.BAND_NAMES)
        self.assertEqual(h.BAND_RESOLUTION["B03"], "R05")
        self.assertEqual(h.BAND_RESOLUTION["B01"], "R10")
        self.assertEqual(h.BAND_RESOLUTION["B02"], "R10")
        self.assertEqual(h.BAND_RESOLUTION["B04"], "R10")
        self.assertEqual(h.BAND_PIXEL_SIZE_M["B03"], 500)
        self.assertEqual(h.BAND_PIXEL_SIZE_M["B01"], 1000)
        for idx in range(5, 17):
            self.assertEqual(h.BAND_RESOLUTION[f"B{idx:02d}"], "R20")
            self.assertEqual(h.BAND_PIXEL_SIZE_M[f"B{idx:02d}"], 2000)

    def test_single_band_labels_cover_all_ahi_bands(self):
        self.assertEqual(len(h.SINGLE_BAND_LABELS), 16)
        self.assertEqual(set(h.SINGLE_BAND_LABELS.values()), set(h.BAND_NAMES))
        for label, band in h.SINGLE_BAND_LABELS.items():
            self.assertEqual(h.COMPOSITE_BANDS[label], (band,))

    def test_each_band_download_name_and_calibration(self):
        info = h.parse_url(h.USER_URL)
        dt = h.datetime.strptime(info.timestamp, "%Y%m%d_%H%M")
        with tempfile.TemporaryDirectory() as tmp_dir:
            for band in h.BAND_NAMES:
                tasks = h.make_download_tasks(info, dt, (band,), h.Path(tmp_dir))
                self.assertEqual(len(tasks), info.total_segments)
                first_name = tasks[0].destination.name
                expected_part = f"_{band}_{info.area}_{h.BAND_RESOLUTION[band]}_S01{info.total_segments:02d}.DAT"
                self.assertIn(expected_part, first_name)
                self.assertTrue(tasks[0].url.endswith(f"{first_name}.bz2"))
                expected_calibration = "brightness_temperature" if band in h.IR_BANDS else "reflectance"
                self.assertEqual(h.calibration_for_band(band), expected_calibration)

    def test_target_pixel_size_uses_finest_required_band(self):
        self.assertEqual(h.target_pixel_size_m("True Color RGB (Enhanced)"), 500)
        self.assertEqual(h.target_pixel_size_m("B13 (Infrared Window)"), 2000)
        self.assertEqual(h.target_pixel_size_m("B01 (Blue Visible)"), 1000)
        self.assertEqual(h.target_pixel_size_m("Day Snow-Fog RGB"), 500)

    def test_worker_clamps(self):
        self.assertEqual(h.clamp_download_workers(14), 4)
        self.assertEqual(h.clamp_download_workers(0), 1)
        self.assertEqual(h.clamp_dask_workers(7), 2)
        self.assertEqual(h.clamp_dask_workers(0), 1)

    def test_required_bands_include_night_fallback(self):
        bands = h.required_bands("True Color RGB (Enhanced)", use_night_fallback=True)
        self.assertIn("B13", bands)
        self.assertIn("B03", bands)

        day_only_bands = h.required_bands("True Color RGB (Enhanced)", use_night_fallback=False)
        self.assertNotIn("B13", day_only_bands)

    def test_select_active_composite(self):
        self.assertEqual(
            h.select_active_composite("True Color RGB (Enhanced)", True, use_night_fallback=True),
            "B13 (Infrared Window)",
        )
        self.assertEqual(
            h.select_active_composite("True Color RGB (Enhanced)", True, use_night_fallback=False),
            "True Color RGB (Enhanced)",
        )
        self.assertEqual(
            h.select_active_composite("Night Microphysics RGB", True, use_night_fallback=True),
            "Night Microphysics RGB",
        )

    def test_output_filename(self):
        info = h.parse_url(h.USER_URL)
        dt = h.datetime.strptime(info.timestamp, "%Y%m%d_%H%M")
        original = h.IMAGE_FORMAT
        try:
            h.IMAGE_FORMAT = "png"
            single = h.output_filename(info, dt, "B13 (Infrared Window)", "Single Image", 0)
            frame = h.output_filename(info, dt, "B13 (Infrared Window)", "Timelapse", 12)
            self.assertEqual(single.suffix, ".png")
            self.assertIn("B13_Infrared_Window", single.name)
            self.assertEqual(frame.name, "frame_0012.png")
            self.assertEqual(h.writer_for_output(single), "simple_image")

            h.IMAGE_FORMAT = "tif"
            geotiff = h.output_filename(info, dt, "B13 (Infrared Window)", "Single Image", 0)
            self.assertEqual(geotiff.suffix, ".tif")
            self.assertEqual(h.writer_for_output(geotiff), "geotiff")
        finally:
            h.IMAGE_FORMAT = original

    def test_custom_composites_stay_lazy(self):
        y = [0, 1, 2, 3]
        x = [0, 1, 2, 3]
        attrs = {"area": "dummy", "sensor": "ahi"}
        b03 = xr.DataArray(
            da.ones((4, 4), chunks=(2, 2)) * 50,
            dims=("y", "x"),
            coords={"y": y, "x": x},
            attrs=attrs,
        )
        b01 = xr.DataArray(
            da.ones((4, 4), chunks=(2, 2)) * 35,
            dims=("y", "x"),
            coords={"y": y, "x": x},
            attrs=attrs,
        )
        b02 = xr.DataArray(
            da.ones((4, 4), chunks=(2, 2)) * 45,
            dims=("y", "x"),
            coords={"y": y, "x": x},
            attrs=attrs,
        )
        b04 = xr.DataArray(
            da.ones((4, 4), chunks=(2, 2)) * 55,
            dims=("y", "x"),
            coords={"y": y, "x": x},
            attrs=attrs,
        )
        b08 = xr.DataArray(
            da.ones((4, 4), chunks=(2, 2)) * 230,
            dims=("y", "x"),
            coords={"y": y, "x": x},
            attrs=attrs,
        )
        b13 = xr.DataArray(
            da.ones((4, 4), chunks=(2, 2)) * 250,
            dims=("y", "x"),
            coords={"y": y, "x": x},
            attrs=attrs,
        )
        b15 = xr.DataArray(
            da.ones((4, 4), chunks=(2, 2)) * 245,
            dims=("y", "x"),
            coords={"y": y, "x": x},
            attrs=attrs,
        )

        sandwich = h.create_sandwich_composite(b03, b13)
        heavy = h.create_heavy_rainfall_rgb(b08, b13, b15)
        snow_fog = h.create_day_snow_fog_rgb(b03, b03, b13)
        true_color_repro = h.create_true_color_reproduction_fallback(b01, b02, b03, b04)
        self.assertIsInstance(sandwich.data, da.Array)
        self.assertIsInstance(heavy.data, da.Array)
        self.assertIsInstance(snow_fog.data, da.Array)
        self.assertIsInstance(true_color_repro.data, da.Array)
        self.assertEqual(sandwich.dtype, h.np.uint8)
        self.assertEqual(heavy.dtype, h.np.uint8)
        self.assertEqual(snow_fog.dtype, h.np.uint8)
        self.assertEqual(true_color_repro.dtype, h.np.uint8)
        self.assertEqual(sandwich.attrs["mode"], "RGB")
        self.assertEqual(true_color_repro.attrs["mode"], "RGB")
        self.assertNotIn("calibration", sandwich.attrs)
        self.assertEqual(sandwich.dims[0], "bands")
        self.assertEqual(heavy.shape, (3, 4, 4))
        self.assertEqual(snow_fog.shape, (3, 4, 4))
        self.assertEqual(true_color_repro.shape, (3, 4, 4))

    def test_all_single_band_products_build_lazy_uint8_rgb(self):
        attrs = {"area": "dummy", "sensor": "ahi"}
        for band in h.BAND_NAMES:
            value = 250 if band in h.IR_BANDS else 50
            source = xr.DataArray(
                da.ones((4, 4), chunks=(2, 2)) * value,
                dims=("y", "x"),
                attrs=attrs,
            )
            product = h.single_band_to_rgb(source, band, f"custom_{band.lower()}_rgb")
            self.assertIsInstance(product.data, da.Array)
            self.assertEqual(product.dtype, h.np.uint8)
            self.assertEqual(product.shape, (3, 4, 4))
            self.assertEqual(product.attrs["mode"], "RGB")

    def test_config_validation(self):
        config = h.default_config()
        h.validate_configuration(config)

        config.download_workers = 0
        with self.assertRaises(ValueError):
            h.validate_configuration(config)

        config = h.default_config()
        config.resampler = "bilinear"
        with self.assertRaises(ValueError):
            h.validate_configuration(config)

        config = h.default_config()
        config.add_border_lines = True
        config.border_line_color = "not-a-color"
        with self.assertRaises(ValueError):
            h.validate_configuration(config)

    def test_low_ram_resampler_rejects_bilinear(self):
        config = h.default_config()
        config.resampler = "bilinear"
        with self.assertRaises(ValueError):
            h.resample_scene_low_ram(None, None, config)

    def test_native_common_area_snaps_to_b13_grid(self):
        projection = {"proj": "geos", "lon_0": 140.7, "h": 35785863, "a": 6378137, "b": 6356752.3}
        first = AreaDefinition("a", "a", "a", projection, 20, 20, (0, 0, 40000, 40000))
        shifted = AreaDefinition("b", "b", "b", projection, 20, 20, (2000, 2000, 42000, 42000))

        target = h.native_compatible_common_area([first, shifted], 500)
        factor = h.BAND_PIXEL_SIZE_M["B13"] // 500
        b13_height = target.height // factor
        b13_width = target.width // factor

        self.assertEqual(target.width % factor, 0)
        self.assertEqual(target.height % factor, 0)
        for area in (first, shifted):
            self.assertEqual(h.area_slice_shape(area, target), (b13_height, b13_width))

    def test_resampler_forwards_dataset_filter(self):
        config = h.default_config()
        scene = mock.Mock()
        target = mock.Mock()
        expected = mock.Mock()
        scene.resample.return_value = expected

        result = h.resample_scene_low_ram(scene, target, config, datasets=("B13",))

        self.assertIs(result, expected)
        scene.resample.assert_called_once_with(target, datasets=("B13",), resampler="native")

    def test_border_overlay_options(self):
        self.assertEqual(h.parse_rgb_color("green"), (0, 255, 0))
        self.assertEqual(h.parse_rgb_color("#123abc"), (18, 58, 188))
        self.assertEqual(h.parse_rgb_color("1, 2, 3"), (1, 2, 3))

        config = h.default_config()
        self.assertIsNone(h.build_overlay_options(config))
        config.add_border_lines = True
        config.border_line_color = "green"
        overlay = h.build_overlay_options(config)
        self.assertEqual(overlay["color"], (0, 255, 0))
        self.assertIn("coast_dir", overlay)

    def test_large_png_switches_to_geotiff(self):
        area = mock.Mock(width=22000, height=22000)
        config = h.default_config()
        path = h.Path("big.png")
        self.assertEqual(h.enforce_safe_output_format(path, area, config), h.Path("big.tif"))

        small_area = mock.Mock(width=100, height=100)
        self.assertEqual(h.enforce_safe_output_format(path, small_area, config), path)

    def test_save_retries_without_failed_overlay(self):
        scene = mock.Mock()
        scene.save_dataset.side_effect = [ModuleNotFoundError("No module named 'pycoast'"), None]
        output = h.Path("out.png")

        result = h.save_dataset_with_optional_overlay(
            scene,
            "dataset",
            output,
            "simple_image",
            enhance=False,
            overlay={"color": (0, 255, 0)},
            fill_value=0,
        )

        self.assertEqual(scene.save_dataset.call_count, 2)
        self.assertIn("overlay", scene.save_dataset.call_args_list[0].kwargs)
        self.assertNotIn("overlay", scene.save_dataset.call_args_list[1].kwargs)
        self.assertEqual(result, output)

    def test_png_empty_image_retries_as_geotiff(self):
        scene = mock.Mock()
        scene.save_dataset.side_effect = [ValueError("cannot write empty image"), None]

        result = h.save_dataset_with_optional_overlay(
            scene,
            "dataset",
            h.Path("out.png"),
            "simple_image",
            enhance=False,
            overlay=None,
            fill_value=0,
        )

        self.assertEqual(scene.save_dataset.call_count, 2)
        self.assertEqual(scene.save_dataset.call_args_list[1].kwargs["writer"], "geotiff")
        self.assertTrue(scene.save_dataset.call_args_list[1].kwargs["filename"].endswith(".tif"))
        self.assertEqual(result, h.Path("out.tif"))

    def test_require_module_reports_missing_dependency(self):
        with self.assertRaises(RuntimeError):
            h.require_module("definitely_missing_himawari_dependency", "testing")

    @mock.patch("himawari_lowram_processor.has_module", return_value=False)
    def test_geotiff_dependency_checked_for_large_output(self, _mock_has_module):
        config = h.default_config()
        info = h.parse_url(h.USER_URL)
        start = h.datetime.strptime(info.timestamp, "%Y%m%d_%H%M")
        area = mock.Mock(width=22000, height=22000)

        with self.assertRaises(RuntimeError):
            h.validate_runtime_dependencies(config, info, start, area)

    def test_pyspectral_quality_handling(self):
        self.assertEqual(
            h.SATPY_OPTIONAL_DEP_FALLBACKS["True Color RGB (Enhanced)"],
            "true_color_nocorr",
        )
        self.assertNotIn("True Color Reproduction Image", h.SATPY_OPTIONAL_DEP_FALLBACKS)
        self.assertIn("True Color Reproduction Image", h.QUALITY_CRITICAL_COMPOSITES)
        exc = ModuleNotFoundError("No module named 'pyspectral'")
        exc.name = "pyspectral"
        self.assertTrue(h.missing_optional_dependency(exc, "pyspectral"))
        self.assertFalse(h.missing_optional_dependency(exc, "not_pyspectral"))
        self.assertIn("pyspectral", h.missing_pyspectral_message("True Color Reproduction Image"))

    def test_missing_satpy_dataset_detection(self):
        exc = KeyError("\"No dataset matching 'DataQuery(name='true_color_reproduction')' found\"")

        self.assertTrue(h.missing_satpy_dataset(exc, "true_color_reproduction"))
        self.assertFalse(h.missing_satpy_dataset(exc, "true_color"))

    @mock.patch("himawari_lowram_processor.cleanup_paths")
    @mock.patch("himawari_lowram_processor.save_dataset_with_optional_overlay")
    @mock.patch("himawari_lowram_processor.resample_scene_low_ram")
    @mock.patch("himawari_lowram_processor.download_segments")
    @mock.patch("himawari_lowram_processor.Scene")
    def test_true_color_reproduction_missing_satpy_composite_uses_custom_fallback(
        self,
        mock_scene_class,
        mock_download,
        mock_resample,
        mock_save,
        _mock_cleanup,
    ):
        attrs = {"area": "dummy", "sensor": "ahi"}
        bands = {
            band: xr.DataArray(da.ones((4, 4), chunks=(2, 2)) * 50, dims=("y", "x"), attrs=attrs)
            for band in ("B01", "B02", "B03", "B04")
        }
        original_scene = mock.Mock()
        original_scene.load.side_effect = KeyError(
            "\"No dataset matching 'DataQuery(name='true_color_reproduction')' found\""
        )
        fallback_scene = mock.MagicMock()
        fallback_scene.__getitem__.side_effect = lambda key: bands[key]
        fallback_scene.load.return_value = None
        mock_scene_class.side_effect = [original_scene, fallback_scene]
        mock_resample.return_value = fallback_scene
        mock_save.return_value = h.Path("out.png")
        mock_download.return_value = [h.Path(f"segment-{idx}.dat") for idx in range(40)]
        events = []

        def progress(message, current, total):
            events.append((message, current, total))

        config = h.default_config()
        config.composite_choice = "True Color Reproduction Image"
        config.use_night_fallback = False
        config.allow_quality_fallback = False
        info = h.parse_url(h.USER_URL)
        dt = h.datetime.strptime(info.timestamp, "%Y%m%d_%H%M")

        result = h.process_frame(dt, info, mock.Mock(width=10, height=10), 0, 1, config=config, progress=progress)

        self.assertEqual(result, h.Path("out.png"))
        mock_save.assert_called_once()
        self.assertEqual(mock_save.call_args.args[1], h.CUSTOM_DATASET_NAMES["True Color Reproduction Image"])
        self.assertFalse(mock_save.call_args.kwargs["enhance"])
        self.assertTrue(any("custom low-RAM fallback" in message for message, _current, _total in events))

    def test_download_segments_progress_for_existing_files(self):
        events = []

        def progress(message, current, total):
            events.append((message, current, total))

        task = h.DownloadTask("https://example.test/file.bz2", h.Path(__file__))
        result = h.download_segments([task], workers=1, auto_download=False, progress=progress)

        self.assertEqual(result, [h.Path(__file__)])
        self.assertTrue(events)
        self.assertEqual(events[-1][1:], (1, 1))

    def test_streaming_bz2_handles_concatenated_streams(self):
        decompressor = bz2.BZ2Decompressor()
        payload = bz2.compress(b"one") + bz2.compress(b"two")
        decompressor, data = h.decompress_bz2_chunk(decompressor, payload)
        self.assertEqual(data, b"onetwo")
        self.assertTrue(decompressor.eof)

    def test_download_segments_honors_canceled_event(self):
        cancel_event = threading.Event()
        cancel_event.set()
        task = h.DownloadTask("https://example.test/file.bz2", h.Path("missing.dat"))

        with self.assertRaises(h.ProcessingCancelled):
            h.download_segments([task], workers=1, cancel_event=cancel_event)

    @mock.patch("himawari_lowram_processor.concurrent.futures.ThreadPoolExecutor")
    def test_download_segments_waits_for_workers_after_cancel(self, mock_executor_class):
        cancel_event = threading.Event()
        executor = mock.Mock()
        future = mock.Mock()
        executor.submit.return_value = future
        mock_executor_class.return_value = executor

        def wait_side_effect(_pending, timeout, return_when):
            cancel_event.set()
            return set(), {future}

        task = h.DownloadTask("https://example.test/file.bz2", h.Path("missing.dat"))
        with mock.patch("himawari_lowram_processor.concurrent.futures.wait", side_effect=wait_side_effect):
            with self.assertRaises(h.ProcessingCancelled):
                h.download_segments([task], workers=1, cancel_event=cancel_event)

        executor.shutdown.assert_called_once_with(wait=True, cancel_futures=True)

    def test_run_honors_precanceled_event(self):
        cancel_event = threading.Event()
        cancel_event.set()

        with self.assertRaises(h.ProcessingCancelled):
            h.run(h.default_config(), cancel_event=cancel_event)

    @mock.patch("himawari_lowram_processor.native_compatible_common_area")
    @mock.patch("himawari_lowram_processor.cleanup_paths")
    @mock.patch("himawari_lowram_processor.Scene")
    @mock.patch("himawari_lowram_processor.download_segments")
    def test_common_area_scan_forwards_download_progress(
        self,
        mock_download,
        mock_scene_class,
        mock_cleanup,
        mock_native_area,
    ):
        events = []
        config = h.default_config()
        info = h.parse_url(h.USER_URL)
        dt = h.datetime.strptime(info.timestamp, "%Y%m%d_%H%M")
        fake_path = h.Path("fake-b13.dat")
        fake_area = mock.Mock(width=10, height=10)
        fake_scene = mock.Mock()
        fake_scene.__getitem__ = mock.Mock(return_value=mock.Mock(attrs={"area": fake_area}))
        mock_scene_class.return_value = fake_scene
        mock_download.return_value = [fake_path] * info.total_segments
        mock_native_area.return_value = fake_area

        def progress(message, current, total):
            events.append((message, current, total))

        result = h.common_area_from_frames(info, [dt], 2000, config=config, progress=progress)

        self.assertIs(result, fake_area)
        self.assertIs(mock_download.call_args.kwargs["progress"], progress)
        self.assertTrue(events)
        self.assertEqual(events[0][0], "Downloading B13 scan segments for common area")
        mock_cleanup.assert_not_called()

    @mock.patch("himawari_lowram_processor.process_frame", return_value=None)
    @mock.patch("himawari_lowram_processor.validate_runtime_dependencies")
    @mock.patch("himawari_lowram_processor.common_area_from_frames")
    def test_single_image_raises_when_no_frame_processes(self, mock_common_area, _mock_validate, _mock_process):
        config = h.default_config()
        config.mode = "Single Image"
        mock_common_area.return_value = mock.Mock(width=10, height=10)

        with self.assertRaisesRegex(RuntimeError, "no output was created"):
            h.run(config)


    @mock.patch("himawari_lowram_processor.process_frame", return_value=None)
    @mock.patch("himawari_lowram_processor.validate_runtime_dependencies")
    @mock.patch("himawari_lowram_processor.common_area_from_frames")
    def test_timelapse_raises_when_no_frames_process(self, mock_common_area, _mock_validate, _mock_process):
        config = h.default_config()
        config.mode = "Timelapse"
        config.hours_back = 1
        config.interval_minutes = 60
        mock_common_area.return_value = mock.Mock(width=10, height=10)

        with self.assertRaisesRegex(RuntimeError, "no frames were processed"):
            h.run(config)


if __name__ == "__main__":
    unittest.main()
