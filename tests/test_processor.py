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

    def test_area_reference_band_uses_finest_product_band(self):
        self.assertEqual(h.area_reference_band("True Color Reproduction Image"), "B03")
        self.assertEqual(h.area_reference_band("True Color RGB (Enhanced)"), "B03")
        self.assertEqual(h.area_reference_band("Natural Color RGB"), "B03")
        self.assertEqual(h.area_reference_band("B13 (Infrared Window)"), "B13")
        self.assertEqual(h.area_reference_band("Day Microphysics RGB"), "B04")

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

    def test_coarse_sample_area_caps_large_full_disk(self):
        projection = {"proj": "geos", "lon_0": 140.7, "h": 35785863, "a": 6378137, "b": 6356752.31414}
        area = AreaDefinition(
            "full",
            "Full Disk",
            "full",
            projection,
            22000,
            22000,
            (-5_500_000, -5_500_000, 5_500_000, 5_500_000),
        )

        sample = h.coarse_sample_area(area)

        self.assertLessEqual(sample.width, h.NIGHT_CHECK_SAMPLE_PIXELS)
        self.assertLessEqual(sample.height, h.NIGHT_CHECK_SAMPLE_PIXELS)
        self.assertEqual(sample.area_extent, area.area_extent)

    def test_coarse_sample_area_keeps_small_area(self):
        area = mock.Mock(width=128, height=96)

        self.assertIs(h.coarse_sample_area(area), area)

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
        self.assertNotIn("calibration", true_color_repro.attrs)
        self.assertEqual(sandwich.dims[0], "bands")
        self.assertEqual(heavy.shape, (3, 4, 4))
        self.assertEqual(snow_fog.shape, (3, 4, 4))
        self.assertEqual(true_color_repro.shape, (3, 4, 4))

    def test_true_color_reproduction_fallback_balanced_enhancement(self):
        attrs = {"area": "dummy", "sensor": "ahi", "calibration": "reflectance"}
        b01 = xr.DataArray(da.from_array([[18.0, 22.0], [26.0, 30.0]], chunks=(1, 2)), dims=("y", "x"), attrs=attrs)
        b02 = xr.DataArray(da.from_array([[30.0, 38.0], [46.0, 54.0]], chunks=(1, 2)), dims=("y", "x"), attrs=attrs)
        b03 = xr.DataArray(da.from_array([[42.0, 50.0], [58.0, 66.0]], chunks=(1, 2)), dims=("y", "x"), attrs=attrs)
        b04 = xr.DataArray(da.from_array([[36.0, 44.0], [52.0, 60.0]], chunks=(1, 2)), dims=("y", "x"), attrs=attrs)

        enhanced = h.create_true_color_reproduction_fallback(b01, b02, b03, b04)
        flat_red = h.scale_reflectance(b03, max_value=100.0, gamma=1.25)
        flat_green = h.xr_clip(
            h.scale_reflectance(b02, max_value=100.0, gamma=1.18) * 0.58
            + h.scale_reflectance(b03, max_value=100.0, gamma=1.2) * 0.30
            + h.scale_reflectance(b04, max_value=100.0, gamma=1.15) * 0.12,
            0.0,
            1.0,
        )
        flat_blue = h.scale_reflectance(b01, max_value=100.0, gamma=1.2)
        old_flat = h.rgb_dataarray(flat_red, flat_green, flat_blue, name="old", standard_name="old")

        enhanced_values = enhanced.compute()
        old_values = old_flat.compute()

        self.assertIsInstance(enhanced.data, da.Array)
        self.assertEqual(enhanced.dtype, h.np.uint8)
        self.assertEqual(enhanced.shape, (3, 2, 2))
        self.assertNotIn("calibration", enhanced.attrs)
        self.assertGreaterEqual(int(enhanced_values.min()), 0)
        self.assertLessEqual(int(enhanced_values.max()), 255)
        self.assertFalse(bool((enhanced_values.sel(bands="R") == enhanced_values.sel(bands="G")).all()))
        self.assertFalse(bool((enhanced_values.sel(bands="G") == enhanced_values.sel(bands="B")).all()))
        self.assertFalse(bool((enhanced_values == old_values).all()))

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

    def test_config_validation_rejects_bad_user_inputs(self):
        bad_values = [
            ("user_url", "   ", "Himawari URL"),
            ("mode", "Movie", "MODE"),
            ("composite_choice", "Not a product", "Unsupported"),
            ("interval_minutes", 0, "INTERVAL_MINUTES"),
            ("hours_back", 0, "HOURS_BACK"),
            ("fps", 0, "FPS"),
            ("dask_num_workers", 0, "Dask workers"),
            ("ram_limit_gb", 0, "RAM limit"),
            ("image_format", "jpg", "IMAGE_FORMAT"),
            ("timelapse_format", "avi", "TIMELAPSE_FORMAT"),
            ("resampler", "bilinear", "RESAMPLER"),
        ]
        for field, value, message in bad_values:
            with self.subTest(field=field):
                config = h.default_config()
                setattr(config, field, value)
                with self.assertRaisesRegex(ValueError, message):
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

    def test_native_compatibility_detects_fldk_b13_derived_500m_mismatch(self):
        projection = {"proj": "geos", "lon_0": 140.7, "h": 35785863, "a": 6378137, "b": 6356752.3}
        b01 = AreaDefinition(
            "b01",
            "b01",
            "b01",
            projection,
            11000,
            11000,
            (-5500000.035542117, -5500000.035542117, 5500000.035542117, 5500000.035542117),
        )
        b03 = AreaDefinition(
            "b03",
            "b03",
            "b03",
            projection,
            22000,
            22000,
            (-5499999.968358421, -5499999.968358421, 5499999.968358421, 5499999.968358421),
        )
        b13 = AreaDefinition(
            "b13",
            "b13",
            "b13",
            projection,
            5500,
            5500,
            (-5499999.901174725, -5499999.901174725, 5499999.901174725, 5499999.901174725),
        )
        band_areas = {"B01": b01, "B02": b01, "B03": b03, "B04": b01, "B13": b13}

        b13_target = h.native_compatible_common_area([b13], 500)
        b03_target = h.native_compatible_common_area([b03], 500, source_pixel_size_m=500)

        self.assertIn("B03", h.native_area_compatibility_error(band_areas, b13_target))
        self.assertIsNone(h.native_area_compatibility_error(band_areas, b03_target))

    def test_native_common_area_refines_r301_target_for_visible_band_compatibility(self):
        projection = {"proj": "geos", "lon_0": 140.7, "h": 35785863, "a": 6378137, "b": 6356752.31414}
        frame_extents = [
            (
                "20250801_2020",
                (-350000.002, 3080000.020, 650000.004, 4080000.026),
                (-349999.998, 3079999.982, 649999.996, 4079999.977),
            ),
            (
                "20250801_2220",
                (-350000.002, 3080000.020, 650000.004, 4080000.026),
                (-349999.998, 3079999.982, 649999.996, 4079999.977),
            ),
            (
                "20250802_0020",
                (-320000.002, 3100000.020, 680000.004, 4100000.026),
                (-319999.998, 3099999.982, 679999.996, 4099999.976),
            ),
            (
                "20250802_0220",
                (-290000.002, 3130000.020, 710000.005, 4130000.027),
                (-289999.998, 3129999.982, 709999.996, 4129999.976),
            ),
            (
                "20250802_0420",
                (-280000.002, 3160000.020, 720000.005, 4160000.027),
                (-279999.998, 3159999.982, 719999.996, 4159999.976),
            ),
        ]
        b03_areas = []
        compatibility_areas = {}
        for frame, b01_extent, b03_extent in frame_extents:
            b01_area = AreaDefinition(f"{frame}_b01", "b01", "b01", projection, 1000, 1000, b01_extent)
            b03_area = AreaDefinition(f"{frame}_b03", "b03", "b03", projection, 2000, 2000, b03_extent)
            b03_areas.append(b03_area)
            for band in ("B01", "B02", "B04"):
                compatibility_areas[f"{frame}:{band}"] = b01_area
            compatibility_areas[f"{frame}:B03"] = b03_area

        b03_only_target = h.native_compatible_common_area(b03_areas, 500, source_pixel_size_m=500)
        refined_target = h.native_compatible_common_area(
            b03_areas,
            500,
            source_pixel_size_m=500,
            compatibility_areas=compatibility_areas,
        )

        self.assertEqual((b03_only_target.width, b03_only_target.height), (1860, 1840))
        self.assertIsNotNone(h.native_area_compatibility_error(compatibility_areas, b03_only_target))
        self.assertEqual((refined_target.width, refined_target.height), (1860, 1838))
        self.assertIsNone(h.native_area_compatibility_error(compatibility_areas, refined_target))

    def test_native_common_area_daytime_lock_excludes_fallback_only_b13(self):
        projection = {"proj": "geos", "lon_0": 140.7, "h": 35785863, "a": 6378137, "b": 6356752.31414}
        b01_area = AreaDefinition(
            "b01",
            "b01",
            "b01",
            projection,
            1000,
            1000,
            (-350000.002, 3080000.020, 650000.004, 4080000.026),
        )
        b03_area = AreaDefinition(
            "b03",
            "b03",
            "b03",
            projection,
            2000,
            2000,
            (-349999.998, 3079999.982, 649999.996, 4079999.977),
        )
        b13_area = AreaDefinition(
            "b13",
            "b13",
            "b13",
            projection,
            500,
            500,
            (-350000.0, 3080000.0, 650000.0, 4080000.0),
        )
        day_compatibility_areas = {
            "frame:B01": b01_area,
            "frame:B02": b01_area,
            "frame:B03": b03_area,
            "frame:B04": b01_area,
        }
        fallback_areas = {"frame:B13": b13_area}

        target = h.native_compatible_common_area(
            [b03_area],
            500,
            source_pixel_size_m=500,
            compatibility_areas=day_compatibility_areas,
        )

        self.assertIsNone(h.native_area_compatibility_error(day_compatibility_areas, target))
        self.assertIsNotNone(h.native_area_compatibility_error(fallback_areas, target))

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

    def test_timelapse_rejects_geotiff_frame_output(self):
        config = h.default_config()
        config.mode = "Timelapse"
        info = h.parse_url(h.USER_URL)
        start = h.datetime.strptime(info.timestamp, "%Y%m%d_%H%M")
        area = mock.Mock(width=22000, height=22000)

        with self.assertRaisesRegex(RuntimeError, "Timelapse frames would be too large"):
            h.validate_runtime_dependencies(config, info, start, area)

    def test_pyspectral_quality_handling(self):
        self.assertEqual(
            h.SATPY_OPTIONAL_DEP_FALLBACKS["True Color RGB (Enhanced)"],
            "true_color_nocorr",
        )
        self.assertIn("True Color RGB (Enhanced)", h.CUSTOM_SATPY_MISSING_DATASET_FALLBACKS)
        self.assertNotIn("True Color Reproduction Image", h.SATPY_OPTIONAL_DEP_FALLBACKS)
        self.assertIn("True Color Reproduction Image", h.QUALITY_CRITICAL_COMPOSITES)
        exc = ModuleNotFoundError("No module named 'pyspectral'")
        exc.name = "pyspectral"
        self.assertTrue(h.missing_optional_dependency(exc, "pyspectral"))
        self.assertFalse(h.missing_optional_dependency(exc, "not_pyspectral"))
        self.assertIn("pyspectral", h.missing_pyspectral_message("True Color Reproduction Image"))

    def test_missing_satpy_dataset_detection(self):
        exc = KeyError("\"No dataset matching 'DataQuery(name='true_color_reproduction')' found\"")
        chained = RuntimeError("outer")
        chained.__cause__ = KeyError("Dataset true_color_reproduction not found after resampling")

        self.assertTrue(h.missing_satpy_dataset(exc, "true_color_reproduction"))
        self.assertTrue(h.missing_satpy_dataset(chained, "true_color_reproduction"))
        self.assertFalse(h.missing_satpy_dataset(exc, "true_color"))
        self.assertTrue(
            h.use_true_color_reproduction_fallback(
                exc,
                "True Color Reproduction Image",
                "true_color_reproduction",
            )
        )
        true_color_exc = KeyError("\"No dataset matching 'DataQuery(name='true_color')' found\"")
        self.assertTrue(
            h.use_custom_satpy_missing_dataset_fallback(
                true_color_exc,
                "True Color RGB (Enhanced)",
                "true_color",
            )
        )

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
        source_area = mock.Mock()
        source_area.get_area_slices.return_value = (slice(0, 4), slice(0, 4))
        attrs = {"area": source_area, "sensor": "ahi"}
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

        result = h.process_frame(dt, info, mock.Mock(width=4, height=4), 0, 1, config=config, progress=progress)

        self.assertEqual(result, h.Path("out.png"))
        mock_save.assert_called_once()
        self.assertEqual(mock_save.call_args.args[1], h.CUSTOM_DATASET_NAMES["True Color Reproduction Image"])
        self.assertFalse(mock_save.call_args.kwargs["enhance"])
        self.assertTrue(any("custom low-RAM fallback" in message for message, _current, _total in events))

    @mock.patch("himawari_lowram_processor.cleanup_paths")
    @mock.patch("himawari_lowram_processor.save_custom_satpy_missing_dataset_fallback")
    @mock.patch("himawari_lowram_processor.resample_scene_low_ram")
    @mock.patch("himawari_lowram_processor.download_segments")
    @mock.patch("himawari_lowram_processor.Scene")
    def test_true_color_reproduction_resample_missing_dataset_uses_custom_fallback(
        self,
        mock_scene_class,
        mock_download,
        mock_resample,
        mock_fallback,
        mock_cleanup,
    ):
        original_scene = mock.Mock()
        original_scene.load.return_value = None
        mock_scene_class.return_value = original_scene
        mock_resample.side_effect = KeyError(
            "\"No dataset matching 'DataQuery(name='true_color_reproduction')' found\""
        )
        mock_fallback.return_value = h.Path("out.png")
        mock_download.return_value = [h.Path(f"segment-{idx}.dat") for idx in range(40)]
        config = h.default_config()
        config.composite_choice = "True Color Reproduction Image"
        config.use_night_fallback = False
        info = h.parse_url(h.USER_URL)
        dt = h.datetime.strptime(info.timestamp, "%Y%m%d_%H%M")

        result = h.process_frame(dt, info, mock.Mock(width=10, height=10), 0, 1, config=config)

        self.assertEqual(result, h.Path("out.png"))
        mock_fallback.assert_called_once()
        self.assertEqual(mock_fallback.call_args.args[1:3], ("True Color Reproduction Image", "true_color_reproduction"))
        mock_cleanup.assert_called_once()

    @mock.patch("himawari_lowram_processor.cleanup_paths")
    @mock.patch("himawari_lowram_processor.save_custom_satpy_missing_dataset_fallback")
    @mock.patch("himawari_lowram_processor.resample_scene_low_ram")
    @mock.patch("himawari_lowram_processor.download_segments")
    @mock.patch("himawari_lowram_processor.Scene")
    def test_true_color_rgb_resample_missing_dataset_uses_custom_fallback(
        self,
        mock_scene_class,
        mock_download,
        mock_resample,
        mock_fallback,
        mock_cleanup,
    ):
        original_scene = mock.Mock()
        original_scene.load.return_value = None
        mock_scene_class.return_value = original_scene
        mock_resample.side_effect = KeyError("\"No dataset matching 'DataQuery(name='true_color')' found\"")
        mock_fallback.return_value = h.Path("out.png")
        mock_download.return_value = [h.Path(f"segment-{idx}.dat") for idx in range(40)]
        config = h.default_config()
        config.composite_choice = "True Color RGB (Enhanced)"
        config.use_night_fallback = False
        info = h.parse_url(h.USER_URL)
        dt = h.datetime.strptime(info.timestamp, "%Y%m%d_%H%M")

        result = h.process_frame(dt, info, mock.Mock(width=10, height=10), 0, 1, config=config)

        self.assertEqual(result, h.Path("out.png"))
        mock_fallback.assert_called_once()
        self.assertEqual(mock_fallback.call_args.args[1:3], ("True Color RGB (Enhanced)", "true_color"))
        mock_cleanup.assert_called_once()

    @mock.patch("himawari_lowram_processor.cleanup_paths")
    @mock.patch("himawari_lowram_processor.save_custom_satpy_missing_dataset_fallback")
    @mock.patch("himawari_lowram_processor.download_segments")
    @mock.patch("himawari_lowram_processor.Scene")
    def test_true_color_rgb_load_missing_dataset_uses_custom_fallback(
        self,
        mock_scene_class,
        mock_download,
        mock_fallback,
        mock_cleanup,
    ):
        original_scene = mock.Mock()
        original_scene.load.side_effect = KeyError("\"No dataset matching 'DataQuery(name='true_color')' found\"")
        mock_scene_class.return_value = original_scene
        mock_fallback.return_value = h.Path("out.png")
        mock_download.return_value = [h.Path(f"segment-{idx}.dat") for idx in range(40)]
        config = h.default_config()
        config.composite_choice = "True Color RGB (Enhanced)"
        config.use_night_fallback = False
        info = h.parse_url(h.USER_URL)
        dt = h.datetime.strptime(info.timestamp, "%Y%m%d_%H%M")

        result = h.process_frame(dt, info, mock.Mock(width=10, height=10), 0, 1, config=config)

        self.assertEqual(result, h.Path("out.png"))
        mock_fallback.assert_called_once()
        self.assertEqual(mock_fallback.call_args.args[1:3], ("True Color RGB (Enhanced)", "true_color"))
        mock_cleanup.assert_called_once()

    @mock.patch("himawari_lowram_processor.cleanup_paths")
    @mock.patch("himawari_lowram_processor.download_segments")
    def test_process_frame_downloads_night_fallback_band_for_day_product(self, mock_download, _mock_cleanup):
        config = h.default_config()
        config.composite_choice = "True Color Reproduction Image"
        config.use_night_fallback = True
        info = h.parse_url(h.USER_URL)
        dt = h.datetime.strptime(info.timestamp, "%Y%m%d_%H%M")
        mock_download.return_value = []

        result = h.process_frame(dt, info, mock.Mock(width=10, height=10), 0, 1, config=config)

        self.assertIsNone(result)
        task_names = [task.destination.name for task in mock_download.call_args.args[0]]
        self.assertTrue(any("_B13_" in name for name in task_names))

    def test_download_segments_progress_for_existing_files(self):
        events = []

        def progress(message, current, total):
            events.append((message, current, total))

        task = h.DownloadTask("https://example.test/file.bz2", h.Path(__file__))
        result = h.download_segments([task], workers=1, auto_download=False, progress=progress)

        self.assertEqual(result, [h.Path(__file__)])
        self.assertTrue(events)
        self.assertEqual(events[-1][1:], (1, 1))

    def test_download_workload_summary_describes_bands_scans_and_workers(self):
        info = h.parse_url(h.USER_URL)
        dt = h.datetime.strptime(info.timestamp, "%Y%m%d_%H%M")
        with tempfile.TemporaryDirectory() as tmp_dir:
            tasks = h.make_download_tasks(info, dt, ("B01", "B02", "B03", "B04", "B13"), h.Path(tmp_dir))

        summary = h.download_workload_summary(tasks, worker_count=4)

        self.assertEqual(summary, "Downloading 50 segments (5 bands x 10 FLDK scans, 4 workers)")

    def test_download_task_label_uses_band_and_segment(self):
        info = h.parse_url(h.USER_URL)
        dt = h.datetime.strptime(info.timestamp, "%Y%m%d_%H%M")
        with tempfile.TemporaryDirectory() as tmp_dir:
            task = h.make_download_tasks(info, dt, ("B01",), h.Path(tmp_dir))[6]

        self.assertEqual(h.download_task_label(task), "B01 S07/10")

    @mock.patch("himawari_lowram_processor.stream_download_and_extract")
    def test_download_segments_progress_includes_workload_and_active_segments(self, mock_stream):
        events = []
        info = h.parse_url(h.USER_URL)
        dt = h.datetime.strptime(info.timestamp, "%Y%m%d_%H%M")
        with tempfile.TemporaryDirectory() as tmp_dir:
            tasks = h.make_download_tasks(info, dt, ("B01",), h.Path(tmp_dir))[:2]

            def progress(message, current, total):
                events.append((message, current, total))

            mock_stream.side_effect = [task.destination for task in tasks]

            result = h.download_segments(tasks, workers=1, progress=progress)

        self.assertEqual(result, [task.destination for task in tasks])
        self.assertEqual(events[0], ("Downloading 2 segments (1 band x 10 FLDK scans, 1 worker)", 0, 2))
        self.assertTrue(any(event[0] == "Downloading B01 S01/10" for event in events))
        self.assertTrue(any(event[0] == "Downloaded B01 S02/10 (2/2)" for event in events))

    @mock.patch("himawari_lowram_processor.requests.get")
    def test_stream_download_removes_stale_part_before_retry(self, mock_get):
        with tempfile.TemporaryDirectory() as tmp_dir:
            destination = h.Path(tmp_dir) / "segment.dat"
            part_path = destination.with_suffix(destination.suffix + ".part")
            part_path.write_text("stale")
            payload = bz2.compress(b"fresh")
            response = mock.Mock()
            response.status_code = 200
            response.iter_content.return_value = [payload]
            response.__enter__ = mock.Mock(return_value=response)
            response.__exit__ = mock.Mock(return_value=None)
            mock_get.return_value = response

            result = h.stream_download_and_extract(h.DownloadTask("https://example.test/file.bz2", destination))

            self.assertEqual(result, destination)
            self.assertEqual(destination.read_bytes(), b"fresh")
            self.assertFalse(part_path.exists())

    @mock.patch("himawari_lowram_processor.requests.get")
    def test_stream_download_http_error_cleans_stale_part(self, mock_get):
        with tempfile.TemporaryDirectory() as tmp_dir:
            destination = h.Path(tmp_dir) / "missing.dat"
            part_path = destination.with_suffix(destination.suffix + ".part")
            part_path.write_text("stale")
            response = mock.Mock()
            response.status_code = 404
            response.__enter__ = mock.Mock(return_value=response)
            response.__exit__ = mock.Mock(return_value=None)
            mock_get.return_value = response

            result = h.stream_download_and_extract(h.DownloadTask("https://example.test/missing.bz2", destination))

            self.assertIsNone(result)
            self.assertFalse(part_path.exists())

    @mock.patch("himawari_lowram_processor.requests.get")
    def test_stream_download_stale_part_unlink_failure_is_handled(self, mock_get):
        with tempfile.TemporaryDirectory() as tmp_dir:
            destination = h.Path(tmp_dir) / "segment.dat"
            part_path = destination.with_suffix(destination.suffix + ".part")
            part_path.write_text("stale")
            task = h.DownloadTask("https://example.test/file.bz2", destination)

            with mock.patch("himawari_lowram_processor.remove_partial_download", return_value=False):
                result = h.stream_download_and_extract(task)

        self.assertIsNone(result)
        mock_get.assert_not_called()

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

    @mock.patch("himawari_lowram_processor.cleanup_paths")
    def test_cleanup_partial_downloads_only_removes_part_files(self, mock_cleanup):
        with tempfile.TemporaryDirectory() as tmp_dir:
            frame_dir = h.Path(tmp_dir)
            part_path = frame_dir / "segment.dat.part"
            complete_path = frame_dir / "segment.dat"
            part_path.write_text("partial")
            complete_path.write_text("complete")

            h.cleanup_partial_downloads(frame_dir)

        mock_cleanup.assert_called_once()
        self.assertEqual(mock_cleanup.call_args.args[0], [part_path])

    @mock.patch("himawari_lowram_processor.cleanup_paths")
    def test_success_cleanup_keeps_completed_download_cache(self, mock_cleanup):
        frame_dir = h.Path("temp") / "20240725_0400"

        h.cleanup_partial_downloads(frame_dir)

        mock_cleanup.assert_called_once()
        glob_arg = mock_cleanup.call_args.args[0]
        self.assertTrue(all(path.suffix == ".part" for path in glob_arg))

    @mock.patch("himawari_lowram_processor.cleanup_paths")
    def test_assemble_timelapse_deletes_frame_paths_when_configured(self, mock_cleanup):
        writer = mock.Mock()
        writer.__enter__ = mock.Mock(return_value=writer)
        writer.__exit__ = mock.Mock(return_value=None)
        fake_imageio = mock.Mock()
        fake_imageio.get_writer.return_value = writer
        fake_imageio.imread.side_effect = ["frame-a", "frame-b"]
        config = h.default_config()
        config.mode = "Timelapse"
        config.timelapse_format = "gif"
        config.delete_timelapse_frames = True
        paths = [h.Path("frame_0000.png"), h.Path("frame_0001.png")]
        info = h.parse_url(h.USER_URL)

        with mock.patch("imageio.v2.get_writer", fake_imageio.get_writer), mock.patch(
            "imageio.v2.imread", fake_imageio.imread
        ):
            result = h.assemble_timelapse(paths, info, config=config)

        self.assertEqual(result.suffix, ".gif")
        self.assertEqual(writer.append_data.call_args_list, [mock.call("frame-a"), mock.call("frame-b")])
        mock_cleanup.assert_called_once_with(paths)

    @mock.patch("himawari_lowram_processor.cleanup_paths")
    def test_assemble_timelapse_keeps_frame_paths_when_configured(self, mock_cleanup):
        writer = mock.Mock()
        writer.__enter__ = mock.Mock(return_value=writer)
        writer.__exit__ = mock.Mock(return_value=None)
        fake_imageio = mock.Mock()
        fake_imageio.get_writer.return_value = writer
        fake_imageio.imread.return_value = "frame"
        config = h.default_config()
        config.mode = "Timelapse"
        config.timelapse_format = "gif"
        config.delete_timelapse_frames = False
        info = h.parse_url(h.USER_URL)

        with mock.patch("imageio.v2.get_writer", fake_imageio.get_writer), mock.patch(
            "imageio.v2.imread", fake_imageio.imread
        ):
            h.assemble_timelapse([h.Path("frame_0000.png")], info, config=config)

        mock_cleanup.assert_not_called()

    @mock.patch("himawari_lowram_processor.cleanup_paths")
    def test_assemble_timelapse_falls_back_to_gif_without_ffmpeg(self, _mock_cleanup):
        writer = mock.Mock()
        writer.__enter__ = mock.Mock(return_value=writer)
        writer.__exit__ = mock.Mock(return_value=None)
        fake_imageio = mock.Mock()
        fake_imageio.get_writer.return_value = writer
        fake_imageio.imread.return_value = "frame"
        config = h.default_config()
        config.mode = "Timelapse"
        config.timelapse_format = "mp4"
        config.delete_timelapse_frames = False
        info = h.parse_url(h.USER_URL)

        with mock.patch("imageio.v2.get_writer", fake_imageio.get_writer), mock.patch(
            "imageio.v2.imread", fake_imageio.imread
        ), mock.patch("himawari_lowram_processor.has_module", return_value=False):
            result = h.assemble_timelapse([h.Path("frame_0000.png")], info, config=config)

        self.assertEqual(result.suffix, ".gif")
        fake_imageio.get_writer.assert_called_once()
        self.assertEqual(fake_imageio.get_writer.call_args.kwargs["mode"], "I")

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
        fake_path = h.Path("fake-b03.dat")
        fake_area = mock.Mock(width=10, height=10)
        fake_scene = mock.Mock()
        fake_scene.__getitem__ = mock.Mock(return_value=mock.Mock(attrs={"area": fake_area}))
        mock_scene_class.return_value = fake_scene
        mock_download.return_value = [fake_path] * info.total_segments
        mock_native_area.return_value = fake_area

        def progress(message, current, total):
            events.append((message, current, total))

        result = h.common_area_from_frames(info, [dt], 500, "B03", config=config, progress=progress)

        self.assertIs(result, fake_area)
        self.assertEqual(mock_download.call_args.args[0][0].destination.name, "HS_H09_20240725_0400_B03_FLDK_R05_S0110.DAT")
        self.assertIs(mock_download.call_args.kwargs["progress"], progress)
        fake_scene.load.assert_called_once_with(["B03"], calibration="reflectance")
        mock_native_area.assert_called_once_with(
            [fake_area],
            500,
            source_pixel_size_m=500,
            compatibility_areas={"20240725_0400:B03": fake_area},
        )
        self.assertTrue(events)
        self.assertEqual(events[0][0], "Downloading B03 scan segments for common area")
        mock_cleanup.assert_not_called()

    @mock.patch("himawari_lowram_processor.native_compatible_common_area")
    @mock.patch("himawari_lowram_processor.Scene")
    @mock.patch("himawari_lowram_processor.download_segments")
    def test_common_area_scan_loads_compatibility_bands(
        self,
        mock_download,
        mock_scene_class,
        mock_native_area,
    ):
        config = h.default_config()
        info = h.parse_url(h.USER_URL)
        dt = h.datetime.strptime(info.timestamp, "%Y%m%d_%H%M")
        fake_area = mock.Mock(width=10, height=10)
        fake_scene = mock.Mock()
        fake_scene.__getitem__ = mock.Mock(return_value=mock.Mock(attrs={"area": fake_area}))
        mock_scene_class.return_value = fake_scene
        mock_download.return_value = [h.Path(f"segment-{idx}.dat") for idx in range(info.total_segments * 4)]
        mock_native_area.return_value = fake_area

        result = h.common_area_from_frames(
            info,
            [dt],
            500,
            "B03",
            compatibility_bands=("B01", "B02", "B03", "B04"),
            config=config,
        )

        self.assertIs(result, fake_area)
        task_names = [task.destination.name for task in mock_download.call_args.args[0]]
        self.assertTrue(any("_B01_" in name for name in task_names))
        self.assertTrue(any("_B03_" in name for name in task_names))
        fake_scene.load.assert_any_call(["B01"], calibration="reflectance")
        fake_scene.load.assert_any_call(["B03"], calibration="reflectance")
        self.assertEqual(len(mock_native_area.call_args.kwargs["compatibility_areas"]), 4)

    @mock.patch("himawari_lowram_processor.process_frame")
    @mock.patch("himawari_lowram_processor.validate_runtime_dependencies")
    @mock.patch("himawari_lowram_processor.common_area_from_frames")
    def test_run_passes_day_product_bands_to_common_area(self, mock_common_area, _mock_validate, mock_process):
        config = h.default_config()
        config.mode = "Single Image"
        config.composite_choice = "True Color Reproduction Image"
        config.use_night_fallback = True
        mock_common_area.return_value = mock.Mock(width=10, height=10)
        mock_process.return_value = h.Path("out.png")

        h.run(config)

        self.assertEqual(
            mock_common_area.call_args.kwargs["compatibility_bands"],
            h.area_compatibility_bands("True Color Reproduction Image"),
        )
        self.assertNotIn("B13", mock_common_area.call_args.kwargs["compatibility_bands"])

    @mock.patch("himawari_lowram_processor.process_frame")
    @mock.patch("himawari_lowram_processor.validate_runtime_dependencies")
    @mock.patch("himawari_lowram_processor.common_area_from_frames")
    def test_run_single_band_common_area_keeps_single_required_band(
        self,
        mock_common_area,
        _mock_validate,
        mock_process,
    ):
        config = h.default_config()
        config.mode = "Single Image"
        config.composite_choice = "B13 (Infrared Window)"
        mock_common_area.return_value = mock.Mock(width=10, height=10)
        mock_process.return_value = h.Path("out.png")

        h.run(config)

        self.assertEqual(mock_common_area.call_args.kwargs["compatibility_bands"], ("B13",))

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

    @mock.patch("himawari_lowram_processor.assemble_timelapse", return_value=h.Path("movie.gif"))
    @mock.patch("himawari_lowram_processor.process_frame")
    @mock.patch("himawari_lowram_processor.validate_runtime_dependencies")
    @mock.patch("himawari_lowram_processor.common_area_from_frames")
    def test_timelapse_warns_when_only_some_frames_process(
        self,
        mock_common_area,
        _mock_validate,
        mock_process,
        mock_assemble,
    ):
        events = []
        config = h.default_config()
        config.mode = "Timelapse"
        config.hours_back = 1
        config.interval_minutes = 30
        mock_common_area.return_value = mock.Mock(width=10, height=10)
        mock_process.side_effect = [h.Path("frame_0000.png"), None, h.Path("frame_0002.png")]

        result = h.run(config, progress=lambda message, current, total: events.append((message, current, total)))

        self.assertEqual(result, [h.Path("movie.gif")])
        mock_assemble.assert_called_once()
        self.assertTrue(any("successful frames" in message for message, _current, _total in events))


if __name__ == "__main__":
    unittest.main()
