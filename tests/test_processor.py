import unittest
import bz2
import io
import tempfile
import threading
import warnings
from unittest import mock

import himawari_lowram_processor as h
import dask.array as da
import xarray as xr
from pyresample.geometry import AreaDefinition


class FakeWidget:
    def __init__(self):
        self.configured: dict[str, object] = {}

    def configure(self, **kwargs):
        self.configured.update(kwargs)


class FakeVar:
    def __init__(self, value=""):
        self.value = value

    def set(self, value):
        self.value = value

    def get(self):
        return self.value


class FakeRoot:
    def __init__(self):
        self.clipboard = ""
        self.updated = False

    def clipboard_clear(self):
        self.clipboard = ""

    def clipboard_append(self, text):
        self.clipboard += text

    def update_idletasks(self):
        self.updated = True


def write_test_png(path):
    from PIL import Image

    path.parent.mkdir(parents=True, exist_ok=True)
    Image.new("RGB", (2, 2), (10, 20, 30)).save(path)


def fake_imageio_writer_creating_file():
    writer = mock.Mock()
    writer.__enter__ = mock.Mock(return_value=writer)

    def exit_writer(*_args):
        h.Path(writer.output_path).write_bytes(b"movie")
        return None

    writer.__exit__ = mock.Mock(side_effect=exit_writer)

    def get_writer(path, **_kwargs):
        writer.output_path = path
        return writer

    return writer, get_writer


class FakeScheduledRoot(FakeRoot):
    def __init__(self):
        super().__init__()
        self.callbacks = []

    def after_idle(self, callback):
        self.callbacks.append(callback)

    def after(self, _delay, callback):
        self.callbacks.append(callback)


class FakePane:
    def __init__(self, heights):
        self.heights = list(heights)
        self.sash_positions = []

    def winfo_height(self):
        if self.heights:
            return self.heights.pop(0)
        return 800

    def sashpos(self, index, position):
        self.sash_positions.append((index, position))


class FakeNotebook:
    def __init__(self):
        self.hidden = []
        self.added = []
        self.selected = None

    def hide(self, tab_id):
        self.hidden.append(tab_id)

    def add(self, tab_id, **kwargs):
        self.added.append((tab_id, kwargs))

    def select(self, tab_id):
        self.selected = tab_id


class FakeEvent:
    def __init__(self, delta=0, num=None):
        self.delta = delta
        self.num = num


class ProcessorTests(unittest.TestCase):
    def test_configure_known_warning_filters_suppresses_noisy_optional_gpu_and_projection_warnings(self):
        with warnings.catch_warnings(record=True) as captured:
            warnings.simplefilter("always")
            h.configure_known_warning_filters()
            warnings.warn_explicit(
                "CUDA path could not be detected. Set CUDA_PATH environment variable if CuPy fails to load.",
                UserWarning,
                "cupy/_environment.py",
                284,
                module="cupy._environment",
            )
            warnings.warn_explicit(
                "invalid value encountered in cos",
                RuntimeWarning,
                "dask/_task_spec.py",
                768,
                module="dask._task_spec",
            )
            warnings.warn_explicit(
                "invalid value encountered in sin",
                RuntimeWarning,
                "dask/_task_spec.py",
                768,
                module="dask._task_spec",
            )

        self.assertEqual(captured, [])

    def test_app_version_label(self):
        self.assertRegex(h.APP_VERSION, r"^\d{4}\.\d{2}\.\d{2}\.\d+$")
        self.assertIn(h.APP_VERSION, h.app_version_label())
        self.assertIn(h.APP_DISPLAY_NAME, h.app_version_label())

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

    def test_latest_fldk_url_from_listing_uses_first_b01_segment(self):
        xml = """<?xml version="1.0" encoding="UTF-8"?>
        <ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
          <Contents><Key>AHI-L1b-FLDK/2026/05/25/0400/HS_H09_20260525_0400_B01_FLDK_R10_S0110.DAT.bz2</Key></Contents>
          <Contents><Key>AHI-L1b-FLDK/2026/05/25/0400/HS_H09_20260525_0400_B02_FLDK_R10_S0110.DAT.bz2</Key></Contents>
        </ListBucketResult>
        """

        url = h.latest_fldk_url_from_listing(xml)

        self.assertEqual(
            url,
            "https://noaa-himawari9.s3.amazonaws.com/AHI-L1b-FLDK/2026/05/25/0400/HS_H09_20260525_0400_B01_FLDK_R10_S0110.DAT.bz2",
        )

    def test_latest_fldk_url_from_listing_returns_none_without_scan_key(self):
        xml = """<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"></ListBucketResult>"""

        self.assertIsNone(h.latest_fldk_url_from_listing(xml))

    def test_find_latest_fldk_url_continues_after_network_failure(self):
        xml = """<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
          <Contents><Key>AHI-L1b-FLDK/2026/05/25/0400/HS_H09_20260525_0400_B01_FLDK_R10_S0110.DAT.bz2</Key></Contents>
        </ListBucketResult>"""
        calls = []

        def fetch(prefix):
            calls.append(prefix)
            if len(calls) == 1:
                raise h.requests.RequestException("temporary")
            return xml

        url = h.find_latest_fldk_url(
            now=h.datetime(2026, 5, 25, 4, 10),
            lookback_hours=1,
            fetch_listing=fetch,
        )

        self.assertTrue(url.endswith("_B01_FLDK_R10_S0110.DAT.bz2"))
        self.assertGreaterEqual(len(calls), 2)

    def test_find_latest_fldk_url_reports_no_scan_found(self):
        with self.assertRaisesRegex(RuntimeError, "No FLDK scans found"):
            h.find_latest_fldk_url(
                now=h.datetime(2026, 5, 25, 4, 10),
                lookback_hours=1,
                fetch_listing=lambda _prefix: "<ListBucketResult />",
            )

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

    def test_parse_local_hsd_segment_accepts_dat_and_bz2(self):
        dat = h.parse_local_hsd_segment("HS_H09_20240725_0400_B13_FLDK_R20_S0110.DAT")
        compressed = h.parse_local_hsd_segment("HS_H09_20240725_0400_B13_FLDK_R20_S0210.dat.bz2")

        self.assertEqual(dat.timestamp, "20240725_0400")
        self.assertEqual(dat.band, "B13")
        self.assertEqual(dat.area, "FLDK")
        self.assertEqual(dat.segment, 1)
        self.assertFalse(dat.compressed)
        self.assertTrue(compressed.compressed)

    def test_parse_local_hsd_segment_rejects_bad_name_and_resolution(self):
        with self.assertRaisesRegex(ValueError, "file name"):
            h.parse_local_hsd_segment("bad-file.bz2")
        with self.assertRaisesRegex(ValueError, "B13 files must use R20"):
            h.parse_local_hsd_segment("HS_H09_20240725_0400_B13_FLDK_R05_S0110.DAT")

    def test_sorted_local_segments_rejects_duplicates_and_mixed_scans(self):
        with self.assertRaisesRegex(ValueError, "Duplicate local segment"):
            h.sorted_local_segments([
                "HS_H09_20240725_0400_B13_FLDK_R20_S0110.DAT",
                "HS_H09_20240725_0400_B13_FLDK_R20_S0110.dat.bz2",
            ])
        with self.assertRaisesRegex(ValueError, "one scan at a time"):
            h.sorted_local_segments([
                "HS_H09_20240725_0400_B13_FLDK_R20_S0110.DAT",
                "HS_H09_20240725_0410_B13_FLDK_R20_S0210.DAT",
            ])

    def test_import_local_hsd_segments_sorts_and_streams_to_cache(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            source_dir = h.Path(tmp_dir) / "source"
            temp_dir = h.Path(tmp_dir) / "temp"
            source_dir.mkdir()
            s02 = source_dir / "HS_H09_20240725_0400_B13_FLDK_R20_S0210.DAT.bz2"
            s01 = source_dir / "HS_H09_20240725_0400_B13_FLDK_R20_S0110.DAT"
            s02.write_bytes(bz2.compress(b"two"))
            s01.write_bytes(b"one")

            result = h.import_local_hsd_segments([s02, s01], temp_dir)

            self.assertEqual(result.bands, ("B13",))
            self.assertEqual(len(result.imported_paths), 2)
            self.assertEqual(result.imported_paths[0].name, "HS_H09_20240725_0400_B13_FLDK_R20_S0110.DAT")
            self.assertEqual(result.imported_paths[0].read_bytes(), b"one")
            self.assertEqual(result.imported_paths[1].read_bytes(), b"two")
            self.assertIn("AHI-L1b-FLDK/2024/07/25/0400/", result.synthetic_url)

    def test_import_local_hsd_segments_reuses_existing_cache_file(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            source_dir = h.Path(tmp_dir) / "source"
            temp_dir = h.Path(tmp_dir) / "temp"
            source_dir.mkdir()
            source = source_dir / "HS_H09_20240725_0400_B13_FLDK_R20_S0110.DAT"
            source.write_bytes(b"new")
            destination = temp_dir / "20240725_0400" / source.name
            destination.parent.mkdir(parents=True)
            destination.write_bytes(b"existing")

            result = h.import_local_hsd_segments([source], temp_dir)

            self.assertEqual(result.imported_paths, ())
            self.assertEqual(result.reused_paths, (destination,))
            self.assertEqual(destination.read_bytes(), b"existing")

    def test_import_local_hsd_segments_reports_missing_selected_file(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            missing = h.Path(tmp_dir) / "HS_H09_20240725_0400_B13_FLDK_R20_S0110.DAT"

            with self.assertRaisesRegex(FileNotFoundError, "Local HSD import file"):
                h.import_local_hsd_segments([missing], h.Path(tmp_dir) / "temp")

    def test_import_local_hsd_segments_reports_empty_selected_file(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            source = h.Path(tmp_dir) / "HS_H09_20240725_0400_B13_FLDK_R20_S0110.DAT"
            source.write_bytes(b"")

            with self.assertRaisesRegex(FileNotFoundError, "empty"):
                h.import_local_hsd_segments([source], h.Path(tmp_dir) / "temp")

    def test_parse_local_hsd_segment_rejects_invalid_timestamp(self):
        with self.assertRaisesRegex(ValueError, "Invalid local HSD timestamp"):
            h.parse_local_hsd_segment("HS_H09_20241399_0400_B13_FLDK_R20_S0110.DAT")

    def test_offline_source_url_for_target_area_uses_target_folder(self):
        result = h.LocalImportResult(
            sat_id="HS_H09",
            timestamp="20240725_0400",
            area="R301",
            total_segments=1,
            imported_paths=(),
            reused_paths=(),
            bands=("B13",),
        )

        url = h.offline_source_url_for_import(result)
        info = h.parse_url(url)

        self.assertIn("AHI-L1b-Target", url)
        self.assertEqual(info.area, "R301")

    def test_import_local_hsd_segments_cleans_failed_bz2_part(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            source_dir = h.Path(tmp_dir) / "source"
            temp_dir = h.Path(tmp_dir) / "temp"
            source_dir.mkdir()
            source = source_dir / "HS_H09_20240725_0400_B13_FLDK_R20_S0110.DAT.bz2"
            source.write_bytes(b"not bz2")

            with self.assertRaises(Exception):
                h.import_local_hsd_segments([source], temp_dir)

            part = temp_dir / "20240725_0400" / "HS_H09_20240725_0400_B13_FLDK_R20_S0110.DAT.part"
            self.assertFalse(part.exists())

    def test_offline_preflight_blocks_missing_cached_segments(self):
        config = h.default_config()
        config.auto_download = False
        config.composite_choice = "B13 (Infrared Window)"

        with tempfile.TemporaryDirectory() as tmp_dir:
            result = h.preflight_run(config, temp_dir=h.Path(tmp_dir))

        self.assertFalse(result.ok)
        self.assertTrue(any("Offline cache is missing" in error for error in result.errors))

    def test_offline_preflight_passes_complete_cached_segments(self):
        config = h.default_config()
        config.auto_download = False
        config.composite_choice = "B13 (Infrared Window)"
        info = h.parse_url(config.user_url)
        dt = h.datetime.strptime(info.timestamp, "%Y%m%d_%H%M")

        with tempfile.TemporaryDirectory() as tmp_dir:
            temp_dir = h.Path(tmp_dir)
            for task in h.make_download_tasks(info, dt, ("B13",), temp_dir):
                task.destination.write_bytes(b"cached")

            result = h.preflight_run(config, temp_dir=temp_dir)

        self.assertTrue(result.ok)

    def test_offline_preflight_requires_product_bands_not_only_imported_band(self):
        config = h.default_config()
        config.auto_download = False
        config.composite_choice = "True Color Reproduction Image"
        info = h.parse_url(config.user_url)
        dt = h.datetime.strptime(info.timestamp, "%Y%m%d_%H%M")

        with tempfile.TemporaryDirectory() as tmp_dir:
            temp_dir = h.Path(tmp_dir)
            for task in h.make_download_tasks(info, dt, ("B13",), temp_dir):
                task.destination.write_bytes(b"cached")

            result = h.preflight_run(config, temp_dir=temp_dir)

        self.assertFalse(result.ok)
        self.assertTrue(any("Offline cache is missing" in error for error in result.errors))

    def test_download_segments_offline_ignores_empty_cache_files(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            empty = h.Path(tmp_dir) / "empty.dat"
            full = h.Path(tmp_dir) / "full.dat"
            empty.write_bytes(b"")
            full.write_bytes(b"cached")
            tasks = [
                h.DownloadTask("https://example.test/empty.bz2", empty),
                h.DownloadTask("https://example.test/full.bz2", full),
            ]

            found = h.download_segments(tasks, workers=1, auto_download=False)

        self.assertEqual(found, [full])

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

    def test_recommend_performance_settings_safe_mode_is_conservative(self):
        profile = h.SystemPerformanceProfile(total_ram_gb=16.0, available_ram_gb=10.0, cpu_count=8, cpu_percent=20.0)

        result = h.recommend_performance_settings(profile, "safe")

        self.assertEqual(result.download_workers, 2)
        self.assertEqual(result.dask_num_workers, 1)
        self.assertEqual(result.dask_chunk_size, "32MiB")
        self.assertLessEqual(result.ram_limit_gb, 8.0)

    def test_recommend_performance_settings_best_performance_uses_headroom(self):
        profile = h.SystemPerformanceProfile(total_ram_gb=64.0, available_ram_gb=32.0, cpu_count=12, cpu_percent=10.0)

        result = h.recommend_performance_settings(profile, "best_performance")

        self.assertEqual(result.download_workers, 4)
        self.assertEqual(result.dask_num_workers, 2)
        self.assertEqual(result.dask_chunk_size, "128MiB")
        self.assertGreaterEqual(result.ram_limit_gb, 12.0)

    @mock.patch("himawari_lowram_processor.importlib.util.find_spec", return_value=None)
    def test_gpu_support_status_reports_missing_cupy(self, _mock_spec):
        status = h.gpu_support_status()

        self.assertFalse(status.ok)
        self.assertIn("CuPy is not installed", status.detail)

    @mock.patch("himawari_lowram_processor.importlib.util.find_spec", return_value=object())
    def test_gpu_support_status_reports_kernel_test_failure(self, _mock_spec):
        class FakeRuntime:
            @staticmethod
            def getDeviceCount():
                return 1

            @staticmethod
            def getDeviceProperties(_device):
                return {"name": b"Fake GPU"}

        class FakeCuda:
            runtime = FakeRuntime()
            Stream = mock.Mock(null=mock.Mock(synchronize=mock.Mock()))

        fake_cupy = mock.Mock(
            __version__="13.0",
            cuda=FakeCuda(),
            float32=float,
            asarray=mock.Mock(side_effect=RuntimeError("missing headers")),
        )

        with mock.patch.dict("sys.modules", {"cupy": fake_cupy}):
            status = h.gpu_support_status()

        self.assertFalse(status.ok)
        self.assertIn("CUDA kernel test failed", status.detail)
        self.assertIn("GPU Fix", status.detail)

    @mock.patch("himawari_lowram_processor.importlib.util.find_spec", return_value=object())
    def test_gpu_support_status_accepts_kernel_test_success(self, _mock_spec):
        class FakeArray:
            def __init__(self, value):
                self.value = float(value)

            def __add__(self, other):
                return FakeArray(self.value + float(other))

            def astype(self, _dtype):
                return self

        class FakeRuntime:
            @staticmethod
            def getDeviceCount():
                return 1

            @staticmethod
            def getDeviceProperties(_device):
                return {"name": b"Fake GPU"}

        class FakeCuda:
            runtime = FakeRuntime()
            Stream = mock.Mock(null=mock.Mock(synchronize=mock.Mock()))

        fake_cupy = mock.Mock(
            __version__="13.0",
            cuda=FakeCuda(),
            float32=float,
            asarray=mock.Mock(return_value=FakeArray(1.0)),
            asnumpy=mock.Mock(side_effect=lambda array: [array.value]),
            get_default_memory_pool=mock.Mock(return_value=mock.Mock(free_all_blocks=mock.Mock())),
        )

        with mock.patch.dict("sys.modules", {"cupy": fake_cupy}):
            status = h.gpu_support_status()

        self.assertTrue(status.ok)
        self.assertEqual(status.device_name, "Fake GPU")

    @mock.patch("himawari_lowram_processor.gpu_support_status")
    def test_require_gpu_ready_raises_when_unavailable(self, mock_status):
        mock_status.return_value = h.GpuSupportStatus(False, "missing")

        with self.assertRaisesRegex(RuntimeError, "GPU acceleration is enabled"):
            h.require_gpu_ready()

    @mock.patch("himawari_lowram_processor.gpu_support_status")
    def test_require_gpu_ready_returns_status_when_available(self, mock_status):
        ready = h.GpuSupportStatus(True, "ready", device_name="Test GPU", device_count=1, package_version="13")
        mock_status.return_value = ready

        self.assertEqual(h.require_gpu_ready(), ready)

    @mock.patch("himawari_lowram_processor.gpu_support_status")
    def test_config_validation_blocks_unavailable_gpu(self, mock_status):
        config = h.default_config()
        config.gpu_acceleration = True
        mock_status.return_value = h.GpuSupportStatus(False, "missing")

        with self.assertRaisesRegex(RuntimeError, "GPU acceleration is enabled"):
            h.validate_configuration(config)

    @mock.patch("himawari_lowram_processor.gpu_support_status")
    def test_setup_status_reports_unavailable_gpu(self, mock_status):
        config = h.default_config()
        config.gpu_acceleration = True
        mock_status.return_value = h.GpuSupportStatus(False, "missing")

        status = h.build_setup_status(config, h.Path("C:/Himawari/outputs"), h.Path("C:/Himawari/temp"))

        self.assertFalse(status.ok)
        self.assertTrue(any("GPU acceleration is enabled" in error for error in status.errors))

    @mock.patch("himawari_lowram_processor.gpu_support_status")
    def test_preflight_blocks_unavailable_gpu(self, mock_status):
        config = h.default_config()
        config.gpu_acceleration = True
        mock_status.return_value = h.GpuSupportStatus(False, "missing")

        result = h.preflight_run(config, h.Path("C:/Himawari/outputs"), h.Path("C:/Himawari/temp"))

        self.assertFalse(result.ok)
        self.assertTrue(any("GPU acceleration is enabled" in error for error in result.errors))

    @mock.patch("himawari_lowram_processor.gpu_support_status")
    def test_setup_status_reports_ready_gpu(self, mock_status):
        config = h.default_config()
        config.gpu_acceleration = True
        mock_status.return_value = h.GpuSupportStatus(True, "ready", device_name="Test GPU", device_count=1)

        with tempfile.TemporaryDirectory() as tmp_dir:
            status = h.build_setup_status(config, h.Path(tmp_dir) / "outputs", h.Path(tmp_dir) / "temp")

        self.assertTrue(status.ok)
        self.assertIn("GPU acceleration: experimental", status.display_text())

    @mock.patch("himawari_lowram_processor.gpu_support_status")
    def test_setup_status_warns_when_gpu_settings_are_cpu_memory_bottlenecked(self, mock_status):
        config = h.default_config()
        config.gpu_acceleration = True
        config.dask_chunk_size = "16MiB"
        config.ram_limit_gb = 2.0
        mock_status.return_value = h.GpuSupportStatus(True, "ready", device_name="Test GPU", device_count=1)

        with tempfile.TemporaryDirectory() as tmp_dir:
            status = h.build_setup_status(config, h.Path(tmp_dir) / "outputs", h.Path(tmp_dir) / "temp")

        self.assertTrue(status.ok)
        display = status.display_text()
        self.assertIn("larger Dask chunks", display)
        self.assertIn("RAM limit is low", display)

    @mock.patch("himawari_lowram_processor.gpu_support_status")
    def test_preflight_blocks_gpu_for_unsupported_product(self, mock_status):
        config = h.default_config()
        config.gpu_acceleration = True
        config.composite_choice = "B13 (Infrared Window)"
        mock_status.return_value = h.GpuSupportStatus(True, "ready", device_name="Test GPU", device_count=1)

        result = h.preflight_run(config, h.Path("C:/Himawari/outputs"), h.Path("C:/Himawari/temp"))

        self.assertFalse(result.ok)
        self.assertTrue(any("GPU acceleration is currently limited" in error for error in result.errors))

    def test_gpu_true_color_reproduction_block_returns_cpu_uint8_rgb(self):
        if not h.gpu_support_status().ok:
            self.skipTest("CuPy GPU support is not available")
        b01 = h.np.array([[20.0, 40.0], [60.0, h.np.nan]], dtype=h.np.float32)
        b02 = h.np.array([[30.0, 50.0], [70.0, 90.0]], dtype=h.np.float32)
        b03 = h.np.array([[40.0, 60.0], [80.0, 100.0]], dtype=h.np.float32)
        b04 = h.np.array([[25.0, 45.0], [65.0, 85.0]], dtype=h.np.float32)

        result = h.gpu_true_color_reproduction_block(b01, b02, b03, b04)

        self.assertIsInstance(result, h.np.ndarray)
        self.assertEqual(result.shape, (3, 2, 2))
        self.assertEqual(result.dtype, h.np.uint8)
        self.assertGreater(int(result.max()), 0)

    def test_gpu_true_color_reproduction_block_rejects_mismatched_shapes(self):
        if not h.gpu_support_status().ok:
            self.skipTest("CuPy GPU support is not available")
        b01 = h.np.ones((2, 2), dtype=h.np.float32)
        b02 = h.np.ones((2, 3), dtype=h.np.float32)
        b03 = h.np.ones((2, 2), dtype=h.np.float32)
        b04 = h.np.ones((2, 2), dtype=h.np.float32)

        with self.assertRaisesRegex(ValueError, "mismatched visible band shapes"):
            h.gpu_true_color_reproduction_block(b01, b02, b03, b04)

    def test_gpu_true_color_reproduction_block_hybrid_returns_cpu_uint8_rgb(self):
        if not h.gpu_support_status().ok:
            self.skipTest("CuPy GPU support is not available")
        b01 = h.np.ones((2, 2), dtype=h.np.float32) * 20.0
        b02 = h.np.ones((2, 2), dtype=h.np.float32) * 30.0
        b03 = h.np.array([[1.0, 3.0], [8.0, 12.0]], dtype=h.np.float32)
        b04 = h.np.ones((2, 2), dtype=h.np.float32) * 25.0
        b13 = h.np.ones((2, 2), dtype=h.np.float32) * 230.0

        result = h.gpu_true_color_reproduction_block(b01, b02, b03, b04, b13, use_hybrid=True)

        self.assertIsInstance(result, h.np.ndarray)
        self.assertEqual(result.shape, (3, 2, 2))
        self.assertEqual(result.dtype, h.np.uint8)
        self.assertGreater(int(result[:, 0, 0].mean()), int(result[:, 1, 1].mean()))

    @mock.patch("himawari_lowram_processor.gpu_support_status")
    def test_build_gpu_custom_composite_returns_cpu_backed_dask_chunks(self, mock_status):
        mock_status.return_value = h.GpuSupportStatus(True, "ready", device_name="Test GPU", device_count=1)
        if not h.gpu_support_status().ok:
            self.skipTest("CuPy GPU support is not available")
        area = AreaDefinition(
            "test",
            "test",
            "latlon",
            {"proj": "longlat", "datum": "WGS84"},
            2,
            2,
            (100.0, -10.0, 102.0, -8.0),
        )
        attrs = {"area": area, "sensor": "ahi"}
        scene = mock.MagicMock()
        data = {
            band: xr.DataArray(
                da.ones((2, 2), chunks=(1, 1)) * value,
                dims=("y", "x"),
                attrs=attrs,
            )
            for band, value in {"B01": 20.0, "B02": 30.0, "B03": 40.0, "B04": 25.0}.items()
        }
        scene.__getitem__.side_effect = lambda key: data[key]
        config = h.default_config()
        config.gpu_acceleration = True
        config.use_night_fallback = False

        name, dataset = h.build_gpu_custom_composite(scene, "True Color Reproduction Image", config)
        computed = dataset.data.compute()

        self.assertEqual(name, h.CUSTOM_DATASET_NAMES["True Color Reproduction Image"])
        self.assertIsInstance(computed, h.np.ndarray)
        self.assertEqual(computed.shape, (3, 2, 2))
        self.assertEqual(computed.dtype, h.np.uint8)

    @mock.patch("himawari_lowram_processor.gpu_support_status")
    def test_build_gpu_custom_composite_reports_missing_band(self, mock_status):
        mock_status.return_value = h.GpuSupportStatus(True, "ready", device_name="Test GPU", device_count=1)
        area = AreaDefinition(
            "test",
            "test",
            "latlon",
            {"proj": "longlat", "datum": "WGS84"},
            2,
            2,
            (100.0, -10.0, 102.0, -8.0),
        )
        attrs = {"area": area, "sensor": "ahi"}
        data = {
            band: xr.DataArray(da.ones((2, 2), chunks=(1, 1)), dims=("y", "x"), attrs=attrs)
            for band in ("B01", "B02", "B03")
        }
        scene = mock.MagicMock()

        def get_band(key):
            if key not in data:
                raise KeyError(key)
            return data[key]

        scene.__getitem__.side_effect = get_band
        config = h.default_config()
        config.gpu_acceleration = True
        config.use_night_fallback = False

        with self.assertRaisesRegex(RuntimeError, "missing required resampled band"):
            h.build_gpu_custom_composite(scene, "True Color Reproduction Image", config)

    @mock.patch("himawari_lowram_processor.gpu_support_status")
    def test_build_gpu_custom_composite_reports_mismatched_band_shape(self, mock_status):
        mock_status.return_value = h.GpuSupportStatus(True, "ready", device_name="Test GPU", device_count=1)
        area = AreaDefinition(
            "test",
            "test",
            "latlon",
            {"proj": "longlat", "datum": "WGS84"},
            2,
            2,
            (100.0, -10.0, 102.0, -8.0),
        )
        attrs = {"area": area, "sensor": "ahi"}
        data = {
            "B01": xr.DataArray(da.ones((2, 2), chunks=(1, 1)), dims=("y", "x"), attrs=attrs),
            "B02": xr.DataArray(da.ones((2, 2), chunks=(1, 1)), dims=("y", "x"), attrs=attrs),
            "B03": xr.DataArray(da.ones((2, 2), chunks=(1, 1)), dims=("y", "x"), attrs=attrs),
            "B04": xr.DataArray(da.ones((3, 2), chunks=(1, 1)), dims=("y", "x"), attrs=attrs),
        }
        scene = mock.MagicMock()
        scene.__getitem__.side_effect = lambda key: data[key]
        config = h.default_config()
        config.gpu_acceleration = True
        config.use_night_fallback = False

        with self.assertRaisesRegex(RuntimeError, "expected \\(2, 2\\)"):
            h.build_gpu_custom_composite(scene, "True Color Reproduction Image", config)

    def test_dataarray_to_cpu_chunks_preserves_lazy_dask_array(self):
        source = xr.DataArray(da.ones((2, 2), chunks=(1, 1)), dims=("y", "x"), attrs={"name": "demo"})

        with mock.patch("himawari_lowram_processor.dask_array_to_cpu_chunks", return_value=source.data) as mock_cpu:
            result = h.dataarray_to_cpu_chunks(source)

        self.assertIsInstance(result.data, da.Array)
        self.assertEqual(result.attrs["name"], "demo")
        mock_cpu.assert_called_once()

    def test_dask_array_to_cpu_chunks_leaves_numpy_blocks_unchanged(self):
        source = da.ones((2, 2), chunks=(1, 1))

        result = h.dask_array_to_cpu_chunks(source)

        self.assertIsInstance(result.compute(), h.np.ndarray)

    def test_maybe_cpu_scene_after_gpu_converts_named_datasets(self):
        config = h.default_config()
        config.gpu_acceleration = True
        scene = mock.MagicMock()
        first = xr.DataArray(da.ones((2, 2), chunks=(1, 1)), dims=("y", "x"))
        second = xr.DataArray(da.ones((2, 2), chunks=(1, 1)), dims=("y", "x"))
        scene.__getitem__.side_effect = lambda name: {"B03": first, "rgb": second}[name]

        with mock.patch("himawari_lowram_processor.dataarray_to_cpu_chunks", side_effect=lambda data: data) as mock_cpu:
            result = h.maybe_cpu_scene_after_gpu(scene, ("B03", "missing", "rgb"), config)

        self.assertIs(result, scene)
        self.assertEqual(mock_cpu.call_count, 2)
        assigned = [call.args[0] for call in scene.__setitem__.call_args_list]
        self.assertEqual(assigned, ["B03", "rgb"])

    def test_required_bands_include_night_fallback(self):
        bands = h.required_bands("True Color RGB (Enhanced)", use_night_fallback=True)
        self.assertIn("B13", bands)
        self.assertIn("B03", bands)

        day_only_bands = h.required_bands("True Color RGB (Enhanced)", use_night_fallback=False)
        self.assertNotIn("B13", day_only_bands)

    def test_select_active_composite(self):
        self.assertEqual(
            h.select_active_composite("True Color RGB (Enhanced)", True, use_night_fallback=True),
            "True Color RGB (Enhanced)",
        )
        self.assertEqual(
            h.select_active_composite(
                "True Color RGB (Enhanced)",
                True,
                use_night_fallback=True,
                night_fallback_mode="whole_frame_ir",
            ),
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

    def test_night_check_band_prefers_b03_then_reflectance_band(self):
        self.assertEqual(h.night_check_band_for_bands(("B01", "B03", "B13")), "B03")
        self.assertEqual(h.night_check_band_for_bands(("B05", "B13")), "B05")
        self.assertIsNone(h.night_check_band_for_bands(("B13", "B14")))

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

    def test_visible_sample_is_dark_ignores_small_bright_edge(self):
        values = da.zeros((10, 10), chunks=(5, 5))
        values = da.where(da.indices((10, 10), chunks=(5, 5))[1] == 9, 80.0, values)
        sample = xr.DataArray(values, dims=("y", "x"))

        is_dark, max_reflectance, bright_fraction, valid_pixels = h.visible_sample_is_dark(sample)

        self.assertTrue(is_dark)
        self.assertEqual(valid_pixels, 36)
        self.assertEqual(max_reflectance, 0.0)
        self.assertEqual(bright_fraction, 0.0)

    def test_visible_sample_is_day_when_center_is_bright(self):
        sample = xr.DataArray(da.ones((10, 10), chunks=(5, 5)) * 20.0, dims=("y", "x"))

        is_dark, _max_reflectance, bright_fraction, _valid_pixels = h.visible_sample_is_dark(sample)

        self.assertFalse(is_dark)
        self.assertGreater(bright_fraction, h.NIGHT_CHECK_BRIGHT_FRACTION)

    def test_is_visible_dark_fails_toward_night_fallback(self):
        scene = mock.Mock()
        scene.load.side_effect = RuntimeError("bad reflectance")

        with self.assertLogs(h.LOG, level="WARNING") as captured:
            result = h.is_visible_dark(scene, mock.Mock(width=10, height=10))

        self.assertTrue(result)
        self.assertTrue(any("using night fallback" in message for message in captured.output))

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

    def test_output_template_supports_safe_tokens(self):
        config = h.default_config()
        config.composite_choice = "B13 (Infrared Window)"
        config.output_template = "{scan_time}_{area}_{product}_{mode}_{band}_{format}"
        info = h.parse_url(h.USER_URL)
        dt = h.datetime.strptime(info.timestamp, "%Y%m%d_%H%M")

        output = h.output_filename(
            info,
            dt,
            config.composite_choice,
            config.mode,
            0,
            config.image_format,
            config=config,
        )

        self.assertIn("20240725_0400_FLDK_B13_Infrared_Window_Single_Image_B13_png", output.name)

    def test_output_template_rejects_unsafe_values(self):
        with self.assertRaisesRegex(ValueError, "Unknown output template token"):
            h.validate_output_template("{unknown}")
        with self.assertRaisesRegex(ValueError, "path separators"):
            h.validate_output_template("folder/{scan_time}")
        with self.assertRaisesRegex(ValueError, "required"):
            h.validate_output_template("")
        with self.assertRaisesRegex(ValueError, "reserved device name"):
            h.validate_output_template("CON")

    def test_scan_choice_listing_extracts_bands(self):
        xml = """<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
          <Contents><Key>AHI-L1b-FLDK/2026/05/25/0400/HS_H09_20260525_0400_B01_FLDK_R10_S0110.DAT.bz2</Key></Contents>
          <Contents><Key>AHI-L1b-FLDK/2026/05/25/0400/HS_H09_20260525_0400_B13_FLDK_R20_S0110.DAT.bz2</Key></Contents>
          <Contents><Key>AHI-L1b-FLDK/2026/05/25/0400/HS_H09_20260525_0400_B13_FLDK_R20_S0210.DAT.bz2</Key></Contents>
        </ListBucketResult>"""

        choices = h.fldk_scan_choices_from_listing(xml)

        self.assertEqual([choice.band for choice in choices], ["B01", "B13"])
        self.assertTrue(choices[0].url.startswith(h.NOAA_HIMAWARI9_BUCKET))

    def test_find_recent_scan_choices_handles_network_failures(self):
        xml = """<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
          <Contents><Key>AHI-L1b-FLDK/2026/05/25/0400/HS_H09_20260525_0400_B01_FLDK_R10_S0110.DAT.bz2</Key></Contents>
        </ListBucketResult>"""
        calls = []

        def fetch(prefix):
            calls.append(prefix)
            if len(calls) == 1:
                raise h.requests.RequestException("temporary")
            return xml

        choices = h.find_recent_fldk_scan_choices(
            now=h.datetime(2026, 5, 25, 4, 10),
            lookback_hours=1,
            fetch_listing=fetch,
            limit=1,
        )

        self.assertEqual(len(choices), 1)
        self.assertEqual(choices[0].band, "B01")

    def test_phase_status_formats_common_messages(self):
        self.assertEqual(h.phase_from_progress_message("Downloading B13"), "Downloading")
        self.assertEqual(h.phase_from_progress_message("Resampling"), "Resampling")
        self.assertIn("2/10", h.format_phase_status("Added frame 2/10", 2, 10))

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

        config = h.default_config()
        config.crosshair_type = "triangle"
        with self.assertRaisesRegex(ValueError, "Crosshair type"):
            h.validate_configuration(config)

        config = h.default_config()
        config.crosshair_color = "not-a-color"
        with self.assertRaisesRegex(ValueError, "Crosshair color"):
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
            ("dask_chunk_size", "256MiB", "Dask chunk size"),
            ("ram_limit_gb", 0, "RAM limit"),
            ("image_format", "jpg", "IMAGE_FORMAT"),
            ("timelapse_format", "avi", "TIMELAPSE_FORMAT"),
            ("resampler", "bilinear", "RESAMPLER"),
            ("ram_limit_gb", float("nan"), "RAM limit"),
            ("max_safe_png_pixels", 0, "Max safe PNG pixels"),
        ]
        for field, value, message in bad_values:
            with self.subTest(field=field):
                config = h.default_config()
                setattr(config, field, value)
                with self.assertRaisesRegex(ValueError, message):
                    h.validate_configuration(config)

    def test_setup_status_reports_non_numeric_values_without_crashing(self):
        config = h.default_config()
        config.ram_limit_gb = float("nan")
        config.border_line_width = float("inf")
        config.add_border_lines = True

        status = h.build_setup_status(config, h.Path("C:/Himawari/out"), h.Path("C:/Himawari/temp"))

        self.assertFalse(status.ok)
        self.assertTrue(any("RAM limit" in error for error in status.errors))
        self.assertTrue(any("Border line width" in error for error in status.errors))

    def test_setup_status_summarizes_valid_fldk_source(self):
        config = h.default_config()

        with tempfile.TemporaryDirectory() as tmp_dir:
            status = h.build_setup_status(config, h.Path(tmp_dir) / "outputs", h.Path(tmp_dir) / "temp")

        text = status.display_text()
        self.assertTrue(status.ok)
        self.assertIn("Source: FLDK 20240725_0400, 10 segments per band", text)
        self.assertIn("Download estimate", text)

    def test_setup_status_reports_invalid_url(self):
        config = h.default_config()
        config.user_url = "not a himawari url"

        status = h.build_setup_status(config)

        self.assertFalse(status.ok)
        self.assertTrue(any("URL not recognised" in error for error in status.errors))
        self.assertIn("Fix before starting", status.display_text())

    def test_setup_status_warns_for_fldk_png_true_color_geotiff_switch(self):
        config = h.default_config()
        config.composite_choice = "True Color RGB (Enhanced)"
        config.image_format = "png"

        status = h.build_setup_status(config, h.Path("C:/Himawari/outputs"), h.Path("C:/Himawari/temp"))

        self.assertTrue(any("auto-switch to GeoTIFF" in warning for warning in status.warnings))

    def test_setup_status_does_not_use_native_png_warning_for_flat_map(self):
        config = h.default_config()
        config.composite_choice = "True Color RGB (Enhanced)"
        config.image_format = "png"
        config.map_view = "flat"

        with tempfile.TemporaryDirectory() as tmp_dir:
            status = h.build_setup_status(config, h.Path(tmp_dir) / "outputs", h.Path(tmp_dir) / "temp")

        self.assertTrue(status.ok)
        self.assertTrue(any("Map view: Web Mercator flat map" in detail for detail in status.details))
        self.assertFalse(any("Full-disk 500 m PNG" in warning for warning in status.warnings))

    def test_setup_status_warns_for_border_overlay_requirements(self):
        config = h.default_config()
        config.add_border_lines = True

        with tempfile.TemporaryDirectory() as tmp_dir:
            with mock.patch.object(h, "PROJECT_DIR", h.Path(tmp_dir)):
                status = h.build_setup_status(config, h.Path("C:/Himawari/outputs"), h.Path("C:/Himawari/temp"))

        self.assertFalse(status.ok)
        self.assertTrue(any("Border lines require" in warning for warning in status.warnings))
        self.assertTrue(any("GSHHS/WDBII" in warning for warning in status.warnings))
        self.assertTrue(any("GSHHS_l_L1.shp" in error for error in status.errors))

    @mock.patch.object(h.Path, "resolve", autospec=True)
    def test_setup_status_warns_for_cloud_sync_paths(self, mock_resolve):
        def fake_resolve(path, strict=False):
            return h.Path("C:/Users/Isaac/OneDrive/Desktop/Himawari")

        mock_resolve.side_effect = fake_resolve
        config = h.default_config()

        status = h.build_setup_status(config, h.Path("C:/Users/Isaac/OneDrive/out"), h.Path("C:/Himawari/temp"))

        self.assertTrue(any("Cloud-sync path detected" in warning for warning in status.warnings))

    @mock.patch.object(h.Path, "resolve", autospec=True)
    def test_setup_status_ignores_cloud_synced_project_when_output_and_temp_are_local(self, mock_resolve):
        def fake_resolve(path, strict=False):
            text = str(path)
            if "HimawariLocal" in text:
                return h.Path("C:/HimawariLocal")
            return h.Path("C:/Users/Isaac/OneDrive/Desktop/Himawari")

        mock_resolve.side_effect = fake_resolve
        config = h.default_config()

        status = h.build_setup_status(config, h.Path("C:/HimawariLocal/out"), h.Path("C:/HimawariLocal/temp"))

        self.assertFalse(any("Cloud-sync path detected" in warning for warning in status.warnings))

    def test_setup_status_reports_unwritable_output_folder(self):
        config = h.default_config()

        with tempfile.TemporaryDirectory() as tmp_dir:
            output_file = h.Path(tmp_dir) / "not-a-folder"
            output_file.write_text("blocked", encoding="utf-8")
            temp_dir = h.Path(tmp_dir) / "temp"

            status = h.build_setup_status(config, output_file, temp_dir)

        self.assertFalse(status.ok)
        self.assertTrue(any("Output folder is not writable" in error for error in status.errors))

    @mock.patch.object(h.Path, "resolve", autospec=True)
    def test_saved_project_default_paths_migrate_off_cloud_sync(self, mock_resolve):
        def fake_resolve(path, strict=False):
            return path

        mock_resolve.side_effect = fake_resolve
        with tempfile.TemporaryDirectory() as tmp_dir:
            settings_path = h.Path(tmp_dir) / "settings.json"
            project_dir = h.Path(tmp_dir) / "OneDrive" / "project"
            output_dir = project_dir / "outputs"
            temp_dir = project_dir / "temp"
            write_data = h.serialize_gui_settings(h.default_config(), output_dir, temp_dir)
            h.write_json_file(settings_path, write_data)
            original_project = h.PROJECT_DIR
            try:
                h.PROJECT_DIR = project_dir
                loaded = h.load_gui_settings(settings_path)
            finally:
                h.PROJECT_DIR = original_project

        self.assertIsNotNone(loaded)
        self.assertEqual(loaded[1], h.OUTPUT_DIR)
        self.assertEqual(loaded[2], h.TEMP_DIR)

    def test_overlay_status_reports_missing_data(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            status = h.overlay_status(h.Path(tmp_dir), module_checker=lambda _module: True)

        self.assertFalse(status.ok)
        self.assertTrue(any("overlays" in item for item in status.missing_data))
        self.assertTrue(any("GSHHS_l_L1.shp" in item for item in status.missing_data))

    def test_overlay_status_rejects_arbitrary_shape_file(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            overlays = h.Path(tmp_dir) / "overlays"
            overlays.mkdir()
            (overlays / "coast.shp").write_text("fake")

            status = h.overlay_status(h.Path(tmp_dir), module_checker=lambda _module: True)

        self.assertFalse(status.ok)
        self.assertTrue(any("GSHHS_l_L1" in item for item in status.missing_data))

    def test_overlay_status_ready_when_required_pycoast_files_exist(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            for path in h.overlay_data_required_paths(h.Path(tmp_dir)):
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text("fake")
            for path in h.missing_overlay_sidecar_paths(h.Path(tmp_dir)):
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text("fake")

            status = h.overlay_status(h.Path(tmp_dir), module_checker=lambda _module: True)

        self.assertTrue(status.ok)

    def test_overlay_status_requires_nonempty_files_and_sidecars(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            for path in h.overlay_data_required_paths(h.Path(tmp_dir)):
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text("")

            status = h.overlay_status(h.Path(tmp_dir), module_checker=lambda _module: True)

        self.assertFalse(status.ok)
        self.assertTrue(any("GSHHS_l_L1.shp" in item for item in status.missing_data))
        self.assertTrue(any("GSHHS_l_L1.shx" in item for item in status.missing_data))

    def test_overlay_status_reports_missing_packages(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            for path in h.overlay_data_required_paths(h.Path(tmp_dir)):
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text("fake")
            for path in h.missing_overlay_sidecar_paths(h.Path(tmp_dir)):
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text("fake")

            status = h.overlay_status(h.Path(tmp_dir), module_checker=lambda module: module != "pycoast")

        self.assertFalse(status.ok)
        self.assertEqual(status.missing_packages, ("pycoast",))

    def test_build_run_summary_counts_frames_bands_and_segments(self):
        config = h.default_config()
        config.mode = "Timelapse"
        config.hours_back = 1
        config.interval_minutes = 30
        config.composite_choice = "B13 (Infrared Window)"

        summary = h.build_run_summary(config)

        self.assertEqual(summary.frames, 2)
        self.assertEqual(summary.bands, ("B13",))
        self.assertEqual(summary.total_segments, 20)
        self.assertIn("B13", summary.display_text())

    def test_required_bands_adds_hybrid_night_inputs_for_true_color(self):
        bands = h.required_bands(
            "True Color Reproduction Image",
            use_night_fallback=True,
            night_fallback_mode="hybrid",
        )

        self.assertIn("B03", bands)
        self.assertIn("B13", bands)
        self.assertEqual(bands.count("B13"), 1)

    def test_whole_frame_night_fallback_keeps_previous_band_behavior(self):
        bands = h.required_bands(
            "Day Convective Storm RGB",
            use_night_fallback=True,
            night_fallback_mode="whole_frame_ir",
        )

        self.assertIn("B14", bands)
        self.assertIn("B08", bands)

    def test_visible_dark_weight_feathers_between_day_and_night(self):
        b03 = xr.DataArray(
            da.from_array([[0.0, 0.9, 2.0]], chunks=(1, 3)),
            dims=("y", "x"),
        )

        weight = h.visible_dark_weight(b03).compute()

        self.assertAlmostEqual(float(weight[0, 0]), 1.0)
        self.assertAlmostEqual(float(weight[0, 1]), 0.5)
        self.assertAlmostEqual(float(weight[0, 2]), 0.0)

    def test_hybrid_day_night_rgb_preserves_dask_and_fills_dark_pixels(self):
        day = h.rgb_dataarray(
            xr.DataArray(da.from_array([[1.0, 0.2]], chunks=(1, 2)), dims=("y", "x")),
            xr.DataArray(da.from_array([[0.0, 0.2]], chunks=(1, 2)), dims=("y", "x")),
            xr.DataArray(da.from_array([[0.0, 0.2]], chunks=(1, 2)), dims=("y", "x")),
            "day",
            "day",
        )
        b03 = xr.DataArray(da.from_array([[0.0, 12.0]], chunks=(1, 2)), dims=("y", "x"))
        b13 = xr.DataArray(da.from_array([[190.0, 305.0]], chunks=(1, 2)), dims=("y", "x"))

        hybrid = h.create_hybrid_day_night_rgb(day, b03, b13, "True Color Reproduction Image")

        self.assertTrue(hasattr(hybrid.data, "dask"))
        result = hybrid.compute()
        self.assertGreater(int(result.sel(bands="R")[0, 0]), 200)
        self.assertEqual(int(result.sel(bands="R")[0, 1]), int(day.sel(bands="R")[0, 1].compute()))
        self.assertEqual(result.attrs["night_fallback_mode"], "hybrid")

    def test_custom_composite_hybrid_requires_and_uses_b13(self):
        attrs = {"area": mock.Mock(), "sensor": "ahi"}
        arrays = {
            band: xr.DataArray(da.ones((2, 2), chunks=(1, 1)) * value, dims=("y", "x"), attrs=attrs)
            for band, value in {
                "B01": 30.0,
                "B02": 35.0,
                "B03": 0.0,
                "B04": 40.0,
                "B13": 190.0,
            }.items()
        }
        scene = mock.MagicMock()
        scene.__contains__.side_effect = lambda key: key in arrays
        scene.__getitem__.side_effect = lambda key: arrays[key]
        config = h.default_config()
        config.use_night_fallback = True
        config.night_fallback_mode = "hybrid"

        name, dataset = h.build_custom_composite(scene, "True Color Reproduction Image", False, config)

        self.assertEqual(name, h.hybrid_dataset_name("True Color Reproduction Image"))
        self.assertEqual(dataset.attrs["night_fallback_mode"], "hybrid")
        self.assertGreater(int(dataset.sel(bands="R")[0, 0].compute()), 200)

    def test_flat_map_area_defaults_are_bounded_himawari_region(self):
        config = h.default_config()
        config.map_view = "flat"

        area = h.flat_map_area(config)

        self.assertEqual((area.height, area.width), (3018, 2400))
        self.assertIn("Mercator", area.crs.coordinate_operation.method_name)
        self.assertEqual(area.proj_id, "webmerc")
        self.assertAlmostEqual(area.area_extent[0], 8905559.263461886)
        self.assertAlmostEqual(area.area_extent[1], -8399737.889818357)
        self.assertAlmostEqual(area.area_extent[2], 22263898.158654712)
        self.assertAlmostEqual(area.area_extent[3], 8399737.889818357)
        self.assertGreater(area.area_extent[2], 20037508.342789244)

    def test_flat_map_validation_rejects_invalid_numbers_and_bounds(self):
        cases = [
            ("flat_resolution_deg", 0, "resolution must be positive"),
            ("flat_resolution_deg", float("inf"), "resolution must be finite"),
            ("flat_min_lat", float("nan"), "min latitude must be finite"),
            ("flat_min_lon", "west", "min longitude must be a finite number"),
            ("flat_min_lat", 60.0, "min latitude must be less than max latitude"),
            ("flat_max_lat", 86.0, "latitude bounds must be between"),
            ("flat_min_lon", 200.0, "min longitude must be less than max longitude"),
            ("flat_max_lon", 361.0, "longitude bounds must be between -360 and 360"),
        ]

        for field, value, message in cases:
            with self.subTest(field=field, value=value):
                config = h.default_config()
                config.map_view = "flat"
                setattr(config, field, value)

                with self.assertRaisesRegex(ValueError, message):
                    h.validate_flat_map_settings(config)

    def test_flat_map_validation_rejects_empty_output_size(self):
        config = h.default_config()
        config.map_view = "flat"
        config.flat_min_lat = 0.0
        config.flat_max_lat = 0.01
        config.flat_min_lon = 100.0
        config.flat_max_lon = 100.01
        config.flat_resolution_deg = 1.0

        with self.assertRaisesRegex(ValueError, "empty output"):
            h.validate_flat_map_settings(config)

    def test_flat_map_settings_reject_excessive_pixel_count(self):
        config = h.default_config()
        config.map_view = "flat"
        config.flat_resolution_deg = 0.005

        with self.assertRaisesRegex(ValueError, "Flat map would be"):
            h.validate_flat_map_settings(config)

    def test_build_setup_status_reports_invalid_flat_settings_without_crashing(self):
        config = h.default_config()
        config.map_view = "flat"
        config.flat_resolution_deg = float("nan")

        status = h.build_setup_status(config)

        self.assertFalse(status.ok)
        self.assertTrue(any("Flat map resolution must be finite" in error for error in status.errors))

    def test_preflight_reports_invalid_flat_settings_without_crashing(self):
        config = h.default_config()
        config.map_view = "flat"
        config.flat_min_lon = "west"

        result = h.preflight_run(config)

        self.assertFalse(result.ok)
        self.assertTrue(any("Flat map min longitude must be a finite number" in error for error in result.errors))

    def test_output_filename_appends_flat_map_suffix(self):
        config = h.default_config()
        config.map_view = "flat"
        info = h.parse_url(h.USER_URL)
        dt = h.datetime.strptime(info.timestamp, "%Y%m%d_%H%M")

        path = h.output_filename(info, dt, config.composite_choice, "Single Image", 0, "png", config=config)

        self.assertIn("Flat_Map", path.stem)

    def test_flat_map_output_behavior_uses_flat_target_dimensions(self):
        config = h.default_config()
        config.map_view = "flat"
        config.image_format = "png"
        config.max_safe_png_pixels = 10_000
        info = h.parse_url(h.USER_URL)
        start = h.datetime.strptime(info.timestamp, "%Y%m%d_%H%M")

        output = h.output_behavior_for_config(config, info, start)

        self.assertIn("flat target 2400x3018 px", output)
        self.assertIn(".tif", output)

    def test_timelapse_frame_size_guard_blocks_large_targets(self):
        config = h.default_config()
        config.mode = "Timelapse"
        area = mock.Mock(width=8000, height=6000)

        with self.assertRaisesRegex(RuntimeError, "Timelapse frame target"):
            h.validate_timelapse_frame_size(area, config)

    def test_flat_map_resampler_forces_nearest_even_when_config_native(self):
        config = h.default_config()
        config.map_view = "flat"
        config.resampler = "native"
        scene = mock.Mock()
        target = mock.Mock()
        expected = mock.Mock()
        scene.resample.return_value = expected

        result = h.resample_scene_low_ram(scene, target, config, datasets=("B13",))

        self.assertIs(result, expected)
        scene.resample.assert_called_once_with(
            target,
            datasets=("B13",),
            resampler="nearest",
            radius_of_influence=10000,
        )

    def test_preflight_blocks_invalid_url(self):
        config = h.default_config()
        config.user_url = "bad"

        result = h.preflight_run(config)

        self.assertFalse(result.ok)
        self.assertTrue(any("URL not recognised" in error for error in result.errors))

    def test_preflight_blocks_missing_overlay_data_when_enabled(self):
        config = h.default_config()
        config.add_border_lines = True

        with tempfile.TemporaryDirectory() as tmp_dir:
            with mock.patch.object(h, "PROJECT_DIR", h.Path(tmp_dir)):
                result = h.preflight_run(config, h.Path("C:/Himawari/outputs"), h.Path("C:/Himawari/temp"))

        self.assertFalse(result.ok)
        self.assertTrue(any("GSHHS_l_L1.shp" in error for error in result.errors))

    def test_preflight_does_not_require_overlay_data_when_disabled(self):
        config = h.default_config()
        config.add_border_lines = False

        result = h.preflight_run(config, h.Path("C:/Himawari/outputs"), h.Path("C:/Himawari/temp"))

        self.assertTrue(all("GSHHS_l_L1" not in error for error in result.errors))

    def test_validate_overlay_ready_blocks_missing_data_when_enabled(self):
        config = h.default_config()
        config.add_border_lines = True

        with tempfile.TemporaryDirectory() as tmp_dir:
            with mock.patch.object(h, "PROJECT_DIR", h.Path(tmp_dir)):
                with self.assertRaisesRegex(RuntimeError, "overlay setup is incomplete"):
                    h.validate_overlay_ready_for_run(config, h.Path(tmp_dir))

    def test_validate_overlay_ready_ignores_missing_data_when_disabled(self):
        config = h.default_config()
        config.add_border_lines = False

        with tempfile.TemporaryDirectory() as tmp_dir:
            h.validate_overlay_ready_for_run(config, h.Path(tmp_dir))

    def test_balanced_single_preset_allows_missing_overlay_data_from_enabled_base(self):
        base = h.default_config()
        base.add_border_lines = True
        base.border_line_color = "#123456"
        base.border_line_width = 2.5

        config = h.preset_config("Balanced Single", base)
        result = h.preflight_run(config, h.Path("C:/Himawari/outputs"), h.Path("C:/Himawari/temp"))

        self.assertFalse(config.add_border_lines)
        self.assertEqual(config.border_line_color, "#123456")
        self.assertEqual(config.border_line_width, 2.5)
        self.assertTrue(all("GSHHS_l_L1" not in error for error in result.errors))
        self.assertTrue(all("Border lines require" not in warning for warning in result.warnings))

    def test_preset_config_values_are_safe(self):
        base = h.default_config()
        base.add_border_lines = True
        base.border_line_color = "#abcdef"
        base.border_line_width = 3.0
        base.map_view = "flat"
        base.flat_min_lat = -45.0
        base.flat_max_lat = 45.0
        base.flat_min_lon = 100.0
        base.flat_max_lon = 180.0
        base.flat_resolution_deg = 0.1

        balanced = h.preset_config("Balanced Single", base)
        fast = h.preset_config("Fast IR Check", base)
        timelapse = h.preset_config("Low-RAM Timelapse", base)

        self.assertFalse(balanced.add_border_lines)
        self.assertFalse(fast.add_border_lines)
        self.assertFalse(timelapse.add_border_lines)
        self.assertEqual(balanced.border_line_color, "#abcdef")
        self.assertEqual(fast.border_line_width, 3.0)
        self.assertEqual(fast.composite_choice, "B13 (Infrared Window)")
        self.assertEqual(balanced.download_workers, 2)
        self.assertEqual(balanced.dask_chunk_size, "32MiB")
        self.assertEqual(fast.download_workers, 1)
        self.assertEqual(fast.dask_chunk_size, "32MiB")
        self.assertEqual(timelapse.mode, "Timelapse")
        self.assertEqual(timelapse.download_workers, 1)
        self.assertEqual(timelapse.dask_num_workers, 1)
        self.assertEqual(timelapse.dask_chunk_size, "16MiB")
        self.assertEqual(timelapse.resampler, "native")
        self.assertEqual(balanced.map_view, "flat")
        self.assertEqual(fast.map_view, "flat")
        self.assertEqual(timelapse.map_view, "flat")
        self.assertEqual(balanced.flat_min_lat, -45.0)
        self.assertEqual(timelapse.flat_resolution_deg, 0.1)

    def test_gui_settings_round_trip_ignores_unknown_fields(self):
        config = h.default_config()
        config.composite_choice = "B13 (Infrared Window)"
        config.map_view = "flat"
        config.crosshair_type = "plus"
        config.crosshair_color = "#12abef"
        config.flat_min_lat = -45.0
        config.flat_max_lat = 45.0
        config.flat_min_lon = 90.0
        config.flat_max_lon = 180.0
        config.flat_resolution_deg = 0.1
        with tempfile.TemporaryDirectory() as tmp_dir:
            settings_path = h.Path(tmp_dir) / "settings.json"
            data = h.serialize_gui_settings(config, h.Path("C:/out"), h.Path("C:/tmp"))
            data["config"]["unknown"] = "ignored"
            settings_path.write_text(h.json.dumps(data), encoding="utf-8")

            loaded = h.load_gui_settings(settings_path)

        self.assertIsNotNone(loaded)
        loaded_config, output_dir, temp_dir = loaded
        self.assertEqual(loaded_config.composite_choice, "B13 (Infrared Window)")
        self.assertEqual(loaded_config.map_view, "flat")
        self.assertEqual(loaded_config.crosshair_type, "plus")
        self.assertEqual(loaded_config.crosshair_color, "#12abef")
        self.assertEqual(loaded_config.flat_min_lat, -45.0)
        self.assertEqual(loaded_config.flat_max_lat, 45.0)
        self.assertEqual(loaded_config.flat_min_lon, 90.0)
        self.assertEqual(loaded_config.flat_max_lon, 180.0)
        self.assertEqual(loaded_config.flat_resolution_deg, 0.1)
        self.assertEqual(output_dir, h.Path("C:/out").resolve())
        self.assertEqual(temp_dir, h.Path("C:/tmp").resolve())

    def test_gui_settings_old_schema_loads_flat_defaults(self):
        config = h.default_config()
        data = h.serialize_gui_settings(config, h.Path("C:/out"), h.Path("C:/tmp"))
        for key in (
            "map_view",
            "flat_min_lat",
            "flat_max_lat",
            "flat_min_lon",
            "flat_max_lon",
            "flat_resolution_deg",
            "crosshair_type",
            "crosshair_color",
        ):
            data["config"].pop(key)

        with tempfile.TemporaryDirectory() as tmp_dir:
            settings_path = h.Path(tmp_dir) / "settings.json"
            settings_path.write_text(h.json.dumps(data), encoding="utf-8")

            loaded = h.load_gui_settings(settings_path)

        self.assertIsNotNone(loaded)
        loaded_config = loaded[0]
        self.assertEqual(loaded_config.map_view, h.MAP_VIEW)
        self.assertEqual(loaded_config.flat_min_lat, h.FLAT_MIN_LAT)
        self.assertEqual(loaded_config.flat_max_lat, h.FLAT_MAX_LAT)
        self.assertEqual(loaded_config.flat_min_lon, h.FLAT_MIN_LON)
        self.assertEqual(loaded_config.flat_max_lon, h.FLAT_MAX_LON)
        self.assertEqual(loaded_config.flat_resolution_deg, h.FLAT_RESOLUTION_DEG)
        self.assertEqual(loaded_config.crosshair_type, h.CROSSHAIR_TYPE)
        self.assertEqual(loaded_config.crosshair_color, h.CROSSHAIR_COLOR)

    def test_gui_settings_corrupt_file_returns_none(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            settings_path = h.Path(tmp_dir) / "settings.json"
            settings_path.write_text("{bad", encoding="utf-8")

            self.assertIsNone(h.load_gui_settings(settings_path))

    def test_custom_presets_round_trip_and_protect_builtins(self):
        config = h.default_config()
        config.composite_choice = "B13 (Infrared Window)"
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = h.Path(tmp_dir) / "presets.json"

            presets = h.save_custom_preset("IR check", config, path)
            loaded = h.load_custom_presets(path)

            self.assertIn("IR check", presets)
            self.assertEqual(loaded["IR check"].composite_choice, "B13 (Infrared Window)")
            with self.assertRaisesRegex(ValueError, "Built-in"):
                h.save_custom_preset("Balanced Single", config, path)

    def test_custom_presets_corrupt_file_returns_empty(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = h.Path(tmp_dir) / "presets.json"
            path.write_text("{bad", encoding="utf-8")

            self.assertEqual(h.load_custom_presets(path), {})

    def test_recent_runs_round_trip_caps_and_formats(self):
        config = h.default_config()
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = h.Path(tmp_dir) / "recent.json"
            for idx in range(h.RECENT_RUN_LIMIT + 2):
                record = h.build_recent_run_record(
                    "complete",
                    config,
                    f"2026-05-26T00:{idx:02d}:00Z",
                    outputs=[h.Path(f"out_{idx}.png")],
                )
                h.append_recent_run(record, path)

            loaded = h.load_recent_runs(path)

        self.assertEqual(len(loaded), h.RECENT_RUN_LIMIT)
        self.assertIn("Status: complete", h.format_recent_run_summary(loaded[0]))
        self.assertEqual(h.config_from_recent_run(loaded[0]).composite_choice, config.composite_choice)

    def test_recent_runs_corrupt_file_returns_empty(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = h.Path(tmp_dir) / "recent.json"
            path.write_text("{bad", encoding="utf-8")

            self.assertEqual(h.load_recent_runs(path), [])

    def test_preview_metadata_handles_missing_unsupported_and_safe_image(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = h.Path(tmp_dir)
            missing = h.safe_preview_metadata(tmp_path / "missing.png")
            unsupported = tmp_path / "movie.mp4"
            unsupported.write_bytes(b"fake")
            oversized = tmp_path / "big.png"
            oversized.write_bytes(b"fake")
            image_path = tmp_path / "small.png"

            from PIL import Image

            Image.new("RGB", (8, 6), color=(1, 2, 3)).save(image_path)

            self.assertFalse(missing.exists)
            self.assertFalse(h.safe_preview_metadata(unsupported).supported_preview)
            self.assertFalse(h.safe_preview_metadata(oversized, max_bytes=1).supported_preview)
            safe = h.safe_preview_metadata(image_path)
            self.assertTrue(safe.supported_preview)
            self.assertEqual(safe.dimensions, (8, 6))

    def test_completion_and_cancel_messages_include_manifest_context(self):
        config = h.default_config()
        config.mode = "Timelapse"

        done = h.completion_message([h.Path("movie.gif")], config, h.Path("outputs"))
        canceled = h.cancel_resume_message(config, [h.Path("frame_0000.png")], h.Path("outputs"))

        self.assertIn("movie.gif", done)
        self.assertIn("Manifest:", done)
        self.assertIn("Retrying", canceled)

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

    def test_native_common_area_locks_full_disk_from_reference_band_not_compatibility_extents(self):
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
        compatibility_areas = {"frame:B01": b01, "frame:B02": b01, "frame:B03": b03, "frame:B04": b01, "frame:B13": b13}

        target = h.native_compatible_common_area(
            [b03],
            500,
            source_pixel_size_m=500,
            compatibility_areas=compatibility_areas,
        )

        self.assertEqual((target.width, target.height), (22000, 22000))
        self.assertIsNone(h.native_area_compatibility_error(compatibility_areas, target))

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
            499,
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

    def test_zoom_earth_style_respects_selected_border_options_for_flat_map(self):
        config = h.default_config()
        config.map_view = "flat"
        config.add_border_lines = True
        config.border_line_color = "green"
        config.border_line_width = 4.0
        config.zoom_earth_style = True

        overlay = h.build_overlay_options(config)

        self.assertEqual(overlay["color"], (0, 255, 0))
        self.assertEqual(overlay["width"], 4.0)

    def test_new_map_overlay_defaults_are_off_and_old_settings_load(self):
        config = h.default_config()
        self.assertFalse(config.add_map_labels)
        self.assertFalse(config.add_night_boundary)
        self.assertFalse(config.add_crosshair)
        self.assertEqual(config.crosshair_type, h.CROSSHAIR_TYPE)
        self.assertEqual(config.crosshair_color, h.CROSSHAIR_COLOR)
        self.assertFalse(config.zoom_earth_style)

        with tempfile.TemporaryDirectory() as tmp_dir:
            settings_path = h.Path(tmp_dir) / "settings.json"
            data = h.serialize_gui_settings(h.default_config(), h.Path(tmp_dir) / "out", h.Path(tmp_dir) / "temp")
            for key in (
                "add_map_labels",
                "add_night_boundary",
                "add_crosshair",
                "crosshair_type",
                "crosshair_color",
                "zoom_earth_style",
            ):
                data["config"].pop(key, None)
            h.write_json_file(settings_path, data)

            loaded = h.load_gui_settings(settings_path)

        self.assertIsNotNone(loaded)
        loaded_config = loaded[0]
        self.assertFalse(loaded_config.add_map_labels)
        self.assertFalse(loaded_config.add_night_boundary)
        self.assertFalse(loaded_config.add_crosshair)
        self.assertEqual(loaded_config.crosshair_type, h.CROSSHAIR_TYPE)
        self.assertEqual(loaded_config.crosshair_color, h.CROSSHAIR_COLOR)
        self.assertFalse(loaded_config.zoom_earth_style)

    def test_zoom_earth_labels_include_new_zealand(self):
        labels = {label.replace("\n", " ") for label, _lat, _lon, _kind in h.ZOOM_EARTH_LABEL_POINTS}

        self.assertIn("NEW ZEALAND", labels)

    def test_flat_map_visual_style_requires_flat_map(self):
        config = h.default_config()
        config.zoom_earth_style = True

        errors = h.setup_configuration_errors(config)

        self.assertTrue(any("require flat map output" in error for error in errors))

    def test_apply_flat_map_visual_overlays_draws_supported_png_layers(self):
        from PIL import Image

        with tempfile.TemporaryDirectory() as tmp_dir:
            path = h.Path(tmp_dir) / "styled.png"
            Image.new("RGB", (120, 80), (80, 90, 100)).save(path)
            area = AreaDefinition(
                "flat",
                "flat",
                "flat",
                h.WEB_MERCATOR_PROJ4,
                120,
                80,
                h.web_mercator_extent(-60, 60, 80, 200),
            )
            config = h.default_config()
            config.map_view = "flat"
            config.zoom_earth_style = True
            config.add_map_labels = True
            config.add_night_boundary = True
            config.add_crosshair = True

            with Image.open(path) as image:
                before = image.copy()
            result = h.apply_flat_map_visual_overlays(
                path,
                area,
                config,
                h.datetime(2024, 7, 25, 4, 0),
                "True Color Reproduction Image",
            )
            with Image.open(path) as image:
                after = image.copy()

        self.assertEqual(result, path)
        self.assertNotEqual(before.tobytes(), after.tobytes())

    def test_zoom_earth_enhancement_brightens_and_saturates_true_color(self):
        from PIL import Image

        pixels = h.np.zeros((24, 24, 3), dtype=h.np.uint8)
        pixels[:, :] = (55, 75, 95)
        pixels[6:18, 6:18] = (120, 90, 55)
        before = Image.fromarray(pixels, mode="RGB")
        after = h.apply_zoom_earth_true_color_enhancement(before).convert("RGB")

        def luminance_and_saturation(image):
            values = h.np.asarray(image, dtype=h.np.float32) / 255.0
            luminance = (0.2126 * values[:, :, 0] + 0.7152 * values[:, :, 1] + 0.0722 * values[:, :, 2]).mean()
            maximum = values.max(axis=2)
            minimum = values.min(axis=2)
            saturation = h.np.where(maximum > 0.0, (maximum - minimum) / maximum, 0.0).mean()
            return float(luminance), float(saturation)

        before_luminance, before_saturation = luminance_and_saturation(before)
        after_luminance, after_saturation = luminance_and_saturation(after)

        self.assertGreater(after_luminance, before_luminance + 0.05)
        self.assertGreater(after_saturation, before_saturation + 0.03)

    def test_flat_map_validity_mask_rejects_zero_source_fill(self):
        area = AreaDefinition("flat", "flat", "flat", h.WEB_MERCATOR_PROJ4, 3, 2, h.web_mercator_extent(-10, 10, 100, 103))
        attrs = {"area": area}
        scene = {
            "B01": xr.DataArray(
                da.from_array([[1.0, 0.0, h.np.nan], [0.0, 2.0, 0.0]], chunks=(2, 3)),
                dims=("y", "x"),
                attrs=attrs,
            ),
            "B13": xr.DataArray(
                da.from_array([[0.0, 0.0, 0.0], [250.0, 0.0, 0.0]], chunks=(2, 3)),
                dims=("y", "x"),
                attrs=attrs,
            ),
        }

        mask = h.flat_map_validity_mask_from_scene(scene, ("B01", "B13"), area)

        expected = h.np.asarray([[True, False, False], [True, True, False]], dtype=bool)
        self.assertTrue(h.np.array_equal(mask, expected))

    def test_flat_map_visual_overlays_replace_edge_limb_with_rectangular_basemap(self):
        from PIL import Image

        with tempfile.TemporaryDirectory() as tmp_dir:
            path = h.Path(tmp_dir) / "limb.png"
            pixels = h.np.zeros((20, 20, 3), dtype=h.np.uint8)
            pixels[:, :] = (90, 105, 120)
            pixels[:, -4:] = (0, 0, 0)
            pixels[8:12, 8:12] = (0, 0, 0)
            Image.fromarray(pixels, mode="RGB").save(path)
            area = AreaDefinition("flat", "flat", "flat", h.WEB_MERCATOR_PROJ4, 20, 20, h.web_mercator_extent(-10, 10, 100, 120))
            config = h.default_config()
            config.map_view = "flat"
            config.zoom_earth_style = True

            h.apply_flat_map_visual_overlays(
                path,
                area,
                config,
                h.datetime(2024, 7, 25, 4, 0),
                "True Color Reproduction Image",
            )
            with Image.open(path) as image:
                styled = h.np.asarray(image.convert("RGB"))
            basemap = h.np.asarray(h.build_flat_map_basemap_image(area).convert("RGB"))

        self.assertTrue(h.np.array_equal(styled[:, -4:, :], basemap[:, -4:, :]))
        self.assertFalse(h.np.array_equal(styled[8:12, 8:12, :], basemap[8:12, 8:12, :]))

    def test_flat_map_visual_overlays_replace_invalid_true_color_pixels_with_basemap(self):
        from PIL import Image

        with tempfile.TemporaryDirectory() as tmp_dir:
            path = h.Path(tmp_dir) / "styled.png"
            Image.new("RGB", (12, 8), (120, 130, 140)).save(path)
            area = AreaDefinition("flat", "flat", "flat", h.WEB_MERCATOR_PROJ4, 12, 8, h.web_mercator_extent(-10, 10, 100, 112))
            config = h.default_config()
            config.map_view = "flat"
            config.zoom_earth_style = True
            mask = h.np.ones((8, 12), dtype=bool)
            mask[:, -3:] = False
            h.apply_flat_map_visual_overlays(
                path,
                area,
                config,
                h.datetime(2024, 7, 25, 4, 0),
                "True Color Reproduction Image",
                valid_mask=mask,
            )
            with Image.open(path) as image:
                pixels = h.np.asarray(image.convert("RGB"))
            basemap = h.np.asarray(h.build_flat_map_basemap_image(area).convert("RGB"))

        self.assertTrue(h.np.array_equal(pixels[:, -3:, :], basemap[:, -3:, :]))
        self.assertFalse(h.np.array_equal(pixels[:, :-3, :], basemap[:, :-3, :]))

    def test_generated_flat_map_basemap_is_rectangular_and_varied(self):
        area = AreaDefinition("flat", "flat", "flat", h.WEB_MERCATOR_PROJ4, 120, 80, h.web_mercator_extent(-60, 60, 80, 200))

        basemap = h.np.asarray(h.build_flat_map_basemap_image(area).convert("RGB"))

        self.assertEqual(basemap.shape, (80, 120, 3))
        self.assertGreater(int(h.np.unique(basemap.reshape(-1, 3), axis=0).shape[0]), 8)
        self.assertFalse(h.np.all(basemap == h.np.asarray(h.FLAT_MAP_INVALID_FILL, dtype=h.np.uint8)))

    def test_flat_map_visual_overlays_fill_invalid_single_band_pixels_with_dark_ocean(self):
        from PIL import Image

        with tempfile.TemporaryDirectory() as tmp_dir:
            path = h.Path(tmp_dir) / "single.png"
            Image.new("RGB", (12, 8), (120, 130, 140)).save(path)
            area = AreaDefinition("flat", "flat", "flat", h.WEB_MERCATOR_PROJ4, 12, 8, h.web_mercator_extent(-10, 10, 100, 112))
            config = h.default_config()
            config.map_view = "flat"
            config.zoom_earth_style = True
            mask = h.np.ones((8, 12), dtype=bool)
            mask[:, -3:] = False
            h.apply_flat_map_visual_overlays(
                path,
                area,
                config,
                h.datetime(2024, 7, 25, 4, 0),
                "B13 (Infrared Window)",
                valid_mask=mask,
            )
            with Image.open(path) as image:
                pixels = h.np.asarray(image.convert("RGB"))

        self.assertTrue(h.np.all(pixels[:, -3:, :] == h.np.asarray(h.FLAT_MAP_INVALID_FILL, dtype=h.np.uint8)))

    def test_flat_map_visual_overlays_use_png_alpha_as_invalid_mask(self):
        from PIL import Image

        with tempfile.TemporaryDirectory() as tmp_dir:
            path = h.Path(tmp_dir) / "alpha.png"
            pixels = h.np.zeros((8, 12, 4), dtype=h.np.uint8)
            pixels[:, :, :3] = (120, 130, 140)
            pixels[:, :, 3] = 255
            pixels[:, -3:, 3] = 0
            Image.fromarray(pixels, mode="RGBA").save(path)
            area = AreaDefinition("flat", "flat", "flat", h.WEB_MERCATOR_PROJ4, 12, 8, h.web_mercator_extent(-10, 10, 100, 112))
            config = h.default_config()
            config.map_view = "flat"
            config.zoom_earth_style = True

            h.apply_flat_map_visual_overlays(
                path,
                area,
                config,
                h.datetime(2024, 7, 25, 4, 0),
                "True Color Reproduction Image",
            )
            with Image.open(path) as image:
                styled = h.np.asarray(image.convert("RGB"))
            basemap = h.np.asarray(h.build_flat_map_basemap_image(area).convert("RGB"))

        self.assertTrue(h.np.array_equal(styled[:, -3:, :], basemap[:, -3:, :]))

    def test_flat_map_visual_geotiff_preserves_profile_and_styles_rgb(self):
        import rasterio

        with tempfile.TemporaryDirectory() as tmp_dir:
            path = h.Path(tmp_dir) / "styled.tif"
            area = AreaDefinition("flat", "flat", "flat", h.WEB_MERCATOR_PROJ4, 12, 8, h.web_mercator_extent(-10, 10, 100, 112))
            rgb = xr.DataArray(
                da.from_array(
                    [
                        [[80] * 12 for _ in range(8)],
                        [[90] * 12 for _ in range(8)],
                        [[100] * 12 for _ in range(8)],
                    ],
                    chunks=(1, 4, 6),
                ),
                dims=("bands", "y", "x"),
                coords={"bands": ["R", "G", "B"]},
                attrs={"area": area, "mode": "RGB"},
            ).astype(h.np.uint8)
            h.write_rgb_geotiff_low_ram(rgb, path, area)
            with rasterio.open(path) as src:
                original_transform = src.transform
                original_crs = src.crs
            config = h.default_config()
            config.map_view = "flat"
            config.zoom_earth_style = True
            config.add_night_boundary = True
            mask = h.np.ones((8, 12), dtype=bool)
            mask[-2:, -2:] = False

            result = h.apply_flat_map_visual_overlays(
                path,
                area,
                config,
                h.datetime(2024, 7, 25, 4, 0),
                "True Color Reproduction Image",
                valid_mask=mask,
            )
            with rasterio.open(path) as src:
                styled = src.read((1, 2, 3))
                colorinterp = tuple(src.colorinterp)
                styled_width = src.width
                styled_height = src.height
                styled_count = src.count
                styled_crs = src.crs
                styled_transform = src.transform
            basemap = h.np.asarray(h.build_flat_map_basemap_image(area).convert("RGB"))

        self.assertEqual(result, path)
        self.assertEqual(styled_width, 12)
        self.assertEqual(styled_height, 8)
        self.assertEqual(styled_count, 3)
        self.assertEqual(styled_crs, original_crs)
        self.assertEqual(styled_transform, original_transform)
        self.assertEqual(colorinterp[0], rasterio.enums.ColorInterp.red)
        self.assertTrue(h.np.array_equal(h.np.moveaxis(styled, 0, -1)[-2:, -2:, :], basemap[-2:, -2:, :]))

    def test_flat_map_visual_geotiff_uses_alpha_band_as_invalid_mask(self):
        import rasterio
        from rasterio.transform import from_origin

        with tempfile.TemporaryDirectory() as tmp_dir:
            path = h.Path(tmp_dir) / "alpha.tif"
            area = AreaDefinition("flat", "flat", "flat", h.WEB_MERCATOR_PROJ4, 12, 8, h.web_mercator_extent(-10, 10, 100, 112))
            profile = {
                "driver": "GTiff",
                "width": 12,
                "height": 8,
                "count": 4,
                "dtype": "uint8",
                "crs": area.crs,
                "transform": from_origin(0, 0, 1, 1),
                "photometric": "RGB",
            }
            data = h.np.zeros((4, 8, 12), dtype=h.np.uint8)
            data[0:3, :, :] = h.np.asarray([120, 130, 140], dtype=h.np.uint8)[:, None, None]
            data[3, :, :] = 255
            data[3, :, -3:] = 0
            with rasterio.open(path, "w", **profile) as dst:
                dst.write(data)
                dst.colorinterp = (
                    rasterio.enums.ColorInterp.red,
                    rasterio.enums.ColorInterp.green,
                    rasterio.enums.ColorInterp.blue,
                    rasterio.enums.ColorInterp.alpha,
                )
            config = h.default_config()
            config.map_view = "flat"
            config.zoom_earth_style = True

            h.apply_flat_map_visual_overlays(
                path,
                area,
                config,
                h.datetime(2024, 7, 25, 4, 0),
                "True Color Reproduction Image",
            )
            with rasterio.open(path) as src:
                styled = h.np.moveaxis(src.read((1, 2, 3)), 0, -1)
            basemap = h.np.asarray(h.build_flat_map_basemap_image(area).convert("RGB"))

        self.assertTrue(h.np.array_equal(styled[:, -3:, :], basemap[:, -3:, :]))

    def test_flat_map_visual_style_draws_selected_border_color_after_enhancement(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = h.Path(tmp_dir) / "border.png"
            from PIL import Image

            Image.new("RGB", (20, 10), (90, 95, 100)).save(path)
            area = AreaDefinition("flat", "flat", "flat", h.WEB_MERCATOR_PROJ4, 20, 10, h.web_mercator_extent(-10, 10, 100, 120))
            config = h.default_config()
            config.map_view = "flat"
            config.zoom_earth_style = True
            config.add_border_lines = True
            config.border_line_color = "green"
            overlay = h.build_overlay_options(config)

            with mock.patch("himawari_lowram_processor.direct_overlay_to_image") as mock_overlay:
                h.apply_flat_map_visual_overlays(
                    path,
                    area,
                    config,
                    h.datetime(2024, 7, 25, 4, 0),
                    "True Color Reproduction Image",
                    overlay_options=overlay,
                )

        mock_overlay.assert_called_once()
        self.assertEqual(mock_overlay.call_args.args[2]["color"], (0, 255, 0))

    def test_flat_map_visual_style_draws_configured_crosshair(self):
        from PIL import Image

        for crosshair_type in h.CROSSHAIR_TYPES:
            with self.subTest(crosshair_type=crosshair_type):
                with tempfile.TemporaryDirectory() as tmp_dir:
                    path = h.Path(tmp_dir) / "crosshair.png"
                    Image.new("RGB", (80, 80), (20, 25, 30)).save(path)
                    area = AreaDefinition("flat", "flat", "flat", h.WEB_MERCATOR_PROJ4, 80, 80, h.web_mercator_extent(-10, 10, 100, 120))
                    config = h.default_config()
                    config.map_view = "flat"
                    config.add_crosshair = True
                    config.crosshair_type = crosshair_type
                    config.crosshair_color = "#ff0000"

                    h.apply_flat_map_visual_overlays(
                        path,
                        area,
                        config,
                        h.datetime(2024, 7, 25, 4, 0),
                        "B13 (Infrared Window)",
                    )
                    with Image.open(path) as image:
                        pixels = h.np.asarray(image.convert("RGB"))

                red_pixels = (pixels[:, :, 0] > 180) & (pixels[:, :, 1] < 90) & (pixels[:, :, 2] < 90)
                self.assertGreater(int(red_pixels.sum()), 0)

    def test_night_boundary_draws_visible_high_contrast_pixels(self):
        from PIL import Image

        area = AreaDefinition("flat", "flat", "flat", h.WEB_MERCATOR_PROJ4, 240, 120, h.web_mercator_extent(-60, 60, 80, 200))
        image = Image.new("RGBA", (240, 120), (120, 120, 120, 255))

        h.draw_night_boundary(image, area, h.datetime(2024, 7, 25, 4, 0))
        pixels = h.np.asarray(image.convert("RGB"))

        bright_pixels = (pixels[:, :, 0] > 220) & (pixels[:, :, 1] > 220) & (pixels[:, :, 2] > 220)
        dark_pixels = (pixels[:, :, 0] < 50) & (pixels[:, :, 1] < 50) & (pixels[:, :, 2] < 50)
        self.assertGreater(int(bright_pixels.sum()), 0)
        self.assertGreater(int(dark_pixels.sum()), 0)

    def test_visible_polyline_segments_split_projection_jumps(self):
        segments = h.visible_polyline_segments(
            [(0.0, 0.0), (1.0, 0.0), (200.0, 0.0), (201.0, 0.0), None, (2.0, 2.0), (3.0, 2.0)],
            max_jump_px=10.0,
        )

        self.assertEqual(len(segments), 3)
        self.assertEqual(segments[0], [(0.0, 0.0), (1.0, 0.0)])
        self.assertEqual(segments[1], [(200.0, 0.0), (201.0, 0.0)])

    def test_zoom_earth_enhancement_does_not_run_for_single_band_product(self):
        from PIL import Image

        with tempfile.TemporaryDirectory() as tmp_dir:
            path = h.Path(tmp_dir) / "single.png"
            Image.new("RGB", (20, 20), (80, 90, 100)).save(path)
            area = AreaDefinition("flat", "flat", "flat", h.WEB_MERCATOR_PROJ4, 20, 20, (0, 0, 20, 20))
            config = h.default_config()
            config.map_view = "flat"
            config.zoom_earth_style = True

            with Image.open(path) as image:
                before = image.copy()
            h.apply_flat_map_visual_overlays(path, area, config, h.datetime(2024, 7, 25, 4, 0), "B13 (Infrared Window)")
            with Image.open(path) as image:
                after = image.copy()

        self.assertEqual(before.tobytes(), after.tobytes())

    def test_large_png_switches_to_geotiff(self):
        area = mock.Mock(width=22000, height=22000)
        config = h.default_config()
        path = h.Path("big.png")
        self.assertEqual(h.enforce_safe_output_format(path, area, config), h.Path("big.tif"))

        small_area = mock.Mock(width=100, height=100)
        self.assertEqual(h.enforce_safe_output_format(path, small_area, config), path)

    def test_save_retries_without_failed_overlay(self):
        scene = mock.Mock()
        def save_dataset(_dataset_name, **kwargs):
            if "overlay" in kwargs:
                raise ModuleNotFoundError("No module named 'pycoast'")
            h.Path(kwargs["filename"]).write_bytes(b"saved")

        scene.save_dataset.side_effect = save_dataset
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
        def save_dataset(_dataset_name, **kwargs):
            if kwargs["writer"] == "simple_image":
                raise ValueError("cannot write empty image")
            h.Path(kwargs["filename"]).write_bytes(b"saved")

        scene.save_dataset.side_effect = save_dataset

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

    def test_rgb_geotiff_validation_rejects_constant_near_black(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = h.Path(tmp_dir) / "black.tif"
            area = AreaDefinition(
                "test",
                "test",
                "latlon",
                {"proj": "longlat", "datum": "WGS84"},
                4,
                4,
                (100.0, -10.0, 104.0, -6.0),
            )
            data = xr.DataArray(
                da.ones((3, 4, 4), chunks=(1, 2, 2)),
                dims=("bands", "y", "x"),
                coords={"bands": ["R", "G", "B"]},
                attrs={"area": area, "mode": "RGB"},
            ).astype(h.np.uint8)

            h.write_rgb_geotiff_low_ram(data, path, area)
            degenerate, reason = h.rgb_geotiff_is_degenerate(path)

        self.assertTrue(degenerate)
        self.assertIn("near black", reason)

    def test_rgb_geotiff_validation_accepts_visible_data(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = h.Path(tmp_dir) / "visible.tif"
            area = AreaDefinition(
                "test",
                "test",
                "latlon",
                {"proj": "longlat", "datum": "WGS84"},
                4,
                4,
                (100.0, -10.0, 104.0, -6.0),
            )
            base = da.from_array(
                [
                    [[0, 32, 64, 96], [16, 48, 80, 112], [32, 64, 96, 128], [48, 80, 112, 144]],
                    [[20, 52, 84, 116], [36, 68, 100, 132], [52, 84, 116, 148], [68, 100, 132, 164]],
                    [[40, 72, 104, 136], [56, 88, 120, 152], [72, 104, 136, 168], [88, 120, 152, 184]],
                ],
                chunks=(1, 2, 2),
            )
            data = xr.DataArray(
                base,
                dims=("bands", "y", "x"),
                coords={"bands": ["R", "G", "B"]},
                attrs={"area": area, "mode": "RGB"},
            ).astype(h.np.uint8)

            h.write_rgb_geotiff_low_ram(data, path, area)
            degenerate, reason = h.rgb_geotiff_is_degenerate(path)

        self.assertFalse(degenerate)
        self.assertIn("visible", reason)

    def test_direct_rgb_geotiff_writer_preserves_lazy_rgb_stats(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = h.Path(tmp_dir) / "b11.tif"
            area = AreaDefinition(
                "test",
                "test",
                "latlon",
                {"proj": "longlat", "datum": "WGS84"},
                4,
                4,
                (100.0, -10.0, 104.0, -6.0),
            )
            source = xr.DataArray(
                da.from_array(
                    [[190.0, 220.0, 250.0, 280.0], [200.0, 230.0, 260.0, 290.0], [210.0, 240.0, 270.0, 300.0], [215.0, 245.0, 275.0, 305.0]],
                    chunks=(2, 2),
                ),
                dims=("y", "x"),
                attrs={"area": area, "sensor": "ahi", "calibration": "brightness_temperature"},
            )
            rgb = h.single_band_to_rgb(source, "B11", "custom_b11_rgb")

            h.write_rgb_geotiff_low_ram(rgb, path, area)
            degenerate, _reason = h.rgb_geotiff_is_degenerate(path)

        self.assertFalse(degenerate)

    def test_direct_rgb_png_writer_writes_lazy_rgb_without_satpy(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = h.Path(tmp_dir) / "rgb.png"
            rgb = xr.DataArray(
                da.from_array(
                    [
                        [[0, 64], [128, 255]],
                        [[10, 74], [138, 245]],
                        [[20, 84], [148, 235]],
                    ],
                    chunks=(1, 2, 2),
                ),
                dims=("bands", "y", "x"),
                coords={"bands": ["R", "G", "B"]},
            ).astype(h.np.uint8)

            h.write_rgb_png_low_ram(rgb, path)

            from PIL import Image

            with Image.open(path) as image:
                self.assertEqual(image.mode, "RGB")
                self.assertEqual(image.size, (2, 2))
                self.assertEqual(image.getpixel((1, 0)), (64, 74, 84))

    @mock.patch("himawari_lowram_processor.direct_overlay_writer")
    def test_direct_png_overlay_draws_coastlines_and_borders(self, mock_writer_class):
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = h.Path(tmp_dir) / "rgb.png"
            area = AreaDefinition(
                "test",
                "test",
                "latlon",
                {"proj": "longlat", "datum": "WGS84"},
                2,
                2,
                (100.0, -10.0, 102.0, -8.0),
            )
            from PIL import Image

            Image.new("RGB", (2, 2), (10, 20, 30)).save(path)
            writer = mock.Mock()
            mock_writer_class.return_value = writer

            result = h.apply_direct_overlay_to_image_file(
                path,
                area,
                {
                    "coast_dir": str(h.Path(tmp_dir) / "overlays"),
                    "color": (0, 255, 0),
                    "width": 1.0,
                    "level_coast": 1,
                    "level_borders": 1,
                    "resolution": "l",
                },
            )

        self.assertEqual(result, path)
        writer.add_coastlines.assert_called_once()
        writer.add_borders.assert_called_once()

    def test_direct_rgb_writer_rejects_non_rgb_dataarray(self):
        source = xr.DataArray(da.ones((2, 2), chunks=(1, 1)), dims=("y", "x"))

        with self.assertRaisesRegex(ValueError, "Direct RGB output requires"):
            h.prepared_rgb_dask_array(source)

    def test_direct_rgb_geotiff_rejects_area_dimension_mismatch(self):
        area = AreaDefinition(
            "test",
            "test",
            "latlon",
            {"proj": "longlat", "datum": "WGS84"},
            3,
            2,
            (100.0, -10.0, 103.0, -8.0),
        )
        rgb = xr.DataArray(
            da.ones((3, 2, 2), chunks=(1, 2, 2)).astype(h.np.uint8),
            dims=("bands", "y", "x"),
            coords={"bands": ["R", "G", "B"]},
        )

        with tempfile.TemporaryDirectory() as tmp_dir:
            with self.assertRaisesRegex(ValueError, "dimensions do not match"):
                h.write_rgb_geotiff_low_ram(rgb, h.Path(tmp_dir) / "rgb.tif", area)

    @mock.patch("himawari_lowram_processor.apply_flat_map_visual_overlays")
    @mock.patch("himawari_lowram_processor.save_dataset_with_optional_overlay")
    def test_satpy_flat_true_color_passes_validity_mask_to_styling(self, mock_save, mock_style):
        area = AreaDefinition(
            "flat",
            "flat",
            "latlon",
            {"proj": "longlat", "datum": "WGS84"},
            3,
            2,
            (100.0, -10.0, 103.0, -8.0),
        )
        output = h.Path("out.png")
        mock_save.return_value = output
        mock_style.return_value = output
        b01 = xr.DataArray(
            da.from_array([[1.0, 0.0, h.np.nan], [1.0, 2.0, 0.0]], chunks=(2, 3)),
            dims=("y", "x"),
            attrs={"area": area},
        )
        b02 = xr.DataArray(
            da.from_array([[h.np.nan, h.np.nan, h.np.nan], [1.0, h.np.nan, h.np.nan]], chunks=(2, 3)),
            dims=("y", "x"),
            attrs={"area": area},
        )
        dataset = xr.DataArray(
            da.ones((2, 3), chunks=(2, 3)),
            dims=("y", "x"),
            attrs={"area": area},
        )
        blank = xr.DataArray(
            da.from_array([[h.np.nan, h.np.nan, h.np.nan], [h.np.nan, h.np.nan, h.np.nan]], chunks=(2, 3)),
            dims=("y", "x"),
            attrs={"area": area},
        )
        resampled = {"true_color": dataset, "B01": b01, "B02": b02, "B03": blank, "B04": blank}
        config = h.default_config()
        config.map_view = "flat"
        config.zoom_earth_style = True
        config.use_night_fallback = False

        h.save_satpy_dataset_output(
            resampled,
            "True Color RGB (Enhanced)",
            "true_color",
            output,
            config,
            overlay_options=None,
            scan_time=h.datetime(2024, 7, 25, 4, 0),
        )

        valid_mask = mock_style.call_args.kwargs["valid_mask"]
        expected = h.np.asarray([[True, False, False], [True, True, False]], dtype=bool)
        self.assertTrue(h.np.array_equal(valid_mask, expected))

    @mock.patch("himawari_lowram_processor.save_dataset_with_optional_overlay")
    @mock.patch("himawari_lowram_processor.write_rgb_png_low_ram")
    @mock.patch("himawari_lowram_processor.resample_scene_low_ram")
    def test_custom_rgb_png_uses_direct_writer_not_satpy_png(self, mock_resample, mock_write_png, mock_save):
        area = AreaDefinition(
            "test",
            "test",
            "latlon",
            {"proj": "longlat", "datum": "WGS84"},
            2,
            2,
            (100.0, -10.0, 102.0, -8.0),
        )
        attrs = {"area": area, "sensor": "ahi"}
        bands = {
            band: xr.DataArray(da.ones((2, 2), chunks=(2, 2)) * 50, dims=("y", "x"), attrs=attrs)
            for band in ("B01", "B02", "B03", "B04")
        }
        scene = mock.MagicMock()
        scene.__getitem__.side_effect = lambda key: bands[key]
        resampled = mock.MagicMock()
        resampled.__getitem__.side_effect = lambda key: bands[key]
        mock_resample.return_value = resampled
        output = h.Path("out.png")
        mock_write_png.return_value = output
        config = h.default_config()
        config.gpu_acceleration = False
        config.use_night_fallback = False

        result = h.save_custom_composite_output(
            scene,
            "True Color Reproduction Image",
            area,
            output,
            config,
            is_night=False,
            overlay_options=None,
            scan_time=h.datetime(2024, 7, 25, 4, 0),
        )

        self.assertEqual(result, output)
        mock_write_png.assert_called_once()
        mock_save.assert_not_called()

    @mock.patch("himawari_lowram_processor.save_dataset_with_optional_overlay")
    @mock.patch("himawari_lowram_processor.write_rgb_png_low_ram")
    @mock.patch("himawari_lowram_processor.build_gpu_custom_composite")
    @mock.patch("himawari_lowram_processor.resample_scene_low_ram")
    def test_gpu_flat_true_color_png_uses_gpu_block_and_direct_writer(
        self,
        mock_resample,
        mock_build_gpu,
        mock_write_png,
        mock_save,
    ):
        area = AreaDefinition(
            "test",
            "test",
            "latlon",
            {"proj": "longlat", "datum": "WGS84"},
            2,
            2,
            (100.0, -10.0, 102.0, -8.0),
        )
        rgb = xr.DataArray(
            da.ones((3, 2, 2), chunks=(1, 2, 2)).astype(h.np.uint8),
            dims=("bands", "y", "x"),
            coords={"bands": ["R", "G", "B"]},
            attrs={"area": area, "mode": "RGB", "name": "gpu_rgb"},
        )
        scene = mock.MagicMock()
        resampled = mock.MagicMock()
        mock_resample.return_value = resampled
        mock_build_gpu.return_value = ("gpu_rgb", rgb)
        output = h.Path("out.png")
        mock_write_png.return_value = output
        config = h.default_config()
        config.map_view = "flat"
        config.gpu_acceleration = True
        config.use_night_fallback = False

        result = h.save_custom_composite_output(
            scene,
            "True Color Reproduction Image",
            area,
            output,
            config,
            is_night=False,
            overlay_options=None,
            scan_time=h.datetime(2024, 7, 25, 4, 0),
        )

        self.assertEqual(result, output)
        mock_build_gpu.assert_called_once_with(resampled, "True Color Reproduction Image", config)
        mock_write_png.assert_called_once_with(rgb, output)
        mock_save.assert_not_called()

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

        with self.assertRaisesRegex(RuntimeError, "Timelapse frame target"):
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

    def test_satpy_resample_datasets_for_composite(self):
        self.assertIsNone(
            h.satpy_resample_datasets_for_composite(
                "True Color Reproduction Image",
                "true_color_reproduction",
            )
        )
        self.assertEqual(
            h.satpy_resample_datasets_for_composite("True Color RGB (Enhanced)", "true_color"),
            ["true_color"],
        )

    def test_satpy_resample_datasets_adds_hybrid_inputs(self):
        config = h.default_config()
        config.composite_choice = "True Color Reproduction Image"
        config.use_night_fallback = True
        config.night_fallback_mode = "hybrid"

        datasets = h.satpy_resample_datasets("True Color Reproduction Image", "true_color_reproduction", config)

        self.assertEqual(datasets, ["true_color_reproduction", "B03", "B13"])

    @mock.patch("himawari_lowram_processor.cleanup_paths")
    @mock.patch("himawari_lowram_processor.write_rgb_png_low_ram")
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
        self.assertTrue(any("custom low-RAM fallback" in message for message, _current, _total in events))

    @mock.patch("himawari_lowram_processor.cleanup_paths")
    @mock.patch("himawari_lowram_processor.native_area_compatibility_error")
    @mock.patch("himawari_lowram_processor.write_rgb_png_low_ram")
    @mock.patch("himawari_lowram_processor.resample_scene_low_ram")
    @mock.patch("himawari_lowram_processor.download_segments")
    @mock.patch("himawari_lowram_processor.Scene")
    def test_flat_map_custom_fallback_skips_native_compatibility_check(
        self,
        mock_scene_class,
        mock_download,
        mock_resample,
        mock_save,
        mock_native_compatibility,
        _mock_cleanup,
    ):
        original_scene = mock.Mock()
        original_scene.load.side_effect = KeyError(
            "\"No dataset matching 'DataQuery(name='true_color_reproduction')' found\""
        )
        attrs = {"area": h.flat_map_area(h.default_config()), "sensor": "ahi"}
        bands = {
            band: xr.DataArray(da.ones((4, 4), chunks=(2, 2)) * 50, dims=("y", "x"), attrs=attrs)
            for band in ("B01", "B02", "B03", "B04")
        }
        fallback_scene = mock.MagicMock()
        fallback_scene.__getitem__.side_effect = lambda key: bands[key]
        fallback_scene.__setitem__.return_value = None
        fallback_scene.load.return_value = None
        mock_scene_class.side_effect = [original_scene, fallback_scene]
        mock_resample.return_value = fallback_scene
        mock_save.return_value = h.Path("out.png")
        mock_download.return_value = [h.Path(f"segment-{idx}.dat") for idx in range(40)]
        config = h.default_config()
        config.map_view = "flat"
        config.composite_choice = "True Color Reproduction Image"
        config.use_night_fallback = False
        info = h.parse_url(h.USER_URL)
        dt = h.datetime.strptime(info.timestamp, "%Y%m%d_%H%M")
        master_area = h.flat_map_area(config)

        result = h.process_frame(dt, info, master_area, 0, 1, config=config)

        self.assertEqual(result, h.Path("out.png"))
        mock_native_compatibility.assert_not_called()
        mock_resample.assert_called_once_with(
            fallback_scene,
            master_area,
            config,
            datasets=h.required_bands("True Color Reproduction Image", False, config.night_fallback_mode),
        )

    @mock.patch("himawari_lowram_processor.cleanup_paths")
    @mock.patch("himawari_lowram_processor.save_custom_satpy_missing_dataset_fallback")
    @mock.patch("himawari_lowram_processor.save_dataset_with_optional_overlay")
    @mock.patch("himawari_lowram_processor.resample_scene_low_ram")
    @mock.patch("himawari_lowram_processor.download_segments")
    @mock.patch("himawari_lowram_processor.Scene")
    def test_true_color_reproduction_resamples_without_dataset_filter(
        self,
        mock_scene_class,
        mock_download,
        mock_resample,
        mock_save,
        mock_fallback,
        _mock_cleanup,
    ):
        original_scene = mock.Mock()
        original_scene.load.return_value = None
        mock_scene_class.return_value = original_scene
        resampled_scene = mock.Mock()
        mock_resample.return_value = resampled_scene
        mock_save.return_value = h.Path("out.png")
        mock_download.return_value = [h.Path(f"segment-{idx}.dat") for idx in range(40)]
        config = h.default_config()
        config.composite_choice = "True Color Reproduction Image"
        config.use_night_fallback = False
        config.night_fallback_mode = "whole_frame_ir"
        info = h.parse_url(h.USER_URL)
        dt = h.datetime.strptime(info.timestamp, "%Y%m%d_%H%M")

        result = h.process_frame(dt, info, mock.Mock(width=10, height=10), 0, 1, config=config)

        self.assertEqual(result, h.Path("out.png"))
        mock_resample.assert_called_once_with(
            original_scene,
            mock.ANY,
            config,
            datasets=None,
        )
        mock_save.assert_called_once()
        self.assertIs(mock_save.call_args.args[0], resampled_scene)
        self.assertEqual(mock_save.call_args.args[1], "true_color_reproduction")
        mock_fallback.assert_not_called()

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

    def test_streaming_bz2_writes_concatenated_streams_without_buffering(self):
        decompressor = bz2.BZ2Decompressor()
        payload = bz2.compress(b"one") + bz2.compress(b"two")
        output = io.BytesIO()

        decompressor = h.write_decompressed_bz2_chunk(decompressor, payload, output)

        self.assertEqual(output.getvalue(), b"onetwo")
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

    def test_timelapse_manifest_tracks_frames_and_resume_paths(self):
        config = h.default_config()
        config.mode = "Timelapse"
        info = h.parse_url(h.USER_URL)
        steps = [h.datetime(2024, 7, 25, 3, 40), h.datetime(2024, 7, 25, 4, 0)]
        with tempfile.TemporaryDirectory() as tmp_dir:
            frame_dir = h.Path(tmp_dir) / "frames"
            run_id = h.stable_run_id(config, info, steps)
            manifest = h.build_timelapse_manifest(run_id, config, info, steps, frame_dir)
            frame_path = frame_dir / "frame_0000.png"
            write_test_png(frame_path)
            h.update_manifest_frame(manifest, 0, frame_path, "complete")

            self.assertEqual(h.resume_frame_path(manifest, 0), frame_path)
            self.assertIsNone(h.resume_frame_path(manifest, 1))

    def test_resume_frame_path_rejects_corrupt_nonempty_png(self):
        manifest = {"frames": [{"index": 0, "path": ""}]}
        with tempfile.TemporaryDirectory() as tmp_dir:
            frame_path = h.Path(tmp_dir) / "frame_0000.png"
            frame_path.write_bytes(b"not a png")
            manifest["frames"][0]["path"] = str(frame_path)

            self.assertIsNone(h.resume_frame_path(manifest, 0))

    def test_stable_run_id_changes_for_flat_map_settings(self):
        config = h.default_config()
        config.mode = "Timelapse"
        info = h.parse_url(config.user_url)
        steps = [h.datetime(2024, 7, 25, 4, 0)]
        first = h.stable_run_id(config, info, steps)
        config.map_view = "flat"
        second = h.stable_run_id(config, info, steps)

        self.assertNotEqual(first, second)

    def test_stable_run_id_changes_for_crosshair_style(self):
        config = h.default_config()
        config.mode = "Timelapse"
        config.map_view = "flat"
        config.add_crosshair = True
        info = h.parse_url(config.user_url)
        steps = [h.datetime(2024, 7, 25, 4, 0)]
        first = h.stable_run_id(config, info, steps)
        config.crosshair_color = "#ff0000"
        second = h.stable_run_id(config, info, steps)

        self.assertNotEqual(first, second)

    def test_load_or_create_timelapse_manifest_reuses_existing(self):
        config = h.default_config()
        config.mode = "Timelapse"
        info = h.parse_url(h.USER_URL)
        steps = [h.datetime(2024, 7, 25, 4, 0)]
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = h.Path(tmp_dir) / "manifest.json"
            frame_dir = h.Path(tmp_dir) / "frames"
            run_id = h.stable_run_id(config, info, steps)
            manifest = h.load_or_create_timelapse_manifest(path, run_id, config, info, steps, frame_dir)
            manifest["movie"] = "movie.gif"
            h.save_timelapse_manifest(path, manifest)

            loaded = h.load_or_create_timelapse_manifest(path, run_id, config, info, steps, frame_dir)

        self.assertEqual(loaded["movie"], "movie.gif")

    @mock.patch("himawari_lowram_processor.cleanup_paths")
    def test_assemble_timelapse_deletes_frame_paths_when_configured(self, mock_cleanup):
        writer, get_writer = fake_imageio_writer_creating_file()
        fake_imageio = mock.Mock()
        fake_imageio.get_writer.side_effect = get_writer
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
        writer, get_writer = fake_imageio_writer_creating_file()
        fake_imageio = mock.Mock()
        fake_imageio.get_writer.side_effect = get_writer
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
        _writer, get_writer = fake_imageio_writer_creating_file()
        fake_imageio = mock.Mock()
        fake_imageio.get_writer.side_effect = get_writer
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
            h.area_compatibility_bands("True Color Reproduction Image", True, "hybrid"),
        )
        self.assertIn("B13", mock_common_area.call_args.kwargs["compatibility_bands"])

    @mock.patch("himawari_lowram_processor.process_frame")
    @mock.patch("himawari_lowram_processor.validate_runtime_dependencies")
    @mock.patch("himawari_lowram_processor.common_area_from_frames")
    def test_run_flat_map_skips_native_common_area(self, mock_common_area, mock_validate, mock_process):
        config = h.default_config()
        config.mode = "Single Image"
        config.map_view = "flat"
        mock_process.return_value = h.Path("out.png")

        h.run(config)

        mock_common_area.assert_not_called()
        area = mock_validate.call_args.args[3]
        self.assertEqual((area.height, area.width), (3018, 2400))
        self.assertEqual(mock_process.call_args.args[2].area_id, "himawari_flat_map")

    @mock.patch("himawari_lowram_processor.process_frame")
    @mock.patch("himawari_lowram_processor.validate_runtime_dependencies")
    @mock.patch("himawari_lowram_processor.common_area_from_frames")
    def test_run_logs_app_version(self, mock_common_area, _mock_validate, mock_process):
        config = h.default_config()
        mock_common_area.return_value = mock.Mock(width=10, height=10)
        mock_process.return_value = h.Path("out.png")

        with self.assertLogs(h.LOG, level="INFO") as captured:
            h.run(config)

        self.assertTrue(any(f"App version: {h.APP_VERSION}" in message for message in captured.output))

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

    @mock.patch("himawari_lowram_processor.assemble_timelapse", return_value=h.Path("movie.gif"))
    @mock.patch("himawari_lowram_processor.process_frame")
    @mock.patch("himawari_lowram_processor.validate_runtime_dependencies")
    @mock.patch("himawari_lowram_processor.common_area_from_frames")
    def test_run_reuses_existing_manifest_frame(
        self,
        mock_common_area,
        _mock_validate,
        mock_process,
        _mock_assemble,
    ):
        config = h.default_config()
        config.mode = "Timelapse"
        config.hours_back = 1
        config.interval_minutes = 60
        info = h.parse_url(config.user_url)
        steps = h.frame_datetimes(h.datetime.strptime(info.timestamp, "%Y%m%d_%H%M"), config.mode, 1, 60)
        mock_common_area.return_value = mock.Mock(width=10, height=10)
        original_output = h.OUTPUT_DIR
        try:
            with tempfile.TemporaryDirectory() as tmp_dir:
                h.OUTPUT_DIR = h.Path(tmp_dir)
                run_id = h.stable_run_id(config, info, steps)
                frame_dir = h.timelapse_frame_dir(run_id, h.OUTPUT_DIR)
                frame = frame_dir / "frame_0000.png"
                write_test_png(frame)
                manifest = h.build_timelapse_manifest(run_id, config, info, steps, frame_dir)
                h.update_manifest_frame(manifest, 0, frame, "complete")
                h.save_timelapse_manifest(h.timelapse_manifest_path(run_id, h.OUTPUT_DIR), manifest)

                result = h.run(config)

            self.assertEqual(result, [h.Path("movie.gif")])
            mock_process.assert_not_called()
        finally:
            h.OUTPUT_DIR = original_output

    def test_format_environment_results_and_error_report(self):
        result = mock.Mock(ok=False, critical=False, detail="missing")
        result.name = "psutil"

        text = h.format_environment_results([result])
        report = h.build_error_report("boom", h.default_config(), "log line", [h.Path("out.png")])

        self.assertIn("[WARN] psutil: missing", text)
        self.assertIn("boom", report)
        self.assertIn("out.png", report)

    @mock.patch("himawari_lowram_processor.subprocess.Popen")
    def test_gui_environment_fix_uses_current_python_with_fix_flag(self, mock_popen):
        app = object.__new__(h.HimawariProcessorApp)
        app._append_log = mock.Mock()

        h.HimawariProcessorApp._open_environment_fix(app)

        command = mock_popen.call_args.args[0]
        self.assertIn(h.sys.executable, command)
        self.assertIn(str(h.PROJECT_DIR / "check_environment.py"), command)
        self.assertIn("--fix", command)
        app._append_log.assert_called_once()

    @mock.patch("himawari_lowram_processor.subprocess.Popen")
    def test_gui_environment_auto_fix_uses_auto_flag(self, mock_popen):
        app = object.__new__(h.HimawariProcessorApp)
        app._append_log = mock.Mock()

        h.HimawariProcessorApp._open_environment_auto_fix(app)

        command = mock_popen.call_args.args[0]
        self.assertIn(h.sys.executable, command)
        self.assertIn(str(h.PROJECT_DIR / "check_environment.py"), command)
        self.assertIn("--auto", command)
        app._append_log.assert_called_once()

    def test_gui_copy_output_paths_uses_clipboard(self):
        app = object.__new__(h.HimawariProcessorApp)
        app.root = FakeRoot()
        app.last_outputs = [h.Path("out.png")]
        app._append_log = mock.Mock()

        h.HimawariProcessorApp._copy_output_paths(app)

        self.assertEqual(app.root.clipboard, "out.png")
        app._append_log.assert_called_once()

    def test_gui_copy_error_report_generates_report(self):
        app = object.__new__(h.HimawariProcessorApp)
        app.root = FakeRoot()
        app.last_error_report = ""
        app.last_config = h.default_config()
        app.last_outputs = []
        app.log_text = mock.Mock()
        app.log_text.get.return_value = "log"
        app._append_log = mock.Mock()

        h.HimawariProcessorApp._copy_error_report(app)

        self.assertIn("No processing error", app.root.clipboard)

    @mock.patch("himawari_lowram_processor.messagebox.showwarning")
    @mock.patch("himawari_lowram_processor.os.startfile")
    def test_gui_open_missing_path_warns_and_falls_back_to_output_folder(self, mock_startfile, mock_warning):
        app = object.__new__(h.HimawariProcessorApp)
        app._append_log = mock.Mock()
        original_output = h.OUTPUT_DIR
        try:
            with tempfile.TemporaryDirectory() as tmp_dir:
                h.OUTPUT_DIR = h.Path(tmp_dir) / "outputs"
                missing_output = h.Path(tmp_dir) / "deleted" / "out.png"

                h.HimawariProcessorApp._open_path(app, missing_output)

                mock_warning.assert_called_once()
                mock_startfile.assert_called_once_with(str(h.OUTPUT_DIR))
                self.assertTrue(h.OUTPUT_DIR.exists())
                self.assertTrue(any("Requested path is unavailable" in call.args[0] for call in app._append_log.call_args_list))
        finally:
            h.OUTPUT_DIR = original_output

    @mock.patch("himawari_lowram_processor.messagebox.showwarning")
    @mock.patch("himawari_lowram_processor.os.startfile", side_effect=OSError("cannot open"))
    def test_gui_open_existing_path_reports_startfile_failure(self, mock_startfile, mock_warning):
        app = object.__new__(h.HimawariProcessorApp)
        app._append_log = mock.Mock()

        with tempfile.TemporaryDirectory() as tmp_dir:
            target = h.Path(tmp_dir)
            result = h.HimawariProcessorApp._open_existing_path(app, target, "Test folder")

        self.assertFalse(result)
        mock_startfile.assert_called_once_with(str(target))
        mock_warning.assert_called_once()
        self.assertTrue(any("Could not open test folder" in call.args[0] for call in app._append_log.call_args_list))

    def test_gui_mouse_wheel_units_supports_common_platform_events(self):
        self.assertEqual(h.HimawariProcessorApp._mouse_wheel_units(FakeEvent(delta=120)), -1)
        self.assertEqual(h.HimawariProcessorApp._mouse_wheel_units(FakeEvent(delta=-120)), 1)
        self.assertEqual(h.HimawariProcessorApp._mouse_wheel_units(FakeEvent(num=4)), -1)
        self.assertEqual(h.HimawariProcessorApp._mouse_wheel_units(FakeEvent(num=5)), 1)
        self.assertEqual(h.HimawariProcessorApp._mouse_wheel_units(FakeEvent()), 0)

    def test_gui_initial_split_position_uses_clamped_ratio(self):
        self.assertEqual(h.HimawariProcessorApp._initial_split_position(0), 220)
        self.assertEqual(h.HimawariProcessorApp._initial_split_position(240), 120)
        self.assertEqual(h.HimawariProcessorApp._initial_split_position(800), 400)
        self.assertEqual(h.HimawariProcessorApp._initial_split_position(800, ratio=0.05), 160)
        self.assertEqual(h.HimawariProcessorApp._initial_split_position(800, ratio=0.95), 640)

    def test_gui_main_split_waits_for_real_height(self):
        app = object.__new__(h.HimawariProcessorApp)
        app.root = FakeScheduledRoot()
        app.main_pane = FakePane([1, 240, 800])

        h.HimawariProcessorApp._configure_main_split(app)
        while app.root.callbacks:
            callback = app.root.callbacks.pop(0)
            callback()

        self.assertEqual(app.main_pane.sash_positions, [(0, 400)])

    def test_gui_main_split_does_not_use_unsupported_minsize_option(self):
        source = h.Path(h.__file__).read_text(encoding="utf-8")

        self.assertNotIn("self.main_pane.add(notebook_pane, weight=1, minsize=", source)
        self.assertNotIn("self.main_pane.add(self.log_frame, weight=1, minsize=", source)

    def test_gui_view_mode_advanced_restores_and_selects_advanced_tab(self):
        app = object.__new__(h.HimawariProcessorApp)
        app.advanced_tab_id = "advanced-tab"
        app.notebook = FakeNotebook()
        app.ui_mode_var = FakeVar("Advanced")

        h.HimawariProcessorApp._refresh_ui_mode(app)

        self.assertEqual(app.notebook.added, [("advanced-tab", {"text": "Advanced"})])
        self.assertEqual(app.notebook.selected, "advanced-tab")

    def test_gui_view_mode_simple_hides_advanced_tab(self):
        app = object.__new__(h.HimawariProcessorApp)
        app.advanced_tab_id = "advanced-tab"
        app.notebook = FakeNotebook()
        app.ui_mode_var = FakeVar("Simple")

        h.HimawariProcessorApp._refresh_ui_mode(app)

        self.assertEqual(app.notebook.hidden, ["advanced-tab"])

    def test_gui_running_state_disables_mutable_controls(self):
        app = object.__new__(h.HimawariProcessorApp)
        app.start_button = FakeWidget()
        app.stop_button = FakeWidget()
        app.choose_output_button = FakeWidget()
        app.choose_temp_button = FakeWidget()
        app.open_temp_button = FakeWidget()
        app.simple_output_button = FakeWidget()
        app.open_output_button = FakeWidget()
        app.check_env_button = FakeWidget()
        app.quick_fix_button = FakeWidget()
        app.auto_fix_button = FakeWidget()
        app.latest_url_button = FakeWidget()
        app.scan_browser_button = FakeWidget()
        app.local_files_button = FakeWidget()
        app.safe_perf_button = FakeWidget()
        app.best_perf_button = FakeWidget()
        app.overlay_check_button = FakeWidget()
        app.open_last_button = FakeWidget()
        app.copy_paths_button = FakeWidget()
        app.copy_error_button = FakeWidget()
        app.custom_preset_box = FakeWidget()
        app._path_controls = (
            app.choose_output_button,
            app.choose_temp_button,
            app.open_temp_button,
            app.simple_output_button,
        )
        app._refresh_mode_state = mock.Mock()

        h.HimawariProcessorApp._set_running(app, True)

        self.assertEqual(app.start_button.configured["state"], "disabled")
        self.assertEqual(app.stop_button.configured["state"], "normal")
        self.assertEqual(app.choose_output_button.configured["state"], "disabled")
        self.assertEqual(app.choose_temp_button.configured["state"], "disabled")
        self.assertEqual(app.open_output_button.configured["state"], "disabled")
        self.assertEqual(app.check_env_button.configured["state"], "disabled")
        self.assertEqual(app.quick_fix_button.configured["state"], "disabled")
        self.assertEqual(app.auto_fix_button.configured["state"], "disabled")
        self.assertEqual(app.latest_url_button.configured["state"], "disabled")
        self.assertEqual(app.scan_browser_button.configured["state"], "disabled")
        self.assertEqual(app.safe_perf_button.configured["state"], "disabled")
        self.assertEqual(app.best_perf_button.configured["state"], "disabled")
        self.assertEqual(app.overlay_check_button.configured["state"], "disabled")
        self.assertEqual(app.copy_error_button.configured["state"], "disabled")
        self.assertEqual(app.custom_preset_box.configured["state"], "disabled")

        h.HimawariProcessorApp._set_running(app, False)

        self.assertEqual(app.start_button.configured["state"], "normal")
        self.assertEqual(app.stop_button.configured["state"], "disabled")
        self.assertEqual(app.choose_output_button.configured["state"], "normal")
        self.assertEqual(app.choose_temp_button.configured["state"], "normal")
        self.assertEqual(app.latest_url_button.configured["state"], "normal")
        self.assertEqual(app.scan_browser_button.configured["state"], "normal")
        self.assertEqual(app.safe_perf_button.configured["state"], "normal")
        self.assertEqual(app.best_perf_button.configured["state"], "normal")

    def test_gui_rerun_recent_settings_loads_saved_config(self):
        app = object.__new__(h.HimawariProcessorApp)
        config = h.default_config()
        config.composite_choice = "B13 (Infrared Window)"
        record = h.build_recent_run_record("complete", config, "2026-05-26T00:00:00Z")
        app.recent_runs = [record]
        app.recent_tree = mock.Mock()
        app.recent_tree.selection.return_value = [record.run_id]
        app._set_config_vars = mock.Mock()
        app.notebook = mock.Mock()
        app._append_log = mock.Mock()

        h.HimawariProcessorApp._rerun_selected_recent_settings(app)

        loaded_config = app._set_config_vars.call_args.args[0]
        self.assertEqual(loaded_config.composite_choice, "B13 (Infrared Window)")
        app.notebook.select.assert_called_once_with(0)

    def test_gui_copy_selected_recent_paths_uses_clipboard(self):
        app = object.__new__(h.HimawariProcessorApp)
        config = h.default_config()
        record = h.build_recent_run_record(
            "complete",
            config,
            "2026-05-26T00:00:00Z",
            outputs=[h.Path("out.png")],
        )
        app.root = FakeRoot()
        app.recent_runs = [record]
        app.recent_tree = mock.Mock()
        app.recent_tree.selection.return_value = [record.run_id]
        app._append_log = mock.Mock()

        h.HimawariProcessorApp._copy_selected_recent_paths(app)

        self.assertEqual(app.root.clipboard, "out.png")

    @mock.patch("himawari_lowram_processor.os.startfile")
    @mock.patch("himawari_lowram_processor.messagebox.showwarning")
    @mock.patch("himawari_lowram_processor.has_module", return_value=True)
    def test_gui_overlay_check_creates_overlay_folder_without_opening_it(
        self,
        _mock_has_module,
        mock_warning,
        mock_startfile,
    ):
        app = object.__new__(h.HimawariProcessorApp)
        app._append_log = mock.Mock()
        original_project = h.PROJECT_DIR
        try:
            with tempfile.TemporaryDirectory() as tmp_dir:
                h.PROJECT_DIR = h.Path(tmp_dir)

                h.HimawariProcessorApp._check_overlays(app)

                self.assertTrue((h.Path(tmp_dir) / "overlays").exists())
                mock_warning.assert_called_once()
                self.assertTrue(app._append_log.called)
                self.assertTrue(any("Overlay folder:" in call.args[0] for call in app._append_log.call_args_list))
                mock_startfile.assert_not_called()
        finally:
            h.PROJECT_DIR = original_project

    def test_gui_path_fields_refresh_from_selected_directories(self):
        app = object.__new__(h.HimawariProcessorApp)
        app.output_dir_var = FakeVar()
        app.temp_dir_var = FakeVar()

        original_output = h.OUTPUT_DIR
        original_temp = h.TEMP_DIR
        try:
            h.OUTPUT_DIR = h.Path("C:/Himawari/outputs")
            h.TEMP_DIR = h.Path("C:/Himawari/temp")

            h.HimawariProcessorApp._refresh_path_fields(app)

            self.assertEqual(app.output_dir_var.get(), str(h.OUTPUT_DIR))
            self.assertEqual(app.temp_dir_var.get(), str(h.TEMP_DIR))
        finally:
            h.OUTPUT_DIR = original_output
            h.TEMP_DIR = original_temp

    def test_gui_read_config_rejects_non_finite_flat_value(self):
        app = object.__new__(h.HimawariProcessorApp)
        config = h.default_config()
        app.url_var = FakeVar(config.user_url)
        app.mode_var = FakeVar(config.mode)
        app.composite_var = FakeVar(config.composite_choice)
        app.hours_var = FakeVar(str(config.hours_back))
        app.interval_var = FakeVar(str(config.interval_minutes))
        app.fps_var = FakeVar(str(config.fps))
        app.auto_download_var = FakeVar(config.auto_download)
        app.gpu_acceleration_var = FakeVar(config.gpu_acceleration)
        app.night_fallback_var = FakeVar(config.use_night_fallback)
        app.night_fallback_mode_var = FakeVar(config.night_fallback_mode)
        app.download_workers_var = FakeVar(str(config.download_workers))
        app.timelapse_format_var = FakeVar(config.timelapse_format)
        app.delete_frames_var = FakeVar(config.delete_timelapse_frames)
        app.image_format_var = FakeVar(config.image_format)
        app.output_template_var = FakeVar(config.output_template)
        app.resampler_var = FakeVar(config.resampler)
        app.quality_fallback_var = FakeVar(config.allow_quality_fallback)
        app.border_lines_var = FakeVar(config.add_border_lines)
        app.border_color_var = FakeVar(config.border_line_color)
        app.border_width_var = FakeVar(str(config.border_line_width))
        app.map_labels_var = FakeVar(config.add_map_labels)
        app.night_boundary_var = FakeVar(config.add_night_boundary)
        app.crosshair_var = FakeVar(config.add_crosshair)
        app.crosshair_type_var = FakeVar(config.crosshair_type)
        app.crosshair_color_var = FakeVar(config.crosshair_color)
        app.zoom_earth_style_var = FakeVar(config.zoom_earth_style)
        app.map_view_var = FakeVar("flat")
        app.flat_min_lat_var = FakeVar("nan")
        app.flat_max_lat_var = FakeVar(str(config.flat_max_lat))
        app.flat_min_lon_var = FakeVar(str(config.flat_min_lon))
        app.flat_max_lon_var = FakeVar(str(config.flat_max_lon))
        app.flat_resolution_var = FakeVar(str(config.flat_resolution_deg))
        app.ram_limit_var = FakeVar(str(config.ram_limit_gb))
        app.chunk_var = FakeVar(config.dask_chunk_size)
        app.dask_workers_var = FakeVar(str(config.dask_num_workers))

        with self.assertRaisesRegex(ValueError, "Flat map min latitude must be finite"):
            h.HimawariProcessorApp._read_config(app)

    @mock.patch("himawari_lowram_processor.system_performance_profile")
    def test_gui_applies_performance_recommendation_to_fields(self, mock_profile):
        app = object.__new__(h.HimawariProcessorApp)
        app.download_workers_var = FakeVar("")
        app.dask_workers_var = FakeVar("")
        app.chunk_var = FakeVar("")
        app.ram_limit_var = FakeVar("")
        app._update_setup_status = mock.Mock()
        app._write_current_settings = mock.Mock()
        app._append_log = mock.Mock()
        mock_profile.return_value = h.SystemPerformanceProfile(
            total_ram_gb=64.0,
            available_ram_gb=32.0,
            cpu_count=12,
            cpu_percent=10.0,
        )

        h.HimawariProcessorApp._apply_performance_recommendation(app, "best_performance")

        self.assertEqual(app.download_workers_var.get(), "4")
        self.assertEqual(app.dask_workers_var.get(), "2")
        self.assertEqual(app.chunk_var.get(), "128MiB")
        self.assertEqual(app.ram_limit_var.get(), "16.0")
        app._update_setup_status.assert_called_once()
        app._write_current_settings.assert_called_once()
        self.assertTrue(app._append_log.called)

    @mock.patch("himawari_lowram_processor.messagebox.askyesno", return_value=False)
    @mock.patch("himawari_lowram_processor.gpu_support_status")
    def test_gui_gpu_toggle_reverts_when_support_missing(self, mock_status, mock_ask):
        app = object.__new__(h.HimawariProcessorApp)
        app.gpu_acceleration_var = FakeVar(True)
        app._update_setup_status = mock.Mock()
        app._write_current_settings = mock.Mock()
        app._append_log = mock.Mock()
        app._open_gpu_environment_fix = mock.Mock()
        mock_status.return_value = h.GpuSupportStatus(False, "missing")

        h.HimawariProcessorApp._toggle_gpu_acceleration(app)

        self.assertFalse(app.gpu_acceleration_var.get())
        app._open_gpu_environment_fix.assert_not_called()
        mock_ask.assert_called_once()

    @mock.patch("himawari_lowram_processor.gpu_support_status")
    def test_gui_gpu_toggle_accepts_ready_support(self, mock_status):
        app = object.__new__(h.HimawariProcessorApp)
        app.gpu_acceleration_var = FakeVar(True)
        app._update_setup_status = mock.Mock()
        app._write_current_settings = mock.Mock()
        app._append_log = mock.Mock()
        mock_status.return_value = h.GpuSupportStatus(True, "ready", device_name="Test GPU", device_count=1)

        h.HimawariProcessorApp._toggle_gpu_acceleration(app)

        self.assertTrue(app.gpu_acceleration_var.get())
        app._update_setup_status.assert_called_once()
        app._write_current_settings.assert_called_once()
        messages = [call.args[0] for call in app._append_log.call_args_list]
        self.assertTrue(any("GPU acceleration enabled: ready" in message for message in messages))
        self.assertTrue(any("custom true-color math only" in message for message in messages))

    @mock.patch("himawari_lowram_processor.messagebox.showinfo")
    @mock.patch("himawari_lowram_processor.import_local_hsd_segments")
    def test_gui_local_import_updates_source_and_offline_mode(self, mock_import, mock_info):
        app = object.__new__(h.HimawariProcessorApp)
        result = h.LocalImportResult(
            sat_id="HS_H09",
            timestamp="20240725_0400",
            area="FLDK",
            total_segments=10,
            imported_paths=(h.Path("cached.dat"),),
            reused_paths=(),
            bands=("B13",),
        )
        mock_import.return_value = result
        app.url_var = FakeVar("")
        app.auto_download_var = FakeVar(True)
        app.mode_var = FakeVar("Timelapse")
        app.status_var = FakeVar("")
        app._update_setup_status = mock.Mock(return_value=h.SetupStatus(True, (), (), ()))
        app._write_current_settings = mock.Mock()
        app._append_log = mock.Mock()

        h.HimawariProcessorApp._import_local_hsd_files(app, ["file.dat"])

        self.assertIn("AHI-L1b-FLDK", app.url_var.get())
        self.assertFalse(app.auto_download_var.get())
        self.assertEqual(app.mode_var.get(), "Single Image")
        app._update_setup_status.assert_called_once()
        app._write_current_settings.assert_called_once()
        mock_info.assert_called_once()

    def test_gui_optional_drop_target_skips_without_tkinterdnd2(self):
        app = object.__new__(h.HimawariProcessorApp)
        app.pending_log_messages = []
        app.local_drop_label = mock.Mock()

        original_import = __import__

        def fake_import(name, *args, **kwargs):
            if name == "tkinterdnd2":
                raise ImportError("missing")
            return original_import(name, *args, **kwargs)

        with mock.patch("builtins.__import__", side_effect=fake_import):
            h.HimawariProcessorApp._setup_optional_local_drop_target(app)

        app.local_drop_label.configure.assert_called_once()
        self.assertIn("Local Files", app.local_drop_label.configure.call_args.kwargs["text"])

    def test_gui_optional_drop_target_registers_when_tkinterdnd2_available(self):
        app = object.__new__(h.HimawariProcessorApp)
        app.local_drop_label = mock.Mock()
        app._handle_local_file_drop = mock.Mock()
        app._append_log = mock.Mock()
        fake_module = mock.Mock(DND_FILES="DND_Files")

        original_import = __import__

        def fake_import(name, *args, **kwargs):
            if name == "tkinterdnd2":
                return fake_module
            return original_import(name, *args, **kwargs)

        with mock.patch("builtins.__import__", side_effect=fake_import):
            h.HimawariProcessorApp._setup_optional_local_drop_target(app)

        app.local_drop_label.drop_target_register.assert_called_once_with("DND_Files")
        app.local_drop_label.dnd_bind.assert_called_once_with("<<Drop>>", app._handle_local_file_drop)

    def test_gui_flushes_pending_log_messages_after_log_widget_exists(self):
        app = object.__new__(h.HimawariProcessorApp)
        app.pending_log_messages = ["queued one", "queued two"]
        app.log_text = mock.Mock()

        h.HimawariProcessorApp._flush_pending_log_messages(app)

        self.assertEqual(app.pending_log_messages, [])
        inserted = [call.args[1] for call in app.log_text.insert.call_args_list]
        self.assertEqual(inserted, ["queued one\n", "queued two\n"])


if __name__ == "__main__":
    unittest.main()
