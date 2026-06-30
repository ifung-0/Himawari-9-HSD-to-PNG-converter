import unittest
import json
import warnings
import zipfile
from io import StringIO
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import mock

import check_environment as env
import himawari_lowram_processor as processor


class FakeDataID:
    def __init__(self, name):
        self.name = name

    def __getitem__(self, key):
        if key == "name":
            return self.name
        raise KeyError(key)


class EnvironmentCheckTests(unittest.TestCase):
    def test_version_tuple_parses_numeric_prefix(self):
        self.assertGreaterEqual(env.version_tuple("0.60.0"), (0, 60))
        self.assertEqual(env.version_tuple("1.2.3rc1"), (1, 2, 3))

    def test_minimum_versions_from_requirements_parses_simple_bounds(self):
        with TemporaryDirectory() as tmp_dir:
            requirements = Path(tmp_dir) / "requirements.txt"
            requirements.write_text(
                "satpy>=0.60\n"
                "scipy>=1.15\n"
                "imageio-ffmpeg>=0.4; platform_system != \"Emscripten\"\n"
                "example[extra]>=1.2,!=1.3\n",
                encoding="utf-8",
            )

            minimums = env.minimum_versions_from_requirements(requirements)

        self.assertEqual(minimums["satpy"], "0.60")
        self.assertEqual(minimums["scipy"], "1.15")
        self.assertEqual(minimums["imageio-ffmpeg"], "0.4")
        self.assertEqual(minimums["example"], "1.2")

    def test_app_version_result_uses_processor_version(self):
        result = env.check_app_version()

        self.assertTrue(result.ok)
        self.assertEqual(result.name, "app version")
        self.assertIn(processor.APP_VERSION, result.detail)

    def test_banner_prints_app_version(self):
        with mock.patch("sys.stdout", new_callable=StringIO) as stdout:
            env.print_banner()

        output = stdout.getvalue()
        self.assertIn("App:", output)
        self.assertIn(processor.APP_VERSION, output)

    def test_pip_install_command_uses_current_python_and_requirements(self):
        command = env.pip_install_command(upgrade=True)

        self.assertEqual(command[0], env.sys.executable)
        self.assertIn("--upgrade", command)
        self.assertEqual(command[-2], "-r")
        self.assertEqual(command[-1], str(env.REQUIREMENTS_FILE))

    def test_gpu_pip_install_command_uses_gpu_requirements(self):
        command = env.gpu_pip_install_command(upgrade=True)

        self.assertEqual(command[0], env.sys.executable)
        self.assertIn("--upgrade", command)
        self.assertEqual(command[-2], "-r")
        self.assertEqual(command[-1], str(env.GPU_REQUIREMENTS_FILE))

    @mock.patch("check_environment.subprocess.run")
    def test_check_pip_available_reports_failure(self, mock_run):
        mock_run.return_value = mock.Mock(returncode=1, stdout="", stderr="No module named pip")

        result = env.check_pip_available("python")

        self.assertFalse(result.ok)
        self.assertTrue(result.critical)
        self.assertIn("pip is unavailable", result.detail)

    @mock.patch("check_environment.subprocess.run")
    def test_check_pip_available_reports_success(self, mock_run):
        mock_run.return_value = mock.Mock(returncode=0, stdout="pip 25.0", stderr="")

        result = env.check_pip_available("python")

        self.assertTrue(result.ok)
        self.assertFalse(result.critical)
        self.assertIn("pip 25.0", result.detail)

    def test_supported_python_version_accepts_312_and_313(self):
        self.assertTrue(env.supported_python_version((3, 12)))
        self.assertTrue(env.supported_python_version((3, 13)))
        self.assertFalse(env.supported_python_version((3, 14)))

    def test_venv_python_path_uses_project_venv(self):
        path = env.venv_python_path()

        self.assertIn(".venv", str(path))
        self.assertTrue(str(path).endswith("python.exe") or str(path).endswith("python"))

    @mock.patch("check_environment.importlib.import_module", return_value=object())
    @mock.patch("check_environment.importlib.util.find_spec", return_value=object())
    @mock.patch("check_environment.package_version", return_value="1.2.0")
    def test_check_package_reports_supported_minimum_version(self, _mock_version, _mock_spec, _mock_import):
        package = env.PackageCheck("demo", "demo-dist", "testing")

        result = env.check_package(package, {"demo-dist": "1.0"})

        self.assertTrue(result.ok)
        self.assertIn("1.2.0 >= 1.0", result.detail)

    @mock.patch("check_environment.importlib.import_module", return_value=object())
    @mock.patch("check_environment.importlib.util.find_spec", return_value=object())
    @mock.patch("check_environment.package_version", return_value="0.9.0")
    def test_check_package_reports_outdated_critical_dependency(self, _mock_version, _mock_spec, _mock_import):
        package = env.PackageCheck("demo", "demo-dist", "testing")

        result = env.check_package(package, {"demo-dist": "1.0"})

        self.assertFalse(result.ok)
        self.assertTrue(result.critical)
        self.assertIn("older than required 1.0", result.detail)

    @mock.patch("check_environment.importlib.import_module", return_value=object())
    @mock.patch("check_environment.importlib.util.find_spec", return_value=object())
    @mock.patch("check_environment.package_version", return_value=None)
    def test_check_package_reports_import_distribution_mismatch(self, _mock_version, _mock_spec, _mock_import):
        package = env.PackageCheck("demo", "demo-dist", "testing")

        result = env.check_package(package, {})

        self.assertFalse(result.ok)
        self.assertIn("metadata was not found", result.detail)

    @mock.patch("check_environment.importlib.util.find_spec", return_value=None)
    @mock.patch("check_environment.package_version", return_value=None)
    def test_check_package_preserves_optional_warning_classification(self, _mock_version, _mock_spec):
        package = env.PackageCheck("demo", "demo-dist", "testing", critical=False)

        result = env.check_package(package, {})

        self.assertFalse(result.ok)
        self.assertFalse(result.critical)
        self.assertIn("module is not importable", result.detail)

    @mock.patch("check_environment.importlib.util.find_spec", return_value=object())
    @mock.patch("check_environment.package_version", return_value="1.0")
    def test_check_package_suppresses_known_cupy_cuda_path_warning(self, _mock_version, _mock_spec):
        package = env.PackageCheck("demo", "demo-dist", "testing")

        def noisy_import(_name):
            warnings.warn_explicit(
                "CUDA path could not be detected. Set CUDA_PATH environment variable if CuPy fails to load.",
                UserWarning,
                "cupy/_environment.py",
                284,
                module="cupy._environment",
            )
            return object()

        with mock.patch("check_environment.importlib.import_module", side_effect=noisy_import):
            with warnings.catch_warnings(record=True) as captured:
                warnings.simplefilter("always")
                result = env.check_package(package, {})

        self.assertTrue(result.ok)
        self.assertEqual(captured, [])

    def test_tkinterdnd2_is_optional_drag_drop_check(self):
        package = next(package for package in env.PACKAGE_CHECKS if package.import_name == "tkinterdnd2")

        self.assertFalse(package.critical)
        self.assertEqual(package.distribution_name, "tkinterdnd2")
        self.assertIn("drag/drop", package.purpose)

    def test_has_critical_failures_ignores_optional_warnings(self):
        results = [
            env.CheckResult("optional", False, "missing", critical=False),
            env.CheckResult("critical", True, "ok", critical=True),
        ]
        self.assertFalse(env.has_critical_failures(results))

        results.append(env.CheckResult("critical missing", False, "missing", critical=True))
        self.assertTrue(env.has_critical_failures(results))

    def test_result_counts_and_grouping(self):
        results = [
            env.CheckResult("python version", True, "ok"),
            env.CheckResult("psutil", False, "missing", critical=False),
            env.CheckResult("CuPy GPU package", False, "missing", critical=False),
            env.CheckResult("unused root programs", False, "archive recommended", critical=False),
            env.CheckResult("satpy", False, "missing", critical=True),
            env.CheckResult("default output folder", True, "ok"),
        ]

        self.assertEqual(env.result_group(results[0]), "Python")
        self.assertEqual(env.result_group(results[1]), "Packages")
        self.assertEqual(env.result_group(results[2]), "GPU")
        self.assertEqual(env.result_group(results[3]), "Project Cleanup")
        self.assertEqual(env.result_group(results[5]), "Paths")
        self.assertEqual(env.result_counts(results), (2, 3, 1))
        self.assertEqual(env.status_label(results[0]), "OK")
        self.assertEqual(env.status_label(results[1]), "WARN")
        self.assertEqual(env.status_label(results[4]), "FAIL")

    @mock.patch("check_environment.importlib.util.find_spec", return_value=None)
    def test_check_gpu_support_reports_missing_cupy_as_warning(self, _mock_spec):
        results = env.check_gpu_support()

        self.assertTrue(any(result.name == "GPU requirements file" for result in results))
        missing = next(result for result in results if result.name == "CuPy GPU package")
        self.assertFalse(missing.ok)
        self.assertFalse(missing.critical)

    @mock.patch("check_environment.importlib.util.find_spec", return_value=object())
    def test_check_gpu_support_reports_kernel_test_failure(self, _mock_spec):
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
            results = env.check_gpu_support()

        kernel = next(result for result in results if result.name == "CUDA kernel test")
        self.assertFalse(kernel.ok)
        self.assertFalse(kernel.critical)
        self.assertIn("--gpu-fix", kernel.detail)

    @mock.patch("check_environment.importlib.util.find_spec", return_value=object())
    def test_check_gpu_support_reports_kernel_test_success(self, _mock_spec):
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
            results = env.check_gpu_support()

        kernel = next(result for result in results if result.name == "CUDA kernel test")
        self.assertTrue(kernel.ok)
        self.assertIn("kernel operation succeeded", kernel.detail)

    @mock.patch("check_environment.check_gpu_support", return_value=[env.CheckResult("CuPy GPU package", True, "ok", critical=False)])
    def test_run_checks_can_include_gpu_diagnostics(self, mock_gpu):
        with mock.patch("check_environment.check_packages", return_value=[]), \
            mock.patch("check_environment.has_critical_failures", return_value=True):
            results = env.run_checks(include_gpu=True)

        self.assertTrue(any(result.name == "CuPy GPU package" for result in results))
        mock_gpu.assert_called_once()

    @mock.patch("check_environment.print_banner")
    @mock.patch("check_environment.print_results")
    @mock.patch("check_environment.print_next_steps")
    @mock.patch("check_environment.run_gpu_fix", return_value=0)
    @mock.patch("check_environment.run_checks")
    def test_main_gpu_fix_runs_gpu_repair_and_rechecks(
        self,
        mock_run_checks,
        mock_gpu_fix,
        _mock_next_steps,
        _mock_print_results,
        _mock_banner,
    ):
        clean = [env.CheckResult("python version", True, "ok")]
        mock_run_checks.return_value = clean

        with mock.patch("check_environment.sys.argv", ["check_environment.py", "--gpu-fix", "--plain"]):
            result = env.main()

        self.assertEqual(result, 0)
        mock_gpu_fix.assert_called_once()
        self.assertEqual(mock_run_checks.call_count, 2)
        mock_run_checks.assert_called_with(include_gpu=True)

    def test_print_results_groups_output(self):
        results = [
            env.CheckResult("python version", True, "3.12 supported"),
            env.CheckResult("psutil", False, "missing", critical=False),
            env.CheckResult("default output folder", True, "writable"),
        ]

        with mock.patch("sys.stdout", new_callable=StringIO) as stdout:
            env.print_results(results)

        output = stdout.getvalue()
        self.assertIn("Python:", output)
        self.assertIn("Packages:", output)
        self.assertIn("Paths:", output)
        self.assertIn("Summary: 2 OK, 1 warning(s), 0 critical failure(s)", output)

    def test_print_results_plain_output_keeps_one_line_per_check(self):
        results = [
            env.CheckResult("python version", True, "3.12 supported"),
            env.CheckResult("cloud sync location", False, "OneDrive", critical=False),
        ]

        with mock.patch("sys.stdout", new_callable=StringIO) as stdout:
            env.print_results(results, grouped=False)

        output = stdout.getvalue()
        self.assertIn("[OK] python version: 3.12 supported", output)
        self.assertIn("[WARN] cloud sync location: OneDrive", output)
        self.assertNotIn("Paths:", output)

    def test_print_next_steps_for_optional_warnings_mentions_fix_and_cli(self):
        results = [env.CheckResult("psutil", False, "missing", critical=False)]

        with mock.patch("sys.stdout", new_callable=StringIO) as stdout:
            env.print_next_steps(results)

        output = stdout.getvalue()
        self.assertIn("Optional checks need attention: psutil", output)
        self.assertIn("--fix", output)
        self.assertIn("GUI or CLI", output)
        self.assertIn("non-synced", output)

    def test_print_next_steps_for_gpu_warning_mentions_gpu_fix(self):
        results = [env.CheckResult("CuPy GPU package", False, "missing", critical=False)]

        with mock.patch("sys.stdout", new_callable=StringIO) as stdout:
            env.print_next_steps(results)

        output = stdout.getvalue()
        self.assertIn("--gpu-fix", output)

    def test_print_next_steps_for_ready_environment_mentions_launchers(self):
        results = [env.CheckResult("python version", True, "ok")]

        with mock.patch("sys.stdout", new_callable=StringIO) as stdout:
            env.print_next_steps(results)

        output = stdout.getvalue()
        self.assertIn("Environment looks ready", output)
        self.assertIn("himawari gui", output)
        self.assertIn("himawari cli", output)
        self.assertNotIn("run_dashboard.bat", output)

    def test_final_status_code_fails_only_for_critical_failures(self):
        self.assertEqual(env.final_status_code([env.CheckResult("optional", False, "warn", critical=False)]), 0)
        self.assertEqual(env.final_status_code([env.CheckResult("critical", False, "fail")]), 1)

    def test_cleanup_scan_detects_known_and_unknown_root_programs(self):
        with TemporaryDirectory() as tmp_dir:
            project_dir = Path(tmp_dir)
            for name in env.CORE_ROOT_FILES:
                (project_dir / name).write_text("core", encoding="utf-8")
            (project_dir / "HW_9_to_png_tiff 2.(old version)ipynb").write_text("{}", encoding="utf-8")
            (project_dir / "tiff_to_png_converter.py").write_text("print('old')", encoding="utf-8")
            (project_dir / "himawari_dashboard.py").write_text("print('old')", encoding="utf-8")
            (project_dir / "scratch_tool.py").write_text("print('extra')", encoding="utf-8")
            (project_dir / "notes.txt").write_text("ignore", encoding="utf-8")

            candidates = env.root_cleanup_candidates(project_dir)

        names = {candidate.path.name for candidate in candidates}
        self.assertIn("HW_9_to_png_tiff 2.(old version)ipynb", names)
        self.assertIn("tiff_to_png_converter.py", names)
        self.assertIn("himawari_dashboard.py", names)
        self.assertIn("scratch_tool.py", names)
        self.assertNotIn("check_environment.py", names)
        self.assertNotIn("notes.txt", names)

    def test_project_cleanup_check_reports_warning_for_candidates(self):
        with TemporaryDirectory() as tmp_dir:
            project_dir = Path(tmp_dir)
            (project_dir / "run_dashboard.bat").write_text("python dashboard.py", encoding="utf-8")

            result = env.check_project_cleanup(project_dir)

        self.assertFalse(result.ok)
        self.assertFalse(result.critical)
        self.assertIn("run_dashboard.bat", result.detail)

    def test_project_cleanup_check_ok_when_no_candidates(self):
        with TemporaryDirectory() as tmp_dir:
            project_dir = Path(tmp_dir)
            (project_dir / "check_environment.py").write_text("core", encoding="utf-8")

            result = env.check_project_cleanup(project_dir)

        self.assertTrue(result.ok)
        self.assertFalse(result.critical)

    def test_runtime_json_check_accepts_valid_json_and_warns_for_broken_json(self):
        with TemporaryDirectory() as tmp_dir:
            project_dir = Path(tmp_dir)
            (project_dir / "himawari_gui_settings.json").write_text('{"ok": true}', encoding="utf-8")
            valid = env.check_runtime_json_files(project_dir)
            (project_dir / "himawari_recent_runs.json").write_text("{broken", encoding="utf-8")
            broken = env.check_runtime_json_files(project_dir)

        self.assertTrue(valid.ok)
        self.assertFalse(broken.ok)
        self.assertFalse(broken.critical)
        self.assertIn("himawari_recent_runs.json", broken.detail)

    def test_repair_runtime_json_files_archives_invalid_json_only(self):
        with TemporaryDirectory() as tmp_dir:
            project_dir = Path(tmp_dir)
            valid_path = project_dir / "himawari_gui_settings.json"
            broken_path = project_dir / "himawari_recent_runs.json"
            valid_path.write_text('{"ok": true}', encoding="utf-8")
            broken_path.write_text("{broken", encoding="utf-8")

            result = env.repair_runtime_json_files(project_dir, timestamp="20260102_030405")
            archive_dir = project_dir / "cleanup_archive" / "20260102_030405" / "invalid_runtime_json"
            manifest = json.loads((archive_dir / "manifest.json").read_text(encoding="utf-8"))

            self.assertTrue(result.ok)
            self.assertTrue(valid_path.exists())
            self.assertFalse(broken_path.exists())
            self.assertTrue((archive_dir / "himawari_recent_runs.json").exists())
            self.assertEqual(Path(manifest[0]["original_path"]).name, "himawari_recent_runs.json")
            self.assertIn("JSONDecodeError", manifest[0]["parse_error"])

    def test_repair_runtime_json_files_noops_when_all_valid(self):
        with TemporaryDirectory() as tmp_dir:
            project_dir = Path(tmp_dir)
            (project_dir / "himawari_gui_settings.json").write_text('{"ok": true}', encoding="utf-8")

            result = env.repair_runtime_json_files(project_dir, timestamp="20260102_030405")

            self.assertTrue(result.ok)
            self.assertIn("no invalid", result.detail)
            self.assertFalse((project_dir / "cleanup_archive").exists())

    def test_ensure_support_folders_creates_missing_folders(self):
        with TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir) / "appdata"
            folders = (root, root / "logs", root / "temp", root / "cache")

            result = env.ensure_support_folders(folders)

            self.assertTrue(result.ok)
            for folder in folders:
                self.assertTrue(folder.is_dir())

    def test_check_support_folders_accepts_creatable_targets(self):
        with TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir) / "appdata"
            folders = (root / "logs", root / "temp", root / "cache")

            result = env.check_support_folders(folders)

            self.assertTrue(result.ok)
            self.assertIn("writable or creatable", result.detail)
            self.assertFalse((root / "logs").exists())

    def test_cleanup_stale_partial_downloads_removes_only_part_files(self):
        with TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir) / "cache"
            nested = root / "nested"
            nested.mkdir(parents=True)
            stale = nested / "frame.dat.bz2.part"
            keep = nested / "frame.dat.bz2"
            stale.write_text("partial", encoding="utf-8")
            keep.write_text("complete", encoding="utf-8")

            result = env.cleanup_stale_partial_downloads((root,))

            self.assertTrue(result.ok)
            self.assertFalse(stale.exists())
            self.assertTrue(keep.exists())
            self.assertIn("removed 1", result.detail)

    def test_archive_unused_programs_moves_candidates_and_writes_manifest(self):
        with TemporaryDirectory() as tmp_dir:
            project_dir = Path(tmp_dir)
            old_file = project_dir / "tiff_to_png_converter.py"
            old_notebook = project_dir / "HW_9_to_png_tiff 2.(old version)ipynb"
            extra_file = project_dir / "scratch_tool.py"
            core_file = project_dir / "himawari_lowram_processor.py"
            old_file.write_text("old", encoding="utf-8")
            old_notebook.write_text("{}", encoding="utf-8")
            extra_file.write_text("extra", encoding="utf-8")
            core_file.write_text("core", encoding="utf-8")

            result = env.archive_unused_programs(project_dir, timestamp="20260102_030405")
            archive_dir = project_dir / "cleanup_archive" / "20260102_030405"
            manifest = json.loads((archive_dir / "manifest.json").read_text(encoding="utf-8"))

            self.assertTrue(result.ok)
            self.assertFalse(old_file.exists())
            self.assertFalse(old_notebook.exists())
            self.assertFalse(extra_file.exists())
            self.assertTrue(core_file.exists())
            self.assertTrue((archive_dir / "tiff_to_png_converter.py").exists())
            self.assertTrue((archive_dir / "HW_9_to_png_tiff 2.(old version)ipynb").exists())
            self.assertTrue((archive_dir / "scratch_tool.py").exists())
            self.assertEqual(
                {Path(item["original_path"]).name for item in manifest},
                {
                    "HW_9_to_png_tiff 2.(old version)ipynb",
                    "tiff_to_png_converter.py",
                    "scratch_tool.py",
                },
            )

    @mock.patch("check_environment.shutil.move", side_effect=OSError("locked"))
    def test_archive_unused_programs_reports_move_failures_without_crashing(self, _mock_move):
        with TemporaryDirectory() as tmp_dir:
            project_dir = Path(tmp_dir)
            (project_dir / "tiff_to_png_converter.py").write_text("old", encoding="utf-8")

            result = env.archive_unused_programs(project_dir, timestamp="20260102_030405")

        self.assertFalse(result.ok)
        self.assertFalse(result.critical)
        self.assertIn("OSError", result.detail)

    @mock.patch("check_environment.print_banner")
    @mock.patch("check_environment.print_results")
    @mock.patch("check_environment.print_next_steps")
    @mock.patch("check_environment.run_checks")
    @mock.patch("check_environment.archive_unused_programs")
    def test_main_archive_unused_runs_cleanup_without_pip(
        self,
        mock_archive,
        mock_run_checks,
        _mock_next_steps,
        _mock_print_results,
        _mock_banner,
    ):
        clean = [env.CheckResult("python version", True, "ok")]
        mock_run_checks.return_value = clean
        mock_archive.return_value = env.CheckResult("archive unused programs", True, "archived", critical=False)

        with mock.patch("check_environment.sys.argv", ["check_environment.py", "--archive-unused"]):
            result = env.main()

        self.assertEqual(result, 0)
        mock_archive.assert_called_once_with(env.PROJECT_DIR)
        self.assertEqual(mock_run_checks.call_count, 2)

    @mock.patch("check_environment.print_banner")
    @mock.patch("check_environment.print_results")
    @mock.patch("check_environment.print_next_steps")
    @mock.patch("check_environment.recheck_after_repair_fresh")
    @mock.patch("check_environment.run_fix", return_value=0)
    @mock.patch("check_environment.run_checks")
    def test_main_fix_runs_even_when_initial_checks_are_clean(
        self,
        mock_run_checks,
        mock_run_fix,
        mock_recheck_fresh,
        _mock_next_steps,
        _mock_print_results,
        _mock_banner,
    ):
        clean = [env.CheckResult("python version", True, "ok")]
        mock_run_checks.return_value = clean
        mock_recheck_fresh.return_value = clean

        with mock.patch("check_environment.sys.argv", ["check_environment.py", "--fix"]):
            result = env.main()

        self.assertEqual(result, 0)
        mock_run_fix.assert_called_once_with(
            install_overlays=True,
            force_overlay_data=False,
            archive_unused=True,
        )
        self.assertEqual(mock_run_checks.call_count, 1)

    @mock.patch("check_environment.print_banner")
    @mock.patch("check_environment.print_results")
    @mock.patch("check_environment.print_next_steps")
    @mock.patch("check_environment.recheck_after_repair_fresh")
    @mock.patch("check_environment.run_fix", return_value=0)
    @mock.patch("check_environment.run_checks")
    def test_main_fix_rechecks_in_fresh_process(
        self,
        mock_run_checks,
        _mock_run_fix,
        mock_recheck_fresh,
        _mock_next_steps,
        _mock_print_results,
        _mock_banner,
    ):
        clean = [env.CheckResult("python version", True, "ok")]
        mock_run_checks.return_value = clean
        mock_recheck_fresh.return_value = [env.CheckResult("fresh recheck", True, "ok", critical=False)]

        with mock.patch("check_environment.sys.argv", ["check_environment.py", "--fix", "--plain"]):
            result = env.main()

        self.assertEqual(result, 0)
        mock_recheck_fresh.assert_called_once_with(grouped=False)

    @mock.patch("check_environment.print_banner")
    @mock.patch("check_environment.print_results")
    @mock.patch("check_environment.print_next_steps")
    @mock.patch("check_environment.recheck_after_repair_fresh")
    @mock.patch("check_environment.run_auto_fix", return_value=0)
    @mock.patch("check_environment.run_checks")
    def test_main_auto_forwards_overlay_options(
        self,
        mock_run_checks,
        mock_auto_fix,
        mock_recheck_fresh,
        _mock_next_steps,
        _mock_print_results,
        _mock_banner,
    ):
        clean = [env.CheckResult("python version", True, "ok")]
        mock_run_checks.return_value = clean
        mock_recheck_fresh.return_value = clean

        with mock.patch(
            "check_environment.sys.argv",
            ["check_environment.py", "--auto", "--skip-overlay-data", "--force-overlay-data"],
        ):
            result = env.main()

        self.assertEqual(result, 0)
        mock_auto_fix.assert_called_once_with(
            clean,
            install_overlays=False,
            force_overlay_data=True,
            archive_unused=True,
        )

    @mock.patch("check_environment.print_banner")
    @mock.patch("check_environment.print_results")
    @mock.patch("check_environment.print_next_steps")
    @mock.patch("check_environment.recheck_after_repair_fresh")
    @mock.patch("check_environment.run_fix", return_value=0)
    @mock.patch("check_environment.run_checks")
    def test_main_fix_can_skip_archive_cleanup(
        self,
        mock_run_checks,
        mock_run_fix,
        mock_recheck_fresh,
        _mock_next_steps,
        _mock_print_results,
        _mock_banner,
    ):
        clean = [env.CheckResult("python version", True, "ok")]
        mock_run_checks.return_value = clean
        mock_recheck_fresh.return_value = clean

        with mock.patch("check_environment.sys.argv", ["check_environment.py", "--fix", "--no-archive-unused"]):
            result = env.main()

        self.assertEqual(result, 0)
        mock_run_fix.assert_called_once_with(
            install_overlays=True,
            force_overlay_data=False,
            archive_unused=False,
        )

    @mock.patch("check_environment.print_banner")
    @mock.patch("check_environment.print_results")
    @mock.patch("check_environment.print_next_steps")
    @mock.patch("check_environment.recheck_after_repair_fresh")
    @mock.patch("check_environment.run_auto_fix", return_value=0)
    @mock.patch("check_environment.run_checks")
    def test_main_auto_rechecks_supported_python_in_fresh_process(
        self,
        mock_run_checks,
        _mock_auto_fix,
        mock_recheck_fresh,
        _mock_next_steps,
        _mock_print_results,
        _mock_banner,
    ):
        clean = [env.CheckResult("python version", True, "ok")]
        mock_run_checks.return_value = clean
        mock_recheck_fresh.return_value = [env.CheckResult("fresh recheck", True, "ok", critical=False)]

        with mock.patch("check_environment.sys.argv", ["check_environment.py", "--auto"]):
            result = env.main()

        self.assertEqual(result, 0)
        mock_recheck_fresh.assert_called_once_with(grouped=True)

    @mock.patch("check_environment.print_banner")
    @mock.patch("check_environment.print_results")
    @mock.patch("check_environment.print_next_steps")
    @mock.patch("check_environment.recheck_after_repair_fresh")
    @mock.patch("check_environment.run_fix", return_value=0)
    @mock.patch("check_environment.run_checks")
    def test_main_returns_nonzero_for_post_repair_critical_failure(
        self,
        mock_run_checks,
        _mock_run_fix,
        mock_recheck_fresh,
        _mock_next_steps,
        _mock_print_results,
        _mock_banner,
    ):
        clean = [env.CheckResult("python version", True, "ok")]
        failed = [env.CheckResult("satpy", False, "missing")]
        mock_run_checks.return_value = clean
        mock_recheck_fresh.return_value = failed

        with mock.patch("check_environment.sys.argv", ["check_environment.py", "--fix"]):
            result = env.main()

        self.assertEqual(result, 1)

    @mock.patch("check_environment.metadata.version", return_value="0.59.0")
    def test_satpy_version_requires_minimum(self, _mock_version):
        result = env.check_satpy_version()

        self.assertFalse(result.ok)
        self.assertIn("older", result.detail)

    @mock.patch("check_environment.subprocess.call", return_value=0)
    @mock.patch("check_environment.check_pip_available", return_value=env.CheckResult("pip repair tool", True, "pip ok"))
    @mock.patch("check_environment.run_local_repair_tasks")
    @mock.patch("check_environment.archive_unused_programs")
    @mock.patch("check_environment.install_overlay_data")
    def test_run_fix_invokes_pip_installs_overlay_data_and_archives_cleanup(
        self,
        mock_overlay_install,
        mock_archive,
        mock_local_repairs,
        _mock_pip,
        mock_call,
    ):
        mock_overlay_install.return_value = env.OverlayInstallResult(True, True, "ok")
        mock_archive.return_value = env.CheckResult("archive unused programs", True, "archived", critical=False)
        mock_local_repairs.return_value = [env.CheckResult("support folders", True, "ok", critical=False)]

        with mock.patch("sys.stdout", new_callable=StringIO):
            result = env.run_fix()

        self.assertEqual(result, 0)
        mock_call.assert_called_once_with(env.pip_install_command(upgrade=True), cwd=env.PROJECT_DIR)
        mock_local_repairs.assert_called_once_with(env.PROJECT_DIR)
        mock_overlay_install.assert_called_once_with(open_folder=True, force=False)
        mock_archive.assert_called_once_with(env.PROJECT_DIR)

    @mock.patch("check_environment.subprocess.call", return_value=0)
    @mock.patch("check_environment.check_pip_available", return_value=env.CheckResult("pip repair tool", True, "pip ok"))
    @mock.patch("check_environment.run_local_repair_tasks")
    @mock.patch("check_environment.archive_unused_programs")
    @mock.patch("check_environment.ensure_overlay_folder")
    @mock.patch("check_environment.install_overlay_data")
    def test_run_fix_can_skip_overlay_data(
        self,
        mock_overlay_install,
        mock_overlay_folder,
        mock_archive,
        mock_local_repairs,
        _mock_pip,
        _mock_call,
    ):
        mock_archive.return_value = env.CheckResult("archive unused programs", True, "archived", critical=False)
        mock_local_repairs.return_value = [env.CheckResult("support folders", True, "ok", critical=False)]

        with mock.patch("sys.stdout", new_callable=StringIO):
            result = env.run_fix(install_overlays=False)

        self.assertEqual(result, 0)
        mock_overlay_install.assert_not_called()
        mock_overlay_folder.assert_called_once_with(open_folder=True)
        mock_local_repairs.assert_called_once_with(env.PROJECT_DIR)
        mock_archive.assert_called_once_with(env.PROJECT_DIR)

    @mock.patch("check_environment.subprocess.call", return_value=0)
    @mock.patch("check_environment.check_pip_available", return_value=env.CheckResult("pip repair tool", True, "pip ok"))
    @mock.patch("check_environment.run_local_repair_tasks")
    @mock.patch("check_environment.archive_unused_programs")
    @mock.patch("check_environment.install_overlay_data")
    def test_run_fix_returns_failure_when_overlay_install_fails(
        self,
        mock_overlay_install,
        mock_archive,
        mock_local_repairs,
        _mock_pip,
        _mock_call,
    ):
        mock_overlay_install.return_value = env.OverlayInstallResult(False, False, "download failed")
        mock_local_repairs.return_value = [env.CheckResult("support folders", True, "ok", critical=False)]

        result = env.run_fix()

        self.assertEqual(result, 1)
        mock_local_repairs.assert_called_once_with(env.PROJECT_DIR)
        mock_overlay_install.assert_called_once_with(open_folder=True, force=False)
        mock_archive.assert_not_called()

    @mock.patch("check_environment.subprocess.call", return_value=0)
    @mock.patch("check_environment.check_pip_available", return_value=env.CheckResult("pip repair tool", True, "pip ok"))
    @mock.patch("check_environment.run_local_repair_tasks")
    @mock.patch("check_environment.archive_unused_programs")
    @mock.patch("check_environment.install_overlay_data")
    def test_run_fix_can_force_overlay_data_install(
        self,
        mock_overlay_install,
        mock_archive,
        mock_local_repairs,
        _mock_pip,
        _mock_call,
    ):
        mock_overlay_install.return_value = env.OverlayInstallResult(True, True, "ok")
        mock_archive.return_value = env.CheckResult("archive unused programs", True, "archived", critical=False)
        mock_local_repairs.return_value = [env.CheckResult("support folders", True, "ok", critical=False)]

        with mock.patch("sys.stdout", new_callable=StringIO):
            result = env.run_fix(force_overlay_data=True)

        self.assertEqual(result, 0)
        mock_local_repairs.assert_called_once_with(env.PROJECT_DIR)
        mock_overlay_install.assert_called_once_with(open_folder=True, force=True)
        mock_archive.assert_called_once_with(env.PROJECT_DIR)

    @mock.patch("check_environment.subprocess.call", return_value=0)
    @mock.patch("check_environment.check_pip_available", return_value=env.CheckResult("pip repair tool", True, "pip ok"))
    @mock.patch("check_environment.run_local_repair_tasks")
    @mock.patch("check_environment.archive_unused_programs")
    @mock.patch("check_environment.install_overlay_data")
    def test_run_fix_can_skip_archive_cleanup(
        self,
        mock_overlay_install,
        mock_archive,
        mock_local_repairs,
        _mock_pip,
        _mock_call,
    ):
        mock_overlay_install.return_value = env.OverlayInstallResult(True, True, "ok")
        mock_local_repairs.return_value = [env.CheckResult("support folders", True, "ok", critical=False)]

        result = env.run_fix(archive_unused=False)

        self.assertEqual(result, 0)
        mock_local_repairs.assert_called_once_with(env.PROJECT_DIR)
        mock_archive.assert_not_called()

    @mock.patch("check_environment.subprocess.call", return_value=0)
    @mock.patch("check_environment.check_pip_available", return_value=env.CheckResult("pip repair tool", True, "pip ok"))
    def test_run_gpu_fix_invokes_gpu_requirements(self, _mock_pip, mock_call):
        result = env.run_gpu_fix()

        self.assertEqual(result, 0)
        mock_call.assert_called_once_with(env.gpu_pip_install_command(upgrade=True), cwd=env.PROJECT_DIR)

    @mock.patch("check_environment.subprocess.call")
    @mock.patch("check_environment.check_pip_available", return_value=env.CheckResult("pip repair tool", False, "missing pip"))
    @mock.patch("check_environment.run_local_repair_tasks")
    def test_run_fix_stops_when_pip_is_unavailable(self, mock_local_repairs, _mock_pip, mock_call):
        result = env.run_fix()

        self.assertEqual(result, 1)
        mock_call.assert_not_called()
        mock_local_repairs.assert_not_called()

    @mock.patch("check_environment.run_fix", return_value=0)
    @mock.patch("check_environment.run_environment_check_with", return_value=0)
    @mock.patch("check_environment.venv_python_path")
    def test_auto_fix_uses_existing_venv_for_unsupported_python(
        self,
        mock_venv_path,
        mock_check_repaired,
        mock_run_fix,
    ):
        python_path = mock.Mock()
        python_path.exists.return_value = True
        mock_venv_path.return_value = python_path
        results = [env.CheckResult("python version", False, "unsupported")]

        result = env.run_auto_fix(results)

        self.assertEqual(result, 0)
        mock_run_fix.assert_called_once_with(
            python_path,
            install_overlays=True,
            force_overlay_data=False,
            archive_unused=True,
        )
        mock_check_repaired.assert_called_once_with(python_path)

    @mock.patch("check_environment.run_fix", return_value=0)
    @mock.patch("check_environment.run_environment_check_with", return_value=0)
    @mock.patch("check_environment.create_venv")
    @mock.patch("check_environment.venv_python_path")
    def test_auto_fix_creates_venv_for_unsupported_python(
        self,
        mock_venv_path,
        mock_create_venv,
        mock_check_repaired,
        mock_run_fix,
    ):
        missing_path = mock.Mock()
        missing_path.exists.return_value = False
        created_path = mock.Mock()
        mock_venv_path.return_value = missing_path
        mock_create_venv.return_value = created_path
        results = [env.CheckResult("python version", False, "unsupported")]

        result = env.run_auto_fix(results)

        self.assertEqual(result, 0)
        mock_create_venv.assert_called_once_with()
        mock_run_fix.assert_called_once_with(
            created_path,
            install_overlays=True,
            force_overlay_data=False,
            archive_unused=True,
        )
        mock_check_repaired.assert_called_once_with(created_path)

    @mock.patch("check_environment.satpy_config_file")
    def test_missing_true_color_reproduction_config_is_optional_with_fallback_note(self, mock_config_file):
        reader = mock.Mock()
        reader.exists.return_value = True
        composite = mock.Mock()
        composite.exists.return_value = True
        composite.read_text.return_value = "composites:\n  true_color:\n"
        mock_config_file.side_effect = [reader, composite]

        results = env.check_satpy_ahi_configs()
        reproduction = next(result for result in results if result.name == "AHI true_color_reproduction composite")

        self.assertFalse(reproduction.ok)
        self.assertFalse(reproduction.critical)
        self.assertIn("custom low-RAM fallback", reproduction.detail)

    @mock.patch("check_environment.satpy_compositor_names_for_sensor")
    def test_parsed_registry_reports_true_color_reproduction_available(self, mock_names):
        mock_names.return_value = {"true_color", "true_color_reproduction"}

        result = env.check_satpy_true_color_registry()

        self.assertTrue(result.ok)
        self.assertIn("available", result.detail)

    @mock.patch.dict("check_environment.os.environ", {"SATPY_CONFIG_PATH": "C:/custom/satpy"}, clear=True)
    @mock.patch("check_environment.satpy_compositor_names_for_sensor")
    def test_parsed_registry_warns_when_true_color_reproduction_missing(self, mock_names):
        mock_names.return_value = {"true_color", "true_color_nocorr"}

        result = env.check_satpy_true_color_registry()

        self.assertFalse(result.ok)
        self.assertFalse(result.critical)
        self.assertIn("parsed AHI compositor registry", result.detail)
        self.assertIn("SATPY_CONFIG_PATH=C:/custom/satpy", result.detail)

    @mock.patch.dict("check_environment.os.environ", {}, clear=True)
    def test_satpy_config_environment_ok_without_custom_paths(self):
        result = env.check_satpy_config_environment()

        self.assertTrue(result.ok)
        self.assertFalse(result.critical)
        self.assertIn("no custom Satpy", result.detail)

    @mock.patch.dict("check_environment.os.environ", {"SATPY_CONFIG_PATH": "Z:/missing/satpy"}, clear=True)
    def test_satpy_config_environment_warns_for_missing_path(self):
        result = env.check_satpy_config_environment()

        self.assertFalse(result.ok)
        self.assertFalse(result.critical)
        self.assertIn("SATPY_CONFIG_PATH", result.detail)

    @mock.patch.dict("check_environment.os.environ", {}, clear=True)
    def test_satpy_config_environment_accepts_existing_paths(self):
        with TemporaryDirectory() as tmp_dir:
            with mock.patch.dict("check_environment.os.environ", {"PPP_CONFIG_DIR": tmp_dir}, clear=True):
                result = env.check_satpy_config_environment()

        self.assertTrue(result.ok)
        self.assertIn("PPP_CONFIG_DIR", result.detail)

    def test_satpy_compositor_names_extracts_data_id_name(self):
        with mock.patch(
            "satpy.composites.config_loader.load_compositor_configs_for_sensors",
            return_value=({"ahi": {FakeDataID("true_color_reproduction"): object()}}, {}),
        ):
            names = env.satpy_compositor_names_for_sensor("ahi")

        self.assertIn("true_color_reproduction", names)

    def test_project_true_color_fallback_runtime_catches_missing_satpy_dataset(self):
        result = env.check_project_true_color_fallback_runtime()

        self.assertTrue(result.ok)
        self.assertIn("custom low-RAM fallback", result.detail)
        self.assertIn("true_color", result.detail)
        self.assertIn("true_color_reproduction", result.detail)

    def test_overlay_data_check_reports_missing_expected_layout(self):
        with TemporaryDirectory() as tmp_dir:
            result = env.check_overlay_data(Path(tmp_dir))

        self.assertFalse(result.ok)
        self.assertFalse(result.critical)
        self.assertIn("GSHHS_l_L1.shp", result.detail)
        self.assertIn("WDBII_border_l_L1.dbf", result.detail)

    def test_overlay_data_check_rejects_arbitrary_shape_file(self):
        with TemporaryDirectory() as tmp_dir:
            overlays = Path(tmp_dir) / "overlays"
            overlays.mkdir()
            (overlays / "coast.shp").write_text("fake", encoding="utf-8")

            result = env.check_overlay_data(Path(tmp_dir))

        self.assertFalse(result.ok)
        self.assertIn("GSHHS_l_L1.shp", result.detail)

    def test_overlay_data_check_accepts_required_files(self):
        with TemporaryDirectory() as tmp_dir:
            for path in env.overlay_data_required_paths(Path(tmp_dir)):
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text("fake", encoding="utf-8")
            for path in env.missing_overlay_sidecar_paths(Path(tmp_dir)):
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text("fake", encoding="utf-8")

            result = env.check_overlay_data(Path(tmp_dir))

        self.assertTrue(result.ok)
        self.assertIn("required low-resolution", result.detail)

    def test_overlay_data_check_rejects_empty_required_files(self):
        with TemporaryDirectory() as tmp_dir:
            for path in env.overlay_data_required_paths(Path(tmp_dir)):
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text("", encoding="utf-8")

            result = env.check_overlay_data(Path(tmp_dir))

        self.assertFalse(result.ok)
        self.assertIn("GSHHS_l_L1.shp", result.detail)

    def test_overlay_data_check_reports_missing_sidecar_files(self):
        with TemporaryDirectory() as tmp_dir:
            for path in env.overlay_data_required_paths(Path(tmp_dir)):
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text("fake", encoding="utf-8")

            result = env.check_overlay_data(Path(tmp_dir))

        self.assertFalse(result.ok)
        self.assertIn("sidecar", result.detail)
        self.assertIn("GSHHS_l_L1.shx", result.detail)

    def test_pycoast_overlay_runtime_skips_when_data_missing(self):
        with TemporaryDirectory() as tmp_dir:
            result = env.check_pycoast_overlay_runtime(Path(tmp_dir))

        self.assertFalse(result.ok)
        self.assertIn("skipped", result.detail)

    def test_pycoast_overlay_runtime_reports_missing_package(self):
        with TemporaryDirectory() as tmp_dir:
            for path in env.overlay_data_required_paths(Path(tmp_dir)):
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text("fake", encoding="utf-8")
            for path in env.missing_overlay_sidecar_paths(Path(tmp_dir)):
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text("fake", encoding="utf-8")
            with mock.patch("check_environment.importlib.util.find_spec", return_value=None):
                result = env.check_pycoast_overlay_runtime(Path(tmp_dir))

        self.assertFalse(result.ok)
        self.assertIn("missing overlay package", result.detail)

    def test_pycoast_overlay_runtime_ok_when_imports_available(self):
        with TemporaryDirectory() as tmp_dir:
            for path in env.overlay_data_required_paths(Path(tmp_dir)):
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text("fake", encoding="utf-8")
            for path in env.missing_overlay_sidecar_paths(Path(tmp_dir)):
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text("fake", encoding="utf-8")
            with mock.patch("check_environment.importlib.util.find_spec", return_value=object()):
                with mock.patch.dict("sys.modules", {"pycoast": mock.Mock(), "aggdraw": mock.Mock()}):
                    result = env.check_pycoast_overlay_runtime(Path(tmp_dir))

        self.assertTrue(result.ok)
        self.assertIn("import successfully", result.detail)

    def test_overlay_archive_member_suffixes_include_required_low_resolution_files(self):
        suffixes = env.overlay_archive_member_suffixes()

        self.assertIn("GSHHS_shp/l/GSHHS_l_L1.shp", suffixes)
        self.assertIn("GSHHS_shp/l/GSHHS_l_L1.dbf", suffixes)
        self.assertIn("WDBII_shp/l/WDBII_border_l_L1.shp", suffixes)
        self.assertIn("WDBII_shp/l/WDBII_border_l_L1.dbf", suffixes)
        self.assertIn("GSHHS_shp/l/GSHHS_l_L1.shx", suffixes)

    def test_overlay_archive_urls_use_reachable_soest_mirrors_not_dead_noaa_path(self):
        self.assertNotIn(
            "https://www.ngdc.noaa.gov/mgg/shorelines/data/gshhs/latest/gshhg-shp-2.3.7.zip",
            env.GSHHG_ARCHIVE_URLS,
        )
        self.assertIn("https://www.soest.hawaii.edu/wessel/gshhg/gshhg-shp-2.3.7.zip", env.GSHHG_ARCHIVE_URLS)
        self.assertIn("https://www.soest.hawaii.edu/pwessel/gshhg/gshhg-shp-2.3.7.zip", env.GSHHG_ARCHIVE_URLS)
        self.assertIn("https://ftp.soest.hawaii.edu/gshhg/gshhg-shp-2.3.7.zip", env.GSHHG_ARCHIVE_URLS)

    def test_extract_overlay_archive_installs_required_files_and_sidecars(self):
        with TemporaryDirectory() as tmp_dir:
            project_dir = Path(tmp_dir) / "project"
            archive_path = Path(tmp_dir) / "gshhg.zip"
            with zipfile.ZipFile(archive_path, "w") as archive:
                archive.writestr("gshhg-shp-2.3.7/GSHHS_shp/l/GSHHS_l_L1.shp", "coast shp")
                archive.writestr("gshhg-shp-2.3.7/GSHHS_shp/l/GSHHS_l_L1.dbf", "coast dbf")
                archive.writestr("gshhg-shp-2.3.7/GSHHS_shp/l/GSHHS_l_L1.shx", "coast shx")
                archive.writestr("gshhg-shp-2.3.7/WDBII_shp/l/WDBII_border_l_L1.shp", "border shp")
                archive.writestr("gshhg-shp-2.3.7/WDBII_shp/l/WDBII_border_l_L1.dbf", "border dbf")
                archive.writestr("gshhg-shp-2.3.7/WDBII_shp/l/WDBII_border_l_L1.shx", "border shx")
                archive.writestr("gshhg-shp-2.3.7/WDBII_shp/l/WDBII_border_l_L1.prj", "border prj")
                archive.writestr("gshhg-shp-2.3.7/GSHHS_shp/h/GSHHS_h_L1.shp", "ignored")

            extracted, missing = env.extract_overlay_archive(archive_path, project_dir)

            self.assertEqual(missing, ())
            self.assertEqual(extracted, 7)
            self.assertEqual(
                (project_dir / "overlays" / "GSHHS_shp" / "l" / "GSHHS_l_L1.shx").read_text(),
                "coast shx",
            )
            self.assertFalse((project_dir / "overlays" / "GSHHS_shp" / "h" / "GSHHS_h_L1.shp").exists())

    def test_extract_overlay_archive_reports_missing_required_members(self):
        with TemporaryDirectory() as tmp_dir:
            archive_path = Path(tmp_dir) / "gshhg.zip"
            with zipfile.ZipFile(archive_path, "w") as archive:
                archive.writestr("GSHHS_shp/l/GSHHS_l_L1.shp", "coast shp")

            with self.assertRaisesRegex(RuntimeError, "missing required overlay members"):
                env.extract_overlay_archive(archive_path, Path(tmp_dir) / "project")

    @mock.patch("check_environment.download_file")
    def test_install_overlay_data_reuses_existing_complete_data(self, mock_download):
        with TemporaryDirectory() as tmp_dir:
            project_dir = Path(tmp_dir)
            for path in env.overlay_data_required_paths(project_dir):
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text("fake", encoding="utf-8")
            for path in env.missing_overlay_sidecar_paths(project_dir):
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text("fake", encoding="utf-8")

            with mock.patch("sys.stdout", new_callable=StringIO):
                result = env.install_overlay_data(project_dir, open_folder=False)

        self.assertTrue(result.ok)
        self.assertFalse(result.installed)
        mock_download.assert_not_called()

    def test_install_overlay_data_repairs_missing_sidecars_from_cache(self):
        with TemporaryDirectory() as tmp_dir:
            project_dir = Path(tmp_dir) / "project"
            archive_path = Path(tmp_dir) / "gshhg.zip"
            for path in env.overlay_data_required_paths(project_dir):
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text("fake", encoding="utf-8")
            with zipfile.ZipFile(archive_path, "w") as archive:
                for suffix in env.overlay_archive_member_suffixes():
                    archive.writestr(f"root/{suffix}", suffix)

            with mock.patch("sys.stdout", new_callable=StringIO):
                result = env.install_overlay_data(project_dir, open_folder=False, archive_path=archive_path)

        self.assertTrue(result.ok)
        self.assertTrue(result.installed)
        self.assertFalse(result.missing_paths)

    @mock.patch("check_environment.download_file")
    def test_install_overlay_data_uses_local_archive_when_cache_missing(self, mock_download):
        with TemporaryDirectory() as tmp_dir:
            project_dir = Path(tmp_dir) / "project"
            project_dir.mkdir()
            archive_path = project_dir / env.GSHHG_SHAPEFILE_ARCHIVE
            with zipfile.ZipFile(archive_path, "w") as archive:
                for suffix in env.overlay_archive_member_suffixes():
                    archive.writestr(f"root/{suffix}", suffix)
            missing_cache = Path(tmp_dir) / "cache" / env.GSHHG_SHAPEFILE_ARCHIVE

            with mock.patch("sys.stdout", new_callable=StringIO):
                result = env.install_overlay_data(project_dir, open_folder=False, archive_path=missing_cache)

        self.assertTrue(result.ok)
        self.assertTrue(result.installed)
        mock_download.assert_not_called()

    def test_local_overlay_archive_candidates_include_friend_fallback_locations(self):
        with TemporaryDirectory() as tmp_dir:
            project_dir = Path(tmp_dir) / "project"
            cache_dir = Path(tmp_dir) / "cache"
            candidates = env.local_overlay_archive_candidates(project_dir, cache_dir=cache_dir)

        self.assertIn(project_dir / env.GSHHG_SHAPEFILE_ARCHIVE, candidates)
        self.assertIn(project_dir / "overlays" / env.GSHHG_SHAPEFILE_ARCHIVE, candidates)
        self.assertIn(cache_dir / env.GSHHG_SHAPEFILE_ARCHIVE, candidates)

    @mock.patch("check_environment.download_file", return_value=(False, "network down"))
    def test_install_overlay_data_reports_download_failure(self, mock_download):
        with TemporaryDirectory() as tmp_dir:
            archive_path = Path(tmp_dir) / "missing-gshhg.zip"
            with mock.patch("sys.stdout", new_callable=StringIO):
                with mock.patch("sys.stderr", new_callable=StringIO) as stderr:
                    result = env.install_overlay_data(
                        Path(tmp_dir),
                        open_folder=False,
                        archive_path=archive_path,
                        urls=("https://example.test/gshhg.zip",),
                    )

        self.assertFalse(result.ok)
        self.assertIn(env.GSHHG_SHAPEFILE_ARCHIVE, result.detail)
        self.assertIn("Every attempted mirror failed", result.detail)
        self.assertIn("place gshhg-shp-2.3.7.zip", result.detail)
        self.assertIn("GSHHS_l_L1.shp", result.detail)
        self.assertIn("Could not download required overlay data archive", stderr.getvalue())
        mock_download.assert_called_once()

    def test_install_overlay_data_extracts_cached_archive(self):
        with TemporaryDirectory() as tmp_dir:
            project_dir = Path(tmp_dir) / "project"
            archive_path = Path(tmp_dir) / "gshhg.zip"
            with zipfile.ZipFile(archive_path, "w") as archive:
                for suffix in env.overlay_archive_member_suffixes():
                    archive.writestr(f"root/{suffix}", suffix)

            with mock.patch("sys.stdout", new_callable=StringIO):
                result = env.install_overlay_data(project_dir, open_folder=False, archive_path=archive_path)

        self.assertTrue(result.ok)
        self.assertTrue(result.installed)
        self.assertFalse(result.missing_paths)

    @mock.patch("check_environment.open_path_in_file_manager")
    def test_ensure_overlay_folder_creates_folder_and_can_open_on_windows(self, mock_open_path):
        with TemporaryDirectory() as tmp_dir:
            with mock.patch("sys.stdout", new_callable=StringIO):
                overlays_dir = env.ensure_overlay_folder(Path(tmp_dir), open_folder=True)
                self.assertTrue(overlays_dir.exists())

        mock_open_path.assert_called_once_with(overlays_dir)

    def test_launcher_helpers_ok_when_expected_scripts_are_referenced(self):
        with TemporaryDirectory() as tmp_dir:
            project_dir = Path(tmp_dir)
            for helper, script in env.LAUNCHER_HELPERS.items():
                (project_dir / helper).write_text(f'python "{script}" %*\n', encoding="utf-8")

            result = env.check_launcher_helpers(project_dir)

        self.assertTrue(result.ok)
        self.assertFalse(result.critical)

    def test_launcher_helpers_warn_for_missing_or_bad_helpers(self):
        with TemporaryDirectory() as tmp_dir:
            project_dir = Path(tmp_dir)
            (project_dir / "himawari.bat").write_text("python wrong.py\n", encoding="utf-8")

            result = env.check_launcher_helpers(project_dir)

        self.assertFalse(result.ok)
        self.assertFalse(result.critical)
        self.assertIn("himawari.bat does not reference", result.detail)

    def test_check_writable_folder_target_accepts_missing_child_with_writable_parent(self):
        with TemporaryDirectory() as tmp_dir:
            target = Path(tmp_dir) / "outputs"

            result = env.check_writable_folder_target("default output folder", target)

        self.assertTrue(result.ok)
        self.assertIn("can be created", result.detail)
        self.assertFalse(target.exists())

    def test_check_writable_folder_target_fails_for_file_path(self):
        with TemporaryDirectory() as tmp_dir:
            target = Path(tmp_dir) / "outputs"
            target.write_text("not a folder", encoding="utf-8")

            result = env.check_writable_folder_target("default output folder", target)

        self.assertFalse(result.ok)
        self.assertIn("not a folder", result.detail)

    @mock.patch("check_environment.tempfile.NamedTemporaryFile", side_effect=PermissionError("denied"))
    def test_check_writable_folder_target_reports_permission_error(self, _mock_tempfile):
        with TemporaryDirectory() as tmp_dir:
            result = env.check_writable_folder_target("default temp folder", Path(tmp_dir))

        self.assertFalse(result.ok)
        self.assertIn("PermissionError", result.detail)

    @mock.patch.object(env.Path, "resolve", autospec=True)
    def test_cloud_sync_location_warns_for_onedrive_path(self, mock_resolve):
        def fake_resolve(path, strict=False):
            return Path("C:/Users/Isaac/OneDrive/Desktop/project")

        mock_resolve.side_effect = fake_resolve

        result = env.check_cloud_sync_locations([Path("C:/Users/Isaac/OneDrive/Desktop/project")])

        self.assertFalse(result.ok)
        self.assertFalse(result.critical)
        self.assertIn("cloud-sync", result.detail)
        self.assertIn("OneDrive", result.detail)
        self.assertEqual(env.result_group(env.CheckResult("cloud sync location", False, "OneDrive", critical=False)), "Paths")

    @mock.patch.object(env.Path, "resolve", autospec=True)
    def test_default_cloud_sync_check_ignores_synced_project_when_write_paths_are_local(self, mock_resolve):
        def fake_resolve(path, strict=False):
            text = str(path)
            if "Himawari9LowRamProcessor" in text:
                return Path("C:/Users/Isaac/AppData/Local/Himawari9LowRamProcessor")
            return Path("C:/Users/Isaac/OneDrive/Desktop/project")

        mock_resolve.side_effect = fake_resolve
        fake_app = mock.Mock(
            OUTPUT_DIR=Path("C:/Users/Isaac/AppData/Local/Himawari9LowRamProcessor/outputs"),
            TEMP_DIR=Path("C:/Users/Isaac/AppData/Local/Himawari9LowRamProcessor/temp"),
        )

        with mock.patch.dict("sys.modules", {"himawari_lowram_processor": fake_app}):
            result = env.check_cloud_sync_locations()

        self.assertTrue(result.ok)

    @mock.patch.object(env.Path, "resolve", autospec=True)
    def test_cloud_sync_location_accepts_local_path(self, mock_resolve):
        def fake_resolve(path, strict=False):
            return Path("C:/Himawari/project")

        mock_resolve.side_effect = fake_resolve

        result = env.check_cloud_sync_locations([Path("C:/Himawari/project")])

        self.assertTrue(result.ok)


if __name__ == "__main__":
    unittest.main()
