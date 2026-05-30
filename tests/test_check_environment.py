import unittest
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
            env.CheckResult("satpy", False, "missing", critical=True),
            env.CheckResult("default output folder", True, "ok"),
        ]

        self.assertEqual(env.result_group(results[0]), "Python")
        self.assertEqual(env.result_group(results[1]), "Packages")
        self.assertEqual(env.result_group(results[3]), "Paths")
        self.assertEqual(env.result_counts(results), (2, 1, 1))
        self.assertEqual(env.status_label(results[0]), "OK")
        self.assertEqual(env.status_label(results[1]), "WARN")
        self.assertEqual(env.status_label(results[2]), "FAIL")

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

    def test_print_next_steps_for_ready_environment_mentions_launchers(self):
        results = [env.CheckResult("python version", True, "ok")]

        with mock.patch("sys.stdout", new_callable=StringIO) as stdout:
            env.print_next_steps(results)

        output = stdout.getvalue()
        self.assertIn("Environment looks ready", output)
        self.assertIn("run_gui.bat", output)
        self.assertIn("run_cli.bat", output)

    def test_final_status_code_fails_only_for_critical_failures(self):
        self.assertEqual(env.final_status_code([env.CheckResult("optional", False, "warn", critical=False)]), 0)
        self.assertEqual(env.final_status_code([env.CheckResult("critical", False, "fail")]), 1)

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
        mock_run_fix.assert_called_once_with(install_overlays=True, force_overlay_data=False)
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
        mock_auto_fix.assert_called_once_with(clean, install_overlays=False, force_overlay_data=True)

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
    @mock.patch("check_environment.install_overlay_data")
    def test_run_fix_invokes_pip_and_installs_overlay_data(self, mock_overlay_install, _mock_pip, mock_call):
        mock_overlay_install.return_value = env.OverlayInstallResult(True, True, "ok")

        result = env.run_fix()

        self.assertEqual(result, 0)
        mock_call.assert_called_once_with(env.pip_install_command(upgrade=True), cwd=env.PROJECT_DIR)
        mock_overlay_install.assert_called_once_with(open_folder=True, force=False)

    @mock.patch("check_environment.subprocess.call", return_value=0)
    @mock.patch("check_environment.check_pip_available", return_value=env.CheckResult("pip repair tool", True, "pip ok"))
    @mock.patch("check_environment.ensure_overlay_folder")
    @mock.patch("check_environment.install_overlay_data")
    def test_run_fix_can_skip_overlay_data(self, mock_overlay_install, mock_overlay_folder, _mock_pip, _mock_call):
        result = env.run_fix(install_overlays=False)

        self.assertEqual(result, 0)
        mock_overlay_install.assert_not_called()
        mock_overlay_folder.assert_called_once_with(open_folder=True)

    @mock.patch("check_environment.subprocess.call", return_value=0)
    @mock.patch("check_environment.check_pip_available", return_value=env.CheckResult("pip repair tool", True, "pip ok"))
    @mock.patch("check_environment.install_overlay_data")
    def test_run_fix_returns_failure_when_overlay_install_fails(self, mock_overlay_install, _mock_pip, _mock_call):
        mock_overlay_install.return_value = env.OverlayInstallResult(False, False, "download failed")

        result = env.run_fix()

        self.assertEqual(result, 1)
        mock_overlay_install.assert_called_once_with(open_folder=True, force=False)

    @mock.patch("check_environment.subprocess.call", return_value=0)
    @mock.patch("check_environment.check_pip_available", return_value=env.CheckResult("pip repair tool", True, "pip ok"))
    @mock.patch("check_environment.install_overlay_data")
    def test_run_fix_can_force_overlay_data_install(self, mock_overlay_install, _mock_pip, _mock_call):
        mock_overlay_install.return_value = env.OverlayInstallResult(True, True, "ok")

        result = env.run_fix(force_overlay_data=True)

        self.assertEqual(result, 0)
        mock_overlay_install.assert_called_once_with(open_folder=True, force=True)

    @mock.patch("check_environment.subprocess.call")
    @mock.patch("check_environment.check_pip_available", return_value=env.CheckResult("pip repair tool", False, "missing pip"))
    def test_run_fix_stops_when_pip_is_unavailable(self, _mock_pip, mock_call):
        result = env.run_fix()

        self.assertEqual(result, 1)
        mock_call.assert_not_called()

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
        self.assertIn("Attempted URLs", result.detail)
        self.assertIn("GSHHS_l_L1.shp", result.detail)
        self.assertIn("download overlay data", stderr.getvalue())
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

    @mock.patch("check_environment.os.startfile", create=True)
    def test_ensure_overlay_folder_creates_folder_and_can_open_on_windows(self, mock_startfile):
        with TemporaryDirectory() as tmp_dir:
            with mock.patch("check_environment.os.name", "nt"):
                with mock.patch("sys.stdout", new_callable=StringIO):
                    overlays_dir = env.ensure_overlay_folder(Path(tmp_dir), open_folder=True)
                    self.assertTrue(overlays_dir.exists())

        mock_startfile.assert_called_once_with(str(overlays_dir))

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
            (project_dir / "run_gui.bat").write_text("python wrong.py\n", encoding="utf-8")
            (project_dir / "run_cli.bat").write_text("python himawari_cli.py\n", encoding="utf-8")

            result = env.check_launcher_helpers(project_dir)

        self.assertFalse(result.ok)
        self.assertFalse(result.critical)
        self.assertIn("run_gui.bat does not reference", result.detail)
        self.assertIn("missing check_environment.bat", result.detail)

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
