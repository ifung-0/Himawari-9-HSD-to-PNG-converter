import unittest
from unittest import mock

import check_environment as env


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

    def test_pip_install_command_uses_current_python_and_requirements(self):
        command = env.pip_install_command(upgrade=True)

        self.assertEqual(command[0], env.sys.executable)
        self.assertIn("--upgrade", command)
        self.assertEqual(command[-2], "-r")
        self.assertEqual(command[-1], str(env.REQUIREMENTS_FILE))

    def test_supported_python_version_accepts_312_and_313(self):
        self.assertTrue(env.supported_python_version((3, 12)))
        self.assertTrue(env.supported_python_version((3, 13)))
        self.assertFalse(env.supported_python_version((3, 14)))

    def test_venv_python_path_uses_project_venv(self):
        path = env.venv_python_path()

        self.assertIn(".venv", str(path))
        self.assertTrue(str(path).endswith("python.exe") or str(path).endswith("python"))

    def test_has_critical_failures_ignores_optional_warnings(self):
        results = [
            env.CheckResult("optional", False, "missing", critical=False),
            env.CheckResult("critical", True, "ok", critical=True),
        ]
        self.assertFalse(env.has_critical_failures(results))

        results.append(env.CheckResult("critical missing", False, "missing", critical=True))
        self.assertTrue(env.has_critical_failures(results))

    @mock.patch("check_environment.metadata.version", return_value="0.59.0")
    def test_satpy_version_requires_minimum(self, _mock_version):
        result = env.check_satpy_version()

        self.assertFalse(result.ok)
        self.assertIn("older", result.detail)

    @mock.patch("check_environment.subprocess.call", return_value=0)
    def test_run_fix_invokes_pip(self, mock_call):
        result = env.run_fix()

        self.assertEqual(result, 0)
        mock_call.assert_called_once_with(env.pip_install_command(upgrade=True), cwd=env.PROJECT_DIR)

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
        mock_run_fix.assert_called_once_with(python_path)
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
        mock_run_fix.assert_called_once_with(created_path)
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


if __name__ == "__main__":
    unittest.main()
