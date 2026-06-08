import unittest
from unittest import mock

import check_environment
import install_requirements as installer


class InstallRequirementsTests(unittest.TestCase):
    def test_build_command_can_upgrade(self):
        command = installer.build_command(upgrade=True)

        self.assertEqual(command[0], check_environment.sys.executable)
        self.assertIn("--upgrade", command)
        self.assertEqual(command[-2], "-r")
        self.assertEqual(command[-1], str(installer.REQUIREMENTS_FILE))

    @mock.patch("install_requirements.check_environment.check_pip_available")
    @mock.patch("install_requirements.subprocess.call", return_value=0)
    @mock.patch("install_requirements.check_environment.install_overlay_data")
    def test_main_installs_overlay_data_after_packages(self, mock_overlay_install, mock_call, mock_pip):
        mock_overlay_install.return_value = check_environment.OverlayInstallResult(True, True, "ok")
        mock_pip.return_value = check_environment.CheckResult("pip repair tool", True, "pip ready")

        with mock.patch("sys.argv", ["install_requirements.py", "--upgrade"]):
            result = installer.main()

        self.assertEqual(result, 0)
        mock_call.assert_called_once_with(installer.build_command(True), cwd=installer.PROJECT_DIR)
        mock_pip.assert_called_once_with()
        mock_overlay_install.assert_called_once_with(open_folder=False, force=False)

    @mock.patch("install_requirements.check_environment.check_pip_available")
    @mock.patch("install_requirements.subprocess.call", return_value=0)
    @mock.patch("install_requirements.check_environment.ensure_overlay_folder")
    @mock.patch("install_requirements.check_environment.install_overlay_data")
    def test_main_can_skip_overlay_data(self, mock_overlay_install, mock_overlay_folder, _mock_call, mock_pip):
        mock_pip.return_value = check_environment.CheckResult("pip repair tool", True, "pip ready")

        with mock.patch("sys.argv", ["install_requirements.py", "--skip-overlay-data"]):
            result = installer.main()

        self.assertEqual(result, 0)
        mock_overlay_install.assert_not_called()
        mock_overlay_folder.assert_called_once_with(open_folder=False)

    @mock.patch("install_requirements.check_environment.check_pip_available")
    @mock.patch("install_requirements.subprocess.call", return_value=0)
    @mock.patch("install_requirements.check_environment.install_overlay_data")
    def test_main_returns_failure_when_overlay_install_fails(self, mock_overlay_install, _mock_call, mock_pip):
        mock_overlay_install.return_value = check_environment.OverlayInstallResult(False, False, "failed")
        mock_pip.return_value = check_environment.CheckResult("pip repair tool", True, "pip ready")

        with mock.patch("sys.argv", ["install_requirements.py"]):
            result = installer.main()

        self.assertEqual(result, 1)

    @mock.patch("install_requirements.check_environment.check_pip_available")
    @mock.patch("install_requirements.subprocess.call", return_value=0)
    @mock.patch("install_requirements.check_environment.install_overlay_data")
    def test_main_can_force_overlay_reinstall(self, mock_overlay_install, _mock_call, mock_pip):
        mock_overlay_install.return_value = check_environment.OverlayInstallResult(True, True, "ok")
        mock_pip.return_value = check_environment.CheckResult("pip repair tool", True, "pip ready")

        with mock.patch("sys.argv", ["install_requirements.py", "--force-overlay-data"]):
            result = installer.main()

        self.assertEqual(result, 0)
        mock_overlay_install.assert_called_once_with(open_folder=False, force=True)

    @mock.patch("install_requirements.check_environment.check_pip_available")
    @mock.patch("install_requirements.subprocess.call")
    @mock.patch("install_requirements.check_environment.install_overlay_data")
    def test_main_returns_failure_when_pip_missing(self, mock_overlay_install, mock_call, mock_pip):
        mock_pip.return_value = check_environment.CheckResult("pip repair tool", False, "missing pip")

        with mock.patch("sys.argv", ["install_requirements.py"]):
            result = installer.main()

        self.assertEqual(result, 1)
        mock_call.assert_not_called()
        mock_overlay_install.assert_not_called()

    def test_main_returns_failure_when_requirements_file_missing(self):
        with mock.patch("install_requirements.REQUIREMENTS_FILE", installer.Path("missing-requirements.txt")):
            with mock.patch("sys.argv", ["install_requirements.py"]):
                result = installer.main()

        self.assertEqual(result, 1)


if __name__ == "__main__":
    unittest.main()
