import unittest
from unittest import mock

import check_environment as env


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


if __name__ == "__main__":
    unittest.main()
