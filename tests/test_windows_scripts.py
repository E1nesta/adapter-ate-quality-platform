from pathlib import Path
import unittest


class WindowsScriptTests(unittest.TestCase):
    def script_text(self, name):
        return Path("scripts", name).read_text(encoding="utf-8")

    def test_windows_demo_scripts_are_present(self):
        for name in [
            "bootstrap_demo.ps1",
            "run_mvp_demo.ps1",
            "run_extension_demo.ps1",
            "start_api.ps1",
        ]:
            self.assertTrue(Path("scripts", name).exists(), name)

    def test_bootstrap_uses_windows_virtual_environment(self):
        text = self.script_text("bootstrap_demo.ps1")

        self.assertIn(".venv-win", text)
        self.assertIn("requirements.txt", text)
        self.assertIn("run_extension_demo.ps1", text)

    def test_mvp_script_prefers_msvc_and_falls_back_to_mingw(self):
        text = self.script_text("run_mvp_demo.ps1")

        self.assertIn("cl.exe", text)
        self.assertIn("/std:c++17", text)
        self.assertIn("g++", text)
        self.assertIn("ate_line_simulator.exe", text)
        self.assertIn("adapter_ate.processor", text)
        self.assertIn("adapter_ate.reports", text)

    def test_extension_script_runs_model_and_api_smoke(self):
        text = self.script_text("run_extension_demo.ps1")

        self.assertIn("adapter_ate.ai_model", text)
        self.assertIn("scripts\\api_smoke.py", text)
        self.assertIn("adapter_ate.storage", text)

    def test_start_api_defaults_to_csv_data_source(self):
        text = self.script_text("start_api.ps1")

        self.assertIn("ATE_DATA_SOURCE", text)
        self.assertIn("csv", text)
        self.assertIn("adapter_ate.api", text)
        self.assertIn("127.0.0.1", text)
        self.assertIn("5000", text)


if __name__ == "__main__":
    unittest.main()
