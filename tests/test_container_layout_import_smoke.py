from __future__ import annotations

import os
import shutil
import subprocess
import sys
import tempfile
import textwrap
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


class ContainerLayoutImportSmokeTests(unittest.TestCase):
    def test_logic_imports_cleanly_from_container_style_app_layout(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            app_root = Path(tmpdir) / "app"
            shutil.copytree(ROOT / "web", app_root / "web")
            shutil.copytree(ROOT / "shared", app_root / "shared")

            (app_root / "yaml.py").write_text(
                "safe_load = lambda *args, **kwargs: {}\n"
                "safe_dump = lambda *args, **kwargs: ''\n"
                "dump = lambda *args, **kwargs: ''\n",
                encoding="utf-8",
            )
            (app_root / "requests.py").write_text(
                textwrap.dedent(
                    """
                    class _Response:
                        status_code = 200
                        headers = {}
                        content = b""

                        def raise_for_status(self):
                            return None

                        def json(self):
                            return {}


                    def get(*args, **kwargs):
                        return _Response()


                    def post(*args, **kwargs):
                        return _Response()
                    """
                ).strip()
                + "\n",
                encoding="utf-8",
            )

            result = subprocess.run(
                [
                    sys.executable,
                    "-c",
                    (
                        "from web import logic, run_logic; "
                        "print(logic.__file__); "
                        "print(run_logic.__file__)"
                    ),
                ],
                capture_output=True,
                text=True,
                cwd=app_root,
                env={**os.environ, "PYTHONPATH": str(app_root)},
                check=True,
            )

            self.assertIn(str(app_root / "web" / "logic.py"), result.stdout)
            self.assertIn(str(app_root / "web" / "run_logic.py"), result.stdout)


if __name__ == "__main__":
    unittest.main()
