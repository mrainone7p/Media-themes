from __future__ import annotations

import sys
import tempfile
import types
import unittest
from pathlib import Path

sys.modules.setdefault("yaml", types.SimpleNamespace(safe_load=lambda *args, **kwargs: {}, safe_dump=lambda *args, **kwargs: ""))
sys.modules.setdefault("requests", types.SimpleNamespace(get=lambda *args, **kwargs: None, post=lambda *args, **kwargs: None))

import web.integrations as integrations


class CleanupTempDownloadsTests(unittest.TestCase):
    def test_cleanup_temp_downloads_removes_matching_temp_files(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            folder = Path(tmpdir)
            keep = folder / "keep.me"
            download_tmp = folder / "mt_tmp_demo.mp3"
            trim_tmp = folder / "mt_trim_demo.mp3"
            keep.write_text("keep", encoding="utf-8")
            download_tmp.write_text("tmp", encoding="utf-8")
            trim_tmp.write_text("trim", encoding="utf-8")

            integrations.cleanup_temp_downloads(folder, "demo")

            self.assertFalse(download_tmp.exists())
            self.assertFalse(trim_tmp.exists())
            self.assertTrue(keep.exists())


if __name__ == "__main__":
    unittest.main()
