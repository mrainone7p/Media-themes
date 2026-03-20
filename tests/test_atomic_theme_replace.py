from __future__ import annotations

import sys
import tempfile
import types
import unittest
from pathlib import Path

sys.modules.setdefault("yaml", types.SimpleNamespace(safe_load=lambda *args, **kwargs: {}, safe_dump=lambda *args, **kwargs: ""))
sys.modules.setdefault("requests", types.SimpleNamespace(get=lambda *args, **kwargs: None, post=lambda *args, **kwargs: None))

from shared import file_utils


class AtomicThemeReplaceTests(unittest.TestCase):
    def test_atomic_replace_swaps_in_new_file(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            folder = Path(tmpdir)
            destination = folder / "theme.mp3"
            prepared = folder / "theme.trim.mp3"
            destination.write_text("old")
            prepared.write_text("new")

            replaced_existing = file_utils.atomic_replace_file(prepared, destination)

            self.assertTrue(replaced_existing)
            self.assertEqual("new", destination.read_text())
            self.assertFalse(prepared.exists())

    def test_atomic_replace_restores_original_when_new_file_is_missing(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            folder = Path(tmpdir)
            destination = folder / "theme.mp3"
            prepared = folder / "theme.trim.mp3"
            destination.write_text("old")

            with self.assertRaises(FileNotFoundError):
                file_utils.atomic_replace_file(prepared, destination)

            self.assertTrue(destination.exists())
            self.assertEqual("old", destination.read_text())


if __name__ == "__main__":
    unittest.main()
