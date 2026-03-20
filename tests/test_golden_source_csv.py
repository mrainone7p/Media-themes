from __future__ import annotations

import sys
import types
import unittest

sys.modules.setdefault("yaml", types.SimpleNamespace(safe_load=lambda *args, **kwargs: {}, safe_dump=lambda *args, **kwargs: ""))
sys.modules.setdefault("requests", types.SimpleNamespace(get=lambda *args, **kwargs: None, post=lambda *args, **kwargs: None))

import script.media_tracks as media_tracks
import shared.golden_source_csv as golden_source_csv
import web.logic as logic


class GoldenSourceSharedParserTests(unittest.TestCase):
    SAMPLE_CSV = """ tmdb_id , title , year , source_url , start_offset , end_offset , updated_at , notes , verified\n123,Example Movie,1999,https://example.test/theme, 1:02 ,15,2026-03-20,Ready,yes\n456,Needs URL,2001,,7,,2026-03-20,Pending,no\n,Missing Id,2005,https://example.test/skip,0,0,2026-03-20,Skip,yes\n"""

    def test_shared_parser_normalizes_headers_offsets_and_legacy_fields(self):
        rows = golden_source_csv.parse_golden_source_csv_rows(self.SAMPLE_CSV)

        self.assertEqual(2, len(rows))
        self.assertEqual("123", rows[0]["tmdb_id"])
        self.assertEqual("1:02", rows[0]["start_offset"])
        self.assertEqual("15", rows[0]["end_offset"])
        self.assertEqual("", rows[1]["source_url"])
        self.assertNotIn("verified", rows[0])

    def test_web_parser_keeps_blank_source_rows_for_import_summary(self):
        rows = logic.parse_golden_source_csv(self.SAMPLE_CSV)

        self.assertEqual(2, len(rows))
        self.assertEqual("", rows[1]["source_url"])
        self.assertEqual("0", rows[1]["end_offset"])

    def test_worker_parser_requires_source_urls_for_matching_catalog(self):
        rows = media_tracks._parse_golden_source_text(self.SAMPLE_CSV)

        self.assertEqual(["123"], list(rows.keys()))
        self.assertEqual("https://example.test/theme", rows["123"]["source_url"])
        self.assertEqual("15", rows["123"]["end_offset"])
        self.assertEqual("123", rows["123"]["tmdb_id"])


if __name__ == "__main__":
    unittest.main()
