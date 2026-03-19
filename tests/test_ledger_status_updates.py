from __future__ import annotations

import sys
import types
import unittest
from pathlib import Path

sys.modules.setdefault("yaml", types.SimpleNamespace(safe_load=lambda *args, **kwargs: {}, safe_dump=lambda *args, **kwargs: ""))
sys.modules.setdefault("requests", types.SimpleNamespace(get=lambda *args, **kwargs: None, post=lambda *args, **kwargs: None))

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT / 'web') not in sys.path:
    sys.path.insert(0, str(ROOT / 'web'))
if str(ROOT / 'shared') not in sys.path:
    sys.path.insert(0, str(ROOT / 'shared'))

import logic


class SaveLedgerRowUpdatesTests(unittest.TestCase):
    def test_missing_to_approved_uses_original_status_for_validation(self):
        row = {
            'rating_key': '1',
            'title': 'Example',
            'status': 'MISSING',
            'url': '',
            'theme_exists': '0',
            'notes': '',
        }

        saved_row, error = logic.save_ledger_row_updates(row, {
            'url': 'https://example.com/theme',
            'status': 'APPROVED',
        })

        self.assertIsNone(saved_row)
        self.assertEqual('APPROVAL_REQUIRES_STAGED', error['reason_code'])
        self.assertEqual('MISSING', error['current_status'])
        self.assertEqual('APPROVED', error['attempted_status'])
        self.assertEqual('1', error['rating_key'])
        self.assertEqual('Example', error['title'])
        self.assertEqual('MISSING', row['status'])

    def test_missing_to_staged_without_url_fails(self):
        row = {
            'rating_key': '1',
            'title': 'Example',
            'status': 'MISSING',
            'url': '',
            'theme_exists': '0',
            'notes': '',
        }

        saved_row, error = logic.save_ledger_row_updates(row, {'status': 'STAGED'})

        self.assertIsNone(saved_row)
        self.assertEqual('MISSING_URL', error['reason_code'])
        self.assertEqual('MISSING', error['current_status'])
        self.assertEqual('STAGED', error['attempted_status'])
        self.assertEqual('MISSING', row['status'])

    def test_staged_to_approved_passes(self):
        row = {
            'rating_key': '1',
            'title': 'Example',
            'status': 'STAGED',
            'url': 'https://example.com/theme',
            'theme_exists': '0',
            'notes': '',
        }

        saved_row, error = logic.save_ledger_row_updates(row, {'status': 'APPROVED'})

        self.assertIs(saved_row, row)
        self.assertIsNone(error)
        self.assertEqual('APPROVED', row['status'])


if __name__ == '__main__':
    unittest.main()
