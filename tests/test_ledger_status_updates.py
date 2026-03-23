from __future__ import annotations

import sys
import types
import unittest

sys.modules.setdefault("yaml", types.SimpleNamespace(safe_load=lambda *args, **kwargs: {}, safe_dump=lambda *args, **kwargs: ""))
sys.modules.setdefault("requests", types.SimpleNamespace(get=lambda *args, **kwargs: None, post=lambda *args, **kwargs: None))

import web.logic as logic


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

    def test_stale_available_with_saved_source_restages_before_approval(self):
        row = {
            'rating_key': '1',
            'title': 'Example',
            'status': 'AVAILABLE',
            'url': 'https://example.com/theme',
            'theme_exists': '0',
            'notes': '',
        }

        saved_row, error = logic.save_ledger_row_updates(row, {'status': 'STAGED'})

        self.assertIs(saved_row, row)
        self.assertIsNone(error)
        self.assertEqual('STAGED', row['status'])

        saved_row, error = logic.save_ledger_row_updates(row, {'status': 'APPROVED'})

        self.assertIs(saved_row, row)
        self.assertIsNone(error)
        self.assertEqual('APPROVED', row['status'])

    def test_stale_available_without_saved_source_normalizes_to_missing_rules(self):
        row = {
            'rating_key': '1',
            'title': 'Example',
            'status': 'AVAILABLE',
            'url': '',
            'theme_exists': '0',
            'notes': '',
        }

        saved_row, error = logic.save_ledger_row_updates(row, {'status': 'APPROVED'})

        self.assertIsNone(saved_row)
        self.assertEqual('APPROVAL_REQUIRES_STAGED', error['reason_code'])
        self.assertEqual('AVAILABLE', error['current_status'])
        self.assertEqual('APPROVED', error['attempted_status'])
        self.assertEqual('AVAILABLE', row['status'])

    def test_url_matching_golden_source_keeps_golden_origin(self):
        row = {
            'rating_key': '1',
            'title': 'Example',
            'status': 'STAGED',
            'url': '',
            'golden_source_url': 'https://example.com/golden',
            'source_origin': 'unknown',
            'theme_exists': '0',
            'notes': '',
        }

        saved_row, error = logic.save_ledger_row_updates(row, {
            'url': 'https://example.com/golden',
            'status': 'APPROVED',
        })

        self.assertIs(saved_row, row)
        self.assertIsNone(error)
        self.assertEqual('golden_source', row['source_origin'])

    def test_non_golden_manual_url_stays_manual_origin(self):
        row = {
            'rating_key': '1',
            'title': 'Example',
            'status': 'STAGED',
            'url': '',
            'golden_source_url': 'https://example.com/golden',
            'source_origin': 'unknown',
            'theme_exists': '0',
            'notes': '',
        }

        saved_row, error = logic.save_ledger_row_updates(row, {
            'url': 'https://example.com/manual',
            'status': 'APPROVED',
        })

        self.assertIs(saved_row, row)
        self.assertIsNone(error)
        self.assertEqual('manual', row['source_origin'])


if __name__ == '__main__':
    unittest.main()
