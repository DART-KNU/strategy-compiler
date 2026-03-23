"""
Unit tests for ticker normalization.

Verifies that:
  - A-prefixed tickers normalize correctly
  - 6-digit codes pass through unchanged
  - Whitespace is trimmed
  - Invalid codes return None
  - Leading zeros are preserved
"""

import sys
from pathlib import Path

# Allow running from db/ root
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import unittest
from src.utils.ticker import normalize_ticker, is_valid_canonical_ticker, normalize_ticker_strict


class TestTickerNormalization(unittest.TestCase):

    # ---- A-prefix removal ----

    def test_a_prefix_samsung(self):
        """A005930 -> 005930"""
        self.assertEqual(normalize_ticker("A005930"), "005930")

    def test_a_prefix_skhynix(self):
        """A000660 -> 000660"""
        self.assertEqual(normalize_ticker("A000660"), "000660")

    def test_a_prefix_zero(self):
        """A000000 -> 000000 (synthetic edge case)"""
        self.assertEqual(normalize_ticker("A000000"), "000000")

    def test_lowercase_a_prefix(self):
        """a005930 -> 005930"""
        self.assertEqual(normalize_ticker("a005930"), "005930")

    # ---- Already canonical ----

    def test_already_canonical_005930(self):
        """005930 -> 005930 (no change needed)"""
        self.assertEqual(normalize_ticker("005930"), "005930")

    def test_already_canonical_000660(self):
        """000660 -> 000660"""
        self.assertEqual(normalize_ticker("000660"), "000660")

    def test_already_canonical_with_leading_zeros(self):
        """000010 -> 000010 (leading zeros preserved)"""
        self.assertEqual(normalize_ticker("000010"), "000010")

    # ---- Whitespace trimming ----

    def test_whitespace_stripped_a_prefix(self):
        """  A005930  -> 005930"""
        self.assertEqual(normalize_ticker("  A005930  "), "005930")

    def test_whitespace_stripped_plain(self):
        """  005930  -> 005930"""
        self.assertEqual(normalize_ticker("  005930  "), "005930")

    def test_tab_whitespace(self):
        """\t000660\t -> 000660"""
        self.assertEqual(normalize_ticker("\t000660\t"), "000660")

    # ---- Invalid / rejected inputs ----

    def test_none_returns_none(self):
        self.assertIsNone(normalize_ticker(None))

    def test_empty_string_returns_none(self):
        self.assertIsNone(normalize_ticker(""))

    def test_whitespace_only_returns_none(self):
        self.assertIsNone(normalize_ticker("   "))

    def test_short_code_returns_none(self):
        """5-digit codes are not canonical."""
        self.assertIsNone(normalize_ticker("05930"))

    def test_non_digit_after_a_strip(self):
        """A0082N0 after stripping A -> 0082N0 (not 6 digits, contains N)"""
        # 0082N0 has a letter, so should be rejected
        result = normalize_ticker("0082N0")
        self.assertIsNone(result)

    def test_eight_digit_returns_none(self):
        """8-digit codes are not canonical."""
        self.assertIsNone(normalize_ticker("00593000"))

    def test_letters_only_returns_none(self):
        self.assertIsNone(normalize_ticker("SAMSUNG"))

    def test_dash_returns_none(self):
        self.assertIsNone(normalize_ticker("-"))

    # ---- Leading zeros preserved ----

    def test_leading_zeros_preserved(self):
        """Critical: 000030 must not become '30' or '0003' etc."""
        result = normalize_ticker("000030")
        self.assertEqual(result, "000030")
        self.assertEqual(len(result), 6)

    def test_a_prefix_leading_zeros(self):
        """A000010 -> 000010"""
        result = normalize_ticker("A000010")
        self.assertEqual(result, "000010")
        self.assertEqual(len(result), 6)

    # ---- is_valid_canonical_ticker ----

    def test_valid_canonical(self):
        self.assertTrue(is_valid_canonical_ticker("005930"))
        self.assertTrue(is_valid_canonical_ticker("000000"))
        self.assertTrue(is_valid_canonical_ticker("999999"))

    def test_invalid_canonical(self):
        self.assertFalse(is_valid_canonical_ticker("A005930"))
        self.assertFalse(is_valid_canonical_ticker("05930"))
        self.assertFalse(is_valid_canonical_ticker("00593A"))

    # ---- normalize_ticker_strict ----

    def test_strict_raises_on_none(self):
        with self.assertRaises(ValueError):
            normalize_ticker_strict(None)

    def test_strict_raises_on_invalid(self):
        with self.assertRaises(ValueError):
            normalize_ticker_strict("INVALID")

    def test_strict_passes_on_valid(self):
        self.assertEqual(normalize_ticker_strict("A005930"), "005930")

    # ---- Equality: A-prefixed == non-prefixed ----

    def test_a_prefix_equals_plain(self):
        """Core requirement: A005930 == 005930 after normalization."""
        self.assertEqual(normalize_ticker("A005930"), normalize_ticker("005930"))

    def test_a_prefix_equals_plain_000660(self):
        """A000660 == 000660"""
        self.assertEqual(normalize_ticker("A000660"), normalize_ticker("000660"))


if __name__ == "__main__":
    unittest.main()
