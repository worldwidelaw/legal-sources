"""Regression tests for the Australian Federal Register adapter."""

from __future__ import annotations

import importlib.util
import sys
import types
import unittest
from pathlib import Path


MODULE_PATH = Path(__file__).with_name("bootstrap.py")
if "common.pdf_extract" not in sys.modules:
    pdf_extract = types.ModuleType("common.pdf_extract")
    pdf_extract.extract_pdf_markdown = lambda *_args, **_kwargs: None
    pdf_extract.preload_existing_ids = lambda *_args, **_kwargs: set()
    sys.modules["common.pdf_extract"] = pdf_extract
SPEC = importlib.util.spec_from_file_location("au_federal_register_bootstrap", MODULE_PATH)
assert SPEC is not None and SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


class _RateLimiter:
    def __init__(self) -> None:
        self.calls = 0

    def wait(self) -> None:
        self.calls += 1


class _Response:
    def __init__(self, payload: object) -> None:
        self.payload = payload
        self.raise_calls = 0

    def raise_for_status(self) -> None:
        self.raise_calls += 1

    def json(self) -> object:
        return self.payload


class _Client:
    def __init__(self, response: _Response) -> None:
        self.response = response
        self.urls: list[str] = []

    def get(self, url: str) -> _Response:
        self.urls.append(url)
        return self.response


def _scraper(payload: object):
    scraper = object.__new__(MODULE.AustraliaFederalRegisterScraper)
    scraper.rate_limiter = _RateLimiter()
    scraper.client = _Client(_Response(payload))
    return scraper


class AustraliaFederalRegisterCountTests(unittest.TestCase):
    def test_uses_inline_odata_count(self) -> None:
        scraper = _scraper({"@odata.count": 13732, "value": [{}]})

        count = scraper._get_total_count("collection eq 'Act'")

        self.assertEqual(count, 13732)
        self.assertEqual(scraper.rate_limiter.calls, 1)
        self.assertEqual(
            scraper.client.urls,
            [
                "/v1/titles?$top=1&$skip=0&$count=true"
                "&$filter=collection eq 'Act'"
            ],
        )
        self.assertEqual(scraper.client.response.raise_calls, 1)

    def test_rejects_the_observed_minimum_integer_sentinel(self) -> None:
        scraper = _scraper({"@odata.count": -9223372036854775808, "value": []})

        with self.assertRaisesRegex(ValueError, "valid @odata.count"):
            scraper._get_total_count("collection eq 'Act'")

    def test_rejects_missing_boolean_or_non_integer_counts(self) -> None:
        for payload in (
            {"value": []},
            {"@odata.count": True, "value": []},
            {"@odata.count": "13732", "value": []},
        ):
            with self.subTest(payload=payload):
                with self.assertRaisesRegex(ValueError, "valid @odata.count"):
                    _scraper(payload)._get_total_count()


if __name__ == "__main__":
    unittest.main()
