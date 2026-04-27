#!/usr/bin/env python3
"""
US/JuriscraperUpdater -- Daily US Court Opinions via Juriscraper

Uses Free Law Project's Juriscraper (BSD-2 licensed) to fetch recent court
opinions directly from state and federal court websites. Downloads opinion
PDFs and extracts full text.

Coverage: 198 state court scrapers + 22 federal appellate scrapers.
Not all courts are accessible (some block automated requests).

Usage:
  python bootstrap.py bootstrap --sample   # Fetch 15 sample records
  python bootstrap.py bootstrap --full      # Fetch from all courts
  python bootstrap.py update --since YYYY-MM-DD  # Recent opinions
  python bootstrap.py test                  # Test which scrapers work
"""

import asyncio
import hashlib
import importlib
import json
import logging
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional, Tuple

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

import httpx

from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.JuriscraperUpdater")

# Mapping from Juriscraper module prefix to US state ISO 3166-2 code.
# Only the "primary" court module for each state is listed; appellate
# divisions etc. share the same state code.
MODULE_PREFIX_TO_STATE = {
    "ala": "US-AL", "alaska": "US-AK", "ariz": "US-AZ", "ark": "US-AR",
    "cal": "US-CA", "colo": "US-CO", "conn": "US-CT", "dc": "US-DC",
    "delaware": "US-DE", "fla": "US-FL", "ga": "US-GA", "haw": "US-HI",
    "idaho": "US-ID", "ill": "US-IL", "ind": "US-IN", "iowa": "US-IA",
    "kan": "US-KS", "ky": "US-KY", "la": "US-LA", "me": "US-ME",
    "md": "US-MD", "mass": "US-MA", "mich": "US-MI", "minn": "US-MN",
    "miss": "US-MS", "mo": "US-MO", "mont": "US-MT", "neb": "US-NE",
    "nev": "US-NV", "nh": "US-NH", "nj": "US-NJ", "nm": "US-NM",
    "ny": "US-NY", "nc": "US-NC", "nd": "US-ND", "ohio": "US-OH",
    "okla": "US-OK", "or": "US-OR", "ore": "US-OR", "pa": "US-PA",
    "ri": "US-RI", "sc": "US-SC", "sd": "US-SD", "tenn": "US-TN",
    "tex": "US-TX", "utah": "US-UT", "vt": "US-VT", "va": "US-VA",
    "wash": "US-WA", "wva": "US-WV", "wis": "US-WI", "wyo": "US-WY",
    # Federal
    "ca1": "US", "ca2": "US", "ca3": "US", "ca4": "US", "ca5": "US",
    "ca6": "US", "ca7": "US", "ca8": "US", "ca9": "US", "ca10": "US",
    "ca11": "US", "cadc": "US", "cafc": "US", "scotus": "US",
}


def _get_state_code(module_name: str) -> str:
    """Extract state code from a Juriscraper module path."""
    short = module_name.split(".")[-1]
    # Try exact match first
    if short in MODULE_PREFIX_TO_STATE:
        return MODULE_PREFIX_TO_STATE[short]
    # Try prefix matching (e.g. "calctapp_1st" → "cal" → "US-CA")
    for prefix, code in sorted(MODULE_PREFIX_TO_STATE.items(),
                                key=lambda x: -len(x[0])):
        if short.startswith(prefix):
            return code
    return "US"


def _get_court_label(module_name: str) -> str:
    """Get a human-readable court label from module name."""
    return module_name.split(".")[-1]


async def _download_pdf(url: str, timeout: int = 60) -> Optional[bytes]:
    """Download a PDF from a URL."""
    try:
        async with httpx.AsyncClient(
            follow_redirects=True,
            timeout=timeout,
            headers={"User-Agent": "LegalDataHunter/1.0 (legal research)"},
        ) as client:
            resp = await client.get(url)
            resp.raise_for_status()
            if len(resp.content) < 100:
                return None
            return resp.content
    except Exception as e:
        logger.warning(f"Failed to download {url[:80]}: {e}")
        return None


def _extract_text(pdf_bytes: bytes) -> str:
    """Extract text from PDF bytes using the common extractor."""
    return extract_pdf_markdown(
        source="US/JuriscraperUpdater",
        source_id="",
        pdf_bytes=pdf_bytes,
        table="case_law",
    ) or ""


async def _scrape_court(module_name: str) -> List[Dict[str, Any]]:
    """Run a single Juriscraper scraper and return raw opinion metadata."""
    try:
        mod = importlib.import_module(module_name)
        site = mod.Site()
        await site.parse()

        results = []
        count = len(site)
        if count == 0:
            return []

        for i in range(count):
            try:
                record = {
                    "case_name": site.case_names[i] if site.case_names else "",
                    "case_date": None,
                    "download_url": site.download_urls[i] if site.download_urls else "",
                    "docket_number": "",
                    "status": "",
                    "court_module": module_name,
                }
                if site.case_dates and i < len(site.case_dates):
                    d = site.case_dates[i]
                    if hasattr(d, "isoformat"):
                        record["case_date"] = d.isoformat()[:10]
                    elif isinstance(d, str):
                        record["case_date"] = d[:10]
                if hasattr(site, "docket_numbers") and site.docket_numbers and i < len(site.docket_numbers):
                    record["docket_number"] = site.docket_numbers[i] or ""
                if hasattr(site, "statuses") and site.statuses and i < len(site.statuses):
                    record["status"] = site.statuses[i] or ""
                results.append(record)
            except (IndexError, AttributeError):
                continue

        return results
    except Exception as e:
        logger.debug(f"Scraper {module_name} failed: {e}")
        return []


SAMPLE_SCRAPERS = [
    "juriscraper.opinions.united_states.state.ill",
    "juriscraper.opinions.united_states.state.fla",
    "juriscraper.opinions.united_states.state.wash",
    "juriscraper.opinions.united_states.state.mass",
    "juriscraper.opinions.united_states.state.colo",
    "juriscraper.opinions.united_states.state.minn",
    "juriscraper.opinions.united_states.state.ark",
    "juriscraper.opinions.united_states.state.dc",
    "juriscraper.opinions.united_states.state.delaware",
]


class JuriscraperUpdater(BaseScraper):
    """Scraper that uses Juriscraper to fetch recent US court opinions."""

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self._available_scrapers: Optional[List[str]] = None
        self._use_sample_scrapers = False

    def _get_scrapers(self, category: str = "state") -> List[str]:
        """Get list of available Juriscraper modules for a category."""
        from juriscraper.lib.importer import build_module_list
        if category == "state":
            return build_module_list("juriscraper.opinions.united_states.state")
        elif category == "federal":
            return build_module_list("juriscraper.opinions.united_states.federal_appellate")
        elif category == "sample":
            return list(SAMPLE_SCRAPERS)
        else:
            state = build_module_list("juriscraper.opinions.united_states.state")
            fed = build_module_list("juriscraper.opinions.united_states.federal_appellate")
            return state + fed

    def _test_scrapers(self, scrapers: List[str]) -> Tuple[List[str], List[str]]:
        """Test which scrapers work. Returns (working, failed) lists."""
        working = []
        failed = []

        async def test_one(mod_name):
            try:
                mod = importlib.import_module(mod_name)
                site = mod.Site()
                await site.parse()
                n = len(site)
                if n > 0:
                    return (mod_name, n, True)
                return (mod_name, 0, False)
            except Exception as e:
                return (mod_name, 0, False)

        async def test_all():
            # Test in batches of 10 to avoid overwhelming
            for i in range(0, len(scrapers), 10):
                batch = scrapers[i:i+10]
                tasks = [test_one(m) for m in batch]
                results = await asyncio.gather(*tasks)
                for mod_name, count, ok in results:
                    if ok:
                        working.append(mod_name)
                        logger.info(f"  OK: {mod_name} ({count} opinions)")
                    else:
                        failed.append(mod_name)
                await asyncio.sleep(1)

        asyncio.run(test_all())
        return working, failed

    def bootstrap(self, sample_mode: bool = False, sample_size: int = 10) -> dict:
        self._use_sample_scrapers = sample_mode
        return super().bootstrap(sample_mode=sample_mode, sample_size=sample_size)

    def fetch_all(self, scraper_list: Optional[List[str]] = None) -> Generator[Dict[str, Any], None, None]:
        """Fetch recent opinions from courts."""
        if self._use_sample_scrapers:
            scrapers = scraper_list or self._get_scrapers("sample")
        else:
            scrapers = scraper_list or self._get_scrapers("all")
        logger.info(f"Attempting {len(scrapers)} Juriscraper scrapers...")

        delay = self.config.get("fetch", {}).get("delay", 2.0)

        async def run_all():
            all_results = []
            for i in range(0, len(scrapers), 5):
                batch = scrapers[i:i+5]
                tasks = [_scrape_court(m) for m in batch]
                batch_results = await asyncio.gather(*tasks)
                for results in batch_results:
                    all_results.extend(results)
                await asyncio.sleep(1)
            return all_results

        raw_opinions = asyncio.run(run_all())
        logger.info(f"Found {len(raw_opinions)} opinions across scrapers")

        for opinion in raw_opinions:
            url = opinion.get("download_url")
            if not url:
                continue

            pdf_bytes = asyncio.run(_download_pdf(url))
            if not pdf_bytes:
                continue

            text = _extract_text(pdf_bytes)
            if not text or len(text) < 100:
                logger.warning(f"Insufficient text for {opinion.get('case_name', '')}: {len(text)} chars")
                continue

            opinion["text"] = text
            yield opinion
            time.sleep(delay)

    def fetch_updates(self, since: Optional[str] = None) -> Generator[Dict[str, Any], None, None]:
        """Fetch recent opinions (Juriscraper always returns the latest batch)."""
        # Juriscraper scrapers return the most recent opinions by default,
        # so fetch_updates is effectively the same as fetch_all
        yield from self.fetch_all()

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        module_name = raw.get("court_module", "")
        court_label = _get_court_label(module_name)
        state_code = _get_state_code(module_name)
        case_name = raw.get("case_name", "")
        case_date = raw.get("case_date", "")
        docket = raw.get("docket_number", "")

        # Build a stable ID from court + docket or case name + date
        id_input = f"{court_label}|{docket or case_name}|{case_date}"
        doc_hash = hashlib.sha256(id_input.encode()).hexdigest()[:12]
        doc_id = f"US-JS-{court_label}-{doc_hash}"

        return {
            "_id": doc_id,
            "_source": "US/JuriscraperUpdater",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": case_name,
            "text": raw.get("text", ""),
            "date": case_date,
            "url": raw.get("download_url", ""),
            "case_number": docket,
            "court": court_label,
            "court_module": module_name,
            "jurisdiction": state_code,
            "status": raw.get("status", ""),
        }

    def test_connection(self) -> bool:
        """Test that at least some Juriscraper scrapers work."""
        # Test a few known-good scrapers
        test_modules = [
            "juriscraper.opinions.united_states.state.ill",
            "juriscraper.opinions.united_states.state.fla",
            "juriscraper.opinions.united_states.state.wash",
        ]

        async def test():
            for mod_name in test_modules:
                try:
                    mod = importlib.import_module(mod_name)
                    site = mod.Site()
                    await site.parse()
                    n = len(site)
                    if n > 0:
                        logger.info(f"Connection test OK: {mod_name} returned {n} opinions")
                        return True
                except Exception:
                    continue
            return False

        return asyncio.run(test())


def main():
    import argparse

    parser = argparse.ArgumentParser(description="US/JuriscraperUpdater court opinion fetcher")
    subparsers = parser.add_subparsers(dest="command")

    boot_parser = subparsers.add_parser("bootstrap", help="Bootstrap data")
    boot_parser.add_argument("--sample", action="store_true", help="Sample mode (15 records)")
    boot_parser.add_argument("--full", action="store_true", help="Full bootstrap (all courts)")
    boot_parser.add_argument("--count", type=int, default=15, help="Sample count")

    upd_parser = subparsers.add_parser("update", help="Incremental update")
    upd_parser.add_argument("--since", required=True, help="YYYY-MM-DD")

    subparsers.add_parser("test", help="Test which scrapers work")

    args = parser.parse_args()
    scraper = JuriscraperUpdater()

    if args.command == "test":
        logger.info("Testing Juriscraper scrapers...")
        state_scrapers = scraper._get_scrapers("state")
        fed_scrapers = scraper._get_scrapers("federal")
        logger.info(f"Testing {len(state_scrapers)} state + {len(fed_scrapers)} federal scrapers...")
        working, failed = scraper._test_scrapers(state_scrapers + fed_scrapers)
        logger.info(f"\nResults: {len(working)} working, {len(failed)} failed")
        for m in working:
            print(f"  OK: {m}")
        sys.exit(0 if working else 1)

    elif args.command == "bootstrap":
        if args.sample:
            stats = scraper.bootstrap(sample_mode=True, sample_size=args.count)
        elif args.full:
            stats = scraper.bootstrap(sample_mode=False)
        else:
            stats = scraper.bootstrap(sample_mode=True, sample_size=args.count)
        logger.info(f"Bootstrap complete: {json.dumps(stats, indent=2, default=str)}")

    elif args.command == "update":
        count = 0
        data_dir = scraper.source_dir / "data"
        data_dir.mkdir(parents=True, exist_ok=True)
        with open(data_dir / "updates.jsonl", "w") as f:
            for raw in scraper.fetch_updates(since=args.since):
                record = scraper.normalize(raw)
                f.write(json.dumps(record, ensure_ascii=False, default=str) + "\n")
                count += 1
        logger.info(f"Fetched {count} updates since {args.since}")

    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
