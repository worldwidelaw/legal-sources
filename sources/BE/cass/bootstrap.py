#!/usr/bin/env python3
"""
BE/CASS -- Belgian Court of Cassation Case Law Fetcher

Fetches case law from the Belgian Court of Cassation (Cour de Cassation /
Hof van Cassatie) via JUPORTAL (juportal.be).

Strategy:
  - Uses ECLI sitemaps for document discovery (EU standard format)
  - Sitemap index: robots.txt lists daily sitemaps with ECLI entries
  - Each sitemap entry contains rich metadata (date, abstract, subject, etc.)
  - Full text: juportal.be/content/ECLI:... returns HTML with decision text
  - Filters for court code "CASS" to get only Court of Cassation decisions

Endpoints:
  - Sitemap index: https://juportal.just.fgov.be/JUPORTAsitemap/YYYY/MM/DD/sitemap_index_1.xml
  - Sitemap: https://juportal.just.fgov.be/JUPORTAsitemap/YYYY/MM/DD/sitemap_N.xml
  - Content: https://juportal.be/content/ECLI:BE:CASS:YYYY:...

Data:
  - Case law from 1958 to present
  - ~49,000+ decisions
  - Languages: French, Dutch, German
  - License: Open Government Data

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records for validation
  python bootstrap.py update             # Incremental update (recent sitemaps only)
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import html
import xml.etree.ElementTree as ET
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Generator, Optional, Dict, Any, List
from urllib.parse import unquote

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.BE.cass")

# Checkpoint file for resuming across sessions
CHECKPOINT_FILE = Path(__file__).parent / "checkpoint.json"

# Base URLs
SITEMAP_BASE = "https://juportal.just.fgov.be/JUPORTAsitemap"
CONTENT_BASE = "https://juportal.be"
ROBOTS_URL = "https://juportal.be/robots.txt"

# Namespaces for sitemap XML
NS = {
    'sm': 'http://www.sitemaps.org/schemas/sitemap/0.9',
    'ecli': 'https://e-justice.europa.eu/ecli'
}


class CassScraper(BaseScraper):
    """
    Scraper for BE/CASS -- Belgian Court of Cassation.
    Country: BE
    URL: https://juportal.be

    Data types: case_law
    Auth: none (Open Government Data)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        # Flag to control checkpoint usage
        self._use_checkpoint = True

        self.client = HttpClient(
            base_url=CONTENT_BASE,
            headers={
                "User-Agent": "WorldWideLaw/1.0 (Open Data Research)",
                "Accept-Language": "fr,nl,de",
            },
            timeout=60,
        )

        self.sitemap_client = HttpClient(
            base_url=SITEMAP_BASE,
            headers={
                "User-Agent": "WorldWideLaw/1.0 (Open Data Research)",
            },
            timeout=60,
        )

    def _get_sitemap_urls_from_robots(self) -> List[str]:
        """
        Parse robots.txt to get all sitemap URLs.
        Returns list of sitemap index URLs ordered by date (newest first).
        """
        try:
            resp = self.client.get("/robots.txt")
            resp.raise_for_status()
            content = resp.text

            sitemap_urls = []
            for line in content.split('\n'):
                if line.startswith('Sitemap:'):
                    url = line.split(':', 1)[1].strip()
                    sitemap_urls.append(url)

            # Sort by date (newest first) - URL contains date
            sitemap_urls.sort(reverse=True)
            return sitemap_urls

        except Exception as e:
            logger.error(f"Failed to fetch robots.txt: {e}")
            return []

    def _parse_sitemap_index(self, url: str) -> List[str]:
        """
        Parse a sitemap index XML to get individual sitemap URLs.
        """
        try:
            self.rate_limiter.wait()
            resp = self.client.session.get(url, timeout=60)
            resp.raise_for_status()

            root = ET.fromstring(resp.content)

            sitemap_urls = []
            for sitemap in root.findall('.//sm:sitemap/sm:loc', NS):
                sitemap_urls.append(sitemap.text)

            return sitemap_urls

        except Exception as e:
            logger.warning(f"Failed to parse sitemap index {url}: {e}")
            return []

    def _parse_sitemap(self, url: str) -> List[Dict[str, Any]]:
        """
        Parse a sitemap XML and extract Court of Cassation entries.
        Returns list of metadata dicts for CASS decisions.
        """
        try:
            self.rate_limiter.wait()
            resp = self.client.session.get(url, timeout=60)
            resp.raise_for_status()

            root = ET.fromstring(resp.content)

            entries = []
            for url_elem in root.findall('.//sm:url', NS):
                # Get the ECLI from loc
                loc = url_elem.find('sm:loc', NS)
                if loc is None or loc.text is None:
                    continue

                # Extract ECLI from URL
                ecli_match = re.search(r'ECLI:BE:CASS:\d{4}:[A-Z0-9.]+', loc.text)
                if not ecli_match:
                    continue

                ecli = ecli_match.group(0)

                # Parse metadata from ecli:document
                doc = url_elem.find('.//ecli:document', NS)
                if doc is None:
                    continue

                meta = doc.find('ecli:metadata', NS)
                if meta is None:
                    continue

                # Extract fields
                entry = {
                    'ecli': ecli,
                }

                # Date
                date_elem = meta.find('ecli:date', NS)
                if date_elem is not None and date_elem.text:
                    entry['date'] = date_elem.text

                # Language
                lang_elem = meta.find('ecli:language', NS)
                if lang_elem is not None and lang_elem.text:
                    entry['language'] = lang_elem.text

                # Title (case name)
                title_elem = meta.find('ecli:title', NS)
                if title_elem is not None and title_elem.text:
                    entry['title'] = html.unescape(title_elem.text.strip())

                # Abstract
                abstract_elem = meta.find('ecli:abstract', NS)
                if abstract_elem is not None and abstract_elem.text:
                    entry['abstract'] = html.unescape(abstract_elem.text.strip())

                # Subject/domain
                subjects = []
                for subj in meta.findall('ecli:subject', NS):
                    if subj.text:
                        subjects.append(html.unescape(subj.text.strip()))
                if subjects:
                    entry['subjects'] = subjects

                # Description/keywords
                descriptions = []
                for desc in meta.findall('ecli:description', NS):
                    if desc.text:
                        descriptions.append(html.unescape(desc.text.strip()))
                if descriptions:
                    entry['keywords'] = descriptions

                # Role number
                for ref in meta.findall('ecli:reference', NS):
                    if ref.text and ('rôle' in ref.text.lower() or 'rolnummer' in ref.text.lower()):
                        role_match = re.search(r'[A-Z]\.\d{2}\.\d+\.[A-Z]', ref.text)
                        if role_match:
                            entry['role_number'] = role_match.group(0)

                # Court name
                court_fr = meta.find('ecli:creator[@lang="fr"]', NS)
                court_nl = meta.find('ecli:creator[@lang="nl"]', NS)
                if court_fr is not None and court_fr.text:
                    entry['court_fr'] = court_fr.text
                if court_nl is not None and court_nl.text:
                    entry['court_nl'] = court_nl.text

                entries.append(entry)

            return entries

        except Exception as e:
            logger.warning(f"Failed to parse sitemap {url}: {e}")
            return []

    def _fetch_full_text(self, ecli: str, language: str = None) -> str:
        """
        Fetch and extract full decision text from JUPORTAL content page.
        """
        try:
            # Construct URL
            url = f"/content/{ecli}"
            if language:
                url += f"/{language.upper()}"

            self.rate_limiter.wait()
            resp = self.client.get(url)
            resp.raise_for_status()

            content = resp.text

            # Check if not found
            if 'NOT FOUND' in content or 'niet gevonden' in content.lower():
                return ""

            # Extract decision text from HTML
            # The text is in a fieldset with legend "Texte de la décision" or "Tekst van de beslissing"
            text_parts = []

            # Find the decision text div
            # Pattern: <div id="...">content</div> after "Texte de la décision" or similar
            decision_pattern = re.compile(
                r'(?:Texte de la décision|Tekst van de beslissing|Text des Urteils).*?<div[^>]*>(.*?)</div>\s*(?:<p><a href|</fieldset>)',
                re.DOTALL | re.IGNORECASE
            )

            match = decision_pattern.search(content)
            if match:
                raw_text = match.group(1)
                # Clean HTML
                clean_text = re.sub(r'<br\s*/?>', '\n', raw_text)
                clean_text = re.sub(r'<[^>]+>', ' ', clean_text)
                clean_text = html.unescape(clean_text)
                clean_text = re.sub(r'\s+', ' ', clean_text).strip()
                clean_text = re.sub(r' +', ' ', clean_text)
                # Restore paragraph breaks
                clean_text = re.sub(r'\s*\n\s*', '\n', clean_text)
                text_parts.append(clean_text)

            # If main pattern didn't work, try a fallback
            if not text_parts:
                # Look for substantial text blocks after the fieldset
                fieldset_pattern = re.compile(
                    r'<fieldset[^>]*id="[^"]*bwiGyQ[^"]*"[^>]*>.*?<div[^>]*>(.*?)</div>',
                    re.DOTALL
                )
                for match in fieldset_pattern.finditer(content):
                    raw_text = match.group(1)
                    if len(raw_text) > 500:  # Substantial text
                        clean_text = re.sub(r'<br\s*/?>', '\n', raw_text)
                        clean_text = re.sub(r'<[^>]+>', ' ', clean_text)
                        clean_text = html.unescape(clean_text)
                        clean_text = re.sub(r'\s+', ' ', clean_text).strip()
                        text_parts.append(clean_text)

            # Last fallback: extract all text from <p> tags
            if not text_parts:
                p_texts = re.findall(r'<p[^>]*>([^<]+(?:<br[^>]*>[^<]+)*)</p>', content)
                for pt in p_texts:
                    clean = re.sub(r'<br\s*/?>', '\n', pt)
                    clean = html.unescape(clean.strip())
                    if len(clean) > 100:
                        text_parts.append(clean)

            full_text = '\n'.join(text_parts)
            return full_text.strip()

        except Exception as e:
            logger.warning(f"Failed to fetch full text for {ecli}: {e}")
            return ""

    def _discover_eclis(self, max_sitemaps: int = None) -> Generator[Dict[str, Any], None, None]:
        """
        Discover ECLIs from sitemaps.
        Yields metadata dicts for Court of Cassation decisions.
        """
        sitemap_index_urls = self._get_sitemap_urls_from_robots()

        if not sitemap_index_urls:
            logger.error("No sitemap URLs found in robots.txt")
            return

        logger.info(f"Found {len(sitemap_index_urls)} sitemap index URLs")

        sitemap_count = 0
        for index_url in sitemap_index_urls:
            if max_sitemaps and sitemap_count >= max_sitemaps:
                break

            logger.info(f"Processing sitemap index: {index_url}")
            sitemap_urls = self._parse_sitemap_index(index_url)

            for sitemap_url in sitemap_urls:
                if max_sitemaps and sitemap_count >= max_sitemaps:
                    break

                entries = self._parse_sitemap(sitemap_url)
                sitemap_count += 1

                for entry in entries:
                    yield entry

    def _load_checkpoint(self) -> dict:
        """Load checkpoint from file if it exists."""
        if CHECKPOINT_FILE.exists():
            try:
                with open(CHECKPOINT_FILE, "r") as f:
                    return json.load(f)
            except json.JSONDecodeError:
                logger.warning("Invalid checkpoint file, starting fresh")
        return {"fetched_eclis": [], "phase": "discovery", "sitemap_index": 0}

    def _save_checkpoint(self, checkpoint: dict):
        """Save checkpoint to file."""
        with open(CHECKPOINT_FILE, "w") as f:
            json.dump(checkpoint, f, indent=2)
        logger.debug(f"Checkpoint saved: {len(checkpoint.get('fetched_eclis', []))} ECLIs processed")

    def _clear_checkpoint(self):
        """Clear checkpoint file."""
        if CHECKPOINT_FILE.exists():
            CHECKPOINT_FILE.unlink()
            logger.info("Checkpoint cleared")

    def fetch_all(self) -> Generator[dict, None, None]:
        """
        Yield all Court of Cassation decisions.
        Discovers ECLIs from sitemaps and fetches full text for each.
        Supports checkpoint/resume via self._use_checkpoint flag.
        """
        use_checkpoint = self._use_checkpoint

        # Load checkpoint
        if use_checkpoint:
            checkpoint = self._load_checkpoint()
            fetched_eclis = set(checkpoint.get("fetched_eclis", []))
            if fetched_eclis:
                logger.info(f"Resuming from checkpoint: {len(fetched_eclis)} ECLIs already fetched")
        else:
            checkpoint = {"fetched_eclis": [], "phase": "discovery", "sitemap_index": 0}
            fetched_eclis = set()

        seen_eclis = set(fetched_eclis)  # Include already fetched in seen
        fetched_count = len(fetched_eclis)

        for meta in self._discover_eclis():
            ecli = meta.get('ecli')
            if not ecli or ecli in seen_eclis:
                continue

            seen_eclis.add(ecli)

            # Fetch full text
            language = meta.get('language', 'fr')
            full_text = self._fetch_full_text(ecli, language)

            if not full_text:
                logger.warning(f"No full text for {ecli}, skipping")
                continue

            meta['full_text'] = full_text
            fetched_count += 1
            fetched_eclis.add(ecli)

            # Save checkpoint periodically
            if use_checkpoint and fetched_count % 100 == 0:
                # Only keep last 50000 ECLIs to limit file size
                recent_eclis = list(fetched_eclis)[-50000:]
                checkpoint = {
                    "fetched_eclis": recent_eclis,
                    "total_fetched": fetched_count,
                    "last_update": datetime.now(timezone.utc).isoformat(),
                }
                self._save_checkpoint(checkpoint)
                logger.info(f"Checkpoint saved: {fetched_count} ECLIs processed")

            yield meta

        # Clear checkpoint on successful completion
        if use_checkpoint:
            self._clear_checkpoint()
            logger.info(f"Bootstrap complete - {fetched_count} total ECLIs - checkpoint cleared")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """
        Yield decisions updated since the given date.
        Uses recent sitemaps only.
        """
        # Calculate how many days of sitemaps to check
        days_back = (datetime.now(timezone.utc) - since).days + 1
        max_sitemaps = min(days_back * 50, 200)  # Roughly 50 sitemaps per day

        seen_eclis = set()

        for meta in self._discover_eclis(max_sitemaps=max_sitemaps):
            ecli = meta.get('ecli')
            if not ecli or ecli in seen_eclis:
                continue

            # Filter by date
            date_str = meta.get('date')
            if date_str:
                try:
                    doc_date = datetime.fromisoformat(date_str).replace(tzinfo=timezone.utc)
                    if doc_date < since:
                        continue
                except:
                    pass

            seen_eclis.add(ecli)

            language = meta.get('language', 'fr')
            full_text = self._fetch_full_text(ecli, language)

            if not full_text:
                continue

            meta['full_text'] = full_text
            yield meta

    def normalize(self, raw: dict) -> dict:
        """
        Transform raw document data into standard schema.

        CRITICAL: Includes full text in the 'text' field.
        """
        ecli = raw.get('ecli', '')
        full_text = raw.get('full_text', '')

        # Parse chamber from ECLI (e.g., 1F, 2N, 3F)
        chamber = ""
        chamber_match = re.search(r'\.(\d[A-Z])\.\d+$', ecli)
        if chamber_match:
            chamber = chamber_match.group(1)

        # Determine language
        language = raw.get('language', '')
        if not language and chamber:
            if 'F' in chamber:
                language = 'fr'
            elif 'N' in chamber:
                language = 'nl'

        # Get court name
        court = raw.get('court_fr') or raw.get('court_nl') or 'Cour de cassation'

        # Combine subjects and keywords
        subjects = raw.get('subjects', [])
        keywords = raw.get('keywords', [])

        return {
            # Required base fields
            "_id": ecli,
            "_source": "BE/CASS",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            # Standard fields
            "title": raw.get('title', ''),
            "text": full_text,  # MANDATORY FULL TEXT
            "date": raw.get('date', ''),
            "url": f"{CONTENT_BASE}/content/{ecli}",
            # Case law specific
            "ecli": ecli,
            "court": court,
            "chamber": chamber,
            "language": language,
            "role_number": raw.get('role_number', ''),
            "abstract": raw.get('abstract', ''),
            "subjects": subjects,
            "keywords": keywords,
        }

    def test_connection(self):
        """Quick connectivity test."""
        print("Testing Belgian Court of Cassation (JUPORTAL) endpoints...")

        # Test robots.txt
        print("\n1. Testing robots.txt...")
        try:
            sitemap_urls = self._get_sitemap_urls_from_robots()
            print(f"   Found {len(sitemap_urls)} sitemap index URLs")
            if sitemap_urls:
                print(f"   Most recent: {sitemap_urls[0]}")
        except Exception as e:
            print(f"   ERROR: {e}")

        # Test sitemap
        print("\n2. Testing sitemap parsing...")
        try:
            if sitemap_urls:
                sitemap_list = self._parse_sitemap_index(sitemap_urls[0])
                print(f"   Found {len(sitemap_list)} sitemaps in index")

                if sitemap_list:
                    entries = self._parse_sitemap(sitemap_list[0])
                    cass_entries = [e for e in entries if 'CASS' in e.get('ecli', '')]
                    print(f"   Found {len(cass_entries)} CASS entries in first sitemap")
                    if cass_entries:
                        print(f"   Sample ECLI: {cass_entries[0].get('ecli')}")
        except Exception as e:
            print(f"   ERROR: {e}")

        # Test content endpoint
        print("\n3. Testing content endpoint...")
        try:
            # Use a known good ECLI
            test_ecli = "ECLI:BE:CASS:2023:ARR.20231215.1F.2"
            text = self._fetch_full_text(test_ecli, 'FR')
            print(f"   Text length: {len(text)} characters")
            if text:
                print(f"   Sample: {text[:200]}...")
        except Exception as e:
            print(f"   ERROR: {e}")

        print("\nTest complete!")


def main():
    scraper = CassScraper()

    if len(sys.argv) < 2:
        print(
            "Usage: python bootstrap.py [bootstrap|update|test|status|clear-checkpoint] "
            "[--sample] [--sample-size N] [--no-checkpoint] [--clear-checkpoint]"
        )
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv
    no_checkpoint = "--no-checkpoint" in sys.argv
    clear_checkpoint = "--clear-checkpoint" in sys.argv
    sample_size = 12
    if "--sample-size" in sys.argv:
        idx = sys.argv.index("--sample-size")
        sample_size = int(sys.argv[idx + 1])

    if command == "status":
        checkpoint = scraper._load_checkpoint()
        print("Checkpoint status:")
        print(f"  Total fetched ECLIs: {len(checkpoint.get('fetched_eclis', []))}")
        print(f"  Total count: {checkpoint.get('total_fetched', 'N/A')}")
        print(f"  Last update: {checkpoint.get('last_update', 'N/A')}")

    elif command == "clear-checkpoint":
        scraper._clear_checkpoint()
        print("Checkpoint cleared")

    elif command == "test":
        scraper.test_connection()

    elif command == "bootstrap":
        if clear_checkpoint:
            scraper._clear_checkpoint()

        if sample_mode:
            stats = scraper.run_sample(n=sample_size)
            print(
                f"\nSample complete: "
                f"{stats.get('sample_records_saved', 0)} records saved to sample/"
            )
        else:
            scraper._use_checkpoint = not no_checkpoint
            stats = scraper.bootstrap()
            print(
                f"\nBootstrap complete: {stats['records_new']} new, "
                f"{stats['records_updated']} updated, "
                f"{stats['records_skipped']} skipped"
            )
        print(json.dumps(stats, indent=2))

    elif command == "update":
        stats = scraper.update()
        print(
            f"\nUpdate complete: {stats['records_new']} new, "
            f"{stats['records_updated']} updated"
        )
        print(json.dumps(stats, indent=2))

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
