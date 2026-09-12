#!/usr/bin/env python3
"""
INTL/Juricaf -- Francophone Supreme Court Decisions

Fetches supreme court decisions from juricaf.org, the AHJUCAF database of
francophone judicial decisions covering 48 countries/institutions.

Strategy:
  - Enumerate decision URLs from the published sitemap: /sitemap.xml fans out to
    38 chunks of up to 50,000 <loc> entries each (~1.85M decisions total)
  - Fetch each decision page and extract full text from <article> tag
  - Parse metadata from header (date, court, case number)

The sitemap replaced search pagination (`/recherche/+/facet_pays:X?page=N`) for
two reasons: robots.txt disallows `?page=` and `?tri=` for `User-agent: *` while
explicitly publishing the sitemap, and paginating 10 results at a time cost
~186,000 requests just to learn the URLs.

Data Coverage:
  - ~1.85M decisions from 48 francophone jurisdictions
  - Supreme/cassation courts of France, Belgium, Luxembourg, Switzerland,
    Canada, Monaco, Senegal, Madagascar, Benin, Mali, Niger, etc.
  - International courts: OHADA, UEMOA, CEMAC, ECOWAS, ECHR, CJEU

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records
  python bootstrap.py update             # Incremental update
"""

import sys
import json
import logging
import re
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional
from html import unescape

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.INTL.Juricaf")

BASE_URL = "https://juricaf.org"
SITEMAP_INDEX = f"{BASE_URL}/sitemap.xml"

# Save progress every N decision URLs so a killed run replays at most this many.
CHECKPOINT_EVERY = 25

# Map the country prefix of a decision slug (FRANCE-CONSEILDETAT-18850213-62565)
# to our ISO code. Covers all 48 jurisdictions Juricaf facets on — the previous
# hand-maintained search list omitted nine of them (Egypte, Hongrie, Maurice,
# Pologne, Rwanda, Sao Tomé, Vietnam, CADHP, OEA).
COUNTRY_MAP = {
    "ANDORRE": "AD", "BELGIQUE": "BE", "BENIN": "BJ", "BULGARIE": "BG",
    "BURKINAFASO": "BF", "BURUNDI": "BI", "CAMBODGE": "KH", "CAMEROUN": "CM",
    "CANADA": "CA", "COMORES": "KM", "CONGO": "CG",
    "CONGODEMOCRATIQUE": "CD", "COTEDIVOIRE": "CI", "EGYPTE": "EG",
    "FRANCE": "FR", "GABON": "GA", "GUINEE": "GN", "HAITI": "HT",
    "HONGRIE": "HU", "LIBAN": "LB", "LUXEMBOURG": "LU", "MADAGASCAR": "MG",
    "MALI": "ML", "MAROC": "MA", "MAURICE": "MU", "MAURITANIE": "MR",
    "MONACO": "MC", "NIGER": "NE", "POLOGNE": "PL",
    "REPUBLIQUECENTRAFRICAINE": "CF", "REPUBLIQUETCHEQUE": "CZ",
    "ROUMANIE": "RO", "RWANDA": "RW", "SAOTOMEETPRINCIPE": "ST",
    "SENEGAL": "SN", "SUISSE": "CH", "TCHAD": "TD", "TOGO": "TG",
    "TUNISIE": "TN", "VIETNAM": "VN",
    # Regional and international courts
    "CADHP": "INTL", "CEDEAO": "INTL", "CEMAC": "INTL", "OEA": "INTL",
    "OHADA": "INTL", "UEMOA": "INTL",
    "CEDH": "CoE", "CJUE": "EU",
}


class JuricafScraper(BaseScraper):
    """Scraper for Juricaf francophone supreme court decisions."""

    def __init__(self, source_dir: str = None, sample_mode: bool = False):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self._sample_mode = sample_mode
        self._checkpoint_path = self.source_dir / "data" / "checkpoint.json"
        self._checkpoint = self._load_checkpoint()
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "LegalDataHunter/1.0 (legal research; +https://github.com/worldwidelaw/legal-sources)",
            "Accept": "text/html,application/xhtml+xml",
            "Accept-Language": "fr,en",
        })

    # ── Checkpoint ────────────────────────────────────────────────────

    def _load_checkpoint(self) -> dict:
        """Resume state: which sitemap chunks are done, and where the current one stopped."""
        empty = {"sitemaps_done": [], "cursor": {}}
        if self._sample_mode or not self._checkpoint_path.exists():
            return empty
        try:
            with open(self._checkpoint_path) as f:
                data = json.load(f)
            return {
                "sitemaps_done": list(data.get("sitemaps_done", [])),
                "cursor": dict(data.get("cursor", {})),
            }
        except (json.JSONDecodeError, OSError) as e:
            logger.warning(f"Ignoring unreadable checkpoint ({e}); starting from scratch")
            return empty

    def _save_checkpoint(self):
        if self._sample_mode:
            return
        self._checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self._checkpoint_path.with_suffix(".tmp")
        with open(tmp, "w") as f:
            json.dump(self._checkpoint, f)
        tmp.replace(self._checkpoint_path)

    # ── Enumeration ───────────────────────────────────────────────────

    def _get(self, url: str, timeout: int = 60):
        """Rate-limited GET. Returns the response, or None on error/non-200."""
        time.sleep(1)
        try:
            resp = self.session.get(url, timeout=timeout)
        except requests.RequestException as e:
            logger.error(f"  Error fetching {url}: {e}")
            return None
        if resp.status_code != 200:
            logger.warning(f"  {url} returned {resp.status_code}")
            return None
        return resp

    def _sitemap_chunks(self) -> list:
        """Return the sitemap chunk URLs listed in the sitemap index."""
        resp = self._get(SITEMAP_INDEX)
        if resp is None:
            raise RuntimeError(f"Could not read sitemap index {SITEMAP_INDEX}")
        chunks = re.findall(r"<loc>\s*([^<]+?)\s*</loc>", resp.text)
        if not chunks:
            raise RuntimeError(f"Sitemap index {SITEMAP_INDEX} listed no chunks")
        logger.info(f"Sitemap index: {len(chunks)} chunks")
        return chunks

    def _sitemap_entries(self, chunk_url: str) -> list:
        """Return ``[(decision_url, lastmod), ...]`` for one sitemap chunk."""
        resp = self._get(chunk_url)
        if resp is None:
            return []
        entries = []
        for block in re.finditer(r"<url>(.*?)</url>", resp.text, re.DOTALL):
            body = block.group(1)
            loc = re.search(r"<loc>\s*([^<]+?)\s*</loc>", body)
            if not loc or "/arret/" not in loc.group(1):
                continue
            lastmod = re.search(r"<lastmod>\s*([^<]+?)\s*</lastmod>", body)
            entries.append((loc.group(1), lastmod.group(1) if lastmod else None))
        logger.info(f"  {chunk_url.rsplit('/', 1)[-1]}: {len(entries)} decision URLs")
        return entries

    @staticmethod
    def _resume_index(entries: list, cursor: dict) -> int:
        """Where to restart inside a sitemap chunk we were part-way through.

        Juricaf regenerates its sitemaps as decisions are added, so a stored
        offset can drift. Trust it only if the URL still sits at that offset;
        otherwise look the URL up, and fall back to replaying the chunk (already
        stored decisions are skipped without a fetch anyway).
        """
        idx = cursor.get("index")
        last_url = cursor.get("last_url")
        if not last_url:
            return 0
        if isinstance(idx, int) and 0 <= idx < len(entries) and entries[idx][0] == last_url:
            return idx + 1
        for i, (url, _lastmod) in enumerate(entries):
            if url == last_url:
                return i + 1
        logger.warning("  Checkpointed URL is gone from this chunk; replaying it")
        return 0

    def _id_for_url(self, url: str) -> str:
        """The ``_id`` normalize() will assign, derivable without fetching the page."""
        slug = url.split("/arret/")[-1] if "/arret/" in url else url
        return f"juricaf-{slug}"

    def _fetch_decision(self, url: str) -> Optional[dict]:
        """Fetch and parse a single decision page."""
        time.sleep(1)
        try:
            resp = self.session.get(url, timeout=30)
            if resp.status_code != 200:
                return None
        except requests.RequestException as e:
            logger.error(f"  Error fetching {url}: {e}")
            return None

        html = resp.text

        # Extract title from h1
        h1 = re.search(r'<h1[^>]*>(.*?)</h1>', html, re.DOTALL)
        title = ""
        if h1:
            title = re.sub(r'<[^>]+>', '', h1.group(1)).strip()
            title = re.sub(r'^\|\s*', '', title).strip()

        # Extract full text from <article>
        article = re.search(r'<article[^>]*>(.*?)</article>', html, re.DOTALL)
        text = ""
        if article:
            text = re.sub(r'<[^>]+>', '', article.group(1))
            text = unescape(text).strip()
            # Clean up excessive whitespace but preserve paragraph breaks
            text = re.sub(r'[ \t]+', ' ', text)
            text = re.sub(r'\n{3,}', '\n\n', text)
            text = text.strip()

        # Parse URL slug for metadata
        slug = url.split("/arret/")[-1] if "/arret/" in url else ""
        parts = slug.split("-") if slug else []

        # Extract date from header metadata
        date_match = re.search(r'(\d{2}/\d{2}/\d{4})', html)
        date_str = None
        if date_match:
            try:
                dt = datetime.strptime(date_match.group(1), "%d/%m/%Y")
                date_str = dt.strftime("%Y-%m-%d")
            except ValueError:
                pass

        # Fallback: extract date from slug (format: YYYYMMDD)
        if not date_str and len(parts) >= 3:
            date_part = parts[-2] if len(parts[-2]) == 8 else None
            if not date_part:
                for p in parts:
                    if len(p) == 8 and p.isdigit():
                        date_part = p
                        break
            if date_part and date_part.isdigit():
                try:
                    dt = datetime.strptime(date_part, "%Y%m%d")
                    date_str = dt.strftime("%Y-%m-%d")
                except ValueError:
                    pass

        # Extract country and court from slug
        country_name = parts[0] if parts else ""
        court_parts = []
        for p in parts[1:]:
            if len(p) == 8 and p.isdigit():
                break
            court_parts.append(p)
        court = " ".join(court_parts) if court_parts else ""

        # Extract case number from header
        case_num_match = re.search(r'N°\s*([^\s<]+)', html)
        case_number = case_num_match.group(1) if case_num_match else (parts[-1] if parts else "")

        # Extract ECLI if present
        ecli_match = re.search(r'(ECLI:[^\s<"]+)', html)
        ecli = ecli_match.group(1) if ecli_match else None

        return {
            "slug": slug,
            "title": title,
            "text": text,
            "date": date_str,
            "court": court,
            "country_origin": country_name,
            "case_number": case_number,
            "ecli": ecli,
            "url": url,
        }

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield every decision listed in the sitemap.

        Enumeration and fetching are interleaved, so records reach storage from
        the first chunk onward and a run cut short by the fleet's 100-hour cap
        still leaves a usable partial corpus. At ~1.85M decisions and one
        request per second this source cannot finish in a single slot, so
        successive runs must continue rather than restart (#1425): progress is
        checkpointed every CHECKPOINT_EVERY URLs, and any decision already in
        storage is skipped without a request.
        """
        chunks = self._sitemap_chunks()
        if self._sample_mode:
            # The trailing chunk is the short one (~3K URLs vs 50K).
            chunks = chunks[-1:]

        done = set(self._checkpoint["sitemaps_done"])
        cursor = self._checkpoint["cursor"]
        if done:
            logger.info(f"Resuming: {len(done)}/{len(chunks)} sitemap chunks already complete")

        for chunk in chunks:
            if chunk in done:
                continue

            entries = self._sitemap_entries(chunk)
            if not entries:
                continue

            start = self._resume_index(entries, cursor) if cursor.get("sitemap") == chunk else 0
            if start:
                logger.info(f"  Resuming at entry {start}/{len(entries)}")
            if self._sample_mode:
                # Stride so the samples span several jurisdictions, not just
                # the first court in the chunk.
                entries = entries[start::max(1, len(entries) // 15)]
                start = 0

            fetched = skipped = 0
            for offset, (url, _lastmod) in enumerate(entries[start:], start=start):
                if not self._sample_mode and self.storage.exists(self._id_for_url(url)):
                    skipped += 1
                else:
                    try:
                        decision = self._fetch_decision(url)
                        if decision and decision.get("text") and len(decision["text"]) > 50:
                            fetched += 1
                            yield decision
                        else:
                            # Juricaf carries metadata-only stubs for some older
                            # decisions (empty <article>); nothing to extract.
                            logger.debug(f"  No text for {url}")
                    except Exception as e:
                        logger.error(f"  Error on {url}: {e}")

                if not self._sample_mode and offset % CHECKPOINT_EVERY == 0:
                    self._checkpoint["cursor"] = {
                        "sitemap": chunk, "index": offset, "last_url": url,
                    }
                    self._save_checkpoint()

            logger.info(
                f"  {chunk.rsplit('/', 1)[-1]}: {fetched} decisions fetched, "
                f"{skipped} already stored"
            )
            # New decisions are appended to the trailing chunk until it fills up,
            # so never retire it — re-walking costs nothing once its decisions
            # are stored, since the exists() check skips them without a request.
            if chunk != chunks[-1]:
                self._checkpoint["sitemaps_done"].append(chunk)
            self._checkpoint["cursor"] = {}
            cursor = {}
            self._save_checkpoint()

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Yield decisions whose sitemap <lastmod> is on or after ``since``."""
        cutoff = since.date().isoformat()
        for chunk in self._sitemap_chunks():
            for url, lastmod in self._sitemap_entries(chunk):
                if lastmod and lastmod < cutoff:
                    continue
                try:
                    decision = self._fetch_decision(url)
                    if decision and decision.get("text") and len(decision["text"]) > 50:
                        yield decision
                except Exception as e:
                    logger.error(f"  Error on {url}: {e}")

    def normalize(self, raw: dict) -> dict:
        """Transform raw decision data into standard schema."""
        slug = raw.get("slug", "unknown")
        country_origin = raw.get("country_origin", "")
        iso_code = COUNTRY_MAP.get(country_origin.upper(), "INTL")
        court = raw.get("court", "")

        # Build a readable court name
        court_name = court.replace("COUR", "Cour").replace("SUPREME", "suprême")
        if not court_name:
            court_name = country_origin or "Unknown Court"

        title = raw.get("title", "")
        if not title:
            title = f"{country_origin} {court} {raw.get('case_number', slug)}"

        return {
            "_id": f"juricaf-{slug}",
            "_source": "INTL/Juricaf",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": raw.get("text", ""),
            "date": raw.get("date"),
            "url": raw.get("url", ""),
            "court": court_name,
            "country_origin": iso_code,
            "case_number": raw.get("case_number", ""),
            "ecli": raw.get("ecli"),
        }


# ── CLI entry point ──────────────────────────────────────────────────

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|bootstrap-fast|update|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample = "--sample" in sys.argv
    scraper = JuricafScraper(sample_mode=sample)

    if command == "test":
        # Quick connectivity test against the smallest sitemap chunk
        chunks = scraper._sitemap_chunks()
        urls = [u for u, _lm in scraper._sitemap_entries(chunks[-1])]
        print(f"Found {len(urls)} decisions in {chunks[-1]}")
        if urls:
            decision = scraper._fetch_decision(urls[0])
            if decision:
                print(f"Title: {decision['title']}")
                print(f"Date: {decision['date']}")
                print(f"Text length: {len(decision.get('text', ''))}")
                print(f"Text preview: {decision.get('text', '')[:200]}...")
        sys.exit(0)

    if command in ("bootstrap", "bootstrap-fast", "bootstrap_fast"):
        if sample or command == "bootstrap":
            result = scraper.bootstrap(sample_mode=sample, sample_size=15)
        else:
            result = scraper.bootstrap_fast()
        print(json.dumps(result, indent=2, default=str))
    elif command == "update":
        result = scraper.update()
        print(json.dumps(result, indent=2, default=str))
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
