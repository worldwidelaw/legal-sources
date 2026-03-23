#!/usr/bin/env python3
"""
ES/ConstitutionalCourt -- Spanish Constitutional Court Data Fetcher

Fetches Constitutional Court decisions (sentencias, autos, declaraciones) from
the Sistema HJ - Buscador de jurisprudencia constitucional.

Strategy:
  - Direct access via sequential IDs: /HJ/es/Resolucion/Show/{id}
  - IDs range from 1 (SENTENCIA 1/1981) to ~32000+ (current)
  - Parse HTML to extract full text from structured sections
  - ECLI identifier available for each decision

Endpoints:
  - Resolution: https://hj.tribunalconstitucional.es/HJ/es/Resolucion/Show/{id}
  - Document: https://hj.tribunalconstitucional.es/HJ/es/Resolucion/GetDocumentResolucion/{id}

Data:
  - Types: SENTENCIA (judgment), AUTO (order), DECLARACION (declaration)
  - Coverage: 1981-present
  - Languages: Spanish (primary), English, French translations available
  - License: Public domain (official court decisions)

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records for validation
  python bootstrap.py update             # Incremental update (since last run)
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import html
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List, Tuple

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.ES.ConstitutionalCourt")

# Base URL for HJ System
BASE_URL = "https://hj.tribunalconstitucional.es"

# Headers for requests
HEADERS = {
    "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
    "Accept": "text/html,application/xhtml+xml",
    "Accept-Language": "es-ES,es;q=0.9",
}

# Known ID ranges - the database has multiple valid ID ranges with gaps between them
# Range 1: Historical decisions (1981-2024) - IDs 1 to 30220 (~81% density, ~24,500 valid)
# Range 2: Recent decisions (2024-present) - IDs 31220 to ~32100 (with internal gaps of 200+ IDs)
# The gap from 30221 to 31219 is entirely empty (pre-allocated unused IDs)
# Within the recent range, there are gaps like 31680-31900 that are 200+ IDs wide
MIN_ID = 1
MAX_ID = 32500  # Upper bound, may need updates as new decisions are published

# Known valid ID ranges (start, end) - updated 2026-02-22
# Historical range has ~81% density (many valid IDs scattered)
# Recent range has ~50% density with large internal gaps
KNOWN_ID_RANGES = [
    (1, 30220),        # Main historical range (1981-2024)
    (31220, 32100),    # Recent/new decisions range (2024+)
]

# Checkpoint file for resuming across sessions
CHECKPOINT_FILE = Path(__file__).parent / "checkpoint.json"


class ConstitutionalCourtScraper(BaseScraper):
    """
    Scraper for ES/ConstitutionalCourt -- Spanish Constitutional Court.
    Country: ES
    URL: https://hj.tribunalconstitucional.es

    Data types: case_law
    Auth: none (public decisions)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url=BASE_URL,
            headers=HEADERS,
            timeout=60,
        )

        # Track the highest valid ID found
        self.max_valid_id = MIN_ID
        # Flag to control checkpoint usage
        self._use_checkpoint = True

    def _load_checkpoint(self) -> dict:
        """Load checkpoint from file if it exists."""
        if CHECKPOINT_FILE.exists():
            try:
                with open(CHECKPOINT_FILE, "r") as f:
                    return json.load(f)
            except json.JSONDecodeError:
                logger.warning("Invalid checkpoint file, starting fresh")
        return {"last_id": None, "fetched_count": 0, "max_id": None}

    def _save_checkpoint(self, checkpoint: dict):
        """Save checkpoint to file."""
        checkpoint["last_update"] = datetime.now(timezone.utc).isoformat()
        with open(CHECKPOINT_FILE, "w") as f:
            json.dump(checkpoint, f, indent=2)
        logger.debug(f"Checkpoint saved at ID {checkpoint.get('last_id')}")

    def _clear_checkpoint(self):
        """Clear checkpoint file."""
        if CHECKPOINT_FILE.exists():
            CHECKPOINT_FILE.unlink()
            logger.info("Checkpoint cleared")

    def _is_valid_resolution_page(self, html_content: str) -> bool:
        """Check if the HTML page contains a valid resolution (not the search page)."""
        # If it's the search page, it will have the search form title
        if 'Sistema HJ - Buscador de jurisprudencia constitucional</title>' in html_content:
            # But also check if it has resolution content
            if '<div id="complete_resolucion"' not in html_content:
                return False

        # Valid resolution pages have ECLI and resolution content
        return 'ECLI:ES:TC:' in html_content and 'class="section"' in html_content

    def _parse_resolution(self, html_content: str, resolution_id: int) -> Optional[Dict[str, Any]]:
        """
        Parse HTML content of a resolution page to extract metadata and full text.

        Returns None if the page is not a valid resolution.
        """
        if not self._is_valid_resolution_page(html_content):
            return None

        # Unescape HTML entities for better regex matching
        html_decoded = html.unescape(html_content)

        doc = {}
        doc["_internal_id"] = resolution_id

        # Extract title from page title: "Sistema HJ - Resolución: SENTENCIA 1/1981"
        title_match = re.search(r'<title>Sistema HJ - Resolución: ([^<]+)</title>', html_decoded)
        if title_match:
            doc["title"] = title_match.group(1).strip()
        else:
            doc["title"] = ""

        # Also try to get full title from resolucion-identifier which includes date
        identifier_match = re.search(
            r'<li id="resolucion-identifier">\s*<h2>\s*([^<]+(?:,\s*de\s+\d{1,2}\s+de\s+\w+)?)',
            html_decoded,
            re.DOTALL
        )
        if identifier_match:
            full_title = identifier_match.group(1).strip()
            full_title = re.sub(r'\s+', ' ', full_title)
            if full_title and len(full_title) > len(doc.get("title", "")):
                doc["title"] = full_title

        # Extract ECLI: ECLI:ES:TC:YYYY:NNN or ECLI:ES:TC:YYYY:NNNA
        ecli_match = re.search(r'ECLI:ES:TC:\d{4}:\d+[AD]?', html_decoded)
        if ecli_match:
            doc["ecli"] = ecli_match.group(0)
        else:
            doc["ecli"] = ""

        # Parse resolution type and number from title
        type_match = re.match(r'(SENTENCIA|AUTO|DECLARACI[ÓO]N)\s+(\d+)/(\d{4})', doc["title"], re.IGNORECASE)
        if type_match:
            doc["resolution_type"] = type_match.group(1).upper().replace('Ó', 'O')
            doc["resolution_number"] = int(type_match.group(2))
            doc["resolution_year"] = int(type_match.group(3))
        else:
            doc["resolution_type"] = ""
            doc["resolution_number"] = None
            doc["resolution_year"] = None

        # Extract date from title: "SENTENCIA 1/1981, de 26 de enero"
        date_match = re.search(
            r'de\s+(\d{1,2})\s+de\s+(enero|febrero|marzo|abril|mayo|junio|julio|agosto|septiembre|octubre|noviembre|diciembre)(?:\s+de\s+(\d{4}))?',
            doc["title"],
            re.IGNORECASE
        )
        if date_match:
            day = int(date_match.group(1))
            month_name = date_match.group(2).lower()
            year = int(date_match.group(3)) if date_match.group(3) else doc.get("resolution_year")

            month_map = {
                'enero': 1, 'febrero': 2, 'marzo': 3, 'abril': 4,
                'mayo': 5, 'junio': 6, 'julio': 7, 'agosto': 8,
                'septiembre': 9, 'octubre': 10, 'noviembre': 11, 'diciembre': 12
            }
            month = month_map.get(month_name, 1)

            if year:
                doc["date"] = f"{year:04d}-{month:02d}-{day:02d}"
            else:
                doc["date"] = ""
        else:
            # Try to get from resolution year
            if doc.get("resolution_year"):
                doc["date"] = f"{doc['resolution_year']}-01-01"
            else:
                doc["date"] = ""

        # Extract BOE info: [Núm, 47 ] 24/02/1981
        boe_match = re.search(r'\[N[úu]m,?\s*(\d+)\s*\]\s*(\d{2}/\d{2}/\d{4})', html_decoded)
        if boe_match:
            doc["boe_number"] = int(boe_match.group(1))
            boe_date = boe_match.group(2)
            # Convert DD/MM/YYYY to YYYY-MM-DD
            parts = boe_date.split('/')
            doc["boe_date"] = f"{parts[2]}-{parts[1]}-{parts[0]}"
        else:
            doc["boe_number"] = None
            doc["boe_date"] = ""

        # Extract chamber/organ: Sala Segunda, Pleno, etc.
        chamber_match = re.search(r'<th>\s*[ÓO]rgano\s*</th>\s*<td>\s*([^<]+)\s*</td>', html_decoded, re.IGNORECASE)
        if chamber_match:
            doc["chamber"] = html.unescape(chamber_match.group(1).strip())
        else:
            doc["chamber"] = ""

        # Extract magistrates
        magistrates_match = re.search(
            r'<th>\s*Magistrados\s*</th>\s*<td>\s*<span>\s*<p>([^<]+)',
            html_decoded,
            re.IGNORECASE | re.DOTALL
        )
        if magistrates_match:
            doc["magistrates"] = html.unescape(magistrates_match.group(1).strip())
        else:
            doc["magistrates"] = ""

        # Extract case type and number
        case_match = re.search(
            r'<label title="([^"]+)">\s*([^<]+)\s*</label>',
            html_decoded
        )
        if case_match:
            case_info = html.unescape(case_match.group(2).strip())
            doc["case_number"] = case_info

            # Parse case type from the info
            if 'amparo' in case_info.lower():
                doc["case_type"] = "Recurso de amparo"
            elif 'inconstitucionalidad' in case_info.lower():
                doc["case_type"] = "Recurso de inconstitucionalidad"
            elif 'cuesti' in case_info.lower() and 'inconstitucionalidad' in case_info.lower():
                doc["case_type"] = "Cuestión de inconstitucionalidad"
            elif 'conflicto' in case_info.lower():
                doc["case_type"] = "Conflicto de competencia"
            else:
                doc["case_type"] = case_info.split()[0] if case_info else ""
        else:
            doc["case_type"] = ""
            doc["case_number"] = ""

        # Extract full text from structured sections
        full_text_parts = []

        # 1. Header/preamble (resolucion-cabecera)
        cabecera_match = re.search(
            r'<p id="resolucion-cabecera"[^>]*>([^<]+(?:<[^>]+>[^<]*</[^>]+>[^<]*)*)</p>',
            html_decoded,
            re.DOTALL
        )
        if cabecera_match:
            text = self._clean_html(cabecera_match.group(1))
            if text:
                full_text_parts.append(text)

        # 2. Kings name section
        kings_match = re.search(
            r'<h3 id="kings-name"[^>]*>([^<]+(?:<[^>]+>[^<]*</[^>]+>[^<]*)*)</h3>',
            html_decoded,
            re.DOTALL
        )
        if kings_match:
            text = self._clean_html(kings_match.group(1))
            if text:
                full_text_parts.append(text)

        # 3. Sentencia description
        sentencia_match = re.search(
            r'<p id="resolucion-sentencia"[^>]*>(.*?)</p>',
            html_decoded,
            re.DOTALL
        )
        if sentencia_match:
            text = self._clean_html(sentencia_match.group(1))
            if text:
                full_text_parts.append(text)

        # 4. Antecedentes section
        antecedentes_match = re.search(
            r'<div class="section" id="antecedentes-container">(.*?)</div>\s*(?:<div class="section"|$)',
            html_decoded,
            re.DOTALL
        )
        if antecedentes_match:
            section_title = "I. Antecedentes"
            full_text_parts.append(f"\n{section_title}\n")

            items = re.findall(
                r'<div class="section_item-container">\s*<p>(.*?)</p>\s*</div>',
                antecedentes_match.group(1),
                re.DOTALL
            )
            for item in items:
                text = self._clean_html(item)
                if text:
                    full_text_parts.append(text)

        # 5. Fundamentos jurídicos section
        fundamentos_match = re.search(
            r'<div class="section" id="fundamentos-container">(.*?)</div>\s*(?:<div class="section"|$)',
            html_decoded,
            re.DOTALL
        )
        if fundamentos_match:
            section_title = "II. Fundamentos jurídicos"
            full_text_parts.append(f"\n{section_title}\n")

            items = re.findall(
                r'<div class="section_item-container">\s*<p>(.*?)</p>\s*</div>',
                fundamentos_match.group(1),
                re.DOTALL
            )
            for item in items:
                text = self._clean_html(item)
                if text:
                    full_text_parts.append(text)

        # 6. Fallo (ruling) section
        dictamen_match = re.search(
            r'<div class="section" id="dictamen-container">(.*?)</div>\s*(?:<div class="section"|$)',
            html_decoded,
            re.DOTALL
        )
        if dictamen_match:
            section_title = "Fallo"
            full_text_parts.append(f"\n{section_title}\n")

            # Extract all paragraphs in the fallo
            for p_match in re.finditer(r'<p[^>]*>(.*?)</p>', dictamen_match.group(1), re.DOTALL):
                text = self._clean_html(p_match.group(1))
                if text:
                    full_text_parts.append(text)

        # 7. Votos particulares (dissenting opinions) if any
        votos_match = re.search(
            r'<div class="section" id="votos-container">(.*?)</div>\s*(?:</div>|$)',
            html_decoded,
            re.DOTALL
        )
        if votos_match and votos_match.group(1).strip():
            section_title = "Votos particulares"
            full_text_parts.append(f"\n{section_title}\n")

            for p_match in re.finditer(r'<p[^>]*>(.*?)</p>', votos_match.group(1), re.DOTALL):
                text = self._clean_html(p_match.group(1))
                if text:
                    full_text_parts.append(text)

        doc["full_text"] = '\n\n'.join(full_text_parts)

        # Generate URL
        doc["url"] = f"{BASE_URL}/HJ/es/Resolucion/Show/{resolution_id}"

        return doc

    def _clean_html(self, text: str) -> str:
        """Clean HTML: decode entities, remove tags, normalize whitespace."""
        if not text:
            return ""

        # Decode HTML entities
        text = html.unescape(text)

        # Replace <br> with newlines
        text = re.sub(r'<br\s*/?>', '\n', text, flags=re.IGNORECASE)

        # Remove span item-number elements but keep content
        text = re.sub(r'<span class="item-number[^"]*">([^<]+)</span>', r'\1', text)

        # Remove all other HTML tags
        text = re.sub(r'<[^>]+>', ' ', text)

        # Normalize whitespace
        text = re.sub(r'[ \t]+', ' ', text)
        text = re.sub(r'\n\s*\n+', '\n\n', text)

        return text.strip()

    def _fetch_resolution(self, resolution_id: int) -> Optional[Dict[str, Any]]:
        """
        Fetch a single resolution by its ID.

        Returns None if the ID doesn't correspond to a valid resolution.
        """
        url = f"/HJ/es/Resolucion/Show/{resolution_id}"

        try:
            self.rate_limiter.wait()
            resp = self.client.get(url)
            resp.raise_for_status()

            html_content = resp.content.decode('utf-8', errors='replace')
            return self._parse_resolution(html_content, resolution_id)

        except Exception as e:
            logger.warning(f"Failed to fetch resolution {resolution_id}: {e}")
            return None

    def _find_max_valid_id_in_range(self, low: int, high: int) -> int:
        """
        Binary search to find the highest valid resolution ID within a range.
        Returns -1 if no valid ID found in range.
        """
        original_low = low

        # First, verify the high bound has any valid IDs
        doc = self._fetch_resolution(high)
        if doc is not None:
            # Need to search higher
            while doc is not None and high < 50000:
                high += 500
                doc = self._fetch_resolution(high)

        # Binary search
        while low < high:
            mid = (low + high + 1) // 2
            doc = self._fetch_resolution(mid)

            if doc is not None:
                low = mid
            else:
                high = mid - 1

        # Verify we found a valid ID
        if low >= original_low:
            doc = self._fetch_resolution(low)
            if doc is not None:
                return low

        return -1

    def _find_max_valid_id(self) -> int:
        """
        Find the highest valid resolution ID (should be in the recent range 31220+).
        """
        recent_max = self._find_max_valid_id_in_range(31220, 35000)
        if recent_max >= 31220:
            logger.info(f"Found max valid ID: {recent_max}")
            return recent_max

        # Fallback - try historical range
        historical_max = self._find_max_valid_id_in_range(1, 30500)
        if historical_max > 0:
            logger.info(f"Found max valid ID in historical range: {historical_max}")
            return historical_max

        return 31649  # Fallback to known valid ID

    def _get_valid_ranges(self) -> List[Tuple[int, int]]:
        """
        Get list of valid ID ranges to iterate, sorted for newest first.
        Each range is (start_id, end_id) where we iterate from end_id down to start_id.

        The database has two distinct valid ID ranges:
        - Historical range: IDs 1 to 30220 (decisions from 1981 to ~2024, ~81% density)
        - Recent range: IDs 31220 to ~32100 (decisions from ~2024 to present, ~50% density)

        There's an empty gap from 30221 to 31219.
        Within the recent range, there are internal gaps of 200+ consecutive empty IDs.
        """
        # Find actual max ID in the recent range (31220+)
        logger.info("Finding maximum valid resolution ID in recent range...")
        recent_max = self._find_max_valid_id_in_range(31220, MAX_ID)
        if recent_max < 31220:
            recent_max = 32050  # Fallback based on Feb 2026 data
        logger.info(f"Recent range max ID: {recent_max}")

        # Build the two known ranges
        # Historical range - IDs 1-30220 (30221 is first empty ID)
        # Recent range - found the actual max dynamically
        ranges = [
            (31220, recent_max),   # Recent range first (newest decisions)
            (1, 30220),            # Historical range second
        ]

        return ranges

    def fetch_all(self) -> Generator[dict, None, None]:
        """
        Yield all resolutions by iterating through known valid ID ranges.

        The database has multiple valid ID ranges with gaps between them:
        - Range 1: IDs 1-30225 (historical decisions 1981-2024)
        - Range 2: IDs 31220-31700+ (recent decisions 2024+)

        Iterates from highest to lowest within each range, processing newest range first.
        Supports checkpoint/resume for large runs via self._use_checkpoint flag.
        """
        use_checkpoint = self._use_checkpoint

        # Load checkpoint
        if use_checkpoint:
            checkpoint = self._load_checkpoint()
            current_range_idx = checkpoint.get("range_idx", 0)
            start_id = checkpoint.get("last_id")
            fetched_count = checkpoint.get("fetched_count", 0)
            ranges = checkpoint.get("ranges")

            if start_id and ranges:
                logger.info(f"Resuming from checkpoint: range {current_range_idx}, ID {start_id}, {fetched_count} fetched")
            else:
                ranges = None
                current_range_idx = 0
                start_id = None
                fetched_count = 0
        else:
            checkpoint = {}
            ranges = None
            current_range_idx = 0
            start_id = None
            fetched_count = 0

        # Get valid ranges if not in checkpoint
        if ranges is None:
            ranges = self._get_valid_ranges()
            logger.info(f"Will iterate through {len(ranges)} ID ranges: {ranges}")

        # Process each range
        for range_idx, (range_start, range_end) in enumerate(ranges):
            # Skip ranges we've already completed
            if range_idx < current_range_idx:
                continue

            logger.info(f"Processing range {range_idx + 1}/{len(ranges)}: IDs {range_end} down to {range_start}")

            # Determine starting point for this range
            if range_idx == current_range_idx and start_id:
                current_id = start_id
            else:
                current_id = range_end

            # Note: We do NOT use a consecutive failure limit here because:
            # 1. The recent range (31220-32100) has internal gaps of 200+ empty IDs
            # 2. The historical range (1-30220) has ~19% empty IDs scattered throughout
            # Instead, we simply iterate through the entire range

            for resolution_id in range(current_id, range_start - 1, -1):
                doc = self._fetch_resolution(resolution_id)

                if doc is None:
                    # Empty ID, just continue - don't count or break
                    continue

                if not doc.get("full_text"):
                    logger.warning(f"No full text for resolution {resolution_id}, skipping")
                    continue

                fetched_count += 1

                # Save checkpoint periodically
                if use_checkpoint and fetched_count % 100 == 0:
                    checkpoint = {
                        "range_idx": range_idx,
                        "last_id": resolution_id,
                        "fetched_count": fetched_count,
                        "ranges": ranges,
                    }
                    self._save_checkpoint(checkpoint)
                    logger.info(f"Checkpoint saved: {fetched_count} resolutions, range {range_idx + 1}, ID {resolution_id}")

                yield doc

            logger.info(f"Completed range {range_idx + 1}: {fetched_count} total resolutions so far")

        # Clear checkpoint on successful completion
        if use_checkpoint:
            self._clear_checkpoint()
            logger.info(f"Bootstrap complete - {fetched_count} total resolutions - checkpoint cleared")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """
        Yield resolutions added since the given date.

        Since the HJ system uses sequential IDs, we find the max ID and
        work backwards until we hit resolutions older than 'since'.
        """
        logger.info(f"Fetching updates since {since.isoformat()}")

        max_id = self._find_max_valid_id()

        for resolution_id in range(max_id, MIN_ID - 1, -1):
            doc = self._fetch_resolution(resolution_id)

            if doc is None:
                continue

            # Check if resolution date is before 'since'
            if doc.get("date"):
                try:
                    doc_date = datetime.fromisoformat(doc["date"])
                    if doc_date < since.replace(tzinfo=None):
                        logger.info(f"Reached resolution from {doc['date']}, stopping updates")
                        break
                except ValueError:
                    pass  # Continue if date parsing fails

            if not doc.get("full_text"):
                continue

            yield doc

    def normalize(self, raw: dict) -> dict:
        """
        Transform raw resolution data into standard schema.

        CRITICAL: Includes full text in the 'text' field.
        """
        ecli = raw.get("ecli", "")
        title = raw.get("title", "")
        full_text = raw.get("full_text", "")
        date_str = raw.get("date", "")
        url = raw.get("url", "")

        # Use ECLI as primary ID, fallback to internal ID
        doc_id = ecli if ecli else f"TC-{raw.get('_internal_id', 'unknown')}"

        return {
            # Required base fields
            "_id": doc_id,
            "_source": "ES/ConstitutionalCourt",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            # Standard fields
            "title": title,
            "text": full_text,  # MANDATORY FULL TEXT
            "date": date_str,
            "url": url,
            # Resolution identifiers
            "ecli": ecli,
            "resolution_type": raw.get("resolution_type", ""),
            "resolution_number": raw.get("resolution_number"),
            "resolution_year": raw.get("resolution_year"),
            # Court information
            "chamber": raw.get("chamber", ""),
            "magistrates": raw.get("magistrates", ""),
            # Case information
            "case_type": raw.get("case_type", ""),
            "case_number": raw.get("case_number", ""),
            # Publication information
            "boe_date": raw.get("boe_date", ""),
            "boe_number": raw.get("boe_number"),
            # Language
            "language": "es",
        }

    def test_connection(self):
        """Quick connectivity test."""
        print("Testing HJ System (Tribunal Constitucional) endpoints...")

        # Test a known good resolution from historical range
        print("\n1. Testing resolution endpoint (ID 1 - SENTENCIA 1/1981)...")
        try:
            doc = self._fetch_resolution(1)
            if doc:
                print(f"   Title: {doc.get('title', 'N/A')}")
                print(f"   ECLI: {doc.get('ecli', 'N/A')}")
                print(f"   Date: {doc.get('date', 'N/A')}")
                print(f"   Text length: {len(doc.get('full_text', ''))} characters")
                print(f"   Text sample: {doc.get('full_text', '')[:200]}...")
            else:
                print("   ERROR: Could not fetch resolution")
        except Exception as e:
            print(f"   ERROR: {e}")

        # Test a resolution from the recent range
        print("\n2. Testing recent range (ID 31500)...")
        try:
            doc = self._fetch_resolution(31500)
            if doc:
                print(f"   Title: {doc.get('title', 'N/A')}")
                print(f"   ECLI: {doc.get('ecli', 'N/A')}")
                print(f"   Date: {doc.get('date', 'N/A')}")
                print(f"   Text length: {len(doc.get('full_text', ''))} characters")
            else:
                print("   ID 31500 not valid (expected if in gap)")
        except Exception as e:
            print(f"   ERROR: {e}")

        # Test the gap region (should be empty)
        print("\n3. Testing gap region (ID 31000 - should be empty)...")
        try:
            doc = self._fetch_resolution(31000)
            if doc:
                print(f"   UNEXPECTED: ID 31000 is valid - gap may have changed")
            else:
                print("   Confirmed: ID 31000 is in the empty gap (expected)")
        except Exception as e:
            print(f"   ERROR: {e}")

        # Test finding max ID and ranges
        print("\n4. Finding valid ID ranges...")
        try:
            ranges = self._get_valid_ranges()
            print(f"   Known valid ranges: {KNOWN_ID_RANGES}")
            print(f"   Active ranges (with actual max): {ranges}")

            total_potential = sum(end - start + 1 for start, end in ranges)
            print(f"   Total potential IDs to check: ~{total_potential:,}")
        except Exception as e:
            print(f"   ERROR: {e}")

        print("\nTest complete!")


def main():
    scraper = ConstitutionalCourtScraper()

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
        print(f"  Last ID processed: {checkpoint.get('last_id', 'N/A')}")
        print(f"  Total fetched: {checkpoint.get('fetched_count', 0)}")
        print(f"  Max ID: {checkpoint.get('max_id', 'N/A')}")
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
