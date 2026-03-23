#!/usr/bin/env python3
"""
GR/HellenicParliament -- Greek Legislation (Raptarchis Code) Data Fetcher

Fetches Greek legislation from the Permanent Greek Legislation Code (Raptarchis),
available as the AI-team-UoA/greek_legal_code dataset on HuggingFace.

Strategy:
  - Downloads parquet files from HuggingFace (volume config: train+test+validation)
  - Each record has full text and a volume label (0-46, representing 47 thematic volumes)
  - Extracts title and metadata (law number, date, type) from the text using regex
  - ~47,000 legal resources covering 1834-2015

Note: The hellenicparliament.gr API exists but returns HTTP 403 for all
programmatic access (Akamai CDN blocks non-browser requests). This source
uses the Raptarchis academic dataset as an alternative.

Data:
  - Greek legislation from 1834 to 2015
  - 47 thematic volumes, 389 chapters, 2,285 subjects
  - Laws, decrees, ministerial decisions, regulations

License: Open (academic dataset)

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records for validation
  python bootstrap.py update             # Incremental update (no-op for static dataset)
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import re
import hashlib
import logging
import tempfile
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.GR.HellenicParliament")

# HuggingFace parquet URLs for the volume configuration
HF_BASE = "https://huggingface.co/datasets/AI-team-UoA/greek_legal_code/resolve/main/volume"
SPLITS = ["train", "test", "validation"]

# Volume label to name mapping (47 thematic volumes)
VOLUME_NAMES = {
    0: "ΚΟΙΝΩΝΙΚΗ ΠΡΟΝΟΙΑ",
    1: "ΓΕΩΡΓΙΚΗ ΝΟΜΟΘΕΣΙΑ",
    2: "ΡΑΔΙΟΦΩΝΙΑ ΚΑΙ ΤΥΠΟΣ",
    3: "ΒΙΟΜΗΧΑΝΙΚΗ ΝΟΜΟΘΕΣΙΑ",
    4: "ΥΓΕΙΟΝΟΜΙΚΗ ΝΟΜΟΘΕΣΙΑ",
    5: "ΠΟΛΕΜΙΚΟ ΝΑΥΤΙΚΟ",
    6: "ΤΑΧΥΔΡΟΜΕΙΑ - ΤΗΛΕΠΙΚΟΙΝΩΝΙΕΣ",
    7: "ΔΑΣΗ ΚΑΙ ΚΤΗΝΟΤΡΟΦΙΑ",
    8: "ΕΚΚΛΗΣΙΑΣΤΙΚΗ ΝΟΜΟΘΕΣΙΑ",
    9: "ΑΤΟΜΙΚΑ ΚΑΙ ΠΟΛΙΤΙΚΑ ΔΙΚΑΙΩΜΑΤΑ",
    10: "ΚΟΙΝΩΝΙΚΗ ΑΣΦΑΛΙΣΗ",
    11: "ΑΛΙΕΙΑ",
    12: "ΕΜΠΟΡΙΚΟ ΔΙΚΑΙΟ ΚΑΙ ΝΟΜΟΘΕΣΙΑ",
    13: "ΔΗΜΟΣΙΑ ΕΡΓΑ",
    14: "ΤΟΥΡΙΣΜΟΣ",
    15: "ΝΟΜΟΘΕΣΙΑ ΓΙΑ ΤΟΥΣ ΕΛΛΗΝΕΣ ΤΟΥ ΕΞΩΤΕΡΙΚΟΥ",
    16: "ΠΟΙΝΙΚΗ ΝΟΜΟΘΕΣΙΑ",
    17: "ΔΗΜΟΣΙΟΫΠΑΛΛΗΛΙΚΟΣ ΚΩΔΙΚΑΣ",
    18: "ΤΟΠΙΚΗ ΑΥΤΟΔΙΟΙΚΗΣΗ",
    19: "ΕΚΠΑΙΔΕΥΤΙΚΗ ΝΟΜΟΘΕΣΙΑ",
    20: "ΟΙΚΟΝΟΜΙΚΗ ΝΟΜΟΘΕΣΙΑ",
    21: "ΠΟΛΙΤΙΚΗ ΔΙΚΑΙΟΣΥΝΗ",
    22: "ΠΟΛΙΤΙΚΗ ΑΕΡΟΠΟΡΙΑ",
    23: "ΜΕΤΑΦΟΡΕΣ ΚΑΙ ΕΠΙΚΟΙΝΩΝΙΕΣ",
    24: "ΤΕΧΝΙΚΑ ΕΡΓΑ",
    25: "ΕΡΓΑΤΙΚΗ ΝΟΜΟΘΕΣΙΑ",
    26: "ΛΙΜΕΝΙΚΗ ΝΟΜΟΘΕΣΙΑ",
    27: "ΕΜΠΟΡΙΚΗ ΝΑΥΤΙΛΙΑ",
    28: "ΕΘΝΙΚΗ ΑΜΥΝΑ",
    29: "ΕΞΩΤΕΡΙΚΕΣ ΥΠΟΘΕΣΕΙΣ",
    30: "ΣΤΡΑΤΙΩΤΙΚΗ ΝΟΜΟΘΕΣΙΑ",
    31: "ΜΕΤΑΛΛΕΙΑ ΚΑΙ ΟΡΥΧΕΙΑ",
    32: "ΠΟΛΕΟΔΟΜΙΚΗ ΝΟΜΟΘΕΣΙΑ",
    33: "ΚΤΗΜΑΤΟΛΟΓΙΟ",
    34: "ΝΑΡΚΩΤΙΚΑ",
    35: "ΔΗΜΟΣΙΑ ΤΑΞΗ",
    36: "ΠΕΡΙΒΑΛΛΟΝΤΙΚΗ ΝΟΜΟΘΕΣΙΑ",
    37: "ΕΝΕΡΓΕΙΑ",
    38: "ΠΝΕΥΜΑΤΙΚΗ ΙΔΙΟΚΤΗΣΙΑ",
    39: "ΑΛΛΟΔΑΠΟΙ",
    40: "ΑΘΛΗΤΙΣΜΟΣ",
    41: "ΠΟΛΙΤΙΣΤΙΚΗ ΚΛΗΡΟΝΟΜΙΑ",
    42: "ΑΓΡΟΤΙΚΗ ΑΝΑΠΤΥΞΗ",
    43: "ΔΗΜΟΣΙΑ ΥΓΕΙΑ",
    44: "ΧΩΡΟΤΑΞΙΑ",
    45: "ΔΙΟΙΚΗΤΙΚΗ ΔΙΚΑΙΟΣΥΝΗ",
    46: "ΣΥΝΤΑΓΜΑΤΙΚΗ ΝΟΜΟΘΕΣΙΑ",
}

# Patterns to extract metadata from Greek legal text
# Matches patterns like: ΝΟΜΟΣ υπ' αριθ. 1234, Ν. 1234/1984, ΝΟΜΟΣ 4567/2017
LAW_NUMBER_RE = re.compile(
    r'(?:ΝΟΜΟ[ΣΥ]|Ν\.|Ν\.Δ\.|ΑΝΑΓΚ\.\s*ΝΟΜΟΣ|Π\.Δ\.|ΠΡΟΕΔΡΙΚΟ ΔΙΑΤΑΓΜΑ|'
    r'ΑΠΟΦΑΣΗ|Β\.Δ\.|ΒΑΣΙΛΙΚΟ ΔΙΑΤΑΓΜΑ|Κ\.Υ\.Α\.)[\s]*'
    r'(?:υπ[\'᾿]\s*αριθ(?:μ)?\.?\s*)?'
    r'(\d[\d/]*)',
    re.IGNORECASE
)

# Date patterns: "της 30 Αυγ./2 Σεπτ. 1939", "1984", "2017"
YEAR_RE = re.compile(r'\b(1[89]\d{2}|20[012]\d)\b')

# Law type detection
LAW_TYPE_PATTERNS = [
    (re.compile(r'ΑΝΑΓΚ(?:ΑΣΤΙΚΟΣ)?\.?\s*ΝΟΜΟΣ', re.IGNORECASE), "emergency_law"),
    (re.compile(r'ΠΡΟΕΔΡΙΚΟ\s*ΔΙΑΤΑΓΜΑ|Π\.Δ\.', re.IGNORECASE), "presidential_decree"),
    (re.compile(r'ΒΑΣΙΛΙΚΟ\s*ΔΙΑΤΑΓΜΑ|Β\.Δ\.', re.IGNORECASE), "royal_decree"),
    (re.compile(r'Ν(?:ΟΜΟΘΕΤΙΚΟ)?\.?\s*Δ(?:ΙΑΤΑΓΜΑ)?\.?', re.IGNORECASE), "legislative_decree"),
    (re.compile(r'ΑΠΟΦΑΣΗ\s+ΥΠΟΥΡΓ', re.IGNORECASE), "ministerial_decision"),
    (re.compile(r'ΑΠΟΦΑΣΗ\s+ΠΡΩΘΥΠΟΥΡΓ', re.IGNORECASE), "prime_minister_decision"),
    (re.compile(r'ΑΠΟΦΑΣΗ', re.IGNORECASE), "decision"),
    (re.compile(r'ΝΟΜΟΣ|^Ν\.', re.IGNORECASE), "law"),
    (re.compile(r'Κ\.Υ\.Α\.', re.IGNORECASE), "joint_ministerial_decision"),
]


def _extract_title(text: str) -> str:
    """Extract title from the first line or sentence of the text."""
    # Take first line or first 200 chars, whichever is shorter
    first_line = text.split('\n')[0].strip()
    if len(first_line) > 200:
        # Try to cut at a sentence boundary
        cut = first_line[:200].rfind('.')
        if cut > 50:
            first_line = first_line[:cut + 1]
        else:
            first_line = first_line[:200] + "..."
    return first_line


def _extract_law_number(text: str) -> Optional[str]:
    """Try to extract law/decree number from text."""
    match = LAW_NUMBER_RE.search(text[:500])
    if match:
        return match.group(1).strip()
    return None


def _extract_year(text: str) -> Optional[str]:
    """Extract the earliest year mentioned in the first 500 chars."""
    years = YEAR_RE.findall(text[:500])
    if years:
        return years[0]
    return None


def _detect_law_type(text: str) -> str:
    """Detect the type of legal instrument from the text."""
    snippet = text[:300]
    for pattern, law_type in LAW_TYPE_PATTERNS:
        if pattern.search(snippet):
            return law_type
    return "legislation"


class HellenicParliamentScraper(BaseScraper):
    """
    Scraper for GR/HellenicParliament -- Greek Legislation (Raptarchis Code).
    Country: GR
    URL: https://huggingface.co/datasets/AI-team-UoA/greek_legal_code

    Data types: legislation
    Auth: none (Open Data)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

    def _download_parquet(self, split: str) -> Path:
        """Download a parquet file from HuggingFace to a temp location."""
        import requests

        url = f"{HF_BASE}/{split}-00000-of-00001.parquet"
        cache_dir = self.source_dir / ".cache"
        cache_dir.mkdir(exist_ok=True)
        local_path = cache_dir / f"{split}.parquet"

        if local_path.exists():
            logger.info(f"Using cached {split} split at {local_path}")
            return local_path

        logger.info(f"Downloading {split} split from {url}...")
        resp = requests.get(url, timeout=300, stream=True)
        resp.raise_for_status()

        with open(local_path, 'wb') as f:
            for chunk in resp.iter_content(chunk_size=8192):
                f.write(chunk)

        size_mb = local_path.stat().st_size / (1024 * 1024)
        logger.info(f"Downloaded {split} split: {size_mb:.1f}MB")
        return local_path

    def fetch_all(self) -> Generator[dict, None, None]:
        """
        Yield all documents from the Raptarchis dataset.
        Downloads parquet files from HuggingFace and iterates through records.
        """
        import pandas as pd

        for split in SPLITS:
            try:
                parquet_path = self._download_parquet(split)
                df = pd.read_parquet(parquet_path)
                logger.info(f"Processing {split} split: {len(df)} records")

                for idx, row in df.iterrows():
                    yield {
                        "text": row["text"],
                        "label": int(row["label"]),
                        "split": split,
                        "index": int(idx),
                    }

                # Free memory
                del df

            except Exception as e:
                logger.error(f"Error processing {split} split: {e}")
                continue

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """
        No incremental updates for static dataset.
        Returns empty generator.
        """
        logger.info("Raptarchis is a static dataset (1834-2015). No incremental updates available.")
        return
        yield  # Make this a generator

    def normalize(self, raw: dict) -> dict:
        """
        Transform a raw Raptarchis record into a standardized schema.

        Returns a dict with full text and extracted metadata.
        """
        text = raw.get("text", "").strip()
        if not text:
            return None

        label = raw.get("label", -1)
        split = raw.get("split", "unknown")
        index = raw.get("index", 0)

        # Generate stable unique ID
        text_hash = hashlib.sha256(text.encode("utf-8")).hexdigest()[:16]
        doc_id = f"GR-RAPT-{text_hash}"

        # Extract metadata from text
        title = _extract_title(text)
        law_number = _extract_law_number(text)
        year = _extract_year(text)
        law_type = _detect_law_type(text)
        volume_name = VOLUME_NAMES.get(label, f"Volume {label}")

        # Build date from extracted year
        date_str = f"{year}-01-01" if year else None

        return {
            "_id": doc_id,
            "_source": "GR/HellenicParliament",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date_str,
            "url": "https://huggingface.co/datasets/AI-team-UoA/greek_legal_code",
            "volume_id": label,
            "volume_name": volume_name,
            "law_number": law_number,
            "law_type": law_type,
        }


if __name__ == "__main__":
    scraper = HellenicParliamentScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv

    if command == "bootstrap":
        stats = scraper.bootstrap(sample_mode=sample_mode, sample_size=15)
        print(f"\nBootstrap complete: {stats}")

    elif command == "update":
        stats = scraper.update()
        print(f"\nUpdate complete: {stats}")

    elif command == "test":
        print("Testing HuggingFace connectivity...")
        import requests
        url = f"{HF_BASE}/test-00000-of-00001.parquet"
        try:
            resp = requests.head(url, timeout=30, allow_redirects=True)
            print(f"  Status: {resp.status_code}")
            print(f"  Content-Length: {resp.headers.get('content-length', 'unknown')}")
            if resp.status_code in (200, 302):
                print("  ✓ Connection successful")
            else:
                print(f"  ✗ Unexpected status: {resp.status_code}")
        except Exception as e:
            print(f"  ✗ Connection failed: {e}")

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
