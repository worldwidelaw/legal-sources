#!/usr/bin/env python3
"""
DE/BfJ -- German Federal Office of Justice (Bundesamt für Justiz)

Fetches 200+ official justice statistics and publications as PDF reports:
  - Justizstatistiken (extradition, legal aid, hate crime, courts, etc.)
  - Infomaterial (brochures, business figures, annual magazines)
  - Criminal justice overviews, personnel statistics, telecom monitoring

Source: https://www.bundesjustizamt.de
Discovery: Two listing pages (Justizstatistiken + Infomaterial)
Content: PDF documents extracted via common/pdf_extract

Usage:
    python bootstrap.py bootstrap --sample
    python bootstrap.py bootstrap --full
    python bootstrap.py updates --since YYYY-MM-DD
"""

import argparse
import hashlib
import json
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Generator, List, Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
SCRIPT_DIR = Path(__file__).parent
SAMPLE_DIR = SCRIPT_DIR / "sample"
ROOT_DIR = SCRIPT_DIR.parent.parent.parent

sys.path.insert(0, str(ROOT_DIR))

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
SOURCE_ID = "DE/BfJ"
BASE_URL = "https://www.bundesjustizamt.de"
LISTING_PAGES = [
    f"{BASE_URL}/DE/Service/Justizstatistiken/Justizstatistiken_node.html",
    f"{BASE_URL}/DE/Service/Infomaterial/Infomaterial_node.html",
]
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 LegalDataHunter/1.0"
)
REQUEST_DELAY = 1.5

# Categories for classification based on URL path
CATEGORIES = {
    "Auslieferung": "extradition_statistics",
    "Beratungshilfe": "legal_aid_statistics",
    "Betreuung": "guardianship_statistics",
    "Gerichte": "courts_overview",
    "Geschaeftsentwicklung": "case_workload",
    "Hasskriminalitaet": "hate_crime_statistics",
    "Juristenausbildung": "legal_education",
    "Richterstatistik": "judge_statistics",
    "Personalbestand": "personnel_statistics",
    "Straftaten": "far_right_crime_statistics",
    "Rechtspflegerpruefung": "court_officer_exams",
    "Rehabilitierung": "rehabilitation_statistics",
    "Antragseingaenge": "rehabilitation_statistics",
    "Schiedspersonen": "arbitrator_statistics",
    "Schoeffenstatistik": "lay_judge_statistics",
    "Geschlechterparitaet": "gender_parity_statistics",
    "Strafrechtspflege": "criminal_justice_overview",
    "Criminal_Justice": "criminal_justice_overview",
    "TKUE": "telecom_monitoring",
    "Verkehrsdaten": "telecom_monitoring",
    "Online_Durchsuchung": "telecom_monitoring",
    "Telemedi": "telecom_monitoring",
    "BfJ_Infobroschuere": "bfj_brochure",
    "BfJ-Magazin": "bfj_magazine",
    "BfJ_Geschaeftszahlen": "bfj_business_figures",
    "Geschaeftsbericht": "bfj_annual_report",
}


class BfJClient:
    """Client for fetching BfJ publications."""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": USER_AGENT,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "de-DE,de;q=0.9,en;q=0.8",
        })

    def list_documents(self) -> List[Dict]:
        """Discover all PDF documents from BfJ listing pages."""
        all_docs = []
        seen_urls = set()

        for page_url in LISTING_PAGES:
            print(f"  Fetching: {page_url}")
            try:
                resp = self.session.get(page_url, timeout=30)
                resp.raise_for_status()
            except requests.RequestException as exc:
                print(f"  Error fetching {page_url}: {exc}")
                continue

            soup = BeautifulSoup(resp.text, "html.parser")
            docs = self._extract_pdf_links(soup, page_url)

            for doc in docs:
                if doc["pdf_url"] not in seen_urls:
                    seen_urls.add(doc["pdf_url"])
                    all_docs.append(doc)

            print(f"    Found {len(docs)} PDF links ({len(all_docs)} total unique)")
            time.sleep(REQUEST_DELAY)

        return all_docs

    def _extract_pdf_links(self, soup: BeautifulSoup, page_url: str) -> List[Dict]:
        """Extract all PDF download links from a listing page."""
        docs = []

        for link in soup.find_all("a", href=True):
            href = link["href"]

            # Only interested in PDF files
            if ".pdf" not in href.lower():
                continue

            # Build absolute URL
            if href.startswith("/"):
                pdf_url = f"{BASE_URL}{href}"
            elif href.startswith("http"):
                pdf_url = href
            else:
                pdf_url = urljoin(page_url, href)

            # Skip non-BfJ PDFs
            if "bundesjustizamt.de" not in pdf_url:
                continue

            # Skip survey forms and instruction sheets (not doctrine)
            lower_href = href.lower()
            if any(skip in lower_href for skip in [
                "erhebungsbogen", "ausfuellanleitung", "flyer_ausbildung",
                "flyer_praktikum", "flyer_einblicke",
            ]):
                continue

            # Extract title from link text or parent context
            title = self._extract_title(link)
            if not title or len(title) < 5:
                continue

            # Clean URL (remove __blob parameter for ID stability)
            clean_url = pdf_url.split("?")[0]
            doc_id = hashlib.sha256(clean_url.encode()).hexdigest()[:16]

            # Extract year from filename if present
            year_match = re.search(r"(\d{4})", clean_url.split("/")[-1])
            date_str = f"{year_match.group(1)}-01-01" if year_match else ""

            # Classify category
            category = self._classify(clean_url)

            docs.append({
                "id": doc_id,
                "title": title,
                "url": clean_url,
                "pdf_url": pdf_url,
                "date": date_str,
                "category": category,
            })

        return docs

    def _extract_title(self, link) -> str:
        """Extract a meaningful title from a PDF link element."""
        # Direct link text
        text = link.get_text(strip=True)
        if text and len(text) >= 10:
            # Clean up common PDF suffix patterns
            text = re.sub(r"PDF,?\s*[\d,.]+\s*[KMG]B.*$", "", text, flags=re.I)
            text = re.sub(r"\s*\(PDF,?\s*[\d,.]+\s*[KMG]B.*?\)", "", text, flags=re.I)
            text = re.sub(r"\s*,?\s*(nicht\s+)?barrierefrei.*$", "", text, flags=re.I)
            text = re.sub(r"\s*,?\s*Datei ist.*$", "", text, flags=re.I)
            return text.strip()

        # Check parent elements for title context
        parent = link.parent
        for _ in range(3):
            if parent is None:
                break
            # Check for heading siblings
            for tag in ("h2", "h3", "h4", "strong"):
                heading = parent.find(tag)
                if heading:
                    heading_text = heading.get_text(strip=True)
                    if heading_text and len(heading_text) >= 5:
                        # Combine heading with link text for specificity
                        if text and text != heading_text:
                            return f"{heading_text} - {text}"
                        return heading_text
            parent = parent.parent

        # Fallback: derive from filename
        filename = link["href"].split("/")[-1].split("?")[0]
        filename = filename.replace(".pdf", "").replace("_", " ")
        if len(filename) >= 5:
            return filename

        return text or ""

    def _classify(self, url: str) -> str:
        """Classify document category from URL."""
        filename = url.split("/")[-1]
        for key, category in CATEGORIES.items():
            if key.lower() in filename.lower():
                return category
        return "general"

    def download_pdf(self, pdf_url: str, retries: int = 3) -> Optional[bytes]:
        """Download PDF and return raw bytes."""
        for attempt in range(retries):
            try:
                resp = self.session.get(pdf_url, timeout=120)
                if resp.status_code == 429:
                    time.sleep(2 ** (attempt + 2))
                    continue
                if resp.status_code in (403, 404):
                    return None
                resp.raise_for_status()
                ct = resp.headers.get("Content-Type", "")
                if "pdf" in ct or b"%PDF" in resp.content[:20]:
                    return resp.content
                return None
            except requests.RequestException:
                if attempt < retries - 1:
                    time.sleep(2)
                    continue
                return None
        return None


# ---------------------------------------------------------------------------
# Text extraction
# ---------------------------------------------------------------------------

def _extract_text(pdf_bytes: bytes, doc_id: str) -> str:
    """Extract text from PDF using common/pdf_extract or fallbacks."""
    try:
        from common.pdf_extract import extract_pdf_markdown
        text = extract_pdf_markdown(
            source=SOURCE_ID,
            source_id=doc_id,
            pdf_bytes=pdf_bytes,
            table="doctrine",
            force=True,
        )
        if text:
            return text
    except (ImportError, TypeError):
        pass

    try:
        import pdfplumber
        import io
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            pages = [p.extract_text() or "" for p in pdf.pages]
            return "\n\n".join(p for p in pages if p)
    except ImportError:
        pass

    try:
        from pypdf import PdfReader
        import io
        reader = PdfReader(io.BytesIO(pdf_bytes))
        pages = [p.extract_text() or "" for p in reader.pages]
        return "\n\n".join(p for p in pages if p)
    except ImportError:
        pass

    return ""


# ---------------------------------------------------------------------------
# Normalize
# ---------------------------------------------------------------------------

def normalize(doc: Dict, text: str) -> Dict:
    """Transform into standard schema."""
    return {
        "_id": f"DE-BfJ-{doc['id']}",
        "_source": SOURCE_ID,
        "_type": "doctrine",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": doc["title"],
        "text": text,
        "date": doc.get("date", ""),
        "url": doc["url"],
        "category": doc.get("category", "general"),
    }


# ---------------------------------------------------------------------------
# Fetch logic
# ---------------------------------------------------------------------------

def fetch_sample(count: int = 15) -> List[Dict]:
    """Fetch a sample of BfJ documents."""
    client = BfJClient()
    records = []

    print("Discovering BfJ publications...")
    docs = client.list_documents()
    print(f"Found {len(docs)} PDF documents")

    for doc in docs:
        if len(records) >= count:
            break

        print(f"\n  [{len(records)+1}/{count}] {doc['title'][:65]}...")

        time.sleep(REQUEST_DELAY)
        pdf_bytes = client.download_pdf(doc["pdf_url"])

        if not pdf_bytes:
            print(f"       Skipping: PDF download failed")
            continue

        text = _extract_text(pdf_bytes, doc["id"])
        if len(text) < 200:
            print(f"       Skipping: text too short ({len(text)} chars)")
            continue

        record = normalize(doc, text)
        records.append(record)
        print(f"       OK: {len(text):,} chars ({doc['category']})")

    return records


def fetch_all(since: Optional[str] = None) -> Generator[Dict, None, None]:
    """Fetch all BfJ documents."""
    client = BfJClient()

    print("Discovering all BfJ publications...")
    docs = client.list_documents()
    print(f"Total documents found: {len(docs)}")

    yielded = 0
    skipped = 0

    for doc in docs:
        if since and doc.get("date") and doc["date"] < since:
            continue

        time.sleep(REQUEST_DELAY)
        pdf_bytes = client.download_pdf(doc["pdf_url"])

        if not pdf_bytes:
            skipped += 1
            continue

        text = _extract_text(pdf_bytes, doc["id"])
        if len(text) < 200:
            skipped += 1
            continue

        record = normalize(doc, text)
        yielded += 1

        if yielded % 20 == 0:
            print(f"  Progress: {yielded:,} fetched, {skipped} skipped")

        yield record

    print(f"\nTotal: {yielded:,} fetched, {skipped} skipped")


# ---------------------------------------------------------------------------
# Save / validate
# ---------------------------------------------------------------------------

def save_samples(records: List[Dict]) -> None:
    """Save sample records to sample/."""
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)
    for i, record in enumerate(records):
        path = SAMPLE_DIR / f"record_{i:04d}.json"
        with open(path, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)
    all_path = SAMPLE_DIR / "all_samples.json"
    with open(all_path, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)
    print(f"\nSaved {len(records)} samples to {SAMPLE_DIR}")


def validate_samples() -> bool:
    """Validate sample records."""
    samples = sorted(SAMPLE_DIR.glob("record_*.json"))
    if len(samples) < 10:
        print(f"FAIL: Only {len(samples)} samples, need >= 10")
        return False

    ok = True
    text_lengths = []
    for path in samples:
        with open(path, "r", encoding="utf-8") as f:
            rec = json.load(f)
        text = rec.get("text", "")
        text_lengths.append(len(text))
        if not text:
            print(f"FAIL: {path.name} missing text")
            ok = False
        for field in ("_id", "_source", "_type", "title"):
            if not rec.get(field):
                print(f"WARN: {path.name} missing {field}")
        if text and re.search(r"<[a-z]+[^>]*>", text, re.IGNORECASE):
            print(f"WARN: {path.name} may contain HTML tags")

    avg = sum(text_lengths) / len(text_lengths) if text_lengths else 0
    print(f"\nValidation:")
    print(f"  Samples: {len(samples)}")
    print(f"  Avg text: {avg:,.0f} chars")
    print(f"  Min text: {min(text_lengths):,} chars")
    print(f"  Max text: {max(text_lengths):,} chars")
    print(f"  Valid: {ok}")
    return ok


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="DE/BfJ fetcher")
    sub = parser.add_subparsers(dest="command")

    bp = sub.add_parser("bootstrap", help="Initial data fetch")
    bp.add_argument("--sample", action="store_true", help="Fetch sample only")
    bp.add_argument("--full", action="store_true", help="Full fetch")

    up = sub.add_parser("updates", help="Fetch updates")
    up.add_argument("--since", required=True, help="YYYY-MM-DD")

    sub.add_parser("validate", help="Validate samples")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    if args.command == "validate":
        valid = validate_samples()
        sys.exit(0 if valid else 1)

    if args.command == "bootstrap":
        if args.sample:
            print("Fetching sample BfJ documents...")
            records = fetch_sample()
            if records:
                save_samples(records)
                validate_samples()
                sys.exit(0 if len(records) >= 10 else 1)
            else:
                print("No records fetched!", file=sys.stderr)
                sys.exit(1)
        elif args.full:
            count = 0
            for rec in fetch_all():
                count += 1
            print(f"Fetched {count} BfJ documents")
        else:
            parser.print_help()
            sys.exit(1)

    elif args.command == "updates":
        count = 0
        for rec in fetch_all(since=args.since):
            count += 1
        print(f"Fetched {count} updates since {args.since}")


if __name__ == "__main__":
    main()
