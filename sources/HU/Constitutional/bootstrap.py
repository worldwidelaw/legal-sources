#!/usr/bin/env python3
"""
Hungarian Constitutional Court (Alkotmánybíróság) Data Fetcher

Extracts Constitutional Court decisions from alkotmanybirosag.hu.
- Uses browser automation for the /ugykereso/ search interface
- Fetches decision details from /ugyadatlap pages
- Downloads PDFs from media server as fallback
- Extracts full text using pdfplumber

Data source: https://alkotmanybirosag.hu
License: Public Domain
Coverage: 1990-present (~10,000+ decisions)
"""

import argparse
import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator, Optional, List, Dict
from html import unescape

import requests

# Add common directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

# Try to import BrowserScraper
try:
    from common.browser_scraper import BrowserScraper
    HAS_BROWSER = True
except ImportError:
    HAS_BROWSER = False
    print("Warning: BrowserScraper not available. Install playwright: pip install playwright && playwright install chromium")

# Try to import pdfplumber for PDF text extraction
try:
    import pdfplumber
    HAS_PDFPLUMBER = True
except ImportError:
    HAS_PDFPLUMBER = False
    print("Warning: pdfplumber not installed. Install with: pip install pdfplumber")

SOURCE_ID = "HU/Constitutional"
BASE_URL = "https://alkotmanybirosag.hu"
MEDIA_URL = "https://media.alkotmanybirosag.hu"
SEARCH_URL = f"{BASE_URL}/ugykereso/"
DETAIL_URL = f"{BASE_URL}/ugyadatlap"
RECENT_DECISIONS_URL = f"{BASE_URL}/a-legfrissebb-dontesek/"

HEADERS = {
    "User-Agent": "World Wide Law/1.0 (EU Legal Research)",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "hu-HU,hu;q=0.9,en;q=0.8",
}

# Checkpoint file for resume support
CHECKPOINT_FILE = Path(__file__).parent / ".checkpoint.json"


def clean_html(html_text: str) -> str:
    """Remove HTML tags and clean text."""
    if not html_text:
        return ""
    text = unescape(html_text)
    text = re.sub(r'<[^>]+>', ' ', text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text


def extract_pdf_text(pdf_content: bytes) -> str:
    """Extract text from PDF content using pdfplumber."""
    if not HAS_PDFPLUMBER:
        return ""

    import io
    text_parts = []

    try:
        with pdfplumber.open(io.BytesIO(pdf_content)) as pdf:
            for page in pdf.pages:
                page_text = page.extract_text()
                if page_text:
                    text_parts.append(page_text)
    except Exception as e:
        print(f"Error extracting PDF text: {e}")
        return ""

    return "\n\n".join(text_parts)


def fetch_pdf_text(pdf_url: str, session: requests.Session) -> str:
    """Download PDF and extract text."""
    if not pdf_url or not HAS_PDFPLUMBER:
        return ""

    # Normalize URL
    if "89.135.41.81" in pdf_url:
        pdf_url = pdf_url.replace("http://89.135.41.81/wp-content/uploads", MEDIA_URL)

    try:
        response = session.get(pdf_url, timeout=60)
        response.raise_for_status()
        return extract_pdf_text(response.content)
    except Exception as e:
        print(f"Error fetching PDF {pdf_url}: {e}")
        return ""


def load_checkpoint() -> dict:
    """Load checkpoint data for resume support."""
    if CHECKPOINT_FILE.exists():
        with open(CHECKPOINT_FILE, 'r') as f:
            return json.load(f)
    return {"processed_ids": [], "last_year": None}


def save_checkpoint(data: dict):
    """Save checkpoint data."""
    with open(CHECKPOINT_FILE, 'w') as f:
        json.dump(data, f)


def clear_checkpoint():
    """Clear checkpoint file."""
    if CHECKPOINT_FILE.exists():
        CHECKPOINT_FILE.unlink()


class ConstitutionalCourtScraper:
    """Browser-based scraper for Hungarian Constitutional Court decisions."""

    def __init__(self, headless: bool = True):
        self.headless = headless
        self.scraper = None
        self.page = None
        self.session = requests.Session()

    def __enter__(self):
        if HAS_BROWSER:
            self.scraper = BrowserScraper(headless=self.headless, timeout=60000)
            self.scraper.start()
            self.page = self.scraper.new_page()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.scraper:
            self.scraper.stop()

    def _wait_for_loading(self, max_seconds: int = 60):
        """Wait for the Loading... indicator to disappear."""
        for i in range(max_seconds):
            time.sleep(1)
            visible_text = self.page.evaluate('() => document.body.innerText')
            if 'Loading...' not in visible_text:
                return True
        return False

    def search_by_year(self, year: int) -> List[Dict]:
        """
        Search for all decisions from a specific year.
        Returns list of decision stubs with id and basic info.
        """
        print(f"Searching for decisions from year {year}...")

        # Navigate to search page
        self.page.goto(SEARCH_URL, wait_until='networkidle')
        time.sleep(3)

        # Find and click the year select (first select control)
        select_controls = self.page.query_selector_all('.select__control')
        if not select_controls:
            print("Error: Could not find select controls")
            return []

        select_controls[0].click()
        time.sleep(1)

        # Select the target year
        options = self.page.query_selector_all('.select__option')
        year_found = False
        for opt in options:
            if opt.inner_text().strip() == str(year):
                opt.click()
                year_found = True
                break

        if not year_found:
            print(f"Year {year} not found in options")
            return []

        time.sleep(1)

        # Click search button
        self.page.click('button.search-start')

        # Wait for results
        print(f"Waiting for search results...")
        if not self._wait_for_loading(60):
            print("Warning: Loading did not complete in time")

        # Extract results count and decision links
        visible_text = self.page.evaluate('() => document.body.innerText')

        # Parse total count
        count_match = re.search(r'A keresés eredménye \((\d+)\)', visible_text)
        total_count = int(count_match.group(1)) if count_match else 0
        print(f"Found {total_count} decisions for year {year}")

        # Extract all decision links
        links = self.page.evaluate('''() => {
            const links = document.querySelectorAll('a[href*="ugyadatlap?id="]');
            return Array.from(links).map(l => ({
                href: l.href,
                text: l.innerText.trim()
            }));
        }''')

        decisions = []
        for link in links:
            # Extract ID from URL
            id_match = re.search(r'id=([A-Z0-9]+)', link['href'])
            if id_match:
                decisions.append({
                    'id': id_match.group(1),
                    'url': link['href'],
                    'title': link['text']
                })

        return decisions

    def fetch_decision_detail(self, decision_id: str) -> Optional[Dict]:
        """
        Fetch full details for a decision by its ID.
        Returns normalized record with full text.
        """
        url = f"{DETAIL_URL}?id={decision_id}"

        try:
            self.page.goto(url, wait_until='networkidle')
            time.sleep(2)

            # Wait for content to load
            self._wait_for_loading(30)

            # Extract structured data from the page
            visible_text = self.page.evaluate('() => document.body.innerText')

            # Parse decision number
            decision_number = ""
            number_match = re.search(r'A HATÁROZAT SZÁMA\s*\n\s*([^\n]+)', visible_text)
            if number_match:
                decision_number = number_match.group(1).strip()

            # Parse case number
            case_number = ""
            case_match = re.search(r'ÜGYSZÁM\s*\n\s*([^\n]+)', visible_text)
            if case_match:
                case_number = case_match.group(1).strip()

            # Parse date
            date_str = None
            date_match = re.search(r'A HATÁROZAT KELTE\s*\n\s*([^\n]+)', visible_text)
            if date_match:
                raw_date = date_match.group(1).strip()
                # Parse "Budapest, 2024.12.03" format
                parsed = re.search(r'(\d{4})\.(\d{2})\.(\d{2})', raw_date)
                if parsed:
                    date_str = f"{parsed.group(1)}-{parsed.group(2)}-{parsed.group(3)}"

            # Parse procedure type
            procedure_type = ""
            proc_match = re.search(r'ELJÁRÁS TÍPUSA\s*\n\s*([^\n]+)', visible_text)
            if proc_match:
                procedure_type = proc_match.group(1).strip()

            # Parse rapporteur
            rapporteur = ""
            rap_match = re.search(r'ELŐADÓ ALKOTMÁNYBÍRÓ\s*\n\s*([^\n]+)', visible_text)
            if rap_match:
                rapporteur = rap_match.group(1).strip()

            # Parse subject
            subject = ""
            subj_match = re.search(r'AZ ÜGY TÁRGYA\s*\n\s*([^\n]+)', visible_text)
            if subj_match:
                subject = subj_match.group(1).strip()

            # Extract full text (between "A HATÁROZAT SZÖVEGE" and the end)
            full_text = ""
            text_match = re.search(r'A HATÁROZAT SZÖVEGE\s*\n(.+?)(?=\n\n\n|\Z)', visible_text, re.DOTALL)
            if text_match:
                full_text = text_match.group(1).strip()

            # If no text found in HTML, try PDF
            if len(full_text) < 100:
                pdf_links = self.page.evaluate('''() => {
                    const links = document.querySelectorAll('a[href*=".pdf"]');
                    return Array.from(links).map(l => l.href);
                }''')

                for pdf_url in pdf_links:
                    if 'AB_' in pdf_url or 'vegzes' in pdf_url or 'hatarozat' in pdf_url:
                        print(f"  Fetching PDF: {pdf_url}")
                        pdf_text = fetch_pdf_text(pdf_url, self.session)
                        if len(pdf_text) > len(full_text):
                            full_text = pdf_text
                        break

            # Build URL
            public_url = url

            # Determine decision type from decision_number
            decision_type = "határozat"
            if "végzés" in decision_number.lower():
                decision_type = "végzés"

            # Generate ID
            doc_id = f"HU_CONST_{decision_id}"

            return {
                "_id": doc_id,
                "_source": SOURCE_ID,
                "_type": "case_law",
                "_fetched_at": datetime.now(timezone.utc).isoformat(),
                "title": decision_number,
                "text": full_text,
                "date": date_str,
                "url": public_url,
                "decision_number": decision_number,
                "case_number": case_number,
                "decision_type": decision_type,
                "procedure_type": procedure_type,
                "rapporteur": rapporteur,
                "subject": subject,
                "lotus_notes_id": decision_id,
            }

        except Exception as e:
            print(f"Error fetching decision {decision_id}: {e}")
            return None


def fetch_all_browser(years: List[int] = None, resume: bool = True) -> Iterator[dict]:
    """
    Fetch all decisions using browser automation.

    Args:
        years: List of years to fetch. If None, fetches all years from 1990-current.
        resume: If True, resume from checkpoint.
    """
    if not HAS_BROWSER:
        raise RuntimeError("BrowserScraper not available. Install playwright.")

    current_year = datetime.now().year
    if years is None:
        years = list(range(current_year, 1989, -1))  # Most recent first

    # Load checkpoint for resume
    checkpoint = load_checkpoint() if resume else {"processed_ids": [], "last_year": None}
    processed_ids = set(checkpoint.get("processed_ids", []))

    with ConstitutionalCourtScraper(headless=True) as scraper:
        for year in years:
            print(f"\n{'='*60}")
            print(f"Processing year {year}")
            print(f"{'='*60}")

            decisions = scraper.search_by_year(year)

            for i, decision in enumerate(decisions):
                decision_id = decision['id']

                # Skip if already processed
                if decision_id in processed_ids:
                    print(f"Skipping {decision['title']} (already processed)")
                    continue

                print(f"\nFetching {i+1}/{len(decisions)}: {decision['title']}...")

                record = scraper.fetch_decision_detail(decision_id)
                if record:
                    text_len = len(record.get("text", ""))
                    print(f"  -> {record['decision_number']} ({text_len} chars)")

                    # Update checkpoint
                    processed_ids.add(decision_id)
                    checkpoint["processed_ids"] = list(processed_ids)
                    checkpoint["last_year"] = year
                    save_checkpoint(checkpoint)

                    yield record

                # Rate limiting
                time.sleep(2)

            print(f"Completed year {year}")


# Legacy functions for backward compatibility with recent decisions
def extract_nextjs_data(html: str) -> dict:
    """Extract __NEXT_DATA__ from Next.js rendered page."""
    match = re.search(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', html, re.DOTALL)
    if match:
        return json.loads(match.group(1))
    return {}


def fetch_recent_decisions(session: requests.Session) -> list:
    """Fetch decisions from the recent decisions page (legacy method)."""
    try:
        response = session.get(RECENT_DECISIONS_URL, headers=HEADERS, timeout=30)
        response.raise_for_status()

        data = extract_nextjs_data(response.text)
        if not data:
            return []

        decisions = data.get("props", {}).get("pageProps", {}).get("decisions", [])
        return decisions
    except Exception as e:
        print(f"Error fetching recent decisions: {e}")
        return []


def normalize_legacy(raw: dict, full_text: str = "") -> dict:
    """Transform raw decision data into standard schema (legacy format)."""
    acf = raw.get("acf", {})

    decision_number = acf.get("decision_number", "")
    doc_id = f"HU_CONST_{decision_number.replace('/', '_')}" if decision_number else f"HU_CONST_{raw.get('id', '')}"

    date_str = raw.get("date", "")
    if date_str:
        try:
            dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
            date_iso = dt.strftime("%Y-%m-%d")
        except:
            date_iso = date_str[:10] if len(date_str) >= 10 else None
    else:
        date_iso = None

    summary = clean_html(acf.get("lead", ""))

    pdf_url = acf.get("attachments_pdf", "")
    if "89.135.41.81" in pdf_url:
        pdf_url = pdf_url.replace("http://89.135.41.81/wp-content/uploads", MEDIA_URL)

    slug = raw.get("slug", "")
    public_url = f"{BASE_URL}/a-legfrissebb-dontesek/{slug}/" if slug else ""

    categories = raw.get("_embedded", {}).get("wp:term", [[]])[0]
    decision_type = "decision"
    for cat in categories:
        cat_slug = cat.get("slug", "")
        if cat_slug == "decisions":
            decision_type = "határozat"
        elif cat_slug == "complaints":
            decision_type = "végzés"

    text_content = full_text if full_text else summary

    return {
        "_id": doc_id,
        "_source": SOURCE_ID,
        "_type": "case_law",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": clean_html(raw.get("title", {}).get("rendered", "")),
        "text": text_content,
        "date": date_iso,
        "url": public_url,
        "decision_number": decision_number,
        "decision_type": decision_type,
        "summary": summary,
        "pdf_url": pdf_url,
        "official_db_link": acf.get("link_attachments_view", ""),
        "subject": clean_html(acf.get("postmeta_info", "")),
    }


def fetch_all() -> Iterator[dict]:
    """Fetch all available decisions with full text."""
    # Use browser scraper for full archive if available
    if HAS_BROWSER:
        yield from fetch_all_browser()
    else:
        # Fall back to legacy method (recent decisions only)
        session = requests.Session()

        print("Fetching recent decisions (legacy mode - limited to ~50)...")
        decisions = fetch_recent_decisions(session)
        print(f"Found {len(decisions)} decisions")

        for i, decision in enumerate(decisions):
            acf = decision.get("acf", {})
            pdf_url = acf.get("attachments_pdf", "")

            print(f"Processing {i+1}/{len(decisions)}: {acf.get('decision_number', 'unknown')}...")

            full_text = ""
            if pdf_url and HAS_PDFPLUMBER:
                full_text = fetch_pdf_text(pdf_url, session)
                time.sleep(1.5)

            yield normalize_legacy(decision, full_text)


def fetch_updates(since: datetime) -> Iterator[dict]:
    """Fetch decisions modified since a given date."""
    session = requests.Session()

    decisions = fetch_recent_decisions(session)

    for decision in decisions:
        date_str = decision.get("modified", decision.get("date", ""))
        if date_str:
            try:
                dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
                if dt.replace(tzinfo=timezone.utc) >= since.replace(tzinfo=timezone.utc):
                    acf = decision.get("acf", {})
                    pdf_url = acf.get("attachments_pdf", "")
                    full_text = fetch_pdf_text(pdf_url, session) if pdf_url and HAS_PDFPLUMBER else ""
                    yield normalize_legacy(decision, full_text)
                    time.sleep(1.5)
            except:
                continue


def bootstrap_sample(sample_dir: Path, count: int = 12, use_browser: bool = True):
    """Fetch sample records for validation."""
    sample_dir.mkdir(parents=True, exist_ok=True)

    total_text_chars = 0
    records_with_text = 0
    saved_count = 0

    if use_browser and HAS_BROWSER:
        print("Using browser scraper for sample...")
        with ConstitutionalCourtScraper(headless=True) as scraper:
            # Get decisions from current year
            current_year = datetime.now().year
            decisions = scraper.search_by_year(current_year)

            if not decisions:
                print(f"No decisions found for {current_year}, trying previous year...")
                decisions = scraper.search_by_year(current_year - 1)

            for i, decision in enumerate(decisions[:count]):
                print(f"Processing {i+1}/{count}: {decision['title']}...")

                record = scraper.fetch_decision_detail(decision['id'])
                if not record:
                    continue

                text_len = len(record.get("text", ""))
                if text_len > 100:
                    records_with_text += 1
                    total_text_chars += text_len

                # Save to sample directory
                safe_name = re.sub(r'[^a-zA-Z0-9_-]', '_', record.get('decision_number', f'record_{i}'))
                filename = f"{safe_name}.json"
                filepath = sample_dir / filename

                with open(filepath, "w", encoding="utf-8") as f:
                    json.dump(record, f, ensure_ascii=False, indent=2)

                saved_count += 1
                print(f"  Saved: {filename} ({text_len} chars)")

                time.sleep(2)
    else:
        # Legacy mode
        session = requests.Session()
        print("Fetching decisions for sample (legacy mode)...")
        decisions = fetch_recent_decisions(session)

        if not decisions:
            print("ERROR: No decisions found!")
            return

        for i, decision in enumerate(decisions[:count]):
            acf = decision.get("acf", {})
            decision_number = acf.get("decision_number", f"unknown_{i}")

            print(f"Processing {i+1}/{count}: {decision_number}...")

            full_text = ""
            pdf_url = acf.get("attachments_pdf", "")
            if pdf_url and HAS_PDFPLUMBER:
                full_text = fetch_pdf_text(pdf_url, session)
                time.sleep(1.5)

            record = normalize_legacy(decision, full_text)

            text_len = len(record.get("text", ""))
            if text_len > 100:
                records_with_text += 1
                total_text_chars += text_len

            safe_name = re.sub(r'[^a-zA-Z0-9_-]', '_', decision_number)
            filename = f"{safe_name}.json"
            filepath = sample_dir / filename

            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)

            saved_count += 1
            print(f"  Saved: {filename} ({text_len} chars)")

    # Print summary
    print("\n" + "="*60)
    print("SAMPLE SUMMARY")
    print("="*60)
    print(f"Total records saved: {saved_count}")
    print(f"Records with full text (>100 chars): {records_with_text}")
    if records_with_text > 0:
        avg_chars = total_text_chars // records_with_text
        print(f"Average text length: {avg_chars:,} chars")
    print(f"Sample directory: {sample_dir}")


def main():
    parser = argparse.ArgumentParser(description="Hungarian Constitutional Court Data Fetcher")
    parser.add_argument("command", choices=["bootstrap", "fetch", "updates", "sample"],
                       help="Command to run")
    parser.add_argument("--sample", action="store_true",
                       help="Fetch sample records for validation")
    parser.add_argument("--count", type=int, default=12,
                       help="Number of sample records to fetch")
    parser.add_argument("--since", type=str,
                       help="Fetch updates since date (ISO format)")
    parser.add_argument("--year", type=int,
                       help="Fetch decisions from a specific year")
    parser.add_argument("--no-browser", action="store_true",
                       help="Use legacy mode (no browser automation)")
    parser.add_argument("--no-resume", action="store_true",
                       help="Start fresh, don't resume from checkpoint")
    parser.add_argument("--clear-checkpoint", action="store_true",
                       help="Clear checkpoint file and exit")

    args = parser.parse_args()

    script_dir = Path(__file__).parent
    sample_dir = script_dir / "sample"

    if args.clear_checkpoint:
        clear_checkpoint()
        print("Checkpoint cleared.")
        return

    if args.command == "bootstrap":
        if args.sample:
            bootstrap_sample(sample_dir, args.count, use_browser=not args.no_browser)
        else:
            # Full bootstrap
            if args.year:
                years = [args.year]
            else:
                years = None  # All years

            for record in fetch_all_browser(years=years, resume=not args.no_resume):
                print(json.dumps(record, ensure_ascii=False))

    elif args.command == "sample":
        bootstrap_sample(sample_dir, args.count, use_browser=not args.no_browser)

    elif args.command == "fetch":
        for record in fetch_all():
            print(json.dumps(record, ensure_ascii=False))

    elif args.command == "updates":
        if not args.since:
            print("ERROR: --since required for updates command")
            sys.exit(1)
        since = datetime.fromisoformat(args.since)
        for record in fetch_updates(since):
            print(json.dumps(record, ensure_ascii=False))


if __name__ == "__main__":
    main()
