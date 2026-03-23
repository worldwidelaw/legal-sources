#!/usr/bin/env python3
"""
PL/NSA-Tax -- Polish Supreme Administrative Court - Tax (Financial Chamber)

Fetches tax-related case law from the Financial Chamber (Izba Finansowa) of
the Supreme Administrative Court (NSA) via CBOSA.

Strategy:
  - Searches for Financial Chamber case numbers: III FSK, III FZ, III FPS
  - Session-based pagination: POST /cbo/search then GET /cbo/find?p=N
  - Full text: Sentencja + Uzasadnienie from HTML pages

Source: https://orzeczenia.nsa.gov.pl
Coverage: ~14,500 tax decisions (III FSK: 11,507 + III FZ: 2,933 + III FPS: 15)

Usage:
  python bootstrap.py bootstrap --sample     # Fetch sample records
  python bootstrap.py bootstrap              # Full fetch
  python bootstrap.py bootstrap-fast         # Full fetch via base class fast mode
  python bootstrap.py update                 # Recent decisions
  python bootstrap.py test-api               # Connectivity test
"""

import sys
import json
import logging
import time
import re
import html
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Generator, Optional, List

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.PL.NSA-Tax")

BASE_URL = "https://orzeczenia.nsa.gov.pl"

# Financial Chamber case number prefixes
TAX_SIGNATURES = ["III FSK", "III FZ", "III FPS"]

CHECKPOINT_FILE = Path(__file__).parent / "checkpoint.json"


class NSATaxScraper(BaseScraper):
    """
    Scraper for PL/NSA-Tax -- NSA Financial Chamber (tax decisions).
    Country: PL
    URL: https://orzeczenia.nsa.gov.pl

    Data types: case_law
    Auth: none (public access)
    Coverage: ~14,500 tax-related judgments from Izba Finansowa
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.5",
                "Accept-Encoding": "gzip, deflate",
                "Content-Type": "application/x-www-form-urlencoded",
            },
            timeout=60,
            verify=False,
        )

    # -- Checkpoint helpers -------------------------------------------------

    def _load_checkpoint(self) -> dict:
        if CHECKPOINT_FILE.exists():
            try:
                with open(CHECKPOINT_FILE, "r") as f:
                    return json.load(f)
            except json.JSONDecodeError:
                logger.warning("Invalid checkpoint file, starting fresh")
        return {
            "sig_index": 0,
            "page": 1,
            "total_fetched": 0,
            "fetched_ids": [],
            "last_update": None,
        }

    def _save_checkpoint(self, checkpoint: dict):
        with open(CHECKPOINT_FILE, "w") as f:
            json.dump(checkpoint, f, indent=2)

    def _clear_checkpoint(self):
        if CHECKPOINT_FILE.exists():
            CHECKPOINT_FILE.unlink()
            logger.info("Checkpoint cleared")

    # -- HTML parsing helpers ------------------------------------------------

    def _extract_doc_ids(self, html_content: str) -> List[str]:
        pattern = r'href="/doc/([A-Z0-9]+)"'
        matches = re.findall(pattern, html_content)
        seen = set()
        unique_ids = []
        for doc_id in matches:
            if doc_id not in seen:
                seen.add(doc_id)
                unique_ids.append(doc_id)
        return unique_ids

    def _extract_total_results(self, html_content: str) -> int:
        pattern = r'Znaleziono\s+(\d+)\s+orzeczeń'
        match = re.search(pattern, html_content)
        return int(match.group(1)) if match else 0

    def _extract_total_pages(self, html_content: str) -> int:
        pattern = r'Str\.\s*\d+\s*z\s*(\d+)'
        match = re.search(pattern, html_content)
        return int(match.group(1)) if match else 0

    def _parse_document_html(self, html_content: str, doc_id: str) -> dict:
        data = {"doc_id": doc_id}

        title_match = re.search(r'<TITLE>([^<]+)</TITLE>', html_content, re.IGNORECASE)
        if title_match:
            data["title"] = html.unescape(title_match.group(1).strip())

        if "title" in data:
            case_match = re.match(r'^([IVX]+\s+[A-Z]+\s+\d+/\d+)', data["title"])
            if not case_match:
                case_match = re.match(r'^([IVX]+\s+[A-Z]+/[A-Za-z]+\s+\d+/\d+)', data["title"])
            if case_match:
                data["case_number"] = case_match.group(1)

        court_match = re.search(
            r'<td class="lista-label">Sąd</td>.*?<td class="info-list-value">\s*'
            r'(Naczelny Sąd Administracyjny|Wojewódzki Sąd Administracyjny[^<]*)',
            html_content, re.DOTALL
        )
        if court_match:
            data["court"] = html.unescape(court_match.group(1).strip())

        date_match = re.search(r'>Data orzeczenia</[^>]+>.*?<td[^>]*>(\d{4}-\d{2}-\d{2})', html_content, re.DOTALL | re.IGNORECASE)
        if date_match:
            data["judgment_date"] = date_match.group(1)

        judges_match = re.search(r'<td class="lista-label">Sędziowie</td>.*?<td class="info-list-value">\s*([^<]+)', html_content, re.DOTALL)
        if judges_match:
            judges_text = html.unescape(judges_match.group(1).strip())
            judges = [j.strip() for j in re.split(r'<br\s*/?>', judges_text) if j.strip()]
            if not judges:
                judges = [judges_text]
            data["judges"] = judges

        keywords_match = re.search(r'<td class="lista-label">Hasła tematyczne</td>.*?<td class="info-list-value">\s*([^<]+)', html_content, re.DOTALL)
        if keywords_match:
            keywords_text = html.unescape(keywords_match.group(1).strip())
            data["keywords"] = [k.strip() for k in keywords_text.split(',') if k.strip()]

        # Full text - Sentencja
        sentencja_match = re.search(
            r'<div class="lista-label">Sentencja</div>\s*<span class="info-list-value-uzasadnienie">\s*(.+?)</span>',
            html_content, re.DOTALL
        )
        sentencja = sentencja_match.group(1) if sentencja_match else ""

        # Full text - Uzasadnienie
        uzasadnienie_match = re.search(
            r'<div class="lista-label">Uzasadnienie</div>\s*<span class="info-list-value-uzasadnienie">\s*(.+?)</span>',
            html_content, re.DOTALL
        )
        uzasadnienie = uzasadnienie_match.group(1) if uzasadnienie_match else ""

        full_text = ""
        if sentencja:
            full_text += self._clean_html(sentencja)
        if uzasadnienie:
            if full_text:
                full_text += "\n\n---\n\n"
            full_text += self._clean_html(uzasadnienie)

        data["text"] = full_text

        legal_bases_match = re.search(r'<td class="lista-label">Powołane przepisy</td>.*?<td class="info-list-value">\s*([^<]+)', html_content, re.DOTALL)
        if legal_bases_match:
            data["legal_bases"] = html.unescape(legal_bases_match.group(1).strip())

        decision_type_match = re.search(r'<span class="war_header">([^<]+)</span>', html_content)
        if decision_type_match:
            data["decision_type"] = html.unescape(decision_type_match.group(1).strip())

        return data

    def _clean_html(self, text: str) -> str:
        if not text:
            return ""
        text = html.unescape(text)
        text = re.sub(r'<[Pp]>', '\n', text)
        text = re.sub(r'</[Pp]>', '', text)
        text = re.sub(r'<[Bb][Rr]\s*/?>', '\n', text)
        text = re.sub(r'<[^>]+>', ' ', text)
        text = re.sub(r'[ \t]+', ' ', text)
        text = re.sub(r'\n{3,}', '\n\n', text)
        text = re.sub(r'^\s+', '', text, flags=re.MULTILINE)
        return text.strip()

    # -- API methods ---------------------------------------------------------

    def _initiate_search(self, signature: str, date_from: Optional[str] = None, date_to: Optional[str] = None) -> tuple:
        """POST to /cbo/search to initiate a search session. Returns (doc_ids, total_results, total_pages)."""
        form_data = {
            "wszystkieSlowa": "",
            "sygnatura": signature,
            "sad": "Naczelny Sąd Administracyjny",
            "wystepowanie": "gdziekolwiek",
            "odmiana": "on",
            "odDaty": date_from or "",
            "doDaty": date_to or "",
            "rodzaj": "dowolny",
        }

        self.rate_limiter.wait()

        try:
            resp = self.client.post("/cbo/search", data=form_data)
            resp.raise_for_status()
            doc_ids = self._extract_doc_ids(resp.text)
            total = self._extract_total_results(resp.text)
            total_pages = self._extract_total_pages(resp.text)
            return doc_ids, total, total_pages
        except Exception as e:
            logger.error(f"Search error (sig={signature}): {e}")
            time.sleep(3)
            try:
                resp = self.client.post("/cbo/search", data=form_data)
                resp.raise_for_status()
                doc_ids = self._extract_doc_ids(resp.text)
                total = self._extract_total_results(resp.text)
                total_pages = self._extract_total_pages(resp.text)
                return doc_ids, total, total_pages
            except Exception as e2:
                logger.error(f"Retry failed: {e2}")
                return [], 0, 0

    def _fetch_search_page(self, page: int) -> List[str]:
        """GET /cbo/find?p=N for paginated results within current search session."""
        self.rate_limiter.wait()
        try:
            resp = self.client.get(f"/cbo/find?p={page}")
            resp.raise_for_status()
            return self._extract_doc_ids(resp.text)
        except Exception as e:
            logger.warning(f"Failed to fetch search page {page}: {e}")
            return []

    def _fetch_document(self, doc_id: str) -> Optional[dict]:
        self.rate_limiter.wait()
        try:
            resp = self.client.get(f"/doc/{doc_id}")
            resp.raise_for_status()
            return self._parse_document_html(resp.text, doc_id)
        except Exception as e:
            logger.warning(f"Failed to fetch document {doc_id}: {e}")
            return None

    def _paginate_all_signatures(
        self,
        max_pages_per_sig: Optional[int] = None,
        date_from: Optional[str] = None,
        date_to: Optional[str] = None,
        use_checkpoint: bool = False,
    ) -> Generator[dict, None, None]:
        if use_checkpoint:
            checkpoint = self._load_checkpoint()
            sig_index = checkpoint.get("sig_index", 0)
            page = checkpoint.get("page", 1)
            total_fetched = checkpoint.get("total_fetched", 0)
            fetched_ids = set(checkpoint.get("fetched_ids", []))
        else:
            sig_index = 0
            page = 1
            total_fetched = 0
            fetched_ids = set()

        for si in range(sig_index, len(TAX_SIGNATURES)):
            sig = TAX_SIGNATURES[si]
            if si > sig_index:
                page = 1  # reset page for new signature

            logger.info(f"Fetching {sig} cases (starting page {page})...")

            # Initiate search session with POST
            first_doc_ids, total_results, total_pages = self._initiate_search(
                signature=sig, date_from=date_from, date_to=date_to
            )
            logger.info(f"  {sig}: {total_results} total results, {total_pages} pages")

            if total_results == 0:
                continue

            # Process page 1 results (already returned by POST)
            if page == 1:
                for doc_id in first_doc_ids:
                    if doc_id in fetched_ids:
                        continue
                    doc_data = self._fetch_document(doc_id)
                    if doc_data and doc_data.get("text"):
                        yield doc_data
                        total_fetched += 1
                        fetched_ids.add(doc_id)
                page = 2
            else:
                # Resuming from checkpoint — re-initiate search and skip to checkpoint page
                pass

            # Paginate remaining pages using GET /cbo/find?p=N
            while page <= total_pages:
                if max_pages_per_sig and page > max_pages_per_sig:
                    break

                doc_ids = self._fetch_search_page(page)
                if not doc_ids:
                    logger.info(f"  No results on page {page}, stopping")
                    break

                for doc_id in doc_ids:
                    if doc_id in fetched_ids:
                        continue
                    doc_data = self._fetch_document(doc_id)
                    if doc_data and doc_data.get("text"):
                        yield doc_data
                        total_fetched += 1
                        fetched_ids.add(doc_id)

                page += 1

                if use_checkpoint:
                    recent_ids = list(fetched_ids)[-5000:]
                    self._save_checkpoint({
                        "sig_index": si,
                        "page": page,
                        "total_fetched": total_fetched,
                        "fetched_ids": recent_ids,
                        "last_update": datetime.now(timezone.utc).isoformat(),
                    })

                if page % 50 == 0:
                    logger.info(f"    Page {page}/{total_pages} ({total_fetched} fetched so far)")

        if use_checkpoint:
            self._clear_checkpoint()
        logger.info(f"Fetch complete: {total_fetched} tax decisions")

    # -- Abstract method implementations ------------------------------------

    def fetch_all(self, use_checkpoint: bool = True) -> Generator[dict, None, None]:
        logger.info("Starting full fetch of NSA Financial Chamber tax decisions...")
        for doc in self._paginate_all_signatures(use_checkpoint=use_checkpoint):
            yield doc

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        date_from = since.strftime("%Y-%m-%d")
        logger.info(f"Fetching NSA tax decisions since {date_from}")
        for doc in self._paginate_all_signatures(date_from=date_from):
            yield doc

    def normalize(self, raw: dict) -> dict:
        doc_id = raw.get("doc_id", "")
        title = raw.get("title", f"NSA-Tax {doc_id}")
        text = raw.get("text", "")
        judgment_date = raw.get("judgment_date", "")
        case_number = raw.get("case_number", "")
        court = raw.get("court", "Naczelny Sąd Administracyjny")
        decision_type = raw.get("decision_type", "")
        judges = raw.get("judges", [])
        keywords = raw.get("keywords", [])
        legal_bases = raw.get("legal_bases", "")

        return {
            "_id": doc_id,
            "_source": "PL/NSA-Tax",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": judgment_date,
            "url": f"https://orzeczenia.nsa.gov.pl/doc/{doc_id}",
            "case_number": case_number,
            "doc_id": doc_id,
            "court": court,
            "chamber": "Izba Finansowa",
            "decision_type": decision_type,
            "judges": judges,
            "keywords": keywords,
            "legal_bases": legal_bases,
        }

    # -- Custom commands ----------------------------------------------------

    def test_api(self):
        print("Testing NSA Financial Chamber connectivity...")
        for sig in TAX_SIGNATURES:
            doc_ids, total, pages = self._initiate_search(signature=sig)
            print(f"  {sig}: {total} results, {pages} pages, {len(doc_ids)} on first page")

        # Test pagination
        doc_ids_p1, _, _ = self._initiate_search(signature="III FSK")
        if doc_ids_p1:
            time.sleep(1)
            doc_ids_p2 = self._fetch_search_page(2)
            overlap = set(doc_ids_p1) & set(doc_ids_p2)
            print(f"\n  Pagination test: page 1={len(doc_ids_p1)} IDs, page 2={len(doc_ids_p2)} IDs, overlap={len(overlap)}")

            # Fetch one document
            doc = self._fetch_document(doc_ids_p1[0])
            if doc:
                print(f"\n  Sample document {doc_ids_p1[0]}:")
                print(f"    Title: {doc.get('title', 'N/A')[:80]}")
                print(f"    Date: {doc.get('judgment_date', 'N/A')}")
                print(f"    Text: {len(doc.get('text', ''))} chars")
        print("\nTest complete!")


# -- CLI Entry Point -------------------------------------------------------

def main():
    import argparse

    parser = argparse.ArgumentParser(description="PL/NSA-Tax fetcher")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "update", "test-api", "status", "clear-checkpoint"],
    )
    parser.add_argument("--sample", action="store_true")
    parser.add_argument("--sample-size", type=int, default=12)
    parser.add_argument("--no-checkpoint", action="store_true")
    parser.add_argument("--clear-checkpoint", action="store_true")
    parser.add_argument("--full", action="store_true", help="Full fetch (default behavior)")
    parser.add_argument("--workers", type=int, default=5, help="Workers for bootstrap-fast")
    parser.add_argument("--batch-size", type=int, default=100, help="Batch size for bootstrap-fast")

    args = parser.parse_args()
    scraper = NSATaxScraper()

    if args.command == "status":
        checkpoint = scraper._load_checkpoint()
        print(f"  Page: {checkpoint.get('page', 1)}")
        print(f"  Sig index: {checkpoint.get('sig_index', 0)}")
        print(f"  Fetched: {checkpoint.get('total_fetched', 0)}")
        sys.exit(0)

    elif args.command == "clear-checkpoint":
        scraper._clear_checkpoint()
        sys.exit(0)

    elif args.command == "test-api":
        scraper.test_api()

    elif args.command == "bootstrap":
        if args.clear_checkpoint:
            scraper._clear_checkpoint()

        if args.sample:
            stats = scraper.run_sample(n=args.sample_size)
            print(f"\nSample complete: {stats.get('sample_records_saved', 0)} records saved")
        else:
            data_dir = Path(__file__).parent / "data"
            data_dir.mkdir(exist_ok=True)
            count = 0
            errors = 0
            try:
                for raw in scraper.fetch_all(use_checkpoint=not args.no_checkpoint):
                    try:
                        record = scraper.normalize(raw)
                        filename = data_dir / f"{record['_id']}.json"
                        with open(filename, "w", encoding="utf-8") as f:
                            json.dump(record, f, ensure_ascii=False, indent=2)
                        count += 1
                        if count % 500 == 0:
                            logger.info(f"Saved {count} documents")
                    except Exception as e:
                        logger.warning(f"Error normalizing: {e}")
                        errors += 1
            except KeyboardInterrupt:
                logger.info(f"Interrupted. Saved {count} documents.")
                sys.exit(1)
            print(f"\nBootstrap complete: {count} documents, {errors} errors")

    elif args.command == "bootstrap-fast":
        stats = scraper.bootstrap_fast(
            max_workers=args.workers,
            batch_size=args.batch_size,
        )
        print(json.dumps(stats, indent=2))

    elif args.command == "update":
        stats = scraper.update()
        print(f"\nUpdate: {stats['records_new']} new, {stats['records_updated']} updated")


if __name__ == "__main__":
    main()
