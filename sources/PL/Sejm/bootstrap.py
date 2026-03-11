#!/usr/bin/env python3
"""
PL/Sejm -- Polish Parliament Data Fetcher

Fetches Polish parliamentary data from the official Sejm API:
- Transcripts: Full text of parliamentary speeches and debates
- Interpellations: Questions from MPs to ministers with full text replies

Strategy:
  - List proceedings by term: GET /sejm/term{N}/proceedings
  - Get transcripts: GET /sejm/term{N}/proceedings/{num}/{date}/transcripts
  - Get statement HTML: GET /sejm/term{N}/proceedings/{num}/{date}/transcripts/{statementNum}
  - List interpellations: GET /sejm/term{N}/interpellations
  - Get interpellation body: GET /sejm/term{N}/interpellations/{num}/body

API Documentation:
  - Base URL: https://api.sejm.gov.pl
  - OpenAPI: https://api.sejm.gov.pl/sejm/openapi/
  - Swagger UI: https://api.sejm.gov.pl/sejm/openapi/ui/

Data Coverage:
  - Term 10: 2023-present (current term)
  - Terms 7-9: 2011-2023 (archived)
  - Thousands of interpellations per term
  - Full transcripts for all plenary sessions

Usage:
  python bootstrap.py bootstrap           # Full initial pull
  python bootstrap.py bootstrap --sample  # Fetch sample records for validation
  python bootstrap.py update              # Incremental update (recent items)
  python bootstrap.py test-api            # Quick API connectivity test
"""

import sys
import json
import logging
import re
import html
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Generator, Optional, Dict, Any, List

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

import requests
from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.PL.Sejm")

# API configuration
BASE_URL = "https://api.sejm.gov.pl/sejm"
CURRENT_TERM = 10

# Limit statements per proceeding day to avoid huge data
MAX_STATEMENTS_PER_DAY = 50


class SejmScraper(BaseScraper):
    """
    Scraper for PL/Sejm -- Polish Parliament proceedings and interpellations.
    Country: PL
    URL: https://www.sejm.gov.pl

    Data types: parliamentary_proceedings
    Auth: none (Open Government Data)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,application/json;q=0.8,*/*;q=0.7",
            "Accept-Language": "pl,en;q=0.9",
        })

    def _api_get(self, endpoint: str, timeout: int = 60) -> Optional[Any]:
        """Make GET request to API endpoint and return JSON."""
        url = f"{BASE_URL}{endpoint}"
        try:
            self.rate_limiter.wait()
            resp = self.session.get(url, timeout=timeout)
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.JSONDecodeError:
            return None
        except Exception as e:
            logger.warning(f"API request failed for {endpoint}: {e}")
            return None

    def _api_get_html(self, endpoint: str, timeout: int = 60) -> str:
        """Make GET request and return HTML content."""
        url = f"{BASE_URL}{endpoint}"
        try:
            self.rate_limiter.wait()
            resp = self.session.get(url, timeout=timeout)
            resp.raise_for_status()
            return resp.text
        except Exception as e:
            logger.warning(f"API request failed for {endpoint}: {e}")
            return ""

    def _clean_html(self, html_content: str) -> str:
        """Extract clean text from HTML content."""
        if not html_content:
            return ""

        # Remove script and style tags
        content = re.sub(r'<script[^>]*>.*?</script>', '', html_content, flags=re.DOTALL | re.IGNORECASE)
        content = re.sub(r'<style[^>]*>.*?</style>', '', content, flags=re.DOTALL | re.IGNORECASE)

        # Remove HTML comments
        content = re.sub(r'<!--.*?-->', '', content, flags=re.DOTALL)

        # Replace common block elements with newlines
        content = re.sub(r'<br\s*/?>', '\n', content, flags=re.IGNORECASE)
        content = re.sub(r'</p>', '\n', content, flags=re.IGNORECASE)
        content = re.sub(r'</div>', '\n', content, flags=re.IGNORECASE)
        content = re.sub(r'</h[1-6]>', '\n\n', content, flags=re.IGNORECASE)
        content = re.sub(r'</li>', '\n', content, flags=re.IGNORECASE)

        # Remove remaining tags
        content = re.sub(r'<[^>]+>', '', content)

        # Decode HTML entities
        content = html.unescape(content)

        # Clean up whitespace
        content = re.sub(r'\n{3,}', '\n\n', content)
        content = re.sub(r' +', ' ', content)
        content = re.sub(r'^\s+', '', content, flags=re.MULTILINE)

        return content.strip()

    def _list_proceedings(self, term: int = CURRENT_TERM) -> List[Dict[str, Any]]:
        """List all proceedings for a term."""
        endpoint = f"/term{term}/proceedings"
        data = self._api_get(endpoint)
        if data and isinstance(data, list):
            logger.info(f"Found {len(data)} proceedings for term {term}")
            return data
        return []

    def _get_transcripts_list(self, term: int, proc_num: int, date: str) -> List[Dict[str, Any]]:
        """Get list of statements/transcripts for a proceeding day."""
        endpoint = f"/term{term}/proceedings/{proc_num}/{date}/transcripts"
        data = self._api_get(endpoint)
        if data and isinstance(data, dict) and "statements" in data:
            return data["statements"]
        return []

    def _get_statement_html(self, term: int, proc_num: int, date: str, statement_num: int) -> str:
        """Get HTML content of a specific statement."""
        endpoint = f"/term{term}/proceedings/{proc_num}/{date}/transcripts/{statement_num}"
        return self._api_get_html(endpoint)

    def _list_interpellations(self, term: int = CURRENT_TERM, limit: int = 100, offset: int = 0) -> List[Dict[str, Any]]:
        """List interpellations for a term."""
        endpoint = f"/term{term}/interpellations?limit={limit}&offset={offset}"
        data = self._api_get(endpoint)
        if data and isinstance(data, list):
            return data
        return []

    def _get_interpellation_body(self, term: int, num: int) -> str:
        """Get HTML body of an interpellation."""
        endpoint = f"/term{term}/interpellations/{num}/body"
        return self._api_get_html(endpoint)

    def _get_reply_body(self, term: int, interp_num: int, reply_key: str) -> str:
        """Get HTML body of an interpellation reply."""
        endpoint = f"/term{term}/interpellations/{interp_num}/reply/{reply_key}/body"
        return self._api_get_html(endpoint)

    def fetch_transcripts(self, term: int = CURRENT_TERM, max_statements: int = None) -> Generator[dict, None, None]:
        """
        Yield transcript statements from proceedings.

        Each yielded dict contains:
        - Metadata about the speaker and proceeding
        - Full text of the statement
        """
        proceedings = self._list_proceedings(term)
        count = 0

        for proc in proceedings:
            proc_num = proc.get("number")
            dates = proc.get("dates", [])
            proc_title = proc.get("title", f"Proceeding {proc_num}")

            for date in dates:
                statements = self._get_transcripts_list(term, proc_num, date)
                logger.info(f"Found {len(statements)} statements for proceeding {proc_num} on {date}")

                for stmt in statements[:MAX_STATEMENTS_PER_DAY]:
                    stmt_num = stmt.get("num", 0)
                    if stmt_num == 0:  # Skip the Marszałek overview entry
                        continue

                    html_content = self._get_statement_html(term, proc_num, date, stmt_num)
                    if not html_content:
                        continue

                    full_text = self._clean_html(html_content)
                    if not full_text or len(full_text) < 50:
                        continue

                    yield {
                        "type": "transcript",
                        "term": term,
                        "proceeding_num": proc_num,
                        "proceeding_title": proc_title,
                        "date": date,
                        "statement_num": stmt_num,
                        "speaker_name": stmt.get("name", ""),
                        "speaker_function": stmt.get("function", ""),
                        "speaker_id": stmt.get("memberID"),
                        "start_time": stmt.get("startDateTime"),
                        "end_time": stmt.get("endDateTime"),
                        "is_rapporteur": stmt.get("rapporteur", False),
                        "full_text": full_text,
                        "html": html_content,
                    }

                    count += 1
                    if max_statements and count >= max_statements:
                        return

    def fetch_interpellations(self, term: int = CURRENT_TERM, max_items: int = None) -> Generator[dict, None, None]:
        """
        Yield interpellations with full text bodies.

        Each yielded dict contains:
        - Metadata about the interpellation
        - Full text of the question
        - Full text of any replies
        """
        offset = 0
        count = 0
        batch_size = 50

        while True:
            interpellations = self._list_interpellations(term, limit=batch_size, offset=offset)
            if not interpellations:
                break

            logger.info(f"Processing {len(interpellations)} interpellations from offset {offset}")

            for interp in interpellations:
                num = interp.get("num")
                if not num:
                    continue

                # Get interpellation body
                body_html = self._get_interpellation_body(term, num)
                body_text = self._clean_html(body_html)

                if not body_text or len(body_text) < 50:
                    continue

                # Get replies
                replies_data = []
                for reply in interp.get("replies", []):
                    reply_key = reply.get("key")
                    if reply_key and not reply.get("onlyAttachment"):
                        reply_html = self._get_reply_body(term, num, reply_key)
                        reply_text = self._clean_html(reply_html)
                        if reply_text:
                            replies_data.append({
                                "from": reply.get("from", ""),
                                "date": reply.get("receiptDate", ""),
                                "text": reply_text,
                            })

                yield {
                    "type": "interpellation",
                    "term": term,
                    "num": num,
                    "title": interp.get("title", ""),
                    "receipt_date": interp.get("receiptDate", ""),
                    "sent_date": interp.get("sentDate", ""),
                    "from_mps": interp.get("from", []),
                    "to": interp.get("to", []),
                    "full_text": body_text,
                    "replies": replies_data,
                    "last_modified": interp.get("lastModified", ""),
                }

                count += 1
                if max_items and count >= max_items:
                    return

            offset += batch_size
            if len(interpellations) < batch_size:
                break

    def fetch_all(self) -> Generator[dict, None, None]:
        """
        Yield all parliamentary documents from both transcripts and interpellations.
        """
        logger.info("Starting full Sejm fetch...")

        # Fetch transcripts
        logger.info("Fetching transcripts...")
        for doc in self.fetch_transcripts(term=CURRENT_TERM):
            yield doc

        # Fetch interpellations
        logger.info("Fetching interpellations...")
        for doc in self.fetch_interpellations(term=CURRENT_TERM):
            yield doc

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """
        Yield documents updated since the given date.

        For interpellations, checks lastModified field.
        For transcripts, checks proceeding dates.
        """
        since_str = since.strftime("%Y-%m-%d")
        logger.info(f"Fetching updates since {since_str}...")

        # Fetch recent interpellations
        offset = 0
        batch_size = 50

        while True:
            interpellations = self._list_interpellations(CURRENT_TERM, limit=batch_size, offset=offset)
            if not interpellations:
                break

            found_old = False
            for interp in interpellations:
                last_mod = interp.get("lastModified", "")
                if last_mod and last_mod[:10] >= since_str:
                    num = interp.get("num")
                    body_html = self._get_interpellation_body(CURRENT_TERM, num)
                    body_text = self._clean_html(body_html)

                    if body_text and len(body_text) >= 50:
                        replies_data = []
                        for reply in interp.get("replies", []):
                            reply_key = reply.get("key")
                            if reply_key and not reply.get("onlyAttachment"):
                                reply_html = self._get_reply_body(CURRENT_TERM, num, reply_key)
                                reply_text = self._clean_html(reply_html)
                                if reply_text:
                                    replies_data.append({
                                        "from": reply.get("from", ""),
                                        "date": reply.get("receiptDate", ""),
                                        "text": reply_text,
                                    })

                        yield {
                            "type": "interpellation",
                            "term": CURRENT_TERM,
                            "num": num,
                            "title": interp.get("title", ""),
                            "receipt_date": interp.get("receiptDate", ""),
                            "sent_date": interp.get("sentDate", ""),
                            "from_mps": interp.get("from", []),
                            "to": interp.get("to", []),
                            "full_text": body_text,
                            "replies": replies_data,
                            "last_modified": last_mod,
                        }
                else:
                    found_old = True

            offset += batch_size
            if found_old or len(interpellations) < batch_size:
                break

    def normalize(self, raw: dict) -> dict:
        """
        Transform raw API data into standard schema.

        CRITICAL: Includes full text in the 'text' field.
        """
        doc_type = raw.get("type", "")

        if doc_type == "transcript":
            # Create unique ID for transcript statement
            term = raw.get("term", CURRENT_TERM)
            proc_num = raw.get("proceeding_num", 0)
            date = raw.get("date", "")
            stmt_num = raw.get("statement_num", 0)
            doc_id = f"PL/Sejm/T{term}/P{proc_num}/{date}/S{stmt_num}"

            # Build title from speaker and proceeding
            speaker = raw.get("speaker_name", "")
            function = raw.get("speaker_function", "")
            proc_title = raw.get("proceeding_title", "")
            title = f"{function} {speaker} - {proc_title}".strip()
            if title.startswith(" - "):
                title = title[3:]

            # Build URL
            url = f"https://sejm.gov.pl/sejm{term}.nsf/wypowiedz.xsp?posiedzenie={proc_num}&dzien={date}&wyession={stmt_num}"

            return {
                "_id": doc_id,
                "_source": "PL/Sejm",
                "_type": "parliamentary_proceedings",
                "_fetched_at": datetime.now(timezone.utc).isoformat(),
                "title": title,
                "text": raw.get("full_text", ""),  # MANDATORY FULL TEXT
                "date": date,
                "url": url,
                "doc_type": "transcript",
                "term": term,
                "proceeding_num": proc_num,
                "proceeding_title": proc_title,
                "statement_num": stmt_num,
                "speaker_name": speaker,
                "speaker_function": function,
                "speaker_id": raw.get("speaker_id"),
                "language": "pl",
            }

        elif doc_type == "interpellation":
            # Create unique ID for interpellation
            term = raw.get("term", CURRENT_TERM)
            num = raw.get("num", 0)
            doc_id = f"PL/Sejm/T{term}/INT{num}"

            # Build full text including replies
            full_text = raw.get("full_text", "")
            replies = raw.get("replies", [])
            if replies:
                reply_texts = []
                for r in replies:
                    reply_texts.append(f"\n\n--- Reply from {r.get('from', 'Unknown')} ({r.get('date', '')}) ---\n\n{r.get('text', '')}")
                full_text += "\n".join(reply_texts)

            # Build URL
            url = f"https://sejm.gov.pl/sejm{term}.nsf/interpelacja.xsp?typ=int&nr={num}"

            return {
                "_id": doc_id,
                "_source": "PL/Sejm",
                "_type": "parliamentary_proceedings",
                "_fetched_at": datetime.now(timezone.utc).isoformat(),
                "title": raw.get("title", ""),
                "text": full_text,  # MANDATORY FULL TEXT
                "date": raw.get("receipt_date", "") or raw.get("sent_date", ""),
                "url": url,
                "doc_type": "interpellation",
                "term": term,
                "interpellation_num": num,
                "from_mps": raw.get("from_mps", []),
                "to": raw.get("to", []),
                "reply_count": len(replies),
                "last_modified": raw.get("last_modified", ""),
                "language": "pl",
            }

        else:
            # Unknown type
            return {
                "_id": f"PL/Sejm/unknown/{datetime.now(timezone.utc).timestamp()}",
                "_source": "PL/Sejm",
                "_type": "parliamentary_proceedings",
                "_fetched_at": datetime.now(timezone.utc).isoformat(),
                "title": raw.get("title", "Unknown"),
                "text": raw.get("full_text", ""),
                "date": "",
                "url": "",
                "doc_type": doc_type,
                "language": "pl",
            }

    def test_api(self):
        """Quick connectivity and API test."""
        print("Testing Polish Sejm API...")

        # Test proceedings
        print("\n1. Testing proceedings endpoint...")
        proceedings = self._list_proceedings(CURRENT_TERM)
        if proceedings:
            print(f"   Found {len(proceedings)} proceedings for term {CURRENT_TERM}")
            print(f"   First: {proceedings[0].get('title', '')[:60]}...")
        else:
            print("   ERROR: No proceedings returned")
            return

        # Test transcripts
        print("\n2. Testing transcripts endpoint...")
        if proceedings:
            proc = proceedings[-1]  # Latest proceeding
            proc_num = proc.get("number")
            dates = proc.get("dates", [])
            if dates:
                date = dates[0]
                statements = self._get_transcripts_list(CURRENT_TERM, proc_num, date)
                print(f"   Found {len(statements)} statements for proceeding {proc_num} on {date}")
                if statements and len(statements) > 1:
                    stmt = statements[1]  # Skip Marszałek overview
                    stmt_num = stmt.get("num")
                    html_content = self._get_statement_html(CURRENT_TERM, proc_num, date, stmt_num)
                    text = self._clean_html(html_content)
                    print(f"   Statement {stmt_num} from {stmt.get('name')}: {len(text)} chars")
                    print(f"   Preview: {text[:200]}...")

        # Test interpellations
        print("\n3. Testing interpellations endpoint...")
        interpellations = self._list_interpellations(CURRENT_TERM, limit=5)
        if interpellations:
            print(f"   Found interpellations (showing first 5)")
            interp = interpellations[0]
            num = interp.get("num")
            body_html = self._get_interpellation_body(CURRENT_TERM, num)
            body_text = self._clean_html(body_html)
            print(f"   Interpellation {num}: {interp.get('title', '')[:50]}...")
            print(f"   Body length: {len(body_text)} chars")
            print(f"   Preview: {body_text[:200]}...")
        else:
            print("   ERROR: No interpellations returned")

        print("\nAPI test complete!")


def main():
    scraper = SejmScraper()

    if len(sys.argv) < 2:
        print(
            "Usage: python bootstrap.py [bootstrap|update|test-api] "
            "[--sample] [--sample-size N]"
        )
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv
    sample_size = 12
    if "--sample-size" in sys.argv:
        idx = sys.argv.index("--sample-size")
        sample_size = int(sys.argv[idx + 1])

    if command == "test-api":
        scraper.test_api()

    elif command == "bootstrap":
        if sample_mode:
            # For sample mode, fetch a mix of transcripts and interpellations
            print(f"Fetching {sample_size} sample records...")
            saved = 0
            sample_dir = scraper.source_dir / "sample"
            sample_dir.mkdir(parents=True, exist_ok=True)

            # First half from interpellations
            interp_count = sample_size // 2
            for raw in scraper.fetch_interpellations(term=CURRENT_TERM, max_items=interp_count):
                normalized = scraper.normalize(raw)
                if normalized.get("text"):
                    filename = f"{normalized['_id'].replace('/', '_')}.json"
                    filepath = sample_dir / filename
                    with open(filepath, "w", encoding="utf-8") as f:
                        json.dump(normalized, f, ensure_ascii=False, indent=2)
                    saved += 1
                    print(f"  [{saved}] {normalized['doc_type']}: {normalized['title'][:50]}... ({len(normalized['text'])} chars)")

            # Second half from transcripts
            transcript_count = sample_size - saved
            for raw in scraper.fetch_transcripts(term=CURRENT_TERM, max_statements=transcript_count + 5):
                normalized = scraper.normalize(raw)
                if normalized.get("text") and len(normalized["text"]) > 200:
                    filename = f"{normalized['_id'].replace('/', '_')}.json"
                    filepath = sample_dir / filename
                    with open(filepath, "w", encoding="utf-8") as f:
                        json.dump(normalized, f, ensure_ascii=False, indent=2)
                    saved += 1
                    print(f"  [{saved}] {normalized['doc_type']}: {normalized['title'][:50]}... ({len(normalized['text'])} chars)")
                    if saved >= sample_size:
                        break

            print(f"\nSample complete: {saved} records saved to sample/")

            # Calculate stats
            text_lengths = []
            for f in sample_dir.glob("*.json"):
                with open(f, "r", encoding="utf-8") as fp:
                    doc = json.load(fp)
                    text_lengths.append(len(doc.get("text", "")))

            if text_lengths:
                avg_len = sum(text_lengths) // len(text_lengths)
                print(f"Average text length: {avg_len} chars")

            stats = {"sample_records_saved": saved}
            print(json.dumps(stats, indent=2))
        else:
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
