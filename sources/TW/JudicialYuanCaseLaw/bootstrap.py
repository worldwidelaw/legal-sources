#!/usr/bin/env python3
"""
TW/JudicialYuanCaseLaw - Taiwan Judicial Yuan Judgment Search Fetcher

Fetches full-text court judgments from Taiwan's Judicial Yuan.
Covers all court levels: Supreme Court, High Courts, District Courts,
Administrative Courts, Constitutional Court, Disciplinary Court.

Data source: https://judgment.judicial.gov.tw/
Method: ASP.NET search form + HTML scraping of judgment pages
License: Public domain (Taiwan government open data)
Rate limit: ~2 seconds between requests

Court systems:
  M = Criminal, V = Civil, A = Administrative, P = Disciplinary, C = Constitutional

Usage:
  python bootstrap.py bootstrap --sample   # Fetch 15 sample records
  python bootstrap.py bootstrap             # Full bootstrap
  python bootstrap.py test                  # Test connectivity
"""

import argparse
import html as html_mod
import json
import re
import sys
import time
from datetime import datetime, timezone
from html.parser import HTMLParser
from pathlib import Path
from typing import Dict, Generator, List, Optional, Tuple
from urllib.parse import quote, unquote

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

SOURCE_ID = "TW/JudicialYuanCaseLaw"
SAMPLE_DIR = Path(__file__).parent / "sample"
BASE_URL = "https://judgment.judicial.gov.tw"
FJUD_URL = f"{BASE_URL}/FJUD"

HEADERS = {
    "User-Agent": "LegalDataHunter/1.0 (Open Data Research; github.com/ZachLaik/LegalDataHunter)",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

# Major courts to fetch from
COURTS = {
    "TPS": "最高法院 (Supreme Court)",
    "TPA": "最高行政法院 (Supreme Administrative Court)",
    "TPH": "臺灣高等法院 (Taiwan High Court)",
    "TPD": "臺灣臺北地方法院 (Taipei District Court)",
    "SCD": "臺灣新竹地方法院 (Hsinchu District Court)",
    "TCD": "臺灣臺中地方法院 (Taichung District Court)",
    "KSD": "臺灣高雄地方法院 (Kaohsiung District Court)",
}

# Court systems
SYSTEMS = {
    "V": "Civil",
    "M": "Criminal",
}


class HTMLTextExtractor(HTMLParser):
    """Extract plain text from HTML."""

    def __init__(self):
        super().__init__()
        self.text_parts = []
        self.skip_tags = {"script", "style", "head", "meta", "link"}
        self.current_skip = False

    def handle_starttag(self, tag, attrs):
        if tag.lower() in self.skip_tags:
            self.current_skip = True

    def handle_endtag(self, tag):
        if tag.lower() in self.skip_tags:
            self.current_skip = False
        elif tag.lower() in {"p", "div", "br", "h1", "h2", "h3", "li", "tr"}:
            self.text_parts.append("\n")

    def handle_data(self, data):
        if not self.current_skip:
            self.text_parts.append(data)

    def handle_entityref(self, name):
        if not self.current_skip:
            self.text_parts.append(html_mod.unescape(f"&{name};"))

    def handle_charref(self, name):
        if not self.current_skip:
            self.text_parts.append(html_mod.unescape(f"&#{name};"))

    def get_text(self):
        return "".join(self.text_parts)


def strip_html(text: str) -> str:
    """Extract plain text from HTML content."""
    if not text:
        return ""
    parser = HTMLTextExtractor()
    try:
        parser.feed(text)
        result = parser.get_text()
    except Exception:
        result = re.sub(r"<[^>]+>", " ", text)

    result = re.sub(r"&nbsp;", " ", result)
    result = re.sub(r"\n{3,}", "\n\n", result)
    result = re.sub(r" {2,}", " ", result)
    result = re.sub(r"^\s+$", "", result, flags=re.MULTILINE)
    return result.strip()


class JudicialYuanFetcher:
    """Fetcher for Taiwan Judicial Yuan court judgments."""

    def __init__(self):
        self.session = requests.Session()
        retry = Retry(total=3, backoff_factor=1, status_forcelist=[429, 502, 503, 504])
        adapter = HTTPAdapter(max_retries=retry)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)
        self.session.headers.update(HEADERS)

    def _get_search_tokens(self) -> Dict[str, str]:
        """Get ASP.NET ViewState tokens from the search form."""
        r = self.session.get(f"{FJUD_URL}/Default_AD.aspx", timeout=30)
        r.raise_for_status()
        html = r.text

        vs = re.search(r'__VIEWSTATE.*?value="([^"]+)"', html)
        ev = re.search(r'__EVENTVALIDATION.*?value="([^"]+)"', html)
        vsg = re.search(r'__VIEWSTATEGENERATOR.*?value="([^"]+)"', html)

        if not vs or not ev:
            raise ValueError("Failed to extract ViewState tokens")

        return {
            "__VIEWSTATE": vs.group(1),
            "__VIEWSTATEGENERATOR": vsg.group(1) if vsg else "",
            "__EVENTVALIDATION": ev.group(1),
        }

    def search_judgments(
        self,
        court: str,
        system: str,
        year_start: int,
        month_start: int,
        year_end: int,
        month_end: int,
    ) -> Optional[str]:
        """
        Submit a search and return the query hash for result pagination.

        Args:
            court: Court code (e.g., "TPS")
            system: Case system (V=Civil, M=Criminal)
            year_start/month_start: Start date (ROC year, month)
            year_end/month_end: End date (ROC year, month)

        Returns:
            Query hash string, or None if search failed
        """
        tokens = self._get_search_tokens()

        data = {
            "__VIEWSTATE": tokens["__VIEWSTATE"],
            "__VIEWSTATEGENERATOR": tokens["__VIEWSTATEGENERATOR"],
            "__EVENTVALIDATION": tokens["__EVENTVALIDATION"],
            "__VIEWSTATEENCRYPTED": "",
            "jud_court": court,
            "jud_sys": system,
            "jud_year": "",
            "sel_judword": "",
            "jud_case": "",
            "jud_no": "",
            "jud_no_end": "",
            "dy1": str(year_start),
            "dm1": f"{month_start:02d}",
            "dd1": "01",
            "dy2": str(year_end),
            "dm2": f"{month_end:02d}",
            "dd2": "28",
            "jud_title": "",
            "jud_jmain": "",
            "jud_kw": "",
            "KbStart": "",
            "KbEnd": "",
            "judtype": "",
            "whosub": "0",
            "ctl00$cp_content$btnQry": "送出查詢",
        }

        r = self.session.post(
            f"{FJUD_URL}/Default_AD.aspx", data=data, timeout=30
        )
        r.raise_for_status()

        m = re.search(r'<iframe[^>]*src="([^"]+)"', r.text)
        if not m:
            return None

        iframe_url = html_mod.unescape(m.group(1))
        q_match = re.search(r"q=([a-f0-9]+)", iframe_url)
        return q_match.group(1) if q_match else None

    def get_result_page(
        self, query_hash: str, page: int = 1
    ) -> Tuple[List[str], int]:
        """
        Fetch a page of search results.

        Returns:
            Tuple of (list of judgment IDs, total result count)
        """
        url = f"{FJUD_URL}/qryresultlst.aspx?ty=JUDBOOK&q={query_hash}&sort=DS&page={page}"
        r = self.session.get(url, timeout=30)
        r.raise_for_status()

        # Extract judgment IDs
        ids = re.findall(r"data\.aspx\?ty=JD&amp;id=([^\"&]+)", r.text)
        decoded_ids = [unquote(id_str) for id_str in ids]

        # Extract total count
        total = 0
        total_m = re.search(r"共\s*([\d,]+)\s*筆", r.text)
        if total_m:
            total = int(total_m.group(1).replace(",", ""))

        return decoded_ids, total

    def fetch_judgment(self, judgment_id: str) -> Optional[Dict]:
        """Fetch full text of a single judgment."""
        url = f"{FJUD_URL}/data.aspx?ty=JD&id={quote(judgment_id)}&ot=in"
        try:
            r = self.session.get(url, timeout=30)
            r.raise_for_status()
        except Exception as e:
            print(f"    Error fetching {judgment_id}: {e}")
            return None

        html = r.text

        # Extract title from <title> tag
        title_m = re.search(r"<title>\s*(.+?)\s*</title>", html)
        title = title_m.group(1).strip() if title_m else ""

        # Extract metadata from the jud div
        metadata = {}
        rows = re.findall(
            r'<div class="col-th">(.*?)</div>\s*<div class="col-td">(.*?)</div>',
            html,
            re.DOTALL,
        )
        for th, td in rows:
            key = strip_html(th).strip().rstrip("：:")
            val = strip_html(td).strip()
            if key and val:
                metadata[key] = val

        # Extract judgment full text from jud_content div
        # The content is inside: <div class="col-td jud_content">...</div>
        text = ""
        content_m = re.search(
            r'class="col-td jud_content"[^>]*>(.*?)(?=<div class="law-tool-box"|<div class="row">\s*</div>\s*</div>)',
            html,
            re.DOTALL,
        )
        if not content_m:
            # Broader fallback
            content_m = re.search(
                r'class="col-td jud_content"[^>]*>(.*)',
                html,
                re.DOTALL,
            )

        if content_m:
            raw_content = content_m.group(1)
            # Find the end - look for closing divs pattern
            depth = 1
            end_idx = 0
            i = 0
            while i < len(raw_content) and depth > 0:
                if raw_content[i:i+5] == "<div " or raw_content[i:i+4] == "<div>":
                    depth += 1
                elif raw_content[i:i+6] == "</div>":
                    depth -= 1
                    if depth == 0:
                        end_idx = i
                        break
                i += 1

            if end_idx > 0:
                raw_content = raw_content[:end_idx]

            text = strip_html(raw_content)

        if not text and not metadata:
            return None

        return {
            "judgment_id": judgment_id,
            "title": title,
            "metadata": metadata,
            "text": text,
        }

    def normalize(self, raw: Dict) -> Optional[Dict]:
        """Normalize a raw judgment to standard schema."""
        text = raw.get("text", "")
        if not text or len(text) < 100:
            return None

        judgment_id = raw.get("judgment_id", "")
        metadata = raw.get("metadata", {})

        # Parse judgment ID: COURT,YEAR,TYPE,NUM,DATE,SEQ
        parts = judgment_id.split(",")
        court_code = parts[0] if parts else ""
        roc_year = parts[1] if len(parts) > 1 else ""
        case_type = parts[2] if len(parts) > 2 else ""
        case_num = parts[3] if len(parts) > 3 else ""
        date_str = parts[4] if len(parts) > 4 else ""

        # Convert ROC date to ISO date
        iso_date = None
        if date_str and len(date_str) == 8:
            try:
                y = int(date_str[:4])
                m = int(date_str[4:6])
                d = int(date_str[6:8])
                iso_date = f"{y:04d}-{m:02d}-{d:02d}"
            except ValueError:
                pass

        # If no date from ID, try metadata
        if not iso_date:
            meta_date = metadata.get("裁判日期", "")
            roc_m = re.search(r"(\d+)\s*年\s*(\d+)\s*月\s*(\d+)\s*日", meta_date)
            if roc_m:
                y = int(roc_m.group(1)) + 1911
                m = int(roc_m.group(2))
                d = int(roc_m.group(3))
                iso_date = f"{y:04d}-{m:02d}-{d:02d}"

        doc_id = judgment_id.replace(",", "_")
        title = raw.get("title", "") or metadata.get("裁判字號", "")

        return {
            "_id": f"TW_{doc_id}",
            "_source": SOURCE_ID,
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": iso_date,
            "url": f"{FJUD_URL}/data.aspx?ty=JD&id={quote(judgment_id)}&ot=in",
            "court": court_code,
            "case_type": case_type,
            "case_number": f"{roc_year},{case_type},{case_num}" if roc_year else "",
            "cause": metadata.get("裁判案由", ""),
        }

    def fetch_all(self, sample: bool = False) -> Generator[Dict, None, None]:
        """Fetch court judgments with full text."""
        if sample:
            # For sample: fetch from Supreme Court, recent month
            courts = [("TPS", "M"), ("TPS", "V")]
            year_start = year_end = 114  # ROC year = 2025
            month_start = month_end = 1
            max_per_search = 8
        else:
            courts = [(c, s) for c in COURTS for s in SYSTEMS]
            year_start = year_end = 113  # ROC year = 2024
            month_start = 1
            month_end = 12
            max_per_search = 100

        total_yielded = 0

        for court, system in courts:
            court_name = COURTS.get(court, court)
            sys_name = SYSTEMS.get(system, system)
            print(f"\n  Searching {court_name} [{sys_name}]...")

            try:
                query_hash = self.search_judgments(
                    court, system, year_start, month_start, year_end, month_end
                )
            except Exception as e:
                print(f"    Search failed: {e}")
                continue

            if not query_hash:
                print("    No results")
                continue

            time.sleep(1)

            page = 1
            page_yielded = 0
            while True:
                try:
                    ids, total = self.get_result_page(query_hash, page)
                except Exception as e:
                    print(f"    Page {page} failed: {e}")
                    break

                if page == 1:
                    print(f"    Found {total} judgments")

                if not ids:
                    break

                for jid in ids:
                    if max_per_search and page_yielded >= max_per_search:
                        break

                    time.sleep(2)  # Rate limit
                    raw = self.fetch_judgment(jid)
                    if not raw:
                        continue

                    record = self.normalize(raw)
                    if record:
                        yield record
                        total_yielded += 1
                        page_yielded += 1
                        print(
                            f"    [{total_yielded}] {record['title'][:60]} "
                            f"({len(record['text'])} chars)"
                        )

                        if sample and total_yielded >= 15:
                            return

                if max_per_search and page_yielded >= max_per_search:
                    break

                # Check if there are more pages (20 results per page)
                if len(ids) < 20:
                    break
                page += 1
                time.sleep(1)

        print(f"\n  Total: {total_yielded} judgments fetched")


def test_connection():
    """Test connectivity to Taiwan Judicial Yuan."""
    print("Testing Taiwan Judicial Yuan connectivity...")
    fetcher = JudicialYuanFetcher()

    print("\n1. Getting search form tokens...")
    try:
        tokens = fetcher._get_search_tokens()
        print(f"   OK: Got ViewState ({len(tokens['__VIEWSTATE'])} chars)")
    except Exception as e:
        print(f"   FAIL: {e}")
        return False

    print("\n2. Submitting test search (Supreme Court, Criminal, Jan 2025)...")
    try:
        qh = fetcher.search_judgments("TPS", "M", 114, 1, 114, 1)
        if qh:
            print(f"   OK: Query hash = {qh}")
        else:
            print("   FAIL: No query hash returned")
            return False
    except Exception as e:
        print(f"   FAIL: {e}")
        return False

    time.sleep(1)

    print("\n3. Fetching result list...")
    try:
        ids, total = fetcher.get_result_page(qh, 1)
        print(f"   OK: {total} total results, {len(ids)} on first page")
    except Exception as e:
        print(f"   FAIL: {e}")
        return False

    if not ids:
        print("   FAIL: No judgment IDs found")
        return False

    time.sleep(2)

    print(f"\n4. Fetching judgment: {ids[0]}...")
    try:
        raw = fetcher.fetch_judgment(ids[0])
        if raw:
            record = fetcher.normalize(raw)
            if record:
                print(f"   OK: {record['title'][:80]}")
                print(f"   Text length: {len(record['text'])} chars")
                print(f"   Date: {record['date']}")
                print(f"   Text preview: {record['text'][:150]}...")
            else:
                print("   FAIL: Normalization returned None")
                return False
        else:
            print("   FAIL: Could not fetch judgment")
            return False
    except Exception as e:
        print(f"   FAIL: {e}")
        return False

    print("\nAll tests passed!")
    return True


def main():
    parser = argparse.ArgumentParser(
        description="TW/JudicialYuanCaseLaw Taiwan Court Judgments Fetcher"
    )
    parser.add_argument("command", choices=["bootstrap", "test"])
    parser.add_argument("--sample", action="store_true")
    args = parser.parse_args()

    if args.command == "test":
        success = test_connection()
        sys.exit(0 if success else 1)

    elif args.command == "bootstrap":
        SAMPLE_DIR.mkdir(parents=True, exist_ok=True)
        fetcher = JudicialYuanFetcher()
        count = 0

        for record in fetcher.fetch_all(sample=args.sample):
            filename = re.sub(r"[^\w\-]", "_", record["_id"]) + ".json"
            filepath = SAMPLE_DIR / filename
            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)
            count += 1
            print(f"  Saved: {filepath.name}")

        print(f"\nBootstrap complete: {count} records saved to {SAMPLE_DIR}")


if __name__ == "__main__":
    main()
