#!/usr/bin/env python3
"""
Uzbekistan Legislation (Lex.uz) Data Fetcher

Official national database of legislative information of the Republic of Uzbekistan.
https://lex.uz/

Document pages are server-rendered inside <div id="divCont">.  Each element is a
`<div class="{CLASS} lx_elem">` wrapper holding a `<div class="lx_elem2">` UI chrome
block followed by the payload container.  The payload container used to be
`<a id="NNNN">…</a>`; lex.uz now emits `<div name="NNNN" id="NNNN">…</div>` (issue
#1289 — the old anchor-shaped regex matched nothing, so every document extracted
empty text).  The parser below walks the wrapper with a balanced-div scan and takes
whatever survives after the chrome is removed, so it is agnostic to the payload tag.

Search enumeration uses the ASP.NET results grid at /ru/search/nat with a date range.
Pagination is a WebForms postback: the "Следующий" (next) link posts
`ucFoundActsControl$LinkButton1` with the page's __VIEWSTATE, and the server keeps the
result cursor in the session — so a cookie jar is mandatory.  Crawling one year at a
time keeps each cursor short and lets the checkpoint resume year by year.

~70K acts across all languages.  No authentication required.
"""

import json
import logging
import os
import re
import subprocess
import sys
import tempfile
import time
import urllib.parse
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Iterator, Optional, List, Set

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

BASE_URL = "https://lex.uz"
SEARCH_URL = f"{BASE_URL}/ru/search/nat"
DOC_URL = f"{BASE_URL}/ru/docs/{{}}"

# Oldest acts in the national database
FIRST_YEAR = 1991

DATA_DIR = Path(__file__).parent / 'data'
RECORDS_PATH = DATA_DIR / 'records.jsonl'
CHECKPOINT_PATH = DATA_DIR / 'checkpoint.json'

# Postback targets on the results grid
NEXT_PAGE_TARGET = 'ucFoundActsControl$LinkButton1'
LAST_PAGE_TARGET = 'ucFoundActsControl$lblpage'

# lx_elem classes that carry header/metadata rather than body text
META_CLASSES = {
    'ACT_FORM', 'ACT_TITLE', 'ACCEPTING_BODY', 'SIGNATURE', 'DEPARTMENTAL',
    'ACT_ESSENTIAL_ELEMENTS', 'ACT_ESSENTIAL_ELEMENTS_NUM', 'UNOFFIAL',
}

# lx_elem classes that are page furniture, never document content
SKIP_CLASS_RE = re.compile(r'^(APPL_BANNER|BANNER|ADV)')


def strip_html(text: str) -> str:
    """Remove HTML tags and decode entities."""
    text = re.sub(r'<br\s*/?>', '\n', text)
    text = re.sub(r'</(p|div|tr|h\d)>', '\n', text)
    text = re.sub(r'<[^>]+>', '', text)
    text = text.replace('&nbsp;', ' ').replace('&amp;', '&')
    text = text.replace('&lt;', '<').replace('&gt;', '>')
    text = text.replace('&quot;', '"').replace('&#39;', "'")
    text = text.replace('&laquo;', '«').replace('&raquo;', '»')
    text = text.replace('&mdash;', '—').replace('&ndash;', '–')
    return text


def extract_divCont(html: str) -> str:
    """Return the HTML from <div id="divCont"> onwards."""
    m = re.search(r'<div\s+id="divCont"[^>]*>', html)
    if not m:
        return ''
    return html[m.end():]


def iter_lx_elements(html: str) -> Iterator[tuple]:
    """Yield (css_class, inner_html) for every `<div class="X lx_elem">` wrapper.

    Uses a balanced <div> scan so nested markup (tables, appendices) stays intact.
    """
    for m in re.finditer(r'<div\s+class="([A-Z_0-9]+)\s+lx_elem"[^>]*>', html):
        start = m.end()
        depth = 1
        end = len(html)
        for t in re.finditer(r'<div\b|</div>', html[start:]):
            if t.group(0) == '</div>':
                depth -= 1
                if depth == 0:
                    end = start + t.start()
                    break
            else:
                depth += 1
        yield m.group(1), html[start:end]


def _strip_chrome(inner: str) -> str:
    """Drop the lx_elem2 toolbar (comment/audio/permalink buttons) from an element."""
    m = re.search(r'<div\s+class="lx_elem2"[^>]*>', inner)
    if not m:
        return inner
    start = m.start()
    depth = 1
    end = len(inner)
    for t in re.finditer(r'<div\b|</div>', inner[m.end():]):
        if t.group(0) == '</div>':
            depth -= 1
            if depth == 0:
                end = m.end() + t.end()
                break
        else:
            depth += 1
    return inner[:start] + inner[end:]


def parse_document_content(html: str) -> Dict[str, Any]:
    """Parse a document page and return structured fields."""
    divcont = extract_divCont(html)
    if not divcont:
        return {}

    doc_type = ''
    title = ''
    signature = ''
    body_parts: List[str] = []
    meta_parts: List[str] = []

    for cls, inner in iter_lx_elements(divcont):
        if SKIP_CLASS_RE.match(cls):
            continue
        text = strip_html(_strip_chrome(inner)).strip()
        if not text:
            continue

        if cls == 'ACT_FORM':
            doc_type = doc_type or text
        elif cls == 'ACT_TITLE':
            title = title or text
        elif cls == 'SIGNATURE':
            signature = signature or text
        elif cls == 'UNOFFIAL':
            meta_parts.append(f"[{text}]")
        elif cls in META_CLASSES:
            meta_parts.append(text)
        else:
            # ACT_TEXT, TEXT_HEADER_DEFAULT, BY_DEFAULT, FOOTNOTE, ACT_TITLE_APPL,
            # GRIF_PARLAMENT, COMMENT_FOR_WARNING and any class lex.uz adds later.
            body_parts.append(text)

    if not body_parts and not title:
        return {}

    full_text = '\n\n'.join(body_parts)
    if signature:
        full_text += f"\n\n{signature}"

    full_text = re.sub(r'[ \t]+', ' ', full_text)
    full_text = re.sub(r'\n{3,}', '\n\n', full_text)
    full_text = full_text.strip()

    return {
        'doc_type': doc_type,
        'title': title,
        'text': full_text,
        'signature': signature,
        'meta': meta_parts,
    }


def parse_title_meta(html: str) -> Dict[str, str]:
    """Extract doc number and date from the <title> tag."""
    m = re.search(r'<title>\s*(?:&nbsp;)?\s*(.+?)\s*</title>', html, re.DOTALL)
    if not m:
        return {}
    raw = m.group(1).replace('&nbsp;', ' ').strip()
    # Format: "3604-сон 27.01.2025. Об утверждении..."
    dm = re.match(r'(\S+)\s+(\d{2}\.\d{2}\.\d{4})\.\s*(.*)', raw)
    if dm:
        date_str = dm.group(2)
        try:
            iso_date = datetime.strptime(date_str, '%d.%m.%Y').strftime('%Y-%m-%d')
        except ValueError:
            iso_date = date_str
        return {
            'doc_number': dm.group(1),
            'date': iso_date,
            'title_from_meta': dm.group(3).strip(),
        }
    return {'title_from_meta': raw}


class LexUzFetcher:
    """Fetcher for Uzbekistan legislation from lex.uz"""

    def __init__(self, slow_mode: bool = False):
        self.slow_mode = slow_mode
        self.doc_delay = 3.0 if slow_mode else 1.5
        self.page_delay = 5.0 if slow_mode else 2.0
        self._jar = Path(tempfile.mkdtemp(prefix='lexuz_')) / 'cookies.txt'

    # ------------------------------------------------------------------ HTTP

    def _curl(self, url: str, post_data: Optional[str] = None,
              max_attempts: int = 3) -> Optional[str]:
        """GET or POST via curl, sharing one cookie jar for the ASP.NET session."""
        for attempt in range(max_attempts):
            cmd = ['curl', '-s', '-L', '--max-time', '60',
                   '-c', str(self._jar), '-b', str(self._jar),
                   '-H', 'User-Agent: Mozilla/5.0 (compatible; LegalDataHunter/1.0)',
                   '-H', 'Accept: text/html,application/xhtml+xml',
                   '-H', 'Accept-Language: ru,en;q=0.5']
            if post_data is not None:
                cmd += ['-X', 'POST',
                        '-H', 'Content-Type: application/x-www-form-urlencoded',
                        '--data', post_data]
            cmd.append(url)
            try:
                result = subprocess.run(cmd, capture_output=True, text=True, timeout=90)
                if result.returncode == 0 and result.stdout:
                    return result.stdout
            except subprocess.TimeoutExpired:
                pass
            delay = min(5 * (2 ** attempt), 30)
            logger.warning(f"Request failed (attempt {attempt+1}/{max_attempts}) "
                           f"for {url}, waiting {delay}s...")
            time.sleep(delay)
        return None

    def _curl_get(self, url: str, max_attempts: int = 3) -> Optional[str]:
        return self._curl(url, None, max_attempts)

    @staticmethod
    def _extract_asp_fields(html: str) -> Dict[str, str]:
        """Extract ASP.NET hidden form fields from HTML."""
        fields = {}
        for m in re.finditer(r'<input[^>]+name="(__[^"]+)"[^>]+value="([^"]*)"', html):
            fields[m.group(1)] = m.group(2)
        for m in re.finditer(r'<input[^>]+value="([^"]*)"[^>]+name="(__[^"]+)"', html):
            fields.setdefault(m.group(2), m.group(1))
        return fields

    def _postback(self, url: str, html: str, target: str) -> Optional[str]:
        """Fire an ASP.NET __doPostBack against the current results page."""
        fields = self._extract_asp_fields(html)
        form = {
            '__VIEWSTATE': fields.get('__VIEWSTATE', ''),
            '__VIEWSTATEGENERATOR': fields.get('__VIEWSTATEGENERATOR', ''),
            '__EVENTVALIDATION': fields.get('__EVENTVALIDATION', ''),
            '__EVENTTARGET': target,
            '__EVENTARGUMENT': '',
        }
        return self._curl(url, urllib.parse.urlencode(form))

    # ---------------------------------------------------------------- search

    @staticmethod
    def _has_next_page(html: str) -> bool:
        """True when the "Следующий" link is an active postback (not disabled)."""
        m = re.search(r'<a\s+id="ucFoundActsControl_LinkButton1"([^>]*)>', html)
        return bool(m) and '__doPostBack' in m.group(1)

    @staticmethod
    def _parse_search_doc_ids(html: str) -> List[Dict[str, str]]:
        """Extract document IDs and basic info from search results HTML."""
        results: List[Dict[str, str]] = []
        seen: Set[str] = set()
        # Links like: <a class="lx_link" href="/ru/docs/7965945?query=...">TITLE</a>
        for m in re.finditer(
            r'<a[^>]+class="lx_link"[^>]+href="(/ru/docs/(\d+)[^"]*)"[^>]*>(.*?)</a>',
            html, re.DOTALL
        ):
            doc_id = m.group(2)
            if doc_id in seen:
                continue
            seen.add(doc_id)
            results.append({
                'doc_id': doc_id,
                'title': strip_html(m.group(3)).strip(),
                'href': m.group(1),
            })

        dates = re.findall(r'dd-table__main-item_date[^>]*>([^<]+)<', html)
        for i, r in enumerate(results):
            if i < len(dates):
                raw_date = dates[i].strip()
                try:
                    r['date'] = datetime.strptime(raw_date, '%d.%m.%Y').strftime('%Y-%m-%d')
                except ValueError:
                    r['date'] = raw_date

        return results

    def search_legislation(self, date_from: str = "", date_to: str = "",
                           lang: str = "", form_id: str = "",
                           max_pages: int = 0) -> Iterator[Dict[str, str]]:
        """Search for legislation and yield result stubs across all pages.

        `lang` is left empty by default: filtering to lang=1 (Russian) drops ~72% of
        the corpus (2025: 65 pages filtered vs 234 unfiltered).
        """
        params = {}
        if date_from:
            params['from'] = date_from
        if date_to:
            params['to'] = date_to
        if lang:
            params['lang'] = lang
        if form_id:
            params['form_id'] = form_id

        url = SEARCH_URL + ('?' + urllib.parse.urlencode(params) if params else '')

        logger.info(f"Searching: {url}")
        html = self._curl_get(url)
        if not html:
            logger.error(f"Failed to load search page: {url}")
            return

        page = 1
        while True:
            results = self._parse_search_doc_ids(html)
            if not results:
                logger.info(f"No results on page {page}, stopping")
                break

            logger.info(f"Page {page}: {len(results)} results")
            for r in results:
                yield r

            if max_pages and page >= max_pages:
                logger.info(f"Reached max pages ({max_pages})")
                break

            if not self._has_next_page(html):
                logger.info(f"Last page reached ({page})")
                break

            page += 1
            time.sleep(self.page_delay)
            nxt = self._postback(url, html, NEXT_PAGE_TARGET)
            if not nxt:
                logger.warning(f"Failed to load page {page}; stopping this window")
                break
            html = nxt

    # -------------------------------------------------------------- document

    def fetch_document(self, doc_id: str) -> Optional[Dict[str, Any]]:
        """Fetch a single document with full text."""
        url = DOC_URL.format(doc_id)
        html = self._curl_get(url)
        if not html:
            return None

        content = parse_document_content(html)
        if not content or not content.get('text'):
            logger.warning(f"Doc {doc_id}: no text extracted")
            return None

        title_meta = parse_title_meta(html)

        return {
            'doc_id': doc_id,
            'title': content.get('title') or title_meta.get('title_from_meta', ''),
            'text': content['text'],
            'date': title_meta.get('date', ''),
            'doc_number': title_meta.get('doc_number', ''),
            'doc_type': content.get('doc_type', ''),
            'url': url,
        }

    # ----------------------------------------------------------- checkpoints

    @staticmethod
    def _load_checkpoint() -> Dict[str, Any]:
        if CHECKPOINT_PATH.exists():
            try:
                with open(CHECKPOINT_PATH, encoding='utf-8') as fh:
                    cp = json.load(fh)
                cp.setdefault('completed_years', [])
                cp.setdefault('seen_ids', [])
                return cp
            except (json.JSONDecodeError, OSError) as exc:
                logger.warning(f"Ignoring unreadable checkpoint: {exc}")
        return {'completed_years': [], 'seen_ids': []}

    @staticmethod
    def _save_checkpoint(completed_years: List[int], seen: Set[str]) -> None:
        DATA_DIR.mkdir(exist_ok=True)
        tmp = CHECKPOINT_PATH.with_suffix('.tmp')
        with open(tmp, 'w', encoding='utf-8') as fh:
            json.dump({'completed_years': sorted(completed_years),
                       'seen_ids': sorted(seen)}, fh)
        tmp.replace(CHECKPOINT_PATH)

    # ------------------------------------------------------------- crawlers

    def fetch_all(self, first_year: int = FIRST_YEAR,
                  last_year: Optional[int] = None) -> Iterator[Dict[str, Any]]:
        """Fetch every act, one publication year at a time, resuming from checkpoint.

        Completed years are skipped with no network calls; within a partially-crawled
        year the page walk is replayed (cheap) but already-fetched documents are
        skipped (expensive part), so restarts advance monotonically.
        """
        last_year = last_year or datetime.now().year
        cp = self._load_checkpoint()
        completed = set(cp['completed_years'])
        seen: Set[str] = set(cp['seen_ids'])
        if completed:
            logger.info(f"Resuming: {len(completed)} years done, {len(seen)} docs seen")

        count = 0
        for year in range(last_year, first_year - 1, -1):
            if year in completed:
                logger.info(f"Year {year}: already complete, skipping")
                continue

            logger.info(f"=== Year {year} ===")
            year_new = 0
            for result in self.search_legislation(
                date_from=f'01.01.{year}', date_to=f'31.12.{year}'
            ):
                doc_id = result.get('doc_id')
                if not doc_id or doc_id in seen:
                    continue
                doc = self.fetch_document(doc_id)
                seen.add(doc_id)
                if doc:
                    if not doc.get('date') and result.get('date'):
                        doc['date'] = result['date']
                    yield doc
                    count += 1
                    year_new += 1
                # Flush on documents *visited*, not documents yielded: a year full of
                # Russian-shell records (body published Uzbek-only) yields little but
                # still costs a fetch each, and re-walking it on restart is wasteful.
                if len(seen) % 50 == 0:
                    self._save_checkpoint(sorted(completed), seen)
                time.sleep(self.doc_delay)

            completed.add(year)
            self._save_checkpoint(sorted(completed), seen)
            logger.info(f"Year {year} complete: {year_new} new documents "
                        f"({count} this run)")

        logger.info(f"Fetched {count} documents total")

    def fetch_updates(self, since: datetime) -> Iterator[Dict[str, Any]]:
        """Fetch documents published since a given date."""
        since_str = since.strftime('%d.%m.%Y')
        today_str = datetime.now().strftime('%d.%m.%Y')
        count = 0
        for result in self.search_legislation(date_from=since_str, date_to=today_str):
            doc_id = result.get('doc_id')
            if not doc_id:
                continue
            doc = self.fetch_document(doc_id)
            if doc:
                if not doc.get('date') and result.get('date'):
                    doc['date'] = result['date']
                yield doc
                count += 1
            time.sleep(self.doc_delay)
        logger.info(f"Fetched {count} updated documents since {since_str}")

    def normalize(self, raw_doc: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize document to standard schema."""
        return {
            '_id': str(raw_doc.get('doc_id', '')),
            '_source': 'UZ/LexUz',
            '_type': 'legislation',
            '_fetched_at': datetime.now().isoformat(),
            'title': raw_doc.get('title', ''),
            'text': raw_doc.get('text', ''),
            'date': raw_doc.get('date', ''),
            'doc_number': raw_doc.get('doc_number', ''),
            'doc_type': raw_doc.get('doc_type', ''),
            'url': raw_doc.get('url', ''),
        }


def bootstrap_full(slow_mode: bool = False):
    """Stream the whole corpus to data/records.jsonl (fleet path)."""
    DATA_DIR.mkdir(exist_ok=True)
    fetcher = LexUzFetcher(slow_mode=slow_mode)

    written = 0
    with open(RECORDS_PATH, 'a', encoding='utf-8') as out:
        for doc in fetcher.fetch_all():
            normalized = fetcher.normalize(doc)
            if not normalized.get('text') or len(normalized['text']) < 100:
                continue
            out.write(json.dumps(normalized, ensure_ascii=False) + '\n')
            out.flush()
            written += 1
            if written % 100 == 0:
                logger.info(f"[+] {written} records written to {RECORDS_PATH}")

    logger.info(f"bootstrap complete: {written} records written to {RECORDS_PATH}")
    if written == 0:
        logger.error("No records written!")
        sys.exit(1)


def bootstrap_sample(slow_mode: bool = False):
    """Fetch a sample of documents for testing."""
    sample_dir = Path(__file__).parent / 'sample'
    sample_dir.mkdir(exist_ok=True)

    for f in sample_dir.glob('*.json'):
        f.unlink()

    fetcher = LexUzFetcher(slow_mode=slow_mode)

    count = 0
    target = 15

    for result in fetcher.search_legislation(
        date_from='01.01.2024', date_to='31.12.2025', max_pages=10,
    ):
        if count >= target:
            break

        doc_id = result.get('doc_id')
        if not doc_id:
            continue

        logger.info(f"[{count+1}/{target}] Fetching doc {doc_id}: "
                    f"{result.get('title', '')[:60]}...")
        doc = fetcher.fetch_document(doc_id)
        if not doc:
            logger.warning(f"Skipping doc {doc_id} - no content")
            continue

        normalized = fetcher.normalize(doc)

        if not normalized.get('text') or len(normalized['text']) < 100:
            logger.warning(f"Skipping doc {doc_id} - text too short "
                           f"({len(normalized.get('text', ''))} chars)")
            continue

        out_path = sample_dir / f"{doc_id}.json"
        with open(out_path, 'w', encoding='utf-8') as f:
            json.dump(normalized, f, ensure_ascii=False, indent=2)

        count += 1
        logger.info(f"  Saved {out_path.name} ({len(normalized['text'])} chars)")
        time.sleep(fetcher.doc_delay)

    logger.info(f"\nSample complete: {count} documents saved to {sample_dir}/")
    validate_sample(sample_dir)


def validate_sample(sample_dir: Path):
    """Validate sample data quality."""
    files = list(sample_dir.glob('*.json'))
    if not files:
        logger.error("No sample files found!")
        return

    total = len(files)
    has_text = 0
    has_title = 0
    has_date = 0
    text_lengths = []

    for f in files:
        with open(f, 'r', encoding='utf-8') as fh:
            doc = json.load(fh)
        if doc.get('text') and len(doc['text']) > 50:
            has_text += 1
            text_lengths.append(len(doc['text']))
        if doc.get('title'):
            has_title += 1
        if doc.get('date'):
            has_date += 1

    logger.info(f"\n=== VALIDATION SUMMARY ===")
    logger.info(f"Total samples: {total}")
    logger.info(f"With full text: {has_text}/{total}")
    logger.info(f"With title: {has_title}/{total}")
    logger.info(f"With date: {has_date}/{total}")
    if text_lengths:
        avg_len = sum(text_lengths) // len(text_lengths)
        logger.info(f"Text length: min={min(text_lengths)}, avg={avg_len}, max={max(text_lengths)}")

    if has_text < total:
        logger.warning(f"WARNING: {total - has_text} documents missing full text!")
    if total >= 10 and has_text >= 10:
        logger.info("PASS: 10+ documents with full text")
    else:
        logger.warning(f"FAIL: Need 10+ docs with text, got {has_text}")


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Uzbekistan LexUz Legislation Fetcher')
    parser.add_argument('command', choices=['bootstrap', 'bootstrap-fast', 'validate'],
                        help='Command to run')
    parser.add_argument('--sample', action='store_true',
                        help='Fetch sample data only')
    parser.add_argument('--slow', action='store_true',
                        help='Use slower rate limiting')
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    if args.command in ('bootstrap', 'bootstrap-fast'):
        if args.sample:
            bootstrap_sample(slow_mode=args.slow)
        else:
            bootstrap_full(slow_mode=args.slow)
    elif args.command == 'validate':
        sample_dir = Path(__file__).parent / 'sample'
        validate_sample(sample_dir)
