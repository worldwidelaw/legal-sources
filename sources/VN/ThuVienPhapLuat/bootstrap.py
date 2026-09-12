#!/usr/bin/env python3
"""
Vietnamese Legal Library (Thu Vien Phap Luat) Data Fetcher

Vietnamese legal documents from thuvienphapluat.vn, accessed via the
HuggingFace dataset th1nhng0/vietnamese-legal-documents (CC-BY-4.0).

Dataset layout (as republished by the maintainer):
  - config 'metadata' / split 'data'  -> 171,556 rows, Vietnamese column names,
    string ids (some non-numeric, e.g. 'vbpqta_2709')
  - config 'content'  / split 'data'  -> 170,824 rows, columns (id, content_html)
  - configs 'legacy_metadata' / 'legacy_content' are the pre-restructure dumps;
    legacy_content currently 500s server-side, so we do not use them.

Metadata and content are joined on the string `id`.
"""

import argparse
import html as html_lib
import json
import logging
import os
import re
import sqlite3
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Iterator, List, Optional

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

DATASET = "th1nhng0/vietnamese-legal-documents"
HF_ROWS_API = "https://datasets-server.huggingface.co/rows"
HF_FILTER_API = "https://datasets-server.huggingface.co/filter"
DATASET_URL = f"https://huggingface.co/datasets/{DATASET}"
SEARCH_URL = "https://thuvienphapluat.vn/page/tim-van-ban.aspx?keyword="

MIN_TEXT_LEN = 100

# Vietnamese metadata column -> normalized field name
META_FIELDS = {
    'title': 'title',
    'so_ky_hieu': 'document_number',
    'ngay_ban_hanh': 'issuance_date',
    'loai_van_ban': 'legal_type',
    'ngay_co_hieu_luc': 'effect_date',
    'ngay_het_hieu_luc': 'effectless_date',
    'nganh': 'legal_sectors',
    'linh_vuc': 'legal_field',
    'co_quan_ban_hanh': 'issuing_authority',
    'chuc_danh': 'signer_title',
    'nguoi_ky': 'signers',
    'pham_vi': 'scope',
    'tinh_trang_hieu_luc': 'effect_status',
}
META_COLUMNS = list(META_FIELDS.values())

_SCRIPT_RE = re.compile(r'<(script|style|head)\b[^>]*>.*?</\1>', re.I | re.S)
_BLOCK_RE = re.compile(r'</?(p|div|br|tr|li|h[1-6]|table|section)\b[^>]*>', re.I)
_TAG_RE = re.compile(r'<[^>]+>')
_WS_RE = re.compile(r'[ \t ]+')
_NL_RE = re.compile(r'\n{3,}')


def html_to_text(raw: Optional[str]) -> str:
    """Strip HTML markup to readable plain text (no external dependencies)."""
    if not raw:
        return ''
    text = _SCRIPT_RE.sub(' ', raw)
    text = _BLOCK_RE.sub('\n', text)
    text = _TAG_RE.sub(' ', text)
    text = html_lib.unescape(text)
    text = text.replace(' ', ' ').replace('\r', '')
    text = _WS_RE.sub(' ', text)
    text = '\n'.join(line.strip() for line in text.split('\n'))
    text = _NL_RE.sub('\n\n', text)
    return text.strip()


def _s(value: Any) -> str:
    """Coerce any dataset value to a plain string safe for SQLite TEXT binding."""
    if value is None:
        return ''
    if isinstance(value, str):
        return value
    if isinstance(value, (list, tuple)):
        return '; '.join(_s(v) for v in value if v is not None)
    if isinstance(value, dict):
        return json.dumps(value, ensure_ascii=False)
    return str(value)


def _http_json(url: str, timeout: int = 120, retries: int = 4) -> Dict[str, Any]:
    """GET a JSON document with retry/backoff on throttling and transient errors."""
    req = urllib.request.Request(url, headers={'User-Agent': 'LegalDataHunter/1.0'})
    last_err = None
    for attempt in range(retries):
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return json.loads(resp.read().decode('utf-8'))
        except urllib.error.HTTPError as e:
            last_err = e
            if e.code in (429, 500, 502, 503, 504):
                wait = 5 * (attempt + 1)
                logger.warning(f"HTTP {e.code} from HF API, retrying in {wait}s")
                time.sleep(wait)
                continue
            raise
        except Exception as e:  # timeouts, connection resets
            last_err = e
            time.sleep(3 * (attempt + 1))
    raise RuntimeError(f"HF API request failed after {retries} attempts: {last_err}")


def hf_fetch_rows(config: str, split: str = "data", offset: int = 0,
                  length: int = 100) -> List[Dict[str, Any]]:
    """Fetch rows from the HuggingFace datasets-server API."""
    params = {
        'dataset': DATASET, 'config': config, 'split': split,
        'offset': str(offset), 'length': str(length),
    }
    data = _http_json(HF_ROWS_API + '?' + urllib.parse.urlencode(params))
    if 'error' in data:
        logger.warning(f"HF API error: {str(data['error'])[:200]}")
        return []
    return [r['row'] for r in data.get('rows', [])]


def hf_filter_rows(config: str, where: str, split: str = "data",
                   length: int = 10) -> List[Dict[str, Any]]:
    """Fetch rows matching a SQL-ish WHERE clause via the datasets-server filter API."""
    params = {
        'dataset': DATASET, 'config': config, 'split': split,
        'where': where, 'offset': '0', 'length': str(length),
    }
    data = _http_json(HF_FILTER_API + '?' + urllib.parse.urlencode(params))
    if 'error' in data:
        logger.warning(f"HF filter error: {str(data['error'])[:200]}")
        return []
    return [r['row'] for r in data.get('rows', [])]


class ThuVienPhapLuatFetcher:
    """Fetcher for Vietnamese legislation via the HuggingFace dataset."""

    def __init__(self):
        self.delay = 1.0

    # ------------------------------------------------------------------ #
    # Sampling                                                            #
    # ------------------------------------------------------------------ #

    def fetch_joined_batch(self, offset: int = 0, length: int = 20) -> List[Dict[str, Any]]:
        """Fetch a batch of content rows and join each with its metadata row.

        The two configs are NOT row-aligned (content offset 0 starts at id
        132934, metadata offset 0 at id 8733), so metadata is looked up per id
        through the datasets-server filter endpoint.
        """
        content_rows = hf_fetch_rows('content', offset=offset, length=length)
        if not content_rows:
            logger.error("Failed to fetch content rows")
            return []

        joined = []
        for row in content_rows:
            doc_id = _s(row.get('id'))
            text = html_to_text(row.get('content_html'))
            if not doc_id or len(text) < MIN_TEXT_LEN:
                continue
            meta_rows = hf_filter_rows('metadata', where=f'"id"=\'{doc_id}\'', length=1)
            meta = meta_rows[0] if meta_rows else {}
            joined.append(self._build(doc_id, text, meta))
            time.sleep(self.delay)
        return joined

    @staticmethod
    def _build(doc_id: str, text: str, meta: Dict[str, Any]) -> Dict[str, Any]:
        doc = {'id': doc_id, 'text': text}
        for src, dst in META_FIELDS.items():
            doc[dst] = _s(meta.get(src))
        return doc

    # ------------------------------------------------------------------ #
    # Full corpus                                                         #
    # ------------------------------------------------------------------ #

    def fetch_all(self) -> Iterator[Dict[str, Any]]:
        """Stream the whole corpus.

        Metadata is cached in an on-disk SQLite table (all columns TEXT — dataset
        ids are strings and some are non-numeric, e.g. 'vbpqta_2709') so the
        content stream can be joined without holding 170K rows in memory.
        """
        try:
            from datasets import load_dataset
        except ImportError:
            logger.warning("datasets library unavailable; falling back to the rows API")
            yield from self._fetch_all_via_api()
            return

        db_path = os.path.join(tempfile.gettempdir(), 'vn_tvpl_meta.db')
        db = sqlite3.connect(db_path)
        db.execute('PRAGMA journal_mode=WAL')
        columns = ', '.join(f'{c} TEXT' for c in META_COLUMNS)
        db.execute(f'CREATE TABLE IF NOT EXISTS meta (id TEXT PRIMARY KEY, {columns})')
        db.execute('DELETE FROM meta')
        db.commit()

        placeholders = ','.join('?' * (len(META_COLUMNS) + 1))
        insert_sql = f'INSERT OR REPLACE INTO meta VALUES ({placeholders})'
        select_sql = f'SELECT {", ".join(META_COLUMNS)} FROM meta WHERE id=?'

        logger.info("Streaming metadata into SQLite cache...")
        meta_ds = load_dataset(DATASET, 'metadata', split='data', streaming=True)
        batch, meta_count = [], 0
        for row in meta_ds:
            batch.append(tuple([_s(row.get('id'))] + [_s(row.get(c)) for c in META_FIELDS]))
            if len(batch) >= 5000:
                db.executemany(insert_sql, batch)
                db.commit()
                meta_count += len(batch)
                batch = []
                if meta_count % 50000 == 0:
                    logger.info(f"Cached {meta_count} metadata records...")
        if batch:
            db.executemany(insert_sql, batch)
            db.commit()
            meta_count += len(batch)
        logger.info(f"Cached {meta_count} metadata records in SQLite")

        logger.info("Streaming content...")
        content_ds = load_dataset(DATASET, 'content', split='data', streaming=True)
        count = 0
        for row in content_ds:
            doc_id = _s(row.get('id'))
            text = html_to_text(row.get('content_html'))
            if not doc_id or len(text) < MIN_TEXT_LEN:
                continue
            meta_row = db.execute(select_sql, (doc_id,)).fetchone()
            meta = dict(zip(META_FIELDS.keys(), meta_row)) if meta_row else {}
            yield self._build(doc_id, text, meta)
            count += 1
            if count % 10000 == 0:
                logger.info(f"Processed {count} documents...")

        db.close()
        try:
            os.unlink(db_path)
        except OSError:
            pass
        logger.info(f"Fetched {count} documents total")

    def _fetch_all_via_api(self, page: int = 100) -> Iterator[Dict[str, Any]]:
        """Fallback full fetch: page both configs through the rows API."""
        logger.info("Building metadata index from the rows API...")
        meta_index: Dict[str, Dict[str, Any]] = {}
        offset = 0
        while True:
            rows = hf_fetch_rows('metadata', offset=offset, length=page)
            if not rows:
                break
            for row in rows:
                meta_index[_s(row.get('id'))] = row
            offset += len(rows)
            if len(rows) < page:
                break
        logger.info(f"Indexed {len(meta_index)} metadata records")

        offset, count = 0, 0
        while True:
            rows = hf_fetch_rows('content', offset=offset, length=page)
            if not rows:
                break
            for row in rows:
                doc_id = _s(row.get('id'))
                text = html_to_text(row.get('content_html'))
                if not doc_id or len(text) < MIN_TEXT_LEN:
                    continue
                yield self._build(doc_id, text, meta_index.get(doc_id, {}))
                count += 1
            offset += len(rows)
            if len(rows) < page:
                break
            if count % 10000 < page:
                logger.info(f"Processed {count} documents...")
        logger.info(f"Fetched {count} documents total")

    def fetch_updates(self, since: datetime) -> Iterator[Dict[str, Any]]:
        """Fetch documents issued since a given date."""
        since_str = since.strftime('%Y-%m-%d')
        for doc in self.fetch_all():
            if parse_date(doc.get('issuance_date', '')) >= since_str:
                yield doc

    # ------------------------------------------------------------------ #

    def normalize(self, raw_doc: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize a document to the standard schema."""
        doc_number = raw_doc.get('document_number', '')
        url = SEARCH_URL + urllib.parse.quote(doc_number) if doc_number else DATASET_URL
        return {
            '_id': _s(raw_doc.get('id')),
            '_source': 'VN/ThuVienPhapLuat',
            '_type': 'legislation',
            '_fetched_at': datetime.now().isoformat(),
            'title': raw_doc.get('title', ''),
            'text': raw_doc.get('text', ''),
            'date': parse_date(raw_doc.get('issuance_date', '')),
            'document_number': doc_number,
            'legal_type': raw_doc.get('legal_type', ''),
            'legal_sectors': raw_doc.get('legal_sectors', ''),
            'legal_field': raw_doc.get('legal_field', ''),
            'issuing_authority': raw_doc.get('issuing_authority', ''),
            'signers': raw_doc.get('signers', ''),
            'signer_title': raw_doc.get('signer_title', ''),
            'scope': raw_doc.get('scope', ''),
            'effect_date': parse_date(raw_doc.get('effect_date', '')),
            'effectless_date': parse_date(raw_doc.get('effectless_date', '')),
            'effect_status': raw_doc.get('effect_status', ''),
            'url': url,
        }


def parse_date(value: Optional[str]) -> str:
    """Normalize a dataset date (dd/mm/yyyy or ISO) to ISO 8601, else ''."""
    value = _s(value).strip()
    if not value:
        return ''
    for fmt in ('%d/%m/%Y', '%Y-%m-%d', '%d-%m-%Y', '%Y-%m-%dT%H:%M:%S'):
        try:
            return datetime.strptime(value, fmt).strftime('%Y-%m-%d')
        except ValueError:
            continue
    return ''


def bootstrap_sample(sample_size: int = 15):
    """Fetch a sample of documents for validation."""
    sample_dir = Path(__file__).parent / 'sample'
    sample_dir.mkdir(exist_ok=True)
    for f in sample_dir.glob('*.json'):
        f.unlink()

    fetcher = ThuVienPhapLuatFetcher()
    logger.info("Fetching sample from the HuggingFace dataset...")

    count = 0
    offset = 0
    while count < sample_size and offset < 200:
        for doc in fetcher.fetch_joined_batch(offset=offset, length=sample_size + 5):
            if count >= sample_size:
                break
            normalized = fetcher.normalize(doc)
            if len(normalized['text']) < MIN_TEXT_LEN:
                continue
            out_path = sample_dir / f"{normalized['_id']}.json"
            with open(out_path, 'w', encoding='utf-8') as f:
                json.dump(normalized, f, ensure_ascii=False, indent=2)
            count += 1
            logger.info(f"[{count}/{sample_size}] Saved {out_path.name} "
                        f"({len(normalized['text'])} chars)")
        offset += sample_size + 5

    if not count:
        logger.error("Failed to fetch any documents")
        sys.exit(1)

    logger.info(f"\nSample complete: {count} documents saved to {sample_dir}/")
    validate_sample(sample_dir)


def bootstrap_full():
    """Stream the full corpus to data/records.jsonl (used by the fleet runner)."""
    out_dir = Path(__file__).parent / 'data'
    out_dir.mkdir(exist_ok=True)
    out_path = out_dir / 'records.jsonl'

    fetcher = ThuVienPhapLuatFetcher()
    written = 0
    with open(out_path, 'w', encoding='utf-8') as fh:
        for doc in fetcher.fetch_all():
            normalized = fetcher.normalize(doc)
            if len(normalized['text']) < MIN_TEXT_LEN:
                continue
            fh.write(json.dumps(normalized, ensure_ascii=False) + '\n')
            written += 1
    logger.info(f"bootstrap_fast complete: {written} written -> {out_path}")


def validate_sample(sample_dir: Path):
    """Validate sample data quality."""
    files = list(sample_dir.glob('*.json'))
    if not files:
        logger.error("No sample files found!")
        return

    total = len(files)
    has_text = has_title = has_date = 0
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

    logger.info("\n=== VALIDATION SUMMARY ===")
    logger.info(f"Total samples: {total}")
    logger.info(f"With full text: {has_text}/{total}")
    logger.info(f"With title: {has_title}/{total}")
    logger.info(f"With date: {has_date}/{total}")
    if text_lengths:
        logger.info(f"Text length: min={min(text_lengths)}, "
                    f"avg={sum(text_lengths) // len(text_lengths)}, max={max(text_lengths)}")

    if has_text < total:
        logger.warning(f"WARNING: {total - has_text} documents missing full text!")
    if total >= 10 and has_text >= 10:
        logger.info("PASS: 10+ documents with full text")
    else:
        logger.warning(f"FAIL: Need 10+ docs with text, got {has_text}")


def main():
    parser = argparse.ArgumentParser(description='Vietnamese Legal Library Fetcher')
    parser.add_argument('command', choices=['bootstrap', 'bootstrap-fast', 'update', 'validate'])
    parser.add_argument('--sample', action='store_true', help='Fetch sample data only')
    parser.add_argument('--full', action='store_true', help='Fetch all records')
    args = parser.parse_args()

    if args.command == 'validate':
        validate_sample(Path(__file__).parent / 'sample')
    elif args.command == 'bootstrap' and args.sample and not args.full:
        bootstrap_sample()
    else:
        bootstrap_full()


if __name__ == '__main__':
    main()
