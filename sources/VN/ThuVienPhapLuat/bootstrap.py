#!/usr/bin/env python3
"""
Vietnamese Legal Library (Thu Vien Phap Luat) Data Fetcher

518,255 Vietnamese legal documents from thuvienphapluat.vn, accessed via
HuggingFace dataset th1nhng0/vietnamese-legal-documents (CC-BY-4.0).

Two configs: 'metadata' (id, title, url, legal_type, etc.) and
'content' (id, full text). Joined by integer id field.
"""

import json
import logging
import os
import sqlite3
import subprocess
import sys
import tempfile
import time
import urllib.parse
import urllib.request
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Iterator, Optional, List

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

DATASET = "th1nhng0/vietnamese-legal-documents"
HF_ROWS_API = "https://datasets-server.huggingface.co/rows"
HF_BASE = "https://huggingface.co/datasets"


def hf_fetch_rows(config: str, split: str = "data", offset: int = 0,
                  length: int = 100) -> List[Dict[str, Any]]:
    """Fetch rows from HuggingFace datasets-server API."""
    params = {
        'dataset': DATASET,
        'config': config,
        'split': split,
        'offset': str(offset),
        'length': str(length),
    }
    url = HF_ROWS_API + '?' + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers={
        'User-Agent': 'LegalDataHunter/1.0',
    })
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            data = json.loads(resp.read().decode('utf-8'))
        if 'error' in data:
            logger.warning(f"HF API error: {data['error'][:200]}")
            return []
        return [r['row'] for r in data.get('rows', [])]
    except Exception as e:
        logger.error(f"HF API request failed: {e}")
        return []


class ThuVienPhapLuatFetcher:
    """Fetcher for Vietnamese legislation via HuggingFace dataset."""

    def __init__(self):
        self.delay = 1.0

    def fetch_metadata_batch(self, offset: int = 0, length: int = 100) -> List[Dict]:
        """Fetch a batch of metadata records."""
        return hf_fetch_rows('metadata', offset=offset, length=length)

    def fetch_content_batch(self, offset: int = 0, length: int = 100) -> List[Dict]:
        """Fetch a batch of content records.

        Note: The content config has large row groups in early shards.
        Offsets >= 500000 (shard 10) work reliably via the rows API.
        For full fetch, use datasets library streaming instead.
        """
        return hf_fetch_rows('content', offset=offset, length=length)

    def fetch_joined_batch(self, offset: int = 0, length: int = 15) -> List[Dict]:
        """Fetch metadata and content, join by id.

        For sampling: metadata is fetchable at any offset, content only from
        offset >= 500000 via rows API. We fetch both at a matching offset.
        """
        # Content API works from offset 500000+ (smaller shard)
        content_offset = max(offset, 500000)

        content_rows = self.fetch_content_batch(offset=content_offset, length=length)
        if not content_rows:
            logger.error("Failed to fetch content")
            return []

        content_by_id = {r['id']: r['content'] for r in content_rows}
        content_ids = list(content_by_id.keys())

        # Now fetch metadata for these same IDs
        # The metadata and content are in the same order, so same offset works
        meta_rows = self.fetch_metadata_batch(offset=content_offset, length=length)
        meta_by_id = {r['id']: r for r in meta_rows}

        joined = []
        for cid in content_ids:
            meta = meta_by_id.get(cid, {})
            text = content_by_id[cid]
            if not text or len(text) < 50:
                continue
            joined.append({
                'id': cid,
                'title': meta.get('title', ''),
                'text': text,
                'document_number': meta.get('document_number', ''),
                'url': meta.get('url', ''),
                'legal_type': meta.get('legal_type', ''),
                'legal_sectors': meta.get('legal_sectors', ''),
                'issuing_authority': meta.get('issuing_authority', ''),
                'issuance_date': meta.get('issuance_date', ''),
                'signers': meta.get('signers', ''),
            })

        return joined

    def fetch_all(self) -> Iterator[Dict[str, Any]]:
        """Fetch all documents using datasets library streaming.

        Uses SQLite temp file for metadata cache to avoid OOM on low-memory VPS.
        """
        try:
            from datasets import load_dataset
        except ImportError:
            logger.error("datasets library required for full fetch. pip install datasets")
            return

        # Use SQLite on disk instead of in-memory dict to avoid OOM
        db_path = os.path.join(tempfile.gettempdir(), 'vn_tvpl_meta.db')
        db = sqlite3.connect(db_path)
        db.execute('PRAGMA journal_mode=WAL')
        db.execute('''CREATE TABLE IF NOT EXISTS meta (
            id INTEGER PRIMARY KEY,
            title TEXT, document_number TEXT, url TEXT,
            legal_type TEXT, legal_sectors TEXT,
            issuing_authority TEXT, issuance_date TEXT, signers TEXT
        )''')
        db.execute('DELETE FROM meta')
        db.commit()

        logger.info("Streaming metadata into SQLite cache...")
        meta_ds = load_dataset(DATASET, 'metadata', split='data', streaming=True)
        batch = []
        meta_count = 0
        for row in meta_ds:
            batch.append((
                row['id'], row.get('title', ''), row.get('document_number', ''),
                row.get('url', ''), row.get('legal_type', ''),
                row.get('legal_sectors', ''), row.get('issuing_authority', ''),
                row.get('issuance_date', ''), row.get('signers', ''),
            ))
            if len(batch) >= 5000:
                db.executemany('INSERT OR REPLACE INTO meta VALUES (?,?,?,?,?,?,?,?,?)', batch)
                db.commit()
                meta_count += len(batch)
                batch = []
                if meta_count % 100000 == 0:
                    logger.info(f"Cached {meta_count} metadata records...")
        if batch:
            db.executemany('INSERT OR REPLACE INTO meta VALUES (?,?,?,?,?,?,?,?,?)', batch)
            db.commit()
            meta_count += len(batch)

        logger.info(f"Cached {meta_count} metadata records in SQLite")
        logger.info("Streaming content...")

        content_ds = load_dataset(DATASET, 'content', split='data', streaming=True)
        count = 0
        for row in content_ds:
            doc_id = row['id']
            text = row.get('content', '')
            if not text or len(text) < 50:
                continue

            cur = db.execute('SELECT title, document_number, url, legal_type, legal_sectors, issuing_authority, issuance_date, signers FROM meta WHERE id=?', (doc_id,))
            meta_row = cur.fetchone()
            if meta_row:
                title, doc_num, url, ltype, lsectors, authority, idate, signers = meta_row
            else:
                title = doc_num = url = ltype = lsectors = authority = idate = signers = ''

            yield {
                'id': doc_id,
                'title': title,
                'text': text,
                'document_number': doc_num,
                'url': url,
                'legal_type': ltype,
                'legal_sectors': lsectors,
                'issuing_authority': authority,
                'issuance_date': idate,
                'signers': signers,
            }
            count += 1
            if count % 10000 == 0:
                logger.info(f"Processed {count} documents...")

        db.close()
        try:
            os.unlink(db_path)
        except OSError:
            pass
        logger.info(f"Fetched {count} documents total")

    def fetch_updates(self, since: datetime) -> Iterator[Dict[str, Any]]:
        """Fetch documents published since a given date.

        Filters by issuance_date from the full dataset stream.
        """
        since_str = since.strftime('%Y-%m-%d')
        for doc in self.fetch_all():
            date = doc.get('issuance_date', '')
            if date and date >= since_str:
                yield doc

    def normalize(self, raw_doc: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize document to standard schema."""
        # Parse date to ISO format
        date = raw_doc.get('issuance_date', '')
        if date:
            # Try dd/mm/yyyy format
            for fmt in ('%d/%m/%Y', '%Y-%m-%d', '%d-%m-%Y'):
                try:
                    dt = datetime.strptime(date, fmt)
                    date = dt.strftime('%Y-%m-%d')
                    break
                except ValueError:
                    continue

        return {
            '_id': str(raw_doc.get('id', '')),
            '_source': 'VN/ThuVienPhapLuat',
            '_type': 'legislation',
            '_fetched_at': datetime.now().isoformat(),
            'title': raw_doc.get('title', ''),
            'text': raw_doc.get('text', ''),
            'date': date,
            'document_number': raw_doc.get('document_number', ''),
            'legal_type': raw_doc.get('legal_type', ''),
            'legal_sectors': raw_doc.get('legal_sectors', ''),
            'issuing_authority': raw_doc.get('issuing_authority', ''),
            'signers': raw_doc.get('signers', ''),
            'url': raw_doc.get('url', ''),
        }


def bootstrap_sample():
    """Fetch a sample of documents for testing."""
    sample_dir = Path(__file__).parent / 'sample'
    sample_dir.mkdir(exist_ok=True)

    # Clear old samples
    for f in sample_dir.glob('*.json'):
        f.unlink()

    fetcher = ThuVienPhapLuatFetcher()

    logger.info("Fetching sample from HuggingFace dataset...")
    docs = fetcher.fetch_joined_batch(offset=500000, length=20)

    if not docs:
        logger.error("Failed to fetch any documents")
        sys.exit(1)

    count = 0
    for doc in docs:
        if count >= 15:
            break

        normalized = fetcher.normalize(doc)
        if not normalized.get('text') or len(normalized['text']) < 100:
            continue

        out_path = sample_dir / f"{normalized['_id']}.json"
        with open(out_path, 'w', encoding='utf-8') as f:
            json.dump(normalized, f, ensure_ascii=False, indent=2)

        count += 1
        logger.info(f"[{count}/15] Saved {out_path.name} ({len(normalized['text'])} chars)")

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
    parser = argparse.ArgumentParser(description='Vietnamese Legal Library Fetcher')
    parser.add_argument('command', choices=['bootstrap', 'validate'],
                        help='Command to run')
    parser.add_argument('--sample', action='store_true',
                        help='Fetch sample data only')
    args = parser.parse_args()

    if args.command == 'bootstrap':
        if args.sample:
            bootstrap_sample()
        else:
            logger.info("Full fetch not implemented in bootstrap mode. Use --sample.")
    elif args.command == 'validate':
        sample_dir = Path(__file__).parent / 'sample'
        validate_sample(sample_dir)
