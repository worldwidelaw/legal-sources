#!/usr/bin/env python3
"""
Czech e-Sbírka (Collection of Laws) Data Fetcher

Official open data from the Czech Ministry of Interior
https://www.e-sbirka.cz / https://zakony.gov.cz

This fetcher uses bulk download files from opendata.eselpoint.cz:
- 002PravniAkt.json.gz: Legal act metadata (5MB)
- 004PravniAktFragment.json.gz: Text fragments with content (503MB)

The text fragments are assembled to create complete document text.

NOTE: Initial download of the fragments file takes 5-10 minutes.
Files are cached locally after first download.
"""

import gc
import gzip
import json
import logging
import os
import re
import sys
import time
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Iterator, Optional

import requests

# Check for ijson at import time - required for this scraper
try:
    import ijson
except ImportError:
    print("ERROR: ijson is required for CZ/eSbirka (500MB+ fragment file)")
    print("Install it with: pip install ijson>=3.2.0")
    print("Without ijson, parsing the fragment file would cause OOM.")
    sys.exit(1)

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Constants
OPENDATA_BASE = "https://opendata.eselpoint.cz/datove-sady-esbirka"
ACTS_URL = f"{OPENDATA_BASE}/002PravniAkt.json.gz"
FRAGMENTS_URL = f"{OPENDATA_BASE}/004PravniAktFragment.json.gz"

# Cache directory for downloaded files
CACHE_DIR = Path(__file__).parent / '.cache'


class ESbirkaFetcher:
    """Fetcher for Czech e-Sbírka legislation data"""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'WorldWideLaw/1.0 (https://github.com/worldwidelaw/legal-sources)'
        })
        # Disable SSL verification (their cert sometimes has issues)
        self.session.verify = False
        # Suppress InsecureRequestWarning
        import urllib3
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

        # Ensure cache directory exists
        CACHE_DIR.mkdir(exist_ok=True)

        # Cache for loaded data
        self._acts_cache: Optional[Dict[str, Dict]] = None
        self._fragments_cache: Optional[Dict[int, Dict]] = None

    def _get_cached_or_download(self, url: str, cache_name: str) -> Dict:
        """Download JSON file with caching"""
        cache_path = CACHE_DIR / f"{cache_name}.json"

        # Check if cache exists and is recent (less than 1 day old)
        if cache_path.exists():
            cache_age = time.time() - cache_path.stat().st_mtime
            if cache_age < 86400:  # 24 hours
                logger.info(f"Loading from cache: {cache_path}")
                with open(cache_path, 'r', encoding='utf-8') as f:
                    return json.load(f)
                logger.info(f"Cache is {cache_age/3600:.1f}h old, using cached version")

        # Download fresh
        logger.info(f"Downloading {url}...")
        logger.info("NOTE: This may take 5-10 minutes for large files.")
        start = time.time()

        response = self.session.get(url, timeout=600, stream=True)
        response.raise_for_status()

        # Download with progress
        total_size = int(response.headers.get('content-length', 0))
        downloaded = 0
        chunks = []

        for chunk in response.iter_content(chunk_size=1024*1024):  # 1MB chunks
            chunks.append(chunk)
            downloaded += len(chunk)
            if total_size:
                pct = downloaded * 100 / total_size
                logger.info(f"Downloading: {downloaded/1024/1024:.1f}MB / {total_size/1024/1024:.1f}MB ({pct:.1f}%)")

        content_gzipped = b''.join(chunks)

        # Decompress and parse JSON
        logger.info("Decompressing...")
        content = gzip.decompress(content_gzipped)
        data = json.loads(content.decode('utf-8'))

        elapsed = time.time() - start
        size_mb = len(content_gzipped) / (1024 * 1024)
        logger.info(f"Downloaded and parsed {size_mb:.1f}MB in {elapsed:.1f}s")

        # Save to cache
        logger.info(f"Saving to cache: {cache_path}")
        with open(cache_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False)

        return data

    def _load_acts(self) -> Dict[str, Dict]:
        """Load legal acts metadata, cached"""
        if self._acts_cache is None:
            data = self._get_cached_or_download(ACTS_URL, "acts")
            # Index by act code (akt-kód)
            self._acts_cache = {}
            for item in data.get('položky', []):
                code = item.get('akt-kód')
                if code:
                    self._acts_cache[code] = item
            logger.info(f"Indexed {len(self._acts_cache)} acts")
        return self._acts_cache

    def _get_fragments_db_path(self) -> Path:
        """Get path to SQLite fragments database"""
        return CACHE_DIR / "fragments.db"

    def _init_fragments_db(self) -> sqlite3.Connection:
        """Initialize or open SQLite fragments database"""
        db_path = self._get_fragments_db_path()
        conn = sqlite3.connect(str(db_path))
        conn.execute("""
            CREATE TABLE IF NOT EXISTS fragments (
                frag_id INTEGER PRIMARY KEY,
                base_id INTEGER,
                iri TEXT,
                text TEXT
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_iri ON fragments(iri)")
        conn.commit()
        return conn

    def _is_fragments_db_ready(self) -> bool:
        """Check if fragments DB exists and has data"""
        db_path = self._get_fragments_db_path()
        if not db_path.exists():
            return False
        try:
            conn = sqlite3.connect(str(db_path))
            count = conn.execute("SELECT COUNT(*) FROM fragments").fetchone()[0]
            conn.close()
            return count > 0
        except:
            return False

    def _stream_fragments_to_db(self):
        """
        Stream download and parse fragments directly to SQLite.
        This avoids loading the entire 500MB+ JSON into memory.
        """
        db_path = self._get_fragments_db_path()
        gz_path = CACHE_DIR / "fragments.json.gz"

        # Download gzip file if not cached
        if not gz_path.exists():
            logger.info(f"Downloading fragments to {gz_path}...")
            logger.info("NOTE: This is a ~500MB file. Be patient!")
            start = time.time()

            response = self.session.get(FRAGMENTS_URL, timeout=1800, stream=True)
            response.raise_for_status()

            total_size = int(response.headers.get('content-length', 0))
            downloaded = 0

            with open(gz_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=1024*1024):
                    f.write(chunk)
                    downloaded += len(chunk)
                    if total_size:
                        pct = downloaded * 100 / total_size
                        if pct % 10 < 1:  # Log every ~10%
                            logger.info(f"Downloading: {downloaded/1024/1024:.1f}MB / {total_size/1024/1024:.1f}MB ({pct:.1f}%)")

            elapsed = time.time() - start
            logger.info(f"Downloaded {downloaded/1024/1024:.1f}MB in {elapsed:.1f}s")

        # Stream parse and insert into SQLite
        logger.info("Streaming fragments to SQLite database...")
        conn = self._init_fragments_db()

        # Use WAL mode for better performance with batched writes
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA cache_size=-32000")  # 32MB cache
        conn.execute("PRAGMA temp_store=MEMORY")

        conn.execute("DELETE FROM fragments")  # Clear existing
        conn.commit()

        start = time.time()
        count = 0
        batch = []
        batch_size = 5000  # Reduced from 10000 to lower memory usage

        # ijson is already imported at module level (required dependency)
        logger.info("Using ijson for streaming parse")

        with gzip.open(gz_path, 'rb') as f:
            for item in ijson.items(f, 'položky.item'):
                frag_id = item.get('fragment-id')
                if frag_id:
                    batch.append((
                        frag_id,
                        item.get('fragment-base-id', frag_id),
                        item.get('iri', ''),
                        item.get('fragment-text', '')
                    ))

                    if len(batch) >= batch_size:
                        conn.executemany(
                            "INSERT OR REPLACE INTO fragments (frag_id, base_id, iri, text) VALUES (?, ?, ?, ?)",
                            batch
                        )
                        conn.commit()
                        count += len(batch)
                        batch = []  # Clear batch immediately after commit
                        gc.collect()  # Force garbage collection to free memory

                        if count % 50000 == 0:
                            logger.info(f"Indexed {count} fragments...")

        # Insert remaining batch
        if batch:
            conn.executemany(
                "INSERT OR REPLACE INTO fragments (frag_id, base_id, iri, text) VALUES (?, ?, ?, ?)",
                batch
            )
            conn.commit()
            count += len(batch)

        elapsed = time.time() - start
        logger.info(f"Indexed {count} fragments in {elapsed:.1f}s")
        conn.close()
        gc.collect()  # Final garbage collection

    def _load_fragments(self) -> sqlite3.Connection:
        """
        Return a SQLite connection for fragment queries.
        Initializes the database from download if not already done.
        """
        if not self._is_fragments_db_ready():
            self._stream_fragments_to_db()
        return self._init_fragments_db()

    def _build_act_text(self, act: Dict, fragments_db: sqlite3.Connection) -> str:
        """Build complete text for an act from fragments using SQLite queries"""
        act_iri = act.get('iri', '')

        # Extract the ELI path (e.g., "eli/cz/sb/1918/8")
        if 'eli/' not in act_iri:
            return ""

        eli_path = act_iri.split('eli/')[-1]
        eli_pattern = f'%eli/{eli_path}%'

        # Query matching fragments ordered by base_id
        cursor = fragments_db.execute(
            """
            SELECT base_id, text FROM fragments
            WHERE iri LIKE ? AND text IS NOT NULL AND text != ''
            ORDER BY base_id
            """,
            (eli_pattern,)
        )

        # Assemble text from query results
        texts = [row[1] for row in cursor]
        full_text = '\n'.join(texts)

        # Clean HTML tags like <var>
        full_text = re.sub(r'<[^>]+>', '', full_text)
        # Normalize whitespace
        full_text = re.sub(r'\n{3,}', '\n\n', full_text)
        full_text = re.sub(r' {2,}', ' ', full_text)

        return full_text.strip()

    def fetch_all(self, limit: int = None) -> Iterator[Dict[str, Any]]:
        """
        Fetch all documents with full text.

        Args:
            limit: Maximum number of documents to fetch (None for all)

        Yields:
            Raw document dictionaries with full text
        """
        # Load data
        logger.info("Loading acts metadata...")
        acts = self._load_acts()

        logger.info("Loading fragments (this may take a while on first run)...")
        fragments_db = self._load_fragments()

        # Sort by year descending (most recent first)
        sorted_acts = sorted(
            acts.items(),
            key=lambda x: (x[1].get('akt-rok-předpisu', 0), x[1].get('akt-číslo-předpisu', 0)),
            reverse=True
        )

        count = 0
        checked = 0

        try:
            for act_code, act in sorted_acts:
                if limit and count >= limit:
                    break

                checked += 1

                # Build full text from fragments using SQLite
                text = self._build_act_text(act, fragments_db)

                # Only yield if we have actual text
                if text and len(text) > 100:
                    yield {
                        'code': act_code,
                        'citation': act.get('akt-citace', ''),
                        'title': act.get('akt-název-vyhlášený', ''),
                        'year': act.get('akt-rok-předpisu'),
                        'number': act.get('akt-číslo-předpisu'),
                        'collection': act.get('akt-sbírka-kód', 'sb'),
                        'iri': act.get('iri', ''),
                        'text': text,
                        'raw': act
                    }
                    count += 1

                    if count % 10 == 0:
                        logger.info(f"Found {count} documents with text (checked {checked} acts)...")
        finally:
            fragments_db.close()

        logger.info(f"Total: {count} documents with text out of {checked} checked")

    def fetch_updates(self, since: datetime) -> Iterator[Dict[str, Any]]:
        """Fetch documents updated since a given date."""
        yield from self.fetch_all()

    def normalize(self, raw_doc: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize document to standard schema."""
        collection = raw_doc.get('collection', 'sb')
        year = raw_doc.get('year', '')
        number = raw_doc.get('number', '')

        if year and number:
            url = f"https://www.e-sbirka.cz/{collection}/{year}/{number}"
        else:
            url = "https://www.e-sbirka.cz"

        date = f"{year}-01-01" if year else None

        return {
            '_id': raw_doc['code'],
            '_source': 'CZ/eSbirka',
            '_type': 'legislation',
            '_fetched_at': datetime.now().isoformat(),
            'title': raw_doc.get('title', ''),
            'citation': raw_doc.get('citation', ''),
            'text': raw_doc.get('text', ''),
            'year': raw_doc.get('year'),
            'number': raw_doc.get('number'),
            'collection': raw_doc.get('collection', 'sb'),
            'date': date,
            'url': url,
            'language': 'cs'
        }


def main():
    """Main entry point for testing and bootstrap"""

    if len(sys.argv) > 1 and sys.argv[1] == 'bootstrap':
        fetcher = ESbirkaFetcher()
        sample_dir = Path(__file__).parent / 'sample'
        sample_dir.mkdir(exist_ok=True)

        logger.info("Starting bootstrap...")
        logger.info("NOTE: First run requires downloading ~500MB of data. Be patient!")

        sample_count = 0
        target_count = 10 if '--sample' in sys.argv else 100

        for raw_doc in fetcher.fetch_all(limit=target_count):
            if sample_count >= target_count:
                break

            normalized = fetcher.normalize(raw_doc)
            text_len = len(normalized.get('text', ''))

            if text_len < 100:
                continue

            # Save to sample directory
            filename = f"{normalized['_id'].replace('/', '_')}.json"
            filepath = sample_dir / filename

            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(normalized, f, indent=2, ensure_ascii=False)

            logger.info(f"Saved [{sample_count+1}/{target_count}]: {normalized['citation']} ({text_len} chars)")
            sample_count += 1

        logger.info(f"Bootstrap complete. Saved {sample_count} documents to {sample_dir}")

        # Print summary
        files = list(sample_dir.glob('*.json'))
        total_chars = 0
        for f in files:
            with open(f, 'r', encoding='utf-8') as fp:
                data = json.load(fp)
                total_chars += len(data.get('text', ''))

        print(f"\n=== SUMMARY ===")
        print(f"Sample files: {len(files)}")
        print(f"Total text chars: {total_chars:,}")
        print(f"Average chars/doc: {total_chars // max(len(files), 1):,}")

    else:
        # Test mode
        fetcher = ESbirkaFetcher()
        print("Testing e-Sbírka fetcher...")

        count = 0
        for raw_doc in fetcher.fetch_all(limit=3):
            normalized = fetcher.normalize(raw_doc)
            print(f"\n--- Document {count + 1} ---")
            print(f"ID: {normalized['_id']}")
            print(f"Citation: {normalized['citation']}")
            print(f"Title: {normalized['title'][:80]}")
            print(f"Year: {normalized['year']}")
            print(f"Text length: {len(normalized.get('text', ''))}")
            print(f"Text preview: {normalized.get('text', '')[:300]}...")
            count += 1


if __name__ == '__main__':
    main()
