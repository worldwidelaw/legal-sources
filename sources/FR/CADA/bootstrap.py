#!/usr/bin/env python3
"""
CADA (Commission d'accès aux documents administratifs) Data Fetcher

Fetches administrative opinions on document access requests published by the
CADA as open data on data.gouv.fr.

The CADA is an independent French administrative authority that issues opinions
when citizens are denied access to administrative documents.

Data source:
- Dataset: https://www.data.gouv.fr/datasets/avis-et-conseils-de-la-cada
- Consolidated CSV export ("Ensemble consolidé des avis et conseils de la CADA"),
  resolved at runtime through the data.gouv.fr catalog API so a re-publication
  under a new dated URL is picked up automatically.
- 60,000+ opinions since 1984; the `Avis` column carries the full opinion text.

The former standalone portal at cada.data.gouv.fr was retired: every path now
301s to https://www.data.gouv.fr/explore/cada/... and the old /api/search
endpoint 404s there, which is what made the full crawl exit 1 (issue #1396).

License: Licence Ouverte / Open Licence (Etalab)
"""

import argparse
import csv
import io
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator, Optional

import requests

# Constants
DATASET_API = "https://www.data.gouv.fr/api/1/datasets/avis-et-conseils-de-la-cada/"
EXPLORE_BASE = "https://www.data.gouv.fr/explore/cada"
CONSOLIDATED_TITLE_HINT = "ensemble consolid"
USER_AGENT = "LegalDataHunter/1.0 (Open Data Research)"

# Opinion bodies routinely exceed the 128 KB csv default.
csv.field_size_limit(64 * 1024 * 1024)


def clean_text(text: str) -> str:
    """Clean up opinion text."""
    if not text:
        return ""
    # Normalize whitespace
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n\s*\n\s*\n+', '\n\n', text)
    return text.strip()


def parse_date(date_str: str) -> Optional[str]:
    """Parse a CADA session date to ISO format (YYYY-MM-DD)."""
    if not date_str:
        return None

    date_str = date_str.strip()

    # Consolidated CSV format: "03/03/1984"
    m = re.match(r'^(\d{2})/(\d{2})/(\d{4})$', date_str)
    if m:
        day, month, year = m.groups()
        try:
            datetime(int(year), int(month), int(day))
        except ValueError:
            return None
        return f"{year}-{month}-{day}"

    # RFC 2822 format, as served by the retired JSON API
    try:
        from email.utils import parsedate_to_datetime
        return parsedate_to_datetime(date_str).strftime('%Y-%m-%d')
    except (ValueError, TypeError):
        pass

    # ISO format
    if re.match(r'^\d{4}-\d{2}-\d{2}', date_str):
        return date_str[:10]

    return None


def _split_multi(value: str) -> list[str]:
    """Split a CSV column that packs several values behind '/' or ','."""
    if not value:
        return []
    parts = re.split(r'\s*[/,]\s*', value.strip())
    return [p for p in parts if p]


def normalize(raw: dict) -> Optional[dict]:
    """Transform a consolidated-CSV row into the normalized schema."""
    opinion_id = (raw.get('Numéro de dossier') or '').strip()
    if not opinion_id:
        return None

    text = clean_text(raw.get('Avis') or '')
    if not text:
        return None

    session_date = parse_date(raw.get('Séance') or '')
    if not session_date:
        # Fall back to the year column so a malformed session date does not
        # cost us the record (date is a required temporal key downstream).
        year = (raw.get('Année') or '').strip()
        if re.match(r'^\d{4}$', year):
            session_date = f"{year}-01-01"

    subject = clean_text(raw.get('Objet') or '')
    administration = (raw.get('Administration') or '').strip()
    opinion_type = (raw.get('Type') or 'Avis').strip()

    title = subject if subject else f"{opinion_type} {opinion_id} - {administration}".strip(' -')
    if len(title) > 200:
        title = title[:197] + "..."

    return {
        '_id': f"FR/CADA/{opinion_id}",
        '_source': 'FR/CADA',
        '_type': 'doctrine',  # CADA issues opinions/doctrine, not binding decisions
        '_fetched_at': datetime.now(timezone.utc).isoformat(),
        'opinion_id': opinion_id,
        'title': title,
        'date': session_date,
        'url': f"{EXPLORE_BASE}/{opinion_id}",
        'text': text,
        'subject': subject,
        'administration': administration,
        'meanings': _split_multi(raw.get('Sens et motivation') or ''),
        'topics': _split_multi(raw.get('Thème et sous thème') or ''),
        'tags': _split_multi(raw.get('Mots clés') or ''),
        'part': (raw.get('Partie') or '').strip() or None,
        'type': opinion_type,
    }


def get_session() -> requests.Session:
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT, "Accept": "*/*"})
    return session


def resolve_consolidated_csv(session: Optional[requests.Session] = None) -> dict:
    """
    Resolve the consolidated CSV resource through the data.gouv.fr catalog API.

    The export is republished under a new dated URL every few months, so the URL
    is never hardcoded. Raises loudly rather than returning an empty corpus, so a
    catalog change fails the run instead of silently ingesting nothing.
    """
    session = session or get_session()
    response = session.get(DATASET_API, timeout=60)
    response.raise_for_status()
    resources = response.json().get('resources', [])

    csv_resources = [r for r in resources if (r.get('format') or '').lower() == 'csv']
    if not csv_resources:
        raise RuntimeError(
            f"No CSV resource on {DATASET_API} — the dataset layout changed"
        )

    consolidated = [
        r for r in csv_resources
        if CONSOLIDATED_TITLE_HINT in (r.get('title') or '').lower()
    ]
    # The consolidated export is by far the largest file; fall back to size if
    # the publisher renames it.
    chosen = max(
        consolidated or csv_resources,
        key=lambda r: r.get('filesize') or 0,
    )

    print(
        f"Consolidated export: {chosen.get('title')} "
        f"({(chosen.get('filesize') or 0) / (1024 ** 2):.1f} MB)",
        file=sys.stderr,
    )
    return chosen


def iter_csv_rows(url: str, session: Optional[requests.Session] = None) -> Generator[dict, None, None]:
    """
    Stream the consolidated CSV row by row.

    The export is ~190 MB with opinion bodies inline, so it is decoded through a
    TextIOWrapper over the raw socket instead of being buffered — memory stays
    flat regardless of corpus size, and quoted fields keep their real newlines.
    """
    session = session or get_session()
    with session.get(url, stream=True, timeout=600) as response:
        response.raise_for_status()
        response.raw.decode_content = True
        stream = io.TextIOWrapper(
            response.raw, encoding='utf-8', errors='replace', newline=''
        )
        for row in csv.DictReader(stream):
            yield row


def fetch_all(session: Optional[requests.Session] = None) -> Generator[dict, None, None]:
    """Yield every CADA opinion from the consolidated export."""
    session = session or get_session()
    resource = resolve_consolidated_csv(session)

    seen: set[str] = set()
    rows = 0
    yielded = 0
    skipped = 0

    for row in iter_csv_rows(resource['url'], session):
        rows += 1
        doc = normalize(row)
        if doc is None:
            skipped += 1
            continue
        if doc['_id'] in seen:
            continue
        seen.add(doc['_id'])
        yielded += 1
        yield doc

        if yielded % 5000 == 0:
            print(f"  {yielded:,} opinions...", file=sys.stderr)

    print(
        f"\nTotal: {yielded:,} opinions from {rows:,} CSV rows "
        f"({skipped:,} rows without an id or opinion text)",
        file=sys.stderr,
    )

    if yielded == 0:
        raise RuntimeError(
            f"Consolidated export {resource['url']} yielded 0 opinions — "
            "column layout probably changed"
        )


def fetch_updates(since: datetime,
                  session: Optional[requests.Session] = None) -> Generator[dict, None, None]:
    """Yield opinions from sessions on or after `since`."""
    since_date = since.date().isoformat()
    for doc in fetch_all(session):
        if doc.get('date') and doc['date'] >= since_date:
            yield doc


def bootstrap_sample(limit: int = 15) -> None:
    """
    Fetch sample records for testing.

    The consolidated export is ordered oldest-first, so taking the first N
    matches would sample only the terse mid-1980s opinions. Reservoir sampling
    over the single stream gives a spread across all 40+ years for the same cost.
    """
    import random

    sample_dir = Path(__file__).parent / 'sample'
    sample_dir.mkdir(exist_ok=True)

    for f in sample_dir.glob('*.json'):
        f.unlink()

    print("Fetching CADA sample data...", file=sys.stderr)

    rng = random.Random(42)  # deterministic samples across runs
    samples: list[dict] = []
    considered = 0

    for doc in fetch_all():
        if len(doc.get('text', '')) < 400:
            continue
        considered += 1
        if len(samples) < limit:
            samples.append(doc)
        else:
            j = rng.randrange(considered)
            if j < limit:
                samples[j] = doc

    samples.sort(key=lambda d: d.get('date') or '')
    topic_counts: dict[str, int] = {}
    for doc in samples:
        topic = doc['topics'][0] if doc['topics'] else 'Other'
        topic_counts[topic] = topic_counts.get(topic, 0) + 1

    count = 0
    total_chars = 0

    for doc in samples:
        safe_id = doc['opinion_id'].replace('/', '-').replace('\\', '-')
        filepath = sample_dir / f"{safe_id}.json"

        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(doc, f, ensure_ascii=False, indent=2)

        text_len = len(doc.get('text', ''))
        total_chars += text_len
        print(f"Saved {filepath.name} ({text_len:,} chars)", file=sys.stderr)
        count += 1

    avg_chars = total_chars // count if count > 0 else 0
    print(f"\nSaved {count} sample records to {sample_dir}", file=sys.stderr)
    print(f"Average text length: {avg_chars:,} chars", file=sys.stderr)

    print("\nTopic breakdown:", file=sys.stderr)
    for topic, c in sorted(topic_counts.items(), key=lambda x: -x[1]):
        print(f"  - {topic}: {c}", file=sys.stderr)


def run_full() -> int:
    """Stream the whole corpus to data/records.jsonl (what the pipeline ingests)."""
    data_dir = Path(__file__).parent / 'data'
    data_dir.mkdir(parents=True, exist_ok=True)
    jsonl_path = data_dir / 'records.jsonl'

    count = 0
    with open(jsonl_path, 'w', encoding='utf-8') as out:
        for doc in fetch_all():
            out.write(json.dumps(doc, ensure_ascii=False) + '\n')
            count += 1
            if count % 5000 == 0:
                out.flush()

    print(f"\nBootstrap complete: {count} records written to {jsonl_path}", file=sys.stderr)
    return count


def show_stats() -> None:
    """Summarize the corpus from the consolidated export."""
    from collections import Counter

    years: Counter = Counter()
    administrations: Counter = Counter()
    meanings: Counter = Counter()
    total = 0

    for doc in fetch_all():
        total += 1
        if doc.get('date'):
            years[doc['date'][:4]] += 1
        if doc.get('administration'):
            administrations[doc['administration']] += 1
        for meaning in doc.get('meanings', []):
            meanings[meaning] += 1

    print(f"Total opinions: {total:,}")
    print(f"Years covered: {min(years)}-{max(years)}" if years else "No dates")

    print("\nTop 10 administrations:")
    for admin, count in administrations.most_common(10):
        print(f"  {admin}: {count:,}")

    print("\nBy meaning (outcome):")
    for meaning, count in meanings.most_common(10):
        print(f"  {meaning}: {count:,}")


def main():
    parser = argparse.ArgumentParser(
        description="CADA (Commission d'accès aux documents administratifs) Data Fetcher"
    )
    subparsers = parser.add_subparsers(dest='command')

    bootstrap_parser = subparsers.add_parser('bootstrap', help='Fetch opinions')
    bootstrap_parser.add_argument('--sample', action='store_true', help='Fetch sample records')
    bootstrap_parser.add_argument('--limit', type=int, default=15, help='Number of sample records')
    bootstrap_parser.add_argument('--full', action='store_true', help='Fetch all records')

    # The fleet wrapper invokes bootstrap-fast; without it argparse exits 2 and
    # the pipeline falls back to re-ingesting sample/.
    fast_parser = subparsers.add_parser('bootstrap-fast', help='Full pull (fleet entry point)')
    fast_parser.add_argument('--workers', type=int, default=1, help='Unused, kept for compatibility')
    fast_parser.add_argument('--batch', type=int, default=100, help='Unused, kept for compatibility')

    updates_parser = subparsers.add_parser('updates', help='Fetch opinions since a date')
    updates_parser.add_argument('--since', required=True, help='YYYY-MM-DD')

    subparsers.add_parser('stats', help='Show dataset statistics')

    args = parser.parse_args()

    if args.command == 'bootstrap':
        if args.sample:
            bootstrap_sample(args.limit)
        else:
            run_full()
    elif args.command == 'bootstrap-fast':
        run_full()
    elif args.command == 'updates':
        since = datetime.strptime(args.since, '%Y-%m-%d').replace(tzinfo=timezone.utc)
        count = 0
        for _ in fetch_updates(since):
            count += 1
        print(f"{count} opinions since {args.since}", file=sys.stderr)
    elif args.command == 'stats':
        show_stats()
    else:
        parser.print_help()


if __name__ == '__main__':
    main()
