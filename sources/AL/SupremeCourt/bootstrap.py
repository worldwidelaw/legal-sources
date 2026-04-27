#!/usr/bin/env python3
"""
Albanian Supreme Court (Gjykata e Lartë) - Case Law Fetcher

Fetches court decisions from two sources:
1. Archive 1999-2019: Monthly bundle documents (.doc) from Gatsby page-data API
2. Bulletins 2020+: Individual decisions (.doc) embedded in informative bulletins

The archive contains monthly bundles with multiple decisions per file.
Recent bulletins contain individual decision files with maxima and summaries.
"""

import argparse
import json
import os
import re
import subprocess
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator, Optional
from urllib.parse import urljoin

import requests

# Try to import python-docx for .docx files
try:
    import docx
    HAS_PYTHON_DOCX = True
except ImportError:
    HAS_PYTHON_DOCX = False


class AlbanianSupremeCourtFetcher:
    """Fetcher for Albanian Supreme Court decisions."""

    SOURCE_ID = "AL/SupremeCourt"
    API_BASE = "https://www.gjykataelarte.gov.al/page-data"
    MEDIA_BASE = "https://gjykata-media.s3.eu-central-1.amazonaws.com"

    # Albanian month names for parsing
    ALBANIAN_MONTHS = {
        'janar': 1, 'shkurt': 2, 'mars': 3, 'prill': 4,
        'maj': 5, 'qershor': 6, 'korrik': 7, 'gusht': 8,
        'shtator': 9, 'tetor': 10, 'nentor': 11, 'dhjetor': 12
    }

    def __init__(self, sample_dir: Optional[str] = None):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'LegalDataHunter/1.0 (research project)',
            'Accept': 'application/json'
        })
        self.sample_dir = Path(sample_dir) if sample_dir else Path(__file__).parent / 'sample'
        self.sample_dir.mkdir(parents=True, exist_ok=True)
        self._check_doc_tools()

    def _check_doc_tools(self):
        """Check if tools for .doc extraction are available and warn if not."""
        tools = []
        if sys.platform == 'darwin':
            tools.append(('textutil', ['textutil', '-help']))
        tools.extend([
            ('antiword', ['antiword', '--version']),
            ('catdoc', ['catdoc', '-V']),
            ('libreoffice', ['libreoffice', '--version']),
        ])
        available = []
        for name, cmd in tools:
            try:
                subprocess.run(cmd, capture_output=True, timeout=5)
                available.append(name)
            except (FileNotFoundError, subprocess.TimeoutExpired):
                pass
        if not available:
            print("WARNING: No .doc extraction tool found!", file=sys.stderr)
            print("This source uses .doc files. Install one of:", file=sys.stderr)
            print("  - antiword (apt install antiword)", file=sys.stderr)
            print("  - catdoc (apt install catdoc)", file=sys.stderr)
            print("  - libreoffice (apt install libreoffice)", file=sys.stderr)
            print("Without a tool, no records will be extracted.", file=sys.stderr)
        else:
            print(f"Doc extraction tools available: {', '.join(available)}", file=sys.stderr)

    def _fetch_json(self, url: str) -> Optional[dict]:
        """Fetch JSON from a URL."""
        try:
            time.sleep(1)  # Rate limiting
            resp = self.session.get(url, timeout=30)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            print(f"Error fetching {url}: {e}", file=sys.stderr)
            return None

    def _download_file(self, url: str) -> Optional[bytes]:
        """Download a file from URL."""
        try:
            time.sleep(1)  # Rate limiting
            resp = self.session.get(url, timeout=60)
            resp.raise_for_status()
            return resp.content
        except Exception as e:
            print(f"Error downloading {url}: {e}", file=sys.stderr)
            return None

    def _extract_text_from_doc(self, content: bytes, filename: str) -> Optional[str]:
        """Extract text from .doc or .docx file content."""
        ext = os.path.splitext(filename)[1].lower()

        # Save to temp file
        with tempfile.NamedTemporaryFile(suffix=ext, delete=False) as tmp:
            tmp.write(content)
            tmp_path = tmp.name

        try:
            text = None

            # Try textutil (macOS native - best for .doc)
            if sys.platform == 'darwin':
                try:
                    result = subprocess.run(
                        ['textutil', '-convert', 'txt', '-stdout', tmp_path],
                        capture_output=True,
                        timeout=30
                    )
                    if result.returncode == 0:
                        text = result.stdout.decode('utf-8', errors='replace')
                except Exception:
                    pass

            # Try antiword for .doc files (Linux/Unix)
            if text is None and ext == '.doc':
                try:
                    result = subprocess.run(
                        ['antiword', tmp_path],
                        capture_output=True,
                        timeout=30
                    )
                    if result.returncode == 0:
                        text = result.stdout.decode('utf-8', errors='replace')
                except FileNotFoundError:
                    pass
                except Exception:
                    pass

            # Try catdoc as another fallback for .doc files (Linux/Unix)
            if text is None and ext == '.doc':
                try:
                    result = subprocess.run(
                        ['catdoc', '-w', tmp_path],
                        capture_output=True,
                        timeout=30
                    )
                    if result.returncode == 0:
                        text = result.stdout.decode('utf-8', errors='replace')
                except FileNotFoundError:
                    pass
                except Exception:
                    pass

            # Try LibreOffice headless conversion for .doc files
            if text is None and ext == '.doc':
                try:
                    # Convert to txt using LibreOffice
                    with tempfile.TemporaryDirectory() as tmpdir:
                        result = subprocess.run(
                            ['libreoffice', '--headless', '--convert-to', 'txt:Text',
                             '--outdir', tmpdir, tmp_path],
                            capture_output=True,
                            timeout=60
                        )
                        if result.returncode == 0:
                            txt_path = os.path.join(tmpdir, os.path.splitext(os.path.basename(tmp_path))[0] + '.txt')
                            if os.path.exists(txt_path):
                                with open(txt_path, 'r', encoding='utf-8', errors='replace') as f:
                                    text = f.read()
                except FileNotFoundError:
                    pass
                except Exception:
                    pass

            # Fallback to python-docx for .docx
            if text is None and ext == '.docx' and HAS_PYTHON_DOCX:
                try:
                    doc = docx.Document(tmp_path)
                    paragraphs = [p.text for p in doc.paragraphs]
                    text = '\n'.join(paragraphs)
                except Exception:
                    pass

            # Clean up text
            if text:
                # Remove excessive whitespace
                text = re.sub(r'\n{3,}', '\n\n', text)
                text = text.strip()

            return text

        finally:
            os.unlink(tmp_path)

    def _parse_decision_from_bundle(self, text: str, doc_info: dict) -> list[dict]:
        """Parse individual decisions from a bundle document.

        Bundle documents have structure:
        1. Table of contents: "Vendimi Nr. XX (KOLEGJI ...)\tPARTIES\tPAGE"
        2. Full decisions starting with "REPUBLIKA E SHQIPERISE" header

        We split on the header pattern to get full decision texts.
        """
        decisions = []

        # Extract year and month from document info
        title = doc_info.get('title', '')
        year = doc_info.get('year')
        month = doc_info.get('month')

        # Split by decision header pattern
        # Full decisions start with "REPUBLIKA E SHQIPERISE" followed by "GJYKATA E LARTE"
        # and then the college name and decision number
        header_pattern = r'REPUBLIKA E SHQIPERI[SË]E\s*\n\s*GJYKATA E LART[EË]'

        # Find all positions where decisions start
        parts = re.split(header_pattern, text, flags=re.IGNORECASE)

        if len(parts) <= 1:
            # If can't split properly, store as bundle document
            return [{
                '_id': f"AL-SC-bundle-{year or 'unknown'}-{month or 'unknown'}",
                '_source': self.SOURCE_ID,
                '_type': 'case_law',
                '_fetched_at': datetime.now(timezone.utc).isoformat(),
                'title': title,
                'text': text,
                'date': f"{year}-{month:02d}-01" if year and month else None,
                'year': year,
                'month': month,
                'url': doc_info.get('url', ''),
                'document_type': 'bundle',
                'college': 'mixed'
            }]

        # First part is the table of contents, skip it
        for i, decision_text in enumerate(parts[1:], start=1):
            decision_text = decision_text.strip()
            if len(decision_text) < 500:  # Skip if too short for a real decision
                continue

            # Re-add the header that was split out
            full_text = "REPUBLIKA E SHQIPERISE\nGJYKATA E LARTE\n" + decision_text

            # Extract decision number from "Nr. XX i Vendimit" pattern
            num_match = re.search(r'Nr\.\s*(\d+)\s+i\s+Vendimit', decision_text, re.IGNORECASE)
            if not num_match:
                # Try alternative pattern
                num_match = re.search(r'Vendimi\s+Nr\.?\s*(\d+)', decision_text, re.IGNORECASE)
            decision_num = num_match.group(1) if num_match else str(i)

            # Extract college type from header
            college = 'unknown'
            if 'KOLEGJI CIVIL' in decision_text.upper():
                college = 'civil'
            elif 'KOLEGJI PENAL' in decision_text.upper():
                college = 'penal'
            elif re.search(r'KOLEGJI\s+ADMINISTR', decision_text.upper()):
                college = 'administrative'
            elif 'KOLEGJET E BASHK' in decision_text.upper() or 'KOLEGJI TREGTAR' in decision_text.upper():
                college = 'united'

            # Extract parties from PADITES/I PADITUR pattern
            applicant = None
            respondent = None
            applicant_match = re.search(r'PADIT[EË]S[I]?:\s*([^\n]+)', decision_text, re.IGNORECASE)
            if applicant_match:
                applicant = applicant_match.group(1).strip()[:200]
            respondent_match = re.search(r'I PADITUR:\s*([^\n]+)', decision_text, re.IGNORECASE)
            if respondent_match:
                respondent = respondent_match.group(1).strip()[:200]

            # Try "X kundër Y" pattern if above didn't work
            if not applicant:
                parties_match = re.search(r'qe i perket:\s*\n?([^\n]+?)\s+kund[eë]r\s+([^\n]+)', decision_text, re.IGNORECASE)
                if parties_match:
                    applicant = parties_match.group(1).strip()[:200]
                    respondent = parties_match.group(2).strip()[:200]

            # Extract date from decision
            date = None
            date_match = re.search(r'date[s]?\s+(\d{1,2})\.(\d{1,2})\.(\d{4})', decision_text)
            if date_match:
                day, month_num, year_num = date_match.groups()
                date = f"{year_num}-{month_num.zfill(2)}-{day.zfill(2)}"
            elif year and month:
                date = f"{year}-{month:02d}-01"

            # Create decision title
            decision_title = f"Vendimi Nr. {decision_num}"
            if applicant and respondent:
                decision_title += f" - {applicant[:40]} kundër {respondent[:40]}"

            decision = {
                '_id': f"AL-SC-{year or 'unknown'}-{month or 'unknown'}-{decision_num}",
                '_source': self.SOURCE_ID,
                '_type': 'case_law',
                '_fetched_at': datetime.now(timezone.utc).isoformat(),
                'title': decision_title,
                'text': full_text,
                'date': date,
                'year': year,
                'month': month,
                'decision_number': decision_num,
                'college': college,
                'applicant': applicant,
                'respondent': respondent,
                'url': doc_info.get('url', ''),
                'document_type': 'decision',
                'bundle_title': title
            }
            decisions.append(decision)

        return decisions

    def _parse_bulletin_decision(self, text: str, doc_info: dict) -> dict:
        """Parse a single decision from a bulletin document."""
        # Extract decision number from title or text
        decision_num = None
        title = doc_info.get('title', '')

        # Pattern: nr. 00-YYYY-XXXX (XXX) or Nr. XXXX
        num_match = re.search(r'nr\.?\s*(\d{2}-\d{4}-\d+(?:\s*\(\d+\))?)', title, re.IGNORECASE)
        if num_match:
            decision_num = num_match.group(1)
        else:
            num_match = re.search(r'Vendimi\s+nr\.?\s*(\d+)', title, re.IGNORECASE)
            if num_match:
                decision_num = num_match.group(1)

        # Extract date from title
        date = None
        date_match = re.search(r'dat[ëe]\s+(\d{1,2})\.(\d{1,2})\.(\d{4})', title)
        if date_match:
            day, month, year = date_match.groups()
            date = f"{year}-{month.zfill(2)}-{day.zfill(2)}"

        # Extract college
        college = 'unknown'
        if 'civil' in title.lower():
            college = 'civil'
        elif 'penal' in title.lower():
            college = 'penal'
        elif 'administrat' in title.lower():
            college = 'administrative'

        # Extract maxima (legal principle)
        maxima = None
        maxima_match = re.search(r'Maksima\s*[-–—]\s*(.+?)(?=Fjalë kyçe|Përmbledhje|$)', text, re.DOTALL | re.IGNORECASE)
        if maxima_match:
            maxima = maxima_match.group(1).strip()
            # Clean HTML tags
            maxima = re.sub(r'<[^>]+>', '', maxima)

        # Extract keywords
        keywords = []
        keywords_match = re.search(r'Fjalë kyçe\s*[-–—]\s*(.+?)(?=Përmbledhje|$)', text, re.DOTALL | re.IGNORECASE)
        if keywords_match:
            kw_text = keywords_match.group(1).strip()
            kw_text = re.sub(r'<[^>]+>', '', kw_text)
            keywords = [k.strip() for k in re.split(r'[,;]', kw_text) if k.strip()]

        return {
            '_id': f"AL-SC-{decision_num or doc_info.get('id', 'unknown')}",
            '_source': self.SOURCE_ID,
            '_type': 'case_law',
            '_fetched_at': datetime.now(timezone.utc).isoformat(),
            'title': title,
            'text': text,
            'date': date,
            'decision_number': decision_num,
            'college': college,
            'maxima': maxima,
            'keywords': keywords,
            'url': doc_info.get('url', ''),
            'document_type': 'bulletin_decision',
            'bulletin_slug': doc_info.get('bulletin_slug')
        }

    def fetch_archive_documents(self) -> Iterator[dict]:
        """Fetch documents from the 1999-2019 archive."""
        archive_url = f"{self.API_BASE}/sq/vendimet-e-gjykates/vendimet-1999-2019/page-data.json"
        data = self._fetch_json(archive_url)

        if not data:
            return

        # Navigate to article body
        try:
            article = data['result']['data']['api']['article']
            body = article.get('body', [])
        except (KeyError, TypeError):
            print("Could not parse archive data structure", file=sys.stderr)
            return

        # Find referenced tabs with file attachments
        for item in body:
            if item.get('__typename') == 'API_ComponentArticlePatternBodyReferencedTabs':
                tabs = item.get('tabs', [])
                for tab_ref in tabs:
                    tab = tab_ref.get('tab', {})
                    tab_title = tab.get('tabTitle', {})
                    year_text = tab_title.get('text_sq', '').strip()

                    # Extract year
                    year_match = re.search(r'(\d{4})', year_text)
                    year = int(year_match.group(1)) if year_match else None

                    # Get attached files
                    tab_body = tab.get('body', [])
                    for attachment in tab_body:
                        if attachment.get('__typename') == 'API_ComponentArticlePatternBodyAttachedFile':
                            file_info = attachment.get('file', {})
                            file_url = file_info.get('url')

                            if not file_url:
                                continue

                            title_obj = attachment.get('title') or {}
                            title = title_obj.get('text_sq', '') or title_obj.get('text_en', '') or ''

                            # Parse month from title
                            month = None
                            for month_name, month_num in self.ALBANIAN_MONTHS.items():
                                if title and month_name in title.lower():
                                    month = month_num
                                    break

                            yield {
                                'url': file_url,
                                'title': title,
                                'year': year,
                                'month': month,
                                'type': 'archive'
                            }

    def _get_bulletin_slugs(self, max_pages: int = 5) -> list[dict]:
        """Get bulletin slugs from paginated list using pageContext.articles."""
        slugs = []
        for page in range(1, max_pages + 1):
            if page == 1:
                url = f"{self.API_BASE}/sq/lajme/buletini/page-data.json"
            else:
                url = f"{self.API_BASE}/sq/lajme/buletini/{page}/page-data.json"

            data = self._fetch_json(url)
            if not data:
                break

            # New API: articles in pageContext
            articles = (data.get('result', {}).get('pageContext', {})
                        .get('articles', []))
            # Fallback to old API structure
            if not articles:
                try:
                    result = data['result']['data']['api']
                    articles = (result.get('newsList', {}).get('news', [])
                                or result.get('news', []))
                except (KeyError, TypeError):
                    pass

            if not articles:
                break

            for item in articles:
                slug = item.get('slug')
                if slug:
                    slugs.append({
                        'slug': slug,
                        'publishDate': item.get('publishDate'),
                        'title': (item.get('title', {}).get('text_sq', '')
                                  if isinstance(item.get('title'), dict)
                                  else item.get('title', ''))
                    })

        return slugs

    def _parse_decisions_from_inline_html(self, raw_html: str, bulletin_info: dict) -> list[dict]:
        """Parse individual decisions from bulletin inline HTML text."""
        import html as html_mod
        # Strip HTML tags, decode entities
        text = re.sub(r'<[^>]+>', '\n', raw_html)
        text = html_mod.unescape(text)
        text = re.sub(r'\n{3,}', '\n\n', text).strip()

        # Split on decision headers: "Vendimi nr. XX-YYYY-ZZZZ, datë DD.MM.YYYY i Kolegjit X"
        header_pattern = r'(Vendimi? nr\.\s*\d[\d\-\(\)\s,\.a-zA-Zëë]+i Kolegjit[^\n]+)'
        parts = re.split(header_pattern, text, flags=re.IGNORECASE)

        decisions = []
        # parts alternates: [preamble, header1, body1, header2, body2, ...]
        i = 1
        while i < len(parts) - 1:
            header = parts[i].strip()
            body = parts[i + 1].strip()
            i += 2

            if len(body) < 100:
                continue

            # Extract decision number
            num_match = re.search(r'nr\.\s*([\d\-\(\)\s]+)', header, re.IGNORECASE)
            decision_num = num_match.group(1).strip() if num_match else None

            # Extract date
            date = None
            date_match = re.search(r'dat[ëe]\s+(\d{1,2})\.(\d{1,2})\.(\d{4})', header)
            if date_match:
                day, month, year = date_match.groups()
                date = f"{year}-{month.zfill(2)}-{day.zfill(2)}"

            # Extract college
            college = 'unknown'
            header_lower = header.lower()
            if 'civil' in header_lower:
                college = 'civil'
            elif 'penal' in header_lower:
                college = 'penal'
            elif 'administrat' in header_lower:
                college = 'administrative'
            elif 'bashk' in header_lower:
                college = 'united'

            # Extract maxima
            maxima = None
            maxima_match = re.search(
                r'Maksima\s*[-–—]\s*(.+?)(?=Fjalë kyçe|Përmbledhje|$)',
                body, re.DOTALL | re.IGNORECASE)
            if maxima_match:
                maxima = maxima_match.group(1).strip()

            # Extract keywords
            keywords = []
            kw_match = re.search(
                r'Fjalë kyçe\s*[-–—]\s*(.+?)(?=Përmbledhje|$)',
                body, re.DOTALL | re.IGNORECASE)
            if kw_match:
                kw_text = kw_match.group(1).strip()
                keywords = [k.strip() for k in re.split(r'[,;]', kw_text) if k.strip()]

            full_text = f"{header}\n\n{body}"
            decision_id = (f"AL-SC-{decision_num.replace(' ', '')}"
                          if decision_num else
                          f"AL-SC-bulletin-{len(decisions)}")

            decisions.append({
                '_id': decision_id,
                '_source': self.SOURCE_ID,
                '_type': 'case_law',
                '_fetched_at': datetime.now(timezone.utc).isoformat(),
                'title': header,
                'text': full_text,
                'date': date,
                'decision_number': decision_num,
                'college': college,
                'maxima': maxima,
                'keywords': keywords,
                'url': f"https://www.gjykataelarte.gov.al/sq/lajme/buletini/{bulletin_info['slug']}",
                'document_type': 'bulletin_decision',
                'bulletin_slug': bulletin_info['slug'],
                'bulletin_title': bulletin_info.get('title', ''),
                'language': 'sq'
            })

        return decisions

    def fetch_bulletin_decisions(self, max_pages: int = 5) -> Iterator[dict]:
        """Fetch decisions from bulletins using inline HTML text (no .doc needed)."""
        slugs = self._get_bulletin_slugs(max_pages=max_pages)
        print(f"  Found {len(slugs)} bulletins", file=sys.stderr)

        for info in slugs:
            slug = info['slug']
            detail_url = f"{self.API_BASE}/sq/lajme/buletini/{slug}/page-data.json"
            detail_data = self._fetch_json(detail_url)
            if not detail_data:
                continue

            try:
                article = detail_data['result']['data']['api']['newsArticle']
                body = article.get('body', [])
            except (KeyError, TypeError):
                continue

            # Collect all inline HTML from paragraph blocks
            raw_html = ''
            for item in body:
                if 'Paragraph' in item.get('__typename', ''):
                    for c in item.get('content', []):
                        raw_html += c.get('text_sq', '')

            if not raw_html:
                continue

            decisions = self._parse_decisions_from_inline_html(raw_html, info)
            print(f"  {slug}: {len(decisions)} decisions", file=sys.stderr)
            yield from decisions

    def fetch_all(self, sample_only: bool = False) -> Iterator[dict]:
        """Fetch all court decisions."""
        seen_ids = set()
        record_count = 0
        max_sample = 15 if sample_only else float('inf')

        # Fetch from bulletins first (inline HTML — works without .doc tools)
        print("Fetching from bulletins (inline text)...", file=sys.stderr)
        for decision in self.fetch_bulletin_decisions(
                max_pages=2 if sample_only else 10):
            if record_count >= max_sample:
                break
            did = decision['_id']
            if did in seen_ids:
                continue
            seen_ids.add(did)
            yield decision
            record_count += 1

        # Fetch from archive (.doc files — requires extraction tools)
        if record_count < max_sample:
            print("\nFetching from 1999-2019 archive...", file=sys.stderr)
            for doc_info in self.fetch_archive_documents():
                if record_count >= max_sample:
                    break

                url = doc_info['url']
                print(f"  Downloading: {doc_info['title']}", file=sys.stderr)
                content = self._download_file(url)
                if not content:
                    continue

                text = self._extract_text_from_doc(content, url)
                if not text or len(text) < 500:
                    continue

                decisions = self._parse_decision_from_bundle(text, doc_info)
                for decision in decisions:
                    if record_count >= max_sample:
                        break
                    did = decision['_id']
                    if did in seen_ids:
                        continue
                    seen_ids.add(did)
                    yield decision
                    record_count += 1

                if sample_only and record_count >= max_sample:
                    break

        print(f"\nTotal records: {record_count}", file=sys.stderr)

    def fetch_updates(self, since: str) -> Iterator[dict]:
        """Fetch decisions updated since a date.

        For this source, we check recent bulletins only (inline text).
        """
        for decision in self.fetch_bulletin_decisions(max_pages=3):
            yield decision

    def normalize(self, raw: dict) -> dict:
        """Normalize a raw record to standard schema."""
        return {
            '_id': raw.get('_id', 'unknown'),
            '_source': self.SOURCE_ID,
            '_type': 'case_law',
            '_fetched_at': raw.get('_fetched_at', datetime.now(timezone.utc).isoformat()),
            'title': raw.get('title', ''),
            'text': raw.get('text', ''),
            'date': raw.get('date'),
            'url': raw.get('url', ''),
            'decision_number': raw.get('decision_number'),
            'college': raw.get('college'),
            'year': raw.get('year'),
            'month': raw.get('month'),
            'applicant': raw.get('applicant'),
            'respondent': raw.get('respondent'),
            'maxima': raw.get('maxima'),
            'keywords': raw.get('keywords'),
            'document_type': raw.get('document_type'),
            'language': 'sq'
        }

    def bootstrap(self, sample_only: bool = False):
        """Run the bootstrap process."""
        print(f"Starting {'sample' if sample_only else 'full'} bootstrap for {self.SOURCE_ID}")

        records = []
        for raw in self.fetch_all(sample_only=sample_only):
            normalized = self.normalize(raw)
            records.append(normalized)

            # Save individual sample files for inspection
            if sample_only:
                filename = f"{normalized['_id'].replace('/', '_')}.json"
                filepath = self.sample_dir / filename
                with open(filepath, 'w', encoding='utf-8') as f:
                    json.dump(normalized, f, ensure_ascii=False, indent=2)

        print(f"\nBootstrap complete: {len(records)} records")

        # Validation summary
        if records:
            text_lengths = [len(r.get('text', '')) for r in records]
            avg_length = sum(text_lengths) / len(text_lengths)
            has_text = sum(1 for r in records if r.get('text'))
            print(f"  Records with text: {has_text}/{len(records)}")
            print(f"  Avg text length: {avg_length:.0f} chars")
            print(f"  Sample files saved to: {self.sample_dir}")

            # Write to data/records.jsonl for VPS pipeline ingestion
            data_dir = Path(__file__).parent / 'data'
            data_dir.mkdir(parents=True, exist_ok=True)
            jsonl_path = data_dir / 'records.jsonl'
            with open(jsonl_path, 'w', encoding='utf-8') as f:
                for record in records:
                    f.write(json.dumps(record, ensure_ascii=False) + '\n')
            print(f"  Records written to: {jsonl_path}")

        return records


def main():
    parser = argparse.ArgumentParser(description='Albanian Supreme Court case law fetcher')
    parser.add_argument('command', choices=['bootstrap', 'fetch_updates'],
                       help='Command to run')
    parser.add_argument('--sample', action='store_true',
                       help='Only fetch sample records')
    parser.add_argument('--since', type=str,
                       help='Fetch updates since date (ISO format)')
    parser.add_argument('--output', type=str,
                       help='Output directory for sample files')
    parser.add_argument("--full", action="store_true", help="Fetch all records")

    args = parser.parse_args()

    fetcher = AlbanianSupremeCourtFetcher(sample_dir=args.output)

    if args.command == 'bootstrap':
        fetcher.bootstrap(sample_only=args.sample)
    elif args.command == 'fetch_updates':
        since = args.since or (datetime.now(timezone.utc).isoformat())
        for record in fetcher.fetch_updates(since):
            print(json.dumps(record, ensure_ascii=False))


if __name__ == '__main__':
    main()
