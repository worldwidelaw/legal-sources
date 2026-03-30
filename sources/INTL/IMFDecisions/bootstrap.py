#!/usr/bin/env python3
"""
INTL/IMFDecisions -- IMF Selected Decisions & Legal Framework

Fetches Executive Board decisions from the Wayback Machine archive of the IMF
SelectedDecisions application (~347 decisions from the 39th edition, 2017).

Strategy:
  - Parse DecisionsList.aspx from Wayback (2018 capture) for decision IDs + titles
  - Fetch each Description.aspx page from Wayback (2019 captures) for full text
  - Extract metadata (decision number, title, article section) and clean HTML

Usage:
    python bootstrap.py bootstrap --sample   # Fetch 15 sample records
    python bootstrap.py bootstrap            # Full fetch all decisions
    python bootstrap.py test                 # Quick connectivity test
"""

import json
import logging
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator, Optional
from urllib.parse import quote, unquote

import requests
from bs4 import BeautifulSoup

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.INTL.IMFDecisions")

LIST_URL = (
    "https://web.archive.org/web/20180322142610/"
    "http://www.imf.org:80/external/SelectedDecisions/DecisionsList.aspx"
)
DETAIL_URL_TEMPLATE = (
    "https://web.archive.org/web/2019/"
    "https://www.imf.org/external/SelectedDecisions/Description.aspx?decision={}"
)
RATE_LIMIT = 2


def _extract_date_from_text(text: str) -> Optional[str]:
    """Try to extract a date from decision text (adoption date patterns)."""
    patterns = [
        r'(?:adopted|effective|approved)\s+(?:on\s+)?(\w+\s+\d{1,2},?\s+\d{4})',
        r'(\w+\s+\d{1,2},?\s+\d{4})',
        r'(\d{1,2}\s+\w+\s+\d{4})',
    ]
    month_map = {
        'january': '01', 'february': '02', 'march': '03', 'april': '04',
        'may': '05', 'june': '06', 'july': '07', 'august': '08',
        'september': '09', 'october': '10', 'november': '11', 'december': '12',
    }
    for pattern in patterns:
        m = re.search(pattern, text, re.IGNORECASE)
        if m:
            date_str = m.group(1).strip().rstrip(',')
            # Try Month DD, YYYY
            dm = re.match(r'(\w+)\s+(\d{1,2}),?\s+(\d{4})', date_str)
            if dm:
                month = month_map.get(dm.group(1).lower())
                if month:
                    return f"{dm.group(3)}-{month}-{int(dm.group(2)):02d}"
            # Try DD Month YYYY
            dm = re.match(r'(\d{1,2})\s+(\w+)\s+(\d{4})', date_str)
            if dm:
                month = month_map.get(dm.group(2).lower())
                if month:
                    return f"{dm.group(3)}-{month}-{int(dm.group(1)):02d}"
    return None


class IMFDecisionsScraper(BaseScraper):
    """Scraper for INTL/IMFDecisions."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
            "Accept": "text/html,application/xhtml+xml,*/*",
        })

    def _fetch_decision_list(self) -> list[dict]:
        """Parse the DecisionsList page for decision IDs and titles."""
        resp = self.session.get(LIST_URL, timeout=60, allow_redirects=True)
        resp.raise_for_status()

        soup = BeautifulSoup(resp.text, 'html.parser')
        links = soup.find_all('a', href=re.compile(r'Description\.aspx\?decision='))

        decisions = []
        seen = set()
        for link in links:
            href = link.get('href', '')
            m = re.search(r'decision=([^&]+)', href)
            if not m or m.group(1) == 'list':
                continue

            decision_id = unquote(m.group(1))
            if decision_id in seen:
                continue
            seen.add(decision_id)

            # Title is in the same table row, cell index 1
            row = link.find_parent('tr')
            title = ""
            if row:
                cells = row.find_all('td')
                if len(cells) >= 3:
                    title = cells[1].get_text(strip=True)
                elif len(cells) >= 2:
                    title = cells[0].get_text(strip=True)

            # Article section from parent heading
            article_section = ""
            heading = link.find_previous(['h2', 'h3', 'h4'])
            if heading:
                article_section = heading.get_text(strip=True)

            decisions.append({
                "decision_id": decision_id,
                "title": title,
                "article_section": article_section,
            })

        logger.info(f"Found {len(decisions)} decisions in list page")
        return decisions

    def _fetch_decision_text(self, decision_id: str) -> Optional[str]:
        """Fetch full text of a decision from Wayback Machine."""
        url = DETAIL_URL_TEMPLATE.format(quote(decision_id, safe=''))
        try:
            resp = self.session.get(url, timeout=60, allow_redirects=True)
            resp.raise_for_status()
        except requests.RequestException as e:
            logger.warning(f"Error fetching decision {decision_id}: {e}")
            return None

        soup = BeautifulSoup(resp.text, 'html.parser')

        # Remove Wayback Machine toolbar
        for wb in soup.find_all('div', id='wm-ipp-base'):
            wb.decompose()

        content_div = soup.find('div', {'id': 'MainContent'})
        if not content_div:
            content_div = soup.find('div', class_='content')
        if not content_div:
            # Fallback: find the largest text block
            body = soup.find('body')
            if body:
                content_div = body

        if not content_div:
            return None

        # Clean the text
        text = content_div.get_text(separator='\n', strip=True)
        # Remove navigation artifacts
        text = re.sub(r'<Previous Document\s*', '', text)
        text = re.sub(r'Next Document>\s*', '', text)
        text = re.sub(r'Prepared by the Legal Department.*?$', '', text, flags=re.MULTILINE)
        text = re.sub(r'As updated as of.*?$', '', text, flags=re.MULTILINE)
        # Clean whitespace
        text = re.sub(r'\n\s*\n\s*\n+', '\n\n', text)
        return text.strip()

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all IMF decisions."""
        decisions = self._fetch_decision_list()
        for i, dec in enumerate(decisions):
            try:
                time.sleep(RATE_LIMIT)
                text = self._fetch_decision_text(dec["decision_id"])
                if text and len(text) > 50:
                    dec["text"] = text
                    yield dec
                    logger.info(f"Fetched {i+1}/{len(decisions)}: {dec['decision_id']}")
                else:
                    logger.warning(f"No text for decision {dec['decision_id']}")
            except Exception as e:
                logger.error(f"Error on decision {dec['decision_id']}: {e}")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """No incremental updates — Wayback archive is static."""
        return
        yield

    def normalize(self, raw: dict) -> Optional[dict]:
        """Transform raw IMF decision into standardized schema."""
        text = raw.get("text", "")
        if not text or len(text) < 50:
            return None

        decision_id = raw.get("decision_id", "")
        safe_id = re.sub(r'[^a-zA-Z0-9_-]', '_', decision_id)

        # Extract title: prefer list page title, then try first line "-- Title"
        title = raw.get("title", "")
        if not title:
            m = re.search(r'--\s*(.+?)(?:\n|$)', text)
            if m:
                title = m.group(1).strip()
        if not title:
            title = f"IMF Decision {decision_id}"

        # Clean boilerplate from text
        text = re.sub(
            r'^Selected Decisions.*?--\s*[^\n]*\n', '', text, count=1, flags=re.DOTALL
        )
        text = re.sub(r'&lt;?Previous Document\s*', '', text)
        text = re.sub(r'Next Document>?\s*', '', text)
        text = text.strip()

        date = _extract_date_from_text(text)

        return {
            "_id": f"INTL-IMF-{safe_id}",
            "_source": "INTL/IMFDecisions",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date,
            "url": f"https://www.imf.org/external/SelectedDecisions/Description.aspx?decision={decision_id}",
            "decision_no": decision_id,
            "article_section": raw.get("article_section", ""),
        }


if __name__ == "__main__":
    scraper = IMFDecisionsScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|test] [--sample]")
        sys.exit(1)

    cmd = sys.argv[1]
    sample = "--sample" in sys.argv

    if cmd == "test":
        print("Testing IMF Decisions connectivity...")
        try:
            decisions = scraper._fetch_decision_list()
            print(f"OK: Found {len(decisions)} decisions in list")
            if decisions:
                text = scraper._fetch_decision_text(decisions[0]["decision_id"])
                if text:
                    print(f"OK: First decision has {len(text)} chars of text")
                else:
                    print("FAIL: Could not fetch decision text")
                    sys.exit(1)
        except Exception as e:
            print(f"FAIL: {e}")
            sys.exit(1)

    elif cmd == "bootstrap":
        sample_dir = scraper.source_dir / "sample"
        sample_dir.mkdir(exist_ok=True)

        count = 0
        limit = 15 if sample else None

        for raw in scraper.fetch_all():
            normalized = scraper.normalize(raw)
            if normalized is None:
                continue

            count += 1
            out_path = sample_dir / f"{count:04d}.json"
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(normalized, f, ensure_ascii=False, indent=2)

            logger.info(f"Saved decision {normalized['decision_no']} ({count} total, {len(normalized['text'])} chars)")

            if limit and count >= limit:
                break

        print(f"Saved {count} records to {sample_dir}/")

    else:
        print(f"Unknown command: {cmd}")
        sys.exit(1)
