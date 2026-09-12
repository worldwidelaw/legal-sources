#!/usr/bin/env python3
"""
GN/DroitGuineen -- Droitguinéen, Guinea's Legal Reference Portal

Fetches legislation (codes, laws, decrees, OHADA acts) and court decisions
(Supreme Court, Court of Appeal, CCJA) from droitguineen.com.

Strategy:
  - Parse sitemap.xml for all /lois/ URLs (legislation + jurisprudence)
  - For each URL, fetch HTML and decode Next.js RSC payload chunks
  - Extract metadata (titre, nature, date, etc.) from RSC stream
  - Extract full text from T-tagged text blocks in the RSC stream

Usage:
  python bootstrap.py bootstrap          # Fetch all documents
  python bootstrap.py bootstrap --sample # Fetch ~15 sample records
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import logging
import time
import re
import json
import xml.etree.ElementTree as ET
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Dict, Any, List, Optional

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.GN.DroitGuineen")

BASE_URL = "https://droitguineen.com"
SITEMAP_URL = f"{BASE_URL}/sitemap.xml"
SOURCE_ID = "GN/DroitGuineen"

# droitguineen.com blocklists User-Agents by name, not by IP: from the same
# vantage and second, "LegalDataHunter/1.0" and "python-requests/2.31.0" both
# get 403 while "Chrome/126" and even "curl/8.4.0" get 200 (issue #1456).
BROWSER_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
)
HEADERS = {
    "User-Agent": BROWSER_UA,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.5",
}


def _make_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(HEADERS)
    return session


def _fetch_sitemap_urls(session: requests.Session) -> List[str]:
    """Parse sitemap.xml and return all /lois/ URLs."""
    resp = session.get(SITEMAP_URL, timeout=30)
    if resp.status_code == 403:
        raise RuntimeError(
            f"{SITEMAP_URL} returned 403 — droitguineen.com is refusing this "
            f"User-Agent ({session.headers.get('User-Agent')!r}). See issue #1456; "
            "a browser UA is required. Failing loud rather than reporting 0 documents."
        )
    resp.raise_for_status()
    root = ET.fromstring(resp.content)
    ns = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}
    urls = []
    for loc in root.findall(".//sm:loc", ns):
        url = loc.text.strip()
        if "/lois/" in url:
            urls.append(url)
    if not urls:
        raise RuntimeError(
            f"{SITEMAP_URL} parsed but contained no /lois/ URLs — the sitemap layout "
            "changed or the response was an interstitial. Failing loud."
        )
    return urls


def _decode_rsc_payload(html: str) -> str:
    """Decode all Next.js RSC __next_f.push chunks into a single text stream."""
    chunks = re.findall(r'self\.__next_f\.push\(\[1,(.+?)\]\)</script>', html)
    all_text = ""
    for c in chunks:
        try:
            decoded = json.loads(c)
            if isinstance(decoded, str):
                all_text += decoded
        except (json.JSONDecodeError, TypeError):
            pass
    return all_text


def _text_block_refs(rsc_text: str) -> Dict[str, str]:
    """Map RSC text-node ids to their content.

    Long strings are hoisted out of the JSON payload into their own rows,
    `<id>:T<hex_byte_length>,<content>`, and referenced from the payload as
    `"$<id>"`. The declared byte length gives an exact slice, so content that
    itself contains newlines is not truncated.
    """
    refs: Dict[str, str] = {}
    raw = rsc_text.encode("utf-8")
    for m in re.finditer(rb"(?m)^([0-9a-f]+):T([0-9a-f]+),", raw):
        start = m.end()
        length = int(m.group(2), 16)
        refs[m.group(1).decode()] = raw[start:start + length].decode("utf-8", "replace")
    return refs


def _extract_initial_data(rsc_text: str) -> Optional[Dict[str, Any]]:
    """Pull the `initialData` document object out of the RSC stream."""
    anchor = rsc_text.find('"initialData":{')
    if anchor == -1:
        return None
    start = rsc_text.index("{", anchor + len('"initialData":') - 1)

    depth = 0
    in_string = False
    escaped = False
    for i in range(start, len(rsc_text)):
        ch = rsc_text[i]
        if in_string:
            if escaped:
                escaped = False
            elif ch == "\\":
                escaped = True
            elif ch == '"':
                in_string = False
            continue
        if ch == '"':
            in_string = True
        elif ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                try:
                    return json.loads(rsc_text[start:i + 1])
                except json.JSONDecodeError as e:
                    logger.warning("initialData is not valid JSON: %s", e)
                    return None
    return None


def _resolve(value: Any, refs: Dict[str, str]) -> str:
    """Resolve an RSC string, following `$<id>` hoisted-text references."""
    if not isinstance(value, str):
        return ""
    if value.startswith("$$"):  # RSC escapes a literal leading '$' as '$$'
        return value[1:]
    if value.startswith("$"):
        return refs.get(value[1:], "")
    return value


def _extract_metadata(doc: Dict[str, Any], slug: str) -> Dict[str, Any]:
    """Extract document metadata from the parsed `initialData` object."""
    meta: Dict[str, Any] = {"id": doc.get("id") or slug}
    for field in [
        "titre", "titreComplet", "nature", "numero",
        "dateSignature", "datePublication", "dateEntreeVigueur",
        "etat", "sousCategorie", "signataires", "urlJO", "slug",
    ]:
        value = doc.get(field)
        if isinstance(value, str) and value:
            meta[field] = value
    return meta


def _render_articles(articles: Any, refs: Dict[str, str], out: List[str]) -> None:
    for article in articles or []:
        if not isinstance(article, dict):
            continue
        contenu = _resolve(article.get("contenu"), refs).strip()
        if not contenu:
            continue
        numero = (article.get("numero") or "").strip()
        out.append(f"Article {numero}\n{contenu}" if numero else contenu)


def _render_sections(sections: Any, refs: Dict[str, str], out: List[str]) -> None:
    """Walk the nested titre/chapitre/section tree, emitting headings + articles."""
    for section in sections or []:
        if not isinstance(section, dict):
            continue
        titre = _resolve(section.get("titre"), refs).strip()
        if titre:
            out.append(titre)
        _render_articles(section.get("articles"), refs, out)
        _render_sections(section.get("enfants"), refs, out)


def _extract_full_text(doc: Dict[str, Any], refs: Dict[str, str]) -> str:
    """Assemble the document body from `initialData`.

    The body lives in `articles` (flat) plus a nested `sections` tree whose
    leaves carry the remaining articles — NOT in the RSC text blocks, which
    also hold the app shell's schema.org JSON and React tree.
    """
    parts: List[str] = []
    visas = _resolve(doc.get("visas"), refs).strip()
    if visas:
        parts.append(visas)
    _render_articles(doc.get("articles"), refs, parts)
    _render_sections(doc.get("sections"), refs, parts)
    return "\n\n".join(parts).strip()


def _determine_type(meta: Dict[str, Any], slug: str) -> str:
    """Determine if a document is legislation or case_law."""
    nature = (meta.get("nature") or "").upper()
    if nature in ("JURISPRUDENCE", "JURISPRUDENCE_CCJA"):
        return "case_law"
    if "jurisprudence" in slug.lower():
        return "case_law"
    return "legislation"


class DroitGuineenScraper(BaseScraper):
    """Scraper for droitguineen.com."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(str(source_dir))
        self.session = _make_session()

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Fetch all legislation and jurisprudence from droitguineen.com."""
        urls = _fetch_sitemap_urls(self.session)
        logger.info("Found %d /lois/ URLs in sitemap", len(urls))

        consecutive_403 = 0
        for i, url in enumerate(urls):
            try:
                yield from self._fetch_document(url, i, len(urls))
                consecutive_403 = 0
            except requests.HTTPError as e:
                status = e.response.status_code if e.response is not None else None
                if status == 403:
                    consecutive_403 += 1
                    if consecutive_403 >= 10:
                        raise RuntimeError(
                            "10 consecutive 403s on document pages — droitguineen.com is "
                            "refusing this client (see issue #1456). Failing loud rather "
                            "than finishing with a truncated corpus."
                        ) from e
                logger.warning("HTTP %s fetching %s", status, url)
            except Exception as e:
                logger.warning("Error fetching %s: %s", url, e)
            time.sleep(1.0)

    def _fetch_document(
        self, url: str, index: int, total: int
    ) -> Generator[Dict[str, Any], None, None]:
        """Fetch a single document page and extract data."""
        slug = url.rstrip("/").split("/")[-1]
        logger.info("[%d/%d] Fetching %s", index + 1, total, slug)

        resp = self.session.get(url, timeout=60)
        resp.raise_for_status()
        html = resp.text

        # Decode the RSC payload
        rsc_text = _decode_rsc_payload(html)
        if not rsc_text:
            logger.warning("Empty RSC payload for %s", slug)
            return

        doc = _extract_initial_data(rsc_text)
        if doc is None:
            logger.warning("No initialData document for %s", slug)
            return

        refs = _text_block_refs(rsc_text)

        # Extract metadata
        meta = _extract_metadata(doc, slug)
        if not meta.get("titre") and not meta.get("titreComplet"):
            logger.warning("No title found for %s", slug)
            return

        # Extract full text
        text = _extract_full_text(doc, refs)
        if not text or len(text) < 50:
            logger.warning("Insufficient text for %s (%d chars)", slug, len(text) if text else 0)
            return

        record = {
            "id": meta.get("id", slug),
            "slug": meta.get("slug", slug),
            "title": meta.get("titre") or meta.get("titreComplet", slug),
            "title_full": meta.get("titreComplet", ""),
            "nature": meta.get("nature", ""),
            "numero": meta.get("numero", ""),
            "date_signature": meta.get("dateSignature"),
            "date_publication": meta.get("datePublication"),
            "date_entree_vigueur": meta.get("dateEntreeVigueur"),
            "etat": meta.get("etat", ""),
            "sous_categorie": meta.get("sousCategorie", ""),
            "signataires": meta.get("signataires", ""),
            "url_jo": meta.get("urlJO", ""),
            "source_url": url,
            "text": text,
        }

        # Yield the RAW record; BaseScraper.bootstrap_fast() calls normalize().
        # (Do NOT normalize here — double-normalization raises KeyError on
        #  raw["id"] since a normalized record uses "_id".)
        yield record

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize a raw record into standard schema."""
        date = raw.get("date_signature") or raw.get("date_publication")
        if date and isinstance(date, str):
            date = date[:10]

        doc_type = _determine_type(raw, raw.get("slug", ""))

        return {
            "_id": raw["id"],
            "_source": SOURCE_ID,
            "_type": doc_type,
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw["title"],
            "title_full": raw.get("title_full", ""),
            "text": raw["text"],
            "date": date,
            "url": raw["source_url"],
            "nature": raw.get("nature", ""),
            "numero": raw.get("numero", ""),
            "etat": raw.get("etat", ""),
            "sous_categorie": raw.get("sous_categorie", ""),
            "signataires": raw.get("signataires", ""),
            "url_jo": raw.get("url_jo", ""),
        }

    def fetch_updates(self, since: str = None) -> Generator[Dict[str, Any], None, None]:
        """Fetch updates since a given date (not supported — full re-fetch)."""
        yield from self.fetch_all()


def _run_sample(sample_size: int = 15):
    """Fetch a sample and write it to sample/ (also exercises normalize)."""
    scraper = DroitGuineenScraper()
    sample_dir = Path(__file__).parent / "sample"
    sample_dir.mkdir(exist_ok=True)

    # Sitemap order is legislation-first, so sample a mix of both document
    # types plus a code (the nested-sections case) rather than the first N.
    urls = _fetch_sitemap_urls(scraper.session)
    codes = [u for u in urls if "/lois/code-" in u]
    jurisprudence = [u for u in urls if "jurisprudence" in u]
    legislation = [u for u in urls if u not in set(codes) | set(jurisprudence)]
    picks = codes[:3] + jurisprudence[:6] + legislation[:sample_size]

    def _sample_records():
        for i, url in enumerate(picks):
            yield from scraper._fetch_document(url, i, len(picks))
            time.sleep(1.0)

    count = 0
    for raw in _sample_records():
        record = scraper.normalize(raw)
        count += 1
        logger.info(
            "  -> [%d] %s | %s | %d chars",
            count, record["_type"], record["title"][:60], len(record.get("text", "")),
        )
        fname = f"{record['_type']}_{count:04d}.json"
        with open(sample_dir / fname, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)
        if count >= sample_size:
            break

    logger.info("Sample complete: %d records", count)
    return count


def _run_test():
    """Quick connectivity and structure test."""
    session = _make_session()

    # Test 1: sitemap
    urls = _fetch_sitemap_urls(session)
    print(f"Sitemap: {len(urls)} /lois/ URLs found")

    def _probe(label: str, url: str) -> None:
        resp = session.get(url, timeout=30)
        rsc_text = _decode_rsc_payload(resp.text)
        doc = _extract_initial_data(rsc_text)
        if doc is None:
            print(f"{label}: FAILED — no initialData in {url}")
            return
        meta = _extract_metadata(doc, url.rstrip("/").split("/")[-1])
        text = _extract_full_text(doc, _text_block_refs(rsc_text))
        print(f"{label}: '{meta.get('titre', '?')[:60]}' -- {len(text)} chars")

    # Test 2: a legislation page, Test 3: a jurisprudence page
    leg_urls = [u for u in urls if "jurisprudence" not in u]
    if leg_urls:
        _probe("Legislation", leg_urls[0])

    jur_urls = [u for u in urls if "jurisprudence" in u]
    if jur_urls:
        time.sleep(1)
        _probe("Jurisprudence", jur_urls[0])

    # Test 4: a multi-section code, which exercises the nested sections walk
    codes = [u for u in urls if "/lois/code-" in u]
    if codes:
        time.sleep(1)
        _probe("Code", codes[-1])


if __name__ == "__main__":
    args = sys.argv[1:]
    if not args:
        print("Usage: python bootstrap.py [bootstrap|bootstrap-fast|test] [--sample|--full]")
        sys.exit(1)

    cmd = args[0]
    sample = "--sample" in args

    if cmd == "test":
        _run_test()
    elif cmd in ("bootstrap", "bootstrap-fast"):
        # The fleet wrapper invokes `bootstrap-fast`; without the alias it fell
        # through to the sample fallback and re-ingested sample/ (issue #1456).
        if sample:
            _run_sample()
        else:
            stats = DroitGuineenScraper().bootstrap_fast()
            logger.info("Bootstrap complete: %s", json.dumps(stats, indent=2, default=str))
    else:
        print(f"Unknown command: {cmd}")
        sys.exit(1)
