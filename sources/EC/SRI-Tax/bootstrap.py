#!/usr/bin/env python3
"""
EC/SRI-Tax -- Ecuador Servicio de Rentas Internas Tax Guidance

Fetches tax doctrine from Ecuador's SRI (Internal Revenue Service):
  - Extractos de las Absoluciones de Consultas Tributarias (2014-present)
  - Normativa Institucional Vigente

Strategy:
  - Discover the compilation PDFs from the live "extractos de consultas" page
    rather than a frozen URL list, so a newly published year is picked up on the
    next run. A curated list of known documents is merged in as a floor, and the
    page also serves year archives as .zip, whose PDFs are unpacked.
  - Each compilation bundles a whole year of rulings. Split it into one record
    per ruling on its `Oficio:` line — SRI's own per-ruling identifier — so the
    corpus is individually addressable rather than 14 opaque year-blobs. A
    document whose layout does not split is emitted whole, never dropped.

Source: https://www.sri.gob.ec/extractos-de-consultas
Auth: none (open data)

Usage:
  python bootstrap.py bootstrap            # Sample pull (15 records)
  python bootstrap.py bootstrap --full     # Full pull to data/records.jsonl
  python bootstrap.py bootstrap-fast       # Full pull, concurrent normalize
  python bootstrap.py update --since DATE  # Incremental update
  python bootstrap.py test-api             # Quick connectivity test
"""

import io
import sys
import json
import logging
import re
import zipfile
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, List
from urllib.parse import unquote, urljoin

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.EC.SRI-Tax")

BASE_URL = "https://www.sri.gob.ec"
INDEX_URL = f"{BASE_URL}/extractos-de-consultas"
DOWNLOAD_BASE = f"{BASE_URL}/o/sri-portlet-biblioteca-alfresco-internet/descargar"

# Known documents, kept as a floor under the live discovery: the Normativa
# Institucional Vigente lives off a different page and would otherwise be lost,
# and a page redesign must not silently shrink the corpus to nothing.
CURATED_DOCUMENTS = [
    ("c536bac6-7315-4c49-975a-3d55c60f4329", "Extractos+consultas+enero+-+diciembre+2014.pdf"),
    ("fd4c7aff-52c7-4b9d-b0c6-29f53b314893", "Extractos+consultas+enero+-+diciembre+2015.pdf"),
    ("5e828f65-743f-49a4-a769-bf3af882bdb2", "Extractos+consultas+enero+-+diciembre+2016.pdf"),
    ("238d9a6d-5f50-426d-bf8c-6a3c21ec10c9", "Extractos%20consulta%20enero%20-%20diciembre%202017.pdf"),
    ("40977c93-2ffd-4b64-9077-0fe3cade0218", "Extractos%20consultas%20enero%20-%20diciembre%202018.pdf"),
    ("9c3cdf84-5102-4cba-9344-129a6ab5be62", "Extractos%20consulta%20enero%20-%20diciembre%202019.pdf"),
    ("d074de83-517d-4a49-ac1b-7d7dc3ade3a3", "Extractos%20consulta%20enero%20-%20diciembre%202020.pdf"),
    ("240015d5-7c72-4ef5-8b75-20464168ee28", "Extractos%20consultas%20enero%20-%20diciembre%202021.pdf"),
    ("3dc62943-ac90-492e-a6d4-5d62b37fee10", "Extractos%20consultas%20enero%20-%20diciembre%202022.pdf"),
    ("4dd8cd7f-ba47-4206-b6c2-8ed4c367b46c", "EXTRACTOS_CONSULTAS_TRIBUTARIAS_FORMALES%202023.pdf"),
    ("41a82537-efa4-4e84-9d45-326f57dd8199", "EXTRACTOS_CONSULTAS_VINCULANTES_ISEM2024.pdf"),
    ("fd3fa4a9-ab76-4426-9f4f-08dee57993d8", "normativa_institucional_vigente.pdf"),
]

# A compilation that splits into fewer than this is treated as an unrecognized
# layout and emitted whole rather than as a couple of mis-cut fragments.
MIN_SEGMENTS = 5

DOWNLOAD_RE = re.compile(r'href="([^"]*?/descargar/[^"]+)"')
OFICIO_RE = re.compile(r'^[ \t]*Oficio[:\s]+(?P<oficio>[A-Za-z0-9][A-Za-z0-9\-/.]{5,})[ \t]*$')
DATE_RE = re.compile(
    r'^[ \t]*(?:Fecha[:\s]+)?(?P<d>\d{1,2})\s+de\s+(?P<m>[A-Za-zÁÉÍÓÚáéíóúñ]+)\s+de[l]?\s+(?P<y>\d{4})'
    r'[ \t]*\.?[ \t]*$',
    re.I,
)
REFERENCE_RE = re.compile(r'^[ \t]*Referencia[:\s]+(?P<ref>.{3,200}?)[ \t]*$', re.M)
YEAR_RE = re.compile(r'(20\d{2})')

MONTHS = {
    m: i
    for i, m in enumerate(
        "enero febrero marzo abril mayo junio julio agosto "
        "septiembre octubre noviembre diciembre".split(),
        1,
    )
}
MONTHS["setiembre"] = 9


def _iso_date(line: str) -> Optional[str]:
    """Parse a Spanish date line ('16 de ENERO de 2014') into ISO 8601."""
    m = DATE_RE.match(line or "")
    if not m:
        return None
    month = MONTHS.get(m.group("m").lower())
    if not month:
        return None
    return f"{m.group('y')}-{month:02d}-{int(m.group('d')):02d}"


def _slug(value: str, limit: int = 60) -> str:
    return re.sub(r"[^A-Za-z0-9]+", "-", value).strip("-")[:limit]


def _pretty_name(filename: str) -> str:
    """Human title from an Alfresco filename."""
    name = unquote(filename).rsplit("/", 1)[-1]
    name = re.sub(r"\.(pdf|zip)$", "", name, flags=re.I)
    name = re.sub(r"[-_+]+", " ", name)
    name = re.sub(r"\s*-?signed\s*", " ", name, flags=re.I)
    return re.sub(r"\s+", " ", name).strip()


def split_consultas(text: str) -> List[dict]:
    """
    Cut a yearly compilation into its individual rulings.

    Every ruling carries an `Oficio:` line, and the line above it holds the
    signing date — bare in the 2014-2018 layout, prefixed `Fecha:` from 2019 on.
    Anchoring on `Oficio:` and treating the date line as optional covers both.
    """
    lines = text.split("\n")
    anchors = []
    for i, line in enumerate(lines):
        m = OFICIO_RE.match(line)
        if not m:
            continue
        start, date = i, None
        if i and _iso_date(lines[i - 1]):
            start, date = i - 1, _iso_date(lines[i - 1])
        anchors.append((start, m.group("oficio"), date))

    segments = []
    for k, (start, oficio, date) in enumerate(anchors):
        end = anchors[k + 1][0] if k + 1 < len(anchors) else len(lines)
        body = "\n".join(lines[start:end]).strip()
        if not body:
            continue
        ref = REFERENCE_RE.search(body)
        segments.append(
            {
                "oficio": oficio,
                "date": date,
                "text": body,
                "reference": ref.group("ref").strip() if ref else "",
            }
        )
    return segments


class SRITaxScraper(BaseScraper):
    """
    Scraper for EC/SRI-Tax -- Ecuador SRI Tax Doctrine.
    Country: EC
    URL: https://www.sri.gob.ec/
    Data types: doctrine
    Auth: none
    """

    SOURCE_ID = "EC/SRI-Tax"

    def __init__(self, source_dir=None):
        super().__init__(source_dir or str(Path(__file__).parent))
        self.client = HttpClient(
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                              "AppleWebKit/537.36 (KHTML, like Gecko) "
                              "Chrome/120.0.0.0 Safari/537.36",
            },
            timeout=(15, 180),
        )
        # Sample runs cap how many rulings each compilation contributes so the
        # sample spans years instead of being the first 15 rulings of 2014.
        self.max_per_doc: Optional[int] = None
        self._seen_ids: set = set()

    # ── Discovery ─────────────────────────────────────────────────────

    def _discover_documents(self) -> List[dict]:
        """Compilation URLs from the live index, merged with the curated floor."""
        found: List[str] = []
        try:
            resp = self.client.get(INDEX_URL)
            resp.raise_for_status()
            found = [urljoin(BASE_URL, h) for h in DOWNLOAD_RE.findall(resp.text)]
            logger.info(f"Discovered {len(found)} download links on the live index")
        except Exception as e:
            logger.warning(f"Live index unavailable ({e}) — using the curated list only")

        urls = list(dict.fromkeys(found))
        for uuid, filename in CURATED_DOCUMENTS:
            url = f"{DOWNLOAD_BASE}/{uuid}/{filename}"
            if url not in urls:
                urls.append(url)

        docs = []
        for url in urls:
            filename = url.rsplit("/", 1)[-1]
            if not re.search(r"\.(pdf|zip)$", filename, re.I):
                continue
            title = _pretty_name(filename)
            year = YEAR_RE.findall(unquote(filename))
            docs.append(
                {
                    "url": url,
                    "filename": filename,
                    "title": title,
                    "year": int(year[-1]) if year else None,
                    "is_zip": filename.lower().endswith(".zip"),
                }
            )
        logger.info(f"{len(docs)} compilations to process")
        return docs

    def _fetch(self, url: str) -> Optional[bytes]:
        try:
            resp = self.client.get(url)
            resp.raise_for_status()
        except Exception as e:
            logger.warning(f"Download failed {url}: {e}")
            return None
        return resp.content or None

    def _pdfs_for(self, doc: dict) -> List[tuple]:
        """Yield (label, pdf_bytes) for a document — unpacking a .zip archive."""
        blob = self._fetch(doc["url"])
        if not blob:
            return []

        if not doc["is_zip"]:
            return [(doc["title"], blob)]

        try:
            archive = zipfile.ZipFile(io.BytesIO(blob))
        except zipfile.BadZipFile as e:
            logger.warning(f"Not a readable zip: {doc['url']} ({e})")
            return []

        members = []
        for name in archive.namelist():
            if not name.lower().endswith(".pdf"):
                continue
            try:
                members.append((_pretty_name(name), archive.read(name)))
            except Exception as e:
                logger.warning(f"Could not read {name} from {doc['url']}: {e}")
        logger.info(f"Unpacked {len(members)} PDFs from {doc['filename']}")
        return members

    # ── Fetching ──────────────────────────────────────────────────────

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield one raw record per individual ruling, with full text."""
        docs = self._discover_documents()
        if not docs:
            raise RuntimeError("No SRI compilations found on the live index or in the curated list")

        emitted = 0
        for doc in docs:
            for label, pdf_bytes in self._pdfs_for(doc):
                # force=True: these are a handful of large compilations that must
                # be re-read on every run to re-cut the rulings inside them. The
                # skip-if-already-in-Neon guard is keyed on the compilation, not
                # the ruling, so honouring it emitted nothing at all (#1577).
                text = extract_pdf_markdown(
                    source=self.SOURCE_ID,
                    source_id=_slug(label),
                    pdf_bytes=pdf_bytes,
                    table="doctrine",
                    force=True,
                )
                if not text or not text.strip():
                    logger.warning(f"No extractable text: {label}")
                    continue

                for raw in self._records_for(doc, label, text):
                    emitted += 1
                    yield raw

        if not emitted:
            raise RuntimeError("Extracted 0 records from the SRI compilations")
        logger.info(f"Fetch complete: {emitted} records")

    def _records_for(self, doc: dict, label: str, text: str) -> Generator[dict, None, None]:
        """Split one compilation into rulings, or emit it whole if it won't split."""
        segments = split_consultas(text)
        year = doc.get("year") or (YEAR_RE.findall(label) or [None])[-1]

        if len(segments) < MIN_SEGMENTS:
            logger.info(f"{label}: unrecognized layout ({len(segments)} segments) — emitting whole")
            yield {
                "doc_id": _slug(label),
                "title": label,
                "text": text,
                "date": f"{year}-01-01" if year else None,
                "url": doc["url"],
                "year": year,
                "oficio": "",
                "reference": "",
                "document_type": "normativa" if "normativa" in label.lower() else "compilacion",
                "compilation": label,
            }
            return

        logger.info(f"{label}: {len(segments)} rulings")
        count = 0
        for i, seg in enumerate(segments):
            if self.max_per_doc is not None and count >= self.max_per_doc:
                break
            count += 1

            # Oficio numbers repeat across a handful of rulings, so fall back to
            # the position within the compilation to keep _id unique.
            doc_id = _slug(seg["oficio"]) or f"{_slug(label)}-{i:04d}"
            if doc_id in self._seen_ids:
                doc_id = f"{doc_id}-{i:04d}"
            self._seen_ids.add(doc_id)

            title = seg["reference"] or f"Consulta tributaria {seg['oficio']}"
            yield {
                "doc_id": doc_id,
                "title": f"Absolución de consulta {seg['oficio']} — {title}",
                "text": seg["text"],
                "date": seg["date"] or (f"{year}-01-01" if year else None),
                "url": doc["url"],
                "year": year,
                "oficio": seg["oficio"],
                "reference": seg["reference"],
                "document_type": "consulta_tributaria",
                "compilation": label,
            }

    def fetch_updates(self, since) -> Generator[dict, None, None]:
        """Yield rulings signed on or after `since`."""
        if isinstance(since, str):
            since = datetime.strptime(since[:10], "%Y-%m-%d").replace(tzinfo=timezone.utc)
        cutoff = since.strftime("%Y-%m-%d")
        for raw in self.fetch_all():
            if raw.get("date") and raw["date"] >= cutoff:
                yield raw

    def normalize(self, raw: dict) -> Optional[dict]:
        """Transform a raw ruling into the standard schema."""
        text = (raw.get("text") or "").strip()
        if not text:
            return None

        return {
            "_id": f"EC-SRI-{raw['doc_id']}",
            "_source": self.SOURCE_ID,
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw["title"][:500],
            "text": text,
            "date": raw.get("date"),
            "url": raw["url"],
            "institution": "Servicio de Rentas Internas (SRI)",
            "document_type": raw.get("document_type", "consulta_tributaria"),
            "oficio": raw.get("oficio", ""),
            "reference": raw.get("reference", ""),
            "compilation": raw.get("compilation", ""),
            "year": raw.get("year"),
        }

    def test_api(self) -> bool:
        """Quick connectivity and split test."""
        logger.info("Testing EC/SRI-Tax document access...")
        docs = self._discover_documents()
        if not docs:
            logger.error("No compilations discovered")
            return False

        for doc in docs:
            for label, pdf_bytes in self._pdfs_for(doc):
                text = extract_pdf_markdown(
                    source=self.SOURCE_ID,
                    source_id=_slug(label),
                    pdf_bytes=pdf_bytes,
                    table="doctrine",
                    force=True,
                )
                if not text:
                    continue
                segments = split_consultas(text)
                logger.info(
                    f"OK: {label} — {len(pdf_bytes)} bytes, {len(text)} chars, "
                    f"{len(segments)} rulings"
                )
                logger.info("All tests passed")
                return True

        logger.error("No document yielded extractable text")
        return False


# -- CLI entry point ---------------------------------------------------------

def main():
    import argparse

    parser = argparse.ArgumentParser(description="EC/SRI-Tax -- Ecuador SRI tax doctrine")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "update", "test-api", "test"],
    )
    parser.add_argument("--sample", action="store_true", help="Sample mode (default)")
    parser.add_argument("--full", action="store_true", help="Fetch the whole corpus")
    parser.add_argument("--count", type=int, default=15, help="Sample size")
    parser.add_argument("--since", help="update: only rulings signed since YYYY-MM-DD")
    args = parser.parse_args()

    scraper = SRITaxScraper()

    if args.command in ("test-api", "test"):
        sys.exit(0 if scraper.test_api() else 1)

    if args.command == "bootstrap-fast":
        stats = scraper.bootstrap_fast()
        logger.info(f"Bootstrap-fast complete: {json.dumps(stats, indent=2, default=str)}")

    elif args.command == "bootstrap":
        sample_mode = args.sample or not args.full
        if sample_mode:
            scraper.max_per_doc = 3
        stats = scraper.bootstrap(sample_mode=sample_mode, sample_size=args.count)
        logger.info(f"Bootstrap complete: {json.dumps(stats, indent=2, default=str)}")

    elif args.command == "update":
        since = (
            datetime.strptime(args.since, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            if args.since
            else datetime(datetime.now(timezone.utc).year - 1, 1, 1, tzinfo=timezone.utc)
        )
        saved = 0
        for raw in scraper.fetch_updates(since):
            record = scraper.normalize(raw)
            if record:
                scraper.storage.write(scraper._dedup_key(record), record)
                saved += 1
        scraper.storage.flush()
        logger.info(f"Update complete: {saved} records written")


if __name__ == "__main__":
    main()
