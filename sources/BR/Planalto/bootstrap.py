#!/usr/bin/env python3
"""
BR/Planalto -- Brazil Federal Legislation Fetcher

Two-step approach:
  1. Senate Open Data API lists federal norms by type and year
  2. normas.leg.br API provides structured full text in JSON-LD format
  3. Fallback: Planalto website HTML scraping when normas has no inline text

Supported legislation types: LEI (ordinary laws), LCP (complementary laws),
DEC (decrees), EMC (constitutional amendments), MPV (provisional measures),
DLG (legislative decrees).

Usage:
  python bootstrap.py bootstrap          # Full initial pull (recent year)
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import time
import xml.etree.ElementTree as ET
from html import unescape as html_unescape
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.BR.Planalto")

SENATE_API = "https://legis.senado.leg.br/dadosabertos"
NORMAS_API = "https://normas.leg.br/api/public"
PLANALTO_BASE = "https://www.planalto.gov.br"

# Map Senate tipo codes to LexML URN type segments
TIPO_TO_URN = {
    "LEI-n": "lei",
    "LCP": "lei.complementar",
    "DEC-n": "decreto",
    "MPV-ss": "medida.provisoria",
    "DLG": "decreto.legislativo",
    "EMC-n": "emenda.constitucional",
}

# Legislation types to fetch (focus on primary legislation)
LEGISLATION_TYPES = ["LEI-n", "LCP", "DEC-n", "EMC-n", "MPV-ss", "DLG"]


def _ato_range(year: int) -> str:
    """Get the Ato year range for Planalto URLs (4-year blocks starting 2000)."""
    if year < 2000:
        return ""
    start = year - ((year - 2000) % 4) + 2000 if (year - 2000) % 4 != 0 else year
    # Blocks: 2000-2002, 2003-2006, 2007-2010, 2011-2014, 2015-2018, 2019-2022, 2023-2026
    ranges = [
        (2000, 2002), (2003, 2006), (2007, 2010), (2011, 2014),
        (2015, 2018), (2019, 2022), (2023, 2026), (2027, 2030),
    ]
    for s, e in ranges:
        if s <= year <= e:
            return f"{s}-{e}"
    return f"{year}-{year+3}"


class PlanaltoScraper(BaseScraper):
    """Scraper for BR/Planalto -- Brazilian Federal Legislation."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        try:
            from common.http_client import HttpClient
            self.client = HttpClient(timeout=60)
        except ImportError:
            self.client = None

    def _http_get(self, url: str, params: dict = None, accept: str = None,
                  timeout: int = 60, encoding: str = None) -> Optional[bytes]:
        """Make HTTP GET request and return raw bytes."""
        for attempt in range(3):
            try:
                if self.client:
                    headers = {}
                    if accept:
                        headers["Accept"] = accept
                    headers["User-Agent"] = "LegalDataHunter/1.0"
                    resp = self.client.get(url, params=params, headers=headers)
                    if resp.status_code == 200:
                        return resp.content
                    logger.warning(f"HTTP {resp.status_code} for {url}")
                    if resp.status_code in (403, 404):
                        return None
                else:
                    import urllib.request
                    import urllib.parse
                    full_url = url
                    if params:
                        qs = urllib.parse.urlencode(params, doseq=True)
                        full_url = f"{url}?{qs}"
                    req = urllib.request.Request(full_url)
                    req.add_header("User-Agent", "LegalDataHunter/1.0")
                    if accept:
                        req.add_header("Accept", accept)
                    with urllib.request.urlopen(req, timeout=timeout) as resp:
                        return resp.read()
            except Exception as e:
                logger.warning(f"Attempt {attempt+1} failed for {url}: {e}")
                if attempt < 2:
                    time.sleep(3 * (attempt + 1))
        return None

    def _senate_list_year(self, ano: int) -> List[Dict[str, str]]:
        """List all legislation from Senate API for a given year."""
        url = f"{SENATE_API}/legislacao/lista"
        raw = self._http_get(url, params={"sigla": "LEI", "ano": str(ano)})
        if not raw:
            return []

        try:
            root = ET.fromstring(raw)
        except ET.ParseError as e:
            logger.warning(f"XML parse error for {ano}: {e}")
            return []

        results = []
        for doc in root.findall(".//documento"):
            tipo = doc.findtext("tipo", "")
            if tipo not in TIPO_TO_URN:
                continue
            item = {
                "id": doc.get("id", ""),
                "tipo": tipo,
                "numero": doc.findtext("numero", ""),
                "normaNome": doc.findtext("normaNome", ""),
                "ementa": doc.findtext("ementa", ""),
                "dataassinatura": doc.findtext("dataassinatura", ""),
            }
            results.append(item)
        return results

    def _build_urn(self, doc: Dict[str, str]) -> Optional[str]:
        """Construct LexML URN from Senate API document metadata."""
        tipo = doc.get("tipo", "")
        urn_type = TIPO_TO_URN.get(tipo)
        if not urn_type:
            return None

        date_br = doc.get("dataassinatura", "")
        numero = doc.get("numero", "")
        if not date_br or not numero:
            return None

        try:
            parts = date_br.split("/")
            date_iso = f"{parts[2]}-{parts[1]}-{parts[0]}"
        except (IndexError, ValueError):
            return None

        numero_clean = numero.replace(".", "")
        return f"urn:lex:br:federal:{urn_type}:{date_iso};{numero_clean}"

    def _fetch_full_text_normas(self, urn: str) -> Optional[str]:
        """Fetch full text from normas.leg.br API (works best for LEI/LCP)."""
        url = f"{NORMAS_API}/normas"
        params = {"urn": urn, "tipo_documento": "maior-detalhe"}
        raw = self._http_get(url, params=params, accept="application/json",
                             timeout=30)
        if not raw:
            return None

        try:
            data = json.loads(raw.decode("utf-8", errors="replace"))
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            logger.warning(f"JSON parse error for {urn}: {e}")
            return None

        # Collect text from hasPart/workExample structure
        texts = []
        self._collect_texts(data, texts)
        if texts:
            return "\n".join(texts)
        return None

    def _build_planalto_url(self, doc: Dict[str, str]) -> Optional[str]:
        """Construct Planalto URL from document metadata."""
        tipo = doc.get("tipo", "")
        numero = doc.get("numero", "").replace(".", "")
        date_br = doc.get("dataassinatura", "")

        try:
            parts = date_br.split("/")
            year = int(parts[2])
        except (IndexError, ValueError):
            return None

        ato = _ato_range(year)

        if tipo == "LEI-n":
            return f"{PLANALTO_BASE}/ccivil_03/_Ato{ato}/{year}/Lei/L{numero}.htm"
        elif tipo == "DEC-n":
            return f"{PLANALTO_BASE}/ccivil_03/_Ato{ato}/{year}/Decreto/D{numero}.htm"
        elif tipo == "LCP":
            if year >= 2019:
                return f"{PLANALTO_BASE}/ccivil_03/_Ato{ato}/{year}/Lei/Lcp{numero}.htm"
            else:
                return f"{PLANALTO_BASE}/ccivil_03/LEIS/LCP/Lcp{numero}.htm"
        elif tipo == "EMC-n":
            return f"{PLANALTO_BASE}/ccivil_03/Constituicao/Emendas/Emc/emc{numero}.htm"
        elif tipo == "MPV-ss":
            return f"{PLANALTO_BASE}/ccivil_03/_Ato{ato}/{year}/Mpv/Mpv{numero}.htm"
        elif tipo == "DLG":
            return f"{PLANALTO_BASE}/ccivil_03/_Ato{ato}/{year}/Dlg/Dlg-{numero}-{year}.htm"

        return None

    def _fetch_full_text_planalto(self, doc: Dict[str, str]) -> Optional[str]:
        """Fetch full text from Planalto website as HTML fallback.

        Uses urllib directly (not HttpClient) with a single attempt to avoid
        triggering Planalto's connection-reset rate limiting.
        """
        import urllib.request
        url = self._build_planalto_url(doc)
        if not url:
            return None

        try:
            req = urllib.request.Request(url)
            req.add_header("User-Agent", "LegalDataHunter/1.0")
            with urllib.request.urlopen(req, timeout=15) as resp:
                raw = resp.read()
        except Exception as e:
            logger.debug(f"Planalto fetch failed for {url}: {e}")
            return None

        try:
            html = raw.decode("latin-1", errors="replace")
        except Exception:
            html = raw.decode("utf-8", errors="replace")

        return self._extract_text_from_planalto_html(html)

    def _extract_text_from_planalto_html(self, html: str) -> Optional[str]:
        """Extract law text from Planalto HTML page."""
        # Remove script and style blocks
        html = re.sub(r"<script[^>]*>.*?</script>", "", html, flags=re.DOTALL | re.IGNORECASE)
        html = re.sub(r"<style[^>]*>.*?</style>", "", html, flags=re.DOTALL | re.IGNORECASE)

        # Try to find the main content area
        # Planalto uses various patterns; look for the text after "Presidência da República"
        # and the main body content
        body_match = re.search(r"<body[^>]*>(.*)</body>", html, re.DOTALL | re.IGNORECASE)
        if not body_match:
            return None

        body = body_match.group(1)

        # Replace <br> and </p> with newlines
        body = re.sub(r"<br\s*/?>", "\n", body, flags=re.IGNORECASE)
        body = re.sub(r"</p>", "\n", body, flags=re.IGNORECASE)

        # Strip remaining HTML tags
        text = re.sub(r"<[^>]+>", "", body)

        # Decode HTML entities
        text = html_unescape(text)

        # Replace non-breaking spaces
        text = text.replace("\xa0", " ")

        # Clean up whitespace: collapse multiple spaces on each line, remove empty lines
        lines = []
        for line in text.split("\n"):
            line = re.sub(r"[ \t]+", " ", line).strip()
            if line:
                lines.append(line)

        text = "\n".join(lines)

        # Remove header/footer boilerplate
        # Find where the actual law content starts (usually "DECRETO", "LEI", etc.)
        for marker in ["DECRETO Nº", "DECRETO N", "LEI Nº", "LEI N", "LEI COMPLEMENTAR",
                        "EMENDA CONSTITUCIONAL", "MEDIDA PROVISÓRIA"]:
            idx = text.find(marker)
            if idx >= 0:
                text = text[idx:]
                break

        # Remove footer (usually "Este texto não substitui")
        for footer in ["Este texto não substitui", "Brasília,", "* *"]:
            idx = text.rfind(footer)
            if idx > len(text) // 2:  # Only trim if in the second half
                text = text[:idx].rstrip()
                break

        if len(text) < 50:
            return None

        return text

    def _fetch_full_text(self, doc: Dict[str, str], urn: str) -> Optional[str]:
        """Try normas.leg.br first, then fall back to Planalto HTML."""
        # Try normas.leg.br API first (structured text, best quality)
        text = self._fetch_full_text_normas(urn)
        if text and len(text) >= 50:
            return text

        # Fallback: Planalto HTML scraping
        logger.debug(f"Normas had no text for {urn}, trying Planalto...")
        time.sleep(1)
        text = self._fetch_full_text_planalto(doc)
        if text and len(text) >= 50:
            return text

        return None

    def _collect_texts(self, obj: Any, texts: List[str], depth: int = 0):
        """Recursively collect text fields from nested hasPart/workExample."""
        if depth > 20:
            return
        if isinstance(obj, dict):
            if "text" in obj and isinstance(obj["text"], str):
                clean = self._strip_xml(obj["text"]).strip()
                if clean:
                    texts.append(clean)
            for val in obj.values():
                if isinstance(val, (dict, list)):
                    self._collect_texts(val, texts, depth + 1)
        elif isinstance(obj, list):
            for item in obj:
                self._collect_texts(item, texts, depth + 1)

    @staticmethod
    def _strip_xml(text: str) -> str:
        """Remove XML/HTML tags from text."""
        clean = re.sub(r"<[^>]+>", "", text)
        clean = re.sub(r"\s+", " ", clean)
        return clean.strip()

    def _date_br_to_iso(self, date_br: str) -> str:
        """Convert dd/mm/yyyy to yyyy-mm-dd."""
        try:
            parts = date_br.split("/")
            return f"{parts[2]}-{parts[1]}-{parts[0]}"
        except (IndexError, ValueError):
            return date_br

    def normalize(self, raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        urn = raw.get("_urn", "")
        date_br = raw.get("dataassinatura", "")
        date_iso = self._date_br_to_iso(date_br)
        title = raw.get("normaNome", "")
        text = raw.get("_full_text", "")
        ementa = raw.get("ementa", "")
        tipo = raw.get("tipo", "")

        if not text or len(text) < 50:
            return None

        return {
            "_id": urn or f"BR-PL-{raw.get('id', '')}",
            "_source": "BR/Planalto",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date_iso,
            "url": f"https://normas.leg.br/?urn={urn}" if urn else "",
            "urn": urn,
            "ementa": ementa,
            "tipo": tipo,
            "numero": raw.get("numero", ""),
        }

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Fetch legislation by iterating over years.

        Prioritizes laws (LEI, LCP, EMC) over decrees since they have better
        full-text coverage in the normas.leg.br API.
        """
        current_year = datetime.now().year
        count = 0
        # Priority order: laws first (best normas coverage), then decrees
        type_priority = ["LEI-n", "LCP", "EMC-n", "MPV-ss", "DLG", "DEC-n"]

        for year in range(current_year, current_year - 5, -1):
            logger.info(f"Listing legislation for {year}...")
            docs = self._senate_list_year(year)
            logger.info(f"  Found {len(docs)} documents for {year}")
            time.sleep(1)

            # Sort docs by type priority
            type_order = {t: i for i, t in enumerate(type_priority)}
            docs.sort(key=lambda d: type_order.get(d.get("tipo", ""), 99))

            for doc in docs:
                urn = self._build_urn(doc)
                if not urn:
                    continue

                time.sleep(1.5)
                full_text = self._fetch_full_text(doc, urn)
                if not full_text or len(full_text) < 50:
                    logger.debug(f"  No text for {urn}")
                    continue

                doc["_urn"] = urn
                doc["_full_text"] = full_text
                count += 1
                yield doc

        logger.info(f"Completed: {count} documents")

    def fetch_updates(self, since: str = None) -> Generator[Dict[str, Any], None, None]:
        """Fetch legislation from the current year."""
        current_year = datetime.now().year
        count = 0

        logger.info(f"Listing legislation for {current_year}...")
        docs = self._senate_list_year(current_year)
        time.sleep(1)

        for doc in docs:
            if since:
                date_iso = self._date_br_to_iso(doc.get("dataassinatura", ""))
                if date_iso < since:
                    continue

            urn = self._build_urn(doc)
            if not urn:
                continue

            time.sleep(1.5)
            full_text = self._fetch_full_text(doc, urn)
            if not full_text or len(full_text) < 50:
                continue

            doc["_urn"] = urn
            doc["_full_text"] = full_text
            count += 1
            yield doc

        logger.info(f"Updates: {count} documents")

    def test(self) -> bool:
        """Quick connectivity test: list + full text fetch."""
        docs = self._senate_list_year(2024)
        if not docs:
            logger.error("Senate API returned no documents")
            return False
        logger.info(f"Senate API: {len(docs)} docs for 2024 (filtered types)")

        # Test with a law first (normas API has inline text for laws)
        lei_docs = [d for d in docs if d.get("tipo") == "LEI-n"]
        dec_docs = [d for d in docs if d.get("tipo") == "DEC-n"]

        for label, subset in [("LEI", lei_docs), ("DEC", dec_docs)]:
            for doc in subset[:3]:
                urn = self._build_urn(doc)
                if not urn:
                    continue
                logger.info(f"Testing {label}: {urn}")
                full_text = self._fetch_full_text(doc, urn)
                if full_text and len(full_text) > 50:
                    logger.info(
                        f"Full text OK ({label}): {doc.get('normaNome', '?')[:60]} "
                        f"({len(full_text)} chars)"
                    )
                    return True
                time.sleep(1)

        logger.error("Could not fetch full text for any test document")
        return False


def main():
    import argparse

    parser = argparse.ArgumentParser(description="BR/Planalto data fetcher")
    parser.add_argument(
        "command",
        choices=["bootstrap", "update", "test"],
        help="Command to run",
    )
    parser.add_argument(
        "--sample",
        action="store_true",
        help="Only fetch a small sample (for validation)",
    )
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = PlanaltoScraper()

    if args.command == "test":
        success = scraper.test()
        sys.exit(0 if success else 1)

    elif args.command == "bootstrap":
        stats = scraper.bootstrap(sample_mode=args.sample)
        logger.info(f"Bootstrap complete: {json.dumps(stats, indent=2)}")

    elif args.command == "update":
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)
        count = 0
        for record in scraper.fetch_updates():
            out_path = sample_dir / f"update_{count:04d}.json"
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)
            count += 1
            if count >= 50:
                break
        logger.info(f"Update complete: {count} records")


if __name__ == "__main__":
    main()
