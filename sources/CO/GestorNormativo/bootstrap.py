#!/usr/bin/env python3
"""
CO/GestorNormativo -- Colombia Gestor Normativo (Función Pública) Fetcher

Fetches full text of Colombian norms from Función Pública's Gestor Normativo.

Strategy:
  - Use search API to enumerate norm IDs by document type
  - For each norm ID, fetch norma.php?i={id} and extract full text
  - Clean HTML, decode entities, strip CSS/JS artifacts

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py bootstrap-fast     # Alias for bootstrap
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import time
import re
import html as html_module
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper, as_date_str

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.CO.GestorNormativo")

BASE_URL = "https://www.funcionpublica.gov.co/eva/gestornormativo"
SEARCH_URL = f"{BASE_URL}/gestion/funphp/funajax.php"

# Document type IDs from the advanced search dropdown
# Focus on legislation-relevant types for bootstrap
DOC_TYPES = {
    18: "Ley",
    11: "Decreto",
    986: "Decreto Ley",
    2: "Acto Legislativo",
    29: "Resolución",
    8: "Constitución Política",
    30: "Sentencia",
    925: "Concepto Sala de Consulta C.E.",
    6: "Circular",
    13: "Directiva",
    3: "Acuerdo",
    987: "Decretos Salariales",
    7: "Concepto",
    1205: "Auto",
    825: "Circular Externa",
    184: "Circular Conjunta",
    # NB 985 "Documento CONPES" was retired upstream — it is gone from the
    # consulta_avanzada.php `tipodoc` dropdown and the search endpoint answers
    # "Número de documentos encontrados: 0" for it. Kept out so a permanently
    # empty type does not read as a discovery failure (issue #1514).
    905: "Estatutos",
    989: "Reglamento",
    988: "Criterio Unificado",
    785: "Concepto Marco",
    14: "Documento de Relatoria",
    845: "Circular Unificada",
    1185: "Comunicado",
    1245: "Circular Vicepresidencial",
    1225: "Directiva Vicepresidencial",
    1285: "Directiva Presidencial",
    1305: "Conceptos Guias",
}

# Priority types for sample/bootstrap (most important first)
PRIORITY_TYPES = [18, 11, 986, 2, 29, 8, 30, 925]

REQUEST_TIMEOUT = 30
MIN_DELAY = 0.4          # healthy vantage answers in ~0.6s; don't idle past that
MAX_DELAY = 10.0         # back off this far when the host starts timing out
SPEEDUP_AFTER = 20       # consecutive successes before easing the delay back down
CHECKPOINT_VERSION = 1
CHECKPOINT_EVERY = 50    # flush progress every N documents
# Abort loudly rather than reporting a near-empty corpus when the host has stopped
# answering entirely (datacenter-IP throttle, per issue #1501).
MAX_CONSECUTIVE_FAILURES = 60
# Consecutive pre-cutoff documents before a type is considered caught up.
UPDATE_STALE_STREAK = 5

# The site template hard-codes this as `dateModified` on most norm pages, so it
# says nothing about the norm — see _extract_date.
TEMPLATE_DATE = "2015-12-01"
SPANISH_MONTHS = {
    "enero": 1, "febrero": 2, "marzo": 3, "abril": 4, "mayo": 5, "junio": 6,
    "julio": 7, "agosto": 8, "septiembre": 9, "setiembre": 9, "octubre": 10,
    "noviembre": 11, "diciembre": 12,
}


class GestorNormativoScraper(BaseScraper):
    """Scraper for CO/GestorNormativo — Colombian norms from Función Pública."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        try:
            from common.http_client import HttpClient
            self.client = HttpClient(timeout=REQUEST_TIMEOUT)
        except ImportError:
            self.client = None

        # Adaptive pacing. funcionpublica.gov.co answers in ~0.6s from a healthy
        # vantage but read-times-out in bursts from datacenter IPs (issue #1501),
        # so pace on observed behaviour instead of a fixed sleep that costs ~17h
        # of pure waiting across the ~40K-document corpus.
        self._delay = MIN_DELAY
        self._ok_streak = 0
        self._fail_streak = 0
        # Last transport exception seen by _http_get, so a run that enumerates
        # nothing can name the real cause instead of reporting an empty corpus.
        self._last_http_error: Optional[BaseException] = None

        self._checkpoint_path = source_dir / "data" / "checkpoint.json"
        self._checkpoint = self._load_checkpoint()
        self._done: set = set(self._checkpoint.get("done", []))
        self._pending_flush = 0

    # ---------------------------------------------------------------- checkpoint

    def _load_checkpoint(self) -> Dict[str, Any]:
        """Load the resume checkpoint, tolerating a missing or corrupt file."""
        try:
            with open(self._checkpoint_path, encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, dict) and data.get("version") == CHECKPOINT_VERSION:
                logger.info(
                    f"Checkpoint: {len(data.get('done', []))} norms already fetched, "
                    f"{len(data.get('types', {}))} types enumerated"
                )
                return data
            logger.info("Checkpoint version mismatch — starting fresh")
        except FileNotFoundError:
            pass
        except Exception as e:
            logger.warning(f"Unreadable checkpoint ({e}) — starting fresh")
        return {"version": CHECKPOINT_VERSION, "done": [], "types": {}}

    def _save_checkpoint(self, force: bool = False) -> None:
        """Persist progress. Written via a temp file so a kill can't truncate it."""
        self._pending_flush += 1
        if not force and self._pending_flush < CHECKPOINT_EVERY:
            return
        self._pending_flush = 0
        self._checkpoint["done"] = sorted(self._done)
        self._checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self._checkpoint_path.with_suffix(".json.tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(self._checkpoint, f)
        tmp.replace(self._checkpoint_path)

    # ------------------------------------------------------------------ fetching

    def _pace(self, ok: bool) -> None:
        """Adapt the inter-request delay to how the host is actually behaving."""
        if ok:
            self._fail_streak = 0
            self._ok_streak += 1
            if self._ok_streak >= SPEEDUP_AFTER:
                self._ok_streak = 0
                self._delay = max(MIN_DELAY, self._delay * 0.8)
        else:
            self._ok_streak = 0
            self._fail_streak += 1
            self._delay = min(MAX_DELAY, self._delay * 1.5)
        time.sleep(self._delay)

    def _http_get(self, url: str, headers: Optional[Dict] = None) -> Optional[str]:
        """HTTP GET returning response text."""
        default_headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/120.0.0.0 Safari/537.36",
        }
        if headers:
            default_headers.update(headers)

        for attempt in range(3):
            try:
                if self.client:
                    resp = self.client.get(url, headers=default_headers)
                    if resp.status_code == 200 and len(resp.text) > 10:
                        return resp.text
                    if resp.status_code in (404, 500):
                        return None
                else:
                    import urllib.request
                    req = urllib.request.Request(url, headers=default_headers)
                    with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT) as resp:
                        data = resp.read().decode("utf-8", errors="replace")
                        if len(data) > 10:
                            return data
                        return None
            except Exception as e:
                self._last_http_error = e
                logger.debug(f"Attempt {attempt+1} failed for {url}: {e}")
                if attempt < 2:
                    time.sleep(2 * (attempt + 1))
        return None

    def _assert_discovery_worked(self, empty_types: int) -> None:
        """Fail loud when not one document type enumerated any norm.

        Every network call here funnels through ``_http_get``, which returns
        ``None`` on any transport fault, so a whole-vantage failure (the
        funcionpublica.gov.co TLS chain rejecting verification, a datacenter-IP
        throttle) used to surface as ``[]`` from every type, an exit-0 run with
        0 records, and a pipeline that re-ingested the bundled samples as if the
        corpus were up to date (issue #1514, same false-completion class as
        #1397-#1402). The per-document path already fails loud after
        ``MAX_CONSECUTIVE_FAILURES``; discovery now does too.

        The search endpoint always answers with norms for every configured type,
        so "all types empty" is never a legitimate outcome.
        """
        if empty_types < len(DOC_TYPES):
            return
        cause = (
            f" Last transport error: {self._last_http_error!r}"
            if self._last_http_error is not None
            else ""
        )
        raise RuntimeError(
            f"All {len(DOC_TYPES)} document types enumerated 0 norms from "
            f"{SEARCH_URL} — the search endpoint is unreachable from this "
            f"vantage, not empty; failing loud rather than reporting an empty "
            f"corpus (see issue #1514).{cause}"
        )

    def _get_norm_ids_for_type(self, type_id: int, refresh: bool = False) -> List[str]:
        """Get all norm IDs for a given document type via search API.

        The `pagina` parameter is accepted but ignored server-side — every page
        returns the whole result set for the type (verified: pagina=1/2/3/50 all
        return the same 1,669 Ley ids) — so one request enumerates the type
        completely, newest id first. Results are cached in the checkpoint because
        the enumeration is what a restart would otherwise repeat needlessly.
        """
        cached = self._checkpoint.get("types", {}).get(str(type_id))
        if cached and not refresh:
            return cached

        url = (f"{SEARCH_URL}?t=ejecuta_busqueda_avanzada2"
               f"&tipdoc={type_id}&pagina=1")
        headers = {
            "Referer": f"{BASE_URL}/consulta_avanzada.php",
        }
        text = self._http_get(url, headers=headers)
        if not text:
            return []

        ids = re.findall(r'norma\.php\?i=(\d+)', text)
        ids = list(dict.fromkeys(ids))  # deduplicate preserving order
        if ids:
            self._checkpoint.setdefault("types", {})[str(type_id)] = ids
            self._save_checkpoint(force=True)
        return ids

    def _parse_norm_page(self, norm_id: str, raw_html: str) -> Optional[Dict[str, Any]]:
        """Parse a norm page and extract structured data."""
        # Title
        title_m = re.search(
            r'titulo-norma[^>]*><strong>(.*?)</strong>', raw_html, re.DOTALL
        )
        title = html_module.unescape(title_m.group(1).strip()) if title_m else None
        if not title:
            return None

        # Extract text from descripcion-contenido div
        text = self._extract_content_text(raw_html)
        if not text or len(text) < 50:
            return None

        date_str = self._extract_date(title, text, raw_html)

        # Description from og:description
        desc_m = re.search(r'og:description"\s+content="([^"]+)"', raw_html)
        description = html_module.unescape(desc_m.group(1).strip()) if desc_m else ""

        # Infer norm type from title
        norm_type = self._infer_type(title)

        return {
            "norm_id": norm_id,
            "title": title,
            "text": text,
            "norm_type": norm_type,
            "date": date_str,
            "description": description,
        }

    def _extract_date(self, title: str, text: str, raw_html: str) -> Optional[str]:
        """Best-effort ISO date for a norm.

        The page's `dateModified` meta is NOT the norm's date — most pages carry
        the site template's default (TEMPLATE_DATE), which would date a 2026 law
        to 2015 and, worse, make every document look older than any refresh
        cutoff so the update lane finds nothing. So read the norm's own date out
        of its opening line ("LEY N. 2565 DEL 12 DE FEBRERO DE 2026"), fall back
        to the year in the title ("Ley 2565 de 2026"), and only then trust the
        meta tag.
        """
        head = text[:400]
        m = re.search(
            r'\b(\d{1,2})\s+DE\s+(' + "|".join(SPANISH_MONTHS) + r')\s+DE\s+(\d{4})',
            head, re.IGNORECASE)
        if m:
            day, month, year = m.group(1), m.group(2).lower(), m.group(3)
            return f"{year}-{SPANISH_MONTHS[month]:02d}-{int(day):02d}"

        # Title carries the year for effectively every norm ("Decreto 149 de 2026").
        m = re.search(r'\bde\s+((?:1[89]|20)\d{2})\b', title)
        if m:
            return f"{m.group(1)}-01-01"

        date_m = re.search(r'dateModified"\s+content="([^"]+)"', raw_html)
        if date_m and not date_m.group(1).startswith(TEMPLATE_DATE):
            return date_m.group(1)
        return None

    def _extract_content_text(self, raw_html: str) -> Optional[str]:
        """Extract and clean text from the descripcion-contenido div."""
        marker = '<div class="descripcion-contenido">'
        pos = raw_html.find(marker)
        if pos == -1:
            return None

        start = pos + len(marker)
        # Find matching closing div by tracking depth
        depth = 1
        i = start
        end = len(raw_html)
        while depth > 0 and i < end:
            next_open = raw_html.find("<div", i)
            next_close = raw_html.find("</div>", i)
            if next_close == -1:
                break
            if next_open != -1 and next_open < next_close:
                depth += 1
                i = next_open + 4
            else:
                depth -= 1
                if depth == 0:
                    content = raw_html[start:next_close]
                    break
                i = next_close + 6
        else:
            content = raw_html[start:start + 200000]

        # Remove style and script blocks
        content = re.sub(r'<style[^>]*>.*?</style>', '', content, flags=re.DOTALL)
        content = re.sub(r'<script[^>]*>.*?</script>', '', content, flags=re.DOTALL)

        # Convert br tags to newlines
        content = re.sub(r'<br\s*/?>', '\n', content)

        # Strip all HTML tags
        content = re.sub(r'<[^>]+>', ' ', content)

        # Decode HTML entities
        content = html_module.unescape(content)

        # Clean whitespace
        content = re.sub(r'[ \t]+', ' ', content)
        content = re.sub(r'\n[ \t]+', '\n', content)
        content = re.sub(r'\n{3,}', '\n\n', content)
        content = content.strip()

        # Remove CSS artifacts that may leak through
        if content.startswith(('@font-face', 'body {', ':root{')):
            # CSS leaked — try to find actual text after it
            # Look for common norm text patterns
            for pattern in [r'(?:LEY|DECRETO|SENTENCIA|RESOLUCIÓN|ACUERDO)',
                            r'(?:ARTÍCULO|ARTICULO)\s+\d+']:
                m = re.search(pattern, content, re.IGNORECASE)
                if m:
                    content = content[m.start():]
                    break

        return content if len(content) >= 50 else None

    def _infer_type(self, title: str) -> str:
        """Infer document type from title."""
        title_lower = title.lower()
        for keyword, dtype in [
            ("ley ", "Ley"), ("decreto ley", "Decreto Ley"),
            ("decreto", "Decreto"), ("sentencia", "Sentencia"),
            ("resolución", "Resolución"), ("resolucion", "Resolución"),
            ("acto legislativo", "Acto Legislativo"),
            ("concepto", "Concepto"), ("circular", "Circular"),
            ("acuerdo", "Acuerdo"), ("constitución", "Constitución"),
            ("auto ", "Auto"), ("directiva", "Directiva"),
        ]:
            if keyword in title_lower:
                return dtype
        return "Otro"

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Transform raw data into standard schema."""
        norm_id = raw["norm_id"]
        return {
            "_id": f"CO-GestorNormativo-{norm_id}",
            "_source": "CO/GestorNormativo",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw["title"],
            "text": raw["text"],
            "norm_type": raw.get("norm_type", ""),
            "date": raw.get("date"),
            "description": raw.get("description", ""),
            "url": f"{BASE_URL}/norma.php?i={norm_id}",
            "norm_id": norm_id,
        }

    def fetch_all(self, sample: bool = False) -> Generator[Dict[str, Any], None, None]:
        """Fetch all norms. If sample=True, fetch ~15 from priority types.

        Yields RAW parsed dicts — BaseScraper calls normalize() on each.

        Resumable: norm ids already fetched in an earlier run are skipped with no
        network call at all, so a torn-down worker's replacement advances instead
        of re-walking the same prefix (issue #1501).
        """
        if sample or getattr(self, "_sample_mode", False):
            yield from self._fetch_sample()
            return

        skipped = 0
        empty_types = 0
        for type_id in DOC_TYPES:
            type_name = DOC_TYPES[type_id]
            ids = self._get_norm_ids_for_type(type_id)
            if not ids:
                empty_types += 1
            todo = [i for i in ids if i not in self._done]
            skipped += len(ids) - len(todo)
            logger.info(
                f"{type_name} (id={type_id}): {len(ids)} norms, "
                f"{len(todo)} to fetch, {len(ids) - len(todo)} already done"
            )
            if not todo:
                continue

            for norm_id in todo:
                yield from self._fetch_and_mark(norm_id)
            self._save_checkpoint(force=True)

        self._assert_discovery_worked(empty_types)
        self._save_checkpoint(force=True)
        if skipped:
            logger.info(f"Resumed run skipped {skipped} already-fetched norms")

    def _fetch_and_mark(self, norm_id: str) -> Generator[Dict[str, Any], None, None]:
        """Fetch one norm, pace, checkpoint it, and yield it if it parsed."""
        parsed = self._fetch_single_norm(norm_id)
        self._pace(ok=parsed is not None)

        if self._fail_streak >= MAX_CONSECUTIVE_FAILURES:
            raise RuntimeError(
                f"{MAX_CONSECUTIVE_FAILURES} consecutive failures fetching "
                f"{BASE_URL}/norma.php — host is refusing or timing out on this "
                f"vantage (see issue #1501); failing loud rather than reporting a "
                f"truncated corpus. Progress is checkpointed, so a re-run resumes."
            )

        # Only a successfully parsed norm is marked done; a timed-out one is left
        # for the next run to retry.
        if parsed:
            self._done.add(norm_id)
            self._save_checkpoint()
            yield parsed

    def _fetch_sample(self) -> Generator[Dict[str, Any], None, None]:
        """Fetch ~15 sample records from priority document types."""
        count = 0
        target = 15

        for type_id in PRIORITY_TYPES:
            if count >= target:
                break
            type_name = DOC_TYPES[type_id]
            logger.info(f"Fetching sample IDs for type: {type_name}")
            ids = self._get_norm_ids_for_type(type_id)
            if not ids:
                continue

            # Take first 3 from each type
            for norm_id in ids[:3]:
                if count >= target:
                    break
                parsed = self._fetch_single_norm(norm_id)
                self._pace(ok=parsed is not None)
                if parsed:
                    yield parsed
                    count += 1

    def _fetch_single_norm(self, norm_id: str) -> Optional[Dict[str, Any]]:
        """Fetch and parse a single norm page, returning the RAW parsed dict."""
        url = f"{BASE_URL}/norma.php?i={norm_id}"
        raw_html = self._http_get(url)
        if not raw_html or len(raw_html) < 100:
            logger.warning(f"Empty/unavailable page for norm {norm_id}")
            return None

        # Retired/unknown ids serve a ~13KB "Esta página no está disponible" shell
        # that carries neither titulo-norma nor descripcion-contenido, so
        # _parse_norm_page rejects it on the missing title.
        parsed = self._parse_norm_page(norm_id, raw_html)
        if not parsed:
            logger.warning(f"Could not parse norm {norm_id}")
            return None

        logger.info(f"  → {parsed['title']} ({len(parsed['text'])} chars)")
        return parsed

    def fetch_updates(self, since: str) -> Generator[Dict[str, Any], None, None]:
        """Fetch norms published since a date.

        Previously a no-op, which is why the update-stale lane degraded into a
        blind full re-crawl of an already-ingested corpus (issue #1501). The
        search endpoint returns each type's ids newest-first, so walking the head
        of every type and stopping once enough consecutive documents predate
        `since` costs a few hundred requests instead of ~40K.
        """
        cutoff = as_date_str(since)[:10] if since else ""
        logger.info(f"Fetching norms published since {cutoff or 'the beginning'}")

        empty_types = 0
        for type_id in DOC_TYPES:
            type_name = DOC_TYPES[type_id]
            # Re-enumerate: a cached list from the last run has no new ids in it.
            ids = self._get_norm_ids_for_type(type_id, refresh=True)
            if not ids:
                empty_types += 1
                continue

            stale_streak = 0
            new_count = 0
            for norm_id in ids:  # newest first
                if stale_streak >= UPDATE_STALE_STREAK:
                    break
                parsed = self._fetch_single_norm(norm_id)
                self._pace(ok=parsed is not None)
                if not parsed:
                    continue

                doc_date = (parsed.get("date") or "")[:10]
                if cutoff and doc_date and doc_date < cutoff:
                    stale_streak += 1
                    continue

                stale_streak = 0
                new_count += 1
                self._done.add(norm_id)
                self._save_checkpoint()
                yield parsed

            logger.info(f"{type_name}: {new_count} norms since {cutoff}")

        self._assert_discovery_worked(empty_types)
        self._save_checkpoint(force=True)

    def test_connection(self) -> bool:
        """Quick connectivity test."""
        text = self._http_get(f"{BASE_URL}/norma.php?i=300")
        if text and "Ley 87 de 1993" in text:
            logger.info("Connection test passed — Ley 87 de 1993 accessible")
            return True
        logger.error("Connection test failed")
        return False


def main():
    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py "
              "[bootstrap|bootstrap-fast|update|test] [--sample] [--full] [--since DATE]")
        sys.exit(1)

    scraper = GestorNormativoScraper()
    command = sys.argv[1]
    sample = "--sample" in sys.argv
    full = "--full" in sys.argv

    if command == "test":
        ok = scraper.test_connection()
        sys.exit(0 if ok else 1)

    if command == "update":
        since = ""
        if "--since" in sys.argv:
            since = sys.argv[sys.argv.index("--since") + 1]
        if since:
            stats = {"records_fetched": 0}
            for raw in scraper.fetch_updates(since):
                record = scraper.normalize(raw)
                scraper.storage.write(scraper._dedup_key(record), record)
                stats["records_fetched"] += 1
        else:
            stats = scraper.update()
        logger.info(f"Update complete: {stats}")
        sys.exit(0)

    if command in ("bootstrap", "bootstrap-fast"):
        # Run through BaseScraper so records stream to data/records.jsonl with
        # dedup and validation, rather than being dumped into sample/ (#798 class).
        if sample and not full:
            scraper._sample_mode = True
            stats = scraper.run_sample(15)
        else:
            stats = scraper.bootstrap()

        logger.info(f"Bootstrap complete: {stats}")
        if not stats.get("records_fetched"):
            logger.error("No records fetched — see the log above for the cause")
            sys.exit(1)
        sys.exit(0)

    print(f"Unknown command: {command}")
    sys.exit(1)


if __name__ == "__main__":
    main()
