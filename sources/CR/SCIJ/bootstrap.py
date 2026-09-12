#!/usr/bin/env python3
"""
CR/SCIJ -- Costa Rica Sistema Costarricense de Información Jurídica

Fetches Costa Rican legislation from SCIJ (Attorney General's office).

Platform migration (issue #1455)
--------------------------------
SCIJ moved off the old ASP.NET WebForms site at pgrweb.go.cr/scij: every
`nrm_norma.aspx` / `nrm_texto_completo.aspx` URL now answers 302 with a
Location on https://sinalevi.go.cr/ResultadosNormativa/Informacion. The old
crawler read a 302 as "invalid ID", so after the migration every one of the
107,000 IDs was skipped — no records, and no progress log either, because the
progress line only fired on a successful fetch. That is the "hang" reported in
#1455; the stuck :443 socket was a second, independent bug (the fetch helper
drove `client.session` directly, bypassing HttpClient's wall-clock deadline).

Strategy (new platform):
  - The norm ID space carried over unchanged (old nValor2 == new param1).
  - POST _CargarTextoCompleto with version=-1 as a cheap existence probe: the
    JSON redirect it returns carries param2 = the current version id, or 0 when
    the ID holds no norm. One ~0.2s request per candidate ID.
  - For a live ID, POST _CargarFicha (metadata) and _CargarTextoCompleto
    (full text) with that version id.
  - Probing runs in fetch_all(); the two per-document fetches run in
    normalize(), so `bootstrap-fast` overlaps them across its worker pool
    (BaseScraper only parallelises normalize()).

Resumability:
  IDs walked are checkpointed to data/scij_checkpoint.json, so a torn-down or
  timed-out fleet slot resumes where it stopped instead of restarting at ID 1.

Data:
  - ~100,000 norms from 1821 to present
  - Types: Constitution, laws, decrees, treaties, regulations, etc.
  - Full text in HTML (Word-exported), cleaned to plain text
  - Language: Spanish

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap-fast     # Full pull, concurrent (fleet entry point)
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py update --since ... # Recent norms only
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import threading
from pathlib import Path
from datetime import datetime, timezone
from html import unescape
from typing import Generator, Dict, Any, Optional

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper, as_date_str
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.CR.SCIJ")

BASE_URL = "https://sinalevi.go.cr"
TEXT_ENDPOINT = BASE_URL + "/ResultadosNormativa/_CargarTextoCompleto"
FICHA_ENDPOINT = BASE_URL + "/ResultadosNormativa/_CargarFicha"
DOC_URL = BASE_URL + "/ResultadosNormativa/Informacion?param1={id}&param2={version}&param3=1"

# Sample IDs known to hold norms with full text (laws, decrees, treaties)
SAMPLE_IDS = [100, 500, 1000, 5000, 10000, 20000, 30000, 40000, 50000,
              60000, 70000, 80000, 85000, 90000, 95000, 100000, 105000, 107000]

# Highest ID confirmed live (2026-08-19). The real ceiling is discovered at
# runtime — hardcoded ceilings go stale and silently truncate the corpus.
KNOWN_MAX_ID = 107_500
CEILING_PROBE_STEP = 100
CEILING_PROBE_MISSES = 10  # stop after this many consecutive empty step-probes

CHECKPOINT_EVERY = 50   # IDs between checkpoint flushes
CHECKPOINT_REWIND = 200  # resume this far back, covering docs still in flight
HEARTBEAT_EVERY = 250   # IDs between progress lines, hit or miss

# A live ID always redirects to itself carrying its current version id;
# param2=0 means "no norm behind this ID".
_VERSION_RE = re.compile(r"param2=(\d+)")

# Chrome-ish UA: the platform's WAF is content-negotiating, and the endpoints
# are XHR-only.
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Accept-Language": "es-CR,es;q=0.9,en;q=0.5",
    "X-Requested-With": "XMLHttpRequest",
    "Origin": BASE_URL,
    "Referer": BASE_URL + "/Escritorio/SCIJ",
}

# Navigation chrome the text view wraps around the norm body.
BOILERPLATE_LINES = {
    "ficha de la norma",
    "-usted está en la última versión de la norma-",
    "ir a la última versión",
    "anterior",
    "siguiente",
    "ir al menú de normativa",
    "no hay artículos disponibles para mostrar.",
}
_VERSION_LINE_RE = re.compile(r"^versión de la norma:\s*\d+\s+de\s+\d+$", re.I)


class SCIJUnreachable(RuntimeError):
    """Raised when the platform refuses us outright, so a run fails loud.

    A silent zero reads as "the corpus is empty" and is exactly how the
    migration went unnoticed for a full fleet cycle.
    """


class CostaRicaSCIJScraper(BaseScraper):
    """
    Scraper for CR/SCIJ -- Costa Rica SCIJ legislation.
    Country: CR
    URL: https://sinalevi.go.cr/

    Data types: legislation
    Auth: none (Open Data)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.client = HttpClient(
            headers=dict(HEADERS),
            timeout=(10, 45),
            # Hard ceiling per call. Without it a single trickling response
            # holds the crawl forever (#1455): `timeout` is per socket read,
            # not per request.
            wall_timeout=120,
            # sinalevi.go.cr intermittently completes the handshake without
            # sending its intermediate CA. HttpClient first repairs the chain
            # properly by AIA-fetching that intermediate; this allowlist only
            # authorises the unverified last resort, for this host alone, so a
            # bad handshake costs a warning instead of the document (#1484).
            insecure_ssl_hosts={"sinalevi.go.cr"},
        )
        self.data_dir = source_dir / "data"
        self.checkpoint_path = self.data_dir / "scij_checkpoint.json"
        self._lock = threading.Lock()
        # Transport failures, counted apart from "this ID holds no norm". Both
        # used to return None and read identically, so two days of TLS errors
        # looked like a sparse ID range (#1484).
        self._transport_errors = 0
        self._transport_error_sample: Dict[str, int] = {}

    # ------------------------------------------------------------------
    # HTTP
    # ------------------------------------------------------------------
    def _record_transport_error(self, url: str, exc: Exception) -> None:
        """Count a failed hop and log the first few of each kind at WARNING."""
        label = type(exc).__name__
        with self._lock:
            self._transport_errors += 1
            seen = self._transport_error_sample.get(label, 0) + 1
            self._transport_error_sample[label] = seen
            total = self._transport_errors
        if seen <= 3:
            logger.warning(f"Transport error #{seen} ({label}) on {url}: {exc}")
        elif total % 250 == 0:
            logger.warning(
                f"{total} transport errors so far: {self._transport_error_sample}"
            )

    def _post(self, url: str, data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """POST one endpoint, returning the parsed JSON body or None.

        A transport failure is recorded before returning None so it can never
        pass as an empty answer from a healthy server.
        """
        try:
            self.rate_limiter.wait()
            resp = self.client.post(url, data=data)
            if resp.status_code != 200:
                logger.debug(f"{url} -> HTTP {resp.status_code} for {data}")
                self._record_transport_error(
                    url, RuntimeError(f"HTTP {resp.status_code}")
                )
                return None
            return resp.json()
        except Exception as e:
            self._record_transport_error(url, e)
            return None

    def _current_version(self, norm_id: int) -> Optional[int]:
        """Existence probe: current version id for a norm, or None if absent.

        `version=-1` asks the platform to resolve the norm's live version; it
        answers with a redirect whose param2 is that version id, or 0 when no
        norm sits behind the ID.
        """
        payload = self._post(
            TEXT_ENDPOINT,
            {"idFichaNorma": str(norm_id), "version": "-1", "busqueda": ""},
        )
        if payload is None:
            return None
        m = _VERSION_RE.search(payload.get("direccion") or "")
        if not m:
            return None
        version = int(m.group(1))
        return version or None

    # ------------------------------------------------------------------
    # Parsing
    # ------------------------------------------------------------------
    @staticmethod
    def _html_to_text(html: str) -> str:
        """Strip a fragment of Word-exported HTML down to plain text."""
        html = re.sub(r"<(script|style)[^>]*>.*?</\1>", " ", html,
                      flags=re.DOTALL | re.IGNORECASE)
        html = re.sub(r"<!--.*?-->", " ", html, flags=re.DOTALL)
        html = re.sub(r"<(?:br|p|div|tr|li|h[1-6])[^>]*/?\s*>", "\n", html,
                      flags=re.IGNORECASE)
        html = re.sub(r"</(?:p|div|tr|li|h[1-6])>", "\n", html,
                      flags=re.IGNORECASE)
        html = re.sub(r"<[^>]+>", " ", html)
        text = unescape(html)
        text = text.replace("\xa0", " ")
        text = re.sub(r"[ \t]+", " ", text)
        lines = [line.strip() for line in text.split("\n")]
        return "\n".join(line for line in lines if line)

    def _parse_ficha(self, html: str) -> Dict[str, Any]:
        """Extract structured metadata from the _CargarFicha fragment."""
        meta: Dict[str, Any] = {}

        # Card header: "Tratados Internacionales: 8355 del 28/05/2003"
        m = re.search(
            r'class="k-card-header[^"]*"[^>]*>\s*(.*?)\s*</div>', html, re.DOTALL
        )
        if m:
            header = unescape(re.sub(r"<[^>]+>", "", m.group(1))).strip()
            header = re.sub(r"\s+", " ", header)
            meta["header"] = header
            hm = re.match(r"(.*?):\s*([\w\-./]+)?(?:\s+del\s+(\d{1,2}/\d{1,2}/\d{4}))?$",
                          header)
            if hm:
                meta["norm_type"] = (hm.group(1) or "").strip()
                meta["norm_number"] = (hm.group(2) or "").strip()
                if hm.group(3):
                    meta["date"] = self._parse_date(hm.group(3))

        # Title: first bolded card title in the details block
        m = re.search(r'class="k-card-title pb-3"[^>]*>\s*<b>(.*?)</b>', html, re.DOTALL)
        if m:
            title = unescape(re.sub(r"<[^>]+>", "", m.group(1))).strip()
            meta["title"] = re.sub(r"\s+", " ", title)

        # Read Ente Emisor off the markup: the value is the tail of its own
        # <p>, and once tags are stripped there is nothing left to bound it.
        m = re.search(r"Ente Emisor:\s*</b>(.*?)</p>", html, re.DOTALL | re.IGNORECASE)
        if m:
            issuer = unescape(re.sub(r"<[^>]+>", " ", m.group(1))).strip()
            meta["issuing_body"] = re.sub(r"\s+", " ", issuer)

        flat = re.sub(r"\s+", " ", unescape(re.sub(r"<[^>]+>", " ", html)))

        m = re.search(r"Fecha de vigencia desde:\s*(\d{1,2}/\d{1,2}/\d{4})", flat)
        if m:
            meta["date_vigencia"] = self._parse_date(m.group(1))

        m = re.search(r"N° Gaceta:\s*(\d+)\s*del\s*(\d{1,2}/\d{1,2}/\d{4})", flat)
        if m:
            meta["gaceta"] = f"N° {m.group(1)} del {m.group(2)}"
            meta["date_publication"] = self._parse_date(m.group(2))

        m = re.search(r"Versión de la Norma:\s*(\d+)\s*de\s*(\d+)", flat)
        if m:
            meta["version_label"] = f"{m.group(1)} de {m.group(2)}"

        return meta

    @staticmethod
    def _parse_date(value: str) -> Optional[str]:
        """DD/MM/YYYY -> ISO 8601, or None when the date is unusable."""
        m = re.search(r"(\d{1,2})/(\d{1,2})/(\d{4})", value or "")
        if not m:
            return None
        try:
            dt = datetime(int(m.group(3)), int(m.group(2)), int(m.group(1)))
        except ValueError:
            return None
        return dt.strftime("%Y-%m-%d")

    def _extract_fulltext(self, html: str) -> str:
        """Extract the norm body from the _CargarTextoCompleto fragment."""
        # The body sits after the version-navigation row; everything before it
        # is chrome (headings, prev/next buttons) plus the Word <style> block.
        m = re.search(r'id="divVersionSiguiente".*?</div>\s*</div>', html,
                      re.DOTALL | re.IGNORECASE)
        if m:
            html = html[m.end():]

        text = self._html_to_text(html)
        # "Ficha Artículo N" is the per-article footer link the viewer appends
        # after every article body, not part of the norm.
        text = re.sub(r"(?im)^\s*Ficha\s+Art[íi]culo\s*\d*\s*$\n?", "", text)

        lines = text.split("\n")
        # Drop any navigation chrome that survived the cut.
        while lines and (
            lines[0].lower() in BOILERPLATE_LINES or _VERSION_LINE_RE.match(lines[0])
        ):
            lines.pop(0)
        while lines and (
            lines[-1].lower() in BOILERPLATE_LINES or _VERSION_LINE_RE.match(lines[-1])
        ):
            lines.pop()
        text = "\n".join(lines).strip()

        # Trailing generation stamp from the export footer
        text = re.sub(r"\n?Fecha de generación:.*$", "", text, flags=re.DOTALL)
        return text.strip()

    # ------------------------------------------------------------------
    # Document fetch
    # ------------------------------------------------------------------
    def _fetch_document(self, norm_id: int, version: int) -> Optional[Dict[str, Any]]:
        """Fetch one norm's metadata and full text at a known version."""
        payload = self._post(
            TEXT_ENDPOINT,
            {"idFichaNorma": str(norm_id), "version": str(version), "busqueda": ""},
        )
        if not payload or not payload.get("html"):
            return None
        text = self._extract_fulltext(payload["html"])
        if len(text) < 30:
            logger.debug(f"ID {norm_id}: no usable full text ({len(text)} chars)")
            return None

        meta: Dict[str, Any] = {}
        ficha = self._post(
            FICHA_ENDPOINT, {"idFichaNorma": str(norm_id), "version": str(version)}
        )
        if ficha and ficha.get("html"):
            meta = self._parse_ficha(ficha["html"])

        # Fall back to the headings the text view carries when the ficha is thin.
        if not meta.get("title") or not meta.get("norm_type"):
            heads = re.findall(r'id="h1-Size"[^>]*>\s*(.*?)\s*</h1>', payload["html"],
                               re.DOTALL)
            heads = [re.sub(r"\s+", " ", unescape(re.sub(r"<[^>]+>", "", h))).strip()
                     for h in heads]
            if heads and not meta.get("norm_type"):
                hm = re.match(r"(.*?)\s+([\w\-./]+)$", heads[0])
                if hm:
                    meta.setdefault("norm_type", hm.group(1).strip())
                    meta.setdefault("norm_number", hm.group(2).strip())
            if len(heads) > 1 and not meta.get("title"):
                meta["title"] = heads[1]

        parts = [p for p in (meta.get("norm_type"), meta.get("norm_number")) if p]
        header = " ".join(parts)
        title = meta.get("title") or ""
        full_title = f"{header} - {title}" if header and title else (header or title)
        if not full_title:
            full_title = f"Norma SCIJ {norm_id}"

        # `date` is a required temporal key: prefer the date the norm carries,
        # then its publication date, then the start of its validity (#995).
        date = meta.get("date") or meta.get("date_publication") or meta.get("date_vigencia")

        return {
            "norm_id": norm_id,
            "version_id": version,
            "norm_type": meta.get("norm_type", ""),
            "norm_number": meta.get("norm_number", ""),
            "title": full_title,
            "text": text,
            "date": date,
            "date_vigencia": meta.get("date_vigencia"),
            "issuing_body": meta.get("issuing_body", ""),
            "gaceta": meta.get("gaceta", ""),
            "version_label": meta.get("version_label", ""),
            "url": DOC_URL.format(id=norm_id, version=version),
        }

    def normalize(self, raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Normalize a raw document to standard schema.

        fetch_all() yields only {norm_id, version}; the two document fetches
        happen here so `bootstrap-fast` overlaps them across worker threads.
        """
        doc = raw if "text" in raw else self._fetch_document(
            raw["norm_id"], raw["version"]
        )
        if not doc:
            return None

        return {
            "_id": f"CR/SCIJ:{doc['norm_id']}",
            "_source": "CR/SCIJ",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": doc.get("title", ""),
            "text": doc.get("text", ""),
            "date": doc.get("date"),
            "url": doc.get("url", ""),
            "norm_id": doc.get("norm_id"),
            "version_id": doc.get("version_id"),
            "norm_type": doc.get("norm_type", ""),
            "norm_number": doc.get("norm_number", ""),
            "issuing_body": doc.get("issuing_body", ""),
            "date_vigencia": doc.get("date_vigencia"),
            "gaceta": doc.get("gaceta", ""),
            "version_label": doc.get("version_label", ""),
            "jurisdiction": "CR",
        }

    # ------------------------------------------------------------------
    # Checkpointing
    # ------------------------------------------------------------------
    def _load_checkpoint(self) -> int:
        """Return the ID to resume from (rewound to cover in-flight docs)."""
        try:
            with open(self.checkpoint_path, "r", encoding="utf-8") as f:
                state = json.load(f)
            last = int(state.get("last_id", 0))
        except (OSError, ValueError, TypeError):
            return 1
        if last <= 0:
            return 1
        resume = max(1, last - CHECKPOINT_REWIND + 1)
        logger.info(
            f"Resuming from checkpoint: last walked ID {last}, restarting at {resume}"
        )
        return resume

    def _save_checkpoint(self, last_id: int, fetched: int, skipped: int) -> None:
        try:
            self.data_dir.mkdir(parents=True, exist_ok=True)
            tmp = self.checkpoint_path.with_suffix(".tmp")
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(
                    {
                        "last_id": last_id,
                        "fetched": fetched,
                        "skipped": skipped,
                        "updated_at": datetime.now(timezone.utc).isoformat(),
                    },
                    f,
                )
            tmp.replace(self.checkpoint_path)
        except OSError as e:
            logger.warning(f"Could not write checkpoint: {e}")

    # ------------------------------------------------------------------
    # Enumeration
    # ------------------------------------------------------------------
    def _discover_max_id(self) -> int:
        """Probe past the known ceiling so new norms are not silently dropped."""
        last_hit = KNOWN_MAX_ID
        misses = 0
        probe = KNOWN_MAX_ID
        while misses < CEILING_PROBE_MISSES:
            probe += CEILING_PROBE_STEP
            if self._current_version(probe):
                last_hit = probe
                misses = 0
            else:
                misses += 1
        ceiling = last_hit + CEILING_PROBE_STEP * CEILING_PROBE_MISSES
        logger.info(f"ID ceiling probe: last live ID ~{last_hit}, scanning to {ceiling}")
        return ceiling

    def _walk_ids(self, start_id: int, max_id: int, checkpoint: bool
                  ) -> Generator[Dict[str, Any], None, None]:
        """Probe an ID range, yielding {norm_id, version} for live norms."""
        fetched = 0
        skipped = 0
        probe_errors = 0

        for norm_id in range(start_id, max_id + 1):
            version = self._current_version(norm_id)
            if version is None:
                skipped += 1
                probe_errors += 1
            else:
                probe_errors = 0
                fetched += 1
                yield {"norm_id": norm_id, "version": version}

            scanned = norm_id - start_id + 1

            # Fail loud rather than walking 100K IDs against a wall: a long
            # unbroken run of dead probes with nothing found means the platform
            # is refusing us, not that the corpus ended.
            if fetched == 0 and probe_errors >= 500:
                raise SCIJUnreachable(
                    f"{probe_errors} consecutive empty probes from ID {start_id} "
                    f"with zero norms found — sinalevi.go.cr is refusing this "
                    f"vantage or the endpoint contract changed."
                )

            if checkpoint and scanned % CHECKPOINT_EVERY == 0:
                self._save_checkpoint(norm_id, fetched, skipped)
            if scanned % HEARTBEAT_EVERY == 0:
                # Heartbeat on IDs *scanned*, not on hits: the old progress line
                # only fired on a hit, so a dead range logged nothing at all and
                # a healthy crawl was indistinguishable from a hung one (#1455).
                logger.info(
                    f"Progress: ID {norm_id}/{max_id} — {fetched} live, {skipped} empty"
                )

        if checkpoint:
            self._save_checkpoint(max_id, fetched, skipped)
        logger.info(f"Completed: {fetched} norms found, {skipped} empty IDs")
        if self._transport_errors:
            # Loud, because these are IDs the corpus lost to the network rather
            # than IDs that hold nothing — the distinction #1484 turned on.
            logger.error(
                f"DEGRADED: {self._transport_errors} transport error(s) during "
                f"this walk {self._transport_error_sample} — the affected IDs "
                f"were skipped, not confirmed empty. Re-run from the checkpoint."
            )

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Fetch all norms by walking the ID space, resuming from checkpoint."""
        start_id = self._load_checkpoint()
        max_id = self._discover_max_id()
        logger.info(f"Starting full fetch: IDs {start_id} to {max_id}")
        yield from self._walk_ids(start_id, max_id, checkpoint=True)

    def fetch_updates(self, since: str = None) -> Generator[Dict[str, Any], None, None]:
        """Fetch recent norms — new norms take the highest IDs."""
        # `update()` passes a datetime; this body treats `since` as a date string (#1512).
        since = as_date_str(since)
        max_id = self._discover_max_id()
        start_id = max(1, max_id - 5000)
        logger.info(f"Fetching updates: IDs {start_id} to {max_id} (since={since})")

        count = 0
        for ref in self._walk_ids(start_id, max_id, checkpoint=False):
            if since:
                doc = self._fetch_document(ref["norm_id"], ref["version"])
                if not doc:
                    continue
                if doc.get("date") and doc["date"] < since:
                    continue
                count += 1
                yield doc
            else:
                count += 1
                yield ref

        logger.info(f"Updates: {count} norms fetched")

    # ------------------------------------------------------------------
    def bootstrap(self, sample_mode: bool = False, sample_size: int = 10) -> dict:
        """Sample runs walk known-good IDs instead of the head of the range."""
        if sample_mode:
            return super().bootstrap(sample_mode=True, sample_size=sample_size)
        return super().bootstrap(sample_mode=False, sample_size=sample_size)

    def test(self) -> bool:
        """Quick connectivity test."""
        try:
            version = self._current_version(70000)
            if not version:
                logger.error("Test failed: ID 70000 resolved no version")
                return False
            raw = self._fetch_document(70000, version)
            if raw and len(raw.get("text", "")) > 100:
                logger.info(
                    f"Test passed: ID 70000 v{version} - {raw['title'][:80]} "
                    f"({len(raw['text'])} chars, date={raw.get('date')})"
                )
                return True
            logger.error("Test failed: no text returned for ID 70000")
            return False
        except Exception as e:
            logger.error(f"Test failed: {e}")
            return False


class _SampleScraper(CostaRicaSCIJScraper):
    """Sample mode walks curated IDs so 15 records cost 15 probes, not 15,000."""

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        for norm_id in SAMPLE_IDS:
            version = self._current_version(norm_id)
            if version:
                yield {"norm_id": norm_id, "version": version}


def main():
    import argparse

    parser = argparse.ArgumentParser(description="CR/SCIJ data fetcher")
    parser.add_argument(
        "command",
        # `bootstrap-fast` is the fleet wrapper's entry point; without it
        # argparse exits 2 and the wrapper falls back to re-ingesting sample/.
        choices=["bootstrap", "bootstrap-fast", "update", "test"],
        help="Command to run",
    )
    parser.add_argument(
        "--sample",
        action="store_true",
        help="Only fetch a small sample (for validation)",
    )
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    parser.add_argument("--since", default=None, help="ISO date for update runs")
    args = parser.parse_args()

    scraper = _SampleScraper() if args.sample else CostaRicaSCIJScraper()

    if args.command == "test":
        sys.exit(0 if scraper.test() else 1)

    if args.command == "update" and not args.full:
        count = 0
        for raw in scraper.fetch_updates(since=args.since):
            record = scraper.normalize(raw)
            if record:
                count += 1
        logger.info(f"Update complete: {count} records")
        sys.exit(0 if count else 1)

    if args.command == "bootstrap-fast" and not args.sample:
        stats = scraper.bootstrap_fast()
    else:
        stats = scraper.bootstrap(sample_mode=args.sample, sample_size=15)

    fetched = stats.get("records_fetched", 0) or stats.get("sample_records_saved", 0)
    logger.info(f"Bootstrap complete: {fetched} records — {stats}")
    if fetched == 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
