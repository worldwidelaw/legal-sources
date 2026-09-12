"""
Legal Data Hunter - Turkish Constitutional Court Scraper

Fetches case law from the Turkish Constitutional Court (Anayasa Mahkemesi).

The public decision databases (normkararlarbilgibankasi.anayasa.gov.tr and
kararlarbilgibankasi.anayasa.gov.tr) were rebuilt in 2026 as a single React SPA
served under a ``/kbb/`` path prefix; the old ``/Ara?page=N`` listing and
``/ND/{year}/{no}`` / ``/BB/{year}/{no}`` detail pages now 404.  Both hostnames
front the same JSON backend at ``/api``:

  POST /api/core/public/search
       body {"kararTipi": <category>, "page": N (1-based), "size": M,
             "sort": "kararTarihi", "order": "asc"}
       -> {"total": int, "page": int, "data": [ {metadata...}, ... ]}

  GET  /api/core/public/download-decision?id={uuid}&type=pdf&decType={int}
       header X-Captcha-Verified: "{epoch_ms}:{hmac_sha256(epoch_ms, KEY)}"
       -> born-digital application/pdf of the decision (full text)

Search results carry metadata only, so the full text comes from the PDF, which
is born-digital and extracts cleanly with PyMuPDF.

Categories covered (all five the portal publishes):
  NormDenetimi          Norm review (constitutionality) decisions, 1962+
  BireyselBasvuru       Individual applications, 2012+
  SiyasiParti           Political party cases
  YuceDivan             Supreme Criminal Tribunal cases
  YasamaDokunulmazligi  Parliamentary immunity cases
"""

import re
import sys
import json
import time
import hmac
import hashlib
import logging
import threading
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional

import requests

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("TR/AnayasaMahkemesi")


# The SPA signs download requests with a static HMAC key shipped in its public
# JS bundle (assets/index-*.js).  It is a bot speed-bump, not authentication --
# the whole database is open access.
DOWNLOAD_HMAC_KEY = "3RBVB_052XL6_HU3HD_7CN8KI_GEQ1U_KT368"

# PDF text produced by the court's Word->PDF pipeline substitutes G-cedilla /
# G-dotaccent glyphs for the Turkish S-cedilla / I-dotaccent ones.
_TR_GLYPH_FIX = {
    "Ģ": "ş",  # G with cedilla   -> s with cedilla
    "ģ": "ş",  # g with cedilla   -> s with cedilla
    "Ġ": "İ",  # G with dot above -> I with dot above
    "ġ": "i",  # g with dot above -> i
}
_TR_GLYPH_RE = re.compile("[" + "".join(_TR_GLYPH_FIX) + "]")


class TurkishConstitutionalCourtScraper(BaseScraper):
    """
    Scraper for: Turkish Constitutional Court (Anayasa Mahkemesi)
    Country: TR
    URL: https://www.anayasa.gov.tr

    Data types: case_law
    Auth: none
    """

    SITE = "https://kararlarbilgibankasi.anayasa.gov.tr"
    API = SITE + "/api"

    # kararTipi -> decType expected by download-decision
    CATEGORIES = {
        "NormDenetimi": 3,
        "BireyselBasvuru": 2,
        "SiyasiParti": 4,
        "YuceDivan": 5,
        "YasamaDokunulmazligi": 6,
    }

    PAGE_SIZE = 100
    MIN_TEXT_CHARS = 200

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": (
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36"
                ),
                "Accept": "application/json, text/plain, */*",
                "Referer": f"{self.SITE}/kbb/pages/search/Tumu",
                "Origin": self.SITE,
            }
        )
        self._session_lock = threading.Lock()

        self.checkpoint_path = self.source_dir / "data" / "checkpoint.json"
        self._checkpoint = self._load_checkpoint()
        # Sample runs must not advance the full-corpus checkpoint.
        self._checkpointing = True

    def run_sample(self, n: int = 10) -> dict:
        self._checkpointing = False
        self._checkpoint = {"completed": [], "pages": {}}
        try:
            return super().run_sample(n=n)
        finally:
            self._checkpointing = True
            self._checkpoint = self._load_checkpoint()

    # ═══════════════════════════════════════════════════════════════
    # Checkpoint / resume
    # ═══════════════════════════════════════════════════════════════

    def _load_checkpoint(self) -> dict:
        try:
            with open(self.checkpoint_path, encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, dict):
                data.setdefault("completed", [])
                data.setdefault("pages", {})
                return data
        except Exception:
            pass
        return {"completed": [], "pages": {}}

    def _save_checkpoint(self):
        if not getattr(self, "_checkpointing", True):
            return
        try:
            self.checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
            tmp = self.checkpoint_path.with_suffix(".tmp")
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(self._checkpoint, f)
            tmp.replace(self.checkpoint_path)
        except Exception as e:  # checkpointing must never kill a run
            logger.warning(f"Could not write checkpoint: {e}")

    # ═══════════════════════════════════════════════════════════════
    # HTTP helpers
    # ═══════════════════════════════════════════════════════════════

    def _download_auth_header(self) -> str:
        ts = str(int(time.time() * 1000))
        sig = hmac.new(
            DOWNLOAD_HMAC_KEY.encode("utf-8"), ts.encode("utf-8"), hashlib.sha256
        ).hexdigest()
        return f"{ts}:{sig}"

    def _request(self, method: str, url: str, *, attempts: int = 5, **kwargs):
        """Issue a request, backing off on 429/5xx and connection errors."""
        delay = 2.0
        last = None
        for attempt in range(attempts):
            try:
                resp = self.session.request(method, url, timeout=120, **kwargs)
                if resp.status_code in (429, 500, 502, 503, 504):
                    retry_after = resp.headers.get("Retry-After")
                    wait = float(retry_after) if retry_after and retry_after.isdigit() else delay
                    logger.warning(
                        f"HTTP {resp.status_code} from {url} — retrying in {wait:.0f}s "
                        f"({attempt + 1}/{attempts})"
                    )
                    time.sleep(min(wait, 120))
                    delay = min(delay * 2, 120)
                    last = requests.HTTPError(f"HTTP {resp.status_code}")
                    continue
                resp.raise_for_status()
                return resp
            except (requests.ConnectionError, requests.Timeout) as e:
                last = e
                logger.warning(f"{type(e).__name__} on {url} — retrying in {delay:.0f}s")
                time.sleep(delay)
                delay = min(delay * 2, 120)
            except requests.HTTPError as e:
                # 4xx other than 429: not worth retrying
                raise e
        raise RuntimeError(f"Request failed after {attempts} attempts: {url} ({last})")

    def _search(self, karar_tipi: str, page: int, size: int) -> dict:
        self.rate_limiter.wait()
        resp = self._request(
            "POST",
            f"{self.API}/core/public/search",
            json={
                "kararTipi": karar_tipi,
                "page": page,
                "size": size,
                "sort": "kararTarihi",
                "order": "asc",
            },
            headers={"Content-Type": "application/json"},
        )
        return resp.json()

    def _download_pdf(self, decision_id: str, karar_tipi: str) -> bytes:
        dec_type = self.CATEGORIES[karar_tipi]
        with self._session_lock:
            auth = self._download_auth_header()
        resp = self._request(
            "GET",
            f"{self.API}/core/public/download-decision",
            params={"id": decision_id, "type": "pdf", "decType": dec_type},
            headers={"X-Captcha-Verified": auth, "Accept": "*/*"},
        )
        return resp.content

    # ═══════════════════════════════════════════════════════════════
    # Enumeration
    # ═══════════════════════════════════════════════════════════════

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield raw metadata for every decision in every category."""
        if not self._checkpointing:
            # Sample mode: spread the sample across all five categories.
            for karar_tipi in self.CATEGORIES:
                payload = self._search(karar_tipi, 1, 5)
                for row in payload.get("data") or []:
                    row["kararTipi"] = row.get("kararTipi") or karar_tipi
                    yield row
            return

        for karar_tipi in self.CATEGORIES:
            if karar_tipi in self._checkpoint["completed"]:
                logger.info(f"{karar_tipi}: already completed (checkpoint) — skipping")
                continue
            yield from self._fetch_category(karar_tipi)

    def _fetch_category(self, karar_tipi: str) -> Generator[dict, None, None]:
        start_page = int(self._checkpoint["pages"].get(karar_tipi, 0)) + 1
        page = start_page
        total = None
        seen = 0

        while True:
            payload = self._search(karar_tipi, page, self.PAGE_SIZE)
            rows = payload.get("data") or []
            if total is None:
                total = payload.get("total", 0)
                logger.info(
                    f"{karar_tipi}: {total} decisions "
                    f"(resuming at page {start_page})" if start_page > 1
                    else f"{karar_tipi}: {total} decisions"
                )
            if not rows:
                break

            for row in rows:
                row["kararTipi"] = row.get("kararTipi") or karar_tipi
                seen += 1
                yield row

            self._checkpoint["pages"][karar_tipi] = page
            self._save_checkpoint()

            if total and page * self.PAGE_SIZE >= total:
                break
            page += 1

        self._checkpoint["completed"].append(karar_tipi)
        self._save_checkpoint()
        logger.info(f"{karar_tipi}: done ({seen} decisions this run)")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Yield decisions decided on/after ``since`` (newest-first scan)."""
        if since.tzinfo is None:
            since = since.replace(tzinfo=timezone.utc)

        for karar_tipi in self.CATEGORIES:
            page = 1
            stop = False
            while not stop:
                payload = self._search_newest_first(karar_tipi, page, self.PAGE_SIZE)
                rows = payload.get("data") or []
                if not rows:
                    break
                for row in rows:
                    row["kararTipi"] = row.get("kararTipi") or karar_tipi
                    d = self._row_date(row)
                    if d and d < since.strftime("%Y-%m-%d"):
                        stop = True
                        break
                    yield row
                total = payload.get("total", 0)
                if total and page * self.PAGE_SIZE >= total:
                    break
                page += 1

    def _search_newest_first(self, karar_tipi: str, page: int, size: int) -> dict:
        self.rate_limiter.wait()
        resp = self._request(
            "POST",
            f"{self.API}/core/public/search",
            json={
                "kararTipi": karar_tipi,
                "page": page,
                "size": size,
                "sort": "kararTarihi",
                "order": "desc",
            },
            headers={"Content-Type": "application/json"},
        )
        return resp.json()

    # ═══════════════════════════════════════════════════════════════
    # Text extraction
    # ═══════════════════════════════════════════════════════════════

    @staticmethod
    def _fix_turkish(text: str) -> str:
        """Repair the G-cedilla / G-dotaccent glyph substitution, case-aware."""

        def repl(m):
            i = m.start()
            ch = m.group(0)
            fixed = _TR_GLYPH_FIX[ch]
            if ch in ("Ģ", "ģ"):
                # Uppercase Ş only when the surrounding word is uppercase.
                neighbours = text[max(0, i - 1): i] + text[i + 1: i + 2]
                letters = [c for c in neighbours if c.isalpha()]
                if letters and all(c.isupper() for c in letters):
                    return "Ş"
                return "ş"
            return fixed

        return _TR_GLYPH_RE.sub(repl, text)

    def _pdf_text(self, pdf_bytes: bytes) -> str:
        import fitz  # PyMuPDF

        parts = []
        with fitz.open(stream=pdf_bytes, filetype="pdf") as doc:
            for page in doc:
                parts.append(page.get_text())
        text = "\n".join(parts)
        text = self._fix_turkish(text)
        text = text.replace("\xa0", " ")
        text = re.sub(r"[ \t]+", " ", text)
        text = re.sub(r"\n[ \t]+", "\n", text)
        text = re.sub(r"\n{3,}", "\n\n", text)
        return text.strip()

    # ═══════════════════════════════════════════════════════════════
    # Normalization
    # ═══════════════════════════════════════════════════════════════

    @staticmethod
    def _row_date(raw: dict) -> Optional[str]:
        for key in ("kararTarihi", "davaTarihi", "resmiGazeteTarihi", "yayinTarihi"):
            val = raw.get(key)
            if val:
                m = re.match(r"(\d{4}-\d{2}-\d{2})", str(val))
                if m:
                    return m.group(1)
        return None

    @staticmethod
    def _strip_html(value) -> str:
        if not value:
            return ""
        text = re.sub(r"<[^>]+>", " ", str(value))
        text = re.sub(r"&nbsp;?", " ", text)
        from html import unescape

        text = unescape(text)
        return re.sub(r"\s+", " ", text).strip()

    # Legacy `_id` prefixes, kept so a re-run dedups against rows landed by the
    # pre-2026 HTML scraper (which keyed on the /ND/{y}/{n} and /BB/{y}/{n}
    # detail-page paths). kararNo / basvuruNo are unique within those two
    # categories (checked over 3,000 rows each); the three small ones are not
    # (SiyasiParti reuses E./K. numbers across decisions) so they key on the
    # backend UUID.
    _ID_PREFIX = {
        "NormDenetimi": "ND",
        "BireyselBasvuru": "BB",
        "SiyasiParti": "SP",
        "YuceDivan": "YD",
        "YasamaDokunulmazligi": "YDK",
    }

    def _decision_id(self, raw: dict, decision_uuid: str) -> str:
        karar_tipi = raw.get("kararTipi") or "NormDenetimi"
        prefix = self._ID_PREFIX.get(karar_tipi, karar_tipi)
        if karar_tipi == "NormDenetimi":
            natural = (raw.get("kararNo") or "").strip()
        elif karar_tipi == "BireyselBasvuru":
            natural = (raw.get("basvuruNo") or "").strip()
        else:
            natural = ""
        if natural:
            return f"{prefix}/{natural.replace(' ', '')}"
        return f"{prefix}/{decision_uuid}"

    def _title(self, raw: dict) -> str:
        karar_tipi = raw.get("kararTipi", "")
        esas = (raw.get("esasNo") or "").strip()
        karar = (raw.get("kararNo") or "").strip()

        if karar_tipi == "BireyselBasvuru":
            name = (raw.get("basvuruAdi") or "").strip()
            no = (raw.get("basvuruNo") or "").strip()
            if name and no:
                return f"{name} (B. No: {no})"
            if name:
                return name
            if no:
                return f"Bireysel Başvuru B. No: {no}"
        if esas and karar:
            return f"E.{esas}, K.{karar}"
        if esas:
            return f"E.{esas}"
        if karar:
            return f"K.{karar}"
        return f"Anayasa Mahkemesi Kararı ({karar_tipi})"

    def normalize(self, raw: dict) -> dict:
        """
        Transform a raw search row into the standard schema, downloading and
        extracting the decision PDF for the mandatory full text.
        """
        decision_uuid = raw.get("id")
        karar_tipi = raw.get("kararTipi") or "NormDenetimi"

        text = ""
        try:
            pdf_bytes = self._download_pdf(decision_uuid, karar_tipi)
            if pdf_bytes[:4] == b"%PDF":
                text = self._pdf_text(pdf_bytes)
            else:
                logger.warning(f"{decision_uuid}: download was not a PDF — skipping")
        except Exception as e:
            logger.warning(f"{decision_uuid}: PDF download/extract failed: {e}")

        if len(text) < self.MIN_TEXT_CHARS:
            # No full text -> no record (the loader requires a body).
            return None

        ref = (
            (raw.get("basvuruNo") or "").strip()
            if karar_tipi == "BireyselBasvuru"
            else "/".join(x for x in [(raw.get("esasNo") or "").strip(),
                                      (raw.get("kararNo") or "").strip()] if x)
        )
        decision_id = self._decision_id(raw, decision_uuid)

        return {
            "_id": f"TR/AnayasaMahkemesi/{decision_id}",
            "_source": "TR/AnayasaMahkemesi",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),

            # Standard required fields
            "title": self._title(raw),
            "text": text,  # MANDATORY FULL TEXT
            "date": self._row_date(raw),
            "url": (
                f"{self.SITE}/kbb/pages/search/{karar_tipi}"
                f"?id={decision_uuid}&type={karar_tipi}"
            ),

            # Source-specific fields
            "decision_id": decision_id,
            "database": karar_tipi,
            "case_number": (raw.get("esasNo") or "").strip() or None,
            "decision_number": (raw.get("kararNo") or "").strip() or None,
            "application_number": (raw.get("basvuruNo") or "").strip() or None,
            "applicant": (raw.get("basvuruAdi") or "").strip() or None,
            "panel": raw.get("kararVerenBirimLabel"),
            "decision_type": (
                raw.get("kararTuruBasvuruSonucuLabel")
                or raw.get("kararTuruDosyaSonucuLabel")
                or raw.get("kararTuruLabel")
            ),
            "summary": self._strip_html(raw.get("kararKonusu")) or None,
            "official_gazette_date": self._row_date({"kararTarihi": raw.get("resmiGazeteTarihi")}),
            "official_gazette_number": raw.get("resmiGazeteSayisi"),
            "reference": ref or None,
            "pdf_url": (
                f"{self.API}/core/public/download-decision"
                f"?id={decision_uuid}&type=pdf&decType={self.CATEGORIES[karar_tipi]}"
            ),
            "language": "tr",
        }


# ── CLI Entry Point ───────────────────────────────────────────────

def _test():
    scraper = TurkishConstitutionalCourtScraper()
    ok = True
    for karar_tipi in scraper.CATEGORIES:
        payload = scraper._search(karar_tipi, 1, 1)
        total = payload.get("total", 0)
        rows = payload.get("data") or []
        print(f"{karar_tipi:22s} total={total:>6}  rows={len(rows)}")
        if not rows:
            ok = False
            continue
        rec = scraper.normalize(rows[0])
        if rec:
            print(f"   -> {rec['title'][:60]!r} date={rec['date']} chars={len(rec['text'])}")
        else:
            ok = False
            print("   -> NO FULL TEXT")
    print("TEST", "PASSED" if ok else "FAILED")
    return 0 if ok else 1


def main():
    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|bootstrap-fast|update|test] "
              "[--sample] [--sample-size N] [--full]")
        sys.exit(1)

    command = sys.argv[1]

    if command == "test":
        sys.exit(_test())

    scraper = TurkishConstitutionalCourtScraper()
    sample_mode = "--sample" in sys.argv
    sample_size = 12
    if "--sample-size" in sys.argv:
        idx = sys.argv.index("--sample-size")
        sample_size = int(sys.argv[idx + 1])

    if command in ("bootstrap", "bootstrap-fast", "bootstrap_fast"):
        if sample_mode:
            stats = scraper.run_sample(n=sample_size)
            print(f"\nSample complete: {stats.get('sample_records_saved', 0)} records saved to sample/")
        elif command == "bootstrap":
            stats = scraper.bootstrap()
            print(f"\nBootstrap complete: {stats['records_new']} new, "
                  f"{stats['records_updated']} updated, {stats['records_skipped']} skipped")
        else:
            stats = scraper.bootstrap_fast()
            print(f"\nBootstrap complete: {stats['records_new']} new, "
                  f"{stats.get('errors', 0)} errors")
    elif command == "update":
        stats = scraper.update()
        print(f"\nUpdate complete: {stats['records_new']} new, {stats['records_updated']} updated")
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)

    print(json.dumps(stats, indent=2, default=str))


if __name__ == "__main__":
    main()
