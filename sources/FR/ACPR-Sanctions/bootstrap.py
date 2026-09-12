#!/usr/bin/env python3
"""
FR/ACPR-Sanctions -- Autorité de contrôle prudentiel et de résolution (ACPR),
Commission des sanctions -- Décisions.

The ACPR is the French prudential supervisor for the banking and insurance
sectors, an independent administrative authority backed by the Banque de France
(Code monétaire et financier, art. L.612-1 et seq.). Its Commission des
sanctions is an independent adjudicatory body that, after adversarial
proceedings, imposes disciplinary sanctions (blâme, avertissement, sanction
pécuniaire, retrait d'agrément, radiation) on supervised entities for breaches
of their prudential, anti-money-laundering (LCB-FT), governance and
customer-protection obligations. Each decision is a reasoned adjudication of a
specific case by a statutory office-holder = case_law.

The full corpus is published on the ACPR site as a single "Recueil des
sanctions" (compendium of sanctions) listing, from 2010 (creation of the ACP,
later ACPR) to the present: ~110 decisions, banking + insurance + payment /
e-money institutions + intermediaries, all born-digital PDFs (no OCR needed).

Strategy:
  - Fetch the "Recueil des sanctions" listing page. It links every decision to a
    publication page under
    /fr/publications-et-statistiques/publications/decision-de-la-commission-des-sanctions-...
  - Each publication page carries a "Télécharger le document" paragraph with the
    born-digital decision PDF at /system/files/... (the href is rendered with
    spaces around '=' by the Drupal/Twig template).
  - Download each decision PDF and extract full text with PyMuPDF (pdfplumber /
    pypdf fallback). Parse the decision number (n° YYYY-NN) and decision date
    from the page title, falling back to the "Décision rendue le ..." line in
    the PDF body and to the /system/files/YYYY/MM/ path.
  - One record per decision.

Data:
  - ~110 Commission des sanctions decisions, 2010-present
  - Language: French
  - Auth: None (free public access)

Usage:
  python bootstrap.py bootstrap          # Full pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records for validation
  python bootstrap.py bootstrap-fast     # Full pull (runner alias)
  python bootstrap.py update             # Incremental (recent decisions)
  python bootstrap.py test               # Quick connectivity test
"""

import os
import re
import sys
import html
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List
from urllib.parse import quote

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

try:
    import fitz  # PyMuPDF
except ImportError:
    fitz = None

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.FR.ACPR-Sanctions")

BASE_URL = "https://acpr.banque-france.fr"
RECUEIL_PATH = "/fr/reglementation/recueil-des-sanctions"

# acpr.banque-france.fr refuses datacenter IPs (Hetzner/fleet vantages get
# connection resets / 403s while residential FR/EU vantages get 200s — issue
# #1244).  Every page and every /system/files/ decision PDF is mirrored by the
# Internet Archive, so each fetch falls back to the archived copy.
#
# We cannot just replay /web/3000id_/ (= "latest capture"): the ACPR site now
# 403s the Internet Archive's own crawler too, so the most recent snapshot of a
# page is often an archived 403 body.  Ask CDX for the capture list instead and
# replay the newest snapshot that actually succeeded.  Revisit records carry
# statuscode "-" (identical content already stored under an earlier digest) and
# replay fine, so they count as good — filtering CDX on statuscode:200 would
# silently discard half the usable captures.
WAYBACK_CDX = ("https://web.archive.org/cdx/search/cdx"
               "?url={url}&output=json&limit=-8")
WAYBACK_REPLAY = "https://web.archive.org/web/{ts}id_/{url}"
_ARCHIVE_BAD_STATUS = {"403", "404", "429", "500", "502", "503", "504"}
# Number of consecutive live failures (with zero live successes) after which we
# stop attempting the live host altogether and read only from the archive.
LIVE_FAIL_LATCH = 3

TAG_RE = re.compile(r"<[^>]+>")

# Every decision on the recueil links to its publication page.
DECISION_LINK_RE = re.compile(
    r'href="((?:https://acpr\.banque-france\.fr)?'
    r'/fr/publications-et-statistiques/publications/'
    r'decision-de-la-commission-des-sanctions[^"#?]+)"',
    re.I)

# The download button href is rendered as: href ="/system/files/....pdf"
PDF_HREF_RE = re.compile(
    r'href\s*=\s*"((?:https://acpr\.banque-france\.fr)?/[^"]+?\.pdf)"', re.I)
TITLE_RE = re.compile(r"<title>\s*(.*?)\s*</title>", re.S | re.I)

# n° 2024-01  /  ndeg-2024-01  /  procédure n° 2024-01
NUM_RE = re.compile(r"n(?:deg|[°º])\s*[-\s]*(\d{4})\s*-\s*(\d{1,2})", re.I)
# "du 7 novembre 2025"  (title)  and  "rendue le 13 juin 2018" (PDF body)
DATE_TXT_RE = re.compile(
    r"(?:du|rendue\s+le|le)\s+(\d{1,2})(?:er)?\s+"
    r"(janvier|février|fevrier|mars|avril|mai|juin|juillet|ao[uû]t|"
    r"septembre|octobre|novembre|décembre|decembre)\s+(\d{4})", re.I)
# /system/files/2025-11/...  or  /system/files/import/acpr/media/2018/06/18/...
PATH_DATE_RE = re.compile(r"/(\d{4})[/-](\d{2})(?:[/-](\d{2}))?/")

_FR_MONTHS = {
    "janvier": 1, "février": 2, "fevrier": 2, "mars": 3, "avril": 4,
    "mai": 5, "juin": 6, "juillet": 7, "août": 8, "aout": 8,
    "septembre": 9, "octobre": 10, "novembre": 11,
    "décembre": 12, "decembre": 12,
}


def _strip(s: str) -> str:
    return html.unescape(TAG_RE.sub(" ", s or "")).strip()


def _parse_fr_date(text: str) -> Optional[str]:
    if not text:
        return None
    m = DATE_TXT_RE.search(text)
    if not m:
        return None
    day = int(m.group(1))
    month = _FR_MONTHS.get(m.group(2).lower())
    year = int(m.group(3))
    if not month:
        return None
    try:
        return f"{year:04d}-{month:02d}-{day:02d}"
    except (ValueError, TypeError):
        return None


def _path_date(url: str) -> Optional[str]:
    m = PATH_DATE_RE.search(url)
    if not m:
        return None
    y, mo, d = m.group(1), m.group(2), m.group(3) or "01"
    try:
        return f"{int(y):04d}-{int(mo):02d}-{int(d):02d}"
    except (ValueError, TypeError):
        return None


def _pdf_text(pdf_bytes: bytes) -> str:
    """Full text of a born-digital decision PDF via PyMuPDF, with a shared
    pdfplumber/pypdf fallback."""
    if fitz is not None:
        try:
            doc = fitz.open(stream=pdf_bytes, filetype="pdf")
            try:
                text = "\n".join(page.get_text() for page in doc).strip()
            finally:
                doc.close()
            if len(text) >= 120:
                return text
        except Exception as e:
            logger.debug(f"fitz extract failed: {e}")
    try:
        from common import pdf_extract as _pe
        for fn in ("_extract_with_pdfplumber", "_extract_with_pypdf"):
            f = getattr(_pe, fn, None)
            if f:
                try:
                    t = f(pdf_bytes)
                    if t and len(t) >= 120:
                        return t
                except Exception:
                    continue
    except Exception:
        pass
    return ""


def _clean(text: str) -> str:
    lines = [ln.rstrip() for ln in (text or "").replace("\r", "").split("\n")]
    out, blanks = [], 0
    for ln in lines:
        if ln.strip():
            blanks = 0
            out.append(ln.strip())
        else:
            blanks += 1
            if blanks <= 1:
                out.append("")
    return "\n".join(out).strip()


class ACPRSanctionsScraper(BaseScraper):
    """Scraper for the ACPR Commission des sanctions decisions (recueil listing
    + born-digital decision PDFs)."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.client = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                              "AppleWebKit/537.36 (KHTML, like Gecko) "
                              "Chrome/120.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
                "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.8",
            },
            timeout=90,
        )
        # Wayback needs its own client: a different host, no retry storm, and a
        # longer timeout (archive replay of a large PDF can be slow).
        self.archive_client = HttpClient(
            base_url="https://web.archive.org",
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                              "AppleWebKit/537.36 (KHTML, like Gecko) "
                              "Chrome/120.0.0.0 Safari/537.36",
                "Accept": "*/*",
            },
            max_retries=2,
            timeout=180,
            respect_robots=False,
        )
        self._live_ok = 0
        self._live_fail = 0
        # ACPR_FORCE_ARCHIVE=1 skips the live host entirely — used to exercise
        # the archive path from an unblocked vantage.
        self._archive_only = os.environ.get("ACPR_FORCE_ARCHIVE") == "1"

    # -- HTTP helpers ----------------------------------------------------
    def _skip_live(self) -> bool:
        """True once the live host has failed LIVE_FAIL_LATCH times in a row
        without a single success — i.e. this vantage is IP-blocked, so stop
        burning ~90s per request on it and read only from the archive."""
        if self._archive_only:
            return True
        if self._live_ok == 0 and self._live_fail >= LIVE_FAIL_LATCH:
            logger.warning(
                f"{self._live_fail} consecutive live failures and 0 successes "
                "— acpr.banque-france.fr is blocking this vantage; switching to "
                "Internet Archive replay for the rest of the run"
            )
            self._archive_only = True
            return True
        return False

    def _get_live(self, url: str) -> Optional[bytes]:
        if self._skip_live():
            return None
        self.rate_limiter.wait()
        try:
            resp = self.client.get(url)
        except Exception as e:
            self._live_fail += 1
            logger.warning(f"live GET {url} failed: {e}")
            return None
        if resp.status_code != 200:
            self._live_fail += 1
            logger.debug(f"live GET {url}: HTTP {resp.status_code}")
            return None
        self._live_ok += 1
        self._live_fail = 0
        return resp.content

    def _archive_timestamps(self, url: str) -> List[str]:
        """Timestamps of the newest usable captures of `url`, newest first."""
        cdx = WAYBACK_CDX.format(url=quote(url, safe=""))
        self.rate_limiter.wait()
        try:
            resp = self.archive_client.get(cdx)
            rows = resp.json() if resp.status_code == 200 else []
        except Exception as e:
            logger.warning(f"wayback cdx {url}: {e}")
            return []
        if not rows or len(rows) < 2:
            return []
        header, data = rows[0], rows[1:]
        try:
            ts_i, st_i = header.index("timestamp"), header.index("statuscode")
        except ValueError:
            return []
        good = [r[ts_i] for r in data if r[st_i] not in _ARCHIVE_BAD_STATUS]
        return list(reversed(good))

    def _get_archived(self, url: str) -> Optional[bytes]:
        """Raw bytes of the newest Internet Archive capture of `url` that is
        not itself an archived error page."""
        for ts in self._archive_timestamps(url)[:3]:
            self.rate_limiter.wait()
            try:
                resp = self.archive_client.get(
                    WAYBACK_REPLAY.format(ts=ts, url=url))
            except Exception as e:
                logger.warning(f"wayback {ts} {url}: {e}")
                continue
            if resp.status_code == 200 and resp.content:
                return resp.content
            logger.debug(f"wayback {ts} {url}: HTTP {resp.status_code}")
        logger.warning(f"wayback {url}: no usable capture")
        return None

    def _get_html(self, url: str) -> Optional[str]:
        data = self._get_live(url)
        if data is None:
            data = self._get_archived(url)
        if data is None:
            return None
        return data.decode("utf-8", "replace")

    def _fetch_pdf(self, url: str) -> Optional[bytes]:
        data = self._get_live(url)
        if not (data and data.startswith(b"%PDF")):
            data = self._get_archived(url)
        if data and data.startswith(b"%PDF"):
            return data
        logger.warning(f"pdf {url}: no PDF bytes from live host or archive")
        return None

    # -- listing ---------------------------------------------------------
    def _list_decision_pages(self) -> List[str]:
        page = self._get_html(BASE_URL + RECUEIL_PATH)
        if not page:
            return []
        seen, urls = set(), []
        for m in DECISION_LINK_RE.finditer(page):
            u = m.group(1)
            if not u.startswith("http"):
                u = BASE_URL + u
            if u not in seen:
                seen.add(u)
                urls.append(u)
        return urls

    def _build_raw(self, pub_url: str) -> Optional[Dict[str, Any]]:
        page = self._get_html(pub_url)
        if not page:
            return None
        tm = TITLE_RE.search(page)
        title = _strip(tm.group(1)) if tm else ""
        # Drop the trailing " | Autorité de contrôle..." site suffix.
        title = re.split(r"\s*\|\s*Autorité", title)[0].strip()

        pm = PDF_HREF_RE.search(page)
        if not pm:
            return None
        pdf_url = pm.group(1)
        if not pdf_url.startswith("http"):
            pdf_url = BASE_URL + pdf_url

        pdf = self._fetch_pdf(pdf_url)
        if not pdf:
            return None
        try:
            text = _pdf_text(pdf)
        except Exception as e:
            logger.debug(f"extract {pdf_url}: {e}")
            text = ""
        if not text:
            return None

        # decision number
        number = None
        nm = NUM_RE.search(title) or NUM_RE.search(pub_url)
        if nm:
            number = f"{nm.group(1)}-{int(nm.group(2)):02d}"
        # decision date: title -> PDF body -> file path
        date_iso = (_parse_fr_date(title)
                    or _parse_fr_date(text[:2000])
                    or _path_date(pdf_url))
        return {
            "url": pub_url,
            "pdf_url": pdf_url,
            "title": title,
            "number": number,
            "date": date_iso,
            "text": text,
        }

    # -- core ------------------------------------------------------------
    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        pages = self._list_decision_pages()
        if not pages:
            raise RuntimeError(
                "ACPR recueil-des-sanctions listing returned 0 decision links "
                "from both the live site and the Internet Archive "
                "— layout changed or the archive replay is down"
            )
        produced = 0
        for pub_url in pages:
            raw = self._build_raw(pub_url)
            if raw:
                produced += 1
                yield raw
        if produced == 0:
            raise RuntimeError(
                "ACPR: found decision pages but extracted 0 full-text PDFs "
                "— download blocked or PDFs unreadable"
            )
        # Fail loud on a suspiciously partial yield: we got the decision links
        # but most decision PDFs came back empty from BOTH the live host and the
        # archive. Raising here prevents the pipeline from ingesting a small
        # partial corpus as a false "complete".
        if produced < 0.6 * len(pages):
            raise RuntimeError(
                f"ACPR: only {produced} of {len(pages)} decision PDFs extracted "
                "— live /system/files/ downloads failed and the Internet Archive "
                "has no usable capture for the remainder"
            )

    def fetch_updates(self, since: datetime) -> Generator[Dict[str, Any], None, None]:
        """The recueil is a single small listing; re-scan and keep decisions on
        or after `since`."""
        since_date = since.date()
        for pub_url in self._list_decision_pages():
            raw = self._build_raw(pub_url)
            if not raw:
                continue
            d = raw.get("date")
            if d:
                try:
                    if datetime.strptime(d, "%Y-%m-%d").date() < since_date:
                        continue
                except ValueError:
                    pass
            yield raw

    def normalize(self, raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        text = _clean(raw.get("text", "") or "")
        if len(text) < 200:
            return None
        number = raw.get("number")
        date_iso = raw.get("date")
        if number:
            _id = f"FR-ACPR-{number}"
        elif date_iso:
            _id = f"FR-ACPR-{date_iso}"
        else:
            # last resort: slug of the pub url
            _id = "FR-ACPR-" + raw["url"].rstrip("/").split("/")[-1][:60]
        title = raw.get("title") or (
            f"Décision de la Commission des sanctions n° {number}"
            if number else "Décision de la Commission des sanctions")
        return {
            "_id": _id,
            "_source": "FR/ACPR-Sanctions",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date_iso,
            "url": raw.get("url"),
            "pdf_url": raw.get("pdf_url"),
            "decision_number": number,
            "court": "ACPR — Commission des sanctions",
            "jurisdiction": "FR",
            "language": "fr",
        }

    # -- diagnostics -----------------------------------------------------
    def test_connection(self):
        vantage = "Internet Archive (forced)" if self._archive_only else "live"
        print(f"Testing ACPR recueil-des-sanctions listing ({vantage})...")
        pages = self._list_decision_pages()
        print(f"  decision pages found: {len(pages)}")
        if not pages:
            return
        print(f"  first: {pages[0]}")
        raw = self._build_raw(pages[0])
        if raw:
            print(f"  number={raw['number']} date={raw['date']}")
            print(f"  pdf={raw['pdf_url']}")
            print(f"  full text extracted: {len(raw['text'])} chars - OK")
        else:
            print("  could not extract first decision")


def main():
    scraper = ACPRSanctionsScraper()
    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|bootstrap-fast|update|test] [--sample]")
        sys.exit(1)
    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv

    if command == "test":
        scraper.test_connection()
    elif command in ("bootstrap", "bootstrap-fast"):
        if sample_mode:
            logger.info("Running bootstrap in sample mode")
            stats = scraper.bootstrap(sample_mode=True, sample_size=15)
        else:
            logger.info("Running full bootstrap")
            stats = scraper.bootstrap(sample_mode=False)
        logger.info(f"Bootstrap complete: {stats}")
    elif command == "update":
        stats = scraper.update()
        logger.info(f"Update complete: {stats}")
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
