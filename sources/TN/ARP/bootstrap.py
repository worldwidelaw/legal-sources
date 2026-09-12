#!/usr/bin/env python3
"""
TN/ARP -- Assemblee des representants du peuple (Tunisian Parliament) bills

Fetches Tunisian bills (mashari' qawanin / projets de loi), private members'
bills (muqtarahat qawanin / propositions de loi) and the laws they became
from the parliament's own site at arp.tn.

Strategy:
  - arp.tn runs Odoo 12 and exposes its public website models over the
    standard JSON-RPC endpoint /web/dataset/call_kw (no auth, portal user).
    The site's own search widget uses exactly this route, so we consume the
    same data the pages render.
  - Model `gpl.law.project` holds every bill (~584). The three site listings
    (/loi/project/list, /loi/proposition/list, /loi/project/loi) are the same
    model under different domains, so one sweep covers all of them.
  - Full text comes from two places, concatenated per bill:
      1. `gpl.law.item` -- the structured, article-by-article text of the bill
         (HTML `item` field), when parliament has keyed it in.
      2. `ir.attachment` PDFs attached to the bill (bill text as deposited,
         committee reports, and the JORT page carrying the enacted law),
         downloaded from /document/download/<id> and extracted.
  - Many attached PDFs are born-digital and extract cleanly; the rest are
    scans of paper originals with no text layer. Bills that yield no text
    from either route are dropped rather than emitted as metadata-only
    records, which leaves roughly a third of the 584 bills.
  - The download route is served by Odoo's HTTP dispatcher, which 400s a GET
    carrying a JSON Content-Type; the client must not set one session-wide.

Endpoints:
  - RPC:    POST https://www.arp.tn/web/dataset/call_kw
  - Doc:    GET  https://www.arp.tn/document/download/<attachment_id>
  - Page:   GET  https://www.arp.tn/loi/project/<project_id>

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records for validation
  python bootstrap.py update             # Incremental (bills touched since last run)
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import re
import json
import html
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.TN.ARP")

SOURCE_ID = "TN/ARP"
BASE_URL = "https://www.arp.tn"
RPC_ENDPOINT = "/web/dataset/call_kw"
DOWNLOAD_PATH = "/document/download"

# Fields worth carrying from gpl.law.project.
PROJECT_FIELDS = [
    "name", "name_projet", "ref", "sujet", "date", "date_affectation",
    "state", "type_loi_id", "type_pjl", "type_expediteur_id",
    "new_commission_id", "mandat", "session",
    "num_jort", "date_jort", "title_jort", "update_project_id",
    "law_item_original_ids", "write_date",
]

# Bills carry a handful of attachments each; more than this is committee
# paperwork that repeats the same scanned text.
MAX_ATTACHMENTS_PER_BILL = 8
MAX_PDF_BYTES = 40_000_000
MIN_TEXT_CHARS = 300
# How many downloads may come back non-PDF before we call the route broken.
DOWNLOAD_PROBE_SIZE = 12

STATE_LABELS = {
    "draft": "مودع",
    "assigned": "معروض على اللجان",
    "return_to_commission": "أعيد إلى اللجنة",
    "rapport_final": "التقرير جاهز",
    "to_plenary": "معروض على الجلسة العامة",
    "adopted": "تمت المصادقة عليه",
    "refused": "مرفوض",
    "retired": "تم سحبه",
}


def strip_html(raw: str) -> str:
    """Remove HTML tags and decode entities, keeping paragraph breaks."""
    if not raw:
        return ""
    text = html.unescape(raw)
    text = re.sub(r"<(?:br|p|div|h[1-6]|li|tr)[^>]*/?>", "\n", text, flags=re.IGNORECASE)
    text = re.sub(r"<[^>]+>", "", text)
    text = html.unescape(text)
    text = text.replace("\xa0", " ")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n[ \t]+", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def m2o_name(value: Any) -> Optional[str]:
    """Odoo many2one comes back as [id, display_name] or False."""
    if isinstance(value, (list, tuple)) and len(value) == 2:
        return value[1]
    return None


def m2o_id(value: Any) -> Optional[int]:
    if isinstance(value, (list, tuple)) and len(value) == 2:
        return value[0]
    return None


def clean(value: Any) -> Optional[str]:
    """Odoo returns False for empty char/text/date fields."""
    if value is False or value is None:
        return None
    text = str(value).strip()
    return text or None


class ARPScraper(BaseScraper):
    """
    Scraper for TN/ARP -- Tunisian Parliament bills via the public Odoo RPC.
    Country: TN
    URL: https://www.arp.tn
    Data types: legislation
    Auth: none (public portal user)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        # NB: no JSON Accept/Content-Type on the session. Odoo routes on the
        # request's Content-Type, so a session-wide `application/json` made the
        # bodyless GET to /document/download/<id> hit the JSON dispatcher and
        # come back "400 Invalid JSON data: ''" (a 137-byte HTML error page).
        # Every attachment silently failed the %PDF check, so only the 149
        # bills with keyed-in articles produced any text. `json_data=` on the
        # RPC POST sets the JSON content type by itself.
        self.client = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "Mozilla/5.0 (compatible; LegalDataHunter/1.0)",
                "Accept": "*/*",
            },
            timeout=90,
        )
        # Filled once per run by _prefetch_related().
        self._items_by_project: Dict[int, List[dict]] = {}
        self._atts_by_project: Dict[int, List[dict]] = {}
        # Downloads attempted / downloads that returned a real PDF. Used to
        # fail loud if the document route breaks again instead of quietly
        # emitting a metadata-thin corpus.
        self._pdf_tried = 0
        self._pdf_ok = 0

    # ── Odoo JSON-RPC ────────────────────────────────────────────────

    def _rpc(self, model: str, method: str, args: list, kwargs: Optional[dict] = None) -> Any:
        payload = {
            "jsonrpc": "2.0",
            "method": "call",
            "params": {
                "model": model,
                "method": method,
                "args": args,
                "kwargs": kwargs or {},
            },
        }
        self.rate_limiter.wait()
        resp = self.client.post(
            RPC_ENDPOINT, json_data=payload, headers={"Accept": "application/json"}
        )
        if resp.status_code != 200:
            raise RuntimeError(f"{model}.{method}: HTTP {resp.status_code}")
        body = resp.json()
        if "error" in body:
            message = body["error"].get("data", {}).get("message") or body["error"].get("message")
            raise RuntimeError(f"{model}.{method}: {message}")
        return body.get("result")

    def _search_read(self, model: str, domain: list, fields: list,
                     offset: int = 0, limit: int = 0, order: str = "") -> List[dict]:
        return self._rpc(model, "search_read", [domain, fields, offset, limit, order]) or []

    # ── Related data ─────────────────────────────────────────────────

    def _prefetch_related(self, project_ids: Optional[List[int]] = None) -> None:
        """
        Pull every article and every attachment in two calls rather than two
        per bill. Both tables are small (~2k and ~2.8k rows) so this costs a
        couple of seconds and removes ~1,200 round trips.
        """
        domain_items: list = []
        domain_atts: list = [["res_model", "=", "gpl.law.project"],
                             ["mimetype", "=", "application/pdf"]]
        if project_ids:
            domain_items = [["law_project_id", "in", project_ids]]
            domain_atts = domain_atts + [["res_id", "in", project_ids]]

        items = self._search_read(
            "gpl.law.item", domain_items,
            ["name", "item", "law_project_id", "type_id"], order="id asc",
        )
        self._items_by_project = {}
        for item in items:
            pid = m2o_id(item.get("law_project_id"))
            if pid:
                self._items_by_project.setdefault(pid, []).append(item)

        atts = self._search_read(
            "ir.attachment", domain_atts,
            ["name", "res_id", "mimetype", "create_date"], order="id asc",
        )
        self._atts_by_project = {}
        for att in atts:
            pid = att.get("res_id")
            if pid:
                self._atts_by_project.setdefault(pid, []).append(att)

        logger.info(
            "Prefetched %d articles over %d bills and %d PDF attachments over %d bills",
            len(items), len(self._items_by_project), len(atts), len(self._atts_by_project),
        )

    def _articles_text(self, project_id: int) -> str:
        parts = []
        for item in self._items_by_project.get(project_id, []):
            body = strip_html(item.get("item") or "")
            if not body:
                continue
            heading = clean(item.get("name")) or m2o_name(item.get("type_id"))
            parts.append(f"{heading}\n{body}" if heading else body)
        return "\n\n".join(parts).strip()

    def _check_download_route(self, att_id: Any, status: int, body: bytes) -> None:
        """
        A bill whose PDFs are all scans legitimately yields no text, so a single
        bad download is not worth a failure. A run where the FIRST batch of
        downloads never once returns a %PDF means the route itself is broken
        (wrong headers, WAF, moved endpoint) — that must not pass as a merely
        thin corpus, so raise while the evidence is still in hand.
        """
        if self._pdf_ok or self._pdf_tried < DOWNLOAD_PROBE_SIZE:
            return
        raise RuntimeError(
            f"{DOWNLOAD_PATH} returned no PDF in {self._pdf_tried} attempts "
            f"(last: attachment {att_id}, HTTP {status}, "
            f"{len(body)} bytes {body[:80]!r}) — document route is broken"
        )

    def _attachment_text(self, project_id: int) -> str:
        """
        Download and extract the bill's PDFs. Scanned originals have no text
        layer and come back empty; that is expected, not an error, so we just
        keep whatever extracts.
        """
        parts = []
        for att in self._atts_by_project.get(project_id, [])[:MAX_ATTACHMENTS_PER_BILL]:
            att_id = att.get("id")
            url = f"{BASE_URL}{DOWNLOAD_PATH}/{att_id}"
            try:
                self.rate_limiter.wait()
                self._pdf_tried += 1
                resp = self.client.get(f"{DOWNLOAD_PATH}/{att_id}")
                if resp.status_code != 200:
                    self._check_download_route(att_id, resp.status_code, b"")
                    continue
                raw = resp.content
                if not raw.startswith(b"%PDF"):
                    self._check_download_route(att_id, resp.status_code, raw)
                    continue
                self._pdf_ok += 1
                if len(raw) > MAX_PDF_BYTES:
                    continue
                text = extract_pdf_markdown(
                    SOURCE_ID, f"{project_id}/{att_id}",
                    pdf_bytes=raw, table="legislation", force=True,
                )
            except Exception as exc:
                logger.debug("Attachment %s failed: %s", att_id, exc)
                continue
            if not text or len(text.strip()) < 200:
                continue
            label = clean(att.get("name")) or f"وثيقة {att_id}"
            parts.append(f"[{label}] ({url})\n{text.strip()}")
        return "\n\n".join(parts).strip()

    # ── Fetching ─────────────────────────────────────────────────────

    def fetch_all(self) -> Generator[dict, None, None]:
        total = self._rpc("gpl.law.project", "search_count", [[]])
        logger.info("gpl.law.project holds %s bills", total)
        self._prefetch_related()

        offset, page_size = 0, 100
        while True:
            batch = self._search_read(
                "gpl.law.project", [], PROJECT_FIELDS,
                offset=offset, limit=page_size, order="date desc, id desc",
            )
            if not batch:
                break
            logger.info("Fetched bills %d-%d", offset + 1, offset + len(batch))
            for row in batch:
                yield row
            offset += len(batch)
            if offset >= (total or 0):
                break

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """
        `since` is the last crawl time, so filter on write_date -- when the
        record last changed on arp.tn -- not on the bill's deposit date.
        """
        stamp = since.strftime("%Y-%m-%d %H:%M:%S")
        domain = [["write_date", ">=", stamp]]
        rows = self._search_read(
            "gpl.law.project", domain, PROJECT_FIELDS, order="write_date desc",
        )
        logger.info("%d bills modified since %s", len(rows), stamp)
        if not rows:
            return
        self._prefetch_related([r["id"] for r in rows])
        for row in rows:
            yield row

    # ── Normalisation ────────────────────────────────────────────────

    def normalize(self, raw: dict) -> Optional[dict]:
        project_id = raw.get("id")
        if not project_id:
            return None

        title = (
            clean(raw.get("sujet"))
            or clean(raw.get("title_jort"))
            or clean(raw.get("name_projet"))
            or clean(raw.get("name"))
        )
        if not title:
            return None
        title = re.sub(r"\s+", " ", title)

        sections = []
        articles = self._articles_text(project_id)
        if articles:
            sections.append(articles)
        attachments = self._attachment_text(project_id)
        if attachments:
            sections.append(attachments)

        text = "\n\n".join(sections).strip()
        if len(text) < MIN_TEXT_CHARS:
            # Only scanned paper originals — nothing to index.
            return None

        state = clean(raw.get("state"))
        date = clean(raw.get("date"))
        if date:
            date = date[:10]

        return {
            "_id": f"{SOURCE_ID}/{project_id}",
            "_source": SOURCE_ID,
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date,
            "url": f"{BASE_URL}/loi/project/{project_id}",
            "project_id": project_id,
            "ref": clean(raw.get("ref")),
            "law_type": m2o_name(raw.get("type_loi_id")),
            "nature": m2o_name(raw.get("type_pjl")),
            "initiator": m2o_name(raw.get("type_expediteur_id")),
            "committee": m2o_name(raw.get("new_commission_id")),
            "mandate": m2o_name(raw.get("mandat")),
            "session": m2o_name(raw.get("session")),
            "state": state,
            "state_label": STATE_LABELS.get(state or "", state),
            "jort_number": clean(raw.get("num_jort")),
            "jort_date": (clean(raw.get("date_jort")) or "")[:10] or None,
            "jort_title": clean(raw.get("title_jort")),
            "enacted_law": clean(raw.get("update_project_id")),
            "article_count": len(self._items_by_project.get(project_id, [])),
            "attachment_count": len(self._atts_by_project.get(project_id, [])),
            "language": "ar",
            "modified": clean(raw.get("write_date")),
        }


# ── CLI entry point ──────────────────────────────────────────────────

def main():
    import argparse

    parser = argparse.ArgumentParser(description="TN/ARP bootstrap")
    sub = parser.add_subparsers(dest="command")

    boot = sub.add_parser("bootstrap", help="Full bootstrap or sample")
    boot.add_argument("--sample", action="store_true", help="Fetch sample only")
    boot.add_argument("--full", action="store_true", help="Full fetch (all bills)")

    sub.add_parser("update", help="Incremental update")
    sub.add_parser("test", help="Connectivity test")

    args = parser.parse_args()
    scraper = ARPScraper()

    if args.command == "test":
        total = scraper._rpc("gpl.law.project", "search_count", [[]])
        logger.info("OK: %s bills exposed by arp.tn", total)
        rows = scraper._search_read(
            "gpl.law.project", [], ["ref", "sujet", "date"], limit=1, order="date desc",
        )
        if not rows:
            logger.error("FAILED: no data returned")
            sys.exit(1)
        logger.info("Newest: %s -- %s", rows[0].get("ref"), rows[0].get("sujet"))

    elif args.command == "bootstrap":
        sample_mode = args.sample and not args.full
        stats = scraper.bootstrap(sample_mode=sample_mode, sample_size=15)
        logger.info(f"Bootstrap complete: {stats}")

    elif args.command == "update":
        stats = scraper.update()
        logger.info(f"Update complete: {stats}")

    else:
        parser.print_help()


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI dispatches
    # on the literal command name, so alias it (issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
