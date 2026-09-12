#!/usr/bin/env python3
"""
Uzbekistan — Departmental Normative Legal Acts (State Register, Ministry of Justice)

Departmental normative-legal acts (ведомственные нормативно-правовые акты /
idoraviy normativ-huquqiy hujjatlar) are agency- and ministry-level regulations
that, to take legal effect, must be registered in the State Register held by the
Ministry of Justice. The MoJ registry page (minjust.uz/adliya.uz) has been folded
into the gov.uz portal and now only lists registration metadata, but the FULL TEXT
of every registered departmental act is published on the official National Database
of Legislation, lex.uz.

This fetcher therefore pulls departmental acts from lex.uz, filtered to the document
forms that are characteristic of departmental NPAs (Приказ/Buyruq, Положение/Nizom,
Правила/Qoidalar, Инструкция/Yo'riqnoma, Указания, Решение, Регламент, Порядок).
These are distinct from the laws / codes / presidential & cabinet acts already
covered by UZ/LexUz.

Two text layouts are handled:
  * Older acts are server-rendered as HTML inside <div id="divCont"> with semantic
    classes (ACT_TEXT, BY_DEFAULT, ...). Text holders are <a id="N"> or
    <div name="N" id="N">. Russian pages are sometimes empty shells (act published
    only in Uzbek) — we fetch both languages and keep whichever has more text.
  * Recent acts are published as text-based PDFs embedded via PDFObject; the binary
    lives at /pdffile/{id}. We extract those with PyMuPDF.

No authentication required. Russian and Uzbek (Latin) languages.
"""

import json
import logging
import re
import subprocess
import sys
import time
import urllib.parse
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Iterator, Optional, List

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

SOURCE_ID = "UZ/MinjustRegistry"
BASE_URL = "https://lex.uz"

# Document forms characteristic of departmental normative-legal acts registered
# with the Ministry of Justice (form_id values from lex.uz search form).
DEPARTMENTAL_FORMS = {
    '486': 'Приказ',       # Order / Buyruq
    '487': 'Положение',    # Regulation / Nizom
    '488': 'Правила',      # Rules / Qoidalar
    '489': 'Инструкция',   # Instruction / Yo'riqnoma
    '490': 'Указания',     # Directives
    '491': 'Решение',      # Decision
    '575': 'Регламент',    # Reglament
    '573': 'Порядок',      # Procedure / Tartib
}

CONTENT_CLASSES = [
    'ACT_FORM', 'ACT_TITLE', 'ACT_TEXT', 'BY_DEFAULT', 'GRIF_PARLAMENT',
    'DEPARTMENTAL', 'ACCEPTING_BODY', 'SIGNATURE', 'ACT_TITLE_APPL',
    'ACT_ESSENTIAL_ELEMENTS', 'ACT_ESSENTIAL_ELEMENTS_NUM',
]
BODY_CLASSES = {'ACT_TEXT', 'BY_DEFAULT', 'GRIF_PARLAMENT', 'ACT_TITLE_APPL'}
_CLS = '|'.join(CONTENT_CLASSES)

# A content element: <div class="CLASS lx_elem" ...><div class="lx_elem2">
#   <div class="lx_elem3"> ...chrome... </div></div>  followed by the text holder
#   which is either <a id="N">TEXT</a> or <div name="N" id="N">TEXT</div>.
ELEM_RE = re.compile(
    r'<div class="(' + _CLS + r') lx_elem"[^>]*>'
    r'<div class="lx_elem2"><div class="lx_elem3">.*?</div></div>'
    r'(?:<a id="\d+">(?P<a>.*?)</a>|<div name="\d+" id="\d+">(?P<d>.*?)</div>)',
    re.DOTALL,
)


def strip_html(text: str) -> str:
    """Remove HTML tags and decode common entities."""
    text = re.sub(r'<br\s*/?>', '\n', text)
    text = re.sub(r'</p>', '\n', text)
    text = re.sub(r'<[^>]+>', '', text)
    for a, b in [('&nbsp;', ' '), ('&amp;', '&'), ('&lt;', '<'),
                 ('&gt;', '>'), ('&quot;', '"'), ('&#39;', "'"), ('&laquo;', '«'),
                 ('&raquo;', '»')]:
        text = text.replace(a, b)
    return text


def parse_title_meta(html: str) -> Dict[str, str]:
    """Extract registration number / date / title from the <title> tag.

    Two formats appear:
      RU older : "1257-сон 16.07.2003. О порядке проведения тестирования..."
      UZ recent: "3843 03.06.2026 Ko'chmas mulkni texnik inventarizatsiyadan..."
    """
    m = re.search(r'<title>(.*?)</title>', html, re.DOTALL)
    if not m:
        return {}
    raw = strip_html(m.group(1)).strip()
    dm = re.match(r'([\w\-]+?)\s+(\d{2}\.\d{2}\.\d{4})\.?\s*(.*)', raw, re.DOTALL)
    if dm:
        date_str = dm.group(2)
        try:
            iso = datetime.strptime(date_str, '%d.%m.%Y').strftime('%Y-%m-%d')
        except ValueError:
            iso = ''
        return {
            'doc_number': dm.group(1).strip(),
            'date': iso,
            'title': re.sub(r'\s+', ' ', dm.group(3)).strip(),
        }
    return {'title': re.sub(r'\s+', ' ', raw).strip()}


def extract_html_text(html: str) -> Dict[str, Any]:
    """Extract structured fields from a server-rendered lex.uz document page."""
    body_parts: List[str] = []
    form = ''
    title = ''
    reg = ''
    signature = ''
    for m in ELEM_RE.finditer(html):
        cls = m.group(1)
        raw = m.group('a') if m.group('a') is not None else m.group('d')
        txt = strip_html(raw).strip()
        if not txt:
            continue
        if cls == 'ACT_FORM' and not form:
            form = txt
        elif cls == 'ACT_TITLE' and not title:
            title = txt
        elif cls == 'DEPARTMENTAL':
            rm = re.search(r'(?:рег\.?\s*№|р[ўу]йхат рақами|ro[‘\'`]yxat raqami)\s*([\w\-/]+)',
                           txt, re.IGNORECASE)
            if rm:
                reg = rm.group(1)
        elif cls == 'SIGNATURE':
            signature = txt
        elif cls in BODY_CLASSES:
            body_parts.append(txt)
    body = '\n\n'.join(body_parts)
    if signature:
        body += f"\n\n{signature}"
    body = re.sub(r'[ \t]+', ' ', body)
    body = re.sub(r'\n{3,}', '\n\n', body).strip()
    return {'form': form, 'title': title, 'reg': reg, 'text': body}


class MinjustRegistryFetcher:
    """Fetcher for Uzbekistan departmental normative-legal acts via lex.uz."""

    def __init__(self, slow_mode: bool = False):
        self.doc_delay = 3.0 if slow_mode else 1.5
        self.page_delay = 5.0 if slow_mode else 2.0

    # ---------------------------------------------------------------- http
    def _curl(self, url: str, binary_out: Optional[str] = None,
              max_attempts: int = 3) -> Optional[Any]:
        for attempt in range(max_attempts):
            try:
                cmd = ['curl', '-s', '-L', '--max-time', '50',
                       '-H', 'User-Agent: Mozilla/5.0 (compatible; LegalDataHunter/1.0)',
                       '-H', 'Accept-Language: ru,en;q=0.5', url]
                if binary_out:
                    cmd = ['curl', '-s', '-L', '--max-time', '90',
                           '-H', 'User-Agent: Mozilla/5.0 (compatible; LegalDataHunter/1.0)',
                           '-o', binary_out, url]
                    r = subprocess.run(cmd, capture_output=True, timeout=120)
                    if r.returncode == 0 and Path(binary_out).exists() and Path(binary_out).stat().st_size > 0:
                        return True
                else:
                    r = subprocess.run(cmd, capture_output=True, text=True, timeout=70)
                    if r.returncode == 0 and r.stdout:
                        return r.stdout
            except subprocess.TimeoutExpired:
                pass
            delay = min(5 * (2 ** attempt), 30)
            logger.warning(f"GET failed attempt {attempt+1} for {url}, waiting {delay}s...")
            time.sleep(delay)
        return None

    # ------------------------------------------------------------- search
    def _parse_search(self, html: str) -> List[Dict[str, str]]:
        results = []
        for m in re.finditer(
            r'<a[^>]+class="lx_link"[^>]+href="(?:/[a-z]{2})?/docs/(\d+)[^"]*"[^>]*>(.*?)</a>',
            html, re.DOTALL,
        ):
            results.append({'doc_id': m.group(1),
                            'title': strip_html(m.group(2)).strip()})
        # de-dup while preserving order
        seen = set()
        uniq = []
        for r in results:
            if r['doc_id'] in seen:
                continue
            seen.add(r['doc_id'])
            uniq.append(r)
        return uniq

    def search_form(self, form_id: str) -> List[Dict[str, str]]:
        url = f"{BASE_URL}/ru/search/nat?form_id={form_id}"
        html = self._curl(url)
        if not html:
            logger.warning(f"Search failed for form {form_id}")
            return []
        results = self._parse_search(html)
        for r in results:
            r['form_id'] = form_id
        logger.info(f"Form {form_id} ({DEPARTMENTAL_FORMS.get(form_id,'')}): {len(results)} acts")
        return results

    def iter_candidates(self) -> Iterator[Dict[str, str]]:
        seen = set()
        for fid in DEPARTMENTAL_FORMS:
            for r in self.search_form(fid):
                if r['doc_id'] in seen:
                    continue
                seen.add(r['doc_id'])
                yield r
            time.sleep(self.page_delay)

    # -------------------------------------------------------------- fetch
    def _extract_pdf(self, doc_id: str) -> str:
        try:
            import fitz  # PyMuPDF
        except ImportError:
            logger.warning("PyMuPDF not available; cannot extract PDF text")
            return ''
        tmp = f"/tmp/lexuz_{doc_id}.pdf"
        if not self._curl(f"{BASE_URL}/pdffile/{doc_id}", binary_out=tmp):
            return ''
        try:
            doc = fitz.open(tmp)
            text = '\n'.join(p.get_text() for p in doc)
            doc.close()
        except Exception as e:
            logger.warning(f"PDF parse failed for {doc_id}: {e}")
            text = ''
        finally:
            try:
                Path(tmp).unlink()
            except OSError:
                pass
        text = re.sub(r'[ \t]+', ' ', text)
        text = re.sub(r'\n{3,}', '\n\n', text)
        return text.strip()

    def fetch_document(self, doc_id: str, form_id: str = '') -> Optional[Dict[str, Any]]:
        # Try both language renderings; keep whichever has more HTML body text.
        best = {'text': ''}
        meta = {}
        for lang_url in (f"{BASE_URL}/ru/docs/{doc_id}", f"{BASE_URL}/docs/{doc_id}"):
            html = self._curl(lang_url)
            if not html:
                continue
            if not meta.get('title') or not meta.get('date'):
                m = parse_title_meta(html)
                for k, v in m.items():
                    if v and not meta.get(k):
                        meta[k] = v
            parsed = extract_html_text(html)
            if len(parsed['text']) > len(best['text']):
                parsed['url'] = lang_url
                best = parsed
            time.sleep(0.6)

        source = 'html'
        text = best.get('text', '')
        # Fall back to PDF if HTML rendering carried no real body.
        if len(text) < 200:
            pdf_text = self._extract_pdf(doc_id)
            if len(pdf_text) >= len(text):
                text = pdf_text
                source = 'pdf'

        if len(text) < 200:
            logger.warning(f"Doc {doc_id}: no usable full text")
            return None

        title = best.get('title') or meta.get('title', '')
        form = best.get('form') or DEPARTMENTAL_FORMS.get(form_id, '')
        return {
            'doc_id': doc_id,
            'title': title,
            'text': text,
            'date': meta.get('date', ''),
            'doc_number': meta.get('doc_number', ''),
            'reg_number': best.get('reg', '') or meta.get('doc_number', ''),
            'doc_type': form,
            'text_source': source,
            'url': f"{BASE_URL}/docs/{doc_id}",
        }

    # ---------------------------------------------------------- pipeline
    def fetch_all(self) -> Iterator[Dict[str, Any]]:
        count = 0
        for cand in self.iter_candidates():
            doc = self.fetch_document(cand['doc_id'], cand.get('form_id', ''))
            if doc:
                yield doc
                count += 1
            time.sleep(self.doc_delay)
        logger.info(f"Fetched {count} departmental acts total")

    def fetch_updates(self, since: datetime) -> Iterator[Dict[str, Any]]:
        # lex.uz search returns most-recent first; stop once we pass `since`.
        for cand in self.iter_candidates():
            doc = self.fetch_document(cand['doc_id'], cand.get('form_id', ''))
            if not doc:
                continue
            if doc.get('date'):
                try:
                    if datetime.strptime(doc['date'], '%Y-%m-%d') < since:
                        continue
                except ValueError:
                    pass
            yield doc
            time.sleep(self.doc_delay)

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        return {
            '_id': str(raw.get('doc_id', '')),
            '_source': SOURCE_ID,
            '_type': 'legislation',
            '_fetched_at': datetime.now().isoformat(),
            'title': raw.get('title', ''),
            'text': raw.get('text', ''),
            'date': raw.get('date', ''),
            'doc_number': raw.get('doc_number', ''),
            'reg_number': raw.get('reg_number', ''),
            'doc_type': raw.get('doc_type', ''),
            'text_source': raw.get('text_source', ''),
            'url': raw.get('url', ''),
        }


def bootstrap_sample(slow_mode: bool = False, target: int = 15):
    sample_dir = Path(__file__).parent / 'sample'
    sample_dir.mkdir(exist_ok=True)
    for f in sample_dir.glob('*.json'):
        f.unlink()

    fetcher = MinjustRegistryFetcher(slow_mode=slow_mode)
    count = 0
    for cand in fetcher.iter_candidates():
        if count >= target:
            break
        doc_id = cand['doc_id']
        logger.info(f"[{count+1}/{target}] Fetching {doc_id}: {cand.get('title','')[:55]}...")
        doc = fetcher.fetch_document(doc_id, cand.get('form_id', ''))
        if not doc:
            continue
        norm = fetcher.normalize(doc)
        if not norm['text'] or len(norm['text']) < 200:
            continue
        with open(sample_dir / f"{doc_id}.json", 'w', encoding='utf-8') as f:
            json.dump(norm, f, ensure_ascii=False, indent=2)
        count += 1
        logger.info(f"  Saved {doc_id}.json ({len(norm['text'])} chars, {norm['text_source']})")
        time.sleep(fetcher.doc_delay)

    logger.info(f"\nSample complete: {count} documents -> {sample_dir}/")
    validate_sample(sample_dir)


def validate_sample(sample_dir: Path):
    files = list(sample_dir.glob('*.json'))
    if not files:
        logger.error("No sample files found!")
        return
    total = len(files)
    has_text = has_title = has_date = 0
    lengths = []
    for f in files:
        d = json.load(open(f, encoding='utf-8'))
        if d.get('text') and len(d['text']) > 200:
            has_text += 1
            lengths.append(len(d['text']))
        if d.get('title'):
            has_title += 1
        if d.get('date'):
            has_date += 1
    logger.info("\n=== VALIDATION SUMMARY ===")
    logger.info(f"Total samples : {total}")
    logger.info(f"With full text: {has_text}/{total}")
    logger.info(f"With title    : {has_title}/{total}")
    logger.info(f"With date     : {has_date}/{total}")
    if lengths:
        logger.info(f"Text length   : min={min(lengths)} avg={sum(lengths)//len(lengths)} max={max(lengths)}")
    if total >= 10 and has_text >= 10:
        logger.info("PASS: 10+ documents with full text")
    else:
        logger.warning(f"FAIL: need 10+ docs with text, got {has_text}")


if __name__ == '__main__':
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    import argparse
    parser = argparse.ArgumentParser(description='Uzbekistan Departmental NPA (MoJ Registry) Fetcher')
    parser.add_argument('command', choices=['bootstrap', 'validate'])
    parser.add_argument('--sample', action='store_true', help='Fetch sample data only')
    parser.add_argument('--slow', action='store_true', help='Slower rate limiting')
    parser.add_argument('--full', action='store_true', help='Fetch all records')
    args = parser.parse_args()

    if args.command == 'bootstrap':
        if args.sample:
            bootstrap_sample(slow_mode=args.slow)
        else:
            logger.info("Use --sample for bootstrap sampling.")
    elif args.command == 'validate':
        validate_sample(Path(__file__).parent / 'sample')
