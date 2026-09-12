#!/usr/bin/env python3
"""
CH/Fedlex - Swiss Federal Legislation

Two corpora are fetched from the Fedlex SPARQL endpoint:

1. The Classified Compilation (Systematische Rechtssammlung / Recueil
   systematique) — the *consolidated* federal law in force, split into
   individual articles.  This is the part lawyers actually cite
   ("OR Art. 814", "ZGB Art. 14") and it is emitted one record per
   article so a citation resolves to exactly one document.
2. The recent acts published in the Federal Gazette (jolux:Act), kept
   from the original implementation for coverage of amending acts.

The consolidated compilation is crawled FIRST: it is the substantive
corpus, and the gazette walk below it is unbounded (issue #1615 — the
gazette crawl consumed the whole run, so SR 210/220 were never indexed).
"""

import argparse
import json
import os
import re
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Generator, Iterable, Optional
from html import unescape

import requests
from bs4 import BeautifulSoup

# Configuration
SPARQL_ENDPOINT = "https://fedlex.data.admin.ch/sparqlendpoint"
RATE_LIMIT_DELAY = 0.5  # seconds between requests

SOURCE_DIR = Path(__file__).parent

# Languages emitted for the consolidated compilation.  German and French
# are the two most-cited official languages; "it"/"rm"/"en" also exist.
DEFAULT_LANGS = ("de", "fr")

LANG_URI = {
    "de": "http://publications.europa.eu/resource/authority/language/DEU",
    "fr": "http://publications.europa.eu/resource/authority/language/FRA",
    "it": "http://publications.europa.eu/resource/authority/language/ITA",
    "rm": "http://publications.europa.eu/resource/authority/language/ROH",
    "en": "http://publications.europa.eu/resource/authority/language/ENG",
}

# Datatype that marks a taxonomy notation as an SR ("systematic") number.
SR_NOTATION_TYPE = (
    "https://fedlex.data.admin.ch/vocabulary/notation-type/id-systematique"
)
HTML_USER_FORMAT = "https://fedlex.data.admin.ch/vocabulary/user-format/html"

# JOLux ontology prefixes
PREFIXES = """
PREFIX jolux: <http://data.legilux.public.lu/resource/ontology/jolux#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX dcterms: <http://purl.org/dc/terms/>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
"""


def sparql_query(query: str, timeout: int = 60, max_retries: int = 4) -> dict:
    """Execute SPARQL query with retry and exponential backoff."""
    headers = {
        "Accept": "application/sparql-results+json",
        "User-Agent": "LegalDataHunter/1.0 (research project)"
    }

    last_exc = None
    for attempt in range(max_retries + 1):
        try:
            response = requests.post(
                SPARQL_ENDPOINT,
                data={"query": PREFIXES + query},
                headers=headers,
                timeout=timeout,
            )
            response.raise_for_status()
            return response.json()
        except (requests.exceptions.HTTPError, requests.exceptions.ConnectionError,
                requests.exceptions.Timeout) as e:
            last_exc = e
            if attempt < max_retries:
                wait = min(2 ** attempt * 2, 60)  # 2s, 4s, 8s, 16s (capped 60s)
                status = getattr(getattr(e, "response", None), "status_code", "?")
                print(f"  SPARQL retry {attempt+1}/{max_retries} after {status} (wait {wait}s)", file=sys.stderr)
                time.sleep(wait)

    raise last_exc  # type: ignore[misc]


def sparql_bindings(query: str, timeout: int = 60) -> list:
    return sparql_query(query, timeout=timeout).get("results", {}).get("bindings", [])


def _val(row: dict, key: str, default: str = "") -> str:
    return row.get(key, {}).get("value", default) or default


def clean_date(value: str) -> Optional[str]:
    """Return an ISO date, or None when Fedlex carries an implausible year.

    status.json reported a date_range ending in 2204; a handful of gazette
    entries carry typo'd years, and letting them through poisons the whole
    range.  Future *applicability* dates are legitimate but never decades out.
    """
    if not value:
        return None
    m = re.match(r"(\d{4})-(\d{2})-(\d{2})", value)
    if not m:
        return None
    year = int(m.group(1))
    if year < 1200 or year > date.today().year + 10:
        return None
    return m.group(0)


# ---------------------------------------------------------------------------
# Consolidated Classified Compilation (SR / RS)
# ---------------------------------------------------------------------------


def list_sr_acts() -> list:
    """List every act of the Classified Compilation still in force.

    Returns dicts: {"sr", "abstract", "title", "abbreviation",
    "date_document", "date_entry_in_force"}, one per SR number.
    """
    today = date.today().isoformat()
    query = f"""
    SELECT DISTINCT ?sr ?abstract ?title ?abbr ?dateDoc ?dateForce
    WHERE {{
      ?abstract a jolux:ConsolidationAbstract ;
                jolux:classifiedByTaxonomyEntry/skos:notation ?sr .
      FILTER(datatype(?sr) = <{SR_NOTATION_TYPE}>)
      OPTIONAL {{ ?abstract jolux:dateNoLongerInForce ?dateEnd }}
      FILTER(!BOUND(?dateEnd) || ?dateEnd > "{today}"^^xsd:date)
      OPTIONAL {{ ?abstract jolux:dateDocument ?dateDoc }}
      OPTIONAL {{ ?abstract jolux:dateEntryInForce ?dateForce }}
      ?abstract jolux:isRealizedBy ?expr .
      ?expr jolux:language <{LANG_URI['de']}> ;
            jolux:title ?title .
      OPTIONAL {{ ?expr jolux:titleShort ?abbr }}
    }}
    ORDER BY ?sr
    """

    # An SR number is reused when an act is replaced (SR 101 is both the 1874
    # and the 1999 constitution).  Repealed acts are filtered above; keep one
    # entry per SR number so article _ids cannot collide.
    by_sr = {}
    for row in sparql_bindings(query, timeout=180):
        sr = _val(row, "sr")
        abstract = _val(row, "abstract")
        if not sr or not abstract:
            continue
        act = {
            "sr": sr,
            "abstract": abstract,
            "title": _val(row, "title"),
            "abbreviation": _val(row, "abbr"),
            "date_document": clean_date(_val(row, "dateDoc")),
            "date_entry_in_force": clean_date(_val(row, "dateForce")),
        }
        previous = by_sr.get(sr)
        if previous is None or (act["date_entry_in_force"] or "") > (previous["date_entry_in_force"] or ""):
            by_sr[sr] = act
    acts = list(by_sr.values())

    def sort_key(act):
        sr = act["sr"]
        # SR numbers starting with "0." are international treaties; domestic
        # law is what citations resolve against, so crawl it first.
        parts = sr.split(".")
        return (1 if sr.startswith("0.") else 0,
                [int(p) if p.isdigit() else 0 for p in parts], sr)

    acts.sort(key=sort_key)
    return acts


def current_consolidation(abstract_uri: str, lang: str, on: Optional[str] = None) -> Optional[dict]:
    """Newest consolidation of `abstract_uri` in force on `on` (default today).

    Fedlex publishes future consolidations too (an act amended with effect in
    2027 already has a 2027 version), so the applicability date must be capped
    at today or the crawl stores text that is not yet law.
    """
    on = on or date.today().isoformat()
    query = f"""
    SELECT ?consolidation ?dateAppl ?fileUrl
    WHERE {{
      ?consolidation jolux:isMemberOf <{abstract_uri}> ;
                     jolux:dateApplicability ?dateAppl ;
                     jolux:isRealizedBy ?expr .
      FILTER(?dateAppl <= "{on}"^^xsd:date)
      ?expr jolux:language <{LANG_URI[lang]}> ;
            jolux:isEmbodiedBy ?manif .
      ?manif jolux:userFormat <{HTML_USER_FORMAT}> ;
             jolux:isExemplifiedBy ?fileUrl .
    }}
    ORDER BY DESC(?dateAppl)
    LIMIT 1
    """
    rows = sparql_bindings(query)
    if not rows:
        return None
    return {
        "consolidation": _val(rows[0], "consolidation"),
        "date_applicability": _val(rows[0], "dateAppl"),
        "file_url": _val(rows[0], "fileUrl"),
    }


def _element_text(element) -> str:
    """Readable text of one HTML element, footnote markers stripped."""
    for sup in element.find_all("sup"):
        # Footnote back-references are anchors inside <sup>; paragraph
        # numbers ("1", "2", ...) are bare <sup> and are worth keeping.
        if sup.find("a"):
            sup.decompose()
    for junk in element(["script", "style"]):
        junk.decompose()
    text = element.get_text(separator=" ", strip=True)
    text = re.sub(r"\s*\n\s*", "\n", text)
    text = re.sub(r"[ \t ]{2,}", " ", text)
    return text.strip()


def parse_consolidated_html(html: str) -> dict:
    """Split a consolidated act into its articles.

    Returns {"sr", "title", "status_line", "articles": [{id, number, heading, text}]}.
    """
    soup = BeautifulSoup(html, "html.parser")

    preface = soup.find(id="preface")
    sr_number = ""
    act_title = ""
    status_line = ""
    if preface:
        srn = preface.find("p", class_="srnummer")
        if srn:
            sr_number = srn.get_text(strip=True)
        h1 = preface.find(["h1", "h2"])
        if h1:
            act_title = re.sub(r"\s+", " ", h1.get_text(separator=" ", strip=True))
        stat = preface.find("p", class_="erlassdatum")
        if stat:
            status_line = stat.get_text(strip=True)

    def breadcrumb(element) -> str:
        """Titles of the enclosing book/title/chapter sections, outermost first."""
        crumbs = []
        for parent in element.parents:
            if getattr(parent, "name", None) != "section":
                continue
            head = parent.find(class_="heading", recursive=False)
            if head:
                for sup in head.find_all("sup"):
                    if sup.find("a"):  # footnote marker, not part of the title
                        sup.decompose()
                crumbs.append(re.sub(r"\s+", " ", head.get_text(separator=" ", strip=True)))
        return " > ".join(reversed(crumbs))

    articles = []
    for art in soup.find_all("article"):
        art_id = art.get("id") or ""
        if not art_id:
            continue
        context = breadcrumb(art)
        heading_el = art.find(["h1", "h2", "h3", "h4", "h5", "h6"], class_="heading")
        heading = ""
        number = art_id.rsplit("/", 1)[-1]
        if heading_el:
            for sup in heading_el.find_all("sup"):
                # Footnote reference markers, otherwise they read as part of
                # the article number ("Art. 620 312").
                if sup.find("a"):
                    sup.decompose()
            raw_heading = re.sub(r"\s+", " ", heading_el.get_text(separator=" ", strip=True))
            # Headings read "Art. 814" or "Art. 814 a. Vertretung"; keep the
            # descriptive tail as the heading, the number is already in art_id.
            m = re.match(r"^Art\.?\s*([0-9][0-9a-zA-Z]*(?:\s*[a-z]+)?)\s*(.*)$", raw_heading)
            if m:
                number = m.group(1).replace(" ", "")
                heading = m.group(2).strip()
            else:
                heading = raw_heading
            heading_el.decompose()

        body = _element_text(art)
        if not body:
            continue
        articles.append({
            "id": art_id,
            "number": number,
            "heading": heading,
            "context": context,
            "text": body,
        })

    return {
        "sr": sr_number,
        "title": act_title,
        "status_line": status_line,
        "articles": articles,
    }


def fetch_html_content(url: str) -> Optional[str]:
    """Fetch HTML content from URL."""
    try:
        headers = {
            "User-Agent": "LegalDataHunter/1.0 (research project)",
            "Accept": "text/html,application/xhtml+xml"
        }
        response = requests.get(url, headers=headers, timeout=60)
        response.raise_for_status()
        # The filestore serves text/html with no charset parameter, so requests
        # falls back to ISO-8859-1 and every umlaut arrives mojibaked; the
        # documents themselves declare (and are) UTF-8.
        if "charset=" not in response.headers.get("Content-Type", "").lower():
            response.encoding = "utf-8"
        return response.text
    except Exception as e:
        print(f"Error fetching {url}: {e}", file=sys.stderr)
        return None


def fetch_act_articles(act: dict, lang: str, on: Optional[str] = None) -> Generator[dict, None, None]:
    """Yield one raw record per article of a consolidated act."""
    time.sleep(RATE_LIMIT_DELAY)
    cons = current_consolidation(act["abstract"], lang, on=on)
    if not cons or not cons["file_url"]:
        return

    time.sleep(RATE_LIMIT_DELAY)
    html = fetch_html_content(cons["file_url"])
    if not html:
        return

    parsed = parse_consolidated_html(html)
    act_title = parsed["title"] or act["title"]
    abbr = act["abbreviation"]
    eli = act["abstract"].replace("https://fedlex.data.admin.ch/eli/", "")
    cons_date = clean_date(cons["date_applicability"]) or ""
    date_slug = cons_date.replace("-", "")
    base_url = f"https://www.fedlex.admin.ch/eli/{eli}/{date_slug}/{lang}"

    common = {
        "kind": "article",
        "sr_number": act["sr"],
        "abbreviation": abbr,
        "act_title": act_title,
        "act_eli": act["abstract"],
        "consolidation_uri": cons["consolidation"],
        "consolidation_date": cons_date,
        "date_document": act.get("date_document") or "",
        "date_entry_in_force": act.get("date_entry_in_force") or "",
        "status_line": parsed["status_line"],
        "language": lang,
        "file_url": cons["file_url"],
    }

    if not parsed["articles"]:
        # Short instruments (many treaties) carry no <article> markup;
        # emit the whole text rather than dropping the act.
        soup = BeautifulSoup(html, "html.parser")
        main = soup.find(id="lawcontent") or soup
        body = _element_text(main)
        if len(body) >= 100:
            yield dict(common, kind="act", article_id="", article_number="",
                       heading="", url=base_url, text=body)
        return

    for article in parsed["articles"]:
        header = f"{act_title}"
        if abbr:
            header += f" ({abbr})"
        header += f"\nSR {act['sr']}"
        if cons_date:
            header += f" — Stand/Etat {cons_date}"
        label = f"Art. {article['number']}"
        if article["heading"]:
            label += f" {article['heading']}"
        if article.get("context"):
            header += f"\n{article['context']}"
        text = f"{header}\n\n{label}\n\n{article['text']}"

        yield dict(common,
                   article_id=article["id"],
                   article_number=article["number"],
                   heading=article["heading"],
                   context=article.get("context", ""),
                   url=f"{base_url}#{article['id']}",
                   text=text)


def fetch_classified_compilation(langs: Iterable[str] = DEFAULT_LANGS,
                                 on: Optional[str] = None) -> Generator[dict, None, None]:
    """Yield every article of every act in force in the Classified Compilation."""
    acts = list_sr_acts()
    print(f"Classified Compilation: {len(acts)} acts", file=sys.stderr)

    for i, act in enumerate(acts, 1):
        for lang in langs:
            try:
                n = 0
                for record in fetch_act_articles(act, lang, on=on):
                    n += 1
                    yield record
                if n:
                    print(f"[{i}/{len(acts)}] SR {act['sr']} ({lang}): {n} articles",
                          file=sys.stderr)
            except Exception as e:  # keep the crawl alive on a single bad act
                print(f"[{i}/{len(acts)}] SR {act['sr']} ({lang}) failed: {e}",
                      file=sys.stderr)


# ---------------------------------------------------------------------------
# Federal Gazette acts (original implementation)
# ---------------------------------------------------------------------------


def get_recent_acts(limit: int = 100, offset: int = 0) -> list:
    """Get recent legislation acts with metadata."""
    query = f"""
    SELECT DISTINCT ?act ?dateDoc ?processType ?genre ?typeDoc
    WHERE {{
      ?act a jolux:Act ;
           jolux:dateDocument ?dateDoc .
      OPTIONAL {{ ?act jolux:processType ?processType }}
      OPTIONAL {{ ?act jolux:legalResourceGenre ?genre }}
      OPTIONAL {{ ?act jolux:typeDocument ?typeDoc }}
      FILTER(?dateDoc >= "2020-01-01"^^xsd:date)
    }}
    ORDER BY DESC(?dateDoc)
    LIMIT {limit}
    OFFSET {offset}
    """

    result = sparql_query(query)
    return result.get("results", {}).get("bindings", [])


def get_act_expressions(act_uri: str) -> list:
    """Get language expressions for an act."""
    query = f"""
    SELECT ?expr ?title ?lang
    WHERE {{
      <{act_uri}> jolux:isRealizedBy ?expr .
      ?expr jolux:title ?title .
      OPTIONAL {{ ?expr jolux:language ?lang }}
    }}
    """

    result = sparql_query(query)
    return result.get("results", {}).get("bindings", [])


def get_expression_files(expr_uri: str) -> list:
    """Get file manifestations for an expression."""
    query = f"""
    SELECT ?manif ?format ?fileUrl
    WHERE {{
      <{expr_uri}> jolux:isEmbodiedBy ?manif .
      ?manif jolux:format ?format .
      OPTIONAL {{ ?manif jolux:isExemplifiedBy ?fileUrl }}
    }}
    """

    result = sparql_query(query)
    return result.get("results", {}).get("bindings", [])


def extract_text_from_html(html_content: str) -> str:
    """Extract clean text from HTML content."""
    soup = BeautifulSoup(html_content, "html.parser")

    # Remove script and style elements
    for element in soup(["script", "style", "meta", "link"]):
        element.decompose()

    # Get text content
    text = soup.get_text(separator="\n", strip=True)

    # Clean up excessive whitespace
    text = re.sub(r'\n{3,}', '\n\n', text)
    text = re.sub(r' {2,}', ' ', text)

    return text.strip()


def fetch_document(act_uri: str) -> Optional[dict]:
    """Fetch a single document with full text."""
    time.sleep(RATE_LIMIT_DELAY)

    # Get expressions (language versions)
    expressions = get_act_expressions(act_uri)
    if not expressions:
        return None

    # Prefer German, then French, then Italian
    lang_priority = ["DEU", "FRA", "ITA", "ROH"]  # Romansh last
    best_expr = None
    best_title = None
    best_lang = None

    for expr in expressions:
        lang_uri = expr.get("lang", {}).get("value", "")
        lang_code = lang_uri.split("/")[-1] if lang_uri else "UNK"

        if best_expr is None:
            best_expr = expr["expr"]["value"]
            best_title = expr["title"]["value"]
            best_lang = lang_code
        elif lang_code in lang_priority:
            current_idx = lang_priority.index(best_lang) if best_lang in lang_priority else 99
            new_idx = lang_priority.index(lang_code)
            if new_idx < current_idx:
                best_expr = expr["expr"]["value"]
                best_title = expr["title"]["value"]
                best_lang = lang_code

    if not best_expr:
        return None

    time.sleep(RATE_LIMIT_DELAY)

    # Get file manifestations
    files = get_expression_files(best_expr)

    # Find HTML file (preferred) or XML
    html_url = None
    xml_url = None

    for f in files:
        format_uri = f.get("format", {}).get("value", "")
        file_url = f.get("fileUrl", {}).get("value", "")

        if "HTML" in format_uri and file_url and "-an" not in file_url:
            html_url = file_url
        elif "XML" in format_uri and file_url and "-an" not in file_url:
            xml_url = file_url

    # Fetch full text
    content_url = html_url or xml_url
    if not content_url:
        return None

    time.sleep(RATE_LIMIT_DELAY)

    raw_content = fetch_html_content(content_url)
    if not raw_content:
        return None

    text = extract_text_from_html(raw_content)
    if not text or len(text) < 50:
        return None

    return {
        "kind": "gazette",
        "eli_uri": act_uri,
        "expression_uri": best_expr,
        "title": best_title,
        "language": best_lang,
        "file_url": content_url,
        "text": text
    }


def fetch_gazette_acts() -> Generator[dict, None, None]:
    """Yield Federal Gazette acts (amending acts, decrees) with full text."""
    offset = 0
    batch_size = 100

    while True:
        print(f"Fetching gazette batch at offset {offset}...", file=sys.stderr)
        acts = get_recent_acts(limit=batch_size, offset=offset)

        if not acts:
            break

        for act in acts:
            act_uri = act["act"]["value"]
            date_doc = act.get("dateDoc", {}).get("value", "")
            process_type = act.get("processType", {}).get("value", "").split("/")[-1]
            genre = act.get("genre", {}).get("value", "").split("/")[-1]
            type_doc = act.get("typeDoc", {}).get("value", "").split("/")[-1]

            doc = fetch_document(act_uri)
            if doc:
                doc["date_document"] = date_doc
                doc["process_type"] = process_type
                doc["genre"] = genre
                doc["type_document"] = type_doc
                yield doc

        offset += batch_size


# Codes used for the validation sample, so it covers several acts and both
# languages instead of the first dozen articles of the constitution.
SAMPLE_SR = ["101", "210", "220", "311.0", "312.0", "642.11"]


def fetch_sample(langs: Iterable[str] = DEFAULT_LANGS,
                 per_act: int = 2) -> Generator[dict, None, None]:
    """Yield a few articles from each of the best-known federal codes."""
    acts = {a["sr"]: a for a in list_sr_acts()}
    for sr in SAMPLE_SR:
        act = acts.get(sr)
        if not act:
            continue
        for lang in langs:
            emitted = 0
            for record in fetch_act_articles(act, lang):
                if len(record.get("text", "")) < 200:
                    continue  # marginal one-liner, pick a meatier article
                yield record
                emitted += 1
                if emitted >= per_act:
                    break


def fetch_all(langs: Iterable[str] = DEFAULT_LANGS) -> Generator[dict, None, None]:
    """Yield all Swiss legislation: consolidated compilation first, then gazette."""
    yield from fetch_classified_compilation(langs)
    yield from fetch_gazette_acts()


def fetch_updates(since: datetime, langs: Iterable[str] = DEFAULT_LANGS) -> Generator[dict, None, None]:
    """Yield documents that became applicable / were published since a date.

    For consolidated law the availability signal is `dateApplicability`: a new
    consolidation entering force is exactly what changes the text we store.
    """
    since_str = since.strftime("%Y-%m-%d") if hasattr(since, "strftime") else str(since)[:10]
    today = date.today().isoformat()

    query = f"""
    SELECT DISTINCT ?abstract ?sr ?title ?abbr
    WHERE {{
      ?consolidation jolux:isMemberOf ?abstract ;
                     jolux:dateApplicability ?dateAppl .
      FILTER(?dateAppl >= "{since_str}"^^xsd:date && ?dateAppl <= "{today}"^^xsd:date)
      ?abstract a jolux:ConsolidationAbstract ;
                jolux:classifiedByTaxonomyEntry/skos:notation ?sr .
      FILTER(datatype(?sr) = <{SR_NOTATION_TYPE}>)
      ?abstract jolux:isRealizedBy ?expr .
      ?expr jolux:language <{LANG_URI['de']}> ;
            jolux:title ?title .
      OPTIONAL {{ ?expr jolux:titleShort ?abbr }}
    }}
    """

    seen = set()
    for row in sparql_bindings(query, timeout=180):
        abstract = _val(row, "abstract")
        if not abstract or abstract in seen:
            continue
        seen.add(abstract)
        act = {
            "sr": _val(row, "sr"),
            "abstract": abstract,
            "title": _val(row, "title"),
            "abbreviation": _val(row, "abbr"),
        }
        for lang in langs:
            try:
                yield from fetch_act_articles(act, lang)
            except Exception as e:
                print(f"Update error SR {act['sr']} ({lang}): {e}", file=sys.stderr)

    # Gazette acts modified since the cutoff
    gazette_query = f"""
    SELECT DISTINCT ?act ?dateDoc
    WHERE {{
      ?act a jolux:Act ;
           jolux:dateDocument ?dateDoc ;
           dcterms:modified ?modified .
      FILTER(?modified >= "{since_str}"^^xsd:date)
    }}
    ORDER BY DESC(?dateDoc)
    LIMIT 1000
    """

    for row in sparql_bindings(gazette_query):
        act_uri = _val(row, "act")
        doc = fetch_document(act_uri)
        if doc:
            doc["date_document"] = _val(row, "dateDoc")
            yield doc


def normalize(raw: dict) -> dict:
    """Transform raw data into standard schema."""
    fetched_at = datetime.utcnow().isoformat() + "Z"

    if raw.get("kind") in ("article", "act"):
        sr = raw.get("sr_number", "")
        lang = raw.get("language", "de")
        article_id = raw.get("article_id", "")
        doc_id = f"cc/{sr}/{lang}"
        if article_id:
            doc_id += f"/{article_id}"

        abbr = raw.get("abbreviation", "")
        act_title = raw.get("act_title", "")
        if article_id:
            label = f"Art. {raw.get('article_number', '')}".strip()
            if raw.get("heading"):
                label += f" {raw['heading']}"
            title = f"{abbr or act_title} {label}".strip()
            if abbr:
                title = f"{title} — {act_title}"
        else:
            title = f"{abbr + ' — ' if abbr else ''}{act_title}"

        return {
            "_id": doc_id,
            "_source": "CH/Fedlex",
            "_type": "legislation",
            "_fetched_at": fetched_at,
            "eli_uri": raw.get("act_eli", ""),
            "expression_uri": raw.get("consolidation_uri", ""),
            "title": title,
            "text": raw.get("text", ""),
            "language": lang,
            "date": clean_date(raw.get("date_document", "")) or clean_date(raw.get("consolidation_date", "")),
            "consolidation_date": clean_date(raw.get("consolidation_date", "")),
            "date_entry_in_force": clean_date(raw.get("date_entry_in_force", "")),
            "sr_number": sr,
            "abbreviation": abbr,
            "act_title": act_title,
            "article": raw.get("article_number", ""),
            "article_id": article_id,
            "context": raw.get("context", ""),
            "corpus": "classified_compilation",
            "status_line": raw.get("status_line", ""),
            "url": raw.get("url", ""),
            "file_url": raw.get("file_url", ""),
        }

    eli_uri = raw.get("eli_uri", "")
    eli_id = eli_uri.replace("https://fedlex.data.admin.ch/eli/", "")

    return {
        "_id": eli_id,
        "_source": "CH/Fedlex",
        "_type": "legislation",
        "_fetched_at": fetched_at,
        "eli_uri": eli_uri,
        "expression_uri": raw.get("expression_uri", ""),
        "title": raw.get("title", ""),
        "text": raw.get("text", ""),
        "language": raw.get("language", ""),
        "date": clean_date(raw.get("date_document", "")),
        "corpus": "federal_gazette",
        "process_type": raw.get("process_type", ""),
        "genre": raw.get("genre", ""),
        "type_document": raw.get("type_document", ""),
        "url": f"https://www.fedlex.admin.ch/eli/{eli_id}",
        "file_url": raw.get("file_url", "")
    }


def run_bootstrap(sample: bool = False, sample_count: int = 12,
                  langs: Iterable[str] = DEFAULT_LANGS):
    """Fetch the corpus: samples to sample/, full run to data/records.jsonl."""
    sample_dir = SOURCE_DIR / "sample"
    jsonl_file = None

    if sample:
        sample_dir.mkdir(parents=True, exist_ok=True)
        print(f"Fetching {sample_count} sample documents...", file=sys.stderr)
    else:
        data_dir = SOURCE_DIR / "data"
        data_dir.mkdir(parents=True, exist_ok=True)
        jsonl_file = open(data_dir / "records.jsonl", "w", encoding="utf-8")

    count = 0
    total_chars = 0
    stream = fetch_sample(langs) if sample else fetch_all(langs)
    try:
        for raw_doc in stream:
            normalized = normalize(raw_doc)

            text = normalized.get("text", "")
            if not text or len(text) < 100:
                continue

            if sample:
                safe_id = normalized["_id"].replace("/", "_")
                with open(sample_dir / f"{safe_id}.json", "w", encoding="utf-8") as f:
                    json.dump(normalized, f, ensure_ascii=False, indent=2)
                print(f"Saved: {normalized['_id']} ({len(text)} chars)", file=sys.stderr)
            else:
                jsonl_file.write(json.dumps(normalized, ensure_ascii=False) + "\n")
                if count % 500 == 0:
                    jsonl_file.flush()

            total_chars += len(text)
            count += 1

            if sample and count >= sample_count:
                break
    finally:
        if jsonl_file:
            jsonl_file.close()

    print(f"\nBootstrap complete: {count} documents, avg {total_chars // max(count, 1)} chars/doc",
          file=sys.stderr)
    return count


def main():
    parser = argparse.ArgumentParser(description="CH/Fedlex Swiss Legislation Fetcher")
    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    bootstrap_parser = subparsers.add_parser("bootstrap", help="Fetch documents")
    bootstrap_parser.add_argument("--sample", action="store_true", help="Fetch sample data")
    bootstrap_parser.add_argument("--full", action="store_true", help="Fetch the full corpus")
    bootstrap_parser.add_argument("--count", type=int, default=12, help="Number of samples")
    bootstrap_parser.add_argument("--langs", default=",".join(DEFAULT_LANGS),
                                  help="Comma-separated languages for consolidated law")

    update_parser = subparsers.add_parser("update", help="Fetch recent updates")
    update_parser.add_argument("--days", type=int, default=7, help="Days to look back")
    update_parser.add_argument("--since", help="ISO date to look back to")
    update_parser.add_argument("--langs", default=",".join(DEFAULT_LANGS))

    args = parser.parse_args()

    if args.command == "bootstrap":
        langs = [l.strip() for l in args.langs.split(",") if l.strip() in LANG_URI]
        run_bootstrap(sample=args.sample, sample_count=args.count, langs=langs or list(DEFAULT_LANGS))
    elif args.command == "update":
        langs = [l.strip() for l in args.langs.split(",") if l.strip() in LANG_URI]
        if args.since:
            since = datetime.fromisoformat(args.since[:10])
        else:
            since = datetime.utcnow() - timedelta(days=args.days)
        for doc in fetch_updates(since, langs or list(DEFAULT_LANGS)):
            print(json.dumps(normalize(doc), ensure_ascii=False))
    else:
        parser.print_help()


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
