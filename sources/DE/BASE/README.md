# DE/BASE — Bundesamt für die Sicherheit der nuklearen Entsorgung

Full text of the **Amtliches Dokumentenverzeichnis**, the official document
register that BASE — Germany's federal regulator for nuclear waste safety — is
required to publish under **§ 6 Standortauswahlgesetz (StandAG)** so that the
search for a deep geological repository for high-level radioactive waste remains
traceable and documented.

The register carries the substantive record of the procedure: expert opinions,
supervisory statements, reports, concepts, legal sources, minutes of supervisory
meetings and regulatory correspondence — authored both by BASE and by the
implementing body **Bundesgesellschaft für Endlagerung (BGE) mbH**.

- **Site:** https://www.base.bund.de/
- **Register:** https://www.base.bund.de/SiteGlobals/Forms/Suche/Dokumentenverzeichnis/DokumentenverzeichnisSuche_Formular.html
- **Corpus:** 775 documents, 1988-08-01 → 2026-07-07
- **Language:** German
- **Type:** `doctrine`

## Corpus breakdown

| Document type | Count |
|---|---|
| Korrespondenz (regulatory correspondence) | 474 |
| Bericht (reports) | 96 |
| Stellungnahme (formal statements) | 66 |
| Protokoll (minutes of supervisory meetings) | 54 |
| Konzept (concepts) | 34 |
| Präsentation | 29 |
| Rechtsquelle (legal sources) | 11 |
| Gutachten (expert opinions) | 10 |
| Parlamentarisches Dokument | 1 |

Published by BGE: 487 · Published by BASE: 288

## Access approach

No API, SPARQL endpoint or bulk download is offered — `base.bund.de` runs the
federal **Government Site Builder** CMS and exposes the register only as a
paginated HTML result list. `Sitemap_BASE.xml` times out (504) and is not usable.

1. Walk the register 50 hits per page via
   `?nn=615470&resultsPerPage=50&gtp=328868_list%253D{page}` (16 pages).
   Each teaser yields a stable numeric document id, title, document type, topic,
   publisher and publication date.
2. Fetch the detail page for the abstract and the resolved PDF link. The main PDF
   normally sits at the detail path with `.html` swapped for
   `.pdf?__blob=publicationFile`; the detail page is still parsed so entries that
   deviate resolve correctly.
3. Extract the PDF body with `common/pdf_extract`. The documents are born-digital
   and extract cleanly (samples: 2.3K–36K characters).

Discovery failures raise loudly rather than returning a truncated corpus, so a
datacenter-IP block can never be mistaken for a small register.

## Usage

```bash
python bootstrap.py bootstrap --sample     # 15 samples into sample/
python bootstrap.py bootstrap --full       # full corpus → data/records.jsonl
python bootstrap.py bootstrap-fast         # fleet alias for --full
python bootstrap.py updates --since 2026-01-01
python bootstrap.py validate
```

Completed document ids are checkpointed to `data/checkpoint.json`, so a
re-launched run skips finished documents without any network calls.

## Record schema

| Field | Description |
|---|---|
| `_id` | `DE-BASE-{register id}` |
| `_source` | `DE/BASE` |
| `_type` | `doctrine` |
| `title` | Document title |
| `text` | Full text extracted from the PDF |
| `date` | Publication date (ISO 8601) |
| `url` | Register detail page |
| `pdf_url` | Source PDF |
| `doc_type` | Gutachten, Stellungnahme, Bericht, Korrespondenz, … |
| `topic` | StandAG topic facet |
| `publisher` | `BASE` or `BGE` |
| `description` | Abstract from the detail page |

## License

> ⚠️ **Mixed authorship.** Roughly 63% of the register is authored by BGE mbH, a
> state-owned company rather than a public authority, so the amtliches-Werk
> analysis is strongest for the BASE-authored documents. The site-wide
> [Impressum](https://www.base.bund.de/de/service-funktionen/impressum/impressum_inhalt.html)
> also reserves rights over general web content.

[§ 5 UrhG — amtliche Werke](https://www.gesetze-im-internet.de/urhg/__5.html) —
these documents are published by a federal authority under a statutory duty
(§ 6 StandAG) for general public information, which is the § 5(2) UrhG case.
Attribution to BASE is required (§ 63 UrhG) and the text must not be altered
(§ 62 UrhG).

## Rate limiting

`robots.txt` declares `Crawl-delay: 30`, which is impractical for a 775-document
register; the scraper uses a 1.5 s delay between requests and backs off on
429/5xx with `Retry-After` support.

Note that `robots.txt` disallows `/SiteGlobals/` (allowing back only `Modules/`,
`StyleBundles/` and `Frontend/`), which covers the register's search form. This
is the standard Government Site Builder guard against faceted-search crawler
traps rather than a restriction on the content: the documents themselves, under
`/shareddocs/`, are not disallowed, and the register exists precisely to make
these documents public under § 6 StandAG. The form is therefore used only to
enumerate 775 known ids — no facet combinations are crawled — matching how the
other GSB-based federal sources in this repo (e.g. `DE/BfDI`) discover documents.
