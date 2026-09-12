# DE/BfS — Bundesamt für Strahlenschutz (DORIS)

Full text of **DORIS** (*Digitales Online Repositorium und Informations-System*),
the official publication repository of the German Federal Office for Radiation
Protection (BfS). `www.bfs.de` points here for its publications: DORIS is where
the office deposits the technical and regulatory material underpinning German
radiation protection law (StrlSchG / StrlSchV).

- **Agency:** https://www.bfs.de/
- **Repository:** https://doris.bfs.de/
- **Corpus:** 764 items, 1950 → 2026
- **Language:** mostly German, some English
- **Type:** `doctrine`

## What is in it

- **BfS-Schriften** and **Ressortforschungsberichte** commissioned by the BMUV.
  BfS states these results "dienen als Entscheidungshilfen bei der Erarbeitung
  von Strahlenschutzvorschriften" — they are the evidentiary basis on which
  radiation protection rules are drafted.
- **Strahlenschutzforschung Programmreporte** (annual research programme reports).
- Annual reports of the incorporation measuring stations (Inkorporations­mess­stellen).
- Technical reports and position papers on electromagnetic fields, UV exposure,
  medical and occupational exposure, and nuclear emergency preparedness.

## Access approach

DORIS runs **DSpace (JSPUI)**. OAI-PMH (`/oai/request`, `/jspui/oai/request`) and
the DSpace REST API are both disabled, so discovery uses the browse index:

1. `GET /jspui/browse?type=dateissued&sort_by=2&order=DESC&rpp=100&etal=-1&offset=N`
   — 764 rows across 8 pages, each giving the item handle (a URN, e.g.
   `urn:nbn:de:0221-2026042960058`), title and issue date.
2. The item page carries clean **Dublin Core `<meta>` tags** (`DC.title`,
   `DC.creator`, `DCTERMS.issued`, `DCTERMS.abstract`, `DC.publisher`,
   `DC.relation`, `DC.language`), which are used in preference to scraped
   markup, plus the PDF bitstream link.
3. The PDF is extracted with `common/pdf_extract`. The documents are
   born-digital and extract cleanly (samples: 15K–779K characters).

Discovery failures raise loudly rather than returning a truncated corpus, so a
datacenter-IP block can never be mistaken for a small repository.

## Usage

```bash
python bootstrap.py bootstrap --sample     # 15 samples into sample/
python bootstrap.py bootstrap --full       # full corpus → data/records.jsonl
python bootstrap.py bootstrap-fast         # fleet alias for --full
python bootstrap.py updates --since 2026-01-01
python bootstrap.py validate
```

Completed handles are checkpointed to `data/checkpoint.json`, so a re-launched
run skips finished items without any network calls.

## Record schema

| Field | Description |
|---|---|
| `_id` | `DE-BfS-{slugified URN handle}` |
| `_source` | `DE/BfS` |
| `_type` | `doctrine` |
| `title` | `DC.title` |
| `text` | Full text extracted from the PDF bitstream |
| `date` | `DCTERMS.issued` (ISO 8601) |
| `url` | DORIS item page |
| `pdf_url` | PDF bitstream |
| `urn` | Persistent URN identifier |
| `alt_title` | `DCTERMS.alternative` |
| `authors` | `DC.creator` list |
| `publisher` | `DC.publisher` |
| `series` | `DC.relation` (e.g. `BfS-Schriften ; 71/26`) |
| `abstract` | `DCTERMS.abstract` |

## License

> ⚠️ **Commercial use restricted.** DORIS content is served under a
> non-commercial licence. Commercial redistribution requires separate
> permission from BfS.

[BfS DORIS Nutzungslizenz](https://doris.bfs.de/jspui/impressum/lizenz.html) —
an adaptation of
[CC BY-NC-SA 3.0 DE](https://creativecommons.org/licenses/by-nc-sa/3.0/de/legalcode)
"den speziellen Bedürfnissen der nichtkommerziellen Nutzung durch das Bundesamt
für Strahlenschutz angepasst". Clause 4.c limits the grant to uses "die nicht auf
einen geschäftlichen Vorteil oder eine geldwerte Vergütung gerichtet sind", and
BfS expressly reserves the right to collect remuneration for any use beyond that.
Attribution and share-alike apply.

## Rate limiting

1.5 s between requests, with 429/5xx backoff honouring `Retry-After`.
`doris.bfs.de` serves no `robots.txt` at all (404), so no crawl directives apply;
the 30 s `Crawl-delay` on `www.bfs.de` covers a different host.
