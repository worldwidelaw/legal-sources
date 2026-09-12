# EU/EP-Positions — European Parliament positions / legislative resolutions

Full text of the **European Parliament's positions / legislative resolutions**
(its *positions at first and second reading*) in the ordinary — and historic
cooperation / codecision — legislative procedure, as published in the OJ C
series.

- CELEX `5{YYYY}AP{NNNN}` (sector 5 = preparatory acts; `AP` = the EP-position class)
- ~11,800 documents, 1990s–present (carry `P{n}_TA(YYYY)NNNN` "texts adopted" IDs)

These are Parliament's formal legislative-procedure acts on proposed
regulations, directives and decisions — the Parliament-side counterpart to the
Commission proposals in **EU/COM-Documents** and the Council positions in
**EU/Council-Positions**, completing the COM → EP → Council legislative-history
triad.

**Distinct from EU/EuroParl.** EU/EuroParl draws EP *adopted texts* from the EP
Open Data API and only covers 2014+. `EU/EP-Positions` provides the canonical,
CELEX-registered OJ-published legislative resolutions and reaches back to the
1990s — thousands of pre-2014 documents EuroParl lacks.

## How it works

1. **Enumeration** — the public CELLAR SPARQL endpoint
   (`http://publications.europa.eu/webapi/rdf/sparql`) lists every EP position,
   filtered by a *year-scoped* CELEX regex `^5{YYYY}AP`. Year-scoping keeps each
   query small so it never hits the ~10K SPARQL `OFFSET` ceiling.
2. **Full text** via CELLAR HTTP content negotiation
   (`http://publications.europa.eu/resource/celex/{CELEX}`):
   - Positions are served as OJ/Formex **xHTML**
     (`Accept: application/xhtml+xml`) → tags stripped.
   - Where no xHTML manifestation exists, CELLAR answers with a direct
     born-digital **PDF** or a `300 Multiple-Choice` listing of PDF streams. We
     pick the English "part1" PDF (fallback: first English PDF, then `DOC_1`) and
     extract with PyMuPDF (no OCR — these are born-digital).
   - The CELEX is **URL-encoded** because some position CELEX numbers carry a
     parenthesised sub-part suffix (e.g. `52017AP0001(01)`) that the
     `/resource/celex/` path only resolves when the parentheses are escaped.

CELLAR content negotiation bypasses the `eur-lex.europa.eu` AWS-WAF that
202-challenges datacenter IPs, so this source is **fleet-safe**.

This is the same recipe as the sibling sources **EU/COM-Documents**,
**EU/SWD-Documents** and **EU/Council-Positions**, narrowed to the `AP`
descriptor. Corrigenda / empty-body sub-parts fall below the 200-char text floor
and are skipped.

## Data type

`doctrine` — official European Parliament preparatory/legislative-procedure
documents (consistent with the COM-Documents, SWD-Documents and Council-Positions
sources).

## Usage

```bash
python3 bootstrap.py test               # SPARQL + fetch smoke test
python3 bootstrap.py bootstrap --sample # write 15 sample records
python3 bootstrap.py bootstrap          # full corpus → data/records.jsonl
python3 bootstrap.py update             # rescan recent years for new docs
```

## License

[EU institutional reuse — Commission Decision 2011/833/EU](https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX:32011D0833) — reuse of European Parliament / Commission / Council documents authorised, attribution requested. Commercial use permitted.
