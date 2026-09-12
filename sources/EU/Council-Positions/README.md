# EU/Council-Positions — Council of the EU positions at first reading

Full text of the **Council of the European Union's positions at first reading**
(historically *common positions*) in the ordinary legislative procedure, together
with the accompanying *statements of the Council's reasons*.

- CELEX `5{YYYY}AG{NNNN}` (sector 5 = preparatory acts; `AG` = the Council-position class)
- ~1,300 documents, 1987–present

These are the Council's formal legislative-procedure acts on proposed
regulations, directives and decisions — the Council-side counterpart to the
Commission proposals in **EU/COM-Documents** and the Parliament's adopted texts
in **EU/EuroParl**.

## How it works

1. **Enumeration** — the public CELLAR SPARQL endpoint
   (`http://publications.europa.eu/webapi/rdf/sparql`) lists every Council
   position, filtered by a *year-scoped* CELEX regex `^5{YYYY}AG`. Year-scoping
   keeps each query small so it never hits the ~10K SPARQL `OFFSET` ceiling.
2. **Full text** via CELLAR HTTP content negotiation
   (`http://publications.europa.eu/resource/celex/{CELEX}`):
   - Positions are served as OJ/Formex **xHTML**
     (`Accept: application/xhtml+xml`) → tags stripped.
   - Where no xHTML manifestation exists, CELLAR answers with a direct
     born-digital **PDF** or a `300 Multiple-Choice` listing of PDF streams. We
     pick the English "part1" PDF (fallback: first English PDF, then `DOC_1`) and
     extract with PyMuPDF (no OCR — these are born-digital).
   - The CELEX is **URL-encoded** because Council-position CELEX numbers carry a
     parenthesised sub-part suffix (e.g. `52023AG0002(01)`) that the
     `/resource/celex/` path only resolves when the parentheses are escaped.

CELLAR content negotiation bypasses the `eur-lex.europa.eu` AWS-WAF that
202-challenges datacenter IPs, so this source is **fleet-safe**.

This is the same recipe as the sibling sources **EU/COM-Documents** and
**EU/SWD-Documents**, narrowed to the `AG` descriptor. Corrigenda / empty-body
sub-parts (`R(01)`) fall below the 200-char text floor and are skipped.

## Data type

`doctrine` — official Council preparatory/legislative-procedure documents
(consistent with the COM-Documents, SWD-Documents and EESC/CoR opinion sources).

## Usage

```bash
python3 bootstrap.py test               # SPARQL + fetch smoke test
python3 bootstrap.py bootstrap --sample # write 15 sample records
python3 bootstrap.py bootstrap          # full corpus → data/records.jsonl
python3 bootstrap.py update             # rescan recent years for new docs
```

## License

[EU institutional reuse — Commission Decision 2011/833/EU](https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX:32011D0833) — reuse of Council/Commission documents authorised, attribution requested. Commercial use permitted.
