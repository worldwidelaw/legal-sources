# EU/SWD-Documents — European Commission Staff Working Documents

Full text of the European Commission's **Staff Working Documents** (and the
older **SEC** series): impact assessments, evaluations, fitness checks,
executive summaries and accompanying analytical documents.

- CELEX `5{YYYY}SC{NNNN}` (sector 5 = preparatory acts; `SC` = the SWD/SEC class)
- ~8,600 documents, 1975–present

These are the analytical backbone behind EU legislative proposals — the impact
assessments and evaluations that the Commission's proposals (see
**EU/COM-Documents**) accompany.

## How it works

1. **Enumeration** — the public CELLAR SPARQL endpoint
   (`http://publications.europa.eu/webapi/rdf/sparql`) lists every SWD/SEC
   document, filtered by a *year-scoped* CELEX regex `^5{YYYY}SC`. Year-scoping
   keeps each query small so it never hits the ~10K SPARQL `OFFSET` ceiling.
2. **Full text** via CELLAR HTTP content negotiation
   (`http://publications.europa.eu/resource/celex/{CELEX}`):
   - Recent SWDs are served as OJ/Formex **xHTML**
     (`Accept: application/xhtml+xml`) → tags stripped.
   - Most SWDs have no xHTML manifestation; CELLAR answers with a direct
     born-digital **PDF** or a `300 Multiple-Choice` listing of PDF streams. We
     pick the English "part1" PDF (fallback: first English PDF, then `DOC_1`) and
     extract with PyMuPDF (no OCR — these are born-digital).

CELLAR content negotiation bypasses the `eur-lex.europa.eu` AWS-WAF that
202-challenges datacenter IPs, so this source is **fleet-safe**.

This is the same recipe as the sibling sources **EU/COM-Documents**,
**EU/EESC-Opinions** and **EU/CoR-Opinions**, narrowed to the `SC` descriptor.

## Data type

`doctrine` — official Commission analytical/preparatory documents (consistent
with the COM-Documents and EESC/CoR opinion sources).

## Usage

```bash
python3 bootstrap.py test               # SPARQL + fetch smoke test
python3 bootstrap.py bootstrap --sample # write 15 sample records
python3 bootstrap.py bootstrap          # full corpus → data/records.jsonl
python3 bootstrap.py update             # rescan recent years for new docs
```

## License

[EU institutional reuse — Commission Decision 2011/833/EU](https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX:32011D0833) — reuse of Commission documents authorised, attribution requested. Commercial use permitted.
