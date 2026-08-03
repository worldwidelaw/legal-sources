# EU/COM-Documents — European Commission Proposals & Communications

Full text of the European Commission's preparatory documents (**COM documents**):

- **Legislative proposals** — `COM(YYYY) NNNN final`, CELEX `5{YYYY}PC{NNNN}`
- **Communications, reports, green/white papers, recommendations** — CELEX `5{YYYY}DC{NNNN}`

~44,000 documents (≈30K proposals + ≈14K communications), 1959–present.

## How it works

1. **Enumeration** — the public CELLAR SPARQL endpoint
   (`http://publications.europa.eu/webapi/rdf/sparql`) lists every COM document,
   filtered by a *year-scoped* CELEX regex `^5{YYYY}(PC|DC)`. Year-scoping keeps
   each query small so it never hits the ~10K SPARQL `OFFSET` ceiling that a
   single 44K-row query would blow through.
2. **Full text** via CELLAR HTTP content negotiation
   (`http://publications.europa.eu/resource/celex/{CELEX}`):
   - **Communications (DC)** are served as OJ/Formex **xHTML**
     (`Accept: application/xhtml+xml`) → tags stripped.
   - **Proposals (PC)** have no xHTML manifestation; CELLAR answers
     `300 Multiple-Choice` listing the born-digital **PDF** streams. We pick the
     English "ACT part1" PDF (fallback: first English PDF, then `DOC_1`) and
     extract with PyMuPDF (no OCR — these are born-digital).

CELLAR content negotiation bypasses the `eur-lex.europa.eu` AWS-WAF that
202-challenges datacenter IPs, so this source is **fleet-safe**.

This is the same recipe as the sibling sources **EU/EESC-Opinions** and
**EU/CoR-Opinions** (sector-5 `AE`/`AR` CELEX), extended to `PC`/`DC` with a PDF
fallback for proposals.

## Data type

`doctrine` — official Commission preparatory and policy documents (consistent
with the EESC/CoR opinion sources).

## Usage

```bash
python3 bootstrap.py test              # SPARQL + fetch smoke test
python3 bootstrap.py bootstrap --sample  # write 15 sample records
python3 bootstrap.py bootstrap         # full corpus → data/records.jsonl
python3 bootstrap.py update            # rescan recent years for new docs
```

## License

[EU institutional reuse — Commission Decision 2011/833/EU](https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX:32011D0833) — reuse of Commission documents authorised, attribution requested. Commercial use permitted.
