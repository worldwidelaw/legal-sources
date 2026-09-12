# EU/EP-Resolutions — European Parliament non-legislative resolutions

Full text of the **European Parliament's non-legislative resolutions** —
own-initiative reports and topical/urgency resolutions on human rights, foreign
affairs, the environment, the economy, and implementation/monitoring, published
in the OJ C series.

- CELEX `5{YYYY}IP{NNNN}` (sector 5 = preparatory acts; `IP` = the EP-resolution class)
- ~12,200 documents, 1990s–present

These are distinct from the Parliament's **legislative** positions at first and
second reading (descriptor `AP`, source **EU/EP-Positions**). Together with
**EU/COM-Documents** (Commission proposals) and **EU/Council-Positions** they
round out the Parliament's output in the Publications Office repository.

## How it works

1. **Enumeration** — the public CELLAR SPARQL endpoint
   (`http://publications.europa.eu/webapi/rdf/sparql`) lists every EP resolution,
   filtered by a *year-scoped* CELEX regex `^5{YYYY}IP`. Year-scoping keeps each
   query small so it never hits the ~10K SPARQL `OFFSET` ceiling.
2. **Full text** via CELLAR HTTP content negotiation
   (`http://publications.europa.eu/resource/celex/{CELEX}`):
   - Most resolutions are served as OJ/Formex **xHTML**
     (`Accept: application/xhtml+xml`) → tags stripped.
   - Older resolutions come back as a direct born-digital **PDF** (or a
     `300 Multiple-Choice` listing of PDF streams, from which we pick the English
     "part1" PDF), extracted with PyMuPDF (no OCR).

CELLAR content negotiation bypasses the `eur-lex.europa.eu` AWS-WAF that
202-challenges datacenter IPs, so this source is **fleet-safe**.

### Overlap with EU/EuroParl

**EU/EuroParl** draws EP adopted texts from the Parliament Open Data
adopted-texts API, which only reaches back to **2014**. The `IP` descriptor is
the canonical CELEX-registered OJ version reaching back to the **1990s**, so the
several thousand pre-2014 resolutions here are **additive**. The loader dedups on
`_id` (CELEX).

## Data type

`doctrine` — official EP non-legislative/consultative acts (consistent with the
COM-Documents, SWD-Documents, Council-Positions and EP-Positions sources).

## Usage

```bash
python3 bootstrap.py test               # SPARQL + fetch smoke test
python3 bootstrap.py bootstrap --sample # write 15 sample records
python3 bootstrap.py bootstrap          # full corpus → data/records.jsonl
python3 bootstrap.py update             # rescan recent years for new docs
```

## License

[EU institutional reuse — Commission Decision 2011/833/EU](https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX:32011D0833) — reuse of EP/Commission documents authorised, attribution requested. Commercial use permitted.
