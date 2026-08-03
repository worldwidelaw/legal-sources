# US/FERC-eLibrary — FERC eLibrary Commission Orders & Decisions

Full-text adjudicatory issuances from the U.S. **Federal Energy Regulatory
Commission (FERC)** eLibrary, the Commission's docket/issuance repository.

This source targets the **decisional** issuances that resolve specific
proceedings — electric/gas rate filings, hydropower and natural-gas
certificates, complaints, enforcement actions, and rulemaking dockets:

- Commission Orders / Opinions
- Delegated (letter) Orders
- Procedural Orders
- ALJ Initial Decisions
- Compliance Directives, Protective Orders, rehearing orders

Each issuance resolves a specific proceeding → **`case_law`**.

> **Distinct from `US/FERC`.** The existing `US/FERC` source only ingests
> FERC rulemakings/notices published in the *Federal Register* (legislation-
> adjacent). This source targets the full eLibrary issuance corpus.

## Access

A public JSON API sits behind the eLibrary Angular SPA (no auth, no CAPTCHA):

| Step | Endpoint |
|------|----------|
| Search | `POST /eLibrarywebapi/api/Search/AdvancedSearch` (JSON body: `dateSearches`, `categories`, `resultsPerPage`, `curPage`) → `searchHits[]` with accession, docket numbers, `classTypes`, `transmittals[]` |
| Download | `POST /eLibrarywebapi/api/File/DownloadP8File` (JSON body: `{FileType, accession, fileid:0, FileIDAll:<fileId>, fileidLst:[<fileId>], Islegacy:false}`) → raw file bytes |
| Doc page | `https://elibrary.ferc.gov/eLibrary/docinfo?accession_Number={acc}` |

Discovery pages the search month-by-month (newest first) over category
`Issuance`, filtering to decisional document types. Full text comes from each
accession's born-digital transmittal files: **text-layer PDF** (via
`common.pdf_extract`) and **DOCX/TXT** (via a dependency-free pure-python
extractor — modern FERC "Notational Orders" are frequently served as DOCX).
A `<200`-char guard skips scanned/empty issuances; `FIRST_YEAR=2000` excludes
the pre-2000 scanned-image era.

## Usage

```bash
python bootstrap.py test-api            # connectivity + extraction test
python bootstrap.py bootstrap --sample  # ~12 recent samples
python bootstrap.py bootstrap           # full pull (present -> 2000)
```

The full corpus is large (category `Issuance` runs ~20–37K docs/year; the
decisional subset is a large multi-year corpus), so full pulls are long-run /
VPS territory.

## License

[Public Domain — U.S. Government Work](https://www.law.cornell.edu/uscode/text/17/105) — orders and decisions of the Federal Energy Regulatory Commission are official U.S. Government works in the public domain under 17 U.S.C. § 105 and the government-edicts doctrine. Commercial use permitted; no attribution required.
