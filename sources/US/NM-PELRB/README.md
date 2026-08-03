# US/NM-PELRB — New Mexico Public Employee Labor Relations Board (Decisions)

Full text of the decisions of the **New Mexico Public Employee Labor Relations
Board (PELRB)**, the state agency that adjudicates public-sector labor disputes
under the **Public Employee Bargaining Act (PEBA, NMSA 1978 §§ 10-7E-1 et
seq.)**: prohibited-practice complaints, representation / unit-determination
petitions, and related matters. The corpus also includes the reviewing New
Mexico court decisions, select hearing-examiner decisions, and select
arbitration awards the Board publishes.

Each Board order resolves a specific contested case, so every record is
`case_law`.

## Source

Decisions are published as PDFs indexed on server-rendered HTML pages under
`https://www.pelrb.nm.gov/decisions-and-research-aids/`:

- `board-orders/peba-ii/` — Board orders, current PEBA era
- `board-orders/peba-i/` — Board orders, 1993–2003 PEBA era
- `select-hearing-examiner-decisions/`
- `court-decisions/`
- `select-arbitration-awards/`

Each decision is a table/list row whose `<a>` link text is the Board citation
(`47-PELRB-2024`) and whose surrounding cell carries the full caption
(`47-PELRB-2024, PELRB NO. 304-22 In re: {parties}`). The linked PDF holds the
full decision text.

## Method

1. `GET` each of the five index pages.
2. Collect the citation-shaped decision anchors and their captions, skipping
   purely administrative documents (open-meetings resolutions, annual reports,
   audits, practice manuals, bargaining-unit lists).
3. Download each PDF and extract its text via the shared `common.pdf_extract`
   (opendataloader → pdfplumber → OCR fallback).
4. Normalize into the `case_law` schema (~578 unique decisions).

No JavaScript, no CAPTCHA, no authentication.

## Usage

```bash
python bootstrap.py test-api            # connectivity + extraction check
python bootstrap.py bootstrap --sample  # ~12 sample records
python bootstrap.py bootstrap           # full pull
python bootstrap.py bootstrap-fast      # alias for full pull (VPS wrapper)
```

## License

[Public Domain — U.S. Government Work (17 U.S.C. § 105 analogue)](https://www.law.cornell.edu/uscode/text/17/105) — decisions of the New Mexico PELRB and the reviewing New Mexico courts are official state-government works in the public domain under the government-edicts doctrine. Commercial use permitted; no attribution required.
