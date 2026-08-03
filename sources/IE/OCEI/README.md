# IE/OCEI — Commissioner for Environmental Information (Ireland) — Appeal Decisions

The **Commissioner for Environmental Information (OCEI)** is the independent
statutory office that reviews decisions made by public authorities under
Ireland's **European Communities (Access to Information on the Environment)
Regulations 2007–2018** (the "AIE Regulations"). Each published decision is a
formal, binding adjudication determining whether a public authority was
justified in its handling of a request for environmental information —
quasi-judicial **case_law**.

- **Publisher:** Office of the Commissioner for Environmental Information (OCEI), Ireland
- **Site:** https://ocei.ie/en/decisions/
- **Data type:** `case_law`
- **Coverage:** ~770 appeal decisions (listing runs ~77 pages × 10)
- **Auth:** none
- **Full text:** yes — born-digital HTML, no OCR/PDF extraction required

## How it works

1. **Enumerate** every decision by paging the server-rendered listing
   `https://ocei.ie/en/decisions/?page=N`. Each page exposes 10
   `/en/ombudsman-decision/{slug}/` links.
2. **Fetch** each decision's canonical page and extract the full text from the
   gov.ie-style main content region (`<div id="main" role="main">` up to the
   site footer / "Help us improve" feedback form).
3. **Parse** the case number (`OCE-NNNNNN-XXXXXX`), publication date
   (`<time datetime="…">`) and title (applicant & respondent authority).

The OCEI shares the gov.ie "reboot" publishing platform (and the
`ombudsman-decision` URL slug) with the Office of the Information Commissioner
(`IE/OIC`), so the two scrapers are near-identical. This source covers the
distinct **AIE / environmental information** appeal series (case numbers prefixed
`OCE-`), separate from OIC's Freedom of Information review decisions.

## Usage

```bash
python bootstrap.py test               # connectivity + one-record preview
python bootstrap.py bootstrap --sample # ~15 sample decisions
python bootstrap.py bootstrap          # full pull
python bootstrap.py update             # incremental (recent decisions)
```

## License

[PSI Licence (Ireland)](https://data.gov.ie/pages/opendatalicence) — ©
Commissioner for Environmental Information. Re-use is permitted **free of charge
in any format, including commercial use**, subject to accurate reproduction and
acknowledgement of the source.
