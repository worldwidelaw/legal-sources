# IE/OIC — Office of the Information Commissioner (Ireland) — Review Decisions

The **Office of the Information Commissioner (OIC)** is the independent statutory
office that reviews decisions made by public bodies under Ireland's **Freedom of
Information Act 2014** and (for environmental information) the **Access to
Information on the Environment (AIE) Regulations**. Each published review
decision is a formal, binding adjudication determining whether a public body was
justified in its handling of an access request — quasi-judicial **case_law**.

- **Publisher:** Office of the Information Commissioner (OIC), Ireland
- **Site:** https://oic.ie/en/decisions/
- **Data type:** `case_law`
- **Coverage:** ~3,460 review decisions (listing runs ~346 pages × 10)
- **Auth:** none
- **Full text:** yes — born-digital HTML, no OCR/PDF extraction required

## How it works

1. **Enumerate** every decision by paging the server-rendered listing
   `https://oic.ie/en/decisions/?page=N`. Each page exposes 10
   `/en/ombudsman-decision/{slug}/` links.
2. **Fetch** each decision's canonical page and extract the full text from the
   gov.ie-style main content region (`<div id="main" role="main">` up to the
   site footer / "Help us improve" feedback form).
3. **Parse** the case number (`OIC-NNNNNN-XXXXXX`), publication date
   (`<time datetime="…">`) and title (which encodes the applicant and the
   respondent FOI body).

Each normalized record contains the full decision text (Background, Scope of
Review, Analysis and Findings, Decision, Right of Appeal) plus the case
reference, respondent public body, and decision date.

## Usage

```bash
python bootstrap.py test               # connectivity + one-record preview
python bootstrap.py bootstrap --sample # ~15 sample decisions
python bootstrap.py bootstrap          # full pull
python bootstrap.py update             # incremental (recent decisions)
```

## License

[PSI Licence (Ireland)](https://data.gov.ie/pages/opendatalicence) — © Office of
the Information Commissioner. Re-use is permitted **free of charge in any format,
including commercial use**, subject to accurate reproduction and acknowledgement
of the source. See the OIC's own
[Re-use of OIC information](https://oic.ie/en/collection/ecb5e-re-use-of-oic-information)
statement.
