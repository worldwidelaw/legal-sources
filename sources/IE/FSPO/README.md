# IE/FSPO — Financial Services and Pensions Ombudsman of Ireland

Legally binding decisions of the **Financial Services and Pensions Ombudsman
(FSPO)**, the Irish statutory office (Financial Services and Pensions Ombudsman
Act 2017) that investigates and adjudicates complaints against financial service
and pension providers.

FSPO decisions are **legally binding** on both parties, subject only to a
statutory appeal to the High Court — i.e. adjudicative case law. Each decision
is published in full (anonymised) as a born-digital PDF and records the sector,
product/service, conduct(s) complained of, and outcome (Upheld / Partially
upheld / Substantially upheld / Rejected).

## Coverage

- ~2,000+ decisions, 2018–present (~230–280 per year).
- Language: English.
- Distinct from `IE/WRC` (employment/equality tribunal), `IE/BAILII`, and the
  Irish courts sources.

## Access

- **Listing (per year, 25/page):**
  `.../legally-binding-decisions/display.asp?dyear=YYYY&product=0&conduct=0&sector=0&outcome=0&decisionref=&mypage=N`
- **Decision PDF:** `.../documents/{YYYY}-{NNNN}.pdf` (born-digital, has a text
  layer — no OCR needed).
- Full text extracted with PyMuPDF (fitz). The structured header
  (Decision Ref / Sector / Product / Conduct / Outcome) is parsed from the text.
- No auth, no CAPTCHA.

Note: the anonymised published decisions do not carry an explicit decision date,
so `date` is set to the reference year (`YYYY-01-01`).

## Usage

```bash
python bootstrap.py test               # connectivity check
python bootstrap.py bootstrap --sample # 15 sample records
python bootstrap.py bootstrap          # full pull
python bootstrap.py update             # incremental (recent years)
```

## License

[PSI Licence / CC BY 4.0](https://www.gov.ie/en/help/re-use-of-public-sector-information/)
— Irish public sector information re-use framework (default CC BY 4.0).
Attribution required; commercial use permitted.
