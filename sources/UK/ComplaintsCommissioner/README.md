# UK/ComplaintsCommissioner — Office of the Complaints Commissioner

Final Reports of the **Office of the Complaints Commissioner** (The Financial
Regulators Complaints Commissioner), the independent statutory reviewer of
complaints about the UK's financial regulators.

## What this covers

The Complaints Commissioner investigates complaints about how the following
bodies have carried out (or failed to carry out) their functions, under the
Complaints Scheme established by the **Financial Services Act 2012 (ss. 84–87)**:

- **FCA** — Financial Conduct Authority
- **PRA** — Prudential Regulation Authority
- **Bank of England**
- **PSR** — Payment Systems Regulator

Each complaint concludes in a reasoned **Final Report** setting out the
complaint, the regulator's account, the Commissioner's analysis, findings on
whether the complaint is upheld, and any recommendations (e.g. an apology or
ex-gratia payment). These are treated as `case_law` (adjudications on individual
complaints), distinct from the regulators' own enforcement notices (UK/FCA,
UK/PRA) and the sectoral ombudsmen (UK/FinancialOmbudsman, UK/PHSO).

## Access

- All reports are public, no authentication.
- Four static archive pages index every report as a born-digital PDF under
  `frccommissioner.org.uk/wp-content/uploads/`:
  - `/final-reports/fca-the-financial-conduct-authority/`
  - `/final-reports/pra-the-prudential-regulation-authority/`
  - `/final-reports/boe-the-bank-of-england/`
  - `/final-reports/psr-the-payment-systems-regulator/`
- The anchor text supplies the case reference and the Issued (decision) and
  Published dates; the PDF text layer is extracted with PyMuPDF (no OCR).

## Volume

~1,005 full-text final reports (FCA ~1000, PRA ~22, BoE ~3, PSR ~3).

## Usage

```bash
python bootstrap.py bootstrap          # Full pull
python bootstrap.py bootstrap --sample # 15 sample records for validation
python bootstrap.py update             # Incremental (recent issued first)
python bootstrap.py test               # Connectivity test
```

## License

> ⚠️ **Commercial use restricted.** No Open Government Licence applies.

[Custom terms — © The Financial Regulators Complaints Commissioner](https://frccommissioner.org.uk/) —
the site footer states "© The Financial Regulators Complaints Commissioner. All
rights reserved." Reports are published for transparency; commercial reuse is
restricted absent explicit permission.
