# UK/NMC — Nursing and Midwifery Council — Fitness to Practise Outcomes

Full-text fitness-to-practise **determinations** ("reasons") of the **Nursing
and Midwifery Council (NMC)**, the UK statutory regulator for ~800,000 nurses,
midwives and nursing associates.

The NMC's **Fitness to Practise Committee** and **Investigating Committee** sit
under the Nursing and Midwifery Order 2001 and the NMC (Fitness to Practise)
Rules 2004. Each concluded hearing or meeting publishes a reasoned decision
setting out the charges/allegation, the facts found proved, whether the
registrant's fitness to practise is impaired, and the sanction or order imposed
(striking-off, suspension, conditions of practice, caution) or the interim
order made. These are binding professional-regulator adjudications = **case
law**, distinct from:

- **UK/GMC** — doctors (General Medical Council / MPTS)
- **UK/SDT** — solicitors (Solicitors Disciplinary Tribunal)
- **UK/BTAS** — barristers (Bar Tribunals & Adjudication Service)
- **UK/HCPTS** — 15 health & care professions (HCPC)
- **UK/SocialWorkEngland** — social workers

## Data source & method

- Each determination is a born-digital PDF (real text layer, no OCR) at
  `https://www.nmc.org.uk/globalassets/sitedocuments/ftpoutcomes/{year}/{month-year}/reasons-{name}-{type}-{PIN}-{YYYYMMDD}.pdf`.
- The PDFs are indexed from monthly listing pages
  `/concerns-nurses-midwives/hearings/hearings-sanctions/hearings-{month}-{year}/`.
- The scraper probes the last ~24 calendar-month listing pages, keeps the
  200-OK ones, collects every `reasons-*.pdf` href, downloads each PDF and
  extracts the text with PyMuPDF (shared pdfplumber/pypdf fallback), parsing the
  structured header (committee / hearing type / registrant / NMC PIN / register
  part / location / sanction).

### Rolling window

The NMC keeps only a **rolling window of the most recent months** online (older
monthly listing pages are removed under its publication policy, "because
decisions can be changed"). One run therefore captures ~250 determinations
across ~4 live months; re-running over time accumulates the full record — the
pipeline dedups on `_id` (the stable PDF stem). `nmc.org.uk` is excluded from
the Wayback Machine, so historical enumeration relies on the live window.

## Record shape

`_id`, `_source` (`UK/NMC`), `_type` (`case_law`), `title`, `text` (full
determination), `date`, `url`, `registrant`, `registration_number` (NMC PIN),
`register_part`, `location`, `hearing_type`, `fitness_to_practise`, `sanction`,
`interim_order`, `court`, `jurisdiction` (`GB`), `language` (`en`).

## Usage

```bash
python bootstrap.py bootstrap          # Full pull (current live window)
python bootstrap.py bootstrap --sample # Sample records for validation
python bootstrap.py bootstrap-fast     # Full pull (runner alias)
python bootstrap.py update             # Incremental (recent months)
python bootstrap.py test               # Quick connectivity test
```

## License

> ⚠️ **Commercial use restricted.** No explicit open licence is stated; NMC/Crown
> copyright applies to the underlying records.

[NMC website terms and conditions](https://www.nmc.org.uk/terms-and-conditions/) — NMC
fitness-to-practise determinations are public professional-regulator adjudication
records published under the NMC's publication policy. Commercial re-use is
conservatively flagged per project policy, consistent with the sibling UK
professional-regulator tribunal sources (UK/GMC, UK/SDT, UK/BTAS, UK/HCPTS).
