# INTL/AthleticsIntegrityUnit — Athletics Integrity Unit (AIU) First Instance Decisions

The [Athletics Integrity Unit](https://www.athleticsintegrity.org/) (AIU) is the
independent body established by World Athletics to investigate and prosecute
doping and non-doping (integrity / competition-manipulation) violations in the
sport of athletics. Its independent **Disciplinary Tribunal** (secretariat managed
by Sport Resolutions, London) hears and determines all first-instance cases under
the World Athletics Anti-Doping Rules and the World Athletics Integrity Code of
Conduct.

This source captures the full text of the AIU's published **First Instance
Decisions** — the reasoned tribunal decisions on individual athletes and support
personnel (sanctions, periods of ineligibility, disqualifications, etc.).

## Access method

The [First Instance Decisions page](https://www.athleticsintegrity.org/disciplinary-process/first-instance-decisions)
is a single server-rendered HTML table (Date, Respondent, NAT, Violation, Outcome,
Status). The *Outcome* cell of each row links to the born-digital, full-text
decision PDF hosted openly at
`https://www.athleticsintegrity.org/downloads/pdfs/disciplinary-process/en/...`.

`bootstrap.py`:
1. parses every decision PDF link from the table and recovers its row metadata,
2. downloads each PDF (1.5 s rate limit),
3. extracts full text via `common/pdf_extract` (pdfplumber/pypdf fallback).

No authentication, no WAF, reachable from any IP. ~60 decisions currently listed;
the page is updated as new tribunal decisions are rendered.

## Data type

`case_law` — first-instance disciplinary tribunal decisions (sports integrity /
anti-doping adjudication).

## Usage

```bash
python bootstrap.py test               # Print parsed listing entries
python bootstrap.py bootstrap --sample # Fetch 10 sample records
python bootstrap.py bootstrap          # Full pull
```

## License

> ⚠️ **Commercial use restricted.** Decisions are openly published for
> transparency but World Athletics / AIU asserts copyright with no open licence.

[World Athletics / AIU Terms (All Rights Reserved)](https://www.athleticsintegrity.org/) —
decisions are publicly available for transparency; no open licence is granted.
Commercial use flagged restricted per project policy (err on flagging).
