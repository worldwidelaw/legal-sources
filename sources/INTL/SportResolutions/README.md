# INTL/SportResolutions — Sport Resolutions (UK) Published Decisions

[Sport Resolutions](https://www.sportresolutions.com/) is the UK's leading
independent, not-for-profit dispute-resolution service for sport. It provides the
secretariat for the **National Anti-Doping Panel (NADP)** — the UK's independent
anti-doping tribunal — and administers hearings and appeals for a wide range of
sports governing bodies.

This source captures the full text of the publicly available decisions in the
[Published & Time-Limited Decisions](https://www.sportresolutions.com/decisions)
database. Cases span many sports and tribunals, including:

- **Anti-doping** — UK Anti-Doping (UKAD) v athletes before the NADP
- **Athletics** — World Athletics Disciplinary & Appeals Tribunal
- **Tennis** — International Tennis Integrity Agency (ITIA) / ITF
- **Football** — FA / EFL / Club Financial Reporting Unit (CFRU) regulatory cases
- **Other** — rugby (union & league), snooker (WPBSA), sailing, golf (PGA European
  Tour), cricket, skiing & ski-jumping (FIS), boxing, cycling, ice hockey

Each case is a reasoned tribunal/arbitration decision (anti-doping rule
violations, periods of ineligibility, financial-fair-play breaches, etc.).

## Access method

The listing page is server-rendered HTML, paginated by an offset path segment:
`/decisions`, `/decisions/P6`, `/decisions/P12`, … (6 items per page).

`bootstrap.py`:
1. walks the paginated listing, parsing each `div.decision-panel` for the case
   title, detail URL, date, sport and decision type;
2. fetches each detail page and extracts the full-text decision PDF link(s) under
   `/assets/documents/*.pdf`;
3. downloads each PDF (1–1.5 s rate limit) and extracts full text via
   `common/pdf_extract` (pdfplumber/pypdf fallback).

No authentication, no WAF, reachable from any IP. ~60 decisions are listed at any
time. Note: Sport Resolutions **removes** anti-doping decisions once the athlete's
ban has been served, so the live corpus is a rolling set rather than a permanent
archive — periodic re-runs capture newly published decisions.

## Data type

`case_law` — sports-arbitration and anti-doping tribunal decisions.

## Usage

```bash
python bootstrap.py test               # Print parsed listing entries
python bootstrap.py bootstrap --sample # Fetch 10 sample records
python bootstrap.py bootstrap          # Full pull
```

## License

> ⚠️ **Commercial use restricted.** Decisions are published openly for
> transparency but Sport Resolutions / the relevant governing body asserts
> copyright with no open licence.

[Sport Resolutions Terms (All Rights Reserved)](https://www.sportresolutions.com/) —
decisions are publicly available for transparency; no open licence is granted.
Commercial use flagged restricted per project policy (err on flagging).
