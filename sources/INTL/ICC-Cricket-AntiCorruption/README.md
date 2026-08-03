# INTL/ICC-Cricket-AntiCorruption — ICC Cricket Anti-Corruption Tribunal Decisions

Full-text decisions issued under the **International Cricket Council's
Anti-Corruption Code for Participants**. When the ICC Anti-Corruption Tribunal, a
sole adjudicator, or an appeal panel determines a charge brought by the ICC
Integrity Unit against a player, official or other participant — match-fixing,
spot-fixing, accepting/offering bribes, failure to report a corrupt approach,
obstructing an investigation, etc. — the ICC publishes the full redacted decision
as a PDF: findings on liability, the sanction, the period of ineligibility, and
the full legal reasoning.

> **Note:** "ICC" here is the **International Cricket Council** (cricket world
> governing body), **not** the International Criminal Court (`INTL/ICCCaseLaw`,
> `INTL/ICC-TrialChamber`) nor ICC arbitration (`INTL/JusMundi-ICC`).

## Source

- **Publisher:** International Cricket Council (ICC) — Integrity Unit / Anti-Corruption Unit
- **Listing:** https://www.icc-cricket.com/about/integrity/anti-corruption/acu-publications
- **Type:** `case_law`
- **Coverage:** ~38 named-matter decisions (2016–present), reverse-chronological
- **Auth:** none

## How it works

The ACU publications page is a single server-rendered HTML page that links every
named-matter decision PDF. Each anchor carries the decision title and date
(e.g. *"Decision of the ICC in the matter of Mr Irfan Ahmed – 20 April 2016"*),
from which the scraper derives the title and ISO date. Each PDF is downloaded and
its full text extracted. Most PDFs are hosted on `images.icc-cricket.com` (extract
cleanly, ~5K–40K chars each); a small minority on the legacy
`resources.pulse.icc-cricket.com` host may be unavailable and are skipped.

```bash
python bootstrap.py test               # Print discovered decision entries
python bootstrap.py bootstrap --sample # Fetch sample records
python bootstrap.py bootstrap          # Full pull
```

## License

> ⚠️ **Commercial use restricted.** Decisions are published openly for
> transparency, but the ICC asserts copyright with no open licence.

[ICC Terms of Use](https://www.icc-cricket.com/terms-of-use) — All rights
reserved; attribution required, commercial use flagged restricted per project
policy (err on flagging).
