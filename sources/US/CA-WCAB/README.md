# US/CA-WCAB — California Workers' Compensation Appeals Board

Full-text decisions of the California Workers' Compensation Appeals Board (WCAB),
the appellate body that reviews decisions of the state's workers' compensation
administrative law judges. Published by the Department of Industrial Relations at
<https://www.dir.ca.gov/wcab/>.

## Coverage

| Series | Listing | Decisions | Span |
|---|---|---|---|
| En banc | `wcab_enbanc.htm` | ~141 | 1997– |
| Significant panel | `wcab_panel.htm` | ~45 | 1997– |
| Panel decisions | `wcab-Decisions.htm` | ~5,600 | 2021– |

**~5,790 decisions total**, all with full text.

En banc decisions bind all workers' compensation judges and the Board itself
(Cal. Code Regs. tit. 8 § 10325). Significant panel decisions are not binding but
are designated by the Board as persuasive authority on a recurring issue. Ordinary
panel decisions are the Board's routine appellate output. All three adjudicate
specific contested claims, so every record is typed `case_law`.

## Access

Plain `GET`, no auth, JavaScript or CAPTCHA. Each listing is an HTML table whose
rows carry the authoritative metadata; the decision itself is a born-digital PDF
on the same host.

For the two precedential series the table also supplies the official WCAB citation
(e.g. `2026-EB-01`), the *Cal. Comp. Cases* reporter citation, the ADJ case numbers
and the Board's own summary of the holding — all captured rather than re-derived
from the PDF.

### Parsing notes

- `wcab-Decisions.htm` is malformed: several hundred rows omit their opening `<tr>`
  and a few omit the closing one, so rows are recovered by splitting on either tag.
  Matching `<tr>…</tr>` pairs silently drops ~150 decisions.
- A handful of anchors duplicate the link to the DIR staging host
  (`http://oak01web/…`), which does not resolve publicly; those are filtered.
- Filenames are not unique keys. The same decision is linked as `2000-eb2.pdf`,
  `/wcab/2000-eb2.pdf` and `/WCAB/2000-eb2.pdf` (IIS serves all three), while
  *different* decisions in one case differ only by a separator
  (`…ADJ11036278 ADJ15515237.pdf` vs `…ADJ11036278-ADJ15515237.pdf`). IDs therefore
  carry a short digest of the case-folded path: dedup the link variants, keep the
  distinct documents apart.

## Usage

```bash
python bootstrap.py test-api           # parse all three listings, no downloads
python bootstrap.py bootstrap --sample # 15 samples, 5 per series
python bootstrap.py bootstrap          # sequential full pull
python bootstrap.py bootstrap-fast     # concurrent full pull (fleet wrapper)
```

`fetch_updates(since=YYYY-MM-DD)` re-reads the listings and skips decisions the
table already dates earlier, so incremental runs download only new PDFs.

## Record shape

```json
{
  "_id": "US-CA-WCAB-EN_BANC-2024-STEVE_HODDINOTT-4bb9d6",
  "_source": "US/CA-WCAB",
  "_type": "case_law",
  "title": "Steve Hoddinott, et al., vs. Bravo Security Services, Inc., et al. — WCAB En Banc Decision (2024-EB-10)",
  "text": "WORKERS' COMPENSATION APPEALS BOARD\nSTATE OF CALIFORNIA\n…",
  "date": "2024-11-14",
  "url": "https://www.dir.ca.gov/wcab/EnBancdecisions2024/Steve_Hoddinott.pdf",
  "court": "California Workers' Compensation Appeals Board",
  "jurisdiction": "US-CA",
  "decision_series": "en_banc",
  "citation": "2024-EB-10",
  "reporter_citation": "89 Cal.Comp.Case",
  "case_numbers": ["ADJ15760386", "…"],
  "summary": "The Appeals Board previously ordered consolidation of these matters…"
}
```

`citation`, `reporter_citation` and `summary` are populated for the two
precedential series only; the ordinary panel listing has no such columns.

## License

[Public domain — 17 U.S.C. § 105](https://www.law.cornell.edu/uscode/text/17/105) —
decisions of a California state adjudicative body are government edicts and are not
subject to copyright. No attribution required; commercial use permitted.
