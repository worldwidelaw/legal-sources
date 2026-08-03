# INTL/WorldAquatics-Integrity — Aquatics Integrity Unit (AQIU) Decisions

Disciplinary and anti-doping decisions of the **Aquatics Integrity Unit (AQIU)**,
the independent integrity body of **World Aquatics** (formerly FINA). The AQIU
investigates and adjudicates anti-doping, integrity-code, safeguarding and
competition-manipulation matters across the aquatic disciplines — swimming,
water polo, diving, artistic swimming, open water and high diving.

## Data

Two complementary openly published full-text corpora on `aquaticsintegrity.com`:

1. **Reasoned decision PDFs** — born-digital, multi-page Adjudicatory-Body and
   Doping-Panel decisions (facts, rules, reasoning, sanction) linked from the
   [Suspended Persons registry](https://aquaticsintegrity.com/suspended-persons/),
   hosted under `/wp-content/uploads/`. Extracted via `common/pdf_extract`.
2. **Sanction-decision notices** — each disciplinary outcome is also published as
   an official AQIU [news](https://aquaticsintegrity.com/news/) notice carrying
   the respondent, the rule(s) violated, the sanction and the ineligibility
   period. The article body text is extracted from the server-rendered HTML.

Non-decision news (statistics reports, workshops, conferences, strategic plans,
governance/appointment notices) is filtered out. Scanned image-only PDFs with no
extractable text layer are skipped.

The site is a public WordPress install — no login, no WAF, reachable from any IP.

## Usage

```bash
python bootstrap.py test               # list discovered PDFs + news articles
python bootstrap.py bootstrap --sample # fetch a sample
python bootstrap.py bootstrap          # full pull
```

## Record schema

`_id`, `_source`, `_type` (`case_law`), `_fetched_at`, `title`, `text` (full
decision / notice body), `date` (ISO 8601), `url`, `pdf_url`, `document_kind`
(`reasoned_decision` | `sanction_notice`).

## License

> ⚠️ **Commercial use restricted.** Decisions are openly published for
> transparency, but World Aquatics / AQIU asserts copyright with no open licence.

[World Aquatics / AQIU Terms (All Rights Reserved)](https://aquaticsintegrity.com/) —
attribution required; commercial use flagged restricted per project policy.
