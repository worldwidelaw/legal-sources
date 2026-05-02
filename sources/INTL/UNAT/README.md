# INTL/UNAT — UN Appeals Tribunal Judgments

Judgments from the United Nations Appeals Tribunal (UNAT), the appellate body
for UN staff employment disputes. ~1500+ judgments from 2010 to present.

## Data Source

- **URL**: https://www.un.org/en/internaljustice/unat/judgments-orders.shtml
- **Format**: PDF documents linked from yearly HTML index pages
- **Coverage**: 2010–2026, ~80-140 judgments per year
- **Language**: English (some French and Arabic)

## Strategy

1. Scrape yearly index pages (judgments_YYYY.shtml) to find PDF links
2. Filter for UNAT judgment PDFs (pattern: YYYY-UNAT-NNNN.pdf)
3. Download each PDF and extract full text via `common/pdf_extract`

## License

[UN Terms of Use](https://www.un.org/en/about-us/terms-of-use) — public tribunal judgments.
