# INTL/UNDT — UN Dispute Tribunal Judgments

Judgments from the United Nations Dispute Tribunal (UNDT), the first-instance
tribunal for UN staff employment disputes. ~2769 judgments from 2009 to present.

## Data Source

- **URL**: https://www.un.org/en/internaljustice/undt/judgments-orders.shtml
- **Format**: PDF documents linked from yearly HTML index pages
- **Coverage**: 2009–2026, ~100-220 judgments per year
- **Language**: English (some French)

## Strategy

1. Scrape yearly index pages (judgments_YYYY.shtml) to find PDF links
2. Filter for UNDT judgment PDFs (pattern: undt-YYYY-NNN.pdf)
3. Download each PDF and extract full text via `common/pdf_extract`

## License

[UN Terms of Use](https://www.un.org/en/about-us/terms-of-use) — public tribunal judgments.
