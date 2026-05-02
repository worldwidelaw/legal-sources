# INTL/EFTA-ESA — EFTA Surveillance Authority Decisions

EEA enforcement decisions from the EFTA Surveillance Authority (ESA), covering state aid, competition, and internal market compliance for Iceland, Liechtenstein, and Norway.

- **Source:** https://www.eftasurv.int/
- **Coverage:** ~1500+ college decisions (1994–present)
- **Format:** JSON API listing + PDF full text
- **Language:** English

## How it works

1. Paginates the ESA internal JSON API at `/cms/api/node` (50 items/page)
2. Downloads PDF attachments for each decision
3. Extracts full text from PDFs via `common/pdf_extract`

## License

[EFTA Surveillance Authority Terms](https://www.eftasurv.int/) — public enforcement decisions, no restrictive reuse terms found.
