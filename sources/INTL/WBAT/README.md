# INTL/WBAT — World Bank Administrative Tribunal

Judgments and orders from the World Bank Administrative Tribunal (WBAT),
covering employment disputes between the World Bank Group (IBRD, IDA, IFC,
MIGA) and its staff. ~798 decisions from Decision No. 1 (1981) to present.

## Data Source

- **URL**: https://tribunal.worldbank.org/
- **Format**: PDF documents listed on an HTML page
- **Coverage**: Decisions 1–725+ including orders and preliminary objections
- **Language**: English (some French)

## Strategy

1. Scrape the all-judgments page to extract metadata (title, number, date, PDF URL, summary URL)
2. Download each judgment PDF
3. Extract full text via `common/pdf_extract`

## License

[World Bank Administrative Tribunal Terms](https://tribunal.worldbank.org/) — public tribunal decisions, no restrictive reuse terms found.
