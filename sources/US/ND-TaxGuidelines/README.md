# US/ND-TaxGuidelines — North Dakota Office of State Tax Commissioner: Tax Guidelines

Full text of the interpretive **Guidelines** published by the North Dakota
Office of State Tax Commissioner — the agency's official explanations of how
North Dakota taxes apply to specific activities and taxpayer classes (alcohol,
sales & use, income-tax withholding, property-tax assessment, motor fuel,
military service members, and more).

- **Type:** `doctrine` (government tax guidance)
- **Jurisdiction:** US-ND
- **Index:** https://www.tax.nd.gov/guidelines
- **Corpus:** ~94 born-digital PDF guidelines

## How it works

Discovery parses a single server-rendered Drupal index page. Every guideline is
an `<a href>` pointing at a born-digital PDF under
`/sites/www/files/documents/guidelines/{category}/...`, and the anchor text is a
clean human title (e.g. "Alcohol Carriers", "Income Taxation of Native
Americans"). Full text is extracted directly from the PDFs via
`common.pdf_extract` — the documents carry a real text layer, so **no OCR is
required**. No JavaScript, no CAPTCHA, no authentication.

## Usage

```bash
python bootstrap.py test-api            # discovery + extraction smoke test
python bootstrap.py bootstrap --sample  # ~12 sample records
python bootstrap.py bootstrap --full    # full corpus
python bootstrap.py bootstrap-fast      # alias for the full pull (VPS wrapper)
```

## License

[Public Domain (US Government Work)](https://www.law.cornell.edu/uscode/text/17/105) — Guidelines of the North Dakota Office of State Tax Commissioner are official North Dakota state-government works in the public domain under the government-edicts doctrine. Commercial use permitted; no attribution required.
