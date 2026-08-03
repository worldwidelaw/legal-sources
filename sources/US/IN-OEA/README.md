# US/IN-OEA — Indiana Office of Environmental Adjudication (IDEM Final Orders)

Full-text **Final Orders** of the **Indiana Office of Environmental
Adjudication (OEA)**, Indiana's independent administrative tribunal for
appeals of permits, orders and other actions of the Indiana Department
of Environmental Management (IDEM). An Environmental Law Judge hears the
contested case and issues Findings of Fact, Conclusions of Law and a
Final Order that resolves that specific dispute between a party and IDEM
— each is **case_law**.

- **Publisher:** Indiana Office of Administrative Law Proceedings (OALP) / Office of Environmental Adjudication
- **Coverage:** 1996–present, ~389 final orders + related judicial-review orders
- **Type:** `case_law`
- **Jurisdiction:** US-IN (Indiana)

## Access

No JavaScript, no CAPTCHA, no auth. The Final Orders index
(`in.gov/oalp/final-decisions/idem/final-orders/`) is a server-rendered
page linking to one listing page per year
(`/oalp/final-decisions/idem/final-orders/{YEAR}-decisions/`). Each year
page server-renders anchors to the decision documents, served as
born-digital text-layer PDFs at Indiana's document store
(`in.gov/dA/{hash}/{filename}.pdf?language_id=1`). The anchor text is the
party/facility name; the filename encodes the cause number
(`2020OEA1` → "2020 OEA 1"; recent files use the OALP tag,
`2025OALP074` → "2025 OALP 74").

### Strategy

1. GET the Final Orders index → collect the `{YEAR}-decisions` pages.
2. GET each year page → collect the `/dA/.../*.pdf` links + anchor text.
3. Download each PDF (curl, browser UA, ~1 req/s), extract text via
   `common.pdf_extract`, parse cause number + order date, normalize into
   the `case_law` schema.

## Usage

```bash
python bootstrap.py test-api            # connectivity + extraction test
python bootstrap.py bootstrap --sample  # ~12 sample records
python bootstrap.py bootstrap           # full pull (~389 orders)
python bootstrap.py bootstrap-fast      # alias for full pull (VPS wrapper)
```

## Follow-up

The sibling **DNR/CADDNAR** natural-resources adjudications
(`in.gov/oalp/final-decisions/dnr/caddnar`) live under the same OALP tree
but use a separate structure (citation index PDF + volume set) and could
extend this source with hundreds more Indiana administrative cases.

## License

[US Public Domain — 17 U.S.C. § 105](https://www.law.cornell.edu/uscode/text/17/105) — Final Orders of the Indiana Office of Environmental Adjudication are official Indiana state-government works in the public domain under the government-edicts doctrine. Commercial use permitted; no attribution required.
