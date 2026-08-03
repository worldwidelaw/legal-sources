# US/NC-LegalEthics — North Carolina State Bar: Legal (Attorney) Ethics Opinions

Formal ethics opinions adopted by the Council of the **North Carolina State Bar**
on the recommendation of its Ethics Committee. The State Bar is the agency that
licenses and regulates lawyers in North Carolina; each opinion answers a specific
inquiry about a lawyer's obligations under the **North Carolina Rules of
Professional Conduct** and states the Committee's conclusion. This is the State
Bar's official written interpretation of the attorney-conduct rules = **doctrine**.

The corpus spans three historical series, all under one index:

- **CPR** — opinions under the pre-1985 Code of Professional Responsibility
- **RPC** — opinions under the 1985 Rules of Professional Conduct
- **Formal Ethics Opinions** — "{YYYY} Formal Ethics Opinion N", under the
  Revised Rules of Professional Conduct (1997–present)

~578 adopted opinions, 1974–present.

## Access

No JavaScript execution, CAPTCHA, or auth required.

1. The listing page is a Lit web component backed by a **public JSON search
   endpoint**:

   ```
   POST https://www.ncbar.gov/myethicsopinions/search/
   Content-Type: application/json
   {"IndexGuid": "<guid>", "Term": [], "Status": ["adopted"],
    "StartDate": "", "EndDate": "", "Categories": [],
    "PageNumber": <0-based>, "SortBy": "date-asc"}
   ```

   Response: `{"success": true, "data": {"results": [{headline, dateString,
   statusValue, url}, …], "totalCount", "totalPages"}}`. 20 results per page.

2. The `IndexGuid` is read live from the listing page's
   `<ethics-opinions-index-page-sidebar-lit indexGuid="…">` attribute (with a
   hard-coded fallback).

3. Each result `url` is a **born-digital HTML** opinion page; the full text sits
   in `<div class="ethicsContent">` (Inquiry / Opinion / analysis + footnotes)
   and is extracted directly from the HTML — no OCR, no PDF.

## Distinct from other NC sources

- **US/NC-EthicsOpinions** — the NC State Ethics Commission (covers public
  officials/employees, not attorneys).
- **US/NC-AGOpinions** — NC Attorney General legal opinions.

## Usage

```bash
python bootstrap.py test-api              # connectivity + extraction test
python bootstrap.py bootstrap --sample    # ~12 sample records
python bootstrap.py bootstrap             # full pull
```

## License

[Public Domain (North Carolina State Bar Official Record)](https://www.law.cornell.edu/uscode/text/17/105) — ethics opinions adopted by the North Carolina State Bar are official public records of a North Carolina state regulatory agency, published on the State Bar website for public use with no copyright restriction. Commercial use permitted.
