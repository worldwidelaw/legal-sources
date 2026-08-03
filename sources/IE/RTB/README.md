# IE/RTB — Residential Tenancies Board of Ireland (Determination Orders)

The **Residential Tenancies Board (RTB)** is Ireland's statutory
dispute-resolution body for the residential rental sector under the
**Residential Tenancies Act 2004** (as amended). It resolves landlord/tenant
disputes through **adjudication** and **Tenancy Tribunal** hearings. The binding
outcome of every dispute is issued as a **Determination Order** under s.121 of
the Act. Determination Orders are legally binding on the parties and enforceable
in the Circuit Court — i.e. adjudicative case law.

## Corpus

| Category | Count (approx.) |
|----------|-----------------|
| Adjudication Orders | ~16,400 |
| Tribunal Orders | ~5,100 |
| Court Decisions (Enforcement of Orders) | ~33 |
| **Total** | **~21,500** (2015–present) |

Common dispute types include overholding, rent arrears, breach of landlord/tenant
obligations, validity of notice of termination, deposit retention, anti-social
behaviour and rent-pressure-zone rent reviews.

## Access

The RTB publishes each Determination Order as one WordPress post with the order
document attached as a PDF, exposed through the public **WordPress REST API**:

- **Enumerate orders** — one custom post type per category:
  `/wp-json/wp/v2/adjudication-order`, `/wp-json/wp/v2/tribunal-order`,
  `/wp-json/wp/v2/court-decision-order` (100/page, deep pagination). Each post
  carries the parties (title) and a `dispute-type` taxonomy term.
- **Order PDF** — `/wp-json/wp/v2/media?parent={post_id}` returns the attached
  Determination Order PDF.
- **Full text** — the PDFs are **scanned images** (no text layer), so full text
  is extracted via **tesseract OCR**: PyMuPDF rasterises each page and the PNG
  is piped to `tesseract` on stdin. The **Case Reference** (DR/TR number) and the
  order date are parsed from the OCR text. A born-digital text layer, where
  present, is used in preference to OCR.

No authentication is required.

## Output schema

Each normalized record includes: `_id`, `_source` (`IE/RTB`), `_type`
(`case_law`), `title` (parties), `text` (full OCR'd order), `date`, `url`,
`case_ref`, `order_type` (adjudication/tribunal/court-decision), `dispute_types`,
`court`, `jurisdiction` (`IE`), `language` (`en`).

## Usage

```bash
python bootstrap.py bootstrap --sample   # 15 sample records
python bootstrap.py bootstrap            # full pull (~21.5K orders)
python bootstrap.py update               # incremental (recently modified)
python bootstrap.py test                 # connectivity check
```

Requires `PyMuPDF` (fitz) and the `tesseract` binary on the host. OCR of the full
corpus is time-consuming; set `PDF_OCR_DPI` / `PDF_OCR_MAX_PAGES` to tune.

## License

[PSI Licence / CC BY 4.0](https://www.gov.ie/en/help/re-use-of-public-sector-information/) — Irish public sector information re-use framework (default CC BY 4.0); attribution required, commercial use permitted.
