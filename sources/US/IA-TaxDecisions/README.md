# US/IA-TaxDecisions — Iowa Department of Revenue, Administrative & Declaratory Orders

Full text of the Iowa Department of Revenue's published administrative
documents from the **Iowa Revenue Research Library** on the state's
DocsIowaGov portal:

- **Declaratory Orders** — binding rulings on a petitioner's specific facts
  (Iowa Code § 17A.9) → `case_law`
- **Contested-case Orders** — final adjudications of a taxpayer protest →
  `case_law`
- **Director Orders** — e.g. annual corporate income-tax rate certifications
  → `doctrine`
- **Petition-for-rulemaking rulings / policy** → `doctrine`

~1,400 documents, all public, taxpayer-facing, born-digital PDFs.

## Access

No auth, no CAPTCHA on the data endpoints (reCAPTCHA gates only the ADA
request form). Documents are served by `documents.iowa.gov` (DocsIowaGov),
an OpenText-backed library shared by six Iowa agencies.

1. **Discovery** — `POST /home/search` (server-side DataTables), filtered to
   the Revenue organization (OpenText category `10350462`) via
   `query=(OTDCategory:10350462)`, paginated `start`/`length` (max 100/page).
   Each row: `id`, `name` (caption), `mime_type`, `create_date`,
   `short_summary`.
2. **Full text** — `GET /home/download/{id}` returns the raw PDF
   (verified via `%PDF-` magic); extracted with the shared OOM-hardened
   `common.pdf_extract` helper (born-digital, no OCR needed).

## Usage

```bash
python bootstrap.py test-api            # connectivity + extraction test
python bootstrap.py bootstrap --sample  # ~12 samples
python bootstrap.py bootstrap           # full pull (~1,400 docs)
python bootstrap.py bootstrap-fast      # alias for full pull (VPS wrapper)
```

## Record schema

`_id`, `_source`, `_type` (`case_law` or `doctrine`), `_fetched_at`,
`doc_id`, `cite`, `issuer`, `summary`, `title`, `text` (full text), `date`
(ISO 8601), `url`, `jurisdiction` (`US-IA`).

## License

[Public Domain (US Government Work)](https://www.law.cornell.edu/uscode/text/17/105)
— Declaratory orders, contested-case orders and director orders of the Iowa
Department of Revenue are official state-government works published for public
inspection, in the public domain under the government-edicts doctrine.
Commercial use permitted; no attribution required.
