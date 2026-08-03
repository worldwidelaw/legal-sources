# US/AL-TaxTribunal — Alabama Tax Tribunal (Decisions & Orders)

Full text of the decisions and orders of the **Alabama Tax Tribunal**, Alabama's
independent executive-branch quasi-judicial tribunal created by the Alabama
Taxpayer Fairness Act (Act 2014-146, operative October 1 2014; successor to the
Administrative Law Division of the Alabama Department of Revenue). The Tribunal
hears appeals of tax and related matters administered by the Alabama Department
of Revenue and by self-administered counties and municipalities — individual and
corporate income tax, sales & use tax, business privilege tax, financial
institution excise tax, ad valorem tax, withholding, penalties, motor-vehicle and
mandatory liability insurance matters, and more. Disputes are between taxpayers
and the Revenue Department (or a local taxing authority), so the corpus is
**case_law**. The Tribunal also republishes the Administrative Law Division's
decisions back to the 1980s.

## Data access

- **Discovery:** the site is WordPress with a custom `decisions` post type
  exposed via the WP REST API:
  `GET /wp-json/wp/v2/decisions?per_page=100&page=N` (newest-first,
  `X-WP-Total` ≈ 1,934). Each item gives the post id, slug, the entered date
  (post `date`), the docket number (`title`), the canonical `/decisions/{slug}/`
  link, and the `decision-type` / `appeal-type` / `decision-category` taxonomy
  term IDs (resolved to labels via the matching `/wp-json/wp/v2/<taxonomy>`
  endpoints).
- **Full text:** each decision page has a single **"Download PDF"** button — an
  Elementor anchor whose class contains `elementor-button-link` and whose `href`
  is the born-digital text-layer PDF under `/wp-content/uploads/`. The scraper
  downloads it and extracts text via the shared `common.pdf_extract` helper. The
  taxpayer/case name is parsed from the PDF body (the text before
  `Taxpayer,` / `Petitioner,` / `Appellant`).
- No JavaScript-gated content, no CAPTCHA, no auth.

## Usage

```bash
python bootstrap.py test-api            # connectivity + extraction test
python bootstrap.py bootstrap --sample  # ~12 sample records
python bootstrap.py bootstrap           # full pull (~1,930 decisions)
python bootstrap.py bootstrap-fast      # alias for full pull (VPS wrapper)
```

## License

[Public Domain (US Government Work)](https://www.law.cornell.edu/uscode/text/17/105)
— decisions of the Alabama Tax Tribunal are official quasi-judicial government
works, in the public domain under the government-edicts doctrine. Commercial use
permitted; no attribution required.
