# IE/ValuationTribunal — Valuation Tribunal of Ireland (An Binse Luachála)

Full text of the **Valuation Tribunal** of Ireland's written determinations.

The Valuation Tribunal is Ireland's independent statutory body constituted under
the **Valuation Acts 2001–2015**. It hears and determines appeals against the
valuations of commercial and industrial property fixed by **Tailte Éireann**
(formerly the Valuation Office / Commissioner of Valuation) for the purpose of
local-authority commercial rates. Its jurisdiction covers revision appeals,
revaluation and post-revaluation revision appeals, global valuation appeals
(utility/network undertakings), and vacant-site and derelict-site levy appeals.

Each written **determination** finally adjudicates a specific contested appeal —
i.e. adjudicative case law — and is a public government-edict work.

## Data

- **Type:** `case_law`
- **Coverage:** ~3,000 determinations, 2014–present (plus earlier digitised)
- **Language:** English (bilingual EN/GA headers)
- **Format:** born-digital "Final Determination" PDFs (text layer, no OCR)
- **Auth:** none (free public access)

## Access / recipe

- The public judgments page uses an `admin-ajax.php` filter that is
  **AWS-WAF/CAPTCHA-gated**, but the read-only **WordPress REST API** answers
  plain GET requests:
  - `GET /wp-json/wp/v2/posts?per_page=100&page=N` — enumerate all judgment
    posts (~30 pages; `X-WP-Total: 3000`). Each post's title carries the appeal
    number (e.g. `VA23.5.0792 Mulhern's Gala`) and its content links the
    determination PDF.
- Determination PDFs live under
  `/wp-content/uploads/YYYY/MM/{appeal}-Final-Determination-VT-WEBSITE.pdf`.
- Full text extracted with **PyMuPDF (fitz)** — born-digital text layer, no OCR.
- The determination date is parsed from the PDF body
  (`ISSUED ON THE Nth DAY OF MONTH YYYY`), falling back to the WordPress publish
  date; the appeal number is parsed from the post title.

## Usage

```bash
python bootstrap.py test                 # connectivity check
python bootstrap.py bootstrap --sample   # 15 validation samples
python bootstrap.py bootstrap            # full pull
python bootstrap.py update               # incremental (recent posts)
```

## License

[PSI Licence / CC BY 4.0](https://www.gov.ie/en/help/re-use-of-public-sector-information/)
— Irish public sector information re-use framework (default CC BY 4.0).
Attribution required; commercial use permitted.
