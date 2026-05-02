# INTL/OECDTribunal — OECD Administrative Tribunal Judgments

Judgments from the OECD Administrative Tribunal, covering employment
disputes between the OECD and its staff. ~92+ judgments available as
PDFs via predictable URL patterns.

## Data Source

- **URL**: https://www.oecd.org/en/about/administrative-tribunal/all-judgements.html
- **Format**: PDF documents at predictable URLs
- **Coverage**: Judgments 1–120 (with gaps for missing/combined docs)
- **Language**: English and French

## Strategy

1. Enumerate PDF URLs using the pattern `TAOECD_judgement_{N}.pdf`
2. Try both spellings ("judgement" and "judgment") for each number
3. Also check combined documents (e.g., `TAOECD_judgement_86_89.pdf`)
4. Download each PDF and extract full text via `common/pdf_extract`

## License

[OECD Terms of Use](https://www.oecd.org/en/about/terms-conditions.html) — public tribunal judgments, reuse with attribution.
