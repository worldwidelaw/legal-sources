# US/HI-LegalEthics — Disciplinary Board of the Hawaiʻi Supreme Court — Formal (Ethics) Opinions

Full text of the **Formal Opinions** issued by the **Disciplinary Board of the
Hawaiʻi Supreme Court**. Each Formal Opinion interprets the Hawaiʻi Rules of
Professional Conduct as applied to a lawyer's contemplated conduct, advising
**lawyers** — this is legal **doctrine** (the Hawaiʻi member of the
`US/{ST}-LegalEthics` state-bar attorney legal-ethics vein).

- **Publisher:** Disciplinary Board of the Hawaiʻi Supreme Court (Office of
  Disciplinary Counsel), an arm of the Hawaiʻi Supreme Court.
- **Citation:** "Formal Opinion No. 44".
- **Corpus:** ~19 born-digital PDF opinions.
- **Source page:** <https://dbhawaii.org/legal-ethics-advice-for-hawaii-lawyers/>

## How it works

1. A single public page lists every Formal Opinion PDF as an anchor (topic
   title) pointing to a born-digital PDF under `/wp-content/uploads/`
   (WordPress). No JavaScript, CAPTCHA or auth.
2. Each PDF is downloaded and its text extracted with PyMuPDF (fitz) — **no
   OCR**. Records under 200 chars are skipped.
3. The opinion number is read from the body header (`FORMAL OPINION NO. N`),
   falling back to the filename; a combined document (`FO_18_and_22.pdf`) is
   keyed `18-22`. The issue date is the first `Month DD, YYYY` in the body.

## Usage

```bash
python bootstrap.py test-api            # connectivity + extraction test
python bootstrap.py bootstrap --sample  # ~12 sample records
python bootstrap.py bootstrap           # full pull
```

## Fields

`_id`, `_source`, `_type` (`doctrine`), `_fetched_at`, `opinion_number`,
`issuer`, `title`, `text` (full opinion text), `url`, `date`, `jurisdiction`
(`US-HI`).

## License

Public domain — freely published Formal Opinions.

[Public Domain / freely published formal opinions](https://dbhawaii.org/legal-ethics-advice-for-hawaii-lawyers/) — the
Disciplinary Board of the Hawaiʻi Supreme Court is an arm of the Hawaiʻi Supreme
Court, so the 17 U.S.C. § 105 government-edicts rationale applies (like
`US/SC`/`LA`/`WI`/`GA`/`MT`/`DC`/`NM-LegalEthics`). Opinions are published free on
dbhawaii.org with no login, paywall or terms prohibiting reuse. Treated as
effectively public domain — commercial use OK.
