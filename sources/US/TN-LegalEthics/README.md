# US/TN-LegalEthics — Tennessee BPR Formal Ethics Opinions

Full text of the **Formal Ethics Opinions** issued by the **Board of
Professional Responsibility (BPR) of the Supreme Court of Tennessee**.

Each opinion is the Board's written interpretation of the **Tennessee Rules of
Professional Conduct** in response to an inquiry from an attorney, issued as
guidance to lawyers statewide. These are **advisory** (doctrine) — attorney
discipline itself is imposed by the Board's hearing process and the Supreme
Court of Tennessee. One continuous series numbered `{YY|YYYY}-F-{N}` runs from
`80-F-1` (1980) to the present (`2025-F-172` and later), ~171 unique opinions.

The Board of Professional Responsibility is an arm of the Supreme Court of
Tennessee, created under **Tenn. Sup. Ct. R. 9**, with authority over the
licensure and discipline of Tennessee attorneys.

## Source

- **Publisher:** Board of Professional Responsibility of the Supreme Court of Tennessee
- **Index:** <https://www.tbpr.org/for-legal-professionals/formal-ethics-opinions>
- **Detail pages:** `https://www.tbpr.org/ethic_opinions/{slug}`
- **Type:** `doctrine` (attorney-conduct advisory opinions)
- **Jurisdiction:** `US-TN`

## How it works

1. **Discovery** — the single index page links every opinion as
   `/ethic_opinions/{slug}`. Some opinions are linked twice (a bare-number
   slug and a topic slug); they are de-duplicated on the canonical opinion
   number, keeping the fullest text.
2. **Full text** — each detail page renders the opinion body in clean HTML
   inside `<main>`, beginning at an `<h2>` that carries the printed number and
   topic title. Everything from that heading onward is extracted with
   BeautifulSoup — **no PDF, no OCR**.
3. **Number** — parsed from the heading. Newest opinions prefix it with
   "Formal Ethics Opinion"; revision/supplement suffixes are parenthesised
   there (e.g. `2002-F-91(a)`). Two-digit years (80–99) expand to 19xx.
4. **Date** — opinions are dated by the year in the number (`YYYY-01-01`),
   unless the body carries an explicit in-range `Month [DD,] YYYY`.

## Usage

```bash
python bootstrap.py test-api            # connectivity + extraction test
python bootstrap.py bootstrap --sample  # ~12 sample records
python bootstrap.py bootstrap           # full pull (all opinions)
python bootstrap.py bootstrap-fast      # alias for full pull (VPS wrapper)
```

## License

[Public domain — US government edict (17 U.S.C. § 105)](https://www.tbpr.org/for-legal-professionals/formal-ethics-opinions) — the Board of Professional Responsibility is an arm of the Supreme Court of Tennessee (Tenn. Sup. Ct. R. 9). Its Formal Ethics Opinions are published free to the public on tbpr.org with no login, paywall or reuse restrictions. Commercial use OK.

## Distinct from

- **US/TN-Courts** — Tennessee court judgments.
- **US/TN-Legislation** — Tennessee statutes/regulations.

This is the **attorney professional-conduct advisory-opinion** series (lawyers).
