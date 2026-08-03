# US/CA-LegalEthics — State Bar of California (COPRAC): Formal Ethics Opinions

**Formal Opinions** issued by the **State Bar of California's Standing Committee
on Professional Responsibility and Conduct (COPRAC)**. Each opinion applies the
California Rules of Professional Conduct (and the State Bar Act) to a stated
question and advises lawyers whether the described conduct is proper.

- **Type:** `doctrine` (official interpretation of the attorney-conduct rules)
- **Jurisdiction:** US-CA (California)
- **Corpus:** ~188 opinions, "CAL {YYYY}-{N}", CAL 1965-1 → present (CAL 2026-210)
- **Full text:** yes — born-digital HTML (1965–2001) + born-digital PDF (2002–present), no OCR

## Source & access

The State Bar publishes every opinion, in full, on a single **Ethics Opinions**
listing page:

`https://www.calbar.ca.gov/legal-professionals/ethics-compliance-practice-resources/ethics/ethics-opinions`

Each opinion appears as a **direct document link** whose anchor text is exactly
the opinion number (`CAL YYYY-N`). Older opinions (1965–2001) are born-digital
**HTML** pages (`.htm`, which 301-redirect to `/sites/default/files/...`); newer
opinions (2002–present) are born-digital **PDFs**. Filenames are irregular, so
the exact href is always taken from the listing anchor, never constructed.

- HTML opinions: `GET` (following the redirect) and slice the `<body>` text.
- PDF opinions: download and extract the text layer with PyMuPDF (no OCR).

No JavaScript execution, no CAPTCHA, no auth.

## Distinct from other California sources

- **US/CA-FPPC** — Fair Political Practices Commission (advises *public
  officials* on political ethics). This source is the *State Bar* advising
  *lawyers* on the Rules of Professional Conduct.
- Also distinct from California Attorney General opinions.

Sixth source in the state-bar attorney-ethics vein after US/NC-, US/AZ-, US/TX-,
US/UT-, US/VA-LegalEthics.

## Usage

```bash
python bootstrap.py test-api            # connectivity + extraction check
python bootstrap.py bootstrap --sample  # ~12 sample records
python bootstrap.py bootstrap           # full pull (all opinions)
```

## License

[Public Domain — U.S. Government / State Official Record](https://www.law.cornell.edu/uscode/text/17/105) — Formal Ethics Opinions of the State Bar of California's Standing Committee on Professional Responsibility and Conduct (COPRAC) are official public records of a California regulatory body (the State Bar is a public corporation and the administrative arm of the Supreme Court of California), published on calbar.ca.gov for public use with no copyright restriction. Commercial use permitted.
