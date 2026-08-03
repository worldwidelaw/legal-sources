# US/RI-LegalEthics — Rhode Island Supreme Court Ethics Advisory Panel Opinions

Full text of the advisory opinions issued by the **Rhode Island Supreme Court
Ethics Advisory Panel**, which the Supreme Court established in 1986 to give
Rhode Island attorneys confidential, prospective advice on the **Rhode Island
Rules of Professional Conduct**. Each opinion analyses a stated inquiry (FACTS →
analysis → conclusion) and is guidance to **lawyers** = `doctrine` (advisory).

The Panel is an arm of the Supreme Court of Rhode Island, created and governed
by the Court's own rules.

## Corpus

- One continuous numbered series `{YY|YYYY}-{N}` — older opinions use a 2-digit
  year (`87-03`, `98-12`), newer ones a 4-digit year (`2024-01`).
- ~782 opinion PDFs are indexed; **~221 (≈1998–present) are born-digital** with
  a text layer and are captured in full.
- Pre-1998 opinions (1986–1997) survive only as **scanned image PDFs** with no
  text layer and are skipped (no OCR).

## Source & access

- Official page:
  https://www.courts.ri.gov/attorney-resources/Pages/Ethics-Advisory-Panel-default.aspx
- Discovery: the Judiciary's public SharePoint search REST endpoint
  `/_api/search/query?querytext='RIJCourt:"Ethics Advisory Panel"'` returns one
  row per opinion PDF (paginated with `rowlimit` ≤ 500 + `startrow`); each row's
  `Path` is the direct PDF URL under `/Opinions/`.
- Filenames are irregular (raw space vs hyphen, 2- vs 4-digit year, a few with
  no `EAP` prefix). The Path is taken verbatim and URL-quoted; the opinion
  number is parsed from the filename with 2-digit years canonicalised
  (`98` → `1998`).
- Full text: born-digital PDFs → PyMuPDF/`fitz` `get_text`, **no OCR**. No
  login, paywall, CAPTCHA or JavaScript execution required.

## Distinct from

- **US/RI-Courts** — Rhode Island appellate court decisions.
- **US/RI-Legislation** — the Rhode Island General Laws.
- **Rhode Island Ethics Commission** (`ethics.ri.gov`) — advises public
  *officials* on the state Code of Ethics, not lawyers.

## Usage

```bash
python bootstrap.py test-api             # connectivity + extraction test
python bootstrap.py bootstrap --sample   # ~12 sample records
python bootstrap.py bootstrap            # full pull
```

## License

[Public Domain — U.S. government edict (17 U.S.C. § 105)](https://www.law.cornell.edu/uscode/text/17/105) —
the Ethics Advisory Panel was established by, and operates under the rules of,
the Supreme Court of Rhode Island; its opinions interpret the Rhode Island Rules
of Professional Conduct. As the work of a body created and authorized by the
state's highest court, the opinions are treated as public domain under the
government-edicts rationale, consistent with the other state legal-ethics
sources. Published free to the public on courts.ri.gov with no login, paywall or
terms prohibiting reuse. **Commercial use permitted.**
