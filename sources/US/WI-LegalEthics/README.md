# US/WI-LegalEthics — State Bar of Wisconsin Professional Ethics Opinions

Full text of the ethics opinions issued by the **State Bar of Wisconsin's
Standing Committee on Professional Ethics**. Each opinion is the Committee's
written interpretation of the **Wisconsin Supreme Court Rules of Professional
Conduct for Attorneys** (SCR ch. 20), advising lawyers on the ethical propriety
of contemplated conduct.

- **Publisher:** State Bar of Wisconsin (the state's integrated bar)
- **Coverage:** ~411 opinions across several historical series — `E-`/`EF-`
  (Formal), `EI-`/`IE-`/`I-` (Informal), `M-`/`Memo`/`EM-` (Memorandum), from
  the 1950s to present
- **Type:** `doctrine` (advisory ethics opinions interpreting the SCR)
- **Full text:** yes — born-digital PDFs (PyMuPDF, no OCR)

## Source & method

- **Enumeration:** the opinions live in a public SharePoint document library on
  wisbar.org (folder `/formembers/ethics/Ethics Opinions`). The web page shows
  only the first 30 rows, but the full folder is enumerated anonymously via the
  SharePoint REST API:
  `/formembers/ethics/_api/web/GetFolderByServerRelativeUrl('/formembers/ethics/Ethics Opinions')/Files?$top=3000`
  (~415 PDFs, ~411 unique opinion codes).
- **Number:** parsed from the filename (`E-00-01`, `EF-16-03`, `EI-11-01`,
  `M-8-75`, `MEMO-3-73`, …). A standalone trailing letter (e.g. `M-8-75 C`) is
  preserved to avoid collapsing distinct revised opinions.
- **Full text:** each PDF is born-digital; text is extracted with PyMuPDF. The
  title is taken from the PDF's leading
  `Wisconsin (Formal) Ethics Opinion {code}: {title}` line.
- **Date:** WI opinions are dated by year, encoded in the code. Year-first
  series (`E`/`EF`/`EI`/`IE`) use the first number as the 2-digit year;
  year-last series (`M`/`EM`/`I`/`Memo`) use the last. `YY ≤ 25 ⇒ 20YY`, else
  `19YY`. `date ⇒ YYYY-01-01`.

No JavaScript, CAPTCHA or authentication is required.

## Distinct from

- **US/WI-EthicsOpinions** — the executive Wisconsin Ethics Commission (public
  officials / campaign finance), on ethics.wi.gov.
- **US/WI-Courts**, **US/WI-Legislation** — court decisions and statutes.

This is the attorney professional-conduct opinion series that in other states is
built as `US/{ST}-LegalEthics`.

## Usage

```bash
python bootstrap.py test-api             # connectivity + extraction test
python bootstrap.py bootstrap --sample   # ~12 sample records
python bootstrap.py bootstrap            # full pull (all opinions)
```

## License

[Public Domain / freely published advisory opinions](https://www.wisbar.org/formembers/ethics/Pages/Ethics-Opinions.aspx) — no attribution required.

State Bar of Wisconsin professional ethics opinions are published free to the
public on wisbar.org (anonymous SharePoint document library) as interpretations
of the Wisconsin SCR ch. 20 Rules of Professional Conduct. They are advisory and
carry no login, paywall or terms prohibiting reuse. Treated as effectively
public domain, consistent with the other state-bar legal-ethics sources. The
State Bar of Wisconsin is the state's integrated (mandatory) bar, so the
17 U.S.C. § 105 government-edicts rationale applies directly. Commercial use
permitted.
