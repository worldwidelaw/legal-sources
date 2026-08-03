# US/LA-LegalEthics — Louisiana State Bar Association, Rules of Professional Conduct Committee (PUBLIC Ethics Opinions)

Full text of the **PUBLIC** ethics advisory opinions issued by the Louisiana
State Bar Association (LSBA) **Rules of Professional Conduct Committee** (via
its Publications Subcommittee). Each opinion answers, on the basis of an actual
member advisory-service inquiry, how the **Louisiana Rules of Professional
Conduct** apply to contemplated attorney conduct. Only opinions the Committee
has expressly designated **"PUBLIC"** are published and may be cited. These are
**doctrine**.

- **Series:** `YYYY-RPCC-NNN` (e.g. `2005-RPCC-001`, `2021-RPCC-022`)
- **Coverage:** ~22 public opinions, 2005–present
- **Publisher:** Louisiana State Bar Association (lsba.org), Louisiana's *integrated* (mandatory) bar
- **Format:** born-digital PDFs (PyMuPDF text extraction, no OCR)

## How it works

1. **Discovery** — the single public page `/members/EthicsAdvisory.aspx` lists
   every PUBLIC opinion as an anchor whose visible text starts with the number
   and carries the date, e.g. `05-LSBA-RPCC-001 PUBLIC Opinion (04/04/2005)`,
   linking to a PDF under `/documents/Ethics/`.
2. **Extraction** — each PDF is born-digital; text is extracted with PyMuPDF
   (`fitz`). The number and date are taken from the authoritative index anchor
   (the PDF body occasionally carries a typo).

## Usage

```bash
python bootstrap.py test-api             # connectivity + extraction check
python bootstrap.py bootstrap --sample   # ~12 sample records
python bootstrap.py bootstrap            # full pull (all opinions)
```

## Note on corpus size

Only the ~22 opinions the Committee has designated "PUBLIC" are published; the
rest of the LSBA advisory service is confidential (member-only). This source
therefore captures the **complete public corpus**.

## Distinct from other Louisiana sources

- **US/LA-Courts** — Louisiana court decisions.
- **US/LA-Legislation** — Louisiana statutes.

This source is the attorney professional-conduct advisory-opinion series
(lawyers), matching `US/{ST}-LegalEthics` in other states.

## License

Public Domain / freely published advisory opinions —
[LSBA Ethics Advisory Service and Opinions](https://www.lsba.org/members/EthicsAdvisory.aspx).

LSBA PUBLIC ethics opinions are published free to the public on lsba.org,
expressly designated "PUBLIC" and citable, with no login, paywall or terms
prohibiting reuse. The LSBA is Louisiana's **integrated (mandatory)** bar,
established by the Louisiana Supreme Court, so the 17 U.S.C. § 105
government-edicts rationale applies fairly directly (like US/SC-LegalEthics).
Each PDF carries a "© YYYY by the Louisiana State Bar Association" notice, but
the opinions are the mandatory bar's advisory interpretations of the Rules of
Professional Conduct, published free for citation — treated as effectively
public domain. Commercial use: permitted.
