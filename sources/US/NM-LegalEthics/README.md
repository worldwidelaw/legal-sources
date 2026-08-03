# US/NM-LegalEthics — State Bar of New Mexico, Ethics Advisory Committee (Ethics Advisory Opinions)

Full text of the formal ethics advisory opinions issued by the **State Bar of
New Mexico Ethics Advisory Committee**. Each opinion interprets the New Mexico
Rules of Professional Conduct as applied to a lawyer's contemplated conduct to
advise **lawyers** — this is professional-responsibility **doctrine**, not case
law. New Mexico cites its opinions by a `{year}-{n}` number (e.g. *Advisory
Opinion 1987-5*).

## Source

- **Publisher:** State Bar of New Mexico — Ethics Advisory Committee (New
  Mexico's integrated / mandatory bar)
- **Index:** https://www.sbnm.org/Leadership/Committees/Ethics-Advisory-Committee/Ethics-Advisory-Opinions
- **Coverage:** ~94 opinions, 1983–present, each a born-digital PDF.

## How it works

1. `bootstrap.py` fetches the single public listing page and collects every
   ethics-opinion PDF under `/Portals/NMBAR/AboutUs/committees/Ethics/`
   (a DNN portal), de-duplicating on the canonical opinion number.
2. For each opinion it downloads the born-digital PDF and extracts the full
   text with PyMuPDF (fitz) — **no OCR, no login**. Records under 200 chars are
   skipped.
3. `opinion_number` is the filename stem `{YYYY-N}` (sequence zero-padding
   dropped: `2021-001` → `2021-1`); `date` is the opinion year (`YYYY-01-01`).

## Usage

```bash
python bootstrap.py test-api             # connectivity + extraction check
python bootstrap.py bootstrap --sample   # ~12 sample records
python bootstrap.py bootstrap            # full pull
```

## Distinct from

`US/NM-Legislation`, `US/NM-Courts`. This is the attorney professional-conduct
advisory-opinion series (advising lawyers), the New Mexico member of the
`US/{ST}-LegalEthics` vein. Reuses the DNN-portal index → born-digital-PDF
recipe (cf. `US/MT-LegalEthics` on montanabar.org).

## License

[Public Domain / freely published advisory opinions](https://www.sbnm.org/Leadership/Committees/Ethics-Advisory-Committee/Ethics-Advisory-Opinions)
— State Bar of New Mexico ethics opinions are published free to the public on
sbnm.org with no login, paywall or terms prohibiting reuse. The State Bar of New
Mexico is New Mexico's **integrated (mandatory)** bar, so the 17 U.S.C. § 105
government-edicts rationale applies fairly directly (as with the other
integrated-bar legal-ethics sources). Treated as effectively public domain —
commercial use OK.
