# US/DC-LegalEthics — District of Columbia Bar, Legal Ethics Committee (Ethics Opinions)

Full text of the ethics opinions issued by the **D.C. Bar Legal Ethics
Committee**. Each opinion interprets the District of Columbia Rules of
Professional Conduct as applied to a lawyer's contemplated conduct to advise
**lawyers** — this is professional-responsibility **doctrine**, not case law.
The D.C. Bar cites its opinions by number (e.g. *D.C. Legal Ethics Opinion
388*).

## Source

- **Publisher:** District of Columbia Bar — Legal Ethics Committee (the
  District's integrated / mandatory bar)
- **Index:** https://www.dcbar.org/for-lawyers/legal-ethics/ethics-opinions-210-present
- **Coverage:** Opinions 210–present (~180 opinions, roughly 1990–present),
  each on its own born-digital HTML page.

## How it works

1. `bootstrap.py` fetches the single public listing page and collects every
   `.../Ethics-Opinion-{N}` detail-page link (a few numbers carry a
   `-(Revised)` variant, kept as a distinct record).
2. For each opinion it fetches the detail page and extracts the full text from
   the `<article class="c-news-detail">` container (heading + paragraphs) with
   BeautifulSoup — **no PDF, no OCR, no login**.
3. `date` is decoded from the `Published: <Month> <Year>` line the newer
   opinions carry (first-of-month ISO); older opinions carry no explicit
   publication date, so `date` is null for them.

Opinions 2–209 (interpreting the pre-1991 D.C. Code of Professional
Responsibility) are published only as a single consolidated 1991-edition PDF
and are intentionally **excluded** — this source is the current per-opinion HTML
series (210–present).

## Usage

```bash
python bootstrap.py test-api             # connectivity + extraction check
python bootstrap.py bootstrap --sample   # ~12 sample records
python bootstrap.py bootstrap            # full pull
```

## Distinct from

`US/DC-Code`, `US/DC-Legislation`, `US/DC-Courts`, `US/DC-TaxGuidance`. This is
the attorney professional-conduct advisory-opinion series (advising lawyers),
the D.C. member of the `US/{ST}-LegalEthics` vein.

## License

[Public Domain / freely published advisory opinions](https://www.dcbar.org/for-lawyers/legal-ethics/ethics-opinions-210-present)
— D.C. Bar ethics opinions are published free to the public on dcbar.org with
no login, paywall or terms prohibiting reuse. The District of Columbia Bar is
the District's **integrated (mandatory)** bar, so the 17 U.S.C. § 105
government-edicts rationale applies fairly directly (as with the other
integrated-bar legal-ethics sources). Treated as effectively public domain —
commercial use OK.
