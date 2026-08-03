# US/OK-AGOpinions — Oklahoma Attorney General Opinions

Full text of the official written **Opinions of the Attorney General of Oklahoma**.

Under **74 O.S. § 18b**, the Attorney General renders written opinions upon
questions of law submitted by the Legislature, its members, and the officers of
the executive and administrative departments of Oklahoma state government. Such
opinions are **binding** on the officers to whom they are directed until
superseded by a court or by the Attorney General, and are published and cited
`___ OK AG ___` (opinion numbers run `YY-NNN` / `YYYY-NNN`). As official
determinations of specific submitted questions of law they are captured here as
**case_law**, consistent with the other `US/*-AGOpinions` sources.

Distinct from **US/OK-LegalEthics** (Oklahoma Bar Association attorney-ethics
advisory opinions), **US/OK-Courts**, **US/OK-Legislation** and
**US/OK-TaxDecisions**.

## Source

- **Host:** Oklahoma Public Legal Research System (OPLRS), operated by the
  University of Oklahoma Law School — <https://oklegal.onenet.net/agopinions.basic.html>
- **Engine:** CNIDR Isearch-cgi (1.47j) full-text search over the `okag`
  database (OK Attorney General Opinions, 1948–current).
- **No** JavaScript, CAPTCHA or authentication.

## How it works

1. **Search (POST)** `/oklegal-cgi/isearch` with
   `SEARCH_TYPE=SIMPLE`, `DATABASE=okag`, `FIELD_1=HEARING_DATE`,
   `TERM_1=<year>`, `ELEMENT_SET=F` (return **full text inline**),
   `MAXHITS=100`, `START=<offset>`.
2. Each result block carries the opinion's fields inline: `Filename`,
   `ENTRY_DATE`, `APPELLANT` (requesting official), `JURISDICTION`,
   `HEARING_DATE` (issue date), `TEXT_OF_RULE` (**full text**),
   `CITATIONS` (`03-018 (2003) ag` → opinion number + year).
3. **Enumeration:** sweep `HEARING_DATE` year by year, 1948 → current. Years
   with more than 100 opinions are paged via `START`. Records are deduped on
   opinion number (or, when absent, a text hash) — the `okag` database stores
   some opinions twice under different internal filenames.
4. **Full text:** `TEXT_OF_RULE` is HTML (`<p>` paragraphs, `<a>` statute
   links); `BeautifulSoup.get_text` keeps the statute-citation link text and
   strips tags. **No PDF, no OCR.**

### Data-quality note

The `okag` database is contaminated with **Court of Criminal Appeals of
Oklahoma** decisions (particularly in recent years). The scraper keeps only
records whose `JURISDICTION` field contains "Attorney General", so only genuine
AG opinions are ingested.

The per-document `ifetch` CGI is server-side broken for `okag` ("Database okag
does not exist or is corrupted"); `ELEMENT_SET=F` returns the full text inline,
so `ifetch` is not needed. The OSCN mirror (`oscn.net`, `ftdb=STOKAG`)
datacenter-IP-blocks non-Oklahoma vantages; OPLRS is the reachable route.

## Usage

```bash
python bootstrap.py bootstrap            # Full pull (all years)
python bootstrap.py bootstrap --sample   # ~12 samples
python bootstrap.py bootstrap-fast       # Alias for full pull (VPS wrapper)
python bootstrap.py test-api             # Connectivity + extraction test
```

## License

[Public Domain — 17 U.S.C. § 105](https://www.law.cornell.edu/uscode/text/17/105) — Official Opinions of the Attorney General of Oklahoma are edicts of government produced by state officials in the course of official duty, and are in the public domain under the government-edicts rationale. Published free to the public by the Oklahoma Public Legal Research System (University of Oklahoma Law School) with no login, paywall or terms prohibiting reuse. **Commercial use permitted.**
