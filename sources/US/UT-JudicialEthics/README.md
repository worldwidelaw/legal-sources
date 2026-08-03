# US/UT-JudicialEthics — Utah Judicial Ethics Advisory Committee, Ethics Advisory Opinions

Full text of the ethics advisory opinions of the **Utah Judicial Ethics
Advisory Committee**, the committee established by the Utah Supreme Court that
issues written opinions construing the **Utah Code of Judicial Conduct** in
response to inquiries from judges and judicial officers.

Each opinion is the committee's authoritative written interpretation of the
Code — **doctrine**. Two classes are published (1988–present):

| Caption                       | Class    |
|-------------------------------|----------|
| `Informal Opinion No. YY-NN`  | Informal |
| `Formal Opinion No. YY-NN`    | Formal   |

## Access

`utcourts.gov` is an Adobe Experience Manager (AEM) site — no auth, no CAPTCHA.

- One index page lists every opinion:
  `/en/court-records-publications/publications/judicial-ethics-opinions.html`.
- Each opinion is an `<a>` whose text is the caption and whose `href` is either
  a born-digital **PDF** (newer opinions,
  `/content/dam/.../ethics_opinions/{YYYY}/{num}.pdf`) or an **HTML** opinion
  page (older opinions, `/en/.../ethics-opinions/{YYYY}/{num}.html`).
- The `/content/dam` PDF endpoint returns **406** to a bare User-Agent — a full
  browser UA plus an `Accept` header serves the PDF (the session sets both).

Full text comes from the PDF (`common.pdf_extract`) or, for HTML opinions, the
AEM `main .container` content region (nav/header/footer stripped; bytes decoded
utf-8 then cp1252). Opinions are dedup'd by class + number (PDF preferred when
both exist); the issue date is parsed from the opinion body.

## Usage

```bash
python bootstrap.py bootstrap            # Full pull (all opinions)
python bootstrap.py bootstrap --sample   # ~12 samples
python bootstrap.py test-api             # Connectivity + extraction test
```

## License

[Public Domain — 17 U.S.C. § 105](https://www.law.cornell.edu/uscode/text/17/105) — ethics advisory opinions of the Utah Judicial Ethics Advisory Committee are official public records of the Utah judiciary interpreting the Code of Judicial Conduct (government-edict works), published for public use by the Utah Courts. Commercial use permitted; no attribution required.
