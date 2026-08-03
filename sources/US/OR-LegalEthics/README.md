# US/OR-LegalEthics — Oregon State Bar — Formal Ethics Opinions

Full text of the **Formal Ethics Opinions** issued by the **Oregon State Bar**'s
Legal Ethics Committee and adopted by the Board of Governors. Each opinion
applies the Oregon Rules of Professional Conduct (ORPC) to a stated fact
situation and advises lawyers whether the described conduct is proper — this is
the Bar's official written interpretation of the attorney-conduct rules =
**doctrine**.

The corpus is one continuous, globally-numbered series. In September 2005 the
Board of Governors re-issued the older 1991–2004 formal opinions as `2005-{N}`
(2005-1 … 2005-175), conformed to the ORPC that took effect 1 Jan 2005; opinions
issued since then continue the same sequential number with the issue year as the
prefix (e.g. `2011-188`, `2013-189`, … `2026-208`). **~208 opinions** as of 2026.

## Access & recipe

1. The **Formal Ethics Opinion Library – Table of Contents**
   (`https://www.osbar.org/ethics/toc.html`) lists **every** opinion as a direct
   PDF link whose anchor text is `{number}: {title}`. The scraper regex-extracts
   and de-duplicates all `/_docs/ethics/{YYYY-NNN}.pdf` hrefs and takes the
   title from the anchor text after the colon.
2. Each opinion PDF is **born-digital** (text layer) — extracted with PyMuPDF,
   **no OCR**. The body carries `FORMAL OPINION NO {number}`, the topical title,
   and Facts / Discussion / Conclusion sections. A handful of superseded
   opinions are short official stubs (`This opinion has been superseded and
   replaced by OSB Formal Ethics Opinion No …`).

The date is taken from the `YYYY Revision` line in the body when present, else
the number's year prefix → `YYYY-01-01`. No CAPTCHA or authentication; a 1 req/s
browser-UA `requests` session is used.

```bash
python bootstrap.py test-api            # connectivity + extraction check
python bootstrap.py bootstrap --sample  # ~12 sample records
python bootstrap.py bootstrap           # full pull (all opinions)
python bootstrap.py bootstrap-fast      # alias for the full pull (VPS wrapper)
```

## Output schema

`_id`, `_source` (`US/OR-LegalEthics`), `_type` (`doctrine`), `_fetched_at`,
`opinion_number`, `issuer`, `title`, `text` (full opinion body), `date`
(ISO 8601), `url`, `jurisdiction` (`US-OR`).

## Distinct from

- **US/OR-EthicsOpinions** — Oregon Government Ethics Commission (public
  officials), not lawyers.
- **US/OR-AGOpinions** — Oregon Attorney General opinions.

This is the state **bar**'s attorney-ethics formal-opinion series.

## License

[Public Domain (Oregon State Bar Formal Ethics Opinions)](https://www.osbar.org/ethics/ethicsops.html)
— Oregon State Bar Formal Ethics Opinions are published free to the public on
osbar.org as an educational service interpreting the Oregon Rules of
Professional Conduct. They are advisory (no weight of law) and carry no
copyright restriction or paywall on the opinion text. No login or terms
prohibiting reuse. Commercial use permitted.
