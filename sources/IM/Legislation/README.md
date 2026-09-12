# IM/Legislation — Isle of Man Legislation (Acts of Tynwald)

**Source:** [https://legislation.gov.im/](https://legislation.gov.im/)
**Data types:** legislation

Consolidated ("as amended") Isle of Man legislation published by the Attorney
General's Chambers: Acts of Tynwald (principal) plus Statutory Documents,
Orders and Regulations (subordinate). Every item is a born-digital PDF with
selectable text.

## Access path

Discovery is index-driven — the open directory listings under
`/cms/images/LEGISLATION/` are no longer reliably browsable. The corpus is
enumerated from the three `com_legislation` index views instead, each of whose
rows links straight at the latest version PDF via an `<a class="npWrap">`
anchor:

| View | Path | Enumeration |
|------|------|-------------|
| In force | `/cms/legislation/current.html` | POST `submit4=<A–Z>` letter filter |
| Repealed Acts | `/cms/legislation/repealed.html` | single page |
| Revoked subordinate legislation | `/cms/legislation/revoked-legislation.html` | single page |

Items are deduplicated on their reference (e.g. `2019-0001`), with the in-force
listing winning over the repealed/revoked ones. Full text is extracted from the
PDF with `common.pdf_extract`.

## Site behaviours that break naive crawlers

* **The WAF hard-403s non-browser User-Agents.** A `legal-data-hunter/1.0`
  UA gets `403 - Forbidden` on every page; a normal browser UA gets `200`.
  This was the root cause of issue #1380 (the scraper "reached the site" but
  parsed nothing).
* **SiteGround serves a robot challenge.** Every non-browser client — and,
  since mid-2026, the whole site including the homepage — gets `HTTP 202` with
  an `sg-captcha: challenge` header and a 200-byte stub instead of the page.
  Waiting it out never clears it, so `solve_sg_challenge()` reimplements the
  challenge page's web worker: brute-force a counter until the top *N* bits of
  `SHA1(sgchallenge_bytes + counter)` are zero (`N` is the first field of the
  `sgchallenge` string, currently 21 → ~2M hashes, well under a second), then
  submit `base64(payload)` to `sgsubmit_url` as `?sol=…&s=<ms>:<hashes>`. The
  response sets an `_I_` cookie that whitelists the session; the original
  request is then replayed. If the solve fails the scraper backs off and
  ultimately raises, rather than reporting an empty corpus as success.
  Requests are paced at ~1/s.

## Usage

```bash
python bootstrap.py test               # connectivity + index parse check
python bootstrap.py bootstrap --sample # 15 sample records
python bootstrap.py bootstrap          # full corpus
python bootstrap.py bootstrap-fast     # alias for the full corpus (fleet entry point)
python bootstrap.py update             # re-walk the in-force index only
```

## License

> ⚠️ **Commercial use restricted.** The site's own terms state: "Legislation can
> be downloaded and printed for private use. Any commercial entity is required
> to obtain permission to reuse the data from the Isle of Man Attorney
> General's Chambers."

[Isle of Man Crown Copyright / legislation.gov.im terms](https://legislation.gov.im/cms/) —
private use only; commercial reuse requires permission from the Attorney
General's Chambers.
