# US/DE-LegalEthics — Delaware State Bar Association, Committee on Professional Ethics

Legal ethics opinions issued by the **Delaware State Bar Association (DSBA)
Committee on Professional Ethics**. Each opinion answers, on the basis of an
actual member inquiry, how the **Delaware Lawyers' Rules of Professional
Conduct** apply to contemplated attorney conduct. Recommended citation e.g.
*DSBA Comm. on Prof'l Ethics, Op. 2009-1*.

- **Type:** doctrine (advisory opinions interpreting the Rules of Professional Conduct for lawyers)
- **Coverage:** ~55 born-digital opinions, 1989–present (older 1979–1988 opinions are scanned images with no text layer and are skipped — no OCR available in this environment)
- **Jurisdiction:** US-DE

## Source

- **Index:** https://www.dsba.org/publications/ethics-opinions-index/ — a single
  public page linking every opinion PDF; the anchor text is the opinion number
  (e.g. `2009-1`) and the table row carries a one-line "Rules Discussed" summary.
- **Documents:** born-digital PDFs on `media.dsba.org` / `media1.dsba.org`
  (`http`/CloudFront redirects followed), extracted with PyMuPDF (fitz), no OCR.

## Method

1. Fetch the index page, collect every ethics-opinion PDF anchor.
2. Parse the opinion number from the filename (`1989-1.pdf` → `1989-1`,
   `2009-01.pdf` → `2009-1`, `1987.pdf` → `1987-1`; a filed dissent gets a
   `-dissent` suffix).
3. Download each PDF and extract the full text; skip records under 200 chars
   (the scanned 1979–1988 opinions).
4. Parse the decision date from the opinion body (`Month DD, YYYY`), falling
   back to `YYYY-01-01`.

## Distinct from

- **US/DE-EthicsOpinions** — Delaware Public Integrity Commission advisory
  opinions to state **officials/employees** under the state Code of Conduct.
  This source is the **lawyer** professional-conduct series (the DE member of
  the `US/{ST}-LegalEthics` vein).

## License

Public Domain / freely published advisory opinions — [DSBA Ethics Opinions Index](https://www.dsba.org/publications/ethics-opinions-index/).

The DSBA legal ethics opinions are published free to the public on dsba.org /
media.dsba.org, indexed on an open page, with no login, paywall, or terms
prohibiting reuse. The DSBA is a **voluntary** (non-integrated) bar, so the
17 U.S.C. § 105 government-edicts rationale is weaker than in integrated-bar
states; however the opinions are authoritative interpretations of the Delaware
Lawyers' Rules of Professional Conduct (adopted by the Delaware Supreme Court),
published free for citation, and are treated as effectively public domain —
consistent with the other voluntary-bar legal-ethics sources
(US/NY, US/IL, US/CT, US/VT). Commercial use OK (caveated pd-us).
