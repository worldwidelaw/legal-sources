# US/TX-AGOpinions — Texas Attorney General Opinions

Full text of written opinions issued by the **Texas Attorney General**
under Tex. Gov't Code ch. 402. Each opinion is the AG's authoritative
interpretation of Texas law issued at the request of an authorized public
official — official state legal interpretation (**doctrine**).

The corpus spans the lettered series by AG: **KP-** (Paxton, 2015–present),
**GA-** (Abbott, 2002–15), **JC-** (Cornyn, 1999–2002), **DM-** (Morales,
1991–99), and the older letter series back to **O-0001** (Gerald Mann, 1939).

## Source

- Landing page: <https://www.texasattorneygeneral.gov/opinions> (Drupal)
- One page per Attorney General: `/opinions/{ag-slug}`
- Each AG page embeds every opinion as options of per-year
  `<select name="opinion_selectYYYY">` dropdowns → `(year, /node/{id}, number)`
- Detail page `/node/{id}` carries the born-digital PDF link and a Summary
- PDF path: `/sites/default/files/opinion-files/opinion/{year}/{file}.pdf`
  (filename format varies per series, so it is read from the detail page,
  not guessed)

## Method

1. Discover AG slugs from `/opinions`.
2. For each AG (newest term first), parse the year `<select>` dropdowns to
   enumerate every opinion.
3. Fetch each opinion's `/node/{id}` detail page, read the PDF href and
   Summary abstract.
4. Download the born-digital PDF and extract full text with PyMuPDF
   (`fitz`). Older scans were pre-OCR'd by the AG office. Parse the issue
   date and the `Re:` subject line from the body.

Rate limited to ~1 request/second.

## Fields

`opinion_number`, `title`, `summary`, `text` (full PDF text), `date`,
`url`, `pdf_url`, `node_id`.

## License

[Public Domain (US Government Work — Texas)](https://www.law.cornell.edu/uscode/text/17/105) — Texas Attorney General opinions are official state government edicts in the public domain. Commercial use permitted; no attribution required.
