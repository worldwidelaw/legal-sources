# US/CherokeeNationAGOpinions — Cherokee Nation Attorney General Opinions

Formal opinions of the Attorney General of the **Cherokee Nation**, the largest
federally recognized tribe in the United States (~450,000 citizens; a
reservation covering 14 counties of north-eastern Oklahoma, reaffirmed as
Indian country in the wake of *McGirt v. Oklahoma*).

An AG opinion answers a question of Cherokee Nation law put by the Principal
Chief, a Council member or a tribal official, construing the Constitution of
the Cherokee Nation and the Cherokee Nation Code Annotated. Recurring subjects:

- election and absentee-ballot procedure, candidate qualification and withdrawal
- Freedom of Information Act compliance and open-meeting requirements
- Tribal Council term limits and vacancy in the office of Principal Chief
- Gaming Commission jurisdiction, gaming age, licensing of non-gaming vendors
- TERO funds, tribal employment, minimum wage by executive order
- conflicts of interest and the Principal Chief's legal counsel
- guardianship and child-support jurisdiction; ADA service animals

Opinions are numbered `YYYY-CNAG-NN` and run from **2006 to date** (59 published
as of August 2026).

## Why `doctrine` and not `case_law`

An Attorney General opinion interprets the law for the executive branch. It
decides no case, binds no party and does not bind the Cherokee Nation courts —
so it is collected as **doctrine**, alongside tax rulings and official guidance.

Related Cherokee Nation sources:

| Source | Contents |
|---|---|
| `US/CherokeeNationCourts` | Supreme Court and Judicial Appeals Tribunal decisions (case law) |
| `US/CherokeeNationCode` | Cherokee Nation Code Annotated + the Constitutions (legislation) |
| `US/CherokeeNationAGOpinions` | this source — AG opinions (doctrine) |

## Access strategy

The Office of the Attorney General publishes the whole series from one page,
[`attorneygeneral.cherokee.org/opinions/`](https://attorneygeneral.cherokee.org/opinions/),
as an Umbraco "document listing":

```html
<li>
  <a href="/media/qwxnm15u/2025-cnag-02.pdf">2025-CNAG-02</a>
  <div class="document-listing-metadata">248.1 KB -- Created:6/13/2025 | Updated:6/13/2025</div>
  <div>An opinion addressing candidate withdrawal.</div>
</li>
```

The listing paginates at 7 per page, but it honours a `pageSize` parameter, so
`?term=&page=1&pageSize=200` returns the entire series in one request. The
scraper reads the pager back afterwards and **raises** if more than one page is
still offered, so a future server-side cap on `pageSize` surfaces as an error
rather than as a silently truncated corpus. It also raises if the listing
yields fewer than 40 opinions.

The Umbraco media hash in each path (`/media/qwxnm15u/…`) rotates whenever a
file is re-uploaded, so links are read off the listing every run and never
hardcoded.

## Full text and OCR

**Only 6 of the 59 opinions are born-digital.** The rest are scans of the signed
original with no text layer at all. The scraper therefore:

1. tries the PyMuPDF text layer first, and
2. falls back to the shared `common/pdf_extract` cascade
   (opendataloader → pdfplumber → pypdf → **tesseract OCR**) for the scans.

The scans are clean typescript on white and OCR reads them well (5K–36K
characters per opinion). Each record records which path produced it in
`extraction` (`text_layer` / `ocr`).

> **OCR is required.** In an environment with no tesseract binary the scraper
> would only be able to read the ~6 born-digital opinions. Rather than report
> that as a complete corpus, `fetch_all()` raises when every unreadable PDF was
> skipped and OCR never ran once.

## Dates

Each opinion prints its own operative date under the caption:

```
Opinion Number: 2008-CNAG-03

Date Decided: November 19, 2008
```

That is parsed into `date_decided` and used as `date`. The listing's
`Created:` date is only when the OAG uploaded the file — the whole pre-2012
back-catalogue was uploaded in one batch — and is kept separately as
`published_date`.

## Record shape

```json
{
  "_id": "CNAG-2008-cnag-03",
  "_source": "US/CherokeeNationAGOpinions",
  "_type": "doctrine",
  "title": "2008-CNAG-03 — Minimum Gaming Age",
  "text": "OPINION OF THE CHEROKEE NATION ATTORNEY GENERAL …",
  "date": "2008-11-19",
  "citation": "2008-CNAG-03",
  "opinion_number": "2008-CNAG-03",
  "year": 2008,
  "summary": "Minimum Gaming Age",
  "submitted_by": "Jamie Hummingbird, Director of the Cherokee Nation Gaming Commission",
  "date_decided": "2008-11-19",
  "published_date": "2012-01-31",
  "extraction": "ocr",
  "pages": 9,
  "jurisdiction": "US-CHEROKEE-NATION",
  "url": "https://attorneygeneral.cherokee.org/media/li4pgqf2/2008-cnag-03.pdf"
}
```

Citations are normalized to `YYYY-CNAG-NN` regardless of how the OAG wrote them
(`2006-CNAG-1` inside the document, `25-cnag-01` in a filename).

## Usage

```bash
python bootstrap.py bootstrap            # full pull (all opinions)
python bootstrap.py bootstrap --sample   # 12 samples spread over 2006-2025
python bootstrap.py bootstrap-fast       # high-throughput full pull (VPS)
python bootstrap.py test-api             # connectivity test
```

## Coverage limits

- The series starts at **2006**. Opinions issued before then are not published
  online by the OAG.
- The OAG's "Sovereignty Commission Reports" and FOIA/GRA request pages are
  separate document listings on the same site and are **not** in scope here.

## License

[Public Domain — tribal government edict](https://attorneygeneral.cherokee.org/opinions/) —
opinions of the Attorney General of the Cherokee Nation are official acts of a
sovereign tribal government, published in full and without restriction by the
Office of the Attorney General for public access ("They may be viewed on your
screen, printed or even downloaded"). Under the government edicts doctrine the
official legal pronouncements of a government are not subject to copyright.
Freely reusable, including commercially.
