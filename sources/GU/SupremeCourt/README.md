# GU/SupremeCourt — Supreme Court of Guam Opinions

**Source:** [https://guamcourts.gov/courts-council/supreme-court/opinions](https://guamcourts.gov/courts-council/supreme-court/opinions)
**Data types:** case_law

Published opinions of the Supreme Court of Guam, **1996–present (795 opinions** as of
August 2026). The court was established in 1996; the year dropdown goes back to 1990
but 1990–1995 return no records.

## Access path

The Judiciary of Guam rebuilt guamcourts.gov on Drupal (issue #1335), retiring the old
`/Supreme-Court-Opinions/Supreme-Court-Opinions.asp` year-POST form. The corpus is now
split in two:

| Scope | Endpoint |
|---|---|
| Current year | `GET /courts-council/supreme-court/opinions` — static Drupal page |
| 2025 and prior | `GET /legacydata/supreme-court-opinions?action=get_items&type=SPRMOP&year=YYYY` |

The legacy page is a jQuery archive: its year `<select>` fires the `get_items` GET above
and swaps in an HTML fragment of `div.item_for_list` entries. No POST, no session, no
tokens — plain GET per year.

Opinions are born-digital PDFs (`/legacydata/files/Supreme-Court-Opinions/images/…` for
the archive, `/sites/default/files/SupremeCourtOpinions/…` for the current year); full
text is extracted with `pdfplumber`.

## Metadata

Citation (`YYYY Guam N`) comes from the listing's metadata line, falling back to the PDF
file name — current-year file names are inconsistent (`2026 Guam 7.pdf`,
`Opinion (2026 Guam 4).pdf`, `2026Guam01.pdf`), so both paths are needed.

`date` and `docket` are read from the opinion's own caption (`Filed: October 29, 1996`,
`Supreme Court Case No. WRM 96-001`) in preference to the listing, because pre-2010
archive entries carry only a bulk "Posted:" date — often years after the decision — and
no docket at all.

IDs are `GU-SC-{year}-{number:02d}`, unchanged from before the site move.

## Usage

```bash
python3 bootstrap.py test-api              # connectivity + per-year counts
python3 bootstrap.py bootstrap --sample    # 15 records strided across 1996-present
python3 bootstrap.py bootstrap --full      # full corpus -> data/records.jsonl
python3 bootstrap.py bootstrap-fast --full # alias used by the fleet wrapper
```

## License

Public domain — edicts of government. Guam is a U.S. territory; its judicial opinions
are not subject to copyright.
[17 U.S.C. § 105](https://www.law.cornell.edu/uscode/text/17/105) —
see the [Judiciary of Guam disclaimer](https://guamcourts.gov/disclaimer).
