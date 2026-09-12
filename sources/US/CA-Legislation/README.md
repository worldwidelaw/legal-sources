# US/CA-Legislation — California Legislative Information (LegInfo FTP/MySQL)

**Source:** [https://leginfo.legislature.ca.gov/](https://leginfo.legislature.ca.gov/)
**Bulk export:** [https://downloads.leginfo.legislature.ca.gov/](https://downloads.leginfo.legislature.ca.gov/)
**Data types:** legislation

Every operative section of the 29 California statutory codes plus the California
Constitution, with full text — 162,429 sections in the 2025 session export.

## Access path

The scraper reads the Legislature's own **PUBINFO bulk export**, the same
database that backs the LegInfo website:

| file | role |
|---|---|
| `pubinfo_{YYYY}.zip` (~1.2 GB) | biennial session archive; the only one carrying the law tables |
| `LAW_SECTION_TBL.dat` | one tab-delimited row per operative section |
| `LAW_SECTION_TBL_{n}.lob` | that section's body, as CAML XML |
| `LAW_TOC_TBL.dat` | division/title/part/chapter headings, for the breadcrumb |

The schema is documented in `pubinfo_Readme.pdf` at the same location.

Two neighbouring archives are named similarly and sized similarly but contain
only `BILL_*` tables: `pubinfo_daily_{Day}.zip` and `pubinfo_{Day}.zip`. Picking
one of those yields zero sections, so `_dump_session()` confirms a candidate by
reading its central directory over HTTP range requests before downloading it.

### Why not the website

The original implementation walked `codes_displaySection.xhtml` once per
section. At ~121K sections and a 2s delay that is **~67 hours of sleep alone**
against a 100h fleet cap, and because LegInfo publishes no modified-date facet,
every refresh paid it again in full just to hash the results (#1506). The dump
costs one download, and it carries ~40K sections the walk never reached —
including the entire Constitution, which has no `codedisplayexpand` TOC.

The walk is still implemented. It runs on `--walk`, and automatically if the
bulk host is unreachable or stops shipping the law tables.

## Refresh

`update` HEADs the session zip and compares `Last-Modified` against
`data/dump_state.json`. Unchanged means no sections have changed and the refresh
is a single request. Changed means the zip is re-read and each section's text is
compared to its recorded hash, so only real amendments are emitted.

The comparator is deliberately the archive's publication time rather than a
section's chaptering date: a section republished without a new chaptering would
be invisible to the latter.

## Record identity

`_id` is `{LAW_CODE}-{SECTION_NUM}` — unchanged from the walk. The dump writes
section numbers with a trailing period (`1624.`) where the website does not, so
the loader strips it; without that the whole existing corpus would be orphaned
and reinserted rather than updated.

## License

[Public domain — 17 U.S.C. § 105](https://www.law.cornell.edu/uscode/text/17/105)

California government works are not subject to copyright. No authentication is
required for either the website or the bulk export.
