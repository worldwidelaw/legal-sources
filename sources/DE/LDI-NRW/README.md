# DE/LDI-NRW — North Rhine-Westphalia Data Protection Authority

**Landesbeauftragte für Datenschutz und Informationsfreiheit Nordrhein-Westfalen**

The independent supervisory authority for data protection and freedom of
information in North Rhine-Westphalia — Germany's most populous state (~18M
people) and the seat of a large share of the country's corporate GDPR
casework. Added from issue #1362 (credit @Wildvirus).

Site: <https://www.ldi.nrw.de/>

## What this source covers

`doctrine`. Two kinds of document:

| Kind | Count | Source |
|---|---|---|
| `report` | 31 | Annual *Tätigkeitsberichte* / *Datenschutzberichte* linked from [`/berichte`](https://www.ldi.nrw.de/berichte) |
| `guidance` | ~90 | Topic pages under `/datenschutz`, `/informationsfreiheit`, `/infothek` |

The authority does **not** publish individual *Bescheide*, so the annual
reports are the enforcement record: each narrates that year's complaints,
orders, fines and the authority's legal reasoning, running 190K–520K
characters. The guidance pages carry its interpretation of the GDPR, the
DSG NRW and the IFG NRW.

This is additive to `DE/NRW`, which covers state *legislation* only.

## How it works

The site is Drupal but exposes no JSON:API (`/jsonapi` → 404) and its
`sitemap.xml` lists only the homepage, so discovery is a crawl:

- **Reports** — every same-host `.pdf` link on `/berichte`, extracted with
  `common/pdf_extract` (born-digital, no OCR needed).
- **Guidance** — breadth-first from a seed list, keeping only pages under the
  four content prefixes whose `<article>` body exceeds 400 characters.
  Imprint, accessibility statement, contact forms and job ads are excluded.

Reports are yielded before guidance so a truncated run still lands the most
valuable documents. All requests use explicit connect/read timeouts.

### Dates

Newer report filenames carry the year (`31_bericht_2026.pdf`); older ones do
not (`30.-bericht.pdf`). The reports are numbered consecutively and the Nth was
published in year `N + 1995` — verified against every dated filename in the
series — so the ordinal in the title fills the gaps. Guidance pages are
undated (`date: null`); the site publishes no modification date for them.

## Usage

```bash
python bootstrap.py test               # Connectivity: reports found + one guidance page
python bootstrap.py bootstrap --sample # 15 sample records
python bootstrap.py bootstrap          # Full corpus
python bootstrap.py bootstrap-fast     # Same as bootstrap (fleet wrapper entry point)
```

`fetch_updates` re-runs the full crawl: the corpus is small and the site offers
no change feed, so a re-crawl is cheaper than tracking one. The loader upserts
on `_id`.

## License

[Datenlizenz Deutschland – Namensnennung 2.0](https://www.govdata.de/dl-de/by-2-0)
— attribution to LDI NRW required; commercial use permitted. Publications of a
German public-sector body; report PDFs carry no separate restriction.
