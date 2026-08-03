# CA/ON-OSC — Ontario Securities Commission: Instruments, Rules & Policies

Full text of Ontario securities-law **instruments, rules, companion policies and
staff notices** published by the [Ontario Securities Commission
(OSC)](https://www.osc.ca/en/securities-law/instruments-rules-policies).

These are the national/multilateral and Ontario-local instruments that govern
Ontario's capital markets, grouped by the Canadian Securities Administrators
(CSA) numbering system (subject categories 0–9). Each instrument page lists its
current consolidation plus the historical amendments, notices and policies; the
full text is extracted from the linked PDFs.

- **Country / jurisdiction:** Canada — Ontario (`CA-ON`)
- **Data type:** legislation
- **Language:** English
- **Auth:** none

## Access pattern

The OSC site is a Drupal CMS. The scraper walks three levels:

1. Index → category pages `/en/securities-law/instruments-rules-policies/{N}` (N = 0–9)
2. Category → instrument pages `.../{N}/{number}` (e.g. `.../1/13-101`)
3. Instrument → leaf document nodes `.../{N}/{number}/{slug}`

Each leaf node renders a title (`h1.hero__title`), an effective date
(`<time datetime=…>`) and one or more PDF downloads under
`/sites/default/files/pdfs/…`. The PDFs are text-based (not scanned), so text is
extracted directly. One leaf document = one record.

There is no modified-since index, so `update` re-scans all categories and gates
on the leaf date.

## Usage

```bash
python bootstrap.py test                 # connectivity + one extraction
python bootstrap.py bootstrap --sample   # ~12 sample records
python bootstrap.py bootstrap            # full pull
python bootstrap.py bootstrap-fast       # concurrent full-text downloads
python bootstrap.py update               # incremental re-scan
```

## License

> ⚠️ **Commercial use restricted.** The OSC website Terms of Use grant only a
> non-commercial limited licence. See terms below.

[OSC Website Terms of Use](https://www.osc.ca/en/legal) — the OSC grants a
limited, non-exclusive licence to view, print and download Site Content "solely
for non-commercial, educational or informational purposes". Adapting, modifying,
copying, publishing or distributing the content beyond that requires the OSC's
prior written permission. Attribution to the OSC is expected.

The underlying securities instruments are public law, but reproduction of the
osc.ca content is governed by these terms — flagged as non-commercial
accordingly.

## Notes

OSC adjudicative/enforcement decisions are **not** in this source. Ontario's
securities tribunal function moved to the **Capital Markets Tribunal**
(capitalmarketstribunal.ca); its decisions would be a separate `case_law`
source.
