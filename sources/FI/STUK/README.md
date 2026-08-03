# FI/STUK — Radiation and Nuclear Safety Authority YVL / VAL Guides

STUK (Säteilyturvakeskus) is Finland's Radiation and Nuclear Safety Authority.
Under the Nuclear Energy Act it issues the **YVL Guides** (regulatory guides on
nuclear safety, series A–E) and **VAL Guides** (radiation safety), which set the
detailed, binding regulatory requirements for Finnish nuclear facilities and
radiation practices. STUKLEX — STUK's legal database — classifies each guide as
a "Regulation", so they are stored here as `legislation`.

## Source

- **Index pages:** https://stuk.fi/en/yvl-guides and https://stuk.fi/en/val-guides
- **Full text:** each guide's STUKLEX page, `https://www.stuklex.fi/en/ohje/{ID}`
  (e.g. `YVLA-1`, `YVLB-2`, `VAL1`).

## How it works

1. `_load_guide_ids()` fetches the two STUK index pages and extracts every
   `stuklex.fi/en/ohje/{ID}` link (order-preserving, de-duplicated).
2. `_fetch_guide()` fetches each STUKLEX page and extracts the full guide body
   from the server-rendered `document-wrapper` block (up to the page footer),
   stripping HTML tags. The header line `"{Title}, {d.m.yyyy}{ID}"` yields the
   title, issue date (converted to ISO) and guide number.
3. `normalize()` emits the standard record with `_type: legislation` and the
   full guide text in `text`.

## Usage

```bash
python bootstrap.py test                # verify index + one guide page
python bootstrap.py bootstrap --sample  # fetch 15 sample records
python bootstrap.py bootstrap           # full run (~46 YVL + VAL guides)
```

## License

[Finnish Open Government Data](https://www.avoindata.fi/en) — STUKLEX regulatory
guides are public-authority regulatory text published by STUK. Reused with
attribution to STUK. Commercial use permitted.
