# SE/ARN — Allmänna reklamationsnämnden, vägledande beslut

Full text of the guiding decisions (*vägledande beslut*) of the Swedish National Board for Consumer Complaints, published at [arn.se/om-arn/vagledande-beslut](https://www.arn.se/om-arn/vagledande-beslut/).

ARN is Sweden's state alternative-dispute-resolution body for consumer disputes. It hears roughly 10,000 cases a year and issues recommendations that traders follow in the large majority of cases. Only the guiding decisions — the *referat* the Board itself selects as precedent — are published, so this **is** the full public corpus, not a sample of it.

**Coverage:** 137 referat, decided 2018–2026, across the Board's subject divisions (Allmänna 35, Resor 31, Bank 29, Motor 14, Bostad 6, plus Textil, El, Försäkring, Elektronik, Möbler, Sko and the enlarged-panel decisions)
**Language:** Swedish
**Data type:** case_law
**Text:** 2.5K–24K chars per referat (median 8.5K)

## Access

One server-rendered index page — no API, no pagination, no JavaScript, no auth:

```
GET https://www.arn.se/om-arn/vagledande-beslut/
```

The page is a sequence of blocks giving the subject division, the decision date and an editorial headnote alongside a link to the born-digital PDF of the full referat:

```html
<h3>{Ärendeområde}, beslut {YYYY-MM-DD}</h3>
<p>… editorial summary …</p>
<p><a href="/globalassets/extern/pdfer/referat-{YYYY}/…pdf">Referat {caseno}</a></p>
```

## Usage

```bash
python bootstrap.py test-api             # Connectivity test
python bootstrap.py bootstrap --sample   # ~15 sample referat
python bootstrap.py bootstrap            # Full pull (137 referat)
python bootstrap.py bootstrap-fast       # Full pull (VPS)
```

## Gotchas

- **The folder year in the PDF path is the year of publication, not of the decision** — `referat-2019/` holds 2018 case numbers and `referat-2018/` holds 2017 ones. The date comes from the index heading, or from the PDF's own `Beslut YYYY-MM-DD; {caseno}` line, never from the URL.
- Two referat cover a pair of jointly decided cases and carry both case numbers in the filename (`referat-2017-07814-referat-2017-13660.pdf`). Both are kept in `case_numbers`; the first anchors `_id`.
- Referat published from ~2019 on carry a page footer stamped upside-down, which the PDF extractor reads back-to-front (`24770-6202 30-70-6202` for case 2026-07742 decided 2026-07-03). Those lines are stripped from the text, and the reversed date serves as a date fallback.
- The index carries a few section headings that are not decisions; blocks without a referat link are ignored, and any referat found outside a block is logged and still collected.

## License

[Public Domain (Government) — 9 § URL](https://www.riksdagen.se/sv/dokument-och-lagar/dokument/svensk-forfattningssamling/lag-1960729-om-upphovsratt-till-litterara-och_sfs-1960-729/) — decisions of a Swedish public authority (*beslut av myndighet*) are excluded from copyright by 9 § of the Swedish Copyright Act (lag 1960:729). Commercial use permitted, no attribution required.
