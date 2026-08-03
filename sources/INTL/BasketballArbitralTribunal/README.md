# INTL/BasketballArbitralTribunal — FIBA Basketball Arbitral Tribunal (BAT) Awards

The **Basketball Arbitral Tribunal (BAT)** is FIBA's independent sports-arbitration
body that resolves financial and contractual disputes in international basketball —
typically players, coaches, and agents against clubs (unpaid salaries, bonuses,
agent fees, termination disputes). Awards are non-confidential under the BAT Rules
unless the arbitrator orders otherwise, and are published as born-digital PDFs.

## Data

- **Type:** case_law (arbitral awards)
- **Language:** English
- **Coverage:** Case numbers reach BAT 2037/23; the tribunal has operated since 2007.
  Estimated full corpus ~1,000–1,500 awards.
- **Full text:** Yes — born-digital award PDFs are downloaded and text-extracted.

## Access

- Listing: `https://about.fiba.basketball/en/services/basketball-arbitral-tribunal/bat-awards`
  (Next.js page; server-renders the most-recent batch of award cards with case
  number, publication date, parties, summary, and a direct `<a download>` PDF link).
- Award PDFs are hosted on the open Cloudinary host `assets.fiba.basketball`
  (HTTP 200, no login, no WAF/Cloudflare).

### Pagination limitation

The server-rendered page exposes the most-recent ~10 awards. The full historical
corpus is paginated client-side via the Contentful gateway at
`https://digital-api.fiba.basketball/hapi/getcustomgateway` (subscription key is
embedded client-side). Extending `fetch_all` to that gateway is a documented
follow-up; the static listing already provides full-text awards for the recent set.

## Usage

```bash
python bootstrap.py test               # Print parsed listing entries
python bootstrap.py bootstrap --sample # Fetch sample records with full text
python bootstrap.py bootstrap          # Full pull (recent awards)
```

## License

> ⚠️ **Commercial use restricted.** FIBA asserts copyright over its content.

[FIBA Terms & Conditions](https://about.fiba.basketball/en/footer/terms-conditions)
— Awards are openly published (non-confidential per the BAT Rules) but FIBA states
"© Copyright FIBA. All rights reserved." Treat as `custom-terms`, commercial use
restricted (`commercial_use: false`). Attribution to FIBA / BAT required.
