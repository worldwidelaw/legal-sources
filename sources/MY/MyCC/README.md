# MY/MyCC — Malaysia Competition Commission

Enforcement decisions and media releases from the Malaysia Competition Commission (MyCC).

## Coverage

- **Media Releases** — ~74 enforcement announcements, proposed decisions, tribunal appeals, market reviews
- **Case Decisions** — ~40 case PDFs (press releases and some full decisions)

## Data Source

- **URL:** https://www.mycc.gov.my/
- **Format:** HTML media release pages + PDF case documents
- **Language:** English

## Method

HTML scraping of paginated media release listing at `/media-release?page={N}`.
Individual article pages fetched for full text content.
Case PDFs downloaded from `/case` page.

## License

[Public Domain (Government of Malaysia)](https://www.mycc.gov.my/) — official government enforcement publications.
