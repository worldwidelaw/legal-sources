# NG/FCCPC — Nigeria Federal Competition and Consumer Protection Commission

Official publications from Nigeria's competition and consumer protection authority.

## Coverage

- **Type:** doctrine (press releases, enforcement actions, rulings, policy announcements)
- **Language:** English
- **Documents:** ~221 posts
- **Categories:** Releases, Alerts/Announcements, News & Events, Speeches, Tips
- **Source:** [FCCPC](https://fccpc.gov.ng)

## How It Works

Uses the WordPress REST API (`wp-json/wp/v2/posts`) to fetch all publications with full HTML content. Content is cleaned of HTML tags and normalized.

## Usage

```bash
python bootstrap.py test                  # Connectivity test
python bootstrap.py bootstrap --sample    # 15 sample records
python bootstrap.py bootstrap             # Full bootstrap (~221 posts)
```

## License

[FCCPC](https://fccpc.gov.ng) — Official Nigerian government publications. No explicit open data license stated; content is publicly accessible.
