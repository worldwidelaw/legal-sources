# FI/Vero-Guidance

Finnish Tax Authority detailed guidance from Verohallinto (vero.fi).

## Data Source

- **Portal**: https://www.vero.fi/syventavat-vero-ohjeet/
- **Enumeration**: Sitemap XML (1,790 URLs)
- **Format**: HTML full text from `<article id="content-main">`
- **Auth**: None (public)

## Coverage

~1,790 documents: guidance (~472), advance rulings (~1,134), decisions (~118), statements (~55). Language: Finnish (some English translations available).

## License

Public sector information — reuse permitted under Finnish open data terms. See [vero.fi](https://www.vero.fi).

## Usage

```bash
python bootstrap.py test                # Connectivity test
python bootstrap.py bootstrap --sample  # Fetch 15 samples
python bootstrap.py bootstrap           # Full fetch (~1,790 docs)
```
