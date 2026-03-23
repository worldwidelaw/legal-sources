# DE/BRAK — German Federal Bar Association

Legal news and case law commentary from the Bundesrechtsanwaltskammer (BRAK).

## Data Access

- **Method**: Paginated HTML listing + article page scraping
- **Listing**: `https://www.brak.de/newsroom/news/` (111 pages, ~13 articles/page)
- **Content**: Full text from individual HTML article pages
- **Rate limit**: 1.5s between requests
- **Estimated total**: ~1,440 articles

## Content

Articles cover case law commentary, professional ethics, regulatory updates,
and bar association news. Tags/categories are extracted when available.

## Usage

```bash
# Fetch sample (15 records)
python3 bootstrap.py bootstrap --sample

# Fetch all records
python3 bootstrap.py bootstrap --full
```

## License

Public domain official works under German law (§ 5 UrhG).
