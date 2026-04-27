# DE/KEK — German Media Concentration Commission

Media concentration decisions from the KEK (Kommission zur Ermittlung der Konzentration im Medienbereich).

## Data Access

- **Method**: HTML listing + AJAX loadmore pagination
- **Listing**: `https://www.kek-online.de/presse/pressemitteilungen/`
- **Content**: Full text from individual HTML article pages
- **Rate limit**: 1.5s between requests
- **Estimated total**: ~62 articles

## Usage

```bash
python3 bootstrap.py bootstrap --sample
python3 bootstrap.py bootstrap --full
```

## License

Public domain under German law — [§ 5 UrhG](https://www.gesetze-im-internet.de/urhg/__5.html) (official works / amtliche Werke).
