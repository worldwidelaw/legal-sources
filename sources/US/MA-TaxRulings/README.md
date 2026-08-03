# US/MA-TaxRulings — Massachusetts DOR Letter Rulings

Full-text **Letter Rulings** issued by the **Massachusetts Department of Revenue
(DOR)**. A Letter Ruling is the Commissioner's written statement, in response to a
specific taxpayer's request, applying the Massachusetts tax statutes and
regulations to that taxpayer's stated facts. It is official DOR guidance on the
meaning of Massachusetts tax law and is classified as **doctrine**.

## Source

- Index: https://www.mass.gov/lists/dor-letter-rulings (~995 rulings)
- Each ruling: `https://www.mass.gov/letter-ruling/{slug}` (full text in the page)

## Method

1. `GET /lists/dor-letter-rulings` and parse every `/letter-ruling/{slug}` anchor
   (ruling number + title).
2. `GET` each ruling page and extract the `Date:` field (MM/DD/YYYY) and the body
   text from the `ma__rich-text` blocks (balanced-div matching, tags/entities
   stripped). No OCR — the text is native HTML.

## ⚠️ Akamai User-Agent note

`www.mass.gov` sits behind **Akamai Bot Manager**, which returns **HTTP 403** to
requests whose User-Agent *claims to be a browser* (`Mozilla/...`) but then fails
its JS/TLS fingerprint challenge. A **plain, honest non-browser UA**
(`python-requests`/`curl` token) passes with **200** for both the list and the
individual ruling pages. The scraper's `self._ua` is set accordingly — **do not**
change it to a Mozilla string. (This is the same finding that unblocked
`US/MA-DALA`.)

## Usage

```bash
python bootstrap.py test-api            # Connectivity + extraction check
python bootstrap.py bootstrap --sample  # ~12 sample records
python bootstrap.py bootstrap           # Full pull
```

## License

[Public Domain (State of Massachusetts Government Work)](https://www.law.cornell.edu/uscode/text/17/105) —
Letter Rulings of the Massachusetts Department of Revenue are official public
records of the Commonwealth of Massachusetts, published for public use with no
copyright restriction. Commercial use permitted.
