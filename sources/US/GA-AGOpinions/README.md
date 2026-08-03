# US/GA-AGOpinions — Georgia Attorney General Legal Opinions

Full text of official and unofficial legal opinions issued by the Georgia
Department of Law (Office of the Attorney General). Official opinions bind state
agencies; unofficial opinions are advisory. Each answers a legal question posed
by a public official and is an authoritative interpretation of Georgia law —
classified as **doctrine**.

## Source

- **Index:** https://law.georgia.gov/opinions (paginated HTML listings)
- **Documents:** plain HTML pages at `law.georgia.gov/opinions/{YYYY-N}`
- **Coverage:** ~2000-present
- **Auth:** none

## How it works

1. Walk the paginated listings
   (`/opinions/official?page=N` and `/opinions/unofficial?page=N`), extracting
   the `/opinions/{YYYY-N}` opinion links, stopping after two consecutive empty
   pages per listing.
2. Fetch each opinion page and capture the document text from the
   `record-header` (date / *To* recipient / *Re* syllabus) through the `</main>`
   body. Tags are stripped and HTML entities decoded — the pages carry real
   text, so no PDF/OCR is needed.
3. Normalize into the standard doctrine schema (`text` holds the full opinion;
   `syllabus` carries the one-line *Re* summary).

> Note: `law.georgia.gov` requires TLS 1.3, which the system OpenSSL/LibreSSL on
> some hosts does not negotiate. The bootstrap falls back to a `curl` subprocess
> for those requests.

## Usage

```bash
python bootstrap.py test-api            # connectivity + extraction check
python bootstrap.py bootstrap --sample  # ~12 sample documents
python bootstrap.py bootstrap           # full pull (all opinions)
```

## License

[Public Domain (US Government Work — Georgia)](https://www.law.cornell.edu/uscode/text/17/105) — Georgia Attorney General opinions are official state government works in the public domain. Commercial use permitted; no attribution required.
