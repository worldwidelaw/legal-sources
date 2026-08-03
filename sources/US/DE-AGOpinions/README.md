# US/DE-AGOpinions — Delaware Attorney General Opinions

Full text of opinions issued by the **Delaware Attorney General**.

The bulk of the corpus is **FOIA opinion letters** (numbered `YY-IBNN`)
responding to citizen petitions under Delaware's Freedom of Information
Act, 29 *Del. C.* ch. 100; the office also issues formal legal opinions.
Each opinion is an authoritative interpretation of Delaware law
(**doctrine**).

## Source

- **Publisher:** Delaware Department of Justice / Office of the Attorney General
- **Index:** https://attorneygeneral.delaware.gov/opinions/
- **Access:** Open, no authentication. Each opinion is a full-text
  WordPress post (`/YYYY/MM/DD/<slug>/`); a print-version PDF is also linked.

## Strategy

1. Walk the paginated `/opinions/` listing (`/opinions/page/N/`,
   ~10 opinions/page, ~87 pages), collecting post URLs.
2. Fetch each post and extract the opinion body — the HTML between the
   addthis "above-post" and "below-post" tool markers that bracket the
   WordPress post content.
3. Derive the opinion number and date from the post slug/URL.
4. Normalize into the standard `doctrine` schema (full text in `text`).

## Usage

```bash
python bootstrap.py test-api             # Connectivity + extraction test
python bootstrap.py bootstrap --sample   # ~12 sample documents
python bootstrap.py bootstrap            # Full pull (all opinions)
python bootstrap.py bootstrap-fast       # Alias for full pull (VPS wrapper)
```

## Output

Each record contains `_id`, `_source`, `_type` (`doctrine`),
`opinion_number`, `opinion_kind`, `title`, `text` (full opinion body),
`date`, `url`, and `pdf_url`. Sample records run ~4K–14K characters of
clean text.

## License

[Public Domain — U.S. state government work](https://attorneygeneral.delaware.gov/) — opinions of the Delaware Attorney General are official state government works, published openly with no access restriction. Commercial use permitted; no attribution required.
