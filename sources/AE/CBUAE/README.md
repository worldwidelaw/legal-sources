# AE/CBUAE — Central Bank of the UAE Rulebook

Regulations, standards, and guidelines issued by the Central Bank of the UAE (CBUAE).

## Coverage

- **Type:** doctrine (regulations, standards, guidelines, laws)
- **Language:** English
- **Documents:** ~192 regulatory documents
- **Categories:** Banking, Insurance, AML/CFT, Consumer Protection, Other Regulated Entities
- **Source:** [CBUAE Rulebook](https://rulebook.centralbank.ae/en)

## How It Works

The scraper:
1. Discovers all regulation page slugs from 4 category index pages
2. Fetches each regulation's full-text HTML page
3. Extracts and cleans the body text (strips HTML, navigation boilerplate)
4. Normalizes into standard schema with full text

The site is a Drupal 10 application with standard HTML pages. No API required.

## Usage

```bash
python bootstrap.py test                  # Connectivity test
python bootstrap.py bootstrap --sample    # 15 sample records
python bootstrap.py bootstrap             # Full bootstrap (~192 docs)
```

## License

[CBUAE Rulebook](https://rulebook.centralbank.ae/en) — Public regulatory information published for compliance purposes. No explicit open data license stated; content is publicly accessible without authentication.
