# INTL/UNIDROIT — UNIDROIT Instruments

International private law instruments from UNIDROIT (International Institute for the Unification of Private Law), seated in Rome.

## Coverage

- **160 instrument pages** with full text
- Conventions (Cape Town, Geneva Securities, Factoring, Leasing, etc.)
- Model Laws (Franchising, Leasing, Factoring)
- Principles (UNIDROIT Principles 2010 with commentary, ALI/UNIDROIT Principles)
- Guides (Netting, Bank Liquidation, Warehouse Receipts)

## Method

WordPress REST API at `/wp-json/wp/v2/pages`. Full text is embedded in HTML content of WordPress pages. WPBakery shortcodes are stripped during text extraction.

## Usage

```bash
python bootstrap.py test                  # Test API connectivity
python bootstrap.py bootstrap --sample    # Fetch 15 sample records
python bootstrap.py bootstrap             # Full bootstrap (160 records)
```

## License

[UNIDROIT Terms](https://www.unidroit.org/terms-of-use/) — instruments are publicly available for informational/research purposes. Verify terms before commercial redistribution.
