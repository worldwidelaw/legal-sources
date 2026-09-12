# LY/DCAF — Libya DCAF Security Sector Legal Database

**Source:** [https://security-legislation.ly/](https://security-legislation.ly/)
**Data types:** legislation

Libyan legal texts published by DCAF (Geneva Centre for Security Sector
Governance): constitutional law, laws, decrees, resolutions, judicial
decisions, bylaws, declarations and international agreements.

## Access

WordPress REST API. The site is bilingual and WordPress stores each language
as its own post on its own endpoint:

| Language | Endpoint | Posts |
|---|---|---|
| Arabic (original) | `/ar/wp-json/wp/v2/latest-laws` | 2,175 |
| English (translation) | `/wp-json/wp/v2/latest-laws` | 2,155 |

Only 786 of the English posts carry a real translation; the remainder hold an
`ONLY AVAILABLE IN ARABIC` placeholder and are dropped. Expect roughly 2,950
records overall.

Taxonomy labels (`text_type`, `status`, `institution`, `index_category`) are
resolved from `text-type-categories`, `status-categories`,
`institution-categories` and `database-index-categories`. WPML gives each term
a **separate id per language** — an Arabic post references `4218` ("قانون")
which does not exist in the English listing (`4221` "Law") — so both listings
are fetched and merged into one lookup. Reading only the English endpoint
leaves every Arabic record's metadata blank.

Incremental updates use the API's `modified_after` filter, i.e. when the text
became available to us rather than the law's own promulgation date (`date`,
which never advances).

## License

[CC BY 4.0](https://creativecommons.org/licenses/by/4.0/) — attribution required.
