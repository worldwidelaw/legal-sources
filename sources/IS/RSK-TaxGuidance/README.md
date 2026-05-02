# IS/RSK-TaxGuidance — Skatturinn Binding Tax Opinions (Bindandi Álit)

Fetches binding tax opinions from the Icelandic Director of Internal Revenue
(Ríkisskattstjóri / Skatturinn).

- **Source**: https://www.skatturinn.is/fagadilar/bindandi-alit/
- **Coverage**: 1999–present (~100-150 opinions)
- **Language**: Icelandic
- **Type**: doctrine (binding tax interpretations)
- **Full text**: Yes — complete opinions with facts, legal reasoning, and conclusions

## How it works

1. Scrapes the main listing page to discover all opinion links
2. Fetches each opinion's full HTML page
3. Parses the `.article` container for title, date, subject, and full text

## License

[Public Domain](https://www.government.is/publications/legislation/) — Official government guidance from the Icelandic tax authority.
