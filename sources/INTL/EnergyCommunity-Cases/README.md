# INTL/EnergyCommunity-Cases — Energy Community Dispute Settlement Cases

Non-compliance ("dispute settlement") cases brought under **Article 91 of the Energy
Community Treaty** by the Energy Community Secretariat against Contracting Parties.

The Energy Community extends the EU energy, environment, climate and competition
*acquis* to the Western Balkans, Ukraine, Moldova and Georgia. Where a Contracting Party
fails to implement that *acquis*, the Secretariat opens a case that proceeds through
**Opening Letter → Reasoned Opinion → Ministerial Council Decision**. These cases are the
closest analogue to EU infringement proceedings for the Energy Community's ten
Contracting Parties, and there is no other public full-text collection of them.

## Data

- **Type:** `case_law`
- **Coverage:** ~228 Article 91 cases, 2008–present
- **Full text:** yes — the per-case narrative plus the text extracted from the linked
  Ministerial Council Decision / Reasoned Opinion PDFs
- **Sample:** 15 records, 266–188,264 chars (avg ~26K)
- **Countries:** AL, BA, GE, MD, ME, MK, RS, UA, XK (Kosovo\*)

## Access

Plain HTTPS, no login and no API key.

- Registry index: `/enc-lex/cases/registry.html` links every per-case detail page
- Case pages: `/enc-lex/cases/registry/{YYYY}/case{NN}{YY}{CC}.html` (`CC` = country code)
- Decision PDFs: `/dam/jcr:.../*.pdf`

```bash
python bootstrap.py test               # Print discovered case entries
python bootstrap.py bootstrap --sample # Save sample records
python bootstrap.py bootstrap          # Full corpus -> data/records.jsonl
```

> **Note (issue #1205):** `energy-community.org` returns **403** to datacenter IPs
> (Hetzner) and to some US cloud vantages. A/B tested 2026-08-25 from a residential
> vantage: the registry returns **HTTP 200 with both our bot UA and a browser UA**, so
> this is an IP block, *not* a User-Agent block — sending a browser UA will not fix it.
> Build from a residential or EU vantage.

## License

[Energy Community Secretariat reuse notice](https://www.energy-community.org/disclaimer.html) — "Reproduction is authorised, provided the source is acknowledged, save where otherwise stated." Standard EU-institution reuse terms: **commercial use permitted, attribution required.**
