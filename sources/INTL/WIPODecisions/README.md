# INTL/WIPODecisions — WIPO UDRP Domain Name Dispute Decisions

Panel decisions from the WIPO Arbitration and Mediation Center under the Uniform Domain-Name Dispute-Resolution Policy (UDRP).

## Coverage

- **44,000+ decisions** with full HTML text (1999-2021)
- 2022+ decisions are PDF-only (not fetched by this scraper)
- Covers domain name disputes: complainant, respondent, domain names, outcome
- Average 8K-25K chars per decision

## Method

1. Master index at `/decisionsx/index.html` lists year/sequence batches
2. `list.jsp` pages provide case metadata (case number, parties, domains, outcome)
3. `text.jsp?case=X` redirects to the full-text HTML decision page

## Usage

```bash
python bootstrap.py test                  # Test connectivity
python bootstrap.py bootstrap --sample    # Fetch 15 sample records
python bootstrap.py bootstrap             # Full bootstrap (44K+ records)
```

## License

[WIPO Terms of Use](https://www.wipo.int/tools/en/disclaimer.html) — UDRP decisions are publicly available. Verify WIPO terms before commercial redistribution.
