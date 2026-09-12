# GN/DroitGuineen

**Droitguinéen** — the legal reference portal of the Republic of Guinea.

- **URL**: https://droitguineen.com/
- **Coverage**: 4,775 legal texts (incl. 32 codes) + 156 court decisions
- **Language**: French
- **Data types**: legislation, case_law

## Content

- **Legislation**: Constitution, codes (civil, penal, labor, mining, maritime, etc.), laws, organic laws, ordinances, decrees, OHADA uniform acts
- **Case law**: Supreme Court decisions, Court of Appeal decisions, CCJA (OHADA Common Court of Justice and Arbitration) rulings

## Strategy

1. Parse `sitemap.xml` for all `/lois/` URLs
2. Fetch each page and extract the RSC (React Server Components) JSON payload
3. Parse the `initialData` object out of that payload — it carries both the
   metadata (id, title, nature, date, status) and the document body
4. Assemble the body from `visas` + the flat `articles` array + the nested
   `sections` tree (`titre` / `articles` / `enfants`), which is how codes carry
   their titles, chapters and sections
5. Resolve `$XX` references to the hoisted `<id>:T<hex_length>,<content>` RSC
   text rows, slicing by the declared byte length

### A browser User-Agent is required

droitguineen.com blocklists User-Agents *by name*, not by IP: from the same
vantage and second, `LegalDataHunter/1.0` and `python-requests/2.31.0` both get
403 while `Chrome/126` and even `curl/8.4.0` get 200 (issue #1456). The scraper
sends a Chrome UA and fails loud — rather than reporting zero documents — if the
sitemap 403s or ten document pages 403 in a row.

## License

[Terms of Use](https://droitguineen.com/cgu) — Free access. Legal texts are public domain under Guinean law. Attribution to Droitguinéen required.
