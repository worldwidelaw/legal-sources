# SK/UVO-Namietky — Slovakia, Public Procurement Office objection decisions

Full text of the **decisions on objections** (*rozhodnutia o námietkach*) issued
by the Slovak **Úrad pre verejné obstarávanie** (ÚVO, Public Procurement Office),
the national review body for public procurement, in review proceedings under
Act No. 343/2015 Coll. on public procurement.

- **Register:** <https://www.uvo.gov.sk/dohlad/namietky/prehlad-rozhodnuti-o-namietkach-vseobecna-agenda>
- **Type:** `case_law`
- **Language:** Slovak
- **Coverage:** general agenda (*všeobecná agenda*), 2002 → present.
  472 listing pages ≈ 9,435 register entries; entries from roughly 2006 onward
  publish the full decision as a born-digital PDF, which is what this source
  ingests. The oldest entries are summary-only in the register and are skipped
  as having no full text.

## How it works

1. **Listing** — `GET ?page=N` returns a 7-column TYPO3 table, 20 rows per page:
   contracting authority, contract subject, Vestník notice reference, statutory
   ground of the objection, decision number + PDF link, decision date, operative
   outcome. All metadata comes from the register, so nothing has to be mined out
   of the PDF body. The page count is read from the pagination widget rather
   than hardcoded.
2. **Full text** — the row's `rozhodnutie-download/{id}?cHash={hash}` link
   streams the decision PDF, extracted with the shared `common/pdf_extract`
   backends.

Completed listing pages are checkpointed to `data/checkpoint.json`, so a
relaunched run resumes without re-walking the register.

## Gotchas

- The **`cHash` token is mandatory** and can only be harvested from the listing
  row that owns it — the numeric id alone does not resolve. Ids are therefore
  never enumerated.
- There are **two id spaces**: positive (recent) and **negative** (pre-~2015).
  A naive `\d+` regex silently drops the entire historical corpus. Verified:
  id `-6520` → a 2006 decision, 6,707 chars of full text.
- Downloads are served as `application/octet-stream`, so the `%PDF-` magic is
  sniffed instead of trusting the `Content-Type` header.
- Listing pages are **not strictly chronological** and PDF-backed rows are not
  contiguous — pages 400 and 450 hold 20 rows with no PDF while page 470 holds
  one. Stopping at the first PDF-less page would truncate the corpus, so every
  page up to the last is walked.
- If the whole walk yields zero PDF-backed rows the run raises instead of
  reporting an empty corpus, so a future block fails loud rather than silently
  ingesting only the committed samples.

## Related sources

Sibling ÚVO registers, not yet built: `/dohlad/kontrola/prehlad-rozhodnuti-o-kontrolach`
(≈154 pages) and the *správne delikty / pokuty* register (≈32 pages).
Distinct from `CZ/UOHS` (Czech competition and procurement office).

## Usage

```bash
python bootstrap.py test-api             # connectivity + one full-text extraction
python bootstrap.py bootstrap --sample   # 15 sample records
python bootstrap.py bootstrap            # full pull
python bootstrap.py bootstrap-fast       # high-throughput full pull (fleet)
python bootstrap.py updates --since 2026-01-01
```

## License

> ⚠️ **Commercial use restricted (unverified).** The site footer asserts
> "© ÚVO … Všetky práva vyhradené" and publishes no reuse statement.

[Úrad pre verejné obstarávanie — terms not published](https://www.uvo.gov.sk/) —
Slovak copyright law ([Act No. 185/2015 Coll., § 5](https://www.slov-lex.sk/ezbierky/pravne-predpisy/SK/ZZ/2015/185/))
excludes official decisions and other acts of public authorities from copyright
protection, which strongly suggests the decision texts themselves are freely
reusable. With no published statement from the Office, this source is flagged
`commercial_use: false` pending confirmation.
