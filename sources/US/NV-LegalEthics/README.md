# US/NV-LegalEthics — State Bar of Nevada, Ethics Opinions

Full text of the **formal ethics opinions** issued by the **State Bar of
Nevada's Standing Committee on Ethics and Professional Responsibility**. Each
opinion interprets the **Nevada Rules of Professional Conduct** (and, for older
opinions, the predecessor rules) in response to a stated question, to advise
lawyers on their professional obligations = **doctrine** (advisory).

One continuous globally-numbered series, **Formal Opinion No. 1 (1986) → No. 61
(2025, present)**; all published free as PDFs on nvbar.org.

- **Publisher:** State Bar of Nevada — Standing Committee on Ethics and
  Professional Responsibility
- **Listing page:** https://nvbar.org/for-lawyers/ethics-discipline/ethics-opinions/
- **Type:** doctrine
- **Jurisdiction:** US-NV

## How it works

1. The published corpus is indexed on a single listing page which links every
   opinion PDF directly under `/wp-content/uploads/`. Filenames are irregular
   (`opinion_41.pdf`, `Ethics_Op_50.pdf`, `NV-Ethics-Opinion-No.-53.pdf`,
   `Finalized-Opinion-Rule-4.2-No-Contact-Rule_1.13.25-1.pdf`, …), so the href
   is taken verbatim and the opinion **number** is parsed primarily from the
   anchor text (`OPINION 61`) and, when the anchor text is generic (`complete
   opinion, PDF`), from the url-decoded filename.
2. Anchors appear in document order — the canonical modern list first, then a
   legacy duplicate list and `… Summary.pdf` digests. The scraper keeps the
   **first** link per opinion number and **skips summaries**, so it takes the
   full opinion, not its summary.
3. Most opinion PDFs are **born-digital** (text layer) → extracted with PyMuPDF
   (`fitz`), **no OCR**. A block of the middle-era opinions (roughly Nos. 37–52)
   are published only as scanned image PDFs with no text layer and are correctly
   skipped (< 150 chars); Nos. 1–36 and 53–61 are born-digital, so the full pull
   yields **41 full-text opinions**.
4. The issue **date** is parsed from the opinion header (`April 29, 1994`;
   `January __, 2025` with a blank day → day 01) into ISO 8601.

## Usage

```bash
python bootstrap.py test-api            # connectivity + extraction test
python bootstrap.py bootstrap --sample  # ~12 sample records
python bootstrap.py bootstrap           # full pull (all opinions)
```

## Distinct from

- **US/NV-EthicsOpinions** — executive **Nevada Commission on Ethics** (advises
  public officials on the Nevada Ethics in Government Law), a different body.
- **US/NV-Courts**, **US/NV-Legislation**, **US/NV-AGOpinions**.

This is the 24th source in the state-bar attorney legal-ethics vein
(NC/AZ/TX/UT/VA/CA/WA/IL/NY/MI/OR/OH/ME/CT/VT/SC/WI/GA/KY/NH/AL/MO/TN + NV).

## License

[Public Domain](https://www.law.cornell.edu/uscode/text/17/105) — U.S.
government-edict rationale (17 U.S.C. § 105). The State Bar of Nevada is the
state's **integrated (mandatory) bar**, unified under and regulated by the
Supreme Court of Nevada (SCR); its ethics opinions are the work of a
government-authorized body regulating the legal profession, treated as public
domain consistent with the other state-bar legal-ethics sources. Published free
on nvbar.org with no login, paywall or terms prohibiting reuse. **Commercial use
permitted.**
