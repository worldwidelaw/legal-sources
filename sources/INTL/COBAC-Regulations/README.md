# INTL/COBAC-Regulations — Commission Bancaire de l'Afrique Centrale (CEMAC)

Banking regulations (*règlements COBAC*) issued by the **Commission Bancaire de
l'Afrique Centrale (COBAC)**, the supranational banking supervisor of the CEMAC
monetary union — Cameroon, Central African Republic, Chad, Republic of the
Congo, Equatorial Guinea, and Gabon. The texts are published as PDFs on the
website of the **BEAC (Banque des États de l'Afrique Centrale)**.

- **Publisher:** COBAC / BEAC
- **Listing page:** https://www.beac.int/supervision-bancaire/reglements-de-cobac/
- **Data type:** legislation
- **Coverage:** CEMAC regional (6 member states) — banking, prudential, and
  credit-institution supervision regulations
- **Auth:** none

## How it works

1. Scrape the single listing page for every PDF under `/wp-content/uploads/`.
2. De-duplicate near-identical filename variants (`foo.pdf` / `foo-1.pdf`).
3. Download each PDF and extract its full text via `common/pdf_extract.py`.
4. Skip PDFs without a text layer (scanned images, < 500 chars).

About **37 of the ~79 listed PDFs** carry an extractable text layer; the rest
are scanned images and are skipped. (The sibling source `CG/BEAC-Regulations`
is blocked because *its* corpus is entirely scanned — this COBAC page differs.)

## Usage

```bash
python bootstrap.py test                 # connectivity + first-PDF check
python bootstrap.py bootstrap --sample   # save sample records
python bootstrap.py bootstrap-fast       # full high-throughput pull (VPS)
```

## License

[Open Government Data](https://www.beac.int/) — official regulatory texts of the
COBAC/BEAC, a CEMAC public institution, published for public information on
beac.int. No explicit open licence is attached; treated as open government data
(commercial use permitted, attribution appreciated).
