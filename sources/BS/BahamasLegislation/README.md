# BS/BahamasLegislation — Bahamas Legislation

**Source:** [https://laws.bahamas.gov.bs/](https://laws.bahamas.gov.bs/)
**Data types:** legislation

## Access

Live-first, Internet-Archive fallback. Since 2026-08 `laws.bahamas.gov.bs` answers
foreign/datacenter clients with a `403 - Forbidden` page (the fleet sees an HTTP 202
interstitial), so the corpus is read from the Wayback Machine:

1. CDX-enumerate `/cms/images/LEGISLATION/{PRINCIPAL|AMENDING|SUBORDINATE|BILLS}/{year}/{year-NNNN}/*.pdf`
   and keep the best capture per act number (~1,346 documents).
2. Recover act titles from archived captures of the alphabetical-index pages, which
   carry `<a class="npWrap" href="...pdf">Title</a>`; fall back to the CamelCase PDF
   filename, then to `Act No. N of YYYY`.
3. Replay each PDF through `/web/{ts}id_/` and extract full text.

Principal/amending acts keep the historical `BS-{year}-{NNNN}` id; subordinate
legislation is namespaced `BS-SI-{year}-{NNNN}` because the two series reuse numbers.
The live POST-driven index is retried first on every run. Enumerating zero documents
from both paths raises instead of reporting a silent success.

## License

Government open access — Bahamas government legislation portal. Official legal texts published for public access. No explicit open license stated on the site.
