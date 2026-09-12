# US/NJ-LegalEthics — New Jersey Supreme Court Ethics Advisory Opinions

Full text of the advisory ethics opinions issued by the three standing ethics
committees appointed by the **Supreme Court of New Jersey**, digitized by the
**Rutgers Law Library**. Each opinion applies the **New Jersey Rules of
Professional Conduct** and related court rules to a stated question in order to
advise lawyers — i.e. **doctrine** (advisory).

- **ACPE** — Advisory Committee on Professional Ethics (file prefix `acp`)
- **CAA** — Committee on Attorney Advertising (file prefix `caa`)
- **UPL** — Committee on the Unauthorized Practice of Law (file prefix `cua`)

All three committees are appointed by, and act under the authority of, the
Supreme Court of New Jersey (R. 1:19, R. 1:19A, R. 1:22).

- **Publisher:** Rutgers Law Library digital collection (on behalf of the NJ
  Supreme Court committees)
- **Coverage:** 1963–present, ~900 born-digital PDFs → ~849 opinions
- **Type:** `doctrine` (advisory ethics opinions interpreting the NJ RPC)
- **Full text:** yes — PyMuPDF/fitz text layer, no OCR

## Source & method

- **Index:** the collection at `njlaw.rutgers.edu/ethics/` stores every opinion
  as a born-digital PDF under `/ethics/pdfs/`, and that directory has an **open
  directory index** listing all ~902 files. The scraper parses the listing and
  groups files into opinions by `(series prefix, number)`.
- **Multi-part:** a single opinion may span several PDFs
  (`{prefix}{N}_1.pdf`, `{prefix}{N}_2.pdf`, …), concatenated in natural part
  order.
- **Number:** from the filename (`acp724` → ACPE Opinion 724); the header echoes
  it as `OPINION {N}`.
- **Title:** the subject caption printed immediately after the `OPINION {N}`
  header line; falls back to `{series} Opinion {N}`.
- **Date:** the first `Month DD, YYYY` in the body (the issue date printed
  beside the N.J.L.J. citation); falls back to null.

### TLS note

`njlaw.rutgers.edu` completes the TLS handshake but ships the **wrong
intermediate CA** (the leaf is issued by *InCommon RSA Server CA 2*), so a bare
`requests` client fails `CERTIFICATE_VERIFY_FAILED`. Fetches are therefore
routed through the common `HttpClient`, which **AIA-fetches the real
intermediate and verifies the chain properly** (this succeeds here, so the
connection stays verified); `njlaw.rutgers.edu` is additionally on
`insecure_ssl_hosts` as a last-resort fallback, matching CR/SCIJ, VN/CongBao
and INTL/EnergyCharterTreaty. No JavaScript, CAPTCHA or authentication is
required.

## Distinct from

- **US/NJ-EthicsDecisions** — NJ State Ethics Commission (executive-branch
  officials), not the Supreme Court's attorney-conduct committees.
- **US/NJ-Courts** — New Jersey court decisions.
- **US/NJ-Legislation** — New Jersey statutes.

## Usage

```bash
python bootstrap.py test-api             # connectivity + extraction test
python bootstrap.py bootstrap --sample   # ~12 sample records
python bootstrap.py bootstrap            # full pull (all opinions)
```

## License

[Public Domain (US government edict — 17 U.S.C. § 105)](https://www.law.cornell.edu/uscode/text/17/105)
— the ACPE, Committee on Attorney Advertising and Committee on the Unauthorized
Practice of Law are standing committees appointed by, and acting under the
authority of, the Supreme Court of New Jersey. Their advisory opinions interpret
the New Jersey Rules of Professional Conduct and related court rules, so the
texts are the work of a government-authorized body — treated as public domain
under the government-edicts rationale, consistent with the other state
ethics-committee legal-ethics sources. Digitized and published free to the
public by the Rutgers Law Library with no login, paywall or terms prohibiting
reuse. Commercial use permitted; no attribution required.
