# US/KY-LegalEthics — Kentucky Bar Association — Ethics Opinions

Legal-ethics opinions issued by the **Kentucky Bar Association (KBA)** interpreting
the **Kentucky Rules of Professional Conduct (SCR 3.130)** — and, for the older
opinions, the predecessor Canons of Professional Ethics — to advise **lawyers**.
One continuous **"KBA E-" series** running from E-1 (1962) to the E-450s (present);
~385 opinions are published.

The KBA is an **independent agency of the Supreme Court of Kentucky** with authority
to regulate the legal profession (SCR), so the opinions are the work of a
government-authorized body → treated as public domain.

## Data type

`doctrine` — advisory interpretations of the rules governing lawyers. Distinct from
`US/KY-Courts` (Kentucky appellate courts) and `US/KY-Legislation`
(KRS / legislature.ky.gov).

## Access & method

1. `GET` the listing page
   `https://kybar.org/For-Members/Rules-Ethics-Information/Ethics-Opinions`,
   which links every opinion PDF directly as
   `/Portals/0/Admin/Ethics Opinions/KBA E-{NNN}.pdf` (~385 opinions; some numbers
   in the 1..458 range are reserved/withdrawn and not linked). Take the **exact
   href** from the page — never construct the URL (filenames vary: `KBA E-` vs
   `KBA_E-`, some carry a `?ver=` cache-buster).
2. Download each PDF and extract text with **PyMuPDF** (born-digital text layer,
   **no OCR**). Header = `KENTUCKY BAR ASSOCIATION / Ethics Opinion KBA E-{N} /
   Issued: {Month [DD,] YYYY}` + a rules-amendment disclaimer + a
   `Subject:` / `Question[.:]` block + analysis.

The opinion **number** is taken from the filename, zero-pad stripped
(`KBA_E-001` → `E-1`) — never from the PDF body, which sometimes renders it with a
font artifact (e.g. `KBA E-l` for E-1).

## Record shape

```json
{
  "_id": "US/KY-LegalEthics/E-402",
  "_source": "US/KY-LegalEthics",
  "_type": "doctrine",
  "opinion_number": "E-402",
  "title": "KBA E-402: ...",
  "text": "KENTUCKY BAR ASSOCIATION Ethics Opinion KBA E-402 ...",
  "date": "1997-09-01",
  "issuer": "Kentucky Bar Association — Ethics Committee",
  "jurisdiction": "US-KY",
  "url": "https://kybar.org/Portals/0/Admin/Ethics%20Opinions/KBA_E-402.pdf"
}
```

## Usage

```bash
python bootstrap.py test-api            # connectivity + extraction check
python bootstrap.py bootstrap --sample  # ~12 sample records
python bootstrap.py bootstrap           # full pull (all opinions)
python bootstrap.py bootstrap-fast      # alias for full pull (VPS wrapper)
```

## License

[Public Domain (US government edict — 17 U.S.C. § 105)](https://www.law.cornell.edu/uscode/text/17/105) — no attribution required.

Kentucky Bar Association ethics opinions interpret the Kentucky Rules of
Professional Conduct (SCR 3.130). The KBA is an independent agency of the Supreme
Court of Kentucky with authority to regulate the legal profession, so the opinions
are the work of a government-authorized body — treated as public domain under the
government-edicts rationale, consistent with the other state-bar legal-ethics
sources. Published free to the public on kybar.org with no login, paywall or terms
prohibiting reuse. Commercial use permitted.
