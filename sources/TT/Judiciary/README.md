# Trinidad and Tobago Judiciary — Judgments

**Source:** [https://www.ttlawcourts.org/](https://www.ttlawcourts.org/)
**Country:** TT
**Data types:** case_law
**Status:** Complete

Written judgments of the Judiciary of Trinidad and Tobago — the **Court of Appeal**,
the **High Court** and the **Magistrates' Court** — covering decisions from the
1990s to the 2020s (~620 documents with full text).

## How it works

The Judiciary publishes judgments as PDF/RTF/HTM files on its library server
`webopac.ttlawcourts.org` (IP `200.1.109.12`). That host sits on a Trinidad-only
network block and **refuses connections from foreign / datacenter IPs** (verified:
connect timeout on ports 80 and 443 off-island), which is why the source was
previously blocked as `pdf_server_unreachable`.

This scraper instead reads the corpus from the **Internet Archive Wayback Machine**,
which has captured the `webopac.ttlawcourts.org/LibraryJud/Judgments/*` tree:

1. Enumerate every archived judgment document via the Wayback **CDX API**
   (`collapse=urlkey`, `filter=statuscode:200`), keeping the most recent capture
   of each URL and skipping the `photographs/*.jpg` scanned page-images.
2. Download each capture through the raw `/web/<timestamp>id_/<url>` endpoint.
3. Extract full text — PDF via **PyMuPDF (fitz)**, RTF/HTM via a light text strip.
4. Parse the court, judge, case number and decision date from the archived path
   (e.g. `coa/2008/mendonca/CvA_08_45DD16nov2011.pdf` → Court of Appeal, Civil
   Appeal No. 45 of 2008, decided 16 Nov 2011). Party names (`A v B`) are pulled
   from the judgment's `BETWEEN … AND …` block when present.

Requires `PyMuPDF` on the runner (same dependency class as other PDF sources).

## License

[Public Domain — Government Edict](https://www.law.cornell.edu/uscode/text/17/105) — court judgments are edicts of government and are not subject to copyright; freely reproducible including for commercial use. Underlying documents are produced by the Judiciary of Trinidad and Tobago; full text is mirrored via the Internet Archive Wayback Machine.
