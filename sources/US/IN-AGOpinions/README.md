# US/IN-AGOpinions — Indiana Attorney General Official Opinions

Full text of **Official Opinions** issued by the Office of the Indiana
Attorney General. An Official Opinion is a formal, written legal
interpretation answering a question of Indiana law posed by a public
official, and is an authoritative state legal interpretation
(**doctrine**).

## Source

- Index (Tableau dashboard): https://www.in.gov/attorneygeneral/about-the-office/advisory/opinions/
- PDF file store: `https://www.in.gov/attorneygeneral/files/`

## Access strategy

The on-site opinions index is a Tableau dashboard embedded from a
Cloudflare-gated host (`datavizpublic.in.gov`), so it cannot be scraped
directly. However, the opinion PDFs themselves are served from the
un-gated `www.in.gov` file store under a predictable, year-keyed
filename scheme:

| Years        | Filename pattern                          |
|--------------|-------------------------------------------|
| 2009–present | `Official-Opinion-{YYYY}-{N}.pdf`         |
| 2006–2008    | `OfficialOpinion{YYYY}-{N}.pdf`           |

Enumeration probes both schemes year-by-year with cheap HEAD requests
(the site returns a real 404 status for missing files), then downloads
each existing PDF and extracts its text layer via the shared
`common.pdf_extract` helper (no OCR needed — the PDFs are text-based).
Issue dates and the `RE:` subject line are parsed from the opening
lines.

## Output

Each record is normalized to the doctrine schema with full `text`,
`opinion_number`, `title`, `syllabus`, `date`, and `url`.

## Usage

```bash
python bootstrap.py test-api             # connectivity test
python bootstrap.py bootstrap --sample   # ~12 sample documents
python bootstrap.py bootstrap            # full pull
```

## License

[Public Domain (US Government Work — Indiana)](https://www.law.cornell.edu/uscode/text/17/105) — Indiana Attorney General Official Opinions are official state government works in the public domain. Commercial use permitted; no attribution required.
