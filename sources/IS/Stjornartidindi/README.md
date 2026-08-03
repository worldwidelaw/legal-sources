# IS/Stjornartidindi — Iceland Official Gazette (Stjórnartíðindi)

The official **State Gazette of Iceland**, published by the Ministry of Justice
under *Lög um Stjórnartíðindi og Lögbirtingablað nr. 15/2005*, on the island.is
national platform: <https://www.stjornartidindi.is/>.

Three series:

| Series | Contents | ~Count |
|--------|----------|--------|
| **A-deild** | Acts of the Althingi (LÖG) and legislative instruments | ~3,860 |
| **B-deild** | Regulations, tariffs and administrative rules (reglugerðir) | ~34,840 |
| **C-deild** | Treaties and international agreements (milliríkjasamningar) | ~970 |

**~39,600 documents**, all born-digital full text.

## How it works

The public JSON API returns adverts with the **full document text embedded inline**
as `document.html` — no per-document fetch or OCR is needed:

```
GET https://api.stjornartidindi.is/api/v1/adverts?department={a-deild|b-deild|c-deild}&page=N&pageSize=100
```

The response `paging` block gives `totalPages`/`totalItems`; loop over pages. Each
advert carries `id`, `title`, `publicationNumber` (`{full,number,year}`),
`signatureDate`, `publicationDate`, `involvedParty` (issuing ministry), `type`,
and `document.{html,pdfUrl}`. The scraper strips `document.html` to clean text.

## Usage

```bash
python bootstrap.py test                 # connectivity/parse test
python bootstrap.py bootstrap --sample   # 15 sample records (across all 3 series)
python bootstrap.py bootstrap            # full pull (streams to data/records.jsonl)
python bootstrap.py bootstrap-fast       # full pull, concurrent
python bootstrap.py update 2025          # documents published 2025 onward
```

## License

[Public domain — official state publication](https://www.stjornartidindi.is/) —
no attribution required, commercial use permitted.

Stjórnartíðindi is the official State Gazette of Iceland. Its legal texts (laws,
regulations, treaties) are public government works published under *Lög um
Stjórnartíðindi og Lögbirtingablað nr. 15/2005* with no access restriction,
authentication or licence fee — consistent with the `pd-gov` licensing of the
sibling IS/Lagasafn and IS/Reglugerd sources.
