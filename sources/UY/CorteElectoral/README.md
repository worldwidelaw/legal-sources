# UY/CorteElectoral — Uruguay Corte Electoral (Sentencias)

Full-text jurisprudence of Uruguay's **Corte Electoral**, the constitutional-rank
electoral authority (distinct from the Suprema Corte de Justicia and the Tribunal de
lo Contencioso Administrativo). Decisions ("Sentencias") cover electoral and civic
matters from the 1970s to present.

## Source

Official IMPO "Base de Datos de la Corte Electoral":
- Landing: https://www.impo.com.uy/basecorteelectoral/
- Document pages (public, full text):
  `https://www.impo.com.uy/bases/sentencias-corte-electoral/{NUM}-{YEAR}/1`
  e.g. https://www.impo.com.uy/bases/sentencias-corte-electoral/27220-2014/1

## Access & enumeration

IMPO's *search* interface requires a free login, but every **individual sentencia
document page is public**. Sentencia numbers form a single dense, sequential
per-base counter; each number belongs to exactly one (monotonically non-decreasing)
year. The scraper anchors on the verified pair `27220 / 2014` and walks the counter
up and down, trying a small window of candidate years per number, stopping after a
long run of consecutive misses (corpus boundary). No authentication is used.

A non-existent `{NUM}-{YEAR}` pair returns a ~219-char placeholder; a real one
returns the full decision body (served as latin-1 / ISO-8859-1 HTML).

## Usage

```bash
python bootstrap.py test-api                 # connectivity check (anchor doc)
python bootstrap.py bootstrap --sample        # 12 sample sentencias (full text)
python bootstrap.py bootstrap                 # full crawl (idempotent)
python bootstrap.py bootstrap-fast            # streaming full crawl (VPS)
```

## Data type

`case_law` — each record is a single Corte Electoral decision with full text,
decision number, year and decision date.

## License

[Texto Oficial — IMPO (public records)](https://www.impo.com.uy/) — official
Uruguayan state legal texts (sentencias of the Corte Electoral) published by IMPO,
the Centro de Información Oficial. Official legal texts of the State are in the
public domain under Uruguayan law. Commercial use permitted; no attribution required.
