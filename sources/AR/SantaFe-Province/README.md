# AR/SantaFe-Province — Santa Fe Province Legislation

Provincial legislation from Santa Fe, Argentina via the
Sistema de Información de Normativa (SIN).

**URL:** https://www.santafe.gov.ar/normativa/

## Coverage

- Laws (Leyes): ~5,176
- Decrees (Decretos): ~11,022
- Dispositions (Disposiciones): ~4,091
- Resolutions (Resoluciones): ~20,659
- Dictámenes: ~41
- Period: 1978–present
- Language: Spanish

## Strategy

1. POST search to `busqueda.php` with document type filter (paginated, 10/page)
2. Parse HTML table for metadata and detail page links
3. Fetch `item.php` for full metadata (firmantes, temas, jurisdiction)
4. Download PDF via `getFile.php` and extract text with pdfplumber

## License

[Public Domain (Government Work)](https://www.santafe.gov.ar/normativa/) — Argentine government legislation is public domain.
