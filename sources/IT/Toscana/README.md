# IT/Toscana - Toscana Regional Legislation

Tuscany Regional Legislative Database (Raccolta Normativa della Regione Toscana).

## Source Information

- **URL**: https://raccoltanormativa.consiglio.regione.toscana.it
- **Data Type**: Regional legislation
- **Format**: NIR XML (Norme in Rete)
- **License**: CC BY 3.0
- **Coverage**: 1971-present (all 10 legislatures)
- **Update Frequency**: Weekly

## Document Types

- `legge` - Regional laws (leggi regionali)
- `regolamento.consiglio` - Council regulations
- `regolamento.giunta` - Executive regulations (Giunta)

## Data Access

The source provides bulk ZIP downloads organized by:
- Legislature (I-X, 1970-present)
- Document type (laws, council regulations, executive regulations)
- Format (XML, Akoma Ntoso, PDF, RTF, TXT)

Download endpoint:
```
https://raccoltanormativa.consiglio.regione.toscana.it/class/download.php?type=zip&formato=xml&tipo=legge&metaleg=10
```

Parameters:
- `type=zip` - Download as ZIP archive
- `formato` - xml (NIR), akm (Akoma Ntoso), pdf, rtf, txt
- `tipo` - legge, regolamento.consiglio, regolamento.giunta
- `metaleg` - 1-10 (legislature number)

## NIR XML Format

Documents use the NIR (Norme in Rete) Italian standard for legal documents:
- URN identifiers (e.g., `urn:nir:regione.toscana:legge:2025-08-22;56`)
- Rich metadata in `<meta>/<descrittori>` section
- Cross-references to other laws using `<rif xlink:href="...">`
- Full text with structural markup (articles, paragraphs, etc.)

## Usage

```bash
# Fetch sample records
python bootstrap.py bootstrap --sample

# Fetch all records
python bootstrap.py bootstrap --full

# Fetch updates since a date
python bootstrap.py fetch_updates --since 2025-01-01
```

## Sample Output

Each record includes:
- `_id`: URN identifier
- `_source`: "IT/Toscana"
- `_type`: "legislation"
- `title`: Document title
- `doc_type`: legge/regolamento
- `number`: Document number
- `date`: Document date (ISO 8601)
- `text`: Full text content
- `url`: Link to source document
- `keywords`: Subject keywords
- `emanating_body`: Issuing authority

## Coverage Statistics

Estimated ~2,000+ documents across all legislatures:
- X Legislature (2020-present): ~400 documents
- IX Legislature (2015-2020): ~500 documents
- Earlier legislatures: 100-300 each
