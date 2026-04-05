# US/WA-Legislation — Washington State Legislative Web Services

## Overview
Full text of Washington state legislation via official SOAP/REST web services and the lawfilesext.leg.wa.gov file server.

## Data Sources
- **RCW (Revised Code of Washington)**: ~100 titles of codified statutes crawled from `lawfilesext.leg.wa.gov/law/RCW/` directory listings
- **Bills**: 6400+ bill documents per biennium via `LegislativeDocumentService.asmx` SOAP/REST API with HTML full text from `lawfilesext.leg.wa.gov`

## Authentication
None required. All endpoints are publicly accessible.

## Usage
```bash
# Test connectivity
python bootstrap.py test-api

# Fetch sample records (10 RCW + 5 bills)
python bootstrap.py bootstrap --sample

# Full fetch (all RCW + current biennium bills)
python bootstrap.py bootstrap --full
```

## API Endpoints
- SOAP services: https://wslwebservices.leg.wa.gov/
- RCW file server: https://lawfilesext.leg.wa.gov/law/RCW/
- Bill documents: https://lawfilesext.leg.wa.gov/biennium/{biennium}/Htm/Bills/
