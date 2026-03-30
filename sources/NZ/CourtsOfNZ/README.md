# NZ/CourtsOfNZ — Courts of New Zealand Official Judgments

**Source:** https://www.courtsofnz.govt.nz/judgments
**Country:** New Zealand (NZ)
**Data type:** Case law
**Language:** English
**Auth:** None (government open access)

## Overview

Official judgments of public interest from the Supreme Court, Court of Appeal,
and High Court of New Zealand. Published via RSS feeds with PDF attachments.

## Strategy

1. Parse RSS feeds for 3 courts (Supreme, Court of Appeal, High Court)
2. Visit case pages to extract PDF URLs and citations
3. Download judgment PDFs and extract full text with PyPDF2
4. ~242 judgments from 2019 to present

## Usage

```bash
python bootstrap.py bootstrap            # Full pull
python bootstrap.py bootstrap --sample   # 15 sample records
python bootstrap.py test-api             # Connectivity test
```
