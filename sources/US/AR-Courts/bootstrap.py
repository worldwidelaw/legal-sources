#!/usr/bin/env python3
"""
US/AR-Courts -- Supreme Court of Arkansas

Case law via the CourtListener search API. The enumeration, checkpointing,
full-text ladder and CLI live in common/courtlistener.py, which is shared by
every US/{ST}-Courts source; this module declares only what differs.

See common/courtlistener.py and issue #1493 for why the previous per-state
implementation (flat cursor walk, no checkpoint, stored-file-only text, output
written to sample/, no bootstrap-fast alias) never advanced past the most
recent few years.

Usage:
  python bootstrap.py bootstrap --sample   # Sample records
  python bootstrap.py bootstrap --full     # Full checkpointed backfill
  python bootstrap.py bootstrap-fast --full
  python bootstrap.py update --since YYYY-MM-DD
  python bootstrap.py coverage             # Expected-vs-indexed audit per court
  python bootstrap.py test
"""

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.courtlistener import CourtListenerScraper, run_cli


class ARCourtsScraper(CourtListenerScraper):
    SOURCE_ID = 'US/AR-Courts'
    COURT_ORDER = ['ark', 'arkctapp']
    COURT_NAMES = {
        'ark': 'Supreme Court of Arkansas',
        'arkctapp': 'Court of Appeals of Arkansas',
    }
    COURT_ABBRS = {
        'ark': 'ARSC',
        'arkctapp': 'ARCA',
    }
    # _id stays f"{ID_PREFIX}-{abbr}-{cluster_id}", unchanged from the
    # per-state implementation so already-indexed rows keep their dedup key.
    ID_PREFIX = 'US-AR'
    DEFAULT_ABBR = 'ARCT'
    JURISDICTION = 'US-AR'


if __name__ == "__main__":
    run_cli(ARCourtsScraper)
