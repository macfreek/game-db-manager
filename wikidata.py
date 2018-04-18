#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Interface to Wikidata"""

# Type hints
try:
    from typing import Dict, Any
except ImportError:
    Dict = Any = ''  # type: ignore

# local module (only used here for type hints)
from downloader import CachedDownloader


class WikiData:
    ENTITY_URL = 'https://www.wikidata.org/wiki/Special:EntityData/Q{identifier}.json'
    
    def __init__(self, downloader: CachedDownloader):
        self.downloader = downloader

    def get_wikidata(self, appid):
        """Given a WikiData ID, return the data. 
        May raise a IOError if no data can be downloaded."""
        return self.downloader.get_cached_json(self.ENTITY_URL.format(identifier=appid), 
                'wikidata_Q%s.json' % (appid))
