#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Interface to Gog"""

# Type hints
try:
    from typing import Dict, Any
except ImportError:
    Dict = Any = ''  # type: ignore

# local module (only used here for type hints)
from downloader import CachedDownloader

class Gog:
    GAME_URL = 'http://embed.gog.com/reviews/product/{slug}.json'
    
    def __init__(self, downloader: CachedDownloader):
        self.downloader = downloader

    def get_gogdata(self, slug):
        """Given a Gog slug (short key to identify a game), return the data. 
        May raise a IOError if no data can be downloaded."""
        return self.downloader.get_cached_json(self.GAME_URL.format(slug=slug),
                'gog_product_review_%s.json' % (slug))

# See 
# https://gogapidocs.readthedocs.io/
# https://github.com/Yepoleb/pygogapi
# https://www.gog.com/userData.json
