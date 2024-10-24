#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Helper functions for cached downloads.

Written by Freek Dijkstra in March 2018.

Available under MIT license.
(Re-use is permitted, attribution is required; code comes as-is without guarantees)
"""

from urllib.parse import urlparse
from urllib.error import HTTPError
from pathlib import Path
# from exceptions import FileNotFoundError
import shutil
import datetime
import json
import logging
import re
import time
from random import random, uniform
from xml.etree import ElementTree
# external library
try:
    import requests
except ImportError:
    raise ImportError("Package requests is not available. Install using e.g. "
            "`port install py-requests` or `pip install requests`.") from None

# type hints
from http.client import HTTPResponse # noqa (only used for type hints)
URL = str

class CachedDownloader:
    def __init__(self, cachefolder: Path = None, cache_enforce_chance=0.0, delay: float=0.2, includehostname=True) -> None:
        # TODO: if not cachefolder: Create one in /var/tmp
        # Note: cache_enforce_chance is deprecated and ignored
        if cachefolder is None:
            cachefolder = Path(__file__).parent
        self.cachefolder = cachefolder.resolve()
        self.logger = logging.getLogger('cached-downloader')
        self.logger.debug(f"Set cachefolder to {self.cachefolder}")
        self.cachefolder.mkdir(parents=True, exist_ok=True)

        self.session = requests.Session()
        self.mindelay = 0.5*delay
        self.maxdelay = 1.5*delay
        self.prevtime = time.time() - self.maxdelay
        self.includehostname = includehostname

    def add_cookie(self, name, value, domain, path='/'):
        self.session.cookies.set(name, value, domain=domain, path=path)

    def backup(self, sourcefile: Path) -> None:
        """Make a copy of the given file to the cachefolder"""
        try:
            sourcepath = sourcefile.resolve()  # strict=True only introduced in 3.6
            if not sourcepath.exists():
                raise FileNotFoundError("File not found: %s" % (sourcepath))
        except FileNotFoundError:
            self.logger.warning("Can't make backup. File not found: %s" % sourcefile)
            raise
        separator = ' ' if ' ' in sourcepath.stem else '.'
        dest_filename = sourcefile.stem + separator + datetime.date.today().isoformat() \
                    + sourcefile.suffix
        self.logger.debug("Backup '%s' to '%s'." % (sourcefile, dest_filename))
        
        destpath = self.cachefolder / dest_filename
        try:
            shutil.copyfile(str(sourcepath), str(destpath))
        except (OSError):
            self.logger.warning("Can't make backup to %s" % (destpath))
            raise

    def _url_to_short_filename(self, url: str, extension: str='.html') -> Path:
        """Get filename from path and query parameters name *id or *ids.
        does not include the hostname."""
        pu = urlparse(url)
        short_name = pu.path.strip('/')
        short_name = re.sub(r'[^A-Za-z0-9]+', '_', short_name)
        if short_name.endswith("json"):
            short_name = short_name[:-5]
            extension = ".json"
        for query in pu.query.split('&'):
            try:
                k, v = query.split('=')
                k = k.lower()
                if k.endswith('id') or k.endswith('ids') or k in ('params','volts'):
                    # replace sequence of non-word characters with _
                    v = re.sub(r'[^A-Za-z0-9]+', '_', v)
                    short_name += '_' + k + '_' + v
            except (ValueError, IndexError):
                pass  # ignore any errors
        short_name += extension
        if self.includehostname:
            return Path(pu.hostname, short_name)
        return short_name

    def get_cached_url(self, url: URL, cache_name: str=None, ttl: 
                float=1.2, may_extend_cache: bool=False, 
                cookies: dict={}, encoding='utf-8', 
                decode_func=lambda x: x, decode_name='text', binary_mode=False,
                **kwargs):
        """Return a Python object from URL or cache file.
        The ttl is time-to-live of the cache file in days."""
        if not cache_name:
            cache_name = self._url_to_short_filename(url)
        file_path = self.cachefolder / cache_name
        file_path.parent.mkdir(parents=True, exist_ok=True)

        _downloaded_data = False
        data = None
        finalurl = url
        use_cache = False
        if file_path.exists() and time.time() - file_path.stat().st_mtime < ttl * 86400:
            # file exists and is recent (or is not recent, but we enforce cache to trottle downloads)
            self.logger.debug("Fetching %s" % (cache_name))
            if binary_mode:
                with file_path.open('rb') as f:
                    data = f.read()
            else:
                with file_path.open('r', encoding=encoding) as f:
                    data = f.read()
        else:
            try:
                delay = uniform(self.mindelay, self.maxdelay) + self.prevtime - time.time()
                if delay > 0:
                    time.sleep(delay)
                self.logger.debug("Fetching %s (after %.1fs delay)" % (url, delay))
                r = self.session.get(url, cookies=cookies)
                self.prevtime = time.time()
                if binary_mode:
                    data = r.content
                else:
                    data = r.text
                finalurl = r.url
                if finalurl != url:
                    self.logger.info("%s redirects to %s." % (url, finalurl))
                r.raise_for_status()
                _downloaded_data = True
            except requests.ConnectionError as exc:
                self.logger.error("Can't connect to %s: %s" % (url, exc))
                raise ConnectionError("Failed to download data from %s" % url) from None
            except (HTTPError, requests.exceptions.HTTPError) as e:
                self.logger.warning("HTTP error for %s: %s" % (url, e))
                # Regretfully, in case of HTTP Error 429: Too Many Requests,
                # e.headers does not contain a "Retry-after" header on store.steampowered.com/api.
                raise ConnectionError("Failed to download data from %s" % url) from None
        
        try:
            decoded_data = decode_func(data)
        except ValueError as e:
            if len(data) == 0:
                self.logger.error("Donwloaded 0 bytes from %s. Invalid %s" % (url, decode_name))
            if finalurl == url:
                self.logger.error("Can't decode %s from %s: %s" % (url, decode_name, e))
                raise ValueError("Failed to download data from %s" % url) from None
            elif 'login' in finalurl:
                self.logger.error("%s redirects to non-%s login page %s. " \
                              "Please verify login credentials." % (url, decode_name, finalurl))
                raise PermissionError("Redirected to login page from %s" % url) from None
            else:
                self.logger.error("%s redirects to non-%s page %s." % (url, decode_name, finalurl))
                raise ConnectionError("Redirected to non-%s page from %s" % (decode_name, url)) \
                        from None
        if _downloaded_data:
            try:
                self.logger.debug("Write to %s" % (file_path))
                if binary_mode:
                    with file_path.open('wb') as f:
                        f.write(data)
                else:
                    with file_path.open('w', encoding=encoding) as f:
                        f.write(data)
            except OSError as e:
                self.logger.warning("%s" % (e))
                # report and proceed (ignore missing cache)
        return decoded_data
    
    def get_cached_html(self, url: URL, cache_name: str=None, ttl: float=1.2, may_extend_cache: bool=False, cookies: dict={}):
        """Return a BeautifulSoup (parsed html) object from URL or cache file.
        The ttl is time-to-live of the cache file in days."""
        return self.get_cached_url(url, cache_name, ttl=ttl, may_extend_cache=may_extend_cache, cookies=cookies, 
                        decode_func=BeautifulSoup, decode_name='html', binary_mode=False)

    def get_cached_json(self, url: URL, cache_name: str=None, ttl: float=1.2, may_extend_cache: bool=False, cookies: dict={}):
        """Return a Python object from URL or cache file.
        The ttl is time-to-live of the cache file in days."""
        return self.get_cached_url(url, cache_name, ttl=ttl, may_extend_cache=may_extend_cache, cookies=cookies, 
                        decode_func=json.loads, decode_name='JSON', binary_mode=False)

    def get_cached_xml(self, url: URL, cache_name: str=None, ttl: float=1.2, may_extend_cache: bool=False, cookies: dict={}):
        """Return a Python object from URL or cache file.
        The ttl is time-to-live of the cache file in days."""
        return self.get_cached_url(url, cache_name, ttl=ttl, may_extend_cache=may_extend_cache, cookies=cookies, 
                        decode_func=ElementTree.fromstring, decode_name='XML', binary_mode=False)

    def get_cached_binary(self, url: URL, cache_name: str=None, ttl: float=1.2, may_extend_cache: bool=False, cookies: dict={}):
        """Return a Python object from URL or cache file.
        The ttl is time-to-live of the cache file in days."""
        return self.get_cached_url(url, cache_name, ttl=ttl, may_extend_cache=may_extend_cache, cookies=cookies, 
                        decode_name='Image', binary_mode=True)
