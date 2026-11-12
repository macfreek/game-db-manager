#!/usr/bin/env python3
# SPDX-AI-Disclosure: ai-generated
# SPDX-AI-Model: claude-opus-5-5
# SPDX-AI-Provider: Anthropic
# SPDX-AI-Scope: Code generated under supervision; design by the author.

"""
Download from websites that require a login, using the login cookie(s) of
the user.

A BrowserLogin subclass describes one website. It wraps a CachedDownloader
and offers the same get_cached_*() methods, so it can be used in its place.
These methods send the login cookie(s). If the website refuses them (a
PermissionError), new cookies are obtained with the chosen login method, and
the download is retried once. Login methods:

- config:  the cookie value from the configuration file; if refused, the user
           must copy a new one.
- browser: Playwright starts a browser where the user logs in.
- store:   read the cookie store of the user's own web browser
           (not implemented yet).
- cdp:     connect to a Chrome the user started with remote debugging
           (not implemented yet).

Cookies obtained with the last three methods are stored in the cache folder
for later runs.
"""

import json
import logging
import os
from pathlib import Path
from typing import Any, Callable, Dict, Optional, Tuple

DEFAULT_BROWSER = 'chrome'

# third party packages
try:
    from playwright.sync_api import sync_playwright, Error as PlaywrightError
except ImportError:
    sync_playwright = None
    PlaywrightError = Exception
try:
    import browser_cookie3
except ImportError:
    browser_cookie3 = None

# local modules
from downloader import AbstractDownloader, CachedDownloader


# ----------------------------------------------------------------------------
# Classes
# ----------------------------------------------------------------------------

class BrowserLogin(AbstractDownloader):
    """Download with a CachedDownloader, using login cookies; get new cookies
    with the chosen login method when they are missing or refused.

    Subclasses set the site constants. With login method 'browser', the
    browsers 'webkit', 'chromium' and 'firefox' are Playwright's own builds;
    'chrome' and 'msedge' use the installed browser.
    """
    NAME: str = ''
    LOGIN_URL: str = ''
    LOGIN_COOKIE_NAMES: Tuple[str, ...] = ()
    COOKIE_DOMAIN: str = ''
    COOKIE_PATH: str = '/'
    LOGIN_TIMEOUT: int = 300  # seconds

    def __init__(self, downloader: CachedDownloader,
                 login_method: str = 'config',
                 browser: str = DEFAULT_BROWSER,
                 cookie: Optional[str] = None) -> None:
        """Stored cookies (if any) are read from the cache folder of the
        downloader and added to it; this never opens a browser.

        login_method is 'config', 'browser', 'store' or 'cdp' (see the module
        docstring). browser is the browser Playwright starts ('browser'), or
        whose cookie store is read ('store'). cookie is the optional value of
        the first of LOGIN_COOKIE_NAMES, e.g. from the configuration file; it
        replaces a stored cookie of that name."""
        self.login_method = login_method
        self.downloader = downloader
        self.browser = browser
        if login_method == 'browser' and sync_playwright is None:
            logging.error("Interactive log in is not available. Install " \
                    f"`pip install playwright` followed by `playwright install {browser}`.")
        if login_method == 'cdp' and sync_playwright is None:
            # Note: it is possible without playwright, but easier with playwright.
            logging.error("Chrome DevTools Protocol in is not available. Install " \
                    f"`pip install playwright`.")
        if login_method == 'store' and browser_cookie3 is None:
            logging.error("Reading cookies from a web browser is not available. " \
                    "Run `pip install browser_cookie3`.")
        if login_method == 'config' and not cookie:
            logging.error("Session cookie is not defined in the configuration`.")
        self.cookie_path = downloader.cachefolder / f'{self.NAME}_cookies.json'
        self.config_cookie = ({self.LOGIN_COOKIE_NAMES[0]: cookie.strip()}
                              if cookie else None)
        self._add_cookies_to_downloader(self._load_cookies() or {})
        self._add_cookies_to_downloader(self.config_cookie or {})

    def get_cached_url(self, *args, **kwargs) -> Any:
        """Same as CachedDownloader.get_cached_url(), with login."""
        return self._fetch(self.downloader.get_cached_url, *args, **kwargs)

    def get_cached_html(self, *args, **kwargs) -> Any:
        """Same as CachedDownloader.get_cached_html(), with login."""
        return self._fetch(self.downloader.get_cached_html, *args, **kwargs)

    def get_cached_json(self, *args, **kwargs) -> Any:
        """Same as CachedDownloader.get_cached_json(), with login."""
        return self._fetch(self.downloader.get_cached_json, *args, **kwargs)

    def get_cached_xml(self, *args, **kwargs) -> Any:
        """Same as CachedDownloader.get_cached_xml(), with login."""
        return self._fetch(self.downloader.get_cached_xml, *args, **kwargs)

    def get_cached_binary(self, *args, **kwargs) -> Any:
        """Same as CachedDownloader.get_cached_binary(), with login."""
        return self._fetch(self.downloader.get_cached_binary, *args, **kwargs)

    def get_cookies(self) -> Dict[str, str]:
        """Return the stored cookies; log in via the browser if none are
        stored. May raise a PermissionError if the login fails."""
        return self._load_cookies() or self.refresh_cookies()

    def refresh_cookies(self) -> Dict[str, str]:
        """Get new cookies with login_method, store them and return them.
        Call this when a download raises a PermissionError. With 'config',
        log that the cookie must be updated and raise a PermissionError.
        'store' and 'cdp' raise a NotImplementedError for now."""
        if self.login_method == 'config':
            logging.error("The %s login cookie in the configuration is missing or "
                          "refused. Copy a new one from your web browser.",
                          self.NAME)
            raise PermissionError(f"Login for {self.NAME} failed.")
        if self.login_method == 'browser':
            cookies = self._login_interactively()
        elif self.login_method == 'store':
            cookies = self._read_browser_cookie_store(self.browser)
        elif self.login_method == 'cdp':
            cookies = self._login_via_cdp()
        else:
            raise ValueError(f"Unknown login method {self.login_method!r}.")
        self._store_cookies(cookies)
        return cookies

    def _fetch(self, fetch: Callable[..., Any], *args, **kwargs) -> Any:
        """Call fetch(*args, **kwargs) and return its result.

        On a PermissionError, log "Permission denied for <url>", get new
        cookies with refresh_cookies(), add them and call fetch once more.
        refresh_cookies() itself raises a PermissionError with login_method
        'config'. If the retry fails too, log "Login for <site> failed." and
        raise a new PermissionError."""
        try:
            return fetch(*args, **kwargs)
        except PermissionError:
            url = kwargs.get('url', args[0] if args else '')
            logging.error("Permission denied for %s.", url)
        self._add_cookies_to_downloader(self.refresh_cookies())
        try:
            return fetch(*args, **kwargs)
        except PermissionError:
            logging.error("Login for %s failed with login method %s.",
                          self.NAME, self.login_method)
            raise PermissionError(f"Login for {self.NAME} failed.") from None

    def _add_cookies_to_downloader(self, cookies: Dict[str, str]) -> None:
        """Add the cookies to the downloader for COOKIE_DOMAIN and
        COOKIE_PATH."""
        for name, value in cookies.items():
            self.downloader.add_cookie(name, value, self.COOKIE_DOMAIN,
                                       self.COOKIE_PATH)

    def _load_cookies(self) -> Optional[Dict[str, str]]:
        """Return the stored cookies, or None if there are none. May raise a
        json.JSONDecodeError if the cookie file is corrupt."""
        if not self.cookie_path.exists():
            return None
        with self.cookie_path.open(encoding='utf-8') as cookiefile:
            return json.load(cookiefile)

    def _store_cookies(self, cookies: Dict[str, str]) -> None:
        """Store the cookies in the cache folder. A new file is readable by
        the owner only; an existing file keeps its permissions."""
        fd = os.open(self.cookie_path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC,
                     0o600)
        with os.fdopen(fd, 'w', encoding='utf-8') as cookiefile:
            json.dump(cookies, cookiefile)

    def _login_interactively(self) -> Dict[str, str]:
        """Open LOGIN_URL and wait until _is_logged_in() is true, the user
        closes the window, or LOGIN_TIMEOUT expires. Return the last cookies
        seen; the caller's retry decides if they are valid. Logs a warning
        on close or timeout. May raise an ImportError if Playwright is
        missing."""
        if sync_playwright is None:
            raise ImportError("Package playwright is not available. Install "
            f"`pip install playwright` followed by `playwright install {self.browser}`."
            ) from None
        cookies: Dict[str, str] = {}
        with sync_playwright() as pw:
            browser = self._launch_browser(pw)
            context = browser.new_context()
            page = context.new_page()
            page.goto(self.LOGIN_URL)
            for _ in range(self.LOGIN_TIMEOUT):
                cookies = self._read_cookies(context, cookies)
                if self._is_logged_in(cookies, page.url):
                    break
                try:
                    page.wait_for_timeout(1000)
                except PlaywrightError:
                    logging.warning("Login window for %s closed before a login "
                                    "was detected.", self.NAME)
                    break
            else:
                logging.warning("No login for %s detected within %d seconds.",
                                self.NAME, self.LOGIN_TIMEOUT)
            cookies = self._read_cookies(context, cookies)
            browser.close()
        return cookies

    def _read_browser_cookie_store(self, browser: str,
                                   cookie_file: Optional[Path] = None
                                   ) -> Dict[str, str]:
        """Return the cookies for COOKIE_DOMAIN from the cookie store of the
        user's own web browser (e.g. 'opera', 'chrome', 'firefox', 'safari'),
        read with browser_cookie3. cookie_file overrides the default location
        of the store. The user logs in with that browser; no browser is
        opened. May raise an ImportError if browser_cookie3 is missing."""
        if browser_cookie3 is None:
            raise ImportError("Package browser_cookie3 is not available. Install "
                              "`pip install browser_cookie3`.") from None
        # TODO: call browser_cookie3.<browser>(cookie_file=..., domain_name=...)
        # and return {name: value}.
        raise NotImplementedError

    def _login_via_cdp(self, port: int = 9222) -> Dict[str, str]:
        """Connect to a Chrome that the user started with
        --remote-debugging-port=<port>, open LOGIN_URL, wait until
        _is_logged_in() is true, and return the cookies. Unlike
        _login_interactively(), Playwright does not start the browser. May
        raise an ImportError if Playwright is missing."""
        if sync_playwright is None:
            raise ImportError("Package playwright is not available. Install "
                              "`pip install playwright`.") from None
        # TODO: pw.chromium.connect_over_cdp(f'http://localhost:{port}'), use
        # the first context, and share the wait loop with
        # _login_interactively().
        raise NotImplementedError

    def _is_logged_in(self, cookies: Dict[str, str], url: str) -> bool:
        """Return True if the login is complete. Default: all
        LOGIN_COOKIE_NAMES are present and the page is no longer LOGIN_URL.
        Subclasses may override this, e.g. for a login with multiple
        forms."""
        return (all(name in cookies for name in self.LOGIN_COOKIE_NAMES)
                and not url.startswith(self.LOGIN_URL))

    def _launch_browser(self, pw: Any) -> Any:
        """Launch a visible browser of type self.browser and return it."""
        if self.browser in ('chrome', 'msedge'):
            return pw.chromium.launch(channel=self.browser, headless=False)
        # raises AttributeError if self.browser is not a Playwright browser
        # type, e.g. 'opera'
        return getattr(pw, self.browser).launch(headless=False)

    def _read_cookies(self, context: Any,
                      previous: Dict[str, str]) -> Dict[str, str]:
        """Return the browser cookies that apply to LOGIN_URL, or previous if
        the browser can no longer be read."""
        try:
            return {c['name']: c['value']
                    for c in context.cookies(self.LOGIN_URL)}
        except PlaywrightError:
            return previous


class HumbleBundleLogin(BrowserLogin):
    """Login to Humble Bundle."""
    NAME = 'humblebundle'
    LOGIN_URL = 'https://auth.humblebundle.com/login'
    LOGIN_COOKIE_NAMES = ('ory_session_stoiccohens510ht9xek',)
    COOKIE_DOMAIN = '.humblebundle.com'


class SteamLogin(BrowserLogin):
    """Login to Steam."""
    # TODO: verify these constants
    NAME = 'steam'
    LOGIN_URL = 'https://steamcommunity.com/login/home/'
    LOGIN_COOKIE_NAMES = ('steamLoginSecure',)
    COOKIE_DOMAIN = 'steamcommunity.com'
