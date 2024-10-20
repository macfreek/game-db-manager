#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Interface to Steam"""

import logging

# Type hints
try:
    from typing import Dict, Any
except ImportError:
    from collections import defaultdict
    Dict = defaultdict(str)  # type: ignore
    Any = ''  # type: ignore

# local module (only used here for type hints)
from downloader import CachedDownloader


class Steam:
    FULL_GAME_LIST_URL = 'https://api.steampowered.com/ISteamApps/GetAppList/v2/'
    OWNED_GAME_LIST_1_URL = 'https://api.steampowered.com/IPlayerService/GetOwnedGames/v0001/' \
        '?key={apikey}&steamid={userid}&include_appinfo=1&include_played_free_games=1&format=json'
    OWNED_GAME_LIST_2_URL = 'https://steamcommunity.com/id/{username}/games?tab=all&xml=1'
    GAME_INFO_URL = 'https://store.steampowered.com/api/appdetails?appids={appid}'
    
    def __init__(self, apikey: str, steamid: int, steamusername: str, downloader: CachedDownloader) -> None:
        self.apikey = apikey
        self.steamid = steamid
        self.steamusername = steamusername
        self.downloader = downloader

    def get_all_ids(self) -> Dict[int, str]:
        """Return a dict with steam ID -> name for all available games."""
        j = self.downloader.get_cached_json(self.FULL_GAME_LIST_URL,
                'steam_applist.json', ttl=3)
        steamids = {}  # type: Dict[int, str]
        for d in j['applist']['apps']:
            try:
                if int(d['appid']) in steamids:
                    logging.warning("Duplicate ID %s: %s and %s" % \
                            (d['appid'], steamids[int(d['appid'])], d['name']))
                else:
                    steamids[int(d['appid'])] = d['name']
            except KeyError:
                logging.warning("Unexpected JSON format. Expected {'appid': ..., 'name': ...}. " \
                          "Found %s" % d)
                continue
        return steamids

    def get_appdata(self, appid: int) -> Dict[str, Any]:
        """Given a Steam ID, return the data. It will follow any symlink. 
        May raise a KeyError if no data is found.
        May raise a IOError if no data can be downloaded."""
        j = self.downloader.get_cached_json(self.GAME_INFO_URL.format(appid=appid),
                'steam_appdetails_%s.json' % (appid), ttl=20, may_extend_cache=True)
        try:
            j = j[str(appid)]
            success = j['success']
        except KeyError:
            logging.warning(("Unexpected JSON format in api/appdetails?appids=%s. "
                    "Expected {'%s': {'success': ...}...}") % (appid, appid))
            raise KeyError("JSON error while getting data for Steam ID %s" % (appid))
        if not success:
            logging.info("Can't get information for Steam ID %s" % (appid))
            raise KeyError("No data available for Steam ID %s" % (appid))
        try:
            masterid = j['data']['steam_appid']
        except KeyError:
            logging.warning("Unexpected JSON format in api/appdetails?appids=%s. " \
                    "Expected {'%s': {'data': {'steam_appid': ...}...}...}" % (appid, appid))
            raise KeyError("JSON error while getting data for Steam ID %s" % (appid))
        if int(masterid) != int(appid):
            return self.get_appdata(int(masterid))
        else:
            return j['data']

    def get_userapps(self, userid: int=None) -> Dict[int, Dict[str, Any]]:
        """Return a dict of appid->dict with information about all games that a users owns.
        Use the private API. Somehow, this does not return all owned games."""
        if not userid:
            userid = self.steamid
        j = self.downloader.get_cached_json(
                self.OWNED_GAME_LIST_1_URL.format(apikey=self.apikey, userid=userid),
                'steam_ownedgames_%s.json' % (userid), ttl=1.2)
        games = {}
        try:
            for game in j['response']['games']:
                assert isinstance(game['appid'], int)
                games[game['appid']] = game
        except KeyError:
            logging.error("Unexpected JSON format in GetOwnedGames. " \
                    "Expected {'response': {'games': {'appid': ..., ...}...}}")
            raise KeyError("JSON error while getting data for User ID %s" % (userid))
        return games

    def get_userapps_public(self, userid: int=None, username: str=None) -> Dict[int, Dict[str, Any]]:
        """Return a dict of appid->dict with information about all games that a users owns.
        Use the public API. Somehow this returns more games, but requires a user to make their Steam collection public-readable."""
        if not userid:
            userid = self.steamid
        if not username:
            username = self.steamusername
        url = self.OWNED_GAME_LIST_2_URL.format(apikey=self.apikey, username=username)
        cachefile = 'steam_ownedgames_%s.xml' % (username)
        x = self.downloader.get_cached_xml(url, cachefile, ttl=1.2)
        err = x.find('error')
        if err is not None: # Element may exist but still resolve to False.
            logging.warning("Unexpected Error while reading %s (%s): %s" % (cachefile, url, err.text))
            raise KeyError("Unexpected XML response in steamcommunity.com/id/%s/games" % (username))
        try:
            userid_from_username = int(x.find('steamID64').text)
        except Exception as exc:
            print(x)
            print(err)
            print(exc)
        if userid_from_username != userid:
            logging.error("Steam ID mismatch. " \
                    "Steam username %s returns Steam ID %d, not %d." % (username, userid_from_username, userid))
            raise KeyError("Mismatch between Steam ID %d and username %s" % (userid, username))
        games = {}
        try:
            for game in x.find('games'):
                appid = int(game.find('appID').text)
                name = game.find('name').text
                img_logo_url = game.find('logo').text
                games[appid] = {'appid': appid, 'name': name, 'img_logo_url': img_logo_url}
        except KeyError:
            logging.error("Unexpected XML format in steamcommunity.com/id/<username>/games. " \
                    "Expected <gamesList><games><game><appID>...</appID><name>...</name><logo>...</logo></game> .... </games></gameList>")
            raise KeyError("XML error while getting data for username %s" % (username))
        return games
