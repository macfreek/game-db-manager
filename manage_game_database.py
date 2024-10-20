#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Game Database Manager. Store game purchases in local FileMaker database.

Requirements:
port install py-pyodbc  (also installs unixODBC)
install Filemaker ODBC, can be downloaded from http://www.filemaker.com/support/downloads/
"""
import sys
import re
from datetime import date, datetime, timedelta
import logging
import argparse
import difflib
from pathlib import Path
from collections import OrderedDict, defaultdict

# external packages
try:
    import configargparse
except ImportError:
    raise ImportError("Package ConfigArgParse is not available. Install using e.g. "
            "`port install py-configargparse` or `pip install ConfigArgParse`.") from None

# local modules
from filemaker import FileMaker
from downloader import CachedDownloader
from steam import Steam
from humblebundle import HumbleBundle

# Type hints
try:
    from typing import Dict, List, TextIO, Union, Any, Container
except ImportError:
    # backward compatible with Python 3.4
    from collections import defaultdict
    List = Dict = Union = defaultdict(str)  # type: ignore
    TextIO = Any = ''  # type: ignore
URL = str


def print_dict(data: Dict, keys: Container, ignore_keys: Container=(), indent=0, chain=[], output: TextIO=sys.stdout):
    for key, value in data.items():
        if key in ignore_keys:
            continue
        elif isinstance(value, dict):
            print_dict(value, keys, ignore_keys, indent + 1, chain + [key], output)
        elif isinstance(value, list):
            for no, item in enumerate(value):
                if isinstance(item, dict):
                    print_dict(item, keys, ignore_keys, indent + 1, chain + [key, str(no)], output)
        elif key in keys:
            if value:
                keyname = '.'.join(chain + [key])
                output.write(indent * '  ' + keyname + ': ' + str(value).strip() + '\n')

def find_possible_matches(*names: str, name_values: Dict[str, List[Any]], cutoff: float=0.8) -> List[Any]:
    """Find the most likely steam IDs, give a list of possible names. 
    Returns a list with possible ids."""
    for name in names:
        if name in name_values:
            logging.debug("Exact Match %s -> %s -> %s" % (names[0], name, name_values[name]))
            return name_values[name]
    for name in names:
        # ignore type hints of difflib.get_close_matches [https://github.com/python/typeshed#2063]
        matches = []  # type: List[str]
        matches = difflib.get_close_matches(name,  # type: ignore
                            name_values, n=100, cutoff=cutoff)
        if matches:
            options = []
            for match in matches:  # type: str
                # logging.debug("Fuzzy Match %s -> %s -> %s" % (name, match, name_values[match]))
                options.extend(name_values[match])
            logging.debug("Fuzzy Match %s -> %s -> %s" % (names[0], matches, options))
            return options
    logging.debug("No Match %s -> None" % (names[0]))
    return []

def find_missing_steamids(steam: Steam, database: FileMaker, all_games=False, dry_run=False, strict_name_check=False) -> None:
    """Look for all records where the distributor is Steam, but without a Steam ID.
    Try to find these games"""
    steamids = steam.get_all_ids()
    # reverse ID -> Name to Name -> [ID] (assume there may be duplicates)
    steamnames_id = {}  # type: Dict[str, List[int]]
    for k,v in steamids.items():
        try:
            steamnames_id[v].extend([k])
        except KeyError:
            steamnames_id[v] = [k]
    # steamnames = steamnames_id.keys()
    logging.info("Query database for Purchases with missing SteamAppID")
    
    fields = ('Name', 'Parent', 'AppType', 'GameIdentifier', 'SteamAppId', 'Note')
    where = "SteamAppID IS NULL AND AppType='Game'"
    if not all_games:
        where += " AND Distribution LIKE '%Steam%'"
    changecount = 0
    gamecount = 0
    cutoff = 1.0 if strict_name_check else 0.8
    for record in database.select(fields, 'Purchases', where):
        gamecount += 1
        name = record['Name']
        alias = record['GameIdentifier']
        names = [name]
        if alias and alias != name:
            names.append(alias)
        if record['Note']:
            m = re.search(r'known as (.*?)(\.|$)', record['Note'], flags=re.IGNORECASE)
            if m:
                names.append(m.group(1))
        possible_ids = find_possible_matches(*names, 
                name_values=steamnames_id, cutoff=cutoff)  # type: List[int]
        # Check if the found IDs are valid
        possible_ids_copy = possible_ids[:]
        non_steam_ids = []
        for appid in possible_ids_copy:
            try:
                steamdata = steam.get_appdata(appid)
                masterid = steamdata["steam_appid"]
                if steamdata["type"] == "demo":
                    logging.debug("Remove Steam ID %s for demo game" % (masterid))
                    masterid = None
            except KeyError:
                masterid = None
            if masterid != appid:
                possible_ids.remove(appid)
                if masterid is None:
                    non_steam_ids.append(appid)
                elif masterid not in possible_ids:
                    logging.debug("Replace Steam ID %s with %s" % (appid, masterid))
                    possible_ids.append(masterid)
            if non_steam_ids and not possible_ids:
                # All that is left are steam IDs without further information
                logging.warning("No information found for %s (IDs %s)" % (name, non_steam_ids))
                # possible_ids = non_steam_ids
        if not possible_ids:
            print("No steam ID found for %s / %s" % (name, alias))
        elif len(possible_ids) > 1:
            print("Multiple possible steam ID found for %s / %s" % (name, alias))
            for id in possible_ids:
                print("       %s: %s   http://store.steampowered.com/app/%s/" % \
                        (id, steamids[id], id))
                # print(world)
        elif len(possible_ids) == 1:
            steamid = possible_ids[0]
            found_name = ''
            found_date = ''
            try:
                j = steam.get_appdata(steamid)
                found_name = j['name']
                found_date = j['release_date']['date']
            except KeyError:
                pass
            print("Steam ID found for %s : %s  [%s (%s)]" % \
                    (name, possible_ids[0], found_name, found_date))
            
            uwhere = {
                'SteamAppID': None,
                'Name': name,
                'GameIdentifier': alias,
                'AppType': record['AppType']
            }
            update = {'SteamAppId': steamid}
            if not dry_run:
                rowcount = database.update('Purchases', uwhere, update)
                if rowcount != 1:
                    logging.info("Updated (but not committed) %s records" % (rowcount))
                changecount += rowcount
        else:
            # all possible cases of len(possible_ids) should be caught (0, 1, >1)
            raise AssertionError("len(possible_ids) = %s" % (len(possible_ids)))
        # if changecount > 500:
        #     break
    logging.info("Found %s games without Steam App ID" % (gamecount))
    if gamecount > 0:
        logging.info("Made %s updates to the database" % (changecount))
    if changecount > 0:
        database.commit()
        logging.info("Committed all updates to the database")

def add_steam_images(cachedownload: CachedDownloader, database: FileMaker):
    """For games with a steam ID but without an images,
    add the image from Steam."""
    steamapps = steam.get_userapps()
    
    IMAGE_URL = 'http://cdn.akamai.steamstatic.com/steam/apps/{SteamAppId:d}/capsule_184x69.jpg'
    IMAGE_PATH = 'steam_{SteamAppId:d}.jpg'
    
    logging.info("Query database for Purchases")
    # fields = ('Name', 'AppType', 'SteamAppId', "GetAs(Image, 'JPEG') AS Image")
    fields = ('SteamAppId', )
    where = 'SteamAppId IS NOT NULL AND Image IS NULL'
    games = database.select(fields, 'Purchases', where)
    steamids = [int(record['SteamAppId']) for record in games]  # type: List[int]
    
    totalcount = len(steamids)
    steamids = list(set(steamids))  # remove duplicates
    logging.info("Found %d Games with %d missing image" % (totalcount, len(steamids)))
    
    updatecount = 0
    downloadcount = 0
    exception_count = 0
    skip_updates = False
    for steamid in steamids:
        image_url = IMAGE_URL.format(SteamAppId=steamid)
        image_path = IMAGE_PATH.format(SteamAppId=steamid)
        try:
            image = cachedownload.get_cached_binary(image_url, image_path, ttl=100)
        except ConnectionError as e:
            logging.error(str(e))
            continue
        
        # TODO: remove loosy images, so they are not imported in the database
        
        if len(image) < 4000:
            logging.warning("Poor image %s: only %d bytes" % (image_path, len(image)))
            continue
        downloadcount += 1
        uwhere = {
            'Image': None,
            'SteamAppId': steamid
        }
        update = {"PutAs(Image, 'JPEG')": image}
        print("Found Image for Steam ID %d: %s" % (steamid, image_url))
        if not skip_updates:
            try:
                rowcount = database.update('Games', uwhere, update)
                if rowcount != 1:
                    logging.info("Updated (but not committed) %s records" % (rowcount))
                updatecount += rowcount
            except Exception:
                exception_count += 1
                if exception_count > 2:
                    logging.error("Updating containers fails repeatedly. "
                            "This may be a limitation with FileMaker and ODBC. "
                            "Skipping further updates (only downloading images).")
                    skip_updates = True
    logging.info("Updated %d games without Image" % (updatecount))
    logging.info("Downloaded %d Images" % (downloadcount))
    if updatecount > 0 and not skip_updates:
        database.commit()
        logging.info("Committed all updates to the database")
    if skip_updates:
        print("Please manually update images from FileMaker. Use the following script:")
        print("To be written")  # TODO

def verify_steamids(steam: Steam, database: FileMaker):
    """Verify that the SteamIDs listed here are also present in my Steam account.
    If not, either the Steam ID is wrong, I decided to give them away, or I have 
    not claimed it.
    
    Also verify that all Steam games I own are listed in the database. 
    Ignores all DLC (since I don't list all DLC in my database.)"""
    steamapps = steam.get_userapps()
    steamapps_public = steam.get_userapps_public()
    
    public_not_in_private = set(steamapps_public.keys()) - set(steamapps.keys())
    private_not_in_public = set(steamapps.keys()) - set(steamapps_public.keys())
    for steamid in sorted(public_not_in_private):
        logging.info("Steam ID %d [%s] listed in Steam public list, but not in Steam API." % \
                    (steamid, steamapps_public[steamid]['name']))
    for steamid in sorted(private_not_in_public):
        logging.info("Steam ID %d [%s] listed in Steam API, but not in Steam public list." % \
                    (steamid, steamapps[steamid]['name']))
    
    logging.info("Query database for Purchases")
    fields = ('Name', 'AppType', 'SteamAppId', 'Distribution', 'Gift', 'PriceType', 'StoreURL')
    steamid_in_db = set()
    # Loop through all steam IDs in the FileMaker database, 
    # and check if they are actually in steam.
    for game in database.select(fields, 'Purchases'):
        steamdistrib = bool(game['Distribution']) and ("steam" in game['Distribution'].lower())
        isgift = game['Gift'] in ("Ungifted gift", "Given away")
        if game['SteamAppId']:
            steamid = int(game['SteamAppId'])
            steamid_in_db.add(steamid)
        else:
            if steamdistrib:
                if game['StoreURL'] is None or not ( \
                        re.search(r'store\.steampowered\.com/(bundle|sub)', game['StoreURL']) or \
                        re.search(r'(store\.steampowered\.com/app.*){2}', game['StoreURL'])):
                    print("Game '%s': Steam distributed, no Steam ID, and no valid Store URL: %s" \
                            % (game["Name"], game['StoreURL']))
            continue 
        try:
            steamtype = steam.get_appdata(steamid)['type']
        except KeyError:
            steamtype = 'removed'
        
        if steamtype not in ('game', 'dlc', 'removed') and game['AppType'] != 'Media':
                print("%s \'%s\' with Steam ID %s has type %s on Steam. " \
                        "Likely a wrong Steam ID." % \
                        (game['AppType'], game["Name"], steamid, steamtype))
        elif game['AppType'] == 'Game' and steamtype == 'dlc' and game['PriceType'] != 'Freemium':
            print(("%s \'%s\' with Steam ID %s has type DLC on Steam, and is a non-Freemium " \
                    "game in the database. Is it a game or DLC?") % \
                    (game['AppType'], game["Name"], steamid))
            # Other way around: DLC in database and Game in Steam is most likely a case where
            # the database contains the ID of the parent Game (perhaps the DLC does not have 
            # it's own ID).
        elif (game['AppType'] == 'Bundle' and steamtype == 'dlc') or \
                (game['AppType'] == 'Media' and steamtype not in ('series',)) or \
                game['AppType'] not in ('Game', 'DLC', 'Bundle', 'Media'):
            print("%s \'%s\' with Steam ID %s has type %s on Steam. Is this correct?" % \
                    (game['AppType'], game["Name"], steamid, steamtype))
        
        if steamdistrib and steamtype != 'removed' and not isgift and steamid not in steamapps \
                and steamid not in steamapps_public and steamtype != 'dlc':
            try:
                steamappname = steam.get_appdata(steamid)['name']
            except KeyError:
                steamappname = 'removed'
            print("%s \'%s\' with Steam ID %s [%s] in database, is not redeemed on Steam." % \
                    (game['AppType'], game["Name"], steamid, steamappname))

    # Loop through steam ids and see if they are in database
    for steamid in steamapps:
        if steamid not in steamid_in_db:
            try:
                steamdata = steam.get_appdata(steamid)
            except KeyError:
                steamdata = {'type': 'unknown', 'is_free': False}
            if steamdata['type'] == 'game' and not steamdata['is_free']:
                print("%s \'%s\' (ID %s) in Steam account, but not in database." % \
                        (steamdata['type'].capitalize(), steamapps[steamid]['name'], steamid))
    # database.close()

def humble_game_platforms(game) -> List[str]:
    """Given a JSON subproduct structure, return the platforms.
    Note that for Steam, the platforms are not listed."""
    if 'downloads' in game:
        return [distribution['platform'] for distribution in game['downloads']]
    elif game.get('platforms'):
        # TODO: what is the format of game['platforms']?
        assert isintance(game['platforms'], list)
        return game['platforms']
    else:
        logging.warning("No platform found for game {}".format(game['human_name']))

def print_humble_purchases(humble: HumbleBundle, database: FileMaker, verbosity=0):
    """For each Humble Purchase, print the details."""
    order_list = humble.get_order_list()
    for order in order_list:
        order_details = humble.get_order_info(order)
        # category may be bundle (game bundle, book bundle, etc.)
        sys.stdout.write(order + '\n')
        if verbosity == 0:
            include = ('category', 'human_name')
            ignores = ('all_coupon_data', 'downloads', 'payee')
        elif verbosity == 1:
            include = ('category', 'machine_name', 'human_name', 'platform', 'created')
            ignores = ('all_coupon_data', 'payee')
        else:  # verbosity if 2 or more
            include = ('category', 'machine_name', 'human_name', 'key_type', 'platform', \
                       'gamekey', 'created', 'file_size', 'platform', 'platforms', 'available')
            ignores = ('all_coupon_data')
        print_dict(order_details, include, ignores, indent=1)

def get_games_in_humble_order(order_details):
    """Given a (JSON) data structure, yield for each game a dict with keys:
    order, must_include, distribution, machine_name, human_name, platforms (may be empty)
    log debug messages for out-of-the ordinary data structures.
    The original order_details is augmented with a key order_details['has_expired_game']
    """
    DESKTOP_PLATFORMS = set(['mac','windows','linux'])
    NON_DESKTOP_PLATFORMS = set(['asmjs','android'])
    NON_GAME_PLATFORMS = set(['audio','video','ebook'])
    KNOWN_PLATFORMS = DESKTOP_PLATFORMS | NON_DESKTOP_PLATFORMS | NON_GAME_PLATFORMS
    order = order_details['gamekey']
    if order_details['subproducts'] or order_details['tpkd_dict']['all_tpks']:
        # An order with downloads and/or (steam) keys
        games = order_details['subproducts'] + order_details['tpkd_dict']['all_tpks']
    elif order_details['product']['category'] == 'subscriptionplan':
        return
    else:
        # an order without downloads, perhaps only coupons.
        # assume the order is the game itself (uncommon)
        # Log debug message, and keep track of these 'empty' orders.
        logging.debug("Assume Humble Bundle order {} is about a single game".format([order]))
        games = [order_details['product']]
        # games = []
        ## empty_orders.add(order)
    
    if 'has_expired_game' not in order_details:
        order_details['has_expired_game'] = False
    for game in games:
        # skip books, audio, video
        # process non-desktop games, but mark them with must_include = False
        # If must_include is True, the game must be in the database.
        # If must_include is True, the game may be in the database.
        must_include = True
        distribution = []
        if game.get('expiration_date'):
            order_details['has_expired_game'] = True
        if 'downloads' in game:
            distribution = 'humblebundle' # Humble Bundle is the distributor
            platforms = set([distribution['platform'] for distribution in game['downloads']])
            for platform in (platforms - KNOWN_PLATFORMS):
                logging.warning("Unknown platform {} for game {} in Humble Bundle order {}".format(
                            platform, game['human_name'], order
                ))
            if not platforms:  # game['downloads'] == []
                machine_name = game['machine_name']
                if game.get('custom_download_page_box_html'):
                    if "<span class='merch-countdown'>0</span>" in game['custom_download_page_box_html']:
                        order_details['has_expired_game'] = True
                        logging.debug("Mark Humble Bundle order {} as expired due to expired game {}".format(
                                order, game['human_name']
                        ))
                    else:
                        logging.debug("Ignore non-game {} ({}) in Humble Bundle order {}".format(
                                machine_name, game['human_name'], order
                        ))
                    continue
                elif order_details['tpkd_dict']['all_tpks']:
                    # empty downloads. This is odd, but fairly common. Likely there is an associated tpk for the game.
                    logging.debug("Ignore non-downloadable game {} in Humble Bundle order {} ({}). "
                                "Likely there is an associated Steam key in this order.".format(
                                game['human_name'], order, order_details['product']['human_name']
                    ))
                elif order_details['has_expired_game']:
                    logging.debug("Ignore non-downloadable game {} in Humble Bundle order {} ({}). "
                                "Likely the Steam key is expired.".format(
                                game['human_name'], order, order_details['product']['human_name']
                    ))
                else:
                    logging.warning("Ignore non-downloadable game {} [{}] in Humble Bundle order {} ({}).".format(
                                game['human_name'], game['machine_name'], order, order_details['product']['human_name']
                    ))
                distribution = 'unknown'
                must_include = False
            elif (platforms & DESKTOP_PLATFORMS):
                # regular game
                pass
            elif (platforms & NON_DESKTOP_PLATFORMS):
                logging.debug("Ignore non-desktop ({}) game {} in Humble Bundle order {}".format(
                            ', '.join(platforms), game['human_name'], order
                ))
                must_include = False
            else:
                logging.debug("Ignore non-game ({}) item {} in Humble Bundle order {}".format(
                            ', '.join(platforms), game['human_name'], order
                ))
                continue
        elif 'key_type' in game:
            if game['key_type'] in ('steam', 'desura', 'blizzard', 'gog', 'ouya', 'telltale', 'arenanet'):
                distribution = game['key_type']
            elif game['key_type'] in ('generic', 'external_key'):
                logging.debug("Ignore generic key '{}' in Humble Bundle order {} for {}".format(
                        game['machine_name'], order, game['human_name'])
                )
                distribution = ['unknown']
                must_include = False
            else:
                logging.warning("Ignore unknown {} key '{}' in Humble Bundle order {} for {}".format(
                        game['key_type'], game['machine_name'], order, game['human_name'])
                )
                continue
            platforms = []
        else:
            # Bundle without contents, treated as a (possible) game
            must_include = False
            platforms = []
            distribution = ['unknown']
        game = game.copy()
        game['human_name'] = game['human_name'].strip()
        game['order'] = order
        game['platforms'] = list(platforms)   # mac, windows, linux, etc.
        game['distribution'] = distribution   # humblebundle, steam, unknown, etc.
        game['must_include'] = must_include
        yield game

def merge_games_in_humble_order(game_list):
    """Given a list of games, merge games with equal names."""
    pruned_name_list = []
    game_dict = {}
    for game in game_list:
        game_name = game['human_name']
        if game_name in pruned_name_list:
            same_game = game_dict[game_name]
            # TODO: merge game dict into same_game dict
            assert same_game['order'] == game['order']
            for platform in game['platforms']:
                if platform not in same_game['platforms']:
                    same_game['platforms'].append(platform)
            if game['distribution'] not in same_game['distribution']:
                same_game['distribution'] += ', ' + game['distribution']
            same_game['must_include'] = same_game['must_include'] or game['must_include']
        else:
            pruned_name_list.append(game_name)
            game_dict[game_name] = game
    for game_name in sorted(pruned_name_list):
        yield game_dict[game_name]


def add_humble_purchase(humble: HumbleBundle, database: FileMaker, humbleorder: str):
    """Given a Humble Bundle order, add the purchases to the database.
    Does not check if the information is already there. It just adds it."""
    order_details = humble.get_order_info(humbleorder)
    purchasetime = datetime.strptime(order_details['created'], '%Y-%m-%dT%H:%M:%S.%f')
    dates = [purchasetime.date().isoformat()]
    date = (purchasetime + timedelta(hours=5)).date().isoformat()
    # TODO: to be written
    print(order_details['product']['human_name'])
    print("    category: ", order_details['product']['category'])
    print("    machine_name: ", order_details['product']['machine_name'])
    for game in merge_games_in_humble_order(get_games_in_humble_order(order_details)):
        print(" ", game['human_name'])
        print("      distribution: ", game['distribution'])
        print("      platforms:    ", game['platforms'])
        print("      machine_name: ", game['machine_name'])
    # pprint.pprint(order_details)
    print(80*'-')
    
    #, width=120, indent=4)
    

def get_humble_order_dates(order_details):
    """Give the order details, return 1 or 2 possible dates taken into account that 
    purchase may have been done in a different time-zone."""
    purchasetime = datetime.strptime(order_details['created'], '%Y-%m-%dT%H:%M:%S.%f')
    dates = [purchasetime.date().isoformat()]
    date = (purchasetime + timedelta(hours=5)).date().isoformat()
    if date not in dates:
        dates.append(date)
    return dates

def add_humble_orderids(humble: HumbleBundle, database: FileMaker, add_missing=False, dry_run=False):
    """For all games in the database, add the Humble Bundle order ID if it is not set.
    Matching is done based on date and game name.
    Finally, list all Humble Bundle orders without information in the database.
    If add_missing is True, add these to the database."""
    humble_order_list = humble.get_order_list()
    # orders with games (books are pruned)
    game_orders = set()
    empty_orders = set()
    expired_orders = set()
    # order_list = ['zUDt5EqrxRbwpNMZ', 'ZxbxH8vdpEuRf6em']
    DESKTOP_PLATFORMS = set(['mac','windows','linux'])
    NON_DESKTOP_PLATFORMS = set(['asmjs','android'])
    NON_GAME_PLATFORMS = set(['audio','video','ebook'])
    KNOWN_PLATFORMS = DESKTOP_PLATFORMS | NON_DESKTOP_PLATFORMS | NON_GAME_PLATFORMS
    
    # Build data structure: order_by_date[date][game] = [list of game_details]
    def factory():
        return defaultdict(set)
    order_by_date = defaultdict(factory)  # type: Dict[str, Dict[str, Set[Tuple[str,bool,Dict[str, Any]]]]]
    duplicate_games = defaultdict(factory)  # type: Dict[str, Dict[str, Set[str]]]
    total_game_count = 0
    total_other_count = 0
    for order in humble_order_list:
        game_count = 0
        other_count = 0
        order_details = humble.get_order_info(order)
        order_name = order_details['product']['human_name'].strip()
        order_machine_name = order_details['product']['machine_name']
        dates = get_humble_order_dates(order_details)
        
        if order_details['product']['category'] == 'subscriptionplan':
            continue
        for game in get_games_in_humble_order(order_details):
            if game['must_include']:
                game_count += 1
                total_game_count += 1
            else:
                other_count += 1
                total_other_count += 1
            for date in dates:
                human_name = game['human_name']
                if order_by_date[date][human_name]:
                    o_game = order_by_date[date][human_name]
                    if o_game['order_id'] != order:
                        duplicate_games[date][human_name].add(o_game['order_id'])
                        duplicate_games[date][human_name].add(order)
                    # same order, same date, same human_name.
                    game['distribution'] += o_game['distribution']
                    game['must_include'] = game['must_include'] or o_game['must_include']
                game['order_id'] = order
                order_by_date[date][human_name] = game

                # order_by_date[date][game['human_name']].add((order, is_game, game))
        if order_details['has_expired_game']:
            expired_orders.add(order)
            logging.debug("Mark Humble Bundle order {} as expired".format(order))
        if game_count:
            game_orders.add(order)
        elif order_machine_name.endswith('_bookbundle') or order_machine_name.endswith('_bookrebundle'):
            logging.debug("Humble Bundle order %s of %s (%s) is book bundle without games" % (order, date, order_name))
        elif order_machine_name.endswith('_softwarebundle'):
            logging.debug("Humble Bundle order %s of %s (%s) is software bundle without games" % (order, date, order_name))
        elif order_details['has_expired_game']:
            logging.warning("Humble Bundle order %s of %s (%s) has no games because it is expired" % (order, date, order_name))
        elif other_count:
            logging.warning("Humble Bundle order %s of %s (%s) has no games" % (order, date, order_name))
            empty_orders.add(order)
        else:
            logging.warning("Humble Bundle order %s of %s (%s) has no games nor keys" % (order, date, order_name))
            empty_orders.add(order)
    
    logging.info('Found %d games or game keys and %d other items in Humble Bundle purchases.' % 
                (total_game_count, total_other_count))
    
    for date in sorted(duplicate_games.keys()):
        for game, orders in duplicate_games[date].items():
            logging.warning("Multiple purchases of %s on %s: Humble bundle orders %s" % \
                        (game, date, ', '.join(list(orders))))
    # order_by_date is now filled
    
    # Loop all Humble Bundle games in the database, and determine the HumbleOrder, if not set
    logging.info("Query database for Purchases")
    fields = ('Name', 'AppType', 'SteamAppId', 'HumbleSlug', 'HumbleOrder', 'Distribution',
              'Gift', 'PriceType', 'StoreURL', 'PurchaseDate', 'Bundle', 'GameIdentifier', 'Note')
    where = "Vendor LIKE '%Humble Bundle%' OR Distribution LIKE '%Humble Bundle%'"
    order = "PurchaseDate"
    seen_orders = set()
    changecount = 0
    missing_order_count = 0
    gamecount = 0
    for record in database.select(fields, 'Purchases', where, order):  # type: Dict[str, Any]
        name = record['Name']
        alias = record['GameIdentifier']
        names = [name]
        if alias and alias != name:
            names.append(alias)
        if record['Note']:
            m = re.search(r'known as (.*?)(\.|$)', record['Note'], flags=re.IGNORECASE)
            if m:
                names.append(m.group(1))

        acquire_date = record['PurchaseDate'].isoformat()
        # print(type(acquire_date), acquire_date)
        if acquire_date in order_by_date:
            game_purchases = {name: [order['order_id']] for name, order in order_by_date[acquire_date].items()}
            possible_ids = find_possible_matches(*names, name_values=game_purchases, cutoff=0.8)
            possible_ids = set(possible_ids)  # remove duplicates
        else:
            # logging.warning("No Humble Bundle purchases found on %s" % (acquire_date))
            possible_ids = set()
        gamecount += 1
        orders_in_database = [humbleorder.strip() for humbleorder in record['HumbleOrder'].split(',')] \
                    if record['HumbleOrder'] else []
        for humbleorder in orders_in_database:
            seen_orders.add(humbleorder)
        s_orders_in_database = set(orders_in_database)
        if not orders_in_database:
            missing_order_count += 1
        
        # We now have two sets: s_orders_in_database and possible_ids (=in_purchases).
        # Take the following actions, based on these sets, and their overlap.
        # 
        #   in_database      in_database     not in_database
        #  not in_purchases  in_purchases    in_purchases
        #       ∅                ∅               ∅        --- Warning: No known Humble Bundle for purchase of %s on %s
        #       ∅                *               ∅        --- Perfect match, continue!
        #       ∅                *               *        --- Warning: Additional IDs found. Don't add to database
        #       *                *               *        --- Very likely a mistake. give warning: wrong order id.
        #       *                ∅               *        --- Very likely a mistake. give warning: wrong order id.
        #       *                *               ∅        --- Check if order ID is known, and if so, if purchase date matches.
        #       *                ∅               ∅        --- Check if order ID is known, and if so, if purchase date matches.
        #       ∅                ∅               *        --- Found it! Add to database, if exactly 1 ID was found.

        if not possible_ids and not orders_in_database:
            # nothing on record, also nothing found in purchases
            logging.warning("No Humble Bundle order found for purchase of %s on %s." % 
                     (name, acquire_date))
        elif possible_ids == s_orders_in_database:
            continue
        elif orders_in_database:
            if not (s_orders_in_database - possible_ids):
                # Additional IDs found. Don't add to database
                logging.warning("Purchase of %s on %s lists Humble Order(s) %s, found additional order(s) in purchases: %s" %
                        (name, acquire_date, ', '.join(orders_in_database), ', '.join(possible_ids - s_orders_in_database)))
            elif (possible_ids - s_orders_in_database):
                # Orders found in purchases, which are not in database. 
                # Also: orders in database, not found in purchases.
                logging.warning("Purchase of %s on %s lists Humble Order(s) %s, found different order(s) in purchases: %s" %
                        (name, acquire_date, ', '.join(orders_in_database), ', '.join(possible_ids - s_orders_in_database)))
            else:
                for order_id in (s_orders_in_database - possible_ids):
                    # Order ID in database, but nothing found by that name and date.
                    # Check if order ID is known, and if so, if purchase date matches.
                    if order_id not in (game_orders | empty_orders | expired_orders):
                        if record['Gift'] == 'Given away':
                            # Given away orders are not listed anymore.
                            expired_orders.add(order_id)
                        else:
                            logging.warning("Purchase of %s on %s lists Humble Order %s, which is not a known purchases order ID." %
                                     (name, acquire_date, order_id))
                    else:
                        order_details = humble.get_order_info(order_id)
                        order_dates = get_humble_order_dates(order_details)
                        if acquire_date not in order_dates:
                            logging.warning("Purchase of %s on %s lists Humble Order %s, but that order was made on %s" %
                             (name, acquire_date, order_id, order_dates[0]))
        
        # no orders_in_database, check what we found.
        elif len(possible_ids) > 1:
            logging.warning("Multiple possible purchases on %s for %s" % (acquire_date, name))
            continue
        else:
            assert len(possible_ids) == 1
            humbleorder = list(possible_ids)[0]
            print("Humble Bundle purchase found on %s for %s: %s" % \
                    (acquire_date, name, humbleorder))
            
            uwhere = {
                'HumbleOrder': None,
                'Name': name,
                'GameIdentifier': alias,
                'AppType': record['AppType'],
                'PurchaseDate': acquire_date
            }
            update = {'HumbleOrder': humbleorder}
            if not dry_run:
                rowcount = database.update('Purchases', uwhere, update)
                if rowcount != 1:
                    logging.info("Updated (but not committed) %s records" % (rowcount))
                changecount += rowcount
    
    # done looping over all games in the database
    # now report the results
    logging.info("Found %s games without Humble Bundle purchase" % (missing_order_count))
    if gamecount > 0:
        logging.info("Made %s updates to the database" % (changecount))
    if changecount > 0:
        database.commit()
        logging.info("Committed all updates to the database")
    
    def sorted_order_details(order_id_iterable):
        orderlist = [humble.get_order_info(order) for order in order_id_iterable]
        orderlist.sort(key=lambda order_details: order_details['created'])
        return orderlist
    # loop over remaining orders for orders
    for order_id in (seen_orders - game_orders - empty_orders - expired_orders):
        logging.warning("Unknown Humble Bundle order %r in database" % (order_id, ))
    for order_details in sorted_order_details(empty_orders - seen_orders):
        order_id = order_details['gamekey']
        order_name = order_details['product']['human_name'].strip()
        logging.warning("Humble Bundle order %s (%s) is not in the database, and has no content." % \
                        (order_id, order_name))
    for order_details in sorted_order_details(game_orders - seen_orders - expired_orders):
        order_id = order_details['gamekey']
        order_name = order_details['product']['human_name'].strip()
        purchasetime = datetime.strptime(order_details['created'], '%Y-%m-%dT%H:%M:%S.%f')
        date = purchasetime.date().isoformat()
        if add_missing:
            logging.warning("Humble Bundle order %s of %s (%s) is not in database" % (order_id, date, order_name))
            ## add_humble_purchase(humble, database, order)
        else:
            # might be a book bundle...
            logging.warning("Humble Bundle order %s of %s (%s) is not in database" % (order_id, date, order_name))


def verify_humble_purchases(humble: HumbleBundle, database: FileMaker):
    """For each Humble Purchase, check if it is complete."""
    # Things to check:
    # Vendor (must include "Humble Bundle", not "humblebundle")
    # Distribution (also lesser knowns such as Gog, Desura, Telltale)
    # Bundle
    # Purchase Date
    # (total) price
    # Supported platforms
    # missing games in database
    # too many games listed
    # Extensions
    # Gift status
    
    
    order_list = humble.get_order_list()
    # order_list = ['zUDt5EqrxRbwpNMZ', 'ZxbxH8vdpEuRf6em']
    for order in order_list:
        order_details = humble.get_order_info(order)
        # category may be bundle (game bundle, book bundle, etc.)
        # 
        sys.stdout.write(order + '\n')
        # print_dict(order_details, ('category', 'machine_name'),
        #         ('downloads', 'payee', 'all_coupon_data'))
        print_dict(order_details, ('category', 'machine_name', 'human_name', 'key_type', 'platform', 'gamekey', 'created', 'file_size'), ('all_coupon_data'))
        # print_dict(order_details, ('category', 'key_type'), ('downloads', 'all_coupon_data'))
        # print(order_details['product']['category'], order_details['product']['machine_name'],
        #         order)
        # print(order, order_details['claimed'], order_details['is_giftee'],
        #         order_details['product']['partial_gift_enabled'])

def find_missing_gog_ids():
    pass

def find_missing_wikidata_ids():
    pass

def print_gift_list(database: FileMaker, format='mediawiki', output: TextIO=sys.stdout):
    """Print a list of games that I still like to give away."""
    fields = ('Name','DLC','AppType','Parent','Distribution','Platforms','Note','Price', \
            'SteamAppId','StoreURL')
    where = "Gift = 'Ungifted gift'"
    order = "GameIdentifier"
    games = []
    for game in database.select(fields, 'Purchases', where, order):  # type: Dict[str, Any]
        # Make modifications for giving away.
        game['Description'] = '<b>' + game['Name'] + '</b>'
        if game['AppType'] == 'DLC':
            if game['Parent']:
                game['Description'] += '. <em>Requires ' + game['Parent'] + ' base game!</em>'
            else:
                game['Description'] += '. <em>Requires base game!</em>'
        if game['DLC']:
            game['Description'] += ' <small>(including ' + game['DLC'] + ')</small>'
        if game["Note"] is None:
            game["Note"] = ''
        if game['SteamAppId'] is None:
            logging.error("Game %s has no knonw Steam ID" % (game['Name']))
            continue
        game['SteamAppId'] = int(game['SteamAppId'])
        if re.search(r'only give( \w+)? steam( \w+)? (code|key)', game['Note'], flags=re.IGNORECASE):
            game['Distribution'] = 'Steam'
        distribution_urls = {}  # type: Dict[str, List[str]]   # gamename -> list of urls
        distribution_urls = {distrib.strip(): [] for distrib in game['Distribution'].split(',')}
        distrib_uncommon = [distrib for distrib in distribution_urls.keys() \
                if distrib not in ('Steam', 'Gog', 'Humble Bundle')]
        if not game['StoreURL']:
            game['StoreURL'] = "http://store.steampowered.com/app/%s/" % (int(game['SteamAppId']))
        for url in game['StoreURL'].split(','):
            url = url.strip()
            if "steampowered.com" in url:
                destination = "Steam"
            elif "gog.com" in url:
                destination = "Gog"
            elif "humblebundle.com" in url:
                destination = "Humble Bundle"
            elif len(distrib_uncommon) == 1:
                destination = distrib_uncommon[0]
            else:
                destination = "Store"
            if destination in distribution_urls:
                distribution_urls[destination].append(url)
            else:
                logging.info('%s: Ignore store URL %s' % (game['Name'], url))
        # flatten distribution_urls
        # distribution = [list of (storename, url)]
        game['DistributionURLs'] = []
        for distrib, urls in distribution_urls.items():
            if urls:
                for url in urls:
                    game['DistributionURLs'].append((distrib, url))
            else:
                game['DistributionURLs'].append((distrib, None))
        try:
            if float(game['Price']) > 0:
                game['bought'] = '*'
            else:
                game['bought'] = ''
        except ValueError:
            game['bought'] = ''
        games.append(game)
    
    fields2 = ('Name', 'DLC', 'AppType')
    where = "Gift = 'Given away'"
    order = "GameIdentifier"
    given_games = list(database.select(fields2, 'Purchases', where, order))
    for game in given_games:
        game['Description'] = game['Name']
        if game['AppType'] == 'Bundle' and game['DLC']:
            game['Description'] += ' (' + game['DLC'] + ')'
    

    output.write('== Giveaway of Duplicates ==\n')
    output.write('\n')
    output.write("Like every collection, I have duplicates, mostly because I "
        "bought a couple of [https://www.humblebundle.com/ Humble Bundles]. "
        "If you are interested in any of the computer games listed below, "
        "please contact me, and I'll give them away to you for free. "
        "Nearly all distribution is done by Humble Bundle, Steam, or Gog, "
        "which means you need an account at either of these [[Game Distribution Platforms]]. "
        "Titles preceded by an asterisk (*) are those I recommend, or have paid for. "
        "I usually only hand these to people I know in real life, but you can always ask.\n")
    output.write('\n')
    output.write("If you see a title you care for, drop me an email. "
        "My email address can easily be found on "
        "[http://www.macfreek.nl/freek/ this website].\n")
    output.write('\n')
    output.write('{|class="wikitable" style="width: 100%;"\n')
    output.write('! Rec\'d !!style="width: 45%;"| Name !! Distribution !! '\
            'Platforms !!style="width: 190px;"| Preview\n')
    IMAGE_URL = 'http://cdn.akamai.steamstatic.com/steam/apps/{SteamAppId}/capsule_184x69.jpg'
    for game in games:
        game['DistributionURLs'] = ', '.join(['[%s %s]' % (url, destination) if url \
                else destination for destination, url in game['DistributionURLs']])
        game['ImageURL'] = IMAGE_URL.format(**game)
        output.write('|-\n')
        output.write('| %(bought)s || %(Description)s || %(DistributionURLs)s || ' \
                '%(Platforms)s || %(ImageURL)s\n' % game)
    output.write('|}\n\n')
    output.write('So far I have given away %d games. There are %d games still available.\n\n' % \
            (len(given_games), len(games)))
    output.write('Gone are: ')
    output.write(', '.join([game['Description'] for game in given_games]))
    output.write('.\n\n')
    output.write('This list was last updated on {today:%d %B %Y}.\n'.format(today=date.today()))
    output.write('\n[[Category:Games]]\n')


def print_expenses(database: FileMaker, output: TextIO=sys.stdout):
    """Print number of games, total amount, average amount of money spend for each year."""


class ChoicesAction(argparse.Action):
    """Custom store action that adds a `add_choice` method. 
    Choices added this way willl be listed in the help information"""
    def __init__(self, **kwargs):
        super(ChoicesAction, self).__init__(**kwargs)
        if self.choices is None:
            self.choices = []
        self._choices_actions = []
    def __call__(self, parser, namespace, values, option_string=None):
        setattr(namespace, self.dest, values)
    def add_choice(self, choice, help=''):
        self.choices.append(choice)
        # self.container.add_argument(choice, help=help, action='none')
        choice_action = argparse.Action(option_strings=[], dest=choice, help=help)
        self._choices_actions.append(choice_action)
        return choice_action
    def _get_subactions(self):
        return self._choices_actions


class ConfigFileParser(configargparse.DefaultConfigFileParser):
    def parse(self, stream):
        """Parses the keys + values from a config file."""
        # Workaround for https://github.com/bw2/ConfigArgParse/issues/103
        items = OrderedDict()
        for i, line in enumerate(stream):
            line = line.strip()
            if not line or line[0] in ["#", ";", "["] or line.startswith("---"):
                continue
            
            match = re.match(r'^(?P<key>\w+)\s*' 
                             r'(?:(?P<equal>[:=\s])\s*([\'"]?)(?P<value>.+?)?\3)?'
                             r'\s*(?:\s[;#]\s*(?P<comment>.*?)\s*)?$', line)
            if not match:
                raise configargparse.ConfigFileParserException("Unexpected line %s in %s: %s" % 
                        (i, getattr(stream, 'name', 'stream'), line))
            
            key = match.group('key')
            equal = match.group('equal')
            value = match.group('value')
            # comment = match.group('comment')
            if value is None and equal is not None and equal != ' ':
                # distinguish "key = " (value = '')
                # from simply "key" (value = None)
                value = ''
            
            if value is None:  # key-only
                value = 'true'
            elif value.startswith("[") and value.endswith("]"):
                value = [elem.strip() for elem in value[1:-1].split(",")]
            
            items[key] = value
        return items


class ArgumentParser(configargparse.ArgumentParser):
    """Variant of ArgumentParser that fixes various bugs in argparse."""
    def __init__(self, *args, errorlist=[], **kwargs):
        super(ArgumentParser, self).__init__(*args, **kwargs)
        self._errors = errorlist
    def error(self, message):
        # Default behavour is to print message and exit. 
        # That's poor behaviour. Instead:
        # for warnings, store them, for later calling logging.warning.
        # for errors, raise an exception, and let the parent handle it.
        if message.startswith('unrecognized arguments: '):
            self._errors.append((logging.WARNING, message))
        if message.startswith('the following arguments are required'):
            message += '. See %s --help for details' % self.prog
            print(self)
            self._errors.append((logging.ERROR, message))
        else:
            raise configargparse.ConfigFileParserException(message)
    def convert_item_to_command_line_arg(self, action, key, value):
        # workaround for https://github.com/bw2/ConfigArgParse/issues/120
        if action is None:
            message = "Unknown key %s = %s in configuration file" % (key, value)
            self._errors.append((logging.WARNING, message))
            return []
        return super(ArgumentParser, self).convert_item_to_command_line_arg(action, key, value)
    def _get_values(self, action, arg_strings):
        # workaround for bug https://bugs.python.org/issue27227
        # when nargs='*' on a positional, if there were no command-line
        # args, use the default if it is anything other than None
        if (not arg_strings and action.nargs == configargparse.ZERO_OR_MORE and
              not action.option_strings):
            if action.default is not None:
                value = action.default
            else:
                value = arg_strings
            if isinstance(value, list):
                for item in value:
                    self._check_value(action, item)
            else:
                self._check_value(action, value)
            return value
        else:
            return super(ArgumentParser, self)._get_values(action, arg_strings)


def parseargs(argv):
    """Read and return command line aguments and configuration file options"""
    # Store warnings, and print them AFTER parsing the arguments and reading the config file.
    warnings = []
    parser = ArgumentParser(
        default_config_files=['config.ini'],
        # usage='%(prog)s [-h] [-v [-v ...]] [-c CONFIGFILE] [OPTION [OPTION ...]]  [ACTION [ACTION ...]]',
        # ignore_unknown_config_file_keys=True,
        config_file_parser_class=ConfigFileParser,
        errorlist=warnings,
        description="Game Database Manager. Store game purchases in local database.",
        add_config_file_help=False,
        epilog=("All options can be stored set in a configuration file. " 
                "The default path is 'config.ini'. Store options as key=value pairs, "
                "without the '--' in front of the key.")
        )
    parser.register('action', 'store_choice', ChoicesAction)
    
    parser.add_argument('-v', '--verbose', action='count', default=0,
            help="Print more helpful messages")
    parser.add_argument('--loglevel', action='store', default='ERROR',
            help='Alternative way to specify log level, for use in configuration file.')
    # Set default config file in parser.default_config_files, not here.
    # If it is set here, and the file does not exist, configargparse raises an error.
    parser.add_argument('-c', '--config', action='store', dest='configfile', 
            is_config_file=True, help="Path the configuration file")
    parser.add_argument('--cachefolder', action='store', default='/var/tmp',
            dest='cachefolder', help="Path the download cache directory.")
    parser.add_argument('--cache_enforce_chance', action='store', type=float, default=0,
            dest='cache_enforce_chance', help="Chance that the TTL is set to infinity.")
    
    # There parameters are only for use in the configuration file,
    # not on the command line. Consider setting help to argparse.SUPPRESS.
    parser.add_argument('--default_command', action='store', metavar='COMMAND')
    
    group = parser.add_argument_group(title='Subcommands and actions')
    actions = group.add_argument('actions', nargs='?', metavar='COMMAND',
                     action='store_choice', choices=[], default=None)
    actions.add_choice('find-missing-steamids',
            help="Find and store missing Steam IDs for games in the database available on Steam.")
    actions.add_choice('find-all-steamids',
            help="Suggest missing Steam IDs for all games in the database.")
    actions.add_choice('add-steam-images',
            help="Add missing images from Steam.")
    actions.add_choice('verify-steamids',
            help="List discrepencies between the database and Steam for owned games.")
    actions.add_choice('print-humble-purchases',
            help="Print details about all HumbleBundle purchases.")
    actions.add_choice('add-humble-purchase',
            help="Add missing HumbleBundle purchase to the database, given the purchase ID.")
    actions.add_choice('add-humble-purchases',
            help="Add missing HumbleBundle purchases to the database.")
    actions.add_choice('verify-humble-purchases',
            help="Verify if all HumbleBundle purchases are complete in the database.")
    actions.add_choice('print-giftlist',
            help="Output a list of duplicate games I can give away.")

    group = parser.add_argument_group(title='FileMaker options')
    group.add_argument('--filemaker_database_file', metavar='FILEPATH', default='Games.fmp',
            help='The file with the FileMaker database file (this file will be backed up)')
    group.add_argument('--filemaker_database', metavar='NAME', default='Owned Applications',
            help='Name of the FileMaker database to connect to.')
    group.add_argument('--filemaker_username', metavar='USERNAME', default='Admin',
            help='The username used for the ODBC connection to the database.')
    group.add_argument('--filemaker_password', metavar='PASSWORD', default='',
            help='The password used for the ODBC connection to the database.')

    group = parser.add_argument_group(title='Steam options')
    group.add_argument('--steam_api_key', metavar='API_KEY', required=True,
            help='The API key as displayed at https://steamcommunity.com/dev/apikey.')
    group.add_argument('--steam_user_id',  metavar='USER_ID', type=int, required=True,
            help='Your Steam user ID.')
    group.add_argument('--steam_user_name',  metavar='USER_NAME', type=str, required=True,
            help='Your Steam username.')

    group = parser.add_argument_group(title='HumbleBundle options')
    group.add_argument('--humblebundle_sessioncookie', metavar='COOKIE_VALUE', required=True,
            help='The content of the _simpleauth_sess cookie. ' \
                 'The value can be found in your web browser by manually loging ' \
                 'in to humblebundle.com.')
    parser.add_argument('--humbleorder', action='store', 
            dest='humbleorder', help="Limit action to this order ID.")

    group = parser.add_argument_group(title='Gog options')
    group.add_argument('--gog_user_id', metavar='USER_ID', type=int,
            help='Your Steam user ID.')

    group = parser.add_argument_group(title='WikiData options')
    
    options = parser.parse_args(argv)
    
    if not options.actions:
        if options.default_command:
            if options.default_command in actions.choices:
                options.actions = [options.default_command]
            else:
                message = "Default command '%s' unknown. Available options are: %s." % \
                        (options.default_command, ', '.join(actions.choices))
                warnings.append((logging.ERROR, message))
        else:
            warnings.append(logging.ERROR, "No commands specified, nothing to do")
    
    # Match config.verbose and config.loglevel
    # config.loglevel is the default. 
    # config.verbose may further increase it up to debug level.
    loglevels = {
        0: 'ERROR',
        1: 'WARNING',
        2: 'INFO',
        3: 'DEBUG',
    }
    inverse_loglevels = {v: k for k, v in loglevels.items()}
    if options.loglevel not in inverse_loglevels:
        warnings.append(logging.ERROR, "Unknown loglevel %s" % (options.loglevel))
        options.loglevel = 'ERROR'
    verbosity = inverse_loglevels[options.loglevel]
    verbosity += options.verbose
    verbosity = min(3, max(0, verbosity))
    options.verbose = verbosity
    options.loglevel = loglevels[verbosity]
    
    return options, warnings


if __name__ == '__main__':
    logformat = '%(levelname)-8s %(message)s'
    logging.basicConfig(level=logging.WARNING, stream=sys.stderr, format=logformat)
    args = sys.argv[1:] if sys.argv else []
    try:
        config, errors = parseargs(args)
    except configargparse.ConfigFileParserException as e:
        logging.error(e)
        sys.exit(1)

    loglevel = getattr(logging, config.loglevel)
    logging.getLogger().setLevel(loglevel)
    for loglevel, error in errors:
        logging.log(loglevel, error)
        if loglevel == logging.ERROR:
            sys.exit(1)
    logging.debug(config)

    # All resources below are 'lazy', they don't connect to external resources
    # until they are required. So there is no harm in creating the objects,
    # even if they are not needed later.
    
    cachefolder = Path(config.cachefolder)
    cachedownload = CachedDownloader(
            cachefolder=cachefolder, 
            cache_enforce_chance=config.cache_enforce_chance,
            delay = 1.6
    )

    database = FileMaker(database=config.filemaker_database, 
            username=config.filemaker_username, password=config.filemaker_password)
    # The precommit_hook1 is called at most once prior to a database commit.
    databasefile = Path(config.filemaker_database_file)
    database.set_precommit_hook1(lambda: cachedownload.backup(databasefile))
    
    steam = Steam(config.steam_api_key, config.steam_user_id, 
            config.steam_user_name, cachedownload)
    
    humblebundle = HumbleBundle(config.humblebundle_sessioncookie, cachedownload)
    
    if 'find-missing-steamids' in config.actions:
        find_missing_steamids(steam, database)
    if 'find-all-steamids' in config.actions:
        find_missing_steamids(steam, database, 
                all_games=True, dry_run=True, strict_name_check=True)
    if 'add-steam-images' in config.actions:
        add_steam_images(cachedownload, database)
    if 'verify-steamids' in config.actions:
        verify_steamids(steam, database)
    if 'print-humble-purchases' in config.actions:
        print_humble_purchases(humblebundle, database, verbosity=config.verbose)
    if 'add-humble-purchases' in config.actions:
        if config.humbleorder:
            add_humble_purchase(humblebundle, database, humbleorder=config.humbleorder)
        else:
            add_humble_orderids(humblebundle, database, add_missing=True)
    if 'verify-humble-purchases' in config.actions:
        verify_humble_purchases(humblebundle, database)
    if 'print-giftlist' in config.actions:
        print_gift_list(database)
        logging.info("Upload result to "
                "http://www.macfreek.nl/memory/Opinion:Computer_Games#Giveaway_of_Duplicates")
    if 'print-expenses' in config.actions:
        print_expenses(database)
    
    # close the database
    del database

# scripts required to:
# Get wikidata and wikipedia page (search wikipedia + find wikidata based on wikipedia page)
# Add new Humble bundle purchases
# Verify Humble bundle costs
# Add Gog slug
# Add missing images

# Other game databases:
# http://thegamesdb.net/game/20229/
# https://www.igdb.com/games/avara
# http://spong.com/game/11034955/Avara-Power-Mac

# Other collections:
# supported: Steam, Gog, HumbleBundle
# unsupported: PSN, Xbox, Origin, Blizzard
