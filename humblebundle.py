#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Interface to Humble Bundle"""

import logging

# Type hints
try:
    from typing import Dict, List, Any
except ImportError:
    from collections import defaultdict
    Dict = List = defaultdict(str)  # type: ignore
    Any = ''  # type: ignore

# local module (only used here for type hints)
from downloader import CachedDownloader

class HumbleBundle:
    ORDER_LIST_URL = "https://www.humblebundle.com/api/v1/user/order"
    ORDER_INFO_URL = "https://www.humblebundle.com/api/v1/order/{order_id}" \
                     "?wallet_data=true&all_tpkds=true"
    
    def __init__(self, sessioncookie, downloader: CachedDownloader) -> None:
        # self.cookies = {'_simpleauth_sess': sessioncookie}
        self.downloader = downloader
        self.downloader.add_cookie('_simpleauth_sess', sessioncookie, 'humblebundle.com', '/')

    # def get_game_data(self, slug):
    #     """Given a Gog slug (short key to identify a game), return the data.
    #     May raise a IOError if no data can be downloaded."""
    #     return self.downloader.get_cached_json(self.GAME_INFO_URL.format(slug=slug),
    #             'gog_product_review_%s.json' % (slug))

    def get_order_list(self) -> List[str]:
        """Return a list of owned games of the current user.
        The current user is determined by the sessioncookie.
        May raise an PermissionError if the sessioncookie is invalid."""
        try:
            orderlist = self.downloader.get_cached_json(
                    self.ORDER_LIST_URL, 'humble_orders_list.json')
        except PermissionError:
            logging.error("Permission denied for %s. Log in manually with your webbrowser, " \
                "and store the _simpleauth_sess cookie in config.ini, in " \
                "humblebundle_sessioncookie." % (self.ORDER_LIST_URL))
            raise
        return [v['gamekey'] for v in orderlist]
    
    def get_order_info(self, orderid: str):
        """Return details of a given order.
        An order contains information about the order itself, and games that 
        are part of the order, but only if they can be downloaded directly from Humble Bundle.
        Games with only a Steam Key are NOT included in the order details!
        This can be detected with the difference between 'total' and len('subproducts')
        
        The current user is determined by the sessioncookie.
        May raise an PermissionError if the sessioncookie is invalid."""
        try:
            orderdetails = self.downloader.get_cached_json(
                    self.ORDER_INFO_URL.format(order_id=orderid), 
                    'humble_order_%s.json' % (orderid), ttl=400)
        except PermissionError:
            logging.error("Permission denied for %s. Log in manually with your webbrowser, " \
                "and store the _simpleauth_sess cookie in config.ini, in " \
                "humblebundle_sessioncookie.")
            raise
        return orderdetails

    # def get_order_summary(self, orderid: str):
    #     """Return a summary of a given order."""
        
        
# Common values in orderdetails:
# orderkey
# orderdate
# machine_name
# human_name
# category: bundle, subscription (combined subscriptionplan and subscriptioncontent), storefront, widget(purchased elsewhere, otherwise same as storefront)
# platforms: [app, ebook, audio]
# subproducts:
#     machine_name
#     human_name
#     platforms: [mac, linux, windows, android, ebook, video, audio, asmjs (browser)]
#     publisher
#     key_type: download (at Humble Bundle), steam, gog, telltale, ouya, desura, generic(typically in-game content or 3rd party distribution platforms), external_key(usually coupons)
#     steam_app_id  (for steam key_type)
#  also: URL to storefront

    # def get_game_info(self, machine_name:str):
    #     """Return a list of owned games of the current user.
    #     The current user is determined by the sessioncookie.
    #     May raise an PermissionError if the sessioncookie is invalid."""
    #     try:
    #         orderdetails = self.downloader.get_cached_json(
    #                 self.ORDER_INFO_URL.format(order_id=orderid),
    #                 'humble_order_%s.json' % (orderid), ttl=400)
    #     except PermissionError:
    #         logging.error("Permission denied for %s. Log in manually with your webbrowser, " \
    #             "and store the _simpleauth_sess cookie in config.ini, in " \
    #             "humblebundle_sessioncookie.")
    #         raise
    #     return orderdetails
