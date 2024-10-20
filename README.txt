Game Database Manager. Store game purchases in local FileMaker database.

Code is written for my personal use, and is not maintained.
The main script is in manage_game_database.py whose functions are listed bellow (some are work in progress).

The only reusable code is downloader.py, and perhaps part of steam.py

Planned functions in manage_game_database.py:

find-missing-steamids
            Find and store missing Steam IDs for games in the database available on Steam.
            find_missing_steamids(steam, database)
find-all-steamids
            Suggest missing Steam IDs for all games in the database.
            find_missing_steamids(steam, database, 
                    all_games=True, dry_run=True, strict_name_check=True)
add-steam-images
            Add missing images from Steam.
            add_steam_images(cachedownload, database)
verify-steamids
            List discrepencies between the database and Steam for owned games.
            verify_steamids(steam, database)
print-humble-purchases
            Print details about all HumbleBundle purchases.
            print_humble_purchases(humblebundle, database, verbosity=config.verbose)
add-humble-purchase
            Add missing HumbleBundle purchase to the database, given the purchase ID.
            add_humble_purchase(humblebundle, database, humbleorder=config.humbleorder)
add-humble-purchases
            Add missing HumbleBundle purchases to the database.
            add_humble_orderids(humblebundle, database, add_missing=True)
verify-humble-purchases
            Verify if all HumbleBundle purchases are complete in the database.
            verify_humble_purchases(humblebundle, database)
print-giftlist
            Output a list of duplicate games I can give away.
            print_gift_list(database)

print-expenses [unlisted]
            print_expenses(database)

add-steam-properties [not implemented]
            Add information on release date, size, producer, publisher, keywords

find_missing_gog_ids [not implemented]

find_missing_wikidata_ids [not implemented]
