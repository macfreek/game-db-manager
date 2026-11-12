# Humble Bundle website: data for registering purchases

How to get purchases, bundles, games and books from the Humble Bundle website after its
migration to Next.js in November 2026, as needed to write a parser. Based on a browser
recording and on reading the live pages, on 2026-11-11. Some identifiers have been
changed for privacy reasons.

## Summary

The website holds three kinds of records:

1. **Orders:** purchase date, price, and the name and type of each bundle bought.
2. **Bundles:** every bundle owned, newest first.
3. **Items:** every game key, game download or book, with its bundle, key state and
   expiry date.

Items link to their bundle by ID. Bundles link to orders only by name, so an item's
purchase date is only found through its bundle name. There is no JSON API: the data is
embedded in the pages, or returned by Server Actions.

## Pages

| Page            | URL                                  | Holds                                                         |
|-----------------|--------------------------------------|---------------------------------------------------------------|
| Order list      | `/home/purchases`                    | Orders with all details, 6 per page                           |
| Order           | `/home/purchases/<order id>`         | One order                                                     |
| Library list    | `/home/library?page=<n>`             | All bundles; items, 5 per page                                |
| Library bundle  | `/home/library/<bundle id>`          | One bundle with all its items, including file details         |
| Library item    | `/home/library/content/<item id>`    | One item, including file details                              |

The library list is enough for everything except file details of downloads (platforms,
formats, sizes, hashes); those are only on the bundle and item pages.

## Orders

**Source:** `GET /home/purchases` (RSC). The page data contains
`"data": {"results": [Order, ...], "pagination": {"page", "totalPages", "totalDocs"}}`.
Further pages: `GET /home/purchases?page=<n>` (RSC); the browser also prefetches the
next page. Within the list, orders are not strictly sorted by date.

Orders use the commercetools data model. Amounts are integers: divide `centAmount` by
`10 ** fractionDigits`.

| Field                            | Meaning                                                        | Example                                 |
|----------------------------------|----------------------------------------------------------------|-----------------------------------------|
| `id`                             | Internal UUID; in the order page URL                           | `5b8e17c2-9a41-4d0e-b3f7-2c64e8a19d05`  |
| `orderNumber`                    | Order number, as shown on the site and in emails               | `XK4TQ8RVN2MZD`                         |
| `completedAt`                    | Purchase date                                                  | `2026-10-07T09:28:13.402Z`              |
| `createdAt`                      | Migration date for migrated orders, **not** the purchase date  | `2026-11-10T14:52:31.718Z`              |
| `totalPrice`                     | Total paid, including the charity part                         | `{"centAmount": 1163, "currencyCode": "EUR", "fractionDigits": 2}` |
| `orderState`, `paymentState`     | Status                                                         | `Complete`, `Paid`                      |
| `custom.fields.legacyOrderId`    | Same as `orderNumber`, for migrated orders                     | `XK4TQ8RVN2MZD`                         |
| `custom.fields.legacyCreatedAt`  | Same as `completedAt`, for migrated orders                     |                                         |
| `lineItems[]`                    | One per bundle or product bought                               | see below                               |
| `customLineItems[]`              | Charity part, named `Charity Split (<n>% of <sku>)`            |                                         |

Line items:

| Field                       | Meaning                                                  | Example                              |
|-----------------------------|----------------------------------------------------------|--------------------------------------|
| `name.en`                   | Bundle or product name                                   | `Non Non Nonograms!`                 |
| `productKey`                | Bundle key                                               | `nonnonnonograms_bundle`             |
| `productType.obj.key`       | Product type                                             | `bundle`                             |
| `totalPrice`                | Price, excluding the charity part                        |                                      |
| `taxedPrice`                | Net, VAT and gross amounts                               |                                      |
| `variant.sku`               | Bundle and price tier                                    | `nonnonnonograms_bundle_initial`     |
| `variant.attributes[]`      | List of `{name, value}`:                                 |                                      |
| - `bundleType`              | `games`, `books` or `software`                           | `{"key": "games", "label": "Games"}` |
| - `keyTypes`                | Where keys are redeemed                                  | `["steam"]`                          |
| - `keyExpirationDateTime`   | Last date to redeem keys                                 | `2027-05-27T18:00:00.000Z`           |
| - `totalMsrpValue`          | Total list price of the contents, per currency           |                                      |
| - `startDateTime`           | Start of the sales period of the bundle                  |                                      |
| - `endDateTime`             | End of the sales period of the bundle                    |                                      |
| - `giftable`                | `always`, `whenPaid` or `never`                          |                                      |

Order numbers come in three formats:

| Format                                    | Example            | Where                                                  |
|-------------------------------------------|--------------------|--------------------------------------------------------|
| 13 characters, uppercase and digits       | `XK4TQ8RVN2MZD`    | Migrated orders; receipt emails                        |
| 16 characters, uppercase and digits       | `01K3QD8WZT5R7HMB` | Orders placed on the new site                          |
| 16 characters, mixed case (old "gamekey") | `9rKw2Tz7hQmP4dXa` | Old emails (`/?key=...`) and the database field `HumbleOrder`; not on the new site |

## Bundles

**Source of the list:** `GET /home/library` (RSC or full page). The page data contains
`"bundleItems": [...]`, all bundles in one response, newest first.

| Field                                  | Meaning                                           |
|----------------------------------------|---------------------------------------------------|
| `id`                                   | Bundle ID; in the bundle page URL                 |
| `productEntitlementId`                 | Same as `id`                                      |
| `title`                                | Bundle name                                       |
| `type`                                 | Always `bundle`                                   |
| `entitlements.downloads[0].giftStatus` | Gift status of the bundle                         |

There is no purchase date, price or bundle type; those are in the order.

The list currently lacks older bundles (see "Completeness during migration"). Bundle IDs can also be
collected from the items' `productEntitlementId`.

**Source of one bundle:** `GET /home/library/<bundle id>` (RSC). The page data contains
`"userLibraryItem"` with `title`, `type: "bundle"` and `entitlements`:
`keys[]` (one entry per key), `downloads[]` (one entry per download, including file
details) and `redemptionGroups` (always empty). Its `id` differs from the bundle ID in
the URL and is used nowhere else.

## Items

**Source:** a Server Action on the library page; the item list is not in the page data.
`filterUserLibrary`, called as `POST /home/library` with header
`Next-Action: <id of filterUserLibrary>` and a `multipart/form-data` body. The ID
changed within one day (`60068310…` in the morning, `6067723b…` in the afternoon of
2026-11-11), so look it up by function name.

| Form field         | Example      | Meaning                                                  |
|--------------------|--------------|----------------------------------------------------------|
| `_1_page`          | `2`          | Page number, starting at 1                               |
| `_1_limit`         | `5`          | Items per page; the site uses 5, but 1000 works          |
| `_1_cursor`        | `10`         | Always `page × limit`                                    |
| `_1_sort`          | `mostRecent` | Sort order; newest purchase first                        |
| `_1_query`         | (empty)      | Search text                                              |
| `_1_hideRevealed`  | `false`      | Filter                                                   |
| `_1_hideExpired`   | `false`      | Filter                                                   |
| `_1_dateExpires`   | (empty)      | Filter                                                   |
| `_1_requestId`     | `3`          | Counter of the browser                                   |
| `0`                | `["$undefined","$K1"]` | Arguments: previous result (not needed) and the form |

The response `1:` line is `{"results": [item, ...], "pagination": {"page",
"totalPages", "totalDocs"}, "success": true, ...}`. With `_1_limit` 1000, a page is about
1.3 MB; about 8200 items fit in 9 requests. No other headers are needed; the browser's
session cookie is enough. `limit` as a URL parameter has no effect.

An item is either a key or a download. Its details are in one entry of
`entitlements.keys[]` or `entitlements.downloads[]`.

### Item information per page

What each page lists about an individual purchased item (✓ present, ✗ missing):

- **Library:** the list of purchased items, whether bought in the store, through a
  the Humble Choice subscription (formerly Humble Monthly) or in a bundle (`/home/library?page=<n>`). Its entry fields are
  in `entitlements.keys[0]` or `entitlements.downloads[0]`; `id`, `title`, `subTitle`
  and `type` occur both there and in the outer object.
- **Content:** the page of one item (`/home/library/content/<item id>`).
- **Bundle:** the page of one purchased bundle (`/home/library/<bundle id>`), which
  lists all its items.

The order pages (`/home/purchases`) list no individual items.

| Field                                 | Library | Content | Bundle | Meaning                                                           |
|---------------------------------------|---------|---------|--------|-------------------------------------------------------------------|
| `id`                                  | ✓       | ✓       | ✓      | Item ID; in the item page URL; used to reveal the key             |
| `title`                               | ✓       | ✓       | ✓      | Game or book name                                                 |
| `type`                                | ✓       | ✓       | ✓      | `steam`, `gog`, `generic` or `download_drm_free`                  |
| `subTitle`                            | ✓       | keys    | keys   | Bundle name; varies for store purchases                           |
| `contentId`                           | ✓       | ✗       | ✗      | Text identifier, like the old `machine_name`                      |
| `productEntitlementId`                | ✓       | ✗       | ✗      | Bundle ID                                                         |
| `deliveryMethods`                     | ✓       | ✗       | ✗      | e.g. `["steam"]`, `["download"]`                                  |
| `itemType`, `collection`, `userId`    | ✓       | ✗       | ✗      | Always `content-entitlement`, `content-entitlements`; user UUID   |
| `unavailableAfter`                    | ✓       | ✓       | ✓      | Expiry date, or `null`                                            |
| `expired`                             | ✓       | ✓       | ✓      | Past `unavailableAfter`, also for revealed keys                   |
| `giftStatus`                          | ✓       | ✓       | ✓      | `unassigned` or `gifted`                                          |
| `unavailableBefore`                   | ✓       | ✓       | ✓      | Date before which the item is unavailable; always `null` seen     |
| `notGiftable`                         | ✓       | ✓       | ✓      | Status flag (`true`/`false`)                                      |
| `preOrdered`                          | ✓       | ✓       | ✓      | Status flag (`true`/`false`)                                      |
| `outOfStock`                          | ✓       | ✓       | ✓      | Status flag (`true`/`false`)                                      |
| `redeemable`                          | ✓       | ✓       | ✓      | Status flag (`true`/`false`)                                      |
| `redeemedAt`                          | ✓       | ✓       | ✓      | Keys: when revealed, or `null`                                    |
| `redemptionData.keyFormatted`         | ✓       | ✓       | ✓      | Keys: the key, or `null`                                          |
| `redemptionData.externalOrderStatus`  | ✓       | ✓       | ✓      | Keys: always `null`                                               |
| `redemptionData.externalAccountName`  | ✓       | ✓       | ✓      | Keys: always `null`                                               |
| `keyIntegrationId`                    | ✓       | ✓       | ✓      | Keys: same as `type`                                              |
| `customCtaUrl`                        | ✓       | ✓       | ✓      | Keys: link to redemption instructions                             |
| `customCtaLabel`                      | ✓       | ✓       | ✓      | Keys: label of that link                                          |
| `instructionsHtml`                    | ✓       | ✓       | ✓      | Keys: redemption instructions as HTML                             |
| `customDisclaimer`                    | ✓       | ✓       | ✓      | Keys: disclaimer text, e.g. on giftable copies                    |
| `directRedeem`, `gameKey`             | ✓       | ✓       | ✓      | Keys: always `null`                                               |
| `downloads[]`                         | ✗       | ✓       | ✓      | Downloads: platforms and files, see "Downloads"                   |

"keys": on the item and bundle pages, `subTitle` is present for keys, not for downloads.

The site shows "Keys expire. Please reveal your key before <date>." for items with an
`unavailableAfter` date.

### Keys

Every key has a state and a type, independent of each other.

| State        | Shown on the page     | `redeemedAt` | `keyFormatted` | `giftStatus`   |
|--------------|-----------------------|--------------|----------------|----------------|
| Revealed     | The key               | date         | key            | `"unassigned"` |
| Not revealed | A "Reveal" button     | `null`       | `null`         | `"unassigned"` |
| Gifted       | A "Gift Link" button  | `null`       | `null`         | `"gifted"`     |

- **Revealed:** `redeemedAt` and `keyFormatted` are both filled or both `null`.
- **Gifted keys look unrevealed:** only `giftStatus` tells them apart.

| `type`     | Key format                           | Instructions in the data                                           |
|------------|--------------------------------------|--------------------------------------------------------------------|
| `steam`    | `XXXXX-XXXXX-XXXXX` (17 characters)  | `customCtaUrl`, `instructionsHtml`: Humble's Steam instructions    |
| `gog`      | 18 characters (one key seen)         | `instructionsHtml`: GOG's redeem page and Humble's GOG instructions |
| `generic`  | 8 to 32 characters, no fixed format  | None: `customCtaUrl: ""`, `instructionsHtml: null`                 |
| `epic`     | Not seen in the data                 | Not seen                                                           |
| `origin`   | Not seen in the data                 | Not seen                                                           |
| `blizzard` | Not seen in the data                 | Not seen                                                           |
| `uplay`    | Not seen in the data                 | Not seen                                                           |

Types not seen in the data are named in the site's JavaScript.

- **Store icon:** follows the type for `steam` and `gog`; for `generic` not checked.
- **Generic keys** are for other sites, often in-game extras or one key per price tier
  (title `<bundle name> - BT15`).
- **The same game can be both a key and a download** in one bundle, as separate items.

### Downloads

A download item (`type: "download_drm_free"`) has `downloads[]`, one entry per
platform; each has `downloadStruct[]`, one entry per file.

| Field (`downloads[]`)       | Meaning                                  | Example                                     |
|-----------------------------|------------------------------------------|---------------------------------------------|
| `key`                       | `<name>_<platform>`                      | `learningapachedrill_ebook`                 |
| `platform`                  | See table below                          | `ebook`                                     |
| `deliveryMethods`           | `["download"]` or `["audio-download"]`   |                                             |
| `downloadStruct[]`          | Files                                    |                                             |

| Field (`downloadStruct[]`)  | Meaning                                  | Example                                     |
|-----------------------------|------------------------------------------|---------------------------------------------|
| `name`                      | File type                                | `PDF`, `ZIP`                                |
| `fileSize`                  | Size in bytes                            | `8872618`                                   |
| `size`                      | Readable size in older items, else `null`| `"6.3 MB"`                                  |
| `md5`                       | MD5 hash                                 |                                             |
| `sha1`                      | SHA1 hash; `null` in some older items    |                                             |
| `url.web`                   | File name                                | `backtobed_ost_flac_1435169786.zip`         |
| `url.bittorrent`            | Torrent file name                        |                                             |

| `platform` | File types (`name`)                      |
|------------|------------------------------------------|
| `windows`  | `ZIP`, `EXE`, `MSI`                      |
| `mac`      | `ZIP`, `DMG`                             |
| `linux`    | `ZIP`, `GZ`, `DEB`, `RPM`, `SH`, `RUN`   |
| `android`  | `APK`                                    |
| `audio`    | `ZIP`                                    |
| `ebook`    | `PDF`, `EPUB`, `MOBI`                    |

`OTHER` also occurs. The split per platform is assumed from the file types, except for
`audio`, `ebook` and `linux`.

Several files for one platform are alternatives. `name` does not tell them apart;
`url.web` and `fileSize` do:

| Kind                          | Example                                                                         |
|-------------------------------|---------------------------------------------------------------------------------|
| Audio quality                 | `ZIP` files `…_ost_mp3_….zip` and the larger `…_ost_flac_….zip`                 |
| Architecture and package type | `DEB`, `RPM` and `GZ`, each for i386/i686 and amd64/x86_64, of similar size      |
| Book format                   | `PDF`, `EPUB`, `MOBI`                                                           |

Each book is one item; its formats are files, not separate items.

### Oddities

- **Placeholders:** some download items have no `downloads`, such as `No Content`.
  The bundle page hides them, but still shows its "Downloads" heading.
- **Search keeps the page number:** searching the library from `?page=<n>` with
  `n > 1` shows "No Results" when the results fit on fewer pages.
- **Items linking to a bundle page:** some items of very old purchases link to
  `/home/library/<id>` instead of `/home/library/content/<id>`. The one tried returned
  an error page. Assumed to be a temporary glitch.

## Technology

The site is a Next.js application. Data comes in two forms:

| Form                    | Request                                              | Response             |
|-------------------------|------------------------------------------------------|----------------------|
| Page data (RSC payload) | `GET <page URL>` with header `RSC: 1`                | `text/x-component`   |
| Server Action           | `POST <page URL>` with header `Next-Action: <id>`    | `text/x-component`   |

A full page load (no `RSC` header) returns HTML with the same page data embedded in
`self.__next_f.push(...)` script blocks, with quotes escaped as `\"`.

- **RSC payload format:** line based, `<id>:<value>`. Most values are JSON;
  `T<hex length>,` rows hold raw text, with the length in bytes. The data sits inside
  JSON that describes React components; find it by searching for a key such as
  `"data":`, `"bundleItems":` or `"userLibraryItem":` and decoding the JSON value that
  follows (e.g. with Python's `json.JSONDecoder.raw_decode`).
- **Server Action response format:** `0:{...}` holds metadata, including the build ID
  (`"b"`); `1:{...}` holds the return value as plain JSON.
- **Server Action IDs** are 42 hexadecimal characters and change when the site is
  rebuilt. The JavaScript chunks under `/_next/static/chunks/` declare them as
  `createServerReference("<id>", ..., "<functionName>")`, so the current ID can be
  found by function name.
- **Placeholders:** the string `"$undefined"` stands for JavaScript `undefined`; strings
  such as `"$30"` refer to row `30` of the same payload.
- **Authentication:** by cookie; requests carry no `Authorization` header. The login
  cookie is `ory_session_<suffix>` (domain `.humblebundle.com`, HttpOnly, Secure, no
  expiry date, so deleted when the browser closes, unless the browser restores its
  session). Without it, `/home/library` redirects to
  `https://auth.humblebundle.com/login?flow=<UUID>`. The suffix may change; match the
  name by prefix.
- **Cloudflare:** blocks the login page in browsers controlled by Playwright (its own
  WebKit and an installed Chrome): "Security verification failed". Plain requests
  (Python `requests`) pass with only the login cookie `ory_session_<suffix>`, without
  `cf_clearance` or `__cf_bm`, and even with a meaningless `User-Agent`.
- **Rate limit:** loading pages in quick succession can return a page with only the text
  `Rate exceeded.`. The limit is not published.
- **Tracking parameter:** links get a `_gl=...` parameter (Google Analytics); it can be
  left out.
- **The old API is gone:** `/api/v1/user/order` returns 404.

## Relating the records

| Relation                    | How                                                                                     |
|-----------------------------|-----------------------------------------------------------------------------------------|
| Item → bundle               | Item `productEntitlementId` = bundle `id`                                               |
| Bundle → order              | Only by name: bundle `title` (and item `subTitle`) = order line item `name.en`          |
| Bundle bought twice         | Same name twice; match by order: bundles are newest first, orders have a purchase date  |
| Order → bundles             | `lineItems` is a list; all orders seen had one line item                                |

No other ID is shared: the order's IDs (`id`, line item `id`, `productId`, `productKey`,
`bundleItemKeys`, `redeemableItems`) occur nowhere in the library, and no library ID
occurs in the order. Library IDs of migrated purchases start with `c7a551c0`; newer
purchases have other UUIDs.

### Store purchase, bundle or Humble Choice?

A store purchase, a bundle purchase and a Humble Choice month can't be told apart
reliably.

- **Sales product:** each is a single sales product, with one `productEntitlementId`
  shared by its items.
- **Number of items:** store products, bundles and Humble Choice months can all have one
  or more items.
- **Identifier:** bundles often have the word `bundle` in their identifier, usually
  `_bundle_` (e.g. `contentId` `polimines_bundle_steam`, `productKey`
  `nonnonnonograms_bundle`). Humble Choice items have `_choice_` (e.g. `contentId`
  `squirrelwithagun_row_choice_steam`).
- **Name:** store products often have the item's name with ` (steam)` appended as
  `subTitle`, but sometimes the name varies, or `subTitle` is missing. Humble Choice
  items have `<Month> <Year> Humble Choice` as `subTitle`, e.g. `February 2026 Humble
  Choice`.
- **Order date:** until November 2026, a bundle (or Humble Choice subscription) could not be
  combined with other products in one order. So an older order with more than one
  product contains no bundle.
- **Bundle list:** items whose bundle is in the bundle list are bundle purchases. Other
  items may also be, because the bundle list is still incomplete. Humble Choice months
  are not in the bundle list, also not recent ones (e.g. February 2026).

The `subTitle` of a store product names the product, which can differ from the item
`title` and from the name in the order. This table is two store purchases:

| Item `title`                       | Item `subTitle`                                       | Name in the order email          |
|------------------------------------|-------------------------------------------------------|----------------------------------|
| `Kentucky Route Zero`              | `Kentucky Route Zero (steam)`                         | Kentucky Route Zero              |
| `Kentucky Route Zero: PC Edition`  | `Kentucky Route Zero (steam)`                         | Kentucky Route Zero              |
| `Kentucky Route Zero Soundtrack`   | `Kentucky Route Zero (steam)`                         | Kentucky Route Zero              |
| `Dreamfall Chapters` (Steam)       | `Dreamfall Chapters: The Final Cut Edition (steam)`   | Dreamfall Chapters Season Pass   |
| `Dreamfall Chapters` (DRM free)    | `Dreamfall Chapters: The Final Cut Edition (steam)`   | Dreamfall Chapters Season Pass   |

The ` (steam)` suffix belongs to the product: downloads and soundtracks of a product
sold for Steam and DRM-free carry it too. Matching store products to orders by name is
therefore unreliable.

## Actions

**Reveal a key:** `redeemEntitlementAction`, `POST /home/library/<bundle id>` with
`Next-Action: 708150775ec23e3a19ce9e459db7db423e51e97b2f`. Body
`["<item id>", "steam", "$undefined"]`; response `1:{"success": true, "key": "<key>"}`.
Revealing cannot be undone.

**Redeem a revealed key at the store:** a button next to a revealed key opens the
store's redeem page, per key `type` (from the site's JavaScript):

| `type`     | Redeem page                                          |
|------------|------------------------------------------------------|
| `steam`    | `https://store.steampowered.com/account/registerkey` |
| `gog`      | `https://www.gog.com/en/redeem`                      |
| `epic`     | `https://store.epicgames.com/en-US/redeem`           |
| `origin`   | `https://www.ea.com/redeem`                          |
| `blizzard` | `https://account.battle.net/redeem`                  |

`generic` keys have no redeem page; for `uplay` none was found. How the key is passed
was not found in the JavaScript, and not tested, since that would redeem the key.

**Download links:** `getSignedUrlsForEntitlements`
(`704530b2009d4e98944e9a08094388a1b57b0374e4`), body `[["<item id>", ...]]`, returns
per file a signed URL on `cdn.humble.com`, valid for 24 hours, and a `.torrent` URL.
`recordDownload` (`409d8e1cccc7dbd2df6471d9139372d76ce687ffca`) only registers a
download.

## Completeness during migration

On 2026-11-11 the migration of old purchases was still running. The library page and
the order history said so in a notice, and data was missing:

- **Order history:** not complete.
- **Bundle list:** older bundles missing, although their items and bundle pages exist.

These gaps are assumed to be temporary. When the notices disappear, check again. Only a
full download of the library, compared with previous records (redemptions, emails), shows
whether it is complete.

## Coverage

Not in the data at all:

- **Steam App ID:** the closest identifier is the item's `contentId`, e.g.
  `polimines_bundle_steam`. Matching by name is best effort: a purchase is usually a
  Steam application, Steam bundle or Steam package (sub), but not always.
- **Redemption site of `generic` keys:** sites differ and can't be detected reliably;
  the bundle or item name may hint at it. Any detection is best effort and needs manual
  review.

## Open questions


## Appendix: examples

Keys are replaced by their length, download URLs left out.

### Steam key, library list ("Polimines")

```json
{
 "id": "c7a551c0-0d77-4f1d-b81e-3f02d6a94c71",
 "title": "Polimines",
 "subTitle": "Non Non Nonograms!",
 "itemType": "content-entitlement",
 "type": "steam",
 "userId": "<user UUID>",
 "contentId": "polimines_bundle_steam",
 "productEntitlementId": "c7a551c0-0d77-4f1d-a6c3-7e19b48d05f2",
 "collection": "content-entitlements",
 "entitlements": {
  "keys": [
   {
    "id": "c7a551c0-0d77-4f1d-b81e-3f02d6a94c71",
    "title": "Polimines",
    "subTitle": "Non Non Nonograms!",
    "type": "steam",
    "redemptionData": {
     "keyFormatted": null,
     "externalOrderStatus": null,
     "externalAccountName": null
    },
    "redeemedAt": null,
    "notGiftable": false,
    "unavailableBefore": null,
    "unavailableAfter": "2027-05-27T18:00:00.000Z",
    "customDisclaimer": "Steam will not provide extra giftable copies of games you already own.",
    "customCtaLabel": "Steam Instructions",
    "customCtaUrl": "https://support.humblebundle.com/hc/articles/204008710-How-To-Redeem-Steam-Keys",
    "instructionsHtml": "<a href=\"https://support.humblebundle.com/hc/articles/204008710-How-To-Redeem-Steam-Keys\" target=\"_blank\">Steam Instructions</a>",
    "keyIntegrationId": "steam",
    "directRedeem": null,
    "giftStatus": "unassigned",
    "gameKey": null,
    "expired": false,
    "preOrdered": false,
    "outOfStock": false,
    "redeemable": true
   }
  ],
  "downloads": []
 },
 "deliveryMethods": ["steam"],
 "preOrder": "$undefined",
 "releaseDate": "$undefined",
 "choiceDate": "$undefined"
}
```

### GOG key, item page ("Haven")

```json
{
 "id": "c7a551c0-0cc0-4e68-9d42-5a8e17b6f0c3",
 "title": "Haven",
 "subTitle": "Power Up Pride",
 "type": "gog",
 "redemptionData": {
  "keyFormatted": "<18 characters>",
  "externalOrderStatus": null,
  "externalAccountName": null
 },
 "redeemedAt": "2025-10-21T18:47:09.312Z",
 "notGiftable": false,
 "unavailableBefore": null,
 "unavailableAfter": null,
 "customDisclaimer": "",
 "customCtaLabel": "",
 "customCtaUrl": "",
 "instructionsHtml": "<a href=\"https://secure.gog.com/en/redeem/\" target=\"_blank\">Redeem GOG Key</a><br><a href=\"https://support.humblebundle.com/hc/articles/204969217-GOG-Key-Redemption\" target=\"_blank\">GOG Instructions</a>",
 "keyIntegrationId": "gog",
 "directRedeem": null,
 "giftStatus": "unassigned",
 "gameKey": null,
 "expired": false,
 "preOrdered": false,
 "outOfStock": false,
 "redeemable": true
}
```

### Generic key, bundle page (Paizo bundle)

```json
{
 "id": "c7a551c0-0c7d-415f-a0e9-6b4f21c87d53",
 "title": "Humble RPG Bundle: Pathfinder Second Edition \"Happy Birthday, Remaster!\" Bundle from Paizo Inc. - BT15",
 "subTitle": "Humble RPG Bundle: Pathfinder Second Edition \"Happy Birthday, Remaster!\" Bundle from Paizo Inc.",
 "type": "generic",
 "redemptionData": {
  "keyFormatted": "<16 characters>",
  "externalOrderStatus": null,
  "externalAccountName": null
 },
 "redeemedAt": "2024-10-23T21:05:52.648Z",
 "notGiftable": false,
 "unavailableBefore": null,
 "unavailableAfter": "2025-07-30T07:00:00.000Z",
 "customDisclaimer": "",
 "customCtaLabel": "",
 "customCtaUrl": "",
 "instructionsHtml": null,
 "keyIntegrationId": "generic",
 "directRedeem": null,
 "giftStatus": "unassigned",
 "gameKey": null,
 "expired": true,
 "preOrdered": false,
 "outOfStock": false,
 "redeemable": true
}
```

### Book, library list ("Machine Learning with Python Cookbook, 2nd Edition")

```json
{
 "id": "c7a551c0-0d5a-4c41-8f27-d1b5c903e6a8",
 "title": "Machine Learning with Python Cookbook, 2nd Edition",
 "subTitle": "Humble Tech Book Bundle: Python, Rust, and Data by O'Reilly",
 "itemType": "content-entitlement",
 "type": "download_drm_free",
 "userId": "<user UUID>",
 "contentId": "machinelearningwithpythoncookbook2ndedition",
 "productEntitlementId": "c7a551c0-0d5a-4c41-bb06-4e7a92f13d58",
 "collection": "content-entitlements",
 "entitlements": {
  "keys": [],
  "downloads": [
   {
    "id": "c7a551c0-0d5a-4c41-8f27-d1b5c903e6a8",
    "title": "Machine Learning with Python Cookbook, 2nd Edition",
    "type": "download_drm_free",
    "redemptionData": {},
    "notGiftable": false,
    "unavailableBefore": null,
    "unavailableAfter": null,
    "giftStatus": "unassigned",
    "expired": false,
    "preOrdered": false,
    "outOfStock": false,
    "redeemable": true
   }
  ]
 },
 "deliveryMethods": ["download"]
}
```

### Book, bundle page ("Learning Apache Drill")

```json
{
 "id": "c7a551c0-0b1c-4189-9c31-e85f02a7b46d",
 "title": "Learning Apache Drill",
 "type": "download_drm_free",
 "redemptionData": {},
 "downloads": [
  {
   "id": "3a91c4f7-58d2-4e6b-a07f-b2e95d18c463",
   "key": "learningapachedrill_ebook",
   "desktopAppOnly": false,
   "is64BitToggle": false,
   "downloadIdentifier": null,
   "downloadVersionNumber": null,
   "notes": null,
   "deliveryMethods": ["download"],
   "platform": "ebook",
   "downloadStruct": [
    {"id": "237b839764178af3fd34eb46", "arch": null, "fileSize": 8872618, "md5": "d953b7585855ed850cc0767ddbbe7a22", "name": "PDF", "sha1": "8462d09b60f3a0511c165d494b40ef0b085b817e", "size": null, "timestamp": null, "small": 1, "uploadedAt": null, "hdStreamUrl": null, "sdStreamUrl": null, "url": {"bittorrent": "…", "web": "…"}},
    {"id": "bd3ba09a1e1083069b1fd0a5", "arch": null, "fileSize": 14301697, "md5": "dcf60eb7b1a18be99e8f09585d8ef40c", "name": "MOBI", "sha1": "b7e461dd56b7c0f9ad0e3b7fc8277cf8fcefc8e8", "size": null, "timestamp": null, "small": 1, "uploadedAt": null, "hdStreamUrl": null, "sdStreamUrl": null, "url": {"bittorrent": "…", "web": "…"}},
    {"id": "5a2e50ced052467e10eeeb90", "arch": null, "fileSize": 5473319, "md5": "bf5a7ad3e4daee3a35c7eae73a2d59be", "name": "EPUB", "sha1": "083377fa310b242c89ecd85edb3bac69ec41d5a8", "size": null, "timestamp": null, "small": 1, "uploadedAt": null, "hdStreamUrl": null, "sdStreamUrl": null, "url": {"bittorrent": "…", "web": "…"}}
   ],
   "updatedAt": "2026-09-16T16:45:45.228Z",
   "createdAt": "2026-09-16T16:45:45.228Z"
  }
 ],
 "notGiftable": false,
 "unavailableBefore": null,
 "unavailableAfter": null,
 "giftStatus": "unassigned",
 "expired": false,
 "preOrdered": false,
 "outOfStock": false,
 "redeemable": true
}
```
