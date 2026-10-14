# TurnCraft — Shattered Front artwork and R2 deployment

The downloadable `TurnCraft_ShatteredFront_Assets.zip` contains the finished raster library. Extract it and upload the **contents**, preserving all relative folders. Keep the code and lightweight `shattered-front-assets.json` in GitHub.

## Connect the artwork

The default expansion base URL is:

```text
https://pub-cb55798f1eeb4f80a85463da3612ed61.r2.dev/shattered-front/
```

Upload the extracted `ui/`, `campaign/`, `characters/`, `regions/`, `terrain/`, `effects/`, `ranks/`, and `achievements/` folders under the R2 object prefix `shattered-front/`. Upload `asset_manifest.json` beside those folders for inspection. The runtime reads its matching manifest from the repository.

For example, this object must open directly in a browser:

```text
https://pub-cb55798f1eeb4f80a85463da3612ed61.r2.dev/shattered-front/ui/main_menu/battlefield.webp
```

Use the game's Settings screen to change the expansion artwork URL. This stores `turncraft.expansionAssetBase` locally. A deployment can set the global before loading the menu module:

```html
<script>
  window.TURNCRAFT_EXPANSION_ASSET_BASE = 'https://assets.example.com/shattered-front-v1/';
</script>
```

The precedence is global override, saved Settings value, then default URL. The URL must use HTTP or HTTPS and should end in a slash. Region filenames use underscores; campaign identifiers can use hyphens through each region's `assetId`.

The old root R2 paths for existing units, structures, effects and terrain stay intact. The expansion uses its own prefix; uploading this archive must not replace or reorganize existing objects.

## Public access and browser headers

Use a public R2 bucket or an existing publicly accessible asset bucket. A custom domain provides Cloudflare caching for production; Cloudflare describes `r2.dev` as a rate-limited development endpoint. The existing default is preserved for compatibility. See [Cloudflare public bucket documentation](https://developers.cloudflare.com/r2/buckets/public-buckets/).

Serve WebP as `image/webp`, PNG as `image/png`, and JSON as `application/json`. For the current replaceable prefix, a one-hour browser cache is a reasonable deployment setting. A versioned prefix can use a longer cache lifetime; switch the base URL when replacing an art release.

The archive includes `cors-policy.json` for public read access from CatnMice and local development. In R2 bucket Settings, add it through the JSON CORS policy editor. Add any additional exact preview origins you use. CORS origins contain scheme, hostname and optional port; they do not contain a URL path. After changing CORS on an already cached custom domain, refresh the affected cache. See [Cloudflare CORS documentation](https://developers.cloudflare.com/r2/buckets/cors/).

## Library and production metadata

All sixty new original images were generated separately with the built-in `image_gen` tool on October 13, 2026. The three existing faction illustrations were intentionally reused from October 12 to retain Frontier Directorate, Ascendant Conclave and Brood Dominion identity. New art does not rename factions or change rosters.

`generation_record.json` contains every final generation prompt, original filename and SHA-256 provenance. The manifest records logical identifiers, relative paths, pixel dimensions, alpha transparency, file size and SHA-256. Aliases identify intentional shared graphics; thumbnails, banners and cards identify their source illustration. Layout labels and numerical gameplay information remain live HTML.

Ground textures are 256 × 256 sources for the existing 48-pixel square world tiles. Decorations are transparent 128 × 128 sprites. They affect presentation only; collision remains the established `tile.blocked` value.

The command-button atlas contains four isolated 1024 × 256 cells in one column. State order is default, hover, selected, pressed. Individual buttons are tightly cropped around their visible art with eight pixels of padding. The smoke atlas contains eight isolated 256 × 256 frames in four columns and two rows, row-major, at 8 fps. Its frame metadata includes exact source rectangles, duration and center anchor. Each smoke cell has at least 32 pixels of transparent edge space.

The archive also contains `QA_REPORT.json` with file, dimension, hash, transparency and atlas-bound checks. Generated source PNGs stay outside the repository; production WebP/PNG copies are in the download.

## Verify after upload

Open the main background, logo, each region background, all seven rank emblems, and the smoke atlas at the configured base. Then load the game and check desktop and phone layouts, faction switching, mission briefings, region terrain, results and Settings. Missing art falls back gracefully while assets are being uploaded; the complete uploaded library becomes the normal presentation automatically.

Keep the production asset prefix and the repository manifest from the same release. The archive's `asset_manifest.json` and repository `game/turncraft/shattered-front-assets.json` must match.
