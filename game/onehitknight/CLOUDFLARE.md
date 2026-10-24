# One Hit Knight — Cloudflare artwork setup

The game remains a standalone HTML game. Open `onehitknight.html` in a browser to play with its included image artwork. Node and a local web server are not needed for play. The online leaderboard still needs an Internet connection and the existing Firebase configuration.

## Upload the artwork

1. Extract `one-hit-knight-arcade.zip` on your computer. Upload the extracted files, not the ZIP itself.
2. Open **Cloudflare → R2 object storage**, then select your artwork bucket. Create the prefix `one-hit-knight-arcade/v2/`.
3. Upload the contents of the extracted `one-hit-knight-arcade/` folder inside that prefix, preserving every subfolder. The dashboard's **Upload** and **Create folder** controls can do this. A configured S3 client or `rclone copy` can transfer the whole directory while preserving paths.
4. In the bucket's **Settings → Custom Domains**, connect your artwork domain for production. The public `r2.dev` URL can be used for a development check.

Your object keys must look like this:

```text
one-hit-knight-arcade/v2/manifest.json
one-hit-knight-arcade/v2/player/base/...
one-hit-knight-arcade/v2/enemies/goblin/...
one-hit-knight-arcade/v2/bosses/ogre/...
one-hit-knight-arcade/v2/pickups/...
one-hit-knight-arcade/v2/effects/...
one-hit-knight-arcade/v2/environments/castle/...
one-hit-knight-arcade/v2/ui/...
```

Keep the filenames lowercase. The manifest references files relative to the folder containing `manifest.json`.

## Allow the browser to read the images

In the bucket's **Settings → CORS policy**, add this policy for these public game assets:

```json
[
  {
    "AllowedOrigins": ["*"],
    "AllowedMethods": ["GET", "HEAD"],
    "AllowedHeaders": ["*"],
    "ExposeHeaders": ["ETag"],
    "MaxAgeSeconds": 86400
  }
]
```

Wildcard read origins allow the standalone `file://` game as well as the hosted game to request public artwork. Serve PNGs as `image/png`, JSON as `application/json`, and JavaScript as `application/javascript`. If you change CORS after assets were cached, purge that domain's cached assets.

## Change one game setting

Near the top of `game/onehitknight.html`, find `ONE_HIT_KNIGHT_ASSET_BASE` and set it to the public folder URL, with a trailing slash:

```js
window.ONE_HIT_KNIGHT_ASSET_BASE = 'https://assets.example.com/one-hit-knight-arcade/v2/';
```

The empty default uses the image package already included in the HTML. Keep `ONE_HIT_KNIGHT_ASSET_VERSION` aligned with the manifest version. Use a new folder/version for future art releases so older cached files cannot mix with newer metadata.

Before updating the hosted game, open the public `.../v2/manifest.json` URL and one PNG path copied from that manifest. Then start a run and visit later arenas. The game loads the first arena and knight before play, prepares upcoming artwork ahead of each wave, and uses included artwork if an uploaded file is missing. A delivery notice and browser console message identify failed requests; **Retry Assets** retries the host.

## Activate the online leaderboard

The live Firebase project currently accepts anonymous sign-in but denies leaderboard reads. The pull request includes the matching `firestore.rules` fix, verified in the local emulator. After reviewing and merging, publish that complete rules file under **Firebase Console → bible-game-246c0 → Firestore Database → Rules**. This is separate from the R2 artwork upload. No production rules or scores were changed during verification.

## Official references

- [Upload objects and create folders](https://developers.cloudflare.com/r2/objects/upload-objects/)
- [Public buckets and custom domains](https://developers.cloudflare.com/r2/buckets/public-buckets/)
- [Configure CORS](https://developers.cloudflare.com/r2/buckets/cors/)

Checked October 24, 2026. No Cloudflare account settings or uploads were changed by this delivery.
