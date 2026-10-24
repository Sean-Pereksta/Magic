# One Hit Knight — Cloudflare artwork setup

The game remains a standalone HTML game. Open `onehitknight.html` in a browser to play with its included image artwork. Node and a local web server are not needed for play. The online leaderboard still needs an Internet connection and the existing Firebase configuration.

The configured artwork host is `https://pub-b50fd62dbbdf45b2aef043f19a0b7380.r2.dev/`. The game requests `manifest.json` and its relative PNG paths from the root of this bucket.

The updated game expects the `arcade-v2-equipment` manifest. This pack replaces the armor overlays with six complete knight character sheets and includes nine separate weapon sheets. Upload the matching replacement pack to activate these hosted images. While an older version is hosted, the game uses its matching included artwork.

A read-only check on October 24, 2026 confirmed HTTP 200 and `Access-Control-Allow-Origin: *` for the live manifest and knight PNG. CORS is configured. The live manifest still identifies the previous `arcade-v1` pack, pending the replacement upload.

## Upload the artwork

1. Merge and deploy the updated game first. It uses the new included art while the previous remote pack is still hosted. The earlier game renderer does not support the complete-character armor aliases in this replacement pack.
2. Extract `one-hit-knight-arcade-v2.zip` on your computer. Upload the extracted files, not the ZIP itself.
3. Open **Cloudflare → R2 object storage**, then select the bucket for the configured host.
4. Upload the extracted contents directly into the bucket root, preserving every subfolder. Upload PNGs first and replace `manifest.json` last. The ZIP has no extra enclosing directory. The dashboard's **Upload** and **Create folder** controls can do this. A configured S3 client or `rclone copy` can transfer the whole directory while preserving paths.
5. In the bucket's **Settings → Custom Domains**, connect your artwork domain for production. The public `r2.dev` URL can be used for a development check.

Your object keys must look like this:

```text
manifest.json
player/knights/steel.png
player/knights/gold.png
player/knights/shadow.png
player/knights/phoenix.png
player/knights/vampire.png
player/knights/thunder.png
player/weapons/...
enemies/goblin/...
bosses/ogre/...
pickups/...
effects/...
environments/castle/...
ui/...
```

Keep the filenames lowercase. The manifest references files relative to the folder containing `manifest.json`. The smaller `one-hit-knight-character-variants.zip` is for inspecting or reusing the characters and weapons; its `characters-manifest.json` is a subset and must not replace the game's full manifest. Legacy `player/armor/` PNGs are no longer referenced by this version.

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
window.ONE_HIT_KNIGHT_ASSET_BASE = 'https://pub-b50fd62dbbdf45b2aef043f19a0b7380.r2.dev/';
```

This URL is already the default in the PR. An explicit empty string uses the image package already included with the game. Keep `ONE_HIT_KNIGHT_ASSET_VERSION` aligned with the manifest version (`arcade-v2-equipment`). A mismatch rejects the remote manifest and uses the matching included pack. If future uploads use a versioned subfolder, include that subfolder in the base URL and update the version with it.

After uploading the pack, open `https://pub-b50fd62dbbdf45b2aef043f19a0b7380.r2.dev/manifest.json` and one PNG path copied from that manifest. Then start a run and visit later arenas. The game loads the first arena and knight before play, prepares upcoming artwork ahead of each wave, and uses included artwork if an uploaded file is missing. A delivery notice and browser console message identify failed requests; **Retry Assets** retries the host.

## Activate the online leaderboard

The live Firebase project currently accepts anonymous sign-in but denies leaderboard reads. The pull request includes the matching `firestore.rules` fix, verified in the local emulator. After reviewing and merging, publish that complete rules file under **Firebase Console → bible-game-246c0 → Firestore Database → Rules**. This is separate from the R2 artwork upload. No production rules or scores were changed during verification.

## Official references

- [Upload objects and create folders](https://developers.cloudflare.com/r2/objects/upload-objects/)
- [Public buckets and custom domains](https://developers.cloudflare.com/r2/buckets/public-buckets/)
- [Configure CORS](https://developers.cloudflare.com/r2/buckets/cors/)

Checked October 24, 2026. No Cloudflare account settings or uploads were changed by this delivery.
