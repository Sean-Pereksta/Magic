# Basketball 2 graphics on Cloudflare

The graphics release is enabled in `assets/graphics-v2-config.js`. Its base URL is the existing public bucket:

`https://pub-b956b47cfcfd467d9f678959c08a5781.r2.dev/`

The upload uses **the bucket root**. `manifest.json`, `players/`, `courts/`, `arenas/`, `hoops/`, `animations/`, `uniforms/`, `equipment/`, `textures/`, `effects/` and `ui/` are top-level items. Do not add a `basketball2-v2/` prefix to this configuration. The remote manifest and all 59 listed files were downloaded and checked against the release's SHA-256 hashes on October 20, 2026; all matched and allowed cross-origin browser requests.

## What loads

The game loads three role-specific, rigged player models and only the selected court, arena and hoop. Every athlete keeps their profile skin and hair colors, proportions, number and outfit selections. The pack has three authored face/hair variants, one uniform cut and one shoe silhouette; it does not infer facial features or ethnicity from names or skin color.

The existing animation controller selects its twelve established clips. Ten additional clips and motion-event metadata are available in the uploaded pack but do not add gameplay actions automatically. Existing saves, campaign progression, controls, scoring, ball physics and the fixed court camera retain their behavior.

Courts map onto four environments: pro arena, streetball, training gym and championship. These meshes use the game's court dimensions and basket coordinates. They are not a conversion to a full regulation court.

## Keeping rendering costs down

- One directional light casts shadows. Four fill lights still illuminate players without rendering additional shadow maps.
- Low turns shadows off. Medium/High/Ultra use 1024/1536/2048 shadow maps; replaced maps are disposed.
- Mobile, data-saving connections and Low use the lighter player models. A detail change takes effect for player rigs at the next match so a shot cannot swap skeletons mid-animation.
- The active environment changes detail with quality. Only one environment is retained; replaced geometries, materials and textures are disposed.
- Accessories and jersey numbers do not cast shadows.
- The built-in fallback crowd uses 12 instanced batches, bounded population and reduced animation updates. Hidden crowds do not update.
- Automatic quality starts conservatively and only steps down during a match.

The earlier controlled 1280×800 Medium 3v3 test reduced the built-in scene from 1,248 to 319 draw calls before replacing its arena/player assets. This measures renderer work, not real-device frame rate. Device testing is still needed for FPS and battery-life comparisons.

## Loading and recovery

Startup keeps gameplay paused behind a loading message while essential graphics settle. Escape, controller pause and window blur cannot bypass that loading lock. Requests time out after eight seconds; environment decode/import and overall startup also have deadlines.

If Cloudflare is unavailable, the game uses the three bundled low-detail player GLBs and its generated court. The original procedural players remain a last fallback. The root manifest is also bundled. The fallback does not require downloading every court.

Set `enabled: false` in `assets/graphics-v2-config.js` to turn off the external scene/player adapter. The rendering optimizations remain active; remove the extra stylesheet link in `index.html` as well if reverting the menu colors and hero artwork.

Inspect `Basketball2Graphics.diagnostics()` in the browser console for the current environment, loading state, requests, errors and draw counts.

## Validation

Run the existing game unit suite:

```text
npm run test:basketball2
```

Run the graphics integration browser test with the repository's Playwright dependency:

```text
node game/basketball2/tests/graphics-v2-browser.cjs
```

That test uses the configured Cloudflare URL by default and covers all four environments, profile colors, unchanged camera, shadow limits, Low on the next match, mobile assets and a simulated CDN outage. `GRAPHICS_ASSET_DIR` can point to a local copy of the uploaded root for offline runs. `THREE_BUNDLE` can point to a Three r160 UMD bundle. `GRAPHICS_REPORT` and `GRAPHICS_SCREENSHOT` optionally write a JSON report and an actual-game screenshot.

The vendored GLTFLoader comes from Three.js r160 and uses the game's existing Three global. Its MIT license is included beside it. Environment GLBs have embedded textures and require no compression extensions or extra decoders. Player GLBs keep the custom loader's uncompressed contract.
