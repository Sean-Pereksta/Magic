# Football asset authoring — first milestone

The downloadable `ReceiverWindowQB_NextGen_Football_Models.zip` contains the actual player, equipment, texture and animation files. This directory preserves their authoring source, browser inspector, validation tools, manifest contract and acceptance summary. Generated binaries are distributed in that archive for the owner's R2 upload.

The pack has 16 distinct athletes, each with a full-detail and lower-detail GLB, a shared 50-joint rig, nine physique targets, and 31 authored animation clips. The source creates anatomical cross-section surfaces and detailed equipment independently of the current game's procedural figures. Art direction is stylized realism.

`../player-assets.js` is a prepared loader with no URL configured and no game-page hook. Current player rendering, saves, contacts, replay and controls are unchanged. Supplying a URL later enables configuration; production use still requires connecting this rig to the shared football simulation and completing compatibility and performance checks.

## Rebuild and inspect

Use Blender 5.2. Copy this authoring directory to a staging folder, preserving its `preview/`, `source/`, `README.md` and `LICENSE.txt`. Copy the current `../player-assets.js` and `../player-assets.test.cjs` into the staging folder's `source/` before running prepared-loader checks. Replace the staging README with `ASSET_PACK_README.md` for distribution.

From the staging folder:

```text
blender --background --python source/build_football_assets.py -- --pack /absolute/path/to/staging
python preview/serve.py
```

The browser viewer loads actual exported GLBs and supports animations, LOD selection, team colors and physical body adjustments. It includes its matching Three.js r160 dependency locally. Keep the original pack structure; direct `file://` viewing cannot fetch modules and models reliably.

The physique implementation in `preview/physique.js` modifies mesh targets, rest joints, inverse binds and animation translation tracks together. Create it before actor placement and use independent clips/skeletons; later profile changes preserve placement. The prepared loader isolates material, skeleton, inverse-bind and clip state while caching shared geometry/textures.

GLBs are in meters, +Y up and +Z forward. The current field uses two world units per yard (about 2.18723 per meter), while legacy avatars have a separate stylized scale. Production integration must reconcile player/field scale, camera, movement, IK, ball and contact measurements together. Reference glove anchors and extended rig reach must never become automatic catches or invisible extra reach.

## Validate and package

In `source/validation/`, run `npm ci`, then follow its README with the absolute staging path. This checks glTF format and binary data, actual Three.js imports, every-frame animation surfaces, anatomical adjustments, clone independence and the prepared loader against a real GLB. Run `node --test source/player-assets.test.cjs` from staging for isolated cache/resource tests.

After all reports pass:

```text
python source/package_assets.py --pack /absolute/path/to/staging --output /absolute/path/to/ReceiverWindowQB_NextGen_Football_Models.zip
```

Packaging checks the full reports and exact validated GLB hashes, writes checksums for the unpacked contents and verifies the ZIP. `ASSET_PACK_README.md` documents artist limitations, physical units, rig conventions, rebuild steps and R2 folder/CORS requirements.

## Delivery boundary

This milestone supplies assets and inactive loading preparation. Passing windows, catch/defender gameplay, academy and league modes, franchise expansion, training and save migrations remain later work. R2 activation waits for the owner's uploaded public base URL. No target FPS or completed gameplay acceptance is claimed by the offline reports.
