# Cats and Mice tactical art

This original art pack ships with the game. It contains twelve tactical animal
operators, six weapons, eighteen portraits / weapon previews, two faction badges,
five sector markers, and a campaign menu banner. The portraits are vector renders
of the same authored geometry and equipped weapons used in the game.

## Rebuilding

From the repository root, using Python 3.10 or newer:

```sh
python game/neon-arena/tools/build-factions.py
```

There are no Python package requirements. `build-characters.py` supplies generic
mesh-construction helpers; the new operator body, anatomy, equipment, and weapon
meshes are authored independently in `build-factions.py`. Output is deterministic.
An alternate destination can be supplied with `--output /path/to/assets`.

`manifest.json` records the version, exact paths, SHA-256 hashes, byte counts,
triangle counts, material counts, bounds, and rig metadata for every GLB. Each
character uses 6,244–7,432 triangles (399–450 KB); each weapon uses 2,386–3,406
triangles (192–268 KB). All geometry is embedded. Models use indexed triangles,
unit vertex normals, opaque materials, and named rigid parts supported by the
existing renderer. No skeletal extension, compression decoder, texture service,
runtime geometry generator, or new backend is required.

## Runtime integration

Load `neon-factions.js` before `neon-assets.js` and `neon-models.js`.
`NeonFactions.characters` and `.weapons` contain labels, roles, file paths, and
weapon tuning. `.characterById` and `.weaponById` provide lookup by stable ID.
Player choices remain independent of the original Survival cosmetic unlocks.

Each character has nine shared rigid groups: body, head, left/right arms,
left/right legs, tail, and left/right ears. Semantic side labels keep the left
support hand attached to its own arm even when it crosses the character's
center. Meshes are in a rifle-ready stance. Feet rest on Y=0; body height is 2.39.
Head aiming, locomotion, jump/slide/crouch poses, firing, reloading, death,
tail sway, and small ear movements use the existing runtime pose system. Ear
motion inherits head aiming. Ears and tails are cosmetic extremities, so their
height does not shrink the body or change competitive hitboxes.

The world-space weapon receiver is placed at local `[0.32, 1.43, 0.43]`, with
uniform scale `0.70`, before applying actor yaw and ground offset. Weapons face
+Z. These offsets match the authored trigger hand and support hand. Magazine,
bolt, and shotgun pump geometry have independent animation groups; a Purr-90
magazine lifts upward during reload. New weapon barrels remain fixed when the
bolt cycles. The shotgun's tube magazine remains mounted.

Models and GPU buffers are shared across actors. The existing per-view animation
budget limits articulated actors; other actors draw in one call. This is a
stylized rigid-part animation system, not a motion-captured skeletal rig.

## Local hosting and optional CDN

The default `EXPANSION_ASSET_BASE_URL` is `./assets`. All newly referenced files
exist in the repository, including aliases such as `weapons/whisker.glb`, which
resolve to `assets/weapons/new/whisker.glb`. Original weapon/alien hosting remains
unchanged. Campaign, faction selection, and Conquest do not need an R2 upload for
these models.

To publish the pack to R2 or another CDN, copy the asset files preserving the
exact paths in `manifest.json` and `NeonFactions.assetPaths`, then point
`EXPANSION_ASSET_BASE_URL` to that uploaded root. For example, the exact object
keys are `factions/cats/cat_assault.glb`, `factions/mice/mouse_runner.glb`, and
`weapons/new/whisker.glb`. Keep `EXPANSION_FALLBACK_BASE_URL: './assets'` so an
unavailable or corrupt remote model falls back to its bundled counterpart.
Versioned URLs prevent stale cached revisions. No upload has been assumed.

## Validation

```sh
node --test game/neon-arena/tests/faction-assets.test.cjs tests/neon-assets.test.cjs
node --test game/neon-arena/tests/characters-models.test.cjs
node game/neon-arena/tests/model-renderer.cjs
node tests/neon-factions-browser.cjs
```

The new tests load every real GLB, verify hashes and normals, validate all catalog
URLs and CDN fallback, check distinct geometry and semantic anatomy, exercise
ear/head aiming, grounded crouch, shared GPU buffers, bounded animated draw
calls, magazine/bolt/pump movement, and foreground depth restoration. Existing
human-rig and renderer tests cover compatibility with the original assets.

The browser test requires Playwright and Chrome/Chromium (`CHROME_PATH` can
override its location). It renders all eighteen real models through the game's
WebGL renderer, checks eight animation states against an idle pose at the same
time, verifies all 44 gallery asset URLs, and writes contact sheets and a report
to `NEON_QA_OUTPUT` or `work/faction-qa`. Chrome with SwiftShader passed these
checks during this implementation. The geometry tests additionally verify
outward cheek normals and the actual trigger-hand / weapon-grip positions.

Desktop/mobile screenshots and real-device play remain necessary to evaluate
art direction, hand alignment while aiming at extreme pitch, and sustained
frame rate. The 60 FPS desktop / 30–60 FPS mobile targets are not certified by
geometry-budget tests. Shieldbearer equipment is cosmetic; the folded pack
shield does not silently add a new gameplay shield ability.
