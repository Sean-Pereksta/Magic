# Frontier Medic healing artwork and integration

The Field Medic now uses a dedicated 64-pose atlas: eight facings, with two preparation poses, two activation poses, a two-pose active loop, and two completion poses in each facing. The animation turns toward the recorded patient. The equipment light, animated green-white beam, traveling energy pulses and restrained patient-body sparkle use separate body and effects artwork. No healing ground circle is added.

## Delivered artwork and Cloudflare upload

Upload these files to the existing bucket root, retaining these exact keys:

| Repository file / bucket key | Dimensions | Bytes | SHA-256 |
|---|---|---:|---|
| `units/medic-healing.webp` | 1024 × 1280 | 347796 | `43833586660f7fb965099de1406d1188813a5fcc93dfc7a42ca9d953febc29af` |
| `effects/medic-healing.webp` | 1024 × 256 | 78910 | `1e690aa4a1f61d71b7181abd5e75ee881d90ef994b76f426027ffda487663a0e` |

Canonical public URLs:

- `https://pub-cb55798f1eeb4f80a85463da3612ed61.r2.dev/units/medic-healing.webp`
- `https://pub-cb55798f1eeb4f80a85463da3612ed61.r2.dev/effects/medic-healing.webp`

Use `Content-Type: image/webp`. Both atlases contain genuine RGBA transparency. No bucket credentials were available in this task, so the files are committed with the application and work immediately through local paths `/units/medic-healing.webp` and `/effects/medic-healing.webp`. The renderer falls back to the canonical R2 URL if a local file is missing. The preload catalog selects these local assets, avoiding a loading failure before the manual upload. Existing artwork remains on its current R2 paths.

Manifests already updated:

- `game/turncraft/assets/animation-manifest.json`: `assets.medic.states.healPrepare`, `healActivate`, `healLoop`, `healComplete`; measured equipment attachment coordinates under `assets.medic.healing.deviceAnchors`; the separate effect under `effects.medicHealing`; and `localFallbacks` for both canonical URLs.
- `game/turncraft/assets/unit-animation-manifest.json`: identical Medic states, attachment coordinates and local paths.
- `game/turncraft/assets/effects-manifest.json`: `medicHealing` declares the separate beam and body-impact rows.
- `game/turncraft/assets/visual-asset-index.json`: exact dimensions, sizes, hashes and local file locations.

The existing artwork packaging tool includes both files under their top-level bucket keys. The new `tools/package-medic-healing.py` packages just these two files without requiring a download of the existing artwork collection. After the uploads are verified, the local copies can remain as a deployment fallback. To choose R2 as the first source instead, remove these two URLs from `animation-manifest.json.localFallbacks` and the corresponding `localFallback` fields from `visual-asset-index.json`; retain `localPath` so offline verification and packaging continue to use the committed files.

## Layout and crop compatibility

Rows are N, NE, E, SE, S, SW, W and NW. Every body cell is 128 × 160. Columns 0–1 prepare, 2–3 activate, 4–5 loop, and 6–7 complete. Ground anchors are aligned at pixel (64,146). Each cell has at least four transparent pixels on every edge. Measured isolated crops remove disconnected neighboring-row fragments before packing. The combined manifest also records the final decoded alpha bounds and original destination coordinates for every healing pose in its crop audit; this preserves the artwork without resizing or changing its anchor. No pose is created by mirroring, rotating or duplicating an existing frame. The original Medic idle, movement, attack, hit, death and ability atlases and their measured crop repairs remain intact.

The effects atlas has eight 128 × 128 cells per row. Row 0 contains eight animated horizontal beam textures, cropped at cell-local (4,40,120,48) and stretched between the current equipment and patient positions. Row 1 contains eight restrained body-impact sparkle frames.

Artwork was generated using the built-in image tool from the current public Medic locomotion reference. A targeted second generated pass replaced broad device bloom with a small equipment light. Cropping, consistent cell packing and WebP optimization preserve the generated character artwork. The complete prompts are in `turncraft-medic-healing-prompts.json`.

## Timing and synchronization

Existing healing remains 10 health per tick with its unchanged range and scaled cooldown. Target injury priority is unchanged. Each successful automatic heal records `healTargetId`, `healStartedAt`, `healUntil`, `healRange`, the current order and ability clock. These small fields are included by the existing entity serializer, so remote observers derive the same animation from authoritative data.

Preparation takes 140 ms, activation 160 ms, the active beam lasts through the recorded deadline, and completion takes 220 ms before returning to the normal idle or movement state. A beam stops in the interrupting frame when the Medic or patient dies, enters transport, changes faction, leaves range, becomes hidden, or when the Medic receives another order, attacks or uses an ability. Both ends follow interpolated presentation coordinates. Device anchors use the same quality-adjusted animation frame clock as the body. Healing art does not reschedule damage, healing, movement or cooldowns.

Low/Medium/High draw 1/3/5 traveling motes and limit simultaneous visible beams to 12/32/64. Offscreen pairs do not consume the visible beam budget. Texture resolution continues to use the existing quality setting and bounded cache.

## Validation

- `node --test game/turncraft/tests/medic-healing.test.cjs game/turncraft/tests/animation-runtime.test.cjs game/turncraft/tests/preload.test.cjs`: 43 passing tests.
- `node --test game/turncraft/tests/medic-healing-browser.test.cjs`: a real Chrome decode/render check passes. It checks all 64 body poses have distinct pixels, all four-pixel cell gutters are transparent, both atlases decode, the beam paints pixels between body-attached endpoints, and interruption removes it immediately. Set `CHROMIUM_EXECUTABLE_PATH` when the system browser is used instead of a Playwright browser.
- Lifecycle checks cover preparation, activation, active loop, completion, idle, movement tracking, fog, death, transport, attack/ability/order interruption, injury priority, unchanged health/cooldown values, and observer snapshot fields.
