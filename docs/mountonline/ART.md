# Artwork and atlas specification

## Living Kingdoms signature atlas — 2026-10-24

`graphics/mountonline/units/signatures-v3.webp` adds twenty dedicated unit rows with eight poses each. The transparent 768 × 1920 atlas uses 96 × 96 cells and the existing idle/walk/attack/brace/fall animation convention. Its 652,622 bytes bring all essential atlases plus the hero to 2,350,478 bytes. The manifest and Cache API key use artwork version 3.

The built-in image-generation tool created the new bitmap using the existing levy atlas as a style reference. Prompt specification: a transparent, hand-painted semi-realistic medieval 2.5D side-view sprite atlas; twenty equally spaced rows in the exact faction-unit order from `data.mjs`; eight animation columns for idle, two walking poses, attack windup, strike/release, braced defense, falling, and fallen; complete centered bodies, weapons and mounts within each cell; consistent baseline and padding; crisp silhouettes at 55 pixels; no text or grid lines. Each unit's equipment and faction colors follow the roster, including tower shields, explosives, mounted bows/lances, pavises, pikes, axes, javelins, chain flails, and hurled pots.

The returned 793 × 1983 transparent source contained nine columns. Packaging selected columns 0–5 and 7–8, removing the redundant upright pose, and resized each cell independently into the final eight-column grid. Alpha was retained. All twenty portraits were verified against their manifest crops in desktop and phone browser tests. No original atlas was replaced.

`scripts/build-mountonline-assets.cjs` accepts an optional prepared eight-column/twenty-row signature source. A three-source rebuild preserves the deployed signature atlas. Both bitmap rendering and roster portraits read cell dimensions from the manifest. Upload the complete `graphics/mountonline` directory when changing the remote art base.

The troop and world bitmap atlases were created with the built-in image-generation tool for this rebuild, then resized/encoded with Sharp. The original rendered Mountain Blade lobby tile supplies the menu backdrop. The command icons are original native SVG paths.

## Troop sheets

`units/levies.webp` and `units/elite.webp` each have eight columns and seven rows, normalized to 1280 × 1120 pixels (160 × 160 per cell). Crop two pixels inside every cell. Characters face right in the source; the renderer flips the frame for left-facing movement.

| Sheet | Rows, top to bottom |
| --- | --- |
| Levies | Recruit; Archer; Footman; Monk; Veteran archer; Crossbowman; Swordsman |
| Elite | Cavalry; Priest; Mage; Roman sergeant; Roman champion; Dragon knight; Venetian saint |

| Column | Pose |
| --- | --- |
| 0 | Idle |
| 1–2 | Alternating walk/trot |
| 3–4 | Attack windup and strike/release |
| 5 | Defensive brace |
| 6–7 | Falling and fallen |

Prompt specification used for each sheet: a production medieval strategy sprite animation atlas on real transparent RGBA, exact 8 × 7 equal-cell grid, no gutters, text, borders, or scenery; one full character per cell; orthographic three-quarter camera facing right; identical floor baseline and ample transparent margins; polished painted steel/bronze armor with blue cloth accents, readable at small in-game size. Each row repeats its named character through the eight specified poses. Cavalry and dragon knights use brown and armored black horses respectively. Roman units have red crests; monks have brown robes; priests and saints have ivory/gold robes; mages carry an ember staff.

## World sheet

`world/atlas.webp` has four columns and four rows, normalized to 1024 × 1024 pixels (256 × 256 per cell). The first eight cells are opaque ground textures: meadow, forest floor, rocky ground, river water, dirt road, dry meadow, snow, and bridge planks. The final eight cells are cutout oak trees, alpine mountain, village, keep, dark citadel, ruined watchtower, horse-drawn trade cart, and commander standard.

Prompt specification: detailed painted medieval RTS artwork in warm olive meadows, steel-blue slate, and golden sunlight; exact 4 × 4 grid with no gutters or labels. Ground tiles fill their cells. Props use a consistent elevated camera, grounded baseline, and transparent margins. A follow-up background-extraction edit removed visible checkerboard from the bottom object rows while preserving the opaque terrain rows and grid positions. The optimized atlas retains alpha.

## Rebuilding optimized files

```bash
node scripts/build-mountonline-assets.cjs infantry-source.png elite-source.png world-source.png
```

Source PNGs are not runtime dependencies. The script emits WebP files and frame metadata; it also copies the existing `lobby/assets/game-tiles/mountonline.webp` backdrop. Inspect cell boundaries, transparency, all 14 classes, and animation states after changing source art. Do not substitute a missing sprite with an emoji or collision shape. The introductory screen reports an asset failure and keeps game entry disabled until essential assets load.
