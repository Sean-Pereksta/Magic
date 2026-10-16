# Artwork and atlas specification

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
