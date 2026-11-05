# Trash Tactics artwork

## Generation mode

Both illustrations were generated with the built-in `image_gen.imagegen` tool. The atlas request used `transparent_background: true`; the lobby illustration used `transparent_background: false`. No CLI or API-key fallback was used.

## Saved files

All paths below are relative to the Trash Tactics project folder.

| Illustration | Generated source | Preserved project source | Optimized production files |
| --- | --- | --- | --- |
| Character atlas | `exec-8541cf5d-ef69-4727-b010-ea76aacc86a3.png` | `art-source/characters.png` | `dist/assets/rock-icon.png`, `dist/assets/paper-icon.png`, `dist/assets/scissors-icon.png`, `dist/assets/twig-icon.png` |
| Lobby battle illustration | `exec-4ef3fae6-2695-4291-aab1-edca3be073bd.png` | `art-source/lobby-art.png` | `dist/assets/lobby-tile.jpg` |

The 1,254 × 1,254 atlas was cropped into four equal quadrants, preserved as `art-source/rock.png`, `art-source/paper.png`, `art-source/scissors.png`, and `art-source/twig.png`. Production icons were resized to 256 × 256 while preserving alpha transparency. The lobby tile was resized to 1,200 × 675 and encoded as JPEG at quality 88. Original generated artwork is retained in this folder.

## Exact final prompt: character atlas

```text
Use case: stylized-concept. Asset type: transparent 2x2 character sprite atlas for a cartoony RTS browser game. A single square canvas with four isolated full-body anthropomorphic characters, each centered in EXACT equal quadrant cell with large transparent margins and no overlap. Upper-left: chunky irregular gray ROCK wearing an orange headband, little expressive arms and sturdy legs. Upper-right: smiling cream PAPER sheet with a teal folded corner, expressive arms and little legs. Lower-left: silver open SCISSORS with orange loop handles as lower body, arms and legs. Lower-right: adorable branching brown TWIG with arms, little legs, a single small green leaf, a worker. Consistent cute confident board-game cartoon style, thick clean dark charcoal hand-drawn outlines, warm color palette, softly shaded dimensional cutout illustration, expressive faces. Full bodies wholly inside their quadrant with equal visual scale. Transparent background with real alpha, no background panels or grid lines, no text, no labels, no watermark. Characters stand ready for a playful battle. Production-ready crisp raster game icons.
```

## Exact final prompt: lobby illustration

```text
Use case: stylized-concept. Asset type: landscape 16:9 illustrated game lobby tile for a playful browser RTS called Trash Tactics. A dynamic cartoony miniature tabletop battle scene on a warm cream office desktop: anthropomorphic chunky gray ROCK soldiers with arms and legs, smiling cream PAPER sheet soldiers with teal folded corners and arms and legs, silver SCISSORS soldiers with orange handles and arms and legs. Adorable branching brown TWIG workers with arms and legs and tiny green leaves carry colorful scraps from little trash cans back toward a toy office desk headquarters. A friendly toy printer factory and supply cabinet are clearly visible in the background. Clean thick charcoal hand-drawn outlines, soft dimensional shadows, polished board-game illustration, warm cream, teal, terracotta orange and lavender accents, cute and expressive. Exciting fun action with paper confetti and scrap resources. Strong composition that reads small as a game tile, characters prominent. No words, no logo, no text, no watermark. Raster illustration.
```


## Transport and pointer-defense artwork

`render.js` draws the CD reader, flying CDs, defensive computer mouse, computer-pointer projectiles, transport beams, and drop markers directly with Canvas paths. These extend the game's existing illustrated building renderer and need no downloaded assets. The same `drawIcon` artwork appears in production and construction controls and the tutorial. CDs include a spinning iridescent surface, hovering shadow, team trim, six cargo pips, and passenger count. The CD reader has an opening tray and launch light; the mouse has a cable, split buttons, wheel, and click animation. High-contrast arrow cursor projectiles have team-colored trails and impact feedback.
