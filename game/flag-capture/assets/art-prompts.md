# Flag Capture: Riftbound art provenance

Created 2026-11-04 with the built-in OpenAI image generation tool. Original, project-specific character artwork; no external game assets or third-party character references.

`units-atlas.png` is a transparent RGBA PNG, 1448 × 1086 pixels. The atlas has four equal columns and three equal rows (362 × 362 pixels per cell). Every character has clear transparent gutters.

| Row | Faction | Columns, left to right |
| --- | --- | --- |
| 0 | Lumen Brood | leader, scout, brute, mystic |
| 1 | Crooked Carnival | leader, scout, brute, mystic |
| 2 | Aureate Choir | leader, scout, brute, mystic |

## Original generation prompt

+Use case: stylized-concept
Asset type: production-ready transparent character sprite atlas for the original tactical strategy game Flag Capture: Riftbound.
Primary request: ONE sprite-sheet artwork containing exactly 12 original full-body cartoon troops, arranged in a precise grid of 4 equal columns and 3 equal rows. Transparent background. Every sprite isolated and entirely contained in its equal cell, with generous clear gutters, same scale, centered in each cell. Canvas landscape 4:3 aspect ratio, effectively twelve square cells. No grid lines or cell backgrounds.
Style/medium: charming hand-drawn ink cartoon with thick dark expressive outlines, lively angular silhouettes, flat vibrant cel shading and subtle painterly highlights, premium indie tactics game character design. Slight three-quarter view, all facing toward the viewer/front-right, expressive and readable at small size. Not pixel art, not 3D renders.
Subjects by exact grid position, reading left-to-right:
TOP ROW, cyan/teal bioluminescent alien faction Lumen Brood:
1. Leader: proud squat alien with a crown of glowing jellyfish fronds, three eyes, tiny floating teal moon-orbs.
2. Scout: nimble tiny four-legged star-eyed alien with antennae and a big curling tail.
3. Brute: broad muscular turquoise amphibian-crab creature with enormous rounded shell fists and glowing cyan spots.
4. Mystic: elegant long-necked alien orchid creature with tendrils, a luminous pollen orb and a petal hood.
MIDDLE ROW, coral-orange and violet surreal Chaos beast faction Crooked Carnival:
1. Leader: merry multi-eyed asymmetrical horned coral beast with a twisting crescent head and a violet ruff.
2. Scout: wiry orange one-eyed spring-legged beast with curling rabbit-like horns and a toothy grin.
3. Brute: enormous round coral-orange three-armed beast, tiny horns, purple belly and silly furious face.
4. Mystic: violet floating beast with an oversized single eye, several small curling tentacles and orange magical sparks.
BOTTOM ROW, ivory/gold angelic abstract faction Aureate Choir:
1. Leader: hovering gold-ring celestial being, ivory feather wings, luminous star face inside a golden halo.
2. Scout: small swift ivory birdlike angel creature, little golden halo, sweeping ribbon tail and tiny spear.
3. Brute: sturdy rounded ivory ceramic guardian with golden seams, shield-like wing arms and a halo.
4. Mystic: floating ivory veiled celestial with three stacked golden eye-rings, little feathered wings and light-orb hands.
Constraints: exactly 12 complete sprites, 4 columns by 3 rows, one sprite per cell, no overlaps across cells, each remains inside its cell with clear margins. Preserve strong faction colors and four distinct troop silhouettes per faction. No text, letters, labels, UI, emoji, captions, logos, borders, panels, terrain, shadows cast on background, or watermark. Genuinely transparent background.

## Final spacing refinement prompt

+Use case: precise-object-edit
Asset type: transparent 4-column by 3-row game sprite atlas, edit target is the attached atlas.
Primary request: preserve exactly the existing twelve creature designs, colors, cartoon drawing style, row/column order, and 4:3 canvas, but improve practical sprite-sheet spacing. Shrink every creature within its OWN equal square cell to occupy at most 78% of that cell's width and height, centered in that cell. Keep precisely four equal columns and three equal rows, no gridlines. Every sprite and its entire halo, tail, antenna, orb, wing, magic spark MUST fit within its own cell with at least 10% of the cell size as fully transparent gutter on every side. Use genuine clean transparency around sprites; remove background haze and excessive outer glowing particles. Characters should remain crisp and detailed with dark ink outlines. Complete any antenna or feather that was clipped by a cell or canvas edge. Nothing else changes: same twelve designs, same faction colors and roles, exactly one sprite in each cell, no words, labels, UI, or backgrounds.
