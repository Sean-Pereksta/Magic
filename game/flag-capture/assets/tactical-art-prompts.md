# Tactical troop artwork

Generated with the built-in `image_gen` tool on 2026-11-05. No CLI fallback or image-editing scripts were used. The existing `units-atlas.png` supplied only the game's visual style; the nine troop designs are new.

Final asset: `tactical-units-atlas.png`, 1254 × 1254, 8-bit RGBA PNG, arranged in an exact 3 × 3 grid of 418 × 418 cells. Columns are `guardian`, `ranger`, `shaper`; rows are `bloom`, `chaos`, `choir`. Keep the original atlas unchanged.

| Row / faction | Guardian | Ranger | Shaper |
| --- | --- | --- | --- |
| 0 · Lumen Brood | Mossplate Bulwark | Thornskate | Mycelium Weaver |
| 1 · Crooked Carnival | Grinlock Bastion | Rift Jackal | Inkmaw Jester |
| 2 · Aureate Choir | Aegis Cantor | Sunlance Kite | Halo Scribe |

## Generation prompt

Use case: stylized-concept.

Asset type: one production game sprite atlas, 3 columns × 3 rows of nine distinct cartoon strategy-game creatures on a genuinely transparent alpha background.

Reference image role: STYLE REFERENCE ONLY. Match the existing game's chunky dark ink outlines, vibrant cel shaded painterly cartoon finish, approachable weird creature shapes and three faction palettes. Do not recreate or include any of its existing characters. Make new characters matching the following nine exact cell subjects.

Canvas/layout: square image divided into a mathematically exact 3×3 lattice of equal square cells. There are no visible cell borders, background, text, captions, numbers, scenery, ground planes, cast shadows, or grid marks. Each character is centered at the midpoint of its own cell, fully contained with at least 12% transparent padding from every cell edge; the nine center coordinates are x=1/6, 1/2, 5/6 and y=1/6, 1/2, 5/6. Identical visual character height around 72% of its cell. Every cell contains precisely one complete character. No overlap across cells. Slight three-quarter top-down readable battle miniature angle, facing slightly right, consistent lower baselines.

Top row: teal and turquoise bioluminescent alien faction, lavender frills, cream underbellies, yellow eyes. Top-left guardian: Mossplate Bulwark, a stout six-legged armored moss-and-coral crab with a huge living shell shield, broad defensive silhouette. Top-middle ranger: Thornskate, a sleek hovering alien thorn manta creature on long spindly legs, barbed leaf fins, narrow fast silhouette. Top-right shaper: Mycelium Weaver, a mushroom-headed octopus alien weaving a compact loop of luminous roots between its short tentacles, soft purple mushroom cap and teal tentacles.

Middle row: coral-pink and purple surreal Chaos beasts with amber eyes. Middle-left guardian: Grinlock Bastion, a squat accordion shell monster with an enormous grin-shaped shell shield, tiny asymmetrical feet, broad armored silhouette. Middle-middle ranger: Rift Jackal, a lanky asymmetric multi-eyed leaping alien dog with crescent horns, tightly curled tail and long angular limbs. Middle-right shaper: Inkmaw Jester, an ink squid-jester beast with an impossible grin and a short twisting staff, several elegant purple tentacles and coral jester hood; no humanoid body.

Bottom row: ivory, cream, warm gold angelic creatures, dark ink outlines, small amber glowing accents. Bottom-left guardian: Aegis Cantor, a plump non-humanoid winged guardian orb with a large solid circular halo shield, small armored feet, broad cherubic silhouette. Bottom-middle ranger: Sunlance Kite, a long-winged geometric angel bird carrying a short sun spear in its talons, elegant narrow bird silhouette. Bottom-right shaper: Halo Scribe, a floating non-humanoid many-ring angel with small ivory wings and a luminous unfurled scroll held between two tiny abstract hands, ring/scroll silhouette.

Constraints: true transparency, not a checkerboard drawing. No humanoid reskins, no existing reference characters, no extra creatures, no detached particles, no auras, no large effects, no text. Each miniature is cleanly separable at exact one-third cuts and visually readable when scaled down to a small square grid tile. Retain dark crisp outlines and generous transparent gutters.

## Spacing correction prompt

Use case: precise-object-edit. Edit target: the first nine-character transparent sprite atlas. Change only layout spacing and character scale. Preserve every one of the nine character designs, colors, silhouettes, strokes and rendering style, in the same row/column order.

The current characters are too big and some silhouettes cross the exact 3×3 square-cell cuts. Shrink each complete creature to 65% of its current size and reposition it precisely centered in its own mathematical one-third square cell. Maintain the original square canvas and a perfect 3 columns by 3 rows lattice of nine equal square cells. The centers must be exactly (1/6,1/6), (1/2,1/6), (5/6,1/6), (1/6,1/2), (1/2,1/2), (5/6,1/2), (1/6,5/6), (1/2,5/6), (5/6,5/6). Every silhouette and every feather, horn, weapon, halo, shadow, transparent fringe must fit within the middle 65% of its own cell, leaving at least 17.5% completely empty transparent margin between the character and all four edges of its cell. No pixels of any creature can cross the one-third grid lines. Put each character's whole lower extent on a matching baseline at 80% of its own cell height. Keep the transparent canvas; do not draw cell boundaries or checkerboards. No new art, no new characters, no text, no labels, no auras, no particles, no grounds. It should look like nine small clean game cutouts separated by wide transparent gutters.

## Inspection

The selected result was visually inspected at original resolution. All nine requested designs are present in the specified order and remain recognizably different. The PNG's alpha channel contains 1,220,615 fully transparent pixels; no checkerboard or solid backdrop was drawn. A read-only PNG scan confirmed zero pixels with alpha greater than 8 on any cell boundary, so ordinary 300% × 300% CSS sprite-sheet slicing does not cut through a character or pick up another cell's drawing.

The final image intentionally has wide transparent gutters. A larger centered sprite box can match the visual scale of the older atlas while retaining clean frame isolation. The lower row's artwork sits slightly higher inside its cells; a small presentation offset can align its baseline without changing the asset.

Generated source retained under the image tool's generated-images directory. The first draft was not shipped; the final spacing-corrected PNG was copied intact into this project.
