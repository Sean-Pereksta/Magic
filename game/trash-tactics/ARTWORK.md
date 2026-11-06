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

## Tier 2 and Tier 3 Warfare expansion

The new painted artwork was created with the built-in image generator using the original Rock, Paper and Scissors PNGs as visual references. Each new portrait then served as the reference for its character's transparent four-by-four animation sheet. The effects were generated as transparent lifecycle artwork in the same shaded illustration style. Exact final prompts and source references are recorded in `assets/warfare-prompts.json`. Reusable transparent PNGs and optimized runtime WebP files are both preserved.

The expansion uses six original shaded troop portraits and six actual animation sheets generated to match the existing illustrated Rock, Paper and Scissors artwork. `assets/sniper-icon.png`, `airplane-icon.png`, `mage-icon.png`, `divider-icon.png`, `rollingrock-icon.png` and `shears-icon.png` provide the interface portraits. Their corresponding `*-sheet.png` files provide sixteen distinct painted animation frames each. The battlefield renderer crops one frame at a time from each four-column, four-row sheet. Rows are Idle, Move, Attack and Special, played at 4, 8, 12 and 8 frames per second respectively. Aiming and casting advance their special frames according to cast progress. Code-driven orientation, recoil, banking, diving, rolling, trails and afterimages supplement the painted frame animation.

The animation layout, frame sizes, state rows and timing are recorded in `assets/warfare-animation.json`. Canvas artwork supplies a graceful fallback while sheets load and retains the animated buildings, terrain, spell boundaries, targeting guides and team markings. The game has no external art or font requests at runtime. `warfare-gallery.html` is a responsive preview that plays all six troop sheets and all six effects, with Play/Pause and Idle/Move/Attack/Special controls.

The runtime prefers the optimized `.webp` counterpart of every new portrait and animation sheet, preserving transparency. PNG versions remain as reusable art exports and automatic image-loading fallbacks. The original four Tier 1 PNGs are unchanged.

`assets/warfare-effects-sheet.png` supplies eighteen distinct painted effect frames in three columns and six rows. The six rows are precision impact, paper-airplane crash, shadow slash, dome pulse, division swirl and heavy rock impact. Their three columns show early, middle and late stages, selected by each effect's lifetime. Effects crop the real lifecycle frame, then expand, rotate and fade it above the battlefield. `assets/warfare-effects.png`, the original static three-column, two-row effects atlas, remains a loading fallback. Exact collision outlines and the three equal division sectors remain vector geometry, keeping their gameplay boundaries clear at every zoom.

| Asset | Original design and animation |
| --- | --- |
| Scissors Sniper | Slender metal blades, teal handle loops, a pencil-stock rifle, blade barrel and binder-clip receiver. A pulsing optical scope, crouched aiming pose, targeting line, shot progress arc, muzzle flash and rifle recoil communicate the precision shot. |
| Paper Airplane | Layered cream and pale teal paper folds, drawn eyes, wing markings and a tiny dart launcher. Facing turns the entire aircraft, wings bank during flight, altitude leaves a ground shadow and the final dive lowers the aircraft toward its actual impact. Smoke, paper scraps, expanding shockwaves and a bright impact flash complete the crash. |
| Rock Mage | A faceted stone wizard, folded scrap-paper hat, wooden staff with a glowing crystal and three orbiting pebbles. Domes include a circular collision boundary, translucent curved shield, moving highlights, energy lines and an expiration fade. Two visible charge pips accompany a selected Mage. |
| Paper Divider | A floating origami archmage with luminous robe symbols, folded-paper staff, orbiting sheets and three casting sigils. Triple Division has three equal family-coloured sectors and corresponding family icons, a glowing outer ring, elevated radial walls, moving light pulses, paper ribbons and a collapsing-ring effect. |
| Rolling Rock | A large asymmetric armoured boulder with cracks, a determined face and tiny limbs. It retracts its limbs during a rotating roll. Dust plumes, pebble impacts and destination marks show momentum and contact. |
| Shadow Shears | Dark metal blades, violet eyes, pale cutting edges and animated black ribbons. The dash produces fading silhouettes, luminous trails, multi-arc slash flashes and a finishing effect. The command preview shows committed route points plus the pending route segment. |
| Prototype Workshop | Discarded printer shell, cardboard workbench, experimental tools, pencil antennae and exposed gears. Training turns the gears and cycles a mechanical arm with a bright working tip. |
| Shredder Laboratory | A heavy office shredder with visible teeth, twin gear systems, front conveyor, paper feed, illuminated controls and a status beacon. Training animates the conveyor, gears, teeth and falling paper. |
| Stapler Canyon | Layered canyon walls and shadows share the exact terrain footprint. The northern ruler bridge has scale marks; the southern cardboard crossing has corrugation and tape. A large neutral mechanical stapler marks the destructible central gate and scatters metal parts when destroyed. Existing layered plateau and ramp artwork supplies the high-ground silhouettes. |
| Conveyor Chaos | Recycled factory floor, cross and loop conveyors with moving treads and directional chevrons, industrial recycling machines and control consoles. Capturing a console draws a progress ring. Reversing a belt changes both tread movement and arrow orientation. Factory gears rotate above the cached background. |

Static map artwork remains in the existing cache capped at eight million pixels. Troops, belts and spell effects draw at full display resolution. Troop portraits, animation sheets and effect sheets preload once and reuse the decoded images. Offscreen ability geometry, projectiles and map animation are culled; hidden entities and effects retain fog checks. Original Tier 1 raster portraits and their animation remain intact.

Validation: `node --test tests/warfare-art.test.mjs tests/render-terrain.test.mjs` covers all new icons, real animation-row selection and loops, individual frame cropping, all eighteen effect-frame crops, loading fallbacks, both maps with two to four players, finite spell and projectile geometry, air layering, fog checks, scenery caching, reversed conveyor graphics and route previews.
