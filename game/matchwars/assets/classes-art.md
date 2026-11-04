# Match Wars class sprite atlas

## Asset and use

- File: `classes.png`.
- Created with the built-in `image_gen.imagegen` tool, in generation mode with a style reference. No CLI fallback or external image API was used.
- Reference: `troops.png`, inspected before the call and supplied only to establish the existing painted fantasy style.
- Requested composition: 2048 × 1536 pixels, eight columns by six rows.
- Native delivered dimensions: **1448 × 1086 pixels**, exactly **8 × 6 cells of 181 × 181 pixels**. The native result is retained without resizing, cropping, color conversion, or other image editing.
- Pixel format: 32-bit RGBA PNG, with a genuine alpha channel. Read-only inspection found **491,331 fully transparent pixels**.
- File size: **2,864,001 bytes**.
- Generated on November 4, 2026.

The sheet contains twelve new allied classes, four frames each. Every class faces right. Within each four-column group, local frames 0–2 are walk poses and frame 3 is the attack/cast pose. Each cell contains a complete isolated character; spell effects belong to that character's attack frame. Use the native 181-pixel cell dimensions, or derive the cell size from image width divided by eight.

## Frame mapping

All indices below are zero based.

| Row | Element | Columns 0–3 | Columns 4–7 |
| --- | --- | --- | --- |
| 0 | Ember | Ember Knight — red armored sword and shield | Phoenix Adept — red/gold winged support with fire staff |
| 1 | Tide | Cryomancer — blue hood and ice staff | Tide Oracle — white/aqua healer with trident |
| 2 | Grove | Bloom Druid — green/white floral healer | Thorn Guardian — bulky bark armor, shield, and mace |
| 3 | Storm | Tempest Mage — cyan/gold lightning staff | Windcaller — teal monk with scimitar and wind ribbons |
| 4 | Sun | Dawn Priest — white/gold healing staff | Sun Crusader — gold armor and two-handed sword |
| 5 | Void | Necromancer — black/violet robe and skull staff | Hexweaver — violet/magenta witch with spellbook |

The atlas was visually inspected at its native dimensions. All 48 cells contain the intended class poses, with consistent element colors, readable role silhouettes, right-facing attacks, no labels or panels, and transparent space between figures.

## Exact generation prompt

```text
Use case: stylized-concept. Asset type: production 2D game sprite atlas for Match Wars, a fantasy lane battle game.

Generate a BRAND NEW sprite atlas for twelve NEW allied character classes. The attached image is a STYLE REFERENCE ONLY: preserve its cohesive illustrated fantasy game style, dark clean outlines, warm/cool elemental colors, large readable heads, small heroic proportions, and hand-painted highlights. Do not modify, copy, or redraw the reference characters. Make twelve clearly different character silhouettes.

OUTPUT LAYOUT IS CRITICAL: exactly 2048 by 1536 pixels, a 4:3 canvas, divided invisibly into EIGHT equal columns and SIX equal rows. Each cell is exactly 256 by 256 pixels. No grid lines. Exactly 48 separate full-body character frames total, one per cell. True transparent RGBA background throughout, including all gaps; NO colored background, checkerboard, floor, panels, labels, text, numbers, watermark, frames, or ground shadows. Keep all weapon tips, wings, spell effects, hair and cloaks contained inside their own cell with empty margin. Center each figure consistently within its cell. Feet meet the same baseline at 85% of cell height; body scales stay consistent. Every single character faces RIGHT in side-view / slight three-quarter profile. All faces, weapon attacks, and spell effects point toward the RIGHT.

Each row contains TWO distinct allied classes. Columns 1,2,3,4 are four animation frames of the FIRST class. Columns 5,6,7,8 are four animation frames of the SECOND class. Within each group of four, first three frames are visibly different phases of a smooth walk cycle (left foot forward, passing pose, right foot forward); the fourth frame is a strong attack or cast pose. Identity, outfit, scale, head and feet remain registered across the four frames. Attack spell effects must fit inside the cell.

ROW 1 (top; fire element): columns 1-4 EMBER KNIGHT, red and bronze plate armor, enclosed crimson helmet, broad sword and compact shield, confident sturdy swordsman silhouette; fourth frame swings the sword with a small orange flame arc. Columns 5-8 PHOENIX ADEPT, red-and-gold robes, distinctive compact feathered phoenix wings, a fire-support staff, feather crest; fourth frame extends the staff and casts a small golden flame toward the right. Keep wings inside cells.

ROW 2 (water/ice): columns 1-4 CRYOMANCER, blue icy hood and deep-blue robes, crystal ice staff, pointed icy shoulders, no bow; fourth frame thrusts staff and casts a short icy shard to the right. Columns 5-8 TIDE ORACLE, white-and-aqua healer robes, silver crown, a slender trident with blue pearl, flowing elegant silhouette; fourth frame raises trident to release a small aqua healing ribbon.

ROW 3 (nature): columns 1-4 BLOOM DRUID, white-and-green leaf robe, flower garland and flowering healing staff, gentle open-face healer, distinctly floral; fourth frame casts a small swirl of white blossoms. Columns 5-8 THORN GUARDIAN, bulky massive green wood-and-bark armor with thorn ridges, broad wooden shield and short thorn mace, compact tank silhouette; fourth frame drives shield and mace forward.

ROW 4 (storm/wind): columns 1-4 TEMPEST MAGE, cyan-and-gold robe, white swept hair, forked lightning staff, angular sleeves, no armor or spear; fourth frame casts a contained blue lightning bolt to the right. Columns 5-8 WINDCALLER, teal monk tunic, silver curved scimitar, flowing wind ribbons and tied-back hair, agile lightly armored silhouette; fourth frame swings a pale green wind crescent to the right.

ROW 5 (sun): columns 1-4 DAWN PRIEST, white-and-gold clerical robe, sun medallion, tall sun-topped healing staff, peaceful hooded healer; fourth frame raises the staff to a compact golden healing halo. Columns 5-8 SUN CRUSADER, heavy polished gold plate armor, white cape, tall golden helmet, two-handed greatsword, heroic broad shoulders; fourth frame makes a strong rightward greatsword attack with a short golden slash.

ROW 6 (bottom; void): columns 1-4 NECROMANCER, black-and-violet layered robe, skull-topped dark staff, bone necklace, pale face beneath a dark hood, no scythe; fourth frame casts a small violet skull wisp to the right. Columns 5-8 HEXWEAVER, violet-and-magenta witch robe, pointed witch hat, levitating spellbook and one free spellcasting hand, distinct hat and book silhouette; fourth frame opens the book and casts a small magenta hex.

Polish: readable and charming at 64-80 pixel display size, highly consistent rendering and perspective, crisp edges, rich but restrained highlights, clearly recognizable role silhouettes and element palettes. The 8-by-6 equal-cell arrangement and correct class ordering are more important than decorative effects.
```
