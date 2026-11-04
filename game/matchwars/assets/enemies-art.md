# Expanded enemy sprite artwork

`enemies.png` was generated using the built-in image-generation tool with the existing `troops.png` supplied only as a style reference. This is a new asset; the original sheet was not edited. The generated native PNG was copied into the project and visually inspected without postprocessing.

## Atlas layout

Actual dimensions: **1448 × 1086 pixels**, RGBA PNG, 3,144,981 bytes. The atlas has **8 columns × 6 rows** with exact **181 × 181 pixel** cells. Calculate source-cell dimensions from the image width divided by 8 and height divided by 6 rather than assuming the requested resolution.

Each row has character A in columns 0–3 and character B in columns 4–7. Each character's first three frames are locomotion/idle poses; its fourth frame attacks or casts. All characters face right. Mirror enemy sprites during rendering; the Grave Knight can also be used unmirrored for an allied raised Boneguard.

| Row | Columns 0–3 | Columns 4–7 |
| --- | --- | --- |
| 0 | Ember Hound | Ash Witch |
| 1 | Tide Crab | Siren |
| 2 | Briar Spider | Spore Alchemist |
| 3 | Storm Beetle | Zephyr Djinn |
| 4 | Solar Chimera | Inquisitor |
| 5 | Grave Knight | Soul Leech |

Use exact-cell cropping. Some attack effects reach the cell boundary. Alpha verification found a real transparency channel spanning 0–255: 96,738 fully transparent pixels, 1,475,671 partially transparent pixels, and 119 fully opaque pixels. Most apparently vivid empty-area RGB values in raw image viewers carry alpha 1 and are effectively transparent in game compositing; preserve the native alpha channel.

## Final generation prompt

Use case: stylized-concept. Asset type: NEW production-ready transparent animated fantasy enemy sprite atlas. Image1 is STYLE REFERENCE ONLY from the game, keep this same polished colorful small fantasy sprite aesthetic with clear chunky pixels, crisp highlights and dark outline; DO NOT reproduce its characters. Create a brand-new atlas at 2048x1536, exactly 8 equal columns by 6 equal rows, 48 full-body sprites total, every square cell256x256. The background MUST be genuinely transparent RGBA. No labels, no panels, no guides, no grid lines, no text, no ground, no scenery, no floor or drop shadows. Every sprite faces RIGHT in side view. Each cell contains one isolated complete character centered with generous transparent padding, feet/lowest point at85% cell height, consistent scale, no overlap outside cells. Each row is two DIFFERENT characters: first4 columns characterA, last4 columns characterB. Within each set4frames, frames1-3 are consecutive walking/crawling/floating poses with genuinely changed legs/limbs, frame4 is clear attack/cast pose. Keep identity/anatomy/costume/scale consistent across all4frames. Row1 A=Ember Hound, fiery red-orange wolf/hellhound quadruped with burning mane and jaws; B=Ash Witch, orange and purple hooded fire witch with wand. Row2 A=Tide Crab, turquoise heavily armoured giant crab with two visible claws and many legs; B=Siren, blue aquatic finned healer holding conch-shell staff. Row3 A=Briar Spider, green spider with8 thorn legs and bulbous body; B=Spore Alchemist, green mushroom-headed goblin carrying poison flask. Row4 A=Storm Beetle, electric blue armoured beetle with yellow horn and six legs; B=Zephyr Djinn, teal whirlwind genie caster floating on tornado tail. Row5 A=Solar Chimera, golden lion quadruped with eagle wings; B=Inquisitor, white and gold masked hostile priest with holy war hammer. Row6 A=Grave Knight, purple and grey UNDEAD SKELETON knight with visibly bony skull and ribs, sword and shield; B=Soul Leech, violet ghost serpent/shade spirit parasite with tapering spectral tail. Strong varied silhouettes and unmistakably different anatomies across species, recognizable beasts not humanoid recolors. Combat-ready animations, readable at64pixels. Strictly maintain exact regular8by6 atlas layout and transparent cell padding.
