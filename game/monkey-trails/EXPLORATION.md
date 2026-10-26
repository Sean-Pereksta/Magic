# Active monkeys and neighborhood habitats

Choose **Active monkey** as the third action after tapping any conscious monkey in your expedition lineup. The choice is saved separately from the battle lead: it controls the map portrait, foraging, and ordinary rare encounter weights. Swapping battle fighters does not change it. Removing, selling, or knocking out the walking companion falls back to a conscious teammate.

Roster tiles show a forager/scout symbol and a special border; green marks the active walking monkey. Details show the exact extra pickup chance, relative rare encounter weight bonus, and favored resource multipliers. Common companions have small bonuses; rarer companions and named specialists have stronger ones. Evolved elements also contribute. Bonuses apply only from the selected conscious lineup member. Regional pools, expedition depth, introductory fights, guardians, and Ultra access requirements still apply.

Each accepted walking discovery can award one additional pickup. Both normal and bonus finds favor the selected monkey's specialties and current habitat. The bonus is reported separately in notifications and the journal; it does not count as a second discovery or grant extra distance, quest discovery credit, or XP. Saved RNG determines the awards. Viewing traits, previews, and forge values never rolls rewards.

## Materials

| Material | Source | Crafting value | Other use |
| --- | --- | ---: | --- |
| Ingot | Fairly rare walking find; caves and Earth specialists strongly improve odds | 5 | Mineral forging |
| Star Ore | Very rare walking find; mineral specialists and caves improve odds | 12 | Rare and Ultra training beyond level 20 |
| Crown Core | One per defeated Ultra encounter, awarded once with battle rewards | 20 | Ultra training beyond level 20 |

Ingots start around 2.5% per pickup before habitat/specialty multipliers, capped at 15%. Star Ore starts around 0.3% and is capped at 2%. These resources are personal materials, separate from multiplayer supply trading. Star Ore and Crown Cores survive expedition defeat and restart, and can be spent on training or deliberately offered to the forge. Ingots reset with expedition supplies.

Training from current level 20–29 costs rare companions one Star Ore per level, rising to two at 30, three at 40, and four at 50. Legendary Ultras need one Star Ore at 20, two at 35, and three at 50, plus one Crown Core per level until 40 and two thereafter. Existing fruit and XP costs still apply. Training previews show exact owned/required material counts and where to find them. No material requirement applies before level 20; existing levels are preserved.

## Broad local habitats

Each existing land contains deterministic, roughly 480-meter-wide habitat patches with shared irregular boundaries. Groves favor fruit and Leaf essence; cave formations favor ingots, Star Ore, and Earth essence; wetlands favor Water/Ice essence; ruins favor charm materials and Light/Shadow essence. Open trails and landmark clearings have their own profiles. Map colors, vegetation, boundaries, current-habitat labels, and nearby direction hints use the same geography as pickups and encounter ecology.

These are fantasy habitats attached to the game's existing relative walking map, not claims about real properties or traversable paths. Any accepted safe route remains playable. Returning to a position or reopening a save restores the same habitat; zooming changes only the camera. A long accepted walking sample awards each discovery using its own interpolated position, including across boundaries.

## Verification

Run `npm run test:monkey-trails` for engine and integration regressions and `npm run test:monkey-trails:exploration-browser` for the new mobile/desktop flow. Coverage includes saved independent active selection, invalid/KO/sold fallbacks, ordinary versus Ultra reward sources, atomic training costs, permanent rare materials, habitat distributions, long versus short walking samples, all forge tile values, trait badges, encrypted reopening, and walking Grove → Cave → Grove.
