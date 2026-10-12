# Age of Ambition

New campaigns reach useful economic, territorial and diplomatic choices sooner. Existing campaigns retain their map, opening resources and progress.

## Opening rules

- Every House begins with 220 food, 170 wood, 140 stone, 75 iron, 300 gold and 16 horses. Tools and arms still require ordinary manufacturing.
- Turns 1–3 grant two additional construction/recruitment commands. Workshops and city upgrades still add commands, with the existing maximum of eight.
- First-level farms, lumber camps, mines, quarries, roads, barracks and archery ranges take one fewer construction turn during turns 1–3, with a minimum of one turn. Repairs, upgrades, advanced defenses and major projects retain their ordinary requirements.
- Each House has one First Frontier Charter for its first additional town: 25% fewer food, wood and stone, rounded up; normal gold cost; two construction turns instead of three. The charter is spent when construction starts and does not bypass placement, ownership, affordability or command requirements. Its time discount does not stack with another construction discount.
- Each House receives a separate Royal Scouts detachment containing two scouts. They use normal visibility, movement, access, combat and upkeep rules. Their small size makes reconnaissance their useful role.

After founding, each House may select one Royal Decree through turn 6. Benefits end after turn 6, so selecting later leaves fewer active turns. The interface shows the precise effects and remaining duration.

| Decree | Temporary benefits |
| --- | --- |
| Prosperity | +10% developed food, wood, stone, iron and horse output; opening construction acceleration for the listed first-level structures extends through turn 6. Manufactured tools and arms receive no production bonus. |
| Mobilization | 10% cheaper levies, spearmen, archers and scouts, with costs rounded up; +1 land-army movement. |
| Diplomatic Influence | +1 regular diplomatic dispatch each turn; +1 ambassador movement. |

Human and AI Houses use the same resources, discounts, commands, decrees and scouts. AI decree selection considers personality, economic needs, nearby geography and observed strategic opportunities.

## Contested rich territory

Generated worlds contain small premium regions chosen from fertile valleys, iron mountains, ancient timberlands, royal quarries, golden trade crossroads, horse plains and natural harbors. Larger worlds can contain additional regions. Geography determines which types fit; the generator does not fabricate impassable deposits or change existing terrain to accommodate them.

Premium tiles provide approximately 1.5–2.5 times the output of the same developed building tier on normal-quality ground. The premium factor replaces deposit quality rather than multiplying it. Integer output is bounded against comparable normal production, including at small output levels. Building tiers continue to scale normally. Only the matching resource-producing structure receives the regional benefit; advanced secondary producers and manufactured resources do not receive additional regional multipliers.

Farms, lumber camps, mines, quarries and ranches realize the relevant natural-resource potential. Golden crossroads require a market or trading post and a local road. Harbors require the ordinary coastal settlement and harbor development. Discovery and territorial ownership alone generate no premium income. Naval blockades suspend the entire boosted harbor yield.

The generator first verifies a complete set of viable starting packages, then reserves premium regions outside those packages before any House actually selects a capital. A founding capital must be more than four hexes from every premium tile. Starting claims and starter roads cannot consume premium territory. Existing minimum capital separation and the requirement to leave viable starts for remaining Houses remain authoritative.

All generated premium centers are passable and connected by ordinary land routes to at least one viable starting area. The richest sites favor actual mountains, passes and constrained shorelines where the map supplies them. Open objectives remain available for earlier competition. Access follows normal geography and borders; there is no turn-based access lock.

AI expansion evaluates discovered premium deposits using resource need, existing production, distance, legal routes, observed enemies, nearby rivals and available supporting forces. It can establish a town toward a valuable deposit and develop the relevant industry once claimed. Premium ownership creates strategic interests, not an automatic declaration of war.

## Information and diplomacy

Discovered premium regions receive map indicators and an inspection panel showing their designation, production potential, relevant development and observed ownership. Unexplored premium centers and satellites are removed from knowledge projections. Access descriptions remain unsurveyed until the center and surrounding geography are discovered. Remembered ownership remains a dated observation.

The Royal Council briefing uses the player's known surroundings for expansion prospects, valuable resources, defensible approaches and nearby concerns. Kingdom Developments records significant observed changes such as frontier settlements, rich-territory ownership, nearby armies, strategic construction, wars and regional agreements. Entries retain their information source and observation date and can open the relevant map location or diplomacy view.

Diplomatic initiative responds to actual economic opportunities, territorial competition and security concerns. Personality, relationships, memories, proposal rules and cooldowns continue to govern contact. Ordinary End Turn simulation makes no additional Gemini calls; the deterministic simulation remains authoritative for actions and ratification.

## Persistence and validation

The new-campaign marker and per-House opening state preserve charter use, decree choice and duration through save/load and multiplayer snapshots. Existing saves receive no opening resources, charter, decree requirement or regenerated premium map. Their economic and territorial rules remain compatible.

Focused coverage in `tests/opening.test.mjs` and `tests/premium-regions.test.mjs` checks opening benefits and expiry, construction limits, charters, decree persistence, legacy compatibility, deterministic geography, all four supported House counts across all seven map profiles, founding exclusions, movement reachability, quality/tier output bounds, fog projection, invalid save metadata and premium naval blockades. Existing founding, fog, economy, diplomacy and multiplayer tests continue to cover the surrounding systems.

Run the complete rule suite with `npm run test:iron-throne`. Run the additional bounded campaign probe with `npm run test:iron-throne:ambition`; it compares canonical and reloaded campaigns after every turn through turn 6, verifies finite resources and legal troop counts, and requires rival expansion and charter use.

The initial seeded campaign acceptance run produced these outcomes:

| Seed | Houses | Map | Difficulty | Settlements at turn 6 | Charters used |
| --- | --- | --- | --- | --- | --- |
| 42 | 6 | Heartlands | Easy | 11 | 5 |
| 8147 | 8 | Great Divide | Medium | 15 | 7 |
| 92 | 12 | Shattered Realms | Hard | 22 | 10 |
| 1 | 10 | Highland Crown | Insane | 19 | 9 |

`npm run test:iron-throne:ambition-browser` exercises decree selection, persistence, opening information, discovered premium inspection, interactive developments, layout overflow and End Turn at desktop, phone and landscape sizes. It uses the optional Playwright dependencies in `tests/package.json`; `IRON_THRONE_CHROMIUM` can select an installed Chromium browser. The existing Firebase browser workflow also runs this check.

Deploy the updated `firestore.rules` with the client update so multiplayer players can submit the authenticated `royalDecree` command. Other command authorization and structured diplomacy rules remain in force.
