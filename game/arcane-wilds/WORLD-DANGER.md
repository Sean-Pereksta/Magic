# World danger and interactive challenges

This campaign extension replaces ordinary elite modifiers with eleven named
affixes, introduces five pursuit archetypes, and gives exploration objectives
that require movement, interaction and elemental counters. It uses the existing
enemy ability, inventory, campaign, homestead transaction and input systems.

`danger-content.js` loads before combat affinities and enemy abilities;
`danger-combat.js`, `challenge-rooms.js` and `world-danger.js` load afterward,
before runtime stability. No separate save key or currency is introduced.

## Combat and counters

Ordinary elites receive one deterministic affix, occasionally two at difficulty
10 or above. Affixes compose their abilities and suppress the previous automatic
elite modifier. Shadow encounter traits retain their existing behavior.
The extension does not raise the health of existing elites or campaign bosses.

| Affix | Behavior and counter |
| --- | --- |
| Blazing | Warned fire trails; frost extinguishes them and delays the next trail. |
| Stormbound | Warned lightning pools, dashes and chain lightning; spread out and sidestep. |
| Frostborn | Warned freezing ground and temporary ice walls; fire or earth breaks walls. |
| Vampiric | Heals for 65% of actual player health damage; dodges deny the heal. |
| Berserker | Faster below half health, fastest below one quarter; interrupt its final attacks. |
| Mirror | Copies the last offensive spell school as a warned enemy cast. |
| Commander | Nearby units screen ranged allies, flank and retreat when wounded. |
| Necromancer | A visible 1.4-second ritual raises one recent corpse; interrupt with wind, earth or lightning. Risen units yield no XP and cannot be raised again. |
| Juggernaut | Frontal hits deal 22% damage; rear attacks and armor-breaking elements bypass it. |
| Teleporter | Shadow warning precedes a blink and directional strike. |
| Predator | Pursuit dash, leap and chain dash punish straight-line running. |

Dash Hunters and Mounted Raiders use a locked pursuit lane with speed 18.
Chain Dashers advertise each newly targeted leg for at least 0.7 seconds before
moving; subsequent legs are faster. Pouncing Beasts leap onto a marked circle,
then send out a ring with a safe center. Shadow Assassins leave a visible shadow
before emerging. Major attack warnings keep their existing admission limits,
and actual overlap during a dodge earns the existing perfect-dodge reward.

Earth spells now break armor and shields; wind interrupts owned enemy attacks
and weakens nearby enemy projectiles. Frost slows fast enemies, water quenches
fire, fire sears undead and is weakened by wet targets. Existing lightning,
poison, holy, shadow and elemental feedback continues through the same resolver.
Only one matchup applies to a hit, rather than multiplying all matching tags.

Veterans at difficulty 4+ can sidestep an approaching player projectile for
0.2 seconds, with a 3.6-second cooldown. This adds movement, not invulnerability.

## Puzzles and rewards

Nine existing puzzle sites cycle through seven physical puzzle kinds. Optional
dungeon relic chambers add plates in Verdant, mirrors in Meridian and memory in
Gloam. All objects use the existing E / touch USE / controller interaction.
Read the entrance inscription for a clue and a reset button. Roads stay open.

| Puzzle | Objective |
| --- | --- |
| Runes | Visit four stones in the seeded order. A mistake resets progress and warns a trap. |
| Mirrors | Rotate three mirrors so an actual reflected beam reaches its receiver. |
| Plates | Carry and drop two crates so both plates remain occupied together. |
| Elements | Match fire, frost and lightning to three seals. A free conduit makes every loadout viable. |
| Memory | Watch and repeat a four-to-six-step illuminated pattern; replay as needed. |
| Timing | Activate three distant switches within a 10-to-14-second simulation countdown. |
| Combat | Bait a guardian's charges into all three pillars before its armor permits damage. |

Each solution awards a named Rare trinket, materials and 60 gold, or 100 gold
for a solution without mistakes or health damage. Rewards go through the normal
inventory and pay once per journey. Solved status, mirror rotations, crate
positions and elemental seals persist. Memory demonstrations, timed attempts
and a combat guardian restart on reload; a reset never grants loot.

## Bosses and regions

Campaign bosses keep the existing phase thresholds (70% and 35% health).
Stone, sand, frost, fire, storm, nature and void families each have three attack
profiles; later phases introduce chain dashes, rings, beams and ground pressure.
Charges must actually move into a pillar or explosive to expose armor for four
seconds. Destroying all armor objects removes that arena defense permanently.
Frost crystals clear nearby freezing effects and hold ice patches clear for
20 seconds; void crystals disable shield armor. Fire arenas offer interactable
stone refuges that protect against lava for six seconds.

| Region family | Environmental interaction |
| --- | --- |
| Frost / Long Winter | Ice drifts and cracks after sustained standing; dodge out of its warning. |
| Desert | Sinking sand slows walking; burrow and pursuit patterns demand repositioning. |
| Swamp | Mud slows movement, water wets enemies, and fire can ignite gas. |
| Stone / mountain ruins | Falling-rock warnings and powder barrels reward positioning. |
| Arcane / void | Paired portals reposition the player; arcane ground strikes warn first. |
| Volcanic | Warned lava pools, frost suppression and timed stone refuges. |

Ground hazard scheduling stops after victory. Menus pause simulation timers.
Travel clears attacks, rituals, guardians, arena objects and explosion jobs.

## Optional encounters and world problems

Seeded sites host six legendary creatures: Ancient Frost Troll, Crimson Wyvern,
Arcane Colossus, Storm Roc, Elder Sand Worm and Forest Guardian. Each continent
has two locations. Discovery reveals the creature on the map; an explicit
interaction starts the fight. Victory records a trophy and its named relic.
Locations are fixed for a journey, rather than migrating between map nodes.

Other optional sites host elite fights or one of five trials: survive 20 seconds,
complete a duel without damage, use elemental weaknesses on three targets, visit
mobility checkpoints through warned traps, or defeat three elites in succession.
A failed bonus objective still permits the base reward. Existing directional
roads permit retreat and retry without unlocking another continent.

Ten existing event sites cycle through six objective problems. Attackers and
native reinforcement waves must also be defeated before the room can clear:

- Burning village: fill a bucket, extinguish three fires and rescue two people.
- Collapsed mine: channel to clear debris, brace supports and free miners.
- Monster nest: crush eggs before they hatch into bounded, zero-XP threats.
- Broken seal: repair four runes in order while a bounded portal wave spawns.
- Flooded ruin: open two ordered valves to reach the north archive.
- Caravan: escort a moving wagon for 18 seconds, repairing damage from nearby foes.

Channeling cancels on movement or health damage. Completed steps persist;
caravan motion and partial seal sequences restart on reload. Each completed
problem awards one relic and supplies in addition to its normal expedition claim.

## World conditions and settlement

A new journey selects two seeded conditions from Long Winter, Goblin Uprising,
Arcane Storm, Monster Migration, Undead Rising and Age of Plenty. Conditions
change patrol identities, ice crossings, warned strikes and resource yield;
they do not increase opening enemy counts. Night patrols alternate every four
room visits. Character shows the active conditions and an elite/element guide.

Every three distinct expedition claims can queue one settlement situation.
The noticeboard on Hearthglade's grounds offers thirteen situations, including
three optional defenses. Spend supplies, use a placed relevant furnishing, or
win a defense to resolve it. New Watchtower, Barracks, Herbalist Shelter and Mage
Beacon blueprints provide prepared solutions. Retreat never destroys the house.

Event costs and rewards use `AWHome.action` and the existing cloud transaction
adapter, with event IDs and request deduplication. Other home mutations are
blocked during a defense. Victory proof lasts for the current visit; reloading
an unclaimed defense requires winning it again. A pending situation is preserved
until resolved, and repeated claims of the same expedition cannot create events.

## Validation

Run `npm ci --prefix game/arcane-wilds --ignore-scripts`, followed by
`npm test --prefix game/arcane-wilds` using Node 24. The existing CI workflow
automatically includes `world-danger.test.mjs`.

The new checks cover desktop/touch rendering, warnings and frozen lanes, affix
composition and interrupts, all seven real puzzle solutions, partial saves,
all six world problems, all six legendary records, all five trial objectives,
retreat/continent locks, boss collisions, post-victory hazard cleanup and
transactional settlement rewards. Runtime tests use jsdom with finite-coordinate
canvas checks. Physical-device controls, visual balance and live Firebase writes
still require a gameplay pass; automated tests do not substitute for that pass.

The design brief's additional terrain examples, world-map migration, independent
jumping input and permanent spell/decor unlock rewards are future extensions.
Current movement uses the game's dodge, and challenge rewards use its trinkets.
