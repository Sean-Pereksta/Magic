# Flag Capture: Riftbound

A cooldown-based strategy game at the existing `/game/Flag.html` arcade route. Command one of three original factions against an AI army or a friend on the same device. Custom battles offer AI and same-screen play; the five-mission campaign is a solo faction tour. Every piece has its own movement/attack recovery, and each special ability has an independent cooldown. Every new battle begins with a 3–2–1 countdown, then both armies act during live play. Pause gives you time to inspect the battlefield.

## Factions

| Faction | Identity | Army passive |
| --- | --- | --- |
| Lumen Brood (`bloom`) | Bioluminescent aliens with living armor and strange symbiotic powers. | Recover 1 HP in a grove every six seconds and 2 HP when capturing a beacon. |
| Crooked Carnival (`chaos`) | Surreal Chaos beasts that bend bodies and battlefield positions. | Injured troops deal +1 basic attack damage. |
| Aureate Choir (`choir`) | Angelic beings of ivory, gold, wings, and celestial geometry. | Troops beside an ally gain 1 shield every six seconds, up to 3. |

Every faction has seven main troop choices and 39 faction-specific skills across all three armies. Guardians, rangers, and shapers join the original commanders, scouts, brutes, and mystics; each new creature has two distinct specials. In single-player battles, the command dock stays directly below the board, with every special visible as a drawn icon alongside its name and cooldown. Buy your starting warband, choose Arrange troops, then position it on the actual seeded battlefield before starting the countdown. This flow applies to every mode and campaign mission.

The battle view keeps the board, command bars, and a menu button visible. The selected creature's health bar appears beside its name, so its condition stays visible during commands. Open the menu when you want creature details, the battle journal, settings, or help. Reinforcement Clash also offers recruitment there. Creature details explain health, movement identity, attack range, faction doctrine, and ability rules; they are optional during play.

Movement also changes how each army approaches the same map:

| Army | Movement identity |
| --- | --- |
| Lumen Brood | Starting a move in a grove grants one extra tile of movement. Groves become both healing stations and launch points. |
| Crooked Carnival | Blinkmutt scouts and Rift Jackal rangers phase through enemy-occupied cells, but finish on empty ground. Friendly Rift Ink lets other troops phase through foes and grants +1 movement when starting there. |
| Aureate Choir | Commanders, scouts, mystics, rangers, and shapers fly over obstacles and troops. They may land on water, but never on rocks or occupied cells. Brutes and guardians remain grounded. |

| Faction | Troop | Role | Abilities |
| --- | --- | --- | --- |
| Lumen Brood | Prism Matriarch | Leader | **Bloomcall:** heal herself and nearby allies. **Seedgrove:** plant a grove on empty ground within three tiles. **Broodlings:** summon tiny Glowgrubs. |
| Lumen Brood | Skitterglow | Scout | **Sporehop:** blink up to four tiles, leaving a healing grove behind. |
| Lumen Brood | Reefback Grazer | Brute | **Gravity Belch:** damage, pull, and root an enemy. |
| Lumen Brood | Lantern Polyp | Mystic | **Sporeburst:** damage enemies around a target. **Nectar Mote:** restore 2 HP to an ally within three tiles. |
| Crooked Carnival | Mawring Maestro | Leader | **Wrong Note:** damage and root nearby enemies. **Laughing Hex:** curse a foe within three tiles so its next incoming hit deals 2 extra damage. |
| Crooked Carnival | Blinkmutt | Scout | **Sideways Bite:** warp beside an enemy and bite. |
| Crooked Carnival | Mobius Ram | Brute | **Foldhorn:** ram and knock back a target; blocked knockback adds damage. |
| Crooked Carnival | Teacup Oracle | Mystic | **Bad Omen:** damage and curse an enemy so its next incoming hit is stronger. **Crooked Swap:** exchange places with a foe within three tiles when both landing cells are legal. **Pocket Maws:** summon tiny Pocket Maws. |
| Aureate Choir | Halo Architect | Leader | **Concordance:** refresh nearby allies' movement/attack cooldowns and add shields. **Starstep:** teleport to an empty tile within three and gain 2 shield. |
| Aureate Choir | Cherub Comet | Scout | **Mercy Arc:** heal an ally and cleanse roots and curses. |
| Aureate Choir | Belltower Seraph | Brute | **Sanctuary:** shield himself and nearby allies. |
| Aureate Choir | Ophanim Lens | Mystic | **Judgment Ray:** damage enemies in a straight line. **Ward Ray:** give an ally within three tiles 3 shield, up to 5. **Bell Motes:** summon tiny flying Bell Motes. |
| Lumen Brood | Mossplate Bulwark | Guardian | **Rootguard:** shield an adjacent group. **Briar Circle:** plant a lasting thornfield. |
| Lumen Brood | Thornskate | Ranger | **Pinning Quill:** damage and root a foe. **Pollen Skate:** hop, leaving mycelium at both ends. |
| Lumen Brood | Mycelium Weaver | Shaper | **Living Carpet:** paint healing, slowing mycelium. **Germinate:** heal an ally, refreshing its standard cooldown on friendly mycelium. |
| Crooked Carnival | Grinlock Bastion | Guardian | **Grinning Ward:** shield allies and briefly root adjacent foes. **Cinder Confetti:** scatter damaging cinders. |
| Crooked Carnival | Rift Jackal | Ranger | **Crosscut:** strike harder through a pincer. **Riftstep:** hop, leaving Rift Ink at both ends. |
| Crooked Carnival | Inkmaw Jester | Shaper | **Rift Ink:** create phasewalking launch marks that curse enemies. **Curtain Collapse:** consume friendly marks for an area strike. |
| Aureate Choir | Aegis Cantor | Guardian | **Aegis Link:** shield and cleanse an ally. **Quiet Sanctuary:** consecrate ground for recurring protection. |
| Aureate Choir | Sunlance Kite | Ranger | **Sunlance:** attack along a clear row or column, ignoring some shield. **Prism Lattice:** strengthen ranged fire from marked ground. |
| Aureate Choir | Halo Scribe | Shaper | **Dawn Seal:** erase hostile ground and consecrate it. **Procession:** swap with an ally and shield both. |

## Formations and lasting ground

Positioning grants different rewards for each faction. Brood want a connected living network; Carnival want enemies caught between separated hunters; Choir want a straight, consecutive procession. Formation rules are computed from living troops after every move, so breaking a shape immediately removes its benefit.

| Formation | Condition | Benefit |
| --- | --- | --- |
| Living Constellation · Brood | At least three friendly troops connected edge-to-edge. | Every connected troop gains +1 movement and heals 1 HP each six-second pulse. |
| Impossible Pincer · Carnival | You and an ally stand directly opposite one another across an adjacent enemy. | Basic attacks against that flanked enemy gain +1 damage. |
| Choir Procession · Choir | At least three friendly troops occupy consecutive squares in one row or column. | They take 1 less ranged damage, minimum 1, and resonance can build up to 5 shield. |

Painted effects remain on individual ground cells, with their ownership and remaining duration visible. They do not replace the map's underlying terrain. Area casts paint the center and its four orthogonal neighbors where those cells are ground; rocks and water remain unpaintable. Periodic effects trigger every three seconds. One side has at most one effect on a cell: repainting replaces its old mark, while opposing marks may coexist. Repainting never produces stacked damage or extra healing ticks.

| Ground effect | Duration | Tactical use and counterplay |
| --- | --- | --- |
| Living Mycelium | 12s | Allies heal 1 HP per tick; enemy ground steps cost 2 movement. Germinate refreshes standard commands on friendly mycelium. |
| Briar Field | 12s | Enemy ground troops take 1 damage per tick. Fliers ignore it; stepping off the marked ground avoids later ticks. |
| Rift Ink | 12s | Allies starting there phase through foes and move +1. Enemy arrivals and ticks apply a curse. Consume it with Curtain Collapse to trade mobility for damage. |
| Cinder Confetti | 10s | Enemies take 1 damage on arrival and each tick, including fliers. Avoid the mark, leave it, or erase it with Dawn Seal. |
| Quiet Sanctuary | 12s | Allies gain 1 shield, up to 5, and cleanse roots/curses each tick. It rewards holding a position, rather than chasing every target. |
| Prism Lattice | 12s | Friendly ranged basic attacks gain +1 damage while standing there. Enemy ground steps cost 2 movement. Fliers cross it freely. |

The army shop offers six purchasable alternatives per faction, and native expandable **Skills & tactics** cards make their exact effects readable on touchscreens before spending coins. Remove a creature with × to refund its starting price and try another role. The selected-creature inspector also explains its movement and faction formation during deployment.

## Battle rules

- Move, attack, and guard share an individual cooldown: scouts 2 seconds, leaders 3 seconds, brutes 3.5 seconds, and mystics 2.8 seconds. Each troop recovers independently.
- Every special has its own 8–12-second cooldown and can be used while standard commands or another special recover. Using a special also starts that troop's movement/attack cooldown. Roots prevent movement for 2.5 seconds.
- Reachable cells and valid targets are highlighted. Rocks and water block ground movement, with faction movement exceptions described above. Groves reduce ranged attack damage by one. The engine validates every command, including AI commands.
- Objectives depend on the selected battle mode. Scoring and faction passives use a common six-second pulse for both armies; Reinforcement Clash also awards supplies on that pulse.
- Eliminate the entire opposing army to win immediately in any mode. Otherwise, each mode's score goal or time-limit tiebreak decides the battle.
- Standard battles use the armies chosen before deployment. Mid-battle purchases are available only in Reinforcement Clash: unspent starting coins become your supply wallet, and each pulse adds 2 supplies plus your held-beacon count. Recruit scouts for 4 supplies, brutes or guardians for 6, mystics or rangers for 5, or shapers for 6 at your home edge. Recruits wait a full movement/attack cooldown before using standard commands and a full special cooldown before using an ability.
- Summoning abilities work in every mode and recharge in 12 seconds. Choose an empty ground tile within two cells of the caster to summon up to two little allies there and on an adjacent cell, subject to open space and the eight-creature cap. Each tiny ally has 3 HP, 1 attack damage, 2 movement range, no specials, and a one-second arrival delay. Glowgrubs benefit from the Brood's grove network, Pocket Maws move on the ground, and Bell Motes fly. Both sides obey the same army and summoning rules.
- Guard adds 2 shield, up to 5, and starts the piece's standard cooldown. Guard is unavailable at full shield.
- Pause freezes the battle clock, scoring, cooldowns, and AI commands.

Choose factions, an opponent type, map, and battle settings before a new match. AI battles also offer a difficulty choice. Select a troop, choose its command using the large command buttons or hotkeys, then select a highlighted destination or target. Cooldown indicators tell you when each piece can act again. You can also drag pieces to valid destinations on the board.

## Modes and custom battles

| Mode | Objective | Default victory |
| --- | --- | --- |
| Beacon Rush (`beacons`) | Enter a beacon's cell to claim it. Ownership persists until an opponent enters. Each held beacon gives one light every six seconds. | First to 30 light, or higher light at three minutes; surviving HP breaks a score tie. |
| Zone Control (`control`) | Each beacon has a zone covering itself and its four orthogonally adjacent cells. The side with more troops in that zone controls it. Equal occupied forces contest it; empty zones are neutral. | First to 20 light, or higher light at three minutes; surviving HP breaks a score tie. |
| Last Army Standing (`elimination`) | Defeat the opposing army. Beacons do not score light. | Wipe out the enemy, or have more surviving HP at three minutes; controlled beacons break an HP tie. |
| Reinforcement Clash (`reinforcements`) | Capture flags while spending pulse supplies on new troops. This is the mode with mid-battle recruitment. | First to 30 light, eliminate the opposing army, or have higher light at three minutes; surviving HP breaks a score tie. |

An exact tiebreak tie is a draw. Zone control refreshes immediately after commands and clock advancement; light arrives only on the pulse. This rewards holding a zone rather than briefly touching it.

Custom battles offer Pocket (7 × 5), Classic (9 × 7), and Expanse (11 × 9) boards; sparse, balanced, or wild terrain; and five AI challenges: **Story**, **Relaxed**, **Balanced**, **Tactician**, and **Riftlord**. Story gives new commanders the gentlest introduction; choose a higher setting as you learn your army's tricks. Choose Prism Glade, The Great Fracture, or Bellglass Sanctuary as the battlefield theme. Both factions follow the same legal movement, attack, ability, capture, and recruitment rules.

Before every battle, use the warband builder to buy or sell starting troops and arrange their positions. A warband has 1–8 main creatures and exactly one commander, the leader role. Starting prices are leader 6, scout 3, brute 5, mystic 4, guardian 5, ranger 4, and shaper 5. The default starting-money budget is 24; custom battles can set it from 12 to 40, while campaign missions use 24. Repeat any non-leader role while staying within the budget and troop limit.

Place your purchased troops on distinct cells within your own two deployment rows before beginning the countdown. In same-screen play, both players buy and arrange their own warbands. Against AI, the opposing army uses an affordable starting formation. The campaign supplies a suggested warband that you may edit before beginning each mission. Mid-battle purchases remain exclusive to Reinforcement Clash.

Choose **2 players · same screen** for a local battle. Player 1 controls the army at the bottom and uses the bottom command dock. Player 2 controls the army at the top and uses the top dock. Each player keeps an independent troop selection and command mode. Both armies use their cooldowns simultaneously, and a touchscreen supports separate fingers dragging pieces for both players. The shared pause control freezes the whole match. Local play stays on one device and does not use online rooms or matchmaking.

## Short campaign

The five missions tour all three factions and introduce capture, battlefield control, healing, support, and summoning before a larger final fight. Campaign missions specify factions, goals, time limits, and a suggested warband; you can buy, sell, and arrange your starting creatures with 24 money before each mission. They do not offer mid-battle recruitment.

| Mission | Your faction | Mode | Difficulty | Battlefield |
| --- | --- | --- | --- | --- |
| First Light | Lumen Brood | Beacon Rush | Story | 7 × 5, sparse |
| Crooked Ground | Crooked Carnival | Zone Control | Relaxed | 9 × 5, sparse |
| Choir Practice | Aureate Choir | Beacon Rush | Balanced | 9 × 7, balanced |
| Wild Frontier | Lumen Brood | Zone Control | Balanced | 11 × 7, wild |
| The Last Rift | Crooked Carnival | Last Army Standing | Tactician | 11 × 9, wild |

A victory unlocks the next mission. Completed missions remain replayable, and replaying an earlier mission never removes progress. Campaign progress is stored locally by the interface. The campaign module sanitizes saved values, ignores unknown or locked mission completions, and provides reproducible battle options.

## Controls

| Single-player / Player 1 | Player 2 in local play | Action |
| --- | --- | --- |
| `1` | `Q` | Move |
| `2` | `W` | Attack |
| `3` | `E` | First special ability |
| `4` | `R` | Second special ability, when the creature has one |
| `5` | `F` | Third special ability, when the creature has one |
| `G` | `T` | Guard |
| `Space` | Shared | Pause or resume |
| `Escape` | Shared | Cancel the current command |

The single-player command bar stays directly below gameplay; in local play, Player 2's bar sits above the board and Player 1's below it. All specials belonging to the selected creature stay visible together; there is no ability-page switch. Tiny summoned creatures have no special buttons. Click or tap a troop, select its command icon, then choose a colored target. Drag a ready piece onto a valid board destination for direct movement. In same-screen play, each dock retains its own selection while the other player acts. Player 1 uses `1`, `2`, `3`, `4`, `5`, and `G`; Player 2 uses `Q`, `W`, `E`, `R`, `F`, and `T`.

## Files

- `../Flag.html`: retained entry route.
- `engine.mjs`: deterministic rules, faction definitions, legal actions, scoring, recruitment, and AI.
- `campaign.mjs`: five mission briefs, fixed battle options, and sanitized progression helpers.
- `draft-ui.mjs` / `draft-ui.css`: starting-money purchases, refunds, and two-row deployment controls.
- `assets/`: original faction illustrations and their generation provenance.
- `engine.test.mjs`: gameplay rule tests.
- `campaign.test.mjs`: mission configuration, saved-progress recovery, unlock, and replay tests.
- `draft-ui.test.mjs`: affordable warbands, purchases, refunds, limits, deployment swaps, resizing, and engine integration.
- `../../lobby/tests/flag-capture-catalog.test.cjs`: catalog and solo-launch regression tests.

The game retains the `flag` registry key, standalone Play action, favorites/history identity, and route. Its registry mode remains `single`: both AI play and local shared-screen play launch the same self-contained page, with no online lobby action. Its lobby category is Strategy; the existing editorial placement is preserved explicitly in the showcase configuration.

## Validation

Run from the repository root:

```text
node --test game/flag-capture/engine.test.mjs
node --test game/flag-capture/campaign.test.mjs
node --test game/flag-capture/draft-ui.test.mjs
node --test lobby/tests/flag-capture-catalog.test.cjs
```

The focused catalog tests verify the actual loader output, the stable route/key/mode, the existing user-initiated Play handler, no online lobby action, and preserved editorial placement.

## Responsive controls and deployment

Battle updates retain the board, buttons, creature tokens, focus, and running effects. A troop slides with its health, cooldown and shield indicators in a 210ms animation; unrelated commands do not restart that movement. Dragging shows a fingertip preview and valid drop feedback. Each gesture remembers its original troop, owner, command, and special slot. Local players can issue independent commands.

Legal geometry stays visible during recharge. An order entered at most 0.55 seconds early is buffered once per troop; the latest valid order replaces it. An hourglass marks its destination, and its command timer reads Queued. The engine revalidates it before execution. Death, movement, changed ownership, new obstacles, pause, countdown, and victory cancel stale orders. Simulation checks queued commands every 50ms. Arrow keys navigate board focus.

Deployment uses the same seed, terrain and purchased formation as the battle. Moving a troop never rerolls terrain. Auto arrange places scouts and brutes forward and commander/support behind; all placements can be changed manually. Local players can arrange either army or drag their own troops concurrently.

- `input.mjs`: legal target previews, bounded command buffering, gesture snapshots.
- `battle-renderer.mjs`: persistent battlefield and control updates.
- `battle-integration.test.mjs` / `deployment-integration.test.mjs`: actual application handlers exercised with real rules and draft state.
- `PLAYTEST.md` / `playtest.mjs`: reproducible strategic diagnostics and measured browser results.
