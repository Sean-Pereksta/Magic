# Flag Capture: Riftbound

A cooldown-based strategy game at the existing `/game/Flag.html` arcade route. Command one of three original factions against an AI army or a friend on the same device. Custom battles offer AI and same-screen play; the five-mission campaign is a solo faction tour. Every piece has its own movement/attack recovery, and each special ability has an independent cooldown. Every new battle begins with a 3–2–1 countdown, then both armies act during live play. Pause gives you time to inspect the battlefield.

## Factions

| Faction | Identity | Army passive |
| --- | --- | --- |
| Lumen Brood (`bloom`) | Bioluminescent aliens with living armor and strange symbiotic powers. | Recover 1 HP in a grove every six seconds and 2 HP when capturing a beacon. |
| Crooked Carnival (`chaos`) | Surreal Chaos beasts that bend bodies and battlefield positions. | Injured troops deal +1 basic attack damage. |
| Aureate Choir (`choir`) | Angelic beings of ivory, gold, wings, and celestial geometry. | Troops beside an ally gain 1 shield every six seconds, up to 3. |

Every faction has its own troop definitions and abilities. In single-player battles, the command dock sits directly below the gameplay board, with movement, attack, every available special ability, and guard within easy reach. Ability buttons use original drawn icons, names, and cooldown indicators. Leaders and mystics have two or three specials, while scouts and brutes have one, for 21 faction-specific skills in total. The four main roles are faction-specific creatures rather than shared units with different colors. Buy and arrange your starting warband before deployment in any mode, including the campaign.

The battle view keeps the board, command bars, and a menu button visible. The selected creature's health bar appears beside its name, so its condition stays visible during commands. Open the menu when you want creature details, the battle journal, settings, or help. Reinforcement Clash also offers recruitment there. Creature details explain health, movement identity, attack range, faction doctrine, and ability rules; they are optional during play.

Movement also changes how each army approaches the same map:

| Army | Movement identity |
| --- | --- |
| Lumen Brood | Starting a move in a grove grants one extra tile of movement. Groves become both healing stations and launch points. |
| Crooked Carnival | Blinkmutt scouts can phase through enemy-occupied cells, but must finish on an empty cell. Other Carnival troops use ground paths. |
| Aureate Choir | The Halo Architect, Cherub Comet, and Ophanim Lens fly over obstacles and troops. They may land on water, but never on rocks or occupied cells. The Belltower Seraph remains grounded. |

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

## Battle rules

- Move, attack, and guard share an individual cooldown: scouts 2 seconds, leaders 3 seconds, brutes 3.5 seconds, and mystics 2.8 seconds. Each troop recovers independently.
- Every special has its own 8–12-second cooldown and can be used while standard commands or another special recover. Using a special also starts that troop's movement/attack cooldown. Roots prevent movement for 2.5 seconds.
- Reachable cells and valid targets are highlighted. Rocks and water block ground movement, with faction movement exceptions described above. Groves reduce ranged attack damage by one. The engine validates every command, including AI commands.
- Objectives depend on the selected battle mode. Scoring and faction passives use a common six-second pulse for both armies; Reinforcement Clash also awards supplies on that pulse.
- Eliminate the entire opposing army to win immediately in any mode. Otherwise, each mode's score goal or time-limit tiebreak decides the battle.
- Standard battles use the armies chosen before deployment. Mid-battle purchases are available only in Reinforcement Clash: unspent starting coins become your supply wallet, and each pulse adds 2 supplies plus your held-beacon count. Recruit scouts for 4 supplies, brutes for 6, or mystics for 5 at your home edge. Recruits wait a full movement/attack cooldown before using standard commands and a full special cooldown before using an ability.
- Summoning abilities work in every mode and recharge in 12 seconds. Choose an empty ground tile within two cells of the caster to summon up to two little allies there and on an adjacent cell, subject to open space and the eight-creature cap. Each tiny ally has 3 HP, 1 attack damage, 2 movement range, no specials, and a one-second arrival delay. Glowgrubs benefit from the Brood's grove network, Pocket Maws move on the ground, and Bell Motes fly. Both sides obey the same army and summoning rules.
- Guard adds 2 shield, up to 5, and starts the piece's standard cooldown.
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

Before every battle, use the warband builder to buy or sell starting troops and arrange their positions. A warband has 1–8 main creatures and exactly one commander, the leader role. Starting prices are leader 6, scout 3, brute 5, and mystic 4. The default starting-money budget is 24; custom battles can set it from 12 to 40, while campaign missions use 24. Repeat any non-leader role while staying within the budget and troop limit.

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
