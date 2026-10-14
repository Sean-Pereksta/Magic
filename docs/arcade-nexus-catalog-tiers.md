# Arcade Nexus — catalog curation and maturity evidence

Reviewed on 2026-10-14 against the user's redesign brief and the tracked repository state `2cd9d69` (the starting main checkout). This is an **editorial curation decision**, not a generated quality score, release certification or promise of a development schedule. No game code, launch mode or save format changes are implied.

The live curation source is `lobby/lobby-showcase.js`: four `flagships`, eight `premium` picks, twelve additional `builtOut` picks, explicit `tools`, and any future explicit `experimental` overrides. The registry remains authoritative for names, categories, modes, routes and account/host requirements. Existing artwork-manifest tier labels describe the artwork inventory; they must not override the live curation configuration.

## Best Games · Built Out — 24 games

These entries have either explicit flagship/premium placement in the user's brief or additional evidence of a developed game loop, substantive content/progression, persistence or supporting validation. Automated tests listed below are tracked evidence; this curation audit did not execute each game's complete gameplay suite.

| Registry key | Display name | Placement and supporting evidence |
| --- | --- | --- |
| `apestogetherstrong` | Apes Together Strong | User's primary flagship. Dedicated game directory, gameplay/world/equipment/settlement/kingdom documentation and extensive rules, browser and performance tests. |
| `ironthrone` | Iron Thrones | User's primary flagship. Persistent solo/online campaigns, economy, diplomacy, armies, ambition and founding systems; dedicated documentation and rule/browser/online test suites. |
| `warrealms` | War Realms | User's primary flagship. Deck construction and solo/multiplayer battles plus campaign, card validation/balance, triggers, telemetry and presentation test suites under `game/warrealms/tests`. |
| `cat` | Cat & Mouse | User's primary flagship. Dedicated `game/catandmouse` runtime, motion/tactics/sync/late-game/wins integration coverage and documented multiplayer game. |
| `turncraft` | TurnCraft — Shattered Front | User's premium collection. Three factions, story/survival and dedicated multiplayer entry, tracked faction assets and a dedicated test command/package. |
| `crowncouncil` | Faction Forge | User's premium collection. Campaign, troop collection/progression and multiplayer bridge; campaign balance/deployment documentation and progression/rendering/matchmaking/browser tests. |
| `neonbreach` | Neon Breach | User's premium collection. Dedicated modular arena, original character/map assets, local controls and character progression, material/map/model tests in `game/neon-arena`. |
| `arcanewilds` | Arcane Wilds | User's premium collection. Campaign/combat/homestead/world expansion documentation; progression/inventory/cloud-save/mobile/runtime tests. |
| `chrisrunner` | Chris Runner | User's premium collection. Dedicated `game/chris-runner` README plus graphics and upgrade tests, with its retained original promotional artwork. |
| `spacetyrants` | Space Tyrants: Galactic Mandate | User's premium collection. Autonomous empire, economic/logistical/diplomatic/fleet systems with dedicated gameplay and cloud-save tests. |
| `chesswarlord` | Chess Warlord | User's premium collection. Dedicated chess-warlords core/runtime/objectives tests and browser checks; existing eight-player faction multiplayer behavior remains. |
| `hearthmouse` | Hearthmouse | User's premium collection. Full house/colony/predator survival game, downloadable build, survival documentation and controller/render/pathing/performance tests. |
| `cheesehold` | Cheesehold 3D | User explicitly called it a strong actively developed title. Existing progression/defense game plus `game/CHEESEHOLD_OVERHAUL.md` describes navigation, specialist enemies, summoner structures, allied units and later-round terrain. Its document describes implementation scope, not a claim that every described feature was independently release-verified here. |
| `gridbound` | Gridbound Realms | User explicitly called it a strong actively developed title. Actual persistent-world loader and core plus `gridbound-alive-world-patch.js`: shared realm presence/snapshot interpolation, living town/dungeon population and bounded world presentation. Always-online entry remains distinct from hosted rooms. |
| `roguecards` | Tiny Troops Emoji Battle | `game/tiny-troops/README.md` documents formation/campaign/traits/spells/mentors/relics/bench and star progression; dedicated rules/runtime/save/lifecycle modules, local/cloud save compatibility, deterministic combat recovery, and rules/save/browser tests. This is substantive campaign/squad gameplay, not a bare emoji tile. |
| `dwarfworld` | DwarfWorld | Dedicated fortress expansion exposes civic buildings with depth unlocks, dwarf equipment/perks/health, fog-aware navigation, unique relics and compressed world saves. `game/dwarf-world/fortress-command-expansion.test.mjs` and `cloud-save.test.mjs` cover integration contracts, local serializer compatibility and cloud-slot support. |
| `biblegame` | Bible Game | Existing developed Scripture game with multiple solo challenges, competitive multiplayer and ranked profile integration; shares the maintained Scripture Quest implementation and class/content artwork. The registry retains the original up-to-20-player match. |
| `bibleroguelike` | Bible Game — Roguelike | `game/bible-roguelike/README.md` documents a deterministic solo/co-op rules engine, branching encounters, 30 enemies, classes/abilities/consumables/relics, shops/mastery, migrated saved runs and transaction-safe synchronized combat. Dedicated core/sync/browser fixtures cover races, rewards, reconnects, migration and long-run snapshots. |
| `receiverwindowqb` | Receiver Window QB | Dedicated README and franchise/progression/tactics/variety/cloud-save/mobile/runtime tests. Features include a football franchise, roster training/prestige, playbook, physical passing/catching/tackling, replays, local/cloud saves and device controls. |
| `basketball2` | Basketball 2 | `game/basketball2/README.md` describes a full modular game: national-tour cities/rivals/scouting/rewards/XP/saves, AI, scoring, ball physics, contact/animation, camera/audio/quality and shared keyboard/touch/controller actions. Original legacy route redirects to its full entry point. |
| `jesusjumper` | Jesus Run | Dedicated package runs content-validation and boss-simulation checks. Modular input/physics/enemies/bosses/checkpoints/regions/storage accompany a 13-node branching Journey with several bosses and a finale; the versioned journey save tracks completion, checkpoint and permanent powers. Existing endless mode remains alongside Journey. |
| `gateguard` | The Last Five | The version-1 endless roguelite expansion layers onto an existing tactical core. Source defines multiple acts with different enemy weights, scrap, relic ranks, build nodes and quests; actual hooks extend rounds, upgrades, combat, end-game stats and new-game/restart behavior. This evidence supports a developed tactical game beyond a prototype. |
| `knightwarfare` | World Quest | Actual source implements five geography challenge modes, difficulty/area/lives/score options, question progression and terminal results/replay, local and Firestore leaderboards, map/stat cache fallback and touch-map interaction. The lobby key retains its historical name while the displayed game is World Quest. |
| `onehitknight` | One Hit Knight | Actual source implements a complete arcade loop: wave/enemy progression, slash/dash combat, upgrade choices and powerups, bosses, death/game-over/restart, local best scores and online top-ten entry. Source evidence supports a finished playable arcade experience rather than an incomplete experiment. |

All 24 belong above the lower-maturity groups in the complete library. Discover keeps the four flagships most prominent, then the eight premium experiences, then the twelve additional built-out games. A title need not be one of the original four or eight to qualify for the top library tier.

## In Production — 16 games

`grass`, `spelltower`, `historyrush`, `matchwars`, `microwars4`, `border`, `rogueworld`, `village`, `warspan`, `balatlesolo`, `stoneyrelic`, `starcraft2`, `goofywizard`, `forgeflight`, `deepsea`, `circuitbound`.

This is the default lower-priority tier for games without an explicit incomplete label or a sufficiently reviewed built-out promotion. It means their current place in the curated catalog; it does not assert that a developer is working on each title today. All remain playable and searchable through their original actions.

Specific evidence informs the boundary:

- **Circuitbound:** its README explicitly scopes the current work to the first engineering expansion (priorities 1–5). It has real component/network/structure/item simulation and save/load tests, but technology gating, production recipes and deeper simulation capabilities are described as future work. Keep it In Production rather than promoting on code volume or test count alone.
- **Grass Realms:** source includes actual age/doctrine/faction-mastery and game-over UI. This is promising substantive behavior, but a focused completeness/current-quality review was not completed in this lobby task. Its presence, original artwork and multiplayer route are retained.
- **Match Wars:** source includes per-color upgrades, a shop and autosave/load. These demonstrate a real game, but do not alone establish the same current maturity/finish as the promoted set.
- **Deep Sea:** source includes 3D survival, starter/play/upgrade states, evolving abilities and mobile controls. This audit did not fully validate its complete long-run progression or release readiness.
- Remaining entries retain their original routes and supported variants. A working route, absent `(Incomplete)` suffix, large file, polished generated cover, or old `featured` flag alone does not automatically qualify a game for the Best Games tier.
- **Warspan** has an incomplete multiplayer display alias while its primary title is unmarked. Keep one registry key in the default In Production tier rather than splitting or hiding one of its modes.

Promote any of these after reviewing its actual game loop/content, controls and continuation where relevant, and applying the owner's editorial judgment. Promotion requires only moving/adding the existing key in centralized curation, not changing gameplay definitions.

## Experimental — 23 games

`estate`, `mountain`, `mountonline`, `crownlands`, `rock`, `stockkings`, `dutchblitz`, `splinters`, `zombiezone`, `thegame`, `catmobile`, `flag`, `microwars`, `microwars2`, `swarm`, `mobamania`, `trials`, `cardrealms`, `cardrealmsmulti`, `rogueduel`, `castledefense`, `trumprev`, `balatle`.

These entries are explicitly marked `(Incomplete)` in the original registry. The key-based curation system can override that default when the owner intentionally reclassifies a game; it must not infer maturity from an attractive new image. Experimental entries are grouped lower and remain available through expansion/search and their correct solo/room/world/matchmaking actions.

`thegame` is a useful nuance: its tracked README documents substantial multiplayer consistency and presentation work with deterministic protocol tests. That establishes maintenance and a real cooperative card system, but the authoritative lobby title still explicitly marks it incomplete, and its recorded browser validation had not run in that change. This audit therefore preserves its lower tier rather than silently declaring it released. It is a candidate for a future owner-reviewed promotion.

## Tools & Learning — 5 utilities

`pdfreader`, `nflradio`, `mtgcatalog`, `fantasyfootball`, `versemem`.

These are intentional utility/learning experiences: PDF reading, authorized radio/game audio, card catalog/deck building, fantasy-draft management and verse memorization. Tools are not defective or low-quality games; keeping a separate Tools & Learning group makes the distinction clear. Their account behavior, routes and assets remain intact.

## Coverage and preservation

The recommended partition covers all **68 unique effective registry keys exactly once**: **24 Built Out + 16 In Production + 23 Experimental + 5 Tools**. Primary/promotional placement is independent of gameplay category and supported modes. No solo mode, multiplayer permission, start requirement, ranking, save system, route or analytics behavior changes with a tier.

Curation is configurable editorial metadata, not a runtime score calculated from file size, tests, play counts, source dates or artwork. New games default lower until intentionally promoted. Future review can move a key without deleting its game, duplicating its definition or breaking persisted favorites/history.
