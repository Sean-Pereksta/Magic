# Bramblebound · The Endless Road

Standalone expedition roguelite for Cat’N Mice. Open `/game/bramblebound/` through any static HTTP server; there is no build or server dependency. `/game/bramblebound.html` is a compatibility redirect.

Guide Pip through 35 regions and repeating circuits, build a loadout from 441 elemental relics, fuse ingredients, choose 147 talents and abilities, battle group encounters, guardians and ten-round Titans, and develop the Camp of Echoes between expeditions. Four equipment slots and a searchable 400-slot pack support mouse, keyboard, and touch controls. Space pauses combat. Menus and the title screen pause the simulation.

Profiles autosave under `bramblebound.profile.v1`. Existing standalone saves remain compatible; the profile menu exports/imports JSON, including prepared starting kits. Saves are device/browser local. Corrupt saves enter recovery without overwriting the original. No account is required.

## Firebase cloud journeys

Use **Firebase cloud saves** on the title screen, or the cloud section of **Profile & settings**. Assign a save name and password for a new journey; on another device choose **Reopen journey**, enter the same name/password, review it, and confirm the restore. Complete expeditions, Legacy progress, discoveries, prepared starting kits, and settings are included. A restore keeps the replaced local data at `bramblebound.cloud.backup.v1`. Only the cloud save name is remembered at `bramblebound.cloud.name.v1`; the password stays in memory for this session. Once connected, changed progress syncs about every 30 seconds while the game is visible and no dialog is open. Use **Save to Firebase** for an immediate confirmed checkpoint before switching devices. Local autosaves continue independently and work offline.

The implementation matches Arcane Wilds’ current model: Firebase project `bible-game-246c0`, anonymous authentication, and encrypted named documents in the existing `lobbies` collection. Bramblebound uses the `bramblebound-save--` SHA-256 namespace, `bramblebound-cloud-save` game type, `status: active`, and an empty player list; these documents do not appear in waiting-game lists. No global rules change or Firebase rules deployment is required. Those legacy rules permit authenticated reads/writes for generic save documents; this is password-encrypted single-player storage, not owner-only server authorization or an anti-cheat authority. Do not persist passwords or rely on a lobby username as ownership.

Payloads use AES-GCM-256 with PBKDF2-SHA256 at 210,000 iterations, random per-write salts/IVs and optional gzip. Encrypted and expanded sizes are bounded. Server-only reads, validated complete profiles, transaction revision/IV/content checks, and reopen-after-uncertain-write behavior prevent silent stale-device overwrites. Existing slots must be reopened in the current session before updates. Disconnecting removes the local binding and stops autosaving without deleting the Firebase slot. Imports and fresh profiles pause cloud synchronization. `cloud-save.test.cjs` tests real encryption and transport failures; `cloud-browser.cjs` tests desktop/mobile transfers and autosave/disconnect races with an isolated transport.

The title menu adds original generated cover art, continue/start actions, kit preparation, the handbook, profile settings, and a library link. The library registry is `lobby/lobby-core.html`; editorial placement is `lobby/lobby-showcase.js`; responsive art variants and hashes are in `lobby/assets/game-artwork.{json,js}`. The original game source was imported from the user's Bramblebound folder with expanded elemental combat and backward-compatible profile migration.

Run `npm run test:bramblebound` for combat, progression, fusion, save validation, and library integration tests. `node game/bramblebound/tests/browser.cjs` runs browser checks with Playwright installed and Chromium or an installed Edge browser. Set `BRAMBLEBOUND_PLAYWRIGHT` to a Playwright package path if it is not installed in the repo; set `BRAMBLEBOUND_SCREENSHOTS` for screenshots. The browser suite also covers supply use, set discovery, combined pack filters, stable strength sorting, and real touch swipes on full and empty slots.

## Reactions and trail supplies

Three equipped-element sets add deliberate build choices. Each requires at least one relic of both named elements. Two pieces awaken the set; three relics from the same pair strengthen it, leaving one free slot. A four-slot 2+2 loadout can activate two sets. Pack contents never count.

| Set | Two-piece reaction | Three-piece improvement |
| --- | --- | --- |
| Emberglass · Fire + Frost | Fire consumes chill/freeze to shatter up to three foes for 35% of its direct hit and interrupt for 0.35s. | 65% shatter and 0.6s interrupt. |
| Bramble Circuit · Leaf + Storm | Storm hits poisoned foes 20% harder, spreads 40% poison to the group, and advances Leaf relics by 0.3s. | 35% bonus damage, 65% spread and 0.55s recharge. |
| Soulstone · Stone + Spirit | Stone activations grant 2% max HP shield; shielded Spirit activations advance Stone relics by 0.45s. | 4% shield and 0.8s recharge. |

Reaction procs use saved 1.5-second simulation cooldowns and cannot recursively activate relics. The **Element sets** row opens all three requirements and bonuses. Dedicated support relics keep zero damage; the Soulstone loop also supports defensive loadouts.

The **Trail pouch** is separate from the 400 relic slots. Five supplies have original SVG icons: Moss Tonic heals and cleanses; Bark Ward builds shield; Ember Bomb damages the group; Frost Bell freezes the group; Quicksilver advances relic and automatic-ability cooldowns. Open the pouch to pause, choose a supply, and return to the trail. Invalid or wasteful uses are disabled. Shared recovery takes 1.25 seconds of simulation time, including walking; menus and pause never advance it.

New runs start with a tonic and ward and hold six supplies. Finds roll once per cleared wave at 24%, or 50% for guardian/Titan waves, with a find after four dry waves. A full pouch leaves the find behind. Wayfinder’s **Provision Pockets** adds one starting supply and two capacity per rank (four ranks); **Field Alchemy** raises healing, shield, blast, freeze and recharge potency by 20% per rank (three ranks). Active runs retain their purchased snapshot. Old saves gain an empty pouch without refills; supplies and reaction timers survive local exports and encrypted cloud transfer.

The pack has two visible controls: **Combinable** shows items with any valid carried partner, including equipped relics; **Strongest first** orders occupied cards by sustained damage or utility, with stable ties and original storage slots preserved. Control uses an eight-potency-per-second equivalent for the convenience sort; exact comparison cards retain their real units. Search, rarity and these controls work together. Quick horizontal swipes turn pages from filled or empty slots. Hold an occupied relic for 280ms before dragging on touch; mouse dragging remains immediate. Combat adds bounded elemental sparks, impact rings, supply pickup art and distinct set effects, with stationary feedback under reduced motion.

## The Living Legacy expansion

The Camp of Echoes has six permanent districts: Heartwood Sanctuary, Wayfinder’s Lodge, Ancient Forge, Fateweaver, Elemental Grove, and Mythic Shrine. Its 77 purchasable entries include the original 100-rank Heartwood and Iron Grip, new ranked upgrades, branch milestones, six eight-rank elemental masteries and their capstones, all 15 constellations, ten Mythic Oaths, Convergence, and an endgame prestige investment. District scenes grow with investment; compact Grove views and expandable paths keep detailed cards manageable on phones. Each purchase shows its rank, current benefit, next benefit, price, and prerequisites.

Trail Seeds retain their wave and guardian reward foundation. First region guardians, region milestones, and discovery achievements add one-time rewards to the expedition summary. Permanent unlocks remain earned across expeditions and refunds. At camp, a full respec returns the exact recorded purchase cost once and removes refunded ranks, masteries, constellations, Oaths, active preferences, and derived benefits. Historical dead expedition snapshots remain reloadable after refunds.

The Heirloom Vault saves three named starting kits using discovered, unlocked base relics. Kits begin with two equipment positions, or three with Relic Monarch, and receive heirloom levels and other permanent benefits when a new expedition starts. Existing manually prepared kits, including their forged gear, still load and receive those benefits. Legendary Expedition enables an element preference; Relic Calling enables style preferences; the Destiny Board influences talent draft weights. Oaths can be switched freely at camp, initially one at a time and two after buying Convergence.

The expedition pool preserves all 64 previous entries and adds 24 elemental talents, eight cross-element/diversity talents, eight general talents, six automatic abilities, 22 Mythics, and 15 empowered constellation talents. Ordinary drafts remain tied to XP levels. Explicit starting blessings and every-fourth-guardian Fateweaver rewards are the only added bonus drafts. Fateweaver upgrades supply up to five choices, rerolls, banishes, and two-draft echoes. Talent rarity is separate from relic rarity; depth, guardians, gear, permanent mastery, and build preferences govern eligibility and weighted offers. The Sixfold Crown requires all six permanent masteries, while its combat strength uses the current four-slot elemental diversity.

`legacy.js` owns version 2 permanent data within the unchanged version 1 profile envelope and storage key. `living-combat.js` extends the existing progression engine with primary activation, healing, incoming-hit, death, encounter-start, and simulation-clock hooks. Companions and all ability counters/timers are serialized in the run. Local and encrypted Firebase transfers use the same validated profile, preserving prior ranks, seeds, discoveries, equipped gear, prepared kits, and active expeditions.

Only a primary relic activation enters the activation-talent path. Secondary hits, spells, companion attacks, and death bursts cannot recursively trigger it. Death effects process each corpse once, then resolve any newly defeated foes in a bounded encounter sweep. Dedicated healing, shield, stun, and freeze relics retain zero inherent damage; independently purchased offensive powers may still act alongside them. Control is capped at eight seconds and relic cooldowns at a minimum of 0.15 seconds. Summons are capped at 24 with additional summon strength aggregated; rendering shows up to twelve allies and limits visual effects while retaining combat resolution. All combat timers use the supplied simulation time, so pause, accelerated replay, and reload share the same clock.

Run `npm run test:bramblebound` for the existing regressions plus Legacy economy, prerequisites, refunds, migration, all constellations, all new talents/Mythics, support roles, companion lifecycles, and encrypted expansion-state transfer. `npm run test:bramblebound:browser` includes desktop/phone camp purchases, preparation, reload, active-run guards, and crowded combat/render checks. Browser tests accept `BRAMBLEBOUND_PLAYWRIGHT`, `BRAMBLEBOUND_BROWSER`, and `BRAMBLEBOUND_SCREENSHOTS` for local installations and visual QA. Cloud tests use real encryption with an isolated Firebase transport; they do not write a production cloud slot. The automated performance checks are regression checks, not a substitute for extended playtesting on physical low-end phones or long-term economy balancing.

## Expanded road

The expansion adds 15 biomes, 105 enemies, 72 relics, and 30 fusion recipes. Every element has innate combat behavior. Twelve signature weapon mechanics and eight counter types support deliberate loadouts; enemy dossiers and the searchable field guide explain them. A Dewglass Vial is included in every new starting backpack so cleansing is available immediately. Held weapons now have dedicated pixel artwork, elemental trails, rarity markings, and level sockets.

Existing profiles migrate without resetting their run, gear, discoveries, or records. A saved 8- or 20-region circuit finishes before the next circuit opens all 35 regions. New expeditions use 35 regions immediately.

## Support relics and deeper fusions

The support expansion adds 96 relics: 24 regular finds, 48 first-stage fusions, and 24 second-stage fusions. A further 50 fusions add 24 first-stage, 18 second-stage and eight third-stage recipes. Before the Ancient expansion, the catalog contained 156 base relics and 274 recipes: 208 first-stage, 57 second-stage and nine third-stage fusions. Healing-only relics restore HP, shield-only relics build protection, stun-only relics interrupt enemy attack charge, and freeze-only relics encase an enemy in ice and stop its charge. These four roles deal zero damage at every level; their elemental affinities and level milestones improve utility without adding elemental hits, damaging echoes, or offensive on-hit techniques. The guide offers Healing, Shield, Stun and Freeze role filters, and item details show recovery strength or control duration instead of damage. Combine support with an offensive relic to clear encounters.

Every fusion produces an item exactly one level above its highest-level ingredient: level 2 + level 3 becomes level 4, and level 2.5 + level 3.5 becomes level 4.5. Crafted relics can become ingredients for second- and third-stage recipes. For example, Moonfrost Garden and Tempest Glacier Prism fuse into the third-stage Winter Beyond the Moon. Recipe output power and sustained output are at least 5% above both ingredient definitions before the level gain, and output rarity is never below either ingredient. Two identical relics still require matching levels and upgrade by one level. Existing fractional item levels and all previous item IDs remain compatible with local and cloud saves.

Utility potency increases at levels 3, 5 and 10. Healing remains capped at maximum HP, shields at 40% of maximum HP, and stun/freeze duration at eight seconds. Control durations persist across save/reload; older saves receive an empty freeze state. The regression suite covers every recipe at mixed and fractional levels, forging through all three stages, pure utility at milestone levels across all six elements, recovery caps, control timing, and existing save migration. It crafts a complete new third-stage chain from base ingredients with reloads between stages while preserving old gear. The browser expansion suite also checks utility labels, role filters, and fusion previews through the third stage at desktop and phone widths.

Selecting a backpack relic opens a compact comparison above the equipped cards. Each equipped item's icon has signed stat differences, with accessible tooltips explaining damage, healing, shield, lifesteal, control duration and cooldown. A separate icon row describes the selected relic's effects. Values compare predictable single-target output per activation, including rapid follow-ups, level echoes/nova and Aftershock, with current levels, permanent power and applicable talents; target-dependent weaknesses, defenses, damage-over-time and other special damage remain outside the neutral comparison. Lifesteal is a percentage of damage removed, while healing and shield are uncapped potential gains. Viewing or switching comparisons does not change gear, cooldowns, combat state or saved progress.

## Rarity and finds

Six tiers classify sustained damage, utility and signature effects: Common, Uncommon, Rare, Epic, Mythic and Ancient. Their rarity power multipliers are 1×, 1.12×, 1.28×, 1.5×, 1.8× and 2.6×; item details include this bonus in displayed damage. Ancient artifacts can only be forged from two Mythics and never enter random loot pools. Tier changes preserve existing item IDs, levels and recipes. Rare finds receive stronger ornament and auras.

The chance of at least one relic in a normal wave is 24%; guardian waves have a 48% chance, independent of enemy headcount. Rarity is chosen before duplicate, support or ingredient bias. At the first region with all tiers unlocked, ordinary finds are 66.3% Common, 20% Uncommon, 10% Rare, 3.5% Epic and 0.2% Mythic. Mythic odds rise by 0.05 percentage points per ten-region band, capped at 0.35%; guardian Mythics begin at 0.6% and cap at 0.9%. Each Legendary Scent rank adds 0.02 percentage points. Exact local odds appear in the atlas. Fusion remains an alternate route to strong gear. Tests include 28,000 cleared waves and verify rarity frequencies, headcount normalization and owned-Mythic independence.

## Ten-round Titans

Every freshly spawned tenth round replaces the ordinary formation with a colossal Titan and two regional escorts. Six unique encounters rotate through the road, returning with stronger round scaling after round 60. The regional guardian cycle continues on other rounds. The boss banner shows its health, phase and named attack warning; each Titan has two alternating powers and enrages at half health, gaining faster casts and stronger abilities. Control pauses its windup, so control relics and the counter advice in each dossier remain useful. Defeating a Titan grants an eligible Epic cache with a 10% chance to upgrade to a base Mythic. Every fiftieth round guarantees a base Mythic, keeping progress toward Ancient forging without a Mythic flood. Clearing its formation restores 25% maximum HP.

| Round | Titan | Signature powers |
| --- | --- | --- |
| 10 | Yggra, Worldroot Behemoth | Thorn eruptions snare Pip; a living canopy heals and wards its formation. |
| 20 | Pyraxis, the Sun Eater | A collapsing sun leaves persistent fire; a meteor barrage strikes repeatedly. |
| 30 | Isolde, Winter Empress | An ice cathedral chills relic recharge; mirror ice restores her defenses. |
| 40 | Raikora, Tempest Roc | Repeated lightning disrupts abilities; a hurricane hastens its next strike. |
| 50 | Gravemaw, Mountain Heart | A worldbreaking fault corrodes defenses; continental armor restores its ward. |
| 60 | Nyxara, Eclipse Sovereign | A soul eclipse steals life and hexes relics; spectral tides consume shields. |

Titan warning progress, phase, cast count and simulation timers survive local and cloud saves. Existing active encounters are preserved when loading an older profile; the next newly spawned tenth round uses the Titan system.

## Ancient artifact forge

Eleven Ancient recipes raise the catalog to 441 relics and 285 fusion recipes. Each uses exactly two existing Mythics, consumes both ingredients, and creates an Ancient one level above the higher ingredient, including fractional levels. Their base potency and sustained output exceed both ingredients before that level gain. Upgrading two identical Ancients at equal levels follows the existing copy-upgrade rule. The backpack and field guide include an Ancient filter, and dossiers show the recipe, Ancient power and character aura.

| Ancient | Two Mythic ingredients | Power and character effect |
| --- | --- | --- |
| Trident of the Genesis Tide | Kingtide Conch + Mother-Root Crook | A formation-wide living tide, healing and protection; luminous waves and turquoise antlers. |
| Crown of the First Grove | Worldtree Staff + Ambrosia Flask | Poison eruptions, roots and healing; a living crown with orbiting seed stars. |
| Sunforged Cataclysm | Volcano Heart + Eclipse Talon | Formation-wide solar blasts and intense burning; solar wings and molten foot runes. |
| Heart of the Endless Winter | Oracle’s Weatherglass + Cometfall | Freezes the entire formation and grants protection; ice halo and aurora mantle. |
| Regalia of the Storm Sovereign | Tempest Crown + Daybreak Mantle | Six interrupting thunderbolts and protection; lightning motes and a storm cloak. |
| Aegis of the World Titan | Titan Carapace + Gravity Maul | Fills shields and reduces incoming direct hits; floating spectral stone armor. |
| Eclipse of a Thousand Souls | Lantern of Lost Stars + Phoenix Diadem | A soul wave that drains the entire formation; a black moon and violet soul wings. |
| Orrery of Falling Heavens | Firstlight Judgment + Atlas of Returns | Three ward-breaking meteors that fracture foes; orbiting planets and golden star trails. |
| Grail of the Eternal Dawn | Everbloom Fountain + Sunheart Sanctum | Powerful healing, protection and complete cleansing; radiant lotus petals and golden ribbons. |
| Hourglass Before the First Storm | Thunderous Silence + Silent Solstice Bells | Formation-wide stun and faster allied relic recharge; shattered clock rings and teal shards. |
| Voidheart of the Unmade World | Eclipse Worldreaper + Stormwinter Singularity | A ward-stripping singularity and stronger guardian damage; a gravitational halo and violet stars. |

The four pure utility Ancients (freeze, shield, healing and stun) remain harmless: their additional powers improve protection or control without introducing damage. Secondary Ancient attacks never recurse into primary activation effects. Character auras appear while the artifact is equipped; activation effects use the simulation clock and respect reduced motion. Rendering caps visual particles without reducing combat effects.

`node game/bramblebound/tests/ancient-boss-browser.cjs` verifies actual two-Mythic forging, ingredient consumption, fractional levels, Ancient filters and dossiers, equipped-item reload, round-ten spawning, saved attack warnings, live ability casts with reduced motion and enraged rendering at desktop and phone widths. It also checks distinct renderings of all six Titans, twelve boss powers and eleven equipped Ancient transformations. Set `BRAMBLEBOUND_SCREENSHOTS` to capture the forge, boss scenes and visual overview galleries.
