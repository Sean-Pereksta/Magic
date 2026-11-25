# MOBA Mania

A self-contained solo 3v3 pixel MOBA at `/game/CatLanes.html`. Every match has one human hero, two allied bots, and three enemy bots. Choose among eight playable heroes: **Arc Rift**, **Red Dragon Warrior**, **Emerald Druid**, **Pink Angel**, **Dark Ranger**, **Purple Knight**, **Orange Assassin**, and **Blue Angel**. There is no mouse character in the roster.

## Hero expansion designs

[Four proposed heroes](HERO-EXPANSION.md) add an emplacement engineer, an anchor-wielding initiator, a mobile gunslinger, and a time-magic support. The design includes abilities, all seven talent tiers, pixel silhouettes, animation cues, counterplay, and bot behavior. These are future roster designs; the eight heroes above remain the playable roster.

## Play

Choose a hero and an AI difficulty, then start a match. Destroy outer and inner towers to open a route to the enemy crystal. Your own crystal, towers, and healing well need protection. Each lane sends three footmen and two archers per wave. Jungle camps award experience; neutral beasts appear in the river-side pits, and the team that delivers the killing blow summons a siege beast to attack a lane.

Waves begin after 4 seconds and repeat every 19 seconds. Jungle camps contain three creatures and respawn 44 seconds after the whole camp is cleared. The first river beast appears after 55 seconds; another spawn opportunity occurs every 100 seconds, alternating between the two pits without duplicating a living beast in a pit.

New river beasts gain a tier every 100 seconds of match time. Each additional tier now adds **44% of base health and 30% of base attack damage**. The captured lane attacker inherits the defeated beast’s tier, with **46% more base health and 32% more base attack damage** per additional tier. Neutral special attacks add 30% of base damage per tier, and the captured Colossus's shared wards add 25% of their base shield per tier. Growth is linear and the first tier keeps its original strength.

| Beast stage | Neutral health / attack damage | Captured health / attack damage |
| --- | --- | --- |
| Tier 1 | 1.00× / 1.00× base | 1.00× / 1.00× base |
| Tier 3 | 1.88× / 1.60× base | 1.92× / 1.64× base |
| Tier 5 | 2.76× / 2.20× base | 2.84× / 2.28× base |
| Tier 10 | 4.96× / 3.70× base | 5.14× / 3.88× base |

Later captures therefore create substantially stronger lane pushes as well as tougher jungle fights. The 100-second spawn schedule, alternating pits, and one living neutral beast per pit stay the same; an existing beast keeps the tier it received when spawned.

Abilities unlock at levels 1, 2, 4, and 6. Talent choices at levels **3, 5, 8, 10, 12, 15, and 18** specialize each hero. Every tier offers three exclusive choices with small pixel icons: **32 active abilities and 168 talent options** across the roster. The later choices change existing abilities, summon behavior, defensive responses, and attack combinations while keeping the same four ability buttons. Open the skill tree when its badge lights up. The solo simulation pauses while selecting a talent.

### Personal progression and the item shop

Heroes can reach **level 18**. Each next level requires `150 + 50 × (level − 1) + 3 × (level − 1)²` XP, so the opening level costs 150 XP and later levels progressively take longer. Reaching level 18 requires 13,838 XP. Health, mana, attack damage, and level-scaled abilities continue growing through the cap. After the first two minutes, XP rewards from lane creatures, jungle camps, and beasts gradually grow to a maximum of four times their opening value. Heroes still have to participate in those kills; time passing awards no XP by itself.

XP belongs to the heroes who participate in a kill. The finishing hero receives the full XP reward; other allied heroes who damaged that target in the previous ten seconds receive half. Effective healing and shields that actually absorb damage can also qualify a supporting hero for a hero-kill assist. Overhealing, unused shields, and self-support do not qualify, and contributing through several methods still awards only one assist. Nearby teammates who did not contribute receive nothing, including when a tower or river beast falls. The scoreboard's assist count records hero-kill assists.

**Gold comes only from your finishing blows.** Assists, nearby kills, time passing, and unowned allied minions or towers do not earn you gold. Your own projectiles, damage-over-time effects, item procs, and personal summons credit their kills to you. A captured river beast is a team lane attacker, so its subsequent kills do not passively pay its capturer. Finishing a target shows a gold-colored `+N GOLD` notice and updates your personal balance. Hero kill streaks add a visible shutdown bounty of 60 gold per kill beyond the second, capped at 300 bonus gold, paid only to the finishing hero.

Open **SHOP** on desktop or mobile, or press **I**, to buy permanent match upgrades. Combat pauses while the shop is open. The three tiers contain ten items each:

| Tier | Purchase requirements |
| --- | --- |
| Basics | Gold only |
| Upgrades | Gold and one specified basic item |
| Advanced | Gold, one specified basic item, and one specified upgrade |

All 30 items have pixel icons, visible bonuses, prices, and named prerequisites. Purchases activate immediately and appear in the active inventory. Prerequisites stay active after an upgrade; displayed prices are the additional cost, and every item can be purchased once per match. There is no inventory-slot limit. Items improve health, attacks, ability power, cooldowns, and other supported combat stats. Cooldown reduction from items and talents shares a 50% cap. Items and unspent gold survive death and recall; a new match resets both. Bots earn and spend their own final-hit gold under the same purchase rules.

Buying gear immediately updates your hero's stats and future actions. Existing projectiles, already summoned creatures, and cooldowns already counting down keep their original cast values; the next cast uses the new bonuses.

Every advanced item also adds a distinct passive:

| Advanced item | Passive |
| --- | --- |
| Sovereign Aegis | Share a meaningful shield with another nearby allied hero. |
| Worldsplitter | Every third basic hit cleaves enemies in front of you. |
| Starfire Scepter | A direct ability hit calls down a delayed blast. |
| Eternity Hourglass | Three different abilities in a short sequence refund other cooldowns. |
| Celestial Font | Every third cast restores mana and releases a healing pulse. |
| Tempest Wings | A movement ability grants haste and empowers the next basic hit. |
| Thunderbolt Crossbow | Every fourth basic hit chains lightning to nearby enemies. |
| Phoenix Crown | Meaningful healing also grants a short ward. |
| Elderwood Staff | The first summon of each cast becomes a stronger, warded guardian. |
| Crimson Relic | A basic hit on a hero while wounded restores health and grants a ward. |

The shop displays each passive and its recharge conditions. Pin an advanced item to see its remaining total cost and the next required ingredient. Expand a comparison to see actual before-and-after stats and all four ability cooldowns, including the shared 50% cooldown cap. Recommendations follow each hero's role; Blue Angel favors sword, attack-speed, and mobility upgrades.

| Hero | Theme |
| --- | --- |
| Arc Rift | Golden armor, ranged staff, blue storms, forcefields, and spirit warriors |
| Red Dragon Warrior | A line-shaped dragon attack, close-range fire, defensive scales, and a fiery charge |
| Emerald Druid | Healing trees, treants, bears, and wolves |
| Pink Angel | Area healing, inspiration, haste, and protective shields |
| Dark Ranger | Poison arrows, arrow fans, stun bombs, and long-range shots |
| Purple Knight | High health, a directional shield bash that pushes and stuns, shared barriers, a rushing charge, and a stunning ground quake |
| Orange Assassin | Single-target heartseeker thrusts, a quick dash, an execution cut that punishes missing health, and a lunging finisher that refreshes the dash on a kill |
| Blue Angel | A large sword with a wide frost sweep, a fast wing dash, allied inspiration and shields, and a freezing area attack that leaves enemies slowed |

Each hero also starts with a signature passive, shown on the selection screen: Arc Rift builds a third-cast burst; Red Dragon Warrior bursts against burning prey; Emerald Druid wards summons and rewards their attacks with healing; Pink Angel turns effective support into haste and mana; Dark Ranger marks repeated targets; Purple Knight gains a defensive counterattack; Orange Assassin follows movement with an empowered strike; and Blue Angel builds a rhythmic frost strike with allied haste.

### Vision, objectives, and combat warnings

Allied heroes, minions, summons, captured beasts, and structures share vision. Brush conceals opponents until close proximity or a reveal exposes them. Both teams' bots use the same visibility rules as the player. The minimap shows fading, fixed last-seen hero positions for up to eight seconds; those markers do not track hidden movement.

Heavy attacks such as Royal Quake, Winterfall, and Sky Cleave show their area and countdown before landing. Enemy and neutral warnings use distinct color and line treatments. Repeated stuns receive diminishing duration, and the HUD shows remaining stun time. A gold marker on the selected enemy indicates when a basic attack can finish it; absorbed damage and earned gold have distinct feedback.

**River Maw** warns before a directional bite, then becomes a fast lane-clearing attacker when captured. **Moss Colossus** sends staggered, avoidable rock impacts, then becomes a slower siege guardian that shields allied minions and pressures towers. After an allied capture, choose a lane on the HUD within four seconds; the displayed default lane is used if no selection is made.

Bots hold threatened secondary lanes, peel for wounded allies, flank when appropriate, and choose talents and recipes that fit their roles. They approach vulnerable minions for last hits and use jungle camps to catch up through their own kills. Purple Knight and Pink Angel deal additional damage to lane and camp creatures so their defensive roles can still earn personal income. Both human and bot heroes follow these rules.

### Pixel combat effects

Landed attacks release short, directional pixel bursts. Steel hits scatter sparks, fire throws rising embers, frost breaks into icy fragments, poison splashes green, and healing releases small colored motes. Projectiles leave matching pixel trails, while spell casts, dashes, explosions, and heavy impacts have their own accents. Absorbed hits ripple against a shield; breaking its last protection releases a separate burst of shards.

Active shields surround the protected unit with a translucent, faceted shell. Their interiors remain transparent enough to see the hero and animation. The shield's source determines its appearance: Dragon uses orange fire and scales, Arc Rift uses cyan energy, Purple Knight uses amethyst plates, Pink Angel uses rose-gold light, Blue Angel uses icy facets, and Emerald Druid uses green growth. A stronger existing shield keeps its identity when a weaker shield refreshes it. A remaining damage-reduction buff can retain a lighter guard outline after the absorb shield breaks.

These effects follow the actual hit, shield, healing, and buff state. They respect shared vision and concealed units. Pixel bursts are capped separately from persistent spell fields, and the renderer limits extra particles per frame, with smaller budgets on compact screens and when the browser requests reduced motion. Combat warnings, aim guides, and health bars stay above the decorative effects. Cosmetic scatter uses deterministic math without consuming the combat random sequence or changing damage, cooldowns, targeting, or rewards.

### Character motion and the living battlefield

All eight heroes have distinct attack and casting poses, directional weapon motion, and recovery frames. Knights brace for a bash, the ranger draws the bow, the assassin crouches into thrusts, and both angels use their wings and weapons during their abilities. Small hit reactions offset only the drawn sprite. Immediate attacks show their strike immediately; delayed slams prepare against the real impact deadline. Cosmetic movement does not shift a unit's collision position or add a combat delay.

River Maw opens and snaps its jaw during its bite; Moss Colossus lifts its arm and sends rocks toward the existing impact warnings. Higher beast tiers add plates, spines, and luminous details with a bounded set of art stages. Capturing a beast adds a team-colored transformation. Jungle creatures have idle poses, and permanent camp scenery distinguishes wolf dens, boar mud patches, and golem runestones without revealing whether a concealed camp is occupied.

Towers develop cracks below half health and deeper damage below quarter health, shed chips when hit, and collapse into rubble. A destroyed core breaks into floating crystal fragments before the results appear. That 1.2-second finale uses a presentation clock while combat remains finished; the completed match is cleared and mastery is recorded as soon as the core falls.

Resolved heavy attacks leave subdued, harmless ground marks: fire scorches, melting frost, quake cracks, impact stones, and poison droplets. Marks fade within a few seconds and have a separate bounded presentation pool. Visible units raise dust on lanes, bend brush, and disturb the river; heavy beasts make broader wakes. Nearby foliage reacts to visible heavy impacts.

Treants rise through roots, wolves leap into view, bears land heavily, and spirits assemble from blue fragments. Departing summons dissolve with matching details. Finishing-blow gold travels toward your shop counter, level-ups pulse around your hero, newly unlocked abilities highlight briefly, and advanced item purchases display their icon above the hero. These presentations follow actual progression events and do not grant additional rewards.

### Attack orders and tower pressure

**Primary click, tap the battlefield, ATTACK, or Space** selects the nearest visible living enemy nearby, then automatically approaches to your hero's melee or ranged attack distance and keeps fighting. Clicking or tapping directly on an enemy focuses that enemy instead, even when another enemy is closer. Enemy heroes, minions, neutral creatures, captured beasts, and exposed structures are valid targets. Protected inner towers/crystals are excluded until their prerequisites fall.

Automatic selection searches within 360 world units, or your current attack range plus 140 units if that is larger, measured to the enemy's collision edge. It keeps selecting the nearest valid enemy within that area as units move. If none is nearby, the hero stays put with a **READY / No nearby enemy** indicator and engages when an enemy approaches. Shared vision of a remote tower or jungle camp never starts an automatic cross-map chase. An explicitly clicked visible enemy stays focused and can be pursued farther until it becomes invalid or you issue another order.

Releasing ATTACK does not stop the order. A new movement order, successful ability cast, recall, team command, or death clears the previous pursuit. Pressing ATTACK again chooses the nearest nearby enemy anew. **Right click** issues ground movement; keyboard and joystick movement take immediate priority. A primary click or tap on empty ground attacks rather than moving to that location. When a skill is armed, a battlefield click or tap casts it at your aim. Moving the aim pointer alone never changes a basic-attack target or auto-aims an ability.

Towers and crystals fire visible homing bolts. They retain a valid target, usually engage an incoming wave first, and retaliate against enemy heroes who attack a hero they protect. Consecutive shots against one hero grow stronger; leaving range breaks the focus. Bots check the whole route and mobility-skill destination before entering hostile tower range. They escort healthy frontliners and wait for a wave, or commit to a brief finishing attack only when the health, defenders, distance, and likely escape justify it.

Early fortification and reduced creature/summon damage to structures give heroes time to farm before a decisive siege. Fortification gradually fades over ten minutes, and surviving inner towers strengthen the crystal. Matches still end only when a crystal is destroyed; there is no timer that forces a winner.

### Controls

- **WASD / arrow keys:** move.
- **Mouse:** aim spells. Primary click attacks a clicked enemy or the nearest nearby enemy; right click moves. **Space / ATTACK:** engage the nearest nearby enemy until another order.
- **Q, E, R, F:** hold to aim and release to cast the four abilities; the ability bar shows unlock levels, mana, and cooldowns.
- **B:** channel a recall to the healing well. Moving or taking damage interrupts it.
- **I / SHOP:** open the item store and view personal gold, prices, prerequisites, and active upgrades. Escape or the close button returns to combat.
- **Touch:** left thumbstick to move, tap an enemy to focus it, tap the battlefield or ATTACK to engage the nearest nearby enemy, and ability buttons to aim and release a skill shot.
- **Hold/drag the minimap:** temporarily inspect another location; release to return to the hero. This does not move the hero or change spell aim.
- **Controls:** adjust touch-button size, height, inset, and handedness; preferences are saved on this browser. The normal camera follows your hero and looks slightly ahead while aiming a selected ability.
- Use the skill-tree, pause, beast-lane, and team-command controls on the HUD.

The difficulty selector changes bot decisions, teamwork, reaction timing, aim, and retreat behavior. This is a solo match against bots; it does not require a multiplayer server.

### Resume and mastery

An unfinished match saves locally every ten seconds and when pausing or leaving the page. **Resume match** restores the simulation in a paused state, including items, talents, cooldowns, projectiles, summons, warnings, bot plans, and random state. Starting over asks before replacing a different saved match. Invalid or incompatible saves offer a recovery path. Autosaves check the saved match ID and refuse to overwrite a different match unless replacement is confirmed.

The results screen records hero and structure damage, effective healing, absorbed shielding, personal gold, lane/jungle last hits, assists, objectives, shutdowns, and streaks. Match badges highlight those contributions. Completing a match earns per-hero mastery that unlocks armor tints, trails, portrait treatments, and victory colors. Mastery never changes combat stats, and each completed match is credited once. Saves, settings, and mastery belong to the current browser/origin; they are not cloud-synced. When browser storage is unavailable, the game remains playable and reports the persistence limitation.

## Files and hosting

`data.mjs` defines heroes, talents, map geometry, difficulty settings, progression rules, and beast growth. `items.mjs` defines the 30-item catalog, prerequisites, stat bonuses, passive descriptions, purchase validation, and pixel item icons. `engine.mjs` runs deterministic combat, individual rewards, item activation, and AI without browser dependencies; `hero-skills.mjs` implements the knight, assassin, and sword angel abilities. `progression.mjs` handles signature passives, late evolutions, item effects, and delayed impacts. `strategy.mjs` handles shared vision, role assignments, build plans, and match metrics. `save-state.mjs` validates and restores the simulation. `profile.mjs` handles local mastery, settings, recipe planning, and shop comparisons. `renderer.mjs` draws the battlefield, fog, warnings, animated pixel sprites, cosmetics, focus markers, and icons. `combat-visuals.mjs` draws bounded pixel bursts, trails, spell accents, and translucent shields; `unit-animation.mjs` selects combat poses and summon motion; `world-visuals.mjs` draws camp scenery, reactive terrain, destruction, spell aftermath, summons, and reward presentations. `ui.mjs` connects the simulation to keyboard, pointer, touch, local persistence, and the short victory/defeat finale; `style.css` styles the compact HUD and menus.

Serve the existing `game/CatLanes.html` alongside the complete `game/moba-mania/` runtime directory. Native JavaScript modules need an HTTP(S) origin; opening the HTML directly from `file://` is not supported. The game has no CDN, font, sprite, or server dependency. Pixel art is defined in code, so there is no separate model download or asset-host configuration.

The website's existing `/hub/play-signal.mjs` integration remains on the normal route. The desktop pack builder removes that script and copies the complete adjacent runtime to `packs/mobamania/moba-mania/`, keeping `index.html` as the pack entry. On file systems where the historical `CatLanes.html` and `catlanes.html` names collide, it reads the exact uppercase entry from Git.

To rebuild the desktop edition from the selected release checkouts:

```sh
python3 desktop-hub/content/build-packs.py /path/to/Magic /path/to/Truth --only mobamania
```

Rebuild the catalog after packs when preparing a complete desktop release, using the existing release workflow. This source change does not rewrite historical release verification records or claim that an older downloadable launcher already contains the new game.

## Verification

```sh
npm run test:moba-mania
node tests/moba-mania-browser.cjs
npm run bench:moba-mania -- --heroes=bulwark,angel,ember,arc --difficulties=normal
```

The browser suite uses the Playwright-managed Chromium installation by default; set `CHROMIUM_PATH` to use an installed compatible binary.

The mechanics suite checks combat behavior, talent restrictions, all signature and advanced-item passives, individual XP/gold attribution, progression through level 18, recipes and real stat effects, structure protection, wave composition, different beast attacks and captures, fair visibility, support assists, crowd-control limits, warning countdowns, deterministic save/resume, corrupt-save recovery, mastery, bot builds and roles, offline dependencies, and completed simulated matches. The browser suite checks desktop and both phone orientations, hero selection, input, multi-touch aimed casting while holding attack, camera following and minimap peeking, display of all seven talent tiers, shop pinning/comparisons/purchases, control settings, save/reload/resume, saved-match replacement, mastery, results/replay, and offline module dependencies. It injects its inspection hooks only into the test-served module; production code has no cheat or test global.

The pacing benchmark reports match duration, level 6/10/12/15/18 timing, first upgrade and advanced item, final inventory, personal gold, combat/travel activity, and contribution metrics as JSON Lines. It uses deterministic bot-led matches and is a regression aid, not a human win-rate or balance estimate. Defaults cover all eight heroes on normal and hard for up to eighteen minutes; use `--heroes`, `--difficulties`, `--minutes`, and `--seed` to select a sample.

