# MOBA Mania

A self-contained solo 3v3 pixel MOBA at `/game/CatLanes.html`. Every match has one human hero, two allied bots, and three enemy bots. Choose among eight playable heroes: **Arc Rift**, **Red Dragon Warrior**, **Emerald Druid**, **Pink Angel**, **Dark Ranger**, **Purple Knight**, **Orange Assassin**, and **Blue Angel**. There is no mouse character in the roster.

## Play

Choose a hero and an AI difficulty, then start a match. Destroy outer and inner towers to open a route to the enemy crystal. Your own crystal, towers, and healing well need protection. Each lane sends three footmen and two archers per wave. Jungle camps award experience; neutral beasts appear in the river-side pits, and the team that delivers the killing blow summons a siege beast to attack a lane.

Waves begin after 4 seconds and repeat every 19 seconds. Jungle camps contain three creatures and respawn 44 seconds after the whole camp is cleared. The first river beast appears after 55 seconds; another spawn opportunity occurs every 100 seconds, alternating between the two pits without duplicating a living beast in a pit.

New river beasts gain a tier every 100 seconds of match time. Each additional tier adds 22% of base health and 16% of base attack damage. The captured lane attacker inherits the defeated beast’s tier, with 24% more base health and 17% more base attack damage per additional tier. Later captures therefore create stronger lane pushes as well as tougher jungle fights.

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

### Attack orders and tower pressure

Press **ATTACK** or **Space** to select the nearest visible living enemy, then automatically approach to your hero's melee or ranged attack distance and keep fighting. Automatic attacks keep checking the nearest valid visible enemy as units move. Enemy heroes, minions, neutral creatures, captured beasts, and exposed structures are valid targets. Click or tap a visible enemy to explicitly focus it, even when another enemy is closer; when that target becomes invalid, the hero resumes nearest-target attacks. Protected inner towers/crystals are excluded until their prerequisites fall.

Releasing ATTACK does not stop the order. A new movement order, successful ability cast, recall, team command, or death clears the previous pursuit. Pressing ATTACK again chooses the nearest enemy anew. Ground clicks/taps and right clicks issue movement; keyboard and joystick movement take immediate priority. Moving the aim pointer alone never changes a basic-attack target or auto-aims an ability.

Towers and crystals fire visible homing bolts. They retain a valid target, usually engage an incoming wave first, and retaliate against enemy heroes who attack a hero they protect. Consecutive shots against one hero grow stronger; leaving range breaks the focus. Bots check the whole route and mobility-skill destination before entering hostile tower range. They escort healthy frontliners and wait for a wave, or commit to a brief finishing attack only when the health, defenders, distance, and likely escape justify it.

Early fortification and reduced creature/summon damage to structures give heroes time to farm before a decisive siege. Fortification gradually fades over ten minutes, and surviving inner towers strengthen the crystal. Matches still end only when a crystal is destroyed; there is no timer that forces a winner.

### Controls

- **WASD / arrow keys:** move.
- **Mouse:** aim spells. Click an enemy to focus it; click the ground or right click to move. **Space / ATTACK:** engage the nearest enemy until another order.
- **Q, E, R, F:** hold to aim and release to cast the four abilities; the ability bar shows unlock levels, mana, and cooldowns.
- **B:** channel a recall to the healing well. Moving or taking damage interrupts it.
- **I / SHOP:** open the item store and view personal gold, prices, prerequisites, and active upgrades. Escape or the close button returns to combat.
- **Touch:** left thumbstick or ground tap to move, tap an enemy to focus it, ATTACK to engage the nearest enemy, and ability buttons to aim and release a skill shot.
- **Hold/drag the minimap:** temporarily inspect another location; release to return to the hero. This does not move the hero or change spell aim.
- **Controls:** adjust touch-button size, height, inset, and handedness; preferences are saved on this browser. The normal camera follows your hero and looks slightly ahead while aiming a selected ability.
- Use the skill-tree, pause, beast-lane, and team-command controls on the HUD.

The difficulty selector changes bot decisions, teamwork, reaction timing, aim, and retreat behavior. This is a solo match against bots; it does not require a multiplayer server.

### Resume and mastery

An unfinished match saves locally every ten seconds and when pausing or leaving the page. **Resume match** restores the simulation in a paused state, including items, talents, cooldowns, projectiles, summons, warnings, bot plans, and random state. Starting over asks before replacing a different saved match. Invalid or incompatible saves offer a recovery path. Autosaves check the saved match ID and refuse to overwrite a different match unless replacement is confirmed.

The results screen records hero and structure damage, effective healing, absorbed shielding, personal gold, lane/jungle last hits, assists, objectives, shutdowns, and streaks. Match badges highlight those contributions. Completing a match earns per-hero mastery that unlocks armor tints, trails, portrait treatments, and victory colors. Mastery never changes combat stats, and each completed match is credited once. Saves, settings, and mastery belong to the current browser/origin; they are not cloud-synced. When browser storage is unavailable, the game remains playable and reports the persistence limitation.

## Files and hosting

`data.mjs` defines heroes, talents, map geometry, difficulty settings, and progression rules. `items.mjs` defines the 30-item catalog, prerequisites, stat bonuses, passive descriptions, purchase validation, and pixel item icons. `engine.mjs` runs deterministic combat, individual rewards, item activation, and AI without browser dependencies; `hero-skills.mjs` implements the knight, assassin, and sword angel abilities. `progression.mjs` handles signature passives, late evolutions, item effects, and delayed impacts. `strategy.mjs` handles shared vision, role assignments, build plans, and match metrics. `save-state.mjs` validates and restores the simulation. `profile.mjs` handles local mastery, settings, recipe planning, and shop comparisons. `renderer.mjs` draws the battlefield, fog, warnings, animated pixel sprites, cosmetics, focus markers, and icons. `ui.mjs` connects the simulation to keyboard, pointer, touch, and local persistence; `style.css` styles the compact HUD and menus.

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

