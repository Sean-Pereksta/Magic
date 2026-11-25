# MOBA Mania

A self-contained solo 3v3 pixel MOBA at `/game/CatLanes.html`. Every match has one human hero, two allied bots, and three enemy bots. Choose among eight playable heroes: **Arc Rift**, **Red Dragon Warrior**, **Emerald Druid**, **Pink Angel**, **Dark Ranger**, **Purple Knight**, **Orange Assassin**, and **Blue Angel**. There is no mouse character in the roster.

## Play

Choose a hero and an AI difficulty, then start a match. Destroy outer and inner towers to open a route to the enemy crystal. Your own crystal, towers, and healing well need protection. Each lane sends three footmen and two archers per wave. Jungle camps award experience; neutral beasts appear in the river-side pits, and the team that delivers the killing blow summons a siege beast to attack a lane.

Waves begin after 4 seconds and repeat every 19 seconds. Jungle camps contain three creatures and respawn 44 seconds after the whole camp is cleared. The first river beast appears after 55 seconds; another spawn opportunity occurs every 100 seconds, alternating between the two pits without duplicating a living beast in a pit.

New river beasts gain a tier every 100 seconds of match time. Each additional tier adds 22% of base health and 16% of base attack damage. The captured lane attacker inherits the defeated beast’s tier, with 24% more base health and 17% more base attack damage per additional tier. Later captures therefore create stronger lane pushes as well as tougher jungle fights.

Abilities unlock at levels 1, 2, 4, and 6. Talent choices at levels 3, 5, 8, and 10 specialize each hero. Every tier offers three exclusive choices with small pixel icons: 32 active abilities and 96 talent options across the roster. Open the skill tree when its badge lights up. The solo simulation pauses while selecting a talent.

### Personal progression and the item shop

Heroes can reach **level 18**. Each next level requires `150 + 50 × (level − 1) + 3 × (level − 1)²` XP, so the opening level costs 150 XP and later levels progressively take longer. Reaching level 18 requires 13,838 XP. Health, mana, attack damage, and level-scaled abilities continue growing through the new cap; the existing skill and talent unlock levels remain the same.

XP belongs to the heroes who participate in a kill. The finishing hero receives the full XP reward; other allied heroes who damaged that target in the previous ten seconds receive half. Nearby teammates who did not contribute receive nothing, including when a tower or river beast falls. This applies to lane minions, jungle creatures, heroes, and objectives. The scoreboard's assist count records hero-kill assists.

**Gold comes only from your finishing blows.** Assists, nearby kills, time passing, and unowned allied minions or towers do not earn you gold. Your own projectiles, damage-over-time effects, and personal summons credit their kills to you. A captured river beast is a team lane attacker, so its subsequent kills do not passively pay its capturer. Finishing a target shows a gold-colored `+N GOLD` notice and updates your personal balance.

Open **SHOP** on desktop or mobile, or press **I**, to buy permanent match upgrades. Combat pauses while the shop is open. The three tiers contain ten items each:

| Tier | Purchase requirements |
| --- | --- |
| Basics | Gold only |
| Upgrades | Gold and one specified basic item |
| Advanced | Gold, one specified basic item, and one specified upgrade |

All 30 items have pixel icons, visible bonuses, prices, and named prerequisites. Purchases activate immediately and appear in the active inventory. Prerequisites stay active after an upgrade; displayed prices are the additional cost, and every item can be purchased once per match. There is no inventory-slot limit. Items improve health, attacks, ability power, cooldowns, and other supported combat stats. Cooldown reduction from items and talents shares a 50% cap. Items and unspent gold survive death and recall; a new match resets both. Bots earn and spend their own final-hit gold under the same purchase rules.

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

### Attack orders and tower pressure

Press **ATTACK** or **Space** to select the nearest living enemy, then automatically approach to your hero's melee or ranged attack distance and keep fighting. Automatic attacks keep checking the nearest valid enemy as units move. Enemy heroes, minions, neutral creatures, captured beasts, and exposed structures are valid targets. Click or tap an enemy to explicitly focus it, even when another enemy is closer; when it dies, the hero resumes nearest-target attacks. Protected inner towers/crystals are excluded until their prerequisites fall.

Releasing ATTACK does not stop the order. A new movement order, successful ability cast, recall, team command, or death clears the previous pursuit. Pressing ATTACK again chooses the nearest enemy anew. Ground clicks/taps and right clicks issue movement; keyboard and joystick movement take immediate priority. Moving the aim pointer alone never changes a basic-attack target or auto-aims an ability.

Towers and crystals fire visible homing bolts. They retain a valid target, usually engage an incoming wave first, and retaliate against enemy heroes who attack a hero they protect. Consecutive shots against one hero grow stronger; leaving range breaks the focus. Bots check the whole route and mobility-skill destination before entering hostile tower range. They escort healthy frontliners and wait for a wave, or commit to a brief finishing attack only when the health, defenders, distance, and likely escape justify it.

### Controls

- **WASD / arrow keys:** move.
- **Mouse:** aim spells. Click an enemy to focus it; click the ground or right click to move. **Space / ATTACK:** engage the nearest enemy until another order.
- **Q, E, R, F:** hold to aim and release to cast the four abilities; the ability bar shows unlock levels, mana, and cooldowns.
- **B:** channel a recall to the healing well. Moving or taking damage interrupts it.
- **I / SHOP:** open the item store and view personal gold, prices, prerequisites, and active upgrades. Escape or the close button returns to combat.
- **Touch:** left thumbstick or ground tap to move, tap an enemy to focus it, ATTACK to engage the nearest enemy, and ability buttons to aim and release a skill shot.
- Use the skill-tree, minimap, pause, and team-command controls on the HUD. The camera follows your hero.

The difficulty selector changes bot decisions, teamwork, reaction timing, aim, and retreat behavior. This is a solo match against bots; it does not require a multiplayer server.

## Files and hosting

`data.mjs` defines heroes, talents, map geometry, difficulty settings, and progression rules. `items.mjs` defines the 30-item catalog, prerequisites, stat bonuses, purchase validation, and pixel item icons. `engine.mjs` runs deterministic combat, individual rewards, item activation, and AI without browser dependencies; `hero-skills.mjs` implements the knight, assassin, and sword angel abilities. `renderer.mjs` draws the battlefield, animated pixel sprites, focus markers, and icons. `ui.mjs` connects the simulation to keyboard, pointer, and touch input; `style.css` styles the compact HUD and menus.

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
```

The browser suite uses the Playwright-managed Chromium installation by default; set `CHROMIUM_PATH` to use an installed compatible binary.

The mechanics suite checks combat behavior, level/talent restrictions, individual XP and gold attribution, progression through level 18, item recipes and real stat effects, structure protection, wave composition, neutral capture and tier growth, recall/respawn, coordinated bot pushes and defense, actual offline pack dependencies, and completed simulated matches. The browser suite checks desktop and phone layouts, hero selection, input, multi-touch aimed casting while holding attack, camera following, skill-tree and shop pause behavior, all three item tiers, purchase requirements, immediate activation, end/replay resets, and the offline pack's module dependencies. It injects its inspection hooks only into the test-served module; production code has no cheat or test global.
