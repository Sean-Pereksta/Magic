# Apes Together Strong

## Settlement economy and expeditions

Visit a main hut and press **B** to choose **Resource Expeditions**. Food and
lumber each have their own Local, Extended (1,600 paces), or Frontier (2,800
paces) range, plus a shared Balanced, Food First, or Lumber First priority.
The council lists active parties, available adults, and trip status. Workers
physically gather supplies and carry them home; changing priorities or
recruiting a carrier cannot create a remote delivery.

Operational gardens provide dependable food in poor terrain. Adult workers
staff workshops for a slow renewable timber income, while distant gathering
provides larger deliveries. Residents reassess real construction, food,
timber, and defense needs while keeping useful assignments stable. Affordable
commissions reserve their cost once and continue searching for a building
site if the nearby plots are occupied.

Warlord now unlocks permanently at **200 living apes**. Existing saves are
upgraded when loaded; previously acknowledged ceremonies stay acknowledged.
Military escalation thresholds remain separate from royal progression.

See [SETTLEMENT-ECONOMY.md](SETTLEMENT-ECONOMY.md) for implementation details,
validation, measured performance, and limitations.

## World, construction and navigation update

Vehicles and aircraft now use original directional artwork; settlements have
75 authored construction frames across 15 building/development strips. Wider
wood, stone, military and natural crossings share their geometry with navigation.
Q recruits nearby wild, idle, and settled apes, R recalls nearby/field apes, and T recalls field/all apes, with 600 ms
holds and matching touch controls. See [WORLD-UPDATE.md](WORLD-UPDATE.md) for
implementation, tests, measured performance and the remaining performance limits.
Apes and soldiers carry illustrated weapons, shields, tools and supplies with
facing-aware body occlusion. Scouts' sashes have distinct front and rear artwork.

## Illustrated artwork update

The playable standalone build now embeds original animated character and
environment sprite atlases. See [VISUAL-UPDATE.md](VISUAL-UPDATE.md) for the
implemented presentation systems, verification, performance and limits, and
[the sprite library guide](assets/visual/README.md) for source artwork, animation
metadata and exporting the complete game and sprite ZIPs.

## Reign progression, royal settlements and equipment

The total living population across followers and settlements earns two permanent
milestones: **King of the Jungle at 100** and **Warlord at 200**. A full-screen
illustrated ceremony freezes simulation and input until Continue. Each milestone
is acknowledged once and saved; jumping both thresholds presents both in order.
Losses never remove titles or unlocked equipment. The King wears a small first
crown, a taller royal crown, then a dark, horned war crown, including on the map.

The supplied Underpowered King plays before the first milestone, Ceremonial Tom
plays during the royal tier, and Primal Roar plays as Warlord. All three MP3s are
embedded in the standalone HTML. One audio element loads only the selected track;
music remains subject to master/music volume, mute, pause and tab visibility.
The complete standalone build, including the new artwork, is approximately 88 MB, without adding
network requests during play.

Visit a living main hut and press **B** (or Build) to commission expansion:

- **Royal lodge:** 160 timber + 120 village food; reinforces the main lodge and
  lays out a broader district with a second palisade ring.
- **Warlord citadel:** 360 timber + 280 food; reinforces the lodge further and
  lays out a wider district with three palisade rings.
- All affordable housing and facilities are available at every rank.
  Commissions have no building-count, queue, population or district-radius cap.
  Family huts, longhouses and canopy halls house 10, 24 and 40 residents.
  The existing 1,000-living-ape population cap remains.
- Nursery groves and rally groves add birth-rate bonuses; orchards produce
  food. Every completed building accelerates growth. Completed homes plus
  the lodge's six places set local capacity; the shared 1,000-ape cap remains.
- Spear towers, rapid spear batteries and great spear ballistas require real
  adult crews. Shafts travel, collide and deal damage on impact. Human soldiers
  shoot the nearest visible ape, building or ape-owned palisade. Rifle shots
  physically break wall sections, then retarget what is exposed behind them;
  intervening cover and apes stop bullets.

Lodge upgrades, paid projects, damaged buildings and surveyed ring locations
persist in saves. Existing homes stay where they were. Friendly palisades stay
passable for the King and every ape species through a short climb, crest and
jump animation. Human troops must still breach or use openings. Climbing does
not bypass trees, huts, water or unrelated fortress walls.

Visit a completed workshop and press **B** (or Workshop) to spend its village's
timber and food. A shared queue holds up to eight orders and equips only one
individual at a time. Apes physically approach the workshop; the King must stay
nearby while armor is fitted. Raids, destroyed workshops or missing recipients
pause work, and cancellation refunds only unused supplies.

- Individual capuchins receive visible throwing spears for 2.3× ranged damage.
- Individual gorillas receive wooden fist cuffs for 35% stronger strikes.
- Adult gibbons, chimpanzees and capuchins can carry torches for 65% more
  structural damage. Torches illuminate nearby ground and make the bearer easier
  to detect, while still respecting cover and weather visibility.
- Royal armor progresses through 240, 350 and 500 maximum health, with up to
  22% damage protection and 70% stronger King attacks. Fitting preserves the
  existing missing-health amount; repeating a purchase cannot heal the King.

Human response packages grow by 12% at the royal tier and 22% at Warlord, with
shorter dispatch intervals. Finite site reserves, vehicle inventories, regional
force budgets and existing warnings remain in effect. Projectile simulation,
tower acquisition, human structure targeting, torch lights and rendering caches
are bounded so these features do not add unbounded work as settlements expand.

Additional checks:

```sh
node --test game/apes-together-strong/tests/reign-progression.test.cjs game/apes-together-strong/tests/royal-settlements.test.cjs game/apes-together-strong/tests/equipment.test.cjs game/apes-together-strong/tests/palisade-traversal.test.cjs game/apes-together-strong/tests/settlement-combat.test.cjs
node game/apes-together-strong/tests/reign-browser.cjs
```

A solo, procedural horde survival game. The lobby registry launches
`/game/apes-together-strong.html` directly and offers that same self-contained
file as a download. Canvas artwork and Web Audio need no asset downloads,
accounts, multiplayer room, or server-side game state.

## Build and validation

Edit the modules and `shell.html` in this directory, then rebuild the checked-in
HTML. The build uses only Node's standard library.

```sh
npm run build:apes
node game/apes-together-strong/build.cjs --check
npm run test:apes
node --experimental-vm-modules --test lobby/tests/lobby-library.test.cjs
```

The optional browser check requires a local Playwright installation and its
Chromium browser. `CHROMIUM_PATH` can select an existing Chromium executable.
It exercises desktop and touch menus, movement, rescue, settlements, combat
rendering, settings, saved runs, and permadeath.

```sh
npm run test:apes:browser
```

## Design

- **Species orders and sieges:** select gorillas, orangutans, chimpanzees,
  gibbons, capuchins or mandrills with `1`–`6`; `0` selects all followers.
  Shift adds/removes species. `I` or Alt-click picks an individual ape.
  Click/tap successive ground points or visible targets, then Enter / the
  triangle button sends the route. Shift+Enter / `+` appends instead of
  replacing it. Routes contain up to 16 steps; Backspace undoes a draft
  point. Each ape must finish its own waypoint, while 85% arrival releases
  the main group. Blocked steps offer retry and skip. `E` immediately charges.
  `O` or the visible Cancel button discards unissued waypoints and returns to King control. Issued orders continue; Escape always pauses. Selected
  army taps never strike with the King. Drag to pan; wheel or pinch to zoom.
- **Small command dock:** PC commands have a 20px canvas picture, a visible
  key and a 30×34px button. Touch buttons are at least 44px tall; expand the
  command dock to see all commands and four tactical presets. Ctrl+Shift+1–4
  saves a species preset, Ctrl+1–4 recalls it. On touch, hold A–D to save and
  tap to recall. `H` plans a shield advance, `J` sabotage, `K` a climb, `L`
  Rally Roar, `U` log preparation, `Y` regroup and `N` defend.
- **Fortress layers:** new forward bases, armored depots and regional commands
  have guarded gates, interior winches, designated vines/scaffolds, stairs
  and walkable elevated walls. Five procedural interior families vary the
  strongpoints. Regional commands have a separately gated inner compound.
  Gibbons, capuchins and chimpanzees can infiltrate through designated access;
  heavy species wait for gates or breach them. Destroying a winch immediately
  opens its gate and invalidates movement/visibility caches. Ground melee
  cannot hit elevated defenders; wall guards ascend stairs, patrol and shoot
  down. Reinforcements spend finite personnel reserves and stop after
  barracks/radio sabotage. Existing explored layouts retain their damage
  and geometry; new features appear in newly generated fortresses.
- **Species balance:** adult base HP is gorilla 260, orangutan 205, mandrill
  140, chimpanzee 120, gibbon 95 and capuchin 90. King and child health remain
  unchanged; old adult saves preserve their health percentage. Capuchins
  throw for 6 base damage at 168-unit range every 1.25 seconds, with a
  0.42-second windup, release/follow-through animation and cover-aware arc.
  Throws are weaker against structures and do not hurt armored vehicles.
  Mandrill Rally Roar boosts nearby movement and melee speed by 15% for six
  seconds, has a shared 30-second cooldown and makes audible noise.
- **Logs and military intelligence:** gorilla/orangutan logs have 180/145 HP,
  absorb frontal bullets, give partial blast protection, splinter and never
  regenerate in combat. Prepare them from village timber or fallen trees.
  Shield advances place carriers in front at a shared pace. Humans share
  local sightings and delayed radio reports with position, direction,
  estimated group size and confidence; stale reports decay instead of
  tracking unseen apes. Working towers can trigger exterior alarms directly.
  Reports, tower checks, navigation, projectiles and caches have fixed limits.

Siege-specific simulation and browser checks:

```sh
node --test game/apes-together-strong/tests/siege.test.cjs
node game/apes-together-strong/tests/siege-browser.cjs
```

- **Navigation:** apes and humans share a 28-unit clearance grid, bounded A*
  searches, cached routes, segment smoothing, and collision-aware sliding.
  Waypoints only advance when the next segment is clear. Destroyed obstacles
  invalidate routes. Broad tree canopies provide concealment; smaller solid
  trunks leave room for the horde. Old trail steering no longer pulls followers
  backward. Regression tests include enclosed walls, bridges, 100 mixed units
  in dense trees, and 100 followers in an actual generated forest.
- **Horde spacing:** nearby active apes gently make room even when holding,
  attacking, or already close to their destination. Exact overlaps separate in
  stable directions, children use smaller spacing, and followers leave room
  around the crown. Larger hordes keep wider, loose following offsets. Local
  spacing respects obstacles and water, preserves attacks and commands, and
  leaves distant sleeping settlements alone. Tests cover clumps, corridors,
  different update rates, and a moving 100-ape horde.
- **World:** frontier, watershed, orchard, highland, research, and military
  districts influence landscape and development. Major hubs occupy spaced dry
  slots between river bands. Smaller camps, checkpoints, and research outposts
  connect to roads. Compounds have front gates, service openings, and seeded
  layout variants. Distant garrisons retain their wounds and surviving members
  in the save without exhausting the active force budget.
- **Combat:** six ape attack poses, five fall variants, recoil, interruption,
  fading bodies, dropped weapons, and a falling king. Fourteen human roles include
  trackers, radio officers, shield guards, marksmen, grenadiers, medics, assault
  scouts, and heavy gunners. Marksmen lock a warned point before firing; grenade
  circles give 1.8 seconds to escape. Shield guards reward flanking or a swarm.
  Officers illuminate remembered positions with flares. Vehicles, searchlights,
  alarms, finite reinforcements, and helicopters remain part of the hunt.
- **Balance:** untrained adults have 120 health (roughly 2–4 ordinary hits on
  Survival), scouts 150, and young 60. Marksmen enter tier 3+ forces and retain
  warned, lethal shots. Fresh runs build from a two-ape opening cage and a
  six-ape hunter camp. Transport cages hold 2–4, hunter camps 4–7, research
  outposts 8–14, checkpoints 8–16, prisons 18–30, detention camps 42–72,
  experimental camps 80–125, forward bases 32–60, armored bases 60–110,
  and regional command bases 160–260. Larger rewards require later, more
  defended installations. Existing saved captive stocks remain unchanged.
  Military liberation awards 30 food per site tier once. Rescue/population
  threat contributions remain low enough to allow an early tribe to grow.
- **Settlements:** food patches, water, fertility, nearby timber, housing, and
  local human activity affect a camp. Adults forage, build, or guard. Work
  priorities favor growth, food, or defense. Construction produces shelters,
  gardens, lodges, stores, and defenses; occupied villages keep raising young
  while their completed homes have room. Visit to deliver food or recruit adults. Raids consume defenses
  and supplies, and scouts give advance warnings.
  Lodge targets advance one level per 12 inhabitants, reaching level 10 at 108;
  upgrades still require building work and timber. Construction and timber
  gathering are faster, shelters add ten housing, and gardens produce more food.
  Families contribute at least one work unit per second (one per eight
  available adults), with a birth every 30 units. Completed buildings and
  lodge upgrades increase that rate, as do nursery/rally and sanctuary bonuses.
  Terrain ratings, low supplies and recovering safety no longer pause births;
  food still feeds and heals residents. Housing and the shared 1,000-ape cap
  bound births, and empty villages need residents before growth resumes. Young mature after 35 seconds, including while far from the king.
  Builders add persistent huts around the lodge, expanding the actual footprint.
  Each hut has 100 health and provides housing; human fire and explosives leave
  ruins and remove that housing. Repairs and rebuilding consume work and timber.
- **Presentation:** illustrated night forest, pines and hanging wetland trees,
  orchard trees, reeds, farm rows, landscape details, construction and garden
  visuals, region HUD, map management, and a seeded menu. Procedural audio adds
  footsteps, river ambience, varied impacts and falls, and distinct threat cues.
  Low detail, reduced motion, mute, and volume controls are available.

## Primate artwork

The horde includes gorillas, chimpanzees, orangutans, gibbons, mandrills and
capuchins. Each has its own proportions, fur palette, face and silhouette:
broad silverback shoulders, chimpanzee ears, shaggy orange arms and cheek
flanges, long gibbon arms, colorful mandrill muzzles, and curled capuchin tails.
The King remains a crowned silverback. Three coat shades per species add variety
without changing health, speed, attack strength or collision size.

Species and coat shade persist through growth, scouting, settlement work,
combat, blast flight, recovery and fallen bodies. Old saves receive stable
appearances derived from their seed and actor IDs without consuming simulation
randomness or changing existing wounds, ages, proportions or fur records.
The same artwork supports cage previews, distant detail and reduced motion;
species-aware sprite caches keep large hordes bounded.

The optional real-canvas gallery checks species visibility, animation paths,
sprite bounds and cache limits on desktop and mobile:

```sh
node game/apes-together-strong/tests/primate-design-browser.cjs
```

## Village commissions and ape tactics

Stand within 90 world units of a living village's main hut and press **B** or
choose **Build**. The council pauses simulation, audio, movement and pending
touch orders while showing local supplies, queued projects, construction
progress and completed commissions. Selecting a structure pays its timber and
food exactly once. Affordable orders queue immediately, even during attacks.
Worker construction and training pause during attacks and resume afterward.

| Commission | Timber | Village food | Building work |
| --- | ---: | ---: | ---: |
| Spear tower | 26 | 12 | 54 |
| Training ground | 24 | 20 | 48 |
| Palisade section | 12 | 0 | 35 |
| Family hut | 8 | 0 | 18 |
| Garden | 10 | 0 | 22 |
| Food store | 16 | 6 | 35 |
| Workshop | 20 | 8 | 28 |

Build time depends on crew arrival, clearing and available builders. There is
no commission queue, building-count, resident or rank requirement. A settlement
with one available adult assigns a builder. Crews finish their material trip
before walking to the plot, including distant outer rings.

Plots fill concentric rings around the main hut, searching farther outward
without a settlement-radius cap. Trees and rocks become clearing work; water
and existing buildings send the search to the next position. Surveys process
at most 96 candidates per call. If terrain has not loaded or more searching is
needed, the paid order waits for a plot and resumes without another charge,
including after a save/load. Additional palisade orders create further rings.
Lodge upgrades are unique; repeat purchases of the same upgrade are prevented.
The active worker and combat budgets remain bounded while every queued project
and staffed facility gets a turn. Villages still build automatically.

Completed spear towers need a living adult at their guard station. They aim
at enemy soldiers and vehicles within 900 units (batteries: 1,000; ballistas:
1,150), beyond infantry guns and the longest current tank weapon range of 780.
Visible shafts remain in flight to their aim point; swept collisions and
height-aware cover determine hits. Towers can fire over low palisades. Damage does
not occur on the launch frame. Towers, trainees and in-flight spears survive
saves; destroyed or abandoned towers cannot fire. Each staffed training ground
teaches up to three adult residents at a time. Levels one through three need
45, 90 and 135 safe practice ticks and cost three, five and seven village food.
Each permanent level adds 12 maximum health and 8% strike damage; recruitment
and save/reload retain it without healing existing wounds for free.

Every ape receives one persistent, seeded personality without consuming new
simulation randomness. Bold apes prefer frontline troops and armor; guardians
protect the crown and home; saboteurs favor radios, alarms, depots and barriers;
rescuers seek captive cages; skirmishers prioritize snipers, officers and medics.
**X** sends a spread charge toward the pointer, with individual attack lanes.
**T** sends apes against nearby foes in every direction. Existing charge,
recall and hold commands continue working. Touch preserves whether **Charge**
or **Spread charge** was chosen until the ground is tapped.

Adult capuchins, gibbons and chimpanzees can climb the two lower wall tiers.
Training lets other adults climb basic walls. Young stay on the ground;
reinforced and fortress walls require a gate or breach. King and follower
strikes damage contacted enemy walls, including during climbing, so a tribe
can open passages for apes that cannot cross them.

Gameplay no longer prints command acknowledgements, activity labels, damage
numbers, cage instructions or objective hints over the world. Shape, color,
movement, icons and health bars retain feedback. Village attack or approaching
raid warnings remain visible. Controls, explanations, commissioning choices
and save/import errors stay in menus; HUD counters remain readable.

The optional council browser check covers desktop and touch commands, quiet
gameplay, retained attack warnings, paused keyboard access, exact costs,
real construction completion, remote-order denial and saved records:

```sh
node game/apes-together-strong/tests/village-ui-browser.cjs
```

## Military escalation

Active followers guarantee response stages at 25, 60, 120 and 220. The original
campaign threat still matters; settled residents do not count toward the horde
floor. At 200 followers a one-time military mobilization unlocks tanks; at 300
the director coordinates larger operations across separate approach bearings.
Reports expire, targets are remembered positions, and breaking contact still
works. Known settlements receive named army warnings before a column departs.

Responses consume finite site personnel, vehicle inventory and armored capacity.
The active response budget is weighted: infantry 1, elites 1.5, heavy gunners 2,
trucks/jeeps/command vehicles 4, APCs 7, IFVs 9, tanks 12 and aircraft 10. Mortar
teams cost 3. Cargo
reserves its future soldiers' budget before they deploy. Destroying radios cuts
coordination; depots stop armor; fuel cuts vehicles and aircraft; barracks reduce
replacement troops. Troop trucks and APCs unload prepaid passengers in sequence,
and ambushing an occupied transport removes its undeployed reinforcements.

Military riflemen, rangers, heavy assault gunners, engineers and squad leaders
join the original roles. Squads share a reported objective and role formations:
marksmen and medics behind riflemen, rangers on the flanks, infantry screening
armor. Confirmed concentrations of 40 prompt suppression and support reports;
80 prompt a bounded fallback for unsupported squads; supported squads hold and
suppress. Small patrols shadow an army until combined forces can counterattack.
Heavy weapons choose visible clusters, grenadiers avoid overlapping warnings,
and mortar crews need fresh contact, communications and safe firing distance.
Mortars warn for about two seconds, stagger shots and reload in 10–14 seconds.
Killing a leader briefly disrupts coordination. Engineers need 3.5 uninterrupted
seconds away from apes to build temporary barricades or field lights.

Standard tanks have 1,100 HP, a separate slowly rotating turret, and a cannon that commits
to a point for a 1.8-second warning before a shell travels and explodes. Front,
side and rear melee deal 20%, 50% and 100% damage. Five nearby apes slow rotation,
eight disrupt the machine gun, twelve impair the turret, and sixteen overrun the
vehicle, stopping cannon fire and exposing components. Damaged tracks immobilize,
weapon damage increases reload and eventually disables guns, and rear engine
damage produces smoke and eventual destruction. IFVs fire smaller explosive
volleys; APCs provide covering fire while dismounting troops. Forest canopy,
solid trees, buildings, rocks and unbridged rivers constrain armor. Forest cover
also reduces armed scouts and gunships, which orbit reported battle areas, fire
limited bursts and periodically reposition or leave.

Fresh forward bases, armored depots and rare regional command bases use larger
rectangular compounds, layered barricade rings, broad guarded gates, service
openings, multiple floodlights, armor parking and 30–120 defenders. Stronger
inner walls and lower outer sections distinguish each installation's defenses.
Command installations contain major captivity areas. Their placement preserves
regional highways and dry access lanes between river bands. Temporary
checkpoints assemble at road approaches after
their defenders travel from a source; occasional supply columns move between
installations. Distant response corridors load through the existing three-stage
streaming budget and heavy vehicles retain clearance and road preference.

Version-one saves keep the same storage key. Missing military fields derive on
load; existing wounds, spent inventory, passengers, squad reports and one-time
mobilization persist. Newly generated regions receive the new base layouts.

Scenario **L** retains 300 apes, 100 humans, two tanks, three APC/IFVs, three
trucks/jeeps, two helicopters, an alarm, grenades and cannon activity. It runs
alongside A–F with the same AI, visibility, navigation and generation ceilings.
`npm run bench:apes:browser -- --scenario L --frames 180` measures a real canvas;
hardware timing is diagnostic and never a universal frame-rate guarantee.

## Controls and persistence

WASD/arrows move; Shift sprints; Ctrl sneaks; click/Space attacks. Q recruits
nearby wild, idle, and settled apes within 340 world units; active field orders stay intact. R taps recall nearby field apes; holding R for 600 ms
recalls every field ape. T taps recall every field ape; holding T for 600 ms
also mobilizes settlement residents and defenders. E charges toward the pointer,
X spreads the charge, F holds, Z settles nearby followers, Shift+Z settles all, and C
assigns scouts. B opens the council beside a main hut. M/Tab opens the map and
settlement controls; Escape pauses or closes a menu. Touch uses a movement
stick, strike button, command menu and nearby Build button. The R/T command
buttons support the same tap/hold gestures and show hold progress. See
[Horde commands and crossing navigation](HORDE-COMMANDS.md) for exact eligibility,
assignment preservation, and validation details.

The original `ats-crown-save-v1` storage key and JSON save format are retained.
Older tree collision and settlement records migrate on load. Navigation caches
are rebuilt; they are not serialized. Autosave runs every 15 seconds, with
manual save and JSON export/import in Pause. The king's death removes the
living checkpoint and records a legacy score.

The existing balance-revision-two actor migration still preserves relative
health and family progress. The slower rescue pacing applies to fresh world
generation; saved captive counts and past rescue statistics retain their
history. Personality and training fields migrate without replacing existing
species, coats, wounds, ages or settlement membership.

The new regional layout applies to newly generated chunks. Already explored
areas in imported saves keep their existing structures.

## Performance and stress checks

The simulation keeps full detail around the king, active combat, attacked
settlements, alarms and recent rescues. Nearby actors update at 15 Hz with
visual movement interpolation; distant actors advance strategic state at
2 Hz. Family growth, searching and raid journeys continue while distant.
An exceptionally trapped, separated follower can queue a low-priority recovery
corridor. Cohorts share routes while individuals retain local steering,
collision clearance and independent combat. Separation considers up to eight
nearby neighbors rather than every member of a dense clump.

Each fixed simulation step permits 32 AI think operations (at most 20 from
apes, reserving capacity for humans), 96 perception visibility tests, three
new A* searches, 192 A* node expansions, and three small world-generation
stages. A search's preparation, expansion and smoothing resume across steps;
the navigation queue is capped at 384 requests. Settlement economy ticks run
at 1 Hz with staggered deadlines and at most two settlements processed per
step. Collision uses cached spatial data and water checks; queued generation
prioritizes the king's movement direction. Distant chunks retain persistent
state while their detailed objects can sleep, and populated settlements and
active combat pin their collision areas.

Rendering culls actors, scenery, projectiles, corpses and effects with padded
bounds. Terrain chunks use at most eight million cached pixels, with two
cache builds per draw; trees, rocks, food, characters, light beams, glows and
static corpses reuse bounded sprite caches. Lighting geometry is staggered
with at most 96 rendering visibility tests per draw; illumination used by
gameplay remains independent. Bullet collision sweeps the complete traveled
segment and respects walls, trunks and water. Bullet, effect and noise pools
reuse expired objects. The effect budget prioritizes nearby combat, commands
and warnings while merging redundant distant impacts.

The fixed timestep is 1/60 second. A rendering frame runs at most three
simulation steps and discards excessive catch-up time after a stall. A hidden
tab stops simulation, rendering and audio updates and pauses the run. Sustained
slow frames progressively reduce distant visual work; detail restores gradually
after recovery. Close combat, controls, warning cues and spotlight mechanics
retain their gameplay behavior.

Press **F3** or add `?perf` to the game URL to reveal the development monitor.
It reports frame, simulation, rendering and navigation time; active and
simulated actors; visible actors; navigation work and cache hits; projectiles,
effects, slow frames and the current quality level. Developers can also read
`ATS.performance` or call `ATS.debugPerformance(true)` in the browser console.

```sh
npm run bench:apes -- --frames 120 --output simulation.json
npm run bench:apes -- --scenario B --frames 120 --profile
npm run bench:apes -- --scenario D --frames 1800 --output soak.json
npm run bench:apes:browser -- --frames 120 --output browser.json
```

The browser benchmark needs Playwright and Chromium, like the smoke test;
`CHROMIUM_PATH` selects an installed browser. `QA_ARTIFACT_DIR` captures all
selected scenes as PNGs. Both benchmarks accept `--source <module-directory>` for
comparison with a separate baseline checkout, and `--scenario A`, `D`, or
`ABCDEF` selects scenes. The default length is 360 steps. `--profile` wraps
simulation methods with inclusive timing, so nested method totals overlap.

All twelve fixtures use the same world seed and reset actor randomness separately.
A/B retain 100/200 followers in dense generated woodland; C runs 150 apes
against 80 humans; D runs 200 apes against 150 humans with four vehicles,
two helicopters and an active alarm; E includes 180 settlement residents,
100 raiders and two vehicles. Combat actors receive high health only inside
the fixture so the workload stays populated while ordinary damage, weapons,
roles and alarms execute. F advances the king one chunk every half-second
to deliberately saturate streaming and distant navigation. This extreme
synthetic exploration is separate from ordinary player movement.

Representative development measurements on October 7, 2026 used Windows,
Chrome 154 in headless mode, a 1440×900 canvas at DPR 1, full visuals and
120 steps per scene. The browser benchmark measures one simulation step plus
one real canvas draw per animation frame, with automatic quality reduction
disabled. The baseline was captured before the optimization pass.

| Scene | Browser work p95 before | Browser work p95 after | After max | After mean animation-frame interval |
| --- | ---: | ---: | ---: | ---: |
| A: 100 forest followers | 39.7 ms | 8.6 ms | 38.4 ms | 21.4 ms |
| B: 200 forest followers | 62.8 ms | 11.8 ms | 14.8 ms | 17.8 ms |
| C: 150 vs 80 | 21.6 ms | 12.0 ms | 22.1 ms | 20.3 ms |
| D: 200 vs 150, vehicles, alarms | 26.8 ms | 14.1 ms | 25.5 ms | 26.3 ms |
| E: settlement raid | 31.0 ms | 14.4 ms | 23.6 ms | 24.2 ms |
| F: rapid streaming | 530.1 ms | 7.4 ms | 9.4 ms | 17.5 ms |

Canvas work time excludes browser compositing, scheduling and display latency;
the animation-frame intervals are reported separately. These measurements
show lower recurring work and removal of the severe exploration stalls, and
do not establish universal 60 FPS on every browser or device. The cold first
forest frame remains the largest measured optimized canvas-work spike.

The separate Node VM simulation benchmark measured p95 times of
226/378/85/113/112/4263 ms before versus approximately
13/18/16/22/14/9 ms after for A–F. VM timing differs substantially from native
browser execution and must not be converted into a browser FPS claim.
In the instrumented 200-ape forest, blocked queries fell from about
1.93 million to 50,810 over 120 steps, and A* starts fell from 185 to 14.

A 30-second simulation-only D soak retained all 200 apes and 150 humans.
Mean simulation work was 10.2 ms, p95 13.6 ms and maximum 48.1 ms; the
first and last 60-step means were 16.6 and 10.2 ms. Peak counts were 52
projectiles, 214 effects and 67 queued routes. Work stayed within three
A* starts, 192 expansion steps, 32 AI thinks and 96 perception tests per tick.
This verifies the sustained fixture without implying an indefinite soak.

The ordinary `test:apes` suite and existing GitHub workflow include the A–L
operation-budget checks, retained populations, real combat damage, distant
growth and wake-up, remote vehicle combat, fair human perception, swept
projectiles, pool reuse, quality recovery and saved strategic state. The
browser smoke test also checks hidden-tab pause, the F3 monitor and a real
250 ms main-thread stall. Hardware-specific maximum tick gates are optional
through `ATS_MAX_TICK_MS`; portable tests enforce operation ceilings instead
of an unreliable universal wall-clock threshold.

## Relentless war and settlement expansion

`MAX_APE_POPULATION` in `world.js` is the authoritative living population
ceiling. Followers, residents, scouts and young all count. Opening a cage at
985 with 40 captives recruits 15 and preserves the other 25 on the opened cage
and site, including across saves. Returning within 350 units when there is
room recruits the remainder. Rescue statistics increase only for recruited
apes. Families pause at capacity with at most one completed birth of progress;
losing an ape cannot trigger a backlog of hundreds of births. Existing saves
retain their campaigns, supplies, wounds and settlement housing.

Tier 5 remains the visible maximum. Internal intensity counts the whole living
civilization, including settlement residents, and continues scaling toward 1,000:

| Living population | Campaign | Budget from | Soldiers from | Tanks | Armored support | Concurrent operations |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| Below 220 | Early response | 250 | 220 | 3 | 4 | 1 |
| 220–299 | Military Mobilization | 340 | 220 | 3 | 4 | 2 |
| 300–449 | Major War | 500 | 320 | 6 | 9 | 3 |
| 450–649 | Regional War | 950 | 500 | 10 | 16 | 5 |
| 650–849 | Emergency Mobilization | 1,450 | 700 | 15 | 24 | 7 |
| 850–1,000 | Total Regional Campaign | 1,950 | 900 | 22 | 34 | 9 |

Budget and soldier capacity rise within the later bands; at 1,000 apes they
reach 2,250 and 1,000. Vehicle figures are regional ceilings and armored support
counts APCs, IFVs and armored patrols together. Director and reinforcement
intervals shorten as the campaign grows. Reevaluation can redirect field
formations while settlement, interception, blockade and reconnaissance
objectives remain independent. Infantry and cargo both count toward capacity.

Regional War introduces 150-HP assault infantry and 1,500-HP Veteran tanks.
At 650 apes, accurate 110-HP commandos, 1,900-HP Siege tanks and 1,050-HP
Sentinel IFVs join the formations. At 850, 180-HP juggernaut gunners and
2,400-HP Ironclad tanks enter service. Juggernauts carry limited body armor
that fails when six apes surround them. Larger tanks carry stronger frontal
armor, longer-range guns and larger warned explosions; their rear engines,
tracks and weapons remain vulnerable to the same swarm mechanics. Each model
has distinct visible equipment, armor, paint and weapon details.

Actual dispatched columns grow too: ordinary late-war operations can commit
110–300 soldiers, and major offensives can commit 200–380, supplied by several
installations. These are limits, subject to remaining personnel, chassis,
armor supplies and the weighted regional budget. Elite roles and upgraded
vehicles consume their full cost. Transport passenger roles are committed
when troops embark, saved with the carrier and charged before deployment.

Confirmed contact maintains a saved pursuit operation with position, movement
estimate, contact time, roads and known settlements. Intercepts project these
observations rather than reading the king's current position. Twenty-five
seconds of sustained 150+ contact requests reinforcements from real reserves.
Independent field and regional channels can hunt the army while assaulting a
settlement, with a resource share protected for known settlements. Rare major
offensives draw infantry, paired tanks or platoons, armored support and aircraft
from multiple installations. Coordinated approach bearings retain an imperfect
escape sector. Support, killed leaders, density drops and recalled charges
affect fallback, shadowing, firing lines and counterattacks.

The **Find Settlement** button or **V** toggles one home-direction icon for
every founded settlement, including empty bases after their residents join
the horde. Destinations project onto all four screen edges; a nearby base
receives a home marker. Quiet markers use icons, with village names and
warning words reserved for attacks. Nearby markers separate, attack status
changes their color, and the layout handles
camera motion, zoom, desktop and touch. Huts persist as individual plots with
health, damage bars and ruins. Human siege fire requires nearby line of sight;
grenades, shells, mortars and airstrikes can destroy homes. Destroyed homes
remove housing immediately, and builders repair or reconstruct with paid work.
New construction searches outward in rings around the lodge and clears natural obstacles.

Recovery routes are shared by coarse origin/goal cohorts, including distant
followers. Mid-distance separation runs with their 15 Hz simulation while
nearby apes stay immediate. Per-step limits remain 32 AI operations, 96 LOS
tests, three A* starts and 192 expansions. Heavy vehicles and aircraft receive
reserved thinking/visibility work, and rotating processing prevents large
infantry lists from starving cannons. Audio reserves command/warning voices
while distant war ambience uses bounded sampling.

G/H exercise 500/750 moving forest followers. I preserves 1,000 actual apes:
650 following and 350 in distant settlements. J adds 500 apes, 250 humans,
four tanks, six armored vehicles, two helicopters and live grenades/mortars.
K uses 750 apes, 320 humans, six tanks, eight armored vehicles, three helicopters
and explosives. Both wars verify real shots, cannon telegraphs and traveling
shells within the operation budgets. Synthetic combat health retains the
specified workload; it is not a casualty or difficulty calibration.

A separate ordinary-health regression sends 300 normally spaced apes into a
prepared formation of two tanks, an APC and 28 professional troops: 20
riflemen and two each of leaders, heavy gunners, grenadiers and medics. The
king follows the army and renews Charge when it reaches its old destination.
The isolated farmland battle verifies a costly victory with the king surviving.
Reinforcements and terrain cover are disabled to isolate that formation;
unit health, damage, firing rates and
reloads retain normal gameplay values. Tightly packed or unattended charges
can suffer greater losses.

Supported firing lines retain their position and facing as the horde closes,
including through saves. IFV bursts retain their original warned location;
each new location receives the full tell. Cannon, grenade, mortar and airstrike
warnings use the full projected blast radius at every zoom and detail level.

Pre-update October 7, 2026 diagnostic measurements used native headless Chrome 154,
1440×900 at DPR 1, 180 frames and full visual detail:

| Scenario | Mean simulation | Mean render | Total work p95 |
| --- | ---: | ---: | ---: |
| G: 500 followers | 12.09 ms | 6.53 ms | 25.5 ms |
| H: 750 followers | 19.70 ms | 8.59 ms | 39.5 ms |
| I: 1,000 mixed apes | 17.81 ms | 7.24 ms | 32.2 ms |
| J: large battle | 21.40 ms | 21.16 ms | 55.8 ms |
| K: extreme war | 32.37 ms | 28.17 ms | 94.6 ms |

Extreme battles remain expensive. These short fixtures establish population
retention, warning behavior and bounded work, not a universal 60 FPS guarantee.
Normal gameplay reduces visual detail under sustained load; danger warnings
and commands remain active. The browser tests cover the settlement finder,
construction, visible destruction and touch layout in addition to save/reload
and the existing gameplay checks.

```sh
node --test game/apes-together-strong/tests/*.test.cjs
node game/apes-together-strong/tests/settlement-expansion-browser.cjs
npm run bench:apes:browser -- --scenario GHIJK --frames 180
```

## Living kingdoms and prepared campaigns

Fresh settlements start with a small communal shelter. Population, lodge level
and completed buildings set a target footprint of roughly 100 units at ten
residents, 250 at 100, 450 at 400, and 550–650 for major towns. Development
claims that space gradually. Shared worker projects clear individual trunks
before placing structures, yield timber exactly once, and preserve shade and
perimeter trees. Nearby projects require builders to reach their work site and
carry material. Distant economy uses the same persistent projects in aggregate.

Each new hut houses ten apes, with ground, posts, frame and roof stages before
occupancy. Developed 500-resident towns can support 50 or more distinct homes,
connected paths, gardens, stores, work shelters, cooking areas and a level-ten
lodge. Worker cohorts use resource and community zones; young remain near the
core. Danger recalls foragers, shelters young, gathers guards at entrances and
redirects builders to repairs. Lookouts extend warning range. Large settlements
develop irregular perimeter defenses with openings and weak sections.

Human platoons coordinate two to four squads with front, heavy-weapon and
support lines. Engineers build basic 300-HP sections in four to six seconds and
600-HP sections in seven. Damage, fallback and apes at the construction site
interrupt the job. Chokepoints and staging areas have separated sections with
firing gaps, and infantry hold behind cover. Bullets clear low field cover;
apes climb eligible low walls or physically break stronger human sections.
Apes vault their own barriers while
infantry climb or breach slowly, engineers dismantle faster, and tanks can
crush only designated weak ape sections. APCs unload before contact and armor
supports infantry from cleared approaches.

Fresh generated compound walls have four persistent strength and height levels:

| Wall tier | Health | Height | Adult climbing |
| --- | ---: | ---: | --- |
| 1 · Low timber | 240 | 28 | Agile species or trained adults |
| 2 · Reinforced palisade | 620 | 45 | Adult capuchins, gibbons and chimpanzees |
| 3 · Stone rampart | 1,400 | 68 | Breach or gate required |
| 4 · Armored rampart | 2,800 | 95 | Breach or gate required |

Gates add 15% health. Old explored walls preserve their existing health and
geometry. Wall strikes, climb progress and persistent breaches survive saves;
new wall visuals use height, materials, buttresses and battlements to make
the stronger tiers recognizable without floating labels.

Infantry prioritizes firing at visible apes inside its weapon range. Short
repositioning and close backsteps are capped at 16 units over 0.28 seconds,
with 2.4 seconds between steps, and pause for a ready shot or recoil. Soldiers
keep facing the enemy through those moves, preserving their perception cone.
Patrols shadowing a large horde also fire from a stable line instead of retreating
beyond rifle range. Coordinated fallback and regroup orders may cover more
ground toward squad support or prepared cover; fallback commits to a fixed
position, continues firing when possible, and ends on arrival or after roughly
eight seconds.
Tanks also commit to a fixed reverse-support point under pressure, then stop
instead of continuously moving their retreat destination farther away.

Settlement operations progress through reconnaissance, approach, deployment,
engineering and assault. Their target and phase survive saves, as do platoons,
engineer progress, huts, timber work, breaches and supplies. Destroying an army
does not erase pressure from intact installations: generated military sites
reorganize finite campaign reserves every 90 seconds. Destroyed barracks,
depots and fuel permanently remove those reserves' corresponding capacity.
Ruined homes, breached barricades, tree stumps and up to 128 abandoned vehicle
wrecks retain a bounded visual history of the war.

Grenades, tank shells, mortars and airstrikes now produce colored fireballs,
shockwaves, embers and rising smoke. Every exposed ape and fallen body within
the blast reacts, including bodies killed by the impact and a fatal hit on the
King. Falloff changes the impulse and height. Flight sweeps against terrain and
solid obstacles, bodies settle on safe ground, and survivors brace and stand
over a short recovery animation before moving or fighting again. Construction
also pauses while a worker is airborne or recovering. Flight and corpse state
survive saves. Immediate impact collision work is capped while all affected
actors receive their animation, and ordinary bounded actor updates finish the
motion. A dense regression covers 1,000 survivors and 320 fallen bodies.

New regression suites cover development, actual worker navigation, faction
traversal, construction interruption, strategic independence, phased assaults,
finite replenishment, save continuity and cached geometry. The optional living
kingdom browser gallery exercises a 500-resident town in desktop and mobile
views with 40–50+ completed homes and physical defenses:

```sh
node game/apes-together-strong/tests/living-kingdom-browser.cjs
node game/apes-together-strong/tests/blast-browser.cjs
```


## Arsenal and control polish

- Right-click a final point to dispatch the drafted route, ending in an area
  attack. Earlier waypoints are mandatory. The final group attacks visible
  troops, vehicles and enemy structures within 190 world units, and stays near
  the point when clear. `P` / Finish with attack offers the same action on touch.
  An area attack is terminal; Go replaces it with a new route, while Append
  reports why it cannot add stops after a terminal attack and preserves drafts.
- Route action buttons keep their DOM identity during simulation updates.
  Held keys cannot toggle species or duplicate orders; pointer capture, drag
  thresholds, multi-touch suppression, off-canvas release checks and blur
  cleanup prevent stale gestures. Browser modifier shortcuts stay available.
- Six species have distinct gait timing and limb poses. Gibbons bound with
  raised balancing arms; gorillas/chimps use crouched knuckle strides; orangutans
  take slow long-arm steps; capuchin tails counterbalance quick steps; mandrills
  use a compact quadrupedal gait. Climbers alternate their reaching hands.
  Sprite atlases use the same clocks as live poses; reduced-motion stays still.
- Four finite-budget infantry roles join the late military roster: breacher
  (450 population), forward observer (550), volley grenadier (700), rotary
  gunner (850). Existing squads, perception and reinforcement stocks still apply.
- New tank variants: Twinfang (550, three 68-damage shells), Thunderback (750,
  three staggered 90-damage mortar zones), Cyclone (900, a 12-damage rotary sweep).
  Each uses actual chassis inventory and weighted deployment budgets. Warnings
  commit to fixed positions; destroyed weapons and overrun cancel pending bursts.
- Newly generated military blueprints include Gatling Redoubts, Artillery
  Bastions and Iron Citadels. Gatling nests have limited frontal arcs, a one-second
  spin-up, a 1.2-second burst and cooldown. Mortar nests have a close-range blind
  spot. Destroy the linked power relay to interrupt and disable every connected
  emplacement. Existing explored sites, health, finite reserves and cooldowns
  survive loading without replenishment.
- `arsenal.js` contains weapon/blueprint rules; `arsenal-render.js` contains
  canvas art and warning cones. Perception shares the existing work budget,
  nearby nests cap at 12, rotary bullets cap at 320 and dormant bursts cannot
  accumulate delayed shots. Added behavior tests and real desktop/touch checks.

## Kingdom settlement strategy

Open the map with **M** or **Tab** to manage every settlement. Each row has
specialization, defense posture, defender requests, evacuation, a rally flag,
and a destination selector for supply groups. These orders save immediately;
travel and work resume when you close the overview.

| Specialization | Practical effect |
| --- | --- |
| Sanctuary | Family progress is 35% faster; occupied huts provide two extra beds; ordinary healing and wounded elite recovery are twice as fast. Completed housing and the 1,000-ape cap constrain births. |
| War camp | Completed training facilities work 60% faster. Up to three nearby residents also practice basic militia training each second, spending food when they earn a permanent level. More residents guard the village, and local incoming damage is reduced by 14%. |
| Supply village | Food harvest/garden yields rise 35%; genuinely felled local trees supply 50% more timber. Its carriers move 25% faster and can transport up to 60 food instead of 40. |
| Scout outpost | Reveals nearby map terrain and checks a wider area for approaching humans and vehicles every eight seconds, up to 1,200 world units. Warning messages are throttled. |
| Forge / workshop | Building crews work 30% faster. Newly completed palisades have 25% more health. A safe village of at least eight residents can craft one gear kit and one siege-material bundle every 45 seconds for 12 timber and 6 food, storing up to six of each. |

The General settlement focus preserves the old economy. The existing food or
fortification priority still adjusts job allocation. Fortified defense assigns
more guards and reduces local incoming damage another 12%; mobile reserve moves
guards and scouts 20% faster but takes 6% more local damage. Stationed, recovered
champions and veterans add their relevant support effects only while physically
at home; their shared support multipliers cap at 1.75.

**Request defenders** sends up to eight available adults from the nearest safe
settlement with a spare workforce; it creates no new apes. **Evacuate
noncombatants** sends up to twelve children, wounded residents and civilian
workers to a safe home with spare beds, preferring sanctuaries. Pending groups
reserve those beds. Travelers retain their departure-home assignment until
arrival and do not work or train while traveling.

**Send supply group** uses two or three real adult carriers and deducts cargo
at departure, keeping a local food reserve. Food reaches the selected village
or the King only when the surviving group arrives; lost carriers reduce the
delivered share. Village deliveries may also carry surplus timber. Carriers
then physically return home. A direct recruitment or Call order can redirect
them. Each settlement supports at most four simultaneous groups of twelve;
distant groups share short streamed navigation corridors and remain subject
to obstacles and attacks. Cargo, return legs and member IDs survive saving.

The **rally point** is preferred when rescued family groups and elders need a
home. They travel there before the arrival celebration grants 90 seconds of
10% faster construction and 15% faster family progress. Ordinary combat rescues
remain with the traveling army. Recent village events show arrivals, warnings,
shortages, ready equipment and construction opportunities.

At a workshop, **Refit stationed champion** consumes one gear kit for one of a
champion's two possible equipment upgrades. **Issue reinforced log shields**
uses one siege bundle for up to four nearby gorillas/orangutans; shields have
225/180 health and preserve stronger existing champion shields. Crafting and
militia training pause under attack.

Focused regression coverage, including actual distant obstacle navigation and
save/restore during a supply return journey:

```sh
node --test game/apes-together-strong/tests/kingdom.test.cjs
```

## Champions and division leadership

Fortified prison extractions can award named champions with a species archetype,
visible equipment and a passive trait. Each of the six species has four
archetypes: structural bruisers, support builders, saboteurs, scouts, morale
leaders and ranged disruptors. There are at most 24 living champions; excess
elite rewards become veterans. Ordinary followers become eligible commanders
through two training levels or five credited infantry victories.

Champions have species-based additional health and 20% stronger strikes.
Equipment refits add 5% strike damage each, capped at two. Archetype bonuses
apply to their actual objectives: Wallbreakers breach gates/walls, Saboteurs
cut devices, Tank Busters benefit from attacking a vehicle's flank/rear,
Warcallers improve the existing Rally action, and Slinger Aces improve thrown
stones. Support champions only provide settlement production bonuses while
physically stationed there and recovered. Champion identity, equipment,
training, wounds and recovered status survive saves without reapplying bonuses.
Injured legends move and fight more slowly until safely stationed; sanctuary
recovery is twice as fast. Fallen champions appear in the council remembrance.

Open **Kingdom overview → Champions & divisions** to inspect the roster,
station nearby elites, recall them or form up to six named divisions. Choose a
recovered champion/veteran and up to 256 followers from unassigned apes, the
current army selection or the commander's species. Membership is exclusive.
Set a stance, fallback rule, priority and formation, then choose a discovered
objective, settlement, King's position or explored point on the map. A direct
Hold/Charge command puts division orders on standby; **Resume orders** restores
their saved plan. Recall removes responding apes from division orders so they
can follow the King. Q leaves established divisions untouched. Existing
species-route orders take control of the specifically ordered apes.

Divisions share targets and fortress paths. Assaults push; Hold and defense
stances remain close to their assigned ground; Skirmish maintains spacing;
Sabotage prioritizes devices; Rescue skips locked cells for release machinery
and escorts liberated groups; Climb and Breach uses the existing real wall
transitions. Heavies damage closed gates along fortress approaches. Commanders
improve nearby cohesion, speed and damage, with gorilla/mandrill defensive
support. Wounded/outnumbered rules retreat toward the King; loss of the commander
also causes retreat unless Hold at all costs was selected. Orders persist
through a save, including retreat following a commander's death.

Planning runs at most twice per second for six divisions, considering at most
52 candidates per division. Aura work runs once per second, affects at most
48 nearby allies per supporting champion, and keeps transient bonuses out of
save data. Offscreen divisions use the existing 15 Hz distant-order simulation.
Champion gear overlays do not expand the ordinary species sprite atlas;
at most 24 living elites receive the richer geometry, with simplified detail
at distance and reduced-motion support.

```sh
node --test game/apes-together-strong/tests/champions.test.cjs
```

A 120-frame, 1440×900 headless Chrome comparison includes **all** original
siege/arsenal render extensions in both the previous version and this update:

| Scenario | Previous mean work / p95 | Updated mean work / p95 |
| --- | --- | --- |
| 500 moving followers (G) | 20.21 / 28.5 ms | 21.24 / 30.0 ms |
| 750 apes, 320 humans, armor, aircraft and explosives (K) | 72.01 / 123.4 ms | 75.31 / 135.0 ms |

Both versions kept every actor and met the per-frame budgets (at most two path
searches observed, 96 render sight checks, 56 render rays). These are short,
synthetic sustained-combat measurements on one machine, including warm-up;
the extreme scene remains expensive and is not a 60 fps guarantee. Earlier
figures from a runner that omitted the siege/arsenal render extensions are not
directly comparable to this full-render comparison.


## Prison liberation operations

Newly explored military parcels can hold Regional Prisons, Blacksite Research
Compounds, Transfer Fortresses and Quarry Labor Prisons. Explored save regions
keep their geometry, damage and spent supplies. The opening rescue stays gentle.
Open the map to read a discovered prison's power, controls, captive count, escape
progress and specific sabotage objectives. Its main gates, climb access, inner
riot gate, towers, barracks, radio and detention blocks are physical objects.

Power-locked cells ignore raw damage until the generator is destroyed. Internal
release controls unlock both power and steel-door blocks; chain pens can be
broken directly. Disabling power also cuts searchlights. A breach escalates the
siren, closes the intact inner riot gate and commits up to three paid defender
waves; a nearby base may send one rescue-prevention column from its own reserves.
Radio, power and barracks sabotage stop local reinforcement dispatches.

Freed prisoners move in one visible crowd per holding block. Keep an escort
within 210 units, lead the crowd through a real opening, and spend four safe
seconds beyond the perimeter. Unescorted crowds exposed to guards for five
seconds can be recaptured. Injured survivors travel slower; a nearby Rescue
Bearer helps them. Captive slots remain reserved against the 1,000-ape limit while
they escape; rescue statistics, champion awards and food rewards are credited
once they are safe. Families and elders can then travel to the kingdom rally
village. Escapes, wounds, recaptures, locks and rewards persist through saves.

Planning is bounded to eight nearby facilities, twelve escape crowds and eighteen
guard duty updates each quarter-second. Remote crowds wait in saved state;
they never teleport to safety. Structure overlays, champion gear and weather
reuse the existing view culling and adaptive detail system.

## Weather, music and quick attack controls

Weather changes every 150 simulation seconds: clear, fog, rain, wind and storms.
Fog shortens human detection, rain and thunder dampen movement noise, roads
retain normal footing, and wet fields, rocky slopes and marsh edges slow travel.
These rules never block a physical bullet. The weather schedule survives saves.
Rain uses at most 64 screen-space streaks, fog four bands; low detail lowers these
counts and Reduced motion suppresses lightning and drifting animation.

The three supplied campaign MP3s are embedded in the downloadable HTML. The
selected reign track loops
only during play after a player gesture, pauses in menus and hidden tabs, and
respects master mute. Settings has its own music toggle and volume slider.
Default music gain is 22% of the 45% master volume (9.9% effective volume).
Audio remains local; no streaming service or external asset request is needed.
With the illustrated artwork included, the standalone download is approximately 87.7 MB.

**Tap E** issues a 13-second directional charge toward the pointer: humans,
vehicles, occupied cells, gates, towers and other hostile structures ahead
compete by actual distance. **Hold E** for 280 ms charges in the same direction
but targets humans and vehicles, including active and parked tanks; apes route
around obstacles and never redirect their strikes to structures. Releasing a hold does not also issue the tap
action. Both gestures scan and pursue within the same 570 world units per ape,
in a 120-degree forward cone anchored to the direction and position when issued.
Each ape's destination is 570 units along that heading, preserving the horde's
spacing. Without an eligible target, or after a kill, apes continue toward that
destination and wait there until the order expires. Changing the pointer does
not redirect an active charge. On touch, choose either charge mode and tap the
ground to aim it. Selected species/individuals receive the order;
with no selection it applies to all traveling followers. Pausing or leaving the
window cancels an unfinished key gesture. The command dock also offers both
actions and retains the directional Charge button. Nearest-target planning
rotates across large hordes with at most 12 plans per frame, sharing the existing
ape AI budget; rapid switching updates the order while throttling sound/effects.

Additional verification:

```sh
node --test game/apes-together-strong/tests/music-weather.test.cjs game/apes-together-strong/tests/nearest-orders.test.cjs game/apes-together-strong/tests/prisons.test.cjs
node game/apes-together-strong/tests/campaign-browser.cjs
```


## Mobile hold-and-flick commands

Hold open world space for 250 ms, slide to a segment, and release. The compact wheel exposes All Charge (existing E), Hold Position, Call Apes (existing Q), Recall, and Settlement. Release at the center to cancel. Hold over Charge for 280 ms for existing held-E targeting. Hold over Recall for 600 ms to reveal explicit Nearby, All field (R hold / T), and All + residents (T hold) scopes; each still requires release. Returning to center or canceling never issues an order.

Settlement opens existing founding, scouts, finder, map/management, and optional advanced army controls. The contextual Settlement button uses the existing main-hut interaction range; workshops retain equipment access. Advanced species and route controls appear only on demand and preserve the current selection when closed. Normal world taps, selected-army drag/pinch, keyboard controls, and game command mechanics remain shared with desktop. The primary touch HUD retains movement, Strike, pause, and the contextual hut/workshop action.

Pointer ownership isolates the joystick and Strike from world gestures. Pending holds cancel on movement, extra touches, screen changes, blur, death, cancellation, or resize. A canceled world touch cannot click through into a newly opened menu. Wheel DOM updates occur only on opening or selection changes, not per simulation tick.

Validation: `node game/apes-together-strong/tests/mobile-commands-browser.cjs` requires Playwright/Chromium (`CHROMIUM_PATH` can override the executable). It sends real touchscreen events to the source app and checks command dispatch, hold variants, cancellation, control isolation, normal targeting, edge placement, pinch, context range, and the existing settlement menu. `node game/apes-together-strong/tests/horde-commands-browser.cjs --source` checks existing Q/R/T keyboard and on-demand button behavior.
