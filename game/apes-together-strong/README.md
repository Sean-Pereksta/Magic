# Apes Together Strong

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
- **Balance:** adults have 120 health (roughly 2–4 ordinary hits on Survival),
  scouts 150, and young 60. Marksmen enter tier 3+ forces and retain warned,
  lethal shots. Captive counts are substantially higher: checkpoints hold
  18–32, prisons 40–70, detention camps 80–120, and experimental camps 120–180.
  Military liberation awards 30 food per site tier once, and camp supplies and
  ape score rewards are larger. Rescue/population threat contributions are
  lower so one successful rescue does not immediately trigger a military hunt.
- **Settlements:** food patches, water, fertility, nearby timber, housing, and
  local human activity affect a camp. Adults forage, build, or guard. Work
  priorities favor growth, food, or defense. Construction produces shelters,
  gardens, lodges, stores, and defenses; population growth requires food, room,
  and safety. Visit to deliver food or recruit adults. Raids consume defenses
  and supplies, and scouts give advance warnings.
  Lodge targets advance one level per 12 inhabitants, reaching level 10 at 108;
  upgrades still require building work and timber. Construction and timber
  gathering are faster, shelters add 12 housing, and gardens produce more food.
  Each eight adults contribute one family work unit per second toward a birth
  every 30 units, subject to food, housing, safety, local capacity, and the
  shared 1,000-ape limit. Young mature after 35 seconds, including while far from the king.
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

New forward bases, armored depots and rare regional command bases have larger
305/345/430-unit compounds, layered barricade rings, broad guarded gates,
service openings, multiple floodlights, armor parking and 30–78 defenders.
Command installations contain major captivity areas. Roads remain clear of the
expanded perimeters. Temporary checkpoints assemble at road approaches after
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

WASD/arrows move; Shift sprints; Ctrl sneaks; click/Space attacks. Q calls,
E charges toward the pointer, R recalls, F holds, Z settles nearby followers,
Shift+Z settles all, and C assigns scouts. M/Tab opens the map and settlement
controls; Escape pauses. Touch uses a movement stick, strike button, and command
menu.

The original `ats-crown-save-v1` storage key and JSON save format are retained.
Older tree collision and settlement records migrate on load. Navigation caches
are rebuilt; they are not serialized. Autosave runs every 15 seconds, with
manual save and JSON export/import in Pause. The king's death removes the
living checkpoint and records a legacy score.

Balance revision 2 migrates older ape health proportionally, retains family
progress, and increases rewards in existing unbroken cages once. Cleared camps
and past rescue statistics retain their history.

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

The **Find Settlement** button or **V** toggles one named direction/distance
marker for every founded settlement, including empty bases after their residents
join the horde. Destinations project onto all four screen edges; a nearby base
receives a home marker. Nearby
markers separate, attack status changes their color, and the layout handles
camera motion, zoom, desktop and touch. Huts persist as individual plots with
health, damage bars and ruins. Human siege fire requires nearby line of sight;
grenades, shells, mortars and airstrikes can destroy homes. Destroyed homes
remove housing immediately, and builders repair or reconstruct with paid work.
New construction searches bounded dry, unobstructed plots around the lodge.

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
apes must physically break human sections. Apes vault their own barriers while
infantry climb or breach slowly, engineers dismantle faster, and tanks can
crush only designated weak ape sections. APCs unload before contact and armor
supports infantry from cleared approaches.

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
