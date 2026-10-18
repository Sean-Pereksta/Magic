# Neon Breach: Cats vs. Mice expansion

This update adds a playable faction war to the existing browser shooter: twelve
new animal soldiers, six weapons, six physical battlefields, connected-sector
Conquest, and two five-mission campaigns. It extends the existing renderer,
collision, navigation, controls, progression store, and match lifecycle. Original
Deathmatch, Sniper Duel, Siege, Survival, weapons, building, and Survival cosmetic
progression remain available.

## Playing the expansion

Open `game/neon-arena/` through the repository's existing web hosting. Choose
**Campaign**, **Conquest**, or **Cats vs. Mice Team Deathmatch**, then select a
faction, soldier appearance, starting weapon, and difficulty. The character
preview renders the actual GLB and can be rotated by dragging. The equipment
cards use previews generated from the same model geometry.

Team Deathmatch uses the selected faction against the opposing faction, matching
teammate/enemy appearances, opposite-side spawns, spawn protection, and the
existing score/time rules. Local split-screen remains available in Team
Deathmatch and Conquest. Campaign is single-player and disables the second-player
setting. Existing controller bindings and original mode controls remain in place.

### Controls

| Action | Keyboard/mouse | Touch |
| --- | --- | --- |
| Move, look, shoot, jump, build, reload | Existing controls; `R` reloads and `Shift` slides | Existing movement/look controls and action buttons |
| Crouch in faction modes | `C` toggles | Hold **Slide** for approximately 450 ms; tap retains the slide action |
| Aim down sights in faction modes | `G` toggles | Hold **Reload** for approximately 450 ms; tap retains reload |
| Campaign objective / optional alarm relay | `F` near the marker | Contextual pickup/interact button |
| Campaign automatic-fire safety | Manual keyboard shooting is unchanged | Tap **WEAPON SAFE / WEAPON LIVE** |

Start an interaction while near the objective and remain close, on the correct
floor, with a clear physical line to it until the action finishes. A short touch
or `F` press starts a brief interaction window; the HUD shows progress. Defense
and extraction objectives require the marked area to be clear of nearby enemies.
An escort must remain alive and reach the rendezvous/extraction area with you.

Touch Campaign starts with **WEAPON SAFE** so the existing aim assistance cannot
automatically shoot a guard and break stealth. **WEAPON LIVE** restores automatic
fire. This control governs touch automatic fire; it is not a universal weapon
lock applied to keyboard shooting.

## Factions and armory

The Cat Dominion includes Assault Trooper, Heavy Enforcer, Scout, Marksman, Night
Stalker, and Officer. The Mouse Resistance includes Rifleman, Runner, Engineer,
Recon Sniper, Shieldbearer, and Commander. Each has original body/equipment
geometry, recognizable animal anatomy, a distinct silhouette, and faction colors.
Character selection is cosmetic and does not change competitive health or damage.

| Weapon | Role | Visible model features |
| --- | --- | --- |
| Whisker AR | Assault rifle | Vented handguard, detachable magazine, sight, cycling bolt |
| Clawbreaker | Close-range shotgun | Tube magazine, moving pump, shell saddle, muzzle brake |
| Purr-90 | Compact SMG | Compact receiver, top-loading magazine, compact sight |
| Burrower | Suppressed carbine | Banded suppressor, detachable magazine, reduced flash/hearing range |
| Squeakshot | Marksman rifle | Long barrel, scope with mounts and adjustment turrets, bipod |
| Thunderpaw | Support machine gun | Box magazine, ammunition belt, carry handle, cooling rings, bipod |

The new weapons have different damage, range, rate of fire, spread, capacity,
reload duration, and predictable recoil sequences. Movement and airborne fire
increase spread; aiming and crouching reduce it. Recoil recovers between bursts.
They use the existing hit, tracer, impact, muzzle, reload, and sound systems, with
new mechanical-part motion and bounded casing effects. Existing arsenal entries
remain available to the original modes. Campaign and Conquest use the bundled
new arsenal so their model loading does not depend on the old remote gun pack.

## Battlefields and collision

| Battlefield | Playable architecture |
| --- | --- |
| Rail Yard Nine | Enterable passenger cars, signal/communications buildings, maintenance underpass, elevated freight crossing and tower |
| Whiskerhold Fortress | Four compound gates, firing slits, barracks, prison, command rooms, ramparts and watchtowers |
| Floodgate Sewers | Drainage passages, pump/control rooms, central aqueduct and upper cross galleries |
| Cold Storage District | Four enterable refrigeration buildings and offices, cargo routes and overhead loading gantry |
| Clockwork Foundry | Production workshops, turbine/furnace yard, destructible benches and maintenance platforms |
| Night Market Ruins | Enterable shops, alleys, ruined market cover and connected rooftop routes |

Windows are real gaps between separate sills, jambs and lintels. Doorways,
buildings, elevated decks, solid ramps, cover and scenery feed the same physical
geometry used by movement, bullets and visibility. The tests exercise passage
clearance, solid window frames, firing gaps, objective placement, routes between
sectors, and navigation to elevated fighting positions. Destruction updates
geometry/navigation state through the existing structure system.

These layouts retain the engine's approximately **130 × 130 metre** playable
bounds. The expansion adds denser interiors, vertical routes and alternate
approaches within those bounds; it does not implement an unlimited battlefield
or a larger-world streaming engine. Existing material atlases and scenery
prefabs are reused alongside new authored layouts. Menu map artwork consists of
accurate tactical SVG diagrams, not rendered landscape screenshots.

See [battlefield artwork and geometry notes](assets/campaign/maps/README.md).

## Conquest rules

Each battlefield has two headquarters and five sectors linked by a branching
graph. A faction may capture a sector adjacent to its headquarters or connected
controlled territory. Deep captures behind an unconnected front are rejected.
Enemy-held sectors must be neutralized before being captured. Opposing soldiers
contest progress; dead actors, spawn-protected actors, soldiers on another floor,
and actors blocked from the flag by solid geometry cannot capture it.

The default team roster target is **4 soldiers + 1 per controlled flag**. Humans
count toward that roster; the separate active-bot budget limits AI soldiers.
The menu offers budgets of 8, 12 or 16, base rosters of 3–6, and 0–2 extra soldiers
per flag. Every 15 seconds, reinforcements fill available roster places within
the shared cap. Priority alternates between factions under a tight cap. Losing a
sector stops excess replacements without deleting living soldiers.

Forward spawn candidates must be connected and uncontested. The host rejects
forward positions exposed to visible or nearby enemies and falls back to safer
positions. Bots receive Attack, Defend, Reinforce, Flank and Overwatch orders;
some hold owned territory while others advance toward a valid frontier.

The default ticket rule starts each side at 250: casualties cost tickets and a
territorial majority gradually drains the opponent's reserve. The alternative
score rule awards points for owned territory toward 300. The compact HUD shows
sector ownership/contests, capture progress, score or tickets, the next wave,
friendly positions, connected sector links and recent reported enemy sightings.
Sightings expire; the tactical map is not a continuous enemy-location feed.

## Campaigns

| Chapter | Mouse Resistance | Cat Dominion |
| --- | --- | --- |
| 1 | **The Broken Convoy** — recover a convoy manifest and extract from Cold Storage | **The Silent Outpost** — investigate Cold Storage, recover a recorder, rescue and escort an operator |
| 2 | **Behind Enemy Lines** — infiltrate Rail Yard Nine, disable security and steal dispatch intelligence | **The Night Market Hunt** — search caches, secure a relay and recover the scout's dispatch |
| 3 | **Operation Floodgate** — disable surveillance, sabotage pumps, defend the charge and escape | **Beneath the City** — silence a tunnel alarm, recover a supply ledger, tag a junction and extract |
| 4 | **Whiskerhold Rescue** — find the cell roster, free prisoners, escort their commander and extract | **The Foundry Offensive** — sabotage production, secure the vault, survive counterattacks and extract |
| 5 | **Dominion's End** — disable fortress defenses, seize the archive, hold the transmitter and withdraw | **The Underground Citadel** — enter Floodgate's command site, disable defenses, secure evidence and hold its transmission |

The campaigns have different briefings/debriefings and staged objective chains,
with mission-specific site metadata on the six shared battlefields. They combine
reach, intelligence, sabotage, rescue, escort, defense and extraction objectives.
Each mission has a separate local briefing graphic showing its objective route;
the map preview builder regenerates all ten from the playable site coordinates.
The optional relay objective disables alarm escalation. Detection raises alarm
heat and queues bounded reinforcements; breaking contact lets heat decline.
Defense transitions dispatch counterattack orders, including when the active
roster cap prevents adding more soldiers. Difficulty changes guard counts/types,
reaction/accuracy/teamwork settings and defense durations.

Completed missions unlock the next chapter of that faction. Completion records
track stars, best time, completion count, difficulty and cosmetic reward IDs.
Stars reward completion, avoiding alarms and optional relay sabotage. The current
visible campaign reward is an equippable faction weapon finish; named medal IDs
do not imply that a separate armor mesh has been authored for every reward.
No campaign reward increases competitive damage or health.

### Checkpoints and existing profiles

Campaign state lives inside the existing versioned Survival progress payload,
using `neon-breach.survival.v2` and its backup rather than a new save service.
Stage transitions and leaving an active page save checkpoints. The menu can
resume a saved checkpoint or start the selected mission fresh. Restoring a
checkpoint reconstructs the stage, player position/loadout/health and rescued
escort while clearing stale active perception. It is a stage retry, not an exact
snapshot of every bullet, destroyed prop and guard animation.

Guests save locally. An already authenticated, correctly linked lobby profile
can synchronize the same `neonSurvival` field through the existing adapter.
Offline/denied cloud requests do not prevent local play. Save validation rejects
unknown mission/reward data and malformed checkpoints; profile merging preserves
earned completion records. Existing Survival statistics, cosmetics and unrelated
profile fields remain in the same supported storage architecture.

## Perception and tactical behavior

Guards use a central **±35°** vision cone and peripheral **±95°** cone, with
physical visibility checks. Clear central contact is recognized on a perception
update; firing still respects reaction time. Peripheral recognition accumulates
from noticeable movement, while stationary or slowly crouching targets can
remain unidentified. Outside the cone, visual contact is unavailable until the
guard turns.

Gunfire, suppressed fire, footsteps, sprinting, slides, reloads, explosions,
impacts and fallen allies feed the hearing system. Obstructions attenuate hearing;
heard shots provide an uncertain position snapshot, not a live target reference.
The event API also supports door, breach and distraction sounds for hosts that
emit them. Static map doorways are not a new general-purpose operable-door system.

Guards progress through patrol, suspicion, investigation, search, alert,
engagement and regrouping. Losing physical contact clears the live enemy target;
investigation uses expiring last-known snapshots. Nearby teammates can share
recent sight evidence. Cover decisions validate a hiding location and a usable
peek location, and can crouch, retreat/reload, abandon compromised cover or flank.
Squad orders and weapon range influence movement rather than providing hidden
enemy coordinates. Path caches, geometry revisions, staggered decisions and
bounded searches control per-frame cost; stalled movement triggers replanning.

## Assets, rebuilding and deployment

All eighteen new GLBs are committed locally with normals, materials and named
animation groups. Character geometry is approximately 6.2–7.4k triangles per
model; weapons are approximately 2.4–3.4k. Downloads and GPU buffers are shared.
The renderer limits articulated actors per view and draws others as static models
within the existing distance/render budgets. Tails and ears are cosmetic and do
not enlarge competitive body hitboxes. The Shieldbearer's folded pack shield is
also cosmetic equipment.

Rebuild from the repository root:

```sh
python game/neon-arena/tools/build-factions.py
node game/neon-arena/tools/build-war-map-previews.cjs
```

Python 3.10+ is sufficient for model generation; it needs no external packages.
The model builder and tactical-map preview builder are deterministic. Models use
the existing rigid-part glTF renderer, so no new decoder or backend is required.

The default expansion asset root is `./assets`. Optional CDN/R2 hosting uses the
exact relative paths already recorded in the manifest. Upload the files before
changing `EXPANSION_ASSET_BASE_URL`; retain the local fallback. An unavailable or
corrupt optional CDN model retries its bundled counterpart. Original weapon and
alien hosting configuration remains unchanged. This is a local deployment option,
not a claim that every old remote asset or an uncached web page works in airplane
mode.

- [Model manifest: hashes, bounds, counts and exact object keys](assets/factions/manifest.json)
- [Model builder, renderer integration and CDN fallback instructions](assets/factions/README.md)
- [Battlefield diagrams and geometry budgets](assets/campaign/maps/README.md)
- [Runtime faction/weapon catalog](neon-factions.js)

## Validation and remaining verification

Run the full existing Neon unit/regression suite from the repository root:

```sh
npm run test:neon
```

Focused checks and real-browser harnesses:

```sh
node --test tests/neon-war-integration.test.cjs tests/neon-campaign.test.cjs tests/neon-guard-perception.test.cjs
node --test game/neon-arena/tests/conquest.test.cjs game/neon-arena/tests/war-maps.test.cjs game/neon-arena/tests/faction-assets.test.cjs
node game/neon-arena/tests/model-renderer.cjs
node tests/neon-factions-browser.cjs
node tests/neon-war-browser.cjs
npm run test:neon:war-browser
```

Browser harnesses require Playwright and Chrome/Chromium. The asset harness accepts
`CHROME_PATH` and `NEON_QA_OUTPUT`; the game harness accepts `CHROMIUM_PATH` (or
`CHROME_PATH`) and `NEON_WAR_REPORT`. `NEON_WAR_GAMEPLAY_ONLY=1` narrows the latter
to its desktop gameplay run. The browser tests serve their own local files and do not add a
production server dependency.

The recorded validation snapshot during implementation was:

| Check | Completed evidence |
| --- | --- |
| Full unit/regression suite | 258 checks: 257 passed, one optional original raptor-fixture check skipped; zero failures |
| Existing standalone model renderer suite | 13 checks passed; optional uncommitted legacy art-pack validation skipped |
| New actual-model browser test | Twelve character GLBs, six weapon GLBs, eight animation states, magazine motion and all 44 catalog asset URLs passed without browser/loading errors |
| Original browser compatibility suites | All ten passed with real Chrome/WebGL and verified original model fixtures: baseline, menu, tiles, Survival, slide, UI, allies, overhaul, projectile and explosion |
| Expansion runtime browser matrix | All 30 cases passed: six Conquest maps on desktop, touch viewport and local split-screen; all ten scripted campaign objective chains; and both factions' first missions using actual touch taps/holds for weapon safety, crouch and aiming. All ten mission graphics decoded successfully; zero browser or WebGL errors |

Compatibility coverage includes 51 map rendering cases, 2,052 ramp traversals,
twelve HUD layouts, all eight Survival arenas, original save/unlock behavior,
all twenty weapons, split-screen allies, and projectile/explosion wall occlusion.
The original browser suites ran serially to avoid software-renderer contention.

The game harness also passed scripted objective-chain completion and persistence
checks for all ten missions, including defense orders at transitions. It moves
the player to objective sites, removes opponents to isolate stage transitions,
and positions escorts at their destinations. The runtime matrix does not
demonstrate unscripted combat completion,
natural escort pacing or campaign balance.

The following remain explicit limits and manual verification work:

- Animation uses the existing named rigid-part pose system, not skeletal or
  motion-captured clips. Extreme aiming/reloading hand alignment needs playtesting.
- The maps retain the approximately 130 m engine bounds. Their vertical routes
  and buildings are authored within that envelope.
- Campaign story delivery uses text briefings, objectives and debriefings;
  alarm escalation is a gameplay state system, not a cinematic or general
  automatic security-door/lockdown simulation.
- New controller gesture choices, actual touch hardware, browser audio behavior,
  long sessions, existing live multiplayer sessions, weapon balance, stealth
  difficulty, squad effectiveness and comeback pacing require human playtesting.
- Desktop 60 FPS and mobile 30–60 FPS are targets, not certified results.
  Headless Chrome/SwiftShader rendering and geometry budgets do not establish
  real-device sustained frame rate, thermal behavior or memory limits.
- Passing automated checks does not prove every acceptance item in the original
  expansion brief. The checks above distinguish loaded assets, deterministic
  rules, physical routes, scripted integration and behavior still needing review.
