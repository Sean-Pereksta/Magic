# IRONFIELD 2.7 — Grand battlefields and rampart defense

- Added connected walkable castle walls, towers, and gatehouse decks with physical interior stairs. Foot formations can man the walls with one to three ranks and adjustable spacing; elevated ranged guards retain rank volleys and receive parapet protection against exterior shots.
- Added wall-top selection, formation previews, saved guard plans, and custom wall stair sides. Wall and gate collapses trigger a courtyard defense, while intact guards descend and unsupported guards fall. Ground orders continue after safe descent.
- Raised the battle limit to 40,000 soldiers, 20,000 per army, across simulation, builder, army scaling, scenario imports, and full formation saves. Added 8× army scale.
- Added eight authored battlefields and ten historical battles, for 30 authored fields, 32 authored scenarios, and 30 historical scenarios.
- Added roads, mud, marshes, snow, and trenches with movement and cover effects, route costs, and distinct art.
- Added timber palisades, curtain castles, moated castles, bastioned star forts, and concentric citadels, while retaining the original square keep. Added houses, watchtowers, barricades, and hollow gate keeps.
- Preserved structure materials and names through the editor and saves. Gate checks cover every entrance, and moat templates include physical causeways. Expanded custom terrain capacity to 192 areas.
- Added a faster distant-army drawing pass and cached terrain views while retaining detailed sprites and animation at close range. Optimized soldier state, collision access, and unnecessary navigation work. On clear distant ground, large formed companies can share marching calculations; terrain, nearby formations, turns, and combat return them to the individual solver.

## IRONFIELD 2.6 — Fire by ranks and the Campaign Atlas

- Musketeers use a regiment schedule: synchronized successive rank volleys, 24-second reloads and 78 base shot damage. Added three-rank volley and three-file column styles, melee disruption, reforming, and visible rank status.
- Cannon and trebuchet blasts throw enemies farther; catapult impacts also displace and lift troops.
- Added 20 sourced historical scenarios with explicit scaled opening deployments, army names, terrain, formation facings, and approximation notes.
- Modernized the menu with a historical atlas, search, era filters, deployment previews, briefings and linked sources. Army scaling preserves authored deployment positions.
- Improved procedural troop silhouettes, team equipment details, portraits, biome details and terrain animation. Reduced animation settings remain supported.

## IRONFIELD 2.5.1 — gun-line reload balance

- Musketeers take 12 seconds to reload after a shot, up from 4.4 seconds. Field cannons take 22 seconds, up from 9. The longer intervals give cavalry time to close between volleys.
- Fixed a timing bug that advanced the reload twice per simulation step when a bombarding gun was knocked back.

# IRONFIELD 2.5.0 — gunpowder, trebuchets and war clubs

- Muskets fire fast, flat projectiles that stagger the first opposing soldier they hit.
- Field cannons deliver heavy area damage and throw nearby troops. Trebuchets cast a much larger stone over a longer distance and take 24 seconds to reload.
- Berserkers now swing two-handed clubs through a forward arc. Old armies and saved scenarios keep their existing Berserker formations.
- Troop category tabs filter army cards and the custom formation picker. Selecting musketeers, cannons or trebuchets also shows their live reload status.
- Added **Thunder at Stonekeep**, a playable assault with all four weapons.

# IRONFIELD 2.4.0 — marching orders and castle sieges

- Right-click open ground to march, an enemy position to attack, or an enemy fortification to assault. Move and manual Retreat take control immediately. Troops march from their current positions and dress their ranks at the destination.
- Stonekeep, Floodgate Citadel, and The Three Crowns now have enclosed keeps, corner towers, and gated approaches. Defenders can open intact castle gates. A gate can close once the doorway clears. Rams can breach it permanently.
- The Castle brush creates an enclosed square or rectangular fortification. Choose its defender, drag the perimeter, and deploy the garrison in the courtyard. Undo reverses the placement in one step.
- Connected reserves on either side add weight during a sustained clash. Backing ranks can yield along with the front, and prepared heavy reserves can hold it in place. Routed regiments must have lost at least 80% of their troops and suffered a morale collapse.

# IRONFIELD 2.3.2 — lancer traffic on open ground

- Friendly riders on a collision course begin a sideways pass while they still have room. A single parked horse can yield to a charging file and hold its new position after the tail passes.
- Nearby companies advancing together move their leading riders first, including when the next rank belongs to a different company. Neighboring wings charging one enemy keep their approach lanes until they reach combat distance.
- Crossing lancer companies use the available width of the field to pass each other while moving. The player’s destination and the riders’ normal collision and attack rules still govern the march.
- Added the importable `examples/lancer-convoy.ironfield.json` encounter and tests that measure the riders’ forward travel, clearance, and arrival at the enemy line.

# IRONFIELD 2.3.1 — friendly cavalry clearance

- Friendly lancers, cavalry, and mounted archers open small lateral gaps before the ordinary collision pass. Nearby riders can pass the sidestep through a short bounded chain while each body uses real free ground and its shared movement allowance.
- Preserve requested acceleration through a progressing friendly brush. Actual forward travel still determines charge speed and damage; walls, hostile impacts, bracing, and hard stops do not receive free momentum.
- Advance free mounted Charge files from front to back, allowing followers to use ground already vacated by their leaders. Contact uses the existing alternating combat traversal, and every rider still executes a normal collision-checked move. Reuse the per-regiment ordering buffers.
- Keep physically dense mounted charges in their captured approach lanes until the real enemy front is close. This avoids a distant centroid triggering a premature jump to broad combat slots. More open formations retain their existing preparation distance.
- Align the mounted Charge guide with the pace requested by its riders, retaining the existing limits for real body progress, rear ranks, terrain, and turns.
- Keep temporary clearance through passing files, then recover spacing gradually. Held riders retain a shifted individual anchor and facing. New commands, formation changes, splits, retreat, recovery, and tactical transitions discard the old courtesy interaction.
- Reject a chained sidestep that would push a moving recipient backward along its own accepted direction. Different nearby courses retain their movement authority while the other riders make space.
- Give assigned crossing lanes priority over old courtesy offsets, preventing automatic cavalry from becoming trapped at a passing waypoint. Once a shock-cavalry formation deploys against the enemy front, its combat slots keep control of the supporting ranks so friendly sidesteps do not disrupt the plunge or rear attack.
- Show substantial sideways clearance with distance-driven mounted sidesteps. Small corrections preserve the gallop; attacks, turns, and command poses retain their priority.
- Fixed a narrow-passage navigation trap: a horse beside a wall could fit physically but its next goal violated the extra navigation margin. Small local clearance probes now find the passage center before the old coarse search chooses a long detour. A released friendly queue can resume its advance.
- Extend current-step collision searches to cover the additional real body movement. Add packed-rank, interleaved-company, held-rider, command, terrain, and impact regression coverage. The validation report records the final physical measurements and verification scope.
- Reduced the added simulation cost by rejecting neighbors outside the existing movement corridor before checking detailed courtesy readiness, skipping redundant queries during assigned crossing traffic, and avoiding goal work when there is no offset. Movement rules and collision limits remain intact. The validation report includes the remaining measured CPU cost against 2.3.0.

# IRONFIELD 2.3.0 — faster horses and yielding formations

- Increased Royal Cavalry base speed from 92 to 150, Banner Lancers from 87 to 132, and Steppe Outriders from 105 to 162. Mounted commanders increase from 65 to 150 so they can follow the army. Infantry, elephants, and siege equipment keep their existing speeds.
- Added sustained formation pressure. Stronger engaged forces can gradually displace connected opposing ranks, with mass, quality, support, cohesion, and bracing affecting the result. Rear-first movement uses the existing body and terrain collision rules; separated stragglers stay separate.
- Shifted a yielding line's actual formation anchors with its physical displacement so troops do not immediately rush back to obsolete slots. New commands, fallback, rally recovery, routing, splits, formation changes, and tactical phase changes discard old pressure.
- Fixed a command-priority failure during friendly overtaking: fast followers could open a new held passage and pull infantry against a fresh order to leave. A departing commanded formation now keeps that direction while ordinary body collision governs the crossing.
- Fixed nearby path corners being mistaken for arrival during crossing prediction. Automatic traffic now considers the continuing route, preventing a moving formation from being held in an inappropriate passing column. The original Broken Charge and doubled opening retain their physical progress and combat checks.
- Kept faster cavalry together around terrain corners by letting the formation guide account for lagging rear riders. Clear-ground sustained speed stays unchanged, and bodies still follow their own collision-checked routes.
- Added visible backsteps and sidesteps plus an actual-movement GIVING GROUND banner. Attacks, turning, explicit orders, morale, and siege statuses retain their display priority. Detailed poses remain behind the existing performance-detail limits.
- Preserved charge follow-through at the higher mounted speeds: bounded body impact energy now uses a shared physical speed reference, and an aligned explicit Charge retains its committed direction through the opening ranks.
- Added actual-body speed and formation-pressure regression scenarios, including turns, retreats, woodland, narrow rock passages, whole-rank displacement, prepared defenses, rear obstructions, and command changes. The release validation file contains the final measured results and verification scope.

# IRONFIELD 2.2.1 — clearer map sizes

- Added prominent Standard, Large (2.25× area), Huge (4×), and Colossal (6.25×) labels, derived from actual dimensions, across map cards, scenario/map selectors, current-field details, custom sizes, Resume, and saved encounters.
- Put the largest maps and scenarios first. Renamed the main-menu filter to **Large maps · 12**; filtering now selects a matching field when the previous selection is hidden, so Prepare and Quick Battle use a visible choice.
- Replaced the tiny corner multiplier with a full badge row. Widened the battlefield picker and added the selected field's dimensions beneath it.
- Renamed Quick Scale to **Army scale · Soldier counts**, with explicit troop multipliers, to distinguish it from battlefield area.
- Updated the offline bundle and manual. Battle simulation, terrain definitions, strategies, and renderer code retain the verified 2.2.0 implementation.

# IRONFIELD 2.2.0 — larger theaters and coordinated advances

## A main menu and larger battlefields

- Added a full main menu with a battlefield browser, Quick Battle, Custom Encounter, saved scenarios, field manual, settings, and Resume Battle. Opening the menu pauses the encounter; resuming preserves its engine, troops, positions, selection, and accepted orders. A previously paused battle stays paused. Battlefield rendering, simulation, minimap work, and hotkeys stop while the menu is open.
- Added twelve authored battlefields and twelve scenarios. The catalog now contains 22 authored maps plus Custom, 23 scenarios, and 20 troop/engine types. New maps cover open plains, colossal steppe, connected forests, five river crossings, canyon passages, desert, lake approaches, wetlands, and three additional fortified assaults.
- Grand 9,600×6,600, Vast 12,800×8,800, and Colossal 16,000×11,000 worlds provide 2.25×, 4×, and 6.25× the original area. Troop scale is unchanged. The custom editor exposes all four supported sizes; growing preserves coordinates and unsafe shrinking leaves the layout intact.
- Removed fixed old-world clamps from scenario normalization. Large custom terrain, unit offsets, facing, and deployments now survive save/load/import beyond the old 6400×4400 boundary.

## Six more plans that move the troops

- Steady Advance sends a deliberate infantry front forward, preserving lateral jobs and earning a partial walking brace through cohesion, actual forward motion, and correct facing. Pikes and hoplites gain the strongest protection. Stationary bracing remains stronger, and turning, sprinting, or disruption removes the walking brace.
- Rapid Encirclement identifies isolated formations or bounded local groups of neighboring enemy squads. It assigns a pinning front and separated wings, routes around the selected position's whole extent, and closes from the rear. The selection panel reports Single Formation or Enemy Position with the target squad count. A destroyed objective leads to a new local assessment.
- Refused Flank physically withdraws and insets the threatened wing while its opposite commits. Concentrated Breakthrough uses a limited first wave at a weak sector, with shoulders and a following reserve. Elastic Defense yields in stages, regroups, and counterattacks. Echelon Assault releases successive offset waves.
- Rapid wings use shared frontage-aware lane spacing, bounded progress checks, and physical fallback routes when a passage stops advancing. Pinning and support troops target the same assigned enemy position as their wings. Steady formations correct an outer approach onto an enemy front instead of marching past it.
- Fixed an older Encircle delay exposed by the stronger impacts: a cavalry queue could creep toward its outside waypoint and count as healthy progress indefinitely beside a reachable enemy. Progress now accounts for the actual observation interval, allowing a blocked wing to engage that nearby threat while preserving the rear approach and Hammer & Anvil's wait.
- The original eight plans remain available. Direct Move, Face, Hold, Attack, Charge, and Retreat commands override automatic plans; G returns the selection to Auto. Recovery and rally behavior retain priority over army-wide plans.

## More movement in the battle, persistent casualties

- Added eight distance-driven gait phases for the troop types, fast mounted-charge poses, moving braces, protected-hit and recoil reactions, and collapsing casualties. Extra sprite poses are created lazily with a bounded per-frame budget.
- Added Brief 22-second, Long 120-second, and Entire Battle body persistence. Entire Battle preserves casualties until reset, layering later deaths into local piles. The visibility toggle remains separate. Bodies are visual; they do not block movement.
- Settled long-lived casualties use spatial chunk caches. All death records remain available after bitmap eviction, while visible chunk drawing replaces per-corpse work. A 20,000-death stress fixture and pan/zoom cache checks cover bounded ingestion, retention, and memory.
- Large-world terrain keeps a bounded bitmap size. The camera can fit a Colossal field, the minimap reuses unchanged terrain, and large armies update its moving overlay less frequently. Full and Reduced animation choices persist in preferences.

## Mounted impact tuning and verification

The mounted-impact response has been strengthened further while preserving real speed, collision, terrain, bracing, and fresh-order limits. See BALANCE.md for the final physical rules and tests/VALIDATION.md for measured lancer/cavalry results, the complete release verification, and simulation timing limits.

# IRONFIELD 2.1.1 — lancers break weak ranks

Fast charges were losing their forward drive when the front target fell,
and a short, straight-back shove kept weak infantry in a moving wall ahead
of the riders. Lancers now carry their run through that first contact and
push low-mass light troops backward and outward, opening real space through
several ranks. Royal Cavalry receives a smaller light-infantry push bonus.

The effect needs speed. Low-mass bodies consume less mounted momentum,
while prepared polearms, hoplites, heavy troops, equipment, and terrain retain
their resistance. Supporting ranks still absorb and transfer pressure. Body
collision, forced-movement budgets, and speed bounds remain unchanged; raw
troop statistics and direct impact damage formulas are unchanged.

Fresh orders clear the old charge follow-through. A new physical test charges
twelve lancers through four ranks of fifty-six held levies and excludes riders
that simply bypass the flanks. Controls cover short run-ups, hoplites, pikes,
heavy infantry, rock, and an engaged retreat. The package includes a matching
`examples/lancer-breakthrough.ironfield.json` drill. Current measurements and
regression results are in `tests/VALIDATION.md`.

---

# IRONFIELD 2.1.0 — cavalry momentum and functional sieges

## Speed becomes physical charge force

Cavalry, lancers, and mounted archers accelerate from rest. Straight-line riding
builds momentum; turns, blocked motion, terrain, and impacts remove it. Cavalry
and lancers can keep closing through weapon reach to strike an enemy's real
body. Contact damage scales with closing speed squared, uses a bounded
per-pair cooldown, and immediately reduces the horse's speed.

Impact pressure moves bodies through the ordinary swept collision solver.
Supporting ranks absorb force and receive diminishing displacement. Formed
hoplites resist better, prepared pikes sharply interrupt a frontal charge,
and solid terrain can stop the displacement. Light unsupported infantry can
briefly fly, with a ground shadow and a read-only vertical pose. Dead impact
victims finish their flight and settle into the existing temporary bodies.
Move, Hold, Face, and Retreat retain control; retreat turns before withdrawal.

A separate walking controller prevents collision sidesteps from resetting a
horse to its first acceleration step forever. Cavalry formation guides keep a
small lead over the real bodies and pace their wheels to the physical ranks.
This also applies while reforming a newly ordered wedge, so its target shape
does not outrun the soldiers. Actual movement still supplies charge momentum.

The performance pass removes redundant motion initialization and repeated
distance calculations, limits horse-specific calls to mounted troops, and
skips empty-ramp and deployed-tower work in ordinary field battles. Actual
movement still records the full displacement budget for subsequent impacts.

## Walls, gates, rams, and assault towers

The roster now has 20 distinct types: the 18 original field troops plus
Assault Siege Towers and Covered Battering Rams. Each machine has its own
formation, cached silhouette, six strike/reload poses, portrait, and wreck.
Each army can field 12 towers and 12 rams. Original field rosters are unchanged.

Rams physically reach their target and batter gates. Towers must dock at a
wall and deploy for four simulated seconds before an assault ramp opens.
Soldiers climb the ramp and use its real passage through the fortification.
Existing artillery can bombard structures. Fortification damage, cracks,
health, breached rubble, deployment progress, and riding speed are visible.

Right-clicking an enemy structure assigns a siege job. Auto finds useful
assault work; direct movement and withdrawal commands take priority.
Breaches and new ramps invalidate navigation and clearance caches when the
terrain actually changes. Moving machines retain body collision. A docked
tower becomes an engineered platform, while its established ramp remains
usable if the machine is later lost or moved.

## Three prepared siege encounters and custom fortifications

- **Storm the Stonekeep**: 951 attackers and 883 defenders; a curtain wall,
  central gate, and multiple tower approaches.
- **The Siege of Two Bridges**: 809 attackers and 803 defenders; a moat,
  narrow causeways, and two guarded entrances.
- **Assault on Crownwatch**: 847 attackers and 803 defenders; rocky approach
  corridors and a fortified bastion.

The custom editor adds Wall, Gate, and defending-army controls. Painting a
gate into a wall cuts the intersecting masonry so its breach is usable.
Fortification ownership and initial health persist through save and import.
Narrow approaches continue to use real body clearance. Single machines do
not impose formation-interleaving columns on an entire infantry company.

## Verification and source

Independent checks exercise public deployment, commands, and simulation
steps for acceleration, damage, flight, rear-rank pressure, brace resistance,
terrain, real siege arrival/deployment, breaches, and passage traffic. UI
checks cover real input handlers and scenario round-trips. Canvas operation
checks cover airborne/landing continuity and equipment/structure artwork.
The complete regression, scale, and timing evidence is in
`tests/VALIDATION.md`; those headless measurements are not browser FPS.

Two dependency-free modules, `src/impacts.js` and `src/siege.js`, sit between
the tactics and engine scripts. The single-file offline build includes all
source. No browser preview is generated.

---

# IRONFIELD 2.0.0 — stable marches and coordinated army plans

## Stop rebuilding the same march

Passable woodland no longer forces formations into half-width columns.
Formations retain their files and gently adjust spacing while taking the
terrain's normal movement penalty. Genuine hard passages commit to a file
count once, avoiding repeated slot sorting at every intermediate width.
Manual marches also carry the soldiers' current arrangement. Turns preserve
rank relationships, and an about-face changes which existing rank leads.

Open-ground overtakes preserve the passing formation's real footprint and
leave the leading formation's ranks intact. Broad bypasses are used where
there is room; confined crossings continue to open real gaps. Yielding ends
when the crossing clears or its intent changes. Held tactical lines can make
local space for traffic and retain the gaps that actually emerged.

Traffic distinguishes the next path corner from the final destination. Nearing
a bend around rocks no longer makes a moving formation appear stationary and
trap equal-speed followers in a shared waiting plan.

Combat positions are no longer reassigned solely because a short timer
expired. Casualties, real frontage changes, fresh commands, and terrain still
trigger the appropriate changes. Every soldier remains subject to physical
body collision, weapon reach, and terrain clearance.

## Eight plans with visible jobs

A separate tactics module assigns persistent roles and physical destinations.
Encircle sends cavalry and lancers outside the enemy wing, past it, into a rear
approach, and finally into an attack. Horse archers finish in a flanking
harassment role; elephants retain their own battlefield role. Several mounted
formations receive separate lanes and keep their chosen side during the march.
The outer leg clears the chosen target's rear instead of circling past unrelated
artillery farther back. Later waves retain safe entry spacing without adding a
second extension beyond the target. Once the riders reach a physical rear attack
position, they deploy rather than waiting to gather at an exact waypoint.

The plans are Balanced, Full assault, Hold & counter, Encircle, Hammer & anvil,
Attack left, Attack right, and Screen & skirmish. Held reserves, counterattacks,
a covered missile withdrawal, and the cavalry's coordinated rear strike use
actual movement and target intent. They do not change troop statistics.
Full assault commits to targets after the normal frontage allocator distributes
the formations, keeping depth-stacked reinforcements from all choosing the same
central enemy. Formed polearms now use their rank direction for the generic
line or shield-wall frontal defense as well as the brace itself; an individual
soldier turning to strike a flanker cannot rotate those formation defenses.

The new Battle plans panel applies plans during deployment or a battle without
rebuilding the army. Direct player orders take priority, and recovering troops
continue withdrawing or regrouping. Auto / G returns a selected group to the
army plan. The selection panel reports the current phase, and selected
maneuvers draw their remaining route. A new 640-soldier cavalry drill runs from
import and Start with both armies already assigned plans.

## Reduce repeated simulation and drawing work

Collision broad-phase queries separate ordinary and large bodies, while
relative collision geometry is reused through the constraint passes. Shared
march, formation, and speed calculations are cached per regiment for the
current fixed step. Settled soldiers avoid unnecessary navigation work.
Soldier and formation state is initialized consistently to retain fast
JavaScript property access. A conservative map-cell cache skips repeated
terrain scans only where no nearby solid region can affect the body; exact
geometry checks remain active beside rocks, rivers, and bridges.

Large overview views reuse screen-row depth buffers and direct sprite
references instead of performing a comparison sort and atlas lookup for every
soldier on every frame. High detail and close views retain exact depth order.
Adaptive detail responds earlier, with a delay between level changes. Every
living visible soldier continues to be drawn and simulated.

The tests cover physical rank shape, actual rear attacks, collision safety,
new-plan command priority, save/import, and the existing battle behaviors.
The separate performance harness uses fresh processes for each scenario.
See tests/VALIDATION.md for final measurements and their scope.

# IRONFIELD 1.9.1 — unblock automatic crossings in The Broken Charge

## Cause

Automatic formations could inherit a temporary passing destination that stayed
held after their routes had changed. Soldiers reached those temporary slots
and considered their movement complete, even though their own battlefield
destination was still far away. Cleanup waited for the entire formation to
clear the original crossing direction. Some groups consequently waited for
troops that could no longer complete that crossing.

Mixed-size crossings also sent cavalry into a very long shared passing column
beside elephants. Riders spent time exchanging ranks instead of advancing.
The resulting loss of cohesion could label intact formations SHAKEN before
they had reached an enemy.

The preset also exposed overlapping mounted rows at deployment: adjacent
lancers started closer together than their combined body radii. The fixed
row spacing did not include the full width of the mounted formation.

## Correction

Automatic groups crossing on open ground now use a brief, one-sided yield that
keeps their current ranks. The leading group continues toward its own goal.
Crossings are rechecked against physical footprints and current intent, with
bounded waiting and a short retry interval to prevent repeated capture by the
same obsolete maneuver. Completed passages resume from the troops' actual
arrangement.

Fresh movement, retreat, and formation orders clear temporary yielding. Manual crossings,
opposed traffic, and narrow terrain retain physical passage rules. Soldier
collision, terrain clearance, movement limits, armor counters, and troop
statistics remain active.

Default deployment now expands row spacing only where adjacent formations'
actual body extents need additional clearance. Army centering, type order,
rosters, maps, and troop statistics remain the same, and explicitly saved
formation positions are preserved.

Attacks also validate reach against the attacker's actual terrain at release.
This prevents a cached hill-range bonus from briefly extending attacks after
the soldier has already stepped onto flat ground. Weapon statistics and the
normal high-ground bonus are unchanged.

The regression checks include the unmodified Broken Charge preset and a small
elephant/cavalry crossing that isolates the failure. Final before-and-after
measurements and retained regression results are in tests/VALIDATION.md.

# IRONFIELD 1.9 — armor counters, prepared pikes, and hoplite pressure

## A counter system for every troop

All eighteen troop types now have an explicit Light, Medium, or Heavy armor
class and a weapon profile. The profile multiplies damage after armor-point
protection, alongside the existing penetration, shield, formation, facing,
terrain, and charge rules. Ordinary arrows and sling stones favor light targets;
crossbow bolts, ballista bolts, and piercing poleaxes gain advantages against
heavy armor. Every troop has contextual strengths, threats, and use advice.

Army cards and the selected-formation panel open a shared matchup guide. Its
attacker and defender selectors show the exact class factor, armor penetration,
and applicable base mounted bonus separately. The full table compares all
eighteen troops against each armor class. These values come from the same data
used by combat. The archive also includes the complete reference in BALANCE.md.

## Set pikes against a frontal charge

Pike Guard gain a directional prepared defense against mounted troops. A
settled, cohesive line or shield wall can intercept a charge at pike reach,
interrupt its momentum, and punish riders while reducing frontal pressure.
Brace direction follows the formation rather than each soldier's immediate
look direction. Moving, charging, turning, loose order, disorder, and flank
attacks prevent the same full prepared advantage. The ordinary mounted-damage
bonus remains outside a complete brace.

## Bronze Hoplites hold and press the line

The new hoplite has medium armor, a large frontal shield, a long spear, and
firmer physical rank discipline. Supported, cohesive fronts can push lighter
opponents through bounded physical movement. Pressure respects terrain,
occupied bodies, mass, and available space; heavier targets and blocked ground
can stop it. A fresh movement or withdrawal order retains priority over the
automatic pressure behavior.

Bronze shields, crested helmets, cuirasses, greaves, and six spear-and-shield
attack poses give hoplites their own silhouette. Successful physical pushes
drive a brief shield-forward pose, a SHIELD PRESS standard label, and a recent
pushing count. Their portrait, formation icon, and temporary fallen body are
distinct as well. Existing troop artwork and body-lifetime limits are retained.

All eight ready-made battles include hoplites by substituting existing infantry,
without increasing their total army sizes. The default stays at 2,400 troops;
War of Twelve Thousand stays at 6,000 per side and includes all eighteen classes.
Custom placement, facing, splitting, and scenario persistence include the new
type through the shared roster.

Release verification and its limits are recorded in tests/VALIDATION.md.

# IRONFIELD 1.8 — coherent marches, facing, and local fronts

## Begin from the arranged ranks

Both armies use a consistent formation coordinate frame at deployment and
while marching. Saved soldier positions are captured into the formation's
local frame, preserving existing layouts as the advance begins. Facing changes
and the transition into a fighting front are staged through physical movement.

Automatic approaches use local opponents and stable local directions. A wing
arranged around an enemy can maintain its inward approach while other wings
work different fronts. Contact deployment and nearby reserve movement activate
where the fight requires them. Formation flexibility, terrain routes, and real
passing gaps continue to operate when needed.

On open ground, faster automatic formations overtake beside a slower friendly
body. The leading formation can keep its width and depth instead of reversing
to construct a long passing column. Narrow terrain and opposing traffic retain
their physical passing lanes. Automatic melee troops can press into free
assigned contact space while fighting, helping reserves reach the front.
Missile troops under Charge also complete their assigned firing-band approach
while shooting, allowing rear throwers to reach firing positions. The command
retains its fixed firing position and prevents automatic skirmish retreat.

## Face a direction explicitly

**Face / T**, then a direction click, sets orientation at the current position.
The point can lie over terrain because it specifies a direction. Selected
formations show a current-facing arrow and compass label; a short gold preview
shows the requested turn.

Deployment Face rotates the existing footprint when there is clear space.
During battle, Face performs a visible turn and physical re-formation, then
holds the position. Paused orders execute on resuming. Fresh movement and
withdrawal orders can replace the turn. Scenario saves include facing together
with the exact positions of each deployed soldier and split group.

The archive includes `examples/encirclement.ironfield.json`, a six-formation
drill with four inward-facing wings and two defending groups. The README
explains importing it and applying defensive Hold orders after starting.

## See elephant strikes in the overview

Elephant release and follow-through poses remain visible in low-detail,
zoomed-out rendering after actual strikes. The renderer uses the existing
cached sprites and the same number of sprite draws. In-place facing and
withdrawal have separate feedback so their arrows and labels show the actual
order.

## Verification

The final source passes **51/51 engine gates**, **23/23 interface checks**,
**13/13 graphics checks**, and all five phases of the separate
**12,000-soldier command exercise**. All 44 previous engine test bodies and
their assertions are unchanged.

Both armies advance 280.5 world units during the ten-second straight-march
check, with zero relative rank distortion or lateral drift. All 80 riders
overtake moving infantry while its depth stays at 153 units and no leading
soldier walks backward. In the four-sided envelopment, every wing advances
inward without early sideways drift and 22–30 soldiers per wing land real
hits against the central defenders.

New checks also cover elephant hits, deployment/battle facing, command changes
during overtaking, and splitting while marching or turning. Interface tests
exercise the real Face button, T key, direction clicks, and saved layouts.
Four new graphics checks cover facing and overview elephant strikes. The
retained ranged-Charge check records all 16 javelin throwers firing, with no
automatic retreat. Final source hashes, measured results, and validation
limits are in `tests/VALIDATION.md`. These headless checks do not measure
browser rendering FPS.

# IRONFIELD 1.7 — missile formations, living combat, and custom encounters

## Missile troops share usable firing space

Ranged formations use bounded firing sectors and individual firing slots.
Rear soldiers can enter available shooting space instead of waiting at the
back of an extended queue. Javelins use loose short-range skirmish bands with
brief local movement between throws; archers hold broad long-range ranks;
crossbows plant, fire, and reload while favoring armored targets. Slings,
mounted archers, and siege batteries retain their own movement and firing roles.
Firing ranks leave real gaps for reserves, and automatic shooters yield locally
to allies approaching those passages.

Active commands keep priority over automatic repositioning. Existing terrain,
body collision, command response, and retreat rules remain enforced.

## More visible combat

Six staged weapon poses replace the old two-frame attack flash. Javelins,
bows, crossbows, sling stones, melee weapons, mounted attacks, and siege engines
have distinct attack and recovery choreography. Animation follows simulation
time and pauses with the battle.

Fallen troops use cached silhouettes of their bodies and equipment. They remain
for fourteen simulated seconds, fade over eight more, and disappear at twenty-two.
Visible corpse drawing is capped for large battles and can be disabled in settings.

## An empty custom encounter

Custom Map and the custom encounter option now clear both rosters and terrain.
A New empty encounter button also starts fresh directly from the army builder.
Pending roster rebuilds cannot restore the previous army afterward.

The editor provides forest, hill, rock, water, bridge, and erase tools, terrain
undo, ground appearances, and click-to-place formations for either army. Paint
terrain by dragging; bridges span the water they touch. Existing troops must
be moved before solid terrain can be placed over them.

## Split formations into separate commands

Split / B partitions selected formations by physical side while preserving
soldier identities, positions, health, casualty ownership, and army totals.
Both groups inherit their current orders; Move and point Attack get separate
nearby destinations so the halves do not immediately converge on one point.
Each half can then be selected and commanded independently.

Scenario version 2 preserves custom terrain, deployment formation groups, and
individual deployment positions; version 1 imports remain supported.
Deployment splits keep their physical spacing through saving and reset.
Midbattle splits affect the current fight while scenario saves retain its
starting deployment.

## Verification

The frozen source passes **44/44 engine gates**, **19/19 interface checks**, and
**9/9 animation/body checks**, plus all five phases of the **12,000-soldier
command exercise**. Existing collision, command, retreat, and battle pacing
thresholds remain intact.

In the controlled missile scenarios, all **640 javelin throwers** fire on both
the open field and the desert approach, including all **320 initially rear
soldiers**. The earlier release reached 86 and 96 distinct shooters in those
respective fixtures. These checks also require sustained broad firing fronts,
actual projectile damage, bounded movement, and physical body separation.

In the retained mixed-army desert battle, median stationary, nonattacking
reserves fall from the previous release's **58.6% to 10.8%**. Distinct melee
soldiers delivering real hits increase from **871 to 1,009**. A separate
reinforcement check sends **32/32 infantry** through friendly firing ranks
while all **80 archers** continue shooting.

The interface checks exercise blank-map creation, terrain editing, placement,
split commands, roster increases, scaling, and exact deployment restoration
through Save, Load, and Reset. Final-source results, exact hashes, the bounded
12,000-soldier command exercise, and scope limits are recorded in
`tests/VALIDATION.md`.

# IRONFIELD 1.6 — flexible ranks, local fighting, and deliberate retreats

## Fight the enemy actually within reach

Automatic soldiers give immediate weapon-range threats priority even when the
opponent belongs to a different formation than their regiment's assignment.
Held soldiers can defend their own positions without pursuing the enemy or
returning to the banner first. The formation still coordinates its wider
approach across the battlefield.

Directional commands control movement. A nearby unrelated opponent cannot
stop a Charge toward the chosen enemy or pull an ordered approach sideways.
Incidental defense can continue while moving, subject to the same collision
and weapon-range rules. Point Attack aligns advancing ranks with the ordered
route and engages enemies that block that approach.

## Let ranks bend around pressure

Formation positions allow local adjustment around occupied space and terrain,
then recover the chosen shape when the route opens. The solid selection rim
tracks actual soldier positions throughout battle at medium and close zoom;
the faint guide shows the intended shape. Cohesion and formation bonuses still
depend on the physical ranks.

Navigation checks the full intersection with rock corners and bridge openings.
An off-center approach can turn into a nearby passage instead of repeatedly
trying to cut through a corner missed by the old sampled route check.

## Turn, then withdraw

Manual Retreat has a short visible turning phase before withdrawal. Soldiers
face their escape direction, disengage from immediate danger, and then seek
friendly support. The order panel, formation standard, and withdrawal arrow
show the transition. Turning uses simulation time and remains frozen while
paused.

Retreat also works when troops are already falling back or are regrouping.
Repeating the order continues the withdrawal without restarting the turn or
withdrawal timers. Existing commander recovery and recoverable fallback remain
part of the battle. Automatic recovery respects the initial manual withdrawal;
an explicit commander rally can still call the troops back to regroup. Soldiers
reorganize from their current positions after escaping, keeping withdrawal
moving instead of stalling while front and rear rows try to swap places.

## Verification

New regression scenarios cover local contact against an unassigned enemy,
directional orders beside a distracting opponent, deliberate withdrawal,
repeated and paused retreat orders, retreat from a rear-side threat, and an
off-center rock passage. The final source passes **38 engine regressions** and
**12 interface checks**, retaining all 32 earlier engine assertions.

In the engaged-retreat fixture, all eleven recent fighters withdraw within a
simulated second, and their mean withdrawal reaches 407 world units by ten
seconds. A fresh Retreat also keeps nineteen survivors of an automatic fallback
moving despite their earlier recovery timer. All 160 infantry traverse the offset rock
passage, then recover their frontage and cohesion.

The included 12,000-troop command exercise adds a fifth phase for turning and
retreating, with repeated orders during both phases. All five phases pass;
all eleven observed front formations withdraw within one simulated second,
and all 1,370 tracked soldiers make retreat progress. It measures actual front
soldiers and simulation state rather than browser rendering speed. Final results,
source hashes, and reproduction instructions are in `tests/VALIDATION.md`.

# IRONFIELD 1.5 — responsive orders and committed charges

## Give each charge a clear target

**Charge / C** now immediately chooses and commits to a nearby enemy formation.
The approach faces that target instead of retaining an earlier battle-line
direction. **Target / Shift+C** lets the player choose a particular enemy,
including an opponent farther away than another nearby formation. Losing the
target to casualties or a rout triggers a new assignment so the charge can
continue.

Missile troops close to firing range and fire under Charge. The order suppresses
their automatic withdrawal from approaching enemies while preserving their
normal ranged weapons.

## Replace stale movement instructions

New orders discard previous paths, local detours, traffic assignments, combat
targets, and reinforcement goals. Move can disengage soldiers already attacking;
manual Hold captures where the soldiers actually stand. Marches begin with
the soldiers' current physical spacing and gradually align the new ranks.
This prevents a fresh order from first sending rear ranks backward to complete
a perfect layout. Combat positions, terrain, and actual passing maneuvers still
take precedence. Collision, movement speed, and friendly yielding still apply.

Group movement preserves a two-dimensional arrangement and fits the selection
inside the battlefield. Terrain-projected destinations are separated instead
of stacking many banners onto a map edge. Manually moving or charging troops
advance before distant faster followers ask them to open passing lanes,
including when only the front line is selected and rear allies remain on Auto.
When those followers really close the gap, a moving lead formation can open
lanes diagonally while advancing. Followers must still wait for physical space.

## Make accepted orders visible

The command bar keeps the current order highlighted and displays the order,
target, and state of selected formations. Charge arrows point toward the
committed opponent. Paused commands receive an immediate acknowledgement;
partial acceptance reports how many formations accepted and when others are
withdrawing, regrouping, or allied to the chosen target.

Right-click now confirms an armed Move or targeted Charge. Ordinary right-click
still issues Attack-move. Changing a formation returns keyboard focus to the
battlefield, and Escape cancels an unplaced order while retaining the selection.

## Verification

Command regressions measure actual soldier movement, target commitment, and
damage after orders, including commands issued during combat and while paused.
A separate interface integration suite exercises the shipped application's
registered input handlers and immediate feedback against the real engine.
The final source passes **32 engine regressions**. The interface integration
suite separately checks nine command-input and feedback cases.
In the focused fixtures, all nine soldiers already in melee begin disengaging
within half a simulated second, all 160 marching soldiers stop under Hold,
and all fifty selected formations make useful progress within two seconds of
a group Move. The command tests also verify actual damage to chosen enemies,
ranged firing, target-defeat continuation, and cancellation by subsequent orders.

An additional natural-deployment fixture covers the larger-army delay discovered
during verification. It tests Charge for both the whole army and only the front
formations while rear allies remain on Auto. All five observed front formations
advance usefully within the first simulated second in both cases. The gate
measures movement toward actual enemy positions, not accepted-order counters.

The existing crowd, terrain, collision, formation, recovery, and large-army
gates remain enabled. The complete measurements and test boundaries are in
`tests/VALIDATION.md`.

# IRONFIELD 1.4 — clear crowded approaches and commit waiting troops

## Fix persistent marching files

Terrain adaptation now measures the whole usable corridor and permits a
sideways shift into its open space. Individual local paths retain lateral
separation around corners. Allied passing plans remember their normal lane
capacity and widen again when an obstruction clears.

Contact recognition includes the soldiers' occupied bounds. The leading troops
in a stretched formation can acquire nearby enemies and deploy for combat even
while the formation's center remains far behind. This also releases obsolete
passing plans at the battle line.

Ranged formations also leave passing lanes on reaching firing positions. If
their assigned enemy is outside weapon range, individual missile troops can
select another nearby enemy they can actually hit.

## Bring inactive melee troops into combat

Waiting melee troops can seek openings around crowded allies and reinforce
reachable contact beyond their regiment's original target. The approach accounts
for terrain and friendly congestion; nearby attack positions use temporary
reservations to distribute arrivals. Soldiers keep these movement commitments
while their regiment updates its normal formation slots.

This behavior preserves explicit move and hold orders, withdrawal, recovery,
physical body collision, and speed-limited movement. Formation goals and bonuses
continue to apply. Per-strike damage is adjusted for increased participation so
the established battle-pacing check remains meaningful.

## Test the reported congestion

A new mixed-army desert regression sends complete formations through shared
rocky approaches. It measures actual attacks linked to target health loss,
troops that neither move nor attack, and the principal-axis width of persistent
idle files. This catches the queue failure missed by the earlier tests with
independently placed formations at separate approaches.

The previous version fails this new check. The final v1.4 source passes all
**24 regression checks**. In the same 2,340-combatant, 120-second crowded desert
fixture, distinct melee soldiers delivering observed hits increase from
**266 to 1,035**, and the final ten-second attacker count increases from
**60 to 317**. No regiment remains a persistent idle near-single-file tail in
the measured late-combat windows.

A separate ranged-target regression verifies that all sixteen held javelin
troops fire on a reachable alternate enemy without abandoning their position.
The default 2,400-combatant battle finishes naturally at approximately
**341 simulated seconds**, with recoverable fallback and commander rallies.

In the matched 12,000-combatant desert replay at 120 simulated seconds, troops
still assigned to one-file passing plans fall from **5,683 to zero**. Severe
idle thin formations measured from actual soldier positions fall from **eight
to zero**, while recent attackers across all classes rise from **1,528 to 3,009**.
Terrain, body clearance, finite state, and troop conservation checks remain
clean throughout that bounded run.

The final measurements, physical-passage checks, and large-battle observations
are documented in `tests/VALIDATION.md`.

# IRONFIELD 1.3 — broader battles and expanded armies

## Combat spreads across the front

Regiments distribute their pressure across available enemy formations and
approach distinct sections of the front. Individual melee troops seek reachable
contact positions and less contested targets. Rear ranks can feed openings and
work toward free flanks instead of repeatedly pursuing one crowded point.

Forest and terrain approaches account for movement cost and local crowding.
Temporary marching shapes return to fighting frontage as the engagement opens.
Physical soldier collision, friendly passage, recoverable fallback, and
commander rally remain part of the simulation.

The selection panel shows each formation's tactical role and active attackers.
Activity counts soldiers who attacked during the last 1.5 simulated seconds;
this is a measured combat count, rather than the number assigned to a target.

## Four additional troop types

- **Crown Halberdiers:** armored polearm infantry with armor penetration and
  anti-cavalry strength.
- **Javelin Skirmishers:** mobile troops with bucklers and throwing spears.
- **Levy Slingers:** inexpensive missile troops with slings and ammunition
  pouches.
- **Ironwind Ballistae:** wheeled crossbow engines firing heavy armor-piercing
  bolts.

Each has its own animated equipment, silhouette, portrait, and combat data.
The roster now contains seventeen troop types.

## Three additional battlefields

- **Greenwood Vale:** woodland patches separated by navigable glades.
- **Broken Crown Pass:** rocky ridges with several usable approaches.
- **Ashen Basin:** central heights and broad open flanks.

New preset battles showcase these maps and troops. Existing headline army
sizes remain unchanged, including the 2,400-combatant default and the
12,000-combatant epic scenario.

## Verification

The frozen v1.3 simulation and content passed **22 automated checks**, including
the existing collision, passage, formation, fallback, commander, input-bound,
determinism, and scale gates.

The complaint-reproducing scenario uses four attacking infantry regiments
stacked in depth against four defending formations. Compared with v1.2, the
new simulation increased distinct melee attackers from **189 to 357** and the
largest ten-second attacker count from **101 to 171**. Actual damage reached
all four defender formations. The strengthened check also requires sustained
width within individual engagements, rather than counting several small piles
as a broad battle.

Separate terrain fixtures recorded active fighting through all three ridge
approaches and sustained attacks in woodland. Movement remains gradual and
physical; existing no-ghosting and complete allied-passage checks still pass.

The default 2,400-combatant battle completed naturally at approximately
232 simulated seconds. The reduced per-strike damage preserves the opening
survival check while allowing more soldiers to participate.

The final validation report includes exact source hashes, natural battle
completion, large-army measurements, and practical limits in
`tests/VALIDATION.md`.

# IRONFIELD 1.2 — flexible movement and friendly passage

## Formations adapt to their route

- Regiments temporarily narrow and deepen around obstructed terrain and in
  woodland, then restore their chosen combat formation after clearing it.
- Individual soldiers route around blocked ground and keep physical separation.
- Friendly ranks yield to let passing soldiers weave through real gaps. Crossing
  traffic slows movement and disrupts cohesion.
- Selected formations show their changing footprint and current movement state.
  Walking animation follows actual distance, including stops while waiting.

The chosen formation, its combat tradeoffs, recoverable fallback, and commander
rally controls remain available. Movement adaptation is automatic.

## Verification

The final v1.2 source passed all **17 automated checks**, including the existing
combat, commander, fallback, and scale checks. The new movement fixtures verified:

- **160 of 160 infantry** crossed the intended gap between rocks, with physical
  separation, reduced frontage, and recovery of the original formation.
- **24 of 24 cavalry** passed through gaps opened by a held allied pike shield
  wall. All riders cleared; none remained stranded in the wall.
- Two allied formations crossed and reached their opposite destinations, then
  regained cohesion.
- Cavalry narrowed and took longer to march through woodland than through an
  otherwise comparable open route, then restored their frontage.
- Per-step movement bounds rejected instant jumps, while continuous relative
  motion checks enforced separation during crossing.

The real cavalry passage was also rendered and visually checked while troops
were between the pike ranks. `tests/VALIDATION.md` records the exact measurements,
large-army checks, and limits.

# IRONFIELD 1.1 — formations and commanders

This build includes the requested changes to battlefield pacing, crowd movement,
formation advantages, and recovery after retreat.

## Soldiers occupy space

Movement is checked against neighboring soldiers from both armies. Soldiers
stop, slide, and flow around obstructions instead of passing through an intact
rank. The simulation checks the movement segment, not just its endpoint, to
prevent fast troops from skipping through another soldier between frames.

A blocked formation slows down and its banner follows the actual troops.
Displacement from assigned positions, collisions, and flanking pressure reduce
cohesion. Regiments have to reform to regain their full formation advantage.

## Formations affect the battle

Select one or more regiments and use the Formation menu. Deployment changes
arrange the soldiers immediately; changes during combat make them move into
their new positions. Changing formation in combat temporarily reduces cohesion.

| Formation | At full cohesion | Tradeoff |
| --- | --- | --- |
| Battle line | 4% less frontal melee damage taken | A balanced general-purpose formation |
| Shield wall | 18% less frontal melee damage and 28% less frontal ranged damage taken | 28% slower movement, 10% lower attack damage, 20% less charge impact |
| Assault wedge | 30% more charge impact, 10% faster movement | 10% more melee damage taken |
| Loose order | 28% less ranged damage and 40% less splash damage taken; 5% faster movement | 15% more melee damage taken and a wider footprint |

These modifiers weaken as cohesion falls. The selection panel shows current
cohesion and the corresponding effective modifiers.

## Falling back is recoverable

The **Fall back / R** order gathers surviving troops near friendly support.
They remain in the army and can reform. They retain their regiment identity
for orders and casualty accounting; regrouping does not transfer them to the
enemy or erase their losses.

Automatic morale failures also begin as a fallback. Nearby friendly formations
and commanders help them recover. A full rout is reserved for a sustained,
unsupported collapse under severe losses. A manually withdrawn formation that
recovers passively holds its new position; an active commander rally can return
it to automatic service.

## Commanders are a distinct troop type

Field Commanders ride white horses and carry gold-trimmed armor, capes, and
crowned standards. The default armies each include two; the epic armies each
include four. Custom armies may contain up to sixteen commanders per side.

- **J** or the **Commander** button selects and focuses the next commander.
- **V** or **Rally** rallies nearby shaken and falling-back formations.
- The active rally radius is **620 world units**, shown by the gold circle.
- Passive support extends **360 world units**, shown by the inner circle.
- Rally has a **45-second simulation-time cooldown**.
- Rally restores morale and cohesion; it does not resurrect dead soldiers.

The army display tracks falling-back troops separately from permanently routed
troops and counts successful returns to service. Repeated recoveries contribute
to the cumulative rallied count.

## Verification

The updated simulation passed fourteen automated checks, including friendly
and enemy collision, bridge crossing, cohesion loss, formation effects,
commander recovery, passive regrouping, deterministic simulation, and army
limits. The browser interface was also exercised for selection, formation
changes, withdrawal, and a commander rally returning 160 troops to service.

Detailed measurements and the limits of the checks are in `tests/VALIDATION.md`.
