# IRONFIELD 2.6 — Fire by ranks

The Campaign Atlas now includes **20 historical battles**, with searchable eras, named armies, tactical opening deployments, and source notes. Select **Historical battles**, read the deployment briefing, then **Prepare This Battle** to command the opening or **Quick Battle** to start immediately. The scenarios use scaled forces and the available troop classes; every battle explains its approximations in the historical notes. See [HISTORICAL-BATTLES.md](HISTORICAL-BATTLES.md) for the source catalog.

Musketeers now reload for **24 seconds** and deal **78 base damage** per shot. Their companies fire synchronized volleys by rank, with eight seconds between ranks in a three-rank line. **Three-rank volley** spreads the frontage; **Three-file column** makes a narrow formation with three soldiers abreast. Close combat breaks fire discipline, and survivors reform after the attackers clear. The selection panel shows the next rank and firing status. Cannon and trebuchet impacts throw troops much farther, with catapults also scattering packed ranks.

The menus use a new campaign atlas layout. All 23 troop classes have readable equipment, team livery, movement and attack poses. Biomes have distinct ground details, and bounded wind, water, and vegetation movement follows simulation time and the animation settings.

## Massive battle simulator

Assemble two armies, position their formations, and watch individually simulated soldiers fight. Take command of either army—or both—while the opposing battle plans continue to run. Twenty-three troop and engine types, including bronze hoplites, field commanders, assault towers, and covered rams, have different equipment, silhouettes, statistics, and battlefield roles. Light, medium, and heavy armor interact with each weapon's damage profile. Physical ranks, formation cohesion, and recoverable retreats give you time to respond as a battle develops.

## Try the new weapons

Choose **Sieges → Stonekeep Siege → Thunder at Stonekeep**, then **Quick Battle**. Berserkers face the garrison outside the west gate. Musketeers fire from behind the infantry, while cannons and trebuchets can bombard the defenses. Select a gun or trebuchet formation to check its reload.

The army builder has **All**, **Infantry**, **Cavalry**, **Ranged**, and **Siege** tabs. Each tab also filters the troop picker in a custom encounter. Mounted archers, commanders, and elephants appear under Cavalry.

## March, disengage, and fight for ground

Right-click open ground or press **M** and choose a destination. Troops leave melee on the next simulation step, keep their current spacing where the route allows, and form up at the destination. Right-click an enemy position to attack there, or press **A** for an attack-move. **R** breaks contact immediately and seeks friendly support. You can redirect a withdrawing formation with **M**.

In the battlefield menu, **Siege** includes **Stonekeep Siege**, **Floodgate Citadel**, and **The Three Crowns**. These castles enclose their garrisons with walls, corner towers and gates. Select a defending formation and right-click or tap a castle gate to operate it. Both armies can pass an open gate. An occupied doorway must clear before the gate can close, and a destroyed gate stays open. Rams and deployed siege towers provide other ways in.

Under **Custom Encounter → Shape the battlefield → Castle**, click to place a square keep or drag to draw a rectangular enclosure. The gate faces west. Choose its owner with the defender selector. One Undo removes the entire placement.

Ranks in physical contact behind a fighting line can add weight to its shove. The receiving front and its backing move gradually as pressure builds. Bracing, mass, walls, and other bodies affect how far a line can yield. A rout requires at least 80% losses and very low morale.

## Try a packed lancer charge

Import `examples/lancer-convoy.ironfield.json`, select both blue companies, start the battle, and press **C**. The two companies occupy alternating ranks. Watch the riders at the back accelerate as the ranks ahead of them move toward the levy line.

## Friendly riders make room

Lancers, Royal Cavalry, and Steppe Outriders now open small sideways gaps when friendly horses crowd their route. Riders keep the gap open while the following files pass and gently recover their spacing afterward. These are real, visible steps through available ground; the horses keep building forward speed through friendly brushes.

During a clear charge, front riders advance before their following files, letting the rear use the real space the leaders have vacated. Dense mounted formations keep their approach lanes until they near the enemy.

Assigned passing routes keep priority at crossings. Once cavalry engages the enemy front, its supporting ranks retain their combat positions instead of continually shuffling sideways.

A held horse can take a limited sideways courtesy step while retaining its facing and new holding position. Fresh Move, Hold, Face, and Retreat orders replace the old interaction; turning riders complete their turn first. Enemy impacts, prepared pikes, and blocked terrain still consume real momentum. Sideways movement supplies no extra charge damage.

## Faster horses and yielding battle lines

Horses now have a substantial movement advantage over infantry. Base speeds are **150** for Royal Cavalry, **132** for Banner Lancers, and **162** for Steppe Outriders; mounted commanders also use **150**. Foot troops keep their existing speeds. Lancers are deliberately slower than the other horse troops while still easily outpacing infantry. Acceleration, wheeling, forests, crowded passages, and physical impacts continue to affect actual travel.

Stronger forces can gradually drive a connected enemy formation backward during sustained contact. Engaged frontage, supporting ranks, troop mass, quality, cohesion, and a correctly facing brace determine which line yields. The rear ranks step back with the front, maintaining the formation's facing as real space allows. Evenly matched lines resist, and rocks or heavy troops behind the defenders can halt the displacement. A distant straggler is not dragged along with a disconnected group.

Yielding troops use short backward or sideways steps while keeping their weapons toward the enemy. A **GIVING GROUND** banner appears when the line is actually being pushed and no higher-priority command or morale status is displayed. New movement, facing, and retreat orders clear the previous pressure. Retreat starts moving on the next simulation step. A **Hold** order stops that formation's active advance but does not make it immune to a stronger enemy.

## A larger war table

The game opens at a main menu with **Quick Battle**, a browser for all **22 authored battlefields**, **Custom Encounter**, saved scenarios, the field manual, and settings. Select a field and choose **Prepare This Battle** to arrange the armies, or use Quick Battle to start its scenario immediately. **Menu** in the top bar pauses the current battle; **Resume Battle** returns to the same troops, positions, selection, and accepted orders. A battle that was already paused stays paused. The menu performs no simulation or battlefield rendering in the background.

Twelve additional maps bring the total to **23 choices including Custom**, with **24 prepared scenarios**. Maps are labeled **Standard**, **Large**, **Huge**, or **Colossal**, with their area multiplier shown in the battlefield browser, scenario and map selectors, custom editor, and saved encounters. The largest maps appear first. Choose **Large maps · 12** in the main menu to show the twelve larger fields; if the current selection is Standard, this also selects a matching large field for Prepare This Battle and Quick Battle.

The full-width battlefield picker shows map size before the name, with dimensions beneath it. **Army scale · Soldier counts** changes the number of troops; choose a larger battlefield to increase map area.

| Battlefield size | Dimensions in world units | Area relative to the original |
| --- | ---: | ---: |
| Standard | 6,400 × 4,400 | 1× |
| Large | 9,600 × 6,600 | 2.25× |
| Huge | 12,800 × 8,800 | 4× |
| Colossal | 16,000 × 11,000 | 6.25× |

Use the custom editor's **Battlefield Size** choice for any of these dimensions. Enlarging the field preserves all existing troop positions and terrain. Shrinking is allowed when the whole layout fits; otherwise the editor keeps the current field and asks you to move the outlying content. Scenario saves and imports retain the selected dimensions, including deployments beyond the old map boundary. Changing to a different authored battlefield moves formation centers to the corresponding relative locations on the new field while preserving actual rank spacing and facing; this differs from enlarging the same custom encounter, which keeps absolute coordinates.

Six new plans expand the strategy menu to **14**. **Steady Advance** keeps a deliberate, cohesive infantry front with partial protection while marching. **Rapid Encirclement** groups nearby enemy squads into bounded local positions, assigns a pinning front and separate wings, and surrounds the whole selected position. An isolated enemy formation is treated as its own objective. Select a regiment to see its current phase, objective scope, and squad count. **Refused Flank**, **Concentrated Breakthrough**, **Elastic Defense**, and **Echelon Assault** give different staged movement and reserve commands.

In **Settings**, set **Fallen Soldier Persistence → Entire Battle** to leave casualties on the ground in accumulating piles until the encounter is reset. **Long** keeps them for two minutes; **Brief** retains the original 22-second lifetime. Full animations add distance-driven marching, faster mounted gait, couched charges, moving brace poses, shield reactions, hit recoil, and falling poses. Reduced animations keeps essential combat poses with less drawing work.

## Stronger mounted breakthroughs

Banner Lancers clear weak infantry more forcefully and carry their advance through successive ranks. Royal Cavalry also receives a substantial physical push against light foot troops. Speed, mass, supporting ranks, facing, and bracing still determine what the charge can displace. Low-mass light infantry can spill out of the horse's corridor, leaving a real opening for following riders.

Give riders a straight run-up. Short approaches have much less effect; prepared pikes, formed hoplites, heavy infantry, and solid terrain still resist the charge. Rear ranks absorb and receive diminishing pressure. Every body's horizontal movement remains collision checked and bounded. Fresh Move, Hold, Face, Charge, and Retreat commands clear the old follow-through.

Import `examples/lancer-breakthrough.ironfield.json` through **Scenarios** for a compact drill: twelve lancers face fifty-six levies in four ranks, with one rear guard on each side. Select the riders and use **Charge / C**, or start the selected automatic battle plans. The validation report records observed penetration, displacement, and counter results on the final release.

## Cavalry momentum and siege assaults

Cavalry now build speed through a real run-up. Royal Cavalry, Banner Lancers,
and Steppe Outriders accelerate from rest; sharp turns, terrain, blocked travel,
and impacts reduce their actual speed. The selected riders' speed meter shows
how much momentum they have. Horse archers retain their ranged role; cavalry
and lancers can plunge into an opposing line.

A fast physical contact delivers damage based on closing speed squared and
pushes the struck body. Light, unsupported infantry can be thrown into the air.
Their shadow stays on the ground while their body rises, turns, and lands;
casualties finish that motion before becoming fallen sprites with the chosen persistence setting.
Horizontal displacement uses the same collision and terrain constraints as
normal movement. A charge cannot send bodies through rocks, gates, walls,
other soldiers, or the battlefield edge.

Supporting ranks absorb the pressure and are pushed in turn with less force.
A backed front rank therefore moves less than an isolated target. Formed
hoplites resist strongly and can be driven back without being casually thrown
aside. Prepared, correctly facing pikes intercept the horse before it reaches
body contact and remove much of its momentum. Deep ranks can stop a charge;
a thin screen can be pierced as cavalry move into physically cleared space.
Every impact slows the horse, so penetration is earned by speed and available
space. Friendly riders open sideways gaps and preserve forward acceleration
while a pass is progressing. Hard obstructions retain only a bounded walking
effort; steering and sideways steps cannot create charge energy.
Their formation guide stays near the physical ranks while they accelerate and
limits wheeling when the soldiers are still catching up. Fresh commands still work: **Hold** stops a march, and **Retreat** turns the riders while they withdraw and build speed in the new direction.

The movement loop reuses measured travel distance and skips horse-only work
for ordinary infantry. Every moving body still contributes to the physical
displacement budget, so a soldier struck later in the same simulation step
cannot receive extra movement by bypassing collision limits.

The original siege encounters introduce actual fortifications; three larger fortress assaults now extend them:

| Encounter | Assault problem |
| --- | --- |
| **Storm the Stonekeep** | Breach the west gate, flank the north gate, or escort siege towers to the square curtain wall. |
| **The Siege of Two Bridges** | Cross a moat on two narrow causeways, breach the gates, and bring reserves into the defended interior. |
| **Assault on Crownwatch** | Bring columns through rock corridors into a bastion with two defended entrances and a central tower approach. |

**Assault Siege Towers** have a tall timber silhouette, braced stories,
a wheeled base, bowmen, and a lowering bridge. A tower must physically reach
an enemy wall, then complete a **four-second deployment** before its assault
ramp opens. The new passage joins the real navigation map. Troops climb its
sloped deck, cross the wall, and descend on the other side. A deployed tower
serves as a firing platform. The established ramp remains usable if the tower
is subsequently destroyed or moved, so troops already on it retain a route.

**Covered Battering Rams** have a roof, crew, wheels, and a suspended log that
visibly retracts and strikes. They must reach the fortification before doing
structural damage. Rams are especially effective against gates; their slower
wall damage is useful when no gate approach is available. Catapults and
ballistas can bombard structures with real projectiles. Ordinary melee troops
can help batter an entrance, but an escorted ram opens it much faster.

Select an assault formation and **right-click an enemy wall or gate** to assign
that structure. The selection panel shows the work stage, target health, and
tower deployment progress. **Auto / G** chooses useful siege work when an
approach is available. **Move**, **Hold**, **Face**, and **Retreat** replace that
job immediately. Equipment is individually selectable: one tower or ram per
formation, with at most **12 of each per army**.

Gates and walls visibly crack as they lose health. A breach leaves rubble and
opens the ground for every unit. Narrow approaches still have physical limits:
units narrow their ranks, wait for space, and spread after the passage. Siege
engines use body collision without forcing an entire infantry company into a
single-file formation just to pass one machine. Stagger reserves and cover the
engines while the assault creates enough room.

The custom encounter editor includes **Castle**, **Wall**, and **Gate** brushes and a
**Wall & gate defender** choice. Painting a gate across a wall removes the
masonry under that door. Wall ownership and initial health survive scenario
save/export/import; loading restores the starting siege rather than its
mid-battle damage. All original field battles and troop rosters remain available.

## Play

1. Extract the ZIP into a folder.
2. Double-click **IRONFIELD.html** to open the complete game in a modern desktop browser. You can also open **index.html** from the extracted source folder; keep the `src` folder beside it, with all seven scripts and `style.css` inside that folder.
3. On the main menu, choose a battlefield and **Prepare This Battle**. The initial **Clash of Kingdoms** scenario fields 1,200 combatants on each side; **Quick Battle** starts the selected scenario immediately.
4. Adjust either army, choose its battle plan, and select a formation on the field to reposition it before battle.
5. Press **UNLEASH THE ARMIES** or **Space**.

The game runs entirely in your browser. It needs no installation, account, network connection, dependency download, or local server. Desktop mouse and keyboard controls are recommended for the largest battles. Touch controls are also provided.

Use the mouse wheel to move from a view of the whole battlefield to individual soldiers. The army builder and field manual also show portraits of each troop type.

## Controls

| Action | Control |
| --- | --- |
| Open or resume the main menu | **Menu** in the top bar; **Resume Battle** returns to the field |
| Select a regiment | Left-click a soldier or formation |
| Select several regiments | Drag a selection box |
| Add several formations to the selection | Shift-drag |
| Add or remove a single formation | Shift-click |
| Reposition during deployment | Select formations, then right-click their destination |
| Face a direction at the current location | **T**, then click the direction, or use **Face** |
| March or attack during battle | Right-click open ground to march. Right-click an enemy position to attack there or a hostile gate to assault it. |
| Move to a point | **M**, then left-click or right-click a destination |
| Attack-move to a point | **A**, then left-click or right-click a destination |
| Hold the soldiers' current positions | **H** |
| Assign a siege target | Select an assault formation, then right-click a hostile gate or wall |
| Immediately charge a nearby enemy | **C** or **Charge** |
| Choose an enemy formation to charge | **Shift+C** or **Target**, then click an enemy formation |
| Resume the army's automatic battle plan | **G** |
| Choose or change an army strategy | **Battle plans** above the field |
| Disengage immediately and retreat toward safety | **R** |
| Select a field commander | **J** |
| Rally nearby troops with the selected commander | **V** |
| Change selected regiments' formation | Formation menu: line, shield wall, wedge, or loose order |
| Split selected formations into independent groups | **B** or **Split** |
| Start, pause, or resume | **Space** |
| Zoom | Mouse wheel; the **+ / −** buttons also work |
| Pan the camera | Arrow keys or middle-mouse drag |
| Pan when no formations are selected | **W A S D** |
| Move the camera quickly | Click or drag on the tactical map |
| Show the whole field | **F** |
| Cinematic view | **Tab** |
| Command Azure / command Ember / spectate | **1 / 2 / 3** |
| Command both armies | Choose **Command both** in the control menu |
| Cancel an unplaced point order, then deselect on a second press; close a window | **Escape** |

On a touch screen, tap a formation, tap **Move** or **Attack**, and tap its destination. Tap **Face**, then tap a direction to set its orientation at the current position. To select a charge target, tap **Target**, then tap an enemy formation. Use two fingers to pan and pinch to zoom. The tactical map provides another way to move the camera.

Orders are available while the battle is paused. This is useful for coordinating several formations. **Retreat / R** immediately moves troops away from danger. They turn as they withdraw, then seek friendly support to reform. They remain part of the army and can return to action. **Rally / V** requires a selected commander; it helps nearby shaken and retreating troops recover.

## Giving responsive orders

**Charge / C** immediately chooses a nearby enemy with a useful approach and
commits the selected regiment to that target. You do not have to place another
marker. For a specific opponent, use **Target / Shift+C**, then click its
soldiers or banner. The selected formation's order line names its charge target,
and its command arrow points toward that enemy. If the target is destroyed or
routs, the formation chooses another enemy and continues its charge.

Fresh orders replace the soldiers' old paths, attack assignments, and attempts
to reinforce another fight. A new march begins from their current spacing;
they settle into the new ranks while advancing instead of first walking
backward to satisfy a perfect layout. **Move** tells soldiers to disengage and travel to
the marked point, including those already attacking. They may defend themselves
against an enemy in reach while moving. **Hold** stops them at their current
positions instead of letting an old marching route continue. Holding troops
can still yield to friendly traffic, defend themselves, or move into a new
formation if you explicitly change its shape.

Under **Auto**, soldiers give enemies already within weapon reach immediate
attention, including enemies from a different regiment than their formation's
assigned opponent. They can defend the side of a formation without waiting for
the whole regiment to choose another target. **Hold** also allows local defense
without pursuit. Directional movement commands keep control of the soldiers'
feet: an unrelated opponent beside a Charge or ordered approach does not stop
them or draw them off course. Any incidental defensive strikes must allow that
movement to continue. Physical enemy bodies can still obstruct the route.

When several regiments move together, their destinations preserve the group's
two-dimensional layout. The whole layout is fitted inside the battlefield,
with separate destinations around blocked ground, so a large selection does
not receive the same destination along the map edge.

Faster allies following a manually moving or charging formation wait until
they actually close the gap before asking it to open passing lanes. This works
when you command the whole army, individual formations, or a front line whose
rear support remains on Auto. A moving lead formation can open lanes while
advancing; followers still slow down and wait for physical space.

On open ground, faster automatic formations can pass beside a slower formation
while it keeps its marching ranks. Fresh commands discard an obsolete passing
route, and formations whose courses are already separating can continue toward
their different destinations.

When automatic formations cross on open ground, one can briefly yield in its
current ranks while the other continues along its own route. They recheck the
available space and resume as the crossing clears. Temporary yielding has a
bounded duration, and a completed or obsolete crossing cannot immediately
capture the same groups again. Passing troops continue from their actual
positions instead of trying to rebuild an old arrangement. Narrow passages and
opposed traffic still require physical gaps and can take longer to clear.

Missile troops given Charge close to a firing position and fire. They do not
automatically give ground when the enemy approaches during that order. Their
weapons keep their normal ranges; cavalry and infantry seek close combat.

The command bar highlights the current order and shows whether selected troops
are moving, closing on a target, fighting, holding, or recovering. A highlighted
point order is waiting for your destination click. Right-click confirms that
armed order, including Move, rather than changing it to Attack. Changing a
formation returns keyboard control to the battlefield, so the next hotkey
works immediately.

Orders issued while paused are acknowledged immediately and execute when you
resume. If only some selected formations can obey, the message reports the
accepted count and explains when others are withdrawing or regrouping. A
formation in recovery must finish regrouping before it can accept a new combat
order. A nearby commander can help rally it; re-forming still takes time.
**Move and Retreat remain available during withdrawal and regrouping.** Use Move to choose a safe destination. Repeating Retreat keeps the current breakoff in progress.

Orders update intent immediately; physical movement starts on the next
simulation step. Troops still need time and space to turn, pass allies, and
navigate terrain. An intact enemy line continues to block them.

## Facing and marching in formation

Select a formation, press **T** or click **Face**, then click the direction it
should face. A short gold arrow previews that direction at the formation. The
point you click sets orientation; it does not move the formation to that point.
The selected formation also has a current-facing arrow and a compass readout.
When several formations are selected, each faces toward the point from its own
center. A nearby point makes their directions converge; a distant point gives
them more nearly parallel headings.

During deployment, Face turns the existing arrangement around its center when
there is enough clear space. Move the formation into a clearing if the turn is
refused. Its soldiers keep their identities and spacing. Scenario saves retain
facing together with the individual deployment positions, including split groups.
For a group turn, every eligible selected footprint must fit. If any is blocked,
the whole turn is refused and all positions remain unchanged.

During battle, Face orders a visible turn and re-formation at the current
location, followed by **Hold**. Orders issued while paused wait for the simulation
to resume. Use **Move**, **Attack**, **Charge**, or **Auto** when you want that
formation to advance again. **Retreat** immediately starts a withdrawal.

Formations begin their advance from their arranged ranks. Their approach uses
a local opponent and facing, so a wing can work inward toward a pocket while
another group faces a different front. Combat deployment happens near the fight;
nearby threats, terrain, and real allied traffic can still bend the ranks.

For an automatic cavalry encirclement, choose **Encircle** in **Battle plans**.
The infantry engages the front while cavalry and lancers follow committed
routes around the wings to the rear. Horse archers seek a flanking firing
position. You can also arrange the center and wings yourself and use Face to
aim each wing inward. Keep groups under separate commands when they have different
jobs. Once the battle begins, pause to issue **Hold** where you want a line to
stay and use **Target / Shift+C** to commit each advancing wing to its intended
opponent. A fresh **Move** remains available to reposition a group.

### Try the included encirclement drill

Open **Scenarios**, choose **Import JSON**, and load
`examples/encirclement.ironfield.json`. Four Azure formations of 160 swordsmen
surround a central Ember formation of 160 guardians. A second guardian formation
stands farther east. The four wings already face inward, so you can inspect
their orientation before starting.

For a stationary-defender exercise, start and immediately pause,
choose **Command both**, select the two Ember formations with Shift-click, and
press **H**. Resume and let the Azure wings approach under Auto. Scenario files
save the starting arrangement, not battle orders, so apply those Hold orders
after each load or reset. You can instead command the defenders and wings
yourself to experiment with a moving center, reserves, and a closing pocket.
This is a formation-control drill you can adapt for your own battle layouts.

## Build an empty encounter

Choose **New empty encounter**, **Custom encounter · empty field** in the scenario
menu, or **Custom Map** in the battlefield menu. This starts a new deployment
with **both armies empty and no terrain areas**. It also clears the previous
battle's selection, projectiles, effects, and pending roster rebuild. Existing
saved scenarios remain available.

Use the custom editor to build your field:

1. Choose a ground appearance, then select **Forest**, **Hill**, **Rocks**,
   **Water**, **Bridge**, **Wall**, or **Gate**. Choose the defending army for new fortifications. Drag a rectangle on the battlefield, or click to
   place a standard-sized area. Bridges extend across the water area they touch.
2. Use **Erase** to remove areas, **Undo** to reverse a terrain edit, or
   **Clear terrain** to remove the terrain while keeping your armies. Rocks and
   water, walls, and gates cannot be painted over existing soldiers.
3. Choose the army with the Azure or Ember tab. Select a troop type and formation
   size, press **Place on field**, and click an open clearing. Each placement
   adds one formation; the normal roster controls can also add troops.
4. Press **Escape** or **Select troops** to leave terrain editing. Select a
   formation and right-click to move its deployment. **Face / T**, then a direction
   click, sets its orientation. **Split / B** divides it
   into two separately selectable commands.
5. Add troops for both armies, then start. Use **Save scenario** or **Export
   JSON** to keep the terrain, army composition, facing, and deployment groups.

The editor supports up to 96 terrain areas. Direct formation placement accepts
up to 400 soldiers at a time, within the usual army limits; a commander is
placed individually. Middle-mouse dragging still pans while a tool is active,
and touch dragging paints terrain with that tool selected.

### Split a formation

Select one or more formations and press **B** or click **Split**. Each eligible
formation divides into two groups drawn from its current physical sides. The
new groups preserve existing soldiers, health, losses, morale, and formation
shape. Splitting does not move their bodies or create reinforcements.

Both halves initially retain their orders. A shared Move or point Attack gets
separate nearby destinations that preserve the original destination area;
Charge keeps the chosen enemy. Click either half afterward to command it
independently. Split works during deployment, battle, and pause. Commanders and
formations with fewer than two remaining soldiers cannot split; the field has
a 512-formation limit.

Splits made during deployment are saved with the encounter, including the
individual soldiers' positions and facing so the two halves keep their spacing
and orientation on reload.
Splits made during battle apply to that fight; resetting or loading a scenario
restores its saved starting deployment.
If you split while a Face order is turning the formation, each half completes
the requested orientation around its own center, then holds there.

## Build an army

The builder has a separate roster and battle plan for each army. Enter exact quantities, use the troop controls, or mirror one army into the other. Army points are a comparison of the configured troop costs, rather than a spending restriction; this is a sandbox, so uneven battles are allowed.

Each side can contain up to **10,000 combatants**, for a maximum of **20,000 total**. A catapult, elephant, commander, tower, or ram counts as one simulated combatant. The epic preset contains 12,000 combatants. Commanders are limited to **16 per army** and each has its own selectable unit. The original field presets include two commanders per side; the epic preset includes four per side.

Troop portraits and equipment are original graphics drawn in code and cached as sprite frames. Soldiers are not represented by a shared generic marker: infantry carry their own weapon and shield designs, mounted units have horses, lancers carry long pennant lances, catapults have wheeled frames, and elephants have tusks and howdahs. Team colors identify allegiance, while the silhouette and equipment identify the class.

### The twenty-three troop and engine types

| Troop | Appearance and role | How to use it |
| --- | --- | --- |
| **Levy Spearmen** | Lightly equipped spearmen with wooden shields | Fill out a broad line or screen more valuable troops. Their low armor and morale make them vulnerable under pressure. |
| **Shield Swordsmen** | Mail-clad infantry with steel blades and round shields | Form a dependable central line. Their balance of health, armor, and damage suits prolonged melee. |
| **Pike Guard** | Long pikes extend well beyond the front rank | Face a cavalry approach and let a cohesive line settle. Set pikes intercept frontal charges and strongly punish mounted troops. Flanking, disruption, and missile fire reduce this advantage. |
| **Bronze Hoplites** | Crested bronze helmets, large round shields, and long spears | Fight as a supported line or shield wall. Firm ranks, frontal protection, and physical shield pressure help them hold and push lighter infantry; piercing weapons, flanks, and dense bombardment threaten them. |
| **Crown Halberdiers** | Armored infantry with hooked poleaxes | Reinforce the line against armored enemies and mounted attacks. Their heavier weapons trade attack speed for impact and armor penetration. |
| **Iron Guardians** | Heavy plate and large tower shields | Anchor a crossing or absorb pressure while your ranged troops fire. They are slow, and penetrating weapons still threaten them. |
| **Berserkers** | Shock infantry with two-handed war clubs | Approach a tightly packed front from a flank. The main target takes a full hit, and enemies inside the forward arc take splash damage and are pushed away. |
| **Longbow Rangers** | Hooded archers carrying tall bows | Protect them behind infantry and exploit their long range. Their frequent volleys work well against lightly armored troops. |
| **Musketeers** | Long muskets, muzzle smoke and flat ball trajectories | Use a firing line to stagger approaching enemies. One ball strikes the first opposing body in its path. |
| **Arbalest Company** | Armored crossbowmen carrying broad crossbows | Concentrate their powerful, armor-piercing bolts on hard targets. They reload more slowly and have less reach than longbows. |
| **Javelin Skirmishers** | Light troops with bucklers and throwing spears | Work near woodland and on the wings, throwing at close range before giving ground when enemies close. |
| **Levy Slingers** | Cloth-clad missile troops with slings and ammunition pouches | Add inexpensive ranged pressure across a broad front. Their light protection demands a screen against melee attackers. |
| **Royal Cavalry** | Armored riders with swords and shields | Attack exposed wings, intercept isolated troops, or run down archers. Allow room for a charge and avoid a prepared pike front. |
| **Banner Lancers** | Armored horses and long pennant lances | Use a clear approach to deliver a heavy first impact. Their charge is more valuable than leaving them locked against pikes. |
| **Steppe Outriders** | Light mounted bowmen | Use speed and missile fire to threaten an exposed side. They are lightly armored and have shorter bow range than foot archers. |
| **Field Cannons** | Wheeled gun carriages and heavy round shot | Fire into dense ranks for splash damage and airborne knockback. Closed walls stop low shots. |
| **Counterweight Trebuchets** | A swinging counterweight and sling | Lob a boulder into a packed formation or bombard a gate. Protect the crews during the 24-second reload. |
| **Stonebreak Catapults** | Wheeled wooden engines with throwing arms | Bombard tightly packed enemy formations from behind a screen. Stones cause area damage, but the machines move and reload slowly. |
| **Ironwind Ballistae** | Wheeled crossbow engines with heavy limbs and long bolts | Use precise armor-piercing artillery against hard targets. Slow reload and weak close defense require protection. |
| **Assault Siege Tower** | Tall wheeled timber tower, bow platform, battlements, and deployment bridge | Escort it to a wall. After docking it opens an assault ramp and supports the crossing with arrows. |
| **Covered Battering Ram** | Armored timber roof, crew, wheels, and a suspended iron-tipped log | Reach a defended gate, batter it open, then send infantry through the breach. |
| **War Elephants** | Large tusked elephants carrying armored howdahs | Break into crowded infantry with health, impact, and area damage. Their size and cost make concentrated pikes and bolts a serious threat. |
| **Field Commanders** | Gold-armored leaders on white horses, carrying tall standards | Stay near formations that need morale support. Move behind falling-back troops and sound a rally to help them regroup and return to the line. |

## Armor classes and troop counters

Every troop has **Light**, **Medium**, or **Heavy** armor, an armor-point value,
and a weapon profile. Armor points reduce incoming damage according to the
attacker's penetration. The weapon profile then multiplies the result according
to the defender's armor class. Shields, facing, cohesion, terrain, range, charge
impact, and mounted bonuses still affect the fight.

For example, ordinary arrows use **×1.15 against light**, **×0.98 against
medium**, and **×0.82 against heavy** armor. Crossbow bolts use **×1.00 / ×1.08 /
×1.18** and penetrate **80% of armor points**. This gives longbows a role against
light formations and crossbows a role against armored targets without making
either weapon automatically win every encounter.

Click **Matchups & counters** on any army card, the matchup button in the
selected-formation panel, or **Compare matchups** in the field manual. Choose
an attacker and defender to see their armor factor, penetration, mounted bonus,
useful targets, and threats. The complete matrix covers every troop against
all three armor classes. It uses the same data as combat. A damage modifier is
not a prediction of which army wins; numbers, cost, range, terrain, support,
orders, and positioning all matter.

Some counters depend on the situation. Bowmen threaten spearmen at range,
while those spearmen can overwhelm an exposed bow line after reaching it.
Cavalry threaten unsupported missiles but are vulnerable to a prepared pike
front. Heavy guardians resist arrows and light attacks; halberdiers, crossbows,
and ballistae bring penetration against them. Packed shield ranks are harder to
push from the front but offer valuable targets for artillery.

The complete numerical matrix and the strengths and threats for all twenty
troop and engine types are also included in **BALANCE.md**.

### Prepare pikes for cavalry

Use **Battle line** or **Shield wall**, face the likely charge with **T**, and
allow the ranks to settle. Cohesion and a stationary, correctly facing front
make the brace effective. The first ranks can strike charging riders at pike
reach, interrupt their charge, and reduce their frontal impact. The ordinary
mounted-damage bonus still exists outside a full brace, but marching, charging,
turning, loose order, and attacks from the side cannot claim the same prepared
defense. A soldier looking sideways to fight does not instantly rotate the
whole formation's brace.

Use **The Broken Charge** preset to try pikes against a mounted army. Keep a
missile screen or supporting infantry nearby: pikes have light armor and their
special mounted advantage does not apply to ordinary foot soldiers.

### Hold and press with hoplites

Bronze Hoplites have **medium armor**, large frontal shields, and stronger rank
discipline. They keep their assigned places more firmly during contact while
still adapting to terrain and obeying movement orders. A cohesive, supported
front can exert shield pressure on lighter opponents. The defender moves
through ordinary physical space, and the hoplite follows only when there is
room. Occupied ranks, heavier bodies, rocks, water, and battlefield boundaries
limit the displacement.

Use a line or shield wall, leave room behind your opponent, and avoid letting
the hoplites' flanks become exposed. Loose order, disorder, and attacks from the
side reduce their formation advantage. Their slower advance and packed ranks
make piercing missiles and artillery useful counters. A successful physical
push briefly shows a shield-forward pose and **SHIELD PRESS** on nearby or
selected standards; the selection panel counts troops that recently pushed.

**Move**, **Face**, and **Retreat** retain command priority. A firm shield line
does not lock its soldiers into an attack when you order them elsewhere.

### Battle plans

Open **Battle plans** above the battlefield or use the army builder's plan
menu. Plans can change during deployment, a live battle, or a pause without
rebuilding the armies. The panel pauses a live battle; close it and press Space
to resume. During battle, choose **Command both** to change both armies' plans.

| Plan | Commands given to the formations |
| --- | --- |
| **Balanced** | Advance toward local opponents, support the line with missile troops, and keep commanders behind friendly formations. |
| **Full assault** | Commit melee formations to nearby opponents and press the attack. Missile troops close to their distinct firing ranges. |
| **Hold & counter** | Hold the current line and fire. Each threatened sector counterattacks as the enemy closes. If no attack arrives, the army eventually advances. |
| **Encircle** | Infantry engages the front. Cavalry and lancers take outside lanes, pass the enemy wing, approach the rear, then attack. Horse archers harass from the flank. |
| **Hammer & anvil** | Infantry receives the attack while cavalry works around the enemy. Riders reaching the rear wait for the infantry to engage, then attack; the wait is bounded if the enemy refuses to advance. |
| **Attack left** | Lead with the army's left wing and send cavalry around that flank. Keep the opposite wing in reserve, then commit it after contact or a bounded delay. |
| **Attack right** | Lead with the right wing while the left waits in reserve, using the same coordinated release. |
| **Screen & skirmish** | Missile troops approach into range, stop to fire, and withdraw together when threatened. Infantry maintains a covering line and intercepts pursuers. |
| **Steady advance** | Infantry advances deliberately in connected ranks. Aligned pikes and hoplites retain part of their brace while walking forward; turns, fast movement, and disrupted ranks lose it. A planted defense remains stronger. |
| **Rapid encirclement** | A pinning force holds the front while separated wings run around a bounded nearby enemy group. Isolated formations are surrounded individually; adjacent squads are treated as one position instead of one tiny target. |
| **Refused flank** | Pull the exposed wing back into a guarded line while the opposite wing advances. Keep a reserve behind the bend, then commit it when the attack creates an opening. |
| **Concentrated breakthrough** | Choose a weak part of the local enemy front, concentrate an assault there, and bring supporting reserves through the same sector after contact. |
| **Elastic defense** | Receive the approach, withdraw in bounded stages when pressed, and counterattack enemies that follow into the prepared line. Fresh direct orders interrupt the plan immediately. |
| **Echelon assault** | Advance in offset waves, starting with the leading sector and committing the following formations in sequence. The supporting line follows the developing fight instead of charging all at once. |

Left and right are relative to the army's direction of advance. A plan keeps
its deployment axis, assigned wing, and turning points instead of repeatedly
swapping sides as army centers move. Several cavalry formations receive
separate lanes and staging depths. When a flank has no room at the map edge,
Encircle can choose the other wing. Cavalry that has already cleared its
objective's rear can turn inward, and an intercepted maneuver can deploy to
fight the blocking enemy. Terrain and bodies still constrain the route.

Select an automatic formation to see its current phase and remaining movement
route. A cavalry formation must physically reach its turning points before it
begins the rear attack. These plans issue movement, holding, target, and attack
intent. Steady Advance additionally earns a partial walking brace through correct facing, low speed, and physical cohesion; every plan still moves soldiers through ordinary collision and terrain checks.

**Move, Hold, Face, Attack, Charge, and Retreat take priority.** Changing the
army plan affects formations on Auto. **Auto / G** returns selected troops to
the current plan. Recovering formations keep withdrawing or regrouping; the
plan does not cancel that recovery. Selecting the same plan again leaves an
ongoing maneuver in place.

#### Try the automatic cavalry drill

Import `examples/cavalry-encirclement.ironfield.json` through **Scenarios** and
start the battle. Its 640 troops include an Azure infantry center, cavalry and
lancers on the wings, and an Ember guardian/pike line on **Hold & counter**.
Select either mounted formation to watch its outside route and the successive
flank, rear approach, and rear attack phases. Both armies already have plans;
no extra setup commands are needed. The original
`examples/encirclement.ironfield.json` remains available for manually commanding
four inward-facing wings.

## Missile troops have different jobs

Missile formations spread into bounded firing sectors with usable depth. Their
soldiers seek their own firing positions, and idle rear troops can move into
available shooting space. This prevents an entire force from stretching toward
one distant target or leaving its rear formations permanently out of range.
Firing ranks leave physical passages for friendly reserves. Nearby automatic
shooters can also make room for advancing allies, with normal body collision
and movement limits still applying.

| Troop | Automatic battlefield behavior |
| --- | --- |
| **Javelin Skirmishers** | Loose, close-range bands. Throw, then take a short local step while recovering the next spear. Make room for other throwers without pulling the whole army backward. |
| **Longbow Rangers** | Broad shooting ranks at longer range. Draw and release from useful firing positions, holding their ground between shots. |
| **Arbalest Company** | More deliberate firing ranks. Plant to aim, fire a bolt, and reload in place. Favor armored enemies already within reach. |
| **Levy Slingers** | A light screen with smaller spacing adjustments between sling releases, favoring softer targets. |
| **Steppe Outriders** | Mounted harassment with larger local evasive steps and a wider approach. |
| **Stonebreak Catapults** | Spaced long-range batteries with slow reloads and area bombardment. |
| **Ironwind Ballistae** | Planted heavy-bolt batteries that favor armored targets. |

Army cards display these behavior labels. Active Move, Charge, point Attack,
Hold, and Retreat retain their command rules: a local skirmish routine cannot
replace your chosen movement order. Missile troops given Charge still seek a
useful firing position rather than requiring melee contact.

## Formations, physical ranks, and cohesion

Select one or more regiments and choose a formation in the command bar. Each shape changes how troops organize on the ground and offers a different tradeoff. The selection panel shows current **cohesion** alongside morale.

Friendly soldiers yield and compress in crowded traffic. Enemy ranks physically obstruct movement, so a move order does not let a regiment walk directly through an intact enemy line. Crowding, contact, and disruptive movement can pull troops out of their intended positions and reduce cohesion. Give units room to approach, avoid crossing several formations through one another at once, and allow disordered troops to reform.

Formation modifiers are strongest at **100% cohesion**. They blend toward neutral as cohesion falls: for example, a fully cohesive wedge provides a 30% charge bonus, while 50% cohesion reduces that bonus to 15%. Unit armor, weapon properties, facing, terrain, and the other combat rules still apply.

| Formation | Benefit at full cohesion | Tradeoff at full cohesion |
| --- | --- | --- |
| **Battle line** | 4% less frontal melee damage taken | Balanced spacing and movement. |
| **Shield wall** | 18% less frontal melee damage and 28% less frontal ranged damage taken | 28% slower movement, 10% lower attack damage, and 20% less charge impact. |
| **Assault wedge** | 30% more charge impact and 10% faster movement | 10% more melee damage taken. |
| **Loose order** | 28% less ranged damage taken, a 40% splash-damage reduction modifier, and 5% faster movement | 15% more melee damage taken and a wider footprint. |

Choose a shield wall to hold a pressured front, a wedge for a deliberate charge, and loose order when missile or artillery fire is the main danger. Changing shape during battle disrupts cohesion temporarily. Commanders retain their individual command role rather than taking a regiment formation.

### Moving through obstacles and friendly troops

The chosen formation is the shape the regiment tries to restore in open ground.
Individual positions have local flexibility, so soldiers can bend a rank, use a
nearby opening, and adjust their spacing under pressure instead of repeatedly
walking back toward an occupied ideal slot. Their chosen formation and its
cohesion-dependent bonuses still matter, and the ranks recover their intended
shape when they have room.

A march begins from the ranks already on the field. Turning keeps soldiers'
relative places; an about-face lets the existing rear become the new front
after a short turn. The formation's advance slows when its soldiers lag behind
so its standard does not pull a long tail away from the rest.

Forests slow movement and can modestly squeeze the existing files. They no
longer force a wholesale conversion into a narrow column. Rocks and actual
narrow passages can still require fewer files, but the formation commits to
that passage width instead of repeatedly reassigning every soldier as the
width changes. Soldiers follow routes around blocked ground, then recover
space after the obstruction.

The temporary shape uses the whole available passage, including room on the
open side of a rock. Troops can shift their approach sideways and take separate
paths around a corner. Passing lanes widen again as room becomes available.
Soldiers at the head of a formation can defend themselves against nearby enemies
while the regiment keeps its approach organized. Local combat deployment and
reserve movement take over as the formation reaches the fight.

In open ground, formations first look for a broad route beside an allied
formation. Faster followers can overtake without making the slower formation
rebuild its ranks. Crossing routes can use a short yield; genuine confined
crossings still open physical gaps and sidestep through one another's ranks. Body collision remains
active, so crossing takes time and dense traffic can queue. Movement slows,
cohesion falls, and the ranks visibly bend or stretch before reforming. Enemy
ranks continue to obstruct movement and engage attackers.

The selection panel reports **Narrowing for terrain**, **Weaving through allied
ranks**, **Ranks adjusting locally**, or **Re-forming the ranks**. Selected formation outlines follow the
adapted shape. During battle, the solid outline traces the soldiers' actual
positions at close and medium zoom, with a faint guide showing the chosen shape.
Walking animation follows distance traveled, so a soldier waiting
for space stops stepping until it can move again. These adaptations happen
automatically when you issue normal movement orders.

## Battle lines and pressure

Automatic attacks choose local opponents and keep a stable approach before
allocating nearby fighting frontage. Each regiment approaches its own section
of the opposing force while preserving the marching formation. Melee soldiers seek reachable contact positions and
less contested targets; rear ranks can feed openings or move toward available
flanks. Ranged troops support the line from firing distance.

Missile troops can fire at another nearby enemy when their assigned opposing
regiment is outside weapon range. They also leave obsolete passing lanes when
they reach firing positions, allowing the rest of the formation room to deploy.

Melee soldiers that remain inactive behind engaged allies can leave an
unproductive rear position and seek a reachable opening. Their approach accounts
for the crowding of friendly troops and the positions of nearby enemies,
including enemies outside their regiment's original assignment. Short-lived
reservations spread close approaches around an opponent. Detours remain subject
to terrain, movement speed, and body collision; soldiers must walk to the fight.

This reinforcement behavior applies to automatic advances, attack orders, and
charges. Explicit move and hold orders retain their control, and withdrawing or
reforming troops continue their recovery instead of pursuing another fight.

Terrain and local crowding affect the approach. Regiments use navigable lanes
around rock masses and through woodland, then spread into fighting frontage as
contact approaches. Troops still occupy space, and narrow crossings can require
queues. A large army needs room to bring more of its strength into the fight.

The selected-formation panel shows its tactical role and **active attackers**.
That count measures living soldiers who attacked during the preceding **1.5 simulated
seconds**, updated twice per simulated second. A ranged soldier reloading a
slow weapon can fall outside that recent-activity window while still supporting
the battle.

Use **Auto / G** to return a regiment to coordinated battle behavior, **Attack / A**
to advance and engage along an ordered approach, and **Hold / H** when keeping a
specific position matters more than pressing forward.

## Combat animation and fallen troops

Weapons have distinct attack and recovery poses: javelins draw back and release, longbows draw and re-nock, crossbows recoil and lower for reloading, melee troops swing or thrust, and siege engines recoil. Distance-driven marching adds eight gait phases. Fast mounted charges lower weapons; moving braced infantry presents its front. Real hit events trigger shield or recoil reactions, and falling bodies settle before becoming persistent casualties. These poses follow simulation time or actual travel, so a pause freezes the action.

| Persistence setting | Behavior |
| --- | --- |
| **Brief** | Original lifetime: remain solid for 14 seconds, fade, and disappear at 22 seconds. |
| **Long** | Remain for 120 simulated seconds, fading during the final 24. |
| **Entire battle** | Remain until the battle is reset or replaced. Later casualties form visible layers in the same local pile. |

**Fallen Soldiers** toggles visibility independently. Changing persistence also updates casualties already recorded in the encounter. Bodies remain cosmetic and do not block living troops. Airborne casualties complete their motion and landing before entering a settled pile.

Long and Entire Battle modes store settled casualties in spatial chunks. A stable frame draws the visible chunks instead of every corpse, with bounded bitmap memory and incremental processing for a sudden large number of deaths. Evicting a bitmap does not erase casualties; revisiting the area reconstructs them. Brief mode and actively falling bodies retain the existing visible draw limits. Full/Reduced animation settings are separate from body persistence and visual quality.

## Fall back, regroup, and rally

Morale loss first creates an opportunity to fall back and recover. A shaken regiment seeks a viable allied formation or commander, withdraws toward that support, rebuilds morale and cohesion, and can rejoin the fight. The selection panel identifies its condition and recovery target. A full rout is a later collapse; it is separate from an ordinary fallback order.

For a manual **Retreat / R**, the order panel shows **Withdrawing** immediately.
Troops step away from the threat as they turn, then make for friendly support.
An order issued during a pause takes effect when the simulation resumes. Reissuing Retreat keeps the current withdrawal in progress. After the
initial escape, soldiers form marching ranks from their current positions so
they can keep moving toward support without trying to exchange entire rows.
Automatic recovery waits for them to complete that escape and gain separation;
an explicit commander rally remains available when you want them to regroup.

**To rescue a pressured line:**

1. Select the damaged formations and press **R** to pull them back before they are overwhelmed.
2. Keep a healthy friendly formation behind or beside them so they have somewhere to regroup.
3. Press **J** to select a commander and move that commander behind the retreating troops.
4. With the troops inside the commander's gold rally circle, press **V**.
5. Let the ranks reorganize, then use **G** to resume the battle plan or issue a new move or attack order.

A commander provides nearby support within **360 world units**. An active rally reaches **620 world units**, restores up to **28 morale and 35 cohesion**, and can recover nearby fallback formations once they have been retreating for at least three seconds. The active ability has a **45-second cooldown**. The circle and cooldown readout help you time it.

Troops can also regroup near ordinary friendly formations. Passive recovery takes at least twelve seconds and requires enough morale and cohesion; ongoing enemy pressure can delay it. Keep a reserve instead of committing every regiment to the front at once. An unsupported, repeatedly broken formation is much harder to recover.

## Battlefields

There are 22 authored fields plus the empty custom editor. Original maps retain their dimensions; all twelve additional maps are larger.

| Map | Dimensions | Tactical character |
| --- | ---: | --- |
| **The Amber Plains** | 6,400 × 4,400 | Wide rolling grassland with wooded wings and gentle central heights. A balanced battlefield for flanking and large formations. |
| **Three Bridges** | 6,400 × 4,400 | An impassable river cuts the valley in two. Three stone crossings reward a patient defense and a carefully timed attack on the wings. |
| **Saffron Expanse** | 6,400 × 4,400 | Wind-swept sand, isolated ridges, and exposed approaches. Use the broad southern gap to turn an enemy line or place artillery on a rise. |
| **Wintermarch** | 6,400 × 4,400 | Dense pine stands divide the snowy plain into lanes. Cover benefits missile troops, while open central ground still allows a decisive mounted charge. |
| **Greenwood Vale** | 6,400 × 4,400 | Separated woodland stands shelter missile troops while wide glades connect the field. Foot skirmishers can use the trees; cavalry must choose its lanes. |
| **Broken Crown Pass** | 6,400 × 4,400 | A broken spine of rock offers three substantial passages instead of one gate. Secure a ridge approach, threaten another crossing, or send reserves through the middle. |
| **Ashen Basin** | 6,400 × 4,400 | An exposed central rise dominates the basin, with broad northern and southern approaches. Artillery can command the heights while mobile troops contest the outer wings. |
| **Stonekeep Siege** | 6,400 × 4,400 | A stone curtain wall closes the valley. Break the central gate with covered rams, or escort siege towers to establish additional assault ramps along the wall. |
| **The Twin Causeways** | 6,400 × 4,400 | An unbroken moat funnels attackers across two causeways into defended gates. Break one entrance or divide the defense; towers can deploy beyond a secured crossing. |
| **Crownwatch Bastion** | 6,400 × 4,400 | Rock shelves divide the western approach into three narrow lanes. Two fortified entrances lead into the upper bastion, where defenders can hold the shoulders of each breach. |
| **The Sovereign Plain** | 12,800 × 8,800 | Four times the ground of the Amber Plains. Low central rises separate a broad infantry front from open mounted wings, with woodland islands sheltering distant reserves. |
| **The Endless Steppe** | 16,000 × 11,000 | Six and a quarter times the original battlefield area. Widely separated ridgelines leave a huge central plain and distant routes for rapid wings, layered reserves, and army-scale envelopment. |
| **Silverwood Crossroads** | 9,600 × 6,600 | Large separated groves form four broad forest corridors. The central glade favors ordered infantry, while light troops can cross the woods and cavalry can take the open outer lanes. |
| **The Blackwood Marches** | 12,800 × 8,800 | Long forest belts stagger the approaches without sealing them. A steady line can use the broad roads; successive assault waves and skirmishers can contest separate clearings. |
| **The Five Crossing War** | 12,800 × 8,800 | A wide river divides the entire field. Five crossings range from narrow outer bridges to a broad center, so reserves and simultaneous wing attacks matter more than feeding one crowded crossing. |
| **The Redstone Gates** | 9,600 × 6,600 | A broken canyon wall offers four real passageways, each at least five hundred paces wide. Narrow columns can clear the stone, then deploy on the open eastern shelf. |
| **The Sea of Copper** | 16,000 × 11,000 | A vast desert supports sweeping cavalry routes. Rocky escarpments divide the northern sector, while the south stays open for rapid encirclement and countercharges. |
| **The Wintermere Front** | 12,800 × 8,800 | An unfrozen lake blocks the middle of the snowy plain. Broad northern and southern approaches force the armies to choose where to concentrate, with a smaller central causeway for a risky direct attack. |
| **The Reedwater Delta** | 9,600 × 6,600 | Two offset river channels create a large middle island. Three crossings on each bank favor securing a bridgehead, reforming in the center, and attacking a second crossing with reserves. |
| **The Imperial Rampart** | 12,800 × 8,800 | A long stone rampart guards three separated gates. Rams can split the garrison between distant entrances, while escorted towers create new assaults along the wall. |
| **Floodgate Citadel** | 9,600 × 6,600 | A broad moat protects a citadel with three causeways. Each assault must bring an engine across its bridge, open a gate, and clear enough space for the troops waiting behind. |
| **The Three Crowns** | 16,000 × 11,000 | Three broad mountain passages lead to three fortified entrances. Beyond the outer wall, a partial inner redoubt lets defenders trade ground while attackers choose another gate or a distant flank. |
| **Custom Map** | Selectable | An empty battlefield. Place formations and draw your own terrain before starting the encounter. |

Forests provide protection from ranged fire and slow movement, particularly for mounted troops. Hills benefit ranged troops. Water and rock regions block movement; formations plan routes around them and through bridges. Fortifications require a gate breach or deployed assault ramp.

## Ready-made battles

| Preset | Azure side | Ember side | Starting situation |
| --- | ---: | ---: | --- |
| **Clash of Kingdoms** | 1,200 | 1,200 | 2,400 soldiers. Balanced armies meet on open ground. |
| **The Broken Charge** | 1,436 | 1,098 | Massed pikes and archers face a swift mounted host. |
| **Hold the Crossings** | 1,050 | 2,116 | An outnumbered defense stands between three bridges and a larger army. |
| **Thunder on the Dunes** | 1,150 | 1,446 | Long-range bombardment, armored screens, and elephant assaults. |
| **War of Twelve Thousand** | 6,000 | 6,000 | 12,000 individual soldiers. Vast winter armies with every troop class. |
| **War in the Greenwood** | 1,500 | 1,500 | 3,000 soldiers. Woodland skirmishers, archers, and reserves contest the glades. |
| **Three Roads to the Crown** | 2,000 | 2,000 | 4,000 soldiers. Armored polearms and bolt artillery fight for three mountain passages. |
| **Battle for the Ashen Heights** | 3,000 | 3,000 | 6,000 soldiers. Artillery commands the heights while broad wings contest the basin. |
| **Storm the Stonekeep** | 951 | 883 | Rams batter the central gate while two siege towers create additional approaches. Command the assault or take control of the garrison. |
| **The Siege of Two Bridges** | 809 | 803 | Two causeways constrain the assault. Protect the engines across the moat, break a gate, then bring reserves through the opening. |
| **Assault on Crownwatch** | 847 | 803 | Three rocky approach lanes feed two defended gates and a central tower assault. Keep the columns moving and deploy on the far side. |
| **War of the Sovereign Plain** | 2,616 | 2,612 | Two broad armies contest the heights. A braced infantry advance meets a rapid attempt to surround the connected enemy position. |
| **The Great Envelopment** | 4,718 | 4,746 | A huge mounted host has room for separated wings and distant reserves. Its opponent advances as a connected army across the colossal steppe. |
| **The Silverwood Ambush** | 1,743 | 1,803 | An elastic woodland defense gives ground through connected clearings. A succession of assault waves must keep its supports close. |
| **War in the Blackwood** | 2,770 | 2,904 | A staggered assault tests a refused flank among broad forest belts. Success depends on which avenue receives the reserves. |
| **Five Bridges, One Front** | 2,742 | 2,816 | A concentrated breakthrough must secure a crossing while defenders keep a braced river line across a much larger theater. |
| **Through the Redstone Gates** | 1,911 | 1,929 | Successive attack waves pass through four canyon lanes. A compact defense yields and counterattacks where the attackers lose support. |
| **Tempest of Copper** | 2,674 | 2,768 | Two fast armies use the vast desert differently: one surrounds the local enemy position, while the other concentrates on a weak seam. |
| **The Wintermere Offensive** | 2,332 | 2,584 | A lake separates the central causeway from two wide outer approaches. A refused wing must hold long enough for the opposite assault to develop. |
| **The Reedwater Campaign** | 1,909 | 2,043 | Two riverbanks demand two separate deployments. Steady ranks contend with a skirmishing defense among the island groves. |
| **Fall of the Imperial Rampart** | 1,819 | 1,906 | Three gates and three tower approaches divide a large garrison. Keep a reserve ready to exploit the first established passage. |
| **The Three Causeways** | 1,216 | 1,328 | Escort covered rams across three separate bridges, open the gates, then lead ordered infantry through the bridgeheads. |
| **Siege of the Three Crowns** | 2,218 | 2,126 | A colossal mountain fortress with three entrances and an inner redoubt. Open several approaches and leave room for the assault columns to unfold. |

These scenarios offer different problems to solve. Composition, positioning, terrain, plans, and direct commands all affect the outcome. The custom editor starts with no terrain and no troops.

## Morale and victory

Each soldier has individual health, a target, and attack timing. Armor points and classes, penetrating attacks, anti-cavalry bonuses, prepared braces, charge impact, missile fire, area damage, shield pressure, and cohesion-dependent formation modifiers affect combat. Front-line contact and physically occupied ranks limit who can reach an enemy, giving the engagement a more sustained pace than a mass of overlapping attackers.

The main count shows troops **in the army**, including those falling back or regrouping. **Fallen** troops were killed in the simulation. **Falling back** troops are still recoverable. **Routed** troops have left the battle and count as defeated. **Rallied** records successful recovery activity. The results panel gives a breakdown by troop type.

## Save, export, and replay scenarios

Open **Scenarios** to save the current setup on the device, export a JSON scenario, or import one. A scenario records its map, custom terrain, army composition, battle plans, formation groups including deployment splits, deployment positions, facing, and random seed. Loading a scenario returns to deployment. It does **not** restore a battle at its current combat time. Earlier scenario files remain supported.

Browser storage can be unavailable or cleared. Export a JSON copy when you want to keep a scenario independently of that browser. Imported roster counts are normalized to the army limits.

**Fight again** recreates the starting setup. A fixed seed reproduces the simulation when the starting configuration, fixed simulation steps, and commands—including their timing—are the same. The seed does not record your command history automatically.

## Performance and presentation

Start at the default 2,400 combatants, then try the 12,000-combatant preset. The 20,000-combatant cap is a supported configuration limit, not a promise of a particular frame rate on every device.

The simulation caches shared formation calculations once per step and uses
separate collision neighborhoods for ordinary soldiers and large bodies. A
few elephants no longer enlarge every infantry soldier's collision search.
All bodies still participate in collision and terrain checks.

The renderer uses cached terrain and sprite frames, culls off-screen objects,
and changes visual detail with scale. Large overview views use reusable screen
row depth sorting and direct sprite references. Close views and High detail
keep exact depth order. Adaptive detail responds earlier to sustained slow
frames and waits before changing back to avoid flickering between levels. Choose **Adaptive** or **Performance** in graphics settings for larger battles. These settings change drawing detail, not the simulated army size. Labels, health bars, fallen soldiers, and sound can be toggled individually.

Persistent casualties use bounded spatial caches, and the minimap updates its terrain cache only when the battlefield changes. Very large armies refresh the minimap less frequently. The main menu stops battlefield drawing entirely. Large maps keep a bounded landscape bitmap, which can make ground texture softer at extreme close zoom; troop sprites retain their own detail.

The time controls provide **½×, 1×, 2×, and 4×** requested speed. High speeds and very large armies require more processing. The status display shows measured rendering FPS and the simulation's achieved speed; if the hardware cannot keep up, the simulation may run below the requested speed. Reducing army size or requested speed can help.

The automated checks cover actual command response, physical blocking, formation tradeoffs, recovery, commanders, map navigation, and large-army stability. A separate interface check exercises the real input handlers, order display, paused commands, and target selection without opening a preview. These headless checks do not measure browser rendering FPS. See `tests/VALIDATION.md` for the completed checks, measurements, and their limits.

## Source and development

The source is deliberately small and dependency-free:

```text
index.html                 Interface and dialogs
src/style.css              Responsive layout and visual styling
src/data.js                Troop statistics, armor profiles, counters, maps, and presets
src/tactics.js             Persistent army plans, roles, and phased routes
src/impacts.js             Cavalry acceleration, contact damage, and diminishing rank pressure
src/siege.js               Fortification targets, structural damage, and deployed assault ramps
src/engine.js              Deterministic fixed-step battle simulation
src/graphics.js            Canvas renderer, sprite art, terrain, and minimap
src/app.js                 Input, interface state, audio, and scenario persistence
build.py                   Bundles the standalone IRONFIELD.html
tests/engine-test.cjs       Automated engine regression checks
tests/impact-siege-test.cjs  Physical run-up, impact, siege, and narrow-passage checks
tests/lancer-breakthrough-test.cjs  Actual penetration, counters, terrain, and command checks
tests/formation-continuity-test.cjs  Marching, facing, and envelopment checks
tests/counter-formation-test.cjs  Armor, prepared pikes, and hoplite pressure checks
tests/blockage-test.cjs     Broken Charge progress and mixed-size crossing checks
tests/mobility-tactics-test.cjs  Stable marching and physical strategy checks
tests/performance-test.cjs  Isolated, reproducible simulation benchmarks
tests/command-ui-test.cjs   Command input and feedback integration checks
tests/command-scale-test.cjs  Bounded command exercise with 12,000 soldiers
tests/graphics-test.cjs     Animation, corpse lifetime, and drawing checks
tests/menu-world-ui-test.cjs  Main menu, large saves, and persistence controls
tests/army-plans-test.cjs   New strategy movement, brace, and target-scope checks
tests/strategy-edge-test.cjs  Blocked wings, mixed frontages, and missed enemy sectors
tests/expanded-battlefields-test.cjs  Large map deployment and passage checks
tests/mounted-push-test.cjs  Lancer and cavalry penetration controls
tests/friendly-mounted-test.cjs  Friendly mounted clearance, acceleration, and command controls
tests/mounted-speed-test.cjs  Real mounted pace, acceleration, turns, and terrain
tests/formation-pressure-test.cjs  Whole-rank yielding, resistance, and command control
tests/animation-corpses-test.cjs  New poses, persistent pile cache, and large worlds
tests/VALIDATION.md         Tested behaviors and measured limitations
BALANCE.md                  Complete armor matrix and troop counter reference
examples/encirclement.ironfield.json  Four inward-facing wings to import and command
examples/cavalry-encirclement.ironfield.json  Automatic cavalry maneuver drill
examples/lancer-breakthrough.ironfield.json  Lancers against four levy ranks
```

Scripts are ordinary browser IIFEs loaded in this order: `data.js`, `tactics.js`, `impacts.js`, `siege.js`, `engine.js`, `graphics.js`, `app.js`. They share `GameData`, `BattleTactics`, `BattleImpacts`, `BattleSiege`, `BattleEngine`, and `BattleRenderer` globals. This avoids module-loading restrictions when the game is opened directly from disk.

To change gameplay, edit `src/data.js` for troop statistics, maps, and presets; edit `src/engine.js` for simulation behavior, `src/tactics.js` for army strategies, `src/impacts.js` for mounted momentum, and `src/siege.js` for siege engineering. The renderer draws its own assets in `src/graphics.js`. No external art or audio files need downloading.

After editing source, run:

```bash
python3 build.py
```

To run the simulation and command-interface checks with Node installed:

```bash
node tests/engine-test.cjs
node tests/command-ui-test.cjs
node tests/graphics-test.cjs
node tests/impact-siege-test.cjs
```

To reproduce the separate five-phase command exercise at 12,000 soldiers:

```bash
node tests/command-scale-test.cjs
```

It prints JSON measurements for Charge, Hold, reverse Move, and a chosen-target
Charge. Order-processing time is reported separately from simulated movement.

To reproduce the separate simulation benchmark (run it without other heavy
jobs on the same computer):

```bash
node tests/performance-test.cjs
```

It runs The Broken Charge, the 6,000-troop Basin, and the 12,000-troop Epic in
separate Node processes for 30 simulated seconds each, reporting step costs and
ten-second intervals. These are simulation measurements, not browser FPS.

Node and Python are development tools only. Playing the bundled game requires a browser.
