# Trash Tactics

The **Tier 2 and Tier 3 warfare expansion** adds six specialists, two production buildings, animated character/effect sheets, Stapler Canyon and Conveyor Chaos. See [the expansion guide](WARFARE.md) for troop statistics, controls, map mechanics and validation, or open [the animated artwork gallery](warfare-gallery.html).

A quick, cartoony RTS about turning office trash into a tiny empire. Desktop Meadow and Breakroom Blitz each have 2-, 3-, and 4-player layouts: six battleground layouts in all. Play a free-for-all against one, two, or three rival bots at Easy, Normal, or Hard difficulty. Destroy every rival desk to win.

## Play locally

Run `npm start` in this folder, then open the local address printed in the terminal. No installation or external services are required.

## Controls

- **Desktop:** click a unit or building; drag a box to select a squad. Shift adds to the selection. Right-click to move, gather from a trash can, or attack an enemy. Press **A**, then right-click an area to attack-move toward it. The Attack button targets the nearest visible valid enemy. Pan with the arrow keys, middle-mouse drag, or by hovering at the battlefield edge; use the mouse wheel to zoom. Click or drag the minimap to navigate across the battlefield.
- **Mobile:** tap to select; hold, then sweep to box-select. Tap the ground to move or an enemy to attack. Drag empty ground to pan. Use **X** to clear selection and the Attack button to fight the nearest enemy.
- **Shortcuts:** **F2** selects your army; **F3** selects idle workers; **Space** returns home. Select troops and hold **1–4** to assign a command group (or use **Ctrl+1–4**); press the key to select it and double press to center the camera. On mobile, hold one of the four group boxes at the bottom to assign selected troops, tap to select, and double tap to center the camera. **Q** launches rocks, **W** crumples paper, and **E** sweeps scissors after researching their upgrades. Each key affects only that troop type in a mixed squad. **S** stops selected units. **X** clears selection. **Esc** opens or closes the pause menu; **P** also pauses.
- Select a desk to train twigs (**Q**); select a printer to train rock/paper/scissors (**Q/W/E**, with **R** also training scissors) or research upgrades. Select a twig, choose a building, then choose clear, currently visible ground to build. Ability buttons show their key, research lock, and cooldown.

The battlefield fills the screen, with a compact selection panel and no top menu bar. Use the small pause button or **Esc** for the menu. The tutorial is available through **Learn the ropes** in the lobby, **How to play** in the pause menu, or **? / F1** during play.

## Economy and combat

Twigs cost **25 trash**. Each carries **5 trash per trip** from a trash can back to a home desk. One twig can collect from each can at a time. Everyone can attack twigs, so protect your economy and build desks near new resources.

Rocks, paper, and scissors each cost **50 trash** and use one supply. Rock beats scissors, scissors beat paper, and paper beats rock. Combat troops attack once every **1.25 seconds**. Troops refuse both automatic and explicit targets that are their exact counter (rock cannot target paper, paper cannot target scissors, scissors cannot target rock). Matching types can fight, and combat troops can damage buildings and twigs. A home desk costs **200 trash** and provides 12 supply; a cabinet costs **75** and provides 8 more; a printer costs **150** and produces troops.

Squads move in formations, push apart to avoid stacking, and find routes through obstacles and narrow choke points. Use wider routes for large armies and keep your resource lanes clear.

## Terrain and scouting

All six layouts are **4800 × 3200 world pixels**, double the original width and height and four times the playable area. Unit sizes, movement speeds, weapon ranges, and local harvesting trips retain their original scale. Each layout has **4–6 raised areas**, **10–11 expansion locations**, broad ramps, layered cliff edges, secondary cover clusters, and marked resource fields. Desktop Meadow combines paper groves, cork flats, and circuit yards; Breakroom Blitz adds napkin groves, service tiles, and coffee basins. Ground units must use ramps to reach elevated areas; flying CDs and Rock Launch can cross cliffs. Buildings cannot block the ramps.

High ground grants an extra **100 world pixels of sight**. Low-ground troops cannot see uphill or through a raised shelf, while flying CDs can scout over it. These vision rules apply to every team, including bots. Elevation changes sight and access; it does not add a damage bonus.

The battlefield and minimap distinguish three scouting states: unexplored ground is dark, previously explored terrain is dimmed, and currently visible ground is clear. Enemies, resource cans, projectiles, and effects require current vision. Hidden enemy units cannot be clicked or targeted; attack-move into the fog to scout and engage what your troops discover. Explored terrain stays remembered after your scouts leave. New construction requires current sight of its location.

Static scenery and fog textures are cached, with the scenery cache capped at eight megapixels. Offscreen units are culled before sorting, and unchanged HUD values avoid repeated DOM writes. Squads share cached navigation clearance, invalidated when buildings change, and new routes are scheduled across frames. Vision refreshes eight times per second and the HUD checks ten times per second, while movement and drawing continue each animation frame. Resizing preserves the camera's world center.

Each troop's printer upgrade costs **150 trash**:

- **Rock — Launch (Q):** choose a target spot; selected ready rocks leap and land in a close formation.
- **Scissors — Sweep (E):** choose a direction; selected ready scissors slash a short forward arc, defeating paper caught inside it.
- **Paper — Crumple (W):** immediately become invulnerable for 3 seconds and move faster; paper cannot attack while crumpled.

Opponents gather trash, train troops, construct buildings, expand, research, and attack. Difficulty changes their planning speed and aggression rather than granting free resources.

## Flying CDs and pointer defense

Select a twig and build a **CD reader** for **175 trash** (hotkey **V**, 10 seconds of construction). Select the reader and summon a **Flying CD** for **75 trash** (**Q**, 8 seconds). Each CD uses one supply and has room for **six rock, paper, or scissors troops**. Twigs cannot board. Passengers retain their health and supply use and cannot move, attack, use abilities, or be targeted while aboard.

- Select troops and tap/click a friendly CD (or right-click it) to board. Distant troops walk to it; spare selected troops stay on the ground when its six seats are reserved.
- Select one or several CDs, then **Load / L** to bring aboard the nearest available combat troops. Shift-click or box-select CDs for a group operation.
- Choose **Drop / U**, then click/tap a world or minimap destination. Every selected loaded CD flies over scenery to the area and unloads onto separate clear ground positions. A destroyed CD releases surviving passengers on safe nearby ground.
- CDs show six seat pips and a passenger count; the selection panel shows combined cargo. Recalling a command group whose troops are aboard selects their CDs, then returns to the original troops after landing.

Build a **defensive computer mouse** with a twig for **175 trash** (**M**, 8 seconds of construction). It fires visible computer-pointer projectiles at enemy CDs and ground rock, paper, and scissors troops within 360 world pixels. Each click deals **28 impact damage**, then **6 damage per second for 3 seconds**. Repeated clicks from one mouse refresh its lingering damage; separate mice each apply their own damage. It ignores workers, friendly units, buildings, and embarked troops; paper's invulnerability protects it. Ground troops can destroy defensive mice as buildings. Bots spend their own trash on mouse defenses when hostile CDs approach their desks.

## Fortifications and scouting

Select a Twig and use **Wall / Z** to place **20-trash, 1,200-HP wall blocks**. Each takes **3 seconds** to build and snaps to a **48 × 48** square grid. Adjacent blocks connect into continuous rows, corners and junctions. Placement stays active for repeated clicks or taps; **Done placing / Esc** exits the mode without canceling construction. Twigs complete their current job before building queued blocks. Unreachable placement is rejected before payment.

Walls block both armies' ground movement. Aircraft and projectiles pass over them. Individual blocks can be destroyed to open a breach; normal troops can use a single-block opening, while large Rolling Rocks need a wider opening. Select an owned block and choose **Dismantle block** to remove it without a refund. The artwork includes shaded cardboard layers, pencil rails, binder clips, metal couplers, team trim, construction progress and battle damage.

Defensive mice now have **850 HP**, **360 range**, and fire **28 impact damage** every **1.25 seconds**, plus **6 damage per second for 3 seconds**. Their reinforced mounts and twin pointer launchers show the strength increase. Paper Airplanes now scout a **900-pixel radius**, with air vision across cliffs; Flying CDs retain their 520-pixel radius. Walls reveal only their immediate surroundings.

## Artwork

The game includes original raster artwork created with built-in image generation: expressive rock, paper, scissors, and twig characters with arms and legs, charcoal outlines, and warm cream, teal, and orange colors. The lobby illustration shows a tabletop battle with twig workers, trash cans, a desk headquarters, a printer, and a cabinet. Optimized play assets are in `assets/`; generation prompts are recorded in `ARTWORK.md`. See [artwork prompts and saved files](ARTWORK.md) for the exact generation prompts, tool mode, and asset paths.


The CD reader, flying CDs, defensive computer mouse, cursor projectiles, and transport effects use resolution-independent Canvas artwork matching the existing building illustrations. Disc surfaces spin and shimmer, the mouse clicks as it fires, and cursor trails show each shot in flight.
