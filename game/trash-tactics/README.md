# Trash Tactics

A quick, cartoony RTS about turning office trash into a tiny empire. Desktop Meadow and Breakroom Blitz each have 2-, 3-, and 4-player layouts: six battleground layouts in all. Play a free-for-all against one, two, or three rival bots at Easy, Normal, or Hard difficulty. Destroy every rival desk to win.

## Play locally

Run `npm start` in this folder, then open the local address printed in the terminal. No installation or external services are required.

## Controls

- **Desktop:** click a unit or building; drag a box to select a squad. Shift adds to the selection. Right-click to move, gather from a trash can, or attack an enemy. Press **A**, then right-click an area to attack-move toward it. The Attack button targets the nearest enemy. Pan with the arrow keys or middle-mouse drag; use the mouse wheel to zoom.
- **Mobile:** tap to select; hold, then sweep to box-select. Tap the ground to move or an enemy to attack. Drag empty ground to pan. Use **X** to clear selection and the Attack button to fight the nearest enemy.
- **Shortcuts:** **F2** selects your army; **F3** selects idle workers; **Space** returns home. Select troops and hold **1–4** to assign a command group (or use **Ctrl+1–4**); press the key to select it and double press to center the camera. On mobile, hold one of the four group boxes at the bottom to assign selected troops, tap to select, and double tap to center the camera. **E** uses an unlocked ability; **S** stops selected units. **X** clears selection. **Esc** opens or closes the pause menu; **P** also pauses.
- Select a desk to train twigs; select a printer to train troops or research upgrades. Select a twig, choose a building, then choose clear ground to build.

The battlefield fills the screen, with a compact selection panel and no top menu bar. Use the small pause button or **Esc** for the menu. The tutorial is available through **Learn the ropes** in the lobby, **How to play** in the pause menu, or **? / F1** during play.

## Economy and combat

Twigs cost **25 trash**. Each carries **5 trash per trip** from a trash can back to a home desk. One twig can collect from each can at a time. Everyone can attack twigs, so protect your economy and build desks near new resources.

Rocks, paper, and scissors each cost **50 trash** and use one supply. Rock beats scissors, scissors beat paper, and paper beats rock. Combat troops attack once every **1.25 seconds**. Troops refuse both automatic and explicit targets that are their exact counter (rock cannot target paper, paper cannot target scissors, scissors cannot target rock). Matching types can fight, and combat troops can damage buildings and twigs. A home desk costs **200 trash** and provides 12 supply; a cabinet costs **75** and provides 8 more; a printer costs **150** and produces troops.

Squads move in formations, push apart to avoid stacking, and find routes through obstacles and narrow choke points. Use wider routes for large armies and keep your resource lanes clear.

Each troop's printer upgrade costs **150 trash**:

- **Rock — Launch:** leap toward a chosen spot; selected rocks land in a close formation.
- **Scissors — Sweep:** slash a short forward arc, defeating paper caught inside it.
- **Paper — Crumple:** become invulnerable for 3 seconds and move faster; paper cannot attack while crumpled.

Opponents gather trash, train troops, construct buildings, expand, research, and attack. Difficulty changes their planning speed and aggression rather than granting free resources.

## Flying CDs and pointer defense

Select a twig and build a **CD reader** for **175 trash** (hotkey **V**, 10 seconds of construction). Select the reader and summon a **Flying CD** for **75 trash** (**Q**, 8 seconds). Each CD uses one supply and has room for **six rock, paper, or scissors troops**. Twigs cannot board. Passengers retain their health and supply use and cannot move, attack, use abilities, or be targeted while aboard.

- Select troops and tap/click a friendly CD (or right-click it) to board. Distant troops walk to it; spare selected troops stay on the ground when its six seats are reserved.
- Select one or several CDs, then **Load / L** to bring aboard the nearest available combat troops. Shift-click or box-select CDs for a group operation.
- Choose **Drop / U**, then click/tap a world or minimap destination. Every selected loaded CD flies over scenery to the area and unloads onto separate clear ground positions. A destroyed CD releases surviving passengers on safe nearby ground.
- CDs show six seat pips and a passenger count; the selection panel shows combined cargo. Recalling a command group whose troops are aboard selects their CDs, then returns to the original troops after landing.

Build a **defensive computer mouse** with a twig for **175 trash** (**M**, 8 seconds of construction). It fires visible computer-pointer projectiles at enemy CDs and ground rock, paper, and scissors troops within 310 world pixels. Each click deals **12 impact damage**, then **4 damage per second for 3 seconds**. Repeated clicks from one mouse refresh its lingering damage; separate mice each apply their own damage. It ignores workers, friendly units, buildings, and embarked troops; paper's invulnerability protects it. Ground troops can destroy defensive mice as buildings. Bots spend their own trash on mouse defenses when hostile CDs approach their desks.

## Artwork

The game includes original raster artwork created with built-in image generation: expressive rock, paper, scissors, and twig characters with arms and legs, charcoal outlines, and warm cream, teal, and orange colors. The lobby illustration shows a tabletop battle with twig workers, trash cans, a desk headquarters, a printer, and a cabinet. Optimized play assets are in `assets/`; generation prompts are recorded in `ARTWORK.md`. See [artwork prompts and saved files](ARTWORK.md) for the exact generation prompts, tool mode, and asset paths.


The CD reader, flying CDs, defensive computer mouse, cursor projectiles, and transport effects use resolution-independent Canvas artwork matching the existing building illustrations. Disc surfaces spin and shimmer, the mouse clicks as it fires, and cursor trails show each shot in flight.
