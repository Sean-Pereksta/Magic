# TurnCraft faction expansion artwork

All 12 assets live in `graphics/turncraft/` and are loaded from the repository through the existing TurnCraft image pipeline. Filenames retain spaces to match the game's established naming convention. No external image hosting is required.

## Format and visual direction

- 384 × 384 RGBA PNGs with real transparency; no card borders, UI, or baked background.
- Visible alpha bounds fit within 352 × 352 pixels and are centered with at least 16 pixels of transparent padding.
- PNGs use lossless optimization. The complete set is approximately 2.09 MB (2.00 MiB).
- Detailed painted 2D sci-fi art, three-quarter perspective, distinct silhouettes. Frontier uses industrial navy/gunmetal armor and cyan lights; Ascendant uses ivory/gold plating and violet crystals; Brood uses crimson chitin and toxic-green biology.
- Existing Goliath, High Templar, and Roach assets were inspected for the game's painted material style. The expansion art is original and removes the portrait frames found in those references.

## Asset and state mapping

| Unit | Filename | State | Readable features |
| --- | --- | --- | --- |
| Suppressor Exosuit | suppressor exosuit.png | Mobile/combat base | Twin rotary barrels, broad shoulder silhouette, cyan optics |
| Pulse Operative | pulse operative.png | Mobile/combat base | Narrow soldier, large cyan electromagnetic backpack |
| Forward Fabricator | forward fabricator.png | Mobile/undeployed | Six wheels, folded cranes, closed manufacturing roof |
| Forward Fabricator | forward fabricator deployed.png | `fabricatorDeployed` | Open cyan assembly platform, extended stabilizers and cranes |
| Lattice Skiff | lattice skiff.png | Mobile/channel base | Ivory/gold crescent aircraft, violet crystal and spatial rings |
| Chronoweaver | chronoweaver.png | Mobile/cast base | Floating violet robes, gold clocklike rings |
| Void Lancer | void lancer.png | Mobile/charge base | Gold robotic legs, long violet crystal lance |
| Tunnel Maw | tunnel maw.png | Mobile | Crimson segmented worm, circular green throat |
| Tunnel Maw / tunnel exit | tunnel maw open.png | `tunnelDigging`, `tunnelExitId`, or `tunnelExit` | Low living opening, radial organic teeth and green depth |
| Sporeback | sporeback.png | Mobile/cast base | Crimson beetle, large luminous green spore sacs |
| Brood Matriarch | brood matriarch.png | Mobile/cast base | Broad armored abdomen with visible green egg sacs |
| Broodling | broodling.png | Temporary summoned combat unit | Small aggressive crimson insect with hooked claws |

The alternate Fabricator image is an edited state of the new mobile Fabricator image. The open Maw image is an edited state of the new Maw, so transformed units keep their visual identity. Base poses remain the same during ordinary movement and combat; lightweight canvas effects provide animated ability feedback rather than large sprite sheets.

## Ability visual mapping

| Ability | Canvas feedback |
| --- | --- |
| Suppression Sweep | Warm orange cone and nine moving tracer streaks |
| Disruption Pulse | Expanding cyan electrical ring and eight crackling arcs |
| Field Muster | Cyan deployment outline/progress ring, then deployed factory artwork |
| Warp Conduit | Violet channel ellipse, orbiting geometric markers and source connection |
| Temporal Distortion | Outlined transparent violet circle, rotating dashed ring and ten particles |
| Void Spear | Violet charge ring, target line and countdown; bright narrow beam on release |
| Tunnel Breach | Green digging ring, organic opening sprite and dashed source connection |
| Spore Bloom | Outlined translucent green cloud/circle and ten drifting biological particles |
| Blood Clutch | Pulsing luminous green egg ellipses followed by Broodling sprites |

These effects use the existing area-effect lifecycle, fog visibility checks, and command targeting previews. Selection indicators and health/shield bars are drawn separately by the game.

## Generation and optimization

Generation used the built-in image generation tool with `transparent_background: true`, one separate call per requested asset. Ten assets were generated from text; two alternate states used their matching new base artwork as edit targets. No CLI/API fallback was used. Outputs were inspected visually, converted to RGBA, cropped to their alpha bounds, downscaled with a Lanczos filter, centered in a 384-pixel transparent square, and saved with PNG optimization. Every final file was checked for alpha extrema 0 and 255.

## Complete prompt set

### suppressor exosuit.png

```text
Use case: stylized-concept. Asset type: production-ready single unit sprite for the TurnCraft RTS battlefield. Style: professional detailed 2D sci-fi painted game art, readable bold silhouette, realistic painted metal with crisp edges. Three-quarter isometric view from slightly above, entire subject centered inside a square canvas, 12 percent transparent padding on all sides. One subject only, no card frame, no border, no environment, no terrain base, no UI, no text, no watermark. True transparent alpha background, including empty space between limbs. Bright enough to read on dark terrain at small size. Subject: bulky intimidating Frontier Directorate bipedal military exoskeleton, massive dark gunmetal and muted navy shoulder plates with worn brass metal trim, cyan optics, industrial hydraulic legs. Both arms carry heavy six-barrel rotary guns with clearly visible barrels and exposed ammunition belts. Compact broad heavy silhouette. Stable combat stance with twin guns aimed forward to the lower-left. No rocket pods. No firing effects yet. Original design.
```

### pulse operative.png

```text
Use case: stylized-concept. Asset type: production-ready single unit sprite for the TurnCraft RTS battlefield. Style: professional detailed 2D sci-fi painted game art, readable bold silhouette, realistic painted metal with crisp edges. Three-quarter isometric view from slightly above, entire subject centered inside a square canvas, 12 percent transparent padding on all sides. One subject only, no card frame, no border, no environment, no terrain base, no UI, no text, no watermark. True transparent alpha background, including empty space between limbs. Bright enough to read on dark terrain at small size. Subject: a sleek lightly armored Frontier Directorate electronic-warfare soldier. Dark tactical gunmetal and muted navy armor, cyan helmet optical lenses, a very conspicuous rounded glowing electromagnetic backpack with two cyan power coils, compact cyan pulse launcher held ready in both hands. Full-body soldier with narrow agile silhouette. Facing lower-left. Clear distinction from a bulky heavy gunner. Original design.
```

### forward fabricator.png

```text
Use case: stylized-concept. Asset type: production-ready single unit sprite for the TurnCraft RTS battlefield. Style: professional detailed 2D sci-fi painted game art, readable bold silhouette, realistic painted metal with crisp edges. Three-quarter isometric view from slightly above, entire subject centered inside a square canvas, 12 percent transparent padding on all sides. One subject only, no card frame, no border, no environment, no terrain base, no UI, no text, no watermark. True transparent alpha background, including empty space between limbs. Bright enough to read on dark terrain at small size. Subject: a large six-wheeled Frontier Directorate armored mobile factory vehicle with exactly three chunky wheels per side. Dark gunmetal and muted navy industrial armor with worn brass edging and cyan running lights. Long angular hull, folded armored roof manufacturing platform, two folded mechanical assembly cranes on top, retractable stabilizer outriggers visibly stowed. Mobile travelling state, roof closed, tools folded. No gun turret. Three-quarter view pointed toward lower-left. Original design.
```

### lattice skiff.png

```text
Use case: stylized-concept. Asset type: production-ready single unit sprite for the TurnCraft RTS battlefield. Style: professional detailed 2D sci-fi painted game art, readable bold silhouette, ornate metal with crisp edges. Three-quarter isometric view from slightly above, entire subject centered inside a square canvas, transparent padding on all sides. One subject only, no card frame, no border, no environment, no terrain base, no UI, no text, no watermark. True transparent alpha background, including empty space between limbs or rings. Bright enough to read on dark terrain at small size. Subject: elegant Ascendant Conclave crescent-shaped hovering spacecraft with smooth ivory-white plating and ornate burnished gold structural trim. One large glowing purple crystal reactor in the center, several floating golden geometric energy rings encircling the crystal. Compact crescent spaceship silhouette pointing to lower-left, arms extend outward, no legs or wheels. Distinct graceful aircraft, purple propulsion light. Original design.
```

### chronoweaver.png

```text
Use case: stylized-concept. Asset type: production-ready single unit sprite for the TurnCraft RTS battlefield. Style: professional detailed 2D sci-fi painted game art, readable bold silhouette, ornate metal with crisp edges. Three-quarter isometric view from slightly above, entire subject centered inside a square canvas, transparent padding on all sides. One subject only, no card frame, no border, no environment, no terrain base, no UI, no text, no watermark. True transparent alpha background, including empty space between limbs or rings. Bright enough to read on dark terrain at small size. Subject: tall floating Ascendant Conclave psionic figure wearing flowing elaborate violet robes and ornate golden shoulder and chest armor, faceless luminous purple visor. Distinct silhouette has exactly three separated floating circular golden rings like futuristic clock mechanisms surrounding its shoulders and torso, with purple crystals inset into the rings. Full figure levitating, robes taper downward, arms outstretched slightly to manipulate time. Facing lower-left. Original design.
```

### void lancer.png

```text
Use case: stylized-concept. Asset type: production-ready single unit sprite for the TurnCraft RTS battlefield. Style: professional detailed 2D sci-fi painted game art, readable bold silhouette, ornate metal with crisp edges. Three-quarter isometric view from slightly above, entire subject centered inside a square canvas, transparent padding on all sides. One subject only, no card frame, no border, no environment, no terrain base, no UI, no text, no watermark. True transparent alpha background, including empty space between limbs or rings. Bright enough to read on dark terrain at small size. Subject: tall Ascendant Conclave golden robotic siege walker with exactly three clearly separated articulated legs supporting a central armored ivory and gold body. Enormous elongated purple crystalline energy lance/cannon mounted in center and aimed toward lower-left. Triangular tripod silhouette, faceted purple crystalline cannon is the focal point. Delicate intricate golden plating around durable mechanical joints, violet energy accents. No humanoid head, no arms, no ordinary rifle. Original design.
```

### tunnel maw.png

```text
Use case: stylized-concept. Asset type: production-ready single unit sprite for the TurnCraft RTS battlefield. Style: professional detailed 2D sci-fi painted game art, readable bold silhouette, organic chitin textures with crisp outlines. Three-quarter isometric view from slightly above, entire subject centered inside a square canvas, transparent padding on all sides. One subject only, no card frame, no border, no environment, no terrain base, no UI, no text, no watermark. True transparent alpha background, including empty space between legs. Bright enough to read on dark terrain at small size. Subject: gigantic segmented Brood Dominion burrowing alien worm. Thick overlapping crimson and deep red chitin armored segments form a compact curled body rising up toward camera. Enormous circular toothed mouth with hooked digging claws around head and a luminous toxic-green inner throat. Several heavy small hooked digging limbs on its front segments. Intimidating worm silhouette, head aimed lower-left, visible complete curved tail. Above-ground mobile creature state, no surrounding earth. Original design.
```

### sporeback.png

```text
Use case: stylized-concept. Asset type: production-ready single unit sprite for the TurnCraft RTS battlefield. Style: professional detailed 2D sci-fi painted game art, readable bold silhouette, organic chitin textures with crisp outlines. Three-quarter isometric view from slightly above, entire subject centered inside a square canvas, transparent padding on all sides. One subject only, no card frame, no border, no environment, no terrain base, no UI, no text, no watermark. True transparent alpha background, including empty space between legs. Bright enough to read on dark terrain at small size. Subject: thick six-legged Brood Dominion beetle-like alien anti-air creature. Heavy crimson and deep red organic chitin armor on a compact low body. Exactly six splayed hooked legs. Oversized luminous toxic-green fungal sacs clustered on its back, with one central biological spore cannon mouth pointing upward. Massive rounded fungal sac back silhouette distinct from armored melee creature. Sharp small tusked head pointing lower-left. Original design.
```

### brood matriarch.png

```text
Use case: stylized-concept. Asset type: production-ready single unit sprite for the TurnCraft RTS battlefield. Style: professional detailed 2D sci-fi painted game art, readable bold silhouette, organic chitin textures with crisp outlines. Three-quarter isometric view from slightly above, entire subject centered inside a square canvas, transparent padding on all sides. One subject only, no card frame, no border, no environment, no terrain base, no UI, no text, no watermark. True transparent alpha background, including empty space between legs. Bright enough to read on dark terrain at small size. Subject: huge queen-like Brood Dominion broodmother, crimson and deep red thick armor plates, broad segmented abdomen covered in multiple translucent luminous green egg sacs. Six insectoid legs, two sharp long forelimbs, crown-like chitin horns around a small fierce insect head. Bulky low creature with wide abdomen, clearly visible eggs on sides, green biological light underneath plates. Facing lower-left. Original design.
```

### forward fabricator deployed.png

```text
Use case: precise-object-edit. Asset type: deployed-state sprite for TurnCraft Forward Fabricator. Input image: edit target, the exact vehicle to retain. Change only its operational state from mobile to a deployed field manufacturing station. Keep the same six-wheeled armored vehicle identity, dark gunmetal/navy plates, worn brass trim, cyan lights, lower-left orientation, camera angle, professional detailed 2D painted style. Extend four squat stabilizing legs from its sides. Open the armored roof panels outward to reveal a glowing cyan assembly platform. Raise the two mechanical assembly crane arms and bend them over the exposed machinery. No new vehicle, no extra units. Keep entire vehicle and deployed arms centered with transparent padding. True transparent alpha background preserved. No ground, no frame, no text.
```

### tunnel maw open.png

```text
Use case: precise-object-edit. Asset type: open tunnel-state sprite for TurnCraft Tunnel Maw. Input image: creature reference and edit target. Transform this segmented crimson-armored worm into its deployed open tunnel mouth state while keeping its original crimson chitin, ivory hooked teeth, circular toxic-green throat, detailed painted style. Instead of a tall worm torso, show a low broad circular living tunnel opening viewed from above at the same three-quarter isometric angle. Its fleshy crimson armored jaw petals and organic teeth spread around the rim. Luminous toxic green interior leads downward into dark depth. A small ring of cracked dark soil hugs the base, no square terrain slab. Hooked digging claws grip either side. Entire isolated organic tunnel entrance centered inside square with transparent margins. True transparent alpha background preserved, no frame, no UI, no text.
```

### broodling.png

```text
Use case: stylized-concept. Asset type: production-ready temporary summoned unit sprite for TurnCraft. Subject: one small aggressive Brood Dominion hatchling insect creature, compact crimson and deep red chitin armored body, six small hooked sprinting legs, two enlarged slashing front claws, tiny glowing toxic-green eyes and green abdominal glow. Low fast triangular silhouette, small pointed head with ivory fangs, tail short. Face lower-left in an attack-ready scuttle pose. Style: professional detailed 2D sci-fi painted game art with crisp outlines, matching crimson armored broodmother palette. Three-quarter isometric view from slightly above, entire single subject centered in square with transparent padding all sides. True transparent alpha background, including empty space between legs. No egg, no additional creatures, no terrain, no frame, no UI, no text, no watermark.
```

## Verification

The assets were checked for correct filenames, readable faction silhouettes, distinct mobile/deployed states, preserved true alpha, consistent dimensions, and absence of portrait borders. Browser asset loading and gameplay rendering should be validated through the game alongside the expansion integration tests.
