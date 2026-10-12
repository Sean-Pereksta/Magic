# Turn Craft map terrain atlases

Generated independently with the built-in OpenAI `image_gen` tool on 2026-10-12. All five outputs were visually inspected, normalized to 1024 × 1024 pixels with Lanczos resampling, and encoded as RGB WebP at quality 88. Originals remain in Codex generated-image storage.

Each production atlas is an exact 2 × 2 layout for runtime cropping into 512 × 512 source regions:

| Region | Source origin | Terrain role |
| --- | --- | --- |
| Upper left | `(0, 0)` | Traversable primary ground |
| Upper right | `(512, 0)` | Traversable lane/path |
| Lower left | `(0, 512)` | Impassable obstacle/rock surface |
| Lower right | `(512, 512)` | Expansion clearing |

These original overhead material textures contain no units, buildings, mineral objects, gas objects, or baked text. Their restrained detail is intended for sampling down to the 56px board cells.

## ember-rift

- Production asset: `ember-rift.webp`
- Original source: `exec-1e019ed0-1d26-4249-bddc-0e52193a96d6.png`

Final prompt:

```text
Use case: stylized-concept.
Asset type: production terrain texture atlas for the top-down science-fiction RTS game Turn Craft.
Primary request: generate exactly one square 1024x1024 texture atlas arranged as an exact 2x2 grid of four equal 512x512 texture regions. No margins, no gutters, no outlines, no labels, no text; the quadrant boundaries lie exactly at x=512 and y=512.
Style/medium: polished hand-painted realistic RTS environment texture, directly overhead orthographic view looking vertically down at flat terrain surfaces, richly tactile but controlled and low contrast. These four texture regions are sampled by code and each repeats as small 56px terrain tiles.
Composition/framing: each quadrant is a uniform seamless repeatable terrain texture with material variation distributed evenly, details continuing naturally across its own opposing edges. No focal objects or perspective. Each quadrant must remain one flat surface across the entire quadrant.
Lighting: soft neutral ambient overhead light, no directional cast shadows, no sunbeam, no vignette, no horizons.
Quadrant layout: upper-left is traversable primary terrain; upper-right is contrasting traversable lane/path surface; lower-left is solid impassable rock/cliff or obstacle surface; lower-right is an expansion clearing made of an alternate traversable terrain surface.
Constraints: full bleed four-quadrant texture atlas only; no buildings, units, creatures, vehicles, plants standing up, resource nodes, crystals, mineral objects, gas objects, icons, numbers, words, logos, UI, borders, gridlines, or watermarks. Do not add diagonal paths across quadrants or mix the different quadrant materials. Avoid high-frequency noise and extreme bright highlights that would compete with game units.
Map theme: Ember Rift, a volcanic battlefield of cooled lava, basalt, black ash and restrained orange seams.
Upper-left quadrant: charcoal flat basalt ground with small cooled lava plates, dark gray roughness and very sparse thin dull ember-orange fissures.
Upper-right quadrant: flatter worn black volcanic ash lane, visibly lighter ash-gray powder and fine subtle compression streaks, mostly unbroken navigable surface.
Lower-left quadrant: dense solid jagged dark basalt rock faces seen vertically from above, broad chunky angular black stone plates with rust-orange mineral seams; unmistakably rugged and blocked.
Lower-right quadrant: flat cleared burnt-gray volcanic gravel and dark cinder earth, subtle brown-orange undertones, comparatively open and smooth.
Color palette: charcoal, graphite, warm ash gray, tiny restrained burnt-orange details; no giant glowing lava pools.
```

## frostline-basin

- Production asset: `frostline-basin.webp`
- Original source: `exec-4ec5543b-c7a0-4e6f-af93-2ee9977d984a.png`

Final prompt:

```text
Use case: stylized-concept.
Asset type: production terrain texture atlas for the top-down science-fiction RTS game Turn Craft.
Primary request: generate exactly one square 1024x1024 texture atlas arranged as an exact 2x2 grid of four equal 512x512 texture regions. No margins, no gutters, no outlines, no labels, no text; the quadrant boundaries lie exactly at x=512 and y=512.
Style/medium: polished hand-painted realistic RTS environment texture, directly overhead orthographic view looking vertically down at flat terrain surfaces, richly tactile but controlled and low contrast. These four texture regions are sampled by code and each repeats as small 56px terrain tiles.
Composition/framing: each quadrant is a uniform seamless repeatable terrain texture with material variation distributed evenly, details continuing naturally across its own opposing edges. No focal objects or perspective. Each quadrant must remain one flat surface across the entire quadrant.
Lighting: soft neutral ambient overhead light, no directional cast shadows, no sunbeam, no vignette, no horizons.
Quadrant layout: upper-left is traversable primary terrain; upper-right is contrasting traversable lane/path surface; lower-left is solid impassable rock/cliff or obstacle surface; lower-right is an expansion clearing made of an alternate traversable terrain surface.
Constraints: full bleed four-quadrant texture atlas only; no buildings, units, creatures, vehicles, plants standing up, resource nodes, crystals, mineral objects, gas objects, icons, numbers, words, logos, UI, borders, gridlines, or watermarks. Do not add diagonal paths across quadrants or mix the different quadrant materials. Avoid high-frequency noise and extreme bright highlights that would compete with game units.
Map theme: Frostline Basin, a frozen pale blue and white snow-and-ice battlefield.
Upper-left quadrant: flat blue-white packed snow with broad soft granular variation and faint icy compressed ripples.
Upper-right quadrant: exposed smooth pale steel-blue frozen ice lane, translucent matte blue patches and thin subtle frost tracery, no holes or water.
Lower-left quadrant: dense solid rough glacial ice and frost-covered stone plates seen vertically from above, chunky irregular blue-gray blocks capped in pale snow, unmistakably rugged and blocked.
Lower-right quadrant: broad flat fresh snow clearing in soft white with light powdery wind-swept grain, slightly warmer and smoother than the packed snow.
Color palette: pale frost white, icy powder blue, slate blue, softly muted contrast; avoid brilliant dazzling highlights.
```

## verdant-divide

- Production asset: `verdant-divide.webp`
- Original source: `exec-19732249-4c03-4e12-9642-22cf16616b17.png`

Final prompt:

```text
Use case: stylized-concept.
Asset type: production terrain texture atlas for the top-down science-fiction RTS game Turn Craft.
Primary request: generate exactly one square 1024x1024 texture atlas arranged as an exact 2x2 grid of four equal 512x512 texture regions. No margins, no gutters, no outlines, no labels, no text; the quadrant boundaries lie exactly at x=512 and y=512.
Style/medium: polished hand-painted realistic RTS environment texture, directly overhead orthographic view looking vertically down at flat terrain surfaces, richly tactile but controlled and low contrast. These four texture regions are sampled by code and each repeats as small 56px terrain tiles.
Composition/framing: each quadrant is a uniform seamless repeatable terrain texture with material variation distributed evenly, details continuing naturally across its own opposing edges. No focal objects or perspective. Each quadrant must remain one flat surface across the entire quadrant.
Lighting: soft neutral ambient overhead light, no directional cast shadows, no sunbeam, no vignette, no horizons.
Quadrant layout: upper-left is traversable primary terrain; upper-right is contrasting traversable lane/path surface; lower-left is solid impassable rock/cliff or obstacle surface; lower-right is an expansion clearing made of an alternate traversable terrain surface.
Constraints: full bleed four-quadrant texture atlas only; no buildings, units, creatures, vehicles, plants standing up, resource nodes, crystals, mineral objects, gas objects, icons, numbers, words, logos, UI, borders, gridlines, or watermarks. Do not add diagonal paths across quadrants or mix the different quadrant materials. Avoid high-frequency noise and extreme bright highlights that would compete with game units.
Map theme: Verdant Divide, a lush emerald jungle battlefield represented through the ground surfaces.
Upper-left quadrant: flat mossy emerald jungle earth with densely interwoven low groundcover texture and small mottled olive leaf litter pressed into the ground; no upright plants.
Upper-right quadrant: packed warm dark-brown jungle soil lane with a smooth worn surface, sparse flattened muted green leaf litter and fine roots.
Lower-left quadrant: dense solid moss-covered deep green and gray rock formations seen vertically from above, broad chunky rugged stone surfaces and dark crevices, unmistakably blocked; no trees.
Lower-right quadrant: flat cleared dark fertile earth with a thin olive-green moss carpet, subtly open smooth patches and restrained tiny leaves.
Color palette: rich emerald, forest green, olive, warm earth brown, dark slate; soft controlled saturation and contrast.
```

## shattered-dunes

- Production asset: `shattered-dunes.webp`
- Original source: `exec-331ee6a0-4a81-4c4d-9500-a1210a7bf175.png`

Final prompt:

```text
Use case: stylized-concept.
Asset type: production terrain texture atlas for the top-down science-fiction RTS game Turn Craft.
Primary request: generate exactly one square 1024x1024 texture atlas arranged as an exact 2x2 grid of four equal 512x512 texture regions. No margins, no gutters, no outlines, no labels, no text; the quadrant boundaries lie exactly at x=512 and y=512.
Style/medium: polished hand-painted realistic RTS environment texture, directly overhead orthographic view looking vertically down at flat terrain surfaces, richly tactile but controlled and low contrast. These four texture regions are sampled by code and each repeats as small 56px terrain tiles.
Composition/framing: each quadrant is a uniform seamless repeatable terrain texture with material variation distributed evenly, details continuing naturally across its own opposing edges. No focal objects or perspective. Each quadrant must remain one flat surface across the entire quadrant.
Lighting: soft neutral ambient overhead light, no directional cast shadows, no sunbeam, no vignette, no horizons.
Quadrant layout: upper-left is traversable primary terrain; upper-right is contrasting traversable lane/path surface; lower-left is solid impassable rock/cliff or obstacle surface; lower-right is an expansion clearing made of an alternate traversable terrain surface.
Constraints: full bleed four-quadrant texture atlas only; no buildings, units, creatures, vehicles, plants standing up, resource nodes, crystals, mineral objects, gas objects, icons, numbers, words, logos, UI, borders, gridlines, or watermarks. Do not add diagonal paths across quadrants or mix the different quadrant materials. Avoid high-frequency noise and extreme bright highlights that would compete with game units.
Map theme: Shattered Dunes, golden desert sand, red sandstone and weathered ancient stone.
Upper-left quadrant: flat matte golden-tan desert sand with broad very shallow soft ripples and a few tiny scattered rust-red grains.
Upper-right quadrant: hard-packed pale terracotta sandy lane with smooth worn dusty surface and subtle wind streaks, no walls.
Lower-left quadrant: dense solid rugged red sandstone formations seen vertically from above, broad angular ochre and rust stone plates with darker sediment crevices, unmistakably blocked.
Lower-right quadrant: flat expansion clearing of weathered warm gray-brown ancient stone paving half-buried in golden sand, large irregular restrained slab joins, smooth traversable surface; no inscriptions.
Color palette: warm golden sand, muted ochre, terracotta, rust red, dusty taupe; no intense sun shadows.
```

## orbital-foundry

- Production asset: `orbital-foundry.webp`
- Original source: `exec-25fc8819-55ee-418f-8cfb-170d5a28848b.png`

Final prompt:

```text
Use case: stylized-concept.
Asset type: production terrain texture atlas for the top-down science-fiction RTS game Turn Craft.
Primary request: generate exactly one square 1024x1024 texture atlas arranged as an exact 2x2 grid of four equal 512x512 texture regions. No margins, no gutters, no outlines, no labels, no text; the quadrant boundaries lie exactly at x=512 and y=512.
Style/medium: polished hand-painted realistic RTS environment texture, directly overhead orthographic view looking vertically down at flat terrain surfaces, richly tactile but controlled and low contrast. These four texture regions are sampled by code and each repeats as small 56px terrain tiles.
Composition/framing: each quadrant is a uniform seamless repeatable terrain texture with material variation distributed evenly, details continuing naturally across its own opposing edges. No focal objects or perspective. Each quadrant must remain one flat surface across the entire quadrant.
Lighting: soft neutral ambient overhead light, no directional cast shadows, no sunbeam, no vignette, no horizons.
Quadrant layout: upper-left is traversable primary terrain; upper-right is contrasting traversable lane/path surface; lower-left is solid impassable rock/cliff or obstacle surface; lower-right is an expansion clearing made of an alternate traversable terrain surface.
Constraints: full bleed four-quadrant texture atlas only; no buildings, units, creatures, vehicles, plants standing up, resource nodes, crystals, mineral objects, gas objects, icons, numbers, words, logos, UI, borders, gridlines, or watermarks. Do not add diagonal paths across quadrants or mix the different quadrant materials. Avoid high-frequency noise and extreme bright highlights that would compete with game units.
Map theme: Orbital Foundry, a science-fiction industrial station deck in dark navy steel with subdued amber and cyan details.
Upper-left quadrant: flat dark navy-blue industrial steel deck surface of broad square and rectangular worn panels, subtle brushed metal grain, restrained rivets and seams.
Upper-right quadrant: contrasting slightly lighter slate-gray metal maintenance lane with fine repeating nonslip ribs and a sparse thin muted amber stripe integrated into the texture, no text.
Lower-left quadrant: dense solid rugged reinforced dark gunmetal mechanical obstacle plating seen vertically from above, chunky armored steel housings and deep narrow seams, uniformly filled blocked material; no free-standing machines or equipment.
Lower-right quadrant: cleared flat blue-gray assembly deck of larger smooth metal panels with sparse tiny muted cyan embedded floor lights and subtle panel joints.
Color palette: midnight navy, blue-gray, gunmetal, very sparse subdued amber and cyan accents; matte metal, no glossy glare, no control screens.
```
