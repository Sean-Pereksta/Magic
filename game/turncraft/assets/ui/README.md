# Turn Craft command and result illustrations

Original faction-specific main-menu and victory-screen artwork, generated independently with the built-in OpenAI `image_gen` tool on 2026-10-13. Each output was visually inspected at full size, then encoded as RGB WebP at quality 88, method 6, without resizing, cropping, or visual editing. All three production images are 1536 × 1024 pixels (3:2), opaque.

The existing `assets/factions/{faction}.webp` images and their README were inspected as visual guidance before generation. Those existing images were not supplied as image-generation inputs. The prompts below explicitly describe the existing faction identities. All three new scenes leave the left third dark for live interface text and position the hero on the right so the same asset supports main-menu and match-result crops.

## Hosting and packaging

New binary artwork is intended for Cloudflare R2 rather than GitHub. Workspace copies in this directory are temporary packaging inputs and must be excluded from the code commit. Upload the ZIP's `effects` folder into the existing public bucket so these exact object keys exist:

| Faction | Workspace asset | Cloudflare object key |
| --- | --- | --- |
| Frontier Directorate | `frontier-command.webp` | `effects/ui/frontier-command.webp` |
| Ascendant Conclave | `ascendant-command.webp` | `effects/ui/ascendant-command.webp` |
| Brood Dominion | `brood-command.webp` | `effects/ui/brood-command.webp` |

The menu and result renderer use the existing public asset base and retain the existing faction artwork as a visual fallback until these objects are uploaded.

## Source provenance and exact final prompts

### frontier

- Generated source: `exec-b2d9abcb-7208-42de-9835-5810ae1c274d.png`
- Production filename: `frontier-command.webp`
- Intended deployment key: `effects/ui/frontier-command.webp`
- Generator: built-in OpenAI `image_gen`; no CLI or external art source.

```text
Use case: stylized-concept.
Asset type: premium Turn Craft science-fiction strategy game main-menu and victory-screen faction hero artwork, intended as a landscape background behind live HTML text.
Style/medium: richly detailed cinematic hand-painted 3D concept illustration, polished game key art, serious military science fiction, sharp readable character silhouette with atmospheric layered depth. Landscape 3:2 composition, approximately 1536x1024.
Composition/framing: one unmistakable faction commander or dominant creature occupies the right-center of the image, around 67 percent horizontal position, visible from the knees or foreground appendages upward. Hero should remain readable when cropped to a wide panoramic background. Supporting army and faction command architecture recede behind and to the right. The entire left third is dark low-detail atmospheric negative space with no face or focal object, to support UI text. Bottom foreground also dark. Full bleed without borders.
Lighting/mood: triumphant calm after a successful battle, commanding and majestic, radiant atmospheric rim lighting, subtle celebratory sparks or drifting energy without fireworks.
Constraints: entirely original science-fiction design. No text, letters, numerals, signage, logos, insignia, watermarks, UI elements, frames, franchise symbols, or gore. No baked-in title or victory words.
Primary request: illustrate the Frontier Directorate, disciplined industrial human military with practical gunmetal powered armor, blue/cyan technology, and restrained warm gold command lighting.
Scene/backdrop: victorious fortified command outpost atop a rocky alien plateau. Squat reinforced steel command buildings, antenna silhouettes, a large hovering industrial capital ship, and distant formations of infantry and tracked siege tanks visible through cool dust and mist on the right.
Subject: a resolute human infantry commander in angular scratched gunmetal armored exosuit, substantial closed helmet with a striking narrow cyan visor. Standing tall at the right-center, holding a futuristic rifle safely angled downward, one gauntlet resting against an armored command rail. Do not give the commander readable facial features. Armor is heavy practical plates with cables and hydraulics, not sleek fantasy armor.
Color palette: deep midnight navy, charcoal steel, electric cyan accents, restrained amber and sunrise gold edge light.
Materials/textures: brushed and battered steel, ceramic plates, utilitarian mechanics, drifting dust.
```

### ascendant

- Generated source: `exec-195d4138-a7a4-46d4-8ffc-4b22fc93ef85.png`
- Production filename: `ascendant-command.webp`
- Intended deployment key: `effects/ui/ascendant-command.webp`
- Generator: built-in OpenAI `image_gen`; no CLI or external art source.

```text
Use case: stylized-concept.
Asset type: premium Turn Craft science-fiction strategy game main-menu and victory-screen faction hero artwork, intended as a landscape background behind live HTML text.
Style/medium: richly detailed cinematic hand-painted 3D concept illustration, polished game key art, serious military science fiction, sharp readable character silhouette with atmospheric layered depth. Landscape 3:2 composition, approximately 1536x1024.
Composition/framing: one unmistakable faction commander or dominant creature occupies the right-center of the image, around 67 percent horizontal position, visible from the knees or foreground appendages upward. Hero should remain readable when cropped to a wide panoramic background. Supporting army and faction command architecture recede behind and to the right. The entire left third is dark low-detail atmospheric negative space with no face or focal object, to support UI text. Bottom foreground also dark. Full bleed without borders.
Lighting/mood: triumphant calm after a successful battle, commanding and majestic, radiant atmospheric rim lighting, subtle celebratory sparks or drifting energy without fireworks.
Constraints: entirely original science-fiction design. No text, letters, numerals, signage, logos, insignia, watermarks, UI elements, frames, franchise symbols, or gore. No baked-in title or victory words.
Primary request: illustrate the Ascendant Conclave, an advanced elite alien psionic civilization built around precision, geometric golden architecture, crystalline energy and graceful ivory armor.
Scene/backdrop: victorious alien command sanctum on an elevated terrace, elegant gold-rimmed spires, faceted floating crystal pylons, curved elevated causeways and a graceful hovering capital craft against an indigo nebula sky. An orderly small army of elegant psionic armored figures and quadruped ranged constructs recedes to the right.
Subject: a serene majestic alien psionic leader at the right-center in sculptural pale ivory and antique gold plated armor with elongated proportions and tall pointed geometric helmet, glowing violet eyes in an unreadable face. One upturned palm channels a controlled luminous violet/cyan crystal energy sphere held beside the shoulder, while the other hand lowers an elegant energy staff. Purple ceremonial cape flowing behind rather than across the left third.
Color palette: deep indigo and violet shadows, luminous lavender and restrained cyan, warm ivory, antique gold rim highlights.
Materials/textures: smooth ceramic-metal armor, fine geometric gold ornament, faceted crystal, clear controlled energy.
```

### brood

- Generated source: `exec-b2cc1eef-2fcd-416a-aefc-9d630f8a94f9.png`
- Production filename: `brood-command.webp`
- Intended deployment key: `effects/ui/brood-command.webp`
- Generator: built-in OpenAI `image_gen`; no CLI or external art source.

```text
Use case: stylized-concept.
Asset type: premium Turn Craft science-fiction strategy game main-menu and victory-screen faction hero artwork, intended as a landscape background behind live HTML text.
Style/medium: richly detailed cinematic hand-painted 3D concept illustration, polished game key art, serious military science fiction, sharp readable character silhouette with atmospheric layered depth. Landscape 3:2 composition, approximately 1536x1024.
Composition/framing: one unmistakable faction commander or dominant creature occupies the right-center of the image, around 67 percent horizontal position, visible from the knees or foreground appendages upward. Hero should remain readable when cropped to a wide panoramic background. Supporting army and faction command architecture recede behind and to the right. The entire left third is dark low-detail atmospheric negative space with no face or focal object, to support UI text. Bottom foreground also dark. Full bleed without borders.
Lighting/mood: triumphant calm after a successful battle, commanding and majestic, radiant atmospheric rim lighting, subtle celebratory sparks or drifting energy without fireworks.
Constraints: entirely original science-fiction design. No text, letters, numerals, signage, logos, insignia, watermarks, UI elements, frames, franchise symbols, or gore. No baked-in title or victory words.
Primary request: illustrate the Brood Dominion, an adaptive biological alien swarm with obsidian armored chitin, crimson bioluminescence, organic hive command structures, and swarming living armies.
Scene/backdrop: victorious living hive overlooking a conquered alien plain. Ribbed organic spires with red luminous cores, layered bone-like hive ridges, dark root networks and crimson/violet atmospheric clouds. Many small low-detailed swarm silhouettes gather on the right, with a distant winged organism above the hive.
Subject: one towering majestic insectoid brood commander at the right-center, not humanoid, with powerful layered charcoal chitin carapace, a curved crown-like armored crest with coral sensory nodes, four stable lower legs and two raised scythe forelimbs. Distinct intelligent commanding posture, glossy red bioluminescent seams and dark violet reflections, strong clean silhouette.
Color palette: obsidian black and deep burgundy, crimson/coral luminous accents, violet shadow, restrained bone-colored edges.
Materials/textures: layered glossy and matte chitin, clean leathery membranes, ribbed bone, living roots. No blood, gore, corpse piles or gross fluids.
```
