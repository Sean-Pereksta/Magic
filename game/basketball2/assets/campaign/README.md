# Rise to Glory artwork

Original illustrations generated with the built-in image generation tool for Basketball 2's existing blue/coral visual identity. No player or third-party logo assets were replaced.

| File | Use | Size |
| --- | --- | --- |
| `rise-hero.webp` | Franchise entry, creation and clubhouse | 1600 × 640, 117,400 bytes |
| `rise-scouting.webp` | Recruitment / scouting center | 1600 × 640, 93,790 bytes |
| `rise-training.webp` | Practice / player development | 1600 × 640, 75,238 bytes |

The three static WebP files total 286,428 bytes. Dark negative space protects live text contrast, while CSS adapts the composition for narrow screens. Ability, navigation, training and division graphics are native SVG from `js/campaign/rise-graphics.js` and inherit the UI colors without extra downloads or animation loops.

## Player portraits

`portraits/player-01.webp` through `portraits/player-22.webp` form a coordinated set of twenty-two original fictional adult athlete headshots. Each image is 512 × 512 with transparent alpha. The portraits load lazily in rookie selection, roster cards, scouting, comparisons and opponent previews. New markets and future recruiting classes use the expanded skin/hair/build pool. Validated portrait IDs remain attached to each athlete across recruitment, upgrades, saving, reloading and season birthdays; older saves retain the face selected from the original twelve portraits. An explicit skin or hairstyle change still updates the appearance match. An accessible jersey portrait remains available if an image cannot load.

The complete portrait prompt set is in `portraits/prompts.txt`. The portraits were generated with the built-in image generation tool; no external runtime image service is needed.

## Generation prompts

### rise-hero

Use case: stylized-concept. Asset type: finished landscape background illustration for the Rise to Glory franchise campaign in an arcade basketball video game. Create original premium sports editorial artwork, semi-realistic painted graphic-novel style with crisp silhouettes and selective brush texture. Wide panoramic composition around 2.5:1. A neighborhood rooftop basketball court at blue hour leads visually toward a distant glowing championship arena and city skyline, a polished orange basketball and low court lines in the lower right foreground, one hoop with a clean white net on the right, subtle warm gold arena light beyond the city. Theme: building a neighborhood team into champions. Deep midnight navy #111925 and slate blue dominate, powder blue #6aa8e8 rim light, warm coral #f2b19a and small gold highlights. Left 55 percent is quiet dark navy atmospheric negative space to hold live UI text. Keep central upper area low contrast. Detailed basketball/environment scene concentrated on rightmost 40 percent and bottom edge. This is an actual game artwork asset, not a screenshot, not a UI mockup. No text, lettering, numbers, logos, brands, badges, watermark, border or interface. No athletes in this background because an animated 3D captain will be drawn over it by the game. Cinematic and polished but restrained; avoid neon magenta and excessive glowing particles.

### rise-scouting

Use case: stylized-concept. Asset type: finished landscape scouting-center header illustration for the Rise to Glory basketball franchise game. Original semi-realistic painted sports editorial / graphic novel artwork, crisp figures and selective brush texture. Wide panoramic composition around 2.5:1. Three promising adult basketball recruits stand at the edge of a neighborhood indoor training court, a short explosive point guard holding a basketball, a versatile wing, and a tall powerful center, different heights/builds and diverse appearances, powder blue and coral generic basketball uniforms without lettering or numbers. Their purposeful calm poses convey recruitment and assembling a complementary team. In foreground a coach's clipboard with basketball court diagrams but absolutely no words or numbers, partially cropped near bottom right. Dark midnight navy #111925/slate blue surroundings, powder blue #6aa8e8 lighting and warm coral #f2b19a highlights, matching premium illustrated arcade basketball menu art. Scene/athletes concentrated in rightmost 45 percent, left 55 percent quiet deep navy negative space for a UI heading, subtle arena rafters behind them. Actual game background artwork only. No text, typography, letters, numbers, logos, team brands, watermark, border or UI panels. Human anatomy and basketball hands correct. No magenta neon or excessive special effects.

### rise-training

Use case: stylized-concept. Asset type: finished landscape training-center header illustration for the Rise to Glory basketball franchise game. Create original polished semi-realistic painted sports editorial artwork, graphic novel brush texture and crisp silhouettes consistent with a blue/coral basketball game. Wide panoramic composition around 2.5:1. On the right side an orange basketball rises through a graceful shooting arc toward a basketball hoop in an empty practice gym; lower right foreground has two warm coral training cones, a folded powder blue jersey, a basketball on hardwood and subtle white court lines. Communicate focused player development and repetition. Strong deep midnight navy #111925 and slate blue palette, powder blue #6aa8e8 edge lighting, warm coral #f2b19a accents and a small gold light glancing off the rim. Keep left 55 percent calm dark negative space, artwork concentrated right and bottom, hoop visible fully in upper right without cropping off net. A visual shooting arc may be a subtle drawn brush curve. Actual game illustration, not a screenshot or interface mockup. No text, typography, letters, numbers, logos, labels, watermark, borders, badges, buttons or UI panels. No athletes; clean readable objects, no excessive glowing effects.

