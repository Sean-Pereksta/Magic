# An adventurous Monkey Trails world

The Walk screen now offers illustrated trail choices and a World Forecast. The
adventure journal, camp moments, sanctuary home and two team techniques share
the same original vector artwork. These additions use the existing encrypted
save, expedition rules, accepted walking distance and game navigation.

## Trail choices

Choose Berry Grove, Ancient Ruins or Misty Canopy for one stretch ending at the
next earned camp. Choosing is optional and does not grant distance. The choice
survives closing the game. Camps offer a fresh choice, and an encounter already
waiting retains the route under which it was created.

- Berry Grove adds one fruit to a fruit discovery.
- Ancient Ruins adds one charm material every third discovery and after an
  ordinary victory. After the introductory battles, ordinary enemies are one
  level stronger, within the existing level cap.
- Misty Canopy increases uncommon and rare encounter weights by 60% relative
  to common companions. Existing regional, rarity and walking gates still apply.

All three destination scenes are original SVG illustrations with readable
names, descriptions, benefits and risks. They do not describe real-world paths.

## World Forecast

Calendar version 1 defines one 24-hour event every three days, leaving two quiet
days between events. Fruit Bloom begins October 31, 2026 at 00:00 UTC; the five
event types then repeat in order: Fruit Bloom, Monkey Migration, Essence Winds,
Guardian Stirring and Traveling Merchant. The forecast shows the next 21 days
with local start/end timestamps and the player's time zone. It includes effects,
locations and conditions, using the same event definitions as gameplay.

The calendar is bundled with the game and works offline; it does not fetch an
external event feed or imply a successful remote refresh. An unavailable clock
is shown as unconfirmed. Event boundaries are half-open: the start is included
and the end is excluded. Open screens refresh when the active event changes.

Discovery bonuses are evaluated when the discovery is created. Encounters save
their event and route at creation, including when waiting on the map. Beginning
or completing one after an event ends keeps that snapshot. The optional
Bloomwatch Guardian requires two introductory victories and 2,000 feet walked
this expedition; its extra reward is claimable once per event. Merchant trades
require an active merchant event at the moment of the trade, and each offer is
limited to once per event per expedition. Event windows change no essential
companion availability or core progression requirements.

## Journal, home and camp

The adventure journal brings together existing regions, collection records,
encounters, guardians and expedition progress. Three optional goals recognize
prior progress and award permanent decorations. One goal can be followed on the
Walk screen. Claiming a keepsake cannot pay twice.

The Sanctuary shows a home scene with up to six collected monkeys, prioritizing
favorites and the expedition team. Choose Canopy Clearing or unlock River Rest
through exploration, then place owned decorations in three display spots.
Existing tribe decorations and regional keepsakes remain in their original
ownership ledgers and can be displayed. There is no upkeep or currency charge.

At each earned camp, share a meal or study the trail. A meal costs two fruit and
adds one energy per conscious teammate after each of the next three battle
rounds. Study reveals a waiting encounter or a habitat hint from the fantasy
map. The saved camp message remains available after reopening the game. Pack,
recovery, team management, forging, trading and continuing the expedition remain
available. During merchant visits, optional illustrated trades appear at camp.

## Team techniques

Canopy Relay follows a guarding move by a different teammate. Dewfall Chorus
pairs a Leaf or Water setup with a teammate of the complementary element. The
battle UI explains the setup, energy cost, eligibility and effect. Both consume
the active monkey's ordinary turn and helper energy, trigger normal enemy
responses, and are usable once per battle. They do not change online combat.
The action and its costs commit together; feedback persists through victory.

## Assets, accessibility and verification

`adventure-art.mjs` contains original, self-contained SVG artwork; no additional
hosting, image downloads or asset compilation is required. Shared artwork keys
keep route/event identities consistent across screens. Graphics have accessible
names, controls retain text labels, and animation stops for reduced motion,
battery mode and inactive pages. Text and static art remain visible.

Run `npm run test:monkey-trails` for the gameplay and save regressions, and
`npm run test:monkey-trails:adventure-browser` for the real browser checks.
The browser runner accepts `MONKEY_TRAILS_PLAYWRIGHT`,
`MONKEY_TRAILS_BROWSER_EXECUTABLE` and `MONKEY_TRAILS_OUTPUT`, like the other
Monkey Trails checks. The new tests cover routes, event boundaries and saved
encounters, journal rewards, decoration ownership, merchant limits, team setup,
normal turn costs, reduced motion and mobile/desktop layouts.

Deploy the complete changed static modules and CSS together. These changes
require no Firebase rules deployment, scheduled server job or save reset.
