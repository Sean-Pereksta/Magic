# Hearthglade homestead, simultaneous touch controls, and campaign waves

## Playing the update

Clear a dangerous multi-wave encounter to complete the first step of **A Place to Return**. You receive a permanent homestead deed, the first cottage's building costs, starter seeds, and four packed furnishings. Follow the western road from Sunmere to **Hearthglade**; its node is visible on the first continent's map.

Build the cottage from the homestead board. Walk up to its entrance and interact to enter the interior. Build, move, rotate, pack and inspect furnishings through the contextual Home tray. Placement has a world-space preview, explicit confirmation/cancel, arrow controls and keyboard arrows; `R` rotates. Outdoor buildings occupy the workshop yard, annual crops the garden, and fruit trees the orchard. Solid furnishings collide with the player and cannot block entrances or cut off rooms. Expanding uses larger single-story room blueprints; partitions, doorways and floor inlays let you organize the interior. This release does not include free-form roof or multistory construction.

There are four house tiers, thirty furnishing blueprints and ten crop/sapling types. The first tier supplies four beds; the highest tier supplies twenty-four beds and eight trees. Later tiers require visiting the later continents. Existing equipment, spells, mounts, materials and quests remain in their original inventories and save layers.

Every town NPC panel has **Home supplies and seeds**. Suppliers sell basic building materials and seeds native to their continent. Ordinary combat completions also award building supplies once per room. Town-exclusive weapons, equipment and spell services remain in towns. Advanced furniture unlocks by house tier in this release, rather than through a second town-blueprint inventory.

## Useful furnishings

Beds restore health and grant 5% experience for the next three cleared encounters; duplicate beds do not stack this bonus. Hearths restore health. Storage opens the existing inventory, the lectern opens the existing spellbook, and the stable selects existing owned mounts. Cooking and alchemy stations consume harvested produce to prepare healing food. The composter consumes produce plus Plant Fiber for fertilizer. Fertilizer applies once per growth cycle, reduces required growth by 10%, and adds one unit to the yield.

Coinbloom Urns generate 4 gold/hour up to 96. Arcane Condensers generate 1 Arcane Dust/6 hours up to 4. Timber Racks generate 1 Timber/45 minutes up to 32. Stone Caches generate 1 Stone/hour up to 24. Each category permits one placed producer, with two further upgrades. Output stops at storage capacity; excess time is discarded. Moving does not reset a producer, packing stops generation, and upgrades settle output at the previous rate first. Packed stored output can still be collected before dismantling. Gift furnishings have no dismantling refund.

## Crops and real time

Annuals: Lanternberries (30 minutes), Strawberries (2 hours), Goldenroot (4), Honey Melon (8), Moonmint (6), Stormgrapes (12), Emberpeppers (16). Trees: Sunapple (48 hours to establish / 16 hours per subsequent harvest), Frostpear (60 / 20), Starplum (72 / 24).

These are **watered growth times**. Basic beds hold 8 hours of water; improved beds 12; greenhouse beds 16; orchard trees 24. A dry crop pauses rather than dying. Watering early resets coverage from that action, rather than stacking more time. Ready crops do not spoil. Trees retain their original planting date and hold only one harvest; their next cycle starts after collection.

A twelve-hour crop left for ten hours with eight hours of moisture earns exactly eight hours of growth. Watering at hour ten resumes from eight earned hours; it does not retroactively credit the dry interval.

The can refills at the home well. Its upgrade holds 24 charges. Each placed sprinkler supplies its four nearest annual beds via buried channels. Cisterns hold 32 bed-waterings. Offline irrigation is evaluated in chronological watering events, bounded by actual stored water; mature plants consume none. Installing or refilling irrigation never rewrites a past dry interval. A cached, once-per-second prediction drives garden visuals; it never writes timer ticks to Firebase.

## Portals and recall

Build and attune Village Portals to visited first-continent settlements. Continental Portals accept visited towns on unlocked continents. Shadow Gates accept earned Shadow Sanctuary checkpoints only after the third ruler is defeated. Walk up to an attuned portal or use its inspection action. Portal construction consumes resources, but travel does not charge a recurring fare.

After building a cottage, the existing journal offers home recall. Stand still through the short channel. Enemies, unresolved waves/objectives, damage, movement and transitions block or cancel recall. The return anchor resumes the recorded cleared ordinary campaign room; invalid destinations and Shadow expedition returns fall back to Sunmere. Interior doors cannot accidentally route onto a world road. Home rooms remain safe and clear all stale encounter state.

## Input and encounters

Mobile action buttons use independent pointer ownership and press-time activation. Movement/aim intent is sampled immediately; only knob rendering is frame-batched. Releasing or canceling an action finger does not release the joystick. Native pointer compatibility clicks cannot cast twice; keyboard/assistive activation is retained. Rebinding is idempotent, and orientation/focus changes release stuck input.

Both the campaign spawner **and the later regional squad override** use one encounter initializer. Tough sites have 2–4 waves, with at least four explicitly designated wave sites per continent and coverage of at least 25% of eligible combat sites. Dedicated `waves`/`defend` events retain their own three-wave controller, and dedicated bosses keep their boss phases. Reinforcement warnings and the wave HUD use the existing intensity system. Regional reinforcements use the same continent's enemy pool.

The final-clear guard blocks campaign rewards/exits until pending and remaining waves finish. Cleared-wave boundaries are stored in the campaign save. Reloading a partly completed wave restarts that wave; this is not a snapshot of each enemy. The pre-existing single-player combat economy is not converted into an anti-farming server ledger.

## Firebase integration and trust boundary

The home is part of the existing **encrypted named Firestore journey bundle**, alongside the original player and expansion data. There is no second spendable home-gold balance. Local journeys work without Firebase, using local time, until the player assigns an Official Firebase Save. Cloud-loaded legacy journeys acquire a cloud-managed home automatically.

Cloud-managed planting, watering, harvesting, purchases, production collection and construction require a confirmed connection. Plant records persist `plantedAt`, `cycleStartedAt`, `lastWateredAt`, `wateredUntil`, `evaluatedAt`, earned growth and the harvest number. Each home action obtains a server timestamp through the existing permitted save subcollection and commits one encrypted whole-journey update in a Firestore transaction **before** applying the result locally. A revision/ciphertext-IV compare-and-swap rejects a stale device rather than overwriting it. Random action IDs guard duplicate application. A lost commit acknowledgement blocks further home mutations until the named cloud journey is reopened and reconciled.

After restarting a browser, **Reopen Cloud Journey** with its password before editing a cloud home or overwriting its existing slot. Saving to a different name creates a separate slot. Starting a genuinely new journey clears the old cloud binding so it cannot silently replace the old home. No per-frame or per-second cloud writes are made.

**This is not a server-authoritative economy or a new authentication system.** The repository's existing Firestore lobby rules permit authenticated client writes, and the client holds the save password. Timestamps and transactions protect normal timing/concurrency, not against someone modifying game code, local data or the permissive legacy save document. Password encryption is not a replacement for Firestore owner authorization. This update deliberately does not tighten shared lobby rules and risk breaking other games. A fully authoritative inventory plus authenticated journey ownership, backend validation and migration require a separate backend rollout. No Cloud Functions or security rules are deployed by this PR.

Official API references: [Firestore transactions](https://firebase.google.com/docs/firestore/manage-data/transactions), [server timestamps](https://firebase.google.com/docs/firestore/manage-data/add-data#server_timestamp).

## Validation

Run `npm ci --ignore-scripts` then `npm test` from this directory. The suite loads the real entry HTML and production scripts in JSDOM, exercises pointer ownership, all three continents, scripted waves, save/load, physical placement, crop/irrigation clocks, production caps, recipes and portal gating. Firebase tests inject an in-memory transaction/timestamp adapter; they do not contact the production project. No screenshots or HTML previews are generated.

Before release, perform physical Android/iOS multi-touch play tests and a staging Firebase save/reopen test on two devices, including a lost connection during harvest. Canvas proxy tests do not establish physical-device rendering performance or real Firestore rule compatibility. Merging/publishing the site and any backend deployment remain separate actions.
