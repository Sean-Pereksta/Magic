# Living World expansion

The static game now contains **110 species, 135 equipment definitions, 24 additional elemental techniques and 20 exclusive new-species signatures**. Save-stable species and item IDs, the encrypted save namespace, ten regional walking unlocks and existing multiplayer paths are retained. No game server or Cloud Functions are required.

## Progression and battles

Awakened, Elder and Ascendant remain levels 3, 6 and 10. Techniques and passive specializations continue at levels 20, 30, 40, 50 and 60. The library distinguishes permanently discovered techniques from those usable at the current expedition level and active evolution branch. Palm Strike stays required in the four-slot loadout. Replacement selection compares techniques; only **Confirm replacement** changes equipment.

The battle screen shows enemy intent, charge release timing, counter/combo opportunities, statuses, cooldowns and costs. New mechanics include charge interruption, one-time charge delay, defensive counters, guard-breaking feints, status-consuming combinations, team barriers, targeted healing, energy relay, splash and preparation-dependent finishers. Settings offer optional precision timing with a maximum 10% attack bonus; misses and skipping retain the normal move. Assisted timing, battery saving and the system reduced-motion preference avoid the moving meter. Online version 5 resolves these abilities deterministically without timing inputs; versions 1–4 keep their replay rules and historical boss rosters.

## Selling and Forge Reserve

Team Details has **Train, Abilities, Evolve, Equipment and Sell**. Sanctuary **Sell & reserve** filters rarity, minimum level, duplicates and highest power. Selling requires three owned monkeys before the sale, so three may become two. Recruitment and smaller teams remain available. Favorites and protected monkeys require protection removal first, and Ultras require a second confirmation. Ordinary combat, online battles and unresolved encounters block selling.

The confirmation shows the exact payout, every returned equipment copy and affected lineups. Base payouts are 12/20/35/60 fruit, 5/9/15/24 matching essence, 3/6/11/18 species materials and rarity-specific equipment fragments. Dual species split the essence total. Neutral starters use their current evolution or current walking direction. Investment bonuses cap at 20%; fragments do not multiply. The payout goes into a permanent reserve; explicit partial or full withdrawals move it to expedition resources. Resources already withdrawn follow the expedition's normal reset rules.

Every sold individual has a permanent receipt. Cloud commits atomically save removal and reserve credit alongside an immutable opaque receipt, reject stale revisions and prevent saves from dropping prior sale markers. Repeated clicks, encrypted reopening and retries cannot repeat payment. Offline saves retain local receipts; a conflicting device must load the current cloud checkpoint rather than merge resource balances.

## Regional world

Each of the ten regions has three saved rotating side quests and three regional achievement ranks. Quest activities rotate among exploration, discoveries, elemental collection, species encounters, rescue distance, tactical victories and landmark visits. Collection rotates only that quest slot and awards once.

There are 80 destinations: ten bespoke major sites and seven smaller rotating sites per region. Accepted walking creates an opportunity after 1,500–3,500 feet, bounded by existing exploration effects. Destinations persist, may be pinned or dismissed, and use relative fantasy coordinates. **Cannot safely reach** replaces a precise approach with accepted walking on a route chosen by the player. No real streets or route safety are invented.

Temple offerings, bargaining, pattern and constellation puzzles, rescue/exploration objectives, defensive/counter trials, improved forge offerings and a three-stage Crown Vault use distinct interactions. Costs and lottery percentages come from the same implemented reward table. The temple costs 10 fruit and uses 45/30/15/8/1.8/0.2 percent category odds. Each spawned site permits one reward claim. A rolled rare encounter stays available until begun, including after later discoveries. Equipment bonuses roll once and cannot recursively trigger rewards.

## Tribe membership

Nonmembers see actual Firestore directory records, searchable names, admission/capacity filters, public profiles and rankings. Tribes support open or approval admission, pause applications and retain invite-code entry. An approval tribe's code submits an application rather than bypassing approval. Three transactionally reserved application slots limit spam; seven-day expiry checks work during ordinary client reads and operations. Leaders accept/reject requests with atomic single-membership and ten-member capacity checks.

Members see My Tribe with members, events, shared resources/trades, leader applications and rankings by boss progression, weekly contribution and combined crew power. Leaving requires confirmation and blocks active battles and unsettled escrow in both directions. Weekly eligibility stays tied to the originally joined tribe. Public profiles omit presence locations, routes, wallet balances and private save identifiers.

## Artwork and R2

`art-pack/monkey-trails-wild-companions-v4.zip` contains the complete v4 upload, including 802 animation scenes, 330 evolved appearances, 80 destination illustrations, shared UI/environment assets, `manifest.json` and `HOSTING.txt`. Rebuild with:

```sh
node game/monkey-trails/build-assets.mjs
```

Upload ZIP contents preserving `monkey-trails/wild-companions-v4/`. Set `window.MONKEY_TRAILS_ASSET_BASE` before loading the game to the public bucket base plus that prefix. Inline artwork and committed local assets keep gameplay working when hosted artwork is unavailable. Existing hosted v3 bases must be updated to v4 to display the complete new hosted collection.

## Verification and release gates

```sh
npm run test:monkey-trails
npm run test:monkey-trails:living-browser
npm run test:monkey-trails:social-rules
```

The unit suite covers roster, all progression milestones, save migration, deterministic battles and historical replays, equipment lottery/effect caps, sale/withdrawal idempotency and all ten regional experiences. The new browser check exercises actual destination claims, confirmed skill comparison, a three-to-two sale, reserve transfer and encrypted reopening at 320, 390 and 1280 pixels. Firestore emulator checks cover concurrent approvals, last-place capacity races, forged writes, expiration, trade leave restrictions, private/public boundaries and immutable sale history.

Production Firestore rules and hosted R2 assets are **not deployed by this pull request**. Deploy the reviewed rules with the game changes; approval flows and v5 battle validation require the updated rules. Emulator/browser evidence does not replace real Android Chrome/iOS Safari GPS, battery, screen-lock, offline asset-cache or two-device production conflict checks. The existing game does not include a service worker: encrypted checkpoints reopen offline when game code is already available, but an uncached cold offline launch is not an installable PWA.
