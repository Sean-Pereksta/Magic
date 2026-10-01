# Food, regions and trade packages

Food accounting is shared by the treasury, population forecast and turn resolution.
Civilians consume one food per eight people; land and embarked troops consume one
per four troop equivalents (mounted units count twice). Cities consume four plus
one per upgrade, towns five, and ship crews one per four. Towns produce three food;
founding a town costs 30 food. Every troop catalog entry has a provisioning cost.
Army splitting does not reduce food upkeep. Starting stocks remain unchanged.

Food security uses current reserves and three turns of declining net flow:
Abundant, Stable, Strained, Shortage or Famine. Strained supplies reduce growth;
shortage stops growth; exhausted stores use the existing population, happiness and
military attrition rules. Each food category appears in Realm's treasury.

New maps contain coherent desert regions, sparse woodland/fertility, and mineral
pockets. Desert movement costs one point. River farms can produce substantially
more than dry desert farms. Desert settlements support less population. Existing
maps and terrain are preserved when loading. Sand artwork belongs in
`terrain/sand_01.png` through `terrain/sand_06.png` in the existing asset host.
Until supplied, the map renders the procedural sand palette.

Capital founding supplies one sustainable farm and up to two local industries,
chosen from the actual nearby terrain. The farm may start at a higher level to
keep opening food at least +5. Founding never rewrites natural resources. Timber,
minerals and horses are not all guaranteed at each capital.

`economicNeeds` is the shared read-only valuation for AI spending and trading.
It includes stock, three-turn income, civilian/military food reserves, the next
construction, planned musters, expansion, known local resource scarcity, pending
pledges and recurring deliveries. Safe export surplus excludes reserves and
commitments. Construction and expansion priorities use the same needs. Foreign
stores stay private: partner selection uses voluntary export categories or
observed terrain; the recipient validates its own exact availability.

EXCHANGE and RECURRING use `giveItems` / `receiveItems` arrays. Direction remains
from the proposer's perspective. Duplicate resource rows merge, each resource
caps at 1,000 units, and a resource cannot appear on both sides. Scalar fields
mirror the first canonical row for older integrations. Save loading migrates
single-item contracts and stored proposals. Arrays are authoritative for edits.

The Treaty Desk adds/removes resource rows on both sides. Dialogue, dispatches,
contract summaries and counteroffers describe every item. Recipient valuation
prices the entire package using its own needs, protects planned reserves, and can
add payment resources or reduce deliveries. Transfers check both complete costs
before changing either treasury; recurring capacity counts total items per side.
The existing route, consent and ratification rules continue to apply. Gemini can
voice validated packages through the same schema; it cannot execute them.

AI proposals request up to two needed resources and pay with up to three safe
exports. AI-to-AI exchanges require recipient value and availability checks.
Proposals expire after three turns; ordinary House cooldowns last five turns and
critical food proposals wait three. AI-to-AI partners also have a four-turn
cooldown. Declines do not bypass those cooldowns.

Validation: `npm run test:iron-throne` and
`node game/iron-throne/tests/trade-package-browser.mjs` (Playwright Chromium).
