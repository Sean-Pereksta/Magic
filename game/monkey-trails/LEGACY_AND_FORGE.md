# Trail Wisdom, expedition recaps and interactive forging

Open **Legacy · Trail Wisdom** from the game header or the Expedition Complete screen. Open **Pack → Craft / Forge** to mix ingredients. Team cards and a monkey's quick sheet have direct Weapon, Armor and Charm controls. Existing equipment remains usable in every previously supported slot, including Ultra slots.

## Permanent progression

Trail Wisdom is separate from the older Legacy score. Purchases take effect on the next expedition. Existing ranks, their original first four prices and benefits, balances, and current-run bonuses are retained.

| Upgrade | Maximum rank | Maximum benefit |
| --- | --- | --- |
| Trailblazer | 200 | 50,000 virtual starting feet, in 250-foot steps |
| Prepared Expedition | 100 | 200 fruit, 99 essence, one utility item |
| Scout's Protection | 100 | 29.2% reduced damage in the first 98 ordinary battles |
| Trail Instinct | 100 | 15 percentage points toward useful discoveries |
| Experienced Crew | 100 | 200 health and 50 maximum energy |

The first four costs remain unchanged. For each later rank, the price rises by `ceil(0.5 × n + 0.002 × n²)` over the original fourth-rank cost, where n starts at 1. The Trailblazer track takes a very long time to complete; each rank and the next price are visible in the Legacy screen.

Accepted walking earns one Wisdom per 2,000 feet without a per-expedition cap. Each expedition also earns one per three victories, one per two captures, and one per three equipment pieces crafted. These repeatable activity counters reset on restart; existing run counters are adopted without paying older events retroactively. Permanent first-time claims award two for major locations, two for regional milestones/challenges, three for regional guardians, and five for each first Ultra species victory. A completed expedition that advances the distance record by at least 2,000 actual feet adds two Wisdom.

Virtual depth changes encounter position and recap depth. It never adds accepted distance, walking XP, discoveries, regional travel progress or distance rewards. After a deep head start, ordinary enemies scale up with the expedition crew so a newly restarted team is not immediately overwhelmed. Scout's Protection and crew bonuses do not enter competitive player battle snapshots.

Older saves migrate without resetting collection, injuries, equipment, balances, or history. Previously claimed achievements stay claimed, and any distance beyond the former 12-point walking cap is adopted without retroactive rewards. Saved starting depths up to 50,000 feet persist through reload.

## Resource and reward accounting

The furnace reserves quantities in memory. Adding, editing, dragging, removing, canceling and clearing never spend inventory. A stack picker supports 1 through the available quantity, subject to the existing 60-unit furnace limit. Tap-to-add is available alongside mouse dragging and touch hold-and-drag; movement before the hold threshold scrolls normally.

The forge occupies a fixed workspace above the game navigation. The furnace and inventory remain visible together while only inventory contents scroll. Desktop dedicates most of the width to the pack; phones place the compact furnace and rarity summary above it. Each ingredient shows its per-unit crafting power directly below its name. The six actual rarity percentages stay visible, with full costs and elemental bonuses under **Cost & bonuses** and the equipment catalogue under **Possible items**.

The existing forge lottery supplies both the preview and actual result. Rarity percentages are sums of the exact million-ticket outcome table and total 100%. Camp discounts and equipped crafting effects appear before commitment. Spare equipment copies can be dragged or tapped into the furnace and consumed for crafting power. Equipped, protected and favorite copies remain inspection-only. See [Temples and Merchants](TEMPLES_AND_MERCHANTS.md) for directional challenges, pack barter and equipment-smelting details.

Smelting consumes ingredients, advances RNG and records the equipment award in one state change. The operation receipt makes retries return the same result. New rolls stay locked until the encrypted device checkpoint succeeds; **Retry saving equipment** persists the existing result without rolling again. Maximum-rank duplicates now create a fresh equipment copy instead of returning fragments. Older salvage receipts still load and replay.

Expedition completion records the recap, record reward and permanent earnings together under the expedition ID. Reloads and encrypted cloud retries preserve that receipt. Cloud revision conflicts keep the existing explicit conflict workflow; this change adds no server or new backend.

## Verification

- `npm run test:monkey-trails` — rules, save migration, combat, crafting, ownership and progression regressions.
- `npm run test:monkey-trails:browser` — existing capture, camp, battle replacement and defeat/restart flows.
- `npm run test:monkey-trails:forge-browser` — quantity sliders, cancellation, real touch scrolling and dragging, mouse dragging, exact odds, repeated smelt clicks, device-save failure/retry, encrypted reload and immediate equipment transfers at 320, 390 and 1280 pixels.
- `npm run test:monkey-trails:forge-layout` — fixed furnace/inventory frames, condensed preview, independent inventory scrolling, and viewport fit on desktop, phones and short screens.
- `npm run test:monkey-trails:legacy-browser` — recap, upgrade purchase, history, reload and restart at 320, 390 and 1280 pixels.
- `node game/monkey-trails/browser-inventory.cjs` — responsive inventory and equipment movement.

Browser checks accept the existing `MONKEY_TRAILS_PLAYWRIGHT` and `MONKEY_TRAILS_BROWSER_EXECUTABLE` overrides. They serve repository files through browser routing and need no running backend.
