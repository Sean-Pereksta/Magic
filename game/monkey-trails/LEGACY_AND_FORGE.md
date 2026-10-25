# Trail Wisdom, expedition recaps and interactive forging

Open **Legacy · Trail Wisdom** from the game header or the Expedition Complete screen. Open **Pack → Craft / Forge** to mix ingredients. Team cards and a monkey's quick sheet have direct Weapon, Armor and Charm controls. Existing equipment remains usable in every previously supported slot, including Ultra slots.

## Permanent progression

Trail Wisdom is separate from the older Legacy score. Each upgrade has four ranks; purchasing a rank prepares the next expedition, without changing an active run's bonuses.

| Upgrade | Rank costs | Maximum benefit |
| --- | --- | --- |
| Trailblazer | 3, 6, 10, 15 | 1,000 virtual starting feet |
| Prepared Expedition | 3, 5, 8, 12 | 8 fruit, 3 essence, one utility item |
| Scout's Protection | 4, 6, 9, 13 | 10% reduced incoming damage in the first two ordinary battles |
| Trail Instinct | 4, 7, 10, 14 | 3 percentage points toward useful discoveries |
| Experienced Crew | 4, 6, 9, 13 | 8 health and 2 maximum energy |

Accepted walking earns one Wisdom per 2,000 feet, capped at 12 per expedition. Permanent first-time claims award two for major locations, two for regional milestones/challenges, three for regional guardians, and five for each first Ultra species victory. A completed expedition that advances the distance record by at least 2,000 actual feet adds two Wisdom. Restarting without improving the record cannot claim that bonus again.

Virtual depth changes encounter position and recap depth. It never adds accepted distance, walking XP, discoveries, regional travel progress or distance rewards. The opening encounters remain appropriate for the starting crew. Scout's Protection and crew bonuses do not enter competitive player battle snapshots.

Older saves migrate without resetting their collection, injuries, equipment, balances or history. Previously claimed regional milestones and visited major locations are adopted as claimed; older walking is not paid retroactively.

## Resource and reward accounting

The furnace reserves quantities in memory. Adding, editing, dragging, removing, canceling and clearing never spend inventory. A stack picker supports 1 through the available quantity, subject to the existing 60-unit furnace limit. Tap-to-add is available alongside mouse dragging and touch hold-and-drag; movement before the hold threshold scrolls normally.

The forge occupies a fixed workspace above the game navigation. The furnace and inventory remain visible together while only inventory contents scroll. Desktop dedicates most of the width to the pack; phones place the compact furnace and rarity summary above it. Each ingredient shows its per-unit crafting power directly below its name. The six actual rarity percentages stay visible, with full costs and elemental bonuses under **Cost & bonuses** and the equipment catalogue under **Possible items**.

The existing forge lottery supplies both the preview and actual result. Rarity percentages are sums of the exact million-ticket outcome table and total 100%. Camp discounts and equipped crafting effects appear before commitment. Equipment shown in the furnace inventory is inspection-only, so equipped, protected and favorite copies cannot be consumed accidentally.

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
