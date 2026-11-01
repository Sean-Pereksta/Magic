# MTG Draft Table

Draft Table updates the existing **MTG Catalog & Deck Builder** route:

`/game/MTG_Catalog_Deck_Builder.html`

Serve the repository through its normal static hosting. The page loads the adjacent `mtg-deck/style.css`, `app.mjs`, `deck.mjs`, and `scryfall.mjs` files directly; there is no build step or API key. Use HTTP(S) rather than opening the HTML through `file://`, because the application uses JavaScript modules. Card search and price refresh require access to Scryfall.

## Draft workflow

**My draft** is the starting screen. Search a card name, optionally narrow it to a set, and inspect its colors, mana cost, rules, printing, and price. A search or details view does not add cards. **Add** adds one copy of the displayed printing and selected finish to the main deck. **Printings** lets you match the set and collector number on the physical card.

The main deck supports quantity changes, complete row removal, and moving a row to the sideboard. The sideboard keeps cut cards available without counting them toward the main deck's size, mana curve, colors, or dollar subtotal. Undo restores recent edits during the current session.

The editable starting plan is:

| Mana value / type | Target copies | Share of 40-card goal |
| --- | ---: | ---: |
| 0 | 0 | 0% |
| 1 | 2 | 5% |
| 2 | 6 | 15% |
| 3 | 6 | 15% |
| 4 | 4 | 10% |
| 5 | 3 | 7.5% |
| 6+ | 2 | 5% |
| Lands | 17 | 42.5% |
| **Total** | **40** | **100%** |

These are customizable planning targets, not a claim that every Limited deck should use this curve. Changing a target does not remove cards. The UI shows any mismatch between the sum of the spell and land targets and the planned deck size.

### Counts and percentage denominators

- **Mana curve:** nonland copies grouped by the card's API mana value. Each row compares actual copies with the target. Both percentages in **In / goal %** use the planned deck size as their denominator.
- **Spells and lands:** their summary percentages use the current main deck size.
- **Your colors:** mutually exclusive white, blue, black, red, green, multicolor, colorless, and land groups use the current main deck size. Each physical card contributes once, so the underlying shares sum to 100%; displayed rounding can differ slightly.
- **Colors used by your spells:** each color's count is divided by current nonland copies. Multicolor cards contribute to each matching color, so these percentages can overlap. Card colors come from the card's colors, not Commander color identity.

Lands are excluded from the spell curve. For cards with separate faces, the front face determines the default land/spell role; the deck-row role control can explicitly count a spell/land card as a land. A physical card counts once. Split cards use the API's combined mana value, and Adventure cards use the main card's mana value.

**Add basic lands** creates unpriced placeholders. The quantity field makes adding several lands at once convenient. These placeholders count toward the land and deck goals; search and add the exact basic-land printing if its monetary value should be included.

## Card values and catalog

Rows distinguish **Scryfall printing ID, finish, and zone**. Nonfoil, foil, and etched prices use `usd`, `usd_foil`, and `usd_etched` respectively. An unavailable price remains unknown; it never silently falls back to a different finish or becomes a zero-dollar valuation.

The displayed amount is a subtotal of priced copies, with priced/unpriced coverage shown separately. Quantity multiplies the selected finish's unit price. **Refresh prices** requests the saved exact printing IDs while preserving quantities, finishes, and zones. “Checked” timestamps describe when the application fetched the data, not when the underlying market moved. These are Scryfall estimates, not real-time bids, a condition appraisal, or guaranteed sale proceeds.

**Card catalog** supports name, rules-text, or advanced Scryfall searches, with set, color, mana value, type, rarity, and printing filters. Price filtering and sorting use **nonfoil USD**; price-sorted results exclude cards without that price. Per-card finish selection still shows the corresponding foil or etched quote where available.

## Saving and backups

The version-2 deck is saved locally under `mtg.draft.v2`. When no version-2 save exists, the application migrates an existing `mtg.deck` save and preserves the original key. There is no account or cloud synchronization.

Unreadable saves and unavailable browser storage produce a visible warning instead of silently overwriting the saved data. Changes can remain in memory while storage is unavailable. **Deck tools → Download backup** preserves the complete deck, sideboard, printing IDs, finishes, saved prices, and targets in JSON. Restoring a backup replaces the current deck and supports Undo.

Text export is a portable card list. Text import adds named cards and can select a representative printing, so check the printing before relying on the value. Use the JSON backup when exact printing and finish preservation matters.

## API behavior

The client restricts ordinary searches to paper cards and requests one Scryfall page at a time. More results are loaded only when requested, rather than downloading the complete catalog at startup. Search changes cancel or ignore older requests.

One request queue spaces network request starts by at least **600 ms**. Successful GET responses have a short bounded cache; active requests have a timeout. HTTP 429 responses honor `Retry-After` through a client cooldown and show a recoverable error, without automatic retry loops. Price collection requests deduplicate printing IDs and use batches of at most 75.

## Validation

From the repository root:

```sh
node --test game/mtg-deck/tests/deck.test.mjs game/mtg-deck/tests/scryfall.test.mjs
node game/mtg-deck/tests/browser.cjs
```

The model and API tests use Node's test runner and mocked requests. Browser verification uses Playwright with fixture card responses and covers the page's draft workflow. Install the repository's development dependencies and Playwright Chromium if they are not already available in your environment.

## References

- [Scryfall's official card field definitions](https://github.com/scryfall/api-types/blob/main/src/objects/Card/CardFields.ts)
- [Scryfall's official price fields](https://github.com/scryfall/api-types/blob/main/src/objects/Card/values/Prices.ts)
- [Wizards of the Coast: An Introduction to 40-Card Decks](https://magic.wizards.com/en/news/feature/feeling-draft-introduction-40-card-decks-2006-12-30)
