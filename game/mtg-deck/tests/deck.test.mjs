import test from 'node:test';
import assert from 'node:assert/strict';
import {
  DEFAULT_PLAN, BUCKETS, createDeck, normalizeDeck, migrateLegacyDeck,
  addCard, changeQuantity, moveCard, setRole, setPlan, cardPrice, availableFinishes,
  defaultFinish, cardColors, isLand, entryIsLand, manaValue, curveIndex, rulesText,
  imageUrl, summarizeDeck,
} from '../deck.mjs';

const card = (overrides = {}) => ({
  id: 'bolt-printing', name: 'Lightning Bolt', type_line: 'Instant', mana_cost: '{R}', cmc: 1,
  colors: ['R'], color_identity: ['R'], finishes: ['nonfoil', 'foil'],
  prices: { usd: '1.25', usd_foil: '4.50', usd_etched: null }, set: 'tst', collector_number: '123',
  prices_checked_at: '2026-11-01T12:00:00.000Z', oracle_text: 'Lightning Bolt deals 3 damage to any target.',
  ...overrides,
});
const forest = card({ id: 'forest-printing', name: 'Forest', type_line: 'Basic Land — Forest', colors: [], cmc: 0, mana_cost: '', prices: { usd: '0.10', usd_foil: null } });

test('the editable 40-card default has 23 spell slots and 17 lands', () => {
  const deck = createDeck();
  assert.deepEqual(BUCKETS, ['0', '1', '2', '3', '4', '5', '6+']);
  assert.deepEqual(deck.plan, { deckSize: 40, lands: 17, curve: [0, 2, 6, 6, 4, 3, 2] });
  const totals = summarizeDeck(deck);
  assert.equal(totals.curveTargetTotal, 23);
  assert.equal(totals.planTotal, 40);
  deck.plan.curve[1] = 9;
  assert.equal(DEFAULT_PLAN.curve[1], 2);
  assert.equal(createDeck().plan.curve[1], 2);
});

test('quantities can exceed four, finish and printing stay separate, and updates are immutable', () => {
  const original = createDeck();
  const input = card();
  let deck = addCard(original, input, { qty: 6 });
  deck = addCard(deck, input, { qty: 2 });
  deck = addCard(deck, input, { qty: 1, finish: 'foil' });
  deck = addCard(deck, card({ id: 'other-printing' }), { qty: 1 });
  assert.equal(original.entries.length, 0);
  assert.equal(deck.entries.length, 3);
  assert.equal(deck.entries[0].qty, 8);
  assert.equal(summarizeDeck(deck).total, 10);
  assert.equal(summarizeDeck(deck).value, 15.75);
  input.prices.usd = '999.99';
  assert.equal(deck.entries[0].card.prices.usd, '1.25');
  const removed = changeQuantity(deck, deck.entries[0].key, -8);
  assert.equal(removed.entries.length, 2);
  assert.equal(deck.entries[0].qty, 8);
});

test('cutting cards to the sideboard changes only main deck stats and preserves card metadata', () => {
  let deck = addCard(createDeck(), card(), { qty: 3 });
  deck = addCard(deck, card(), { qty: 2, zone: 'sideboard' });
  const original = deck;
  deck = moveCard(deck, deck.entries.find(entry => entry.zone === 'main').key, 'sideboard');
  assert.equal(deck.entries.length, 1);
  assert.equal(deck.entries[0].qty, 5);
  assert.equal(summarizeDeck(deck).total, 0);
  assert.equal(summarizeDeck(deck, 'sideboard').total, 5);
  assert.equal(summarizeDeck(deck, 'sideboard').value, 6.25);
  assert.equal(deck.entries[0].card.collector_number, '123');
  assert.equal(deck.entries[0].card.prices_checked_at, '2026-11-01T12:00:00.000Z');
  assert.equal(summarizeDeck(original).total, 3);
  deck = moveCard(deck, deck.entries[0].key, 'main');
  assert.equal(summarizeDeck(deck).total, 5);
  assert.equal(summarizeDeck(deck, 'sideboard').total, 0);
});

test('lands do not enter the curve, average mana, or colored-spell count', () => {
  let deck = addCard(createDeck(), forest, { qty: 17 });
  deck = addCard(deck, card(), { qty: 2 });
  deck = addCard(deck, card({ id: 'three', cmc: 3, mana_cost: '{1}{R}{R}' }), { qty: 6 });
  const result = summarizeDeck(deck);
  assert.equal(result.total, 25);
  assert.equal(result.spells, 8);
  assert.equal(result.lands, 17);
  assert.deepEqual(result.curve, [0, 2, 0, 6, 0, 0, 0]);
  assert.equal(result.avgMana, 2.5);
  assert.equal(result.colors.C, 0);
  assert.equal(result.colors.R, 8);
  assert.equal(result.pipDemand.R, 14);
  assert.equal(result.composition.L, 17);
});

test('actual colors differ from identity and composition counts multicolor cards exactly once', () => {
  let deck = addCard(createDeck(), card({ colors: ['R'], color_identity: ['R', 'G'] }), { qty: 2 });
  deck = addCard(deck, card({ id: 'multicolor', colors: ['U', 'R'], mana_cost: '{U}{R}', cmc: 2 }), { qty: 3 });
  deck = addCard(deck, card({ id: 'devoid', colors: [], color_identity: ['B'], mana_cost: '{2}{B}', cmc: 3 }), { qty: 1 });
  deck = addCard(deck, forest, { qty: 4 });
  const result = summarizeDeck(deck);
  assert.equal(result.colors.R, 5);
  assert.equal(result.colors.U, 3);
  assert.equal(result.colors.G, 0);
  assert.equal(result.colors.C, 1);
  assert.equal(result.pipDemand.B, 1);
  assert.deepEqual(result.composition, { W: 0, U: 0, B: 0, R: 2, G: 0, C: 1, M: 3, L: 4 });
  assert.equal(Object.values(result.composition).reduce((sum, value) => sum + value, 0), result.total);
});

test('spell/land MDFCs use the front face by default and allow an explicit land allocation', () => {
  const mdfc = card({
    id: 'mdfc', name: 'Bala Ged Recovery // Bala Ged Sanctuary', type_line: 'Sorcery // Land', cmc: 3,
    colors: undefined, mana_cost: undefined, oracle_text: undefined,
    card_faces: [
      { name: 'Bala Ged Recovery', type_line: 'Sorcery', mana_cost: '{2}{G}', colors: ['G'], oracle_text: 'Return target card from your graveyard to your hand.' },
      { name: 'Bala Ged Sanctuary', type_line: 'Land', colors: [], oracle_text: 'This land enters tapped.', image_uris: { normal: 'https://example.com/land.jpg' } },
    ],
  });
  assert.equal(isLand(mdfc), false);
  assert.deepEqual(cardColors(mdfc), ['G']);
  let deck = addCard(createDeck(), mdfc);
  assert.equal(summarizeDeck(deck).spells, 1);
  assert.equal(summarizeDeck(deck).curve[3], 1);
  assert.equal(summarizeDeck(deck).pipDemand.G, 1);
  deck = setRole(deck, deck.entries[0].key, 'land');
  assert.equal(entryIsLand(deck.entries[0]), true);
  assert.equal(summarizeDeck(deck).lands, 1);
  assert.equal(summarizeDeck(deck).spells, 0);
  assert.equal(summarizeDeck(deck).pipDemand.G, 0);
  assert.match(rulesText(mdfc), /Bala Ged Recovery/);
  assert.match(rulesText(mdfc), /Bala Ged Sanctuary/);
  assert.equal(imageUrl(mdfc), 'https://example.com/land.jpg');
  deck = setRole(deck, deck.entries[0].key, 'auto');
  assert.equal(summarizeDeck(deck).spells, 1);
});

test('front land and legacy combined type lines classify correctly', () => {
  assert.equal(isLand({ type_line: 'Land // Creature' }), true);
  assert.equal(isLand({ type_line: 'Sorcery // Land' }), false);
  assert.equal(isLand({ type_line: 'Artifact Land — Cave' }), true);
  assert.equal(isLand({ type_line: 'Creature — Land Shark' }), false);
  assert.equal(isLand({ type_line: 'Landfall Enchantment' }), false);
  assert.equal(isLand({ type_line: 'Creature // Land', card_faces: [{ type_line: 'Land' }, { type_line: 'Creature' }] }), true);
});

test('split and Adventure cards use Scryfall mana value rather than guessing from alternate faces', () => {
  const split = card({ cmc: 6, mana_cost: '{1}{R} // {3}{W}', card_faces: [{ cmc: 2 }, { cmc: 4 }] });
  const adventure = card({ cmc: 4, card_faces: [{ mana_cost: '{2}{G}{G}' }, { mana_cost: '{G}' }] });
  assert.equal(manaValue(split), 6);
  assert.equal(curveIndex(split), 6);
  assert.equal(manaValue(adventure), 4);
  assert.equal(manaValue(card({ cmc: 0, mana_cost: '{X}' })), 0);
  assert.equal(curveIndex(card({ cmc: 12 })), 6);
});

test('mana cost fallback treats X as zero, numeric hybrids correctly, and front faces only', () => {
  assert.equal(manaValue({ mana_cost: '{X}{2}{W/U}{2/B}' }), 5);
  assert.equal(manaValue({ card_faces: [{ mana_cost: '{3}{U}' }, { mana_cost: '{6}{G}' }] }), 4);
  assert.equal(manaValue({ cmc: NaN, mana_cost: '{2}{R}' }), 3);
  assert.equal(manaValue({ cmc: Infinity, mana_cost: '{R}' }), 1);
});

test('hybrid costs share demand between options and never count generic mana as a color', () => {
  const hybrid = card({ mana_cost: '{2}{W/U}{B/P}{2/R}{C}{X}', cmc: 7, colors: ['W', 'U', 'B', 'R'] });
  const totals = summarizeDeck(addCard(createDeck(), hybrid, { qty: 2 }));
  assert.deepEqual(totals.pipDemand, { W: 1, U: 1, B: 1, R: 1, G: 0, C: 2 });
});

test('normal, foil, and etched prices never fall back to a different finish', () => {
  const priced = card({ finishes: ['nonfoil', 'foil', 'etched'], prices: { usd: null, usd_foil: '9.80', usd_etched: '14.00' } });
  assert.equal(defaultFinish(priced), 'nonfoil');
  assert.equal(cardPrice(priced), null);
  assert.equal(cardPrice(priced, 'nonfoil'), null);
  assert.equal(cardPrice(priced, 'foil'), 9.8);
  assert.equal(cardPrice(priced, 'etched'), 14);
  assert.equal(cardPrice(priced, 'unknown'), null);
  let deck = addCard(createDeck(), priced, { qty: 2 });
  deck = addCard(deck, priced, { qty: 3, finish: 'foil' });
  const summary = summarizeDeck(deck);
  assert.equal(summary.value, 29.4);
  assert.equal(summary.pricedCopies, 3);
  assert.equal(summary.unpricedCopies, 2);
  assert.deepEqual(availableFinishes(card({ finishes: ['etched'], prices: { usd_etched: null } })), ['etched']);
});

test('a quoted zero price is distinct from unavailable or malformed prices', () => {
  assert.equal(cardPrice(card({ prices: { usd: '0.00' } })), 0);
  for (const invalid of [null, undefined, '', ' ', 'unknown', -1, Infinity, NaN, false, [], {}]) {
    assert.equal(cardPrice(card({ prices: { usd: invalid } }), 'nonfoil'), null);
  }
  const result = summarizeDeck(addCard(createDeck(), card({ prices: { usd: '0.00' } }), { qty: 2 }));
  assert.equal(result.pricedCopies, 2);
  assert.equal(result.unpricedCopies, 0);
  assert.equal(result.value, 0);
});

test('legacy saves keep counts, printing metadata, land classification, and best supported finish', () => {
  const old = {
    bolt: { card: card(), qty: 7 },
    forest: { card: forest, qty: 17 },
    foilOnly: { card: card({ id: 'foil-only', finishes: undefined, nonfoil: false, foil: true, prices: { usd: null, usd_foil: '8.00' } }), qty: 1 },
  };
  const migrated = migrateLegacyDeck(old);
  assert.equal(migrated.version, 2);
  assert.equal(summarizeDeck(migrated).total, 25);
  assert.equal(summarizeDeck(migrated).lands, 17);
  assert.equal(migrated.entries[2].finish, 'foil');
  assert.equal(migrated.entries[0].card.set, 'tst');
  assert.equal(migrated.entries[0].card.collector_number, '123');
  assert.equal(old.bolt.qty, 7);
  assert.deepEqual(migrateLegacyDeck(migrated), migrated);
});

test('legacy missing normal quotes do not silently revalue cards as foil', () => {
  const saved = card({ finishes: undefined, prices: { usd: null, usd_foil: '20.00' } });
  const migrated = migrateLegacyDeck({ old: { card: saved, qty: 2 } });
  assert.deepEqual(availableFinishes(saved), ['nonfoil', 'foil']);
  assert.equal(migrated.entries[0].finish, 'nonfoil');
  assert.equal(summarizeDeck(migrated).unpricedCopies, 2);
  assert.equal(summarizeDeck(migrated).value, 0);
});

test('conflicting refreshed availability cannot change saved finish identities or merge holdings', () => {
  const refreshed = card({ finishes: ['nonfoil'], prices: { usd: '1.00', usd_foil: null } });
  let deck = normalizeDeck({ entries: [
    { card: refreshed, qty: 2, finish: 'nonfoil', zone: 'main' },
    { card: refreshed, qty: 3, finish: 'foil', zone: 'main' },
  ] });
  assert.equal(deck.entries.length, 2);
  assert.deepEqual(deck.entries.map(entry => entry.finish), ['nonfoil', 'foil']);
  assert.equal(summarizeDeck(deck).value, 2);
  assert.equal(summarizeDeck(deck).unpricedCopies, 3);
  deck = changeQuantity(deck, deck.entries[1].key, 1);
  assert.equal(deck.entries[1].finish, 'foil');
  assert.equal(deck.entries[1].qty, 4);
  // New additions still respect the currently offered finishes.
  deck = addCard(deck, refreshed, { finish: 'foil' });
  assert.equal(deck.entries[0].qty, 3);
  assert.equal(deck.entries[1].qty, 4);
  assert.equal(deck.entries[1].key, 'bolt-printing:foil:main');
});

test('normalization rejects malformed rows, caps quantities, and merges duplicate identities', () => {
  const raw = {
    name: '  Draft night  ', entries: [
      { card: card(), qty: '2' },
      { card: card(), qty: 3, key: 'fabricated' },
      { card: card({ id: 'huge' }), qty: 1e10 },
      { card: card({ id: 'invalid' }), qty: Infinity },
      { card: card({ id: 'negative' }), qty: -4 },
      { card: { id: 'nameless' }, qty: 1 },
      { card: card({ id: '' }), qty: 1 },
      null,
    ],
  };
  const normalized = normalizeDeck(raw);
  assert.equal(normalized.name, 'Draft night');
  assert.equal(normalized.entries.length, 2);
  assert.equal(normalized.entries[0].qty, 5);
  assert.equal(normalized.entries[0].key, 'bolt-printing:nonfoil:main');
  assert.equal(normalized.entries[1].qty, 9999);
  assert.equal(raw.entries[0].qty, '2');
  assert.deepEqual(normalizeDeck(null), createDeck());
  assert.deepEqual(normalizeDeck([]), createDeck());
});

test('prototype-like keys from imported JSON cannot alter prototypes or enter the deck', () => {
  const malicious = JSON.parse('{"__proto__":{"polluted":true},"entries":[{"qty":1,"card":{"id":"safe","name":"Safe","__proto__":{"polluted":true},"prices":{"usd":"1.00","constructor":{"prototype":{"polluted":true}}}}},{"qty":1,"card":{"id":"__proto__","name":"Bad"}}]}');
  const normalized = normalizeDeck(malicious);
  assert.equal(normalized.entries.length, 1);
  assert.equal(Object.hasOwn(normalized.entries[0].card, '__proto__'), false);
  assert.equal(Object.hasOwn(normalized.entries[0].card.prices, 'constructor'), false);
  assert.equal({}.polluted, undefined);
});

test('malformed optional metadata is discarded without losing a valid holding or changing input', () => {
  const malformed = card({
    set: { code: 'bad' }, set_name: ['bad'], collector_number: {}, type_line: 12,
    layout: false, mana_cost: {}, oracle_text: [], rarity: 42, scryfall_uri: ['bad'],
    colors: ['R', null, {}],
    image_uris: { normal: { url: 'bad' }, small: 'https://example.com/small.jpg' },
    card_faces: [null, 12, 'invalid', [],
      { name: 'Front', type_line: 'Sorcery', mana_cost: {}, oracle_text: 'Valid rules.', image_uris: { normal: 'https://example.com/front.jpg', small: 42 } },
      { name: {}, oracle_text: 'Other valid rules.', image_uris: 'invalid' },
    ],
  });
  const snapshot = JSON.stringify(malformed);
  const deck = addCard(createDeck(), malformed, { qty: 2 });
  const normalized = deck.entries[0].card;
  assert.equal(deck.entries[0].qty, 2);
  assert.equal(normalized.name, 'Lightning Bolt');
  for (const field of ['set', 'set_name', 'collector_number', 'type_line', 'layout', 'mana_cost', 'oracle_text', 'rarity', 'scryfall_uri']) {
    assert.equal(Object.hasOwn(normalized, field), false, field);
  }
  assert.deepEqual(normalized.colors, ['R']);
  assert.deepEqual(normalized.image_uris, { small: 'https://example.com/small.jpg' });
  assert.equal(normalized.card_faces.length, 2);
  assert.equal(Object.hasOwn(normalized.card_faces[1], 'name'), false);
  assert.equal(Object.hasOwn(normalized.card_faces[1], 'image_uris'), false);
  assert.equal(imageUrl(normalized), 'https://example.com/front.jpg');
  assert.match(rulesText(normalized), /Valid rules\./);
  assert.doesNotMatch(rulesText(normalized), /\[object Object\]/);
  assert.equal(JSON.stringify(malformed), snapshot);
});

test('image and rules helpers tolerate malformed optional data even before normalization', () => {
  for (const faces of [null, 'invalid', 42, {}, { find: 'not callable' }]) {
    const malformed = card({ card_faces: faces, image_uris: null, oracle_text: {} });
    assert.equal(imageUrl(malformed), '');
    assert.equal(rulesText(malformed), '');
    assert.equal(Object.hasOwn(addCard(createDeck(), malformed).entries[0].card, 'card_faces'), false);
  }
  assert.equal(imageUrl(null), '');
  assert.equal(rulesText(null), '');
  assert.equal(imageUrl({ image_uris: { normal: {} }, card_faces: [null, {}, { image_uris: { normal: 'https://example.com/card.jpg' } }] }), 'https://example.com/card.jpg');
});

test('adding stale search results retains newer prices while accepting other metadata updates', () => {
  const fresh = card({ prices: { usd: '9.25', usd_foil: '14.00' }, prices_checked_at: '2026-11-01T16:00:00.000Z' });
  let deck = addCard(createDeck(), fresh);
  deck = addCard(deck, card({ name: 'Updated name', prices_checked_at: '2026-11-01T12:00:00.000Z' }));
  assert.equal(deck.entries[0].qty, 2);
  assert.equal(deck.entries[0].card.name, 'Updated name');
  assert.equal(deck.entries[0].card.prices.usd, '9.25');
  assert.equal(deck.entries[0].card.prices_checked_at, '2026-11-01T16:00:00.000Z');
  deck = addCard(deck, card({ prices_checked_at: undefined }));
  assert.equal(deck.entries[0].card.prices.usd, '9.25');
  // A newly added finish of this printing can reuse its latest complete quote.
  deck = addCard(deck, card({ prices_checked_at: 'invalid date' }), { finish: 'foil' });
  assert.equal(cardPrice(deck.entries[1].card, 'foil'), 14);
  assert.equal(summarizeDeck(deck).value, 41.75);
  const newer = addCard(deck, card({ prices: { usd: '10.00' }, prices_checked_at: '2026-11-02T16:00:00.000Z' }));
  assert.equal(newer.entries[0].card.prices.usd, '10.00');
  assert.equal(deck.entries[0].card.prices.usd, '9.25');
});

test('target edits preserve visible mismatches, sanitize invalid numbers, and remain immutable', () => {
  const original = createDeck();
  const modified = setPlan(original, { lands: 18, curve: [0, 2, 6, 6, 4, 3, 2] });
  assert.equal(summarizeDeck(modified).planTotal, 41);
  assert.equal(modified.plan.deckSize, 40);
  assert.equal(original.plan.lands, 17);
  const sanitized = setPlan(modified, { deckSize: Infinity, lands: -5, curve: [NaN, -1, 2.9] });
  assert.equal(sanitized.plan.deckSize, 40);
  assert.equal(sanitized.plan.lands, 0);
  assert.deepEqual(sanitized.plan.curve, [0, 0, 2, 6, 4, 3, 2]);
});

test('empty and malformed decks produce finite zero statistics', () => {
  for (const input of [null, {}, { entries: [null, { qty: 'bad' }] }]) {
    const totals = summarizeDeck(input);
    assert.equal(totals.total, 0);
    assert.equal(totals.value, 0);
    assert.equal(totals.avgMana, 0);
    assert.equal(totals.curve.reduce((sum, count) => sum + count, 0), 0);
  }
});
