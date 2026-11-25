const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const Core = require('../game/balatle/core.js');

function card(rank, suit, id, extra) {
  const rv = ({ J: 11, Q: 12, K: 13, A: 14 })[rank] || Number(rank);
  return { id: id || `${rank}${suit}`, rank: String(rank), rv, suit, baseChip: rv === 14 ? 11 : Math.min(10, rv), bonusChip: 0, ...extra };
}

test('Pairs, trips, and quads keep their base value without unrelated filler', () => {
  for (const [count, name, chips, mult] of [[2, 'Pair', 10, 2], [3, 'Three of a Kind', 30, 3], [4, 'Four of a Kind', 60, 7]]) {
    const matching = ['S', 'H', 'D', 'C'].slice(0, count).map(suit => card(8, suit));
    const filler = [card('A', 'C'), card('K', 'H'), card(3, 'D')].slice(0, 5 - count);
    const short = Core.evaluateHand(matching);
    const long = Core.evaluateHand([...matching, ...filler]);
    assert.equal(short.name, name);
    assert.equal(long.name, name);
    assert.deepEqual([short.baseChips, short.baseMult], [chips, mult]);
    assert.deepEqual([long.baseChips, long.baseMult], [chips, mult]);
    assert.deepEqual(long.scoringIds, matching.map(c => c.id));
  }
});

test('High Card scores only the highest rank and Two Pair excludes its kicker', () => {
  const hand = [card(2, 'H'), card('K', 'C'), card(7, 'S'), card('A', 'D'), card(10, 'H')];
  assert.deepEqual(Core.evaluateHand(hand).scoringIds, ['AD']);
  assert.equal(Core.evaluateHand(hand).baseChips, 5);
  assert.equal(Core.evaluateHand([hand[3]]).baseChips, 5);
  const pairs = [card(2, 'H'), card('A', 'D'), card(2, 'C'), card(7, 'H'), card(7, 'S')];
  assert.equal(Core.evaluateHand(pairs).name, 'Two Pair');
  assert.deepEqual(Core.evaluateHand(pairs).scoringIds, ['2H', '2C', '7H', '7S']);
});

test('Straight and flush precedence, wheel aces, and full houses are recognized', () => {
  const wheel = [card('A', 'S'), card(2, 'H'), card(3, 'D'), card(4, 'C'), card(5, 'S')];
  assert.equal(Core.evaluateHand(wheel).name, 'Straight');
  const royal = [10, 'J', 'Q', 'K', 'A'].map(rank => card(rank, 'H'));
  assert.equal(Core.evaluateHand(royal).name, 'Straight Flush');
  assert.deepEqual(Core.evaluateHand(royal).scoringIds, royal.map(c => c.id));
  const flush = [2, 4, 6, 9, 'K'].map(rank => card(rank, 'C'));
  assert.equal(Core.evaluateHand(flush).name, 'Flush');
  const full = [card(7, 'C'), card(7, 'H'), card(7, 'D'), card(9, 'C'), card(9, 'S')];
  assert.equal(Core.evaluateHand(full).name, 'Full House');
  assert.equal(Core.evaluateHand([card(2, 'H'), card(3, 'D'), card(4, 'S'), card(5, 'H'), card(5, 'D')]).name, 'Pair');
  assert.equal(Core.evaluateHand([card('Q', 'H'), card('K', 'D'), card('A', 'S'), card(2, 'H'), card(3, 'D')]).name, 'High Card');
});

test('Wild suits bridge a flush but never bridge a missing rank or conflicting natural suits', () => {
  const cards = [card(2, 'H'), card(4, 'H'), card(7, 'H'), card(10, 'S', 'wild', { wild: true }), card('K', 'H')];
  assert.equal(Core.evaluateHand(cards).name, 'Flush');
  const conflict = cards.map(c => ({ ...c }));
  conflict[0].suit = 'C';
  assert.equal(Core.evaluateHand(conflict).name, 'High Card');
  const missingRank = [card(2, 'H'), card(3, 'C'), card(4, 'D'), card(5, 'C'), card(9, 'S', 'rank-wild', { wild: true })];
  assert.equal(Core.evaluateHand(missingRank).name, 'High Card');
  assert.equal(Core.evaluateHand(cards.map(c => ({ ...c, wild: true }))).name, 'Flush');
});

test('Duplicated cards can make Five of a Kind, Flush House, and Flush Five', () => {
  const five = ['S', 'H', 'D', 'C', 'S'].map((suit, i) => card(8, suit, `copy-${i}`));
  assert.equal(Core.evaluateHand(five).name, 'Five of a Kind');
  assert.deepEqual(Core.evaluateHand(five).scoringIds, five.map(c => c.id));
  const house = [8, 8, 8, 6, 6].map((rank, i) => card(rank, 'H', `house-${i}`));
  assert.equal(Core.evaluateHand(house).name, 'Flush House');
  const sameSuit = five.map(c => ({ ...c, suit: 'S' }));
  assert.equal(Core.evaluateHand(sameSuit).name, 'Flush Five');
  assert.equal(Core.evaluateHand(sameSuit).tier, 12);
  assert.equal(Core.evaluateHand(sameSuit).baseChips, 160);
});

test('Boss downgrades score the downgraded hand and preserve original play order', () => {
  const cards = [card(9, 'D'), card(2, 'H'), card(9, 'C'), card('A', 'S')];
  assert.deepEqual(Core.scoringCards(cards, 'High Card'), [cards[3]]);
  assert.deepEqual(Core.scoringCards(cards, 'Pair'), [cards[0], cards[2]]);
  assert.deepEqual(Core.scoringCards(cards, 'Two Pair'), []);
  assert.deepEqual(Core.scoringCards(cards, 'Unknown'), []);
});

test('Evaluation leaves cards, enhancements, and their order untouched', () => {
  const cards = Object.freeze([Object.freeze(card('K', 'H', 'first', { bonusChip: 20 })), Object.freeze(card('K', 'D', 'second'))]);
  const result = Core.evaluateHand(cards);
  assert.equal(result.name, 'Pair');
  assert.deepEqual(Core.scoringCards(cards, result.name), Array.from(cards));
  assert.equal(cards[0].bonusChip, 20);
  assert.equal(cards[0].id, 'first');
});

test('Empty, oversized, and malformed selections do not create a score', () => {
  for (const cards of [[], null, [null], [card(20, 'S')], [card(4, 'X')], [2, 3, 4, 5, 6, 7].map(rank => card(rank, 'H'))]) {
    assert.equal(Core.evaluateHand(cards), null);
  }
  assert.deepEqual(Core.scoringCards([], 'High Card'), []);
});

test('Blind targets climb within each ante and each blind type grows through eight antes', () => {
  assert.deepEqual([1, 2, 3, 4, 5, 6].map(round => Core.targetForRound(round)), [300, 450, 600, 800, 1200, 1600]);
  for (let round = 1; round <= 28; round += 3) {
    assert.ok(Core.targetForRound(round + 1) > Core.targetForRound(round));
    assert.ok(Core.targetForRound(round + 2) > Core.targetForRound(round + 1));
    assert.ok(Core.targetForRound(round + 3) > Core.targetForRound(round));
  }
  assert.equal(Core.targetForRound(22), 50000);
  assert.equal(Core.targetForRound(24), 100000);
  assert.equal(Core.targetForRound(25), 125000);
  assert.equal(Core.targetForRound(1, 'easy'), 264);
  assert.equal(Core.targetForRound(1, 'hard'), 366);
  assert.equal(Core.targetForRound(0), 300);
  assert.ok(Number.isSafeInteger(Core.targetForRound(999999)));
});

test('Cash-out rewards unused hands and calculates capped interest before rewards', () => {
  assert.deepEqual(Core.settlement({ money: 4, reward: 6, handsLeft: 3 }), { reward: 6, handBonus: 3, interest: 0, total: 9 });
  assert.deepEqual(Core.settlement({ money: 24, reward: 12, handsLeft: 2 }), { reward: 12, handBonus: 2, interest: 4, total: 18 });
  assert.equal(Core.settlement({ money: 1000, reward: 6, handsLeft: 0 }).interest, 5);
  assert.equal(Core.settlement({ money: 1000, interestBoost: 1 }).interest, 10);
  assert.equal(Core.settlement({ money: 25, halveInterest: true }).interest, 2);
  assert.equal(Core.settlement({ money: 25, interestBoost: 1, halveInterest: true }).interest, 5);
  assert.deepEqual(Core.settlement({ money: -1, reward: -1, handsLeft: -2 }), { reward: 0, handBonus: 0, interest: 0, total: 0 });
});

test('The same core loads as a plain browser script without a module loader', () => {
  const context = vm.createContext({});
  vm.runInContext(fs.readFileSync(path.join(__dirname, '../game/balatle/core.js'), 'utf8'), context);
  assert.equal(context.BalatleCore.evaluateHand([card('A', 'H')]).name, 'High Card');
  assert.equal(context.BalatleCore.targetForRound(1), 300);
});
