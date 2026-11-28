const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const Core = require('../game/balatle/core.js');

// The production game remains a standalone page. This harness exposes its actual
// functions inside the VM; it replaces rendering and animation, not game rules.
class Element {
  constructor(id = '') {
    this.id = id;
    this.style = { setProperty() {}, removeProperty() {} };
    this.dataset = {};
    this.attributes = new Map();
    this.listeners = new Map();
    this.children = [];
    this._queries = new Map();
    this.textContent = '';
    this.innerHTML = '';
    this.disabled = false;
    this.hidden = false;
    this.value = '';
    const classes = new Set();
    this.classList = {
      add(...items) { items.forEach(item => classes.add(item)); },
      remove(...items) { items.forEach(item => classes.delete(item)); },
      contains(item) { return classes.has(item); },
      toggle(item, force) {
        const enable = force == null ? !classes.has(item) : !!force;
        if (enable) classes.add(item); else classes.delete(item);
        return enable;
      }
    };
  }
  setAttribute(name, value) { this.attributes.set(name, String(value)); }
  getAttribute(name) { return this.attributes.get(name) ?? null; }
  removeAttribute(name) { this.attributes.delete(name); }
  addEventListener(name, fn) { this.listeners.set(name, fn); }
  removeEventListener(name) { this.listeners.delete(name); }
  appendChild(child) { child.parentElement = this; this.children.push(child); return child; }
  append(...children) { children.forEach(child => this.appendChild(child)); }
  replaceChildren(...children) { this.children = []; this.append(...children); }
  querySelector(selector) {
    if (!this._queries.has(selector)) this._queries.set(selector, new Element(selector));
    return this._queries.get(selector);
  }
  querySelectorAll() { return []; }
  closest() { return null; }
  focus() {}
  remove() {}
}

function harness(storage = new Map()) {
  const elements = new Map();
  const document = {
    body: new Element('body'),
    documentElement: new Element('html'),
    getElementById(id) {
      if (!elements.has(id)) {
        const element = new Element(id);
        element.parentElement = new Element(`${id}-parent`);
        elements.set(id, element);
      }
      return elements.get(id);
    },
    createElement() { return new Element(); },
    querySelector(selector) { return document.getElementById(selector); },
    querySelectorAll() { return []; },
    addEventListener() {}
  };
  const animation = new Proxy({}, { get: (target, property) => target[property] || (() => Promise.resolve()) });
  const sandbox = {
    console,
    document,
    localStorage: { getItem: key => storage.get(key) ?? null, setItem: (key, value) => storage.set(key, String(value)), removeItem: key => storage.delete(key) },
    setTimeout: () => 0,
    clearTimeout() {},
    setInterval: () => 0,
    clearInterval() {},
    requestAnimationFrame: fn => { fn(0); return 0; },
    cancelAnimationFrame() {},
    performance: { now: () => 0 },
    addEventListener() {},
    removeEventListener() {},
    matchMedia: () => ({ matches: true, addEventListener() {} }),
    navigator: {},
    location: { search: '', href: 'http://localhost/game/balatlesolo.html' },
    URLSearchParams,
    prompt: () => null,
    confirm: () => true,
    BalatleCore: Core,
    BalatleTable: animation,
    BalatleUI: animation
  };
  sandbox.window = sandbox;
  const context = vm.createContext(sandbox);
  const html = fs.readFileSync(path.join(__dirname, '../game/balatlesolo.html'), 'utf8');
  const script = Array.from(html.matchAll(/<script\b[^>]*>([\s\S]*?)<\/script>/gi), match => match[1]).find(source => source.includes('function playSelected('));
  assert.ok(script, 'The real solo game script must be present');
  const boot = script.lastIndexOf('\n  boot();');
  assert.ok(boot >= 0, 'The solo boot invocation must be instrumentable');
  const expose = `
  renderAll = function () {};
  renderHand = function () {};
  renderRight = function () {};
  renderTop = function () {};
  renderShop = function () {};
  globalThis.__solo = {
    game, options, handUp, rng, evalHand,
    calculateScore: typeof calculateScore === 'function' ? calculateScore : null,
    playSelected, discardSelected, startNewRun, applyRoundConfig,
    finishRound, enterBuyPhase, nextRound, drawToHand,
    useConsumable, pickBossRuleForRound, roundBossRules, BOSS_RULES,
    buyOffer, effectiveOfferCost, buyLegacyUpgrade, endRun,
    offers: () => shopOffers,
    cancelPendingConsumable: typeof cancelPendingConsumable === 'function' ? cancelPendingConsumable : null,
    FULL_JOKER_POOL, FULL_JESTER_POOL, FULL_CONSUME_POOL, SHOP_CARD_CATALOG, mkJoker
  };
`;
  vm.runInContext(script.slice(0, boot) + expose + script.slice(boot + '\n  boot();'.length), context, { filename: 'balatlesolo.html' });
  const api = context.__solo;
  api.startNewRun();
  api.options.autoFinishOnTarget = false;
  api.game.target = 1_000_000;
  return { api, elements, context, animation, storage };
}

function card(rank, suit, id, extra) {
  const rv = ({ J: 11, Q: 12, K: 13, A: 14 })[rank] || Number(rank);
  const suitSym = ({ S: '♠', H: '♥', D: '♦', C: '♣' })[suit];
  return { id: id || `${rank}${suit}`, rank: String(rank), rv, suit, suitSym, red: suit === 'H' || suit === 'D', baseChip: rv === 14 ? 11 : Math.min(10, rv), bonusChip: 0, ...extra };
}

function selectFixture(api, selected) {
  api.game.hand = [...selected, ...api.game.hand.slice(0, Math.max(0, 8 - selected.length))];
  api.game.sel = new Set(selected.map(c => c.id));
  return selected;
}

test('Previewing a hand never pays money, grows jokers, or upgrades a hand', () => {
  const { api } = harness();
  assert.equal(typeof api.calculateScore, 'function');
  const cards = selectFixture(api, [card(8, 'H'), card(8, 'D')]);
  api.game.jokers = ['Growing Grin', 'Diamond Dealer'].map(name => api.mkJoker(api.FULL_JOKER_POOL.find(item => item.name === name)));
  api.game.jesters = [api.mkJoker(api.FULL_JESTER_POOL.find(item => item.name.includes('Chaos Jester')))];
  const before = JSON.stringify({ money: api.game.money, jokers: api.game.jokers, handUp: api.handUp, score: api.game.blindScore, hands: api.game.handsLeft });
  for (let i = 0; i < 5; i++) api.calculateScore(cards);
  assert.equal(JSON.stringify({ money: api.game.money, jokers: api.game.jokers, handUp: api.handUp, score: api.game.blindScore, hands: api.game.handsLeft }), before);
});

test('Playing a Pair removes its kicker but does not score the kicker or its enhancement', async () => {
  const { api } = harness();
  selectFixture(api, [card(8, 'H'), card('A', 'D', 'kicker', { bonusChip: 100, crowned: true }), card(8, 'C')]);
  await api.playSelected();
  assert.equal(api.game.blindScore, (10 + 8 + 8) * 2);
  assert.equal(api.game.handsLeft, 3);
  assert.equal(api.game.discard.some(c => c.id === 'kicker'), true);
  assert.equal(api.game.hand.some(c => c.id === 'kicker'), false);
});

test('Joker order changes +Mult then ×Mult into the correct sequential result', async () => {
  async function scoreWith(jokers) {
    const { api } = harness();
    selectFixture(api, [card(8, 'H'), card(8, 'C')]);
    api.game.jokers = jokers;
    await api.playSelected();
    return api.game.blindScore;
  }
  const additive = { jid: 'add', name: 'Additive fixture', state: {}, apply(ctx) { ctx.addMult += 4; } };
  const multiplicative = { jid: 'multiply', name: 'Multiplicative fixture', state: {}, apply(ctx) { ctx.mulMult *= 2; } };
  assert.equal(await scoreWith([additive, multiplicative]), 26 * 12);
  assert.equal(await scoreWith([multiplicative, additive]), 26 * 8);
});

test('Rapid Play and Discard inputs cannot spend resources twice during score playback', async () => {
  const { api, animation } = harness();
  selectFixture(api, [card(8, 'H'), card(8, 'D')]);
  api.game.jokers = ['Growing Grin', 'Diamond Dealer'].map(name => api.mkJoker(api.FULL_JOKER_POOL.find(item => item.name === name)));
  let finishAnimation;
  animation.scorePlayback = () => new Promise(resolve => { finishAnimation = resolve; });
  const first = api.playSelected();
  assert.equal(api.game.scoring, true);
  await api.playSelected();
  api.discardSelected();
  assert.equal(api.game.handsLeft, 4);
  assert.equal(api.game.discardsLeft, 3);
  assert.equal(api.game.money, 10);
  finishAnimation();
  await first;
  assert.equal(api.game.handsLeft, 3);
  assert.equal(api.game.discardsLeft, 3);
  assert.equal(api.game.blindScore, 52);
  assert.equal(api.game.money, 11);
  assert.equal(api.game.jokers[0].state.bonus, 1);
  assert.equal(api.game.scoring, false);
});

test('A delayed scoring animation cannot apply its result to a restarted run', async () => {
  const { api, animation } = harness();
  selectFixture(api, [card(8, 'H'), card(8, 'D')]);
  let finishAnimation;
  animation.scorePlayback = () => new Promise(resolve => { finishAnimation = resolve; });
  const stalePlay = api.playSelected();
  api.startNewRun();
  const freshIds = api.game.hand.map(c => c.id);
  finishAnimation();
  await stalePlay;
  assert.equal(api.game.blindScore, 0);
  assert.equal(api.game.handsLeft, 4);
  assert.equal(api.game.money, 10);
  assert.deepEqual(api.game.hand.map(c => c.id), freshIds);
});

test('Repeated Finish requests pay the blind, remaining hands, and interest exactly once', () => {
  const { api } = harness();
  api.game.money = 24;
  api.game.handsLeft = 2;
  api.game.blindScore = api.game.target;
  api.finishRound();
  assert.equal(api.game.mode, 'shop');
  assert.equal(api.game.money, 36); // $6 blind + $2 unused hands + $4 savings interest.
  const total = api.game.totalScore;
  api.finishRound();
  assert.equal(api.game.money, 36);
  assert.equal(api.game.totalScore, total);
});

test('A fresh blind restores the persistent deck, preserving enhancements and cloned cards', () => {
  const { api } = harness();
  const enhanced = api.game.hand[0];
  enhanced.bonusChip = 17;
  enhanced.crowned = true;
  const clone = { ...enhanced, id: 'persistent-clone' };
  api.game.discard.push(clone);
  const removed = api.game.deck.pop();
  const expected = [...api.game.hand, ...api.game.deck, ...api.game.discard].map(c => c.id).sort();
  api.game.sel.add(enhanced.id);
  api.applyRoundConfig(2);
  const allCards = [...api.game.hand, ...api.game.deck, ...api.game.discard];
  assert.deepEqual(allCards.map(c => c.id).sort(), expected);
  assert.equal(new Set(allCards.map(c => c.id)).size, expected.length);
  assert.equal(allCards.some(c => c.id === removed.id), false);
  assert.equal(allCards.find(c => c.id === enhanced.id).bonusChip, 17);
  assert.equal(allCards.find(c => c.id === clone.id).crowned, true);
  assert.equal(api.game.hand.length, 8);
  assert.equal(api.game.discard.length, 0);
  assert.equal(api.game.sel.size, 0);
});

test('Drawing within a blind never recycles its spent cards', () => {
  const { api } = harness();
  api.game.hand = [card('A', 'H')];
  api.game.deck = [];
  api.game.discard = [card('K', 'H'), card('Q', 'C')];
  api.drawToHand();
  assert.equal(api.game.hand.length, 1);
  assert.equal(api.game.deck.length, 0);
  assert.deepEqual(api.game.discard.map(c => c.id), ['KH', 'QC']);
});

test('Cancelling a targeted consumable keeps the item and leaves the deck unchanged', () => {
  const { api } = harness();
  assert.equal(typeof api.cancelPendingConsumable, 'function');
  const paint = { ...api.FULL_CONSUME_POOL.find(item => item.name === 'Tarot: Chip Paint') };
  api.game.cons = [paint];
  const before = JSON.stringify(api.game.hand);
  api.useConsumable(0);
  assert.ok(api.game.pendingUse);
  api.cancelPendingConsumable();
  assert.equal(api.game.pendingUse, null);
  assert.equal(api.game.cons.length, 1);
  assert.equal(api.game.cons[0].name, paint.name);
  assert.equal(JSON.stringify(api.game.hand), before);
});

test('A boss-only consumable is retained if used outside a Boss blind', () => {
  const { api } = harness();
  const cleanse = { ...api.FULL_CONSUME_POOL.find(item => item.name === 'Spectral: Cleanse') };
  api.game.cons = [cleanse];
  api.useConsumable(0);
  assert.equal(api.game.cons.length, 1);
  assert.equal(api.game.cons[0].name, cleanse.name);
  assert.equal(api.game.pendingUse, null);
});

test('Boss encounters cover the roster in a repeatable order and vary across endless cycles', () => {
  const { api } = harness();
  const rosterSize = api.BOSS_RULES.length;
  assert.ok(rosterSize >= 20);
  const schedule = seed => Array.from({ length: rosterSize * 3 }, (_, index) => api.pickBossRuleForRound(seed, (index + 1) * 3).key);
  const bosses = schedule(3381);
  assert.deepEqual(bosses, schedule(3381));
  assert.notDeepEqual(bosses, schedule(9818));
  for (let start = 0; start < bosses.length; start += rosterSize) {
    assert.equal(new Set(bosses.slice(start, start + rosterSize)).size, rosterSize);
  }
  for (let i = 1; i < bosses.length; i++) assert.notEqual(bosses[i], bosses[i - 1]);
  api.game.seed = 3381;
  api.applyRoundConfig(rosterSize * 3 + 3);
  assert.equal(api.game.blindType, 'Boss');
  assert.equal(api.game.bossRule.key, bosses[rosterSize]);
});

test('Echo Jester penalizes repeated hand types only after a committed play', async () => {
  const { api } = harness();
  api.game.blindType = 'Boss';
  api.game.bossRule = api.BOSS_RULES.find(boss => boss.key === 'repeatHand');
  api.game.boss = api.roundBossRules(api.game.bossRule, api.game.seed, 3, api.game.target);
  const firstPair = selectFixture(api, [card(8, 'H', 'first-pair-1'), card(8, 'D', 'first-pair-2')]);
  const secondPair = [card(9, 'H', 'next-pair-1'), card(9, 'C', 'next-pair-2')];
  for (let i = 0; i < 3; i++) assert.equal(api.calculateScore(firstPair).mult, 2);
  assert.equal(api.game.boss.plays, 0);
  await api.playSelected();
  assert.equal(api.game.boss.plays, 1);
  assert.equal(api.calculateScore(secondPair).mult, 2 * 0.55);
  assert.equal(api.calculateScore([secondPair[0]]).mult, 1);
  assert.equal(api.game.boss.plays, 1);
});

test('Clearing a blind awards legacy medallions once and a new run can earn more', () => {
  const { api, storage } = harness();
  const balance = () => JSON.parse(storage.get('balatle_solo_legacy_v1')).medallions;
  api.game.blindScore = api.game.target - 1;
  api.finishRound();
  assert.equal(api.game.mode, 'play');
  api.game.blindScore = api.game.target;
  api.finishRound();
  assert.equal(balance(), 1);
  api.finishRound();
  api.enterBuyPhase();
  assert.equal(balance(), 1);
  api.applyRoundConfig(3);
  api.game.blindScore = api.game.target;
  api.finishRound();
  assert.equal(balance(), 4);
  api.endRun('Run finished.');
  api.buyLegacyUpgrade('pocket-change');
  assert.equal(balance(), 2);
  assert.equal(JSON.parse(storage.get('balatle_solo_legacy_v1')).upgrades['pocket-change'], 1);
  api.startNewRun();
  assert.equal(api.game.money, 13);
  api.game.blindScore = api.game.target;
  api.finishRound();
  assert.equal(balance(), 3);
  assert.equal(harness(storage).api.game.money, 13, 'Purchased upgrades survive a browser reload');
});

test('A purchased shop card enters the next hand once and persists across later blinds', () => {
  const { api } = harness();
  api.game.blindScore = api.game.target;
  api.finishRound();
  const cards = api.offers().filter(offer => offer._kind === 'card');
  assert.ok(cards.length >= 2, 'The shop provides distinct playable card options');
  assert.equal(new Set(cards.map(offer => offer.name)).size, cards.length);
  const offer = cards[0];
  const cost = api.effectiveOfferCost(offer);
  api.game.money = 0;
  api.buyOffer(offer._id);
  assert.equal(api.game.discard.length, 0);
  api.game.money = 100;
  api.buyOffer(offer._id);
  api.buyOffer(offer._id);
  assert.equal(api.game.money, 100 - cost);
  assert.equal(api.game.discard.length, 1);
  const bought = api.game.discard[0];
  assert.equal(bought.editionName, offer.name);
  api.nextRound();
  assert.equal(api.game.hand.filter(card => card.id === bought.id).length, 1);
  api.applyRoundConfig(4);
  const cardsAfterShuffle = [...api.game.hand, ...api.game.deck, ...api.game.discard];
  assert.equal(cardsAfterShuffle.filter(card => card.id === bought.id).length, 1);
  assert.equal(cardsAfterShuffle.find(card => card.id === bought.id).editionName, offer.name);
});

test('Three-Bell Acrobat counts scoring cards and ignores unrelated cards', () => {
  const { api } = harness();
  const jester = api.FULL_JESTER_POOL.find(item => item.name.includes('Three-Bell Acrobat'));
  assert.ok(jester);
  const trip = [card(8, 'H'), card(8, 'C'), card(8, 'D'), card('A', 'S')];
  const pair = [trip[0], trip[1], trip[3]];
  const beforeTrip = api.calculateScore(trip);
  const beforePair = api.calculateScore(pair);
  api.game.jesters = [api.mkJoker(jester)];
  assert.equal(api.calculateScore(trip).mult, beforeTrip.mult * 1.9);
  assert.equal(api.calculateScore(pair).mult, beforePair.mult);
});

test('Gilded Ace never pays from a preview or from an unscored kicker', async () => {
  const { api } = harness();
  const gilded = { ...api.SHOP_CARD_CATALOG.find(offer => offer.name === 'Gilded Ace').card, id: 'gilded' };
  const pairWithKicker = [card(8, 'D'), card(8, 'C'), gilded];
  const before = api.game.money;
  assert.equal(api.calculateScore(pairWithKicker).scoringIds.includes(gilded.id), false);
  assert.equal(api.calculateScore(pairWithKicker).payout.length, 0);
  assert.equal(api.calculateScore([gilded]).payout.length, 1);
  assert.equal(api.game.money, before);
  selectFixture(api, [gilded]);
  await api.playSelected();
  assert.equal(api.game.money, before + 2);
});
