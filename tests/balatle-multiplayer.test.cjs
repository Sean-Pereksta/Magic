const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const Core = require('../game/balatle/core.js');

// Execute the actual multiplayer client with an in-memory Firebase boundary.
// These tests never authenticate, read a live room, or write a player's record.
class Element {
  constructor(id = '') {
    this.id = id;
    this.style = {};
    this.dataset = {};
    this.attributes = new Map();
    this.listeners = new Map();
    this.children = [];
    this.queries = new Map();
    this.parentElement = null;
    this.textContent = '';
    this.disabled = false;
    this.hidden = false;
    const classes = new Set();
    this.classList = {
      add: (...items) => items.forEach(item => classes.add(item)),
      remove: (...items) => items.forEach(item => classes.delete(item)),
      contains: item => classes.has(item),
      toggle(item, force) {
        const next = force == null ? !classes.has(item) : !!force;
        if (next) classes.add(item); else classes.delete(item);
        return next;
      }
    };
  }
  set innerHTML(value) { this.markup = value; this.children = []; }
  get innerHTML() { return this.markup || ''; }
  setAttribute(name, value) { this.attributes.set(name, String(value)); }
  getAttribute(name) { return this.attributes.get(name) ?? null; }
  addEventListener(event, listener) { this.listeners.set(event, listener); }
  appendChild(child) { child.parentElement = this; this.children.push(child); return child; }
  querySelector(selector) {
    if (!this.queries.has(selector)) this.queries.set(selector, new Element(selector));
    return this.queries.get(selector);
  }
  querySelectorAll() { return []; }
  focus() {}
  click() { if (!this.disabled) return this.listeners.get('click')?.({ target: this }); }
}

function harness() {
  const elements = new Map();
  const writes = [];
  const backend = { setDoc: async (reference, value) => { writes.push({ reference, value }); } };
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
    querySelector() { return null; }
  };
  const animation = {
    cancelPlayback() {},
    scorePlayback: () => Promise.resolve({ cancelled: false }),
    cardMarkup: card => `${card.rank}${card.suitSym}`,
    itemArt: () => '',
    escapeHTML: value => String(value),
    toggleSound() {},
    isSoundOn: () => false
  };
  const sandbox = {
    console, document, URLSearchParams,
    location: { search: '?gameId=test-room&username=Tester' },
    setTimeout: () => 0, clearTimeout() {}, setInterval: () => 0, clearInterval() {},
    addEventListener() {}, confirm: () => true,
    initializeApp: () => ({}), getFirestore: () => ({}), getAuth: () => ({}),
    doc: (...args) => args.slice(1).join('/'), collection: (...args) => args.slice(1).join('/'),
    setDoc: (...args) => backend.setDoc(...args), getDoc: async () => ({ exists: () => false }),
    onSnapshot: () => () => {}, runTransaction: async () => {}, serverTimestamp: () => ({}),
    arrayUnion: value => value, signInAnonymously: async () => {}, onAuthStateChanged: () => () => {},
    BalatleCore: Core, BalatleTable: animation
  };
  sandbox.window = sandbox;
  const context = vm.createContext(sandbox);
  const html = fs.readFileSync(path.join(__dirname, '../game/balatle.html'), 'utf8');
  const source = Array.from(html.matchAll(/<script\b[^>]*>([\s\S]*?)<\/script>/gi), match => match[1])
    .find(script => script.includes('function playSelected('));
  assert.ok(source, 'The actual multiplayer script must be found');
  const script = source.replace(/\bimport\s+[\s\S]*?\sfrom\s+"[^"]+";\s*/g, '');
  const boot = script.lastIndexOf('\n  boot();');
  assert.ok(boot >= 0, 'The boot call must be instrumentable');
  const expose = `
    const originalRenderHand = renderHand;
    renderAll = function () {};
    renderTop = function () {};
    renderHand = function () {};
    renderRight = function () {};
    renderShop = function () {};
    renderSelection = function () {};
    renderScoreboard = function () {};
    scheduleAdvanceCheck = function () {};
    globalThis.__multi = {
      game, me, handUp, mkJoker, JOKER_POOL, JESTER_POOL, CONSUME_POOL,
      scoreSelection, playSelected, discardSelected, drawToHand,
      startNewRunLocal, applyRoundConfig, enterBuyPhase, finishRound,
      markReady, tickTimers, checkDeckExhausted,
      useConsumable, cancelConsumable, hideHow,
      buyOffer, effectiveOfferCost,
      offers: () => shopOffers,
      drawCards: originalRenderHand,
      setMatch(value) { match = value; }
    };
  `;
  vm.runInContext(script.slice(0, boot) + expose + script.slice(boot + '\n  boot();'.length), context, { filename: 'balatle.html' });
  const api = context.__multi;
  api.startNewRunLocal();
  api.applyRoundConfig(1, { type: 'Small', target: 1_000_000 });
  return { api, animation, backend, writes, elements, document };
}

function card(rank, suit, extra = {}) {
  const rv = ({ J: 11, Q: 12, K: 13, A: 14 })[rank] || Number(rank);
  return {
    id: `${rank}${suit}`, rank: String(rank), rv, suit,
    suitSym: ({ S: '♠', H: '♥', D: '♦', C: '♣' })[suit],
    red: suit === 'H' || suit === 'D', baseChip: rv === 14 ? 11 : Math.min(10, rv), bonusChip: 0,
    ...extra
  };
}

function select(api, cards) {
  api.game.hand = [...cards, ...api.game.hand.filter(item => !cards.some(c => c.id === item.id)).slice(0, 8 - cards.length)];
  api.game.sel = new Set(cards.map(c => c.id));
}

const flushPromises = async () => { for (let i = 0; i < 5; i++) await Promise.resolve(); };

test('Multiplayer previews do not pay money, grow jokers, or mutate hand upgrades', () => {
  const { api } = harness();
  api.game.jokers = ['Growing Grin', 'Diamond Dealer'].map(name => api.mkJoker(api.JOKER_POOL.find(j => j.name === name)));
  const cards = [card(8, 'H'), card(8, 'D')];
  const before = JSON.stringify({ money: api.game.money, jokers: api.game.jokers, upgrades: api.handUp, score: api.game.blindScore });
  for (let i = 0; i < 8; i++) api.scoreSelection(cards);
  assert.equal(JSON.stringify({ money: api.game.money, jokers: api.game.jokers, upgrades: api.handUp, score: api.game.blindScore }), before);
});

test('Multiplayer shops stock a Jester and an illustrated card that joins the next blind', () => {
  const { api } = harness();
  api.game.blindScore = api.game.target;
  api.enterBuyPhase();
  const offers = [...api.offers()];
  const jokers = offers.filter(offer => offer._kind === 'joker');
  assert.equal(new Set(jokers.map(offer => offer.name)).size, 2);
  assert.deepEqual(offers.map(offer => offer._kind).sort(), ['cons', 'jester', 'joker', 'joker', 'playing', 'voucher']);
  const offer = offers.find(item => item._kind === 'playing');
  const jester = offers.find(item => item._kind === 'jester');
  api.game.money = 100;
  const price = api.effectiveOfferCost(offer) + api.effectiveOfferCost(jester);
  api.buyOffer(jester._id);
  api.buyOffer(offer._id);
  api.buyOffer(offer._id);
  assert.equal(api.game.money, 100 - price);
  assert.equal(api.game.jokers.length, 1);
  assert.equal(api.game.jokers[0].name, jester.name);
  assert.equal(api.game.discard.length, 1);
  const bought = api.game.discard[0];
  assert.equal(bought.editionName, offer.name);
  api.applyRoundConfig(2, { type: 'Big', target: 500 });
  const cards = [...api.game.hand, ...api.game.deck];
  assert.equal(cards.filter(item => item.id === bought.id).length, 1);
  assert.equal(cards.find(item => item.id === bought.id).editionName, offer.name);
});

test('Multiplayer Jester payouts apply on commit and use the scored poker hand', async () => {
  const { api } = harness();
  const oddsBroker = api.JESTER_POOL.find(item => item.name.includes('Odds Broker'));
  assert.ok(oddsBroker);
  api.game.jokers = [api.mkJoker(oddsBroker)];
  const cards = [card(9, 'H'), card(9, 'C'), card('K', 'D'), card('K', 'S'), card('A', 'S')];
  select(api, cards);
  const before = api.game.money;
  for (let i = 0; i < 5; i++) assert.equal(api.scoreSelection(cards).h.name, 'Two Pair');
  assert.equal(api.game.money, before);
  await api.playSelected();
  assert.equal(api.game.money, before + 2);
});

test('A Pair scores only its pair cards while still discarding the selected kicker', async () => {
  const { api } = harness();
  select(api, [card(8, 'H'), card('A', 'D', { id: 'kicker', bonusChip: 100 }), card(8, 'C')]);
  await api.playSelected();
  assert.equal(api.game.blindScore, 52);
  assert.equal(api.game.handsLeft, 3);
  assert.equal(api.game.discard.some(c => c.id === 'kicker'), true);
  assert.equal(api.game.hand.some(c => c.id === 'kicker'), false);
  assert.equal(api.game.scoring, false);
});

test('Ordered +Mult and ×Mult jokers yield different, correct totals', () => {
  const { api } = harness();
  const additive = { jid: 'add', name: 'Add fixture', state: {}, apply(ctx) { ctx.addMult += 4; } };
  const multiply = { jid: 'multiply', name: 'Multiply fixture', state: {}, apply(ctx) { ctx.mulMult *= 2; } };
  const cards = [card(8, 'H'), card(8, 'C')];
  api.game.jokers = [additive, multiply];
  assert.equal(api.scoreSelection(cards).gained, 26 * 12);
  api.game.jokers = [multiply, additive];
  assert.equal(api.scoreSelection(cards).gained, 26 * 8);
});

test('Rapid Play and Discard inputs cannot charge a second hand during playback', async () => {
  const { api, animation } = harness();
  select(api, [card(8, 'H'), card(8, 'D')]);
  api.game.jokers = ['Growing Grin', 'Diamond Dealer'].map(name => api.mkJoker(api.JOKER_POOL.find(j => j.name === name)));
  let resolve;
  animation.scorePlayback = () => new Promise(done => { resolve = done; });
  const playback = api.playSelected();
  api.game.sel = new Set(api.game.hand.slice(0, 2).map(c => c.id));
  api.playSelected();
  api.discardSelected();
  assert.equal(api.game.handsLeft, 3);
  assert.equal(api.game.discardsLeft, 3);
  assert.equal(api.game.money, 11);
  assert.equal(api.game.jokers[0].state.bonus, 1);
  resolve({ cancelled: false });
  await playback;
  assert.equal(api.game.scoring, false);
  assert.equal(api.game.blindScore, 52);
});

test('A late playback completion cannot overwrite a restarted same-numbered round', async () => {
  const { api, animation } = harness();
  select(api, [card(8, 'H'), card(8, 'D')]);
  let resolve;
  animation.scorePlayback = () => new Promise(done => { resolve = done; });
  const playback = api.playSelected();
  api.startNewRunLocal();
  api.applyRoundConfig(1, { type: 'Small', target: 300 });
  const freshHand = api.game.hand.map(c => c.id);
  resolve({ cancelled: false });
  await playback;
  assert.equal(api.game.blindScore, 0);
  assert.equal(api.game.handsLeft, 4);
  assert.deepEqual(api.game.hand.map(c => c.id), freshHand);
  assert.equal(api.game.scoring, false);
});

test('Repeated shop entry pays the blind, spare hands, and capped interest once', () => {
  const { api } = harness();
  api.game.money = 24;
  api.game.handsLeft = 2;
  api.enterBuyPhase(6);
  api.enterBuyPhase(6);
  assert.equal(api.game.money, 36);
  assert.equal(api.game.payout.total, 12);
});

test('Snapshot-driven shop entry racing a finishing write cannot pay twice', async () => {
  const { api, backend } = harness();
  api.game.money = 24;
  api.game.handsLeft = 2;
  api.game.blindScore = api.game.target;
  let resolveWrite;
  backend.setDoc = () => new Promise(done => { resolveWrite = done; });
  const finish = api.finishRound(false);
  assert.equal(api.me.status, 'buy');
  api.enterBuyPhase(6); // A snapshot arrives before the awaited setDoc completes.
  assert.equal(api.game.money, 36);
  resolveWrite();
  await finish;
  assert.equal(api.game.money, 36);
  assert.equal(api.game.mode, 'shop');
});

test('Timer expiry during scoring uses the committed score and safely enters shop', async () => {
  const { api, animation } = harness();
  select(api, [card(8, 'H'), card(8, 'D')]);
  api.game.target = 20;
  api.setMatch({ round: 1, roundDeadlineAt: Date.now() - 1000, rounds: [{ type: 'Small', target: 20 }] });
  let resolve;
  animation.scorePlayback = () => new Promise(done => { resolve = done; });
  animation.cancelPlayback = () => resolve?.({ cancelled: true });
  const playback = api.playSelected();
  api.tickTimers();
  await flushPromises();
  await playback;
  assert.equal(api.me.eliminated, false);
  assert.equal(api.me.status, 'buy');
  assert.equal(api.game.mode, 'shop');
  assert.equal(api.game.blindScore, 52);
  assert.equal(api.me.totalScore, 52);
  assert.equal(api.game.money, 21); // $10 + $6 blind + $3 hands + $2 interest.
});

test('An old finishing write cannot reopen shop after a new round has arrived', async () => {
  const { api, backend } = harness();
  api.game.blindScore = api.game.target;
  let resolveWrite;
  backend.setDoc = () => new Promise(done => { resolveWrite = done; });
  const finish = api.finishRound(false);
  api.me.status = 'play';
  api.applyRoundConfig(2, { type: 'Big', target: 500 });
  resolveWrite();
  await finish;
  assert.equal(api.game.mode, 'play');
  assert.equal(api.game.round, 2);
  assert.equal(api.game.money, 10);
});

test('New rounds reshuffle all surviving cards and retain permanent enhancements', () => {
  const { api } = harness();
  const enhanced = api.game.hand[0];
  enhanced.bonusChip = 23;
  api.game.discard.push({ ...enhanced, id: 'duplicate' });
  const before = [...api.game.hand, ...api.game.deck, ...api.game.discard];
  api.applyRoundConfig(2, { type: 'Big', target: 500 });
  const after = [...api.game.hand, ...api.game.deck, ...api.game.discard];
  assert.equal(after.length, before.length);
  assert.equal(after.find(c => c.id === enhanced.id).bonusChip, 23);
  assert.equal(after.find(c => c.id === 'duplicate').bonusChip, 23);
  assert.equal(api.game.hand.length, 8);
  assert.equal(api.game.discard.length, 0);
  api.game.deck = [];
  api.game.hand = [];
  api.game.discard = [enhanced];
  api.drawToHand();
  assert.equal(api.game.hand.length, 0, 'Used cards never recycle within a blind');
});

test('A targeted consumable is retained on cancel and spent exactly once on use', () => {
  const { api, elements } = harness();
  api.game.cons = [{ ...api.CONSUME_POOL.find(c => c.name === 'Tarot: Chip Paint') }];
  api.useConsumable(0);
  assert.equal(api.game.cons.length, 1);
  assert.equal(api.game.pendingUse.type, 'chipPaint');
  api.cancelConsumable();
  assert.equal(api.game.cons.length, 1);
  api.useConsumable(0);
  const cardToPaint = api.game.hand[0];
  api.drawCards();
  elements.get('hand').children[0].click();
  assert.equal(cardToPaint.bonusChip, 5);
  assert.equal(api.game.cons.length, 0);
  assert.equal(api.game.pendingUse, null);
});

test('Closing a Planet picker keeps the Planet for a later hand upgrade', () => {
  const { api } = harness();
  api.game.cons = [{ ...api.CONSUME_POOL.find(c => c.name === 'Planet: Train Hand') }];
  api.useConsumable(0);
  assert.equal(api.game.pendingUse.type, 'handType');
  api.hideHow();
  assert.equal(api.game.cons.length, 1);
  assert.equal(api.game.pendingUse, null);
  assert.equal(api.handUp.Pair.mult, 0);
});

test('Ready leaves the shop immediately instead of reopening it while waiting', async () => {
  const { api, elements } = harness();
  api.me.status = 'buy';
  api.enterBuyPhase(6);
  await api.markReady();
  assert.equal(api.game.mode, 'wait');
  assert.equal(api.me.status, 'ready');
  assert.equal(elements.get('shopBack').classList.contains('show'), false);
});

test('An exhausted deck under Exactly Five ends instead of leaving an impossible hand', async () => {
  const { api } = harness();
  api.game.deck = [];
  api.game.hand = [card(8, 'H'), card(8, 'D')];
  api.game.boss.exactFive = true;
  api.checkDeckExhausted();
  await flushPromises();
  assert.equal(api.me.eliminated, true);
  assert.equal(api.game.mode, 'end');
});

test('Discard cannot spend a resource or throw away cards when no replacements remain', () => {
  const { api } = harness();
  select(api, [card(8, 'H'), card(8, 'D')]);
  api.game.deck = [];
  const hand = api.game.hand.map(c => c.id);
  api.discardSelected();
  assert.equal(api.game.discardsLeft, 3);
  assert.equal(api.game.discard.length, 0);
  assert.deepEqual(api.game.hand.map(c => c.id), hand);
});
