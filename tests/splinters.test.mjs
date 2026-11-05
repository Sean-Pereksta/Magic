import test from 'node:test';
import assert from 'node:assert/strict';
import { SYMBOLS, RECIPES, DIFFICULTIES, GAME_MODES, gameMode, progressFor, isFaceUp, createGame, availableRecipes, pursueRecipes, flipCard, grabCard, discardCard, cancelGrab, lockRecipe, botStep, advanceTurn, HAND_SIZE, CENTER_SIZE, MAX_CENTER_SIZE } from '../game/splinters/engine.mjs';

function seeded(seed = 12345) {
  return () => { seed = (Math.imul(seed, 1664525) + 1013904223) >>> 0; return seed / 4294967296; };
}
function dealRecipe(state, recipe, player = 0) {
  state.players[player].hand.forEach((card, i) => { card.type = recipe.cards[i] || (i % 2 ? 'eye' : 'key'); });
}
function allCards(state) { return [...state.center, ...state.players.flatMap(player => player.hand)]; }
function invariant(state) {
  assert.equal(state.players.length, 4);
  assert.ok(state.center.length >= CENTER_SIZE && state.center.length <= MAX_CENTER_SIZE);
  for (const player of state.players) {
    assert.equal(player.hand.length, HAND_SIZE);
    assert.ok(player.score >= 0);
  }
  const cards = allCards(state);
  assert.equal(new Set(cards.map(card => card.id)).size, cards.length, 'card ownership is unique');
  assert.ok(cards.every(card => SYMBOLS.some(symbol => symbol.id === card.type)));
}

test('compact symbols and 32 unique exact four/five-card recipes', () => {
  assert.equal(SYMBOLS.length, 8);
  assert.equal(RECIPES.length, 32);
  assert.equal(new Set(RECIPES.map(recipe => recipe.id)).size, RECIPES.length);
  assert.equal(new Set(RECIPES.map(recipe => [...recipe.cards].sort().join(','))).size, RECIPES.length);
  for (const recipe of RECIPES) {
    assert.ok([4, 5].includes(recipe.cards.length));
    assert.ok(recipe.cards.every(type => SYMBOLS.some(symbol => symbol.id === type)));
    assert.ok(['none', 'player', 'card', 'all'].includes(recipe.target));
  }
});

test('opening table offers a discoverable near-match but no free ready spell', () => {
  const state = createGame(seeded());
  invariant(state);
  assert.deepEqual(availableRecipes(state), []);
  const hint = pursueRecipes(state).find(value => value.recipe.id === 'hearthfire');
  assert.ok(hint);
  assert.equal(hint.distance, 1);
  assert.deepEqual(hint.missing, ['ember']);
  assert.equal(hint.keep.length, 3);
  assert.ok(state.center.every(card => card.faceUntil === 0));
});

test('flip is temporary and grab requires exactly one explicit hand return', () => {
  const state = createGame(seeded());
  const center = state.center[0], outgoing = state.players[0].hand[2];
  assert.equal(flipCard(state, center.id, 1000).ok, true);
  assert.equal(center.faceUntil, 5500);
  const before = state.players[0].hand.map(card => card.id);
  assert.equal(grabCard(state, center.id).ok, true);
  assert.deepEqual(state.players[0].hand.map(card => card.id), before);
  assert.equal(advanceTurn(state).ok, false, 'cannot skip a required return');
  assert.equal(lockRecipe(state, 'hearthfire').ok, false, 'cannot burn during a pending return');
  assert.equal(flipCard(state, state.center[1].id, 1200).ok, false);
  assert.equal(discardCard(state, 'missing').ok, false);
  assert.equal(discardCard(state, outgoing.id).ok, true);
  assert.equal(state.players[0].hand[2].id, center.id);
  assert.equal(state.center[0].id, outgoing.id);
  assert.equal(state.center[0].faceUntil, 0);
  assert.equal(state.pendingGrab, null);
  invariant(state);
});

test('canceling a grab leaves every card where it was', () => {
  const state = createGame(seeded()), before = JSON.stringify(allCards(state));
  grabCard(state, state.center[0].id);
  assert.equal(cancelGrab(state).ok, true);
  assert.equal(JSON.stringify(allCards(state)), before);
  assert.equal(discardCard(state, state.players[0].hand[0].id).ok, false);
});

test('burn consumes only the exact matching cards and falls replacements', () => {
  const state = createGame(seeded());
  dealRecipe(state, RECIPES.find(recipe => recipe.id === 'hearthfire'));
  const survivors = state.players[0].hand.slice(4).map(card => card.id);
  const cast = lockRecipe(state, 'hearthfire');
  assert.equal(cast.ok, true);
  assert.equal(state.players[0].score, 10);
  assert.equal(cast.events.find(event => event.type === 'burn').cards.length, 4);
  assert.equal(cast.events.find(event => event.type === 'fall' && event.player === 0).cards.length, 4);
  assert.ok(survivors.every(id => state.players[0].hand.some(card => card.id === id)));
  invariant(state);
});

test('target validation is atomic: no cards or points spent until rival and card chosen', () => {
  const state = createGame(seeded());
  const recipe = RECIPES.find(recipe => recipe.id === 'borrowed-key');
  dealRecipe(state, recipe);
  const before = JSON.stringify(state);
  for (const target of [{}, { player: 0 }, { player: 8 }, { player: 1 }, { player: 1, card: 'missing' }]) {
    assert.equal(lockRecipe(state, recipe.id, target).ok, false);
    assert.equal(JSON.stringify(state), before);
  }
  const given = state.players[0].hand[recipe.cards.length].id, taken = state.players[1].hand[2].id;
  const result = lockRecipe(state, recipe.id, { player: 1, card: taken });
  assert.equal(result.ok, true);
  assert.ok(state.players[0].hand.some(card => card.id === taken));
  assert.ok(state.players[1].hand.some(card => card.id === given));
  invariant(state);
});

test('scuttle sends three random cards to center and refills the rival', () => {
  const state = createGame(seeded());
  const recipe = RECIPES.find(recipe => recipe.id === 'dust-to-dust');
  dealRecipe(state, recipe);
  const oldIds = state.players[2].hand.map(card => card.id);
  const cast = lockRecipe(state, recipe.id, { player: 2 });
  const scattered = cast.events.find(event => event.type === 'scuttle').cards;
  assert.equal(scattered.length, 3);
  assert.ok(scattered.every(card => oldIds.includes(card.id)));
  assert.ok(scattered.every(card => state.center.some(center => center.id === card.id && center.faceUntil === 0)));
  assert.equal(state.players[2].hand.filter(card => oldIds.includes(card.id)).length, 4);
  invariant(state);
});

test('ward blocks exactly one hostile spell, then is consumed', () => {
  const state = createGame(seeded());
  state.players[1].shield = true;
  const recipe = RECIPES.find(recipe => recipe.id === 'dust-to-dust');
  dealRecipe(state, recipe);
  const oldIds = state.players[1].hand.map(card => card.id);
  const cast = lockRecipe(state, recipe.id, { player: 1 });
  assert.equal(state.players[1].shield, false);
  assert.deepEqual(state.players[1].hand.map(card => card.id), oldIds);
  assert.ok(cast.events.some(event => event.type === 'blocked'));
  assert.ok(!cast.events.some(event => event.type === 'scuttle'));
  invariant(state);
});

test('global reveal resolves immediately and expires after its window', () => {
  const state = createGame(seeded());
  const recipe = RECIPES.find(recipe => recipe.id === 'smoke-and-mirrors');
  dealRecipe(state, recipe);
  const now = Date.now();
  const cast = lockRecipe(state, recipe.id);
  assert.equal(cast.ok, true);
  assert.ok(state.center.every(card => card.faceUntil >= now + 7000 && card.faceUntil <= Date.now() + 7000));
  assert.ok(cast.events.some(event => event.type === 'reveal'));
});

test('every recipe resolves without lost cards, duplicates, or undersized hands', () => {
  for (const recipe of RECIPES) {
    const state = createGame(seeded(984));
    dealRecipe(state, recipe);
    const cast = lockRecipe(state, recipe.id, { player: 1, card: state.players[1].hand[0].id });
    assert.equal(cast.ok, true, recipe.name);
    invariant(state);
  }
});

test('AI never evaluates a hidden center type: same known cards yield the same decisions', () => {
  const a = createGame(seeded(456)), b = createGame(seeded(456));
  // Force all rivals to hold unmatchable-looking hands, with a single known center slot.
  for (const state of [a, b]) {
    state.players.slice(1).forEach(player => player.hand.forEach((card, i) => { card.type = SYMBOLS[i].id; }));
    state.center.forEach(card => { card.faceUntil = 0; });
    // Every hidden id is remembered as unknown. The AI's one newly flipped card is deterministic.
  }
  // The first unknown each AI flips can be observed; the rest must stay irrelevant.
  const initialCenter = a.center.map(card => ({ ...card }));
  advanceTurn(a);
  const exposed = new Set(a.events.filter(event => event.type === 'flip').map(event => event.card));
  b.center.forEach(card => { if (!exposed.has(card.id)) card.type = 'bone'; });
  advanceTurn(b);
  const actions = state => state.events.filter(event => ['flip', 'swap', 'burn'].includes(event.type)).map(({ until, ...event }) => event);
  assert.deepEqual(actions(a), actions(b));
  assert.equal(initialCenter.length, CENTER_SIZE);
});

test('repeated AI play has stable ownership, bounded center, and a finite ending', () => {
  for (let seed = 0; seed < 20; seed++) {
    const state = createGame(seeded(seed));
    while (state.status === 'playing') {
      const ready = availableRecipes(state);
      if (ready.length) lockRecipe(state, ready[0].id, { player: 1, card: state.players[1].hand[0].id });
      if (state.status === 'playing') advanceTurn(state);
      invariant(state);
      assert.ok(state.turn <= state.maxTurns);
    }
    assert.ok(state.winners.length >= 1);
    assert.equal(grabCard(state, state.center[0].id).ok, false);
    assert.equal(advanceTurn(state).ok, false);
  }
});

test('first to goal ends immediately; turn limit resolves equal scores as a tie', () => {
  const state = createGame({ rng: seeded(), goal: 10 });
  dealRecipe(state, RECIPES.find(recipe => recipe.id === 'hearthfire'));
  lockRecipe(state, 'hearthfire');
  assert.equal(state.status, 'finished');
  assert.deepEqual(state.winners, [0]);
  const tied = createGame({ rng: seeded(), maxTurns: 1 });
  tied.players.forEach(player => { player.score = 0; player.hand.forEach((card, i) => { card.type = SYMBOLS[i].id; }); });
  // No recipe is close enough to complete in one swap from seven distinct symbols.
  advanceTurn(tied);
  assert.equal(tied.status, 'finished');
  assert.ok(tied.winners.length >= 1);
  const high = Math.max(...tied.players.map(player => player.score));
  assert.ok(tied.winners.every(index => tied.players[index].score === high));
});

test('difficulty changes both pacing and knowledge, and forecasts use only the hand', () => {
  assert.deepEqual(DIFFICULTIES.map(value => value.thinkMs), [10000, 7000, 4200]);
  assert.deepEqual(DIFFICULTIES.map(value => value.cycleChance), [0.75, 0.9, 1]);
  assert.deepEqual(DIFFICULTIES.map(value => value.hints), [3, 1, 0]);
  assert.ok(DIFFICULTIES[0].memory < DIFFICULTIES[1].memory && DIFFICULTIES[1].memory < DIFFICULTIES[2].memory);
  for (const difficulty of DIFFICULTIES) {
    const state = createGame({ rng: seeded(9), difficulty: difficulty.id });
    const hints = pursueRecipes(state);
    state.center.forEach(card => { card.type = 'eye'; });
    assert.deepEqual(pursueRecipes(state), hints, 'forecast never inspects center symbols');
    for (let step = 0; step < 9; step++) botStep(state, 1, 1000 + step * difficulty.thinkMs);
    assert.ok(Object.keys(state.aiMemory[1]).length <= difficulty.memory);
  }
  const easy = createGame(seeded(17), 'easy'), hard = createGame(seeded(17), 'hard');
  // The harder creature looks at two actually exposed cards and plans from them.
  easy.players[1].hand.forEach((card, i) => { card.type = SYMBOLS[i].id; });
  hard.players[1].hand.forEach((card, i) => { card.type = SYMBOLS[i].id; });
  const easyResult = botStep(easy, 1, 10000), hardResult = botStep(hard, 1, 10000);
  assert.equal(easyResult.events.filter(event => event.type === 'flip').length, 1);
  assert.equal(hardResult.events.filter(event => event.type === 'flip').length, 2);
  assert.equal(easy.botTurns, 1);
});

test('stuck hard bots churn spare cards, keep their recipe core, and avoid immediately buying back a discard', () => {
  const makeStuckTable = () => {
    const state = createGame({ rng: seeded(715), difficulty: 'hard', goal: 10000 });
    ['ember', 'ember', 'ember', 'moon', 'key', 'thorn', 'bone'].forEach((type, i) => { state.players[1].hand[i].type = type; });
    state.center.forEach(card => { card.type = 'moth'; card.faceUntil = 0; });
    // This would make the old 40% fallback skip the exchange entirely.
    state._rng = () => 0.99;
    return state;
  };
  const a = makeStuckTable(), b = makeStuckTable();
  const core = a.players[1].hand.slice(0, 3).map(card => card.id);
  const first = botStep(a, 1, 10000);
  const swap = first.events.find(event => event.type === 'swap');
  assert.ok(swap, 'hard bot always cycles a known card when no strict improvement exists');
  assert.ok(!core.includes(swap.outgoing.id));
  assert.ok(core.every(id => a.players[1].hand.some(card => card.id === id)), 'three-Ember plan survives cycling');
  const shown = new Set(first.events.filter(event => event.type === 'flip').map(event => event.card));
  b.center.forEach(card => { if (!shown.has(card.id)) card.type = 'eye'; });
  const other = botStep(b, 1, 10000);
  assert.deepEqual(other.events, first.events, 'unseen card identities cannot influence the cycling choice');
  const second = botStep(a, 1, 15000).events.find(event => event.type === 'swap');
  assert.ok(second);
  assert.notEqual(second.incoming.id, swap.outgoing.id, 'a fresh remembered alternative beats undoing the previous exchange');
  assert.ok(core.every(id => a.players[1].hand.some(card => card.id === id)));
  invariant(a);
});

test('uniform random seven-card hands rarely begin with a completed exact recipe', () => {
  const rng = seeded(1939), state = createGame(rng);
  let ready = 0;
  for (let trial = 0; trial < 10000; trial++) {
    state.players[0].hand.forEach(card => { card.type = SYMBOLS[Math.floor(rng() * SYMBOLS.length)].id; });
    if (availableRecipes(state).length) ready++;
  }
  assert.ok(ready / 10000 < 0.30, `${ready / 100}% of random hands had a ready recipe`);
});

test('harder planning earns more points over equal move budgets across seeded tables', () => {
  const totals = {};
  for (const difficulty of DIFFICULTIES) {
    let total = 0;
    for (let seed = 0; seed < 8; seed++) {
      const state = createGame({ rng: seeded(seed + 399), difficulty: difficulty.id, goal: 10000, maxTurns: 500 });
      const now = Date.now();
      for (let move = 0; move < 12; move++) for (let player = 1; player < 4; player++) botStep(state, player, now + move * 15000 + player * 100);
      total += state.players.slice(1).reduce((sum, player) => sum + player.score, 0);
    }
    totals[difficulty.id] = total;
  }
  assert.ok(totals.hard > totals.normal, JSON.stringify(totals));
  assert.ok(totals.normal > totals.easy, JSON.stringify(totals));
});

test('game modes have separate goals and pacing without changing selected difficulty', () => {
  assert.deepEqual(GAME_MODES.map(mode => [mode.id, mode.goal, mode.goalKind, mode.botPace, mode.openTable]), [
    ['classic', 60, 'points', 1, false], ['quickfire', 30, 'points', 0.75, false],
    ['lantern', 60, 'points', 1, true], ['hunt', 5, 'recipes', 1, false],
  ]);
  assert.equal(gameMode(createGame()).id, 'classic');
  assert.equal(gameMode(createGame({ mode: 'unknown' })).id, 'classic');
  for (const mode of GAME_MODES) {
    const state = createGame({ rng: seeded(), mode: mode.id, difficulty: 'hard' });
    assert.equal(state.mode, mode.id);
    assert.equal(state.goal, mode.goal);
    assert.equal(state.difficulty, 'hard');
    assert.equal(progressFor(state, 0), 0);
    assert.deepEqual(state.players.map(player => player.discoveries), [[], [], [], []]);
  }
});

test('each point-race mode ends at its own goal, while Hunt requires five different valid burns', () => {
  for (const mode of GAME_MODES.filter(mode => mode.goalKind === 'points')) {
    const state = createGame({ rng: seeded(), mode: mode.id });
    state.players[0].score = mode.goal - 10;
    dealRecipe(state, RECIPES[0]);
    assert.equal(lockRecipe(state, RECIPES[0].id).ok, true);
    assert.equal(progressFor(state), mode.goal);
    assert.equal(state.status, 'finished');
    assert.deepEqual(state.winners, [0]);
  }
  const hunt = createGame({ rng: seeded(), mode: 'hunt' });
  for (let i = 0; i < 5; i++) {
    dealRecipe(hunt, RECIPES[i]); lockRecipe(hunt, RECIPES[i].id);
    assert.equal(progressFor(hunt), i + 1);
    if (i === 0) {
      dealRecipe(hunt, RECIPES[0]); lockRecipe(hunt, RECIPES[0].id);
      assert.equal(progressFor(hunt), 1, 'repeat recipes do not advance the hunt');
      assert.equal(hunt.players[0].score, 20, 'repeat recipes still award their points');
    }
    assert.equal(hunt.status, i === 4 ? 'finished' : 'playing');
  }
  assert.deepEqual(hunt.winners, [0]);
});

test('target validation remains atomic in every mode, including Hunt discovery progress', () => {
  for (const mode of GAME_MODES) {
    const state = createGame({ rng: seeded(), mode: mode.id });
    const recipe = RECIPES.find(recipe => recipe.id === 'borrowed-key');
    dealRecipe(state, recipe);
    const before = JSON.stringify(state);
    assert.equal(lockRecipe(state, recipe.id, { player: 1, card: 'missing' }).ok, false);
    assert.equal(JSON.stringify(state), before);
    assert.equal(progressFor(state), 0);
    assert.equal(lockRecipe(state, recipe.id, { player: 1, card: state.players[1].hand[0].id }).ok, true);
    assert.deepEqual(state.players[0].discoveries, [recipe.id]);
  }
});

test('Lanternlight keeps every center card face up through flips, swaps, scatter, renewal, and bot play', () => {
  const state = createGame({ rng: seeded(), mode: 'lantern', difficulty: 'easy' });
  const visible = () => assert.ok(state.center.every(card => card.faceUntil === Infinity && isFaceUp(state, card, Number.MAX_SAFE_INTEGER)));
  visible();
  const value = state.center[0];
  assert.equal(flipCard(state, value.id, 10000).ok, true);
  visible();
  grabCard(state, value.id); discardCard(state, state.players[0].hand[0].id); visible();
  for (const id of ['dust-to-dust', 'rattle-wind', 'smoke-and-mirrors']) {
    dealRecipe(state, RECIPES.find(recipe => recipe.id === id));
    assert.equal(lockRecipe(state, id, { player: 1 }).ok, true); visible(); invariant(state);
  }
  botStep(state, 1, 100000); visible(); invariant(state);
  assert.ok(Object.keys(state.aiMemory[1]).length >= CENTER_SIZE - 1, 'even patient rivals can see the open table instead of forgetting printed symbols');
  const classic = createGame(seeded());
  assert.equal(isFaceUp(classic, classic.center[0], 10000), false);
  flipCard(classic, classic.center[0].id, 10000);
  assert.equal(isFaceUp(classic, classic.center[0], 11000), true);
  assert.equal(isFaceUp(classic, classic.center[0], 15000), false);
});

test('all modes finish at the round limit; Hunt ties use discoveries and ignore unequal point totals', () => {
  for (const mode of GAME_MODES) {
    const state = createGame({ rng: seeded(), mode: mode.id, goal: 10000, maxTurns: 1 });
    state.botTurns = 2;
    state.players[0].score = 99;
    if (mode.id === 'hunt') {
      state.players[0].discoveries = RECIPES.slice(0, 2).map(recipe => recipe.id);
      state.players[1].discoveries = [...state.players[0].discoveries];
    }
    botStep(state, 3, 10000);
    assert.equal(state.status, 'finished');
    assert.deepEqual(state.winners, mode.id === 'hunt' ? [0, 1] : [0]);
    assert.equal(botStep(state, 3).ok, false);
  }
});

test('Hunt forecasts, recipe choices, and hostile targets prefer discovery progress', () => {
  const state = createGame({ rng: seeded(), mode: 'hunt', difficulty: 'hard', goal: 100 });
  state.players[0].discoveries = ['hearthfire'];
  assert.ok(!pursueRecipes(state, 0, 32).some(hint => hint.recipe.id === 'hearthfire'));
  // Both Ashen Crown and Hearthfire are ready; the novel lower-value recipe wins.
  dealRecipe(state, RECIPES.find(recipe => recipe.id === 'ashen-crown'), 1);
  state.players[1].discoveries = ['ashen-crown'];
  const cast = botStep(state, 1, 10000).events.find(event => event.type === 'burn');
  assert.equal(cast.recipe, 'hearthfire');
  const attack = createGame({ rng: seeded(), mode: 'hunt', difficulty: 'hard', goal: 100 });
  dealRecipe(attack, RECIPES.find(recipe => recipe.id === 'dust-to-dust'), 1);
  attack.players[0].score = 100;
  attack.players[2].discoveries = ['hearthfire', 'moonwell'];
  const scatter = botStep(attack, 1, 10000).events.find(event => event.type === 'scuttle');
  assert.equal(scatter.target, 2, 'Hunt leader is determined by discoveries rather than embers');
});
