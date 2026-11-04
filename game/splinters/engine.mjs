// Splinters is an entirely local game. No network, account, or room is required.
export const SYMBOLS = Object.freeze([
  { id: 'ember', name: 'Ember', color: '#cd6439' },
  { id: 'moon', name: 'Moon', color: '#7f91b7' },
  { id: 'moth', name: 'Moth', color: '#b59a62' },
  { id: 'thorn', name: 'Thorn', color: '#658467' },
  { id: 'key', name: 'Key', color: '#bb9346' },
  { id: 'eye', name: 'Eye', color: '#9277a5' },
  { id: 'mushroom', name: 'Mushroom', color: '#af7066' },
  { id: 'bone', name: 'Bone', color: '#aca38d' },
].map(Object.freeze));

const recipe = (id, name, cards, points, effect = 'points', target = 'none', description = '') =>
  Object.freeze({ id, name, cards: Object.freeze(cards.split(' ')), points, effect, target,
    description: description || `Gain ${points} points.` });

export const RECIPES = Object.freeze([
  recipe('hearthfire', 'Hearthfire', 'ember ember ember ember', 10),
  recipe('moonwell', 'Moonwell', 'moon moon moon moon', 10),
  recipe('velvet-wing', 'Velvet Wing', 'moth moth moth moth', 10),
  recipe('bramble-crown', 'Bramble Crown', 'thorn thorn thorn thorn', 10),
  recipe('golden-door', 'Golden Door', 'key key key key', 10),
  recipe('watchtower', 'Watchtower', 'eye eye eye eye', 10),
  recipe('fairy-ring', 'Fairy Ring', 'mushroom mushroom mushroom mushroom', 10),
  recipe('old-king', 'Old King', 'bone bone bone bone', 10),
  recipe('borrowed-key', 'Borrowed Key', 'key key key eye eye', 9, 'trade', 'card', 'Gain 9 points. Swap a chosen rival card with your leftmost surviving card.'),
  recipe('smoke-and-mirrors', 'Smoke & Mirrors', 'ember ember eye eye eye', 9, 'reveal', 'all', 'Gain 9 points. Reveal every center card for 7 seconds.'),
  recipe('silver-lock', 'Silver Lock', 'moon moon moon key key', 10, 'ward', 'none', 'Gain 10 points. A ward blocks the next hostile spell against you.'),
  recipe('dust-to-dust', 'Dust to Dust', 'moth moth bone bone bone', 9, 'scuttle', 'player', 'Gain 9 points. Scatter 3 random cards from a rival into the center. They draw replacements.'),
  recipe('fox-door', 'Fox Door', 'key key thorn thorn thorn', 9, 'trade', 'card', 'Gain 9 points. Swap a chosen rival card with your leftmost surviving card.'),
  recipe('dream-spores', 'Dream Spores', 'moon moon mushroom mushroom mushroom', 9, 'reveal', 'all', 'Gain 9 points. Reveal every center card for 7 seconds.'),
  recipe('candle-wake', 'Candle Wake', 'ember ember ember bone bone', 10, 'drain', 'player', 'Gain 10 points. A rival loses up to 5 points.'),
  recipe('watchful-wings', 'Watchful Wings', 'moth moth moth eye eye', 10, 'ward', 'none', 'Gain 10 points. A ward blocks the next hostile spell against you.'),
  recipe('cinderstorm', 'Cinderstorm', 'ember ember ember thorn thorn', 10, 'scuttle', 'player', 'Gain 10 points. Scatter 3 random rival cards into the center. They draw replacements.'),
  recipe('moon-eye', 'Moon Eye', 'moon moon moon eye eye', 10, 'reveal', 'all', 'Gain 10 points. Reveal every center card for 7 seconds.'),
  recipe('wind-riders', 'Wind Riders', 'moth moth moth moon moon', 10, 'gust', 'all', 'Gain 10 points. Each rival scatters one random card into the center and draws a replacement.'),
  recipe('thorn-sentinel', 'Thorn Sentinel', 'thorn thorn thorn bone bone', 11, 'ward', 'none', 'Gain 11 points. A ward blocks the next hostile spell against you.'),
  recipe('skeleton-key', 'Skeleton Key', 'key key key bone', 6, 'trade', 'card', 'Gain 6 points. Swap a chosen rival card with your leftmost surviving card.'),
  recipe('evil-eye', 'Evil Eye', 'eye eye eye ember', 7, 'drain', 'player', 'Gain 7 points. A rival loses up to 5 points.'),
  recipe('spore-supper', 'Spore Supper', 'mushroom mushroom mushroom moth', 8, 'feast', 'all', 'Gain 8 points. Every other player also gains 2 points.'),
  recipe('rattle-wind', 'Rattle Wind', 'bone bone bone key', 6, 'renew', 'all', 'Gain 6 points. All center cards return to the rafters; fresh cards float down.'),
  recipe('witching-hour', 'Witching Hour', 'moon moon moth eye bone', 14, 'reveal', 'all', 'Gain 14 points. Reveal every center card for 7 seconds.'),
  recipe('bonfire-feast', 'Bonfire Feast', 'ember ember mushroom mushroom key', 16),
  recipe('grave-robber', 'Grave Robber', 'bone bone key key eye', 11, 'trade', 'card', 'Gain 11 points. Swap a chosen rival card with your leftmost surviving card.'),
  recipe('wild-hunt', 'Wild Hunt', 'thorn thorn moth bone ember', 12, 'scuttle', 'player', 'Gain 12 points. Scatter 3 random rival cards into the center. They draw replacements.'),
  recipe('thousand-eyes', 'Thousand Eyes', 'eye eye moth moth moon', 13, 'reveal', 'all', 'Gain 13 points. Reveal every center card for 7 seconds.'),
  recipe('ashen-crown', 'Ashen Crown', 'ember ember ember ember bone', 13, 'gust', 'all', 'Gain 13 points. Each rival scatters one random card and draws a replacement.'),
  recipe('hollow-moon', 'Hollow Moon', 'moon moon moon bone eye', 13, 'drain', 'player', 'Gain 13 points. A rival loses up to 5 points.'),
  recipe('root-and-rune', 'Root & Rune', 'thorn thorn key key mushroom', 14, 'ward', 'none', 'Gain 14 points. A ward blocks the next hostile spell against you.'),
]);

export const DIFFICULTIES = Object.freeze([
  Object.freeze({ id: 'easy', name: 'Cozy', thinkMs: 10000, memory: 4, hints: 3, mistakeChance: 0.42, lookCount: 1, cycleChance: 0.75 }),
  Object.freeze({ id: 'normal', name: 'Cunning', thinkMs: 7000, memory: 9, hints: 1, mistakeChance: 0.12, lookCount: 1, cycleChance: 0.9 }),
  Object.freeze({ id: 'hard', name: 'Wicked', thinkMs: 4200, memory: 24, hints: 0, mistakeChance: 0, lookCount: 2, cycleChance: 1 }),
]);

export const HAND_SIZE = 7;
export const CENTER_SIZE = 12;
export const MAX_CENTER_SIZE = 24;
export const GOAL = 60;
export const MAX_TURNS = 80;

const result = (ok, message, events = [], extra = {}) => ({ ok, message, events, ...extra });
const random = state => Math.max(0, Math.min(0.999999999, Number((state._rng || Math.random)()) || 0));
const pick = (state, list) => list[Math.floor(random(state) * list.length)];
const playing = state => state?.status === 'playing';
const card = (state, type = pick(state, SYMBOLS).id) => ({ id: `c${state.nextCardId++}`, type });
function emit(state, type, fields = {}) {
  const event = { id: state.nextEventId++, type, ...fields };
  state.events.push(event);
  if (state.events.length > 120) state.events.splice(0, state.events.length - 120);
  return event;
}
function since(state, id) { return state.events.filter(event => event.id >= id); }
function counts(cards) {
  const total = {};
  for (const value of cards) { const type = typeof value === 'string' ? value : value.type; total[type] = (total[type] || 0) + 1; }
  return total;
}
function needed(hand, cards) {
  const have = counts(hand), want = counts(cards);
  return Object.entries(want).reduce((sum, [type, n]) => sum + Math.max(0, n - (have[type] || 0)), 0);
}
function refill(state) {
  for (let player = 0; player < state.players.length; player++) {
    const fresh = [];
    while (state.players[player].hand.length < HAND_SIZE) {
      const value = card(state); fresh.push(value); state.players[player].hand.push(value);
    }
    if (fresh.length) emit(state, 'fall', { player, cards: fresh });
  }
  const fresh = [];
  while (state.center.length < CENTER_SIZE) {
    const value = { ...card(state), faceUntil: 0 }; fresh.push(value); state.center.push(value);
  }
  if (fresh.length) emit(state, 'fall', { player: null, cards: fresh });
  if (state.center.length > MAX_CENTER_SIZE) {
    const retired = state.center.splice(0, state.center.length - MAX_CENTER_SIZE);
    emit(state, 'retire', { cards: retired });
  }
}
function checkFinish(state) {
  if (!playing(state)) return;
  if (state.players.some(player => player.score >= state.goal) || state.turn >= state.maxTurns) {
    const high = Math.max(...state.players.map(player => player.score));
    state.status = 'finished';
    state.winners = state.players.flatMap((player, i) => player.score === high ? [i] : []);
    state.pendingGrab = null;
    emit(state, 'finish', { winners: [...state.winners] });
  }
}
function score(state, player, amount) {
  const before = state.players[player].score;
  state.players[player].score = Math.max(0, before + amount);
  emit(state, 'score', { player, points: state.players[player].score - before });
}
function block(state, target) {
  if (!state.players[target].shield) return false;
  state.players[target].shield = false;
  emit(state, 'blocked', { player: target });
  return true;
}
function scatter(state, player, target, amount) {
  if (block(state, target)) return;
  const hand = state.players[target].hand, scattered = [];
  for (let i = 0; i < amount && hand.length; i++) {
    const value = hand.splice(Math.floor(random(state) * hand.length), 1)[0];
    scattered.push(value); state.center.push({ ...value, faceUntil: 0 });
  }
  emit(state, 'scuttle', { player, target, cards: scattered });
}

export function createGame(rng = Math.random, difficulty = 'normal') {
  const options = typeof rng === 'object' && rng ? rng : { rng, difficulty };
  const selectedDifficulty = DIFFICULTIES.find(value => value.id === options.difficulty) || DIFFICULTIES[1];
  const state = { players: ['You', 'Mossback', 'Madame Moth', 'Rook'].map((name, id) =>
    ({ id, name, hand: [], score: 0, shield: false })), center: [], pendingGrab: null,
    events: [], nextCardId: 1, nextEventId: 1, turn: 0, status: 'playing', winners: [],
    goal: options.goal || GOAL, maxTurns: options.maxTurns || MAX_TURNS,
    aiMemory: [{}, {}, {}, {}], aiMemoryOrder: [[], [], [], []],
    aiTurns: [0, 0, 0, 0], aiRecentDiscards: [[], [], [], []],
    difficulty: selectedDifficulty.id, botTurns: 0 };
  Object.defineProperty(state, '_rng', { value: typeof options.rng === 'function' ? options.rng : Math.random, writable: true });
  refill(state);
  // One discoverable near-match teaches searching without awarding a free opening spell.
  ['ember', 'ember', 'ember', 'moon', 'key', 'mushroom', 'bone'].forEach((type, i) => { state.players[0].hand[i].type = type; });
  state.center[0].type = 'ember';
  state.events = [];
  return state;
}

export function availableRecipes(state, playerIndex = 0) {
  const hand = state.players[playerIndex]?.hand;
  return hand ? RECIPES.filter(value => needed(hand, value.cards) === 0) : [];
}

export function pursueRecipes(state, playerIndex = 0, limit = 3) {
  const hand = state.players[playerIndex]?.hand;
  if (!hand) return [];
  return RECIPES.map(recipe => {
    const missing = [...recipe.cards], keep = [];
    for (const card of hand) {
      const i = missing.indexOf(card.type);
      if (i >= 0) { missing.splice(i, 1); keep.push(card.id); }
    }
    return { recipe, owned: recipe.cards.length - missing.length, distance: missing.length, missing, keep };
  }).filter(value => value.distance > 0 && value.distance <= 2)
    .sort((a, b) => a.distance - b.distance || b.recipe.points - a.recipe.points)
    .slice(0, Math.max(0, limit));
}

export function flipCard(state, centerId, now = Date.now()) {
  if (!playing(state)) return result(false, 'This table has finished. Start a new game.');
  if (state.pendingGrab) return result(false, 'Choose a card from your hand to return first.');
  const value = state.center.find(value => value.id === centerId);
  if (!value) return result(false, 'That card is no longer in the center.');
  value.faceUntil = value.faceUntil > now ? 0 : now + 4500;
  const event = emit(state, 'flip', { card: value.id, until: value.faceUntil });
  return result(true, value.faceUntil ? `${SYMBOLS.find(symbol => symbol.id === value.type).name} revealed. Remember its place.` : 'Card turned face down.', [event]);
}

export function grabCard(state, centerId) {
  if (!playing(state)) return result(false, 'This table has finished. Start a new game.');
  if (state.pendingGrab) return result(false, 'Return a hand card or cancel your grab first.');
  if (!state.center.some(value => value.id === centerId)) return result(false, 'That card is no longer in the center.');
  state.pendingGrab = { cardId: centerId };
  return result(true, 'Choose one of your hand cards to return to the center.');
}

export function cancelGrab(state) {
  if (!state.pendingGrab) return result(false, 'No card is being held.');
  state.pendingGrab = null;
  return result(true, 'Grab canceled.');
}

export function discardCard(state, handCardId) {
  if (!playing(state)) return result(false, 'This table has finished. Start a new game.');
  if (!state.pendingGrab) return result(false, 'Grab a center card first.');
  const hand = state.players[0].hand;
  const index = hand.findIndex(value => value.id === handCardId);
  const centerIndex = state.center.findIndex(value => value.id === state.pendingGrab.cardId);
  if (index < 0 || centerIndex < 0) return result(false, 'Choose a card in your hand.');
  const incoming = state.center[centerIndex], outgoing = hand[index];
  hand[index] = { id: incoming.id, type: incoming.type };
  state.center[centerIndex] = { ...outgoing, faceUntil: 0 };
  state.pendingGrab = null;
  const event = emit(state, 'swap', { player: 0, incoming: hand[index], outgoing });
  return result(true, 'One card taken. One card returned.', [event]);
}

export function lockRecipe(state, recipeId, target = {}, playerIndex = 0) {
  if (!playing(state)) return result(false, 'This table has finished. Start a new game.');
  if (state.pendingGrab) return result(false, 'Finish or cancel your card swap first.');
  const owner = state.players[playerIndex], value = RECIPES.find(value => value.id === recipeId);
  if (!owner || !value || needed(owner.hand, value.cards)) return result(false, 'That recipe is not complete in your hand.');
  if ((value.target === 'player' || value.target === 'card') &&
      (!Number.isInteger(target.player) || !state.players[target.player] || target.player === playerIndex)) {
    return result(false, 'Choose a rival for this spell.', [], { needsTarget: value.target });
  }
  if (value.target === 'card' && !state.players[target.player].hand.some(card => card.id === target.card)) {
    return result(false, 'Choose a card in that rival’s hand.', [], { needsTarget: 'card' });
  }
  const firstEvent = state.nextEventId, remaining = [...value.cards], burned = [];
  owner.hand = owner.hand.filter(card => {
    const i = remaining.indexOf(card.type);
    if (i < 0) return true;
    remaining.splice(i, 1); burned.push(card); return false;
  });
  emit(state, 'burn', { player: playerIndex, cards: burned, recipe: value.id });
  score(state, playerIndex, value.points);
  if (value.effect === 'scuttle') scatter(state, playerIndex, target.player, 3);
  if (value.effect === 'trade' && !block(state, target.player)) {
    const other = state.players[target.player].hand;
    const i = other.findIndex(card => card.id === target.card);
    const given = owner.hand[0], taken = other[i];
    owner.hand[0] = taken; other[i] = given;
    emit(state, 'trade', { player: playerIndex, target: target.player, given, taken });
  }
  if (value.effect === 'reveal') {
    const until = Date.now() + 7000;
    for (const card of state.center) card.faceUntil = until;
    emit(state, 'reveal', { until });
  }
  if (value.effect === 'ward') { owner.shield = true; emit(state, 'shield', { player: playerIndex }); }
  if (value.effect === 'gust') for (let i = 0; i < state.players.length; i++) if (i !== playerIndex) scatter(state, playerIndex, i, 1);
  if (value.effect === 'drain' && !block(state, target.player)) score(state, target.player, -5);
  if (value.effect === 'feast') for (let i = 0; i < state.players.length; i++) if (i !== playerIndex) score(state, i, 2);
  if (value.effect === 'renew') {
    const old = state.center.splice(0);
    emit(state, 'retire', { cards: old });
  }
  refill(state);
  checkFinish(state);
  return result(true, `${owner.name} burned ${value.name} for ${value.points} points.`, since(state, firstEvent), { recipe: value });
}

function handQuality(hand, difficulty) {
  // Favor getting close to a particular recipe rather than merely collecting duplicates.
  return Math.max(...RECIPES.map(value => (value.cards.length - needed(hand, value.cards)) / value.cards.length * 100 + value.points / (difficulty === 'hard' ? 5 : 100)));
}
function aiTurn(state, playerIndex, now) {
  const player = state.players[playerIndex], memory = state.aiMemory[playerIndex];
  const setting = DIFFICULTIES.find(value => value.id === state.difficulty) || DIFFICULTIES[1];
  const order = state.aiMemoryOrder[playerIndex];
  const ownTurn = ++state.aiTurns[playerIndex];
  const recent = state.aiRecentDiscards[playerIndex] = state.aiRecentDiscards[playerIndex].filter(value => value.until >= ownTurn);
  const newlySeen = new Set();
  const remember = value => {
    memory[value.id] = value.type;
    const old = order.indexOf(value.id); if (old >= 0) order.splice(old, 1);
    order.push(value.id);
  };
  // Rivals learn only cards that have actually been shown. Hidden card types are never evaluated.
  for (const value of state.center) if (value.faceUntil > now) remember(value);
  for (const id of Object.keys(memory)) if (!state.center.some(value => value.id === id)) delete memory[id];
  for (let i = order.length - 1; i >= 0; i--) if (!memory[order[i]]) order.splice(i, 1);
  let ready = availableRecipes(state, playerIndex);
  if (!ready.length) {
    for (let look = 0; look < setting.lookCount; look++) {
      const unknown = state.center.filter(value => !memory[value.id]);
      if (!unknown.length) break;
      const value = pick(state, unknown); value.faceUntil = now + 2600; remember(value);
      newlySeen.add(value.id);
      emit(state, 'flip', { card: value.id, until: value.faceUntil, player: playerIndex });
    }
    while (order.length > setting.memory) delete memory[order.shift()];
    const known = state.center.filter(value => memory[value.id]);
    let best = null, quality = handQuality(player.hand, state.difficulty);
    for (const value of known) for (let i = 0; i < player.hand.length; i++) {
      const candidate = [...player.hand]; candidate[i] = { id: value.id, type: memory[value.id] };
      const q = handQuality(candidate, state.difficulty);
      if (q > quality + 0.001) { best = { value, i }; quality = q; }
    }
    if (known.length && random(state) < setting.mistakeChance) best = { value: pick(state, known), i: Math.floor(random(state) * player.hand.length) };
    // Keep working through the table when no single exchange advances a recipe.
    // Rank only remembered cards; preserve the best recipe's core and avoid buying
    // back a card this creature discarded during its previous two turns.
    if (!best && known.length && random(state) < setting.cycleChance) {
      const plan = [...RECIPES].sort((a, b) =>
        ((b.cards.length - needed(player.hand, b.cards)) / b.cards.length * 100 + b.points / (state.difficulty === 'hard' ? 5 : 100)) -
        ((a.cards.length - needed(player.hand, a.cards)) / a.cards.length * 100 + a.points / (state.difficulty === 'hard' ? 5 : 100)))[0];
      const missing = [...plan.cards], keep = new Set();
      for (const card of player.hand) {
        const at = missing.indexOf(card.type);
        if (at >= 0) { missing.splice(at, 1); keep.add(card.id); }
      }
      const have = counts(player.hand), want = counts(plan.cards);
      let candidates = [];
      for (const value of known) for (let i = 0; i < player.hand.length; i++) {
        const outgoing = player.hand[i], incomingType = memory[value.id];
        if (state.difficulty === 'hard' && keep.has(outgoing.id)) continue;
        const hand = [...player.hand]; hand[i] = { id: value.id, type: incomingType };
        candidates.push({ value, i, rank: handQuality(hand, state.difficulty) * 1000 +
          (missing.includes(incomingType) ? 10 : 0) + (newlySeen.has(value.id) ? 2 : 0) +
          (!keep.has(outgoing.id) ? 1 : 0) + Math.max(0, have[outgoing.type] - (want[outgoing.type] || 0)) / 10 });
      }
      const forward = candidates.filter(candidate => !recent.some(value => value.id === candidate.value.id));
      if (forward.length) candidates = forward;
      const different = candidates.filter(candidate => memory[candidate.value.id] !== player.hand[candidate.i].type);
      if (different.length) candidates = different;
      best = candidates.sort((a, b) => b.rank - a.rank)[0] || null;
    }
    if (best) {
      const centerIndex = state.center.findIndex(value => value.id === best.value.id);
      const outgoing = player.hand[best.i], incoming = { id: best.value.id, type: memory[best.value.id] };
      player.hand[best.i] = incoming; state.center[centerIndex] = { ...outgoing, faceUntil: 0 };
      delete memory[incoming.id]; remember(outgoing);
      recent.push({ id: outgoing.id, until: ownTurn + 2 });
      while (order.length > setting.memory) delete memory[order.shift()];
      emit(state, 'swap', { player: playerIndex, incoming, outgoing });
    }
    ready = availableRecipes(state, playerIndex);
  }
  if (ready.length) {
    const utility = recipe => recipe.points + (recipe.effect === 'scuttle' ? 3 : recipe.effect === 'ward' && !player.shield ? 2 : recipe.effect === 'drain' ? 4 : 0);
    const best = state.difficulty === 'easy' ? pick(state, ready) : [...ready].sort((a, b) => utility(b) - utility(a))[0];
    const rivals = state.players.map((_, i) => i).filter(i => i !== playerIndex);
    const targetPlayer = state.difficulty === 'easy' ? pick(state, rivals) : [...rivals].sort((a, b) => state.players[b].score - state.players[a].score)[0];
    const target = { player: targetPlayer, card: pick(state, state.players[targetPlayer].hand).id };
    lockRecipe(state, best.id, target, playerIndex);
  }
}

export function botStep(state, playerIndex, now = Date.now()) {
  if (!playing(state)) return result(false, 'This table has finished. Start a new game.');
  if (state.pendingGrab) return result(false, 'Return one of your hand cards first.');
  if (!Number.isInteger(playerIndex) || playerIndex < 1 || playerIndex >= state.players.length) return result(false, 'Choose a creature opponent.');
  const firstEvent = state.nextEventId;
  aiTurn(state, playerIndex, now);
  state.botTurns++;
  state.turn = Math.floor(state.botTurns / 3);
  refill(state);
  checkFinish(state);
  return result(true, `${state.players[playerIndex].name} played.`, since(state, firstEvent));
}

export function advanceTurn(state) {
  if (!playing(state)) return result(false, 'This table has finished. Start a new game.');
  if (state.pendingGrab) return result(false, 'Return one of your hand cards first.');
  const firstEvent = state.nextEventId;
  for (let player = 1; player < state.players.length && playing(state); player++) botStep(state, player);
  return result(true, state.status === 'finished' ? 'The table has a winner.' : 'The creatures have played. Your move.', since(state, firstEvent));
}
