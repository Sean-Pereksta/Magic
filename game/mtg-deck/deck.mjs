/** Pure, versioned draft-deck model. A printing and its finish are never interchangeable prices. */
export const BUCKETS = Object.freeze(['0', '1', '2', '3', '4', '5', '6+']);
export const COLOR_NAMES = Object.freeze({ W: 'White', U: 'Blue', B: 'Black', R: 'Red', G: 'Green', C: 'Colorless' });
export const DEFAULT_PLAN = Object.freeze({ deckSize: 40, lands: 17, curve: Object.freeze([0, 2, 6, 6, 4, 3, 2]) });

const FINISHES = ['nonfoil', 'foil', 'etched'];
const COLORS = ['W', 'U', 'B', 'R', 'G'];
const MAX_QUANTITY = 9999;
const BAD_KEYS = new Set(['__proto__', 'prototype', 'constructor']);
const TEXT_FIELDS = [
  'name', 'layout', 'mana_cost', 'oracle_text', 'type_line', 'set', 'set_name', 'set_id', 'set_type',
  'set_uri', 'set_search_uri', 'scryfall_set_uri', 'collector_number', 'rarity', 'scryfall_uri', 'uri',
  'oracle_id', 'lang', 'power', 'toughness', 'loyalty', 'defense', 'flavor_text', 'flavor_name',
  'printed_name', 'printed_type_line', 'printed_text', 'artist', 'watermark', 'illustration_id',
  'card_back_id', 'released_at', 'prints_search_uri', 'rulings_uri', 'prices_checked_at', 'frame',
  'border_color', 'security_stamp',
];
const record = value => value !== null && typeof value === 'object' && !Array.isArray(value);
const colorCounts = () => ({ W: 0, U: 0, B: 0, R: 0, G: 0, C: 0 });

function numeric(value, fallback = 0) {
  if (typeof value !== 'number' && (typeof value !== 'string' || value.trim() === '')) return fallback;
  const number = Number(value);
  return Number.isFinite(number) ? number : fallback;
}

function integer(value, fallback = 0, minimum = 0, maximum = MAX_QUANTITY) {
  return Math.max(minimum, Math.min(maximum, Math.trunc(numeric(value, fallback))));
}

function safeCopy(value, depth = 0) {
  if (value === null || typeof value === 'string' || typeof value === 'boolean') return value;
  if (typeof value === 'number') return Number.isFinite(value) ? value : null;
  if (depth > 10 || typeof value !== 'object') return undefined;
  if (Array.isArray(value)) return value.map(item => safeCopy(item, depth + 1)).filter(item => item !== undefined);
  const result = {};
  for (const [key, item] of Object.entries(value)) {
    if (BAD_KEYS.has(key)) continue;
    const copied = safeCopy(item, depth + 1);
    if (copied !== undefined) result[key] = copied;
  }
  return result;
}

function normalizeCard(card) {
  if (!record(card) || typeof card.id !== 'string' || !card.id.trim() || BAD_KEYS.has(card.id)) return null;
  if (typeof card.name !== 'string' || !card.name.trim()) return null;
  const result = normalizeOptionalMetadata(safeCopy(card));
  result.id = card.id.trim().slice(0, 256);
  result.name = card.name.trim().slice(0, 500);
  return result;
}

function normalizeOptionalMetadata(card) {
  for (const key of TEXT_FIELDS) if (key in card && typeof card[key] !== 'string') delete card[key];
  for (const key of ['image_uris', 'purchase_uris', 'related_uris', 'legalities']) {
    if (!(key in card)) continue;
    if (!record(card[key])) delete card[key];
    else card[key] = Object.fromEntries(Object.entries(card[key]).filter(([, value]) => typeof value === 'string'));
  }
  for (const key of ['colors', 'color_identity', 'color_indicator', 'produced_mana', 'finishes', 'keywords', 'promo_types', 'frame_effects']) {
    if (!(key in card)) continue;
    if (!Array.isArray(card[key])) delete card[key];
    else card[key] = card[key].filter(value => typeof value === 'string');
  }
  if ('prices' in card) {
    if (!record(card.prices)) delete card.prices;
    else card.prices = Object.fromEntries(Object.entries(card.prices).filter(([, value]) =>
      value === null || typeof value === 'string' || (typeof value === 'number' && Number.isFinite(value))));
  }
  if ('card_faces' in card) {
    if (!Array.isArray(card.card_faces)) delete card.card_faces;
    else card.card_faces = card.card_faces.filter(record).map(normalizeOptionalMetadata);
  }
  return card;
}

function preserveNewerQuote(previous, incoming) {
  const previousTime = typeof previous?.prices_checked_at === 'string' ? Date.parse(previous.prices_checked_at) : NaN;
  const incomingTime = typeof incoming?.prices_checked_at === 'string' ? Date.parse(incoming.prices_checked_at) : NaN;
  if (Number.isFinite(previousTime) && (!Number.isFinite(incomingTime) || previousTime > incomingTime)) {
    return { ...incoming, prices: safeCopy(previous.prices) || {}, prices_checked_at: previous.prices_checked_at };
  }
  return incoming;
}

function normalizePlan(plan) {
  const input = record(plan) ? plan : {};
  return {
    deckSize: integer(input.deckSize, DEFAULT_PLAN.deckSize, 1, 1000),
    lands: integer(input.lands, DEFAULT_PLAN.lands, 0, 1000),
    // Keep mismatches visible: changing one goal never silently rewrites another.
    curve: BUCKETS.map((_, index) => integer(input.curve?.[index], DEFAULT_PLAN.curve[index], 0, 1000)),
  };
}

export function createDeck() {
  return { version: 2, name: 'My draft deck', plan: normalizePlan(DEFAULT_PLAN), entries: [] };
}

export function availableFinishes(card) {
  const declared = Array.isArray(card?.finishes) ? FINISHES.filter(finish => card.finishes.includes(finish)) : [];
  if (declared.length) return declared;
  const inferred = [];
  // Old saves did not store availability. A missing normal quote is not proof
  // that the card is foil-only, so keep normal available unless explicitly ruled out.
  if (card?.nonfoil !== false) inferred.push('nonfoil');
  if (card?.foil === true || (card?.foil !== false && card?.prices?.usd_foil != null)) inferred.push('foil');
  if (card?.prices?.usd_etched != null) inferred.push('etched');
  return inferred.length ? inferred : ['nonfoil'];
}

export function defaultFinish(card) {
  return availableFinishes(card)[0];
}

export function cardPrice(card, finish = defaultFinish(card)) {
  const field = { nonfoil: 'usd', foil: 'usd_foil', etched: 'usd_etched' }[finish];
  if (!field) return null;
  const price = numeric(card?.prices?.[field], NaN);
  return Number.isFinite(price) && price >= 0 ? price : null;
}

export function cardColors(card) {
  const source = Array.isArray(card?.colors) ? card.colors : card?.card_faces?.[0]?.colors;
  return COLORS.filter(color => Array.isArray(source) && source.includes(color));
}

export function isLand(card) {
  // Combined type lines such as "Sorcery // Land" must not turn the front into a land.
  const line = card?.card_faces?.[0]?.type_line || card?.type_line || '';
  const frontTypes = String(line).split('//')[0].split(/[—–]/)[0];
  return /\bLand\b/i.test(frontTypes);
}

export function entryIsLand(entry) {
  return entry?.role === 'land' || (entry?.role !== 'spell' && isLand(entry?.card));
}

function frontManaCost(card) {
  return String(card?.mana_cost ?? card?.card_faces?.[0]?.mana_cost ?? '');
}

export function manaValue(card) {
  const supplied = numeric(card?.cmc, NaN);
  if (Number.isFinite(supplied) && supplied >= 0) return supplied;
  const faceValue = numeric(card?.card_faces?.[0]?.cmc, NaN);
  if (Number.isFinite(faceValue) && faceValue >= 0) return faceValue;
  // Legacy/manual cards may have only a mana cost. X is zero outside the stack.
  return [...frontManaCost(card).matchAll(/\{([^}]+)\}/g)].reduce((sum, match) => {
    const symbol = match[1].toUpperCase();
    if (/^[XYZ]$/.test(symbol)) return sum;
    if (/^H[WUBRG]$/.test(symbol)) return sum + 0.5;
    const options = symbol.split('/').map(part => /^\d+$/.test(part) ? Number(part) : 1);
    return sum + Math.max(...options);
  }, 0);
}

export function curveIndex(card) {
  return Math.min(6, Math.max(0, Math.floor(manaValue(card))));
}

export function rulesText(card) {
  const faces = Array.isArray(card?.card_faces) ? card.card_faces.filter(record) : [];
  if (faces.some(face => typeof face.oracle_text === 'string' && face.oracle_text)) {
    return faces.map(face => [face.name, face.oracle_text].filter(value => typeof value === 'string' && value).join('\n')).filter(Boolean).join('\n\n//\n\n');
  }
  return typeof card?.oracle_text === 'string' ? card.oracle_text : '';
}

export function imageUrl(card, size = 'normal') {
  const direct = record(card?.image_uris) ? card.image_uris[size] : undefined;
  if (typeof direct === 'string' && direct) return direct;
  const faces = Array.isArray(card?.card_faces) ? card.card_faces : [];
  const face = faces.find(item => record(item?.image_uris) && typeof item.image_uris[size] === 'string' && item.image_uris[size]);
  return face ? face.image_uris[size] : '';
}

export function entryKey(cardId, finish = 'nonfoil', zone = 'main') {
  return `${cardId}:${finish}:${zone}`;
}

function normalizeEntry(raw) {
  if (!record(raw)) return null;
  const card = normalizeCard(raw.card);
  const qty = integer(raw.qty);
  if (!card || qty <= 0) return null;
  // A saved finish identifies the holding. A later card snapshot must never
  // silently change that identity, even when its availability metadata disagrees.
  const finish = FINISHES.includes(raw.finish) ? raw.finish : defaultFinish(card);
  const zone = raw.zone === 'sideboard' ? 'sideboard' : 'main';
  const role = raw.role === 'land' || raw.role === 'spell' ? raw.role : 'auto';
  return { key: entryKey(card.id, finish, zone), card, qty, finish, zone, role };
}

export function normalizeDeck(raw) {
  if (!record(raw)) return createDeck();
  const deck = {
    version: 2,
    name: typeof raw.name === 'string' && raw.name.trim() ? raw.name.trim().slice(0, 120) : 'My draft deck',
    plan: normalizePlan(raw.plan),
    entries: [],
  };
  const byKey = new Map();
  for (const rawEntry of Array.isArray(raw.entries) ? raw.entries : []) {
    const entry = normalizeEntry(rawEntry);
    if (!entry) continue;
    const previous = byKey.get(entry.key);
    if (previous) {
      previous.qty = integer(previous.qty + entry.qty);
      previous.card = preserveNewerQuote(previous.card, entry.card);
    } else {
      deck.entries.push(entry);
      byKey.set(entry.key, entry);
    }
  }
  return deck;
}

export function migrateLegacyDeck(legacy) {
  if (!record(legacy)) return createDeck();
  if (Array.isArray(legacy.entries)) return normalizeDeck(legacy);
  const entries = [];
  for (const [id, row] of Object.entries(legacy)) {
    if (BAD_KEYS.has(id) || !record(row) || !record(row.card)) continue;
    entries.push({ ...row, card: { ...row.card, id: row.card.id || id }, zone: 'main', role: 'auto' });
  }
  return normalizeDeck({ entries });
}

export function addCard(deck, card, { qty = 1, finish, zone = 'main' } = {}) {
  const result = normalizeDeck(deck);
  const chosenFinish = availableFinishes(card).includes(finish) ? finish : defaultFinish(card);
  const entry = normalizeEntry({ card, qty, finish: chosenFinish, zone });
  if (!entry) return result;
  for (const existing of result.entries) {
    if (existing.card.id === entry.card.id) entry.card = preserveNewerQuote(existing.card, entry.card);
  }
  const previous = result.entries.find(item => item.key === entry.key);
  if (previous) {
    previous.qty = integer(previous.qty + entry.qty);
    previous.card = entry.card;
  } else result.entries.push(entry);
  return result;
}

export function changeQuantity(deck, key, delta) {
  const result = normalizeDeck(deck);
  const entry = result.entries.find(item => item.key === key);
  if (entry) entry.qty = integer(entry.qty + Math.trunc(numeric(delta)));
  result.entries = result.entries.filter(item => item.qty > 0);
  return result;
}

export function moveCard(deck, key, zone) {
  const result = normalizeDeck(deck);
  if (zone !== 'main' && zone !== 'sideboard') return result;
  const entry = result.entries.find(item => item.key === key);
  if (!entry || entry.zone === zone) return result;
  const destinationKey = entryKey(entry.card.id, entry.finish, zone);
  const destination = result.entries.find(item => item.key === destinationKey);
  if (destination) {
    destination.qty = integer(destination.qty + entry.qty);
    result.entries = result.entries.filter(item => item.key !== key);
  } else {
    entry.zone = zone;
    entry.key = destinationKey;
  }
  return result;
}

export function setRole(deck, key, role) {
  const result = normalizeDeck(deck);
  if (!['auto', 'land', 'spell'].includes(role)) return result;
  const entry = result.entries.find(item => item.key === key);
  if (entry) entry.role = role;
  return result;
}

export function setPlan(deck, plan) {
  const result = normalizeDeck(deck);
  result.plan = normalizePlan({ ...result.plan, ...(record(plan) ? plan : {}) });
  return result;
}

function addPipDemand(demand, card, qty) {
  for (const match of frontManaCost(card).matchAll(/\{([^}]+)\}/g)) {
    const symbol = match[1].toUpperCase();
    if (/^H[WUBRG]$/.test(symbol)) {
      demand[symbol[1]] += qty / 2;
      continue;
    }
    const options = [...new Set(symbol.split('/'))];
    for (const option of options) if (Object.hasOwn(demand, option)) demand[option] += qty / options.length;
  }
}

export function summarizeDeck(deck, zone = 'main') {
  const normalized = normalizeDeck(deck);
  const summary = {
    total: 0, lands: 0, spells: 0, value: 0, pricedCopies: 0, unpricedCopies: 0, avgMana: 0,
    curve: BUCKETS.map(() => 0),
    curveTargetTotal: normalized.plan.curve.reduce((sum, count) => sum + count, 0),
    planTotal: normalized.plan.lands + normalized.plan.curve.reduce((sum, count) => sum + count, 0),
    colors: colorCounts(),
    composition: { ...colorCounts(), M: 0, L: 0 },
    pipDemand: colorCounts(),
  };
  let manaSum = 0;
  for (const entry of normalized.entries) {
    if (entry.zone !== zone) continue;
    const { card, qty, finish } = entry;
    summary.total += qty;
    const price = cardPrice(card, finish);
    if (price === null) summary.unpricedCopies += qty;
    else {
      summary.value += price * qty;
      summary.pricedCopies += qty;
    }
    if (entryIsLand(entry)) {
      summary.lands += qty;
      summary.composition.L += qty;
      continue;
    }
    summary.spells += qty;
    manaSum += manaValue(card) * qty;
    summary.curve[curveIndex(card)] += qty;
    const colors = cardColors(card);
    if (!colors.length) {
      summary.colors.C += qty;
      summary.composition.C += qty;
    } else {
      colors.forEach(color => { summary.colors[color] += qty; });
      summary.composition[colors.length > 1 ? 'M' : colors[0]] += qty;
    }
    addPipDemand(summary.pipDemand, card, qty);
  }
  summary.value = Math.round((summary.value + Number.EPSILON) * 100) / 100;
  summary.avgMana = summary.spells ? manaSum / summary.spells : 0;
  return summary;
}
