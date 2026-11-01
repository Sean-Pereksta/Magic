import {
  BUCKETS, COLOR_NAMES, DEFAULT_PLAN, createDeck, normalizeDeck, migrateLegacyDeck,
  addCard, changeQuantity, moveCard, setRole, setPlan, cardPrice, availableFinishes,
  defaultFinish, cardColors, isLand, entryIsLand, manaValue, rulesText, imageUrl,
  summarizeDeck, entryKey,
} from './deck.mjs';
import { createScryfallClient } from './scryfall.mjs';

const $ = id => document.getElementById(id);
const esc = value => String(value ?? '').replace(/[&<>"']/g, ch => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[ch]));
const money = value => value === null ? 'Unpriced' : Number(value).toLocaleString(undefined, { style: 'currency', currency: 'USD' });
const percent = (count, total) => (total ? 100 * count / total : 0).toLocaleString(undefined, { maximumFractionDigits: 1 }) + '%';
const finishNames = { nonfoil: 'Nonfoil', foil: 'Foil', etched: 'Etched' };
const colorNames = { ...COLOR_NAMES, M: 'Multicolor', L: 'Lands' };
const basics = { W: 'Plains', U: 'Island', B: 'Swamp', R: 'Mountain', G: 'Forest' };
const STORAGE_KEY = 'mtg.draft.v2';
let storageWritable = true;
let storageMessage = '';
let lastSavedRaw = null;
let toastTimer;

function validatedBackup(raw) {
  if (raw?.version !== 2 || !Array.isArray(raw.entries) || !raw.plan || typeof raw.plan !== 'object') throw new Error('Choose a valid Draft Table JSON backup.');
  if (raw.entries.some(row => typeof row?.card?.id !== 'string' || !row.card.id.trim() ||
    typeof row.card.name !== 'string' || !row.card.name.trim() ||
    !Number.isInteger(row.qty) || row.qty <= 0 || row.qty > 9999 ||
    !['nonfoil', 'foil', 'etched'].includes(row.finish) || !['main', 'sideboard'].includes(row.zone))) {
    throw new Error('This backup contains invalid card entries. Your existing deck has not changed.');
  }
  const deck = normalizeDeck(raw);
  if (deck.entries.reduce((sum, row) => sum + row.qty, 0) !== raw.entries.reduce((sum, row) => sum + row.qty, 0)) {
    throw new Error('This backup could not be restored without losing cards. Your existing deck has not changed.');
  }
  return deck;
}

function loadSavedDeck() {
  try {
    const saved = localStorage.getItem(STORAGE_KEY);
    lastSavedRaw = saved;
    if (saved !== null) {
      const parsed = JSON.parse(saved);
      return validatedBackup(parsed);
    }
    const legacy = localStorage.getItem('mtg.deck');
    if (legacy !== null) {
      const parsed = JSON.parse(legacy);
      if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) throw new Error('Unreadable older deck.');
      if (Object.entries(parsed).some(([id, row]) => !row?.card || typeof row.card.name !== 'string' ||
        !row.card.name.trim() || (row.card.id != null && typeof row.card.id !== 'string') ||
        !id.trim() || !Number.isInteger(row.qty) || row.qty < 1 || row.qty > 9999)) throw new Error('Unreadable older deck entries.');
      const migrated = migrateLegacyDeck(parsed);
      if (migrated.entries.length) storageMessage = 'Your existing deck has been brought into Draft Table. The original save is still kept.';
      return migrated;
    }
  } catch {
    // Never overwrite an unreadable save or claim a blocked write succeeded.
    storageWritable = false;
    storageMessage = 'Your saved deck could not be read, or browser storage is unavailable. The original save has been kept. This session stays in memory; use Deck tools → Download backup to keep your changes.';
  }
  return createDeck();
}

const state = { deck: loadSavedDeck(), undo: [], zone: 'main', tab: 'deck', importing: false };
const client = createScryfallClient();
const searchStates = Object.fromEntries(['quick', 'catalog', 'prints'].map(kind => [kind, {
  cards: [], shown: 0, total: 0, next: null, busy: false, token: 0, controller: null, options: null, finishes: new Map(), error: '',
}]));
const cardRegistry = new Map();

function toast(message) {
  clearTimeout(toastTimer);
  $('toast').textContent = message;
  $('toast').hidden = false;
  toastTimer = setTimeout(() => { $('toast').hidden = true; }, 4500);
}

function save() {
  if (storageWritable) {
    try {
      if (localStorage.getItem(STORAGE_KEY) !== lastSavedRaw) {
        storageWritable = false;
        storageMessage = 'Another tab saved a newer deck. This tab now keeps changes in memory to protect that save. Download a backup before reloading to keep both versions.';
        throw new Error('Conflicting save');
      }
      const raw = JSON.stringify(state.deck);
      localStorage.setItem(STORAGE_KEY, raw);
      lastSavedRaw = raw;
      $('saveState').textContent = 'Saved on this device';
      return true;
    } catch {
      if (storageWritable) storageMessage = 'Browser storage is full or unavailable. Your changes are still here, but could not be saved. Download a backup from Deck tools before leaving.';
    }
  }
  $('saveState').textContent = 'Not saved · download a backup';
  $('storageWarning').textContent = storageMessage;
  $('storageWarning').hidden = false;
  return false;
}

function commit(next, message = '', recordUndo = true) {
  if (recordUndo) {
    state.undo.push(state.deck);
    if (state.undo.length > 30) state.undo.shift();
  }
  state.deck = normalizeDeck(next);
  save();
  renderDeck();
  if (message) toast(message);
}

function safeImage(source) {
  try {
    const url = new URL(source);
    if (url.protocol === 'https:' && (url.hostname === 'cards.scryfall.io' || url.hostname.endsWith('.scryfall.com'))) return url.href;
  } catch { /* Untrusted saved/imported artwork is never used as a URL. */ }
  return '';
}

function safeCardLink(source) {
  try {
    const url = new URL(source);
    if (url.protocol === 'https:' && url.hostname === 'scryfall.com') return url.href;
  } catch { /* No external link when the card has no valid provider URL. */ }
  return '';
}

function manaMarkup(cost) {
  return '<span class="mana" aria-label="' + esc(cost || 'No mana cost') + '">' +
    (String(cost || '').match(/\{[^}]+\}/g) || []).map(token => {
      const symbol = token.slice(1, -1);
      const color = /^[WUBRG]$/.test(symbol) ? symbol : 'C';
      return '<span class="mana-symbol" data-color="' + color + '">' + esc(symbol) + '</span>';
    }).join('') + '</span>';
}

function manaCost(card) {
  return card.mana_cost || card.card_faces?.[0]?.mana_cost || '';
}

function colorMarkup(card) {
  const colors = cardColors(card);
  return (colors.length ? colors : ['C']).map(color =>
    '<span class="color-dot" data-color="' + color + '"></span>' + esc(COLOR_NAMES[color])
  ).join(' · ');
}

function printingText(card, finish) {
  if (card.id.startsWith('basic:')) return 'Basic land placeholder · unpriced';
  const set = typeof card.set === 'string' ? card.set.toUpperCase() : '';
  const setName = typeof card.set_name === 'string' ? card.set_name : '';
  const collector = typeof card.collector_number === 'string' || typeof card.collector_number === 'number' ? String(card.collector_number) : '';
  return (setName || set || 'Saved printing') +
    (set ? ' (' + set + ')' : '') +
    (collector ? ' #' + collector : '') +
    (finish ? ' · ' + finishNames[finish] : '');
}

function finishOptions(card, selected) {
  // A saved finish is retained even if later availability metadata disagrees.
  const choices = [...new Set([...availableFinishes(card), ...(selected ? [selected] : [])])];
  return choices.map(finish => '<option value="' + finish + '"' + (selected === finish ? ' selected' : '') + '>' + finishNames[finish] + '</option>').join('');
}

function renderDeck() {
  const t = summarizeDeck(state.deck);
  const side = summarizeDeck(state.deck, 'sideboard');
  const plan = state.deck.plan;
  if (document.activeElement !== $('deckName')) $('deckName').value = state.deck.name;
  $('tabDeckCount').textContent = t.total;
  $('mainZoneCount').textContent = t.total;
  $('sideZoneCount').textContent = side.total;
  $('deckCount').innerHTML = t.total + ' <small>/ ' + plan.deckSize + '</small>';
  $('spellCount').innerHTML = t.spells + ' <small>/ ' + t.curveTargetTotal + '</small>';
  $('landCount').innerHTML = t.lands + ' <small>/ ' + plan.lands + '</small>';
  $('spellPercent').textContent = percent(t.spells, t.total) + ' of current deck';
  $('landPercent').textContent = percent(t.lands, t.total) + ' of current deck';
  $('deckValue').textContent = t.total && !t.pricedCopies ? 'Unpriced' : money(t.value);
  $('priceCoverage').textContent = !t.total ? 'No cards added' : t.pricedCopies + ' of ' + t.total + ' cards priced' + (t.unpricedCopies ? ' · ' + t.unpricedCopies + ' unpriced' : '');
  $('deckProgress').style.width = Math.min(100, t.total / plan.deckSize * 100) + '%';
  const remaining = plan.deckSize - t.total;
  $('deckProgressCopy').textContent = remaining > 0 ? remaining + ' cards to go' : remaining < 0 ? Math.abs(remaining) + ' over your goal' : 'Goal reached';
  $('deckProgressCopy').className = remaining === 0 ? 'good' : remaining < 0 ? 'over' : '';
  $('deckGuidance').textContent = !t.total ? 'Start with your draft picks. Add basic lands when you are ready.' :
    remaining < 0 ? 'Choose ' + Math.abs(remaining) + ' card' + (remaining === -1 ? '' : 's') + ' to cut to reach ' + plan.deckSize + '. Move cuts to the sideboard to keep them handy.' :
    remaining === 0 ? 'Your deck has ' + plan.deckSize + ' cards. Check the curve and colors below before you play.' :
    'Building toward ' + plan.deckSize + ' cards · ' + Math.max(0, t.curveTargetTotal - t.spells) + ' spells and ' + Math.max(0, plan.lands - t.lands) + ' lands below your targets.';
  $('undoBtn').disabled = state.undo.length === 0;
  $('mainZone').setAttribute('aria-pressed', state.zone === 'main');
  $('sideZone').setAttribute('aria-pressed', state.zone === 'sideboard');
  $('cardsTitle').textContent = state.zone === 'main' ? 'Cards in your deck' : 'Your sideboard';
  $('listSummary').textContent = state.zone === 'main' ? 'Main deck only counts toward your goal.' :
    side.total + ' cards · ' + (side.pricedCopies ? money(side.value) + ' known value' : 'no priced cards') + (side.unpricedCopies ? ' · ' + side.unpricedCopies + ' unpriced' : '') + '. Excluded from your deck totals.';
  renderCurve(t);
  renderColors(t);
  renderDeckList();
  renderLandButtons();
  renderPriceDate();
}

function renderCurve(t) {
  const plan = state.deck.plan;
  document.querySelector('.curve-panel .eyebrow').textContent = 'Your ' + plan.deckSize + '-card plan';
  const max = Math.max(1, ...t.curve, ...plan.curve);
  $('averageMana').textContent = 'Average: ' + (t.spells ? t.avgMana.toFixed(2) : '—');
  $('curveChart').innerHTML = BUCKETS.map((bucket, i) =>
    '<div class="curve-column"><div class="bar-space"><i class="bar-goal" style="height:' + (plan.curve[i] / max * 94) + '%"></i><i class="bar-current ' + (t.curve[i] > plan.curve[i] ? 'over-goal' : '') + '" style="height:' + (t.curve[i] / max * 94) + '%"></i></div><span class="bar-caption">' + bucket + '</span></div>'
  ).join('');
  $('curveChart').setAttribute('aria-label', BUCKETS.map((bucket, i) => 'Mana ' + bucket + ': ' + t.curve[i] + ' of ' + plan.curve[i] + ' goal').join('. '));
  const rows = BUCKETS.map((bucket, i) => [bucket, t.curve[i], plan.curve[i]]);
  rows.push(['Lands', t.lands, plan.lands]);
  $('curveRows').innerHTML = rows.map(([label, count, goal]) => {
    const diff = goal - count;
    return '<tr' + (label === 'Lands' ? ' class="land-plan"' : '') + '><td>' + label + '</td><td><strong>' + count + '</strong><span class="goal-number"> / ' + goal + '</span></td>' +
      '<td title="Current / target, each divided by your ' + plan.deckSize + '-card goal">' + percent(count, plan.deckSize) + '<span class="goal-number"> / ' + percent(goal, plan.deckSize) + '</span></td><td class="' + (diff > 0 ? 'need' : diff < 0 ? 'over' : 'good') + '">' +
      (diff > 0 ? 'Need ' + diff : diff < 0 ? Math.abs(diff) + ' over' : goal === 0 ? '—' : 'On target') + '</td></tr>';
  }).join('');
  $('planWarning').hidden = t.planTotal === plan.deckSize;
  $('planWarning').textContent = 'Your targets total ' + t.planTotal + ' cards (' + t.curveTargetTotal + ' spells + ' + plan.lands + ' lands), but your deck-size goal is ' + plan.deckSize + '. Edit targets to make them match.';
}

function colorLine(color, count, total) {
  return '<div class="color-line"><span class="color-label"><i class="color-dot" style="background:var(--' + color + ')"></i>' + colorNames[color] + '</span><span class="color-count">' + count + ' card' + (count === 1 ? '' : 's') + '</span><span class="color-percent">' + percent(count, total) + '</span></div>';
}

function renderColors(t) {
  const groups = ['W', 'U', 'B', 'R', 'G', 'M', 'C', 'L'].filter(color => t.composition[color]);
  $('compositionBar').innerHTML = groups.map(color => '<i style="background:var(--' + color + ');width:' + (t.composition[color] / t.total * 100) + '%"></i>').join('');
  $('colorBreakdown').innerHTML = groups.length ? groups.map(color => colorLine(color, t.composition[color], t.total)).join('') : '<p class="muted small">Add cards to see your color breakdown.</p>';
  const spellGroups = Object.keys(COLOR_NAMES).filter(color => t.colors[color]);
  $('spellColors').innerHTML = spellGroups.length ? spellGroups.map(color => colorLine(color, t.colors[color], t.spells)).join('') : '<p class="muted small">No spells yet.</p>';
}

function renderLandButtons() {
  const t = summarizeDeck(state.deck);
  $('landGoalCopy').textContent = t.lands + ' of ' + state.deck.plan.lands + ' lands in your main deck';
  $('basicLands').innerHTML = Object.entries(basics).map(([color, name]) =>
    '<button data-basic="' + color + '" aria-label="Add ' + name + ' basic land"><span class="mana-symbol" data-color="' + color + '">' + color + '</span><span>+ ' + name + '</span></button>'
  ).join('');
}

function renderPriceDate() {
  const entries = state.deck.entries.filter(entry => !entry.card.id.startsWith('basic:'));
  const checked = entries.map(entry => Date.parse(entry.card.prices_checked_at)).filter(Number.isFinite);
  $('refreshPrices').disabled = !entries.length || refreshing;
  if (!entries.length) $('priceDate').textContent = 'Price estimates · selected printing & finish';
  else if (checked.length !== entries.length) $('priceDate').textContent = 'Some saved prices have no check date. Refresh for current estimates.';
  else {
    const oldest = new Date(Math.min(...checked));
    $('priceDate').textContent = 'Prices checked ' + oldest.toLocaleString(undefined, { month: 'short', day: 'numeric', hour: 'numeric', minute: '2-digit' }) + ' or later. Provider prices update periodically.';
  }
}

function renderDeckList() {
  const root = $('deckList');
  const focused = root.contains(document.activeElement) ? { key: document.activeElement.closest('[data-entry-key]')?.dataset.entryKey, action: document.activeElement.dataset.deckAction } : null;
  const sort = $('deckSort').value;
  const entries = state.deck.entries.filter(entry => entry.zone === state.zone);
  const sortValue = entry => cardPrice(entry.card, entry.finish);
  entries.sort((a, b) => {
    if (sort === 'name') return a.card.name.localeCompare(b.card.name);
    if (sort === 'value') return (sortValue(b) === null ? -1 : sortValue(b) * b.qty) - (sortValue(a) === null ? -1 : sortValue(a) * a.qty) || a.card.name.localeCompare(b.card.name);
    if (sort === 'color') return (entryIsLand(a) ? 'Z' : cardColors(a.card).join('')).localeCompare(entryIsLand(b) ? 'Z' : cardColors(b.card).join('')) || manaValue(a.card) - manaValue(b.card);
    return Number(entryIsLand(a)) - Number(entryIsLand(b)) || manaValue(a.card) - manaValue(b.card) || a.card.name.localeCompare(b.card.name);
  });
  if (!entries.length) {
    root.innerHTML = '<div class="deck-empty"><span class="empty-mark" aria-hidden="true">✦</span><h3>' + (state.zone === 'main' ? 'A great deck starts with a pick.' : 'Keep your cuts within reach.') + '</h3><p>' +
      (state.zone === 'main' ? 'Search for a card above, check its details, and tap Add. Your plan updates as you build.' : 'Use “To sideboard” on a deck card. It stays saved without counting toward your 40-card goal.') + '</p>' +
      (state.zone === 'main' ? '<button class="quiet" data-focus-search>Find my first card</button>' : '') + '</div>';
  } else {
    let group = '';
    root.innerHTML = entries.map(entry => {
      const { card, qty, finish, key } = entry;
      cardRegistry.set(card.id, card);
      const price = cardPrice(card, finish);
      let heading = '';
      const nextGroup = entryIsLand(entry) ? 'Lands' : 'Mana value ' + (manaValue(card) >= 6 ? '6+' : manaValue(card));
      if (sort === 'mana' && nextGroup !== group) { group = nextGroup; heading = '<h3 class="group-heading">' + esc(group) + '</h3>'; }
      const modalLand = card.layout === 'modal_dfc' && card.card_faces?.some(face => /\bLand\b/.test(face.type_line || ''));
      return heading + '<article class="deck-card" data-entry-key="' + esc(key) + '"><div><h3 class="deck-card-title"><button class="text-button deck-card-name" data-deck-action="details">' + esc(card.name) + '</button>' + manaMarkup(manaCost(card)) + '</h3>' +
        '<p class="result-meta">' + esc(printingText(card, finish)) + '</p><p class="result-meta">' + esc(card.type_line || '') + (entry.role !== 'auto' ? ' · Planned as ' + (entry.role === 'land' ? 'a land' : 'a spell') : '') + '</p></div>' +
        '<div class="deck-card-value"><span class="price">' + (price === null ? 'Unpriced' : money(price * qty)) + '</span><small>' + (price === null ? 'No quote for this finish' : money(price) + ' each × ' + qty) + '</small></div>' +
        '<div class="deck-card-actions"><div class="quantity-control"><button data-deck-action="minus" aria-label="Remove one ' + esc(card.name) + '">−</button><output aria-label="Copies of ' + esc(card.name) + '">' + qty + '</output><button data-deck-action="plus" aria-label="Add one ' + esc(card.name) + '">+</button></div>' +
        '<button data-deck-action="move">' + (state.zone === 'main' ? 'To sideboard' : 'To deck') + '</button><button data-deck-action="remove" class="danger" aria-label="Remove all ' + qty + ' copies of ' + esc(card.name) + '">Remove</button>' +
        (availableFinishes(card).length > 1 ? '<label>Finish<select data-deck-action="finish" aria-label="Finish of ' + esc(card.name) + '">' + finishOptions(card, finish) + '</select></label>' : '') +
        (modalLand ? '<label>Count as<select data-deck-action="role" aria-label="Deck role of ' + esc(card.name) + '">' + ['auto', 'spell', 'land'].map(role => '<option value="' + role + '"' + (entry.role === role ? ' selected' : '') + '>' + ({ auto: 'Front face', spell: 'Spell', land: 'Land' }[role]) + '</option>').join('') + '</select></label>' : '') +
        '</div></article>';
    }).join('');
  }
  if (focused?.action) {
    const replacement = [...root.querySelectorAll('[data-entry-key]')].find(row => row.dataset.entryKey === focused.key)?.querySelector('[data-deck-action="' + focused.action + '"]');
    (replacement || $(state.zone === 'main' ? 'mainZone' : 'sideZone')).focus({ preventScroll: true });
  }
}

function addBasic(color) {
  const qty = Number($('landAddQty').value);
  if (!Number.isInteger(qty) || qty < 1 || qty > 99) { $('landAddQty').reportValidity(); return; }
  const name = basics[color];
  const card = { id: 'basic:' + color, name, type_line: 'Basic Land — ' + name, cmc: 0, colors: [], produced_mana: [color], finishes: ['nonfoil'], prices: {}, oracle_text: '{T}: Add {' + color + '}.' };
  state.zone = 'main';
  commit(addCard(state.deck, card, { qty }), 'Added ' + qty + ' ' + name + ' to your deck.');
}

function searchOptions(kind) {
  if (kind === 'quick') return { query: $('quickQuery').value.trim(), set: $('quickSet').value.trim(), mode: 'name', sort: 'name', unique: 'prints' };
  if (kind === 'prints') return searchStates.prints.options;
  return { query: $('catalogQuery').value.trim(), mode: $('catalogMode').value, set: $('catalogSet').value.trim(), color: $('catalogColor').value,
    type: $('catalogType').value, rarity: $('catalogRarity').value, mana: $('catalogMana').value, price: $('catalogPrice').value, sort: $('catalogSort').value, unique: $('catalogUnique').value };
}

function invalidateSearch(kind, message) {
  const s = searchStates[kind];
  s.controller?.abort();
  s.token++;
  s.busy = false;
  s.cards = [];
  s.shown = 0;
  s.next = null;
  s.error = '';
  $(kind + 'Results').innerHTML = '';
  $(kind + 'More').hidden = true;
  $(kind + 'Status').classList.remove('error');
  $(kind + 'Status').textContent = message;
}

async function runSearch(kind) {
  const s = searchStates[kind];
  const options = searchOptions(kind);
  if (kind === 'quick' && !options.query) { invalidateSearch(kind, 'Enter a card name, then press Search.'); $('quickQuery').focus(); return; }
  invalidateSearch(kind, 'Searching cards…');
  s.options = options;
  s.finishes.clear();
  s.busy = true;
  s.controller = new AbortController();
  const token = s.token;
  $(kind + 'Results').setAttribute('aria-busy', 'true');
  try {
    const response = await client.search(options, { signal: s.controller.signal });
    if (token !== s.token) return;
    s.cards = response.data.filter(card => card?.id && card?.name);
    if (kind === 'quick') {
      const exact = options.query.toLocaleLowerCase();
      s.cards.sort((a, b) => Number(b.name.toLocaleLowerCase() === exact) - Number(a.name.toLocaleLowerCase() === exact));
    }
    s.total = response.total_cards;
    s.next = response.next_page;
    s.shown = Math.min(kind === 'catalog' ? 18 : 8, s.cards.length);
    s.busy = false;
    s.error = '';
    renderResults(kind);
  } catch (error) {
    if (token !== s.token || error.name === 'AbortError') return;
    s.error = error.message;
    s.busy = false;
    renderSearchStatus(kind);
  } finally {
    if (token === s.token) { s.busy = false; $(kind + 'Results').setAttribute('aria-busy', 'false'); }
  }
}

async function moreResults(kind) {
  const s = searchStates[kind];
  if (s.busy) return;
  const step = kind === 'catalog' ? 18 : 8;
  if (s.shown < s.cards.length) { s.shown = Math.min(s.cards.length, s.shown + step); renderResults(kind); return; }
  if (!s.next) return;
  s.busy = true;
  const token = s.token;
  $(kind + 'More').disabled = true;
  $(kind + 'More').textContent = 'Loading…';
  try {
    const response = await client.nextPage(s.next, { signal: s.controller.signal });
    if (token !== s.token) return;
    const ids = new Set(s.cards.map(card => card.id));
    s.cards.push(...response.data.filter(card => card?.id && card?.name && !ids.has(card.id)));
    s.next = response.next_page;
    s.shown = Math.min(s.cards.length, s.shown + step);
    s.error = '';
    renderResults(kind);
  } catch (error) {
    if (token !== s.token || error.name === 'AbortError') return;
    s.error = error.message;
    renderSearchStatus(kind);
  } finally {
    if (token === s.token) {
      s.busy = false;
      $(kind + 'More').disabled = false;
      $(kind + 'More').textContent = 'Show more ' + (kind === 'prints' ? 'printings' : 'cards');
    }
  }
}

function renderSearchStatus(kind) {
  const s = searchStates[kind];
  const status = $(kind + 'Status');
  status.classList.toggle('error', Boolean(s.error));
  if (s.error) {
    status.textContent = s.error + ' Your deck is unchanged. ';
    const retry = document.createElement('button');
    retry.className = 'quiet';
    retry.textContent = 'Try again';
    retry.onclick = () => s.cards.length && s.next ? moreResults(kind) : runSearch(kind);
    status.append(retry);
  } else if (!s.cards.length) status.textContent = 'No matching cards. Try a shorter name, a different set code, or fewer filters.';
  else status.textContent = 'Showing ' + s.shown.toLocaleString() + ' of ' + s.total.toLocaleString() + ' matching ' + (s.options?.unique === 'prints' ? 'printings' : 'cards') +
    (kind === 'catalog' && /^value-/.test(s.options.sort) ? ' · Sorted by available nonfoil USD prices; unpriced cards are excluded.' : ' · Tap a card name for full details.');
}

function renderResults(kind) {
  const s = searchStates[kind];
  $(kind + 'Results').innerHTML = s.cards.slice(0, s.shown).map(card => {
    cardRegistry.set(card.id, card);
    const finish = s.finishes.get(card.id) || defaultFinish(card);
    const price = cardPrice(card, finish);
    const art = safeImage(imageUrl(card, 'small'));
    return '<article class="search-card" data-card-id="' + esc(card.id) + '"><button class="card-thumb-button" data-result-action="details" aria-label="View ' + esc(card.name) + '">' +
      (art ? '<img class="card-thumb" src="' + esc(art) + '" alt="' + esc(card.name) + '" loading="lazy" decoding="async">' : '<span class="card-thumb image-placeholder">Card details</span>') + '</button>' +
      '<div><div class="result-title"><h3><button class="text-button" data-result-action="details">' + esc(card.name) + '</button></h3>' + manaMarkup(manaCost(card)) + '</div>' +
      '<p class="result-meta">' + esc(card.type_line || 'Unknown type') + ' · MV ' + manaValue(card) + '</p><p class="result-meta">' + esc(printingText(card)) + (card.rarity ? ' · ' + esc(card.rarity) : '') + '</p>' +
      '<div class="result-colors">' + colorMarkup(card) + '</div><p class="rules-preview">' + esc(rulesText(card) || 'No rules text.') + '</p>' +
      (card.power != null ? '<p class="result-meta">Power / toughness: ' + esc(card.power) + ' / ' + esc(card.toughness) + '</p>' : '') + '</div>' +
      '<div class="result-bottom"><div class="result-price"><span class="price">' + money(price) + '</span><small>' + (price === null ? 'No USD quote' : 'Estimated USD · each') + '</small></div>' +
      '<label class="finish-label">Finish<select data-result-action="finish" aria-label="Finish of ' + esc(card.name) + '">' + finishOptions(card, finish) + '</select></label>' +
      '<div class="result-actions">' + (kind === 'prints' ? '' : '<button class="quiet" data-result-action="prints">Printings</button>') + '<button class="primary" data-result-action="add" aria-label="Add ' + esc(card.name) + ' to deck">+ Add</button></div></div></article>';
  }).join('');
  const more = $(kind + 'More');
  more.hidden = s.shown >= s.cards.length && !s.next;
  more.disabled = false;
  more.textContent = 'Show more ' + (kind === 'prints' ? 'printings' : 'cards');
  renderSearchStatus(kind);
}

function searchResultAction(event, kind) {
  const control = event.target.closest('[data-result-action]');
  if (!control) return;
  const article = control.closest('[data-card-id]');
  const card = cardRegistry.get(article.dataset.cardId);
  if (!card) return;
  const finish = article.querySelector('select[data-result-action="finish"]').value;
  const action = control.dataset.resultAction;
  if (event.type === 'change' && action === 'finish') {
    searchStates[kind].finishes.set(card.id, finish);
    const price = cardPrice(card, finish);
    article.querySelector('.price').textContent = money(price);
    article.querySelector('.result-price small').textContent = price === null ? 'No USD quote' : 'Estimated USD · each';
  } else if (event.type === 'click' && action === 'add') {
    state.zone = 'main';
    commit(addCard(state.deck, card, { finish }), 'Added ' + card.name + ' (' + finishNames[finish] + ') to your deck.');
  } else if (event.type === 'click' && action === 'details') openCard(card, finish);
  else if (event.type === 'click' && action === 'prints') openPrintings(card);
}

function openCard(card, finish = defaultFinish(card)) {
  $('cardDialogTitle').textContent = card.name;
  const validFaces = Array.isArray(card.card_faces) ? card.card_faces.filter(face => face && typeof face === 'object' && !Array.isArray(face)) : [];
  const faces = validFaces.length ? validFaces : [card];
  const artUrls = [...new Set([imageUrl(card), ...faces.map(face => imageUrl(face))].map(safeImage).filter(Boolean))];
  const link = safeCardLink(card.scryfall_uri);
  $('cardDetails').innerHTML = '<div class="card-detail-layout"><div class="face-arts">' + artUrls.map(url => '<img class="large-art" src="' + esc(url) + '" alt="' + esc(card.name) + '" loading="lazy">').join('') + '</div><div class="card-detail-copy">' +
    '<p class="small muted">' + esc(printingText(card, finish)) + '</p><div class="result-colors">' + colorMarkup(card) + ' · Mana value ' + manaValue(card) + '</div>' +
    '<div class="detail-prices">' + availableFinishes(card).map(f => '<span>' + finishNames[f] + ': <b class="price">' + money(cardPrice(card, f)) + '</b></span>').join('') + '</div>' +
    '<div>' + faces.map(face => '<section class="detail-face"><h3>' + esc(face.name || card.name) + '</h3>' + manaMarkup(face.mana_cost) + '<p class="result-meta">' + esc(face.type_line || '') + '</p><p class="oracle">' + esc(face.oracle_text || '') + '</p>' + (face.power != null ? '<p class="small">Power / toughness: ' + esc(face.power) + ' / ' + esc(face.toughness) + '</p>' : '') + '</section>').join('') + '</div>' +
    (link ? '<a href="' + esc(link) + '" target="_blank" rel="noopener noreferrer" class="small">Open this printing on Scryfall ↗</a>' : '') +
    '<p class="small muted">Values are estimates for each finish, not guaranteed sale prices.</p></div></div>';
  $('cardDialog').showModal();
}

function openPrintings(card) {
  searchStates.prints.options = { query: '!' + JSON.stringify(card.name), mode: 'advanced', unique: 'prints', sort: 'name' };
  $('printsTitle').textContent = card.name + ' · printings';
  $('printsDialog').showModal();
  runSearch('prints');
}

function setTab(tab, focus = false) {
  state.tab = tab;
  for (const name of ['deck', 'catalog']) {
    $(name + 'Panel').hidden = name !== tab;
    $(name + 'Tab').setAttribute('aria-selected', name === tab);
    $(name + 'Tab').tabIndex = name === tab ? 0 : -1;
  }
  if (focus) $(tab + 'Tab').focus();
  if (tab === 'catalog' && !searchStates.catalog.options) runSearch('catalog');
}

function fillPlanForm(plan) {
  $('planSize').value = plan.deckSize;
  $('planLands').value = plan.lands;
  $('planInputs').innerHTML = BUCKETS.map((bucket, i) => '<label>' + bucket + ' mana<input type="number" id="planCurve' + i + '" min="0" max="250" step="1" value="' + plan.curve[i] + '" required><span class="target-share" id="planShare' + i + '"></span></label>').join('');
  updatePlanPreview();
}

function planFromForm() {
  return { deckSize: Number($('planSize').value), lands: Number($('planLands').value), curve: BUCKETS.map((_, i) => Number($('planCurve' + i).value)) };
}

function updatePlanPreview() {
  const plan = planFromForm();
  const spells = plan.curve.reduce((a, b) => a + b, 0);
  const total = spells + plan.lands;
  BUCKETS.forEach((_, i) => { $('planShare' + i).textContent = percent(plan.curve[i], plan.deckSize) + ' of deck'; });
  $('planLiveTotal').textContent = spells + ' spells + ' + plan.lands + ' lands = ' + total + ' / ' + plan.deckSize + ' cards' + (total === plan.deckSize ? ' · Targets match' : ' · ' + Math.abs(total - plan.deckSize) + (total > plan.deckSize ? ' over' : ' short'));
  $('planLiveTotal').className = 'notice' + (total === plan.deckSize ? '' : ' warning');
}

let refreshing = false;
async function refreshPrices() {
  if (refreshing) return;
  const ids = [...new Set(state.deck.entries.map(entry => entry.card.id).filter(id => !id.startsWith('basic:')))];
  if (!ids.length) { toast('Add a card printing to refresh its price.'); return; }
  refreshing = true;
  $('refreshPrices').disabled = true;
  $('refreshPrices').textContent = 'Refreshing…';
  try {
    const response = await client.collection(ids);
    const byId = new Map(response.data.map(card => [card.id, card]));
    // Apply to the current deck, not the snapshot at request start. Removed cards stay removed.
    const next = { ...state.deck, entries: state.deck.entries.map(entry => {
      const fresh = byId.get(entry.card.id);
      return fresh ? { ...entry, card: { ...entry.card, prices: fresh.prices || {}, prices_checked_at: fresh.prices_checked_at } } : entry;
    }) };
    commit(next, 'Refreshed ' + byId.size + ' printing' + (byId.size === 1 ? '' : 's') + (response.not_found.length ? '. ' + response.not_found.length + ' could not be refreshed; their saved prices were kept.' : '.'), false);
  } catch (error) {
    toast(error.message + ' Saved prices were kept.');
  } finally {
    refreshing = false;
    $('refreshPrices').textContent = 'Refresh prices';
    renderPriceDate();
  }
}

function download(name, content, type) {
  const url = URL.createObjectURL(new Blob([content], { type }));
  const anchor = document.createElement('a');
  anchor.href = url;
  anchor.download = name;
  document.body.append(anchor);
  anchor.click();
  anchor.remove();
  setTimeout(() => URL.revokeObjectURL(url), 1000);
}

function exportList() {
  const lines = [];
  for (const zone of ['main', 'sideboard']) {
    const entries = state.deck.entries.filter(entry => entry.zone === zone).sort((a, b) => a.card.name.localeCompare(b.card.name));
    if (zone === 'sideboard' && !entries.length) continue;
    lines.push(zone === 'main' ? 'Deck' : '\nSideboard', ...entries.map(entry => entry.qty + ' ' + entry.card.name));
  }
  download('mtg-draft-deck.txt', lines.join('\n'), 'text/plain');
  toast('Card list exported. Use Download backup to preserve printings, finishes, prices, and targets.');
}

let importController;
async function importCards(event) {
  event.preventDefault();
  if (state.importing) return;
  $('importStatus').textContent = '';
  const file = $('importFile').files[0];
  if (file) {
    try {
      if (file.size > 5 * 1024 * 1024) throw new Error('That backup is too large. Choose a Draft Table backup under 5 MB.');
      const raw = JSON.parse(await file.text());
      commit(validatedBackup(raw), 'Backup restored. Undo will bring back your previous deck.');
      $('importDialog').close();
      $('importFile').value = '';
    } catch (error) { $('importStatus').textContent = error.message; }
    return;
  }
  const raw = $('importText').value.trim();
  if (!raw) { $('importStatus').textContent = 'Paste a card list or choose a backup file first.'; return; }
  let zone = 'main';
  const requests = [];
  for (const line of raw.split(/\r?\n/).map(value => value.trim()).filter(Boolean)) {
    if (/^(deck|main deck|maindeck):?$/i.test(line)) { zone = 'main'; continue; }
    if (/^sideboard:?$/i.test(line)) { zone = 'sideboard'; continue; }
    const match = line.match(/^(\d+)\s*x?\s+(.+)$/i);
    if (!match || Number(match[1]) < 1 || Number(match[1]) > 9999) { $('importStatus').textContent = 'Use a quantity and card name on each line, e.g. 2 Lightning Bolt. Check: ' + line; return; }
    requests.push({ qty: Number(match[1]), name: match[2].trim(), zone, line });
  }
  if (!requests.length || requests.length > 100) { $('importStatus').textContent = 'Import between 1 and 100 card-name lines at a time.'; return; }
  state.importing = true;
  importController = new AbortController();
  $('importSubmit').disabled = true;
  const found = [];
  const failed = [];
  try {
    for (let i = 0; i < requests.length; i++) {
      const request = requests[i];
      $('importStatus').textContent = 'Finding ' + (i + 1) + ' of ' + requests.length + ': ' + request.name;
      try {
        const card = await client.named(request.name, { signal: importController.signal });
        found.push({ ...request, card });
      } catch (error) {
        if (error.name === 'AbortError') throw error;
        failed.push({ ...request, error: error.message });
        if (error.status !== 404) {
          failed.push(...requests.slice(i + 1).map(request => ({ ...request, error: 'Not attempted after the request error.' })));
          break;
        }
      }
    }
    if (importController.signal.aborted) return;
    if (found.length) {
      let next = state.deck;
      for (const entry of found) next = addCard(next, entry.card, { qty: entry.qty, zone: entry.zone });
      commit(next, 'Imported ' + found.reduce((sum, entry) => sum + entry.qty, 0) + ' cards. Check printings and finishes for accurate values.');
    }
    if (failed.length) {
      // Keep only unsuccessful lines, including their zones, so retry cannot duplicate successes.
      $('importText').value = ['main', 'sideboard'].flatMap(zone => {
        const rows = failed.filter(row => row.zone === zone);
        return rows.length ? [zone === 'main' ? 'Deck' : 'Sideboard', ...rows.map(row => row.line)] : [];
      }).join('\n');
      $('importStatus').textContent = found.length + ' card-name lines imported. ' + failed.length + ' remain in the box. ' + failed[0].name + ': ' + failed[0].error;
    } else {
      $('importText').value = '';
      $('importDialog').close();
    }
  } catch (error) {
    if (error.name !== 'AbortError') $('importStatus').textContent = error.message;
  } finally {
    state.importing = false;
    $('importSubmit').disabled = false;
  }
}

$('deckTab').addEventListener('click', () => setTab('deck'));
$('catalogTab').addEventListener('click', () => setTab('catalog'));
$('backToDeck').addEventListener('click', () => setTab('deck', true));
document.querySelector('.workspace-tabs').addEventListener('keydown', event => {
  if (!['ArrowLeft', 'ArrowRight', 'Home', 'End'].includes(event.key)) return;
  event.preventDefault();
  setTab(event.key === 'Home' ? 'deck' : event.key === 'End' ? 'catalog' : state.tab === 'deck' ? 'catalog' : 'deck', true);
});
$('deckName').addEventListener('change', event => commit({ ...state.deck, name: event.target.value || 'My draft deck' }));
$('undoBtn').addEventListener('click', () => {
  const previous = state.undo.pop();
  if (previous) commit(previous, 'Last deck change undone.', false);
});
for (const zone of ['main', 'side']) $(zone + 'Zone').addEventListener('click', () => { state.zone = zone === 'main' ? 'main' : 'sideboard'; renderDeck(); });
$('deckSort').addEventListener('change', renderDeckList);
$('deckList').addEventListener('click', event => {
  if (event.target.closest('[data-focus-search]')) { $('quickQuery').focus(); $('quickQuery').scrollIntoView({ block: 'center', behavior: 'smooth' }); return; }
  const button = event.target.closest('button[data-deck-action]');
  if (!button) return;
  const entry = state.deck.entries.find(row => row.key === button.closest('[data-entry-key]')?.dataset.entryKey);
  if (!entry) return;
  const action = button.dataset.deckAction;
  if (action === 'details') openCard(entry.card, entry.finish);
  else if (action === 'plus' || action === 'minus') commit(changeQuantity(state.deck, entry.key, action === 'plus' ? 1 : -1));
  else if (action === 'remove') commit(changeQuantity(state.deck, entry.key, -entry.qty), 'Removed ' + entry.card.name + '. Undo is available.');
  else if (action === 'move') commit(moveCard(state.deck, entry.key, entry.zone === 'main' ? 'sideboard' : 'main'), 'Moved ' + entry.card.name + ' to ' + (entry.zone === 'main' ? 'your sideboard.' : 'your deck.'));
});
$('deckList').addEventListener('change', event => {
  const control = event.target.closest('select[data-deck-action]');
  if (!control) return;
  const entry = state.deck.entries.find(row => row.key === control.closest('[data-entry-key]')?.dataset.entryKey);
  if (!entry) return;
  if (control.dataset.deckAction === 'role') commit(setRole(state.deck, entry.key, control.value));
  else {
    let next = changeQuantity(state.deck, entry.key, -entry.qty);
    next = addCard(next, entry.card, { qty: entry.qty, finish: control.value, zone: entry.zone });
    next = setRole(next, entryKey(entry.card.id, control.value, entry.zone), entry.role);
    commit(next, 'Changed finish to ' + finishNames[control.value] + '.');
  }
});
$('basicLands').addEventListener('click', event => { const button = event.target.closest('[data-basic]'); if (button) addBasic(button.dataset.basic); });
for (const kind of ['quick', 'catalog', 'prints']) {
  $(kind + 'Results').addEventListener('click', event => searchResultAction(event, kind));
  $(kind + 'Results').addEventListener('change', event => searchResultAction(event, kind));
  $(kind + 'More').addEventListener('click', () => moreResults(kind));
}
$('quickForm').addEventListener('submit', event => { event.preventDefault(); runSearch('quick'); });
$('catalogForm').addEventListener('submit', event => { event.preventDefault(); runSearch('catalog'); });
for (const id of ['quickQuery', 'quickSet']) $(id).addEventListener('input', () => invalidateSearch('quick', 'Press Search to find this card.'));
for (const id of ['catalogQuery', 'catalogSet']) $(id).addEventListener('input', () => invalidateSearch('catalog', 'Press Search to apply your search.'));
for (const id of ['catalogMode', 'catalogColor', 'catalogMana', 'catalogPrice', 'catalogSort', 'catalogType', 'catalogRarity', 'catalogUnique']) $(id).addEventListener('change', () => runSearch('catalog'));
document.querySelectorAll('[data-preset]').forEach(button => button.addEventListener('click', () => {
  $('catalogForm').reset();
  if (button.dataset.preset === 'budget') { $('catalogPrice').value = 'under1'; $('catalogSort').value = 'value-asc'; }
  if (button.dataset.preset === 'valuable') { $('catalogPrice').value = '20plus'; $('catalogSort').value = 'value-desc'; $('catalogUnique').value = 'prints'; }
  if (button.dataset.preset === 'reset') $('catalogSort').value = 'name';
  runSearch('catalog');
}));
$('editPlan').addEventListener('click', () => { fillPlanForm(state.deck.plan); $('planDialog').showModal(); });
$('planForm').addEventListener('input', updatePlanPreview);
$('resetPlan').addEventListener('click', () => fillPlanForm(DEFAULT_PLAN));
$('planForm').addEventListener('submit', event => {
  event.preventDefault();
  if (!$('planForm').reportValidity()) return;
  commit(setPlan(state.deck, planFromForm()), 'Your draft targets have been updated.');
  $('planDialog').close();
});
$('refreshPrices').addEventListener('click', refreshPrices);
$('exportBtn').addEventListener('click', exportList);
$('backupBtn').addEventListener('click', () => download('mtg-draft-backup.json', JSON.stringify(state.deck, null, 2), 'application/json'));
$('importBtn').addEventListener('click', () => { $('importDialog').showModal(); $('importStatus').textContent = ''; });
$('importForm').addEventListener('submit', importCards);
$('clearBtn').addEventListener('click', () => {
  if (state.deck.entries.length && confirm('Remove every card from the deck and sideboard? Your targets stay the same. You can Undo this.')) commit({ ...state.deck, entries: [] }, 'Deck cleared. Undo is available.');
});
document.querySelectorAll('.close-dialog').forEach(button => button.addEventListener('click', () => button.closest('dialog').close()));
$('printsDialog').addEventListener('close', () => searchStates.prints.controller?.abort());
$('importDialog').addEventListener('close', () => importController?.abort());
document.querySelectorAll('.tools-content button').forEach(button => button.addEventListener('click', () => { document.querySelector('.tools-menu').open = false; }));
document.addEventListener('error', event => {
  const img = event.target;
  if (!(img instanceof HTMLImageElement)) return;
  const fallback = document.createElement('span');
  fallback.className = 'card-thumb image-placeholder';
  fallback.textContent = 'Art unavailable';
  img.replaceWith(fallback);
}, true);
window.addEventListener('storage', event => {
  if ((event.key !== STORAGE_KEY && event.key !== null) || event.newValue === lastSavedRaw) return;
  storageWritable = false;
  storageMessage = 'Another tab changed the saved deck. This tab now keeps changes in memory to protect that save. Download a backup before reloading to keep both versions.';
  $('storageWarning').textContent = storageMessage;
  $('storageWarning').hidden = false;
  $('saveState').textContent = 'Session only · another tab saved changes';
});
if (storageMessage) {
  $('storageWarning').textContent = storageMessage;
  $('storageWarning').hidden = false;
}
if (!storageWritable) $('saveState').textContent = 'Session only · download a backup';
renderDeck();
