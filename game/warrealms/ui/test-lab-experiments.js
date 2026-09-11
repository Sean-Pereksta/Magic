import { normalizeTestLabCommandDeck } from "./test-lab-simulator.js";
import { getTestLabDeckPreset } from './test-lab-deck-presets.js';
import { EXTENDED_TEST_LAB_STRATEGIES } from './test-lab-strategy-extension.js';

export function experimentSchedule(decks = []) {
  const variants = decks.flatMap(deck => {
    const preset = getTestLabDeckPreset(deck.id);
    const cardIds = preset?.cardIds || deck.cardIds;
    if (!normalizeTestLabCommandDeck(cardIds)) throw new Error('Each test deck must contain 50 eligible cards, with at most four copies of each.');
    const styles = [...new Set(deck.styles || [])];
    if (!styles.length) throw new Error(`Select a playstyle for ${deck.name || preset?.name || deck.id}.`);
    return styles.map(style => {
      if (!EXTENDED_TEST_LAB_STRATEGIES.some(s => s.id === style)) throw new Error('Unknown playstyle.');
      return { id: deck.id, name: deck.name || preset?.name || deck.id, cardIds: [...cardIds], style };
    });
  });
  const pairs = [];
  for (let a = 0; a < variants.length; a++) for (let b = a + 1; b < variants.length; b++) pairs.push([variants[a], variants[b]]);
  if (variants.length === 1) pairs.push([variants[0], null]);
  return pairs;
}

export function recordDeckResult(rows, pair, result) {
  if (!pair) return;
  pair.forEach((deck, index) => {
    if (!deck) return;
    const side = index ? 'b' : 'a';
    const style = result.strategies[side];
    const key = JSON.stringify([deck.id, style]);
    const row = rows.get(key) || { deckId: deck.id, deck: deck.name, style, games: 0, wins: 0, losses: 0, draws: 0 };
    row.games++;
    if (result.draw) row.draws++; else if (result.winnerId === side) row.wins++; else row.losses++;
    rows.set(key, row);
  });
}

export function groupDeckResults(rows, mode = 'combined') {
  const groups = new Map();
  for (const row of rows) {
    const key = mode === 'deck' ? row.deckId : mode === 'style' ? row.style : JSON.stringify([row.deckId, row.style]);
    const group = groups.get(key) || { deck: mode === 'style' ? 'All decks' : row.deck, style: mode === 'deck' ? 'All playstyles' : row.style, games: 0, wins: 0, losses: 0, draws: 0 };
    for (const field of ['games', 'wins', 'losses', 'draws']) group[field] += row[field];
    groups.set(key, group);
  }
  return [...groups.values()].map(row => ({ ...row, winRate: row.games ? row.wins / row.games * 100 : 0 }));
}
