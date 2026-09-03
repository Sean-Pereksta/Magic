import { getTestLabCard } from "./test-lab-simulator.js?v=2";

function number(value) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : 0;
}

function rounded(value, digits = 1) {
  const scale = 10 ** digits;
  return Math.round(number(value) * scale) / scale;
}

function strategyName(id) {
  return String(id || "unknown")
    .replaceAll("_", " ")
    .replace(/\b\w/g, letter => letter.toUpperCase());
}

export function createStrategyAnalytics() {
  return {
    strategies: new Map(),
    cards: new Map()
  };
}

function ensureStrategy(analytics, strategyId) {
  if (!analytics.strategies.has(strategyId)) {
    analytics.strategies.set(strategyId, {
      strategyId,
      games: 0,
      wins: 0,
      losses: 0,
      draws: 0,
      totalTurns: 0,
      purchases: 0
    });
  }
  return analytics.strategies.get(strategyId);
}

function ensureStrategyCard(analytics, strategyId, cardId) {
  const key = `${strategyId}:${cardId}`;
  if (!analytics.cards.has(key)) {
    analytics.cards.set(key, {
      strategyId,
      cardId,
      purchases: 0,
      wins: 0,
      losses: 0,
      draws: 0,
      turnTotal: 0,
      gameSeats: new Set()
    });
  }
  return analytics.cards.get(key);
}

export function recordStrategyGame(analytics, result, gameIndex = 0) {
  for (const playerId of ["a", "b"]) {
    const strategyId = result.strategies?.[playerId] || "unknown";
    const stat = ensureStrategy(analytics, strategyId);
    stat.games += 1;
    stat.totalTurns += number(result.totalTurns);
    if (!result.winnerId) stat.draws += 1;
    else if (result.winnerId === playerId) stat.wins += 1;
    else stat.losses += 1;
  }

  for (const purchase of result.purchases || []) {
    const strategyId = result.strategies?.[purchase.playerId] || "unknown";
    const strategy = ensureStrategy(analytics, strategyId);
    const cardStat = ensureStrategyCard(analytics, strategyId, purchase.cardId);
    strategy.purchases += 1;
    cardStat.purchases += 1;
    cardStat.turnTotal += number(purchase.turn);
    cardStat.gameSeats.add(`${gameIndex}:${purchase.playerId}`);
    if (!result.winnerId) cardStat.draws += 1;
    else if (result.winnerId === purchase.playerId) cardStat.wins += 1;
    else cardStat.losses += 1;
  }
  return analytics;
}

export function strategyRankingRows(analytics) {
  return [...analytics.strategies.values()].map(stat => {
    const decisions = stat.wins + stat.losses;
    return {
      strategyId: stat.strategyId,
      name: strategyName(stat.strategyId),
      games: stat.games,
      wins: stat.wins,
      losses: stat.losses,
      draws: stat.draws,
      winRate: decisions ? rounded(stat.wins / decisions * 100, 1) : 0,
      averageTurns: stat.games ? rounded(stat.totalTurns / stat.games, 1) : 0,
      purchases: stat.purchases,
      purchasesPerGame: stat.games ? rounded(stat.purchases / stat.games, 2) : 0
    };
  }).sort((a, b) => (b.winRate - a.winRate) || (b.games - a.games) || a.name.localeCompare(b.name));
}

export function strategyCardRows(analytics, limit = 50) {
  const totalPurchases = new Map();
  for (const stat of analytics.cards.values()) {
    totalPurchases.set(stat.strategyId, (totalPurchases.get(stat.strategyId) || 0) + stat.purchases);
  }

  const grouped = new Map();
  for (const stat of analytics.cards.values()) {
    const card = getTestLabCard(stat.cardId);
    const decisions = stat.wins + stat.losses;
    const row = {
      strategyId: stat.strategyId,
      strategyName: strategyName(stat.strategyId),
      cardId: stat.cardId,
      name: card?.name || stat.cardId,
      faction: card?.faction || "neutral",
      cost: number(card?.cost),
      purchases: stat.purchases,
      gamesPurchased: stat.gameSeats.size,
      purchaseShare: totalPurchases.get(stat.strategyId)
        ? rounded(stat.purchases / totalPurchases.get(stat.strategyId) * 100, 1)
        : 0,
      wins: stat.wins,
      losses: stat.losses,
      draws: stat.draws,
      winRate: decisions ? rounded(stat.wins / decisions * 100, 1) : 0,
      avgBuyTurn: stat.purchases ? rounded(stat.turnTotal / stat.purchases, 2) : 0
    };
    if (!grouped.has(stat.strategyId)) grouped.set(stat.strategyId, []);
    grouped.get(stat.strategyId).push(row);
  }

  const output = {};
  for (const [strategyId, rows] of grouped.entries()) {
    output[strategyId] = rows
      .sort((a, b) => (b.purchases - a.purchases) || (b.gamesPurchased - a.gamesPurchased) || a.name.localeCompare(b.name))
      .slice(0, Math.max(1, Math.min(50, Math.floor(Number(limit) || 50))));
  }
  return output;
}
