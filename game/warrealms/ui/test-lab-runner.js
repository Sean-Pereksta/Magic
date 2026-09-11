import { experimentSchedule, recordDeckResult } from "./test-lab-experiments.js";
import {
  createTestLabAccumulator,
  recordTestLabGame,
  testLabCardRows,
  testLabSummary
} from "./test-lab-simulator.js?v=3";
import { simulateExtendedTestLabGame as simulateTestLabGame } from "./test-lab-strategy-extension.js?v=1";
import {
  createStrategyAnalytics,
  recordStrategyGame,
  strategyCardRows,
  strategyRankingRows
} from "./test-lab-strategy-analytics.js?v=1";

function clamp(value, minimum, maximum) {
  return Math.max(minimum, Math.min(maximum, value));
}

function strategyPayload(analytics, enabled, limit) {
  if (!enabled) return { strategyRows: [], strategyCards: {} };
  return {
    strategyRows: strategyRankingRows(analytics),
    strategyCards: strategyCardRows(analytics, limit)
  };
}

export async function runTestLabWithHistory(options = {}, hooks = {}) {
  const games = clamp(Math.floor(Number(options.games) || 1000), 1, 50000);
  const batchSize = clamp(Math.floor(Number(options.batchSize) || 20), 1, 250);
  const strategyAnalyticsEnabled = options.strategyAnalyticsEnabled !== false;
  const strategyCardLimit = clamp(Math.floor(Number(options.strategyCardLimit) || 50), 5, 50);
  const accumulator = createTestLabAccumulator();
  const analytics = createStrategyAnalytics();
  const recentGames = [];
  const schedule = experimentSchedule(options.decks || []);
  const deckResults = new Map();
  if (schedule.length > games) throw new Error(`Choose at least ${schedule.length} games to cover every selected matchup, or select fewer playstyles.`);

  for (let index = 0; index < games; index += 1) {
    if (hooks.signal?.aborted) break;
    const scheduled = schedule.length ? schedule[index % schedule.length] : null;
    const pair = scheduled;
    const gameOptions = { ...options, firstPlayer: schedule.length ? (Math.floor(index / schedule.length) % 2 ? "b" : "a") : undefined };
    if (pair) pair.forEach((deck, side) => {
      if (!deck) return;
      const suffix = side ? "B" : "A";
      gameOptions[`strategy${suffix}`] = deck.style;
      gameOptions[`commandDeck${suffix}`] = deck.cardIds;
    });
    const result = simulateTestLabGame(gameOptions, index);
    recordDeckResult(deckResults, pair, result);
    recordTestLabGame(accumulator, result, index);
    if (strategyAnalyticsEnabled) recordStrategyGame(analytics, result, index);
    recentGames.unshift({
      gameNumber: index + 1,
      winnerId: result.winnerId,
      draw: result.draw,
      totalTurns: result.totalTurns,
      rounds: result.rounds,
      strategies: result.strategies,
      authority: result.authority,
      priorities: result.priorities,
      purchases: result.purchases || []
    });
    if (recentGames.length > 10) recentGames.length = 10;

    if ((index + 1) % batchSize === 0 || index + 1 === games) {
      hooks.onProgress?.({
        deckRows: [...deckResults.values()],
        accumulator,
        summary: testLabSummary(accumulator),
        rows: testLabCardRows(accumulator),
        recentGames: [...recentGames],
        ...strategyPayload(analytics, strategyAnalyticsEnabled, strategyCardLimit),
        completed: index + 1,
        requested: games
      });
      await new Promise(resolve => setTimeout(resolve, 0));
    }
  }

  return {
    deckRows: [...deckResults.values()],
    accumulator,
    summary: testLabSummary(accumulator),
    rows: testLabCardRows(accumulator),
    recentGames,
    ...strategyPayload(analytics, strategyAnalyticsEnabled, strategyCardLimit)
  };
}
