import {
  createTestLabAccumulator,
  recordTestLabGame,
  simulateTestLabGame,
  testLabCardRows,
  testLabSummary
} from "./test-lab-simulator.js?v=2";
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

  for (let index = 0; index < games; index += 1) {
    if (hooks.signal?.aborted) break;
    const result = simulateTestLabGame(options, index);
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
    accumulator,
    summary: testLabSummary(accumulator),
    rows: testLabCardRows(accumulator),
    recentGames,
    ...strategyPayload(analytics, strategyAnalyticsEnabled, strategyCardLimit)
  };
}
