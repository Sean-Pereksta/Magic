function livingOpponents(game, actorId) {
  return (game?.players || []).filter(player => player.id !== actorId && !player.eliminated && !player.left);
}

function nextOpponent(game, actorId) {
  const players = game?.players || [];
  const actorIndex = players.findIndex(player => player.id === actorId);
  if (actorIndex < 0) return null;
  for (let step = 1; step <= players.length; step += 1) {
    const candidate = players[(actorIndex + step) % players.length];
    if (candidate && candidate.id !== actorId && !candidate.eliminated && !candidate.left) return candidate;
  }
  return null;
}

export function strategicOpponentScore(player, purpose = "hostile") {
  const authority = Math.max(0, Number(player?.health) || 0);
  const shield = Math.max(0, Number(player?.shield) || 0);
  const bases = (player?.bases || []).length;
  const hand = (player?.hand || []).length;
  if (purpose === "discard") return hand * 12 + bases * 2 - authority * .08;
  if (purpose === "disable") return bases * 14 + shield * .2 - authority * .06;
  return (100 - Math.min(100, authority)) * 1.8 + bases * 5 - shield * .7;
}

export function chooseStrategicOpponent(game, actorId, purpose = "hostile") {
  return livingOpponents(game, actorId)
    .sort((left, right) => strategicOpponentScore(right, purpose) - strategicOpponentScore(left, purpose) || String(left.id).localeCompare(String(right.id)))[0] || null;
}

export function resolveOpponentTargets(game, actorId, targetRule = "nextOpponent", options = {}) {
  const opponents = livingOpponents(game, actorId);
  if (!opponents.length) return [];
  if (targetRule === "allOpponents") return opponents;
  if (targetRule === "lowestAuthorityOpponent") return [opponents.sort((a, b) => Number(a.health) - Number(b.health) || String(a.id).localeCompare(String(b.id)))[0]];
  if (targetRule === "highestAuthorityOpponent") return [opponents.sort((a, b) => Number(b.health) - Number(a.health) || String(a.id).localeCompare(String(b.id)))[0]];
  if (targetRule === "mostBasesOpponent") return [opponents.sort((a, b) => (b.bases || []).length - (a.bases || []).length || String(a.id).localeCompare(String(b.id)))[0]];
  if (targetRule === "chooseOpponent") {
    if (options.targetId) return opponents.filter(player => player.id === options.targetId);
    if (options.isBot) return [chooseStrategicOpponent(game, actorId, options.purpose)].filter(Boolean);
    return [];
  }
  return [nextOpponent(game, actorId)].filter(Boolean);
}

export function opponentTargetOptions(game, actorId, purpose = "hostile") {
  return livingOpponents(game, actorId)
    .map(player => ({
      id: player.id,
      label: player.name || "Opponent",
      authority: Math.max(0, Number(player.health) || 0),
      bases: (player.bases || []).length,
      score: strategicOpponentScore(player, purpose)
    }))
    .sort((left, right) => right.score - left.score || String(left.id).localeCompare(String(right.id)));
}
