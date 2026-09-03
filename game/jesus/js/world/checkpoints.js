export function createCheckpoint(game, x, y) {
  if (game.mode !== "journey") return null;
  const checkpoint = {
    nodeId: game.journeyNode?.id,
    x,
    y,
    hearts: Math.max(1, game.player.hearts),
    powers: { ...game.player.powers },
    roomSeed: game.world.seed,
    activatedAt: Date.now(),
  };
  game.journeySave.checkpoint = checkpoint;
  game.storage.saveJourney(game.journeySave);
  return checkpoint;
}

export function restoreCheckpoint(game) {
  const checkpoint = game.journeySave?.checkpoint;
  if (!checkpoint || checkpoint.nodeId !== game.journeyNode?.id) return false;
  game.player.x = checkpoint.x;
  game.player.y = checkpoint.y;
  game.player.vx = 0;
  game.player.vy = 0;
  game.player.hearts = Math.max(1, checkpoint.hearts);
  game.player.powers = { ...game.player.powers, ...checkpoint.powers };
  game.player.invulnerable = 1.2;
  return true;
}
