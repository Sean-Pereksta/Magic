import { COLORS, VIEW } from "../config.js";
import { telegraphShapes } from "../bosses/boss-attacks.js";

function roundedRect(ctx, x, y, w, h, radius) {
  ctx.beginPath();
  ctx.roundRect(x, y, w, h, radius);
}

function drawBackground(ctx, region, cameraX) {
  const palette = COLORS[region] || COLORS.galilee;
  const gradient = ctx.createLinearGradient(0, 0, 0, VIEW.height);
  gradient.addColorStop(0, palette.sky);
  gradient.addColorStop(1, region === "hell" ? "#130e13" : "#fff9e9");
  ctx.fillStyle = gradient;
  ctx.fillRect(0, 0, VIEW.width, VIEW.height);
  ctx.save();
  ctx.translate(-(cameraX * 0.14) % 280, 0);
  ctx.fillStyle = palette.far;
  ctx.globalAlpha = region === "hell" ? 0.45 : 0.32;
  for (let x = -280; x < VIEW.width + 560; x += 280) {
    ctx.beginPath();
    ctx.moveTo(x, VIEW.floorY);
    ctx.lineTo(x + 130, 205 + (x % 3) * 24);
    ctx.lineTo(x + 280, VIEW.floorY);
    ctx.fill();
  }
  ctx.restore();
}

function drawPlatform(ctx, platform, region) {
  const palette = COLORS[region] || COLORS.galilee;
  if (platform.kind === "cloud" || platform.kind === "blessed") {
    ctx.fillStyle = "rgba(255,255,255,.94)";
    ctx.shadowColor = platform.kind === "blessed" ? "#ffe58a" : "#9ed8ff";
    ctx.shadowBlur = 12;
  } else if (platform.kind === "obsidian") {
    ctx.fillStyle = "#241d26";
    ctx.shadowColor = "#ff5e3c";
    ctx.shadowBlur = 7;
  } else {
    ctx.fillStyle = palette.ground;
    ctx.shadowBlur = 0;
  }
  roundedRect(ctx, platform.x, platform.y, platform.w, platform.h, platform.kind === "ground" ? 8 : 6);
  ctx.fill();
  ctx.shadowBlur = 0;
  ctx.fillStyle = palette.accent;
  ctx.globalAlpha = 0.62;
  ctx.fillRect(platform.x + 2, platform.y + 2, platform.w - 4, 4);
  ctx.globalAlpha = 1;
}

function drawHazard(ctx, hazard, time) {
  if (hazard.type === "lava") {
    ctx.fillStyle = "#c93f2f";
    ctx.fillRect(hazard.x, hazard.y, hazard.w, hazard.h);
    ctx.fillStyle = "#ffbd59";
    for (let x = hazard.x + 8; x < hazard.x + hazard.w; x += 38) ctx.fillRect(x, hazard.y + 4 + Math.sin(time * 5 + x) * 3, 20, 4);
    return;
  }
  if (hazard.type === "fireJet") {
    ctx.fillStyle = "#4a3030";
    ctx.fillRect(hazard.x, hazard.y, hazard.w, hazard.h);
    if (hazard.active) {
      ctx.fillStyle = "rgba(255,102,51,.82)";
      ctx.fillRect(hazard.x + 5, hazard.y - 100, hazard.w - 10, 102);
    }
    return;
  }
  ctx.fillStyle = "#cbd0d8";
  const count = Math.max(2, Math.floor(hazard.w / 15));
  for (let index = 0; index < count; index++) {
    const x = hazard.x + index * (hazard.w / count);
    ctx.beginPath();
    ctx.moveTo(x, hazard.y + hazard.h);
    ctx.lineTo(x + hazard.w / count / 2, hazard.y);
    ctx.lineTo(x + hazard.w / count, hazard.y + hazard.h);
    ctx.fill();
  }
}

function drawPlayer(ctx, player, time) {
  ctx.save();
  ctx.globalAlpha = player.invulnerable > 0 && Math.floor(time * 14) % 2 ? 0.45 : 1;
  ctx.fillStyle = "#f5ead8";
  roundedRect(ctx, player.x + 5, player.y + 17, player.w - 10, player.h - 17, 9);
  ctx.fill();
  ctx.fillStyle = "#734c35";
  ctx.beginPath();
  ctx.arc(player.x + player.w / 2, player.y + 13, 12, 0, Math.PI * 2);
  ctx.fill();
  ctx.strokeStyle = "#d6af5e";
  ctx.lineWidth = 3;
  ctx.beginPath();
  ctx.moveTo(player.x + player.w / 2, player.y + 28);
  ctx.lineTo(player.x + player.w / 2, player.y + 53);
  ctx.stroke();
  ctx.restore();
}

function drawEnemy(ctx, enemy) {
  ctx.save();
  ctx.globalAlpha = enemy.dead ? Math.max(0, (enemy.fade || 0) / 0.3) : 1;
  ctx.fillStyle = enemy.elite ? "#8d304a" : enemy.type.includes("fire") || enemy.type.includes("imp") ? "#a1392f" : "#5b5662";
  roundedRect(ctx, enemy.x, enemy.y, enemy.w, enemy.h, 10);
  ctx.fill();
  ctx.fillStyle = "#f3d7b5";
  ctx.fillRect(enemy.x + 10, enemy.y + 12, 5, 5);
  ctx.fillRect(enemy.x + enemy.w - 15, enemy.y + 12, 5, 5);
  ctx.restore();
}

function drawBoss(ctx, boss) {
  if (!boss) return;
  ctx.save();
  ctx.globalAlpha = boss.state === "DEFEATED" ? Math.max(0.12, boss.stateTime / 1.4) : 1;
  ctx.fillStyle = boss.flash > 0 ? "#ffffff" : boss.definition.region === "hell" ? "#7f2734" : boss.definition.region === "heaven" ? "#f5dc88" : "#57485d";
  roundedRect(ctx, boss.x, boss.y, boss.w, boss.h, 18);
  ctx.fill();
  ctx.strokeStyle = boss.state === "VULNERABLE" ? "#ffe26b" : "rgba(255,255,255,.55)";
  ctx.lineWidth = boss.state === "VULNERABLE" ? 6 : 2;
  ctx.stroke();
  ctx.fillStyle = "#fff";
  ctx.fillRect(boss.x + 18, boss.y + 23, 8, 7);
  ctx.fillRect(boss.x + boss.w - 26, boss.y + 23, 8, 7);
  ctx.restore();
}

function drawTelegraphs(ctx, game) {
  const boss = game.bossController?.boss;
  if (!boss || boss.state !== "TELL") return;
  const shapes = telegraphShapes(boss.currentAttack, game.bossController.arena);
  ctx.save();
  ctx.fillStyle = "rgba(225,45,62,.22)";
  ctx.strokeStyle = "rgba(255,65,82,.9)";
  ctx.lineWidth = 3;
  ctx.setLineDash([10, 8]);
  for (const shape of shapes) {
    if (shape.type === "circle") {
      ctx.beginPath(); ctx.arc(shape.x, shape.y, shape.radius, 0, Math.PI * 2); ctx.fill(); ctx.stroke();
    } else if (shape.type === "safe") {
      ctx.fillStyle = "rgba(255,220,95,.18)";
      ctx.strokeStyle = "rgba(255,224,100,.92)";
      ctx.fillRect(shape.x, shape.y, shape.w, shape.h); ctx.strokeRect(shape.x, shape.y, shape.w, shape.h);
    } else {
      ctx.fillRect(shape.x, shape.y, shape.w, shape.h); ctx.strokeRect(shape.x, shape.y, shape.w, shape.h);
    }
  }
  ctx.restore();
}

function drawBossHazard(ctx, hazard) {
  ctx.save();
  ctx.fillStyle = hazard.type === "fire" ? "#ff6d3d" : hazard.type === "light" ? "#ffe884" : "#9f7cff";
  if (["judgment", "eruption", "attackZone"].includes(hazard.type)) ctx.globalAlpha = 0.6;
  roundedRect(ctx, hazard.x, hazard.y, hazard.w, hazard.h, Math.min(8, hazard.w / 2));
  ctx.fill();
  ctx.restore();
}

export class Renderer {
  constructor(canvas) {
    this.canvas = canvas;
    this.ctx = canvas.getContext("2d");
  }

  draw(game) {
    const ctx = this.ctx;
    drawBackground(ctx, game.world.region, game.camera.x);
    ctx.save();
    game.camera.transform(ctx);

    for (const platform of game.world.platforms) drawPlatform(ctx, platform, game.world.region);
    for (const mover of game.world.movers) drawPlatform(ctx, mover, game.world.region);
    for (const prop of game.world.props || []) {
      if (prop.dead) continue;
      ctx.fillStyle = "#aa927b";
      ctx.fillRect(prop.x, prop.y, prop.w, prop.h);
    }
    for (const hazard of game.world.hazards) drawHazard(ctx, hazard, game.time);
    drawTelegraphs(ctx, game);
    for (const hazard of game.world.bossHazards || []) drawBossHazard(ctx, hazard);
    for (const pickup of game.world.pickups) {
      if (pickup.collected) continue;
      ctx.fillStyle = "#f8d45a";
      ctx.beginPath(); ctx.arc(pickup.x + 14, pickup.y + 14 + Math.sin(game.time * 4 + pickup.x) * 4, 12, 0, Math.PI * 2); ctx.fill();
      ctx.fillStyle = "#fff";
      ctx.font = "bold 14px sans-serif";
      ctx.textAlign = "center";
      ctx.fillText("✦", pickup.x + 14, pickup.y + 19 + Math.sin(game.time * 4 + pickup.x) * 4);
    }
    for (const enemy of game.world.enemies) drawEnemy(ctx, enemy);
    drawBoss(ctx, game.bossController?.boss);
    drawPlayer(ctx, game.player, game.time);

    if (game.mode === "journey" && game.world.checkpointX) {
      const shrineSurface = [...game.world.platforms, ...game.world.movers]
        .filter((surface) => game.world.checkpointX >= surface.x && game.world.checkpointX <= surface.x + surface.w)
        .sort((a, b) => a.y - b.y)[0];
      const shrineBase = shrineSurface?.y || VIEW.floorY;
      ctx.fillStyle = game.checkpointActivated ? "#ffe16c" : "#ddd4bc";
      ctx.fillRect(game.world.checkpointX, shrineBase - 76, 12, 76);
      ctx.beginPath(); ctx.arc(game.world.checkpointX + 6, shrineBase - 82, 18, 0, Math.PI * 2); ctx.fill();
    }
    if (!game.bossController?.boss) {
      ctx.fillStyle = "rgba(255,225,110,.75)";
      ctx.fillRect(game.world.width - 56, VIEW.floorY - 128, 18, 128);
    }
    ctx.restore();

    if (game.debug) this.drawDebug(game);
  }

  drawDebug(game) {
    const ctx = this.ctx;
    const boss = game.bossController?.boss;
    const lines = [
      `FPS: ${Math.round(game.fps)}`,
      `Mode: ${game.mode || "menu"}`,
      `Room: ${game.world.name || game.world.id}`,
      `Region: ${game.world.region}`,
      `Difficulty: ${game.world.tier || game.difficulty}`,
      `Player: ${Math.round(game.player.x)}, ${Math.round(game.player.y)}`,
      `Enemies: ${game.world.enemies.filter((enemy) => !enemy.dead).length}`,
      `Hazards: ${game.world.hazards.length + (game.world.bossHazards?.length || 0)}`,
      `Generated route valid: ${game.world.routeValid === false ? "NO" : "YES"}`,
    ];
    if (boss) lines.push(`Boss: ${boss.definition.name}`, `Phase: ${boss.phase.name}`, `State: ${boss.state}`, `Attack: ${boss.currentAttack?.name || "—"}`, `State time: ${boss.stateTime.toFixed(2)}`, `HP: ${boss.health} / ${boss.maxHealth}`);
    ctx.save();
    ctx.fillStyle = "rgba(8,12,20,.84)";
    ctx.fillRect(12, 136, 300, lines.length * 18 + 20);
    ctx.fillStyle = "#dff6ff";
    ctx.font = "12px ui-monospace, monospace";
    lines.forEach((line, index) => ctx.fillText(line, 24, 158 + index * 18));
    ctx.restore();
  }
}
