import { COLORS, VIEW } from "../config.js";
import { telegraphShapes } from "../bosses/boss-attacks.js";
import { drawEnemy as drawDetailedEnemy } from "./enemy-visuals.js";

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

function drawScenery(ctx, item, region, time, cameraX) {
  if (item.x < cameraX - 300 || item.x > cameraX + VIEW.width + 300) return;
  const s = item.scale || 1;
  const far = item.depth !== "near";
  const baseY = VIEW.floorY + (far ? 4 : 0);
  ctx.save();
  ctx.translate(item.x, baseY);
  ctx.scale((item.flip ? -1 : 1) * s, s);
  ctx.globalAlpha = far ? 0.42 : 0.72;

  if (item.kind === "palm") {
    ctx.strokeStyle = "#6f5a38";
    ctx.lineWidth = 8;
    ctx.beginPath(); ctx.moveTo(0, 2); ctx.quadraticCurveTo(8, -58, 1, -112); ctx.stroke();
    ctx.strokeStyle = "#3f7c55";
    ctx.lineWidth = 7;
    for (let a = -2.5; a <= 2.5; a += 1) {
      ctx.beginPath(); ctx.moveTo(2, -108); ctx.quadraticCurveTo(a * 22, -132 - Math.abs(a) * 3, a * 34, -112 + Math.abs(a) * 5); ctx.stroke();
    }
  } else if (item.kind === "reeds") {
    ctx.strokeStyle = "#667a42";
    ctx.lineWidth = 3;
    for (let i = -4; i <= 4; i++) {
      ctx.beginPath();
      ctx.moveTo(i * 6, 0);
      ctx.quadraticCurveTo(i * 7 + Math.sin(time * 1.8 + item.phase) * 3, -26, i * 5, -52 - (i % 3) * 7);
      ctx.stroke();
    }
  } else if (item.kind === "boat") {
    ctx.fillStyle = "#6a4d32";
    ctx.beginPath(); ctx.moveTo(-44, -16); ctx.lineTo(44, -16); ctx.lineTo(28, 0); ctx.lineTo(-30, 0); ctx.closePath(); ctx.fill();
    ctx.strokeStyle = "#59442f"; ctx.lineWidth = 4;
    ctx.beginPath(); ctx.moveTo(0, -16); ctx.lineTo(0, -78); ctx.stroke();
    ctx.fillStyle = "rgba(236,221,181,.75)";
    ctx.beginPath(); ctx.moveTo(3, -73); ctx.lineTo(34, -42); ctx.lineTo(3, -42); ctx.closePath(); ctx.fill();
  } else if (item.kind === "village" || item.kind === "stoneHouse") {
    ctx.fillStyle = region === "jerusalem" ? "#cbb58d" : "#b99b72";
    ctx.fillRect(-34, -55, 68, 55);
    ctx.fillStyle = "#826b50";
    ctx.beginPath(); ctx.moveTo(-40, -55); ctx.lineTo(0, -78); ctx.lineTo(40, -55); ctx.closePath(); ctx.fill();
    ctx.fillStyle = "rgba(60,48,39,.65)";
    ctx.fillRect(-9, -30, 18, 30);
  } else if (item.kind === "arch" || item.kind === "goldArch") {
    ctx.strokeStyle = item.kind === "goldArch" ? "#e5c45f" : "#9f8166";
    ctx.lineWidth = 13;
    ctx.beginPath(); ctx.moveTo(-34, 0); ctx.lineTo(-34, -54); ctx.quadraticCurveTo(0, -100, 34, -54); ctx.lineTo(34, 0); ctx.stroke();
  } else if (item.kind === "column") {
    ctx.fillStyle = "#b79a84";
    ctx.fillRect(-10, -82, 20, 82);
    ctx.fillRect(-18, -88, 36, 9);
    ctx.fillRect(-18, -7, 36, 7);
  } else if (item.kind === "banner") {
    ctx.strokeStyle = "#6b5a4b"; ctx.lineWidth = 4;
    ctx.beginPath(); ctx.moveTo(0, 0); ctx.lineTo(0, -91); ctx.stroke();
    ctx.fillStyle = "#7b3140";
    ctx.beginPath(); ctx.moveTo(3, -83); ctx.lineTo(38, -76); ctx.lineTo(31, -48); ctx.lineTo(3, -54); ctx.closePath(); ctx.fill();
  } else if (item.kind === "cypress" || item.kind === "olive") {
    ctx.fillStyle = item.kind === "olive" ? "#5f7650" : "#426247";
    ctx.beginPath();
    ctx.ellipse(0, -58, item.kind === "olive" ? 30 : 19, item.kind === "olive" ? 42 : 61, 0, 0, Math.PI * 2);
    ctx.fill();
    ctx.fillStyle = "#756044";
    ctx.fillRect(-4, -24, 8, 24);
  } else if (item.kind === "lamp") {
    ctx.strokeStyle = "#74614b"; ctx.lineWidth = 5;
    ctx.beginPath(); ctx.moveTo(0, 0); ctx.lineTo(0, -62); ctx.stroke();
    ctx.fillStyle = "#f0c75a";
    ctx.beginPath(); ctx.arc(0, -68, 9 + Math.sin(time * 5 + item.phase), 0, Math.PI * 2); ctx.fill();
  } else if (item.kind === "basalt" || item.kind === "ruin") {
    ctx.fillStyle = "#2f252c";
    ctx.beginPath(); ctx.moveTo(-38, 0); ctx.lineTo(-27, -48); ctx.lineTo(-5, -72); ctx.lineTo(8, -45); ctx.lineTo(32, -63); ctx.lineTo(40, 0); ctx.closePath(); ctx.fill();
  } else if (item.kind === "emberVent") {
    ctx.fillStyle = "#2d2227";
    ctx.fillRect(-24, -10, 48, 10);
    ctx.fillStyle = `rgba(255,112,61,${0.28 + Math.sin(time * 5 + item.phase) * 0.08})`;
    ctx.beginPath(); ctx.moveTo(-15, -10); ctx.quadraticCurveTo(-3, -58, 0, -24); ctx.quadraticCurveTo(8, -70, 17, -10); ctx.closePath(); ctx.fill();
  } else if (item.kind === "chain") {
    ctx.strokeStyle = "#6f5e67";
    ctx.lineWidth = 5;
    for (let y = -110; y < 0; y += 17) {
      ctx.beginPath();
      ctx.ellipse(Math.sin(y) * 3, y, 7, 11, (y / 17) % 2 ? 0 : Math.PI / 2, 0, Math.PI * 2);
      ctx.stroke();
    }
  } else if (item.kind === "cloudSpire") {
    ctx.fillStyle = "rgba(255,255,255,.88)";
    for (const cloud of [[0,-22,33],[-26,-10,24],[27,-8,25],[2,-50,18]]) {
      ctx.beginPath(); ctx.arc(cloud[0], cloud[1], cloud[2], 0, Math.PI * 2); ctx.fill();
    }
  } else if (item.kind === "lightColumn") {
    const glow = ctx.createLinearGradient(0, -135, 0, 0);
    glow.addColorStop(0, "rgba(255,248,195,0)");
    glow.addColorStop(1, "rgba(255,226,111,.45)");
    ctx.fillStyle = glow;
    ctx.fillRect(-18, -140, 36, 140);
  } else if (item.kind === "star") {
    ctx.fillStyle = "#fff2a9";
    ctx.translate(0, -70 + Math.sin(time * 2 + item.phase) * 7);
    ctx.rotate(time * 0.35 + item.phase);
    ctx.beginPath();
    for (let i = 0; i < 8; i++) {
      const a = i * Math.PI / 4;
      const r = i % 2 ? 5 : 16;
      ctx.lineTo(Math.cos(a) * r, Math.sin(a) * r);
    }
    ctx.closePath();
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
    for (let x = hazard.x + 8; x < hazard.x + hazard.w; x += 38) {
      ctx.fillRect(x, hazard.y + 4 + Math.sin(time * 5 + x) * 3, 20, 4);
    }
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

function drawLimb(ctx, x1, y1, x2, y2, color, width) {
  ctx.strokeStyle = color;
  ctx.lineWidth = width;
  ctx.lineCap = "round";
  ctx.beginPath();
  ctx.moveTo(x1, y1);
  ctx.lineTo(x2, y2);
  ctx.stroke();
}

function drawPlayer(ctx, player, time) {
  const speed = Math.min(1, Math.abs(player.vx) / 360);
  const phase = time * (8 + speed * 7);
  const airborne = !player.onGround;
  const stride = airborne ? 6 : Math.sin(phase) * 8.5 * speed;
  const armSwing = airborne ? -5 : Math.sin(phase + Math.PI) * 7 * speed;
  const bob = airborne ? 0 : Math.abs(Math.sin(phase * 2)) * 1.4 * speed;
  const hairSweep = Math.sin(time * 3.2) * 1.2 + Math.min(3, Math.abs(player.vx) / 150);
  const centerX = player.x + player.w / 2;
  const feetY = player.y + player.h;

  ctx.save();
  ctx.globalAlpha = player.invulnerable > 0 && Math.floor(time * 14) % 2 ? 0.45 : 1;
  ctx.translate(centerX, feetY - bob);
  ctx.scale(player.facing || 1, 1);

  ctx.strokeStyle = "rgba(232,190,76,.72)";
  ctx.lineWidth = 2.5;
  ctx.beginPath(); ctx.arc(0, -49, 13.5, 0, Math.PI * 2); ctx.stroke();

  ctx.fillStyle = "#5b3828";
  ctx.beginPath(); ctx.ellipse(-2 - hairSweep * 0.25, -49, 11, 15, -0.12, 0, Math.PI * 2); ctx.fill();
  ctx.beginPath(); ctx.ellipse(-8 - hairSweep, -39, 5.5, 12, -0.25, 0, Math.PI * 2); ctx.fill();
  ctx.beginPath(); ctx.ellipse(8 - hairSweep * 0.5, -40, 5, 11, 0.2, 0, Math.PI * 2); ctx.fill();

  drawLimb(ctx, -8, -34, -12 - armSwing * 0.55, -21 + Math.abs(armSwing) * 0.15, "#c58d68", 5.5);
  drawLimb(ctx, -12 - armSwing * 0.55, -21 + Math.abs(armSwing) * 0.15, -8 - armSwing, -10, "#c58d68", 4.5);

  const leftFootX = -5 + stride;
  const rightFootX = 5 - stride;
  drawLimb(ctx, -5, -14, leftFootX * 0.65, -6, "#c58d68", 6);
  drawLimb(ctx, leftFootX * 0.65, -6, leftFootX, -1, "#c58d68", 5);
  drawLimb(ctx, 5, -14, rightFootX * 0.65, -6, "#b97e5d", 6);
  drawLimb(ctx, rightFootX * 0.65, -6, rightFootX, -1, "#b97e5d", 5);
  drawLimb(ctx, leftFootX - 3, 0, leftFootX + 4, 0, "#6f4b34", 2.5);
  drawLimb(ctx, rightFootX - 3, 0, rightFootX + 4, 0, "#6f4b34", 2.5);

  ctx.fillStyle = "#f5ead8";
  ctx.beginPath();
  ctx.moveTo(-9, -39);
  ctx.quadraticCurveTo(0, -42, 9, -39);
  ctx.lineTo(13, -16);
  ctx.quadraticCurveTo(6, -11, 0, -12);
  ctx.quadraticCurveTo(-6, -11, -13, -16);
  ctx.closePath();
  ctx.fill();
  ctx.strokeStyle = "#d9c8ae";
  ctx.lineWidth = 1.2;
  ctx.stroke();
  ctx.strokeStyle = "#d6af5e";
  ctx.lineWidth = 2.3;
  ctx.beginPath(); ctx.moveTo(-1, -38); ctx.lineTo(1, -14); ctx.stroke();
  ctx.strokeStyle = "#b98d46";
  ctx.lineWidth = 2;
  ctx.beginPath(); ctx.moveTo(-10, -24); ctx.lineTo(11, -24); ctx.stroke();

  drawLimb(ctx, 8, -34, 12 + armSwing * 0.55, -22 + Math.abs(armSwing) * 0.1, "#cf9870", 5.5);
  drawLimb(ctx, 12 + armSwing * 0.55, -22 + Math.abs(armSwing) * 0.1, 9 + armSwing, -11, "#cf9870", 4.5);

  ctx.fillStyle = "#d29a72";
  ctx.beginPath(); ctx.ellipse(0, -50, 8.8, 10.5, 0, 0, Math.PI * 2); ctx.fill();
  ctx.fillStyle = "#5b3828";
  ctx.beginPath();
  ctx.arc(-2, -58, 8.5, Math.PI * 1.03, Math.PI * 1.92);
  ctx.lineTo(8, -53);
  ctx.quadraticCurveTo(3, -60, -5, -58);
  ctx.fill();
  ctx.beginPath();
  ctx.moveTo(-6, -48);
  ctx.quadraticCurveTo(0, -40, 6, -48);
  ctx.quadraticCurveTo(5, -39, 0, -36);
  ctx.quadraticCurveTo(-5, -39, -6, -48);
  ctx.fill();
  ctx.fillStyle = "#2f241f";
  ctx.fillRect(-4, -51, 1.5, 1.5);
  ctx.fillRect(3, -51, 1.5, 1.5);
  ctx.restore();
}

function drawBoss(ctx, boss) {
  if (!boss) return;
  ctx.save();
  ctx.globalAlpha = boss.state === "DEFEATED" ? Math.max(0.12, boss.stateTime / 1.4) : 1;
  ctx.fillStyle = boss.flash > 0
    ? "#ffffff"
    : boss.definition.region === "hell"
      ? "#7f2734"
      : boss.definition.region === "heaven"
        ? "#f5dc88"
        : "#57485d";
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
      ctx.beginPath();
      ctx.arc(shape.x, shape.y, shape.radius, 0, Math.PI * 2);
      ctx.fill();
      ctx.stroke();
    } else if (shape.type === "safe") {
      ctx.fillStyle = "rgba(255,220,95,.18)";
      ctx.strokeStyle = "rgba(255,224,100,.92)";
      ctx.fillRect(shape.x, shape.y, shape.w, shape.h);
      ctx.strokeRect(shape.x, shape.y, shape.w, shape.h);
    } else {
      ctx.fillRect(shape.x, shape.y, shape.w, shape.h);
      ctx.strokeRect(shape.x, shape.y, shape.w, shape.h);
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

    for (const item of game.world.scenery || []) drawScenery(ctx, item, game.world.region, game.time, game.camera.x);
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
      const float = Math.sin(game.time * 4 + pickup.x) * 4;
      ctx.fillStyle = "#f8d45a";
      ctx.beginPath();
      ctx.arc(pickup.x + 14, pickup.y + 14 + float, 12, 0, Math.PI * 2);
      ctx.fill();
      ctx.fillStyle = "#fff";
      ctx.font = "bold 14px sans-serif";
      ctx.textAlign = "center";
      ctx.fillText("✦", pickup.x + 14, pickup.y + 19 + float);
    }
    for (const enemy of game.world.enemies) drawDetailedEnemy(ctx, enemy, game.time);
    drawBoss(ctx, game.bossController?.boss);
    drawPlayer(ctx, game.player, game.time);

    if (game.mode === "journey" && game.world.checkpointX) {
      const shrineSurface = [...game.world.platforms, ...game.world.movers]
        .filter((surface) => game.world.checkpointX >= surface.x && game.world.checkpointX <= surface.x + surface.w)
        .sort((a, b) => a.y - b.y)[0];
      const shrineBase = shrineSurface?.y || VIEW.floorY;
      ctx.fillStyle = game.checkpointActivated ? "#ffe16c" : "#ddd4bc";
      ctx.fillRect(game.world.checkpointX, shrineBase - 76, 12, 76);
      ctx.beginPath();
      ctx.arc(game.world.checkpointX + 6, shrineBase - 82, 18, 0, Math.PI * 2);
      ctx.fill();
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
    if (boss) {
      lines.push(
        `Boss: ${boss.definition.name}`,
        `Phase: ${boss.phase.name}`,
        `State: ${boss.state}`,
        `Attack: ${boss.currentAttack?.name || "—"}`,
        `State time: ${boss.stateTime.toFixed(2)}`,
        `HP: ${boss.health} / ${boss.maxHealth}`,
      );
    }
    ctx.save();
    ctx.fillStyle = "rgba(8,12,20,.84)";
    ctx.fillRect(12, 136, 300, lines.length * 18 + 20);
    ctx.fillStyle = "#dff6ff";
    ctx.font = "12px ui-monospace, monospace";
    lines.forEach((lineText, index) => ctx.fillText(lineText, 24, 158 + index * 18));
    ctx.restore();
  }
}
