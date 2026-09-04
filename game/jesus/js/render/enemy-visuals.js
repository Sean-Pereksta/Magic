// Enemy appearance and animation lives here so gameplay AI/hitboxes stay independent
// from presentation. Every enemy type can be visually upgraded without touching physics.

const TAU = Math.PI * 2;

function phaseFor(enemy) {
  const text = String(enemy.id || enemy.type || "enemy");
  let hash = 0;
  for (let i = 0; i < text.length; i++) hash = ((hash << 5) - hash + text.charCodeAt(i)) | 0;
  return (Math.abs(hash) % 1000) / 1000 * TAU;
}

function ellipse(ctx, x, y, rx, ry, color, rotation = 0) {
  ctx.fillStyle = color;
  ctx.beginPath();
  ctx.ellipse(x, y, rx, ry, rotation, 0, TAU);
  ctx.fill();
}

function line(ctx, x1, y1, x2, y2, color, width = 3) {
  ctx.strokeStyle = color;
  ctx.lineWidth = width;
  ctx.lineCap = "round";
  ctx.beginPath();
  ctx.moveTo(x1, y1);
  ctx.lineTo(x2, y2);
  ctx.stroke();
}

function polygon(ctx, points, color, stroke = null, strokeWidth = 1) {
  if (!points.length) return;
  ctx.beginPath();
  ctx.moveTo(points[0][0], points[0][1]);
  for (let i = 1; i < points.length; i++) ctx.lineTo(points[i][0], points[i][1]);
  ctx.closePath();
  ctx.fillStyle = color;
  ctx.fill();
  if (stroke) {
    ctx.strokeStyle = stroke;
    ctx.lineWidth = strokeWidth;
    ctx.stroke();
  }
}

function eliteAura(ctx, enemy, time, phase) {
  if (!enemy.elite) return;
  const pulse = 0.55 + Math.sin(time * 5 + phase) * 0.16;
  ctx.save();
  ctx.globalAlpha *= pulse;
  ctx.strokeStyle = "#e7bc62";
  ctx.lineWidth = 2.2;
  ctx.setLineDash([4, 5]);
  ctx.beginPath();
  ctx.ellipse(0, -enemy.h * 0.48, enemy.w * 0.68, enemy.h * 0.58, 0, 0, TAU);
  ctx.stroke();
  ctx.setLineDash([]);
  ctx.restore();
}

function shadow(ctx, width, alpha = 0.22) {
  ctx.save();
  ctx.globalAlpha *= alpha;
  ellipse(ctx, 0, -1, width, 4, "#141016");
  ctx.restore();
}

function humanoidBase(ctx, enemy, time, phase, palette, options = {}) {
  const speedFactor = enemy.dead ? 0 : 1;
  const gait = Math.sin(time * (options.gaitSpeed || 8) + phase) * speedFactor;
  const bob = Math.abs(Math.sin(time * (options.gaitSpeed || 8) + phase)) * 1.25 * speedFactor;
  const h = enemy.h;
  const torsoY = -h * 0.48 - bob;
  const headY = -h * 0.79 - bob;
  const legTop = -h * 0.34 - bob;
  const footY = -3;
  const stride = gait * Math.min(6.5, enemy.w * 0.16);

  shadow(ctx, enemy.w * 0.35);

  line(ctx, -4, legTop, -5 + stride, footY, palette.leg || palette.skin, 5);
  line(ctx, 4, legTop, 5 - stride, footY, palette.leg || palette.skin, 5);
  line(ctx, -8 + stride, footY, -2 + stride, footY, palette.boot || "#3b2d28", 3);
  line(ctx, 2 - stride, footY, 8 - stride, footY, palette.boot || "#3b2d28", 3);

  if (options.robe) {
    polygon(ctx, [
      [-enemy.w * 0.22, torsoY - h * 0.15],
      [enemy.w * 0.22, torsoY - h * 0.15],
      [enemy.w * 0.30, legTop + 4],
      [0, legTop + 8],
      [-enemy.w * 0.30, legTop + 4],
    ], palette.body, palette.trim || null, 1.2);
  } else {
    ctx.fillStyle = palette.body;
    ctx.beginPath();
    ctx.roundRect(-enemy.w * 0.25, torsoY - h * 0.16, enemy.w * 0.5, h * 0.34, 6);
    ctx.fill();
  }

  if (palette.belt) line(ctx, -enemy.w * 0.22, torsoY + h * 0.05, enemy.w * 0.22, torsoY + h * 0.05, palette.belt, 2);

  const armSwing = gait * 4.5;
  line(ctx, -enemy.w * 0.20, torsoY - 4, -enemy.w * 0.28 - armSwing, torsoY + h * 0.17, palette.arm || palette.skin, 4.5);
  line(ctx, enemy.w * 0.20, torsoY - 4, enemy.w * 0.28 + armSwing, torsoY + h * 0.17, palette.arm || palette.skin, 4.5);

  ellipse(ctx, 0, headY, enemy.w * 0.18, h * 0.12, palette.skin);
  if (palette.hair) {
    ctx.fillStyle = palette.hair;
    ctx.beginPath();
    ctx.arc(0, headY - 2, enemy.w * 0.19, Math.PI, TAU);
    ctx.fill();
  }

  return { gait, bob, torsoY, headY, armSwing };
}

function drawWanderer(ctx, enemy, time, phase) {
  const p = humanoidBase(ctx, enemy, time, phase, {
    skin: "#b88766", body: "#776457", trim: "#a18b72", belt: "#4d4037", hair: "#4b372d", boot: "#433329",
  }, { robe: true, gaitSpeed: 7 });
  // Ragged scarf and travel satchel.
  polygon(ctx, [[-8, p.torsoY - 11], [7, p.torsoY - 10], [4, p.torsoY - 3], [-10, p.torsoY - 4]], "#94775f");
  ellipse(ctx, 10, p.torsoY + 9, 6, 8, "#5f4a37", 0.2);
  line(ctx, -2, p.torsoY - 8, 10, p.torsoY + 4, "#4e3b30", 2);
}

function drawHound(ctx, enemy, time, phase, brim = false) {
  const run = Math.sin(time * 11 + phase);
  const bob = Math.abs(Math.sin(time * 11 + phase)) * 1.6;
  const body = brim ? "#51272b" : "#51463f";
  const dark = brim ? "#2e171a" : "#332d29";
  const accent = brim ? "#ff6c3b" : "#7a6759";
  shadow(ctx, enemy.w * 0.42);
  ctx.save();
  ctx.translate(0, -bob);
  ellipse(ctx, -1, -enemy.h * 0.38, enemy.w * 0.34, enemy.h * 0.18, body, -0.08);
  ellipse(ctx, enemy.w * 0.27, -enemy.h * 0.45, enemy.w * 0.17, enemy.h * 0.15, body, 0.12);
  polygon(ctx, [[enemy.w * .21, -enemy.h * .56], [enemy.w * .28, -enemy.h * .72], [enemy.w * .33, -enemy.h * .55]], dark);
  polygon(ctx, [[enemy.w * .34, -enemy.h * .55], [enemy.w * .43, -enemy.h * .67], [enemy.w * .43, -enemy.h * .49]], dark);
  line(ctx, -enemy.w * .28, -enemy.h * .39, -enemy.w * .42, -enemy.h * .49 + run * 2, dark, 4);
  line(ctx, -enemy.w * .20, -enemy.h * .28, -enemy.w * .22 + run * 5, -2, dark, 5);
  line(ctx, enemy.w * .12, -enemy.h * .28, enemy.w * .13 - run * 5, -2, dark, 5);
  ellipse(ctx, enemy.w * .34, -enemy.h * .47, 2.3, 2.3, brim ? "#ffd06a" : "#e4c39e");
  ellipse(ctx, enemy.w * .43, -enemy.h * .40, 2.6, 2.1, "#181416");
  if (brim) {
    for (let i = -1; i <= 1; i++) {
      const flicker = Math.sin(time * 8 + phase + i) * 3;
      polygon(ctx, [[-9 + i * 8, -enemy.h * .50], [-3 + i * 8, -enemy.h * .70 - flicker], [2 + i * 8, -enemy.h * .49]], accent);
    }
  }
  ctx.restore();
}

function drawLegionary(ctx, enemy, time, phase, variant = "legionary") {
  const colors = variant === "temple-guard"
    ? { skin: "#b98563", body: "#d2c3a4", arm: "#a97959", leg: "#8f6e58", boot: "#4d3d31", belt: "#6b513a", hair: "#3d312b", trim: "#9e7f52" }
    : { skin: "#b87f5f", body: "#8b3f42", arm: "#9e6a55", leg: "#77584b", boot: "#3c302b", belt: "#d2aa59", hair: "#3d2f2b", trim: "#b58c56" };
  const p = humanoidBase(ctx, enemy, time, phase, colors, { robe: variant === "temple-guard", gaitSpeed: 8.5 });
  const metal = variant === "temple-guard" ? "#8e8271" : "#9b9692";
  // Helmet.
  ctx.fillStyle = metal;
  ctx.beginPath();
  ctx.arc(0, p.headY - 2, enemy.w * .20, Math.PI, TAU);
  ctx.fill();
  ctx.fillRect(-enemy.w * .20, p.headY - 2, enemy.w * .40, 4);
  if (variant === "legionary") {
    polygon(ctx, [[-2, p.headY - 10], [0, p.headY - 22], [4, p.headY - 10]], "#6f2630");
    // Sword and small shield.
    line(ctx, 12, p.torsoY + 2, 18 + p.armSwing * .3, p.torsoY + 22, "#bfc3c7", 2.5);
    polygon(ctx, [[-17, p.torsoY - 11], [-8, p.torsoY - 15], [-6, p.torsoY + 10], [-16, p.torsoY + 15], [-22, p.torsoY + 5]], "#7e3439", "#c8a45a", 1.5);
  } else {
    line(ctx, 15, p.torsoY - 8, 18, 2, "#66523b", 3);
    polygon(ctx, [[15, p.torsoY - 8], [22, p.torsoY - 16], [16, p.torsoY - 18]], "#b8b1a1");
  }
}

function drawShieldBearer(ctx, enemy, time, phase) {
  const p = humanoidBase(ctx, enemy, time, phase, {
    skin: "#ac7658", body: "#62494b", arm: "#8b5e4f", leg: "#674d43", boot: "#382d2a", belt: "#a78b56", hair: "#342b28",
  }, { gaitSpeed: 7.5 });
  // Tower shield dominates the silhouette.
  polygon(ctx, [[-enemy.w*.36, p.torsoY-19], [-enemy.w*.05, p.torsoY-16], [-enemy.w*.02, p.torsoY+23], [-enemy.w*.20, p.torsoY+29], [-enemy.w*.42, p.torsoY+18]], "#8c6f4d", "#d0ad66", 2);
  line(ctx, -enemy.w*.25, p.torsoY-10, -enemy.w*.20, p.torsoY+20, "#d0ad66", 2);
  line(ctx, -enemy.w*.36, p.torsoY+4, -enemy.w*.05, p.torsoY+4, "#d0ad66", 2);
  ctx.fillStyle = "#898781";
  ctx.beginPath(); ctx.arc(0, p.headY-3, enemy.w*.2, Math.PI, TAU); ctx.fill();
}

function drawJavelin(ctx, enemy, time, phase) {
  const p = humanoidBase(ctx, enemy, time, phase, {
    skin: "#ba8260", body: "#76564c", arm: "#a56f58", leg: "#6c5146", boot: "#3b302b", belt: "#b18d4f", hair: "#3c302a",
  }, { gaitSpeed: 9 });
  const thrust = Math.sin(time * 4 + phase) * 3;
  line(ctx, 13, p.torsoY - 17 + thrust, 21, 3 + thrust, "#694e32", 3);
  polygon(ctx, [[13, p.torsoY-17+thrust], [19, p.torsoY-27+thrust], [21, p.torsoY-16+thrust]], "#b9bdbe");
  // Leather shoulder guard.
  polygon(ctx, [[-10,p.torsoY-15],[1,p.torsoY-18],[5,p.torsoY-8],[-8,p.torsoY-6]], "#8b6b50");
}

function drawZealot(ctx, enemy, time, phase) {
  const p = humanoidBase(ctx, enemy, time, phase, {
    skin: "#b77e5d", body: "#66513f", arm: "#a46f55", leg: "#765a48", boot: "#40332b", belt: "#8f7048", hair: "#322923", trim: "#8b7057",
  }, { robe: true, gaitSpeed: 10 });
  // Head wrap and curved blade.
  ctx.fillStyle = "#9b7b59";
  ctx.fillRect(-enemy.w*.18, p.headY-8, enemy.w*.36, 5);
  line(ctx, enemy.w*.22, p.torsoY, enemy.w*.34, p.torsoY+18, "#d0d1ce", 3);
  ctx.strokeStyle = "#d0d1ce";
  ctx.lineWidth = 3;
  ctx.beginPath(); ctx.arc(enemy.w*.28, p.torsoY+20, 8, -.5, 1.6); ctx.stroke();
}

function drawStoneGuardian(ctx, enemy, time, phase) {
  const step = Math.sin(time * 5.4 + phase);
  const settle = Math.abs(Math.sin(time * 5.4 + phase)) * 1.3;
  shadow(ctx, enemy.w * .42, .28);
  ctx.save();
  ctx.translate(0, -settle);
  const stone = enemy.elite ? "#7f6e65" : "#766f68";
  const dark = "#4f4a46";
  polygon(ctx, [[-15,-38],[-9,-52],[10,-50],[17,-35],[14,-16],[-13,-15]], stone, dark, 2);
  polygon(ctx, [[-12,-54],[-7,-65],[8,-66],[14,-54],[8,-45],[-9,-46]], "#847c74", dark, 2);
  polygon(ctx, [[-17,-36],[-26,-27],[-22,-10],[-12,-18]], "#68615b", dark, 2);
  polygon(ctx, [[17,-35],[26,-27],[22,-9],[12,-18]], "#68615b", dark, 2);
  line(ctx, -8, -15, -11 + step*3, -1, dark, 8);
  line(ctx, 8, -15, 11 - step*3, -1, dark, 8);
  ellipse(ctx, -5, -57, 2.2, 2.2, "#e5c56c");
  ellipse(ctx, 5, -57, 2.2, 2.2, "#e5c56c");
  // Animated cracks.
  ctx.strokeStyle = enemy.elite ? "#e2b45b" : "#9b8f80";
  ctx.lineWidth = 1.2;
  ctx.beginPath(); ctx.moveTo(0,-47); ctx.lineTo(-4,-35); ctx.lineTo(2,-29); ctx.lineTo(-1,-19); ctx.stroke();
  ctx.restore();
}

function drawImp(ctx, enemy, time, phase) {
  const hop = Math.max(0, Math.sin(time * 8.7 + phase)) * 5;
  const sway = Math.sin(time * 8.7 + phase) * 2;
  shadow(ctx, enemy.w*.28);
  ctx.save(); ctx.translate(0, -hop);
  ellipse(ctx, 0, -enemy.h*.42, enemy.w*.25, enemy.h*.25, enemy.elite ? "#7b2731" : "#743038", sway*.02);
  ellipse(ctx, 0, -enemy.h*.73, enemy.w*.19, enemy.h*.15, "#8f3a3f");
  polygon(ctx, [[-8,-enemy.h*.82],[-14,-enemy.h*.98],[-2,-enemy.h*.85]], "#4b2026");
  polygon(ctx, [[8,-enemy.h*.82],[14,-enemy.h*.98],[2,-enemy.h*.85]], "#4b2026");
  line(ctx, -8, -enemy.h*.28, -11+sway, -2, "#512129", 5);
  line(ctx, 8, -enemy.h*.28, 11-sway, -2, "#512129", 5);
  line(ctx, -enemy.w*.20, -enemy.h*.46, -enemy.w*.34-sway, -enemy.h*.28, "#512129", 4);
  line(ctx, enemy.w*.20, -enemy.h*.46, enemy.w*.34+sway, -enemy.h*.28, "#512129", 4);
  ellipse(ctx, -4, -enemy.h*.74, 2, 2, "#ffc562");
  ellipse(ctx, 4, -enemy.h*.74, 2, 2, "#ffc562");
  // Tail.
  ctx.strokeStyle = "#5d252e"; ctx.lineWidth = 3;
  ctx.beginPath(); ctx.moveTo(-8,-enemy.h*.42); ctx.quadraticCurveTo(-22,-enemy.h*.34,-17+sway,-enemy.h*.17); ctx.stroke();
  polygon(ctx, [[-17+sway,-enemy.h*.17],[-23+sway,-enemy.h*.23],[-24+sway,-enemy.h*.13]], "#5d252e");
  ctx.restore();
}

function drawFireSkull(ctx, enemy, time, phase) {
  const hover = Math.sin(time * 3.5 + phase) * 6;
  const pulse = .75 + Math.sin(time * 7 + phase) * .12;
  ctx.save(); ctx.translate(0, hover);
  ctx.globalAlpha *= .42;
  ellipse(ctx, 0, -enemy.h*.46, enemy.w*.44, enemy.h*.38, "#ff6a35");
  ctx.globalAlpha /= .42;
  for (let i = -2; i <= 2; i++) {
    const flame = 7 + (i%2 ? 3 : 0) + Math.sin(time*9+phase+i)*3;
    polygon(ctx, [[i*6-4,-enemy.h*.67],[i*6,-enemy.h*.67-flame],[i*6+4,-enemy.h*.66]], i%2 ? "#ffb24d" : "#f05a31");
  }
  ellipse(ctx, 0, -enemy.h*.48, enemy.w*.26, enemy.h*.22, "#d5c7ad");
  ctx.fillStyle = "#342329";
  ctx.beginPath(); ctx.ellipse(-6,-enemy.h*.51,4,5,0,0,TAU); ctx.fill();
  ctx.beginPath(); ctx.ellipse(6,-enemy.h*.51,4,5,0,0,TAU); ctx.fill();
  polygon(ctx, [[-3,-enemy.h*.44],[0,-enemy.h*.40],[3,-enemy.h*.44]], "#4a3331");
  ctx.fillStyle = `rgba(255,194,83,${pulse})`;
  ctx.fillRect(-7,-enemy.h*.37,4,3); ctx.fillRect(-1,-enemy.h*.36,3,3); ctx.fillRect(4,-enemy.h*.37,4,3);
  ctx.restore();
}

function drawWraith(ctx, enemy, time, phase) {
  const hover = Math.sin(time * 2.4 + phase) * 7;
  const sway = Math.sin(time * 3 + phase) * 3;
  shadow(ctx, enemy.w*.30, .10);
  ctx.save(); ctx.translate(sway, hover);
  const bodyGradient = ctx.createLinearGradient(0, -enemy.h*.8, 0, 0);
  bodyGradient.addColorStop(0, "rgba(126,137,165,.88)");
  bodyGradient.addColorStop(1, "rgba(126,137,165,.05)");
  polygon(ctx, [[0,-enemy.h*.84],[-enemy.w*.25,-enemy.h*.60],[-enemy.w*.30,-enemy.h*.28],[-enemy.w*.18,-2],[0,-enemy.h*.16],[enemy.w*.18,-2],[enemy.w*.30,-enemy.h*.28],[enemy.w*.25,-enemy.h*.60]], bodyGradient);
  ctx.fillStyle = "#343746";
  ctx.beginPath(); ctx.arc(0,-enemy.h*.73,enemy.w*.22,Math.PI,TAU); ctx.lineTo(enemy.w*.15,-enemy.h*.58); ctx.lineTo(-enemy.w*.15,-enemy.h*.58); ctx.closePath(); ctx.fill();
  ellipse(ctx, -5, -enemy.h*.69, 2, 2, "#c9e8ff");
  ellipse(ctx, 5, -enemy.h*.69, 2, 2, "#c9e8ff");
  line(ctx, -enemy.w*.18,-enemy.h*.53,-enemy.w*.38-sway,-enemy.h*.38,"rgba(151,171,201,.62)",4);
  line(ctx, enemy.w*.18,-enemy.h*.53,enemy.w*.38+sway,-enemy.h*.38,"rgba(151,171,201,.62)",4);
  ctx.restore();
}

function drawUnknown(ctx, enemy, time, phase) {
  const pulse = Math.sin(time * 5 + phase) * 1.5;
  shadow(ctx, enemy.w*.32);
  ctx.fillStyle = enemy.elite ? "#8d304a" : "#5b5662";
  ctx.beginPath();
  ctx.roundRect(-enemy.w/2, -enemy.h + pulse, enemy.w, enemy.h, 10);
  ctx.fill();
  ellipse(ctx, -enemy.w*.16, -enemy.h*.72 + pulse, 2.5, 2.5, "#f3d7b5");
  ellipse(ctx, enemy.w*.16, -enemy.h*.72 + pulse, 2.5, 2.5, "#f3d7b5");
}

const DRAWERS = {
  wanderer: drawWanderer,
  hound: (ctx, enemy, time, phase) => drawHound(ctx, enemy, time, phase, false),
  legionary: (ctx, enemy, time, phase) => drawLegionary(ctx, enemy, time, phase, "legionary"),
  "shield-bearer": drawShieldBearer,
  javelin: drawJavelin,
  "temple-guard": (ctx, enemy, time, phase) => drawLegionary(ctx, enemy, time, phase, "temple-guard"),
  zealot: drawZealot,
  "stone-guardian": drawStoneGuardian,
  imp: drawImp,
  "brim-hound": (ctx, enemy, time, phase) => drawHound(ctx, enemy, time, phase, true),
  "fire-skull": drawFireSkull,
  wraith: drawWraith,
};

export function drawEnemy(ctx, enemy, time = 0) {
  const phase = phaseFor(enemy);
  const alpha = enemy.dead ? Math.max(0, (enemy.fade || 0) / 0.3) : 1;
  const facing = enemy.patrol || 1;
  const drawer = DRAWERS[enemy.type] || drawUnknown;

  ctx.save();
  ctx.globalAlpha = alpha;
  ctx.translate(enemy.x + enemy.w / 2, enemy.y + enemy.h);
  ctx.scale(facing < 0 ? -1 : 1, 1);
  eliteAura(ctx, enemy, time, phase);
  drawer(ctx, enemy, time, phase);
  ctx.restore();
}
