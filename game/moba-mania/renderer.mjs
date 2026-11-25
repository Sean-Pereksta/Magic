import { WORLD, HEROES } from './data.mjs';
import { createEffectBudget, resetEffectBudget, drawCombatBurst, drawShieldShell, drawProjectileTrail, drawSpellAccent, isEffectVisible, EFFECT_PALETTES } from './combat-visuals.mjs';
import { unitPose, summonPose, drawHitGlint, drawMonsterRocks } from './unit-animation.mjs';
import { drawCampScenery, drawReactiveTerrain, drawStructureDamage, isWorldEffect, drawWorldEffect, drawGoldFlight } from './world-visuals.mjs';
import { EXPANSION_HEROES, drawExpansionHero, drawDevice, drawMagazine, drawResonanceResource, drawExpansionUnitAccents, drawExpansionIcon, drawExpansionWarning, drawExpansionAim, drawExpansionProjectile, drawExpansionEffect, drawExpansionZones } from './expansion-visuals.mjs';

// All artwork is authored here in pixels. No remote textures, fonts or image
// decoding are needed: the same sprites work in the site and the offline pack.
const TEAM = ['#67dfef', '#ff8f87'];
const TEAM_DARK = ['#236d97', '#983f58'];
const INK = '#152838';
const spriteCache = new Map();
function cacheSprite(key, sprite) {
  // Quantized poses and a hard ceiling keep long sessions and cosmetic changes
  // from retaining an unlimited collection of little canvases.
  if (spriteCache.size >= 1200) spriteCache.delete(spriteCache.keys().next().value);
  spriteCache.set(key, sprite); return sprite;
}
const clamp = (v, a, b) => Math.max(a, Math.min(b, v));
const fract = v => v - Math.floor(v);
const hash = (x, y = 0) => fract(Math.sin(x * 127.1 + y * 311.7) * 43758.5453);
const alive = u => u.alive !== false && u.hp > 0;
const cosmeticColor = value => typeof value === 'string' && /^#[0-9a-f]{6}$/i.test(value) ? value : null;
function blendColor(base, tint, weight = .36) {
  if (!cosmeticColor(tint)) return base;
  const rgb = color => [1, 3, 5].map(i => parseInt(color.slice(i, i + 2), 16));
  const a = rgb(base), b = rgb(tint);
  return '#' + a.map((v, i) => Math.round(v * (1 - weight) + b[i] * weight).toString(16).padStart(2, '0')).join('');
}
const makeCanvas = (w, h) => {
  const c = document.createElement('canvas');
  c.width = w; c.height = h;
  c.getContext('2d').imageSmoothingEnabled = false;
  return c;
};
function box(c, color, x, y, w, h) {
  c.fillStyle = c._mobaArmorColors?.has(color) ? blendColor(color, c._mobaArmorTint, .62) : color;
  c.fillRect(Math.round(x), Math.round(y), Math.max(1, Math.round(w)), Math.max(1, Math.round(h)));
}
function line(c, color, x, y, x2, y2, width = 2) {
  c.strokeStyle = color; c.lineWidth = width;
  c.beginPath(); c.moveTo(x, y); c.lineTo(x2, y2); c.stroke();
}
function path(c, points, color, width) {
  if (!points?.length) return;
  c.strokeStyle = color; c.lineWidth = width;
  c.lineCap = 'round'; c.lineJoin = 'round'; c.beginPath();
  points.forEach((p, i) => i ? c.lineTo(p.x, p.y) : c.moveTo(p.x, p.y)); c.stroke();
}
function disc(c, color, x, y, r) {
  // Eight-sided contours retain an intentional pixel silhouette when scaled.
  c.fillStyle = color; c.beginPath();
  for (let i = 0; i < 8; i++) {
    const a = i * Math.PI / 4 + Math.PI / 8;
    const px = Math.round((x + Math.cos(a) * r) / 2) * 2;
    const py = Math.round((y + Math.sin(a) * r) / 2) * 2;
    i ? c.lineTo(px, py) : c.moveTo(px, py);
  }
  c.closePath(); c.fill();
}
function ring(c, color, x, y, r, width = 2, dashed = false) {
  c.strokeStyle = color; c.lineWidth = width;
  if (dashed) c.setLineDash([7, 7]);
  c.beginPath();
  const n = Math.max(12, Math.min(48, Math.floor(r / 4)));
  for (let i = 0; i <= n; i++) {
    const a = i / n * Math.PI * 2;
    const px = Math.round((x + Math.cos(a) * r) / 2) * 2;
    const py = Math.round((y + Math.sin(a) * r) / 2) * 2;
    i ? c.lineTo(px, py) : c.moveTo(px, py);
  }
  c.stroke(); c.setLineDash([]);
}
function diamond(c, x, y, w, h, color, highlight = '#e5fcff') {
  c.fillStyle = INK; c.beginPath();
  c.moveTo(x, y - h / 2 - 3); c.lineTo(x + w / 2 + 3, y);
  c.lineTo(x, y + h / 2 + 3); c.lineTo(x - w / 2 - 3, y); c.closePath(); c.fill();
  c.fillStyle = color; c.beginPath();
  c.moveTo(x, y - h / 2); c.lineTo(x + w / 2, y);
  c.lineTo(x, y + h / 2); c.lineTo(x - w / 2, y); c.closePath(); c.fill();
  c.fillStyle = highlight; c.beginPath();
  c.moveTo(x, y - h / 2); c.lineTo(x, y + h / 2);
  c.lineTo(x - w / 2, y); c.closePath(); c.fill();
  line(c, '#ffffffaa', x, y - h / 2 + 6, x + w / 3, y - 1, 2);
}

function heroSprite(id, frame, facing, attacking, cosmetics = {}, poseStage = 0, aimDirection = 0, casting = false, variant = '') {
  frame=poseStage&&poseStage<5?0:frame%4;aimDirection=poseStage?aimDirection:0;casting=poseStage?casting:false;
  const tint = cosmeticColor(cosmetics?.armorTint);
  const key = ['hero', id, frame, facing, attacking, poseStage, aimDirection, casting, variant, tint || 'classic'].join(':');
  if (spriteCache.has(key)) return spriteCache.get(key);
  const expanded = id === 'bulwark' || id === 'ember' || id === 'seraph';
  // Leave room for wings, shields and the greatsword at the end of its swing.
  const spriteWidth = id==='brinehook'?96:id==='vela'?88:EXPANSION_HEROES[id]?80:id === 'druid' ? 88 : id === 'arc' || id === 'seraph' ? 80 : id === 'ranger' ? 72 : expanded ? 64 : 60;
  const out = makeCanvas(spriteWidth, 68), c = out.getContext('2d');
  out.groundPadding = 12; out.bodyTop = 12;
  if(EXPANSION_HEROES[id]){
    if(facing===2){c.translate(spriteWidth,0);c.scale(-1,1);}
    c.translate((spriteWidth-40)/2,12);
    drawExpansionHero(c,id,{frame,stage:poseStage,direction:aimDirection,back:facing===3,casting,variant,tint});
    return cacheSprite(key,out);
  }
  if (tint) {
    const palettes = {
      arc:['#bd8738','#ffe18a','#dfab42','#ffc75f','#ca9336','#ffe398','#8b6530','#ffda85'],
      dragon:['#ae3b47','#ef785d','#c34647','#ff9d68','#782b38','#ef805d'],
      druid:['#124e3b','#3d9f62','#83d880','#a9d9a0','#275e42','#82c86a','#69b970','#a0da83'],
      angel:['#8f487e','#e58db2','#ffc9db','#e8a4b6','#f3d2cd','#d37f9f'],
      ranger:['#3a354d','#5c4d73','#45435e','#9a75a7'],
      bulwark:['#734d9e','#b99ce9','#e1c9fa','#ac84d7','#e6cbff','#4e3674','#b094d8','#694887','#795799','#c2a2eb','#e9d4ff','#482f65','#b9a0dc','#7e4fbb','#cab0ed','#bda2e6','#a785d7','#5a3e83','#805cae'],
      ember:['#6c382e','#df8035','#ffbd6a','#f4b45a','#a34c2c','#ed8736','#ffc273','#e59345','#6b392e','#b45331','#f3a34f'],
      seraph:['#1e4d85','#95cdeb','#3977bd','#a8ddf9','#4f94d6','#87c7ff','#bfddeb','#a6d2e8','#27518d','#7cb9e1'],
    };
    c._mobaArmorColors = new Set(palettes[id] || []); c._mobaArmorTint = tint;
  }
  const back = facing === 3, left = facing === 2;
  if (left) { c.translate(spriteWidth, 0); c.scale(-1, 1); }
  c.translate((spriteWidth - 40) / 2, 12);
  const walk = poseStage ? 0 : [0, 1, 0, -1][frame % 4], bob = poseStage ? 0 : frame % 2;
  const strike = poseStage ? [0,-3,7,4,1][poseStage] : attacking ? [0, 3, 5, 2][frame % 4] : 0;
  const aimAngle = aimDirection * Math.PI / 4;
  const weaponAim = Math.atan2(Math.sin(aimAngle), Math.abs(Math.cos(aimAngle))) + Math.PI / 2;
  const swing = poseStage === 1 ? weaponAim - 1.8 : poseStage === 2 ? weaponAim : poseStage === 3 ? weaponAim + .45 : poseStage === 4 ? .45 : .15;
  c.translate(0, -bob);
  // Mantle, outlined boots and alternately planted feet.
  const colors = {
    arc: ['#183c73', '#2264ac', '#e8b849', '#ffed9b'],
    dragon: ['#6f2942', '#a9434b', '#f37657', '#ffce85'],
    druid: ['#175540', '#25835b', '#83c966', '#cff394'],
    angel: ['#a04a85', '#e38cb4', '#ffd1e0', '#fff4d7'],
    ranger: ['#25283d', '#45435e', '#9a75a7', '#a7ed8d'],
    bulwark: ['#38295b', '#63428c', '#a580d6', '#e4c8ff'],
    ember: ['#69372b', '#b65b2e', '#f39b43', '#ffe2a1'],
    seraph: ['#1c4177', '#356cb2', '#8ac9ed', '#e6f9ff'],
  }[id] || ['#375064', '#547b8a', '#8fc6d1', '#e4f7df'];
  if (tint) for (let i = 0; i < colors.length; i++) colors[i] = blendColor(colors[i], tint);
  box(c, INK, 9, 19, 18, 18);
  box(c, colors[0], 10, 19, 16, 16);
  box(c, colors[1], 11 + walk, 22, 5, 12);
  box(c, colors[0], 23, 29 + walk, 5, 7);
  box(c, INK, 13, 31 + walk, 5, 7);
  box(c, INK, 21, 31 - walk, 5, 7);
  box(c, '#607883', 14, 32 + walk, 3, 3);
  box(c, '#3f5361', 22, 32 - walk, 3, 3);
  box(c, '#263748', 12, 37 + walk, 7, 2);
  box(c, '#263748', 21, 37 - walk, 7, 2);
  if (poseStage === 1 || poseStage === 2) { box(c, INK, 10, 37, 10, 2); box(c, INK, 22, 37, 10, 2); }
  c.save(); c.translate(poseStage === 1 ? -1 : poseStage === 2 ? 2 : poseStage === 3 ? 1 : 0, poseStage === 1 ? 1 : 0);
  if (id === 'angel') {
    // Three articulated feather rows visibly beat during travel.
    const flap = poseStage ? [0,-3,-7,-4,-1][poseStage] : walk * 2;
    box(c, INK, 2, 13 + flap, 9, 17);
    box(c, '#ac76a0', 3, 15 + flap, 7, 14);
    box(c, '#fff0e6', 4, 14 + flap, 4, 12);
    box(c, '#f2bed9', 8, 20 + flap, 4, 10);
    box(c, '#fff5e7', 2, 10 + flap, 4, 13);
    box(c, '#f3cee1', 1, 9 + flap, 3, 8);
    box(c, INK, 28, 13 + flap, 9, 17);
    box(c, '#ac76a0', 29, 15 + flap, 7, 14);
    box(c, '#fff0e6', 31, 14 + flap, 4, 12);
    box(c, '#f2bed9', 26, 20 + flap, 5, 10);
    box(c, '#fff5e7', 34, 10 + flap, 4, 13);
    box(c, '#f3cee1', 36, 9 + flap, 3, 8);
  }
  if (id === 'seraph') {
    const flap = poseStage ? [0,3,-5,-2,0][poseStage] : walk * 2;
    // A broader, layered feather silhouette distinguishes the sword angel
    // from the rose support, including folded feather tips on the back view.
    for (const side of [-1, 1]) {
      const anchor = (side < 0 ? 11 : 27) + (poseStage === 2 ? side * 3 : 0);
      for (let feather = 0; feather < 4; feather++) {
        const x = anchor + side * (5 + feather * 4);
        const y = 12 + feather * 2 + flap;
        box(c, '#163451', x - 2, y - 3, 7, 18 - feather * 2);
        box(c, '#5b98d2', x - 1, y - 2, 5, 15 - feather * 2);
        box(c, feather % 2 ? '#d0f1ff' : '#f0fcff', x, y - 2, 3, 11 - feather);
        box(c, '#92caf1', x + (side < 0 ? -2 : 2), y - 5, 3, 7);
      }
      box(c, '#d4f2ff', anchor, 19 + flap, 4, 11);
    }
  }
  box(c, INK, 10, 17, 17, 13);
  box(c, colors[2], 12, 18, 13, 9);
  box(c, colors[1], 15, 24, 10, 6);
  box(c, colors[3], 12, 18, 5, 3);
  box(c, '#e4c280', 12, 28, 14, 2);
  box(c, '#fff3b0', 17, 27, 3, 4);
  // Head and face are built from real pixels, including an eye highlight.
  box(c, INK, 12, 7, 14, 12);
  box(c, '#d89c75', 14, 10, 10, 8);
  box(c, '#ffcf98', 14, 10, 7, 6);
  if (!back) {
    box(c, '#48352f', 20, 13, 5, 2);
    box(c, '#ffffdd', 21, 13, 2, 1);
    box(c, '#ab7257', 22, 17, 3, 1);
  } else box(c, colors[0], 13, 9, 13, 11);
  if (id === 'arc') {
    box(c, INK, 11, 7, 16, 7);
    box(c, '#bd8738', 12, 7, 14, 5);
    box(c, '#ffe18a', 13, 7, 9, 3);
    box(c, '#dfab42', 11, 11, 4, 7);
    box(c, '#ffe18a', 12, 12, 2, 3);
    box(c, '#ffc75f', 25, 10, 2, 8);
    box(c, '#fff2a4', 17, 5, 3, 4);
    box(c, '#489edc', 17, 3, 3, 3);
    box(c, '#89ecff', 18, 2 - bob, 2, 3);
    box(c, '#ca9336', 8, 18, 6, 5);
    box(c, '#ffe398', 8, 17, 6, 2);
    box(c, '#8b6530', 26, 19, 4, 8);
    box(c, '#ffda85', 25, 18, 5, 4);
    c.save(); c.translate(31,23); c.rotate(poseStage ? swing * .82 : 0);
    box(c, INK, 0, -17, 3, 29); box(c, '#c59b55', 1, -13, 1, 24);
    box(c, '#ffe8a1', -2, -13, 7, 2); box(c, '#287fc7', -2, -20 + bob, 7, 7);
    box(c, '#6ce1ff', -1, -19, 5, 4); box(c, '#e0fcff', 1, -20, 2, 4);
    box(c, '#3687e5', 1, -23, 2, 3); box(c, '#4094e4', -3, -18 - bob, 2, 4);
    if (poseStage === 1 || poseStage === 2) { box(c, '#e7fcff', -4, -22, 2, 7); box(c, '#7ce3ff', 6, -21, 3, 5); }
    c.restore();
  } else if (id === 'dragon') {
    box(c, INK, 11, 6, 16, 7);
    box(c, '#ae3b47', 12, 7, 14, 5);
    box(c, '#ef785d', 13, 7, 7, 2);
    box(c, '#ffcd8a', 10, 3, 3, 6);
    box(c, '#ffe4b2', 9, 2, 2, 4);
    box(c, '#ffcd8a', 25, 2, 3, 6);
    box(c, '#ffe4b2', 28, 1, 2, 3);
    box(c, '#c34647', 11, 11, 4, 7);
    box(c, '#ff9d68', 12, 13, 2, 3);
    box(c, '#782b38', 7, 18, 7, 9);
    box(c, '#ef805d', 8, 18, 6, 3);
    box(c, '#f0c484', 7, 16, 2, 3);
    box(c, '#f0c484', 10, 15, 2, 4);
    // Wide dragon blade arcs away from the body during the strike.
    c.save(); c.translate(30, 26); c.rotate(poseStage ? swing : attacking ? -0.8 + strike * 0.24 : 0.15);
    box(c, INK, -3, -17, 7, 25);
    box(c, '#a95743', -1, 1, 3, 7);
    box(c, '#ffcb7c', -5, -1, 10, 3);
    box(c, '#bdced3', -2, -16, 5, 16);
    box(c, '#edf3dd', -2, -17, 2, 15);
    box(c, '#fa8458', 2, -13, 2, 9);
    c.restore();
    if (casting && poseStage) { box(c, '#ffc97d', 13, 22, 2, 5); box(c, '#ffad55', 20, 20, 5, 2); box(c, '#ffe4a1', 9, 18, 4, 2); }
  } else if (id === 'druid') {
    box(c, '#124e3b', 11, 6, 16, 7);
    box(c, '#3d9f62', 12, 7, 14, 4);
    box(c, '#83d880', 13, 6, 5, 3);
    box(c, '#a9d9a0', 14, 18, 7, 3);
    box(c, '#d0e9c4', 17, 19, 4, 4);
    box(c, '#754f37', 11, 0, 2, 9);
    box(c, '#b99b60', 8, 1, 3, 2);
    box(c, '#967547', 9, 3, 4, 2);
    box(c, '#b99b60', 8, 0, 1, 4);
    box(c, '#754f37', 25, 0, 2, 9);
    box(c, '#b99b60', 27, 2, 4, 2);
    box(c, '#b99b60', 30, 0, 2, 3);
    box(c, '#275e42', 8, 18, 6, 8);
    box(c, '#82c86a', 8, 18, 5, 2);
    const sx = 32 + (poseStage === 1 ? -3 : poseStage === 2 ? 0 : strike);
    c.save(); c.translate(sx, 32); c.rotate(casting ? (poseStage === 1 ? -.22 : poseStage === 2 ? 0 : .1) : poseStage ? swing * .65 : 0);
    const rootX = sx;
    c.translate(-rootX, -32);
    box(c, INK, sx, 8, 3, 28);
    box(c, '#967449', sx + 1, 10, 2, 25);
    box(c, '#b29b58', sx - 2, 8, 5, 3);
    box(c, '#21573f', sx - 4, 4, 9, 5);
    box(c, '#63c76c', sx - 3, 3, 7, 4);
    box(c, '#b1f0a2', sx - 1, 3, 3, 2);
    if (casting && poseStage === 2) { box(c, '#b4f59c', sx - 4, 36, 12, 2); box(c, '#76d38b', sx - 6, 38, 16, 2); }
    c.restore();
    box(c, '#69b970', 10, 26, 4, 6);
    box(c, '#a0da83', 11, 27, 2, 3);
  } else if (id === 'angel') {
    box(c, '#8f487e', 11, 7, 16, 6);
    box(c, '#e58db2', 12, 7, 14, 4);
    box(c, '#ffc9db', 13, 7, 9, 2);
    box(c, '#e8a4b6', 11, 12, 3, 8);
    box(c, '#ffe5a0', 13, 2, 13, 2);
    box(c, '#ffdc7e', 11, 3, 3, 3);
    box(c, '#ffdc7e', 25, 3, 3, 3);
    box(c, '#fff4b9', 14, 5, 11, 1);
    box(c, '#fff3df', 14, 22, 9, 4);
    box(c, '#f3d2cd', 14, 30, 11, 6);
    box(c, '#fff1e4', 14, 30, 4, 5);
    box(c, '#d37f9f', 25, 20, 4, 7);
    const handY = poseStage === 1 ? 16 : poseStage === 2 ? 12 : poseStage === 3 ? 16 : 24;
    box(c, '#ffdab4', 27 + Math.max(0,strike), handY, 4, 3);
    box(c, '#e086d0', 29 + Math.max(0,strike), handY - 7, 4, 4);
    box(c, '#ffe4fa', 30 + Math.max(0,strike), handY - 8, 2, 5);
    if (casting && poseStage) { box(c, '#fff1c1', 7, handY, 4, 3); box(c, '#ffb9e2', 5, handY - 7, 5, 5); }
  } else if (id === 'bulwark') {
    // Broad pauldrons, a closed helm and a shield almost as tall as the body.
    box(c, INK, 7, 17, 24, 14);
    box(c, '#734d9e', 8, 18, 23, 11);
    box(c, '#b99ce9', 8, 17, 8, 5); box(c, '#e1c9fa', 9, 18, 5, 2);
    box(c, '#ac84d7', 24, 17, 8, 6); box(c, '#e6cbff', 25, 18, 5, 2);
    box(c, '#4e3674', 13, 21, 14, 9); box(c, '#b094d8', 14, 22, 11, 4);
    box(c, '#edddff', 18, 22, 3, 6); box(c, '#694887', 13, 30, 13, 4);
    box(c, INK, 10, 6, 18, 14); box(c, '#795799', 11, 6, 16, 13);
    box(c, '#c2a2eb', 12, 6, 10, 4); box(c, '#e9d4ff', 13, 7, 6, 2);
    box(c, '#482f65', 12, 12, 14, 4);
    if (!back) box(c, '#d0f6ff', 15, 13, 9, 1);
    box(c, '#b9a0dc', 18, 11, 3, 9); box(c, '#e9d4ff', 19, 11, 1, 7);
    box(c, '#7e4fbb', 16, 1, 7, 5); box(c, '#cab0ed', 17, 0, 5, 3);
    const shieldX = poseStage === 1 ? 0 : poseStage === 2 ? 22 : poseStage === 3 ? 16 : poseStage === 4 ? 5 : 1;
    box(c, INK, shieldX, 19, 13, 18); box(c, INK, shieldX + 3, 35, 7, 5);
    box(c, '#bda2e6', shieldX + 1, 20, 11, 16); box(c, '#a785d7', shieldX + 4, 35, 5, 3);
    box(c, '#5a3e83', shieldX + 3, 22, 7, 12); box(c, '#805cae', shieldX + 4, 23, 5, 9);
    diamond(c, shieldX + 6, 28, 5, 9, '#9975cf', '#f1dcff');
    // Square-headed hammer rotates with the heavy attack, remaining in frame.
    c.save(); c.translate(31, 25); c.rotate(poseStage ? swing * .75 : attacking ? -0.7 + strike * 0.19 : 0.18);
    box(c, INK, -2, -14, 5, 24); box(c, '#b6a2c8', -1, -11, 2, 20);
    box(c, INK, -6, -18, 13, 8); box(c, '#947ab7', -5, -17, 11, 6);
    box(c, '#e3d1f5', -4, -17, 8, 2); box(c, '#c6abf1', 3, -15, 3, 3);
    c.restore();
  } else if (id === 'ember') {
    // Lean wrapped torso and bright cowl; both daggers have independent arcs.
    box(c, '#6c382e', 13, 21, 12, 10); box(c, '#df8035', 12, 19, 12, 5);
    box(c, '#ffbd6a', 12, 19, 5, 2); box(c, '#f4b45a', 17, 25, 4, 7);
    box(c, INK, 10, 5, 18, 14); box(c, '#a34c2c', 11, 6, 16, 11);
    box(c, '#ed8736', 12, 6, 13, 5); box(c, '#ffc273', 13, 6, 8, 2);
    box(c, '#e59345', 10, 11, 5, 10); box(c, '#6b392e', 14, 15, 13, 6);
    if (!back) { box(c, '#ffd782', 18, 12, 7, 2); box(c, '#fff4c9', 21, 12, 3, 1); }
    const scarf = 6 + (attacking ? 3 : walk * 2);
    box(c, '#b45331', 7 - scarf, 17, scarf + 7, 4); box(c, '#f3a34f', 7 - scarf, 17, scarf + 2, 2);
    for (const side of [-1, 1]) {
      c.save(); c.translate(side < 0 ? 10 : 29 + Math.max(0,strike), side < 0 ? 26 : 23);
      c.rotate(poseStage ? side < 0 ? -.35 - poseStage * .08 : swing : side * (attacking ? .9 - strike * .27 : .35));
      box(c, INK, -2, -10, 5, 17); box(c, '#e9f0e5', -1, -10, 3, 11);
      box(c, '#82a3a8', 1, -7, 1, 8); box(c, '#ffba64', -4, 0, 9, 2);
      box(c, '#9b5734', -1, 2, 3, 5); box(c, '#ffe1ac', -1, -10, 1, 9); c.restore();
    }
    if (attacking) { box(c, '#ffd89b', 32 + strike, 12, 2, 2); box(c, '#ffad56', 4, 31, 3, 2); }
  } else if (id === 'seraph') {
    box(c, '#1e4d85', 11, 8, 17, 6); box(c, '#95cdeb', 12, 7, 14, 5);
    box(c, '#effcff', 13, 7, 9, 2); box(c, '#3977bd', 10, 11, 5, 8);
    box(c, '#a8ddf9', 11, 12, 2, 5); box(c, '#4f94d6', 24, 10, 3, 8);
    box(c, '#87c7ff', 13, 1, 13, 2); box(c, '#d9f7ff', 11, 2, 3, 3);
    box(c, '#d9f7ff', 25, 2, 3, 3); box(c, '#edfaff', 14, 4, 11, 1);
    box(c, '#bfddeb', 9, 18, 7, 5); box(c, '#effbff', 9, 18, 5, 2);
    box(c, '#a6d2e8', 24, 18, 7, 6); box(c, '#ebf9ff', 25, 18, 5, 2);
    box(c, '#27518d', 14, 26, 10, 9); box(c, '#7cb9e1', 14, 28, 3, 7);
    // The oversized greatsword is drawn in full across the entire swing.
    box(c, '#e0edeb', poseStage === 1 ? 23 : 27, 24, 5, 3);
    c.save(); c.translate(29, 27); c.rotate(poseStage ? swing : attacking ? -.85 + strike * .36 : .17);
    box(c, INK, -4, -25, 10, 35); box(c, '#4387b9', -3, -25, 7, 25);
    box(c, '#c5eaf6', -2, -24, 5, 23); box(c, '#f5fdff', -2, -24, 2, 22);
    box(c, '#72bce9', 2, -20, 2, 18); box(c, '#dff7ff', -1, -27, 3, 4);
    box(c, '#b8def0', -7, -1, 15, 4); box(c, '#effcff', -5, -1, 11, 1);
    box(c, '#2e5488', -1, 3, 4, 7); box(c, '#d3efff', -2, 9, 6, 3);
    c.restore();
  } else {
    box(c, INK, 10, 5, 17, 12);
    box(c, '#3a354d', 11, 6, 15, 7);
    box(c, '#5c4d73', 12, 6, 8, 2);
    box(c, '#3a354d', 11, 12, 4, 8);
    box(c, '#332d45', 14, 17, 12, 4);
    if (!back) { box(c, '#91db7e', 20, 13, 5, 2); box(c, '#e1ffba', 21, 13, 2, 1); }
    box(c, '#788552', 8, 17, 5, 7);
    box(c, '#a1b975', 8, 17, 4, 2);
    const bx = 31 + (poseStage === 1 ? 0 : poseStage === 2 ? 3 : poseStage === 3 ? 2 : 0);
    c.save(); c.translate(bx,24); c.rotate(poseStage ? weaponAim - Math.PI / 2 : 0); c.translate(-bx,-24);
    line(c, INK, bx, 13, bx + 5, 19, 3);
    line(c, INK, bx + 5, 19, bx + 5, 28, 3);
    line(c, INK, bx + 5, 28, bx, 34, 3);
    line(c, '#bca071', bx, 13, bx + 5, 19, 1);
    line(c, '#bca071', bx + 5, 19, bx + 5, 28, 1);
    line(c, '#bca071', bx + 5, 28, bx, 34, 1);
    const draw = poseStage === 1 ? 9 : poseStage === 4 ? 3 : 0;
    line(c, '#dbdfc2', bx, 13, bx - draw, 24, 1);
    line(c, '#dbdfc2', bx - draw, 24, bx, 34, 1);
    if (poseStage !== 2 && poseStage !== 3) { box(c, '#dec593', bx - 7 - draw, 23, 14 + draw, 1); box(c, '#8bef6e', bx + 6, 22, 3, 3); }
    box(c, '#e6bb91', bx - draw - 3, 23, 4, 3); c.restore();
  }
  c.restore(); return cacheSprite(key, out);
}

function soldierSprite(kind, team, frame, flip = false) {
  const key = ['soldier', kind, team, frame, flip].join(':');
  if (spriteCache.has(key)) return spriteCache.get(key);
  const out = makeCanvas(26, 32), c = out.getContext('2d');
  if (flip) { c.translate(26, 0); c.scale(-1, 1); }
  const w = [0, 1, 0, -1][frame % 4], cloth = TEAM[team] || '#a2c776';
  box(c, INK, 8, 19, 5, 10 + w); box(c, INK, 15, 19, 5, 10 - w);
  box(c, '#718a91', 9, 21 + w, 3, 5); box(c, '#526774', 16, 21 - w, 3, 5);
  box(c, INK, 7, 11, 13, 12); box(c, TEAM_DARK[team] || '#526748', 8, 12, 11, 10);
  box(c, cloth, 8, 12, 5, 7); box(c, '#d1ceaa', 8, 20, 11, 2);
  box(c, INK, 8, 3, 12, 10); box(c, '#e4b386', 9, 5, 10, 7);
  box(c, '#f5cf9b', 10, 6, 7, 4); box(c, '#263241', 16, 8, 3, 2);
  if (kind === 'footman') {
    box(c, '#516574', 8, 3, 12, 5); box(c, '#b0c5c7', 9, 3, 8, 2);
    box(c, '#71949e', 8, 7, 3, 6); box(c, cloth, 12, 1, 4, 3);
    box(c, INK, 3, 14, 6, 12); box(c, '#b4b2a1', 4, 14, 5, 10);
    box(c, cloth, 5, 15, 3, 7); box(c, '#e9e2bb', 5, 15, 1, 7);
    box(c, INK, 22, 7, 3, 20); box(c, '#dce2d4', 23, 7, 1, 14);
    box(c, '#d3ad59', 20, 20, 6, 2);
  } else {
    box(c, '#3a5b46', 7, 3, 14, 4); box(c, '#66866a', 8, 3, 9, 2);
    box(c, cloth, 10, 2, 5, 2); box(c, '#d6d8a6', 8, 0, 1, 4);
    line(c, '#d6b881', 21, 10, 24, 14, 2);
    line(c, '#d6b881', 24, 14, 24, 23, 2);
    line(c, '#d6b881', 24, 23, 21, 27, 2);
    line(c, '#ede4ba', 21, 10, 21, 27, 1);
    box(c, '#ebd5a6', 17, 18, 8, 1);
  }
  return cacheSprite(key, out);
}

function creatureSprite(type, frame, team, flip, poseStage = 0, tier = 1, idleFrame = 0) {
  frame=poseStage?0:frame%4;idleFrame=poseStage?0:idleFrame;
  const artTier = Math.min(5,Math.max(0,Math.floor(((tier || 1)-1)/2)));
  const key = ['creature', type, frame, team, flip, poseStage, artTier, idleFrame].join(':');
  if (spriteCache.has(key)) return spriteCache.get(key);
  const out = makeCanvas(64, 68), c = out.getContext('2d');
  out.groundPadding = 12; out.bodyTop = 12;
  if (flip) { c.translate(64, 0); c.scale(-1, 1); }
  c.translate(8,12);
  const w = poseStage ? 0 : [0, 2, 0, -2][frame % 4], t = String(type || '').toLowerCase();
  const spirit = /spirit|warrior|wisp/.test(t), tree = /tree|treant|bloom/.test(t);
  const stone = /golem|stone|titan|sentinel|warden|colossus/.test(t), wolf = /wolf|fox|fang/.test(t);
  const bear = /bear|boar|brute/.test(t), dragon = /dragon|drake|wyrm/.test(t);
  if (spirit) {
    box(c, '#183e70', 17, 12, 16, 20); box(c, '#438dd4', 19, 13, 12, 17);
    box(c, '#8be4f5', 20, 13, 4, 12); box(c, '#d5f9fc', 20, 15, 9, 3);
    box(c, '#a1d8ea', 18, 9, 14, 5); box(c, '#dcf7ee', 20, 8, 8, 3);
    box(c, '#244d93', 18, 31, 5, 7 + w); box(c, '#3d87c3', 26, 31, 5, 6 - w);
    box(c, '#a8edff', 12, 19, 3, 18); box(c, '#eafaff', 12, 19, 1, 15);
    box(c, '#81c8ed', 9, 31, 9, 2);
    box(c, '#4b9bdb', 33, 21, 5, 12); box(c, '#aaeaff', 34, 22, 2, 8);
    box(c, '#55c2eb', 18 + frame, 39, 4, 2);
  } else if (tree) {
    box(c, INK, 14, 20, 19, 17); box(c, '#76573b', 17, 22, 14, 13);
    box(c, '#a4884d', 18, 23, 4, 10); box(c, '#4c3e2d', 24, 27, 3, 9);
    box(c, '#5b472f', 13, 33 + w, 7, 7); box(c, '#80623c', 29, 33 - w, 7, 7);
    box(c, '#665134', 8, 22 - w, 7, 12); box(c, '#92744b', 32, 23 + w, 7, 10);
    box(c, '#194738', 8, 8, 31, 17); box(c, '#1f6544', 11, 5, 26, 18);
    box(c, '#438d53', 12, 5, 19, 7); box(c, '#78b466', 15, 4, 12, 4);
    box(c, '#9fd883', 17, 5, 4, 2); box(c, '#54a263', 32, 11, 6, 7);
    box(c, '#e8ef97', 23, 22, 2, 2); box(c, '#e8ef97', 28, 22, 2, 2);
  } else if (stone) {
    box(c, INK, 10, 12, 29, 23); box(c, '#586d73', 12, 14, 26, 19);
    box(c, '#819398', 12, 14, 13, 8); box(c, '#a5b5aa', 14, 13, 8, 3);
    box(c, '#3a4e5d', 17, 32 + w, 7, 8); box(c, '#3a4e5d', 29, 32 - w, 8, 8);
    box(c, '#84928b', 17, 33 + w, 5, 3); box(c, '#667d80', 29, 33 - w, 6, 3);
    box(c, '#304d59', 17, 5, 18, 10); box(c, '#83978b', 18, 4, 16, 8);
    box(c, '#b3bd9d', 19, 4, 9, 3); box(c, '#b8f5cc', 25, 10, 2, 2);
    box(c, '#b8f5cc', 31, 10, 2, 2);
    const leftLift = poseStage === 1 ? -9 : poseStage === 2 ? 4 : poseStage === 3 ? -3 : -w;
    const rightLift = poseStage === 1 ? -24 : poseStage === 2 ? 5 : poseStage === 3 ? -4 : w;
    const reach = poseStage === 2 || poseStage === 3 ? 5 : 0;
    box(c, '#354f59', 3, 17 + leftLift, 10, 15); box(c, '#80938a', 4, 18 + leftLift, 7, 7);
    box(c, '#4b676b', 38 + reach, 17 + rightLift, 8, 16); box(c, '#718c87', 39 + reach, 18 + rightLift, 6, 5);
    if (poseStage === 1) { box(c, INK, 33, -12, 19, 12); box(c, '#8ca381', 35, -11, 15, 9); box(c, '#c2d5a1', 35, -11, 9, 3); }
    box(c, '#74b888', 12, 10, 8, 4);
    box(c, '#a8d478', 15, 9, 4, 3); box(c, '#62bcd0', 25, 21, 6, 7);
    box(c, '#c4f5e0', 27, 22, 2, 3);
    for (let n = 0; n < artTier; n++) {
      const xx = 11 + n * 5;
      box(c, '#263e44', xx - 1, 8 - (n % 2) * 3, 7, 10);
      box(c, '#9eae8b', xx, 8 - (n % 2) * 3, 5, 7); box(c, '#d9e8b1', xx, 8 - (n % 2) * 3, 3, 2);
      box(c, '#8ae8c1', 15 + n * 4, 21 + (n % 2) * 5, 2, 6);
    }
    if (idleFrame === 2 && !poseStage) { box(c, '#749d87', 4, 31, 4, 3); box(c, '#badb9e', 38, 30, 3, 2); }
  } else if (dragon) {
    box(c, '#683b56', 10, 17, 25, 15); box(c, '#a15c6c', 12, 16, 20, 12);
    box(c, '#da9b85', 15, 17, 12, 4); box(c, '#63334c', 7, 29 + w, 8, 8);
    box(c, '#63334c', 26, 29 - w, 8, 8); box(c, '#93475f', 32, 12, 9, 15);
    box(c, '#c87470', 33, 11, 11, 9); box(c, '#efe6aa', 40, 14, 2, 2);
    box(c, '#f5d897', 34, 7, 2, 6); box(c, '#f5d897', 39, 7, 2, 5);
    box(c, '#d98e79', 19, 5 + w, 4, 15); box(c, '#b66877', 11, 8 + w, 9, 9);
    box(c, '#eead8a', 15, 6 + w, 5, 8); box(c, '#6c3554', 3, 24, 9, 5);
    box(c, '#b76d74', 1, 22, 7, 4);
  } else if (/maw/.test(t)) {
    // The river guardian has a broad snapping jaw, plated back and a heavy tail.
    box(c, INK, 7, 15, 31, 19); box(c, '#28576a', 8, 16, 29, 16);
    box(c, '#488f91', 10, 15, 23, 12); box(c, '#7db7a6', 12, 16, 14, 4);
    box(c, '#285363', 10, 31 + w, 8, 8); box(c, '#285363', 28, 30 - w, 8, 9);
    box(c, '#87b8a4', 10, 37 + w, 7, 2); box(c, '#87b8a4', 28, 37 - w, 7, 2);
    const tail = poseStage === 1 ? -3 : poseStage === 2 ? 2 : poseStage === 3 ? 1 : idleFrame - 1;
    box(c, '#355f70', 2, 24 + tail, 9, 8); box(c, '#539793', 1, 23 + tail, 8, 4);
    for (let k = 0; k < 4; k++) {
      box(c, '#b9cd9b', 9 + k * 6, 10 + (k % 2) * 2, 3, 7);
      box(c, '#e1ddae', 9 + k * 6, 9 + (k % 2) * 2, 2, 3);
    }
    const lunge = poseStage === 1 ? -2 : poseStage === 2 ? 5 : poseStage === 3 ? 3 : 0;
    const gape = poseStage === 1 ? 10 : poseStage === 2 ? 1 : poseStage === 3 ? 3 : idleFrame === 1 ? 5 : 3;
    c.save(); c.translate(lunge,poseStage === 1 ? -2 : 0);
    box(c, INK, 29, 12, 17, 15); box(c, '#42878b', 30, 12, 15, 13);
    box(c, '#83b9a4', 31, 11, 10, 4); box(c, '#d2e7b4', 36, 15, 4, 3);
    box(c, '#233a48', 38, 15, 2, 2);
    box(c, '#213744', 34, 21, 13, 5 + gape); box(c, '#b2c8a0', 32, 26 + gape, 14, 4);
    for (let k = 0; k < 3; k++) { box(c, '#fff0c2', 35 + k * 4, 21, 2, 4); box(c, '#fff0c2', 36 + k * 4, 24 + gape, 2, 3); }
    box(c, '#244a5e', 31, 5, 3, 8); box(c, '#96c6ab', 31, 5, 2, 3);
    c.restore();
    for (let n = 0; n < artTier; n++) {
      const xx = 7 + n * 5;
      box(c, '#244555', xx - 1, 6 - n % 2 * 2, 5, 10); box(c, '#a5d4b1', xx, 6 - n % 2 * 2, 3, 8);
      box(c, '#cdefb1', xx, 5 - n % 2 * 2, 2, 3);
      box(c, '#72c9bd', 11 + n * 4, 21, 3, 3);
    }
  } else {
    const dark = wolf ? '#434d65' : bear ? '#65513d' : '#305968';
    const mid = wolf ? '#81899e' : bear ? '#9c7951' : '#5e9b9b';
    const light = wolf ? '#c9cbbb' : bear ? '#c6a475' : '#a3cbb6';
    box(c, INK, 8, 18, 29, 15); box(c, dark, 9, 18, 27, 14);
    box(c, mid, 10, 18, 23, 9); box(c, light, 13, 18, 14, 3);
    box(c, INK, 10, 31 + w, 6, 8); box(c, dark, 11, 31 + w, 5, 6);
    box(c, INK, 28, 31 - w, 6, 8); box(c, dark, 29, 31 - w, 5, 6);
    const sniff = !poseStage ? (idleFrame === 1 ? 3 : idleFrame === 2 ? 1 : 0) : poseStage === 1 ? -2 : poseStage === 2 ? 1 : 0;
    const reach = poseStage === 2 ? 4 : poseStage === 3 ? 2 : 0;
    c.save(); c.translate(reach,sniff);
    box(c, INK, 30, 9, 13, 18); box(c, mid, 31, 10, 11, 15);
    box(c, light, 33, 10, 7, 4); box(c, dark, 41, 17, 5, 7);
    box(c, INK, 35, 16, 3, 2); box(c, '#e0f8bc', 36, 16, 1, 1);
    box(c, dark, 31, 6, 4, 5); box(c, dark, 39, 7, 3, 5);
    box(c, '#c4978a', 32, 8, 2, 3); box(c, dark, 3, 16, 7, 7);
    box(c, light, 3, 15, 4, 4);
    if (!wolf && !bear) {
      box(c, '#dbcf9b', 33, 4, 2, 6); box(c, '#dbcf9b', 39, 3, 2, 7);
      box(c, '#f3df9d', 30, 3, 3, 2); box(c, '#f3df9d', 41, 2, 3, 2);
    }
    if (poseStage === 2) { box(c, INK, 42, 23, 7, 4); box(c, '#f1e7bf', 42, 23, 2, 2); }
    c.restore();
  }
  if (team >= 0) { box(c, TEAM_DARK[team], 19, 18, 8, 5); box(c, TEAM[team], 20, 18, 7, 2); }
  return cacheSprite(key, out);
}

function shadow(c, x, y, r, opacity = 0.28) {
  c.save(); c.globalAlpha = opacity;
  box(c, '#0b202d', x - r, y - r * 0.27, r * 2, r * 0.55);
  box(c, '#0b202d', x - r * 0.75, y - r * 0.44, r * 1.5, r * 0.88);
  c.restore();
}
function tree(c, o, time = 0) {
  const r = o.r || 27, x = o.x, y = o.y, h = hash(x, y);
  if (o.type === 'rock' || o.type === 'cliff') {
    shadow(c, x + 5, y + 6, r * 1.2, 0.3);
    disc(c, '#263d42', x, y - r * 0.22, r);
    disc(c, '#5c7470', x - 3, y - r * 0.35, r * 0.85);
    disc(c, '#83968a', x - r * 0.18, y - r * 0.55, r * 0.53);
    box(c, '#a4ac91', x - r * 0.45, y - r * 0.7, r * 0.45, 3);
    line(c, '#344f50', x + 3, y - r * 0.9, x + 6, y + r * 0.3, 3);
    return;
  }
  const sway = Math.round(Math.sin(time * 1.2 + h * 9) * 2);
  shadow(c, x + 12, y + 5, r * 1.15, 0.3);
  box(c, '#283a2d', x - 6, y - r, 14, r + 7);
  box(c, '#785a3b', x - 4, y - r, 9, r + 4);
  box(c, '#ad8652', x - 3, y - r + 3, 3, r - 3);
  const cy = y - r * 1.15, pine = h > 0.57;
  if (pine) {
    for (let j = 0; j < 3; j++) {
      const yy = cy - r * j * 0.52, rr = r * (1.1 - j * 0.23);
      c.fillStyle = j === 2 ? '#4a9165' : j === 1 ? '#327a53' : '#24583f';
      c.beginPath(); c.moveTo(x + sway, yy - rr * 1.6);
      c.lineTo(x + rr + sway, yy + rr * 0.5); c.lineTo(x - rr + sway, yy + rr * 0.5); c.closePath(); c.fill();
      line(c, '#6ca778', x - rr * 0.25 + sway, yy - rr * 0.5, x - rr * 0.55 + sway, yy + rr * 0.12, 3);
    }
  } else {
    disc(c, '#183e35', x + sway, cy + 3, r * 1.25);
    disc(c, '#266448', x + sway, cy - 5, r * 1.15);
    disc(c, '#408b57', x - r * 0.25 + sway, cy - r * 0.3, r * 0.89);
    disc(c, '#65a967', x - r * 0.35 + sway, cy - r * 0.54, r * 0.56);
    box(c, '#8fc479', x - r * 0.6 + sway, cy - r * 0.62, 7, 4);
    box(c, '#77b66c', x + r * 0.2 + sway, cy - r * 0.4, 5, 3);
    box(c, '#2f7950', x + r * 0.5 + sway, cy + 4, 7, 4);
    if (h < 0.12) {
      box(c, '#e9ca7d', x - 10, cy, 3, 3); box(c, '#e4c58d', x + 9, cy - 10, 3, 3);
    }
  }
}

function crystal(c, u, time) {
  const t = u.team >= 0 ? u.team : 0, x = u.x, y = u.y;
  shadow(c, x, y + 6, 65, 0.4);
  disc(c, '#243e4c', x, y, 70); disc(c, '#526d77', x, y - 6, 65);
  disc(c, '#899d9d', x, y - 11, 59); disc(c, '#344d60', x, y - 12, 45);
  ring(c, TEAM_DARK[t], x, y - 12, 38, 5);
  for (let i = 0; i < 8; i++) {
    const a = i * Math.PI / 4;
    box(c, '#cfceb0', x + Math.cos(a) * 54 - 3, y - 11 + Math.sin(a) * 54 - 3, 6, 6);
  }
  const bob = Math.round(Math.sin(time * 1.9) * 4);
  c.save(); c.globalAlpha = 0.12 + Math.sin(time * 2) * 0.04;
  disc(c, TEAM[t], x, y - 43, 64); c.restore();
  diamond(c, x - 26, y - 38 + bob, 23, 47, TEAM_DARK[t], TEAM[t]);
  diamond(c, x + 29, y - 30 - bob, 22, 41, TEAM_DARK[t], TEAM[t]);
  diamond(c, x, y - 65 + bob, 56, 106, TEAM_DARK[t], TEAM[t]);
  box(c, '#e7fcf1', x - 15, y - 78 + bob, 6, 16);
  for (let i = 0; i < 4; i++) {
    const a = time * 0.6 + i * Math.PI / 2;
    const py = y - 29 + Math.sin(a) * 16;
    box(c, TEAM[t], x + Math.cos(a) * 51, py, 4, 4);
  }
}
function tower(c, u, time) {
  const x = u.x, y = u.y, t = u.team >= 0 ? u.team : 0;
  shadow(c, x + 7, y + 5, 31, 0.35);
  disc(c, '#273f4d', x, y, 35); disc(c, '#6f8384', x, y - 4, 31);
  box(c, '#233c4e', x - 22, y - 62, 44, 58);
  box(c, '#738c95', x - 20, y - 60, 39, 55);
  box(c, '#9ea99e', x - 20, y - 60, 13, 52);
  box(c, '#4a697c', x + 8, y - 60, 11, 55);
  for (let k = 0; k < 4; k++) {
    line(c, '#45616e', x - 18, y - 14 - k * 12, x + 16, y - 14 - k * 12, 2);
    box(c, '#506a75', x + (k % 2 ? -2 : -10), y - 24 - k * 12, 2, 10);
  }
  box(c, '#283f4e', x - 27, y - 69, 54, 14);
  box(c, '#9eaaa0', x - 26, y - 69, 52, 7);
  for (let k = 0; k < 4; k++) box(c, '#adb9ab', x - 26 + k * 15, y - 77, 9, 11);
  box(c, TEAM_DARK[t], x - 5, y - 58, 16, 35);
  box(c, TEAM[t], x - 4, y - 57, 4, 29);
  diamond(c, x + 3, y - 45, 9, 14, TEAM[t]);
  diamond(c, x, y - 87 + Math.sin(time * 2 + x) * 2, 22, 32, TEAM_DARK[t], TEAM[t]);
  if (u.attackAnim > 0) {
    c.save(); c.globalAlpha = 0.7;
    ring(c, '#fff8c2', x, y - 87, 21 + Math.sin(time * 20) * 3, 3); c.restore();
  }
}

function buildTerrain() {
  const out = makeCanvas(Math.ceil(WORLD.width / 2), Math.ceil(WORLD.height / 2));
  const c = out.getContext('2d'); c.scale(0.5, 0.5);
  box(c, '#365f47', 0, 0, WORLD.width, WORLD.height);
  const shades = ['#355e46', '#38634a', '#39654b', '#345e46', '#3a654a'];
  for (let y = 0; y < WORLD.height; y += 32) for (let x = 0; x < WORLD.width; x += 32) {
    const tile = hash(x, y);
    // Offset short patches avoid a visible square grid across the grass.
    box(c, shades[Math.floor(tile * shades.length)], x + tile * 9, y + tile * 12, 13 + tile * 27, 7 + tile * 18);
    const h = hash(x + 11, y + 17);
    box(c, '#46784f', x + h * 27, y + hash(y, x) * 25, 2, 5);
    box(c, '#294f40', x + h * 20 + 3, y + hash(y, x) * 25 + 2, 2, 3);
    if (h > 0.97) { box(c, '#93ae6b', x + 17, y + 20, 3, 2); box(c, '#c5cf8c', x + 18, y + 18, 2, 2); }
  }
  // Broad, clear lanes are bordered by moss. Their imperfect paving is small
  // enough to read as ground texture, never giant checkerboard battlefield tiles.
  for (const lane of WORLD.lanes || []) {
    path(c, lane.points, '#294b3c', 146);
    path(c, lane.points, '#6b7951', 124);
    path(c, lane.points, '#a1a06f', 108);
    path(c, lane.points, '#b9b080', 92);
    for (let j = 1; j < lane.points.length; j++) {
      const a = lane.points[j - 1], b = lane.points[j], dx = b.x - a.x, dy = b.y - a.y;
      const len = Math.hypot(dx, dy), nx = -dy / len, ny = dx / len;
      for (let d = 7; d < len; d += 21) {
        const h = hash(d + j * 91, lane.id), xx = a.x + dx * d / len, yy = a.y + dy * d / len;
        box(c, h < 0.5 ? '#c7bd8c' : '#a6a476', xx + nx * (h - 0.5) * 72, yy + ny * (h - 0.5) * 72, 9 + h * 10, 4);
        if (h < 0.4) box(c, '#758556', xx + nx * 54, yy + ny * 54, 5, 3);
      }
    }
  }
  for (const camp of WORLD.camps || []) drawCampScenery(c,camp);
  for (const patch of WORLD.brush || []) {
    disc(c, '#24523d', patch.x, patch.y, patch.r || 50);
    for (let i = 0; i < 35; i++) {
      const a = hash(i, patch.x) * Math.PI * 2, rr = hash(i, patch.y) * (patch.r || 50);
      const x = patch.x + Math.cos(a) * rr, y = patch.y + Math.sin(a) * rr;
      box(c, '#4b8952', x, y - 5, 2, 9); box(c, '#73a75d', x, y - 5, 2, 2);
    }
  }
  const river = WORLD.river || [], rw = WORLD.riverWidth || 110;
  path(c, river, '#263f40', rw + 30); path(c, river, '#83a18a', rw + 18);
  path(c, river, '#3d7a85', rw + 7); path(c, river, '#286d83', rw);
  path(c, river, '#2d8195', rw * 0.72); path(c, river, '#338fa0', rw * 0.33);
  for (let i = 1; i < river.length; i++) {
    const a = river[i - 1], b = river[i], dx = b.x - a.x, dy = b.y - a.y, len = Math.hypot(dx, dy);
    for (let d = 0; d < len; d += 35) {
      const f = d / len, x = a.x + dx * f, y = a.y + dy * f;
      box(c, '#67b3b3', x + hash(d, i) * rw * 0.65 - rw * 0.3, y, 13, 2);
      box(c, '#4099a5', x + hash(i, d) * rw * 0.6 - rw * 0.3, y + 10, 7, 2);
    }
  }
  for (const pit of WORLD.beastPits || []) {
    disc(c, '#233d3e', pit.x, pit.y, 102);
    disc(c, '#566f69', pit.x, pit.y, 89);
    disc(c, '#314e50', pit.x, pit.y, 76);
    ring(c, '#8eaa8b', pit.x, pit.y, 85, 4);
    for (let k = 0; k < 8; k++) {
      const a = k * Math.PI / 4;
      diamond(c, pit.x + Math.cos(a) * 90, pit.y + Math.sin(a) * 90, 8, 13, '#bda875', '#e6cf91');
    }
    for (let k = -1; k <= 1; k++) line(c, '#899983', pit.x - 17 + k * 12, pit.y - 21, pit.x + k * 12, pit.y + 20, 3);
  }
  for (let i = 0; i < (WORLD.bases || []).length; i++) {
    const base = WORLD.bases[i], well = base.well || base;
    disc(c, '#294b47', base.x, base.y, 182);
    disc(c, '#697d76', base.x, base.y, 162);
    disc(c, '#89998a', base.x, base.y, 150);
    disc(c, '#677f79', base.x, base.y, 132);
    ring(c, '#a6ac8f', base.x, base.y, 141, 4);
    for (let n = 0; n < 16; n++) {
      const a = n * Math.PI / 8;
      line(c, '#506d6a', base.x + Math.cos(a) * 78, base.y + Math.sin(a) * 78, base.x + Math.cos(a) * 130, base.y + Math.sin(a) * 130, 2);
    }
    disc(c, '#244656', well.x, well.y, 76);
    disc(c, '#a1b3a7', well.x, well.y - 5, 67);
    disc(c, '#355f74', well.x, well.y - 7, 55);
    disc(c, '#3a8e9c', well.x, well.y - 8, 47);
    ring(c, TEAM[i], well.x, well.y - 8, 52, 3);
  }
  // A subtle boundary is visible before the collision edge.
  c.strokeStyle = '#233e36'; c.lineWidth = 18; c.strokeRect(4, 4, WORLD.width - 8, WORLD.height - 8);
  return out;
}

export class Renderer {
  constructor(canvas) {
    this.canvas = canvas; this.ctx = canvas.getContext('2d', { alpha: false });
    this.terrain = buildTerrain();
    this.camera = { x: 0, y: 0, width: 960, height: 540, zoom: 1 };
    this.lastTime = null; this.hasCamera = false; this.width = 1280; this.height = 720;
    this.motionPreference = typeof globalThis.matchMedia === 'function' ? globalThis.matchMedia('(prefers-reduced-motion: reduce)') : null;
    this.effectBudget = createEffectBudget(); this.visibleEffects = [];
    this.worldEffectOptions = {drawUnit:(c,e,alpha)=>this.departingSummon(c,e,alpha)};
    this.resize(canvas.clientWidth || 1280, canvas.clientHeight || 720);
  }
  resize(width, height) {
    this.width = Math.max(1, width); this.height = Math.max(1, height);
    // One backing-store pixel covers two CSS pixels. Device-pixel ratio never
    // turns these sprites blurry or makes a phone render a giant framebuffer.
    this.canvas.width = Math.ceil(this.width / 2); this.canvas.height = Math.ceil(this.height / 2);
    this.ctx.imageSmoothingEnabled = false;
    const cssZoom = Math.min(1.6, Math.max(0.8, this.width / 940));
    this.camera.width = this.width / cssZoom;
    this.camera.height = this.height / cssZoom;
    this.camera.zoom = this.canvas.width / this.camera.width;
    this.fogCanvas = null;
  }
  screenToWorld(clientX, clientY) {
    const rect = this.canvas.getBoundingClientRect();
    return {
      x: this.camera.x + (clientX - rect.left) / Math.max(1, rect.width) * this.camera.width,
      y: this.camera.y + (clientY - rect.top) / Math.max(1, rect.height) * this.camera.height,
    };
  }
  worldToScreen(x, y) {
    const rect = this.canvas.getBoundingClientRect();
    return { x: rect.left + (x - this.camera.x) / this.camera.width * rect.width,
      y: rect.top + (y - this.camera.y) / this.camera.height * rect.height };
  }
  visible(x, y, margin = 110) {
    const cam = this.camera;
    return x > cam.x - margin && x < cam.x + cam.width + margin && y > cam.y - margin && y < cam.y + cam.height + margin;
  }
  follow(game, view = {}) {
    const p = game.player || game.units?.find(u => u.id === game.playerId);
    if (!p) return;
    const cam = this.camera;
    let focus = p;
    if (this.peek && Number.isFinite(this.peek.x) && Number.isFinite(this.peek.y)) focus = this.peek;
    else if (alive(p) && view.cameraAim && Number.isFinite(view.cameraAim.x) && Number.isFinite(view.cameraAim.y)) {
      const dx = view.cameraAim.x - p.x, dy = view.cameraAim.y - p.y, distance = Math.hypot(dx, dy) || 1;
      const look = Math.min(distance * .22, cam.width * .16, 150);
      focus = {x:p.x + dx / distance * look, y:p.y + dy / distance * look};
    }
    const targetX = clamp(focus.x - cam.width / 2, 0, Math.max(0, WORLD.width - cam.width));
    const targetY = clamp(focus.y - cam.height * 0.49, 0, Math.max(0, WORLD.height - cam.height));
    const dt = this.lastTime == null ? 1 : clamp(game.time - this.lastTime, 0, 0.1);
    const snap = !this.hasCamera || Math.hypot(targetX - cam.x, targetY - cam.y) > 800;
    const f = snap ? 1 : 1 - Math.exp(-11 * Math.max(dt, 1 / 60));
    cam.x += (targetX - cam.x) * f; cam.y += (targetY - cam.y) * f;
    this.hasCamera = true; this.lastTime = game.time;
  }
  draw(game, view = {}) {
    if (!game) return;
    this.follow(game, view);
    const team = game.player?.team ?? 0;
    const shown = u => !game.canSee || game.canSee(team, u) || (!alive(u) && (u.kind==='tower'||u.kind==='core') && (!game.pointVisible || game.pointVisible(team,u.x,u.y)));
    const seenPoint = (x, y) => !game.pointVisible || game.pointVisible(team, x, y);
    const c = this.ctx, cam = this.camera, time = game.time || 0;
    const presentationTime = Number.isFinite(view.presentationTime) ? clamp(view.presentationTime,time,time+2) : time;
    this.presentationTime = presentationTime;
    resetEffectBudget(this.effectBudget, this.width, this.motionPreference?.matches || false);
    const effects = this.visibleEffects; effects.length = 0;
    const finalCore = e => game.winner != null && e.type === 'structure-collapse' && e.kind === 'core';
    for (const e of game.effects || []) if (this.visible(e.x, e.y, (e.r || 80) + 150) && (isEffectVisible(game, team, e) || finalCore(e))) effects.push(e);
    c.setTransform(1, 0, 0, 1, 0, 0); c.globalAlpha = 1;
    c.fillStyle = '#142c31'; c.fillRect(0, 0, this.canvas.width, this.canvas.height);
    const sx = this.canvas.width / cam.width, sy = this.canvas.height / cam.height;
    c.setTransform(sx, 0, 0, sy, Math.round(-cam.x * sx), Math.round(-cam.y * sy));
    c.drawImage(this.terrain, 0, 0, WORLD.width, WORLD.height);
    this.drawWater(c, time);
    this.drawWells(c, time);
    drawReactiveTerrain(c,game,time,u=>this.visible(u.x,u.y,100),this.effectBudget);
    drawExpansionZones(c,game,time,(x,y,r)=>this.visible(x,y,r||100),this.effectBudget);
    this.drawObjectiveGround(c, game);
    this.drawCombatIndicators(c, game);
    for (const e of effects) if (!finalCore(e)) this.effect(c, e, isWorldEffect(e)?presentationTime:time, true);
    const sorted = [];
    for (const o of WORLD.obstacles || []) if (this.visible(o.x, o.y)) sorted.push({ y: o.y, obstacle: o });
    for (const u of game.units || []) if (this.visible(u.x, u.y, 160) && shown(u)) sorted.push({ y: u.y, unit: u });
    sorted.sort((a, b) => a.y - b.y);
    for (const item of sorted) {
      if (item.obstacle) {
        const o = item.obstacle, p = game.player;
        // Canopies become translucent when the player's body is behind one.
        c.save();
        if (p && Math.abs(p.x - o.x) < (o.r || 30) * 1.6 && p.y < o.y && p.y > o.y - (o.r || 30) * 3.2) c.globalAlpha = 0.5;
        tree(c, o, time); c.restore();
      } else this.unit(c, item.unit, game);
    }
    for (const p of game.projectiles || []) if (this.visible(p.x, p.y, 140) && seenPoint(p.x, p.y)) this.projectile(c, p, time, game);
    drawMonsterRocks(c,game,time,(x,y)=>this.visible(x,y,100),this.effectBudget);
    for (const e of effects) if (e.type !== 'combat-burst' && !finalCore(e)) this.effect(c, e, isWorldEffect(e)?presentationTime:time, false);
    // Fresh impacts win the cosmetic allowance when a crowded fight reaches its
    // budget. Ambient motes cannot starve the hit that just landed.
    for (let i = effects.length - 1; i >= 0; i--) if (effects[i].type === 'combat-burst') this.effect(c, effects[i], time, false);
    this.drawFog(c, game);
    // Once the match is decided, the shattered objective is public information.
    // Its short presentation remains visible even when a distant bot finished it.
    for (const e of effects) if (finalCore(e)) { this.effect(c,e,presentationTime,true); this.effect(c,e,presentationTime,false); }
    this.drawTelegraphs(c, game);
    this.drawAim(c, game, view);
    for (const u of game.units || []) if (alive(u) && this.visible(u.x, u.y, 90) && shown(u)) this.unitBars(c, u, game);
    for (const t of (game.texts || []).slice(-110)) if (seenPoint(t.x, t.y)) this.floatText(c, t);
    this.playerMarker(c, game);
    c.setTransform(1, 0, 0, 1, 0, 0); c.globalAlpha = 1;
    if (view.rewardTarget) {
      const rect = this.canvas.getBoundingClientRect?.() || {left:0,top:0};
      const target = {x:view.rewardTarget.x-rect.left,y:view.rewardTarget.y-rect.top};
      const viewport = {width:this.canvas.width,height:this.canvas.height,cssWidth:this.width,cssHeight:this.height};
      for (const e of effects) if (e.type==='reward-gold' && e.ownerId===game.playerId) drawGoldFlight(c,e,presentationTime,cam,viewport,target,this.effectBudget);
    }
    if (game.player && game.player.hp / game.player.maxHp < 0.25 && alive(game.player)) {
      c.strokeStyle = '#b9445766'; c.lineWidth = 6;
      c.strokeRect(0, 0, this.canvas.width, this.canvas.height);
    }
  }
  drawFog(c, game) {
    if (!game.visionSources) return;
    const team = game.player?.team ?? 0;
    // A small world-space mask follows camera peeks without being rebuilt on
    // every animation frame. Enemy visibility itself always uses live rules.
    const fresh = !this.fogCanvas;
    const fog = this.fogCanvas ||= makeCanvas(Math.ceil(WORLD.width / 8), Math.ceil(WORLD.height / 8));
    if (fresh || this.fogGame !== game || this.fogTeam !== team || game.time - (this.fogAt ?? -1) >= .14) {
      const f = fog.getContext('2d');
      f.setTransform(1, 0, 0, 1, 0, 0); f.globalCompositeOperation = 'source-over';
      f.clearRect(0, 0, fog.width, fog.height); f.fillStyle = '#08192588'; f.fillRect(0, 0, fog.width, fog.height);
      f.globalCompositeOperation = 'destination-out';
      for (const source of game.visionSources(team)) {
        const x = source.x / 8, y = source.y / 8, r = (source.r || source.radius || 1) / 8;
        const gradient = f.createRadialGradient(x, y, r * .86, x, y, r);
        gradient.addColorStop(0, '#000'); gradient.addColorStop(1, '#0000'); f.fillStyle = gradient;
        f.beginPath(); f.arc(x, y, r, 0, Math.PI * 2); f.fill();
      }
      f.globalCompositeOperation = 'source-over';
      this.fogAt = game.time; this.fogTeam = team; this.fogGame = game;
    }
    c.drawImage(fog, 0, 0, WORLD.width, WORLD.height);
  }
  drawTelegraphs(c, game) {
    const team = game.player?.team ?? 0;
    const pending = [
      ...(game.scheduledImpacts || []).map(z => ({...z, remaining:z.executeAt - game.time, total:z.delay || .5})),
      ...(game.zones || []).filter(z => z.delay > 0 && !z.detonated && (z.damage || z.shape)).map(z => ({...z, remaining:z.tickIn, total:z.maxDelay || z.windup || z.delay}))
    ];
    for (const z of pending) {
      if (!(z.remaining > 0) || !this.visible(z.x, z.y, (z.range || z.r || 160) + 100)) continue;
      if (game.pointVisible && !game.pointVisible(team, z.x, z.y) && z.team !== team) continue;
      const color = z.team === team ? '#8ce9ff' : z.team === -1 ? '#ffd077' : '#ff816f';
      const progress = clamp(1 - z.remaining / Math.max(.01, z.total), 0, 1), radius = z.r || 130;
      const shape = z.shape || (z.arc ? 'cone' : 'circle');
      c.save(); c.fillStyle = color; c.strokeStyle = color; c.lineWidth = 3;
      c.setLineDash(z.team === team ? [] : [8, 5]); c.beginPath();
      if (shape === 'line') {
        const angle = z.angle || 0, length = z.range || radius * 2, width = z.width || radius / 3;
        const dx = Math.cos(angle), dy = Math.sin(angle), nx = -dy * width, ny = dx * width;
        c.moveTo(z.x + nx, z.y + ny); c.lineTo(z.x + dx * length + nx, z.y + dy * length + ny);
        c.lineTo(z.x + dx * length - nx, z.y + dy * length - ny); c.lineTo(z.x - nx, z.y - ny); c.closePath();
      } else if (shape === 'cone') {
        const arc = z.arc || Math.PI * .65, angle = z.angle || 0;
        c.moveTo(z.x, z.y); c.arc(z.x, z.y, z.range || radius, angle - arc / 2, angle + arc / 2); c.closePath();
      } else c.arc(z.x, z.y, radius, 0, Math.PI * 2);
      c.globalAlpha = .13 + progress * .16; c.fill(); c.globalAlpha = .9; c.stroke(); c.setLineDash([]);
      c.lineWidth = 4; c.beginPath(); c.arc(z.x, z.y, 13, -Math.PI / 2, -Math.PI / 2 + Math.PI * 2 * progress); c.stroke();
      this.label(c, z.team === team ? 'CAST' : '!', z.x, z.y + 4, color, 10); c.restore();
      drawExpansionWarning(c,z,progress,game.time,this.effectBudget);
    }
  }
  drawWater(c, time) {
    const river = WORLD.river || [], width = WORLD.riverWidth || 110;
    for (let j = 1; j < river.length; j++) {
      const a = river[j - 1], b = river[j], dx = b.x - a.x, dy = b.y - a.y, len = Math.hypot(dx, dy);
      for (let i = 0; i < len; i += 38) {
        const d = (i + (time * 12) % 38) / len;
        const x = a.x + dx * d + (hash(i, j) - 0.5) * width * 0.62, y = a.y + dy * d;
        if (!this.visible(x, y, 10)) continue;
        c.globalAlpha = 0.35 + Math.sin(time * 1.5 + i) * 0.15;
        box(c, '#c0e4c9', x, y, 7 + hash(j, i) * 9, 2);
      }
    }
    c.globalAlpha = 1;
  }
  drawWells(c, time) {
    for (let i = 0; i < (WORLD.bases || []).length; i++) {
      const b = WORLD.bases[i], w = b.well || b;
      if (!this.visible(w.x, w.y, 100)) continue;
      c.save(); c.globalAlpha = 0.25;
      ring(c, '#c6ffdf', w.x, w.y - 8, 28 + (time * 12) % 22, 3);
      ring(c, '#c6ffdf', w.x, w.y - 8, 12 + (time * 12) % 22, 2); c.restore();
      for (let k = 0; k < 6; k++) {
        const a = k * Math.PI / 3 + time * 0.3;
        const x = w.x + Math.cos(a) * 35, y = w.y + Math.sin(a) * 24 - 10 - (time * 12 + k * 9) % 32;
        box(c, '#c2f6d4', x, y, 3, 7); box(c, '#c2f6d4', x - 2, y + 2, 7, 3);
      }
      this.label(c, 'HEALING WELL', w.x, w.y + 87, '#c8e5d3', 10);
    }
  }
  drawObjectiveGround(c, game) {
    for (const pit of WORLD.beastPits || []) {
      if (!this.visible(pit.x, pit.y, 110)) continue;
      const active = (game.units || []).some(u => u.kind === 'beast' && u.team === -1 && alive(u) && Math.hypot(u.x - pit.x, u.y - pit.y) < 150 && (!game.canSee || game.canSee(game.player?.team ?? 0, u)));
      c.save(); c.globalAlpha = active ? 0.55 : 0.25;
      ring(c, active ? '#f4d18a' : '#9bbcb0', pit.x, pit.y, 95 + Math.sin(game.time * 2) * 2, 3); c.restore();
      this.label(c, active ? 'CLAIM THE BEAST' : 'BEAST SHRINE', pit.x, pit.y + 116, active ? '#ffe3a1' : '#a9c4b8', 10);
    }
  }
  drawCombatIndicators(c, game) {
    const player = game.player;
    if (!player || !alive(player)) return;
    const selected = game.units.find(u => u.id === player.targetId && alive(u));
    if (selected && this.visible(selected.x, selected.y) && (!game.canSee || game.canSee(player.team, selected))) {
      const locked = game.attackOrder?.explicit, color = locked ? '#ffe194' : '#ffb88d';
      const radius = (selected.r || 17) + 10 + Math.sin(game.time * 5) * 2;
      c.save(); c.globalAlpha = .9;
      ring(c, color, selected.x, selected.y + 2, radius, 2, !locked);
      for (let i = 0; i < 4; i++) {
        const angle = i * Math.PI / 2 + Math.PI / 4, x = selected.x + Math.cos(angle) * (radius + 7), y = selected.y + Math.sin(angle) * (radius + 7);
        line(c, color, x, y, x - Math.cos(angle - .5) * 9, y - Math.sin(angle - .5) * 9, 3);
        line(c, color, x, y, x - Math.cos(angle + .5) * 9, y - Math.sin(angle + .5) * 9, 3);
      }
      c.restore();
      this.label(c, locked ? 'FOCUS' : 'ATTACKING', selected.x, selected.y + radius + 18, color, 9);
    }
    for (const u of game.units) {
      if (u.kind !== 'tower' || !alive(u) || !this.visible(u.x, u.y, (u.range || 255) + 40)) continue;
      const enemy = u.team !== player.team, targeted = u.targetId === player.id;
      if (enemy && (targeted || selected?.id === u.id || Math.hypot(player.x-u.x, player.y-u.y) < u.range + 135)) {
        c.save(); c.globalAlpha = targeted ? .5 : .2;
        ring(c, targeted ? '#ff765e' : '#ffd0a1', u.x, u.y, u.range, targeted ? 3 : 2, !targeted); c.restore();
      }
      const victim = game.units.find(t => t.id === u.targetId && alive(t));
      if (victim && (targeted || u.attackAnim > 0) && (!game.canSee || game.canSee(player.team, victim))) {
        c.save(); c.globalAlpha = u.attackAnim > 0 ? .65 : .2;
        line(c, enemy ? '#ffbc86' : '#b4edff', u.x, u.y - 87, victim.x, victim.y - 15, u.attackAnim > 0 ? 3 : 1); c.restore();
      }
      if (targeted) this.label(c, 'TOWER TARGET', player.x, player.y + 45, '#ffbc91', 10);
    }
  }
  drawAim(c, game, view) {
    const p = game.player, slot = view.selectedSlot;
    if (!p || !alive(p) || slot == null || slot < 0) return;
    const a = game.getAbility ? game.getAbility(p, slot) : HEROES[p.heroId]?.abilities[slot];
    if (!a) return;
    const aim = view.aim || { x: p.x + Math.cos(p.angle || 0) * a.range, y: p.y + Math.sin(p.angle || 0) * a.range };
    const range = a.range || a.radius || 130, color = a.color || HEROES[p.heroId]?.color || '#a8eaf2';
    const dx = aim.x - p.x, dy = aim.y - p.y, distance = Math.hypot(dx, dy), angle = Math.atan2(dy, dx);
    const tx = p.x + Math.cos(angle) * Math.min(distance, range), ty = p.y + Math.sin(angle) * Math.min(distance, range);
    c.save(); c.globalAlpha = 0.22; ring(c, color, p.x, p.y, range, 2, true);
    if(drawExpansionAim(c,game,p,a,{angle,tx,ty,range,color})){c.restore();return;}
    const type = String(a.type || a.target || '');
    if (a.id === 'glacial-sweep' || a.id === 'shield-bash') {
      const arc = a.arc || Math.PI * (a.id === 'glacial-sweep' ? .85 : .58);
      c.globalAlpha = .17; c.fillStyle = color; c.beginPath(); c.moveTo(p.x, p.y);
      c.arc(p.x, p.y, range, angle - arc / 2, angle + arc / 2); c.closePath(); c.fill();
      c.globalAlpha = .85; c.strokeStyle = color; c.lineWidth = 2; c.stroke();
    } else if (/line|projectile|cone|fan|shot|dash/.test(type)) {
      c.globalAlpha = 0.15;
      line(c, color, p.x, p.y, p.x + Math.cos(angle) * range, p.y + Math.sin(angle) * range, Math.max(10, (a.radius || 14) * 2));
      c.globalAlpha = 0.9;
      const ex = p.x + Math.cos(angle) * range, ey = p.y + Math.sin(angle) * range;
      line(c, color, p.x, p.y, ex, ey, 2);
      line(c, color, ex, ey, ex - Math.cos(angle - 0.45) * 18, ey - Math.sin(angle - 0.45) * 18, 3);
      line(c, color, ex, ey, ex - Math.cos(angle + 0.45) * 18, ey - Math.sin(angle + 0.45) * 18, 3);
      if (/cone|fan/.test(type)) for (const side of [-1, 1]) line(c, color, p.x, p.y, p.x + Math.cos(angle + side * 0.35) * range, p.y + Math.sin(angle + side * 0.35) * range, 2);
    } else {
      const self = /self|aura/.test(type), x = self ? p.x : tx, y = self ? p.y : ty;
      c.globalAlpha = 0.14; disc(c, color, x, y, a.radius || 65);
      c.globalAlpha = 0.8; ring(c, color, x, y, a.radius || 65, 3);
      line(c, '#ecffeb', x - 7, y, x + 7, y, 2); line(c, '#ecffeb', x, y - 7, x, y + 7, 2);
    }
    c.restore();
  }
  unit(c, u, game) {
    if (!alive(u)) {
      if (u.kind === 'tower') {
        shadow(c, u.x, u.y, 31, 0.25);
        for (let k = 0; k < 7; k++) { const a = k * 2.7; disc(c, k % 2 ? '#708078' : '#4a6667', u.x + Math.cos(a) * 23, u.y + Math.sin(a) * 16 - 3, 8 + k); }
      } else if (u.kind === 'hero' && u.respawnIn > 0) {
        c.save(); c.globalAlpha = 0.28;
        ring(c, TEAM[u.team], u.x, u.y, 21, 2); box(c, '#dce4cf', u.x - 2, u.y - 13, 4, 15); box(c, '#dce4cf', u.x - 7, u.y - 10, 14, 4);
        c.restore();
      }
      return;
    }
    if (u.kind === 'core') { crystal(c, u, game.time); drawStructureDamage(c,u,game.time,this.effectBudget); return; }
    if (u.kind === 'tower') { tower(c, u, game.time); drawStructureDamage(c,u,game.time,this.effectBudget); return; }
    if(u.subtype==='sentry'||u.subtype==='mine'){
      drawShieldShell(c,u,game.time,50,44,false,this.effectBudget);drawDevice(c,u,game.time,this.effectBudget);drawShieldShell(c,u,game.time,50,44,true,this.effectBudget);return;
    }
    const pose = unitPose(u,game,game.time,this.effectBudget?.reducedMotion);
    const frame = pose.frame, attack = pose.stage > 0, attackFrame = attack ? pose.stage % 4 : frame;
    const flip = Math.cos(pose.angle) < -0.15;
    let sprite, width, height, scale;
    if (u.kind === 'hero') {
      const facing = flip ? 2 : Math.sin(pose.angle) < -0.55 ? 3 : 0;
      sprite = heroSprite(u.heroId || u.subtype || 'arc',frame,facing,attack,u.cosmetics,pose.stage,pose.direction,pose.cast,pose.variant);
      scale=1.5; width=(u.heroId==='seraph'?72:u.heroId==='cadence'?64:['brinehook','vela','ferrite'].includes(u.heroId)?60:['bulwark','ember','rivet','solstice'].includes(u.heroId)?48:40)*scale;
      height=(['bulwark','ember','seraph'].includes(u.heroId)?48:44)*scale;
      c.save(); c.globalAlpha = u.id === game.playerId || u === game.player ? 0.9 : 0.45;
      ring(c, TEAM[u.team], u.x, u.y + 1, 21, u.id === game.playerId || u === game.player ? 3 : 2); c.restore();
    } else if (u.kind === 'footman' || u.kind === 'archer') {
      sprite = soldierSprite(u.kind, u.team, attackFrame, flip); width = 34; height = 42; scale=width/sprite.width;
    } else {
      sprite = creatureSprite(u.subtype || u.heroId || u.kind,frame,u.team,flip,pose.stage,u.kind==='beast'?u.tier:1,pose.idleFrame);
      const big = u.kind === 'beast', r = u.r || (big ? 35 : 20);
      width = big ? Math.min(140, Math.max(94, r * 2.8) + Math.min(6, (u.tier || 1) - 1) * 2) : Math.max(48, r * 2.5) * (u.scale || 1); height = width * 44 / 48; scale=width/48;
    }
    shadow(c, u.x, u.y + 2, width * 0.3, 0.29);
    const trail = cosmeticColor(u.cosmetics?.trail);
    if (trail && (u.moving || attack)) {
      c.save();
      for (let k = 1; k <= 4; k++) {
        c.globalAlpha = .48 * (1 - k / 5);
        const angle = (u.angle || 0) + Math.PI, distance = k * 10;
        box(c, trail, u.x + Math.cos(angle) * distance - 2, u.y + Math.sin(angle) * distance - 4 - (k % 2) * 5, 4, 4);
      }
      c.restore();
    }
    const status = u.status || {}, invisible = status.stealth || status.invisible;
    const drawWidth=sprite.width*scale,drawHeight=sprite.height*scale;
    const originY=u.y-drawHeight+7+(sprite.groundPadding||0)*scale;
    const birth=summonPose(u,game.time,this.effectBudget?.reducedMotion);
    const action=u.visualAction;
    const dashPose=u.heroId==='ember'&&['cinder-dash','flicker-strike'].includes(action?.abilityId)||u.heroId==='vela'&&action?.abilityId==='sidestep'||u.heroId==='cadence'&&action?.abilityId==='skipbeat';
    if(dashPose&&!this.effectBudget?.reducedMotion&&action?.kind==='cast'&&game.time>=action.startedAt&&game.time-action.startedAt<.34&&Math.hypot(action.toX-action.fromX,action.toY-action.fromY)>30){
      c.save();
      for(const amount of [.3,.65]){
        const xx=action.fromX+(action.toX-action.fromX)*amount,yy=action.fromY+(action.toY-action.fromY)*amount;
        if(game.pointVisible&&!game.pointVisible(game.player?.team??0,xx,yy))continue;
        if(this.effectBudget&&(this.effectBudget.remaining<1||this.effectBudget.accentRemaining<1))break;
        if(this.effectBudget){this.effectBudget.remaining--;this.effectBudget.accentRemaining--;}
        c.globalAlpha=.22*(1-clamp((game.time-action.startedAt)/.34,0,1));
        c.drawImage(sprite,Math.round(xx-drawWidth/2),Math.round(originY+yy-u.y),drawWidth,drawHeight);
      }c.restore();
    }
    drawShieldShell(c, u, game.time, width, height, false, this.effectBudget);
    c.save(); c.globalAlpha=(invisible?.5:1)*birth.alpha;
    if(birth.active&&(birth.rise||birth.assemble)){
      c.beginPath();
      if(birth.assemble){
        for(let row=0;row<8;row++){
          const threshold=(row*3%8)/8;
          if(birth.progress>=threshold)c.rect(u.x-drawWidth/2,originY+row*drawHeight/8,drawWidth,drawHeight/8+1);
        }
      }else c.rect(u.x-drawWidth/2-8,u.y-height-35,drawWidth+16,height+43);
      c.clip();
    }
    c.translate(u.x+pose.dx,u.y+pose.dy-birth.lift); c.rotate(pose.tilt);
    c.drawImage(sprite,Math.round(-drawWidth/2),Math.round(originY-u.y+birth.rise*height),drawWidth,drawHeight); c.restore();
    drawHitGlint(c,u,pose,width,height,this.effectBudget);
    drawShieldShell(c, u, game.time, width, height, true, this.effectBudget);
    drawExpansionUnitAccents(c,u,game.time,this.effectBudget);
    if (u.recallIn > 0 || u.recalling || u.recallTime > 0) {
      c.save(); c.globalAlpha = 0.7;
      ring(c, '#9befff', u.x, u.y, 30, 3, true);
      ring(c, '#d1ffff', u.x, u.y, 22 + Math.sin(game.time * 6) * 4, 2);
      for (let k = 0; k < 8; k++) { const a = k * Math.PI / 4 + game.time; box(c, '#b5faff', u.x + Math.cos(a) * 23, u.y - (game.time * 35 + k * 11) % 65, 3, 6); }
      c.restore();
    }
    const statusOn = value => value === true || (typeof value === 'number' ? value > 0 : value && (value.remaining > 0 || value.time > 0 || value.duration > 0 || value.until > game.time));
    if (statusOn(status.burn) || statusOn(status.burning)) for (let k = 0; k < 5; k++) {
      const x = u.x - 16 + k * 7, y = u.y - (game.time * 34 + k * 9) % 30;
      box(c, '#e9603e', x, y, 5, 9); box(c, '#ffc66b', x + 1, y + 2, 3, 5);
    }
    if (statusOn(status.poison)) for (let k = 0; k < 4; k++) {
      const a = game.time * 1.8 + k * 1.6;
      box(c, '#a7e957', u.x + Math.cos(a) * 18, u.y - 18 + Math.sin(a) * 11, 4, 4);
    }
    if (statusOn(status.stun)) for (let k = 0; k < 3; k++) {
      const a = game.time * 4 + k * Math.PI * 2 / 3;
      box(c, '#ffe7a3', u.x + Math.cos(a) * 15, u.y - height - 1 + Math.sin(a) * 5, 4, 4);
    }
    if (statusOn(status.stun) && u.kind === 'hero') {
      const remaining = status.stun.remaining || 0, duration = status.stun.duration || status.stun.maxDuration || Math.max(1, remaining);
      box(c, '#332e40', u.x - 17, u.y - height - 14, 34, 4);
      box(c, '#ffe7a3', u.x - 17, u.y - height - 14, 34 * clamp(remaining / duration, 0, 1), 4);
      this.label(c, `${remaining.toFixed(1)}s`, u.x, u.y - height - 19, '#ffebbd', 9);
    }
    if (u.kind === 'beast' && u.team >= 0) this.label(c, u.subtype === 'maw' ? 'LANE HUNTER' : 'TOWER GUARDIAN', u.x, u.y + 24, TEAM[u.team], 9);
    if (u.guardian || u.elderGuardian) { c.save(); c.globalAlpha = .8; ring(c, '#c8edb2', u.x, u.y - 15, width * .45, 3); c.restore(); }
    if (statusOn(status.slow)) {
      c.save(); c.globalAlpha = .75;
      ring(c, status.slow.frost ? '#a4e5ff' : '#c2c5de', u.x, u.y + 2, 17, 2, true);
      if (status.slow.frost) for (let k = 0; k < 4; k++) {
        const a = k * Math.PI / 2 + Math.PI / 4, x = u.x + Math.cos(a) * 16, y = u.y + Math.sin(a) * 9;
        diamond(c, x, y - 5, 5, 14, '#64b3e8', '#d7f7ff');
      }
      c.restore();
    }
    if (statusOn(status.haste)) {
      c.save(); c.globalAlpha = 0.65;
      const color = status.haste.color || (u.heroId === 'seraph' ? '#a3e2ff' : '#ffb8df');
      const direction = Math.cos(u.angle || 0) < 0 ? -1 : 1;
      for (let k = 0; k < 3; k++) line(c, color, u.x - direction * 24, u.y - 7 - k * 5, u.x - direction * (33 + k * 3), u.y - 7 - k * 5, 2); c.restore();
    }
  }
  unitBars(c, u, game) {
    const hero = u.kind === 'hero', structure = u.kind === 'core' || u.kind === 'tower', boss = u.kind === 'beast';
    if (!hero && !structure && !boss && u.hp >= u.maxHp) return;
    const w = u.kind === 'core' ? 104 : hero ? 58 : structure || boss ? 70 : 30;
    const y = u.y - (u.kind === 'core' ? 133 : u.kind === 'tower' ? 116 : boss ? Math.max(105, (u.r || 35) * 2.7) : hero ? 77 : 48);
    const x = u.x - w / 2, h = hero || structure ? 6 : 4;
    box(c, '#102935', x - 2, y - 2, w + 4, h + 4);
    box(c, '#35424d', x, y, w, h);
    const color = u.team < 0 ? '#e3c777' : u === game.player || u.id === game.playerId ? '#b8e67d' : TEAM[u.team];
    box(c, color, x, y, Math.max(0, w * clamp(u.hp / u.maxHp, 0, 1)), h);
    if (u.shield > 0) box(c, '#e5f3ff', x, y, Math.min(w, w * u.shield / u.maxHp), 2);
    const player = game.player;
    if (player?.alive && u.id === player.targetId && u.team !== player.team && !game.isProtected?.(u)) {
      const hit = game.lastHitDamage ? game.lastHitDamage(player, u) : player.damage * (u.status?.guard?.factor ?? 1);
      if (u.hp + (u.shield || 0) <= hit) {
        diamond(c, x + w + 10, y + h / 2, 8, 10, '#ffce69', '#fff4c2');
        this.label(c, 'LAST HIT', u.x, y - 8, '#ffe49a', 9);
      }
    }
    if (structure && game.isProtected?.(u)) {
      box(c, '#a8c5d8', x + w + 6, y + 1, 8, 7);
      c.strokeStyle = '#c8dde4'; c.lineWidth = 2;
      c.strokeRect(x + w + 7, y - 4, 6, 6);
      box(c, '#274459', x + w + 9, y + 3, 2, 3);
    }
    if (boss) {
      const tier = u.tier || u.beastTier || u.level || 1;
      const crownY = y - 17;
      box(c, '#f8d893', u.x - 25, crownY + 1, 12, 7);
      for (let k = 0; k < 3; k++) box(c, '#fff3b6', u.x - 25 + k * 5, crownY - 2, 2, 5);
      this.label(c, 'TIER ' + tier, u.x + 10, crownY + 7, '#ffe8b3', 10);
    }
    if (hero) {
      box(c, '#263952', x, y + h + 3, w, 2);
      box(c, '#66aff0', x, y + h + 3, w * clamp((u.mana || 0) / (u.maxMana || 1), 0, 1), 2);
      box(c, '#172d3c', x - 17, y - 2, 14, 13);
      this.label(c, String(u.level || 1), x - 10, y + 8, '#eff4c7', 10);
      if (u !== game.player && u.id !== game.playerId) this.label(c, HEROES[u.heroId]?.name || u.name || '', u.x, y - 8, u.team === 0 ? '#d4e9dc' : '#ffd1c0', 9);
      const bounty = u.shutdownBounty ?? Math.min(300, Math.max(0, (u.streak || 0) - 2) * 60);
      if (bounty > 0) this.label(c, `+${bounty} BOUNTY`, u.x, y - 22, '#ffd37d', 9);
      drawMagazine(c,u,x+w+8,y+1,game.time);
      drawResonanceResource(c,u,x+w+8,y+1,game.time);
    }
  }
  projectile(c, p, time, game) {
    drawProjectileTrail(c, p, time, this.effectBudget);
    if(drawExpansionProjectile(c,p,time,this.effectBudget,game))return;
    const a = p.angle ?? Math.atan2((p.vy || 0), (p.vx || 1)), color = p.color || TEAM[p.team] || '#e1dc9a';
    const type = String(p.type || 'bolt'), len = /arrow|shot/.test(type) ? 19 : /dragon/.test(type) ? 50 : 12;
    c.save(); c.translate(p.x, p.y); c.rotate(a);
    if (/dragon/.test(type)) {
      for (let i = 5; i >= 0; i--) {
        box(c, i % 2 ? '#e36640' : '#ffad59', -i * 10, -8 + Math.sin(time * 13 - i) * 3, 15, 13);
        box(c, '#ffd783', -i * 10, -3 + Math.sin(time * 13 - i) * 2, 13, 4);
      }
      box(c, '#c54140', -2, -13, 21, 23); box(c, '#ffac5f', 1, -11, 18, 17);
      box(c, '#fff0bd', 12, -8, 5, 3); box(c, '#6e3549', 16, -9, 2, 3);
      box(c, '#fff0bd', 0, -18, 3, 7); box(c, '#ed9e64', -16, -22, 14, 8);
      box(c, '#d85949', -25, -17, 25, 5);
    } else if (/lance/.test(type)) {
      c.globalAlpha = 0.3; box(c, color, -30, -7, 34, 14);
      c.globalAlpha = 1; box(c, color, -25, -3, 31, 6); box(c, '#fff4df', -20, -1, 28, 2);
      box(c, '#ffd0ed', -2, -9, 3, 18); box(c, '#fff4df', -6, -5, 11, 10);
    } else if (/arrow|shot|ranger|poison/.test(type)) {
      box(c, '#6b6e52', -len, -1, len, 3); box(c, '#f0dfad', -len, -1, len, 1);
      box(c, color, -3, -3, 6, 5); box(c, '#e4f3ca', 1, -1, 4, 1);
      box(c, '#e4d9c6', -len, -4, 3, 7);
    } else {
      c.globalAlpha = 0.35; box(c, color, -len - 6, -3, len + 6, 6);
      c.globalAlpha = 1; box(c, color, -6, -5, 10, 10); box(c, '#effbd7', -2, -2, 5, 4);
    }
    c.restore();
  }
  effect(c, e, time, ground) {
    if (isWorldEffect(e)) { drawWorldEffect(c,e,time,ground,this.effectBudget,this.worldEffectOptions); return; }
    if (e.type === 'combat-burst') { drawCombatBurst(c, e, time, ground, this.effectBudget); return; }
    if(drawExpansionEffect(c,e,time,ground,this.effectBudget))return;
    drawSpellAccent(c, e, time, ground, this.effectBudget);
    if (e.type === 'flame') return;
    const type = String(e.type || ''), life = e.maxLife ? clamp(e.life / e.maxLife, 0, 1) : 0.65;
    const progress = 1 - life, color = e.color || '#a9dded', radius = e.r || e.radius || 45;
    const isGround = /field|storm|heal|poison|tree|shield|ring|splash|recall|telegraph|frost|quake/.test(type);
    if (isGround !== ground && !/explosion|level|blink|tree|frost|quake/.test(type)) return;
    c.save();
    if (ground) {
      if (/frost/.test(type)) {
        c.globalAlpha = .2 * life; disc(c, '#b4e9ff', e.x, e.y, radius);
        c.globalAlpha = .85 * life; ring(c, color, e.x, e.y, radius * (.45 + progress * .55), 3);
        for (let k = 0; k < 6; k++) {
          const angle = k * Math.PI / 3, length = radius * (.3 + progress * .6);
          line(c, '#e2f9ff', e.x, e.y, e.x + Math.cos(angle) * length, e.y + Math.sin(angle) * length, 2);
          for (const turn of [-1,1]) {
            const x = e.x + Math.cos(angle) * length * .62, y = e.y + Math.sin(angle) * length * .62;
            line(c, color, x, y, x + Math.cos(angle + turn * .8) * length * .22, y + Math.sin(angle + turn * .8) * length * .22, 2);
          }
        }
      } else if (/quake/.test(type)) {
        c.globalAlpha = life * .8;
        ring(c, color, e.x, e.y, radius * (.22 + progress * .78), 5);
        ring(c, '#eddaff', e.x, e.y, radius * (.1 + progress * .5), 2);
        for (let k = 0; k < 8; k++) {
          const a = k * Math.PI / 4 + .2, reach = radius * progress, points = [];
          for (let j = 0; j < 5; j++) {
            const length = reach * (j / 4), offset = j % 2 ? .12 : -.08;
            points.push({x:e.x + Math.cos(a + offset) * length, y:e.y + Math.sin(a + offset) * length});
          }
          path(c, points, '#211e3f', 6); path(c, points, color, 2);
        }
      } else if (/field|shield/.test(type)) {
        c.globalAlpha = 0.12 * Math.min(1, life * 5); disc(c, color, e.x, e.y, radius);
        c.globalAlpha = 0.75 * Math.min(1, life * 4); ring(c, color, e.x, e.y, radius, 3);
        for (let k = 0; k < 12; k++) { const a = k * Math.PI / 6 + time * 0.2; box(c, '#d8f6fb', e.x + Math.cos(a) * radius, e.y + Math.sin(a) * radius - 4, 3, 8); }
      } else if (/heal|tree/.test(type)) {
        c.globalAlpha = 0.14; disc(c, color, e.x, e.y, radius);
        c.globalAlpha = 0.55; ring(c, color, e.x, e.y, radius * (0.7 + Math.sin(time * 2) * 0.05), 2);
        for (let k = 0; k < 8; k++) {
          const a = k * 2.4, rr = radius * (0.2 + hash(k, e.x) * 0.65);
          const x = e.x + Math.cos(a) * rr, y = e.y + Math.sin(a) * rr - (time * 19 + k * 12) % 25;
          c.globalAlpha = 0.5; box(c, color, x - 2, y - 6, 4, 12); box(c, color, x - 6, y - 2, 12, 4);
        }
      } else if (/storm|poison/.test(type)) {
        c.globalAlpha = 0.16; disc(c, color, e.x, e.y, radius);
        c.globalAlpha = 0.6; ring(c, color, e.x, e.y, radius, 2, true);
        for (let k = 0; k < 12; k++) {
          const a = time * 1.6 + k * 2.4, rr = (0.2 + hash(k, e.y) * 0.75) * radius;
          const x = e.x + Math.cos(a) * rr, y = e.y + Math.sin(a) * rr;
          box(c, color, x, y - 9 - (time * 20 + k) % 18, 3, 12);
          if (/storm/.test(type) && k % 3 === Math.floor(time * 6) % 3) {
            line(c, '#d6f5ff', x, y - 37, x - 7, y - 17, 3); line(c, '#d6f5ff', x - 7, y - 17, x + 2, y - 21, 3); line(c, '#d6f5ff', x + 2, y - 21, x - 4, y, 3);
          }
        }
      } else if (/splash/.test(type)) {
        c.globalAlpha = life * 0.7; ring(c, '#b9ebe1', e.x, e.y, 8 + progress * radius, 2);
        for (let k = 0; k < 5; k++) {
          const a = k * 1.25; box(c, '#e2f5d9', e.x + Math.cos(a) * (8 + progress * 20), e.y + Math.sin(a) * (6 + progress * 15) - Math.sin(progress * Math.PI) * 12, 3, 3);
        }
      } else {
        c.globalAlpha = life * 0.6; ring(c, color, e.x, e.y, /explosion|level|blink/.test(type) ? Math.max(7, radius * progress) : radius, 3, /telegraph/.test(type));
      }
    } else if (/tree/.test(type)) {
      c.globalAlpha = Math.min(1, life * 8);
      tree(c, { x: e.x, y: e.y, r: 24, type: 'tree' }, time);
      box(c, '#c3f7a6', e.x - 3, e.y - 18, 6, 3);
      box(c, '#c3f7a6', e.x - 1, e.y - 20, 2, 7);
    } else if (/sweep/.test(type)) {
      const arc = e.arc || Math.PI * .85, start = (e.angle || 0) - arc / 2;
      const end = start + arc * Math.min(1, .2 + progress * 1.5);
      c.globalAlpha = life * .8; c.strokeStyle = color; c.lineWidth = 9;
      c.beginPath(); c.arc(e.x, e.y - 10, radius * .88, start, end); c.stroke();
      c.strokeStyle = '#f1fbff'; c.lineWidth = 3; c.stroke();
      for (let k = 0; k < 7; k++) {
        const a = start + arc * k / 6, r = radius * (.75 + progress * .23);
        box(c, k % 2 ? color : '#f4fcff', e.x + Math.cos(a) * r, e.y - 10 + Math.sin(a) * r, 3, 5);
      }
    } else if (/dash/.test(type)) {
      const x2 = Number.isFinite(e.x2) ? e.x2 : e.x, y2 = Number.isFinite(e.y2) ? e.y2 : e.y;
      c.globalAlpha = life * .3; line(c, color, e.x, e.y - 18, x2, y2 - 18, 18);
      c.globalAlpha = life * .85;
      for (let k = 0; k < 4; k++) {
        const offset = (k - 1.5) * 7;
        line(c, k % 2 ? color : '#f4f5dd', e.x + (x2 - e.x) * progress, e.y + (y2 - e.y) * progress - 18 + offset, x2, y2 - 18 + offset, 2);
      }
    } else if (/frost/.test(type)) {
      c.globalAlpha = life;
      for (let k = 0; k < 10; k++) {
        const a = k * Math.PI / 5 + .2, r = radius * (.25 + hash(k,e.x) * .65);
        const x = e.x + Math.cos(a) * r, y = e.y + Math.sin(a) * r;
        diamond(c, x, y - Math.sin(progress * Math.PI) * 17, 8 * life + 3, 18 * life + 6, '#69afdf', '#d3f5ff');
      }
    } else if (/quake/.test(type)) {
      c.globalAlpha = life;
      for (let k = 0; k < 10; k++) {
        const a = k * 2.4, r = radius * (.2 + progress * .65);
        box(c, k % 2 ? color : '#ded2ed', e.x + Math.cos(a) * r, e.y + Math.sin(a) * r - Math.sin(progress * Math.PI) * (12 + k * 2), 5, 8);
      }
    } else if (/slash|arc/.test(type)) {
      c.globalAlpha = life;
      c.strokeStyle = color; c.lineWidth = 7;
      c.beginPath(); c.arc(e.x, e.y - 12, radius * (0.65 + progress * 0.3), (e.angle || 0) - 0.85 + progress, (e.angle || 0) + 0.6 + progress); c.stroke();
      c.strokeStyle = '#fff7c7'; c.lineWidth = 2; c.stroke();
    } else if (/dragon/.test(type)) {
      this.projectile(c, { ...e, type: 'dragon' }, time);
    } else {
      c.globalAlpha = life;
      const n = /level/.test(type) ? 18 : 12;
      for (let k = 0; k < n; k++) {
        const a = k * Math.PI * 2 / n + hash(k, e.x) * 0.5;
        const rr = radius * (0.12 + progress * (0.65 + hash(k, e.y) * 0.35));
        const x = e.x + Math.cos(a) * rr, y = e.y + Math.sin(a) * rr - Math.sin(progress * Math.PI) * 24;
        box(c, k % 3 ? color : EFFECT_PALETTES[e.theme]?.light || '#fff3b0', x, y, Math.max(2, 7 * life), Math.max(2, /level/.test(type) ? 12 * life : 7 * life));
      }
      if (/explosion|blink/.test(type) && progress < 0.25) {
        c.globalAlpha = (0.25 - progress) * 2; disc(c, '#fff7c7', e.x, e.y, radius * 0.3);
      }
    }
    c.restore();
  }
  departingSummon(c,e,alpha) {
    if(e.subtype==='sentry'||e.subtype==='mine'){drawDevice(c,e,this.presentationTime||0,this.effectBudget,alpha);return;}
    const sprite=creatureSprite(e.subtype||'spirit',0,e.team??0,Math.cos(e.angle||0)<-.15);
    const bodyWidth=Math.max(48,Math.max(12,(e.r||32)-18)*2.5),scale=bodyWidth/48;
    const rise=/spirit/.test(e.subtype||'')&&!this.effectBudget?.reducedMotion?(1-alpha)*20:0;
    c.save();c.globalAlpha=alpha;
    c.drawImage(sprite,Math.round(e.x-sprite.width*scale/2),Math.round(e.y-(sprite.height-sprite.groundPadding)*scale+7-rise),sprite.width*scale,sprite.height*scale);c.restore();
  }
  floatText(c, t) {
    if (!this.visible(t.x, t.y, 10)) return;
    const life = t.maxLife ? clamp(t.life / t.maxLife, 0, 1) : clamp(t.life, 0, 1);
    c.save(); c.globalAlpha = Math.min(1, life * 2.8);
    const text = String(t.text ?? t.value ?? ''), size = /CRIT|LEVEL|!/.test(text) ? 16 : 13;
    c.font = 'bold ' + size + 'px monospace'; c.textAlign = 'center';
    c.lineWidth = 4; c.strokeStyle = '#172a37'; c.strokeText(text, Math.round(t.x), Math.round(t.y));
    c.fillStyle = t.color || '#fff0c6'; c.fillText(text, Math.round(t.x), Math.round(t.y)); c.restore();
  }
  playerMarker(c, game) {
    const p = game.player;
    if (!p || !alive(p)) return;
    const y = p.y - 103 + Math.sin(game.time * 3) * 2;
    c.fillStyle = '#e8f4b7'; c.beginPath(); c.moveTo(p.x - 5, y); c.lineTo(p.x + 5, y); c.lineTo(p.x, y + 6); c.closePath(); c.fill();
  }
  label(c, text, x, y, color = '#d8e9d2', size = 11) {
    c.font = 'bold ' + size + 'px monospace'; c.textAlign = 'center'; c.textBaseline = 'alphabetic';
    c.lineWidth = 3; c.strokeStyle = '#1a3039'; c.strokeText(text, Math.round(x), Math.round(y));
    c.fillStyle = color; c.fillText(text, Math.round(x), Math.round(y));
  }
  drawPortrait(canvas, heroId, size = 64, cosmetics = {}) { drawPortrait(canvas, heroId, size, cosmetics); }
  drawIcon(canvas, ability, size = 32) { drawIcon(canvas, ability, size); }
  drawMinimap(canvas, game) {
    const c = canvas.getContext('2d'), width = canvas.width || 180, height = canvas.height || 124;
    c.imageSmoothingEnabled = false; c.clearRect(0, 0, width, height);
    c.fillStyle = '#173438'; c.fillRect(0, 0, width, height);
    c.drawImage(this.terrain, 0, 0, width, height);
    c.save(); c.fillStyle = '#10242b66'; c.fillRect(0, 0, width, height);
    const sx = width / WORLD.width, sy = height / WORLD.height;
    const team = game.player?.team ?? 0;
    if (game.visionSources) {
      c.save(); c.beginPath();
      for (const source of game.visionSources(team)) {
        const x = source.x * sx, y = source.y * sy, r = (source.r || source.radius) * sx;
        c.moveTo(x + r, y); c.arc(x, y, r, 0, Math.PI * 2);
      }
      c.clip(); c.drawImage(this.terrain, 0, 0, width, height); c.restore();
    }
    for (const ghost of Object.values(game.lastSeen?.[team] || {})) {
      const unit = game.units.find(u => u.id === ghost.id), age = game.time - ghost.time;
      if (ghost.team === team || !ghost.heroId || age <= .1 || age > 8 || (unit && game.canSee?.(team, unit))) continue;
      c.save(); c.globalAlpha = .5 * (1 - age / 8); c.strokeStyle = TEAM[ghost.team] || '#ffa5a0'; c.lineWidth = 1;
      c.strokeRect(Math.round(ghost.x * sx) - 3, Math.round(ghost.y * sy) - 3, 6, 6); c.restore();
    }
    for (const u of game.units || []) {
      if (!alive(u)) continue;
      if (!['hero', 'tower', 'core', 'beast', 'camp'].includes(u.kind)) continue;
      if (game.canSee && !game.canSee(team, u)) continue;
      const x = Math.round(u.x * sx), y = Math.round(u.y * sy), hero = u.kind === 'hero';
      const r = u.kind === 'core' ? 4 : hero || u.kind === 'beast' ? 3 : 2;
      c.fillStyle = '#10242d'; c.fillRect(x - r - 1, y - r - 1, r * 2 + 2, r * 2 + 2);
      c.fillStyle = u.team < 0 ? u.kind === 'beast' ? '#ffe3a2' : '#a8b47d' : TEAM[u.team];
      c.fillRect(x - r, y - r, r * 2, r * 2);
      if (hero) { c.fillStyle = '#f5e7bd'; c.fillRect(x - 1, y - 1, 2, 2); }
    }
    const cam = this.camera;
    c.strokeStyle = '#e2efd397'; c.lineWidth = 1;
    c.strokeRect(Math.round(cam.x * sx) + 0.5, Math.round(cam.y * sy) + 0.5, cam.width * sx, cam.height * sy);
    if (game.player) {
      c.strokeStyle = '#ffffff'; c.lineWidth = 1;
      c.strokeRect(Math.round(game.player.x * sx) - 4.5, Math.round(game.player.y * sy) - 4.5, 9, 9);
    }
    c.restore();
  }
}

export function drawPortrait(canvas, heroId, size = 64, cosmetics = {}) {
  canvas.width = size; canvas.height = size;
  const c = canvas.getContext('2d'); c.imageSmoothingEnabled = false;
  c.clearRect(0, 0, size, size);
  c.fillStyle = '#142c39'; c.fillRect(0, 0, size, size);
  const color = HEROES[heroId]?.color || '#77bfd6';
  c.globalAlpha = 0.2; disc(c, color, size / 2, size * 0.55, size * 0.44); c.globalAlpha = 1;
  const sprite = heroSprite(heroId, 0, 0, false, cosmetics);
  // The combat frame reserves space for a full sword swing. Crop only that
  // empty margin in the idle portrait so the angel's face stays legible.
  const sourceWidth=['seraph','vela','cadence'].includes(heroId)?64:['brinehook','ferrite'].includes(heroId)?60:['bulwark','ember','rivet','solstice'].includes(heroId)?48:40,sourceX=(sprite.width-sourceWidth)/2,sourceHeight=heroId==='cadence'?49:44,sourceY=Math.max(0,(sprite.bodyTop||0)-(heroId==='cadence'?5:0));
  const scale = Math.min(size * .94 / sourceWidth, size * .97 / sourceHeight);
  const width = Math.round(sourceWidth * scale), height = Math.round(sourceHeight * scale);
  c.drawImage(sprite, sourceX, sourceY, sourceWidth, sourceHeight, Math.round((size-width)/2), Math.round((size-height)/2), width, height);
  const frameColor = {verdant:'#86c99c',astral:'#b49ce8',sovereign:'#ead28f'}[cosmetics?.portrait];
  if (frameColor) {
    c.strokeStyle = frameColor; c.lineWidth = Math.max(2, Math.floor(size / 32)); c.strokeRect(2, 2, size - 4, size - 4);
    for (const [x, y] of [[5,5],[size-5,5],[5,size-5],[size-5,size-5]]) diamond(c, x, y, Math.max(4,size/16), Math.max(6,size/12), frameColor);
  }
}

export function drawIcon(canvas, ability = {}, size = 32) {
  canvas.width = size; canvas.height = size;
  const c = canvas.getContext('2d'); c.imageSmoothingEnabled = false;
  c.clearRect(0, 0, size, size); c.save(); c.scale(size / 32, size / 32);
  // An explicit icon key takes precedence over flavor text: e.g. a dragon's
  // mana talent must show mana, rather than accidentally matching "dragon".
  const id = String(ability.icon || ability.id || ability.name || 'storm').toLowerCase();
  const color = ability.color || '#8ce4f2';
  box(c, '#172d3b', 0, 0, 32, 32);
  box(c, '#25404b', 2, 2, 28, 28); box(c, '#1b3340', 3, 3, 26, 26);
  c.globalAlpha = 0.15; disc(c, color, 16, 16, 13); c.globalAlpha = 1;
  if (drawExpansionIcon(c,id)) {
    // The expansion icons have their own authored silhouettes.
  } else if (/^bash$|^charge$/.test(id)) {
    box(c, color, 4, 7, 12, 16); box(c, color, 7, 22, 6, 5);
    box(c, '#4c456a', 7, 10, 6, 11); box(c, '#eedefc', 9, 11, 2, 9);
    line(c, '#fff3d3', 18, 16, 28, 16, 3); line(c, color, 23, 10, 29, 16, 3); line(c, color, 29, 16, 23, 22, 3);
    if (id === 'charge') { box(c, color, 2, 12, 3, 2); box(c, color, 1, 18, 4, 2); }
    else { box(c, '#f8e3a6', 24, 5, 2, 4); box(c, '#f8e3a6', 24, 24, 2, 4); }
  } else if (/fortress/.test(id)) {
    box(c, '#5d456d', 5, 11, 22, 17); box(c, color, 6, 12, 20, 14);
    box(c, color, 5, 5, 5, 9); box(c, color, 14, 5, 4, 9); box(c, color, 22, 5, 5, 9);
    box(c, '#f0ddff', 6, 6, 3, 5); box(c, '#f0ddff', 23, 6, 3, 5);
    box(c, '#433d68', 13, 19, 6, 9); box(c, '#433d68', 8, 15, 3, 3); box(c, '#433d68', 21, 15, 3, 3);
  } else if (/quake/.test(id)) {
    box(c, '#b29bc1', 14, 10, 4, 13); box(c, color, 8, 6, 17, 8); box(c, '#f3e5ff', 9, 6, 13, 2);
    line(c, color, 4, 27, 10, 23, 2); line(c, color, 10, 23, 13, 27, 2);
    line(c, color, 19, 27, 22, 23, 2); line(c, color, 22, 23, 28, 27, 2);
    box(c, '#fff0b9', 15, 24, 3, 5); box(c, '#fff0b9', 3, 18, 3, 3); box(c, '#fff0b9', 26, 17, 3, 3);
  } else if (/^dagger$|execute|frostblade|^sword$|^damage$/.test(id)) {
    const execute = /execute/.test(id), frost = /frost/.test(id);
    if (execute) { ring(c, '#dc835f', 20, 11, 8, 2); line(c, '#dc835f', 20, 2, 20, 20, 1); }
    line(c, '#e9f4e9', 10, 22, 24, 6, /dagger/.test(id) ? 4 : 6);
    line(c, color, 12, 22, 26, 6, 2); line(c, '#f2c888', 6, 17, 17, 27, 3); line(c, '#946747', 6, 28, 11, 22, 4);
    if (frost) { box(c, '#dbfaff', 4, 8, 2, 9); box(c, '#dbfaff', 1, 11, 8, 2); box(c, '#97d9ff', 23, 24, 3, 5); box(c, '#97d9ff', 21, 26, 7, 2); }
    if (/dagger/.test(id)) { box(c, color, 22, 22, 2, 6); box(c, '#ffe0a4', 23, 25, 3, 3); }
  } else if (/^dash$/.test(id)) {
    box(c, '#dda76f', 9, 12, 7, 10); box(c, color, 10, 19, 13, 6); box(c, '#ffe4ac', 11, 20, 9, 2);
    box(c, color, 3, 13, 5, 2); box(c, color, 1, 18, 6, 2); box(c, color, 2, 24, 6, 2);
    line(c, '#fff2c9', 23, 7, 28, 12, 2); line(c, '#fff2c9', 28, 12, 23, 17, 2);
  } else if (/ambush/.test(id)) {
    box(c, color, 9, 6, 14, 4); box(c, color, 7, 10, 18, 13); box(c, '#543c3b', 10, 12, 12, 9);
    box(c, '#ffe4ac', 12, 14, 3, 2); box(c, '#ffe4ac', 18, 14, 3, 2);
    line(c, '#ecf8ee', 5, 28, 10, 21, 3); line(c, '#ecf8ee', 27, 28, 22, 21, 3);
    box(c, '#fbc481', 3, 24, 6, 2); box(c, '#fbc481', 23, 24, 6, 2); box(c, color, 4, 4, 3, 3);
  } else if (/snowflake|^frost$|freeze/.test(id)) {
    for (let k = 0; k < 6; k++) {
      const a = k * Math.PI / 3;
      line(c, color, 16, 16, 16 + Math.cos(a) * 12, 16 + Math.sin(a) * 12, 2);
      for (const side of [-1,1]) {
        const x = 16 + Math.cos(a) * 8, y = 16 + Math.sin(a) * 8;
        line(c, '#e3f8ff', x, y, x + Math.cos(a + side) * 4, y + Math.sin(a + side) * 4, 2);
      }
    }
    box(c, '#f1fdff', 14, 14, 4, 4);
  } else if (/^wing$/.test(id)) {
    for (const side of [-1,1]) for (let feather = 0; feather < 3; feather++) {
      const x = 14 + side * (3 + feather * 4), y = 8 + feather * 2;
      box(c, color, x, y, 4, 14 - feather * 2); box(c, '#eefbff', x, y, 2, 10 - feather);
    }
    box(c, '#c9efff', 14, 11, 4, 14); box(c, color, 10, 26, 12, 2);
  } else if (/rally/.test(id)) {
    box(c, '#d9ebf1', 7, 5, 3, 24); box(c, color, 10, 6, 17, 12); box(c, '#e7f8ff', 11, 7, 10, 2);
    box(c, '#376494', 21, 13, 7, 5); box(c, '#e7f8ff', 15, 10, 3, 5); box(c, '#e7f8ff', 13, 11, 7, 2);
    line(c, color, 14, 24, 18, 20, 2); line(c, color, 18, 20, 22, 24, 2);
  } else if (/health|heart|vigor|lifesteal/.test(id)) {
    box(c, color, 5, 8, 9, 10); box(c, color, 18, 8, 9, 10); box(c, color, 8, 14, 16, 8); box(c, color, 12, 21, 8, 4); box(c, color, 15, 25, 3, 3);
    box(c, '#fff4df', 7, 10, 5, 3);
    if (/lifesteal/.test(id)) { box(c, '#fff4df', 10, 14, 3, 7); box(c, '#fff4df', 20, 14, 3, 7); }
    else { box(c, '#eaffeb', 15, 12, 3, 10); box(c, '#eaffeb', 12, 15, 9, 3); }
  } else if (/mana|reserve/.test(id)) {
    diamond(c, 16, 15, 15, 22, color, '#d9f6ff'); box(c, color, 7, 28, 18, 2);
    box(c, '#d9f6ff', 4, 7, 3, 3); box(c, '#d9f6ff', 26, 20, 3, 3);
  } else if (/cooldown|tempo|clock/.test(id)) {
    ring(c, color, 16, 16, 11, 3); line(c, '#e9f8e5', 16, 8, 16, 16, 3); line(c, '#e9f8e5', 16, 16, 22, 19, 2);
    box(c, color, 23, 4, 5, 3); box(c, color, 25, 5, 3, 7);
  } else if (/range|eye|reach|hunt/.test(id)) {
    ring(c, color, 16, 16, 9, 2); box(c, '#f0f9dc', 14, 14, 4, 4);
    line(c, color, 16, 2, 16, 10, 2); line(c, color, 16, 22, 16, 30, 2);
    line(c, color, 2, 16, 10, 16, 2); line(c, color, 22, 16, 30, 16, 2);
  } else if (/area/.test(id)) {
    ring(c, color, 16, 16, 10, 2, true); disc(c, '#e6f6e5', 16, 16, 3);
    for (const angle of [0,Math.PI/2,Math.PI,Math.PI*1.5]) line(c, color, 16+Math.cos(angle)*5, 16+Math.sin(angle)*5, 16+Math.cos(angle)*13, 16+Math.sin(angle)*13, 2);
  } else if (/shield|field|ward|barrier|aegis|armor/.test(id)) {
    c.fillStyle = color; c.beginPath(); c.moveTo(6, 7); c.lineTo(16, 4); c.lineTo(26, 7); c.lineTo(24, 20); c.lineTo(16, 28); c.lineTo(8, 20); c.closePath(); c.fill();
    c.fillStyle = '#294e66'; c.beginPath(); c.moveTo(9, 9); c.lineTo(16, 7); c.lineTo(23, 9); c.lineTo(21, 19); c.lineTo(16, 24); c.lineTo(11, 19); c.closePath(); c.fill();
    box(c, '#effddd', 14, 10, 3, 10); box(c, '#effddd', 11, 13, 9, 3);
  } else if (/tree|grove|bloom|heal|sanctuary|renew/.test(id)) {
    if (/heal|sanctuary|renew/.test(id)) {
      box(c, color, 12, 5, 8, 22); box(c, color, 5, 12, 22, 8);
      box(c, '#fff8e1', 14, 8, 3, 13); box(c, '#fff8e1', 8, 14, 13, 3);
    } else {
      box(c, '#8b6e45', 14, 16, 5, 13); disc(c, '#347d52', 16, 12, 11); disc(c, color, 13, 10, 8);
      box(c, '#e4efbc', 9, 6, 6, 3); box(c, '#b0d98d', 17, 11, 5, 2);
    }
  } else if (/wolf|bear|treant|spirit|summon|pack|legion/.test(id)) {
    const type = /wolf|pack/.test(id) ? 'wolf' : /bear/.test(id) ? 'bear' : /treant/.test(id) ? 'treant' : 'spirit';
    const sprite=creatureSprite(type,0,-1,false);
    c.drawImage(sprite,(sprite.width-48)/2,sprite.bodyTop||0,48,44,2,2,30,29);
  } else if (/arrow|shot|fan|long|poison|venom/.test(id)) {
    if (/fan|arrows/.test(id)) for (const off of [-7, 7]) { line(c, color, 7, 25, 24 + off * 0.3, 7 + off, 2); box(c, '#e5f6c1', 22 + off * 0.3, 5 + off, 4, 4); }
    line(c, '#d5c28b', 7, 25, 24, 8, 3); box(c, color, 19, 5, 9, 4); box(c, color, 24, 5, 4, 9);
    box(c, '#edf5c5', 6, 21, 6, 3); box(c, '#edf5c5', 8, 23, 3, 5);
    if (/poison|venom/.test(id)) { box(c, '#adf06a', 8, 9, 3, 5); box(c, '#adf06a', 5, 13, 3, 3); }
  } else if (/dragon/.test(id)) {
    box(c, '#b34d47', 6, 11, 18, 13); box(c, color, 10, 7, 15, 13);
    box(c, '#ffe2a4', 12, 4, 3, 6); box(c, '#ffe2a4', 21, 4, 3, 5);
    box(c, '#fff3be', 20, 11, 3, 3); box(c, '#472d39', 21, 11, 1, 2);
    box(c, '#ffc66f', 23, 18, 7, 5); box(c, '#f28e4b', 25, 21, 5, 5);
    box(c, '#e99163', 4, 18, 7, 6); box(c, '#c15f4d', 2, 24, 7, 3);
  } else if (/slash|cleave/.test(id)) {
    line(c, '#fff1ce', 9, 24, 24, 7, 5); line(c, color, 11, 24, 26, 7, 2);
    line(c, '#f0c983', 6, 17, 16, 27, 3); line(c, '#9b6f51', 6, 27, 11, 22, 4);
    box(c, color, 5, 8, 3, 3); box(c, color, 9, 4, 3, 3);
  } else if (/lance/.test(id)) {
    line(c, color, 6, 27, 24, 8, 4); line(c, '#fff0f3', 8, 26, 24, 8, 1);
    box(c, '#ffddf2', 21, 4, 3, 11); box(c, '#ffddf2', 17, 8, 11, 3);
    box(c, '#fff4dd', 20, 7, 5, 5);
  } else if (/bomb|stun/.test(id)) {
    disc(c, '#a69cbd', 15, 18, 10); disc(c, '#5e5875', 14, 17, 8);
    box(c, '#b0c574', 12, 12, 5, 4); box(c, '#e8c78b', 17, 6, 3, 6);
    box(c, '#fff0aa', 21, 3, 3, 6); box(c, '#fff0aa', 19, 5, 7, 2);
  } else if (/speed|inspire|wing|haste|wind/.test(id)) {
    for (let i = 0; i < 3; i++) { line(c, color, 7 + i * 6, 7, 15 + i * 6, 16, 3); line(c, color, 15 + i * 6, 16, 7 + i * 6, 25, 3); }
  } else if (/flame|fire|nova|burn/.test(id)) {
    box(c, '#c34743', 6, 14, 20, 12); box(c, color, 9, 10, 15, 15);
    box(c, '#ffbb68', 13, 5, 7, 18); box(c, '#ffedaa', 13, 15, 6, 11);
    box(c, '#eb7550', 6, 10, 3, 10); box(c, '#ffad62', 23, 9, 3, 14);
  } else if (/blink|step|rift/.test(id)) {
    ring(c, color, 16, 16, 11, 3); ring(c, '#e9f9d7', 16, 16, 6, 2);
    box(c, color, 2, 14, 5, 3); box(c, color, 25, 14, 5, 3);
  } else {
    // Storm / ranged arcane bolt.
    box(c, color, 18, 4, 7, 5); box(c, color, 14, 8, 8, 5); box(c, color, 10, 12, 8, 5);
    box(c, '#dff9f1', 13, 15, 11, 4); box(c, color, 17, 18, 5, 5); box(c, color, 13, 22, 6, 5);
    box(c, '#eefce5', 7, 7, 3, 3); box(c, '#a1def3', 25, 23, 3, 3);
  }
  c.restore();
}

