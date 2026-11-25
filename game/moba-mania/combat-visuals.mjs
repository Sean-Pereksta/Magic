// Cosmetic Canvas effects use their own stateless noise. Rendering never touches
// the match RNG, stores particle entities, or changes a combat value.
const TAU = Math.PI * 2;
const clamp = (v, a, b) => Math.max(a, Math.min(b, v));
const snap = n => Math.round(n / 2) * 2;
const validColor = c => typeof c === 'string' && /^#[0-9a-f]{6}$/i.test(c);
export const EFFECT_PALETTES = Object.freeze({
  steel: Object.freeze({ main:'#edc58d', light:'#fff4d0', dark:'#a47c65' }),
  fire: Object.freeze({ main:'#ff964d', light:'#ffe39a', dark:'#c94d49' }),
  arcane: Object.freeze({ main:'#67cfff', light:'#d8f8ff', dark:'#536ed0' }),
  nature: Object.freeze({ main:'#82dd8b', light:'#d6ffac', dark:'#3d9969' }),
  poison: Object.freeze({ main:'#a8df67', light:'#e4ffab', dark:'#668d49' }),
  holy: Object.freeze({ main:'#f7a8da', light:'#fff0b4', dark:'#bc75b8' }),
  frost: Object.freeze({ main:'#8ad8ff', light:'#e2fbff', dark:'#548fcf' }),
  violet: Object.freeze({ main:'#bf94ef', light:'#f2ddff', dark:'#7857a9' }),
  stone: Object.freeze({ main:'#c3b18a', light:'#f4e0b2', dark:'#776b65' }),
});
const HERO_THEME = Object.freeze({ arc:'arcane', dragon:'fire', druid:'nature', angel:'holy', ranger:'poison', bulwark:'violet', ember:'fire', seraph:'frost' });
const palette = theme => EFFECT_PALETTES[theme] || EFFECT_PALETTES.steel;
function noise(seed, index) {
  let n = (seed ^ Math.imul(index + 1, 0x9e3779b1)) >>> 0;
  n ^= n >>> 16; n = Math.imul(n, 0x7feb352d); n ^= n >>> 15;
  n = Math.imul(n, 0x846ca68b); n ^= n >>> 16;
  return (n >>> 0) / 4294967296;
}
function seedOf(e) { return e.seed ?? ((Math.round(e.x || 0) * 73856093) ^ (Math.round(e.y || 0) * 19349663) ^ ((e.sourceId || e.id || 0) * 83492791)); }
function pixel(c, color, x, y, w = 3, h = w) {
  c.fillStyle = color; c.fillRect(snap(x), snap(y), Math.max(2, snap(w)), Math.max(2, snap(h)));
}
function stroke(c, color, x, y, x2, y2, width = 2) {
  c.strokeStyle = color; c.lineWidth = width; c.beginPath(); c.moveTo(snap(x), snap(y)); c.lineTo(snap(x2), snap(y2)); c.stroke();
}
function contour(c, x, y, rx, ry, start = 0, end = TAU, fill = false) {
  c.beginPath();
  const steps = Math.max(4, Math.ceil((end - start) / TAU * 16));
  for (let i = 0; i <= steps; i++) {
    const a = start + (end - start) * i / steps, xx = snap(x + Math.cos(a) * rx), yy = snap(y + Math.sin(a) * ry);
    if (i) c.lineTo(xx, yy); else c.moveTo(xx, yy);
  }
  if (fill) { c.closePath(); c.fill(); } else c.stroke();
}
function crystal(c, color, light, x, y, size) {
  pixel(c, color, x - size / 2, y - size, size, size * 2);
  pixel(c, light, x - size, y - size / 2, size * 2, size);
}
function take(budget, count = 1, channel = '') {
  if (!budget) return true;
  const key = channel && channel + 'Remaining';
  if (budget.remaining < count || (key && budget[key] < count)) return false;
  budget.remaining -= count; if (key) budget[key] -= count;
  return true;
}
export function createEffectBudget(width = 1280, reducedMotion = false) {
  return resetEffectBudget({}, width, reducedMotion);
}
export function resetEffectBudget(budget, width = 1280, reducedMotion = false) {
  const small = width < 900;
  budget.reducedMotion = !!reducedMotion;
  budget.limit = reducedMotion ? 100 : small ? 260 : 480;
  budget.remaining = budget.limit;
  // Trails and ambient spell motes cannot use the allowance reserved for hits.
  budget.trailRemaining = reducedMotion ? 10 : small ? 40 : 80;
  budget.accentRemaining = reducedMotion ? 24 : small ? 60 : 120;
  return budget;
}
export function shieldTheme(unit) {
  const theme = unit.shield > 0 ? unit.shieldVisual?.theme : unit.status?.guard?.visualTheme;
  return EFFECT_PALETTES[theme] ? theme : HERO_THEME[unit.heroId] || 'arcane';
}
export function isEffectVisible(game, team, effect) {
  if (game.pointVisible && !game.pointVisible(team, effect.x, effect.y)) return false;
  const id = effect.visibilityId ?? effect.followId;
  if (id == null) return true;
  const target = game.units?.find(unit => unit.id === id);
  if (target?.alive !== false && target?.hp > 0) return !game.canSee || game.canSee(team, target);
  // A corpse can no longer pass canSee. Only viewers who saw the hit may see its
  // brief death burst, and the point itself must still be inside current sight.
  return Array.isArray(effect.visibleTo) && effect.visibleTo.includes(team);
}

/** Directional flecks, rising healing sparks and faceted shield fragments. */
export function drawCombatBurst(c, e, time, ground = false, budget) {
  if (!Number.isFinite(e.x) || !Number.isFinite(e.y) || !(e.life > 0)) return 0;
  const life = clamp(e.life / Math.max(.01, e.maxLife || .5), 0, 1), t = 1 - life;
  const theme = e.theme || 'steel', colors = palette(theme), fx = e.fx || 'hit';
  const power = clamp(e.power || 1, .5, 2), radius = clamp(e.r || 32, 12, 90);
  const seed = seedOf(e), reduced = budget?.reducedMotion, travel = reduced ? .42 : 1;
  const color = validColor(e.color) ? e.color : colors.main;
  let emitted = 0;
  c.save();
  if (ground) {
    if ((fx === 'death' || fx === 'shield-break' || (fx === 'hit' && power > 1.2)) && t < .82 && take(budget)) {
      c.globalAlpha = life * .38; c.strokeStyle = color; c.lineWidth = 2;
      contour(c, e.x, e.y, 6 + radius * t * travel, 3 + radius * t * .48 * travel); emitted++;
    }
    c.restore(); return emitted;
  }
  const healing = fx === 'heal', cast = fx === 'cast', shatter = fx === 'shield-break', death = fx === 'death';
  const total = reduced ? (shatter || death ? 7 : 4) : Math.round((shatter ? 21 : death ? 20 : healing ? 8 : cast ? 9 : fx === 'shield-hit' ? 10 : 12) * Math.sqrt(power));
  const cy = e.y - Math.min(29, radius * .53), angle = Number.isFinite(e.angle) ? e.angle : -Math.PI / 2;
  // The central glint is small and local; no full-screen flashes or shake.
  if (!healing && !cast && t < .24 && take(budget)) {
    c.globalAlpha = (1 - t / .24) * .85;
    const size = (shatter ? 10 : 6) * power;
    pixel(c, colors.light, e.x - size, cy - 2, size * 2, 4);
    pixel(c, color, e.x - 2, cy - size, 4, size * 2); emitted++;
  }
  for (let k = 0; k < total; k++) {
    const fade = clamp(life * (1.3 + noise(seed, k + 42) * .7) - noise(seed, k + 71) * .25, 0, 1);
    if (fade <= 0 || !take(budget)) continue;
    const radial = healing || cast || shatter || death;
    const a = radial ? k / total * TAU + noise(seed, k) * .8 : angle + (noise(seed, k) - .5) * 3.8;
    const reach = radius * (radial ? .85 : 1.15) * (0.18 + t * (1 + noise(seed, k + 19))) * travel;
    let x = e.x + Math.cos(a) * reach, y = cy + Math.sin(a) * reach * .65;
    if (healing || cast) {
      x = e.x + Math.cos(a) * radius * (healing ? .65 : .35);
      y = e.y - 8 - t * (healing ? 46 : 29) * travel + Math.sin(a) * radius * .3;
    } else y -= Math.sin(t * Math.PI) * (shatter || death ? 25 : 15) * travel;
    const size = Math.max(2, (shatter || death ? 5 : 4) * (.6 + noise(seed, k + 9) * .6) * (1 - t * .5));
    const tint = k % 3 ? color : colors.light;
    c.globalAlpha = fade * (healing || cast ? .8 : .95);
    if (shatter || theme === 'frost') {
      crystal(c, tint, colors.light, x, y, size);
    } else if (healing || theme === 'holy') {
      pixel(c, tint, x - size, y - 1, size * 2, 2);
      pixel(c, k % 2 ? colors.light : tint, x - 1, y - size, 2, size * 2);
    } else if (theme === 'fire') {
      pixel(c, colors.dark, x - size / 2, y - size, size, size * 2);
      pixel(c, tint, x - size / 2, y, size, size);
    } else if (theme === 'nature' || theme === 'poison') {
      pixel(c, tint, x - size / 2, y - size, size, size * 1.7);
      pixel(c, colors.light, x, y - size, 2, 2);
    } else {
      pixel(c, tint, x, y, size, size);
      if (k % 3 === 0 && !reduced) {
        c.globalAlpha *= .4;
        pixel(c, colors.dark, x - Math.cos(a) * 7, y - Math.sin(a) * 5, size + 2, 2);
      }
    }
    emitted++;
  }
  c.restore(); return emitted;
}

/** A thin, translucent shell is drawn on both sides of the actual hero sprite. */
export function drawShieldShell(c, unit, time, width, height, front = true, budget) {
  const guard = unit.status?.guard;
  const guarding = guard && (guard === true || guard.remaining > 0 || guard.time > 0 || guard.until > time);
  if (unit.alive === false || !(unit.hp > 0) || (!(unit.shield > 0) && !guarding)) return false;
  const theme = shieldTheme(unit), colors = palette(theme), reduced = budget?.reducedMotion;
  const rx = clamp(width * .57, 24, 56), ry = clamp(height * .57, 29, 57);
  const x = unit.x, y = unit.y - height * .47 + 2;
  const at = unit.shieldVisual?.startedAt;
  const entrance = Number.isFinite(at) ? .6 + .4 * clamp((time - at) / .18, 0, 1) : 1;
  const remaining = unit.shield > 0 ? unit.shieldTime : guard?.remaining;
  const fade = entrance * (unit.shield > 0 ? 1 : .65) * (Number.isFinite(remaining) ? .35 + .65 * clamp(remaining / .45, 0, 1) : 1);
  const breath = reduced ? 1 : .95 + Math.sin(time * 2.8 + (unit.id || 0)) * .05;
  c.save();
  // Total tint over the sprite is below 9%; the back layer cannot obscure it.
  c.globalAlpha = (front ? .065 : .10) * fade; c.fillStyle = colors.main;
  contour(c, x, y, rx, ry, 0, TAU, true);
  c.globalAlpha = (front ? .65 : .34) * fade * breath;
  c.strokeStyle = colors.main; c.lineWidth = front ? 3 : 2;
  contour(c, x, y, rx, ry, front ? 0 : Math.PI, front ? Math.PI : TAU);
  if (front) {
    c.globalAlpha = .26 * fade; c.strokeStyle = colors.light; c.lineWidth = 2;
    contour(c, x, y, rx - 4, ry - 4, .14, Math.PI - .14);
    // Angular reflections sit at the edge, leaving the face and weapon clear.
    c.globalAlpha = .64 * fade;
    pixel(c, colors.light, x - rx * .8, y - ry * .48, 3, 10);
    pixel(c, colors.light, x - rx * .68, y - ry * .66, 7, 3);
    c.globalAlpha = .18 * fade;
    stroke(c, colors.light, x - rx * .86, y - ry * .2, x - rx * .53, y + ry * .54);
    stroke(c, colors.light, x + rx * .8, y - ry * .16, x + rx * .52, y + ry * .6);
    for (let k = 0; k < 6; k++) {
      if (!take(budget, 1, 'accent')) break;
      const a = .18 + k * (Math.PI - .36) / 5, xx = x + Math.cos(a) * rx * .88, yy = y + Math.sin(a) * ry * .87;
      c.globalAlpha = .5 * fade;
      if (theme === 'fire') {
        // Overlapping scale-shaped plates and upward embers distinguish Dragon.
        c.globalAlpha = .3 * fade;
        c.fillStyle = colors.dark; c.beginPath(); c.moveTo(snap(xx - 5), snap(yy - 2)); c.lineTo(snap(xx + 5), snap(yy - 2)); c.lineTo(snap(xx), snap(yy + 7)); c.closePath(); c.fill();
        stroke(c, colors.light, xx - 4, yy - 2, xx + 4, yy - 2);
      } else if (theme === 'frost') crystal(c, colors.main, colors.light, xx, yy, 3);
      else if (theme === 'holy') {
        pixel(c, colors.light, xx - 4, yy, 8, 2); pixel(c, colors.main, xx, yy - 4, 2, 8);
      } else if (theme === 'violet') {
        c.strokeStyle = colors.light; c.lineWidth = 2;
        contour(c, xx, yy, 5, 7, 0, TAU);
      } else if (theme === 'nature' || theme === 'poison') {
        pixel(c, colors.main, xx - 3, yy - 4, 4, 8); pixel(c, colors.light, xx + 1, yy - 6, 3, 5);
      } else {
        pixel(c, colors.light, xx - 4, yy - 2, 8, 2);
        pixel(c, colors.main, xx - 1, yy - 5, 2, 8);
      }
    }
    if (!reduced) for (let k = 0; k < 3; k++) {
      if (!take(budget, 1, 'accent')) break;
      const phase = (time * .6 + k / 3 + (unit.id || 0) * .13) % 1, sign = k % 2 ? 1 : -1;
      c.globalAlpha = Math.sin(phase * Math.PI) * .55 * fade;
      pixel(c, k % 2 ? colors.main : colors.light, x + sign * rx * .9, y + ry * .55 - phase * ry * 1.3, 3, theme === 'fire' ? 6 : 3);
    }
  }
  c.restore(); return true;
}

/** Short, spaced motes reinforce projectile direction without particle storage. */
export function drawProjectileTrail(c, p, time, budget) {
  if (!Number.isFinite(p.x) || !Number.isFinite(p.y)) return 0;
  const type = String(p.type || 'bolt'), fire = /dragon|fire/.test(type) || p.damageType === 'fire';
  const arrow = /arrow|shot|ranger|poison/.test(type), lance = /lance/.test(type);
  const theme = p.theme || (fire ? 'fire' : lance ? 'holy' : p.poison ? 'poison' : arrow ? 'steel' : 'arcane');
  const colors = palette(theme), color = validColor(p.color) ? p.color : colors.main;
  const reduced = budget?.reducedMotion, total = reduced ? 2 : fire ? 11 : lance ? 8 : arrow ? 5 : 6;
  const angle = p.angle ?? Math.atan2(p.vy || 0, p.vx || 1), dx = Math.cos(angle), dy = Math.sin(angle);
  const seed = seedOf(p), phase = reduced ? .4 : (time * 13) % 1, length = fire ? 112 : lance ? 86 : arrow ? 58 : 65;
  let emitted = 0; c.save();
  for (let k = 0; k < total; k++) {
    if (!take(budget, 1, 'trail')) break;
    const t = (k + phase) / total, reach = (fire ? 27 : 12) + t * length;
    const offset = (noise(seed, k) - .5) * (arrow ? 4 : 9) * t;
    const x = p.x - dx * reach - dy * offset, y = p.y - dy * reach + dx * offset - (fire ? t * t * 16 : 0);
    c.globalAlpha = (1 - t) * (arrow ? .46 : .64);
    pixel(c, k % 3 ? color : colors.light, x, y, fire ? 6 - t * 4 : 4 - t * 2, fire ? 5 - t * 3 : 2);
    emitted++;
  }
  c.restore(); return emitted;
}

/** Ambient ability signatures share a small allowance, leaving hits readable. */
export function drawSpellAccent(c, e, time, ground = false, budget) {
  if (!Number.isFinite(e.x) || !Number.isFinite(e.y) || !(e.life > 0)) return 0;
  const type = String(e.type || ''), theme = e.theme || (type === 'flame' ? 'fire' : /frost/.test(type) ? 'frost' : /poison/.test(type) ? 'poison' : /storm/.test(type) ? 'arcane' : 'steel');
  const colors = palette(theme), color = validColor(e.color) ? e.color : colors.main;
  const radius = clamp(e.r || e.radius || 45, 8, 400), life = clamp(e.life / Math.max(.01, e.maxLife || .6), 0, 1), t = 1 - life;
  const reduced = budget?.reducedMotion, seed = seedOf(e);
  let emitted = 0; c.save();
  if (type === 'flame') {
    if (ground && take(budget, 1, 'accent')) {
      c.globalAlpha = .55 * life; c.strokeStyle = colors.main; c.lineWidth = 4;
      contour(c, e.x, e.y, radius * (.3 + t * .7), radius * (.3 + t * .7)); emitted++;
    } else if (!ground) {
      const n = reduced ? 8 : 20;
      for (let k = 0; k < n; k++) {
        if (!take(budget, 1, 'accent')) break;
        const a = k / n * TAU, r = radius * (.3 + t * .7);
        const x = e.x + Math.cos(a) * r, y = e.y + Math.sin(a) * r;
        const h = (12 + noise(seed, k) * 20) * life;
        c.globalAlpha = life * .88;
        pixel(c, colors.dark, x - 5, y - h, 9, h + 4);
        pixel(c, colors.main, x - 3, y - h * .75, 6, h * .75);
        pixel(c, colors.light, x - 1, y - h * .3, 3, h * .3); emitted++;
      }
    }
    c.restore(); return emitted;
  }
  if (ground || !/field|storm|poison|heal|tree|ring|blink|explosion/.test(type)) { c.restore(); return 0; }
  const ambient = /field|storm|heal|tree/.test(type), total = reduced ? 3 : /storm|field/.test(type) ? 9 : 7;
  for (let k = 0; k < total; k++) {
    if (!take(budget, 1, 'accent')) break;
    const a = k / total * TAU + noise(seed, k) * .45;
    const phase = reduced ? .4 : (time * (/storm/.test(type) ? 1.5 : .38) + noise(seed, k + 12)) % 1;
    const r = radius * (ambient ? .65 + noise(seed, k + 4) * .27 : .3 + t * .7);
    const x = e.x + Math.cos(a) * r, y = e.y + Math.sin(a) * r - (ambient ? phase * 19 : Math.sin(t * Math.PI) * 17);
    c.globalAlpha = (ambient ? Math.sin(phase * Math.PI) * .6 : life * .68) * Math.min(1, life * 6);
    if (/storm/.test(type) && k % 3 === 0 && !reduced) {
      stroke(c, colors.light, x + 4, y - 28, x - 4, y - 12, 2);
      stroke(c, color, x - 4, y - 12, x + 3, y - 14, 2);
      stroke(c, colors.light, x + 3, y - 14, x, y, 2);
    } else if (/heal|tree/.test(type) || theme === 'holy') {
      pixel(c, colors.light, x - 4, y - 1, 8, 2); pixel(c, color, x - 1, y - 4, 2, 8);
    } else if (/field/.test(type)) {
      pixel(c, color, x - 3, y - 9, 6, 3); pixel(c, colors.light, x - 3, y - 9, 2, 10);
    } else if (theme === 'frost') crystal(c, color, colors.light, x, y, 3);
    else {
      pixel(c, color, x, y, 4, theme === 'fire' ? 7 : 4);
      if (k % 3 === 0) pixel(c, colors.light, x, y, 2, 2);
    }
    emitted++;
  }
  c.restore(); return emitted;
}
