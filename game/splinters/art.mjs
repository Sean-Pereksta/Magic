// All card artwork is drawn locally so the deck needs no image requests.
const W = 256, H = 360;
const fronts = new Map();
let back;
const INK = '#45382c';
const PALETTE = {
  ember: '#9b321c', moon: '#304d87', moth: '#79531c', thorn: '#28532c',
  key: '#805414', eye: '#663e80', mushroom: '#942b3a', bone: '#454539'
};

// Coordinates belong to a 100-square engraving. Shared between Canvas and SVG.
const DESIGNS = {
  ember: {
    paths: [
      'M51 7 C61 28 43 32 58 45 C62 30 75 34 73 19 C91 44 93 59 83 76 C74 93 32 98 19 75 C8 55 23 38 27 25 C27 48 39 45 37 32 C36 21 45 20 51 7 Z',
      'M49 47 C56 59 67 62 63 76 C59 91 34 90 32 74 C30 64 42 59 49 47 Z',
      'M24 57 C20 67 25 76 31 81 M72 48 C81 60 76 75 69 80',
      'M13 30 L10 23 M83 12 L87 5 M9 51 L3 48 M90 37 L97 32',
      'M47 62 C41 71 39 76 45 81 M52 76 L55 81'
    ],
    fill: 'M51 7 C61 28 43 32 58 45 C62 30 75 34 73 19 C91 44 93 59 83 76 C74 93 32 98 19 75 C8 55 23 38 27 25 C27 48 39 45 37 32 C36 21 45 20 51 7 Z'
  },
  moon: {
    paths: [
      'M69 10 C34 12 20 54 45 76 C54 85 66 89 79 85 C50 109 7 87 9 51 C10 20 41 0 69 10 Z',
      'M24 30 C15 48 20 67 32 77 M29 23 L26 27 M39 85 L44 87',
      'M75 25 L78 34 L87 37 L78 40 L75 49 L72 40 L63 37 L72 34 Z',
      'M89 58 L91 64 L97 66 L91 68 L89 74 L87 68 L81 66 L87 64 Z',
      'M45 26 L47 31 M49 27 L43 30 M60 64 L63 64 M61.5 62.5 L61.5 65.5'
    ],
    fill: 'M69 10 C34 12 20 54 45 76 C54 85 66 89 79 85 C50 109 7 87 9 51 C10 20 41 0 69 10 Z'
  },
  moth: {
    paths: [
      'M47 45 C34 24 18 17 6 21 C5 42 13 55 40 59 C18 57 8 68 14 86 C32 89 43 76 49 62 M53 45 C66 24 82 17 94 21 C95 42 87 55 60 59 C82 57 92 68 86 86 C68 89 57 76 51 62',
      'M49 38 C43 44 45 64 50 77 C55 64 57 44 51 38 Z',
      'M47 38 C47 28 35 24 36 15 M53 38 C53 28 65 24 64 15',
      'M42 50 C24 30 15 31 12 31 M58 50 C76 30 85 31 88 31',
      'M38 54 C23 40 15 42 13 43 M62 54 C77 40 85 42 87 43',
      'M40 64 C31 64 25 74 24 79 M60 64 C69 64 75 74 76 79',
      'M27 43 C18 33 15 42 21 47 C29 54 33 48 27 43 Z M73 43 C82 33 85 42 79 47 C71 54 67 48 73 43 Z',
      'M34 73 L30 78 M66 73 L70 78 M48 49 L52 49 M47 54 L53 54 M48 59 L52 59',
      'M7 18 L4 13 M93 18 L96 13 M50 14 L50 7 M46 11 L54 11'
    ]
  },
  thorn: {
    paths: [
      'M43 94 C67 69 28 48 55 9 M50 92 C77 66 34 47 61 8',
      'M46 78 L24 72 L42 66 M58 65 L78 55 L59 54 M45 49 L25 39 L47 39 M54 25 L75 17 L59 17',
      'M44 61 C26 58 16 49 13 36 C26 37 37 44 44 61 Z',
      'M60 45 C77 45 87 35 89 25 C76 24 65 31 60 45 Z',
      'M23 45 L39 56 M32 45 L33 51 M72 36 L64 42 M74 28 L73 36',
      'M53 17 C45 13 45 8 49 4 M61 12 C67 6 68 3 66 1',
      'M25 88 L28 91 M82 75 L85 72 M14 62 L17 64'
    ]
  },
  key: {
    paths: [
      'M47 7 C34 3 24 13 27 25 C13 24 7 35 12 44 C17 54 30 54 36 46 L65 75 L62 82 L69 89 L75 83 L82 90 L91 81 L52 42 C61 36 60 23 52 20 C57 15 54 9 47 7 Z',
      'M37 20 C31 13 27 21 33 27 C39 33 44 26 37 20 Z',
      'M25 34 C17 30 15 38 21 42 C29 46 32 37 25 34 Z',
      'M43 31 C36 31 36 40 43 40 C50 40 50 31 43 31 Z',
      'M45 46 L80 81 M63 69 L68 64 M72 78 L77 73',
      'M19 17 C15 19 12 23 11 27 M59 8 L64 3 M72 14 L79 10 M8 61 L3 63',
      'M72 45 L74 50 L79 52 L74 54 L72 59 L70 54 L65 52 L70 50 Z'
    ],
    fill: 'M47 7 C34 3 24 13 27 25 C13 24 7 35 12 44 C17 54 30 54 36 46 L65 75 L62 82 L69 89 L75 83 L82 90 L91 81 L52 42 C61 36 60 23 52 20 C57 15 54 9 47 7 Z'
  },
  eye: {
    paths: [
      'M4 51 C22 25 63 13 96 50 C70 83 30 81 4 51 Z',
      'M7 51 C32 31 68 29 93 50 M11 58 C34 73 67 74 89 58',
      'M69 50 C69 76 30 75 30 50 C30 25 69 25 69 50 Z',
      'M55 35 C48 39 45 47 47 56 C49 65 57 60 58 53 C59 45 56 40 55 35 Z',
      'M39 37 L42 42 M35 46 L41 47 M36 59 L42 56 M43 65 L46 60 M57 65 L55 60 M64 58 L59 56 M64 44 L60 46',
      'M17 31 L11 23 M33 22 L30 12 M51 18 L51 6 M69 22 L74 12 M84 32 L91 23',
      'M19 73 L13 81 M37 80 L34 90 M54 81 L55 94 M73 76 L78 86 M88 66 L96 72',
      'M50 23 L50 26 M24 13 L26 17 M81 15 L79 19'
    ]
  },
  mushroom: {
    paths: [
      'M8 48 C11 21 32 8 49 9 C72 8 90 25 94 48 C78 57 27 60 8 48 Z',
      'M9 48 C29 39 76 39 94 48 M14 50 C35 65 73 61 89 51',
      'M42 57 C44 72 40 85 33 90 C46 97 64 97 69 89 C58 79 60 65 59 57',
      'M49 61 C52 73 48 84 45 88 M54 67 L56 84',
      'M17 49 L30 54 M28 47 L38 56 M38 45 L44 57 M49 45 L50 58 M61 45 L57 56 M72 46 L63 55 M84 48 L73 54',
      'M31 24 C24 20 17 30 23 33 C29 37 36 28 31 24 Z M63 20 C55 17 50 25 56 29 C63 33 70 23 63 20 Z M77 33 C69 31 67 39 73 40 C81 42 84 35 77 33 Z',
      'M39 34 C33 32 30 39 36 41 C41 42 44 36 39 34 Z M44 14 L46 13',
      'M13 89 L17 76 L21 89 M76 91 L82 77 L83 91 M6 94 C28 90 74 94 94 91 M22 92 L25 85'
    ],
    fill: 'M8 48 C11 21 32 8 49 9 C72 8 90 25 94 48 C78 57 27 60 8 48 Z'
  },
  bone: {
    paths: [
      'M24 8 C13 2 3 14 10 24 C1 31 9 45 20 39 C28 35 31 39 40 49 L58 70 C62 76 58 79 61 87 C65 99 80 96 82 87 C92 91 100 78 92 71 C87 66 84 67 80 65 C69 59 53 39 42 25 C38 20 42 17 39 12 C36 6 29 4 24 8 Z',
      'M25 18 C22 22 18 24 15 26 M29 16 L32 13 M16 32 L21 29',
      'M32 32 C45 43 60 66 74 71 M37 29 C51 44 61 56 72 63',
      'M69 80 C74 77 80 79 82 83 M83 72 L88 76 M68 88 L72 90',
      'M15 65 L20 69 M10 72 L16 74 M69 20 L74 16 M75 27 L83 25',
      'M17 84 L19 89 L24 91 L19 93 L17 98 L15 93 L10 91 L15 89 Z'
    ],
    fill: 'M24 8 C13 2 3 14 10 24 C1 31 9 45 20 39 C28 35 31 39 40 49 L58 70 C62 76 58 79 61 87 C65 99 80 96 82 87 C92 91 100 78 92 71 C87 66 84 67 80 65 C69 59 53 39 42 25 C38 20 42 17 39 12 C36 6 29 4 24 8 Z'
  }
};

function design(type) { return DESIGNS[type] || DESIGNS.ember; }

/** Draw a centered engraved symbol. `size` is its full square extent. */
export function drawSymbol(ctx, type, x, y, size, color = PALETTE[type] || INK) {
  const d = design(type);
  ctx.save();
  ctx.translate(x - size / 2, y - size / 2);
  ctx.scale(size / 100, size / 100);
  ctx.lineWidth = 2.45;
  ctx.lineCap = 'round';
  ctx.lineJoin = 'round';
  if (d.fill) {
    ctx.fillStyle = color;
    ctx.globalAlpha *= .28;
    ctx.fill(new Path2D(d.fill));
    ctx.globalAlpha /= .28;
  }
  ctx.strokeStyle = color;
  for (const p of d.paths) ctx.stroke(new Path2D(p));
  ctx.restore();
}

/** Standalone SVG for recipe labels and other small UI illustrations. */
export function symbolSVG(type) {
  const d = design(type), color = PALETTE[type] || INK;
  return `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 100 100" fill="none" stroke="${color}" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">${d.fill ? `<path d="${d.fill}" fill="${color}" fill-opacity=".1" stroke="none"/>` : ''}${d.paths.map(p => `<path d="${p}"/>`).join('')}</svg>`;
}

function rng(seed) {
  return () => {
    seed = (Math.imul(seed, 1664525) + 1013904223) >>> 0;
    return seed / 4294967296;
  };
}

function rounded(ctx, x, y, w, h, r) {
  ctx.beginPath();
  ctx.moveTo(x + r, y);
  ctx.arcTo(x + w, y, x + w, y + h, r);
  ctx.arcTo(x + w, y + h, x, y + h, r);
  ctx.arcTo(x, y + h, x, y, r);
  ctx.arcTo(x, y, x + w, y, r);
  ctx.closePath();
}

function canvas() {
  const el = document.createElement('canvas');
  el.width = W;
  el.height = H;
  const ctx = el.getContext('2d');
  rounded(ctx, 1, 1, W - 2, H - 2, 15);
  ctx.clip();
  return [el, ctx];
}

function paper(ctx, dark = false) {
  const grad = ctx.createLinearGradient(0, 0, W, H);
  grad.addColorStop(0, dark ? '#273443' : '#eee1bd');
  grad.addColorStop(.4, dark ? '#1a2938' : '#f7ebce');
  grad.addColorStop(1, dark ? '#14212d' : '#d9c397');
  ctx.fillStyle = grad;
  ctx.fillRect(0, 0, W, H);
  const rand = rng(18372);
  for (let i = 0; i < 2300; i++) {
    const x = rand() * W, y = rand() * H;
    ctx.fillStyle = dark ? `rgba(218,173,104,${rand() * .09})` : `rgba(98,68,37,${rand() * .075})`;
    ctx.fillRect(x, y, rand() * 1.3 + .25, rand() * 1.5 + .3);
  }
  ctx.strokeStyle = dark ? '#bfa06d25' : '#78542b12';
  ctx.lineWidth = .5;
  for (let i = 0; i < 110; i++) {
    const x = rand() * W, y = rand() * H;
    ctx.beginPath(); ctx.moveTo(x, y);
    ctx.lineTo(x + rand() * 20 - 10, y + rand() * 6 - 3); ctx.stroke();
  }
  const edge = ctx.createRadialGradient(W / 2, H / 2, 80, W / 2, H / 2, 230);
  edge.addColorStop(0, '#0000'); edge.addColorStop(1, dark ? '#0006' : '#82582945');
  ctx.fillStyle = edge; ctx.fillRect(0, 0, W, H);
}

function star(ctx, x, y, radius, color) {
  ctx.save(); ctx.translate(x, y);
  ctx.strokeStyle = color; ctx.lineWidth = 1;
  ctx.beginPath();
  ctx.moveTo(0, -radius); ctx.lineTo(radius * .26, -radius * .26);
  ctx.lineTo(radius, 0); ctx.lineTo(radius * .26, radius * .26);
  ctx.lineTo(0, radius); ctx.lineTo(-radius * .26, radius * .26);
  ctx.lineTo(-radius, 0); ctx.lineTo(-radius * .26, -radius * .26); ctx.closePath();
  ctx.stroke(); ctx.restore();
}

function border(ctx, dark = false) {
  const color = dark ? '#c29b61' : '#9e784c';
  ctx.strokeStyle = color;
  ctx.lineWidth = 1.2;
  rounded(ctx, 11, 11, W - 22, H - 22, 9); ctx.stroke();
  ctx.lineWidth = .55;
  rounded(ctx, 16, 16, W - 32, H - 32, 6); ctx.stroke();
  ctx.strokeStyle = dark ? '#886d4855' : '#77583738';
  rounded(ctx, 4, 4, W - 8, H - 8, 12); ctx.stroke();
  for (const [x, y, a] of [[24, 24, 0], [W - 24, 24, Math.PI / 2], [W - 24, H - 24, Math.PI], [24, H - 24, -Math.PI / 2]]) {
    ctx.save(); ctx.translate(x, y); ctx.rotate(a);
    ctx.strokeStyle = color; ctx.lineWidth = .9;
    ctx.beginPath(); ctx.moveTo(0, 17); ctx.quadraticCurveTo(1, 1, 17, 0);
    ctx.moveTo(1, 10); ctx.quadraticCurveTo(10, 11, 10, 1);
    ctx.moveTo(0, 1); ctx.lineTo(5, 5); ctx.stroke(); ctx.restore();
  }
}

function sprig(ctx, x, y, flip = 1, color = '#9e784c') {
  ctx.save(); ctx.translate(x, y); ctx.scale(flip, 1);
  ctx.strokeStyle = color; ctx.lineWidth = .9;
  ctx.beginPath(); ctx.moveTo(0, 38); ctx.bezierCurveTo(-14, 6, 13, -15, 0, -42); ctx.stroke();
  for (let i = 0; i < 7; i++) {
    const yy = 29 - i * 10, s = i % 2 ? 1 : -1;
    ctx.beginPath(); ctx.moveTo(-1, yy);
    ctx.quadraticCurveTo(s * 17, yy - 1, s * 16, yy - 13);
    ctx.quadraticCurveTo(s * 5, yy - 15, -1, yy);
    ctx.stroke();
  }
  ctx.restore();
}

/** Cached 256×360 front texture. All fonts are local browser fonts. */
export function cardImage(type) {
  const key = DESIGNS[type] ? type : 'ember';
  if (fronts.has(key)) return fronts.get(key);
  const [el, ctx] = canvas();
  const color = PALETTE[key];
  paper(ctx); border(ctx);
  ctx.textAlign = 'center';
  ctx.fillStyle = color; ctx.font = 'bold 27px Georgia, serif';
  ctx.fillText(key.toUpperCase(), W / 2, 45, 207);
  ctx.strokeStyle = '#96724880'; ctx.lineWidth = .6;
  ctx.beginPath(); ctx.moveTo(55, 52); ctx.lineTo(201, 52); ctx.stroke();
  star(ctx, 128, 65, 5, '#987746');
  // Faint oval plate behind the main engraving.
  ctx.strokeStyle = '#95764b38'; ctx.lineWidth = .7;
  ctx.beginPath(); ctx.ellipse(128, 177, 87, 101, 0, 0, Math.PI * 2); ctx.stroke();
  ctx.setLineDash([1, 5]);
  ctx.beginPath(); ctx.ellipse(128, 177, 92, 107, 0, 0, Math.PI * 2); ctx.stroke();
  ctx.setLineDash([]);
  drawSymbol(ctx, key, 128, 174, 174, color);
  sprig(ctx, 40, 222, 1, '#9a805f65');
  sprig(ctx, 216, 222, -1, '#9a805f65');
  star(ctx, 128, 288, 6, '#987746');
  ctx.strokeStyle = '#96724880';
  ctx.beginPath(); ctx.moveTo(48, 302); ctx.lineTo(98, 302);
  ctx.moveTo(158, 302); ctx.lineTo(208, 302); ctx.stroke();
  ctx.fillStyle = color; rounded(ctx, 24, 305, 208, 32, 5); ctx.fill();
  ctx.fillStyle = '#fff0d0'; ctx.font = 'bold 22px Georgia, serif';
  ctx.fillText(key.toUpperCase(), 128, 329, 196);
  const url = el.toDataURL('image/png'); fronts.set(key, url); return url;
}

/** Every hidden card uses this one identical, symmetric printed back. */
export function backImage() {
  if (back) return back;
  const [el, ctx] = canvas();
  paper(ctx, true); border(ctx, true);
  const gold = '#c09a65';
  // Repeating diamond weave, kept subdued so the center seal reads at a distance.
  ctx.save();
  rounded(ctx, 22, 22, 212, 316, 4); ctx.clip();
  ctx.strokeStyle = '#ad8b5535'; ctx.lineWidth = .6;
  for (let d = -360; d < 560; d += 19) {
    ctx.beginPath(); ctx.moveTo(d, 0); ctx.lineTo(d + 360, 360); ctx.stroke();
    ctx.beginPath(); ctx.moveTo(d, 0); ctx.lineTo(d - 360, 360); ctx.stroke();
  }
  ctx.restore();
  for (const [x, y, flip] of [[42, 88, 1], [214, 88, -1], [42, 272, -1], [214, 272, 1]]) {
    ctx.save(); ctx.translate(x, y); if (y > 180) ctx.rotate(Math.PI);
    sprig(ctx, 0, 0, flip, '#c4a06a'); ctx.restore();
  }
  ctx.fillStyle = '#1b2a38'; ctx.strokeStyle = gold; ctx.lineWidth = 1.3;
  ctx.beginPath(); ctx.moveTo(128, 75); ctx.bezierCurveTo(210, 117, 218, 238, 128, 285);
  ctx.bezierCurveTo(38, 238, 46, 117, 128, 75); ctx.closePath(); ctx.fill(); ctx.stroke();
  ctx.lineWidth = .65;
  ctx.beginPath(); ctx.ellipse(128, 180, 62, 92, 0, 0, Math.PI * 2); ctx.stroke();
  drawSymbol(ctx, 'moon', 128, 170, 89, '#d2ad75');
  star(ctx, 128, 240, 11, '#d2ad75');
  star(ctx, 128, 104, 6, gold);
  star(ctx, 64, 180, 6, gold); star(ctx, 192, 180, 6, gold);
  for (const yy of [44, 316]) {
    ctx.strokeStyle = gold; ctx.lineWidth = .8;
    ctx.beginPath(); ctx.moveTo(73, yy); ctx.quadraticCurveTo(104, yy - 12, 128, yy);
    ctx.quadraticCurveTo(152, yy + 12, 183, yy); ctx.stroke();
    star(ctx, 128, yy, 5, gold);
  }
  back = el.toDataURL('image/png'); return back;
}
