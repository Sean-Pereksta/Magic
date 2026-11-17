import {terrainHeight} from './terrain.js';

export const TERRAIN_OFFSET = Object.freeze({ x: 19, y: 58 });
const artUrl = new URL('./assets/painted-terrain.webp', import.meta.url);
const structuresUrl = new URL('./assets/painted-structures.webp', import.meta.url);
let atlas, atlasLoad, structures, structureLoad;
const swatches = new Map();

// The artwork is one painted atlas. Each map selects a distinct set of three
// materials for its low ground, elevated deck, and exposed cliff faces.
const materials = {
 desktop:     { ground: 0, deck: 5, face: 8, light: '#f1dbaf', shade: '#785434' },
 breakroom:   { ground: 1, deck: 4, face: 0, light: '#dddfb6', shade: '#536b59' },
 canyon:      { ground: 2, deck: 0, face: 7, light: '#ffdb9e', shade: '#5c493c' },
 conveyor:    { ground: 3, deck: 2, face: 6, light: '#e7c68c', shade: '#50514a' },
 inkdelta:    { ground: 4, deck: 1, face: 6, light: '#d3e9cc', shade: '#365951' },
 corkcross:   { ground: 5, deck: 8, face: 3, light: '#f0d88c', shade: '#685135' },
 serverstack: { ground: 6, deck: 2, face: 3, light: '#c3ddc6', shade: '#3a5455' },
 rainyard:    { ground: 7, deck: 2, face: 6, light: '#d1def1', shade: '#3e4d69' },
 cartonworks: { ground: 8, deck: 5, face: 3, light: '#ffe4ac', shade: '#735134' }
};
const materialFor = map => materials[map?.id] || materials.desktop;

export function loadWorldArtwork() {
 if (!atlasLoad) {
  atlas = new Image();
  atlasLoad = new Promise((resolve, reject) => {
   atlas.onload = () => resolve();
   atlas.onerror = () => {
    atlasLoad = undefined;
    reject(new Error('The painted terrain textures could not be loaded.'));
   };
   atlas.src = artUrl.href;
  });
 }
 return atlasLoad;
}
export const worldArtworkReady = () => Boolean(atlas?.naturalWidth);

export function loadStructureArtwork() {
 if (!structureLoad) {
  structures = new Image();
  structureLoad = new Promise((resolve, reject) => {
   structures.onload = () => resolve();
   structures.onerror = () => {
    structureLoad = undefined;
    reject(new Error('The painted buildings could not be loaded.'));
   };
   structures.src = structuresUrl.href;
  });
 }
 return structureLoad;
}

const structureBounds = {
 desk: [26,3,412,407], printer: [488,23,355,365], cabinet: [948,22,281,382],
 cdreader: [27,453,367,331], mouse: [467,428,402,353], trash: [921,423,312,370],
 workshop: [12,816,410,394], laboratory: [440,801,428,408], cd: [905,841,368,300]
};

export function drawPaintedStructure(c, type, options) {
 const bounds = structureBounds[type];
 if (!bounds || !structures?.naturalWidth) return false;
 const [x,y,w,h] = bounds;
 const scale = (type === 'cabinet' ? 58 : 60) / Math.max(w,h);
 const width = w * scale, height = h * scale;
 const shift = options.battlefield ? -height*.38 : -height*.5;
 c.save();
 if (type === 'trash' && options.amount === 0) c.globalAlpha *= .48;
 if (type === 'cd' && options.battlefield) c.rotate(Math.sin(options.spin || 0) * .08);
 if (type === 'mouse' && options.attackAnim > 0) c.translate(0, -1.5);
 c.drawImage(structures,x,y,w,h,-width/2,shift,width,height);
 c.restore();
 return true;
}

function texture(c, index) {
 if (!atlas?.naturalWidth || typeof document === 'undefined') return null;
 if (!swatches.has(index)) {
  const tile = document.createElement('canvas');
  const width = Math.floor(atlas.naturalWidth / 3) - 4;
  const height = Math.floor(atlas.naturalHeight / 3) - 4;
  const sx = Math.round(atlas.naturalWidth / 3 * (index % 3)) + 2;
  const sy = Math.round(atlas.naturalHeight / 3 * Math.floor(index / 3)) + 2;
  tile.width = width * 2;
  tile.height = height * 2;
  const brush = tile.getContext('2d');
  // Mirror into a seamless repeat without modifying the painted brushwork.
  for (let row = 0; row < 2; row++) for (let col = 0; col < 2; col++) {
   brush.setTransform(col ? -1 : 1, 0, 0, row ? -1 : 1, col ? tile.width : 0, row ? tile.height : 0);
   brush.drawImage(atlas, sx, sy, width, height, 0, 0, width, height);
  }
  swatches.set(index, tile);
 }
 return c.createPattern(swatches.get(index), 'repeat');
}

function shape(c, points) {
 c.beginPath();
 points.forEach(([x,y], i) => i ? c.lineTo(x,y) : c.moveTo(x,y));
 c.closePath();
}

function wash(c, pattern, alpha, x, y, w, h) {
 if (!pattern) return;
 c.save();
 c.globalAlpha *= alpha;
 c.fillStyle = pattern;
 c.fillRect(x, y, w, h);
 c.restore();
}

export function paintLowGround(c, map, width, height) {
 const material = materialFor(map);
 c.save();
 c.fillStyle = map?.color || '#e9ddba';
 c.fillRect(0, 0, width, height);
 wash(c, texture(c, material.ground), .78, 0, 0, width, height);
 const light = c.createLinearGradient(0, 0, width, height);
 light.addColorStop(0, material.light + '47');
 light.addColorStop(.46, material.light + '13');
 light.addColorStop(1, material.shade + '66');
 c.fillStyle = light;
 c.fillRect(0, 0, width, height);
 c.restore();
}

export function projectGround(map, x, y) {
 const z = terrainHeight(map || {}, x, y);
 return [x - z * TERRAIN_OFFSET.x, y - z * TERRAIN_OFFSET.y];
}

export function pickGround(map, x, y) {
 const {x:dx,y:dy} = TERRAIN_OFFSET;
 const contains = (r, px, py) => px >= r.x && px <= r.x + r.w && py >= r.y && py <= r.y + r.h;
 for (const r of map?.ramps || []) {
  const [gx, gy] = rampGradient(r);
  const z = (gx * (x - r.x) + gy * (y - r.y) + Number(gx < 0 || gy < 0)) / (1 - gx * dx - gy * dy);
  const px = x + z * dx, py = y + z * dy;
  if (z >= 0 && z <= 1 && contains(r, px, py)) return {x:px,y:py};
 }
 const px = x + dx, py = y + dy;
 if ((map?.plateaus || []).some(p => contains(p, px, py)) && terrainHeight(map, px, py) === 1) return {x:px,y:py};
 return {x,y};
}

function rampGradient({side,w,h}) {
 return side === 'west' ? [1/w,0] : side === 'east' ? [-1/w,0] : side === 'north' ? [0,1/h] : [0,-1/h];
}

function rectangle(r) {
 return [[r.x,r.y],[r.x+r.w,r.y],[r.x+r.w,r.y+r.h],[r.x,r.y+r.h]];
}

function rampPlane(r) {
 const [gx, gy] = rampGradient(r);
 const z0 = Number(gx < 0 || gy < 0);
 const dx = TERRAIN_OFFSET.x, dy = TERRAIN_OFFSET.y;
 return {
  a:1 - dx * gx, b:-dy * gx, c:-dx * gy, d:1 - dy * gy,
  e:dx * (gx * r.x + gy * r.y - z0),
  f:dy * (gx * r.x + gy * r.y - z0)
 };
}

export function paintCliffWorld(c, map) {
 const material = materialFor(map), deck = texture(c, material.deck), face = texture(c, material.face);
 const { x:dx, y:dy } = TERRAIN_OFFSET;
 const plateaus = map?.plateaus || [];
 for (const p of plateaus) {
  const shadow = c.createLinearGradient(0, p.y+p.h-16, 0, p.y+p.h+38);
  shadow.addColorStop(0, '#111c1dc0');
  shadow.addColorStop(1, '#111c1d00');
  c.fillStyle = shadow;
  c.fillRect(p.x-10, p.y+p.h-16, p.w+37, 54);
 }
 for (const wall of map?.cliffs || []) {
  const { x, y, w, h, side } = wall;
  if (w <= 0 || h <= 0 || (side !== 'south' && side !== 'east')) continue;
  const a = side === 'south' ? [x,y+h/2] : [x+w/2,y];
  const b = side === 'south' ? [x+w,y+h/2] : [x+w/2,y+h];
  shape(c, [[a[0]-dx,a[1]-dy],[b[0]-dx,b[1]-dy],b,a]);
  c.save();
  c.clip();
  c.fillStyle = material.shade;
  c.fillRect(x-dx-2,y-dy-2,w+dx+4,h+dy+4);
  wash(c, face, side === 'south' ? .62 : .44, x-dx-2,y-dy-2,w+dx+4,h+dy+4);
  const dark = c.createLinearGradient(a[0]-dx,a[1]-dy,a[0],a[1]);
  dark.addColorStop(0, material.light + '48');
  dark.addColorStop(.22, material.shade + '21');
  dark.addColorStop(1, '#17222bc9');
  c.fillStyle = dark;
  c.fillRect(x-dx-2,y-dy-2,w+dx+4,h+dy+4);
  c.restore();
 }
 for (const p of plateaus) {
  const x = p.x-dx, y = p.y-dy;
  c.fillStyle = material.light;
  c.fillRect(x,y,p.w,p.h);
  wash(c, deck, .69, x,y,p.w,p.h);
  c.lineWidth = 3;
  c.strokeStyle = material.light;
  c.strokeRect(x+1.5,y+1.5,p.w-3,p.h-3);
  c.lineWidth = 1;
  c.strokeStyle = material.shade + 'aa';
  c.strokeRect(x+10,y+10,p.w-20,p.h-20);
  const tone = c.createLinearGradient(x,y,x+p.w,y+p.h);
  tone.addColorStop(0, '#fff3cc42');
  tone.addColorStop(1, material.shade + '4a');
  c.fillStyle = tone;
  c.fillRect(x,y,p.w,p.h);
 }
 for (const r of map?.ramps || []) {
  const {a,b,c:cross,d,e,f} = rampPlane(r);
  const {x,y,w,h} = r;
  const [gx,gy] = rampGradient(r);
  c.save();
  c.transform(a,b,cross,d,e,f);
  c.fillStyle = material.shade;
  c.fillRect(x,y,w,h);
  wash(c, deck, .58, x,y,w,h);
  const light = c.createLinearGradient(gx < 0 ? x+w : x, gy < 0 ? y+h : y, gx > 0 ? x+w : x, gy > 0 ? y+h : y);
  light.addColorStop(0, material.shade + '5a');
  light.addColorStop(1, material.light + '69');
  c.fillStyle = light;
  c.fillRect(x,y,w,h);
  c.lineWidth = 2;
  c.strokeStyle = material.light + 'd2';
  c.strokeRect(x+1,y+1,w-2,h-2);
  c.strokeStyle = material.shade + '6b';
  c.beginPath();
  if (gx) {
   for (let step = x+18; step < x+w-10; step += 19) { c.moveTo(step,y+4); c.lineTo(step,y+h-4); }
  } else {
   for (let step = y+18; step < y+h-10; step += 19) { c.moveTo(x+4,step); c.lineTo(x+w-4,step); }
  }
  c.stroke();
  c.restore();
 }
}

// Reproject only the raised surfaces of the small fog image. Matching the
// steep faces to the upper deck prevents a lower visible tile exposing it.
export function projectTerrainFog(c, source, map, width, height) {
 const w = source.width, h = source.height;
 const {x:dx,y:dy} = TERRAIN_OFFSET;
 c.setTransform(w/width,0,0,h/height,0,0);
 c.clearRect(0,0,width,height);
 c.drawImage(source,0,0,width,height);
 const renderPlane = (poly, matrix) => {
  c.save();
  shape(c,poly);
  c.clip();
  c.clearRect(0,0,width,height);
  c.transform(...matrix);
  c.drawImage(source,0,0,width,height);
  c.restore();
 };
 for (const p of map?.plateaus || []) {
  renderPlane(rectangle({x:p.x-dx,y:p.y-dy,w:p.w+dx,h:p.h+dy}), [1,0,0,1,-dx,-dy]);
 }
 for (const r of map?.ramps || []) {
  const {a,b,c:cross,d,e,f} = rampPlane(r);
  renderPlane(rectangle(r).map(([x,y]) => [a*x+cross*y+e,b*x+d*y+f]), [a,b,cross,d,e,f]);
 }
}
