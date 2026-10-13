const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const { test } = require('node:test');

const source = fs.readFileSync(path.join(__dirname, '../game/neon-arena/neon-arena.js'), 'utf8');
// Run the real production helpers without initializing DOM/WebGL or the animation loop.
function extract(name) {
  const start = source.indexOf(`function ${name}(`);
  assert.notEqual(start, -1, `production helper ${name} exists`);
  let depth = 0, quote = '', comment = '', escaped = false;
  for (let i = source.indexOf('){', start) + 1; i < source.length; i++) {
    const c = source[i], next = source[i + 1];
    if (comment) { if (comment === 'line' && c === '\n') comment = ''; else if (comment === 'block' && c === '*' && next === '/') { comment = ''; i++; } continue; }
    if (quote) { if (escaped) escaped = false; else if (c === '\\') escaped = true; else if (c === quote) quote = ''; continue; }
    if (c === '/' && next === '/') { comment = 'line'; i++; continue; }
    if (c === '/' && next === '*') { comment = 'block'; i++; continue; }
    if (c === '"' || c === "'" || c === '`') { quote = c; continue; }
    if (c === '{') depth++;
    if (c === '}' && --depth === 0) return source.slice(start, i + 1);
  }
  throw new Error(`Could not extract ${name}`);
}
const helpers = [
  'worldToGrid', 'gridToWorld', 'heightToBuildLayer', 'layerToHeight', 'makeBuildKey', 'buildKeysFor',
  'isGridOccupied', 'registerBuildPiece', 'unregisterBuildPiece', 'wallGridFromPoint', 'wallOrientation',
  'wallFromGrid', 'platformFromGrid', 'alignedPlatformCandidate', 'floorWallCandidate', 'stackedWallCandidate',
  'extendedWallCandidate', 'platformAdjacentToWall', 'attachedPlatformCandidate', 'fallbackBuildCandidate',
  'actorBuildGrid', 'clampTargetGridToActor', 'localBuildDistanceCells', 'withinLocalBuildLimit', 'yawWallOrientation', 'buildAimGrid', 'candidateAimPoint',
  'nextFreeWallLayer', 'topBuildLayerAtCell', 'localBuildCells', 'wallCandidateAtCell', 'explicitWallBuildCandidate', 'platformCandidateFromNearbyWall', 'explicitPlatformBuildCandidate', 'getBuildPreview',
  'rayScoreForCandidate', 'addCandidate', 'chooseBuildCandidate', 'buildIntentFor', 'assistedBuildCandidate',
  'buildTypeFor', 'setBuildOverrideFor', 'toggleBuildType', 'humanCharges', 'setHumanCharges',
  'setHumanBuildPreview', 'setHumanBuildFeedback', 'setHumanRecharge', 'getBuildPreviewFor', 'placeBuildFor',
  'pieceBottomY', 'pieceTopY', 'allActiveStructures', 'pieceIsGroundAnchor', 'xzOverlap', 'verticalOverlap',
  'structureContacts', 'connectedToGround', 'directSupportForCandidate', 'supportForCandidate', 'candidateHasSupport',
  'builtPieceStable', 'boxesOverlap', 'wallsMeetAtCorner', 'candidateOverlapsActor', 'candidateBlockedByStructure',
  'validateBuildPlacementRaw', 'validateBuildPlacement', 'isBuildPlacementValid', 'findNearestValidBuildCandidate',
  'rejectBuildPlacement', 'placeBuild', 'collapseStructure', 'collapseUnsupportedBuilds'
];
function arena() {
  const ctx = {
    gridSize: 4, WALL_H: 4, WALL_T: .55, PLATFORM_SIZE: 4, PLATFORM_H: .55, BUILD_LEVEL: 4,
    BUILD_CELL_LIMIT: 2, BUILD_RANGE: 26, BUILD_MAX_STACK: 8, BUILD_MAX: 10, MAX_ACTIVE_BUILDS: 320,
    PLAYER_H: 1.86, HEAD_CLEAR: .18, ARENA_SAFE: 64.3, BUILD_RECHARGE: .8,
    player: { x: 2, z: -3, y: 1.86, r: .5, human: true, yaw: 0, pitch: 0 }, player2: null,
    selectedBuildType: 'wall', buildCharges: 10, buildRecharge: 0, buildFeedback: 0, buildPreview: null,
    bots: [], walls: [], platforms: [], buildPieces: [], activeStructuresCache: [], particles: [],
    activeBuildCount: 0, buildOccupancy: new Map(), performance: { now: () => 1000 },
    clamp: (n, a, b) => Math.max(a, Math.min(b, n)), rand: (a, b) => (a + b) / 2,
    siegeBuildActive: () => false, markBlocksDirty() {}, burst() {}, msg() {}, shadeColor: c => c,
    dedicatedBuildRaycast: () => ctx.cast, lineClear: () => true, closestWallTopAimCandidate: () => null, closestWallEdgePlatformCandidate: () => null,
  };
  ctx.buildActor = ctx.player;
  let id = 0;
  ctx.makeStructurePiece = (type, o) => ({ ...o, type, id: ++id, active: true, destructible: true });
  ctx.blocks = () => {
    ctx.activeStructuresCache = [...ctx.walls, ...ctx.platforms].filter(p => p.active !== false);
    return ctx.activeStructuresCache.map(p => ({ ...p, kind: p.type, structure: p, y: p.type === 'platform' ? p.y + p.h / 2 : p.y }));
  };
  ctx.blocksInBounds = (x, z, w, d, pad = 0) => ctx.blocks().filter(b => Math.abs(x - b.x) <= (w + b.w) / 2 + pad && Math.abs(z - b.z) <= (d + b.d) / 2 + pad);
  ctx.structuresNearPiece = (p, pad = 1) => ctx.blocksInBounds(p.x, p.z, p.w, p.d, pad).map(b => b.structure);
  ctx.topAtPoint = (x, z) => ctx.blocks().filter(b => b.kind === 'platform' && Math.abs(x - b.x) <= b.w / 2 && Math.abs(z - b.z) <= b.d / 2).reduce((top, b) => Math.max(top, b.y + b.h / 2), -999);
  vm.createContext(ctx);
  vm.runInContext(helpers.map(extract).join('\n'), ctx);
  return ctx;
}
function castAt(ctx, piece, face, point, normal) {
  return { origin: { x: ctx.player.x, y: ctx.player.y, z: ctx.player.z }, dir: { x: 0, y: 0, z: 1 }, hit: { piece, face, ...point, normal, block: { ...piece, kind: piece.type, y: piece.type === 'platform' ? piece.y + piece.h / 2 : piece.y } } };
}
function addWall(ctx, gx = 0, gy = 0, gz = 0, extra = {}) {
  const c = ctx.wallFromGrid(gx, gy, gz, 'x', extra), piece = ctx.makeStructurePiece('wall', { ...c, builtBy: ctx.player, grid: { ...c } });
  ctx.walls.push(piece); ctx.buildPieces.push(piece); ctx.activeBuildCount++;
  ctx.registerBuildPiece(piece, ctx.buildKeysFor(c)); return piece;
}
function addFloor(ctx) { const p = ctx.makeStructurePiece('platform', { x: 0, z: 0, w: 120, d: 120, y: -.7, h: .7 }); ctx.platforms.push(p); return p; }

test('assisted intent favors ground walls, wall tops for stacking and elevated sides for platforms', () => {
  const a = arena(), floor = addFloor(a), wall = addWall(a);
  assert.equal(a.buildTypeFor(a.player), 'auto');
  assert.equal(a.buildIntentFor(castAt(a, floor, 'top', { x: 2, y: 0, z: 4 }, { x: 0, y: 1, z: 0 }), a.player), 'wall');
  const side = castAt(a, wall, 'side', { x: 2, y: 2, z: -.275 }, { x: 0, y: 0, z: -1 });
  assert.equal(a.buildIntentFor(side, a.player), 'platform');
  assert.equal(a.assistedBuildCandidate(side).type, 'platform');
  const top = castAt(a, wall, 'top', { x: 2, y: 4, z: 0 }, { x: 0, y: 1, z: 0 });
  const stack = a.assistedBuildCandidate(top);
  assert.equal(stack.type, 'wall'); assert.equal(a.pieceBottomY(stack), 4);
  assert.equal(a.validateBuildPlacement(stack).valid, true);
});

test('wall and platform overrides remain independent for both humans and preserve bot build choice', () => {
  const a = arena(); a.player2 = { ...a.player, x: 10, buildCharges: 10, human: true };
  assert.equal(a.setBuildOverrideFor(a.player, 'platform'), true);
  assert.equal(a.buildTypeFor(a.player), 'platform'); assert.equal(a.selectedBuildType, 'platform');
  assert.equal(a.buildTypeFor(a.player2), 'auto');
  a.setBuildOverrideFor(a.player2, 'wall'); assert.equal(a.buildTypeFor(a.player2), 'wall');
  a.toggleBuildType(a.player); assert.equal(a.buildTypeFor(a.player), 'auto');
  assert.equal(a.setBuildOverrideFor(a.player, 'invalid'), false);
  assert.equal(a.buildTypeFor({ buildType: 'platform' }), 'platform');
});

test('wall attachment and two bridging platforms have identical exact standing surfaces without cracks', () => {
  const a = arena(); addFloor(a); const wall = addWall(a);
  const p = a.platformAdjacentToWall(castAt(a, wall, 'side', { x: 2, y: 3, z: -.275 }, { x: 0, y: 0, z: -1 }).hit);
  assert.equal(a.pieceTopY(p), a.pieceTopY(wall));
  assert.equal(p.z + p.d / 2, wall.z);
  assert.equal(a.validateBuildPlacement(p).valid, true);
  assert.equal(a.placeBuild(p), true);
  const built = a.platforms.at(-1);
  assert.equal(a.pieceTopY(built), a.pieceTopY(p));
  const bridge = a.attachedPlatformCandidate(castAt(a, built, 'side', { x: built.x, y: 3.8, z: built.z - 2 }, { x: 0, y: 0, z: -1 }).hit);
  assert.equal(a.pieceTopY(bridge), a.pieceTopY(built));
  assert.equal(bridge.z + bridge.d / 2, built.z - built.d / 2);
  assert.equal(a.validateBuildPlacement(bridge).valid, true);
});

test('map-authored offsets and fractional elevations are retained by attachments', () => {
  const a = arena(); addFloor(a);
  const wall = a.makeStructurePiece('wall', { x: 2.3, z: .7, w: 5.51, d: .94, y: 1.6, h: 3.2 }); a.walls.push(wall);
  const p = a.platformAdjacentToWall(castAt(a, wall, 'side', { x: 2.1, y: 2, z: .23 }, { x: 0, y: 0, z: -1 }).hit);
  assert.ok(Math.abs(a.pieceTopY(p) - 3.2) < 1e-10);
  assert.ok(Math.abs(p.z + p.d / 2 - .7) < 1e-10);
  assert.equal(a.validateBuildPlacement(p).valid, true);
  assert.equal(a.placeBuild(p), true);
  const built = a.platforms.at(-1), bridge = a.attachedPlatformCandidate(castAt(a, built, 'side', { x: 2, y: 3, z: built.z - 2 }, { x: 0, y: 0, z: -1 }).hit);
  assert.ok(Math.abs(a.pieceTopY(bridge) - 3.2) < 1e-10);
  assert.ok(Math.abs(bridge.z + bridge.d / 2 - (built.z - built.d / 2)) < 1e-10);
});

test('actor capsules block construction, including another human, but touching an existing standing top is safe', () => {
  const a = arena(); addFloor(a);
  a.player.x = 2; a.player.z = 0;
  const wall = a.wallFromGrid(0, 0, 0, 'x');
  assert.equal(a.validateBuildPlacement(wall).reason, 'ACTOR IN THE WAY');
  a.player.z = -3; a.player2 = { x: 2, z: 0, y: 1.86, r: .5, human: true };
  assert.equal(a.validateBuildPlacement(wall).reason, 'ACTOR IN THE WAY');
  a.player2.dead = true;
  assert.equal(a.validateBuildPlacement(wall).valid, true);
  const p = a.platformFromGrid(0, 1, 0);
  assert.equal(a.candidateOverlapsActor(p, { x: 2, z: 2, y: 5.86, r: .5 }), false);
  assert.equal(a.candidateOverlapsActor(p, { x: 2, z: 2, y: 5, r: .5 }), true);
});

test('all authoritative placement limits remain enforced and geometry cannot be built through', () => {
  const a = arena(); addFloor(a); const valid = a.wallFromGrid(0, 0, 0, 'x');
  assert.equal(a.validateBuildPlacement(valid).valid, true);
  a.buildCharges = 0; assert.equal(a.validateBuildPlacement(valid).reason, 'NO CHARGES');
  a.siegeBuildActive = () => true; assert.equal(a.validateBuildPlacement(valid).valid, true);
  a.siegeBuildActive = () => false; a.buildCharges = 10;
  assert.equal(a.validateBuildPlacement(a.wallFromGrid(20, 0, 0, 'x')).reason, '2 SPACES MAX');
  assert.equal(a.validateBuildPlacement(a.wallFromGrid(0, 8, 0, 'x')).reason, 'MAX HEIGHT');
  assert.equal(a.validateBuildPlacement(a.platformFromGrid(0, 0, 0)).valid, false);
  assert.equal(a.validateBuildPlacement(a.platformFromGrid(0, 2, 0)).reason, 'NO SUPPORT');
  a.activeBuildCount = 320; assert.equal(a.validateBuildPlacement(valid).reason, 'STRUCTURE LIMIT'); a.activeBuildCount = 0;
  a.buildOccupancy.set(a.buildKeysFor(valid)[0], {}); assert.equal(a.validateBuildPlacement(valid).reason, 'OCCUPIED'); a.buildOccupancy.clear();
  a.walls.push(a.makeStructurePiece('wall', { x: 2, z: 0, y: 2, w: 3, d: 2, h: 4 }));
  assert.equal(a.validateBuildPlacement(valid).reason, 'BLOCKED');
  a.setBuildOverrideFor(a.player, 'platform'); assert.equal(a.validateBuildPlacement(valid).reason, 'WRONG BUILD MODE');
});

test('safe snap stays nearby and cannot snap into the actor', () => {
  const a = arena(); addFloor(a);
  a.player.x = 2; a.player.z = 0;
  const p = a.wallFromGrid(0, 0, 0, 'x'), snap = a.findNearestValidBuildCandidate(p);
  assert.ok(snap); assert.equal(a.validateBuildPlacement(snap).valid, true);
  assert.equal(a.candidateOverlapsActor(snap, a.player), false);
  assert.ok(Math.max(Math.abs(snap.gx - p.gx), Math.abs(snap.gz - p.gz)) <= 1);
});

test('destroying the anchor collapses dependent bridges and frees their occupancy and counts', () => {
  const a = arena(); addFloor(a); const wall = addWall(a);
  const first = a.platformAdjacentToWall(castAt(a, wall, 'side', { x: 2, y: 2, z: -.275 }, { x: 0, y: 0, z: -1 }).hit);
  assert.equal(a.placeBuild(first), true);
  const p = a.platforms.at(-1), next = a.attachedPlatformCandidate(castAt(a, p, 'side', { x: 2, y: 3.8, z: p.z - 2 }, { x: 0, y: 0, z: -1 }).hit);
  assert.equal(a.placeBuild(next), true);
  const bridge = a.platforms.at(-1); assert.equal(a.builtPieceStable(bridge), true);
  wall.active = false; a.unregisterBuildPiece(wall); a.activeBuildCount--; a.collapseUnsupportedBuilds();
  assert.equal(p.active, false); assert.equal(bridge.active, false);
  assert.equal(a.activeBuildCount, 0); assert.equal(a.buildOccupancy.size, 0);
});


test('the actual preview and placement pipeline accepts assisted types and exact explicit overrides', () => {
  const a = arena(); addFloor(a); const wall = addWall(a);
  a.cast = castAt(a, wall, 'side', { x: 2, y: 2.5, z: -.275 }, { x: 0, y: 0, z: -1 });
  const auto = a.getBuildPreviewFor(a.player);
  assert.equal(auto.type, 'platform'); assert.equal(auto.assisted, true); assert.equal(auto.valid, true);
  assert.equal(a.selectedBuildType, 'platform');
  a.setBuildOverrideFor(a.player, 'wall');
  const manual = a.getBuildPreviewFor(a.player);
  assert.equal(manual.type, 'wall'); assert.equal(manual.assisted, false); assert.equal(manual.valid, true);
  assert.equal(a.placeBuildFor(a.player, manual), true); assert.equal(a.buildCharges, 9);
  assert.equal(a.buildActor, a.player);
  a.setBuildOverrideFor(a.player, 'platform');
  assert.equal(a.getBuildPreviewFor(a.player).type, 'platform');
});

test('clean corner walls remain buildable while offset geometry and arena bounds are blocked', () => {
  const a = arena(); addFloor(a); addWall(a);
  const corner = a.wallFromGrid(0, 0, 0, 'z');
  assert.equal(a.validateBuildPlacement(corner).valid, true);
  a.player.x = 60; a.player.z = 2;
  assert.equal(a.validateBuildPlacement(a.wallFromGrid(16, 0, 0, 'z')).reason, 'BLOCKED');
  const far = a.wallFromGrid(15, 1, 0, 'z', { baseY: 120 });
  assert.equal(a.validateBuildPlacement(far).reason, 'TOO FAR');
});

test('a supported map platform at a fractional height produces a flush wall base', () => {
  const a = arena(); addFloor(a);
  const shelf = a.makeStructurePiece('platform', { x: 2, z: 2, y: 3.35, w: 8, d: 8, h: .85 }); a.platforms.push(shelf);
  const cast = castAt(a, shelf, 'top', { x: 2, y: 4.2, z: 2 }, { x: 0, y: 1, z: 0 });
  const wall = a.floorWallCandidate(cast.hit, cast.dir);
  assert.ok(Math.abs(a.pieceBottomY(wall) - 4.2) < 1e-10);
  assert.equal(a.validateBuildPlacement(wall).valid, true);
});


test('low elevated map steps can bridge at their actual height without permitting floor slabs', () => {
  const a = arena(); addFloor(a); a.player.z = -6;
  const step = a.makeStructurePiece('platform', { x: 2, z: 2, y: 1.15, h: .65, w: 4, d: 4 }); a.platforms.push(step);
  const p = a.attachedPlatformCandidate(castAt(a, step, 'side', { x: 2, y: 1.6, z: 0 }, { x: 0, y: 0, z: -1 }).hit);
  assert.ok(Math.abs(a.pieceTopY(p) - 1.8) < 1e-10); assert.equal(a.validateBuildPlacement(p).valid, true);
  const flat = a.alignedPlatformCandidate(2, -2, 0);
  assert.equal(a.validateBuildPlacement(flat).valid, false);
});
