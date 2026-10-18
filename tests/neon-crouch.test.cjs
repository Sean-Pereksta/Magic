'use strict';
const test = require('node:test'), assert = require('node:assert/strict'), fs = require('node:fs'), path = require('node:path'), vm = require('node:vm');
const source = fs.readFileSync(path.resolve(__dirname, '../game/neon-arena/neon-arena.js'), 'utf8');
function extract(start, end) { return source.slice(source.indexOf(start), source.indexOf(end, source.indexOf(start))); }
function harness() {
  const sounds = [], a = { x: 0, y: 1.86, z: 0, vx: 0, vy: 0, vz: 0, yaw: 0, pitch: 0, onGround: true, r: .52, hp: 100, weapon: 'assault', ammo: 5, reload: 0, cool: 0, team: 1, entityId: 'p1' };
  const context = vm.createContext({ Math, Number, PLAYER_H: 1.86, HUMAN_VISUAL_HEIGHT: 2.39 / 1.86, HUMAN_VISUAL_WIDTH: 1, player: a, game: { started: true, p1Input: 'keyboard' }, fireHeld: false,
    controller: { settings: { lookSensitivity: 1 } }, clamp: (n, min, max) => Math.max(min, Math.min(n, max)), tacticalController: { hear: e => sounds.push(e) },
    neonModelRenderer: { get: () => ({ anchors: { head: [0, 2.15, .12], core: [0, 1.54, .025] } }) }, actorModelPath: () => 'cat', window: { NeonModels: { actorFit: () => ({ offset: [0, 0, 0], scale: [1, 1, 1] }) } },
    hasUpgrade: () => false, updateTacticalSlide: () => false, physics() {}, updateWeaponTimers() {}, updateMortarTarget() {}, pickupCheck() {}, updateBuildChargesFor() {}, humanBuildState: () => ({ held: false }),
    detonateRemoteGrenades: () => false, getUltimateReady: () => false, ultimateActive: () => false, msg() {}, WEAPONS: { assault: { kind: 'bullet', reload: 2, mag: 30, name: 'AR' } }
  });
  vm.runInContext(extract('function updateSlideView(', 'function pushOutXZ(') + extract('function updateHumanMovement(', 'function chargeHumanPlasma(') + extract('function humanTargetY(', 'function lineClear(') + extract('function actorStandingHeight(', 'function actorRampHeight(') + extract('function manualReload(', '\n'), context);
  return { a, sounds, context };
}

test('crouch lowers camera and the actual authored head/chest using the same eased pose', () => {
  const { a, context: c } = harness(); const standingHead = c.humanTargetY(a, 'head'), standingBody = c.humanTargetY(a, 'body');
  a.crouching = true; c.updateSlideView(a, .016); assert.ok(a.crouchPose > 0 && a.crouchPose < .3);
  assert.ok(c.actorEyePosition(a).y < a.y && c.actorEyePosition(a).y > a.y - .2);
  for (let i = 0; i < 90; i++) c.updateSlideView(a, 1 / 60);
  assert.ok(Math.abs(c.actorEyePosition(a).y - 1.21) < .001);
  assert.ok(Math.abs(c.humanTargetY(a, 'head') - standingHead * .73) < .001);
  assert.ok(Math.abs(c.humanTargetY(a, 'body') - standingBody * .73) < .001);
  assert.equal(a.y, 1.86, 'feet and physical elevation remain fixed'); assert.equal(c.actorStandingHeight(a), 2.39);
});

test('crouched head hitboxes cannot remain at standing height or extend above the rendered head', () => {
  const { a, context: c } = harness(), ray = y => c.rayEntity({ x: 0, y, z: -10 }, { x: 0, y: 0, z: 1 }, a, 20);
  assert.equal(ray(2.15).head, true);
  a.crouching = true; a.crouchPose = 1;
  assert.equal(ray(2.15), null); assert.equal(ray(1.57).head, true); assert.equal(ray(1.85), null);
  assert.ok(ray(.62), 'lower legs remain hittable'); assert.ok(ray(1.12), 'the crouched chest is physical');
});

test('crouch and slide do not multiply into an underground camera or tiny hitbox', () => {
  const { a, context: c } = harness(); a.crouchPose = 1; a.slidePose = 1;
  assert.equal(c.actorEyePosition(a).y, 1.86 - .78);
  assert.equal(c.humanTargetY(a, 'head'), 2.15 * .73);
  assert.equal(c.actorStandingHeight(a), 2.39);
});

test('crouching uses slower responsive strafe movement and ADS slows it further', () => {
  function speed(crouch, aim) { const { a, context: c } = harness(); a.crouching = crouch; a.aiming = aim; for (let i = 0; i < 90; i++) c.updateHumanMovement(a, 1 / 60, 1, 0); return Math.hypot(a.vx, a.vz); }
  const standing = speed(false, false), crouched = speed(true, false), aiming = speed(true, true);
  assert.ok(Math.abs(standing - 18.4) < .01); assert.ok(Math.abs(crouched / standing - .52) < .001); assert.ok(Math.abs(aiming / crouched - .8) < .001);
});

test('footstep, sprint and slide sounds are identified separately and quiet crouch has short range', () => {
  const { a, sounds, context: c } = harness(); a.vx = 18;
  c.updateSlideView(a, .4); assert.equal(sounds.at(-1).kind, 'sprint'); assert.equal(sounds.at(-1).range, 15);
  a.crouching = true; a.crouchPose = 1; a.vx = 8; c.updateSlideView(a, .8);
  assert.equal(sounds.at(-1).kind, 'footstep'); assert.equal(sounds.at(-1).range, 3.5);
  a.slideSoundPending = true; c.updateSlideView(a, .01); assert.equal(sounds.at(-1).kind, 'slide'); assert.equal(sounds.at(-1).range, 12);
});

test('a real reload emits one short-range sound and rejected reload input emits none', () => {
  const { a, sounds, context: c } = harness(); c.manualReload(a);
  assert.equal(a.reload, 2); assert.equal(sounds.length, 1); assert.equal(sounds[0].kind, 'reload'); assert.equal(sounds[0].range, 5);
  c.manualReload(a); assert.equal(sounds.length, 1); a.reload = 0; a.ammo = 30; c.manualReload(a); assert.equal(sounds.length, 1);
});
