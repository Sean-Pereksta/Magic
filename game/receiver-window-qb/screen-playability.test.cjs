const test = require('node:test');
const assert = require('node:assert/strict');
const Base = require('./variety.js');
const baseLane = Base.lane;
const V = require('./screen-playability.js');

function segmentDistance(point, a, b) {
  const vx = b.x - a.x;
  const vz = b.z - a.z;
  const den = vx * vx + vz * vz || 1;
  const t = Math.max(0, Math.min(1, ((point.x - a.x) * vx + (point.z - a.z) * vz) / den));
  return Math.hypot(a.x + vx * t - point.x, a.z + vz * t - point.z);
}

test('screen outlets create a real catch window and safe throwing lane', () => {
  const anchor = { x: 8, z: 2 };
  const qb = { x: 0, z: 14 };
  const defenders = [{ x: 8, z: 2 }, { x: 10, z: 1 }];
  const outlet = V.screenOutlet({ anchor, losZ: 0, qb, defenders, awareness: 90 });

  assert.ok(Math.hypot(outlet.x - anchor.x, outlet.z - anchor.z) >= 4,
    'the receiver should relocate far enough to detach from attached coverage');
  assert.ok(Math.min(...defenders.map(d => Math.hypot(outlet.x - d.x, outlet.z - d.z))) >= 4,
    'the chosen catch point should have usable space');
  assert.ok(Math.min(...defenders.map(d => segmentDistance(d, qb, outlet))) >= 1.5,
    'the pass path should avoid underneath defenders');
  assert.ok(outlet.z >= -1 && outlet.z <= 5 && Math.abs(outlet.x) <= 23.4,
    'screen outlets must remain behind the line and in bounds');
  assert.deepEqual(V.screenOutlet({ anchor, losZ: 0, qb, defenders, awareness: 90 }), outlet,
    'the same defensive picture should produce a stable outlet');
});

test('screen carriers briefly set up a block, then turn upfield at full pace', () => {
  const context = {
    x: 0,
    z: 0,
    blockers: [{ x: 0, z: -2, target: 'edge', engaged: false }],
    defenders: [{ id: 'edge', x: 1, z: -5 }],
    awareness: 90
  };

  const setup = V.screenRead({ ...context, elapsed: 0.2 });
  assert.equal(setup.phase, 'PRESS BLOCK');
  assert.equal(setup.pace, 0.86);

  const release = V.screenRead({ ...context, elapsed: 0.55 });
  assert.equal(release.phase, 'FOLLOW SEAL');
  assert.equal(release.pace, 1);
  assert.ok(release.lead.z < context.z, 'the block read should still send the carrier forward');
});

test('a free hitter at the catch point triggers an immediate hot cut', () => {
  const read = V.screenRead({
    x: 0,
    z: 0,
    elapsed: 0.1,
    blockers: [],
    defenders: [{ id: 'free', x: 0.5, z: -1.6 }],
    awareness: 70
  });

  assert.equal(read.phase, 'HOT CUT');
  assert.equal(read.pace, 1);
  assert.ok(read.lead.x < 0, 'the escape should break away from the defender leverage');
  assert.ok(read.lead.z < 0, 'the hot cut must still attack upfield');
});

test('engaged screen blockers create a protected lane instead of repelling the runner', () => {
  const situation = {
    x: 0,
    z: 0,
    bestZ: 0,
    defenders: [{ x: 0, z: -3 }, { x: 3, z: -4 }],
    style: 'YAC Specialist',
    markerZ: -30,
    screen: true,
    awareness: 85
  };
  const unblocked = V.lane({ ...situation, blockers: [] });
  const sealed = V.lane({ ...situation, blockers: [{ x: -1.5, z: -2.5, engaged: true }] });

  assert.ok(sealed.score > unblocked.score + 0.5, 'a useful seal should materially improve that lane');
  assert.ok(sealed.x < 0 && sealed.z < 0, 'the carrier should run off the blocker shoulder and upfield');
});

test('screen catches use a quicker but still visible turn-up animation', () => {
  const screen = V.catchAnimation({ kind: 'CATCH AND TURN', screen: true });
  const ordinary = V.catchAnimation({ kind: 'CATCH AND TURN' });

  assert.equal(screen.name, 'SCREEN TURN-UP');
  assert.ok(screen.duration >= 0.35 && screen.duration < ordinary.duration);
});

test('non-screen lane decisions remain delegated to the original runner logic', () => {
  const options = {
    x: 0,
    z: 0,
    bestZ: 0,
    defenders: [{ x: 1, z: -5 }],
    style: 'Route Technician',
    markerZ: -20,
    screen: false,
    blockers: []
  };
  const tuned = V.lane(options);
  const base = baseLane(options);
  assert.deepEqual(tuned, base);
});
