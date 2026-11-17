import { strict as assert } from 'node:assert';
import { test } from 'node:test';
import { AttackEffects } from './attack-effects.mjs';

const standard = { reduced: false, quality: 'high' };
const reduced = { reduced: true, quality: 'low' };

function harness(time = 100) {
  const calls = [];
  const ctx = new Proxy({}, {
    get(target, name) {
      if (name in target) return target[name];
      return (...args) => calls.push([name, ...args]);
    },
  });
  const board = {
    ctx, camera: { zoom: 1 }, tileX: 31, tileY: 15.5,
    width: 900, height: 720, state: { time, structures: [] }, received: time,
    project(x, y) { return [400 + (x - y) * this.tileX, 320 + (x + y) * this.tileY]; },
  };
  const drawSprite = (...args) => calls.push(['sprite', ...args]);
  const count = name => calls.filter(call => call[0] === name).length;
  return { board, calls, count, drawSprite };
}

const event = (type, kind, extra = {}) => ({
  id: 1, type, kind, time: 100, x: 1, y: 1, targetX: 4, targetY: 2, ...extra,
});

test('ongoing windups survive reconnect while expired, decorative, and stale events are skipped', () => {
  const fx = new AttackEffects();
  assert.equal(fx.add(event('windup', 'cat-charge', { until: 1200 }), 750), true);
  assert.equal(fx.add(event('impact', 'bullet', { reason: 'expired' }), 120), false);
  assert.equal(fx.add(event('attack', 'laser'), 750), false);
  assert.equal(fx.add(event('damage', 'bullet'), 120), false);
  const scene = harness(750);
  fx.drawGround(scene.board, 750, standard);
  assert.ok(scene.count('stroke') >= 2, 'the charging lane still warns after reconnect');
  scene.board.state.time = 1200;
  fx.drawGround(scene.board, 1200, standard);
  assert.equal(fx.events.length, 0);
});

test('major attacks paint warning and impact cues even with reduced motion enabled', () => {
  for (const kind of ['pounce', 'tail-sweep', 'cat-charge', 'powder-blast', 'mend']) {
    const fx = new AttackEffects();
    const scene = harness();
    fx.add(event('windup', kind, { until: 800, radius: 1.75 }), 100);
    fx.drawGround(scene.board, 100, reduced);
    assert.ok(scene.count('stroke') > 0, 'missing warning for ' + kind);
    scene.calls.length = 0;
    fx.add(event('attack', kind, { radius: 1.75 }), 100);
    fx.drawForeground(scene.board, 100, reduced, scene.drawSprite);
    assert.ok(scene.count('sprite') + scene.count('stroke') > 0, 'missing attack cue for ' + kind);
  }
});

test('interrupts clear only the affected actor warning immediately', () => {
  const fx = new AttackEffects();
  const pending = { until: 1000, radius: 2 };
  assert.ok(fx.add(event('windup', 'tail-sweep', { ...pending, entityId: 'cat' }), 100));
  assert.ok(fx.add(event('windup', 'powder-blast', { ...pending, entityId: 'sapper' }), 100));
  assert.ok(fx.add(event('interrupt', 'tail-sweep', { entityId: 'cat', time: 350 }), 350));
  assert.deepEqual(fx.events.filter(entry => entry.type === 'windup').map(entry => entry.entityId), ['sapper']);
  const scene = harness(350);
  fx.drawForeground(scene.board, 350, reduced, scene.drawSprite);
  assert.equal(scene.count('stroke'), 2);
});

test('an active major warning is drawn from current state after combat history rolls over', () => {
  const fx = new AttackEffects();
  const scene = harness(500);
  scene.board.state.enemies = [
    { id: 'cat', x: 1, y: 1, attackX: 4, attackY: 2, attackKind: 'cat-charge', attackRadius: 1, windupUntil: 800 },
  ];
  fx.drawGround(scene.board, 500, reduced);
  const fallbackStrokes = scene.count('stroke');
  assert.ok(fallbackStrokes > 0);
  scene.calls.length = 0;
  fx.add(event('windup', 'cat-charge', { until: 800, entityId: 'cat', radius: 1 }), 500);
  fx.drawGround(scene.board, 500, reduced);
  assert.equal(scene.count('stroke'), fallbackStrokes, 'the retained warning should paint only once');
});

test('ordinary strikes, chains, projectiles and active defense roles paint distinct cues', () => {
  const kinds = [
    ...['claw', 'rat-bite', 'flea-bite', 'talon', 'ram', 'king-bash', 'tesla', 'laser'].map(kind => ['attack', kind]),
    ...['bullet', 'rabbit', 'rocket', 'acorn', 'web'].map(kind => ['impact', kind]),
    ...['repair', 'shield', 'slime', 'drum', 'stink-cloud'].map(kind => ['activation', kind]),
    ['heal', 'mend'], ['charge-end', 'cat-charge'], ['shield-break', 'shield'], ['disrupt', 'stink-cloud'],
  ];
  for (const [type, kind] of kinds) {
    const fx = new AttackEffects();
    const scene = harness();
    assert.ok(fx.add(event(type, kind), 100));
    fx.drawForeground(scene.board, 100, standard, scene.drawSprite);
    assert.ok(scene.count('sprite') + scene.count('stroke') > 0, `missing ${type} cue for ${kind}`);
  }
});

test('launch animation uses the painted muzzle and optional trails respect reduced motion', () => {
  const fx = new AttackEffects();
  const scene = harness(280);
  scene.board.muzzlePoint = id => id === 'turret-a' ? [73, 110] : undefined;
  fx.add(event('shot', 'rocket', { shooterId: 'turret-a', time: 250 }), 280);
  fx.drawForeground(scene.board, 280, standard, scene.drawSprite);
  assert.deepEqual(scene.calls.find(call => call[0] === 'sprite').slice(1, 4), [4, 73, 110]);

  scene.calls.length = 0;
  const rocket = { vx: 6, vy: 1, kind: 'rocket', launchedAt: 100 };
  fx.drawProjectileTrail(scene.board, rocket, { x: 2, y: 2 }, standard);
  assert.equal(scene.count('stroke'), 1);
  assert.equal(scene.count('arc'), 4);
  scene.calls.length = 0;
  fx.drawProjectileTrail(scene.board, rocket, { x: 2, y: 2 }, reduced);
  assert.equal(scene.calls.length, 0);
});
