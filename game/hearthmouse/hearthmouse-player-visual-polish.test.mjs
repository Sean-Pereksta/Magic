import test from 'node:test';
import assert from 'node:assert/strict';
import { polishThirdPersonMouse } from './hearthmouse-player-visual-polish.mjs';

function node(name, scale = { x: 1, y: 1, z: 1 }) {
  return {
    name,
    scale: {
      ...scale,
      multiplyScalar(amount) {
        this.x *= amount;
        this.y *= amount;
        this.z *= amount;
      },
    },
    material: { opacity: 1, roughness: 0.45, metalness: 0.2 },
  };
}

function root(children) {
  return {
    name: 'playerMouseRoot',
    position: { y: 0.1 },
    scale: { x: 1, y: 1, z: 1 },
    userData: {},
    traverse(callback) {
      callback(this);
      for (const child of children) callback(child);
    },
  };
}

test('polishThirdPersonMouse reduces whisker scale and reshapes the body once', () => {
  const whisker = node('whisker-left');
  const body = node('body');
  const eye = node('eye-left');
  const playerRoot = root([whisker, body, eye]);
  const engine = { player: { rig: { root: playerRoot } } };

  assert.equal(polishThirdPersonMouse(engine), true);
  assert.equal(whisker.scale.x, 0.58);
  assert.equal(whisker.scale.y, 0.58);
  assert.equal(whisker.scale.z, 0.58);
  assert.equal(whisker.material.opacity, 0.72);
  assert.equal(body.scale.y, 0.88);
  assert.equal(body.scale.z, 1.08);
  assert.equal(eye.scale.x, 0.9);
  assert.equal(playerRoot.position.y, 0.08800000000000001);

  assert.equal(polishThirdPersonMouse(engine), false);
  assert.equal(whisker.scale.x, 0.58);
});

test('polishThirdPersonMouse returns false until a player rig exists', () => {
  assert.equal(polishThirdPersonMouse({}), false);
});
