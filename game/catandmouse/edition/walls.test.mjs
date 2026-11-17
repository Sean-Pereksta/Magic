import assert from 'node:assert/strict';
import { test } from 'node:test';

import { WALL_DIRECTIONS, isWall, wallConnections, wallKey } from './walls.mjs';

const [west, north, east, south] = WALL_DIRECTIONS;
const center = { type: 'wall', x: 8, y: 11, axis: 'y' };

function index(...structures) {
  return new Map(structures.map(structure => [wallKey(structure.x, structure.y), structure]));
}

function neighbor({ x, y }, type = 'wall') {
  return { x: center.x + x, y: center.y + y, type };
}

test('standalone walls use two opposite arms along the selected axis', () => {
  const walls = index(center);
  assert.deepEqual(wallConnections(center, walls), [north, south]);
  assert.deepEqual(wallConnections({ ...center, axis: 'x' }, walls), [west, east]);
  assert.deepEqual(wallConnections({ ...center, axis: undefined }, walls), [west, east]);
});

test('adjacent wall and barricade types form L, T and cross junctions', () => {
  assert.equal(isWall('wall'), true);
  assert.equal(isWall('barricade'), true);
  assert.equal(isWall('turret'), false);
  assert.deepEqual(wallConnections(center, index(center, neighbor(east, 'barricade'), neighbor(south))), [east, south]);
  assert.deepEqual(wallConnections(center, index(center, neighbor(north), neighbor(east), neighbor(south, 'barricade'))), [north, east, south]);
  assert.deepEqual(wallConnections(center, index(center, ...WALL_DIRECTIONS.map(direction => neighbor(direction)))), WALL_DIRECTIONS);
});

test('diagonals and other towers do not connect, and removing a neighbor restores the standalone axis', () => {
  const walls = index(center, neighbor(east), neighbor({ x: 1, y: 1 }), neighbor(north, 'turret'));
  assert.deepEqual(wallConnections(center, walls), [east]);
  walls.delete(wallKey(center.x + east.x, center.y + east.y));
  assert.deepEqual(wallConnections(center, walls), [north, south]);
});
