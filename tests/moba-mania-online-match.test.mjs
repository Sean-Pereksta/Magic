import test from 'node:test';
import assert from 'node:assert/strict';
import { Game } from '../game/moba-mania/engine.mjs';
import { applyOnlineSnapshot, OnlineMatch } from '../game/moba-mania/online-match.mjs';

function makeMatch() {
  return new OnlineMatch({seed: 11, matchId: 'test-match', roster: [
    [{hero: 'arc', playerId: 'one'}, {hero: 'vela', playerId: 'two'}, {hero: 'angel'}],
    [{hero: 'bulwark', playerId: 'three'}, {hero: 'ferrite'}, {hero: 'druid'}],
  ]});
}

test('only the submitted player moves or casts, and unrefreshed input expires', () => {
  const match = makeMatch();
  const first = match.snapshot('one').units.find(unit => unit.isPlayer);
  const second = match.snapshot('two').units.find(unit => unit.isPlayer);
  const third = match.snapshot('three').units.find(unit => unit.isPlayer);
  assert.equal(match.command('absent', {type: 'move', x: 1, y: 0}), false);
  assert.equal(match.command('one', {type: 'move', x: NaN, y: 0}), false);
  assert.equal(match.command('one', {type: 'move', x: 1, y: 0, heroId: second.id}), true);
  match.advance(.2);
  assert.ok(match.players.get('one').x > first.x + 20);
  assert.equal(match.players.get('two').x, second.x);
  assert.equal(match.players.get('three').x, third.x);
  assert.equal(match.command('one', {type: 'cast', slot: 9, x: first.x + 100, y: first.y}), false);
  assert.equal(match.command('one', {type: 'cast', slot: 0, x: first.x + 100, y: first.y}), true);
  assert.ok(match.players.get('one').cooldowns[0] > 0);
  assert.equal(match.players.get('two').cooldowns[0], 0);
  for (let i = 0; i < 10; i++) match.advance(.1);
  const stopped = match.players.get('one').x;
  match.advance(.2);
  assert.equal(match.players.get('one').x, stopped);
});

test('a live join keeps the bot hero and progression in the same slot', () => {
  const match = makeMatch();
  const hero = match.game.units.find(unit => unit.kind === 'hero' && unit.team === 1 && unit.slot === 1);
  hero.gold = 444;
  assert.equal(match.takeover('four', 1, 0, 'Occupied'), false);
  assert.equal(match.takeover('four', 1, 1, 'Live player'), true);
  assert.equal(match.takeover('four', 1, 2, 'Duplicate'), false);
  assert.equal(match.snapshot('four').playerId, hero.id);
  assert.equal(match.players.get('four'), hero);
  assert.equal(hero.heroId, 'ferrite');
  assert.equal(hero.gold, 444);
  assert.equal(hero.displayName, 'Live player');
  assert.equal(match.release('four'), true);
  assert.equal(match.release('four'), false);
  assert.equal(hero.bot, true);
  assert.equal(hero.isPlayer, false);
  assert.equal(match.snapshot('four'), null);
  assert.equal(match.command('four', {type: 'recall'}), false);
  assert.equal(match.game.units.filter(unit => unit.kind === 'hero').length, 6);
});

test('private snapshots preserve each team fog, per-player orders, and public castle state', () => {
  const match = makeMatch();
  const rose = match.players.get('three');
  assert.equal(match.command('three', {type: 'ping', order: 'push', lane: 0}), true);
  assert.equal(match.command('one', {type: 'attackTarget', targetId: rose.id}), false);
  assert.equal(match.command('three', {type: 'attack'}), true);
  const azureView = match.snapshot('one');
  const roseView = match.snapshot('three');
  assert.equal(azureView.units.some(unit => unit.id === rose.id), false);
  assert.equal(azureView.units.filter(unit => unit.kind === 'core').length, 2);
  assert.equal(azureView.events.some(event => event.type === 'ping'), false);
  assert.equal(roseView.events.some(event => event.type === 'ping'), true);
  assert.deepEqual(roseView.attackOrder, {type: 'attack', targetId: null, explicit: false});
  assert.equal(azureView.attackOrder, null);
  assert.equal(match.game.teamPlans[1].lane, 0);

  const view = new Game();
  assert.equal(applyOnlineSnapshot(view, JSON.parse(JSON.stringify(roseView))), true);
  assert.equal(view.player.heroId, 'bulwark');
  assert.equal(view.player.team, 1);
  assert.equal(view.getAbility(view.player, 0).id, 'shield-bash');
  match.advance(.2);
  assert.equal(applyOnlineSnapshot(view, match.snapshot('three')), true);
  assert.equal(applyOnlineSnapshot(view, roseView), false);
  assert.ok(view.time > roseView.time);
});
