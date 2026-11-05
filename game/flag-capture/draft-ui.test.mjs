import test from 'node:test';
import assert from 'node:assert/strict';
import {armyCost, createGame} from './engine.mjs';
import {createDraft, resizeDraft, renderDraft, applyDraftAction} from './draft-ui.mjs';

function validDeployment(draft) {
  assert.equal(draft.positions.length, draft.army.length);
  assert.equal(new Set(draft.positions.map(point => `${point.x},${point.y}`)).size, draft.army.length);
  const rows = draft.side === 'ai' ? [0, 1] : [draft.height - 2, draft.height - 1];
  for (const point of draft.positions) {
    assert.ok(Number.isInteger(point.x) && point.x >= 0 && point.x < draft.width);
    assert.ok(rows.includes(point.y));
  }
}

test('default and low-budget warbands are affordable, have one commander, and fit both deployment zones', () => {
  for (const money of [12, 24, 40]) for (const side of ['player', 'ai']) for (const [width, height] of [[7, 5], [9, 7], [11, 9]]) {
    const draft = createDraft({money, side, width, height});
    assert.equal(draft.army.filter(role => role === 'leader').length, 1);
    assert.ok(draft.army.length >= 1 && draft.army.length <= 8);
    assert.ok(armyCost(draft.army) <= money);
    validDeployment(draft);
  }
  assert.deepEqual(createDraft().army, ['leader', 'scout', 'scout', 'brute', 'mystic']);
});

test('purchases spend available money; removal refunds it; commander and overspending are rejected', () => {
  const draft = createDraft();
  assert.equal(armyCost(draft.army), 21);
  assert.equal(applyDraftAction(draft, {type: 'buy', role: 'scout'}).ok, true);
  assert.equal(armyCost(draft.army), 24);
  assert.equal(applyDraftAction(draft, {type: 'buy', role: 'mystic'}).ok, false);
  assert.equal(applyDraftAction(draft, {type: 'remove', index: '5'}).ok, true);
  assert.equal(armyCost(draft.army), 21);
  assert.equal(applyDraftAction(draft, {type: 'remove', index: 0}).ok, false);
  assert.equal(applyDraftAction(draft, {type: 'buy', role: 'leader'}).ok, false);
  assert.equal(applyDraftAction(draft, {type: 'buy', role: 'minion'}).ok, false);
  validDeployment(draft);
});

test('eight-creature cap applies even with spare money and removals preserve a valid selection', () => {
  const draft = createDraft({money: 40, army: ['leader']});
  for (let index = 0; index < 7; index++) assert.equal(applyDraftAction(draft, {type: 'buy', role: 'scout'}).ok, true);
  assert.equal(draft.army.length, 8);
  assert.ok(armyCost(draft.army) < draft.money);
  assert.equal(applyDraftAction(draft, {type: 'buy', role: 'scout'}).ok, false);
  assert.equal(applyDraftAction(draft, {type: 'remove', index: 7}).ok, true);
  assert.ok(draft.selectedIndex >= 0 && draft.selectedIndex < draft.army.length);
  validDeployment(draft);
});

test('select and place swap occupied cells and reject cells outside the deployment zone without mutation', () => {
  const draft = createDraft();
  const original = structuredClone(draft.positions);
  assert.equal(applyDraftAction(draft, {type: 'select', index: '1'}).ok, true);
  assert.equal(applyDraftAction(draft, {type: 'place', x: original[0].x, y: original[0].y}).ok, true);
  assert.deepEqual(draft.positions[1], original[0]);
  assert.deepEqual(draft.positions[0], original[1]);
  const before = JSON.stringify(draft);
  for (const action of [{type: 'place', x: 0, y: 0}, {type: 'place', x: 99, y: 6}, {type: 'place', x: 1.2, y: 6}, {type: 'select', index: 99}]) assert.equal(applyDraftAction(draft, action).ok, false);
  assert.equal(JSON.stringify(draft), before);
  validDeployment(draft);
});

test('resizing and faction changes retain valid positions, repair invalid ones, and respect reduced money', () => {
  const draft = createDraft({side: 'ai', money: 40, army: ['leader', 'brute', 'brute', 'brute', 'brute', 'brute']});
  const initial = structuredClone(draft.positions);
  resizeDraft(draft, {faction: 'choir', width: 11, height: 9});
  assert.equal(draft.faction, 'choir');
  assert.deepEqual(draft.positions, initial);
  assert.match(renderDraft(draft), /Halo Architect/);
  resizeDraft(draft, {width: 7, height: 5, money: 12});
  assert.ok(armyCost(draft.army) <= 12);
  assert.equal(draft.army.filter(role => role === 'leader').length, 1);
  validDeployment(draft);
  const bottom = createDraft({height: 9});
  resizeDraft(bottom, {height: 5});
  validDeployment(bottom);
});

test('invalid optional input cannot add unknown roles, duplicate commanders, or unsafe faction markup', () => {
  const army = ['leader', 'leader', 'minion', 'scout'];
  const draft = createDraft({faction: '"><script>', side: 'unknown', width: 400, height: 1, money: 999, army});
  assert.deepEqual(draft.army, ['leader', 'scout']);
  assert.deepEqual(army, ['leader', 'leader', 'minion', 'scout']);
  assert.equal(draft.faction, 'bloom');
  assert.equal(draft.side, 'player');
  assert.equal(draft.money, 40);
  assert.equal(draft.width, 9);
  assert.doesNotMatch(renderDraft(draft), /<script>/);
  validDeployment(draft);
});

test('rendered UI exposes purchases, individual selections, refunds, and two full rows of accessible cells', () => {
  const draft = createDraft({faction: 'chaos', side: 'ai', width: 11, height: 9});
  const html = renderDraft(draft);
  assert.match(html, /Player 2 warband/);
  assert.match(html, /aria-live="polite"/);
  assert.match(html, /aria-pressed="true"/);
  assert.equal((html.match(/data-draft-action="buy"/g) || []).length, 3);
  assert.equal((html.match(/data-draft-action="select"/g) || []).length, draft.army.length);
  assert.equal((html.match(/data-draft-action="place"/g) || []).length, 22);
  assert.match(html, /data-role="scout"/);
  assert.match(html, /Commander is required" disabled/);
  assert.match(html, /data-side="ai" data-x="10" data-y="1"/);
  assert.match(html, /Mawring Maestro/);
  assert.match(html, /--draft-columns:11/);
});

test('purchased and placed warbands can start a legal engine battle unchanged', () => {
  const player = createDraft({faction: 'bloom', width: 7, height: 5, money: 24});
  const rival = createDraft({faction: 'choir', side: 'ai', width: 7, height: 5, money: 24});
  applyDraftAction(player, {type: 'buy', role: 'scout'});
  applyDraftAction(player, {type: 'select', index: 0});
  applyDraftAction(player, {type: 'place', x: 0, y: 3});
  const state = createGame({playerFaction: player.faction, aiFaction: rival.faction, width: 7, height: 5, startingMoney: 24, playerArmy: player.army, aiArmy: rival.army, playerDeployment: player.positions, aiDeployment: rival.positions});
  assert.deepEqual(state.units.filter(unit => unit.side === 'player').map(unit => unit.role), player.army);
  assert.deepEqual(state.units.filter(unit => unit.side === 'player').map(unit => ({x: unit.x, y: unit.y})), player.positions);
  assert.deepEqual(state.units.filter(unit => unit.side === 'ai').map(unit => ({x: unit.x, y: unit.y})), rival.positions);
});
