import test from 'node:test';
import assert from 'node:assert/strict';
import {armyCost, createGame, FACTIONS, DRAFT_ROLES, RECRUIT_ROLES, draftCost} from './engine.mjs';
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
  assert.equal((html.match(/data-draft-action="buy"/g) || []).length, RECRUIT_ROLES.length);
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

test('army and deployment phases expose only the controls needed for that preparation step', () => {
  const draft = createDraft();
  const army = renderDraft(draft, {phase: 'army'});
  assert.equal((army.match(/data-draft-action="buy"/g) || []).length, RECRUIT_ROLES.length);
  assert.equal((army.match(/data-draft-action="remove"/g) || []).length, draft.army.length);
  assert.doesNotMatch(army, /data-draft-action="place"|data-draft-action="arrange"/);
  assert.match(army, /Scouts claim ground/);
  const deployment = renderDraft(draft, {phase: 'deploy'});
  assert.equal((deployment.match(/data-draft-action="place"/g) || []).length, draft.width * 2);
  assert.doesNotMatch(deployment, /data-draft-action="buy"|data-draft-action="remove"/);
  assert.match(deployment, /data-draft-action="arrange" data-side="player"/);
  assert.match(deployment, /Place Prism Matriarch/);
  assert.match(deployment, /Front line.*Back line/);
  assert.match(deployment, /data-phase="deploy"/);
  assert.match(renderDraft(draft, {phase: 'unknown'}), /data-phase="all"/);
});

test('default formations lead with mobile and sturdy troops while protecting commander and mystic', () => {
  for (const [width, height] of [[7, 5], [9, 7], [11, 9]]) {
    const draft = createDraft({width, height});
    for (const [index, role] of draft.army.entries()) {
      assert.equal(draft.positions[index].y, role === 'scout' || role === 'brute' ? height - 2 : height - 1);
    }
    assert.equal(draft.positions[0].x, Math.floor(width / 2));
    assert.ok(draft.positions[1].x < draft.positions[2].x, 'the two default scouts cover opposite flanks');
    assert.equal(draft.positions[3].x, Math.floor(width / 2), 'the default brute starts at the front center');
    validDeployment(draft);
  }
});

test('auto arrangement is deterministic, mirrors both players, and never changes purchases or selection', () => {
  for (const [width, height] of [[7, 5], [9, 7], [11, 9]]) for (const army of [
    ['leader', 'scout', 'scout', 'brute', 'mystic'],
    ['leader', 'mystic', 'mystic', 'mystic', 'mystic', 'mystic', 'mystic', 'mystic'],
    ['leader', 'scout', 'scout', 'scout', 'scout', 'scout', 'scout', 'scout']
  ]) {
    const player = createDraft({money: 40, width, height, army});
    const rival = createDraft({money: 40, width, height, side: 'ai', army});
    const purchases = [...player.army];
    applyDraftAction(player, {type: 'select', index: player.army.length - 1});
    applyDraftAction(player, {type: 'place', x: 0, y: height - 1});
    const selected = player.selectedIndex;
    const result = applyDraftAction(player, {type: 'arrange'});
    assert.equal(result.ok, true);
    assert.match(result.message, /Formation arranged/);
    assert.deepEqual(player.army, purchases);
    assert.equal(player.selectedIndex, selected);
    const once = structuredClone(player.positions);
    applyDraftAction(player, {type: 'arrange'});
    assert.deepEqual(player.positions, once);
    assert.deepEqual(rival.positions, player.positions.map(point => ({x: width - 1 - point.x, y: height - 1 - point.y})));
    validDeployment(player);
    validDeployment(rival);
  }
});

test('buying or resizing preserves manual placements and places new troops in their preferred line', () => {
  const draft = createDraft({money: 40, army: ['leader', 'scout']});
  applyDraftAction(draft, {type: 'select', index: 0});
  applyDraftAction(draft, {type: 'place', x: 0, y: 6});
  const commander = {...draft.positions[0]};
  applyDraftAction(draft, {type: 'buy', role: 'brute'});
  assert.deepEqual(draft.positions[0], commander);
  assert.equal(draft.positions.at(-1).y, 5);
  resizeDraft(draft, {faction: 'choir'});
  assert.deepEqual(draft.positions[0], commander);
});

test('deployment cells identify their creature index and selection feedback names the exact destination', () => {
  const draft = createDraft({side: 'ai'});
  applyDraftAction(draft, {type: 'select', index: 2});
  const html = renderDraft(draft, {phase: 'deploy'});
  const point = draft.positions[2];
  assert.match(html, new RegExp(`data-x="${point.x}" data-y="${point.y}" data-index="2"`));
  const result = applyDraftAction(draft, {type: 'place', x: 0, y: 1});
  assert.equal(result.ok, true);
  assert.match(result.message, /Skitterglow placed at A6/);
  assert.match(renderDraft(draft, {phase: 'deploy'}), /Tap a cell to move it/);
});

test('full battlefield integrations can retain placement controls without duplicating the small deployment grid', () => {
  const draft = createDraft();
  const html = renderDraft(draft, {phase: 'deploy', showGrid: false});
  assert.match(html, /data-draft-action="select"/);
  assert.match(html, /data-draft-action="arrange"/);
  assert.match(html, /Place [^<]+/);
  assert.doesNotMatch(html, /class="draft-deployment"/);
  assert.doesNotMatch(html, /data-draft-action="place"/);
  assert.doesNotMatch(html, /data-draft-action="buy"/);
  const purchases = [...draft.army];
  applyDraftAction(draft, {type: 'select', index: 1});
  assert.deepEqual(draft.army, purchases);
  assert.match(renderDraft(draft, {phase: 'army'}), /data-draft-action="buy"/);
});

test('each faction offers all six alternatives with readable skills, movement and prices before buying',()=>{
  assert.equal(DRAFT_ROLES.length,7);assert.equal(RECRUIT_ROLES.length,6);
  for(const faction of Object.keys(FACTIONS)){
    const draft=createDraft({faction,money:40,army:['leader']}),html=renderDraft(draft,{phase:'army'});
    for(const role of RECRUIT_ROLES){
      const def=FACTIONS[faction].units[role];
      assert.match(html,new RegExp(`data-role="${role}"`));
      assert.ok(html.includes(`Buy ${def.name} for ${draftCost(role)} coins`));
      assert.ok(html.includes(`${def.damage} damage`));
      for(const skill of def.abilities)assert.ok(html.includes(skill.description),`${def.name}: readable ${skill.name}`);
    }
    assert.equal((html.match(/<details class="draft-creature-notes">/g)||[]).length,6);
    assert.match(html,/<details class="draft-inspect">/);
    assert.ok(html.includes(FACTIONS[faction].formation.description));
  }
});

test('new guardians, rangers and shapers can be bought, positioned, refunded and started for either player',()=>{
  for(const faction of Object.keys(FACTIONS))for(const side of ['player','ai']){
    const draft=createDraft({faction,side,money:40,army:['leader']});
    for(const role of ['guardian','ranger','shaper'])assert.equal(applyDraftAction(draft,{type:'buy',role}).ok,true);
    assert.equal(armyCost(draft.army),20);validDeployment(draft);
    const front=side==='player'?5:1,back=side==='player'?6:0;
    assert.equal(draft.positions[1].y,front,'guardian protects the front');
    assert.equal(draft.positions[2].y,back,'ranger starts behind the front');
    assert.equal(draft.positions[3].y,back,'shaper starts protected');
    const options={playerFaction:faction,aiFaction:faction,opponentMode:'local',startingMoney:40};
    options[side==='player'?'playerArmy':'aiArmy']=draft.army;
    options[side==='player'?'playerDeployment':'aiDeployment']=draft.positions;
    const state=createGame(options);
    assert.deepEqual(state.units.filter(u=>u.side===side).map(u=>u.role),draft.army);
    assert.equal(applyDraftAction(draft,{type:'remove',index:2}).ok,true);
    assert.equal(armyCost(draft.army),16);validDeployment(draft);
  }
});

test('tactical troop sprites select their own atlas columns while original troop art remains unchanged',()=>{
  const draft=createDraft({money:40,army:['leader','guardian','ranger','shaper']});
  const html=renderDraft(draft,{phase:'deploy'});
  for(const position of [0,50,100])assert.ok(html.includes(`class="draft-sprite tactical-sprite" style="background-position:${position}% 0%"`));
  assert.ok(html.includes('class="draft-sprite" style="background-position:0% 0%"'));
  assert.equal((html.match(/data-draft-action="select"/g)||[]).length,4);
  assert.doesNotMatch(html,/data-draft-action="buy"/);
});
