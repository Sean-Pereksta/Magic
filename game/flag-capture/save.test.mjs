import test from 'node:test';
import assert from 'node:assert/strict';
import { createGame, MODES, DIFFICULTIES, DEFAULT_ARMY, advance, legalActions, applyAction, unitDef } from './engine.mjs';
import { CAMPAIGN, createCampaignBattle } from './campaign.mjs';
import { parseBattleSave } from './save.mjs';

const saveFor = options => ({ version: 2, game: createGame(options) });
function rejects(change, label) {
  const save = saveFor();
  change(save.game, save);
  assert.equal(parseBattleSave(save), null, label);
}

test('all supported board dimensions and battle modes round-trip as objects and JSON', () => {
  for (const width of [7, 9, 11]) for (const height of [5, 7, 9]) for (const mode of Object.keys(MODES)) {
    const save = saveFor({ width, height, mode, terrain: 'wild', duration: 240 });
    assert.deepEqual(parseBattleSave(save), save, `${width}×${height} ${mode}`);
    assert.deepEqual(parseBattleSave(JSON.stringify(save)), save);
  }
});

test('all five AI difficulty levels survive save and resume', () => {
  assert.deepEqual(Object.keys(DIFFICULTIES), ['story', 'easy', 'normal', 'hard', 'expert']);
  for (const difficulty of Object.keys(DIFFICULTIES)) {
    const save = saveFor({ difficulty });
    assert.equal(save.game.difficulty, difficulty);
    for (let index = 0; index < 30; index++) advance(save.game, .1);
    assert.deepEqual(parseBattleSave(save), save, `${difficulty} object save`);
    assert.deepEqual(parseBattleSave(JSON.stringify(save)), save, `${difficulty} JSON save`);
  }
});

test('all campaign battles and valid campaign IDs resume', () => {
  for (const mission of CAMPAIGN) {
    const save = saveFor(createCampaignBattle(mission.id));
    save.game.campaignId = mission.id;
    assert.equal(save.game.opponentMode, 'ai');
    assert.deepEqual(parseBattleSave(save), save, mission.id);
  }
});

test('local two-player battles resume after legal actions from both armies', () => {
  const save = saveFor({ opponentMode: 'local', playerFaction: 'bloom', aiFaction: 'choir' });
  assert.equal(save.game.opponentMode, 'local');
  for (let index = 0; index < 21; index++) advance(save.game, .1);
  for (const side of ['player', 'ai']) {
    const unit = save.game.units.find(troop => troop.side === side && legalActions(save.game, troop.id).some(action => action.type === 'move'));
    assert.ok(unit, `${side} has a ready local troop`);
    const before = { x: unit.x, y: unit.y };
    const action = legalActions(save.game, unit.id).find(candidate => candidate.type === 'move');
    assert.equal(applyAction(save.game, action).ok, true, `${side} accepts a local command`);
    assert.notDeepEqual({ x: unit.x, y: unit.y }, before);
    assert.deepEqual(parseBattleSave(JSON.stringify(save)), save, `${side} action survives save and resume`);
  }
  const parsed = parseBattleSave(save);
  assert.equal(parsed.game.opponentMode, 'local');
  assert.notEqual(parsed.game.units, save.game.units);
});

test('resumed battle is a fresh deep copy and legacy defaults never mutate input', () => {
  const save = saveFor();
  delete save.game.mode;
  delete save.game.terrain;
  delete save.game.opponentMode;
  for (const flag of save.game.flags) delete flag.contested;
  const snapshot = structuredClone(save);
  const parsed = parseBattleSave(save);
  assert.equal(parsed.game.mode, 'beacons');
  assert.equal(parsed.game.terrain, 'balanced');
  assert.equal(parsed.game.opponentMode, 'ai');
  assert.deepEqual(save, snapshot);
  parsed.game.units[0].hp = 0;
  parsed.game.tiles[0].terrain = 'grove';
  parsed.game.factions.player = 'choir';
  parsed.game.log.push('Only the copy changes.');
  assert.deepEqual(save, snapshot);
});

test('independent ability cooldowns round-trip while legacy secondary abilities start ready', () => {
  const save = saveFor();
  for (const unit of save.game.units) {
    unit.abilityReadyAt = 17.25;
    unit.abilityReadyAts = unitDef(unit).abilities.map((_, index) => index === 0 ? 17.25 : index === 1 ? 4.5 : 29.75);
  }
  assert.deepEqual(parseBattleSave(JSON.stringify(save)), save);
  const legacy = structuredClone(save);
  for (const unit of legacy.game.units) delete unit.abilityReadyAts;
  const parsed = parseBattleSave(legacy);
  for (const unit of parsed.game.units) assert.deepEqual(unit.abilityReadyAts, unitDef(unit).abilities.map((_, index) => index === 0 ? 17.25 : 0));
  assert.ok(legacy.game.units.every(unit => unit.abilityReadyAts === undefined), 'legacy migration does not mutate input');
});

test('legacy two-slot cooldown arrays migrate new summon abilities without losing either deadline', () => {
  const save = saveFor();
  const caster = save.game.units.find(unit => unitDef(unit).abilities.length === 3);
  assert.ok(caster);
  caster.abilityReadyAt = 14;
  caster.abilityReadyAts = [14, 8];
  const parsed = parseBattleSave(save);
  assert.deepEqual(parsed.game.units.find(unit => unit.id === caster.id).abilityReadyAts, [14, 8, 0]);
  assert.deepEqual(caster.abilityReadyAts, [14, 8]);
});

test('legacy battles recover default draft metadata without mutating their source', () => {
  const save = saveFor();
  delete save.game.startingMoney;
  delete save.game.armies;
  delete save.game.deployments;
  const snapshot = structuredClone(save);
  const parsed = parseBattleSave(save);
  assert.equal(parsed.game.startingMoney, 24);
  assert.deepEqual(parsed.game.armies, {player:DEFAULT_ARMY,ai:DEFAULT_ARMY});
  assert.deepEqual(save, snapshot);
});

test('variable budgeted armies and explicit home deployments round-trip', () => {
  for (const mode of Object.keys(MODES)) {
    const save = saveFor({
      mode, startingMoney:12, playerArmy:['leader','scout'], aiArmy:['leader'],
      playerDeployment:[{x:1,y:5},{x:7,y:6}], aiDeployment:[{x:3,y:1}]
    });
    assert.deepEqual(save.game.armies.player, ['leader','scout']);
    assert.equal(save.game.startingMoney, 12);
    assert.deepEqual(parseBattleSave(JSON.stringify(save)), save);
  }
  const maximum = saveFor({startingMoney:40,playerArmy:['leader','scout','scout','scout','scout','brute','brute','mystic']});
  assert.deepEqual(parseBattleSave(maximum), maximum);
  const unspent = saveFor({mode:'reinforcements',startingMoney:40,playerArmy:['leader'],aiArmy:['leader']});
  assert.equal(unspent.game.supplies.player, 34);
  assert.deepEqual(parseBattleSave(unspent), unspent, 'unspent draft money above the original20-coin cap remains resumable');
});

test('living and fallen minions with no special abilities survive save and resume', () => {
  for (const [faction, role, kind] of [['bloom','leader','broodlings'],['chaos','mystic','pocketmaws'],['choir','mystic','bellmotes']]) {
    const save = saveFor({playerFaction:faction});
    const caster = save.game.units.find(unit => unit.side === 'player' && unit.role === role);
    const skillIndex = unitDef(caster).abilities.findIndex(skill => skill.kind === kind);
    const action = legalActions(save.game, caster.id).find(candidate => candidate.type === 'ability' && candidate.skillIndex === skillIndex);
    assert.ok(action, kind);
    assert.equal(applyAction(save.game, action).ok, true);
    const minion = save.game.units.find(unit => unit.side === 'player' && unit.role === 'minion');
    assert.ok(minion, `${kind} created a minion`);
    assert.deepEqual(minion.abilityReadyAts, []);
    assert.equal(minion.abilityReadyAt, 0);
    assert.deepEqual(parseBattleSave(JSON.stringify(save)), save);
    const landing = save.game.tiles[minion.y * save.game.width + minion.x];
    const originalTerrain = landing.terrain;
    landing.terrain = 'water';
    assert.equal(Boolean(parseBattleSave(save)), unitDef(minion).movement === 'Flight', `${kind} minion water landing follows its movement definition`);
    landing.terrain = originalTerrain;
    minion.hp = 0;
    assert.deepEqual(parseBattleSave(save), save, `${kind} fallen minion remains valid`);
    minion.abilityReadyAt = 1;
    assert.equal(parseBattleSave(save), null, 'ability-free minions retain a zero primary alias');
  }
});

test('ordinary combat, deaths and occupied former death squares remain resumable', () => {
  const save = saveFor({ duration: 300 });
  for (let index = 0; index < 300; index++) advance(save.game, .1);
  assert.ok(parseBattleSave(save));
  const fallen = save.game.units[0];
  fallen.hp = 0;
  const living = save.game.units.find(unit => unit.hp > 0 && unit.id !== fallen.id);
  fallen.x = living.x;
  fallen.y = living.y;
  assert.ok(parseBattleSave(save), 'dead positions do not occupy tiles');
});

test('Choir flight can land on water and resume; ground troops and rock landings are rejected', () => {
  for (const role of ['leader', 'scout', 'mystic']) {
    const save = saveFor({ playerFaction: 'choir' });
    const unit = save.game.units.find(troop => troop.side === 'player' && troop.role === role);
    const destination = { x: unit.x, y: unit.y - 1 };
    save.game.tiles[destination.y * save.game.width + destination.x].terrain = 'water';
    const action = legalActions(save.game, unit.id).find(candidate => candidate.type === 'move' && candidate.x === destination.x && candidate.y === destination.y);
    assert.ok(action, `${role} can fly onto water`);
    assert.equal(applyAction(save.game, action).ok, true);
    assert.deepEqual(parseBattleSave(JSON.stringify(save)), save, `${role} water landing survives a save round-trip`);
    save.game.tiles[unit.y * save.game.width + unit.x].terrain = 'rock';
    assert.equal(parseBattleSave(save), null, `${role} cannot resume on a rock`);
  }
  for (const [playerFaction, role] of [['bloom', 'scout'], ['chaos', 'scout'], ['choir', 'brute']]) {
    const save = saveFor({ playerFaction });
    const unit = save.game.units.find(troop => troop.side === 'player' && troop.role === role);
    save.game.tiles[unit.y * save.game.width + unit.x].terrain = 'water';
    assert.equal(parseBattleSave(save), null, `${playerFaction} ${role} cannot occupy water`);
  }
});

test('malformed JSON and wrapper types fail closed', () => {
  for (const value of [null, undefined, false, 1, [], '', '{bad json', '{}', '{"version":1}', {version:2}, {version:2, game:[]}]) {
    assert.equal(parseBattleSave(value), null);
  }
  const circular = {}; circular.self = circular;
  assert.equal(parseBattleSave(circular), null);
  assert.equal(parseBattleSave({version:2, game:() => null}), null);
});

test('tile and flag arrays must be complete, unique, in bounds, and safe to render', () => {
  const changes = [
    game => { game.tiles = {length:63}; },
    game => { game.flags = {length:3}; },
    game => { game.units = {length:10}; },
    game => { game.tiles.pop(); },
    game => { game.tiles[0].x = 1; },
    game => { [game.tiles[0], game.tiles[1]] = [game.tiles[1], game.tiles[0]]; },
    game => { game.tiles[0].terrain = 'lava'; },
    game => { game.flags[0].x = game.width; },
    game => { game.flags[0].owner = 'enemy'; },
    game => { game.flags[0].contested = 'yes'; },
    game => { game.flags[0].name = '<img src=x>'; },
    game => { game.flags[0].x = game.flags[1].x; game.flags[0].y = game.flags[1].y; },
    game => { const flag=game.flags[0]; game.tiles[flag.y*game.width+flag.x].terrain = 'rock'; }
  ];
  changes.forEach((change, index) => rejects(change, `malformed map ${index}`));
});

test('troop IDs, positions, immutable definitions and combat status are validated', () => {
  const changes = [
    game => { game.units[0].id = game.units[1].id; },
    game => { game.units[0].id = 'u1" onclick="bad()'; },
    game => { game.units[0].id = 'u999'; },
    game => { game.units[0].x = game.units[1].x; game.units[0].y = game.units[1].y; },
    game => { game.units[0].x = -1; },
    game => { game.units[0].y = game.height; },
    game => { game.units[0].x = .5; },
    game => { game.units[0].side = 'enemy'; },
    game => { game.units[0].faction = 'choir'; },
    game => { game.units[0].role = 'toString'; },
    game => { game.units[0].hp = -1; },
    game => { game.units[0].hp = game.units[0].maxHp + 1; },
    game => { game.units[0].maxHp++; },
    game => { game.units[0].readyAt = Infinity; },
    game => { game.units[0].abilityReadyAt = NaN; },
    game => { game.units[0].abilityReadyAts = {}; },
    game => { game.units[0].abilityReadyAts = []; },
    game => { game.units[0].abilityReadyAts = [game.units[0].abilityReadyAt]; },
    game => { game.units[0].abilityReadyAts = [game.units[0].abilityReadyAt, Infinity]; },
    game => { game.units[0].abilityReadyAts = [game.units[0].abilityReadyAt, -1]; },
    game => { game.units[0].abilityReadyAts = [game.units[0].abilityReadyAt + 1, 0]; },
    game => { game.units[0].rootedUntil = -1; },
    game => { game.units[0].shield = 6; },
    game => { game.units[0].cursed = 2; },
    game => { const unit=game.units[0]; game.tiles[unit.y*game.width+unit.x].terrain = 'water'; }
  ];
  changes.forEach((change, index) => rejects(change, `malformed troop ${index}`));
});

test('battle metadata, timers, resources and event log remain bounded', () => {
  const changes = [
    game => { game.width = 10; }, game => { game.height = 6; },
    game => { game.map = 'toString'; }, game => { game.factions.ai = '__proto__'; },
    game => { game.mode = 'random'; }, game => { game.terrain = 'random'; },
    game => { game.opponentMode = 'network'; }, game => { game.opponentMode = null; },
    game => { game.difficulty = 'insane'; }, game => { game.activeSide = 'enemy'; },
    game => { game.winner = false; }, game => { game.goal = 0; },
    game => { game.goal = 101; }, game => { game.goal = 1.5; },
    game => { game.duration = 59; }, game => { game.duration = 301; },
    game => { game.time = -1; }, game => { game.time = game.duration + 1; },
    game => { game.nextPulseAt = NaN; }, game => { game.nextAiAt = -1; },
    game => { game.aiRecruitAt = Infinity; }, game => { game.nextId = 1; },
    game => { game.rng = -1; }, game => { game.rng = 0x100000000; },
    game => { game.scores.player = -1; }, game => { game.scores.ai = .5; },
    game => { game.supplies.player = 41; }, game => { game.supplies.ai = '5'; },
    game => { game.log = {length:0}; }, game => { game.log.push({text:'bad'}); },
    game => { game.log = Array(81).fill('too many'); },
    game => { game.campaignId = 'unknown-mission'; }
  ];
  changes.forEach((change, index) => rejects(change, `malformed metadata ${index}`));
});

test('draft metadata rejects overspending, invalid roles and invalid initial deployments', () => {
  const changes = [
    game => { game.startingMoney = 11; }, game => { game.startingMoney = 41; },
    game => { game.startingMoney = 12.5; }, game => { game.armies = []; },
    game => { game.armies.player = []; }, game => { game.armies.ai = ['scout']; },
    game => { game.armies.player = ['leader','leader']; },
    game => { game.armies.player = ['leader','minion']; },
    game => { game.armies.player = ['leader',...Array(8).fill('scout')]; },
    game => { game.startingMoney = 12; game.armies.player = ['leader','brute','brute']; },
    game => { game.deployments = []; }, game => { game.deployments.player = {}; },
    game => { game.deployments.player.pop(); },
    game => { game.deployments.player[0].y = 0; },
    game => { game.deployments.ai[0].y = game.height - 1; },
    game => { game.deployments.player[0].x = game.width; },
    game => { game.deployments.player[0] = {...game.deployments.player[1]}; }
  ];
  changes.forEach((change, index) => rejects(change, `malformed draft ${index}`));
});
