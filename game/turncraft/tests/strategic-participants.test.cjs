const {test} = require('node:test');
const assert = require('node:assert/strict');
const {pathToFileURL} = require('node:url');
const path = require('node:path');
const {simulation} = require('./helpers/expansion-harness.cjs');
const configModule = () => import(pathToFileURL(path.resolve(__dirname, '../shattered-front/battle-config.mjs')).href);

test('solo setup creates all four and eight commanders with individual AI settings', () => {
  for (const [map, count, size] of [['fourfold-divide', 3, 96], ['rift-dominion', 7, 128]]) {
    const run = simulation();
    const state = run.plain(`(()=>{chosenMapId=${JSON.stringify(map)};chosenAiCount=${count};chosenAiConfigs=[{factionId:'brood',difficulty:'pro',style:'aggressive'},{factionId:'frontier',difficulty:'easy',style:'cautious'}];createGame(buildSoloSlots());return serializeState()})()`);
    assert.equal(state.players.length, count + 1);
    assert.equal(state.players.filter(player => player.ai).length, count);
    assert.equal(state.boardWidth, size);assert.equal(state.boardHeight, size);
    assert.equal(state.terrainCode.length, size * size);
    assert.equal(state.players[1].factionId, 'brood');assert.equal(state.players[1].aiDifficulty, 'pro');assert.equal(state.players[2].aiStyle, 'cautious');
    assert.equal(state.entities.filter(entity => ['commandHub','nexus','hatchery'].includes(entity.type)).length, count + 1);
    assert.equal(new Set(state.entities.filter(entity => ['commandHub','nexus','hatchery'].includes(entity.type)).map(entity => `${entity.x},${entity.y}`)).size, count + 1);
  }
});

test('setup recommends compatible maps and rejects over-capacity direct launches', () => {
  const run = simulation();
  const slots = run.plain(`chosenMapId='classic';chosenAiCount=7;buildSoloSlots()`);
  assert.equal(slots.length, 8);assert.equal(run('mapCapacity()'), 8);
  assert.throws(() => run(`chosenMapId='ashen-causeway';createGame(${JSON.stringify(slots)})`), /enough starting positions/);
  assert.equal(run(`chosenMapId='classic';chosenAiCount=3;ensureCompatibleMap();mapCapacity()`), 4);
});

test('large snapshot restores dimensions before terrain decode and rejects corruption atomically', () => {
  const host = simulation(), client = simulation();
  const state = host.plain(`chosenMapId='rift-dominion';chosenAiCount=7;createGame(buildSoloSlots());serializeState()`);
  client.context.remoteFixture = state;
  assert.equal(client('applyRemoteState(remoteFixture)'), true);
  assert.equal(client('BOARD_W'), 128);assert.equal(client('BOARD_H'), 128);
  assert.equal(client('encodeTerrain()'), state.terrainCode);
  assert.deepEqual(client.plain('players.map(p=>[p.name,p.factionId,p.ai])'), state.players.map(p=>[p.name,p.factionId,p.ai]));
  for (const invalid of [{...state, boardWidth:96}, {...state, terrainCode:state.terrainCode.slice(1)}, {...state, players:[...state.players,state.players[0]]}, {...state, entities:[...state.entities,{id:9999,x:128,y:10,faction:0}]}, {...state,entities:{}}, {...state,resourceNodes:{}}, {...state,scanEffects:{}}]) {
    client.context.remoteFixture = invalid;assert.equal(client('applyRemoteState(remoteFixture)'),false);
    assert.equal(client('BOARD_W'),128);assert.equal(client('encodeTerrain()'),state.terrainCode);
  }
  const legacy = host.plain(`chosenMapId='classic';chosenAiCount=2;createGame(buildSoloSlots());serializeState()`);
  delete legacy.boardWidth;delete legacy.boardHeight;client.context.remoteFixture = legacy;
  client('simulationTick=0');assert.equal(client('applyRemoteState(remoteFixture)'),true);assert.equal(client('BOARD_W'),42);assert.equal(client('BOARD_H'),30);
});

test('multiplayer accepts four humans and two humans with AI fill, rejecting incompatible maps', async () => {
  const {validateBattleSettings,battleParticipantCount,battleSlots,LOBBY_MAPS} = await configModule();
  const settings=validateBattleSettings({mapId:'fourfold-divide',maxPlayers:4,fillAi:true,aiDifficulty:'hard',aiStyle:'aggressive'});
  assert.equal(battleParticipantCount(settings,2),4);assert.equal(battleParticipantCount(settings,4),4);
  const room={settings,turncraftSeats:['one','two'],turncraftPlayers:{one:{name:'One',factionId:'frontier'},two:{name:'Two',factionId:'brood'}}};
  const slots=battleSlots(room);assert.equal(slots.length,4);assert.equal(slots.filter(slot=>slot.ai).length,2);assert.equal(slots[2].aiDifficulty,'hard');
  assert.throws(()=>validateBattleSettings({mapId:'ashen-causeway',maxPlayers:4}),/compatible battlefield/);
  assert.throws(()=>validateBattleSettings({mapId:'rift-dominion',maxPlayers:4}),/compatible battlefield/);
  assert.throws(()=>battleParticipantCount(settings,5),/does not fit/);
  assert.ok(LOBBY_MAPS.includes('rift-bastion'));assert.ok(LOBBY_MAPS.includes('classic'));
});
