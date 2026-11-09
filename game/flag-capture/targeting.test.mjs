import test from 'node:test';
import assert from 'node:assert/strict';
import { FACTIONS, unitDef } from './engine.mjs';
import { actionColor, displayTargets, groundPreviewCells } from './targeting.mjs';

const colors = {
  bloomcall: 'heal', sporehop: 'move', belch: 'attack', sporeburst: 'attack',
  wrongnote: 'attack', blinkbite: 'attack', foldhorn: 'attack', omen: 'attack',
  concordance: 'support', mercy: 'heal', sanctuary: 'support', ray: 'attack',
  nectar: 'heal', seedgrove: 'support', wardray: 'support', starstep: 'move',
  laughinghex: 'attack', crookedswap: 'attack',
  broodlings: 'support', pocketmaws: 'support', bellmotes: 'support',
  rootguard:'support',thornfield:'attack',pinshot:'attack',pollenstep:'move',mycelium:'support',germinate:'heal',
  grinward:'support',cinders:'attack',crosscut:'attack',riftstep:'move',rift:'support',detonate:'attack',
  aegislink:'support',sanctum:'support',sunlance:'attack',prismwall:'support',dawnseal:'support',choirswap:'move'
};
const creature = (id, side, x, y, extra = {}) => ({
  id, side, x, y, hp: 8, maxHp: 10, shield: 0, readyAt: 0,
  faction: 'bloom', role: 'leader', ...extra
});
const selfAction = unit => ({ type: 'ability', unitId: unit.id, targetId: unit.id, x: unit.x, y: unit.y, label: 'Canonical skill' });
const stateWith = (...units) => ({ width: 14, height: 11, time: 5, units });

test('ground footprint previews retain their canonical center and exclude unpaintable terrain',()=>{
  const caster=creature('caster','player',1,1,{role:'shaper'}),definition=unitDef(caster);
  const skillIndex=definition.abilities.findIndex(skill=>skill.groundKind==='mycelium');
  const action={type:'ability',unitId:caster.id,skillIndex,x:2,y:2},snapshot=structuredClone(action);
  const tiles=Array.from({length:25},(_,i)=>({x:i%5,y:Math.floor(i/5),terrain:'grass'}));
  tiles[2*5+3].terrain='rock';tiles[3*5+2].terrain='water';tiles[2*5+2].terrain='grove';
  const state={width:5,height:5,tiles,units:[caster]};
  assert.deepEqual(groundPreviewCells(state,caster,action,unitDef),[{x:2,y:1},{x:1,y:2},{x:2,y:2}]);
  assert.deepEqual(action,snapshot,'paint footprint does not turn neighboring squares into different command centers');
  assert.deepEqual(groundPreviewCells(state,caster,{type:'move',x:2,y:2},unitDef),[]);
  assert.deepEqual(groundPreviewCells(state,null,action,unitDef),[]);
});

test('edge footprints clip to the board and non-ground abilities never claim painted cells',()=>{
  const caster=creature('caster','player',0,0,{role:'shaper'}),definition=unitDef(caster),skillIndex=definition.abilities.findIndex(skill=>skill.groundKind==='mycelium');
  const state={tiles:Array.from({length:9},(_,i)=>({x:i%3,y:Math.floor(i/3),terrain:'grass'}))};
  assert.deepEqual(groundPreviewCells(state,caster,{type:'ability',skillIndex,x:0,y:0},unitDef),[{x:0,y:0},{x:1,y:0},{x:0,y:1}]);
  const healer=creature('healer','player',0,0);
  assert.deepEqual(groundPreviewCells(state,healer,{type:'ability',skillIndex:0,x:0,y:0},unitDef),[]);
});

test('Pollen Skate and Riftstep preview both departure and arrival without claiming the intervening route',()=>{
  const state={tiles:Array.from({length:25},(_,i)=>({x:i%5,y:Math.floor(i/5),terrain:'grass'}))};
  for(const faction of ['bloom','chaos']){
    const caster=creature('hop','player',0,2,{faction,role:'ranger'}),skillIndex=1,action={type:'ability',skillIndex,x:3,y:2};
    assert.deepEqual(groundPreviewCells(state,caster,action,unitDef),[{x:0,y:2},{x:3,y:2}]);
    assert.equal(actionColor(action,caster,unitDef),'move');
  }
});

test('Curtain Collapse previews the blast across shorelines because flying enemies there can be damaged',()=>{
  const caster=creature('jester','player',1,3,{faction:'chaos',role:'shaper'});
  const state={tiles:Array.from({length:25},(_,i)=>({x:i%5,y:Math.floor(i/5),terrain:i===7?'water':i===13?'rock':'grass'}))};
  const action={type:'ability',skillIndex:1,x:2,y:2},preview=groundPreviewCells(state,caster,action,unitDef);
  assert.deepEqual(preview,[{x:2,y:1},{x:1,y:2},{x:2,y:2},{x:3,y:2},{x:2,y:3}]);
  assert.equal(actionColor(action,caster,unitDef),'attack');assert.equal(action.x,2);assert.equal(action.y,2);
});

test('Rootguard and Grinning Ward show shieldable allies while retaining their canonical self center',()=>{
  for(const faction of ['bloom','chaos']){
    const caster=creature('guardian','player',2,2,{faction,role:'guardian',shield:5}),friend=creature('friend','player',3,2,{shield:1}),full=creature('full','player',2,3,{shield:5}),enemy=creature('enemy','ai',1,2);
    const action={...selfAction(caster),skillIndex:0},before=structuredClone(action),result=displayTargets(stateWith(caster,friend,full,enemy),caster,[action],unitDef);
    assert.deepEqual(result,[{...action,displayX:3,displayY:2}]);assert.deepEqual(action,before);
    assert.equal(actionColor(result[0],caster,unitDef),'support');
  }
});

test('Grinning Ward keeps a gold caster target when its root is useful but every friendly shield is full',()=>{
  const caster=creature('gate','player',2,2,{faction:'chaos',role:'guardian',shield:5}),friend=creature('friend','player',3,2,{shield:5}),enemy=creature('enemy','ai',1,2),action={...selfAction(caster),skillIndex:0};
  assert.deepEqual(displayTargets(stateWith(caster,friend,enemy),caster,[action],unitDef),[action]);
  assert.deepEqual(displayTargets(stateWith(caster,friend),caster,[action],unitDef),[]);
  assert.equal(actionColor(action,caster,unitDef),'support');
});

test('all thirty-nine unique skills use their gameplay color; only healing skills are green', () => {
  const expectedCounts = {
    bloom: {leader:3,scout:1,brute:1,mystic:2,guardian:2,ranger:2,shaper:2,minion:0},
    chaos: {leader:2,scout:1,brute:1,mystic:3,guardian:2,ranger:2,shaper:2,minion:0},
    choir: {leader:2,scout:1,brute:1,mystic:3,guardian:2,ranger:2,shaper:2,minion:0}
  };
  const kinds = new Set();
  for (const faction of Object.keys(FACTIONS)) {
    for (const role of Object.keys(FACTIONS[faction].units)) {
      const unit = { faction, role };
      const definition = unitDef(unit);
      assert.equal(definition.abilities.length, expectedCounts[faction][role], `${faction} ${role}`);
      for (const [skillIndex, skill] of definition.abilities.entries()) {
        const kind = skill.kind;
        kinds.add(kind);
        const actual = actionColor({ type: 'ability', skillIndex }, unit, unitDef);
        assert.equal(actual, colors[kind], kind);
        assert.equal(actual === 'heal', ['bloomcall', 'mercy', 'nectar', 'germinate'].includes(kind), kind);
      }
      assert.equal(actionColor({ type: 'move' }, unit, unitDef), 'move');
      assert.equal(actionColor({ type: 'attack' }, unit, unitDef), 'attack');
      assert.equal(actionColor({ type: 'guard' }, unit, unitDef), 'support');
    }
  }
  assert.equal(kinds.size, 39);
});

test('all three summons retain direct empty-square targets and use support gold', () => {
  for (const [faction, role, kind] of [['bloom','leader','broodlings'],['chaos','mystic','pocketmaws'],['choir','mystic','bellmotes']]) {
    const caster = creature('caster', 'player', 4, 4, { faction, role });
    const skillIndex = unitDef(caster).abilities.findIndex(skill => skill.kind === kind);
    assert.equal(skillIndex, 2);
    const action = { type:'ability',unitId:caster.id,skillIndex,x:4,y:3 };
    const actions = [action];
    assert.equal(actionColor(action, caster, unitDef), 'support', kind);
    assert.equal(displayTargets(stateWith(caster), caster, actions, unitDef), actions);
    assert.deepEqual(action, {type:'ability',unitId:'caster',skillIndex:2,x:4,y:3});
  }
});

test('mixed ability actions use their own color and only expand the selected self-area skill', () => {
  const caster = creature('caster', 'player', 10, 8, { hp: 10 });
  const friend = creature('friend', 'player', 11, 8);
  const primary = { ...selfAction(caster), skillIndex: 0 };
  const secondary = { type: 'ability', unitId: caster.id, skillIndex: 1, x: 10, y: 7, label: 'Plant grove' };
  const definition = {
    ability: { kind: 'bloomcall', range: 2 },
    abilities: [{ kind: 'bloomcall', range: 2 }, { kind: 'seedgrove', range: 3 }]
  };
  const getDefinition = () => definition;
  const actions = displayTargets(stateWith(caster, friend), caster, [primary, secondary], getDefinition);
  assert.deepEqual(actions, [{ ...primary, displayX: 11, displayY: 8 }, secondary]);
  assert.equal(actions[0].skillIndex, 0);
  assert.equal(actionColor(actions[0], caster, getDefinition), 'heal');
  assert.equal(actionColor(actions[1], caster, getDefinition), 'support');
  assert.equal(actionColor({ ...primary, skillIndex: undefined }, caster, getDefinition), 'heal', 'missing index retains primary compatibility');
});

test('secondary Nectar keeps its direct injured ally target and never expands the primary area', () => {
  const caster = creature('caster', 'player', 4, 4);
  const friend = creature('friend', 'player', 5, 4);
  const nectar = { type: 'ability', unitId: caster.id, targetId: friend.id, skillIndex: 1, x: friend.x, y: friend.y };
  const selfNectar = { ...nectar, targetId: caster.id, x: caster.x, y: caster.y };
  const getDefinition = () => ({ ability: {kind:'bloomcall',range:2}, abilities:[{kind:'bloomcall',range:2},{kind:'nectar',range:3}] });
  const actions = [nectar, selfNectar];
  assert.equal(displayTargets(stateWith(caster, friend), caster, actions, getDefinition), actions);
  assert.equal(actionColor(nectar, caster, getDefinition), 'heal');
  assert.equal(actionColor(selfNectar, caster, getDefinition), 'heal');
  assert.deepEqual(nectar, { type:'ability',unitId:caster.id,targetId:friend.id,skillIndex:1,x:5,y:4 });
});

test('Bloomcall shows only living injured allies inside its Manhattan radius', () => {
  const caster = creature('caster', 'player', 10, 8, { hp: 10 });
  const injured = creature('injured', 'player', 12, 8);
  const full = creature('full', 'player', 10, 9, { hp: 10 });
  const enemy = creature('enemy', 'ai', 9, 8);
  const dead = creature('dead', 'player', 10, 7, { hp: 0 });
  const far = creature('far', 'player', 12, 9);
  const action = selfAction(caster);
  const state = stateWith(caster, injured, full, enemy, dead, far);
  const result = displayTargets(state, caster, [action], unitDef);
  assert.deepEqual(result, [{ ...action, displayX: 12, displayY: 8 }]);
  assert.deepEqual(action, selfAction(caster), 'canonical action was not mutated');
  assert.equal(result[0].x, 10);
  assert.equal(result[0].y, 8);
  assert.equal(result[0].targetId, 'caster');
});

test('injured Bloomcall caster can target itself and full-health allies never appear', () => {
  const caster = creature('caster', 'player', 1, 1);
  const full = creature('full', 'player', 2, 1, { hp: 10 });
  assert.deepEqual(displayTargets(stateWith(caster, full), caster, [selfAction(caster)], unitDef)
    .map(action => [action.displayX, action.displayY]), [[1, 1]]);
});

test('Wrong Note shows enemy effect squares while retaining the self-targeted action', () => {
  const caster = creature('maestro', 'player', 10, 8, { faction: 'chaos' });
  const ally = creature('ally', 'player', 10, 7);
  const enemy = creature('enemy', 'ai', 11, 9);
  const far = creature('far', 'ai', 12, 9);
  const dead = creature('dead', 'ai', 9, 8, { hp: 0 });
  const action = selfAction(caster);
  const result = displayTargets(stateWith(caster, ally, enemy, far, dead), caster, [action], unitDef);
  assert.deepEqual(result, [{ ...action, displayX: 11, displayY: 9 }]);
  assert.equal(actionColor(result[0], caster, unitDef), 'attack');
});

test('Sanctuary highlights only allies who can improve to a four-point ward', () => {
  const caster = creature('bell', 'player', 4, 4, { faction: 'choir', role: 'brute', shield: 5 });
  const ally = creature('ally', 'player', 4, 5, { shield: 3 });
  const capped = creature('capped', 'player', 5, 4, { shield: 5 });
  const enemy = creature('enemy', 'ai', 3, 4);
  const diagonal = creature('diagonal', 'player', 5, 5);
  const result = displayTargets(stateWith(caster, ally, capped, enemy, diagonal), caster, [selfAction(caster)], unitDef);
  assert.deepEqual(result.map(action => [action.displayX, action.displayY]), [[4, 5]]);
  assert.equal(actionColor(result[0], caster, unitDef), 'support');
});

test('Concordance highlights other allies that can gain a refresh or shield', () => {
  const caster = creature('halo', 'player', 5, 5, { faction: 'choir', shield: 0 });
  const cooling = creature('cooling', 'player', 6, 5, { shield: 3, readyAt: 6 });
  const lowShield = creature('low-shield', 'player', 5, 6, { shield: 0, readyAt: 5 });
  const readyCapped = creature('ready-capped', 'player', 4, 5, { shield: 3, readyAt: 5 });
  const enemy = creature('enemy', 'ai', 5, 4);
  const result = displayTargets(stateWith(caster, cooling, lowShield, readyCapped, enemy), caster, [selfAction(caster)], unitDef);
  assert.deepEqual(result.map(action => [action.displayX, action.displayY]), [[6, 5], [5, 6]]);
  assert.ok(result.every(action => action.targetId === caster.id));
});

test('direct-targeted skills and non-ability commands retain canonical coordinates', () => {
  const caster = creature('comet', 'player', 1, 2, { faction: 'choir', role: 'scout' });
  const mercy = { type: 'ability', unitId: caster.id, targetId: 'friend', x: 2, y: 3 };
  const actions = [mercy];
  assert.equal(displayTargets(stateWith(caster), caster, actions, unitDef), actions);
  assert.deepEqual(mercy, { type: 'ability', unitId: caster.id, targetId: 'friend', x: 2, y: 3 });
  const leader = { ...caster, role: 'leader' };
  const move = { type: 'move', unitId: leader.id, x: 2, y: 2 };
  const attack = { type: 'attack', unitId: leader.id, x: 1, y: 2, targetId: 'enemy' };
  const nonSelfAbility = { type: 'ability', unitId: leader.id, x: 2, y: 2, targetId: 'other' };
  assert.deepEqual(displayTargets(stateWith(leader), leader, [move, attack, nonSelfAbility], unitDef), [move, attack, nonSelfAbility]);
});

test('empty actions remain empty and a missing selected unit is safe', () => {
  const caster = creature('caster', 'player', 0, 0);
  const empty = [];
  assert.equal(displayTargets(stateWith(caster), caster, empty, unitDef), empty);
  assert.equal(displayTargets(stateWith(), null, empty, unitDef), empty);
});
