import test from 'node:test';
import assert from 'node:assert/strict';
import { RESOURCES } from '../data.mjs';
import { PLAYER, armiesOf, atWar, commandLimit, createGame, declareWar, kingdom, parseSave, recruitmentCost, resolveMovement, settlements, sizeOf, strategyTurn } from '../core.mjs';
import { constructionSpec, emptyUnits } from '../economy.mjs';
import { endTurn } from '../diplomacy.mjs';
import { rivalTurnReports } from '../expansion-ui.mjs';

const report = (s, owner) => s.strategy.history.at(-1).houses.find(h => h.owner === owner);
function quiet(s) { for (const k of s.kingdoms) k.commands = 0; }
function stock(k, n = 400) { for (const r of RESOURCES) k.resources[r] = n; k.commands = 3; k.population = 140; }
function frontier({ walls = 0 } = {}) {
  const s = createGame(420); quiet(s);
  const k = kingdom(s, 'wintermere'), a = armiesOf(s, k.id)[0], tile = s.tiles['18,4'];
  s.armies = s.armies.filter(a => [PLAYER, k.id].includes(a.owner));
  a.units = { ...emptyUnits(), menAtArms: 40, spearman: 20, archer: 20 };
  Object.assign(tile, { owner:'thornwall', building:'town', terrain:'plains', walls, levels:{town:1,road:1,...(walls?{wall:1}:{})}, project:null });
  declareWar(s, k.id, 'thornwall');
  return { s, k, a, tile };
}

test('all living rivals get one budgeted turn without changing player orders or treasury', () => {
  const s = createGame(), before = structuredClone(s);
  strategyTurn(s);
  assert.equal(s.strategy.lastTurn, 1);
  assert.deepEqual(kingdom(s, PLAYER), kingdom(before, PLAYER));
  assert.deepEqual(armiesOf(s, PLAYER), armiesOf(before, PLAYER));
  assert.equal(s.strategy.history[0].houses.length, 5);
  for (const h of s.strategy.history[0].houses) {
    const k = kingdom(s, h.owner), old = kingdom(before, h.owner);
    const spent = Object.fromEntries(RESOURCES.map(r => [r, 0]));
    const paid = h.actions.filter(a => ['build','recruit'].includes(a.kind));
    assert.ok(paid.length > 0, `${h.owner} takes a productive opening turn`);
    for (const action of paid) {
      const cost = action.kind === 'build' ? constructionSpec(before.tiles[action.tile], action.building).cost : recruitmentCost(before.tiles[action.tile], action.unit);
      for (const [r, n] of Object.entries(cost)) spent[r] += n;
    }
    assert.equal(h.orders, paid.length);
    assert.equal(k.commands, old.commands - h.orders);
    assert.ok(h.orders <= commandLimit(before, k.id));
    for (const r of RESOURCES) assert.equal(k.resources[r], old.resources[r] - spent[r], `${k.id} pays real ${r}`);
  }
  const acted = JSON.stringify(s); strategyTurn(s);
  assert.equal(JSON.stringify(s), acted, 'calling the planner twice cannot grant a second turn');
});

test('End Turn runs rivals every round, completes their projects and records actual recruitment', () => {
  const s = createGame(420), activity = Object.fromEntries(s.kingdoms.slice(1).map(k => [k.id, {build:0,complete:0,recruit:0}]));
  for (const k of s.kingdoms.slice(1)) s.treaties.push({id:k.id,type:'peace',parties:[PLAYER,k.id],expires:100});
  for (let n = 1; n <= 12; n++) {
    endTurn(s);
    assert.equal(s.strategy.lastTurn, n); assert.equal(s.turn, n + 1);
    const round = s.strategy.history.at(-1);
    assert.equal(round.turn, n); assert.equal(round.houses.length, 5);
    for (const h of round.houses) {
      assert.ok(h.reason.length > 0);
      for (const a of h.actions) if (a.kind in activity[h.owner]) activity[h.owner][a.kind]++;
    }
  }
  for (const [id, counts] of Object.entries(activity)) {
    assert.ok(counts.build >= 2 && counts.complete >= 2 && counts.recruit >= 1, `${id}: ${JSON.stringify(counts)}`);
  }
  assert.equal(s.strategy.history.length, 6, 'reports stay bounded');
  assert.ok(settlements(s).length > 6, 'rivals found real settlements');
});

test('a depleted House saves resources and an eliminated House takes no actions', () => {
  const s = createGame(), poor = kingdom(s, 'wintermere');
  for (const r of RESOURCES) poor.resources[r] = 0;
  for (const t of Object.values(s.tiles)) if (t.owner === 'thornwall') t.owner = null;
  strategyTurn(s);
  assert.equal(report(s, poor.id).orders, 0);
  assert.match(report(s, poor.id).reason, /Saving|rebuilding/);
  assert.ok(Object.values(poor.resources).every(n => n === 0));
  assert.equal(report(s, 'thornwall').goal, 'ELIMINATED');
  assert.deepEqual(report(s, 'thornwall').actions, []);
});

test('food recovery chooses an affordable new farm when existing upgrades need unavailable tools', () => {
  const s = createGame(); quiet(s); const k = kingdom(s, 'wintermere');
  stock(k); k.commands = 1; k.resources.food = 0; k.resources.tools = 0;
  for (const t of Object.values(s.tiles)) if (t.owner === k.id && t.building === 'farm') t.building = null;
  const existing = s.tiles['18,4']; Object.assign(existing, {building:'farm',terrain:'plains',resource:'food',quality:'poor',levels:{farm:1,road:1}});
  armiesOf(s,k.id)[0].units = {...emptyUnits(),levy:120};
  const before = {...k.resources}; strategyTurn(s);
  const action = report(s,k.id).actions.find(a => a.kind === 'build');
  assert.equal(action?.building, 'farm'); assert.equal(action.level, 1); assert.notEqual(action.tile, existing.id);
  assert.equal(k.resources.tools, 0); assert.equal(k.resources.wood, before.wood - 20); assert.equal(k.resources.gold, before.gold - 10);
});

test('an army attacks and captures another bot’s vulnerable settlement through normal movement', () => {
  const {s,k,a,tile} = frontier();
  strategyTurn(s); assert.equal(a.target, tile.id); assert.equal(a.order, 'attack');
  const troops = sizeOf(a); resolveMovement(s);
  assert.equal(tile.owner, k.id); assert.ok(sizeOf(a) < troops, 'capture uses ordinary combat losses');
  assert.ok(s.militaryEvents.some(e => e.action === 'capture' && e.attacker === k.id && e.defender === 'thornwall'));
});

test('a threatened defender holds a fortified settlement instead of charging an overwhelming army', () => {
  const s = createGame(); quiet(s); declareWar(s, 'wintermere', 'thornwall');
  const a = armiesOf(s,'wintermere')[0], enemy = armiesOf(s,'thornwall')[0];
  enemy.tile = '18,4'; enemy.units = {...emptyUnits(),knight:100};
  strategyTurn(s);
  assert.equal(kingdom(s,'wintermere').goal,'DEFEND');
  assert.equal(a.tile,'17,4'); assert.equal(a.order,'hold'); assert.equal(a.formation,'defensive');
});

test('an offensive route goes around fortifications the army cannot yet besiege', () => {
  const {s,a,tile}=frontier({walls:60});
  strategyTurn(s);
  assert.equal(a.target,'31,5','the reachable unwalled enemy capital remains a viable target');
  assert.ok(a.path.length>0);assert.ok(!a.path.includes(tile.id),'infantry avoids the intervening walled town');
});

test('wall campaigns build and recruit siege support before attacking fortifications', () => {
  const {s,k,a,tile} = frontier({walls:60}); stock(k); s.turn = 12;
  for(const t of settlements(s,'thornwall')){t.walls=60;t.levels.wall=1;}
  const home = s.tiles['17,4']; home.workshop = true; home.levels.workshop = 1;
  strategyTurn(s);
  assert.equal(home.project?.type,'siegeWorks');
  assert.equal(a.path.length,0,'infantry waits for siege support');
  for(let i=0;i<6;i++){for(const other of s.kingdoms)if(other.id!==k.id)other.commands=0;endTurn(s);}
  assert.ok(s.strategy.history.some(r=>r.houses.some(h=>h.owner===k.id&&h.actions.some(a=>a.kind==='recruit'&&['ram','catapult','trebuchet'].includes(a.unit)))));
  assert.ok(s.militaryEvents.some(e=>e.action==='siege'&&e.attacker===k.id&&e.tile===tile.id));
  assert.ok(tile.walls < 60);
});

test('peace and non-aggression treaties constrain even a strong hostile bot', () => {
  const s = createGame(); quiet(s); s.turn = 20;
  const k = kingdom(s,'redharbor'); stock(k); armiesOf(s,k.id)[0].units = {...emptyUnits(),knight:150};
  for(const other of s.kingdoms.filter(o=>o.id!==k.id)) {
    Object.assign(k.relations[other.id],{opinion:-80,trust:-30,grievance:90});
    s.treaties.push({id:other.id,type:other.id===PLAYER?'non-aggression':'peace',parties:[k.id,other.id],expires:40});
  }
  strategyTurn(s);
  assert.ok(!s.wars.some(w=>w.split(':').includes(k.id)));
});

test('exhausted rival wars can end with a truce while player peace still needs ratification', () => {
  const s = createGame(); quiet(s);
  declareWar(s,'wintermere','thornwall'); declareWar(s,'wintermere',PLAYER);
  for(const id of ['wintermere','thornwall'])armiesOf(s,id)[0].units={...emptyUnits(),levy:8};
  s.turn=30; strategyTurn(s);
  assert.equal(atWar(s,'wintermere','thornwall'),false);
  assert.equal(atWar(s,'wintermere',PLAYER),true);
  assert.ok(s.treaties.some(t=>t.type==='peace'&&t.parties.includes('thornwall')&&t.expires===38));
});

test('rival activity survives saves, resumes deterministically and accepts older saves without reports', () => {
  const s=createGame(42);endTurn(s);const restored=parseSave(JSON.stringify(s));
  endTurn(s);endTurn(restored);assert.deepEqual(restored,s);
  const old=structuredClone(s);delete old.strategy;
  const migrated=parseSave(JSON.stringify(old));assert.equal(migrated.strategy.history.length,0);endTurn(migrated);
  assert.equal(migrated.strategy.lastTurn,migrated.turn-1);
  for(const mutate of [s=>s.strategy.lastTurn=s.turn+1,s=>s.strategy.history[0].houses[0].owner=PLAYER,s=>s.strategy.history[0].houses[0].actions[0]={kind:'build',tile:'missing',building:'farm',level:1}]){
    const damaged=structuredClone(s);mutate(damaged);assert.throws(()=>parseSave(JSON.stringify(damaged)),/rival strategy/);
  }
});

test('public rival reports describe actions without exposing foreign treasuries or unsafe text', () => {
  const s=createGame();endTurn(s);
  for(const k of s.kingdoms.slice(1))k.resources.gold=98765;
  report(s,'wintermere').reason='<img src=x onerror=alert(1)>';
  const html=rivalTurnReports(s);
  assert.match(html,/5 rival councils acted/);assert.match(html,/Started/);assert.match(html,/Completed/);
  assert.doesNotMatch(html,/98765|<img src=x/);assert.match(html,/&lt;img/);
});
