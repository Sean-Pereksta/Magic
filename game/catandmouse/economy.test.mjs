import assert from 'node:assert/strict';
import test from 'node:test';
import { createBattleRuntime } from './runtime-fixture.mjs';

const scenarioExtra = `
  Object.assign(window.__battleFixture, {
    pickups() {
      const now = Date.now();
      gameStarted = false;
      tickCheeseSpawns(now);
      const before = countCheesePickups();
      gameStarted = true;
      tickCheeseSpawns(now);
      const opening = countCheesePickups();
      const interval = nextCheeseSpawnAt - now;
      tickCheeseSpawns(now+1);
      const beforeTimer = countCheesePickups();
      pageSuspended = true;
      tickCheeseSpawns(now+12000);
      const whileSuspended = countCheesePickups();
      pageSuspended = false;
      tickCheeseSpawns(now+12000);
      const afterResume = countCheesePickups();
      tickCheeseSpawns(now+12001);
      const resumed = countCheesePickups();
      for(let i=0;i<35;i++) spawnCheese();
      return {before,opening,interval,beforeTimer,whileSuspended,afterResume,resumed,finally:countCheesePickups(),cap:getCheesePickupCap()};
    },
    async walls() {
      const def = BUILD_DEFS[21];
      if (!isBuildUnlockedForLocalPlayer(21)) throw Error('Ultra Wall is locked');
      playerPos = {x:1,y:1};
      players[uid].cheese = 200;
      cheeseCount = 200;
      localBuildLoadout = [21];
      mobileBuildOrder = [21];
      selectBuild('21');
      const purchases = ['w','a','s','d'];
      for(const dir of purchases) await placeStructure(dir);
      const coords = [[1,0],[0,1],[1,2],[2,1]];
      const wallData = coords.map(([x,y])=>({symbol:grid[y][x],hp:structureHealth[x+'_'+y]}));
      catPos = {x:3,y:1};
      const closedRoute = getCatMouseRoute(players[uid],new Set());
      const beacon = BUILD_DEFS[6].symbol;
      applyLocalRuntimeMutation('set',ccRef(ID.structure(0,0)),{x:0,y:0,type:beacon,health:25});
      await damageStructure(1,0,20,null);
      const nextHp = structureHealth['1_0'];
      applyLocalRuntimeMutation('set',ccRef(ID.wallHealRequest(uid,1,0)),{x:1,y:0,amount:100});
      applyLocalRuntimeMutation('set',ccRef(ID.cheese(1,0)),{x:1,y:0,expiresAt:0});
      applyLocalRuntimeMutation('delete',ccRef(ID.cheese(1,0)));
      applyLocalRuntimeMutation('delete',ccRef(ID.structure(2,1)));
      const openRoute = getCatMouseRoute(players[uid],new Set());
      return {superHp:BUILD_DEFS[5].hp,wallData,closedRoute,openRoute,nextHp,repairHp:structureHealth['1_0'],cheese:cheeseCount,def,survivedLateCheese:grid[0][1]===def.symbol};
    },
    async shared() {
      let release;
      const gate = new Promise(resolve=>{release=resolve;});
      const sharedDocs = new Map();
      const get = ref => Promise.resolve({exists:()=>sharedDocs.has(ref.id),data:()=>sharedDocs.get(ref.id)});
      let inFlight;
      runTransaction = async (_db, action) => (inFlight = gate.then(()=>action({get,set(ref,value){sharedDocs.set(ref.id,value);},delete(ref){sharedDocs.delete(ref.id);}})));
      soloMode=false;isHost=true;
      const accepted = tryPlaceCheesePickupAt(1,1);
      const acceptedAgain = tryPlaceCheesePickupAt(1,1);
      const countWhilePending = countCheesePickups();
      const spawnedEarly = isCheesePickupCell(grid[1][1]);
      sharedDocs.set(ID.structure(1,1),{type:BUILD_DEFS[21].symbol});
      release();
      await inFlight;
      await new Promise(resolve=>Promise.resolve().then(resolve));
      const committedOnWall = sharedDocs.has(ID.cheese(1,1));
      sharedDocs.set(ID.cheese(2,2),{kind:'cheese'});
      playerWins=0;isAlive=true;playerPos={x:2,y:1};cheeseCount=25;
      const placed = await commitMultiplayerStructurePurchase({tx:2,ty:2,buildId:21,def:BUILD_DEFS[21],cost:18});
      const acceptedBeforeTransfer = tryPlaceCheesePickupAt(3,3);
      isHost=false;
      await inFlight;
      return {
        accepted, acceptedAgain, countWhilePending, spawnedEarly, committedOnWall,
        placed:placed.ok, pickupDeleted:!sharedDocs.has(ID.cheese(2,2)), acceptedBeforeTransfer,
        committedAfterTransfer:sharedDocs.has(ID.cheese(3,3))
      };
    }
  });`;

test('classic Ultra Wall can be built repeatedly and shares the wall blocking and support rules', async () => {
  const match = await createBattleRuntime(false, { scenarioExtra });
  const { superHp, wallData, closedRoute, openRoute, nextHp, repairHp, cheese, def, survivedLateCheese } = await match.fixture.walls();
  assert.equal(def.hp, 5 * superHp);
  assert.equal(wallData.length, 4);
  for (const wall of wallData) {
    assert.equal(wall.symbol, def.symbol);
    assert.equal(wall.hp, def.hp);
  }
  assert.equal(closedRoute, null);
  assert.equal(openRoute.length, 2);
  assert.equal(nextHp, def.hp - 10);
  assert.equal(repairHp, def.hp);
  assert.equal(cheese, 200 - 4 * def.cost);
  assert.equal(survivedLateCheese, true);
  assert.deepEqual(match.warnings, []);
});

test('classic natural cheese starts with the opening fill and resumes with a single spawn tick', async () => {
  const match = await createBattleRuntime(false, { scenarioExtra });
  const pickups = match.fixture.pickups();
  assert.equal(pickups.before, 0);
  assert.equal(pickups.opening, 16);
  assert.equal(pickups.interval, 4000);
  assert.equal(pickups.beforeTimer, 16);
  assert.equal(pickups.whileSuspended, 16);
  assert.ok(pickups.afterResume === 18 || pickups.afterResume === 19);
  assert.equal(pickups.resumed, pickups.afterResume);
  assert.equal(pickups.finally, 28);
  assert.equal(pickups.cap, 28);
  assert.deepEqual(match.warnings, []);
});

test('classic shared pickup reservations handle building purchases and host transfers', async () => {
  const match = await createBattleRuntime(false, { scenarioExtra });
  const shared = await match.fixture.shared();
  assert.deepEqual({ ...shared }, {
    accepted: true, acceptedAgain: false, countWhilePending: 1, spawnedEarly: false,
    committedOnWall: false, placed: true, pickupDeleted: true, acceptedBeforeTransfer: true,
    committedAfterTransfer: false
  });
  assert.deepEqual(match.warnings, []);
});
