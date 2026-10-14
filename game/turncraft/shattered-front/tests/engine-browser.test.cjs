const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { chromium } = require('playwright');
const repo = path.resolve(__dirname, '../../../..');
const stub = `const initializeApp=()=>({}),getAuth=()=>({currentUser:{uid:'browser-test'}}),getFirestore=()=>({}),doc=(...parts)=>parts.slice(1).join('/'),getDoc=async()=>({exists:()=>true,data:()=>window.sfLobby}),onAuthStateChanged=(auth,callback)=>{queueMicrotask(()=>callback(auth.currentUser));return()=>{};},signInAnonymously=async()=>{},serverTimestamp=()=>({test:true}),setDoc=async(ref,data)=>{window.sfWrites=window.sfWrites||[];sfWrites.push({ref,data:JSON.parse(JSON.stringify(data))});};`;
const expose = `window.sfTest={run:source=>eval(source),advance(seconds){for(let n=0;n<seconds*10;n++){window.sfClock+=100;updatePersistentOrders();updateCombat(.1);resolveProjectileImpacts();updateProduction(.1);updateRegeneration(.1);updateAreaEffects();window.TurncraftExpansion.tick(.1)}},clearWave(){for(const id of window.TurncraftExpansion.getState().activeIds){const e=entityById(id);if(e)killEntity(e,playerUnits(0).find(u=>!isWorker(u)))}}};window.sfClock=Date.now();nowMs=()=>window.sfClock;resize();window.sfResults=[];addEventListener('sf:result',event=>sfResults.push(event.detail));`;
const source = fs
  .readFileSync(path.join(repo, 'game/turncraft.html'), 'utf8')
  .replace(/^import .*https:\/\/www\.gstatic\.com.*;$/gm, '')
  .replace('<script type="module">', '<script type="module">' + stub)
  .replace('\nboot();', '\n' + expose)
  .replace(
    '<script type="module" src="turncraft/shattered-front/menu.mjs"></script>',
    ''
  );
async function setup(viewport = { width: 1360, height: 900 }) {
  const options = { headless: true, args: ['--no-sandbox'] };
  if (process.env.CHROMIUM_EXECUTABLE_PATH)
    options.executablePath = process.env.CHROMIUM_EXECUTABLE_PATH;
  const browser = await chromium.launch(options),
    page = await browser.newPage({ viewport }),
    errors = [];
  page.on('pageerror', (error) => errors.push(error.message));
  await page.route('**/*', (route) => {
    const url = new URL(route.request().url());
    if (url.hostname !== 'turncraft.test')
      return route.fulfill({ status: 404, body: '' });
    const pathname = decodeURIComponent(url.pathname);
    if (pathname === '/game/turncraft.html')
      return route.fulfill({ contentType: 'text/html', body: source });
    if (pathname === '/game/turncraft/shattered-front/profile.mjs')
      return route.fulfill({
        contentType: 'text/javascript',
        body: 'export async function request(path,options){return window.sfServerRequest(path,options);} export function rankFor(mmr,matches){return matches?(mmr>=1500?"Diamond":mmr>=1100?"Gold":"Silver"):"Unranked";}'
      });
    const file = path.resolve(repo, '.' + pathname);
    if (
      file.startsWith(repo + path.sep) &&
      fs.existsSync(file) &&
      fs.statSync(file).isFile()
    )
      return route.fulfill({
        path: file,
        contentType: file.endsWith('.mjs') ? 'text/javascript' : undefined
      });
    return route.fulfill({ status: 404, body: '' });
  });
  await page.goto('http://turncraft.test/game/turncraft.html');
  try {
    await page.waitForFunction(
      () => window.sfTest && window.TurncraftExpansion,
      {},
      { timeout: 10000 }
    );
    assert.deepEqual(errors, []);
  } catch (error) {
    await browser.close();
    throw new Error(`${error.message}; browser errors: ${errors.join('; ')}`);
  }
  return { browser, page, errors };
}
test('real RTS engine launches every region/faction and preserves movement, production and combat', async () => {
  const { browser, page, errors } = await setup();
  try {
    for (const factionId of ['frontier', 'ascendant', 'brood'])
      for (const region of [
        'amber-wastes',
        'ironwood-basin',
        'sundered-coast',
        'ashglass-expanse',
        'frozen-rift',
        'heart-of-fracture'
      ]) {
        const state = await page.evaluate(
          ({ factionId, region }) => {
            const s = TurncraftExpansion.startSurvival(region, {
              factionId,
              seed: 47
            });
            sfTest.run('draw();');
            return {
              state: s,
              entityCount: sfTest.run('entities.length'),
              map: sfTest.run('chosenMapId'),
              base: sfTest.run('playerStructures(0).some(isBase)')
            };
          },
          { factionId, region }
        );
        assert.equal(state.state.regionId, region);
        assert.equal(state.state.factionId, factionId);
        assert.ok(state.base);
        assert.ok(state.entityCount >= 8);
        assert.equal(state.map, 'sf-' + region);
      }
    const moved = await page.evaluate(() => {
      TurncraftExpansion.startCampaign('amber-wastes-1', {
        factionId: 'frontier',
        seed: 33
      });
      return sfTest.run(
        `(()=>{const u=playerUnits(0).find(e=>e.type==='rifleman'),before={x:u.x,y:u.y};issueMoveOrder(u,10,15);window.sfTest.advance(8);return {before,after:{x:u.x,y:u.y},workers:playerUnits(0).filter(isWorker).length};})()`
      );
    });
    assert.notDeepEqual(moved.after, moved.before);
    assert.equal(moved.workers, 4);
    const combat = await page.evaluate(() =>
      sfTest.run(
        `(()=>{tileVisibleToFaction=()=>true;const unit=playerUnits(0).find(e=>e.type==='rifleman'),enemy=addEntity('unit','zergling',1,unit.x+1,unit.y);unit.attackReadyAt=0;const hp=enemy.hp;fireAt(unit,enemy);window.sfTest.advance(2);return {hp,after:enemy.hp,shots:ensureMatchStats(players[0]).damageDealt};})()`
      )
    );
    assert.ok(combat.after < combat.hp);
    assert.ok(combat.shots > 0);
    assert.deepEqual(errors, []);
  } finally {
    await browser.close();
  }
});
test('real mission finishes, survival reaches 30 waves, and a fresh page restores a live checkpoint', async () => {
  const { browser, page, errors } = await setup();
  try {
    const result = await page.evaluate(() => {
      TurncraftExpansion.startCampaign('amber-wastes-1', {
        factionId: 'frontier',
        seed: 19
      });
      let ticks = 0;
      while (!sfResults.length && ticks++ < 1000) {
        const state = TurncraftExpansion.getState();
        if (state.phase === 'preparation') TurncraftExpansion.nextWave();
        TurncraftExpansion.tick(1);
        sfTest.clearWave();
      }
      return { ticks, result: sfResults[0], gameOver: sfTest.run('gameOver') };
    });
    assert.ok(result.ticks < 1000);
    assert.equal(result.result.won, true);
    assert.equal(result.result.wave, 4);
    assert.ok(result.gameOver);
    const saved = await page.evaluate(() => {
      TurncraftExpansion.startSurvival('sundered-coast', {
        factionId: 'ascendant',
        seed: 390
      });
      let ticks = 0;
      while (TurncraftExpansion.getState().cleared < 30 && ticks++ < 10000) {
        const s = TurncraftExpansion.getState();
        if (s.phase === 'preparation') TurncraftExpansion.nextWave();
        TurncraftExpansion.tick(1);
        sfTest.clearWave();
      }
      TurncraftExpansion.nextWave();
      TurncraftExpansion.tick(1);
      return TurncraftExpansion.checkpoint();
    });
    assert.equal(saved.director.state.cleared, 30);
    assert.equal(saved.director.state.wave, 31);
    assert.ok(saved.director.state.activeIds.length);
    assert.ok(
      (await page.evaluate(() => sfTest.run('entities.length'))) < 150,
      'cleared armies must not accumulate as thousands of dead simulation entities'
    );
    const entityIds = saved.simulation.entities.map((e) => e.id);
    await page.reload();
    await page.waitForFunction(() => window.sfTest);
    const resumed = await page.evaluate((saved) => {
      const state = TurncraftExpansion.resume(saved);
      sfTest.run('draw();');
      return {
        state,
        ids: sfTest.run('entities.map(e=>e.id)'),
        nodes: sfTest.run('resourceNodes.length')
      };
    }, saved);
    assert.equal(resumed.state.wave, 31);
    assert.equal(resumed.state.cleared, 30);
    assert.equal(resumed.state.seed, 390);
    assert.deepEqual(resumed.ids, entityIds);
    assert.ok(resumed.nodes > 0);
    const lost = await page.evaluate(() => {
      sfTest.run(
        'for(const base of playerStructures(0).filter(isBase))killEntity(base,playerUnits(1)[0])'
      );
      return sfResults.at(-1);
    });
    assert.equal(lost.mode, 'survival');
    assert.equal(lost.won, false);
    assert.equal(lost.wave, 30);
    assert.deepEqual(errors, []);
  } finally {
    await browser.close();
  }
});
test('mobile wave HUD stays within the screen and its button is touch sized', async () => {
  const { browser, page, errors } = await setup({ width: 390, height: 844 });
  try {
    await page.evaluate(() =>
      TurncraftExpansion.startCampaign('amber-wastes-1', { seed: 1 })
    );
    const hud = await page.locator('#sfBattleHud').boundingBox(),
      button = await page.locator('#sfNextWave').boundingBox();
    assert.ok(hud.x >= 0 && hud.x + hud.width <= 390);
    assert.ok(button.height >= 44);
    await page.locator('#sfNextWave').click();
    assert.equal(
      await page.evaluate(() => TurncraftExpansion.getState().wave),
      1
    );
    assert.deepEqual(errors, []);
  } finally {
    await browser.close();
  }
});
test('ranked browser takes server snapshots and sends raw commands without simulating locally', async () => {
  const { browser, page, errors } = await setup();
  try {
    const result = await page.evaluate(async () => {
      sfTest.run(
        `chosenMapId='classic';chosenGameMode='annihilation';createGame([{name:'Test',factionId:'frontier',ai:false},{name:'Rival',factionId:'brood',ai:false}]);window.sfServerState=serializeState();gameStarted=false;lobbyId='ranked-room';`
      );
      window.sfLobby = {
        turncraftVersion: 1,
        ranked: true,
        matchId: 'test-match',
        status: 'started',
        members: { 'browser-test': 'Test', rival: 'Rival' },
        turncraftSeats: ['browser-test', 'rival'],
        turncraftPlayers: {
          'browser-test': {
            name: 'Test',
            factionId: 'frontier',
            mmr: 1250,
            rankedMatches: 12
          },
          rival: {
            name: 'Rival',
            factionId: 'brood',
            mmr: 1600,
            rankedMatches: 20
          }
        },
        settings: { mapId: 'classic', gameMode: 'annihilation' }
      };
      window.sfRequests = [];
      window.sfServerRequest = async (path, options) => {
        sfRequests.push({ path, options });
        return { state: sfServerState };
      };
      await sfTest.run('bootNetwork()');
      const id = sfServerState.entities.find(
        (e) => e.faction === 0 && e.kind === 'unit'
      ).id;
      await sfTest.run(`sendCommand({type:'move',entityId:${id},x:10,y:15})`);
      const before = sfTest.run('JSON.stringify(entities)');
      sfTest.run('requestAnimationFrame=()=>0;frame(performance.now()+50)');
      return {
        requests: sfRequests,
        controller: sfTest.run('isController'),
        ranked: sfTest.run('rankedMatchId'),
        localUnchanged: before === sfTest.run('JSON.stringify(entities)'),
        badge: document.getElementById('networkText').textContent
      };
    });
    assert.equal(result.controller, false);
    assert.equal(result.ranked, 'test-match');
    assert.ok(result.localUnchanged);
    assert.match(result.badge, /Server verified/);
    const post = result.requests.find((r) => r.options?.method === 'POST');
    assert.ok(post, JSON.stringify(result.requests));
    assert.deepEqual(Object.keys(post.options.body.command).sort(), [
      'entityId',
      'type',
      'x',
      'y'
    ]);
    assert.match(post.options.body.requestId, /^[a-zA-Z0-9_-]{15,120}$/);
    assert.match(
      await page.locator('#sfMatchCommanders').textContent(),
      /Test[\s\S]*Gold · 1250 MMR[\s\S]*Rival[\s\S]*Diamond · 1600 MMR/
    );
    assert.equal(
      await page.locator('#sfMatchCommanders img[alt="Gold rank"]').count(),
      1
    );
    const receipt = await page.evaluate(async () => {
      window.sfServerState = {
        ...sfServerState,
        simulationTick: sfServerState.simulationTick + 1,
        gameOver: true,
        matchResult: {
          winnerIdx: 0,
          winnerName: 'Test',
          winnerFactionId: 'frontier',
          finishedAt: Date.now(),
          modeId: 'annihilation',
          reason: 'Server victory'
        }
      };
      window.sfServerRequest = async () => ({
        state: sfServerState,
        ratingReceipt: {
          verified: true,
          changes: { 'browser-test': { delta: 17, after: 1017 } }
        }
      });
      await sfTest.run('pollRankedState()');
      return document.getElementById('endText').textContent;
    });
    assert.match(receipt, /\+17 MMR/);
    assert.deepEqual(errors, []);
  } finally {
    await browser.close();
  }
});
test('custom terminal state is persisted immediately and the second client receives its defeat', async () => {
  const host = await setup(),
    guest = await setup();
  try {
    const terminal = await host.page.evaluate(async () => {
      window.sfCompletions = [];
      window.sfServerRequest = async (path, options) => {
        sfCompletions.push({
          path,
          options,
          persisted: sfWrites.at(-1)?.data.gameOver
        });
        return {};
      };
      await sfTest.run(
        `lobbyId='custom-room';stateRef='lobbies/custom-room/chess_starfront/state';isController=true;controllerLease={ownerUid:'browser-test',epoch:1};lobbyData={turncraftVersion:1,turncraftSeats:['browser-test','guest'],turncraftPlayers:{'browser-test':{name:'Host',factionId:'frontier'},guest:{name:'Guest',factionId:'brood'}}};chosenMapId='classic';chosenGameMode='annihilation';createGame([{name:'Host',factionId:'frontier',ai:false},{name:'Guest',factionId:'brood',ai:false}]);`
      );
      sfTest.run(
        'killEntity(playerStructures(1).find(isBase),playerUnits(0)[0]);'
      );
      await new Promise((resolve) => setTimeout(resolve, 0));
      await sfTest.run('stateWriteTask');
      return {
        writes: sfWrites,
        completions: sfCompletions,
        paused: sfTest.run('paused')
      };
    });
    assert.ok(terminal.writes.length >= 1);
    const snapshot = terminal.writes.at(-1).data;
    assert.equal(snapshot.gameOver, true);
    assert.equal(snapshot.matchResult.winnerIdx, 0);
    assert.equal(terminal.completions.length, 1);
    assert.equal(terminal.completions[0].persisted, true);
    assert.equal(terminal.completions[0].path, '/lobbies/custom-room/complete');
    const remote = await guest.page.evaluate((snapshot) => {
      sfTest.run(
        `PLAYER=1;lobbyId='custom-room';isController=false;lobbyData={turncraftVersion:1,turncraftSeats:['host','browser-test'],turncraftPlayers:{host:{name:'Host',factionId:'frontier'},'browser-test':{name:'Guest',factionId:'brood'}}};`
      );
      sfTest.run(`applyRemoteState(${JSON.stringify(snapshot)})`);
      document.getElementById('pauseBtn').click();
      return {
        title: document.getElementById('endTitle').textContent,
        exit: document.getElementById('endExitBtn').textContent,
        paused: sfTest.run('paused'),
        pauseDisabled: document.getElementById('pauseBtn').disabled
      };
    }, snapshot);
    assert.equal(remote.title, 'Defeat');
    assert.equal(remote.exit, 'Return to Lobby');
    assert.equal(remote.paused, false);
    assert.equal(remote.pauseDisabled, true);
    assert.deepEqual(host.errors, []);
    assert.deepEqual(guest.errors, []);
    await guest.page.route(
      '**/game/turncraft-lobby.html?lobbyId=custom-room',
      (route) =>
        route.fulfill({
          contentType: 'text/html',
          body: '<!doctype html><title>Returned to the lobby</title>'
        })
    );
    await guest.page.evaluate(() =>
      sfTest.run(
        `returnTo=location.origin+'/game/turncraft-lobby.html?lobbyId=custom-room';exitGame();`
      )
    );
    await guest.page.waitForURL(
      '**/game/turncraft-lobby.html?lobbyId=custom-room'
    );
  } finally {
    await host.browser.close();
    await guest.browser.close();
  }
});
