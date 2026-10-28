const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { chromium } = require('playwright');
const repo = path.resolve(__dirname, '../../../..');
const stub = `const initializeApp=()=>({}),getAuth=()=>({currentUser:{uid:'browser-test'}}),getFirestore=()=>({}),doc=(...parts)=>parts.slice(1).join('/'),getDoc=async ref=>{window.sfReads=window.sfReads||[];sfReads.push(ref);return {exists:()=>ref.startsWith('users/')?!!window.sfAccounts?.[ref]:true,data:()=>ref.startsWith('users/')?window.sfAccounts?.[ref]:window.sfLobby};},onAuthStateChanged=(auth,callback)=>{queueMicrotask(()=>callback(auth.currentUser));return()=>{};},signInAnonymously=async()=>{},serverTimestamp=()=>({test:true}),runTransaction=async()=>{},setDoc=async(ref,data)=>{window.sfWrites=window.sfWrites||[];sfWrites.push({ref,data:JSON.parse(JSON.stringify(data))});};`;
const expose = `window.sfTest={run:source=>eval(source),advance(seconds){for(let n=0;n<seconds*10;n++){window.sfClock+=100;updatePersistentOrders();updateCombat(.1);resolveProjectileImpacts();updateProduction(.1);updateRegeneration(.1);updateAreaEffects();window.TurncraftExpansion.tick(.1)}},clearWave(){for(const id of window.TurncraftExpansion.getState().activeIds){const e=entityById(id);if(e)killEntity(e,playerUnits(0).find(u=>!isWorker(u)))}}};window.sfClock=Date.now();nowMs=()=>window.sfClock;resize();window.sfResults=[];addEventListener('sf:result',event=>sfResults.push(event.detail));`;
const source = fs
  .readFileSync(path.join(repo, 'game/turncraft.html'), 'utf8')
  .replace(/^import .*https:\/\/www\.gstatic\.com.*;$/gm, '')
  .replace('<script type="module">', '<script type="module">' + stub)
  .replace('\nboot();', '\n' + expose + '\nwindow.TurncraftStartup?.complete();')
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
        body: 'export function rankFor(mmr,matches){return matches?(mmr>=1500?"Diamond":mmr>=1100?"Gold":"Silver"):"Unranked";} export const selectedUsername=()=>localStorage.getItem("gh.username")||null; export const rememberLobby=async()=>{}; export const saveLobbyResult=async()=>({});'
      });
    if (pathname === '/game/turncraft/shattered-front/lobby-store.mjs')
      return route.fulfill({contentType:'text/javascript',body:'export async function completeCustomMatch(context,id){return window.sfCompleteFirestore(context,id);}'});
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
      TurncraftExpansion.startCampaign('amber-wastes-4', {
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
        const objective=state.objectives.find(o=>!o.optional&&o.stage===state.operation.stage&&['discover','capture'].includes(o.type));
        if(objective)sfTest.run(`(()=>{const patrol=playerUnits(0).find(e=>!isWorker(e));patrol.x=${objective.x};patrol.y=${objective.y};})()`);
        TurncraftExpansion.tick(1);
        sfTest.clearWave();
      }
      return { ticks, result: sfResults[0], gameOver: sfTest.run('gameOver') };
    });
    assert.ok(result.ticks < 1000);
    assert.equal(result.result.won, true);
    assert.equal(result.result.completedStages.length,3);
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
      TurncraftExpansion.startSurvival('amber-wastes', { seed: 1 })
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
test('custom multiplayer commands use Firestore while guests leave simulation to the controller', async () => {
  const {browser,page,errors}=await setup();
  try {
    const requests=[];page.on('request',request=>{if(new URL(request.url()).pathname.startsWith('/api/'))requests.push(request.url());});
    const result=await page.evaluate(async()=>{
      sfTest.run(`chosenMapId='classic';chosenGameMode='annihilation';createGame([{name:'Test',factionId:'frontier',ai:false},{name:'Rival',factionId:'brood',ai:false}]);window.sfSharedState=serializeState();lobbyId='custom-room';isController=false;username='Test';lobbyData={turncraftVersion:1,ranked:false,turncraftSeats:['browser-test','rival'],turncraftPlayers:{'browser-test':{name:'Test',factionId:'frontier',mmr:1250,rankedMatches:12},rival:{name:'Rival',factionId:'brood',mmr:1600,rankedMatches:20}}};applyRemoteState(sfSharedState);`);
      const unit=sfSharedState.entities.find(e=>e.faction===0&&e.kind==='unit');
      await sfTest.run(`sendCommand({type:'move',entityId:${unit.id},x:10,y:15})`);
      const before=sfTest.run('JSON.stringify(entities)');sfTest.run('requestAnimationFrame=()=>0;frame(performance.now()+50)');
      return {writes:sfWrites,controller:sfTest.run('isController'),unchanged:before===sfTest.run('JSON.stringify(entities)'),identity:TurncraftExpansion.getIdentity()};
    });
    assert.equal(result.controller,false);assert.equal(result.unchanged,true);assert.equal(result.identity.isRanked,false);
    const command=result.writes.find(row=>row.ref.includes('chess_starfront_commands'));
    assert.ok(command,JSON.stringify(result.writes));assert.equal(command.data.type,'move');assert.equal(command.data.slot,0);assert.equal(command.data.by,'Test');
    assert.match(await page.locator('#sfMatchCommanders').textContent(),/Test[\s\S]*Gold · 1250 MMR[\s\S]*Rival[\s\S]*Diamond · 1600 MMR/);
    assert.equal(await page.locator('#sfMatchCommanders img[alt="Gold rank"]').count(),1);assert.deepEqual(requests,[]);assert.deepEqual(errors,[]);
  } finally {await browser.close();}
});
test('game client contains no HTTP matchmaking or background ranked polling',()=>{
  const html=fs.readFileSync(path.join(repo,'game/turncraft.html'),'utf8');
  assert.doesNotMatch(html,/turncraftRequest|pollRankedState|rankedPollTimer|\/api\/turncraft|\/matches\//);
  assert.match(html,/completeFirestoreMatch\(\{db,auth,api:\{doc,runTransaction\}\}/);
});

test('in-match and result badges read existing account ratings once without adding rating data to the lobby',async()=>{
 const {browser,page,errors}=await setup();try{
  await page.evaluate(()=>{
   window.sfAccounts={'users/SavedGold':{gameStats:{turncraft:{mmr:1250,rankedMatches:12}}},'users/SavedDiamond':{gameStats:{turncraft:{mmr:1625,rankedMatches:20}}}};
   sfTest.run(`chosenMapId='classic';chosenGameMode='annihilation';createGame([{name:'SavedGold',factionId:'frontier',ai:false},{name:'SavedDiamond',factionId:'brood',ai:false}]);window.sfBadgeState=serializeState();lobbyId='custom-room';isController=false;lobbyData={turncraftVersion:1,turncraftSeats:['gold','diamond'],turncraftPlayers:{gold:{name:'SavedGold',factionId:'frontier',ready:true},diamond:{name:'SavedDiamond',factionId:'brood',ready:true}}};applyRemoteState(sfBadgeState);`);
  });
  await page.waitForFunction(()=>document.querySelector('#sfMatchCommanders')?.textContent.includes('Diamond · 1625 MMR'));
  assert.match(await page.locator('#sfMatchCommanders').textContent(),/SavedGold[\s\S]*Gold · 1250 MMR[\s\S]*SavedDiamond[\s\S]*Diamond · 1625 MMR/);
  await page.evaluate(()=>sfTest.run(`sfBadgeState={...sfBadgeState,simulationTick:sfBadgeState.simulationTick+1,gameOver:true,matchResult:{winnerIdx:0,winnerName:'SavedGold',winnerFactionId:'frontier',finishedAt:Date.now(),modeId:'annihilation',reason:'Custom match complete.'}};applyRemoteState(sfBadgeState);renderPostMatchReport(presentedMatchResult);renderPostMatchReport(presentedMatchResult);`));
  assert.equal(await page.locator('#postMatchReport img[alt="Gold rank"]').count(),1);assert.equal(await page.locator('#postMatchReport img[alt="Diamond rank"]').count(),1);
  const state=await page.evaluate(()=>({reads:sfReads.filter(ref=>ref.startsWith('users/')),writes:window.sfWrites||[],lobby:sfTest.run('lobbyData.turncraftPlayers')}));assert.deepEqual(state.reads.sort(),['users/SavedDiamond','users/SavedGold']);assert.deepEqual(state.writes,[]);assert.equal(Object.hasOwn(state.lobby.gold,'mmr'),false);assert.equal(Object.hasOwn(state.lobby.diamond,'rankedMatches'),false);assert.deepEqual(errors,[]);
 }finally{await browser.close();}
});
test('custom terminal state is persisted immediately and the second client receives its defeat', async () => {
  const host = await setup(),
    guest = await setup();
  try {
    const terminal = await host.page.evaluate(async () => {
      window.sfCompletions = [];
      window.sfCompleteFirestore = async (context, id) => {
        sfCompletions.push({
          id,
          uid:context.auth.currentUser.uid,
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
    assert.equal(terminal.completions[0].id, 'custom-room');
    assert.equal(terminal.completions[0].uid, 'browser-test');
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
