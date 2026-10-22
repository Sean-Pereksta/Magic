const {test} = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const {chromium} = require('playwright');

const repo = path.resolve(__dirname, '../../..');
const png = Buffer.from('iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+aD1sAAAAASUVORK5CYII=', 'base64');
const mapPath = '/game/turncraft/assets/maps/';
const builtInMaps = ['ember-rift','frostline-basin','verdant-divide','shattered-dunes','orbital-foundry'];
const multiplayerQuery = '?gameId=shared-match&turncraftLobby=1&username=Guest%20Commander';
const missingGraphic = {
  url:'https://pub-cb55798f1eeb4f80a85463da3612ed61.r2.dev/units/missing-test.webp',
  stage:'image-load',status:404,reason:'The requested graphic was not found on the asset host.'
};

// Keep the actual startup, network routing, expansion engine, and menu modules.
// Only the external services and asset completion timing are controlled here.
const firebase = `
const initializeApp=()=>({}),getFirestore=()=>({});
const authFixture={currentUser:{uid:'test-user'}};
const getAuth=()=>authFixture;
const signInAnonymously=async()=>{window.firebaseCalls.push({type:'signIn'});return{user:authFixture.currentUser}};
const onAuthStateChanged=(auth,callback)=>{window.firebaseCalls.push({type:'auth'});queueMicrotask(()=>callback(auth.currentUser));return()=>{}};
const doc=(db,...parts)=>({path:parts.join('/')});
const collection=(db,...parts)=>({path:parts.join('/'),collection:true});
const serverTimestamp=()=>0;
const fixtureSnapshot=ref=>{
  if(ref.collection)return{docs:[],docChanges:()=>[]};
  const state=ref.path.endsWith('/chess_starfront/state');
  return{exists:()=>true,data:()=>state?window.tcStartupTest.sharedState():window.lobbyFixture};
};
const getDoc=async ref=>{window.firebaseCalls.push({type:'get',path:ref.path});if(window.failLobby&&/^lobbies\\/[^/]+$/.test(ref.path))throw new Error('The shared lobby is unavailable.');return fixtureSnapshot(ref)};
const setDoc=async(ref,data)=>{window.firebaseCalls.push({type:'set',path:ref.path})};
const deleteDoc=async ref=>{window.firebaseCalls.push({type:'delete',path:ref.path})};
const onSnapshot=(ref,callback)=>{window.firebaseCalls.push({type:'watch',path:ref.path});queueMicrotask(()=>callback(fixtureSnapshot(ref)));return()=>{}};
const runTransaction=async()=>{window.firebaseCalls.push({type:'transaction'})};
`;
const inspection = `
window.tcStartupTest={
  get state(){return{gameStarted,player:PLAYER,lobbyId,simulationTick,chosenMapId,chosenGameMode,players:players.map(p=>({name:p.name,factionId:p.factionId,minerals:p.minerals}))}},
  probeCampaignAtlases(){return Promise.all(CATALOG.regions.map(region=>loadMapAtlas(region.mapId)))},
  sharedState(){return{
    gameKey:NET_KEY,version:42,simulationTick:71,controllerEpoch:9,
    mapSeed:12345,activeMapSeed:12345,chosenMapId:'frostline-basin',chosenGameMode:'relic',
    chosenDifficulty:'medium',chosenAiStyle:'balanced',terrainCode:'g'.repeat(BOARD_W*BOARD_H),
    players:[{...makePlayer(0,'Host Commander','frontier'),minerals:850},{...makePlayer(1,'Guest Commander','ascendant'),minerals:735}],
    entities:[],resourceNodes:[],areaEffects:[],pendingImpacts:[],temporaryObstacles:[],scanEffects:[],
    modeState:{mode:'relic',scores:{0:1,1:2},relic:{x:21,y:14,carrierId:null,respawnAt:0}},gameOver:false
  }}
};
`;
const preloadMock = `
window.preloadAttempts=[];
window.TurncraftPreload={
  ...window.TurncraftPreload,
  LEGACY_FILES:['ground.png','cliff.png','mineral.png','gas.png','larva.png','cocoon.png'],
  preload(options={}){return new Promise((resolve,reject)=>{
    if(options.allowFailures!==true)throw new Error('Startup must attempt graphics with allowFailures enabled.');
    const attempt={
      progress(completed,total=4){options.onProgress?.({phase:'images',completed,attempted:completed,total,failed:0,percent:Math.round(completed/total*100),ready:false})},
      resolve(failures=[]){const result={total:4,completed:4-failures.length,attempted:4,failed:failures.length,failures,urls:[],visualManifest:{images:{}},expansionManifest:{assets:{}},animationManifest:{assets:{}}};options.onProgress?.({phase:'complete',...result,percent:100,ready:true});resolve(result)},
      reject(){const error=new Error('1 Turncraft graphic could not be loaded. Check your connection and retry.');options.onProgress?.({phase:'error',completed:3,total:4,failed:1,percent:75,ready:false,error});reject(error)}
    };
    window.preloadAttempts.push(attempt);attempt.progress(0);
  })}
};
`;
const profileMock = `
export function rankFor(){return 'Unranked'}
export function watchProfile(callback){callback({identity:{uid:'test-user',displayName:'Test Commander'},profile:{}});return()=>{}}
export async function saveLobbyResult(){return {}}
export async function rememberLobby(){}
export function selectedUsername(){return localStorage.getItem('gh.username')}
`;

function source() {
  const html = fs.readFileSync(path.join(repo, 'game/turncraft.html'), 'utf8');
  assert.ok(html.includes('\nboot();'), 'The actual main boot call must remain in this regression fixture.');
  return html
    .replace(/^import .* from "https:\/\/www\.gstatic\.com\/firebasejs\/.*;$/gm, '')
    .replace('<script type="module">', `<script type="module">${firebase}`)
    .replace('\nboot();', `\n${inspection}\nboot();`);
}

async function setup({query = '', failLobby = false, width = 1280, height = 800, imageFailures = [], delayDiagnostics = []} = {}) {
  const browser = await chromium.launch({headless:true, executablePath:process.env.CHROMIUM_EXECUTABLE_PATH || undefined, args:['--no-sandbox']});
  const page = await browser.newPage({viewport:{width,height}});
  const errors = [], consoleErrors = [], imageRequests = [], pendingDiagnostics = [];
  page.on('pageerror', error => errors.push(error.message));
  page.on('console', message => { if (message.type() === 'error') consoleErrors.push(message.text()); });
  await page.addInitScript(({failLobby,decodeFailures}) => {
    window.firebaseCalls = [];
    window.failLobby = failLobby;
    window.rejectDecodeUrls = decodeFailures;
    window.decodeCalls = [];
    const decode = HTMLImageElement.prototype.decode;
    HTMLImageElement.prototype.decode = function(...args) {
      const pathname = new URL(this.src, location.href).pathname;
      window.decodeCalls.push(pathname);
      if (window.rejectDecodeUrls.includes(pathname)) return Promise.reject(new Error('The test image cannot be decoded.'));
      return decode.apply(this,args);
    };
    window.lobbyFixture = {
      turncraftVersion:1,status:'started',host:'Host Commander',
      members:{'host-user':'Host Commander','test-user':'Guest Commander'},
      turncraftSeats:['host-user','test-user'],
      turncraftPlayers:{'host-user':{name:'Host Commander',factionId:'frontier'},'test-user':{name:'Guest Commander',factionId:'ascendant'}},
      settings:{mapId:'frostline-basin',gameMode:'relic'},
      chessStarfrontController:{ownerUid:'host-user',ownerName:'Host Commander',epoch:9,expiresAtMs:Date.now()+60000}
    };
  }, {failLobby,decodeFailures:imageFailures.filter(failure => failure.kind === 'decode').map(failure => failure.pathname)});
  const html = source();
  const preloader = fs.readFileSync(path.join(repo,'game/turncraft/preload.js'),'utf8');
  await page.route('**/*', async route => {
    const url = new URL(route.request().url());
    if (url.pathname === '/game/turncraft.html') return route.fulfill({contentType:'text/html',body:html});
    if (url.pathname.endsWith('/turncraft/preload.js')) return route.fulfill({contentType:'text/javascript',body:`${preloader}\n${preloadMock}`});
    if (url.pathname.endsWith('/shattered-front/profile.mjs')) return route.fulfill({contentType:'text/javascript',body:profileMock});
    if (url.pathname.endsWith('/shattered-front/lobby-store.mjs')) return route.fulfill({contentType:'text/javascript',body:'export async function completeCustomMatch(){}'});
    if (/\.(png|webp|jpe?g|gif)(?:$|\?)/i.test(url.pathname)) {
      imageRequests.push(url.href);
      // Expansion regions deliberately have no local map illustrations. A broad
      // successful-image stub would hide the original campaign-atlas regression.
      const syntheticMap = url.pathname.startsWith(mapPath) && !builtInMaps.some(id => url.pathname === `${mapPath}${id}.webp`);
      const failedDownload = imageFailures.some(failure => failure.active !== false && failure.kind === '404' && url.pathname === failure.pathname);
      if(route.request().resourceType()==='fetch'&&delayDiagnostics.includes(url.pathname))await new Promise(resolve=>{const timer=setTimeout(resolve,10000);pendingDiagnostics.push(()=>{clearTimeout(timer);resolve()})});
      if (syntheticMap || failedDownload) return route.fulfill({status:404,body:'This illustration does not exist.',headers:{'Access-Control-Allow-Origin':'*'}});
      return route.fulfill({contentType:'image/png',body:png,headers:{'Access-Control-Allow-Origin':'*'}});
    }
    const file = path.resolve(repo, '.' + decodeURIComponent(url.pathname));
    if (url.hostname === 'turncraft.test' && file.startsWith(repo + path.sep) && fs.existsSync(file) && fs.statSync(file).isFile()) return route.fulfill({path:file});
    return route.fulfill({status:404,body:''});
  });
  try {
    await page.goto(`http://turncraft.test/game/turncraft.html${query}`);
    await page.waitForFunction(() => window.preloadAttempts?.length === 1, null, {timeout:10000});
    return {browser,page,errors,imageRequests,releaseDiagnostics:()=>pendingDiagnostics.splice(0).forEach(resolve=>resolve()),recoverImages:async()=>{imageFailures.forEach(failure=>{failure.active=false});await page.evaluate(()=>{window.rejectDecodeUrls=[]})}};
  } catch (error) {
    await browser.close();
    throw new Error(`${error.message}\nPage errors: ${JSON.stringify(errors)}\nConsole errors: ${JSON.stringify(consoleErrors)}`, {cause:error});
  }
}

async function assertBootBlocked(page) {
  assert.equal(await page.locator('#loadingOverlay').isVisible(), true);
  assert.equal(await page.locator('#loadingCard').isVisible(), true);
  assert.equal(await page.locator('#overlay').isVisible(), false);
  assert.equal(await page.locator('#sfMenu').isVisible(), false);
  assert.equal(await page.evaluate(() => document.documentElement.classList.contains('tc-booting')), true);
  assert.equal(await page.evaluate(() => tcStartupTest.state.gameStarted), false);
  assert.deepEqual(await page.evaluate(() => firebaseCalls), []);
}

function assertOnlyBuiltInMapAtlases(imageRequests) {
  const requested = [...new Set(imageRequests.map(value=>new URL(value).pathname).filter(value=>value.startsWith(mapPath)))].sort();
  assert.deepEqual(requested,builtInMaps.map(id=>`${mapPath}${id}.webp`).sort());
}

async function waitOpened(page,multiplayer = false) {
  await page.locator('#loadingOverlay').waitFor({state:'hidden'});
  if(multiplayer)await page.waitForFunction(()=>window.tcStartupTest.state.gameStarted);
  else await page.getByRole('heading',{name:'TURNCRAFT',exact:true}).waitFor();
  assert.equal(await page.locator('#loadingRetry').isVisible(),false);
}

async function retryGraphics(page) {
  if(!await page.locator('#graphicsWarnings').evaluate(node=>node.open))await page.locator('#graphicsWarnings summary').click();
  await page.getByRole('button',{name:'Retry missing graphics',exact:true}).click();
  await page.waitForFunction(()=>preloadAttempts.length===2);
  assert.equal(await page.locator('#loadingOverlay').isVisible(),false);
  await page.evaluate(()=>preloadAttempts[1].resolve());
  await page.locator('#graphicsWarnings').waitFor({state:'hidden'});
}

test('solo opening shows loading progress, then opens the command center when every graphic is ready', async () => {
  for (const [width,height] of [[1280,800],[390,844]]) {
    const query = width === 390 ? '?mode=solo&gameId=stale-match&lobbyId=stale-lobby' : '';
    const {browser,page,errors,imageRequests} = await setup({width,height,query});
    try {
      await assertBootBlocked(page);
      const card = await page.locator('#loadingCard').boundingBox();
      assert.ok(card.x >= 0 && card.y >= 0 && card.x + card.width <= width + 1 && card.y + card.height <= height + 1);
      await page.evaluate(() => preloadAttempts[0].progress(2));
      assert.match(await page.locator('#loadingCount').textContent(), /2\s*\/\s*4/);
      assert.equal(await page.locator('#loadingFill').evaluate(node => node.style.width), '50%');
      await assertBootBlocked(page);
      await page.evaluate(() => preloadAttempts[0].resolve());
      await page.locator('#loadingOverlay').waitFor({state:'hidden'});
      await page.getByRole('heading',{name:'TURNCRAFT',exact:true}).waitFor();
      assert.equal(await page.locator('#sfMenu').isVisible(), true);
      assert.equal(await page.evaluate(() => tcStartupTest.state.gameStarted), false);
      assert.equal(await page.evaluate(() => tcStartupTest.state.lobbyId), '');
      assert.equal(await page.evaluate(() => document.documentElement.classList.contains('tc-booting')), false);
      assert.deepEqual(await page.evaluate(() => firebaseCalls.filter(call => ['get','watch','set'].includes(call.type))), []);
      assert.deepEqual(await page.evaluate(()=>tcStartupTest.probeCampaignAtlases()),[false,false,false,false,false,false]);
      assertOnlyBuiltInMapAtlases(imageRequests);
      assert.deepEqual(await page.evaluate(()=>TurncraftGraphicsReport.failures),[]);
      assert.deepEqual(errors, []);
    } finally { await browser.close(); }
  }
});

test('multiplayer waits for the graphics before connecting, then joins the existing shared battlefield', async () => {
  const {browser,page,errors,imageRequests} = await setup({query:multiplayerQuery});
  try {
    await assertBootBlocked(page);
    await page.evaluate(() => preloadAttempts[0].resolve());
    await page.locator('#loadingOverlay').waitFor({state:'hidden'});
    await page.waitForFunction(() => window.tcStartupTest.state.gameStarted);
    assert.deepEqual(await page.evaluate(() => tcStartupTest.state), {
      gameStarted:true,player:1,lobbyId:'shared-match',simulationTick:71,
      chosenMapId:'frostline-basin',chosenGameMode:'relic',
      players:[{name:'Host Commander',factionId:'frontier',minerals:850},{name:'Guest Commander',factionId:'ascendant',minerals:735}]
    });
    assert.equal(await page.locator('#overlay').isVisible(), false);
    assert.equal(await page.locator('#sfMenu').isVisible(), false);
    assert.equal(await page.locator('#game').isVisible(), true);
    const calls = await page.evaluate(() => firebaseCalls);
    assert.ok(calls.some(call => call.type === 'get' && call.path === 'lobbies/shared-match'));
    assert.ok(calls.some(call => call.type === 'get' && call.path === 'lobbies/shared-match/chess_starfront/state'));
    assert.ok(calls.some(call => call.type === 'watch' && call.path === 'lobbies/shared-match/chess_starfront/state'));
    assert.equal(calls.some(call => call.type === 'set' && call.path.endsWith('/chess_starfront/state')), false);
    assertOnlyBuiltInMapAtlases(imageRequests);
    assert.deepEqual(errors, []);
  } finally { await browser.close(); }
});

test('missing graphics open solo and multiplayer, explain the failure, and retry without restarting', async () => {
  for(const multiplayer of [false,true]) {
    const pathname=new URL(missingGraphic.url).pathname;
    const {browser,page,errors,imageRequests,releaseDiagnostics} = await setup({query:multiplayer?multiplayerQuery:'',imageFailures:multiplayer?[{pathname,kind:'404'}]:[],delayDiagnostics:multiplayer?[pathname]:[]});
    try {
      await assertBootBlocked(page);
      const failure={...missingGraphic};
      if(multiplayer)delete failure.status;
      const probeStarted=multiplayer?page.waitForRequest(request=>request.url()===missingGraphic.url&&request.resourceType()==='fetch'):null;
      await page.evaluate(failure=>preloadAttempts[0].resolve([failure]),failure);
      if(probeStarted)await probeStarted;
      await waitOpened(page,multiplayer);
      assert.equal(await page.locator('#graphicsWarnings').isVisible(),true);
      if(!multiplayer)assert.match(await page.locator('#graphicsWarnings').textContent(),/HTTP 404/);
      assert.match(await page.locator('#graphicsWarnings').textContent(),/not found on the asset host/);
      assert.equal(await page.locator('#graphicsWarnings a').getAttribute('href'),missingGraphic.url);
      const report = await page.evaluate(()=>TurncraftGraphicsReport);
      assert.equal(report.total,4);assert.equal(report.loaded,3);assert.equal(report.attempted,4);
      assert.deepEqual(report.failures,[failure]);
      const before = await page.evaluate(()=>{window.readyBeforeGraphicsRetry=TurncraftStartup.ready;return{state:tcStartupTest.state,network:firebaseCalls.filter(call=>['get','watch'].includes(call.type))}});
      await retryGraphics(page);
      assert.deepEqual(await page.evaluate(()=>({state:tcStartupTest.state,network:firebaseCalls.filter(call=>['get','watch'].includes(call.type))})),before);
      assert.equal(await page.evaluate(()=>TurncraftStartup.ready===readyBeforeGraphicsRetry),true);
      assert.equal(await page.evaluate(()=>TurncraftStartup.isReady),true);
      assert.deepEqual(await page.evaluate(()=>TurncraftGraphicsReport.failures),[]);
      if(multiplayer){
        await page.evaluate(()=>{window.reportAfterGraphicsRecovery=TurncraftGraphicsReport});
        const probeResponse=page.waitForResponse(response=>response.url()===missingGraphic.url);
        releaseDiagnostics();
        const response=await probeResponse;
        assert.equal(response.status(),404);
        await page.evaluate(()=>new Promise(resolve=>requestAnimationFrame(()=>requestAnimationFrame(resolve))));
        assert.equal(await page.evaluate(()=>TurncraftGraphicsReport===reportAfterGraphicsRecovery),true,'A diagnostic response from the old report must not replace the recovered report.');
        assert.equal(await page.locator('#graphicsWarnings').isVisible(),false,'A late failure probe must not reopen cleared warnings.');
      }
      assertOnlyBuiltInMapAtlases(imageRequests);
      assert.deepEqual(errors,[]);
    } finally { releaseDiagnostics();await browser.close(); }
  }
});

test('an unexpected preload exception still opens available graphics and records a recoverable reason', async () => {
  const {browser,page,errors} = await setup();
  try {
    await page.evaluate(()=>preloadAttempts[0].reject());
    await waitOpened(page);
    assert.equal(await page.locator('#graphicsWarnings').isVisible(),true);
    assert.match(await page.locator('#graphicsWarnings').textContent(),/1 Turncraft graphic could not be loaded/);
    assert.equal(await page.evaluate(()=>TurncraftGraphicsReport.failures[0].stage),'preload');
    await retryGraphics(page);
    assert.deepEqual(errors,[]);
  } finally { await browser.close(); }
});

test('a real built-in map download or decode failure opens the menu and can recover in place', async () => {
  for(const kind of ['404','decode']) {
    const pathname = `${mapPath}ember-rift.webp`;
    const {browser,page,errors,imageRequests,recoverImages} = await setup({imageFailures:[{pathname,kind}]});
    try {
      await page.evaluate(()=>preloadAttempts[0].resolve());
      await waitOpened(page);
      if(kind==='404'){
        await page.waitForFunction(pathname=>TurncraftGraphicsReport.failures.some(failure=>new URL(failure.url).pathname===pathname&&failure.status===404),pathname);
        await page.locator('#graphicsWarnings summary').click();
        assert.match(await page.locator('#graphicsWarnings').textContent(),/Diagnostic GET returned HTTP 404/);
        assert.match(await page.locator('#graphicsWarnings').textContent(),/image was not found at this URL/);
        assert.equal(await page.getByRole('heading',{name:'TURNCRAFT',exact:true}).isVisible(),true);
      }
      const failures = await page.evaluate(()=>TurncraftGraphicsReport.failures);
      assert.ok(failures.some(failure=>new URL(failure.url).pathname===pathname&&failure.stage===(kind==='404'?'download':'decode')),JSON.stringify(failures));
      if(kind==='404')assert.ok(failures.some(failure=>new URL(failure.url).pathname===pathname&&failure.status===404&&/Diagnostic GET returned HTTP 404/.test(failure.probeReason)),JSON.stringify(failures));
      assert.equal(await page.locator('#graphicsWarnings').isVisible(),true);
      assert.equal(await page.evaluate(()=>tcStartupTest.state.gameStarted),false);
      const attempts = imageRequests.filter(value=>new URL(value).pathname===pathname).length;
      const decodes = await page.evaluate(pathname=>decodeCalls.filter(value=>value===pathname).length,pathname);
      await recoverImages();
      await retryGraphics(page);
      if(kind==='404')assert.ok(imageRequests.filter(value=>new URL(value).pathname===pathname).length>attempts,'Retry must request the failed map illustration again.');
      else assert.ok(await page.evaluate(pathname=>decodeCalls.filter(value=>value===pathname).length,pathname)>decodes,'Retry must decode the failed map illustration again, including when its downloaded bytes remain cached.');
      assertOnlyBuiltInMapAtlases(imageRequests);
      assert.deepEqual(errors,[]);
    } finally { await browser.close(); }
  }
});

test('a real Cloudflare terrain 404 opens multiplayer with fallbacks and retries without losing shared state', async () => {
  const pathname = '/terrain/frostline-basin.terrain.webp';
  const {browser,page,errors,imageRequests,recoverImages} = await setup({query:multiplayerQuery,imageFailures:[{pathname,kind:'404'}]});
  try {
    await page.evaluate(()=>preloadAttempts[0].resolve());
    await waitOpened(page,true);
    const failures = await page.evaluate(()=>TurncraftGraphicsReport.failures);
    assert.ok(failures.some(failure=>new URL(failure.url).pathname===pathname&&failure.stage==='download'),JSON.stringify(failures));
    assert.equal(await page.locator('#graphicsWarnings').isVisible(),true);
    const before = await page.evaluate(()=>tcStartupTest.state);
    const attempts = imageRequests.filter(value=>new URL(value).pathname===pathname).length;
    await recoverImages();
    await retryGraphics(page);
    assert.ok(imageRequests.filter(value=>new URL(value).pathname===pathname).length>attempts,'Retry must request the failed Cloudflare terrain again.');
    assert.deepEqual(await page.evaluate(()=>tcStartupTest.state),before);
    assertOnlyBuiltInMapAtlases(imageRequests);
    assert.deepEqual(errors,[]);
  } finally { await browser.close(); }
});

test('an unavailable multiplayer lobby keeps the loading screen open and can retry the same match', async () => {
  const {browser,page,errors} = await setup({query:multiplayerQuery,failLobby:true});
  try {
    await page.evaluate(() => preloadAttempts[0].resolve());
    await page.locator('#loadingRetry').waitFor({state:'visible'});
    assert.equal(await page.locator('#loadingOverlay').isVisible(), true);
    assert.equal(await page.locator('#overlay').isVisible(), false);
    assert.equal(await page.locator('#sfMenu').isVisible(), false);
    assert.equal(await page.evaluate(() => tcStartupTest.state.gameStarted), false);
    await page.evaluate(() => { window.failLobby = false; });
    await page.locator('#loadingRetry').click();
    await page.waitForFunction(() => preloadAttempts.length === 2);
    await page.evaluate(() => preloadAttempts[1].resolve());
    await page.locator('#loadingOverlay').waitFor({state:'hidden'});
    assert.equal(await page.evaluate(() => tcStartupTest.state.lobbyId), 'shared-match');
    assert.equal(await page.evaluate(() => tcStartupTest.state.gameStarted), true);
    assert.deepEqual(errors, []);
  } finally { await browser.close(); }
});
