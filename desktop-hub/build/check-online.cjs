// Usage: node build/check-online.cjs <installed-exe> <new-test-profile-directory>
// Add --dev when <installed-exe> is the development Electron executable.
const fs = require('node:fs');
const path = require('node:path');
const { setTimeout: delay } = require('node:timers/promises');
const { isDeepStrictEqual } = require('node:util');
const { _electron } = require('playwright-core');
const JSON5 = require('json5');

const [exe, profileDir, ...flags] = process.argv.slice(2);
if (!exe || !profileDir || flags.some(flag => flag !== '--dev')) {
  throw Error('Usage: node build/check-online.cjs <exe> <new-test-profile-directory> [--dev]');
}
const profile = path.resolve(profileDir);
if (fs.existsSync(profile)) throw Error('Choose a new directory for this isolated guest profile.');
fs.mkdirSync(profile, {recursive:true});

const official = 'https://www.catnmice.com';
const localGames = ['bannerlands','lastsignal','trashtactics','cat'];
const report = {titles:0, actions:0, routes:[], representative:[], localControls:[], errors:[]};
const contractFields = [
  'route','singleRoute','hotseatRoute','multiplayerRoute','dmRoute','downloadRoute','localEditionRoute',
  'modes','requiresUser','autoUsername','autoRedirect','launchMode','realmId','liveLobbyGame',
  'minPlayers','maxPlayers','joinLabel','hotseatLabel','multiTitle'
];
const routeFields = ['route','singleRoute','hotseatRoute','multiplayerRoute','dmRoute','localEditionRoute','downloadRoute'];

async function waitFor(probe, description, timeout = 30000) {
  const end = Date.now()+timeout;
  while (Date.now()<end) {
    const value = await probe();
    if (value) return value;
    await delay(200);
  }
  throw Error('Timed out waiting for '+description+'.');
}

// Read the remote renderer through Electron. Each probe runs in the actual
// sandboxed game view, which does not expose the desktop integration bridge.
async function readViews(app, probe) {
  return app.evaluate(async ({webContents}, code) => {
    const views = webContents.getAllWebContents().filter(w => w.getURL().startsWith('https://'));
    return Promise.all(views.map(async w => ({
      url:w.getURL(),
      value:await w.executeJavaScript(code).catch(() => null)
    })));
  }, '('+probe.toString()+')()');
}
async function closeGames(app) {
  await app.evaluate(({BrowserWindow}) => {
    for (const win of BrowserWindow.getAllWindows()) {
      if (win.webContents.getURL().includes('toolbar.html')) win.close();
    }
  });
}
function openLibraryView() {
  if (location.pathname!=='/lobby/lobby.html' || !document.querySelector('[data-game-key]')) return null;
  const library = document.querySelector('[data-view="singleplayer"]');
  if (!library) return null;
  library.click();
  const source = [...document.scripts].map(s => s.textContent).find(s => /const GAMES\s*=/.test(s));
  const rows = source?.match(/const GAMES\s*=\s*(\[[\s\S]*?\n\s*\]);/)?.[1];
  if (!rows) return null;
  const controls = [...document.querySelectorAll('[data-game-key]')].map(el => ({
    id:el.dataset.gameKey,
    actions:[...el.querySelectorAll('[data-game-action],[data-live-game]')].map(a => a.dataset.gameAction || 'live')
  }));
  return {rows,controls};
}
function checkMatrix(games, model) {
  // Registry data is parsed as literals. Website code never runs in Node.
  const live = new Map(JSON5.parse(model.rows).map(row => [row.key,row]));
  report.titles=games.length;
  report.actions=games.reduce((n,g) => n+g.actions.length,0);
  for (const g of games) {
    const actual=live.get(g.id);
    if (!actual) {report.errors.push(g.id+' is missing from the live game registry.');continue;}
    for (const field of contractFields) {
      // The native Last Signal launch ignores website routes. Pages can serve
      // the previous lobby registry while the installer build is in progress.
      if (g.id==='lastsignal' && ['route','multiplayerRoute','localEditionRoute','joinLabel'].includes(field)) continue;
      if (!isDeepStrictEqual(actual[field],g[field])) report.errors.push(g.id+' differs from the live registry for '+field+'.');
    }
    for (const action of g.actions) {
      if (!model.controls.some(el => el.id===g.id && el.actions.includes(action.type))) {
        report.errors.push(g.id+' has no live '+action.type+' control.');
      }
    }
    live.delete(g.id);
  }
  for (const id of live.keys()) report.errors.push('Published title missing from desktop catalog: '+id);
}
async function checkRoutes(games) {
  const targets=new Map();
  for (const game of games) {
    for (const key of routeFields) {
      if (!game[key]) continue;
      const url = new URL(game[key],official);
      if (!['https://www.catnmice.com','https://catnmice.com','https://sean-pereksta.github.io'].includes(url.origin)) {
        report.errors.push(game.id+' has an unrecognized route origin: '+url.origin);continue;
      }
      const uses=targets.get(url.href) || [];
      uses.push(game.id+'.'+key);targets.set(url.href,uses);
    }
  }
  const queue=[...targets];
  async function checkNext() {
    while (queue.length) {
      const [url,uses]=queue.shift();
      let status,reason;
      try {
        const res=await fetch(url,{method:'HEAD',signal:AbortSignal.timeout(20000)});
        status=res.status;
        if (!res.ok) reason='HTTP '+res.status;
      } catch(error) {reason=error.cause?.code || error.message;}
      report.routes.push({url,uses,status,reason});
      if (reason) report.errors.push(uses.join(', ')+' route check failed: '+reason);
    }
  }
  await Promise.all(Array.from({length:5},checkNext));
}
function checkScreen() {
  const visible = el => el && el.getClientRects().length && getComputedStyle(el).visibility!=='hidden';
  const selected = document.querySelector('[data-live-filter="catandmouse"][aria-pressed="true"]');
  const catSection=document.querySelector('[data-live-section="catandmouse"]');
  return {
    playable:Boolean(visible(document.querySelector('#btnPlay')) && visible(document.querySelector('#target')) &&
      document.querySelector('#hand .card')),
    catDirectorySelected:Boolean(selected && visible(catSection)),
    directoryStatus:catSection?.querySelector('[data-live-status]')?.textContent,
    gated:Boolean(visible(document.querySelector('#accountGateModal'))),
    login:Boolean(visible(document.querySelector('#accountGateModal')) && document.querySelector('#accountGateModal')?.innerText.includes('Log In'))
  };
}
async function checkGuest(app, lib, id, action, accepts, invocation = ['play',{id,action,mode:'online'}]) {
  try {
    await lib.evaluate(([method,data]) => window.hub.invoke(method,data),invocation);
    const actual=await waitFor(async () => (await readViews(app,checkScreen)).find(row => row.value && accepts(row)), id+' '+action+' destination');
    report.representative.push({id,action,url:actual.url,directoryStatus:id==='cat'?actual.value.directoryStatus:undefined});
  } catch(error) {report.errors.push(id+' '+action+': '+error.message);}
  finally {await closeGames(app);}
}
async function checkLocalControls(lib, state) {
  const controls = locator => locator.evaluateAll(buttons => buttons.map(button => [
    button.textContent.trim(),button.dataset.mode,button.dataset.id
  ]));
  for (const id of localGames) {
    if (!state.installed.includes(id)) throw Error('Setup did not install the local '+id+' game.');
    await lib.locator('.art-button[data-id="'+id+'"]').click();
    const detail = await controls(lib.locator('.hero-body [data-action="play"]'));
    const expected = [['Play','offline',id],['Multiplayer','online',id],['Host Lobby','host',id]];
    if (!isDeepStrictEqual(detail,expected)) throw Error(id+' game details do not default to local play and hosting.');
    if (await lib.locator('.hero-body [data-action="server-manager"][data-id="'+id+'"]').count() !== 1) {
      throw Error(id+' details are missing the managed server shortcut.');
    }
    await lib.locator('.back-line[data-go="Library"]').click();
  }
  await lib.locator('.nav-link[data-go="Live Lobbies"]').click();
  for (const id of localGames) {
    const lobby = await controls(lib.locator('[data-action="play"][data-id="'+id+'"]'));
    const expected = [['Multiplayer','online',id],['Host Lobby','host',id]];
    if (!isDeepStrictEqual(lobby,expected)) throw Error(id+' live lobby controls should open the local game.');
    report.localControls.push(id);
  }
  await lib.locator('.nav-link[data-go="Server Manager"]').click();
  for (const id of localGames) {
    const panel = lib.locator('#server-'+id);
    if (await panel.locator('[data-server-status="offline"]').count() !== 1 ||
        await panel.locator('button[type="submit"]').count() !== 1) {
      throw Error(id+' managed hosting pack is unavailable in the installed launcher.');
    }
  }
  await lib.locator('.nav-link[data-go="Library"]').click();
}
async function main() {
  const args = flags.includes('--dev') ? [path.resolve(__dirname,'..')] : [];
  const app=await _electron.launch({
    executablePath:path.resolve(exe),args:[...args,'--user-data-dir='+profile,'--disable-background-networking'],
    env:{...process.env,CATNMICE_TEST_DATA:profile},timeout:45000
  });
  try {
    const lib=await app.firstWindow();
    await lib.waitForSelector('.game-card',{timeout:45000});
    const state=await lib.evaluate(() => window.hub.invoke('state'));
    if (!state.profile.guest) throw Error('The isolated account must be a guest.');
    await checkLocalControls(lib,state);
    await lib.evaluate(() => window.hub.invoke('community',{section:'lobbies'}));
    const model=await waitFor(async () => (await readViews(app,openLibraryView)).find(row => row.value?.controls.length)?.value,'published game registry');
    checkMatrix(state.catalog.games,model);
    await closeGames(app);
    await checkRoutes(state.catalog.games);
    await checkGuest(app,lib,'balatlesolo','single',row => row.url.startsWith(official+'/game/balatlesolo.html') && row.value.playable);
    await checkGuest(app,lib,'cat','live',row => row.url.startsWith(official+'/lobby/lobby.html') && row.value.catDirectorySelected,['community',{section:'lobbies',id:'cat'}]);
    await checkGuest(app,lib,'village','single',row => row.url.startsWith(official+'/lobby/lobby.html') && row.value.gated && row.value.login);
  } finally {await app.close();}
}
main().catch(error => report.errors.push(error.message)).finally(() => {
  const output=path.join(profile,'online-release-check.json');
  fs.writeFileSync(output,JSON.stringify(report,null,2)+'\n');
  console.log(JSON.stringify({titles:report.titles,actions:report.actions,routes:report.routes.length,representative:report.representative,localControls:report.localControls,errors:report.errors,report:output},null,2));
  if (report.errors.length) process.exitCode=1;
});
