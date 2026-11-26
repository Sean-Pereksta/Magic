const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const {offlineLaunchUrl} = require('../desktop/offline.cjs');
const {cardScript} = require('../desktop/official.cjs');

const core = fs.readFileSync(path.join(__dirname, '../../lobby/lobby-core.html'), 'utf8');
const registry = JSON.parse(JSON.stringify(vm.runInNewContext(core.match(/const GAMES = (\[[\s\S]*?\n\s*\]);/)[1])));
const source = registry.find(game => game.key === 'spelltower');
const actions = require('../../lobby/lobby-library.js').getActions(source);
const game = {...source, id:source.key, actions, offline:{type:'static',entry:'game/spelltower.html'}};
const ui = fs.readFileSync(path.join(__dirname, '../ui/hub.js'), 'utf8');

function controls(installed, online = true, title = game) {
  const buttons = [];
  const context = vm.createContext({
    state:{online,servers:[],saveSyncGames:[]},
    installed:()=>installed, supported:()=>true, playsLocally:()=>false,
    ids:items=>items, hostLobby:()=>'',
    btn:(label,action,data)=>{buttons.push({label,action,data});return label;}
  });
  vm.runInContext(ui.match(/^const usesSeparateOnlineEdition = .+;$/m)[0], context);
  for (const name of ['onlinePlay','gameControls']) {
    const start = ui.indexOf(`function ${name}(`);
    const end = ui.indexOf('\nfunction ', start + 1);
    vm.runInContext(ui.slice(start,end),context);
  }
  return {buttons, render:()=>context.gameControls(title), onlineAction:()=>context.onlinePlay(title)};
}

test('installed Spell Tower exposes local solo and hot seat beside its online lobby', () => {
  const h = controls(true);
  h.render();
  assert.deepEqual(h.buttons.map(b=>[b.label,b.data.mode,b.data.kind]), [
    ['Solo','offline','single'], ['Hot Seat','offline','hotseat'], ['Play Online','online','lobby']
  ]);
  assert.equal(h.onlineAction().type, 'lobby', 'Hot Seat is never selected as the online action');
});

test('uninstalled Spell Tower keeps browser actions available and offers its local pack', () => {
  const h = controls(false);
  h.render();
  assert.equal(h.buttons[0].action, 'install');
  assert.deepEqual(h.buttons.filter(b=>b.action==='play').map(b=>[b.data.mode,b.data.kind]), [
    ['online','lobby'], ['online','single'], ['online','hotseat']
  ]);
});

test('MOBA Mania offers separate solo, online browser and Live Lobbies entries', () => {
  const moba = require('../content/catalog.json').games.find(item => item.id === 'mobamania');
  const h = controls(true, true, moba);
  h.render();
  assert.deepEqual(h.buttons.map(({label, action, data})=>[label, action, data.mode, data.kind]), [
    ['Play Offline', 'play', 'offline', 'single'],
    ['Online Lobbies', 'play', 'online', 'online'],
    ['Live Join', 'game-lobbies', undefined, undefined]
  ]);
  assert.equal(h.buttons.at(-1).data.id, 'mobamania');
});

for (const [action,mode] of [['single','solo'],['hotseat','hotseat']]) {
  test(`installed ${action} opens the bundled entry in ${mode} mode`, () => {
    const url = new URL(offlineLaunchUrl(game,'http://127.0.0.1:23456/game/spelltower.html?gameId=stale&username=old',action));
    assert.equal(url.origin,'http://127.0.0.1:23456');
    assert.equal(url.pathname,'/game/spelltower.html');
    assert.equal(url.search,`?mode=${mode}`);
    assert.equal(url.searchParams.has('gameId'),false);
  });
}

test('offline Spell Tower rejects an online lobby action instead of silently opening the wrong mode', () => {
  assert.throws(()=>offlineLaunchUrl(game,'http://127.0.0.1:23456/game/spelltower.html','lobby'),/Solo or Hot Seat/);
  const noHotseat = {...game,actions:[{type:'single'}]};
  assert.throws(()=>offlineLaunchUrl(noHotseat,'http://127.0.0.1:23456/game/spelltower.html','hotseat'),/Solo or Hot Seat/);
});

test('the launcher can activate the actual Hot Seat control in the website lobby', () => {
  const selectors = [];
  let clicked = 0;
  const result = vm.runInNewContext(cardScript('spelltower','hotseat'), {
    document:{querySelector:selector=>{
      selectors.push(selector);
      return {disabled:false,click:()=>clicked++};
    }}
  });
  assert.equal(result,true);
  assert.equal(clicked,1);
  assert.deepEqual(selectors,['[data-game-key="spelltower"] [data-game-action="hotseat"]']);
  assert.throws(()=>cardScript('spelltower','unsupported'),/Unknown game action/);
});

test('changing Solo, Hot Seat or online lobby cannot silently refocus the wrong open mode', () => {
  const source = fs.readFileSync(path.join(__dirname,'../desktop/windows.cjs'),'utf8');
  const start = source.indexOf('function focusGame(');
  const end = source.indexOf('\nmodule.exports=',start);
  let focused = 0;
  const row = {game,mode:'offline',action:'single',w:{show(){},focus(){focused++;}}};
  const context = vm.createContext({windows:new Map([[1,row]])});
  vm.runInContext(source.slice(start,end),context);
  assert.equal(context.focusGame('spelltower','offline','single'),true);
  assert.equal(focused,1);
  assert.throws(()=>context.focusGame('spelltower','offline','hotseat'),/Close this game window/);
  assert.throws(()=>context.focusGame('spelltower','online','lobby'),/Close this game window/);
  assert.equal(focused,1);
  row.mode='online';row.action='lobby';
  assert.throws(()=>context.focusGame('spelltower','online','hotseat'),/Close this game window/);
  assert.equal(context.focusGame('spelltower','online','lobby'),true);
  assert.equal(context.focusGame('other','offline'),false);
});

test('local routing preserves the installed origin and path even when the public route differs', () => {
  const renamed = {...game,hotseatRoute:'https://catnmice.com/game/spelltower.html?mode=hotseat&players=4'};
  assert.equal(offlineLaunchUrl(renamed,'http://127.0.0.1:23456/index.html','hotseat'),
    'http://127.0.0.1:23456/index.html?mode=hotseat&players=4');
  assert.equal(offlineLaunchUrl({id:'other'},'http://127.0.0.1:23456/index.html?existing=1'),
    'http://127.0.0.1:23456/index.html?existing=1');
});

test('the release pack recipe includes the complete Spelltower module and artwork folder', () => {
  const recipe = fs.readFileSync(path.join(__dirname,'../content/build-packs.py'),'utf8');
  assert.match(recipe,/\('spelltower','spelltower',\[\]\)/);
  const catalog = JSON.parse(fs.readFileSync(path.join(__dirname,'../content/catalog.json'),'utf8'));
  const published = catalog.games.find(row=>row.id==='spelltower');
  for (const field of ['route','singleRoute','hotseatRoute','modes','minPlayers','maxPlayers']) {
    assert.deepEqual(published[field],game[field],field);
  }
  assert.deepEqual(published.actions,actions);
});
