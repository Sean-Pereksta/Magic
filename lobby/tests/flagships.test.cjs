const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const layout = require('../lobby-flagships.js');
const root = path.join(__dirname, '..');
const read = name => fs.readFileSync(path.join(root, name), 'utf8');
const ids = text => [...text.matchAll(/\bid="([^"]+)"/g)].map(match => match[1]);
const registry = text => new vm.Script('('+text.match(/const GAMES = (\[[\s\S]*?\n\s*\]);/)[1]+')').runInNewContext();

for (const view of ['discover','singleplayer','multiplayer','profile']) {
  test(`restores the existing ${view} navigation`, () => assert.equal(layout.initialView(view,false),view));
}
test('first visits and invalid saved tabs open Discover', () => {
  for (const value of [null,'','garbage','__proto__']) assert.equal(layout.initialView(value,false),'discover');
});
test('invitation links take precedence without automatically joining', () => {
  assert.equal(layout.initialView('discover',true),'multiplayer');
});
test('four exact game keys, stable editorial order, missing games omitted', () => {
  const games = [{key:'cat'},{key:'other'},{key:'ironthrone'},{key:'apestogetherstrong'},{key:'warrealms'}];
  assert.deepEqual(layout.selectPicks(games).map(p=>p.key),['apestogetherstrong','warrealms','cat','ironthrone']);
  assert.deepEqual(layout.selectPicks([{key:'other'}]),[]);
});
test('recent games retain order, deduplicate and exclude stale saved keys', () => {
  const games = ['a','b','c','d','e'].map(key=>({key}));
  assert.deepEqual(layout.recentKeys(['old',{key:'a'},'a',null,'b','c','d','e'],games),['a','b','c','d']);
  assert.deepEqual(layout.recentKeys(null,games),[]);
});
test('layout input validation', () => assert.throws(()=>layout.upgradeHtml(null),TypeError));

test('real core: preserve every registry field, control ID and entry action', () => {
  const library = require('../lobby-library.js');
  const core = library.upgradeHtml(read('lobby-core.html'));
  const upgraded = layout.upgradeHtml(core);
  for (const id of ids(core)) assert.ok(ids(upgraded).includes(id),`missing control ${id}`);
  assert.equal(new Set(ids(upgraded)).size,ids(upgraded).length,'duplicate control IDs');
  assert.deepEqual(JSON.parse(JSON.stringify(registry(upgraded))),JSON.parse(JSON.stringify(registry(core))));
  assert.equal(layout.upgradeHtml(upgraded),upgraded,'upgrade must be idempotent');
  for (const name of ['playSingle','joinOnlineGame','launchGameUrl','requireUser','startGame','openCreateLobby','toggleFavorite','copyLobbyLink']) {
    const extract = html => html.slice(html.indexOf(`function ${name}(`),html.indexOf('\n    }',html.indexOf(`function ${name}(`))+6);
    assert.equal(extract(upgraded),extract(core),`${name} behavior changed`);
  }
  for (const script of upgraded.matchAll(/<script type="module">([\s\S]*?)<\/script>/g)) new vm.SourceTextModule(script[1]);
  assert.match(upgraded,/closeModal\(\);showView\("multiplayer"\);listenToCurrentLobby\(\)/);
  assert.match(upgraded,/showView\("multiplayer"\);listenToCurrentLobby\(\);globalThis.GameHubFlagships.revealLobby/);
  assert.equal((upgraded.match(/data-view="discover"/g)||[]).length,3);
  assert.equal((upgraded.match(/    showFirstVisitProfilePrompt\(\);/g)||[]).length,0);
  assert.ok(upgraded.includes('function showFirstVisitProfilePrompt()'),'existing account implementation must remain');
});
test('real core: every missing/ambiguous integration anchor rejects atomically', () => {
  const library = require('../lobby-library.js');
  const core = library.upgradeHtml(read('lobby-core.html'));
  assert.throws(()=>layout.upgradeHtml(core.replace(layout.STARTUP,'changedStartup();')),/startup/);
  assert.throws(()=>layout.upgradeHtml(core+'\n'+layout.STARTUP),/startup/);
});
test('loader keeps all existing game injections and falls back on enhancement failure', async () => {
  const library = require('../lobby-library.js');
  const code = read('lobby.html').match(/<script>\s*([\s\S]*?)<\/script>/)[1];
  const run = async enhancement => {
    let output='';
    const sandbox={GameHubLibrary:library,GameHubFlagships:enhancement,fetch:async()=>({ok:true,text:async()=>read('lobby-core.html')}),document:{open(){},write(html){output=html;},close(){}},console:{error(){},warn(){}}};
    await vm.runInNewContext(code,sandbox);
    return output;
  };
  const baseline=await run(undefined),upgraded=await run(layout),fallback=await run({upgradeHtml(){throw new Error('test');}});
  assert.equal(fallback,baseline);
  assert.ok(upgraded.includes(layout.MARKER));
  assert.deepEqual(JSON.parse(JSON.stringify(registry(upgraded))),JSON.parse(JSON.stringify(registry(baseline))));
  for (const key of ['circuitbound','mtgcatalog','fantasyfootball','receiverwindowqb']) assert.ok(registry(upgraded).some(g=>g.key===key));
});
