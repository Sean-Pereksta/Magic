const test=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const path=require('node:path');
const vm=require('node:vm');
const library=require('../lobby-library.js');
const flagships=require('../lobby-flagships.js');
const arcade=require('../lobby-arcade.js');
const catalog=require('../lobby-catalog-organization.js');
const read=name=>fs.readFileSync(path.join(__dirname,'..',name),'utf8').replace(/\r\n/g,'\n');
const core=read('lobby-core.html'),shared=flagships.upgradeHtml(library.upgradeHtml(core)),upgraded=arcade.upgradeHtml(shared);
const registry=html=>JSON.parse(JSON.stringify(vm.runInNewContext('('+html.match(/const GAMES = (\[[\s\S]*?\n\s*\]);/)[1]+')')));
const games=registry(core);
function functionSource(html,name){const start=html.search(new RegExp(`    (?:async )?function ${name}\\(`));assert.ok(start>=0,name);const end=html.indexOf('\n    }',start);assert.ok(end>start,name);return html.slice(start,end+6);}

test('redesign retains the complete registry and every existing control id',()=>{
  assert.deepEqual(registry(upgraded),games);
  for(const match of core.matchAll(/\bid="([^"]+)"/g))assert.ok(upgraded.includes(`id="${match[1]}"`),match[1]);
  for(const id of ['cnmFlagshipGrid','cnmSearchForm','cnmSearch','cnmMoreMount','cnmRecentGrid','cnmMyRecentGrid','cnmMyFavoritesGrid','cnmPremiumGrid','cnmBuiltOutGrid','cnmLiveGrid','cnmCreateProfile'])assert.ok(upgraded.includes(`id="${id}"`),id);
  for(const id of ['gameStatus','gameResultCount','clearGameFilters','gameActiveFilters'])assert.equal([...upgraded.matchAll(new RegExp(`id="${id}"`,'g'))].length,1,id);
  assert.match(upgraded,/id="gameResultCount" role="status" aria-live="polite"/);
  assert.match(upgraded,/id="gameStatus"[^>]*aria-label="Filter development status"/);
});

test('redesign leaves launch, account, saves, invites and multiplayer handlers unchanged',()=>{
  for(const name of ['launchGameUrl','playSingle','joinOnlineGame','openCreateLobby','createLobby','joinLobby','startGame','redirectStartedGame','toggleFavorite','addRecentGame','login','logout','showLinkedLobbyPrompt','renderProfile','listenToCurrentLobby','handleStartupLobbyState']){
    assert.equal(functionSource(upgraded,name),functionSource(shared,name),name);
  }
  for(const key of ['gh.username','gh.lastLobbyId','gh.staleLobbyCleanupId','gh.lastGameType','gh.lastTab','gh.nextRoute','gh.favorites','gh.recentGames'])assert.ok(upgraded.includes(key),key);
});

test('status classifier covers Complete, In production, Experimental and Tools',()=>{
  assert.equal(catalog.statusForTitle('Cat & Mouse','cat'),'showcase');
  assert.equal(catalog.statusForTitle('A new project','newproject'),'playable');
  assert.equal(catalog.statusForTitle('Estate Defense (Incomplete)','estate'),'prototype');
  assert.equal(catalog.statusForTitle('Mountain Blade Skirmish','mountain'),'prototype');
  assert.equal(catalog.statusForTitle('MTG Catalog & Deck Builder','mtgcatalog'),'tools');
  assert.equal(catalog.cleanTitle(' Estate Defense   (Incomplete) '),'Estate Defense');
  for(const status of ['showcase','playable','prototype','tools'])assert.ok(catalog.statusMeta[status].short);
});

test('status control exposes every stage without changing the existing mode or sort choices',()=>{
  for(const status of ['all','showcase','playable','prototype','tools'])assert.match(upgraded,new RegExp(`<option value="${status}">`));
  for(const id of ['gameMode','gameSort']){
    const select=html=>html.match(new RegExp(`<select id="${id}"[^>]*>[\\s\\S]*?<\\/select>`))[0];
    assert.equal(select(upgraded),select(shared),id);
  }
});

test('enhancement is idempotent and missing integration anchors reject atomically',()=>{
  assert.equal(arcade.upgradeHtml(upgraded),upgraded);
  assert.throws(()=>arcade.upgradeHtml(null),TypeError);
  for(const anchor of ['<section id="view-discover"','<section id="view-mygames"','<div id="categoryPills" class="category-pills"></div>','<select id="gameMode"','      libraryDirty=false;','    globalThis.GameHubFlagships.mount({']){
    assert.throws(()=>arcade.upgradeHtml(shared.replace(anchor,'missing-anchor')),/Arcade layout/,anchor);
  }
});

test('fully transformed core remains valid JavaScript modules',()=>{
  assert.equal(typeof vm.SourceTextModule,'function','Run tests with --experimental-vm-modules');
  const modules=[...upgraded.matchAll(/<script\b[^>]*type="module"[^>]*>([\s\S]*?)<\/script>/g)];
  assert.ok(modules.length);
  for(const [,source]of modules)assert.doesNotThrow(()=>new vm.SourceTextModule(source));
});

test('all original development states and categories remain searchable in the upgraded source',()=>{
  const after=registry(upgraded);
  assert.deepEqual(after.map(game=>game.category),games.map(game=>game.category));
  assert.equal(after.filter(game=>/Incomplete/.test(game.title)).length,games.filter(game=>/Incomplete/.test(game.title)).length);
  for(const game of games)for(const field of ['route','singleRoute','multiplayerRoute','downloadRoute','dmRoute'])assert.equal(after.find(item=>item.key===game.key)[field],game[field],`${game.key}.${field}`);
});
