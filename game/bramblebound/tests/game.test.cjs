const test=require('node:test');
const assert=require('node:assert/strict');
const vm=require('node:vm');
const fs=require('node:fs');
const path=require('node:path');
function game(){const context=vm.createContext({console});context.window=context;for(const name of ['catalog','progression','engine'])vm.runInContext(fs.readFileSync(path.join(__dirname,'..',name+'.js'),'utf8'),context);return {C:context.Catalog,E:context.GameEngine,P:context.Progression};}
test('every region has a guardian and enough distinct enemies for group encounters',()=>{
  const {C,E}=game();assert.equal(C.regions.length,35);assert.ok(Object.keys(C.items).length>=284);
  for(let i=0;i<C.regions.length;i++){assert.equal(C.enemies.filter(e=>e.region===i&&e.boss).length,1);assert.ok(C.enemies.filter(e=>e.region===i&&!e.boss).length>=5);}
  for(let round=1;round<=420;round++){const route=E.routeInfo({round,routeEpoch:1});assert.equal(route.region,Math.floor(((round-1)%140)/4));assert.equal(route.circuit,1+Math.floor((round-1)/140));}
});
test('all fusion recipes consume their ingredients and exceed the highest ingredient level',()=>{
  const {C,E}=game();let recipes=0;
  for(const d of Object.values(C.items)){if(!d.recipe)continue;recipes++;const p=E.freshProfile();E.start(p);p.run.active=[{uid:1,id:d.recipe[0],level:2,cd:0,slot:0}];p.run.pack=[{uid:2,id:d.recipe[1],level:3,cd:0,slot:0}];p.run.nextUid=3;
    assert.equal(E.act(p,'combine',1,2).ok,true,d.id);assert.equal(p.run.active[0].id,d.id);assert.equal(p.run.active[0].level,4);assert.equal(p.run.pack.length,0);
  }assert.ok(recipes>100);
});
test('matching copies upgrade and the last equipped relic cannot be removed',()=>{
  const {E}=game(),p=E.freshProfile();E.start(p);const a=p.run.active[0],b=p.run.pack.find(x=>x.id===a.id);assert.equal(E.act(p,'combine',a.uid,b.uid).ok,true);assert.equal(p.run.active.find(x=>x.id===a.id).level,2);
  E.act(p,'stow',p.run.active.find(x=>x.id!==a.id).uid);assert.equal(E.act(p,'stow',p.run.active[0].uid).ok,false);assert.equal(E.act(p,'discard',p.run.active[0].uid).ok,false);
});
test('seeded journeys survive save/reload, grant seeds on death once, and buy permanent upgrades',()=>{
  const {E}=game();let drafts=0,guardians=0;
  for(let seed=1;seed<=24;seed++){
    let p=E.freshProfile();E.start(p);p.run.seed=seed*17777;
    // Use the free starting ingredients before setting out.
    E.act(p,'combine',p.run.active[0].uid,p.run.pack.find(x=>x.id===p.run.active[0].id).uid);
    for(let step=0;step<3000&&p.run.phase!=='dead';step++){
      if(p.run.draft.length){drafts++;const time=p.run.time;E.tick(p,2);assert.equal(p.run.time,time);E.act(p,'chooseUpgrade',p.run.draft[0]);}
      const events=E.tick(p,.5);if(events.some(e=>e.kind==='round')&&p.run.round%4===0)guardians++;
      if(step%19===0)p=E.validate(JSON.stringify(p));
    }
    assert.equal(p.run.phase,'dead');assert.ok(p.best>=1);assert.ok(p.points>=2);const points=p.points;E.tick(p,5);assert.equal(p.points,points);p=E.validate(JSON.stringify(p));
    assert.equal(E.act(p,'buy','heart').ok,true);E.start(p);assert.equal(p.run.maxHp,102);assert.equal(p.run.phase,'walk');
  }
  assert.ok(drafts>0);assert.ok(guardians>0);
});
test('save validation rejects corrupt profiles and ignores untrusted display fields',()=>{
  const {E}=game(),p=E.freshProfile();E.start(p);const copy=()=>JSON.parse(JSON.stringify(p));
  for(const edit of [p=>p.points=-1,p=>p.run.hp=Infinity,p=>p.run.active[0].id='missing',p=>p.run.pack[0].uid=p.run.active[0].uid,p=>p.run.draft=['not-a-talent']]){const bad=copy();edit(bad);assert.throws(()=>E.validate(bad));}
  const extra=copy();extra.untrusted='<script>';extra.run.active[0].displayName='fake';const safe=E.validate(extra);assert.equal(safe.untrusted,undefined);assert.equal(safe.run.active[0].displayName,undefined);
});
test('library registry, launch action, built-out shelf, and artwork agree',()=>{
  const root=path.resolve(__dirname,'../../..'),read=f=>fs.readFileSync(path.join(root,f),'utf8');
  const source=read('lobby/lobby-core.html'),games=vm.runInNewContext('('+source.match(/const GAMES = (\[[\s\S]*?\n\s*\]);/)[1]+')'),entry=games.find(x=>x.key==='bramblebound');
  assert.equal(games.filter(x=>x.key==='bramblebound').length,1);assert.equal(entry.route,'/game/bramblebound/');assert.deepEqual(require(path.join(root,'lobby/lobby-library.js')).getActions(entry),[{type:'single',label:'Play'}]);
  assert.ok(require(path.join(root,'lobby/lobby-showcase.js')).builtOut.some(x=>x.key===entry.key));
  const manifest=JSON.parse(read('lobby/assets/game-artwork.json')),art=manifest.games[entry.key];assert.equal(art.ready,true);
  for(const variant of ['tile','thumb','hero'])assert.ok(fs.statSync(path.join(root,'lobby/assets',art[variant])).size>1000);
  const context={};vm.runInNewContext(read('lobby/assets/game-artwork.js'),context);assert.equal(JSON.stringify(context.GameHubArtworkManifest),JSON.stringify(manifest));
});
