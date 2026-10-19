const test=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const path=require('node:path');
const vm=require('node:vm');
const Rise=require('../js/campaign/rise-to-glory.js');
const Portraits=require('../js/campaign/rise-portraits.js');

const memoryStorage=()=>{
  const values=new Map();
  return {getItem:key=>values.get(key)||null,setItem:(key,value)=>values.set(key,String(value))};
};

test('the portrait catalog contains immutable, distinct, locally bundled athlete images',()=>{
  assert.equal(Portraits.catalog.length,22);
  assert.ok(Object.isFrozen(Portraits.catalog));
  assert.equal(new Set(Portraits.catalog.map(portrait=>portrait.id)).size,22);
  assert.equal(new Set(Portraits.catalog.map(portrait=>portrait.src)).size,22);
  for(const portrait of Portraits.catalog){
    assert.ok(Object.isFrozen(portrait));
    assert.match(portrait.id,/^player-(?:0[1-9]|1[0-9]|2[0-2])$/);
    assert.equal(portrait.src,`assets/campaign/portraits/${portrait.id}.webp`);
    assert.ok(Number.isInteger(portrait.skin)&&portrait.skin>=0&&portrait.skin<=0xffffff);
    assert.ok(portrait.hairStyles.length>0);
    assert.ok(Object.isFrozen(portrait.hairStyles));
    assert.ok(portrait.ageRange[0]>=18,'all portrait subjects are adults');
    assert.ok(portrait.ageRange[1]>=portrait.ageRange[0]);
    const asset=path.resolve(__dirname,'..',portrait.src);
    assert.ok(fs.existsSync(asset),`${portrait.id} must be bundled with the game`);
    const header=fs.readFileSync(asset).subarray(0,12);
    assert.equal(header.subarray(0,4).toString(),'RIFF');
    assert.equal(header.subarray(8,12).toString(),'WEBP');
  }
});

test('an athlete keeps the same face through recruitment, development, role changes and save reloads',()=>{
  const state=Rise.create({seed:47});
  const prospect=Rise.market(state,{sort:'cost'}).find(player=>player.signingCost<=state.currency);
  assert.ok(prospect,'the test requires an affordable recruit');
  const original=Portraits.select(prospect);
  assert.ok(Portraits.catalog.includes(original));
  assert.equal(Rise.sign(state,prospect.id).ok,true);
  const signed=Rise.player(state,prospect.id);
  assert.equal(Portraits.select(signed).id,original.id);

  Rise.addXP(signed,1200);
  const upgrade=Rise.ATTRIBUTE_KEYS.find(key=>Rise.upgradePreview(state,signed.id,key)?.available);
  assert.ok(upgrade,'earned XP should enable a real rating upgrade');
  assert.equal(Rise.upgrade(state,signed.id,upgrade).ok,true);
  assert.equal(Rise.setRole(state,signed.id,'balanced',3).ok,true);
  signed.name='New captain display name';
  signed.appearance.number='99';
  signed.age++;
  signed.stats.games+=5;
  signed.stats.points+=50;
  state.roster.reverse();
  assert.equal(Portraits.select(signed).id,original.id,'development and display changes must not replace a face');

  const storage=memoryStorage();
  assert.equal(Rise.save(state,storage).ok,true);
  const restored=Rise.load(storage);
  assert.ok(restored);
  assert.equal(Portraits.select(Rise.player(restored,prospect.id)).id,original.id);
  for(const player of Object.values(state.players)){
    assert.equal(Portraits.select(Rise.player(restored,player.id)).id,Portraits.select(player).id,`${player.id} changed after reload`);
  }
});

test('sorting and filtering the scouting market do not change assigned faces or mutate players',()=>{
  const state=Rise.create({seed:62}),players=Object.values(state.players);
  const original=new Map(players.map(player=>[player.id,Portraits.select(player).id]));
  for(const sort of ['overall','cost','potential']){
    for(const player of Rise.market(state,{sort}))assert.equal(Portraits.select(player).id,original.get(player.id));
  }
  for(const player of players){
    const snapshot=JSON.stringify(player);
    Portraits.select(Object.freeze({...player,appearance:Object.freeze({...player.appearance}),physical:Object.freeze({...player.physical})}));
    assert.equal(JSON.stringify(player),snapshot);
  }
});

test('portrait appearance follows the athlete skin, hairstyle and build rather than their name or ratings',()=>{
  const physical={lean:{heightCm:190,weightKg:76},athletic:{heightCm:195,weightKg:94},broad:{heightCm:210,weightKg:130}};
  for(const portrait of Portraits.catalog){
    const player={id:'appearance-match',name:'Any name',age:22,appearance:{skin:portrait.skin,hairStyle:portrait.hairStyles[0]},physical:physical[portrait.build]};
    assert.equal(Portraits.select(player).id,portrait.id,`${portrait.id} should match its depicted appearance`);
    assert.equal(Portraits.select({...player,appearance:{...player.appearance,skin:'#'+portrait.skin.toString(16).padStart(6,'0')}}).id,portrait.id);
  }
  const skin=0x3d2418,identity={id:'same-athlete',appearance:{skin}};
  assert.equal(Portraits.select({...identity,physical:physical.lean}).build,'lean');
  assert.equal(Portraits.select({...identity,physical:physical.broad}).build,'broad');
  const braided={...identity,appearance:{skin,hairStyle:'braids'},physical:physical.lean};
  const changed={...braided,appearance:{skin:0x5d3425,hairStyle:'afro'}};
  assert.notEqual(Portraits.select(braided).id,Portraits.select(changed).id,'an explicit appearance change should update the matching headshot');
  assert.equal(Portraits.select(changed).skin,0x5d3425);
  assert.ok(Portraits.select(changed).hairStyles.includes('afro'));
});

test('portrait assignment is stable for adult age changes and incomplete appearance data',()=>{
  const player=Rise.generateMarket(73)[0],original=Portraits.select(player).id;
  for(const age of [18,21,29,40])assert.equal(Portraits.select({...player,age}).id,original);
  for(const value of [undefined,null,{}, {id:'unknown'}, {id:'unknown',appearance:{skin:'invalid',hairStyle:'unknown'},physical:{heightCm:NaN,weightKg:NaN}}]){
    assert.ok(Portraits.catalog.includes(Portraits.select(value)));
    assert.equal(Portraits.select(value).id,Portraits.select(value).id);
  }
});

test('portrait selection loads directly in a browser without requiring a module loader',()=>{
  const browser={};
  vm.runInNewContext(fs.readFileSync(require.resolve('../js/campaign/rise-portraits.js'),'utf8'),browser);
  assert.equal(typeof browser.RisePortraits?.select,'function');
  for(const player of Rise.generateMarket(83))assert.equal(browser.RisePortraits.select(player).id,Portraits.select(player).id);
});

test('delegated image events reveal a loaded portrait and preserve the SVG fallback on failure',()=>{
  const listeners=new Map(),document={addEventListener:(name,handler,capture)=>{
    assert.equal(capture,true,'image events require capture to reach the delegated handler');
    assert.ok(!listeners.has(name),'a repeated binding must not duplicate listeners');
    listeners.set(name,handler);
  }};
  Portraits.bindFallbacks(document);
  Portraits.bindFallbacks(document);
  assert.deepEqual([...listeners.keys()],['load','error']);
  const classes=new Set(),wrapper={classList:{toggle:(name,enabled)=>enabled?classes.add(name):classes.delete(name)}};
  const image={classList:{contains:name=>name==='rise-headshot-image'},closest:selector=>selector==='.rise-headshot'?wrapper:null,naturalWidth:0,hidden:false};
  listeners.get('load')({type:'load',target:image});
  assert.equal(image.hidden,true);
  assert.ok(classes.has('is-fallback'));
  assert.ok(!classes.has('is-loaded'));
  image.naturalWidth=320;
  listeners.get('load')({type:'load',target:image});
  assert.equal(image.hidden,false);
  assert.ok(classes.has('is-loaded'));
  assert.ok(!classes.has('is-fallback'));
  listeners.get('error')({type:'error',target:image});
  assert.equal(image.hidden,true);
  assert.ok(classes.has('is-fallback'));
  assert.ok(!classes.has('is-loaded'));
  const unrelated={classList:{contains:()=>false},hidden:false};
  listeners.get('error')({type:'error',target:unrelated});
  assert.equal(unrelated.hidden,false);
  assert.doesNotThrow(()=>listeners.get('error')({type:'error',target:{classList:image.classList}}));
});

test('the browser library automatically installs image recovery once for its document',()=>{
  const events=[],document={addEventListener:(...args)=>events.push(args)},browser={document};
  vm.runInNewContext(fs.readFileSync(require.resolve('../js/campaign/rise-portraits.js'),'utf8'),browser);
  assert.deepEqual(events.map(event=>event[0]),['load','error']);
  browser.RisePortraits.bindFallbacks(document);
  assert.equal(events.length,2);
});

test('legacy saved athletes retain their original twelve-face identities after the catalog expands',()=>{
  const raw=Rise.create({seed:47}),styles=['shortFade','braids','buzz','afro','spiky'];
  for(const player of Object.values(raw.players)){
    const index=Number(player.id.split('-').pop());player.appearance.hairStyle=styles[index%5];
    delete player.appearance.portraitId;delete player.appearance.portraitSignature;
  }
  const expected={2:'player-05',3:'player-11',8:'player-09',17:'player-11',18:'player-01',21:'player-03',22:'player-10',23:'player-05',25:'player-02',26:'player-08'};
  for(const opponent of raw.season.opponents)for(const player of opponent.players){
    player.appearance.skin=0x3d2418;player.appearance.hairStyle='braids';delete player.appearance.portraitId;delete player.appearance.portraitSignature;
  }
  const restored=Rise.normalize(raw);assert.ok(restored);
  for(const [index,id] of Object.entries(expected)){
    const player=restored.players['rtg-47-'+index];assert.equal(player.appearance.portraitId,id);assert.equal(Portraits.select(player).id,id);
  }
  for(const opponent of restored.season.opponents)for(const player of opponent.players)assert.equal(Portraits.select(player).id,'player-01','saved opponent appearance also keeps its original face');
  const storage=memoryStorage();assert.equal(Rise.save(restored,storage).ok,true);const twice=Rise.load(storage);
  for(const player of Object.values(restored.players))assert.equal(Portraits.select(twice.players[player.id]).id,player.appearance.portraitId);
});

test('all twenty-two faces are reachable by generated markets and future recruiting classes',()=>{
  const reached=new Set();
  for(let seed=1;seed<=8;seed++)for(const player of Rise.generateMarket(seed)){
    const portrait=Portraits.select(player);reached.add(portrait.id);
    assert.equal(player.appearance.portraitId,portrait.id);
    assert.equal(player.appearance.portraitSignature,Portraits.signature(player));
  }
  assert.deepEqual([...reached].sort(),Portraits.catalog.map(portrait=>portrait.id).sort());
  const state=Rise.create({seed:47}),oldFaces=new Map(Object.values(state.players).map(player=>[player.id,player.appearance.portraitId]));
  const future=new Set();
  for(let season=0;season<8;season++){
    state.season.phase='offseason';assert.equal(Rise.advanceSeason(state).ok,true);
    for(const player of Object.values(state.players).filter(player=>player.id.startsWith('prospect-'))){
      assert.equal(Portraits.select(player).id,player.appearance.portraitId);future.add(player.appearance.portraitId);
    }
  }
  assert.ok([...future].some(id=>Number(id.slice(-2))>12),'new season recruits use the expanded catalog');
  for(const [id,portraitId] of oldFaces)assert.equal(Portraits.select(state.players[id]).id,portraitId,'season advancement does not reassign existing faces');
});

test('saved portrait IDs are validated and deliberate appearance changes still choose an appropriate face',()=>{
  const state=Rise.create({seed:47}),player=state.players[state.captainId];
  player.appearance.portraitId='../../unsafe';player.appearance.portraitSignature=Portraits.signature(player);
  const restored=Rise.normalize(state);assert.ok(restored);assert.ok(Portraits.get(restored.players[player.id].appearance.portraitId));
  const assigned=restored.players[player.id],old=Portraits.select(assigned).id;
  assigned.appearance.skin=0x3d2418;assigned.appearance.hairStyle='afro';
  const changed=Portraits.select(assigned);assert.notEqual(changed.id,old);assert.equal(changed.skin,0x3d2418);assert.ok(changed.hairStyles.includes('afro'));
  assert.ok(Rise.normalize({...state,season:{...state.season,opponents:'ignore malformed derived data'}}));
});
