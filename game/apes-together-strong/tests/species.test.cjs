'use strict';
const test=require('node:test'),assert=require('node:assert/strict');
const {loadEngine}=require('./performance-harness.cjs');
const species=['gorilla','chimpanzee','orangutan','gibbon','mandrill','capuchin'];
function arena(seed='PRIMATE-SPECIES'){
 const c=loadEngine(),g=new c.ATSGame(seed,'survival');
 g.world.objects.clear();g.world._spatial.clear();g.world.sites.clear();g.world.ensure=()=>{};g.world.stream=()=>{};g.world.getSites=()=>[];g.world.getObjects=()=>[];
 g.world.terrain=()=>({biome:'farmland',road:true,water:false});g.world.lineClear=()=>true;g.world.blocked=()=>false;g.spawnSites=()=>{};g.responseDirector=()=>{};g.heliTimer=100000;return {c,g};
}
function valid(a){assert.ok(species.includes(a.species),a.species);assert.ok(Number.isInteger(a.coatVariant)&&a.coatVariant>=0&&a.coatVariant<=2)}
function looks(a){return {species:a.species,coatVariant:a.coatVariant,fur:a.fur,bodyScale:a.bodyScale}}

test('all six species and coat shades are deterministic cosmetics while the King remains a gorilla',()=>{
 const first=arena(),second=arena();assert.deepEqual(Object.keys(first.c.ATSPrimateSpecies),species);assert.equal(first.g.king.species,'gorilla');assert.equal(first.g.king.hp,160);assert.equal(first.g.king.maxHp,160);valid(first.g.king);
 const seen=new Set(),coats=new Set();for(let i=0;i<600;i++){const a=first.g.makeApe(i,0,'hold'),b=second.g.makeApe(-i,100,'hold');valid(a);seen.add(a.species);coats.add(a.coatVariant);assert.deepEqual(looks(a),looks(b));assert.equal(a.hp,120);assert.equal(a.radius,undefined)}
 assert.equal(seen.size,6);assert.equal(coats.size,3);
 const different=arena('OTHER-SPECIES-SEED').g;let changes=0;for(let i=0;i<24;i++){const a=different.makeApe(i,0,'hold');if(a.species!==first.g.apes[i].species||a.coatVariant!==first.g.apes[i].coatVariant)changes++}assert.ok(changes>0,'seed changes produce distinct stable appearances');
});

test('appearance assignment adds no random draws and preserves existing adult and child stats',()=>{
 const {c,g}=arena();let draws=0;c.Math.random=()=>++draws/10;
 const a=g.makeApe(0,0);assert.equal(draws,9);assert.equal(a.dir,.1*Math.PI*2);assert.equal(a.phase,.2*Math.PI*2);assert.equal(a.fur,.3);assert.equal(a.bodyScale,.88+.4*.22);assert.equal(a.attackCD,.5*.4);assert.equal(a.speed,84+.6*18);assert.equal(a.offsetX,(.7-.5)*120);assert.equal(a.offsetY,(.8-.5)*120);assert.equal(a.wander,.9*Math.PI*2);
 const before={...a};g.ensureApeAppearance(a);assert.equal(draws,9);assert.deepEqual({...a},before);
 draws=0;const child=g.makeApe(0,0,'young',null,true);assert.equal(draws,8);assert.equal(child.hp,60);assert.equal(child.maxHp,60);assert.equal(child.speed,67);assert.equal(child.age,0);assert.equal(child.radius,undefined);valid(child);
});

test('rescued captives acquire stable species without changing rescue counts or adult balance',()=>{
 const {g}=arena(),cage={id:'rescue-test',type:'cage',x:0,y:0,count:9,prisoners:9};assert.equal(g.releaseCaptives(cage),9);assert.equal(cage.count,0);assert.equal(g.stats.freed,9);assert.equal(g.population,9);
 for(const a of g.apes){valid(a);assert.equal(a.state,'free');assert.equal(a.hp,120);assert.equal(a.maxHp,120);assert.ok(a.speed>=84&&a.speed<=102);const before=looks(a);g.ensureApeAppearance(a);assert.deepEqual(looks(a),before)}
});

test('real settlement births retain their species and coat through nearby and distant maturation',()=>{
 const {g}=arena();for(let i=0;i<8;i++)g.makeApe(i,0,'follow');g.food=400;assert.equal(g.command('settleAll'),true);const s=g.settlements[0];
 Object.assign(s,{food:400,housing:24,wood:120,gardens:2,safety:1,birthTimer:30,suitability:{fertility:1,wood:24,capacity:1000,water:true}});delete s.structuresVersion;g.colonies.init(s);g.refreshSettlements();g.humanGrid.rebuild([]);g.colonies.tick(s);
 assert.ok(g.stats.born>0);const child=g.apes.find(a=>a.state==='young');assert.ok(child);valid(child);const before=looks(child);child.age=34.9;g.updateApe(child,.1);assert.equal(child.state,'settled');assert.equal(child.hp,120);assert.equal(child.speed,90);assert.deepEqual(looks(child),before);
 const remote=g.makeApe(4000,0,'young',null,true),remoteLook=looks(remote);remote.age=34.9;g.abstractActor(remote,.1,'ape');assert.equal(remote.state,'settled');assert.equal(remote.hp,120);assert.equal(remote.speed,90);assert.deepEqual(looks(remote),remoteLook);
});

test('saved species, shades, wounds, age and original fur/body scale round-trip unchanged',()=>{
 const {c,g}=arena();for(let i=0;i<6;i++){const a=g.makeApe(i*10,0,i===0?'young':i===1?'scout':'hold',null,i===0);a.species=species[i];a.coatVariant=i%3;a.hp-=17;a.age=i===0?12.5:280+i;a.fur=.1+i*.1;a.bodyScale=.9+i*.03}
 g.king.coatVariant=2;g.king.hp=143;g.king.fur=.74;g.king.bodyScale=1.23;const saved=JSON.parse(JSON.stringify(g.serialize())),loaded=c.ATSGame.fromJSON(saved);
 assert.deepEqual(looks(loaded.king),looks(g.king));assert.equal(loaded.king.hp,143);
 for(let i=0;i<6;i++){assert.deepEqual(looks(loaded.apes[i]),looks(g.apes[i]));for(const key of ['hp','maxHp','age','speed','attackCD','state'])assert.equal(loaded.apes[i][key],g.apes[i][key])}
});

test('legacy missing or unknown appearance fields migrate deterministically while valid fields and wounds remain',()=>{
 const {c,g}=arena();for(let i=0;i<4;i++)g.makeApe(i,0,'hold');const input=JSON.parse(JSON.stringify(g.serialize()));delete input.king.species;delete input.king.coatVariant;
 delete input.apes[0].species;delete input.apes[0].coatVariant;input.apes[0].hp=53;input.apes[0].age=319;input.apes[0].fur=.317;input.apes[0].bodyScale=1.079;
 input.apes[1].species='unknown-primate';input.apes[1].coatVariant=4.5;input.apes[2].species='orangutan';input.apes[2].coatVariant=2;input.apes[3].species='__proto__';input.apes[3].coatVariant='1';
 const loaded=c.ATSGame.fromJSON(JSON.parse(JSON.stringify(input))),again=c.ATSGame.fromJSON(JSON.parse(JSON.stringify(input)));valid(loaded.king);assert.equal(loaded.king.species,'gorilla');
 for(let i=0;i<4;i++){valid(loaded.apes[i]);assert.deepEqual(looks(loaded.apes[i]),looks(again.apes[i]))}
 assert.equal(loaded.apes[0].hp,53);assert.equal(loaded.apes[0].age,319);assert.equal(loaded.apes[0].fur,.317);assert.equal(loaded.apes[0].bodyScale,1.079);assert.equal(loaded.apes[2].species,'orangutan');assert.equal(loaded.apes[2].coatVariant,2);
 const roundTrip=c.ATSGame.fromJSON(JSON.parse(JSON.stringify(loaded.serialize())));for(let i=0;i<4;i++)assert.deepEqual(looks(roundTrip.apes[i]),looks(loaded.apes[i]));
});

test('flying apes and corpses preserve their designs and legacy ape/King bodies receive stable looks',()=>{
 const {c,g}=arena(),a=g.makeApe(25,0,'hold');a.species='mandrill';a.coatVariant=2;const before=looks(a);g.launchBlastReaction(a,{x:0,y:0,type:'airstrike',radius:80},{radius:80});assert.deepEqual(looks(a),before);
 g.hurt(a,1000,{x:0,y:0,type:'airstrike'});const corpse=g.corpses[0];assert.equal(corpse.type,'ape');assert.deepEqual(looks(corpse),before);assert.equal(corpse.blastReaction.stage,'flight');
 const input=JSON.parse(JSON.stringify(g.serialize()));delete input.corpses[0].species;input.corpses[0].coatVariant=-1;input.corpses.push({...input.king,type:'ape',hp:0,species:'chimpanzee',coatVariant:1,life:10});input.corpses.push({id:'human-legacy',type:'human',x:0,y:0,hp:0,life:10});
 const loaded=c.ATSGame.fromJSON(input);valid(loaded.corpses[0]);assert.equal(loaded.corpses[0].fur,a.fur);assert.equal(loaded.corpses[0].bodyScale,a.bodyScale);assert.equal(loaded.corpses[1].species,'gorilla');assert.equal(loaded.corpses[1].coatVariant,1);assert.equal(loaded.corpses[2].species,undefined);assert.equal(loaded.corpses[2].coatVariant,undefined);
 const again=c.ATSGame.fromJSON(JSON.parse(JSON.stringify(loaded.serialize())));assert.deepEqual(looks(again.corpses[0]),looks(loaded.corpses[0]));assert.deepEqual(looks(again.corpses[1]),looks(loaded.corpses[1]));
});

test('600 and 1000 apes retain bounded simulation work with no per-frame appearance migration',()=>{
 for(const population of [600,1000]){
  const {g}=arena('PRIMATE-DENSE-'+population);for(let i=0;i<population;i++)g.makeApe(i%25*6,(Math.floor(i/25)-20)*6,'hold');const appearances=Array.from(g.apes,looks);
  let calls=0;const ensure=g.ensureApeAppearance.bind(g);g.ensureApeAppearance=a=>{calls++;return ensure(a)};
  for(let frame=0;frame<20;frame++){g.update(1/60,{});assert.ok(g.performance.counters.aiThinks<=32);assert.ok(g.performance.counters.losTests<=96);assert.ok(g.navigation.stats.frameExpanded<=192);assert.ok(g.navigation.stats.frameSearches<=3)}
  assert.equal(calls,0);assert.equal(g.population,population);for(let i=0;i<population;i++)assert.deepEqual(looks(g.apes[i]),appearances[i]);
 }
});
