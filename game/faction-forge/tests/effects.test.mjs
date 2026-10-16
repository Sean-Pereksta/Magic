import test from 'node:test';
import assert from 'node:assert/strict';
import {createBattleEffects,UNIT_EFFECTS,EFFECT_LIMITS,EffectPool,projectilePosition,drawEffect} from '../effects.js';
import {createBattleAudio,AUDIO_LIMITS} from '../audio.js';
import {unitArtTier,createBattleRenderer,unitEffectLabels} from '../renderer.js';
import {ASSET_MANIFEST} from '../manifest.js';
import {COMBOS} from '../combos.js';
import {EVOLUTIONS,applyEvolution,afterEvolutionAttack,tickEvolutions} from '../evolutions.js';
import {tickCombos} from '../combos.js';
import {createRequire} from 'node:module';
const require=createRequire(import.meta.url);
const {loadModules,createCombatAdapter}=require('../../../server/faction-forge/simulator.cjs');

const silentAudio=()=>({settings:{audio:false,volume:.35},play:()=>false,configure:()=>({audio:false,volume:.35}),clear(){},destroy(){},unlock:async()=>false,activeVoices:0});
function harness(options={}){let time=0;const fx=createBattleEffects({now:()=>time,audio:silentAudio(),...options});return {fx,setTime(t){time=t;},event:(kind,extra={})=>fx.event({id:`${kind}:${time}`,type:'combat',kind,unitType:'vrocket',sourceId:'source',targetId:'target',x1:1,y1:8,x2:4,y2:3,radius:1,at:1000,...extra})};}
const context=()=>new Proxy({calls:[],globalAlpha:1},{get(target,key){if(key in target)return target[key];return (...args)=>target.calls.push([key,...args]);},set(target,key,value){target[key]=value;return true;}});

test('all 32 existing sprite units have distinct attack, ability and death presentation',()=>{
  assert.deepEqual(Object.keys(UNIT_EFFECTS).sort(),Object.keys(ASSET_MANIFEST.units).sort());
  for(const unitType of Object.keys(UNIT_EFFECTS)){
    const {fx,event}=harness();assert.equal(event('attack',{unitType}),true,unitType);
    assert.ok(fx.stats.active>0,unitType+' attack');fx.clear();
    assert.equal(event('ability',{unitType}),true,unitType);assert.ok(fx.stats.active>0,unitType+' ability');fx.clear();
    assert.equal(event('death',{unitType}),true,unitType);fx.step(0);assert.ok(fx.stats.active>0,unitType+' death');fx.destroy();
  }
});

test('every shared combo has bounded activation artwork and readable localized feedback',()=>{
  for(const rule of COMBOS){
    const {fx,event}=harness();assert.equal(event('combo',{comboId:rule.id,name:rule.name,unitType:rule.units[0],targets:[{id:'e2',x:5,y:3}]}),true,rule.id);fx.step(0);
    assert.ok(fx.pool.active.some(e=>e.family==='label'&&e.text===rule.name.toUpperCase()),rule.id);
    assert.ok(fx.stats.active<=EFFECT_LIMITS.high);fx.destroy();
  }
});

test('real evolution hooks across all 64 paths emit only supported presentation events',()=>{
  const emitted=new Set();let serial=0;
  const make=(type,p,x,y,props={})=>({id:'evo-'+(++serial),type,p,x,y,lvl:3,hp:150,maxhp:500,dmg:6,cd:1000,range:5,armor:0,size:1,...props});
  for(const [type,paths] of Object.entries(EVOLUTIONS))for(const path of Object.keys(paths)){
    const actor=make(type,0,2,3,{evolutionDamageTaken:10,wardCd:1000,wardDur:5000,healCd:1200,broodCd:3500,burrowCd:3000}),ally=make('rifleman',0,2,4),target=make('vguard',1,3,4),near=make('rifleman',1,4,4),outer=make('rifleman',1,5,4);
    const st={field:{units:[actor,ally,target,near,outer],wards:[],comboFields:[]}},events=[],hooks={event:e=>events.push(e),applyDamage:(v,d)=>{v.hp-=d;return d;},spawnIllusion:(state,u)=>{state.field.units.push(make('miragewarden',u.p,1,3,{illusion:true}));return true;}};
    applyEvolution(actor,path);tickEvolutions(st,1000,hooks);for(const time of [1000,1180,1360])afterEvolutionAttack(actor,target,time,st,hooks);tickCombos(st,2200,hooks);
    const {fx}=harness();for(const ev of events){emitted.add(ev.kind);assert.equal(fx.event(ev),true,`${type}/${path}: ${ev.kind}`);}fx.step(1000);fx.destroy();
  }
  for(const expected of ['evolution-echo','precision-hit','shockwave','lightning-field','blood-field','rupture','rocket-explosion','rocket','binding','shield','healing','toxic-cloud'])assert.ok(emitted.has(expected),'real hook did not exercise '+expected);
});

test('actual authoritative replays for all 64 evolution paths are fully consumable by the renderer',async()=>{
  const modules=await loadModules(),catalog=createCombatAdapter({seed:'effects-catalog',modules}).troops,kinds=new Set();
  for(const [type,paths] of Object.entries(EVOLUTIONS))for(const path of Object.keys(paths)){
    const st={roster:{p0:[{type,count:1,lvl:3}],p1:[{type:'vguard',count:1,lvl:2},{type:'rifleman',count:1,lvl:1}]},factions:{p0:catalog[type].faction,p1:'Vanir'},relics:{p0:[],p1:[]},tactics:{p0:[],p1:[]},formations:{p0:'balanced',p1:'balanced'},evolutions:{p0:{[type]:path},p1:{}},comboEnhancements:{p0:[],p1:[]}};
    const battle=createCombatAdapter({seed:`effects-${type}-${path}`,modules}).battle(st),{fx,setTime}=harness(),seen=new Set();
    for(const frame of battle.frames){setTime(frame.at);for(const ev of frame.syncedFx){if(seen.has(ev.id))continue;seen.add(ev.id);kinds.add(ev.kind||ev.type);assert.equal(fx.event(ev),true,`${type}/${path}: ${ev.kind||ev.type}`);}fx.step(frame.at);}
    assert.ok(seen.size>0,`${type}/${path} should show real battle events`);fx.destroy();
  }
  for(const expected of ['attack','hit','death','rail-charge','rail-impact','rail-shot','temporal-field','mark','mirage'])assert.ok(kinds.has(expected),expected+' missing from real replays');
});

test('fortress links connect friendly members and precision follow-up uses the Railbreaker origin',()=>{
  const {fx,event}=harness();event('combo',{id:'fortress',unitType:'golem',comboId:'arcane-fortress',links:[{id:'source',x:1,y:8},{id:'ward',x:2,y:7}],targets:[{id:'enemy',x:4,y:3,amount:2}]});fx.step(0);
  const link=fx.pool.active.find(e=>e.family==='energy-link');assert.equal(link.x2,168);assert.equal(link.y2,508);assert.ok(fx.pool.active.some(e=>e.family==='lightning'&&e.x2===304));fx.clear();
  event('combo',{id:'precision',unitType:'marksman',comboId:'precision-execution',followSourceId:'rail',followX:0,followY:9});fx.step(0);const rail=fx.pool.active.find(e=>e.family==='rail');assert.equal(rail.x,32);assert.equal(rail.y,644);fx.destroy();
});

test('actual Rifleman third-shot events launch a distinct follow-up from the firing Rifleman',async()=>{
  const modules=await loadModules();
  const state={roster:{p0:[{type:'rifleman',count:1,lvl:2},{type:'vguard',count:3,lvl:3},{type:'medic',count:3,lvl:3}],p1:[{type:'vguard',count:1,lvl:3},{type:'medic',count:3,lvl:3}]},factions:{p0:'Vanir',p1:'Vanir'},relics:{p0:[],p1:[]},tactics:{p0:[],p1:[]},formations:{p0:'balanced',p1:'balanced'},evolutions:{p0:{rifleman:'a'},p1:{}},comboEnhancements:{p0:[],p1:[]}};
  const battle=createCombatAdapter({seed:'rifleman-burst-origin',modules}).battle(state),events=[...new Map(battle.frames.flatMap(frame=>frame.syncedFx).map(ev=>[ev.id,ev])).values()];
  const bonus=events.find(ev=>ev.unitType==='rifleman'&&ev.kind==='evolution-echo'&&ev.followUp);
  assert.ok(bonus,'Sustained Fire must emit its real third-shot bonus');
  const primary=events.find(ev=>ev.kind==='attack'&&ev.sourceId===bonus.sourceId&&ev.at===bonus.at);
  assert.ok(primary);assert.notEqual(bonus.sourceId,bonus.targetId);
  assert.equal(bonus.x1,primary.x);assert.equal(bonus.y1,primary.y);
  assert.ok(bonus.x1!==bonus.x2||bonus.y1!==bonus.y2,'the origin must not be replaced by the target');
  let time=0;const renderer=createBattleRenderer({now:()=>time,audio:silentAudio(),assets:{enabled:()=>false}});
  renderer.event(primary);renderer.event(bonus);
  const shots=renderer.projectiles.filter(ev=>ev.sourceId===bonus.sourceId&&ev.family==='bullet');
  assert.equal(shots.length,2);assert.equal(shots[0].start,0);assert.equal(shots[1].start,75);
  assert.equal(shots[1].x,bonus.x1*68+32);assert.equal(shots[1].y,bonus.y1*68+32);
  assert.equal(shots[1].x2,bonus.x2*68+32);assert.equal(shots[1].y2,bonus.y2*68+32);
  assert.equal(shots[1].followUp,true);assert.ok(shots[1].start+shots[1].duration<=EFFECT_LIMITS.impactDelayMs);
  renderer.destroy();
});

test('buff cues attach to actual recipients and stay smaller than a tile regardless of aura range',()=>{
  const {fx,event}=harness();
  event('shield',{unitType:'shieldcap',radius:3,targets:[{id:'source',x:1,y:8},{id:'ally',x:3,y:7}]});
  const shields=fx.pool.active.filter(e=>e.family==='unit-shield');
  assert.equal(shields.length,2);assert.deepEqual(shields.map(e=>[e.x,e.y]),[[100,576],[236,508]]);
  assert.ok(shields.every(e=>e.radius<=64*.4));assert.ok(!fx.pool.active.some(e=>e.family==='ring'));
  fx.clear();event('healing',{unitType:'medic',radius:3,targets:[{id:'one',x:2,y:7,amount:2},{id:'two',x:3,y:8,amount:1}]});
  assert.deepEqual(fx.pool.active.filter(e=>e.family==='healing-unit').map(e=>[e.x,e.y]),[[168,508],[236,576]]);
  fx.destroy();
});

test('small shield and poison attachments follow moving units, honor active aliases, and disappear on expiry',()=>{
  let location={x:2,y:3};const {fx}=harness({positionFor:()=>location});
  const unit={id:'moving',hp:5,x:1,y:2,comboShield:2,comboShieldUntil:2000,statusPoisonUntil:1800,suppressedUntil:900,slowUntil:1700};
  fx.syncFields({units:[unit]},1000);fx.step(10);
  assert.equal(fx.attachments.length,3);assert.ok(fx.attachments.every(e=>e.x===168&&e.y===236&&e.radius<=64*.4));
  location={x:3,y:4};fx.step(20);assert.ok(fx.attachments.every(e=>e.x===236&&e.y===304));
  assert.deepEqual(unitEffectLabels(unit,1000),['Shielded','Poisoned','Slowed']);
  fx.syncFields({units:[unit]},2100);assert.equal(fx.attachments.length,0);
  fx.destroy();
});

test('rail impacts, ward destruction and persistent special fields have separate visible shapes',()=>{
  const {fx,event}=harness();for(const kind of ['rail-impact','ward-break','lightning-field','blood-field','rupture'])assert.equal(event(kind,{id:kind,targets:[{id:'ward',x:3,y:3,amount:2}]}),true,kind);fx.step(0);
  for(const family of ['hit','debris','lightning-field','blood-field','rupture'])assert.ok(fx.pool.active.some(e=>e.family===family),family);
  const ctx=context();for(const effect of fx.pool.active)drawEffect(ctx,effect,180,{screenFlash:false});assert.ok(ctx.calls.some(call=>call[0]==='ellipse'));assert.ok(ctx.calls.some(call=>call[0]==='lineTo'));
  fx.syncFields({units:[],comboFields:[{id:'blood',kind:'blood-field',x:3,y:3,radius:1,sourceId:'dead-blood',until:2000}]},1500);assert.equal(fx.stats.persistent,1,'Blood fields persist after their explosive source dies');
  fx.syncFields({units:[],comboFields:[]},2100);assert.equal(fx.stats.persistent,0);fx.destroy();
});

test('rockets physically leave the attacker and end at the actual impact cell',()=>{
  const {fx,event}=harness();event('attack');const missile=fx.pool.active.find(e=>e.family==='missile'),blast=fx.pool.active.find(e=>e.family==='explosion');
  assert.ok(missile);assert.deepEqual({x:missile.x,y:missile.y},{x:100,y:576});
  assert.deepEqual({x:missile.x2,y:missile.y2},{x:304,y:236});
  const start=projectilePosition(missile,missile.start),middle=projectilePosition(missile,missile.start+missile.duration*.5),end=projectilePosition(missile,missile.start+missile.duration);
  assert.equal(start.x,missile.x);assert.equal(start.y,missile.y);assert.equal(end.x,missile.x2);assert.ok(Math.abs(end.y-missile.y2)<1e-6);
  assert.ok(middle.x>start.x&&middle.x<end.x);assert.ok(middle.y<(start.y+end.y)/2);
  assert.equal(blast.start,missile.start+missile.duration);assert.equal(blast.x,missile.x2);assert.equal(blast.y,missile.y2);assert.equal(blast.radius,68+64*.45);
  assert.equal(fx.impactDeadline('target'),blast.start);assert.ok(blast.start<=EFFECT_LIMITS.impactDelayMs);fx.destroy();
});

test('visual travel does not mutate authoritative coordinates, damage, event time or target lists',()=>{
  const {fx}=harness();const ev={id:'immutable',type:'combat',kind:'attack',unitType:'vrocket',sourceId:1,targetId:2,x:1,y:8,targetX:3,targetY:2,at:1250,amount:7,radius:1,targets:[{id:2,x:3,y:2,amount:7},{id:3,x:4,y:2,amount:3}]};
  const before=structuredClone(ev);Object.freeze(ev.targets[0]);Object.freeze(ev.targets[1]);Object.freeze(ev.targets);Object.freeze(ev);
  assert.equal(fx.event(ev),true);fx.step(100);fx.step(220);assert.deepEqual(ev,before);assert.equal(fx.impactDeadline(3),fx.impactDeadline(2));fx.destroy();
});

test('death and hit cues wait for projectile impact even when death arrives first',()=>{
  const {fx,event,setTime}=harness();event('death',{sourceId:'target',unitType:'rifleman'});event('hit');event('attack');
  const deadline=fx.impactDeadline('target');fx.step(deadline-1);
  assert.ok(!fx.pool.active.some(e=>e.family==='debris'&&e.start<deadline));
  setTime(deadline);fx.step(deadline);
  assert.ok(fx.pool.active.some(e=>e.family==='debris'&&e.start===deadline));fx.destroy();
});

test('shield absorption waits for the incoming projectile even when its event arrives first',()=>{
  const {fx,event,setTime}=harness();
  event('shield-hit',{unitType:'vguard',sourceId:'target',x1:4,y1:3,x2:4,y2:3});
  event('attack');const arrival=fx.impactDeadline('target');
  setTime(arrival-1);fx.step(arrival-1);
  assert.equal(fx.pool.active.some(effect=>effect.family==='unit-shield'),false);
  setTime(arrival);fx.step(arrival);
  assert.ok(fx.pool.active.some(effect=>effect.family==='unit-shield'&&effect.start===arrival));
  fx.destroy();
});

test('BloodBurst windup, damage-radius explosion and removal share the presentation deadline',()=>{
  const {fx,event}=harness();event('attack',{unitType:'blood'});const charge=fx.pool.active.find(e=>e.family==='charge'),blast=fx.pool.active.find(e=>e.family==='explosion');
  assert.ok(charge);assert.ok(blast.blood);assert.equal(blast.start,110);assert.equal(fx.impactDeadline('source'),110);assert.equal(fx.impactDeadline('target'),110);assert.equal(blast.x,304);fx.destroy();
});

test('storms contain moving cloud volumes, descending lightning and only real chain targets',()=>{
  const {fx,event}=harness();event('storm',{unitType:'storm',targets:[{id:'target',x:4,y:3},{id:'second',x:5,y:3},{id:'third',x:6,y:4}]});
  const cloud=fx.pool.active.find(e=>e.family==='cloud'),bolts=fx.pool.active.filter(e=>e.family==='lightning');assert.ok(cloud);assert.equal(bolts.length,3);
  assert.equal(bolts[0].x,cloud.x);assert.equal(bolts[0].y,cloud.y);assert.equal(bolts[0].x2,304);assert.equal(bolts[1].x2,372);assert.equal(bolts[2].x2,440);fx.destroy();
});

test('object slots recycle, decorative overload drops first and all completed effects clean up',()=>{
  const pool=new EffectPool(3),ids=new Set(pool.free);pool.acquire({priority:0,start:0,duration:10});pool.acquire({priority:1,start:0,duration:10});pool.acquire({priority:2,start:0,duration:10});
  assert.equal(pool.acquire({priority:0,start:0,duration:10}),null);assert.equal(pool.dropped,1);assert.ok(pool.acquire({priority:3,start:0,duration:10}));assert.equal(pool.active.length,3);
  pool.prune(10);assert.equal(pool.active.length,0);assert.equal(pool.free.length,3);assert.ok(pool.free.every(item=>ids.has(item)));
  const {fx,event,setTime}=harness();for(let i=0;i<300;i++)event('attack',{id:'rocket'+i});assert.ok(fx.stats.active<=EFFECT_LIMITS.high);assert.equal(fx.stats.active+fx.stats.free,EFFECT_LIMITS.objects);
  setTime(5000);fx.step(5000);assert.equal(fx.stats.active,0);fx.clear();assert.equal(fx.stats.seen,0);fx.destroy();
});

test('replayed IDs do not duplicate visual attacks and event history stays bounded',()=>{
  const {fx,event}=harness();event('attack',{id:'same'});const count=fx.stats.active;event('attack',{id:'same'});assert.equal(fx.stats.active,count);
  for(let i=0;i<1300;i++)event('impact',{id:'seen'+i});assert.equal(fx.stats.seen,EFFECT_LIMITS.seenEvents);fx.destroy();
});

test('low intensity and reduced motion retain primary impacts while limiting particles',()=>{
  const high=harness(),low=harness({intensity:'low'}),reduced=harness({reducedMotion:true});
  for(const h of [high,low,reduced])h.event('attack');
  assert.ok(low.fx.stats.active<high.fx.stats.active);assert.ok(reduced.fx.stats.active<low.fx.stats.active);assert.ok(reduced.fx.pool.active.some(e=>e.family==='missile'));
  for(let i=0;i<200;i++)low.event('attack',{id:'load'+i});assert.ok(low.fx.stats.active<=EFFECT_LIMITS.low);
  reduced.fx.configure({intensity:'off'});assert.equal(reduced.fx.stats.active,0);assert.equal(reduced.event('attack',{id:'off'}),true);assert.equal(reduced.fx.stats.active,0);
  for(const h of [high,low,reduced])h.fx.destroy();
});

test('reduced motion holds shockwave geometry still and removes lightning flicker',()=>{
  const effect={family:'ring',x:10,y:20,start:0,duration:500,radius:50,color:'#f00'},first=context(),last=context();drawEffect(first,effect,100,{reducedMotion:true});drawEffect(last,effect,350,{reducedMotion:true});
  assert.deepEqual(first.calls.find(c=>c[0]==='ellipse'),last.calls.find(c=>c[0]==='ellipse'));
  const bolt={...effect,family:'lightning',x2:50,y2:90,seed:123},a=context(),b=context();drawEffect(a,bolt,100,{reducedMotion:true});drawEffect(b,bolt,350,{reducedMotion:true});assert.deepEqual(a.calls.filter(c=>c[0]==='lineTo'),b.calls.filter(c=>c[0]==='lineTo'));
});

test('auto quality adapts to slow frames and persistent fields follow expiry/source life',()=>{
  const {fx,event,setTime}=harness({deviceMemory:8,cores:8});for(let i=0;i<25;i++){setTime(i*45);fx.step(i*45);}assert.equal(fx.quality,'low');event('attack');
  fx.syncFields({units:[{id:'caster',hp:5}],temporalFields:[{sourceId:'caster',x:3,y:3,radius:2,until:1500}],toxic:[{x:4,y:3,until:1600}]},1000);assert.equal(fx.stats.persistent,2);
  fx.syncFields({units:[],temporalFields:[{sourceId:'caster',x:3,y:3,radius:2,until:1500}],toxic:[{x:4,y:3,until:1600}]},1550);assert.equal(fx.stats.persistent,1);
  fx.syncFields({units:[],toxic:[{x:4,y:3,until:1600}]},1600);assert.equal(fx.stats.persistent,0);fx.destroy();
});

test('every effect family draws safely without downloaded art or canvas gradients',()=>{
  const {fx,event,setTime}=harness();for(const unitType of Object.keys(UNIT_EFFECTS)){event('attack',{id:unitType,unitType});event('ability',{id:unitType+'ability',unitType});}
  for(const rule of COMBOS)event('combo',{id:rule.id,comboId:rule.id,name:rule.name,targets:[{id:'extra',x:5,y:3}]});setTime(300);fx.step(300);
  const ctx=context();for(const effect of fx.pool.active)drawEffect(ctx,effect,Math.max(300,effect.start+40),{screenFlash:false,reducedMotion:false});assert.ok(ctx.calls.length>200);fx.destroy();
});

test('battle evolution artwork follows rank without changing permanent Forge tiers',()=>{
  const unit={forgeTier:1,battleRank:3,lvl:3};assert.equal(unitArtTier(unit),3);assert.equal(unit.forgeTier,1);assert.equal(unitArtTier({forgeTier:2,lvl:1}),2);assert.equal(unitArtTier({forgeTier:100,lvl:1}),3);
});

test('teleport afterimages reuse the loaded troop sheet and reduced motion omits ghosts',()=>{
  const art={image:{width:128,height:128},spec:{frameWidth:128,frameHeight:128,columns:1,pivot:[.5,.9375]},frame:0,flip:true};
  const {fx,event}=harness({artFor:()=>art});event('teleport',{unitType:'phaser'});const ghost=fx.pool.active.find(e=>e.family==='afterimage');assert.equal(ghost.art,art);
  const ctx=context();drawEffect(ctx,ghost,150,{});assert.ok(ctx.calls.some(c=>c[0]==='drawImage'));fx.clear();fx.configure({reducedMotion:true});event('teleport',{id:'reduced',unitType:'phaser'});assert.ok(!fx.pool.active.some(e=>e.family==='afterimage'));fx.destroy();
});

function fakeAudio(){
  const created=[];const param=()=>({value:0,setValueAtTime(){},setTargetAtTime(){},linearRampToValueAtTime(){},exponentialRampToValueAtTime(){}});
  const node=()=>({connect(){},disconnect(){},gain:param(),frequency:param(),Q:param(),start(){},stop(){},onended:null});
  const context={state:'suspended',sampleRate:8000,currentTime:0,destination:{},resume:async()=>{context.state='running';},close:async()=>{context.state='closed';},createGain:node,createBuffer:(_,length)=>({getChannelData:()=>new Float32Array(length)}),createBufferSource:()=>{const n=node();created.push(n);return n;},createOscillator:()=>{const n=node();created.push(n);return n;},createBiquadFilter:node};return {context,created};
}
test('audio requires a gesture, bounds simultaneous voices and per-kind mixing, and cleans up',async()=>{
  const {context,created}=fakeAudio();let time=0;const audio=createBattleAudio({now:()=>time,contextFactory:()=>context});assert.equal(audio.play('missile'),false);assert.equal(await audio.unlock(),true);
  assert.equal(audio.play('missile'),true);assert.equal(audio.play('missile'),false);assert.equal(audio.activeVoices,1);
  const kinds=['explosion','lightning','melee','bullet','magic','heal','swarm','toxic','shield'];for(let i=0;i<20;i++){time+=110;audio.play(kinds[i%kinds.length]);}
  assert.equal(audio.activeVoices,AUDIO_LIMITS.voices);created[0].onended();assert.equal(audio.activeVoices,AUDIO_LIMITS.voices-1);audio.configure({audio:false});assert.equal(audio.activeVoices,0);assert.equal(audio.play('missile'),false);audio.destroy();assert.equal(context.state,'closed');
});

test('missing or denied audio never interrupts battle effects',async()=>{
  const audio=createBattleAudio({contextFactory:()=>{throw new Error('Unavailable');}});assert.equal(await audio.unlock(),false);assert.equal(audio.play('explosion'),false);audio.configure({volume:.5});audio.clear();audio.destroy();
  const {fx,event}=harness();assert.equal(event('unknown'),false);assert.equal(fx.event({type:'combat',kind:'attack',x:NaN,y:3}),false);fx.destroy();assert.equal(event('attack'),false);
});
