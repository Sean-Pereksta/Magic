const {test}=require('node:test');
const assert=require('node:assert/strict');
const path=require('node:path');
const {CombatEffects,COMBAT_QUALITY,ATTACK_FAMILIES,SpriteRuntime,floatingLabels}=require('../animation.js');
const {simulation}=require('./helpers/expansion-harness.cjs');
const animation=path.resolve(__dirname,'../animation.js');
const unit=(id=1)=>({id,type:'dragoon',kind:'unit',faction:0,x:4,y:4,px:4,py:4,hp:100,maxHp:100,shield:80,abilityReadyAt:0,attackReadyAt:0});
function paint(){const calls=[];return{calls,ctx:new Proxy({globalAlpha:1,save(){},restore(){}},{get(target,key){if(key in target)return target[key];return(...args)=>calls.push([key,...args])}})}}
const screen=(x,y)=>({x:x*48,y:y*48});

test('every major attack family has an explicit presentation while preserving actual launch and contact clocks',()=>{
  for(const type of ['rifleman','marauder','hellion','siegeTank','goliath','battlecruiser','dragoon','zealot','templar','sentry','immortal','disruptor','carrier','hydralisk','roach','ravager','lurker','mutalisk','defiler','ultralisk'])assert.ok(ATTACK_FAMILIES[type],type);
  const effects=new CombatEffects(),e=unit();e.attackReadyAt=1200;
  assert.equal(effects.prepare(e,1000),false);assert.equal(effects.prepare(e,1050),true);assert.equal(effects.events[0].end,1200);
  e.projectileAnim={startAt:1200,endAt:1780,x:4.5,y:4.5,tx:8.5,ty:4.5,style:'energyOrb',color:'#badfff'};
  assert.equal(effects.attack(e,1199),false);assert.equal(effects.attack(e,1200),true);assert.equal(effects.events.find(e=>e.kind==='release').start,1200);
  e.attackReadyAt=0;assert.equal(effects.attack(e,1210),false,'a cooldown refresh cannot re-release an existing projectile');
  const {ctx,calls}=paint();assert.equal(effects.drawTrails(ctx,e.projectileAnim,1199,{worldToScreen:screen}),false);assert.equal(effects.drawTrails(ctx,e.projectileAnim,1500,{worldToScreen:screen}),true);assert.ok(calls.some(c=>c[0]==='stroke'));
  assert.equal(effects.drawTrails(ctx,e.projectileAnim,1780,{worldToScreen:screen}),false);effects.observe(e,1800);assert.equal(effects.events.some(e=>e.kind==='impact'),false,'projectile completion never invents a hit');
  assert.equal(e.projectileAnim.endAt,1780);assert.equal(e.attackReadyAt,0);
});

test('actual shield, armor and flesh damage produces distinct bounded feedback, including heavy hits and deaths',()=>{
  const effects=new CombatEffects(),e=unit();effects.observe(e,1000);e.shield=50;e.lastDamagedAt=1100;
  effects.impact(e,1100,{shieldLoss:30,attacker:{type:'immortal'}});effects.observe(e,1100);
  assert.equal(effects.events.length,1,'local hook and observed snapshot deduplicate');assert.equal(effects.events[0].defense,'shield');assert.equal(effects.events[0].family,'lance');assert.equal(effects.events[0].scale,1.5);
  effects.impact({...e,hp:95,shield:0,lastDamagedAt:1200},1200,{loss:5,armored:true,attacker:{type:'rifleman'}});
  effects.impact({...e,id:2,hp:95,shield:0,lastDamagedAt:1200},1200,{loss:5,attacker:{type:'roach'}});
  assert.equal(effects.events.at(-2).defense,'armor');assert.equal(effects.events.at(-1).defense,'flesh');assert.equal(effects.events.at(-1).family,'acid');
  assert.equal(effects.impact(e,1200,{loss:0,shieldLoss:0}),false);assert.equal(effects.impact({...e,id:3},1200,{loss:50,visible:false}),false);
  effects.death(e,1200);assert.equal(effects.events.at(-1).kind,'death');assert.equal(effects.death(e,1200),false);
});

test('remote Blink snaps every caster once and uses synchronized departure silhouettes without a walking segment',()=>{
  const runtime=new SpriteRuntime(),troops=[unit(1),{...unit(2),x:5,px:5},{...unit(3),y:5,py:5}];
  runtime.beginFrame(troops,1000);for(const e of troops){e.x-=2;e.lastAbilityId='phaseStep';e.abilityReadyAt=15000;e.moveReadyAt=2500}
  runtime.beginFrame(troops,1100,{remote:true});
  assert.equal(runtime.combat.events.filter(e=>e.kind==='teleportOut').length,3);assert.equal(runtime.combat.events.filter(e=>e.kind==='teleportIn').length,3);
  assert.ok(runtime.combat.events.every(e=>e.start===1100));for(const e of troops){const state=runtime.states.get(e.id);assert.equal(state.segment,null);assert.equal(state.moving,false);assert.equal(e.px,e.x);assert.equal(e.moveReadyAt,2500);assert.equal(e.abilityReadyAt,15000)}
  runtime.beginFrame(JSON.parse(JSON.stringify(troops)),1150,{remote:true});assert.equal(runtime.combat.events.length,6,'repeated snapshot does not restart a cast');
  runtime.beginFrame(troops,1800,{remote:true});assert.equal(runtime.combat.events.length,0);assert.equal(runtime.combat.particles.length,0);
});

test('cooldown extensions and resets do not replay casts; explicit cast markers recognize a new ability once',()=>{
  const runtime=new SpriteRuntime(),e={...unit(),lastAbilityId:'phaseStep',abilityReadyAt:15000,lastAbilityCastAt:1000};runtime.beginFrame([e],2000);
  for(let i=1;i<=5;i++){e.abilityReadyAt+=30;runtime.beginFrame([e],2000+i*100,{remote:true})}
  assert.equal(runtime.combat.events.length,0,'Temporal Distortion recovers timers without a new cast');assert.equal(runtime.states.get(e.id).event,null);
  e.lastAbilityCastAt=2700;e.abilityReadyAt=16000;e.x-=2;runtime.beginFrame([e],2700,{remote:true});assert.equal(runtime.combat.events.filter(e=>e.kind==='teleportIn').length,1);
  e.abilityReadyAt+=30;runtime.beginFrame([e],2800,{remote:true});assert.equal(runtime.combat.events.filter(e=>e.kind==='teleportIn').length,1);
  const legacy=new CombatEffects(),old={...unit(),lastAbilityId:'phaseStep',abilityReadyAt:15000};legacy.observe(old,2000);old.abilityReadyAt+=30;legacy.observe(old,2100);assert.equal(legacy.events.length,0,'legacy snapshots also ignore an extension of an active cooldown');
});

test('large desktop and mobile battles reuse effect pools, cap fields and discard finished local state',()=>{
  for(const quality of Object.keys(COMBAT_QUALITY)){
    const effects=new CombatEffects(quality);for(let i=0;i<10000;i++)effects.emit('battle:'+i,'impact',i%20,Math.floor(i/20)%20,1000,{family:i%2?'artillery':'spores',defense:i%3?'armor':'shield',scale:1.5});
    assert.equal(effects.events.length,effects.budget.events);assert.equal(effects.particles.length,effects.budget.particles);assert.ok(effects.seen.size<=effects.budget.events*6);
    const eventObjects=new Set(effects.events),particleObjects=new Set(effects.particles);effects.update(2500);assert.equal(effects.events.length,0);assert.equal(effects.particles.length,0);
    for(let i=0;i<10000;i++)effects.emit('second:'+i,'impact',1,1,3000,{family:'plasma'});
    assert.ok(effects.events.every(e=>eventObjects.has(e)),'events reuse their bounded pool');assert.ok(effects.particles.every(e=>particleObjects.has(e)),'particles reuse their bounded pool');
    let atlasDraws=0;const atlasPaint=paint();effects.draw(atlasPaint.ctx,3100,{worldToScreen:screen,atlas:()=>{atlasDraws++;return true}});assert.ok(atlasDraws>0&&atlasDraws<=effects.budget.atlas);atlasDraws=0;effects.draw(atlasPaint.ctx,3300,{worldToScreen:screen,atlas:()=>{atlasDraws++;return true}});assert.equal(atlasDraws,0,'impact atlases stop after 260 ms while pooled aftermath fades');
    const {ctx}=paint(),fields=Array.from({length:100},(_,i)=>({id:i,kind:'psionicStorm',x:i%10,y:Math.floor(i/10),size:5,startAt:3000,endAt:8000}));
    assert.equal(effects.drawFields(ctx,fields,[],3500,{worldToScreen:screen}),effects.budget.fields,'cap also applies inside multi-tile fields');
    const hidden=paint();assert.equal(effects.draw(hidden.ctx,3500,{visible:()=>false,worldToScreen:screen}),0);assert.equal(hidden.calls.length,0);
    assert.equal(effects.drawFields(hidden.ctx,fields,[],3500,{visible:()=>false,worldToScreen:screen}),0);assert.equal(hidden.calls.length,0);
    effects.setQuality('low');for(let i=0;i<200;i++)effects.emit('quality-change:'+i,'impact',1,1,3600,{family:'plasma'});assert.ok(effects.events.length<=COMBAT_QUALITY.low.events);assert.ok(effects.particles.length<=COMBAT_QUALITY.low.particles);effects.reset();assert.equal(effects.stats().remembered,0);assert.equal(effects.stats().observed,0);
  }
});

test('bile telegraphs the actual radius and never detonates until the authoritative warning resolves',()=>{
  const effects=new CombatEffects(),effect={id:'bile_1',kind:'bileWarning',x:3,y:3,size:1,startAt:1000,impactAt:2500,endAt:2900},draw=paint();
  effects.drawFields(draw.ctx,[effect],[],1700,{worldToScreen:screen});assert.equal(effects.events.length,0);assert.ok(draw.calls.some(c=>c[0]==='arc'&&c[3]===48*1.35));
  effects.drawFields(draw.ctx,[effect],[],2500,{worldToScreen:screen});assert.equal(effects.events.length,0,'an elapsed clock alone is not a simulated hit');
  effect.resolved=true;effects.drawFields(draw.ctx,[effect],[],2500,{worldToScreen:screen});assert.equal(effects.events.length,1);assert.equal(effects.events[0].start,2500);effects.drawFields(draw.ctx,[effect],[],2550,{worldToScreen:screen});assert.equal(effects.events.length,1);
});

test('damage labels aggregate repeated numbers, stay above units and preserve the simulation feedback array',()=>{
  const labels=Array.from({length:600},(_,i)=>({x:i%2+.5,y:4,text:'-6',color:'#ff8792',life:.8,max:1})),before=JSON.stringify(labels),grouped=floatingLabels(labels,24);
  assert.equal(grouped.length,2);assert.ok(grouped.every(label=>label.text==='-1800'));assert.ok(grouped.every(label=>label.y<4));assert.equal(JSON.stringify(labels),before);
  const spaced=labels.map((e,i)=>({...e,x:i+.5}));assert.equal(floatingLabels(spaced,24).length,24);
});

test('real simulation hooks emit impacts only when projectile damage resolves and keep cosmetics out of snapshots',()=>{
  const run=simulation('frontier',{preloadScripts:[animation]});const result=run.plain(`(()=>{const attacker=addEntity('unit','rifleman',0,4,4),target=addEntity('unit','zealot',1,7,4);spriteRuntime.reset();updateSpritePresentation();const hp=target.hp,shield=target.shield;fireAt(attacker,target);const initial=spriteRuntime.combat.events.filter(e=>e.kind==='impact').length,impactAt=attacker.projectileAnim.endAt;testClock=impactAt-1;resolveProjectileImpacts();const before=spriteRuntime.combat.events.filter(e=>e.kind==='impact').length;testClock=impactAt;resolveProjectileImpacts();const after=spriteRuntime.combat.events.filter(e=>e.kind==='impact'),state=serializeEntity(target);updateSpritePresentation();return{initial,before,after:after.map(e=>({start:e.start,defense:e.defense})),impactAt,hp,shield,hpAfter:target.hp,shieldAfter:target.shield,duplicate:spriteRuntime.combat.events.filter(e=>e.kind==='impact').length,serialized:Object.keys(state)}})()`);
  assert.equal(result.initial,0);assert.equal(result.before,0);assert.equal(result.after.length,1);assert.equal(result.after[0].start,result.impactAt);assert.equal(result.after[0].defense,'shield');assert.ok(result.shieldAfter<result.shield);assert.equal(result.hpAfter,result.hp);assert.equal(result.duplicate,1);assert.ok(!result.serialized.some(k=>/combat|particle|cosmetic/i.test(k)));
});
