/** Shared deterministic combo rules. All time, damage and placement come from
 * the authoritative simulation; presentation consumes the returned events. */
import {calculateCounterDamage} from './combat-rules.js';
export const COMBO_LIMITS = Object.freeze({ radius:2, targets:5, damagePerTarget:4,
  controlMs:450, fieldsPerSide:6, fieldTicks:3, healPerActivation:3, enhancementLevel:2 });
const rule = (id,name,faction,units,description,trigger,cooldown,counterplay) => Object.freeze({id,name,faction,units:Object.freeze(units),description,trigger,cooldown,counterplay});
export const COMBOS = Object.freeze([
  rule('static-tempest','Static Tempest','Aesir',['storm','runebinder'],'Lightning chains through enemies held by Rune Binder.','bind',4200,'Spread apart or eliminate the Storm Seer.'),
  rule('thunderclap','Thunderclap','Aesir',['storm','guardian'],'A Guardian slam calls down a lightning shockwave.','slam',5000,'Keep ranged troops away from the frontline.'),
  rule('lightning-echo','Lightning Echo','Aesir',['storm','phaser'],'Phasers echo lightning from recently charged targets.','charged-hit',2600,'Interrupt the caster or isolate charged targets.'),
  rule('arcane-fortress','Arcane Fortress','Aesir',['wardmaker','architect','golem'],'Linked wards shield the Golem and retaliate after repeated hits.','fortress',5500,'Attack from multiple angles or destroy the builders.'),
  rule('blood-chain','Corrosive Chain Reaction','Helions',['blood','spitter'],'A nearby Spitter primes a BloodBurst detonation with a bounded corrosive blast.','detonate',4200,'Separate explosive units and eliminate the Spitter before contact.'),
  rule('underground-detonation','Underground Detonation','Helions',['burrower','blood'],'A BloodBurst blast exploits a recently ambushed enemy.','ambushed-detonation',3800,'Protect the backline and separate support troops.'),
  rule('plague-explosion','Plague Explosion','Helions',['defiler','blood'],'Detonating near toxic enemies creates a short damaging plague cloud.','toxic-detonation',4500,'Move away from toxic ground and focus the Defiler.'),
  rule('brood-detonation','Brood Detonation','Helions',['matron','blood','hellspawn'],'Detonations send nearby Hell Spawn into a brief surge.','swarm-detonation',4500,'Explosive attacks punish clustered swarms.'),
  rule('predators-feast',"Predator's Feast",'Helions',['viper','carrion','siphonleech'],'Viper strikes against wounded prey send controlled healing to a nearby leech.','wounded-hit',3200,'Protect wounded troops and apply wound to the leech.'),
  rule('guided-barrage','Guided Barrage','Vanir',['vrocket','signalhawk'],'Rockets strike marked targets with a guided secondary blast.','marked-rocket',4200,'Dive the fragile Signal Hawk or spread the backline.'),
  rule('suppression-bombardment','Suppression Bombardment','Vanir',['vrocket','suppressor'],'Rockets bombard clusters recently suppressed by allied fire.','suppressed-rocket',4000,'Assassins can reach the artillery backline.'),
  rule('electromagnetic-barrage','Electromagnetic Barrage','Vanir',['emp','vrocket'],'Rocket hits in an EMP zone release an electrical pulse.','disrupted-rocket',4600,'Separate troops and eliminate the EMP Grenadier.'),
  rule('fortified-artillery','Fortified Artillery','Vanir',['vguard','shieldcap','vrocket'],'Damage absorbed by the protected frontline earns a charged artillery strike.','absorbed-damage',6000,'Bypass the guard and pressure the Rockets directly.'),
  rule('precision-execution','Precision Execution','Vanir',['signalhawk','marksman','railbreaker'],'Marksman fire at a marked enemy coordinates a piercing rail follow-up.','marked-precision',5000,'Use swarms or interrupt the slow Railbreaker.')
]);
export const COMBO_BY_ID = Object.freeze(Object.fromEntries(COMBOS.map(c=>[c.id,c])));
const alive = u => u && u.hp>0 && !u.illusion;
const sideKey = p => p===0?'p0':'p1';
const dist = (a,b,hooks={}) => hooks.distance ? hooks.distance(a,b) : Math.max(Math.abs(a.x-b.x),Math.abs(a.y-b.y));
const rank = u => Math.max(1,Math.min(3,Number(u.lvl||u.battleRank)||1));
const disabled = (u,now) => (u.statusShockUntil||0)>now || (u.bindUntil||0)>now;
function levelFor(members,enhanced) {
  const ranks=members.map(rank).sort((a,b)=>b-a);
  return ranks[0]>=3 && ranks.slice(1).some(r=>r>=2) ? 3 : enhanced || ranks.every(r=>r>=2) ? 2 : 1;
}
/** Only relevant faction combinations are shown; duplicate requirements count. */
export function comboStatus(roster=[],faction,enhancements=[]) {
  const counts={},ranks={};
  for(const row of roster) { counts[row.type]=(counts[row.type]||0)+Math.max(0,row.count||0);ranks[row.type]=Math.max(ranks[row.type]||0,rank(row)); }
  return COMBOS.filter(c=>c.faction===faction&&c.units.some(k=>counts[k])).map(c=>{
    const used={},missing=[];for(const key of c.units){used[key]=(used[key]||0)+1;if(used[key]>(counts[key]||0))missing.push(key);}
    const level=missing.length?0:levelFor(c.units.map(type=>({lvl:ranks[type]})),enhancements.includes(c.id));
    return {...c,missing,level,status:missing.length?'missing':level===1?'ready':'active'};
  });
}
export function combatEvent(kind,u,target,now,extra={}) {
  return {type:'combat',kind,unitType:u.type,rank:rank(u),battleRank:rank(u),sourceId:u.id,targetId:target?.id,
    x:u.x,y:u.y,targetX:target?.x??u.x,targetY:target?.y??u.y,x1:u.x,y1:u.y,x2:target?.x??u.x,y2:target?.y??u.y,at:now,duration:900,...extra};
}
function emit(hooks,kind,u,t,now,extra={}) {hooks.event?.(combatEvent(kind,u,t,now,extra));}
function membersFor(st,u,c,now) {
  if(!st.field.units.some(v=>v.id===u.id))return null;
  const deck=st.decks?.[sideKey(u.p)];
  if(deck&&!c.units.every(type=>deck.troops?.includes(type)))return null;
  const used=new Set(),members=[];
  for(const type of c.units){
    const member=(type===u.type&&!used.has(u.id)?u:null)||st.field.units.find(v=>alive(v)&&v.p===u.p&&v.type===type&&!used.has(v.id)&&!disabled(v,now));
    if(!member || member!==u&&!alive(member))return null;used.add(member.id);members.push(member);
  }
  return members;
}
function activate(st,u,t,id,now,hooks,perform) {
  const c=COMBO_BY_ID[id],members=membersFor(st,u,c,now);if(!members)return false;
  const field=st.field,cooldowns=field.comboCooldowns||(field.comboCooldowns={}),key=id==='blood-chain'?`${u.p}:${id}:${u.id}`:`${u.p}:${id}`;
  if(now<(cooldowns[key]||0))return false;
  const level=levelFor(members,(st.comboEnhancements?.[sideKey(u.p)]||[]).includes(id));
  const result=perform(level,members);if(!result)return false;
  cooldowns[key]=now+c.cooldown;
  emit(hooks,'combo',u,t,now,{comboId:id,name:c.name,level,radius:result.radius||0,targets:result.targets||[],duration:result.duration||1100,...result});
  return true;
}
/** A bounded secondary hit; deliberately does not call afterAttack recursively. */
export function comboHit(st,u,v,raw,now,hooks={},ignoreArmor=false) {
  if(!alive(v))return 0;
  let amount=Math.max(1,Math.min(COMBO_LIMITS.damagePerTarget,Math.round(raw)));
  // The triggering attack owns the budget even when a supporting Storm Seer
  // or Railbreaker is the damage source. No source can bypass that cap.
  const budget=st.field._secondaryBudget;
  if(budget){if(budget.remaining<=0)return 0;amount=Math.min(amount,budget.remaining);}
  amount=calculateCounterDamage(u,v,amount,{now,ignoreArmor,secondary:true,auraReduce:hooks.auraReduce?.(v,st.field)||0}).damage;
  if((u.range||1)>1&&hooks.wardDamage)amount=hooks.wardDamage(u,v,amount,st.field);
  const before=v.hp;hooks.applyDamage?hooks.applyDamage(v,amount,st):v.hp-=amount;
  const actual=Math.max(0,Math.min(before,before-v.hp));
  if(budget)budget.remaining-=actual;
  return actual;
}
function area(st,u,t,raw,now,hooks,{radius=1,maxTargets=COMBO_LIMITS.targets,ignoreArmor=false,control=0}={}) {
  const targets=st.field.units.filter(v=>alive(v)&&v.p!==u.p&&dist(t,v,hooks)<=Math.min(COMBO_LIMITS.radius,radius))
    .sort((a,b)=>dist(t,a,hooks)-dist(t,b,hooks)).slice(0,maxTargets);
  return targets.map(v=>{
    const amount=comboHit(st,u,v,raw,now,hooks,ignoreArmor);
    if(alive(v)&&control&&now>=(v.comboControlReadyAt||0)){v.slowUntil=Math.max(v.slowUntil||0,now+Math.min(control,COMBO_LIMITS.controlMs));v.comboControlReadyAt=now+1800;}
    return {id:v.id,x:v.x,y:v.y,amount};
  });
}
export function noteComboAmbush(u,t,now) {
  if(t&&u.type==='burrower'&&dist(u,t)<=2){t.ambushedBySide=u.p;t.ambushedUntil=now+3500;}
}
export function noteComboDamage(victim,damage,st,now,hooks={}) {
  if(damage<=0)return;
  if(['vguard','shieldcap'].includes(victim.type)){
    const meter=st.field.artilleryCharge||(st.field.artilleryCharge={p0:0,p1:0});
    meter[sideKey(victim.p)]=Math.min(40,(meter[sideKey(victim.p)]||0)+Math.min(8,damage));
  }
  if(victim.type==='golem')victim.fortressHits=Math.min(8,(victim.fortressHits||0)+1);
}
function addField(st,u,t,now,kind,damage,radius=1,duration=2400) {
  const fields=st.field.comboFields||(st.field.comboFields=[]);
  if(fields.filter(f=>f.p===u.p&&f.until>now).length>=COMBO_LIMITS.fieldsPerSide)return false;
  const serial=st.field.comboFieldSerial=(st.field.comboFieldSerial||0)+1;
  fields.push({id:`field-${u.p}-${serial}`,p:u.p,sourceId:u.id,unitType:u.type,x:t.x,y:t.y,kind,damage,radius:Math.min(radius,2),until:now+duration,nextTick:now+800,tickMs:800,ticks:0});return true;
}
export function afterComboAttack(u,t,now,st,hooks={}) {
  if(!t || u.illusion)return;
  if(u.type==='storm'){t.lightningChargedBy=u.p;t.lightningChargedUntil=now+2600;}
  if(u.type==='suppressor'&&alive(t)){t.suppressedBySide=u.p;t.suppressedUntil=now+Math.min(1800,u.suppressDur||900);}
  if(u.type==='emp'){for(const v of st.field.units)if(alive(v)&&v.p!==u.p&&dist(t,v,hooks)<=1){v.disruptedBySide=u.p;v.disruptedUntil=now+2200;}}
  if(u.type==='defiler'){for(const v of st.field.units)if(alive(v)&&v.p!==u.p&&dist(t,v,hooks)<=1){v.toxicBySide=u.p;v.toxicUntil=now+3500;}}
  if(u.type==='runebinder'&&(t.bindUntil||0)>now)activate(st,u,t,'static-tempest',now,hooks,(level,members)=>{
    const seer=members.find(v=>v.type==='storm');if(dist(seer,t,hooks)>5)return false;
    const targets=area(st,seer,t,level+1,now,hooks,{radius:level===3?2:1,maxTargets:level+2});
    for(const hit of targets){const v=st.field.units.find(v=>v.id===hit.id);if(v){v.lightningChargedBy=u.p;v.lightningChargedUntil=now+2600;}}
    return {targets,radius:level===3?2:1,duration:1800};
  });
  if(u.type==='guardian')activate(st,u,t,'thunderclap',now,hooks,(level,members)=>{
    if(dist(members.find(v=>v.type==='storm'),t,hooks)>5)return false;
    return {targets:area(st,u,t,level+1,now,hooks,{radius:1,control:250+level*60}),radius:1};
  });
  if(u.type==='phaser'&&t.lightningChargedBy===u.p&&(t.lightningChargedUntil||0)>now)activate(st,u,t,'lightning-echo',now,hooks,level=>({targets:area(st,u,t,level,now,hooks,{radius:1,maxTargets:level+1}),radius:1}));
  if(u.type==='blood'){
    activate(st,u,t,'blood-chain',now,hooks,(level,members)=>{
      const spitter=members.find(v=>v.type==='spitter');if(dist(spitter,t,hooks)>6)return false;
      const targets=area(st,u,t,Math.min(3,level+1),now,hooks,{radius:1});
      for(const hit of targets){const enemy=st.field.units.find(v=>v.id===hit.id);if(enemy)enemy.corrosionUntil=Math.max(enemy.corrosionUntil||0,now+2200);}
      return {targets,radius:1,charged:true};
    });
    if(st.field.units.some(v=>v.p!==u.p&&v.ambushedBySide===u.p&&(v.ambushedUntil||0)>now&&dist(t,v,hooks)<=1))activate(st,u,t,'underground-detonation',now,hooks,level=>({targets:area(st,u,t,level+1,now,hooks,{radius:1}),radius:1}));
    if(st.field.units.some(v=>v.p!==u.p&&((v.toxicBySide===u.p&&(v.toxicUntil||0)>now)||(v.statusPoisonUntil||0)>now)&&dist(t,v,hooks)<=1))activate(st,u,t,'plague-explosion',now,hooks,level=>{
      if(!addField(st,u,t,now,'toxic-cloud',1,1,2400))return false;
      const targets=area(st,u,t,level,now,hooks,{radius:1});for(const hit of targets){const v=st.field.units.find(v=>v.id===hit.id);if(v)v.woundUntil=Math.max(v.woundUntil||0,now+1800);}
      return {targets,radius:1,duration:2400};
    });
    activate(st,u,t,'brood-detonation',now,hooks,(level,members)=>{
      const swarms=st.field.units.filter(v=>alive(v)&&v.p===u.p&&v.type==='hellspawn'&&dist(u,v,hooks)<=3).slice(0,8);
      if(!swarms.length)return false;for(const v of swarms)v.broodSurgeUntil=Math.max(v.broodSurgeUntil||0,now+2200);
      let summoned=false;if(level>=2){const matron=members.find(v=>v.type==='matron');summoned=!!hooks.spawnSummon?.(st,matron,'hellspawn');}
      return {targets:swarms.map(v=>({id:v.id,x:v.x,y:v.y,amount:0})),radius:2,summoned};
    });
  }
  if(u.type==='viper'&&u.lastAttackDamage!==0&&((t.woundUntil||0)>now||t.hp<=t.maxhp*.5))activate(st,u,t,'predators-feast',now,hooks,(level,members)=>{
    const leech=members.find(v=>v.type==='siphonleech');if(!alive(leech)||dist(u,leech,hooks)>3||leech.hp>=leech.maxhp)return false;
    const amount=Math.min(leech.maxhp-leech.hp,Math.floor(Math.min(3,level+1)*(hooks.healScale?.(leech,now)??((leech.woundUntil||0)>now?.5:1))));
    if(!amount)return false;leech.hp+=amount;return {targets:[{id:leech.id,x:leech.x,y:leech.y,amount}],healing:true,radius:0};
  });
  if(u.type==='vrocket'){
    // Explosive artillery is the practical counter to layered magical wards.
    // Damage is tied to this impact, never to a particle or repeated frame.
    const broken=[];
    for(const ward of st.field.wards||[])if(ward.p!=null&&ward.p!==u.p&&ward.hp>0&&ward.until>now&&dist(t,ward,hooks)<=1){
      const amount=Math.min(ward.hp,rank(u)>=3?3:2);ward.hp-=amount;broken.push({x:ward.x,y:ward.y,amount});
    }
    if(broken.length)emit(hooks,'ward-break',u,t,now,{targets:broken,radius:1,duration:700});
    if(t.markedBySide===u.p&&(t.markedUntil||0)>now)activate(st,u,t,'guided-barrage',now,hooks,level=>({targets:area(st,u,t,level+1,now,hooks,{radius:1,maxTargets:level+1}),radius:1}));
    if(t.suppressedBySide===u.p&&(t.suppressedUntil||0)>now)activate(st,u,t,'suppression-bombardment',now,hooks,level=>({targets:area(st,u,t,level+1,now,hooks,{radius:1,control:250}),radius:1}));
    if(t.disruptedBySide===u.p&&(t.disruptedUntil||0)>now)activate(st,u,t,'electromagnetic-barrage',now,hooks,level=>({targets:area(st,u,t,level+1,now,hooks,{radius:1,ignoreArmor:true,control:350}),radius:1}));
    if((st.field.artilleryCharge?.[sideKey(u.p)]||0)>=12)activate(st,u,t,'fortified-artillery',now,hooks,(level,members)=>{
      if(members.filter(v=>v!==u).some(v=>dist(u,v,hooks)>4))return false;
      st.field.artilleryCharge[sideKey(u.p)]=0;return {targets:area(st,u,t,level+2,now,hooks,{radius:level===3?2:1}),radius:level===3?2:1};
    });
  }
  if(u.type==='marksman'&&t.markedBySide===u.p&&(t.markedUntil||0)>now)activate(st,u,t,'precision-execution',now,hooks,(level,members)=>{
    const rail=members.find(v=>v.type==='railbreaker');if(dist(rail,t,hooks)>(rail.range||7))return false;
    return {targets:area(st,rail,t,level+1,now,hooks,{radius:level>=2?1:0,maxTargets:level>=2?2:1,ignoreArmor:true}),radius:0,followSourceId:rail.id,followX:rail.x,followY:rail.y};
  });
}
export function tickCombos(st,now,hooks={}) {
  const field=st.field;
  field.comboFields=(field.comboFields||[]).filter(f=>f.until>now&&f.ticks<COMBO_LIMITS.fieldTicks);
  for(const f of field.comboFields){if(now<f.nextTick)continue;f.nextTick=now+f.tickMs;f.ticks++;
    const source=field.units.find(v=>v.id===f.sourceId)||{id:f.sourceId,p:f.p,type:f.unitType,lvl:1,x:f.x,y:f.y,range:1};
    const targets=area(st,source,f,f.damage,now,hooks,{radius:f.radius});emit(hooks,f.kind,source,f,now,{targets,radius:f.radius,duration:800});
  }
  for(const golem of field.units.filter(v=>alive(v)&&v.type==='golem'&&!disabled(v,now))){
    activate(st,golem,golem,'arcane-fortress',now,hooks,(level,members)=>{
      if(members.some(v=>dist(golem,v,hooks)>4))return false;
      const wards=(field.wards||[]).filter(w=>w.p===golem.p&&dist(golem,w,hooks)<=3).slice(0,4);
      if(!wards.length)return false;
      for(const w of wards){w.hp=Math.min(8,(w.hp||1)+1);w.fortressUntil=now+3000;}
      golem.comboShield=Math.min(4,level+1);golem.comboShieldUntil=now+3000;
      let targets=[];if(level>=2&&(golem.fortressHits||0)>=3){targets=area(st,golem,golem,level,now,hooks,{radius:2,maxTargets:3});golem.fortressHits=0;}
      return {targets,radius:2,shield:golem.comboShield,links:members.map(v=>({id:v.id,x:v.x,y:v.y})),duration:1800};
    });
  }
}
