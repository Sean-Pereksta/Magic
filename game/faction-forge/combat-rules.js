/** Deterministic combat decisions used by the browser and extracted simulator.
 * Bonuses add to one bounded multiplier; penetration removes real armor before
 * mitigation. Neither ranks nor combinations multiply the counter modifier. */
export const COMBAT_LIMITS = Object.freeze({activePerSide:Infinity,counterBonus:.25,maxCounterMultiplier:1.35,interceptRange:2,interceptCooldown:2200,shieldCapacity:4});
const groups={
  defender:['vguard','guardian','shieldcap','golem'],
  precision:['marksman','railbreaker'],
  artillery:['vrocket','storm','blood'],
  assassin:['miner','blade','viper','burrower'],
  support:['medic','wardmaker','architect','matron','signalhawk','chronoweaver','miragewarden','hiveherald','carrion'],
  magic:['phaser','storm','runebinder','chronoweaver'],
  corrosion:['spitter','defiler','carrion'],
  control:['suppressor','runebinder','emp','chronoweaver'],
  swarm:['hellspawn','blood'],
  ranged:['rifleman','vrocket','medic','suppressor','marksman','emp','railbreaker','signalhawk','phaser','storm','wardmaker','architect','runebinder','chronoweaver','miragewarden','roach','spitter','carrion','hiveherald']
};
export const COMBAT_ROLES=Object.freeze(Object.fromEntries(Object.entries(groups).map(([key,types])=>[key,Object.freeze(types)])));
/** Alternate simultaneous attack priority; array insertion order never grants
 * one starting seat the first hit throughout an entire battle. */
export function combatTurnOrder(units,now,field={}) {
  const first=Math.floor(Math.max(0,now-(field.startedAt||now))/180)%2;
  return [...units.filter(u=>u.p===first),...units.filter(u=>u.p!==first)];
}
const has=(group,u)=>groups[group].includes(u?.type);
const alive=u=>u&&u.hp>0;
const center=u=>({x:u.x+((u.size||1)-1)/2,y:u.y+((u.size||1)-1)/2});
const dist=(a,b,hooks={})=>hooks.distance?hooks.distance(a,b):Math.max(Math.abs(a.x-b.x),Math.abs(a.y-b.y));
const heavy=u=>(u.armor||0)>=2||u.size===2||has('defender',u);
const disabled=(u,now)=>(u.bindUntil||0)>now||(u.statusShockUntil||0)>now;
export function combatTags(type) {return Object.keys(groups).filter(group=>groups[group].includes(type));}
export function counterProfile(attacker,target) {
  let bonus=0,pen=0,reason='';
  const favorable=(value,label)=>{if(value>bonus){bonus=value;reason=label;}};
  if(has('precision',attacker)){
    pen=attacker.type==='marksman'?.65:.75;
    if(heavy(target))favorable(.25,'armor hunter');
    if(has('swarm',target))bonus=-.2;
  }
  if(has('magic',attacker)){pen=Math.max(pen,.5);if(heavy(target))favorable(.2,'magic against armor');}
  if(has('corrosion',attacker)){pen=Math.max(pen,.35);if(heavy(target)||has('support',target))favorable(.25,'corrosion and attrition');}
  if(has('artillery',attacker)&&has('swarm',target))favorable(.3,'blast against swarm');
  if(has('assassin',attacker)&&(has('ranged',target)||has('support',target)))favorable(.25,'backline flanker');
  if(attacker.type==='viper')pen=Math.max(pen,.25);
  if(has('defender',attacker)&&(has('assassin',target)||has('swarm',target)))favorable(.25,'intercepting melee');
  if(has('control',attacker)&&((target.range||1)<=1||(target.cd||0)>=1600))favorable(.25,'disrupting slow attacks');
  if(attacker.type==='marksman'&&attacker.evolution==='a')pen=.85;
  return {multiplier:Math.max(.8,Math.min(COMBAT_LIMITS.maxCounterMultiplier,1+bonus)),penetration:pen,counter:reason};
}
export function calculateCounterDamage(attacker={},target={},raw=0,{now=0,auraReduce=0,ignoreArmor=false,secondary=false}={}) {
  const profile=counterProfile(attacker,target),base=Math.max(0,Number(raw)||0);
  if(!base)return {...profile,damage:0,armorBlocked:0};
  const corroded=Math.max(target.corrosionUntil||0,target.statusAcidUntil||0)>now;
  const armor=Math.max(0,(Number(target.armor)||0)-(corroded?1:0));
  const penetration=ignoreArmor?1:Math.max(profile.penetration,attacker.armorPenetration||0);
  const remaining=ignoreArmor?0:Math.max(0,armor*(1-penetration)-(attacker.armorPierce||0)-(attacker.antiArmorRounds?1:0));
  // Secondary damage already has a per-attack budget; it receives penetration
  // but no extra counter multiplier and cannot produce a recursive burst.
  const multiplier=secondary?1:profile.multiplier;
  const mitigation=ignoreArmor?0:Math.min(1,Math.max(0,auraReduce))+(target.rangedReduce&&(attacker.range||1)>1?Math.min(1,target.rangedReduce):0);
  let damage=Math.max(1,Math.round(base*multiplier-remaining-mitigation));
  if(target.legendary&&target.type==='roach'&&target.hp<=target.maxhp*.5)damage=Math.max(1,damage-1);
  return {...profile,multiplier,penetration,damage,armorBlocked:remaining};
}
/** Guards intercept one shot from their facing lane, never every backline hit.
 * Artillery splash, penetration and flanking angles remain reliable answers. */
export function interceptTarget(attacker,target,units,now,hooks={}) {
  if(!target||has('defender',target)||attacker.illusion||attacker.trueDamage||attacker.type==='railbreaker'||attacker.type==='marksman')return target;
  const a=center(attacker),b=center(target),dx=b.x-a.x,dy=b.y-a.y,len=Math.hypot(dx,dy)||1;
  const guards=units.filter(g=>alive(g)&&g.p===target.p&&g.id!==target.id&&has('defender',g)&&!disabled(g,now)&&now>=(g.nextInterceptAt||0)&&dist(g,target,hooks)<=COMBAT_LIMITS.interceptRange).filter(g=>{
    const c=center(g),projection=((c.x-a.x)*dx+(c.y-a.y)*dy)/len;
    const perpendicular=Math.abs((c.x-a.x)*dy-(c.y-a.y)*dx)/len;
    return projection>0&&projection<len&&perpendicular<=.8&&dist(attacker,g,hooks)<dist(attacker,target,hooks);
  }).sort((x,y)=>dist(x,target,hooks)-dist(y,target,hooks)||x.x-y.x||x.y-y.y);
  const guard=guards[0];if(!guard)return target;
  guard.nextInterceptAt=now+COMBAT_LIMITS.interceptCooldown;
  guard.interceptUntil=now+400;return guard;
}
export function chooseTacticalTarget(u,units,now,hooks={}) {
  const candidates=units.filter(v=>alive(v)&&v.p!==u.p);
  const range=(u.rangeDebuffUntil||0)>now?Math.max(1,(u.range||1)-1):(u.range||1);
  const allies=units.filter(v=>alive(v)&&v.p===u.p&&!v.illusion);
  const scores=candidates.map(v=>{
    const d=dist(u,v,hooks),inside=d<=range;
    let score=-(d*1.7)-Math.max(0,d-range)*2+(inside?6:0)+(1-v.hp/Math.max(1,v.maxhp))*2;
    score+=(counterProfile(u,v).multiplier-1)*12;
    if(has('precision',u)&&heavy(v))score+=7+Math.min(3,v.armor||0);
    if(has('assassin',u)){
      if(has('ranged',v))score+=9;
      if(has('support',v))score+=3;
      if(has('defender',v))score-=5;
      if(units.some(g=>alive(g)&&g.p===v.p&&has('defender',g)&&dist(g,v,hooks)<=2))score-=2;
    }
    if(has('artillery',u))score+=candidates.filter(other=>other.id!==v.id&&dist(v,other,hooks)<=1).length*3;
    if(has('defender',u))score+=allies.filter(a=>has('ranged',a)&&dist(a,v,hooks)<=2&&dist(a,u,hooks)<=3).length*6;
    if(has('control',u)&&((v.range||1)<=1||(v.cd||0)>=1600))score+=4;
    if(v.markedBySide===u.p&&(v.markedUntil||0)>now&&has('precision',u))score+=3;
    if(v.illusion)score+=!u.splash1&&!u.chain3&&d<=range?4:-8;
    if(v.id===u.targetId)score+=.6;
    if(has('ranged',u)&&!has('precision',u))score+=Math.min(2,(v.dmg||0)/4);
    return {v,score};
  });
  scores.sort((a,b)=>b.score-a.score||dist(u,a.v,hooks)-dist(u,b.v,hooks)||a.v.x-b.v.x||(u.p===0?a.v.y-b.v.y:b.v.y-a.v.y)||a.v.type.localeCompare(b.v.type));
  return scores[0]?.v||null;
}
export function chooseCombatMove(u,target,units,hooks={}) {
  const candidates=[[u.x,u.y],[u.x+1,u.y],[u.x-1,u.y],[u.x,u.y+1],[u.x,u.y-1]];
  const ranged=has('ranged',u)&&(u.range||1)>2,flank=has('assassin',u);
  const nearest=probe=>Math.min(...units.filter(v=>alive(v)&&v.p!==u.p).map(v=>dist(probe,v,hooks)));
  const desired=ranged?Math.max(2,Math.min(u.range||3,has('support',u)?3:4)):1;
  const ranked=candidates.filter(([x,y])=>x===u.x&&y===u.y||hooks.canMoveTo?.(u,units,x,y)).map(([x,y])=>{
    const probe={...u,x,y},d=dist(probe,target,hooks),danger=nearest(probe);
    let score=ranged?Math.abs(d-desired)*2:d*3;
    if(ranged&&danger<=1)score+=8;
    score+=units.filter(v=>alive(v)&&v.p===u.p&&v.id!==u.id&&dist(probe,v,hooks)<=1).length*.3;
    if(flank)score+=Math.min(x,(hooks.width||8)-1-x)*.55;
    if(has('defender',u)){
      const protectedAlly=units.filter(v=>alive(v)&&v.p===u.p&&has('ranged',v)).sort((a,b)=>(b.dmg||0)-(a.dmg||0))[0];
      if(protectedAlly&&dist(u,protectedAlly,hooks)<=3&&dist(target,protectedAlly,hooks)<=4)score+=dist(probe,protectedAlly,hooks)*1.2;
    }
    return {x,y,score};
  });
  ranked.sort((a,b)=>a.score-b.score||Math.abs(a.x-u.x)-Math.abs(b.x-u.x)||a.x-b.x||(u.p===0?a.y-b.y:b.y-a.y));
  const best=ranked[0];return best&&(best.x!==u.x||best.y!==u.y)?best:null;
}
export function keepFiringDistance(u,units,now,hooks={}) {
  if(!has('ranged',u)||(u.range||1)<=2||u.illusion||disabled(u,now)||now<(u.nextKiteAt||0))return false;
  const threat=units.filter(v=>alive(v)&&v.p!==u.p&&dist(u,v,hooks)<=1).sort((a,b)=>dist(u,a,hooks)-dist(u,b,hooks))[0];
  if(!threat)return false;
  const tile=chooseCombatMove(u,threat,units,hooks);u.nextKiteAt=now+1400;
  if(!tile||dist({...u,...tile},threat,hooks)<=dist(u,threat,hooks))return false;
  u.x=tile.x;u.y=tile.y;return true;
}
export function chooseFlankLanding(u,target,units,maxDistance,hooks={}) {
  const tiles=[];
  for(let dx=-maxDistance;dx<=maxDistance;dx++)for(let dy=-maxDistance;dy<=maxDistance;dy++){
    const x=u.x+dx,y=u.y+dy,probe={...u,x,y};
    if((dx===0&&dy===0)||!hooks.canMoveTo?.(u,units,x,y)||dist(u,probe,hooks)>maxDistance)continue;
    const d=dist(probe,target,hooks);
    if(d>=dist(u,target,hooks))continue;
    const exposure=units.filter(v=>alive(v)&&v.p!==u.p&&has('defender',v)&&dist(v,probe,hooks)<=1).length;
    tiles.push({x,y,score:d*4+exposure*3+Math.min(x,(hooks.width||8)-1-x)*.2});
  }
  tiles.sort((a,b)=>a.score-b.score||a.x-b.x||(u.p===0?a.y-b.y:b.y-a.y));return tiles[0]||null;
}
