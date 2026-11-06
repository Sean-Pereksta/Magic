/* Bramblebound road simulation. No DOM or timers; the caller supplies elapsed seconds. */
(() => {
  'use strict';
  const root = globalThis;
  const C = () => root.Catalog;
  const P = () => root.Progression;
  const L = () => root.Legacy;
  const WALK = 3.2, ACTIVE = 4, PACK = 400, WAVE_RULES=2;
  const utilityStyles=new Set(['heal','shield','stun','freeze']);
  const dropChances=Object.freeze({normal:.24,guardian:.48});
  const xpNeeded = level => 14+Math.round((level-1)*5.5);
  const OLD_REGIONS=8, WAVES_PER_REGION=4;
  const ailments=()=>({corrosion:0,snare:0,hex:0,burn:0,chill:0,shock:0});
  const counter=(r,key)=>r.active.some(it=>C().items[it.id].counter===key);
  const expeditionXpRate = 0.22;
  function expeditionDefaults(){return {routeEpoch:1,level:1,xp:0,draft:[],skills:{},abilityTimers:{},foes:0,waveSize:0,
    nextEnemyUid:1,enemies:[],targetCursor:0,venomClock:0,waveRules:WAVE_RULES,bestAtStart:null,deathCause:null,
    ailments:ailments(),routeRegions:C().regions.length,routeCircuit:1,routeHistory:[]};}
  const perks = {
    heart: {name:'Heartwood', description:'Start each journey with 14 more maximum health.', max:100},
    claw: {name:'Iron grip', description:'All items deal 8% more base damage per rank.', max:100},
    pace: {name:'Trail memory', description:'Unlock 2×, 3×, then 4× speed on cleared rounds.', max:3}
  };
  const ok = message => ({ok:true,message});
  const fail = message => ({ok:false,message});
  const owns = (obj, key) => Object.prototype.hasOwnProperty.call(obj,key);
  function freshProfile() { return {version:1,worldRegions:C().regions.length,points:0,best:0,deaths:0,routeMarks:Array(C().regions.length*WAVES_PER_REGION).fill(0),seenItems:[],seenEnemies:[],perks:{heart:0,claw:0,pace:0},run:null}; }
  function perkCost(p,key) {
    if(L())return L().cost(p,key);
    if (!owns(perks,key) || p.perks[key]>=perks[key].max) return null;
    const rank=p.perks[key];
    return key==='pace' ? [2,6,12][rank] : 1+rank*2+Math.floor(rank*rank/4);
  }
  function perkPreview(p,key){
    if(!owns(perks,key))return null;
    const rank=p.perks[key],value=n=>key==='heart'?`${88+14*n} max HP`:key==='claw'?`${100+8*n}% relic damage`:`${1+n}× replay pace`;
    return {current:value(rank),next:rank<perks[key].max?value(rank+1):null};
  }
  function awardBreakdown(r){return [
    ...(r.legacyAwards||[]),{label:'Journey completed',amount:1},{label:'Waves cleared',amount:r.kills},{label:'Guardian waves cleared',amount:Math.floor(r.kills/4)}
  ];}
  function discover(arr,id){if(!arr.includes(id))arr.push(id);}
  const maxSpeed = p => 1+p.perks.pace;
  function routeInfo(r,round=r.round){
    const epoch=r.routeEpoch||1, historical=round<epoch;
    const past=(r.routeHistory||[]).filter(s=>s.epoch<=round).at(-1);
    const regionsPerCircuit=historical?(past?.regions||OLD_REGIONS):(r.routeRegions||C().regions.length);
    const origin=historical?(past?.epoch||1):epoch, length=regionsPerCircuit*WAVES_PER_REGION;
    const lap=Math.floor((round-origin)/length), circuitStart=origin+lap*length;
    const region=Math.floor((round-circuitStart)/WAVES_PER_REGION),regionStart=circuitStart+region*WAVES_PER_REGION;
    const circuit=(historical?(past?.circuit||1):(r.routeCircuit||1+(epoch-1)/(OLD_REGIONS*WAVES_PER_REGION)))+lap;
    return {region,circuit,waveInRegion:round-regionStart+1,regionsPerCircuit,circuitStart,regionStart,regionEnd:regionStart+WAVES_PER_REGION-1};
  }
  const routeSlot=r=>r.region*WAVES_PER_REGION+(r.round-1)%WAVES_PER_REGION;
  const isReplay=(p,r=p.run)=>!!r && p.routeMarks[routeSlot(r)]>=r.round;

  function rng(r) {
    let x = r.seed; x ^= x<<13; x ^= x>>>17; x ^= x<<5; r.seed=x>>>0;
    return r.seed/4294967296;
  }
  const pick = (r,arr) => arr[Math.floor(rng(r)*arr.length)];
  function makeItem(r,id,level=1,cd=0) { return {uid:r.nextUid++,id,level,cd}; }
  function levelText(l) { return String(l); }
  function itemPower(p,it) {
    const n=it.level-1;
    const d=C().items[it.id];
    return d.power*(C().rarities[d.rarity]?.powerScale||1)*(1+.31*n+.009*n*n)*(1+.08*p.perks.claw)*(1+(d.recipe?(p.run?.legacy?.ranks.mastercraft||0)*.03:0))*(1+(p.run?.legacy?.ranks.celestial_canopy||0)*.05);
  }
  const utilityMilestones=it=>1+(it.level>=3?.4:0)+(it.level>=5?.2:0)+(it.level>=10?.65:0);
  const utilityMultiplier=(p,d)=>p.run?P().utilityMultiplier(p.run,d):1;
  function itemUtility(p,it){return itemPower(p,it)*utilityMultiplier(p,C().items[it.id])*utilityMilestones(it);}
  function itemDuration(p,it){
    const d=C().items[it.id];
    return Math.min(8,(d.duration||.75)*Math.pow(itemPower(p,it)/d.power,.25)*Math.pow(utilityMultiplier(p,d),.25)*utilityMilestones(it)*(d.element==='frost'?1+.03*(p.run?.legacy?.masteries.frost||0):1));
  }
  const itemCooldown = (it,r) => r ? P().cooldown(r,C().items[it.id]) : C().items[it.id].cooldown;
  function itemComparison(p,it){
    const d=C().items[it.id],r=p.run,power=itemPower(p,it);
    const stats={damage:0,heal:0,shield:0,lifesteal:0,stun:0,freeze:0,chill:0,cooldown:itemCooldown(it,r)};
    const healing=value=>r?P().healing(r,value):value;
    if(utilityStyles.has(d.style)){
      if(d.style==='heal')stats.heal=healing(itemUtility(p,it));
      else if(d.style==='shield')stats.shield=itemUtility(p,it);
      else stats[d.style]=itemDuration(p,it);
      return stats;
    }
    // Compare one activation against a neutral foe. Include unconditional build
    // bonuses and direct follow-ups; conditional hits and damage over time are
    // described by effect icons rather than a misleading total damage estimate.
    const reach=r?P().upgrades.long_arm.value(P().rankOf(r,'long_arm')):0;
    const stoneShield=d.element==='stone'?1+.04*(r?.legacy?.masteries.stone||0):1;
    const initial=power*(utilityMultiplier(p,d)/(d.style==='guard'?stoneShield:1)+reach);
    stats.damage=initial*(1+(d.style==='rapid'?.23:0)+(d.special==='aftershock'?.45:0)+(it.level>=3?.4:0)+(it.level>=10?.65:0));
    if(d.style==='mend')stats.heal=power*.55;
    if(d.style==='guard')stats.shield=power*.60;
    if(d.style==='drain')stats.lifesteal=.23;
    if(d.style==='shock')stats.stun=.44;
    if(d.style==='chill')stats.chill=2.1;
    if(d.element==='frost')stats.chill=Math.max(stats.chill,1.1);
    if(d.element==='spirit')stats.shield+=initial*.045;
    if(it.level>=5)stats.shield+=1+.12*power;
    if(d.special==='leech')stats.lifesteal+=.18;
    if(d.special==='renewal'){stats.heal+=initial*.13;stats.shield+=initial*.12;}
    if(d.special==='permafrost'){stats.stun=Math.max(stats.stun,.9);stats.chill=2.1;}
    stats.shield*=stoneShield;
    if(d.element==='spirit')stats.lifesteal*=1+.04*(r?.legacy?.masteries.spirit||0);
    stats.heal=healing(stats.heal);stats.lifesteal=healing(stats.lifesteal);
    return stats;
  }
  function start(p) {
    if (p.run && p.run.phase!=='dead') return fail('A journey is already in progress.');
    const hp=88+p.perks.heart*14;
    const r={...expeditionDefaults(),bestAtStart:p.best,phase:'walk',round:1,region:0,hp,maxHp:hp,shield:0,distance:0,time:0,kills:0,
      active:[],pack:[],enemy:null,lastDrop:null,speed:maxSpeed(p),paceChoice:maxSpeed(p),
      walkLeft:WALK,lastAward:0,nextUid:1,seed:((Math.random()*4294967295)>>>0)||1,venom:0};
    r.speed=isReplay(p,r)?r.paceChoice:1;
    const starters=C().starter;
    r.active.push(makeItem(r,starters[0]),makeItem(r,starters[1],1,.48));
    r.pack.push(makeItem(r,starters[0]));
    const recipe=Object.values(C().items).find(x=>x.recipe?.includes(starters[0]) && !x.recipe.includes(starters[1]) && x.recipe.every(id=>!C().items[id].recipe));
    const partner=recipe?.recipe.find(id=>id!==starters[0]) || Object.keys(C().items).find(id=>!starters.includes(id));
    r.pack.push(makeItem(r,partner));
    if(C().items.dewglass_vial)r.pack.push(makeItem(r,'dewglass_vial'));
    for(const zone of [r.active,r.pack]) zone.forEach((it,slot)=>{it.slot=slot;discover(p.seenItems,it.id);});
    p.run=r;
    if(L()){L().start(p,r,(id)=>makeItem(r,id));P().onStart?.(r,[]);draftBonus(r);}
    return ok('A new journey begins.');
  }
  function spawn(p,ev) {
    const r=p.run, guardian=r.round%4===0;
    // A solo encounter teaches the road. Escorts join the first guardian, then
    // additional lanes enter as the first circuit progresses.
    const count = r.round===1?1:r.round<4?2:r.round<7?3:r.round<11?4:5;
    r.waveRules=WAVE_RULES;
    const pool=C().enemies.filter(e=>e.region===r.region && !e.boss);
    r.waveSize=count;r.enemies=[];r.targetCursor=0;r.phase='fight';
    for(let lane=0;lane<count;lane++){
      const d=guardian&&lane===0?C().enemies.find(e=>e.region===r.region&&e.boss):pool.splice(Math.floor(rng(r)*pool.length),1)[0];
      const e=buildEnemy(r.round,d,count,r.waveRules);
      e.uid=r.nextEnemyUid++;e.lane=lane;e.attackProgress=lane*.11;
      r.enemies.push(e);discover(p.seenEnemies,d.id);
    }
    if(L())L().recordRegion(p,r);P().onSpawn?.(r,ev);
    r.enemy=r.enemies[0];add(ev,'round','leaf',1,r.enemy.name,'encounter');
  }
  function buildEnemy(round,d,count,rules) {
    const boss=d.boss, n=round-1, classic=rules===1;
    // Legacy fights retain their saved rules until the wave ends.
    const vitality=classic?22+6*n+.17*n*n:22+6.4*n+.72*n*n;
    const strike=classic?5+.66*n+.004*n*n:4.8+.72*n+.053*n*n;
    const healthSpread=1+(classic?.67:.43)*(count-1), attackSpread=1+(classic?.72:.44)*(count-1);
    const traitPower=d.trait==='brute'?1.32:d.trait==='swarm'?.82:1;
    // Compare imported enemy stats exactly, including historical FP order.
    const maxHp=Math.round(vitality*(boss?(classic?1.42:1.56):1)/healthSpread);
    const damage=strike*(boss?(classic?1.25:1.32):1)*traitPower/attackSpread;
    const tempo=d.trait==='swift'?.67:d.trait==='brute'?1.24:d.trait==='swarm'?.78:1;
    const period=(classic?(boss?2.2:2.65):(boss?2.12:2.62)/(1+.22*n/(n+9)))*tempo;
    return {id:d.id,name:d.name,species:d.species,color:d.color,trait:d.trait,affliction:d.affliction||null,weak:d.weak,resist:d.resist,boss,hp:maxHp,maxHp,damage,period,attackProgress:0,burn:0,poison:0,chill:0,stun:0,freeze:0,statusClock:0,ward:d.trait==='warded'||d.affliction==='bulwark'?1:0,swings:0,echoes:[],rooted:0,fracture:0,haunt:0,hauntTime:0,leafHits:0,frostHits:0,stormHits:0};
  }
  function find(r,uid) { return r.active.concat(r.pack).find(i=>i.uid===uid); }
  function previewCombine(p,a,b) {
    const r=p.run;
    if(!r || r.phase==='dead' || a===b) return fail('Choose two separate items.');
    const x=find(r,a),y=find(r,b); if(!x||!y) return fail('Both items must be carried.');
    return previewPair(x,y);
  }
  function previewPair(x,y){
    if(!x||!y||x.uid===y.uid)return fail('Choose two separate items.');
    if(x.id===y.id){
      if(x.level!==y.level) return fail('Matching items must be the same level.');
      return {ok:true,id:x.id,level:x.level+1,message:`Upgrade to ${C().items[x.id].name} · Lv ${levelText(x.level+1)}`};
    }
    const d=C().items[C().combo(x.id,y.id)];
    if(!d) return fail('These items have no fusion recipe.');
    const level=Math.max(x.level,y.level)+1;
    return {ok:true,id:d.id,level,message:`Fuse into ${d.name} · Lv ${levelText(level)}`};
  }
  function next(p) {
    const r=p.run;
    const previous=routeInfo(r);
    if(r.routeRegions<C().regions.length&&r.round===previous.circuitStart+r.routeRegions*WAVES_PER_REGION-1){
      r.routeHistory.push({epoch:r.routeEpoch,regions:r.routeRegions,circuit:r.routeCircuit});
      r.routeEpoch=r.round+1;r.routeRegions=C().regions.length;r.routeCircuit=previous.circuit+1;
    }
    r.round++; r.region=routeInfo(r).region; r.phase='walk'; r.walkLeft=WALK;
    r.enemy=null;r.enemies=[];r.waveSize=0;r.waveRules=WAVE_RULES;r.venomClock=0; r.venom=0;r.ailments=ailments();r.speed=isReplay(p,r)?r.paceChoice:1;
  }
  // Arrays contain occupied items only. Slot numbers preserve intentional gaps.
  const slotAt=(r,zone,slot)=>r[zone].find(it=>it.slot===slot);
  function firstOpen(zone){let i=0;while(zone.some(x=>x.slot===i))i++;return i;}
  function remove(arr,x){arr.splice(arr.indexOf(x),1);}
  function combineInto(p,x,y,dest,slot,res){
    const r=p.run, level=res.level, cd=Math.min(Math.max(x.cd,y.cd),C().items[res.id].cooldown);
    remove(r.active.includes(x)?r.active:r.pack,x);remove(r.active.includes(y)?r.active:r.pack,y);
    const it=makeItem(r,res.id,level,cd);it.slot=slot;dest.push(it);discover(p.seenItems,it.id);
    if(x.id!==y.id&&(r.legacy?.ranks.fusion_fortune||0)&&rng(r)<r.legacy.ranks.fusion_fortune*.025&&r.pack.length<PACK){const copy=makeItem(r,rng(r)<.5?x.id:y.id);copy.slot=firstOpen(r.pack);r.pack.push(copy);}
    return {...ok(res.message),operation:x.id===y.id?'upgrade':'fuse',uid:it.uid};
  }
  function transfer(p,x,zone,slot,merge=true) {
    const r=p.run,cap=zone==='active'?ACTIVE:zone==='pack'?PACK:0;
    if(!Number.isInteger(slot)||slot<0||slot>=cap)return fail('Choose a valid gear slot.');
    const from=r.active.includes(x)?r.active:r.pack,to=r[zone],y=slotAt(r,zone,slot);
    if(x===y)return {...ok('Item is already there.'),operation:'none',uid:x.uid};
    const res=merge&&y?previewCombine(p,x.uid,y.uid):null;
    if(from===r.active && to===r.pack && from.length===1 && (!y || res?.ok))return fail('Keep at least one item equipped.');
    if(res?.ok)return combineInto(p,x,y,to,slot,res);
    const old=x.slot;
    if(from!==to){remove(from,x);if(y){remove(to,y);from.push(y);}to.push(x);}
    if(y)y.slot=old;x.slot=slot;
    return {...ok(y?'Gear swapped.':zone==='active'?'Item equipped.':'Item stowed.'),operation:y?'swap':'move',uid:x.uid};
  }
  function act(p,action,...args) {
    const r=p.run, [a,b]=args;
    if(action==='legacyBuy')return L()?L().purchase(p,a):fail('Camp unavailable.');
    if(action==='respec')return L()?L().respec(p):fail('Camp unavailable.');
    if(action==='buy') {
      if(L())return L().purchase(p,a);
      if(r&&r.phase!=='dead') return fail('Visit camp between journeys to spend points.');
      const cost=perkCost(p,a);
      if(cost===null) return fail('That upgrade is unavailable.');
      if(p.points<cost) return fail(`Requires ${cost} points.`);
      p.points-=cost; p.perks[a]++; return ok(`${perks[a].name} upgraded.`);
    }
    if(!r||r.phase==='dead') return fail('Begin a journey first.');
    if(action==='rerollDraft'){if(!r.draft.length||!(r.draftRerolls>0))return fail('No rerolls remain.');r.draftRerolls--;r.draft=P().offer(r,()=>rng(r),3,'reroll');return ok('Destiny redrawn.');}
    if(action==='banishTalent'){if(!r.draft.includes(a)||!(r.banishes>0))return fail('Choose an offered talent and an available banish.');r.banishes--;r.banished??=[];r.banished.push(a);r.draft=P().offer(r,()=>rng(r),3,'reroll');return ok('Talent banished for this expedition.');}
    if(action==='echoTalent'){if(!r.draft.includes(a)||!(r.echoesLeft>0))return fail('Choose an offered talent and an available echo.');r.echoesLeft--;r.echoChoices??=[];r.echoChoices.push({id:a,drafts:2});return ok('Talent remembered for two drafts.');}
    if(action==='chooseUpgrade') {
      if(!r.draft.includes(a))return fail('Choose one of the three offered upgrades.');
      P().apply(r,a);if(r.draftIsBonus){r.bonusTalentsChosen=(r.bonusTalentsChosen||0)+1;r.draftIsBonus=false;}r.draft=[];if(!draftBonus(r))levelUp(r,[]);
      return ok(P().upgrades[a].name+' upgraded.');
    }
    if(action==='speed') {
      if(!Number.isInteger(a)||a<1||a>maxSpeed(p)) return fail('Unlock this pace at camp.');
      r.paceChoice=a; r.speed=isReplay(p,r)?a:1;
      return ok(r.speed===a?`Pace set to ${a}×.`:`${a}× remembered for cleared rounds.`);
    }
    const x=find(r,a);if(!x)return fail('Choose a carried item.');
    const equipped=r.active.includes(x), from=equipped?r.active:r.pack;
    if(action==='drop')return transfer(p,x,b,args[2]);
    if(action==='combine') {
      const res=previewCombine(p,a,b);if(!res.ok)return res;
      const y=find(r,b), dest=equipped||r.active.includes(y)?r.active:r.pack;
      return combineInto(p,x,y,dest,dest.includes(x)?x.slot:y.slot,res);
    }
    if(action==='swap') {
      const y=find(r,b);if(!y||r.active.includes(y)===equipped)return fail('Select one belt and one pack item.');
      return transfer(p,x,equipped?'pack':'active',y.slot,false);
    }
    if(action==='equip'&&!equipped){
      if(r.active.length>=ACTIVE)return fail('Belt full. Swap with an equipped item.');
      return transfer(p,x,'active',firstOpen(r.active));
    }
    if(action==='stow'&&equipped){
      if(r.pack.length>=PACK)return fail('Pack full.');
      return transfer(p,x,'pack',firstOpen(r.pack));
    }
    if(action==='discard') {
      if(equipped&&r.active.length===1)return fail('Keep at least one item equipped.');
      remove(from,x);return ok('Item discarded.');
    }
    return fail('That action is unavailable.');
  }
  function add(events,kind,element,strength,label,pattern,target){
    const event={kind,element,strength,label,pattern,age:0};
    if(target){event.targetUid=target.uid;event.enemyId=target.id;event.lane=target.lane;}
    events.push(event);return event;
  }
  function draftBonus(r){if(!(r.bonusTalentCredits>0)||r.draft.length||r.phase==='dead')return false;r.bonusTalentCredits--;r.draftIsBonus=true;r.draft=P().offer(r,()=>rng(r),3,'bonus');return true;}
  function levelUp(r,ev){
    const threshold=xpNeeded(r.level);
    if(r.phase==='dead'||r.draft.length)return;if(draftBonus(r))return;if(r.xp<threshold)return;
    r.xp-=threshold;r.level++;r.draft=P().offer(r,()=>rng(r),3);
    add(ev,'level','spirit',r.level,'Level '+r.level,'level');
  }
  function matches(tag,element) {return Array.isArray(tag)?tag.includes(element):tag===element;}
  function heal(r,n){const before=r.hp,total=P().healing(r,n);r.hp=Math.min(r.maxHp,r.hp+total);P().afterHealing?.(r,total,r.hp-before);return r.hp-before;}
  function hurt(r,n,kind,enemy,ev=[]){
    if(kind==='venom'||kind==='scorch')n*=(1-(r.legacy?.ranks.venom_resistance||0)*.05)*(1-(r.legacy?.ranks.thickened_bark||0)*.015);
    const absorbed=Math.min(n,r.shield),damage=n-absorbed,before=r.hp;
    r.shield-=absorbed;r.hp=Math.max(0,before-damage);
    P().onHurt?.(r,n,enemy,ev,absorbed);
    if(before>0 && r.hp===0){r.deathCause={kind,damage};if(enemy)r.deathCause.enemyId=enemy.id;P().survive?.(r,r.enemies,ev);}
    return damage;
  }
  function secondary(ev,e,amount,element,pattern,it){
    const loss=Math.min(e.hp,Math.max(0,amount));e.hp=Math.max(0,e.hp-loss);
    Object.assign(add(ev,'special',element,it.level,String(Math.ceil(loss)),pattern,e),{itemId:it.id,special:pattern,specialId:pattern,amount:loss});return loss;
  }
  function elemental(p,it,e,damage,ev){
    const r=p.run,el=C().items[it.id].element,others=r.enemies.filter(x=>x!==e&&x.hp>0);
    const status=(label,pattern)=>Object.assign(add(ev,'status',el,it.level,label,pattern,e),{itemId:it.id});
    if(el==='leaf'){
      e.poison=Math.max(e.poison,Math.min(e.maxHp*.08,e.poison+damage*.07*(1+.05*(r.legacy?.masteries.leaf||0))));
      e.leafHits=(e.leafHits+1)%3;if(e.leafHits===0){e.rooted=.8;status('Rooted','root');}
    }else if(el==='fire'){
      e.burn=Math.max(e.burn,damage*.22*(1+.05*(r.legacy?.masteries.fire||0)));
      if(e.poison>0){e.poison*=.5;secondary(ev,e,damage*.18,el,'wildfire',it);for(const foe of others.slice(0,2)){foe.burn=Math.max(foe.burn,damage*.12);secondary(ev,foe,damage*.12,el,'wildfire',it);}}
    }else if(el==='frost'){
      e.chill=Math.max(e.chill,1.1);e.frostHits=(e.frostHits+1)%3;
      if(e.frostHits===0){e.stun=Math.max(e.stun,.65);status('Frozen','freeze');}
    }else if(el==='storm'){
      e.attackProgress=Math.max(0,e.attackProgress-.025);e.stormHits=(e.stormHits+1)%3;
      if(e.stormHits===0){e.stun=Math.max(e.stun,.22);status('Charged arc','chain');for(const foe of others.slice(0,2))secondary(ev,foe,damage*.32,el,'chain',it);}
      if(e.chill>0&&others.length){secondary(ev,others[0],damage*.18,el,'superconduct',it);others[0].stun=Math.max(others[0].stun,.2);}
    }else if(el==='stone'){
      e.fracture=Math.min(5+2*P().rankOf(r,'fracture_point'),e.fracture+1);e.attackProgress=Math.max(0,e.attackProgress-.06);status('Fracture '+e.fracture,'fracture');
    }else if(el==='spirit'){
      e.hauntTime=4;e.haunt++;
      r.shield=Math.min(r.maxHp*.4,r.shield+damage*.045);
      if(e.haunt>=Math.max(1,3-P().rankOf(r,'haunt_amplifier'))){e.haunt=0;e.echoes.push({delay:.48,damage:damage*.35*(1+.04*(r.legacy?.masteries.spirit||0)),el,pattern:'soulmark'});status('Soul echo','haunt');}
    }
  }
  function special(p,it,e,damage,removed,ev){
    const r=p.run,d=C().items[it.id],id=d.special;if(!id)return;
    const others=r.enemies.filter(x=>x!==e&&x.hp>0),strike=(target,n)=>secondary(ev,target,n,d.element,id,it);
    let triggered=true;
    if(id==='ricochet'){if(others.length)strike(others[0],damage*.38);else triggered=false;}
    if(id==='execute'){if(e.hp>0&&e.hp/e.maxHp<.3)strike(e,damage*.65);else triggered=false;}
    if(id==='rupture'){if(e.burn||e.poison||e.chill||e.haunt){strike(e,damage*.4);e.burn*=.5;e.poison*=.5;}else triggered=false;}
    if(id==='chain')for(const foe of others.slice(0,2))strike(foe,damage*.28);
    if(id==='leech'){const n=heal(r,removed*.18*(1+(d.element==='spirit'?.04*(r.legacy?.masteries.spirit||0):0)));Object.assign(add(ev,'heal',d.element,1,'+'+Math.ceil(n),'drain',e),{itemId:it.id,amount:n,sourceUid:e.uid});}
    if(id==='wardbreak'){e.ward=0;e.fracture=Math.min(5+2*P().rankOf(r,'fracture_point'),e.fracture+2);}
    if(id==='bloom')for(const foe of others.slice(0,2)){foe.poison=Math.max(foe.poison,Math.min(foe.maxHp*.08,foe.poison+damage*.12*(1+.05*(r.legacy?.masteries.leaf||0))));strike(foe,damage*.1);}
    if(id==='permafrost'){e.chill=2.1;e.stun=Math.max(e.stun,.9);}
    if(id==='aftershock')e.echoes.push({delay:.6,damage:damage*.45,el:d.element,pattern:'aftershock'});
    if(id==='soulmark'){e.haunt=Math.min(2,e.haunt+1);e.hauntTime=4;}
    if(id==='renewal'){heal(r,damage*.13);r.shield=Math.min(r.maxHp*.4,r.shield+damage*.12*(d.element==='stone'?1+.04*(r.legacy?.masteries.stone||0):1));}
    if(id==='overload'){
      it.charges=((it.charges||0)+1)%3;
      if(it.charges===0){for(const foe of [e,...others].slice(0,3))strike(foe,damage*.42);}else triggered=false;
    }
    if(triggered)Object.assign(add(ev,'special',d.element,it.level,id==='permafrost'?'Deep freeze':id==='wardbreak'?'Ward broken':id==='soulmark'?'Soulmarked':'',id,e),{itemId:it.id,special:id,specialId:id});
  }
  function cleanse(r,it,ev){
    const d=C().items[it.id];if(d.counter!=='cleanse')return;
    if(r.venom>0||Object.values(r.ailments).some(v=>v>0)){
      r.venom=0;r.ailments=ailments();Object.assign(add(ev,'status',d.element,it.level,'Cleansed','cleanse'),{itemId:it.id});
    }
  }
  function fire(p,it,e,ev) {
    const r=p.run,d=C().items[it.id],el=d.element,style=d.style;
    cleanse(r,it,ev);
    // Dedicated support and control activations never enter the damage path:
    // no elemental hits, offensive signatures, hit procs, echoes or novas.
    if(utilityStyles.has(style)){
      let event;
      if(style==='heal'){
        const gain=heal(r,itemUtility(p,it));
        event=add(ev,'heal',el,it.level,gain>0?(gain<.5?'+<1':`+${Math.round(gain)}`):'Health full','heal');
        event.amount=gain;
      }else if(style==='shield'){
        const before=r.shield;r.shield=Math.min(r.maxHp*.4,before+itemUtility(p,it));
        const gain=r.shield-before;
        event=add(ev,'status',el,it.level,gain>0?(gain<.5?'Shield +<1':`Shield +${Math.round(gain)}`):'Shield full','guard');
        event.amount=gain;
      }else{
        const duration=itemDuration(p,it);
        e[style]=Math.max(e[style]||0,duration);
        event=add(ev,'status',el,it.level,style==='freeze'?'Frozen':'Stunned',style==='freeze'?'freeze':'shock',e);
        event.duration=duration;
      }
      event.itemId=it.id;event.sourceUid=it.uid;
      P().onActivation?.(r,r.enemies,e,d,0,ev,()=>rng(r));
      it.cd=itemCooldown(it,r);return;
    }
    const power=itemPower(p,it);let damage=power*P().hitMultiplier(r,d,e,r.enemies.filter(x=>x.hp>0).length);
    damage*=(1+e.fracture*.035+e.haunt*.03)*(r.ailments.hex>0?.8:1);
    const weak=matches(e.weak,el),resist=matches(e.resist,el);
    damage*=weak?1.68:resist?.60:1;
    const pierces=style==='pierce'||d.special==='wardbreak'||d.counter==='wardbreaker';
    if(e.trait==='armored'&&!pierces) damage*=.74;
    if(e.trait==='veteran'&&e.hp<e.maxHp*.45)damage*=.83;
    if(e.ward&&!pierces){damage*=.28;e.ward=0;}else if(pierces)e.ward=0;
    const removed=Math.min(e.hp,damage);e.hp-=removed;
    const amount=Math.max(1,Math.round(damage));
    add(ev,'attack',el,it.level,weak?`${amount} WEAK`:resist?`${amount} RESIST`:`${amount}`,style,e);
    Object.assign(ev[ev.length-1],{itemId:it.id,special:d.special||null,specialId:d.special||null});
    elemental(p,it,e,damage,ev);
    if(style==='burn'){e.burn=Math.max(e.burn,damage*.60*(1+.05*(r.legacy?.masteries.fire||0)));add(ev,'status',el,1,'Burn','burn',e);}
    if(style==='poison'){e.poison+=damage*.23*(1+.05*(r.legacy?.masteries.leaf||0));add(ev,'status',el,1,'Poison','poison',e);}
    if(style==='chill'){e.chill=Math.max(2.1,e.chill);add(ev,'status',el,1,'Slowed','chill',e);}
    if(style==='shock'){e.stun=Math.max(e.stun,Math.min(.95,e.stun+.44));add(ev,'status',el,1,'Stunned','shock',e);}
    if(style==='rapid') e.echoes.push({delay:.13,damage:damage*.23,el,pattern:'rapid'});
    if(style==='drain'||style==='mend'){
      const drain=style==='drain',h=heal(r,drain?removed*.23*(1+(el==='spirit'?.04*(r.legacy?.masteries.spirit||0):0)):power*.55);
      if(h>0){
        const gain=add(ev,'heal',el,1,h<.5?'+<1':`+${Math.round(h)}`,drain?'drain':'heal',drain?e:null);
        gain.amount=h;gain.itemId=it.id;if(drain)gain.sourceUid=e.uid;
      }
    }
    if(style==='guard'){
      const before=r.shield;r.shield=Math.min(r.maxHp*.40,before+power*.60*(el==='stone'?1+.04*(r.legacy?.masteries.stone||0):1));
      const gain=r.shield-before;
      const guard=add(ev,'status',el,1,gain>0?(gain<.5?'Shield +<1':`Shield +${Math.round(gain)}`):'Shield full','guard');
      guard.amount=gain;guard.itemId=it.id;
    }
    if(it.level>=3){e.echoes.push({delay:.32,damage:damage*.4,el,pattern:'echo'});add(ev,'milestone',el,3,'','echo',e);}
    if(it.level>=5){r.shield=Math.min(r.maxHp*.40,r.shield+(1+.12*power)*(el==='stone'?1+.04*(r.legacy?.masteries.stone||0):1));add(ev,'milestone',el,5,'','ring');}
    if(it.level>=10){const nova=damage*.65;
      for(const foe of r.enemies){if(foe!==e&&foe.hp<=0)continue;foe.hp=Math.max(0,foe.hp-nova);add(ev,'milestone',el,10,`+${Math.round(nova)}`,'nova',foe);}
    }
    special(p,it,e,damage,removed,ev);
    P().afterHit(r,r.enemies,e,removed,d,ev,()=>rng(r));
    P().onActivation?.(r,r.enemies,e,d,removed,ev,()=>rng(r));
    it.cd=itemCooldown(it,r);
  }
  const rarityOf=d=>d.rarity||'common';
  function dropEligible(d,boss,round){
    if((d.unlockRound||1)>round)return false;
    return !d.recipe || (boss && d.recipe.every(id=>!C().items[id].recipe));
  }
  function lootPools(r,boss){
    const pools=Object.fromEntries(Object.keys(C().rarities).map(key=>[key,[]]));
    for(const d of Object.values(C().items))if(dropEligible(d,boss,r.round))pools[rarityOf(d)].push(d);
    return pools;
  }
  function lootOdds(r,boss=false,pools=lootPools(r,boss)){
    const band=Math.min(3,Math.floor(r.region/10)),odds={common:0,
      uncommon:pools.uncommon.length?(boss?.25:.20)+band*.01:0,
      rare:pools.rare.length?(boss?.16:.10)+band*.012:0,
      epic:pools.epic.length?(boss?.065:.035)+band*.006:0,
      mythic:pools.mythic.length?(boss?.018:.008)+band*(boss?.004:.002):0};
    if(r.legacy){const shift=(r.legacy.ranks.legendary_scent||0);if(pools.epic.length)odds.epic+=shift*.0035;if(pools.mythic.length)odds.mythic+=shift*.0008;}
    odds.common=1-odds.uncommon-odds.rare-odds.epic-odds.mythic;return odds;
  }
  function localPick(r,pool){
    let total=0;
    const weight=d=>{let n=d.dropRegion===r.region?3:1;const l=r.legacy;if(l){if(l.config.favoredElement===d.element)n*=2.25;if(l.config.style===d.style)n*=1.75;if(!l.seenItems.includes(d.id))n*=1+(l.ranks.relic_hunter||0)*.08;if(r.active.concat(r.pack).some(it=>C().combo(it.id,d.id)))n*=1+(l.ranks.ingredient_sense||0)*.06;}return n;};
    for(const d of pool)total+=weight(d);
    let draw=rng(r)*total;
    for(const d of pool){draw-=weight(d);if(draw<0)return d;}
    return pool[pool.length-1];
  }
  function rollDrop(r,boss){
    // Select rarity before any duplicate, support, ingredient or guardian bonus.
    // This keeps a carried mythic from raising the probability of another one.
    const pools=lootPools(r,boss),odds=lootOdds(r,boss,pools),tierRoll=rng(r);
    let tier='common',cumulative=0;
    for(const candidate of ['mythic','epic','rare','uncommon']){cumulative+=odds[candidate];if(tierRoll<cumulative){tier=candidate;break;}}
    const eligible=pools[tier],bases=eligible.filter(d=>!d.recipe),owned=r.active.concat(r.pack);
    const roll=rng(r),baseLevel=1+Math.floor((r.round-1)/5);
    let selection=[];
    if(roll<.24){
      const allowed=new Set(eligible.map(d=>d.id));
      const copies=owned.filter(it=>allowed.has(it.id));
      if(copies.length){
        const d=localPick(r,copies.map(it=>C().items[it.id]));
        const it=pick(r,copies.filter(x=>x.id===d.id));
        return makeItem(r,it.id,it.level);
      }
    }else if(roll<.42){
      selection=bases.filter(x=>(roll<.33?['mend','heal','shield']:['drain','stun','freeze']).includes(x.style));
    }else if(boss && roll>=.84){
      selection=eligible.filter(x=>x.recipe);
    }else if(roll<.62){
      const held=new Set(owned.map(x=>x.id));
      selection=bases.filter(x=>!held.has(x.id)&&owned.some(y=>C().combo(x.id,y.id)));
    }
    const d=localPick(r,selection.length?selection:bases.length?bases:eligible);
    return makeItem(r,d.id,baseLevel+(boss&&rng(r)<.35?1:0));
  }
  function collectDrop(p,item,foe){
    const r=p.run, def=C().items[item.id];
    const drop={kind:'loot',id:item.id,round:r.round,level:item.level,element:def.element,outcome:'full',age:0,strength:1,pattern:'loot',label:def.name};
    if(foe){drop.enemyId=foe.id;drop.enemyName=foe.name;drop.targetUid=foe.uid;drop.lane=foe.lane;}
    if(r.pack.length<PACK){item.slot=firstOpen(r.pack);r.pack.push(item);drop.outcome='packed';drop.uid=item.uid;}
    else {
      const match=r.active.concat(r.pack).find(x=>x.id===item.id && x.level===item.level);
      if(match){match.level++;drop.outcome='upgraded';drop.level=match.level;drop.uid=match.uid;}
    }
    if(!p.seenItems.includes(item.id))r.relicDiscoveries=(r.relicDiscoveries||0)+1;
    r.dropsThisWave=(r.dropsThisWave||0)+1;discover(p.seenItems,item.id);r.lastDrop={...drop};return drop;
  }
  function enemyDrop(p,foe,ev){
    const r=p.run;
    // Independent kill rolls, normalized to the original headcount. The chance
    // Preserve a 24%/48% chance of at least one find across the whole wave,
    // independent of group size. Rarity is rolled separately, before item bias.
    const extra=(r.legacy?.ranks.treasure_nose||0)*.01;const miss=Math.pow(1-Math.min(.8,dropChances.normal+extra),1/r.waveSize);
    const chance=foe.boss ? 1-(1-Math.min(.9,dropChances.guardian+extra))/Math.pow(miss,r.waveSize-1) : 1-miss;
    let copies=rng(r)<chance?1:0;
    if(foe.boss&&(r.legacy?.ranks.guardian_hoard||0)&&rng(r)<r.legacy.ranks.guardian_hoard*.05)copies++;
    for(let i=0;i<copies;i++){const item=rollDrop(r,foe.boss);item.level+=P().dropBonus(r,()=>rng(r));if((r.legacy?.ranks.anvil_blessing||0)&&rng(r)<r.legacy.ranks.anvil_blessing*.03)item.level++;ev.push(collectDrop(p,item,foe));}
  }
  function finish(p,ev){
    const r=p.run,boss=r.round%4===0;
    if(L()){if(boss)L().guardian(p,r);r.emptyWaves=r.dropsThisWave?0:(r.emptyWaves||0)+1;const luck=r.legacy?.ranks.lucky_streak||0;if(luck&&r.emptyWaves>=7-luck){ev.push(collectDrop(p,rollDrop(r,boss)));r.emptyWaves=0;}r.dropsThisWave=0;}
    r.kills++;p.best=Math.max(p.best,r.round);
    const slot=routeSlot(r);p.routeMarks[slot]=Math.max(p.routeMarks[slot],r.round);
    const h=heal(r,r.maxHp*(boss?.17:.075));if(h>=.5)add(ev,'heal','leaf',1,`+${Math.round(h)}`,'rest');
    P().victory(r,ev);
    for(let i=0;i<(r.bonusDrops||0);i++)ev.push(collectDrop(p,rollDrop(r,boss)));r.bonusDrops=0;
    next(p);add(ev,'round','leaf',1,`Round ${r.round}`,'round');
  }
  function die(p,ev){
    const r=p.run;if(L())L().completed(p,r);r.phase='dead';r.hp=0;r.speed=1;r.lastAward=awardBreakdown(r).reduce((sum,row)=>sum+row.amount,0);p.points+=r.lastAward;p.deaths++;
    add(ev,'dead','spirit',1,`+${r.lastAward} points`,'dead');
  }
  function selectTarget(r,it){
    const d=C().items[it.id],n=r.enemies.length,el=d.element;
    if(d.style==='heal'||d.style==='shield')return r.enemies.find(e=>e.hp>0)||null;
    if(d.style==='stun'||d.style==='freeze'){
      // Spread control across foes, prioritizing an unheld enemy about to strike.
      return r.enemies.filter(e=>e.hp>0).sort((a,b)=>
        Math.max(a.stun,a.freeze||0)-Math.max(b.stun,b.freeze||0)||b.attackProgress-a.attackProgress)[0]||null;
    }
    let target,score=-1;
    for(let i=0;i<n;i++){
      const index=(r.targetCursor+i)%n,e=r.enemies[index];if(e.hp<=0)continue;
      const quality=e.weak===el?2:e.resist===el?0:1;
      if(quality>score){score=quality;target=index;}
    }
    if(target===undefined)return null;
    r.targetCursor=(target+1)%n;return r.enemies[target];
  }
  function advanceFoe(r,e,dt,ev){
    if(e.hp<=0)return;
    for(let i=e.echoes.length-1;i>=0;i--){
      const echo=e.echoes[i];echo.delay-=dt;
      if(echo.delay<=0){e.hp=Math.max(0,e.hp-echo.damage);add(ev,'hit',echo.el,echo.pattern==='echo'?3:1,`${Math.round(echo.damage)}`,echo.pattern,e);e.echoes.splice(i,1);}
    }
    e.statusClock+=dt;
    if(e.statusClock>=1){e.statusClock-=1;
      if(e.burn>0){const n=Math.min(e.burn,Math.max(.5,e.burn*.64));e.burn-=n;e.hp=Math.max(0,e.hp-n);add(ev,'hit','fire',1,`${Math.ceil(n)}`,'burn',e);}
      if(e.poison>0){e.hp=Math.max(0,e.hp-e.poison);add(ev,'hit','leaf',1,`${Math.ceil(e.poison)}`,'poison',e);}
      if(e.trait==='regenerating'&&e.hp>0)e.hp=Math.min(e.maxHp,e.hp+e.maxHp*.018);
    }
    if(e.hp<=0 || r.hp<=0)return;
    const held=Math.max(e.stun,e.freeze||0),activeTime=Math.max(0,dt-held);
    e.attackProgress+=activeTime/e.period*(e.chill>0?.50:1)*(e.rooted>0?.25:1);
    e.stun=Math.max(0,e.stun-dt);e.freeze=Math.max(0,(e.freeze||0)-dt);e.chill=Math.max(0,e.chill-dt);
    e.rooted=Math.max(0,e.rooted-dt);e.hauntTime=Math.max(0,e.hauntTime-dt);if(!e.hauntTime)e.haunt=0;
    if(e.attackProgress>=1&&held<dt){e.attackProgress-=1;e.swings++;
      const incoming=e.damage*(e.trait==='veteran'&&e.swings%3===0?1.38:1);
      const adjusted=P().incoming(r,incoming,e,ev,()=>rng(r))*(r.ailments.corrosion>0?1.15:1)*(counter(r,'insulated')&&e.affliction==='shocking'?.85:1);
      const dealt=hurt(r,adjusted,'strike',e,ev);
      if(e.trait==='venom' && adjusted>0)r.venom=Math.min(e.damage*.42,r.venom+e.damage*.14*(counter(r,'antivenom')?.15:1));
      if(e.trait==='leech')e.hp=Math.min(e.maxHp,e.hp+dealt*.45);
      if(e.trait==='warded'&&e.swings%3===0)e.ward=1;
      if(e.affliction==='bulwark'&&e.swings%2===0)e.ward=1;
      if(dealt>0)afflict(r,e,ev);
      add(ev,'enemy',e.resist||'stone',1,dealt<.5?'Blocked':`−${Math.round(dealt)}`,e.trait,e).sourceUid=e.uid;
    }
  }
  function afflict(r,e,ev){
    const a=r.ailments,key=e.affliction;let applied=false;
    if(key==='corrosive'&&!counter(r,'purify')){a.corrosion=6;applied=true;}
    if(key==='entangling'&&!counter(r,'unbinding')){a.snare=2.5;applied=true;}
    if(key==='hexed'&&!counter(r,'purify')){a.hex=4;applied=true;}
    if(key==='scorching'){a.burn+=e.damage*(counter(r,'blastguard')?.06:counter(r,'thaw')?.10:.24);applied=true;}
    if(key==='freezing'&&!counter(r,'thaw')){a.chill=2.5;applied=true;}
    if(key==='shocking'&&!counter(r,'insulated')){a.shock=.8;applied=true;}
    if(applied)add(ev,'status',key==='scorching'?'fire':key==='freezing'?'frost':key==='shocking'?'storm':key==='hexed'?'spirit':'leaf',1,key,'affliction',e).sourceUid=e.uid;
  }
  function protect(r,dt){
    const a=r.ailments;
    for(const key of ['corrosion','snare','hex','chill','shock'])a[key]=Math.max(0,a[key]-dt);
    if(counter(r,'antivenom'))r.venom=Math.max(0,r.venom-dt*.9);
    if(counter(r,'thaw')){a.chill=0;a.burn=Math.max(0,a.burn-dt*.5);}
    if(counter(r,'insulated'))a.shock=0;
    if(counter(r,'unbinding'))a.snare=0;
    if(counter(r,'purify'))a.hex=a.corrosion=0;
    if(a.corrosion>0)r.shield=Math.max(0,r.shield-dt*r.maxHp*.01);
  }
  function step(p,dt,ev){
    const r=p.run;r.time+=dt;r.xp+=dt*.45*expeditionXpRate*(1+(r.legacy?.ranks.guardian_wisdom||0)*.05);
    protect(r,dt);
    if(r.phase==='walk'){
      r.distance+=dt*41;r.walkLeft=Math.max(0,r.walkLeft-dt);
      for(const it of r.active)it.cd=Math.max(0,it.cd-dt);
      if(r.walkLeft<=0)spawn(p,ev);
    }else{
      r.distance+=dt*13;
      for(const it of r.active){
        it.cd=Math.max(0,it.cd-dt*(r.ailments.snare>0?.7:1)*(r.ailments.chill>0?.65:1));
        if(it.cd<=0){const target=selectTarget(r,it);if(target)fire(p,it,target,ev);}
      }
      P().tickAbilities(r,r.enemies,dt*(r.ailments.shock>0?.6:1),ev);
      for(const e of r.enemies)advanceFoe(r,e,dt,ev);
      r.venomClock+=dt;
      if(r.venomClock>=1){r.venomClock-=1;if(r.venom>0){hurt(r,r.venom,'venom',null,ev);r.venom=Math.max(0,r.venom-.6);}if(r.ailments.burn>0){const n=r.ailments.burn*.5;hurt(r,n,'scorch',null,ev);r.ailments.burn=Math.max(0,r.ailments.burn-n);add(ev,'enemy','fire',1,'−'+Math.ceil(n),'scorching');}}
      const resolvedDeaths=new Set();
      while(r.enemies.some(e=>e.hp<=0&&!resolvedDeaths.has(e.uid)))for(const e of r.enemies){
        if(e.hp>0||resolvedDeaths.has(e.uid))continue;resolvedDeaths.add(e.uid);
        const event=add(ev,'kill',e.weak,1,e.name,'kill',e);
        event.enemy={uid:e.uid,lane:e.lane,name:e.name,id:e.id,species:e.species,color:e.color,boss:e.boss,hp:0,maxHp:e.maxHp};
        r.foes++;r.xp+=((e.boss?17:8)+Math.sqrt(r.round)*1.4)*expeditionXpRate*(1+(r.legacy?.ranks.guardian_wisdom||0)*.05);
        P().onDeaths?.(r,[e],r.enemies,ev,()=>rng(r));
        enemyDrop(p,e,ev);
        if(e.trait==='volatile'){
          const damage=P().incoming(r,e.damage*.48*(counter(r,'blastguard')?.25:1),e,ev,()=>rng(r)), n=hurt(r,damage,'burst',e,ev);
          add(ev,'enemy','fire',1,`−${Math.round(n)}`,'volatile',e).sourceUid=e.uid;
        }
      }
      r.enemies=r.enemies.filter(e=>e.hp>0);r.enemy=r.enemies[0]||null;
      if(r.hp<=0&&!P().survive?.(r,r.enemies,ev))die(p,ev);
      else if(!r.enemies.length)finish(p,ev);
    }
    levelUp(r,ev);
  }
  function tick(p,dt){
    const r=p.run,ev=[];if(!r||!Number.isFinite(dt)||dt<=0)return ev;
    let real=Math.min(5,dt);
    while(real>1e-8&&!r.draft.length&&(r.phase==='walk'||r.phase==='fight')){
      r.speed=isReplay(p,r)?r.paceChoice:1;
      const elapsed=Math.min(real,.075/r.speed);step(p,elapsed*r.speed,ev);real-=elapsed;
    }
    return ev;
  }
  // Copy only recognized fields. Rebuild display strings from the trusted catalogue.
  function validate(raw) {
    const bad=()=>{throw Error('Invalid or incompatible Bramblebound profile.');};
    const object=v=>{if(!v||typeof v!=='object'||Array.isArray(v))bad();return v;};
    const number=(v,min=0,max=Number.MAX_SAFE_INTEGER)=>{if(typeof v!=='number'||!Number.isFinite(v)||v<min||v>max)bad();return v;};
    const integer=(v,min=0,max=Number.MAX_SAFE_INTEGER)=>{if(!Number.isSafeInteger(v))bad();return number(v,min,max);};
    const array=(v,cap)=>{if(!Array.isArray(v)||v.length>cap)bad();return v;};
    const id=(v,ids)=>{if(typeof v!=='string'||!ids.has(v))bad();return v;};
    const itemIds=new Set(Object.keys(C().items)),enemyIds=new Set(C().enemies.map(e=>e.id)),elements=new Set(Object.keys(C().elements));
    const journal=(v,ids)=>[...new Set(array(v,ids.size).map(x=>id(x,ids)))];
    const src=object(typeof raw==='string'?JSON.parse(raw):raw),p=freshProfile();
    if(src.version!==1)bad();p.points=integer(src.points);p.best=integer(src.best);p.deaths=integer(src.deaths);
    if(src.routeMarks===undefined){
      // Old players cleared the first eight regions repeatedly. Remember the
      // strongest completed wave for each known stage; leave expanded stages new.
      const oldLength=OLD_REGIONS*WAVES_PER_REGION;
      p.routeMarks=p.routeMarks.map((_,i)=>i<oldLength&&i<p.best?i+1+oldLength*Math.floor((p.best-i-1)/oldLength):0);
    }else{
      const marks=array(src.routeMarks,p.routeMarks.length);
      const expected=src.worldRegions===undefined?marks.length:integer(src.worldRegions,1,C().regions.length)*WAVES_PER_REGION;
      if(![OLD_REGIONS*4,80,p.routeMarks.length].includes(marks.length)||marks.length!==expected)bad();
      marks.forEach((v,i)=>{integer(v,0,p.best);if(v&&(v-1)%WAVES_PER_REGION!==i%WAVES_PER_REGION)bad();p.routeMarks[i]=v;});
    }
    p.seenItems=journal(src.seenItems,itemIds);p.seenEnemies=journal(src.seenEnemies,enemyIds);
    const ranks=object(src.perks);
    for(const key of Object.keys(perks))p.perks[key]=integer(ranks[key],0,perks[key].max);
    if(L())L().migrate(p,src.legacy);
    if(src.run===null)return p;
    const s=object(src.run),r=expeditionDefaults();p.run=r;
    if(L()){
      const snap=s.legacy===undefined?L().snapshot(p):L().validateSnapshot(s.legacy,p);r.legacy=L().snapshot(p);if(snap.version!==2)bad();
      for(const key of ['ranks','masteries','milestones','constellations','oaths','config','seenItems','seenEnemies']){if(JSON.stringify(snap[key])!==JSON.stringify(r.legacy[key])){
        if(key==='seenItems')r.legacy.seenItems=journal(snap.seenItems,itemIds);else if(key==='seenEnemies')r.legacy.seenEnemies=journal(snap.seenEnemies,enemyIds);else if(s.phase!=='dead')bad();
      }}
      if(s.phase==='dead')r.legacy=snap;
      for(const key of ['guardians','dropsThisWave','emptyWaves','legacyRewards','relicDiscoveries','draftRerolls','banishes','echoesLeft','bonusTalentCredits','startingTalentCredits','bonusTalentsChosen'])r[key]=integer(s[key]??0);
      r.draftIsBonus=!!s.draftIsBonus;r.campUnlocksAtStart=array(s.campUnlocksAtStart||[],9).map(v=>id(v,new Set(['heartwood','wayfinder','forge','fate','grove','mythic','constellations','convergence','endgame'])));r.banished=array(s.banished||[],Object.keys(P().upgrades).length).map(v=>id(v,new Set(Object.keys(P().upgrades))));
      r.echoChoices=array(s.echoChoices||[],2).map(v=>({id:id(v.id,new Set(Object.keys(P().upgrades))),drafts:integer(v.drafts,0,2)}));
      r.legacyAwards=array(s.legacyAwards||[],50).map(v=>{object(v);if(typeof v.label!=='string'||v.label.length>80)bad();return {label:v.label,amount:integer(v.amount)};});if(r.legacyAwards.reduce((n,a)=>n+a.amount,0)!==r.legacyRewards)bad();
    }
    if(P().validateLivingState&&s.living!==undefined)r.living=P().validateLivingState(s.living);
    r.waveRules=s.waveRules===undefined?1:integer(s.waveRules,1,WAVE_RULES);
    r.bestAtStart=s.bestAtStart==null?null:integer(s.bestAtStart,0,p.best);
    if(s.deathCause!=null){
      const c=object(s.deathCause), kind=id(c.kind,new Set(['strike','venom','burst','scorch']));
      if(s.phase!=='dead')bad();r.deathCause={kind,damage:number(c.damage)};
      if(c.enemyId!==undefined)r.deathCause.enemyId=id(c.enemyId,enemyIds);
    }
    r.phase=id(s.phase,new Set(['walk','fight','loot','dead']));r.round=integer(s.round,1);
    const oldLength=OLD_REGIONS*WAVES_PER_REGION;
    r.routeEpoch=s.routeEpoch===undefined?1+oldLength*Math.floor((r.round-1)/oldLength):integer(s.routeEpoch,1,r.round);
    if(s.routeRegions===undefined){
      if((r.routeEpoch-1)%oldLength!==0)bad();
      r.routeRegions=s.routeEpoch===undefined?OLD_REGIONS:(src.routeMarks?.length===140?C().regions.length:20);
      r.routeCircuit=1+(r.routeEpoch-1)/oldLength;
      r.routeHistory=r.routeEpoch>1?[{epoch:1,regions:OLD_REGIONS,circuit:1}]:[];
    }else{
      r.routeRegions=integer(s.routeRegions,1,C().regions.length);if(![8,20,C().regions.length].includes(r.routeRegions))bad();
      r.routeCircuit=integer(s.routeCircuit,1);
      r.routeHistory=array(s.routeHistory||[],3).map(segment=>{object(segment);const value={epoch:integer(segment.epoch,1,r.routeEpoch),regions:integer(segment.regions,1,C().regions.length),circuit:integer(segment.circuit,1)};if(![8,20,C().regions.length].includes(value.regions))bad();return value;});
      const segments=[...r.routeHistory,{epoch:r.routeEpoch,regions:r.routeRegions,circuit:r.routeCircuit}];
      if(segments[0].epoch!==1||segments[0].circuit!==1)bad();
      for(let i=1;i<segments.length;i++){const a=segments[i-1],b=segments[i],cycles=(b.epoch-a.epoch)/(a.regions*4);if(!Number.isInteger(cycles)||cycles<1||b.circuit!==a.circuit+cycles||b.regions<a.regions)bad();}
    }
    r.region=routeInfo(r).region;if(s.region!==r.region)bad();
    r.maxHp=number(s.maxHp,1,88+14*perks.heart.max);r.hp=number(s.hp,0,r.maxHp);r.shield=number(s.shield,0,r.maxHp*.4+.00001);
    r.distance=number(s.distance);r.time=number(s.time);r.kills=integer(s.kills);
    if((r.phase==='dead')!==(r.hp===0))bad();
    if(r.kills!==r.round-(r.phase==='loot'?0:1)||p.best<r.kills)bad();
    r.speed=integer(s.speed,1,r.phase==='dead'?4:maxSpeed(p));r.paceChoice=integer(s.paceChoice,1,r.phase==='dead'?4:maxSpeed(p));
    if(!isReplay(p,r)||r.phase==='dead')r.speed=1;
    const maxHealth=88+14*p.perks.heart;
    if(r.phase==='dead' ? (r.maxHp-88)%14!==0 || r.maxHp<88 : r.maxHp!==maxHealth)bad();
    r.walkLeft=number(s.walkLeft,0,WALK);r.lastAward=integer(s.lastAward);
    if(r.lastAward!==(r.phase==='dead'?1+r.kills+Math.floor(r.kills/4)+(r.legacyRewards||0):0))bad();
    r.nextUid=integer(s.nextUid,1);r.seed=integer(s.seed,1,4294967295);r.venom=number(s.venom);
    if(s.ailments!==undefined){const a=object(s.ailments);for(const key of Object.keys(r.ailments))r.ailments[key]=number(a[key]??0,0,key==='burn'?Number.MAX_SAFE_INTEGER:6);}
    const uids=new Set();
    const item=x=>{object(x);const uid=integer(x.uid,1,r.nextUid-1),name=id(x.id,itemIds);if(uids.has(uid))bad();uids.add(uid);const it={uid,id:name,level:number(x.level,1),cd:number(x.cd,0,C().items[name].cooldown+.00001)};if(x.charges!==undefined)it.charges=integer(x.charges,0,2);return it;};
    function slots(list,capacity){
      const used=new Set();return array(list,capacity).map((entry,index)=>{
        const it=item(entry), slot=integer(entry.slot===undefined?index:entry.slot,0,capacity-1);
        if(used.has(slot))bad();used.add(slot);it.slot=slot;return it;
      });
    }
    r.active=slots(s.active,ACTIVE);r.pack=slots(s.pack,PACK);if(!r.active.length)bad();
    r.lastDrop=null;
    if(s.lastDrop!=null){
      const entry=object(s.lastDrop), itemId=id(entry.id,itemIds),def=C().items[itemId];
      const outcome=id(entry.outcome,new Set(['packed','upgraded','full']));
      const activeFoeDrop=entry.enemyId!==undefined&&(r.phase==='fight'||r.phase==='dead');
      r.lastDrop={kind:'loot',id:itemId,round:integer(entry.round,1,activeFoeDrop?r.round:r.kills),level:number(entry.level,1),element:def.element,outcome,age:0,strength:1,pattern:'loot',label:def.name};
      if(outcome!=='full')r.lastDrop.uid=integer(entry.uid,1,r.nextUid-1);
      if(entry.enemyId!==undefined){
        const enemyId=id(entry.enemyId,enemyIds),foe=C().enemies.find(e=>e.id===enemyId);
        if(foe.region!==routeInfo(r,r.lastDrop.round).region)bad();
        Object.assign(r.lastDrop,{enemyId,enemyName:foe.name,targetUid:integer(entry.targetUid,1,integer(s.nextEnemyUid,2)-1),lane:integer(entry.lane,0,4)});
      }
    }
    const upgraded=s.level!==undefined;
    if(upgraded){
      const upgradeIds=new Set(Object.keys(P().upgrades));
      r.level=integer(s.level,1);r.xp=number(s.xp);r.foes=integer(s.foes,r.kills);
      r.draft=array(s.draft,5).map(choice=>id(choice,upgradeIds));
      if(r.draft.length!==0 && (r.draft.length<3 || new Set(r.draft).size!==r.draft.length))bad();
      const chosen=object(s.skills),timers=object(s.abilityTimers);let ranks=0;
      for(const [key,value] of Object.entries(chosen)){
        id(key,upgradeIds);r.skills[key]=integer(value,1);ranks+=value;
      }
      if(ranks!==r.level-1-(r.draft.length&&!r.draftIsBonus?1:0)+(r.bonusTalentsChosen||0))bad();
      for(const [key,time] of Object.entries(timers)){
        id(key,upgradeIds);
        const cooldown=P().upgrades[key].cooldown;
        if(!owns(r.skills,key)||!(cooldown>0))bad();
        r.abilityTimers[key]=number(time,0,cooldown+.00001);
      }
      r.nextEnemyUid=integer(s.nextEnemyUid,1);r.targetCursor=integer(s.targetCursor,0,4);
      r.waveSize=integer(s.waveSize,0,5);r.venomClock=number(s.venomClock,0,1);
    }else r.foes=r.kills;
    if(r.phase==='loot'){
      // v1 paused at an already-rewarded cache. Consume one saved option once;
      // neither roll a new item nor repeat the kill, healing, or record increment.
      if(s.enemy!==null)bad();const offered=array(s.loot,3).map(item);if(!offered.length)bad();
      collectDrop(p,pick(r,offered));next(p);return p;
    }
    if(s.loot!==undefined && array(s.loot,3).length)bad();
    if(r.phase==='walk'){
      if(s.enemy!==null || (upgraded && (r.waveSize!==0 || array(s.enemies,5).length)))bad();
      r.waveRules=WAVE_RULES;r.enemy=null;return p;
    }
    if(!upgraded)r.waveSize=1;
    const stored=upgraded?array(s.enemies,r.waveSize):[s.enemy];
    if((!stored.length && r.phase==='fight')||r.waveSize<1)bad();
    const enemyUids=new Set(), lanes=new Set();
    r.enemies=stored.map(rawEnemy=>{
      const v=object(rawEnemy),enemyId=id(v.id,enemyIds),d=C().enemies.find(e=>e.id===enemyId);
      if(d.region!==r.region)bad();
      const lane=upgraded?integer(v.lane,0,r.waveSize-1):0;
      if(d.boss!==(r.round%4===0 && lane===0))bad();
      const uid=upgraded?integer(v.uid,1,r.nextEnemyUid-1):r.nextEnemyUid++;
      if(enemyUids.has(uid)||lanes.has(lane))bad();enemyUids.add(uid);lanes.add(lane);
      const e=buildEnemy(r.round,d,r.waveSize,r.waveRules);e.uid=uid;e.lane=lane;
      if(v.boss!==e.boss || v.maxHp!==e.maxHp || v.damage!==e.damage || v.period!==e.period)bad();
      e.hp=number(v.hp,0,e.maxHp);
      if(r.phase==='fight' && e.hp===0)bad();
      e.attackProgress=number(v.attackProgress,0,1);e.ward=integer(v.ward,0,1);e.swings=integer(v.swings);
      e.burn=number(v.burn);e.poison=number(v.poison);e.chill=number(v.chill,0,8);
      e.stun=number(v.stun,0,8);e.freeze=number(v.freeze??0,0,8);e.statusClock=number(v.statusClock,0,1);
      e.rooted=number(v.rooted??0,0,8);e.fracture=number(v.fracture??0,0,35);e.haunt=integer(v.haunt??0,0,20);e.hauntTime=number(v.hauntTime??0,0,4);
      for(const key of ['leafHits','frostHits','stormHits'])e[key]=integer(v[key]??0,0,2);
      e.echoes=array(v.echoes,40).map(x=>{object(x);return {delay:number(x.delay,0,1),damage:number(x.damage),el:id(x.el,elements),pattern:id(x.pattern,new Set(['echo','rapid','soulmark','aftershock']))};});
      return e;
    }).filter(e=>e.hp>0);
    r.enemy=r.enemies[0]||null;
    return p;
  }
  root.GameEngine={routeInfo,isReplay,lootOdds,dropEligible,dropChances,freshProfile,start,tick,act,validate,xpNeeded,previewPair,previewCombine,slotAt,perks,perkCost,perkPreview,awardBreakdown,maxSpeed,itemPower,itemUtility,itemDuration,itemComparison,itemCooldown,levelText,walkDuration:WALK,activeCapacity:ACTIVE,packCapacity:PACK};
})();
