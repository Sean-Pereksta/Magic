/* Bramblebound road simulation. No DOM or timers; the caller supplies elapsed seconds. */
(() => {
  'use strict';
  const root = globalThis;
  const C = () => root.Catalog;
  const P = () => root.Progression;
  const WALK = 3.2, ACTIVE = 4, PACK = 400, WAVE_RULES=2;
  const xpNeeded = level => 14+Math.round((level-1)*5.5);
  const OLD_REGIONS=8, WAVES_PER_REGION=4;
  const expeditionXpRate = 0.22;
  function expeditionDefaults(){return {routeEpoch:1,level:1,xp:0,draft:[],skills:{},abilityTimers:{},foes:0,waveSize:0,
    nextEnemyUid:1,enemies:[],targetCursor:0,venomClock:0,waveRules:WAVE_RULES,bestAtStart:null,deathCause:null};}
  const perks = {
    heart: {name:'Heartwood', description:'Start each journey with 14 more maximum health.', max:100},
    claw: {name:'Iron grip', description:'All items deal 8% more base damage per rank.', max:100},
    pace: {name:'Trail memory', description:'Unlock 2×, 3×, then 4× speed on cleared rounds.', max:3}
  };
  const ok = message => ({ok:true,message});
  const fail = message => ({ok:false,message});
  const owns = (obj, key) => Object.prototype.hasOwnProperty.call(obj,key);
  function freshProfile() { return {version:1,points:0,best:0,deaths:0,routeMarks:Array(C().regions.length*WAVES_PER_REGION).fill(0),seenItems:[],seenEnemies:[],perks:{heart:0,claw:0,pace:0},run:null}; }
  function perkCost(p,key) {
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
    {label:'Journey completed',amount:1},{label:'Waves cleared',amount:r.kills},{label:'Guardian waves cleared',amount:Math.floor(r.kills/4)}
  ];}
  function discover(arr,id){if(!arr.includes(id))arr.push(id);}
  const maxSpeed = p => 1+p.perks.pace;
  function routeInfo(r,round=r.round){
    const epoch=r.routeEpoch||1, historical=round<epoch;
    const regionsPerCircuit=historical?OLD_REGIONS:C().regions.length;
    const origin=historical?1:epoch, length=regionsPerCircuit*WAVES_PER_REGION;
    const lap=Math.floor((round-origin)/length), circuitStart=origin+lap*length;
    const region=Math.floor((round-circuitStart)/WAVES_PER_REGION),regionStart=circuitStart+region*WAVES_PER_REGION;
    const circuit=1+lap+(historical?0:(epoch-1)/(OLD_REGIONS*WAVES_PER_REGION));
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
    return C().items[it.id].power*(1+.31*n+.009*n*n)*(1+.08*p.perks.claw);
  }
  const itemCooldown = (it,r) => r ? P().cooldown(r,C().items[it.id]) : C().items[it.id].cooldown;
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
    for(const zone of [r.active,r.pack]) zone.forEach((it,slot)=>{it.slot=slot;discover(p.seenItems,it.id);});
    p.run=r;
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
    return {id:d.id,name:d.name,species:d.species,color:d.color,trait:d.trait,weak:d.weak,resist:d.resist,boss,hp:maxHp,maxHp,damage,period,attackProgress:0,burn:0,poison:0,chill:0,stun:0,statusClock:0,ward:d.trait==='warded'?1:0,swings:0,echoes:[]};
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
    const level=x.level+(y.level-x.level)/2;
    return {ok:true,id:d.id,level,message:`Fuse into ${d.name} · Lv ${levelText(level)}`};
  }
  function next(p) {
    const r=p.run;
    r.round++; r.region=routeInfo(r).region; r.phase='walk'; r.walkLeft=WALK;
    r.enemy=null;r.enemies=[];r.waveSize=0;r.waveRules=WAVE_RULES;r.venomClock=0; r.venom=0; r.speed=isReplay(p,r)?r.paceChoice:1;
  }
  // Arrays contain occupied items only. Slot numbers preserve intentional gaps.
  const slotAt=(r,zone,slot)=>r[zone].find(it=>it.slot===slot);
  function firstOpen(zone){let i=0;while(zone.some(x=>x.slot===i))i++;return i;}
  function remove(arr,x){arr.splice(arr.indexOf(x),1);}
  function combineInto(p,x,y,dest,slot,res){
    const r=p.run, level=res.level, cd=Math.min(Math.max(x.cd,y.cd),C().items[res.id].cooldown);
    remove(r.active.includes(x)?r.active:r.pack,x);remove(r.active.includes(y)?r.active:r.pack,y);
    const it=makeItem(r,res.id,level,cd);it.slot=slot;dest.push(it);discover(p.seenItems,it.id);
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
    if(action==='buy') {
      if(r&&r.phase!=='dead') return fail('Visit camp between journeys to spend points.');
      const cost=perkCost(p,a);
      if(cost===null) return fail('That upgrade is unavailable.');
      if(p.points<cost) return fail(`Requires ${cost} points.`);
      p.points-=cost; p.perks[a]++; return ok(`${perks[a].name} upgraded.`);
    }
    if(!r||r.phase==='dead') return fail('Begin a journey first.');
    if(action==='chooseUpgrade') {
      if(!r.draft.includes(a))return fail('Choose one of the three offered upgrades.');
      P().apply(r,a);r.draft=[];levelUp(r,[]);
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
  function levelUp(r,ev){
    const threshold=xpNeeded(r.level);
    if(r.phase==='dead'||r.draft.length||r.xp<threshold)return;
    r.xp-=threshold;r.level++;r.draft=P().offer(r,()=>rng(r),3);
    add(ev,'level','spirit',r.level,'Level '+r.level,'level');
  }
  function matches(tag,element) {return Array.isArray(tag)?tag.includes(element):tag===element;}
  function heal(r,n){const before=r.hp;r.hp=Math.min(r.maxHp,r.hp+P().healing(r,n));return r.hp-before;}
  function hurt(r,n,kind,enemy){
    const absorbed=Math.min(n,r.shield),damage=n-absorbed,before=r.hp;
    r.shield-=absorbed;r.hp=Math.max(0,before-damage);
    if(before>0 && r.hp===0){r.deathCause={kind,damage};if(enemy)r.deathCause.enemyId=enemy.id;}
    return damage;
  }
  function fire(p,it,e,ev) {
    const r=p.run,d=C().items[it.id],el=d.element,style=d.style;
    const power=itemPower(p,it);let damage=power*P().hitMultiplier(r,d,e,r.enemies.filter(x=>x.hp>0).length);
    const weak=matches(e.weak,el),resist=matches(e.resist,el);
    damage*=weak?1.68:resist?.60:1;
    if(e.trait==='armored'&&style!=='pierce') damage*=.74;
    if(e.trait==='veteran'&&e.hp<e.maxHp*.45)damage*=.83;
    if(e.ward&&style!=='pierce'){damage*=.28;e.ward=0;}
    const removed=Math.min(e.hp,damage);e.hp-=removed;
    const amount=Math.max(1,Math.round(damage));
    add(ev,'attack',el,it.level,weak?`${amount} WEAK`:resist?`${amount} RESIST`:`${amount}`,style,e);
    ev[ev.length-1].itemId=it.id;
    if(style==='burn'){e.burn=Math.max(e.burn,damage*.60);add(ev,'status',el,1,'Burn','burn',e);}
    if(style==='poison'){e.poison+=damage*.23;add(ev,'status',el,1,'Poison','poison',e);}
    if(style==='chill'){e.chill=Math.max(2.1,e.chill);add(ev,'status',el,1,'Slowed','chill',e);}
    if(style==='shock'){e.stun=Math.min(.95,e.stun+.44);add(ev,'status',el,1,'Stunned','shock',e);}
    if(style==='rapid') e.echoes.push({delay:.13,damage:damage*.23,el,pattern:'rapid'});
    if(style==='drain'||style==='mend'){
      const drain=style==='drain',h=heal(r,drain?removed*.23:power*.55);
      if(h>0){
        const gain=add(ev,'heal',el,1,h<.5?'+<1':`+${Math.round(h)}`,drain?'drain':'heal',drain?e:null);
        gain.amount=h;gain.itemId=it.id;if(drain)gain.sourceUid=e.uid;
      }
    }
    if(style==='guard'){
      const before=r.shield;r.shield=Math.min(r.maxHp*.40,before+power*.60);
      const gain=r.shield-before;
      const guard=add(ev,'status',el,1,gain>0?(gain<.5?'Shield +<1':`Shield +${Math.round(gain)}`):'Shield full','guard');
      guard.amount=gain;guard.itemId=it.id;
    }
    if(it.level>=3){e.echoes.push({delay:.32,damage:damage*.4,el,pattern:'echo'});add(ev,'milestone',el,3,'','echo',e);}
    if(it.level>=5){r.shield=Math.min(r.maxHp*.40,r.shield+1+.12*power);add(ev,'milestone',el,5,'','ring');}
    if(it.level>=10){const nova=damage*.65;
      for(const foe of r.enemies){if(foe!==e&&foe.hp<=0)continue;foe.hp=Math.max(0,foe.hp-nova);add(ev,'milestone',el,10,`+${Math.round(nova)}`,'nova',foe);}
    }
    P().afterHit(r,r.enemies,e,removed,d,ev,()=>rng(r));
    it.cd=itemCooldown(it,r);
  }
  const rarityOf=d=>d.rarity||'common';
  function dropEligible(d,boss,round){
    if((d.unlockRound||1)>round)return false;
    return !d.recipe || (boss && d.recipe.every(id=>!C().items[id].recipe));
  }
  function lootPools(r,boss){
    const pools={common:[],rare:[],mythic:[]};
    for(const d of Object.values(C().items))if(dropEligible(d,boss,r.round))pools[rarityOf(d)].push(d);
    return pools;
  }
  function lootOdds(r,boss=false,pools=lootPools(r,boss)){
    const band=Math.min(2,Math.floor(r.region/7));
    const rare=pools.rare.length?(boss?.22:.12)+band*.04:0;
    const mythic=pools.mythic.length?(boss?.03+band*.02:.008+band*.007):0;
    return {common:1-rare-mythic,rare,mythic};
  }
  function localPick(r,pool){
    let total=0;
    for(const d of pool)total+=d.dropRegion===r.region?3:1;
    let draw=rng(r)*total;
    for(const d of pool){draw-=d.dropRegion===r.region?3:1;if(draw<0)return d;}
    return pool[pool.length-1];
  }
  function rollDrop(r,boss){
    // Select rarity before any duplicate, support, ingredient or guardian bonus.
    // This keeps a carried mythic from raising the probability of another one.
    const pools=lootPools(r,boss),odds=lootOdds(r,boss,pools),tierRoll=rng(r);
    const tier=tierRoll<odds.mythic?'mythic':tierRoll<odds.mythic+odds.rare?'rare':'common';
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
      selection=bases.filter(x=>x.style===(roll<.33?'mend':'drain'));
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
    discover(p.seenItems,item.id);r.lastDrop={...drop};return drop;
  }
  function enemyDrop(p,foe,ev){
    const r=p.run;
    // Independent kill rolls, normalized to the original headcount. The chance
    // that every roll misses is .84 on a normal wave and .63 on a guardian wave.
    const miss=Math.pow(.84,1/r.waveSize);
    const chance=foe.boss ? 1-.63/Math.pow(miss,r.waveSize-1) : 1-miss;
    if(rng(r)>=chance)return;
    const item=rollDrop(r,foe.boss);item.level+=P().dropBonus(r,()=>rng(r));
    ev.push(collectDrop(p,item,foe));
  }
  function finish(p,ev){
    const r=p.run,boss=r.round%4===0;
    r.kills++;p.best=Math.max(p.best,r.round);
    const slot=routeSlot(r);p.routeMarks[slot]=Math.max(p.routeMarks[slot],r.round);
    const h=heal(r,r.maxHp*(boss?.17:.075));if(h>=.5)add(ev,'heal','leaf',1,`+${Math.round(h)}`,'rest');
    P().victory(r,ev);
    next(p);add(ev,'round','leaf',1,`Round ${r.round}`,'round');
  }
  function die(p,ev){
    const r=p.run;r.phase='dead';r.hp=0;r.speed=1;r.lastAward=awardBreakdown(r).reduce((sum,row)=>sum+row.amount,0);p.points+=r.lastAward;p.deaths++;
    add(ev,'dead','spirit',1,`+${r.lastAward} points`,'dead');
  }
  function selectTarget(r,it){
    const n=r.enemies.length,el=C().items[it.id].element;
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
    if(e.stun<=0)e.attackProgress+=dt/e.period*(e.chill>0?.50:1);
    e.stun=Math.max(0,e.stun-dt);e.chill=Math.max(0,e.chill-dt);
    if(e.attackProgress>=1){e.attackProgress-=1;e.swings++;
      const incoming=e.damage*(e.trait==='veteran'&&e.swings%3===0?1.38:1);
      const adjusted=P().incoming(r,incoming,e,ev,()=>rng(r));
      const dealt=hurt(r,adjusted,'strike',e);
      if(e.trait==='venom' && adjusted>0)r.venom=Math.min(e.damage*.42,r.venom+e.damage*.14);
      if(e.trait==='leech')e.hp=Math.min(e.maxHp,e.hp+dealt*.45);
      if(e.trait==='warded'&&e.swings%3===0)e.ward=1;
      add(ev,'enemy',e.resist||'stone',1,dealt<.5?'Blocked':`−${Math.round(dealt)}`,e.trait,e).sourceUid=e.uid;
    }
  }
  function step(p,dt,ev){
    const r=p.run;r.time+=dt;r.xp+=dt*.45*expeditionXpRate;
    if(r.phase==='walk'){
      r.distance+=dt*41;r.walkLeft=Math.max(0,r.walkLeft-dt);
      for(const it of r.active)it.cd=Math.max(0,it.cd-dt);
      if(r.walkLeft<=0)spawn(p,ev);
    }else{
      r.distance+=dt*13;
      for(const it of r.active){
        it.cd=Math.max(0,it.cd-dt);
        if(it.cd<=0){const target=selectTarget(r,it);if(target)fire(p,it,target,ev);}
      }
      P().tickAbilities(r,r.enemies,dt,ev);
      for(const e of r.enemies)advanceFoe(r,e,dt,ev);
      r.venomClock+=dt;
      if(r.venomClock>=1){r.venomClock-=1;if(r.venom>0){hurt(r,r.venom,'venom');r.venom=Math.max(0,r.venom-.6);}}
      for(const e of r.enemies){
        if(e.hp>0)continue;
        const event=add(ev,'kill',e.weak,1,e.name,'kill',e);
        event.enemy={uid:e.uid,lane:e.lane,name:e.name,id:e.id,species:e.species,color:e.color,boss:e.boss,hp:0,maxHp:e.maxHp};
        r.foes++;r.xp+=((e.boss?17:8)+Math.sqrt(r.round)*1.4)*expeditionXpRate;
        enemyDrop(p,e,ev);
        if(e.trait==='volatile'){
          const damage=P().incoming(r,e.damage*.48,e,ev,()=>rng(r)), n=hurt(r,damage,'burst',e);
          add(ev,'enemy','fire',1,`−${Math.round(n)}`,'volatile',e).sourceUid=e.uid;
        }
      }
      r.enemies=r.enemies.filter(e=>e.hp>0);r.enemy=r.enemies[0]||null;
      if(r.hp<=0)die(p,ev);
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
      if(marks.length!==p.routeMarks.length)bad();
      p.routeMarks=marks.map((v,i)=>{integer(v,0,p.best);if(v&&(v-1)%WAVES_PER_REGION!==i%WAVES_PER_REGION)bad();return v;});
    }
    p.seenItems=journal(src.seenItems,itemIds);p.seenEnemies=journal(src.seenEnemies,enemyIds);
    const ranks=object(src.perks);
    for(const key of Object.keys(perks))p.perks[key]=integer(ranks[key],0,perks[key].max);
    if(src.run===null)return p;
    const s=object(src.run),r=expeditionDefaults();p.run=r;
    r.waveRules=s.waveRules===undefined?1:integer(s.waveRules,1,WAVE_RULES);
    r.bestAtStart=s.bestAtStart==null?null:integer(s.bestAtStart,0,p.best);
    if(s.deathCause!=null){
      const c=object(s.deathCause), kind=id(c.kind,new Set(['strike','venom','burst']));
      if(s.phase!=='dead')bad();r.deathCause={kind,damage:number(c.damage)};
      if(c.enemyId!==undefined)r.deathCause.enemyId=id(c.enemyId,enemyIds);
    }
    r.phase=id(s.phase,new Set(['walk','fight','loot','dead']));r.round=integer(s.round,1);
    const oldLength=OLD_REGIONS*WAVES_PER_REGION;
    r.routeEpoch=s.routeEpoch===undefined?1+oldLength*Math.floor((r.round-1)/oldLength):integer(s.routeEpoch,1,r.round);
    if((r.routeEpoch-1)%oldLength!==0)bad();
    r.region=routeInfo(r).region;if(s.region!==r.region)bad();
    r.maxHp=number(s.maxHp,1,88+14*perks.heart.max);r.hp=number(s.hp,0,r.maxHp);r.shield=number(s.shield,0,r.maxHp*.4+.00001);
    r.distance=number(s.distance);r.time=number(s.time);r.kills=integer(s.kills);
    if((r.phase==='dead')!==(r.hp===0))bad();
    if(r.kills!==r.round-(r.phase==='loot'?0:1)||p.best<r.kills)bad();
    r.speed=integer(s.speed,1,maxSpeed(p));r.paceChoice=integer(s.paceChoice,1,maxSpeed(p));
    if(!isReplay(p,r)||r.phase==='dead')r.speed=1;
    const maxHealth=88+14*p.perks.heart;
    if(r.phase==='dead' ? r.maxHp>maxHealth || (r.maxHp-88)%14!==0 || r.maxHp<88 : r.maxHp!==maxHealth)bad();
    r.walkLeft=number(s.walkLeft,0,WALK);r.lastAward=integer(s.lastAward);
    if(r.lastAward!==(r.phase==='dead'?1+r.kills+Math.floor(r.kills/4):0))bad();
    r.nextUid=integer(s.nextUid,1);r.seed=integer(s.seed,1,4294967295);r.venom=number(s.venom);
    const uids=new Set();
    const item=x=>{object(x);const uid=integer(x.uid,1,r.nextUid-1),name=id(x.id,itemIds);if(uids.has(uid))bad();uids.add(uid);return {uid,id:name,level:number(x.level,1),cd:number(x.cd,0,C().items[name].cooldown+.00001)};};
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
      r.draft=array(s.draft,3).map(choice=>id(choice,upgradeIds));
      if(r.draft.length!==0 && (r.draft.length!==3 || new Set(r.draft).size!==3))bad();
      const chosen=object(s.skills),timers=object(s.abilityTimers);let ranks=0;
      for(const [key,value] of Object.entries(chosen)){
        id(key,upgradeIds);r.skills[key]=integer(value,1);ranks+=value;
      }
      if(ranks!==r.level-1-(r.draft.length?1:0))bad();
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
      e.burn=number(v.burn);e.poison=number(v.poison);e.chill=number(v.chill,0,2.1);
      e.stun=number(v.stun,0,.95);e.statusClock=number(v.statusClock,0,1);
      e.echoes=array(v.echoes,40).map(x=>{object(x);return {delay:number(x.delay,0,1),damage:number(x.damage),el:id(x.el,elements),pattern:id(x.pattern,new Set(['echo','rapid']))};});
      return e;
    }).filter(e=>e.hp>0);
    r.enemy=r.enemies[0]||null;
    return p;
  }
  root.GameEngine={routeInfo,isReplay,lootOdds,dropEligible,freshProfile,start,tick,act,validate,xpNeeded,previewPair,previewCombine,slotAt,perks,perkCost,perkPreview,awardBreakdown,maxSpeed,itemPower,itemCooldown,levelText,walkDuration:WALK,activeCapacity:ACTIVE,packCapacity:PACK};
})();
