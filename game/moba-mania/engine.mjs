import { HEROES, DIFFICULTIES, WORLD, RULES, lanePoint, segmentDistance, xpToNextLevel } from './data.mjs';
import { castHeroSkill, botSkillAim } from './hero-skills.mjs';
import { ITEMS, itemBonus, purchaseState } from './items.mjs';

const clamp=(v,a,b)=>Math.max(a,Math.min(b,v));
const dist=(a,b)=>Math.hypot(a.x-b.x,a.y-b.y);
const lerp=(a,b,t)=>a+(b-a)*t;
const structure=u=>u.kind==='tower'||u.kind==='core';
const hostile=(a,b)=>a.team!==b.team && b.alive;
const COLORS={physical:'#fff1cf',magic:'#86d8ff',fire:'#ff9e5b',poison:'#a9ee78',heal:'#98f2b8',shield:'#beccff'};
const movementStatus=layers=>{
  const strongest=layers.reduce((best,s)=>(s.factor||0)>(best.factor||0)?s:best,layers[0]);
  return {...strongest,layers,remaining:Math.max(...layers.map(s=>s.remaining)),factor:Math.max(...layers.map(s=>s.factor||0)),
    attack:Math.max(...layers.map(s=>s.attack||0)),frost:layers.some(s=>s.frost)};
};

/** Deterministic, DOM-free battle simulation; all durations are seconds. */
export class Game {
  constructor({hero='arc',difficulty='normal',seed=Date.now()}={}) {
    this.difficulty=DIFFICULTIES[difficulty]?difficulty:'normal';
    this.seed=Number(seed)>>>0||1; this.randomState=this.seed;
    this.time=0; this.paused=false; this.winner=null; this.units=[]; this.projectiles=[];
    this.effects=[]; this.texts=[]; this.events=[]; this.zones=[]; this.nextId=1; this.eventId=1;
    this.stats={kills:[0,0],towers:[0,0],beasts:[0,0],damage:[0,0]};
    this.waveIn=RULES.firstWave; this.waveNumber=0; this.beastIn=RULES.firstBeast; this.beastIndex=0;
    this.teamPlans=[{type:'lanes',lane:1,until:20},{type:'lanes',lane:1,until:20}];
    this.planIn=1; this.order=null; this.aim={x:600,y:1100}; this.moveTarget=null;this.attackOrder=null;this.attackHeld=false;
    this.campStates=WORLD.camps.map(c=>({...c,respawnIn:0,spawned:false}));
    this.grid=new Map(); this.navCache=new Map();
    const chosen=HEROES[hero]?hero:'arc';
    const partners={arc:['dragon','druid'],dragon:['angel','ranger'],druid:['bulwark','seraph'],angel:['dragon','ember'],ranger:['bulwark','seraph'],bulwark:['arc','ember'],ember:['bulwark','angel'],seraph:['bulwark','ranger']};
    const allies=[chosen,...(partners[chosen]||Object.keys(HEROES).filter(id=>id!==chosen).slice(0,2))];
    const pool=Object.keys(HEROES).filter(id=>!allies.includes(id));
    // A seeded shuffle varies the opposing lineup while retaining exactly three heroes per side.
    for(let i=pool.length-1;i>0;i--){const j=Math.floor(this.random()*(i+1));[pool[i],pool[j]]=[pool[j],pool[i]];}
    const enemies=pool.slice(0,3);
    for(let team=0;team<2;team++) {
      const base=WORLD.bases[team];
      this.addUnit('core',team,base.x,base.y,{r:48,hp:4000,range:270,damage:58,attackRate:1.25,name:team?'Rose Crystal':'Azure Crystal'});
      for(let lane=0;lane<3;lane++)for(let tier=0;tier<2;tier++) {
        const p=lanePoint(lane,team?1-(tier===0?.31:.13):(tier===0?.31:.13));
        this.addUnit('tower',team,p.x,p.y,{r:28,hp:tier===0?1950:2350,range:255,damage:tier===0?55:68,attackRate:1.05,lane,tier,name:WORLD.lanes[lane].name+' '+(tier?'inner':'outer')+' tower'});
      }
      const roster=team?enemies:allies;
      const jungleSlot=roster.findIndex(id=>HEROES[id].aiRole==='jungler'||id==='druid'||id==='ranger'||id==='ember');
      roster.forEach((id,slot)=> {
        const h=this.makeHero(id,team,slot,team===0&&slot===0);
        h.role=slot===jungleSlot?'jungler':HEROES[id].aiRole==='support'||id==='angel'?'support':'laner';
        if(h.isPlayer){this.player=h;this.playerId=h.id;}
      });
    }
    for(const camp of this.campStates)this.spawnCamp(camp);
    this.rebuildGrid();
    this.announce('Protect your crystal. Break a lane, then destroy theirs.','start');
  }
  random(){let x=this.randomState;x^=x<<13;x^=x>>>17;x^=x<<5;this.randomState=x>>>0;return this.randomState/4294967296;}
  addUnit(kind,team,x,y,opts={}) {
    const u={id:this.nextId++,kind,team,x,y,r:12,hp:100,maxHp:100,mana:0,maxMana:0,alive:true,angle:team?Math.PI:0,
      moving:false,attackAnim:0,attackTimer:.1,status:{},shield:0,shieldTime:0,level:1,speed:120,range:42,damage:20,attackRate:1,
      lane:1,targetId:null,vx:0,vy:0,splashIn:0,deathTime:0,...opts};
    u.maxHp=opts.maxHp??u.hp;u.maxMana=opts.maxMana??u.mana;
    this.units.push(u);return u;
  }
  makeHero(heroId,team,slot,isPlayer=false){
    const def=HEROES[heroId],well=WORLD.bases[team].well;
    const h=this.addUnit('hero',team,well.x+(team?-45:45),well.y+(slot-1)*38,{heroId,name:isPlayer?def.name:def.name+' · '+(team?'Rose':'Azure'),
      hp:def.hp,mana:def.mana,speed:def.speed,range:def.range,damage:def.damage,attackRate:def.attackRate,r:17,
      isPlayer,bot:!isPlayer,slot,lane:slot===0?1:slot===1?0:2,role:slot===2?'jungler':heroId==='angel'?'support':'laner',
      xp:0,xpNext:xpToNextLevel(1),level:1,cooldowns:[0,0,0,0],talents:{},kills:0,deaths:0,assists:0,gold:0,goldEarned:0,items:[],lastGoldGain:null,shopIn:1,respawnIn:0,aiState:'departing',
      thinkIn:this.random()*.25,brain:{},lastDamageAt:-100,contributors:{},recallIn:0,baseHp:def.hp,baseMana:def.mana});
    return h;
  }
  bonus(hero,key){
    if(!hero?.heroId)return 0;
    let value=itemBonus(hero,key);for(const t of HEROES[hero.heroId].talents){const selected=t.choices.find(c=>c.id===hero.talents[t.level]);value+=selected?.bonuses?.[key]||0;}
    return value;
  }
  getAbility(hero,slot){
    const raw=HEROES[hero?.heroId]?.abilities[slot];if(!raw)return null;
    let radius=raw.radius*(1+this.bonus(hero,'area')),range=raw.range*(1+this.bonus(hero,'range'));
    if(raw.id==='flame-ring')radius*=1+this.bonus(hero,'flameRadius');
    if(raw.id==='stun-bomb')radius*=1+this.bonus(hero,'bombRadius');
    if(raw.id==='royal-quake')radius*=1+this.bonus(hero,'quakeRadius');
    if(raw.id==='glacial-sweep'){radius*=1+this.bonus(hero,'sweepRadius');range*=1+this.bonus(hero,'sweepRadius');}
    if(raw.id==='northern-inspire')radius*=1+this.bonus(hero,'inspireArea');
    if(raw.id==='rampart-rush')range*=1+this.bonus(hero,'rushDistance');
    if(raw.id==='cinder-dash'||raw.id==='flicker-strike')range*=1+this.bonus(hero,'dashDistance');
    return {...raw,range,radius,cooldown:raw.cooldown*(1-Math.min(.5,this.bonus(hero,'cooldown')))};
  }
  availableTalents(hero){return HEROES[hero?.heroId]?.talents.filter(t=>t.level<=hero.level&&!hero.talents[t.level])||[];}
  chooseTalent(hero,tierLevel,choiceId){
    if(!hero?.heroId||hero.level<tierLevel||hero.talents[tierLevel]||this.winner!==null)return false;
    const t=HEROES[hero.heroId].talents.find(t=>t.level===tierLevel),choice=t?.choices.find(c=>c.id===choiceId);if(!choice)return false;
    const oldHp=hero.maxHp,oldMana=hero.maxMana;hero.talents[tierLevel]=choiceId;this.refreshStats(hero);
    hero.hp=Math.min(hero.maxHp,hero.hp+hero.maxHp-oldHp);hero.mana=Math.min(hero.maxMana,hero.mana+hero.maxMana-oldMana);
    this.effect('level',hero.x,hero.y,52,HEROES[hero.heroId].color,.75);
    if(hero.isPlayer)this.announce(choice.name+' learned.','talent');return true;
  }
  refreshStats(hero){
    const def=HEROES[hero.heroId];hero.maxHp=Math.round((def.hp+(hero.level-1)*62)*(1+this.bonus(hero,'hp')));
    hero.maxMana=Math.round((def.mana+(hero.level-1)*18)*(1+this.bonus(hero,'mana')));
    hero.damage=(def.damage+(hero.level-1)*4.2)*(1+this.bonus(hero,'damage'));
    hero.range=def.range*(1+this.bonus(hero,'range'));hero.speed=def.speed*(1+this.bonus(hero,'speed'));
    hero.attackRate=def.attackRate/(1+this.bonus(hero,'attackSpeed'));
  }
  buyItem(hero,itemId){
    if(hero?.kind!=='hero'||!this.units.includes(hero)||this.winner!==null)return false;
    const state=purchaseState(hero,itemId);if(!state.ok)return false;
    const oldHp=hero.maxHp,oldMana=hero.maxMana;
    hero.gold-=state.item.cost;hero.items.push(state.item.id);this.refreshStats(hero);
    // A health upgrade adds its new capacity once; it never heals a dead hero or refills missing HP.
    if(hero.alive){hero.hp=Math.min(hero.maxHp,hero.hp+hero.maxHp-oldHp);hero.mana=Math.min(hero.maxMana,hero.mana+hero.maxMana-oldMana);}
    if(hero.isPlayer)this.announce(state.item.name+' equipped. Its bonuses are active.','purchase');
    return true;
  }
  gainXP(hero,amount){
    if(hero?.kind!=='hero'||!Number.isFinite(amount)||amount<=0||hero.level>=RULES.maxLevel)return;
    hero.xp+=amount;
    while(hero.level<RULES.maxLevel&&hero.xp>=hero.xpNext){
      hero.xp-=hero.xpNext;hero.level++;hero.xpNext=xpToNextLevel(hero.level);
      const oldHp=hero.maxHp,oldMana=hero.maxMana;this.refreshStats(hero);
      if(hero.alive){hero.hp=Math.min(hero.maxHp,hero.hp+(hero.maxHp-oldHp)+65);hero.mana=Math.min(hero.maxMana,hero.mana+(hero.maxMana-oldMana)+35);}
      this.effect('level',hero.x,hero.y,60,'#ffe38e',.9);this.float(hero,'LEVEL '+hero.level,'#ffe38e');
      if(hero.isPlayer)this.announce('Level '+hero.level+(this.availableTalents(hero).length?' · choose a talent':' · growing stronger'),'level');
    }
    if(hero.level===RULES.maxLevel){hero.xp=0;hero.xpNext=0;}
  }
  announce(text,type='info'){this.events.push({id:this.eventId++,text,time:this.time,type});if(this.events.length>60)this.events.shift();}
  effect(type,x,y,r,color,life=0.6,extra={}){const e={type,x,y,r,color,life,maxLife:life,...extra};this.effects.push(e);if(this.effects.length>400)this.effects.splice(0,this.effects.length-400);return e;}
  float(unit,text,color='#fff1cf'){this.texts.push({x:unit.x+(this.random()-.5)*18,y:unit.y-unit.r-8,text:String(text),color,life:1,maxLife:1});if(this.texts.length>100)this.texts.shift();}
  heal(target,amount,source=null,quiet=false){
    if(!target?.alive||structure(target)||amount<=0)return 0;
    const n=Math.min(target.maxHp-target.hp,amount*(1+this.bonus(source,'healing')));target.hp+=n;
    if(n>=6&&!quiet)this.float(target,'+'+Math.round(n),COLORS.heal);return n;
  }
  shield(target,amount,duration=5){
    if(!target.alive||structure(target))return;
    target.shield=Math.max(target.shield,amount);target.shieldTime=Math.max(target.shieldTime,duration);
  }
  owner(source){return source?.kind==='hero'?source:this.units.find(u=>u.id===source?.ownerId)||source;}
  isProtected(unit){
    if(unit.kind==='tower'&&unit.tier===1)return this.units.some(u=>u.alive&&u.kind==='tower'&&u.team===unit.team&&u.lane===unit.lane&&u.tier===0);
    if(unit.kind==='core')return [0,1,2].every(lane=>this.units.some(u=>u.alive&&u.kind==='tower'&&u.team===unit.team&&u.lane===lane&&u.tier===1));
    return false;
  }
  damage(target,amount,source,type='physical',quiet=false){
    if(!target?.alive||!Number.isFinite(amount)||amount<=0||this.winner!==null)return 0;
    if(source&&source.team===target.team)return 0;
    if(this.isProtected(target)){if(!quiet&&source?.isPlayer&&(!target.protectTextAt||this.time-target.protectTextAt>1)){this.float(target,'SHIELDED','#a6b7f0');target.protectTextAt=this.time;}return 0;}
    const owner=this.owner(source);
    if((target.kind==='camp'||(target.kind==='beast'&&target.team===-1))&&owner?.heroId)amount*=1+this.bonus(owner,'jungleDamage');
    if(target.status.guard)amount*=target.status.guard.factor;
    const impact=amount;
    if(target.shield>0){const absorbed=Math.min(target.shield,amount);target.shield-=absorbed;amount-=absorbed;if(absorbed>8&&!quiet)this.float(target,Math.round(absorbed),'#b8caff');}
    target.lastDamageAt=this.time;target.recallIn=0;
    if(target.team===-1&&source){target.aggroId=source.id;target.aggroUntil=this.time+7;}
    if(owner?.kind==='hero'&&owner.team!==target.team&&impact>0){target.contributors??={};target.contributors[owner.id]=this.time;if(target.team>=0)this.stats.damage[owner.team]+=Math.min(target.hp,amount);}
    if(target.kind==='hero'&&owner?.kind==='hero'&&owner.alive&&owner.team!==target.team){
      for(const tower of this.units)if(structure(tower)&&tower.team===target.team&&tower.alive&&dist(tower,target)<=tower.range+target.r&&dist(tower,owner)<=tower.range+owner.r){
        tower.aggroId=owner.id;tower.aggroUntil=this.time+3;tower.targetId=owner.id;
      }
    }
    const actual=Math.min(target.hp,amount);target.hp-=amount;
    if(actual>1&&!quiet)this.float(target,Math.round(actual),COLORS[type]||type);
    if(target.hp<=0)this.kill(target,source);
    return actual;
  }
  kill(target,source){
    if(!target?.alive||source?.team===target.team)return;
    target.alive=false;target.hp=0;target.deathTime=.7;target.shield=0;
    const owner=this.owner(source),team=owner?.team??source?.team;
    this.effect(target.kind==='core'?'explosion':'ring',target.x,target.y,target.kind==='core'?170:target.r*2.5,target.team===0?'#64caff':target.team===1?'#ff98be':'#e5d698',.7);
    const xp=target.kind==='hero'?150+target.level*12:target.kind==='tower'?115:target.kind==='camp'?55:target.kind==='beast'?200+40*((target.tier||1)-1):target.kind==='footman'?22:target.kind==='archer'?28:target.kind==='summon'?9:0;
    // Only an actual hero finisher and its recent combat partners receive personal rewards.
    // Proximity, an allied objective capture, and owning an ordinary lane unit grant nothing.
    const killer=owner?.kind==='hero'&&owner.team!==target.team&&(team===0||team===1)?owner:null;
    let assistants=[];
    if(team===0||team===1){
      assistants=this.units.filter(u=>{
        const hitAt=target.contributors?.[u.id],age=this.time-hitAt;
        return u.kind==='hero'&&u.team===team&&u.team!==target.team&&u.id!==killer?.id&&Number.isFinite(hitAt)&&age>=0&&age<=RULES.assistWindow;
      });
      if(killer)this.gainXP(killer,xp);
      for(const hero of assistants)this.gainXP(hero,xp*RULES.assistXpFraction);
    }
    const gold=target.kind==='hero'?180+target.level*10:target.kind==='tower'?130:target.kind==='camp'?40:target.kind==='beast'?180+40*((target.tier||1)-1):target.kind==='footman'?18:target.kind==='archer'?24:target.kind==='summon'?6:0;
    if(killer&&gold){
      killer.gold+=gold;killer.goldEarned+=gold;killer.lastGoldGain={amount:gold,time:this.time,x:target.x,y:target.y};
      if(killer.isPlayer)this.float(killer,'+'+gold+' GOLD','#ffd76b');
    }
    if(target.kind==='hero'){
      if(target.isPlayer)this.cancelPlayerOrder();
      target.deaths++;target.respawnIn=7+target.level*1.25+Math.min(8,this.time/120);target.recallIn=0;target.status={};
      if(team===0||team===1){this.stats.kills[team]++;if(killer)killer.kills++;
        for(const hero of assistants)hero.assists++;
      }
      this.announce((owner?.heroId?HEROES[owner.heroId].name:source?.name||'The wilds')+' defeated '+HEROES[target.heroId].name+'.','kill');
    }
    if(target.kind==='tower'){
      if(team===0||team===1)this.stats.towers[team]++;
      this.announce((target.team===0?'Azure':'Rose')+' '+WORLD.lanes[target.lane].name.toLowerCase()+' '+(target.tier?'inner':'outer')+' tower has fallen!','tower');
    }
    if(target.kind==='camp'){
      const camp=this.campStates.find(c=>c.id===target.campId);
      if(camp&&!this.units.some(u=>u.alive&&u.kind==='camp'&&u.campId===camp.id)){camp.respawnIn=RULES.campRespawn;camp.spawned=false;if(owner?.isPlayer)this.announce('Camp cleared · returns in '+RULES.campRespawn+'s.','camp');}
    }
    if(target.kind==='beast'&&target.team===-1&&(team===0||team===1)){
      this.stats.beasts[team]++;
      const lane=this.teamPlans[team]?.lane??target.lane,p=lanePoint(lane,.5);
      const tier=target.tier||1;
      const allied=this.addUnit('beast',team,p.x,p.y,{subtype:target.subtype,name:target.name,tier,level:tier,scale:1+Math.min(.3,(tier-1)*.04),r:34,hp:1350*(1+(tier-1)*.24),damage:90*(1+(tier-1)*.17),speed:106,range:58,attackRate:1.15,lane,waypoint:0,siege:true});
      allied.path=this.lanePath(lane,team,allied);allied.waypoint=0;
      this.effect('level',p.x,p.y,120,team?'#ff98be':'#66d8ff',1.3);
      this.announce((team===0?'Azure':'Rose')+' claimed tier '+tier+' '+target.name+'! It marches '+WORLD.lanes[lane].name.toLowerCase()+'.','beast');
    }
    if(target.kind==='core'){
      this.winner=1-target.team;this.announce(this.winner===0?'VICTORY · The Rose Crystal is shattered!':'DEFEAT · The Azure Crystal has fallen.','end');
    }
  }
  applyStatus(target,type,duration,extra={}){
    if(!target.alive||structure(target))return;
    const old=target.status[type];
    if(type==='haste'||type==='slow'){
      // Keep each strength's own lifetime: inspiration cannot weaken or extend a faster dash buff.
      const layers=old?.layers|| (old?[{...old}]:[]);
      const same=layers.find(s=>['factor','attack','frost','color'].every(key=>s[key]===extra[key]));
      if(same)same.remaining=Math.max(same.remaining,duration);else layers.push({...extra,remaining:duration});
      target.status[type]=movementStatus(layers);
    }else target.status[type]={...old,...extra,remaining:Math.max(duration,old?.remaining||0)};
  }
  spawnWave(team){
    const scaling=1+Math.floor(this.time/105)*.12;
    for(let lane=0;lane<3;lane++)for(let i=0;i<5;i++){
      const p=lanePoint(lane,team?.985:.015),ranged=i>=3;
      const u=this.addUnit(ranged?'archer':'footman',team,p.x+(team?1:-1)*i*15,p.y+(i%3-1)*15,{hp:Math.round((ranged?100:155)*scaling),damage:(ranged?23:17)*scaling,
        range:ranged?185:34,speed:ranged?107:113,r:ranged?9:11,attackRate:ranged?1.12:.9,lane,waypoint:0});
      u.path=this.lanePath(lane,team,u);
    }
  }
  lanePath(lane,team,from){
    const points=WORLD.lanes[lane].points.map(p=>({...p}));if(team)points.reverse();
    if(!from)return points;
    let nearest=0,best=Infinity;for(let i=0;i<points.length-1;i++){const d=segmentDistance(from.x,from.y,points[i],points[i+1]);if(d<best){best=d;nearest=i;}}
    const path=points.slice(nearest+1);
    while(path.length>1&&dist(from,path[0])<72)path.shift();
    return path;
  }
  spawnCamp(camp){
    const config={boar:{hp:225,damage:23,speed:112,r:15},wolf:{hp:175,damage:26,speed:150,r:13},golem:{hp:315,damage:28,speed:87,r:19}}[camp.type];
    for(let i=0;i<3;i++){
      const angle=i*Math.PI*2/3,u=this.addUnit('camp',-1,camp.x+Math.cos(angle)*30,camp.y+Math.sin(angle)*30,{...config,hp:config.hp*(i===0?1.4:.7),damage:config.damage*(i===0?1.2:.75),
        subtype:camp.type,campId:camp.id,homeX:camp.x,homeY:camp.y,range:36,attackRate:1.2,aggroUntil:0,name:camp.type==='golem'?'Grove Golem':camp.type==='boar'?'Bramble Boar':'Wild Wolf',leader:i===0});
      u.scale=i===0?1.15:.8;
    }
    camp.spawned=true;camp.respawnIn=0;
  }
  spawnBeast(index=0){
    const pit=WORLD.beastPits[index%WORLD.beastPits.length];
    if(this.units.some(u=>u.alive&&u.kind==='beast'&&u.team===-1&&u.pitId===pit.id))return null;
    const tier=1+Math.floor(this.time/100);
    const u=this.addUnit('beast',-1,pit.x,pit.y,{subtype:pit.type,pitId:pit.id,tier,level:tier,scale:1+Math.min(.3,(tier-1)*.04),name:pit.type==='maw'?'River Maw':'Moss Colossus',hp:1350*(1+(tier-1)*.22),damage:50*(1+(tier-1)*.16),
      speed:95,range:65,r:36,attackRate:1.2,lane:pit.lane,homeX:pit.x,homeY:pit.y,aggroUntil:0,slamIn:4});
    this.effect('explosion',u.x,u.y,150,'#e8ca77',1.2);this.announce('Tier '+tier+' '+u.name+' awakens between the lanes! Defeat it to summon a siege beast.','beast');return u;
  }
  recall(hero){
    if(!hero?.alive||hero.kind!=='hero'||hero.recallIn>0||this.winner!==null||this.paused)return false;
    if(hero.isPlayer)this.cancelPlayerOrder();
    hero.recallIn=RULES.recallTime;hero.recallStart={x:hero.x,y:hero.y};hero.targetId=null;
    if(hero.isPlayer){this.moveTarget=null;this.announce('Recalling · stay still for '+RULES.recallTime+' seconds.','recall');}return true;
  }
  command(type,lane=1){
    if(!['push','defend','beast'].includes(type)||this.winner!==null)return false;
    this.cancelPlayerOrder();
    this.order={type,lane:clamp(Number(lane)||0,0,2),until:this.time+22};
    this.teamPlans[0]={...this.order};
    this.announce(type==='beast'?'Team: contest the river beast!':'Team: '+(type==='push'?'push ':'defend ')+WORLD.lanes[this.order.lane].name.toLowerCase()+' lane.','ping');return true;
  }
  cast(hero,slot,aim){
    const a=this.getAbility(hero,slot);
    if(!a||!hero.alive||hero.level<a.unlock||hero.cooldowns[slot]>0||hero.mana<a.mana||hero.status.stun||this.paused||this.winner!==null)return false;
    if(!aim||!Number.isFinite(aim.x)||!Number.isFinite(aim.y))return false;
    this.rebuildGrid();
    const dx=aim.x-hero.x,dy=aim.y-hero.y,d=Math.hypot(dx,dy)||1,angle=d>1?Math.atan2(dy,dx):hero.angle;
    const at={x:clamp(hero.x+dx/d*Math.min(d,a.range),28,WORLD.width-28),y:clamp(hero.y+dy/d*Math.min(d,a.range),28,WORLD.height-28)};
    hero.mana-=a.mana;hero.cooldowns[slot]=a.cooldown;hero.recallIn=0;hero.angle=angle;hero.attackAnim=.32;
    const power=(1+(hero.level-1)*.075)*(1+this.bonus(hero,'damage'))*(1+this.bonus(hero,'spellDamage'));
    const zone=(type,duration,props={})=>this.zones.push({type,x:at.x,y:at.y,r:a.radius,life:duration,maxLife:duration,ownerId:hero.id,team:hero.team,tickIn:0,...props});
    const area=(center,r,damage,type='magic',status=null)=>{
      for(const target of this.nearby(center.x,center.y,r+55))if(hostile(hero,target)&&dist(center,target)<r+target.r){this.damage(target,damage*(structure(target)?.35:1),hero,type);if(status)this.applyStatus(target,status.type,status.duration,{...status,sourceId:hero.id});}
    };
    const shieldAmount=n=>n*(1+this.bonus(hero,'shield'))*(1+(hero.level-1)*.08);
    switch(a.id){
      case 'blue-storm':
        zone('storm',3.6,{delay:.55,tickIn:.55,damage:44*power*(1+this.bonus(hero,'stormDamage')),color:a.color,detonated:false});
        this.effect('storm',at.x,at.y,a.radius,a.color,3.6,{delay:.55,team:hero.team});break;
      case 'forcefield':
        zone('field',5,{shield:shieldAmount(130),color:a.color,applied:new Set(),healing:this.bonus(hero,'fieldHeal')});
        this.effect('field',at.x,at.y,a.radius,a.color,5,{team:hero.team});break;
      case 'spirit-warriors':this.summon(hero,'spirit',3+this.bonus(hero,'spiritCount'),at,14);break;
      case 'rift-step':{
        const end=this.safePoint(at,hero.r);this.effect('blink',hero.x,hero.y,55,a.color,.5);hero.x=end.x;hero.y=end.y;hero.path=null;hero.pathGoal=null;
        this.effect('explosion',hero.x,hero.y,a.radius,a.color,.7);area(hero,a.radius,130*power);break;}
      case 'dragon-flight':this.fireSkill(hero,angle,{type:'dragon',color:a.color,r:a.radius,range:a.range,speed:420,damage:105*power,pierce:99,damageType:'fire',burn:9*power*(1+this.bonus(hero,'burnDamage')),burnDuration:4+this.bonus(hero,'burnDuration')});break;
      case 'flame-ring':this.effect('flame',hero.x,hero.y,a.radius,a.color,.9);area(hero,a.radius,95*power*(1+this.bonus(hero,'flameDamage')),'fire',{type:'burn',duration:4+this.bonus(hero,'burnDuration'),dps:12*power*(1+this.bonus(hero,'burnDamage'))});break;
      case 'dragon-shield':this.applyStatus(hero,'guard',5+this.bonus(hero,'guardDuration'),{factor:.5});this.shield(hero,shieldAmount(100),5+this.bonus(hero,'guardDuration'));this.effect('shield',hero.x,hero.y,hero.r+18,a.color,.7,{followId:hero.id});break;
      case 'sky-cleave':{
        const end=this.safePoint(at,hero.r),start={x:hero.x,y:hero.y};hero.x=end.x;hero.y=end.y;hero.path=null;hero.pathGoal=null;
        this.effect('slash',start.x,start.y,35,a.color,.45,{x2:hero.x,y2:hero.y,angle});this.effect('explosion',hero.x,hero.y,a.radius,a.color,.6);area(hero,a.radius,130*power,'fire',{type:'stun',duration:1});break;}
      case 'healing-tree':zone('tree',8+this.bonus(hero,'treeDuration'),{healing:22+hero.level*2.5,slow:this.bonus(hero,'treeSlow'),color:a.color});this.effect('tree',at.x,at.y,a.radius,a.color,8+this.bonus(hero,'treeDuration'),{team:hero.team});break;
      case 'treants':this.summon(hero,'treant',2,at,16+this.bonus(hero,'summonDuration'));break;
      case 'bear':this.summon(hero,'bear',1+this.bonus(hero,'bearCount'),at,18+this.bonus(hero,'summonDuration'));break;
      case 'wolves':this.summon(hero,'wolf',3+this.bonus(hero,'wolfCount'),at,15+this.bonus(hero,'summonDuration'));break;
      case 'rose-light':zone('heal',3.2,{healing:35+hero.level*3,damage:27*power,color:a.color});this.effect('heal',at.x,at.y,a.radius,a.color,3.2,{team:hero.team});break;
      case 'inspire':
        this.effect('ring',at.x,at.y,a.radius,a.color,1);for(const ally of this.nearby(at.x,at.y,a.radius+45))if(ally.alive&&ally.team===hero.team&&!structure(ally)&&dist(ally,at)<a.radius+ally.r)this.applyStatus(ally,'haste',5+this.bonus(hero,'inspireDuration'),{factor:.3,attack:.25});break;
      case 'sanctuary':
        this.effect('field',at.x,at.y,a.radius,a.color,1.1,{team:hero.team});for(const ally of this.nearby(at.x,at.y,a.radius+45))if(ally.alive&&ally.team===hero.team&&dist(ally,at)<a.radius+ally.r)this.shield(ally,shieldAmount(190),6);break;
      case 'seraph-lance':this.fireSkill(hero,angle,{type:'lance',color:a.color,r:a.radius,range:a.range,speed:590,damage:150*power,pierce:99,healing:110,damageType:'magic'});break;
      case 'venom-arrow':this.fireSkill(hero,angle,{type:'poison',color:a.color,r:a.radius,range:a.range,speed:565,damage:70*power,pierce:0,poison:15*power*(1+this.bonus(hero,'poisonDamage')),poisonDuration:5+this.bonus(hero,'poisonDuration'),damageType:'poison'});break;
      case 'fan-arrows':{
        const n=5+this.bonus(hero,'fanCount');for(let i=0;i<n;i++)this.fireSkill(hero,angle+(i-(n-1)/2)*.155,{type:'arrow',color:a.color,r:a.radius,range:a.range,speed:550,damage:52*power,pierce:0,damageType:'physical'});break;}
      case 'stun-bomb':zone('bomb',.6,{delay:.55,tickIn:.55,damage:80*power,stun:1.4+this.bonus(hero,'stunDuration'),color:a.color});this.effect('poison',at.x,at.y,a.radius,a.color,.6,{delay:.55,team:hero.team});break;
      case 'long-shot':this.fireSkill(hero,angle,{type:'longshot',color:a.color,r:a.radius,range:a.range,speed:830,damage:205*power*(1+this.bonus(hero,'longshotDamage')),pierce:3,damageType:'physical'});break;
      default:castHeroSkill(this,hero,a,{aim,angle,at,power,area,shieldAmount,zone});break;
    }
    if(hero.isPlayer)this.cancelPlayerOrder();
    const restore=this.bonus(hero,'castHeal');if(restore)this.heal(hero,hero.maxHp*restore,hero);
    return true;
  }
  summon(hero,subtype,count,at,lifetime){
    // A fresh cast replaces that owner's old same-species summons; no unbounded army stacking.
    for(const old of this.units)if(old.alive&&old.kind==='summon'&&old.ownerId===hero.id&&old.subtype===subtype){old.alive=false;old.deathTime=.25;}
    const config={spirit:{hp:240,damage:28,speed:184,range:43,r:13},treant:{hp:420,damage:27,speed:124,range:46,r:19},bear:{hp:740,damage:61,speed:151,range:50,r:23},wolf:{hp:200,damage:31,speed:218,range:37,r:12}}[subtype];
    const scale=(1+(hero.level-1)*.08)*(1+this.bonus(hero,'summonPower'));
    for(let i=0;i<count;i++){
      const angle=i*Math.PI*2/count,p=this.safePoint({x:at.x+Math.cos(angle)*35,y:at.y+Math.sin(angle)*35},config.r);
      this.addUnit('summon',hero.team,p.x,p.y,{...config,hp:config.hp*scale,damage:config.damage*scale,subtype,ownerId:hero.id,lifetime,attackRate:.95/(1+this.bonus(hero,'summonHaste')),lane:hero.lane,level:hero.level});
      this.effect('level',p.x,p.y,40,HEROES[hero.heroId].color,.6);
    }
  }
  fireSkill(hero,angle,props){
    const p={id:this.nextId++,x:hero.x+Math.cos(angle)*(hero.r+8),y:hero.y+Math.sin(angle)*(hero.r+8),prevX:hero.x,prevY:hero.y,angle,
      vx:Math.cos(angle)*props.speed,vy:Math.sin(angle)*props.speed,team:hero.team,ownerId:hero.id,sourceId:hero.id,life:props.range/props.speed,hit:new Set(),...props};
    this.projectiles.push(p);return p;
  }
  basicAttack(unit,target){
    if(!unit?.alive||!target?.alive||!hostile(unit,target)||this.isProtected(target)||unit.attackTimer>0||unit.status.stun||unit.recallIn)return false;
    const d=dist(unit,target);if(d>unit.range+target.r+unit.r*.35)return false;
    unit.attackTimer=unit.attackRate/(1+(unit.status.haste?.attack||0));unit.attackAnim=.26;unit.angle=Math.atan2(target.y-unit.y,target.x-unit.x);
    if(unit.range>100){
      const color=unit.kind==='tower'||unit.kind==='core'?(unit.team?'#ff98bc':'#87d5ff'):HEROES[unit.heroId]?.color||'#edd8a0';
      let attackDamage=unit.damage;
      if(structure(unit)){
        const continuing=unit.focusId===target.id&&this.time-(unit.lastShotAt??-100)<3.2;
        unit.focusShots=target.kind==='hero'?(continuing?Math.min(3,(unit.focusShots||0)+1):0):0;
        unit.focusId=target.id;unit.lastShotAt=this.time;
        if(target.kind==='hero')attackDamage*=1.35*(1+unit.focusShots*.25);
      }
      this.projectiles.push({id:this.nextId++,x:unit.x,y:unit.y-8,prevX:unit.x,prevY:unit.y-8,angle:unit.angle,team:unit.team,ownerId:unit.ownerId??unit.id,sourceId:unit.id,
        targetId:target.id,speed:structure(unit)?650:unit.kind==='archer'?400:520,life:2.5,type:unit.heroId==='ranger'||unit.kind==='archer'?'arrow':'bolt',color,r:structure(unit)?6:4,damage:attackDamage,pierce:0,hit:new Set(),basic:true});
    }else{
      this.effect('slash',unit.x,unit.y,unit.range,'#ffe7b9',.2,{angle:unit.angle,x2:target.x,y2:target.y});
      const dealt=this.damage(target,unit.damage,unit,'physical');
      if(unit.subtype==='bear')this.applyStatus(target,'slow',1.2,{factor:.3});
      if(unit.siege&&structure(target))this.damage(target,unit.damage*.8,unit,'physical',true);
      const life=this.bonus(unit,'lifesteal');if(life)this.heal(unit,dealt*life,unit,true);
    }
    return true;
  }
  update(dt,input={}){
    if(this.paused||this.winner!==null||!Number.isFinite(dt)||dt<=0)return;
    this.playerCommands(input);
    let left=Math.min(dt,2);while(left>1e-8&&this.winner===null){const step=Math.min(.05,left);this.step(step,input);left-=step;}
  }
  step(dt,input){
    this.time+=dt;this.waveIn-=dt;this.beastIn-=dt;
    if(this.waveIn<=0){this.spawnWave(0);this.spawnWave(1);this.waveNumber++;this.waveIn+=RULES.waveInterval;}
    if(this.beastIn<=0){this.spawnBeast(this.beastIndex++);this.beastIn+=RULES.beastInterval;}
    for(const camp of this.campStates)if(!camp.spawned){camp.respawnIn-=dt;if(camp.respawnIn<=0)this.spawnCamp(camp);}
    this.rebuildGrid();this.planIn-=dt;if(this.planIn<=0){this.planIn=1.6;this.planTeams();}
    for(const unit of this.units){
      unit.moving=false;unit.vx=unit.vy=0;unit.attackAnim=Math.max(0,unit.attackAnim-dt);unit.attackTimer=Math.max(0,unit.attackTimer-dt);unit.splashIn=Math.max(0,unit.splashIn-dt);
      if(!unit.alive){unit.deathTime-=dt;if(unit.kind==='hero'){unit.respawnIn=Math.max(0,unit.respawnIn-dt);if(unit.respawnIn===0)this.respawn(unit);}continue;}
      this.tickStatuses(unit,dt);if(!unit.alive)continue;
      if(unit.kind==='hero'){
        for(let i=0;i<4;i++)unit.cooldowns[i]=Math.max(0,unit.cooldowns[i]-dt);
        unit.mana=Math.min(unit.maxMana,unit.mana+(3.7+this.bonus(unit,'manaRegen'))*dt);
        if(this.time-unit.lastDamageAt>6)this.heal(unit,1.5*dt,null,true);
        const well=WORLD.bases[unit.team].well;
        if(dist(unit,well)<RULES.wellRadius){this.heal(unit,unit.maxHp*.17*dt,null,true);unit.mana=Math.min(unit.maxMana,unit.mana+unit.maxMana*.2*dt);}
        if(unit.isPlayer&&unit.recallIn>0&&(Math.hypot(input.move?.x||0,input.move?.y||0)>.05||input.moveTo||input.attack)){unit.recallIn=0;}
        if(unit.recallIn>0){unit.recallIn-=dt;if(unit.recallIn<=0){unit.x=well.x;unit.y=well.y;unit.path=null;unit.targetId=null;this.effect('blink',unit.x,unit.y,70,HEROES[unit.heroId].color,.8);if(unit.isPlayer)this.moveTarget=null;}continue;}
        if(unit.status.stun)continue;
        if(unit.isPlayer)this.tickPlayer(unit,dt,input);
        else if(unit.bot)this.tickBot(unit,dt);
      }else if(unit.status.stun)continue;
      else if(structure(unit))this.tickStructure(unit);
      else if(unit.team===-1)this.tickNeutral(unit,dt);
      else if(unit.kind==='summon')this.tickSummon(unit,dt);
      else this.tickLaneUnit(unit,dt);
    }
    this.tickProjectiles(dt);this.tickZones(dt);
    for(const e of this.effects){e.life-=dt;if(e.followId){const u=this.units.find(u=>u.id===e.followId);if(u){e.x=u.x;e.y=u.y;}}}
    for(const t of this.texts){t.life-=dt;t.y-=25*dt;}
    this.effects=this.effects.filter(e=>e.life>0);this.texts=this.texts.filter(t=>t.life>0);
    this.units=this.units.filter(u=>u.alive||u.kind==='hero'||structure(u)||u.deathTime>0);
  }
  tickStatuses(u,dt){
    if(u.shieldTime>0){u.shieldTime-=dt;if(u.shieldTime<=0)u.shield=0;}
    for(const [key,s] of Object.entries(u.status)){
      if(s.layers){
        for(const layer of s.layers)layer.remaining-=dt;
        const active=s.layers.filter(layer=>layer.remaining>0);
        if(active.length)u.status[key]=movementStatus(active);else delete u.status[key];
        continue;
      }
      s.remaining-=dt;
      if((key==='burn'||key==='poison')&&s.dps){
        const source=this.units.find(v=>v.id===s.sourceId);this.damage(u,s.dps*dt,source,key==='burn'?'fire':'poison',true);
        s.textIn=(s.textIn||0)-dt;if(s.textIn<=0){this.float(u,Math.round(s.dps*.6),key==='burn'?COLORS.fire:COLORS.poison);s.textIn=.6;}
      }
      if(s.remaining<=0)delete u.status[key];
    }
  }
  respawn(h){
    const well=WORLD.bases[h.team].well;h.x=well.x+(h.team?-36:36);h.y=well.y+(h.slot-1)*30;h.hp=h.maxHp;h.mana=h.maxMana;h.alive=true;h.status={};h.shield=0;h.recallIn=0;
    h.cooldowns=h.cooldowns.map(c=>Math.min(c,3));h.contributors={};h.brain={};h.path=null;h.pathGoal=null;h.targetId=null;h.thinkIn=0;h.aiState='departing';
    if(h.isPlayer)this.cancelPlayerOrder();this.effect('level',h.x,h.y,60,HEROES[h.heroId].color,.9);
  }
  validPlayerTarget(target){return !!target&&target.alive&&hostile(this.player,target)&&!this.isProtected(target);}
  nearestPlayerEnemy(){
    let nearest=null,distance=Infinity;
    for(const target of this.units)if(this.validPlayerTarget(target)){
      const d=dist(this.player,target);if(d<distance){nearest=target;distance=d;}
    }
    return nearest;
  }
  cancelPlayerOrder(){
    this.attackOrder=null;this.moveTarget=null;
    if(this.player){this.player.targetId=null;this.player.path=null;this.player.pathGoal=null;this.player.navPath=null;this.player.navGoal=null;this.player.navTime=0;}
  }
  attackNearest(){
    if(!this.player?.alive||this.paused||this.winner!==null)return false;
    this.cancelPlayerOrder();this.player.recallIn=0;
    const target=this.nearestPlayerEnemy();
    this.attackOrder={type:'attack',targetId:target?.id??null,explicit:false};
    this.player.targetId=this.attackOrder.targetId;return true;
  }
  attackTarget(targetId){
    const target=this.units.find(u=>u.id===targetId);
    if(!this.player?.alive||this.paused||this.winner!==null||!this.validPlayerTarget(target))return false;
    this.cancelPlayerOrder();this.player.recallIn=0;
    this.attackOrder={type:'attack',targetId:target.id,explicit:true};this.player.targetId=target.id;return true;
  }
  movePlayerTo(point){
    if(!this.player?.alive||this.paused||this.winner!==null||!Number.isFinite(point?.x)||!Number.isFinite(point?.y))return false;
    this.cancelPlayerOrder();this.player.recallIn=0;this.moveTarget=this.safePoint(point,this.player.r);return true;
  }
  playerCommands(input){
    const pressed=!!input.attack&&!this.attackHeld;this.attackHeld=!!input.attack;
    if(!this.player.alive)return;
    // A move has priority even during a stun; releasing it must not revive an old chase.
    if(Math.hypot(Number(input.move?.x)||0,Number(input.move?.y)||0)>.05){this.cancelPlayerOrder();this.player.recallIn=0;}
    else if(Number.isFinite(input.moveTo?.x)&&Number.isFinite(input.moveTo?.y))this.movePlayerTo(input.moveTo);
    else if(pressed)this.attackNearest();
  }
  tickPlayer(h,dt,input){
    if(input.aim&&Number.isFinite(input.aim.x)&&Number.isFinite(input.aim.y)){this.aim=input.aim;h.angle=Math.atan2(input.aim.y-h.y,input.aim.x-h.x);}
    const mx=Number(input.move?.x)||0,my=Number(input.move?.y)||0;
    if(Math.hypot(mx,my)>.05){this.moveDirect(h,mx,my,dt);return;}
    if(this.moveTarget){if(dist(h,this.moveTarget)<12)this.moveTarget=null;else this.moveToward(h,this.moveTarget,dt);return;}
    if(!this.attackOrder)return;
    let target=this.units.find(u=>u.id===this.attackOrder.targetId);
    if(!this.attackOrder.explicit||!this.validPlayerTarget(target)){
      const previousId=target?.id;target=this.nearestPlayerEnemy();this.attackOrder.targetId=target?.id??null;this.attackOrder.explicit=false;
      if(target?.id!==previousId){h.path=null;h.pathGoal=null;h.navPath=null;h.navGoal=null;h.navTime=0;}
    }
    h.targetId=target?.id??null;
    if(target){
      h.angle=Math.atan2(target.y-h.y,target.x-h.x);
      if(dist(h,target)>h.range+target.r+h.r*.35)this.moveToward(h,target,dt);
      this.basicAttack(h,target);
    }
  }
  tickStructure(u){
    const valid=t=>t?.alive&&t.team>=0&&t.team!==u.team&&!structure(t)&&dist(u,t)<=u.range+t.r;
    let target=this.units.find(t=>t.id===u.aggroId&&u.aggroUntil>this.time&&valid(t));
    if(!target)target=this.units.find(t=>t.id===u.targetId&&valid(t));
    if(!target){
      const options=this.nearby(u.x,u.y,u.range+50).filter(valid);
      options.sort((a,b)=>((a.kind==='hero'?160:0)+dist(a,u))-((b.kind==='hero'?160:0)+dist(b,u)));target=options[0];
    }
    u.targetId=target?.id??null;
    if(target)this.basicAttack(u,target);
    else{u.focusId=null;u.focusShots=0;}
  }
  tickLaneUnit(u,dt){
    let target=this.units.find(v=>v.id===u.targetId&&v.alive&&v.team!==u.team&&v.team>=0&&dist(v,u)<290&&!this.isProtected(v));
    if(!target)target=this.findTarget(u,225,false);
    if(target){u.targetId=target.id;if(!this.basicAttack(u,target)&&dist(u,target)>u.range+target.r+u.r*.3)this.moveToward(u,target,dt);}
    else{
      u.targetId=null;const path=u.path||this.lanePath(u.lane,u.team,u);u.path=path;
      while(u.waypoint<path.length&&dist(u,path[u.waypoint])<28)u.waypoint++;
      const goal=path[u.waypoint]||WORLD.bases[1-u.team];this.moveToward(u,goal,dt);
    }
  }
  tickSummon(u,dt){
    u.lifetime-=dt;if(u.lifetime<=0){u.alive=false;u.deathTime=.4;this.effect('ring',u.x,u.y,35,'#a9e1cc',.4);return;}
    const owner=this.units.find(v=>v.id===u.ownerId),ownerTarget=this.units.find(v=>v.id===owner?.targetId&&v.alive);
    let target=ownerTarget&&dist(u,ownerTarget)<460?ownerTarget:this.findTarget(u,300,true);
    if(target){u.targetId=target.id;if(!this.basicAttack(u,target)&&dist(u,target)>u.range+target.r)this.moveToward(u,target,dt);}
    else if(owner?.alive&&dist(u,owner)>65)this.moveToward(u,{x:owner.x+Math.cos(u.id)*42,y:owner.y+Math.sin(u.id)*42},dt);
    else if(!owner?.alive)this.tickLaneUnit(u,dt);
  }
  tickNeutral(u,dt){
    const home={x:u.homeX,y:u.homeY};let target=this.units.find(v=>v.id===u.aggroId&&v.alive&&v.team>=0&&dist(v,home)<280);
    if(target&&u.aggroUntil>this.time&&dist(u,home)<270){
      if(u.kind==='camp')for(const ally of this.nearby(u.x,u.y,140))if(ally.kind==='camp'&&ally.campId===u.campId&&!ally.aggroId){ally.aggroId=target.id;ally.aggroUntil=this.time+6;}
      if(!this.basicAttack(u,target)&&dist(u,target)>u.range+target.r)this.moveToward(u,target,dt);
      if(u.kind==='beast'){
        u.slamIn-=dt;if(u.slamIn<=0){u.slamIn=5.5;this.zones.push({type:'wild-slam',x:u.x,y:u.y,r:120,life:.85,maxLife:.85,delay:.7,tickIn:.7,ownerId:u.id,team:-1,damage:70,color:'#e6c878'});this.effect('storm',u.x,u.y,120,'#dfbb75',.85,{delay:.7,team:-1});}
      }
    }else{
      if(u.aggroId)u.contributors={};
      u.aggroId=null;if(dist(u,home)>20)this.moveToward(u,home,dt);
      if(u.hp<u.maxHp)u.hp=Math.min(u.maxHp,u.hp+u.maxHp*.09*dt);
    }
  }
  findTarget(u,range,includeNeutral=false){
    const options=this.nearby(u.x,u.y,range+60).filter(t=>hostile(u,t)&&(includeNeutral||t.team>=0)&&dist(t,u)<=range+t.r&&!this.isProtected(t));
    options.sort((a,b)=>this.targetScore(u,a)-this.targetScore(u,b));return options[0]||null;
  }
  targetScore(u,t){
    let score=dist(u,t);if(t.kind==='hero')score-=55;if(structure(t))score+=80;if(t.team===-1)score+=60;
    if(u.kind==='hero'){score-=Math.max(0,1-t.hp/t.maxHp)*75;const focus=this.teamPlans[u.team]?.focusId;if(t.id===focus)score-=100;}
    return score;
  }
  tickProjectiles(dt){
    for(const p of this.projectiles){
      p.life-=dt;if(p.life<=0)continue;p.prevX=p.x;p.prevY=p.y;
      if(p.targetId){const target=this.units.find(u=>u.id===p.targetId&&u.alive);if(!target){p.life=0;continue;}p.angle=Math.atan2(target.y-p.y,target.x-p.x);p.vx=Math.cos(p.angle)*p.speed;p.vy=Math.sin(p.angle)*p.speed;}
      p.x+=p.vx*dt;p.y+=p.vy*dt;
      const source=this.units.find(u=>u.id===p.sourceId)||this.units.find(u=>u.id===p.ownerId)||{id:p.sourceId,team:p.team,ownerId:p.ownerId};
      const candidates=this.nearby((p.prevX+p.x)/2,(p.prevY+p.y)/2,Math.hypot(p.x-p.prevX,p.y-p.prevY)/2+70).filter(u=>u.alive&&!p.hit.has(u.id)&&u.id!==p.sourceId&&
        (p.targetId?u.id===p.targetId:(u.team!==p.team||p.healing&&!structure(u)))&&segmentDistance(u.x,u.y,{x:p.prevX,y:p.prevY},{x:p.x,y:p.y})<u.r+p.r);
      candidates.sort((a,b)=>Math.hypot(a.x-p.prevX,a.y-p.prevY)-Math.hypot(b.x-p.prevX,b.y-p.prevY));
      for(const target of candidates){
        p.hit.add(target.id);
        if(target.team===p.team&&p.healing)this.heal(target,p.healing,source);
        else{
          const actual=this.damage(target,p.damage*(structure(target)&&!p.basic ? .4 : 1),source,p.damageType||'physical');
          if(p.burn)this.applyStatus(target,'burn',p.burnDuration,{dps:p.burn,sourceId:p.ownerId});
          if(p.poison)this.applyStatus(target,'poison',p.poisonDuration,{dps:p.poison,sourceId:p.ownerId});
          if(p.basic&&source?.heroId&&this.bonus(source,'lifesteal'))this.heal(source,actual*this.bonus(source,'lifesteal'),source,true);
          p.pierce--;if(p.pierce<0){p.life=0;break;}
        }
        this.effect('ring',target.x,target.y,Math.min(35,target.r+8),p.color,.2);
      }
      if(p.x<0||p.y<0||p.x>WORLD.width||p.y>WORLD.height)p.life=0;
    }
    this.projectiles=this.projectiles.filter(p=>p.life>0);
  }
  tickZones(dt){
    for(const z of this.zones){
      z.life-=dt;z.tickIn-=dt;if(z.tickIn>0)continue;
      const source=this.units.find(u=>u.id===z.ownerId);if(!source){z.life=0;continue;}
      const units=this.nearby(z.x,z.y,z.r+55).filter(u=>u.alive&&dist(u,z)<z.r+u.r);
      if(z.type==='bomb'||z.type==='wild-slam'){
        for(const t of units)if(t.team!==z.team){this.damage(t,z.damage*(structure(t)?.35:1),source,z.type==='bomb'?'magic':'physical');if(z.stun)this.applyStatus(t,'stun',z.stun);else this.applyStatus(t,'slow',1.5,{factor:.35});}
        this.effect('explosion',z.x,z.y,z.r,z.color,.6);z.life=0;
      }else if(z.type==='storm'){
        const damage=z.damage*(z.detonated?1:1.65);z.detonated=true;for(const t of units)if(t.team!==z.team)this.damage(t,damage*(structure(t)?.35:1),source,'magic');
        this.effect('explosion',z.x,z.y,z.r,z.color,.4);z.tickIn=.7;
      }else if(z.type==='field'){
        for(const t of units)if(t.team===z.team){if(!z.applied.has(t.id)){this.shield(t,z.shield,5);z.applied.add(t.id);}if(z.healing)this.heal(t,z.healing*.5,source);}
        else this.applyStatus(t,'slow',.6,{factor:.25});z.tickIn=.5;
      }else if(z.type==='tree'||z.type==='heal'){
        for(const t of units)if(t.team===z.team)this.heal(t,z.healing,source);
        else {if(z.damage)this.damage(t,z.damage*(structure(t)?.35:1),source,'magic');if(z.slow)this.applyStatus(t,'slow',1.1,{factor:z.slow});}
        z.tickIn=1;
      }
    }
    this.zones=this.zones.filter(z=>z.life>0);
  }
  rebuildGrid(){this.grid.clear();for(const u of this.units)if(u.alive){const key=(Math.floor(u.x/160))+','+(Math.floor(u.y/160));let list=this.grid.get(key);if(!list)this.grid.set(key,list=[]);list.push(u);}}
  nearby(x,y,r){const result=[];for(let gx=Math.floor((x-r)/160);gx<=Math.floor((x+r)/160);gx++)for(let gy=Math.floor((y-r)/160);gy<=Math.floor((y+r)/160);gy++){const list=this.grid.get(gx+','+gy);if(list)result.push(...list);}return result;}
  speed(u){return u.speed*(1-(u.status.slow?.factor||0))*(1+(u.status.haste?.factor||0));}
  safePoint(p,r=17){
    const out={x:clamp(p.x,r+15,WORLD.width-r-15),y:clamp(p.y,r+15,WORLD.height-r-15)};
    for(let pass=0;pass<3;pass++)for(const o of WORLD.obstacles){const dx=out.x-o.x,dy=out.y-o.y,d=Math.hypot(dx,dy),min=r+o.r;if(d<min){const a=d?Math.atan2(dy,dx):0;out.x=o.x+Math.cos(a)*(min+1);out.y=o.y+Math.sin(a)*(min+1);}}
    out.x=clamp(out.x,r+15,WORLD.width-r-15);out.y=clamp(out.y,r+15,WORLD.height-r-15);return out;
  }
  clearLine(a,b,r=17){return !WORLD.obstacles.some(o=>Math.abs(o.x-(a.x+b.x)/2)<Math.abs(a.x-b.x)/2+o.r+r&&Math.abs(o.y-(a.y+b.y)/2)<Math.abs(a.y-b.y)/2+o.r+r&&segmentDistance(o.x,o.y,a,b)<o.r+r+3);}
  moveDirect(u,dx,dy,dt){
    const length=Math.hypot(dx,dy);if(length<.01)return;dx/=length;dy/=length;const speed=this.speed(u),oldX=u.x,oldY=u.y;
    const p=this.safePoint({x:u.x+dx*speed*dt,y:u.y+dy*speed*dt},u.r);
    if(u.bot){
      const target=u.brain?.action!=='retreat'&&this.units.find(v=>v.id===u.brain?.targetId&&v.alive);
      if(this.towerThreatOnPath(u,p,target||null,false)){u.aiState='waiting for wave';return;}
    }
    u.x=p.x;u.y=p.y;
    u.vx=(u.x-oldX)/dt;u.vy=(u.y-oldY)/dt;u.moving=Math.hypot(u.vx,u.vy)>3;if(u.moving){u.angle=Math.atan2(u.vy,u.vx);u.recallIn=0;}
    if(u.moving&&u.splashIn<=0&&WORLD.river.slice(1).some((p,i)=>segmentDistance(u.x,u.y,WORLD.river[i],p)<WORLD.riverWidth/2)){u.splashIn=.22;this.effect('splash',u.x,u.y,15,'#a1e6ef',.38);}
  }
  moveToward(u,goal,dt){
    if(!goal||!Number.isFinite(goal.x)||!Number.isFinite(goal.y))return;
    let target=goal;
    if(!this.clearLine(u,goal,u.r)){
      if(!u.navPath||!u.navGoal||dist(u.navGoal,goal)>95||u.navTime<this.time){u.navPath=this.findPath(u,goal,u.r);u.navGoal={x:goal.x,y:goal.y};u.navTime=this.time+2.5;}
      while(u.navPath?.length&&dist(u,u.navPath[0])<24)u.navPath.shift();if(u.navPath?.length)target=u.navPath[0];
    }else{u.navPath=null;u.navGoal=null;}
    if(u.bot){
      const combatTarget=u.brain?.action!=='retreat'&&this.units.find(v=>v.id===u.brain?.targetId&&v.alive);
      const danger=this.towerThreatOnPath(u,target,combatTarget||null,false);
      if(danger){
        // A terrain detour can cross a tower even when the requested straight route is safe.
        u.navPath=null;u.navGoal=null;u.navTime=0;u.aiState='waiting for wave';
        if(dist(u,danger)<danger.range+u.r+70)this.moveDirect(u,u.x-danger.x|| (u.team?1:-1),u.y-danger.y,dt);
        return;
      }
    }
    if(dist(u,target)<5)return;
    let dx=target.x-u.x,dy=target.y-u.y;
    // Small local repulsion preserves the readable five-unit formation.
    let sx=0,sy=0;for(const other of this.nearby(u.x,u.y,50)){if(other===u||structure(other)||!other.alive)continue;const d=dist(u,other),min=u.r+other.r+3;if(d>0&&d<min){sx+=(u.x-other.x)/d*(min-d)*2;sy+=(u.y-other.y)/d*(min-d)*2;}}
    const len=Math.hypot(dx,dy)||1;dx=dx/len*100+sx;dy=dy/len*100+sy;this.moveDirect(u,dx,dy,dt);
  }
  findPath(from,to,r=17){
    const size=64,cols=Math.ceil(WORLD.width/size),rows=Math.ceil(WORLD.height/size),key=(x,y)=>y*cols+x;
    const start={x:clamp(Math.floor(from.x/size),0,cols-1),y:clamp(Math.floor(from.y/size),0,rows-1)},goal={x:clamp(Math.floor(to.x/size),0,cols-1),y:clamp(Math.floor(to.y/size),0,rows-1)};
    let blockedCells=this.navCache.get(r);
    if(!blockedCells){blockedCells=new Uint8Array(cols*rows);for(let y=0;y<rows;y++)for(let x=0;x<cols;x++)blockedCells[key(x,y)]=WORLD.obstacles.some(o=>Math.hypot(o.x-(x+.5)*size,o.y-(y+.5)*size)<o.r+r+8)?1:0;this.navCache.set(r,blockedCells);}
    const blocked=(x,y)=>x<0||y<0||x>=cols||y>=rows||blockedCells[key(x,y)]===1;
    if(blocked(goal.x,goal.y)){
      let found=false;for(let radius=1;radius<5&&!found;radius++)for(let y=-radius;y<=radius&&!found;y++)for(let x=-radius;x<=radius;x++)if(!blocked(goal.x+x,goal.y+y)){goal.x+=x;goal.y+=y;found=true;break;}
    }
    const startKey=key(start.x,start.y),goalKey=key(goal.x,goal.y),open=[{...start,g:0,f:0}],cost=new Map([[startKey,0]]),came=new Map(),closed=new Set();
    let endKey=startKey,steps=0;
    while(open.length&&steps++<2000){
      let best=0;for(let i=1;i<open.length;i++)if(open[i].f<open[best].f)best=i;
      const current=open.splice(best,1)[0],k=key(current.x,current.y);if(closed.has(k))continue;closed.add(k);endKey=k;if(k===goalKey)break;
      for(const [dx,dy] of [[1,0],[-1,0],[0,1],[0,-1],[1,1],[1,-1],[-1,1],[-1,-1]]){
        const x=current.x+dx,y=current.y+dy;if(blocked(x,y)||(dx&&dy&&(blocked(current.x+dx,current.y)||blocked(current.x,current.y+dy))))continue;
        const nk=key(x,y),g=current.g+(dx&&dy?1.414:1);if(g>=(cost.get(nk)??Infinity))continue;
        cost.set(nk,g);came.set(nk,k);open.push({x,y,g,f:g+Math.hypot(x-goal.x,y-goal.y)});
      }
    }
    if(endKey!==goalKey)return [this.safePoint(to,r)];
    const path=[];while(endKey!==startKey){const x=endKey%cols,y=Math.floor(endKey/cols);path.push({x:(x+.5)*size,y:(y+.5)*size});endKey=came.get(endKey);if(endKey===undefined)break;}
    path.reverse();path.push(this.safePoint(to,r));
    // A* supplies routing; prune the staircase into collision-safe straight segments.
    const smooth=[];let current=from,i=0;while(i<path.length){let far=i;for(let j=i;j<path.length;j++)if(this.clearLine(current,path[j],r))far=j;else break;smooth.push(path[far]);current=path[far];i=far+1;}
    return smooth;
  }
  planTeams(){
    for(let team=0;team<2;team++){
      const heroes=this.units.filter(u=>u.kind==='hero'&&u.team===team&&u.alive),enemyHeroes=this.units.filter(u=>u.kind==='hero'&&u.team===1-team&&u.alive);
      const structures=this.units.filter(u=>u.alive&&structure(u)&&u.team===team);
      let danger=null,best=0;
      for(const s of structures){const foes=this.nearby(s.x,s.y,430).filter(u=>u.alive&&u.team===1-team&&!structure(u)&&dist(u,s)<430);const score=foes.reduce((n,u)=>n+(u.kind==='hero'?3:u.siege?4:.5),0)*(s.kind==='core'?2:1)+(foes.length?1-s.hp/s.maxHp:0);if(score>best){best=score;danger=s;}}
      const cfg=DIFFICULTIES[this.difficulty],old=this.teamPlans[team];
      const core=structures.find(s=>s.kind==='core');
      const coreThreat=core&&this.nearby(core.x,core.y,400).some(u=>u.alive&&u.team===1-team&&!structure(u)&&dist(core,u)<370);
      if(coreThreat){this.teamPlans[team]={type:'defend',lane:1,x:core.x,y:core.y,until:this.time+5};continue;}
      if(team===0&&this.order?.until>this.time){this.teamPlans[0]={...this.order};continue;}
      if(danger&&best>=3){this.teamPlans[team]={type:'defend',lane:danger.lane??1,x:danger.x,y:danger.y,until:this.time+5};continue;}
      const beast=this.units.find(u=>u.alive&&u.kind==='beast'&&u.team===-1);
      if(beast&&heroes.filter(h=>h.hp/h.maxHp>.55).length>=2&&this.time>60&&cfg.coordination>.5&&(!old||old.type==='beast'||this.time%75<40)){
        this.teamPlans[team]={type:'beast',lane:beast.lane,beastId:beast.id,x:beast.x,y:beast.y,until:this.time+8};continue;
      }
      if(this.time>65){
        const scores=[0,1,2].map(lane=>{
          const tower=this.nextStructure(team,lane),allies=this.units.filter(u=>u.alive&&u.team===team&&u.lane===lane&&!structure(u)&&u.kind!=='hero'),foes=enemyHeroes.filter(h=>h.lane===lane);
          return {lane,score:(tower?.kind==='core'?8:tower?.tier?4:0)+allies.length*.3-(foes.length*1.3)+(tower?1-tower.hp/tower.maxHp:0)*2};
        }).sort((a,b)=>b.score-a.score);
        const lane=old?.type==='push'&&old.until>this.time?old.lane:scores[0].lane;
        const forward=this.nextStructure(team,lane),focus=enemyHeroes.filter(h=>dist(h,forward||WORLD.bases[1-team])<550).sort((a,b)=>a.hp/a.maxHp-b.hp/b.maxHp)[0];
        this.teamPlans[team]={type:'push',lane,focusId:focus?.id,until:old?.type==='push'&&old.until>this.time?old.until:this.time+18};
      }else this.teamPlans[team]={type:'lanes',lane:1,until:65};
    }
  }
  nextStructure(team,lane){return this.units.find(u=>u.alive&&u.kind==='tower'&&u.team!==team&&u.lane===lane&&u.tier===0)||this.units.find(u=>u.alive&&u.kind==='tower'&&u.team!==team&&u.lane===lane&&u.tier===1)||this.units.find(u=>u.alive&&u.kind==='core'&&u.team!==team);}
  botConfig(hero){return hero.team===0&&this.difficulty==='easy'?DIFFICULTIES.normal:DIFFICULTIES[this.difficulty];}
  canDiveTower(h,tower,target){
    if(!target?.alive||target.team===h.team||target.team===-1||this.isProtected(target))return false;
    const cfg=this.botConfig(h),health=h.hp/h.maxHp;
    const local=this.units.filter(u=>u.alive&&!structure(u)&&dist(u,tower)<tower.range+140);
    const defenders=local.filter(u=>u.kind==='hero'&&u.team===tower.team);
    const friends=local.filter(u=>u.kind==='hero'&&u.team===h.team);
    const shields=h.shield||0,guard=h.status.guard?.factor||1;
    const towerDamage=tower.damage*1.35*1.75*guard;
    const exitTime=Math.max(0,tower.range+60-dist(h,tower))/Math.max(1,this.speed(h));
    const defenderDamage=defenders.reduce((sum,u)=>sum+u.damage/Math.max(.3,u.attackRate),0);
    const exposure=towerDamage*(2+Math.ceil(exitTime/tower.attackRate))+defenderDamage*1.5;
    const survives=h.hp+shields-exposure>h.maxHp*.35;
    if(structure(target)&&target.hp<=h.damage*2.2&&health>.55&&survives)return true;
    if(target.kind==='hero'){
      // A wave does not excuse attacking a hero: retaliation redirects the tower onto the attacker.
      const close=dist(h,target)<=h.range+target.r+this.speed(h)*1.1;
      const finishable=target.hp<=h.damage*2.1&&target.hp/target.maxHp<.22;
      const backing=friends.some(u=>u.id!==h.id&&u.hp/u.maxHp>.55)||defenders.length===1;
      return cfg.coordination>.9&&health>.72&&close&&finishable&&backing&&friends.length>=defenders.length&&survives;
    }
    const aggro=tower.aggroId===h.id&&tower.aggroUntil>this.time||tower.targetId===h.id;
    if(aggro||health<Math.max(.48,cfg.retreat+.12)||defenders.length>friends.length+1)return false;
    // Escort real, healthy frontliners already inside range; nearby units outside range cannot tank.
    const frontline=local.filter(u=>u.team===h.team&&u.id!==h.id&&u.kind!=='hero'&&dist(u,tower)<=tower.range+u.r&&u.hp>tower.damage*1.25);
    const tank=local.find(u=>u.id===tower.targetId&&u.team===h.team&&u.id!==h.id&&u.hp+(u.shield||0)>tower.damage*3);
    return frontline.reduce((sum,u)=>sum+u.hp,0)>tower.damage*3||!!tank;
  }
  towerThreatOnPath(h,goal,target=null,stopAtReach=true){
    if(!goal||!Number.isFinite(goal.x)||!Number.isFinite(goal.y))return null;
    const d=dist(h,goal),reach=stopAtReach&&target?h.range+target.r+h.r*.3:0;
    const travel=Math.max(0,d-reach),end={x:h.x+(goal.x-h.x)/(d||1)*travel,y:h.y+(goal.y-h.y)/(d||1)*travel};
    const threats=this.units.filter(u=>u.alive&&structure(u)&&u.team===1-h.team&&segmentDistance(u.x,u.y,h,end)<u.range+h.r+30);
    threats.sort((a,b)=>dist(h,a)-dist(h,b));
    return threats.find(tower=>{
      // Moving out of danger is always allowed; avoid blocking a retreat with its own safety check.
      const now=dist(h,tower),after=dist(end,tower);
      if(now<tower.range+h.r+30&&after>now+.01&&(end.x-h.x)*(h.x-tower.x)+(end.y-h.y)*(h.y-tower.y)>0)return false;
      return !this.canDiveTower(h,tower,target);
    })||null;
  }
  waitOutsideTower(h,tower,dt){
    const dx=h.x-tower.x,dy=h.y-tower.y,d=Math.hypot(dx,dy)||1;
    const away=d>1?{x:dx/d,y:dy/d}:{x:h.team?1:-1,y:0};
    const goal={x:tower.x+away.x*(tower.range+h.r+85),y:tower.y+away.y*(tower.range+h.r+85)};
    h.targetId=null;h.aiState=dist(h,tower)<tower.range+h.r?'leaving tower fire':'waiting for wave';
    if(dist(h,goal)>10)this.moveToward(h,goal,dt);
  }
  tickBot(h,dt){
    h.shopIn-=dt;if(h.shopIn<=0){h.shopIn=2;this.botShop(h);}
    const cfg=this.botConfig(h);h.thinkIn-=dt;
    if(h.thinkIn<=0){h.thinkIn=cfg.think;this.thinkBot(h,cfg);}
    const brain=h.brain||{};
    if(brain.action==='retreat'){
      const well=WORLD.bases[h.team].well;
      if(dist(h,well)>95){
        const foes=this.nearby(h.x,h.y,420).filter(u=>u.team===1-h.team&&u.alive);
        if(!foes.length&&this.time-h.lastDamageAt>3.5&&dist(h,well)>450){this.recall(h);return;}
        this.moveToward(h,well,dt);
      }
      this.botCast(h,cfg,null,true);return;
    }
    const target=this.units.find(u=>u.id===brain.targetId&&u.alive);
    if(target){
      const dangerous=this.towerThreatOnPath(h,target,target);
      if(dangerous){this.waitOutsideTower(h,dangerous,dt);this.botCast(h,cfg,null,true);return;}
      h.targetId=target.id;
      this.botCast(h,cfg,target,false);
      if(!target.alive)return;
      const d=dist(h,target),reach=h.range+target.r;
      if(d<=reach+h.r*.35){
        this.basicAttack(h,target);
        if(h.range>150&&d<115&&target.kind==='hero'&&cfg.coordination>.65)this.moveToward(h,{x:h.x+(h.x-target.x)*1.1,y:h.y+(h.y-target.y)*1.1},dt);
      }else this.moveToward(h,target,dt);
    }else if(brain.goal){
      h.targetId=null;const dangerous=this.towerThreatOnPath(h,brain.goal);
      if(dangerous){this.waitOutsideTower(h,dangerous,dt);return;}
      this.moveToward(h,brain.goal,dt);
    }
    if(cfg.dodge&&h.aiState!=='retreating'){
      const danger=this.zones.find(z=>z.team!==h.team&&(z.type==='storm'||z.type==='bomb'||z.type==='wild-slam')&&z.tickIn>0.13&&dist(h,z)<z.r+15);
      if(danger){const dx=h.x-danger.x||1,dy=h.y-danger.y;this.moveDirect(h,dx,dy,dt*.7);h.aiState='dodging';}
    }
  }
  botShop(hero){
    if(!hero?.bot||!hero.alive)return false;
    const preferred=hero.heroId==='bulwark'?['hp','shield']:hero.heroId==='druid'?['summonPower','healing']:hero.heroId==='angel'||hero.heroId==='seraph'?['healing','shield']:hero.heroId==='ranger'||hero.heroId==='ember'?['damage','attackSpeed']:['spellDamage','cooldown'];
    const score=item=>item.tier*2+preferred.reduce((n,key)=>n+(item.bonuses[key]||0)*12,0)+(item.bonuses.hp||0)*4;
    const options=ITEMS.filter(item=>purchaseState(hero,item.id).ok).sort((a,b)=>score(b)-score(a)||a.cost-b.cost);
    return options.length?this.buyItem(hero,options[0].id):false;
  }
  thinkBot(h,cfg){
    for(const tier of this.availableTalents(h))this.chooseTalent(h,tier.level,tier.choices[(h.slot+Math.floor(tier.level/3))%tier.choices.length].id);
    const health=h.hp/h.maxHp;
    if(health<cfg.retreat||(h.brain?.action==='retreat'&&(health<.88||h.mana/h.maxMana<.65))){h.brain={action:'retreat'};h.aiState='retreating';return;}
    if(h.mana<15&&health<.7){h.brain={action:'retreat'};h.aiState='recovering';return;}
    const plan=this.teamPlans[h.team],foes=this.nearby(h.x,h.y,380).filter(u=>u.alive&&u.team===1-h.team&&!structure(u)&&dist(u,h)<380),allies=this.nearby(h.x,h.y,430).filter(u=>u.alive&&u.team===h.team&&dist(u,h)<430);
    const enemyPower=foes.reduce((n,u)=>n+(u.kind==='hero'?2.5:u.siege?3:.3)*u.hp/u.maxHp,0),allyPower=allies.reduce((n,u)=>n+(u.kind==='hero'?2.5:u.siege?3:.3)*u.hp/u.maxHp,0);
    if(cfg.coordination>.6&&enemyPower>allyPower*1.65&&health<.72){h.brain={action:'retreat'};h.aiState='outnumbered';return;}
    if(plan.type==='defend'){
      h.lane=plan.lane;const pos={x:plan.x??lanePoint(plan.lane,h.team?.75:.25).x,y:plan.y??lanePoint(plan.lane,h.team?.75:.25).y};
      const defense=this.nearby(pos.x,pos.y,500).filter(u=>u.alive&&u.team===1-h.team&&!structure(u)&&dist(u,pos)<500).sort((a,b)=>dist(h,a)-dist(h,b))[0];
      h.brain={action:'defend',targetId:defense?.id,goal:pos};h.aiState='defending '+WORLD.lanes[h.lane].name.toLowerCase();return;
    }
    const threat=foes.filter(u=>u.kind==='hero').sort((a,b)=>this.targetScore(h,a)-this.targetScore(h,b))[0];
    if(threat&&dist(h,threat)<285&&(!plan||plan.type!=='beast'||dist(h,threat)<190)){h.brain={action:'fight',targetId:threat.id};h.aiState='fighting';return;}
    if(plan.type==='beast'){
      const beast=this.units.find(u=>u.id===plan.beastId&&u.alive)||this.units.find(u=>u.alive&&u.team===-1&&u.kind==='beast');
      if(beast){h.brain={action:'beast',targetId:beast.id};h.aiState='contesting beast';h.lane=beast.lane;return;}
    }
    const shouldJungle=h.role==='jungler'&&(this.time<90||plan.type==='lanes'||(h.level<5&&this.time%80<35));
    if(shouldJungle){
      const camps=this.units.filter(u=>u.alive&&u.kind==='camp'&&(h.team===0?u.x<1600:u.x>1600)).sort((a,b)=>dist(h,a)-dist(h,b));
      if(camps[0]){h.brain={action:'jungle',targetId:camps[0].id};h.aiState='clearing jungle';return;}
    }
    if(plan.type==='push'&&cfg.coordination>.5)h.lane=plan.lane;
    const laneUnits=this.units.filter(u=>u.alive&&u.team===1-h.team&&u.lane===h.lane&&!structure(u)&&u.team>=0);
    laneUnits.sort((a,b)=>dist(h,a)-dist(h,b));
    const target=laneUnits.find(u=>dist(h,u)<500)||this.nextStructure(h.team,h.lane);
    const friendlyWave=this.units.filter(u=>u.alive&&u.team===h.team&&u.lane===h.lane&&(u.kind==='footman'||u.kind==='archer'||u.siege));
    const forward=friendlyWave.sort((a,b)=>h.team?a.x-b.x:b.x-a.x)[0];
    if(target&&structure(target)){
      const tank=forward&&dist(forward,target)<target.range+80;
      const close=dist(h,target)<target.range+150;
      if(!tank&&close){
        const away=Math.atan2(h.y-target.y,h.x-target.x);h.brain={action:'wait',goal:{x:target.x+Math.cos(away)*(target.range+100),y:target.y+Math.sin(away)*(target.range+100)}};h.aiState='escorting wave';return;
      }
    }
    // Travel via the lane until near battle; do not cut through every forest on the way out.
    let goal=target||WORLD.bases[1-h.team];
    if(dist(h,goal)>520){
      const path=this.lanePath(h.lane,h.team,h);goal=path[0]||goal;
      h.brain={action:'push',goal};
    }else h.brain={action:'push',targetId:target?.id,goal};
    h.aiState='pushing '+WORLD.lanes[h.lane].name.toLowerCase();
  }
  botCast(h,cfg,target,retreat){
    // Thinking cadence, spell range checks and mana reserves differ by difficulty; stats do not.
    if(h.nextCastAt>this.time)return;h.nextCastAt=this.time+cfg.think*.8;
    const friends=this.nearby(h.x,h.y,330).filter(u=>u.alive&&u.team===h.team&&!structure(u)&&dist(h,u)<330);
    const hurt=friends.sort((a,b)=>a.hp/a.maxHp-b.hp/b.maxHp)[0],foes=this.nearby(h.x,h.y,320).filter(u=>u.alive&&u.team!==h.team&&!structure(u)&&dist(h,u)<320);
    for(let slot=0;slot<4;slot++){
      const a=this.getAbility(h,slot);if(h.level<a.unlock||h.cooldowns[slot]>0||h.mana<a.mana)continue;
      let aim=target?{x:target.x+(target.vx||0)*.35*cfg.lead,y:target.y+(target.vy||0)*.35*cfg.lead}:{x:h.x,y:h.y};
      let should=false;
      if(['bulwark','ember','seraph'].includes(h.heroId)){
        const customAim=botSkillAim(this,h,a,target,retreat,cfg);should=!!customAim;if(customAim)aim={...customAim};
      }else switch(a.id){
        case 'healing-tree':case 'rose-light':should=!!hurt&&hurt.hp/hurt.maxHp<.82||a.id==='rose-light'&&foes.length>=2;aim=hurt&&hurt.hp/hurt.maxHp<.85?{x:hurt.x,y:hurt.y}:aim;break;
        case 'forcefield':case 'sanctuary':should=foes.length>0&&friends.some(u=>u.kind==='hero'&&u.hp/u.maxHp<.78);aim=hurt?{x:hurt.x,y:hurt.y}:aim;break;
        case 'inspire':should=!!target&&friends.length>=2&&dist(h,target)<320;aim=hurt?{x:hurt.x,y:hurt.y}:aim;break;
        case 'dragon-shield':should=foes.length>0&&h.hp/h.maxHp<.8;break;
        case 'flame-ring':should=foes.some(u=>dist(h,u)<a.radius+u.r);break;
        case 'spirit-warriors':case 'treants':case 'bear':case 'wolves':should=!!target&&dist(h,target)<360&&!retreat;break;
        case 'rift-step':case 'sky-cleave':should=!!target&&!retreat&&dist(h,target)>115&&dist(h,target)<a.range+70&&h.hp/h.maxHp>.5;break;
        default:should=!!target&&dist(h,target)<a.range+target.r&&!retreat;
      }
      if(!should)continue;
      if(a.type==='line'||a.type==='cone'||a.type==='ground'&&!['healing-tree','rose-light','forcefield','sanctuary','inspire'].includes(a.id)){
        aim.x+=(this.random()-.5)*cfg.aimError;aim.y+=(this.random()-.5)*cfg.aimError;
      }
      if(a.mobility||a.id==='rift-step'||a.id==='sky-cleave'){
        const d=dist(h,aim),end={x:h.x+(aim.x-h.x)/(d||1)*Math.min(d,a.range),y:h.y+(aim.y-h.y)/(d||1)*Math.min(d,a.range)};
        if(this.towerThreatOnPath(h,end,target,false))continue;
      }
      if(this.cast(h,slot,aim))break;
    }
  }
}
