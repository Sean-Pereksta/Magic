// Monkey Trails: deterministic, DOM-free game rules. Distances are meters internally.
export const VERSION = 1, FEET = 0.3048, RESOURCE_STEP = 250 * FEET, ENCOUNTER_STEP = 2000 * FEET;
export const ELEMENTS = {
  leaf: {name:'Leaf', icon:'❧', color:'#76d99c', direction:'N', resource:'Canopy seeds', beats:'water', weak:'fire'},
  fire: {name:'Fire', icon:'♨', color:'#ff986d', direction:'S', resource:'Ember stones', beats:'leaf', weak:'water'},
  storm:{name:'Storm',icon:'ϟ',color:'#d8b4ff',direction:'E',resource:'Storm shards',beats:'water',weak:'leaf'},
  water:{name:'Water',icon:'≈',color:'#7acbff',direction:'W',resource:'River pearls',beats:'fire',weak:'storm'}
};
export const DIRECTIONS = {N:'leaf', S:'fire', E:'storm', W:'water'};
export const SPECIES = {
  capuchin:{name:'Capuchin',title:'The Forager',hp:78,atk:11,def:8,speed:13,trait:'One extra fruit at every trail cache.'},
  macaque:{name:'Macaque',title:'The Striker',hp:84,atk:15,def:8,speed:10,trait:'Critical hits are twice as likely.'},
  langur:{name:'Langur',title:'The Herbalist',hp:86,atk:10,def:10,speed:11,trait:'Healing restores 25% more health.'},
  mandrill:{name:'Mandrill',title:'The Warden',hp:102,atk:12,def:13,speed:6,trait:'Brace blocks 70% of incoming damage.'},
  spider:{name:'Spider Monkey',title:'The Acrobat',hp:73,atk:12,def:7,speed:18,trait:'A 10% chance to dodge an attack.'},
  howler:{name:'Howler',title:'The Channeler',hp:88,atk:12,def:9,speed:9,trait:'Elemental attacks deal 15% more damage.'}
};
export const MOVES = {
  strike:{name:'Palm strike',type:'neutral',power:18,cost:0,desc:'Reliable physical attack. No energy cost.'},
  brace:{name:'Brace',type:'neutral',cost:0,guard:true,priority:3,desc:'Block most damage this turn; recover 2 energy.'},
  breathe:{name:'Deep breath',type:'neutral',cost:0,energy:7,desc:'Recover 7 energy instead of attacking.'},
  cinder:{name:'Cinder fist',type:'fire',power:26,cost:3,status:'burn',desc:'Fire damage; burn for two turns.'},
  flare:{name:'Solar roar',type:'fire',power:43,cost:6,minStage:2,desc:'Heavy fire damage.'},
  vine:{name:'Thorn lash',type:'leaf',power:22,cost:3,status:'poison',desc:'Leaf damage; poison for three turns.'},
  mend:{name:'Canopy mend',type:'leaf',cost:5,heal:0.30,minStage:2,desc:'Restore 30% of maximum health.'},
  tide:{name:'Tidal palm',type:'water',power:27,cost:3,status:'slow',desc:'Water damage; slow for two turns.'},
  spring:{name:'Cleansing spring',type:'water',cost:5,heal:0.22,cleanse:true,minStage:2,desc:'Heal 22% and clear all status effects.'},
  spark:{name:'Lightning jab',type:'storm',power:24,cost:3,priority:1,desc:'A fast storm strike with turn priority.'},
  thunder:{name:'Thunder howl',type:'storm',power:41,cost:6,minStage:2,desc:'Heavy storm damage.'},
  wildfire:{name:'Wildfire bloom',types:['fire','leaf'],type:'fire',power:38,cost:6,status:'burn',desc:'Fire/leaf hybrid attack. Uses the better matchup.'},
  steam:{name:'Steam cyclone',types:['fire','water'],type:'water',power:38,cost:6,status:'slow',desc:'Fire/water hybrid attack. Uses the better matchup.'},
  plasma:{name:'Plasma pounce',types:['fire','storm'],type:'storm',power:38,cost:6,priority:1,desc:'Fire/storm hybrid attack with turn priority.'},
  monsoon:{name:'Monsoon canopy',types:['leaf','water'],type:'leaf',power:32,cost:6,drain:0.35,desc:'Leaf/water hybrid attack. Restore 35% of damage dealt.'},
  tempest:{name:'Tempest vines',types:['leaf','storm'],type:'leaf',power:36,cost:6,status:'poison',desc:'Leaf/storm hybrid attack with poison.'},
  typhoon:{name:'Typhoon crash',types:['water','storm'],type:'water',power:39,cost:6,status:'slow',desc:'Water/storm hybrid attack with slow.'}
};
export const HYBRIDS = {
  'fire+leaf':{name:'Wildfire',move:'wildfire'},'fire+water':{name:'Steam',move:'steam'},
  'fire+storm':{name:'Plasma',move:'plasma'},'leaf+water':{name:'Monsoon',move:'monsoon'},
  'leaf+storm':{name:'Tempest',move:'tempest'},'storm+water':{name:'Typhoon',move:'typhoon'}
};
export const GEAR = {
  fang:{name:'Sunfang wraps',slot:'weapon',stat:'atk',amount:2,icon:'✦'},
  shell:{name:'River-shell vest',slot:'armor',stat:'def',amount:2,icon:'◈'},
  heart:{name:'Canopy heart',slot:'charm',stat:'hp',amount:9,icon:'♥'},
  wind:{name:'Gale anklet',slot:'charm',stat:'speed',amount:2,icon:'ϟ'}
};
export const ITEMS = {berry:{name:'Healing berry',desc:'Restore 35% health.'},tonic:{name:'Spark tonic',desc:'Restore 8 energy.'},salve:{name:'Cleanse balm',desc:'Clear effects and restore 15% health.'}};
export const clamp=(n,a,b)=>Math.max(a,Math.min(b,n));
export function random(s) { let x=s.rng|0; x^=x<<13; x^=x>>>17; x^=x<<5; s.rng=x>>>0; return s.rng/4294967296; }
const choose=(s,a)=>a[Math.floor(random(s)*a.length)];
export function hybrid(types){return HYBRIDS[[...types].sort().join('+')]||null;}
export function formName(p){return `${p.stage===3?'Ascendant ':p.stage===2?'Elder ':''}${hybrid(p.types)?.name||ELEMENTS[p.types[0]]?.name||'Trail'} ${SPECIES[p.species].name}`;}
export function effectiveness(type,defenders){
  if(type==='neutral')return 1;
  // Multiplication keeps dual weaknesses, resistances, and cancellations meaningful.
  const chart={fire:{leaf:2,water:.5,fire:.5},leaf:{water:2,fire:.5,leaf:.5,storm:2},water:{fire:2,leaf:.5,water:.5},storm:{water:2,leaf:.5,storm:.5}};
  return defenders.reduce((n,t)=>n*(chart[type]?.[t]??1),1);
}
export function moveEffect(move,defenders){return Math.max(...(move.types||[move.type]).map(t=>effectiveness(t,defenders)));}
export function stats(p,gear={}){
  const b=SPECIES[p.species], lv=p.level-1, stage=p.stage||0;
  const out={hp:b.hp+lv*8+stage*16,atk:b.atk+lv*2+stage*3,def:b.def+lv*1.4+stage*2,speed:b.speed+lv*.65+stage,maxEnergy:12+stage*2};
  for(const [id,tier] of Object.entries(gear))if(GEAR[id])out[GEAR[id].stat]+=GEAR[id].amount*tier;
  if(p.elite)for(const key of ['hp','atk','def'])out[key]*=1.18;
  for(const k in out)out[k]=Math.round(out[k]); return out;
}
export function equipped(s){const g={}; for(const id of Object.values(s.meta.equipped))if(id&&s.meta.gear[id])g[id]=s.meta.gear[id];return g;}
export const playerStats=s=>stats(s.run.monkey,equipped(s));
export function unlocked(p){return Object.entries(MOVES).filter(([,m])=>!m.types?(m.type==='neutral'||p.types.includes(m.type))&&p.stage>=(m.minStage||0):m.types.every(t=>p.types.includes(t))).map(([id])=>id);}
export function say(s,text){s.log.push(text);s.log=s.log.slice(-16);}
function monkey(species,level=1,types=[],stage=0){const p={species,level,types,stage,hp:1,energy:12,status:{},loadout:['strike','brace','breathe'],training:0};p.hp=stats(p).hp;return p;}
export function newSave(name,species='capuchin',mode='gps',seed=Date.now()>>>0){
  if(!SPECIES[species])throw Error('Choose a valid starter.');
  const s={version:VERSION,name,mode,rng:(seed>>>0)||1234567,meta:{runs:0,legacy:0,best:0,gear:{},equipped:{weapon:null,armor:null,charm:null},history:[]},run:null,log:[]};
  restart(s,species);return s;
}
export function restart(s,species=s.run?.monkey.species||'capuchin'){
  if(!SPECIES[species])throw Error('Unknown starter.');
  if(s.run&&s.run.phase!=='dead')throw Error('Your current run is still alive.');
  const bonus=Math.min(20,s.meta.legacy);
  s.meta.runs++;
  const id=`${Date.now().toString(36)}-${Math.floor(random(s)*0xffffffff).toString(36)}-${s.meta.runs}`;
  s.run={id,phase:'trail',distance:0,nextCache:RESOURCE_STEP,nextEncounter:ENCOUNTER_STEP,wins:0,encounters:0,
    monkey:monkey(species),fruit:10+bonus*2,essence:{fire:0,leaf:0,water:0,storm:0},items:{berry:2+Math.floor(bonus/5),tonic:1,salve:1},
    vector:{east:0,north:0},weights:{N:0,S:0,E:0,W:0},direction:'N',battle:null,startedAt:Date.now()};
  s.run.monkey.hp=playerStats(s).hp;s.log=[];say(s,`Run ${s.meta.runs}: a new trail, with ${s.run.fruit} fruit. Equipment survives defeat.`);
}
export function direction(east,north,fallback='N'){if(Math.hypot(east,north)<2)return fallback;return Math.abs(north)>=Math.abs(east)?north>=0?'N':'S':east>=0?'E':'W';}
export function odds(s){const w=s.run.weights,total=Object.values(w).reduce((a,b)=>a+b,0);return Object.fromEntries(Object.keys(w).map(d=>[DIRECTIONS[d],total ? .1+.6*w[d]/total : .25]));}
function rollElement(s){let n=random(s);for(const [type,p]of Object.entries(odds(s))){n-=p;if(n<=0)return type;}return 'leaf';}
function findGear(s,guaranteed=false){
  if(!guaranteed&&random(s)>.32)return;
  const id=choose(s,Object.keys(GEAR)),tier=clamp(1+Math.floor(s.run.wins/5),1,12);
  if((s.meta.gear[id]||0)>=tier){s.run.fruit+=8;say(s,'Duplicate relic salvaged into 8 fruit.');return;}
  s.meta.gear[id]=tier;const slot=GEAR[id].slot;
  if(!s.meta.equipped[slot])s.meta.equipped[slot]=id;
  say(s,`Permanent equipment: ${GEAR[id].name} +${tier}.`);
}
function cache(s){
  const r=s.run,type=DIRECTIONS[r.direction],fruit=3+Math.floor(random(s)*3)+(r.monkey.species==='capuchin'?1:0);
  const essence=2+Math.floor(random(s)*2);r.fruit+=fruit;r.essence[type]+=essence;
  let item='';if(random(s)<.32){item=choose(s,Object.keys(ITEMS));r.items[item]=Math.min(99,r.items[item]+1);}
  say(s,`${Math.round(r.nextCache/FEET)} ft cache: +${fruit} fruit, +${essence} ${ELEMENTS[type].resource.toLowerCase()}${item?`, ${ITEMS[item].name}`:''}.`);
}
export function advance(s,meters,east,north){
  if(s.run.phase!=='trail'||![meters,east,north].every(Number.isFinite)||meters<=0)return 0;
  // Only count up to the encounter. Never bank movement through a paused battle.
  const r=s.run,used=Math.min(meters,r.nextEncounter-r.distance),ratio=used/meters;
  r.vector.east+=east*ratio;r.vector.north+=north*ratio;r.direction=direction(r.vector.east,r.vector.north,r.direction);
  r.weights[r.direction]+=used;r.distance+=used;
  while(r.distance+1e-7>=r.nextCache){cache(s);r.nextCache+=RESOURCE_STEP;}
  if(r.distance+1e-7>=r.nextEncounter)encounter(s);
  return used;
}
function encounter(s){
  const r=s.run;r.encounters++;const boss=r.encounters%5===0;
  const lv=clamp(1+Math.floor(r.encounters*1.15),1,90),first=rollElement(s),types=[first];
  if(r.encounters>=4&&random(s)<.45){const second=rollElement(s);if(second!==first)types.push(second);}
  const stage=r.encounters>=9?3:r.encounters>=4?2:1;
  const enemy=monkey(choose(s,Object.keys(SPECIES)),lv,types,stage);enemy.elite=boss;enemy.hp=stats(enemy).hp;enemy.energy=stats(enemy).maxEnergy;
  enemy.loadout=['strike',...unlocked(enemy).filter(id=>MOVES[id].power&&id!=='strike').slice(-3)];
  r.battle={enemy,turn:1,ready:false,boss,intent:'strike'};r.phase='battle';planEnemy(s);say(s,`${boss?'Guardian':'Wild rival'}: ${formName(enemy)} (level ${lv}). Stop somewhere safe before battling.`);
}
function planEnemy(s){
  const {enemy}=s.run.battle,ps=s.run.monkey;const options=unlocked(enemy).filter(id=>MOVES[id].cost<=enemy.energy);
  const heal=options.find(id=>MOVES[id].heal);
  if(enemy.hp<stats(enemy).hp*.35&&heal&&random(s)<.8){s.run.battle.intent=heal;return;}
  const ranked=options.filter(id=>MOVES[id].power).sort((a,b)=>MOVES[b].power*moveEffect(MOVES[b],ps.types)-MOVES[a].power*moveEffect(MOVES[a],ps.types));
  s.run.battle.intent=random(s)<.75?ranked[0]||'strike':choose(s,ranked.slice(0,3))||'strike';
}
export function readyBattle(s){if(s.run.phase==='battle')s.run.battle.ready=true;}
export function score(s){const r=s.run;return Math.floor(r.distance/FEET+1e-6)+r.wins*500+r.monkey.stage*1000+(r.monkey.level-1)*100;}
function endRun(s){
  const r=s.run;if(r.phase==='dead')return;r.phase='dead';r.monkey.hp=0;
  const points=score(s),earned=Math.max(1,Math.floor(r.wins/2)+Math.floor(r.distance/(4000*FEET)));
  s.meta.legacy=Math.min(100,s.meta.legacy+earned);s.meta.best=Math.max(s.meta.best,points);
  s.meta.history.unshift({id:r.id,name:s.name,score:points,feet:Math.floor(r.distance/FEET+1e-6),wins:r.wins,level:r.monkey.level,form:formName(r.monkey),mode:s.mode,endedAt:Date.now()});
  s.meta.history=s.meta.history.slice(0,25);say(s,`Run ended: ${points} points. +${earned} legacy. Equipment and starting-supply bonuses survive.`);
}
function victory(s){
  const r=s.run,b=r.battle;r.wins++;const xp=16+b.enemy.level*4;r.monkey.training+=xp;r.fruit+=10+b.enemy.level*2;
  for(const t of b.enemy.types)r.essence[t]+=4+(b.boss?6:0);
  r.items.berry=Math.min(99,r.items.berry+1);findGear(s,b.boss);
  r.monkey.status={};r.monkey.energy=playerStats(s).maxEnergy;
  r.monkey.hp=Math.min(playerStats(s).hp,r.monkey.hp+Math.round(playerStats(s).hp*.12));
  r.battle=null;r.phase='trail';r.nextEncounter+=ENCOUNTER_STEP;r.vector={east:0,north:0};r.weights={N:0,S:0,E:0,W:0};
  say(s,`Victory! +${xp} training, a healing berry, elemental supplies, and 12% health. A new compass origin begins here.`);
}
function heal(p,amount,max){p.hp=Math.min(max,p.hp+Math.round(amount*(p.species==='langur'?1.25:1)));}
function execute(s,actor,target,id,actorStats,targetStats,label){
  const m=MOVES[id];actor.energy-=m.cost;
  if(m.guard){actor.guard=actor.species==='mandrill'?.3:.45;actor.energy=Math.min(actorStats.maxEnergy,actor.energy+2);say(s,`${label} braces.`);return;}
  if(m.energy){actor.energy=Math.min(actorStats.maxEnergy,actor.energy+m.energy);say(s,`${label} recovers energy.`);return;}
  if(m.cleanse)actor.status={};
  if(m.heal){heal(actor,actorStats.hp*m.heal,actorStats.hp);say(s,`${label} uses ${m.name}.`);return;}
  if(target.species==='spider'&&random(s)<.10){say(s,`${m.name} was dodged!`);return;}
  const eff=moveEffect(m,target.types),stab=(m.types||[m.type]).some(t=>actor.types.includes(t))?1.2:1;
  const crit=random(s)<(actor.species==='macaque'?.16:.08),howl=actor.species==='howler'&&m.type!=='neutral'?1.15:1;
  const damage=Math.max(1,Math.round((m.power+actorStats.atk*.85-targetStats.def*.45)*eff*stab*howl*(crit?1.5:1)*(.92+random(s)*.16)*(target.guard||1)));
  target.hp=Math.max(0,target.hp-damage);say(s,`${label}: ${m.name} → ${damage}${crit?' CRITICAL':''}${eff!==1?` (×${eff})`:''}.`);
  if(m.status&&target.hp>0){const immune=m.status==='burn'&&target.types.includes('fire')||m.status==='poison'&&target.types.includes('leaf');if(!immune)target.status[m.status]=m.status==='poison'?3:2;}
  if(m.drain)heal(actor,damage*m.drain,actorStats.hp);
}
function tick(p,max,label,s){
  for(const [status,turns]of Object.entries(p.status)){
    if(status==='burn'||status==='poison'){const n=Math.max(1,Math.round(max*.045));p.hp=Math.max(0,p.hp-n);say(s,`${label} takes ${n} ${status} damage.`);}
    if(turns<=1)delete p.status[status];else p.status[status]--;
  }
}
function applyItem(s,id){
  const r=s.run,p=r.monkey,st=playerStats(s);if(!ITEMS[id]||!(r.items[id]>0))throw Error('That item is unavailable.');
  if(id==='berry'&&p.hp>=st.hp)throw Error('Health is already full.');
  if(id==='tonic'&&p.energy>=st.maxEnergy)throw Error('Energy is already full.');
  if(id==='salve'&&!Object.keys(p.status).length&&p.hp>=st.hp)throw Error('No healing or cleansing is needed.');
  r.items[id]--;if(id==='berry')heal(p,st.hp*.35,st.hp);if(id==='tonic')p.energy=Math.min(st.maxEnergy,p.energy+8);
  if(id==='salve'){p.status={};heal(p,st.hp*.15,st.hp);}say(s,`Used ${ITEMS[id].name}.`);
}
export function turn(s,id,isItem=false){
  const r=s.run,b=r.battle,p=r.monkey;if(r.phase!=='battle'||!b?.ready)throw Error('Stop safely and begin the encounter first.');
  const enemy=b.enemy,ps=playerStats(s),es=stats(enemy),move=MOVES[id];
  if(!isItem&&(!p.loadout.includes(id)||!move||p.energy<move.cost))throw Error('Ability unavailable or insufficient energy.');
  if(isItem)applyItem(s,id);
  const em=MOVES[b.intent];p.guard=0;enemy.guard=0;
  const initiative=(actor,st,m)=>(m?.priority||0)*1000+st.speed*(actor.status.slow ? .6 : 1);
  const playerFirst=isItem||initiative(p,ps,move)>=initiative(enemy,es,em);
  const playerAction=()=>{if(!isItem)execute(s,p,enemy,id,ps,es,'You');};
  const enemyAction=()=>execute(s,enemy,p,b.intent,es,ps,'Rival');
  if(playerFirst){playerAction();if(enemy.hp>0)enemyAction();}else{enemyAction();if(p.hp>0)playerAction();}
  if(p.hp>0&&enemy.hp>0){tick(p,ps.hp,'You',s);tick(enemy,es.hp,'Rival',s);}
  p.guard=0;enemy.guard=0;
  if(p.hp<=0){endRun(s);return;}if(enemy.hp<=0){victory(s);return;}
  p.energy=Math.min(ps.maxEnergy,p.energy+1);enemy.energy=Math.min(es.maxEnergy,enemy.energy+1);b.turn++;planEnemy(s);
}
export function useItem(s,id){if(s.run.phase==='dead')throw Error('Begin a new run.');if(s.run.phase==='battle')return turn(s,id,true);applyItem(s,id);}
export function train(s){
  const r=s.run,p=r.monkey;if(r.phase!=='trail')throw Error('Train between encounters.');if(p.level>=60)throw Error('Maximum level reached.');
  const cost=8+p.level*4,discount=Math.min(cost-2,Math.floor(p.training/4));if(r.fruit<cost-discount)throw Error('Gather more fruit to train.');
  const before=playerStats(s).hp;r.fruit-=cost-discount;p.training=Math.max(0,p.training-discount*4);p.level++;
  p.hp=Math.min(playerStats(s).hp,p.hp+playerStats(s).hp-before+8);say(s,`Level ${p.level}! Training cost ${cost-discount} fruit.`);
}
export function evolutionCost(s,type){
  const p=s.run.monkey,next=p.stage+1,required=[0,3,6,10][next];if(!ELEMENTS[type]||next>3)return null;
  if(next===1)return {level:required,cost:{[type]:12},types:[type]};
  if(next===2)return {level:required,cost:{[type]:24},types:[...new Set([...p.types,type])]};
  if(!p.types.includes(type))return null;
  return {level:required,cost:Object.fromEntries(p.types.map(t=>[t,p.types.length===1?40:24])),types:p.types};
}
export function evolve(s,type){
  const r=s.run,p=r.monkey,offer=evolutionCost(s,type);if(r.phase!=='trail'||!offer)throw Error('That evolution is unavailable.');
  if(p.level<offer.level)throw Error(`Reach level ${offer.level} first.`);
  if(Object.entries(offer.cost).some(([t,n])=>r.essence[t]<n))throw Error('Gather more of the required essences.');
  const old=playerStats(s).hp;for(const [t,n]of Object.entries(offer.cost))r.essence[t]-=n;
  p.types=[...offer.types];p.stage++;p.hp=Math.min(playerStats(s).hp,p.hp+playerStats(s).hp-old);p.energy=playerStats(s).maxEnergy;
  const moves=unlocked(p).filter(id=>MOVES[id].type!=='neutral');p.loadout=['strike',...moves.slice(-2),'brace'];
  say(s,`Evolved into ${formName(p)}! Change your four equipped abilities in the Monkey tab.`);
}
export function equip(s,id){if(s.run.phase!=='trail'||!s.meta.gear[id])throw Error('Equip owned relics between battles.');s.meta.equipped[GEAR[id].slot]=id;s.run.monkey.hp=Math.min(s.run.monkey.hp,playerStats(s).hp);say(s,`Equipped ${GEAR[id].name}.`);}
export function setMove(s,slot,id){const p=s.run.monkey;if(s.run.phase!=='trail'||!Number.isInteger(slot)||slot<0||slot>3||(slot===0&&id!=='strike')||!unlocked(p).includes(id))throw Error('Change abilities between encounters.');if(p.loadout.includes(id)&&p.loadout[slot]!==id)throw Error('That ability is already equipped.');p.loadout[slot]=id;}
export function validate(s){
  if(!s||s.version!==VERSION||!['gps','practice'].includes(s.mode)||typeof s.name!=='string'||s.name.length>30)throw Error('Unsupported save.');
  const r=s.run,p=r?.monkey;if(!r||!SPECIES[p?.species]||!['trail','battle','dead'].includes(r.phase))throw Error('Invalid creature checkpoint.');
  for(const n of [r.distance,r.nextCache,r.nextEncounter,r.fruit,p.hp,p.energy,p.level,s.rng,s.meta?.legacy])if(!Number.isFinite(n)||n<0)throw Error('Invalid save numbers.');
  if(p.level<1||p.level>60||p.stage<0||p.stage>3||p.types.length>2||p.types.some(t=>!ELEMENTS[t])||new Set(p.types).size!==p.types.length)throw Error('Invalid evolution.');
  if(p.loadout[0]!=='strike'||!p.loadout.length||p.loadout.length>4||p.loadout.some(id=>!unlocked(p).includes(id))||new Set(p.loadout).size!==p.loadout.length)throw Error('Invalid ability loadout.');
  if(!r.essence||Object.keys(ELEMENTS).some(t=>!Number.isFinite(r.essence[t])||r.essence[t]<0)||!r.items||Object.keys(ITEMS).some(t=>!Number.isFinite(r.items[t])||r.items[t]<0))throw Error('Invalid inventory.');
  if(!r.vector||!Object.values(r.vector).every(Number.isFinite)||!r.weights||!Object.values(r.weights).every(n=>Number.isFinite(n)&&n>=0))throw Error('Invalid trail checkpoint.');
  if(r.phase==='battle'&&(!r.battle||!SPECIES[r.battle.enemy?.species]||!MOVES[r.battle.intent]||!Number.isFinite(r.battle.enemy.hp)))throw Error('Invalid battle checkpoint.');
  if(!s.meta.gear||!s.meta.equipped||!Array.isArray(s.meta.history)||s.meta.history.length>25||Object.entries(s.meta.gear).some(([id,n])=>!GEAR[id]||!Number.isInteger(n)||n<1||n>12))throw Error('Invalid legacy inventory.');
  if(!Array.isArray(s.log)||s.log.length>16||JSON.stringify(s).length>100000)throw Error('Save is too large.');return s;
}
