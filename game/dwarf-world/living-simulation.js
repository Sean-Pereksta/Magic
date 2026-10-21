/* Living Mountain: deterministic, browser-native fortress simulation.
 * Host owns terrain, equipment/combat and navigation. All goods have a location.
 * No timers, DOM, network, or storage side effects in this module. */
(function(root, factory) {
  const api = factory();
  if (typeof module === 'object' && module.exports) module.exports = api;
  else root.DwarfLivingSimulation = api;
})(typeof globalThis !== 'undefined' ? globalThis : this, function() {
  'use strict';
  const VERSION = 1;
  const ROLES = ['miner','hauler','mason','smith','farmer','cook','healer','soldier','engineer','explorer'];
  const MATERIALS = ['stone','ore','ingot','equipment','mushroom','meal','block','medicine','knowledge'];
  const ROOMS = {
    quarters: {name:'Residential Quarters',profession:null,cost:12,color:'#bfa273'},
    dining: {name:'Dining Hall',profession:null,cost:14,color:'#d9b46f'},
    kitchen: {name:'Kitchen',profession:'cook',cost:18,color:'#de9766',recipe:{input:{mushroom:2},output:{meal:4},seconds:8}},
    garden: {name:'Mushroom Garden',profession:'farmer',cost:16,color:'#a1b979',recipe:{input:{},output:{mushroom:4},seconds:10}},
    granary: {name:'Granary',profession:null,cost:12,color:'#af9861'},
    armory: {name:'Armory',profession:'soldier',cost:20,color:'#a7b9c7'},
    smelter: {name:'Smelter',profession:'smith',cost:22,color:'#df9267',recipe:{input:{ore:3},output:{ingot:2},seconds:12}},
    smithy: {name:'Smithy',profession:'smith',cost:24,color:'#c99579',recipe:{input:{ingot:2},output:{equipment:1},seconds:16}},
    stoneworks: {name:'Stoneworking Workshop',profession:'mason',cost:16,color:'#b7b3a5',recipe:{input:{stone:3},output:{block:2},seconds:8}},
    vault: {name:'Storage Vault',profession:null,cost:14,color:'#c8af6c'},
    medical: {name:'Medical Quarters',profession:'healer',cost:20,color:'#83c3b2',recipe:{input:{mushroom:2},output:{medicine:2},seconds:10}},
    library: {name:'Library',profession:'explorer',cost:22,color:'#9ba5d1',recipe:{input:{},output:{knowledge:1},seconds:30}},
    guardroom: {name:'Guardroom',profession:'soldier',cost:16,color:'#be927f'},
    market: {name:'Marketplace',profession:null,cost:22,color:'#c9b084'},
    throne: {name:'Throne Hall',profession:null,cost:32,color:'#dfbc6f'}
  };
  const INFRA = {
    road:{name:'Stone Road',cost:2,hp:120},rail:{name:'Minecart Rail',cost:6,hp:140},
    depot:{name:'Connected Depot',cost:15,hp:240},hauling:{name:'Hauling Station',cost:12,hp:200},
    gate:{name:'Reinforced Gate',cost:18,hp:420},wall:{name:'Fortified Wall',cost:10,hp:360},
    slit:{name:'Arrow Slit',cost:12,hp:260},barricade:{name:'Barricade',cost:8,hp:180},
    trap:{name:'Trap Corridor',cost:12,hp:120},watch:{name:'Watch Post',cost:14,hp:220},
    lift:{name:'Lift Network Station',cost:22,hp:260}
  };
  const clamp = (n,a,b)=>Math.max(a,Math.min(b,n));
  const finite = (n,f=0)=>Number.isFinite(n)?n:f;
  const distance = (a,b)=>Math.abs(a.x-b.x)+Math.abs(a.y-b.y);
  const center = r=>({x:r.x+Math.floor((r.w||1)/2),y:r.y+Math.floor((r.h||1)/2)});
  const key = (x,y)=>x+','+y;
  const hash = s=>Array.from(String(s)).reduce((n,c)=>(Math.imul(n,31)+c.charCodeAt(0))>>>0,7);
  const inventory = value=>Object.fromEntries(MATERIALS.map(k=>[k,clamp(Math.floor(finite(value?.[k])),0,1000000)]));
  function fresh(base={x:0,y:0}) {
    return {version:VERSION,time:0,nextId:1,rooms:[],infrastructure:[],deposits:[],designations:[],
      stores:[{id:'founders-store',type:'depot',x:base.x,y:base.y,w:1,h:1,active:true,inventory:inventory({stone:36,meal:12})}],
      chronicle:[],priorities:{mining:1,construction:1,food:1,defense:1,rescue:1,exploration:1},readiness:'normal',settlements:[],factions:[],caravans:[],stats:{produced:0,hauled:0,masterworks:0,rescues:0}};
  }
  function migrate(raw, base, width=1440, height=1440) {
    if (!raw) return fresh(base);
    if (!raw || typeof raw!=='object' || Array.isArray(raw)) throw new Error('Invalid Living Mountain save.');
    if (finite(raw.version,0)>VERSION) throw new Error('This Living Mountain save needs a newer game version.');
    const s = fresh(base), copy=JSON.parse(JSON.stringify(raw));
    Object.assign(s,copy,{version:VERSION,time:Math.max(0,finite(copy.time)),nextId:Math.max(1,Math.floor(finite(copy.nextId,1)))});
    for(const [name,max] of Object.entries({rooms:128,infrastructure:4096,deposits:8192,designations:2048,stores:512,chronicle:160,settlements:48,factions:256,caravans:24})) {
      if(copy[name]!==undefined&&!Array.isArray(copy[name])) throw new Error('Invalid '+name+' in fortress save.');
      s[name] = copy[name] || s[name];
      if(s[name].length>max) throw new Error('Fortress '+name+' exceeds supported capacity; the save was kept unchanged.');
    }
    const ids=new Set();
    for(const e of [...s.rooms,...s.infrastructure,...s.deposits,...s.stores,...s.designations,...s.settlements]) {
      if(!Number.isInteger(e.x)||!Number.isInteger(e.y)||e.x<0||e.y<0||e.x>=width||e.y>=height) throw new Error('Invalid fortress coordinates.');
      if(e.id){if(ids.has(e.id))throw new Error('Duplicate fortress object identity.');ids.add(e.id)}
      if(e.inventory) e.inventory=inventory(e.inventory);
    }
    for(const r of s.rooms) {
      if(!ROOMS[r.type]||!Number.isInteger(r.w)||!Number.isInteger(r.h)||r.w<2||r.h<2||r.w>12||r.h>12||r.x+r.w>width||r.y+r.h>height) throw new Error('Invalid room in fortress save.');
      r.inventory=inventory(r.inventory);r.level=clamp(Math.floor(finite(r.level,1)),1,3);r.progress=clamp(finite(r.progress),0,1);r.active=r.progress>=1;r.shortage='';
    }
    for(const v of s.infrastructure) if(!INFRA[v.type])throw new Error('Unknown infrastructure in fortress save.');
    for(const k of Object.keys(fresh().priorities))s.priorities[k]=clamp(finite(copy.priorities?.[k],1),0,3);
    if(!['normal','alert','lockdown'].includes(s.readiness))s.readiness='normal';
    s.stats=Object.assign(fresh().stats,copy.stats||{});
    return s;
  }
  function create(host, raw) {
    let state=migrate(raw,host.base(),host.width,host.height);
    const reservations=new Map(), tileClaims=new Map(), infraMap=new Map(), roomMap=new Map(), blocked=new Map();
    const reservedAmounts=new Map(),reservedByDwarf=new Map(),locationIndex=new Map(),depositTiles=new Map(),sourceBuckets=new Map(),bucketOffsets=new Map();
    const SUPPLY_CELL=16,LOCAL_SOURCES=96,REMOTE_SOURCES=32;
    let stockpiles=[],supplySources=[],supplyDirty=true,sourceCursor=0;
    const diagnostics={haulQueries:0,sourceChecks:0,maxSourceCandidates:0,supplyRebuilds:0};
    let schedule=0,revision=0,researchBoost=0;
    const dwarves=()=>host.dwarves();
    const id=p=>p+'-'+state.nextId++;
    const own=(k,d)=>!reservations.has(k)||reservations.get(k)===d.id;
    function log(text){state.chronicle.push({id:id('event'),time:state.time,text:String(text).slice(0,500)});if(state.chronicle.length>160)state.chronicle.shift();host.changed?.()}
    function reindex(){infraMap.clear();roomMap.clear();for(const v of state.infrastructure)infraMap.set(key(v.x,v.y),v);for(const r of state.rooms)for(let y=r.y;y<r.y+r.h;y++)for(let x=r.x;x<r.x+r.w;x++)roomMap.set(key(x,y),r);supplyDirty=true;revision++}
    function indexSource(source){if(!source.inventory)return;const point=center(source),k=key(Math.floor(point.x/SUPPLY_CELL),Math.floor(point.y/SUPPLY_CELL));let bucket=sourceBuckets.get(k);if(!bucket){bucket=[];sourceBuckets.set(k,bucket)}bucket.push(source);supplySources.push(source)}
    function rebuildSupplies(){
      locationIndex.clear();depositTiles.clear();sourceBuckets.clear();supplySources=[];
      stockpiles=[...state.stores,...state.rooms.filter(r=>r.active),...state.infrastructure.filter(v=>v.inventory)];
      for(const collection of [state.rooms,state.stores,state.infrastructure,state.deposits,state.settlements])for(const entry of collection)locationIndex.set(entry.id,entry);
      for(const pile of state.deposits){depositTiles.set(key(pile.x,pile.y),pile);indexSource(pile)}
      for(const store of stockpiles)indexSource(store);
      for(const k of bucketOffsets.keys())if(!sourceBuckets.has(k))bucketOffsets.delete(k);
      sourceCursor%=Math.max(1,supplySources.length);supplyDirty=false;diagnostics.supplyRebuilds++;
    }
    function ensureSupplies(){if(supplyDirty)rebuildSupplies()}
    function candidateSources(d){ensureSupplies();const result=[],seen=new Set(),bx=Math.floor(d.x/SUPPLY_CELL),by=Math.floor(d.y/SUPPLY_CELL);
      // Inspect a bounded part of nearby buckets, rotating dense buckets so the
      // first pile in a crowded corridor cannot permanently hide its neighbors.
      for(let ring=0;ring<=2&&result.length<LOCAL_SOURCES;ring++)for(let dy=-ring;dy<=ring&&result.length<LOCAL_SOURCES;dy++)for(let dx=-ring;dx<=ring&&result.length<LOCAL_SOURCES;dx++){
        if(Math.max(Math.abs(dx),Math.abs(dy))!==ring)continue;const k=key(bx+dx,by+dy),bucket=sourceBuckets.get(k);if(!bucket?.length)continue;
        const start=(bucketOffsets.get(k)||0)%bucket.length,count=Math.min(bucket.length,LOCAL_SOURCES-result.length);
        for(let n=0;n<count;n++){const source=bucket[(start+n)%bucket.length];if(!seen.has(source.id)){seen.add(source.id);result.push(source)}}bucketOffsets.set(k,(start+count)%bucket.length);
      }
      // Every query advances through distant sources as well. Rebuilding the
      // one-second index does not reset this cursor, so remote supplies remain
      // discoverable even in a large fortress with many nearby empty stores.
      const count=Math.min(REMOTE_SOURCES,supplySources.length);
      for(let n=0;n<count;n++){const source=supplySources[sourceCursor];sourceCursor=(sourceCursor+1)%supplySources.length;if(!seen.has(source.id)){seen.add(source.id);result.push(source)}}
      diagnostics.haulQueries++;diagnostics.sourceChecks+=result.length;diagnostics.maxSourceCandidates=Math.max(diagnostics.maxSourceCandidates,result.length);return result;
    }
    const reserveKey=(source,resource)=>source+'\u0000'+resource;
    function releaseHaul(d){const held=reservedByDwarf.get(d.id);if(!held)return;const remaining=(reservedAmounts.get(held.key)||0)-held.amount;if(remaining>0)reservedAmounts.set(held.key,remaining);else reservedAmounts.delete(held.key);reservedByDwarf.delete(d.id)}
    function reserveHaul(d,j){if(j.kind!=='haul'||d.living?.cargo)return;const k=reserveKey(j.source,j.resource),amount=Math.max(0,finite(j.amount));if(!amount)return;reservedAmounts.set(k,(reservedAmounts.get(k)||0)+amount);reservedByDwarf.set(d.id,{key:k,amount})}
    function person(d){
      if(!d.living) d.living={profession:d.guardPostId?'soldier':'miner',hunger:15,fatigue:10,morale:75,skill:0,history:['Arrived at the mountain.'],relationships:{},activity:'Exploring',shift:hash(d.id)%3};
      const p=d.living;
      if(!ROLES.includes(p.profession))p.profession='miner';
      p.hunger=clamp(finite(p.hunger,15),0,100);p.fatigue=clamp(finite(p.fatigue,10),0,100);p.morale=clamp(finite(p.morale,75),0,100);p.skill=Math.max(0,finite(p.skill));
      if(!Array.isArray(p.history))p.history=[];if(!p.relationships||typeof p.relationships!=='object')p.relationships={};
      return p;
    }
    function remember(d,text){const p=person(d);p.history.push(text);p.history=p.history.slice(-16);log(d.name+': '+text)}
    function canWalk(x,y,enemy=false){const v=infraMap.get(key(x,y));return !v||!['wall','slit','barricade'].includes(v.type)&&!(v.type==='gate'&&(enemy||state.readiness==='lockdown'))}
    function open(x,y){return host.walkable(x,y)&&canWalk(x,y)}
    function stores(){ensureSupplies();return stockpiles}
    function location(ref){ensureSupplies();return locationIndex.get(ref)||state.settlements.find(s=>s.id===ref)}
    function cancel(d){const p=person(d),j=p.job;
      // Ingredients belong to the in-progress job until completion. Interrupted
      // work returns them once, before the reservation is released.
      if(j?.kind==='produce'&&j.started&&!j.completed){const dest=location(j.dest),inputs=j.inputs||ROOMS[dest?.type]?.recipe?.input||{};if(!dest?.inventory&&!canDeposit(Math.round(d.x),Math.round(d.y),inputs))return false;j.started=false;for(const[k,n]of Object.entries(inputs)){if(dest?.inventory)dest.inventory[k]=(dest.inventory[k]||0)+n;else deposit(Math.round(d.x),Math.round(d.y),k,n)}}
      releaseHaul(d);if(j?.claim&&reservations.get(j.claim)===d.id)reservations.delete(j.claim);p.job=null;p.path=null;d.path=null;return true;
    }
    function canDeposit(x,y,contents={}){ensureSupplies();const pile=depositTiles.get(key(x,y));return (!!pile||state.deposits.length<8192)&&Object.entries(contents).every(([res,n])=>MATERIALS.includes(res)&&Number.isFinite(n)&&n>=0&&(pile?.inventory[res]||0)+n<=1000000)}
    function dropCargo(d){const p=person(d);if(!p.cargo)return true;if(!deposit(Math.round(d.x),Math.round(d.y),p.cargo.resource,p.cargo.amount))return false;p.cargo=null;return true}
    function deposit(x,y,resource,amount=1){if(!MATERIALS.includes(resource)||!Number.isFinite(amount)||amount<=0||!canDeposit(x,y,{[resource]:amount}))return false;let pile=depositTiles.get(key(x,y));if(!pile){pile={id:id('pile'),x,y,inventory:inventory()};state.deposits.push(pile);depositTiles.set(key(x,y),pile);locationIndex.set(pile.id,pile);indexSource(pile)}pile.inventory[resource]+=amount;return true}
    function preview(type,x,y,w=3,h=3){
      const def=ROOMS[type];if(!def)return {ok:false,reason:'Unknown room.'};
      if(![x,y,w,h].every(Number.isInteger)||w<2||h<2||w>12||h>12)return{ok:false,reason:'Rooms must be 2–12 tiles wide and long.'};
      if(state.rooms.length>=128)return{ok:false,reason:'The fortress supports 128 rooms.'};
      for(let yy=y;yy<y+h;yy++)for(let xx=x;xx<x+w;xx++)if(!host.explored(xx,yy)||!open(xx,yy)||roomMap.has(key(xx,yy))||host.occupied?.(xx,yy)||infraMap.has(key(xx,yy)))return{ok:false,reason:'Choose explored, excavated, unoccupied floor.'};
      let exits=0;
      for(let xx=x;xx<x+w;xx++){if(open(xx,y-1))exits++;if(open(xx,y+h))exits++}
      for(let yy=y;yy<y+h;yy++){if(open(x-1,yy))exits++;if(open(x+w,yy))exits++}
      if(!exits)return{ok:false,reason:'Carve a corridor to this room first.'};
      return{ok:true,cost:def.cost+Math.max(0,w*h-9),stone:Math.ceil(w*h/3),reason:exits+' corridor connections'};
    }
    function designate(type,x,y,w=3,h=3,style='carved'){
      const p=preview(type,x,y,w,h);if(!p.ok)return p;if(!host.spend(p.cost))return{ok:false,reason:'Need '+p.cost+' gold.'};
      const r={id:id('room'),type,x,y,w,h,style:['carved','mosaic','royal'].includes(style)?style:'carved',level:1,inventory:inventory(),progress:0,active:false,stoneNeeded:p.stone,shortage:'Waiting for stone and a builder',produced:0};state.rooms.push(r);reindex();log(ROOMS[type].name+' designated. Haulers will bring '+p.stone+' stone.');host.changed?.();return{ok:true,room:r};
    }
    function placeInfrastructure(type,x,y){
      const def=INFRA[type];if(!def||!Number.isInteger(x)||!Number.isInteger(y)||!host.explored(x,y)||!open(x,y)||infraMap.has(key(x,y))||roomMap.has(key(x,y))||host.occupied?.(x,y))return{ok:false,reason:'Choose an empty, explored corridor tile.'};
      if(state.infrastructure.length>=4096)return{ok:false,reason:'Infrastructure capacity reached.'};
      if(dwarves().some(d=>Math.round(d.x)===x&&Math.round(d.y)===y)&&['wall','slit','gate','barricade'].includes(type))return{ok:false,reason:'Wait until the passage is clear.'};
      if(!host.spend(def.cost))return{ok:false,reason:'Need '+def.cost+' gold.'};
      const v={id:id('infra'),type,x,y,hp:def.hp,maxHp:def.hp,w:1,h:1};if(['depot','hauling','lift'].includes(type))v.inventory=inventory();state.infrastructure.push(v);reindex();host.terrainChanged?.(x,y);host.changed?.();return{ok:true,infrastructure:v};
    }
    function removeInfrastructure(infrastructureId){const v=state.infrastructure.find(v=>String(v.id)===String(infrastructureId));if(!v)return{ok:false,reason:'This infrastructure no longer exists.'};
      if(MATERIALS.some(k=>v.inventory?.[k]>0)&&!canDeposit(v.x,v.y,v.inventory))return{ok:false,reason:'Loose supplies are at capacity. Haul nearby piles before dismantling.'};
      for(const d of dwarves())if(d.living?.job?.dest===v.id||d.living?.job?.source===v.id)cancel(d);
      for(const k of MATERIALS)if(v.inventory?.[k])deposit(v.x,v.y,k,v.inventory[k]);
      state.infrastructure=state.infrastructure.filter(item=>item!==v);reindex();host.terrainChanged?.(v.x,v.y);host.changed?.();log(INFRA[v.type].name+' dismantled; any supplies remain on site.');return{ok:true};
    }
    function digArea(x,y,w,h){if(![x,y,w,h].every(Number.isInteger)||w<1||h<1||w>48||h>48)return{ok:false,reason:'Brush an area up to 48 × 48 tiles.'};const existing=new Set(state.designations.map(v=>key(v.x,v.y)));let n=0;for(let yy=y;yy<y+h;yy++)for(let xx=x;xx<x+w;xx++)if(state.designations.length<2048&&host.explored(xx,yy)&&host.diggable?.(xx,yy)&&!existing.has(key(xx,yy))){state.designations.push({id:id('dig'),x:xx,y:yy});n++}host.changed?.();return{ok:n>0,reason:n?n+' mining orders placed.':'Choose visible solid rock.'}}
    function setProfession(dwarfId,role){const d=dwarves().find(v=>String(v.id)===String(dwarfId));if(!d||!ROLES.includes(role)||!cancel(d))return false;person(d).profession=role;if(role!=='soldier')d.guardPostId=null;remember(d,'Began work as '+role+'.');return true}
    function setReadiness(value){if(!['normal','alert','lockdown'].includes(value))return false;state.readiness=value;for(const d of dwarves())cancel(d);reindex();host.terrainChanged?.();log('Fortress readiness: '+value+'.');return true}
    function setPriority(k,value){if(!(k in state.priorities))return false;state.priorities[k]=clamp(Math.round(finite(value,1)),0,3);host.changed?.();return true}
    function claim(d,j){const p=person(d);if(p.job||j.claim&&!own(j.claim,d))return false;releaseHaul(d);if(j.claim)reservations.set(j.claim,d.id);p.job={...j,elapsed:0};reserveHaul(d,p.job);p.path=null;p.retry=0;d.path=null;return true}
    function available(source,res){
      // A workplace's own input buffer is not surplus. Without this floor,
      // understaffed workshops can shuttle the same scarce ore back and forth.
      let keep=0;const room=roomMap.get(key(source.x,source.y));
      if(room===source){
        if(!source.active)keep=res==='stone'?source.stoneNeeded:0;
        else{
          keep=(ROOMS[source.type].recipe?.input?.[res]||0)*3;
          if(source.type==='dining'&&res==='meal')keep=12;
          if(source.type==='armory'&&res==='equipment'||source.type==='medical'&&res==='medicine')keep=6;
          if(res==='block'&&source.type!=='stoneworks'&&source.level<3)keep=4;
        }
      }
      return Math.max(0,(source.inventory?.[res]||0)-keep-(reservedAmounts.get(reserveKey(source.id,res))||0));
    }
    function bestStore(d,predicate){let best=null,score=Infinity;for(const s of stores()){if(!predicate(s)||(blocked.get(d.id+':'+s.id)||0)>=state.time)continue;const cost=distance(d,center(s));if(cost<score){best=s;score=cost}}return best}
    function needJobs(d){const p=person(d);if(p.cargo){const dst=bestStore(d,s=>['vault','granary','depot','hauling','market'].includes(s.type));if(dst)return claim(d,{kind:'deliver',dest:dst.id,goal:center(dst)});return false}
      if(state.readiness==='lockdown'&&p.profession!=='soldier'){const r=bestStore(d,s=>s.type==='quarters'||s.type==='guardroom')||state.stores[0];if(r)return claim(d,{kind:'shelter',dest:r.id,goal:center(r)});}
      if(d.hp<d.maxHp*.65){const r=bestStore(d,s=>s.type==='medical'&&(s.inventory.medicine||0)>0);if(r)return claim(d,{kind:'recover',dest:r.id,goal:center(r)})}
      if(p.hunger>58){const r=bestStore(d,s=>(s.inventory.meal||0)>0);if(r)return claim(d,{kind:'eat',dest:r.id,goal:center(r)})}
      const offShift=state.time>120&&Math.floor(state.time/80)%3===(p.shift||0);
      if(p.fatigue>68||offShift&&p.fatigue>28){const r=bestStore(d,s=>s.type==='quarters');if(r)return claim(d,{kind:'sleep',dest:r.id,goal:center(r)})}
      const socialDue=state.time>=(p.socialAfter||0),scheduledVisit=p.socialAfter>0&&socialDue;
      if(socialDue&&(p.morale<55&&p.fatigue>20||offShift&&(p.morale<90&&p.fatigue>20||scheduledVisit))){const r=bestStore(d,s=>s.type==='dining'||s.type==='throne'||s.type==='market');if(r)return claim(d,{kind:'social',dest:r.id,goal:center(r)})}
      return false;
    }
    function priorityFor(r){return state.priorities[!r.active?'construction':['garden','kitchen','dining','granary'].includes(r.type)?'food':['smelter','smithy','armory','guardroom'].includes(r.type)?'defense':r.type==='medical'?'rescue':r.type==='library'?'exploration':'construction']||0}
    function haulJob(d){
      const inputs=[];for(const r of state.rooms){if(!r.active)inputs.push({r,res:'stone',desired:r.stoneNeeded});else {const recipe=ROOMS[r.type].recipe;for(const [res,n]of Object.entries(recipe?.input||{}))inputs.push({r,res,desired:n*3});if(r.type==='dining')inputs.push({r,res:'meal',desired:12});if(r.type==='armory')inputs.push({r,res:'equipment',desired:6});if(r.type==='medical')inputs.push({r,res:'medicine',desired:6});if(r.type!=='stoneworks'&&r.level<3)inputs.push({r,res:'block',desired:4});}}
      const sources=candidateSources(d),byResource=new Map(MATERIALS.map(k=>[k,[]])),capacity=person(d).profession==='hauler'?8:3;let best=null,score=Infinity;
      for(const source of sources){const goal=center(source),travel=distance(d,goal);for(const resource of MATERIALS){const amount=available(source,resource);if(amount>0)byResource.get(resource).push({source,goal,travel,amount})}}
      for(const {r,res,desired}of inputs){const gap=desired-r.inventory[res],priority=priorityFor(r);if(gap<=0||!priority)continue;const destination=center(r);for(const entry of byResource.get(res)){const{source,goal,travel,amount}=entry;if(source.id===r.id)continue;const routeKey=source.id+'>'+r.id+':'+res;if(!own(routeKey,d)||(blocked.get(routeKey)||0)>state.time)continue;const cost=(4+travel+distance(goal,destination))/priority;if(cost<score){score=cost;best={kind:'haul',source:source.id,dest:r.id,resource:res,amount:Math.min(gap,amount,capacity),goal,claim:routeKey}}}}
      // Collect loose quarry output into a real stockpile; nothing moves until pickup and delivery.
      if(!best){const r=bestStore(d,s=>['vault','granary','depot','hauling'].includes(s.type));if(r)for(const [res,entries]of byResource)for(const {source,goal,travel,amount}of entries){if(depositTiles.get(key(source.x,source.y))!==source)continue;const c=source.id+'>'+r.id+':'+res;if(own(c,d)&&(blocked.get(c)||0)<=state.time&&travel<score){score=travel;best={kind:'haul',source:source.id,dest:r.id,resource:res,amount:Math.min(amount,8),goal,claim:c}}}}
      return best&&claim(d,best);
    }
    function digJob(d){for(const v of state.designations){if(!own('dig:'+v.id,d)||(blocked.get(d.id+':'+v.id)||0)>state.time)continue;const stands=[[1,0],[-1,0],[0,1],[0,-1]].map(([dx,dy])=>({x:v.x+dx,y:v.y+dy})).filter(s=>open(s.x,s.y)).sort((a,b)=>distance(d,a)-distance(d,b));if(stands.length)return claim(d,{kind:'dig',dest:v.id,goal:stands[0],claim:'dig:'+v.id})}return false}
    function choose(d){const p=person(d);if(needJobs(d))return true;
      if(p.profession==='miner'&&d.pickaxe&&state.priorities.mining>state.priorities.construction&&digJob(d))return true;
      const builders=p.profession==='mason'||p.profession==='engineer'||p.profession==='miner';
      if(state.priorities.construction&&builders){const room=state.rooms.filter(r=>!r.active&&r.inventory.stone>=r.stoneNeeded&&own('work:'+r.id,d)&&(blocked.get(d.id+':'+r.id)||0)<=state.time).sort((a,b)=>distance(d,center(a))-distance(d,center(b)))[0];if(room)return claim(d,{kind:'construct',dest:room.id,goal:center(room),claim:'work:'+room.id})}
      if(p.profession==='hauler'||p.profession==='engineer'||p.profession==='miner'&&state.rooms.some(r=>!r.active))if(haulJob(d))return true;
      if(p.profession==='miner'&&d.pickaxe&&state.priorities.mining&&digJob(d))return true;
      const workplaces=state.rooms.filter(r=>r.active&&ROOMS[r.type].profession===p.profession&&priorityFor(r)).sort((a,b)=>(distance(d,center(a))+4)/priorityFor(a)-(distance(d,center(b))+4)/priorityFor(b));
      for(const r of workplaces){if(!own('work:'+r.id,d)||(blocked.get(d.id+':'+r.id)||0)>state.time)continue;const def=ROOMS[r.type],recipe=def.recipe;
        if(def.profession!==p.profession||!recipe)continue;
        if(['garden','kitchen'].includes(r.type)&&!state.priorities.food)continue;
        if(r.type==='library'&&!state.priorities.exploration)continue;
        if(Object.keys(recipe.output).some(k=>r.inventory[k]>=64)){r.shortage='Output full — assign a hauler';continue}
        const missing=Object.entries(recipe.input).find(([k,n])=>r.inventory[k]<n);if(missing){r.shortage='Needs '+missing[0];continue}r.shortage='';return claim(d,{kind:'produce',dest:r.id,goal:center(r),claim:'work:'+r.id});
      }
      if(p.profession==='soldier'&&state.priorities.defense){const r=bestStore(d,s=>s.type==='armory'&&s.inventory.equipment>0);if(r&&(!d.armor||!d.weapon))return claim(d,{kind:'equip',dest:r.id,goal:center(r)});const posts=state.rooms.filter(r=>r.type==='guardroom'&&r.active);if(posts.length){const r=posts[hash(d.id)%posts.length];return claim(d,{kind:'patrol',dest:r.id,goal:center(r)})}}
      if(p.profession==='healer'&&state.priorities.rescue){const patient=dwarves().filter(v=>v!==d&&v.hp>0&&v.hp<v.maxHp*.5&&own('rescue:'+v.id,d)).sort((a,b)=>distance(d,a)-distance(d,b))[0];if(patient)return claim(d,{kind:'rescue',patient:patient.id,goal:{x:Math.round(patient.x),y:Math.round(patient.y)},claim:'rescue:'+patient.id})}
      if(p.profession==='explorer'&&state.priorities.exploration){const target=state.settlements.find(s=>!s.discovered&&host.explored(s.x,s.y)&&(blocked.get(d.id+':'+s.id)||0)<state.time);if(target)return claim(d,{kind:'explore',dest:target.id,goal:center(target)})}
      return false;
    }
    function walk(d,dt,j){const p=person(d),goal=j.goal;if(distance(d,goal)<.08){d.x=goal.x;d.y=goal.y;return true}
      p.activity=p.cargo?'Hauling '+p.cargo.resource:'Traveling to '+j.kind;
      if(!p.path?.length){if(p.retry>state.time)return false;p.retry=state.time+.4;const route=host.route(Math.round(d.x),Math.round(d.y),goal.x,goal.y);if(!route){j.wait=(j.wait||0)+.4;if(j.wait>12){blocked.set(j.claim||d.id+':'+j.dest,state.time+12);blocked.set(d.id+':'+j.dest,state.time+12);cancel(d)}return false}p.path=route.map(v=>({x:v.x,y:v.y}));j.wait=0;
        // A point route excludes its starting cell. A dwarf inherited from the
        // base movement can be off-center within the destination tile, so finish
        // the last fraction of a tile instead of repeatedly requesting [].
        if(!p.path.length){if(Math.round(d.x)===goal.x&&Math.round(d.y)===goal.y&&open(goal.x,goal.y))p.path=[{x:goal.x,y:goal.y}];else return false;}
      }
      const next=p.path[0];if(!open(next.x,next.y)){p.path=null;p.retry=state.time+.2;return false}
      const k=key(next.x,next.y),claimant=tileClaims.get(k);if(claimant&&claimant!==d.id){p.congestion=(p.congestion||0)+dt;if(p.congestion>2){p.path=null;p.congestion=0}return false}tileClaims.set(k,d.id);p.congestion=0;
      const track=infraMap.get(key(Math.round(d.x),Math.round(d.y))),boost=track?.type==='rail'&&p.cargo?2.1:track?.type==='road'?1.35:track?.type==='lift'?1.5:1;
      const dx=next.x-d.x,dy=next.y-d.y,len=Math.hypot(dx,dy),speed=Math.max(.5,d.speed||2)*(1+p.skill*.0005)*boost*(p.hunger>90?.8:1),step=speed*dt;
      if(len<=step){d.x=next.x;d.y=next.y;p.path.shift()}else {d.x+=dx/len*step;d.y+=dy/len*step}return false;
    }
    function updateDwarf(d,dt){const p=person(d);p.hunger=Math.min(100,p.hunger+dt*.10);p.fatigue=Math.min(100,p.fatigue+dt*.055);
      const hardship=(p.hunger>85?.12:0)+(p.fatigue>85?.10:0)+(d.hp<d.maxHp*.5?.08:0);if(hardship)p.morale=Math.max(0,p.morale-dt*hardship);
      const danger=host.threat?.(d.x,d.y,8);if(danger&&p.profession==='soldier'){if(p.job)cancel(d);p.activity='Defending';return false}
      if(danger&&p.profession!=='soldier'&&state.readiness!=='normal'&&p.job?.kind!=='shelter'){cancel(d);const r=bestStore(d,s=>s.type==='quarters')||state.stores[0];if(r)claim(d,{kind:'shelter',dest:r.id,goal:center(r)})}
      if(d.carrying){p.activity='Returning treasure';return false}
      if(!danger&&!p.job&&(p.celebrateUntil||0)>state.time){p.activity='Celebrating';return true}
      if(!p.job){p.nextThink=finite(p.nextThink)-dt;if(p.nextThink<=0){p.nextThink=.6+hash(d.id)%7*.03;choose(d)}}
      const j=p.job;if(!j){p.activity=p.profession==='miner'&&state.priorities.mining?'Mining frontier':'Awaiting work';return p.profession==='miner'?!state.priorities.mining:p.profession==='explorer'?!state.priorities.exploration:p.profession!=='soldier'}
      if(!walk(d,dt,j))return true;
      const dst=location(j.dest);j.elapsed+=dt;
      if(j.kind==='haul'){
        if(!p.cargo){const source=location(j.source);if(!source){cancel(d);return true}const amount=Math.min(j.amount,source.inventory[j.resource]||0);if(amount<=0){cancel(d);return true}releaseHaul(d);source.inventory[j.resource]-=amount;p.cargo={resource:j.resource,amount};j.goal=center(dst||state.stores[0]);p.path=null;j.elapsed=0;return true}
        if(dst){dst.inventory[p.cargo.resource]+=p.cargo.amount;state.stats.hauled+=p.cargo.amount;p.cargo=null}cancel(d);return true;
      }
      if(j.kind==='deliver'){if(dst&&p.cargo){dst.inventory[p.cargo.resource]+=p.cargo.amount;p.cargo=null}cancel(d);return true}
      if(j.kind==='dig'){const target=state.designations.find(v=>v.id===j.dest);if(!target||!host.diggable(target.x,target.y)){state.designations=state.designations.filter(v=>v.id!==j.dest);cancel(d);return true}p.activity='Mining';if(j.elapsed>=1/Math.max(.3,d.miningRate||1)){j.elapsed=0;host.mine?.(target.x,target.y,d.miningPower||4,d)}return true}
      if(j.kind==='construct'){if(!dst){cancel(d);return true}p.activity='Constructing';if(dst.inventory.stone<dst.stoneNeeded){cancel(d);return true}dst.progress=Math.min(1,dst.progress+dt/Math.max(4,dst.w*dst.h*.7));if(dst.progress>=1){dst.inventory.stone-=dst.stoneNeeded;dst.active=true;dst.shortage='';supplyDirty=true;remember(d,'Completed '+ROOMS[dst.type].name+'.');cancel(d)}return true}
      if(j.kind==='produce'){
        if(!dst){cancel(d);return true}const recipe=ROOMS[dst.type].recipe;p.activity=({smithy:'Smithing',smelter:'Smelting',garden:'Farming',kitchen:'Cooking',stoneworks:'Stoneworking',medical:'Preparing medicine',library:'Studying'})[dst.type];
        if(!j.started){if(Object.entries(recipe.input).some(([k,n])=>dst.inventory[k]<n)){cancel(d);return true}j.inputs={...recipe.input};for(const[k,n]of Object.entries(recipe.input))dst.inventory[k]-=n;j.started=true}
        const talent=(d.trait?.id==='hard_worker'||d.trait?.id==='strong')?0.08:0;
        if(j.elapsed>=recipe.seconds/(1+p.skill*.003+researchBoost+talent+(dst.level-1)*.1)){for(const[k,n]of Object.entries(recipe.output))dst.inventory[k]+=n;p.skill++;dst.produced++;state.stats.produced++;p.morale=Math.min(100,p.morale+1);if(p.skill%20===0){state.stats.masterworks++;remember(d,'Created a masterwork in the '+ROOMS[dst.type].name+'.');for(const[k,n]of Object.entries(recipe.output))dst.inventory[k]+=n;p.celebrateUntil=state.time+3}j.completed=true;cancel(d)}return true;
      }
      if(j.kind==='eat'){p.activity='Eating';if(j.elapsed>=2){if(dst?.inventory.meal>0){dst.inventory.meal--;p.hunger=Math.max(0,p.hunger-75);p.morale=Math.min(100,p.morale+(dst.type==='dining'?12:4))}cancel(d)}return true}
      if(j.kind==='sleep'){p.activity='Sleeping';p.fatigue=Math.max(0,p.fatigue-dt*5);if(p.fatigue<10)cancel(d);return true}
      if(j.kind==='social'){p.activity='Socializing';p.morale=Math.min(100,p.morale+dt*3);if(j.elapsed>=7){const other=dwarves().find(v=>v!==d&&distance(v,d)<4);if(other){p.relationships[other.id]=clamp(finite(p.relationships[other.id])+1,-10,10);if(p.relationships[other.id]===3)remember(d,'Became friends with '+other.name+'.')}p.socialAfter=state.time+120+hash(d.id)%60;cancel(d)}return true}
      if(j.kind==='recover'){p.activity='Recovering';if(j.elapsed>=3){if(dst?.inventory.medicine>0){dst.inventory.medicine--;d.hp=Math.min(d.maxHp,d.hp+d.maxHp*.35)}cancel(d)}return true}
      if(j.kind==='equip'){p.activity='Equipping';if(dst?.inventory.equipment>0&&host.equip?.(d)){dst.inventory.equipment--;remember(d,'Collected equipment from the armory.')}cancel(d);return true}
      if(j.kind==='patrol'){p.activity='Patrolling';if(j.elapsed>4)cancel(d);return true}
      if(j.kind==='rescue'){const patient=dwarves().find(v=>v.id===j.patient);if(!patient||distance(d,patient)>2){cancel(d);return true}p.activity='Tending wounded';if(j.elapsed>4){patient.hp=Math.min(patient.maxHp,patient.hp+patient.maxHp*.2);state.stats.rescues++;remember(d,'Stabilized '+patient.name+' in the tunnels.');cancel(d)}return true}
      if(j.kind==='explore'){if(dst&&!dst.discovered){dst.discovered=true;remember(d,'Discovered '+dst.name+'.')}cancel(d);return true}
      if(j.kind==='shelter'){p.activity='Sheltering';p.fatigue=Math.max(0,p.fatigue-dt);if(state.readiness==='normal'&&!danger)cancel(d);return true}
      cancel(d);return true;
    }
    function begin(dt){state.time+=Math.min(.25,Math.max(0,dt));tileClaims.clear();schedule+=dt;if(schedule<1)return;schedule=0;
      const living=new Set(dwarves().filter(d=>d.hp>0).map(d=>d.id));for(const[k,v]of reservations)if(!living.has(v))reservations.delete(k);
      for(const dwarfId of reservedByDwarf.keys())if(!living.has(dwarfId))releaseHaul({id:dwarfId});
      for(const[k,v]of blocked)if(v<state.time)blocked.delete(k);
      state.deposits=state.deposits.filter(s=>MATERIALS.some(k=>s.inventory[k]>0));
      // Dead carriers may drop cargo below; invalidate removed-pile lookups
      // before those drops so they cannot refill an orphaned array entry.
      supplyDirty=true;
      state.designations=state.designations.filter(s=>host.diggable?.(s.x,s.y));
      researchBoost=Math.min(.2,state.rooms.filter(r=>r.active&&r.type==='library').reduce((n,r)=>n+r.inventory.knowledge*.01,0));
      for(const d of dwarves())if(d.hp<=0){dropCargo(d);cancel(d)}
      for(const r of state.rooms){if(!r.active)r.shortage=r.inventory.stone<r.stoneNeeded?'Needs '+(r.stoneNeeded-r.inventory.stone)+' stone':'Waiting for a builder';}
      ensureSupplies();
    }
    function serialize(){return JSON.parse(JSON.stringify(state))}
    function restoreReservations(){reservations.clear();reservedAmounts.clear();reservedByDwarf.clear();for(const d of dwarves()){const p=person(d);p.path=null;p.retry=0;p.nextThink=0;if(p.job?.claim){if(reservations.has(p.job.claim))p.job=null;else reservations.set(p.job.claim,d.id)}if(p.job)reserveHaul(d,p.job)}}
    function restore(raw){state=migrate(raw,host.base(),host.width,host.height);blocked.clear();reindex();rebuildSupplies();restoreReservations();return state}
    function removeRoom(roomId){const r=state.rooms.find(v=>v.id===roomId);if(!r)return false;const contents={...r.inventory};for(const d of dwarves()){const j=d.living?.job;if(j?.kind==='produce'&&j.dest===roomId&&j.started&&!j.completed)for(const[k,n]of Object.entries(j.inputs||ROOMS[r.type].recipe?.input||{}))contents[k]=(contents[k]||0)+n}const point=center(r);if(Object.values(contents).some(n=>n>0)&&!canDeposit(point.x,point.y,contents))return false;for(const d of dwarves())if(d.living?.job?.dest===roomId||d.living?.job?.source===roomId)cancel(d);for(const k of MATERIALS)if(r.inventory[k])deposit(point.x,point.y,k,r.inventory[k]);state.rooms=state.rooms.filter(v=>v!==r);reindex();log(ROOMS[r.type].name+' dismantled; supplies left on site.');return true}
    function upgrade(roomId){const r=state.rooms.find(v=>v.id===roomId);if(!r||!r.active||r.level>=3)return{ok:false,reason:'Select a completed room below level 3.'};if(r.inventory.block<4)return{ok:false,reason:'Needs 4 stone blocks delivered to this room.'};if(!host.spend(ROOMS[r.type].cost*r.level))return{ok:false,reason:'Not enough gold.'};r.inventory.block-=4;r.level++;r.style=r.level===3?'royal':'mosaic';log(ROOMS[r.type].name+' upgraded to level '+r.level+'.');return{ok:true}}
    reindex();rebuildSupplies();restoreReservations();
    return {get state(){return state},get revision(){return revision},get diagnostics(){return {...diagnostics}},person,log,begin,updateDwarf,deposit,canDeposit,preview,designate,placeInfrastructure,removeInfrastructure,digArea,setProfession,setReadiness,setPriority,canWalk,infraAt:(x,y)=>infraMap.get(key(x,y)),roomAt:(x,y)=>roomMap.get(key(x,y)),cancel,dropCargo,serialize,restore,removeRoom,upgrade,reindex,center,stores};
  }
  return {VERSION,ROOMS,ROLES,INFRA,MATERIALS,fresh,migrate,create,center};
});
