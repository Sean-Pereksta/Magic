/* Read-only presentation adapters. Nothing in this module is serialized and no
   simulation random numbers are consumed. Snapshots exist only to detect events. */
const STX_GV_LIMITS = Object.freeze({sprites:96, spriteBuilds:1, pulses:48,
  combat:240, craft:360, civilians:110, orbitTraffic:36, planetDetail:24, routes:80, labels:30, stations:100});
const STX_GV_FAMILIES = ['Terran','Ocean','Desert','Volcanic','Ice','Barren',
  'Toxic','Jungle','Industrial','Gas giant','Exotic'];
const STX_GV_TIERS = [
  {name:'Patrol',min:0,count:2,span:10,hull:'corvette'},
  {name:'Small fleet',min:10,count:4,span:18,hull:'frigate'},
  {name:'Task force',min:35,count:9,span:30,hull:'cruiser'},
  {name:'Battle fleet',min:100,count:18,span:47,hull:'battleship'},
  {name:'Armada',min:250,count:32,span:65,hull:'carrier'},
  {name:'Doomstack',min:500,count:48,span:88,hull:'dreadnought'}
];
const stxGV = {time:0,lastNow:null,lastScan:-1,world:null,index:null,hover:null,
  pickedRoute:null,pointer:null,pointerDown:null,quality:1,cost:0,slow:0,fast:0,
  sprites:new Map(),pulses:Array.from({length:STX_GV_LIMITS.pulses},()=>({until:0})),
  history:new Map(),routeHistory:new Map(),routesWarm:false,hits:[],labels:[],stats:{},budget:{},
  reduced:typeof matchMedia==='function'&&matchMedia('(prefers-reduced-motion: reduce)').matches};

function stxGVHash(value){
  let h=2166136261;
  for(const c of String(value)){h^=c.charCodeAt(0);h=Math.imul(h,16777619)}
  return h>>>0;
}
function stxGVFamily(p){
  // Resource-independent geology stays stable as economies and ownership change.
  const named=String(p.biome||p.planetType||'').toLowerCase();
  const known=STX_GV_FAMILIES.find(x=>x.toLowerCase()===named);
  return known||STX_GV_FAMILIES[stxGVHash(`${p.id}:${p.name}:${p.x}:${p.y}`)%STX_GV_FAMILIES.length];
}
function stxGVTier(power){
  const n=Math.max(0,Number(power)||0);
  return STX_GV_TIERS.reduce((tier,next)=>n>=next.min?next:tier,STX_GV_TIERS[0]);
}
function stxGVDevelopment(p){
  if(p.owner==null)return 0;
  const i=p.infra||{},score=(i.city||0)+(i.factory||0)*.8+
    (i.shipyard||0)*.5+Math.min(5,(p.pop||0)*8)+Math.min(2,p.prosperity||0);
  return p.home?4:score>=12?3:score>=5?2:1;
}
function stxGVRoles(p){
  const i=p.infra||{},f=p.orbitalFacilities||[],s=p.specialization||'';
  return [
    ['capital',p.home&&p.owner!=null?100:0],
    ['fortress',(i.defense||0)*2+(p.orbitals?.base||0)*3+(s==='Military Stronghold'?12:0)],
    ['industry',(i.factory||0)*2+(s==='Industrial Center'?12:0)],
    ['mining',(i.mine||0)*1.5+(s==='Mining World'?12:0)],
    ['components',p.stxEconomicFocus==='components'?15:0],
    ['shipyard',(i.shipyard||0)*2.5],
    ['logistics',f.filter(x=>(x.modules||[]).includes('logistics')&&x.hp>0).length*6],
    ['trade',(p.tradeHub||0)*2+(p.tradeStation?.level||0)*3+(/Trade Hub|Free Port/.test(s)?12:0)],
    ['population',(i.city||0)+(s==='Overcrowded Core'?12:0)],
    ['research',(i.research||0)*3]
  ].filter(([,n])=>n>0&&p.owner!=null).sort((a,b)=>b[1]-a[1]).map(([name])=>name);
}
function stxGVColor(owner){return empire(owner)?.color||'#889bb8'}
function stxGVPointVisible(p,pad=100){return !!p&&visible(p.x,p.y,pad)}
function stxGVIndex(){
  const planets=new Map(state.planets.map(p=>[p.id,p]));
  const sensors=state.planets.filter(p=>p.owner===0).map(p=>({x:p.x,y:p.y,r:Math.max(660,stxScanRange(p))}));
  const bases=new Map((state.deepSpaceBases||[]).map(p=>[p.id,p]));
  const operations=new Map((state.deepSpaceOperations||[]).filter(o=>o.active!==false).map(o=>[o.id,o]));
  const moving=new Map(),groups=new Map(),busy=new Set(),battles=[],fleets=new Map(state.fleets.map(f=>[f.id,f]));
  for(const s of state.ships)if(s.fleetId&&!moving.has(s.fleetId))moving.set(s.fleetId,s);
  for(const b of [...state.battles,...(state.deepSpaceBattles||[])]){
    for(const id of [...(b.attackerFleetIds||[]),...(b.defenderFleetIds||[])])busy.add(id);
    const location=planets.get(b.planetId)||bases.get(b.baseId);
    if(location)battles.push({b,location,invasion:!!b.planetId});
  }
  for(const f of state.fleets){
    if(f.destroyed||moving.has(f.id))continue;
    const key=`${f.owner}:${f.location}`;
    if(!groups.has(key))groups.set(key,[]);
    groups.get(key).push(f.id);
  }
  // Space-control contests already inflict periodic losses in the simulation.
  // Show those exposed engagements as fleet combat, distinct from invasions.
  for(const op of operations.values()){
    const f=fleets.get(op.fleetId),target=op.targetType==='base'?bases.get(op.targetId):planets.get(op.targetId);
    if(!f||!target||!op.nextControlCheckAt||op.nextControlCheckAt<=state.simTime||busy.has(f.id))continue;
    if(f.owner!==0&&target.owner!==0&&!sensors.some(s=>Math.hypot(s.x-op.x,s.y-op.y)<=s.r))continue;
    const ids=op.targetType==='base'?(target.dockedFleetIds||[]):groups.get(`${target.owner}:${target.id}`)||[];
    const defenders=ids.map(id=>fleets.get(id)).filter(d=>d&&!d.destroyed&&!moving.has(d.id)&&!busy.has(d.id));
    const fixed=op.targetType==='planet'?(target.orbitals?.base||0)*10+(target.orbitals?.station||0)*4:0;
    const strength=defenders.reduce((sum,d)=>sum+d.strength,fixed);if(strength<=0)continue;
    const b={id:`control:${op.id}`,attacker:op.owner,defender:target.owner,
      attackerStrength:f.strength,attackerInitial:Math.max(f.strength,op.strength||0),
      defenderStrength:strength,defenderInitial:defenders.reduce((sum,d)=>sum+Math.max(d.strength,d.maxServiceStrength||0),fixed),
      attackerFleetIds:[f.id],defenderFleetIds:defenders.map(d=>d.id)};
    for(const id of [...b.attackerFleetIds,...b.defenderFleetIds])busy.add(id);
    battles.push({b,location:target,invasion:false,focusKind:op.targetType==='base'?'base':'planet'});
  }
  const orbitIndex=new Map();
  for(const ids of groups.values())ids.sort((a,b)=>String(a).localeCompare(String(b))).forEach((id,i)=>orbitIndex.set(id,i));
  return {planets,bases,operations,moving,busy,battles,orbitIndex,
    fleets,
    sensors};
}
function stxGVDetected(p){return stxGV.index.sensors.some(s=>Math.hypot(s.x-p.x,s.y-p.y)<=s.r)}
function stxGVFleetPosition(f){
  if(!f||f.destroyed)return null;
  const ix=stxGV.index,s=ix.moving.get(f.id);
  if(s)return {x:s.x,y:s.y,ship:s};
  const operation=ix.operations.get(f.deepSpaceOperationId),base=ix.bases.get(f.deepSpaceBaseId);
  if(operation)return {x:operation.x,y:operation.y,operation};
  if(base)return {x:base.x,y:base.y,base};
  const p=ix.planets.get(f.location);if(!p)return null;
  const n=ix.orbitIndex.get(f.id)||0,ring=50+stxGVTier(f.strength).span*.7+(n%3)*18+Math.floor(n/3)*8;
  const angle=stxFleetPhase(f.id)+(stxGV.reduced?0:stxGV.time)*(.13+(n%3)*.022)*(n%2?-1:1);
  return {x:p.x+Math.cos(angle)*ring,y:p.y+Math.sin(angle)*ring*.46,
    planet:p,stationed:true,orbitRadius:ring,orbitAngle:angle};
}
function stxGVShipVisible(s){
  if(!STX_MILITARY_TYPES.has(s.type)||s.owner===0||stxGVDetected(s))return true;
  const target=stxGV.index.planets.get(s.to);
  return (target?.owner===0&&Math.hypot(s.x-target.x,s.y-target.y)<980)||
    state.battles.some(b=>b.planetId===s.to&&(b.attacker===0||b.defender===0));
}
function stxGVPriority(p,{selected=false,owner=null,power=0,battle=false}={}){
  const s=worldToScreen(p.x,p.y),distance=Math.hypot(s.x-innerWidth/2,s.y-innerHeight/2);
  return (selected?100000:0)+(owner===0?10000:0)+(battle?3000:0)+
    Math.min(2000,power*2)-distance;
}
function stxGVFocused(kind,id){
  return (kind==='planet'&&state.selected?.id===id)||
    (kind==='fleet'&&state.stxFocusedFleet===id)||
    (kind==='base'&&state.stxSelectedDeepBaseId===id)||
    (kind==='facility'&&state.stxSelectedFacilityId===id)||
    (kind==='route'&&stxGV.pickedRoute===id)||
    (stxGV.hover?.kind===kind&&stxGV.hover.id===id);
}
function stxGVPulse(kind,p,color,extra={}){
  if(!p||!Number.isFinite(p.x)||!Number.isFinite(p.y))return;
  const priority=stxGVPriority(p,{owner:p.owner,selected:state.selected?.id===p.id});
  const slot=stxGV.pulses.find(f=>f.until<=stxGV.time)||
    stxGV.pulses.reduce((a,b)=>a.priority<b.priority?a:b);
  if(slot.until>stxGV.time&&slot.priority>priority)return;
  Object.assign(slot,{kind,x:p.x,y:p.y,id:p.id,color,start:stxGV.time,
    until:stxGV.time+1.8,priority,oldColor:null,...extra});
}
function stxGVSnapshot(key,value,p,kind,color){
  const previous=stxGV.history.get(key);
  stxGV.history.set(key,value);
  if(previous!==undefined&&previous!==value)stxGVPulse(kind,p,color);
  return previous;
}
function stxGVObserve(){
  if(stxGV.world!==state.planets||state.simTime<stxGV.lastScan){
    stxGV.world=state.planets;stxGV.history.clear();stxGV.routeHistory.clear();
    stxGV.pulses.forEach(f=>f.until=0);stxGV.pickedRoute=null;stxGV.hover=null;stxGV.lastScan=-1;stxGV.routesWarm=false;
  }
  // Read even while paused: a manual declaration or an order can change state
  // without advancing simTime. Equal snapshots do not replay effects.
  const initial=stxGV.lastScan<0,seen=new Set();
  const remember=(key,value)=>{seen.add(key);const old=stxGV.history.get(key);stxGV.history.set(key,value);return old};
  for(const p of state.planets){
    const old=remember(`planet:${p.id}`,p.owner);
    if(!initial&&old!==undefined&&old!==p.owner)stxGVPulse(old==null?'colony':'capture',p,stxGVColor(p.owner),{oldColor:stxGVColor(old)});
    const facilities=[...(p.orbitalFacilities||[]),...(p.tradeStation?[{id:`trade:${p.id}`,hp:p.tradeStation.hp??1}]:[])];
    for(const f of facilities){const alive=f.hp>0,was=remember(`facility:${f.id}`,alive);if(was===true&&!alive)stxGVPulse('station-destroyed',p,'#ff8568')}
  }
  for(const base of stxGV.index.bases.values()){
    const old=remember(`base:${base.id}`,base.status);
    if(old&&old!==base.status&&base.status==='wreck')stxGVPulse('station-destroyed',base,'#ff8568');
  }
  for(const {b,location,invasion} of stxGV.index.battles){
    const old=remember(`battle:${b.id}`,{a:b.attackerInitial,d:b.defenderInitial});
    if(!initial&&!old)stxGVPulse(invasion?'invasion':'battle',location,'#ff627f');
    else if(old&&(b.attackerInitial>old.a+.1||b.defenderInitial>old.d+.1))
      stxGVPulse('reinforcement',location,stxGVColor(b.attackerInitial>old.a?b.attacker:b.defender));
  }
  for(const f of state.fleets){
    // Do not expose undetected enemy losses or orders through map pulses.
    const pos=stxGVFleetPosition(f)||stxGV.index.planets.get(f.location)||stxGV.index.bases.get(f.deepSpaceBaseId);
    const old=remember(`fleet:${f.id}`,{destroyed:f.destroyed,power:f.maxServiceStrength||f.strength,ship:stxGV.index.moving.get(f.id)?.id});
    if(!initial&&pos&&(f.owner===0||stxGVDetected(pos))){
      if(old&&!old.destroyed&&f.destroyed&&old.power>=100)stxGVPulse('fleet-destroyed',pos,stxGVColor(f.owner));
      const ship=stxGV.index.moving.get(f.id);
      if(ship&&old?.ship!==ship.id)stxGVPulse('departure',pos,stxGVColor(f.owner),{fleetId:f.id});
    }
  }
  for(const war of state.wars){
    const old=remember(`war:${war.id}`,war.active);
    if(!initial&&war.active&&!old){
      const p=state.planets.find(p=>p.owner===war.a&&p.home)||state.planets.find(p=>p.owner===war.a);
      stxGVPulse('war',p,'#ff627f');
    }
  }
  for(const key of stxGV.history.keys())if(!seen.has(key))stxGV.history.delete(key);
  stxGV.lastScan=state.simTime;
}
function stxGVRouteKind(s){
  if(STX_MILITARY_TYPES.has(s.type))return 'military';
  if(s.projectDelivery||s.deepBaseId)return 'logistics';
  if(s.commercial)return 'trade';
  if(s.type==='supply'||s.type==='construction')return 'logistics';
  if(s.commercial||['freighter','tanker','liner','luxury','trade'].includes(s.type))return 'trade';
  return 'civilian';
}
function stxGVRouteBlocked(r){
  if(r.kind!=='trade'&&r.kind!=='logistics')return '';
  const a=r.a,b=r.b;
  if(a.status==='wreck'||b.status==='wreck')return 'Station destroyed';
  if(a.underAttack||b.underAttack)return 'System under attack';
  if(a.owner!=null&&b.owner!=null&&a.owner!==b.owner){
    if(empiresAtWar(a.owner,b.owner))return 'War';
    if(stxRTEmbargoed(a.owner,b.owner))return 'Embargo';
  }
  for(const op of stxGV.index.operations.values()){
    if(op.owner===r.owner)continue;
    // Match the actual interception geometry: an active operation's radius.
    const dx=b.x-a.x,dy=b.y-a.y,t=clamp(((op.x-a.x)*dx+(op.y-a.y)*dy)/Math.max(1,dx*dx+dy*dy),0,1);
    if(Math.hypot(a.x+dx*t-op.x,a.y+dy*t-op.y)<=op.radius&&
      empiresAtWar(op.owner,r.owner))return op.kind==='blockade'?'Blockade':'Commerce raid';
  }
  return '';
}
function stxGVRoutes(){
  const routes=new Map(),ix=stxGV.index;
  for(const s of state.ships){
    if(s.stxCancelled||s.stxIntercepted||!stxGVShipVisible(s))continue;
    const endpoint=(x,y,planetId)=>{
      const planet=ix.planets.get(planetId),base=ix.bases.get(s.deepBaseId);
      if(planet&&Math.hypot(planet.x-x,planet.y-y)<1)return planet;
      if(base&&Math.hypot(base.x-x,base.y-y)<1)return base;
      return Number.isFinite(x)&&Number.isFinite(y)?{x,y,id:`point:${x}:${y}`,owner:s.owner,name:'Deep space'}:null;
    };
    // Deep transits reuse from/to as compatibility aliases. Their explicit
    // coordinates, rather than those aliases, describe the actual journey.
    const a=s.stxDeepTransit?endpoint(s.startX,s.startY,s.from):ix.planets.get(s.from);
    const b=s.stxDeepTransit?endpoint(s.targetX,s.targetY,s.to):ix.planets.get(s.to);
    if(!a||!b)continue;
    const kind=stxGVRouteKind(s),key=`${kind}:${a.id}:${b.id}`,r=routes.get(key)||
      {id:key,a,b,owner:s.owner,kind,ships:[],cargo:new Set(),fleetId:s.fleetId};
    r.ships.push(s);Object.keys(s.cargo||{}).forEach(c=>r.cargo.add(c));routes.set(key,r);
  }
  // Remember only observed shipping corridors briefly so disruption remains
  // visible after the simulation withdraws a ship. Never invent commerce lanes.
  for(const r of routes.values())if(r.kind==='trade'||r.kind==='logistics'){
    const old=stxGV.routeHistory.get(r.id),blocked=stxGVRouteBlocked(r);
    if(!old&&stxGV.routesWarm&&r.kind==='trade')stxGVPulse('trade',r.b,'#efc572');
    if(old&&!old.blocked&&blocked)stxGVPulse('interruption',{x:(r.a.x+r.b.x)/2,y:(r.a.y+r.b.y)/2},'#ff627f');
    stxGV.routeHistory.set(r.id,{...r,ships:[],last:stxGV.time,blocked});
  }
  for(const [id,r] of stxGV.routeHistory){
    if(stxGV.time-r.last>12){stxGV.routeHistory.delete(id);continue}
    if(!routes.has(id)){
      const a=ix.planets.get(r.a.id)||ix.bases.get(r.a.id)||r.a,b=ix.planets.get(r.b.id)||ix.bases.get(r.b.id)||r.b;
      const restored={...r,a,b},blocked=stxGVRouteBlocked(restored);
      if(blocked){restored.blocked=blocked;routes.set(id,restored)}
    }
  }
  while(stxGV.routeHistory.size>STX_GV_LIMITS.routes*4)stxGV.routeHistory.delete(stxGV.routeHistory.keys().next().value);
  stxGV.routesWarm=true;return [...routes.values()];
}
function stxGVBattleBudget(power,selected,zoom=state.camera.zoom){
  const desired=power<60?18:power<250?44:power<900?78:100;
  return Math.max(6,Math.floor(desired*(selected?1:stxGV.quality)*(zoom<.42?.25:zoom<.85?.65:1)));
}
function stxGVSpend(kind,n=1){
  if(stxGV.budget[kind]<n)return false;
  stxGV.budget[kind]-=n;stxGV.stats[kind]=(stxGV.stats[kind]||0)+n;return true;
}
