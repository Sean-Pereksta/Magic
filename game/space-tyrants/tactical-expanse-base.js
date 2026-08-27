/* Space Tyrants — Tactical Expanse layer.
   Injected inside the core IIFE by space-tyrants.html so it can extend the
   existing simulation without duplicating or replacing Living Galaxy state. */

const STX_VERSION = 1;
const STX_MILITARY_TYPES = new Set(["fleet", "patrol"]);
const STX_SCAN_COST = {components: 58, titanium: 22, rare: 18, helium: 10};
const STX_WORLD_TARGET = {w: 11600, h: 7600};
const STX_ACTIVITY_LIMIT = 36;
let stxLastStrategicTick = -999;
let stxPointerDown = null;
let stxShowSensors = true;

WORLD.w = STX_WORLD_TARGET.w;
WORLD.h = STX_WORLD_TARGET.h;
DIRECTIVE_LABEL.mineWorks = "Imperial Mineworks Program";
DIRECTIVE_LABEL.factoryWave = "Factory Construction Wave";
DIRECTIVE_LABEL.sensorArray = "Frontier Sensor Array";
DIRECTIVE_LABEL.invasion = "Targeted Planetary Invasion";

function stxEnsureEmpireState(e){
  if(!e) return;
  e.tech = e.tech || {};
  if(!Number.isFinite(e.tech.sensors)) e.tech.sensors = 0;
  e.invasionPlans = Array.isArray(e.invasionPlans) ? e.invasionPlans : [];
  e.shipActivity = Array.isArray(e.shipActivity) ? e.shipActivity : [];
}
function stxEnsurePlanetState(p){
  if(!p) return;
  p.scanArray = Number.isFinite(p.scanArray) ? p.scanArray : 0;
  p.scanProject = p.scanProject || null;
  p.incomingThreat = p.incomingThreat || null;
}
function stxEnsureState(){
  state.empires.forEach(stxEnsureEmpireState);
  state.planets.forEach(stxEnsurePlanetState);
}
function stxActivity(text, planetId=null, fleetId=null, tone=""){
  const e = empire(0);
  if(!e) return;
  stxEnsureEmpireState(e);
  e.shipActivity.unshift({id:`a${Math.floor(random()*1e9)}`, text, planetId, fleetId, tone, time:state.simTime});
  e.shipActivity = e.shipActivity.slice(0, STX_ACTIVITY_LIMIT);
}
function stxDistanceXY(x,y,p){return Math.hypot(x-p.x,y-p.y)}
function stxScanRange(p){
  const tech = empire(0)?.tech?.sensors || 0;
  return 520 + (p.scanArray||0)*1180 + tech*520 + (p.infra?.research||0)*105 + (p.orbitals?.station||0)*120;
}
function stxPointDetected(x,y){
  return playerWorlds().some(p=>stxDistanceXY(x,y,p) <= Math.max(660,stxScanRange(p)));
}
function stxMilitaryShipVisible(s){
  if(!s || !STX_MILITARY_TYPES.has(s.type)) return true;
  if(s.owner===0) return true;
  if(stxPointDetected(s.x,s.y)) return true;
  const target = state.planets.find(p=>p.id===s.to);
  if(target?.owner===0 && stxDistanceXY(s.x,s.y,target)<980) return true;
  return state.battles.some(b=>b.planetId===s.to && (b.attacker===0||b.defender===0));
}
function stxFleetPosition(f){
  if(!f || f.destroyed) return null;
  const ship = state.ships.find(s=>s.fleetId===f.id);
  if(ship) return {x:ship.x,y:ship.y,ship,planet:null,label:ship.to?state.planets.find(p=>p.id===ship.to)?.name:null};
  const planet = state.planets.find(p=>p.id===f.location);
  if(planet) return {x:planet.x,y:planet.y,ship:null,planet,label:planet.name};
  return null;
}
function stxFleetVisible(f){
  if(!f || f.destroyed) return false;
  if(f.owner===0) return true;
  const pos=stxFleetPosition(f);
  return !!pos && stxPointDetected(pos.x,pos.y);
}
function stxFocusPoint(x,y,zoom=1.18){
  state.camera.x=clamp(x,0,WORLD.w);
  state.camera.y=clamp(y,0,WORLD.h);
  state.camera.zoom=clamp(Math.max(state.camera.zoom,zoom),.22,2.15);
}
function stxFocusFleet(id){
  const f=fleetRecord(id),pos=stxFleetPosition(f);
  if(!f||!pos) return showToast("Fleet position is not currently available");
  if(f.owner!==0&&!stxFleetVisible(f)) return showToast("That fleet is outside current sensor coverage");
  stxFocusPoint(pos.x,pos.y,1.3);
  if(pos.planet){state.selected=pos.planet;renderPlanet();}
  else if(pos.ship?.to){const p=state.planets.find(q=>q.id===pos.ship.to);if(p)state.selected=p;}
  state.stxFocusedFleet=id;
  showToast(`${f.name} · ${f.status}`);
  stxRefreshTacticalRail(true);
}
function stxFocusBattle(id){
  const b=state.battles.find(x=>x.id===id),p=b&&state.planets.find(q=>q.id===b.planetId);
  if(!b||!p) return;
  stxFocusPoint(p.x,p.y,1.35);state.selected=p;renderPlanet();
  showToast(`Battle view: ${p.name}`);
}

function stxAddFrontierWorlds(count=44,legacy=false){
  const desiredMin=legacy?94:106;
  if(state.planets.length>=desiredMin) return;
  const seen=new Set(state.planets.map(p=>p.name));
  let nameCursor=state.planets.length, attempts=0, made=0;
  const ownedOrCore=()=>state.planets.filter(p=>p.owner!==null||!p.scattered);
  while(made<count && attempts<count*80){
    attempts++;
    let x,y;
    if(made < Math.floor(count*.58) && ownedOrCore().length>2){
      const a=pick(ownedOrCore()), b=pick(ownedOrCore().filter(q=>q!==a));
      const t=rand(.24,.76);
      x=a.x+(b.x-a.x)*t+rand(-430,430);
      y=a.y+(b.y-a.y)*t+rand(-360,360);
    }else{
      x=rand(360,WORLD.w-360);y=rand(330,WORLD.h-330);
    }
    x=clamp(x,260,WORLD.w-260);y=clamp(y,250,WORLD.h-250);
    if(state.planets.some(p=>Math.hypot(p.x-x,p.y-y)<285)) continue;
    const base=WORLD_NAMES[nameCursor++%WORLD_NAMES.length];let name=base,n=2;
    while(seen.has(name)) name=`${base} ${roman(n++)}`;
    seen.add(name);
    const p=makePlanet(name,x,y,null,false,state.planets.length);
    p.scattered=true;p.r=rand(10,18);p.intel=.04;stxEnsurePlanetState(p);
    state.planets.push(p);made++;
  }
  recomputeLanes();generateSpaceArt();
}
function stxSpreadNewGalaxy(){
  const oldCx=4100,oldCy=2600,newCx=WORLD.w/2,newCy=WORLD.h/2;
  state.planets.forEach(p=>{
    p.x=clamp((p.x-oldCx)*1.28+newCx,300,WORLD.w-300);
    p.y=clamp((p.y-oldCy)*1.25+newCy,300,WORLD.h-300);
  });
  stxAddFrontierWorlds(48,false);
  const home=playerWorlds().find(p=>p.home)||playerWorlds()[0];
  if(home){state.camera.x=home.x;state.camera.y=home.y;state.camera.zoom=.58;state.selected=home;}
  recomputeLanes();generateSpaceArt();
  logEvent(`Deep-space survey charts now span ${state.planets.length} worlds across a much larger galactic theater.`,"good");
}
const STX_coreGenerateGalaxy=generateGalaxy;
generateGalaxy=function(){
  WORLD.w=STX_WORLD_TARGET.w;WORLD.h=STX_WORLD_TARGET.h;
  STX_coreGenerateGalaxy();stxEnsureState();stxSpreadNewGalaxy();
};
const STX_coreLoadGame=loadGame;
loadGame=function(){
  WORLD.w=STX_WORLD_TARGET.w;WORLD.h=STX_WORLD_TARGET.h;
  const ok=STX_coreLoadGame();
  if(ok){stxEnsureState();stxAddFrontierWorlds(34,true);recomputeLanes();generateSpaceArt();}
  return ok;
};

