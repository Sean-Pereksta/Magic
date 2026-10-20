import { VIEW } from "../config.js";
import { normalizeBiome } from "./regions.js";

const FLOOR=VIEW.floorY, WIDTH=3200;
export const EXPLORATION_BOUNDS={top:-700,bottom:1000};
export const CHAMBER_STYLES=["vertical","elevator","shaft","mixed","guardian","cave","suspended","multiple-route"];
const APPEARANCES=["slab","boulder","iron","rubble","fortress","cave","seal","temple"];
const NAMES=["Olive Stair Reliquary","Counterweight Sanctuary","Watchman's Shaft","Broken Bell Climb","Upper Guardian Court","Buried Spring Passage","Suspended Stone Shrine","Twin Paths Sanctuary"];

function start(style,index) {
  const r={id:`exploration-${style}`,name:NAMES[index],width:WIDTH,difficulty:1,exploration:true,chamberStyle:style,
    tags:["exploration","locked-gate",style,"authored"],authoredHazards:true,checkpointX:100,finalArenaSafe:false,
    platforms:[],movers:[],hazards:[],enemySpawns:[],pickupSpawns:[],props:[],switches:[],explorationRoute:[]};
  r.appearance=APPEARANCES[index];return r;
}
function shelf(r,x,offset,w=210,extra={}) {
  r.platforms.push({x,y:FLOOR+offset,w,h:28,kind:"platform",oneWay:true,route:true,exploration:true,...extra});
  return {surface:"platforms",index:r.platforms.length-1};
}
function ground(r,x,w,offset=0,extra={}) {return shelf(r,x,offset,w,{h:1000-FLOOR-offset,kind:"ground",oneWay:false,...extra});}
function lift(r,x,offset,w,settings={}) {
  const p={x,y:FLOOR+offset,baseX:x,baseY:FLOOR+offset,w,h:28,kind:"moving",oneWay:true,exploration:true,
    axis:"y",range:200,period:7.4,phase:Math.PI/2,motion:"lift",recoverable:true,...settings};
  p.speed=Math.PI*2/p.period;r.movers.push(p);r.tags.push("movers");return {surface:"movers",index:r.movers.length-1};
}
function path(r,...points) {r.explorationRoute=points;return r;}
function object(r,point,kind="key",offsetX=80) {
  const p=r[point.surface][point.index],local=r.switches.length;
  r.switches.push({exploration:true,kind,x:p.x+offsetX,y:p.y-28,w:38,h:28,activated:false,surface:point});
  if(kind==="key"||kind==="relic")r.pickupSpawns.push({x:p.x+offsetX,y:p.y-34,w:28,h:28,type:kind==="key"?"gateKey":"gateRelic",unlockLocal:local,exploration:true});
  point.unlockLocals=[...(point.unlockLocals||[]),local];return local;
}
function finish(r,entry,exit,key,route,alternate=null) {
  const deck=r[key.surface][key.index];
  // Arrival and collection occupy the first half of this broad fight platform.
  // Guardians patrol the other half, leaving a clear mandatory landing zone.
  r.guardianSurface=key;r.enemySpawns.push({x:deck.x+deck.w*.65,y:deck.y-76,h:76,elite:true,guardian:true,hp:5,
    guardBounds:{left:deck.x+deck.w*.5,right:deck.x+deck.w-30},safeLanding:true});
  r.props.push({kind:"chamberBackdrop",topAligned:true,x:650,y:r.chamberStyle==="cave"?FLOOR+20:-300,w:1670,
    h:r.chamberStyle==="cave"?360:780,chamberStyle:r.chamberStyle,atmosphere:true});
  r.props.push({kind:"explorationClue",x:2310,y:FLOOR,w:100,h:130,chamberStyle:r.chamberStyle,
    targetX:deck.x+100,targetY:deck.y-50,routeSign:r.chamberStyle==="cave"?"Passage Sealed — Find the Relic Below":"Passage Sealed — Find the Relic Above"});
  r.platforms.push({x:2490,y:-700,w:520,h:100,kind:"ceiling",oneWay:false,exploration:true});
  r.platforms.push({x:2600,y:-700,w:72,h:1700,kind:"gate",oneWay:false,mandatory:true,exploration:true,
    frameTop:-700,frameBottom:1000,appearance:r.appearance,unlockLocals:r.switches.map((_,i)=>i),open:false});
  r.gateLocal=r.platforms.length-1;
  r.pickupSpawns.push({x:210,y:FLOOR-28,type:"bread"});
  const last=route.at(-1),lastPad=r[last.surface][last.index];
  const landingX=lastPad.x<1300?Math.max(100,lastPad.x-160):Math.min(2420,lastPad.x+lastPad.w+60);
  path(r,...route,{...entry,x:2390,landingX,returnToGate:true},{...exit,x:2830,exit:true});
  if(alternate)r.alternateExplorationRoute=[...alternate,{...entry,x:2390,landingX,returnToGate:true},{...exit,x:2830,exit:true}];
  return r;
}

function vertical() {
  const r=start("vertical",0),a=ground(r,0,2600,0,{entry:true}),z=ground(r,2600,600,0,{exit:true});
  const b=shelf(r,760,-105),c=shelf(r,1030,-210,180),d=shelf(r,1280,-315,170),key=shelf(r,1530,-420,520,{arena:true,safe:true});
  object(r,key,"key");return finish(r,a,z,key,[{...a,x:650},b,c,d,key,d,c,b]);
}
function elevator() {
  const r=start("elevator",1),a=ground(r,0,2600,0,{entry:true}),z=ground(r,2600,600,0,{exit:true});
  const first=lift(r,810,-170,220,{range:170,period:7.8}),rest=shelf(r,1100,-320,230,{safe:true});
  const second=lift(r,1410,-430,210,{range:110,period:6.8,phase:-Math.PI/2,motion:"alternating"});
  const key=shelf(r,1700,-530,520,{arena:true,safe:true});object(r,key,"relic");
  return finish(r,a,z,key,[{...a,x:710},first,rest,second,key,second,rest,first]);
}
function shaft() {
  const r=start("shaft",2),a=ground(r,0,2600,0,{entry:true}),z=ground(r,2600,600,0,{exit:true});
  r.platforms.push({x:850,y:-300,w:40,h:620,kind:"wall",oneWay:false,shaft:true});
  r.platforms.push({x:1070,y:-300,w:40,h:620,kind:"wall",oneWay:false,shaft:true});
  const rest=shelf(r,890,-220,92,{safe:true}),upper=shelf(r,978,-440,92,{safe:true});
  const high=shelf(r,890,-660,92,{safe:true});
  const key=shelf(r,820,-860,470,{safe:true,arena:true});object(r,key,"switch",80);
  finish(r,a,z,key,[{...a,x:960},{...key,wallShaft:{left:890,right:1070},unlockLocals:[0]},
    {...high,drop:true},{...upper,drop:true},{...rest,drop:true},{...a,x:960,drop:true}]);
  r.shaftRestingSurfaces=[rest,upper];return r;
}
function mixed() {
  const r=start("mixed",3),a=ground(r,0,2600,0,{entry:true}),z=ground(r,2600,600,0,{exit:true});
  const b=shelf(r,740,-100),c=shelf(r,1010,-205,190,{kind:"collapsing",temporary:true}),rest=shelf(r,1220,-205,140,{safe:true});
  const m=lift(r,1480,-300,210,{range:85,period:6.4});
  const key=shelf(r,1780,-395,500,{arena:true,safe:true});object(r,key,"pressure");
  const returning=shelf(r,2310,-235,200,{safe:true});return finish(r,a,z,key,[{...a,x:650},b,c,rest,m,key,returning]);
}
function guardian() {
  const r=start("guardian",4),a=ground(r,0,2600,0,{entry:true}),z=ground(r,2600,600,0,{exit:true});
  const b=shelf(r,700,-110),c=shelf(r,970,-220),key=shelf(r,1260,-330,760,{arena:true,safe:true});
  object(r,key,"key");object(r,key,"switch",205);
  return finish(r,a,z,key,[{...a,x:610},b,c,{...key,x:deckX(r,key,205)},c,b]);
}
function deckX(r,p,offset) {return r[p.surface][p.index].x+offset;}
function cave() {
  const r=start("cave",5),a=ground(r,0,720,0,{entry:true}),z=ground(r,2380,820,0,{exit:true});
  const b=shelf(r,790,95,220),c=shelf(r,1080,195,220),key=ground(r,1370,510,280,{arena:true,safe:true});
  const up=shelf(r,1920,175,190),last=shelf(r,2150,75,200);object(r,key,"relic");
  const result=finish(r,a,z,key,[a,b,c,key,up,last]);
  // Return to the surface on the near side of the gate, after the cave climb.
  result.explorationRoute[result.explorationRoute.length-2]={...z,x:2420,landingX:2430,returnToGate:true};
  return result;
}
function suspended() {
  const r=start("suspended",6),a=ground(r,0,650,0,{entry:true}),z=ground(r,2440,760,0,{exit:true});
  const dock=shelf(r,760,-110,220,{safe:true}),ferry=lift(r,1260,-220,230,{axis:"x",range:260,period:8.2,motion:"ferry",phase:-Math.PI/2});
  const key=shelf(r,1790,-220,450,{arena:true,safe:true}),last=shelf(r,2310,-110,180,{safe:true});object(r,key,"relic");
  const result=finish(r,a,z,key,[a,dock,ferry,key,last]);result.explorationRoute[result.explorationRoute.length-2]={...z,x:2500,landingX:2500,returnToGate:true};return result;
}
function multiple() {
  const r=start("multiple-route",7),a=ground(r,0,2600,0,{entry:true}),z=ground(r,2600,600,0,{exit:true});
  const b=shelf(r,740,-100,210),c=shelf(r,1000,-200,210),d=shelf(r,1260,-300,210),key=shelf(r,1530,-400,650,{arena:true,safe:true});
  const m=lift(r,510,-230,210,{range:170,period:8}),upper=shelf(r,840,-400,210),bridge=shelf(r,1160,-400,210);object(r,key,"key");
  return finish(r,a,z,key,[{...a,x:650},b,c,d,key,bridge,upper,m],[{...a,x:400},m,upper,bridge,key,bridge,upper,m]);
}

export const EXPLORATION_ROOM_LIBRARY=[vertical(),elevator(),shaft(),mixed(),guardian(),cave(),suspended(),multiple()];
const REGION_ORDER={countryside:[0,1,7,5,4,3,6,2],desert:[5,0,6,1,7,2,3,4],city:[2,4,7,1,0,3,6,5],mountains:[1,2,6,3,0,7,5,4],ruins:[4,5,7,0,2,3,1,6],storm:[6,3,1,2,7,0,4,5]};
export function explorationRoomsFor(biome) {return REGION_ORDER[normalizeBiome(biome)].map(index=>EXPLORATION_ROOM_LIBRARY[index]);}
