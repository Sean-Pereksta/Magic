// Large tabletop battlefields share their actual collision geometry, routes and
// resource roles with simulation, AI, lobby previews and cached world artwork.
import {enrichTerrain,terrainHeight} from './terrain.js';
export const FRONTIER_WIDTH=9600,FRONTIER_HEIGHT=6400;
export const FRONTIER_MAPS={
 inkdelta:{id:'inkdelta',name:'Inkspill Delta',color:'#c9d5d1',tagline:'Three causeways across a flooded desk',brief:'Control three broad crossings, scout the ink banks, and protect exposed scrap islands.',features:['Three causeways','Ink rivers','Rich outer banks']},
 corkcross:{id:'corkcross',name:'Corkboard Crossroads',color:'#d6bf92',tagline:'A four-ramp crown with open flanking routes',brief:'Hold the central cork crown or bypass it along the north and south pinboard routes.',features:['Four-ramp center','Outer flanks','Elevated scrap']},
 serverstack:{id:'serverstack',name:'Server Stack',color:'#b8cfca',tagline:'Circuit corridors below an open air route',brief:'Split armies between rack corridors, defend service junctions, or fly over the server walls.',features:['Dual ground routes','Rack corridors','Open air lanes']}
};
const legacyBriefs={desktop:{tagline:'Wide paper roads and raised desk-mat lookouts',brief:'Choose the central road, high-ground shelves or the exposed outer paper lanes.'},breakroom:{tagline:'Service routes around cups and napkin terraces',brief:'Contest the tile junctions while protecting side routes around the breakroom shelves.'},canyon:{tagline:'Three strategic crossings over a deep desk divide',brief:'Use the broad bridges, or break the central Stapler Gate to open a direct route.'},conveyor:{tagline:'A giant recycling line with reversible transport routes',brief:'Capture belt consoles, exploit conveyor speed and flank through the outer factory loop.'}};
const clamp=(v,a,b)=>Math.max(a,Math.min(b,v)),distance=(a,b)=>Math.hypot(a.x-b.x,a.y-b.y);
const circleHits=(p,o,r)=>{const x=clamp(p.x,o.x,o.x+o.w),y=clamp(p.y,o.y,o.y+o.h);return (p.x-x)**2+(p.y-y)**2<r*r;};
export function frontierClear(map,p,r=40){return p.x>=r+25&&p.y>=r+25&&p.x<=map.width-r-25&&p.y<=map.height-r-25&&!(map.obstacles||[]).some(o=>circleHits(p,o,r));}
const clearLine=(map,a,b,r)=>{const n=Math.ceil(distance(a,b)/45);for(let i=0;i<=n;i++)if(!frontierClear(map,{x:a.x+(b.x-a.x)*i/n,y:a.y+(b.y-a.y)*i/n},r))return false;return true;};
const route=(name,role,points,width=460,risk='contested')=>({name,role,points,width,risk});
function camps(players){return players===4?[{x:2300,y:700},{x:7300,y:700},{x:7300,y:5700},{x:2300,y:5700}]:players===3?[{x:2050,y:5463},{x:7550,y:5463},{x:4800,y:700}]:[{x:1050,y:3200},{x:8550,y:3200}];}
function template(id,players){return {...FRONTIER_MAPS[id],width:FRONTIER_WIDTH,height:FRONTIER_HEIGHT,players,camps:camps(players),obstacles:[],lanes:[],laneInfo:[],expansions:[],terrainLayouts:[],conveyors:[],stations:[],bridges:[],canyons:[],frontier:true};}
function addRoute(map,definition){map.lanes.push(definition.points);map.laneInfo.push({...definition,id:`${map.id}-lane-${map.lanes.length}`,points:definition.points});}
function addExpansion(map,x,y,label,role='outer',risk='exposed',richness=1.7){map.expansions.push({x,y,label,role,risk,richness,amountMultiplier:richness});}
function appendApproaches(map,hubs){
 for(let team=0;team<map.camps.length;team++){
  const camp=map.camps[team],sorted=hubs.slice().sort((a,b)=>distance(a,camp)-distance(b,camp));
  for(const [index,hub]of sorted.slice(0,2).entries())addRoute(map,route(`Camp ${team+1} ${index?'flank approach':'main approach'}`,index?'flank':'main',[[camp.x,camp.y],[hub.x,hub.y]],420,index?'exposed':'safe'));
 }
}
function inkdelta(players){
 const map=template('inkdelta',players);
 for(const [y,h]of [[0,400],[1800,950],[3650,900],[6100,300]])map.obstacles.push({x:4380,y,w:840,h,type:'inkpool',terrain:true});
 const crossings=[{y:1100,h:1400,name:'Northern blotter causeway'},{y:3200,h:900,name:'Center card causeway'},{y:5325,h:1550,name:'Southern cork causeway'}];
 for(const crossing of crossings){map.bridges.push({x:4140,y:crossing.y-crossing.h/2,w:1320,h:crossing.h,type:'blotterbridge',label:crossing.name});addRoute(map,route(crossing.name,crossing.y===3200?'main':'crossing',[[1650,crossing.y],[3550,crossing.y],[4800,crossing.y],[6050,crossing.y],[7950,crossing.y]],Math.min(620,crossing.h-180),crossing.y===3200?'contested':'exposed'));}
 for(const x of [3400,6200])addRoute(map,route(x<4800?'West bank flank':'East bank flank','flank',[[x,900],[x,3200],[x,5500]],400,'exposed'));
 map.terrainLayouts=[[2760,2060,1000,600,'West ink-bank shelf',{opening:420,depth:340}],[5840,2060,1000,600,'East ink-bank shelf',{opening:420,depth:340}],[2600,3910,1060,560,'West delta terrace',{opening:400,depth:340}],[5940,3910,1060,560,'East delta terrace',{opening:400,depth:340}]];
 for(const [x,y,w,h,type]of [[1600,1800,560,340,'book'],[7440,1800,560,340,'book'],[1550,4220,600,320,'pinbox'],[7450,4220,600,320,'pinbox'],[700,700,480,280,'tape'],[8420,5420,480,280,'tape']])map.obstacles.push({x,y,w,h,type});
 for(const [x,y,label]of [[3650,3200,'West causeway scrap'],[5950,3200,'East causeway scrap'],[4850,3200,'Center causeway cache']])addExpansion(map,x,y,label,'center','contested',1.35);
 for(const [x,y,label]of [[800,1200,'Northwest ink-bank salvage'],[8800,1200,'Northeast ink-bank salvage'],[800,5200,'Southwest ink-bank salvage'],[8800,5200,'Southeast ink-bank salvage']])addExpansion(map,x,y,label);
 appendApproaches(map,[{x:3400,y:1100},{x:3400,y:3200},{x:3400,y:5325},{x:6200,y:1100},{x:6200,y:3200},{x:6200,y:5325}]);
 return map;
}
function corkcross(players){
 const map=template('corkcross',players);
 map.terrainLayouts=[[3900,2550,1800,1300,'The cork crown',{sides:['west','east','north','south'],opening:600,depth:440}],[1500,2380,1100,640,'West pin terrace',{opening:440,depth:360}],[7000,2380,1100,640,'East pin terrace',{opening:440,depth:360}],[3700,1080,900,560,'North memo shelf',{opening:400,depth:340}],[5000,4800,900,560,'South memo shelf',{opening:400,depth:340}]];
 for(const [x,y,w,h,type]of [[3150,2180,510,460,'corkbundle'],[5940,3760,510,460,'corkbundle'],[3150,3800,510,440,'pinbox'],[5940,2180,510,440,'pinbox'],[1000,3600,600,340,'book'],[8000,3600,600,340,'book'],[680,1400,560,280,'ruler'],[8360,1400,560,280,'ruler']])map.obstacles.push({x,y,w,h,type});
 addRoute(map,route('Crown east-west ramp road','main',[[1100,3200],[2900,3200],[4800,3200],[6700,3200],[8500,3200]],540));
 addRoute(map,route('Crown north-south ramp road','main',[[4800,700],[4800,2000],[4800,3200],[4800,4400],[4800,5700]],540));
 addRoute(map,route('Northern pinboard flank','flank',[[1700,1900],[3500,1900],[6200,1900],[7900,1900]],460,'exposed'));
 addRoute(map,route('Southern pinboard flank','flank',[[1700,4550],[3500,4550],[6200,4550],[7900,4550]],460,'exposed'));
 addRoute(map,route('Western perimeter route','flank',[[1000,1100],[1000,3200],[1000,5400]],420,'exposed'));
 addRoute(map,route('Eastern perimeter route','flank',[[8600,1100],[8600,3200],[8600,5400]],420,'exposed'));
 addExpansion(map,4800,3200,'Crown cache','center','contested',1.5);
 for(const [x,y,label]of [[850,850,'Northwest pin salvage'],[8750,850,'Northeast pin salvage'],[850,5550,'Southwest pin salvage'],[8750,5550,'Southeast pin salvage']])addExpansion(map,x,y,label);
 appendApproaches(map,[{x:2850,y:1900},{x:6750,y:1900},{x:2850,y:4550},{x:6750,y:4550}]);
 return map;
}
function serverstack(players){
 const map=template('serverstack',players);
 for(const x of [3100,5700])for(const [y,h]of [[0,1750],[2450,1500],[4650,1750]])map.obstacles.push({x,y,w:800,h,type:'serverrack',terrain:true});
 // North/south service breaks and three vertical corridors have generous
 // formation clearance; aircraft can fly directly above the solid rack banks.
 addRoute(map,route('Northern service crossing','crossing',[[1100,2100],[2550,2100],[4800,2100],[7050,2100],[8500,2100]],520));
 addRoute(map,route('Southern service crossing','crossing',[[1100,4300],[2550,4300],[4800,4300],[7050,4300],[8500,4300]],520));
 for(const [x,name]of [[2300,'West circuit corridor'],[4800,'Central circuit spine'],[7300,'East circuit corridor']])addRoute(map,route(name,x===4800?'main':'flank',[[x,650],[x,2100],[x,4300],[x,5750]],500,x===4800?'contested':'exposed'));
 map.airLanes=[{id:'rack-overflight',name:'Rack overflight route',points:[[1100,3200],[4800,3200],[8500,3200]]}];
 map.terrainLayouts=[[1450,2720,1000,700,'West service gantry',{opening:480,depth:340}],[7150,2720,1000,700,'East service gantry',{opening:480,depth:340}],[4270,1680,1060,560,'North circuit lookout',{opening:400,depth:340}],[4270,4160,1060,560,'South circuit lookout',{opening:400,depth:340}]];
 for(const [x,y,w,h]of [[1160,1300,420,330],[8020,4700,420,330],[1170,4580,420,330],[8010,1300,420,330]])map.obstacles.push({x,y,w,h,type:'machine'});
 for(const [x,y,label]of [[4800,2550,'North server-junction cache'],[4800,3850,'South server-junction cache']])addExpansion(map,x,y,label,'center','contested',1.4);
 for(const [x,y,label]of [[700,700,'Northwest cable salvage'],[8900,700,'Northeast cable salvage'],[700,5700,'Southwest cable salvage'],[8900,5700,'Southeast cable salvage']])addExpansion(map,x,y,label);
 appendApproaches(map,[{x:2300,y:2100},{x:7300,y:2100},{x:2300,y:4300},{x:7300,y:4300},{x:4800,y:2100}]);
 return map;
}

// Legacy terrain arrays deliberately share references (cliffs also belong to
// obstacles, ramps belong to plateaus). Scale each object exactly once.
function scaleGeometry(map,scale){
 const seen=new Set(),scaleObject=o=>{if(!o||seen.has(o))return;seen.add(o);for(const key of ['x','y','w','h','r'])if(Number.isFinite(o[key]))o[key]*=scale;if(o.ramps)o.ramps.forEach(scaleObject);};
 for(const key of ['obstacles','camps','expansions','plateaus','ramps','cliffs','regions','conveyors','stations','bridges','canyons','chasms'])for(const o of map[key]||[])scaleObject(o);
 for(const key of ['gate','gateObstacle'])scaleObject(map[key]);
 map.lanes=(map.lanes||[]).map(lane=>(Array.isArray(lane)?lane:lane.points||[]).map(([x,y])=>[x*scale,y*scale]));
 for(const row of map.terrainLayouts||[])for(let i=0;i<4;i++)row[i]*=scale;
 map.width*=scale;map.height*=scale;
}
function safeNatural(map,camp,team){
 const base=Math.atan2(map.height/2-camp.y,map.width/2-camp.x),desired=620;
 for(const length of [desired,760,900])for(const turn of [0,.3,-.3,.6,-.6,.9,-.9,1.2,-1.2]){
  const p={x:Math.round(camp.x+Math.cos(base+turn)*length),y:Math.round(camp.y+Math.sin(base+turn)*length)};
  if(frontierClear(map,p,225)&&clearLine(map,camp,p,90)&&Math.abs(terrainHeight(map,p.x,p.y)-terrainHeight(map,camp.x,camp.y))<.1)return {...p,natural:true,naturalTeam:team,role:'natural',risk:'safe',richness:1,amountMultiplier:1,label:`Camp ${team+1} natural scrap`};
 }
 throw new Error(`${map.id}/${map.players}: no safe natural expansion for camp ${team+1}`);
}

export function finalizeFrontierMap(map,{scale=1}={}){
 if(map.frontierFinalized)return map;
 if(scale!==1)scaleGeometry(map,scale);
 map.frontier=true;map.frontierFinalized=true;map.width=FRONTIER_WIDTH;map.height=FRONTIER_HEIGHT;
 Object.assign(map,legacyBriefs[map.id]||{});
 map.expansions=(map.expansions||[]).filter(p=>!p.natural);
 for(let team=0;team<map.camps.length;team++)map.expansions.unshift(safeNatural(map,map.camps[team],team));
 if(!FRONTIER_MAPS[map.id])for(const [x,y,label]of [[850,850,'Northwest outer salvage'],[8750,850,'Northeast outer salvage'],[850,5550,'Southwest outer salvage'],[8750,5550,'Southeast outer salvage']])if(frontierClear(map,{x,y},220)&&map.camps.every(c=>distance(c,{x,y})>650)&&map.expansions.every(e=>distance(e,{x,y})>360))addExpansion(map,x,y,label);
 // Expansion pads need a buildable desk center, and their resource clusters
 // need enough clear same-height ground around that center.
 map.expansions=map.expansions.filter(p=>frontierClear(map,p,80));
 for(const [i,p]of map.expansions.entries()){
  const role=p.role||(p.lookout||terrainHeight(map,p.x,p.y)>.5?'highground':distance(p,{x:map.width/2,y:map.height/2})<1900?'center':'outer');
  Object.assign(p,{id:p.id||`${map.id}-expansion-${i+1}`,role,risk:p.risk||(role==='natural'?'safe':role==='outer'?'exposed':'contested'),richness:p.richness||({natural:1,center:1.35,highground:1.25,outer:1.65})[role],label:p.label||({natural:'Natural scrap field',center:'Contested center scrap',highground:'Elevated scrap cache',outer:'Outer salvage field'})[role]});p.amountMultiplier=p.richness;
 }
 const prior=map.laneInfo||[];
 map.laneInfo=map.lanes.map((points,i)=>({id:`${map.id}-lane-${i+1}`,name:i===0?'Main road':i===1?'Northern flank':i===2?'Southern flank':`Lookout route ${i-2}`,role:i===0?'main':i<3?'flank':'highground',width:i===0?500:400,risk:i===0?'contested':'exposed',...(prior[i]||{}),points}));
 map.regions=map.expansions.map((p,i)=>({x:p.x,y:p.y,r:230+i%3*28,type:map.id==='serverstack'?'circuit':map.id==='inkdelta'?'ink':map.id==='corkcross'?'cork':map.id==='breakroom'?'tile':'scrap',label:p.label,role:p.role}));
 map.strategic={area:map.width*map.height,startingCans:6,naturalDistance:620,minimumLaneWidth:400,resourceFields:map.expansions.length,theme:map.id};
 return map;
}

export function createFrontierBattlefield(id,players=2){
 if(!FRONTIER_MAPS[id])return null;
 const map=({inkdelta,corkcross,serverstack})[id](Math.max(2,Math.min(4,players)));
 return finalizeFrontierMap(enrichTerrain(map));
}
