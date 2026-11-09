import test from 'node:test';
import assert from 'node:assert/strict';
import {Game,TYPES,MAPS} from '../engine.js';
import {createFrontierBattlefield,finalizeFrontierMap,FRONTIER_MAPS} from '../frontier-maps.js';
import {paintFrontierMap,drawFrontierObstacle} from '../frontier-art.js';
import {tabletopDetailPlacements} from '../tabletop-details.js';

const ids=['desktop','breakroom','canyon','conveyor',...Object.keys(FRONTIER_MAPS)];
function advancePoint(g,team,r=18){const camp=g.starts[team],dx=g.width/2-camp.x,dy=g.height/2-camp.y,d=Math.hypot(dx,dy);return {x:camp.x+dx/d*310,y:camp.y+dy/d*310,r,type:'rock'};}
function assertRoute(g,start,target,label){
 const path=g._lineFree(start.x,start.y,target.x,target.y,start.r)?[{x:target.x,y:target.y}]:g._path(start,target.x,target.y);
 assert(path.length,`${label}: navigation returns a route`);const end=path.at(-1);assert(Math.hypot(end.x-target.x,end.y-target.y)<1,`${label}: route reaches the actual destination (${end.x},${end.y} vs ${target.x},${target.y})`);
 let previous=start;for(const p of path){assert(g._lineFree(previous.x,previous.y,p.x,p.y,start.r),`${label}: every segment respects solid terrain`);previous=p;}
 return path;
}

test('all seven maps provide four times the former area and preserve the local starting economy',()=>{
 assert.equal(Object.keys(MAPS).length,7);
 for(const map of ids)for(const players of [2,3,4]){
  const g=new Game({map,players});assert.equal(g.width,9600);assert.equal(g.height,6400);assert.equal(g.map.camps.length,players);
  assert(g.map.expansions.length>=10);assert.equal(g.map.laneInfo.length,g.map.lanes.length);
  for(let team=0;team<players;team++){
   const home=g.entities.find(e=>e.type==='desk'&&e.team===team),cans=g.entities.filter(e=>e.homeTeam===team);
   assert.equal(cans.length,6);assert(cans.every(e=>e.amount===1500&&Math.abs(Math.hypot(e.x-home.x,e.y-home.y)-205)<.01),'home harvesting distances and yields retain unit scale');
   assert.equal(g.entities.filter(e=>e.team===team&&e.type==='twig').length,5);
   for(const e of g.entities.filter(e=>e.team===team))assert(!g._blocked(e.x,e.y,e.r,e.id),`${map}/${players}: ${e.type} starts on usable ground`);
   const natural=g.map.expansions.find(e=>e.naturalTeam===team);assert(natural);assert.equal(natural.role,'natural');assert.equal(natural.risk,'safe');assert(Math.hypot(natural.x-home.x,natural.y-home.y)<=901);
  }
  assert(g.map.expansions.some(p=>p.role==='outer'&&p.richness>=1.5));
  assert(g.entities.some(e=>e.type==='trash'&&e.expansionRole==='outer'&&e.amount>=2700),'rich fields offer actual additional harvestable scrap');
  assert(g.entities.filter(e=>e.team>0).every(e=>!g.isVisible(e,0)),'larger maps retain meaningful initial scouting');
  const closest=Math.min(...g.starts.slice(1).map(s=>Math.hypot(s.x-g.starts[0].x,s.y-g.starts[0].y)));
  assert(closest/TYPES.rock.speed>30,'a ground scout needs over thirty seconds of straight travel between starting yards');
  for(const p of g.map.expansions){assert(p.label&&p.id&&p.risk&&p.role);assert.equal(p.amountMultiplier,p.richness);assert(!g._blocked(p.x,p.y,64),`${map}/${players}/${p.label}: expansion has a buildable center`);}
 }
 assert.equal(TYPES.rock.r,18);assert.equal(TYPES.rock.speed,123);assert.equal(TYPES.twig.speed,150);
});

test('an airplane must physically cross the larger rack battlefield before revealing its rival',()=>{
 const g=new Game({map:'serverstack'});g._think=Infinity;g.entities=g.entities.filter(e=>e.type==='desk');
 const start=g.starts[0],rival=g.starts[1],scout=g._spawn('airplane',0,start.x,start.y),enemy=g.entities.find(e=>e.team===1);
 g.command([scout.id],{type:'move',x:rival.x,y:rival.y});
 for(let i=0;i<125;i++)g.update(.08);
 assert(!g.isVisible(enemy,0),'ten seconds of fast air scouting does not prematurely reveal the distant base');
 const flight=Math.ceil(Math.hypot(rival.x-start.x,rival.y-start.y)/TYPES.airplane.speed)+2;
 for(let t=10;t<flight;t+=.08)g.update(.08);
 assert(g.isVisible(enemy,0),'a scout that reaches the rival airspace reveals it over the rack walls');
});

for(const map of ids)test(`${map}: every spawn reaches all resource fields and rival approach lanes`,()=>{
 for(const players of [2,3,4]){
  const g=new Game({map,players});g.entities=[];g._think=Infinity;
  for(let team=0;team<players;team++){
   const start=advancePoint(g,team);assert(!g._blocked(start.x,start.y,start.r),`${map}/${players}/${team}: forward deployment point is clear`);
   for(const target of g.map.expansions)assertRoute(g,start,target,`${map}/${players}/${team}/${target.label}`);
   for(let rival=team+1;rival<players;rival++)assertRoute(g,start,advancePoint(g,rival),`${map}/${players}/${team} to rival ${rival}`);
   const natural=g.map.expansions.find(e=>e.naturalTeam===team);assertRoute(g,{...start,r:40},natural,`${map}/${players}/${team}: heavy unit reaches natural`);
  }
 }
});

test('new maps have actual separate crossings, four-sided crown access and an aerial rack shortcut',()=>{
 const ink=createFrontierBattlefield('inkdelta',2);assert(ink.obstacles.filter(o=>o.type==='inkpool').length>=4);assert.equal(ink.bridges.length,3);assert(ink.laneInfo.filter(l=>l.role==='crossing'||l.role==='main').length>=3);
 const cork=createFrontierBattlefield('corkcross',2),crown=cork.plateaus.find(p=>p.label==='The cork crown');assert.equal(crown.ramps.length,4);assert.deepEqual(new Set(crown.ramps.map(r=>r.side)),new Set(['west','east','north','south']));
 const server=createFrontierBattlefield('serverstack',2);assert.equal(server.obstacles.filter(o=>o.type==='serverrack').length,6);assert.equal(server.airLanes.length,1);assert(server.laneInfo.filter(l=>l.role==='crossing').length>=2);
});

test('legacy finalization scales shared references once and is safe to call again',()=>{
 const cliff={x:200,y:300,w:14,h:90},ramp={x:300,y:300,w:200,h:240},map={id:'desktop',players:2,width:4800,height:3200,camps:[{x:400,y:800},{x:4400,y:2400}],expansions:[],obstacles:[cliff],cliffs:[cliff],ramps:[ramp],plateaus:[{x:400,y:300,w:600,h:320,ramps:[ramp]}],lanes:[[[400,800],[4400,2400]]]};
 finalizeFrontierMap(map,{scale:2});assert.equal(cliff.x,400);assert.equal(ramp.x,600);const once=JSON.stringify(map);finalizeFrontierMap(map,{scale:2});assert.equal(JSON.stringify(map),once);
});

function context(){const calls=[];return new Proxy({calls},{get(o,k){if(k in o)return o[k];return (...args)=>{assert(args.every(a=>typeof a!=='number'||Number.isFinite(a)),`${String(k)} has finite coordinates`);calls.push({method:k,args});};},set(o,k,v){o[k]=v;return true;}});}
test('frontier scenery follows finite real footprints and retains bounded static detail',()=>{
 for(const map of Object.keys(FRONTIER_MAPS)){
  const g=new Game({map}),renderer={game:g},c=context();paintFrontierMap(renderer,c,'base');paintFrontierMap(renderer,c);assert(c.calls.length>150);
  for(const o of g.map.obstacles.filter(o=>['inkpool','serverrack','corkbundle','pinbox'].includes(o.type)))assert(drawFrontierObstacle(renderer,c,o));
  const points=tabletopDetailPlacements(g);assert(points.length>30&&points.length<=480);
 }
});
