import test from 'node:test';
import assert from 'node:assert/strict';
import {LANDS} from './lands.mjs';
import {HABITATS,HABITAT_SIZE_METERS,habitatAt,habitatAtPosition,visibleHabitatPatches,habitatMapLayer,habitatMapLegend} from './habitats.mjs';
import {terrainMap} from './terrain.mjs';

const stateAt=(landId,position)=>({world:{activeLand:landId,regions:{[landId]:{position}}},run:{vector:{east:9999,north:9999},distance:0}});
const cross=(a,b,p)=>(b.east-a.east)*(p.north-a.north)-(b.north-a.north)*(p.east-a.east);
const inside=(p,polygon)=>polygon.every((a,i)=>cross(a,polygon[(i+1)%polygon.length],p)>=-1e-5);

test('neighborhood habitat identity persists through reloads, encounter vectors and small walking segments',()=>{
 for(const land of LANDS){
  for(const location of [{east:0,north:0},{east:-985.5,north:1547.25},{east:1e7,north:-1e7}]){
   const state=stateAt(land.id,location),before=JSON.stringify(state),here=habitatAt(state);
   assert.equal(JSON.stringify(state),before,'habitat lookup does not modify a saved game');
   assert.deepEqual(here,habitatAt(JSON.parse(before)));
   state.run.distance=1e8;state.run.vector={east:0,north:0};state.run.phase='battle';
   assert.deepEqual(here,habitatAt(state),'encounter and expedition progress cannot move a world habitat');
   assert.equal(here.landId,land.id);assert.equal(here.sizeMeters,480);
  }
  const center=habitatAtPosition(land.id,0,0).center;
  for(const delta of [{east:120,north:0},{east:-120,north:0},{east:0,north:120},{east:0,north:-120}])assert.equal(habitatAtPosition(land.id,center.east+delta.east,center.north+delta.north).patchId,habitatAtPosition(land.id,0,0).patchId,'a habitat contains a substantial walking area');
 }
});

test('shared irregular borders form continuous neighborhoods across positive and negative world coordinates',()=>{
 for(const land of LANDS){
  const rows=visibleHabitatPatches(land.id,{position:{east:0,north:0},width:2000,height:2000,scale:1});
  const byCell=new Map(rows.map(row=>[`${row.cell.east}:${row.cell.north}`,row]));
  for(const row of rows){
   const east=byCell.get(`${row.cell.east+1}:${row.cell.north}`),north=byCell.get(`${row.cell.east}:${row.cell.north+1}`);
   if(east){assert.deepEqual(row.polygon[1],east.polygon[0]);assert.deepEqual(row.polygon[2],east.polygon[3]);}
   if(north){assert.deepEqual(row.polygon[3],north.polygon[0]);assert.deepEqual(row.polygon[2],north.polygon[1]);}
  }
  for(let east=-900;east<=900;east+=61)for(let north=-900;north<=900;north+=67){const here=habitatAtPosition(land.id,east,north);assert.ok(inside({east,north},here.polygon),'lookup uses the region drawn by its polygon');}
 }
});

test('a walk stays in broad habitats instead of changing every few map cells',()=>{
 const runs=[];
 for(let east=-1400;east<=1400;east+=10){
  const here=habitatAtPosition('emerald-jungle',east,0),last=runs.at(-1);
  if(last?.id===here.patchId)last.distance+=10;else runs.push({id:here.patchId,distance:10});
 }
 assert.ok(runs.length>=5&&runs.length<=8);
 for(const run of runs.slice(1,-1))assert.ok(run.distance>=380&&run.distance<=580,`${run.id} covers ${run.distance} m`);
 assert.equal(HABITAT_SIZE_METERS,480);
});

test('nearby neighborhoods offer distinct real resource profiles in every land',()=>{
 for(const land of LANDS){
  const found=new Map();
  for(let x=-2;x<=2;x++)for(let y=-2;y<=2;y++){const row=habitatAtPosition(land.id,x*480,y*480);found.set(row.id,row);}
  assert.equal(found.size,6,'all habitat types are reachable in the local neighborhood');
  assert.ok(found.get('grove').resourceWeights.fruit>found.get('cave').resourceWeights.fruit);
  assert.ok(found.get('cave').resourceWeights.ingot>found.get('grove').resourceWeights.ingot);
  assert.ok(found.get('cave').resourceWeights.earth>found.get('grove').resourceWeights.earth);
  assert.ok(found.get('riverbank').resourceWeights.water>found.get('cave').resourceWeights.water);
  assert.ok(found.get('ruins').resourceWeights.charmMaterial>found.get('grove').resourceWeights.charmMaterial);
  for(const row of found.values()){assert.ok(row.description.length>20);assert.ok(row.element);assert.ok(Object.values(row.resourceWeights).every(n=>Number.isFinite(n)&&n>0));}
 }
});

test('terrain displays the same habitat and resources used by discovery lookup at every camera',()=>{
 for(const [east,north]of [[0,0],[480,0],[-480,-480],[239.9,239.9],[-240.1,-240.1]]){
  const position={east,north},here=habitatAtPosition('emerald-jungle',east,north),svg=terrainMap('emerald-jungle',{position});
  assert.match(svg,new RegExp(`data-current-patch="${here.patchId}"`));
  assert.match(svg,new RegExp(`data-current-habitat="${here.id}"`));
  assert.ok(svg.includes(here.resources));assert.ok(svg.includes(`${here.name} · 480 m habitat`));
  assert.match(svg,/class="terrain-habitats"/);assert.match(svg,/data-habitat-size="480"/);
  assert.match(svg,/class="habitat-ground-mark"/);assert.match(svg,/stroke-dasharray="8 8"/);
 }
 const origin=habitatAtPosition('emerald-jungle',0,0),cave=habitatAtPosition('emerald-jungle',480,0);
 assert.equal(origin.id,'grove');assert.equal(cave.id,'cave');
 const one=visibleHabitatPatches('emerald-jungle',{position:origin.center}),zoomed=visibleHabitatPatches('emerald-jungle',{position:origin.center,zoom:.65});
 assert.deepEqual(one.find(p=>p.patchId===origin.patchId),zoomed.find(p=>p.patchId===origin.patchId),'zoom changes visibility rather than world geometry');
});

test('invalid coordinates and imported region names cannot generate invalid or unbounded maps',()=>{
 const here=habitatAtPosition('<script>',NaN,Infinity);assert.equal(here.landId,'emerald-jungle');assert.equal(here.id,'grove');
 assert.deepEqual(habitatAt(null),habitatAtPosition('emerald-jungle',0,0));
 for(const zoom of [.65,1,2,NaN,Infinity]){
  const rows=visibleHabitatPatches('<script>',{position:{east:Infinity,north:NaN},zoom});assert.ok(rows.length>0&&rows.length<16);
  const svg=habitatMapLayer('<script>',{position:{east:NaN,north:Infinity},zoom:Number.isFinite(zoom)?zoom:1})+habitatMapLegend('<script>');
  assert.doesNotMatch(svg,/NaN|Infinity|<script>/);assert.ok(svg.length<100000);
 }
 assert.equal(Object.isFrozen(HABITATS.cave.resourceWeights),true);
});

test('diagonal accepted walking awards the same habitat pickups in one sample or many, including ribbon expiry',async()=>{
 const C=await import('./core.mjs'),single=C.newSave('Habitat route samples','capuchin','practice',13579);
 single.world.regions[single.world.activeLand].position={east:1140,north:-930};
 single.run.nextEncounter=single.run.nextCamp=1e9;single.run.discoveryProgress=.37;single.run.bonuses.ribbon=200;
 const split=structuredClone(single),events=[],smallEvents=[],distance=1400;
 assert.equal(C.advance(single,distance,-distance*.8,distance*.6,event=>events.push(event)),distance);
 for(let accepted=0;accepted<distance;){const meters=Math.min(17,distance-accepted);assert.equal(C.advance(split,meters,-meters*.8,meters*.6,event=>smallEvents.push(event)),meters);accepted+=meters;}
 assert.deepEqual(events,smallEvents,'sample sizes cannot change either habitat or generated resources along the same accepted route');
 assert.ok(new Set(events.map(row=>row.habitat)).size>=3,'the route crosses multiple broad habitats');
 assert.equal(single.rng,split.rng);assert.equal(single.run.discoveries,split.run.discoveries);assert.equal(single.run.fruit,split.run.fruit);
 assert.deepEqual(single.run.essence,split.run.essence);assert.deepEqual(single.run.items,split.run.items);
 assert.equal(habitatAt(single).patchId,habitatAt(split).patchId);assert.equal(single.run.bonuses.ribbon,0);assert.equal(split.run.bonuses.ribbon,0);
});
