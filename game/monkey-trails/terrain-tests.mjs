import test from 'node:test';
import assert from 'node:assert/strict';
import {LANDS} from './lands.mjs';
import {terrainMap,terrainAt,worldToMap,MAP_METERS_TO_PIXELS} from './terrain.mjs';

const near=(a,b)=>assert.ok(Math.abs(a-b)<1e-8,`${a} should equal ${b}`);
const props=svg=>new Map([...svg.matchAll(/<use href="[^"]+" data-cell="([^"]+)" data-world-east="([^"]+)" data-world-north="([^"]+)" transform="translate\(([-.\d]+) ([-.\d]+)\)/g)].map(m=>[m[1],{east:Number(m[2]),north:Number(m[3]),x:Number(m[4]),y:Number(m[5])}]));

test('walking north, south, east and west moves world features around a centered player',()=>{
  const target={east:40,north:20};
  assert.deepEqual(worldToMap({east:7,north:-11},{east:7,north:-11}),{x:300,y:180});
  const origin=worldToMap(target);
  for(const [position,dx,dy] of [[{east:15,north:0},-33,0],[{east:-15,north:0},33,0],[{east:0,north:15},0,33],[{east:0,north:-15},0,-33]]){
    const moved=worldToMap(target,position);near(moved.x-origin.x,dx);near(moved.y-origin.y,dy);
  }
  const zoomed=worldToMap(target,{east:0,north:0},1.5);near(zoomed.x-300,(origin.x-300)*1.5);near(zoomed.y-180,(origin.y-180)*1.5);
  assert.equal(MAP_METERS_TO_PIXELS,2.2);
});

test('the same landmarks and props persist across positive and negative rendering boundaries',()=>{
  for(const position of [{east:27.9,north:27.9},{east:-28.1,north:-28.1},{east:259.9,north:-420.1}]){
    const moved={east:position.east+.2,north:position.north+.2};
    const before=props(terrainMap('emerald-jungle',{position})),after=props(terrainMap('emerald-jungle',{position:moved}));
    let shared=0;
    for(const [key,p] of before){const q=after.get(key);if(!q)continue;shared++;assert.equal(p.east,q.east);assert.equal(p.north,q.north);assert.ok(Math.abs((q.x-p.x)+.44)<.015);assert.ok(Math.abs((q.y-p.y)-.44)<.015);}
    assert.ok(shared>10,'neighboring cameras retain the same world features');
    assert.equal(terrainMap('emerald-jungle',{position}),terrainMap('emerald-jungle',{position}),'a revisit exactly reproduces the same view');
  }
});

test('all ten regions have distinct maps, clear starting paths and deterministic terrain',()=>{
  const rendered=new Set();
  for(const land of LANDS){
    const svg=terrainMap(land.id),surface=terrainAt(land.id,0,0);
    assert.equal(surface.kind,'trail');assert.equal(surface.land,land.id);
    assert.match(svg,new RegExp(`data-land="${land.id}"`));assert.match(svg,/class="terrain-bridge"/);assert.match(svg,/class="terrain-landmark"/);
    assert.match(svg,/>20 m<\/text>/);assert.match(svg,/class="terrain-camera"/);
    assert.equal(svg,terrainMap(land.id));rendered.add(svg);
    for(const [east,north]of [[-420.001,-260.001],[-28,28],[1e7,-1e7]])assert.deepEqual(terrainAt(land.id,east,north),terrainAt(land.id,east,north));
  }
  assert.equal(rendered.size,10);
});

test('world coordinates stay exact in metadata and invalid inputs remain bounded and safe',()=>{
  const position={east:12.3456789,north:-98.7654321},svg=terrainMap('emerald-jungle',{position,uid:'"><script>alert(1)</script>'});
  assert.match(svg,/data-map-east="12\.3456789"/);assert.match(svg,/data-map-north="-98\.7654321"/);assert.doesNotMatch(svg,/<script>|NaN|Infinity/);
  assert.deepEqual(worldToMap({east:NaN,north:Infinity}),{x:300,y:180});
  for(const zoom of [NaN,Infinity,-100,0,.65,1,2,100]){
    const view=terrainMap('unknown',{position:{east:Infinity,north:NaN},zoom});assert.match(view,/data-land="emerald-jungle"/);assert.doesNotMatch(view,/NaN|Infinity/);
    assert.ok(Number(view.match(/data-prop-count="(\d+)"/)[1])<500);assert.ok(view.length<250000,'nearby rendering stays bounded at every zoom');
  }
});

test('the traveled route includes its return leg when revisiting an earlier point',()=>{
  const svg=terrainMap('emerald-jungle',{position:{east:0,north:0},trail:[{east:0,north:0},{east:15,north:0},{east:15,north:15}]});
  assert.match(svg,/d="M300 180L333 180L333 147L300 180"/);
  assert.equal((svg.match(/terrain-traveled/g)||[]).length,1);
});
