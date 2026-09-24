import test from 'node:test';
import assert from 'node:assert/strict';
import { GEOGRAPHY_PATHS, geographyLayers } from '../geography-assets.mjs';
import { buildGeography, waterMask, maskEdges, canonicalMask, rotateMask, HEX_DIRECTIONS, neighborId, oppositeEdge, GeographyCache, riverKind } from '../geography.mjs';
import { edgePort, shoreGeometry, coastDrawing, riverDrawing } from '../geography-art.mjs';
import { createGame, parseSave } from '../core.mjs';
const tile=(q,r,terrain='plains',river=false)=>({id:`${q},${r}`,q,r,terrain,river});
const board=(radius=3)=>Object.fromEntries(Array.from({length:radius*2+1},(_,r)=>Array.from({length:radius*2+1},(_,q)=>tile(q-radius,r-radius))).flat().map(t=>[t.id,t]));
const pixel=t=>[25*Math.sqrt(3)*(t.q+t.r/2),37.5*t.r];
const world=(p,t)=>p.map((v,i)=>v+pixel(t)[i]);
const close=(a,b)=>Math.hypot(a[0]-b[0],a[1]-b[1])<1e-8;
function reciprocal(tiles,graph) {
  for(const [id,g] of graph) for(const edge of maskEdges(g.riverMask)) {
    const n=tiles[neighborId(tiles[id],edge)];
    if(g.mouthMask&(1<<edge)) assert.ok(!n||n.terrain==='water');
    else {
      assert.ok(n,`${id} river must have a receiver`);
      assert.ok(graph.get(n.id).riverMask&(1<<oppositeEdge(edge)),`${id} -> ${n.id} must be reciprocal`);
      assert.ok(close(world(edgePort(edge),tiles[id]),world(edgePort(oppositeEdge(edge)),n)));
    }
  }
}
test('all 64 masks, rotations and river edge combinations are exact',()=>{
  for(let mask=0;mask<64;mask++) {
    const normalized=canonicalMask(mask);
    assert.equal(rotateMask(normalized.mask,normalized.rotation),mask);
    const tiles=board();tiles['0,0'].river=true;
    for(const edge of maskEdges(mask)) tiles[neighborId(tiles['0,0'],edge)].river=true;
    const graph=buildGeography(tiles);
    assert.equal(graph.get('0,0').riverMask,mask);
    reciprocal(tiles,graph);
    for(const p of [...riverDrawing(mask),...coastDrawing(mask)]) assert.doesNotMatch(p.d,/NaN|undefined|Infinity/);
  }
  assert.equal(riverKind(9),'straight');assert.equal(riverKind(3),'bend');assert.equal(riverKind(5),'bend');
  assert.equal(riverKind(21),'fork');assert.equal(riverKind(0),'source');assert.equal(riverKind(1),'source');assert.equal(riverKind(9,1),'mouth');
});
test('all coastline masks face water and share exact endpoints with adjacent coasts',()=>{
  for(let mask=0;mask<64;mask++) {
    const tiles=board(),center=tiles['0,0'];
    for(const edge of maskEdges(mask)) tiles[neighborId(center,edge)].terrain='water';
    assert.equal(waterMask(tiles,center),mask);
    for(const shore of shoreGeometry(mask)) for(const p of [shore.start,shore.end].filter(Boolean)) {
      const found=HEX_DIRECTIONS.some((_,edge)=>{
        const n=tiles[neighborId(center,edge)];if(n.terrain==='water') return false;
        return shoreGeometry(waterMask(tiles,n)).some(g=>[g.start,g.end].filter(Boolean).some(other=>close(world(p,center),world(other,n))));
      });
      assert.ok(found,`shore mask ${mask} has an unpaired endpoint`);
    }
  }
});
test('legacy campaigns get an outlet, explicit source, and unchanged gameplay data',()=>{
  for(const preset of ['crossroads','highlands']) for(const seed of [1,42,8147]) {
    const state=createGame(seed,preset),before=JSON.stringify(state),graph=buildGeography(state.tiles);
    assert.equal(JSON.stringify(state),before);
    assert.ok([...graph.values()].some(g=>g.mouthMask),'river reaches sea');
    assert.ok([...graph.values()].some(g=>g.hasRiver&&g.kind==='source'),'upstream spring');
    reciprocal(state.tiles,graph);
    assert.deepEqual(buildGeography(parseSave(before).tiles),graph);
  }
});
test('all six mouth orientations, islands, bays and outer board edges',()=>{
  for(let edge=0;edge<6;edge++) {
    const tiles=board(),t=tiles['0,0'];t.river=true;
    tiles[neighborId(t,oppositeEdge(edge))].river=true;
    tiles[neighborId(t,edge)].terrain='water';
    const graph=buildGeography(tiles);assert.equal(graph.get(t.id).mouthMask,1<<edge);reciprocal(tiles,graph);
  }
  const island={'0,0':tile(0,0)};assert.equal(buildGeography(island).get('0,0').coastMask,63);
  assert.equal(shoreGeometry(63).length,1);
  assert.equal(shoreGeometry(21).length,3,'three distinct bays retain dry land connections');
  assert.equal(shoreGeometry(31).length,1,'five wet sides form a connected peninsula');
});
test('cache invalidates on in-place geography edits and coast bases follow mainland',()=>{
  const tiles=board(),cache=new GeographyCache(),first=cache.get(tiles);
  assert.equal(cache.get(tiles),first);
  tiles['0,0'].terrain='coast';tiles['1,0'].terrain='forest';tiles['0,1'].terrain='forest';tiles['-1,1'].terrain='forest';
  assert.notEqual(cache.get(tiles),first);assert.equal(cache.get(tiles).get('0,0').ground,'forest');
  const next=cache.get(tiles);tiles['0,0'].river=true;assert.notEqual(cache.get(tiles),next);
  assert.ok(cache.get(tiles).get('0,0').hasRiver);
});
test('illustrated PNG selection preserves masks and uses native fallback for missing shapes',()=>{
  assert.equal(Object.keys(GEOGRAPHY_PATHS).length,47);
  const shapes={coast_single:1,coast_corner:3,coast_bay:7,coast_point:31,river_source:1,river_bend:5,river_straight:9,river_fork:21};
  for(let mask=0;mask<64;mask++) for(const type of ['coast','river']) {
    const g={coastMask:type==='coast'?mask:0,riverMask:mask,mouthMask:0,hasRiver:type==='river'};
    const layers=geographyLayers(g);
    const supported=Object.entries(shapes).some(([name,m])=>name.startsWith(type)&&canonicalMask(mask).mask===m);
    assert.equal(layers.length,supported?1:0);
    for(const l of layers) {
      assert.ok(GEOGRAPHY_PATHS[l.name]?.startsWith('geography-v2/'));
      const [,shape,degrees]=l.name.match(/^(.*)_(\d{3})$/);
      assert.equal(rotateMask(shapes[shape],Number(degrees)/60),mask);
      assert.equal(l.rotation,0,'do not rotate a baked rotation twice');
    }
  }
  for(let edge=0;edge<6;edge++) {
    const g={coastMask:0,riverMask:(1<<edge)|(1<<((edge+3)%6)),mouthMask:1<<edge,hasRiver:true};
    assert.deepEqual(geographyLayers(g),[{name:`river_mouth_${String(edge*60).padStart(3,'0')}`,rotation:0}]);
  }
  assert.deepEqual(geographyLayers({coastMask:1,riverMask:21,mouthMask:1,hasRiver:true}),[],'branched mouths use connected native art');
});
