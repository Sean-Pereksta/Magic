import test from 'node:test';
import assert from 'node:assert/strict';
import {Game,MAPS,TYPES} from '../engine.js';

const advance=(g,seconds)=>{for(let t=0;t<seconds;t+=.05)g.update(.05);};
function arena(map='desktop',players=2){const g=new Game({map,players});g._think=Infinity;g.entities=g.entities.filter(e=>e.type==='desk');return g;}

test('all six maps have reachable elevated scrap fields and two useful ramps per lookout',()=>{
  for(const map of ['desktop','breakroom'])for(const players of [2,3,4]){
    const g=arena(map,players);
    assert(g.map.plateaus.length>=4);assert.equal(g.map.ramps.length,g.map.plateaus.length*2);assert(g.map.expansions.length>=10);assert(g.map.coverClusters>=8);
    for(const p of g.map.plateaus){
      const center={x:p.x+p.w/2,y:p.y+p.h/2};
      assert.equal(g.elevationAt(center.x,center.y),1);
      const ramp=p.ramps[0],start={x:ramp.x-20,y:ramp.y+ramp.h/2};
      assert.equal(g.elevationAt(start.x,start.y),0);
      const troop=g._spawn('rock',0,start.x,start.y);
      assert(!g._blocked(troop.x,troop.y,troop.r),`${map}/${players}: ramp approach is clear`);
      assert(g._lineFree(start.x,start.y,center.x,center.y,troop.r),`${map}/${players}: broad ramp connects both elevations`);
      g.command([troop.id],{type:'move',...center});advance(g,5);
      assert(Math.hypot(troop.x-center.x,troop.y-center.y)<15,`${map}/${players}: a troop walks up the ramp`);
      assert(g.elevationAt(troop.x,troop.y)>.9);
      troop.hp=0;
      assert(!g.canBuild('cabinet',ramp.x+ramp.w/2,ramp.y+ramp.h/2),'buildings cannot seal a ramp');
      const outside=[{x:center.x,y:p.y-70},{x:center.x,y:p.y+p.h+70}].find(point=>!g._blocked(point.x,point.y,18));
      // Some center shelves meet a book wall; their north/south approaches are
      // solid scenery. The two open ramps above still verify access to them.
      if(!outside){assert(g.map.obstacles.some(o=>!o.terrain&&center.x>o.x&&center.x<o.x+o.w&&Math.abs(o.y+o.h-p.y)<100));continue;}
      assert(!g._lineFree(outside.x,outside.y,center.x,center.y,18),'the cliff blocks a direct shortcut');
      const detour=g._path({...outside,r:18},center.x,center.y);
      assert(detour.length>=2,`${map}/${players}/${p.id}: navigation finds a ramp instead of crossing a cliff`);
      let previous=outside;
      for(const point of detour){assert(g._lineFree(previous.x,previous.y,point.x,point.y,18));previous=point;}
    }
    const original=new Game({map,players});
    assert(original.entities.filter(e=>e.lookout).length>=2,'elevated areas offer contested resources');
    for(const can of original.entities.filter(e=>e.type==='trash'))assert(!original._blocked(can.x,can.y,can.r),'resources have safe ground');
  }
});

test('high ground grants vision over the low ground while its cliff conceals units from below',()=>{
  const g=arena(),p=g.map.plateaus[0],x=p.x+180,y=p.y+p.h/2;
  const high=g._spawn('rock',0,x,y),low=g._spawn('rock',1,p.x-140,y);
  assert(g.isVisible(low,0));assert(g.canTarget(high,low));
  assert(!g.isVisible(high,1));assert(!g.canTarget(low,high));
  low.x=x;low.y=y+45;g._updateVision();
  assert(g.isVisible(high,1),'climbing onto the shelf reveals its defenders');
  assert(g.canTarget(low,high));
});

test('exploration persists after a scout leaves but hidden enemies cannot be targeted',()=>{
  const g=arena(),p=g.map.plateaus[0],x=p.x+p.w/2,y=p.y+p.h/2;
  const scout=g._spawn('rock',0,x,y),enemy=g._spawn('scissors',1,x+60,y);
  assert(g.canSee(0,x,y));assert(g.isExplored(0,x,y));assert(g.isVisible(enemy,0));
  const revision=g.fog.revision;
  scout.x=2250;scout.y=1400;g._updateVision();
  assert(g.fog.revision>revision);assert(!g.canSee(0,x,y));assert(g.isExplored(0,x,y));
  assert(!g.isVisible(enemy,0));assert(!g.canTarget(scout,enemy));
  g.command([scout.id],{type:'attack',targetId:enemy.id});
  assert.equal(scout.order.type,'idle');
  assert(!g.isExplored(0,2250,100),'untouched corners remain unexplored');
});

test('flying CDs scout over cliffs; embarked troops add no independent vision',()=>{
  const g=arena(),p=g.map.plateaus[0],x=p.x+p.w/2,y=p.y+p.h/2;
  const cd=g._spawn('cd',0,p.x-180,y),enemy=g._spawn('scissors',1,x,y),troop=g._spawn('rock',0,cd.x,cd.y);
  assert(g.loadTransport([troop.id],[cd.id]).ok);assert(troop.transportedBy);
  assert(g.isVisible(enemy,0),'airborne scouts can see onto elevated shelves');
  cd.x=2250;cd.y=1400;troop.x=x;troop.y=y;g._updateVision();
  assert(!g.isVisible(enemy,0),'a passenger has no sight separate from its carrier');
  assert(!g.isVisible(troop,0),'passengers are absent from the ground battlefield');
});

test('fog refresh uses cached sight footprints and construction requires current scouting',()=>{
  const g=arena(),worker=g._spawn('twig',0,600,800);g.teams[0].trash=1000;
  assert(g.build([worker.id],'cabinet',750,800).ok);
  assert(!g.build([worker.id],'cabinet',1800,1200).ok);
  const cache=g._sightCache.size;g._updateVision();assert.equal(g._sightCache.size,cache,'stationary sources reuse their terrain rays');
  assert(g.fog.teams[0].visible instanceof Uint8Array);assert.equal(g.fog.teams[0].visible.length,g.fog.cols*g.fog.rows);
});

test('Paper Airplanes scout a dedicated 900-pixel air radius and preserve exploration after leaving',()=>{
  const g=arena(),cd=g._spawn('cd',0,1500,1400),enemy=g._spawn('scissors',1,2300,1400),far=g._spawn('rock',1,2500,1400);
  assert(!g.isVisible(enemy,0),'ordinary transport vision does not reach this distant lane');
  const airplane=g._spawn('airplane',0,1500,1400);assert.equal(TYPES.airplane.vision,900);
  assert(g.isVisible(enemy,0),'the scouting airplane reveals enemies well beyond ordinary troop and CD vision');
  assert(!g.isVisible(far,0),'scouting still has a finite range');
  const cache=g._sightCache.size;g._updateVision();assert.equal(g._sightCache.size,cache);
  airplane.x=3800;airplane.y=2600;g._updateVision();
  assert(!g.isVisible(enemy,0));assert(g.isExplored(0,enemy.x,enemy.y));
  assert(cd.hp>0,'transport rules and normal CD sight remain available');
});
