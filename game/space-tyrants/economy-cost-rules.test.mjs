import assert from 'node:assert/strict';
import test from 'node:test';
import {createSimulation} from './simulation-harness.mjs';

const json=(h,expr)=>JSON.parse(h.run(`JSON.stringify(${expr})`));
const near=(a,b)=>assert.ok(Math.abs(a-b)<1e-8,`${a} != ${b}`);
function lab(){
  const h=createSimulation();
  h.run(`state.p=playerWorlds()[0];state.p.underAttack=false;state.p.infra.factory=2;state.p.infra.shipyard=1;state.p.stock.trained=.05;state.p.reserveVessels=60;state.p.stxLocalProjects=[];state.p.localProject=null;state.p.physicalProjects=[];state.p.orders=[];state.ships=[];state.fleets=[];empire(0).credits=500;for(const r of RESOURCES.concat(['components','equipment']))state.p.stock[r]=1000;stxLPEnsure(state.p);`);
  return h;
}

test('factory and shipyard recipes contain raw resources only',()=>{
  const h=lab();
  for(const type of ['factory','shipyard']){
    const recipe=json(h,`STX_PS_RECIPES.${type}`);
    assert.equal('components' in recipe,false,type);
    assert.equal('equipment' in recipe,false,type);
    assert.ok(Object.keys(recipe).length>=3,type);
    assert.ok(Math.abs(Object.values(recipe).reduce((n,v)=>n+v,0)-1)<1e-5,type);
  }
  for(const type of ['factory','shipyard']){
    const need=json(h,`stxOLBuildCost('${type}',state.p,{})`);
    assert.equal('components' in need,false,type);
    assert.equal('equipment' in need,false,type);
    assert.ok(Object.keys(need).every(r=>RESOURCES.includes(r)),type);
  }
});

test('factory and shipyard projects can start with zero credits and charge raw materials only',()=>{
  const h=lab();
  h.run('empire(0).credits=0');
  assert.equal(h.run(`startLocalProject(state.p,'factory','test')`),true);
  near(h.run('empire(0).credits'),0);
  const localNeed=json(h,`stxSDDescriptors(state.p).find(d=>d.q.type==='factory').need`);
  assert.equal('components' in localNeed,false);assert.equal('equipment' in localNeed,false);
  assert.equal(h.run(`stxOLQueueProject(state.p,'shipyard')`),true);
  near(h.run('empire(0).credits'),0);
  const physical=json(h,`state.p.physicalProjects.find(q=>q.kind==='shipyard')`);
  assert.equal(physical.creditCost,0);assert.equal('components' in physical.need,false);assert.equal('equipment' in physical.need,false);
});

test('commissioning spends credits while leaving material stocks untouched',()=>{
  const h=lab();
  h.run(`state.before={credits:empire(0).credits,stock:{...state.p.stock},ships:state.p.reserveVessels,crew:state.p.stock.trained};state.f=stxLPMobilize(state.p,15,'fleet','Cost Test')`);
  assert.ok(h.run('state.f'));
  near(h.run('empire(0).credits'),h.run('state.before.credits-30'));
  near(h.run('state.p.reserveVessels'),h.run('state.before.ships-15'));
  near(h.run('state.p.stock.trained'),h.run('state.before.crew-.003'));
  for(const r of json(h,'RESOURCES.concat(["components","equipment"])'))near(h.run(`state.p.stock['${r}']`),h.run(`state.before.stock['${r}']`));
  assert.equal(h.run('state.f.commissionCredits'),30);
});

test('individual ship construction spends raw resources only',()=>{
  const h=lab();
  h.run(`state.before={credits:empire(0).credits,components:state.p.stock.components,equipment:state.p.stock.equipment,iron:state.p.stock.iron,titanium:state.p.stock.titanium,helium:state.p.stock.helium,rare:state.p.stock.rare,reserve:state.p.reserveVessels}`);
  assert.equal(h.run('stxECRBuildShip(state.p,1)'),true);
  near(h.run('empire(0).credits'),h.run('state.before.credits'));
  near(h.run('state.p.stock.components'),h.run('state.before.components'));near(h.run('state.p.stock.equipment'),h.run('state.before.equipment'));
  near(h.run('state.p.stock.iron'),h.run('state.before.iron-4'));near(h.run('state.p.stock.titanium'),h.run('state.before.titanium-3'));near(h.run('state.p.stock.helium'),h.run('state.before.helium-2'));near(h.run('state.p.stock.rare'),h.run('state.before.rare-1'));
  near(h.run('state.p.reserveVessels'),h.run('state.before.reserve+1'));
});

test('personnel transit reaches distant owned planets for credits and never moves ships or materials',()=>{
  const h=lab();
  h.run(`state.dest=state.planets.find(p=>p.owner===null);state.dest.owner=0;state.dest.x=state.p.x+3000;state.dest.y=state.p.y;state.dest.stock.trained=0;stxLPEnsure(state.dest);state.dest.reserveVessels=7;state.before={credits:empire(0).credits,ships:state.p.reserveVessels,crew:state.p.stock.trained,components:state.p.stock.components,equipment:state.p.stock.equipment};`);
  assert.ok(h.run('stxLPNearby(state.dest).some(p=>p.id===state.p.id)'));
  assert.equal(h.run('stxLPTransfer(state.p,state.dest,15)'),true);
  near(h.run('state.p.reserveVessels'),h.run('state.before.ships'));
  near(h.run('state.p.stock.trained'),h.run('state.before.crew-.003'));
  near(h.run('state.p.stock.components'),h.run('state.before.components'));near(h.run('state.p.stock.equipment'),h.run('state.before.equipment'));
  near(h.run('empire(0).credits'),h.run('state.before.credits-4'));
  assert.equal(h.run('state.ships[0].cargo.militaryVessels||0'),0);near(h.run('state.ships[0].cargo.trained'),.003);
});

test('inspector exposes the new cost model',()=>{
  const h=lab(),html=h.run('stxLPPanel(state.p)');
  assert.match(html,/Fleet & Shipyard Orders/);assert.match(html,/Build 1 reserve ship/);assert.match(html,/PERSONNEL TRANSIT · OWNED PLANETS/);assert.match(html,/factory & shipyard: raw resources only, no credit fee/);assert.doesNotMatch(html,/Rush 10 vessels/);
});
