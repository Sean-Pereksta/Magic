'use strict';
const assert=require('node:assert/strict'),path=require('node:path');
const {bootUI}=require('./command-ui-test.cjs');
const ui=bootUI(process.argv[2]||path.resolve(__dirname,'..')),D=ui.window.GameData,el=id=>ui.document.getElementById(id);
const config={name:'Rampart command drill',map:'custom',seed:8317,
  customMap:{name:'The garrison court',width:6400,height:4400,biome:'grassland',regions:D.castleWalls(2500,1200,2000,2000,{team:0})},
  armies:[{name:'Garrison',doctrine:'defensive',roster:{archer:8}},{name:'Attackers',doctrine:'defensive',roster:{guardian:4}}],
  formations:[{team:0,type:'archer',count:8,x:3500,y:2200,formation:'line',facing:Math.PI},{team:1,type:'guardian',count:4,x:1200,y:2200,formation:'line',facing:0}]};
ui.storage.set('ironfield.scenarios.v1',JSON.stringify([{format:'ironfield-scenario',version:2,name:config.name,config}]));
el('scenariosButton').click();const list=el('savedScenarios');list.dispatch('click',{target:list.querySelector('[data-load-scenario="0"]')});
const r=ui.engine.regiments.find(r=>r.type==='archer');ui.select(r);
assert.equal(el('rampartControls').hidden,false);
const elevated=ui.engine.units[r.unitIds[0]];el('clearSelection').click();elevated._rampartZ=64;
ui.point(elevated.x,elevated.y-64);assert.equal(el('selectionPanel').hidden,false,'Clicking a visibly elevated soldier did not select it');
elevated._rampartZ=0;
const before=ui.engine.units.map(u=>[u.x,u.y,u._rampartZ]);
const wall=ui.engine.structures.find(s=>s.walkable&&s.type==='wall'&&s.w>s.h&&s.stairs?.length);
assert(wall&&wall.deckHeight>0);
el('manRampartButton').click();
assert.match(el('commandHint').textContent,/friendly wall/);
ui.point(wall.x+wall.w/2,wall.y+10-wall.deckHeight,{button:2});
assert(r._rampartJob);assert.equal(r._rampartJob.structureId,wall.id);
assert.deepEqual(ui.engine.units.map(u=>[u.x,u.y,u._rampartZ]),before,'Assigning a deployment guard order teleported troops onto the wall');
assert.equal(el('formationSelect').disabled,true);assert.equal(el('splitFormation').disabled,true);
el('rampartRanks').value='3';el('rampartRanks').dispatch('change');
el('rampartSpacing').value='compact';el('rampartSpacing').dispatch('change');
assert.equal(r.rampartRanks,3);assert.equal(r.rampartSpacing,'compact');
const preview=ui.engine.siege.previewRampart([r.id],wall.id,{ranks:3,spacing:'compact'});
assert(preview.length>0);assert(preview.every(p=>p.z>0&&Number.isInteger(p.rank)));
assert.deepEqual(ui.engine.units.map(u=>[u.x,u.y,u._rampartZ]),before,'Preview mutated the real formation');
el('scenarioName').value='Saved wall formation';el('saveScenario').click();
const saved=JSON.parse(ui.storage.get('ironfield.scenarios.v1'))[0],order=saved.config.formations.find(f=>f.type==='archer').rampart;
assert.equal(order.structureId,wall.id);assert.equal(order.ranks,3);assert.equal(order.spacing,'compact');
const deck=saved.config.customMap.regions.find(s=>s.walkable&&s.stairs?.length);
assert(deck.deckHeight>0&&deck.rampartSide&&Number.isFinite(deck.courtyardX)&&Number.isFinite(deck.courtyardY));
assert(deck.stairs.every(s=>s.id&&s.width>0&&['x','y'].includes(s.axis)&&Number.isFinite(s.groundX)&&Number.isFinite(s.deckY)));
el('leaveRampartButton').click();
assert(r._rampartJob.leaving);assert(!ui.window.IRONFIELD.config.formations.find(f=>f.type==='archer').rampart);
// A completed courtyard response must restore normal ground controls even
// though its human-readable history label remains visible on the regiment.
r._rampartJob=null;r.rampartStatus='Courtyard reorganized';ui.select(r);
assert.equal(el('formationSelect').disabled,false);assert.equal(el('splitFormation').disabled,false);
assert.equal(el('leaveRampartButton').disabled,true);assert.match(el('rampartStatus').textContent,/Courtyard defense ready/);
ui.storage.set('ironfield.scenarios.v1',JSON.stringify([saved]));el('scenariosButton').click();
const restoredList=el('savedScenarios');restoredList.dispatch('click',{target:restoredList.querySelector('[data-load-scenario="0"]')});
assert.equal(ui.window.IRONFIELD.config.formations.find(f=>f.type==='archer').rampart.structureId,wall.id,'Reload lost the explicitly chosen wall');
ui.engine.start();const restored=ui.engine.regiments.find(r=>r.type==='archer');
assert.equal(restored._rampartJob.structureId,wall.id,'The saved plan resumed on a different wall');
console.log('PASS Wall command, real preview, rank/spacing changes, no deployment teleport, metadata/save roundtrip, courtyard command, and restored ground controls');
const editor=bootUI(process.argv[2]||path.resolve(__dirname,'..')),field=id=>editor.document.getElementById(id);
field('newEncounter').click();field('customWorldSize').value='colossal';field('customWorldSize').dispatch('change');
function paint(type,x,y,x2,y2){
  editor.document.querySelector(`[data-terrain-tool="${type}"]`).click();
  field('battlefield').dispatch('pointerdown',{clientX:x,clientY:y});
  field('battlefield').dispatch('pointermove',{clientX:x2,clientY:y2});
  field('battlefield').dispatch('pointerup',{clientX:x2,clientY:y2});
}
paint('wall',4000,2400,4100,3600);
assert.equal(field('wallAccessOptions').hidden,false);
let custom=editor.engine.structures.find(s=>s.type==='wall');
assert(custom.walkable&&custom.stairs.length===1);assert(custom.stairs[0].groundX>custom.x+custom.w);
assert.equal(custom.rampartSide,'west');
field('wallStairSide').value='negative';field('wallStairSide').dispatch('change');
paint('wall',7000,2400,7100,3600);
custom=editor.engine.structures.find(s=>s.x===7000);
assert(custom.stairs[0].groundX<custom.x);assert.equal(custom.rampartSide,'east');
paint('gate',7000,2800,7100,3000);
const gate=editor.engine.structures.find(s=>s.type==='gate');assert(gate.walkable&&gate.castleGate&&gate.stairs.length===1);
assert(!editor.engine.map.regions.some(s=>s.type==='wall'&&s.x<gate.x+gate.w&&s.x+s.w>gate.x&&s.y<gate.y+gate.h&&s.y+s.h>gate.y),'Gate retained solid wall beneath its opening');
for(const s of editor.engine.structures)for(const stair of s.stairs||[])assert(stair.deckX>=s.x&&stair.deckX<=s.x+s.w&&stair.deckY>=s.y&&stair.deckY<=s.y+s.h,'Carved wall kept a staircase belonging to another section');
const count=editor.engine.structures.length;paint('wall',80,4500,180,5400);
assert.equal(editor.engine.structures.length,count,'The editor accepted a staircase outside the field');
console.log('PASS Custom wall stair sides, walkable gate carving, stair ownership, and field-boundary rejection');

// Contextual right-click must work directly, without arming Man ramparts.
// Use projected tops outside the ground footprint to exercise visible picking.
const direct=bootUI(process.argv[2]||path.resolve(__dirname,'..')),control=id=>direct.document.getElementById(id);
direct.storage.set('ironfield.scenarios.v1',JSON.stringify([{format:'ironfield-scenario',version:2,name:config.name,config}]));
control('scenariosButton').click();const directList=control('savedScenarios');directList.dispatch('click',{target:directList.querySelector('[data-load-scenario="0"]')});
const ranged=direct.engine.regiments.find(r=>r.type==='archer');direct.select(ranged);
const walls=direct.engine.structures.filter(s=>s.team===ranged.team&&s.walkable&&s.type==='wall'&&s.w>s.h&&s.stairs?.length);
assert(walls.length>=2,'The contextual command fixture requires two distinct friendly wall targets');
const bodies=()=>direct.engine.units.map(u=>[u.x,u.y,u._rampartZ]),original=bodies();
function directWallOrder(target,phase,moveArmed=false){
  assert.equal(direct.engine.phase,phase);
  if(moveArmed){direct.key('m');assert.match(control('commandHint').textContent,/Move:/,'The conflict control never armed Move');}
  const x=target.x+target.w/2,y=target.y+8-target.deckHeight;
  assert.equal(direct.engine.siege.structureAt(x,y),null,'The contextual click must select the projected top rather than the ground footprint');
  direct.point(x,y,{button:2});
  assert(ranged._rampartJob,`Right-clicking the visible wall top in ${phase} did not issue a guard order`);
  assert.equal(ranged._rampartJob.structureId,target.id,`The contextual ${phase} command kept its previous wall target`);
  assert.equal(direct.window.IRONFIELD.config.formations.find(f=>f.type==='archer').rampart.structureId,target.id,`The contextual ${phase} command did not preserve the selected wall in the battle plan`);
  assert.equal(direct.engine.phase,phase,'A contextual wall order unexpectedly changed the battle phase');
  assert.deepEqual(bodies(),original,'Issuing a contextual wall order instantly repositioned troops instead of using stairs');
}
directWallOrder(walls[0],'deployment');
control('scenarioName').value='Direct wall guard plan';control('saveScenario').click();
const directSaved=JSON.parse(direct.storage.get('ironfield.scenarios.v1'))[0];
assert.equal(directSaved.config.formations.find(f=>f.type==='archer').rampart.structureId,walls[0].id,'Saving lost the contextual deployment wall target');
direct.key(' ');assert.equal(direct.engine.phase,'running');
directWallOrder(walls[1],'running',true);
direct.key(' ');assert.equal(direct.engine.phase,'paused');
directWallOrder(walls[0],'paused',true);
console.log('PASS Direct ranged right-click on visible wall tops in deployment, running and paused battles, including armed Move, saved guard plans, and no instant movement');
