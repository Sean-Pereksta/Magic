'use strict';
const test=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const path=require('node:path');
const A=require('./formation-audibles-core.js');

const spreadPass={name:'Mesh',routes:['Drag','Post','Corner','Drag'],xs:[-18,-6,6,18],category:'Intermediate'};
const tripsScreen={name:'WR Bubble',family:'Screens',routes:['Go','Bubble','Lead','Lead'],xs:[-18,7,12,18],screen:1};
const bunch={name:'Bunch Flood',family:'Bunch',routes:['Go','Flat','Out','Corner'],xs:[-18,8,10.5,13]};
const stack={name:'Stack Switch',family:'Stack',routes:['Slant','Wheel','Slant','Wheel'],xs:[-12,-12,12,12]};

test('formation metadata distinguishes spread, trips screen, bunch and stack sets',()=>{
  assert.equal(A.playMeta(spreadPass).shape,'Spread');
  assert.equal(A.playMeta(tripsScreen).shape,'Trips');
  assert.equal(A.playMeta(tripsScreen).screen,true);
  assert.equal(A.playMeta(tripsScreen).group,'Screens');
  assert.equal(A.playMeta(bunch).shape,'Bunch');
  assert.equal(A.playMeta(stack).shape,'Stack');
});

test('a pass-to-screen set change creates more defensive surprise than a same-set pass check',()=>{
  const screen=A.defenseResponsePlan({fromPlay:spreadPass,toPlay:tripsScreen,coverageName:'PRESS MAN',seed:91});
  const same=A.defenseResponsePlan({fromPlay:spreadPass,toPlay:{...spreadPass,name:'Levels'},coverageName:'PRESS MAN',seed:91});
  assert.ok(screen.mismatch>same.mismatch);
  assert.ok(screen.surprise>same.surprise);
  assert.ok(screen.averageDelay>same.averageDelay);
  assert.equal(screen.passToScreen,true);
});

test('the defense learns repeated audibles and repeated checks cannot permanently freeze it',()=>{
  const first=A.defenseResponsePlan({fromPlay:spreadPass,toPlay:tripsScreen,coverageName:'OFF MAN',repeatCount:0,audibleCount:1,seed:410});
  const learned=A.defenseResponsePlan({fromPlay:spreadPass,toPlay:tripsScreen,coverageName:'OFF MAN',repeatCount:3,audibleCount:4,seed:410});
  assert.ok(learned.learned>first.learned);
  assert.ok(learned.surprise<first.surprise);
  assert.ok(learned.averageDelay<first.averageDelay);
});

test('defensive adjustment remains bounded and recovers after the snap',()=>{
  const plan=A.defenseResponsePlan({fromPlay:spreadPass,toPlay:tripsScreen,coverageName:'ALL OUT PRESSURE',seed:7});
  assert.equal(plan.defenders.length,6);
  for(const defender of plan.defenders){
    assert.ok(defender.delay>=.1&&defender.delay<=1.45);
    assert.ok(defender.postSnapHold>=.03&&defender.postSnapHold<=.88);
    assert.ok(defender.bustChance<=.31);
    assert.ok(defender.correctionAt>=defender.delay&&defender.correctionAt<=3.15);
    assert.ok(defender.assignmentIndex>=0&&defender.assignmentIndex<6);
    if(defender.role==='safety')assert.equal(defender.assignmentIndex,defender.index);
  }
});

test('physical movement advances at a capped speed without teleporting',()=>{
  const first=A.moveTowards({x:0,y:0,z:0},{x:10,y:0,z:0},2);
  assert.deepEqual({x:first.x,y:first.y,z:first.z,reached:first.reached},{x:2,y:0,z:0,reached:false});
  const finish=A.moveTowards({x:9,y:0,z:0},{x:10,y:0,z:0},2);
  assert.equal(finish.x,10);assert.equal(finish.reached,true);
});

test('recommended audibles prioritize a screen counter when the current call is a pass',()=>{
  const catalog=A.buildCatalog([tripsScreen,bunch,stack]);
  const current=catalog.find(play=>play.name==='Mesh');
  const recommended=A.recommendedPlays(catalog,current);
  assert.equal(recommended[0].screen,true);
  assert.ok(recommended.some(play=>play.shape!=='Spread'));
});

test('the browser entrypoint loads formation audibles before the game loop',()=>{
  const html=fs.readFileSync(path.resolve(__dirname,'../receiver-window-qb.html'),'utf8');
  const audible=html.indexOf('receiver-window-qb/formation-audibles-core.js');
  const runtime=html.indexOf('receiver-window-qb/formation-audibles-runtime.js');
  const game=html.indexOf('receiver-window-qb/game.js');
  assert.ok(audible>=0,'formation audible script is missing');
  assert.ok(runtime>=0,'formation audible runtime is missing');
  assert.ok(game>=0,'game script is missing');
  assert.ok(audible<runtime&&runtime<game,'audible rules and runtime must load before game.js so scene actors can be observed');
});
