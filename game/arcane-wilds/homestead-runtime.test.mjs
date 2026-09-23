import test from 'node:test';
import assert from 'node:assert/strict';
import {runtime} from './runtime-test-helper.mjs';
function start(touch=false){const h=runtime(touch);h.run('startNewGame();game.player.invuln=1000;');h.step(2);return h;}
function clearWaves(h){h.run(`for(let g=0;g<8&&!game.roomData.cleared;g++){game.enemies=[];if(game.roomData.worldEvent)Object.assign(game.roomData.worldEvent,{time:20,wave:3,pending:0});markRoomCleared();if(!game.roomData.cleared){intensityTickEncounter(3);intensityTickEncounter(1.1);}}`);}
async function home(h){h.run(`const danger=Object.values(AWCampaignData.nodes).find(n=>n.continent==='verdant'&&n.type==='danger');AWCampaign.enter(danger.id);`);clearWaves(h);assert.equal(h.run('AWHome.state().deed'),true);h.run(`AWCampaign.enter(AWHome.HOME);`);assert.equal(await h.run(`AWHome.action('buildHouse')`),true);}
function point(h,target,type,id,x=95,y=50){const e=new h.w.Event(type,{bubbles:true,cancelable:true});Object.defineProperties(e,{pointerId:{value:id},pointerType:{value:'touch'},isPrimary:{value:id===1},clientX:{value:x},clientY:{value:y},button:{value:0}});target.dispatchEvent(e);return e;}
for(const touch of [false,true])test(`${touch?'touch':'desktop'} home can be built, entered, furnished, gardened, rendered, saved and reopened`,async()=>{
 const h=start(touch);try{await home(h);
  assert.equal(h.run('game.enemies.length'),0);assert.equal(h.run('game.roomData.cleared'),true);
  h.run('AWHome.enterHouse()');assert.equal(h.run('AWHome.inside()'),true);
  const id=h.run(`AWHome.state().items.find(i=>i.kind==='bed').id`);
  assert.equal(await h.run(`AWHome.action('place',{id:${JSON.stringify(id)},x:6,y:4})`),true);
  h.run('game.player.hp=1');await h.run(`AWHome.action('rest',{id:${JSON.stringify(id)}})`);assert.equal(h.run('game.player.hp===game.player.maxHp'),true);assert.equal(h.run('AWHome.state().rested'),3);
  h.run('AWHome.leaveHouse()');assert.equal(h.run('AWHome.inside()'),false);
  await h.run(`AWHome.action('bed',{x:11,y:2})`);const plot=h.run('AWHome.state().plots[0].id');
  assert.equal(await h.run(`AWHome.action('plant',{id:${JSON.stringify(plot)},crop:'lanternberry'})`),true);
  assert.equal(await h.run(`AWHome.action('water',{id:${JSON.stringify(plot)}})`),true);
  const planted=h.run('AWHome.state().plots[0].plant.plantedAt'),journey=h.run('AWHome.state().journeyId');h.run('AWHomeUI.open("garden")');h.step(4);assert.match(h.w.document.getElementById('awHomePanel').textContent,/Lanternberries/);
  h.run('AWHomeUI.close();saveGame();loadGame();beginWorld()');assert.equal(h.run('AWHome.state().journeyId'),journey);assert.equal(h.run('AWHome.state().plots[0].plant.plantedAt'),planted);assert.equal(h.run('AWHome.state().items.filter(i=>!i.packed).length'),1);
  h.step(4);assert.deepEqual(h.errors,[]);
 }finally{h.close();}
});
test('new journey does not inherit a prior home or cloud save binding',async()=>{
 const h=start();try{await home(h);const id=h.run('AWHome.state().journeyId');h.run(`awCloudBind('previous','test-password');startNewGame()`);assert.notEqual(h.run('AWHome.state().journeyId'),id);assert.equal(h.run('AWHome.state().tier'),0);assert.equal(h.run('awCloudBoundName'),'');assert.equal(h.run('AWHome.state().deed'),false);
 }finally{h.close();}
});
test('interior exit cannot route onto a continent road and solid furniture stops dodges',async()=>{
 const h=start();try{await home(h);h.run('AWHome.enterHouse();transitionRoom(1,0,"E")');assert.equal(h.run('game.campaign.room'),1);assert.equal(h.run('doorOpen("E")'),false);
  const id=h.run(`AWHome.state().items.find(i=>i.kind==='bed').id`);await h.run(`AWHome.action('place',{id:${JSON.stringify(id)},x:6,y:4})`);
  h.run(`game.player.x=5.9;game.player.y=4.5;game.player.dodgeTime=.4;game.player.dodgeDir={x:1,y:0};playerMovement(.25)`);
  assert.ok(h.run('game.player.x')<6);h.run('AWHome.leaveHouse()');assert.equal(h.run('game.campaign.room'),0);
 }finally{h.close();}
});
test('physical placement has explicit confirm and cancel, and costs nothing on cancel',async()=>{
 const h=start(true);try{await home(h);const id=h.run(`AWHome.state().items.find(i=>i.kind==='hearth').id`),gold=h.run('game.gold');
  h.run(`AWHomeUI.startPlacement('item',${JSON.stringify(id)})`);assert.ok(h.w.document.body.classList.contains('aw-home-building'));assert.equal(h.run('modalPause'),true);assert.ok(h.w.document.querySelector('[data-confirm]'));h.step(2);assert.deepEqual(h.errors,[]);
  h.run('AWHomeUI.cancelPlacement()');assert.equal(h.run('game.gold'),gold);assert.equal(h.run(`AWHome.state().items.find(i=>i.id===${JSON.stringify(id)}).packed`),true);
 }finally{h.close();}
});
test('recall rejects combat and pending waves, then returns to the cleared source room',async()=>{
 const h=start();try{await home(h);h.run(`AWCampaign.enter('verdant-road');game.roomData.cleared=false;intensityState().encounter={roomKey:game.roomData.key,wave:1,totalWaves:2,pending:{time:1},grace:0};AWHome.startRecall();`);
  assert.equal(h.run('AWHome.safe()'),false);h.step(5);assert.equal(h.run('AWHome.atHome()'),false);
  h.run(`game.enemies=[];game.roomData.cleared=true;intensityState().encounter=null;game.player.x=9;game.player.y=7;AWHome.startRecall();update(2.6);`);
  assert.equal(h.run('AWHome.atHome()'),true);assert.equal(h.run('AWHome.state().returnPoint.node'),'verdant-road');h.run('AWHome.returnAdventure()');assert.equal(h.run('game.campaign.current'),'verdant-road');assert.equal(h.run('game.roomData.cleared'),true);
 }finally{h.close();}
});
for(const ci of ['verdant','meridian','gloam'])test(`${ci} has guaranteed wave sites and room rewards wait for the final group`,()=>{
 const h=start();try{
  const count=h.run(`AWCampaignData.continent('${ci}').nodes.map(id=>AWCampaignData.nodes[id]).filter(n=>n.multiWave).length`);assert.ok(count>=4);
  h.run(`game.campaign.unlocked=['verdant','meridian','gloam'];AWCampaign.enter(Object.values(AWCampaignData.nodes).find(n=>n.continent==='${ci}'&&n.type==='danger').id);`);
  const waves=h.run('intensityState().encounter.totalWaves');assert.ok(waves>=2&&waves<=4);const gold=h.run('game.gold');h.run('game.enemies=[];markRoomCleared()');assert.equal(h.run('game.roomData.cleared'),false);assert.equal(h.run('game.gold'),gold);assert.equal(h.run('AWHome.safe()'),false);
  clearWaves(h);assert.equal(h.run('game.roomData.cleared'),true);const clearedGold=h.run('game.gold');h.run('markRoomCleared()');assert.equal(h.run('game.gold'),clearedGold);
 }finally{h.close();}
});
test('cleared-wave checkpoints survive save/load and do not skip an active first wave',()=>{
 const h=start();try{h.run(`AWCampaign.enter(Object.values(AWCampaignData.nodes).find(n=>n.continent==='verdant'&&n.type==='danger').id);game.enemies=[];markRoomCleared();saveGame();`);const key=h.run('game.roomData.key');assert.equal(h.run(`game.campaign.waveProgress[${JSON.stringify(key)}]`),1);
  h.run('loadGame();beginWorld()');assert.equal(h.run('intensityState().encounter.wave'),2);assert.equal(h.run('game.roomData.cleared'),false);assert.ok(h.run('game.enemies.length')>0);clearWaves(h);assert.equal(h.run('game.roomData.cleared'),true);
 }finally{h.close();}
});
test('safe home rooms never create an encounter even after high-threat combat',async()=>{
 const h=start();try{await home(h);h.run(`game.campaign.unlocked.push('gloam');AWCampaign.enter('gloam-road');AWCampaign.enter(AWHome.HOME)`);assert.equal(h.run('intensityState().encounter'),null);assert.equal(h.run('game.enemies.length'),0);h.run('AWHome.enterHouse()');assert.equal(h.run('game.enemies.length'),0);assert.equal(h.run('intensityState().encounter'),null);
 }finally{h.close();}
});
test('three independent fingers preserve movement while casting and dodging in either release order',()=>{
 const h=start(true);try{
  for(const id of ['moveZone','aimZone']){const e=h.w.document.getElementById(id);e.getBoundingClientRect=()=>({left:0,top:0,width:100,height:100});e.setPointerCapture=()=>{};}
  const move=h.w.document.getElementById('moveZone'),aim=h.w.document.getElementById('aimZone'),dodge=h.w.document.getElementById('mobileDodge');h.run(`var testCasts=0,testDodges=0;castSpell=()=>{testCasts++};dodge=()=>{testDodges++};game.player.dodgeCd=0;game.player.spellState[game.player.activeSpells[0]]={cd:0};renderSpellBar();`);
  const spell=h.w.document.querySelector('#spells .spell-slot');
  point(h,move,'pointerdown',1,95,50);point(h,aim,'pointerdown',2,80,30);point(h,dodge,'pointerdown',3);
  assert.equal(h.run('testDodges'),1);assert.equal(h.run('moveStick.pointer'),1);assert.equal(h.run('aimStick.pointer'),2);assert.ok(h.run('AWInput.move.x')>.5);
  point(h,dodge,'pointerup',3);assert.equal(h.run('moveStick.active'),true);
  point(h,spell,'pointerdown',4);assert.equal(h.run('testCasts'),1);h.run('renderSpellBar()');assert.equal(h.w.document.querySelector('#spells .spell-slot'),spell);
  point(h,move,'pointerup',1);assert.equal(h.run('aimStick.active'),true);assert.ok(spell.classList.contains('aw-input-held'));point(h,spell,'pointerup',4);
  const click=new h.w.MouseEvent('click',{bubbles:true,detail:1});spell.dispatchEvent(click);assert.equal(h.run('testCasts'),1);
  point(h,move,'pointerdown',5);point(h,spell,'pointerdown',6);point(h,spell,'pointercancel',6);assert.equal(h.run('moveStick.pointer'),5);
  h.w.dispatchEvent(new h.w.Event('orientationchange'));assert.equal(h.run('moveStick.pointer'),null);assert.equal(h.run('aimStick.pointer'),null);assert.equal(h.run('AWInput.move.x'),0);
 }finally{h.close();}
});
test('keyboard/assistive activation still works and native compatibility clicks cannot double-fire',()=>{
 const h=start(true);try{h.run(`var activations=0;dodge=()=>{activations++};game.player.dodgeCd=0;`);const b=h.w.document.getElementById('mobileDodge');b.click();assert.equal(h.run('activations'),1);point(h,b,'pointerdown',2);assert.equal(h.run('activations'),2);b.dispatchEvent(new h.w.MouseEvent('click',{bubbles:true,detail:1}));assert.equal(h.run('activations'),2);
 }finally{h.close();}
});
for(const event of ['waves','defend'])test(`${event} objectives keep exactly one three-wave controller`,()=>{
 const h=start();try{h.run(`const n=Object.values(AWCampaignData.nodes).find(n=>n.type==='event'&&n.event==='${event}');game.campaign.unlocked=['verdant','meridian','gloam'];AWCampaign.enter(n.id);`);
 assert.equal(h.run('intensityState().encounter.totalWaves'),1);
 for(let wave=1;wave<3;wave++){h.run('game.enemies=[];markRoomCleared()');assert.equal(h.run('game.roomData.cleared'),false);h.run('update(1.1)');assert.equal(h.run('game.roomData.worldEvent.wave'),wave+1);assert.ok(h.run('game.enemies.length')>0);}
 h.run('game.enemies=[];markRoomCleared()');assert.equal(h.run('game.roomData.cleared'),true);assert.equal(h.run('AWHome.state().deed'),true);
 }finally{h.close();}
});
