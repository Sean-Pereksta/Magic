'use strict';
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),vm=require('node:vm');
const context=vm.createContext({});context.window=context;
vm.runInContext(fs.readFileSync(path.join(__dirname,'../attack-command-input.js'),'utf8'),context);
function input(){const calls=[];return{calls,input:new context.ATSAttackCommandInput((kind,aim)=>calls.push({kind,aim:aim===undefined?undefined:JSON.parse(JSON.stringify(aim))}))}}
const aim={x:1,y:0};

test('single E tap waits for the double-tap window and commits once',()=>{
 const {input:i,calls}=input();assert.equal(i.press(10,aim),true);assert.equal(i.release(50),true);
 i.tick(289.99);assert.deepEqual(calls,[]);i.tick(290);assert.deepEqual(calls,[{kind:'nearestTarget',aim}]);
 i.tick(1000);assert.equal(i.release(1000),false);assert.equal(calls.length,1);
});
test('quick double E tap emits only one fan attack using the second aim',()=>{
 const {input:i,calls}=input(),second={x:0,y:-1};i.press(0,aim);i.release(20);i.press(140,second);i.release(170);
 assert.deepEqual(calls,[{kind:'spreadCharge',aim:second}]);i.tick(1000);assert.equal(calls.length,1);
});
test('second press reserves the tap beyond its pending deadline until release',()=>{
 const {input:i,calls}=input();i.press(0,aim);i.release(10);i.press(249.99,aim);
 i.tick(250);i.tick(400);assert.deepEqual(calls,[]);i.release(529.98);
 assert.deepEqual(calls,[{kind:'spreadCharge',aim}]);
});
test('E hold fires at the exact threshold once and release cannot emit a tap',()=>{
 const {input:i,calls}=input();i.press(25,aim);i.tick(304.99);assert.deepEqual(calls,[]);
 i.tick(305);i.tick(900);i.release(1000);i.tick(2000);
 assert.deepEqual(calls,[{kind:'nearestHuman',aim}]);
});
test('release after the hold threshold works without an intervening animation frame',()=>{
 for(const elapsed of [280,281,1000]){const {input:i,calls}=input();i.press(0,aim);i.release(elapsed);i.tick(2000);assert.deepEqual(calls,[{kind:'nearestHuman',aim}])}
 const {input:i,calls}=input();i.press(0,aim);i.release(279.99);i.tick(519.99);assert.deepEqual(calls,[{kind:'nearestTarget',aim}]);
});
test('holding the second E press suppresses both the tap and fan attack',()=>{
 for(const frame of [false,true]){
  const {input:i,calls}=input(),second={x:-1,y:0};i.press(0,aim);i.release(20);i.press(100,second);
  i.tick(260);assert.deepEqual(calls,[]);if(frame)i.tick(380);i.release(380);i.tick(1000);
  assert.deepEqual(calls,[{kind:'nearestHuman',aim:second}]);
 }
});
test('second press at exactly the double-tap deadline starts a new tap regardless of frame order',()=>{
 for(const frame of [false,true]){
  const {input:i,calls}=input(),second={x:0,y:1};i.press(0,aim);i.release(20);if(frame)i.tick(260);
  i.press(260,second);assert.deepEqual(calls,[{kind:'nearestTarget',aim}]);i.release(280);i.tick(520);
  assert.deepEqual(calls,[{kind:'nearestTarget',aim},{kind:'nearestTarget',aim:second}]);
 }
});
test('a second press after the deadline flushes the first tap even before the next frame',()=>{
 const {input:i,calls}=input();i.press(0,aim);i.release(20);i.press(261,aim);
 assert.deepEqual(calls,[{kind:'nearestTarget',aim}]);i.release(300);i.tick(540);assert.equal(calls.length,2);
 assert.equal(calls[1].kind,'nearestTarget');
});
test('duplicate and repeated presses cannot reset a hold or fabricate a double tap',()=>{
 const {input:i,calls}=input();i.press(0,aim);assert.equal(i.press(50,{x:0,y:1}),false);assert.equal(i.press(200,aim,true),false);
 i.tick(280);i.release(300);assert.deepEqual(calls,[{kind:'nearestHuman',aim}]);
 i.press(400,aim);i.release(420);assert.equal(i.press(450,aim,true),false);i.tick(660);
 assert.deepEqual(calls.map(c=>c.kind),['nearestHuman','nearestTarget']);
});
test('three quick taps form one double tap followed by one independent single',()=>{
 const {input:i,calls}=input();i.press(0,aim);i.release(20);i.press(60,aim);i.release(80);i.press(100,aim);i.release(120);
 assert.deepEqual(calls.map(c=>c.kind),['spreadCharge']);i.tick(360);
 assert.deepEqual(calls.map(c=>c.kind),['spreadCharge','nearestTarget']);
});
test('four quick taps form two fan attacks without a leftover single',()=>{
 const {input:i,calls}=input();for(const now of [0,60,120,180]){i.press(now,aim);i.release(now+20)}i.tick(1000);
 assert.deepEqual(calls.map(c=>c.kind),['spreadCharge','spreadCharge']);
});
test('cancel on pause, blur, or visibility change discards all pending gesture states',()=>{
 for(const stage of ['pressed','tapped','secondPressed','held']){
  const {input:i,calls}=input();i.press(0,aim);
  if(stage!=='pressed')i.release(20);
  if(stage==='secondPressed'||stage==='held')i.press(80,aim);
  if(stage==='held')i.tick(360);
  const before=calls.length;i.cancel();assert.equal(i.release(400),false);i.tick(2000);assert.equal(calls.length,before,stage);
  i.press(2100,aim);i.release(2120);i.tick(2360);assert.equal(calls.at(-1).kind,'nearestTarget',stage+' starts fresh after cancellation');
 }
});
test('delayed tap and holds preserve the initial plain aim payload after mutations',()=>{
 for(const gesture of ['tap','hold','double','secondHold']){
  const {input:i,calls}=input(),first={x:1,y:0,extra:{waypoints:[{x:2,y:3}]}};
  i.press(0,first);first.x=9;first.extra.waypoints[0].x=9;
  if(gesture==='hold'){i.tick(280)}
  else{
   i.release(20);
   if(gesture==='tap')i.tick(260);
   else{const second={x:0,y:-1,extra:{waypoints:[{x:4,y:5}]}};i.press(80,second);second.y=9;second.extra.waypoints.push({x:99,y:99});if(gesture==='double')i.release(100);else i.tick(360)}
  }
  assert.deepEqual(calls[0].aim,gesture==='tap'||gesture==='hold'?{x:1,y:0,extra:{waypoints:[{x:2,y:3}]}}:{x:0,y:-1,extra:{waypoints:[{x:4,y:5}]}});
 }
});
test('invalid timestamps and stray releases do not arm a gesture or commit commands',()=>{
 const {input:i,calls}=input();assert.equal(i.press(NaN,aim),false);assert.equal(i.release(0),false);i.tick(Infinity);assert.deepEqual(calls,[]);
 i.press(0,aim);assert.equal(i.release(NaN),false);i.tick(NaN);assert.deepEqual(calls,[]);i.release(20);i.tick(260);assert.equal(calls.length,1);
});
test('gesture thresholds are available to help labels and integration',()=>{
 assert.equal(context.ATSAttackCommandInput.holdMs,280);assert.equal(context.ATSAttackCommandInput.doubleTapMs,240);
 assert.equal(context.ATSAttackCommands.holdMs,280);assert.equal(context.ATSAttackCommands.doubleTapMs,240);
});
