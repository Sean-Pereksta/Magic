import test from 'node:test';
import assert from 'node:assert/strict';
import {StepDetector,TrailPedometer,STEP_METERS} from './pedometer.mjs';

const gravity=(value=9.81)=>({accelerationIncludingGravity:{x:0,y:0,z:value}});
const linear=value=>({acceleration:{x:value,y:0,z:0}});
function walk(detector,from,to,period=500){
  const steps=[];for(let t=from;t<=to;t+=20){const sample=detector.sample(gravity(9.81+2*Math.sin(2*Math.PI*t/period)),t);if(sample.step)steps.push({t,...sample});}return steps;
}

test('gravity magnitude rejects stationary noise and tracks periodic walking once per stride',()=>{
  const still=new StepDetector();for(let t=0;t<5000;t+=20)assert.equal(still.sample(gravity(9.81+.12*Math.sin(t)),t).step,false);
  const steps=walk(new StepDetector(),0,5000);
  assert.equal(steps.length,9);assert.ok(steps.every(s=>s.mph===null||Math.abs(s.mph-3.40909)<.01));
});

test('gravity-based tracking works in any phone orientation',()=>{
  const a=new StepDetector(),b=new StepDetector();let steps=0;
  for(let t=0;t<=3000;t+=20){const value=9.81+2*Math.sin(2*Math.PI*t/500),left=a.sample(gravity(value),t),right=b.sample({accelerationIncludingGravity:{x:value/3,y:value*2/3,z:value*2/3}},t);assert.equal(left.step,right.step);if(left.step)steps++;}
  assert.equal(steps,5);
});

test('acceleration-only phones detect peaks while repeated peaks and excess cadence are ignored',()=>{
  const detector=new StepDetector(),steps=[];
  for(let t=0;t<=3000;t+=20)if(detector.sample(linear(t%500>=100&&t%500<200?2:0),t).step)steps.push(t);
  assert.deepEqual(steps,[600,1100,1600,2100,2600]);
  const fast=new StepDetector(),accepted=[];
  for(let t=0;t<=3000;t+=20)if(fast.sample(linear(t%100===0?3:0),t).step)accepted.push(t);
  assert.ok(accepted.every((t,i)=>i===0||t-accepted[i-1]>=250));
  const held=new StepDetector();held.sample(linear(0),0);held.sample(linear(0),300);assert.equal(held.sample(linear(2),400).step,true);assert.equal(held.sample(linear(3),900).step,false,'A held peak needs a trough before another step');
});

test('signed acceleration-only gait counts a full cycle once in every phone orientation',()=>{
  for(const axis of [{x:1,y:0,z:0},{x:-1,y:0,z:0},{x:0,y:1,z:0},{x:0,y:0,z:-1},{x:1/3,y:2/3,z:2/3}]){
    const detector=new StepDetector(),steps=[];
    for(let t=0;t<=5000;t+=10){
      const value=2*Math.sin(2*Math.PI*t/500),sample=detector.sample({acceleration:{x:axis.x*value,y:axis.y*value,z:axis.z*value}},t);
      if(sample.step)steps.push(t);
    }
    assert.equal(steps.length,9,`One signed cycle per step for ${JSON.stringify(axis)}`);
    assert.ok(steps.every((t,i)=>i===0||t-steps[i-1]>=490),'Negative half-cycles do not create extra steps');
  }
});

test('acceleration gait axis follows gradual phone turns and reacquires abrupt axis changes',()=>{
  const gradual=new StepDetector(),turned=[];
  for(let t=0;t<=5000;t+=10){
    const angle=t/5000*Math.PI,value=2*Math.sin(2*Math.PI*t/500);
    if(gradual.sample({acceleration:{x:Math.cos(angle)*value,y:Math.sin(angle)*value,z:0}},t).step)turned.push(t);
  }
  assert.equal(turned.length,9,'A slow 180-degree phone turn retains one step per full cycle');
  const abrupt=new StepDetector(),changed=[];
  for(let t=0;t<=5000;t+=10){
    const value=2*Math.sin(2*Math.PI*t/500),acceleration=t<2500?{x:value,y:0,z:0}:{x:0,y:value,z:0};
    if(abrupt.sample({acceleration},t).step)changed.push(t);
  }
  assert.equal(changed.length,8,'An abrupt 90-degree turn skips reacquisition rather than inventing steps');
  assert.ok(changed.every((t,i)=>i===0||t-changed[i-1]>=490));
});

test('normal low-amplitude vibration does not count as acceleration-only walking',()=>{
  const detector=new StepDetector();
  for(let t=0;t<=5000;t+=10){const wave=.2*Math.sin(t/12);assert.equal(detector.sample({acceleration:{x:wave,y:wave*.5,z:-wave*.8}},t).step,false);}
});

test('invalid readings, repeated timestamps and long gaps never credit catch-up distance',()=>{
  const d=new StepDetector();assert.equal(d.sample({acceleration:{x:null,y:null,z:null}},0).valid,false);
  assert.equal(d.sample(gravity(),0).step,false);assert.equal(d.sample(gravity(),300).step,false);assert.equal(d.sample(gravity(12),400).step,true);
  assert.equal(d.sample(gravity(12),400).step,false);assert.equal(d.sample(gravity(12),350).step,false);
  assert.equal(d.sample(gravity(12),10000).step,false);assert.equal(d.sample(gravity(12),10020).step,false);
  assert.equal(d.sample(linear(0),11000).step,false,'Changing sensor source resets warmup');
});

function environment({motion=true,permission,wakeLock,hidden=false,secure=true}={}){
  const keys=['isSecureContext','DeviceMotionEvent','document','navigator','addEventListener','removeEventListener','setTimeout','clearTimeout'];
  const original=new Map(keys.map(key=>[key,Object.getOwnPropertyDescriptor(globalThis,key)]));
  const target=new EventTarget(),document=new EventTarget(),timers=new Map();let clock=0,serial=0,permissionCalls=0;const originalNow=Date.now;
  document.hidden=hidden;
  const Motion=motion?class {}:undefined;if(Motion&&permission!==undefined)Motion.requestPermission=()=>{permissionCalls++;return typeof permission==='function'?permission():Promise.resolve(permission);};
  const values={isSecureContext:secure,DeviceMotionEvent:Motion,document,navigator:{...(wakeLock?{wakeLock}:{})},addEventListener:target.addEventListener.bind(target),removeEventListener:target.removeEventListener.bind(target),setTimeout:(fn,ms)=>{const id=++serial;timers.set(id,{fn,ms});return id;},clearTimeout:id=>timers.delete(id)};
  for(const [key,value]of Object.entries(values))Object.defineProperty(globalThis,key,{configurable:true,writable:true,value});Date.now=()=>clock;
  return {document,timers,get permissionCalls(){return permissionCalls;},send(value,t){clock=t;const event=new Event('devicemotion');Object.assign(event,value);target.dispatchEvent(event);},restore(){Date.now=originalNow;for(const key of keys){const descriptor=original.get(key);if(descriptor)Object.defineProperty(globalThis,key,descriptor);else delete globalThis[key];}}};
}

test('treadmill start requests motion permission immediately and emits directional estimated steps only',async()=>{
  const env=environment({permission:'granted'});try{
    const fixes=[],statuses=[],tracker=new TrailPedometer(fix=>fixes.push(fix),info=>statuses.push(info));
    tracker.setHeading(3,4);const started=tracker.start({wakeLock:false});assert.equal(env.permissionCalls,1,'The permission request must run before the first await');await started;
    env.send(gravity(),0);env.send(gravity(),300);env.send(gravity(12),400);
    assert.equal(fixes.length,1);assert.equal(fixes[0].meters,STEP_METERS);assert.ok(Math.abs(fixes[0].east-STEP_METERS*.6)<1e-10);assert.ok(Math.abs(fixes[0].north-STEP_METERS*.8)<1e-10);assert.equal(fixes[0].source,'treadmill');assert.equal(fixes[0].steps,1);assert.equal('lat' in fixes[0],false);assert.equal('positionValid' in fixes[0],false);
    tracker.setHeading(0,0);assert.deepEqual(tracker.direction,{east:.6,north:.8});assert.equal(env.timers.size,0);
    tracker.stop();env.send(gravity(),700);env.send(gravity(12),900);assert.equal(fixes.length,1);
    await tracker.start({wakeLock:false});assert.equal(tracker.steps,0);env.send(gravity(12),1500);assert.equal(fixes.length,1,'Restart does not count a stale peak');tracker.stop();
  }finally{env.restore();}
});

test('missing, denied, insecure and hidden motion tracking fail with actionable status',async()=>{
  for(const options of [{motion:false},{permission:'denied'},{secure:false},{hidden:true}]){
    const env=environment(options);try{const tracker=new TrailPedometer(()=>{});await assert.rejects(tracker.start({wakeLock:false}),/motion|Motion|foreground|HTTPS/);assert.equal(tracker.active,false);assert.equal(env.timers.size,0);}finally{env.restore();}
  }
});

test('a stop during permission or wake-lock requests ignores stale completion and releases late locks',async()=>{
  let grant;let env=environment({permission:()=>new Promise(resolve=>{grant=resolve;})});
  try{const statuses=[],tracker=new TrailPedometer(()=>assert.fail('No stale fixes'),info=>statuses.push(info)),started=tracker.start({wakeLock:false});tracker.stop();const count=statuses.length;grant('granted');await started;assert.equal(tracker.active,false);assert.equal(statuses.length,count);assert.equal(env.timers.size,0);}finally{env.restore();}
  let lockResolve,released=0;env=environment({wakeLock:{request:()=>new Promise(resolve=>{lockResolve=resolve;})}});
  try{const tracker=new TrailPedometer(()=>{}),started=tracker.start();tracker.stop();lockResolve({release:async()=>{released++;}});await started;assert.equal(released,1);assert.equal(tracker.lock,null);assert.equal(tracker.active,false);}finally{env.restore();}
});

test('zero acceleration is valid sensor data, while no usable data stops tracking with instructions',async()=>{
  const env=environment();try{
    const statuses=[],tracker=new TrailPedometer(()=>assert.fail('Stationary sensors must not walk'),info=>statuses.push(info));
    await tracker.start({wakeLock:false});env.send(linear(0),0);assert.equal(tracker.active,true);assert.equal(env.timers.size,0);env.send(linear(0),500);env.send(linear(0),1000);assert.equal(tracker.steps,0);tracker.stop();
    await tracker.start({wakeLock:false});const timer=[...env.timers.values()][0];env.send({},2000);timer.fn();assert.equal(tracker.active,false);assert.match(statuses.at(-1).reason,/No motion data.*site settings/);assert.equal(env.timers.size,0);
  }finally{env.restore();}
});

test('hiding the document stops tracking and old listeners cannot credit steps',async()=>{
  const env=environment();try{
    const fixes=[],tracker=new TrailPedometer(fix=>fixes.push(fix));await tracker.start({wakeLock:false});env.send(gravity(),0);env.send(gravity(),300);env.document.hidden=true;env.document.dispatchEvent(new Event('visibilitychange'));assert.equal(tracker.active,false);assert.equal(env.timers.size,0);env.document.hidden=false;env.send(gravity(12),400);assert.equal(fixes.length,0);
  }finally{env.restore();}
});
