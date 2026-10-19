const test=require('node:test'),assert=require('node:assert/strict'),C=require('./camera-follow.js');
const distance=(a,b)=>Math.hypot(a.x-b.x,a.y-b.y,a.z-b.z),field={minX:-26,maxX:26,minZ:-70,maxZ:56,groundY:0,margin:6};
const seed=()=>({position:{x:0,y:3.2,z:44},lookAt:{x:0,y:2.4,z:31},direction:{x:0,y:0,z:-1}});
function bounded(s){for(const v of [s.position,s.lookAt,s.direction,s.targetPosition,s.targetLookAt])for(const key of ['x','y','z'])assert.ok(Number.isFinite(v[key]),key+' must be finite');assert.ok(s.position.x>=-32&&s.position.x<=32);assert.ok(s.position.z>=-76&&s.position.z<=62);assert.ok(s.position.y>=3.2&&s.position.y<=65);assert.ok(s.lookAt.y>=.12);assert.ok(distance(s.position,s.lookAt)>.01);}
function flight(fps,type='lob'){
  const origin={x:0,y:2.4,z:43},vx=type==='lob'?4:9,vz=type==='lob'?-24:-39,vy=type==='lob'?13:4.5;let previous=seed();
  for(let i=1;i<=fps*1.6;i++){const t=i/fps,before=previous;previous=C.flightCamera({ball:{x:vx*t,y:Math.max(.16,origin.y+vy*t-4.905*t*t),z:origin.z+vz*t},velocity:{x:vx,y:vy-9.81*t,z:vz},origin,elapsed:t,field,previous,dt:1/fps});bounded(previous);assert.ok(distance(before.position,previous.position)<=85/fps+1e-8);}
  return previous;
}
test('lob and bullet tracking remain consistent at 30, 60 and 120 Hz',()=>{
  for(const type of ['lob','bullet']){const low=flight(30,type),mid=flight(60,type),high=flight(120,type);assert.ok(distance(low.position,high.position)<.13,type+' camera drift');assert.ok(distance(mid.position,high.position)<.05,type+' camera drift');assert.ok(distance(low.lookAt,high.lookAt)<.13,type+' look drift');}
});
test('a release seeded from the actual camera does not cut to the chase pose or mutate aiming inputs',()=>{
  const previous=seed(),args={ball:{x:1,y:2.4,z:42},velocity:{x:2,y:8,z:-34},origin:{x:0,y:2.4,z:43},elapsed:0,previous,field,dt:1/60},before=JSON.stringify(args),s=C.flightCamera(args);
  assert.equal(JSON.stringify(args),before);assert.ok(distance(previous.position,s.position)<.6);assert.ok(distance(s.position,s.targetPosition)>2);bounded(s);
});
test('a violent tip changes the chase heading smoothly and never tunnels under the turf',()=>{
  let previous=flight(60,'bullet');for(let i=0;i<120;i++){const t=i/60,old=previous;previous=C.flightCamera({ball:{x:Math.min(29,15+t*12),y:Math.max(-2,4-6*t),z:-20+t*22},velocity:{x:12,y:-6,z:22},origin:{x:0,y:2.4,z:43},previous,dt:1/60,field,elapsed:2+t});bounded(previous);assert.ok(distance(old.position,previous.position)<=85/60+1e-8);const dot=old.direction.x*previous.direction.x+old.direction.z*previous.direction.z;assert.ok(dot>=Math.cos(1.8/60)-1e-10,'Tip must not reverse camera heading in one frame');}
  assert.ok(previous.direction.x*(12/Math.hypot(12,22))+previous.direction.z*(22/Math.hypot(12,22))>.999,'Camera eventually follows the changed flight direction');assert.ok(previous.lookAt.y<.13);
});
test('possession and replay transitions continue from the flight camera with bounded steps',()=>{
  let previous=flight(60),old=previous;for(let i=0;i<60;i++){const next=C.carrierCamera({carrier:{x:8+i/20,y:0,z:2-i/10},velocity:{x:3,y:0,z:-6},previous,dt:1/60,field});bounded(next);assert.ok(distance(previous.position,next.position)<=85/60+1e-8);previous=next;}
  assert.ok(distance(old.position,previous.position)>1);const bridge=C.transitionCamera({position:{x:-25,y:14,z:20},lookAt:{x:8,y:1.5,z:-4},previous,dt:1/60,field});bounded(bridge);assert.ok(distance(previous.position,bridge.position)<=85/60+1e-8);
  const reset=C.flightCamera({ball:{x:0,y:2.4,z:42},velocity:{x:0,y:5,z:-30},previous:seed(),dt:0,field});assert.deepEqual(reset.position,seed().position);
});
test('a grounded stationary ball settles without orbiting back toward its release direction',()=>{
  let previous=C.flightCamera({ball:{x:12,y:.16,z:-20},velocity:{x:20,y:0,z:0},field}),direction={...previous.direction};
  for(let i=0;i<120;i++)previous=C.flightCamera({ball:{x:12,y:.16,z:-20},velocity:{x:0,y:0,z:0},origin:{x:0,y:3.15,z:43},elapsed:4+i/60,previous,dt:1/60,field});
  bounded(previous);assert.ok(distance(previous.direction,direction)<1e-10);assert.ok(distance(previous.position,previous.targetPosition)<.001);assert.ok(distance(previous.lookAt,{x:12,y:.16,z:-20})<.001);
});
test('missing values, stopped balls, long frames and malformed bounds stay finite and constrained',()=>{
  for(const input of [{},{field:null,previous:{lookAt:{x:NaN},position:{x:NaN},direction:{y:Infinity}}},{ball:{x:NaN,y:-100,z:Infinity},velocity:{x:Infinity,y:NaN,z:null},dt:NaN},{ball:{x:1e8,y:1e8,z:-1e8},velocity:{x:0,y:0,z:0},dt:10,previous:seed()},{field:{minX:9,maxX:-2,minZ:8,maxZ:-6,groundY:4,margin:-2},ball:{x:0,y:0,z:0}}]){
    const s=C.flightCamera(input);for(const v of [s.position,s.lookAt,s.direction])for(const key of ['x','y','z'])assert.ok(Number.isFinite(v[key]));assert.ok(s.position.y>=(input.field?.groundY||0)+1.5);assert.ok(distance(s.position,s.lookAt)>.01);
  }
  const s=C.flightCamera({ball:{x:15,y:0,z:-20},velocity:{x:0,y:0,z:0},previous:seed(),dt:10,field});assert.ok(distance(seed().position,s.position)<=8.5+1e-8,'A stalled frame may not teleport the camera');
});
