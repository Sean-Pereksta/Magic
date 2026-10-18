'use strict';
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path');
const models=require('../neon-models.js');
const weapons=new Map(models.FACTION_WEAPONS.map(id=>[id,models.parseGLB(fs.readFileSync(path.join(__dirname,'../assets/weapons/new',id+'.glb')),'weapons/'+id+'.glb')]));
const near=(a,b,message)=>assert.ok(Math.abs(a-b)<1e-5,message+': '+a+' != '+b);

test('muzzle sockets come from the real barrel bore and transform with the held weapon',()=>{
  for(const [id,model] of weapons){
    const muzzle=models.weaponMuzzle(model);near(muzzle[0],0,id+' bore center');near(muzzle[1],.025,id+' bore height');
    if(id==='purr90')assert.ok(model.bounds.max[2]-muzzle[2]>.1,'Purr-90 top furniture extends past the actual bore; a bounding-box tip would be wrong');
    else near(muzzle[2],model.bounds.max[2],id+' bore is the forward tip');
    const matrix=models.compose([2,3,4],[.4,1.2,-.1],[.68,.68,.68]),point=models.transform(matrix,muzzle);
    assert.ok(point.every(Number.isFinite));assert.ok(Math.hypot(point[0]-2,point[1]-3,point[2]-4)>.2);
    muzzle[2]=-99;assert.notEqual(models.weaponMuzzle(model)[2],-99,'callers cannot mutate the parsed anchor');
  }
  const authored={rig:{muzzle:[.1,.2,.3]},bounds:{min:[-1,-1,-1],max:[1,1,2]}};assert.deepEqual(models.weaponMuzzle(authored),[.1,.2,.3]);
  assert.deepEqual(models.weaponMuzzle({bounds:{min:[-.5,-.3,-1],max:[.5,.5,2]}}),[0,.1,2]);
});

test('shot animation is opt-in and preserves cooldown-driven legacy poses',()=>{
  for(const id of models.WEAPONS){const s=models.weaponVisualState('weapons/'+id+'.glb',{recoil:.4,charge:.6,heat:1},2);
    assert.equal(s.timed,false);assert.equal(s.bolt,.4);assert.equal(s.pump,.4);assert.equal(s.barrel,.4);assert.equal(s.pulse,0);assert.equal(s.emissive,0);near(s.energyAngle,Math.sin(16)*.6*.03,'unchanged charge movement');}
});

test('short firing pulses vary by mechanism and finish independently of long weapon cooldowns',()=>{
  const shot=(id,age,extra={})=>models.weaponVisualState('weapons/'+id+'.glb',{shotAge:age,recoil:1,...extra},4);
  assert.ok(shot('whisker',.025).bolt>.9);assert.equal(shot('clawbreaker',.02).pump,0);assert.ok(shot('clawbreaker',.10).pump>.9);
  assert.ok(shot('clawbreaker',.07).pulse>0);assert.equal(shot('purr90',.07).pulse,0);assert.ok(shot('burrower',0).pulse<shot('whisker',0).pulse);
  for(const id of models.WEAPONS){const s=shot(id,.4);assert.equal(s.bolt,0,id+' bolt returned');assert.equal(s.pump,0,id+' pump returned');assert.equal(s.barrel,0,id+' barrel returned');assert.equal(s.pulse,0,id+' flash expired');}
  const state={shotAge:.02,heat:.5,charge:.7,shotColor:'#80c0ff'},before=structuredClone(state),s=shot('plasma',.02,state);assert.deepEqual(state,before);near(s.color[0],128/255,'shot color');assert.ok(s.emissive>0&&s.emissive<=1.4);
  for(const age of [-1,NaN,Infinity])assert.equal(shot('whisker',age).pulse,0,'invalid shot age cannot create light');
});

function glMock(){
  let bound;const state={draws:[],uploads:0,shaders:[],uniforms:{},ranges:[]};
  const gl={VERTEX_SHADER:1,FRAGMENT_SHADER:2,COMPILE_STATUS:3,LINK_STATUS:4,ARRAY_BUFFER:5,STATIC_DRAW:6,TRIANGLES:7,FLOAT:8,DEPTH_TEST:9,LEQUAL:10,BLEND:11,CULL_FACE:12,
    createProgram:()=>({}),createShader:type=>({type}),shaderSource:(shader,source)=>state.shaders.push(source),compileShader(){},getShaderParameter:()=>true,getShaderInfoLog:()=>'',attachShader(){},linkProgram(){},getProgramParameter:()=>true,deleteShader(){},
    getAttribLocation:(_,name)=>['aPosition','aNormal','aColor','aEmissive'].indexOf(name),getUniformLocation:(_,name)=>name,createBuffer:()=>({}),bindBuffer:(_,b)=>{bound=b;},bufferData:(_,data)=>{bound.count=data.length/12;state.uploads++;},
    useProgram(){},enable(){},disable(){},depthFunc(){},depthRange:(a,b)=>state.ranges.push([a,b]),uniform1f:(name,v)=>{assert.ok(Number.isFinite(v));state.uniforms[name]=v;},uniform3fv:(name,v)=>{assert.ok([...v].every(Number.isFinite));state.uniforms[name]=[...v];},uniformMatrix4fv:(name,transpose,v)=>{assert.equal(transpose,false);assert.ok([...v].every(Number.isFinite));state.uniforms[name]=[...v];},enableVertexAttribArray(){},vertexAttribPointer(){},drawArrays:(mode,start,count)=>{assert.equal(count,bound.count);state.draws.push(structuredClone(state.uniforms));},deleteBuffer(){},deleteProgram(){}};
  return {gl,state};
}

test('model shot lighting reuses static buffers and existing draw calls, and cannot leak onto other actors',()=>{
  const {gl,state}=glMock(),model=weapons.get('whisker'),cache={get:()=>model,status:()=>({})},renderer=models.createRenderer(gl,{cache}),camera={position:[0,1,-4],right:[1,0,0],up:[0,1,0],forward:[0,0,1],aspect:.7};
  renderer.beginView(camera,2);renderer.drawWeapon(model.path,models.compose(),{shotAge:.015,shotColor:'#ff8040'},true);
  assert.equal(state.draws.length,model.parts.length);assert.ok(state.draws.every(d=>d.uShotPulse>0));assert.deepEqual(state.ranges,[[0,.12],[0,1]]);
  const uploads=state.uploads,firstBolt=state.draws[model.parts.findIndex(p=>p.key==='bolt')].uModel;
  state.draws.length=0;renderer.drawWeapon(model.path,models.compose(),{shotAge:1,heat:.8});assert.equal(state.uploads,uploads,'no per-shot geometry upload');assert.ok(state.draws.every(d=>d.uShotPulse===0));
  const idleBolt=state.draws[model.parts.findIndex(p=>p.key==='bolt')].uModel;assert.ok(firstBolt[14]<idleBolt[14]-.1,'the real bolt moves back independently of its receiver');
  state.draws.length=0;renderer.drawActor(model.path,{x:0,y:1.86,z:3,yaw:0,hp:100,armor:0,onGround:true},{animate:false});assert.equal(state.draws[0].uShotPulse,0);assert.equal(state.draws[0].uEmissiveBoost,0);
  renderer.beginView({...camera,position:[3,1,-4]},3);assert.equal(state.uniforms.uShotPulse,0);assert.deepEqual(state.uniforms.uCamera,[3,1,-4]);renderer.dispose();
});
