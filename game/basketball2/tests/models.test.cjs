'use strict';
const test=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const path=require('node:path');
const MODELS=path.join(__dirname,'..','assets','models');
const CLIPS=['idle','run','sprint','defensive_shuffle','pivot','jump_shot','layup','dunk','pass','catch','block','crossover'];
const BONE_NAMES=['root','hips','spine','chest','neck','head',...['L','R'].flatMap(s=>['clavicle','upper_arm','forearm','hand','thumb','thigh','shin','foot','toes'].map(n=>n+'_'+s))];

function readGLB(file) {
  const bytes=fs.readFileSync(path.join(MODELS,file));
  assert.equal(bytes.readUInt32LE(0),0x46546c67,'binary glTF magic');
  assert.equal(bytes.readUInt32LE(4),2,'glTF version 2');
  assert.equal(bytes.readUInt32LE(8),bytes.length,'GLB byte length');
  const jsonSize=bytes.readUInt32LE(12);
  assert.equal(bytes.readUInt32LE(16),0x4e4f534a,'JSON chunk');
  const json=JSON.parse(bytes.subarray(20,20+jsonSize).toString('utf8'));
  const binaryStart=28+jsonSize;
  assert.equal(bytes.readUInt32LE(binaryStart-4),0x004e4942,'BIN chunk');
  const binary=bytes.subarray(binaryStart);
  function accessor(id) {
    const a=json.accessors[id],view=json.bufferViews[a.bufferView];
    const n={SCALAR:1,VEC2:2,VEC3:3,VEC4:4,MAT4:16}[a.type],stride={5123:2,5125:4,5126:4}[a.componentType];
    const offset=(view.byteOffset||0)+(a.byteOffset||0),read={5123:'readUInt16LE',5125:'readUInt32LE',5126:'readFloatLE'}[a.componentType];
    assert.ok(offset+a.count*n*stride<=binary.length,'accessor within embedded buffer');
    return Array.from({length:a.count},(_,i)=>Array.from({length:n},(_,j)=>binary[read](offset+(i*n+j)*stride)));
  }
  return {json,binary,accessor,bytes};
}
const add=(a,b)=>a.map((v,i)=>v+b[i]);
const rotate=(q,v)=>{
  const [x,y,z,w]=q,[a,b,c]=v;
  const ix=w*a+y*c-z*b,iy=w*b+z*a-x*c,iz=w*c+x*b-y*a,iw=-x*a-y*b-z*c;
  return [ix*w+iw*-x+iy*-z-iz*-y,iy*w+iw*-y+iz*-x-ix*-z,iz*w+iw*-z+ix*-y-iy*-x];
};
const product=(q,r)=>[
  q[3]*r[0]+q[0]*r[3]+q[1]*r[2]-q[2]*r[1],q[3]*r[1]-q[0]*r[2]+q[1]*r[3]+q[2]*r[0],q[3]*r[2]+q[0]*r[1]-q[1]*r[0]+q[2]*r[3],q[3]*r[3]-q[0]*r[0]-q[1]*r[1]-q[2]*r[2]
];
function poseBones(model,animationName,frameFraction) {
  const {json,accessor}=model,clip=json.animations.find(a=>a.name===animationName),locals=json.nodes.map(n=>({p:n.translation||[0,0,0],q:[0,0,0,1]}));
  if(clip) for(const channel of clip.channels) {
    const data=accessor(clip.samplers[channel.sampler].output),value=data[Math.round(frameFraction*(data.length-1))];
    locals[channel.target.node][channel.target.path==='rotation'?'q':'p']=value;
  }
  const world=[];
  function visit(index,parent={p:[0,0,0],q:[0,0,0,1]}) {
    const l=locals[index],node=json.nodes[index];world[index]={p:add(parent.p,rotate(parent.q,l.p)),q:product(parent.q,l.q)};
    for(const child of node.children||[])visit(child,world[index]);
  }
  for(const node of json.scenes[json.scene].nodes)visit(node);
  return world;
}

for(const variant of ['guard','wing','center']) {
  test(`${variant} GLB embeds complete renderable mesh, rig, and animation data`,()=>{
    const model=readGLB('player-'+variant+'.glb'),{json,accessor,bytes}=model;
    assert.equal(json.asset.version,'2.0');
    assert.ok(bytes.length<900*1024,'individual asset stays below 900 KiB');
    assert.ok(!json.buffers.some(b=>b.uri),'no external buffers');
    assert.ok(!json.images?.some(image=>image.uri),'no external image requests');
    assert.equal(json.skins.length,1);assert.equal(json.skins[0].joints.length,24);
    const boneSet=new Set(json.skins[0].joints.map(i=>json.nodes[i].name));
    assert.deepEqual(boneSet,new Set(BONE_NAMES));
    assert.deepEqual(new Set(json.animations.map(a=>a.name)),new Set(CLIPS));
    const mesh=json.meshes[0];assert.equal(mesh.primitives.length,12);
    assert.ok(mesh.primitives.every(p=>p.mode===4));
    const attrs=mesh.primitives[0].attributes,positions=accessor(attrs.POSITION),normals=accessor(attrs.NORMAL),weights=accessor(attrs.WEIGHTS_0),joints=accessor(attrs.JOINTS_0);
    assert.ok(positions.length>7000&&positions.length<12000);
    assert.equal(normals.length,positions.length);assert.equal(weights.length,positions.length);assert.equal(joints.length,positions.length);
    for(let i=0;i<positions.length;i++) {
      assert.ok(positions[i].every(Number.isFinite));
      assert.ok(Math.abs(Math.hypot(...normals[i])-1)<.0001);
      assert.ok(weights[i].every(w=>w>=0&&w<=1));
      assert.ok(Math.abs(weights[i].reduce((a,b)=>a+b,0)-1)<.00001,'normalized skin weights');
      assert.ok(joints[i].every(j=>j>=0&&j<24));
    }
    assert.ok(weights.filter(w=>w.filter(n=>n>.1).length>1).length>1000,'soft joint deformation is present');
    for(const primitive of mesh.primitives) assert.ok(accessor(primitive.indices).flat().every(i=>i<positions.length));
    const rest=poseBones(model),ibm=accessor(json.skins[0].inverseBindMatrices);
    json.skins[0].joints.forEach((node,i)=>rest[node].p.forEach((v,k)=>assert.ok(Math.abs(v+ibm[i][12+k])<.00001,'inverse bind restores rest pose')));
    for(const clip of json.animations)for(const sampler of clip.samplers) {
      assert.equal(sampler.interpolation,'LINEAR');const times=accessor(sampler.input).flat(),values=accessor(sampler.output);
      assert.equal(times.length,values.length);assert.ok(times.every((v,i)=>i===0||v>times[i-1]));
      assert.ok(values.flat().every(Number.isFinite));
      if(values[0].length===4)assert.ok(values.every(q=>Math.abs(Math.hypot(...q)-1)<.0001),'unit animation quaternions');
    }
  });
  test(`${variant} shot and locomotion clips move articulated hands and knees`,()=>{
    const model=readGLB('player-'+variant+'.glb'),names=Object.fromEntries(model.json.nodes.map((n,i)=>[n.name,i]));
    const rest=poseBones(model),shot=poseBones(model,'jump_shot',.53),layup=poseBones(model,'layup',.59),block=poseBones(model,'block',.52),run1=poseBones(model,'run',.25),run2=poseBones(model,'run',.75);
    assert.ok(shot[names.hand_R].p[1]>rest[names.head].p[1]+.12,'shooting hand reaches above head');
    assert.ok(layup[names.hand_R].p[1]>rest[names.head].p[1]+.15,'layup reaches toward rim');
    assert.ok(block[names.hand_L].p[1]>rest[names.head].p[1]+.12,'block extends both arms');
    assert.ok(Math.abs(run1[names.foot_L].p[2]-run2[names.foot_L].p[2])>.35,'run feet alternate through a real stride');
    assert.ok(Math.abs(run1[names.hand_R].p[2]-run2[names.hand_R].p[2])>.25,'run arm swing counterbalances legs');
    assert.equal(shot[names.root].p[0],0);assert.equal(shot[names.root].p[2],0,'clips remain in place');
  });
}

test('athlete variants have distinct authored proportions and bounded assets',()=>{
  const models=['guard','wing','center'].map(v=>readGLB('player-'+v+'.glb'));
  const heights=models.map(m=>m.json.nodes.find(n=>n.name==='AthleteRoot').extras.authoredHeight);
  assert.ok(heights[0]<heights[1]&&heights[1]<heights[2]);
  const manifest=JSON.parse(fs.readFileSync(path.join(MODELS,'manifest.json'),'utf8'));
  assert.deepEqual(manifest.clips,CLIPS);assert.equal(manifest.models.length,3);
});
