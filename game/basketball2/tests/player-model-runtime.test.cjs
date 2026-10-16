const test=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const path=require('node:path');
const vm=require('node:vm');

const threeBundle=process.env.THREE_BUNDLE || path.resolve(__dirname,'../../../..','three.min.js');
const hasThree=fs.existsSync(threeBundle);
const runtime=fs.readFileSync(path.join(__dirname,'../js/player-models.js'),'utf8');
const bytes=kind=>{
  const data=fs.readFileSync(path.join(__dirname,`../assets/models/player-${kind}.glb`));
  return data.buffer.slice(data.byteOffset,data.byteOffset+data.byteLength);
};
function harness(){
  const warnings=[];
  const ctx=vm.createContext({console:{log(){},warn:(...args)=>warnings.push(args)},ArrayBuffer,DataView,Uint8Array,Int8Array,Uint16Array,Int16Array,Uint32Array,Float32Array,TextDecoder,
    shotCharging:false,chargingPlayer:null,dunkActive:false,layupActive:false,ballState:'loose',ballSideValue:1,passFlight:{active:false},crossover:{active:false},matchPlayers:[]});
  vm.runInContext(fs.readFileSync(threeBundle,'utf8'),ctx);
  vm.runInContext(runtime,ctx);
  ctx.warnings=warnings;
  return ctx;
}
function character(ctx,number='23'){
  return {id:'p1',group:new ctx.THREE.Group(),profile:{height:1,width:1,skin:0x704327,hair:0x080808,number},jerseyStyle:{primary:0x258fcd,secondary:0x123656,trim:0xffffff},sneaker:{primary:0xff6600,sole:0x112233},
    angle:0,velocity:new ctx.THREE.Vector3(),onGround:true,hasBall:false,moveState:'normal',moveMeta:{},anim:{headYaw:.25},dribbleBounce:0};
}
function instantiate(ctx,asset,player){
  player.playerModel=ctx.createBasketballPlayerInstance(asset,player);
  player.group.add(player.playerModel.group);player.group.updateMatrixWorld(true);
  return player.playerModel;
}
function mutate(source,edit){
  const data=Buffer.from(source),jsonSize=data.readUInt32LE(12);
  const json=JSON.parse(data.toString('utf8',20,20+jsonSize));
  edit(json);
  const text=Buffer.from(JSON.stringify(json)),jsonPad=Buffer.alloc(Math.ceil(text.length/4)*4,0x20);text.copy(jsonPad);
  const binary=data.subarray(20+jsonSize+8);
  const result=Buffer.alloc(12+8+jsonPad.length+8+binary.length);
  result.writeUInt32LE(0x46546c67,0);result.writeUInt32LE(2,4);result.writeUInt32LE(result.length,8);
  result.writeUInt32LE(jsonPad.length,12);result.writeUInt32LE(0x4e4f534a,16);jsonPad.copy(result,20);
  result.writeUInt32LE(binary.length,20+jsonPad.length);result.writeUInt32LE(0x004e4942,24+jsonPad.length);binary.copy(result,28+jsonPad.length);
  return result.buffer.slice(result.byteOffset,result.byteOffset+result.byteLength);
}

test('GLB loader rejects truncated headers before using the rendering library',()=>{
  const ctx=vm.createContext({ArrayBuffer,DataView,Uint8Array,TextDecoder});vm.runInContext(runtime,ctx);
  assert.throws(()=>ctx.parseBasketballPlayerGLB(new ArrayBuffer(8)),/invalid GLB size/);
  assert.throws(()=>ctx.parseBasketballPlayerGLB(new ArrayBuffer(24)),/invalid GLB 2.0 header/);
});

test('all shipped GLBs create independent skeletons and materials with shared immutable geometry',{skip:!hasThree},()=>{
  const ctx=harness();
  for(const kind of ['guard','wing','center']){
    const asset=ctx.parseBasketballPlayerGLB(bytes(kind),kind);
    assert.equal(asset.clips.length,12);
    const a=character(ctx,'3'),b=character(ctx,'44');b.jerseyStyle.primary=0xe45939;
    const first=instantiate(ctx,asset,a),second=instantiate(ctx,asset,b);
    assert.equal(first.bones.size,24);
    assert.notEqual(first.bones.get('hand_L'),second.bones.get('hand_L'));
    assert.notEqual(first.skeletons[0],second.skeletons[0]);
    assert.equal(first.nodes.find(node=>node.name==='Athlete').children[0].geometry,second.nodes.find(node=>node.name==='Athlete').children[0].geometry);
    assert.notEqual(first.materials[0],second.materials[0]);
    assert.equal(first.materials.find(material=>material.name==='jersey').color.getHex(),0x258fcd);
    assert.equal(second.materials.find(material=>material.name==='jersey').color.getHex(),0xe45939);
    assert.equal(first.materials.find(material=>material.name==='skin').color.getHex(),a.profile.skin);
    const geometry=asset.meshes[0][0].geometry,weights=geometry.getAttribute('skinWeight').array;
    assert.equal(geometry.getAttribute('position'),asset.meshes[0][1].geometry.getAttribute('position'),'material primitives share their GPU vertex attribute');
    for(let i=0;i<weights.length;i+=4) assert.ok(Math.abs(weights[i]+weights[i+1]+weights[i+2]+weights[i+3]-1)<1e-5);
    const left=ctx.getModelHandWorld(a,'left'),right=ctx.getModelHandWorld(a,'right');
    assert.ok(left.x<right.x,'game hand side matches local player side');
    assert.ok(left.y>0 && right.y<2.4);
    ctx.disposePlayerModel(a);ctx.disposePlayerModel(b);
  }
});

test('animation chooses speed-scaled locomotion, charge and release poses, and pass/block actions',{skip:!hasThree},()=>{
  const ctx=harness(),asset=ctx.parseBasketballPlayerGLB(bytes('wing')),player=character(ctx);instantiate(ctx,asset,player);
  player.velocity.set(2,0,0);ctx.updatePlayerModel(player,1/60);
  assert.equal(player.playerModel.current.getClip().name,'run');
  const jogCadence=player.playerModel.current.getEffectiveTimeScale();
  player.velocity.set(4,0,0);ctx.updatePlayerModel(player,1/60);
  assert.ok(player.playerModel.current.getEffectiveTimeScale()>jogCadence);
  player.velocity.set(7,0,0);ctx.updatePlayerModel(player,1/60);
  assert.equal(player.playerModel.current.getClip().name,'sprint');
  ctx.chargingPlayer=player;ctx.shotCharging=true;ctx.updatePlayerModel(player,1/60);
  assert.equal(player.playerModel.mode,'charge');assert.ok(player.playerModel.current.paused);
  ctx.shotCharging=false;player.shotMotionTime=.45;ctx.updatePlayerModel(player,1/60);
  assert.equal(player.playerModel.mode,'release');
  assert.ok(player.playerModel.current.time/player.playerModel.current.getClip().duration>=.52);
  player.shotMotionTime=0;player.passMotionTime=.2;ctx.updatePlayerModel(player,1/60);
  assert.equal(player.playerModel.current.getClip().name,'pass');
  player.passMotionTime=0;player.blockTime=.3;ctx.updatePlayerModel(player,1/60);
  assert.equal(player.playerModel.current.getClip().name,'block');
  player.handsUpTime=.42;ctx.updatePlayerModel(player,1/60);
  assert.ok(Math.abs(player.playerModel.current.time/player.playerModel.current.getClip().duration-.48)<1e-6,'a held contest shows raised hands');
  player.blockTime=0;player.handsUpTime=0;player.moveState='pumpfake';player.moveTimer=.2;player.moveDuration=.4;ctx.updatePlayerModel(player,1/60);
  assert.equal(player.playerModel.mode,'pumpfake');
});

test('dribble IK and head look remain finite over time and world hand points follow player transforms',{skip:!hasThree},()=>{
  const ctx=harness(),asset=ctx.parseBasketballPlayerGLB(bytes('guard')),player=character(ctx);instantiate(ctx,asset,player);
  ctx.ballState='dribble';player.hasBall=true;
  for(let i=0;i<300;i++){
    player.dribbleBounce=i*.13;ctx.updatePlayerModel(player,1/60);
    const hand=ctx.getModelHandWorld(player,'right');
    assert.ok(hand.toArray().every(Number.isFinite));assert.ok(hand.x>0,'dribble remains on the selected hand side');
  }
  assert.ok(Math.abs(player.playerModel.bones.get('head').rotation.y)<.7,'head look does not accumulate every frame');
  const point=ctx.getModelHandWorld(player,'right');
  player.group.position.set(3,1,-4);
  const moved=ctx.getModelHandWorld(player,'right');
  assert.ok(moved.distanceTo(point.clone().add(new ctx.THREE.Vector3(3,1,-4)))<1e-6);
  player.group.rotation.y=Math.PI;
  assert.ok(ctx.getModelHandWorld(player,'right').x<3);
});

test('malformed buffer views, joint indices, hierarchy and clips are rejected',{skip:!hasThree},()=>{
  const ctx=harness(),source=bytes('wing');
  const edits=[
    [json=>{json.bufferViews[0].byteStride=3;},/alignment or stride/],
    [json=>{json.accessors[0].count=500000;},/exceeds its buffer view/],
    [json=>{json.accessors[0].sparse={count:1};},/invalid accessor reference/],
    [json=>{json.skins[0].joints[0]=1000;},/invalid skin joints/],
    [json=>{json.nodes[0].children.push(0);},/invalid node hierarchy/],
    [json=>{json.animations.pop();},/missing basketball animation clips/],
    [json=>{json.animations[0].samplers[0].interpolation='CUBICSPLINE';},/unsupported animation channel/]
  ];
  for(const [edit,expected] of edits) assert.throws(()=>ctx.parseBasketballPlayerGLB(mutate(source,edit)),expected);
});

test('async model fetches coalesce and failures preserve playable legacy geometry with one warning',{skip:!hasThree},async()=>{
  const ctx=harness();let calls=0;
  ctx.fetch=async()=>{calls++;throw new Error('offline');};
  const first=ctx.loadPlayerModels(),second=ctx.loadPlayerModels();
  assert.equal(await first,false);assert.equal(await second,false);assert.equal(calls,3);
  const player=character(ctx),legacy=new ctx.THREE.Group();player.group.add(legacy);
  assert.equal(ctx.attachPlayerModel(player),false);assert.equal(legacy.visible,true);
  assert.equal(ctx.warnings.filter(args=>String(args[0]).includes('could not load')).length,1);
});

test('disposing an attached model restores fallback visibility without disposing cached geometry',{skip:!hasThree},()=>{
  const ctx=harness(),asset=ctx.parseBasketballPlayerGLB(bytes('wing')),player=character(ctx);
  const body=new ctx.THREE.Group(),shadow=new ctx.THREE.Group();player.group.add(body,shadow);player.shadow=shadow;player.legacyVisualNodes=[body];
  vm.runInContext('basketballPlayerModels.assets',ctx).set('wing',asset);
  let disposed=0;asset.meshes[0][0].geometry.addEventListener('dispose',()=>disposed++);
  assert.equal(ctx.attachPlayerModel(player),true);assert.equal(body.visible,false);assert.equal(shadow.visible,true);
  const material=player.playerModel.materials[0];let materialDisposed=0;material.addEventListener('dispose',()=>materialDisposed++);
  ctx.disposePlayerModel(player);
  assert.equal(player.playerModel,null);assert.equal(body.visible,true);assert.equal(disposed,0);assert.equal(materialDisposed,1);
  assert.equal(ctx.getModelHandWorld(player,'right'),null);
});
