import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import vm from 'node:vm';
import {register} from 'node:module';
import {fileURLToPath,pathToFileURL} from 'node:url';

// Download/extract the published asset ZIP and set QB_FOOTBALL_ASSET_PACK to
// reproduce this offline test. The current asset-authoring workspace is also
// detected. CI without the external assets skips rather than using substitutes.
const here=path.dirname(fileURLToPath(import.meta.url));
const pack=path.resolve(process.env.QB_FOOTBALL_ASSET_PACK||path.join(here,'../../../assets/pack'));
test('actual GLBs drive skin contacts, physique, replay ownership and game ball attachment',{
  skip:!fs.existsSync(path.join(pack,'models/receivers/wr_speed_lod1.glb')),
},async()=>{
  const vendor=pathToFileURL(path.join(here,'asset-tools/preview/vendor/')).href;
  const resolver=`import fs from 'node:fs/promises';const base=${JSON.stringify(vendor)};
    export async function resolve(s,c,n){if(s==='three')return {url:new URL('three.module.js',base).href,shortCircuit:true};if(s.startsWith('three/addons/'))return {url:new URL('addons/'+s.slice(13),base).href,shortCircuit:true};return n(s,c);}
    export async function load(u,c,n){if(u.includes('/asset-tools/preview/')&&u.endsWith('.js'))return {format:'module',source:await fs.readFile(new URL(u),'utf8'),shortCircuit:true};return n(u,c);}`;
  register('data:text/javascript,'+encodeURIComponent(resolver),import.meta.url);
  const THREE=await import('three'),{GLTFLoader}=await import('three/addons/loaders/GLTFLoader.js');
  const {createPlayerRuntime,WORLD_UNITS_PER_METRE}=await import('./player-runtime.mjs');
  const scaleContract=globalThis.QBPlayerScale;
  assert.equal(scaleContract.actorScale,1.4);
  assert.ok(Math.abs(WORLD_UNITS_PER_METRE-(2.27/1.88)*1.4)<1e-12);
  globalThis.self=globalThis;globalThis.ProgressEvent=class{constructor(type,init){Object.assign(this,init);}};
  // Node verifies imports/deformation. The browser checks actual PNG rendering.
  globalThis.createImageBitmap=async blob=>{const b=Buffer.from(await blob.arrayBuffer());return {width:b.readUInt32BE(16),height:b.readUInt32BE(20),close(){}};};
  const manifest=JSON.parse(fs.readFileSync(path.join(pack,'manifest.json'))),loader=new GLTFLoader(),rawTemplates=new Map();let loads=0;
  const runtime=createPlayerRuntime({baseURL:'https://fixture.test/',ids:['wr_speed','cb_ballhawk'],fetch:async()=>({ok:true,json:async()=>manifest}),loadGLTF:async url=>{
    loads++;const relative=new URL(url).pathname.slice(1),bytes=fs.readFileSync(path.join(pack,relative));const gltf=await new Promise((yes,no)=>loader.parse(bytes.buffer.slice(bytes.byteOffset,bytes.byteOffset+bytes.byteLength),'',yes,no));rawTemplates.set(relative,gltf);return gltf;
  }});
  await runtime.ready;assert.equal(runtime.status,'ready');assert.equal(loads,2);
  const roots=[];
  function legacy(scale=[1,1,1]){const root=new THREE.Group(),rig=new THREE.Group();root.add(rig);const hands=[new THREE.Object3D(),new THREE.Object3D()];hands[0].position.set(-.3,1.5,.4);hands[1].position.set(.3,1.5,.4);hands.forEach(h=>rig.add(h));root.userData={visualRig:rig,hands,appearance:{jerseyPrimary:'#00aa66'}};root.scale.set(...scale);roots.push(root);return root;}
  const profile={modelId:'wr_speed',strength:80,physical:{heightM:1.8,wingspanRatio:1.08,handRatio:1.1,chestSize:.7,armThickness:.6,legPower:.6}};
  const a=legacy([.8,1.2,1.1]),b=legacy([1.1,.8,.9]);a.position.set(4,0,-8);b.position.set(-3,0,-9);
  const ca=runtime.attach(a,profile),cb=runtime.attach(b,profile);
  assert.equal(a.userData.visualRig.visible,false);assert.equal(ca.clips.length,31);assert.equal(ca.contactLimbs.length,34);assert.equal(ca.contacts.fingers.length,28);
  const height=c=>new THREE.Box3().setFromObject(c.model,true).getSize(new THREE.Vector3()).y;
  assert.ok(Math.abs(height(ca)-height(cb))<1e-5,'Legacy root height must not scale the imported athlete twice');
  const short=runtime.attach(legacy([1.4,1.4,1.4]),{...profile,physical:{...profile.physical,heightM:1.6}}),tall=runtime.attach(legacy([1.4,1.4,1.4]),{...profile,physical:{...profile.physical,heightM:2.1}});
  assert.ok(Math.abs(height(tall)/height(short)-2.1/1.6)<1e-5,'Actual skinned height follows the physical profile');
  // Compare a neutral athlete against the original exported metre geometry,
  // independently of both the legacy root scale and the runtime conversion.
  const entry=manifest.assets.find(a=>a.id==='wr_speed'),raw=rawTemplates.get(entry.lods.low).scene;
  raw.updateMatrixWorld(true);raw.traverse(o=>o.skeleton?.update());
  const rawSize=new THREE.Box3().setFromObject(raw,true).getSize(new THREE.Vector3());
  const neutralRoot=legacy([.93*1.4,1.08*1.4,1.04*1.4]);
  const neutralProfile={modelId:entry.id,strength:60,physical:{heightM:entry.heightM,shoulderWidthM:entry.measurements.shoulder_joint_width_m,
    weightKg:entry.weightKg,wingspanRatio:1.03,handRatio:1,chestSize:0,armThickness:0,legPower:0}};
  const neutral=runtime.attach(neutralRoot,neutralProfile);neutral.mixer.stopAllAction();neutral.updateContact();
  const realSize=new THREE.Box3().setFromObject(neutral.model,true).getSize(new THREE.Vector3());
  for(const key of ['x','y','z'])assert.ok(Math.abs(realSize[key]/rawSize[key]-WORLD_UNITS_PER_METRE)<5e-5,'Actual '+key+' surface is enlarged exactly once');
  assert.ok(Math.abs(realSize.y/(rawSize.y*(2.27/1.88))-1.4)<5e-5,'Visible skin height is forty percent above the previous metre conversion');
  const rawBones=new Map();raw.traverse(o=>{if(o.isBone)rawBones.set(o.userData.name||o.name,o);});
  for(const anchor of neutral.contactLimbs){
    const expected=rawBones.get(anchor.userData.sourceBone).getWorldPosition(new THREE.Vector3()).multiplyScalar(WORLD_UNITS_PER_METRE);
    const actual=anchor.parent.getWorldPosition(new THREE.Vector3());
    assert.ok(actual.distanceTo(expected)<5e-5,'Contact bone follows the enlarged exported surface: '+anchor.userData.sourceBone);
    const worldScale=anchor.getWorldScale(new THREE.Vector3());for(const key of ['x','y','z'])assert.ok(Math.abs(worldScale[key]-WORLD_UNITS_PER_METRE)<1e-5,'Contact dimensions use the same uniform enlargement');
  }
  const rawHead=rawBones.get('head').getWorldPosition(new THREE.Vector3()).multiplyScalar(WORLD_UNITS_PER_METRE);
  assert.ok(neutral.bones.get('head').getWorldPosition(new THREE.Vector3()).distanceTo(rawHead)<5e-5,'The head and rendered body share the same enlarged stature');
  const secondPose=[...cb.bones].map(([n,bone])=>[n,bone.matrixWorld.elements.slice()]);
  const actor={mesh:a,profile,runIntensity:1};for(let i=0;i<20;i++)ca.pose(actor,1/60);assert.equal(ca.activeClip,'sprint');
  actor.catchPose=1;actor.handTargets=[true,true];for(let i=0;i<45;i++)ca.pose(actor,1/60);assert.equal(ca.activeClip,'catch_two_hand');
  for(const [n,values]of secondPose)assert.deepEqual(cb.bones.get(n).matrixWorld.elements,values);
  for(const anchor of ca.contactLimbs){assert.equal(anchor.parent.isBone,true);assert.equal(anchor.userData.actualGLB,true);assert.ok(anchor.userData.contactRadius>0&&anchor.userData.contactRadius<.15);}
  const replay=runtime.cloneForReplay(a),sourceSkins=[],replaySkins=[];a.traverse(o=>{if(o.skeleton)sourceSkins.push(o.skeleton);});replay.traverse(o=>{if(o.skeleton)replaySkins.push(o.skeleton);});
  assert.notEqual(sourceSkins[0],replaySkins[0]);assert.notEqual(sourceSkins[0].bones[0],replaySkins[0].bones[0]);assert.notEqual(sourceSkins[0].boneInverses,replaySkins[0].boneInverses);
  // Exercise the actual game contact/attachment functions with the imported rig.
  const game=fs.readFileSync(path.join(here,'game.js'),'utf8');
  function extract(name){const start=game.indexOf('function '+name+'(');assert.ok(start>=0);let end=game.indexOf('{',start),depth=1;for(end++;depth;end++){if(game[end]==='{')depth++;if(game[end]==='}')depth--;}return game.slice(start,end);}
  const ball={position:new THREE.Vector3(),rotation:new THREE.Euler(),visible:false};
  const context={THREE,PLAYER_SCALE:scaleContract.actorScale,QB_EYE_HEIGHT:scaleContract.eyeHeight,QBPlayerScale:scaleContract,
    contactStep:1/60,ballCarrier:actor,ball,ballPrev:new THREE.Vector3(),previousBallAxis:new THREE.Vector3(0,0,1),throwTime:0,
    footballAxis:()=>new THREE.Vector3(0,0,1)};vm.createContext(context);
  vm.runInContext(['pointSegmentDistance','sampleContactRig','sweepLimb','playerBallContacts','differentContactHand','attachBallToCarrier'].map(extract).join('\n')+';globalThis.hooks={sampleContactRig,sweepLimb,playerBallContacts,differentContactHand,attachBallToCarrier};',context);
  const limbs=context.hooks.sampleContactRig(actor,true);assert.equal(limbs.length,34);
  assert.ok(limbs.every(l=>l.mesh.userData.actualGLB),'Loaded players never use hidden procedural contact shapes');
  const right=ca.contacts.fingers.filter(f=>f.userData.contactSide===0),left=ca.contacts.fingers.find(f=>f.userData.contactSide===1);
  assert.equal(context.hooks.differentContactHand({mesh:right[0]},{mesh:right[1]}),false,'Two fingers of one hand are not two hands');
  assert.equal(context.hooks.differentContactHand({mesh:right[0]},{mesh:left}),true);
  const palm=limbs[0],center=palm.mesh.getWorldPosition(new THREE.Vector3());
  assert.ok(context.hooks.sweepLimb(center.clone().add(new THREE.Vector3(.4,0,0)),center.clone().add(new THREE.Vector3(-.4,0,0)),palm,.10));
  assert.equal(context.hooks.sweepLimb(center.clone().add(new THREE.Vector3(0,0,1.5)),center.clone().add(new THREE.Vector3(0,.1,1.5)),palm,.10),null);
  // The broad phase must not reject any real contact solely because athletes
  // were enlarged, including low fingers, high arms and the tallest physique.
  const tallRoot=tall.holder.parent,tallProfile={...profile,physical:{...profile.physical,heightM:2.1}};
  const standingHead=tall.bones.get('head').getWorldPosition(new THREE.Vector3()).y;
  for(const h of tallRoot.userData.hands)h.position.y=3.5;
  const highPointActor={mesh:tallRoot,profile:tallProfile,catchPose:1,handTargets:[true,true],jumpY:.6};
  tallRoot.position.y=.6;
  for(let i=0;i<75;i++)tall.pose(highPointActor,1/60);
  assert.equal(tall.activeClip,'catch_high_point');
  assert.ok(Math.max(...tall.contacts.hands.map(h=>h.getWorldPosition(new THREE.Vector3()).y))-.6>standingHead+.15,'The tallest athlete really raises his rendered gloves above his standing head');
  for(const [controller,root,playerProfile]of [[neutral,neutralRoot,neutralProfile],[tall,tallRoot,tallProfile]]){
    const physicalActor={mesh:root,profile:playerProfile};context.hooks.sampleContactRig(physicalActor,true);
    for(const anchor of controller.contactLimbs){
      const point=anchor.getWorldPosition(new THREE.Vector3());context.ballPrev.copy(point).add(new THREE.Vector3(.4,0,0));context.ball.position.copy(point).add(new THREE.Vector3(-.4,0,0));
      physicalActor.contactLock=false;const hits=context.hooks.playerBallContacts(physicalActor);
      assert.ok(hits.some(hit=>hit.limb.mesh===anchor),'Broad phase preserves a real enlarged '+anchor.userData.sourceBone+' contact');
    }
  }
  context.hooks.attachBallToCarrier();const midpoint=ca.contacts.hands[0].getWorldPosition(new THREE.Vector3()).lerp(ca.contacts.hands[1].getWorldPosition(new THREE.Vector3()),.5);
  assert.ok(ball.position.distanceTo(midpoint)<1e-7,'A secured football attaches to the actual GLB palms');
  runtime.release(a);assert.equal(a.userData.visualRig.visible,true);assert.ok(cb.loaded);runtime.dispose();
});
