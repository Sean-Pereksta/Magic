#!/usr/bin/env node
// Load every GLB through the same Three.js revision used by the game.
// Texture PNG data is decoded in Node; GPU rendering is a separate browser check.
import fs from 'node:fs';
import path from 'node:path';
import zlib from 'node:zlib';
import {fileURLToPath} from 'node:url';
import * as THREE from 'three';
import {GLTFLoader} from 'three/addons/loaders/GLTFLoader.js';
const here=path.dirname(fileURLToPath(import.meta.url));
const defaultRoot=[path.join(here,'../pack'),path.join(here,'../..')].find(p=>fs.existsSync(path.join(p,'manifest.json')))||path.join(here,'../pack');
const root=path.resolve(process.argv[2]||defaultRoot);
const reportDir=path.join(root,'validation');
const onlyPattern=process.argv[3]==='--only'?process.argv[4]:null;
fs.mkdirSync(reportDir,{recursive:true});
const failures=[],assets=[];
const manifest=fs.existsSync(path.join(root,'manifest.json'))?JSON.parse(fs.readFileSync(path.join(root,'manifest.json'),'utf8')):null;
let decodedImages=0;
globalThis.self=globalThis;
globalThis.ProgressEvent??=class ProgressEvent {constructor(type,init){this.type=type;Object.assign(this,init);}};
globalThis.createImageBitmap=async(blob)=>{
  const bytes=Buffer.from(await blob.arrayBuffer());
  if(!bytes.subarray(0,8).equals(Buffer.from([137,80,78,71,13,10,26,10]))) throw Error('Smoke decoder supports PNG textures only');
  const width=bytes.readUInt32BE(16),height=bytes.readUInt32BE(20),depth=bytes[24],colorType=bytes[25];
  const idat=[];
  for(let p=8;p+12<=bytes.length;){const n=bytes.readUInt32BE(p),t=bytes.toString('ascii',p+4,p+8);if(t==='IDAT')idat.push(bytes.subarray(p+8,p+8+n));p+=n+12;}
  const raw=zlib.inflateSync(Buffer.concat(idat));
  const channels={0:1,2:3,3:1,4:2,6:4}[colorType];
  if(depth!==8||!channels||raw.length!==(width*channels+1)*height) throw Error('PNG image data/dimensions mismatch or unsupported PNG format');
  decodedImages++;
  return {width,height,data:raw,close(){}};
};
function walk(dir){return fs.readdirSync(dir,{withFileTypes:true}).flatMap(e=>e.isDirectory()?walk(path.join(dir,e.name)):[path.join(dir,e.name)]);}
function vertexBounds(scene,full=false){
  scene.updateMatrixWorld(true);const bounds=new THREE.Box3(),v=new THREE.Vector3();let count=0;
  scene.traverse(o=>{
    if(!o.isMesh)return;
    if(o.isSkinnedMesh)o.skeleton.update();
    const pos=o.geometry.attributes.position,step=full?1:Math.max(1,Math.floor(pos.count/256));
    for(let i=0;i<pos.count;i+=step){o.getVertexPosition(i,v);v.applyMatrix4(o.matrixWorld);if(![v.x,v.y,v.z].every(Number.isFinite))throw Error('Non-finite deformed vertex');bounds.expandByPoint(v);count++;}
  });
  return {min:bounds.min.toArray(),max:bounds.max.toArray(),size:bounds.getSize(new THREE.Vector3()).toArray(),sampledVertices:count};
}
for(const file of walk(root).filter(f=>f.toLowerCase().endsWith('.glb')&&(!onlyPattern||f.replaceAll('\\','/').endsWith(onlyPattern)))){
  const relative=path.relative(root,file).replaceAll('\\','/');
  try {
    const bytes=fs.readFileSync(file),buffer=bytes.buffer.slice(bytes.byteOffset,bytes.byteOffset+bytes.byteLength);
    const imageCountBefore=decodedImages;
    const gltf=await new Promise((resolve,reject)=>new GLTFLoader().parse(buffer,'',resolve,reject));
    const scene=gltf.scene,base=vertexBounds(scene,true),meshes=[],bones=[];
    scene.traverse(o=>{if(o.isMesh)meshes.push(o);if(o.isBone)bones.push(o);});
    const entry=manifest?.archetypes?.find(a=>a.model===relative);
    let manifestGeometryComparison=null;
    if(entry?.measurements?.mesh_bounds_gltf_min&&entry?.measurements?.mesh_bounds_gltf_max){
      const m=entry.measurements;
      const boundError=Math.max(...base.min.map((v,i)=>Math.abs(v-m.mesh_bounds_gltf_min[i])),...base.max.map((v,i)=>Math.abs(v-m.mesh_bounds_gltf_max[i])));
      const anchorErrors=[];
      for(const suffix of ['L','R'])for(const [anchor,joint]of [['shoulder','upper_arm'],['elbow','forearm'],['wrist','hand']]){
        const expected=m.contact_anchors_gltf?.[suffix]?.[anchor],bone=bones.find(b=>(b.userData.name||b.name)===joint+'.'+suffix);
        if(expected&&bone)anchorErrors.push({name:anchor+'.'+suffix,error:new THREE.Vector3(...expected).distanceTo(bone.getWorldPosition(new THREE.Vector3()))});
      }
      if(boundError>0.002)failures.push({file:relative,message:'Manifest mesh bounds differ by more than 2mm: '+boundError});
      if(anchorErrors.some(a=>a.error>0.002))failures.push({file:relative,message:'Manifest shoulder/elbow/wrist anchor differs from actual rig by more than 2mm'});
      manifestGeometryComparison={maximumBoundsError:boundError,anchorErrors};
    }
    const bindPose=[];
    const firstSkin=meshes.find(m=>m.isSkinnedMesh)?.skeleton;
    if(firstSkin)for(let i=0;i<firstSkin.bones.length;i++){
      const transform=new THREE.Matrix4().multiplyMatrices(firstSkin.bones[i].matrixWorld,firstSkin.boneInverses[i]);
      const identity=new THREE.Matrix4().elements;
      const maximumIdentityDeviation=Math.max(...transform.elements.map((v,j)=>Math.abs(v-identity[j])));
      bindPose.push({bone:firstSkin.bones[i].name,maximumIdentityDeviation});
    }
    const materials=new Set(meshes.flatMap(m=>Array.isArray(m.material)?m.material:[m.material]));
    const textures=[...materials].flatMap(m=>['map','normalMap','roughnessMap','metalnessMap','emissiveMap','aoMap'].filter(k=>m[k]).map(k=>m[k]));
    const morphNames=[...new Set(meshes.flatMap(m=>Object.keys(m.morphTargetDictionary||{})))];
    const uniformScaleTests=[];
    if(relative.startsWith('models/')){
      const scale=scene.scale.clone();
      for(const factor of [0.9,1.1]){
        scene.scale.copy(scale).multiplyScalar(factor);const bounds=vertexBounds(scene,true);
        const maximumSizeError=Math.max(...bounds.size.map((v,i)=>Math.abs(v-base.size[i]*factor)));
        if(maximumSizeError>0.0001)failures.push({file:relative,message:'Whole-rig height scaling changed bounds nonlinearly: '+maximumSizeError});
        uniformScaleTests.push({factor,bounds,maximumSizeError});
      }
      scene.scale.copy(scale);scene.updateMatrixWorld(true);
    }
    const morphs=[];
    for(const name of morphNames){
      for(const mesh of meshes)if(mesh.morphTargetDictionary?.[name]!==undefined)mesh.morphTargetInfluences[mesh.morphTargetDictionary[name]]=1;
      const b=vertexBounds(scene,true);const difference=Math.max(...b.size.map((v,i)=>Math.abs(v-base.size[i])));
      let maxDelta=0;
      for(const mesh of meshes){const index=mesh.morphTargetDictionary?.[name];if(index===undefined)continue;const a=mesh.geometry.morphAttributes.position?.[index];if(a)for(let i=0;i<a.count;i++)maxDelta=Math.max(maxDelta,Math.hypot(a.getX(i),a.getY(i),a.getZ(i)));}
      if(relative.startsWith('models/')&&!(maxDelta>0.000001))throw Error('Player morph has no nonzero geometric change: '+name);
      morphs.push({name,bounds:b,maxDelta,maximumBoundsSizeChange:difference});
      for(const mesh of meshes)if(mesh.morphTargetDictionary?.[name]!==undefined)mesh.morphTargetInfluences[mesh.morphTargetDictionary[name]]=0;
    }
    const mixer=new THREE.AnimationMixer(scene),animationSamples=[];
    const savedPose=bones.map(b=>[b,b.position.clone(),b.quaternion.clone(),b.scale.clone()]);
    const criticalNames=['root','pelvis','chest','head','hand.L','hand.R'];
    const criticalBones=Object.fromEntries(criticalNames.map(n=>[n,bones.find(b=>(b.userData.name||b.name)===n)]).filter(([,b])=>b));
    const initialRootY=criticalBones.root?.getWorldPosition(new THREE.Vector3()).y||0;
    const fingerRest=[];
    for(const side of ['L','R']){
      const hand=criticalBones['hand.'+side],finger=bones.find(b=>(b.userData.name||b.name)==='index_03.'+side);
      if(hand&&finger){const inv=hand.matrixWorld.clone().invert();fingerRest.push({side,hand,finger,position:finger.getWorldPosition(new THREE.Vector3()).applyMatrix4(inv),palm:new THREE.Vector3(0,0,1).transformDirection(inv)});}
    }
    for(const clip of gltf.animations){
      const action=mixer.clipAction(clip);action.reset();action.setLoop(THREE.LoopOnce,1);action.clampWhenFinished=true;action.play();
      const samples=[];
      for(const fraction of [0,0.25,0.5,0.75,0.999]){
        mixer.setTime(clip.duration*fraction);const bounds=vertexBounds(scene);
        const joints=Object.fromEntries(Object.entries(criticalBones).map(([n,b])=>[n,b.getWorldPosition(new THREE.Vector3()).toArray()]));
        samples.push({time:clip.duration*fraction,bounds,joints});
      }
      if(samples.some(s=>s.bounds.size.some(v=>!Number.isFinite(v)||v>10)))throw Error('Animation produces implausible/nonfinite extents: '+clip.name);
      if((/high.?point/i.test(clip.name)||clip.name==='jump'||/jump.?swat/i.test(clip.name))&&criticalBones.root){
        const lift=Math.max(...samples.map(s=>s.joints.root[1]-initialRootY));
        if(lift<0.3)failures.push({file:relative,clip:clip.name,message:'Jump root does not rise at least 0.30m in glTF +Y: '+lift});
      }
      if(/high.?point/i.test(clip.name)&&criticalBones['hand.L']&&criticalBones['hand.R']){
        const reach=Math.max(...samples.flatMap(s=>[s.joints['hand.L'][1],s.joints['hand.R'][1]]));
        if(reach<base.max[1]+0.1)failures.push({file:relative,clip:clip.name,message:'High-point hands do not reach at least 0.10m above standing helmet: '+reach});
      }
      if(/div|fall|tackl|recover/i.test(clip.name)){
        const badCore=samples.some(s=>['pelvis','chest','head'].some(n=>s.joints[n]&&s.joints[n][1]<-0.1));
        if(badCore)failures.push({file:relative,clip:clip.name,message:'Prone/transition core joint more than 0.10m below ground'});
        const badHands=samples.some(s=>['hand.L','hand.R'].some(n=>s.joints[n]&&s.joints[n][1]<-0.2));
        if(badHands)failures.push({file:relative,clip:clip.name,message:'Prone/transition hand joint more than 0.20m below ground'});
      }
      animationSamples.push({name:clip.name,duration:clip.duration,tracks:clip.tracks.length,samples});
      mixer.stopAllAction();for(const [b,p,q,s]of savedPose){b.position.copy(p);b.quaternion.copy(q);b.scale.copy(s);}scene.updateMatrixWorld(true);
    }
    const fingerCurlTests=[];
    const gripClip=gltf.animations.find(a=>a.name==='catch_two_hand');
    if(gripClip&&fingerRest.length){
      const action=mixer.clipAction(gripClip);action.reset();action.play();mixer.setTime(Math.min(0.57,gripClip.duration));scene.updateMatrixWorld(true);
      for(const rest of fingerRest){
        const p=rest.finger.getWorldPosition(new THREE.Vector3()).applyMatrix4(rest.hand.matrixWorld.clone().invert());
        const towardPalm=p.sub(rest.position).dot(rest.palm);
        if(towardPalm<0.005)failures.push({file:relative,message:'Gripping index finger does not curl toward palm: '+rest.side+' '+towardPalm});
        fingerCurlTests.push({hand:rest.side,towardPalmDisplacement:towardPalm});
      }
      mixer.stopAllAction();for(const [b,p,q,s]of savedPose){b.position.copy(p);b.quaternion.copy(q);b.scale.copy(s);}scene.updateMatrixWorld(true);
    }
    if(relative.startsWith('models/')&&(base.size[1]<1.5||base.size[1]>2.7))throw Error('Player rest height outside adult football athlete bounds (meters): '+base.size[1]);
    if(textures.some(t=>!t.image||!t.image.width||!t.image.height))throw Error('Texture did not load with dimensions');
    const record={file:relative,loaded:true,threeRevision:THREE.REVISION,meshCount:meshes.length,skinnedMeshCount:meshes.filter(m=>m.isSkinnedMesh).length,boneCount:bones.length,materialCount:materials.size,textureCount:textures.length,decodedImages:decodedImages-imageCountBefore,restBounds:base,manifestGeometryComparison,bindPose,uniformScaleTests,morphs,animations:animationSamples,fingerCurlTests};
    assets.push(record);console.log('Three.js loaded '+relative);
  }catch(error){failures.push({file:relative,message:error.stack});}
}
const report={generatedAt:new Date().toISOString(),passed:failures.length===0,threeRevision:THREE.REVISION,scope:'Actual GLTFLoader import, PNG decompression, skeleton deformation, independent physique morphs, and five time samples per animation. This Node check does not create a GPU renderer or validate gameplay, contact/IK or device performance.',loadedAssets:assets.length,decodedImages,failures,assets};
fs.writeFileSync(path.join(reportDir,onlyPattern?'partial-three-load-smoke.json':'three-load-smoke.json'),JSON.stringify(report,null,2)+'\n');
console.log(JSON.stringify({passed:report.passed,loadedAssets:report.loadedAssets,decodedImages,failures},null,2));
process.exitCode=report.passed?0:1;
