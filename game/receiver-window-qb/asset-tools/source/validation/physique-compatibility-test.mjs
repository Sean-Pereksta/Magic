#!/usr/bin/env node
// Real Three.js r160 physique regression. Run after npm ci in this validation directory.
// node physique-compatibility-test.mjs [extracted-pack-directory]
import fs from 'node:fs';import path from 'node:path';import {fileURLToPath} from 'node:url';import assert from 'node:assert/strict';import * as THREE from 'three';import {GLTFLoader} from 'three/addons/loaders/GLTFLoader.js';import {clone} from 'three/addons/utils/SkeletonUtils.js';
const directory=path.dirname(fileURLToPath(import.meta.url));
function findPack(){let candidate=directory;while(path.dirname(candidate)!==candidate){if(fs.existsSync(path.join(candidate,'manifest.json')))return candidate;candidate=path.dirname(candidate);}throw Error('Pass the extracted pack directory as the first argument.');}
const root=path.resolve(process.argv[2]||findPack());
globalThis.self=globalThis;globalThis.ProgressEvent??=class ProgressEvent{constructor(type,init){Object.assign(this,init);}};
// Texture decoding/rendering is covered by three-load-smoke and the browser preview.
globalThis.createImageBitmap=async blob=>({width:512,height:512,close(){}});
const helper=path.join(directory,'physique-under-test.mjs');fs.writeFileSync(helper,fs.readFileSync(path.join(root,'preview/physique.js'),'utf8'));
const {createPhysiqueController}=await import(new URL('./physique-under-test.mjs?'+Date.now(),import.meta.url));
const manifest=JSON.parse(fs.readFileSync(path.join(root,'manifest.json'),'utf8')),entry=manifest.archetypes.find(a=>a.id==='qb_field_general');assert.ok(entry,'Reference QB exists in manifest');
const bytes=fs.readFileSync(path.join(root,entry.model));const gltf=await new Promise((resolve,reject)=>new GLTFLoader().parse(bytes.buffer.slice(bytes.byteOffset,bytes.byteOffset+bytes.byteLength),'',resolve,reject));
function inverseState(scene){const out=[];scene.traverse(o=>{if(o.isSkinnedMesh)for(const m of o.skeleton.boneInverses)out.push(...m.elements);});return out;}
function bounds(model){model.updateMatrixWorld(true);const b=new THREE.Box3(),p=new THREE.Vector3();model.traverse(o=>{if(o.isSkinnedMesh){o.skeleton.update();for(let i=0;i<o.geometry.attributes.position.count;i++){o.getVertexPosition(i,p);p.applyMatrix4(o.matrixWorld);b.expandByPoint(p);}}});return b;}
function pose(model){model.updateMatrixWorld(true);const out=[];model.traverse(o=>{if(o.isBone)out.push(...o.matrixWorld.elements);});return out;}
function positionTracks(clips){return clips.flatMap(c=>c.tracks.filter(t=>THREE.PropertyBinding.parseTrackName(t.name).propertyName==='position').flatMap(t=>Array.from(t.values)));}
function snapshot(model,clips){const out=[...model.scale.toArray(),...pose(model),...positionTracks(clips),...inverseState(model)];model.traverse(o=>{if(o.morphTargetInfluences)out.push(...o.morphTargetInfluences);});return out;}
function difference(a,b){assert.equal(a.length,b.length);let max=0;for(let i=0;i<a.length;i++)max=Math.max(max,Math.abs(a[i]-b[i]));return max;}
const templateInverses=inverseState(gltf.scene),templateTracks=positionTracks(gltf.animations);
const model=clone(gltf.scene),clips=gltf.animations.map(c=>c.clone()),second=clone(gltf.scene),secondClips=gltf.animations.map(c=>c.clone());
const controller=createPhysiqueController(model,clips,entry),secondController=createPhysiqueController(second,secondClips,entry);
controller.apply({strength:60});secondController.apply({strength:60});
const baseline=bounds(model),baselinePose=pose(model),baselineClips=positionTracks(clips),secondState=snapshot(second,secondClips);
const profile={strength:95,shoulders:1,chest:1,arms:1,legs:1,weight:1,reach:1,hands:1,extension:1};controller.apply(profile);const high=bounds(model),highPose=pose(model);controller.apply(profile);const repeatPose=pose(model);
assert.ok(difference(highPose,repeatPose)<1e-6,'Applying profile twice must not accumulate skeleton offsets');assert.ok(high.getSize(new THREE.Vector3()).x>baseline.getSize(new THREE.Vector3()).x+.1);
assert.equal(difference(secondState,snapshot(second,secondClips)),0,'Another clone is unchanged by physique rebinding');
assert.equal(difference(templateInverses,inverseState(gltf.scene)),0,'Cached source inverse-bind matrices are unchanged');assert.equal(difference(templateTracks,positionTracks(gltf.animations)),0,'Cached source animation tracks are unchanged');
let inverseError=0;model.traverse(o=>{if(o.isSkinnedMesh)o.skeleton.bones.forEach((bone,i)=>{const m=bone.matrixWorld.clone().multiply(o.skeleton.boneInverses[i]);inverseError=Math.max(inverseError,difference(m.elements,new THREE.Matrix4().elements));});});assert.ok(inverseError<1e-5);
for(const heightM of ['invalid',NaN,Infinity,-Infinity]){const before=snapshot(model,clips);assert.throws(()=>controller.apply({...profile,heightM}));assert.equal(difference(before,snapshot(model,clips)),0,'Invalid height cannot partially change an athlete');}
controller.apply({strength:60});const resetPose=pose(model),clipResetError=difference(positionTracks(clips),baselineClips);assert.ok(difference(baselinePose,resetPose)<1e-6,'Reset returns bind rest');assert.ok(clipResetError<1e-6,'Reset returns original animation translations');
controller.apply({...profile,heightM:2.2});const tall=bounds(model),baseHeight=Number(entry.heightM??entry.measurements.height_m);assert.ok(Math.abs(tall.getSize(new THREE.Vector3()).y-high.getSize(new THREE.Vector3()).y*2.2/baseHeight)<1e-4,'Uniform height applies to whole visible athlete');
function localVertices(actor){actor.updateMatrixWorld(true);const inverse=actor.matrixWorld.clone().invert(),out=[],v=new THREE.Vector3();actor.traverse(o=>{if(o.isSkinnedMesh){o.skeleton.update();for(let i=0;i<o.geometry.attributes.position.count;i++){o.getVertexPosition(i,v);v.applyMatrix4(o.matrixWorld).applyMatrix4(inverse);out.push(v.x,v.y,v.z);}}});return out;}
function localBones(actor){actor.updateMatrixWorld(true);const inverse=actor.matrixWorld.clone().invert(),out=[];actor.traverse(o=>{if(o.isBone)out.push(...inverse.clone().multiply(o.matrixWorld).elements);});return out;}
controller.apply({...profile,heightM:2.2});const actorLocal=localVertices(model),anchorsLocal=localBones(model);
const outer=new THREE.Group(),wrapper=new THREE.Group();outer.position.set(-3,2,5);outer.rotation.set(.08,-.3,.12);outer.scale.set(1.15,.8,1.05);wrapper.position.set(7,.2,-4);wrapper.rotation.set(.1,.65,-.05);wrapper.scale.set(.8,1.1,1.2);outer.add(wrapper);wrapper.add(model);model.position.set(4,.5,-3);model.rotation.y=.7;outer.updateMatrixWorld(true);
const placedBefore=localVertices(model);controller.apply({...profile,heightM:2.2});outer.updateMatrixWorld(true);const placedAfter=localVertices(model),placedAnchors=localBones(model);
const placementVertexError=difference(actorLocal,placedAfter),placementBeforeError=difference(placedBefore,placedAfter),placementAnchorError=difference(anchorsLocal,placedAnchors);
assert.ok(placementVertexError<1e-5,'Translated, rotated and scaled wrappers preserve the athlete local visible shape');assert.ok(placementBeforeError<1e-5,'Applying the same profile after placement does not move visible skin');assert.ok(placementAnchorError<1e-5,'Placed skeletal contact anchors preserve their model-local measurements');
controller.apply({...profile,heightM:1.6});const placedShort=localVertices(model);controller.apply({...profile,heightM:2.2});assert.ok(difference(actorLocal,localVertices(model))<1e-5,'Stature changes after placement do not accumulate shape/bind offsets');assert.equal(difference(secondState,snapshot(second,secondClips)),0,'Placed rebinding leaves another actor unchanged');
const report={passed:true,scope:'CPU model/rig/morph/animation ownership checks; no gameplay or GPU performance claims.',file:entry.model,boneCount:controller.bones.size,baseline:baseline.getSize(new THREE.Vector3()).toArray(),profile:high.getSize(new THREE.Vector3()).toArray(),heightScale:tall.getSize(new THREE.Vector3()).toArray(),repeatError:difference(highPose,repeatPose),resetError:difference(baselinePose,resetPose),clipResetError,inverseError,independentClones:true,invalidHeightNonmutation:true,placementVertexError,placementBeforeError,placementAnchorError};
fs.writeFileSync(path.join(root,'validation/physique-compatibility.json'),JSON.stringify(report,null,2)+'\n');console.log(JSON.stringify(report,null,2));fs.unlinkSync(helper);
