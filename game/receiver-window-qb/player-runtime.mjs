import * as THREE from 'three';
import {GLTFLoader} from 'three/addons/loaders/GLTFLoader.js';
import {clone as cloneSkeleton} from 'three/addons/utils/SkeletonUtils.js';
import {OrbitControls} from 'three/addons/controls/OrbitControls.js';
import {createPhysiqueController} from './asset-tools/preview/physique.js';
import './player-assets.js';
import './player-scale.js';

// Imported surfaces, bones and contact anchors share the fallback's larger
// display scale. Root appearance scale is still cancelled to avoid applying
// either stature or the presentation multiplier twice.
export const WORLD_UNITS_PER_METRE=globalThis.QBPlayerScale.worldUnitsPerMetre;
export const DEFAULT_ASSET_BASE_URL='https://pub-d799b6ea742b40eda80627485e6b234a.r2.dev';
const clamp=(n,a,b)=>Math.max(a,Math.min(b,Number(n)||0));
const originalName=b=>b.userData.name||b.name;
function ownedClone(template){
  const scene=cloneSkeleton(template.scene),materials=new Map(),skeletons=new Set();
  scene.traverse(o=>{
    if(o.skeleton&&!skeletons.has(o.skeleton)){skeletons.add(o.skeleton);o.skeleton.boneInverses=o.skeleton.boneInverses.map(m=>m.clone());}
    if(o.geometry){o.geometry.userData.sharedPlayer=true;o.castShadow=true;o.receiveShadow=true;}
    const copy=m=>{if(!m)return m;if(!materials.has(m))materials.set(m,m.clone());return materials.get(m);};
    if(o.material)o.material=Array.isArray(o.material)?o.material.map(copy):copy(o.material);
  });
  const animations=template.animations.map(clip=>clip.clone());
  return {scene,animations,materials};
}
function actorHeight(profile,entry){
  const value=Number(profile?.physical?.heightM??profile?.heightM??profile?.measurements?.heightM??(profile?.heightCm?profile.heightCm/100:undefined)??entry.heightM??entry.measurements?.height_m);
  return clamp(value||1.88,1.55,2.2);
}
function selectArchetype(profile={},offense=true,options={}){
  profile=profile||{};
  if(profile.modelId||profile.modelSelection||profile.physique?.modelId)return profile.modelId||profile.modelSelection||profile.physique.modelId;
  const position=String(profile.position||options.position||'').toLowerCase(),arch=String(profile.archetype||'').toLowerCase();
  const h=Number(profile.heightM||(profile.heightCm?profile.heightCm/100:0));
  if(!offense){if(position==='lb'||position.includes('line')||options.kind==='linebacker')return 'linebacker_coverage';if(['s','fs','ss'].includes(position)||position.includes('safe')||options.kind==='safety')return 'safety_hybrid';return Number(profile.strength)>80?'cb_press':'cb_ballhawk';}
  if(position==='qb')return Number(profile.speed)>80?'qb_mobile':'qb_field_general';
  if(position==='rb')return Number(profile.strength)>80?'rb_power':'rb_elusive';
  if(position==='te')return Number(profile.strength)>85?'te_blocking':'te_receiving';
  if(h>=1.97||arch.includes('jump'))return 'wr_jump_ball';
  if(Number(profile.strength)>=86||arch.includes('power')||arch.includes('physical'))return 'wr_physical';
  if(Number(profile.speed)>=82||arch.includes('burner')||arch.includes('deep'))return 'wr_speed';
  if(Number(profile.catching)>=84||arch.includes('possession')||arch.includes('sure'))return 'wr_possession';
  if(Number(profile.cutting)>=80||arch.includes('route')||arch.includes('comeback'))return 'wr_route_technician';
  return 'wr_slot';
}
function teamColors(scene,look={}){
  scene.traverse(o=>{for(const material of Array.isArray(o.material)?o.material:[o.material]){
    if(!material?.color)continue;const name=material.name.toLowerCase();let color;
    if(name.includes('jersey'))color=look.jerseyPrimary;
    else if(name.includes('helmet'))color=look.helmetColor;
    else if(name.includes('pants'))color=look.pantsColor;
    else if(name.includes('glove'))color=look.gloveColor||look.jerseyAccent;
    else if(name.includes('cleat'))color=look.cleatColor;
    else if(name==='skin')color=look.skinColor;
    else if(name.includes('accent')||name.includes('seams'))color=look.jerseyAccent;
    else if(name.includes('number'))color=look.numberColor;
    if(color)material.color.set(color);
  }});
}
function gameplayClips(clips){
  return clips.map(source=>{
    const clip=source.clone(),prone=/div|fall|recover|tackle/.test(clip.name);
    // Horizontal displacement belongs to game physics. Retain authored prone
    // grounding; jumps use the actor's actual ballistic height exactly once.
    for(const track of clip.tracks){const b=THREE.PropertyBinding.parseTrackName(track.name);if(b.nodeName==='root'&&b.propertyName==='position')for(let i=0;i<track.values.length;i+=3){track.values[i]=0;track.values[i+2]=0;if(!prone)track.values[i+1]=0;}}
    return clip;
  });
}
function animationFor(a){
  if(a.celebrating||a.celebrationTime>0)return 'celebration_touchdown';
  if(a.qbRelease||a.throwPose>0)return 'qb_release';
  const technique=a.passTechnique;
  if(a.passTechniqueState?.phase==='recovery'||a.diveRecovery>0)return 'recovery_get_up';
  if(technique){
    if(technique.dive)return technique.canIntercept?'interception_dive':'defense_dive_pbu';
    const names={standing_swat:'defense_standing_swat',jump_swat:'defense_jump_swat',dive_swat:'defense_dive_pbu',interception:'interception_snatch',undercut:'interception_undercut',trailing_breakup:'defense_trailing_breakup',late_disruption:'defense_trailing_breakup',tip_drill:'defense_tip_drill'};
    if(names[technique.technique])return names[technique.technique];
  }
  if(a.catchDive)return 'catch_dive';
  if(a.diveTime>0)return 'tackle_wrap';
  if(a.finishPose)return a.finishPose.wrapTarget?'tackle_wrap':'fall';
  if(a.hasBall&&a.catchStyleTime>0){const names={'HIGH POINT':'catch_high_point','ONE HAND':'catch_one_hand','BACK SHOULDER':'catch_back_shoulder','OVER THE SHOULDER':'catch_over_shoulder','TOE TAP':'catch_toe_tap'};return names[a.catchStyle]||'catch_two_hand';}
  if((a.swatPose||0)>.3)return a.jumpY>.08?'defense_jump_swat':'defense_standing_swat';
  if((a.catchPose||0)>.25||a.handTargets)return a.catchOneHand?'catch_one_hand':a.jumpY>.08?'catch_high_point':'catch_two_hand';
  if(a.comebackPlant>0)return a.visualCutSide>0?'plant_right':'plant_left';
  if((a.plantPose||0)>.45)return a.visualCutSide>0?'route_cut_right':'route_cut_left';
  if((a.runIntensity||0)>.78)return 'sprint';
  if((a.runIntensity||0)>.30)return 'jog';
  if((a.runIntensity||0)>.05)return 'walk';
  return 'idle_ready';
}
function rotateWorld(bone,direction,to){
  if(direction.lengthSq()<1e-12||to.lengthSq()<1e-12)return;
  const delta=new THREE.Quaternion().setFromUnitVectors(direction.normalize(),to.normalize());
  const world=bone.getWorldQuaternion(new THREE.Quaternion()).premultiply(delta);
  const parent=bone.parent.getWorldQuaternion(new THREE.Quaternion()).invert();
  bone.quaternion.copy(parent.multiply(world));bone.updateMatrixWorld(true);
}
function solveArm(upper,lower,hand,target,side){
  const shoulder=upper.getWorldPosition(new THREE.Vector3()),elbow=lower.getWorldPosition(new THREE.Vector3()),wrist=hand.getWorldPosition(new THREE.Vector3());
  const u=shoulder.distanceTo(elbow),l=elbow.distanceTo(wrist),delta=target.clone().sub(shoulder),distance=clamp(delta.length(),.005,u+l-.0001);
  if(!(u>0&&l>0))return;
  delta.normalize();const endpoint=shoulder.clone().addScaledVector(delta,distance),along=(u*u-l*l+distance*distance)/(2*distance);
  let bend=elbow.clone().sub(shoulder);bend.addScaledVector(delta,-bend.dot(delta));
  if(bend.lengthSq()<1e-8)bend.set(side*.5,-.5,-.4).addScaledVector(delta,-delta.dot(new THREE.Vector3(side*.5,-.5,-.4)));
  const wanted=shoulder.clone().addScaledVector(delta,along).addScaledVector(bend.normalize(),Math.sqrt(Math.max(0,u*u-along*along)));
  rotateWorld(upper,elbow.clone().sub(shoulder),wanted.clone().sub(shoulder));
  const newElbow=lower.getWorldPosition(new THREE.Vector3()),newWrist=hand.getWorldPosition(new THREE.Vector3());
  rotateWorld(lower,newWrist.sub(newElbow),endpoint.sub(newElbow));
}
function contactAnchors(model,bones){
  const boxes=new Map(),skeletons=new Set();model.updateMatrixWorld(true);
  model.traverse(o=>{if(o.skeleton)skeletons.add(o.skeleton);});for(const s of skeletons)s.update();
  const wanted=n=>/^(hand|upper_arm|forearm)\.[LR]$/.test(n)||/^(thumb|index|middle|ring|pinky)_\d+\.[LR]$/.test(n);
  const inverses=new Map([...bones].filter(([n])=>wanted(n)).map(([n,b])=>[n,b.matrixWorld.clone().invert()]));
  model.traverse(mesh=>{
    if(!mesh.isSkinnedMesh)return;const {position,skinIndex,skinWeight}=mesh.geometry.attributes;if(!skinIndex||!skinWeight)return;
    const v=new THREE.Vector3();
    for(let i=0;i<position.count;i++){
      const weights=[skinWeight.getX(i),skinWeight.getY(i),skinWeight.getZ(i),skinWeight.getW(i)],indices=[skinIndex.getX(i),skinIndex.getY(i),skinIndex.getZ(i),skinIndex.getW(i)];
      const index=indices[weights.indexOf(Math.max(...weights))],name=originalName(mesh.skeleton.bones[index]);if(!inverses.has(name))continue;
      mesh.getVertexPosition(i,v);v.applyMatrix4(mesh.matrixWorld).applyMatrix4(inverses.get(name));
      if(!boxes.has(name))boxes.set(name,new THREE.Box3());boxes.get(name).expandByPoint(v);
    }
  });
  const result={hands:[],arms:[],forearms:[],fingers:[]};
  for(const side of ['R','L']){
    const definitions=[['hand.'+side,'hands'],['upper_arm.'+side,'arms'],['forearm.'+side,'forearms']];
    for(const [name]of bones)if(/^(thumb|index|middle|ring|pinky)_\d+\./.test(name)&&name.endsWith('.'+side))definitions.push([name,'fingers']);
    for(const [name,kind]of definitions){
      const box=boxes.get(name),bone=bones.get(name);if(!box||!bone)continue;
      const size=box.getSize(new THREE.Vector3()),anchor=new THREE.Object3D();anchor.name='Contact_'+name;anchor.position.copy(box.getCenter(new THREE.Vector3()));
      // An inscribed capsule measured from the actual bound surface. In
      // particular a palm is not the old .105-world-unit procedural sphere.
      const radius=Math.max(.001,Math.min(size.x,size.z,size.y)*.40);
      anchor.userData={contactRadius:radius,boneLength:Math.max(radius*2,size.y),contactHand:kind==='hands'||kind==='fingers',contactForearm:kind==='forearms',contactSide:side==='R'?0:1,sourceBone:name,actualGLB:true};
      bone.add(anchor);result[kind].push(anchor);
    }
  }
  return result;
}
export function createPlayerRuntime(options={}){
  const THREERevision=String(THREE.REVISION),loader=options.gltfLoader||new GLTFLoader();loader.setCrossOrigin?.('anonymous');
  const assetLoader=globalThis.QBPlayerAssets.create({baseURL:options.baseURL||DEFAULT_ASSET_BASE_URL,fetch:options.fetch||globalThis.fetch,
    loadGLTF:options.loadGLTF||((url)=>loader.loadAsync(url)),clone:cloneSkeleton,maxTemplates:32});
  const templates=new Map(),controllers=new Set(),pending=new Map(),errors=[];
  const runtime={status:'loading',manifest:null,quality:options.quality||'low',errors,THREERevision,worldUnitsPerMetre:WORLD_UNITS_PER_METRE};
  function progress(done,total){
    runtime.loaded=templates.size;runtime.total=total;
    const element=globalThis.document?.getElementById('graphicsStatus');
    const message=done<total?`Loading football players · ${done}/${total}`:templates.size===total?`Detailed football graphics ready · ${templates.size}/${total} players`:`Football graphics · ${templates.size}/${total} players loaded; ${errors.length} load failures use lightweight graphics`;
    if(element)element.textContent=message;options.onProgress?.({done,total,loaded:templates.size,failed:errors.length,message});
  }
  async function preload(){
    runtime.manifest=await assetLoader.getManifest();
    const list=runtime.manifest.assets.filter(a=>!options.ids||options.ids.includes(a.id));
    let cursor=0,done=0;progress(0,list.length);
    await Promise.all(Array.from({length:Math.min(4,list.length)},async()=>{while(cursor<list.length){const entry=list[cursor++];try{const instance=await assetLoader.load(entry.id,{lod:runtime.quality==='high'?'high':'low'});templates.set(entry.id,instance);}catch(error){errors.push({id:entry.id,message:String(error.message||error)});}finally{progress(++done,list.length);}}}));
    runtime.status=templates.size?'ready':'fallback';if(!templates.size)throw new Error(errors[0]?.message||'Football GLBs could not load.');
    return runtime;
  }
  function attach(rootMesh,profile={},attachOptions={}){
    profile=profile||{};
    if(rootMesh.userData.assetController)return rootMesh.userData.assetController;
    const id=selectArchetype(profile,attachOptions.offense!==false,attachOptions),template=templates.get(id);
    if(!template){rootMesh.userData.assetFailure=errors.find(e=>e.id===id)?.message||'Requested football GLB failed to preload: '+id;return null;}
    const instance=ownedClone(template),model=instance.scene,entry=runtime.manifest.assets.find(a=>a.id===id);
    const physique=createPhysiqueController(model,instance.animations,entry),physical=profile.physical||{};
    physique.apply({shoulders:clamp((Number(physical.shoulderWidthM)/Number(entry.measurements?.shoulder_joint_width_m)-1)/.12,0,1),
      chest:physical.chestSize,arms:physical.armThickness,legs:physical.legPower,
      reach:clamp((Number(physical.wingspanRatio)-1.03)/.09,0,1),hands:clamp((Number(physical.handRatio)-1)/.20,0,1),
      weight:clamp((Number(physical.weightKg)-Number(entry.weightKg))/30,0,1),
      ...profile.physique,strength:profile.strength??60,heightM:actorHeight(profile,entry)});
    const holder=new THREE.Group();holder.name='FootballGLB_'+id;
    // Legacy appearance scale continues to serve the old actor/controller, but
    // must not apply stature twice to the measured metre-based athlete.
    holder.scale.set(WORLD_UNITS_PER_METRE/Math.max(.01,rootMesh.scale.x),WORLD_UNITS_PER_METRE/Math.max(.01,rootMesh.scale.y),WORLD_UNITS_PER_METRE/Math.max(.01,rootMesh.scale.z));
    holder.add(model);rootMesh.add(holder);
    teamColors(model,rootMesh.userData.appearance);
    const bones=new Map();model.traverse(b=>{if(b.isBone)bones.set(originalName(b),b);});
    const contacts=contactAnchors(model,bones),contactLimbs=[...contacts.hands,...contacts.arms,...contacts.forearms,...contacts.fingers];
    if(contacts.hands.length!==2||contacts.arms.length!==2||contacts.forearms.length!==2||contacts.fingers.length<28){holder.removeFromParent();for(const m of instance.materials.values())m.dispose();throw new Error('Loaded GLB has incomplete physical hand/arm chains.');}
    const clips=gameplayClips(instance.animations),mixer=new THREE.AnimationMixer(model),actions=new Map(clips.map(c=>[c.name,mixer.clipAction(c)]));
    const legacy=rootMesh.userData.visualRig,legacyVisibility=legacy?.visible;if(legacy)legacy.visible=false;
    let current=null,disposed=false;const tracked=[null,null];
    function play(name){
      const next=actions.get(name)||actions.get('idle_ready');if(!next||next===current)return;
      next.reset();const loop=/^(idle_ready|walk|jog|sprint|defense_backpedal)$/.test(name);
      next.setLoop(loop?THREE.LoopRepeat:THREE.LoopOnce,loop?Infinity:1);next.clampWhenFinished=!loop;next.enabled=true;next.play();
      if(current)current.crossFadeTo(next,.13,false);current=next;
    }
    function updateContact(){rootMesh.updateMatrixWorld(true);for(const mesh of instance.scene.children)mesh.updateMatrixWorld?.(true);}
    function pose(actor,dt=0){
      if(disposed)return;dt=clamp(dt,0,.1);play(animationFor(actor));mixer.update(dt);model.updateMatrixWorld(true);
      const active=!!(actor.handTargets||actor.hasBall||actor.catchDive||(actor.catchPose||0)>.15||(actor.swatPose||0)>.15||(actor.blockPose||0)>.1||actor.stiffArmTime>0);
      if(active){
        const desired=rootMesh.userData.hands||[];
        for(let i=0;i<2;i++){
          const side=i===0?'R':'L',hand=bones.get('hand.'+side),palm=contacts.hands[i],upper=bones.get('upper_arm.'+side),lower=bones.get('forearm.'+side);
          if(!desired[i]||!hand||!upper||!lower)continue;
          const target=desired[i].getWorldPosition(new THREE.Vector3()),palmPosition=palm.getWorldPosition(new THREE.Vector3());
          if(!tracked[i])tracked[i]=palmPosition.clone();const delta=target.sub(tracked[i]),step=dt*(actor.profile?11:8);
          if(delta.length()>step)delta.setLength(step);tracked[i].add(delta);
          const wrist=hand.getWorldPosition(new THREE.Vector3()),offset=palmPosition.sub(wrist),wristTarget=tracked[i].clone().sub(offset);
          solveArm(upper,lower,hand,wristTarget,i===0?-1:1);
        }
      }else{tracked[0]=null;tracked[1]=null;}
      updateContact();
    }
    function release(){
      if(disposed)return;disposed=true;mixer.stopAllAction();mixer.uncacheRoot(model);holder.removeFromParent();
      for(const m of instance.materials.values())m.dispose();const textures=new Set();model.traverse(o=>{if(o.skeleton?.boneTexture)textures.add(o.skeleton.boneTexture);});for(const t of textures)t.dispose();
      if(legacy)legacy.visible=legacyVisibility;delete rootMesh.userData.assetController;delete rootMesh.userData.assetContact;controllers.delete(controller);
    }
    const controller={id,model,holder,bones,clips,mixer,physique,contactLimbs,contacts,pose,updateContact,release,
      get activeClip(){return current?.getClip().name||null;},get loaded(){return !disposed;}};
    Object.defineProperty(rootMesh.userData,'assetController',{value:controller,writable:true,configurable:true,enumerable:false});
    Object.defineProperty(rootMesh.userData,'assetContact',{value:contacts,writable:true,configurable:true,enumerable:false});
    controllers.add(controller);play('idle_ready');mixer.update(0);updateContact();return controller;
  }
  runtime.attach=attach;runtime.release=root=>root?.userData?.assetController?.release();
  runtime.cloneForReplay=root=>{
    const copy=cloneSkeleton(root),seen=new Set();copy.traverse(o=>{if(o.skeleton&&!seen.has(o.skeleton)){seen.add(o.skeleton);o.skeleton.boneInverses=o.skeleton.boneInverses.map(m=>m.clone());}});return copy;
  };
  runtime.dispose=()=>{for(const c of [...controllers])c.release();for(const t of templates.values())t.release();templates.clear();assetLoader.dispose();runtime.status='disposed';};
  runtime.ready=preload().catch(error=>{runtime.status='fallback';runtime.error=String(error.message||error);const status=globalThis.document?.getElementById('graphicsStatus');if(status)status.textContent='Football graphics could not load. Lightweight graphics are available; reload to retry.';return runtime;});
  runtime.getArchetype=selectArchetype;
  runtime.preview=(host,players=[])=>{
    if(!host||typeof document==='undefined')return ()=>{};
    let closed=false,cleanup=()=>{};host.textContent='Preparing 3D team preview…';
    function build(){
      if(closed)return;if(runtime.status!=='ready'){host.textContent='3D preview is unavailable because football assets could not load.';return;}
      const list=players.filter(Boolean).slice(0,12);if(!list.length){host.textContent='Select athletes to preview.';return;}
      const canvas=document.createElement('canvas');canvas.style.cssText='width:100%;height:340px;display:block;touch-action:none;border-radius:12px';canvas.setAttribute('aria-label','Interactive 3D football team preview');
      const labels=document.createElement('div');labels.style.cssText='display:flex;gap:12px;justify-content:space-around;flex-wrap:wrap;padding:10px';
      for(const p of list){const label=document.createElement('span');label.textContent=`${p.name||p.position||'Athlete'} · ${Math.round(actorHeight(p,{heightM:1.88})*100)} cm · STR ${p.strength||60}`;labels.append(label);}
      const help=document.createElement('p');help.textContent='Drag to rotate. Scroll or pinch to zoom. All athletes use the same scale.';host.replaceChildren(canvas,labels,help);
      const renderer=new THREE.WebGLRenderer({canvas,antialias:true,alpha:true});renderer.setPixelRatio(Math.min(globalThis.devicePixelRatio||1,1.5));renderer.outputColorSpace=THREE.SRGBColorSpace;renderer.toneMapping=THREE.ACESFilmicToneMapping;
      const scene=new THREE.Scene();scene.background=new THREE.Color(0x101c2c);scene.add(new THREE.HemisphereLight(0xe9f6ff,0x29333e,2.5));const light=new THREE.DirectionalLight(0xffffff,3);light.position.set(3,6,5);scene.add(light);
      const spacing=1.45*globalThis.QBPlayerScale.actorScale,width=Math.max(3,list.length*spacing),camera=new THREE.PerspectiveCamera(33,1,.05,100);camera.position.set(.4,2.1,Math.max(4,width*.95));
      const controls=new OrbitControls(camera,canvas);controls.target.set(0,1.25*globalThis.QBPlayerScale.actorScale,0);controls.enableDamping=true;controls.minDistance=2.8;controls.maxDistance=32;controls.maxPolarAngle=Math.PI*.51;
      const floor=new THREE.Mesh(new THREE.PlaneGeometry(width+3,6),new THREE.MeshStandardMaterial({color:0x233342,roughness:.9}));floor.rotation.x=-Math.PI/2;floor.position.y=-.015;scene.add(floor);
      const actors=[];for(let i=0;i<list.length;i++){const p=list[i],root=new THREE.Group();root.position.x=(i-(list.length-1)/2)*spacing;root.userData.appearance=p.appearance||{};scene.add(root);const controller=attach(root,p,{offense:!['CB','S','LB'].includes(p.position)});if(controller)actors.push({mesh:root,profile:p,runIntensity:0,controller});}
      const resize=()=>{const w=Math.max(1,host.clientWidth||600);renderer.setSize(w,340,false);camera.aspect=w/340;camera.position.z=Math.max(4.8,width/(2*Math.tan(33*Math.PI/360)*camera.aspect)*1.12);camera.updateProjectionMatrix();};resize();const observer=typeof ResizeObserver==='function'?new ResizeObserver(resize):null;observer?.observe(host);
      let frame=0,last=performance.now();const tick=now=>{if(closed)return;const dt=Math.min(.05,(now-last)/1000);last=now;for(const a of actors)a.controller.pose(a,dt);controls.update();renderer.render(scene,camera);frame=requestAnimationFrame(tick);};frame=requestAnimationFrame(tick);
      cleanup=()=>{cancelAnimationFrame(frame);observer?.disconnect();controls.dispose();for(const a of actors)runtime.release(a.mesh);floor.geometry.dispose();floor.material.dispose();renderer.dispose();renderer.forceContextLoss();};
    }
    runtime.ready.then(()=>{try{build();}catch(error){if(!closed)host.textContent='3D preview could not start: '+String(error.message||error);}});
    return ()=>{if(closed)return;closed=true;cleanup();};
  };
  return runtime;
}
export async function boot(state=globalThis.QBGraphics||{},options={}){
  const runtime=createPlayerRuntime(options);state.status='loading';state.attach=(...args)=>runtime.attach(...args);state.release=root=>runtime.release(root);state.cloneForReplay=root=>runtime.cloneForReplay(root);state.preview=(...args)=>runtime.preview(...args);state.runtime=runtime;
  await runtime.ready;state.status=runtime.status;state.error=runtime.error||null;return state;
}
