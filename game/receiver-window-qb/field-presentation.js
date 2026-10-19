/* Static field dressing. Existing presentation.js owns the crowd/benches/scoreboard.
 * No animation loop, downloads, simulation changes or per-frame allocations.
 */
(function(root){
'use strict';
const sharedTextures=new Map();
const defaults=Object.freeze({startZ:40,goalZ:-60,endZ:-70,halfWidth:26,unitsPerYard:2});
const bounded=(value,fallback,min,max)=>Number.isFinite(value)?Math.max(min,Math.min(max,value)):fallback;
function normalizeWorld(raw={}){
 if(!raw||typeof raw!=='object')raw={};
 const startZ=bounded(raw.startZ,defaults.startZ,-10000,10000),unitsPerYard=bounded(raw.unitsPerYard,2,.25,20);
 const goalZ=Number.isFinite(raw.goalZ)&&raw.goalZ<=startZ-unitsPerYard?Math.max(startZ-unitsPerYard*200,raw.goalZ):startZ-50*unitsPerYard;
 return {startZ,goalZ,endZ:Number.isFinite(raw.endZ)&&raw.endZ<goalZ?Math.max(goalZ-20*unitsPerYard,raw.endZ):goalZ-5*unitsPerYard,halfWidth:bounded(raw.halfWidth,26,4,150),unitsPerYard};
}
function rng(seed){return ()=>{seed=(Math.imul(seed,1664525)+1013904223)>>>0;return seed/4294967296;};}
function canvasTexture(T,factory,width,height,draw){
 const canvas=factory?.();if(!canvas)return null;canvas.width=width;canvas.height=height;const context=canvas.getContext?.('2d');if(!context)return null;draw(context,width,height);
 const texture=new T.CanvasTexture(canvas);texture.colorSpace=T.SRGBColorSpace;texture.wrapS=texture.wrapT=T.ClampToEdgeWrapping;texture.magFilter=T.LinearFilter;texture.minFilter=T.LinearMipmapLinearFilter;texture.anisotropy=1;return {texture,canvas,context};
}
function acquire(T,factory,key,width,height,draw){
 let entry=sharedTextures.get(key);if(!entry){const made=canvasTexture(T,factory,width,height,draw);if(!made)return null;entry={...made,refs:0};sharedTextures.set(key,entry);}entry.refs++;return entry;
}
function release(key){const entry=sharedTextures.get(key);if(entry&&--entry.refs===0){entry.texture.dispose();sharedTextures.delete(key);}}
function turfPaint(c,w,h){
 // Neutral fibres multiply the actual team/venue turf tint. Seed is fixed:
 // switching themes and quality never makes the field shimmer or reroll.
 const random=rng(0x51424647);c.fillStyle='#d0d5cc';c.fillRect(0,0,w,h);
 for(let stripe=0;stripe<13;stripe++){c.fillStyle=stripe%2?'rgba(48,68,42,.075)':'rgba(255,255,241,.045)';c.fillRect(0,stripe*h/13,w,h/13+.5);}
 for(let i=0;i<8500;i++){
  const x=random()*w,y=random()*h,length=.7+random()*3.1;c.strokeStyle=i%3?'rgba(34,55,27,.055)':'rgba(255,255,239,.085)';c.lineWidth=.45+random()*.6;c.beginPath?.();c.moveTo?.(x,y);c.lineTo?.(x+(random()-.5)*1.3,y-length);c.stroke?.();
 }
 for(let i=0;i<180;i++){const x=random()*w,y=random()*h;c.fillStyle='rgba(54,67,37,.025)';c.fillRect(x,y,2+random()*7,.5+random()*2);}
}
function numberPaint(c,w,h){
 c.clearRect(0,0,w,h);c.textAlign='center';c.textBaseline='middle';c.font='900 166px Arial, sans-serif';c.lineWidth=3;c.strokeStyle='rgba(25,47,33,.28)';c.fillStyle='#faf8eb';
 for(let i=0;i<5;i++){const x=(i+.5)*w/5;c.strokeText?.(String((i+1)*10),x,h*.51,w/5-26);c.fillText(String((i+1)*10),x,h*.51,w/5-26);}
}
function create({THREE:T,scene,turf,endzone,quality='high',world={},canvasFactory}={}){
 if(!T||!scene?.add)throw new Error('QBFieldGraphics requires THREE and a scene.');
 const field=normalizeWorld(world),doc=world?.document||root.document,factory=canvasFactory||(()=>doc?.createElement?.('canvas'));
 const group=new T.Group();group.name='QBFieldGraphics';group.userData.staticField=true;scene.add(group);
 const essentials=new T.Group(),detail=new T.Group();essentials.name='Field paint and boundaries';detail.name='Field finishing details';group.add(essentials,detail);
 const geometries=new Set(),materials=new Set(),textures=new Set(),instanceMeshes=new Set(),sharedKeys=[];let disposed=false,qualityLevel='high',wordKey='';
 const ownGeo=geometry=>{geometries.add(geometry);return geometry;},ownMat=material=>{materials.add(material);return material;};
 const boxGeometry=ownGeo(new T.BoxGeometry(1,1,1)),planeGeometry=ownGeo(new T.PlaneGeometry(1,1));
 const paint=ownMat(new T.MeshStandardMaterial({color:0xf0f0dc,roughness:1,metalness:0}));
 const accent=ownMat(new T.MeshStandardMaterial({color:0x37688a,roughness:.94})),concrete=ownMat(new T.MeshStandardMaterial({color:0xb4b5ae,roughness:1}));
 const orange=ownMat(new T.MeshStandardMaterial({color:0xf5852b,roughness:.93})),black=ownMat(new T.MeshStandardMaterial({color:0x263237,roughness:1}));
 const endAccent=ownMat(new T.MeshStandardMaterial({color:0xf1f4e6,roughness:.98,transparent:true,opacity:.24,depthWrite:false,polygonOffset:true,polygonOffsetFactor:-1}));
 const setTransform=(object,x,y,z,sx,sy,sz)=>{object.position.set(x,y,z);object.scale.set(sx,sy,sz);object.updateMatrix();};
 function instances(name,rows,material,parent=essentials){
  const mesh=new T.InstancedMesh(boxGeometry,material,rows.length),dummy=new T.Object3D();mesh.name=name;instanceMeshes.add(mesh);
  rows.forEach((r,i)=>{setTransform(dummy,...r);mesh.setMatrixAt(i,dummy.matrix);});mesh.instanceMatrix.needsUpdate=true;mesh.receiveShadow=true;mesh.castShadow=false;mesh.computeBoundingBox?.();mesh.computeBoundingSphere?.();parent.add(mesh);return mesh;
 }
 const span=field.startZ-field.goalZ,extendedMid=(field.startZ+field.endZ)/2,depth=field.goalZ-field.endZ,centerEnd=(field.endZ+field.goalZ)/2;
 const edges=[];for(const side of [-1,1])edges.push([side*(field.halfWidth+.47),.023,extendedMid,.68,.028,span+depth]);
 edges.push([0,.024,field.goalZ,field.halfWidth*2+.8,.03,.32],[0,.024,field.endZ,field.halfWidth*2+.8,.03,.32]);
 instances('Broad painted boundary apron',edges,paint);
 const outer=[];for(const side of [-1,1])outer.push([side*(field.halfWidth+.96),.023,extendedMid,.14,.03,span+depth]);instances('Team sideline pinstripes',outer,accent);
 const hashRows=[],fineRows=[];const count=Math.floor(span/field.unitsPerYard);
 for(let yard=1;yard<count;yard++){
  if(yard%5===0)continue;const z=field.goalZ+yard*field.unitsPerYard;
  for(const side of [-1,1]){hashRows.push([side*Math.min(9,field.halfWidth*.35),.025,z,.72,.025,.105]);fineRows.push([side*(field.halfWidth-.46),.025,z,.64,.025,.105]);}
 }
 const hashes=instances('One-yard interior hash marks',hashRows,paint),sidelineHashes=instances('One-yard sideline ticks',fineRows,paint,detail);
 const turfEntry=acquire(T,factory,'turf-v1',512,1024,turfPaint),numberEntry=acquire(T,factory,'yard-numbers-v1',1024,256,numberPaint);
 if(turfEntry)sharedKeys.push('turf-v1');if(numberEntry)sharedKeys.push('yard-numbers-v1');
 const originalTurf=turf?.material,originalEndzone=endzone?.material;
 const turfMaterial=originalTurf?.clone?ownMat(originalTurf.clone()):null,endzoneMaterial=originalEndzone?.clone?ownMat(originalEndzone.clone()):null;
 if(turfMaterial){turf.material=turfMaterial;if(turfEntry)turfMaterial.map=turfEntry.texture;turfMaterial.roughness=.99;turfMaterial.metalness=0;turfMaterial.needsUpdate=true;}
 if(endzoneMaterial){endzone.material=endzoneMaterial;endzoneMaterial.roughness=.96;endzoneMaterial.metalness=0;}
 if(numberEntry){
  const vertices=[],uvs=[],indices=[],normal=[],v=new T.Vector3();
  for(let yards=10;yards<=50;yards+=10){const z=field.goalZ+yards*field.unitsPerYard;if(z>field.startZ+.001)continue;
   for(const side of [-1,1]){const centerX=side*Math.max(1,field.halfWidth-7),centerZ=z-2.1,rotation=new T.Euler(-Math.PI/2,0,side*Math.PI/2),first=vertices.length/3;
    for(const [x,y,u,texV]of[[-1.4,-1.75,0,0],[1.4,-1.75,1,0],[1.4,1.75,1,1],[-1.4,1.75,0,1]]){v.set(x,y,0).applyEuler(rotation);vertices.push(v.x+centerX,.039,v.z+centerZ);uvs.push(((yards/10-1)+u)/5,texV);normal.push(0,1,0);}
    indices.push(first,first+1,first+2,first,first+2,first+3);
   }
  }
  const geometry=ownGeo(new T.BufferGeometry());geometry.setAttribute('position',new T.Float32BufferAttribute(vertices,3));geometry.setAttribute('normal',new T.Float32BufferAttribute(normal,3));geometry.setAttribute('uv',new T.Float32BufferAttribute(uvs,2));geometry.setIndex(indices);geometry.computeBoundingBox();geometry.computeBoundingSphere();
  const material=ownMat(new T.MeshStandardMaterial({map:numberEntry.texture,color:0xffffff,roughness:1,transparent:true,alphaTest:.06,depthWrite:false,polygonOffset:true,polygonOffsetFactor:-1,side:T.DoubleSide}));
  const numbers=new T.Mesh(geometry,material);numbers.name='Crisp sideline yard numbers';numbers.receiveShadow=true;essentials.add(numbers);
 }
 const pylonRows=[],baseRows=[];for(const side of [-1,1])for(const z of [field.goalZ,field.endZ]){pylonRows.push([side*field.halfWidth,.36,z,.23,.72,.23]);baseRows.push([side*field.halfWidth,.045,z,.35,.09,.35]);}
 instances('End-zone corner pylons',pylonRows,orange);instances('Pylon rubber feet',baseRows,black);
 const walk=[];for(const side of [-1,1])walk.push([side*(field.halfWidth+2.2),-.012,extendedMid,1.55,.018,span+depth]);instances('Sideline service walk',walk,concrete,detail);
 const chevrons=[];for(const side of [-1,1])for(let i=0;i<8;i++)chevrons.push([side*(field.halfWidth-1.2),.025,field.endZ+.7+i*(depth-1.4)/7,1.7,.022,.13]);const endAccents=instances('End-zone perimeter accents',chevrons,endAccent,detail);
 const word=canvasTexture(T,factory,1024,256,()=>{});
 let wordMesh=null;
 if(word){textures.add(word.texture);const material=ownMat(new T.MeshStandardMaterial({map:word.texture,color:0xffffff,roughness:1,transparent:true,alphaTest:.045,depthWrite:false,polygonOffset:true,polygonOffsetFactor:-1,side:T.DoubleSide}));wordMesh=new T.Mesh(planeGeometry,material);wordMesh.name='End-zone identity';wordMesh.rotation.x=-Math.PI/2;wordMesh.position.set(0,.032,centerEnd);wordMesh.scale.set(Math.min(43,field.halfWidth*1.65),depth*.58,1);wordMesh.receiveShadow=true;essentials.add(wordMesh);}
 const postGroup=new T.Group();postGroup.name='Goal post';essentials.add(postGroup);const gold=ownMat(new T.MeshStandardMaterial({color:0xf2c94c,roughness:.58,metalness:.22}));
 const postGeometry=ownGeo(new T.CylinderGeometry(.085,.085,1,8));
 const posts=new T.InstancedMesh(postGeometry,gold,5),postDummy=new T.Object3D();posts.name='Goal-post tubes';instanceMeshes.add(posts);postGroup.add(posts);let postIndex=0;
 function post(x,y,z,length,axis='y'){postDummy.position.set(x,y,z);postDummy.scale.set(1,length,1);postDummy.rotation.set(axis==='z'?Math.PI/2:0,0,axis==='x'?Math.PI/2:0);postDummy.updateMatrix();posts.setMatrixAt(postIndex++,postDummy.matrix);}
 post(0,2.8,field.endZ-3.4,5.6);post(0,5.6,field.endZ-2.2,2.4,'z');post(0,5.6,field.endZ-1,10,'x');post(-5,7.85,field.endZ-1,4.5);post(5,7.85,field.endZ-1,4.5);posts.instanceMatrix.needsUpdate=true;posts.computeBoundingBox?.();posts.computeBoundingSphere?.();
 let palette={primary:'#1b4c75',secondary:'#f5f1df',name:'RECEIVER WINDOW',turfColor:'#317d48',wet:false};
 function color(value,fallback){return typeof value==='number'&&Number.isFinite(value)&&value>=0&&value<=0xffffff||typeof value==='string'&&/^#[\da-f]{3}(?:[\da-f]{3})?$/i.test(value)?value:fallback;}
 function theme(raw={}){
  if(disposed)return false;if(typeof raw!=='object'||raw===null)raw={};
  palette={...palette,primary:color(raw.primary??raw.jersey??raw.endzoneColor,palette.primary),secondary:color(raw.secondary??raw.accent,palette.secondary),turfColor:color(raw.turfColor??raw.fieldColor,palette.turfColor),name:typeof raw.name==='string'?raw.name.replace(/[\r\n]/g,' ').slice(0,36).toUpperCase():palette.name,wet:typeof raw.wet==='boolean'?raw.wet:palette.wet};
  accent.color.set(palette.primary);endAccent.color.set(palette.secondary);
  if(turfMaterial){turfMaterial.color.set(palette.turfColor);if(palette.wet)turfMaterial.color.multiplyScalar(.92);turfMaterial.roughness=palette.wet?.79:.99;}
  if(endzoneMaterial)endzoneMaterial.color.set(palette.primary);
  const next=palette.name+'|'+palette.secondary+'|'+palette.primary;
  if(word&&next!==wordKey){wordKey=next;const c=word.context;c.clearRect(0,0,1024,256);c.textAlign='center';c.textBaseline='middle';c.font='900 138px Arial, sans-serif';c.lineWidth=7;c.strokeStyle=palette.primary;c.fillStyle=palette.secondary;c.strokeText?.(palette.name,512,132,968);c.fillText(palette.name,512,132,968);word.texture.needsUpdate=true;}
  return api;
 }
 function setQuality(value){if(disposed)return false;qualityLevel=value===0||value==='low'?'low':value===1||value==='medium'?'medium':'high';detail.visible=qualityLevel!=='low';endAccents.visible=qualityLevel==='high';return api;}
 function dispose(){
  if(disposed)return;disposed=true;scene.remove(group);if(turf&&turf.material===turfMaterial)turf.material=originalTurf;if(endzone&&endzone.material===endzoneMaterial)endzone.material=originalEndzone;
  for(const mesh of instanceMeshes)mesh.dispose();for(const g of geometries)g.dispose();for(const m of materials)m.dispose();for(const texture of textures)texture.dispose();for(const key of sharedKeys)release(key);group.clear();
 }
 const api={group,theme,quality:setQuality,dispose,world:{...field},lighting:{toneMapping:T.ACESFilmicToneMapping,exposure:1.04,shadowNormalBias:.035},get stats(){let draws=0,instancesCount=0,totalDraws=0,totalInstances=0;group.traverse(o=>{if(o.isMesh){const count=o.isInstancedMesh?o.count:1;totalDraws++;totalInstances+=count;let visible=true;for(let p=o;p;p=p.parent)if(!p.visible){visible=false;break;}if(visible){draws++;instancesCount+=count;}}});return {draws,instances:instancesCount,totalDraws,totalInstances,geometries:geometries.size,materials:materials.size,textures:sharedKeys.length+textures.size,quality:qualityLevel,disposed,interiorHashes:hashRows.length,sidelineTicks:fineRows.length};}};
 setQuality(quality);theme();return api;
}
const api={create,normalizeWorld,defaults,get sharedTextureCount(){return sharedTextures.size;}};if(typeof module==='object'&&module.exports)module.exports=api;root.QBFieldGraphics=api;
})(typeof globalThis!=='undefined'?globalThis:this);
