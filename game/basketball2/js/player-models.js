/* The player assets use a deliberately small, uncompressed glTF 2.0 contract.
 * Keeping this loader on the game's existing THREE instance avoids a second
 * rendering library and leaves the procedural players available as a fallback.
 */
const basketballPlayerModels = {
  assets:new Map(), pending:null, warned:false,
  remoteBase:"https://pub-b956b47cfcfd467d9f678959c08a5781.r2.dev/",
  files:{guard:"assets/models/player-guard.glb",wing:"assets/models/player-wing.glb",center:"assets/models/player-center.glb"}
};

// Equipment is saved as small region overrides. Legacy profiles and selected
// jersey/sneaker definitions remain the defaults; a profile may supply its own
// outfit without mutating any shared catalog entry.
function resolveBasketballEquipment(profile={},jerseyStyle={},sneakerStyle={},equipment={}){
  const color=(value,fallback)=>{
    if(typeof value==="string" && /^#[0-9a-f]{6}$/i.test(value)) return parseInt(value.slice(1),16);
    return Number.isInteger(value) && value>=0 && value<=0xffffff ? value : fallback;
  };
  const aliases={sleeve:"sleeves",wristband:"wristbands",accessory:"accessories",shoes:"sneaker"};
  const asItem=value=>typeof value==="number" || typeof value==="string" ? {color:value} : value && typeof value==="object" && !Array.isArray(value) ? value : {};
  const item=name=>({...asItem(profile.equipment?.[name] ?? profile.equipment?.[aliases[name]]),...asItem(equipment?.[name] ?? equipment?.[aliases[name]])});
  const accent=color(item("accent").color,jerseyStyle.trim ?? jerseyStyle.accent ?? profile.accent ?? 0xffffff);
  const region=(name,fallback,enabled=true)=>{
    const spec=item(name);
    return {color:color(spec.color ?? spec.primary,fallback),accent:color(spec.accent,accent),enabled:typeof spec.enabled==="boolean" ? spec.enabled : enabled,
      side:["left","right","both"].includes(spec.side) ? spec.side : name==="wristband" ? "both" : "right"};
  };
  const name=profile.name || "";
  const regions={
    jersey:region("jersey",jerseyStyle.primary ?? profile.jersey ?? 0x258fcd),
    shorts:region("shorts",jerseyStyle.secondary ?? profile.shorts ?? 0x123656),
    shoes:region("shoes",sneakerStyle.primary ?? 0xf5f5f5),
    socks:region("socks",0xf5f5f5),headband:region("headband",0xffffff,/Flash/.test(name) || profile.hairStyle==="headband"),
    sleeve:region("sleeve",0x111820,/Claw|Crafty|Smooth/.test(name)),
    wristband:region("wristband",accent),accessory:region("accessory",accent,false),accent:{color:accent,enabled:true}
  };
  regions.shoes.accent=color(item("shoes").accent,sneakerStyle.accent ?? 0xffffff);
  regions.shoes.sole=color(item("shoes").sole,sneakerStyle.sole ?? 0x162234);
  return {regions,
    jerseyStyle:{...jerseyStyle,primary:regions.jersey.color,secondary:color(item("jersey").secondary,jerseyStyle.secondary ?? regions.jersey.color),trim:regions.jersey.accent,accent},
    shortsStyle:{...jerseyStyle,primary:regions.shorts.color,secondary:regions.shorts.color,trim:regions.shorts.accent},
    sneakerStyle:{...sneakerStyle,primary:regions.shoes.color,accent:regions.shoes.accent,sole:regions.shoes.sole}};
}

function getBasketballEquipmentAppearance(char){
  return char.appearance || resolveBasketballEquipment(char.profile || {},char.jerseyStyle || {},char.sneaker || {},char.equipment || {});
}

function basketballEquipmentMaterialColors(char){
  const {regions:r}=getBasketballEquipmentAppearance(char),profile=char.profile || {};
  return {jersey:r.jersey.color,shorts:r.shorts.color,trim:r.jersey.accent,shorts_trim:r.shorts.accent,
    skin:profile.skin ?? 0xc6865a,hair:profile.hair ?? 0x201410,sneaker:r.shoes.color,sneaker_accent:r.shoes.accent,
    sole:r.shoes.sole,sock:r.socks.color,sock_trim:r.socks.accent,headband:r.headband.color,sleeve:r.sleeve.color,
    wristband:r.wristband.color,wristband_left:r.wristband.color,wristband_right:r.wristband.color,accessory:r.accessory.color,eyes:0x131b25,mouth:0x583125,number:0xffffff,player_number_overlay:r.jersey.accent};
}

function parseBasketballPlayerGLB(source,label="basketball player"){
  const fail=message=>{throw new Error(`${label}: ${message}`);};
  if(!(source instanceof ArrayBuffer) || source.byteLength<20 || source.byteLength>32*1024*1024) fail("invalid GLB size");
  const header=new DataView(source);
  if(header.getUint32(0,true)!==0x46546c67 || header.getUint32(4,true)!==2 || header.getUint32(8,true)!==source.byteLength) fail("invalid GLB 2.0 header");
  let documentJSON=null, binary=null, cursor=12;
  while(cursor<source.byteLength){
    if(cursor+8>source.byteLength) fail("truncated chunk header");
    const length=header.getUint32(cursor,true), type=header.getUint32(cursor+4,true);
    cursor+=8;
    if(length%4 || cursor+length>source.byteLength) fail("invalid chunk bounds");
    if(type===0x4e4f534a){
      if(documentJSON || cursor!==20 || length>2*1024*1024) fail("invalid JSON chunk");
      try{documentJSON=JSON.parse(new TextDecoder().decode(new Uint8Array(source,cursor,length)).trim());}
      catch(error){fail("invalid glTF JSON");}
    }else if(type===0x004e4942){
      if(!documentJSON || binary) fail("invalid BIN chunk");
      binary=new Uint8Array(source,cursor,length);
    }else fail("unsupported GLB chunk");
    cursor+=length;
  }
  const gltf=documentJSON;
  if(!gltf || !binary || gltf.asset?.version!=="2.0") fail("missing glTF 2.0 data");
  if((gltf.extensionsRequired || []).length) fail("compressed or extended assets are not supported");
  if(!Array.isArray(gltf.buffers) || gltf.buffers.length!==1 || gltf.buffers[0].uri || !Number.isInteger(gltf.buffers[0].byteLength) || gltf.buffers[0].byteLength>binary.byteLength || binary.byteLength-gltf.buffers[0].byteLength>3) fail("expected one embedded buffer");
  if((gltf.images || []).length || (gltf.textures || []).length) fail("player assets must use material colors");
  if(!Array.isArray(gltf.nodes) || !gltf.nodes.length || gltf.nodes.length>256 || (gltf.animations || []).length>64) fail("invalid node or animation count");
  const componentTypes={5120:[Int8Array,1,"getInt8"],5121:[Uint8Array,1,"getUint8"],5122:[Int16Array,2,"getInt16"],5123:[Uint16Array,2,"getUint16"],5125:[Uint32Array,4,"getUint32"],5126:[Float32Array,4,"getFloat32"]};
  const itemSizes={SCALAR:1,VEC2:2,VEC3:3,VEC4:4,MAT4:16};
  const accessorCache=new Map();
  function accessor(index){
    if(accessorCache.has(index)) return accessorCache.get(index);
    const spec=gltf.accessors?.[index], view=gltf.bufferViews?.[spec?.bufferView];
    if(!Number.isInteger(index) || !spec || !view || spec.sparse || view.buffer!==0) fail("invalid accessor reference");
    const component=componentTypes[spec.componentType], size=itemSizes[spec.type];
    if(!component || !size || !Number.isInteger(spec.count) || spec.count<1 || spec.count>500000) fail("unsupported accessor format");
    const [ArrayType,bytes,read]=component;
    const offset=spec.byteOffset || 0, viewOffset=view.byteOffset || 0;
    const elementBytes=size*bytes, stride=view.byteStride || elementBytes;
    if(!Number.isInteger(offset) || !Number.isInteger(viewOffset) || !Number.isInteger(view.byteLength) || !Number.isInteger(stride) || offset<0 || viewOffset<0 || offset%bytes || viewOffset%bytes || stride<elementBytes || stride%bytes || (view.byteStride && (stride<4 || stride>252))) fail("invalid accessor alignment or stride");
    if(viewOffset+view.byteLength>gltf.buffers[0].byteLength || offset+(spec.count-1)*stride+elementBytes>view.byteLength) fail("accessor exceeds its buffer view");
    const data=new ArrayType(spec.count*size);
    const input=new DataView(binary.buffer,binary.byteOffset+viewOffset,view.byteLength);
    for(let row=0;row<spec.count;row++) for(let col=0;col<size;col++){
      const value=input[read](offset+row*stride+col*bytes,true);
      if(!Number.isFinite(value)) fail("non-finite accessor value");
      data[row*size+col]=value;
    }
    const result={array:data,size,count:spec.count,normalized:!!spec.normalized,type:spec.componentType};
    accessorCache.set(index,result);
    return result;
  }
  const nodeNames=new Set();
  const parents=new Array(gltf.nodes.length).fill(-1);
  gltf.nodes.forEach((node,index)=>{
    const name=node.name || `node_${index}`;
    if(nodeNames.has(name) || /[.\[\]:\/]/.test(name)) fail("node names must be unique animation targets");
    nodeNames.add(name);
    for(const child of node.children || []){
      if(!Number.isInteger(child) || !gltf.nodes[child] || child===index || parents[child]!==-1) fail("invalid node hierarchy");
      parents[child]=index;
    }
    for(const [key,count] of [["translation",3],["rotation",4],["scale",3],["matrix",16]]){
      if(node[key] && (node[key].length!==count || node[key].some(value=>!Number.isFinite(value)))) fail("invalid node transform");
    }
  });
  gltf.nodes.forEach((node,index)=>{
    let ancestor=index, depth=0;
    while(parents[ancestor]!==-1){ancestor=parents[ancestor];if(++depth>gltf.nodes.length) fail("cyclic node hierarchy");}
  });
  const skinSpecs=(gltf.skins || []).map(skin=>{
    if(!Array.isArray(skin.joints) || !skin.joints.length || skin.joints.length>128 || new Set(skin.joints).size!==skin.joints.length || skin.joints.some(index=>!Number.isInteger(index) || !gltf.nodes[index])) fail("invalid skin joints");
    const inverse=skin.inverseBindMatrices==null ? null : accessor(skin.inverseBindMatrices);
    if(inverse && (inverse.size!==16 || inverse.type!==5126 || inverse.count!==skin.joints.length)) fail("invalid inverse bind matrices");
    return {joints:skin.joints.slice(),inverse};
  });
  if(!skinSpecs.length) fail("player has no skeleton");
  const materials=(gltf.materials || []).map(spec=>{
    const pbr=spec.pbrMetallicRoughness || {}, color=pbr.baseColorFactor || [1,1,1,1];
    if(color.length!==4 || color.some(value=>!Number.isFinite(value))) fail("invalid material color");
    const material=new THREE.MeshStandardMaterial({
      color:new THREE.Color().setRGB(color[0],color[1],color[2]),
      opacity:color[3],roughness:pbr.roughnessFactor ?? .75,metalness:pbr.metallicFactor ?? 0,
      side:spec.doubleSided ? THREE.DoubleSide : THREE.FrontSide,
      transparent:spec.alphaMode==="BLEND",alphaTest:spec.alphaMode==="MASK" ? (spec.alphaCutoff ?? .5) : 0
    });
    material.name=spec.name || "player_material";
    return material;
  });
  const geometries=[],bufferAttributes=new Map();
  try{
    const meshes=(gltf.meshes || []).map(mesh=>{
      if(!Array.isArray(mesh.primitives) || !mesh.primitives.length || mesh.primitives.length>32) fail("invalid mesh primitives");
      return mesh.primitives.flatMap(primitive=>{
        if((primitive.mode ?? 4)!==4 || primitive.targets || primitive.extensions) fail("unsupported player primitive");
        const geometry=new THREE.BufferGeometry();
        geometries.push(geometry);
        const attributes={POSITION:["position",3],NORMAL:["normal",3],TEXCOORD_0:["uv",2],JOINTS_0:["skinIndex",4],WEIGHTS_0:["skinWeight",4]};
        let vertexCount=0;
        for(const [semantic,[name,size]] of Object.entries(attributes)){
          if(primitive.attributes?.[semantic]==null){if(["POSITION","JOINTS_0","WEIGHTS_0"].includes(semantic)) fail(`missing ${semantic}`);continue;}
          const attribute=accessor(primitive.attributes[semantic]);
          if(attribute.size!==size || (vertexCount && attribute.count!==vertexCount)) fail("inconsistent vertex attributes");
          if(semantic==="POSITION"){
            if(attribute.type!==5126 || attribute.normalized) fail("invalid position format");
            vertexCount=attribute.count;
          }
          if(semantic==="JOINTS_0" && (![5121,5123].includes(attribute.type) || attribute.normalized)) fail("invalid joint format");
          if(semantic==="WEIGHTS_0" && !(attribute.type===5126 || ([5121,5123].includes(attribute.type) && attribute.normalized))) fail("invalid weight format");
          const key=`${semantic}:${primitive.attributes[semantic]}`;
          let bufferAttribute=bufferAttributes.get(key);
          if(!bufferAttribute){
            if(semantic==="WEIGHTS_0"){
              const weights=new Float32Array(attribute.array.length);
              for(let vertex=0;vertex<attribute.count;vertex++){
                let total=0;
                for(let joint=0;joint<4;joint++){
                  const weight=attribute.array[vertex*4+joint];
                  if(weight<0) fail("negative skin weight");
                  total+=weight;
                }
                if(total<=0) fail("vertex has no skin weights");
                for(let joint=0;joint<4;joint++) weights[vertex*4+joint]=attribute.array[vertex*4+joint]/total;
              }
              bufferAttribute=new THREE.BufferAttribute(weights,4);
            }else bufferAttribute=new THREE.BufferAttribute(attribute.array,attribute.size,attribute.normalized);
            bufferAttributes.set(key,bufferAttribute);
          }
          // Material primitives share vertex accessors. Reusing the attribute
          // object also shares its GPU buffer across those draw calls.
          geometry.setAttribute(name,bufferAttribute);
        }
        if(primitive.indices!=null){
          const indices=accessor(primitive.indices);
          if(indices.size!==1 || ![5121,5123,5125].includes(indices.type) || indices.count%3 || indices.array.some(index=>index>=vertexCount)) fail("invalid triangle indices");
          geometry.setIndex(new THREE.BufferAttribute(indices.type===5121 ? new Uint16Array(indices.array) : indices.array,1));
        }else if(vertexCount%3) fail("invalid triangle vertex count");
        if(!geometry.getAttribute("normal")) geometry.computeVertexNormals();
        if(!materials[primitive.material]) fail("invalid material reference");
        geometry.computeBoundingBox();geometry.computeBoundingSphere();
        geometry.userData.sharedPlayerGeometry=true;
        // Older GLBs use one trim material for clothes, sock stripes and shoe
        // accents. Split only their tiny index lists once per cached asset;
        // all regions still share the original vertex/skin GPU attributes.
        // Wristbands are also split by side so one arm can remain uncovered.
        const regionMaterial=materials[primitive.material].name;
        if(regionMaterial==="trim" || regionMaterial==="wristband"){
          const authored=gltf.nodes.find(node=>node.extras?.profileHeight)?.extras || {};
          const height=authored.profileHeight || 1,position=geometry.getAttribute("position");
          const source=geometry.index?.array || Array.from({length:position.count},(_,index)=>index),regions=new Map();
          for(let index=0;index<source.length;index+=3){
            const y=(position.getY(source[index])+position.getY(source[index+1])+position.getY(source[index+2]))/(3*height);
            const x=(position.getX(source[index])+position.getX(source[index+1])+position.getX(source[index+2]))/3;
            const name=regionMaterial==="wristband" ? `wristband_${x<0 ? "left" : "right"}` : y<.24 ? "sneaker_accent" : y<.5 ? "sock_trim" : y<1.145 ? "shorts_trim" : "trim";
            if(!regions.has(name)) regions.set(name,[]);
            regions.get(name).push(source[index],source[index+1],source[index+2]);
          }
          return Array.from(regions,([name,indices],index)=>{
            let materialIndex=materials.findIndex(material=>material.name===name);
            if(materialIndex<0){const material=materials[primitive.material].clone();material.name=name;materialIndex=materials.push(material)-1;}
            const part=index===0 ? geometry : new THREE.BufferGeometry();
            if(index>0){geometries.push(part);for(const [name,attribute] of Object.entries(geometry.attributes)) part.setAttribute(name,attribute);}
            part.setIndex(new THREE.BufferAttribute(new (position.count>65535 ? Uint32Array : Uint16Array)(indices),1));
            part.boundingBox=geometry.boundingBox;part.boundingSphere=geometry.boundingSphere;part.userData.sharedPlayerGeometry=true;
            return {geometry:part,material:materialIndex};
          });
        }
        return [{geometry,material:primitive.material}];
      });
    });
    for(const node of gltf.nodes){
      if(node.mesh!=null && (!meshes[node.mesh] || !skinSpecs[node.skin])) fail("player mesh is missing a valid skin");
      if(node.mesh!=null){
        const jointCount=skinSpecs[node.skin].joints.length;
        for(const primitive of meshes[node.mesh]){
          if(primitive.geometry.getAttribute("skinIndex").array.some(value=>value>=jointCount)) fail("vertex references an invalid joint");
        }
      }
    }
    const clips=(gltf.animations || []).map(animation=>{
      const tracks=[];
      for(const channel of animation.channels || []){
        const sampler=animation.samplers?.[channel.sampler], node=gltf.nodes[channel.target?.node];
        const properties={rotation:["quaternion",4],translation:["position",3],scale:["scale",3]};
        const property=properties[channel.target?.path];
        if(!sampler || !node || !property || (sampler.interpolation || "LINEAR")!=="LINEAR") fail("unsupported animation channel");
        const times=accessor(sampler.input), values=accessor(sampler.output);
        if(times.size!==1 || times.type!==5126 || values.type!==5126 || values.size!==property[1] || values.count!==times.count || times.array[0]<0) fail("invalid animation samples");
        for(let i=1;i<times.count;i++) if(times.array[i]<=times.array[i-1]) fail("animation times must increase");
        const Track=channel.target.path==="rotation" ? THREE.QuaternionKeyframeTrack : THREE.VectorKeyframeTrack;
        tracks.push(new Track(`${node.name || `node_${channel.target.node}`}.${property[0]}`,times.array,values.array,THREE.InterpolateLinear));
      }
      if(!tracks.length) fail("empty animation clip");
      return new THREE.AnimationClip(animation.name || `clip_${tracks.length}`,-1,tracks);
    });
    const sceneSpec=gltf.scenes?.[gltf.scene ?? 0];
    if(!sceneSpec?.nodes?.length || sceneSpec.nodes.some(index=>!gltf.nodes[index] || parents[index]!==-1)) fail("invalid default scene");
    const required=["idle","run","sprint","defensive_shuffle","pivot","jump_shot","layup","dunk","pass","catch","block","crossover"];
    if(required.some(name=>!clips.some(clip=>clip.name===name))) fail("missing basketball animation clips");
    return {gltf,meshes,materials,skinSpecs,clips,roots:sceneSpec.nodes.slice(),label};
  }catch(error){
    geometries.forEach(geometry=>geometry.dispose());materials.forEach(material=>material.dispose());
    throw error;
  }
}

async function loadPlayerModels(){
  if(basketballPlayerModels.pending) return basketballPlayerModels.pending;
  basketballPlayerModels.pending=Promise.all(Object.entries(basketballPlayerModels.files).map(async ([kind,url])=>{
    const sources=[basketballPlayerModels.remoteBase+url.split("/").pop(),url];
    let lastError;
    for(const [index,source] of sources.entries()){
      const controller=index===0 ? new AbortController() : null;
      const timeout=controller ? setTimeout(()=>controller.abort(),6500) : null;
      try{
        const response=await fetch(source,controller ? {signal:controller.signal,credentials:"omit"} : undefined);
        if(!response.ok) throw new Error(`${source}: HTTP ${response.status}`);
        const asset=parseBasketballPlayerGLB(await response.arrayBuffer(),source);
        basketballPlayerModels.assets.set(kind,asset);
        return {loaded:true,remote:index===0,error:lastError};
      }catch(error){lastError=error;}
      finally{if(timeout!==null) clearTimeout(timeout);}
    }
    return {loaded:false,remote:false,error:lastError};
  })).then(results=>{
    const loaded=results.every(result=>result.loaded);
    const fallback=results.find(result=>!result.remote);
    if(fallback && !basketballPlayerModels.warned){
      console.warn(loaded ? "Basketball remote player models could not load; using bundled GLB players." : "Basketball player models could not load; using the built-in players.",fallback.error);
      basketballPlayerModels.warned=true;
    }
    if(typeof matchPlayers!=="undefined") matchPlayers.forEach(attachPlayerModel);
    return loaded;
  });
  return basketballPlayerModels.pending;
}

function getBasketballPlayerModelKind(char){
  const profile=char.profile || {};
  if(profile.height>=1.15 || profile.width>=1.2) return "center";
  if(profile.height<=.98 && profile.width<=.95) return "guard";
  return "wing";
}

function createBasketballPlayerInstance(asset,char){
  const group=new THREE.Group();group.name="basketball_player_asset";
  group.userData.basketballPlayerModel=true;
  const jointIndices=new Set(asset.skinSpecs.flatMap(skin=>skin.joints));
  const nodes=asset.gltf.nodes.map((spec,index)=>{
    const node=jointIndices.has(index) ? new THREE.Bone() : new THREE.Group();
    node.name=spec.name || `node_${index}`;
    if(spec.matrix){new THREE.Matrix4().fromArray(spec.matrix).decompose(node.position,node.quaternion,node.scale);}
    else{
      if(spec.translation) node.position.fromArray(spec.translation);
      if(spec.rotation) node.quaternion.fromArray(spec.rotation).normalize();
      if(spec.scale) node.scale.fromArray(spec.scale);
    }
    return node;
  });
  asset.gltf.nodes.forEach((spec,index)=>(spec.children || []).forEach(child=>nodes[index].add(nodes[child])));
  asset.roots.forEach(index=>group.add(nodes[index]));
  const materials=asset.materials.map(material=>material.clone());
  const textures=[],ownedGeometries=[];
  applyBasketballPlayerMaterials(materials,char,textures);
  const skeletons=asset.skinSpecs.map(skin=>new THREE.Skeleton(skin.joints.map(index=>nodes[index]),skin.joints.map((index,joint)=>skin.inverse ? new THREE.Matrix4().fromArray(skin.inverse.array,joint*16) : new THREE.Matrix4())));
  asset.gltf.nodes.forEach((spec,index)=>{
    if(spec.mesh==null) return;
    for(const primitive of asset.meshes[spec.mesh]){
      const mesh=new THREE.SkinnedMesh(primitive.geometry,materials[primitive.material]);
      mesh.name=`${nodes[index].name}_${mesh.material.name}`;
      mesh.castShadow=true;mesh.receiveShadow=true;
      // Animated limbs can leave their bind-pose sphere. At most six small
      // player meshes are on court, so disabling culling avoids disappearing hands.
      mesh.frustumCulled=false;
      mesh.userData.sharedPlayerGeometry=true;
      nodes[index].add(mesh);
      mesh.bind(skeletons[spec.skin],new THREE.Matrix4());
    }
  });
  group.updateMatrixWorld(true);
  const bounds=new THREE.Box3().setFromObject(group), baseHeight=Math.max(.1,bounds.max.y-bounds.min.y);
  const profile=char.profile || {};
  const authored=asset.gltf.nodes.find(node=>node.extras?.authoredHeight)?.extras || asset.gltf.asset?.extras || {};
  const authoredWidth=authored.profileWidth || 1;
  // Existing court/collision dimensions use a 2.4-unit base player height.
  group.scale.set((profile.width || 1)/authoredWidth,2.4*(profile.height || 1)/baseHeight,(profile.width || 1)/authoredWidth);
  group.position.y=-bounds.min.y*group.scale.y;
  const bones=new Map(nodes.filter(node=>node.isBone).map(node=>[node.name,node]));
  addBasketballPlayerNumbers(bones,char,authored,materials,textures,ownedGeometries);
  const left=nodes.find(node=>node.name==="hand_L"),right=nodes.find(node=>node.name==="hand_R");
  const sideBones=left && right && left.getWorldPosition(new THREE.Vector3()).x>right.getWorldPosition(new THREE.Vector3()).x
    ? {left:"R",right:"L"} : {left:"L",right:"R"};
  const equipmentNodes=addBasketballModelEquipment(bones,sideBones,char,authored,materials,ownedGeometries);
  const mixer=new THREE.AnimationMixer(group), actions=new Map();
  for(const clip of asset.clips){
    const action=mixer.clipAction(clip);
    const once=["jump_shot","layup","dunk","pass","catch","block","crossover"].includes(clip.name);
    action.setLoop(once ? THREE.LoopOnce : THREE.LoopRepeat,once ? 1 : Infinity);
    action.clampWhenFinished=once;
    actions.set(clip.name,action);
  }
  const state={group,nodes,bones,sideBones,skeletons,mixer,actions,materials,textures,ownedGeometries,equipmentNodes,current:null,mode:null,hidden:[],previousAngle:char.angle || 0,turnLean:0,elapsed:0,basePose:[]};
  setBasketballPlayerAction(state,"idle","idle");
  mixer.update(0);
  state.basePose=Array.from(bones.values(),bone=>({bone,quaternion:bone.quaternion.clone()}));
  group.updateMatrixWorld(true);
  return state;
}

function applyBasketballPlayerMaterials(materials,char,textures){
  const colors=basketballEquipmentMaterialColors(char),appearance=getBasketballEquipmentAppearance(char);
  for(const material of materials){
    if(colors[material.name]!=null) material.color.setHex(colors[material.name]);
    if(material.name==="sneaker"){
      material.emissive.setHex(appearance.sneakerStyle.type==="glow" ? appearance.regions.shoes.accent : 0x000000);
      material.emissiveIntensity=appearance.sneakerStyle.type==="glow" ? .18 : 0;
    }
    if(material.name==="number") material.visible=false;
    if(material.name==="sock" || material.name==="sock_trim") material.visible=appearance.regions.socks.enabled;
    if(material.name==="wristband") material.visible=appearance.regions.wristband.enabled;
    if(material.name.startsWith("wristband_")){
      const side=material.name.slice(10),region=appearance.regions.wristband;
      material.visible=region.enabled && (region.side==="both" || region.side===side);
    }
  }
}

function addBasketballModelEquipment(bones,sideBones,char,authored,materials,geometries){
  const appearance=getBasketballEquipmentAppearance(char),w=authored.profileWidth || 1,h=authored.profileHeight || 1,nodes=[];
  function add(region,bone,geometry,position,side=null){
    if(!bone){geometry.dispose();return;}
    const spec=appearance.regions[region],material=new THREE.MeshStandardMaterial({color:spec.color,roughness:.72});
    material.name=region;materials.push(material);geometries.push(geometry);
    const mesh=new THREE.Mesh(geometry,material);mesh.name=`equipment_${region}_${side || "center"}`;
    mesh.position.set(...position);mesh.visible=spec.enabled && (!side || spec.side==="both" || spec.side===side);
    mesh.castShadow=true;bone.add(mesh);nodes.push({node:mesh,region,side});return mesh;
  }
  const headband=add("headband",bones.get("head"),new THREE.TorusGeometry(.117*w,.014*w,6,22),[0,.035*h,0]);
  if(headband) headband.rotation.x=Math.PI/2;
  for(const side of ["left","right"]){
    const bone=bones.get(`forearm_${sideBones[side]}`);
    const sleeve=add("sleeve",bone,new THREE.CylinderGeometry(.078*w,.057*w,.25*h,10),[0,-.12*h,.005*w],side);
    if(sleeve) sleeve.rotation.z=side==="left" ? -.08 : .08;
    add("accessory",bones.get(`upper_arm_${sideBones[side]}`),new THREE.CylinderGeometry(.105*w,.105*w,.075*h,10),[0,-.15*h,0],side);
  }
  return nodes;
}

function updateBasketballPlayerEquipment(char,equipment={}){
  if(!char) return;
  char.equipment=equipment;
  char.appearance=resolveBasketballEquipment(char.profile || {},char.baseJerseyStyle || char.jerseyStyle || {},char.baseSneakerStyle || char.sneaker || {},equipment);
  char.jerseyStyle=char.appearance.jerseyStyle;char.sneaker=char.appearance.sneakerStyle;
  const model=char.playerModel;
  if(model){
    applyBasketballPlayerMaterials(model.materials,char,model.textures);
    for(const entry of model.equipmentNodes || []){
      const region=char.appearance.regions[entry.region];
      entry.node.visible=region.enabled && (!entry.side || region.side==="both" || region.side===entry.side);
    }
  }
  const colors=basketballEquipmentMaterialColors(char);
  for(const material of char.equipmentMaterials || []){
    const region=material.name;
    if(colors[region]!=null) material.color.setHex(colors[region]);
    if(material.emissive && material.emissiveIntensity>0 && ["trim","shorts_trim","sneaker_accent"].includes(region)) material.emissive.setHex(colors[region]);
    if(["jersey","shorts","sneaker"].includes(region) && material.map){
      const previous=material.map;
      material.map=region==="jersey" ? makeJerseyTexture(char.appearance.jerseyStyle,char.profile.number || "23") : region==="shorts" ? makeShortsTexture(char.appearance.shortsStyle) : makeShoeTexture(char.appearance.sneakerStyle);
      material.color.setHex(0xffffff);material.needsUpdate=true;previous.dispose();
    }
  }
  for(const entry of char.equipmentNodes || []){
    const region=char.appearance.regions[entry.region];
    entry.node.visible=region.enabled && (!entry.side || region.side==="both" || region.side===entry.side);
  }
}

function addBasketballPlayerNumbers(bones,char,authored,materials,textures,ownedGeometries){
  const chest=bones.get("chest");
  if(!chest || typeof document==="undefined") return;
  const canvas=document.createElement("canvas");canvas.width=128;canvas.height=128;
  const ctx=canvas.getContext("2d");if(!ctx) return;
  ctx.clearRect(0,0,128,128);ctx.fillStyle="#ffffff";ctx.font="bold 94px Arial";ctx.textAlign="center";ctx.textBaseline="middle";ctx.fillText(String(char.profile?.number || "23"),64,69,122);
  const texture=new THREE.CanvasTexture(canvas);texture.colorSpace=THREE.SRGBColorSpace;textures.push(texture);
  const material=new THREE.MeshStandardMaterial({map:texture,color:getBasketballEquipmentAppearance(char).regions.jersey.accent,transparent:true,alphaTest:.2,roughness:.8,depthWrite:false});
  material.name="player_number_overlay";materials.push(material);
  const width=authored.profileWidth || 1,height=authored.profileHeight || 1;
  const geometry=new THREE.PlaneGeometry(.26*width,.27*height);ownedGeometries.push(geometry);
  for(const front of [true,false]){
    const plane=new THREE.Mesh(geometry,material);plane.name=front ? "jersey_number_front" : "jersey_number_back";
    plane.position.set(0,-.07*height,(front ? .159 : -.160)*width);
    if(!front) plane.rotation.y=Math.PI;
    chest.add(plane);
  }
}

function attachPlayerModel(char){
  if(!char?.group || char.playerModel) return !!char?.playerModel;
  const asset=basketballPlayerModels.assets.get(getBasketballPlayerModelKind(char));
  if(!asset) return false;
  try{
    const state=createBasketballPlayerInstance(asset,char);
    const keep=new Set([char.shadow,char.hookIndicator,char.shotIndicator,char.defenseIndicator,char.staminaIndicator,char.selectionMarker,char.controlMarker,char.teamRing,char.receiverRing,char.controlLabel]);
    const legacy=char.legacyVisualNodes || char.group.children.filter(child=>!keep.has(child) && !child.userData?.basketballPlayerModel);
    state.hidden=legacy.map(node=>({node,visible:node.visible}));
    state.hidden.forEach(({node})=>{node.visible=false;});
    char.group.add(state.group);char.playerModel=state;
    char.group.updateMatrixWorld(true);
    return true;
  }catch(error){
    if(!basketballPlayerModels.warned){console.warn("Basketball player rig could not initialize; using the built-in player.",error);basketballPlayerModels.warned=true;}
    return false;
  }
}

function setBasketballPlayerAction(state,name,mode=name){
  const action=state.actions.get(name) || state.actions.get("idle");
  if(!action) return;
  if(state.current===action && state.mode===mode) return;
  const previous=state.current;
  action.reset().setEffectiveTimeScale(1).setEffectiveWeight(1).play();
  if(previous && previous!==action) previous.crossFadeTo(action,.12,false);
  else if(!previous) action.fadeIn(.08);
  state.current=action;state.mode=mode;
}

function chooseBasketballPlayerAnimation(char){
  const charging=typeof shotCharging!=="undefined" && shotCharging && typeof chargingPlayer!=="undefined" && chargingPlayer===char;
  if(typeof dunkActive!=="undefined" && dunkActive && dunkOwner===char.id) return {name:"dunk",mode:"dunk",phase:char.moveMeta?.dunk?.posePhase ?? Math.min(1,dunkTimer/Math.max(.001,dunkDuration))};
  if(typeof layupActive!=="undefined" && layupActive && layupOwner===char.id) return {name:"layup",mode:"layup",phase:Math.min(1,layupTimer/Math.max(.001,layupDuration))};
  if(charging) return {name:"jump_shot",mode:"charge",phase:Math.min(.49,.14+(char.moveMeta?.chargeHeld || 0)*.45)};
  if(char.moveState==="pumpfake") return {name:"jump_shot",mode:"pumpfake",phase:.1+.24*Math.sin(Math.min(1,(char.moveTimer || 0)/Math.max(.001,char.moveDuration || .4))*Math.PI)};
  if(char.shotMotionTime>0) return {name:"jump_shot",mode:"release",phase:.52+.48*(1-Math.min(1,char.shotMotionTime/(char.moveMeta?.shotReleaseDuration||.46)))};
  if(char.passMotionTime>0) return {name:"pass",phase:1-Math.min(1,char.passMotionTime/.32)};
  if(char.blockTime>0 || char.handsUpTime>0 || !char.onGround && char.blockBoost) return {name:"block",mode:char.aiBrain?.role==="rebound" ? "rebound" : "block",phase:char.onGround && char.handsUpTime>0 ? .48 : 1-Math.min(1,Math.max(char.blockTime || 0,char.handsUpTime || 0)/.42)};
  if(typeof ballState!=="undefined" && ballState==="pass" && typeof passFlight!=="undefined" && passFlight.active && passFlight.targetId===char.id) return {name:"catch",phase:.35};
  if(char.moveState==="gather" || char.moveState==="bobble") return {name:"catch",phase:Math.min(1,(char.moveTimer || 0)/Math.max(.001,char.moveDuration || .35))};
  if(char.hasBall && char.catchAnimation>0) return {name:"catch",mode:"receive",phase:.6+.4*(1-Math.min(1,char.catchAnimation/.28))};
  if(typeof crossover!=="undefined" && crossover.active && crossover.player===char) return {name:"crossover",phase:Math.min(1,crossover.progress)};
  const speed=char.velocity?.length() || 0;
  const role=char.aiBrain?.role;
  if(!char.hasBall && speed<1.5 && (char.aiScreen || char.plantedScreen || role==="screen")) return {name:"idle",mode:"screen",speed:1};
  if(!char.hasBall && speed<1.5 && role==="box_out" && ["shot","loose"].includes(typeof ballState!=="undefined" ? ballState : "")) return {name:"idle",mode:"box_out",speed:1};
  const defending=typeof teamOf==="function" && typeof possession!=="undefined" && teamOf(possession)!==char.teamId;
  if(defending){
    if(speed>4.1 && ["closeout","recover","recovery","switch"].includes(role)) return {name:"run",mode:role==="closeout" ? "closeout" : "recover",speed:speed/3.8};
    return {name:"defensive_shuffle",speed:Math.max(.18,speed/3.5)};
  }
  if(speed>.15 && !char.hasBall && ["cut","roll","backdoor"].includes(role)) return {name:speed>4.7 ? "sprint" : "run",mode:role,speed:Math.max(.14,speed/(speed>4.7 ? 6.5 : 3.8))};
  if(speed>.15) return {name:speed>4.7 ? "sprint" : "run",speed:Math.max(.14,speed/(speed>4.7 ? 6.5 : 3.8))};
  if(Math.abs(char.playerModel?.turnLean || 0)>.06) return {name:"pivot",speed:.8};
  return {name:"idle",speed:1};
}

function updatePlayerModel(char,dt){
  if(!char?.playerModel && !attachPlayerModel(char)) return;
  const state=char.playerModel;
  dt=Math.max(0,Math.min(.1,dt || 0));state.elapsed+=dt;
  const angle=char.angle || 0, delta=Math.atan2(Math.sin(angle-state.previousAngle),Math.cos(angle-state.previousAngle));
  const turnTarget=THREE.MathUtils.clamp(-delta/Math.max(dt,.001)*.035,-.12,.12);
  state.turnLean=THREE.MathUtils.lerp(state.turnLean,turnTarget,1-Math.exp(-dt*11));state.previousAngle=angle;
  const animation=chooseBasketballPlayerAnimation(char);
  // Restore the mixer result before applying the next frame. Clips only key
  // the joints they need; otherwise a procedural wrist/look offset would build
  // up indefinitely on a joint that the current clip leaves untouched.
  state.basePose.forEach(({bone,quaternion})=>bone.quaternion.copy(quaternion));
  setBasketballPlayerAction(state,animation.name,animation.mode || animation.name);
  const action=state.current;
  action.paused=animation.phase!=null;
  if(animation.phase!=null) action.time=THREE.MathUtils.clamp(animation.phase,0,.999)*action.getClip().duration;
  else action.setEffectiveTimeScale(THREE.MathUtils.clamp(animation.speed || 1,.1,1.8));
  state.mixer.update(dt);
  state.basePose.forEach(pose=>pose.quaternion.copy(pose.bone.quaternion));
  state.group.rotation.z=state.turnLean;
  state.group.rotation.x=["cut","roll","backdoor"].includes(state.mode) ? .06 : state.mode==="screen" ? .035 : 0;
  state.group.updateMatrixWorld(true);
  const head=state.bones.get("head");
  if(head) head.rotation.y+=THREE.MathUtils.clamp(char.anim?.headYaw || 0,-.45,.45);
  const dribbling=char.hasBall && typeof ballState!=="undefined" && ballState==="dribble" && !["jump_shot","crossover"].includes(animation.name) && char.moveState!=="gather";
  if(state.mode==="dunk"){
    poseBasketballDunkHands(char);
  }else if(state.mode==="screen" || state.mode==="box_out"){
    // A legal planted screen reads as braced feet and protected hands. This is
    // an additive pose on the existing rig, with no movement lock or new clip.
    for(const gameSide of ["left","right"]){
      const sign=gameSide==="left" ? -1 : 1,side=state.sideBones[gameSide],thigh=state.bones.get(`thigh_${side}`);
      if(thigh) thigh.rotation.z+=sign*-.12;
      const boxOut=state.mode==="box_out";
      const target=new THREE.Vector3(sign*(boxOut ? .46 : .14)*(char.profile?.width || 1),(boxOut ? 1.28 : 1.12)*(char.profile?.height || 1),boxOut ? .05 : .19);
      solveBasketballArmIK(state,side,char.group.localToWorld(target));
    }
  }else if(state.mode==="closeout"){
    const arm=state.bones.get(`upper_arm_${state.sideBones.left}`);
    if(arm) arm.rotation.x-=.85;
  }else if(dribbling){
    const gameSide=typeof ballSideValue!=="undefined" && ballSideValue<0 ? "left" : "right";
    const side=state.sideBones[gameSide];
    const sign=gameSide==="left" ? -1 : 1, phase=char.dribbleBounce || 0;
    const width=char.profile?.width || 1, height=char.profile?.height || 1;
    const localTarget=new THREE.Vector3(sign*.59*width,(1.09+Math.sin(phase)*.13)*height,.31*width);
    char.group.updateMatrixWorld(true);
    solveBasketballArmIK(state,side,char.group.localToWorld(localTarget));
    const hand=state.bones.get(`hand_${side}`);
    if(hand) hand.rotation.x+=Math.sin(phase)*.18;
  }else if(char.swipeTime>0 && typeof ball!=="undefined" && ball?.position){
    const gameSide=char.swipeSide==="left" ? "left" : "right",side=state.sideBones[gameSide];
    const hand=state.bones.get(`hand_${side}`);
    if(hand){
      const reach=Math.sin(THREE.MathUtils.clamp(1-char.swipeTime/.34,0,1)*Math.PI);
      const target=hand.getWorldPosition(new THREE.Vector3()).lerp(ball.position,reach);
      solveBasketballArmIK(state,side,target);
    }
  }
  state.group.updateMatrixWorld(true);
}

function solveBasketballArmIK(state,side,target){
  const upper=state.bones.get(`upper_arm_${side}`), lower=state.bones.get(`forearm_${side}`), hand=state.bones.get(`hand_${side}`);
  if(!upper || !lower || !hand) return;
  const shoulder=upper.getWorldPosition(new THREE.Vector3()), elbow=lower.getWorldPosition(new THREE.Vector3()), wrist=hand.getWorldPosition(new THREE.Vector3());
  const upperLength=shoulder.distanceTo(elbow), lowerLength=elbow.distanceTo(wrist);
  if(upperLength<.001 || lowerLength<.001) return;
  const axis=target.clone().sub(shoulder), distance=THREE.MathUtils.clamp(axis.length(),Math.abs(upperLength-lowerLength)+.001,upperLength+lowerLength-.001);
  axis.normalize();
  const along=(upperLength*upperLength-lowerLength*lowerLength+distance*distance)/(2*distance);
  const bendHeight=Math.sqrt(Math.max(0,upperLength*upperLength-along*along));
  const bend=new THREE.Vector3(side==="L" ? -.3 : .3,-.15,-1).applyQuaternion(state.group.getWorldQuaternion(new THREE.Quaternion()));
  bend.addScaledVector(axis,-bend.dot(axis));
  if(bend.lengthSq()<.0001) bend.set(0,1,0).addScaledVector(axis,-axis.y);
  bend.normalize();
  const desiredElbow=shoulder.clone().addScaledVector(axis,along).addScaledVector(bend,bendHeight);
  rotateBasketballBoneToward(upper,elbow.clone().sub(shoulder),desiredElbow.clone().sub(shoulder));
  upper.updateMatrixWorld(true);
  const nextElbow=lower.getWorldPosition(new THREE.Vector3()), nextWrist=hand.getWorldPosition(new THREE.Vector3());
  rotateBasketballBoneToward(lower,nextWrist.sub(nextElbow),shoulder.clone().addScaledVector(axis,distance).sub(nextElbow));
  lower.updateMatrixWorld(true);
}

function rotateBasketballBoneToward(bone,from,to){
  if(from.lengthSq()<.000001 || to.lengthSq()<.000001) return;
  const change=new THREE.Quaternion().setFromUnitVectors(from.normalize(),to.normalize());
  const world=bone.getWorldQuaternion(new THREE.Quaternion()).premultiply(change);
  const parent=bone.parent ? bone.parent.getWorldQuaternion(new THREE.Quaternion()).invert() : new THREE.Quaternion();
  bone.quaternion.copy(parent.multiply(world)).normalize();
}

function getModelJointWorld(char,name){
  const joint=char?.playerModel?.bones.get(name);
  if(!joint) return null;
  char.group.updateMatrixWorld(true);
  return joint.getWorldPosition(new THREE.Vector3());
}

function getModelHandWorld(char,side){
  const suffix=char?.playerModel?.sideBones[side==="left" ? "left" : "right"];
  return suffix ? getModelJointWorld(char,`hand_${suffix}`) : null;
}

function getModelForearmWorld(char,side){
  const suffix=char?.playerModel?.sideBones[side==="left" ? "left" : "right"];
  return suffix ? getModelJointWorld(char,`forearm_${suffix}`) : null;
}

function getBasketballDunkReach(char,side){
  const suffix=char.playerModel?.sideBones[side],model=char.playerModel;
  if(model&&suffix){
    const shoulder=getModelJointWorld(char,`upper_arm_${suffix}`),elbow=getModelJointWorld(char,`forearm_${suffix}`),hand=getModelHandWorld(char,side);
    const knee=Math.max(getModelJointWorld(char,"shin_L").y,getModelJointWorld(char,"shin_R").y)-char.group.position.y;
    const length=shoulder.distanceTo(elbow)+elbow.distanceTo(hand);
    return {shoulder:shoulder.sub(char.group.position),length,knee};
  }
  const arm=side==="left"?char.leftArm:char.rightArm,fore=side==="left"?char.leftForearm:char.rightForearm,hand=side==="left"?char.leftHand:char.rightHand;
  if(!arm?.position||!fore?.position||!hand?.position) return null;
  return {shoulder:char.group.localToWorld(arm.position.clone()).sub(char.group.position),length:arm.position.distanceTo(fore.position)+fore.position.distanceTo(hand.position),knee:.75*char.profile.height};
}

function poseBasketballDunkHands(char){
  const meta=char.moveMeta?.dunk;if(!meta?.handStarts||!char.group.worldToLocal) return;
  const t=meta.posePhase ?? 0,gather=THREE.MathUtils.clamp(t/.2,0,1),lift=THREE.MathUtils.clamp((t-.2)/.34,0,1);
  const landing=meta.landingProgress || 0;
  for(const side of ["left","right"]){
    if(!meta.twoHands&&meta.style!=="twohand"&&side!==meta.side) continue;
    const sign=side==="left"?-1:1;
    const ready=new THREE.Vector3(sign*(meta.twoHands?.19:.13)*char.profile.width,1.28*char.profile.height,.32*char.profile.width);
    if(meta.style==="tomahawk"||meta.style==="poster") {ready.y+=.33;ready.z=-.22;}
    if(meta.style==="contact") {ready.y-=.14;ready.z+=.12;}
    if(meta.style==="reverse") {ready.x+=sign*.28;ready.z=-.24;}
    if(meta.style==="windmill") {ready.x+=sign*.45;ready.y-=.25;}
    const local=meta.handStarts[side].clone().lerp(ready,gather*gather*(3-2*gather));
    const target=char.group.localToWorld(local).lerp(meta.handTargets[side],lift*lift*(3-2*lift));
    if(meta.style==="windmill"&&lift>0&&lift<1){
      const wheel=Math.sin(lift*Math.PI)*.35;
      target.x+=sign*wheel;target.y-=wheel*.7;
    }
    if(landing>0) target.lerp(char.group.localToWorld(meta.handStarts[side].clone()),landing*landing*(3-2*landing));
    if(char.playerModel){solveBasketballArmIK(char.playerModel,char.playerModel.sideBones[side],target);continue;}
    // The procedural player has independent limb meshes rather than bones.
    // Place those same pieces along the reach so its hand follows the finish.
    const hand=side==="left"?char.leftHand:char.rightHand,fore=side==="left"?char.leftForearm:char.rightForearm,arm=side==="left"?char.leftArm:char.rightArm;
    const wrist=char.group.worldToLocal(target),shoulder=new THREE.Vector3(sign*.34*char.profile.width,1.58*char.profile.height,0);
    const elbow=shoulder.clone().lerp(wrist,.5).add(new THREE.Vector3(sign*.1,-.08,0));
    hand.position.copy(wrist);arm.position.copy(shoulder).lerp(elbow,.5);fore.position.copy(elbow).lerp(wrist,.5);
    arm.quaternion.setFromUnitVectors(new THREE.Vector3(0,1,0),shoulder.clone().sub(elbow).normalize());
    fore.quaternion.setFromUnitVectors(new THREE.Vector3(0,1,0),elbow.clone().sub(wrist).normalize());
  }
}

function disposePlayerModel(char){
  const state=char?.playerModel;
  if(!state) return;
  state.mixer.stopAllAction();state.mixer.uncacheRoot(state.group);
  state.skeletons.forEach(skeleton=>skeleton.dispose());
  state.materials.forEach(material=>material.dispose());state.textures.forEach(texture=>texture.dispose());
  state.ownedGeometries.forEach(geometry=>geometry.dispose());
  state.hidden.forEach(({node,visible})=>{node.visible=visible;});
  state.group.removeFromParent();char.playerModel=null;
}
