/* Endless Breach 2.0: selected-atlas environment renderer and layered map data.
 * The existing game scales horizontal logical coordinates by 1.45; heights are
 * already world units. Keep collision and navigation in that same convention.
 * No dependencies; the original vertex-color renderer remains available.
 */
(function (root) {
  'use strict';
  const WORLD_SCALE = 1.45, EYE_HEIGHT = 1.86, STANDING_HEIGHT = 2.39, CLEARANCE_MARGIN = .18, STRIDE = 10, SURFACE_STRIDE = 13;
  const script = typeof document !== 'undefined' && document.currentScript;
  const BASE = script ? new URL('assets/survival/', script.src).href : 'assets/survival/';
  const names = ['Breach Ring','Derelict Station','Xenofall Wastes','Black Ice Cryolab','The Living Hive','Drowned Powerworks','Orbital Graveyard','The Swarm Heart'];
  const ids = ['breach_ring','derelict_station','xenofall_wastes','black_ice_cryolab','living_hive','drowned_powerworks','orbital_graveyard','swarm_heart'];
  const waves = [0,5,10,15,20,25,30,40];
  const descriptions = ['Fortify a military perimeter with watchtowers and cargo lanes.','Two abandoned platforms, curved tunnels and a crossing concourse.','Open alien canyons, bone arches, corrosive pools and crystal ridges.','Research rooms, a frozen courtyard and elevated coolant passages.','Winding ribbed chambers and membrane bridges above nesting grounds.','Flooded turbine halls, wet channels and powered maintenance bridges.','Wrecked docking bays, shattered hulls and low gravity observation decks.','An immense beating heart surrounded by branching living galleries.'];
  let selected = null, loadSerial = 0, revision = 0, navCache = null;
  const meshCache = new Map(), gpu = new Map();
  const surfaceIds = new WeakMap(); let nextSurfaceId = 0;
  const clamp = (n,a,b) => Math.max(a,Math.min(b,n));
  const inside = (x,z,b,pad=0) => Math.abs(x-b.x)<=b.w/2+pad && Math.abs(z-b.z)<=b.d/2+pad;
  const pathURL = (path,base=BASE) => new URL(path, new URL(base, typeof location !== 'undefined' ? location.href : 'http://localhost/')).href;
  function catalog() { return ids.map((id,i)=>({id,name:names[i],unlockWave:waves[i],description:descriptions[i],card:pathURL('tilesets/'+id+'/map_card.webp')})); }
  async function json(url) {
    const response=await fetch(url); if(!response.ok)throw new Error('Missing survival asset '+url+' ('+response.status+')');
    return response.json();
  }
  function imageLoad(url) {
    return new Promise((resolve,reject)=>{const image=new Image();image.onload=()=>{if(![1024,2048].includes(image.width)||image.height!==image.width){reject(new Error('Survival atlas must be 2048×2048 or its 1024×1024 mobile variant: '+url));return;}resolve(image);};image.onerror=()=>reject(new Error('Missing survival texture '+url));image.src=url;});
  }
  async function load(id, options={}) {
    if(!ids.includes(id))throw new Error('Unknown survival arena: '+id);
    if(selected?.blueprint.id===id && selected.image)return selected.blueprint;
    const serial=++loadSerial, base=options.baseUrl||BASE;
    const manifest=await json(pathURL('manifest.json',base)), entry=manifest.maps.find(m=>m.id===id);
    if(!entry)throw new Error('Arena missing from survival manifest: '+id);
    if(manifest.worldScale!==WORLD_SCALE)throw new Error('Survival manifest world scale differs from game');
    const [blueprint,tileset,prefabs]=await Promise.all([json(pathURL(entry.blueprint,base)),json(pathURL(entry.tileset,base)),json(pathURL(entry.prefabs,base))]);
    const prefabRoot=pathURL('tilesets/'+id+'/',base), definitions={};
    await Promise.all(prefabs.items.map(async item=>{const url=pathURL(item.file,prefabRoot),definition=await json(url),meshURL=pathURL(definition.geometry,url);if(!meshCache.has(meshURL))meshCache.set(meshURL,json(meshURL));definitions[item.id]={...definition,mesh:await meshCache.get(meshURL)};}));
    const mobile=options.mobile??(typeof matchMedia==='function'&&matchMedia('(max-width: 760px)').matches);
    const atlasURL=pathURL(mobile&&tileset.atlasMobile?tileset.atlasMobile:tileset.atlasColor,pathURL(entry.tileset,base));
    const image=options.noImage?null:await imageLoad(atlasURL);
    validateBlueprint(blueprint,tileset,definitions);
    if(serial!==loadSerial)return blueprint; // A later selection owns the renderer.
    disposeGPU(); selected={blueprint,tileset,definitions,image,atlasURL,objects:[],collision:[],dynamic:[],tileMap:new Map(tileset.tiles.map(t=>[t.id,t]))};
    revision++; navCache=null;
    return blueprint;
  }
  function validatePrefabCollision(mesh,label){
    if(!mesh?.collision?.length)throw new Error('Solid prefab has no physical shapes: '+label);
    for(const c of mesh.collision)if(![c.x,c.z,c.y,c.w,c.d,c.h].every(Number.isFinite)||Math.min(c.w,c.d,c.h)<=0)throw new Error('Invalid compound collision in '+label);
    for(let i=0;i<mesh.positions.length;i+=3){const x=mesh.positions[i],y=mesh.positions[i+1],z=mesh.positions[i+2];if(!mesh.collision.some(c=>Math.abs(x-c.x)<=c.w/2+.045&&Math.abs(z-c.z)<=c.d/2+.045&&y>=c.y-.045&&y<=c.y+c.h+.045))throw new Error('Visible mesh component lacks collision: '+label+' vertex '+i/3);}
    return true;
  }
  function validateBlueprint(b,t,prefabs) {
    if(b.coordinateSystem.horizontalScale!==WORLD_SCALE)throw new Error('Map horizontal scale invalid: '+b.id);
    const tileSet=new Set(t.tiles.map(tile=>tile.id));
    if(t.tiles.length!==64)throw new Error('Tileset must define 64 materials: '+t.id);
    for(const tile of t.tiles){const [x,y,w,h]=tile.pixelRect;if(x<8||y<8||w!==240||h!==240||x+w>2048||y+h>2048)throw new Error('Invalid padded atlas crop '+tile.id);}
    for(const object of [...b.walls,...b.platforms,...b.ramps,...b.floorRegions,...b.hazardZones])if(!tileSet.has(object.tile))throw new Error('Missing material '+object.tile+' in '+b.id);
    for(const prop of b.props){
      const mesh=prefabs[prop.prefab]?.mesh;
      if(!mesh)throw new Error('Missing prefab '+prop.prefab+' in '+b.id);
      if(prop.decorative&&!prop.background&&!prop.decorativeReason)throw new Error('Solid-looking prefab needs collision or an explicit scenery reason: '+b.id+'/'+prop.prefab);
    }
    const checked=new Set();
    for(const prop of b.props){
      if(prop.decorative||checked.has(prop.prefab))continue;checked.add(prop.prefab);
      validatePrefabCollision(prefabs[prop.prefab].mesh,b.id+'/'+prop.prefab);
    }
    if(!b.playerSpawns.length||b.spawnZones.length<4)throw new Error('Map has insufficient spawn routes: '+b.id);
    for(const r of b.ramps)if(Math.abs(r.toHeight-r.fromHeight)/(r.axis==='x'?r.w:r.d)>.6)throw new Error('Ramp too steep: '+b.id);
    const propSolids=[];
    for(const p of b.props)if(!p.decorative){const co=Math.cos(p.yaw||0),si=Math.sin(p.yaw||0);for(const c of prefabs[p.prefab].mesh.collision){const x=c.x*p.w,z=c.z*p.d;propSolids.push({x:p.x+x*co+z*si,z:p.z-x*si+z*co,w:Math.abs(c.w*p.w*co)+Math.abs(c.d*p.d*si),d:Math.abs(c.w*p.w*si)+Math.abs(c.d*p.d*co),y:(p.y||0)+c.y*p.h,h:c.h*p.h});}}
    const report=validateGeometry({...b,walls:[...b.walls,...propSolids]});if(!report.valid)throw new Error(report.errors.map(e=>e.message).join('; '));
    return true;
  }
  /* Pure release validator, shared by authored blueprints and the three engine
   * arenas. Use full standing model/collision height, never the eye height.
   * A grounded plinth is allowed; an apparently open low void is rejected.
   */
  function validateGeometry(input,options={}){
    const errors=[],standingHeight=options.standingHeight||STANDING_HEIGHT,minimum=standingHeight+CLEARANCE_MARGIN,scale=input.worldCoordinates?1:WORLD_SCALE;
    const normalize=(s,kind)=>({...s,x:s.x*scale,z:s.z*scale,w:s.w*scale,d:s.d*scale,bottom:input.worldCoordinates&&kind==='wall'?s.y-s.h/2:(s.y||0),kind});
    const walls=(input.walls||[]).filter(p=>p.active!==false).map(p=>normalize(p,'wall')),decks=(input.platforms||[]).filter(p=>p.active!==false).map(p=>normalize(p,'platform')),ramps=(input.ramps||[]).map(r=>({...r,x:r.x*(input.worldCoordinates?WORLD_SCALE:scale),z:r.z*(input.worldCoordinates?WORLD_SCALE:scale),w:r.w*(input.worldCoordinates?WORLD_SCALE:scale),d:r.d*(input.worldCoordinates?WORLD_SCALE:scale)}));
    for(const p of [...walls,...decks])if(![p.x,p.z,p.w,p.d,p.bottom,p.h].every(Number.isFinite)||Math.min(p.w,p.d,p.h)<=0)errors.push({code:'invalid-solid',message:(input.id||'arena')+': non-finite or empty solid'});
    for(let index=0;index<decks.length;index++){
      const deck=decks[index];if(deck.bottom<=.05||deck.rampCollision)continue;
      let failure=null;
      const nx=Math.max(2,Math.ceil(deck.w/1.1)),nz=Math.max(2,Math.ceil(deck.d/1.1));
      for(let ix=0;ix<=nx&&!failure;ix++)for(let iz=0;iz<=nz&&!failure;iz++){
        const x=deck.x-deck.w/2+.12+(deck.w-.24)*ix/nx,z=deck.z-deck.d/2+.12+(deck.d-.24)*iz/nz,supports=[0];
        for(const lower of decks){const top=lower.bottom+lower.h;if(lower!==deck&&top<deck.bottom-.03&&inside(x,z,lower))supports.push(top);}
        for(const r of ramps)if(inside(x,z,r)){const t=clamp((r.axis==='x'?(x-r.x)/r.w:(z-r.z)/r.d)+.5,0,1),h=r.fromHeight+(r.toHeight-r.fromHeight)*t;if(h<deck.bottom-.03)supports.push(h);}
        for(const support of supports){
          const gap=deck.bottom-support;if(gap>=minimum-.001)continue;
          // Solid ground-to-deck structures intentionally have no passage.
          const blocked=walls.some(w=>inside(x,z,w,.38)&&w.bottom<=support+.12&&w.bottom+w.h>support+.2)||decks.some(p=>p!==deck&&inside(x,z,p)&&p.bottom<=support+.05&&p.bottom+p.h>=deck.bottom-.03);
          if(!blocked){failure={code:'overhead-clearance',index,x,z,clearance:gap,required:minimum,message:(input.id||'arena')+': deck '+index+' has '+gap.toFixed(2)+'m overhead clearance; needs '+minimum.toFixed(2)+'m'};break;}
        }
      }
      if(failure)errors.push(failure);
    }
    // The same standing body must fit the complete ascent, including side lanes;
    // sparse center samples can miss a thin wall across an otherwise valid ramp.
    for(let index=0;index<ramps.length;index++){
      const r=ramps[index];let failure=null;
      for(let i=1;i<40&&!failure;i++)for(const lane of [-.3,0,.3]){
        const t=i/40,x=r.x+(r.axis==='x'?(t-.5)*r.w:lane*r.w),z=r.z+(r.axis==='z'?(t-.5)*r.d:lane*r.d),feet=r.fromHeight+(r.toHeight-r.fromHeight)*t;
        if(walls.some(w=>inside(x,z,w,.55)&&feet<w.bottom+w.h-.15&&feet+standingHeight>w.bottom+.05)){failure={code:'blocked-ramp',index,x,z,message:(input.id||'arena')+': ramp '+index+' has a solid obstacle in its standing route'};break;}
      }
      if(failure)errors.push(failure);
    }
    return {valid:errors.length===0,standingHeight,minimumClearance:minimum,errors};
  }
  function convert(b) {return {...b,x:b.x*WORLD_SCALE,z:b.z*WORLD_SCALE,w:(b.w||0)*WORLD_SCALE,d:(b.d||0)*WORLD_SCALE};}
  function physicalMaterial(source){
    if(source.material)return source.material;
    const prefab=source.prefab||'';
    if(/glass|cryopod|experiment_chamber/.test(prefab))return 'glass';
    if(/rock|cliff|stone|crystal|meteor|monolith/.test(prefab))return 'concrete';
    if(/organic|alien_heart|egg|brood|nest|cocoon|rib|biological|living_gateway|tendril|cactus/.test(prefab))return 'organic';
    if(prefab)return 'metal';
    return /hive|heart/.test(selected.blueprint.id)?'organic':/xenofall/.test(selected.blueprint.id)?'concrete':'metal';
  }
  function tag(piece,source) {
    if(!piece)return null;
    piece.survivalEnvironment=true;piece.survivalMaterial=source.tile;piece.mapPieceId=source.mapPieceId;piece.material=physicalMaterial(source);piece.prefab=source.prefab;piece.rampCollision=!!source.rampCollision;
    if(source.hp!=null && !source.safety){piece.hp=piece.maxHp=source.hp;piece.destructible=true;}
    if(source.tags?.includes('breach_perimeter'))piece.survivorEdgeWall=true;
    return piece;
  }
  /* callbacks use the old addWall/addPlat signatures and MUST return the added
   * engine piece. addNav accepts runtime node objects, addSpawn runtime objects.
   * The main engine keeps its existing collision, weapons and building objects.
   */
  function install(callbacks) {
    if(!selected)throw new Error('Load a survival arena before installing collision');
    const b=selected.blueprint; selected.sceneryOnly=false;selected.standardAmbient=null;selected.objects=[];selected.collision=[];selected.dynamic=[];
    let next=0;
    function addBox(source,kind='wall',visual=true) {
      source={...source,mapPieceId:b.id+':'+(++next)};
      const col=b.theme.floorA;
      const piece=kind==='platform'?callbacks.addPlatform(source.x,source.z,source.w,source.d,source.y,source.h,col,source.safety!==false && source.hp==null):callbacks.addWall(source.x,source.z,source.w,source.d,source.h,col,source.y||0,source.safety===true);
      tag(piece,source);
      const record={source,kind,piece,world:convert(source)};
      selected.collision.push(record);if(visual)selected.objects.push(record);
      return record;
    }
    // A safe continuous footing prevents water channels, decorative holes or
    // demolished platforms from trapping ground enemies and blocking a wave.
    addBox({x:0,z:0,w:b.size[0],d:b.size[1],y:-.6,h:.6,safety:true,tile:b.defaultFloor},'platform',false);
    for(const w of b.walls)addBox(w);
    for(const p of b.platforms)addBox(p,'platform');
    for(const r of b.ramps){
      selected.objects.push({source:r,kind:'ramp',world:convert(r)});
      const steps=Math.max(12,Math.ceil(Math.abs(r.toHeight-r.fromHeight)/.125)),length=r.axis==='x'?r.w:r.d;
      for(let i=0;i<steps;i++){
        const at=(i+.5)/steps,top=r.fromHeight+(r.toHeight-r.fromHeight)*at;
        if(top<.04)continue;
        const s={x:r.x,z:r.z,w:r.w,d:r.d,y:0,h:top,safety:true,tile:r.tile,rampCollision:true};
        s[r.axis]+=(at-.5)*length; s[r.axis==='x'?'w':'d']=length/steps+.012;
        addBox(s,'platform',false);
      }
    }
    for(const p of b.props){
      const definition=selected.definitions[p.prefab],source={...p,mapPieceId:b.id+':prop:'+(++next)},records=[];
      if(!p.decorative)for(const c of definition.mesh.collision||[]){
        const yaw=p.yaw||0,s=Math.sin(yaw),co=Math.cos(yaw),x=c.x*p.w,z=c.z*p.d;
        records.push(addBox({x:p.x+x*co+z*s,z:p.z-x*s+z*co,w:Math.abs(c.w*p.w*co)+Math.abs(c.d*p.d*s),d:Math.abs(c.w*p.w*s)+Math.abs(c.d*p.d*co),h:c.h*p.h,y:p.y+c.y*p.h,tile:p.tile,material:p.material,prefab:p.prefab,hp:p.hp||1200,safety:false},'wall',false));
      }
      const compoundPieces=records.map(r=>r.piece).filter(Boolean);for(const piece of compoundPieces)piece.compoundPieces=compoundPieces;
      selected.objects.push({source,kind:'mesh',world:convert(source),definition,collisionRecords:records});
    }
    const bound=b.boundaries,hx=b.size[0]/2,hz=b.size[1]/2;
    for(const edge of [{x:0,z:-hz,w:b.size[0],d:.6},{x:0,z:hz,w:b.size[0],d:.6},{x:-hx,z:0,w:.6,d:b.size[1]},{x:hx,z:0,w:.6,d:b.size[1]}])addBox({...edge,y:0,h:bound.height,tile:bound.tile,topTile:'upper_00',safety:true});
    // Reserve event geometry up front; only toggling active changes gameplay.
    for(const zone of b.hazardZones)if(zone.structure){const object=addBox({...zone.structure,y:0,safety:false,hp:650});if(object.piece)object.piece.active=false;object.eventZone=zone;selected.dynamic.push(object);}
    callbacks.setTheme?.({...b.theme});
    for(const s of b.playerSpawns)callbacks.addSpawn?.({x:s.x*WORLD_SCALE,z:s.z*WORLD_SCALE,y:EYE_HEIGHT+(s.y||0),r:s.r*WORLD_SCALE});
    selected.collisionBuckets=new Map();for(const record of selected.collision){const w=record.world;for(let x=Math.floor((w.x-w.w/2-1)/8);x<=Math.floor((w.x+w.w/2+1)/8);x++)for(let z=Math.floor((w.z-w.d/2-1)/8);z<=Math.floor((w.z+w.d/2+1)/8);z++){const key=x+':'+z;if(!selected.collisionBuckets.has(key))selected.collisionBuckets.set(key,[]);selected.collisionBuckets.get(key).push(record);}}
    for(const data of gpu.values()){data.dirty=true;data.objectStates.clear();}
    revision++;navCache=null;
    for(const node of navigation().nodes)callbacks.addNav?.({...node,y:node.y+EYE_HEIGHT,r:2.1});
    return {blueprint:b,theme:b.theme,objects:selected.objects,collision:selected.collision};
  }
  function installScenery(props,callbacks,ambientDetails=[]){
    if(!selected)throw new Error('Load the arena atlas before installing scenery');
    selected.sceneryOnly=true;selected.standardAmbient=ambientDetails;selected.objects=[];selected.collision=[];selected.dynamic=[];
    let next=0;const checked=new Set();
    for(const p of props){
      const definition=selected.definitions[p.prefab];if(!definition?.mesh?.collision?.length)throw new Error('Missing solid arena prefab '+p.prefab);
      if(!checked.has(p.prefab)){validatePrefabCollision(definition.mesh,'standard/'+p.prefab);checked.add(p.prefab);}
      const source={y:0,yaw:0,...p,mapPieceId:'arena:prop:'+(++next)},records=[],yaw=source.yaw,si=Math.sin(yaw),co=Math.cos(yaw);
      for(const c of definition.mesh.collision){
        const cx=c.x*p.w,cz=c.z*p.d,s={x:p.x+cx*co+cz*si,z:p.z-cx*si+cz*co,w:Math.abs(c.w*p.w*co)+Math.abs(c.d*p.d*si),d:Math.abs(c.w*p.w*si)+Math.abs(c.d*p.d*co),y:source.y+c.y*p.h,h:c.h*p.h,tile:p.tile,material:p.material,prefab:p.prefab,hp:p.hp||1500,mapPieceId:source.mapPieceId+':'+records.length};
        const piece=callbacks.addWall(s.x,s.z,s.w,s.d,s.h,'#506876',s.y,false);tag(piece,s);const record={source:s,kind:'wall',piece,world:convert(s)};selected.collision.push(record);records.push(record);
      }
      const compound=records.map(r=>r.piece).filter(Boolean);for(const piece of compound)piece.compoundPieces=compound;
      selected.objects.push({source,kind:'mesh',world:convert(source),definition,collisionRecords:records});
    }
    for(const data of gpu.values())data.dirty=true;
    revision++;navCache=null;return {objects:selected.objects,collision:selected.collision};
  }
  function rampHeight(x,z) {
    if(!selected)return -Infinity;
    let height=-Infinity;
    for(const ramp of selected.blueprint.ramps){const r=convert(ramp);if(!inside(x,z,r))continue;const t=clamp(((r.axis==='x'?x-r.x:z-r.z)/(r.axis==='x'?r.w:r.d))+.5,0,1);height=Math.max(height,r.fromHeight+(r.toHeight-r.fromHeight)*t);}
    return height;
  }
  function rampSupport(actor,eyeHeight=EYE_HEIGHT) {const h=rampHeight(actor.x,actor.z);return Number.isFinite(h)&&Math.abs(actor.y-eyeHeight-h)<.20;}
  function followRamp(actor,old,eyeHeight=EYE_HEIGHT) {
    if(!selected||actor.flying||actor.climbing||actor.vy>1)return false;
    const now=rampHeight(actor.x,actor.z),before=rampHeight(old.x,old.z),feet=old.y-eyeHeight;
    if(!Number.isFinite(now)||now<-.02)return false;
    const reachable=old.grounded && (Math.abs(feet-(Number.isFinite(before)?before:now))<.38 || Math.abs(feet-now)<.30);
    const landing=actor.vy<=0&&feet>=now-.08&&actor.y-eyeHeight<=now+.08;
    if(!reachable&&!landing)return false;
    actor.y=now+eyeHeight;actor.vy=0;actor.onGround=true;return true;
  }
  function active(record) {
    if(record.kind==='mesh')return !record.collisionRecords.length || record.collisionRecords.some(c=>c.piece?.active!==false);
    return record.piece?.active!==false;
  }
  function environmentStateChanged() { revision++; }
  function surfaceInfo(x,z,feet=0) {
    if(!selected)return {walkable:false,height:0,hazard:null,layer:0};
    const b=selected.blueprint,lim=b.size[0]/2*WORLD_SCALE;
    if(Math.abs(x)>lim-.9||Math.abs(z)>lim-.9)return {walkable:false,height:0,hazard:null,layer:0};
    let height=0,layer=0;
    for(const record of selected.objects){if(record.kind!=='platform'||!active(record))continue;const p=record.source,c=record.world,top=p.y+p.h;if(inside(x,z,c) && top<=feet+.65 && top>height){height=top;layer=1;}}
    const ramp=rampHeight(x,z);if(Number.isFinite(ramp)&&ramp<=feet+1.1){height=Math.max(height,ramp);layer=height>.4?1:0;}
    let obstacle=null;
    for(const record of selected.collisionBuckets?.get(Math.floor(x/8)+':'+Math.floor(z/8))||selected.collision){if(record.kind!=='wall'||!active(record))continue;const w=record.world;if(!inside(x,z,w,.55))continue;const bottom=w.y||0,top=bottom+w.h;if(height+STANDING_HEIGHT>bottom+.05&&height<top-.15){obstacle=record;break;}}
    const hazard=b.hazardZones.find(z=>inside(x,z,convert(z))&&Math.abs((z.y||0)-height)<2)||null;
    // Destructible defenses are routable goals. AI can request their target box.
    const destructible=obstacle?.piece?.destructible ?? (obstacle?.source.hp!=null);
    return {walkable:!obstacle || destructible,height,hazard,layer,obstacle:obstacle?.piece||null,destructible:!!destructible,cost:obstacle?4:hazard?.kind==='acid'?3:1};
  }
  function pointInfo(x,z,actor) {return surfaceInfo(x,z,actor?Math.max(0,(actor.y||EYE_HEIGHT)-(actor.eyeHeight||EYE_HEIGHT)):0);}
  function canTraverse(from,to,actor={}) {
    if(actor.flying)return true;
    const fromHeight=from.height??from.y??0,toHeight=to.height??to.y??0;
    const startGround=actor.y!=null?Math.max(0,actor.y-(actor.eyeHeight||EYE_HEIGHT)):fromHeight;
    const start=surfaceInfo(from.x,from.z,startGround),finish=surfaceInfo(to.x,to.z,toHeight);
    if(!start.walkable||!finish.walkable)return false;
    const distance=Math.hypot(to.x-from.x,to.z-from.z),steps=Math.max(2,Math.ceil(distance/.8));
    let previous=start.height;
    for(let i=1;i<=steps;i++){const t=i/steps,expected=fromHeight+(toHeight-fromHeight)*t,info=surfaceInfo(from.x+(to.x-from.x)*t,from.z+(to.z-from.z)*t,Math.max(expected,previous));if(!info.walkable||Math.abs(info.height-previous)>.75)return false;previous=info.height;}
    return Math.abs(previous-finish.height)<.8;
  }
  function navigation() {
    if(!selected)return {nodes:[],links:[],spawnZones:[],burrowZones:[],revision};
    if(navCache?.revision===revision)return navCache;
    const surfaceSignature=selected.objects.filter(o=>o.kind==='platform'||o.kind==='ramp').map(o=>active(o)?'1':'0').join('');
    // Most changes are a destroyed wall or a temporary membrane. They only
    // alter traversal cost, so preserve the expensive spatial/link assembly.
    // An actual floor/deck loss still regenerates the layered graph below.
    if(navCache?.surfaceSignature===surfaceSignature){const nodes=navCache.nodes.map(n=>({...n,cost:surfaceInfo(n.x,n.z,n.height).cost||1}));navCache={...navCache,nodes,revision};return navCache;}
    const b=selected.blueprint,nodes=[],links=[],unique=new Map(),step=b.navigation.gridStep||4,lim=40;
    function node(x,z,y,layer=0){const wx=x*WORLD_SCALE,wz=z*WORLD_SCALE,key=[Math.round(wx*10),Math.round(wz*10),Math.round(y*10)].join(':');if(unique.has(key))return unique.get(key);const info=surfaceInfo(wx,wz,y);if(!info.walkable)return null;const item={id:'n'+nodes.length,x:wx,z:wz,y:info.height,height:info.height,layer,cost:info.cost};nodes.push(item);unique.set(key,item);return item;}
    for(let z=-lim;z<=lim;z+=step)for(let x=-lim;x<=lim;x+=step)node(x,z,0);
    for(const p of b.platforms)for(let z=p.z-p.d/2+1;z<=p.z+p.d/2-1;z+=3)for(let x=p.x-p.w/2+1;x<=p.x+p.w/2-1;x+=3)node(x,z,p.y+p.h,1);
    for(const r of b.ramps){const length=r.axis==='x'?r.w:r.d,steps=Math.ceil(length/2);for(let i=0;i<=steps;i++){const t=i/steps;node(r.x+(r.axis==='x'?(t-.5)*r.w:0),r.z+(r.axis==='z'?(t-.5)*r.d:0),r.fromHeight+(r.toHeight-r.fromHeight)*t,1);}}
    for(const zone of b.spawnZones)node(zone.x,zone.z,zone.y||0);
    for(const s of b.playerSpawns)node(s.x,s.z,s.y||0);
    const climbLinks=[];
    for(const p of b.platforms)for(const side of [-1,1]){const ground=node(p.x+side*(p.w/2+.8),p.z,0),upper=node(p.x+side*(p.w/2-.8),p.z,p.y+p.h,1);if(ground&&upper&&upper.y-ground.y>2)climbLinks.push({from:ground.id,to:upper.id,kind:'climb',cost:Math.hypot(upper.x-ground.x,upper.z-ground.z)+Math.abs(upper.y-ground.y)});}
    // Small spatial buckets avoid an O(nodes²) graph build.
    const buckets=new Map(),cell=6.5;
    for(const n of nodes){const key=Math.floor(n.x/cell)+':'+Math.floor(n.z/cell);if(!buckets.has(key))buckets.set(key,[]);buckets.get(key).push(n);}
    for(const n of nodes){const bx=Math.floor(n.x/cell),bz=Math.floor(n.z/cell);for(let dx=-1;dx<=1;dx++)for(let dz=-1;dz<=1;dz++)for(const v of buckets.get((bx+dx)+':'+(bz+dz))||[]){if(Number(v.id.slice(1))<=Number(n.id.slice(1)))continue;const distance=Math.hypot(v.x-n.x,v.z-n.z);if(distance>6.7||distance<.05)continue;if(!canTraverse(n,v,{y:n.y+EYE_HEIGHT}))continue;const kind=Math.abs(v.y-n.y)>.25?'ramp':'walk';links.push({from:n.id,to:v.id,kind,cost:distance*Math.max(n.cost,v.cost)});}}
    // Climbers have explicit optional face connections; ordinary ground species
    // still reach every deck through the independent ramp/walk graph above.
    links.push(...climbLinks);
    navCache={nodes,links,revision,surfaceSignature,spawnZones:b.spawnZones.map(s=>({...s,x:s.x*WORLD_SCALE,z:s.z*WORLD_SCALE,r:s.r*WORLD_SCALE})),burrowZones:b.burrowZones.map(s=>({...s,x:s.x*WORLD_SCALE,z:s.z*WORLD_SCALE,r:s.r*WORLD_SCALE}))};
    return navCache;
  }
  function spawnPoint(kind='ground',rng=Math.random) {
    kind=kind==='fly'||kind==='flyer'?'fly':/burrow/i.test(kind)?'burrow':'ground';
    const zones=navigation().spawnZones.filter(s=>!s.types||s.types.includes(kind));
    if(!zones.length)return {x:0,z:0,y:EYE_HEIGHT};
    const offset=Math.floor(rng()*zones.length)%zones.length;
    for(let i=0;i<zones.length*6;i++){const s=zones[(offset+Math.floor(i/6))%zones.length],a=rng()*Math.PI*2,r=i%6===5?0:rng()*s.r*.6,x=s.x+Math.cos(a)*r,z=s.z+Math.sin(a)*r,info=surfaceInfo(x,z,s.y||0);if(info.walkable&&!info.obstacle)return {x,z,y:info.height+EYE_HEIGHT};}
    const safe=navigation().nodes.find(n=>Math.hypot(n.x,n.z)>20&&!surfaceInfo(n.x,n.z,n.height).obstacle);if(safe)return {x:safe.x,z:safe.z,y:safe.height+EYE_HEIGHT};
    throw new Error('No physical enemy spawn route in '+selected.blueprint.id);
  }
  function pickupPoint(rng=Math.random) {
    if(!selected)return {x:0,z:0,y:0};
    const zones=selected.blueprint.pickupRegions;
    for(let i=0;i<24;i++){const s=zones[Math.floor(rng()*zones.length)%zones.length],x=(s.x+(rng()-.5)*s.w)*WORLD_SCALE,z=(s.z+(rng()-.5)*s.d)*WORLD_SCALE,info=pointInfo(x,z);if(info.walkable&&!info.obstacle&&!info.hazard)return {x,z,y:info.height};}
    return {x:selected.blueprint.playerSpawns[0].x*WORLD_SCALE,z:selected.blueprint.playerSpawns[0].z*WORLD_SCALE,y:0};
  }
  function canBuild(x,z,y=0) {return !!selected?.blueprint.buildRegions.some(r=>inside(x,z,convert(r))&&y<r.maxHeight)&&!pointInfo(x,z,{y:y+EYE_HEIGHT}).hazard;}

  /* Textured surfaces use atlas-local UVs baked once into cached static chunks.
   * No mipmaps: 8px gutters and half-texel inner insets keep neighboring slots
   * out of both minified and magnified samples, including mobile WebGL1.
   */
  function material(id) {return selected.tileMap.get(id)||selected.tileMap.get('floor_00');}
  function vertex(out,p,n,u,v,tile,pulse=0){const t=material(tile),uv=t.uv;out.push(p[0],p[1],p[2],uv[0]+clamp(u,0,1)*(uv[2]-uv[0]),uv[1]+clamp(v,0,1)*(uv[3]-uv[1]),n[0],n[1],n[2],t.emissive||0,pulse);}
  function quad(out,a,b,c,d,tile,n=[0,1,0],pulse=0){for(const [p,u,v] of [[a,0,0],[b,1,0],[c,1,1],[a,0,0],[c,1,1],[d,0,1]])vertex(out,p,n,u,v,tile,pulse);}
  function subdividedFace(out,a,b,c,d,tile,n,unit=5.8){
    const width=Math.hypot(...a.map((v,i)=>v-b[i])),height=Math.hypot(...a.map((v,i)=>v-d[i])),nx=Math.max(1,Math.ceil(width/unit)),ny=Math.max(1,Math.ceil(height/unit));
    const p=(u,v)=>a.map((value,i)=>value+(b[i]-value)*u+(d[i]-value)*v);
    for(let x=0;x<nx;x++)for(let y=0;y<ny;y++)quad(out,p(x/nx,y/ny),p((x+1)/nx,y/ny),p((x+1)/nx,(y+1)/ny),p(x/nx,(y+1)/ny),tile,n);
  }
  function boxGeometry(out,o,kind){const x=o.world.x,z=o.world.z,w=o.world.w/2,d=o.world.d/2,s=o.source,y0=s.y||0,y1=y0+s.h;const A=[x-w,y0,z-d],B=[x+w,y0,z-d],C=[x+w,y0,z+d],D=[x-w,y0,z+d],E=[x-w,y1,z-d],F=[x+w,y1,z-d],G=[x+w,y1,z+d],H=[x-w,y1,z+d];subdividedFace(out,E,F,G,H,s.topTile||(kind==='platform'?s.tile:'upper_00'),[0,1,0]);subdividedFace(out,A,D,C,B,s.sideTile||(kind==='platform'?'trim_01':s.tile),[0,-1,0]);subdividedFace(out,A,E,H,D,s.sideTile||s.tile,[-1,0,0]);subdividedFace(out,B,C,G,F,s.sideTile||s.tile,[1,0,0]);subdividedFace(out,A,B,F,E,s.sideTile||s.tile,[0,0,-1]);subdividedFace(out,D,H,G,C,s.sideTile||s.tile,[0,0,1]);}
  /* Generic engine surfaces already use world-space x/z dimensions. Unlike
   * blueprint walls, engine wall y is its center; platform y is its base.
   * UVs repeat by tessellating inside the padded slot, never across the atlas.
   */
  function surfaceKey(piece){if(piece.id!=null)return typeof piece.id+':'+piece.id;if(!surfaceIds.has(piece))surfaceIds.set(piece,++nextSurfaceId);return 'object:'+surfaceIds.get(piece);}
  function surfaceDefinition(piece,options={}){
    if(!selected||!piece||piece.active===false||piece.survivalEnvironment||!['wall','platform'].includes(piece.type))return null;
    const {x,z,w,d,h}=piece,y=piece.y??0;
    if(![x,z,w,d,h,y].every(Number.isFinite)||Math.min(w,d,h)<=0)return null;
    const bottom=piece.type==='wall'?y-h/2:y,unit=Number.isFinite(options.tileUnit)&&options.tileUnit>0?Math.max(.5,options.tileUnit):5.8;
    const side=piece.surfaceSideTile||piece.surfaceTile||piece.tile||(piece.type==='wall'?'wall_00':'wall_01');
    const top=piece.surfaceTopTile||(piece.type==='wall'?'upper_00':piece.builtBy?'floor_01':'floor_00'),under=piece.surfaceBottomTile||'trim_01';
    const color=piece.surfaceTint||piece.tint||piece.c,hex=typeof color==='string'&&/^#[\da-f]{6}$/i.test(color)?parseInt(color.slice(1),16):null;
    let tint=Array.isArray(color)&&color.length>=3?color.slice(0,3).map(v=>Number.isFinite(v)?clamp(v,0,1):1):hex==null?[1,1,1]:[16,8,0].map(shift=>.82+((hex>>shift)&255)/255*.18);
    const health=Number.isFinite(piece.hp)&&Number.isFinite(piece.maxHp)&&piece.maxHp>0?clamp(piece.hp/piece.maxHp,0,1):1,damage=health>.75?0:health>.5?1:health>.25?2:3;
    tint=tint.map((v,i)=>v*(1-damage*.12)+(i===0?damage*.015:0));
    if(piece.armorBoosted||piece.bonusHp>0)tint=tint.map((v,i)=>i===0?v*.92:Math.min(1,v*1.04));
    const key=surfaceKey(piece),chunk=chunkKey(x,z),signature=[x,z,w,d,h,bottom,unit,side,top,under,...tint].join('|');
    return {key,pieceId:piece.id??key,chunk,signature,x,y:bottom+h/2,z,w,d,h,bottom,unit,side,top,under,tint,r:Math.hypot(w,d)/2};
  }
  function surfaceFace(out,a,b,c,d,tile,n,unit,tint){
    const width=Math.hypot(...a.map((v,i)=>v-b[i])),height=Math.hypot(...a.map((v,i)=>v-d[i])),nx=Math.ceil(width/unit),ny=Math.ceil(height/unit);
    const point=(u,v)=>a.map((value,i)=>value+(b[i]-value)*u+(d[i]-value)*v);
    for(let x=0;x<nx;x++)for(let y=0;y<ny;y++){
      const x0=x*unit/width,x1=Math.min(1,(x+1)*unit/width),y0=y*unit/height,y1=Math.min(1,(y+1)*unit/height),u=(x1-x0)*width/unit,v=(y1-y0)*height/unit;
      const A=point(x0,y0),B=point(x1,y0),C=point(x1,y1),D=point(x0,y1);
      for(const [p,tu,tv] of [[A,0,0],[B,u,0],[C,u,v],[A,0,0],[C,u,v],[D,0,v]]){vertex(out,p,n,tu,tv,tile);out.push(...tint);}
    }
  }
  function surfaceGeometry(s){
    const out=[],x=s.x,z=s.z,w=s.w/2,d=s.d/2,y0=s.bottom,y1=y0+s.h,A=[x-w,y0,z-d],B=[x+w,y0,z-d],C=[x+w,y0,z+d],D=[x-w,y0,z+d],E=[x-w,y1,z-d],F=[x+w,y1,z-d],G=[x+w,y1,z+d],H=[x-w,y1,z+d];
    for(const [a,b,c,d,tile,n] of [[E,F,G,H,s.top,[0,1,0]],[A,D,C,B,s.under,[0,-1,0]],[A,E,H,D,s.side,[-1,0,0]],[B,C,G,F,s.side,[1,0,0]],[A,B,F,E,s.side,[0,0,-1]],[D,H,G,C,s.side,[0,0,1]]])surfaceFace(out,a,b,c,d,tile,n,s.unit,s.tint);
    return new Float32Array(out);
  }
  function assembleSurfaceChunks(states,onlyKeys=null){
    const chunks=new Map();
    for(const s of states.values()){
      if(onlyKeys&&!onlyKeys.has(s.chunk))continue;
      if(!chunks.has(s.chunk)){const [cx,cz]=s.chunk.split(':').map(Number);chunks.set(s.chunk,{key:s.chunk,x:cx*24+12,z:cz*24+12,r:0,arrays:[],length:0,pieceIds:[]});}
      const c=chunks.get(s.chunk);c.arrays.push(s.data);c.length+=s.data.length;c.pieceIds.push(s.pieceId);c.r=Math.max(c.r,Math.hypot(s.x-c.x,s.z-c.z)+s.r);
    }
    return [...chunks.values()].map(c=>{const data=new Float32Array(c.length);let offset=0;for(const array of c.arrays){data.set(array,offset);offset+=array.length;}return {key:c.key,x:c.x,z:c.z,r:c.r,pieceIds:c.pieceIds,data};});
  }
  function buildSurfaceChunks(pieces,options={}){const states=new Map();for(const piece of pieces||[]){const s=surfaceDefinition(piece,options);if(s)states.set(s.key,{...s,data:surfaceGeometry(s)});}return assembleSurfaceChunks(states);}
  function floorGeometry(out){const b=selected.blueprint,step=4,half=b.size[0]/2;for(let x=-half;x<half;x+=step)for(let z=-half;z<half;z+=step){const xm=x+step/2,zm=z+step/2;let tile=b.defaultFloor;for(const r of b.floorRegions)if(inside(xm,zm,r))tile=r.tile;for(const r of b.hazardZones)if(inside(xm,zm,r)&&!(r.y>0))tile=r.tile;const y=.015;quad(out,[x*WORLD_SCALE,y,z*WORLD_SCALE],[(x+step)*WORLD_SCALE,y,z*WORLD_SCALE],[(x+step)*WORLD_SCALE,y,(z+step)*WORLD_SCALE],[x*WORLD_SCALE,y,(z+step)*WORLD_SCALE],tile);}}
  function rampGeometry(out,o){const r=o.world,source=o.source,a=source.fromHeight+.025,b=source.toHeight+.025,x=r.x,z=r.z,w=r.w/2,d=r.d/2;const corners=r.axis==='x'?[[x-w,a,z-d],[x+w,b,z-d],[x+w,b,z+d],[x-w,a,z+d]]:[[x-w,a,z-d],[x+w,a,z-d],[x+w,b,z+d],[x-w,b,z+d]];const dx=corners[1][0]-corners[0][0],dy=corners[1][1]-corners[0][1],dz=corners[3][2]-corners[0][2],dyz=corners[3][1]-corners[0][1],norm=[-dy/(dx||1),1,-dyz/(dz||1)],len=Math.hypot(...norm);subdividedFace(out,...corners,source.tile,norm.map(n=>n/len));}
  function meshGeometry(out,o){const mesh=o.definition.mesh,s=o.source,yaw=s.yaw||0,co=Math.cos(yaw),si=Math.sin(yaw),world=o.world;for(const index of mesh.indices){const ix=index*3,uv=index*2,x=mesh.positions[ix]*world.w,z=mesh.positions[ix+2]*world.d,nx=mesh.normals[ix]/world.w,ny=mesh.normals[ix+1]/s.h,nz=mesh.normals[ix+2]/world.d,len=Math.hypot(nx,ny,nz)||1;vertex(out,[world.x+x*co+z*si,(s.y||0)+mesh.positions[ix+1]*s.h,world.z-x*si+z*co],[(nx*co+nz*si)/len,ny/len,(-nx*si+nz*co)/len],mesh.uvs[uv],mesh.uvs[uv+1],s.tile,s.pulse?1:0);if(s.surfaceType==='planet')out[out.length-2]=-2;if(s.surfaceType==='stars')out[out.length-2]=-3;}}
  function chunkKey(x,z){return Math.floor(x/24)+':'+Math.floor(z/24);}
  function buildChunks(onlyKeys=null){const chunks=new Map(),size=24;function get(x,z){const key=chunkKey(x,z);if(onlyKeys&&!onlyKeys.has(key))return null;if(!chunks.has(key))chunks.set(key,{key,data:[],x:Math.floor(x/size)*size+size/2,z:Math.floor(z/size)*size+size/2,r:34});return chunks.get(key).data;}const floor=[];if(!selected.sceneryOnly)floorGeometry(floor);for(let i=0;i<floor.length;i+=STRIDE*6){const x=floor[i],z=floor[i+2],out=get(x,z);if(out)out.push(...floor.slice(i,i+STRIDE*6));}for(const o of selected.objects)if(active(o)){const out=get(o.world.x,o.world.z);if(!out)continue;if(o.source.background)chunks.get(chunkKey(o.world.x,o.world.z)).background=true;if(o.kind==='mesh')meshGeometry(out,o);else if(o.kind==='ramp')rampGeometry(out,o);else boxGeometry(out,o,o.kind);}return [...chunks.values()].map(c=>({...c,data:new Float32Array(c.data)}));}
  // A tiny shared atlas buffer supplies motion without lights, simulations or
  // per-detail draw calls. Pure scenery has no physical/collision representation.
  function buildAmbientGeometry(time=0,quality='high'){
    const out=[],details=selected?.standardAmbient||selected?.blueprint.ambientDetails||[],budget=quality==='low'?8:18;
    for(let index=0;index<Math.min(details.length,budget);index++){
      const s=details[index],x=s.x*WORLD_SCALE,z=s.z*WORLD_SCALE,y=s.y||1,phase=time+index*1.73;
      if(s.kind==='fan'){
        for(let i=0;i<6;i++){const a=phase*2.6+i*Math.PI/3,b=a+.24,r=s.r||1.4;
          quad(out,[x+Math.cos(a)*.25,y+Math.sin(a)*.25,z],[x+Math.cos(a)*r,y+Math.sin(a)*r,z],[x+Math.cos(b)*r,y+Math.sin(b)*r,z],[x+Math.cos(b)*.25,y+Math.sin(b)*.25,z],'prop_03',[0,0,-1]);}
      }else{
        const count=['steam','dust','spore'].includes(s.kind)?3:1;
        for(let i=0;i<count;i++){
          const t=(phase*.55+i/count)%1,steam=s.kind==='steam',drift=Math.sin(phase*.7+i)*.2,cy=y+(steam?t*1.8:Math.sin(phase+i)*.08),size=steam?.10+t*.16:s.kind==='pulse'?.19+Math.sin(phase*2.3)*.04:.09;
          if(s.kind==='spark'&&Math.sin(phase*23)<.55||s.kind==='flicker'&&Math.sin(phase*17)<-.2)continue;
          quad(out,[x+drift-size,cy-size,z],[x+drift+size,cy-size,z],[x+drift+size,cy+size,z],[x+drift-size,cy+size,z],steam?'trim_04':'prop_05',[0,0,-1]);
        }
      }
    }
    return new Float32Array(out);
  }
  function compile(gl,type,source){const shader=gl.createShader(type);gl.shaderSource(shader,source);gl.compileShader(shader);if(!gl.getShaderParameter(shader,gl.COMPILE_STATUS))throw new Error(gl.getShaderInfoLog(shader));return shader;}
  const VS='attribute vec3 aPos;attribute vec2 aUV;attribute vec3 aNormal;attribute vec2 aEffects;attribute vec3 aTint;uniform vec3 uCamera;uniform vec3 uRight;uniform vec3 uUp;uniform vec3 uForward;uniform float uAspect;uniform mediump float uTime;varying vec2 vUV;varying vec3 vNormal;varying vec3 vView;varying vec2 vEffects;varying vec3 vTint;varying float vDistance;void main(){vec3 pos=aPos+aNormal*aEffects.y*sin(uTime*2.3)*.075;vec3 d=pos-uCamera;vec3 c=vec3(dot(d,uRight),dot(d,uUp),dot(d,uForward));float n=.055;float f=180.;gl_Position=vec4(c.x*1.44*uAspect,c.y*1.44,(f+n)/(f-n)*c.z-2.*f*n/(f-n),c.z);vUV=aUV;vNormal=aNormal;vView=-d;vEffects=aEffects;vTint=aTint;vDistance=length(d);}';
  const FS='precision mediump float;uniform sampler2D uAtlas;uniform vec3 uFog;uniform float uFogDensity;uniform float uAmbient;uniform mediump float uTime;varying vec2 vUV;varying vec3 vNormal;varying vec3 vView;varying vec2 vEffects;varying vec3 vTint;varying float vDistance;void main(){vec4 tex=texture2D(uAtlas,vUV);vec3 n=normalize(vNormal);float light=uAmbient+.48*max(0.,dot(n,normalize(vec3(.25,.9,.4))));if(vEffects.x<-2.5){gl_FragColor=vec4(vec3(.75,.84,1.)*(.85+.15*sin(uTime*.4+vDistance)),1.);return;}if(vEffects.x<-1.5){float land=smoothstep(.04,.14,sin(vUV.x*340.)*sin(vUV.y*210.)+.30*sin(vUV.x*730.+vUV.y*630.));float cloud=smoothstep(.32,.75,sin(vUV.y*820.+sin(vUV.x*240.)*5.)*.5+.45);vec3 ocean=vec3(.025,.13,.27),continent=vec3(.12,.27,.21);vec3 planet=mix(mix(ocean,continent,land),vec3(.73,.86,.92),cloud*.68);planet=mix(planet,tex.rgb,.15);float atmosphere=pow(1.-max(0.,dot(n,normalize(vView))),3.);planet=planet*(.16+.85*max(0.,dot(n,normalize(vec3(-.3,.8,.5)))))+vec3(.06,.35,.65)*atmosphere;gl_FragColor=vec4(planet,1.);return;}float glow=vEffects.x*(.80+.20*sin(uTime*2.3));vec3 color=tex.rgb*(light+glow)*vTint;float fog=max(smoothstep(75.,170.,vDistance)*.64,smoothstep(10.,48.,vDistance)*uFogDensity);gl_FragColor=vec4(mix(color,uFog,fog),1.);}';
  function createGPU(gl){const program=gl.createProgram(),vs=compile(gl,gl.VERTEX_SHADER,VS),fs=compile(gl,gl.FRAGMENT_SHADER,FS);gl.attachShader(program,vs);gl.attachShader(program,fs);gl.linkProgram(program);gl.deleteShader(vs);gl.deleteShader(fs);if(!gl.getProgramParameter(program,gl.LINK_STATUS))throw new Error(gl.getProgramInfoLog(program));const texture=gl.createTexture();gl.bindTexture(gl.TEXTURE_2D,texture);gl.pixelStorei(gl.UNPACK_FLIP_Y_WEBGL,false);gl.texImage2D(gl.TEXTURE_2D,0,gl.RGBA,gl.RGBA,gl.UNSIGNED_BYTE,selected.image);gl.texParameteri(gl.TEXTURE_2D,gl.TEXTURE_MIN_FILTER,gl.LINEAR);gl.texParameteri(gl.TEXTURE_2D,gl.TEXTURE_MAG_FILTER,gl.LINEAR);gl.texParameteri(gl.TEXTURE_2D,gl.TEXTURE_WRAP_S,gl.CLAMP_TO_EDGE);gl.texParameteri(gl.TEXTURE_2D,gl.TEXTURE_WRAP_T,gl.CLAMP_TO_EDGE);const uniforms={};for(const name of ['uCamera','uRight','uUp','uForward','uAspect','uTime','uAtlas','uFog','uFogDensity','uAmbient'])uniforms[name]=gl.getUniformLocation(program,name);const attrs={};for(const name of ['aPos','aUV','aNormal','aEffects','aTint'])attrs[name]=gl.getAttribLocation(program,name);const data={program,texture,uniforms,attrs,chunks:[],dirty:true,signature:'',objectStates:new Map(),surfaces:new Map(),surfaceChunks:new Map(),surfaceUploads:0,surfaceGeometryBuilds:0,surfaceDrawCalls:0};const vaoExt=gl.getExtension?.('OES_vertex_array_object');if(vaoExt){data.vaoExt=vaoExt;data.vao=vaoExt.createVertexArrayOES();}gpu.set(gl,data);return data;}
  function stateSignature(){return selected.collision.map(r=>r.piece?.active===false?'0':'1').join('');}
  function beginAtlasDraw(gl,camera,options={},environment=false){
    const previous={program:gl.getParameter(gl.CURRENT_PROGRAM),buffer:gl.getParameter(gl.ARRAY_BUFFER_BINDING),active:gl.getParameter(gl.ACTIVE_TEXTURE)};
    gl.activeTexture(gl.TEXTURE0);previous.texture=gl.getParameter(gl.TEXTURE_BINDING_2D);
    const data=gpu.get(gl)||createGPU(gl),u=data.uniforms,a=data.attrs;
    if(data.vao){
      // A private VAO avoids dozens of driver queries on every split-screen view.
      previous.vao=gl.getParameter(data.vaoExt.VERTEX_ARRAY_BINDING_OES);previous.vaoExt=data.vaoExt;
      if(a.aTint>=0)previous.tint={attr:a.aTint,value:gl.getVertexAttrib(a.aTint,gl.CURRENT_VERTEX_ATTRIB)};
      data.vaoExt.bindVertexArrayOES(data.vao);
    }else previous.attrs=Object.values(a).filter(attr=>attr>=0).map(attr=>({attr,enabled:gl.getVertexAttrib(attr,gl.VERTEX_ATTRIB_ARRAY_ENABLED),buffer:gl.getVertexAttrib(attr,gl.VERTEX_ATTRIB_ARRAY_BUFFER_BINDING),size:gl.getVertexAttrib(attr,gl.VERTEX_ATTRIB_ARRAY_SIZE),type:gl.getVertexAttrib(attr,gl.VERTEX_ATTRIB_ARRAY_TYPE),normalized:gl.getVertexAttrib(attr,gl.VERTEX_ATTRIB_ARRAY_NORMALIZED),stride:gl.getVertexAttrib(attr,gl.VERTEX_ATTRIB_ARRAY_STRIDE),offset:gl.getVertexAttribOffset(attr,gl.VERTEX_ATTRIB_ARRAY_POINTER),value:gl.getVertexAttrib(attr,gl.CURRENT_VERTEX_ATTRIB)}));
    gl.useProgram(data.program);gl.bindTexture(gl.TEXTURE_2D,data.texture);gl.uniform1i(u.uAtlas,0);gl.uniform3fv(u.uCamera,camera.position);gl.uniform3fv(u.uRight,camera.right);gl.uniform3fv(u.uUp,camera.up);gl.uniform3fv(u.uForward,camera.forward);gl.uniform1f(u.uAspect,camera.aspect);gl.uniform1f(u.uTime,options.time??performance.now()/1000);gl.uniform3fv(u.uFog,options.fog||selected.blueprint.theme.sky);
    const events=environment?eventStatus(selected.eventClock||0):[],fog=events.some(e=>e.type==='fog'&&e.state==='active'),blackout=events.some(e=>e.type==='visibility'&&e.state==='active');
    gl.uniform1f(u.uFogDensity,fog?.68:0);gl.uniform1f(u.uAmbient,blackout?.28:.50);
    for(const attr of [a.aPos,a.aUV,a.aNormal,a.aEffects])if(attr>=0)gl.enableVertexAttribArray(attr);
    return {data,previous};
  }
  function atlasAttributes(gl,a,stride){
    gl.vertexAttribPointer(a.aPos,3,gl.FLOAT,false,stride*4,0);gl.vertexAttribPointer(a.aUV,2,gl.FLOAT,false,stride*4,12);gl.vertexAttribPointer(a.aNormal,3,gl.FLOAT,false,stride*4,20);gl.vertexAttribPointer(a.aEffects,2,gl.FLOAT,false,stride*4,32);
    if(a.aTint>=0){if(stride===SURFACE_STRIDE){gl.enableVertexAttribArray(a.aTint);gl.vertexAttribPointer(a.aTint,3,gl.FLOAT,false,stride*4,40);}else{gl.disableVertexAttribArray(a.aTint);gl.vertexAttrib3f(a.aTint,1,1,1);}}
  }
  function endAtlasDraw(gl,previous){
    if(previous.vaoExt){if(previous.tint)gl.vertexAttrib4fv(previous.tint.attr,previous.tint.value);previous.vaoExt.bindVertexArrayOES(previous.vao);}
    else for(const state of previous.attrs){if(state.buffer){gl.bindBuffer(gl.ARRAY_BUFFER,state.buffer);gl.vertexAttribPointer(state.attr,state.size,state.type,state.normalized,state.stride,state.offset);}if(state.enabled)gl.enableVertexAttribArray(state.attr);else gl.disableVertexAttribArray(state.attr);gl.vertexAttrib4fv(state.attr,state.value);}
    gl.bindBuffer(gl.ARRAY_BUFFER,previous.buffer);gl.bindTexture(gl.TEXTURE_2D,previous.texture);gl.activeTexture(previous.active);gl.useProgram(previous.program);
  }
  function render(gl,camera,options={}){
    if(!selected?.image||!selected.objects.length)return false;
    const {data,previous}=beginAtlasDraw(gl,camera,options,!selected.sceneryOnly),signature=stateSignature(),changed=new Set();
    if(signature!==data.signature){data.signature=signature;revision++;for(const object of selected.objects){const state=active(object),key=object.source.mapPieceId||object; if(data.objectStates.get(key)!==state)changed.add(chunkKey(object.world.x,object.world.z));data.objectStates.set(key,state);}}
    if(data.dirty||changed.size){const update=buildChunks(data.dirty?null:changed),replacement=new Set(update.map(c=>c.key));const retained=[];for(const c of data.chunks){if(data.dirty||changed.has(c.key)||replacement.has(c.key))gl.deleteBuffer(c.buffer);else retained.push(c);}data.chunks=retained.concat(update.map(c=>{const buffer=gl.createBuffer();gl.bindBuffer(gl.ARRAY_BUFFER,buffer);gl.bufferData(gl.ARRAY_BUFFER,c.data,gl.STATIC_DRAW);return {key:c.key,background:c.background,buffer,count:c.data.length/STRIDE,x:c.x,z:c.z,r:c.r};}));data.dirty=false;}
    const distance=options.quality==='low'?105:options.quality==='medium'?145:180;
    for(const c of data.chunks){if(!c.background&&Math.hypot(c.x-camera.position[0],c.z-camera.position[2])>distance+c.r)continue;gl.bindBuffer(gl.ARRAY_BUFFER,c.buffer);atlasAttributes(gl,data.attrs,STRIDE);gl.drawArrays(gl.TRIANGLES,0,c.count);}
    if((selected.standardAmbient||selected.blueprint.ambientDetails)?.length){
      const time=options.time||0,key=Math.floor(time*30)+':'+options.quality;
      if(data.ambientKey!==key){const ambient=buildAmbientGeometry(time,options.quality);data.ambientBuffer ||= gl.createBuffer();gl.bindBuffer(gl.ARRAY_BUFFER,data.ambientBuffer);gl.bufferData(gl.ARRAY_BUFFER,ambient,gl.DYNAMIC_DRAW||gl.STATIC_DRAW);data.ambientCount=ambient.length/STRIDE;data.ambientKey=key;}
      gl.bindBuffer(gl.ARRAY_BUFFER,data.ambientBuffer);atlasAttributes(gl,data.attrs,STRIDE);gl.drawArrays(gl.TRIANGLES,0,data.ambientCount);
    }
    endAtlasDraw(gl,previous);return true;
  }
  function renderSurfaces(gl,camera,pieces,options={}){
    if(!selected?.image)return false;
    const {data,previous}=beginAtlasDraw(gl,camera,options,options.environment===true),seen=new Set(),changed=new Set();
    for(const piece of pieces||[]){
      const s=surfaceDefinition(piece,options);if(!s)continue;seen.add(s.key);const old=data.surfaces.get(s.key);
      if(old?.signature===s.signature)continue;
      if(old)changed.add(old.chunk);changed.add(s.chunk);data.surfaces.set(s.key,{...s,data:surfaceGeometry(s)});data.surfaceGeometryBuilds++;
    }
    for(const [key,old] of data.surfaces)if(!seen.has(key)){data.surfaces.delete(key);changed.add(old.chunk);}
    if(changed.size){
      for(const key of changed){const old=data.surfaceChunks.get(key);if(old)gl.deleteBuffer(old.buffer);data.surfaceChunks.delete(key);}
      for(const chunk of assembleSurfaceChunks(data.surfaces,changed)){const buffer=gl.createBuffer();gl.bindBuffer(gl.ARRAY_BUFFER,buffer);gl.bufferData(gl.ARRAY_BUFFER,chunk.data,gl.STATIC_DRAW);data.surfaceChunks.set(chunk.key,{...chunk,data:undefined,buffer,count:chunk.data.length/SURFACE_STRIDE});data.surfaceUploads++;}
    }
    const distance=options.quality==='low'?105:options.quality==='medium'?145:180;data.surfaceDrawCalls=0;
    for(const c of data.surfaceChunks.values()){if(Math.hypot(c.x-camera.position[0],c.z-camera.position[2])>distance+c.r)continue;gl.bindBuffer(gl.ARRAY_BUFFER,c.buffer);atlasAttributes(gl,data.attrs,SURFACE_STRIDE);gl.drawArrays(gl.TRIANGLES,0,c.count);data.surfaceDrawCalls++;}
    endAtlasDraw(gl,previous);return true;
  }
  function surfaceStats(gl){const data=gpu.get(gl);return {chunks:data?.surfaceChunks.size||0,uploads:data?.surfaceUploads||0,vertices:data?[...data.surfaceChunks.values()].reduce((sum,c)=>sum+c.count,0):0,pieces:data?.surfaces.size||0,geometryBuilds:data?.surfaceGeometryBuilds||0,drawCalls:data?.surfaceDrawCalls||0};}
  function disposeGPU(){for(const [gl,data] of gpu){for(const c of data.chunks)gl.deleteBuffer(c.buffer);for(const c of data.surfaceChunks.values())gl.deleteBuffer(c.buffer);if(data.ambientBuffer)gl.deleteBuffer(data.ambientBuffer);if(data.vao)data.vaoExt.deleteVertexArrayOES(data.vao);gl.deleteTexture(data.texture);gl.deleteProgram(data.program);}gpu.clear();}
  function dispose(){loadSerial++;disposeGPU();selected=null;navCache=null;revision++;}
  function eventStatus(time,wave=1){if(!selected)return [];const b=selected.blueprint;const events=b.hazardZones.map((zone,index)=>{if(!zone.period)return {...zone,index,state:'passive',x:zone.x*WORLD_SCALE,z:zone.z*WORLD_SCALE,w:zone.w*WORLD_SCALE,d:zone.d*WORLD_SCALE};const phase=(time+index*17)%zone.period,warning=zone.warning||4,duration=zone.duration||7,state=phase<warning?'warning':phase<warning+duration?'active':'idle';return {...zone,index,state,phase,x:zone.x*WORLD_SCALE,z:zone.z*WORLD_SCALE,w:zone.w*WORLD_SCALE,d:zone.d*WORLD_SCALE};});for(const event of b.events)if(['fog','visibility'].includes(event.type)){const index=b.hazardZones.length+events.length,phase=time%event.period,state=phase<event.warning?'warning':phase<event.warning+event.duration?'active':'idle';events.push({...event,index,phase,state,kind:event.type==='fog'?'freeze':'blackout',x:0,z:0,w:b.size[0]*WORLD_SCALE,d:b.size[1]*WORLD_SCALE});}return events;}
  function updateEvents(time,wave=1){const events=eventStatus(time,wave);let changed=false;for(const object of selected?.dynamic||[]){const zone=events.find(e=>e.index===selected.blueprint.hazardZones.indexOf(object.eventZone)),on=zone?.state==='active',cycle=Math.floor((time+zone.index*17)/zone.period);if(on&&object.eventPhaseActive&&object.piece?.active===false)object.destroyedCycle=cycle;const desired=on&&object.destroyedCycle!==cycle;if(object.piece&&object.piece.active!==desired){object.piece.active=desired;if(desired)object.piece.hp=object.piece.maxHp;changed=true;}object.eventPhaseActive=on;}if(selected)selected.eventClock=time;if(changed)environmentStateChanged();return {events,changed};}
  function groundModifier(actor,time){if(!selected)return {speed:1,gravity:1};const info=pointInfo(actor.x,actor.z,actor),water=selected.blueprint.floorRegions.find(r=>r.kind==='water'&&inside(actor.x,actor.z,convert(r))&&info.height<.5),hazards=eventStatus(time).filter(e=>inside(actor.x,actor.z,e)&&Math.abs((e.y||0)-info.height)<2);let speed=water?.slow||1,gravity=1;for(const h of hazards){if(h.kind==='low_gravity')gravity=h.gravity||.5;else if(h.state==='active'&&h.slow)speed=Math.min(speed,h.slow);}return {speed,gravity};}
  // Pure setup hook used by node-based regression tests, avoiding browser IO.
  function hydrateForTest(blueprint,tileset,definitions,options={}){dispose();validateBlueprint(blueprint,tileset,definitions);selected={blueprint,tileset,definitions,image:options.image||null,objects:[],collision:[],dynamic:[],tileMap:new Map(tileset.tiles.map(t=>[t.id,t]))};return blueprint;}
  const api={WORLD_SCALE,STANDING_HEIGHT,CLEARANCE_MARGIN,catalog,load,install,installScenery,render,renderSurfaces,surfaceStats,buildSurfaceChunks,SURFACE_STRIDE,dispose,pointInfo,canTraverse,navigation,spawnPoint,pickupPoint,canBuild,rampHeight,rampSupport,followRamp,environmentStateChanged,eventStatus,updateEvents,groundModifier,validateBlueprint,validateGeometry,hydrateForTest,buildChunks,buildAmbientGeometry};
  Object.defineProperties(api,{current:{get:()=>selected?.blueprint||null},revision:{get:()=>revision},loaded:{get:()=>!!selected?.image},canRenderSurfaces:{get:()=>!!selected?.image},hasSurfaceMaterials:{get:()=>!!selected?.image},assetRoot:{get:()=>BASE}});
  root.NeonSurvivalMaps=api;
  if(typeof module!=='undefined'&&module.exports)module.exports=api;
})(typeof window!=='undefined'?window:globalThis);
