/* Asset preparation only. No runtime hook, default URL, renderer or network side effect. */
(function(root){
  'use strict';
  const idPattern=/^[a-z0-9][a-z0-9_-]{0,79}$/i;
  function base(value){
    if(typeof value!=='string')throw new Error('Asset base URL must be a string.');
    const url=value.trim();
    if(!url)return '';
    if(url.length>2048||/[\\\s?#\u0000-\u001f\u007f]/.test(url)||url.startsWith('//'))throw new Error('Invalid asset base URL.');
    if(/^[a-z][a-z0-9+.-]*:/i.test(url)){
      let parsed;try{parsed=new URL(url);}catch{throw new Error('Invalid asset base URL.');}
      if(!['http:','https:'].includes(parsed.protocol)||parsed.username||parsed.password)throw new Error('Asset base URL must use HTTP or HTTPS without credentials.');
      return parsed.href.replace(/\/+$/,'')+'/';
    }
    if(url.includes(':')||/%(?:2f|5c|2e|25)/i.test(url))throw new Error('Invalid relative asset base URL.');
    try{decodeURIComponent(url);}catch{throw new Error('Invalid relative asset base URL.');}
    return url.replace(/\/+$/,'')+'/';
  }
  function path(value){
    // Pack paths intentionally use portable ASCII names. Encodings, dot segments and
    // absolute URLs cannot redirect a manifest entry outside the configured directory.
    if(typeof value!=='string'||value.length>512||!value.split('/').every(p=>/^[a-z0-9_-][a-z0-9._-]*$/i.test(p)))throw new Error('Invalid asset path.');
    return value;
  }
  function freeze(value){
    if(value&&typeof value==='object'){Object.values(value).forEach(freeze);Object.freeze(value);}
    return value;
  }
  function manifest(raw){
    if(!raw||raw.schemaVersion!==1||!Array.isArray(raw.assets)||!raw.assets.length||raw.assets.length>256)throw new Error('Unsupported football asset manifest.');
    const copy=JSON.parse(JSON.stringify(raw)),ids=new Set();
    for(const a of copy.assets){
      if(!a||typeof a.id!=='string'||!idPattern.test(a.id)||ids.has(a.id))throw new Error('Invalid or duplicate football asset ID.');
      ids.add(a.id);
      if(!a.path||!path(a.path).toLowerCase().endsWith('.glb'))throw new Error('A football asset must identify a GLB file.');
      if(a.lods){
        if(typeof a.lods!=='object'||Array.isArray(a.lods))throw new Error('Invalid asset LOD mapping.');
        for(const [lod,file] of Object.entries(a.lods))if(!idPattern.test(lod)||!path(file).toLowerCase().endsWith('.glb'))throw new Error('Invalid asset LOD.');
      }
    }
    return freeze(copy);
  }
  function resources(scene){
    const found=new Set();
    scene.traverse(o=>{
      if(o.geometry)found.add(o.geometry);
      if(o.skeleton?.boneTexture)found.add(o.skeleton.boneTexture);
      for(const m of (Array.isArray(o.material)?o.material:[o.material]))if(m){
        found.add(m);for(const v of Object.values(m))if(v?.isTexture)found.add(v);
      }
    });
    return found;
  }
  function create(options={}){
    const baseURL=base(options.baseURL??''),enabled=!!baseURL;
    const fetcher=options.fetch??root.fetch,loadGLTF=options.loadGLTF,clone=options.clone;
    const maxTemplates=Math.max(1,Math.min(32,Math.floor(Number(options.maxTemplates)||8)));
    const cache=new Map(),owners=new Map();
    let manifestPromise=null,closed=false;
    const ensure=()=>{if(closed)throw new Error('Football asset loader has been disposed.');};
    const resolve=file=>baseURL+path(file);
    function releaseResources(entry){
      if(!entry.retired||entry.refs||!entry.resources)return;
      for(const resource of entry.resources){const count=owners.get(resource)-1;if(count)owners.set(resource,count);else{owners.delete(resource);resource.dispose?.();}}
      entry.resources=null;entry.template=null;
    }
    function retire(entry){entry.retired=true;releaseResources(entry);}
    function trim(){while(cache.size>maxTemplates){const [key,entry]=cache.entries().next().value;cache.delete(key);retire(entry);}}
    async function getManifest(){
      ensure();if(!enabled)return null;
      if(!manifestPromise){
        if(typeof fetcher!=='function')throw new Error('A fetch implementation is required to load the manifest.');
        const pending=Promise.resolve().then(()=>fetcher(resolve('manifest.json'),{credentials:'omit',redirect:'error'})).then(response=>{
          if(!response?.ok)throw new Error('Football asset manifest could not be loaded.');
          return response.json();
        }).then(manifest);
        manifestPromise=pending;
        pending.catch(()=>{if(manifestPromise===pending)manifestPromise=null;});
      }
      return manifestPromise;
    }
    function template(file){
      let entry=cache.get(file);
      if(entry){cache.delete(file);cache.set(file,entry);return entry;}
      entry={refs:0,retired:false,resources:null,template:null};
      entry.promise=Promise.resolve().then(()=>loadGLTF(resolve(file))).then(gltf=>{
        if(!gltf?.scene||typeof gltf.scene.traverse!=='function')throw new Error('Football GLB has no usable scene.');
        entry.template=gltf;entry.resources=resources(gltf.scene);
        for(const resource of entry.resources)owners.set(resource,(owners.get(resource)||0)+1);
        releaseResources(entry);return gltf;
      }).catch(error=>{if(cache.get(file)===entry)cache.delete(file);retire(entry);throw error;});
      cache.set(file,entry);trim();return entry;
    }
    async function load(id,{lod='high'}={}){
      ensure();if(!enabled)return null;
      if(typeof loadGLTF!=='function'||typeof clone!=='function')throw new Error('GLTF loading and a skinned-scene clone implementation are required.');
      if(typeof id!=='string'||!idPattern.test(id)||typeof lod!=='string'||!idPattern.test(lod))throw new Error('Invalid football asset selection.');
      const pack=await getManifest();ensure();
      const asset=pack.assets.find(a=>a.id===id);if(!asset)throw new Error('Unknown football asset: '+id);
      const file=asset.lods?.[lod]??(lod==='high'?asset.path:null);if(!file)throw new Error('Unknown football asset LOD: '+lod);
      const entry=template(file);entry.refs++;
      const materials=new Map();let scene=null,released=false;
      function release(){
        if(released)return;released=true;
        scene?.removeFromParent?.();
        for(const m of materials.values())m.dispose?.();
        // SkeletonUtils.clone owns its skeleton and GPU bone texture independently.
        const boneTextures=new Set();scene?.traverse(o=>{if(o.skeleton?.boneTexture)boneTextures.add(o.skeleton.boneTexture);});
        for(const t of boneTextures)if(!entry.resources?.has(t))t.dispose?.();
        entry.refs--;releaseResources(entry);
      }
      try{
        const gltf=await entry.promise;ensure();const candidate=clone(gltf.scene);
        if(!candidate||candidate===gltf.scene||typeof candidate.traverse!=='function')throw new Error('Skinned-scene clone must return an independent scene.');
        scene=candidate;
        const sourceSkeletons=new Set(),sourceBones=new Set(),sourceNodes=new Set();
        gltf.scene.traverse(o=>{sourceNodes.add(o);if(o.skeleton){sourceSkeletons.add(o.skeleton);for(const bone of o.skeleton.bones||[])sourceBones.add(bone);}});
        scene.traverse(o=>{
          if(sourceNodes.has(o)||o.skeleton&&(sourceSkeletons.has(o.skeleton)||(o.skeleton.bones||[]).some(b=>sourceBones.has(b))))throw new Error('Skinned-scene clone reused a template skeleton or node.');
        });
        const isolatedSkeletons=new Set();
        scene.traverse(o=>{
          // Three r160 Skeleton.clone shares the inverse-bind array. Physique
          // rebinding clears/replaces that array, so it also needs deep ownership.
          if(o.skeleton&&!isolatedSkeletons.has(o.skeleton)){
            isolatedSkeletons.add(o.skeleton);
            if(o.skeleton.boneInverses)o.skeleton.boneInverses=o.skeleton.boneInverses.map(matrix=>matrix.clone());
          }
          const isolate=m=>{if(!m)return m;if(!materials.has(m)){if(typeof m.clone!=='function')throw new Error('Football material cannot be cloned.');const copy=m.clone();if(copy===m)throw new Error('Football material clone is not independent.');materials.set(m,copy);}return materials.get(m);};
          if(o.material)o.material=Array.isArray(o.material)?o.material.map(isolate):isolate(o.material);
        });
        // Physique adaptation rebases translation tracks, so clips must be owned by
        // the instance even though the immutable source data stays cached.
        const animations=(gltf.animations||[]).map(clip=>{
          if(typeof clip.clone!=='function')throw new Error('Football animation clip cannot be cloned.');
          return clip.clone();
        });
        return {scene,animations,asset,lod,release};
      }catch(error){release();throw error;}
    }
    function dispose(){if(closed)return;closed=true;manifestPromise=null;for(const entry of cache.values())retire(entry);cache.clear();}
    return Object.freeze({baseURL,enabled,maxTemplates,getManifest,load,dispose,get cachedTemplates(){return cache.size;}});
  }
  const api={create};
  if(typeof module!=='undefined')module.exports=api;root.QBPlayerAssets=api;
})(typeof globalThis!=='undefined'?globalThis:this);
