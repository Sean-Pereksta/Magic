/* Basketball 2 graphics pack v2. Classic script; load immediately before bootstrap.js.
 * Opt-in only. No save, AI, physics, camera or input changes.
 * GLTFLoader uses the existing THREE r160 global. */
(() => {
  'use strict';
  const config=globalThis.BASKETBALL2_GRAPHICS;
  if(!config?.enabled || !config.baseURL) return;
  const scriptURL=document.currentScript.src;
  const base=new URL(config.baseURL.endsWith('/')?config.baseURL:config.baseURL+'/',location.href);
  if(base.protocol!=='https:' && !(base.protocol==='http:' && ['localhost','127.0.0.1'].includes(base.hostname)))
    throw new Error('Graphics pack requires HTTPS (or localhost for a preview).');
  const fallbackPath=config.fallbackBase || 'assets/graphics-v2/';
  const fallback=new URL(fallbackPath.endsWith('/')?fallbackPath:fallbackPath+'/',location.href);
  const state={release:'basketball2-v2',errors:[],players:null,environment:null,loading:false,requests:0,activeScene:null,legacyArena:null,legacyHoop:null,environmentReady:Promise.resolve(false)};
  globalThis.Basketball2Graphics=state;
  let manifestPromise,loaderPromise,sceneSequence=0,loadPromise,playerSequence=0,requestedPlayerLOD=null;
  let activeEnvironment=null,environmentJob=null,hoopVisual=null;
  const playerAssetUsers=new Map(),retiredPlayerAssets=new Set();
  const timeoutValue=(value,fallbackValue)=>Number.isFinite(Number(value))&&Number(value)>0?Number(value):fallbackValue;
  const requestTimeout=timeoutValue(config.timeoutMs,8000);
  const environmentTimeout=timeoutValue(config.environmentTimeoutMs,20000);
  const loadingTimeout=timeoutValue(config.loadingTimeoutMs,30000);
  const low=()=>config.playerLOD==='low' || (typeof graphicsQuality!=='undefined' && graphicsQuality==='low') ||
    (typeof gameSettings!=='undefined' && (gameSettings.get('quality')==='low' ||
      (gameSettings.get('quality')==='auto' && typeof adaptiveQuality!=='undefined' && adaptiveQuality.chooseInitialLevel()==='low'))) ||
    (config.playerLOD!=='standard' && (matchMedia('(pointer:coarse)').matches || navigator.connection?.saveData));
  const lod=()=>low()?'low':'high';
  function diagnostic(kind,error){
    const message=`${kind}: ${error.message || String(error)}`;
    state.errors.push(message);
    console.warn('[Basketball 2 graphics]',message);
    window.dispatchEvent(new CustomEvent('basketball2-graphics-status',{detail:{kind,error:message}}));
  }
  async function request(url,type='arrayBuffer',parentSignal){
    const controller=new AbortController(),timer=setTimeout(()=>controller.abort(),requestTimeout);
    const abort=()=>controller.abort();
    if(parentSignal?.aborted)controller.abort();
    else parentSignal?.addEventListener('abort',abort,{once:true});
    state.requests++;
    try{
      const r=await fetch(url,{credentials:'omit',signal:controller.signal});
      if(!r.ok) throw new Error(`HTTP ${r.status} at ${new URL(url,location.href).pathname}`);
      return await r[type]();
    }finally{clearTimeout(timer);parentSignal?.removeEventListener('abort',abort);}
  }
  function manifest(){
    return manifestPromise ||= request(new URL('manifest.json',base),'json').catch(error=>{
      diagnostic('Remote manifest unavailable; using bundled fallback manifest',error);
      return request(new URL('manifest.json',fallback),'json');
    }).then(m=>{
      if(m.release!=='basketball2-v2' || !m.players || !m.environments) throw new Error('Incompatible graphics manifest');
      return m;
    });
  }
  function localPath(value){
    if(typeof value!=='string' || value.startsWith('/') || value.includes('..') || value.includes(':') || value.includes('\\'))
      throw new Error('Invalid relative asset path');
    return value;
  }
  function disposePlayerAsset(asset){
    const geometries=new Set(asset.meshes.flat().map(primitive=>primitive.geometry));
    geometries.forEach(geometry=>geometry.dispose());
    asset.materials.forEach(material=>material.dispose());
  }
  function releaseRetiredPlayers(){
    for(const asset of retiredPlayerAssets){
      if(playerAssetUsers.get(asset))continue;
      disposePlayerAsset(asset);retiredPlayerAssets.delete(asset);playerAssetUsers.delete(asset);
    }
  }
  // Match players and campaign previews share source geometry. Retire it only
  // after every instance has released it, including a late loading result.
  const originalCreatePlayer=createBasketballPlayerInstance;
  createBasketballPlayerInstance=function(asset,...args){
    const instance=originalCreatePlayer.call(this,asset,...args);
    instance.graphicsV2Asset=asset;
    playerAssetUsers.set(asset,(playerAssetUsers.get(asset)||0)+1);
    return instance;
  };
  const originalDisposePlayer=disposePlayerModel;
  disposePlayerModel=function(char){
    const asset=char?.playerModel?.graphicsV2Asset;
    const result=originalDisposePlayer.apply(this,arguments);
    if(asset)playerAssetUsers.set(asset,Math.max(0,(playerAssetUsers.get(asset)||0)-1));
    releaseRetiredPlayers();
    return result;
  };
  function applyPlayerShadowPolicy(){
    if(typeof matchPlayers==='undefined')return;
    matchPlayers.forEach(char=>char.playerModel?.group.traverse(node=>{
      if(node.isMesh && node.userData.basketballNoShadow)node.castShadow=false;
    }));
  }
  async function getPlayer(m,kind,wanted){
    const remote=localPath(m.players[kind][wanted]),backup=localPath(m.players[kind].low);
    let asset;
    try{asset=parseBasketballPlayerGLB(await request(new URL(remote,base)),remote);}
    catch(error){
      diagnostic(`${kind}: remote ${wanted} unavailable; using detailed low LOD`,error);
      asset=parseBasketballPlayerGLB(await request(new URL(backup,fallback)),backup);
    }
    return asset;
  }
  loadPlayerModels=function(){
    const wanted=low()?'low':'standard';
    if(loadPromise && requestedPlayerLOD===wanted) return loadPromise;
    requestedPlayerLOD=wanted;
    const sequence=++playerSequence,kinds=['guard','wing','center'];
    loadPromise=manifest().then(m=>Promise.allSettled(kinds.map(kind=>getPlayer(m,kind,wanted)))).then(results=>{
      if(sequence!==playerSequence){
        results.forEach(result=>{if(result.status==='fulfilled')disposePlayerAsset(result.value);});
        return false;
      }
      const failures=results.filter(r=>r.status==='rejected');
      failures.forEach(r=>diagnostic('Bundled player failed',r.reason));
      results.forEach((result,index)=>{
        if(result.status!=='fulfilled')return;
        const previous=basketballPlayerModels.assets.get(kinds[index]);
        if(previous)retiredPlayerAssets.add(previous);
        basketballPlayerModels.assets.set(kinds[index],result.value);
      });
      state.players=failures.length?'partial':'ready';
      // Never replace an active shooting/dunking rig after the loading deadline.
      // Newly loaded assets remain available for the next match instead.
      if(typeof matchPlayers!=='undefined' && (state.loading || !gameStarted)){
        matchPlayers.forEach(char=>{
          const next=basketballPlayerModels.assets.get(getBasketballPlayerModelKind(char));
          if(char.playerModel && next && char.playerModel.graphicsV2Asset!==next)disposePlayerModel(char);
          attachPlayerModel(char);
        });
      }
      applyPlayerShadowPolicy();releaseRetiredPlayers();
      return !failures.length;
    }).catch(error=>{
      if(sequence===playerSequence){diagnostic('Player loading failed',error);state.players='failed';}
      return false;
    });
    basketballPlayerModels.pending=loadPromise;
    return loadPromise;
  };
  function captureBuild(original,name,keepOutside=()=>false){
    return function(...args){
      const before=new Set(scene.children),result=original.apply(this,args),group=new THREE.Group();
      group.name=name;
      scene.children.filter(n=>!before.has(n)&&!keepOutside(n)).forEach(n=>group.add(n));
      scene.add(group);
      return {result,group};
    };
  }
  const originalArena=captureBuild(buildArena,'legacy_arena_v2_fallback');
  buildArena=function(...args){const x=originalArena.apply(this,args);state.legacyArena=x.group;return x.result;};
  const originalHoop=captureBuild(buildHoop,'legacy_hoop_v2_fallback',node=>typeof scoreboardPlane!=='undefined' && node===scoreboardPlane);
  buildHoop=function(...args){const x=originalHoop.apply(this,args);state.legacyHoop=x.group;return x.result;};
  function prepareHoopVisual(group){
    function pivot(name,names,position){
      const nodes=names.map(key=>group.getObjectByName(key)).filter(Boolean);
      if(!nodes.length)return null;
      const result=new THREE.Group();result.name=name;result.position.copy(position);group.add(result);
      nodes.forEach(node=>{node.position.sub(position);result.add(node);});
      return result;
    }
    return {
      board:pivot('graphics_v2_board_reaction',['hoop_backboard','hoop_frame'],backboardBasePosition),
      rim:pivot('graphics_v2_rim_reaction',['hoop_rim','rim_enamel'],rimBasePosition),
      net:pivot('graphics_v2_net_reaction',['hoop_net'],HOOP_POS),
      inverseRimRest:new THREE.Quaternion().setFromEuler(new THREE.Euler(Math.PI/2,0,0)).invert()
    };
  }
  const originalHoopReaction=updateHoopReaction;
  updateHoopReaction=function(...args){
    const result=originalHoopReaction.apply(this,args),visual=hoopVisual;
    if(!visual)return result;
    if(visual.board)visual.board.position.copy(backboard.position);
    if(visual.rim){
      visual.rim.position.copy(rim.position);
      visual.rim.quaternion.copy(rim.quaternion).multiply(visual.inverseRimRest);
    }
    if(visual.net){
      visual.net.position.copy(rim.position);
      visual.net.quaternion.copy(rim.quaternion).multiply(visual.inverseRimRest);
      const life=netWobbleTimer>0?netWobbleTimer/(.28+Math.max(netWobbleStrength,.2)*.32):0;
      const amplitude=.065*netWobbleStrength*life,time=performance.now()*.02;
      // Pivoting at the rim keeps the top attached without per-vertex updates
      // or a second net simulation; the original contact timers remain authoritative.
      visual.net.rotateX(Math.cos(time*1.15)*amplitude*.6);
      visual.net.rotateZ(Math.sin(time)*amplitude);
      visual.net.scale.y=1+amplitude*.3;
    }
    return result;
  };
  const originalCrowd=typeof updateCrowd==='function'?updateCrowd:null;
  if(originalCrowd) updateCrowd=function(...args){if(!state.activeScene)return originalCrowd.apply(this,args);};
  const originalTheme=applyCourtTheme;
  applyCourtTheme=function(key,...args){
    const result=originalTheme.call(this,key,...args);
    state.environmentReady=replaceEnvironment(key);
    return result;
  };
  const originalQuality=setGraphicsQuality;
  setGraphicsQuality=function(...args){
    const result=originalQuality.apply(this,args);
    applyPlayerShadowPolicy();
    // Static environment LOD may change safely; rigged players retain their
    // current asset for this match so a dunk or shot never swaps skeletons.
    if(state.legacyArena) state.environmentReady=replaceEnvironment(selectedCourt);
    return result;
  };
  // Hold simulation behind an accessible loading curtain until essential assets settle.
  // The original start function still owns roster setup, court selection and countdown.
  let startSequence=0,pauseAfterLoading=false;
  const originalPause=setPaused;
  setPaused=function(paused){
    if(!state.loading)return originalPause.apply(this,arguments);
    pauseAfterLoading=!!paused;
    // Escape, a controller, blur and settings must not release the loading lock.
    originalPause.call(this,true);
  };
  const originalTogglePause=togglePause;
  togglePause=function(){
    if(state.loading){pauseAfterLoading=!pauseAfterLoading;originalPause(true);return;}
    return originalTogglePause.apply(this,arguments);
  };
  const originalSimulation=simulateBasketball;
  simulateBasketball=function(...args){if(!state.loading)return originalSimulation.apply(this,args);};
  function hideCurtain(){
    const curtain=document.getElementById('bb2GraphicsLoading');
    if(curtain){curtain.hidden=true;curtain.style.display='none';}
  }
  function cancelLoading(){
    ++startSequence;state.loading=false;hideCurtain();
  }
  const originalMenuScreen=showMenuScreen;
  showMenuScreen=function(...args){
    if(!gameStarted)cancelLoading();
    return originalMenuScreen.apply(this,args);
  };
  async function latestEnvironment(sequence){
    let pending;
    do{pending=state.environmentReady;await pending;}
    while(sequence===startSequence && pending!==state.environmentReady);
  }
  const originalStart=startGame;
  startGame=function(...args){
    const result=originalStart.apply(this,args);
    if(result===false || !gameStarted)return result;
    const sequence=++startSequence;
    state.loading=true;pauseAfterLoading=false;
    gamePaused=true;
    inputManager.clearAll();
    // startGame applies settings after its court selection. Recheck the final
    // quality now and coalesce with an already loading matching environment.
    state.environmentReady=replaceEnvironment(selectedCourt);
    let curtain=document.getElementById('bb2GraphicsLoading');
    if(!curtain){
      curtain=document.createElement('div');curtain.id='bb2GraphicsLoading';
      curtain.setAttribute('role','status');curtain.setAttribute('aria-live','polite');
      curtain.style.cssText='position:fixed;inset:0;z-index:99999;display:grid;place-items:center;background:#07121df2;color:#f4eddf;font:600 20px Arial;text-align:center';
      document.body.appendChild(curtain);
    }
    curtain.hidden=false;curtain.style.display='grid';
    curtain.textContent='Preparing players and court…';
    let loadingTimer;
    const deadline=new Promise(resolve=>{loadingTimer=setTimeout(()=>{
      if(sequence===startSequence){
        cancelEnvironment();
        diagnostic('Loading deadline reached; continuing with available graphics',new Error('Startup timed out'));
      }
      resolve();
    },loadingTimeout);});
    Promise.race([Promise.allSettled([loadPlayerModels(),latestEnvironment(sequence)]),deadline]).then(()=>{
      clearTimeout(loadingTimer);
      if(sequence!==startSequence)return;
      state.loading=false;hideCurtain();
      if(gameStarted && !gameOver)originalPause(pauseAfterLoading || document.hidden || !!inputManager.suspended);
    });
    return result;
  };
  function environmentName(key){
    if(config.environment){
      if(!['pro-arena','streetball','training-gym','championship'].includes(config.environment))
        throw new Error(`Unknown forced environment ${config.environment}`);
      return config.environment;
    }
    if(/gold|vegas|royal/i.test(key))return 'championship';
    if(/warehouse|legacy|fieldhouse|gym|practice/i.test(key))return 'training-gym';
    if(/street|blacktop|cage|neon|sunset|bay|roof|beach|city|miami|atlanta|desert/i.test(key))return 'streetball';
    return 'pro-arena';
  }
  async function loadSceneAsset(file,job){
    loaderPromise ||= import(new URL('vendor/GLTFLoader.mjs',scriptURL)).then(({GLTFLoader})=>new GLTFLoader());
    const [loader,bytes]=await Promise.all([loaderPromise,request(new URL(localPath(file),base),'arrayBuffer',job.controller.signal)]);
    if(job.cancelled)throw new Error('Environment superseded');
    const gltf=await loader.parseAsync(bytes,base.href);
    // Module imports and image decoders cannot be aborted. A late result must
    // be disposed rather than resurrect an obsolete arena after the deadline.
    if(job.cancelled || job.sequence!==sceneSequence){disposeScene(gltf.scene);throw new Error('Environment superseded');}
    gltf.scene.traverse(o=>{
      if(!o.isMesh)return;
      o.castShadow=false;o.receiveShadow=true;
      for(const material of (Array.isArray(o.material)?o.material:[o.material])){
        if(material.map) material.map.anisotropy=Math.min(4,renderer.capabilities.getMaxAnisotropy());
      }
    });
    return gltf.scene;
  }
  function disposeScene(group){
    if(!group)return;
    const geometry=new Set(),materials=new Set(),textures=new Set();
    group.traverse(o=>{
      if(o.geometry) geometry.add(o.geometry);
      if(o.material) for(const m of (Array.isArray(o.material)?o.material:[o.material]))materials.add(m);
    });
    materials.forEach(m=>{Object.values(m).forEach(v=>{if(v?.isTexture)textures.add(v);});m.dispose();});
    textures.forEach(t=>{t.dispose();if(t.source?.data?.close)t.source.data.close();});
    geometry.forEach(g=>g.dispose());group.removeFromParent();
  }
  function cancelEnvironment(){
    const job=environmentJob;
    if(!job)return;
    job.cancelled=true;job.controller.abort();job.rejectCancellation(new Error('Environment superseded'));
    job.parts.splice(0).forEach(disposeScene);
    environmentJob=null;
  }
  function restoreLegacy(){
    hoopVisual=null;disposeScene(state.activeScene);state.activeScene=null;activeEnvironment=null;state.environment=null;
    if(state.legacyArena)state.legacyArena.visible=true;
    if(state.legacyHoop)state.legacyHoop.visible=true;
  }
  function replaceEnvironment(key){
    let name;
    try{name=environmentName(key);}catch(error){
      cancelEnvironment();++sceneSequence;restoreLegacy();diagnostic('Court unavailable; current built-in court retained',error);return Promise.resolve(false);
    }
    const quality=lod(),identity=`${name}:${quality}`;
    if(environmentJob?.identity===identity)return environmentJob.promise;
    cancelEnvironment();
    const sequence=++sceneSequence;
    if(activeEnvironment===identity)return Promise.resolve(true);
    const job={sequence,identity,controller:new AbortController(),cancelled:false,parts:[],rejectCancellation:null,promise:null};
    environmentJob=job;
    const cancellation=new Promise((resolve,reject)=>{job.rejectCancellation=reject;});
    let timer;
    const deadline=new Promise((resolve,reject)=>{timer=setTimeout(()=>{
      job.cancelled=true;job.controller.abort();reject(new Error('Environment loading deadline exceeded'));
    },environmentTimeout);});
    const work=(async()=>{
      const m=await manifest(),definition=m.environments[name];
      if(!definition)throw new Error(`Unknown environment ${name}`);
      const files=[definition.court[quality],definition.arena[quality],m.hoop[quality]];
      // Serial decoding avoids three concurrent image/GLB decode spikes.
      for(const file of files){
        if(job.cancelled || sequence!==sceneSequence)return false;
        const part=await loadSceneAsset(file,job);
        if(job.cancelled || sequence!==sceneSequence){disposeScene(part);return false;}
        job.parts.push(part);
      }
      if(job.cancelled || sequence!==sceneSequence)return false;
      const group=new THREE.Group();group.name=`graphics_v2_${name}`;
      const nextHoopVisual=prepareHoopVisual(job.parts[2]);
      job.parts.splice(0).forEach(p=>group.add(p));
      scene.add(group);
      const previous=state.activeScene;
      state.activeScene=group;state.environment=name;activeEnvironment=identity;hoopVisual=nextHoopVisual;
      if(state.legacyArena)state.legacyArena.visible=false;
      if(state.legacyHoop)state.legacyHoop.visible=false;
      disposeScene(previous);
      return true;
    })();
    job.promise=Promise.race([work,cancellation,deadline]).catch(error=>{
      job.cancelled=true;job.controller.abort();job.parts.splice(0).forEach(disposeScene);
      if(sequence===sceneSequence && environmentJob===job){
        restoreLegacy();
        diagnostic('Court unavailable; current built-in court retained',error);
      }
      return false;
    }).finally(()=>{
      clearTimeout(timer);
      if(environmentJob===job)environmentJob=null;
    });
    return job.promise;
  }
  state.reloadEnvironment=()=>(state.environmentReady=replaceEnvironment(selectedCourt));
  state.diagnostics=()=>({release:state.release,players:state.players,environment:state.environment,environmentLOD:activeEnvironment?.split(':')[1] || null,loading:state.loading,requests:state.requests,errors:[...state.errors],
    renderer:typeof renderer!=='undefined'?{calls:renderer.info.render.calls,triangles:renderer.info.render.triangles,geometries:renderer.info.memory.geometries,textures:renderer.info.memory.textures}:null});
})();
