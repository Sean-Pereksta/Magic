const {app,BrowserWindow,dialog,ipcMain,net,protocol,session,Menu}=require('electron');
const fs=require('node:fs');
const path=require('node:path');
const {pathToFileURL}=require('node:url');
const {spawn}=require('node:child_process');
const {newProfile,writeJson,readState,inDir}=require('./storage.cjs');
const {SITE,LOBBY,isLobby,cardScript,onLobby,validInvite,snapshot,pause}=require('./official.cjs');
const {portFor,startOffline}=require('./offline.cjs');
const {usesLocalHost,hasLocalHostPack,startLocalHost,catGuestInvite,joinGuestInvite}=require('./local-host.cjs');
const {createArenaReader}=require('./arena-rooms.cjs');
const {supportsSaveSync,configureSaveSession}=require('./save-sync.cjs');
const {gameOnPage}=require('./gameplay.cjs');
const {authorizeDesktop}=require('./verified.cjs');
const {createGames,busy,focusGame}=require('./windows.cjs');
const {getFeed,stageItem,isRejectedUpdate,markRejectedUpdate,prepareSaveDirectory,stagedReleaseSequence,canUseReleaseOverlay}=require('./updates.cjs');
const importLocal=filename=>import(pathToFileURL(filename).href);
protocol.registerSchemesAsPrivileged([{scheme:'catnmice',privileges:{standard:true,secure:true,supportFetchAPI:true}}]);
function start({root,bundled,data,ready,startupFailed}){
  const state=readState(data),save=()=>writeJson(path.join(data,'library.json'),state);
  const prefs=path.join(bundled,'release.json');
  const release=fs.existsSync(prefs)?JSON.parse(fs.readFileSync(prefs,'utf8')):{};
  const publisherUpdates=release.provider==='github'||!!release.key;
  const baseContent=path.join(bundled,'content');
  const original=JSON.parse(fs.readFileSync(path.join(baseContent,'catalog.json'),'utf8'));
  const versionDir=(id,hash)=>path.join(data,'versions',id,hash);
  const bundledDir=id=>path.join(baseContent,'packs',id);
  const bundledSpec=id=>original.games.find(g=>g.id===id)?.offline;
  const installedDir=id=>{const p=state.packs[id];return p?.active?versionDir(id,p.active):path.join(baseContent,'packs',id);};
  const contentDir=()=>canUseReleaseOverlay(data,'catalog',state.packs.catalog?.active,release)?installedDir('catalog'):baseContent;
  let catalog;
  function refreshCatalog(){
    const value=JSON.parse(fs.readFileSync(path.join(contentDir(),'catalog.json'),'utf8'));
    if(value.version!==1||!Array.isArray(value.games)||!value.games.every(g=>/^[a-z0-9_-]+$/i.test(g.id)&&g.actions?.length))throw Error('Invalid Catnmice catalog.');
    catalog=value;
  }
  refreshCatalog();
  const known=id=>{const g=catalog.games.find(g=>g.id===id);if(!g)throw Error('This game is not in the current catalog.');return g;};
  const profile=()=>state.profiles.find(p=>p.id===state.active);
  function packSpec(id){
    if(!state.packs[id]?.active)return bundledSpec(id);
    const info=path.join(installedDir(id),'hub-pack.json');
    return fs.existsSync(info)?JSON.parse(fs.readFileSync(info,'utf8')):state.packs[id].spec;
  }
  function validPack(id,spec,folder){
    if(usesLocalHost(id))return hasLocalHostPack(id,spec,folder);
    if(id==='lastsignal'){
      return spec?.type==='native-windows' && spec.entry==='Last Signal.exe' &&
        ['Last Signal.exe','Hosting/cloudflared.exe','Assets/models.bin','Mobile/index.html']
          .every(file=>fs.statSync(path.join(folder,file),{throwIfNoEntry:false})?.isFile());
    }
    return typeof spec?.entry==='string' && fs.statSync(inDir(folder,spec.entry.split(/[?#]/)[0]),{throwIfNoEntry:false})?.isFile();
  }
  function localPack(id){
    const active=state.packs[id]?.active;
    if(active){
      const root=installedDir(id),spec=packSpec(id);
      if(validPack(id,spec,root))return {root,spec,version:active};
    }
    const root=bundledDir(id),spec=bundledSpec(id);
    if(validPack(id,spec,root))return {root,spec,version:'bundled'};
    return null;
  }
  // Older downloadable packs can still cover ordinary offline titles. For host
  // games, a stale static download must never hide the new bundled edition.
  const installed=g=>usesLocalHost(g.id)||g.id==='lastsignal'
    ? !!localPack(g.id) : !!validPack(g.id,packSpec(g.id),installedDir(g.id));
  const publicProfile=({id,name,guest,avatar,username})=>({id,name,guest,avatar,username});
  let lib,open,model,clientFactory,online=false,updateCheck=null;
  const notify=()=>{if(lib && !lib.isDestroyed())lib.webContents.send('changed');};
  const snapshots=new Map(),openingSessions=new Map(),activities=new Map(),inFlight=new Map(),launching=new Set(),touched=new Set();
  const nativeGames=new Map(),localGames=new Map(),installing=new Map();
  const readPublicArenas=createArenaReader();
  function gameBusy(id){
    return busy(id)||(id?localGames.has(id):localGames.size>0)||Array.from(nativeGames.values()).some(entry=>!id||entry.id===id);
  }
  function profileChanged(p,s){
    p.username=s.username;p.name=s.name||s.username;p.guest=false;p.avatar=s.avatar;
    if(Array.isArray(s.favorites))p.favorites=s.favorites.filter(id=>catalog.games.some(g=>g.id===id));
    const ids=(s.recent||[]).map(x=>typeof x==='string'?x:x.key).filter(Boolean);
    p.recent=[...new Set([...(p.recent||[]),...ids])].slice(0,12);
    save();notify();
  }
  async function officialSession(p){
    const key=p.partition;
    if(openingSessions.has(key))return openingSessions.get(key);
    const opening=loadOfficialSession(p).finally(()=>openingSessions.delete(key));
    openingSessions.set(key,opening);return opening;
  }
  async function loadOfficialSession(p){
    const current=snapshots.get(p.id);
    if(current && !current.isDestroyed() && isLobby(current.webContents.getURL()))return current.webContents;
    const win=new BrowserWindow({show:false,webPreferences:{partition:p.partition,sandbox:true,nodeIntegration:false,contextIsolation:true}});
    snapshots.set(p.id,win);win.webContents.setWindowOpenHandler(()=>({action:'deny'}));
    try{await win.loadURL(LOBBY);await onLobby(win.webContents,'!!document.querySelector("[data-game-key]")');return win.webContents;}
    catch(e){if(!win.isDestroyed())win.destroy();snapshots.delete(p.id);throw e;}
  }
  function activity(p){
    if(activities.has(p.id))return activities.get(p.id);
    state.activity??={};
    const storage={getItem:k=>state.activity[k],setItem:(k,v)=>{state.activity[k]=v;save();},removeItem:k=>{delete state.activity[k];save();}};
    const api=clientFactory({storage,guestProfile:p.id,withLock:fn=>Promise.resolve().then(fn),getAccount:async()=>{
      if(p.guest || !online)return null;
      const wc=await officialSession(p);
      return {username:p.username,token:async()=>{
        if(!isLobby(wc.getURL()))throw Error('Official account session unavailable.');
        return wc.executeJavaScript(`(async()=>{const m=await import('https://www.gstatic.com/firebasejs/10.7.1/firebase-auth.js');const auth=m.getAuth();await auth.authStateReady();if(!auth.currentUser)throw Error('Sign in again');return auth.currentUser.getIdToken()})()`);
      }};
    }});
    activities.set(p.id,api);return api;
  }
  function played(g,p){
    p.recent=[g.id,...p.recent.filter(id=>id!==g.id)].slice(0,12);save();notify();
    activity(p).gameplayStarted(g.id,p.guest?null:p.username).then(()=>online&&activity(p).sync()).catch(()=>{});
  }
  async function networkTick(){
    try{const r=await net.fetch(LOBBY,{method:'HEAD',cache:'no-store',signal:AbortSignal.timeout(5000)});online=r.ok;}catch{online=false;}
    notify();
    if(!online)return;
    const active=profile();
    if(!active.guest){
      officialSession(active).then(snapshot).then(remote=>{if(remote?.username===active.username)profileChangedIfNeeded(active,remote);}).catch(()=>{});
    }
    for(const p of state.profiles)activity(p).sync().catch(()=>{});
    try{const counts=await model.getCounts();if(counts.hub && counts.games && Number.isFinite(Date.parse(counts.asOf))){state.counts=counts;save();notify();}}catch{}
  }
  function getState(){
    const c=state.counts;
    const rollbackAvailable=Object.keys(state.packs).filter(id=>state.packs[id].canRollback&&(id!=='catalog'||contentDir()!==baseContent));
    const saveSyncGames=catalog.games.filter(g=>supportsSaveSync(g.id)&&installed(g)).map(g=>g.id);
    const hubBuild=root===bundled?release.sequence:stagedReleaseSequence(data,'hub',currentHash('hub'));
    return {version:app.getVersion(),hubBuild,restartPending:restartPending(),catalog,profile:publicProfile(profile()),profiles:state.profiles.map(publicProfile),online,installed:catalog.games.filter(installed).map(g=>g.id),recent:profile().recent,favorites:profile().favorites,settings:state.settings,counts:c.hub?{...c,hub:{...c.hub,asOf:c.asOf},games:Object.fromEntries(Object.entries(c.games).map(([id,row])=>[id,{...row,asOf:c.asOf}]))}:{},downloads:state.downloads,rollbackAvailable,saveSyncGames,publisherUpdates};
  }
  function currentHash(id){if(id==='hub')return root===bundled?null:path.basename(root);if(id==='catalog'&&contentDir()===baseContent)return null;return state.packs[id]?.active;}
  function restartPending(){
    try{
      const {active}=JSON.parse(fs.readFileSync(path.join(data,'hub-version.json'),'utf8'));
      return active!==currentHash('hub')&&canUseReleaseOverlay(data,'hub',active,release)&&
        !isRejectedUpdate(data,{id:'hub',sha256:active});
    }catch{return false;}
  }
  function pointer(item){
    if(item.id==='hub'){writeJson(path.join(data,'hub-version.json'),{active:item.sha256,previous:currentHash('hub')});return;}
    state.packs[item.id]={active:item.sha256,previous:currentHash(item.id),canRollback:true};save();
    if(item.id==='catalog'){try{refreshCatalog();}catch(e){rollback('catalog');throw e;}}
  }
  function rollback(id){
    if(gameBusy(id))throw Error('Close this game before restoring its previous version.');
    const p=state.packs[id];if(!p?.canRollback)throw Error('There is no earlier pack to restore.');
    if(p.active)markRejectedUpdate(data,id,p.active);
    state.packs[id]={active:p.previous,previous:null};save();if(id==='catalog')refreshCatalog();notify();
  }
  function row(id,status,message){state.downloads=[{id,status,message},...state.downloads.filter(x=>x.id!==id)].slice(0,40);save();notify();}
  async function apply(item){
    if(inFlight.has(item.id))return inFlight.get(item.id);
    const job=(async()=>{
      row(item.id,'downloading','Downloading '+item.version);
      const staged=await stageItem(item,data);
      const game=catalog.games.find(g=>g.id===item.id);
      if(game){
        const descriptor=JSON.parse(fs.readFileSync(path.join(staged,'hub-pack.json'),'utf8'));
        const type=descriptor.type;
        if(!['static','node','native-windows'].includes(type) || (type==='native-windows' && game.id!=='lastsignal') ||
            !validPack(game.id,descriptor,staged))throw Error('This update does not contain the complete local game.');
      }
      if((item.id==='hub'||item.id==='catalog')?gameBusy():gameBusy(item.id)){
        state.deferred??={};state.deferred[item.id]=item;row(item.id,'queued','Ready when the game closes');return;
      }
      pointer(item);row(item.id,'ready',item.id==='hub'?'Restart the hub to apply':'Installed '+item.version);
    })().catch(e=>{row(item.id,'paused',e.message);throw e;}).finally(()=>inFlight.delete(item.id));
    inFlight.set(item.id,job);return job;
  }
  function afterClose(id,failed){
    if(failed && id && state.packs[id]?.canRollback){
      rollback(id);row(id,'restored','Restored the last working version and its saves');
    }
    for(const item of Object.values(state.deferred||{})){
      if((item.id==='hub'||item.id==='catalog')?gameBusy():gameBusy(item.id))continue;
      try{pointer(item);delete state.deferred[item.id];row(item.id,'ready',item.id==='hub'?'Restart the hub to apply':'Installed '+item.version);}catch(e){row(item.id,'paused',e.message);}
    }
  }
  async function check(id){
    updateCheck??=getFeed(release,state.sequence||0).finally(()=>{updateCheck=null;});
    const feed=await updateCheck;
    state.sequence=feed.sequence;save();
    if(id && !feed.items.some(i=>i.id===id))throw Error('No package is available on the release channel for this game.');
    let changed=0;
    for(const item of feed.items){
      if(id && item.id!==id)continue;
      if(isRejectedUpdate(data,item))continue;
      const g=catalog.games.find(g=>g.id===item.id);
      if(item.id!=='catalog'&&item.id!=='hub' && (!g || (!id && !installed(g))))continue;
      if(currentHash(item.id)===item.sha256 || (!currentHash(item.id) && release.components?.[item.id]===item.sha256 && (!g||installed(g))))continue;
      if(item.runtime && item.runtime!==process.versions.electron)throw Error('This release needs a newer Windows runtime. Run the latest full Setup.');
      await apply(item);
      changed++;
    }
    return {changed,source:feed.source,restartPending:restartPending()};
  }
  function install(id){
    if(!installing.has(id)){
      const game=known(id);
      installing.set(id,installGame(id).catch(error=>{
        row(game.id,'paused',error.message);
        throw error;
      }).finally(()=>installing.delete(id)));
    }
    return installing.get(id);
  }
  async function installGame(id){
    const g=known(id);if(!g.offline&&!usesLocalHost(id)&&id!=='lastsignal')throw Error('This game is online only.');
    if(installed(g))return;
    const bundled=bundledSpec(id);
    const compatibleSetup=usesLocalHost(id)?bundled?.hosting===true:!!bundled;
    if(!compatibleSetup)return check(id);
    const sourceFile=path.join(path.dirname(process.execPath),'setup-source.txt');
    if(!fs.existsSync(sourceFile))return check(id);
    const exe=fs.readFileSync(sourceFile,'utf16le').replace(/^\uFEFF/,'').trim();
    if(!path.isAbsolute(exe)||path.extname(exe).toLowerCase()!=='.exe'||!fs.existsSync(exe))return check(id);
    row(id,'installing','Installing the bundled pack');
    try{
      await new Promise((resolve,reject)=>{const child=spawn(exe,['/S','/PACK='+id],{windowsHide:true});child.once('error',reject);child.once('exit',code=>code===0?resolve():reject(Error('Setup returned '+code)));});
    }catch(error){
      if(!usesLocalHost(id)&&id!=='lastsignal')throw error;
    }
    if(!installed(g) && (usesLocalHost(id)||id==='lastsignal'))await check(id);
    if(!installed(g))throw Error('Setup could not restore this pack. Run the full installer to repair it.');
    row(id,'ready','Installed');
  }
  function saveSession(g,p,version){
    const base=path.join(data,'saves',p.id,g.id);
    const v=version||currentHash(g.id)||'bundled';
    const next=prepareSaveDirectory(base,v,state.packs[g.id]?.previous||'bundled',touched);
    touched.add(next);
    return session.fromPath(next);
  }
  function arenaInvite(value){
    try{
      const url=new URL(value);
      const publicHost=url.protocol==='https:' && !url.port && /^[a-z0-9-]+\.trycloudflare\.com$/i.test(url.hostname);
      const localHost=url.protocol==='http:' && url.hostname==='127.0.0.1' && !!url.port;
      if((publicHost||localHost) && !url.username && !url.password && !url.search &&
          ['/','/mobile','/mobile/','/ws'].includes(url.pathname) && /^#[A-Za-z0-9_-]{24}$/.test(url.hash)){
        return url.origin+'/'+url.hash;
      }
    }catch{}
    return null;
  }
  async function playWeb(game,p,action,mode='online'){
    const chosen=action||game.actions.find(a=>a.type!=='single')?.type||game.actions[0].type;
    if(!game.actions.some(a=>a.type===chosen))throw Error('The website does not support this action.');
    await open({title:game.title,game,mode,player:p,url:LOBBY,session:session.fromPartition(p.partition),action:chosen,initial:cardScript(game.id,chosen),onProfile:s=>{if(p.username===s.username)profileChangedIfNeeded(p,s);}});
  }
  async function play({id,mode,action,invite}){
    const game=known(id),p=profile();
    if(focusGame(id,mode)||launching.has(id))return;
    launching.add(id);
    let server,disconnect;
    const closeLocal=()=>{disconnect?.();server?.close();};
    try{
      if(id==='lastsignal'){
        if(!['offline','online','host','join'].includes(mode))throw Error('Choose Play, Multiplayer or Host for Last Signal.');
        if(!installed(game))await install(id);
        const pack=localPack(id);
        if(!pack)throw Error('Install the Last Signal Windows pack to play.');
        const command=mode==='host'?['--host']:mode==='online' && action!=='single'?['--multiplayer']:[];
        if(mode==='join'){
          const address=arenaInvite(invite);
          if(!address)throw Error('Paste the full Last Signal invitation.');
          command.push('--join',address);
        }
        const key=id+':'+mode,existing=nativeGames.get(key);
        if(existing && existing.child.exitCode===null){
          if(mode==='join' && existing.invite!==invite)throw Error('Close the previous invited arena before joining another.');
          return {nativePid:existing.child.pid};
        }
        const child=spawn(inDir(pack.root,pack.spec.entry),command,{cwd:pack.root,stdio:'ignore',shell:false,detached:true});
        await new Promise((resolve,reject)=>{
          child.once('error',reject);
          child.once('spawn',()=>{
            nativeGames.set(key,{id,child,invite});
            child.once('exit',()=>{
              if(nativeGames.get(key)?.child!==child)return;
              nativeGames.delete(key);
              afterClose(id,false);
            });
            child.unref();
            resolve();
          });
        });
        played(game,p);
        return {nativePid:child.pid};
      }
      if(usesLocalHost(id)){
        if(!['offline','online','host'].includes(mode))throw Error('Choose Play, Multiplayer or Host Lobby.');
        await localGames.get(id)?.closed;
        if(!installed(game))await install(id);
        const pack=localPack(id);
        if(!pack)throw Error('Install this game’s Windows hosting pack from Downloads or run the latest Catnmice Setup.');
        server=startLocalHost({id,mode,root:pack.root,spec:pack.spec,port:portFor(id),
          data:path.join(data,'hosting',p.id,id),onExit:()=>{localGames.delete(id);afterClose(id,false);}});
        localGames.set(id,server);
        const {url}=await server.ready;
        const ses=saveSession(game,p,pack.version);
        const origin=new URL(url).origin;
        ses.setPermissionRequestHandler((_contents,permission,callback,details)=>{
          callback(permission==='fullscreen'||(permission==='clipboard-sanitized-write'&&
            details.isMainFrame&&URL.canParse(details.requestingUrl)&&new URL(details.requestingUrl).origin===origin));
        });
        const title=mode==='host'?game.title+' · Host Lobby':game.title;
        await open({title,game,mode,player:p,url,session:ses,offline:true,localHost:true,
          guestInvite:id==='cat'?catGuestInvite:undefined,close:closeLocal,
          requestHost:id==='trashtactics'&&mode!=='host'?()=>play({id,mode:'host'}):undefined});
        return;
      }
      if(mode==='offline'||mode==='save-sync'){
        if(mode==='save-sync' && (!online||!supportsSaveSync(id)))throw Error('Connect to Catnmice to use the game’s cloud save.');
        if(state.packs[id]?.active && !installed(game) && state.packs[id].canRollback){
          rollback(id);row(id,'restored','Restored the previous game pack and its saves');
        }
        if(!installed(game))await install(id);
        const ses=saveSession(game,p);
        server=await startOffline({...game,offline:packSpec(id)},installedDir(id),undefined,{sync:mode==='save-sync'});
        const origin=new URL(server.url).origin;
        disconnect=configureSaveSession(ses,{gameId:id,origin,sync:mode==='save-sync'});
        await open({title:mode==='save-sync'?game.title+' · Cloud saves':game.title,game,mode,player:p,url:server.url,session:ses,offline:true,close:closeLocal});
      }else if(mode==='host'){
        if(!game.hostRoute)throw Error('This game does not have a Host Lobby action.');
        const url=validInvite(new URL(game.hostRoute,SITE).href);
        await open({title:game.title+' · Host Lobby',game,mode,player:p,url,session:session.fromPartition(p.partition),action:'host'});
      }else{
        await playWeb(game,p,action);
      }
    }catch(e){closeLocal();throw e;}finally{launching.delete(id);}
  }
  function profileChangedIfNeeded(p,s){
    if(p.name!==s.name || p.avatar!==s.avatar || JSON.stringify(p.favorites)!==JSON.stringify(s.favorites) || s.recent?.some(r=>!p.recent.includes(typeof r==='string'?r:r.key)))profileChanged(p,s);
  }
  function saveSignedIn(pending,s){
    let p=state.profiles.find(p=>!p.guest&&p.username===s.username);
    if(!p){p=pending;state.profiles.push(p);}else{
      p.partition=pending.partition;
      const old=snapshots.get(p.id);if(old&&!old.isDestroyed())old.destroy();
      const fresh=snapshots.get(pending.id);snapshots.delete(pending.id);snapshots.delete(p.id);
      if(fresh)snapshots.set(p.id,fresh);
    }
    state.active=p.id;profileChanged(p,s);
  }
  async function account({mode,id}){
    if(mode==='guest'||mode==='switch'){
      const p=mode==='guest'?state.profiles.find(p=>p.guest):state.profiles.find(p=>p.id===id);
      if(!p)throw Error('Unknown local profile.');
      state.active=p.id;save();notify();return;
    }
    if(!['login','register','verified'].includes(mode))throw Error('Unknown account action.');
    const pending=newProfile(),ses=session.fromPartition(pending.partition);
    if(mode==='verified'){
      const username=await authorizeDesktop(root,pending.partition);
      const wc=await officialSession(pending);
      for(let i=0;i<60;i++){const s=await snapshot(wc);if(s?.username===username){saveSignedIn(pending,s);return;}await pause(200);}
      throw Error('Browser approval succeeded, but the official lobby has not loaded the profile. Try again when connected.');
    }
    const tab=mode==='login'?'showLoginTab':'showCreateTab';
    await open({title:mode==='login'?'Log In · Catnmice':'Create a Catnmice Profile',url:LOBBY,mode,session:ses,initial:'(()=>{const b=document.querySelector("#accountButton"),t=document.getElementById("'+tab+'");if(!b||!t||!document.querySelector("[data-game-key]"))return false;b.click();t.click();return true})()',onProfile:s=>{
      saveSignedIn(pending,s);
    }});
  }
  async function favorite(id){
    known(id);const p=profile();if(p.guest)throw Error('Log in to save favorites to Catnmice.');
    const wc=await officialSession(p),before=(await snapshot(wc))?.favorites?.includes(id);
    if(typeof before!=='boolean')throw Error('Sign in to your Catnmice profile again.');
    await onLobby(wc,cardScript(id,'favorite'));
    for(let i=0;i<30;i++){await pause(120);const updated=await snapshot(wc);if(updated && updated.favorites.includes(id)!==before){profileChanged(p,updated);return;}}
    throw Error('The website has not confirmed this favorite yet.');
  }
  async function handle(name,d){
    switch(name){
      case 'state':return getState();
      case 'publicArenas':return readPublicArenas();
      case 'play':return play(d);
      case 'account':return account(d);
      case 'favorite':return favorite(d.id);
      case 'install':return install(d.id);
      case 'update':return check(d.id);
      case 'restart':
        if(!restartPending())return;
        if(gameBusy()||launching.size||inFlight.size||installing.size)throw Error('Close your games and let active downloads finish before restarting.');
        setImmediate(()=>{app.relaunch();app.quit();});return;
      case 'rollback':return rollback(d.id);
      case 'settings':for(const k of ['autoUpdate','reducedMotion'])if(typeof d[k]==='boolean')state.settings[k]=d[k];save();notify();return;
      case 'rankings':{
        const id=d.id==='cat'?'catandmouse':d.id;
        const v=await model.getRankings(id);return Array.isArray(v)?{players:v}:v;
      }
      case 'invite':{
        const arena=arenaInvite(d.url);
        if(arena)return play({id:'lastsignal',mode:'join',invite:arena});
        const guest=joinGuestInvite(d.url);
        const ses=guest?session.fromPartition('persist:catnmice-guest-invites'):session.fromPartition(profile().partition);
        return open({title:'Catnmice invitation',url:guest||validInvite(d.url),player:profile(),session:ses}).then(()=>undefined);
      }
      case 'community':{
        let url=LOBBY,initial;
        if(d.section==='profile')initial='(()=>{const el=document.querySelector(\'[data-view="profile"]\');if(!el)return false;el.click();return true})()';
        else if(d.section==='ranked' && ['bannerlands','cat','catandmouse'].includes(d.id))url=SITE+'/hub/ranked.html?game='+(d.id==='cat'?'catandmouse':d.id);
        else if(['ranked','rankings'].includes(d.section) && d.id==='trashtactics')return playWeb(known(d.id),profile(),'online','web');
        else if(d.section==='lobbies'&&d.id)return playWeb(known(d.id),profile(),undefined,'web');
        else if(d.section==='lobbies')url=LOBBY+'#live-lobbies';
        else if(d.section!=='rankings')throw Error('Unknown community destination.');
        await open({title:'Catnmice · '+d.section,url,initial,player:profile(),session:session.fromPartition(profile().partition)});return;
      }
      case 'linkGuest':{
        const p=profile();if(p.guest)throw Error('Sign in first.');
        const guest=state.profiles.find(g=>g.guest);if(!guest)throw Error('No local guest.');
        const accountActivity=activity(p),guestKey='catnmice.hub.guest.'+guest.id;
        const proof=state.activity[guestKey];if(!proof)return;
        state.activity['catnmice.hub.guest.'+p.id]=proof;save();await accountActivity.linkCurrentGuest();
        if(state.activity[guestKey]===proof)delete state.activity[guestKey];save();return;
      }
      default:throw Error('Unknown launcher command.');
    }
  }
  app.whenReady().then(async()=>{
    [model,{createActivityClient:clientFactory}]=await Promise.all([importLocal(path.join(root,'services/read-model.mjs')),importLocal(path.join(root,'services/web/activity.mjs'))]);
    Menu.setApplicationMenu(null);
    app.on('session-created',s=>{s.setPermissionRequestHandler((_wc,p,done)=>done(p==='fullscreen'));});
    protocol.handle('catnmice',async req=>{
      try{const u=new URL(req.url),p=decodeURIComponent(u.pathname);
        if(u.host!=='hub')return new Response(null,{status:404});
        const file=p.startsWith('/ui/')?inDir(path.join(root,'ui'),p.slice(4)): /^\/content\/(art|screenshots|shots)\//.test(p)?inDir(contentDir(),p.slice(9)):null;
        if(!file || !fs.existsSync(file))return new Response(null,{status:404});
        return net.fetch(pathToFileURL(file).href);
      }catch{return new Response(null,{status:404});}
    });
    const icon=path.join(bundled,'build/icon.ico');
    lib=new BrowserWindow({width:1430,height:900,minWidth:680,minHeight:480,backgroundColor:'#101319',icon,show:false,autoHideMenuBar:true,webPreferences:{preload:path.join(root,'desktop/preload.cjs'),sandbox:true,contextIsolation:true,nodeIntegration:false}});
    lib.webContents.setWindowOpenHandler(()=>({action:'deny'}));lib.webContents.on('will-navigate',(e,u)=>{if(u!=='catnmice://hub/ui/index.html')e.preventDefault();});
    open=createGames({root,icon,library:lib,played,resolveGame:(url,id)=>gameOnPage(catalog,url,id),onClosed:afterClose});
    ipcMain.handle('hub',(e,name,d={})=>{if(e.sender!==lib.webContents||e.senderFrame!==lib.webContents.mainFrame||e.senderFrame.url!=='catnmice://hub/ui/index.html')throw Error('Untrusted launcher window.');return handle(name,d);});
    lib.once('ready-to-show',()=>{lib.show();});lib.on('closed',()=>app.quit());app.on('second-instance',()=>{if(!lib.isDestroyed()){lib.restore();lib.focus();}});
    app.on('before-quit',()=>{for(const game of localGames.values())game.close();});
    await lib.loadURL('catnmice://hub/ui/index.html');
    const usable=await lib.webContents.executeJavaScript(`(async()=>{const deadline=Date.now()+12000;while(Date.now()<deadline){if(document.querySelector('.game-card') && (await window.hub.invoke('state')).catalog.games.length===${catalog.games.length})return true;await new Promise(r=>setTimeout(r,120))}return false})()`);
    if(!usable)throw Error('The launcher interface did not load.');
    ready();
    for(const item of state.downloads.filter(item=>item.status==='installing' && (usesLocalHost(item.id)||item.id==='lastsignal'))){
      const game=catalog.games.find(game=>game.id===item.id);
      if(game && installed(game))row(item.id,'ready','Installed');
      else row(item.id,'paused','Installation was interrupted. Choose Install to retry.');
    }
    afterClose();void networkTick();setInterval(networkTick,60000).unref();
    const automatic=()=>{if(state.settings.autoUpdate && publisherUpdates)check().catch(e=>row('hub','paused',e.message));};
    automatic();setInterval(automatic,4*3600*1000).unref();
  }).catch(e=>{if(startupFailed?.(e))return;dialog.showErrorBox('Catnmice Game Hub',e.message);app.exit(1);});
}
module.exports={start};
