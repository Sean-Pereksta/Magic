const {app,BrowserWindow,dialog,ipcMain,net,protocol,session,Menu}=require('electron');
const fs=require('node:fs');
const path=require('node:path');
const {pathToFileURL}=require('node:url');
const {spawn}=require('node:child_process');
const {newProfile,writeJson,readState,inDir}=require('./storage.cjs');
const {SITE,LOBBY,isLobby,cardScript,onLobby,validInvite,snapshot,pause}=require('./official.cjs');
const {startOffline}=require('./offline.cjs');
const {gameOnPage}=require('./gameplay.cjs');
const {createGames,busy,focusGame}=require('./windows.cjs');
const {getFeed,stageItem,isRejectedUpdate,markRejectedUpdate,prepareSaveDirectory}=require('./updates.cjs');
const importLocal=filename=>import(pathToFileURL(filename).href);
protocol.registerSchemesAsPrivileged([{scheme:'catnmice',privileges:{standard:true,secure:true,supportFetchAPI:true}}]);
function start({root,bundled,data,ready,startupFailed}){
  const state=readState(data),save=()=>writeJson(path.join(data,'library.json'),state);
  const prefs=path.join(bundled,'release.json');
  const release=fs.existsSync(prefs)?JSON.parse(fs.readFileSync(prefs,'utf8')):{};
  const baseContent=path.join(bundled,'content');
  const original=JSON.parse(fs.readFileSync(path.join(baseContent,'catalog.json'),'utf8'));
  const versionDir=(id,hash)=>path.join(data,'versions',id,hash);
  const installedDir=id=>{const p=state.packs[id];return p?.active?versionDir(id,p.active):path.join(baseContent,'packs',id);};
  const contentDir=()=>state.packs.catalog?.active?installedDir('catalog'):baseContent;
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
    if(!state.packs[id]?.active)return original.games.find(g=>g.id===id)?.offline;
    const info=path.join(installedDir(id),'hub-pack.json');
    return fs.existsSync(info)?JSON.parse(fs.readFileSync(info,'utf8')):state.packs[id].spec;
  }
  const installed=g=>{const spec=packSpec(g.id);return !!spec && fs.existsSync(inDir(installedDir(g.id),spec.entry.split(/[?#]/)[0]));};
  const publicProfile=({id,name,guest,avatar,username})=>({id,name,guest,avatar,username});
  let lib,open,model,clientFactory,online=false,updateCheck=null;
  const notify=()=>{if(lib && !lib.isDestroyed())lib.webContents.send('changed');};
  const snapshots=new Map(),activities=new Map(),inFlight=new Map(),launching=new Set(),touched=new Set();
  function profileChanged(p,s){
    p.username=s.username;p.name=s.name||s.username;p.guest=false;p.avatar=s.avatar;
    if(Array.isArray(s.favorites))p.favorites=s.favorites.filter(id=>catalog.games.some(g=>g.id===id));
    const ids=(s.recent||[]).map(x=>typeof x==='string'?x:x.key).filter(Boolean);
    p.recent=[...new Set([...(p.recent||[]),...ids])].slice(0,12);
    save();notify();
  }
  async function officialSession(p){
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
    for(const p of state.profiles)activity(p).sync().catch(()=>{});
    try{const counts=await model.getCounts();if(counts.hub && counts.games && Number.isFinite(Date.parse(counts.asOf))){state.counts=counts;save();notify();}}catch{}
  }
  function getState(){
    const c=state.counts;
    const rollbackAvailable=Object.keys(state.packs).filter(id=>state.packs[id].canRollback);
    return {version:app.getVersion(),catalog,profile:publicProfile(profile()),profiles:state.profiles.map(publicProfile),online,installed:catalog.games.filter(installed).map(g=>g.id),recent:profile().recent,favorites:profile().favorites,settings:state.settings,counts:c.hub?{...c,hub:{...c.hub,asOf:c.asOf},games:Object.fromEntries(Object.entries(c.games).map(([id,row])=>[id,{...row,asOf:c.asOf}]))}:{},downloads:state.downloads,rollbackAvailable,publisherUpdates:!!release.key};
  }
  function currentHash(id){if(id==='hub')return root===bundled?null:path.basename(root);return state.packs[id]?.active;}
  function pointer(item){
    if(item.id==='hub'){writeJson(path.join(data,'hub-version.json'),{active:item.sha256,previous:currentHash('hub')});return;}
    state.packs[item.id]={active:item.sha256,previous:currentHash(item.id),canRollback:true};save();
    if(item.id==='catalog'){try{refreshCatalog();}catch(e){rollback('catalog');throw e;}}
  }
  function rollback(id){
    if(busy(id))throw Error('Close this game before restoring its previous version.');
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
        if(!['static','node'].includes(descriptor.type)||!fs.existsSync(inDir(staged,descriptor.entry.split(/[?#]/)[0])))throw Error('This update does not contain its game entry point.');
      }
      if((item.id==='hub'||item.id==='catalog')?busy():busy(item.id)){
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
      if((item.id==='hub'||item.id==='catalog')?busy():busy(item.id))continue;
      try{pointer(item);delete state.deferred[item.id];row(item.id,'ready',item.id==='hub'?'Restart the hub to apply':'Installed '+item.version);}catch(e){row(item.id,'paused',e.message);}
    }
  }
  async function check(id){
    updateCheck??=getFeed(release.key,state.sequence||0).finally(()=>{updateCheck=null;});
    const feed=await updateCheck;
    state.sequence=feed.sequence;save();
    if(id && !feed.items.some(i=>i.id===id))throw Error('No package is available on the release channel for this game.');
    for(const item of feed.items){
      if(id && item.id!==id)continue;
      if(isRejectedUpdate(data,item))continue;
      const g=catalog.games.find(g=>g.id===item.id);
      if(item.id!=='catalog'&&item.id!=='hub' && (!g || (!id && !installed(g))))continue;
      if(currentHash(item.id)===item.sha256 || (!currentHash(item.id) && release.components?.[item.id]===item.sha256 && (!g||installed(g))))continue;
      if(item.runtime && item.runtime!==process.versions.electron)throw Error('This release needs a newer Windows runtime. Run the latest full Setup.');
      await apply(item);
    }
  }
  async function install(id){
    const g=known(id);if(!g.offline)throw Error('This game is online only.');
    if(installed(g))return;
    const sourceFile=path.join(path.dirname(process.execPath),'setup-source.txt');
    if(!fs.existsSync(sourceFile))return check(id);
    const exe=fs.readFileSync(sourceFile,'utf16le').replace(/^\uFEFF/,'').trim();
    if(!path.isAbsolute(exe)||path.extname(exe).toLowerCase()!=='.exe'||!fs.existsSync(exe))return check(id);
    row(id,'installing','Installing the bundled pack');
    await new Promise((resolve,reject)=>{const child=spawn(exe,['/S','/PACK='+id],{windowsHide:true});child.once('error',reject);child.once('exit',code=>code===0?resolve():reject(Error('Setup returned '+code)));});
    if(!installed(g))throw Error('Setup could not restore this pack. Run the full installer to repair it.');
    row(id,'ready','Installed');
  }
  function saveSession(g,p){
    const base=path.join(data,'saves',p.id,g.id);
    const v=currentHash(g.id)||'bundled';
    const next=prepareSaveDirectory(base,v,state.packs[g.id]?.previous||'bundled',touched);
    touched.add(next);
    return session.fromPath(next);
  }
  async function play({id,mode,action}){
    const game=known(id),p=profile();
    if(focusGame(id)||launching.has(id))return;
    launching.add(id);
    let server;
    try{
      if(mode==='offline'){
        if(state.packs[id]?.active && !installed(game) && state.packs[id].canRollback){
          rollback(id);row(id,'restored','Restored the previous game pack and its saves');
        }
        if(!installed(game))await install(id);
        const ses=saveSession(game,p);
        server=await startOffline({...game,offline:packSpec(id)},installedDir(id));
        const origin=new URL(server.url).origin;
        ses.webRequest.onBeforeRequest((details,cb)=>{const u=new URL(details.url);cb({cancel:['http:','https:','ws:','wss:'].includes(u.protocol)&&!((u.origin===origin)||(u.protocol==='ws:'&&u.host===new URL(origin).host))});});
        await open({title:game.title,game,player:p,url:server.url,session:ses,offline:true,close:server.close});
      }else{
        const chosen=action||game.actions.find(a=>a.type!=='single')?.type||game.actions[0].type;
        if(!game.actions.some(a=>a.type===chosen))throw Error('The website does not support this action.');
        await open({title:game.title,game,player:p,url:LOBBY,session:session.fromPartition(p.partition),action:chosen,initial:cardScript(id,chosen),onProfile:s=>{if(p.username===s.username)profileChangedIfNeeded(p,s);}});
      }
    }catch(e){server?.close();throw e;}finally{launching.delete(id);}
  }
  function profileChangedIfNeeded(p,s){
    if(p.name!==s.name || p.avatar!==s.avatar || JSON.stringify(p.favorites)!==JSON.stringify(s.favorites))profileChanged(p,s);
  }
  async function account({mode,id}){
    if(mode==='guest'||mode==='switch'){
      const p=mode==='guest'?state.profiles.find(p=>p.guest):state.profiles.find(p=>p.id===id);
      if(!p)throw Error('Unknown local profile.');
      state.active=p.id;save();notify();return;
    }
    if(!['login','register'].includes(mode))throw Error('Unknown account action.');
    const pending=newProfile(),ses=session.fromPartition(pending.partition);
    const tab=mode==='login'?'showLoginTab':'showCreateTab';
    await open({title:mode==='login'?'Log In · Catnmice':'Create a Catnmice Profile',url:LOBBY,mode,session:ses,initial:'(()=>{const b=document.querySelector("#accountButton"),t=document.getElementById("'+tab+'");if(!b||!t||!document.querySelector("[data-game-key]"))return false;b.click();t.click();return true})()',onProfile:s=>{
      let p=state.profiles.find(p=>!p.guest&&p.username===s.username);
      if(!p){p=pending;state.profiles.push(p);}else{p.partition=pending.partition;const old=snapshots.get(p.id);if(old&&!old.isDestroyed())old.destroy();snapshots.delete(p.id);}
      state.active=p.id;profileChanged(p,s);
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
      case 'play':return play(d);
      case 'account':return account(d);
      case 'favorite':return favorite(d.id);
      case 'install':return install(d.id);
      case 'update':return check(d.id);
      case 'rollback':return rollback(d.id);
      case 'settings':for(const k of ['autoUpdate','reducedMotion'])if(typeof d[k]==='boolean')state.settings[k]=d[k];save();notify();return;
      case 'rankings':{
        const id=d.id==='cat'?'catandmouse':d.id;
        const v=await model.getRankings(id);return Array.isArray(v)?{players:v}:v;
      }
      case 'invite':return open({title:'Catnmice invitation',url:validInvite(d.url),player:profile(),session:session.fromPartition(profile().partition)}).then(()=>undefined);
      case 'community':{
        let url=LOBBY,initial;
        if(d.section==='profile')initial='(()=>{const el=document.querySelector(\'[data-view="profile"]\');if(!el)return false;el.click();return true})()';
        else if(d.section==='ranked' && ['bannerlands','cat','catandmouse'].includes(d.id))url=SITE+'/hub/ranked.html?game='+(d.id==='cat'?'catandmouse':d.id);
        else if(d.section==='rankings' && d.id==='trashtactics')return play({id:d.id,mode:'online'});
        else if(d.section==='lobbies'&&d.id)return play({id:d.id,mode:'online'});
        else if(d.section==='lobbies')url=LOBBY+'#live-lobbies';
        else if(d.section!=='rankings')throw Error('Unknown community destination.');
        await open({title:'Catnmice · '+d.section,url,initial,player:profile(),session:session.fromPartition(profile().partition)});return;
      }
      case 'linkGuest':{const p=profile();if(p.guest)throw Error('Sign in first.');const guest=state.profiles.find(g=>g.guest);if(!guest)throw Error('No local guest.');const accountActivity=activity(p);const guestKey='catnmice.hub.guest.'+guest.id;const accountGuestKey='catnmice.hub.guest.'+p.id;if(!state.activity[guestKey])return;state.activity[accountGuestKey]=state.activity[guestKey];save();await accountActivity.linkCurrentGuest();delete state.activity[guestKey];save();return;}
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
    await lib.loadURL('catnmice://hub/ui/index.html');
    const usable=await lib.webContents.executeJavaScript(`(async()=>{const deadline=Date.now()+12000;while(Date.now()<deadline){if(document.querySelector('.game-card') && (await window.hub.invoke('state')).catalog.games.length===${catalog.games.length})return true;await new Promise(r=>setTimeout(r,120))}return false})()`);
    if(!usable)throw Error('The launcher interface did not load.');
    ready();afterClose();void networkTick();setInterval(networkTick,60000).unref();
    const automatic=()=>{if(state.settings.autoUpdate && release.key)check().catch(e=>row('hub','paused',e.message));};
    automatic();setInterval(automatic,4*3600*1000).unref();
  }).catch(e=>{if(startupFailed?.(e))return;dialog.showErrorBox('Catnmice Game Hub',e.message);app.exit(1);});
}
module.exports={start};
