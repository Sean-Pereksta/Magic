const {BrowserWindow,WebContentsView,ipcMain}=require('electron');
const path=require('node:path');
const {pathToFileURL}=require('node:url');
const {isLobby,onLobby,cardScript,snapshot,LOBBY}=require('./official.cjs');
const windows=new Map(),byGame=new Map();
function createGames({library,root,icon,played,resolveGame,onClosed}) {
  ipcMain.on('game-control',(e,a)=>windows.get(e.sender.id)?.control(a));
  ipcMain.on('played-input',(e,declared)=>{
    const row=byGame.get(e.sender.id);
    if(row && row.player && !row.counted && e.senderFrame===e.sender.mainFrame && !isLobby(e.sender.getURL())){
      const game=(row.armed && row.game)||resolveGame(e.sender.getURL(),declared);
      if(game){row.counted=true;played(game,row.player);}
    }
  });
  return async function open({title,url,game,player,offline,session,mode,onProfile,action,initial,close}) {
    const w=new BrowserWindow({title,width:1360,height:850,minWidth:650,minHeight:410,backgroundColor:'#11171d',icon,autoHideMenuBar:true,webPreferences:{preload:path.join(root,'desktop/toolbar-preload.cjs'),sandbox:true}});
    const view=new WebContentsView({webPreferences:{session,preload:path.join(root,'desktop/play-preload.cjs'),contextIsolation:true,nodeIntegration:false,sandbox:true,webviewTag:false}});
    const wc=view.webContents;
    w.contentView.addChildView(view);
    let shown=false,dead=false,accountTimer,attempt=0;
    function layout(){
      if(dead)return;
      const [width,height]=w.getContentSize(),bar=w.isFullScreen()?0:44;
      view.setBounds({x:0,y:bar,width,height:shown?Math.max(0,height-bar):0});
    }
    const status=msg=>{if(!dead)w.webContents.send('game-status',msg);};
    const safe=raw=>{try{const u=new URL(raw);return offline?u.origin===new URL(url).origin:(u.protocol==='https:'&&!u.username&&!u.password);}catch{return false;}};
    const control=a=>{
      if(a==='back'){w.close();return;}
      if(a==='full'){w.setFullScreen(!w.isFullScreen());return;}
      if(a==='reload'){load(wc.getURL() && safe(wc.getURL())?wc.getURL():url);}
    };
    function shortcut(e,k){
      if(k.type!=='keyDown')return;
      if(k.key==='F11'||(k.key==='Escape'&&w.isFullScreen())){e.preventDefault();control('full');}
      if(k.alt && k.key==='ArrowLeft'){e.preventDefault();control('back');}
    }
    const row={w,game,player,control,armed:false,counted:false};windows.set(w.webContents.id,row);byGame.set(wc.id,row);
    function failure(message){shown=false;layout();row.failed=true;status(message+'  Select Reload to try again or Library to return.');}
    async function load(target){
      try{await wc.loadURL(target);}catch(e){if(!dead && e.code!=='ERR_ABORTED')failure('Could not open this page. '+(e.code||'Check the connection.'));}
    }
    wc.setWindowOpenHandler(({url:target})=>{if(safe(target))setImmediate(()=>load(target));return {action:'deny'};});
    wc.on('will-navigate',(e,href)=>{if(!safe(href))e.preventDefault();});
    wc.on('will-redirect',(e,href)=>{if(!safe(href))e.preventDefault();});
    wc.on('did-fail-load',(_,code,desc,href,main)=>{if(main && code!==-3)failure('Page unavailable. '+desc);});
    wc.on('render-process-gone',()=>failure('The game closed unexpectedly.'));
    wc.on('before-input-event',shortcut);w.webContents.on('before-input-event',shortcut);
    wc.on('did-finish-load',async()=>{
      if(dead)return;
      shown=true;row.failed=false;status('');layout();wc.focus();
      if(game&&!isLobby(wc.getURL()))row.armed=offline || (action && safe(wc.getURL()));
      if(isLobby(wc.getURL()) && attempt++===0 && initial){
        try{await onLobby(wc,initial);}catch(err){if(!dead&&isLobby(wc.getURL())){status(err.message);}}
      }
    });
    if(onProfile){
      let syncing=false;
      accountTimer=setInterval(async()=>{
        if(syncing||dead||wc.isDestroyed())return;
        syncing=true;
        try{const profile=await snapshot(wc);if(!dead && profile){await onProfile(profile,wc);if(mode==='login'||mode==='register')w.close();}}
        catch{}finally{syncing=false;}
      },750);
    }
    w.on('resize',layout);w.on('enter-full-screen',layout);w.on('leave-full-screen',layout);
    const barId=w.webContents.id,viewId=wc.id;
    w.on('closed',()=>{dead=true;clearInterval(accountTimer);windows.delete(barId);byGame.delete(viewId);if(!wc.isDestroyed())wc.close();close?.();onClosed?.(game?.id,offline&&row.failed);if(!library.isDestroyed()){library.show();library.focus();}});
    await w.loadFile(path.join(root,'desktop/toolbar.html'),{query:{title}});
    layout();void load(url);
    return row;
  };
}
function busy(id){return [...windows.values()].some(r=>!id||r.game?.id===id);}
function focusGame(id){const row=[...windows.values()].find(r=>r.game?.id===id);if(row){row.w.show();row.w.focus();return true;}return false;}
module.exports={createGames,busy,focusGame};
