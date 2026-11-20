const {BrowserWindow,WebContentsView,dialog,ipcMain,session:electronSession,shell}=require('electron');
const path=require('node:path');
const {randomUUID}=require('node:crypto');
const {pathToFileURL}=require('node:url');
const {isLobby,onLobby,snapshot,SITE,validInvite}=require('./official.cjs');
const {managedInvite,restrictGuestSession}=require('./managed-invite.cjs');
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
  return async function open({title,url,game,player,offline,localHost,guestInvite,requestHost,session,mode,onProfile,action,initial,close,returnTo,returnLabel,guestOrigin}) {
    const localEdition=!localHost && game?.hostRoute && game.localEditionRoute
      ? validInvite(new URL(game.localEditionRoute,SITE).href) : null;
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
    const status=(msg,showLocalEdition=false)=>{if(!dead)w.webContents.send('game-status',{msg,showLocalEdition});};
    const openLocalEdition=()=>shell.openExternal(localEdition).catch(()=>{
      status('Could not open the Local Edition page. Try opening it from the Catnmice website.',true);
    });
    const localOrigin=new URL(url).origin;
    const safe=raw=>{try{const u=new URL(raw);return !u.username&&!u.password&&
      (guestOrigin?u.protocol==='https:'&&u.origin===guestOrigin:offline?u.protocol==='http:'&&u.origin===localOrigin:u.protocol==='https:');}catch{return false;}};
    const fromLocal=()=>offline&&URL.canParse(wc.getURL())&&new URL(wc.getURL()).origin===localOrigin;
    let openingGuest=false;
    const openGuest=target=>{
      if(!fromLocal())return;
      const managed=managedInvite(game?.id,target);
      if(managed){
        if(openingGuest)return;
        openingGuest=true;
        const isolated=electronSession.fromPartition('catnmice-guest-'+randomUUID(),{cache:false});
        const origin=new URL(managed).origin;
        restrictGuestSession(isolated,origin);
        void open({title:game.title+' Multiplayer',url:managed,session:isolated,mode:'join',guestOrigin:origin,returnTo:w,returnLabel:'Back to '+game.title})
          .catch(()=>status('Could not open the selected game server.'))
          .finally(()=>{openingGuest=false;});
        return;
      }
      const invite=guestInvite?.(target);
      if(invite)void shell.openExternal(invite).catch(()=>status('Could not open the guest invitation.'));
    };
    const control=a=>{
      if(a==='back'){w.close();return;}
      if(a==='full'){w.setFullScreen(!w.isFullScreen());return;}
      if(a==='local-edition' && localEdition){void openLocalEdition();return;}
      if(a==='reload'){load(wc.getURL() && safe(wc.getURL())?wc.getURL():url);}
    };
    function shortcut(e,k){
      if(k.type!=='keyDown')return;
      if(k.key==='F11'||(k.key==='Escape'&&w.isFullScreen())){e.preventDefault();control('full');}
      if(k.alt && k.key==='ArrowLeft'){e.preventDefault();control('back');}
    }
    const row={w,game,mode,player,control,armed:false,counted:false};windows.set(w.webContents.id,row);byGame.set(wc.id,row);
    function failure(message){
      shown=false;layout();row.failed=true;
      if(mode==='host' && !localHost){
        status('Could not connect to online hosting. Select Reload to try again, or try the game’s Local Edition.',!!localEdition);
      }else status(message+'  Select Reload to try again or Library to return.');
    }
    async function load(target){
      try{await wc.loadURL(target);}catch(e){if(!dead && e.code!=='ERR_ABORTED')failure('Could not open this page. '+(e.code||'Check the connection.'));}
    }
    wc.setWindowOpenHandler(({url:target})=>{
      if(offline && localEdition && target===localEdition)setImmediate(openLocalEdition);
      else if(safe(target))setImmediate(()=>load(target));
      else openGuest(target);
      return {action:'deny'};
    });
    wc.on('will-navigate',(e,href)=>{
      href=e.url||href;
      if(requestHost && fromLocal() && href===new URL('/__catnmice/start-host',url).href){
        e.preventDefault();
        w.close();
        void requestHost().catch(error=>dialog.showErrorBox('Trash Tactics · Host Lobby',error.message));
      }else if(!safe(href)){e.preventDefault();openGuest(href);}
    });
    wc.on('will-redirect',(e,href)=>{if(!safe(e.url||href))e.preventDefault();});
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
    w.on('closed',()=>{dead=true;clearInterval(accountTimer);windows.delete(barId);byGame.delete(viewId);if(!wc.isDestroyed())wc.close();close?.();onClosed?.(game?.id,offline&&row.failed);const parent=returnTo&&!returnTo.isDestroyed()?returnTo:library;if(!parent.isDestroyed()){parent.show();parent.focus();}});
    await w.loadFile(path.join(root,'desktop/toolbar.html'),{query:returnLabel?{title,returnLabel}:{title}});
    layout();void load(url);
    return row;
  };
}
function busy(id){return [...windows.values()].some(r=>!id||r.game?.id===id);}
function focusGame(id,mode){const row=[...windows.values()].find(r=>r.game?.id===id);if(row){if(row.mode!==mode)throw Error('Close this game window before switching play modes.');row.w.show();row.w.focus();return true;}return false;}
module.exports={createGames,busy,focusGame};
