// Run with a fresh isolated data directory. Never joins public matches or enters credentials.
const fs=require('node:fs');
const path=require('node:path');
const {execFileSync}=require('node:child_process');
const {_electron}=require('playwright-core');
const args=process.argv.slice(2),exe=args[0],userDir=path.resolve(args[1]);
if(!exe || !args[1] || fs.existsSync(userDir))throw Error('Usage: node build/smoke.cjs <exe> <new-data-directory> [--dev] [--offline-all]');
fs.mkdirSync(userDir,{recursive:true});
function nativeWindow(pid,title){
  if(!Number.isInteger(pid)||pid<1)throw Error('Native game did not start');
  const probe="$game=Get-Process -Id "+pid+" -ErrorAction Stop; try {$limit=(Get-Date).AddSeconds(25); do {$game.Refresh(); if($game.HasExited){throw 'Game exited before menu'}; if($game.MainWindowTitle -like 'Last Signal*"+title+"*'){exit 0}; Start-Sleep -Milliseconds 150} while((Get-Date) -lt $limit); throw 'Game menu did not open'} finally {if(!$game.HasExited){$null=$game.CloseMainWindow(); if(!$game.WaitForExit(3000)){$game.Kill()}}}";
  execFileSync('powershell.exe',['-NoProfile','-NonInteractive','-Command',probe],{windowsHide:true,timeout:35000});
}
async function localSelector(app,selector){
  await app.evaluate(async({webContents},selector)=>{
    const until=Date.now()+25000;
    while(Date.now()<until){
      const wc=webContents.getAllWebContents().find(w=>w.getURL().startsWith('http://127.0.0.1:'));
      if(wc && !wc.isLoadingMainFrame()){
        const visible=await wc.executeJavaScript('!!document.querySelector('+JSON.stringify(selector)+')?.getClientRects().length').catch(()=>false);
        if(visible)return;
      }
      await new Promise(r=>setTimeout(r,150));
    }
    throw Error('Local game did not open '+selector);
  },selector);
}
const closeGames=app=>app.evaluate(({BrowserWindow})=>{for(const w of BrowserWindow.getAllWindows())if(w.webContents.getURL().includes('toolbar.html'))w.close();});
(async()=>{
  const dev=args.includes('--dev');
  const app=await _electron.launch({executablePath:path.resolve(exe),args:[...(dev?[path.join(__dirname,'..')]:[]),'--user-data-dir='+userDir,'--disable-background-networking'],timeout:45000,env:{...process.env,CATNMICE_TEST_DATA:userDir}});
  const report={games:[],hostControls:[],errors:[]};
  try{
    const lib=await app.firstWindow();await lib.waitForFunction(()=>location.protocol==='catnmice:' && document.querySelector('.nav-link'));
    const state=await lib.evaluate(()=>window.hub.invoke('state'));
    report.catalog=state.catalog.games.length;report.installed=state.installed.length;
    if(!report.catalog || !report.installed)throw Error('Missing catalog or game packs.');
    if(args.includes('--offline-all') && report.installed!==state.catalog.games.filter(g=>g.offline).length)throw Error('Full Setup did not install every declared offline pack.');
    const dirs=state.catalog.games.filter(g=>state.installed.includes(g.id));
    for(const g of (args.includes('--offline-all')?dirs:dirs.slice(0,2))){
      try{
        const launched=await lib.evaluate(id=>window.hub.invoke('play',{id,mode:'offline'}),g.id);
        if(g.offline?.type==='native-windows'){
          nativeWindow(launched?.nativePid,'Main Menu');
          report.games.push(g.id);
          continue;
        }
        await app.evaluate(async({webContents},id)=>{
          const until=Date.now()+25000;
          while(Date.now()<until){
            const wc=webContents.getAllWebContents().find(w=>w.getURL().startsWith('http://127.0.0.1:'));
            if(wc && !wc.isLoadingMainFrame()){const text=await wc.executeJavaScript('document.body?.innerText?.trim().length || document.querySelectorAll("canvas,iframe").length');if(text)return;}
            await new Promise(r=>setTimeout(r,150));
          }
          throw Error('Game did not render: '+id);
        },g.id);
        report.games.push(g.id);
      }catch(e){report.errors.push({game:g.id,reason:e.message});}
      finally{
        await closeGames(app);
      }
    }
    if(args.includes('--offline-all')){
      // Bannerlands and Cat expose host setup ahead of opening the relay.
      // Trash's local lobby loads the same host form and server as its public
      // launcher. Its real tunnel has its own Windows hosting workflow.
      const controls=[
        {id:'bannerlands',mode:'host',selector:'form[data-pane="host"] [name="lobbyName"]'},
        {id:'cat',mode:'host',selector:'#room-name'},
        {id:'trashtactics',mode:'online',selector:'#onlineHost'},
        {id:'lastsignal',mode:'host'},
      ];
      for(const {id,mode,selector} of controls){
        try{
          const launched=await lib.evaluate(data=>window.hub.invoke('play',data),{id,mode});
          if(selector)await localSelector(app,selector);
          else nativeWindow(launched?.nativePid,'OnlineLobby');
          report.hostControls.push(id);
        }catch(e){report.errors.push({game:id,mode,reason:e.message});}
        finally{await closeGames(app);}
      }
    }
    await lib.screenshot({path:path.join(userDir,'library.png')});
    console.log(JSON.stringify(report,null,2));
    if(report.errors.length)process.exitCode=1;
  }finally{await app.close();}
})().catch(e=>{console.error(e);process.exitCode=1;});
