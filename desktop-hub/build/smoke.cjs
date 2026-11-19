// Run with a fresh isolated data directory. Never joins public matches or enters credentials.
const fs=require('node:fs');
const path=require('node:path');
const {_electron}=require('playwright-core');
const args=process.argv.slice(2),exe=args[0],userDir=path.resolve(args[1]);
if(!exe || !args[1] || fs.existsSync(userDir))throw Error('Usage: node build/smoke.cjs <exe> <new-data-directory> [--dev] [--offline-all]');
fs.mkdirSync(userDir,{recursive:true});
(async()=>{
  const dev=args.includes('--dev');
  const app=await _electron.launch({executablePath:path.resolve(exe),args:[...(dev?[path.join(__dirname,'..')]:[]),'--user-data-dir='+userDir,'--disable-background-networking'],timeout:45000,env:{...process.env,CATNMICE_TEST_DATA:userDir}});
  const report={games:[],errors:[]};
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
          const {execFileSync}=require('node:child_process');
          if(!Number.isInteger(launched?.nativePid)||launched.nativePid<1)throw Error('Native game did not start');
          const probe="$game=Get-Process -Id "+launched.nativePid+" -ErrorAction Stop; try {$limit=(Get-Date).AddSeconds(25); do {$game.Refresh(); if($game.HasExited){throw 'Game exited before menu'}; if($game.MainWindowTitle -like 'Last Signal*Main Menu*'){exit 0}; Start-Sleep -Milliseconds 150} while((Get-Date) -lt $limit); throw 'Game menu did not open'} finally {if(!$game.HasExited){$null=$game.CloseMainWindow(); if(!$game.WaitForExit(3000)){$game.Kill()}}}";
          execFileSync('powershell.exe',['-NoProfile','-NonInteractive','-Command',probe],{windowsHide:true,timeout:35000});
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
        await app.evaluate(({BrowserWindow})=>{for(const w of BrowserWindow.getAllWindows())if(w.webContents.getURL().includes('toolbar.html'))w.close();});
      }
    }
    await lib.screenshot({path:path.join(userDir,'library.png')});
    console.log(JSON.stringify(report,null,2));
    if(report.errors.length)process.exitCode=1;
  }finally{await app.close();}
})().catch(e=>{console.error(e);process.exitCode=1;});
