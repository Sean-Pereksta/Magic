const crypto=require('node:crypto');
const path=require('node:path');
const {BrowserWindow,shell}=require('electron');
const {setTimeout:delay}=require('node:timers/promises');
const {SITE}=require('./official.cjs');
const api='https://hub.catnmice.com/v1/desktop/';
async function request(route,body,signal){
  const stop=AbortSignal.timeout(9000);
  const res=await fetch(api+route,{method:'POST',redirect:'error',headers:{'Content-Type':'application/json'},body:JSON.stringify(body),credentials:'omit',signal:signal?AbortSignal.any([signal,stop]):stop});
  if(res.status===410)throw Error('The authorization expired or was declined.');
  if(!res.ok)throw Error('The verified-profile service is unavailable. You can still use the standard Catnmice login.');
  return res.json();
}
async function authorizeDesktop(root,partition){
  let verifier=crypto.randomBytes(32).toString('base64url');
  const challenge=crypto.createHash('sha256').update(verifier).digest('base64url');
  const ticket=await request('start',{challenge}).catch(()=>{throw Error('The verified-profile service is unavailable. The standard Catnmice login is available in Player profiles.');});
  if(!/^[A-Za-z0-9_-]{16,128}$/.test(ticket.id)||!/^[A-Z0-9 -]{4,16}$/i.test(ticket.code))throw Error('Invalid authorization request.');
  const until=Math.min(Date.now()+10*60_000,new Date(ticket.expiresAt).getTime());
  if(!Number.isFinite(until)||until<=Date.now())throw Error('The authorization request has expired.');
  const stop=new AbortController();
  const prompt=new BrowserWindow({width:520,height:360,resizable:false,minimizable:false,autoHideMenuBar:true,backgroundColor:'#15171e',webPreferences:{sandbox:true,contextIsolation:true}});
  prompt.once('closed',()=>stop.abort());
  let complete=false,sessionWindow,result;
  try{
    await prompt.loadFile(path.join(root,'desktop/verify.html'));
    await prompt.webContents.executeJavaScript('document.getElementById("code").textContent='+JSON.stringify(ticket.code));
    const authorize=new URL('/hub/authorize-desktop.html',SITE);
    authorize.hash=new URLSearchParams({request:ticket.id}).toString();
    await shell.openExternal(authorize.href);
    while(!stop.signal.aborted && Date.now()<until){
      await delay(Math.max(3000,Math.min(10000,Number(ticket.pollIntervalMs)||3000)),null,{signal:stop.signal});
      result=await request('redeem',{id:ticket.id,verifier},stop.signal);
      if(result.state==='approved')break;
    }
    if(result?.state!=='approved'||typeof result.customToken!=='string'||!result.customToken||typeof result.username!=='string'||!result.username.trim()||stop.signal.aborted)throw Error('Authorization did not complete.');
    sessionWindow=new BrowserWindow({show:false,webPreferences:{partition,sandbox:true,nodeIntegration:false,contextIsolation:true}});
    const url=SITE+'/hub/desktop-session.html',wc=sessionWindow.webContents;
    wc.setWindowOpenHandler(()=>({action:'deny'}));
    wc.on('will-navigate',(e,target)=>{if(target!==url)e.preventDefault();});wc.on('will-redirect',e=>e.preventDefault());
    await wc.loadURL(url);
    let initialized=false;
    for(let i=0;i<100 && !stop.signal.aborted;i++){
      if(wc.getURL()===url && await wc.executeJavaScript('typeof window.catnmiceCompleteDesktopSignIn==="function"')){initialized=true;break;}
      await delay(100,null,{signal:stop.signal});
    }
    if(!initialized || wc.getURL()!==url)throw Error('Catnmice could not open its account session page.');
    const username=result.username;
    const payload=JSON.stringify({customToken:result.customToken,username});
    let confirmed;
    try{confirmed=await wc.executeJavaScript('window.catnmiceCompleteDesktopSignIn('+payload+')');}
    catch{throw Error('Catnmice could not verify this device session.');}
    if(confirmed?.username!==username || stop.signal.aborted)throw Error('Catnmice did not confirm the selected profile.');
    complete=true;return username;
  }catch(e){if(stop.signal.aborted)throw Error('Authorization canceled.');throw e;}
  finally{
    if(!complete)request('cancel',{id:ticket.id,verifier}).catch(()=>{});
    if(result)delete result.customToken;
    verifier=null;if(sessionWindow&&!sessionWindow.isDestroyed())sessionWindow.destroy();
    if(!prompt.isDestroyed())prompt.destroy();
  }
}
module.exports={authorizeDesktop};
