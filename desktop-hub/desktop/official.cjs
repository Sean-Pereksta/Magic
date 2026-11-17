const SITE = 'https://www.catnmice.com';
const LOBBY = SITE+'/lobby/lobby.html';
function isLobby(raw) {
  try {const u=new URL(raw); return u.origin===SITE && u.pathname==='/lobby/lobby.html';} catch{return false;}
}
const pause=ms=>new Promise(r=>setTimeout(r,ms));
async function onLobby(wc,js,ms=24000) {
  const end=Date.now()+ms;
  while(Date.now()<end && !wc.isDestroyed() && isLobby(wc.getURL())){
    try {const value=await wc.executeJavaScript(js,true); if(value)return value;} catch{}
    await pause(180);
  }
  throw Error('The website could not open that control. Reload or use the Catnmice lobby in this window.');
}
function cardScript(id,kind) {
  if(!/^[a-z0-9_-]+$/i.test(id) || !['single','online','live','lobby','favorite'].includes(kind))throw Error('Unknown game action.');
  const selector='[data-game-key="'+id+'"] '+ (kind==='live'?'[data-live-game]':kind==='favorite'?'.favorite-btn':'[data-game-action="'+kind+'"]');
  return '(()=>{let el=document.querySelector('+JSON.stringify(selector)+');if(el && !el.disabled){el.click();return true}document.querySelector(\'[data-view="singleplayer"]\')?.click();return false})()';
}
const snapshotScript = `(()=>{const text = id=>document.getElementById(id)?.textContent?.trim()||'';
  const read=(key)=>{try{return JSON.parse(localStorage.getItem(key)||'[]')}catch{return []}};
  const username=localStorage.getItem('gh.username')||'';
  if(!username || text('accountButtonText')!==username)return null;
  return {username,name:text('profileName')||username,avatar:document.querySelector('#profileAvatar img')?.src||text('profileAvatar'),favorites:read('gh.favorites'),recent:read('gh.recentGames')};
})()`;
async function snapshot(wc){return isLobby(wc.getURL()) ? wc.executeJavaScript(snapshotScript).catch(()=>null) : null;}
function validInvite(raw){
  const u=new URL(raw);
  if(u.protocol!=='https:' || u.username || u.password || u.port)throw Error('Use an HTTPS Catnmice game invitation.');
  const official=u.hostname==='catnmice.com'||u.hostname.endsWith('.catnmice.com')||(u.hostname==='sean-pereksta.github.io' && u.pathname.startsWith('/Truth/'));
  if(!official)throw Error('Paste an official Catnmice or Truth invite link.');
  return u.href;
}
module.exports={SITE,LOBBY,pause,isLobby,onLobby,cardScript,snapshot,validInvite};
