const service='https://hub.catnmice.com';
const game=new URLSearchParams(location.search).get('game')==='catandmouse'?'catandmouse':'bannerlands';
const el=id=>document.getElementById(id);
let running=false, generation=0, auth, username;
el('title').textContent=game==='bannerlands'?'Bannerlands ranked 1v1':'Ranked Survival Challenge';
el('rules').textContent=game==='bannerlands'?'Command one house against one verified player. Your season starts at 1,000 Elo, with five placement matches.':'Face the same battlefield and difficulty as your opponent. Clear more waves to win. Survival time breaks a tie.';
const status=message=>{el('status').textContent=message;};
async function api(route,data,authenticated=false){
  const headers={};if(data)headers['Content-Type']='application/json';
  if(authenticated){await auth.authStateReady();if(!auth.currentUser||!username)throw new Error('Sign in through the Catnmice library to play ranked.');headers.Authorization='Bearer '+await auth.currentUser.getIdToken();}
  const response=await fetch(service+route,{method:data?'POST':'GET',headers,body:data?JSON.stringify(data):undefined,cache:'no-store',redirect:'error',signal:AbortSignal.timeout(10000)});
  const result=await response.json();if(!response.ok)throw new Error(result.error||'The ranked service is unavailable.');return result;
}
async function leaderboard(){
  try{const board=await api('/v1/leaderboard?game='+game);el('season').textContent=board.season+(board.players.length?'':' · No completed ranked matches yet.');
    for(const player of board.players){const row=el('players').insertRow();for(const value of [player.username,player.rating,player.tier,player.change>0?'+'+player.change:player.change])row.insertCell().textContent=value;}}
  catch{el('season').textContent='Leaderboard unavailable.';}
}
async function queue(turn){
  try{
    const ready=await api('/v1/queue',{game,username},true);
    if(turn!==generation)return;
    if(ready.state==='ready'){location.assign(service+'/hand-off#'+new URLSearchParams({key:ready.handoff}));return;}
    if(ready.state==='complete'){await api('/v1/cancel',{username},true);return queue(turn);}
    status('Waiting for another verified player…');setTimeout(()=>{if(running&&generation===turn)queue(turn);},2500);
  }catch(e){if(turn===generation){running=false;el('join').disabled=false;el('cancel').hidden=true;status(e.message);}}
}
el('join').onclick=()=>{running=true;el('join').disabled=true;el('cancel').hidden=false;status('Joining ranked queue…');queue(++generation);};
el('cancel').onclick=async()=>{generation++;running=false;el('cancel').hidden=true;try{await api('/v1/cancel',{username},true);status('Left the queue.');}catch(e){status(e.message);}el('join').disabled=false;};
await Promise.all([leaderboard(),(async()=>{
  try{const [{getApps,initializeApp},{getAuth},{firebaseConfig}]=await Promise.all([import('https://www.gstatic.com/firebasejs/10.7.1/firebase-app.js'),import('https://www.gstatic.com/firebasejs/10.7.1/firebase-auth.js'),import('/lobby/firebase-config.mjs')]);
    auth=getAuth(getApps().find(a=>a.name==='[DEFAULT]')||initializeApp(firebaseConfig));await auth.authStateReady();
    username=localStorage.getItem('gh.username');
    if(!username||!auth.currentUser)throw new Error('Sign in through the Catnmice library to play ranked.');
    const {matches}=await api('/v1/history',null,true);
    for(const match of matches.filter(m=>m.game===game)){const mine=match.players.find(p=>p.username===username);if(!mine)continue;const item=document.createElement('li');item.textContent=new Date(match.completedAt).toLocaleDateString()+' · '+match.players.map(p=>p.username).join(' vs ')+' · '+mine.rating+' Elo ('+(mine.change>0?'+':'')+mine.change+')';el('history').append(item);}
    el('join').disabled=false;status('Playing as '+username+'.');
  }catch(e){status(e.message==='Failed to fetch'?'The official ranked service is unavailable.':e.message);}
})()]);
