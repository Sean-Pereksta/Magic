/* TurnCraft menu + shared Cat'N Mice room browser. DOM only; battle logic stays in turncraft.html. */
(function(root){
 'use strict';
 function makeSeasonUi(host){
  const season=root.TurncraftSeason;
  if(!season)return null;
  const card=document.getElementById('startCard'),overlay=document.getElementById('overlay');
  const e=s=>String(s==null?'':s).replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
  const opts=()=>({base:String(root.TURNCRAFT_MENU_ASSET_BASE||'').replace(/\\/g,'/').replace(/\\/$/,'')});
  function url(path,fallback){
   const base=String(root.TURNCRAFT_MENU_ASSET_BASE||'').trim().replace(/\/$/,'');
   return base&&/^https:\/\//.test(base)?base+'/'+path:fallback;
  }
  const fallback='/game/turncraft/assets/factions/frontier.webp';
  const home=document.createElement('section');home.id='tcSeasonPortal';home.setAttribute('aria-label','TurnCraft game modes');
  const mainImage=url('ui/main-menu.webp',fallback);
  home.innerHTML='<div class="tc-season-cover"><img id="tcSeasonHeroImage" class="tc-season-cover-img" alt="" decoding="async" src="'+e(mainImage)+'"><span class="tc-season-kicker">TURNCRAFT • SEASON OF THE RIFT</span><h2>COMMAND THE <span>RIFT</span></h2><p>One battlefield. Three civilizations. A story that survives beyond the last wave.</p></div>'+
   '<div class="tc-season-toolbar"><div id="tcSeasonIdentity" class="tc-season-account">COMMANDER</div><div><button class="tc-season-action" id="tcSeasonRankings" type="button">Rankings</button> <button class="tc-season-action" id="tcSeasonHub" type="button">Cat’N Mice Hub ↗</button></div></div>'+
   '<div class="tc-season-content" id="tcSeasonHome"><div class="tc-section-heading"><span>Choose your path</span><span>Solo / Story / Endless / Online</span></div><div class="tc-mode-grid" id="tcSeasonModes"></div></div>'+
   '<div class="tc-season-content" id="tcSeasonLobby" hidden><div class="tc-section-heading"><span id="tcLobbyTitle">Multiplayer Command Center</span><button class="tc-season-action" id="tcLobbyBack" type="button">← Modes</button></div>'+
   '<div id="tcLobbyBrowser"><div class="tc-dialog-actions"><input id="tcLobbyName" maxlength="50" aria-label="Room name" placeholder="Name your lobby" style="flex:1;min-width:135px;background:#0d2133;border:1px solid #80b7d26e;border-radius:10px;color:white;padding:10px">'+
   '<select id="tcLobbyPrivacy" aria-label="Room privacy" style="background:#0d2133;border:1px solid #80b7d26e;border-radius:10px;color:white;padding:10px"><option value="public">Public</option><option value="private">Private</option></select>'+
   '<button class="tc-season-action primary" id="tcLobbyCreate" type="button">Create Lobby</button><button class="tc-season-action" id="tcLobbyReload" type="button">Refresh</button></div>'+
   '<div id="tcLobbyStatus" role="status"></div><div id="tcLobbyRows" aria-live="polite"></div></div>'+
   '<div id="tcLobbyRoom"><h3 id="tcRoomName" style="margin:0 0 9px"></h3><div id="tcRoomSeats"></div><div class="tc-dialog-actions"><button class="tc-season-action primary" id="tcRoomReady" type="button">Ready</button><button class="tc-season-action gold" id="tcRoomStart" type="button">Start Match</button><button class="tc-season-action" id="tcRoomInvite" type="button">Copy Invite</button><button class="tc-season-action" id="tcRoomLeave" type="button">Leave Room</button></div></div></div>'+
   '<div class="tc-season-foot"><span>Progression saved with your Cat’N Mice profile when available</span><span>New waves, real armies, and shared online rooms</span></div>';
  card.prepend(home);
  const setupbar=document.createElement('div');setupbar.id='tcSeasonSetupBar';setupbar.className='tc-season-setupbar';
  setupbar.innerHTML='<button type="button" id="tcSeasonSetupBack" class="tc-season-action">← Modes</button><div><h2 id="tcSeasonSetupTitle">Prepare for battle</h2><p id="tcSeasonSetupDesc">Choose a faction and deploy.</p></div><span id="tcSeasonSetupBadge" class="tc-season-chip">SOLO</span>';
  card.prepend(setupbar);
  const panel=document.createElement('section');panel.id='tcSeasonRegionPanel';
  panel.innerHTML='<div class="tc-section-heading"><span>Campaign regions</span><span id="tcRegionCount"></span></div><div class="tc-region-grid" id="tcRegionGrid"></div><div id="tcSeasonChapterStory"></div>';
  card.querySelector('.menuHeader')?.after(panel);
  const ranks=document.createElement('div');ranks.id='tcSeasonRankStrip';ranks.setAttribute('aria-live','polite');ranks.textContent='Loading player rank…';
  card.querySelector('#startStatus')?.before(ranks);
  const hud=document.createElement('div');hud.id='tcWaveHud';hud.hidden=true;hud.setAttribute('role','status');hud.innerHTML='<b id="tcWaveTitle"></b><span id="tcWaveDetail"></span>';document.body.append(hud);
  const $=id=>document.getElementById(id);
  let currentMode='home',currentRegion='classic',progress=season.safeCampaign(),savedInCloud=false;
  let listUnsub=null,roomUnsub=null,roomId='',roomData=null,lobbyCache=[],rankCache=new Map();
  function status(msg){$('tcLobbyStatus').textContent=msg;}
  function setImageFallback(img){img.onerror=()=>{img.onerror=null;img.src=fallback};}
  setImageFallback($('tcSeasonHeroImage'));
  function profileKey(){return 'tc.season.'+String(host.username()||'guest').toLowerCase()}
  function readLocal(){try{return season.safeCampaign(JSON.parse(localStorage.getItem(profileKey())||'{}'))}catch{return season.safeCampaign()}}
  async function load(){
   progress=readLocal();renderIdentity();renderRegions();
   const user=host.username();if(!user||user==='Commander'||user==='Guest')return;
   try{
    const snap=await host.fs.getDoc(host.fs.doc(host.db,'users',user));
    if(snap.exists()){
      const d=snap.data();savedInCloud=!!host.auth.currentUser&&d.chatAuthUid===host.auth.currentUser.uid;
      const remote=season.safeCampaign(d.turncraftSeason);
      progress.completed=[...new Set([...progress.completed,...remote.completed])];
      progress.unlocked=Math.max(progress.unlocked,remote.unlocked);
      progress.endlessBest=Math.max(progress.endlessBest,remote.endlessBest);
      for(const r of season.REGIONS)progress.bestWaves[r.id]=Math.max(progress.bestWaves[r.id],remote.bestWaves[r.id]);
      progress.medals=[...new Set([...progress.medals,...remote.medals])];
      try{localStorage.setItem(profileKey(),JSON.stringify(progress))}catch{}
      renderIdentity();renderRegions();displayRank(user,d.turncraftMmr);
    }
   }catch(err){console.warn('TurnCraft campaign profile is local-only:',err?.message||err)}
  }
  async function save(){
   try{localStorage.setItem(profileKey(),JSON.stringify(progress))}catch{}
   if(!savedInCloud||!host.auth.currentUser)return;
   try{
     const user=host.username(),ref=host.fs.doc(host.db,'users',user),snap=await host.fs.getDoc(ref);
     if(!snap.exists()||snap.data().chatAuthUid!==host.auth.currentUser.uid)return;
     await host.fs.setDoc(ref,{turncraftSeason:progress},{merge:true});
   }catch(err){console.warn('TurnCraft campaign cloud save deferred:',err?.message||err)}
  }
  function displayRank(name,raw,extraTarget){
   const score=raw&&typeof raw==='object'?raw:{},t=season.rank(score.rating,score.games);
   const element=extraTarget||ranks;
   element.innerHTML='<img src="'+e(url('rankings/'+t.id+'.png','/game/turncraft/assets/factions/frontier.webp'))+'" alt="" loading="lazy"><b>'+e(name)+'</b> • '+e(t.name)+' '+(t.games?e(t.rating)+' MMR':'— Unranked');
   element.querySelector('img')?.addEventListener('error',event=>event.currentTarget.style.display='none',{once:true});
  }
  async function playerRank(name){
   if(!name)return {name:'Unranked',rating:1000,games:0};
   if(rankCache.has(name))return rankCache.get(name);
   try{
    const snap=await host.fs.getDoc(host.fs.doc(host.db,'users',name));
    const value=snap.exists()?snap.data().turncraftMmr:{};
    const t=season.rank(value?.rating,value?.games);
    rankCache.set(name,t);return t;
   }catch{return {name:'Unranked',rating:1000,games:0}}
  }
  async function rankedLine(names){
   const all=await Promise.all(names.map(async n=>({name:n,t:await playerRank(n)})));
   if(!document.contains(ranks))return;
   ranks.innerHTML=all.map(x=>'<span style="margin-right:8px">'+e(x.name)+' <b>'+e(x.t.name)+'</b> '+(x.t.games?e(x.t.rating):'—')+'</span>').join('');
  }
  function renderIdentity(){
   $('tcSeasonIdentity').innerHTML='COMMANDER <b>'+e(host.username()||'Guest')+'</b> <span style="color:#a8d9ec">• '+e(progress.completed.length)+'/6 regions • Best Endless '+e(progress.endlessBest)+'</span>';
  }
  function regionUnlocked(index){return index<progress.unlocked}
  function renderRegions(){
   $('tcRegionCount').textContent=progress.completed.length+' / '+season.REGIONS.length+' cleared';
   $('tcRegionGrid').innerHTML='';
   season.REGIONS.forEach((r,i)=>{
    const enabled=regionUnlocked(i),selected=r.id===currentRegion,button=document.createElement('button');
    button.type='button';button.className='tc-region-card';button.disabled=!enabled;button.setAttribute('aria-pressed',String(selected));
    button.innerHTML='<img alt="" loading="lazy" src="'+e(url('regions/'+r.id+'-tile.webp','/game/turncraft/assets/factions/'+(r.enemy==='brood'?'brood':r.enemy==='ascendant'?'ascendant':'frontier')+'.webp'))+'"><span class="tc-region-copy"><b>'+e((i+1)+'. '+r.name)+(enabled?'':' 🔒')+'</b><small>'+e(enabled?r.waves+' story waves • Best '+(progress.bestWaves[r.id]||0):'Clear the previous region to unlock')+'</small></span>';
    button.querySelector('img').onerror=ev=>ev.currentTarget.style.opacity='.5';
    button.onclick=()=>selectRegion(r.id);
    $('tcRegionGrid').append(button);
   });
   const region=season.REGIONS.find(r=>r.id===currentRegion)||season.REGIONS[0];
   $('tcSeasonChapterStory').textContent=(currentMode==='endless'?'Endless assault — ':region.reward+' — ')+region.story+' '+region.description;
  }
  function selectRegion(id){
   const i=season.REGIONS.findIndex(r=>r.id===id);if(i<0||!regionUnlocked(i))return;
   currentRegion=id;host.setMode(currentMode,id);renderRegions();setSetupTitle();
  }
  function setSetupTitle(){
   const titles={solo:['Skirmish Operations','Choose an army, map, opponents and objective.','SOLO'],campaign:['Season of the Rift','Survive each region’s waves to uncover the source of the signal.','CAMPAIGN'],endless:['Endless Defense','Hold as long as possible; every wave becomes more dangerous.','ENDLESS'],online:['Shared Battlefield','Vote on a faction, battlefield and objective with your room.','MULTIPLAYER']};
   const a=titles[currentMode]||titles.solo; $('tcSeasonSetupTitle').textContent=a[0];$('tcSeasonSetupDesc').textContent=a[1];$('tcSeasonSetupBadge').textContent=a[2];
   card.classList.toggle('tc-season-region',currentMode==='campaign'||currentMode==='endless');
   document.getElementById('startBtn').textContent=currentMode==='campaign'?'Begin Chapter • Survive '+(season.REGIONS.find(r=>r.id===currentRegion)?.waves||7)+' Waves':currentMode==='endless'?'Begin Endless Defense':'Deploy Command Base ↗';
  }
  function showHome(){
   currentMode='home';stopLobbyWatch();card.classList.remove('tc-season-lobby','tc-season-setup','tc-season-region');card.classList.add('tc-season-choose');
   $('tcSeasonHome').hidden=false;$('tcSeasonLobby').hidden=true;hud.hidden=true;
   overlay.style.display='flex';renderIdentity();
  }
  function showSetup(mode='solo',id='classic'){
   currentMode=mode;currentRegion=id;stopLobbyWatch();card.classList.remove('tc-season-choose','tc-season-lobby');
   card.classList.add('tc-season-setup');host.setMode(mode,id);renderRegions();setSetupTitle();
   overlay.style.display='flex';void rankedLine(mode==='online'?host.onlineNames():[host.username()]);
  }
  function renderModes(){
   const choices=[
    ['campaign','STORY CAMPAIGN','The Rift Campaign','Six regions. Distinct threats. Seven to twelve waves each.','ui/campaign-intro.webp'],
    ['solo','CUSTOM WARFARE','Solo Skirmish','Classic TurnCraft economy, bases, AI tactics and objectives.','regions/classic-hero.webp'],
    ['endless','UNENDING WAR','Endless Survival','Build, adapt and defend until your last command base falls.','regions/orbital-foundry-hero.webp'],
    ['multiplayer','RANKED / CASUAL','Multiplayer','Find, host and invite players using the shared Cat’N Mice rooms.','ui/multiplayer-lobby.webp']];
   const holder=$('tcSeasonModes');holder.replaceChildren();
   choices.forEach(row=>{
    const button=document.createElement('button');button.type='button';button.className='tc-mode';
    button.innerHTML='<img loading="lazy" alt="" src="'+e(url(row[4],fallback))+'"><span class="tc-mode-brow">'+e(row[1])+'</span><b>'+e(row[2])+'</b><small>'+e(row[3])+'</small>';
    setImageFallback(button.querySelector('img'));
    button.onclick=()=>row[0]==='multiplayer'?openBrowser():showSetup(row[0],row[0]==='endless'?season.REGIONS[progress.unlocked-1].id:'classic');
    holder.append(button);
   });
  }
  async function requireAccount(){
   const name=host.username();
   if(!name||['Guest','Commander'].includes(name)){status('Open Cat’N Mice Hub and sign in to your profile to host or join a room.');return false}
   if(!host.auth.currentUser){status('Firebase is connecting. Retry after authentication.');return false}
   try{
    const snap=await host.fs.getDoc(host.fs.doc(host.db,'users',name));
    if(!snap.exists()){status('Create a Cat’N Mice profile before joining multiplayer.');return false}
    if(snap.data().chatAuthUid&&snap.data().chatAuthUid!==host.auth.currentUser.uid){status('Your profile session has changed. Sign in through the hub again.');return false}
    return true;
   }catch(err){status('Could not verify your Cat’N Mice account: '+(err?.message||'connection unavailable'));return false}
  }
  function stopLobbyWatch(){if(listUnsub){listUnsub();listUnsub=null}if(roomUnsub){roomUnsub();roomUnsub=null}roomData=null;roomId=''}
  function openBrowser(){
   currentMode='multiplayer';card.classList.remove('tc-season-choose','tc-season-setup','tc-season-region');card.classList.add('tc-season-lobby');
   $('tcSeasonHome').hidden=true;$('tcSeasonLobby').hidden=false;overlay.style.display='flex';status('Finding TurnCraft rooms…');
   $('tcLobbyName').value='TurnCraft — '+(host.username()||'Commanders');
   renderLobbyRows();browse();
  }
  function browse(){
   if(listUnsub)listUnsub();
   const f=host.fs;
   const q=f.query(f.collection(host.db,'lobbies'),f.where('gameType','==','turncraft'),f.where('status','==','waiting'));
   listUnsub=f.onSnapshot(q,snapshot=>{lobbyCache=snapshot.docs.map(d=>({id:d.id,...d.data()}));renderLobbyRows();if(!roomId)status(lobbyCache.length+' open TurnCraft lobbies');},
    err=>{status('Live lobby browse is unavailable: '+(err?.message||'retry'));});
  }
  function renderLobbyRows(){
   const visible=lobbyCache.filter(x=>x.status==='waiting'&&(x.visibility!=='private'||x.host===host.username()||x.players?.includes(host.username())));
   $('tcLobbyRows').innerHTML='';
   if(!visible.length){$('tcLobbyRows').textContent='No rooms are waiting. Create the first one or invite a friend.';return}
   visible.sort((a,b)=>(b.createdAt||0)-(a.createdAt||0)).forEach(room=>{
     const names=Array.isArray(room.players)?room.players:[],full=names.length>=3&&!names.includes(host.username());
     const row=document.createElement('div');row.className='tc-lobby-row';
     row.innerHTML='<div><b>'+e(room.name||'TurnCraft Match')+'</b><small>'+e(room.host||'Unknown')+' hosts • '+e(room.visibility||'public')+'</small></div><span>'+e(names.length)+'/3 Commanders</span>';
     const button=document.createElement('button');button.textContent=names.includes(host.username())?'Open':'Join';button.disabled=full;
     button.onclick=()=>void joinRoom(room.id);row.append(button);$('tcLobbyRows').append(row);
   });
  }
  async function createRoom(){
   if(!await requireAccount())return;
   const name=$('tcLobbyName').value.trim();if(!name){status('Enter a room name.');return}
   const f=host.fs;try{
    const ref=await f.addDoc(f.collection(host.db,'lobbies'),{name:name.slice(0,50),gameType:'turncraft',status:'waiting',players:[host.username()],host:host.username(),hostUid:host.auth.currentUser.uid,members:{[host.auth.currentUser.uid]:host.username()},visibility:$('tcLobbyPrivacy').value,createdAt:Date.now(),updatedAt:Date.now(),tcSeasonReady:{}});
    watchRoom(ref.id);status('Your TurnCraft room is ready to invite players.');
   }catch(err){status('Could not create room: '+(err?.message||'unknown error'))}
  }
  async function joinRoom(id){
   if(!await requireAccount())return;
   const f=host.fs,ref=f.doc(host.db,'lobbies',id);
   try{
    await f.runTransaction(host.db,async tx=>{
     const snap=await tx.get(ref);if(!snap.exists())throw Error('Lobby closed.');
     const d=snap.data(),names=Array.isArray(d.players)?d.players:[];
     if(d.status!=='waiting')throw Error('Match has already started.');
     if(d.visibility==='private'&&!names.includes(host.username()))throw Error('Private room: use the invitation in the Game Hub.');
     if(!names.includes(host.username())&&names.length>=3)throw Error('Room is full.');
     if(!names.includes(host.username()))tx.update(ref,{players:[...names,host.username()],members:{...(d.members||{}),[host.auth.currentUser.uid]:host.username()},updatedAt:Date.now()});
    });watchRoom(id);
   }catch(err){status('Could not join room: '+(err?.message||'unknown error'))}
  }
  function watchRoom(id){
   if(roomUnsub)roomUnsub();roomId=id;
   $('tcLobbyBrowser').hidden=true;$('tcLobbyRoom').style.display='block';
   const f=host.fs;
   roomUnsub=f.onSnapshot(f.doc(host.db,'lobbies',id),snap=>{
     if(!snap.exists()){status('Room closed.');leaveRoomUI();return}
     roomData=snap.data();
     if(roomData.status==='started'&&roomData.players?.includes(host.username())){launchRoom(id);return}
     renderRoom();
   },err=>status('Room connection lost: '+(err?.message||'unknown error')));
  }
  function renderRoom(){
   const d=roomData||{},names=Array.isArray(d.players)?d.players:[],ready=d.tcSeasonReady||{};
   $('tcRoomName').textContent=d.name||'TurnCraft Room';
   $('tcRoomSeats').innerHTML=names.map(n=>'<div class="tc-lobby-seat"><span>'+e(n)+(d.host===n?' ★ Host':'')+'</span><em>'+e(ready[n]?'READY':'NOT READY')+' <span class="tc-rank" data-rank-user="'+e(n)+'">· Loading rank</span></em></div>').join('');
   $('tcRoomReady').textContent=ready[host.username()]?'Unready':'Ready';
   $('tcRoomStart').hidden=d.host!==host.username();
   $('tcRoomStart').disabled=names.length<2||!names.every(n=>ready[n]);
   status(names.length<2?'Invite at least one other player.':names.every(n=>ready[n])?'All commanders ready. Host can start.':'Waiting for commanders to ready.');
   for(const el of $('tcRoomSeats').querySelectorAll('[data-rank-user]')){
     const n=el.dataset.rankUser;void playerRank(n).then(t=>{if(el.isConnected)el.textContent='· '+t.name+(t.games?' '+t.rating:'')});
   }
  }
  async function toggleReady(){
   if(!roomId||!roomData)return;
   const f=host.fs,ref=f.doc(host.db,'lobbies',roomId),name=host.username();
   try{await f.runTransaction(host.db,async tx=>{
     const snap=await tx.get(ref);if(!snap.exists()||snap.data().status!=='waiting'||!snap.data().players?.includes(name))throw Error('Room is not waiting.');
     const ready={...(snap.data().tcSeasonReady||{})};ready[name]=!ready[name];tx.update(ref,{tcSeasonReady:ready,updatedAt:Date.now()});
   })}catch(err){status('Ready update failed: '+(err?.message||'retry'))}
  }
  async function startRoom(){
   if(!roomId||!roomData)return;
   const id=roomId,f=host.fs,ref=f.doc(host.db,'lobbies',id);
   try{await f.runTransaction(host.db,async tx=>{
     const snap=await tx.get(ref);if(!snap.exists())throw Error('Room closed.');
     const d=snap.data(),names=d.players||[];
     if(d.host!==host.username()||d.status!=='waiting'||names.length<2||!names.every(n=>d.tcSeasonReady?.[n]))throw Error('Host, two players, and all-ready status required.');
     tx.update(ref,{status:'started',updatedAt:Date.now()});
   });launchRoom(id)}catch(err){status('Start failed: '+(err?.message||'retry'))}
  }
  function launchRoom(id){
   stopLobbyWatch();
   const target=new URL('/game/turncraft.html',location.origin);
   target.searchParams.set('gameId',id);target.searchParams.set('username',host.username());target.searchParams.set('returnTo','/lobby/lobby.html');
   location.assign(target.href);
  }
  function leaveRoomUI(){
   if(roomUnsub){roomUnsub();roomUnsub=null}roomId='';roomData=null;$('tcLobbyBrowser').hidden=false;$('tcLobbyRoom').style.display='none';
  }
  async function leaveRoom(){
   if(!roomId)return;
   const f=host.fs,id=roomId,ref=f.doc(host.db,'lobbies',id),name=host.username();
   leaveRoomUI();
   try{await f.runTransaction(host.db,async tx=>{
     const snap=await tx.get(ref);if(!snap.exists()||snap.data().status!=='waiting')return;
     const d=snap.data(),players=(d.players||[]).filter(n=>n!==name);
     if(!players.length){tx.delete(ref);return}
     const ready={...(d.tcSeasonReady||{})},members={...(d.members||{})};delete ready[name];
     for(const [uid,n] of Object.entries(members))if(n===name)delete members[uid];
     tx.update(ref,{players,tcSeasonReady:ready,members,host:d.host===name?players[0]:d.host,updatedAt:Date.now()});
   })}catch(err){status('Room exit could not sync: '+(err?.message||'retry'))}
  }
  function inviteRoom(){
   if(!roomId)return;
   const link=new URL('/lobby/lobby.html',location.origin);link.searchParams.set('lobby',roomId);
   if(navigator.clipboard?.writeText)navigator.clipboard.writeText(link.href).then(()=>status('Invitation link copied.')).catch(()=>status('Invite: '+link.href));
   else status('Invite: '+link.href);
  }
  function updateWave(wave,statusText,enemyCount,isBoss){
   hud.hidden=false;hud.classList.toggle('tc-wave-boss',!!isBoss);
   $('tcWaveTitle').textContent='WAVE '+wave+(isBoss?' • COMMANDER':'');
   $('tcWaveDetail').textContent=statusText+(enemyCount?' • '+enemyCount+' hostiles remaining':'');
  }
  async function recordOutcome(mode,regionId,wave,victory){
   const r=season.REGIONS.find(region=>region.id===regionId);if(!r)return;
   progress=season.advance(progress,regionId,Math.max(0,wave),victory&&mode==='campaign',mode==='endless');
   renderIdentity();renderRegions();await save();
  }
  function rankModal(){
   const score=rankCache.get(host.username())||season.rank(1000,0);
   alert('TurnCraft ranked: '+score.name+(score.games?' • '+score.rating+' MMR':' • no verified ranked matches yet')+'. Ratings appear on Cat’N Mice profiles; solo waves and practice do not affect MMR.');
  }
  $('tcSeasonRankings').onclick=rankModal;
  $('tcSeasonHub').onclick=()=>location.assign('/lobby/lobby.html');
  $('tcSeasonSetupBack').onclick=showHome;
  $('tcLobbyBack').onclick=()=>{if(roomId){status('Leave your lobby first.');return}showHome()};
  $('tcLobbyCreate').onclick=()=>void createRoom();
  $('tcLobbyReload').onclick=browse;
  $('tcRoomReady').onclick=()=>void toggleReady();
  $('tcRoomStart').onclick=()=>void startRoom();
  $('tcRoomInvite').onclick=inviteRoom;
  $('tcRoomLeave').onclick=()=>void leaveRoom();
  renderModes();renderRegions();
  return {load,showHome,showSetup,openBrowser,updateWave,recordOutcome,rankedLine,displayRank,progress:()=>progress,mode:()=>currentMode,region:()=>currentRegion,stopLobbyWatch};
 }
 root.TurncraftSeasonRuntime=Object.freeze({makeSeasonUi});
})(typeof window!=='undefined'?window:globalThis);
