import {team,parseSchedule,resolveVoice,playable,feedQueue,cycle,RadioPlayer} from './core.mjs';
import {catalog} from './catalog.mjs';
import {PROVIDERS,providerCandidates,safeListeningUrl} from './providers.mjs';
import {discoverTeamStreams,mergeInAppQueues} from './station-discovery.mjs';

const $=id=>document.getElementById(id);
const read=(key,fallback)=>{try{return JSON.parse(localStorage.getItem(`nfl-dial:${key}`))??fallback;}catch{return fallback;}};
const save=(key,value)=>{try{localStorage.setItem(`nfl-dial:${key}`,JSON.stringify(value));}catch{}}
const notice=message=>{$('notice').textContent=message;};
const label=g=>`${g.away} vs ${g.home}`;
const button=(text,fn,cls='')=>{const b=document.createElement('button');b.textContent=text;b.className=cls;b.onclick=fn;return b;};
const paragraph=(text,cls='')=>{const p=document.createElement('p');p.textContent=text;p.className=cls;return p;};
const badge=(text,kind='free')=>{const s=document.createElement('span');s.className=`sourceBadge ${kind}`;s.textContent=text;return s;};

let games=[],feeds=[],current=null,requestedTeam=null,activeFeed=null,phase='idle',scheduleBusy=false,playbackToken=0;
let selected=new Set(Array.isArray(read('rotation',[]))?read('rotation',[]).filter(v=>typeof v==='string'):[]);
let preferences=read('preferences',{});if(!preferences||typeof preferences!=='object'||Array.isArray(preferences))preferences={};
let preferencesPending=null;
const stationCache=new Map();
const stationPending=new Map();

const currentGame=()=>games.find(g=>g.id===current);
const rotation=()=>games.filter(g=>selected.has(g.id)).map(g=>g.id);
const streamsFor=teamId=>stationCache.get(teamId)||[];
const externalButton=$('externalFallback');

function hideExternalFallback(){
 externalButton.hidden=true;
 externalButton.classList.remove('glow');
 externalButton.textContent='External options';
}
function showExternalFallback(){
 externalButton.hidden=false;
 externalButton.classList.add('glow');
 externalButton.textContent='NO IN-APP STREAM · EXTERNAL OPTIONS';
 notice('Every available in-app stream was tried. External provider options are separate and will never open unless you choose them.');
}

async function warmTeam(teamId){
 if(!teamId)return [];
 if(stationCache.has(teamId))return stationCache.get(teamId);
 if(stationPending.has(teamId))return stationPending.get(teamId);
 const work=discoverTeamStreams(teamId).then(list=>{
  stationCache.set(teamId,list);
  stationPending.delete(teamId);
  renderGames();
  return list;
 }).catch(()=>{
  stationCache.set(teamId,[]);
  stationPending.delete(teamId);
  renderGames();
  return [];
 });
 stationPending.set(teamId,work);
 return work;
}
function warmGame(g){if(!g)return;void warmTeam(g.away);void warmTeam(g.home);}
function warmRotation(){for(const id of rotation())warmGame(games.find(g=>g.id===id));}

function publicQueue(g,target,role){
 if(!target||role==='national'||role==='spanish')return [];
 return streamsFor(target).map(feed=>({...feed,team:target}));
}
function inAppQueue(g,target,role,preferred){
 return mergeInAppQueues(feedQueue(feeds,g,target,preferred,role),publicQueue(g,target,role));
}

const player=new RadioPlayer(()=>new Audio(),(state,feed)=>{
 phase=state;activeFeed=feed;
 $('pause').textContent=state==='playing'||state==='loading'?'⏸':'▶';
 $('liveDot').classList.toggle('playing',state==='playing');
 if(feed)$('nowStation').textContent=`${feed.name} · ${feed.gameAudio?'verified game audio':'free station stream'}`;
 const messages={
  loading:'Connecting inside Catnmice…',
  playing:feed?.gameAudio?'Listening to verified game audio in app':'Listening to the station in app · game carriage is controlled by the broadcaster',
  paused:'Paused',
  fallback:'That stream did not play. Trying the next in-app option…',
  unavailable:'No in-app stream played.',
  blocked:'Your browser blocked autoplay. Tap ▶ once to resume the in-app stream.'
 };
 notice(messages[state]||state);
 if(state==='loading'||state==='playing'||state==='fallback')hideExternalFallback();
 if(state==='unavailable')showExternalFallback();
 if(state==='playing'&&preferencesPending===feed?.id){preferences[requestedTeam]=feed.id;save('preferences',preferences);preferencesPending=null;}
 if('mediaSession' in navigator){
  navigator.mediaSession.playbackState=state==='playing'?'playing':'paused';
  if(feed&&typeof MediaMetadata!=='undefined')navigator.mediaSession.metadata=new MediaMetadata({title:currentGame()?label(currentGame()):'NFL Radio Dial',artist:feed.name,album:'Catnmice'});
 }
});

async function routePlayback(g,role,manual=false){
 const token=++playbackToken;
 hideExternalFallback();
 const target=role==='home'?g.home:role==='away'?g.away:requestedTeam||g.away;
 requestedTeam=target;
 let queue=inAppQueue(g,target,role,preferences[target]);
 if(queue.length){
  if(manual)preferencesPending=queue[0].id;
  player.select(queue);
  return;
 }
 if(target&&role!=='national'&&role!=='spanish'&&!stationCache.has(target)){
  phase='searching';activeFeed=null;
  $('nowStation').textContent=`Searching free in-app ${team(target)?.nickname||'team'} stations…`;
  notice('Searching every mapped free/no-login station option before showing anything external…');
  const discovered=await warmTeam(target);
  if(token!==playbackToken||current!==g.id)return;
  queue=mergeInAppQueues(feedQueue(feeds,g,target,preferences[target],role),discovered);
  if(queue.length){if(manual)preferencesPending=queue[0].id;player.select(queue);return;}
 }
 player.select([]);
}

function selectGame(id,teamId,fromVoice=false){
 if(!fromVoice)cancelSpeech();
 const g=games.find(item=>item.id===id);if(!g)return;
 current=id;requestedTeam=teamId||g.away;preferencesPending=null;
 warmGame(g);
 $('nowGame').textContent=`${label(g)} · ${g.state==='in'?'LIVE':g.state==='post'?'FINAL':new Date(g.date).toLocaleString([],{weekday:'short',hour:'numeric',minute:'2-digit'})}`;
 void routePlayback(g);
 renderRotation();
}
function selectRole(role){
 cancelSpeech();const g=currentGame();if(!g){notice('Choose a game first.');return;}
 if(role==='home'||role==='away')requestedTeam=g[role];
 void routePlayback(g,role,true);
}
function selectInAppStation(feed){
 cancelSpeech();const g=currentGame();if(!g)return;
 const target=feed.team||requestedTeam;
 requestedTeam=target;preferencesPending=feed.id;hideExternalFallback();
 let queue;
 if(feed.gameAudio&&playable(feed,g)){
  const rest=inAppQueue(g,target,feed.language==='es'?'spanish':undefined,feed.id);
  queue=[feed,...rest.filter(item=>item.id!==feed.id)];
 }else{
  const rest=mergeInAppQueues(streamsFor(target),feedQueue(feeds,g,target,preferences[target]));
  queue=[feed,...rest.filter(item=>item.id!==feed.id)];
 }
 player.select(queue);
 if($('stationDialog').open)$('stationDialog').close();
}
function nextGame(direction){const id=cycle(rotation(),current,direction);if(id)selectGame(id);else notice('Add games to your rotation first.');}
function toggle(){
 cancelSpeech();
 if(phase==='playing'||phase==='loading')player.pause();
 else if(player.audio)player.resume();
 else if(current)void routePlayback(currentGame());
 else nextGame(1);
}
function toggleResume(){if(player.audio)player.resume();else if(current)void routePlayback(currentGame());else nextGame(1);}

function renderGames(){
 $('games').replaceChildren();
 for(const g of games){
  const card=document.createElement('article');card.className=`card ${selected.has(g.id)?'selected':''}`;
  card.append(paragraph(g.state==='in'?`● LIVE · ${g.status}`:g.state==='post'?`FINAL · ${g.status}`:new Date(g.date).toLocaleString([],{weekday:'short',month:'short',day:'numeric',hour:'numeric',minute:'2-digit'}),'status'));
  const h=document.createElement('h3');h.textContent=label(g);card.append(h,paragraph(`${team(g.away).name} at ${team(g.home).name}`,'teams'));
  const verified=feeds.filter(f=>playable(f,g)).length;
  const discovered=streamsFor(g.away).length+streamsFor(g.home).length;
  const searching=stationPending.has(g.away)||stationPending.has(g.home);
  const text=verified?`${verified} verified game feed${verified===1?'':'s'} + ${discovered} free in-app station stream${discovered===1?'':'s'}`:discovered?`${discovered} free/no-login in-app station stream${discovered===1?'':'s'} found${searching?' · still searching':''}`:searching?'Searching free in-app streams…':'Add to rotation to search free in-app streams';
  card.append(paragraph(text,'feeds'));
  card.append(button(selected.has(g.id)?'✓ In rotation':'+ Add to rotation',()=>{
   if(selected.has(g.id))selected.delete(g.id);else{selected.add(g.id);warmGame(g);}
   save('rotation',[...selected]);renderGames();renderRotation();
  }));
  $('games').append(card);
 }
 if(!games.length)$('games').append(paragraph('No games are scheduled in the current NFL week. Refresh to check again.'));
 $('count').textContent=`${rotation().length} selected`;$('start').disabled=!rotation().length;
}
function renderRotation(){
 $('rotation').replaceChildren();
 for(const id of rotation()){
  const g=games.find(item=>item.id===id);if(!g)continue;
  const b=button('',()=>selectGame(id),'dialButton'+(current===id?' active':''));
  const strong=document.createElement('strong');strong.textContent=label(g);
  const small=document.createElement('small');small.textContent=g.state==='in'?'● LIVE · IN-APP STREAMS FIRST':g.status;
  b.append(strong,small);$('rotation').append(b);
 }
 for(const id of ['prev','next'])$(id).disabled=!rotation().length;
}

const sourceBadges=(labelText='IN APP')=>{const box=document.createElement('div');box.className='sourceBadges';box.append(badge('FREE','free'),badge(labelText,'providerTag'));return box;};
function inAppRow(feed){
 const row=document.createElement('div');row.className='station sourceOption';row.append(sourceBadges(feed.gameAudio?'VERIFIED GAME':'DIRECT STREAM'));
 row.append(button(`${activeFeed?.id===feed.id?'●':'▶'} ${feed.name}`,()=>selectInAppStation(feed)),paragraph(feed.gameAudio?'Verified authorized game audio.':'Public station stream plays directly in Catnmice. The station may substitute non-game programming online.'));
 return row;
}
function externalCandidatesFor(g,target,role){
 const raw=providerCandidates(catalog,g,target,role,preferences[target]);
 const options=[];
 for(const option of raw){
  const converted=option.type==='widget'?{...option,type:'external',inApp:false,url:option.widget?.url,label:`${option.widget?.name||option.label} · ${PROVIDERS[option.provider]?.name||option.provider}`,detail:'EXTERNAL ONLY · This provider player does not meet Catnmice single-tap in-app playback requirements.'}:option;
  if(converted.url&&safeListeningUrl(converted.url))options.push(converted);
 }
 const seen=new Set();
 return options.filter(option=>{const key=option.url;if(seen.has(key))return false;seen.add(key);return true;});
}
function externalRow(option){
 const row=document.createElement('div');row.className='station sourceOption externalSource';
 const box=document.createElement('div');box.className='sourceBadges';box.append(badge(option.auth==='none'?'FREE':'LOGIN',option.auth==='none'?'free':'login'),badge('EXTERNAL','externalTag'));row.append(box);
 const a=document.createElement('a');a.className='provider';a.href=option.url;a.target='_blank';a.rel='noopener noreferrer';a.textContent=`Open ${option.label} ↗`;
 row.append(a,paragraph(option.detail||'External provider option.'));return row;
}
function stations(includeExternal=false){
 const g=currentGame();if(!g){notice('Tap a game in your rotation first.');return;}
 const target=requestedTeam||g.away;
 $('stationHint').textContent=includeExternal?`${label(g)} · Every in-app option was exhausted. External services are shown separately below.`:`${label(g)} · Only streams that can play inside Catnmice are shown. External providers stay hidden until all in-app choices fail.`;
 const list=$('stationList');list.replaceChildren();
 const roles=document.createElement('div');roles.className='roles';for(const role of ['home','away','national','spanish'])roles.append(button(role.toUpperCase(),()=>selectRole(role)));list.append(roles);
 const h=document.createElement('h3');h.textContent='PLAY IN APP';list.append(h);
 const direct=feedQueue(feeds,g,target,preferences[target]);
 const discovered=streamsFor(target);
 const inApp=mergeInAppQueues(direct,discovered);
 if(inApp.length)for(const feed of inApp)list.append(inAppRow(feed));
 else list.append(paragraph(stationPending.has(target)?'Still searching free direct streams…':'No playable in-app stream was found for this selection.'));
 if(includeExternal){
  const eh=document.createElement('h3');eh.textContent='EXTERNAL ONLY · OPTIONAL';list.append(eh);
  const options=externalCandidatesFor(g,target);
  if(options.length)for(const option of options)list.append(externalRow(option));
  else list.append(paragraph('No verified external provider destination is mapped for this broadcast.'));
 }
 if(!$('stationDialog').open)$('stationDialog').showModal();
}

async function json(url){const controller=new AbortController();const timer=setTimeout(()=>controller.abort(),12000);try{const r=await fetch(url,{cache:'no-store',signal:controller.signal});if(!r.ok)throw new Error(`HTTP ${r.status}`);return await r.json();}finally{clearTimeout(timer);}}
async function refresh(){
 if(scheduleBusy)return;scheduleBusy=true;$('refresh').disabled=true;
 try{
  const results=await Promise.allSettled([json('https://site.api.espn.com/apis/site/v2/sports/football/nfl/scoreboard'),json('./nfl-radio/feeds.json')]);
  if(results[1].status==='fulfilled'&&Array.isArray(results[1].value.feeds))feeds=results[1].value.feeds.filter(f=>f&&typeof f.id==='string'&&typeof f.name==='string');
  if(results[0].status==='rejected')throw results[0].reason;
  games=parseSchedule(results[0].value);save('schedule',{at:Date.now(),games});
  $('scheduleStatus').textContent=`${results[0].value.week?.number?'Week '+results[0].value.week.number+' · ':''}Updated ${new Date().toLocaleTimeString([],{hour:'numeric',minute:'2-digit'})}`;
  selected=new Set([...selected].filter(id=>games.some(g=>g.id===id)));save('rotation',[...selected]);warmRotation();
 }catch{
  if(!games.length){const cached=read('schedule',null);if(cached&&Date.now()-cached.at<86400000&&Array.isArray(cached.games))games=cached.games.filter(g=>g&&team(g.home)&&team(g.away)&&Number.isFinite(Date.parse(g.date)));}
  $('scheduleStatus').textContent=games.length?'Offline · showing cached schedule, live status may be outdated':'Schedule unavailable. Check your connection and retry.';
 }finally{scheduleBusy=false;$('refresh').disabled=false;renderGames();renderRotation();}
}

function speak(text,then){
 if(!('speechSynthesis' in window)){then?.();return;}
 speechSynthesis.cancel();const utterance=new SpeechSynthesisUtterance(text);utterance.rate=1.08;
 let done=false;const finish=()=>{if(done)return;done=true;clearTimeout(timer);then?.();};const timer=setTimeout(finish,5000);utterance.onend=finish;utterance.onerror=finish;speechSynthesis.speak(utterance);
}
let commandSerial=0;
function cancelSpeech(){commandSerial++;window.speechSynthesis?.cancel();}
function handleCommand(text){
 cancelSpeech();const token=commandSerial;const result=resolveVoice(text,games,currentGame());notice(`Heard: “${text}”`);
 if(result.kind==='game'){
  player.pause();const phrase=result.matchup?`${team(result.team).name} versus ${team(result.game.home===result.team?result.game.away:result.game.home).name}`:team(result.team).name;
  speak(phrase,()=>{if(token===commandSerial)selectGame(result.game.id,result.team,true);});
 }else if(result.kind==='feed')selectRole(result.role);
 else if(result.kind==='cycle')nextGame(result.direction);
 else if(result.kind==='nextStation'){
  const g=currentGame();if(!g)return notice('Choose a game first.');const queue=inAppQueue(g,requestedTeam,undefined,preferences[requestedTeam]);
  if(!queue.length){showExternalFallback();return;}
  const index=queue.findIndex(f=>f.id===activeFeed?.id);selectInAppStation(queue[(index+1+queue.length)%queue.length]);
 }else if(result.kind==='pause')player.pause();
 else if(result.kind==='resume')toggleResume();
 else{notice(result.message);speak(result.message);}
}

for(const role of ['home','away','national','spanish'])$('broadcastRoles').append(button(role.toUpperCase(),()=>selectRole(role)));
$('start').onclick=()=>{$('setup').hidden=true;$('dial').hidden=false;$('edit').hidden=false;renderRotation();nextGame(1);};
$('edit').onclick=()=>{$('setup').hidden=false;$('dial').hidden=true;$('edit').hidden=true;};
$('listening').onclick=()=>{$('listeningDialog').showModal();};
$('closeListening').onclick=()=>$('listeningDialog').close();
$('refresh').onclick=refresh;$('stations').onclick=()=>stations(false);$('closeStations').onclick=()=>$('stationDialog').close();
externalButton.onclick=()=>stations(true);
$('prev').onclick=()=>nextGame(-1);$('next').onclick=()=>nextGame(1);$('pause').onclick=toggle;
$('commandForm').onsubmit=e=>{e.preventDefault();const text=$('command').value.trim();if(text)handleCommand(text);$('command').value='';};

const Recognition=window.SpeechRecognition||window.webkitSpeechRecognition;
let recognition=null,held=false,voiceText='',voiceTimer=null,voiceStarted=false,voiceError=false;
const mic=$('mic');
function release(cancel=false){held=false;mic.setAttribute('aria-pressed','false');mic.textContent='🎙 HOLD TO TALK';if(cancel){recognition?.abort();clearTimeout(voiceTimer);}else if(voiceStarted)recognition?.stop();}
function hold(){
 if(held||recognition)return;cancelSpeech();held=true;voiceText='';voiceStarted=false;voiceError=false;mic.setAttribute('aria-pressed','true');mic.textContent='LISTENING… RELEASE TO SWITCH';
 const voiceToken=commandSerial;const wasPlaying=phase==='playing';if(player.audio)player.audio.volume=.12;
 recognition=new Recognition();const session=recognition;session.lang='en-US';session.continuous=true;session.interimResults=true;
 session.onstart=()=>{voiceStarted=true;if(!held)session.stop();};session.onresult=e=>{voiceText=Array.from(e.results).map(r=>r[0].transcript).join(' ').trim();notice(`Hearing: ${voiceText}`);};
 session.onerror=e=>{voiceError=true;voiceText='';notice(e.error==='not-allowed'?'Microphone permission denied. Allow the microphone or type a team below.':`Voice unavailable (${e.error}). Type a team below.`);};
 session.onend=()=>{clearTimeout(voiceTimer);recognition=null;voiceStarted=false;held=false;mic.setAttribute('aria-pressed','false');mic.textContent='🎙 HOLD TO TALK';if(player.audio)player.audio.volume=1;if(voiceToken!==commandSerial)return;if(voiceText)handleCommand(voiceText);else if(wasPlaying&&!voiceError)notice('No team heard. Hold to talk again, or type below.');};
 try{session.start();voiceTimer=setTimeout(()=>release(),15000);}catch{recognition=null;release(true);if(player.audio)player.audio.volume=1;notice('Voice could not start. Type a team below.');}
}
if(!Recognition||!window.isSecureContext){mic.disabled=true;mic.textContent='VOICE UNAVAILABLE · TYPE BELOW';}
else{
 mic.onpointerdown=e=>{if(e.button!==0)return;e.preventDefault();mic.setPointerCapture(e.pointerId);hold();};mic.onpointerup=e=>{e.preventDefault();release();};mic.onpointercancel=()=>{voiceText='';release(true);};mic.oncontextmenu=e=>e.preventDefault();mic.onkeydown=e=>{if((e.key===' '||e.key==='Enter')&&!e.repeat){e.preventDefault();hold();}};mic.onkeyup=e=>{if(e.key===' '||e.key==='Enter'){e.preventDefault();release();}};window.addEventListener('blur',()=>{if(held){voiceText='';release(true);}});
}
if('mediaSession' in navigator)for(const [action,handler]of Object.entries({play:toggleResume,pause:()=>player.pause(),previoustrack:()=>nextGame(-1),nexttrack:()=>nextGame(1)})){try{navigator.mediaSession.setActionHandler(action,handler);}catch{}}
new ResizeObserver(()=>document.documentElement.style.setProperty('--player-height',`${$('player').offsetHeight}px`)).observe($('player'));
window.addEventListener('pagehide',()=>{cancelSpeech();voiceText='';release(true);player.stop();});
window.addEventListener('online',refresh);document.addEventListener('visibilitychange',()=>{if(!document.hidden)refresh();});
hideExternalFallback();refresh();setInterval(()=>{if(!document.hidden)refresh();if(activeFeed?.gameAudio&&!playable(activeFeed,currentGame()))void routePlayback(currentGame());},60000);
