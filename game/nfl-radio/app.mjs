import {team,parseSchedule,resolveVoice,playable,feedQueue,cycle,RadioPlayer} from './core.mjs';
import {catalog,directories} from './catalog.mjs';
import {PROVIDERS,widgets,validWidget,widgetFor,listeningLink,safeListeningUrl} from './providers.mjs';
const $=id=>document.getElementById(id);
const read=(key,fallback)=>{try{return JSON.parse(localStorage.getItem(`nfl-dial:${key}`))??fallback;}catch{return fallback;}};
const save=(key,value)=>{try{localStorage.setItem(`nfl-dial:${key}`,JSON.stringify(value));}catch{/* The dial also works without storage. */}};
let games=[],feeds=[],current=null,requestedTeam=null,activeFeed=null,phase='idle',scheduleBusy=false;
let selected=new Set(Array.isArray(read('rotation',[]))?read('rotation',[]).filter(v=>typeof v==='string'):[]);
let preferences=read('preferences',{});if(!preferences||typeof preferences!=='object'||Array.isArray(preferences))preferences={};
let preferencesPending=null;
let providerMode=read('providerMode','auto');if(!PROVIDERS[providerMode])providerMode='auto';
let activeWidget=null,externalLink=null,widgetTimer=null;
const notice=message=>{$('notice').textContent=message;};
const label=g=>`${g.away} vs ${g.home}`;
const currentGame=()=>games.find(g=>g.id===current);
const rotation=()=>games.filter(g=>selected.has(g.id)).map(g=>g.id);
const button=(text,fn,cls='')=>{const b=document.createElement('button');b.textContent=text;b.className=cls;b.onclick=fn;return b;};
const paragraph=(text,cls='')=>{const p=document.createElement('p');p.textContent=text;p.className=cls;return p;};
const player=new RadioPlayer(()=>new Audio(),(state,feed)=>{
 phase=state;activeFeed=feed;
 $('pause').textContent=state==='playing'||state==='loading'?'⏸':'▶';
 $('liveDot').classList.toggle('playing',state==='playing');
 $('nowStation').textContent=feed?`${feed.name} · ${feed.language==='es'?'Spanish':feed.team?team(feed.team)?.name||feed.team:'National'}`:'No verified game stream available';
 const messages={loading:'Connecting to game audio…',playing:'Listening live',paused:'Paused',fallback:`Switching to another ${team(requestedTeam)?.nickname||'game'} station.`,unavailable:'No verified direct game audio is available. See Stations for official listening options.',blocked:'Your browser requires a tap to allow audio. Tap ▶ to resume.'};
 notice(messages[state]||state);
 if(state==='playing'&&preferencesPending===feed?.id){preferences[requestedTeam]=feed.id;save('preferences',preferences);preferencesPending=null;}
 if('mediaSession' in navigator){navigator.mediaSession.playbackState=state==='playing'?'playing':'paused';if(feed&&typeof MediaMetadata!=='undefined')navigator.mediaSession.metadata=new MediaMetadata({title:currentGame()?label(currentGame()):'NFL Radio Dial',artist:feed.name,album:'Catnmice'});}
});
function clearProvider(){
 clearTimeout(widgetTimer);activeWidget=null;externalLink=null;$('widgetMount').replaceChildren();$('providerPlayer').hidden=true;$('stopProvider').hidden=true;$('providerLink').hidden=true;
 $('pause').disabled=false;
}
function showWidget(widget){
 cancelSpeech();
 if(!validWidget(widget))return notice('Unsupported station widget.');
 player.stop();clearProvider();activeFeed=null;activeWidget=widget;phase='provider';
 $('providerPlayer').hidden=false;$('stopProvider').hidden=false;$('pause').disabled=true;
 $('liveDot').classList.remove('playing');$('nowStation').textContent=`${widget.name} · official iHeart player`;
 const frame=document.createElement('iframe');frame.title=`${widget.name} official iHeart player`;frame.src=widget.embedUrl;
 frame.allow='autoplay; geolocation';frame.referrerPolicy='strict-origin-when-cross-origin';
 const help='Use the official player below. It may require Play; game audio depends on your location and the station’s schedule.';
 $('providerMessage').textContent=help;$('providerLink').href=widget.url;$('providerLink').textContent='Open station on iHeart ↗';$('providerLink').hidden=false;
 // Cross-origin load does not prove audio is playing. Never paint a false LIVE indicator.
 frame.onload=()=>{if(activeWidget===widget){clearTimeout(widgetTimer);$('providerMessage').textContent=help;}};
 frame.onerror=()=>{if(activeWidget===widget)$('providerMessage').textContent='The station widget could not load. Open it on iHeart below.';};
 widgetTimer=setTimeout(()=>{if(activeWidget===widget)$('providerMessage').textContent='If the station player is not available, open it on iHeart below.';},12000);
 $('widgetMount').append(frame);notice('Official station player selected. Game availability is checked by iHeart.');
 preferences[requestedTeam]=widget.id;save('preferences',preferences);
}
function showExternal(link,launch=false){
 cancelSpeech();
 if(!link||!safeListeningUrl(link.url))return notice('No verified provider destination for this broadcast.');
 player.stop();clearProvider();activeFeed=null;phase='external';externalLink=link;
 $('liveDot').classList.remove('playing');$('pause').disabled=true;
 $('providerPlayer').hidden=false;$('providerLink').hidden=false;$('providerLink').href=link.url;$('providerLink').textContent=link.label+' ↗';
 $('providerMessage').textContent=link.detail+' Playback stays with the provider.';
 $('nowStation').textContent=link.label;notice('Provider listening option selected. Playback is not controlled by Catnmice.');
 if(launch){
  // A normal provider-owned login/player destination. No credential or session sharing.
  const tab=window.open(link.url,'_blank');
  if(tab){tab.opener=null;notice('Opened the official player. Sign in there if prompted.');}
  else notice('Your browser blocked the provider window. Use the listening link below.');
 }
}
function routePlayback(g,role,manual=false){
 clearProvider();
 const target=role==='home'?g.home:role==='away'?g.away:requestedTeam;
 requestedTeam=target;
 if(providerMode!=='auto'){
  const link=listeningLink(providerMode,g,target,role);
  if(link){showExternal(link,true);return;}
  player.select([]);notice(`No scheduled ${role||'team'} provider broadcast is listed. Choose another broadcast in Stations.`);return;
 }
 const queue=feedQueue(feeds,g,target,preferences[target],role);
 if(queue.length){if(manual)preferencesPending=queue[0].id;player.select(queue);return;}
 const widget=(!role||role==='home'||role==='away')&&widgetFor(target,preferences[target]);
 if(widget){showWidget(widget);return;}
 const link=listeningLink('sirius',g,target,role);
 if(link){showExternal(link);return;}
 player.select([]);
}
function speak(text,then){
 if(!('speechSynthesis' in window)){then?.();return;}
 speechSynthesis.cancel();const utterance=new SpeechSynthesisUtterance(text);utterance.rate=1.08;
 let done=false;const finish=()=>{if(done)return;done=true;clearTimeout(timer);then?.();};
 const timer=setTimeout(finish,5000);utterance.onend=finish;utterance.onerror=finish;speechSynthesis.speak(utterance);
}
let commandSerial=0;
function cancelSpeech(){commandSerial++;window.speechSynthesis?.cancel();}
function selectGame(id,teamId,fromVoice=false){
 if(!fromVoice)cancelSpeech();
 const g=games.find(g=>g.id===id);if(!g)return;
 current=id;requestedTeam=teamId||g.away;preferencesPending=null;
 $('nowGame').textContent=`${label(g)} · ${g.state==='in'?'LIVE':g.state==='post'?'FINAL':new Date(g.date).toLocaleString([],{weekday:'short',hour:'numeric',minute:'2-digit'})}`;
 routePlayback(g);renderRotation();
}
function selectRole(role){
 cancelSpeech();const g=currentGame();if(!g){notice('Choose a game first.');return;}
 if(role==='home'||role==='away')requestedTeam=g[role];
 routePlayback(g,role,true);
}
function selectStation(feed){
 cancelSpeech();const g=currentGame();if(!playable(feed,g)){notice('This broadcast is not available for direct playback.');return;}
 clearProvider();requestedTeam=feed.team||requestedTeam;preferencesPending=feed.id;
 const queue=feedQueue(feeds,g,requestedTeam,feed.id,feed.language==='es'?'spanish':undefined);
 player.select([feed,...queue.filter(f=>f.id!==feed.id)]);$('stationDialog').close();
}
function nextGame(direction){const id=cycle(rotation(),current,direction);if(id)selectGame(id);else notice('Add games to your rotation first.');}
function toggle(){cancelSpeech();if(activeWidget)return notice('Use the official station player’s controls.');if(externalLink)return notice('Use the provider player’s controls.');if(phase==='playing'||phase==='loading')player.pause();else if(player.audio)player.resume();else if(current)selectGame(current,requestedTeam);else nextGame(1);}
function renderGames(){
 $('games').replaceChildren();
 for(const g of games){const card=document.createElement('article');card.className=`card ${selected.has(g.id)?'selected':''}`;
 card.append(paragraph(g.state==='in'?`● LIVE · ${g.status}`:g.state==='post'?`FINAL · ${g.status}`:new Date(g.date).toLocaleString([],{weekday:'short',month:'short',day:'numeric',hour:'numeric',minute:'2-digit'}),'status'));
 const h=document.createElement('h3');h.textContent=label(g);card.append(h,paragraph(`${team(g.away).name} at ${team(g.home).name}`,'teams'));
 const direct=feeds.filter(f=>playable(f,g));
 const names=widgets.filter(f=>[g.home,g.away].includes(f.team)).map(f=>f.name);
 card.append(paragraph(direct.length?`${direct.length} verified game feed${direct.length===1?'':'s'} available`:names.length?`${names.join(' / ')} · local iHeart player. SiriusXM home + away · login required`:'SiriusXM home + away · login required. NFL+ live audio available on provider site','feeds'));
 card.append(button(selected.has(g.id)?'✓ In rotation':'+ Add to rotation',()=>{selected.has(g.id)?selected.delete(g.id):selected.add(g.id);save('rotation',[...selected]);renderGames();renderRotation();}));
 $('games').append(card);}
 if(!games.length)$('games').append(paragraph('No games are scheduled in the current NFL week. Refresh to check again.'));
 $('count').textContent=`${rotation().length} selected`;$('start').disabled=!rotation().length;
}
function renderRotation(){
 $('rotation').replaceChildren();for(const id of rotation()){const g=games.find(x=>x.id===id);const b=button('',()=>selectGame(id),'dialButton'+(current===id?' active':''));const strong=document.createElement('strong');strong.textContent=label(g);const small=document.createElement('small');small.textContent=g.state==='in'?'● LIVE · SELECT BROADCAST':g.status;b.append(strong,small);$('rotation').append(b);}
 for(const id of ['prev','next'])$(id).disabled=!rotation().length;
}
function stations(){
 const g=currentGame();if(!g){notice('Tap a game in your rotation first.');return;}
 $('stationHint').textContent=`${label(g)} · Choosing a playable feed starts audio and remembers it for ${team(requestedTeam).name}.`;
 const list=$('stationList');list.replaceChildren();const roles=document.createElement('div');roles.className='roles';
 for(const role of ['home','away','national','spanish'])roles.append(button(role.toUpperCase(),()=>selectRole(role)));list.append(roles);
 const all=[...feeds.filter(f=>f.gameIds?.includes(g.id)),...catalog.filter(f=>!f.team||[g.home,g.away].includes(f.team))];
 for(const [heading,filter] of [[team(g.away).name,f=>f.team===g.away],[team(g.home).name,f=>f.team===g.home],['Other broadcasts',f=>!f.team]]){
 const h=document.createElement('h3');h.textContent=heading;list.append(h);
 const tId=heading===team(g.away).name?g.away:heading===team(g.home).name?g.home:null;
 if(tId){
  const paid=listeningLink('sirius',g,tId);
  if(paid){const row=document.createElement('div');row.className='station';row.append(button(paid.label+' ↗',()=>{requestedTeam=tId;showExternal(paid,true);$('stationDialog').close();}),paragraph(paid.detail));list.append(row);}
  for(const widget of widgets.filter(w=>w.team===tId)){
   const row=document.createElement('div');row.className='station';row.append(button(`${widget.name} · in-app iHeart player`,()=>{cancelSpeech();requestedTeam=tId;showWidget(widget);$('stationDialog').close();}),paragraph('Official widget. Local-market game restrictions; the station’s ordinary programming may air outside game time.'));list.append(row);
  }
 }
 const rows=all.filter(filter);
 for(const f of rows){const row=document.createElement('div');row.className='station';const available=playable(f,g);
 if(available)row.append(button(`${activeFeed?.id===f.id?'●':'○'} ${f.name}${preferences[requestedTeam]===f.id?' · Preferred':''}`,()=>selectStation(f)));
 else{row.append(paragraph(f.name));if(/^https:\/\//.test(f.sourceUrl||'')){const a=document.createElement('a');a.href=f.sourceUrl;a.target='_blank';a.rel='noopener noreferrer';a.className='provider';a.textContent=f.access==='subscription'?'Open provider · subscription required ↗':'Official listening option ↗';row.append(a);}}
 row.append(paragraph(available?`${f.kind} · ${f.language==='es'?'Spanish':'English'} · verified game audio`:f.note||'No verified direct stream for this game.'));list.append(row);}
 const t=heading===team(g.away).name?g.away:heading===team(g.home).name?g.home:null;
 if(t){const a=document.createElement('a');a.href=`https://www.${directories[t]}/`;a.target='_blank';a.rel='noopener noreferrer';a.className='provider';a.textContent=`${team(t).nickname} official site ↗`;list.append(a);}
 }
 if(!$('stationDialog').open)$('stationDialog').showModal();
}
async function json(url){const controller=new AbortController();const timer=setTimeout(()=>controller.abort(),12000);try{const r=await fetch(url,{cache:'no-store',signal:controller.signal});if(!r.ok)throw new Error(`HTTP ${r.status}`);return await r.json();}finally{clearTimeout(timer);}}
async function refresh(){
 if(scheduleBusy)return;scheduleBusy=true;$('refresh').disabled=true;
 try{
  const results=await Promise.allSettled([json('https://site.api.espn.com/apis/site/v2/sports/football/nfl/scoreboard'),json('./nfl-radio/feeds.json')]);
  if(results[1].status==='fulfilled'&&Array.isArray(results[1].value.feeds))feeds=results[1].value.feeds.filter(f=>f&&typeof f.id==='string'&&typeof f.name==='string');
  else notice('Broadcast catalog could not refresh. Previously loaded feeds still expire on schedule.');
  if(results[0].status==='rejected')throw results[0].reason;
  games=parseSchedule(results[0].value);save('schedule',{at:Date.now(),games});
  $('scheduleStatus').textContent=`${results[0].value.week?.number?'Week '+results[0].value.week.number+' · ':''}Updated ${new Date().toLocaleTimeString([],{hour:'numeric',minute:'2-digit'})}`;
  // Drop departed weeks only after a successful fresh schedule, never on a network error.
  selected=new Set([...selected].filter(id=>games.some(g=>g.id===id)));save('rotation',[...selected]);
 }catch{
  if(!games.length){const cached=read('schedule',null);if(cached&&Date.now()-cached.at<86400000&&Array.isArray(cached.games))games=cached.games.filter(g=>g&&team(g.home)&&team(g.away)&&Number.isFinite(Date.parse(g.date)));}
  $('scheduleStatus').textContent=games.length?'Offline · showing cached schedule, live status may be outdated':'Schedule unavailable. Check your connection and retry.';
 }finally{scheduleBusy=false;$('refresh').disabled=false;renderGames();renderRotation();}
}
function handleCommand(text){
 cancelSpeech();const token=commandSerial;const result=resolveVoice(text,games,currentGame());
 notice(`Heard: “${text}”`);
 if(result.kind==='game'){
  player.pause();const phrase=result.matchup?`${team(result.team).name} versus ${team(result.game.home===result.team?result.game.away:result.game.home).name}`:team(result.team).name;
  speak(phrase,()=>{if(token===commandSerial)selectGame(result.game.id,result.team,true);});
 }else if(result.kind==='feed')selectRole(result.role);
 else if(result.kind==='cycle')nextGame(result.direction);
 else if(result.kind==='nextStation'){
  const g=currentGame();if(!g)return notice('Choose a game first.');
  const list=feeds.filter(f=>playable(f,g));if(!list.length){const options=widgets.filter(w=>[g.home,g.away].includes(w.team));if(options.length){const next=options[(options.findIndex(w=>w.id===activeWidget?.id)+1)%options.length];requestedTeam=next.team;showWidget(next);}else notice('Use Stations to choose another provider broadcast.');return;}
  selectStation(list[(list.findIndex(f=>f.id===activeFeed?.id)+1)%list.length]);
 }else if(result.kind==='pause'){if(activeWidget){clearProvider();phase='paused';notice('Station stopped.');}else if(externalLink)notice('Pause audio in the provider player.');else player.pause();}else if(result.kind==='resume')toggleResume();
 else{notice(result.message);speak(result.message);}
}
function toggleResume(){if(activeWidget)return notice('Use Play in the official player below.');if(externalLink)return showExternal(externalLink,true);if(player.audio)player.resume();else if(current)selectGame(current,requestedTeam);else nextGame(1);}
for(const role of ['home','away','national','spanish'])$('broadcastRoles').append(button(role.toUpperCase(),()=>selectRole(role)));
$('start').onclick=()=>{$('setup').hidden=true;$('dial').hidden=false;$('edit').hidden=false;renderRotation();nextGame(1);};
$('edit').onclick=()=>{$('setup').hidden=false;$('dial').hidden=true;$('edit').hidden=true;};
$('listening').onclick=()=>{$('providerMode').value=providerMode;$('modeDescription').textContent=PROVIDERS[providerMode].description;$('listeningDialog').showModal();};
$('providerMode').onchange=()=>{providerMode=$('providerMode').value;save('providerMode',providerMode);$('modeDescription').textContent=PROVIDERS[providerMode].description;};
$('closeListening').onclick=()=>$('listeningDialog').close();
$('stopProvider').onclick=()=>{cancelSpeech();clearProvider();phase='paused';$('nowStation').textContent='Station stopped';notice('Station stopped. Tap a game to select audio again.');};
$('refresh').onclick=refresh;$('stations').onclick=stations;$('closeStations').onclick=()=>$('stationDialog').close();
$('prev').onclick=()=>nextGame(-1);$('next').onclick=()=>nextGame(1);$('pause').onclick=toggle;
$('commandForm').onsubmit=e=>{e.preventDefault();const text=$('command').value.trim();if(text)handleCommand(text);$('command').value='';};
const Recognition=window.SpeechRecognition||window.webkitSpeechRecognition;
let recognition=null,held=false,voiceText='',voiceTimer=null,voiceStarted=false,voiceError=false;
const mic=$('mic');
function release(cancel=false){
 held=false;mic.setAttribute('aria-pressed','false');mic.textContent='🎙 HOLD TO TALK';
 if(cancel){recognition?.abort();clearTimeout(voiceTimer);}else if(voiceStarted)recognition?.stop();
}
function hold(){
 if(held||recognition)return;cancelSpeech();held=true;voiceText='';voiceStarted=false;voiceError=false;
 mic.setAttribute('aria-pressed','true');mic.textContent='LISTENING… RELEASE TO SWITCH';
 const voiceToken=commandSerial;const suspendedWidget=activeWidget;if(suspendedWidget)clearProvider();
 const wasPlaying=phase==='playing';if(player.audio)player.audio.volume=.12;
 recognition=new Recognition();const session=recognition;session.lang='en-US';session.continuous=true;session.interimResults=true;
 session.onstart=()=>{voiceStarted=true;if(!held)session.stop();};
 session.onresult=e=>{voiceText=Array.from(e.results).map(r=>r[0].transcript).join(' ').trim();notice(`Hearing: ${voiceText}`);};
 session.onerror=e=>{voiceError=true;voiceText='';notice(e.error==='not-allowed'?'Microphone permission denied. Allow the microphone or type a team below.':`Voice unavailable (${e.error}). Type a team below.`);};
 session.onend=()=>{clearTimeout(voiceTimer);recognition=null;voiceStarted=false;held=false;mic.setAttribute('aria-pressed','false');mic.textContent='🎙 HOLD TO TALK';if(player.audio)player.audio.volume=1;if(voiceToken!==commandSerial)return;if(voiceText)handleCommand(voiceText);else if(suspendedWidget)showWidget(suspendedWidget);else if(wasPlaying&&!voiceError)notice('No team heard. Hold to talk again, or type below.');};
 try{session.start();voiceTimer=setTimeout(()=>release(),15000);}catch{recognition=null;release(true);if(player.audio)player.audio.volume=1;notice('Voice could not start. Type a team below.');}
}
if(!Recognition||!window.isSecureContext){mic.disabled=true;mic.textContent='VOICE UNAVAILABLE · TYPE BELOW';}
else{
 mic.onpointerdown=e=>{if(e.button!==0)return;e.preventDefault();mic.setPointerCapture(e.pointerId);hold();};
 mic.onpointerup=e=>{e.preventDefault();release();};mic.onpointercancel=()=>{voiceText='';release(true);};
 mic.oncontextmenu=e=>e.preventDefault();mic.onkeydown=e=>{if((e.key===' '||e.key==='Enter')&&!e.repeat){e.preventDefault();hold();}};
 mic.onkeyup=e=>{if(e.key===' '||e.key==='Enter'){e.preventDefault();release();}};
 window.addEventListener('blur',()=>{if(held){voiceText='';release(true);}});
}
if('mediaSession' in navigator)for(const [action,handler]of Object.entries({play:toggleResume,pause:()=>player.pause(),previoustrack:()=>nextGame(-1),nexttrack:()=>nextGame(1)})){try{navigator.mediaSession.setActionHandler(action,handler);}catch{}}
new ResizeObserver(()=>document.documentElement.style.setProperty('--player-height',`${$('player').offsetHeight}px`)).observe($('player'));
window.addEventListener('pagehide',()=>{cancelSpeech();voiceText='';release(true);clearProvider();player.stop();});
window.addEventListener('online',refresh);document.addEventListener('visibilitychange',()=>{if(!document.hidden)refresh();});
refresh();setInterval(()=>{if(!document.hidden)refresh();if(activeFeed&&!playable(activeFeed,currentGame()))player.select([]);},60000);
