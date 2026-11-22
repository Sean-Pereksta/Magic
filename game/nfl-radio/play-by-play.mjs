import {loadNaturalVoice,onNaturalVoiceStatus} from './natural-voice.mjs';
import {readSettings as readRecapSettings} from './game-updates.mjs';
import {gamePlayerStats,playStatFooter} from './player-stats.mjs';
import {installStatsView} from './stats-view.mjs';
import {installFieldView} from './field-view.mjs';
import {leagueName,storagePrefix,API,SCOREBOARD_URL} from './league.mjs';
import {browserSpeechAvailable,cancelBrowserSpeech,enqueueBrowserSpeech,getBrowserSpeechQueueState,playBrowserSpeechNow,removeQueuedSpeech,resumeBrowserSpeech,unlockBrowserSpeech,onBrowserSpeechQueueState} from './speech-queue.mjs';
import {setRadioDucked} from './radio-audio-bridge.mjs';
import {PLAY_TYPES,formatPlay,mergePlayers,summaryPlayers,rosterPlayers,shouldAnnouncePlay} from './play-formatter.mjs';
import {createPlayTracker,playIdentity,playsFromSummary} from './play-tracker.mjs';
import {STORAGE_KEY,readPlayByPlaySettings,settingsForGame,selectedGameIds,normalizeScoreInterval} from './play-settings.mjs';
import {readVoiceSettings,saveVoiceSettings,englishVoices,playVoiceOptions} from './voice-settings.mjs';
export {readPlayByPlaySettings,selectedGameIds,normalizeScoreInterval,DEFAULT_SCORE_INTERVAL_MINUTES,MAX_SCORE_INTERVAL_MINUTES} from './play-settings.mjs';
export const PLAY_POLL_MS=5000;


const searchKey=value=>String(value??'').normalize('NFD').replace(/[\u0300-\u036f]/g,'').toLowerCase().replace(/[^a-z0-9]+/g,' ').trim();
const clean=value=>String(value??'').replace(/\s+/g,' ').trim();
const canonical=value=>({WSH:'WAS',JAC:'JAX',LA:'LAR'}[value]||value);
const scoreNumber=value=>Number.isFinite(Number(value))?Number(value):0;
const competition=event=>event?.competitions?.[0]||null;
const competitors=event=>competition(event)?.competitors||[];
const teamLabel=entry=>entry?.team?.shortDisplayName||entry?.team?.name||entry?.team?.displayName||entry?.team?.abbreviation||'';
const gameState=event=>competition(event)?.status?.type?.state||event?.status?.type?.state||'pre';
function eventMatchup(event){return ['away','home'].map(role=>canonical(competitors(event).find(c=>c.homeAway===role)?.team?.abbreviation)).filter(Boolean);}
export function radioLabelMatchup(label){const match=clean(label).match(/^([A-Z]{2,3})\s+vs\s+([A-Z]{2,3})\b/);return match?[canonical(match[1]),canonical(match[2])]:[];}
export function matchupMatchesRadioLabel(matchup,label){const current=radioLabelMatchup(label);return current.length===2&&matchup?.length===2&&current[0]===canonical(matchup[0])&&current[1]===canonical(matchup[1]);}
export function eventMatchesRadioLabel(event,label){return matchupMatchesRadioLabel(eventMatchup(event),label);}
export function latestPlayAnnouncement(event,{players=[]}={}){
  const raw=competition(event)?.situation?.lastPlay;
  if(!raw||gameState(event)!=='in')return null;
  const formatted=formatPlay(raw,{event,players:mergePlayers(players,summaryPlayers(null,event))});
  return formatted.text?{...formatted,gameId:String(event.id),key:playIdentity(raw),matchup:eventMatchup(event),raw,play:formatted.text,speech:formatted.text}:null;
}
export function collectNewPlayAnnouncements(events,selectedIds,seenKeys=new Map(),{announceInitial=false,settings={},players=[]}={}){
  const selected=new Set((selectedIds||[]).map(String)),announcements=[];
  for(const event of events||[]){
    if(!selected.has(String(event.id)))continue;
    const out=latestPlayAnnouncement(event,{players});if(!out)continue;
    const previous=seenKeys.get(out.gameId);
    const seen=previous instanceof Set?previous:new Set(previous?[previous]:[]);
    const fresh=!seen.has(out.key);seen.add(out.key);seenKeys.set(out.gameId,seen);
    if(fresh&&(previous||announceInitial)&&shouldAnnouncePlay(out.raw,settingsForGame(settings,out.gameId),{event,players,formatted:out}))announcements.push(out);
  }
  return announcements;
}
function scoreStatusLabel(event){
  const c=competition(event);
  const state=gameState(event);
  const detail=clean(c?.status?.type?.shortDetail||c?.status?.type?.detail||event?.status?.type?.shortDetail||event?.status?.type?.detail);
  if(state==='post')return 'Final';
  if(/half/i.test(detail))return 'Halftime';
  return '';
}

export function scoreLine(event){
  const state=gameState(event);
  if(state==='pre')return '';
  const away=competitors(event).find(c=>c.homeAway==='away');
  const home=competitors(event).find(c=>c.homeAway==='home');
  const awayName=teamLabel(away);
  const homeName=teamLabel(home);
  if(!awayName||!homeName)return '';
  const prefix=scoreStatusLabel(event);
  const line=`${awayName} ${scoreNumber(away?.score)}, ${homeName} ${scoreNumber(home?.score)}.`;
  return prefix?`${prefix}: ${line}`:line;
}

export function buildScoreUpdateSpeech(events,{selectedIds=[],scope='rotation'}={}){
  const selected=new Set((selectedIds||[]).map(String));
  const lines=(events||[])
    .filter(event=>gameState(event)!=='pre')
    .filter(event=>scope==='all'||selected.has(String(event?.id??'')))
    .map(scoreLine)
    .filter(Boolean);
  return lines.length?`${leagueName} score update. ${lines.join(' ')}`:'';
}

async function fetchJson(url){
  const controller=new AbortController(),timer=setTimeout(()=>controller.abort(),9000);
  try{const response=await fetch(url,{cache:'no-store',signal:controller.signal});if(!response.ok)throw new Error(`HTTP ${response.status}`);return await response.json();}finally{clearTimeout(timer);}
}
function element(tag,text,attrs={}){
  const node=document.createElement(tag);if(text!=null)node.textContent=text;
  for(const [key,value] of Object.entries(attrs))if(key==='class')node.className=value;else if(key in node)node[key]=value;else node.setAttribute(key,value);
  return node;
}
export function installLivePlayByPlay(){
  if(typeof document==='undefined'||!document.getElementById('activeGame'))return;
  const $=id=>document.getElementById(id);
  let settings=readPlayByPlaySettings(),voiceSettings=readVoiceSettings();
  let recapsEnabled=readRecapSettings().enabled;
  let gameOptionsKey='',playerPaintKey='',listeningGamesKey='',transcriptKey='',lastSpoken='';
  let events=[],viewedGame='',polling=false,nextScoreAt=Date.now()+settings.scoreIntervalMinutes*60000,disposed=false,dataFailed=false;
  let renderPending=false;
  const tracker=createPlayTracker(),summaries=new Map(),rosters=new Map(),playersByGame=new Map(),historyStatus=new Map();
  const liveGames=new Set(),gameRequests=new Set();
  const fields=installFieldView();
  const statsView=installStatsView(),statsByGame=new Map();
  const paintFields=()=>fields?.render(events,selectedGameIds(),id=>tracker.history(id).at(-1),{stale:dataFailed,focusedGame:viewedGame,getFooter:id=>playStatFooter(tracker.history(id).at(-1),statsByGame.get(id)||[])});
  const save=()=>{try{localStorage.setItem(STORAGE_KEY,JSON.stringify(settings));}catch{}};
  const currentRadioId=()=>$('nowGame')?.dataset.gameId||'';
  const onAirGame=()=>$('nowGame')?.dataset.playingGameId||'';
  const monitored=()=>selectedGameIds();
  const selectedPlayers=id=>settings.selectedPlayersByGame[String(id)]||[];
  const rotationEvents=()=>{const ids=new Set(monitored());return events.filter(event=>ids.has(String(event.id)));};
  const gameName=event=>eventMatchup(event).join(' vs ')||event?.shortName||'Game';
  const canHear=event=>{
    const id=String(event.id),mode=settingsForGame(settings,id).mode;
    return mode!=='off'&&(mode!=='players'||selectedPlayers(id).length>0)&&(settings.includeRadioGame||onAirGame()!==id);
  };
  const currentEvent=()=>events.find(e=>String(e.id)===viewedGame);
  const renderLiveUpdates=()=>{
    if(renderPending)return;
    renderPending=true;
    requestAnimationFrame(()=>{
      renderPending=false;
      if(disposed)return;
      paintHero();
      if($('playByPlayDialog').open)paintPlayers();
    });
  };
  const allowed=entry=>{
    const event=events.find(e=>String(e.id)===entry.gameId);
    const latest=tracker.history(entry.gameId).find(play=>play.key===entry.key)||entry;
    return !!event&&settings.enabled&&monitored().includes(entry.gameId)&&canHear(event)&&
      shouldAnnouncePlay(latest.raw,settingsForGame(settings,entry.gameId),{event,players:playersByGame.get(entry.gameId)||[],formatted:latest});
  };
  const status=message=>{$('playByPlayStatus').textContent=message;};
  const replayKey=entry=>`transcript-replay:${entry.gameId}:${entry.key}`;
  function paintReplayButtons(state){
    const active=state.current?.source==='transcript-replay'?state.current.key:'';
    for(const button of document.querySelectorAll('.replayPlay[data-speech-key]')){
      const playing=button.dataset.speechKey===active;
      button.textContent=playing?'■':'🔊';
      button.setAttribute('aria-pressed',String(playing));
      button.setAttribute('aria-label',`${playing?'Stop':'Play'} audio for ${button.dataset.playLabel}`);
      button.title=playing?'Stop replay':'Replay this play';
      button.disabled=!state.available;
    }
  }
  const paintVoiceState=(state=getBrowserSpeechQueueState())=>{
    const ready=rotationEvents().filter(canHear),live=ready.filter(event=>gameState(event)==='in').length;
    const pending=!state.activated&&!state.starting&&!state.speaking&&(settings.enabled||recapsEnabled);
    $('resumeVoice').hidden=!state.available||(!state.blocked&&!pending);
    const indicator=$('speechIndicator');
    const queue=state.queued?` · ${state.queued} queued`:'';
    let message='Play calls off',tone='off';
    if(!state.available){message='Voice unavailable';tone='attention';}
    else if(state.blocked||pending){message=(state.blocked?'Voice needs a tap':'Tap to start voice')+queue;tone='attention';}
    else if(state.suspended){message='Voice paused for microphone'+queue;tone='attention';}
    else if(state.speaking){message=(state.current?.source==='transcript-replay'?'Playing replay':'Speaking')+queue;tone='speaking';}
    else if(state.starting){message=(state.current?.source==='transcript-replay'?'Starting replay':'Starting audio')+queue;tone='on';}
    else if(settings.enabled){
      tone='on';
      if(dataFailed)message='Play calls on · reconnecting to ESPN';
      else if(!monitored().length)message='Play calls on · add games to your rotation';
      else if(!events.length)message='Play calls on · waiting for the schedule';
      else if(!ready.length)message=!settings.includeRadioGame&&onAirGame()?'Play calls on · on-air game excluded':'Play calls on · choose games or follow players';
      else if(!live)message='Play calls on · waiting for live games';
      else message=`Play calls on · ${live} live ${live===1?'game':'games'}`;
      message+=queue;
    }
    if(indicator.textContent!==message)indicator.textContent=message;
    indicator.dataset.tone=tone;
    $('lastSpoken').textContent=lastSpoken?`Last play call ${lastSpoken}`:'';
    paintReplayButtons(state);
  };
  const modeOptions=[['','Use default'],['every','All players'],['touchdowns','Touchdowns only'],['players','Followed players only'],['off','Mute play calls']];
  function paintListeningGames(){
    const games=rotationEvents(),container=$('listeningGames');
    const key=JSON.stringify(games.map(event=>{const id=String(event.id);return [id,gameName(event),settings.modeByGame[id],selectedPlayers(id).length,onAirGame()===id&&!settings.includeRadioGame];}));
    if(key===listeningGamesKey)return;
    listeningGamesKey=key;
    const focused=document.activeElement?.dataset.listenGame;
    container.replaceChildren();
    for(const event of games){
      const id=String(event.id),name=gameName(event);
      const row=element('div',null,{class:'listeningGame'});
      const label=element('label',null,{htmlFor:`listen-game-${id}`});
      const detail=onAirGame()===id&&!settings.includeRadioGame?'On-air broadcast · play calls paused':`${selectedPlayers(id).length} followed`;
      label.append(element('strong',name),element('small',detail));
      const select=element('select',null,{id:`listen-game-${id}`});select.dataset.listenGame=id;
      for(const [value,text] of modeOptions)select.append(element('option',text,{value}));
      select.value=settings.modeByGame[id]||'';
      select.onchange=()=>{if(select.value)settings.modeByGame[id]=select.value;else delete settings.modeByGame[id];saveFilters();};
      row.append(label,select);container.append(row);
      if(focused===id)select.focus({preventScroll:true});
    }
    if(!games.length)container.append(element('p','Add games to your rotation to set up play calls.',{class:'emptyState'}));
  }
  const paintControls=()=>{
    $('defaultPlayMode').value=settings.mode;
    for(const input of $('playTypes').querySelectorAll('input'))input.checked=settings.playTypes.includes(input.value);
    for(const button of document.querySelectorAll('[data-play-mode]')){
      const mode=button.dataset.playMode;
      button.setAttribute('aria-pressed',String(mode===settings.mode&&(mode!=='every'||!settings.playTypes.length)));
    }
    $('fieldVoiceToggle').textContent=settings.enabled?'Play calls on':'Enable play calls';
    $('fieldVoiceToggle').setAttribute('aria-pressed',String(settings.enabled));
    $('announcementsToggle').textContent=settings.enabled?'Play calls on':'Enable play calls';
    $('announcementsToggle').setAttribute('aria-pressed',String(settings.enabled));
    $('playByPlayEnabled').checked=settings.enabled;$('playByPlayDuckRadio').checked=settings.duckRadio;
    $('includeRadioGame').checked=settings.includeRadioGame;$('playByPlayScoreInterval').value=settings.scoreIntervalMinutes;$('playByPlayScoreScope').value=settings.scoreScope;
    $('voiceStyle').value=voiceSettings.style;
    $('voiceEngine').value=voiceSettings.engine||'device';$('naturalVoice').value=voiceSettings.naturalVoice||'af_heart';
    const games=rotationEvents();
    const followed=games.flatMap(event=>selectedPlayers(event.id));
    const count=followed.length;
    const names=[...new Set(followed.map(p=>p.name))];
    $('selectedPlayerSummary').textContent=names.slice(0,4).join(' · ')+(names.length>4?` · +${names.length-4} more`:'');
    $('selectedPlayerSummary').hidden=!count;
    $('selectedPlayerCount').textContent=count?`· ${count}`:'';
    $('listenToFollowed').hidden=settings.mode==='players';
    $('listenToFollowed').disabled=!count;
    const overrides=games.filter(event=>settings.modeByGame[String(event.id)]).length;
    const types=settings.playTypes.map(type=>PLAY_TYPES[type]);
    const detail=settings.mode==='touchdowns'?'Touchdowns only':[settings.mode==='players'?'Followed players':'All players',types.length?types.join(', '):'Every play type'].join(' · ');
    $('announcementHint').textContent=!browserSpeechAvailable()?'Voice is unavailable in this browser. Live transcripts still work.':
      `${detail}${overrides?` · ${overrides} ${overrides===1?'game has':'games have'} custom settings`:''}`;
    paintListeningGames();paintVoiceState();
  };
  const paintVoiceList=()=>{
    const voices=englishVoices(globalThis.speechSynthesis?.getVoices?.()||[]),select=$('voiceSelect');
    select.replaceChildren(element('option','Automatic · best available English',{value:''}));
    for(const voice of voices)select.append(element('option',`${voice.name} · ${voice.lang}`,{value:voice.voiceURI}));
    if(voiceSettings.voiceURI&&!voices.some(v=>v.voiceURI===voiceSettings.voiceURI))select.append(element('option','Saved voice unavailable · using automatic',{value:voiceSettings.voiceURI}));
    select.value=voiceSettings.voiceURI;
  };
  const paintGameOptions=()=>{
    const ids=monitored(),available=events.filter(e=>ids.includes(String(e.id)));
    tracker.retain(ids);
    for(const cache of [summaries,playersByGame,historyStatus,statsByGame])for(const id of cache.keys())if(!ids.includes(id))cache.delete(id);
    for(const id of liveGames)if(!ids.includes(id))liveGames.delete(id);
    if(!available.some(e=>String(e.id)===viewedGame))viewedGame=available.find(e=>String(e.id)===currentRadioId())?.id||available[0]?.id||'';
    viewedGame=String(viewedGame);
    const optionsKey=available.map(e=>`${e.id}:${eventMatchup(e).join('-')}`).join('|');
    if(optionsKey===gameOptionsKey){$('transcriptGame').value=viewedGame;return;}
    gameOptionsKey=optionsKey;
    for(const id of ['transcriptGame','playerGame']){
      const select=$(id),previous=select.value;
      select.replaceChildren();
      if(id==='playerGame'||!available.length)select.append(element('option',available.length?'All rotation games':'Add a game to your rotation',{value:''}));
      for(const event of available)select.append(element('option',eventMatchup(event).join(' vs '),{value:String(event.id)}));
      select.value=id==='transcriptGame'?viewedGame:available.some(e=>String(e.id)===previous)?previous:'';
    }
  };
  function paintHero(){
    paintFields();
    const snapshot=summaries.get(viewedGame);
    statsView.render(statsByGame.get(viewedGame)||[],eventMatchup(currentEvent()).join(' vs '),snapshot?`${dataFailed||snapshot.failed?'Data delayed · last saved totals':'Latest ESPN game totals'} · ${new Date(snapshot.at).toLocaleTimeString()} · may trail live plays`:'Waiting for ESPN player stats.');
    const event=currentEvent(),c=competition(event),hero=$('gameScore');hero.replaceChildren();
    $('gameLiveState').textContent=dataFailed?'RECONNECTING':event?gameState(event)==='in'?'LIVE':gameState(event)==='post'?'FINAL':'UPCOMING':'YOUR DIAL';
    $('gameLiveState').classList.toggle('isLive',!!event&&gameState(event)==='in'&&!dataFailed);
    if(!event){hero.append(element('p','Pick your games. We’ll follow the action.',{class:'gameEmpty'}));$('gameSituation').textContent='Add games below to start your live companion.';paintTranscript();return;}
    const gameStatus=c?.status||event.status||{};
    for(const role of ['away','home']){
      const entry=competitors(event).find(c=>c.homeAway===role),team=entry?.team||{};
      const side=element('div',null,{class:`scoreTeam ${role}`});
      const logo=team.logo||team.logos?.[0]?.href;
      if(/^https:\/\//.test(logo||'')){const img=element('img',null,{src:logo,alt:'',width:72,height:72});img.onerror=()=>{img.hidden=true;};side.append(img);}
      const name=element('div',null,{class:'teamIdentity'});name.append(element('span',team.location||'',{class:'teamCity'}),element('strong',team.name||team.shortDisplayName||team.abbreviation||'Team'));
      if(gameState(event)==='in'&&String(c.situation?.possession)===String(team.id||entry.id))name.append(element('span','● Possession',{class:'possession'}));
      side.append(name,element('b',gameState(event)==='pre'?'—':entry?.score??'—',{class:'scoreValue'}));hero.append(side);
    }
    const clock=gameStatus.displayClock,period=gameStatus.period;
    const time=gameState(event)==='in'&&clock&&period?`${period>4?'OT':`Q${period}`} · ${clock}`:gameStatus.type?.shortDetail||gameStatus.type?.detail||'Scheduled';
    $('gameSituation').textContent=[time,c?.situation?.downDistanceText].filter(Boolean).join('  ·  ');
    paintTranscript();
  }
  function paintTranscript(){
    const history=tracker.history(viewedGame),latest=history.at(-1),list=$('transcriptList');
    $('latestPlay').textContent=latest?.text||'Waiting for the next play.';
    $('latestPlayStats').textContent=playStatFooter(latest,statsByGame.get(viewedGame)||[]);
    $('latestPlayLabel').textContent=latest?.label||'LIVE PLAY CALL';
    $('latestCall').dataset.kind=latest?.kind||'play';
    $('transcriptCount').textContent=String(history.length);
    $('transcriptMeta').textContent=historyStatus.get(viewedGame)||'Recent plays appear here when ESPN posts them.';
    const stats=statsByGame.get(viewedGame)||[];
    const rows=history.slice().reverse().map(entry=>({entry,footer:playStatFooter(entry,stats)}));
    const key=JSON.stringify([viewedGame,rows.map(({entry,footer})=>[entry.key,entry.text,entry.label,entry.kind,entry.important,entry.period,entry.clock,footer])]);
    if(key===transcriptKey)return;
    transcriptKey=key;
    const focused=list.contains(document.activeElement)?document.activeElement.dataset.speechKey:null;
    bindReplay($('replayLatestCall'),latest);
    list.replaceChildren();
    for(const {entry,footer} of rows){
      const row=element('li',null,{class:`transcriptPlay ${entry.important?'important':''}`});row.dataset.kind=entry.kind;
      const meta=element('div',null,{class:'playMeta'});meta.append(element('strong',entry.label),element('span',[entry.period?`Q${entry.period}`:'',entry.clock].filter(Boolean).join(' · ')));
      const call=element('div',null,{class:'playWithAudio'}),button=element('button',null,{class:'replayPlay',type:'button'});
      bindReplay(button,entry);
      call.append(element('p',entry.text),button);
      row.append(meta,call);
      if(footer)row.append(element('small',footer,{class:'playStatFooter'}));list.append(row);
      if(focused===button.dataset.speechKey)button.focus({preventScroll:true});
    }
    if(!history.length)list.append(element('li','No plays yet for this game.',{class:'emptyState'}));
    paintReplayButtons(getBrowserSpeechQueueState());
  }
  function bindReplay(button,entry){
    button.hidden=!entry;
    if(!entry){delete button.dataset.speechKey;button.onclick=null;return;}
    const key=replayKey(entry);
    button.dataset.speechKey=key;
    button.dataset.playLabel=[entry.label,entry.period?`Q${entry.period}`:'',entry.clock].filter(Boolean).join(' · ');
    button.onclick=()=>{
      if(getBrowserSpeechQueueState().current?.key===key)cancelBrowserSpeech(item=>item.key===key);
      else replayPlay(entry);
    };
  }
  async function ensureRoster(team){
    if(!team?.id)return [];
    const id=String(team.id),existing=rosters.get(id);
    if(existing?.pending)return existing.pending;
    if(existing&&Date.now()<existing.expires)return existing.players;
    const entry={players:existing?.players||[],expires:Date.now()+60000};
    entry.pending=fetchJson(`${API}/teams/${encodeURIComponent(id)}/roster`).then(data=>{entry.players=rosterPlayers(data,team);entry.expires=Date.now()+3600000;entry.failed=false;return entry.players;}).catch(()=>{entry.failed=true;return entry.players;}).finally(()=>{entry.pending=null;});
    rosters.set(id,entry);return entry.pending;
  }
  function cachePlayers(event,summary){
    const id=String(event.id);
    const lists=competitors(event).map(c=>rosters.get(String(c.team.id))?.players||[]);
    const saved=selectedPlayers(id).map(p=>({id:p.id,displayName:p.name,team:competitors(event).find(c=>c.team.abbreviation===p.team)?.team||{abbreviation:p.team}}));
    const players=mergePlayers(saved,...lists,summaryPlayers(summary,event));playersByGame.set(id,players);
    return players;
  }
  function loadRosters(games=rotationEvents()){
    for(const event of games){
      const id=String(event.id);
      const requests=competitors(event).filter(c=>{
        const roster=rosters.get(String(c.team.id));return !roster||(!roster.pending&&Date.now()>=roster.expires);
      }).map(c=>ensureRoster(c.team));
      if(!requests.length)continue;
      void Promise.all(requests).then(()=>{
        if(disposed||!monitored().includes(id))return;
        cachePlayers(event,summaries.get(id)?.data);
        if($('playByPlayDialog').open)paintPlayers();
      });
    }
  }
  function paintPlayers(){
    const id=$('playerGame').value,query=searchKey($('playerSearch').value);
    const all=rotationEvents(),games=id?all.filter(event=>String(event.id)===id):all;
    const positionSelect=$('playerPosition'),previousPosition=positionSelect.value;
    const gamePlayers=event=>playersByGame.get(String(event.id))||[];
    const positions=[...new Set(games.flatMap(gamePlayers).map(player=>player.position).filter(Boolean))].sort();
    if([...positionSelect.options].slice(1).map(option=>option.value).join('|')!==positions.join('|')){
      positionSelect.replaceChildren(element('option','All positions',{value:''}),...positions.map(position=>element('option',position,{value:position})));
      positionSelect.value=positions.includes(previousPosition)?previousPosition:'';
    }
    const position=positionSelect.value,list=$('playerList'),tags=$('selectedPlayers');
    const rosterStates=games.flatMap(event=>competitors(event).map(c=>rosters.get(String(c.team.id))));
    const renderKey=JSON.stringify([id,query,position,all.map(event=>[event.id,selectedPlayers(event.id)]),games.map(gamePlayers),rosterStates.map(roster=>[!!roster?.pending,!!roster?.failed])]);
    if(renderKey===playerPaintKey)return;
    playerPaintKey=renderKey;
    const focused=document.activeElement?.dataset.playerKey,scroll=list.scrollTop;
    const searchWords=query.split(' ').filter(Boolean);
    list.replaceChildren();tags.replaceChildren();
    const toggle=(id,player)=>{
      const existing=selectedPlayers(id),has=existing.some(p=>p.id===player.id);
      settings.selectedPlayersByGame[id]=has?existing.filter(p=>p.id!==player.id):[...existing,{id:player.id,name:player.name,team:player.team}];
      saveFilters();paintPlayers();
    };
    for(const event of all)for(const player of selectedPlayers(event.id)){
      const chip=element('button',null,{class:'playerChip','aria-label':`Remove ${player.name}, ${gameName(event)}`});
      chip.append(document.createTextNode(`${player.name} ×`),element('small',gameName(event)));
      chip.onclick=()=>{toggle(String(event.id),player);$('playerSearch').focus({preventScroll:true});};tags.append(chip);
    }
    let matchesCount=0;
    for(const event of games){
      const gameId=String(event.id),players=gamePlayers(event),chosen=selectedPlayers(gameId);
      const gameGroup=element('section',null,{class:'pickerGame'});
      gameGroup.append(element('h4',gameName(event)));
      for(const {team} of competitors(event)){
        const name=team.displayName||team.shortDisplayName||team.abbreviation;
        const matches=players.filter(player=>{
          if(!player.id||(player.teamId!==String(team.id)&&player.team!==team.abbreviation)||(position&&player.position!==position))return false;
          const searchable=searchKey(`${player.name} ${player.position} ${name} ${team.abbreviation}`);
          return searchWords.every(word=>searchable.includes(word));
        }).sort((a,b)=>a.name.localeCompare(b.name));
        if(!matches.length)continue;
        const group=element('section',null,{class:'rosterGroup'});group.append(element('h5',name));
        for(const player of matches){
          const option=element('button',null,{class:'playerOption','aria-pressed':String(chosen.some(p=>p.id===player.id))});
          option.dataset.playerKey=`${gameId}:${player.id}`;
          option.append(element('span',player.name),element('small',[player.position,team.abbreviation].filter(Boolean).join(' · ')));
          option.onclick=()=>toggle(gameId,player);
          group.append(option);
        }
        matchesCount+=matches.length;gameGroup.append(group);
      }
      if(gameGroup.childElementCount>1)list.append(gameGroup);
    }
    if(focused)[...list.querySelectorAll('.playerOption')].find(button=>button.dataset.playerKey===focused)?.focus({preventScroll:true});
    list.scrollTop=scroll;
    const loading=rosterStates.some(roster=>roster?.pending),failed=rosterStates.some(roster=>roster?.failed);
    $('rosterStatus').textContent=!games.length?'Add games to your rotation to find players.':[
      `${matchesCount} ${matchesCount===1?'player':'players'} found · ${tags.childElementCount} followed across your rotation`,
      loading?'Loading rosters…':failed?'Some rosters could not load. Use Retry rosters to try again.':''
    ].filter(Boolean).join('. ');
    $('retryRoster').disabled=!games.length;
  }
  function enqueuePlay(entry){
    if(!entry?.text||!allowed(entry))return false;
    const textFor=play=>{
      if(rotationEvents().filter(event=>gameState(event)==='in'&&canHear(event)).length<2)return play.text;
      const matchup=competitors(events.find(event=>String(event.id)===play.gameId)).map(teamLabel).filter(Boolean);
      return matchup.length===2?`${matchup.join(' and ')}. ${play.text}`:play.text;
    };
    return enqueueBrowserSpeech(entry.text,{
      getText:()=>textFor(tracker.history(entry.gameId).find(p=>p.key===entry.key)||entry),
      source:'play-by-play',key:`play-by-play:${entry.gameId}:${entry.key}`,...playVoiceOptions(entry,voiceSettings.style),
      shouldPlay:()=>allowed(entry),
      onStart:()=>{if(settings.duckRadio)setRadioDucked(true);status(`Speaking: ${entry.label}`);},
      onEnd:()=>{lastSpoken=new Date().toLocaleTimeString([],{hour:'numeric',minute:'2-digit'});setRadioDucked(false);status('Ready for the next play.');},
      onError:error=>{setRadioDucked(false);if(error!=='canceled')status(error==='speech-blocked'?'Voice needs a tap. Use Resume voice; your calls are still queued.':'Recovering voice; your calls remain queued.');},
      onSkip:()=>setRadioDucked(false)
    });
  }
  function playOnDemand(text,options){
    const queued=playBrowserSpeechNow(text,{
      ...options,
      onStart:()=>{if(settings.duckRadio)setRadioDucked(true);options.onStart?.();},
      onEnd:()=>{setRadioDucked(false);options.onEnd?.();},
      onError:error=>{setRadioDucked(false);if(error==='speech-blocked')status('Audio did not start. Tap Resume voice to retry.');}
    });
    if(!queued)status('This browser could not start speech playback.');
  }
  function replayPlay(entry){
    playOnDemand(entry.text,{
      source:'transcript-replay',key:replayKey(entry),forceDevice:true,
      ...playVoiceOptions(entry,voiceSettings.style),
      getText:()=>tracker.history(entry.gameId).find(play=>play.key===entry.key)?.text||entry.text,
      onStart:()=>status(`Replaying ${entry.label}`),
      onEnd:()=>{lastSpoken=new Date().toLocaleTimeString([],{hour:'numeric',minute:'2-digit'});status('Replay finished.');}
    });
  }
  function confirmVoice(){
    playOnDemand(settings.enabled?'Play calls are on.':'Voice is ready.',{
      source:'voice-activation',key:'voice-activation',forceDevice:true,preferLocal:true,
      onEnd:()=>status(settings.enabled?'Ready for the next matching play.':'Voice is ready.')
    });
  }
  function enqueueScores({manual=false}={}){
    const speech=buildScoreUpdateSpeech(events,{selectedIds:monitored(),scope:settings.scoreScope});
    if(!speech){status('No started games available for a score update.');return;}
    enqueueBrowserSpeech(speech,{source:'score-update',key:`score-update:${speech}`,rate:1.08,shouldPlay:()=>manual||settings.enabled,
      onStart:()=>{if(settings.duckRadio)setRadioDucked(true);},onEnd:()=>setRadioDucked(false),onError:()=>setRadioDucked(false)});
  }
  async function updateGame(event){
    const id=String(event.id),state=gameState(event),last=competition(event)?.situation?.lastPlay;
    if(gameRequests.has(id))return;
    if(state==='pre'){
      tracker.ingest(event,[],{complete:true});
      statsByGame.set(id,gamePlayerStats(null,cachePlayers(event,null)));
      return;
    }
    gameRequests.add(id);
    try{
      const old=summaries.get(id),lastKey=last?playIdentity(last):'';
      let snapshot=old,failed=false;
      if(!old||old.failed||old.key!==lastKey||Date.now()-old.at>30000){
        try{
          const data=await fetchJson(`${API}/summary?event=${encodeURIComponent(id)}`);
          if(!data||typeof data!=='object')throw new Error('Invalid ESPN summary');
          snapshot={data,key:lastKey,at:Date.now()};
        }catch{failed=true;snapshot=old?{...old,failed:true}:undefined;}
      }
      if(disposed||!monitored().includes(id))return;
      if(snapshot)summaries.set(id,snapshot);
      const summary=snapshot?.data;
      const complete=!failed&&(!!summary?.drives||Array.isArray(summary?.plays));
      const players=cachePlayers(event,summary);
      statsByGame.set(id,gamePlayerStats(summary,players));
      historyStatus.set(id,complete?'Latest 60 plays · updated from ESPN':'Latest play only · retrying ESPN play history.');
      const plays=playsFromSummary(summary,last);
      if(plays.length){
        const result=tracker.ingest(event,plays,{players,settings:settingsForGame(settings,id),complete});
        if(state==='in'||liveGames.has(id))for(const entry of result.announcements)enqueuePlay(entry);
      }
      if(state==='in')liveGames.add(id);
    }catch{
      if(!disposed&&monitored().includes(id))historyStatus.set(id,'Could not read play history · retrying ESPN.');
    }finally{
      gameRequests.delete(id);
      if(!disposed&&monitored().includes(id))renderLiveUpdates();
    }
  }
  async function poll(){
    if(polling||disposed||(!monitored().length&&!(settings.enabled&&settings.scoreIntervalMinutes&&settings.scoreScope==='all')))return;
    polling=true;
    try{
      const board=await fetchJson(SCOREBOARD_URL);if(disposed)return;
      if(!Array.isArray(board?.events))throw new Error('Invalid ESPN scoreboard');
      events=board.events;dataFailed=false;paintGameOptions();paintControls();
      $('liveDataStatus').textContent=`Updated ${new Date().toLocaleTimeString([],{hour:'numeric',minute:'2-digit'})}`;
      const targets=rotationEvents();
      loadRosters(targets);
      for(const event of targets)void updateGame(event);
      if(settings.enabled&&settings.scoreIntervalMinutes&&Date.now()>=nextScoreAt){enqueueScores();nextScoreAt=Date.now()+settings.scoreIntervalMinutes*60000;}
      paintHero();if($('playByPlayDialog').open)paintPlayers();
    }catch{dataFailed=true;$('liveDataStatus').textContent='Live data interrupted · retrying';status('ESPN is unavailable. Saved plays remain visible; live updates will retry.');paintHero();paintVoiceState();}
    finally{polling=false;}
  }
  const saveFilters=()=>{
    save();
    removeQueuedSpeech(item=>item.source==='play-by-play'&&!item.shouldPlay?.());
    paintControls();
  };
  const setEnabled=value=>{
    const wasEnabled=settings.enabled;
    settings.enabled=!!value;save();
    if(settings.enabled){
      const state=getBrowserSpeechQueueState();
      if(!wasEnabled||state.blocked||(!state.activated&&!state.starting))confirmVoice();
      nextScoreAt=Date.now()+settings.scoreIntervalMinutes*60000;void poll();
    }else cancelBrowserSpeech(item=>['play-by-play','score-update','voice-activation'].includes(item.source));
    paintControls();
  };
  const openSettings=section=>{
    paintVoiceList();paintGameOptions();paintListeningGames();loadRosters();paintPlayers();$('playByPlayDialog').showModal();
    if(section==='players'){$('playerSearch').focus({preventScroll:true});$('playerSelection').scrollIntoView({block:'start'});}
    if(section==='filters')$('listenFilterHeading').focus();
  };
  for(const [value,text] of Object.entries(PLAY_TYPES)){
    const choice=element('label',null),input=element('input',null,{type:'checkbox',value});
    input.onchange=()=>{settings.playTypes=[...$('playTypes').querySelectorAll('input:checked')].map(control=>control.value);saveFilters();};
    choice.append(input,document.createTextNode(text));$('playTypes').append(choice);
  }
  for(const button of document.querySelectorAll('[data-play-mode]'))button.onclick=()=>{
    settings.mode=button.dataset.playMode;
    if(settings.mode==='every')settings.playTypes=[];
    saveFilters();setEnabled(true);
    if(settings.mode==='players')openSettings('players');
  };
  $('fieldVoiceToggle').onclick=()=>setEnabled(!settings.enabled);
  $('announcementsToggle').onclick=()=>setEnabled(!settings.enabled);$('playByPlayEnabled').onchange=()=>setEnabled($('playByPlayEnabled').checked);
  $('defaultPlayMode').onchange=()=>{settings.mode=$('defaultPlayMode').value;saveFilters();};
  $('listenToFollowed').onclick=()=>{settings.mode='players';saveFilters();setEnabled(true);$('playerSearch').focus();};
  $('playByPlayButton').onclick=()=>openSettings();$('choosePlayers').onclick=()=>openSettings('players');$('editPlayFilters').onclick=()=>openSettings('filters');$('closePlayByPlay').onclick=()=>$('playByPlayDialog').close();
  $('playByPlayDuckRadio').onchange=()=>{settings.duckRadio=$('playByPlayDuckRadio').checked;save();const state=getBrowserSpeechQueueState();setRadioDucked(settings.duckRadio&&state.speaking&&state.current?.source!=='voice-command');};
  $('includeRadioGame').onchange=()=>{settings.includeRadioGame=$('includeRadioGame').checked;saveFilters();};
  $('playByPlayScoreInterval').onchange=()=>{settings.scoreIntervalMinutes=normalizeScoreInterval($('playByPlayScoreInterval').value);save();nextScoreAt=Date.now()+settings.scoreIntervalMinutes*60000;paintControls();};
  $('playByPlayScoreScope').onchange=()=>{settings.scoreScope=$('playByPlayScoreScope').value;save();};
  onNaturalVoiceStatus(message=>{$('naturalVoiceStatus').textContent=message;});
  $('voiceEngine').onchange=()=>{voiceSettings.engine=$('voiceEngine').value;saveVoiceSettings(voiceSettings);};
  $('naturalVoice').onchange=()=>{voiceSettings.naturalVoice=$('naturalVoice').value;saveVoiceSettings(voiceSettings);};
  $('loadNaturalVoice').onclick=async()=>{const button=$('loadNaturalVoice');button.disabled=true;try{await loadNaturalVoice();voiceSettings.engine='natural';voiceSettings.naturalVoice=$('naturalVoice').value;saveVoiceSettings(voiceSettings);paintControls();}catch(error){$('naturalVoiceStatus').textContent=error.message+' Device voice remains available.';}finally{button.disabled=false;}};
  $('voiceSelect').onchange=()=>{voiceSettings.voiceURI=$('voiceSelect').value;saveVoiceSettings(voiceSettings);};
  $('voiceStyle').onchange=()=>{voiceSettings.style=$('voiceStyle').value;saveVoiceSettings(voiceSettings);};
  $('previewVoice').onclick=()=>playOnDemand('Your football radio companion is ready.',{source:'voice-preview',key:'voice-preview',...playVoiceOptions({},voiceSettings.style)});
  $('speakLatestPlays').onclick=()=>{const latest=tracker.history(viewedGame).at(-1);if(latest)replayPlay(latest);else status('No plays are available to replay.');};
  $('speakScoresNow').onclick=()=>{unlockBrowserSpeech();enqueueScores({manual:true});};
  $('transcriptGame').onchange=()=>{viewedGame=$('transcriptGame').value;paintHero();};
  $('playerGame').onchange=()=>paintPlayers();$('playerPosition').onchange=()=>paintPlayers();$('playerSearch').oninput=()=>paintPlayers();
  $('retryRoster').onclick=()=>{
    const id=$('playerGame').value,games=rotationEvents().filter(event=>!id||String(event.id)===id);
    for(const event of games)for(const c of competitors(event)){const entry=rosters.get(String(c.team.id));if(entry&&!entry.pending)entry.expires=0;}
    loadRosters(games);paintPlayers();
  };
  const updates=$('gameUpdatesButton');if(updates)$('extraVoiceActions').append(updates);
  onBrowserSpeechQueueState(paintVoiceState);
  $('resumeVoice').onclick=()=>{if(getBrowserSpeechQueueState().queued)unlockBrowserSpeech();else confirmVoice();};
  globalThis.speechSynthesis?.addEventListener?.('voiceschanged',paintVoiceList);
  window.addEventListener('nfl-radio:scoreboard',e=>{if(!events.length){events=e.detail.events||[];paintGameOptions();paintHero();paintControls();}void poll();});
  window.addEventListener('nfl-radio:rotation',()=>{paintGameOptions();paintHero();paintControls();if($('playByPlayDialog').open)paintPlayers();void poll();});
  window.addEventListener('nfl-radio:broadcast',()=>{paintListeningGames();paintVoiceState();});
  window.addEventListener('nfl-radio:recaps',event=>{recapsEnabled=event.detail.enabled;paintVoiceState();});
  window.addEventListener('nfl-radio:inspect',e=>{viewedGame=String(e.detail.gameId);paintGameOptions();paintHero();statsView.open();});
  window.addEventListener('nfl-radio:selection',e=>{viewedGame=String(e.detail.gameId);paintGameOptions();paintHero();});
  window.addEventListener('storage',e=>{if(e.key===STORAGE_KEY){settings=readPlayByPlaySettings();paintControls();if($('playByPlayDialog').open)paintPlayers();}if(e.key===`${storagePrefix}:rotation`){paintGameOptions();paintHero();paintControls();void poll();}});
  const reconnect=()=>{if(settings.enabled||recapsEnabled)resumeBrowserSpeech();void poll();};
  document.addEventListener('visibilitychange',()=>{if(!document.hidden)reconnect();});
  window.addEventListener('online',reconnect);
  window.addEventListener('pageshow',event=>{if(event.persisted)reconnect();});
  const timer=setInterval(poll,PLAY_POLL_MS);
  window.addEventListener('pagehide',event=>{if(event.persisted)return;disposed=true;clearInterval(timer);setRadioDucked(false);});
  paintControls();paintVoiceList();paintGameOptions();paintHero();void poll();
}
if(typeof document!=='undefined')installLivePlayByPlay();
