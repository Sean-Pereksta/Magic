import {OnlineConnection} from './online-client.mjs';
import {OFFICIAL_ORIGIN,SITE_ORIGINS} from './online-rules.mjs';
import {MAP_BRIEFS,mapScreenshotUrl} from './skirmish-menu.js';
import {renderLeaderboard,renderRankProfile} from './rank-ui.mjs';

export function setupOnlineLobby(root,{selectedMap,onLoad,onStart,onState,onResult,onRating,onStatus,onClosed}) {
  const find=id=>root.querySelector('#'+id);
  const menu=find('mainMenu'), solo=find('soloLobby'), singleplayer=find('singleplayerOpen');
  const pane=find('onlineLobby'), status=find('onlineStatus'), form=find('onlineCreate'), rows=find('onlineRooms');
  const play=find('onlineStart'), roster=find('onlineRoster'), invite=find('onlineInvite'), rank=find('onlineRanked');
  const hostStatus=find('onlineHostStatus'), titleInput=find('onlineTitle');
  const current=new URL(location.href);
  const local=location.hostname==='localhost' || location.hostname==='127.0.0.1';
  const hosted=local || location.hostname.endsWith('.trycloudflare.com');
  const origin=hosted ? location.origin : OFFICIAL_ORIGIN;
  const fromSite=SITE_ORIGINS.includes(location.origin) && location.origin!==OFFICIAL_ORIGIN;
  let token=local ? new URLSearchParams(current.hash.slice(1)).get('host') || '' : '';
  if (local) try {
    if (token) sessionStorage.setItem('trash-tactics-host-token',token);
    else token=sessionStorage.getItem('trash-tactics-host-token') || '';
  } catch {}
  const inviteCode=current.searchParams.get('room')?.toUpperCase() || '';
  if (token) {current.hash='';history.replaceState(history.state,'',current);}
  const handle=find('onlineName');
  const launch=find('onlineOpen');
  const matchBox=find('onlineMatch');
  const mapSelect=find('onlineMap');
  for (const [id,brief] of Object.entries(MAP_BRIEFS)) mapSelect.add(new Option(brief.name,id));
  const mapPicker=document.createElement('div');
  const selectedMapImage=document.createElement('img');
  selectedMapImage.alt='';
  selectedMapImage.className='online-map-icon';
  mapPicker.className='online-map-picker';
  mapSelect.before(mapPicker);
  mapPicker.append(selectedMapImage,mapSelect);
  const showMapIcon=()=>{selectedMapImage.src=mapScreenshotUrl(mapSelect.value);};
  mapSelect.addEventListener('change',showMapIcon);
  let connection,room,health,poll,link,loading=0,connecting=false,running=false;
  try {handle.value=localStorage.getItem('trash-tactics-multiplayer-name') || '';} catch {}
  const note=message=>{status.textContent=message;};
  const button=(text,listener)=>{
    const node=document.createElement('button');
    node.className='secondary-button'; node.type='button'; node.textContent=text;
    node.addEventListener('click',listener);
    return node;
  };
  const text=(tag,value)=>{
    const el=document.createElement(tag);
    el.textContent=value;
    return el;
  };
  const sameService=target=>target===origin;
  const canRank=()=>fromSite && !!health?.ranked;
  async function request(path,base=origin) {
    const response=await fetch(new URL(path,base),{signal:AbortSignal.timeout(5000)});
    if (!response.ok) throw new Error('Service unavailable');
    return response.json();
  }
  function setControls() {
    form.hidden=!!room;
    rank.hidden=!canRank();
    matchBox.hidden=!room;
    rows.hidden=!!room;
    play.hidden=!room || connection?.seat?.team!==0 || room.phase!=='waiting';
    play.disabled=!room?.seats.every(p=>p?.connected);
    find('onlineBack').hidden=running;
  }
  function renderRoom(next) {
    room=next;
    hostStatus.textContent='';
    find('onlineRoomTitle').textContent=next.name;
    find('onlineRoomMap').textContent=(MAP_BRIEFS[next.map]?.name || next.map)+' · '+(next.ranked?'Ranked 1v1':'Casual 1v1');
    roster.replaceChildren();
    for (const [i,seat] of next.seats.entries()) {
      const node=text('li',seat?.profile ? '' : seat?.name || 'Open seat');
      if (seat?.profile) renderRankProfile(node,seat.profile);
      if (next.phase==='loading' && seat?.connected) node.append(text('small',seat.ready?'Ready':'Loading artwork…'));
      else node.append(text('small',seat ? seat.connected ? (i===0?'Host':'Challenger') : 'Reconnecting…' : 'Waiting for a challenger'));
      roster.append(node);
    }
    const url=next.url && new URL(next.url);
    if (url && (url.origin===OFFICIAL_ORIGIN && fromSite)) {
      const site=new URL(location.href);
      site.search='?room='+next.id; site.hash='';
      link=site.href;
    } else link=url?.href || new URL('/?room='+next.id,origin).href;
    invite.hidden=next.phase!=='waiting';
    find('onlineInviteLink').href=link;
    find('onlineInviteLink').textContent=link;
    setControls();
    if (next.phase==='waiting') note(next.seats[1]?'Both players are in the lobby.':'Share the invite or wait for a challenger.');
  }
  function renderRooms(list) {
    rows.replaceChildren();
    const sorted=[...list].sort((a,b)=>Number(b.id===inviteCode)-Number(a.id===inviteCode));
    for (const entry of sorted) {
      const item=text('li','');
      const mapDetails=text('div','');
      mapDetails.className='online-room-map';
      const screenshot=mapScreenshotUrl(entry.map);
      if(screenshot){
        const image=document.createElement('img');
        image.className='online-map-icon';
        image.alt='';
        image.loading='lazy';
        image.src=screenshot;
        mapDetails.append(image);
      }
      const desc=text('span','');
      desc.append(text('strong',entry.name),text('small',(MAP_BRIEFS[entry.map]?.name || entry.map)+' · '+(entry.ranked?'Ranked':entry.community?'Community':'Casual')+' · '+entry.count+'/'+entry.capacity));
      mapDetails.append(desc);
      const action=button('Join',()=>{
        if (entry.community && !hosted) {
          const url=new URL(entry.url);
          if (url.protocol==='https:' && /^[a-z0-9-]+\.trycloudflare\.com$/.test(url.hostname)) location.assign(url);
        } else void connect({type:'join',room:entry.id},entry.ranked);
      });
      item.append(mapDetails,action); rows.append(item);
    }
    if (!list.length) rows.append(text('li','No open lobbies. Host a lobby or join with an invite.'));
  }
  async function refresh() {
    if (pane.hidden) return;
    try {
      health=await request('/api/health');
      const data=await request('/api/lobbies');
      renderRooms(data.rooms);
      if (!room && !connecting && !hostStatus.textContent) {
        if (inviteCode && !data.rooms.some(entry=>entry.id===inviteCode)) note('This invite does not have an open seat.');
        else if (inviteCode) note('Your invitation is ready. Choose Join to take the open seat.');
        else note('Choose an open lobby or host your own.');
      }
      if (health.community && token) {
        find('onlineDirectory').textContent={
          listed:'Listed in the Catnmice lobby browser.',
          ready:'The public invite is ready. An open lobby will appear in the Catnmice lobby browser.',
          'invite-only':'The lobby directory is unreachable. You can still share the public invite.',
          offline:'Local host is ready. Waiting for a public tunnel.'
        }[health.directory] || '';
      }
    } catch {
      if (!room && !connecting && !hostStatus.textContent) note('The online service is unreachable. Solo skirmish is available.');
    }
    setControls();
  }
  async function connect(payload,ranked=false) {
    if (connecting || room) return;
    connecting=true;
    hostStatus.textContent='';
    const hosting=payload.type==='create' && !ranked;
    function reportError(message) {
      if (hosting && !room) {
        note('');
        hostStatus.textContent=message+' Try hosting from the Trash Tactics Local Edition.';
      } else note(message);
    }
    try {
      const name=handle.value.trim();
      try {localStorage.setItem('trash-tactics-multiplayer-name',name);} catch {}
      if (ranked) {
        if (!fromSite || !sameService(OFFICIAL_ORIGIN)) throw new Error('Open Ranked from the Catnmice Game Hub to use your profile.');
        const {getRankedToken}=await import('./ranked-auth.mjs');
        payload.token=await getRankedToken(origin);
      }
      const client=new OnlineConnection(origin);
      connection=client;
      const bind=(type,handler)=>client.addEventListener(type,event=>{
        if (connection===client) handler(event.detail);
      });
      bind('seat',()=>{connecting=false;});
      bind('room',renderRoom);
      bind('connection',({connected})=>{
        if (running) onStatus(connected?'':'Reconnecting to the host…');
        else if (!connected) note('Reconnecting to the host…');
      });
      bind('load',async data=>{
        const job=++loading;
        note('Loading battlefield artwork…');
        try {
          await onLoad({...data,team:client.seat.team,connection:client});
          if (loading===job && connection===client) client.send({type:'ready'});
        } catch {
          if (loading===job) client.send({type:'loadFailed'});
        }
      });
      bind('loadFailed',data=>{loading++;onClosed(data.message,false);note(data.message);});
      bind('start',data=>{
        if (!running) {
          running=true;
          onStart({...data,team:client.seat.team,connection:client,ranked:room?.ranked});
        }
        onStatus('');
      });
      bind('state',onState);
      bind('result',onResult);
      bind('rating',onRating);
      bind('ack',data=>{if (!data.ok) onStatus(data.reason,true);});
      bind('closed',data=>leave(data.message,false));
      bind('error',data=>{
        if (data.code==='SESSION_EXPIRED') {leave(data.message,false);return;}
        if (!room) {connecting=false;client.close(false);}
        reportError(data.message);
      });
      note('Connecting to the host…');
      client.open({...payload,name});
    } catch (error) {connecting=false;reportError(error.message);}
  }
  function show(panel,focusTarget) {
    for (const view of [menu,solo,pane]) view.hidden=view!==panel;
    focusTarget.focus();
  }
  function open(focusTarget=find('onlineHeading')) {
    mapSelect.value=selectedMap();
    showMapIcon();
    hostStatus.textContent='';
    setControls();
    show(pane,focusTarget);
    clearInterval(poll);
    poll=setInterval(()=>void refresh(),4000);
    void refresh();
  }
  function leave(message='',notify=true) {
    connection?.close(notify);connection=null;
    const wasRunning=running;
    room=null;running=false;connecting=false;loading++;
    setControls();
    onClosed(message,wasRunning);
    if (message) note(message);
    setTimeout(()=>void refresh(),3000);
  }
  singleplayer.addEventListener('click',()=>show(solo,find('soloHeading')));
  find('soloBack').addEventListener('click',()=>show(menu,singleplayer));
  launch.addEventListener('click',()=>open());
  find('onlineBack').addEventListener('click',()=>{
    if (room || connecting) leave();
    clearInterval(poll);
    show(menu,launch);
  });
  find('onlineLeave').addEventListener('click',()=>leave('You left the lobby.'));
  find('onlineRefresh').addEventListener('click',()=>void refresh());
  play.addEventListener('click',()=>connection?.send({type:'start'}));
  form.addEventListener('submit',event=>{
    event.preventDefault();
    const ranked=event.submitter===rank;
    if (ranked && !canRank()) return;
    void connect({type:'create',map:mapSelect.value,title:titleInput.value,ranked,hostToken:token},ranked);
  });
  find('onlineCopy').addEventListener('click',async()=>{
    try {await navigator.clipboard.writeText(link);note('Invite copied.');}
    catch {find('onlineInviteLink').focus();note('Copy the invite link above.');}
  });
  find('onlineStandings').addEventListener('toggle',async event=>{
    if (!event.currentTarget.open) return;
    const list=find('onlineLeaderboard');
    try {renderLeaderboard(list,(await request('/api/leaderboard',OFFICIAL_ORIGIN)).players);}
    catch {list.textContent='The Catnmice leaderboard is unavailable.';}
  });
  find('onlineProfileSearch').addEventListener('submit',async event=>{
    event.preventDefault();
    const card=find('onlineProfile');
    const username=find('onlineUsername').value.trim();
    try {renderRankProfile(card,(await request('/api/profile?username='+encodeURIComponent(username),OFFICIAL_ORIGIN)).player);}
    catch {card.textContent='No ranked profile found. Check the Catnmice username.';}
  });
  if (!inviteCode && current.searchParams.get('host')==='1') open(titleInput);
  else if (token || inviteCode || current.searchParams.has('online')) open();
  return {open,leave};
}
