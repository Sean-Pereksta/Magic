import {randomBytes,randomUUID} from 'node:crypto';
import {WebSocketServer} from 'ws';
import {Game} from './engine.js';
import {MAP_BRIEFS} from './skirmish-menu.js';
import {cleanName,executeOrder,LOAD_TIMEOUT_MS,ONLINE_VERSION,RECONNECT_MS,SITE_ORIGINS,SNAPSHOT_INTERVAL,STEP} from './online-rules.mjs';
import {snapshotFor} from './online-snapshot.mjs';

const key = () => randomBytes(24).toString('base64url');
const MAX_MATCH_SECONDS = 45*60;
const profileCard = ({displayName,username,rating,wins,losses,draws,games}) => ({displayName,username,rating,wins,losses,draws,games});

export function attachMultiplayer(server, {rankings=null,community=false,hostToken='',publicOrigin='',onRooms=()=>{}}={}) {
  const rooms = new Map();
  const wss = new WebSocketServer({noServer:true,maxPayload:16*1024,perMessageDeflate:false});
  let tunnelOrigin = publicOrigin;
  const send = (ws,data) => {
    if (ws?.readyState !== 1 || ws.bufferedAmount > 256*1024) return false;
    ws.send(JSON.stringify(data));
    return true;
  };
  const problem = (ws,message,code) => send(ws,{type:'error',message,code});
  const tell = (room,data) => room.seats.forEach(player => send(player?.ws,data));
  const metadata = room => ({
    id:room.code,name:room.name,map:room.map,ranked:room.ranked,
    count:room.seats.filter(Boolean).length,capacity:2,
    url:tunnelOrigin ? new URL('/?room='+room.code,tunnelOrigin).href : ''
  });
  const openRooms = () => [...rooms.values()]
    .filter(room => room.phase === 'waiting' && room.seats[0]?.ws && !room.seats[1])
    .map(metadata);
  function roomView(room) {
    tell(room,{
      type:'room',...metadata(room),phase:room.phase,
      seats:room.seats.map(p => p ? {name:p.name,connected:!!p.ws,ready:p.ready,profile:p.identity ? profileCard(p.identity) : null} : null)
    });
    onRooms();
  }
  function clearRoom(room, message) {
    if (!rooms.delete(room.code)) return;
    tell(room,{type:'closed',message});
    for (const player of room.seats) if (player?.ws) player.ws.player=null;
    onRooms();
  }
  function sendState(room,player,events) {
    if (!player?.ws) return;
    const state = snapshotFor(room.game,player,events);
    if (send(player.ws,state)) {
      if (state.fog) player.fogRevision = state.fog.revision;
      if (state.memory) player.memoryAt = state.time;
    }
  }
  async function finish(room,winner,reason,scoreable=true) {
    if (room.phase !== 'playing') return;
    room.phase='finished';
    room.deadline=Date.now()+RECONNECT_MS;
    room.game.winner=winner;
    room.game.matchComplete=true;
    const result = {
      type:'result',winner,reason,report:room.game.getMatchReport(),
      ratingStatus:room.ranked ? scoreable ? 'pending' : 'unrated' : 'casual'
    };
    room.result=result;
    room.seats.forEach(player => sendState(room,player,room.events));
    tell(room,result);
    if (!room.ranked || !scoreable) return;
    for (let attempt=1; attempt<=3; attempt++) {
      try {
        const rating = await rankings.recordResult({id:room.id,players:room.seats.map(player => player.identity),winnerTeam:winner});
        result.ratingStatus='saved';
        result.ratings=rating.players.map(({uid,...view}) => view);
        break;
      } catch (error) {
        if (attempt === 3) {
          console.error('Trash Tactics rating write failed',room.id,error.code || error.name);
          result.ratingStatus='failed';
        } else await new Promise(resolve => setTimeout(resolve,attempt*500));
      }
    }
    tell(room,{type:'rating',status:result.ratingStatus,ratings:result.ratings});
  }
  function beginLoading(room) {
    room.phase='loading';
    room.deadline=Date.now()+LOAD_TIMEOUT_MS;
    room.game=new Game({map:room.map,online:true,onEvent:event=>{
      if (event.type === 'hit') {
        const t=event.target;
        if (t) room.events.push({type:'hit',amount:event.amount,target:{id:t.id,team:t.team,x:t.x,y:t.y},team:t.team});
      } else if (event.type === 'toast' || event.type === 'deposit') room.events.push(event);
    }});
    roomView(room);
    tell(room,{type:'load',map:room.map});
  }
  function cancelLoading(room, message) {
    room.phase='waiting';
    room.game=null;
    room.deadline=Date.now()+30*60_000;
    room.commands.length=0;
    room.events.length=0;
    for (const player of room.seats) if (player) player.ready=false;
    tell(room,{type:'loadFailed',message});
    roomView(room);
  }
  function removePlayer(player, intentional=false) {
    const room=player.room;
    if (room.phase === 'playing') {
      if (intentional) {
        const opponent=room.seats[1-player.team];
        void finish(room,opponent?.ws ? opponent.team : null,'forfeit',!!opponent?.ws && room.game.time >= 90);
      }
      return;
    }
    if (room.phase === 'loading') cancelLoading(room,'A player left the loading screen.');
    if (player.team === 0 || room.phase === 'finished') {
      clearRoom(room,'The lobby closed.');
      return;
    }
    room.seats[player.team]=null;
    roomView(room);
  }
  function bind(ws,room,team,identity,name) {
    const player={ws,team,identity,name,room,session:key(),ready:false,lastSeq:0};
    room.seats[team]=player;
    ws.player=player;
    send(ws,{type:'seat',room:room.code,team,session:player.session});
    roomView(room);
  }
  async function join(ws,data) {
    const creating=data.type === 'create';
    const requestedId=typeof data.room === 'string' ? data.room.toUpperCase() : '';
    const existing=rooms.get(requestedId);
    const ranked=creating ? data.ranked === true : existing?.ranked;
    if (creating && community && (!hostToken || data.hostToken !== hostToken)) return problem(ws,'Open the host window from the launcher to create a lobby.');
    if (creating && !Object.hasOwn(MAP_BRIEFS,data.map)) return problem(ws,'Choose a battleground.');
    if (!creating && (!existing || existing.phase !== 'waiting' || existing.seats[1])) return problem(ws,'This lobby no longer has an open seat.');
    if (ranked && !rankings) return problem(ws,'Ranked PvP is currently unavailable.');
    let identity=null;
    if (ranked) {
      if (typeof data.token !== 'string' || data.token.length > 8192) return problem(ws,'Sign in to your linked Catnmice account to play Ranked.','ACCOUNT_NOT_LINKED');
      try { identity=await rankings.identity(data.token); }
      catch (error) { return problem(ws,error.message,error.code); }
    }
    if (ws.readyState !== 1 || ws.player) return;
    const name=identity?.displayName || cleanName(data.name) || 'Guest';
    if (creating) {
      if (rooms.size >= (community ? 1 : 20)) return problem(ws,'This host is full. Try another lobby.');
      let code;
      do { code=randomBytes(5).toString('hex').toUpperCase(); } while (rooms.has(code));
      const room={
        id:randomUUID(),code,map:data.map,name:cleanName(data.title) || name+"'s lobby",ranked,
        phase:'waiting',seats:[null,null],commands:[],events:[],deadline:Date.now()+30*60_000,
        nextSnapshot:0
      };
      rooms.set(code,room);
      bind(ws,room,0,identity,name);
      return;
    }
    if (rooms.get(requestedId)!==existing || existing.phase!=='waiting' || existing.seats[1]) return problem(ws,'This lobby no longer has an open seat.');
    if (identity && identity.uid === existing.seats[0]?.identity?.uid) return problem(ws,'Ranked matches require two different Catnmice accounts.');
    bind(ws,existing,1,identity,name);
  }
  async function receive(ws,data) {
    if (!data || typeof data !== 'object') return;
    const player=ws.player;
    if (!player) {
      if (data.type === 'create' || data.type === 'join') return join(ws,data);
      if (data.type === 'resume') {
        const room=rooms.get(data.room);
        const saved=room?.seats.find(p => p && p.session === data.session);
        if (!saved || (saved.disconnectUntil && Date.now()>saved.disconnectUntil)) return problem(ws,'The reconnect window has expired.','SESSION_EXPIRED');
        if (saved.ws) { saved.ws.player=null; saved.ws.close(1000); }
        saved.ws=ws; saved.disconnectUntil=null; saved.fogRevision=undefined; saved.memoryAt=undefined;
        ws.player=saved;
        send(ws,{type:'seat',room:room.code,team:saved.team,session:saved.session});
        roomView(room);
        if (room.phase === 'loading') send(ws,{type:'load',map:room.map});
        if (room.phase === 'playing') {
          send(ws,{type:'start',map:room.map});
          sendState(room,saved,[]);
        }
        if (room.phase === 'finished') send(ws,room.result);
      }
      return;
    }
    const room=player.room;
    switch (data.type) {
      case 'start':
        if (player.team===0 && room.phase==='waiting' && room.seats.every(seat => seat?.ws)) beginLoading(room);
        break;
      case 'ready':
        if (room.phase !== 'loading') break;
        player.ready=true;
        roomView(room);
        if (room.seats.every(seat => seat?.ws && seat.ready)) {
          room.phase='playing';
          roomView(room);
          tell(room,{type:'start',map:room.map});
          room.seats.forEach(seat => sendState(room,seat,[]));
        }
        break;
      case 'loadFailed':
        if (room.phase==='loading') cancelLoading(room,'A player could not load this battlefield. Try again.');
        break;
      case 'leave':
        removePlayer(player,true);
        player.ws=null; ws.player=null; player.disconnectUntil=Date.now();
        break;
      case 'order':
        if (room.phase==='playing' && room.commands.length < 80 && Number.isSafeInteger(data.seq) && data.seq>player.lastSeq) {
          player.lastSeq=data.seq;
          room.commands.push({player,data});
        }
        break;
    }
  }
  wss.on('connection',ws=>{
    ws.lastPing=Date.now(); ws.packets=0; ws.burst=ws.lastPing;
    ws.on('pong',()=>{ws.lastPing=Date.now();});
    send(ws,{type:'hello',version:ONLINE_VERSION,community,ranked:!!rankings});
    ws.on('error',()=>{});
    ws.on('message',(raw,binary)=>{
      const now=Date.now();
      if (now-ws.burst > 1000) { ws.burst=now; ws.packets=0; }
      if (binary || ++ws.packets>70) { ws.close(1008,'Message limit'); return; }
      let data;
      try { data=JSON.parse(raw); } catch { return; }
      if (ws.processing) return;
      const joining=data?.type==='create' || data?.type==='join';
      if (joining) ws.processing=true;
      receive(ws,data).catch(()=>problem(ws,'The host could not process this request.')).finally(()=>{
        if (joining) ws.processing=false;
      });
    });
    ws.on('close',()=>{
      const player=ws.player;
      if (!player || player.ws !== ws) return;
      player.ws=null;
      player.disconnectUntil=Date.now()+RECONNECT_MS;
      roomView(player.room);
    });
  });
  function upgrade(req,socket,head) {
    if (req.url !== '/api/play' || wss.clients.size>=80) { socket.destroy(); return; }
    const origin=req.headers.origin;
    const host=req.headers.host;
    const sameOrigin=origin === 'http://'+host || origin === 'https://'+host;
    if (origin && !sameOrigin && (community || !SITE_ORIGINS.includes(origin))) {
      socket.end('HTTP/1.1 403 Forbidden\r\nConnection: close\r\n\r\n'); return;
    }
    wss.handleUpgrade(req,socket,head,ws=>wss.emit('connection',ws,req));
  }
  server.on('upgrade',upgrade);

  function simulate(room) {
    const game=room.game;
    for (const {player,data} of room.commands.splice(0)) {
      let result;
      try { result=executeOrder(game,player.team,data); }
      catch { result={ok:false,reason:'That command could not be completed.'}; }
      send(player.ws,{type:'ack',seq:data.seq,...result});
    }
    game.update(STEP);
    if (game.winner !== null) { void finish(room,game.winner,'desks'); return; }
    if (game.teams.every(team=>team.eliminated) || game.time>=MAX_MATCH_SECONDS) { void finish(room,null,'draw'); return; }
    if (game.time+1e-6 < room.nextSnapshot) return;
    room.nextSnapshot=game.time+SNAPSHOT_INTERVAL;
    for (const player of room.seats) sendState(room,player,room.events);
    room.events.length=0;
  }
  let last=performance.now(), debt=0, pingAt=0;
  const timer=setInterval(()=>{
    const time=performance.now(), now=Date.now();
    debt=Math.min(STEP*3,debt+(time-last)/1000);
    last=time;
    if (now>=pingAt) {
      pingAt=now+10_000;
      for (const ws of wss.clients) if (now-ws.lastPing>25_000) ws.terminate(); else ws.ping();
    }
    for (const room of rooms.values()) {
      if (room.phase==='loading' && now>=room.deadline) cancelLoading(room,'The battlefield took too long to load.');
      if (['waiting','finished'].includes(room.phase) && now>=room.deadline) { clearRoom(room,'The lobby expired.'); continue; }
      const expired=room.seats.filter(p => p?.disconnectUntil && now>=p.disconnectUntil);
      for (const player of expired) removePlayer(player,true);
    }
    while (debt>=STEP) {
      debt-=STEP;
      for (const room of rooms.values()) if (room.phase==='playing') simulate(room);
    }
  },16);
  timer.unref();
  return {
    openRooms,
    setPublicOrigin(origin) { tunnelOrigin=origin; for (const room of rooms.values()) roomView(room); },
    stop() {
      clearInterval(timer);
      server.off('upgrade',upgrade);
      for (const ws of wss.clients) ws.terminate();
      wss.close();
      rooms.clear();
    }
  };
}
