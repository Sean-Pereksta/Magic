import {initializeApp, getApps} from 'https://www.gstatic.com/firebasejs/10.7.1/firebase-app.js';
import {getAuth, signInAnonymously, onAuthStateChanged} from 'https://www.gstatic.com/firebasejs/10.7.1/firebase-auth.js';
import {getFirestore, doc, collection, getDocFromServer, setDoc, onSnapshot, runTransaction, serverTimestamp, writeBatch} from 'https://www.gstatic.com/firebasejs/10.7.1/firebase-firestore.js';
import {RULES, Maze, finite, stampMs, runKey, fresh, living, distance, trapWindow, usableTrap, placementError, chooseSpawn, energy, stepEnemies, LatestWriter} from './core.mjs';

const config={apiKey:'AIzaSyB7twY7z31ucB6pGA8JC_HrVMZhA8lNaJA',authDomain:'bible-game-246c0.firebaseapp.com',projectId:'bible-game-246c0',storageBucket:'bible-game-246c0.appspot.com',messagingSenderId:'959619818996',appId:'1:959619818996:web:5a9fbf492e23c765e445a1'};
const uuid=()=>crypto.randomUUID();
const fail=(message,code='failed-precondition')=>Object.assign(new Error(message),{code});
async function restoredAuth(auth) {
  await new Promise((resolve,reject)=>{
    let off=()=>{};
    const timer=setTimeout(()=>{off();reject(fail('Sign-in timed out. Check your connection and retry.'));},12000);
    off=onAuthStateChanged(auth,()=>{clearTimeout(timer);off();resolve();},e=>{clearTimeout(timer);off();reject(e);});
  });
  return auth.currentUser||(await signInAnonymously(auth)).user;
}
export class RelicSession {
  constructor({gameId,name,dm=false}) {
    if(!gameId||gameId.includes('/'))throw fail('Open Stoney’s Relic from a valid multiplayer lobby.');
    this.gameId=gameId;this.name=String(name||'Player').slice(0,22);this.dmView=dm;
    this.sessionId=uuid();this.players=new Map();this.traps=new Map();this.listeners=new Set();this.off=[];
    this.s=null;this.maze=null;this.runtime={};this.enemies={};this.offset=0;this.ready=new Set();
    this.errors=new Map();this.actions=new Map();this.closed=false;this.pose=null;this.seq=0;this.lastHouse=0;this.lastPublish=0;this.lastClaim=0;
    this.lastSim=performance.now();this.lastHeartbeat=0;this.lastClockSeq=-1;this.hostEpoch=-1;
    this.hiddenAt=document.hidden?Date.now():0;this.visibility=()=>{this.hiddenAt=document.hidden?this.now():0;this.offerPosition();this.writer?.flush(true);this.emit();};
    this.connectivity=()=>{this.offerPosition();if(navigator.onLine)this.writer?.flush(true);this.emit();};
  }
  now() { return Date.now()+this.offset; }
  get self() { return this.players.get(this.uid); }
  get amDM() { return this.dmView&&this.s?.dmUid===this.uid; }
  get synchronized() { return navigator.onLine&&!this.closed&&this.ready.size===4&&!this.errors.size; }
  get host() {
    return this.synchronized&&!document.hidden&&this.runtime.runId===runKey(this.s)&&this.runtime.hostId===this.uid&&
      this.runtime.hostSession===this.sessionId&&finite(this.runtime.hostUntil)>this.now();
  }
  get status() {
    if(this.errors.size)return [...this.errors.values()][0];
    if(!navigator.onLine)return 'Offline — reconnecting';
    if(!this.synchronized)return 'Syncing with Firebase…';
    if(this.writer?.failures)return 'Retrying position sync…';
    if(this.writer?.active&&this.now()-this.writer.lastAttempt>3000)return 'Waiting for Firebase acknowledgement…';
    if(this.s?.phase==='play'&&finite(this.runtime.hostUntil)<=this.now())return 'Reconnecting enemy host…';
    return 'Connected';
  }
  subscribe(fn) { this.listeners.add(fn);return()=>this.listeners.delete(fn); }
  emit() { for(const fn of this.listeners)fn(this); }
  report(where,error) {
    console.error(`[stoney:${where}]`,error);
    if(['permission-denied','unauthenticated','auth/operation-not-allowed'].includes(error.code))this.errors.set(where,`${error.code}: ${error.message}`);
    this.notice=`${where}: ${error.message||'Connection interrupted; retrying.'}`;this.emit();
  }
  async start() {
    const app=getApps().find(a=>a.name==='[DEFAULT]')||initializeApp(config);this.db=getFirestore(app);
    this.uid=(await restoredAuth(getAuth(app))).uid;
    this.lobbyRef=doc(this.db,'lobbies',this.gameId);this.playersRef=collection(this.lobbyRef,'players');this.trapsRef=collection(this.lobbyRef,'traps');
    this.myRef=doc(this.playersRef,this.uid);this.runtimeRef=doc(this.lobbyRef,'stoneyRuntime','state');
    const lobby=await getDocFromServer(this.lobbyRef);
    if(!lobby.exists())throw fail('This lobby no longer exists. Return to the lobby list.');
    // One server-clock calibration per join; never estimate time from an old cached snapshot.
    const sent=Date.now();await setDoc(this.myRef,{clockAt:serverTimestamp()},{merge:true});
    const clock=await getDocFromServer(this.myRef);this.offset=stampMs(clock.data().clockAt)-(sent+Date.now())/2;
    this.hiddenAt=document.hidden?this.now():0;
    if(this.dmView)await this.initDM();
    await this.join();
    this.writer=new LatestWriter(value=>this.writePosition(value),{now:()=>this.now(),onError:e=>this.report('Position',e)});
    this.listen();
    document.addEventListener('visibilitychange',this.visibility);window.addEventListener('online',this.connectivity);window.addEventListener('offline',this.connectivity);
    this.timer=setInterval(()=>this.tick(),50);this.offerPosition();this.emit();return this;
  }
  async initDM() {
    const seed=Math.floor(Math.random()*2000000000),id=uuid();
    await runTransaction(this.db,async tx=>{
      const snap=await tx.get(this.lobbyRef);if(!snap.exists())throw fail('Lobby missing.');
      const d=snap.data(),s=d.stoney;
      if((d.dm&&d.dm!==this.name)||(s?.dmUid&&s.dmUid!==this.uid))return; // viewer, not a second DM
      if(!s) {
        tx.update(this.lobbyRef,{dm:d.dm||this.name,stoney:{seed,w:21,h:21,cellSize:4,runId:id,dmUid:this.uid,phase:'setup',setupDeadline:this.now()+30000,
          crown:null,carrierId:null,carrierName:null,dmEnergyBase:0,dmEnergyStamp:this.now(),startAt:null,endAt:null,winner:null,winnerName:null,
          spawnRevision:0,lastEvent:'Dungeon Master is preparing the dungeon…',lastEventAt:this.now()}});
      } else {
        // update(), not set({"stoney.key":...}), so dotted names become real nested fields.
        const patch={'stoney.dmUid':this.uid};
        if(!s.runId)patch['stoney.runId']=runKey(s);
        if(!Number.isFinite(s.spawnRevision))patch['stoney.spawnRevision']=0;
        tx.update(this.lobbyRef,patch);
      }
    });
  }
  async join() {
    const result=await runTransaction(this.db,async tx=>{
      const [lobby,player]=await Promise.all([tx.get(this.lobbyRef),tx.get(this.myRef)]);
      if(!lobby.exists())throw fail('Lobby missing.');
      const s=lobby.data().stoney||null,old=player.data()||{},key=runKey(s),maze=s?new Maze(s):null;
      const resume=maze&&(!old.runId||old.runId===key)&&Number.isFinite(old.x)&&Number.isFinite(old.z)&&!maze.blocked(old.x,old.z);
      const spawn=resume?old:maze?.spawn(this.uid)||{x:0,z:0};
      const state={uid:this.uid,name:this.name,role:this.dmView?(s?.dmUid===this.uid?'dm':'viewer'):'player',sessionId:this.sessionId,runId:key,
        x:spawn.x,z:spawn.z,yaw:resume?finite(old.yaw):0,vx:0,vz:0,ready:!!maze,
        alive:resume?old.alive!==false:true,deadUntil:resume?finite(old.deadUntil):0,
        protectedUntil:resume?finite(old.protectedUntil):this.now()+RULES.protectionMs,
        spawnSeq:finite(old.spawnSeq)+(resume?0:1),seq:finite(old.seq)+1,online:true,hidden:document.hidden,
        hiddenAt:document.hidden?this.now():0,updatedAt:this.now(),sentLocal:Date.now(),serverAt:serverTimestamp()};
      tx.set(this.myRef,state,{merge:true});return {s,state};
    });
    this.s=result.s;this.maze=this.s?new Maze(this.s):null;this.pose={...result.state};this.seq=result.state.seq;
  }
  listen() {
    const watch=(ref,key,fn)=>{
      this.off.push(onSnapshot(ref,{includeMetadataChanges:true},snap=>{
        if(snap.metadata.fromCache)this.ready.delete(key);else this.ready.add(key);
        // Gameplay ownership and results are acknowledged server state, not speculative local writes.
        if(!snap.metadata.hasPendingWrites){try{this.errors.delete(key);fn(snap);}catch(error){this.ready.delete(key);this.errors.set(key,`${key}: ${error.message}`);this.report(key,error);}}
        this.emit();
      },error=>{this.ready.delete(key);this.errors.set(key,`${key} sync failed: ${error.message}`);this.report(key,error);}));
    };
    watch(this.lobbyRef,'lobby',snap=>{
      if(!snap.exists()){this.errors.set('lobby','Lobby closed. Return to the lobby list.');return;}
      const old=this.s,s=snap.data().stoney||null;
      if(s&&(!old||`${old.seed}:${old.w}:${old.h}:${old.cellSize}`!==`${s.seed}:${s.w}:${s.h}:${s.cellSize}`))this.maze=new Maze(s);
      this.s=s;
      if(s&&this.pose?.runId!==runKey(s))void this.action('new-round',()=>this.join());
    });
    watch(this.playersRef,'players',snap=>{
      for(const change of snap.docChanges({includeMetadataChanges:true})) {
        const id=change.doc.id;if(change.type==='removed'){this.players.delete(id);continue;}
        const p=change.doc.data();this.players.set(id,{...p,uid:id,name:String(p.name||'Player').slice(0,22)});
        if(id===this.uid) {
          if(p.sessionId&&p.sessionId!==this.sessionId){this.errors.set('session','This player is active in another tab. Close that tab and reconnect here.');continue;}
          if(p.spawnSeq!==this.pose?.spawnSeq||p.runId!==this.pose?.runId)this.pose={...p};
          if(p.seq>this.lastClockSeq&&p.sentLocal&&Date.now()-p.sentLocal<5000&&stampMs(p.serverAt)) {
            this.lastClockSeq=p.seq;const estimate=stampMs(p.serverAt)-(p.sentLocal+Date.now())/2;
            this.offset=this.offset*.9+estimate*.1;
          }
        }
      }
    });
    watch(this.trapsRef,'traps',snap=>{
      for(const change of snap.docChanges()) {
        if(change.type==='removed')this.traps.delete(change.doc.id);
        else {const t=change.doc.data();if(Object.hasOwn(RULES.cost,t.type))this.traps.set(change.doc.id,{...t,id:change.doc.id});}
      }
    });
    watch(this.runtimeRef,'runtime',snap=>{
      const r=snap.data()||{};
      if(this.runtime.runId===r.runId&&finite(r.seq)<finite(this.runtime.seq))return;
      const changedHost=this.runtime.hostEpoch!==r.hostEpoch||this.runtime.runId!==r.runId;
      this.runtime=r;
      if(changedHost||!this.host)this.enemies=r.runId===runKey(this.s)?{...(r.enemies||{})}:{};
    });
  }
  setPosition(p) { this.pose={...this.pose,...p}; }
  offerPosition() {
    if(!this.writer||!this.pose)return;
    this.writer.offer({...this.pose,alive:this.self?.alive!==false,deadUntil:finite(this.self?.deadUntil),hidden:document.hidden,hiddenAt:this.hiddenAt,
      ready:!!this.maze,runId:runKey(this.s),sessionId:this.sessionId,online:true});
  }
  async writePosition(value) {
    if(!navigator.onLine||this.closed)throw fail('Offline — waiting to reconnect.','unavailable');
    const seq=++this.seq,local=Date.now();
    await runTransaction(this.db,async tx=>{
      const snap=await tx.get(this.myRef),p=snap.data();
      if(!p||p.sessionId!==this.sessionId)throw fail('Player session changed. Reconnect this tab.');
      if(p.runId!==value.runId)throw fail('Waiting for the current round.');
      const patch={online:true,hidden:value.hidden,hiddenAt:value.hiddenAt,seq,updatedAt:this.now(),sentLocal:local,serverAt:serverTimestamp()};
      // Movement cannot resurrect a player or overwrite a newer respawn / another tab's position.
      if(p.alive!==false&&p.spawnSeq===value.spawnSeq)Object.assign(patch,{x:value.x,z:value.z,yaw:value.yaw,vx:finite(value.vx),vz:finite(value.vz)});
      tx.update(this.myRef,patch);
    });
  }
  async flushPosition() {
    this.offerPosition();if(this.writer.active)await this.writer.active;
    return this.writer.flush(true);
  }
  async action(key,fn,cooldown=600) {
    const a=this.actions.get(key)||{busy:false,next:0,failures:0};
    if(a.busy||this.now()<a.next||this.closed)return false;
    a.busy=true;this.actions.set(key,a);
    try { const result=await fn();a.failures=0;a.next=this.now()+cooldown;return result??true; }
    catch(error){a.next=this.now()+Math.min(16000,750*2**Math.min(4,a.failures++));this.report(key,error);return false;}
    finally {a.busy=false;this.emit();}
  }
  async playerAction(kind,detail={}) {
    if(!this.synchronized||this.dmView||!this.s||!this.pose)return false;
    return this.action(kind,async()=>{
      if(!await this.flushPosition())return false;
      const position={x:this.pose.x,z:this.pose.z},key=runKey(this.s);
      await runTransaction(this.db,async tx=>{
        const [ls,ps]=await Promise.all([tx.get(this.lobbyRef),tx.get(this.myRef)]),s=ls.data()?.stoney,p=ps.data();
        if(!s||!p||runKey(s)!==key||p.runId!==key||p.sessionId!==this.sessionId||s.phase!=='play')return;
        const now=this.now(),maze=this.maze;
        if(kind==='respawn') {
          if(p.alive!==false||now<finite(p.deadUntil))return;
          const spawn=maze.spawn(this.uid);
          tx.update(this.myRef,{...spawn,yaw:0,vx:0,vz:0,alive:true,deadUntil:0,protectedUntil:now+RULES.protectionMs,spawnSeq:finite(p.spawnSeq)+1,updatedAt:now,serverAt:serverTimestamp()});return;
        }
        if(p.alive===false||finite(p.deadUntil)>now||now>=s.endAt)return;
        if(kind==='pickup') {
          if(s.carrierId||!s.crown||distance(position,s.crown)>1.6||distance(p,s.crown)>1.8||!maze.clear(position,s.crown))return;
          tx.update(this.lobbyRef,{'stoney.carrierId':this.uid,'stoney.carrierName':this.name,'stoney.lastEvent':`👑 ${this.name} has the crown!`,'stoney.lastEventAt':now});
        } else if(kind==='escape') {
          if(s.carrierId!==this.uid||s.winner||!maze.inRoom(position)||!maze.inRoom(p))return;
          tx.update(this.lobbyRef,{'stoney.phase':'ended','stoney.winner':'players','stoney.winnerId':this.uid,'stoney.winnerName':this.name,'stoney.lastEvent':`🏆 ${this.name} escaped with the crown! Players win!`,'stoney.lastEventAt':now});
        } else if(kind==='caught') {
          if(finite(p.protectedUntil)>now)return;
          const [ts,rs]=await Promise.all([tx.get(doc(this.trapsRef,detail.id)),tx.get(this.runtimeRef)]);
          const t=ts.data(),r=rs.data();
          if(!t||!usableTrap(t,s,now)||now<trapWindow(t,s).armedAt||t.type==='fog')return;
          const e=t.type==='fire'?t:r?.runId===key&&now-finite(r.frameAt)<1800?r.enemies?.[detail.id]:null;
          // Small tolerance covers the capped render interpolation, not arbitrary remote hits.
          if(!e||distance(p,e)>(t.type==='fire'?1.85:1.8)||!maze.clear(p,e))return;
          tx.update(this.myRef,{alive:false,deadUntil:now+RULES.respawnMs,vx:0,vz:0,updatedAt:now,serverAt:serverTimestamp()});
          if(s.carrierId===this.uid)tx.update(this.lobbyRef,{'stoney.carrierId':null,'stoney.carrierName':null,'stoney.crown':position,'stoney.lastEvent':`💥 ${this.name} dropped the crown (${t.type}).`,'stoney.lastEventAt':now});
        }
      });
      return true;
    });
  }
  async startRound() {
    if(!this.amDM||!this.synchronized)return false;
    return this.action('Start round',()=>runTransaction(this.db,async tx=>{
      const snap=await tx.get(this.lobbyRef),s=snap.data()?.stoney;
      if(!s||s.dmUid!==this.uid||s.phase!=='setup')return;
      this.writeStart(tx,s);
    }));
  }
  writeStart(tx,s) {
    const now=this.now(),maze=new Maze(s),crown=s.crown||chooseSpawn('crown',s,maze,this.players,this.traps,now)||maze.center(0);
    tx.update(this.lobbyRef,{'stoney.phase':'play','stoney.crown':crown,'stoney.carrierId':null,'stoney.carrierName':null,'stoney.startAt':now,'stoney.endAt':now+RULES.roundMs,'stoney.lastEvent':'The dungeon is open — find the crown!','stoney.lastEventAt':now});
  }
  async place(type,point) {
    if(!this.amDM||!this.synchronized||!this.maze)return false;
    const error=placementError(type,point,this.s,this.maze,this.players,this.traps,this.now());
    if(error){this.notice=error;this.emit();return false;}
    const trapRef=doc(this.trapsRef),key=runKey(this.s),revision=finite(this.s.spawnRevision);
    const playerIds=[...this.players.keys()],trapIds=[...this.traps.keys()];
    return this.action('Placement',()=>runTransaction(this.db,async tx=>{
      const [ls,existing,...docs]=await Promise.all([tx.get(this.lobbyRef),tx.get(trapRef),...playerIds.map(id=>tx.get(doc(this.playersRef,id))),...trapIds.map(id=>tx.get(doc(this.trapsRef,id)))]);
      const s=ls.data()?.stoney;if(existing.exists())return; // retries reuse one ID
      if(!s||s.dmUid!==this.uid||runKey(s)!==key)throw fail('The dungeon changed. Try again.');
      if(finite(s.spawnRevision)!==revision)throw fail('Another placement just changed the dungeon. Try again.');
      const players=new Map(),traps=new Map();
      playerIds.forEach((id,i)=>{if(docs[i].exists())players.set(id,docs[i].data());});
      trapIds.forEach((id,i)=>{if(docs[playerIds.length+i].exists())traps.set(id,docs[playerIds.length+i].data());});
      const now=this.now(),error=placementError(type,point,s,this.maze,players,traps,now);if(error)throw fail(error);
      if(type==='crown') {
        tx.update(this.lobbyRef,{'stoney.crown':point,'stoney.carrierId':null,'stoney.carrierName':null,'stoney.spawnRevision':revision+1,'stoney.lastEvent':'👑 The crown has been placed.','stoney.lastEventAt':now});return;
      }
      const available=energy(s,now),cost=RULES.cost[type];if(available<cost)throw fail(`Not enough energy (${Math.floor(available)}/${cost}).`);
      // Energy and the spawn succeed together, or neither does. Type is captured before awaits.
      tx.update(this.lobbyRef,{'stoney.dmEnergyBase':available-cost,'stoney.dmEnergyStamp':now,'stoney.spawnRevision':revision+1});
      tx.set(trapRef,{type,x:point.x,z:point.z,runId:key,placedBy:this.name,placedAt:now,armedAt:s.phase==='play'?now+RULES.spawnWarningMs:0,ttlMs:RULES.ttl[type]});
    }),300);
  }
  tick() {
    if(this.closed||this.s?.phase==='ended')return;
    const now=this.now(),perf=performance.now(),dt=Math.min(.1,Math.max(0,(perf-this.lastSim)/1000));this.lastSim=perf;
    if(now-this.lastHeartbeat>=250){this.lastHeartbeat=now;this.offerPosition();if(this.synchronized)void this.writer.flush();}
    if(!this.synchronized||!this.s||document.hidden)return;
    if(!this.host&&now-this.lastClaim>=1800) {
      this.lastClaim=now;
      const candidates=[...this.players.values()].filter(p=>p.role!=='viewer'&&fresh(p,now)&&!p.hidden&&p.ready&&p.runId===runKey(this.s));
      candidates.sort((a,b)=>(a.role==='dm'?-1:0)-(b.role==='dm'?-1:0)||a.uid.localeCompare(b.uid));
      if(candidates[0]?.uid===this.uid&&candidates[0]?.sessionId===this.sessionId)void this.action('Host handoff',()=>this.claimHost());
    }
    if(this.host) {
      if(this.s.phase==='play') {
        const roster=new Map(this.players);if(this.self&&!this.dmView)roster.set(this.uid,{...this.self,...this.pose,updatedAt:now,serverAt:now});
        this.enemies=stepEnemies(this.maze,this.s,this.traps,roster,this.enemies,dt,now);
      }
      const cadence=Object.keys(this.enemies).length&&this.s.phase==='play'?350:2000;
      if(now-this.lastPublish>=cadence){this.lastPublish=now;void this.action('Enemy sync',()=>this.publish(),0);}
      if(now-this.lastHouse>=1500){this.lastHouse=now;void this.action('Round sync',()=>this.maintain(),0);}
    }
  }
  async claimHost() {
    const key=runKey(this.s);
    await runTransaction(this.db,async tx=>{
      const [rs,ls,ps]=await Promise.all([tx.get(this.runtimeRef),tx.get(this.lobbyRef),tx.get(this.myRef)]),r=rs.data()||{},s=ls.data()?.stoney,p=ps.data(),now=this.now();
      if(!s||runKey(s)!==key||s.phase==='ended'||!p||p.role==='viewer'||p.sessionId!==this.sessionId||document.hidden)return;
      if(r.runId===key&&finite(r.hostUntil)>now)return;
      tx.set(this.runtimeRef,{runId:key,hostId:this.uid,hostSession:this.sessionId,hostEpoch:finite(r.hostEpoch)+1,hostUntil:now+RULES.leaseMs,
        seq:finite(r.seq)+1,enemies:r.runId===key?r.enemies||{}:{},frameAt:now,serverAt:serverTimestamp()});
    });
  }
  async publish() {
    const key=runKey(this.s),epoch=this.runtime.hostEpoch,enemies=structuredClone(this.enemies),now=this.now();
    await runTransaction(this.db,async tx=>{
      const snap=await tx.get(this.runtimeRef),r=snap.data();
      if(!r||r.runId!==key||r.hostId!==this.uid||r.hostSession!==this.sessionId||r.hostEpoch!==epoch||r.hostUntil<=this.now()||document.hidden)return;
      tx.update(this.runtimeRef,{enemies,frameAt:now,seq:finite(r.seq)+1,hostUntil:this.now()+RULES.leaseMs,serverAt:serverTimestamp()});
    });
  }
  async maintain() {
    const key=runKey(this.s);
    await runTransaction(this.db,async tx=>{
      const [ls,rs]=await Promise.all([tx.get(this.lobbyRef),tx.get(this.runtimeRef)]),s=ls.data()?.stoney,r=rs.data(),now=this.now();
      if(!s||runKey(s)!==key||r?.runId!==key||r.hostId!==this.uid||r.hostSession!==this.sessionId||r.hostUntil<=now)return;
      if(s.phase==='setup'&&now>=s.setupDeadline){this.writeStart(tx,s);return;}
      if(s.phase!=='play')return;
      const carrier=s.carrierId?(await tx.get(doc(this.playersRef,s.carrierId))).data():null;
      if(now>=finite(s.endAt)) {
        tx.update(this.lobbyRef,{'stoney.phase':'ended','stoney.winner':'dm','stoney.winnerName':ls.data().dm||'Dungeon Master','stoney.lastEvent':'Time is up. The Dungeon Master wins!','stoney.lastEventAt':now});return;
      }
      if(s.carrierId&&(!carrier||!living(carrier,now,s)||(carrier.hidden&&now-finite(carrier.hiddenAt)>RULES.staleMs))) {
        const drop=carrier&&!this.maze.blocked(carrier.x,carrier.z)?{x:carrier.x,z:carrier.z}:this.maze.center(this.maze.entry);
        tx.update(this.lobbyRef,{'stoney.carrierId':null,'stoney.carrierName':null,'stoney.crown':drop,'stoney.lastEvent':'👑 The disconnected carrier dropped the crown.','stoney.lastEventAt':now});
      }
    });
    if(this.now()-finite(this.lastPurge)>12000) {
      this.lastPurge=this.now();const expired=[...this.traps].filter(([,t])=>!usableTrap(t,this.s,this.now())).slice(0,24);
      if(expired.length){const batch=writeBatch(this.db);for(const [id] of expired)batch.delete(doc(this.trapsRef,id));await batch.commit();}
    }
  }
  stop() {
    if(this.closed)return;this.closed=true;clearInterval(this.timer);this.writer?.close();for(const off of this.off)off();this.off=[];
    document.removeEventListener('visibilitychange',this.visibility);window.removeEventListener('online',this.connectivity);window.removeEventListener('offline',this.connectivity);
    // Best effort only. Stale detection and host leases still work when the tab vanishes abruptly.
    if(this.myRef&&navigator.onLine)void runTransaction(this.db,async tx=>{
      const p=(await tx.get(this.myRef)).data();if(p?.sessionId===this.sessionId)tx.update(this.myRef,{online:false,hidden:true,hiddenAt:this.now(),serverAt:serverTimestamp()});
    }).catch(()=>{});
  }
}
