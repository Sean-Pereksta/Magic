import {RULES,COST,TTL,finite,stamp,roundKey,makeMaze,spawnPoint,blocked,isOnline,isLiving,energy,trapWindow,placementProblem,defaultCrown,canPickup,canEscape,EnemyDirector,LatestWriter} from './core.mjs';

/** Firestore stays the transport: existing lobby/auth configuration and no rules relaxation. */
export async function firebaseDriver() {
  const [appSDK,authSDK,fs]=await Promise.all([
    import('https://www.gstatic.com/firebasejs/10.7.1/firebase-app.js'),
    import('https://www.gstatic.com/firebasejs/10.7.1/firebase-auth.js'),
    import('https://www.gstatic.com/firebasejs/10.7.1/firebase-firestore.js')]);
  const config={apiKey:'AIzaSyB7twY7z31ucB6pGA8JC_HrVMZhA8lNaJA',authDomain:'bible-game-246c0.firebaseapp.com',projectId:'bible-game-246c0',storageBucket:'bible-game-246c0.appspot.com',messagingSenderId:'959619818996',appId:'1:959619818996:web:5a9fbf492e23c765e445a1'};
  const app=appSDK.getApps().length?appSDK.getApp():appSDK.initializeApp(config),auth=authSDK.getAuth(app),db=fs.getFirestore(app);
  const user=await new Promise((resolve,reject)=>{
    const timeout=setTimeout(()=>{off();reject(new Error('Sign-in timed out. Check your connection and Firebase Authentication.'));},15000);
    const off=authSDK.onAuthStateChanged(auth,async current=>{off();try{resolve(current||(await authSDK.signInAnonymously(auth)).user);}catch(e){reject(e);}finally{clearTimeout(timeout);}},reject);
  });
  const ref=path=>fs.doc(db,path), data=snap=>snap.exists()?snap.data():null;
  return {uid:user.uid,time:()=>fs.serverTimestamp(),
    get:async path=>data(await fs.getDocFromServer(ref(path))),
    set:(path,value)=>fs.setDoc(ref(path),value,{merge:true}),
    transaction:fn=>fs.runTransaction(db,tx=>fn({get:async path=>data(await tx.get(ref(path))),set:(path,value)=>tx.set(ref(path),value,{merge:true}),update:(path,value)=>tx.update(ref(path),value),delete:path=>tx.delete(ref(path))})),
    listenDoc:(path,cb,error)=>fs.onSnapshot(ref(path),{includeMetadataChanges:true},snap=>cb(data(snap),snap.metadata),error),
    listenCollection:(path,cb,error)=>fs.onSnapshot(fs.collection(db,path),{includeMetadataChanges:true},snap=>cb(snap.docChanges().map(c=>({id:c.doc.id,type:c.type,data:c.doc.data()})),snap.metadata),error)
  };
}
export class StoneyRoom {
  constructor(driver,{id,name='Player',role='player',onChange=()=>{},onStatus=()=>{},onEvent=()=>{},now=Date.now,visible=()=>!globalThis.document?.hidden}={}) {
    if(!id||id.includes('/')||id.length>150)throw new Error('Missing or invalid gameId. Open this game from its lobby.');
    this.d=driver;this.uid=driver.uid;this.id=id;this.name=String(name).slice(0,22);this.role=role;
    this.session=globalThis.crypto?.randomUUID?.()||`${Date.now()}-${Math.random().toString(36).slice(2)}`;
    this.base=`lobbies/${id}`;this.playerPath=`${this.base}/players/${this.uid}`;this.posePath=`${this.base}/stoneyPoses/${this.uid}-${this.session}`;this.syncPath=`${this.base}/stoneySync/state`;
    this.onChange=onChange;this.onStatus=onStatus;this.onEvent=onEvent;this.localNow=now;this.visible=visible;this.offset=0;
    this.statuses=new Map();this.poses=new Map();this.traps=new Map();this.unsubs=[];this.busy=new Set();this.cooldowns=new Map();this.closed=false;this.joined='';this.world=null;this.pose=null;this.lastPose=null;this.lastOffer=0;this.lastHostAt=0;this.lastClaim=0;this.lastMaintenance=0;this.pendingHits=new Set();this.epoch=0;this.readsReady=new Set();this.failedReads=new Map();this.lastEvent='';this.offline=false;this.lastAck=0;this.net='Connecting…';
    this.writer=new LatestWriter(value=>{if(!this.healthy()||!this.joined)throw new Error('Waiting for a live connection.');return this.d.set(this.posePath,{...value,serverAt:this.d.time()});},{now:()=>this.now(),onAck:v=>{this.lastPose=v;this.lastAck=this.now();this.report();},onError:e=>this.error(e)});
  }
  now(){return this.localNow()+this.offset;}
  healthy(){return !this.closed&&!this.locked&&!this.offline&&['lobby','players','poses','traps','world'].every(key=>this.readsReady.has(key));}
  setStatus(text){if(text!==this.net){this.net=text;this.onStatus(text);}}
  report(){if(this.locked)this.setStatus('Paused: this player opened in another tab');else if(this.offline)this.setStatus('Offline — movement paused');else if(this.failedReads.size)this.setStatus(`Connection issue: ${[...this.failedReads.values()][0]?.message||'listener stopped — reload to reconnect'}`);else if(!this.healthy()||!this.joined)this.setStatus('Syncing…');else if(this.writer.inFlight&&this.now()-this.lastAck>10000)this.setStatus('Reconnecting — movement paused');else if(this.writer.failures)this.setStatus(`Retrying movement sync (${this.writer.failures})`);else if(this.legacyPlayers().length)this.setStatus('Older client present — reopen all Stoney tabs');else this.setStatus(this.isHost()?'Online • simulation host':'Online');}
  playable(){return this.healthy()&&this.joined===roundKey(this.state)&&this.now()-this.lastAck<12000&&!this.legacyPlayers().length;}
  error(e){this.lastError=e;console.error('[stoney]',e);this.setStatus(e?.code==='permission-denied'?'Permission denied — check deployed Firestore rules':`Sync issue: ${e?.message||e}`);}
  applyLobby(lobby){
    this.lobby=lobby||{};const old=roundKey(this.state);this.state=lobby?.stoney||null;
    if(this.state&&roundKey(this.state)!==old){this.maze=makeMaze(this.state.w,this.state.h,this.state.seed,this.state.cellSize);this.director=new EnemyDirector(this.maze);this.pendingHits.clear();this.writer.pending=null;this.joined='';this.joinRound().catch(e=>this.error(e));}
    const event=this.state?.lastEventAt?`${this.state.lastEventAt}:${this.state.lastEvent}`:'';
    if(event&&event!==this.lastEvent){this.lastEvent=event;this.onEvent(this.state.lastEvent||'');}
    this.onChange();
  }
  async start(){
    this.setStatus('Signing in…');
    // A server-confirmed timestamp calibrates deadlines/presence rather than trusting device clocks.
    const before=this.localNow();await this.d.set(this.posePath,{uid:this.uid,sessionId:this.session,stoneyRole:this.role,online:false,serverAt:this.d.time()});
    const probe=await this.d.get(this.posePath),after=this.localNow();
    if(stamp(probe?.serverAt))this.offset=stamp(probe.serverAt)-(before+after)/2;
    if(this.closed)return this;
    await this.d.transaction(async tx=>{
      const lobby=await tx.get(this.base),s=lobby?.stoney,now=this.now();
      if(this.role!=='dm'){if(!lobby)tx.set(this.base,{createdAt:now,gameType:'stoney'});return;}
      if(lobby?.dmUid&&lobby.dmUid!==this.uid)throw new Error('Only the assigned Dungeon Master may open the DM controls.');
      if(!lobby?.dmUid&&lobby?.dm&&lobby.dm!==this.name)throw new Error(`The assigned Dungeon Master is ${lobby.dm}.`);
      const fresh={seed:Math.floor(Math.random()*2000000000),w:21,h:21,cellSize:4,phase:'setup',setupDeadline:now+30000,crown:null,carrierId:null,carrierName:null,dmEnergyBase:0,dmEnergyStamp:now,startAt:null,endAt:null,winner:null,winnerName:null,lastEvent:'The Dungeon Master is preparing the dungeon…',lastEventAt:now,protocol:2};
      if(!lobby)tx.set(this.base,{createdAt:now,gameType:'stoney',dm:this.name,dmUid:this.uid,stoney:fresh});
      else tx.update(this.base,{dmUid:this.uid,...(!lobby.dm?{dm:this.name}:{}),...(!s?{stoney:fresh}:{'stoney.protocol':2})});
    });
    const lobby=await this.d.get(this.base);if(this.closed)return this;this.applyLobby(lobby);
    this.subscribe();
    this.lastAck=this.now();await this.joinRound();this.report();return this;
  }
  subscribe(){
    for(const off of this.unsubs)off();this.unsubs=[];this.readsReady.clear();this.failedReads.clear();this.statuses.clear();this.poses.clear();this.traps.clear();
    const failure=(key,e)=>{this.readsReady.delete(key);this.failedReads.set(key,e);this.error(e);};
    const metadata=(key,meta)=>{if(meta?.fromCache)this.readsReady.delete(key);else if(!meta?.hasPendingWrites)this.readsReady.add(key);this.report();};
    this.unsubs.push(this.d.listenDoc(this.base,(data,meta)=>{metadata('lobby',meta);this.applyLobby(data);},e=>failure('lobby',e)));
    for(const [key,path,map] of [['players',`${this.base}/players`,this.statuses],['poses',`${this.base}/stoneyPoses`,this.poses],['traps',`${this.base}/traps`,this.traps]]) {
      this.unsubs.push(this.d.listenCollection(path,(changes,meta)=>{metadata(key,meta);for(const c of changes){if(c.type==='removed')map.delete(c.id);else map.set(c.id,{...c.data,id:c.id,...(key==='players'?{uid:c.id}:{})});}
        const self=this.statuses.get(this.uid);if(key==='players'&&!meta?.fromCache&&!meta?.hasPendingWrites&&this.joined&&self?.sessionId&&self.sessionId!==this.session){this.locked=true;this.writer.close();this.report();}
        this.onChange();},e=>failure(key,e)));
    }
    this.unsubs.push(this.d.listenDoc(this.syncPath,(w,meta)=>{
      metadata('world',meta);if(meta?.hasPendingWrites)return;
      if(w&&this.world&&w.round===this.world.round&&(finite(w.epoch)<finite(this.world.epoch)||(w.epoch===this.world.epoch&&w.seq<this.world.seq)))return;
      this.world=w;this.worldReceived=this.now();this.onChange();
    },e=>failure('world',e)));
  }
  async joinRound(){
    if(!this.state||this.closed||this.busy.has('join')||this.joined===roundKey(this.state))return;
    this.busy.add('join');const key=roundKey(this.state),m=this.maze;
    try{
      const self=await this.d.transaction(async tx=>{
        const lobby=await tx.get(this.base);if(roundKey(lobby?.stoney)!==key)return null;
        const old=await tx.get(this.playerPath),oldPose=old?.sessionId?await tx.get(`${this.base}/stoneyPoses/${this.uid}-${old.sessionId}`):null;
        const same=old?.stoneyRound===key&&old.stoneyRole===this.role;
        let p=same&&oldPose?.life===old.life?oldPose:old;
        if(!same||!p||blocked(m,{x:finite(p.x,Infinity),z:finite(p.z,Infinity)}))p=spawnPoint(m,this.uid);
        const value={uid:this.uid,name:this.name,stoneyRole:this.role,protocol:2,stoneyRound:key,sessionId:this.session,life:same?finite(old.life):0,x:p.x,z:p.z,yaw:same?finite(p.yaw):0,alive:same?old.alive!==false:true,deadUntil:same?finite(old.deadUntil):0,shieldUntil:same?finite(old.shieldUntil):this.now()+RULES.shield,joinedAt:old?.joinedAt||this.now(),updatedAt:this.now()};
        tx.set(this.playerPath,value);return value;
      });
      if(!self||this.closed)return;this.statuses.set(this.uid,self);this.pose={x:self.x,z:self.z,yaw:self.yaw,vx:0,vz:0};this.joined=key;this.lastPose=null;this.lastOffer=0;this.onChange();
    }finally{this.busy.delete('join');if(this.state&&key!==roundKey(this.state))this.joinRound().catch(e=>this.error(e));}
  }
  players(){
    const out=[];for(const [uid,p] of this.statuses){if(p.stoneyRound&&p.stoneyRound!==roundKey(this.state))continue;
      const pose=this.poses.get(`${uid}-${p.sessionId}`),valid=pose&&pose.round===roundKey(this.state)&&pose.life===finite(p.life)&&Number.isFinite(pose.x)&&Number.isFinite(pose.z);
      out.push({...p,...(valid?{x:pose.x,z:pose.z,yaw:pose.yaw,vx:pose.vx,vz:pose.vz,serverAt:pose.serverAt,updatedAt:pose.updatedAt,online:pose.online,active:pose.active}:{}),x:finite(valid?pose.x:p.x),z:finite(valid?pose.z:p.z),yaw:finite(valid?pose.yaw:p.yaw),vx:finite(valid?pose.vx:p.vx),vz:finite(valid?pose.vz:p.vz),uid});
    }return out;
  }
  legacyPlayers(){return this.players().filter(p=>p.protocol!==2&&isOnline(p,this.now()));}
  me(){return this.statuses.get(this.uid);}
  activeTraps(){return [...this.traps.values()].filter(t=>(!t.round||t.round===roundKey(this.state))&&this.state&&trapWindow(t,this.state).expiresAt>this.now());}
  setPose(p){this.pose={...this.pose,...p};}
  setOffline(value){if(this.closed)return;this.offline=value;if(value){this.writer.pending=null;this.readsReady.clear();}else{this.subscribe();this.lastOffer=0;}this.report();}
  isHost(){return this.world?.round===roundKey(this.state)&&this.world.owner===this.uid&&this.world.session===this.session&&this.world.epoch===this.epoch&&this.now()-(stamp(this.world.serverAt)||finite(this.world.updatedAt))<RULES.lease;}
  async guarded(key,fn,delay=600){if(this.closed||this.busy.has(key)||this.now()<(this.cooldowns.get(key)||0))return false;
    this.busy.add(key);this.lastError=null;try{return await fn();}catch(e){this.error(e);return false;}finally{this.busy.delete(key);this.cooldowns.set(key,this.now()+delay);}}
  tick(dt=.05){
    if(this.closed)return;const now=this.now(),self=this.me();this.report();
    if(this.healthy()&&this.joined&&self&&this.pose){
      const moving=!this.lastPose||Math.hypot(this.pose.x-this.lastPose.x,this.pose.z-this.lastPose.z)>.035||Math.abs(this.pose.yaw-this.lastPose.yaw)>.025||this.lastPose.active!==this.visible()||this.lastPose.life!==finite(self.life);
      if(now-this.lastOffer>=(moving?RULES.moveInterval:RULES.idleInterval)){
        this.lastOffer=now;this.writer.offer({...this.pose,uid:this.uid,sessionId:this.session,round:this.joined,life:finite(self.life),online:true,active:this.visible(),updatedAt:now});
      }this.writer.flush();
    }
    if(!this.playable()||!this.visible()||!this.state)return;
    if(now-this.lastClaim>1000){this.lastClaim=now;this.claimHost();}
    if(this.isHost()){
      if(this.state.phase==='play'&&this.director){for(const id of this.director.step(this.activeTraps(),this.state,this.players(),now,dt))this.pendingHits.add(id);}
      const interval=this.director?.enemies.size?RULES.worldInterval:2000;
      if(now-this.lastHostAt>=interval||this.pendingHits.size){this.lastHostAt=now;this.publish();}
      if(now-this.lastMaintenance>=1000){this.lastMaintenance=now;this.maintain();}
    }
  }
  claimHost(){return this.guarded('claim',async()=>{
    const now=this.now(),w=this.world;
    if(w?.round===roundKey(this.state)&&now-(stamp(w.serverAt)||finite(w.updatedAt))<RULES.lease)return;
    const candidates=this.players().filter(p=>p.protocol===2&&p.active!==false&&isOnline(p,now)).sort((a,b)=>(a.uid===w?.owner?1:0)-(b.uid===w?.owner?1:0)||(a.stoneyRole==='dm'?-1:0)-(b.stoneyRole==='dm'?-1:0)||a.uid.localeCompare(b.uid));
    if(candidates[0]?.uid!==this.uid)return;
    const won=await this.d.transaction(async tx=>{
      const current=await tx.get(this.syncPath),lobby=await tx.get(this.base),self=await tx.get(this.playerPath);
      if(!self||self.sessionId!==this.session||roundKey(lobby?.stoney)!==this.joined)return null;
      if(current?.round===this.joined&&this.now()-(stamp(current.serverAt)||finite(current.updatedAt))<RULES.lease)return null;
      const next={owner:this.uid,session:this.session,round:this.joined,epoch:finite(current?.epoch)+1,seq:0,enemies:current?.round===this.joined?current.enemies||[]:[],updatedAt:this.now(),serverAt:this.d.time()};
      tx.set(this.syncPath,next);return next;
    });
    if(won){this.epoch=won.epoch;this.director.restore(won.enemies);this.pendingHits.clear();this.world={...won,serverAt:null};this.lastHostAt=0;}
  },300);}
  publish(){return this.guarded('publish',async()=>{
    const enemies=this.director.snapshot(),hits=[...this.pendingHits];this.pendingHits.clear();const expectedEpoch=this.epoch;
    const committed=await this.d.transaction(async tx=>{
      const w=await tx.get(this.syncPath),lobby=await tx.get(this.base),s=lobby?.stoney,now=this.now();
      if(!w||w.owner!==this.uid||w.session!==this.session||w.epoch!==expectedEpoch||w.round!==this.joined||roundKey(s)!==this.joined||now-(stamp(w.serverAt)||finite(w.updatedAt))>=RULES.lease)return false;
      const victims=[];for(const uid of hits){const p=await tx.get(`${this.base}/players/${uid}`);const pose=p?.sessionId?await tx.get(`${this.base}/stoneyPoses/${uid}-${p.sessionId}`):null;if(p&&pose&&pose.life===finite(p.life)&&pose.round===this.joined)victims.push({...p,...pose,uid,alive:p.alive,deadUntil:p.deadUntil,shieldUntil:p.shieldUntil});}
      // All reads precede writes. Crown drop and death are committed together.
      if(s?.phase==='play'&&now<finite(s.endAt))for(const p of victims){
        if(!isLiving(p,now)||finite(p.shieldUntil)>now)continue;
        const contact=enemies.some(e=>Math.hypot(e.x-p.x,e.z-p.z)<1.15)||this.activeTraps().some(t=>t.type==='fire'&&now>=trapWindow(t,s).armedAt&&Math.hypot(t.x-p.x,t.z-p.z)<1.1);
        if(!contact)continue;
        tx.update(`${this.base}/players/${p.uid}`,{alive:false,deadUntil:now+RULES.respawn,x:p.x,z:p.z,updatedAt:now});
        if(s.carrierId===p.uid)tx.update(this.base,{'stoney.carrierId':null,'stoney.carrierName':null,'stoney.crown':{x:p.x,z:p.z},'stoney.lastEvent':`👑 ${p.name} was caught and dropped the crown!`,'stoney.lastEventAt':now});
      }
      tx.update(this.syncPath,{enemies:s?.phase==='play'?enemies:[],seq:finite(w.seq)+1,updatedAt:now,serverAt:this.d.time()});return true;
    });
    if(!committed)this.epoch=0;
  },0);}
  maintain(){return this.guarded('maintain',async()=>{
    const now=this.now();if(this.state.phase==='setup'&&now>=finite(this.state.setupDeadline)){await this.ready(true);return;}
    if(this.state.phase!=='play')return;
    await this.d.transaction(async tx=>{
      const w=await tx.get(this.syncPath),lobby=await tx.get(this.base),s=lobby?.stoney;
      if(!this.owns(w)||s?.phase!=='play')return;
      const carrier=s.carrierId?await tx.get(`${this.base}/players/${s.carrierId}`):null;
      const pose=carrier?.sessionId?await tx.get(`${this.base}/stoneyPoses/${s.carrierId}-${carrier.sessionId}`):null;
      if(this.now()>=finite(s.endAt)){tx.update(this.base,{'stoney.phase':'ended','stoney.winner':'dm','stoney.winnerName':lobby.dm||'Dungeon Master','stoney.lastEvent':'Time is up. The Dungeon Master wins!','stoney.lastEventAt':this.now()});return;}
      if(s.carrierId&&(!carrier||carrier.alive===false||!pose||this.now()-(stamp(pose.serverAt)||finite(pose.updatedAt))>20000)){
        let p=pose?.round===this.joined&&pose.life===finite(carrier?.life)?pose:carrier||spawnPoint(this.maze);if(blocked(this.maze,p))p=spawnPoint(this.maze);
        tx.update(this.base,{'stoney.carrierId':null,'stoney.carrierName':null,'stoney.crown':{x:p.x,z:p.z},'stoney.lastEvent':'👑 The disconnected carrier’s crown is back on the ground.','stoney.lastEventAt':this.now()});
      }
    });
    if(now-(this.lastPurge||0)>10000){this.lastPurge=now;const expired=[...this.traps.values()].filter(t=>trapWindow(t,this.state).expiresAt<now-2000).slice(0,24);const stale=[...this.poses.values()].filter(p=>now-(stamp(p.serverAt)||finite(p.updatedAt))>60000).slice(0,16);
      if(expired.length||stale.length)await this.d.transaction(async tx=>{const w=await tx.get(this.syncPath);const paths=[...expired.map(t=>`${this.base}/traps/${t.id}`),...stale.map(p=>`${this.base}/stoneyPoses/${p.id}`)];const values=[];for(const path of paths)values.push(await tx.get(path));if(!this.owns(w))return;paths.forEach((path,i)=>{const v=values[i];if(v&&(path.includes('/traps/')?trapWindow(v,this.state).expiresAt<now-2000:now-(stamp(v.serverAt)||finite(v.updatedAt))>60000))tx.delete(path);});});
    }
  },600);}
  owns(w){return w?.owner===this.uid&&w.session===this.session&&w.epoch===this.epoch&&w.round===this.joined&&this.now()-(stamp(w.serverAt)||finite(w.updatedAt))<RULES.lease;}
  async readSelf(tx){const status=await tx.get(this.playerPath),pose=status?.sessionId?await tx.get(`${this.base}/stoneyPoses/${this.uid}-${status.sessionId}`):null;if(!status||status.sessionId!==this.session||!pose||pose.life!==finite(status.life)||pose.round!==this.joined)return null;return {...status,...pose,uid:this.uid,alive:status.alive,deadUntil:status.deadUntil,shieldUntil:status.shieldUntil};}
  pickup(){return this.guarded('crown',async()=>{if(!this.playable())return;await this.d.transaction(async tx=>{const lobby=await tx.get(this.base),p=await this.readSelf(tx),s=lobby?.stoney;if(!p||roundKey(s)!==this.joined||!canPickup(s,p,this.now()))return;tx.update(this.base,{'stoney.carrierId':this.uid,'stoney.carrierName':this.name,'stoney.crownDiscovered':true,'stoney.lastEvent':`👑 ${this.name} has the crown — escort them to the entrance!`,'stoney.lastEventAt':this.now()});});},500);}
  escape(){return this.guarded('escape',async()=>{if(!this.playable())return;await this.d.transaction(async tx=>{const lobby=await tx.get(this.base),p=await this.readSelf(tx),s=lobby?.stoney;if(!p||roundKey(s)!==this.joined||!canEscape(this.maze,s,p,this.now()))return;tx.update(this.base,{'stoney.phase':'ended','stoney.winner':'players','stoney.winnerId':this.uid,'stoney.winnerName':this.name,'stoney.lastEvent':`🏆 ${this.name} escaped with the crown!`,'stoney.lastEventAt':this.now()});});},500);}
  respawn(){return this.guarded('respawn',async()=>{
    if(!this.playable())return;
    const next=await this.d.transaction(async tx=>{const lobby=await tx.get(this.base),p=await tx.get(this.playerPath);if(!p||p.sessionId!==this.session||p.stoneyRound!==this.joined||roundKey(lobby?.stoney)!==this.joined||lobby.stoney.phase!=='play'||p.alive!==false||this.now()<finite(p.deadUntil))return null;const pos=spawnPoint(this.maze,this.uid),patch={...pos,alive:true,deadUntil:0,shieldUntil:this.now()+RULES.shield,life:finite(p.life)+1,updatedAt:this.now()};tx.update(this.playerPath,patch);return {...p,...patch};});
    if(next){this.statuses.set(this.uid,next);this.pose={x:next.x,z:next.z,yaw:0,vx:0,vz:0};this.lastOffer=0;this.onChange();}
  },500);}
  ready(automatic=false){return this.guarded('ready',async()=>{if(!this.playable())return;
    await this.d.transaction(async tx=>{const lobby=await tx.get(this.base),s=lobby?.stoney,w=automatic?await tx.get(this.syncPath):null;if(!s||s.phase!=='setup'||roundKey(s)!==this.joined)return;if(automatic){if(!this.owns(w)||this.now()<finite(s.setupDeadline))return;}else if(lobby.dmUid!==this.uid)throw new Error('Only the Dungeon Master can start early.');const now=this.now();tx.update(this.base,{'stoney.phase':'play','stoney.protocol':2,'stoney.crown':s.crown||defaultCrown(this.maze),'stoney.carrierId':null,'stoney.carrierName':null,'stoney.crownDiscovered':false,'stoney.startAt':now,'stoney.endAt':now+RULES.duration,'stoney.lastEvent':'The dungeon is open. Find the crown and escape together!','stoney.lastEventAt':now});});
  },800);}
  place(type,point){return this.guarded('place',async()=>{
    if(!this.playable())throw new Error('Wait for the connection to finish syncing.');if(type!=='crown'&&!(type in COST))throw new Error('Unknown trap type.');
    const trapId=`${this.session}-${Math.random().toString(36).slice(2)}`,playerIds=[...this.statuses.keys()],trapIds=[...this.traps.keys()];
    await this.d.transaction(async tx=>{const lobby=await tx.get(this.base),s=lobby?.stoney;if(lobby?.dmUid!==this.uid)throw new Error('Only the Dungeon Master can place items.');if(!s||roundKey(s)!==this.joined||!['setup','play'].includes(s.phase))throw new Error('This round is not accepting spawns.');
      // Presence positions are already streamed. Reading every moving pose inside this transaction
      // would cause retries under load; the protected radius and spawn warning cover transit.
      const statuses=await Promise.all(playerIds.map(uid=>tx.get(`${this.base}/players/${uid}`)));
      const trapData=await Promise.all(trapIds.map(id=>tx.get(`${this.base}/traps/${id}`)));
      const players=statuses.flatMap((p,i)=>{if(!p)return [];const uid=playerIds[i],pose=this.poses.get(`${uid}-${p.sessionId}`);return [{...p,...(pose?.life===finite(p.life)&&pose.round===this.joined?pose:{}),uid,alive:p.alive,deadUntil:p.deadUntil}];});
      const traps=trapData.flatMap((t,i)=>t?[{...t,id:trapIds[i]}]:[]);
      const now=this.now(),problem=placementProblem(this.maze,s,point,players,traps,now,type);if(problem)throw new Error(problem);
      if(type==='crown'){tx.update(this.base,{'stoney.crown':{x:point.x,z:point.z},'stoney.lastEvent':'👑 The crown has been placed.','stoney.lastEventAt':now});return;}
      const remaining=energy(s,now)-COST[type];if(remaining<0)throw new Error(`Not enough energy (${Math.floor(energy(s,now))}/${COST[type]}).`);
      tx.update(this.base,{'stoney.dmEnergyBase':remaining,'stoney.dmEnergyStamp':now});
      tx.set(`${this.base}/traps/${trapId}`,{type,x:point.x,z:point.z,round:this.joined,placedBy:this.name,placedUid:this.uid,placedAt:now,ttlMs:TTL[type],telegraphMs:RULES.spawnWarning});
    });this.onEvent(type==='crown'?'Crown placed.':`${type[0].toUpperCase()+type.slice(1)} placed — energy and spawn saved together.`);return true;
  },300);}
  close(){this.closed=true;this.writer.close();for(const off of this.unsubs)off();this.unsubs=[];this.readsReady.clear();}
}
