import {firebaseConfig} from '../../lobby/firebase-config.mjs';
import {setupMeta,claimSeat,configureSeat,configureCampaign,startCampaign,seatFor,houseIds,millis,ownsLease,LEASE_MS,HEARTBEAT_MS,GRACE_MS,resolutionDue,resolveRound,applyBoundaryTakeovers} from './multiplayer-rounds.mjs';
import {applyCommand} from './multiplayer-commands.mjs';
import {decodePayload,encodePayload,packCampaign,joinCampaign,playerView} from './multiplayer-state.mjs';

export function multiplayerRoute(search) {
  const p=new URLSearchParams(search),id=p.get('gameId')||p.get('lobbyId');
  if(!id)return null;
  if(!/^[A-Za-z0-9_-]{1,100}$/.test(id))throw new Error('Invalid campaign link.');
  return {lobbyId:id,username:(p.get('username')||'Ruler').slice(0,40)};
}
export class FirebaseCampaign {
  constructor({lobbyId,username,onState=()=>{},onStatus=()=>{},onError=()=>{}}){
    Object.assign(this,{lobbyId,username,onState,onStatus,onError});
    this.token=crypto.randomUUID();this.sequence=0;this.pending=new Map();this.unsubs=[];
    this.online=false;this.meta=null;this.presence={};this.stopped=false;this.busy=false;this.lastVersion=0;
  }
  now(){return Date.now()+(this.clockOffset||0);}
  emit(){this.onStatus({meta:this.meta,presence:this.presence,uid:this.uid,online:this.online,pending:this.pending.size,houseId:seatFor(this.meta,this.uid)});}
  error(e){this.online=false;this.emit();this.onError(e.message||'Connection lost. Reconnecting…');}
  async connect(){
    const [app,auth,f]=await Promise.all([
      import('https://www.gstatic.com/firebasejs/10.7.1/firebase-app.js'),
      import('https://www.gstatic.com/firebasejs/10.7.1/firebase-auth.js'),
      import('https://www.gstatic.com/firebasejs/10.7.1/firebase-firestore.js')
    ]);
    this.f=f;const firebase=app.getApps().length?app.getApps()[0]:app.initializeApp(firebaseConfig);
    this.db=f.getFirestore(firebase);this.auth=auth.getAuth(firebase);
    await new Promise((resolve,reject)=>{let off;off=auth.onAuthStateChanged(this.auth,()=>{queueMicrotask(()=>off?.());resolve();},reject);});
    if(!this.auth.currentUser)await auth.signInAnonymously(this.auth);
    this.uid=this.auth.currentUser.uid;this.lobbyRef=f.doc(this.db,'lobbies',this.lobbyId);
    this.metaRef=this.ref('iron_throne','meta');this.stateRef=this.ref('iron_throne','state');this.worldRef=this.ref('iron_throne','world');
    await this.transaction(async tx=>{
      const [l,m]=await Promise.all([tx.get(this.lobbyRef),tx.get(this.metaRef)]);
      if(!l.exists()||l.data().gameType!=='ironthrone'||!l.data().members?.[this.uid])throw new Error('Join this Iron Thrones campaign through the shared lobby first.');
      this.username=l.data().members[this.uid];
      if(!m.exists())tx.set(this.metaRef,setupMeta(l.data(),this.now()));
    });
    this.unsubs.push(f.onSnapshot(this.metaRef,{includeMetadataChanges:true},snap=>{
      if(!snap.exists())return;
      this.meta=snap.data();
      if(snap.metadata.fromCache){this.online=false;this.emit();return;}
      this.online=true;this.watchHouse();this.watchCommands();this.emit();void this.pump();
    },e=>this.error(e)));
    this.unsubs.push(f.onSnapshot(this.worldRef,{includeMetadataChanges:true},snap=>{
      if(!snap.exists()||snap.metadata.hasPendingWrites||snap.metadata.fromCache)return;
      this.worldEnvelope=snap.data();void this.receive();
    },e=>this.error(e)));
    this.unsubs.push(f.onSnapshot(f.collection(this.db,'lobbies',this.lobbyId,'iron_throne_presence'),snap=>{
      this.presence=Object.fromEntries(snap.docs.map(d=>[d.id,d.data()]));this.emit();
    },e=>this.error(e)));
    this.unsubs.push(f.onSnapshot(f.query(f.collection(this.db,'lobbies',this.lobbyId,'iron_throne_commands'),f.where('uid','==',this.uid),f.where('status','==','pending')),snap=>{
      // Restores pending indicators after a refresh without resubmitting commands.
      for(const d of snap.docs)this.watchReceipt(d.id);
    },e=>this.error(e)));
    // Install recovery before the first heartbeat: a transient offline SDK
    // response during refresh must not leave this tab without a retry timer.
    this.timer=setInterval(()=>void this.tick(),HEARTBEAT_MS);
    this.pumpTimer=setInterval(()=>void this.pump(),1500);
    this.wake=()=>void this.tick();window.addEventListener('online',this.wake);
    this.offline=()=>{this.online=false;this.emit();};window.addEventListener('offline',this.offline);
    this.connected=true;await this.tick();return this;
  }
  ref(collection,id){return this.f.doc(this.db,'lobbies',this.lobbyId,collection,id);}
  transaction(fn){return this.f.runTransaction(this.db,fn);}
  async heartbeat(){
    const ref=this.ref('iron_throne_presence',this.uid),start=Date.now();
    await this.transaction(async tx=>{await tx.get(this.metaRef);tx.set(ref,{uid:this.uid,name:this.username,at:this.f.serverTimestamp()});});
    const snap=await this.f.getDocFromServer(ref);
    this.clockOffset=millis(snap.data()?.at)-(start+Date.now())/2;
    this.online=true;this.emit();
  }
  async tick(){
    if(this.stopped||this.ticking)return;this.ticking=true;
    try{await this.heartbeat();await this.renewLease();await this.pump();}
    catch(e){this.error(e);}finally{this.ticking=false;}
  }
  async renewLease(){
    await this.transaction(async tx=>{
      const snap=await tx.get(this.metaRef);if(!snap.exists())return;
      const meta=snap.data(),now=this.now();if(!seatFor(meta,this.uid))return;
      const current=meta.lease,fresh=millis(current?.expiresAt)>now;
      if(fresh&&(current.uid!==this.uid||current.token!==this.token))return;
      const changed=!fresh||current?.token!==this.token;
      if(changed)meta.epoch++;
      meta.lease={uid:this.uid,token:this.token,expiresAt:this.f.Timestamp.fromMillis(now+LEASE_MS)};
      // Administrative ownership follows the active campaign after an absence.
      const host=await tx.get(this.ref('iron_throne_presence',meta.hostUid));
      const lobby=await tx.get(this.lobbyRef);
      if(meta.hostUid!==this.uid&&now-Math.max(millis(host.data()?.at),meta.startedAt??meta.planningAt)>GRACE_MS){
        meta.hostUid=this.uid;tx.update(this.lobbyRef,{hostUid:this.uid,host:this.username,updatedAt:now});
      }
      tx.update(this.metaRef,{lease:meta.lease,epoch:meta.epoch,hostUid:meta.hostUid});
    });
  }
  watchHouse(){
    const id=seatFor(this.meta,this.uid);if(id===this.watchedHouse)return;
    this.privateUnsub?.();this.privateEnvelope=null;this.watchedHouse=id;
    if(id)this.privateUnsub=this.f.onSnapshot(this.ref('iron_throne_private',id),{includeMetadataChanges:true},snap=>{
      if(!snap.exists()||snap.metadata.hasPendingWrites||snap.metadata.fromCache)return;
      this.privateEnvelope=snap.data();void this.receive();
    },e=>this.error(e));
  }
  watchCommands(){
    const controlling=ownsLease(this.meta,this.uid,this.token,this.now())&&this.meta.phase!=='setup';
    if(controlling===this.watchingCommands)return;
    this.commandUnsub?.();this.watchingCommands=controlling;this.commandDirty=false;
    if(controlling)this.commandUnsub=this.f.onSnapshot(this.f.query(this.f.collection(this.db,'lobbies',this.lobbyId,'iron_throne_commands'),this.f.where('status','==','pending')),snap=>{
      if(snap.metadata.fromCache)return;this.commandDirty=snap.docs.length>0;if(this.commandDirty)void this.pump();
    },e=>{this.watchingCommands=false;this.error(e);});
  }
  async receive(){
    const world=this.worldEnvelope,priv=this.privateEnvelope,id=this.watchedHouse;
    if(!id||!world||!priv||world.stateVersion!==priv.stateVersion||world.stateVersion<=this.lastVersion)return;
    try{
      const [w,p]=await Promise.all([decodePayload(world.payload),decodePayload(priv.payload)]);
      if(world.stateVersion<=this.lastVersion||id!==this.watchedHouse)return;
      this.lastVersion=world.stateVersion;this.state=playerView(w,p,id);this.onState(this.state);this.emit();
    }catch(e){this.error(e);}
  }
  async setup(action,args={}){
    await this.transaction(async tx=>{
      const snap=await tx.get(this.metaRef),m=snap.data();
      if(action==='claim')claimSeat(m,this.uid,this.username,args.houseId);
      else if(action==='seat')configureSeat(m,this.uid,args.houseId,args.kind);
      else if(action==='configure')configureCampaign(m,this.uid,args);
      else throw new Error('Unknown lobby action.');
      // Keep seat/options edits independent of concurrent lease heartbeats.
      tx.update(this.metaRef,action==='configure'?{options:m.options}:{seats:m.seats,ready:m.ready});
    });
    await this.renewLease();
  }
  async start(){
    await this.transaction(async tx=>{
      const [mSnap,sSnap,lSnap]=await Promise.all([tx.get(this.metaRef),tx.get(this.stateRef),tx.get(this.lobbyRef)]);
      const m=mSnap.data();if(sSnap.exists()||m.phase!=='setup')throw new Error('This campaign has already started.');
      const state=startCampaign(m,this.uid,this.now());
      m.epoch++;m.lease={uid:this.uid,token:this.token,expiresAt:this.f.Timestamp.fromMillis(this.now()+LEASE_MS)};
      const packed=await packCampaign(state,m);this.writeCampaign(tx,packed,m);
      tx.update(this.lobbyRef,{status:'started',updatedAt:this.now()});
    });
  }
  writeCampaign(tx,packed,meta){
    tx.set(this.stateRef,packed.canonical);tx.set(this.worldRef,packed.world);
    for(const id of houseIds)tx.set(this.ref('iron_throne_private',id),packed.privateByHouse[id]);
    if(meta.stateVersion===1)tx.set(this.metaRef,meta);
    else {
      // Renewals may extend this lease while a snapshot is being packed. Keep
      // the fresh expiry; epoch/host checks still fence a replaced controller.
      const {lease,...commit}=meta;tx.update(this.metaRef,commit);
    }
  }
  async readCampaign(tx){
    const docs=await Promise.all([tx.get(this.stateRef),...houseIds.map(id=>tx.get(this.ref('iron_throne_private',id)))]);
    if(docs.some(d=>!d.exists()))throw new Error('Campaign snapshot is incomplete.');
    const version=docs[0].data().stateVersion;
    if(docs.some(d=>d.data().stateVersion!==version))throw new Error('Campaign versions do not match.');
    const values=await Promise.all(docs.map(d=>decodePayload(d.data().payload)));
    return joinCampaign(values[0],Object.fromEntries(houseIds.map((id,i)=>[id,values[i+1]])));
  }
  watchReceipt(id){
    if(this.pending.has(id))return;
    this.pending.set(id,null);this.emit();
    const off=this.f.onSnapshot(this.ref('iron_throne_commands',id),snap=>{
      if(!snap.exists()||snap.metadata.hasPendingWrites)return;
      const d=snap.data();if(d.status==='pending')return;
      queueMicrotask(()=>{off();this.pending.delete(id);});this.pending.delete(id);this.emit();
      if(d.status==='rejected')this.onError(d.error||'Order rejected.');
    },e=>this.error(e));
    this.pending.set(id,off);
  }
  async submit(type,args={}){
    if(!this.online||!this.state||!['planning','founding'].includes(this.meta?.phase))throw new Error('Reconnecting or resolving. Wait for the current campaign state.');
    const id=`${this.token}_${++this.sequence}`,ref=this.ref('iron_throne_commands',id);
    const c={id,clientId:this.token,sequence:this.sequence,uid:this.uid,actorHouseId:seatFor(this.meta,this.uid),turn:this.state.turn,stateVersion:this.lastVersion,epoch:this.meta.epoch,type,args,status:'pending',createdAt:this.f.serverTimestamp()};
    // Offline transactions fail instead of queuing stale game orders for replay.
    await this.transaction(async tx=>{
      const [meta,existing]=await Promise.all([tx.get(this.metaRef),tx.get(ref)]);
      if(existing.exists())return;
      if(meta.data().phase!==(type==='found'?'founding':'planning')||meta.data().turn!==c.turn||meta.data().epoch!==c.epoch)throw new Error('The round or controller changed. Review and submit again.');
      tx.set(ref,c);
    });
    this.watchReceipt(id);void this.pump();return id;
  }
  async pump(){
    if(this.stopped||this.busy||!this.online||!ownsLease(this.meta,this.uid,this.token,this.now())||this.meta.phase==='setup'||this.meta.phase==='ended')return;
    this.busy=true;
    try{
      if(['planning','founding'].includes(this.meta.phase)&&this.commandDirty){
        this.commandDirty=false;
        const pending=await this.f.getDocs(this.f.query(this.f.collection(this.db,'lobbies',this.lobbyId,'iron_throne_commands'),this.f.where('status','==','pending')));
        const docs=pending.docs.sort((a,b)=>millis(a.data().createdAt)-millis(b.data().createdAt)||a.data().sequence-b.data().sequence||a.id.localeCompare(b.id));
        for(const d of docs.slice(0,12))await this.process(d.id);
        if(docs.length>12)this.commandDirty=true;
      }
      if(this.meta.phase==='resolving'||this.state&&resolutionDue(this.meta,this.presence,this.now(),this.state))await this.advance();
    }catch(e){this.commandDirty=true;this.error(e);}finally{this.busy=false;}
  }
  async process(id){
    await this.transaction(async tx=>{
      const [ms,cs]=await Promise.all([tx.get(this.metaRef),tx.get(this.ref('iron_throne_commands',id))]);
      const m=ms.data();if(!ownsLease(m,this.uid,this.token,this.now())||!cs.exists()||cs.data().status!=='pending')return;
      const state=await this.readCampaign(tx),c=cs.data();
      let result;
      try{result=applyCommand(state,m,c,{presence:this.presence,now:this.now()});}
      catch{result={ok:false,error:'Invalid order arguments. Review your order and try again.'};}
      if(result.ok){m.stateVersion++;this.writeCampaign(tx,await packCampaign(state,m),m);}
      tx.update(cs.ref,{status:result.ok?'accepted':'rejected',error:result.ok?'':result.error,stateVersionApplied:m.stateVersion,resolvedAt:this.f.serverTimestamp()});
    });
  }
  async advance(){
    await this.transaction(async tx=>{
      const ms=await tx.get(this.metaRef),m=ms.data();
      if(!ownsLease(m,this.uid,this.token,this.now())||!['planning','resolving'].includes(m.phase))return;
      const state=await this.readCampaign(tx);
      const uids=[...new Set(houseIds.map(id=>m.seats[id].uid).filter(Boolean))];
      const ps=await Promise.all(uids.map(uid=>tx.get(this.ref('iron_throne_presence',uid))));
      const presence=Object.fromEntries(uids.map((uid,i)=>[uid,ps[i].data()])),now=this.now();
      if(m.phase==='planning'){
        if(!resolutionDue(m,presence,now,state))return;
        // The lock is a durable phase. A new lease owner resumes this exact turn.
        m.phase='resolving';tx.update(this.metaRef,{phase:m.phase});return;
      }
      applyBoundaryTakeovers(m);resolveRound(state,m,presence,now);m.stateVersion++;
      this.writeCampaign(tx,await packCampaign(state,m),m);
      tx.set(this.ref('iron_throne_snapshots',String(state.turn%3)),{turn:state.turn,stateVersion:m.stateVersion,epoch:m.epoch,payload:await encodePayload(state)});
    });
  }
  close(){
    this.stopped=true;clearInterval(this.timer);clearInterval(this.pumpTimer);
    for(const off of this.unsubs)off();this.privateUnsub?.();this.commandUnsub?.();for(const off of this.pending.values())off?.();
    window.removeEventListener('online',this.wake);window.removeEventListener('offline',this.offline);
  }
}
