import {socialFirebaseContext} from './storage.mjs';
import {SocialStore,createFirestoreSocialAdapter,nearbyPlayers,SOCIAL_CONFIG} from './social-store.mjs';
import {snapshotTeam,newRaidAttempt,raidAction,duelStake} from './social-combat.mjs';
import {localSocial,availableInPack,depositFromPack,retryPackDeposits,applySocialReceipt} from './social-local.mjs';
const copy=x=>structuredClone(x);
// Firestore map key order may differ from the local snapshot. Only confirmed
// profile snapshots acknowledge a crew; a heartbeat can lose a race to a lock.
const crewSignature=team=>JSON.stringify(team,(_key,value)=>value&&typeof value==='object'&&!Array.isArray(value)?Object.fromEntries(Object.entries(value).sort(([a],[b])=>a.localeCompare(b))):value);
async function defaultConnection(){const {db,f,auth}=await socialFirebaseContext();return{adapter:createFirestoreSocialAdapter({db,f}),uid:auth.currentUser.uid};}
export class SocialController{
 constructor({getState,getVault,persist,onUpdate=()=>{},onBattle=()=>{},onReturn=()=>{},connect=defaultConnection,now=Date.now}={}){
  Object.assign(this,{getState,getVault,persist,onUpdate,onBattle,onReturn,connect,now});
  this.model={status:'idle',error:'',me:null,tribe:null,members:[],presence:[],nearby:[],offers:[],challenges:[],duel:null,raid:null,receipts:[],now:now(),localFruit:0,localItems:{}};
  this.subscriptions=[];this.crewProfiles=new Map();this.crewWatches=new Map();this.generation=0;this.receiptTask=Promise.resolve();this.actionTask=null;this.lastHeartbeat=0;this.lastBusy=null;this.discoverable=false;this.latestLocation=null;this.teamSignature=null;this.heartbeatTask=null;
 }
 get uid(){return this.store?.uid;}
 get locked(){return !!(this.model.me?.activeDuel||this.model.raid||this.model.me?.activeRaid);}
 get busy(){return this.locked||!['trail','camp'].includes(this.getState()?.run.phase);}
 async start(){
  const generation=await this.stop(),state=this.getState();if(!state||generation!==this.generation)return;
  if(state.mode==='practice'){this.model.status='practice';this.emit();return;}
  this.model.status='connecting';this.model.error='';this.emit();
  try{
   const context=await this.connect();if(generation!==this.generation)return;
   this.store=new SocialStore(context.adapter,{uid:context.uid,now:this.now});
   const me=await this.store.register({saveId:this.getVault().id,name:state.name,team:snapshotTeam(state)});if(generation!==this.generation)return;
   this.model.me=me;this.model.status='ready';this.teamSignature=crewSignature(me.team);
   await retryPackDeposits(this.localOptions());if(generation!==this.generation)return;
   const error=e=>{this.model.error=`Online update unavailable: ${e.message}`;this.emit();};
   this.subscriptions.push(this.store.watchPlayer(this.uid,p=>{const changed=this.model.me?.activeDuel!==p?.activeDuel;this.model.me=p;this.teamSignature=crewSignature(p?.team);if(p?.tribeId!==this.tribeCode)this.watchTribe(p?.tribeId);if(p?.activeDuel!==this.duelId)this.watchActiveDuel(p?.activeDuel);if(changed&&p?.activeDuel)this.onBattle();this.emit();},error));
   this.subscriptions.push(this.store.watchPresence(list=>{this.model.presence=list;this.emit();},error));
   this.subscriptions.push(this.store.watchChallenges(list=>{this.model.challenges=list;this.emit();},error));
   this.subscriptions.push(this.store.watchOffers(list=>{this.model.offers=list;this.emit();},error));
   this.subscriptions.push(this.store.watchReceipts(list=>{this.model.receipts=list;this.queueReceipts();this.emit();},error));
   if(me.activeRaid)await this.restoreRaid(me.activeRaid);
   if(me.activeDuel)this.onBattle();
   await this.heartbeat(true);this.timer=setInterval(()=>{void this.heartbeat();this.emit();},25000);this.emit();
  }catch(e){if(generation===this.generation){this.model.status='offline';this.model.error=e.code==='permission-denied'?'Multiplayer rules are not enabled yet. Your trail is saved; retry after the Monkey Trails Firestore rules are published.':e.message;this.emit();}}
 }
 async stop(){
  const generation=++this.generation;clearInterval(this.timer);this.timer=null;for(const unsubscribe of this.subscriptions)unsubscribe();this.subscriptions=[];this.tribeUnsubscribe?.();this.tribeUnsubscribe=null;this.tribeCode=null;this.duelUnsubscribe?.();this.duelUnsubscribe=null;this.duelId=null;for(const entry of this.crewWatches.values())entry.unsubscribe?.();this.crewWatches.clear();this.crewProfiles.clear();this.teamSignature=null;
  const previous=this.store;this.store=null;if(previous)void previous.heartbeat({online:false,busy:true}).catch(()=>{});
  this.model={...this.model,status:'idle',me:null,tribe:null,members:[],duel:null,raid:null,receipts:[],challenges:[],offers:[],presence:[],nearby:[]};return generation;
 }
 emit(){
  this.model.now=this.now();const nearby=this.uid?nearbyPlayers(this.model.presence,this.uid,this.now()):[];this.watchCrewProfiles(nearby);
  const profile=uid=>uid===this.uid?this.model.me:this.crewProfiles.get(uid),presence=uid=>this.model.presence.find(x=>x.uid===uid)||{};
  this.model.members=(this.model.tribe?.members||[]).map(uid=>{const p=profile(uid);return p?{...p,...presence(uid)}:null;}).filter(Boolean);
  this.model.canClaimTimeout=!!(this.model.duel?.status==='active'&&this.now()-this.model.duel.updatedAt>=90000&&this.model.duel.state?.players[this.model.duel.state.turn]!==this.uid);const state=this.getState();this.model.localFruit=state?.run.fruit||0;this.model.localItems=state?.run.items||{};this.model.busy=this.busy;this.model.discoverable=this.discoverable;
  this.model.nearby=nearby.map(p=>({...profile(p.uid),...p}));this.onUpdate(this.model);
 }
 watchCrewProfiles(nearby){
  const desired=new Set([...(this.model.tribe?.members||[]),...nearby.map(p=>p.uid)].filter(uid=>uid&&uid!==this.uid));
  for(const [uid,entry]of this.crewWatches)if(!desired.has(uid)){this.crewWatches.delete(uid);entry.unsubscribe?.();this.crewProfiles.delete(uid);}
  if(!this.store)return;
  const store=this.store,generation=this.generation;
  for(const uid of desired)if(!this.crewWatches.has(uid)){
   const entry={unsubscribe:null};this.crewWatches.set(uid,entry);
   entry.unsubscribe=store.watchPlayer(uid,p=>{if(generation!==this.generation||this.store!==store||this.crewWatches.get(uid)!==entry)return;if(p)this.crewProfiles.set(uid,p);else this.crewProfiles.delete(uid);this.emit();},e=>{if(this.crewWatches.get(uid)===entry){this.model.error=e.message;this.emit();}});
  }
 }
 chooseDuel(){if(this.model.me?.activeDuel!==this.duelId)this.watchActiveDuel(this.model.me?.activeDuel);}
 watchActiveDuel(id){this.duelUnsubscribe?.();this.duelId=id||null;this.model.duel=null;if(id)this.duelUnsubscribe=this.store.watchDuel(id,duel=>{if(this.duelId===id){this.model.duel=duel;this.emit();}},error=>{this.model.error=error.message;this.emit();});}
 watchTribe(code){
  this.tribeUnsubscribe?.();this.tribeCode=code||null;this.model.tribe=null;this.model.members=[];if(!code)return;
  this.tribeUnsubscribe=this.store.watchTribe(code,tribe=>{if(this.tribeCode!==code)return;this.model.tribe=tribe;this.emit();},e=>{this.model.error=e.message;this.emit();});
 }
 localOptions(){return{state:this.getState(),store:this.store,uid:this.uid,saveId:this.getVault().id,persist:this.persist};}
 async heartbeat(force=false){
  if(!this.store||this.model.status!=='ready')return;
  const store=this.store,generation=this.generation,online=!document.hidden,busy=this.busy,team=this.locked?null:snapshotTeam(this.getState()),changed=team&&crewSignature(team)!==this.teamSignature;
  if(!force&&!changed&&this.now()-this.lastHeartbeat<25000&&this.lastBusy===busy&&online===this.lastOnline)return;
  if(this.heartbeatTask?.generation===generation){this.heartbeatTask.queued=true;this.heartbeatTask.queuedForce||=force;return this.heartbeatTask.promise;}
  this.lastHeartbeat=this.now();this.lastBusy=busy;this.lastOnline=online;
  const task={generation,queued:false,queuedForce:false,promise:null};this.heartbeatTask=task;
  task.promise=(async()=>{
   try{
    if(online&&!busy&&this.discoverable&&(!this.latestLocation||this.now()-this.latestLocation.at>45000))await this.refreshNearbyFix();
    if(generation!==this.generation||store!==this.store)return;
    await store.heartbeat({online,busy,discoverable:this.discoverable,location:this.latestLocation,...(changed?{team}:{})});
   }catch(e){if(generation===this.generation&&store===this.store){this.model.error=e.message;this.emit();}}
   finally{if(this.heartbeatTask===task){this.heartbeatTask=null;if(task.queued)void this.heartbeat(task.queuedForce);}}
  })();return task.promise;
 }
 observeLocation(fix){
  if(!fix||!Number.isFinite(fix.lat)||!Number.isFinite(fix.lon))return;
  // Only a short-lived, coarse point is published after the explicit opt-in.
  this.latestLocation={lat:Math.round(fix.lat*1000)/1000,lon:Math.round(fix.lon*1000)/1000,accuracy:Math.min(1000,Math.max(0,fix.accuracy||25)+80),at:fix.timestamp||this.now()};
 }
 async shareNearby(enabled){
  this.discoverable=!!enabled;
  if(!enabled){this.latestLocation=null;await this.heartbeat(true);this.emit();return;}
  try{await this.refreshNearbyFix();}catch(error){this.discoverable=false;this.latestLocation=null;await this.heartbeat(true);this.emit();throw error;}
  await this.heartbeat(true);this.emit();
 }
 async refreshNearbyFix(){
  if(this.locationTask)return this.locationTask;
  if(!navigator.geolocation)throw Error('This browser does not provide location. Tribe challenges still work.');
  this.locationTask=new Promise((resolve,reject)=>navigator.geolocation.getCurrentPosition(resolve,reject,{enableHighAccuracy:true,maximumAge:0,timeout:12000})).then(position=>{if(this.discoverable)this.observeLocation({lat:position.coords.latitude,lon:position.coords.longitude,accuracy:position.coords.accuracy,timestamp:position.timestamp});}).finally(()=>{this.locationTask=null;});return this.locationTask;
 }
 queueReceipts(){
  this.receiptTask=this.receiptTask.then(async()=>{const state=this.getState();if(!state||state.run.phase==='dead')return;let changed=false;
   for(const receipt of this.model.receipts)if(!receipt.seen)changed=applySocialReceipt(state,receipt)||changed;
   if(changed){await this.persist();this.emit();}
  }).catch(e=>{this.model.error=`Reward delivery pending: ${e.message}`;this.emit();});return this.receiptTask;
 }
 async dismissReceipt(id){
  await this.queueReceipts();const receipt=this.model.receipts.find(r=>r.id===id);if(!receipt)return;
  if(!localSocial(this.getState()).applied[`${receipt.uid}:${receipt.id}`])throw Error('This reward will be delivered after you begin your next expedition.');
  await this.persist();await this.store.acknowledgeReceipt(id);
 }
 async fund(asset,amount){
  if(this.getVault().conflict)throw Error('Resolve the saved trail conflict before moving supplies.');
  await retryPackDeposits(this.localOptions());const me=await this.store.getPlayer(),balance=asset==='fruit'?me.wallet.fruit:me.wallet.items[asset]||0,missing=Math.max(0,amount-balance);
  if(missing)await depositFromPack({...this.localOptions(),asset,amount:missing});
 }
 requireFree(){if(!this.store||this.model.status!=='ready')throw Error('Connect multiplayer first.');if(this.busy)throw Error('Return from the current battle before beginning another.');}
 async refreshProfile(){if(this.heartbeatTask?.generation===this.generation)await this.heartbeatTask.promise;const me=await this.store.register({saveId:this.getVault().id,name:this.getState().name,team:snapshotTeam(this.getState())});this.teamSignature=crewSignature(me.team);await this.heartbeat(true);}
 async challenge(uid){this.requireFree();await this.refreshProfile();const me=await this.store.getPlayer(),target=Math.min(duelStake(snapshotTeam(this.getState())),availableInPack(this.getState(),'fruit')+me.wallet.fruit);await this.fund('fruit',target);return this.store.challenge(uid);}
 async accept(id){this.requireFree();await this.refreshProfile();const me=await this.store.getPlayer(),target=Math.min(duelStake(snapshotTeam(this.getState())),availableInPack(this.getState(),'fruit')+me.wallet.fruit);await this.fund('fruit',target);const duel=await this.store.acceptChallenge(id);this.onBattle();return duel;}
 async startRaid(){
  this.requireFree();await this.refreshProfile();const ticket=await this.store.startRaid();const attempt=newRaidAttempt(ticket.team,ticket.bossLevel??ticket.level,ticket.seed);
  localSocial(this.getState()).raid={ticket,actions:[]};this.model.raid={...attempt,attemptId:ticket.id,actions:[],submitted:false};await this.persist();this.onBattle();await this.heartbeat(true);this.emit();
 }
 async restoreRaid(id){
  const saved=localSocial(this.getState()).raid,ticket=saved?.ticket?.id===id?saved.ticket:await this.store.getRaid(id);
  if(!ticket||ticket.submitted)return;
  const attempt=newRaidAttempt(ticket.team,ticket.bossLevel??ticket.level,ticket.seed),actions=saved?.ticket?.id===id?saved.actions:[];for(const action of actions)raidAction(attempt,action);
  localSocial(this.getState()).raid={ticket,actions};this.model.raid={...attempt,attemptId:id,actions:copy(actions),submitted:false};this.onBattle();
 }
 async raidMove(action){const raid=this.model.raid;if(!raid||raid.submitted)throw Error('Start a new hourly attempt first.');raidAction(raid,{...action,expectedSequence:action.expectedSequence??raid.sequence});raid.actions.push(action);localSocial(this.getState()).raid.actions=copy(raid.actions);await this.persist();this.emit();}
 async submitRaid(){const raid=this.model.raid;if(!raid||raid.status!=='finished')throw Error('Complete all ten turns first.');const result=await this.store.submitRaid(raid.attemptId,raid.actions);raid.submitted=true;raid.result=result;await this.persist();this.emit();}
 async returnToTrail(){
  let me=await this.store.getPlayer();
  if(me?.activeDuel){
    const duel=this.model.duel||await this.store.adapter.get(`duels/${me.activeDuel}`);
    if(duel?.status==='finished'){
      const payout=duel.state.winner===null?duel.escrow[this.uid]:duel.state.winner===this.uid?Object.values(duel.escrow).reduce((a,b)=>a+b,0):0;
      if(payout)await this.withdraw('fruit',payout,`payout-${duel.id}`,true);
    }
    me=await this.store.returnToTrail();
  }
  if(me?.activeRaid){await this.store.abandonRaid(me.activeRaid);me=await this.store.getPlayer();}
  this.model.me=me;this.chooseDuel();
  this.model.raid=null;delete localSocial(this.getState()).raid;await this.persist();await this.queueReceipts();await this.heartbeat(true);this.onReturn();this.emit();
 }
 async deposit(asset,amount){if(this.getVault().conflict)throw Error('Resolve the saved trail conflict before moving supplies.');return depositFromPack({...this.localOptions(),asset,amount});}
 async withdraw(asset,amount,operationId=crypto.randomUUID(),battlePayout=false){
  if(!battlePayout&&!['trail','camp'].includes(this.getState().run.phase))throw Error('Return to the trail before claiming supplies.');
  await this.store.withdraw(asset,amount,operationId);const receipt=await this.store.adapter.get(`receipts/${this.uid}/entries/${operationId}`);if(receipt&&applySocialReceipt(this.getState(),receipt))await this.persist();await this.queueReceipts();this.emit();
 }
 async run(task){if(this.actionTask)throw Error('Wait for the current online action to finish.');this.model.error='';this.model.pendingAction=true;this.actionTask=Promise.resolve().then(task);this.emit();try{return await this.actionTask;}finally{this.actionTask=null;this.model.pendingAction=false;await this.heartbeat(true);this.emit();}}
}
