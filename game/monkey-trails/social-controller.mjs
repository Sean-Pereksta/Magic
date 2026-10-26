import {socialFirebaseContext} from './social-connection.mjs';
import {SocialStore,createFirestoreSocialAdapter,nearbyPlayers,SOCIAL_CONFIG,applicationStatus} from './social-store.mjs';
import {snapshotTeam,newRaidAttempt,raidAction,duelStake} from './social-combat.mjs';
import {tribeWeek,TRIBE_WEEK,weeklyProgress,cleanContribution,recordTribeProgress,applyAdventureClaim} from './tribe-adventures.mjs';
import {localSocial,availableInPack,depositFromPack,retryPackDeposits,applySocialReceipt} from './social-local.mjs';
const copy=x=>structuredClone(x);
// Firestore map key order may differ from the local snapshot. Only confirmed
// profile snapshots acknowledge a crew; a heartbeat can lose a race to a lock.
const crewSignature=team=>JSON.stringify(team,(_key,value)=>value&&typeof value==='object'&&!Array.isArray(value)?Object.fromEntries(Object.entries(value).sort(([a],[b])=>a.localeCompare(b))):value);
async function defaultConnection(saveId){const {db,f,auth}=await socialFirebaseContext(saveId);return{adapter:createFirestoreSocialAdapter({db,f}),uid:auth.currentUser.uid};}
export class SocialController{
 constructor({getState,getVault,persist,onUpdate=()=>{},onBattle=()=>{},onReturn=()=>{},connect=defaultConnection,now=Date.now}={}){
  Object.assign(this,{getState,getVault,persist,onUpdate,onBattle,onReturn,connect,now});
  this.model={status:'idle',error:'',me:null,tribe:null,members:[],presence:[],nearby:[],offers:[],challenges:[],duel:null,raid:null,receipts:[],now:now(),localFruit:0,localItems:{},adventure:null,adventureMembers:[],adventureClaims:[],previousAdventure:null,tribeRankings:[],rankingsStatus:'idle',rankingsError:'',rankingsUpdatedAt:null};
  this.subscriptions=[];this.crewProfiles=new Map();this.crewWatches=new Map();this.generation=0;this.receiptTask=Promise.resolve();this.actionTask=null;this.lastHeartbeat=0;this.lastBusy=null;this.discoverable=false;this.latestLocation=null;this.teamSignature=null;this.heartbeatTask=null;
  Object.assign(this.model,{directory:[],directoryStatus:'idle',directorySearch:'',directoryAvailableOnly:false,applications:[],tribeApplications:[],publicTribe:null,publicMembers:[],rankingMetric:'bosses',weeklyMembership:null});
 }
 get uid(){return this.store?.uid;}
 get locked(){return !!(this.model.me?.activeDuel||this.model.raid||this.model.me?.activeRaid);}
 get busy(){return this.locked||!['trail','camp'].includes(this.getState()?.run.phase);}
 async start(){
  const generation=await this.stop(),state=this.getState();if(!state||generation!==this.generation)return;
  if(state.mode==='practice'){this.model.status='practice';this.emit();return;}
  this.model.status='connecting';this.model.error='';this.emit();
  try{
   const context=await this.connect(this.getVault().id);if(generation!==this.generation)return;
   this.store=new SocialStore(context.adapter,{uid:context.uid,now:this.now});
   const me=await this.store.register({saveId:this.getVault().id,name:state.name,team:snapshotTeam(state)});if(generation!==this.generation)return;
   const capabilities=await this.store.initializeCompatibility();if(generation!==this.generation)return;this.model.capabilities=capabilities;
   if(!this.model.capabilities.publicActivity&&this.model.rankingMetric==='weekly')this.model.rankingMetric='bosses';
   this.model.me=me;this.model.status='ready';this.teamSignature=crewSignature(me.team);
   void this.refreshTribeRankings();
   void this.refreshDirectory();
   await retryPackDeposits(this.localOptions());if(generation!==this.generation)return;
   const error=e=>{this.model.error=`Online update unavailable: ${e.message}`;this.emit();};
   this.subscriptions.push(this.store.watchPlayer(this.uid,p=>{const changed=this.model.me?.activeDuel!==p?.activeDuel;this.model.me=p;this.teamSignature=crewSignature(p?.team);if(p?.tribeId!==this.tribeCode)this.watchTribe(p?.tribeId);if(p?.activeDuel!==this.duelId)this.watchActiveDuel(p?.activeDuel);if(changed&&p?.activeDuel)this.onBattle();this.emit();},error));
   this.subscriptions.push(this.store.watchPresence(list=>{this.model.presence=list;this.emit();},error));
   this.subscriptions.push(this.store.watchChallenges(list=>{this.model.challenges=list;this.emit();},error));
   this.subscriptions.push(this.store.watchOffers(list=>{this.model.offers=list;this.emit();},error));
   this.subscriptions.push(this.store.watchReceipts(list=>{this.model.receipts=list;this.queueReceipts();this.emit();},error));
   this.subscriptions.push(this.store.watchAdventureClaims(list=>{this.model.adventureClaims=list;this.queueAdventureClaims();this.emit();},error));
   if(this.model.capabilities.tribeApplications)this.subscriptions.push(this.store.watchApplications(list=>{this.model.applications=list.map(r=>({...r,displayStatus:applicationStatus(r,this.now())}));void this.store?.expireApplications(list).catch(error);this.emit();},error));
   await this.loadPreviousAdventure().catch(error);
   if(me.activeRaid)await this.restoreRaid(me.activeRaid);
   if(me.activeDuel)this.onBattle();
   await this.heartbeat(true);this.timer=setInterval(()=>{void this.heartbeat();this.emit();},25000);this.emit();
  }catch(e){if(generation===this.generation){this.model.status='offline';this.model.error=e.code==='permission-denied'?'Multiplayer rules are not enabled yet. Your trail is saved; retry after the Monkey Trails Firestore rules are published.':e.message;this.emit();}}
 }
 async stop(){
  const generation=++this.generation;this.applicationUnsubscribe?.();this.applicationUnsubscribe=null;this.applicationTribe=null;for(const stop of this.adventureWatches||[])stop();this.adventureWatches=[];this.adventureWatchKey=null;clearInterval(this.timer);this.timer=null;for(const unsubscribe of this.subscriptions)unsubscribe();this.subscriptions=[];this.tribeUnsubscribe?.();this.tribeUnsubscribe=null;this.tribeCode=null;this.duelUnsubscribe?.();this.duelUnsubscribe=null;this.duelId=null;for(const entry of this.crewWatches.values())entry.unsubscribe?.();this.crewWatches.clear();this.crewProfiles.clear();this.teamSignature=null;
  const previous=this.store;this.store=null;if(previous)void previous.heartbeat({online:false,busy:true}).catch(()=>{});
  this.model={...this.model,status:'idle',capabilities:null,me:null,tribe:null,members:[],duel:null,raid:null,receipts:[],challenges:[],offers:[],presence:[],nearby:[],adventure:null,adventureMembers:[],adventureClaims:[],previousAdventure:null,tribeRankings:[],rankingsStatus:'idle',rankingsError:'',rankingsUpdatedAt:null,directory:[],directoryStatus:'idle',applications:[],tribeApplications:[],publicTribe:null,publicMembers:[],weeklyMembership:null};return generation;
 }
 emit(){
  this.model.now=this.now();this.model.ownBossRank=this.store?.tribeRanks?.get(this.model.me?.tribeId)||null;if(this.store&&this.model.me?.tribeId)this.watchAdventure(this.model.me.tribeId);const nearby=this.uid?nearbyPlayers(this.model.presence,this.uid,this.now()):[];this.watchCrewProfiles(nearby);
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
 async refreshTribeRankings(){
  const store=this.store,generation=this.generation;
  if(!store||this.model.status!=='ready')return[];
  if(this.rankingsTask?.generation===generation)return this.rankingsTask.promise;
  const task={generation,promise:null};this.rankingsTask=task;
  this.model.rankingsStatus='loading';this.model.rankingsError='';this.emit();
  task.promise=(async()=>{
   try{
    const rows=await store.listTribeRankings(this.model.rankingMetric,this.model.me?.tribeId);if(generation!==this.generation||store!==this.store)return[];
    this.model.tribeRankings=rows;this.model.ownTribeRanking=rows.own||null;this.model.rankingsStatus='ready';this.model.rankingsUpdatedAt=this.now();this.emit();return rows;
   }catch(error){
    if(generation===this.generation&&store===this.store){this.model.rankingsStatus='error';this.model.rankingsError=`Rankings unavailable: ${error.message}`;this.emit();}return[];
   }finally{if(this.rankingsTask===task)this.rankingsTask=null;}
  })();return task.promise;
 }
 async refreshDirectory({search=this.model.directorySearch,availableOnly=this.model.directoryAvailableOnly}={}){const store=this.store,generation=this.generation;if(!store||this.model.status!=='ready')return[];const request=++this.directoryRequest||1;this.directoryRequest=request;this.model.directorySearch=String(search||'');this.model.directoryAvailableOnly=!!availableOnly;this.model.directoryStatus='loading';this.emit();try{const rows=await store.listDirectory({search,availableOnly});if(this.generation===generation&&this.store===store&&request===this.directoryRequest){this.model.directory=rows;this.model.directoryStatus='ready';this.emit();}return rows;}catch(e){if(this.generation===generation&&request===this.directoryRequest){this.model.directoryStatus='error';this.model.error=`Tribe directory unavailable: ${e.message}`;this.emit();}return[];}}
 async setTribeRanking(metric){if(!['bosses','weekly','power'].includes(metric))return;if(metric==='weekly'&&this.model.capabilities?.publicActivity===false)throw Error('Public weekly rankings are unavailable on this server.');if(this.rankingsTask)await this.rankingsTask.promise;if(this.model.rankingMetric!==metric){this.model.tribeRankings=[];this.model.ownTribeRanking=null;}this.model.rankingMetric=metric;return this.refreshTribeRankings();}
 async viewTribe(id){const store=this.store,generation=this.generation;if(!store)return;const request=++this.publicProfileRequest||1;this.publicProfileRequest=request;const value=await store.getPublicTribe(id);if(generation!==this.generation||store!==this.store||request!==this.publicProfileRequest)return;if(!value)throw Error('This tribe is no longer available.');this.model.publicTribe=value.tribe;this.model.publicMembers=value.members;this.emit();}
 closeTribeProfile(){this.publicProfileRequest=(this.publicProfileRequest||0)+1;this.model.publicTribe=null;this.model.publicMembers=[];this.emit();}
 async requestTribe(id){this.requireFree();const result=await this.store.requestTribe(id);await this.refreshDirectory();await this.refreshTribeRankings();return result;}
 async reviewApplication(id,accept){this.requireFree();const result=await this.store.reviewApplication(id,accept);await this.refreshDirectory();return result;}
 cancelApplication(id){return this.store.cancelApplication(id);}
 async leaveTribe(){this.requireFree();await this.store.leaveTribe();this.model.previousAdventure=null;this.closeTribeProfile();await this.refreshDirectory();await this.refreshTribeRankings();}
 watchLeaderApplications(tribe){const code=this.model.capabilities?.tribeApplications!==false&&tribe?.ownerId===this.uid?tribe.id:null;if(code===this.applicationTribe)return;this.applicationUnsubscribe?.();this.applicationUnsubscribe=null;this.applicationTribe=code;this.model.tribeApplications=[];if(code)this.applicationUnsubscribe=this.store.watchTribeApplications(code,list=>{if(this.applicationTribe!==code)return;this.model.tribeApplications=list.map(r=>({...r,displayStatus:applicationStatus(r,this.now())}));void this.store?.expireApplications(list).catch(()=>{});this.emit();},e=>{this.model.error=e.message;this.emit();});}
 watchActiveDuel(id){this.duelUnsubscribe?.();this.duelId=id||null;this.model.duel=null;if(id)this.duelUnsubscribe=this.store.watchDuel(id,duel=>{if(this.duelId===id){this.model.duel=duel;this.emit();}},error=>{this.model.error=error.message;this.emit();});}
 watchTribe(code){
  this.tribeUnsubscribe?.();this.watchLeaderApplications(null);for(const stop of this.adventureWatches||[])stop();this.adventureWatches=[];this.adventureWatchKey=null;this.model.adventure=null;this.model.adventureMembers=[];this.tribeCode=code||null;this.model.tribe=null;this.model.members=[];if(!code)return;
  this.tribeUnsubscribe=this.store.watchTribe(code,tribe=>{if(this.tribeCode!==code)return;this.model.tribe=tribe;this.watchLeaderApplications(tribe);this.emit();},e=>{this.model.error=e.message;this.emit();});
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
    if(online&&this.model.me?.tribeId){const state=this.getState();await store.contributeAdventure(cleanContribution(weeklyProgress(state,this.now())));const previousWeek=tribeWeek(this.now())-TRIBE_WEEK,previous=state.meta.tribeAdventure?.weeks?.[String(previousWeek)];if(previous){await store.contributeAdventure(cleanContribution(previous),previousWeek);await this.loadPreviousAdventure();}await this.queueAdventureClaims();}
   }catch(e){if(generation===this.generation&&store===this.store){this.model.error=e.message;this.emit();}}
   finally{if(this.heartbeatTask===task){this.heartbeatTask=null;if(task.queued)void this.heartbeat(task.queuedForce);}}
  })();return task.promise;
 }
 observeLocation(fix){
  if(!fix||!Number.isFinite(fix.lat)||!Number.isFinite(fix.lon))return;
  // Only a short-lived, coarse point is published after the explicit opt-in.
  const accuracy=Math.max(0,fix.accuracy||25)+80;
  this.model.locationMessage=accuracy>500?'Location accuracy is too low to find nearby crews. Try again outdoors.':'';
  this.latestLocation=accuracy>500?null:{lat:Math.round(fix.lat*1000)/1000,lon:Math.round(fix.lon*1000)/1000,accuracy,at:Math.round(fix.timestamp||this.now())};
 }
 async shareNearby(enabled){
  this.discoverable=!!enabled;
  if(!enabled){this.latestLocation=null;this.model.locationMessage='';await this.heartbeat(true);this.emit();return;}
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
   for(const receipt of this.model.receipts)if(!receipt.seen){changed=applySocialReceipt(state,receipt)||changed;if(receipt.type==='trade')changed=recordTribeProgress(state,{trades:1},`trade:${receipt.id}`,this.now())||changed;}
   if(changed){await this.persist();this.emit();}
  }).catch(e=>{this.model.error=`Reward delivery pending: ${e.message}`;this.emit();});return this.receiptTask;
 }
 async dismissReceipt(id){
  await this.queueReceipts();const receipt=this.model.receipts.find(r=>r.id===id);if(!receipt)return;
  if(!localSocial(this.getState()).applied[`${receipt.uid}:${receipt.id}`])throw Error('This reward will be delivered after you begin your next expedition.');
  await this.persist();await this.store.acknowledgeReceipt(id);
 }
 watchAdventure(tribeId){
  const week=tribeWeek(this.now()),key=`${tribeId}:${week}`;if(this.adventureWatchKey===key)return;
  for(const stop of this.adventureWatches||[])stop();this.adventureWatches=[];this.adventureWatchKey=key;this.model.adventure=null;this.model.adventureMembers=[];
  const error=e=>{this.model.error=`Shared adventure pending: ${e.message}`;this.emit();};
  this.adventureWatches.push(this.store.watchAdventure(tribeId,value=>{if(this.adventureWatchKey===key){this.model.adventure=value;this.emit();}},error,week));
  this.adventureWatches.push(this.store.watchAdventureMembers(tribeId,value=>{if(this.adventureWatchKey===key){this.model.adventureMembers=value;this.emit();}},error,week));
  const store=this.store;void store.adapter.get(`adventureMembers/${store.adventureIdentity(week)}`).then(member=>{if(this.store===store&&this.adventureWatchKey===key){this.model.weeklyMembership=member;this.emit();}}).catch(error);
  void this.loadPreviousAdventure().catch(error);
 }
 async loadPreviousAdventure(){
  const store=this.store;if(!store)return;const week=tribeWeek(this.now())-TRIBE_WEEK;
  const member=await store.adapter.get(`adventureMembers/${store.adventureIdentity(week)}`);
  if(!member||member.tribeId!==this.model.me?.tribeId)return;
  const summary=await store.adapter.get(`adventures/${member.summaryId}`);if(this.store===store)this.model.previousAdventure={member,summary};
 }
 async joinAdventure(){this.requireFree();weeklyProgress(this.getState(),this.now());await this.persist();const member=await this.store.joinAdventure(cleanContribution(weeklyProgress(this.getState(),this.now())));this.model.weeklyMembership=member;await this.heartbeat(true);this.emit();return member;}
 async claimAdventure(weekStart=tribeWeek(this.now())){
  if(this.getVault().conflict)throw Error('Resolve the saved trail conflict before claiming supplies.');
  const claim=await this.store.claimAdventure(Number(weekStart)||tribeWeek(this.now()));
  if(claim.saveId===this.getVault().id&&applyAdventureClaim(this.getState(),claim))await this.persist();
  await this.queueAdventureClaims();this.emit();return claim;
 }
 queueAdventureClaims(){
  this.receiptTask=this.receiptTask.then(async()=>{const state=this.getState();if(!state||this.getVault()?.conflict)return;let changed=false;for(const claim of this.model.adventureClaims||[])if(claim.saveId===this.getVault()?.id)changed=applyAdventureClaim(state,claim)||changed;if(changed)await this.persist();}).catch(e=>{this.model.error=`Adventure reward delivery pending: ${e.message}`;this.emit();});return this.receiptTask;
 }
 recordTrade(id){recordTribeProgress(this.getState(),{trades:1},`trade:${id}`,this.now());return this.persist();}
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
  this.requireFree();await this.refreshProfile();const ticket=await this.store.startRaid();const attempt=newRaidAttempt(ticket.team,ticket.bossLevel??ticket.level,ticket.seed,ticket.version??1,ticket.patternVersion||0);
  localSocial(this.getState()).raid={ticket,actions:[]};this.model.raid={...attempt,attemptId:ticket.id,actions:[],submitted:false};await this.persist();this.onBattle();await this.heartbeat(true);this.emit();
 }
 async restoreRaid(id){
  const saved=localSocial(this.getState()).raid,ticket=saved?.ticket?.id===id?saved.ticket:await this.store.getRaid(id);
  if(!ticket||ticket.submitted)return;
  const attempt=newRaidAttempt(ticket.team,ticket.bossLevel??ticket.level,ticket.seed,ticket.version??1,ticket.patternVersion||0),actions=saved?.ticket?.id===id?saved.actions:[];for(const action of actions)raidAction(attempt,action);
  localSocial(this.getState()).raid={ticket,actions};this.model.raid={...attempt,attemptId:id,actions:copy(actions),submitted:false};this.onBattle();
 }
 async raidMove(action){const raid=this.model.raid;if(!raid||raid.submitted)throw Error('Start a new hourly attempt first.');raidAction(raid,{...action,expectedSequence:action.expectedSequence??raid.sequence});raid.actions.push(action);localSocial(this.getState()).raid.actions=copy(raid.actions);await this.persist();this.emit();}
 async submitRaid(){const raid=this.model.raid;if(!raid||raid.status!=='finished')throw Error('Complete all ten turns first.');const result=await this.store.submitRaid(raid.attemptId,raid.actions);raid.submitted=true;raid.result=result;recordTribeProgress(this.getState(),{raids:1},`raid:${raid.attemptId}`,this.now());await this.persist();if(result.defeated)void this.refreshTribeRankings();this.emit();}
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

