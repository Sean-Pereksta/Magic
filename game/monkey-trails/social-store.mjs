// Firestore-only social state. Transactions serialize membership, escrow and turn revisions.
import {ITEMS} from './core.mjs';
import {isPersonalMaterial} from './exploration.mjs';
import {teamPower} from './power.mjs';
import {tribeWeek,TRIBE_WEEK,TRIBE_ADVENTURE,cleanContribution,emptyContribution,contributionPoints,adventureReward} from './tribe-adventures.mjs';
import {newRaidAttempt,raidAction,bossHealth,bossReward,newDuel,duelAction,duelStake,duelRewards,SOCIAL_COMBAT_VERSION} from './social-combat.mjs';
export const SOCIAL_ROOT=['monkeyTrailsSocial','v1'];
export const SOCIAL_CONFIG={maxMembers:10,raidCooldown:3600000,presenceTTL:90000,challengeTTL:120000,mile:1609.344,maxOffers:20,maxApplications:3,applicationTTL:604800000};
export const TRIBE_RANKING_LIMIT=25;
const clone=x=>x==null?x:structuredClone(x),blankWallet=()=>({fruit:0,items:{}});
const cleanId=x=>{if(typeof x!=='string'||!x.length||x.length>160||x.includes('/'))throw Error('Invalid social identity.');return x;};
const count=n=>{if(!Number.isSafeInteger(n)||n<1||n>100000)throw Error('Choose a whole amount between 1 and 100,000.');return n;};
const assetId=x=>{if(x!=='fruit'&&(!Object.hasOwn(ITEMS,x)||isPersonalMaterial(x)))throw Error('Unknown supply. Personal crafting and training materials cannot enter the tribe pouch.');return x;};
const asset=v=>({asset:assetId(v?.asset),amount:count(v?.amount)});
const balance=(w,id)=>id==='fruit'?w.fruit:(w.items[id]||0);
function changeWallet(p,id,delta){assetId(id);const w=p.wallet;if(balance(w,id)+delta<0)throw Error('Not enough supplies in your social pouch.');if(id==='fruit')w.fruit+=delta;else w.items[id]=(w.items[id]||0)+delta;}
const ms=v=>typeof v?.toMillis==='function'?v.toMillis():Number(v)||0;
const average=team=>Math.max(1,Math.floor(team.reduce((n,p)=>n+p.level,0)/Math.max(1,team.length)));
const admission=value=>value==='approval'?'approval':'open';
const applicationId=(uid,slot)=>`${cleanId(uid)}-${slot}`;
const fullCapabilities=()=>({tribeApplications:true,tradeLocks:true,publicActivity:true});
const permissionDenied=error=>['permission-denied','firestore/permission-denied'].includes(error?.code);
export function applicationStatus(request,now=Date.now()){const status=['pending','accepted','rejected','cancelled','expired'].includes(request?.status)?request.status:'expired';return status==='pending'&&request.expiresAt<=now?'expired':status;}
export function publicTribe(tribe,rank=null){return{id:tribe.id,name:String(tribe.name||'Unnamed tribe').slice(0,32),emblem:cleanEmblem(tribe.emblem),memberCount:tribe.members?.length||0,bossLevel:tribe.boss?.level||1,bossesDefeated:Math.max(0,(tribe.boss?.level||1)-1),admissionMode:admission(tribe.admissionMode),acceptingApplications:tribe.acceptingApplications!==false&&(tribe.members?.length||0)<SOCIAL_CONFIG.maxMembers,rank};}
export function cleanEmblem(value={}){value=value&&typeof value==='object'?value:{};const shapes=['shield','circle','diamond'],symbols=['leaf','flame','bolt','moon','crown','paw'];return{shape:shapes.includes(value.shape)?value.shape:'shield',symbol:symbols.includes(value.symbol)?value.symbol:'leaf',color:/^#[0-9a-fA-F]{6}$/.test(value.color||'')?value.color:'#477a51'};}
// Rankings use permanent shared boss progress, not private player profiles or
// client-reported crew power. Equal boss levels share a competition rank.
export function rankTribes(tribes=[]){
 const rows=tribes.filter(t=>t&&typeof t.id==='string'&&Number.isSafeInteger(t.boss?.level)&&t.boss.level>=1).map(t=>({id:t.id,name:String(t.name||'Unnamed tribe').slice(0,32),emblem:cleanEmblem(t.emblem||{}),memberCount:Array.isArray(t.members)?t.members.length:0,bossLevel:t.boss.level,bossesDefeated:t.boss.level-1})).sort((a,b)=>b.bossLevel-a.bossLevel||(a.id<b.id?1:a.id>b.id?-1:0));
 let rank=0,previous=null;return rows.map((row,index)=>{if(row.bossLevel!==previous)rank=index+1;previous=row.bossLevel;return{...row,rank};});
}
function cleanName(name,max=32){name=String(name||'').trim();if(name.length<3||name.length>max)throw Error(`Use a name of 3–${max} characters.`);return name;}
function usableTeam(team){if(!Array.isArray(team)||team.length<1||team.length>3)throw Error('Choose one to three monkeys first.');return clone(team);}
export function locationDistance(a,b){if(!a||!b)return Infinity;const rad=Math.PI/180,dlat=(a.lat-b.lat)*rad,dlon=(a.lon-b.lon)*rad,h=Math.sin(dlat/2)**2+Math.cos(a.lat*rad)*Math.cos(b.lat*rad)*Math.sin(dlon/2)**2;return 12742017.6*Math.asin(Math.sqrt(Math.min(1,h)));}
function online(p,now){return !!p?.online&&ms(p.seenAt)>now-SOCIAL_CONFIG.presenceTTL&&ms(p.expiresAt)>now;}
function freshLocation(p,now){const x=p?.location;return p?.discoverable&&x&&[x.lat,x.lon,x.accuracy,x.at].every(Number.isFinite)&&Math.abs(x.lat)<=90&&Math.abs(x.lon)<=180&&x.accuracy>=0&&x.accuracy<=500&&x.at>now-SOCIAL_CONFIG.presenceTTL&&x.at<=now+10000;}
export function nearbyPlayers(presences,ownUid,now=Date.now()){
 const own=presences.find(p=>p.uid===ownUid);if(!online(own,now)||own.busy||!freshLocation(own,now))return[];
 return presences.filter(p=>p.uid!==ownUid&&online(p,now)&&!p.busy&&freshLocation(p,now)&&locationDistance(own.location,p.location)+own.location.accuracy+p.location.accuracy<=SOCIAL_CONFIG.mile).map(p=>({...p,distance:locationDistance(own.location,p.location)}));
}
const ppath=id=>`players/${cleanId(id)}`,receiptPath=(uid,id)=>`receipts/${cleanId(uid)}/entries/${cleanId(id)}`;
function requirePlayer(p){if(!p)throw Error('Connect a saved trail to multiplayer first.');return p;}
function requireFree(p,presence,now){requirePlayer(p);if(p.activeDuel||p.activeRaid||!online(presence,now)||presence.busy)throw Error('That player is busy or no longer online.');}
function touch(p,now){p.version++;p.updatedAt=now;return p;}
function payload(value={}){return{fruit:value.fruit||0,xp:value.xp||0,items:clone(value.items||{}),...(value.teamIds?{teamIds:[...value.teamIds]}:{}),...(value.payoutFruit!==undefined?{payoutFruit:value.payoutFruit}:{})};}

export class SocialStore{
 constructor(adapter,{uid,now=Date.now,id=()=>crypto.randomUUID()}={}){this.adapter=adapter;this.uid=cleanId(uid);this.now=now;this.makeId=id;}
 // The original v1 rules support open tribes, trades and adventures. Later
 // deployments add applications, trade locks and public weekly activity. Probe
 // only our own application slot; a denied optional read must not disable v1.
 initializeCompatibility(){
  if(!this.compatibilityPromise)this.compatibilityPromise=(async()=>{
   const capabilities=fullCapabilities();
   try{await this.adapter.get(`applications/${applicationId(this.uid,0)}`);}
   catch(error){if(!permissionDenied(error))throw error;capabilities.tribeApplications=false;capabilities.tradeLocks=false;capabilities.publicActivity=false;}
   this.capabilities=Object.freeze(capabilities);return this.capabilities;
  })().catch(error=>{this.compatibilityPromise=null;throw error;});
  return this.compatibilityPromise;
 }
 async register({saveId,name,team}){cleanId(saveId);name=cleanName(String(name||'').trim().slice(0,30),30);team=usableTeam(team);const now=this.now();return this.adapter.transaction(async tx=>{let p=await tx.get(ppath(this.uid));if(p){if(p.saveId!==saveId)throw Error('This online identity is linked to a different trail. Use its original saved trail.');p.name=name;if(!p.activeDuel&&!p.activeRaid){p.team=team;p.averageLevel=average(team);}touch(p,now);}else p={uid:this.uid,saveId,name,team,averageLevel:average(team),tribeId:null,wallet:blankWallet(),activeDuel:null,activeRaid:null,lastRaidAt:0,version:0,lastOp:'',createdAt:now,updatedAt:now};tx.set(ppath(this.uid),p);return p;});}
 getPlayer(id=this.uid){return this.adapter.get(ppath(id));}
 async heartbeat({online:isOnline=true,busy=false,discoverable=false,location=null,team}={}){const p=requirePlayer(await this.getPlayer()),now=this.now();if(location&&(![location.lat,location.lon,location.accuracy,location.at].every(Number.isFinite)||!Number.isSafeInteger(location.at)||Math.abs(location.lat)>90||Math.abs(location.lon)>180||location.accuracy<0||location.accuracy>500))location=null;const value={uid:this.uid,name:p.name,tribeId:p.tribeId,online:!!isOnline,busy:!!(busy||p.activeDuel||p.activeRaid),discoverable:!!discoverable,location:discoverable&&isOnline&&location?{lat:location.lat,lon:location.lon,accuracy:location.accuracy,at:location.at}:null,seenAt:now,expiresAt:isOnline?now+SOCIAL_CONFIG.presenceTTL:now};if(team&&!p.activeDuel&&!p.activeRaid){p.team=usableTeam(team);p.averageLevel=average(p.team);touch(p,now);await this.adapter.transaction(async tx=>{const current=requirePlayer(await tx.get(ppath(this.uid)));if(!current.activeDuel&&!current.activeRaid)tx.set(ppath(this.uid),{...current,team:p.team,averageLevel:p.averageLevel,updatedAt:now,version:current.version+1});});}await this.adapter.transaction(tx=>tx.set(`presence/${this.uid}`,value));return value;}
 async createTribe({name,emblem,admissionMode='open',acceptingApplications=true}){name=cleanName(name);const capabilities=await this.initializeCompatibility();if(!capabilities.tribeApplications&&(admission(admissionMode)!=='open'||!acceptingApplications))throw Error('These server rules support open tribes. Approval and paused admissions need the newer tribe rules.');const code=this.makeId().replace(/[^a-zA-Z0-9]/g,'').slice(0,8).toUpperCase(),now=this.now();return this.adapter.transaction(async tx=>{const p=requirePlayer(await tx.get(ppath(this.uid))),old=await tx.get(`tribes/${code}`);if(p.tribeId)throw Error('Leave your current tribe first.');if(p.activeDuel||p.activeRaid)throw Error('Finish your current battle first.');if(old)throw Error('That tribe code was taken. Try again.');const tribe={id:code,code,name,ownerId:this.uid,emblem:cleanEmblem(emblem),...(capabilities.tribeApplications?{admissionMode:admission(admissionMode),acceptingApplications:!!acceptingApplications}:{}),members:[this.uid],boss:{level:1,hp:bossHealth(1),maxHp:bossHealth(1),revision:0},createdAt:now,updatedAt:now};p.tribeId=code;touch(p,now);tx.set(`tribes/${code}`,tribe);tx.set(ppath(this.uid),p);return tribe;});}
 joinTribe(code){return this.requestTribe(String(code||'').trim().toUpperCase());}
 async requestTribe(code){cleanId(code);const capabilities=await this.initializeCompatibility(),now=this.now();return this.adapter.transaction(async tx=>{
  const p=requirePlayer(await tx.get(ppath(this.uid))),tribe=await tx.get(`tribes/${code}`),slots=[];
  if(capabilities.tribeApplications)for(let slot=0;slot<SOCIAL_CONFIG.maxApplications;slot++)slots.push(await tx.get(`applications/${applicationId(this.uid,slot)}`));
  if(!tribe)throw Error('No tribe has that code.');if(p.tribeId===code)return tribe;if(p.tribeId)throw Error('Leave your current tribe first.');if(p.activeDuel||p.activeRaid)throw Error('Finish your current battle first.');
  if(tribe.members.length>=SOCIAL_CONFIG.maxMembers)throw Error('This tribe already has ten members.');if(tribe.acceptingApplications===false)throw Error('This tribe has paused admissions.');
  const pending=slots.find(r=>r?.tribeId===code&&applicationStatus(r,now)==='pending');if(pending)return pending;
  if(admission(tribe.admissionMode)==='open'){tribe.members.push(this.uid);if(!tribe.ownerId)tribe.ownerId=this.uid;tribe.updatedAt=now;p.tribeId=code;touch(p,now);tx.set(`tribes/${code}`,tribe);tx.set(ppath(this.uid),p);return tribe;}
  if(!capabilities.tribeApplications)throw Error('This tribe requires approval, which is unavailable under these server rules.');
  const slot=slots.findIndex(r=>!r||applicationStatus(r,now)!=='pending');if(slot<0)throw Error('You can have three pending applications. Cancel one or wait for it to expire.');
  const id=applicationId(this.uid,slot),r={id,uid:this.uid,slot,tribeId:code,tribeName:tribe.name,name:p.name,status:'pending',createdAt:now,updatedAt:now,expiresAt:now+SOCIAL_CONFIG.applicationTTL,decidedBy:null};tx.set(`applications/${id}`,r);return r;
 });}
 async reviewApplication(id,accept){cleanId(id);if(!(await this.initializeCompatibility()).tribeApplications)throw Error('Tribe applications are unavailable under these server rules.');const now=this.now();return this.adapter.transaction(async tx=>{
  const r=await tx.get(`applications/${id}`);if(!r)throw Error('This application no longer exists.');
  const t=await tx.get(`tribes/${r.tribeId}`),p=requirePlayer(await tx.get(ppath(r.uid))),presence=await tx.get(`presence/${r.uid}`);
  if(t?.ownerId!==this.uid)throw Error('Only the tribe leader can review applications.');if(applicationStatus(r,now)!=='pending')throw Error('This application is no longer pending.');
  if(accept){if(p.tribeId)throw Error('This adventurer already joined a tribe.');if(p.activeDuel||p.activeRaid||online(presence,now)&&presence.busy)throw Error('This adventurer is in a battle. Try again after they return.');if(t.members.length>=SOCIAL_CONFIG.maxMembers)throw Error('This tribe already has ten members.');t.members.push(p.uid);t.lastApplication=id;t.updatedAt=now;p.tribeId=t.id;touch(p,now);tx.set(`tribes/${t.id}`,t);tx.set(ppath(p.uid),p);}
  r.status=accept?'accepted':'rejected';r.decidedBy=this.uid;r.updatedAt=now;tx.set(`applications/${id}`,r);return r;
 });}
 async cancelApplication(id){cleanId(id);if(!(await this.initializeCompatibility()).tribeApplications)throw Error('Tribe applications are unavailable under these server rules.');const now=this.now();return this.adapter.transaction(async tx=>{const r=await tx.get(`applications/${id}`);if(!r||r.uid!==this.uid||r.status!=='pending')throw Error('This application is no longer pending.');r.status=applicationStatus(r,now)==='expired'?'expired':'cancelled';r.updatedAt=now;r.decidedBy=this.uid;tx.set(`applications/${id}`,r);return r;});}
 async expireApplications(rows){if(!(await this.initializeCompatibility()).tribeApplications)return;for(const r of rows||[])if(r.status==='pending'&&r.expiresAt<=this.now())await this.adapter.transaction(async tx=>{const current=await tx.get(`applications/${cleanId(r.id)}`);if(current?.status==='pending'&&current.expiresAt<=this.now()){current.status='expired';current.updatedAt=this.now();current.decidedBy=this.uid;tx.set(`applications/${r.id}`,current);}});}
 watchApplications(fn,error){return this.adapter.watchQuery('applications',[['uid','==',this.uid]],fn,error);}
 watchTribeApplications(tribeId,fn,error){return this.adapter.watchQuery('applications',[['tribeId','==',cleanId(tribeId)]],fn,error);}
 async updateTribe({name,emblem,admissionMode,acceptingApplications}){const capabilities=await this.initializeCompatibility();if(!capabilities.tribeApplications&&(admissionMode!==undefined&&admission(admissionMode)!=='open'||acceptingApplications===false))throw Error('These server rules support open tribes. Approval and paused admissions need the newer tribe rules.');const now=this.now();return this.adapter.transaction(async tx=>{const p=requirePlayer(await tx.get(ppath(this.uid))),t=await tx.get(`tribes/${p.tribeId}`);if(!t||t.ownerId!==this.uid)throw Error('Only the tribe founder can change its banner.');if(name!==undefined)t.name=cleanName(name);if(emblem!==undefined)t.emblem=cleanEmblem(emblem);if(capabilities.tribeApplications&&admissionMode!==undefined)t.admissionMode=admission(admissionMode);if(capabilities.tribeApplications&&acceptingApplications!==undefined)t.acceptingApplications=!!acceptingApplications;t.updatedAt=now;tx.set(`tribes/${t.id}`,t);return t;});}
 async leaveTribe(){const capabilities=await this.initializeCompatibility(),pending=(await Promise.all(['from','to'].map(side=>this.adapter.list('offers',[[side,'==',this.uid],['status','==','pending']])))).flat();if(pending.length)throw Error('Cancel or resolve your pending trades before leaving.');const now=this.now();return this.adapter.transaction(async tx=>{const p=requirePlayer(await tx.get(ppath(this.uid))),lock=capabilities.tradeLocks?await tx.get(`tradeLocks/${this.uid}`):null;if(!p.tribeId)return;if(lock?.offers.length)throw Error('Cancel or resolve your pending trades before leaving.');if(p.activeDuel||p.activeRaid)throw Error('Finish your current battle first.');const t=await tx.get(`tribes/${p.tribeId}`);if(t){t.members=t.members.filter(x=>x!==this.uid);if(t.ownerId===this.uid)t.ownerId=t.members[0]||null;if(capabilities.tribeApplications&&!t.members.length){t.admissionMode='open';t.acceptingApplications=true;}t.updatedAt=now;tx.set(`tribes/${t.id}`,t);}p.tribeId=null;touch(p,now);tx.set(ppath(this.uid),p);});}
 async tradeLocks(tx,id,parties,add,now){if(!(await this.initializeCompatibility()).tradeLocks)return;for(const uid of parties){const lock=await tx.get(`tradeLocks/${uid}`)||{uid,offers:[]};lock.offers=lock.offers.filter(x=>x!==id);if(add){if(lock.offers.length>=SOCIAL_CONFIG.maxOffers)throw Error('Resolve existing trade offers before creating another.');lock.offers.push(id);}lock.lastOffer=id;lock.updatedAt=now;tx.set(`tradeLocks/${uid}`,lock);}}
 adventureIdentity(weekStart=tribeWeek(this.now())){return `${weekStart}-${this.uid}`;}
 async joinAdventure(counters){
  const now=this.now(),weekStart=tribeWeek(now),id=this.adventureIdentity(weekStart),clean=cleanContribution(counters);
  return this.adapter.transaction(async tx=>{
   const p=requirePlayer(await tx.get(ppath(this.uid)));if(!p.tribeId)throw Error('Join a tribe first.');
   const tribe=await tx.get(`tribes/${p.tribeId}`),existing=await tx.get(`adventureMembers/${id}`);
   if(!tribe?.members.includes(this.uid))throw Error('Your tribe membership changed. Reconnect and try again.');
   if(existing){if(existing.tribeId!==tribe.id)throw Error('This week’s adventure stays with your first tribe. Join another adventure next week.');return existing;}
   const summaryId=`${weekStart}-${tribe.id}`,summary=await tx.get(`adventures/${summaryId}`);
   const member={id,uid:this.uid,saveId:p.saveId,tribeId:tribe.id,summaryId,weekStart,weekEnd:weekStart+TRIBE_WEEK,baseline:clone(clean),counters:clone(clean),contribution:emptyContribution(),points:0,joinedAt:now,updatedAt:now};
   if(!summary)tx.set(`adventures/${summaryId}`,{id:summaryId,tribeId:tribe.id,weekStart,weekEnd:weekStart+TRIBE_WEEK,target:TRIBE_ADVENTURE.target,points:0,updatedAt:now});
   tx.set(`adventureMembers/${id}`,member);return member;
  });
 }
 async contributeAdventure(counters,weekStart=tribeWeek(this.now())){
  const capabilities=await this.initializeCompatibility(),now=this.now(),id=this.adventureIdentity(weekStart),incoming=cleanContribution(counters);
  if(![tribeWeek(now),tribeWeek(now)-TRIBE_WEEK].includes(weekStart))return null;
  return this.adapter.transaction(async tx=>{
   const member=await tx.get(`adventureMembers/${id}`);if(!member)return null;
   const p=requirePlayer(await tx.get(ppath(this.uid))),tribe=await tx.get(`tribes/${member.tribeId}`),summary=await tx.get(`adventures/${member.summaryId}`);
   if(p.tribeId!==member.tribeId||!tribe?.members.includes(this.uid)||!summary)return null;
   for(const key of TRIBE_ADVENTURE.keys)member.counters[key]=Math.max(member.counters[key],incoming[key]);
   const contribution=Object.fromEntries(TRIBE_ADVENTURE.keys.map(key=>[key,member.counters[key]-member.baseline[key]])),points=contributionPoints(contribution),delta=points-member.points;
   if(points===member.points&&JSON.stringify(member.contribution)===JSON.stringify(contribution))return member;
   summary.points+=delta;summary.updatedAt=now;member.contribution=contribution;member.points=points;member.updatedAt=now;
   tx.set(`adventures/${summary.id}`,summary);tx.set(`adventureMembers/${id}`,member);if(capabilities.publicActivity&&weekStart===tribeWeek(now))tx.set(`tribePublicActivity/${member.tribeId}`,{id:member.tribeId,weekStart:member.weekStart,points:summary.points,updatedAt:now});return member;
  });
 }
 async claimAdventure(weekStart=tribeWeek(this.now())){
  const now=this.now(),id=this.adventureIdentity(weekStart);
  return this.adapter.transaction(async tx=>{
   const previous=await tx.get(`adventureClaims/${id}`);if(previous)return previous;
   const member=await tx.get(`adventureMembers/${id}`),p=requirePlayer(await tx.get(ppath(this.uid)));
   if(!member||member.points<=0)throw Error('Contribute to this adventure before claiming its reward.');
   const tribe=await tx.get(`tribes/${member.tribeId}`),summary=await tx.get(`adventures/${member.summaryId}`);
   if(p.tribeId!==member.tribeId||!tribe?.members.includes(this.uid))throw Error('Return to the tribe you explored with before claiming.');
   if(!summary||summary.points<summary.target)throw Error('Your tribe is still working toward its shared goal.');
   if(now>=member.weekEnd+TRIBE_WEEK)throw Error('This adventure’s reward window has ended.');
   const claim={id,uid:this.uid,saveId:p.saveId,tribeId:member.tribeId,summaryId:member.summaryId,weekStart:member.weekStart,contribution:clone(member.contribution),reward:adventureReward(member.weekStart),claimedAt:now};
   tx.set(`adventureClaims/${id}`,claim);return claim;
  });
 }
 watchAdventure(tribeId,fn,error,weekStart=tribeWeek(this.now())){return this.adapter.watchDoc(`adventures/${weekStart}-${cleanId(tribeId)}`,fn,error);}
 watchAdventureMembers(tribeId,fn,error,weekStart=tribeWeek(this.now())){return this.adapter.watchQuery('adventureMembers',[['tribeId','==',tribeId],['weekStart','==',weekStart]],fn,error);}
 watchAdventureClaims(fn,error){return this.adapter.watchQuery('adventureClaims',[['uid','==',this.uid]],fn,error);}
 listTribes(){return this.adapter.list('tribes',[],30);}
 async listDirectory({search='',availableOnly=false}={}){const tribes=await this.adapter.list('tribes',[],null),ranks=new Map(rankTribes(tribes).map(t=>[t.id,t.rank])),term=String(search).trim().toLocaleLowerCase();this.tribeRanks=ranks;return tribes.map(t=>publicTribe(t,ranks.get(t.id))).filter(t=>(!term||t.name.toLocaleLowerCase().includes(term))&&(!availableOnly||t.acceptingApplications)).sort((a,b)=>a.rank-b.rank||a.name.localeCompare(b.name));}
 async getPublicTribe(id){const capabilities=await this.initializeCompatibility(),t=await this.adapter.get(`tribes/${cleanId(id)}`);if(!t)return null;const profiles=await Promise.all(t.members.map(uid=>this.getPlayer(uid))),activity=capabilities.publicActivity?await this.adapter.get(`tribePublicActivity/${id}`):null;return{tribe:{...publicTribe(t),weeklyContribution:capabilities.publicActivity?(activity?.weekStart===tribeWeek(this.now())?activity.points:0):null},members:profiles.filter(Boolean).map(p=>({uid:p.uid,name:p.name,team:clone(p.team),averageLevel:p.averageLevel}))};}
 async listTribeRankings(metric='bosses',ownId=null){
  if(metric==='bosses')return rankTribes(await this.adapter.list('tribes',[],TRIBE_RANKING_LIMIT,[['boss.level','desc']]));
  const tribes=await this.adapter.list('tribes',[],null),rows=tribes.map(t=>publicTribe(t));
  if(metric==='weekly'){if(!(await this.initializeCompatibility()).publicActivity)throw Error('Public weekly rankings are unavailable under these server rules. Your tribe adventure still works.');const activity=await this.adapter.list('tribePublicActivity',[['weekStart','==',tribeWeek(this.now())]],null),points=new Map(activity.map(a=>[a.id,a.points]));for(const row of rows)row.score=points.get(row.id)||0;}
  else if(metric==='power'){const players=await this.adapter.list('players',[],null),power=new Map();for(const p of players){const t=tribes.find(t=>t.id===p.tribeId);if(t?.members.includes(p.uid))power.set(p.tribeId,(power.get(p.tribeId)||0)+teamPower(p.team));}for(const row of rows)row.score=power.get(row.id)||0;}
  else throw Error('Unknown tribe ranking.');
  rows.sort((a,b)=>b.score-a.score||a.id.localeCompare(b.id));let rank=0,previous=null;const ranked=rows.map((r,i)=>{if(previous!==r.score)rank=i+1;previous=r.score;return{...r,rank,metric};}),top=ranked.slice(0,TRIBE_RANKING_LIMIT);if(ownId)top.own=ranked.find(row=>row.id===ownId)||null;return top;
 }
 async walletOp(kind,operationId,details,mutate){const id=cleanId(operationId||this.makeId()),now=this.now();return this.adapter.transaction(async tx=>{const prior=await tx.get(`operations/${id}`);if(prior){if(prior.actor!==this.uid||prior.kind!==kind||JSON.stringify(prior.details.request)!==JSON.stringify(details))throw Error('That operation ID is already in use.');return prior.details.result??prior;}const result=await mutate(tx,id,now),wallets={};for(const [uid,{before,after}]of Object.entries(result.wallets||{})){wallets[uid]={before:clone(before),after:clone(after)};const p=result.players[uid];p.lastOp=id;touch(p,now);tx.set(ppath(uid),p);}const op={id,kind,actor:this.uid,participants:result.participants||Object.keys(wallets),details:{...details,...result.details,request:details,wallets,receipts:Object.fromEntries((result.receipts||[]).map(r=>[r.uid,{type:r.type,title:r.title,payload:r.payload,walletCredited:r.walletCredited}])),...(result.returnValue?{result:result.returnValue}:{})},createdAt:now};tx.set(`operations/${id}`,op);for(const r of result.receipts||[])tx.set(receiptPath(r.uid,r.id),r);return result.returnValue??op;});}
 receipt(id,uid,type,title,value,sourceId,now,walletCredited=true){return{id,uid,type,title,payload:payload(value),walletCredited,sourceId,createdAt:now,seen:false};}
 deposit(assetName,amount,operationId){assetName=assetId(assetName);amount=count(amount);return this.walletOp('deposit',operationId,{uid:this.uid,asset:assetName,amount},async tx=>{const p=requirePlayer(await tx.get(ppath(this.uid))),before=clone(p.wallet);changeWallet(p,assetName,amount);return{players:{[this.uid]:p},wallets:{[this.uid]:{before,after:p.wallet}},details:{saveId:p.saveId}};});}
 withdraw(assetName,amount,operationId){assetName=assetId(assetName);amount=count(amount);return this.walletOp('withdraw',operationId,{uid:this.uid,asset:assetName,amount},async(tx,id,now)=>{const p=requirePlayer(await tx.get(ppath(this.uid))),before=clone(p.wallet);changeWallet(p,assetName,-amount);const value=assetName==='fruit'?{fruit:amount}:{items:{[assetName]:amount}};return{players:{[this.uid]:p},wallets:{[this.uid]:{before,after:p.wallet}},receipts:[this.receipt(id,this.uid,'withdraw','Supplies returned to your pack',value,id,now,false)]};});}
 gift(toId,assetName,amount){toId=cleanId(toId);assetName=assetId(assetName);amount=count(amount);if(toId===this.uid)throw Error('Choose another tribe member.');return this.walletOp('gift',null,{from:this.uid,to:toId,asset:assetName,amount},async(tx,id,now)=>{const a=requirePlayer(await tx.get(ppath(this.uid))),b=requirePlayer(await tx.get(ppath(toId))),t=await tx.get(`tribes/${a.tribeId}`);if(!t||a.tribeId!==b.tribeId||!t.members.includes(toId)||!t.members.includes(this.uid))throw Error('Gifts are for members of your tribe.');const aa=clone(a.wallet),bb=clone(b.wallet);changeWallet(a,assetName,-amount);changeWallet(b,assetName,amount);const value=assetName==='fruit'?{fruit:amount}:{items:{[assetName]:amount}};return{players:{[a.uid]:a,[b.uid]:b},wallets:{[a.uid]:{before:aa,after:a.wallet},[b.uid]:{before:bb,after:b.wallet}},receipts:[this.receipt(id,toId,'gift',`${a.name} sent you supplies`,value,id,now)]};});}
 offerTrade(toId,give,want){toId=cleanId(toId);give=asset(give);want=asset(want);if(toId===this.uid)throw Error('Choose another tribe member.');const id=this.makeId();return this.walletOp('trade-offer',id,{offerId:id,from:this.uid,to:toId,give,want},async(tx,opId,now)=>{const a=requirePlayer(await tx.get(ppath(this.uid))),b=requirePlayer(await tx.get(ppath(toId))),t=await tx.get(`tribes/${a.tribeId}`);if(!t||a.tribeId!==b.tribeId||!t.members.includes(toId))throw Error('Trade with members of your tribe.');const before=clone(a.wallet);changeWallet(a,give.asset,-give.amount);const offer={id,from:this.uid,to:toId,tribeId:t.id,give,want,status:'pending',createdAt:now,updatedAt:now};await this.tradeLocks(tx,id,[a.uid,b.uid],true,now);tx.set(`offers/${id}`,offer);return{players:{[a.uid]:a},wallets:{[a.uid]:{before,after:a.wallet}},participants:[a.uid,b.uid],returnValue:offer};});}
 acceptTrade(id){id=cleanId(id);return this.walletOp('trade',`accept-${id}`,{offerId:id},async(tx,opId,now)=>{const o=await tx.get(`offers/${id}`);if(!o||o.to!==this.uid||o.status!=='pending')throw Error('That trade is no longer available.');const a=requirePlayer(await tx.get(ppath(o.from))),b=requirePlayer(await tx.get(ppath(o.to))),t=await tx.get(`tribes/${o.tribeId}`);if(!t||a.tribeId!==o.tribeId||b.tribeId!==o.tribeId)throw Error('Both players must still belong to this tribe.');const aa=clone(a.wallet),bb=clone(b.wallet);changeWallet(b,o.want.asset,-o.want.amount);changeWallet(a,o.want.asset,o.want.amount);changeWallet(b,o.give.asset,o.give.amount);o.status='accepted';o.updatedAt=now;await this.tradeLocks(tx,id,[a.uid,b.uid],false,now);tx.set(`offers/${id}`,o);return{players:{[a.uid]:a,[b.uid]:b},wallets:{[a.uid]:{before:aa,after:a.wallet},[b.uid]:{before:bb,after:b.wallet}},details:{from:o.from,to:o.to,give:o.give,want:o.want},receipts:[this.receipt(opId,a.uid,'trade','Tribe trade complete',o.want.asset==='fruit'?{fruit:o.want.amount}:{items:{[o.want.asset]:o.want.amount}},opId,now),this.receipt(opId,b.uid,'trade','Tribe trade complete',o.give.asset==='fruit'?{fruit:o.give.amount}:{items:{[o.give.asset]:o.give.amount}},opId,now)]};});}
 cancelTrade(id){id=cleanId(id);return this.walletOp('trade-cancel',`cancel-${id}`,{offerId:id},async(tx,opId,now)=>{const o=await tx.get(`offers/${id}`);if(!o||![o.from,o.to].includes(this.uid)||o.status!=='pending')throw Error('That trade is no longer available.');const a=requirePlayer(await tx.get(ppath(o.from))),before=clone(a.wallet);changeWallet(a,o.give.asset,o.give.amount);o.status='cancelled';o.updatedAt=now;await this.tradeLocks(tx,id,[o.from,o.to],false,now);tx.set(`offers/${id}`,o);return{players:{[a.uid]:a},wallets:{[a.uid]:{before,after:a.wallet}},participants:[o.from,o.to],details:{from:o.from,to:o.to,give:o.give,want:o.want}};});}
 async startRaid(){const id=this.makeId(),now=this.now();return this.adapter.transaction(async tx=>{const p=requirePlayer(await tx.get(ppath(this.uid))),presence=await tx.get(`presence/${this.uid}`),t=await tx.get(`tribes/${p.tribeId}`);requireFree(p,presence,now);if(!t||!t.members.includes(this.uid))throw Error('Join a tribe before fighting its boss.');if(p.lastRaidAt&&now-p.lastRaidAt<SOCIAL_CONFIG.raidCooldown)throw Error('Your next tribe boss attempt unlocks one hour after the previous attempt.');const attempt={id,version:SOCIAL_COMBAT_VERSION,patternVersion:1,uid:this.uid,tribeId:t.id,bossLevel:t.boss.level,team:clone(p.team),seed:Math.floor(now%4294967295)||1,startedAt:now,submitted:false,damage:0,actions:[],completedAt:null};p.activeRaid=id;p.lastRaidAt=now;touch(p,now);tx.set(ppath(this.uid),p);tx.set(`raids/${id}`,attempt);return attempt;});}
 getRaid(id){return this.adapter.get(`raids/${cleanId(id)}`);}
 async abandonRaid(id){if(!id)id=(await this.getPlayer())?.activeRaid;id=cleanId(id);const now=this.now();return this.adapter.transaction(async tx=>{const p=requirePlayer(await tx.get(ppath(this.uid))),a=await tx.get(`raids/${id}`);if(!a||a.uid!==this.uid||p.activeRaid!==id)throw Error('That attempt is not active.');p.activeRaid=null;touch(p,now);a.submitted=true;a.completedAt=now;tx.set(ppath(this.uid),p);tx.set(`raids/${id}`,a);return a;});}
 async submitRaid(id,actions){id=cleanId(id);if(!Array.isArray(actions)||actions.length!==10)throw Error('Complete all ten turns before submitting.');return this.walletOp('raid',`raid-${id}`,{attemptId:id},async(tx,opId,now)=>{
   const a=await tx.get(`raids/${id}`);if(!a||a.uid!==this.uid)throw Error('Unknown tribe boss attempt.');if(a.submitted)throw Error('This attempt was already submitted.');const p=requirePlayer(await tx.get(ppath(this.uid))),t=await tx.get(`tribes/${a.tribeId}`);if(p.activeRaid!==id||!t||p.tribeId!==a.tribeId)throw Error('This tribe boss attempt is no longer active.');
   const attempt=newRaidAttempt(a.team,a.bossLevel,a.seed,a.version??1,a.patternVersion||0);for(const action of actions)raidAction(attempt,action);if(attempt.status!=='finished')throw Error('Complete all ten turns first.');
   const players={[p.uid]:p},wallets={},receipts=[];p.activeRaid=null;a.actions=clone(actions);a.submitted=true;a.damage=attempt.totalDamage;a.completedAt=now;
   let defeated=false,reward=null,damageApplied=0;
   if(t.boss.level===a.bossLevel){damageApplied=Math.min(t.boss.hp,attempt.totalDamage);t.boss.hp-=damageApplied;t.boss.revision++;t.updatedAt=now;
    if(t.boss.hp<=0){defeated=true;reward=bossReward(a.bossLevel);for(const uid of t.members){let member=players[uid]||await tx.get(ppath(uid));if(!member)continue;players[uid]=member;const before=clone(member.wallet);member.wallet.fruit+=reward.fruit;for(const [item,n]of Object.entries(reward.items||{}))changeWallet(member,item,n);wallets[uid]={before,after:member.wallet};receipts.push(this.receipt(opId,uid,'raid',`Your tribe defeated boss ${a.bossLevel}`,{...reward,teamIds:member.team.map(m=>m.id)},opId,now));}t.boss={level:a.bossLevel+1,hp:bossHealth(a.bossLevel+1),maxHp:bossHealth(a.bossLevel+1),revision:t.boss.revision};}
    tx.set(`tribes/${t.id}`,t);
   }
   if(!wallets[p.uid]){touch(p,now);tx.set(ppath(p.uid),p);}
   tx.set(`raids/${id}`,a);return{players,wallets,participants:t.members,details:{tribeId:t.id,level:a.bossLevel,damage:attempt.totalDamage,damageApplied,defeated,reward},receipts,returnValue:{attempt:a,damageApplied,defeated,reward,boss:t.boss}};
  });}
 async challenge(toId){toId=cleanId(toId);if(toId===this.uid)throw Error('Choose another player.');const id=this.makeId(),now=this.now();return this.adapter.transaction(async tx=>{const a=requirePlayer(await tx.get(ppath(this.uid))),b=requirePlayer(await tx.get(ppath(toId))),pa=await tx.get(`presence/${this.uid}`),pb=await tx.get(`presence/${toId}`);requireFree(a,pa,now);requireFree(b,pb,now);const same=a.tribeId&&a.tribeId===b.tribeId,t=same?await tx.get(`tribes/${a.tribeId}`):null,tribe=!!t&&t.members.includes(a.uid)&&t.members.includes(b.uid),nearby=nearbyPlayers([pa,pb],this.uid,now).some(p=>p.uid===toId);if(!tribe&&!nearby)throw Error('Challenge an online tribe member or an opted-in player within one mile.');const c={id,from:this.uid,to:toId,eligibility:tribe?'tribe':'nearby',status:'pending',expiresAt:now+SOCIAL_CONFIG.challengeTTL,createdAt:now,updatedAt:now};tx.set(`challenges/${id}`,c);return c;});}
 acceptChallenge(id){id=cleanId(id);return this.walletOp('duel-start',`start-${id}`,{challengeId:id},async(tx,opId,now)=>{
   const c=await tx.get(`challenges/${id}`);if(!c||c.to!==this.uid||c.status!=='pending'||c.expiresAt<=now)throw Error('This challenge expired or was already answered.');
   const a=requirePlayer(await tx.get(ppath(c.from))),b=requirePlayer(await tx.get(ppath(c.to))),pa=await tx.get(`presence/${a.uid}`),pb=await tx.get(`presence/${b.uid}`);requireFree(a,pa,now);requireFree(b,pb,now);const same=a.tribeId&&a.tribeId===b.tribeId,t=same?await tx.get(`tribes/${a.tribeId}`):null,tribe=!!t&&t.members.includes(a.uid)&&t.members.includes(b.uid),nearby=nearbyPlayers([pa,pb],a.uid,now).some(p=>p.uid===b.uid);if(!tribe&&!nearby)throw Error('The challenger is no longer an eligible online player.');
   const initialProfiles=[a,b].map(p=>({id:p.uid,name:p.name,team:clone(p.team)})),seed=Math.floor(now%4294967295)||1,state=newDuel(initialProfiles[0],initialProfiles[1],seed),escrow={},wallets={};for(const p of[a,b]){const before=clone(p.wallet),stake=Math.min(p.wallet.fruit,duelStake(p.team));escrow[p.uid]=stake;changeWallet(p,'fruit',-stake);wallets[p.uid]={before,after:p.wallet};p.activeDuel=id;}
   const duel={id,players:[a.uid,b.uid],initialProfiles,seed,state,actions:[],revision:0,createdAt:now,updatedAt:now,escrow,settled:false,status:'active'};c.status='accepted';c.updatedAt=now;tx.set(`challenges/${id}`,c);tx.set(`duels/${id}`,duel);return{players:{[a.uid]:a,[b.uid]:b},wallets,details:{duelId:id,escrow},returnValue:duel};
  });}
 async declineChallenge(id){id=cleanId(id);const now=this.now();return this.adapter.transaction(async tx=>{const c=await tx.get(`challenges/${id}`);if(!c||![c.from,c.to].includes(this.uid)||c.status!=='pending')throw Error('This challenge is no longer pending.');c.status='declined';c.updatedAt=now;tx.set(`challenges/${id}`,c);return c;});}
 async act(id,action,expectedSequence){id=cleanId(id);const now=this.now();return this.adapter.transaction(async tx=>{
   const d=await tx.get(`duels/${id}`);if(!d||!d.players.includes(this.uid))throw Error('This battle belongs to other players.');if(d.status!=='active')throw Error('This battle has finished.');if(d.revision!==expectedSequence)throw Error('A newer turn arrived. Wait for the synchronized battle.');
   const state=newDuel(d.initialProfiles[0],d.initialProfiles[1],d.seed,d.state?.version??1);for(const entry of d.actions)duelAction(state,entry.uid,entry.action);duelAction(state,this.uid,action,expectedSequence);d.actions.push({uid:this.uid,action:clone(action)});d.state=state;d.revision++;d.updatedAt=now;
   if(state.status==='finished')await this.settleDuel(tx,d,now);else tx.set(`duels/${id}`,d);return d;
  });}
 async settleDuel(tx,d,now){
   const opId=`duel-${d.id}`,prior=await tx.get(`operations/${opId}`);if(prior)return;const players={};for(const uid of d.players)players[uid]=requirePlayer(await tx.get(ppath(uid)));const rewards=duelRewards(d.state),wallets={},receipts=[],winner=d.state.winner;
   for(const uid of d.players){const p=players[uid],before=clone(p.wallet),payout=winner===null?d.escrow[uid]:winner===uid?Object.values(d.escrow).reduce((n,x)=>n+x,0):0;p.wallet.fruit+=payout;p.lastOp=opId;touch(p,now);wallets[uid]={before,after:clone(p.wallet)};const xp=rewards[uid]?.xp??12;receipts.push(this.receipt(opId,uid,'duel',winner===null?'Friendly battle drawn':winner===uid?'You won your friendly battle':'Friendly battle complete',{fruit:winner===uid?d.escrow[d.players.find(x=>x!==uid)]:0,xp,teamIds:d.initialProfiles.find(p=>p.id===uid).team.map(m=>m.id),payoutFruit:payout},opId,now));tx.set(ppath(uid),p);}
   d.status='finished';d.settled=true;const op={id:opId,kind:'duel',actor:this.uid,participants:d.players,details:{duelId:d.id,winner,escrow:d.escrow,rewards:clone(rewards),wallets,receipts:Object.fromEntries(receipts.map(r=>[r.uid,{type:r.type,title:r.title,payload:r.payload,walletCredited:r.walletCredited}]))},createdAt:now};tx.set(`operations/${opId}`,op);for(const r of receipts)tx.set(receiptPath(r.uid,r.id),r);tx.set(`duels/${d.id}`,d);
  }
 async surrender(id,expectedSequence){const d=await this.adapter.get(`duels/${cleanId(id)}`);return this.act(id,{type:'surrender'},expectedSequence??d?.revision);}
 async claimTimeout(id){id=cleanId(id);const now=this.now();return this.adapter.transaction(async tx=>{
   const d=await tx.get(`duels/${id}`);if(!d||!d.players.includes(this.uid)||d.status!=='active')throw Error('No active friendly battle.');
   if(now-d.updatedAt<90000)throw Error('The current player still has time to choose.');const state=newDuel(d.initialProfiles[0],d.initialProfiles[1],d.seed,d.state?.version??1);for(const e of d.actions)duelAction(state,e.uid,e.action);
   const absent=d.players[state.turn];if(absent===this.uid)throw Error('Choose an action or surrender your own turn.');
   const action={type:'surrender'};duelAction(state,absent,action);d.actions.push({uid:absent,action,timeout:true,claimedBy:this.uid});d.state=state;d.revision++;d.updatedAt=now;await this.settleDuel(tx,d,now);return d;
  });}
 async returnToTrail(){const now=this.now();return this.adapter.transaction(async tx=>{const p=requirePlayer(await tx.get(ppath(this.uid)));if(p.activeDuel){const d=await tx.get(`duels/${p.activeDuel}`);if(d?.status==='active')throw Error('Finish or surrender your friendly battle first.');p.activeDuel=null;touch(p,now);tx.set(ppath(this.uid),p);}return p;});}
 async acknowledgeReceipt(id){id=cleanId(id);return this.adapter.transaction(async tx=>{const r=await tx.get(receiptPath(this.uid,id));if(!r)return;r.seen=true;tx.set(receiptPath(this.uid,id),r);return r;});}
 watchPlayer(id,fn,error){return this.adapter.watchDoc(ppath(id),fn,error);}
 watchTribe(code,fn,error){return this.adapter.watchDoc(`tribes/${cleanId(code)}`,fn,error);}
 watchDuel(id,fn,error){return this.adapter.watchDoc(`duels/${cleanId(id)}`,fn,error);}
 watchDuels(fn,error){return this.adapter.watchQuery('duels',[['players','array-contains',this.uid]],fn,error);}
 watchPresence(fn,error){return this.adapter.watchQuery('presence',[['online','==',true]],fn,error);}
 watchReceipts(fn,error){return this.adapter.watchQuery(`receipts/${this.uid}/entries`,[['seen','==',false]],fn,error);}
 watchChallenges(fn,error){return this.watchTwoSides('challenges','from','to',fn,error);}
 watchOffers(fn,error){return this.watchTwoSides('offers','from','to',fn,error);}
 watchTwoSides(collection,a,b,fn,error){let left=[],right=[];const emit=()=>fn([...new Map([...left,...right].map(x=>[x.id,x])).values()]);const u=this.adapter.watchQuery(collection,[[a,'==',this.uid]],v=>{left=v;emit();},error),v=this.adapter.watchQuery(collection,[[b,'==',this.uid]],v=>{right=v;emit();},error);return()=>{u();v();};}
}

// Firestore disallows arrays directly inside arrays. Preserve the canonical combat
// shape using tagged maps only at those array boundaries, then decode on reads.
export function encodeSocialWire(value,inArray=false){
 if(Array.isArray(value)){const list=value.map(v=>encodeSocialWire(v,true));return inArray?{__socialArray:list}:list;}
 if(value&&typeof value==='object')return Object.fromEntries(Object.entries(value).map(([key,v])=>[key,encodeSocialWire(v,false)]));
 return value;
}
export function decodeSocialWire(value){
 if(Array.isArray(value))return value.map(decodeSocialWire);
 if(value&&typeof value==='object'){
  if(Object.keys(value).length===1&&Array.isArray(value.__socialArray))return value.__socialArray.map(decodeSocialWire);
  return Object.fromEntries(Object.entries(value).map(([key,v])=>[key,decodeSocialWire(v)]));
 }return value;
}

export function createFirestoreSocialAdapter({db,f}){
 const ref=path=>f.doc(db,...SOCIAL_ROOT,...path.split('/')),collection=path=>f.collection(db,...SOCIAL_ROOT,...path.split('/'));
 const query=(path,filters=[],limit=100,order=[])=>f.query(collection(path),...filters.map(([key,op,value])=>f.where(key,op,value)),...order.map(([key,direction])=>f.orderBy(key,direction)),...(limit==null?[]:[f.limit(limit)]));
 const rows=s=>s.docs.map(d=>({id:d.id,...decodeSocialWire(d.data())}));
 return{
  async get(path){const d=await f.getDocFromServer(ref(path));return d.exists()?decodeSocialWire(d.data()):null;},
  async list(path,filters=[],limit=100,order=[]){return rows(await f.getDocs(query(path,filters,limit,order)));},
  transaction(callback){return f.runTransaction(db,async native=>{const writes=[];const result=await callback({async get(path){const d=await native.get(ref(path));return d.exists()?decodeSocialWire(d.data()):null;},set(path,value){writes.push(['set',path,clone(value)]);},delete(path){writes.push(['delete',path]);}});for(const[type,path,value]of writes)if(type==='set')native.set(ref(path),encodeSocialWire(value));else native.delete(ref(path));return result;},{maxAttempts:5});},
  watchDoc(path,fn,error){return f.onSnapshot(ref(path),d=>fn(d.exists()?decodeSocialWire(d.data()):null),error);},
  watchQuery(path,filters,fn,error,limit=null){return f.onSnapshot(query(path,filters,limit),s=>fn(rows(s)),error);}
 };
}
// Deterministic serializable adapter used by integration tests and the local two-browser fixture.
export function createMemorySocialAdapter(seed={}){
 let docs=new Map(Object.entries(clone(seed))),tail=Promise.resolve();const listeners=new Set();
 const matches=(d,filters)=>filters.every(([k,op,v])=>op==='array-contains'?Array.isArray(d[k])&&d[k].includes(v):op==='=='?d[k]===v:op==='>'?d[k]>v:op==='<'?d[k]<v:false);
 const field=(value,path)=>path.split('.').reduce((row,key)=>row?.[key],value),compare=(a,b)=>a<b?-1:a>b?1:0;
 const get=async path=>clone(docs.get(path)||null),list=async(path,filters=[],limit=100,order=[])=>{
  const rows=[...docs].filter(([p,d])=>p.startsWith(path+'/')&&p.slice(path.length+1).indexOf('/')<0&&matches(d,filters)&&order.every(([key])=>field(d,key)!==undefined));
  if(order.length)rows.sort(([ap,a],[bp,b])=>{for(const[key,direction]of order){const delta=compare(field(a,key),field(b,key))*(direction==='desc'?-1:1);if(delta)return delta;}return compare(ap,bp)*(order.at(-1)[1]==='desc'?-1:1);});
  return rows.slice(0,limit==null?undefined:limit).map(([p,d])=>({id:p.split('/').at(-1),...clone(d)}));
 };
 const emit=()=>{for(const fn of listeners)queueMicrotask(fn);};
 return{get,list,async transaction(callback){const work=tail.then(async()=>{const next=new Map([...docs].map(([p,d])=>[p,clone(d)]));const result=await callback({get:async p=>clone(next.get(p)||null),set:(p,d)=>next.set(p,clone(d)),delete:p=>next.delete(p)});docs=next;emit();return clone(result);});tail=work.catch(()=>{});return work;},watchDoc(path,fn){let active=true;const run=()=>active&&get(path).then(fn);listeners.add(run);run();return()=>{active=false;listeners.delete(run);};},watchQuery(path,filters,fn,error,limit=null){let active=true;const run=()=>active&&list(path,filters,limit).then(fn);listeners.add(run);run();return()=>{active=false;listeners.delete(run);};},dump:()=>Object.fromEntries([...docs].map(([p,d])=>[p,clone(d)]))};
}

