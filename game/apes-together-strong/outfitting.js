/* Versioned body/weapon/shield/utility slots and shared, resource-backed batches.
 * Existing equipment jobs and royal armor levels remain valid and unchanged. */
(() => {
'use strict';
const G=ATSGame.prototype,E=ATSEquipment.prototype,S=ATSSiege.prototype;
const ALL=['gorilla','chimpanzee','orangutan','gibbon','mandrill','capuchin'],LIGHT=['chimpanzee','gibbon','capuchin'],HEAVY=['gorilla','orangutan'];
const clamp=(n,a,b)=>Math.max(a,Math.min(b,n)),dist=(a,b)=>Math.hypot(a.x-b.x,a.y-b.y),ITEMS={};
function item(id,label,slot,wood,food,seconds,data={}){ITEMS[id]=Object.freeze({id,label,slot,wood,food,seconds,population:0,species:ALL,workshop:false,...data})}
function armor(id,label,reduction,population,wood,food,grade,speed=1,stealth=1,data={}){item(id,label,'body',wood,food,4+grade*2,{reduction,population,grade,speed,stealth,workshop:population>=60,...data})}
armor('vine','Vine-Wrapped Padding',.04,0,5,3,1);
armor('leaf','Leafweave Vest',.06,0,8,5,1,1,.94);
armor('bark','Basic Bark Armor',.09,0,12,7,1,.98,1.05);
armor('hardwood','Hardwood Chestguard',.13,25,18,10,2,.97,1.08);
armor('shoulders','Reinforced Shoulder Plates',.15,40,22,12,2,.96,1.12);
armor('shell','Shell-Plate Harness',.18,60,28,16,2,.93,1.17,{climb:.9});
armor('resin','Resinwood Battle Armor',.17,80,32,19,2,.99,1.05,{climb:1.05});
armor('ironwood','Ironwood Warplate',.24,100,40,26,3,.88,1.28,{climb:.8});
armor('royalHeavy','Royal Heavy Battle Armor',.28,200,55,36,3,.84,1.4,{climb:.75});
armor('climbPadding','Lightweight Climbing Protection',.08,25,16,12,1,1,.95,{climb:1.12});
armor('elite','Elite Champion Armor',.30,0,50,30,4,.88,1.3,{rare:2,workshop:true});
armor('warlordPlate','Ironback Warlord Plate',.30,0,54,34,4,.85,1.35,{rare:2,workshop:true,breach:1.1});
armor('phantomWraps','Shadowhand Master Infiltrator Wraps',.21,0,50,34,4,1,.78,{rare:2,workshop:true,climb:1.12});
armor('mastercraft','Emberwood Mastercraft Harness',.27,0,50,34,4,.96,1.12,{rare:2,workshop:true,build:1.1});
item('wovenRoyal','Woven Royal Guard','royal',24,28,14,{king:true,grade:1,maxHp:185,damage:1.04,reduction:.04});
item('timberRoyal','Timber Crown Harness','royal',45,55,18,{king:true,grade:2,population:25,maxHp:210,damage:1.08,reduction:.06});
item('club','Wooden Club','weapon',5,3,4,{damage:1.12,art:'baton'});
item('sling','Stone Sling','weapon',7,4,4,{species:['capuchin'],ranged:1.25,art:'sling'});
item('knuckles','Hardwood Knuckle Wraps','weapon',9,6,5,{species:HEAVY,damage:1.15,art:'cuffs'});
item('cuffs','Reinforced Fist Cuffs','weapon',36,30,22,{species:['gorilla'],population:100,workshop:true,legacy:true,art:'cuffs'});
item('warSpear','Wooden War Spear','weapon',15,10,7,{species:['chimpanzee','mandrill','orangutan'],population:25,damage:1.1,reach:13,art:'spear'});
item('spear','Throwing Spear Bundle','weapon',28,24,18,{species:['capuchin'],population:100,workshop:true,legacy:true,art:'spear'});
item('maul','Heavy Gate Maul','weapon',32,20,10,{species:['gorilla'],population:60,workshop:true,breach:1.35,speed:.94,art:'maul'});
item('staff','Reinforced Staff','weapon',14,10,6,{species:['chimpanzee','mandrill'],population:25,damage:1.18,reach:7,art:'staff'});
item('torch','Raider Torch','weapon',24,28,16,{species:LIGHT,population:100,workshop:true,legacy:true,art:'torch'});
item('cutters','Saboteur Cutters','weapon',20,16,8,{species:['chimpanzee','capuchin'],population:40,sabotage:1.25,art:'cutters'});
item('hook','Climbing Hook','weapon',12,10,6,{species:LIGHT,population:25,climb:1.15,art:'hook'});
item('gauntlets','Heavy Battle Gauntlets','weapon',42,30,12,{species:HEAVY,population:200,workshop:true,damage:1.45,art:'chainWrap'});
item('siegeGauntlets','Rare Siege Gauntlets','weapon',42,30,12,{species:HEAVY,rare:2,workshop:true,damage:1.35,breach:1.25,art:'chainWrap'});
item('buckler','Bark Buckler','shield',9,5,5,{species:['gorilla','orangutan','chimpanzee','mandrill'],shieldHp:85,art:'buckler'});
item('log','Reinforced Log Shield','shield',22,14,8,{species:HEAVY,population:25,shieldHp:240,speed:.94,art:'logShield'});
item('ironShield','Ironwood Shield','shield',35,25,12,{species:HEAVY,population:100,workshop:true,shieldHp:330,speed:.9,art:'ironShield'});
item('ancientShield','Ancient Ironwood Shield','shield',40,30,14,{species:HEAVY,rare:2,workshop:true,shieldHp:400,speed:.9,art:'ancientShield'});
item('eliteShield','Elite Shield Harness','shield',40,30,12,{species:HEAVY,rare:2,workshop:true,shieldHp:360,speed:.96,art:'ancientShield'});
item('camo','Camouflage Wraps','utility',12,9,5,{species:LIGHT,stealth:.84,art:'scoutSash'});
item('scout','Scout Pouches','utility',8,6,4,{carry:20,art:'satchel'});
item('silent','Silent Foot Wraps','utility',10,8,5,{species:LIGHT,stealth:.9,climb:1.06,art:'utilityBelt'});
item('rope','Rope Harness','utility',12,9,6,{species:LIGHT,climb:1.15,art:'ropePack'});
item('medical','Medical Supply Pack','utility',22,20,10,{population:40,medical:true,art:'medkit'});
item('builder','Builder Tool Rig','utility',18,12,8,{build:1.2,art:'toolRig'});
item('lumber','Lumber Carrying Harness','utility',16,10,7,{carry:24,lumber:1.15,art:'ropePack'});
item('drum','Command Drum','utility',20,18,8,{species:['mandrill'],population:40,rally:2,art:'drum'});
item('sabotage','Sabotage Kit','utility',22,16,8,{species:LIGHT,population:40,sabotage:1.2,art:'jammer'});
item('climbGear','Reinforced Climbing Gear','utility',26,20,10,{species:LIGHT,population:60,workshop:true,climb:1.25,art:'hook'});
item('mantle','Veteran War Mantle','utility',24,18,9,{population:80,guard:.04,art:'warMantle'});
item('royalMantle','Reinforced Royal Battle Mantle','utility',32,24,12,{rare:2,workshop:true,guard:.06,art:'warMantle'});
item('eliteShoulders','Elite Champion Shoulder Guards','utility',30,24,12,{rare:2,workshop:true,guard:.05,art:'shoulderPlate'});
const PRESETS=Object.freeze({heavy:{name:'Heavy Assault',items:['ironwood','maul','ironShield','mantle']},shieldwall:{name:'Shieldwall',items:['shell','knuckles','log','mantle']},scout:{name:'Scout',items:['leaf','sling','scout']},infiltrator:{name:'Infiltrator',items:['climbPadding','cutters','rope']},guard:{name:'Village Guard',items:['bark','club','buckler','builder']}});
const SLOTS=['body','weapon','shield','utility'];
const orderStamp=a=>[a?.armyOrder?.group||'',a?.recallOrder?.id||'',a?.nearestOrder?.until||'',a?.divisionId||'',a?.state||''].join(':');
const modifierCache=new WeakMap(),spec=(a,slot)=>ITEMS[a.outfit?.[slot]];
G.outfitModifiers=function(a){
 const outfit=a.outfit;if(!outfit)return null;const old=modifierCache.get(outfit);if(old&&old.body===outfit.body&&old.weapon===outfit.weapon&&old.shield===outfit.shield&&old.utility===outfit.utility&&old.liveShield===(a.shield?.hp>0))return old;
 const m={body:outfit.body,weapon:outfit.weapon,shield:outfit.shield,utility:outfit.utility,liveShield:a.shield?.hp>0,reduction:0,damage:1,ranged:1,breach:1,speed:1,stealth:1,climb:1,sabotage:1,build:1,lumber:1,carry:16,reach:0,guard:0,rally:0};
 for(const slot of SLOTS){const it=spec(a,slot);if(!it||slot==='shield'&&!(a.shield?.hp>0))continue;for(const k of ['damage','ranged','breach','speed','stealth','climb','sabotage','build','lumber'])m[k]*=it[k]||1;for(const k of ['reduction','reach','guard','rally'])m[k]+=it[k]||0;m.carry=Math.max(m.carry,it.carry||16)}
 m.reduction=Math.min(.33,1-(1-Math.min(.3,m.reduction))*(1-m.guard));m.damage=Math.min(1.5,m.damage);m.sabotage=Math.min(1.5,m.sabotage);m.climb=Math.min(1.5,m.climb);m.build=Math.min(1.3,m.build);modifierCache.set(outfit,m);return m;
};
G.outfitCarry=function(a){return this.outfitModifiers(a)?.carry||16};
E.initOutfitting=function(){if(!this.batches){this.batches=[];this.loadouts={};this.rareMaterials=0;this.outfitSerial=0;this.fitting=new Map();this.medics=new Set();this.outfitStats={assigned:0,equipped:0,active:0}}};
E.stations=function(){const result=this.workshops().map(p=>({...p,camp:false}));for(const s of this.g.settlements)if(s.population>0&&s.lodge?.hp!==0)result.push({s,w:{id:s.id+':camp',x:s.x,y:s.y,hp:s.lodge?.hp||100,kind:'camp'},camp:true});return result};
E.station=function(settlementId,id){const s=this.g.settlement(settlementId);if(!s)return null;if(id===s.id+':camp')return s.population>0&&s.lodge?.hp!==0?{s,w:{id,x:s.x,y:s.y,kind:'camp'},camp:true}:null;return this.workshop(settlementId,id)};
E.outfitRecipients=function(scope='selected',s,value){
 const g=this.g;let list=scope==='king'?[g.king]:scope==='selected'?g.siege.selectedMembers():scope==='division'?(g.champions.divisions[value]?.memberIds||[]).map(id=>g.apesById.get(id)):scope==='species'?g.apes.filter(a=>a.species===value):g.apes.filter(a=>a.settlementId===s?.id&&a.job==='guardian');
 return list.filter(a=>a&&a.hp>0&&a.state!=='young'&&!['free','idle'].includes(a.state)&&!a.recoveryRemaining&&!a.kingdomMission&&!a.expeditionCargo).sort((a,b)=>dist(a,s||g.king)-dist(b,s||g.king));
};
E.outfitAvailable=function(it,station){this.initOutfitting();if(!station)return'Choose a camp or workshop.';if(dist(this.g.king,station.w)>100)return'Visit this crafting station.';if(station.s.attack)return'Crafting pauses while the village is under attack.';if(it.workshop&&station.camp)return'Requires a completed workshop.';if(this.g.population<it.population)return'Unlocks at '+it.population+' apes.';if(it.rare&&this.rareMaterials<it.rare)return'Requires '+it.rare+' rare heartwood from prison escorts.';return''};
E.outfitEligible=function(a,it){
 if(!a||a.hp<=0||a.state==='young'||a.recoveryRemaining||a.kingdomMission||a.expeditionCargo)return false;
 if(it.king)return a.id==='king'&&!this.armorLevel&&(a.outfit?.royal!==it.id)&&(!a.outfit?.royal||ITEMS[a.outfit.royal]?.maxHp<it.maxHp);
 if(a.id==='king'||['free','idle'].includes(a.state)||!it.species.includes(a.species))return false;
 if(a.outfit?.[it.slot]===it.id&&!(it.slot==='shield'&&(a.shield?.hp||0)<it.shieldHp)||it.legacy&&a.equipment?.[it.id])return false;
 if(it.slot==='shield'&&a.shield?.hp>0&&a.shield.maxHp>=it.shieldHp&&!a.outfit?.shield)return false;
 return true;
};
E.outfitCatalog=function(settlementId,stationId){this.initOutfitting();const station=this.station(settlementId,stationId);return Object.values(ITEMS).map(it=>({...it,reason:this.outfitAvailable(it,station),equipped:this.g.apes.reduce((n,a)=>n+(a.hp>0&&(a.outfit?.[it.slot]===it.id||it.legacy&&!!a.equipment?.[it.id])?1:0),0),queued:this.batches.filter(b=>b.item===it.id).reduce((n,b)=>n+b.ids.length-b.index,0)}))};
E.orderBatch=function(settlementId,stationId,itemId,ids,quantity=1){
 this.initOutfitting();const it=ITEMS[itemId],station=this.station(settlementId,stationId),reason=it?this.outfitAvailable(it,station):'Unknown equipment.';
 if(reason){this.status=reason;return{ok:false,reason}}
 if(this.batches.length>=16)return{ok:false,reason:'The shared queue holds sixteen batches.'};
 const reserved=new Set();for(const b of this.batches)if(ITEMS[b.item]?.slot===it.slot)for(const id of b.ids.slice(b.index))reserved.add(id);
 const requested=[...new Set(ids||[])].slice(0,1000),eligible=requested.filter(id=>!reserved.has(id)&&!this.queue.some(j=>j.actorId===id)&&this.outfitEligible(id==='king'?this.g.king:this.g.apesById.get(id),it));
 const torchRoom=itemId==='torch'?Math.max(0,24-this.torchIds.size-this.queue.filter(j=>j.kind==='torch').length-this.batches.filter(b=>b.item==='torch').reduce((n,b)=>n+b.ids.length-b.index,0)):1000;
 const limit=quantity==='all'?1000:clamp(Math.floor(Number(quantity)||1),1,1000),s=station.s,afford=Math.min(Math.floor(s.wood/Math.max(1,it.wood)),Math.floor(s.food/Math.max(1,it.food)),it.rare?Math.floor(this.rareMaterials/it.rare):1000),accepted=eligible.slice(0,Math.min(limit,afford,torchRoom));
 if(!accepted.length){this.status=eligible.length?'Not enough village supplies.':'No eligible recipients, or their equipment slot is already queued.';return{ok:false,reason:this.status,assigned:0,missing:Math.min(limit,eligible.length)}}
 const count=accepted.length;s.wood-=count*it.wood;s.food-=count*it.food;this.rareMaterials-=count*(it.rare||0);
 const batch={id:'outfit-'+ ++this.outfitSerial,item:itemId,settlementId,stationId,ids:accepted,orders:Object.fromEntries(accepted.map(id=>[id,orderStamp(id==='king'?this.g.king:this.g.apesById.get(id))])),index:0,progress:0,completed:0,createdAt:this.g.time};this.batches.push(batch);
 this.status=count+' assigned · '+Math.max(0,Math.min(limit,eligible.length)-count)+' missing supplies · fitting at '+(station.camp?'camp':'workshop')+'.';return{ok:true,assigned:count,missing:Math.max(0,Math.min(limit,eligible.length)-count),batchId:batch.id};
};
E.cancelBatch=function(id){this.initOutfitting();const i=this.batches.findIndex(b=>b.id===id);if(i<0)return false;const b=this.batches[i],it=ITEMS[b.item],s=this.g.settlement(b.settlementId),remaining=b.ids.length-b.index,fraction=Math.max(0,remaining-b.progress/it.seconds);if(s){s.wood+=Math.floor(it.wood*fraction);s.food+=Math.floor(it.food*fraction)}this.rareMaterials+=Math.max(0,remaining-(b.progress>0?1:0))*(it.rare||0);this.fitting.delete(b.ids[b.index]);this.batches.splice(i,1);this.status='Batch cancelled; unused supplies returned.';return true};
E.saveLoadout=function(name,items){this.initOutfitting();name=String(name||'').trim().slice(0,36);if(!name||['__proto__','constructor','prototype'].includes(name)||!this.loadouts[name]&&Object.keys(this.loadouts).length>=12)return false;const seen=new Set(),list=[];for(const id of(items||[])){const it=ITEMS[id];if(it&&!it.king&&!seen.has(it.slot)){list.push(id);seen.add(it.slot)}}if(!list.length)return false;this.loadouts[name]={name,items:list};return true};
E.orderLoadout=function(settlementId,stationId,preset,ids,quantity='all'){this.initOutfitting();const p=this.loadouts[preset]||PRESETS[preset];if(!p)return{ok:false,reason:'Unknown loadout.'};const results=p.items.map(id=>this.orderBatch(settlementId,stationId,id,ids,quantity));this.status=results.reduce((n,r)=>n+(r.assigned||0),0)+' fittings queued · '+results.filter(r=>!r.ok).length+' item types unavailable.';return{ok:results.some(r=>r.ok),results}};
E.fitItem=function(a,id){
 const it=ITEMS[id];if(!it||!a||a.hp<=0)return false;this.initOutfitting();const outfit=a.outfit||(a.outfit={version:2});
 if(it.king){const deficit=Math.max(0,a.maxHp-a.hp);outfit.royal=id;if(!this.armorLevel){a.maxHp=it.maxHp;a.hp=Math.max(1,it.maxHp-deficit)}}
 else{outfit[it.slot]=id;if(it.slot==='weapon'){a.equipment=a.equipment||{};for(const key of ['cuffs','spear','torch'])delete a.equipment[key];this.torchIds.delete(a.id);if(it.legacy){a.equipment[id]=true;if(id==='torch')this.torchIds.add(a.id)}}
  if(it.slot==='shield')a.shield={hp:it.shieldHp,maxHp:it.shieldHp,equipmentId:id};
  if(it.slot==='utility'){this.medics.delete(a.id);if(it.medical)this.medics.add(a.id)}
 }
 delete a._outfitModifiers;outfit.version=2;a.outfitFittedAt=this.g.time;this.g._lightsAt=-1;return true;
};
E.batchState=function(b){const station=this.station(b.settlementId,b.stationId),a=b.ids[b.index]==='king'?this.g.king:this.g.apesById.get(b.ids[b.index]);if(!station)return'Paused — station destroyed';if(station.s.attack)return'Paused — village under attack';if(!a||a.hp<=0)return'Paused — recipient lost; cancel for supplies';if(a.kingdomMission||a.recoveryRemaining||a.siegeTransition||a.wallClimb||a.onWallId||this.g.blastActive(a)||this.g.time-(a.lastHit??-100)<3)return'Paused — recipient unavailable';if(b.orders?.[a.id]!==undefined?b.orders[a.id]!==orderStamp(a):b.orderGroup!==undefined&&b.orderGroup!==(a.armyOrder?.group||null))return'Paused — new army orders';return dist(a,station.w)>85?'Traveling to fitting station':'Fitting'};
E.tickOutfitting=function(dt){
 this.initOutfitting();this.fitting.clear();let active=0;const stations=new Set();
 for(const b of this.batches){if(active>=4)break;if(stations.has(b.stationId))continue;stations.add(b.stationId);active++;const a=b.ids[b.index]==='king'?this.g.king:this.g.apesById.get(b.ids[b.index]);if(b.orderGroup===undefined)b.orderGroup=a?.armyOrder?.group||null;this.fitting.set(b.ids[b.index],b);const state=this.batchState(b);if(state!=='Fitting')continue;b.progress+=Math.min(dt,1);const it=ITEMS[b.item];if(b.progress>=it.seconds){this.fitItem(a,b.item);b.index++;b.completed++;b.progress=0;delete b.orderGroup;this.outfitStats.equipped++;if(b.index>=b.ids.length)b.done=true}}
 this.batches=this.batches.filter(b=>!b.done);this.outfitStats.active=active;
 if(this.medics.size&&this.g.time>=(this.nextMedical||0)){this.nextMedical=this.g.time+4;const roster=Array.from(this.medics),count=Math.min(8,roster.length);for(let k=0;k<count;k++){const id=roster[((this.medicalCursor||0)+k)%roster.length];const a=this.g.apesById.get(id);if(!a||a.hp<=0){this.medics.delete(id);continue}if(this.g.humanGrid.nearest(a.x,a.y,230,1,h=>h.hp>0).length||this.g.vehicleGrid.nearest(a.x,a.y,230,1,v=>v.hp>0).length)continue;const s=this.g.settlement(a.settlementId),supplies=s||this.g;if(supplies.food<1)continue;const target=this.g.apeGrid.nearest(a.x,a.y,70,1,p=>p.hp>0&&p.hp<p.maxHp&&this.g.time-(p.lastHit??-100)>6)[0];if(target){supplies.food--;target.hp=Math.min(target.maxHp,target.hp+4)}}this.medicalCursor=((this.medicalCursor||0)+count)%Math.max(1,roster.length)}
};
const tick=E.tick;E.tick=function(dt){tick.call(this,dt);if(!this.g.ended&&dt>0&&(this.batches?.length||this.medics?.size))this.tickOutfitting(dt)};
const update=E.updateApe;E.updateApe=function(a,dt){const b=this.fitting?.get(a.id);if(!b)return update.call(this,a,dt);const state=this.batchState(b);if(!['Traveling to fitting station','Fitting'].includes(state))return false;const p=this.station(b.settlementId,b.stationId);if(state==='Traveling to fitting station')this.g.move(a,p.w.x+40-a.x,p.w.y+15-a.y,(a.speed||90)*.9,dt);else{a.moving=false;a.activity='fitting equipment';a.dir=Math.atan2(p.w.y-a.y,p.w.x-a.x)}return true};
const oldArmor=Object.getOwnPropertyDescriptor(E,'armor').get;Object.defineProperty(E,'armor',{configurable:true,get(){return oldArmor.call(this)||ITEMS[this.g.king.outfit?.royal]||null}});
const finish=E.finish;E.finish=function(job){finish.call(this,job);if(job.kind==='armor'&&this.g.king.equipment?.armorLevel){if(this.g.king.outfit)delete this.g.king.outfit.royal}};
const save=E.serialize;E.serialize=function(){const d=save.call(this);this.initOutfitting();d.outfitting={version:2,serial:this.outfitSerial,rareMaterials:this.rareMaterials,loadouts:this.loadouts,batches:this.batches.map(b=>({...b,ids:[...b.ids]}))};return d};
const restore=E.restore;E.restore=function(data){restore.call(this,data);this.initOutfitting();this.batches=[];this.fitting.clear();this.medics.clear();const saved=data?.outfitting;this.loadouts={};this.outfitSerial=Math.max(0,Math.floor(saved?.serial||0));this.rareMaterials=clamp(Math.floor(saved?.rareMaterials||0),0,10000);
 for(const a of [...this.g.apes,this.g.king]){const o=a.outfit;if(!o)continue;const clean={version:2};for(const slot of SLOTS){const it=ITEMS[o[slot]];if(it?.slot===slot&&a.id!=='king'&&it.species.includes(a.species))clean[slot]=it.id}const royal=ITEMS[o.royal];if(a.id==='king'&&royal?.king&&!this.armorLevel){clean.royal=royal.id;a.maxHp=royal.maxHp;a.hp=clamp(a.hp,1,a.maxHp)}a.outfit=clean;if(clean.utility==='medical')this.medics.add(a.id);delete a._outfitModifiers;}
 if(saved?.version!==2)return;for(const p of Object.values(saved.loadouts||{}).slice(0,12))this.saveLoadout(p.name,p.items);
 const assigned=new Set();for(const b of(saved.batches||[]).slice(0,16)){const it=ITEMS[b.item];if(!it||!this.g.settlement(b.settlementId))continue;const ids=[...new Set((b.ids||[]).slice(Math.max(0,Number(b.index)||0)))].filter(id=>{const a=id==='king'?this.g.king:this.g.apesById.get(id),key=id+':'+it.slot;if(!a||assigned.has(key)||!this.outfitEligible(a,it))return false;assigned.add(key);return true}).slice(0,1000);if(ids.length)this.batches.push({id:'outfit-'+ ++this.outfitSerial,item:it.id,settlementId:b.settlementId,stationId:b.stationId,ids,index:0,progress:clamp(Number(b.progress)||0,0,it.seconds),completed:0,orders:b.orders&&Object.fromEntries(ids.map(id=>[id,String(b.orders[id]??orderStamp(id==='king'?this.g.king:this.g.apesById.get(id)))])),orderGroup:b.orderGroup,createdAt:Number(b.createdAt)||this.g.time})}
};
const move=G.move;G.move=function(a,dx,dy,speed,dt,...args){return move.call(this,a,dx,dy,speed*(this.outfitModifiers(a)?.speed||1),dt,...args)};
const damage=G.apeDamage;G.apeDamage=function(a,n){const m=this.outfitModifiers(a);return damage.call(this,a,n)*(n<=8?m?.ranged||1:m?.damage||1)};
const hurt=G.hurt;G.hurt=function(a,n,source){if(a.id!=='king')n*=1-(this.outfitModifiers(a)?.reduction||0);return hurt.call(this,a,n,source)};
const objectDamage=G.damageObject;G.damageObject=function(o,n,a){const m=a&&this.outfitModifiers(a);if(m){if(['gate','wall','humanBarricade','heavyBarricade'].includes(o.type))n*=m.breach;if(['gateControl','prisonControl','generator','radio','alarm'].includes(o.type))n*=m.sabotage}return objectDamage.call(this,o,n,a)};
const stealth=G.stealthFactor;G.stealthFactor=function(a){return clamp(stealth.call(this,a)*(this.outfitModifiers(a)?.stealth||1),.22,2.2)};
const transition=S.transition;S.transition=function(a,dt){return transition.call(this,a,dt*(this.g.outfitModifiers(a)?.climb||1))};
const climb=ATSApeTactics.prototype.updateClimb;ATSApeTactics.prototype.updateClimb=function(a,dt){return climb.call(this,a,dt*(this.g.outfitModifiers(a)?.climb||1))};
const attack=S.attack;S.attack=function(a,t,kind,dt){const reach=this.g.outfitModifiers(a)?.reach||0,d=kind==='object'?this.g.objectDistance(a,t):dist(a,t);if(reach&&kind==='human'&&d>30&&d<=30+reach&&this.meleeAllowed(a,t)&&this.clearRay(a,t)){a.dir=Math.atan2(t.y-a.y,t.x-a.x);if(a.attackCD<=0){a.attackCD=.85;this.g.animateAttack(a,t,'hook');this.g.hurt(t,this.g.apeDamage(a,20),a)}return true}return attack.call(this,a,t,kind,dt)};
const rally=S.updateRally;S.updateRally=function(a){const before=this.rallyReady;rally.call(this,a);if(this.rallyReady>before&&this.g.outfitModifiers(a)?.rally)for(const p of this.g.apeGrid.nearest(a.x,a.y,190,40,q=>q.hp>0))p.rallyUntil=Math.max(p.rallyUntil||0,this.g.time+8)};
const reward=ATSPrisonOperations.prototype.reward;ATSPrisonOperations.prototype.reward=function(s,c){const pending=!c.rewardClaimed&&!s.prison.rareRewardClaimed&&['blacksite','transfer'].includes(s.prison.kind),first=this.g.apes.length;reward.call(this,s,c);if(pending&&c.rewardClaimed){const e=this.g.equipment;e.initOutfitting();e.rareMaterials+=4;const a=this.g.apes[first];if(a){const id={ironback:'warlordPlate',shadowhand:'phantomWraps',emberwood:'mastercraft'}[this.g.tribe.id]||'elite';e.fitItem(a,id)}c.rareRewardClaimed=true;s.prison.rareRewardClaimed=true;this.g.notify('Prison escort safe: rare armor and 4 heartwood recovered.','gold')}};

const selectTribe=G.chooseTribe;G.chooseTribe=function(id){const result=selectTribe.call(this,id);if(result){this.king.tribeId=id;this.king.appearanceVersion=2}return result};
const loadGame=ATSGame.fromJSON;ATSGame.fromJSON=function(...args){const g=loadGame.apply(this,args);g.king.tribeId=['ironback','shadowhand','emberwood'].includes(g.tribe.id)?g.tribe.id:'ironback';g.king.appearanceVersion=2;return g};
window.ATSOutfitItems=Object.freeze(ITEMS);window.ATSLoadouts=PRESETS;
})();
