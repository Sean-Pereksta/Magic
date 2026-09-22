'use strict';
/* Named regional gear, expedition mounts and tactical enemies. Uses the existing
   equipment slots and combat loop; no parallel inventory or save service. */
(() => {
  const D=AWCampaignData,A=AWCampaign;
  const gear=[
    ['heartwoodStaff','Heartwood Staff','weapon','Rare','verdant','briarwatch','natureMend','Nature spell kills create a healing pulse.',{type:'staff',damage:1.2}],
    ['thornkeeperRobes','Thornkeeper Robes','armor','Rare','verdant','verdant-boss1','thornkeeper','Melee attackers take 25% of the health damage they deal.',{}],
    ['greatWolfFang','Fang of the Great Wolf','weapon','Rare','verdant','verdant-mount','isolation','Weapon hits deal 30% more to enemies with no allies within 2.5 paces.',{type:'daggers',attack:1.5}],
    ['briarCrown','Briar Crown','trinket','Rare','verdant','briarwatch','briarDuration','Roots and slows last 30% longer.',{mods:{spell:.05}}],
    ['mosswalkerBoots','Mosswalker Boots','trinket','Uncommon','verdant','greenharbor','mosswalker','Move 18% faster when no enemy is within 4 paces.',{mods:{move:.03}}],
    ['huntersMoon','Hunter’s Moon Pendant','trinket','Rare','verdant','verdant-boss4','hunterMoon','Marked and elite kills grant extra gold and a regional material.',{}],
    ['grovekeeperRing','Grovekeeper Ring','trinket','Rare','verdant','briarwatch','summonHealth','Summons gain 35% health; wisps persist 25% longer.',{}],
    ['bloomheartCharm','Bloomheart Charm','trinket','Epic','verdant','verdant-shrine','overheal','Overhealing becomes a 5-second shield, up to 30.',{}],
    ['widowvineWand','Widowvine Wand','weapon','Rare','verdant','verdant-dungeon','poisonPower','Spells deal 25% more damage to poisoned targets.',{type:'staff'}],
    ['rangerLongbow','Ranger’s Longbow','weapon','Rare','verdant','greenharbor','longbow','Weapon hits gain 30% damage beyond 5 paces, but lose 15% within 2.',{type:'bow',range:12}],
    ['beastmasterCloak','Beastmaster Cloak','armor','Epic','verdant','briarwatch','summonHaste','After dodging, summons attack 35% faster for 4 seconds.',{move:1.06}],
    ['ancientBark','Ancient Bark Plate','armor','Epic','verdant','verdant-boss2','bark','Heavy bark grants strong armor at the cost of 8% movement speed.',{armor:.2,hp:1.3,move:.92}],
    ['emeraldCodex','Emerald Codex','trinket','Epic','verdant','verdant-ruler','natureCodex','+1 active spell slot and 15% Nature potency.',{spellSlotBonus:1}],
    ['herbalistSatchel','Traveling Herbalist’s Satchel','trinket','Rare','verdant','greenharbor','herbalist','Purchased restorative and warding draughts last 50% longer.',{}],
    ['stormglassScepter','Stormglass Scepter','weapon','Epic','meridian','stormrest','extraChain','Lightning hits have a 20% chance to arc once to an additional enemy.',{type:'scepter'}],
    ['prismguardArmor','Prismguard Armor','armor','Epic','meridian','meridian-boss4','projectileWard','Projectile health damage raises a 2-second ward against shots from that direction (8-second recovery).',{}],
    ['frostwalkerGreaves','Frostwalker Greaves','trinket','Rare','meridian','prismhold','frostwalk','Ignore chilling auras and frost terrain movement penalties.',{mods:{move:.04}}],
    ['shardstormRing','Shardstorm Ring','trinket','Epic','meridian','prismhold','shardCrit','Spell hits can critically strike and release one seeking crystal shard.',{}],
    ['thunderlordMantle','Thunderlord Mantle','armor','Epic','meridian','stormrest','perfectStorm','Perfect dodges strike a nearby enemy with lightning.',{}],
    ['cinderforgedHammer','Cinderforged Hammer','weapon','Epic','meridian','emberfall','hammer','Slow, powerful weapon hits knock back light foes.',{type:'hammer',attack:.65,damage:1.7,range:5}],
    ['ashenBulwark','Ashen Bulwark','armor','Epic','meridian','emberfall','ashen','Halve burn damage and resist hostile forced movement for 2 seconds after a hit (8-second recovery).',{armor:.14}],
    ['glassblade','Glassblade','weapon','Rare','meridian','prismhold','glassCrit','Fast, lighter strikes have a 25% critical chance.',{type:'daggers',attack:1.7,damage:.8}],
    ['tempestCompass','Tempest Compass','trinket','Rare','meridian','meridian-landmark','clearSprint','Clearing combat grants 5 seconds of increased movement speed.',{}],
    ['crystalHeart','Crystal Heart','trinket','Epic','meridian','prismhold','shieldCapacity','New shields gain 35% additional strength.',{mods:{ward:.2}}],
    ['embercoilAmulet','Embercoil Amulet','trinket','Rare','meridian','emberfall','burnPop','Burning enemies explode on death for minor area damage.',{}],
    ['snowblindHood','Snowblind Hood','armor','Rare','meridian','meridian-boss2','snowblind','Slowed enemies deal 20% less damage to you.',{}],
    ['stormrunnerBoots','Stormrunner Boots','trinket','Rare','meridian','stormrest','dodgeKill','Kills reduce remaining dodge cooldown by 0.3 seconds.',{}],
    ['meridianSpellframe','Meridian Spellframe','trinket','Epic','meridian','meridian-ruler','meridianFrame','+1 active spell slot and 12% Storm/Crystal potency.',{spellSlotBonus:1}],
    ['eventideStaff','Eventide Staff','weapon','Epic','gloam','redwatch','voidArea','Void spell areas grow 25%; their cooldowns grow 12%.',{type:'staff'}],
    ['astralCrown','Astral Crown','trinket','Legendary','gloam','astral','celestialRefund','Every third Celestial cast refunds 30% of its cooldown.',{}],
    ['bloodrootVestments','Bloodroot Vestments','armor','Epic','gloam','gloam-boss2','lowHealth','Spell potency rises 25% below 35% health.',{}],
    ['nightglassRing','Nightglass Ring','trinket','Epic','gloam','bonehaven','shadowAccuracy','Enemies hit by Shadow spells fire less accurate shots for 4 seconds.',{}],
    ['starcallerPendant','Starcaller Pendant','trinket','Epic','gloam','astral','fifthStar','Every fifth spell cast launches a seeking star.',{}],
    ['gravekeeperLantern','Gravekeeper Lantern','trinket','Rare','gloam','bonehaven','undead','Nearby undead take 25% more damage.',{}],
    ['riftwalkBoots','Riftwalk Boots','trinket','Epic','gloam','redwatch','riftDodge','Dodging extends phasing protection by 0.18 seconds.',{}],
    ['soulkeeperCharm','Soulkeeper Charm','trinket','Epic','gloam','gloam-boss1','eliteMend','Elite kills restore 4% of maximum health.',{}],
    ['fallenSuns','Crown of Fallen Suns','trinket','Legendary','gloam','gloam-boss4','sunfire','Celestial spells deal 35% more to burning enemies.',{}],
    ['voidheartArmor','Voidheart Armor','armor','Legendary','gloam','gloam-boss3','largeHit','Losing 15% maximum health to one hit grants a 3-second protective void field (12-second recovery).',{}],
    ['grandArcanum','Grand Arcanum Codex','trinket','Legendary','gloam','astral','grandCodex','+2 active spell slots, up to the five-slot maximum.',{spellSlotBonus:2,cost:{stardust:20}}],
    ['lastHorizon','Eye of the Last Horizon','trinket','Legendary','gloam','gloam-ruler','horizon','Every sixth cast gains 35% power, larger area and no cooldown. A luminous ring shows readiness.',{}]
  ];
  for(const [id,name,slot,rarity,region,source,special,desc,extra] of gear){
    const ci=D.continents.findIndex(c=>c.id===region),color=D.continents[ci].color;
    D.items[id]={name,slot,rarity,price:160+ci*420+(rarityRank[rarity]||0)*90,region,source,special,desc:desc+` Source: ${D.towns[source]?.name||D.nodes[source]?.name}.`,color,trim:color,...(slot==='weapon'?{type:'staff',damage:1.18,attack:1.08,range:9,speed:10}:slot==='armor'?{hp:1.12,move:1,armor:.075,helm:'hood'}:{mods:{}}),...extra};
    if(D.towns[source]){D.towns[source].stock.push(id);if(slot==='weapon'&&!D.towns[source].roles.some(r=>['Merchant','Weaponsmith','Blacksmith'].includes(r)))D.towns[source].roles.push('Weaponsmith');if(slot==='armor'&&!D.towns[source].roles.some(r=>['Merchant','Armorer','Blacksmith'].includes(r)))D.towns[source].roles.push('Armorer');if(slot==='trinket'&&!D.towns[source].roles.some(r=>['Merchant','Relic Dealer','Enchanter','Blacksmith'].includes(r)))D.towns[source].roles.push('Relic Dealer');}
    else if(D.nodes[source])(D.nodes[source].rewardItems||=[]).push(id);
  }
  D.items.heartwood.name='Sovereign Heartwood Staff';
  D.items.restorativeDraught={name:'Restorative Draught',kind:'provision',price:65,duration:12,effect:'regeneration',desc:'Restore 2 health per second for 12 seconds.'};
  D.items.wardingDraught={name:'Warding Draught',kind:'provision',price:80,duration:15,effect:'ward',desc:'Reduce incoming damage by 12% for 15 seconds.'};
  for(const town of Object.values(D.towns))if(town.roles.includes('Alchemist'))town.stock.push('restorativeDraught','wardingDraught');
  const settlementStock=[['hidearmor','restorativeDraught','starterstaff','naturecharm','naturecharm'],['stormboots','wardingDraught','cinderplate','prismwand','prismcrown'],['bloodfang','restorativeDraught','boneward','starrobe','astralcodex']];
  D.continents.forEach((c,ci)=>{for(let i=0;i<5;i++){const town=D.towns[`${c.id}_settlement_${i}`];if(!town)continue;town.stock.push(settlementStock[ci][i]);if(i===3){town.roles.push('Relic Dealer','Merchant');town.spells=[['briarCage','seedSentry'],['crystalBarricade','hailOrbit'],['soulChain','astralSentinel']][ci].filter(id=>SPELLS[id]);}}});
  // Crafting components are deliberately tied to their home region.
  for(const [town,material] of [['briarwatch','moonstone'],['astral','stardust'],['bonehaven','voidshard'],['greenharbor','herbs']]){
    const id=`regional_${material}`;D.items[id]||={name:`${MATERIALS[material].name} bundle`,kind:'material',material,price:100,count:5};D.towns[town].stock.push(id);
  }
  D.items.emeraldCodex.cost={moonstone:8};D.items.meridianSpellframe.cost={frost:10,dust:10};
  Object.assign(D.mounts,{
    verdantElk:{name:'Verdant Elk',icon:'🦌',color:'#b9de91',speed:1.55,cost:0,terrain:['forest','thornwild'],route:'forestTrail',desc:'Trailblazer: swift in forests, resistant to natural slows, and opens hidden forest shortcuts. Earned at Elk Sanctuary after discovering Silverwood.'},
    stormclaw:{name:'Stormclaw',icon:'⚡',color:'#8cdbf2',speed:1.65,cost:0,terrain:['stormlands'],route:'stormPath',desc:'Lightning Sprint builds after 2 seconds of movement. Opens Storm Paths. Earned after the Storm Knight Regent.'},
    astralGryphon:{name:'Astral Gryphon',icon:'🦅',color:'#dac5ff',speed:1.85,cost:0,terrain:['celestial','gloam'],route:'flightRoute',desc:'Opens designated Flight Routes. Arrival grants 5 seconds of movement speed after dismounting. Earned after the Last Sun Warden.'}
  });
  const mountNodes=[D.nodes['verdant-mount'],D.nodes['meridian-mount'],D.nodes['gloam-mount']];
  mountNodes.forEach((n,i)=>{n.mount=['verdantElk','stormclaw','astralGryphon'][i];n.mountRequires=i===0?'verdant-shrine':i===1?'meridian-boss1':'gloam-boss4';n.name=['Elk Sanctuary','Stormclaw Crag','Astral Gryphon Aerie'][i];});
  const has=key=>equippedItems().some(i=>i.special===key),timers={};let casts=0,celestialCasts=0,summonHaste=0,sprint=0,incoming=null,processing=false,castContext=null,horizonCast=false;
  const school=id=>{const s=SPELLS[id];return s?.category||(/thorn|root|briar|poison|grove|spore|bloom/i.test(id)?'Nature':/star|sun|celestial|seraph/i.test(id)?'Celestial':/void|soul|eclipse|eventide/i.test(id)?'Shadow/Void':/storm|chain|thunder|tempest|gust/i.test(id)?'Storm':'Arcane');};
  const tagSchool=tag=>/nature|poison|thorns/.test(tag)?'Nature':/star|celestial/.test(tag)?'Celestial':/soul|void|shadow/.test(tag)?'Shadow/Void':/lightning|storm/.test(tag)?'Storm':null;
  const poisoned=e=>e.regionalPoison>0||!!AWContinentalSpells.state().poisons.get(e);
  function ward(amount,time){const p=game.player;p.shield=Math.max(p.shield,amount*(has('shieldCapacity')?1.35:1));p.shieldTime=Math.max(p.shieldTime,time);}
  function shot(at,target,damage,color,tag='arcane'){if(!target)return;const d=norm(target.x-at.x,target.y-at.y);magicProjectile({x:at.x,y:at.y,vx:d.x*8,vy:d.y*8,damage,color,kind:'arcane',life:1.5,seek:1.5,tag});}
  const oldBuy=A.buy;A.buy=function(id){const t=D.items[id];if(t?.kind!=='provision')return oldBuy(id);if(!A.inCampaign()||!D.towns[A.current()?.town]?.stock.includes(id)||game.gold<t.price)return false;game.gold-=t.price;A.state().provisions[t.effect]=t.duration*(has('herbalist')?1.5:1);saveGame();return true;};
  const oldClaim=A.claimSite;A.claimSite=function(){const n=A.current();if(n.mountRequires&&!A.state().cleared.includes(n.mountRequires)){toastMsg(`First discover or defeat ${D.nodes[n.mountRequires].name}.`);return false;}const ok=oldClaim();if(ok)grantItems();return ok;};
  function grantItems(){const s=A.state();if(!s)return; s.itemClaims||=[];let changed=false;
    for(const n of Object.values(D.nodes))if(!n.shadow&&(n.mount||['shrine','landmark','resource','treasure','puzzle'].includes(n.type)?s.claimed:s.cleared).includes(n.id))for(const id of n.rewardItems||[])if(!s.itemClaims.includes(id)){s.itemClaims.push(id);s.rewards.push(id);changed=true;}
    if(changed){toastMsg('Named equipment earned — claim it in Character.');saveGame();}
  }
  const oldClear=markRoomCleared;markRoomCleared=function(){const was=game.roomData?.cleared;oldClear();if(!was&&game.roomData?.cleared){if(has('clearSprint'))sprint=5;grantItems();}};
  const oldLoad=loadRoom;loadRoom=function(){summonHaste=0;const result=oldLoad();grantItems();return result;};
  const oldMods=spellMods;spellMods=function(id){const m=oldMods(id),s=school(id);if(has('natureCodex')&&s==='Nature')m.power*=1.15;if(has('meridianFrame')&&(s==='Storm'||/crystal|prism/i.test(id)))m.power*=1.12;if(has('lowHealth')&&game.player.hp<game.player.maxHp*.35)m.power*=1.25;if(has('voidArea')&&s==='Shadow/Void')m.cdr*=1.12;if(has('horizon')&&casts%6===5)m.power*=1.35;return m;};
  const oldGround=groundEffect;groundEffect=function(kind,x,y,r,...args){if(has('voidArea')&&['void','singularity'].includes(kind))r*=1.25;if(horizonCast)r*=1.25;return oldGround(kind,x,y,r,...args);};
  for(const id of Object.keys(SPELLS)){
    const key=SPELLS[id].cast;if(SPELL_CASTS[key]._regionalWrapped)continue;const fn=SPELL_CASTS[key];
    const wrapped=function(spell,s,m){castContext=school(spell);horizonCast=has('horizon')&&casts%6===5;try{return fn(spell,s,m);}finally{castContext=null;horizonCast=false;}};wrapped._regionalWrapped=true;SPELL_CASTS[key]=wrapped;
  }
  const oldCast=castSpell;castSpell=function(slot){const id=game.player?.activeSpells[slot],before=game.player?.spellState[id]?.cd||0,p=game.player;if(!id)return oldCast(slot);const shield=p.shield;oldCast(slot);if(has('shieldCapacity')&&p.shield>shield)p.shield=shield+(p.shield-shield)*1.35;const st=p.spellState[id];
    if((st?.cd>before||id==='wildstep'&&st?.reserveCd&&before===0)&&running&&!paused&&!modalPause){casts++;if(has('horizon')&&casts%6===0)st.cd=0;if(has('fifthStar')&&casts%5===0)shot(p,nearestEnemy(p,10),28+game.level,'#ffe6ad','celestial');if(school(id)==='Celestial'&&has('celestialRefund')&&++celestialCasts%3===0)st.cd*=.7;}
  };
  const oldHeal=healPlayer;healPlayer=function(amount){const p=game.player,excess=p?Math.max(0,p.hp+amount-p.maxHp):0;oldHeal(amount);if(excess&&has('overheal'))ward(Math.min(30,(p.shield||0)+excess),5);};
  const oldHit=damageEnemy;damageEnemy=function(e,amount,tag='',dot=false){
    if(!e||e.dead)return;const s=castContext||tagSchool(tag),weapon=tag.includes('weapon');
    if(has('isolation')&&weapon&&!game.enemies.some(o=>o!==e&&!o.dead&&dist(o,e)<2.5))amount*=1.3;
    if(has('longbow')&&weapon)amount*=dist(e,game.player)>5?1.3:dist(e,game.player)<2?.85:1;
    if(has('poisonPower')&&!weapon&&poisoned(e))amount*=1.25;
    if(has('undead')&&/skeleton|necro|vampire|revenant|knight|oracle|leech|devourer/i.test(e.type)&&dist(e,game.player)<5)amount*=1.25;
    if(/fire|ember|burn/.test(tag))e.regionalBurn=3;if(/poison/.test(tag))e.regionalPoison=3;
    if(has('sunfire')&&s==='Celestial'&&e.regionalBurn>0)amount*=1.35;
    if(has('shadowAccuracy')&&s==='Shadow/Void')e.regionalBlind=4;
    const crit=!dot&&!processing&&Math.random()<(weapon&&has('glassCrit')?.25:!weapon&&has('shardCrit')?.12:0);
    if(crit)amount*=1.5;
    const marked=AWContinentalSpells.state().marks.has(e),hp=e.hp;oldHit(e,amount,tag,dot);
    if(weapon&&has('hammer')&&!e.boss){const d=norm(e.x-game.player.x,e.y-game.player.y);e.x=clamp(e.x+d.x*.65,.4,ROOM_W-.4);e.y=clamp(e.y+d.y*.65,.4,ROOM_H-.4);}
    if(!processing&&crit&&!weapon){processing=true;shot(game.player,nearestEnemy(e,6),amount*.25,'#bdeeff','regionalShard');processing=false;}
    if(!processing&&has('extraChain')&&s==='Storm'&&Math.random()<.2){processing=true;const t=nearestEnemy(e,4,o=>o!==e);if(t){fx('lightning',e.x,e.y,.2,'#aff5ff',{toX:t.x,toY:t.y,width:2});oldHit(t,amount*.3,'regionalArc',true);}processing=false;}
    if(hp>0&&e.dead){if(has('natureMend')&&s==='Nature'){healPlayer(3);fx('bloom',game.player.x,game.player.y,.4,'#aadd99',{r:1});}if(has('hunterMoon')&&(marked||e.elite)){game.gold+=4;addMaterial(A.current()?.material||'hide',1,true);}}
  };
  const oldKill=killEnemy;killEnemy=function(e,tag){if(!e||e.dead)return;const burning=e.regionalBurn>0;oldKill(e,tag);if(has('eliteMend')&&e.elite)healPlayer(game.player.maxHp*.04);if(has('dodgeKill'))game.player.dodgeCd=Math.max(0,game.player.dodgeCd-.3);if(has('burnPop')&&burning&&!processing){processing=true;radialDamage(e.x,e.y,1.3,12+game.level*.6,'regionalEmber');processing=false;fx('explosion',e.x,e.y,.35,'#ffb273',{r:1.3});}};
  const oldHurt=damagePlayer;damagePlayer=function(amount){const p=game.player;if(!p)return;const q=game.projectiles.find(q=>q.owner==='enemy'&&q.life>0&&dist(q,p)<q.r+p.r+.12),source=incoming||q?.regionalSource;
    if(has('ashen')&&(q&&/fire|ember/.test(q.kind)||incoming?.regionalBurnSource))amount*=.5;
    if(has('snowblind')&&source?.slow>0)amount*=.8;
    if((A.state()?.provisions?.ward||0)>0)amount*=.88;
    if(timers.voidProtection>0)amount*=.6;
    if(q&&timers.directionWard>0&&p.regionalWardDir){const d=norm(q.x-p.x,q.y-p.y);if(d.x*p.regionalWardDir.x+d.y*p.regionalWardDir.y>.3)amount*=.55;}
    const hp=p.hp;oldHurt(amount);const dealt=Math.max(0,hp-p.hp);if(!dealt)return;
    if(has('thornkeeper')&&incoming&&dist(incoming,p)<2)damageEnemy(incoming,dealt*.25,'regionalThorns',true);
    if(has('projectileWard')&&q&&!(timers.prismRecovery>0)){p.regionalWardDir=norm(-q.vx,-q.vy);timers.directionWard=2;timers.prismRecovery=8;fx('ward',p.x,p.y,2,'#bdeeff',{follow:true});}
    if(has('ashen')&&!(timers.ashenRecovery>0)){timers.anchored=2;timers.ashenRecovery=8;}
    if(has('largeHit')&&dealt>=p.maxHp*.15&&!(timers.voidRecovery>0)){timers.voidProtection=3;timers.voidRecovery=12;fx('ward',p.x,p.y,3,'#ae83dc',{follow:true});}
  };
  const oldEnemyAI=updateEnemyAI;updateEnemyAI=function(e,...args){incoming=e;try{return oldEnemyAI(e,...args);}finally{incoming=null;}};
  const oldEnemyShot=enemyProjectile;enemyProjectile=function(e,d,speed,kind,extra){if(e.regionalBlind>0){const a=Math.atan2(d.y,d.x)+Math.sin(e.phase+elapsed*7)*.38;d={x:Math.cos(a),y:Math.sin(a)};}const count=game.projectiles.length,result=oldEnemyShot(e,d,speed,kind,extra);for(let i=count;i<game.projectiles.length;i++)game.projectiles[i].regionalSource=e;return result;};
  const oldPerfect=intensityPerfectDodge;intensityPerfectDodge=function(){oldPerfect();if(has('perfectStorm')){const e=nearestEnemy(game.player,6);if(e){damageEnemy(e,28+game.level,'lightning');fx('lightning',game.player.x,game.player.y,.25,'#99eeff',{toX:e.x,toY:e.y,width:3});}}};
  const oldDodge=dodge;dodge=function(){const before=game.player?.dodgeCd||0;oldDodge();if(game.player?.dodgeCd>before){if(has('summonHaste'))summonHaste=4;if(has('riftDodge'))game.player.invuln+=.18;}};
  const oldMove=playerMovement;playerMovement=function(dt){const p=game.player,base=p.speed,s=A.state(),m=s?.riding&&D.mounts[s.activeMount],moving=Math.hypot(AWInput.move.x,AWInput.move.y)>.1;
    p.regionalMountRun=m&&moving?(p.regionalMountRun||0)+dt:0;
    if(m&&s.activeMount==='stormclaw'&&p.regionalMountRun>2)p.speed*=1.22;
    if(has('mosswalker')&&!nearestEnemy(p,4)||sprint>0)p.speed*=1.18;
    if((p.regionalChill||0)>0&&!has('frostwalk'))p.speed*=m&&s.activeMount==='verdantElk'?.9:.7;
    try{return oldMove(dt);}finally{p.speed=base;}
  };
  const lastStatus=new WeakMap();const oldUpdate=updateSpellEntities;updateSpellEntities=function(dt){if(!running||paused||modalPause||roomTransition)return;
    for(const key of Object.keys(timers))timers[key]=Math.max(0,timers[key]-dt);summonHaste=Math.max(0,summonHaste-dt);sprint=Math.max(0,sprint-dt);
    const p=game.player,provisions=A.state()?.provisions||{};for(const key of Object.keys(provisions)){provisions[key]=Math.max(0,provisions[key]-dt);if(key==='regeneration'&&provisions[key]>0)healPlayer(2*dt);}
    p.regionalChill=Math.max(0,(p.regionalChill||0)-dt);
    const actors=AWContinentalSpells.state().actors;
    for(const a of [...actors,...game.summons]){if(!a.regionalGearApplied){if(has('summonHealth')){if(a.hp){a.hp*=1.35;a.maxHp*=1.35;}else{a.life*=1.25;a.maxLife*=1.25;}}a.regionalGearApplied=true;}if(summonHaste>0){if(Number.isFinite(a.tick))a.tick-=dt*.35;if(Number.isFinite(a.shot))a.shot-=dt*.35;}}
    for(const e of game.enemies){e.regionalPoison=Math.max(0,(e.regionalPoison||0)-dt);e.regionalBurn=Math.max(0,(e.regionalBurn||0)-dt);e.regionalBlind=Math.max(0,(e.regionalBlind||0)-dt);if(has('briarDuration')){const prior=lastStatus.get(e)||{};for(const key of ['slow','stun'])if(e[key]>(prior[key]||0)+.05)e[key]*=1.3;lastStatus.set(e,{slow:e.slow,stun:e.stun});}}
    oldUpdate(dt);
  };
  const oldEffects=updateEnemyEffects;updateEnemyEffects=function(dt){const p=game.player,at={x:p.x,y:p.y};oldEffects(dt);if(timers.anchored>0){p.x=at.x;p.y=at.y;}if(has('frostwalk')&&p.tailwind<0)p.tailwind=0;};
  const oldDraw=drawPlayer;drawPlayer=function(p){oldDraw(p);const s=A.state();if(s?.riding){const m=D.mounts[s.activeMount],pt=worldToScreen(p.x,p.y);ctx.save();ctx.translate(pt.x,pt.y);ctx.strokeStyle=m.color;ctx.lineWidth=2;
      if(['astralGryphon','shadowGryphon'].includes(s.activeMount)){for(const side of [-1,1]){ctx.beginPath();ctx.moveTo(side*5,-26);ctx.lineTo(side*52,-52+Math.sin(elapsed*6)*10);ctx.lineTo(side*32,-13);ctx.closePath();ctx.stroke();}}else if(s.activeMount==='verdantElk'){for(const side of [-1,1]){ctx.beginPath();ctx.moveTo(24,-45);ctx.lineTo(24+side*18,-69);ctx.lineTo(24+side*22,-59);ctx.stroke();}}else if(s.activeMount==='stormclaw'){ctx.beginPath();ctx.moveTo(-26,0);ctx.lineTo(-9,-8);ctx.lineTo(-15,3);ctx.lineTo(26,-3);ctx.stroke();}ctx.restore();}
    if(has('horizon')&&casts%6===5){const q=worldToScreen(p.x,p.y);ctx.save();ctx.strokeStyle='#fff0b0';ctx.lineWidth=2;ctx.beginPath();ctx.ellipse(q.x,q.y-20,26,34,elapsed*.7,0,TAU);ctx.stroke();ctx.restore();}
  };
  window.AWRegionalContent={gearIds:gear.map(g=>g[0]),has,grantItems,ward,timers,school,spellArea:id=>(has('voidArea')&&school(id)==='Shadow/Void'?1.25:1)*(horizonCast?1.25:1),get incoming(){return incoming;},set incoming(e){incoming=e;}};
})();
