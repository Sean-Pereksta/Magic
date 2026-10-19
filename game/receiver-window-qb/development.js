/* Persistent recruitment, depth and development rules. No renderer or network dependency. */
(function(root){
  'use strict';
  const P=root.QBProgression||(typeof require==='function'?require('./progression.js'):null);
  const LIMITS=Object.freeze({market:48,pool:256,bench:32,programSessions:3,cycleSessions:16,arcadeGrowthM:.03});
  const positions=Object.freeze({QB:{name:'Quarterback',height:[1.73,2.08],weight:[78,119]},WR:{name:'Receiver',height:[1.65,2.08],weight:[67,117]},RB:{name:'Running back',height:[1.65,1.98],weight:[77,122]},TE:{name:'Tight end',height:[1.83,2.13],weight:[99,139]},CB:{name:'Cornerback',height:[1.65,2.04],weight:[68,108]},S:{name:'Safety',height:[1.70,2.08],weight:[77,119]},LB:{name:'Linebacker',height:[1.77,2.11],weight:[94,141]}});
  const programs=Object.freeze({
    accuracy:{name:'QB accuracy',stats:{catching:1},skill:'accuracy',positions:['QB']},
    release:{name:'Quick release',stats:{turning:1},skill:'release',positions:['QB']},
    routes:{name:'Route laboratory',stats:{cutting:1,turning:.5},skill:'routeRunning'},
    contested:{name:'Contested hands',stats:{catching:1,strength:.4},skill:'contested'},
    coverage:{name:'Coverage footwork',stats:{turning:1,cutting:.5},skill:'coverage',positions:['CB','S','LB']},
    swat:{name:'Swat timing',stats:{athleticism:1},skill:'swat',positions:['CB','S','LB']},
    interception:{name:'Interception tracking',stats:{catching:1},skill:'interception',positions:['CB','S','LB']},
    vertical:{name:'Vertical mechanics',stats:{athleticism:1},skill:'vertical'},
    strength:{name:'Strength and power',stats:{strength:1},skill:'balance'},
    balance:{name:'Contact balance',stats:{strength:.7,evasion:.7},skill:'balance'}
  });
  const academyIds=Object.freeze(['precision','windows','contested','defense','routes','pressure']);
  const passPositions=['WR','RB','TE'],unitPositions={QB:['QB'],CB1:['CB','S'],CB2:['CB','S'],S:['S','CB'],LB:['LB']};
  const first=['Adrian','Andre','Ashton','Blake','Cameron','Darius','Devin','Elias','Emmett','Isaiah','Jalen','Jamal','Jordan','Julian','Kai','Kendall','Malik','Marcus','Miles','Nico','Noah','Owen','Reece','Roman','Samir','Theo','Tristan','Tyler','Victor','Wesley','Xavier','Zion'];
  const last=['Adams','Banks','Bennett','Brooks','Carter','Coleman','Davis','Ellis','Fields','Foster','Grant','Gray','Green','Harris','Hayes','Hill','Jackson','James','Knight','Lewis','Marshall','Mills','Morgan','Price','Reed','Rhodes','Robinson','Sanders','Scott','Turner','Walker','Wright'];
  const clamp=(n,a,b)=>Math.max(a,Math.min(b,Number.isFinite(Number(n))?Number(n):a));
  const integer=(n,a=0,b=Number.MAX_SAFE_INTEGER)=>Math.floor(clamp(n,a,b));
  const round=(n,places=3)=>Number(n.toFixed(places));
  const text=(n,fallback='',max=80)=>typeof n==='string'&&n.trim()?n.trim().slice(0,max):fallback;
  const hash=value=>{let n=2166136261;for(const c of String(value)){n^=c.charCodeAt(0);n=Math.imul(n,16777619);}return n>>>0;};
  const random=seed=>()=>{seed=(Math.imul(seed,1664525)+1013904223)>>>0;return seed/4294967296;};
  const overall=p=>Math.round(P.stats.reduce((sum,k)=>sum+P.rating(p[k]),0)/P.stats.length);
  const modelIds={QB:['qb_field_general','qb_mobile'],WR:['wr_speed','wr_route_technician','wr_possession','wr_jump_ball','wr_physical','wr_slot'],RB:['rb_elusive','rb_power'],TE:['te_receiving','te_blocking'],CB:['cb_press','cb_ballhawk'],S:['safety_hybrid'],LB:['linebacker_coverage']};
  function roster(f){return [...f.team,...(f.bench||[])];}
  function findPlayer(f,id){return roster(f).find(p=>p.id===id)||null;}
  function cycle(f){return integer(f.development?.trainingEpoch??f.round,1);}
  function refreshPhysical(p,mode=p.physical?.heightMode||'realistic'){
    const pos=positions[p.position]||positions.WR,a=p.physical||{},legacyHeight=.0254*(68.3+.0805*Math.min(140,P.effective(p.size||55)));
    const adultHeight=round(clamp(a.adultHeightM??a.heightM??legacyHeight,...pos.height)),growth=round(clamp(a.heightGrowthM||0,0,LIMITS.arcadeGrowthM),4);
    const baseStrength=clamp(a.baseStrength??p.strength,1,Number.MAX_SAFE_INTEGER),baseWeight=round(clamp(a.baseWeightKg??a.weightKg??(pos.weight[0]+(pos.weight[1]-pos.weight[0])*.48),...pos.weight),1);
    const strength=clamp(P.effective(p.strength)/140,0,1),height=round(clamp(adultHeight+(mode==='arcade'?growth:0),...pos.height));
    const build=['lean','balanced','power'].includes(a.build)?a.build:(p.strength>p.speed+12?'power':p.speed>p.strength+12?'lean':'balanced');
    const weight=round(clamp(baseWeight+Math.max(0,P.effective(p.strength)-P.effective(baseStrength))*.15,...pos.weight),1);
    const frame=build==='power'?1.08:build==='lean'?.94:1;
    p.physical={version:1,heightMode:mode,adultHeightM:round(adultHeight),heightM:round(height),heightGrowthM:round(growth,4),baseWeightKg:round(baseWeight,1),weightKg:round(weight,1),baseStrength,build,
      wingspanRatio:round(clamp(a.wingspanRatio??1.03,.96,1.12)),handRatio:round(clamp(a.handRatio??1,.88,1.15)),
      muscularity:round(clamp(.16+strength*.68+(build==='power'?.1:0),0,1)),shoulderWidthM:round(height*.245*frame*(.9+.2*strength)),
      chestSize:round(clamp((weight-pos.weight[0])/(pos.weight[1]-pos.weight[0])*.55+strength*.45,0,1)),armThickness:round(clamp(strength*.78+(build==='power'?.15:.04),0,1)),legPower:round(clamp(P.effective(p.athleticism)/140*.45+strength*.55,0,1))};
    return p.physical;
  }
  function physicalProfile(p){
    const a=p.physical||refreshPhysical(p),height=a.heightM;
    return {...a,wingspanM:round(height*a.wingspanRatio),handSizeM:round(.185*(height/1.88)*a.handRatio),reachM:round(height*1.16+(a.wingspanRatio-1.03)*height*.4),modelId:p.modelId};
  }
  function physique(p){const a=physicalProfile(p);return {...a,scaleX:round(a.shoulderWidthM/.48),scaleY:round(a.heightM/1.88),scaleZ:round(.88+a.chestSize*.25)};}
  function enrichPlayer(p,{position='WR',mode='realistic',id}={}){
    P.migratePlayer(p);p.id=text(p.id,id||'player_'+hash(p.name));p.position=positions[p.position]?p.position:position;p.age=integer(p.age??23,18,39);
    const seed=hash(p.id),rng=random(seed),dev=p.development&&typeof p.development==='object'?p.development:{};
    p.modelId=modelIds[p.position].includes(p.modelId)?p.modelId:modelIds[p.position][seed%modelIds[p.position].length];
    p.specialties=Object.fromEntries(['accuracy','release','routeRunning','contested','coverage','swat','interception','vertical','balance'].map(k=>[k,integer(p.specialties?.[k]??((p.catching+p.turning+p.athleticism)/3),1,Number.MAX_SAFE_INTEGER)]));
    const potential=integer(dev.potential??overall(p)+8+Math.floor(rng()*14),overall(p),Math.max(125,overall(p)));
    p.development={potential,tendency:['Technician','Explosive','Power','Ball hawk'][seed%4],ceilings:Object.fromEntries(P.stats.map(k=>[k,integer(dev.ceilings?.[k]??Math.max(p[k]+6,potential+Math.floor(rng()*10)-3),p[k],Number.MAX_SAFE_INTEGER)])),
      academyPoints:integer(dev.academyPoints,0,100),sessions:Object.fromEntries(Object.keys(programs).map(k=>[k,integer(dev.sessions?.[k],0,1000000)])),
      cycle:integer(dev.cycle,0),counts:Object.fromEntries(Object.keys(programs).map(k=>[k,integer(dev.counts?.[k],0,LIMITS.programSessions)]))};
    p.abilities=(Array.isArray(p.abilities)?p.abilities:[]).filter(x=>typeof x==='string').map(x=>x.slice(0,40)).slice(0,4);
    if(!p.abilities.length){if(p.speed>=90)p.abilities.push('Deep Threat');if(p.strength>=88)p.abilities.push('Contact Balance');if(p.catching>=88)p.abilities.push('Sure Hands');if(p.athleticism>=90)p.abilities.push('High Point');if(!p.abilities.length)p.abilities.push(p.development.tendency);}
    const chemistry=p.chemistry&&typeof p.chemistry==='object'&&!Array.isArray(p.chemistry)?p.chemistry:{},concepts=chemistry.concepts&&typeof chemistry.concepts==='object'&&!Array.isArray(chemistry.concepts)?chemistry.concepts:{};
    p.chemistry={xp:clamp(chemistry.xp,0,10000),concepts:Object.fromEntries(Object.entries(concepts).slice(-64).map(([key,value])=>[key.slice(0,80),clamp(value,0,1000)]))};
    p.price=Number.isFinite(Number(p.price))?integer(p.price,100):P.signingPrice(overall(p));
    refreshPhysical(p,mode);return p;
  }
  function prospect(f){
    const d=f.development,n=d.nextId++,rng=random(hash(d.seed+':'+n)),position=Object.keys(positions)[n%7],limits=positions[position],roll=rng();
    const band=roll<.70?[57,76,'Prospect']:roll<.89?[77,86,'Developing star']:roll<.975?[87,94,'Elite']:[95,105,'Generational'];
    const target=band[0]+Math.floor(rng()*(band[1]-band[0]+1));
    const p={id:'dev_'+d.seed.toString(36)+'_'+n.toString(36),name:first[Math.floor(rng()*first.length)]+' '+last[Math.floor(rng()*last.length)],position,age:18+Math.floor(rng()*14),archetype:position==='WR'?['Deep Threat','Route Technician','Possession Receiver','Power Receiver','Blocking Receiver'][n%5]:limits.name,rarity:band[2],price:P.signingPrice(target),trainings:0};
    for(const k of P.stats)p[k]=Math.max(30,target+Math.floor(rng()*25)-12);
    const biases={QB:{catching:8,turning:6,speed:-8},WR:{speed:7,catching:8},RB:{evasion:8,strength:4},TE:{strength:10,size:12,speed:-7},CB:{speed:9,turning:7},S:{athleticism:8,catching:5},LB:{strength:12,size:10,speed:-8}}[position];
    for(const [k,v] of Object.entries(biases))p[k]=Math.max(30,p[k]+v);
    p.physical={adultHeightM:limits.height[0]+rng()*(limits.height[1]-limits.height[0]),baseWeightKg:limits.weight[0]+rng()*(limits.weight[1]-limits.weight[0]),build:['lean','balanced','power'][Math.floor(rng()*3)],wingspanRatio:.98+rng()*.1,handRatio:.92+rng()*.18};
    enrichPlayer(p,{mode:d.heightMode});p.price=P.signingPrice(overall(p));return p;
  }
  function refill(f){while(f.market.length<LIMITS.market){if(!f.development.pool.length)f.development.pool.push(prospect(f));f.market.push(f.development.pool.shift());}while(f.development.pool.length<LIMITS.pool)f.development.pool.push(prospect(f));f.development.pool.length=LIMITS.pool;}
  function normalize(f){
    if(!f||!Array.isArray(f.team)||f.team.length!==4)throw new Error('Development requires the four existing starters.');
    const raw=f.development&&typeof f.development==='object'?f.development:{},heightMode=raw.heightMode==='arcade'?'arcade':'realistic';
    if(raw.version>1)throw new Error('This player-development save is newer than this game.');
    const previousRound=integer(raw.highestRound??f.round,1),highestRound=Math.max(previousRound,integer(f.round,1));
    f.development={version:1,seed:integer(raw.seed??hash(f.team.map(p=>p.id||p.name).join('|')),0,4294967295),nextId:integer(raw.nextId,0,1e9),heightMode,highestRound,season:integer(raw.season??1,1,1000000),trainingEpoch:integer(integer(raw.trainingEpoch??previousRound,1)+highestRound-previousRound,1),
      pool:Array.isArray(raw.pool)?raw.pool.slice(0,LIMITS.pool):[],lineups:{},units:{},academy:{},trainingCycle:integer(raw.trainingCycle,0),trainingUsed:integer(raw.trainingUsed,0,LIMITS.cycleSessions)};
    const used=new Set(),clean=(items,kind,max)=>items.slice(0,max).filter(p=>p&&typeof p==='object').map((p,i)=>enrichPlayer(p,{mode:heightMode,id:kind+'_'+i+'_'+hash(p.name)})).filter(p=>{if(used.has(p.id))return false;used.add(p.id);return true;});
    f.team=clean(f.team,'starter',4);if(f.team.length!==4)throw new Error('Starting player IDs must be distinct.');
    f.bench=clean(Array.isArray(f.bench)?f.bench:[],'bench',LIMITS.bench);f.market=clean(Array.isArray(f.market)?f.market:[],'market',LIMITS.market);
    f.development.pool=clean(f.development.pool,'pool',LIMITS.pool);
    const prefix='dev_'+f.development.seed.toString(36)+'_';for(const id of used)if(id.startsWith(prefix)){const sequence=parseInt(id.slice(prefix.length),36);if(Number.isSafeInteger(sequence))f.development.nextId=Math.max(f.development.nextId,sequence+1);}
    refill(f);
    const owned=new Set(roster(f).map(p=>p.id));
    for(const [key,ids] of Object.entries(raw.lineups||{}).slice(0,24)){const name=text(key,'',32);if(name&&Array.isArray(ids)&&ids.length===4&&new Set(ids).size===4&&ids.every(id=>owned.has(id)))f.development.lineups[name]=ids.slice();}
    if(!f.development.lineups.Standard)f.development.lineups.Standard=f.team.map(p=>p.id);
    for(const [slot,allowed] of Object.entries(unitPositions)){const p=findPlayer(f,raw.units?.[slot]);if(p&&allowed.includes(p.position)&&!Object.values(f.development.units).includes(p.id))f.development.units[slot]=p.id;}
    for(const id of academyIds){const r=raw.academy?.[id];if(r&&typeof r==='object')f.development.academy[id]={cycle:integer(r.cycle,0),score:clamp(r.score,0,100),bestScore:clamp(r.bestScore??r.score,0,100),tier:integer(r.tier,0,3)};}
    return f;
  }
  function validate(f){
    for(const [name,items,max] of [['bench',f.bench,LIMITS.bench],['prospect pool',f.development?.pool,LIMITS.pool]]){
      if(items!==undefined&&!Array.isArray(items))throw new Error('Invalid '+name+' in save.');if(items?.length>max)throw new Error('Too many players in '+name+'.');
      for(const p of items||[]){if(!p||typeof p.name!=='string'||p.name.length>80)throw new Error('Invalid player in '+name+'.');for(const k of P.stats.slice(0,6))if(!Number.isSafeInteger(p[k])||p[k]<1)throw new Error('Invalid ratings in '+name+'.');}
    }
    for(const p of [...f.team,...f.market,...(f.bench||[]),...(f.development?.pool||[])])for(const key of ['heightM','adultHeightM','weightKg','baseWeightKg'])if(p.physical?.[key]!==undefined&&!Number.isFinite(p.physical[key]))throw new Error('Invalid physical measurement in save.');
    return f;
  }
  function queryMarket(f,filters={}){
    const fields={overall:p=>overall(p),price:p=>p.price,age:p=>p.age,height:p=>p.physical.heightM,weight:p=>p.physical.weightKg,strength:p=>p.strength,speed:p=>p.speed,jumping:p=>p.athleticism,catching:p=>p.catching,potential:p=>p.development.potential};
    const query=text(filters.search).toLowerCase();let rows=f.market.filter(p=>(!filters.position||filters.position==='all'||p.position===filters.position)&&(!query||(p.name+' '+p.archetype+' '+p.abilities.join(' ')+' '+p.rarity).toLowerCase().includes(query))&&(!filters.ability||p.abilities.includes(filters.ability)));
    for(const [key,get] of Object.entries(fields)){const min=Number(filters['min'+key[0].toUpperCase()+key.slice(1)]),max=Number(filters['max'+key[0].toUpperCase()+key.slice(1)]);if(filters['min'+key[0].toUpperCase()+key.slice(1)]!==''&&Number.isFinite(min))rows=rows.filter(p=>get(p)>=min);if(filters['max'+key[0].toUpperCase()+key.slice(1)]!==''&&Number.isFinite(max))rows=rows.filter(p=>get(p)<=max);}
    const get=fields[filters.sort]||fields.overall,direction=filters.direction==='asc'?1:-1;return rows.slice().sort((a,b)=>direction*(get(a)-get(b))||a.name.localeCompare(b.name));
  }
  function refreshMarket(f,{charge=true}={}){normalize(f);if(charge&&f.cash<175)return {ok:false,reason:'A new scouting board costs $175.'};if(charge)f.cash-=175;const old=f.market;f.market=[];f.development.pool.push(...old);refill(f);return {ok:true,cost:charge?175:0};}
  function sign(f,id,{starterSlot=null,betweenDrives=false}={}){
    if(starterSlot!==null&&!betweenDrives)return {ok:false,reason:'Change starters between drives.'};
    normalize(f);const index=f.market.findIndex(p=>p.id===id),p=f.market[index];if(!p)return {ok:false,reason:'That prospect is no longer listed.'};
    if(f.cash<p.price)return {ok:false,reason:'You need $'+(p.price-f.cash)+' more.'};
    if(f.bench.length>=LIMITS.bench)return {ok:false,reason:'The 32-player bench is full.'};
    if(starterSlot!==null&&(!Number.isInteger(starterSlot)||starterSlot<0||starterSlot>3||!passPositions.includes(p.position)))return {ok:false,reason:'A passing slot requires a receiver, running back or tight end.'};
    f.cash-=p.price;p.contract={signedRound:f.round,cost:p.price,years:2};f.market.splice(index,1);
    if(starterSlot===null)f.bench.push(p);else{f.bench.push(f.team[starterSlot]);f.team[starterSlot]=p;}
    refill(f);return {ok:true,player:p};
  }
  function substitute(f,id,slot,{betweenDrives=false}={}){
    if(!betweenDrives)return {ok:false,reason:'Substitutions are available between drives.'};normalize(f);
    const i=f.bench.findIndex(p=>p.id===id),p=f.bench[i];if(!p||!Number.isInteger(slot)||slot<0||slot>3||!passPositions.includes(p.position))return {ok:false,reason:'Choose an eligible bench target and an X/H/Y/Z slot.'};
    const old=f.team[slot];f.team[slot]=p;f.bench[i]=old;return {ok:true,player:p,replaced:old};
  }
  function saveLineup(f,name,ids=f.team.map(p=>p.id)){
    normalize(f);name=text(name,'',32);if(!name||!Array.isArray(ids)||ids.length!==4||new Set(ids).size!==4||!ids.every(id=>passPositions.includes(findPlayer(f,id)?.position)))return {ok:false,reason:'A lineup needs four distinct owned pass targets.'};
    if(!f.development.lineups[name]&&Object.keys(f.development.lineups).length>=24)return {ok:false,reason:'Keep at most 24 formation lineups.'};f.development.lineups[name]=ids.slice();return {ok:true};
  }
  function activateLineup(f,name,{betweenDrives=false}={}){
    if(!betweenDrives)return {ok:false,reason:'Change personnel between drives.'};normalize(f);const ids=f.development.lineups[name];if(!ids)return {ok:false,reason:'That formation lineup is unavailable.'};
    const all=roster(f),next=ids.map(id=>all.find(p=>p.id===id));if(next.some(p=>!p||!passPositions.includes(p.position)))return {ok:false,reason:'A saved lineup contains an unavailable pass target.'};f.team=next;f.bench=all.filter(p=>!ids.includes(p.id));return {ok:true};
  }
  function assignUnit(f,slot,id,{betweenDrives=false}={}){if(!betweenDrives)return {ok:false,reason:'Assign depth between drives.'};normalize(f);const p=findPlayer(f,id);if(!p||!unitPositions[slot]?.includes(p.position))return {ok:false,reason:'That player does not fit the selected unit.'};for(const key of Object.keys(f.development.units))if(f.development.units[key]===id)delete f.development.units[key];f.development.units[slot]=id;return {ok:true};}
  function unitPlayer(f,slot){return findPlayer(f,f.development?.units?.[slot])||roster(f).filter(p=>unitPositions[slot]?.includes(p.position)).sort((a,b)=>overall(b)-overall(a))[0]||null;}
  function release(f,id,{betweenDrives=false}={}){if(!betweenDrives)return {ok:false,reason:'Release bench contracts between drives.'};normalize(f);const index=f.bench.findIndex(p=>p.id===id);if(index<0)return {ok:false,reason:'Move an athlete to the bench before releasing the contract.'};const [p]=f.bench.splice(index,1);for(const [name,ids] of Object.entries(f.development.lineups))if(ids.includes(id))delete f.development.lineups[name];for(const [slot,player] of Object.entries(f.development.units))if(player===id)delete f.development.units[slot];return {ok:true,player:p};}
  function beginSeason(f,seasonNumber){normalize(f);const next=integer(seasonNumber,1,1000000),old=f.development.season;if(next===old)return {ok:true,changed:false};if(next!==old+1)return {ok:false,reason:'Advance seasons in order.'};f.development.season=next;f.development.trainingEpoch++;for(const p of roster(f)){p.age=Math.min(39,p.age+1);refreshPhysical(p,f.development.heightMode);}return {ok:true,changed:true};}
  function trainingStatus(f,p,programId){
    const program=programs[programId];if(!program)return {ok:false,reason:'Unknown development program.'};if(program.positions&&!program.positions.includes(p.position))return {ok:false,reason:'This program is for '+program.positions.join('/')+' players.'};
    const current=cycle(f),count=p.development.cycle===current?p.development.counts[programId]:0,used=f.development.trainingCycle===current?f.development.trainingUsed:0;
    if(count>=LIMITS.programSessions||used>=LIMITS.cycleSessions)return {ok:false,reason:'This cycle is complete. Win the next campaign matchup to renew program sessions.'};
    if(!Object.keys(program.stats).some(k=>p[k]<p.development.ceilings[k]))return {ok:false,reason:'This athlete has reached this program’s development ceiling.'};
    const cost=Math.round((120+count*90)*(p.development.academyPoints>0?.75:1));return {ok:true,cost,remaining:LIMITS.programSessions-count,cycleRemaining:LIMITS.cycleSessions-used};
  }
  function train(f,id,programId){
    normalize(f);const p=findPlayer(f,id);if(!p)return {ok:false,reason:'Select an owned athlete.'};const status=trainingStatus(f,p,programId);if(!status.ok)return status;if(f.cash<status.cost)return {ok:false,reason:'You need $'+(status.cost-f.cash)+' more.'};
    const program=programs[programId],current=cycle(f);if(p.development.cycle!==current){p.development.cycle=current;p.development.counts=Object.fromEntries(Object.keys(programs).map(k=>[k,0]));}if(f.development.trainingCycle!==current){f.development.trainingCycle=current;f.development.trainingUsed=0;}
    const affinity=(p.development.tendency==='Power'&&['strength','balance','contested'].includes(programId))||(p.development.tendency==='Technician'&&['accuracy','release','routes','coverage'].includes(programId))||(p.development.tendency==='Explosive'&&programId==='vertical')||(p.development.tendency==='Ball hawk'&&['swat','interception'].includes(programId));
    const gain=2+(affinity?1:0)+(p.age<=22?1:0),gains={};for(const [stat,weight] of Object.entries(program.stats)){const before=p[stat];p[stat]=Math.min(p.development.ceilings[stat],P.rating(p[stat]+Math.max(1,Math.round(gain*weight))));gains[stat]=p[stat]-before;}
    p.specialties[program.skill]=Math.min(Math.max(p.specialties[program.skill],110,p.development.potential),p.specialties[program.skill]+gain);p.development.counts[programId]++;p.development.sessions[programId]++;f.development.trainingUsed++;if(p.development.academyPoints)p.development.academyPoints--;f.cash-=status.cost;
    if(programId==='vertical'&&f.development.heightMode==='arcade')p.physical.heightGrowthM=Math.min(LIMITS.arcadeGrowthM,p.physical.heightGrowthM+.0025);
    refreshPhysical(p,f.development.heightMode);return {ok:true,cost:status.cost,gains,player:p};
  }
  function awardAcademy(f,{challengeId,score,playerId}={}){
    if(!academyIds.includes(challengeId)||!Number.isFinite(score)||score<0||score>100)return {ok:false,reason:'Invalid academy result.'};normalize(f);const current=cycle(f),tier=score>=95?3:score>=80?2:score>=60?1:0,previous=f.development.academy[challengeId],old=previous?.cycle===current?previous:{cycle:current,score:0,tier:0};
    const payouts=[0,30,70,120],cash=Math.max(0,payouts[tier]-payouts[old.tier]),points=Math.max(0,tier-old.tier),p=findPlayer(f,playerId);
    f.development.academy[challengeId]={cycle:current,score:Math.max(score,old.score),bestScore:Math.max(score,previous?.bestScore||previous?.score||0),tier:Math.max(tier,old.tier)};f.cash+=cash;if(p)p.development.academyPoints=Math.min(100,p.development.academyPoints+points);
    return {ok:true,cash,points:p?points:0,medal:['Practice','Bronze','Silver','Gold'][tier],personalBest:f.development.academy[challengeId].bestScore};
  }
  function setHeightMode(f,mode){if(!['realistic','arcade'].includes(mode))return {ok:false,reason:'Choose realistic or arcade development.'};normalize(f);f.development.heightMode=mode;for(const p of [...roster(f),...f.market,...f.development.pool])refreshPhysical(p,mode);return {ok:true};}
  function describe(p){const sorted=P.stats.slice().sort((a,b)=>p[b]-p[a]);return {overall:overall(p),potential:p.development.potential,strengths:sorted.slice(0,3),weaknesses:sorted.slice(-2).reverse(),playstyle:p.archetype,abilities:p.abilities.slice(),physical:physicalProfile(p)};}
  function builderPreview(input={}){
    const position=positions[input.position]?input.position:'WR',range=positions[position],build=['lean','balanced','power'].includes(input.build)?input.build:'balanced';
    const p={id:'builder_'+hash(JSON.stringify(input)),name:text(input.name,'Custom Athlete',80),position,age:integer(input.age??20,18,34),archetype:position==='WR'?(build==='power'?'Power Receiver':build==='lean'?'Deep Threat':'Route Technician'):range.name,trainings:0};for(const k of P.stats)p[k]=65;
    if(build==='lean'){p.speed=78;p.evasion=75;p.strength=56;p.size=48;}if(build==='power'){p.strength=80;p.catching=72;p.speed=56;p.size=80;}
    p.physical={adultHeightM:clamp(input.heightM??(range.height[0]+range.height[1])/2,...range.height),baseWeightKg:clamp(input.weightKg??(range.weight[0]+range.weight[1])/2,...range.weight),build};enrichPlayer(p);p.price=P.signingPrice(overall(p));return p;
  }
  function buildPlayer(f,input={}){normalize(f);const p=builderPreview(input);if(f.cash<p.price)return {ok:false,reason:'Building this athlete costs $'+p.price+'.'};if(f.bench.length>=LIMITS.bench)return {ok:false,reason:'The bench is full.'};p.id='built_'+f.development.seed.toString(36)+'_'+(f.development.nextId++).toString(36);p.contract={signedRound:f.round,cost:p.price,years:2};f.cash-=p.price;f.bench.push(p);return {ok:true,player:p};}
  const api={LIMITS,positions,programs,academyIds,passPositions,unitPositions,overall,roster,findPlayer,cycle,normalize,validate,enrichPlayer,refreshPhysical,physicalProfile,physique,queryMarket,refreshMarket,sign,substitute,saveLineup,activateLineup,assignUnit,unitPlayer,release,beginSeason,trainingStatus,train,awardAcademy,setHeightMode,describe,builderPreview,buildPlayer};
  if(typeof module!=='undefined')module.exports=api;root.QBDevelopment=api;
})(typeof globalThis!=='undefined'?globalThis:this);
