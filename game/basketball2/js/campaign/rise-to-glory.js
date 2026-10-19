/* Rise to Glory: deterministic campaign data and progression, independent of Tour saves. */
(function(root,factory){
  const api=factory();
  if(typeof module==="object" && module.exports) module.exports=api;
  if(root) root.RiseToGlory=api;
})(typeof globalThis!=="undefined"?globalThis:this,function(){
  "use strict";
  const VERSION=3, SAVE_KEY="basketballRiseToGloryV3", BACKUP_KEY=SAVE_KEY+"Backup", ROSTER_LIMIT=10;
  const SAVE_FORMAT="basketball2-campaign-save", TRANSFER_VERSION=1, MAX_SAVE_BYTES=8*1024*1024;
  const clamp=(n,a,b)=>Math.max(a,Math.min(b,n));
  const integer=(n,a=0,b=1000000)=>clamp(Math.round(Number(n)||0),a,b);
  const clone=value=>JSON.parse(JSON.stringify(value));
  const own=(object,key)=>!!object&&Object.prototype.hasOwnProperty.call(object,key);
  const text=(value,fallback,max=32)=>String(value||fallback).replace(/[<>\x00-\x1f]/g,"").slice(0,max);
  const ok=(message,extra)=>({ok:true,message,...extra});
  const fail=message=>({ok:false,message});
  const color=(value,fallback)=>/^#[0-9a-f]{6}$/i.test(value)?value:fallback;
  const ATTRIBUTES={
    Shooting:[["closeShot","Close shot"],["midrange","Midrange"],["threePoint","Three-point"],["contestedShooting","Contested shooting"],["releaseStability","Release stability"]],
    Finishing:[["layups","Layups"],["dunking","Dunking"],["contactFinishing","Contact finishing"],["standingDunk","Standing dunk"],["aerialControl","Aerial control"]],
    Athleticism:[["speed","Speed"],["acceleration","Acceleration"],["strength","Strength"],["jumping","Jumping"],["agility","Agility"],["stamina","Stamina"],["rebounding","Rebounding"]],
    Playmaking:[["dribbling","Dribbling"],["ballSecurity","Ball security"],["passing","Passing"],["courtVision","Court vision"],["passingAccuracy","Passing accuracy"],["screening","Screening"]],
    Defense:[["perimeterDefense","Perimeter defense"],["interiorDefense","Interior defense"],["blocking","Blocking"],["steals","Steals"],["lateralMovement","Lateral movement"]],
    Mental:[["composure","Composure"],["clutchShooting","Clutch shooting"],["consistency","Consistency"],["basketballIQ","Basketball IQ"]]
  };
  for(const category of Object.keys(ATTRIBUTES)) ATTRIBUTES[category]=ATTRIBUTES[category].map(([key,name])=>({key,name,category}));
  const ATTRIBUTE_LIST=Object.values(ATTRIBUTES).flat();
  const ATTRIBUTE_KEYS=ATTRIBUTE_LIST.map(attribute=>attribute.key);
  const ARCHETYPES={
    playmaker:{name:"Pass-First Playmaker",profileKey:"crafty",runtime:"playmaker",position:"PG",height:[176,194],weight:[73,92],strengths:["passing","courtVision","passingAccuracy","dribbling","ballSecurity"],weaknesses:["standingDunk","blocking","strength"],abilities:["floorGeneral","catchAndShoot","quickRelease","clutchShooter"]},
    sharpshooter:{name:"Perimeter Sharpshooter",profileKey:"deeprange",runtime:"sharpshooter",position:"SG",height:[183,203],weight:[77,98],strengths:["threePoint","releaseStability","consistency","midrange"],weaknesses:["contactFinishing","standingDunk","interiorDefense"],abilities:["deepRange","cornerSpecialist","catchAndShoot","quickRelease","deadeye"]},
    midrange:{name:"Midrange Shot Creator",profileKey:"smooth",runtime:"sharpshooter",position:"SG",height:[185,205],weight:[81,102],strengths:["midrange","releaseStability","dribbling","contestedShooting","composure"],weaknesses:["standingDunk","blocking","threePoint"],abilities:["midrangeAssassin","pullUpSpecialist","stepBackCreator","deadeye","clutchShooter"]},
    slasher:{name:"Explosive Slasher",profileKey:"rocket",runtime:"slasher",position:"SF",height:[183,206],weight:[78,105],strengths:["acceleration","speed","layups","jumping","dunking","aerialControl"],weaknesses:["threePoint","contestedShooting","screening"],abilities:["highFlyer","aerialAcrobat","posterizer","contactSpecialist","stepBackCreator"]},
    interior:{name:"Interior Powerhouse",profileKey:"bruiser",runtime:"interior",position:"C",height:[200,219],weight:[102,129],strengths:["strength","contactFinishing","standingDunk","dunking","rebounding","screening","closeShot"],weaknesses:["speed","dribbling","threePoint","lateralMovement"],abilities:["powerFinisher","contactSpecialist","posterizer","screenSetter","rimGuardian"]},
    defender:{name:"Perimeter Stopper",profileKey:"claw",runtime:"defender",position:"SF",height:[187,208],weight:[85,108],strengths:["perimeterDefense","steals","lateralMovement","agility","stamina"],weaknesses:["contestedShooting","threePoint","standingDunk"],abilities:["pickpocket","rimGuardian","catchAndShoot","screenSetter"]},
    rimProtector:{name:"Rim Protector",profileKey:"tower",runtime:"interior",position:"C",height:[205,222],weight:[105,132],strengths:["blocking","interiorDefense","rebounding","standingDunk","basketballIQ"],weaknesses:["dribbling","speed","threePoint","acceleration"],abilities:["rimGuardian","powerFinisher","screenSetter","contactSpecialist"]},
    stretchBig:{name:"Stretch Big",profileKey:"sky",runtime:"sharpshooter",position:"PF",height:[199,215],weight:[95,119],strengths:["threePoint","midrange","screening","releaseStability","rebounding"],weaknesses:["acceleration","dribbling","contactFinishing"],abilities:["catchAndShoot","cornerSpecialist","deepRange","screenSetter","rimGuardian"]},
    allRounder:{name:"Two-Way All-Rounder",profileKey:"sky",runtime:"all_rounder",position:"SF",height:[189,210],weight:[87,113],strengths:["layups","passing","perimeterDefense","basketballIQ","stamina"],weaknesses:["standingDunk","contestedShooting"],abilities:["catchAndShoot","clutchShooter","floorGeneral","contactSpecialist","pickpocket"]}
  };
  const ABILITIES={
    powerFinisher:{name:"Power Finisher",branch:"power",requirements:{dunking:62,strength:62},description:"Two-handed power gathers resist smaller defenders; +6% momentum contact control per rank."},
    highFlyer:{name:"High Flyer",branch:"aerial",requirements:{jumping:65,dunking:60},description:"Unlock one-handed aerial finishes and +5% takeoff lift per rank."},
    contactSpecialist:{name:"Contact Specialist",branch:"power",requirements:{contactFinishing:68,strength:60},description:"Absorb moderate contact; +7% contact gather stability per rank."},
    aerialAcrobat:{name:"Aerial Acrobat",branch:"aerial",requirements:{aerialControl:70,jumping:65},requires:"highFlyer",description:"Unlock reverse and twisting finishes; improve difficult-angle gather control."},
    posterizer:{name:"Posterizer",branch:"power",requirements:{dunking:85,jumping:78,contactFinishing:78},requiresAny:["highFlyer","powerFinisher"],level:8,description:"Elite contact finishes from an earned runway. Positioned rim protection can still stop the dunk."},
    midrangeAssassin:{name:"Midrange Assassin",branch:"midrange",requirements:{midrange:60,releaseStability:55},description:"+3 percentage points to balanced midrange accuracy per rank."},
    deadeye:{name:"Deadeye",branch:"shotCreation",requirements:{contestedShooting:68,composure:62},description:"Reduce moderate contest penalties by 8% per rank; heavy contests remain difficult."},
    catchAndShoot:{name:"Catch & Shoot",branch:"perimeter",requirements:{releaseStability:58,threePoint:54},description:"+4% release consistency per rank after a timely pass."},
    pullUpSpecialist:{name:"Pull-Up Specialist",branch:"midrange",requirements:{midrange:67,agility:60},requires:"midrangeAssassin",description:"+4% balance recovery per rank when stopping smoothly from a drive."},
    stepBackCreator:{name:"Step-Back Creator",branch:"shotCreation",requirements:{dribbling:68,agility:65},description:"+6% controlled step-back separation and balance per rank."},
    cornerSpecialist:{name:"Corner Specialist",branch:"perimeter",requirements:{threePoint:65,releaseStability:60},requires:"catchAndShoot",description:"+3 percentage points on balanced corner catch-and-shoot attempts per rank."},
    deepRange:{name:"Deep Range",branch:"perimeter",requirements:{threePoint:75,releaseStability:65},description:"Extend effective three-point distance by 0.45 court units per rank."},
    clutchShooter:{name:"Clutch Shooter",branch:"shotCreation",requirements:{clutchShooting:65,composure:65},description:"Reduce late-game timing penalties by 10% per rank in close games."},
    quickRelease:{name:"Quick Release",branch:"shotCreation",requirements:{releaseStability:72,consistency:62},description:"Release 6% faster per rank with a slightly tighter timing requirement."},
    floorGeneral:{name:"Floor General",branch:"playmaking",requirements:{passing:65,courtVision:65},description:"Earlier cutting reads and +5% pass reception stability per rank."},
    rimGuardian:{name:"Rim Guardian",branch:"defense",requirements:{blocking:65,interiorDefense:62},description:"Better vertical contest discipline and +5% well-timed block control per rank."},
    pickpocket:{name:"Pickpocket",branch:"defense",requirements:{steals:65,perimeterDefense:60},description:"+5% controlled steal reach per rank when the ball is exposed."},
    screenSetter:{name:"Screen Setter",branch:"playmaking",requirements:{screening:62,strength:58},description:"Stable screen angles and +6% teammate route awareness per rank."}
  };
  // Describe the physical effects used by player-development.js, not a promised make percentage.
  const ABILITY_EFFECTS={
    powerFinisher:"Two-handed power finishes: +12% slam force per rank, with stronger contact resistance when carrying momentum.",
    highFlyer:"One-handed aerial finishes: +0.085 m takeoff reach and +0.09 m gather range per rank.",
    contactSpecialist:"Unlock contact finishes and add 6.5 percentage points of gather contact resistance per rank.",
    aerialAcrobat:"Unlock reverse and windmill finishes; +10 percentage points of aerial control per rank and more forgiving approach angles.",
    posterizer:"Unlock elite contact posters from a controlled runway; +3.5 percentage points of contact resistance per rank. A positioned rim protector can still stop the finish.",
    midrangeAssassin:"Reduce balanced midrange launch dispersion by 0.012 m per rank. Timing, balance and contests still decide the attempt.",
    deadeye:"Reduce moderate defensive pressure by 13% per rank, subject to the combined contest-resistance cap. Heavy contests still matter.",
    catchAndShoot:"Reduce launch dispersion by up to 0.014 m per rank after a quality pass received within 1.35 seconds, while balanced.",
    pullUpSpecialist:"Reduce controlled pull-up launch dispersion by 0.012 m per rank; tolerate more movement after a smooth stop.",
    stepBackCreator:"Add 0.38 m/s to controlled step-back separation per rank and reduce balanced step-back launch dispersion by 0.013 m per rank.",
    cornerSpecialist:"Reduce corner catch-and-shoot launch dispersion by 0.018 m per rank after a timely pass.",
    deepRange:"Extend effective three-point range by 0.7 court units per rank before additional distance dispersion applies.",
    clutchShooter:"Increase timing tolerance by 13% per rank in close late-game situations.",
    quickRelease:"Fill the shot meter 13% faster and shorten follow-through by 55 ms per rank. The green timing half-window narrows by 0.007.",
    rimGuardian:"Increase well-timed block contact reach by 5% per rank; positioning and timing remain essential.",
    pickpocket:"Increase controlled steal reach against an exposed ball by 5% per rank. Protected dribbles remain difficult."
  };
  for(const [key,description] of Object.entries(ABILITY_EFFECTS)) ABILITIES[key].description=description;
  for(const [key,ability] of Object.entries(ABILITIES)) Object.assign(ability,{key,maxRank:3,cost:2});
  const ROLES=["balanced","primary","shooter","playmaker","slasher","anchor","stopper","sixth_man"];
  const OFFENSES=["motion","pick_roll","pick_pop","drive_kick","isolation","give_go","post_up","fast_break"];
  const DEFENSES=["balanced","protect_paint","perimeter","pressure","switch"];
  const LOGOS=["comet","crown","bolt","falcon","mountain","orbit","shield","wave"];
  const JERSEYS=["classic","stripe","chevron","split","city"];
  const DIVISIONS=[
    {id:"street",name:"Street League",courtKey:"seattleRooftop",difficulty:"easy",base:48,reward:450,offense:"motion",defense:"balanced"},
    {id:"city",name:"City League",courtKey:"newYorkCage",difficulty:"normal",base:57,reward:650,offense:"drive_kick",defense:"pressure"},
    {id:"regional",name:"Regional League",courtKey:"chicagoHardwood",difficulty:"hard",base:65,reward:900,offense:"pick_roll",defense:"switch"},
    {id:"national",name:"National League",courtKey:"bostonLegacy",difficulty:"elite",base:74,reward:1200,offense:"pick_pop",defense:"perimeter"},
    {id:"elite",name:"Elite Championship",courtKey:"vegasFinals",difficulty:"impossible",base:83,reward:1600,offense:"motion",defense:"switch"}
  ];
  const TRAINING=[
    {id:"midrange",name:"Elbow Workshop",description:"Take balanced midrange shots with the actual shot meter.",attribute:"midrange",attempts:8,target:4,teamSize:1,kind:"shooting",distance:6},
    {id:"threePoint",name:"Around the Arc",description:"Create open threes and hold a consistent release.",attribute:"threePoint",attempts:8,target:3,teamSize:1,kind:"shooting",distance:9},
    {id:"dunking",name:"Above the Rim",description:"Earn a runway and finish at the rim through the real gather system.",attribute:"dunking",attempts:6,target:3,teamSize:1,kind:"dunking",distance:4},
    {id:"passing",name:"Passing Windows",description:"Deliver purposeful completed passes to moving teammates.",attribute:"passingAccuracy",attempts:8,target:6,teamSize:3,kind:"passing"},
    {id:"dribbling",name:"Handle Lab",description:"Protect the ball and beat defenders with controlled changes of direction.",attribute:"ballSecurity",attempts:6,target:4,teamSize:1,kind:"dribbling"},
    {id:"defense",name:"Defensive Slides",description:"Contest, steal, block or rebound without conceding easy baskets.",attribute:"lateralMovement",attempts:6,target:3,teamSize:1,kind:"defense"},
    {id:"team",name:"Five-Pass Advantage",description:"Execute team offense and create assisted baskets in a live scrimmage.",attribute:"courtVision",attempts:6,target:3,teamSize:3,kind:"team"}
  ];
  const TRAINING_SIGNATURES={midrange:["pullUpSpecialist","midrangeAssassin","deadeye"],threePoint:["deepRange","cornerSpecialist","catchAndShoot"],
    dunking:["posterizer","aerialAcrobat","contactSpecialist","highFlyer","powerFinisher"],passing:["floorGeneral","catchAndShoot"],
    dribbling:["stepBackCreator","quickRelease","pickpocket"],defense:["rimGuardian","pickpocket"],team:["screenSetter","floorGeneral","catchAndShoot"]};
  const FIRST=["Jalen","Micah","Kai","Andre","Nico","Malik","Isaiah","Elias","Devin","Cam","Darius","Trey","Milo","Noah","Jordan","Reese","Tyler","Quinn","Zion","Dante","Jules","Leo","Amari","Owen"];
  const LAST=["Cole","Reyes","Ward","Monroe","King","Price","Bell","Grant","Harris","Vale","Stone","Cross","Vaughn","Brooks","Hayes","Fox","Banks","West","Lane","Carter","Santos","Park","Young","Reed"];
  const ARCHETYPE_KEYS=Object.keys(ARCHETYPES);
  const skinTones=[0x5d3425,0xc8845b,0x704327,0xe1aa7a,0x3d2418,0x8f5737,0xd49a6a];
  function random(seed){let n=integer(seed,1,2147483647);return ()=>{n=(Math.imul(n,1664525)+1013904223)>>>0;return n/4294967296;};}
  function average(values){return values.reduce((sum,n)=>sum+n,0)/Math.max(1,values.length);}
  function overall(player){
    const archetype=ARCHETYPES[player.archetype]||ARCHETYPES.allRounder;
    return Math.round(average(ATTRIBUTE_KEYS.map(key=>player.ratings[key]))*.62+average(archetype.strengths.map(key=>player.ratings[key]))*.38);
  }
  function tier(value){return value<54?"Prospect":value<63?"Role Player":value<72?"Starter":value<82?"Star":value<91?"Superstar":"Legend";}
  function refresh(player){player.overall=overall(player);player.tier=tier(player.overall);return player;}
  function generatePlayer(index,seed=24,baseOverride){
    const rng=random(seed*997+index*271+19);
    const archetype=ARCHETYPE_KEYS[index%ARCHETYPE_KEYS.length], definition=ARCHETYPES[archetype];
    const secondaryArchetype=ARCHETYPE_KEYS[(index+3+Math.floor(rng()*4))%ARCHETYPE_KEYS.length];
    const secondary=ARCHETYPES[secondaryArchetype];
    const band=index<15?0:Math.min(4,Math.floor((index-15)/17));
    const base=baseOverride===undefined?(index<15?43+Math.floor(rng()*8):47+band*9+Math.floor(rng()*7)):baseOverride;
    const heightCm=Math.round(definition.height[0]+rng()*(definition.height[1]-definition.height[0]));
    const weightKg=Math.round(definition.weight[0]+rng()*(definition.weight[1]-definition.weight[0]));
    const ratings={},caps={};
    const potentialRoll=74+rng()*19+(index<15?3:0);
    const potential=integer(baseOverride===undefined?potentialRoll:baseOverride+28,70,99);
    for(const key of ATTRIBUTE_KEYS){
      const specialized=definition.strengths.includes(key),weak=definition.weaknesses.includes(key),support=secondary.strengths.includes(key);
      let rating=base+Math.round(rng()*12-6)+(specialized?12:0)+(support?3:0)-(weak?13:0);
      let cap=potential+(specialized?3:weak?-21:support?-5:-10);
      if(heightCm>206 && ["speed","acceleration","agility","lateralMovement","dribbling"].includes(key)) cap-=8;
      if(heightCm<189 && ["standingDunk","blocking","interiorDefense","rebounding"].includes(key)) cap-=12;
      if(weightKg<88 && ["strength","contactFinishing","screening"].includes(key)) cap-=8;
      caps[key]=integer(cap,48,99); ratings[key]=integer(rating,25,caps[key]);
    }
    const abilityPotential=[...new Set([...definition.abilities,...secondary.abilities.slice(0,2)])];
    const player={id:`rtg-${seed}-${index}`,name:`${FIRST[index%FIRST.length]} ${LAST[(Math.floor(index/FIRST.length)*7+index*7)%LAST.length]}`,
      age:18+Math.floor(rng()*(index<15?4:13)),position:definition.position,archetype,secondaryArchetype,profileKey:definition.profileKey,
      physical:{heightCm,weightKg},appearance:{skin:skinTones[index%skinTones.length],hair:0x1b110c,hairStyle:["shortFade","braids","buzz","afro","spiky"][index%5],number:String(1+index%98)},equipment:{},
      ratings,caps,potential,abilityPotential,abilities:{},branches:[],xp:0,level:1,upgradePoints:index<15?2:0,invested:0,
      role:"balanced",priority:1,signingCost:0,unlockDivision:band,availableSeason:1,training:{},trainingTokens:{},stats:emptyStats(),development:{levels:0,ratingGains:0,trainingSessions:0},
      strengths:definition.strengths.slice(0,3),weaknesses:definition.weaknesses.slice(0,2),
      scoutingReport:`${definition.name} with ${secondary.name.toLowerCase()} tendencies. ${definition.strengths.slice(0,2).map(label).join(" and ")} create advantages; ${definition.weaknesses.slice(0,2).map(label).join(" and ")} need protection. ${heightCm<190?"Explosive guard size limits interior reach.":heightCm>207?"Long reach and screen presence come with slower lateral recovery.":"Versatile wing frame fits several matchups."}`};
    refresh(player);
    player.signingCost=index<15?110+integer((player.overall-40)*8,0,250):integer(200+Math.pow(Math.max(0,player.overall-42),1.6)*6,180,6500);
    if(index>=15 && band>=2){
      for(const key of abilityPotential){if(Object.entries(ABILITIES[key].requirements).every(([stat,value])=>ratings[stat]>=value)&&!ABILITIES[key].requires&&!ABILITIES[key].requiresAny) player.abilities[key]=band>=4?2:1;}
      player.level=1+band*2;
    }
    return player;
  }
  function label(key){return ATTRIBUTE_LIST.find(attribute=>attribute.key===key)?.name||key;}
  function generateMarket(seed=24,count=96){return Array.from({length:integer(count,15,160)},(_,index)=>generatePlayer(index,seed));}
  function rookiePool(seed=24){return generateMarket(seed,15);}
  function emptyStats(){return {games:0,wins:0,losses:0,points:0,assists:0,rebounds:0,steals:0,blocks:0,screens:0,stops:0,goodShots:0,fgm:0,fga:0,threeMade:0,threeAttempts:0,dunks:0,turnovers:0};}
  function create(options={}){
    const seed=integer(options.seed||24,1,999999);
    const players=Object.fromEntries(generateMarket(seed).map(player=>[player.id,player]));
    const requested=Array.isArray(options.starterIds)?[...new Set(options.starterIds)]:[];
    const rookies=Object.values(players).slice(0,15);
    const selected=requested.length===3&&requested.every(id=>rookies.some(player=>player.id===id))?requested:[rookies[0].id,rookies[1].id,rookies[4].id];
    const captain=players[selected[0]];
    captain.name=text(options.captainName,captain.name);captain.captain=true;captain.upgradePoints=4;
    captain.appearance.number=String(integer(options.captainNumber??captain.appearance.number,0,99));
    if(["shortFade","braids","buzz","afro","spiky","headband"].includes(options.captainHairStyle)) captain.appearance.hairStyle=options.captainHairStyle;
    if(skinTones.includes(Number(options.captainSkin))) captain.appearance.skin=Number(options.captainSkin);
    if(options.captainPosition && ["PG","SG","SF","PF","C"].includes(options.captainPosition)) captain.position=options.captainPosition;
    const state={version:VERSION,mode:"rise-to-glory",seed,createdAt:Date.now(),updatedAt:Date.now(),
      team:{name:text(options.name,"Neighborhood Hoops"),primaryColor:color(options.primaryColor,"#ff6b35"),secondaryColor:color(options.secondaryColor,"#15253f"),
        jerseyStyle:JERSEYS.includes(options.jerseyStyle)?options.jerseyStyle:"classic",logo:LOGOS.includes(options.logo)?options.logo:"comet",
        offense:OFFENSES.includes(options.offense)?options.offense:"motion",defense:DEFENSES.includes(options.defense)?options.defense:"balanced",matchups:{},substitution:"balanced"},
      players,roster:selected,starters:selected.slice(),captainId:captain.id,currency:720,teamXP:0,level:1,record:{wins:0,losses:0},
      season:null,history:[],awards:[],cosmetics:["classic"],lastReward:null,trainingCredits:{},rivalries:{},revision:0};
    state.season=createSeason(state,0,1);
    return state;
  }
  const OPPONENT_NAMES=["Court Comets","Iron Borough","Arc Society","Skyline Flight","Glass Union","Midnight Motion","Crown Collective"];
  const OPPONENT_ARCHETYPES=[["playmaker","slasher","sharpshooter"],["defender","interior","rimProtector"],["sharpshooter","stretchBig","midrange"],["slasher","slasher","playmaker"],["rimProtector","stretchBig","defender"],["midrange","playmaker","interior"],["allRounder","midrange","rimProtector"]];
  function createSeason(state,division,number){
    const definition=DIVISIONS[division];
    const opponents=OPPONENT_NAMES.map((name,index)=>{
      const players=OPPONENT_ARCHETYPES[index].map((archetype,slot)=>{
        const target=ARCHETYPE_KEYS.indexOf(archetype);
        const playerIndex=180+index*27+slot*9+target;
        const player=generatePlayer(playerIndex,state.seed+number*19+division*61,definition.base+Math.min(7,number-1)*2+(index===6?4:0));
        player.id=`cpu-${number}-${division}-${index}-${slot}`;player.unlockDivision=0;
        return player;
      });
      return {id:`rival-${index}`,name,players,offense:["pick_roll","post_up","pick_pop","fast_break","give_go","motion","isolation"][index],
        defense:["balanced","protect_paint","perimeter","pressure","protect_paint","switch","switch"][index],
        identity:["Cuts, passes and clean catch-and-shoot chances","Physical screens, paint seals and disciplined help","Two shooting threats stretch every defensive rotation","Explosive transition attacks and airborne finishers","Rim protection, rebounds and second chances","Elbow actions and patient post entries","Adaptive two-way stars and championship execution"][index],rival:index===6};
    });
    const schedule=[];
    for(let index=0;index<7;index++){
      schedule.push({id:`s${number}-d${division}-g${index}`,round:schedule.length+1,opponentId:opponents[index].id,phase:"regular",kind:"League game",teamSize:3,targetScore:15,status:"upcoming"});
      if(index===1||index===4) schedule.push({id:`s${number}-d${division}-c${index}`,round:schedule.length+1,opponentId:opponents[6-index].id,phase:"challenge",kind:index===1?"1v1 Rivalry":"2v2 Cup",teamSize:index===1?1:2,targetScore:11,status:"upcoming"});
    }
    return {number,division,phase:"regular",opponents,schedule,completed:[],playerStats:{},standings:[{id:"player",name:state.team.name,wins:0,losses:0,pointsFor:0,pointsAgainst:0},...opponents.map(opponent=>({id:opponent.id,name:opponent.name,wins:0,losses:0,pointsFor:0,pointsAgainst:0}))],championships:0,champion:null};
  }
  function roster(state){return (state?.roster||[]).map(id=>state.players[id]).filter(Boolean);}
  function player(state,id){return typeof id==="string"&&own(state?.players,id)?state.players[id]:null;}
  function market(state,options={}){
    let players=Object.values(state.players).filter(player=>!state.roster.includes(player.id)&&player.availableSeason<=state.season.number&&player.unlockDivision<=state.season.division);
    if(options.position) players=players.filter(player=>player.position===options.position);
    if(options.archetype) players=players.filter(player=>player.archetype===options.archetype);
    if(options.affordable) players=players.filter(player=>player.signingCost<=state.currency);
    const sort=options.sort||"overall";
    return players.sort((a,b)=>sort==="cost"?a.signingCost-b.signingCost:sort==="potential"?b.potential-a.potential:b.overall-a.overall);
  }
  function touch(state){state.revision=(state.revision||0)+1;state.updatedAt=Date.now();}
  function sign(state,id){
    const recruit=player(state,id);
    if(!recruit||state.roster.includes(id)) return fail("Choose an available recruit.");
    if(state.roster.length>=ROSTER_LIMIT) return fail("Your 10-player roster is full. Release a bench player first.");
    if(recruit.unlockDivision>state.season.division||recruit.availableSeason>state.season.number) return fail("This prospect is not available at your division yet.");
    if(state.currency<recruit.signingCost) return fail("Earn more team credits to sign this player.");
    state.currency-=recruit.signingCost;state.roster.push(id);touch(state);
    return ok(`${recruit.name} signed.`,{player:recruit,cost:recruit.signingCost});
  }
  function release(state,id){
    if(!state.roster.includes(id)) return fail("This player is not on your roster.");
    if(state.roster.length<=3) return fail("Keep at least three athletes on your roster.");
    if(id===state.captainId) return fail("Your captain stays with the franchise.");
    const athlete=player(state,id),refund=Math.round(athlete.signingCost*.3);
    state.roster=state.roster.filter(value=>value!==id);state.starters=state.starters.filter(value=>value!==id);
    for(const candidate of state.roster) if(state.starters.length<3&&!state.starters.includes(candidate)) state.starters.push(candidate);
    state.currency+=refund;touch(state);return ok(`${athlete.name} released.`,{refund});
  }
  function setStarters(state,ids){
    if(!Array.isArray(ids)||ids.length!==3||new Set(ids).size!==3||!ids.every(id=>state.roster.includes(id))) return fail("Choose three different roster players.");
    state.starters=ids.slice();touch(state);return ok("Starting lineup updated.");
  }
  function setRole(state,id,role,priority){
    const athlete=player(state,id);
    if(!state.roster.includes(id)||!ROLES.includes(role)) return fail("Choose a valid roster player and role.");
    athlete.role=role;if(priority!==undefined) athlete.priority=integer(priority,1,3);touch(state);return ok("Player assignment updated.");
  }
  function setStrategy(state,options={}){
    if(OFFENSES.includes(options.offense)) state.team.offense=options.offense;
    if(DEFENSES.includes(options.defense)) state.team.defense=options.defense;
    if(["balanced","fresh_legs","matchup","starters"].includes(options.substitution)) state.team.substitution=options.substitution;
    if(options.matchups && typeof options.matchups==="object") state.team.matchups=Object.fromEntries(Object.entries(options.matchups).filter(([id,target])=>state.roster.includes(id)&&typeof target==="string").slice(0,10));
    touch(state);return ok("Game plan saved.");
  }
  function xpRequired(level){return 100+level*45;}
  function addXP(athlete,amount){
    const gained=integer(amount,0,1500);athlete.xp+=gained;let levels=0;
    while(athlete.xp>=xpRequired(athlete.level)&&athlete.level<35){athlete.xp-=xpRequired(athlete.level);athlete.level++;athlete.upgradePoints+=3;athlete.development.levels++;levels++;}
    return {xp:gained,levels,upgradePoints:levels*3};
  }
  function upgradePreview(state,id,key){
    const athlete=player(state,id);
    if(!athlete||!ATTRIBUTE_KEYS.includes(key)) return null;
    const current=athlete.ratings[key],cost=current>=80?3:current>=68?2:1,gain=Math.min(athlete.caps[key]-current,current>=85?1:2);
    return {key,name:label(key),current,next:current+gain,cap:athlete.caps[key],gain,cost,available:state.roster.includes(id)&&gain>0&&athlete.upgradePoints>=cost&&athlete.invested<240};
  }
  function upgrade(state,id,key){
    const athlete=player(state,id),preview=upgradePreview(state,id,key);
    if(!preview||!state.roster.includes(id)) return fail("Choose a roster athlete and attribute.");
    if(preview.gain<=0) return fail("This player has reached their physical or archetype cap in this rating.");
    if(athlete.invested>=240) return fail("Development focus is complete. This athlete cannot master every skill.");
    if(athlete.upgradePoints<preview.cost) return fail(`This upgrade costs ${preview.cost} upgrade points.`);
    athlete.ratings[key]=preview.next;athlete.upgradePoints-=preview.cost;athlete.invested+=preview.gain;athlete.development.ratingGains+=preview.gain;refresh(athlete);touch(state);
    return ok(`${preview.name}: ${preview.current} → ${preview.next}.`,preview);
  }
  function abilityPreview(state,id,key){
    const athlete=player(state,id),ability=own(ABILITIES,key)?ABILITIES[key]:null;
    if(!athlete||!ability) return null;
    const rank=athlete.abilities[key]||0,trainingToken=rank===0&&!!athlete.trainingTokens?.[key],cost=trainingToken?0:2+rank,missing=[];
    if(!athlete.abilityPotential.includes(key)) missing.push("Outside signature potential");
    if(rank>=3) missing.push("Maximum rank");
    if(athlete.level<(ability.level||1)) missing.push(`Level ${ability.level}`);
    for(const [rating,value] of Object.entries(ability.requirements)) if(athlete.ratings[rating]<value+rank*4) missing.push(`${label(rating)} ${value+rank*4}`);
    if(ability.requires&&!athlete.abilities[ability.requires]) missing.push(ABILITIES[ability.requires].name);
    if(ability.requiresAny&&!ability.requiresAny.some(candidate=>athlete.abilities[candidate])) missing.push(ability.requiresAny.map(candidate=>ABILITIES[candidate].name).join(" or "));
    if(!athlete.branches.includes(ability.branch)&&athlete.branches.length>=2) missing.push("Two specialization branches already selected");
    if(athlete.upgradePoints<cost) missing.push(`${cost} upgrade points`);
    return {...ability,rank,nextRank:rank+1,cost,trainingToken,missing,available:state.roster.includes(id)&&missing.length===0};
  }
  function learnAbility(state,id,key){
    const athlete=player(state,id),preview=abilityPreview(state,id,key);
    if(!preview||!state.roster.includes(id)) return fail("Choose a roster player and signature ability.");
    if(!preview.available) return fail(preview.missing.join(" · "));
    athlete.upgradePoints-=preview.cost;athlete.abilities[key]=preview.nextRank;if(athlete.trainingTokens) delete athlete.trainingTokens[key];if(!athlete.branches.includes(preview.branch)) athlete.branches.push(preview.branch);touch(state);
    return ok(`${preview.name} rank ${preview.nextRank} unlocked.`,{ability:key,rank:preview.nextRank});
  }
  function contributionXP(stats,won){
    return integer(28+(won?32:12)+integer(stats.points)*2+integer(stats.assists)*12+integer(stats.rebounds)*9+integer(stats.steals)*13+integer(stats.blocks)*14+integer(stats.screens)*8+integer(stats.stops)*10+integer(stats.goodShots)*5-Math.min(18,integer(stats.turnovers)*3),25,420);
  }
  function getStandings(state){return state.season.standings.slice().sort((a,b)=>b.wins-a.wins||(b.pointsFor-b.pointsAgainst)-(a.pointsFor-a.pointsAgainst)||a.id.localeCompare(b.id));}
  function nextGame(state){return state?.season?.schedule.find(game=>game.status!=="complete")||null;}
  function opponent(state,game=nextGame(state)){
    const rival=state.season.opponents.find(team=>team.id===game?.opponentId);
    return rival?{...rival,rivalryRecord:{...(state.rivalries?.[rival.id]||{wins:0,losses:0})}}:null;
  }
  function prepareMatch(state){
    const game=nextGame(state),rival=opponent(state,game);
    if(!game||!rival) return null;
    const selected=state.starters.map(id=>player(state,id)).filter(Boolean).slice(0,game.teamSize);
    return {game:clone(game),home:selected.map(clone),away:rival.players.slice(0,game.teamSize).map(clone),bench:roster(state).filter(athlete=>!selected.some(starter=>starter.id===athlete.id)).map(clone),
      teamSize:game.teamSize,offense:state.team.offense,defense:state.team.defense,awayOffense:rival.offense,awayDefense:rival.defense,difficulty:DIVISIONS[state.season.division].difficulty,courtKey:DIVISIONS[state.season.division].courtKey,
      chemistry:chemistry(selected),opponent:rival.name,teamName:state.team.name,targetScore:game.targetScore};
  }
  function recordStanding(season,id,otherId,score,otherScore){
    const team=season.standings.find(team=>team.id===id),other=season.standings.find(team=>team.id===otherId);
    if(!team||!other) return;
    team.pointsFor+=score;team.pointsAgainst+=otherScore;other.pointsFor+=otherScore;other.pointsAgainst+=score;
    if(score>otherScore){team.wins++;other.losses++;}else{team.losses++;other.wins++;}
  }
  function simulateRound(state,game){
    const others=state.season.opponents.filter(team=>team.id!==game.opponentId);
    const rng=random(state.seed+state.season.number*733+state.season.completed.length*131);
    for(let index=0;index<others.length;index+=2){
      const a=others[index],b=others[index+1];
      const strengthA=average(a.players.map(athlete=>athlete.overall)),strengthB=average(b.players.map(athlete=>athlete.overall));
      const aWins=rng()<clamp(.5+(strengthA-strengthB)*.025,.18,.82),losing=7+Math.floor(rng()*7);
      recordStanding(state.season,a.id,b.id,aWins?15:losing,aWins?losing:15);
    }
  }
  function finishSeason(state,champion){
    const season=state.season;season.phase="offseason";season.champion=champion?"player":(season.champion||opponent(state,season.schedule.at(-1))?.id||"cpu");
    const reward=champion?DIVISIONS[season.division].reward:180+season.division*75;
    state.currency+=reward;
    const award={season:season.number,division:season.division,name:champion?`${DIVISIONS[season.division].name} Champions`:"Season Development",reward};
    state.awards.push(award);
    if(champion){
      season.championships=1;const cosmetic=["streetStripe","cityPulse","regionalChevron","nationalGold","eliteCrown"][season.division];
      if(!state.cosmetics.includes(cosmetic)) state.cosmetics.push(cosmetic);
      const contribution=athlete=>{const stats=season.playerStats[athlete.id]||emptyStats();return stats.points+stats.assists*2+stats.blocks*3+stats.rebounds+stats.steals*2+stats.screens+stats.stops;};
      const mvp=roster(state).slice().sort((a,b)=>contribution(b)-contribution(a)||a.id.localeCompare(b.id))[0];
      state.awards.push({season:season.number,name:"Tournament MVP",playerId:mvp.id,playerName:mvp.name});
      state.lastReward={...award,cosmetic};
    }else state.lastReward=award;
    const developed=roster(state).slice().sort((a,b)=>b.development.ratingGains-a.development.ratingGains)[0];
    if(developed?.development.ratingGains) state.awards.push({season:season.number,name:"Player Development Award",playerId:developed.id,playerName:developed.name});
    state.history.push({number:season.number,division:season.division,champion,standings:clone(getStandings(state)),games:clone(season.completed)});
    state.history=state.history.slice(-20);
  }
  function progressSchedule(state,game,won){
    const season=state.season;
    if(game.phase==="playoff"){
      if(!won){season.champion=game.opponentId;finishSeason(state,false);return;}
      if(game.kind==="Championship"){finishSeason(state,true);return;}
      const finalist=getStandings(state).find(team=>team.id!=="player"&&team.id!==game.opponentId);
      season.phase="final";season.schedule.push({id:`s${season.number}-final`,round:season.schedule.length+1,opponentId:finalist.id,phase:"playoff",kind:"Championship",teamSize:3,targetScore:21,status:"upcoming"});return;
    }
    if(nextGame(state)) return;
    const standings=getStandings(state),seed=standings.findIndex(team=>team.id==="player");
    if(seed>=4){finishSeason(state,false);return;}
    season.phase="playoffs";
    const pairing=standings[3-seed];
    season.schedule.push({id:`s${season.number}-semi`,round:season.schedule.length+1,opponentId:pairing.id,phase:"playoff",kind:"Semifinal",teamSize:3,targetScore:17,status:"upcoming"});
  }
  function sanitizeBoxRows(state,rows){
    if(!Array.isArray(rows)) return [];
    const seen=new Set();
    return rows.filter(row=>row&&player(state,row.id)&&!seen.has(row.id)&&(seen.add(row.id)||true)).slice(0,ROSTER_LIMIT).map(row=>{
      const stats=emptyStats();for(const key of Object.keys(stats)) stats[key]=integer(row.stats?.[key],0,199);
      return {id:row.id,stats};
    });
  }
  function recordSeasonContribution(state,id,stats,won){
    const total=state.season.playerStats[id]||(state.season.playerStats[id]=emptyStats());
    for(const key of Object.keys(total)) if(!["games","wins","losses"].includes(key)) total[key]+=integer(stats[key],0,199);
    total.games++;total[won?"wins":"losses"]++;
  }
  function recordGame(state,result={}){
    const game=nextGame(state);
    if(!game) return fail("This season is complete. Begin the next season from the hub.");
    if(result.gameId&&result.gameId!==game.id) return fail("This result belongs to an earlier fixture and was not awarded twice.");
    const won=!!result.won;
    let homeScore=integer(result.homeScore,0,199),awayScore=integer(result.awayScore,0,199);
    if(homeScore===awayScore||(homeScore>awayScore)!==won){homeScore=won?game.targetScore:Math.max(0,game.targetScore-4);awayScore=won?Math.max(0,game.targetScore-4):game.targetScore;}
    const rows=Array.isArray(result.players)?result.players:[];
    const awards=[],boxRows=[];
    for(const athlete of roster(state)){
      const contribution=rows.find(row=>row.id===athlete.id),played=!!contribution||state.starters.slice(0,game.teamSize).includes(athlete.id);
      if(!played) continue;
      const stats=contribution?.stats||{};
      for(const key of Object.keys(emptyStats())) if(!["games","wins","losses"].includes(key)) athlete.stats[key]+=integer(stats[key],0,199);
      athlete.stats.games++;athlete.stats[won?"wins":"losses"]++;
      const box=sanitizeBoxRows(state,[{id:athlete.id,stats}])[0];boxRows.push(box);recordSeasonContribution(state,athlete.id,box.stats,won);
      awards.push({id:athlete.id,name:athlete.name,...addXP(athlete,contributionXP(stats,won))});
    }
    const credits=(won?165:80)+state.season.division*35+(game.phase==="challenge"&&won?100:0)+(game.phase==="playoff"&&won?100:0);
    state.currency+=credits;state.teamXP+=won?100:45;state.level=1+Math.floor(state.teamXP/300);state.record[won?"wins":"losses"]++;
    game.status="complete";game.result={won,homeScore,awayScore};
    const completed={id:game.id,kind:game.kind,opponentId:game.opponentId,opponentName:opponent(state,game).name,won,homeScore,awayScore,credits,players:boxRows};
    state.season.completed.push(completed);
    state.rivalries||={};state.rivalries[game.opponentId]||={wins:0,losses:0};state.rivalries[game.opponentId][won?"wins":"losses"]++;
    state.lastReward={name:won?"Victory":"Game complete",credits,players:awards};
    if(game.phase==="regular"){recordStanding(state.season,"player",game.opponentId,homeScore,awayScore);simulateRound(state,game);}
    if(game.phase==="challenge"&&won) state.awards.push({season:state.season.number,name:game.kind,credits:100});
    progressSchedule(state,game,won);touch(state);
    return ok(won?"Victory! Team credits and contribution XP awarded.":"Game complete. Your athletes earned contribution XP.",{credits,players:awards,result:completed,seasonComplete:state.season.phase==="offseason"});
  }
  function advanceSeason(state){
    if(state.season.phase!=="offseason") return fail("Finish this season before entering the offseason.");
    const promoted=state.season.champion==="player",division=Math.min(4,state.season.division+(promoted?1:0)),number=state.season.number+1;
    for(const athlete of roster(state)){
      athlete.age++;addXP(athlete,60);athlete.training={};
      if(athlete.age>31){for(const key of ["speed","acceleration","jumping"]) athlete.ratings[key]=Math.max(25,athlete.ratings[key]-1);refresh(athlete);}
    }
    for(let index=0;index<9;index++){
      const prospect=generatePlayer(1000+number*9+index,state.seed,45+division*7);
      prospect.id=`prospect-${number}-${index}`;prospect.age=18+index%3;prospect.availableSeason=number;prospect.unlockDivision=division;
      prospect.signingCost=200+division*180+index*28;state.players[prospect.id]=prospect;
    }
    state.season=createSeason(state,division,number);state.trainingCredits={};touch(state);
    return ok(`${DIVISIONS[division].name}, season ${number}. New prospects have joined the market.`,{promoted,division,number});
  }
  function trainingPlan(state,id,trainingId,tier=1){
    const athlete=player(state,id),drill=TRAINING.find(training=>training.id===trainingId);
    if(!athlete||!state.roster.includes(id)||!drill) return null;
    const rank=integer(tier,1,3),completed=athlete.training[trainingId]?.tier||0;
    return {...drill,tier:rank,target:Math.min(drill.attempts-1,drill.target+rank-1),unlocked:rank<=completed+1,xp:25+rank*10,playerId:id,rating:athlete.ratings[drill.attribute]};
  }
  function awardTraining(state,id,trainingId,performance={}){
    const plan=trainingPlan(state,id,trainingId,performance.tier||1),athlete=player(state,id);
    if(!plan||!plan.unlocked) return fail("Complete the previous training tier first.");
    const attempts=integer(performance.attempts,0,100),successes=integer(performance.successes??performance.makes,0,100);
    if(attempts<plan.attempts||successes<plan.target||successes>attempts) return fail(`Complete ${plan.target} successful actions in ${plan.attempts} live attempts.`);
    const creditKey=`${id}:${state.season.completed.length}`,used=state.trainingCredits[creditKey]||0;
    if(used>=3) return fail("Training XP complete for this game week. Play the next campaign fixture to refresh.");
    state.trainingCredits[creditKey]=used+1;
    const previous=athlete.training[trainingId]?.tier||0;
    athlete.training[trainingId]={tier:Math.max(previous,plan.tier),sessions:(athlete.training[trainingId]?.sessions||0)+1};athlete.development.trainingSessions++;
    const reward=addXP(athlete,plan.xp);
    if(plan.tier>previous){athlete.upgradePoints++;reward.upgradePoints++;}
    let signatureMessage="";
    if(plan.tier===3&&previous<3){
      const signature=(TRAINING_SIGNATURES[trainingId]||[]).find(key=>athlete.abilityPotential.includes(key)&&!athlete.abilities[key]);
      if(signature){
        athlete.trainingTokens||={};athlete.trainingTokens[signature]=true;
        const preview=abilityPreview(state,id,signature),learned=preview.available&&learnAbility(state,id,signature).ok;
        reward.signature={key:signature,name:ABILITIES[signature].name,unlocked:!!learned};
        signatureMessage=learned?` Mastery unlock: ${ABILITIES[signature].name} rank 1.`:` ${ABILITIES[signature].name} mastery tuition earned: first rank is free once its rating, prerequisite and branch requirements are met.`;
      }
    }
    touch(state);return ok(`${plan.name} complete: +${reward.xp} XP${plan.tier>previous?" and +1 upgrade point":""}.${signatureMessage}`,reward);
  }
  function chemistry(athletes){
    const players=Array.isArray(athletes)?athletes:athletes?.starters?.map(id=>player(athletes,id)).filter(Boolean)||[],has=key=>players.some(athlete=>athlete.archetype===key||athlete.secondaryArchetype===key);
    const shooters=players.filter(athlete=>athlete.ratings.threePoint>=60||athlete.ratings.midrange>=67).length;
    const synergies=[];
    if(has("playmaker")&&(has("sharpshooter")||has("midrange"))) synergies.push({name:"Read & Release",effect:"Earlier off-ball cuts and cleaner pass reception.",key:"playmakerShooter"});
    if(has("slasher")&&has("stretchBig")) synergies.push({name:"Open Runway",effect:"Stretch spacing opens driving and cutting lanes.",key:"slasherStretch"});
    if((has("interior")||has("rimProtector"))&&has("defender")) synergies.push({name:"Inside-Out Defense",effect:"Perimeter pressure rotates into disciplined rim help.",key:"coverage"});
    if(has("playmaker")&&has("slasher")) synergies.push({name:"Flight Connection",effect:"Passers anticipate athletic cuts earlier.",key:"lobConnection"});
    if(shooters>=2&&has("slasher")) synergies.push({name:"Drive & Kick",effect:"Shooters keep corners open while the driver attacks.",key:"driveKick"});
    const ratings={};for(const category of Object.keys(ATTRIBUTES)) ratings[category]=Math.round(average(players.flatMap(athlete=>ATTRIBUTES[category].map(attribute=>athlete.ratings[attribute.key]))));
    const sorted=Object.entries(ratings).sort((a,b)=>b[1]-a[1]);
    return {score:clamp(48+synergies.length*9+new Set(players.map(athlete=>athlete.archetype)).size*4,35,95),synergies,ratings,strengths:sorted.slice(0,2).map(([key])=>key),weaknesses:sorted.slice(-2).map(([key])=>key),overall:Math.round(average(players.map(athlete=>athlete.overall)))};
  }
  function compare(a,b){return ATTRIBUTE_LIST.map(attribute=>({...attribute,a:a.ratings[attribute.key],b:b.ratings[attribute.key],difference:b.ratings[attribute.key]-a.ratings[attribute.key]}));}
  function toProfile(athlete,team={}){
    const r=athlete.ratings,definition=ARCHETYPES[athlete.archetype]||ARCHETYPES.allRounder;
    const scaled=(value,low=.55,high=1.45)=>low+(value/100)*(high-low);
    const height=clamp(athlete.physical.heightCm/196,.89,1.16),weight=athlete.physical.weightKg;
    return {name:athlete.name,desc:athlete.scoutingReport,bodyType:athlete.position==="C"?"Tall":athlete.position==="PG"?"Guard":"Wing",archetype:definition.runtime,
      campaignArchetype:athlete.archetype,campaignPlayerId:athlete.id,profileKey:athlete.profileKey,ratings:{...r},abilities:{...athlete.abilities},
      physical:{...athlete.physical},equipment:sanitizeEquipment(athlete.equipment),height,width:clamp(.83+(weight-73)/135,.83,1.25),
      speed:scaled(r.speed,.72,1.2),sprint:scaled(r.acceleration,.72,1.28),jump:scaled(r.jumping,.65,1.52),
      layup:scaled(average([r.layups,r.contactFinishing]),.55,1.6),handle:scaled(average([r.dribbling,r.ballSecurity]),.48,1.65),
      shot:scaled(average([r.midrange,r.threePoint,r.releaseStability]),.55,1.75),block:scaled(average([r.blocking,r.interiorDefense]),.45,1.9),
      steal:scaled(average([r.steals,r.perimeterDefense]),.5,1.65),hook:scaled(average([r.closeShot,r.contactFinishing]),.5,1.5),passing:scaled(average([r.passing,r.passingAccuracy,r.courtVision]),.6,1.55),
      strength:scaled(r.strength,.6,1.5),staminaRating:r.stamina/100,role:athlete.role,offensivePriority:athlete.priority,
      jersey:parseInt((team.primaryColor||"#ff6b35").slice(1),16),shorts:parseInt((team.secondaryColor||"#15253f").slice(1),16),
      skin:athlete.appearance.skin,hair:athlete.appearance.hair,hairStyle:athlete.appearance.hairStyle,number:athlete.appearance.number};
  }
  function sanitizeEquipment(value){
    if(!value||typeof value!=="object") return {};
    const result={};
    for(const key of ["jersey","shorts","shoes","socks","headband","sleeve","wristband","accessory","accent"]){
      const raw=value[key];if(!raw||typeof raw!=="object") continue;
      const item={};
      for(const field of ["color","accent","sole"]){
        if(typeof raw[field]==="string"&&/^#[0-9a-f]{6}$/i.test(raw[field])) item[field]=raw[field];
        else if(Number.isInteger(raw[field])&&raw[field]>=0&&raw[field]<=0xffffff) item[field]=raw[field];
      }
      if(typeof raw.enabled==="boolean") item.enabled=raw.enabled;
      if(["left","right","both"].includes(raw.side)) item.side=raw.side;
      if(Object.keys(item).length) result[key]=item;
    }
    return result;
  }
  function normalizePlayer(raw,fallback){
    if(!raw||typeof raw!=="object") return fallback;
    const athlete=clone(fallback);
    athlete.name=text(raw.name,athlete.name);athlete.age=integer(raw.age||athlete.age,18,55);
    for(const key of ATTRIBUTE_KEYS){athlete.caps[key]=integer(raw.caps?.[key]??athlete.caps[key],25,athlete.caps[key]);athlete.ratings[key]=integer(raw.ratings?.[key]??athlete.ratings[key],25,athlete.caps[key]);}
    athlete.appearance.number=String(integer(raw.appearance?.number??athlete.appearance.number,0,99));
    if(["shortFade","braids","buzz","afro","spiky","headband"].includes(raw.appearance?.hairStyle)) athlete.appearance.hairStyle=raw.appearance.hairStyle;
    if(skinTones.includes(Number(raw.appearance?.skin))) athlete.appearance.skin=Number(raw.appearance.skin);
    athlete.equipment=sanitizeEquipment(raw.equipment);
    athlete.xp=integer(raw.xp,0,2000);athlete.level=integer(raw.level||1,1,35);athlete.upgradePoints=integer(raw.upgradePoints,0,250);athlete.invested=integer(raw.invested,0,240);
    athlete.abilities={};for(const key of Object.keys(ABILITIES)) if(athlete.abilityPotential.includes(key)&&raw.abilities?.[key]) athlete.abilities[key]=integer(raw.abilities[key],1,3);
    athlete.branches=[...new Set(Object.keys(athlete.abilities).map(key=>ABILITIES[key].branch))].slice(0,2);
    athlete.role=ROLES.includes(raw.role)?raw.role:"balanced";athlete.priority=integer(raw.priority||1,1,3);
    athlete.stats=emptyStats();for(const key of Object.keys(athlete.stats)) athlete.stats[key]=integer(raw.stats?.[key],0,10000000);
    for(const key of Object.keys(athlete.development)) athlete.development[key]=integer(raw.development?.[key],0,100000);
    athlete.training={};for(const drill of TRAINING) if(raw.training?.[drill.id]) athlete.training[drill.id]={tier:integer(raw.training[drill.id].tier,1,3),sessions:integer(raw.training[drill.id].sessions,0,10000)};
    athlete.trainingTokens={};for(const key of athlete.abilityPotential) if(raw.trainingTokens?.[key]===true&&!athlete.abilities[key]) athlete.trainingTokens[key]=true;
    athlete.captain=!!raw.captain;return refresh(athlete);
  }
  function normalize(raw){
    if(!raw||typeof raw!=="object"||raw.mode!=="rise-to-glory"||!raw.team||!raw.players||!Array.isArray(raw.roster)||!raw.season) return null;
    if(Number(raw.version)>VERSION) return null;
    try{
      const state=create({...raw.team,seed:raw.seed});
      const number=integer(raw.season.number||1,1,999),division=integer(raw.season.division,0,4);
      // Regenerate immutable recruiting information and CPU ratings; only validated progress is restored.
      for(let season=2;season<=Math.min(number,100);season++) for(let index=0;index<9;index++){
        const id=`prospect-${season}-${index}`,data=raw.players[id];if(!data) continue;
        const prospect=generatePlayer(1000+season*9+index,state.seed,45+integer(data.unlockDivision,0,4)*7);
        prospect.id=id;prospect.age=18+index%3;prospect.availableSeason=season;prospect.unlockDivision=integer(data.unlockDivision,0,4);prospect.signingCost=200+prospect.unlockDivision*180+index*28;state.players[id]=prospect;
      }
      for(const id of Object.keys(state.players)) state.players[id]=normalizePlayer(raw.players[id],state.players[id]);
      state.roster=[...new Set(raw.roster.filter(id=>player(state,id)))].slice(0,ROSTER_LIMIT);
      if(state.roster.length<3) return null;
      state.captainId=state.roster.includes(raw.captainId)?raw.captainId:state.roster[0];
      for(const athlete of Object.values(state.players)) athlete.captain=athlete.id===state.captainId;
      state.starters=[...new Set((Array.isArray(raw.starters)?raw.starters:[]).filter(id=>state.roster.includes(id)))].slice(0,3);
      for(const id of state.roster) if(state.starters.length<3&&!state.starters.includes(id)) state.starters.push(id);
      state.currency=integer(raw.currency,0,10000000);state.teamXP=integer(raw.teamXP,0,10000000);state.level=1+Math.floor(state.teamXP/300);
      state.record={wins:integer(raw.record?.wins),losses:integer(raw.record?.losses)};
      state.createdAt=Number(raw.createdAt)||state.createdAt;state.revision=integer(raw.revision,0,10000000);
      state.cosmetics=[...new Set((Array.isArray(raw.cosmetics)?raw.cosmetics:["classic"]).filter(value=>typeof value==="string"&&value.length<40))].slice(0,30);
      state.team.cosmetic=state.cosmetics.includes(raw.team.cosmetic)?raw.team.cosmetic:"classic";
      state.awards=Array.isArray(raw.awards)?raw.awards.filter(value=>value&&typeof value.name==="string").slice(-200).map(value=>({name:text(value.name,"Award",80),season:integer(value.season),division:integer(value.division,0,4),playerId:player(state,value.playerId)?value.playerId:undefined,playerName:text(value.playerName,"",40),reward:integer(value.reward)})):[];
      state.history=Array.isArray(raw.history)?raw.history.filter(value=>value&&typeof value==="object").slice(-20).map(value=>({number:integer(value.number),division:integer(value.division,0,4),champion:!!value.champion,
        standings:Array.isArray(value.standings)?value.standings.filter(row=>row&&typeof row.id==="string").slice(0,8).map(row=>({id:text(row.id,"cpu"),name:text(row.name,"Team"),wins:integer(row.wins,0,7),losses:integer(row.losses,0,7),pointsFor:integer(row.pointsFor),pointsAgainst:integer(row.pointsAgainst)})):[],
        games:Array.isArray(value.games)?value.games.filter(row=>row&&typeof row.id==="string").slice(0,11).map(row=>({id:text(row.id,"game",60),kind:text(row.kind,"League game"),opponentId:text(row.opponentId,"cpu"),opponentName:text(row.opponentName,"Opponent"),won:!!row.won,homeScore:integer(row.homeScore,0,199),awayScore:integer(row.awayScore,0,199),credits:integer(row.credits,0,500),players:sanitizeBoxRows(state,row.players)})):[]})):[];
      const savedAwards=clone(state.awards),savedHistory=clone(state.history),savedCosmetics=state.cosmetics.slice();
      state.team.substitution=["balanced","fresh_legs","matchup","starters"].includes(raw.team.substitution)?raw.team.substitution:"balanced";
      state.team.matchups=raw.team.matchups&&typeof raw.team.matchups==="object"?Object.fromEntries(Object.entries(raw.team.matchups).filter(([id,target])=>state.roster.includes(id)&&typeof target==="string").slice(0,10)):{};
      state.season=createSeason(state,division,number);
      const completed=Array.isArray(raw.season.completed)?raw.season.completed.slice(0,11):[];
      // Replay results to rebuild schedule/standings without trusting malformed or stale derived data.
      for(const result of completed){
        const game=nextGame(state);if(!game||game.id!==result.id) break;
        game.status="complete";game.result={won:!!result.won,homeScore:integer(result.homeScore,0,199),awayScore:integer(result.awayScore,0,199)};
        const boxes=sanitizeBoxRows(state,result.players);
        state.season.completed.push({...game.result,id:game.id,kind:game.kind,opponentId:game.opponentId,opponentName:opponent(state,game).name,credits:integer(result.credits,0,500),players:boxes});
        for(const row of boxes) recordSeasonContribution(state,row.id,row.stats,game.result.won);
        if(game.phase==="regular"){recordStanding(state.season,"player",game.opponentId,game.result.homeScore,game.result.awayScore);simulateRound(state,game);}
        // finishSeason awards are derived from replay, then restore the saved economy below.
        progressSchedule(state,game,game.result.won);
      }
      state.currency=integer(raw.currency,0,10000000);state.awards=savedAwards;state.history=savedHistory;state.cosmetics=savedCosmetics;
      state.rivalries={};
      if(raw.rivalries&&typeof raw.rivalries==="object"){
        for(let index=0;index<OPPONENT_NAMES.length;index++){
          const id=`rival-${index}`,record=raw.rivalries[id];if(record) state.rivalries[id]={wins:integer(record.wins),losses:integer(record.losses)};
        }
      }else{
        const seen=new Set();
        for(const result of [...state.history.flatMap(season=>season.games),...state.season.completed]){
          if(seen.has(result.id)||!/^rival-[0-6]$/.test(result.opponentId)) continue;
          seen.add(result.id);state.rivalries[result.opponentId]||={wins:0,losses:0};state.rivalries[result.opponentId][result.won?"wins":"losses"]++;
        }
      }
      state.lastReward=raw.lastReward&&typeof raw.lastReward==="object"?clone(raw.lastReward):null;
      state.trainingCredits={};for(const [key,value] of Object.entries(raw.trainingCredits||{}).slice(-100)) if(typeof key==="string"&&key.length<80) state.trainingCredits[key]=integer(value,0,3);
      return state;
    }catch(_error){return null;}
  }
  function storageOrDefault(storage){if(storage) return storage;try{return typeof localStorage!=="undefined"?localStorage:null;}catch(_error){return null;}}
  function save(state,storage){
    const target=storageOrDefault(storage);if(!target) return fail("Campaign progress remains in memory; browser storage is unavailable.");
    try{
      state.version=VERSION;state.updatedAt=Date.now();
      const old=target.getItem(SAVE_KEY);if(old){try{if(normalize(JSON.parse(old))) target.setItem(BACKUP_KEY,old);}catch(_error){}}
      target.setItem(SAVE_KEY,JSON.stringify(state));return ok("Campaign saved.");
    }catch(_error){return fail("Campaign progress remains in memory; the browser could not write this save.");}
  }
  function load(storage){
    const target=storageOrDefault(storage);if(!target) return null;
    for(const key of [SAVE_KEY,BACKUP_KEY,"basketballRiseToGloryV2","basketballRiseToGloryV1"]){
      try{const serialized=target.getItem(key);if(!serialized) continue;const state=normalize(JSON.parse(serialized));if(state){if(key!==SAVE_KEY) state.recovered=true;return state;}}catch(_error){}
    }
    return null;
  }
  function saveSummary(state){
    return {teamName:state.team.name,captainName:player(state,state.captainId).name,season:state.season.number,
      division:DIVISIONS[state.season.division].name,wins:state.record.wins,losses:state.record.losses,currency:state.currency,
      completedGames:state.season.completed.length,roster:roster(state).map(athlete=>({name:athlete.name,overall:athlete.overall,level:athlete.level,role:athlete.role}))};
  }
  const recordObject=value=>!!value&&typeof value==="object"&&!Array.isArray(value);
  function validateTransferState(raw){
    if(!recordObject(raw)||raw.mode!=="rise-to-glory")return fail("Choose a Rise to Glory campaign save.");
    if(!Number.isInteger(raw.version)||raw.version<1||raw.version>VERSION)return fail("This campaign save version is not supported. Update the game if the save is newer.");
    if(!recordObject(raw.team)||typeof raw.team.name!=="string"||!raw.team.name.trim()||!recordObject(raw.players)||
      !recordObject(raw.season)||!Number.isInteger(raw.seed)||raw.seed<1||!Number.isInteger(raw.season.number)||raw.season.number<1||raw.season.number>999||
      !Number.isInteger(raw.season.division)||raw.season.division<0||raw.season.division>=DIVISIONS.length||
      !Array.isArray(raw.roster)||raw.roster.length<3||raw.roster.length>ROSTER_LIMIT||new Set(raw.roster).size!==raw.roster.length||
      !Array.isArray(raw.starters)||raw.starters.length!==3||new Set(raw.starters).size!==3||!raw.roster.includes(raw.captainId)||
      raw.starters.some(id=>!raw.roster.includes(id))||Object.keys(raw.players).length>1200)return fail("This file is missing valid team, roster or season information.");
    for(const id of raw.roster){
      const athlete=own(raw.players,id)?raw.players[id]:null;
      if(typeof id!=="string"||!/^(rtg-\d+-\d+|prospect-\d+-\d+)$/.test(id)||!recordObject(athlete)||
        !recordObject(athlete.ratings)||!recordObject(athlete.caps)||ATTRIBUTE_KEYS.some(key=>!Number.isFinite(athlete.ratings[key])||!Number.isFinite(athlete.caps[key])))
        return fail("This file contains an incomplete player in its roster.");
    }
    const normalized=normalize(raw);
    if(!normalized||normalized.roster.length!==raw.roster.length||raw.roster.some(id=>!normalized.roster.includes(id)))return fail("This campaign could not be restored safely.");
    return ok("Campaign checked. Review the team before importing.",{state:normalized,summary:saveSummary(normalized)});
  }
  function inspectSave(serialized){
    if(typeof serialized!=="string")return fail("Choose a JSON campaign save file.");
    if(serialized.length>MAX_SAVE_BYTES||new TextEncoder().encode(serialized).length>MAX_SAVE_BYTES)return fail("This save file is too large. The limit is 8 MB.");
    try{
      const parsed=JSON.parse(serialized,(key,value)=>{if(["__proto__","constructor","prototype"].includes(key))throw new Error("Unsafe key");return value;});
      if(recordObject(parsed)&&own(parsed,"format")){
        if(parsed.format!==SAVE_FORMAT||parsed.transferVersion!==TRANSFER_VERSION||parsed.mode!=="rise-to-glory")return fail("This save file format or version is not supported.");
        return validateTransferState(parsed.save);
      }
      return validateTransferState(parsed);
    }catch(_error){return fail("This file is not valid campaign JSON. Your current save has not changed.");}
  }
  function exportSave(state){
    const validated=validateTransferState(state);if(!validated.ok)return validated;
    const exportedAt=new Date().toISOString(),safeName=validated.state.team.name.toLowerCase().replace(/[^a-z0-9]+/g,"-").replace(/^-|-$/g,"")||"franchise";
    const serialized=JSON.stringify({format:SAVE_FORMAT,transferVersion:TRANSFER_VERSION,mode:"rise-to-glory",exportedAt,save:validated.state},null,2);
    if(new TextEncoder().encode(serialized).length>MAX_SAVE_BYTES)return fail("This campaign is too large to export as a supported save file.");
    return ok("Campaign export ready.",{serialized,filename:`basketball2-${safeName}-season-${validated.state.season.number}.json`,summary:validated.summary});
  }
  function importSave(serialized,storage){
    const checked=inspectSave(serialized);if(!checked.ok)return checked;
    const target=storageOrDefault(storage);if(!target)return fail("Browser storage is unavailable. Your current team has not changed.");
    let previous,backup,backupWritten=false,saveWritten=false;
    try{
      previous=target.getItem(SAVE_KEY);backup=target.getItem(BACKUP_KEY);
      // Keep the last campaign before replacement; do not replace a good backup with corrupt data.
      const oldValid=previous&&(()=>{try{return !!normalize(JSON.parse(previous));}catch(_error){return false;}})();
      if(oldValid){target.setItem(BACKUP_KEY,previous);backupWritten=true;}
      checked.state.updatedAt=Date.now();target.setItem(SAVE_KEY,JSON.stringify(checked.state));saveWritten=true;
      return ok(oldValid?"Campaign imported. Your previous campaign is kept as a backup.":"Campaign imported. Your franchise is ready to continue.",{state:checked.state,summary:checked.summary,backedUp:!!oldValid});
    }catch(_error){
      // localStorage has no transaction API: undo any completed write before reporting failure.
      const restore=(key,value)=>{if(value===null)target.removeItem(key);else target.setItem(key,value);};
      try{if(saveWritten)restore(SAVE_KEY,previous);if(backupWritten)restore(BACKUP_KEY,backup);}catch(_rollbackError){}
      return fail("The browser could not store this import. Your current team has not changed. Export it before trying again.");
    }
  }
  return {VERSION,SAVE_KEY,BACKUP_KEY,ROSTER_LIMIT,SAVE_FORMAT,TRANSFER_VERSION,MAX_SAVE_BYTES,ATTRIBUTES,ATTRIBUTE_LIST,ATTRIBUTE_KEYS,ARCHETYPES,ABILITIES,DIVISIONS,TRAINING,ROLES,OFFENSES,DEFENSES,LOGOS,JERSEYS,
    create,generateMarket,rookiePool,player,roster,market,sign,release,setStarters,setRole,setStrategy,upgradePreview,upgrade,abilityPreview,learnAbility,xpRequired,addXP,contributionXP,
    getStandings,nextGame,opponent,prepareMatch,recordGame,advanceSeason,trainingPlan,awardTraining,chemistry,compare,toProfile,overall,tier,normalize,save,load,saveSummary,exportSave,inspectSave,importSave};
});
