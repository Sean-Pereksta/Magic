const NATIONAL_TOUR_STOPS = [
  {
    id:"seattle",city:"Seattle",region:"WA",x:87,y:78,courtKey:"seattleRooftop",difficultyKey:"rookie",
    profileKey:"flash",sneakerKey:"lightning",jerseyKey:"lightningStrike",
    name:'DEVIN "JET" COLE',archetype:"Lightning Guard",height:"6′1″ Speed Guard",
    strengths:["First step","Open-court speed","Recovery"],weakness:"Can be forced into traffic",
    style:"Changes pace constantly and attacks a defender's top foot.",reward:{type:"Sneakers",key:"lightning",label:"Lightning Lows"}
  },
  {
    id:"portland",city:"Portland",region:"OR",x:92,y:137,courtKey:"portlandWarehouse",difficultyKey:"easy",
    profileKey:"crafty",sneakerKey:"streetLow",jerseyKey:"streetCamo",
    name:'MILO "SLEIGHT" REYES',archetype:"Street Magician",height:"6′3″ Playmaker",
    strengths:["Crossovers","Hesitations","Ball protection"],weakness:"Average rim defense",
    style:"Chains low crossovers and punishes impatient reaches.",reward:{type:"Jersey",key:"streetCamo",label:"Street Camo"}
  },
  {
    id:"san-francisco",city:"San Francisco",region:"CA",x:80,y:245,courtKey:"bayBlacktop",difficultyKey:"easy",
    profileKey:"claw",sneakerKey:"lockdown",jerseyKey:"chromeSlash",
    name:'ANDRE "THE GATE" WARD',archetype:"Lockdown",height:"6′6″ Two-Way Wing",
    strengths:["Containment","Reach timing","Physical cuts"],weakness:"Inconsistent deep jumper",
    style:"Beats drives to the spot and turns loose handles into runouts.",reward:{type:"Court",key:"bayBlacktop",label:"Bay Blacktop"}
  },
  {
    id:"los-angeles",city:"Los Angeles",region:"CA",x:144,y:342,courtKey:"laSunset",difficultyKey:"normal",
    profileKey:"deeprange",sneakerKey:"solarOrange",jerseyKey:"solarFlare",
    name:'TREY "DAYLIGHT" MONROE',archetype:"Sniper",height:"6′4″ Shot Creator",
    strengths:["Deep range","Quick release","Step-backs"],weakness:"Light interior presence",
    style:"Uses one hard dribble to manufacture daylight from anywhere.",reward:{type:"Jersey",key:"solarFlare",label:"Solar Flare"}
  },
  {
    id:"phoenix",city:"Phoenix",region:"AZ",x:248,y:366,courtKey:"phoenixDesert",difficultyKey:"normal",
    profileKey:"bruiser",sneakerKey:"lavaBurst",jerseyKey:"inferno",
    name:'MARCUS "THE WALL" KING',archetype:"Paint Beast",height:"6′8″ Paint Defender",
    strengths:["Interior defense","Blocks","Contact"],weakness:"Slow lateral recovery",
    style:"Protects the paint and forces disciplined pull-up jumpers.",reward:{type:"Sneakers",key:"lavaBurst",label:"Lava Bursts"}
  },
  {
    id:"denver",city:"Denver",region:"CO",x:405,y:260,courtKey:"denverSummit",difficultyKey:"hard",
    profileKey:"rimreaper",sneakerKey:"riser",jerseyKey:"skylineSlash",
    name:'JALEN "ALTITUDE" PRICE',archetype:"High Flyer",height:"6′7″ Explosive Wing",
    strengths:["Vertical pop","Contact finishes","Chase-downs"],weakness:"Loose gather in crowds",
    style:"Turns one clean runway into a finish above the square.",reward:{type:"Sneakers",key:"riser",label:"Sky Risers"}
  },
  {
    id:"dallas",city:"Dallas",region:"TX",x:493,y:405,courtKey:"dallasFieldhouse",difficultyKey:"hard",
    profileKey:"claw",sneakerKey:"tigerClaw",jerseyKey:"tigerFire",
    name:'NICO "QUICK HANDS" BELL',archetype:"Pickpocket",height:"6′2″ Pressure Guard",
    strengths:["Ball-side steals","Recovery","Transition"],weakness:"Reach attempts expose his hips",
    style:"Shows space, then attacks the first high dribble he sees.",reward:{type:"Jersey",key:"tigerFire",label:"Tiger Fire"}
  },
  {
    id:"chicago",city:"Chicago",region:"IL",x:650,y:210,courtKey:"chicagoHardwood",difficultyKey:"elite",
    profileKey:"tower",sneakerKey:"tower",jerseyKey:"classicBlue",
    name:'ISAIAH "SKYLINE" GRANT',archetype:"Tower",height:"7′0″ Rim Anchor",
    strengths:["Standing blocks","Rebounds","Hook shots"],weakness:"Slow change of direction",
    style:"Owns vertical space but can be pulled away from the rim.",reward:{type:"Court",key:"chicagoHardwood",label:"Chicago Hardwood"}
  },
  {
    id:"atlanta",city:"Atlanta",region:"GA",x:752,y:346,courtKey:"atlantaNight",difficultyKey:"elite",
    profileKey:"smooth",sneakerKey:"roseBlaze",jerseyKey:"roseHeat",
    name:'CAM "VELVET" HARRIS',archetype:"Midrange Assassin",height:"6′5″ Scoring Wing",
    strengths:["Pull-ups","Fadeaways","Footwork"],weakness:"Gives up straight-line speed",
    style:"Uses shoulder feints and balance to live at the elbows.",reward:{type:"Jersey",key:"roseHeat",label:"Rose Heat"}
  },
  {
    id:"miami",city:"Miami",region:"FL",x:849,y:464,courtKey:"miamiNeon",difficultyKey:"nightmare",
    profileKey:"rocket",sneakerKey:"neonGlow",jerseyKey:"neonNight",
    name:'DARIUS "SKYSHOW" VALE',archetype:"High Flyer",height:"6′6″ Showman",
    strengths:["Dunks","Side hops","Momentum"],weakness:"Can over-attack the rim",
    style:"Feeds off highlights and becomes dangerous after consecutive makes.",reward:{type:"Sneakers",key:"neonGlow",label:"Neon Glow"}
  },
  {
    id:"new-york",city:"New York",region:"NY",x:875,y:185,courtKey:"newYorkCage",difficultyKey:"nightmare",
    profileKey:"crafty",sneakerKey:"cityDash",jerseyKey:"cityRush",
    name:'MALIK "THE MAYOR" STONE',archetype:"Street Magician",height:"6′4″ Complete Guard",
    strengths:["Counters","Handle","Late-clock scoring"],weakness:"Can be bodied off his line",
    style:"Reads your first answer and builds the next possession around it.",reward:{type:"Court",key:"newYorkCage",label:"New York Cage"}
  },
  {
    id:"boston",city:"Boston",region:"MA",x:923,y:156,courtKey:"bostonLegacy",difficultyKey:"legend",
    profileKey:"claw",sneakerKey:"iceFangHighs",jerseyKey:"royalLegacy",
    name:'ELIJAH "OLD SCHOOL" CROSS',archetype:"Complete Stopper",height:"6′7″ Veteran Wing",
    strengths:["Positioning","Contested finishes","Composure"],weakness:"Prefers a deliberate pace",
    style:"Never wastes a step and forces you to win possessions twice.",reward:{type:"Jersey",key:"royalLegacy",label:"Royal Legacy"}
  },
  {
    id:"las-vegas",city:"Las Vegas",region:"NV",x:222,y:296,courtKey:"vegasFinals",difficultyKey:"impossible",
    profileKey:"rimreaper",sneakerKey:"thunderBeasts",jerseyKey:"chromeElite",
    name:'JORDAN "THE CROWN" VAUGHN',archetype:"National Champion",height:"6′8″ Complete Superstar",
    strengths:["Advanced counters","Three-level scoring","Elite defense"],weakness:"No safe weakness—make him react",
    style:"An all-around champion who adapts to your tendencies as the game develops.",reward:{type:"Title",key:"nationalChampion",label:"National 1v1 Champion"}
  }
];

function createNationalTourEnemies(){
  return NATIONAL_TOUR_STOPS.map((stop,index)=>({
    id:`tour-${stop.id}`,
    stopId:stop.id,
    stopIndex:index,
    name:stop.name,
    profileKey:stop.profileKey,
    sneakerKey:stop.sneakerKey,
    jerseyKey:stop.jerseyKey,
    courtKey:stop.courtKey,
    difficultyKey:stop.difficultyKey,
    archetype:stop.archetype,
    wins:0,
    losses:0
  }));
}

function createNationalTourState(){
  const enemies = createNationalTourEnemies();
  return {
    version:2,
    mode:"national-tour",
    active:true,
    playerProfileKey:selectedP1,
    playerSneakerKey:selectedP1Sneaker,
    playerJerseyKey:selectedP1Jersey,
    round:1,
    currentStopIndex:0,
    maxRounds:NATIONAL_TOUR_STOPS.length,
    playerWins:0,
    playerLosses:0,
    xp:0,
    level:1,
    winStreak:0,
    unlocked:["classicBlue","classic"],
    enemies,
    standings:[],
    schedule:enemies.map((enemy,index)=>({round:index+1,enemyId:enemy.id,courtKey:enemy.courtKey,city:NATIONAL_TOUR_STOPS[index].city})),
    completedGames:[],
    nextOpponentId:enemies[0].id,
    nextCourtKey:enemies[0].courtKey,
    finalsQualified:false,
    finalsComplete:false,
    champion:null,
    travelFrom:null,
    travelTo:0,
    lastReward:null
  };
}

function normalizeNationalTourState(state){
  if(!state || typeof state !== "object") return null;
  if(state.version === 2 && state.mode === "national-tour") return state;
  const migrated = createNationalTourState();
  migrated.playerProfileKey = state.playerProfileKey || selectedP1;
  migrated.playerSneakerKey = state.playerSneakerKey || selectedP1Sneaker;
  migrated.playerJerseyKey = state.playerJerseyKey || selectedP1Jersey;
  migrated.playerWins = Number(state.playerWins || 0);
  migrated.playerLosses = Number(state.playerLosses || 0);
  migrated.currentStopIndex = Math.min(NATIONAL_TOUR_STOPS.length-1,Math.max(0,Number(state.round || 1)-1));
  migrated.round = migrated.currentStopIndex+1;
  migrated.xp = migrated.playerWins*120;
  migrated.level = 1+Math.floor(migrated.xp/250);
  migrated.nextOpponentId = migrated.enemies[migrated.currentStopIndex].id;
  migrated.nextCourtKey = migrated.enemies[migrated.currentStopIndex].courtKey;
  return migrated;
}

function getCurrentTourStop(){
  if(!tournamentState) return NATIONAL_TOUR_STOPS[0];
  return NATIONAL_TOUR_STOPS[Math.min(NATIONAL_TOUR_STOPS.length-1,Math.max(0,tournamentState.currentStopIndex ?? tournamentState.round-1))];
}

function calculateTourXP(playerWon){
  if(!p1) return playerWon ? 120 : 35;
  const highlights = p1.stats.blocks*12 + p1.stats.steals*10 + p1.stats.perfect*8 + p1.stats.ankle*10;
  const efficiency = p1.stats.fga ? Math.round((p1.stats.fgm/p1.stats.fga)*30) : 0;
  return Math.max(20,(playerWon?100:28)+highlights+efficiency);
}

function awardTourResult(playerWon){
  const state = tournamentState;
  const stopIndex = state.currentStopIndex ?? state.round-1;
  const stop = NATIONAL_TOUR_STOPS[stopIndex];
  const earnedXP = calculateTourXP(playerWon);
  state.xp = Number(state.xp || 0)+earnedXP;
  state.level = 1+Math.floor(state.xp/250);
  state.winStreak = playerWon ? Number(state.winStreak || 0)+1 : 0;
  state.lastReward = null;
  if(playerWon && stop?.reward){
    state.unlocked = Array.isArray(state.unlocked) ? state.unlocked : [];
    if(!state.unlocked.includes(stop.reward.key)) state.unlocked.push(stop.reward.key);
    state.lastReward = {...stop.reward,earnedXP};
  }else{
    state.lastReward = {type:"Tour XP",key:"xp",label:`+${earnedXP} XP`,earnedXP};
  }
  return earnedXP;
}

function escapeTourText(value){
  return String(value ?? "").replace(/[&<>"']/g,char=>({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}[char]));
}

function tourRouteMarkup(state){
  const current = Math.min(NATIONAL_TOUR_STOPS.length-1,Math.max(0,state.currentStopIndex ?? state.round-1));
  const points = NATIONAL_TOUR_STOPS.map(stop=>`${stop.x},${stop.y}`).join(" ");
  const completedPoints = NATIONAL_TOUR_STOPS.slice(0,current+1).map(stop=>`${stop.x},${stop.y}`).join(" ");
  const stopNodes = NATIONAL_TOUR_STOPS.map((stop,index)=>{
    const status = index < current ? "complete" : index === current ? "current" : "upcoming";
    return `<g class="tour-stop ${status}" transform="translate(${stop.x} ${stop.y})" aria-label="${escapeTourText(stop.city)} ${status}">
      <circle r="${index===current?12:8}"></circle>
      <text y="-17" text-anchor="middle">${escapeTourText(stop.city)}</text>
    </g>`;
  }).join("");
  const marker = NATIONAL_TOUR_STOPS[current];
  const previousIndex = Number.isInteger(state.travelFrom) ? Math.min(current,Math.max(0,state.travelFrom)) : current;
  const previousMarker = NATIONAL_TOUR_STOPS[previousIndex];
  const travelAnimation = previousIndex!==current
    ? `<animateTransform attributeName="transform" type="translate" from="${previousMarker.x} ${previousMarker.y}" to="${marker.x} ${marker.y}" dur=".9s" fill="freeze" calcMode="spline" keySplines=".2 .8 .2 1"></animateTransform>`
    : "";
  return `<svg class="tour-map-svg" viewBox="0 0 1000 560" role="img" aria-label="National 1v1 Tour map">
    <path class="tour-country" d="M55 88 L145 65 L245 86 L355 78 L454 101 L570 89 L675 111 L744 93 L815 121 L929 112 L958 175 L911 225 L888 318 L850 410 L790 447 L713 424 L628 451 L523 433 L439 455 L351 422 L272 439 L211 402 L158 363 L109 292 L76 215 Z"></path>
    <polyline class="tour-route route-future" points="${points}"></polyline>
    <polyline class="tour-route route-complete" points="${completedPoints}"></polyline>
    ${stopNodes}
    <g class="tour-player-marker" transform="translate(${marker.x} ${marker.y})">${travelAnimation}<circle r="18"></circle><text y="5" text-anchor="middle">★</text></g>
  </svg>`;
}

function tourScoutMarkup(stop){
  return `<article class="scout-card">
    <div class="scout-kicker">${escapeTourText(stop.city)} · ${escapeTourText(stop.archetype)}</div>
    <h2>${escapeTourText(stop.name)}</h2>
    <div class="scout-height">${escapeTourText(stop.height)}</div>
    <div class="scout-grid">
      <div><h3>Strengths</h3><ul>${stop.strengths.map(item=>`<li>${escapeTourText(item)}</li>`).join("")}</ul></div>
      <div><h3>Weakness</h3><p>${escapeTourText(stop.weakness)}</p></div>
    </div>
    <p class="scout-style"><strong>Play style:</strong> ${escapeTourText(stop.style)}</p>
  </article>`;
}

function renderNationalTour(container,state=tournamentState){
  if(!container || !state) return;
  const stop = NATIONAL_TOUR_STOPS[Math.min(NATIONAL_TOUR_STOPS.length-1,Math.max(0,state.currentStopIndex ?? state.round-1))];
  const reward = state.lastReward ? `<div class="tour-reward"><span>Unlocked</span><strong>${escapeTourText(state.lastReward.label)}</strong></div>` : "";
  container.innerHTML = `<div class="tour-layout">
    <section class="tour-map-panel">
      <div class="tour-meta"><span>${state.playerWins}-${state.playerLosses} RECORD</span><span>LEVEL ${state.level || 1}</span><span>${state.xp || 0} TOUR XP</span></div>
      ${tourRouteMarkup(state)}
      <div class="tour-progress"><span style="width:${Math.min(100,((state.currentStopIndex||0)/(NATIONAL_TOUR_STOPS.length-1))*100)}%"></span></div>
    </section>
    <section class="tour-scout-panel">${tourScoutMarkup(stop)}${reward}</section>
  </div>`;
}
