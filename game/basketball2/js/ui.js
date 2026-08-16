function buildMenu(){
  buildPrimaryMenu();
  buildCharacterCards("p1Cards","p1");
  buildCharacterCards("p2Cards","p2");
  buildCharacterCards("tourP1Cards","tournamentP1");
  setupSneakerSelectors();
  setupJerseySelectors();
  setupCourtSelector();
  setupTournamentSelectors();

  document.getElementById("onePlayerBtn").onclick = () => {
    twoPlayerMode = false;
    document.getElementById("onePlayerBtn").classList.add("selected");
    document.getElementById("twoPlayerBtn").classList.remove("selected");
    document.getElementById("modeText").textContent = "1P vs Bot";
    document.getElementById("p2Title").textContent = "Bot Character";
    updateInputDeviceUI();
  };

  document.getElementById("twoPlayerBtn").onclick = () => {
    twoPlayerMode = true;
    document.getElementById("twoPlayerBtn").classList.add("selected");
    document.getElementById("onePlayerBtn").classList.remove("selected");
    document.getElementById("modeText").textContent = "2P Split Screen";
    document.getElementById("p2Title").textContent = "Player 2 Character";
    updateInputDeviceUI();
  };

  const diffBox = document.getElementById("difficultyButtons");
  Object.keys(difficultySettings).forEach(d => {
    const btn = document.createElement("button");
    btn.className = "difficultyBtn" + (d === selectedDifficulty ? " selected" : "");
    btn.textContent = d.toUpperCase();
    btn.onclick = () => {
      selectedDifficulty = d;
      document.querySelectorAll(".difficultyBtn").forEach(b => b.classList.remove("selected"));
      btn.classList.add("selected");
    };
    diffBox.appendChild(btn);
  });

  buildOptionButtons("gameModeButtons",[
    {label:"First to 7",onClick:()=>targetScore=7,selected:()=>targetScore===7},
    {label:"First to 11",onClick:()=>targetScore=11,selected:()=>targetScore===11},
    {label:"First to 21",onClick:()=>targetScore=21,selected:()=>targetScore===21}
  ]);
  buildOptionButtons("ruleButtons",[
    {label:"Win by 2",onClick:()=>{gameMode.winBy2=!gameMode.winBy2;winBy=gameMode.winBy2?2:1;},selected:()=>gameMode.winBy2},
    {label:"Make it take it",onClick:()=>gameMode.makeItTakeIt=!gameMode.makeItTakeIt,selected:()=>gameMode.makeItTakeIt},
    {label:"Loser's ball",onClick:()=>gameMode.losersBall=!gameMode.losersBall,selected:()=>gameMode.losersBall},
    {label:"Simulation",onClick:()=>gameSettings.set("gameplayPreset","simulation"),selected:()=>gameSettings.get("gameplayPreset")==="simulation"},
    {label:"Arcade",onClick:()=>gameSettings.set("gameplayPreset","arcade"),selected:()=>gameSettings.get("gameplayPreset")==="arcade"}
  ]);
  buildOptionButtons("qualityButtons",[
    {label:"Auto",onClick:()=>{gameSettings.set("quality","auto");adaptiveQuality.applyRequestedLevel();},selected:()=>gameSettings.get("quality")==="auto"},
    {label:"Low",onClick:()=>{gameSettings.set("quality","low");adaptiveQuality.applyRequestedLevel();},selected:()=>gameSettings.get("quality")==="low"},
    {label:"Medium",onClick:()=>{gameSettings.set("quality","medium");adaptiveQuality.applyRequestedLevel();},selected:()=>gameSettings.get("quality")==="medium"},
    {label:"High",onClick:()=>{gameSettings.set("quality","high");adaptiveQuality.applyRequestedLevel();},selected:()=>gameSettings.get("quality")==="high"},
    {label:"Ultra",onClick:()=>{gameSettings.set("quality","ultra");adaptiveQuality.applyRequestedLevel();},selected:()=>gameSettings.get("quality")==="ultra"}
  ]);

  document.getElementById("startBtn").onclick = ()=>{ tutorialMode=false; startGame(); };
  document.getElementById("hideUiBtn").onclick = toggleUI;
  document.getElementById("rematchBtn").onclick = () => rematchSamePlayers();
  document.getElementById("changeCharsBtn").onclick = () => returnToCharacterMenu();
  showPrimaryMenu();
}

function buildPrimaryMenu(){
  document.getElementById("primaryQuickPlayBtn").onclick = ()=>showQuickPlaySetup();
  document.getElementById("primaryTournamentBtn").onclick = ()=>showTournamentSetup();
  document.getElementById("primaryLoadTournamentBtn").onclick = ()=>loadTournament();
  document.getElementById("quickPlayBackBtn").onclick = ()=>showPrimaryMenu();
  document.getElementById("tournamentSetupBackBtn").onclick = ()=>showPrimaryMenu();
  document.getElementById("startTournamentBtn").onclick = ()=>startNewTournament();
  document.getElementById("saveTournamentBtn").onclick = ()=>saveTournament();
  document.getElementById("continueTournamentBtn").onclick = ()=>continueTournamentFlow();
  document.getElementById("betweenMainMenuBtn").onclick = ()=>showPrimaryMenu();
}

function showMenuScreen(id){
  document.querySelectorAll("#menuBox .menuScreen").forEach(el=>el.classList.remove("active"));
  const target = document.getElementById(id);
  if(target) target.classList.add("active");
  document.getElementById("menu").style.display = "flex";
}
function showPrimaryMenu(message=""){
  gameStarted = false;
  gamePaused = false;
  tutorialMode = false;
  tutorialCoach?.stop();
  document.body.classList.remove("tutorial-active","touch-controls-active");
  isTournamentGame = false;
  isChampionshipGame = false;
  showMenuScreen("primaryMenuScreen");
  document.getElementById("primaryMenuMsg").textContent = message;
}
function showQuickPlaySetup(){
  showMenuScreen("quickPlayScreen");
}
function showTournamentSetup(message=""){
  showMenuScreen("tournamentSetupScreen");
  document.getElementById("tournamentSetupMsg").textContent = message;
  refreshTournamentSelectorUI();
}

function setupSneakerSelectors(){
  const rotate = (which,dir)=>{
    if(which === "p1"){
      const i = sneakerKeys.indexOf(selectedP1Sneaker);
      selectedP1Sneaker = sneakerKeys[(i + dir + sneakerKeys.length) % sneakerKeys.length];
    }else{
      const i = sneakerKeys.indexOf(selectedP2Sneaker);
      selectedP2Sneaker = sneakerKeys[(i + dir + sneakerKeys.length) % sneakerKeys.length];
    }
    refreshSneakerSelectorUI();
  };
  document.getElementById("p1SneakerPrev").onclick = ()=>rotate("p1",-1);
  document.getElementById("p1SneakerNext").onclick = ()=>rotate("p1",1);
  document.getElementById("p2SneakerPrev").onclick = ()=>rotate("p2",-1);
  document.getElementById("p2SneakerNext").onclick = ()=>rotate("p2",1);
  refreshSneakerSelectorUI();
}

function refreshSneakerSelectorUI(){
  document.getElementById("p1SneakerName").textContent = sneakers[selectedP1Sneaker].name;
  document.getElementById("p2SneakerName").textContent = sneakers[selectedP2Sneaker].name;
  document.getElementById("p1SneakerStyle").textContent = sneakers[selectedP1Sneaker].style || "Style";
  document.getElementById("p2SneakerStyle").textContent = sneakers[selectedP2Sneaker].style || "Style";
  drawSneakerPreview(document.getElementById("p1SneakerPreview"),sneakers[selectedP1Sneaker],performance.now()*.001);
  drawSneakerPreview(document.getElementById("p2SneakerPreview"),sneakers[selectedP2Sneaker],performance.now()*.001 + .8);
  refreshCharacterCardPreviews();
}

function setupJerseySelectors(){
  const rotate = (which,dir)=>{
    if(which === "p1"){
      const i = jerseyKeys.indexOf(selectedP1Jersey);
      selectedP1Jersey = jerseyKeys[(i + dir + jerseyKeys.length) % jerseyKeys.length];
    }else{
      const i = jerseyKeys.indexOf(selectedP2Jersey);
      selectedP2Jersey = jerseyKeys[(i + dir + jerseyKeys.length) % jerseyKeys.length];
    }
    refreshJerseySelectorUI();
  };
  document.getElementById("p1JerseyPrev").onclick = ()=>rotate("p1",-1);
  document.getElementById("p1JerseyNext").onclick = ()=>rotate("p1",1);
  document.getElementById("p2JerseyPrev").onclick = ()=>rotate("p2",-1);
  document.getElementById("p2JerseyNext").onclick = ()=>rotate("p2",1);
  refreshJerseySelectorUI();
}
function refreshJerseySelectorUI(){
  document.getElementById("p1JerseyName").textContent = jerseys[selectedP1Jersey].name;
  document.getElementById("p2JerseyName").textContent = jerseys[selectedP2Jersey].name;
  document.getElementById("p1JerseyStyle").textContent = jerseys[selectedP1Jersey].style || "Style";
  document.getElementById("p2JerseyStyle").textContent = jerseys[selectedP2Jersey].style || "Style";
  drawJerseyPreview(document.getElementById("p1JerseyPreview"),jerseys[selectedP1Jersey]);
  drawJerseyPreview(document.getElementById("p2JerseyPreview"),jerseys[selectedP2Jersey]);
  refreshCharacterCardPreviews();
}

function setupTournamentSelectors(){
  const rotateSneaker = (dir)=>{
    const i = sneakerKeys.indexOf(selectedP1Sneaker);
    selectedP1Sneaker = sneakerKeys[(i + dir + sneakerKeys.length) % sneakerKeys.length];
    refreshTournamentSelectorUI();
  };
  const rotateJersey = (dir)=>{
    const i = jerseyKeys.indexOf(selectedP1Jersey);
    selectedP1Jersey = jerseyKeys[(i + dir + jerseyKeys.length) % jerseyKeys.length];
    refreshTournamentSelectorUI();
  };
  document.getElementById("tourP1SneakerPrev").onclick = ()=>rotateSneaker(-1);
  document.getElementById("tourP1SneakerNext").onclick = ()=>rotateSneaker(1);
  document.getElementById("tourP1JerseyPrev").onclick = ()=>rotateJersey(-1);
  document.getElementById("tourP1JerseyNext").onclick = ()=>rotateJersey(1);
  refreshTournamentSelectorUI();
}
function refreshTournamentSelectorUI(){
  document.getElementById("tourP1SneakerName").textContent = sneakers[selectedP1Sneaker].name;
  document.getElementById("tourP1SneakerStyle").textContent = sneakers[selectedP1Sneaker].style || "Style";
  drawSneakerPreview(document.getElementById("tourP1SneakerPreview"),sneakers[selectedP1Sneaker],performance.now()*.001 + .3);
  document.getElementById("tourP1JerseyName").textContent = jerseys[selectedP1Jersey].name;
  document.getElementById("tourP1JerseyStyle").textContent = jerseys[selectedP1Jersey].style || "Style";
  drawJerseyPreview(document.getElementById("tourP1JerseyPreview"),jerseys[selectedP1Jersey]);
  document.querySelectorAll("#tourP1Cards .card").forEach((card)=>{
    const key = card.querySelector(".cardPreview")?.dataset.profile;
    if(key && profiles[key]) drawProfileCardPreview(card.querySelector(".cardPreview"),profiles[key],selectedP1Jersey,selectedP1Sneaker);
    card.classList.toggle("selected",key === selectedP1);
  });
}

function setupCourtSelector(){
  const rotate=(dir)=>{
    const i = courtKeys.indexOf(selectedCourt);
    selectedCourt = courtKeys[(i + dir + courtKeys.length) % courtKeys.length];
    refreshCourtSelectorUI();
  };
  document.getElementById("courtPrev").onclick = ()=>rotate(-1);
  document.getElementById("courtNext").onclick = ()=>rotate(1);
  refreshCourtSelectorUI();
}
function refreshCharacterCardPreviews(){
  document.querySelectorAll("#p1Cards .card").forEach((card)=>{
    const key = card.querySelector(".cardPreview")?.dataset.profile;
    if(key && profiles[key]) drawProfileCardPreview(card.querySelector(".cardPreview"),profiles[key],selectedP1Jersey,selectedP1Sneaker);
  });
  document.querySelectorAll("#p2Cards .card").forEach((card)=>{
    const key = card.querySelector(".cardPreview")?.dataset.profile;
    if(key && profiles[key]) drawProfileCardPreview(card.querySelector(".cardPreview"),profiles[key],selectedP2Jersey,selectedP2Sneaker);
  });
}

function refreshCourtSelectorUI(){
  document.getElementById("courtName").textContent = courts[selectedCourt].name;
  document.getElementById("courtStyle").textContent = courts[selectedCourt].style || (courts[selectedCourt].theme || "Theme");
  drawCourtPreview(document.getElementById("courtPreview"),courts[selectedCourt]);
}

function createTournamentEnemies(){
  return createNationalTourEnemies();
  /* Legacy opponent ladder retained below only for save migration reference. */
  return [
    {id:"e1",name:"Rookie Park Guard",profileKey:"sky",sneakerKey:"classic",jerseyKey:"classicBlue",courtKey:"classicArena",difficultyKey:"rookie",wins:0,losses:0},
    {id:"e2",name:"Street Slasher",profileKey:"rocket",sneakerKey:"street",jerseyKey:"cityRush",courtKey:"neonStreet",difficultyKey:"easy",wins:0,losses:0},
    {id:"e3",name:"Neon Shooter",profileKey:"smooth",sneakerKey:"midnightPulse",jerseyKey:"neonNight",courtKey:"beachCourt",difficultyKey:"easy",wins:0,losses:0},
    {id:"e4",name:"Paint Bully",profileKey:"bruiser",sneakerKey:"beastClaws",jerseyKey:"beastMode",courtKey:"royalCourt",difficultyKey:"normal",wins:0,losses:0},
    {id:"e5",name:"Coastline Wing",profileKey:"flash",sneakerKey:"lightning",jerseyKey:"skyline",courtKey:"blacktop",difficultyKey:"normal",wins:0,losses:0},
    {id:"e6",name:"Royal Defender",profileKey:"claw",sneakerKey:"royalRush",jerseyKey:"royalGold",courtKey:"galaxyCourt",difficultyKey:"hard",wins:0,losses:0},
    {id:"e7",name:"Blacktop Claw",profileKey:"claw",sneakerKey:"tigerClaw",jerseyKey:"darkClaw",courtKey:"lavaRun",difficultyKey:"hard",wins:0,losses:0},
    {id:"e8",name:"Galaxy Shotmaker",profileKey:"smooth",sneakerKey:"galaxyFoams",jerseyKey:"galaxy",courtKey:"thunderCourt",difficultyKey:"elite",wins:0,losses:0},
    {id:"e9",name:"Lava Bruiser",profileKey:"bruiser",sneakerKey:"lavaBurst",jerseyKey:"inferno",courtKey:"sunsetPark",difficultyKey:"elite",wins:0,losses:0},
    {id:"e10",name:"Ice Tower",profileKey:"tower",sneakerKey:"iceFangHighs",jerseyKey:"frostbite",courtKey:"icePalace",difficultyKey:"nightmare",wins:0,losses:0},
    {id:"e11",name:"Cyber Reaper",profileKey:"rimreaper",sneakerKey:"chromeFlash",jerseyKey:"chromeElite",courtKey:"cyberGrid",difficultyKey:"nightmare",wins:0,losses:0},
    {id:"e12",name:"Final Boss Sniper",profileKey:"deeprange",sneakerKey:"thunderBeasts",jerseyKey:"royalLegacy",courtKey:"jungleRun",difficultyKey:"legend",wins:0,losses:0}
  ];
}
function startNewTournament(){
  tournamentState = createNationalTourState();
  gameSettings.set("gameplayPreset","simulation");
  saveTournament(false);
  showTournamentBetweenRounds("Your national tour begins in Seattle.");
}
function startTournamentRound(){
  if(!tournamentState) return;
  const idx = Math.max(0,Math.min(tournamentState.currentStopIndex ?? tournamentState.round-1,tournamentState.enemies.length-1));
  currentTournamentEnemy = tournamentState.enemies[idx];
  tournamentState.currentStopIndex = idx;
  tournamentState.round = idx+1;
  tournamentState.nextOpponentId = currentTournamentEnemy.id;
  tournamentState.nextCourtKey = currentTournamentEnemy.courtKey;
  isTournamentGame = true;
  isChampionshipGame = idx === NATIONAL_TOUR_STOPS.length-1;
  gameSettings.set("gameplayPreset","simulation");
  startGame();
}
function finishTournamentGame(winner){
  gameStarted = false;
  updateTouchPresentation();
  const playerWon = winner === "p1";
  if(playerWon) tournamentState.playerWins += 1; else tournamentState.playerLosses += 1;
  if(currentTournamentEnemy){
    if(playerWon) currentTournamentEnemy.losses += 1; else currentTournamentEnemy.wins += 1;
  }
  const stopIndex = tournamentState.currentStopIndex ?? tournamentState.round-1;
  const stop = NATIONAL_TOUR_STOPS[stopIndex];
  const earnedXP = awardTourResult(playerWon);
  tournamentState.completedGames.push({
    round:stopIndex+1,
    type:isChampionshipGame?"championship":"tour-stop",
    city:stop?.city,
    opponentId:currentTournamentEnemy?.id,
    result:playerWon?"W":"L",
    xp:earnedXP,
    score:[scoreP1,scoreP2]
  });
  triggerSlowMo(.35,.65);
  if(isChampionshipGame){
    tournamentState.finalsComplete = playerWon;
    tournamentState.champion = playerWon ? TOURNAMENT_PLAYER_ID : null;
    saveTournament(false);
    if(playerWon) showTournamentVictory();
    else showTournamentBetweenRounds(`Las Vegas defended the crown. You earned ${earnedXP} XP—study the matchup and try again.`);
    return;
  }
  tournamentState.travelFrom = stopIndex;
  if(playerWon) tournamentState.currentStopIndex = Math.min(NATIONAL_TOUR_STOPS.length-1,stopIndex+1);
  tournamentState.travelTo = tournamentState.currentStopIndex;
  tournamentState.round = tournamentState.currentStopIndex+1;
  const next = tournamentState.enemies[tournamentState.currentStopIndex];
  tournamentState.nextOpponentId = next.id;
  tournamentState.nextCourtKey = next.courtKey;
  saveTournament(false);
  const result = playerWon
    ? `${stop.city} conquered. +${earnedXP} XP. The tour moves to ${NATIONAL_TOUR_STOPS[tournamentState.currentStopIndex].city}.`
    : `${stop.city} stays unfinished. +${earnedXP} XP. Rematch when ready.`;
  showTournamentBetweenRounds(result);
}
function simulateEnemyRoundResults(){
  if(!tournamentState) return;
  const shuffled = [...tournamentState.enemies].sort(()=>Math.random()-.5);
  for(let i=0;i<shuffled.length-1;i+=2){
    const a = shuffled[i], b = shuffled[i+1];
    const sa = tournamentDifficultyStrength[a.difficultyKey] || 1;
    const sb = tournamentDifficultyStrength[b.difficultyKey] || 1;
    let chanceA = sa/(sa+sb);
    chanceA = THREE.MathUtils.clamp(chanceA + (Math.random()-.5)*0.18,0.12,0.88);
    const aWon = Math.random() < chanceA;
    if(aWon){ a.wins++; b.losses++; } else { b.wins++; a.losses++; }
    tournamentState.completedGames.push({round:tournamentState.round,type:"sim",a:a.id,b:b.id,winner:aWon?a.id:b.id});
  }
}
function updateTournamentStandings(){
  if(!tournamentState) return;
  const rows = [{id:TOURNAMENT_PLAYER_ID,name:"You",wins:tournamentState.playerWins,losses:tournamentState.playerLosses,strength:6,difficultyKey:"player"}];
  tournamentState.enemies.forEach(e=>rows.push({id:e.id,name:e.name,wins:e.wins,losses:e.losses,strength:tournamentDifficultyStrength[e.difficultyKey]||1,difficultyKey:e.difficultyKey}));
  rows.sort((a,b)=> (b.wins-a.wins) || (a.losses-b.losses) || (b.strength-a.strength) || (Math.random()-.5));
  tournamentState.standings = rows.map((r,idx)=>({...r,rank:idx+1}));
}
function standingsHTML(){
  const rows = (tournamentState?.standings || []).map(r=>{
    const last = [...(tournamentState.completedGames||[])].reverse().find(g=>g.opponentId===r.id || g.a===r.id || g.b===r.id || (r.id===TOURNAMENT_PLAYER_ID && g.result));
    const lastResult = r.id===TOURNAMENT_PLAYER_ID ? (last?.result || "-") : (last?.winner===r.id?"W":(last?"L":"-"));
    return `<tr class="${r.id===TOURNAMENT_PLAYER_ID?"playerRow":""}"><td>${r.rank}</td><td>${r.name}</td><td>${r.wins}-${r.losses}</td><td>${lastResult}</td><td>${r.difficultyKey.toUpperCase()}</td></tr>`;
  }).join("");
  return `<div class="sectionTitle">Tournament Standings</div><table class="standingsTable"><thead><tr><th>Rank</th><th>Player/Enemy</th><th>Record</th><th>Last Result</th><th>Difficulty/Strength</th></tr></thead><tbody>${rows}</tbody></table><div class="sub">Top 2 after Round 12 advance to the Championship.</div>`;
}
function showTournamentBetweenRounds(resultText="Tour updated."){
  showMenuScreen("tournamentBetweenScreen");
  const stop = getCurrentTourStop();
  document.getElementById("saveTournamentBtn").textContent = "Save Tour";
  document.getElementById("continueTournamentBtn").textContent = tournamentState.finalsComplete ? "Champion" : `Play ${stop.city}`;
  document.getElementById("betweenMainMenuBtn").textContent = "Main Menu";
  document.getElementById("saveTournamentBtn").onclick = ()=>saveTournament();
  document.getElementById("continueTournamentBtn").onclick = ()=>continueTournamentFlow();
  document.getElementById("betweenMainMenuBtn").onclick = ()=>showPrimaryMenu();
  document.getElementById("tournamentBetweenTitle").textContent = tournamentState.finalsComplete ? "National 1v1 Champion" : "National 1v1 Tour";
  document.getElementById("tournamentBetweenSub").textContent = resultText;
  renderNationalTour(document.getElementById("tournamentStandingsWrap"),tournamentState);
  document.getElementById("tournamentNextPreview").textContent = tournamentState.finalsComplete ? "The national crown is yours." : `${stop.city} · ${(courts[stop.courtKey]||{}).name || stop.courtKey}`;
  document.getElementById("tournamentBetweenMsg").textContent = "";
}
function saveTournament(showToast=true){
  if(!tournamentState) return;
  localStorage.setItem(TOURNAMENT_SAVE_KEY,JSON.stringify(tournamentState));
  if(showToast){
    const el = document.getElementById("tournamentBetweenMsg");
    if(el) el.textContent = "Tour saved.";
  }
}
function loadTournament(){
  const raw = localStorage.getItem(TOURNAMENT_SAVE_KEY) || localStorage.getItem(LEGACY_TOURNAMENT_SAVE_KEY);
  if(!raw){ showPrimaryMenu("No saved tour found."); return; }
  try{
    tournamentState = normalizeNationalTourState(JSON.parse(raw));
  }catch(e){ showPrimaryMenu("The saved tour could not be loaded."); return; }
  if(!tournamentState?.active){ showPrimaryMenu("No saved tour found."); return; }
  selectedP1 = tournamentState.playerProfileKey || selectedP1;
  selectedP1Sneaker = tournamentState.playerSneakerKey || selectedP1Sneaker;
  selectedP1Jersey = tournamentState.playerJerseyKey || selectedP1Jersey;
  saveTournament(false);
  showTournamentBetweenRounds(tournamentState.finalsComplete ? "Championship tour loaded." : "Tour loaded.");
}
function continueTournamentFlow(){
  if(!tournamentState) return;
  if(tournamentState.finalsComplete){ showTournamentVictory(); return; }
  startTournamentRound();
}
function showTournamentFinalsScreen(){
  updateTournamentStandings();
  const top2 = tournamentState.standings.slice(0,2);
  const playerTop2 = top2.some(r=>r.id===TOURNAMENT_PLAYER_ID);
  tournamentState.finalsQualified = playerTop2;
  if(!playerTop2){
    showTournamentLoss("You finished outside the Top 2. Tournament over.");
    saveTournament(false);
    return;
  }
  const other = top2.find(r=>r.id!==TOURNAMENT_PLAYER_ID);
  const opp = tournamentState.enemies.find(e=>e.id===other.id);
  currentTournamentEnemy = opp;
  showMenuScreen("tournamentBetweenScreen");
  document.getElementById("tournamentBetweenTitle").textContent = "You made the Championship!";
  document.getElementById("tournamentBetweenSub").textContent = `${opp.name} (${opp.wins}-${opp.losses}) at ${(courts.goldLeague||courts[opp.courtKey]).name}. Championship opponent is faster and stronger.`;
  document.getElementById("tournamentStandingsWrap").innerHTML = standingsHTML();
  document.getElementById("tournamentNextPreview").textContent = "Championship game to 11, win by 2.";
  document.getElementById("continueTournamentBtn").textContent = "Start Championship";
  document.getElementById("continueTournamentBtn").onclick = ()=>startChampionshipGame();
}
function startChampionshipGame(){
  if(!currentTournamentEnemy) return;
  isTournamentGame = true;
  isChampionshipGame = true;
  selectedCourt = "goldLeague" in courts ? "goldLeague" : currentTournamentEnemy.courtKey;
  tournamentState.nextCourtKey = selectedCourt;
  startGame();
}
function showTournamentVictory(){
  isTournamentGame = false;
  showMenuScreen("tournamentBetweenScreen");
  document.getElementById("tournamentBetweenTitle").textContent = "NATIONAL 1V1 CHAMPION";
  document.getElementById("tournamentBetweenSub").textContent = `Final record ${tournamentState.playerWins}-${tournamentState.playerLosses}. Las Vegas belongs to you.`;
  renderNationalTour(document.getElementById("tournamentStandingsWrap"),tournamentState);
  document.getElementById("tournamentNextPreview").textContent = "SEATTLE TO LAS VEGAS · TOUR COMPLETE";
  document.getElementById("continueTournamentBtn").textContent = "New Tour";
  document.getElementById("continueTournamentBtn").onclick = ()=>showTournamentSetup();
  document.getElementById("betweenMainMenuBtn").textContent = "Main Menu";
  document.getElementById("saveTournamentBtn").textContent = "Quick Play";
  document.getElementById("saveTournamentBtn").onclick = ()=>showQuickPlaySetup();
  spawnConfetti();
  updateLEDBoards("NATIONAL CHAMPION");
}
function showTournamentLoss(message){
  isTournamentGame = false;
  showMenuScreen("tournamentBetweenScreen");
  document.getElementById("tournamentBetweenTitle").textContent = "Tour Paused";
  document.getElementById("tournamentBetweenSub").textContent = message;
  renderNationalTour(document.getElementById("tournamentStandingsWrap"),tournamentState);
  document.getElementById("tournamentNextPreview").textContent = "The route remains open.";
  document.getElementById("continueTournamentBtn").textContent = "Return to Tour";
  document.getElementById("continueTournamentBtn").onclick = ()=>showTournamentBetweenRounds("Ready for the next matchup.");
}
function createTournamentBoostedProfile(enemy,isFinal=false){
  const base = JSON.parse(JSON.stringify(profiles[enemy.profileKey] || profiles.claw));
  const roundNum = (tournamentState?.currentStopIndex ?? tournamentState?.round-1 ?? 0)+1;
  const roundBoost = 1 + (roundNum-1)*0.012;
  ["speed","sprint","block","steal","layup","shot","handle","hook"].forEach(k=>base[k] *= roundBoost);
  const archetype = enemy.archetype || "";
  if(archetype.includes("Lightning")){ base.speed*=1.1;base.sprint*=1.12; }
  if(archetype.includes("Magician")){ base.handle*=1.16;base.steal*=1.04; }
  if(archetype.includes("Lockdown")||archetype.includes("Stopper")){ base.block*=1.12;base.steal*=1.14; }
  if(archetype.includes("Sniper")){ base.shot*=1.16;base.handle*=1.06; }
  if(archetype.includes("Beast")||archetype.includes("Tower")){ base.width*=1.1;base.block*=1.12;base.layup*=1.08; }
  if(archetype.includes("High Flyer")){ base.jump*=1.15;base.layup*=1.12; }
  if(archetype.includes("Pickpocket")){ base.steal*=1.2;base.speed*=1.05; }
  if(archetype.includes("Midrange")){ base.shot*=1.12;base.handle*=1.08; }
  base.width *= 1+(roundBoost-1)*.25;
  if(isFinal){
    ["speed","sprint","block","steal","layup","shot","handle","hook","jump"].forEach(k=>base[k]*=1.08);
    base.width *= 1.06;
  }
  return base;
}

function toHex(n){ return "#" + n.toString(16).padStart(6,"0"); }

function drawPattern(ctx,patternName,palette,area={x:0,y:0,w:256,h:256}){
  const {x,y,w,h} = area;
  ctx.save();
  ctx.beginPath(); ctx.rect(x,y,w,h); ctx.clip();
  const c1 = toHex(palette.primary || 0x222222);
  const c2 = toHex(palette.secondary || 0xffffff);
  const trim = toHex(palette.trim || palette.accent || 0xffd56a);
  if(patternName === "flames"){
    for(let i=0;i<8;i++){ ctx.fillStyle = i%2?"rgba(255,210,120,.5)":"rgba(255,90,40,.5)"; ctx.beginPath(); ctx.moveTo(x+i*w/8,y+h); ctx.quadraticCurveTo(x+i*w/8+w/18,y+h*.35-Math.random()*h*.2,x+i*w/8+w/10,y+h); ctx.fill(); }
  }else if(patternName === "lightning"){
    ctx.strokeStyle = trim; ctx.lineWidth = 3;
    for(let i=0;i<4;i++){ const sx=x+w*(.08+i*.24); ctx.beginPath(); ctx.moveTo(sx,y+h*.1); ctx.lineTo(sx+w*.07,y+h*.35); ctx.lineTo(sx+w*.02,y+h*.37); ctx.lineTo(sx+w*.11,y+h*.78); ctx.stroke(); }
  }else if(patternName === "ice"){
    ctx.strokeStyle = "rgba(255,255,255,.55)"; ctx.lineWidth = 2;
    for(let i=0;i<11;i++){ ctx.beginPath(); const sx=x+Math.random()*w, sy=y+Math.random()*h; ctx.moveTo(sx,sy); ctx.lineTo(sx+(Math.random()-.5)*36,sy+(Math.random()-.5)*36); ctx.stroke(); }
  }else if(patternName === "slime" || patternName === "drip"){
    ctx.fillStyle = "rgba(100,255,70,.45)";
    for(let i=0;i<7;i++) ctx.fillRect(x+i*w/7,y+h*.05,w/12,h*(.28+Math.random()*.46));
  }else if(patternName === "claw"){
    ctx.strokeStyle = "rgba(255,255,255,.75)"; ctx.lineWidth = 4;
    for(let i=0;i<3;i++){ ctx.beginPath(); ctx.moveTo(x+w*(.15+i*.2),y+h*.15); ctx.lineTo(x+w*(.35+i*.2),y+h*.9); ctx.stroke(); }
  }else if(patternName === "city"){
    ctx.fillStyle = "rgba(255,255,255,.22)";
    for(let i=0;i<12;i++) ctx.fillRect(x+i*w/12,y+h*(.45+Math.random()*.3),w/15,h*.5);
  }else if(patternName === "stars"){
    ctx.fillStyle = "rgba(255,255,255,.82)";
    for(let i=0;i<30;i++) ctx.fillRect(x+Math.random()*w,y+Math.random()*h,2,2);
  }else if(patternName === "chrome"){
    const g = ctx.createLinearGradient(x,y,x+w,y+h); g.addColorStop(0,"rgba(255,255,255,.45)"); g.addColorStop(.5,"rgba(80,120,160,.25)"); g.addColorStop(1,"rgba(255,255,255,.5)");
    ctx.fillStyle = g; ctx.fillRect(x,y,w,h);
  }else if(patternName === "pulse" || patternName === "wave"){
    ctx.strokeStyle = trim; ctx.lineWidth = 2;
    for(let i=0;i<6;i++){ ctx.beginPath(); for(let px=0;px<=w;px+=8){ const py = y + h*(.2+i*.12) + Math.sin(px*.08+i)*6; if(px===0)ctx.moveTo(x+px,py); else ctx.lineTo(x+px,py);} ctx.stroke(); }
  }else if(patternName === "camo"){
    [c1,c2,trim].forEach((col,idx)=>{ ctx.fillStyle = col + (idx===2?"aa":"99"); for(let i=0;i<9;i++) ctx.fillRect(x+Math.random()*w,y+Math.random()*h,20+Math.random()*25,12+Math.random()*20); });
  }else if(patternName === "sunburst"){
    ctx.strokeStyle = "rgba(255,235,150,.5)"; ctx.lineWidth = 3;
    for(let i=0;i<14;i++){ const a=(i/14)*Math.PI*2; ctx.beginPath(); ctx.moveTo(x+w/2,y+h/2); ctx.lineTo(x+w/2+Math.cos(a)*w*.55,y+h/2+Math.sin(a)*h*.55); ctx.stroke(); }
  }else if(patternName === "crown"){
    ctx.strokeStyle = trim; ctx.lineWidth = 3; ctx.strokeRect(x+w*.32,y+h*.28,w*.36,h*.3);
    ctx.beginPath(); ctx.moveTo(x+w*.32,y+h*.28); ctx.lineTo(x+w*.42,y+h*.15); ctx.lineTo(x+w*.5,y+h*.28); ctx.lineTo(x+w*.58,y+h*.13); ctx.lineTo(x+w*.68,y+h*.28); ctx.stroke();
  }else if(patternName === "fang"){
    ctx.fillStyle = "rgba(255,255,255,.7)";
    for(let i=0;i<5;i++) ctx.fillRect(x+w*(.12+i*.16),y+h*.1,8,h*.75);
  }else if(patternName === "prism" || patternName === "geo"){
    for(let i=0;i<12;i++){ ctx.fillStyle = `hsla(${(i*33)%360},80%,70%,.35)`; ctx.beginPath(); ctx.moveTo(x+Math.random()*w,y+Math.random()*h); ctx.lineTo(x+Math.random()*w,y+Math.random()*h); ctx.lineTo(x+Math.random()*w,y+Math.random()*h); ctx.fill(); }
  }else if(patternName === "petal"){
    ctx.fillStyle = "rgba(255,190,220,.45)";
    for(let i=0;i<11;i++){ ctx.beginPath(); const px=x+Math.random()*w, py=y+Math.random()*h; ctx.ellipse(px,py,8,14,Math.random()*Math.PI,0,Math.PI*2); ctx.fill(); }
  }else if(patternName === "slash" || patternName === "slash_city"){
    ctx.fillStyle = "rgba(255,255,255,.25)";
    for(let i=0;i<5;i++){
      const sx = x + w*(0.1 + i*0.18);
      ctx.beginPath();
      ctx.moveTo(sx,y+h*.06);
      ctx.lineTo(sx+w*.11,y+h*.06);
      ctx.lineTo(sx-w*.07,y+h*.94);
      ctx.lineTo(sx-w*.18,y+h*.94);
      ctx.closePath();
      ctx.fill();
    }
    if(patternName === "slash_city"){
      ctx.fillStyle = "rgba(220,240,255,.35)";
      for(let i=0;i<8;i++) ctx.fillRect(x+w*(0.05+i*0.115),y+h*.55,w*.07,h*.4-(i%3)*h*.08);
    }
  }else if(patternName === "royal_trim"){
    ctx.strokeStyle = trim;
    ctx.lineWidth = Math.max(2,w*0.02);
    ctx.strokeRect(x+w*.08,y+h*.08,w*.84,h*.84);
    ctx.strokeRect(x+w*.18,y+h*.18,w*.64,h*.64);
    ctx.fillStyle = "rgba(255,213,106,.22)";
    ctx.fillRect(x+w*.2,y+h*.2,w*.6,h*.6);
  }else if(patternName === "scratch" || patternName === "talon" || patternName === "beast" || patternName === "venom"){
    ctx.strokeStyle = patternName === "venom" ? "rgba(130,255,120,.7)" : "rgba(255,255,255,.7)";
    ctx.lineWidth = Math.max(2,w*0.015);
    for(let i=0;i<5;i++){
      const sx = x + w*(0.08+i*0.17);
      ctx.beginPath();
      ctx.moveTo(sx,y+h*.15);
      ctx.lineTo(sx+w*.12,y+h*.88);
      ctx.stroke();
    }
    if(patternName === "beast"){
      ctx.fillStyle = "rgba(255,140,70,.22)";
      for(let i=0;i<4;i++) ctx.fillRect(x+w*(.12+i*.2),y+h*.2,w*.1,h*.6);
    }
  }else if(patternName === "frost"){
    ctx.strokeStyle = "rgba(255,255,255,.6)";
    ctx.lineWidth = Math.max(1.5,w*0.008);
    for(let i=0;i<7;i++){
      const sy = y+h*(0.12+i*0.11);
      ctx.beginPath();
      ctx.moveTo(x+w*.08,sy);
      ctx.lineTo(x+w*.92,sy+h*.04*Math.sin(i*1.6));
      ctx.stroke();
    }
    ctx.fillStyle = "rgba(255,255,255,.24)";
    for(let i=0;i<16;i++){
      const px = x+w*(0.08 + (i%8)*0.11);
      const py = y+h*(0.1 + Math.floor(i/8)*0.36 + ((i%2)*0.03));
      ctx.fillRect(px,py,3,3);
    }
  }
  ctx.restore();
}

function makeJerseyTexture(jerseyDef,numberText="23"){
  const c = document.createElement("canvas"); c.width = 512; c.height = 512;
  const ctx = c.getContext("2d");
  const primary = toHex(jerseyDef.primary);
  const secondary = toHex(jerseyDef.secondary);
  const trim = toHex(jerseyDef.trim);
  const bodyX = 88, bodyY = 62, bodyW = 336, bodyH = 382;
  ctx.fillStyle = primary; ctx.fillRect(0,0,512,512);
  const panelGrad = ctx.createLinearGradient(0,bodyY,0,bodyY+bodyH);
  panelGrad.addColorStop(0,secondary);
  panelGrad.addColorStop(1,primary);
  ctx.fillStyle = panelGrad;
  ctx.fillRect(bodyX,bodyY,bodyW,bodyH);
  drawPattern(ctx,jerseyDef.pattern,{primary:jerseyDef.primary,secondary:jerseyDef.secondary,trim:jerseyDef.trim},{x:bodyX+8,y:bodyY+16,w:bodyW-16,h:bodyH-42});
  ctx.fillStyle = trim;
  ctx.fillRect(bodyX-18,bodyY+20,14,bodyH-38); ctx.fillRect(bodyX+bodyW+4,bodyY+20,14,bodyH-38);
  ctx.fillRect(bodyX+16,bodyY+22,bodyW-32,12);
  ctx.fillRect(bodyX+26,bodyY+bodyH-26,bodyW-52,11);
  ctx.fillRect(bodyX+14,bodyY+56,18,bodyH-112);
  ctx.fillRect(bodyX+bodyW-32,bodyY+56,18,bodyH-112);
  ctx.strokeStyle = "rgba(255,255,255,.42)";
  ctx.lineWidth = 3;
  ctx.strokeRect(bodyX+5,bodyY+5,bodyW-10,bodyH-10);
  ctx.fillStyle = "#ffffff";
  ctx.strokeStyle = "rgba(0,0,0,.35)";
  ctx.lineWidth = 7;
  ctx.font = "bold 136px Arial";
  ctx.textAlign = "center";
  ctx.textBaseline = "middle";
  ctx.strokeText(numberText,256,250);
  ctx.fillText(numberText,256,250);
  ctx.fillStyle = trim;
  ctx.font = "bold 36px Arial";
  ctx.fillText("ACADEMY",256,327);
  const tex = new THREE.CanvasTexture(c); tex.needsUpdate=true; return tex;
}
function makeShortsTexture(jerseyDef){
  const c = document.createElement("canvas"); c.width=256; c.height=256; const ctx=c.getContext("2d");
  ctx.fillStyle = toHex(jerseyDef.secondary); ctx.fillRect(0,0,256,256);
  drawPattern(ctx,jerseyDef.pattern,{primary:jerseyDef.primary,secondary:jerseyDef.secondary,trim:jerseyDef.trim},{x:16,y:18,w:224,h:212});
  ctx.fillStyle = toHex(jerseyDef.trim); ctx.fillRect(0,0,256,16); ctx.fillRect(0,240,256,16); ctx.fillRect(20,0,10,256); ctx.fillRect(226,0,10,256);
  ctx.fillRect(106,0,44,256);
  ctx.fillStyle = "rgba(255,255,255,.18)";
  ctx.fillRect(36,38,18,162); ctx.fillRect(202,38,18,162);
  const tex = new THREE.CanvasTexture(c); tex.needsUpdate=true; return tex;
}
function makeShoeTexture(shoeDef){
  const c = document.createElement("canvas"); c.width=320; c.height=160; const ctx=c.getContext("2d");
  const bodyGrad = ctx.createLinearGradient(0,0,320,0);
  bodyGrad.addColorStop(0,toHex(shoeDef.primary)); bodyGrad.addColorStop(.62,toHex(shoeDef.accent)); bodyGrad.addColorStop(1,toHex(shoeDef.primary));
  ctx.fillStyle = bodyGrad; ctx.fillRect(0,0,320,160);
  drawPattern(ctx,shoeDef.pattern||"geo",{primary:shoeDef.primary,secondary:shoeDef.accent,trim:shoeDef.sole,accent:shoeDef.accent},{x:16,y:20,w:288,h:104});
  ctx.fillStyle = toHex(shoeDef.sole); ctx.fillRect(0,126,320,34);
  ctx.fillStyle = "rgba(255,255,255,.48)"; ctx.fillRect(10,114,300,3);
  ctx.fillStyle = "rgba(0,0,0,.22)"; ctx.fillRect(0,141,320,5);
  ctx.fillStyle = "#f6f6f6";
  for(let i=0;i<6;i++) ctx.fillRect(108 + i*21,72 + (i%2)*2,10,4);
  ctx.fillStyle = toHex(shoeDef.accent); ctx.fillRect(18,46,26,42); ctx.fillRect(250,84,58,18); ctx.fillRect(120,102,80,8);
  if(shoeDef.type === "high" || shoeDef.type === "boost") ctx.fillRect(34,28,44,32);
  if(shoeDef.type === "boost") ctx.fillRect(72,132,40,11);
  if(shoeDef.type === "glow") { ctx.fillStyle = "rgba(120,244,255,.62)"; ctx.fillRect(224,126,72,13); }
  const tex = new THREE.CanvasTexture(c); tex.needsUpdate=true; return tex;
}

function drawSneakerPreview(canvas,sneaker,time=0){
  const ctx = canvas.getContext("2d"); const w = canvas.width, h = canvas.height;
  ctx.clearRect(0,0,w,h); ctx.fillStyle = "rgba(8,14,26,.86)"; ctx.fillRect(0,0,w,h);
  const swing = Math.sin(time*2.1)*6; const x = w*.52 + swing; const y = h*.58;
  ctx.save(); ctx.translate(x,y); ctx.rotate(-0.07);
  const soleH = sneaker.type==="boost"?18:14;
  ctx.fillStyle = toHex(sneaker.sole); ctx.fillRect(-68,6,136,soleH);
  ctx.fillStyle = "rgba(255,255,255,.36)"; ctx.fillRect(-64,8,124,3);
  ctx.fillStyle = toHex(sneaker.primary);
  ctx.beginPath(); ctx.moveTo(-66,11); ctx.quadraticCurveTo(-18,-31,46,-19); ctx.lineTo(66,2); ctx.lineTo(58,12); ctx.lineTo(-56,20); ctx.closePath(); ctx.fill();
  ctx.fillStyle = toHex(sneaker.accent); ctx.fillRect(-50,-6,18,18); ctx.fillRect(-8,-10,44,6); ctx.fillRect(30,4,22,9);
  ctx.fillStyle = "#f2f2f2";
  for(let i=0;i<5;i++){ ctx.fillRect(-18 + i*12,-5 + (i%2),7,3); }
  ctx.fillStyle = "rgba(255,255,255,.3)"; ctx.fillRect(-14,-8,30,2);
  if(sneaker.type==="high" || sneaker.type==="boost"){ ctx.fillStyle = toHex(sneaker.accent); ctx.fillRect(-60,-20,28,17); }
  if(sneaker.type==="boost"){ ctx.fillStyle = "rgba(165,248,255,.82)"; ctx.fillRect(10,12,34,6); }
  if(sneaker.type==="glow"){ ctx.fillStyle = "rgba(120,240,255,.65)"; ctx.fillRect(22,10,28,7); ctx.fillRect(-4,10,16,6); }
  ctx.restore();
}

function drawJerseyPreview(canvas,jersey){
  const ctx = canvas.getContext("2d"); const w = canvas.width, h = canvas.height;
  ctx.clearRect(0,0,w,h); ctx.fillStyle="rgba(8,14,26,.8)"; ctx.fillRect(0,0,w,h);
  const tex = makeJerseyTexture(jersey,"23");
  ctx.drawImage(tex.image,78,10,100,56);
  const shorts = makeShortsTexture(jersey);
  ctx.drawImage(shorts.image,184,20,56,44);
}
function drawCourtPreview(canvas,court){
  const ctx = canvas.getContext("2d"); const w = canvas.width, h = canvas.height;
  ctx.clearRect(0,0,w,h); ctx.fillStyle = court.woodA; ctx.fillRect(0,0,w,h); ctx.fillStyle = court.woodB; ctx.fillRect(0,h*.2,w,h*.16); ctx.fillRect(0,h*.62,w,h*.16);
  drawPattern(ctx,court.theme,{primary:parseInt(court.woodA.replace('#',''),16),secondary:parseInt(court.woodB.replace('#',''),16),trim:court.trimColor},{x:14,y:12,w:w-28,h:h-24});
  ctx.strokeStyle = toHex(court.lineColor); ctx.lineWidth=2.4; ctx.strokeRect(10,8,w-20,h-16); ctx.beginPath(); ctx.moveTo(w/2,8); ctx.lineTo(w/2,h-8); ctx.stroke(); ctx.beginPath(); ctx.arc(w/2,h/2,12,0,Math.PI*2); ctx.stroke();
  ctx.fillStyle = toHex(court.trimColor); ctx.fillRect(0,0,w,4); ctx.fillRect(0,h-4,w,4);
  ctx.fillStyle = "rgba(255,255,255,.95)"; ctx.font = "bold 13px Arial"; ctx.textAlign = "center"; ctx.fillText(court.logoText,w/2,h/2+4);
}

function buildOptionButtons(containerId, options){
  const box = document.getElementById(containerId);
  options.forEach(opt=>{
    const btn = document.createElement("button");
    btn.className = "optionBtn";
    btn.textContent = opt.label;
    btn.onclick = () => {
      opt.onClick();
      [...box.children].forEach(c=>c.classList.remove("selected"));
      options.forEach((o,i)=>{ if(o.selected()) box.children[i].classList.add("selected"); });
    };
    if(opt.selected()) btn.classList.add("selected");
    box.appendChild(btn);
  });
}

function drawProfileCardPreview(canvas,profile,jerseyKey,sneakerKey){
  const ctx = canvas.getContext("2d");
  const w = canvas.width, h = canvas.height;
  ctx.clearRect(0,0,w,h);
  ctx.fillStyle = "rgba(8,14,24,.9)"; ctx.fillRect(0,0,w,h);
  const jersey = jerseys[jerseyKey] || jerseys.classicBlue;
  const shoe = sneakers[sneakerKey] || sneakers.classic;
  const scale = THREE.MathUtils.clamp(profile.height,0.9,1.24);
  const bw = 26*profile.width, bh = 34*scale;
  ctx.fillStyle = toHex(profile.skin); ctx.beginPath(); ctx.arc(40,24,10*scale,0,Math.PI*2); ctx.fill();
  ctx.fillStyle = toHex(profile.hair); ctx.fillRect(30,13,20,6);
  ctx.fillStyle = toHex(jersey.primary); ctx.fillRect(28,34,bw,bh);
  ctx.fillStyle = toHex(jersey.secondary); ctx.fillRect(30,58,bw-4,18*scale);
  ctx.fillStyle = toHex(jersey.trim); ctx.fillRect(28,34,bw,4);
  ctx.fillStyle = "rgba(255,255,255,.9)"; ctx.font = "bold 11px Arial"; ctx.fillText(profile.number || "0",40,52);
  ctx.fillStyle = toHex(shoe.primary); ctx.fillRect(24,76,18,7); ctx.fillRect(46,76,18,7);
  ctx.fillStyle = toHex(shoe.sole); ctx.fillRect(24,82,18,3); ctx.fillRect(46,82,18,3);
  ctx.fillStyle = "rgba(255,255,255,.75)"; ctx.font = "bold 10px Arial"; ctx.fillText(profile.bodyType || "",104,18);
  ctx.fillStyle = toHex(jersey.primary); ctx.fillRect(84,30,56,20); ctx.fillStyle = toHex(jersey.secondary); ctx.fillRect(84,50,56,12);
  ctx.fillStyle = toHex(shoe.primary); ctx.fillRect(84,66,26,10); ctx.fillStyle = toHex(shoe.sole); ctx.fillRect(84,76,26,4);
}

function buildCharacterCards(containerId, which){
  const box = document.getElementById(containerId);
  Object.entries(profiles).forEach(([key,p]) => {
    const div = document.createElement("div");
    const selected = which === "p1" ? key === selectedP1 : (which === "tournamentP1" ? key === selectedP1 : key === selectedP2);
    div.className = "card" + (selected ? " selected" : "");
    div.innerHTML = `
      <div class="cardName">${p.name} #${p.number}</div>
      <div class="cardDesc">${p.desc}</div>
      <canvas class="cardPreview" width="160" height="86" data-profile="${key}"></canvas>
      <div class="cardDesc">${p.bodyType || "Balanced"} • Speed ${Math.round(p.speed*100)} | Handle ${Math.round(p.handle*100)} | Block ${Math.round(p.block*100)}</div>
    `;
    div.onclick = () => {
      if(which === "p1") selectedP1 = key;
      else if(which === "tournamentP1") selectedP1 = key;
      else selectedP2 = key;
      box.querySelectorAll(".card").forEach(c => c.classList.remove("selected"));
      div.classList.add("selected");
      if(which === "tournamentP1") refreshTournamentSelectorUI();
    };
    box.appendChild(div);
    const canvas = div.querySelector(".cardPreview");
    const jerseyKey = which === "p1" || which === "tournamentP1" ? selectedP1Jersey : selectedP2Jersey;
    const sneakerKey = which === "p1" || which === "tournamentP1" ? selectedP1Sneaker : selectedP2Sneaker;
    if(canvas) drawProfileCardPreview(canvas,p,jerseyKey,sneakerKey);
  });
}

function startGame(){
  if(isTournamentGame && tournamentState){
    twoPlayerMode = false;
    const enemy = currentTournamentEnemy || tournamentState.enemies.find(e=>e.id===tournamentState.nextOpponentId);
    if(enemy){
      selectedP1 = tournamentState.playerProfileKey;
      selectedP1Sneaker = tournamentState.playerSneakerKey;
      selectedP1Jersey = tournamentState.playerJerseyKey;
      selectedP2 = enemy.profileKey;
      selectedP2Sneaker = enemy.sneakerKey;
      selectedP2Jersey = enemy.jerseyKey;
      selectedCourt = enemy.courtKey || tournamentState.nextCourtKey || selectedCourt;
      selectedDifficulty = enemy.difficultyKey || "normal";
      targetScore = 11;
      winBy = 2;
    }
    gameSettings.set("gameplayPreset","simulation");
  }
  winBy = isTournamentGame ? 2 : (gameMode.winBy2 ? 2 : 1);
  if(p1) scene.remove(p1.group);
  if(p2) scene.remove(p2.group);
  if(debugDirHelpers && debugDirHelpers.group){
    scene.remove(debugDirHelpers.group);
    debugDirHelpers = null;
  }

  const p1Profile = JSON.parse(JSON.stringify(profiles[selectedP1]));
  p1 = createCharacter(p1Profile, P1_OFFENSE_START, "p1");
  let p2Profile = JSON.parse(JSON.stringify(profiles[selectedP2]));
  if(isTournamentGame && currentTournamentEnemy){
    p2Profile = createTournamentBoostedProfile(currentTournamentEnemy,isChampionshipGame);
  }
  p2 = createCharacter(p2Profile, P2_DEFENSE_START, "p2");
  applyCourtTheme(selectedCourt);

  scene.add(p1.group);
  scene.add(p2.group);
  ensureDebugDirectionMeshes();

  document.getElementById("menu").style.display = "none";
  document.getElementById("p1Label").style.display = "none";
  document.getElementById("p2Label").style.display = "none";
  document.getElementById("modeText").textContent = twoPlayerMode ? "2P Split Screen" : "1P vs Bot";

  gameStarted = true;
  gamePaused = false;
  gameOver = false;
  scoreP1 = 0;
  scoreP2 = 0;
  hideWinnerOverlay();
  updateScoreboard();
  document.getElementById("p1HudName").textContent = p1.profile.name;
  document.getElementById("p2HudName").textContent = p2.profile.name;
  document.body.classList.toggle("tutorial-active",tutorialMode);
  gameSettings.apply();
  adaptiveQuality.applyRequestedLevel();
  updateTouchPresentation();
  if(isTournamentGame){
    const stop = getCurrentTourStop();
    updateLEDBoards(`${stop.city.toUpperCase()} · NATIONAL TOUR`);
  }
  initBotBrain();
  resetPossession("p1", true);
}
