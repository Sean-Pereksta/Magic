import {VIEW,PHYSICS,POWER_LABELS} from "./config.js";
import {InputController} from "./input.js";
import {TouchLayoutEditor} from "./touch-layout.js";
import {Camera} from "./camera.js";
import {createPlayer,damagePlayer,updateMovers,updatePlayer,updateWorldEnvironment} from "./physics.js";
import {createLevel,updateWorldStreaming,persistWorldState,restoreWorldState,CHUNK_WIDTH,LAYOUT_VERSION} from "./world/generator.js";
import {createEndlessRun,beginEndlessSegment,advanceEndlessRun,recordEndlessDefeats,endlessMeters,formatMeters} from "./world/endless-run.js";
import {updateGateProgress} from "./world/gate-progress.js";
import {REGIONS,BIOME_ORDER,endlessChunkCount} from "./world/regions.js";
import {JOURNEY_NODES,JOURNEY_BY_ID,completeJourneyNode,reachableNodes} from "./world/journey.js";
import {BossController} from "./bosses/boss-controller.js";
import {updateEnemies,emitCombatEffect} from "./enemies/enemies.js";
import {collectPickups,grantBlessing,updateBlessings,useAbility} from "./powers/blessings.js";
import {cleanBoosts,PICKUP_MESSAGES} from "./powers/boosts.js";
import {GameStorage} from "./save/storage.js";
import {AssetManager} from "./assets/asset-manager.js";
import {Renderer} from "./render/renderer.js";
import {HUD,formatTime} from "./render/hud.js";

const byId=id=>document.getElementById(id);
const SCREEN_IDS={menu:"mainMenu",worlds:"worldScreen",loading:"loadingScreen",paused:"pauseScreen",dead:"deathScreen",complete:"completeScreen",info:"infoScreen",touchLayout:"touchLayoutScreen"};
const UI_ART=["ui-heart-full","ui-heart-empty","ui-touch-jump","ui-touch-dash","ui-touch-blessing","ui-hud-faith"];
const clone=value=>JSON.parse(JSON.stringify(value));
const newStats=()=>({time:0,collectibles:0,secrets:0,damage:0,deaths:0,totalSecrets:0});
const randomSeed=()=>{const n=new Uint32Array(1);if(globalThis.crypto?.getRandomValues)crypto.getRandomValues(n);else n[0]=Date.now();return n[0]||1;};
function menuWorld(){return{id:"menu",name:"Galilee Countryside",region:"countryside",biome:"countryside",width:5000,height:1000,platforms:[],movers:[],hazards:[],enemies:[],pickups:[],props:[],scenery:[],checkpoints:[],effects:[],bossHazards:[],enemyProjectiles:[],weather:{disabled:true}};}

export class JesusRunGame{
  constructor(){
    this.canvas=byId("gameCanvas");this.input=new InputController();this.storage=new GameStorage();this.assets=new AssetManager();this.camera=new Camera();this.renderer=new Renderer(this.canvas,this.assets);this.hud=new HUD(this.assets);
    this.player=createPlayer();this.world=menuWorld();this.bossController=new BossController(boss=>this.handleBossDefeated(boss));this.journeySave=this.storage.loadJourney();
    this.mode=null;this.screen="menu";this.running=false;this.paused=false;this.initialized=false;this.debug=false;this.time=0;this.fps=60;this.accumulator=0;this.loadGeneration=0;this.lastFrame=performance.now();this.runStats=newStats();this.score=0;this.depth=0;this.retryTask=null;this.checkpoint=null;this.prefetchedBoss=false;this.journeyNode=null;this.challenge=false;
    this.bindUI();this.touchLayout=new TouchLayoutEditor(this);this.resize();window.addEventListener("resize",()=>this.resize());window.addEventListener("blur",()=>{if(this.screen==="playing")this.pause();});document.addEventListener("visibilitychange",()=>{if(document.hidden&&this.screen==="playing")this.pause();});
    window.addEventListener("pagehide",()=>this.saveCurrentProgress());requestAnimationFrame(t=>this.frame(t));this.boot();
  }
  resize(){const r=this.canvas.getBoundingClientRect(),aspect=Math.max(.3,r.width/Math.max(1,r.height));VIEW.width=Math.round(Math.max(360,Math.min(1440,540*aspect)));VIEW.height=Math.round(VIEW.width/aspect);this.canvas.width=VIEW.width;this.canvas.height=VIEW.height;this.renderer.clearCache();this.touchLayout?.apply();if(this.initialized)this.camera.reset(this.player,this.world);}
  bindUI(){
    const bind=(id,fn)=>byId(id).addEventListener("click",fn);
    bind("journeyBtn",()=>this.showWorlds());bind("endlessBtn",()=>this.startEndless());bind("controlsBtn",()=>this.showControls("menu"));bind("recordsBtn",()=>this.showRecords());bind("worldBack",()=>this.showMainMenu());
    bind("continueBtn",()=>this.resumeSaved());bind("pauseBtn",()=>this.pause());bind("resumeBtn",()=>this.resume());bind("restartBtn",()=>this.restartLevel());bind("pauseMenu",()=>this.showMainMenu());bind("pauseControls",()=>this.showControls("paused"));
    bind("retryBtn",()=>this.retryCheckpoint());bind("deathMenu",()=>this.showMainMenu());bind("completeWorlds",()=>this.mode==="endless"?this.showMainMenu():this.showWorlds());
    bind("nextBtn",()=>{if(this.mode==="endless")this.startEndless();else{const node=JOURNEY_NODES[Math.min(5,(this.journeyNode?.index||0)+1)];if(this.journeyNode?.index===5)this.showWorlds();else this.startLevel(node.id);}});
    bind("infoBack",()=>{this.input.cancelRebind();this.setScreen(this.infoReturn||"menu");});bind("loadRetry",()=>this.retryTask?.());bind("loadBack",()=>{this.loadGeneration++;this.assets.cancelLoads();this.initialized=false;this.showMainMenu();});
    this.input.bindMobile(byId("joystick"),byId("stick"),byId("jumpBtn"),byId("dashBtn"),byId("abilityBtn"));
  }
  setScreen(screen){this.screen=screen;this.running=screen==="playing";this.paused=screen==="paused";this.input.enabled=this.running;if(!this.running)this.input.reset();this.accumulator=0;
    byId("shell").classList.toggle("endless-mode",this.mode==="endless");
    for(const[name,id]of Object.entries(SCREEN_IDS))byId(id).classList.toggle("visible",name===screen);
    byId("gameHud").classList.toggle("hidden",!["playing","paused","dead"].includes(screen));byId("mobileControls").classList.toggle("hidden",screen!=="playing");byId("bossHud").classList.toggle("hidden",screen!=="playing"||!this.bossController.boss);byId("shell").classList.toggle("playing",screen==="playing");byId("rotateHint").hidden=screen!=="playing"||this.runStats.time>8;
    if(this.running){this.lastFrame=performance.now();this.canvas.focus({preventScroll:true});}
  }
  async boot(){
    const generation=++this.loadGeneration;this.retryTask=()=>this.boot();this.setScreen("loading");this.loadingTitle("Preparing the journey");
    const ids=["countryside-background-sky","countryside-background-far","countryside-background-near","player.jesus.idle",...UI_ART];
    try{await this.assets.initialize();let loaded=0;await Promise.all(ids.map(async id=>{await this.assets.ensure(id,{pin:id.startsWith("ui-")});if(generation===this.loadGeneration)this.loadingProgress({loaded:++loaded,total:ids.length,ratio:loaded/ids.length,label:"Preparing artwork"});}));if(generation!==this.loadGeneration)return;this.initialized=true;this.configureUIArt();this.showMainMenu();}catch(error){if(generation===this.loadGeneration)this.loadingFailure(error);}
  }
  configureUIArt(){for(const[id,art]of[["jumpBtn","ui-touch-jump"],["dashBtn","ui-touch-dash"],["abilityBtn","ui-touch-blessing"]]){const e=byId(id);e.style.backgroundImage=`url("${this.assets.getURL(art)}")`;e.style.backgroundSize="100% 100%";e.style.backgroundColor="transparent";e.style.borderColor="transparent";if(id==="abilityBtn")e.querySelector("span").style.visibility="hidden";else e.textContent="";}}
  loadingTitle(title){byId("loadTitle").textContent=title;byId("loadError").hidden=true;byId("loadProgress").value=0;byId("loadLabel").textContent="Loading this world's artwork…";byId("loadStatus").textContent="";}
  loadingProgress(p){byId("loadProgress").value=p.ratio;byId("loadStatus").textContent=`${p.loaded} / ${p.total}`;}
  loadingFailure(error){this.lastLoadError={message:error.message,assets:error.failedIDs||[]};byId("loadError").hidden=false;byId("loadLabel").textContent="The world is waiting for its artwork.";byId("loadStatus").textContent="Your checkpoint is safe.";this.setScreen("loading");}
  showMainMenu(){this.saveCurrentProgress();this.setScreen("menu");this.mode=null;this.input.cancelRebind();}
  showWorlds(){this.saveCurrentProgress();this.mode="journey";this.setScreen("worlds");const container=byId("worldList"),reachable=new Set(reachableNodes(this.journeySave));container.replaceChildren();
    for(const node of JOURNEY_NODES){const done=this.journeySave.completed.includes(node.id),button=document.createElement("button"),img=new Image(),name=document.createElement("b"),detail=document.createElement("small");button.className=`world-option${done?" complete":""}`;button.disabled=!reachable.has(node.id);button.dataset.world=node.id;img.src=this.assets.getURL(`ui-world-${node.id}`);img.alt="";name.textContent=node.name;const record=this.journeySave.records[node.id];detail.textContent=done?(record?`Best ${formatTime(record.bestTime)}${record.noHit?" · No hit":""}`:"Complete"):reachable.has(node.id)?"Explore":"Locked";button.append(img,name,detail);button.addEventListener("click",()=>this.startLevel(node.id,{challenge:byId("challengeMode").checked}));container.append(button);}
    const checkpoint=this.journeySave.checkpoint;byId("continueBtn").hidden=!checkpoint;byId("challengeChoice").hidden=!this.journeySave.completed.length;byId("challengeMode").checked=false;
  }
  createRunPlayer(saved=null){const p=createPlayer();p.maxHearts=this.challenge?3:4;p.hearts=p.maxHearts;for(const power of this.journeySave.permanentPowers)grantBlessing(p,power);updateBlessings(p,{effects:[]},0);
    if(saved){p.boosts=cleanBoosts(saved.boosts);p.powers={...p.powers,...saved.powers};p.shieldUnlocked=Boolean(saved.shieldUnlocked||p.shieldUnlocked);p.holyLightCapacity=Math.max(3,Math.min(5,Number(saved.holyLightCapacity)||3));p.powers.holyLight=Math.min(p.holyLightCapacity,Math.max(0,Number(p.powers.holyLight)||0));p.hearts=Math.min(p.maxHearts,Math.max(1,Number(saved.hearts)||p.maxHearts));}return p;
  }
  playerSnapshot(){const p=this.player;return{powers:{...p.powers},boosts:cleanBoosts(p.boosts),hearts:p.hearts,shieldUnlocked:p.shieldUnlocked,holyLightCapacity:p.holyLightCapacity};}
  async startLevel(nodeId,{checkpoint=null,challenge=false,seed=null,keepStats=false,carryPlayer=null}={}){
    const node=JOURNEY_BY_ID[nodeId];if(!node)return;const generation=++this.loadGeneration;this.mode=this.mode==="endless"?"endless":"journey";this.journeyNode=node;this.challenge=checkpoint?.challenge??challenge;this.setScreen("loading");this.loadingTitle(node.name);this.retryTask=()=>this.startLevel(nodeId,{checkpoint,challenge:this.challenge,seed,keepStats,carryPlayer});
    if(!keepStats)this.runStats=checkpoint?.stats?clone(checkpoint.stats):newStats();this.checkpoint=checkpoint?clone(checkpoint):null;this.prefetchedBoss=false;this.bossController.boss=null;this.bossController.arena=null;
    this.player=this.createRunPlayer(checkpoint?.player||carryPlayer);const chosenSeed=checkpoint?.seed??seed??randomSeed();
    await new Promise(resolve=>requestAnimationFrame(resolve));if(generation!==this.loadGeneration)return;
    try{
      if(checkpoint?.boss){await this.loadBossWorld(nodeId,checkpoint.bossId||REGIONS[nodeId].bossId,generation);return;}
      const level=createLevel({biome:nodeId,seed:chosenSeed,difficulty:Math.min(6,node.difficulty+(this.challenge?1:0)+(this.mode==="endless"?Math.floor(this.depth/6):0)),mode:this.mode,abilities:this.player.powers,chunkCount:this.mode==="endless"?endlessChunkCount(this.depth):null,layoutVersion:checkpoint?(checkpoint.worldState?.layoutVersion||1):LAYOUT_VERSION});
      if(!level.routeValid)throw new Error("This route could not be validated.");this.world=level;this.world.playerPowers={...this.player.powers,...this.player.boosts};this.world.endingBoss=this.mode==="journey"||((this.depth+1)%3===0)?level.bossId:null;this.world.exitOpen=!this.world.endingBoss;
      if(checkpoint?.worldState)restoreWorldState(level,checkpoint.worldState,checkpoint.x);
      this.player.x=checkpoint?.x??level.spawn.x;this.player.y=checkpoint?.y??level.spawn.y;this.player.previousX=this.player.x;this.player.previousY=this.player.y;this.player.lastSafe={x:this.player.x,y:this.player.y};this.player.invulnerable=1.5;
      if(this.mode==="endless")beginEndlessSegment(this.endlessRun,this.player.x);
      this.runStats.totalSecrets=level.totalSecrets||0;this.camera.focusArena(null);this.camera.reset(this.player,level);
      await this.assets.loadLevel(level,p=>{if(generation===this.loadGeneration)this.loadingProgress(p);});await this.assets.ensure(UI_ART,{pin:true});for(const[power,value]of Object.entries(this.player.powers))if(value||power==="holyLight"||power==="shield"&&this.player.shieldUnlocked)await this.assets.ensurePlayerPower(power);
      if(generation!==this.loadGeneration)return;this.initialized=true;this.configureUIArt();this.setScreen("playing");if(!this.checkpoint)this.activateCheckpoint(level.checkpoints[0],false,null,!carryPlayer);this.hud.showBanner(node.name,checkpoint?"Checkpoint restored":REGIONS[nodeId].lesson,2.8);
      if(this.mode==="journey"){this.journeySave.currentNode=nodeId;this.storage.saveJourney(this.journeySave);}
    }catch(error){if(generation===this.loadGeneration)this.loadingFailure(error);}
  }
  startJourneyNode(nodeId,retry=false){return this.startLevel(nodeId,{checkpoint:retry?this.checkpoint:null});}
  resumeSaved(){const c=this.journeySave.checkpoint;if(!c)return;this.mode="journey";return this.startLevel(c.nodeId,{checkpoint:c});}
  startEndless(){this.mode="endless";this.depth=0;this.endlessSeed=randomSeed();this.endlessRun=createEndlessRun(this.storage.endlessBest(this.journeySave));this.score=0;this.runStats=newStats();this.checkpoint=null;return this.startLevel("countryside",{seed:this.endlessSeed});}
  async enterBoss(){if(this.screen!=="playing"||!this.world.endingBoss)return;const bossId=this.world.endingBoss;this.activateCheckpoint(this.world.checkpoints.at(-1),true,bossId);const generation=++this.loadGeneration;this.setScreen("loading");this.loadingTitle("The guardian trial");this.retryTask=()=>this.startLevel(this.journeyNode.id,{checkpoint:this.checkpoint,keepStats:true});try{await this.loadBossWorld(this.journeyNode.id,bossId,generation);}catch(error){if(generation===this.loadGeneration)this.loadingFailure(error);}}
  async loadBossWorld(nodeId,bossId,generation){
    const originalCheckpoint=this.checkpoint;const world={...menuWorld(),biome:nodeId,region:nodeId,checkpoints:[],enemyPool:[],bossId,loadBossNow:true,bossArena:true,endingBoss:bossId};this.bossController.start(bossId,Math.min(6,this.journeyNode.difficulty+(this.challenge?1:0)),world,this.player);
    world.bounds={left:0,right:world.width,top:-300,bottom:1000};world.deathY=1000;world.regionName=this.journeyNode.name;world.playerPowers={...this.player.powers,...this.player.boosts};world.hazardTypes=(world.hazards||[]).map(h=>h.type);world.weather=REGIONS[world.biome]?.weather||{disabled:true};this.world=world;this.player.previousX=this.player.x;this.player.previousY=this.player.y;this.camera.focusArena(this.bossController.arena);this.camera.reset(this.player,world);
    if(this.mode==="endless")beginEndlessSegment(this.endlessRun,this.player.x);
    await this.assets.loadLevel(world,p=>{if(generation===this.loadGeneration)this.loadingProgress(p);});for(const[power,value]of Object.entries(this.player.powers))if(value||power==="holyLight"||power==="shield"&&this.player.shieldUnlocked)await this.assets.ensurePlayerPower(power);if(generation!==this.loadGeneration)return;this.initialized=true;this.checkpoint=originalCheckpoint;this.setScreen("playing");this.hud.showBanner(this.bossController.boss.definition.name,"Read the warning. Strike during the opening.",3);
  }
  activateCheckpoint(c,boss=false,bossId=null,heal=true){if(!c)return;c.activated=true;if(heal)this.player.hearts=this.player.maxHearts;const checkpoint={nodeId:this.journeyNode.id,seed:this.world.seed??this.checkpoint?.seed??1,x:boss?160:c.x,y:boss?406:c.y,boss,bossId,challenge:this.challenge,player:this.playerSnapshot(),stats:clone(this.runStats),worldState:boss?this.checkpoint?.worldState??persistWorldState(this.world):persistWorldState(this.world)};this.checkpoint=checkpoint;
    if(this.mode==="journey"){this.journeySave.checkpoint=clone(checkpoint);this.storage.saveJourney(this.journeySave);}if(this.runStats.time>2)emitCombatEffect(this.world,"fx-checkpoint-activation",c.x+(c.w||48)/2,c.floorY??c.y+this.player.h,{duration:.8,scale:.35});if(this.runStats.time>2)this.hud.showBanner("Checkpoint lit","A safe place to return",1.5);
  }
  saveCurrentProgress(){if(this.mode!=="journey"||!this.checkpoint||["complete","worlds"].includes(this.screen))return;this.checkpoint.stats.time=this.runStats.time;this.checkpoint.stats.damage=this.runStats.damage;this.checkpoint.stats.deaths=this.runStats.deaths;this.journeySave.checkpoint=clone(this.checkpoint);this.storage.saveJourney(this.journeySave);}
  commitGateProgress(events){
    if(this.mode!=="journey"||!this.checkpoint||this.world.bossArena)return;
    const state=this.checkpoint.worldState;
    if(!state||state.seed!==this.world.seed||state.biome!==this.world.biome)return;
    const ids=new Set(events.filter(event=>event.type==="mechanism").map(event=>event.mechanismId));
    if(!ids.size)return;
    const activated=new Set(state.activated||[]),collected=new Set(state.collected||[]),dead=new Set(state.dead||[]);
    for(const mechanism of this.world.switches||[])if(ids.has(mechanism.id)&&mechanism.activated){
      activated.add(mechanism.id);
      if(mechanism.guardianId&&(this.world._state?.dead?.has(mechanism.guardianId)||(this.world.enemies||[]).some(enemy=>enemy.id===mechanism.guardianId&&enemy.dead)))dead.add(mechanism.guardianId);
    }
    for(const pickup of this.world.pickups||[])if(ids.has(pickup.unlockId)&&pickup.collected&&!collected.has(pickup.id)){
      collected.add(pickup.id);this.checkpoint.stats.collectibles++;
      if(pickup.secret)this.checkpoint.stats.secrets++;
    }
    state.activated=[...activated].sort();state.collected=[...collected].sort();state.dead=[...dead].sort();
    this.journeySave.checkpoint=clone(this.checkpoint);this.storage.saveJourney(this.journeySave);
  }
  pause(){if(this.screen!=="playing")return;this.saveCurrentProgress();this.setScreen("paused");}
  resume(){if(this.screen!=="paused")return;this.setScreen("playing");}
  restartLevel(){if(this.mode==="endless")return this.startEndless();const seed=this.world.seed??this.checkpoint?.seed;this.checkpoint=null;this.journeySave.checkpoint=null;this.storage.saveJourney(this.journeySave);return this.startLevel(this.journeyNode.id,{challenge:this.challenge,seed});}
  retryCheckpoint(){if(this.mode==="endless")return this.startEndless();const c=this.checkpoint||this.journeySave.checkpoint;if(!c)return this.restartLevel();const stats={...c.stats,time:this.runStats.time+5,damage:this.runStats.damage,deaths:this.runStats.deaths};this.runStats=stats;return this.startLevel(c.nodeId,{checkpoint:c,keepStats:true});}
  handleDeath(){if(this.screen!=="playing")return;this.runStats.deaths++;this.player.state="death";this.player.animTime=0;this.setScreen("dead");const stats=byId("endlessResults");stats.hidden=this.mode!=="endless";if(this.mode==="endless"){
    const run=this.endlessRun,distance=endlessMeters(run);this.score=Math.floor(run.distance);
    const result=this.storage.updateEndlessRecord(this.journeySave,{distance,regionsReached:run.regionsReached,enemiesDefeated:run.enemiesDefeated,collectibles:this.runStats.collectibles,time:this.runStats.time});
    byId("deathTitle").textContent="Endless run complete";byId("deathDetail").textContent=`${formatMeters(distance)} · ${formatTime(this.runStats.time)}`;
    for(const[id,value]of[["resultRegions",run.regionsReached],["resultEnemies",run.enemiesDefeated],["resultCollectibles",this.runStats.collectibles],["resultPreviousBest",formatMeters(result.previousBest)]])byId(id).textContent=value;
    byId("endlessPersonalBest").hidden=!result.newPersonalBest;byId("retryBtn").textContent="Run again";this.storage.saveScore("Traveler",this.score);
  }else{byId("deathTitle").textContent="Try again";byId("deathDetail").textContent="Your checkpoint is safe.";byId("retryBtn").textContent="Return to checkpoint";}this.saveCurrentProgress();}
  handleBossDefeated(){if(this.mode==="endless")this.endlessRun.enemiesDefeated++;this.world.exitOpen=true;this.completeLevel();}
  completeLevel(){if(this.screen!=="playing")return;
    if(this.mode==="endless"){this.depth++;this.endlessRun.regionsReached=this.depth+1;const carryPlayer=this.playerSnapshot();return this.startLevel(BIOME_ORDER[this.depth%6],{seed:(this.endlessSeed+this.depth*1291)>>>0,keepStats:true,carryPlayer});}
    const node=this.journeyNode,key=node.id+(this.challenge?":challenge":""),record=this.storage.updateRecord(this.journeySave,key,{...this.runStats,targetTime:node.targetTime});this.journeySave=completeJourneyNode(this.journeySave,node.id);const powers=new Set(this.journeySave.permanentPowers);powers.add(node.reward);this.journeySave.permanentPowers=[...powers];this.storage.saveJourney(this.journeySave);this.checkpoint=null;
    byId("completeKicker").textContent=node.index===5?"JOURNEY COMPLETE":"WORLD COMPLETE";byId("completeTitle").textContent=node.name;byId("completeStats").textContent=`${formatTime(this.runStats.time)} · ${this.runStats.secrets} secrets · Best ${formatTime(record.bestTime)}${this.runStats.damage===0?" · No hit":""}${this.runStats.time<=node.targetTime?" · Time target met":""}`;byId("completeReward").textContent=`${POWER_LABELS[node.reward]} unlocked`;byId("nextBtn").textContent=node.index===5?"Return to worlds":"Next world";this.setScreen("complete");
  }
  showControls(returnTo=this.screen){this.infoReturn=returnTo;this.setScreen("info");byId("infoTitle").textContent="Controls";const body=byId("infoBody");body.replaceChildren();const copy=document.createElement("p");copy.className="help-copy";copy.textContent="Move, jump, and dash. Hold jump for height; push into a wall to slide, then jump away. On touch screens use the left pad and the two right buttons. A single blessing button appears when ready. Hold Down, S, or drag the pad down to look below. Add jump to drop through a ledge. Water grants an extra air jump for 45 seconds. Wind sandals boost running and dashing for 30 seconds. Fire pickups give three casts on the same blessing button; light returns when they run out.";body.append(copy);const arrange=document.createElement('button');arrange.id='arrangeTouchBtn';arrange.className='primary large';arrange.textContent='Arrange touch controls';arrange.addEventListener('click',()=>this.touchLayout.open());body.append(arrange);if(this.touchLayoutNotice){const notice=document.createElement('p');notice.className='help-copy';notice.textContent=this.touchLayoutNotice;body.append(notice);}const list=document.createElement("div");list.className="key-list";for(const[action,label]of[["left","Move left"],["right","Move right"],["jump","Jump"],["dash","Dash"],["ability","Blessing"]]){const text=document.createElement("span"),button=document.createElement("button");text.textContent=label;const name=()=>this.input.bindings[action]===" "?"Space":this.input.bindings[action].toUpperCase();button.textContent=name();button.addEventListener("click",()=>{button.textContent="Press a key…";this.input.rebind(action,()=>this.showControls(returnTo));});list.append(text,button);}body.append(list);const notes=document.createElement("p");notes.className="help-copy";notes.textContent="Stomp enemies, attack from their weak side, and read their tells. Guardians become vulnerable after marked attacks. Light recharges; a cast outside range costs nothing. Checkpoints save your journey. Revisit worlds for secrets, faster times, and no-hit records. Challenge mode gives you three hearts.";body.append(notes);}
  async showRecords(){this.infoReturn="menu";this.setScreen("info");byId("infoTitle").textContent="Your records";const body=byId("infoBody");body.replaceChildren();for(const node of JOURNEY_NODES){const r=this.journeySave.records[node.id],row=document.createElement("div"),name=document.createElement("span"),detail=document.createElement("b");row.className="record-row";name.textContent=node.name;detail.textContent=r?`${formatTime(r.bestTime)}${r.noHit?" · No hit":""}`:"—";row.append(name,detail);body.append(row);}const row=document.createElement("div");row.className="record-row";row.textContent=`Endless best · ${formatMeters(this.storage.endlessBest(this.journeySave))}`;body.append(row);}
  update(dt){
    if(this.screen!=="playing")return;this.time+=dt;this.runStats.time+=dt;this.world.time=(this.world.time||0)+dt;this.world.playerX=this.player.x;
    if(!this.world.bossArena)updateWorldStreaming(this.world,this.player.x,this.camera.x);
    const beforeHearts=this.player.hearts,oldState=this.player.state;
    updateMovers(this.world,this.world.time,dt);updatePlayer(this.player,this.world,this.input,dt);updateWorldEnvironment(this.world,this.player,dt,this.world.time);
    if(this.mode==="endless")advanceEndlessRun(this.endlessRun,this.player.x,this.world);
    if(this.bossController.boss)this.bossController.update(this.world,this.player,dt,this.camera);else updateEnemies(this.world,this.player,dt);if(this.screen!=="playing")return;
    this.runStats.damage+=Math.max(0,beforeHearts-this.player.hearts);updateBlessings(this.player,this.world,dt);if(this.input.abilityPressed())useAbility(this.player,this.world,this.bossController);
    if(this.mode==="endless")recordEndlessDefeats(this.endlessRun,this.world);
    const gateEvents=updateGateProgress(this.world,this.player,dt);
    const pending=this.world.pickups.filter(p=>!p.collected);const collected=collectPickups(this.world,this.player);if(collected.length){this.runStats.collectibles+=collected.length;this.runStats.secrets+=pending.filter(p=>p.collected&&p.secret).length;for(const power of collected)if(PICKUP_MESSAGES[power])this.assets.ensurePlayerPower(power).catch(error=>this.recoverGraphics(error));}
    gateEvents.push(...updateGateProgress(this.world,this.player,0));if(gateEvents.length){this.commitGateProgress(gateEvents);const event=gateEvents.at(-1);this.hud.showBanner(event.title,event.detail,2.8);}
    if(collected.length){const type=collected.find(p=>['water','wind','fireRain'].includes(p))||collected.find(p=>PICKUP_MESSAGES[p]);if(type){const [title,detail]=PICKUP_MESSAGES[type];this.hud.showBanner(title,type==='water'&&this.player.powers.wings?'Another air jump alongside Wings · 45 seconds':detail.replace('Use blessing',matchMedia('(pointer:coarse)').matches?'Tap blessing':'Press '+this.input.bindings.ability.toUpperCase()),2.4);}}
    if(oldState!=="dash"&&this.player.state==="dash")emitCombatEffect(this.world,"fx-dash-trail",this.player.x+this.player.w/2,this.player.y+this.player.h,{duration:.25,scale:.35,facing:this.player.facing});
    if(this.player.y>(this.world.deathY||1000)){this.player.invulnerable=0;this.player.powers.shield=false;this.player.hearts=0;this.runStats.damage++;}
    if(this.player.hearts<=0){this.handleDeath();return;}
    if(!this.world.bossArena){if(this.lastPowerChunk!==this.world.streaming?.start){this.lastPowerChunk=this.world.streaming?.start;this.assets.prefetchPowers(this.world.pickups.filter(p=>!p.collected).map(p=>p.type)).catch(()=>{});}for(const c of this.world.checkpoints)if(!c.activated&&this.player.x>=c.x&&Math.abs(this.player.y-c.y)<150)this.activateCheckpoint(c);
      if(this.world.endingBoss&&!this.prefetchedBoss&&this.world.width-this.player.x<CHUNK_WIDTH*2){this.prefetchedBoss=true;this.assets.prefetchBoss(this.world.endingBoss).catch(()=>{this.prefetchedBoss=false;});}
      if(this.world.endingBoss&&this.player.x>=this.world.bossTriggerX){this.enterBoss();return;}if(!this.world.endingBoss&&this.player.x>=this.world.width-120){this.completeLevel();return;}
    }
    this.camera.update(this.player,dt,this.world,{lookDown:this.input.downHeld()});if(this.mode==="endless")this.score=Math.floor(this.endlessRun.distance);
  }
  recoverGraphics(error){if(this.screen!=="playing")return;const generation=++this.loadGeneration;this.setScreen("loading");this.loadingTitle("Preparing artwork");const ids=error.failedIDs||[...this.renderer.missingAssets];this.retryTask=async()=>{this.loadingTitle("Preparing artwork");try{await this.assets.ensure(ids);if(generation!==this.loadGeneration)return;this.setScreen("playing");}catch(e){if(generation===this.loadGeneration)this.loadingFailure(e);}};this.retryTask();}
  frame(now){const elapsed=Math.min(.08,Math.max(0,(now-this.lastFrame)/1000));this.lastFrame=now;if(elapsed>0)this.fps+=(1/elapsed-this.fps)*.05;
    if(!Object.values(this.input.bindings).includes("f1")&&this.input.consume("f1"))this.debug=!this.debug;if(this.input.consume("escape")||!Object.values(this.input.bindings).includes("p")&&this.input.consume("p")){if(this.screen==="playing")this.pause();else if(this.screen==="paused")this.resume();}
    if(this.screen==="playing"){this.accumulator=Math.min(this.accumulator+elapsed,PHYSICS.fixedStep*PHYSICS.maxSteps);let steps=0;while(this.accumulator>=PHYSICS.fixedStep&&steps++<PHYSICS.maxSteps&&this.screen==="playing"){this.update(PHYSICS.fixedStep);this.input.clearFrame();this.accumulator-=PHYSICS.fixedStep;}}
    if(this.screen==="dead")this.player.animTime=Math.min(2,(this.player.animTime||0)+elapsed);
    if(this.initialized&&this.screen!=="loading"){this.renderStats=this.renderer.draw(this,this.accumulator/PHYSICS.fixedStep);if(this.renderer.error&&this.screen==="playing")this.recoverGraphics(this.renderer.error);}
    this.hud.update(this,elapsed);if(this.runStats.time>8)byId("rotateHint").hidden=true;requestAnimationFrame(t=>this.frame(t));
  }
}
if(typeof window!=="undefined"&&typeof document!=="undefined")window.jesusRunGame=new JesusRunGame();
