import assert from 'node:assert/strict';
import {createEndlessRun,beginEndlessSegment,advanceEndlessRun,recordEndlessDefeats,endlessMeters,formatMeters} from '../js/world/endless-run.js';
import {GameStorage} from '../js/save/storage.js';
import {createJourneySave} from '../js/world/journey.js';
import {JesusRunGame} from '../js/game.js';
import {HUD} from '../js/render/hud.js';

const values=new Map();
globalThis.localStorage={getItem:key=>values.get(key)||null,setItem:(key,value)=>values.set(key,value)};
const storage=new GameStorage(),save=createJourneySave();
save.completed=['countryside','desert'];save.permanentPowers=['shield','sandals'];save.records.city={bestTime:400,runs:3};
values.set('jesusRunHighscores',JSON.stringify([{name:'Legacy',score:12450,createdAt:1}]));
assert.equal(storage.endlessBest(save),1245,'historical world-unit scores migrate to meters without erasing scores');
const run=createEndlessRun(storage.endlessBest(save));
beginEndlessSegment(run,80);assert.equal(endlessMeters(run),0,'spawn offset adds no distance');
advanceEndlessRun(run,480,{width:5000});assert.equal(endlessMeters(run),40);
for(const x of [100,450,80,480])advanceEndlessRun(run,x,{width:5000});
assert.equal(endlessMeters(run),40,'repeated backtracking cannot farm distance');
advanceEndlessRun(run,580,{width:5000});assert.equal(endlessMeters(run),50);
// Arena spawns and biome resets add no artificial untraveled tail or spawn gap.
beginEndlessSegment(run,160);assert.equal(endlessMeters(run),50);
advanceEndlessRun(run,460,{width:1500});assert.equal(endlessMeters(run),80);
for(let biome=0;biome<6;biome++){
  const before=endlessMeters(run);beginEndlessSegment(run,80);
  assert.equal(endlessMeters(run),before);
  advanceEndlessRun(run,1080,{width:2000});advanceEndlessRun(run,400,{width:2000});advanceEndlessRun(run,1080,{width:2000});
  assert.equal(endlessMeters(run),before+100,`biome ${biome} adds actual forward progress once`);
}
advanceEndlessRun(run,Infinity);advanceEndlessRun(run,NaN);assert.equal(endlessMeters(run),680);
const enemies=[{id:'guard-a',dead:true,hp:0},{id:'guard-b',dead:false,hp:1},{id:'fall',dead:true,hp:2}];
recordEndlessDefeats(run,{enemies});recordEndlessDefeats(run,{enemies});assert.equal(run.enemiesDefeated,1);
recordEndlessDefeats(run,{enemies:[{id:'guard-a',dead:true,hp:0}]});assert.equal(run.enemiesDefeated,1,'stream reload cannot award the same defeat twice');
beginEndlessSegment(run,80);recordEndlessDefeats(run,{enemies});assert.equal(run.enemiesDefeated,2,'the next segment has distinct enemies');
let result=storage.updateEndlessRecord(save,{distance:680,regionsReached:7,enemiesDefeated:2,collectibles:25,time:123});
assert.deepEqual(result,{previousBest:1245,bestDistance:1245,newPersonalBest:false});
result=storage.updateEndlessRecord(save,{distance:1245.5,regionsReached:8,enemiesDefeated:7,collectibles:30,time:200});
assert.deepEqual(result,{previousBest:1245,bestDistance:1245.5,newPersonalBest:true});
assert.equal(storage.endlessBest(storage.loadJourney()),1245.5);
assert.deepEqual(storage.loadJourney().completed,save.completed);assert.deepEqual(storage.loadJourney().permanentPowers,save.permanentPowers);
assert.deepEqual(storage.loadJourney().records.city,{bestTime:400,runs:3});assert.equal(storage.localScores()[0].score,12450);
assert.equal(formatMeters(1245.9),'1,245 m');

// Exercise the actual game lifecycle across all biomes and a guardian arena.
const elements=new Map();
const element=id=>{if(!elements.has(id))elements.set(id,{hidden:false,textContent:'',style:{},dataset:{},classList:{toggle(){},add(){},remove(){}},setAttribute(){},replaceChildren(){},append(){}});return elements.get(id);};
globalThis.document={getElementById:element};globalThis.window={};globalThis.Image=class{};
globalThis.requestAnimationFrame=fn=>fn(0);
const game=Object.create(JesusRunGame.prototype);
Object.assign(game,{storage,journeySave:save,loadGeneration:0,assets:{loadLevel:async()=>{},ensure:async()=>{},ensurePlayerPower:async()=>{}},
  camera:{focusArena(){},reset(){}},hud:{showBanner(){}},bossController:{boss:null,arena:null,start(kind,difficulty,world,player){this.boss={definition:{name:'Guardian'},health:10,maxHealth:10};Object.assign(world,{width:1500,platforms:[],pickups:[],hazards:[],enemies:[],effects:[],bossArena:true});player.x=160;player.y=406;}},
  setScreen(screen){this.screen=screen;},loadingTitle(){},configureUIArt(){}});
await game.startEndless();assert.equal(game.screen,'playing');assert.equal(endlessMeters(game.endlessRun),0);
let distance=0;
for(let depth=0;depth<6;depth++){
  assert.equal(game.world.biome,['countryside','desert','city','mountains','ruins','storm'][depth]);
  const previous=endlessMeters(game.endlessRun);assert.equal(previous,distance);
  advanceEndlessRun(game.endlessRun,game.player.x+1000,game.world);distance+=100;
  if(depth===2){
    const beforeArena=endlessMeters(game.endlessRun);await game.enterBoss();
    assert.equal(endlessMeters(game.endlessRun),beforeArena,'enterBoss never credits the unvisited region tail');
    advanceEndlessRun(game.endlessRun,460,game.world);distance+=30;
  }
  await game.completeLevel();assert.equal(endlessMeters(game.endlessRun),distance,'completeLevel keeps cumulative distance');
}
assert.equal(game.endlessRun.regionsReached,7);assert.equal(distance,630);
game.runStats.collectibles=33;game.endlessRun.enemiesDefeated=9;game.handleDeath();
assert.equal(element('deathDetail').textContent.startsWith('630 m'),true);
assert.equal(element('resultRegions').textContent,7);assert.equal(element('resultEnemies').textContent,9);assert.equal(element('resultCollectibles').textContent,33);
assert.equal(element('resultPreviousBest').textContent,'1,245 m');assert.equal(element('endlessPersonalBest').hidden,true);
const hud=new HUD({getURL:()=>''});hud.update(game,.1);assert.equal(element('endlessMeters').textContent,'630 m');assert.equal(element('endlessDistance').hidden,false);
game.mode='journey';hud.update(game,.1);assert.equal(element('endlessDistance').hidden,true);
await game.startEndless();assert.equal(endlessMeters(game.endlessRun),0,'retry starts at zero with records preserved');
advanceEndlessRun(game.endlessRun,80+20000,{width:30000});game.handleDeath();assert.equal(element('endlessPersonalBest').hidden,false);
assert.equal(storage.endlessBest(storage.loadJourney()),2000);
const normalLoad=game.assets.loadLevel;
game.assets.loadLevel=async()=>{throw new Error('Simulated artwork failure');};
await game.startEndless();assert.equal(game.screen,'loading');assert.equal(endlessMeters(game.endlessRun),0);assert.equal(game.depth,0);
game.assets.loadLevel=normalLoad;await game.retryTask();assert.equal(game.screen,'playing');assert.equal(endlessMeters(game.endlessRun),0,'retrying a failed initial load credits no spawn movement');
game.runStats.collectibles=11;advanceEndlessRun(game.endlessRun,1080,game.world);
game.assets.loadLevel=async()=>{throw new Error('Simulated biome artwork failure');};
await game.completeLevel();assert.equal(game.screen,'loading');assert.equal(game.depth,1);assert.equal(endlessMeters(game.endlessRun),100);
game.assets.loadLevel=normalLoad;await game.retryTask();assert.equal(game.depth,1,'retrying the same biome never increments region depth twice');
assert.equal(endlessMeters(game.endlessRun),100);assert.equal(game.runStats.collectibles,11);assert.equal(game.endlessRun.regionsReached,2);
game.world.endingBoss='warBeast';game.assets.loadLevel=async()=>{throw new Error('Simulated guardian artwork failure');};
await game.enterBoss();assert.equal(game.screen,'loading');assert.equal(endlessMeters(game.endlessRun),100);
game.assets.loadLevel=normalLoad;await game.retryTask();assert.equal(game.screen,'playing');assert.equal(game.world.bossArena,true);
assert.equal(endlessMeters(game.endlessRun),100,'failed arena loads and checkpoint retries never add region width or spawn offset');
game.input={cancelRebind(){}};game.showMainMenu();assert.equal(game.mode,null);assert.equal(game.screen,'menu');
await game.startEndless();assert.equal(endlessMeters(game.endlessRun),0);assert.equal(game.runStats.collectibles,0);assert.equal(storage.endlessBest(storage.loadJourney()),2000,'backing out and starting again retains the best completed run');
console.log('Endless live distance, all biome/arena transitions, defeats, HUD/results, personal best migration, preserved saves and failed-load/retry/backout lifecycle passed.');
