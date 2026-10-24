const fs=require('fs'),vm=require('vm'),assert=require('assert');
const path=require('path');
const filename=process.argv[2]||path.join(__dirname,'..','game','onehitknight.html');
const storage=new Map(),elements=new Map(),events=[];
const drawing=new Proxy({},{get:(_,key)=>key==='measureText'?()=>({width:20}):()=>{}});
function element(id){if(!elements.has(id))elements.set(id,{id,style:{},dataset:{},classList:{toggle(){},add(){},remove(){}},innerHTML:'',textContent:'',disabled:false,value:'KNIGHT',children:[],onclick:null,getContext:()=>drawing,addEventListener(){},appendChild(e){this.children.push(e);},getBoundingClientRect:()=>({left:0,top:0,width:132,height:132}),setPointerCapture(){},releasePointerCapture(){},focus(){},select(){}});return elements.get(id);}
let time=0;
const browser={console,Math,Date,Set,Map,URLSearchParams,Number,String,JSON,Array,Object,Promise,parseInt,parseFloat,devicePixelRatio:1,innerWidth:960,innerHeight:540,location:{search:'?test'},performance:{now:()=>time},navigator:{maxTouchPoints:0},matchMedia:()=>({matches:false}),requestAnimationFrame(){},addEventListener(){},setTimeout,clearTimeout,CustomEvent:class{constructor(type,options){this.type=type;this.detail=options?.detail;}},dispatchEvent(e){events.push(e);},localStorage:{getItem:k=>storage.get(k)||null,setItem:(k,v)=>storage.set(k,String(v))},document:{getElementById:element,createElement:t=>({...element('auto'+Math.random()),tagName:t}),body:element('body'),documentElement:{},fullscreenElement:null}};
browser.window=browser;browser.globalThis=browser;
vm.createContext(browser);
const html=fs.readFileSync(filename,'utf8');
const scripts=[...html.matchAll(/<script>([\s\S]*?)<\/script>/g)];
let source=scripts.find(s=>s[1].includes('const ENEMY_PROGRESSION='))?.[1];
assert(source,'Main game script must be present');assert(source.includes('ARCADE_COMBAT_V2'),'Arcade combat integration is required');
// Isolate gameplay from image decoding, canvas/UI layout, and external services. The browser
// integration suite tests those separately; these assertions run offline without dependencies.
const renderStart=source.indexOf('// One Hit Knight image-atlas runtime.');
if(renderStart>=0){const renderEnd=source.indexOf('function loop(ts)',renderStart);assert(renderEnd>renderStart);source=source.slice(0,renderStart)+"function draw(){}\nfunction reset(m){return resetArcadeCore(m);}\n"+source.slice(renderEnd);}
const uiStart=source.indexOf('// Cabinet interface, gesture-unlocked audio,');
if(uiStart>=0){const uiEnd=source.indexOf('initOnline(); restoreMenu(); updateHud();',uiStart);assert(uiEnd>uiStart);source=source.slice(0,uiStart)+"let controllerVec={x:0,y:0};function escapeHtml(s){return String(s);}\n"+source.slice(uiEnd);}
vm.runInContext(source,browser,{timeout:5000});
const api=browser.OHKTest;assert(api,'Test API must be opt-in via ?test');
const tick=(n,dt=.01)=>{for(let i=0;i<n;i++){time+=dt*1000;api.update(dt);}};
function quiet(){api.state.enemies.length=0;api.state.stats.waveTarget=999;api.state.arcade.props=[];api.state.arcade.bossWave=true;}
function target(type='goblin',dx=24,dy=0){api.spawnEnemy(type);const e=api.state.enemies.at(-1);Object.assign(e,{x:api.state.player.x+dx,y:api.state.player.y+dy,cooldown:999,spd:0,stun:1,state:'move'});return e;}
const reports=[];
for(const id of Object.keys(api.WEAPONS)){
 api.reset('CLASSIC');quiet();api.state.player.weapon=id;const e=target();api.doSlash();const first=api.state.player.attack;assert.equal(first.phase,'anticipate');assert.equal(e.dead,false,'input must never apply immediate hit');api.doSlash();assert.strictEqual(api.state.player.attack,first,'attack cannot reset itself');tick(1,.005);assert.equal(e.dead,false,'anticipation must be harmless');tick(80,.005);assert.equal(e.dead,true,id+' active sprite must land its hit');assert.equal(api.state.stats.kills,1);assert.equal(api.state.player.attack,null,id+' must recover to idle');reports.push('weapon '+id);
}
for(let dir=0;dir<4;dir++){
 api.reset('CLASSIC');quiet();api.state.player.faceDir=dir;const angle=dir*Math.PI/2,e=target('goblin',Math.cos(angle)*24,Math.sin(angle)*24);api.doSlash();tick(40);assert.equal(e.dead,true,'direction '+dir+' slash must match facing');
}
reports.push('all four movement-based attack directions');
api.reset('CLASSIC');quiet();const behind=target('goblin',-25,0);api.doSlash();tick(40);assert.equal(behind.dead,false,'four-direction swing must not hit behind knight');
api.reset('CLASSIC');quiet();const perfect=target();Object.assign(perfect,{state:'windup',windup:.5,stateTimer:.09});api.doSlash();tick(18,.005);assert.equal(perfect.dead,true);assert.equal(api.state.stats.perfectStrikes,1);assert(api.state.arcade.slowmo>0);reports.push('animation hit window, direction, late-windup Perfect Strike');
api.reset('CLASSIC');quiet();api.spawnBoss();const boss=api.state.enemies.at(-1);Object.assign(boss,{x:api.state.player.x+24,y:api.state.player.y,state:'move',spd:0,cooldown:999,stun:5,entrance:0});assert.equal(api.canStrikeEnemy(boss),false);api.doSlash();tick(40);assert.equal(boss.dead,false,'armored boss must survive early sword');boss.state='recover';boss.stateTimer=1;assert.equal(api.canStrikeEnemy(boss),true);api.doSlash();tick(12);assert.equal(boss.dead,true,'one well-timed strike defeats exposed boss');assert.equal(api.state.stats.bosses,1);reports.push('boss armor and readable one-hit opening');
api.reset('CLASSIC');quiet();api.state.upgrades.shield=1;api.die();assert.equal(api.state.gameOver,false);assert.equal(api.state.upgrades.shield,0);assert(api.state.player.blockTimer>0);api.reset('60 SECOND');quiet();api.state.upgrades.shield=5;api.state.stats.minuteLeft=.001;api.update(.01);assert.equal(api.state.gameOver,true,'clock expiration must not consume shields indefinitely');assert(api.state.stats.achievements.includes('mayhem-complete'));api.reset('CLASSIC');reports.push('shield and timed mode completion');
quiet();api.collectPower({kind:'knight-wings',x:200,y:100});api.collectPower({kind:'big',x:200,y:100});const speed=api.state.upgrades.speed,slash=api.state.upgrades.slash;api.showChoices();tick(100);assert.equal(api.state.player.buffs.wings,8,'upgrade pause freezes buffs');assert.equal(api.state.player.buffs.giant,8);assert.equal(api.state.upgrades.speed,speed);assert.equal(api.state.upgrades.slash,slash);assert.equal(api.state.choices.length,23);assert(api.state.choices.every(c=>c.rarity&&c.item));reports.push('upgrade cadence, rarity metadata, paused effect durations');
api.reset('CLASSIC');quiet();let stale=0;api.state.arcade.tasks.push({fn:()=>stale++,left:.01,token:api.state.arcade.runToken});api.reset('CLASSIC');quiet();tick(8);assert.equal(stale,0,'restart must clear delayed gameplay');api.collectPower({kind:'haste',x:200,y:100});api.reset('CLASSIC');quiet();tick(10);assert.equal(api.state.upgrades.speed,1);assert.equal(api.state.player.buffs.haste,undefined);reports.push('restart clears tasks and temporary stat changes');
api.reset('CLASSIC');quiet();for(const id of ['quick-boots','royal-score','flame-sword','shadow-knight'])api.state.choices.find(c=>c.item===id)[2]();assert.equal(api.state.player.equipment.boots,'gold');assert.equal(api.state.player.equipment.helmet,'crown');assert.equal(api.state.player.equipment.weapon,'flame');assert.equal(api.state.player.weapon,'flame');assert.equal(api.state.player.equipment.armor,'shadow');assert.equal(api.state.player.equipment.cape,'shadow');reports.push('equipment upgrades preserve independent visible layers');
api.reset('CLASSIC');quiet();for(let n=0;n<30;n++){target();api.doSlash();tick(30);}assert.equal(api.state.stats.combo,30);assert.equal(api.state.stats.bestCombo,30);assert(api.state.player.frenzy>0);assert(api.state.stats.highestMultiplier>=2);assert(api.state.stats.achievements.includes('frenzy'));reports.push('30-kill combo milestones and Knight Frenzy');
const added=api.PICKUP_POOL.filter(p=>p[0].includes('-'));assert.equal(added.length,20);for(const [kind]of api.PICKUP_POOL){api.reset('CLASSIC');quiet();api.collectPower({kind,x:200,y:100});assert.equal(api.state.stats.pickups,1);}reports.push('all 30 pickup handlers including 20 weighted additions');
for(const name of api.ARCADE_EVENTS){api.reset('CLASSIC');quiet();api.startArcadeEvent(name);assert.equal(api.state.arcade.event,name);api.update(.25);assert(api.state.arcade.eventLeft<15);}reports.push('all seven arcade events');
for(const mode of ['CLASSIC','CHAOS','60 SECOND']){api.reset(mode);api.update(.016);assert.equal(api.state.mode,mode);assert.equal(api.state.running,true);}reports.push('all three original modes');
const rosterMatch=source.match(/const ENEMY_PROGRESSION=(\[[^;]+\])/);assert(rosterMatch,'Original progression is retained');const roster=vm.runInNewContext(rosterMatch[1]);assert.equal(roster.length,34);assert.equal(new Set(roster).size,34);api.reset('CLASSIC');quiet();for(const type of roster){api.spawnEnemy(type);const e=api.state.enemies.at(-1);assert.equal(e.type,type);assert(e.windup>0&&e.attack>0&&e.recover>0,type+' retains its attack state machine');}
const bossNames=['ogre','warlock','behemoth','wyrm','king','necroking','titan'];for(let i=0;i<bossNames.length;i++){api.state.stats.bosses=i;api.spawnBoss();assert.equal(api.state.enemies.at(-1).type,bossNames[i]);}reports.push('all 34 original enemy state machines and seven boss identities');
assert(html.includes("collection('highscores')"));assert(html.includes('firebase.firestore.FieldValue.serverTimestamp()'));reports.push('Firestore score persistence source retained; adapter behavior tested in browser');
for(const id of api.state.arcade.realTimers)clearTimeout(id);
console.log(JSON.stringify({passed:reports.length,checks:reports,events:events.length},null,2));
