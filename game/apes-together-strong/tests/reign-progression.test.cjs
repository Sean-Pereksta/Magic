'use strict';
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),vm=require('node:vm');
const {loadEngine}=require('./performance-harness.cjs');
function setup(){const c=loadEngine();if(!c.ATSReignTiers)vm.runInContext(fs.readFileSync(path.join(__dirname,'../progression.js'),'utf8'),c);return {c,g:new c.ATSGame('REIGN-TEST')}}
function population(g,n){while(g.apes.length<n)g.makeApe(50,50,'follow');g.apes.forEach((a,i)=>a.hp=i<n?a.maxHp:0);g.checkProgression()}
test('a rescue reaching the threshold earns its crown even if an ape dies in the same tick',()=>{const {g}=setup();for(let i=0;i<100;i++)g.makeApe(50,50,'follow');g.apes[0].hp=0;assert.equal(g.population,99);assert.equal(g.nextCoronation(),1);g.checkProgression();assert.equal(g.king.crownTier,1);g.ended=true;assert.equal(g.nextCoronation(),0,'death never stalls on a ceremony')});
test('100 and 200 living apes trigger ordered, one-time ceremonies and permanent crowns',()=>{
 const {g}=setup();population(g,99);assert.equal(g.nextCoronation(),0);assert.equal(g.king.crownTier,0);
 population(g,100);assert.equal(g.nextCoronation(),1);assert.equal(g.king.crownTier,1);assert.equal(g.acknowledgeCoronation(2),false);assert.equal(g.acknowledgeCoronation(1),true);assert.equal(g.nextCoronation(),0);
 population(g,199);assert.equal(g.nextCoronation(),0);population(g,200);assert.equal(g.nextCoronation(),2);g.acknowledgeCoronation(2);
 population(g,60);assert.equal(g.progression.tier,2);assert.equal(g.king.crownTier,2);assert.equal(g.nextCoronation(),0);population(g,200);assert.equal(g.nextCoronation(),0);
});
test('jumping both thresholds queues both ceremonies and save/load preserves pending acknowledgment',()=>{
 const {g,c}=setup();population(g,205);assert.equal(g.nextCoronation(),1);const d=JSON.parse(JSON.stringify(g.serialize()));let loaded=c.ATSGame.fromJSON(d);assert.equal(loaded.nextCoronation(),1);loaded.acknowledgeCoronation(1);assert.equal(loaded.nextCoronation(),2);loaded.acknowledgeCoronation(2);loaded=c.ATSGame.fromJSON(JSON.parse(JSON.stringify(loaded.serialize())));assert.equal(loaded.nextCoronation(),0);assert.equal(loaded.king.crownTier,2);
 delete d.progression;loaded=c.ATSGame.fromJSON(d);assert.equal(loaded.nextCoronation(),1);assert.equal(loaded.population,205);
});
test('reign counts residents across villages, while invasions retain finite reserves',()=>{
 const {g}=setup();population(g,100);g.apes.forEach(a=>a.state='settled');assert.equal(g.followers.length,0);g.checkProgression();assert.equal(g.progression.tier,1);
 const site={strength:11,tier:1,vehicleInventory:{},x:0,y:0};g.world.militaryCapacity=()=>({vehicleInventory:{}});const royal=g.responsePackage(site,g.king,true);assert.ok(royal.people<=11);const royalInterval=g.reinforcementInterval;g._progression.tier=0;assert.ok(royalInterval<g.reinforcementInterval);
});
test('old 200-resident saves unlock Warlord and never replay acknowledged ceremonies',()=>{
 const {g,c}=setup();population(g,200);g.apes.forEach(a=>a.state='settled');
 const data=JSON.parse(JSON.stringify(g.serialize()));data.progression={version:1,tier:1,acknowledged:1,peak:200};
 let loaded=c.ATSGame.fromJSON(data);assert.equal(loaded.progression.tier,2);assert.equal(loaded.nextCoronation(),2);assert.equal(loaded.king.crownTier,2);
 loaded.acknowledgeCoronation(2);loaded=c.ATSGame.fromJSON(JSON.parse(JSON.stringify(loaded.serialize())));assert.equal(loaded.nextCoronation(),0);
 for(const a of loaded.apes.slice(50))a.hp=0;loaded.checkProgression();assert.equal(loaded.progression.tier,2);assert.equal(loaded.nextCoronation(),0);
});
test('track switch reuses a single audio player, skips reloads and follows all three tiers',async()=>{
 const sources=['underpowered-king','ceremonial-tom','primal-roar'];let plays=0,loads=0;
 const music={paused:true,dataset:{},load(){loads++},play(){plays++;this.paused=false;return Promise.resolve()},pause(){this.paused=true}};
 const c=vm.createContext({Math,Promise,Set,console,document:{hidden:false,getElementById(id){return id==='campaignMusic'?music:{textContent:'data:audio/mpeg;base64,'+id}}}});c.window=c;vm.runInContext(fs.readFileSync(path.join(__dirname,'../audio.js'),'utf8'),c);const audio=new c.ATSAudio();audio.ctx={state:'running',currentTime:0};audio.pause(true);
 for(let tier=0;tier<3;tier++){audio.setReignTier(tier);assert.equal(music.dataset.track,sources[tier]);assert.equal(music.paused,true);audio.setReignTier(tier);assert.equal(loads,tier+1)}
 audio.pause(false);await new Promise(r=>setImmediate(r));assert.equal(plays,1);assert.equal(music.loop,true);audio.pause(true);assert.equal(music.paused,true);
});
