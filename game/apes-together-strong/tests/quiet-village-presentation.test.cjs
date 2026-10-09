'use strict';
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),vm=require('node:vm');
function renderer(){
 const calls=[],gradient={addColorStop(){}};
 const context=()=>new Proxy({globalAlpha:1,createLinearGradient:()=>gradient,createRadialGradient:()=>gradient},{get(o,k){if(k in o)return o[k];return(...args)=>{for(const a of args)if(typeof a==='number')assert.ok(Number.isFinite(a),'finite '+String(k));calls.push([k,...args])}},set(o,k,v){o[k]=v;return true}});
 const canvas=()=>({width:0,height:0,getBoundingClientRect:()=>({width:900,height:600}),getContext:()=>context()});
 const c=vm.createContext({console,Math,Map,Set,devicePixelRatio:1,innerWidth:900,innerHeight:600,document:{createElement:canvas}});c.window=c;
 for(const name of ['render','render-details'])vm.runInContext(fs.readFileSync(path.join(__dirname,'..',name+'.js'),'utf8'),c,{filename:name+'.js'});
 c.ATSUtil={hash:()=>0};const r=new c.ATSRenderer(canvas());r.time=10;r.quality='high';r.actorSpriteBudget=1000;calls.length=0;return{r,calls};
}
const textCalls=calls=>calls.filter(c=>c[0]==='fillText'||c[0]==='strokeText');
const ape=fields=>({id:'ape-art',species:'orangutan',coatVariant:1,x:0,y:0,dir:0,phase:0,bodyScale:1.1,hp:80,maxHp:100,age:240,...fields});

test('ordinary ground has no procedural grass strokes while farms, reeds and water retain details',()=>{
 for(const biome of ['forest','farmland','wetland','water']){
  const {r,calls}=renderer(),world={terrain:()=>({biome,water:biome==='water'}),getSites:()=>[]};
  r.groundBuildBudget=0;r.drawGround(world,{x:0,y:0},220);
  const strokes=calls.filter(c=>c[0]==='stroke').length;
  if(biome==='forest'){assert.equal(strokes,0,'both ground layers omit decorative line grass');assert.ok(calls.some(c=>c[0]==='fill'),'underlying terrain color remains')}
  else assert.ok(strokes>0,biome+' retains meaningful ground details');
 }
});

test('world status, work, signs and bonus effects remain wordless while their geometry survives',()=>{
 const {r,calls}=renderer();
 r.drawObject(r.ctx,{id:'captives',type:'cage',count:3,rescueOpened:true,w:62,h:50});
 for(const type of ['gate','wall','house','depot','barracks'])r.drawObject(r.ctx,{id:type,type,x:0,y:0,w:80,h:18,height:70,wallTier:3,hp:90,maxHp:100,commandCenter:type==='house'});
 for(const state of ['combat','radio','patrol'])r.drawHuman(r.ctx,{id:state,x:0,y:0,hp:70,maxHp:100,state,suspicion:.8,role:'engineer',kind:'rifle',constructing:{remaining:1},engineerJob:{x:10,y:5}});
 r.drawHumanSprite(r.ctx,{id:'cached-human',x:0,y:0,hp:70,maxHp:100,state:'combat',suspicion:1,role:'leader',kind:'rifle'});
 for(const kind of ['tank','apc','ifv','truck'])r.drawVehicle(r.ctx,{kind,x:0,y:0,hp:200,maxHp:600,mobilityDamage:100,weaponDamage:100,engineDamage:90,overrun:true,dismounting:true});
 r.drawThreats({time:10,vehicles:[{kind:'tank',x:0,y:0,hp:100,cannonTarget:{x:50,y:30,start:9,duration:1.8}}],humans:[{id:'leader',x:0,y:0,hp:80,role:'leader',squadId:'squad',squadOrder:'Hold/Suppress',cohesionLossUntil:20}],forces:{hazards:[{type:'grenade',x:30,y:30,fromX:0,fromY:0,start:9,fuse:2,radius:90},{type:'mortar',x:20,y:20,start:9,fuse:2,radius:100},{type:'airstrike',x:30,y:30,start:9,fuse:2,radius:140}]}});
 for(const kind of ['hut','lodge','barrier','spearTower','training'])r.drawConstruction(r.ctx,{kind,stage:2,progress:.5,treeIds:['tree']});
 r.drawSettlement(r.ctx,{id:'peaceful',name:'Peaceful Grove',x:0,y:0,level:5,radius:200,known:true,attack:false,defense:40,maxDefense:100});
 r.drawSettlementGuides({king:{x:0,y:0},settlements:[{name:'Peaceful Grove',x:100,y:50,attack:false}]});
 r.drawEffects(r.ctx,['+food','COUNTERATTACK','MASS HORDE CONTACT','HOLD / SUPPRESS'].map(text=>({type:'text',text,x:0,y:0,life:.5,maxLife:1})));
 assert.deepEqual(textCalls(calls),[]);
 assert.ok(calls.some(c=>c[0]==='arc'),'awareness and impact icons remain visible');
 assert.ok(calls.some(c=>c[0]==='ellipse'&&c[3]>100),'explosive danger circles remain visible');
 assert.ok(calls.some(c=>c[0]==='roundRect'),'health and construction bars remain visible');
});

test('only attacked village names and UNDER ATTACK appear in the world; maps retain useful labels',()=>{
 const {r,calls}=renderer(),settlements=[{id:'quiet',name:'Quiet Grove',x:0,y:0,level:3,radius:140,attack:false},{id:'attacked',name:'Ember Grove',x:160,y:40,level:4,radius:200,attack:true}];
 for(const village of settlements)r.drawSettlement(r.ctx,village);
 r.drawSettlementGuides({king:{x:0,y:0},settlements});
 const labels=textCalls(calls).map(c=>c[1]);assert.ok(labels.includes('Ember Grove'));assert.ok(labels.includes('UNDER ATTACK'));assert.ok(labels.every(t=>t==='Ember Grove'||t==='UNDER ATTACK'));
 calls.length=0;r.drawMap({king:{x:0,y:0},settlements,world:{discovered:new Set(),terrain:()=>({biome:'forest'}),getSites:()=>[{name:'Human fortress',x:30,y:20,known:true}]}},{width:0,height:0,getBoundingClientRect:()=>({width:640,height:420}),getContext:()=>r.ctx});
 assert.ok(textCalls(calls).some(c=>c[1]==='Quiet Grove'));assert.ok(textCalls(calls).some(c=>c[1]==='Human fortress'));
});

test('commissioned tower and training construction have distinct physical stages and completed art',()=>{
 const {r,calls}=renderer(),stages={};
 for(const kind of ['spearTower','training']){
  stages[kind]=[];
  for(let stage=0;stage<4;stage++){calls.length=0;r.drawConstruction(r.ctx,{kind,stage,progress:stage/4});stages[kind].push(JSON.stringify(calls));assert.deepEqual(textCalls(calls),[])}
  assert.equal(new Set(stages[kind]).size,4,'each stage adds visible construction');
  calls.length=0;r.drawSettlementProp(r.ctx,{id:kind,kind,stage:4,progress:1,hp:100,maxHp:220});assert.ok(r.villageSprites.has(kind+':0:false'));assert.ok(calls.some(c=>c[0]==='drawImage'));
  r.drawSettlementProp(r.ctx,{id:kind,kind,stage:4,hp:0,maxHp:220});assert.ok(r.villageSprites.has(kind+':0:true'),'destroyed commissioned facility has separate rubble art');
 }
 assert.notEqual(stages.spearTower[3],stages.training[3]);
});

test('traveling spears follow recorded world positions and platform height with a bounded draw count',()=>{
 const {r,calls}=renderer(),spear={id:'spear',x:120,y:0,prevX:114,prevY:0,fromX:0,fromY:0,targetX:240,targetY:0,speed:360,duration:2/3,elapsed:1/3,life:1,fromZ:90,targetZ:20};
 r.drawSpears({time:10,settlements:[{spears:[spear]}]});
 const p=r.project(120,0,77);assert.ok(calls.some(c=>c[0]==='translate'&&c[1]===p.x&&c[2]===p.y));assert.ok(calls.some(c=>c[0]==='lineTo'&&c[1]===-3&&c[2]===0));assert.deepEqual(textCalls(calls),[]);
 calls.length=0;r.drawSpears({time:10,settlements:[{spears:[{...spear,x:150,elapsed:.45}]}]});assert.ok(!calls.some(c=>c[0]==='translate'&&c[1]===p.x&&c[2]===p.y),'the shaft moves with the combat record');
 calls.length=0;r.drawSpears({time:10,settlements:[{spears:Array.from({length:150},(_,i)=>({...spear,id:i}))}]});assert.equal(calls.filter(c=>c[0]==='translate').length,128);
 calls.length=0;r.drawSpears({time:10,settlements:[{spears:[{...spear,x:50000,y:50000,prevX:49999,prevY:50000,fromX:49950,fromY:50000,targetX:50100,targetY:50000}]}]});assert.equal(calls.length,0,'offscreen spears are culled');
});

test('wall tiers, height and damage change geometry within the existing bounded cache',()=>{
 const {r,calls}=renderer(),wall={w:140,h:18,hp:100,maxHp:100};
 for(let tier=1;tier<=4;tier++){calls.length=0;r.drawWall(r.ctx,{...wall,wallTier:tier,visualHeight:tier*26});assert.ok(Array.from(r.wallSprites.keys()).some(k=>k.includes(':'+tier*26+':'+tier+':')));assert.deepEqual(textCalls(calls),[])}
 const intact=r.wallSprites.size;r.drawWall(r.ctx,{...wall,wallTier:4,visualHeight:104,hp:20});assert.equal(r.wallSprites.size,intact+1,'a breach uses different cached geometry');
 for(let i=0;i<40;i++)r.drawWall(r.ctx,{...wall,w:70+i,wallTier:i%4+1});assert.equal(r.wallSprites.size,24);
 const before=r.wallSprites.size;r.drawWall(r.ctx,{...wall,w:5000,h:1000,wallTier:4});assert.equal(r.wallSprites.size,before,'huge fortress faces cannot allocate oversized cached canvases');
});

test('wall climbs and crest occupancy preserve species art, body scale and uncached reaction paths',()=>{
 const {r,calls}=renderer();r.drawApe(r.ctx,ape({climbingWallId:'wall',wallClimbUntil:11,wallClimbHeight:76}),false);
 assert.ok(calls.some(c=>c[0]==='translate'&&c[1]===0&&c[2]===-76));assert.ok(calls.some(c=>c[0]==='scale'&&c[1]===1.1&&c[2]===1.1));assert.deepEqual(textCalls(calls),[]);
 calls.length=0;r.drawApe(r.ctx,ape({onWallId:'wall',wallClimbUntil:9,wallClimbHeight:76}),false);assert.ok(calls.some(c=>c[0]==='translate'&&c[2]===-76),'standing on the crest keeps the body elevated');
 let cached=0;const live=[];r.drawApe=(c,a,king)=>{if(!king)live.push(a.id)};r.drawApeSprite=()=>cached++;
 const apes=Array.from({length:80},(_,i)=>ape({id:'ape-'+i}));Object.assign(apes[78],{climbingWallId:'wall',wallClimbUntil:11,wallClimbHeight:35});Object.assign(apes[79],{onWallId:'wall',wallClimbHeight:35});
 r.draw({time:10,king:{id:'king',x:0,y:0,hp:160,maxHp:160},apes,humans:[],vehicles:[],helis:[],settlements:[],world:{objects:new Map([['wall',{x:0,y:0}]]),terrain:()=>({biome:'forest'}),getObjects:()=>[],getSites:()=>[]},forces:{hazards:[]}},0);
 assert.equal(cached,78);assert.deepEqual(live,['ape-78','ape-79']);
});
