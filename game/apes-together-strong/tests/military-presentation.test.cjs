'use strict';
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),vm=require('node:vm');
function renderer(){
 const calls=[];
 const gradient={addColorStop(){}};
 const context=()=>new Proxy({globalAlpha:1,createLinearGradient:()=>gradient,createRadialGradient:()=>gradient},{get(o,k){if(k in o)return o[k];return(...args)=>{for(const a of args)if(typeof a==='number')assert.ok(Number.isFinite(a),'finite '+String(k)+' coordinate');calls.push([k,...args])}},set(o,k,v){o[k]=v;return true}});
 const canvas=()=>({width:0,height:0,getBoundingClientRect:()=>({width:900,height:600}),getContext:()=>context()});
 const c=vm.createContext({console,Math,Map,Set,devicePixelRatio:1,innerWidth:900,innerHeight:600,document:{createElement:canvas}});c.window=c;
 for(const name of ['render','render-details','audio'])vm.runInContext(fs.readFileSync(path.join(__dirname,'..',name+'.js'),'utf8'),c,{filename:name+'.js'});
 const r=new c.ATSRenderer(canvas());r.actorSpriteBudget=8;r.time=10;calls.length=0;return{c,r,calls};
}
test('military hull cache stays bounded while doors, damage and turret draw live',()=>{
 const{r,calls}=renderer();
 for(const kind of ['tank','apc','ifv','truck'])for(let i=0;i<8;i++)r.drawVehicle(r.ctx,{kind,x:0,y:0,hp:250,maxHp:650,mobilityDamage:60,weaponDamage:80,engineDamage:70,overrun:kind==='tank',dismounting:kind==='apc',troops:5,capacity:10,cannonFlash:.1,turretDir:i*.6,dir:i%2?Math.PI:0});
 assert.equal(r.vehicleSprites.size,4);
 assert.ok(calls.some(a=>a[0]==='fillText'&&a[1]==='OVERRUN'));
 assert.ok(calls.some(a=>a[0]==='fillText'&&a[1]==='DISMOUNT'));
 const before=r.vehicleSprites.get('apc');r.drawObject(r.ctx,{type:'vehicle',vehicleType:'apc',x:0,y:0});assert.equal(r.vehicleSprites.get('apc'),before);
});
test('cannon telegraph remains at the committed location and shell uses one trail',()=>{
 const{r,calls}=renderer(),target={x:60,y:30,start:9,duration:1.8,until:10.8};
 r.drawThreats({time:10,vehicles:[{kind:'tank',x:-50,y:0,hp:1100,cannonTarget:target}],humans:[],forces:{hazards:[{type:'shell',x:30,y:15,fromX:-50,fromY:0,targetX:60,targetY:30,start:9.8,life:.2,duration:.4,radius:104}]}});
 assert.ok(calls.some(a=>a[0]==='fillText'&&a[1]==='CANNON LOCK'));
 const p=r.project(target.x,target.y);assert.ok(calls.some(a=>a[0]==='translate'&&a[1]===p.x&&a[2]===p.y));
 assert.ok(!r.glowSprites.has('221,126,111'),'shell is not also rendered as a flare');
 calls.length=0;r.drawThreats({time:10,vehicles:[],humans:[],forces:{hazards:[{type:'fortification',x:0,y:0,life:70,start:9}]}});assert.equal(calls.length,0,'temporary construction lifetimes are not visual flare hazards');
 calls.length=0;r.drawThreats({time:10,vehicles:[{kind:'tank',x:50000,y:0,hp:100,cannonTarget:{...target,x:50100,y:0}}],humans:[],forces:{hazards:[]}});
 assert.equal(calls.length,0,'offscreen cannon geometry is culled');
});
test('new role, aircraft, impact and ape reaction geometry is finite',()=>{
 const{r}=renderer();
 for(const role of ['rifleman','ranger','heavy','engineer','leader'])r.drawHuman(r.ctx,{id:role,role,x:0,y:0,hp:100,maxHp:100,state:'combat',dir:0});
 for(const kind of ['recon','scout','gunship'])r.drawHeli(r.ctx,{kind,x:0,y:0,hp:250,maxHp:500,attackTimer:.1,dir:1});
 for(const fields of [{climbingVehicleId:'tank',climbUntil:11},{staggerUntil:11},{knockbackUntil:11}])r.drawApe(r.ctx,{id:'reaction',x:0,y:0,hp:50,maxHp:100,...fields});
 r.drawEffects(r.ctx,[{type:'tankImpact',x:0,y:0,life:.8,maxLife:1,radius:104}]);
});
test('impact and squad cues keep their budgets after leaders die',()=>{
 const{r,calls}=renderer();
 r.drawEffects(r.ctx,Array.from({length:60},()=>({type:'tankImpact',x:0,y:0,life:.8,maxLife:1,radius:104})));
 assert.equal(calls.filter(a=>a[0]==='translate').length,20,'visible explosion budget');
 calls.length=0;r.drawThreats({time:10,vehicles:[],forces:{hazards:[]},humans:[{id:'survivor',squadId:'leader-down',role:'rifleman',x:0,y:0,hp:100,cohesionLossUntil:15}]});
 assert.ok(calls.some(a=>a[0]==='fillText'&&a[1]==='REGROUP'),'surviving squad displays its loss of cohesion');
});
test('large hordes keep climb and blast reactions outside the idle atlas',()=>{
 const{r,c}=renderer();c.ATSUtil={hash:()=>0};let live=0,cached=0;
 r.drawApe=(ctx,a,king)=>{if(!king)live++};r.drawApeSprite=()=>cached++;
 const apes=Array.from({length:80},(_,i)=>({id:'ape-'+i,x:0,y:0,hp:100,maxHp:100}));
 Object.assign(apes[77],{climbingVehicleId:'tank',climbUntil:11});apes[78].staggerUntil=11;apes[79].knockbackUntil=11;
 r.draw({time:10,king:{id:'king',x:0,y:0,hp:160,maxHp:160},apes,humans:[],vehicles:[],helis:[],world:{terrain:()=>({biome:'forest'}),getObjects:()=>[],getSites:()=>[]},forces:{hazards:[]}},0);
 assert.equal(cached,77);assert.equal(live,3);
});
test('new procedural cues throttle and only nearby engines use ambience voices',()=>{
 const{c}=renderer(),a=new c.ATSAudio(),voices=[];
 a.ctx={currentTime:10,state:'running'};a._tone=(...v)=>voices.push(['tone',...v]);a._noise=(...v)=>voices.push(['noise',...v]);
 for(const cue of ['tank','cannon','apc','truck','military','mobilization','rumble','recon','scout','gunship','overrun']){const before=voices.length;a.play(cue);assert.ok(voices.length>before,cue+' has procedural sound');const after=voices.length;a.play(cue);assert.equal(voices.length,after,cue+' rate limit')}
 const heard=[];a.play=(...v)=>heard.push(v);a.windGain={gain:{setTargetAtTime(){}}};a.windFilter={frequency:{setTargetAtTime(){}}};a.insectTime=a.beatTime=100;
 const g={king:{x:0,y:0},world:{terrain:()=>({biome:'forest'})},vehicles:[{kind:'tank',x:5000,y:0,hp:1100}],helis:[{kind:'gunship',x:5000,y:0,hp:450}]};a.update(g,.016);assert.deepEqual(heard,[]);
 a.ctx.currentTime=12;g.vehicles.push({kind:'apc',x:60,y:0,hp:600});g.helis.push({kind:'recon',x:0,y:80,hp:200});a.update(g,.016);assert.deepEqual(heard.map(v=>v[0]),['apc','recon']);
});
