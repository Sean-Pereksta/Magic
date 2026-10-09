const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),vm=require('node:vm'),path=require('node:path');
function game(){
 const c=vm.createContext({console,Math,Map,Set});c.window=c;
 for(const f of ['world','navigation','settlements','forces','sim'])vm.runInContext(fs.readFileSync(path.join(__dirname,'..',f+'.js'),'utf8'),c);
 const g=new c.ATSGame('spacing');g.world.ensure=()=>{};g.world.objects.clear();g.world._spatial.clear();g.world.sites.clear();g.world.terrain=()=>({biome:'forest',water:false,road:false});g.spawnSites=()=>{};g.king.x=-400;
 return g;
}
function ape(g,x=0,y=0,state='hold'){
 const a=g.makeApe(x,y,state);Object.assign(a,{phase:0,speed:90,offsetX:0,offsetY:0,attackCD:0,target:{x,y}});return a;
}
function run(g,seconds=4,fps=60,check=()=>{}){for(let i=0;i<seconds*fps;i++){g.update(1/fps,{});check()}}
const distance=(a,b)=>Math.hypot(a.x-b.x,a.y-b.y);
test('idle held apes separate from exact overlaps smoothly and keep holding nearby',()=>{
 const g=game(),apes=Array.from({length:12},()=>ape(g));let previous=apes.map(a=>({x:a.x,y:a.y}));
 run(g,5,60,()=>{apes.forEach((a,i)=>{assert.ok(distance(a,previous[i])<3,'no teleporting');assert.equal(g.world.blocked(a.x,a.y,10),false)});previous=apes.map(a=>({x:a.x,y:a.y}))});
 for(let i=0;i<apes.length;i++)for(let j=i+1;j<apes.length;j++)assert.ok(distance(apes[i],apes[j])>12);
 for(const a of apes){assert.equal(a.state,'hold');assert.ok(Math.hypot(a.x,a.y)<70)}
});
test('followers inside their arrival radius make room for the king without moving him',()=>{
 const g=game();g.king.x=0;const a=ape(g,0,0,'follow'),b=ape(g,0,0,'follow');run(g);
 assert.ok(distance(a,b)>20);assert.ok(distance(a,g.king)>20);assert.ok(distance(b,g.king)>20);assert.equal(g.king.x,0);assert.equal(g.king.y,0);
});
test('spacing remains similar at 30, 60 and 120 updates per second',()=>{
 const gaps=[];for(const fps of [30,60,120]){const g=game(),a=ape(g),b=ape(g);run(g,4,fps);gaps.push(distance(a,b))}
 assert.ok(Math.max(...gaps)-Math.min(...gaps)<1);
});
test('separation respects narrow corridors, solid walls and water',()=>{
 const g=game();for(const y of [-26,26]){const o={id:'wall-'+y,x:0,y,w:400,h:10,r:5,collision:'rect',solid:true,hp:100};g.world.objects.set(o.id,o);g.world._indexObject(o)}
 g.world.terrain=(x,y)=>({biome:'forest',water:x>60,road:false});const apes=Array.from({length:10},()=>ape(g));
 run(g,6,60,()=>{for(const a of apes){assert.equal(g.world.blocked(a.x,a.y,10),false);assert.ok(Math.abs(a.y)<=11);assert.ok(a.x<=60)}});
 assert.ok(Math.max(...apes.map(a=>a.x))-Math.min(...apes.map(a=>a.x))>40);
});
test('apes on opposite sides of a wall exert no pressure on each other',()=>{
 const g=game(),o={id:'wall',x:0,y:0,w:2,h:200,r:1,collision:'rect',solid:true,hp:100};g.world.objects.set(o.id,o);g.world._indexObject(o);
 const a=ape(g,-12),b=ape(g,12);g.spreadApes(1/60);assert.equal(a.x,-12);assert.equal(b.x,12);
});
test('combatants make room while retaining attack direction and damage',()=>{
 const g=game(),a=ape(g,0,0,'charge'),b=ape(g,0,0,'charge'),h=g.makeHuman(24,0,null);g.humanGrid.rebuild([h]);g.apeGrid.rebuild([g.king,a,b]);
 g.updateApe(a,1/60);g.updateApe(b,1/60);const dir=a.dir;g.spreadApes(1/60);
 assert.ok(h.hp<70);assert.ok(distance(a,b)>0);assert.equal(a.dir,dir);assert.ok(a.attackTimer>0);assert.equal(a.state,'charge');
});
test('dead apes and dormant distant settlements do not drift or repel',()=>{
 const g=game(),a=ape(g),dead=ape(g);dead.hp=0;g.spreadApes(1/60);assert.equal(a.x,0);assert.equal(a.y,0);
 g.settlements.push({id:'remote',x:3000,y:0,attack:false});const remote=ape(g,3000,0,'settled');remote.settlementId='remote';const other=ape(g,3000,0,'young');other.settlementId='remote';g.spreadApes(1/60);assert.equal(remote.x,3000);assert.equal(other.x,3000);
});
test('a large horde spreads out and still travels with the king',()=>{
 const g=game();g.king.x=0;
 for(let i=0;i<100;i++){const a=ape(g,0,0,'follow');a.offsetX=Math.sin(i*2.399)*Math.sqrt((i+1)/100)*60;a.offsetY=Math.cos(i*2.399)*Math.sqrt((i+1)/100)*60;a.phase=i*2.399}
 run(g,8);assert.ok(Math.max(...g.apes.map(a=>distance(a,g.king)))>200);
 for(let i=0;i<8*60;i++)g.update(1/60,{x:1});
 assert.ok(g.apes.every(a=>distance(a,g.king)<350));assert.ok(g.apes.every(a=>a.state==='follow'));
});
