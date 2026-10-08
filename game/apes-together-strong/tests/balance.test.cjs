const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),vm=require('node:vm');
function game(){const c=vm.createContext({console,Math,Map,Set});c.window=c;for(const f of ['world','navigation','settlements','forces','sim'])vm.runInContext(fs.readFileSync(path.join(__dirname,'..',f+'.js'),'utf8'),c);const g=new c.ATSGame('balance');g.world.objects.clear();g.world._spatial.clear();g.world.sites.clear();g.world.ensure=()=>{};g.world.terrain=()=>({biome:'forest',water:false});g.world.lineClear=()=>true;g.world.getSites=()=>[];g.spawnSites=()=>{};return{g,c}}
function colony(n){const {g,c}=game();for(let i=0;i<n;i++)g.makeApe(i,0,'follow');g.food=n*10;g.command('settleAll');const s=g.settlements[0];Object.assign(s,{food:n*10,housing:n+48,wood:120,gardens:Math.ceil(n/8),suitability:{fertility:1,wood:24,capacity:c.MAX_APE_POPULATION,water:true}});delete s.structuresVersion;g.colonies.init(s);return{g,c,s}}
function seconds(g,s,n){for(let i=0;i<n;i++){g.time++;g.refreshSettlements();g.colonies.tick(s);g.navigation.beginFrame(g.time);for(const a of g.apes)if(a.state==='young')g.updateApe(a,1)}g.refreshSettlements()}
test('healthy adults take 2–4 ordinary weapon hits; scouts are tougher',()=>{
 for(const [weapon,hits]of [['pistol',4],['rifle',3],['assault',4],['machine',4],['shotgun',2]]){const {g}=game(),a=g.makeApe(0,0),h=g.makeHuman(200,0,null);h.kind=weapon;let shots=0;while(a.hp>0){g.bullets=[];g.shoot(h,a);for(const b of g.bullets)g.hurt(a,b.damage,h);shots++;assert.ok(shots<6)}assert.equal(shots,hits,weapon)}
 const {g}=game();assert.equal(g.makeApe(0,0,'scout').maxHp,150);assert.equal(g.makeApe(0,0).maxHp,120);
});
test('snipers are reserved for tier 3+ forces and still kill a defended scout in one hit',()=>{
 const {g}=game();for(const tier of [1,2])for(let i=0;i<150;i++){const h=g.makeHuman(200,0,{tier});assert.notEqual(h.role,'sniper')}
 const late=[];for(let i=0;i<150;i++)late.push(g.makeHuman(200,0,{tier:3}));assert.ok(late.some(h=>h.role==='sniper'));
 for(const difficulty of ['wanderer','survival','relentless']){g.difficulty=difficulty;const s={id:'defended',x:0,y:0,radius:100,defense:1000};g.settlements=[s];const a=g.makeApe(0,0,'scout',s.id),h=late.find(h=>h.role==='sniper');g.bullets=[];g.shoot(h,a);g.hurt(a,g.bullets[0].damage,h);assert.ok(a.hp<=0,difficulty)}
});
test('young grow in 35 seconds both nearby and in distant settlements',()=>{
 for(const x of [0,3000]){const {g}=game(),s={id:'home',x,y:0,attack:false};g.settlements=[s];const a=g.makeApe(x,0,'young',s.id,true);a.age=34;g.updateApe(a,.9);assert.equal(a.state,'young');g.updateApe(a,.1);assert.equal(a.state,'settled');assert.equal(a.maxHp,120)}
});
test('settlement levels catch up to inhabitants through faster construction',()=>{
 // Visible clearing, frame, roof and completion stages take time even when
 // many builders share a project. Established colonies still catch up.
 for(const [population,level]of [[24,3],[60,6],[120,10]]){const {g,s}=colony(population);s.birthTimer=-100000;g.king.x=3000;for(let i=0;i<90;i++){s.wood=120;seconds(g,s,1)}assert.equal(s.level,level);assert.ok(s.housing>=population+Math.max(8,Math.ceil(population*.25)))}
});
test('larger communities raise more offspring and the young become useful adults',()=>{
 const small=colony(16),large=colony(64);seconds(small.g,small.s,60);seconds(large.g,large.s,60);assert.ok(small.g.stats.born>=3);assert.ok(large.g.stats.born>=small.g.stats.born*3);assert.ok(large.g.apes.filter(a=>a.state==='settled').length>64);assert.ok(large.s.food>0);
});
test('families require adult residents and housing but low supplies and attacks do not pause births',()=>{
 for(const blocked of ['food','attack','children','housing']){const {g,s}=colony(16);s.birthTimer=30;if(blocked==='food'){s.food=0;s.gardens=0}else if(blocked==='attack')g.humans=[{id:'raider',x:0,y:0,hp:100,state:'combat'}];else if(blocked==='children')for(const a of g.apes)a.state='young';else{s.housing=16;delete s.structuresVersion;g.colonies.init(s);s.wood=0;s.suitability.wood=0}s.safety=1;g.refreshSettlements();g.colonies.tick(s);assert.equal(g.stats.born>0,blocked==='food'||blocked==='attack',blocked)}
});
test('births respect the shared ape population limit',()=>{
 const {c}=game(),{g,s}=colony(c.MAX_APE_POPULATION-2);s.birthTimer=300;g.colonies.tick(s);assert.equal(g.population,c.MAX_APE_POPULATION);g.refreshSettlements();s.birthTimer=300;g.colonies.tick(s);assert.equal(g.population,c.MAX_APE_POPULATION);assert.equal(g.stats.born,2);assert.ok(s.birthTimer<=30);
});
test('military facilities hold substantially more captives with matching cage totals',()=>{
 const {c}=game(),w=new c.ATSWorld('balance-sites');w.ensure(0,0,1400);w.ensure(6400,6400,4200);const ranges={transport:[2,4],hunter:[4,7],research:[8,14],checkpoint:[8,16],prison:[18,30],detention:[42,72],experimental:[80,125],forwardBase:[32,60],armoredDepot:[60,110],regionalCommand:[160,260]};
 assert.equal(w.sites.get('opening-rescue').count,2);assert.equal(w.sites.get('opening-hunters').count,6);
 let military=0;for(const s of w.sites.values()){if(s.tutorial)continue;const [lo,hi]=ranges[s.type];assert.ok(s.count>=lo&&s.count<=hi);const cages=s.objects.map(id=>w.objects.get(id)).filter(o=>o.type==='cage');assert.equal(cages.reduce((sum,o)=>sum+o.count,0),s.count);if(s.tier>=3)military++}assert.ok(military>0);
});
test('liberating a military facility awards supplies exactly once and ape score rewards increase',()=>{
 const {g}=game(),s={id:'base',name:'Base',tier:3,objects:[],strength:0};g.checkSite(s);assert.equal(g.food,90);g.checkSite(s);assert.equal(g.food,90);const score=g.score;g.stats.freed++;assert.equal(g.score-score,250);g.stats.largestHorde++;assert.equal(g.score-score,340);g.stats.largestSettlement++;assert.equal(g.score-score,400);
});
test('early rescues reach containment only while active followers remain above its threshold',()=>{
 const {g}=game();for(let i=0;i<40;i++)g.makeApe(i,0,'follow');g.stats.freed=40;g.stats.bases=1;g.time=90;g.tickSecond();assert.equal(g.tier,2);
 for(const a of g.apes)a.state='settled';g.tickSecond();assert.equal(g.tier,1);
});
test('old saves migrate ape wounds while preserving recorded captive stock',()=>{
 const c=game().c,g=new c.ATSGame('migration-balance'),a=g.makeApe(0,0),child=g.makeApe(10,0,'young',null,true);a.maxHp=48;a.hp=24;child.maxHp=22;child.hp=11;child.age=45;
 const s=g.world.sites.get('opening-hunters'),cage=s.objects.map(id=>g.world.objects.get(id)).find(o=>o.type==='cage');cage.count=cage.prisoners=s.count=6;
 const saved=JSON.parse(JSON.stringify(g.serialize()));delete saved.balanceVersion;const loaded=c.ATSGame.fromJSON(saved);assert.equal(loaded.apes[0].maxHp,120);assert.equal(loaded.apes[0].hp,60);assert.equal(loaded.apes[1].maxHp,60);assert.equal(loaded.apes[1].hp,30);assert.equal(loaded.apes[1].age,17.5);assert.equal(loaded.world.objects.get(cage.id).count,6);
 const again=c.ATSGame.fromJSON(JSON.parse(JSON.stringify(loaded.serialize())));assert.equal(again.apes[0].hp,60);assert.equal(again.world.objects.get(cage.id).count,6);assert.equal(again.serialize().balanceVersion,2);
});
