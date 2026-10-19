const test=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const path=require('node:path');
const vm=require('node:vm');
class Vector3{
  constructor(x=0,y=0,z=0){this.set(x,y,z);} set(x,y,z){Object.assign(this,{x,y,z});return this;}
  setY(y){this.y=y;return this;} copy(v){return this.set(v.x,v.y,v.z);} clone(){return new Vector3(this.x,this.y,this.z);}
  addScaledVector(v,s){this.x+=v.x*s;this.y+=v.y*s;this.z+=v.z*s;return this;} add(v){return this.addScaledVector(v,1);} sub(v){return this.addScaledVector(v,-1);}
  multiplyScalar(s){return this.set(this.x*s,this.y*s,this.z*s);} divideScalar(s){return this.multiplyScalar(1/s);}
  length(){return Math.hypot(this.x,this.y,this.z);} normalize(){return this.divideScalar(this.length()||1);}
  dot(v){return this.x*v.x+this.y*v.y+this.z*v.z;} lerp(v,t){return this.set(this.x+(v.x-this.x)*t,this.y+(v.y-this.y)*t,this.z+(v.z-this.z)*t);}
}
function fixture(profile={},style='balanced'){
  const settings={defensiveIQ:.9,offensiveIQ:.9,spacingIQ:.9,courtVision:1,lookAhead:.4,readError:0,shotDiscipline:.9,
    quickReadTime:.4,passAdvantage:.1,crossoverUse:0,pumpFakeUse:0,reactionMin:.2,reactionMax:.3,contestTiming:.9};
  const random=Object.create(Math);random.random=()=>.8;
  const c=vm.createContext({Math:random,THREE:{Vector3,MathUtils:{clamp:(v,a,b)=>Math.max(a,Math.min(b,v)),lerp:(a,b,t)=>a+(b-a)*t,randFloat:(a,b)=>(a+b)/2}},
    HOOP_POS:new Vector3(0,3.28,-10.8),THREE_RADIUS:7.15,COURT_W:18,simulationTime:10,
    teamOffensiveStyles:[style,'balanced'],difficultySettings:{normal:settings},selectedDifficulty:'normal',botBrain:null,
    gameStarted:true,gamePaused:false,gameOver:false,possessionFrozen:false,ballState:'dribble',ball:{position:new Vector3()},ballVelocity:new Vector3(),passFlight:{active:false},
    shotCharging:false,chargingPlayer:null,layupActive:false,dunkActive:false,ballSideValue:1,flatDistance:(a,b)=>Math.hypot(a.x-b.x,a.z-b.z),
    keepInCourt(v){v.x=Math.max(-8.2,Math.min(8.2,v.x));v.z=Math.max(-12.2,Math.min(12.2,v.z));},
    getForward:a=>new Vector3(Math.sin(a),0,Math.cos(a)),attemptCrossover(){},startPumpFake(){},startPostUp(p){c.posts++;p.moveState='postup';},posts:0});
  const make=(id,teamId,x,z)=>({id,teamId,group:{position:new Vector3(x,0,c.HOOP_POS.z+z),rotation:{y:0}},velocity:new Vector3(),angle:Math.PI,
    bodyRadius:.55,profile:{archetype:'all_rounder',shot:1,layup:1,passing:1,handle:1,width:1,height:1,speed:1,sprint:1,block:1,steal:1},moveMeta:{},stamina:1,sprint:1,onGround:true,moveState:'normal'});
  c.matchPlayers=[make('handler',0,0,7.65),make('wing',0,-7,4),make('second',0,7,4),make('guard',1,7,1),make('help',1,-7,1),make('third',1,6,12)];
  c.p1=c.owner=c.matchPlayers[0];Object.assign(c.owner.profile,profile);c.p2=c.matchPlayers[3];
  c.getBallHandler=()=>c.owner;c.getPlayerById=id=>c.matchPlayers.find(p=>p.id===id);
  c.getTeammates=p=>c.matchPlayers.filter(o=>o.teamId===p.teamId&&o!==p);c.getOpponents=p=>c.matchPlayers.filter(o=>o.teamId!==p.teamId);
  c.getNearestOpponent=p=>c.getOpponents(p).sort((a,b)=>c.flatDistance(a.group.position,p.group.position)-c.flatDistance(b.group.position,p.group.position))[0];
  c.getBetweenPlayerAndRimTarget=(p,o,s)=>o.group.position.clone().addScaledVector(c.HOOP_POS.clone().sub(o.group.position).setY(0).normalize(),s);
  for(const file of ['ai-reads.js','ai-coordination.js','ai.js','ai-defense.js','campaign/rise-tactics.js']) vm.runInContext(fs.readFileSync(path.join(__dirname,'../js',file),'utf8'),c);
  vm.runInContext('initBotBrain()',c);c.settings=settings;c.shots=0;c.beginAIShot=()=>c.shots++;
  c.owner.aiBrain.actionCooldown=0;c.owner.aiBrain.possessionAge=1;c.owner.aiBrain.moveCooldown=0;
  c.decide=()=>c.decideAIHandler(c.owner,c.owner.aiBrain,settings);
  return c;
}
const shooter={role:'shooter',ratings:{threePoint:65,midrange:62,strength:50,closeShot:55},abilities:{catchAndShoot:2,quickRelease:2}};

test('a shooter assignment turns the same clean arc catch into a quick shot instead of a long open drive',()=>{
  for(const style of ['balanced','pick_roll','motion']){
    const c=fixture(shooter,style),p=c.owner,b=p.aiBrain;p.lastCatchTime=9.9;p.lastPassQuality=.9;b.possessionAge=.16;
    assert.ok(c.aiFindDriveLane(p,c.settings).drive,'fixture offers a tempting long drive');
    c.decide();assert.equal(b.decision,'shoot',style);assert.equal(b.shotRead,'catch_and_shoot');assert.equal(c.shots,1);
    assert.equal(c.flatDistance(b.target,p.group.position),0);assert.equal(b.driveUntil,0);
  }
  const plain=fixture({ratings:{threePoint:65,midrange:62,strength:50,closeShot:55}});plain.owner.lastCatchTime=9.9;
  plain.owner.aiBrain.possessionAge=.16;plain.decide();assert.equal(plain.shots,0,'a generalist takes the normal read time with the same ratings');
});

test('Catch & Shoot and Quick Release speed the catch read while feet wait for the normal release',()=>{
  const c=fixture({...shooter,abilities:{}}),p=c.owner,b=p.aiBrain;p.lastCatchTime=9.9;b.possessionAge=.16;
  c.decide();assert.equal(c.shots,0);assert.equal(b.decision,'shoot');assert.equal(c.flatDistance(b.target,p.group.position),0);
  p.profile.abilities={catchAndShoot:2,quickRelease:2};c.decide();assert.equal(c.shots,1);
  p.velocity.set(1.8,0,0);c.decide();assert.equal(c.shots,1,'moving catch settles before a jumper');
  p.lastCatchTime=1;p.velocity.set(0,0,0);b.possessionAge=.16;c.decide();assert.equal(c.shots,1,'old catches do not receive the quick-read bonus');
});

test('faceguarding, distant range and rotating closeouts cancel the catch shot without overriding shot physics',()=>{
  for(const kind of ['faceguard','range','closeout']){
    const c=fixture(shooter),p=c.owner;p.lastCatchTime=9.9;p.aiBrain.possessionAge=.3;
    if(kind==='faceguard')c.p2.group.position.copy(p.group.position).add(new Vector3(0,0,-.9));
    if(kind==='range')p.group.position.z=c.HOOP_POS.z+10.5;
    if(kind==='closeout'){c.p2.group.position.copy(p.group.position).add(new Vector3(0,0,-2.5));c.p2.velocity.set(0,0,6);}
    c.decide();assert.equal(c.shots,0,kind);assert.notEqual(p.aiBrain.shotRead,'catch_and_shoot',kind);
  }
});

test('shooters keep close free finishes and an established running lane, while slashers prefer the longer drive',()=>{
  const c=fixture(shooter),p=c.owner;p.group.position.set(0,0,c.HOOP_POS.z+2.5);c.decide();
  assert.equal(p.aiBrain.decision,'finish');assert.equal(c.shots,1);
  p.group.position.z=c.HOOP_POS.z+7.65;p.velocity.set(0,0,-3);p.lastCatchTime=-Infinity;c.decide();assert.equal(p.aiBrain.decision,'drive');
  const slasher=fixture({role:'slasher',archetype:'slasher',ratings:{threePoint:65,midrange:62},abilities:{highFlyer:2,posterizer:1}});
  slasher.decide();assert.equal(slasher.owner.aiBrain.decision,'drive');
});

test('a midrange specialist creates at the elbow while a shooter spaces outside the arc and an anchor seals low',()=>{
  const c=fixture(shooter),p=c.matchPlayers[1],plan=c.aiGetTeamPlan(c.owner);
  p.profile={...p.profile,...shooter};p.aiBrain.role='space';c.risePreferredSpot(p,p.aiBrain,plan,false);
  assert.equal(p.aiBrain.role,'shooting_outlet');assert.ok(c.flatDistance(p.aiBrain.target,c.HOOP_POS)>7.15);
  p.profile={...p.profile,role:'balanced',abilities:{midrangeAssassin:2,pullUpSpecialist:1},ratings:{midrange:85,threePoint:50}};
  p.aiBrain.role='space';plan.action='motion';c.risePreferredSpot(p,p.aiBrain,plan,false);assert.equal(p.aiBrain.role,'elbow');
  p.profile={...p.profile,role:'anchor',archetype:'interior',abilities:{screenSetter:2},ratings:{strength:85,threePoint:35,midrange:45}};
  p.aiBrain.role='space';plan.action='balanced';c.risePreferredSpot(p,p.aiBrain,plan,false);assert.equal(p.aiBrain.role,'post');assert.ok(c.flatDistance(p.aiBrain.target,c.HOOP_POS)<3.6);
});

test('a bruiser protects a single-matchup post, uses the common backdown action, and passes out of help',()=>{
  const c=fixture({archetype:'interior',role:'anchor',width:1.25,height:1.2,ratings:{strength:88,threePoint:30,midrange:45,closeShot:70},abilities:{powerFinisher:2,contactSpecialist:1}});
  const p=c.owner,b=p.aiBrain;p.group.position.set(1.7,0,c.HOOP_POS.z+3.5);c.p2.group.position.set(1.5,0,c.HOOP_POS.z+2.2);
  c.decide();assert.equal(b.decision,'post');assert.equal(b.protect,true);assert.equal(b.sprint,false);assert.equal(c.posts,1);assert.equal(b.postRead,'backdown');
  const plan=c.aiGetTeamPlan(p),receiver=c.matchPlayers[1],shot={value:.4,quality:.4,space:1,distance:4},lane={openness:0,drive:false,direct:false};
  c.matchPlayers[4].group.position.copy(p.group.position).add(new Vector3(-1.3,0,0));
  const purpose=c.aiPassingPurpose(p,receiver,{safe:true,points:3,catchValue:.65,catchQuality:.72,catchSpace:3,value:.7},shot,lane,plan,c.settings);
  assert.equal(purpose.reason,'inside_out');
  b.possessionAge=5;c.decide();assert.notEqual(b.decision,'post','an unproductive post does not stall forever');
});

test('role coverage works in custom games without campaign globals, respects explicit matchups and retains switches',()=>{
  const c=fixture(),defense=c.matchPlayers.filter(p=>p.teamId===0),opponents=c.getOpponents(c.owner);
  defense[0].profile.role='stopper';defense[1].profile.role='anchor';
  opponents[2].profile.ratings={threePoint:95,midrange:90,dribbling:88,strength:45,closeShot:45};
  opponents[1].profile.ratings={threePoint:30,midrange:40,strength:95,closeShot:92,standingDunk:95};
  const plan={},assignments=c.aiDefenseResolveAssignments(defense,0,null,plan);
  assert.equal(assignments.handler,opponents[2].id);assert.equal(assignments.wing,opponents[1].id);assert.equal(new Set(Object.values(assignments)).size,3);
  const switched={...assignments,handler:assignments.wing,wing:assignments.handler};
  const retained=c.aiDefenseResolveAssignments(defense,0,{assignments:switched},plan);assert.equal(retained.handler,switched.handler);assert.equal(retained.wing,switched.wing);
});

test('ratings, learned skills and assignment biases remain inputs rather than artificial physical boosts',()=>{
  const c=fixture(shooter),before=JSON.stringify(c.matchPlayers.map(p=>p.profile));
  c.owner.lastCatchTime=9.9;c.decide();c.aiTeamChemistry(c.owner);c.aiGetTeamPlan(c.owner);
  assert.equal(JSON.stringify(c.matchPlayers.map(p=>p.profile)),before);
  const a=c.aiPlayerTendencies(c.owner);c.owner.profile.abilities.catchAndShoot=3;
  assert.ok(c.aiPlayerTendencies(c.owner).catchShoot>a.catchShoot);
});

test('rim guardians stay down on fakes and contest an actual gather through the common block action',()=>{
  const c=fixture(),defender=c.p2,handler=c.owner;let jumps=0;
  defender.aiBrain.actionCooldown=0;
  defender.group.position.copy(handler.group.position).add(new Vector3(0,0,-1));
  c.aiAssignedOpponent=()=>handler;c.aiCoordinateDefense=()=>false;c.tryJumpBlock=()=>jumps++;c.raiseHands=()=>{};
  c.Math.random=()=>.6;c.settings.fakeBiteChance=1;handler.moveState='pumpfake';
  c.decideAIDefense(defender,defender.aiBrain,handler,c.settings);assert.equal(jumps,1);
  defender.aiBrain.actionCooldown=0;defender.profile.role='anchor';defender.profile.abilities={rimGuardian:3};
  c.decideAIDefense(defender,defender.aiBrain,handler,c.settings);assert.equal(jumps,1,'disciplined anchor does not bite on this fake');
  handler.moveState='normal';handler.moveMeta.chargeHeld=.8;c.shotCharging=true;c.chargingPlayer=handler;
  c.decideAIDefense(defender,defender.aiBrain,handler,c.settings);assert.equal(jumps,2,'a real gather still earns a timed normal block');
});

test('Pickpocket attempts controlled reaches only when the exposed ball is visibly within reach',()=>{
  const c=fixture(),defender=c.p2,handler=c.owner;let reaches=0;
  defender.aiBrain.actionCooldown=0;
  defender.group.position.copy(handler.group.position).add(new Vector3(0,0,-1));
  c.aiAssignedOpponent=()=>handler;c.aiCoordinateDefense=()=>false;c.handleHandAction=()=>reaches++;c.punishBadReach=()=>{};
  c.Math.random=()=>.18;c.settings.stealDiscipline=.5;c.settings.mistakeChance=0;handler.dribbleStyle='sprint';c.ball.position.copy(handler.group.position);
  c.decideAIDefense(defender,defender.aiBrain,handler,c.settings);assert.equal(reaches,0);
  defender.profile.abilities={pickpocket:3};c.decideAIDefense(defender,defender.aiBrain,handler,c.settings);assert.equal(reaches,1);
  defender.aiBrain.actionCooldown=0;c.ball.position.x+=4;c.decideAIDefense(defender,defender.aiBrain,handler,c.settings);assert.equal(reaches,1,'a hidden or distant ball gets no skill-based reach');
  c.ball.position.copy(handler.group.position);handler.dribbleStyle='protect';c.decideAIDefense(defender,defender.aiBrain,handler,c.settings);assert.equal(reaches,1,'protected dribble is not an invitation to reach');
});
