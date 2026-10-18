const test=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const path=require('node:path');
const vm=require('node:vm');

class Vector3{
  constructor(x=0,y=0,z=0){this.set(x,y,z);}
  set(x,y,z){this.x=x;this.y=y;this.z=z;return this;}
  setY(y){this.y=y;return this;}
  copy(v){return this.set(v.x,v.y,v.z);}
  clone(){return new Vector3(this.x,this.y,this.z);}
  add(v){return this.addScaledVector(v,1);}
  sub(v){return this.addScaledVector(v,-1);}
  addScaledVector(v,s){this.x+=v.x*s;this.y+=v.y*s;this.z+=v.z*s;return this;}
  multiplyScalar(s){return this.set(this.x*s,this.y*s,this.z*s);}
  divideScalar(s){return this.multiplyScalar(1/s);}
  lengthSq(){return this.x*this.x+this.y*this.y+this.z*this.z;}
  length(){return Math.sqrt(this.lengthSq());}
  normalize(){return this.divideScalar(this.length()||1);}
  dot(v){return this.x*v.x+this.y*v.y+this.z*v.z;}
  lerp(v,t){this.x+=(v.x-this.x)*t;this.y+=(v.y-this.y)*t;this.z+=(v.z-this.z)*t;return this;}
}
const profiles={
  playmaker:{archetype:'playmaker',passing:1.5,handle:1.3},
  sharpshooter:{archetype:'sharpshooter',shot:1.5},
  slasher:{archetype:'slasher',sprint:1.35,layup:1.4},
  interior:{archetype:'interior',width:1.35,height:1.2,layup:1.3,shot:.75,speed:.9},
  defender:{archetype:'defender',block:1.5,steal:1.4},
  all_rounder:{archetype:'all_rounder'}
};
function fixture(style='balanced',size=3,types=[]){
  const random=Object.create(Math);random.random=()=>.8;
  const settings={defensiveIQ:.8,offensiveIQ:.9,spacingIQ:.8,courtVision:.9,patience:.7,shotDiscipline:.7,
    reactionMin:.2,reactionMax:.3,crossoverUse:0,pumpFakeUse:0,stealDiscipline:0,mistakeChance:0,
    fakeBiteChance:0,contestTiming:.7,passAdvantage:.1};
  const ctx=vm.createContext({Math:random,THREE:{Vector3,MathUtils:{clamp:(n,a,b)=>Math.max(a,Math.min(b,n)),lerp:(a,b,t)=>a+(b-a)*t,randFloat:(a,b)=>(a+b)/2}},
    HOOP_POS:new Vector3(0,3.28,-10.8),COURT_W:18,teamOffensiveStyles:[style,'balanced'],
    difficultySettings:{normal:settings},selectedDifficulty:'normal',botBrain:null,
    gameStarted:true,gamePaused:false,gameOver:false,possessionFrozen:false,
    ballState:'dribble',ball:{position:new Vector3()},ballVelocity:new Vector3(),passFlight:{active:false},
    shotCharging:false,chargingPlayer:null,layupActive:false,dunkActive:false,ballSideValue:1,
    flatDistance:(a,b)=>Math.hypot(a.x-b.x,a.z-b.z),
    keepInCourt(v){v.x=Math.max(-8.2,Math.min(8.2,v.x));v.z=Math.max(-12.2,Math.min(12.2,v.z));},
    getForward:a=>new Vector3(Math.sin(a),0,Math.cos(a)),lerpAngle:(a,b,t)=>a+(b-a)*t,
    updateModernPlayerControl(){},attemptCrossover(){},startPumpFake(){},handleActionButton(){},
    tryJumpBlock(){},raiseHands(){},handleHandAction(){},punishBadReach(){},passCalls:[]});
  ctx.matchPlayers=[];
  for(let teamId=0;teamId<2;teamId++) for(let slot=0;slot<size;slot++){
    ctx.matchPlayers.push({id:`${teamId}_${slot}`,teamId,controllerSlot:teamId===0&&slot===0?'p1':null,
      group:{position:new Vector3(teamId===1?(slot===0?7:-7):slot===0?0:slot===1?-5:5,0,teamId===1?-1-slot:slot===0?1:-3),rotation:{y:0}},
      velocity:new Vector3(),angle:Math.PI,bodyRadius:.55,
      profile:{shot:1,layup:1,passing:1,handle:1,width:1,height:1,speed:1,sprint:1,block:1,steal:1,...profiles[types[slot]]},
      moveMeta:{},stamina:1,sprint:1,onGround:true,moveState:'normal'});
  }
  ctx.p1=ctx.matchPlayers[0];ctx.p2=ctx.matchPlayers[size];ctx.owner=ctx.p1;
  ctx.getBallHandler=()=>ctx.owner;
  ctx.getPlayerById=id=>ctx.matchPlayers.find(player=>player.id===id);
  ctx.getTeammates=char=>ctx.matchPlayers.filter(player=>player.teamId===char.teamId&&player!==char);
  ctx.getOpponents=char=>ctx.matchPlayers.filter(player=>player.teamId!==char.teamId);
  ctx.getNearestOpponent=char=>ctx.getOpponents(char).reduce((best,p)=>!best||ctx.flatDistance(p.group.position,char.group.position)<ctx.flatDistance(best.group.position,char.group.position)?p:best,null);
  ctx.getBetweenPlayerAndRimTarget=(defender,offense,spacing)=>offense.group.position.clone().addScaledVector(ctx.HOOP_POS.clone().sub(offense.group.position).setY(0).normalize(),spacing);
  ctx.tryDirectionalPass=(char,receiver)=>{ctx.passCalls.push([char.id,receiver.id]);return true;};
  for(const file of ['ai-reads.js','ai-coordination.js','ai.js','ai-defense.js']) vm.runInContext(fs.readFileSync(path.join(__dirname,'../js',file),'utf8'),ctx);
  vm.runInContext('initBotBrain();globalThis.tick=(dt,frozen=false)=>{teamAITime+=dt;updateAITactics(getBallHandler(),dt,frozen);};globalThis.plans=()=>({offense:aiTeamPlans,defense:aiDefensePlans});',ctx);
  ctx.settings=settings;
  ctx.offBall=()=>ctx.getTeammates(ctx.owner).filter(player=>!player.controllerSlot).forEach(player=>ctx.decideAIOffBall(player,player.aiBrain,ctx.owner,settings));
  return ctx;
}

test('all eight authored plays assign valid, separated small-team roles in both 2v2 and 3v3',()=>{
  for(const size of [2,3]) for(const style of ['motion','pick_roll','pick_pop','drive_kick','isolation','give_go','post_up','fast_break']){
    const ctx=fixture(style,size,['playmaker','interior','sharpshooter']);ctx.tick(.01);
    const plan=ctx.plans().offense[0];
    if(style==='pick_roll'||style==='pick_pop'){
      ctx.getPlayerById(plan.actorId).group.position.copy(plan.screenTarget);
      ctx.tick(.45);ctx.offBall();
      assert.equal(ctx.getPlayerById(plan.actorId).aiBrain.role,'screen',`${size} ${style} plants a screen`);
      ctx.tick(1.1);
    }else if(style==='motion'||style==='drive_kick'||style==='isolation'||style==='give_go') ctx.tick(.7);
    ctx.offBall();
    const support=ctx.getTeammates(ctx.owner),roles=support.map(p=>p.aiBrain.role);
    const expected={motion:'cut',pick_roll:'roll',pick_pop:'pop',drive_kick:'outlet',isolation:'clear',give_go:'hub',post_up:'post',fast_break:'break'}[style];
    assert.ok(roles.includes(expected),`${size} ${style}: ${roles}`);
    assert.equal(support.filter(p=>['cut','roll','post','break'].includes(p.aiBrain.role)).length,['motion','pick_roll','post_up','fast_break'].includes(style)?1:0);
    for(const p of support){
      assert.ok(Number.isFinite(p.aiBrain.target.x)&&Number.isFinite(p.aiBrain.target.z));
      assert.ok(Math.abs(p.aiBrain.target.x)<=8.2&&Math.abs(p.aiBrain.target.z)<=12.2);
      assert.equal(p.controllerSlot,null);
    }
    if(size===3) assert.ok(ctx.flatDistance(support[0].aiBrain.target,support[1].aiBrain.target)>1.65,`${style} keeps a distinct outlet`);
  }
});

test('explicit and stat-derived identities choose different actions and preserve player attributes',()=>{
  const ctx=fixture();
  const before=JSON.stringify(ctx.owner.profile);
  for(const type of Object.keys(profiles)){
    ctx.owner.profile={shot:1,layup:1,handle:1,width:1,height:1,speed:1,sprint:1,block:1,steal:1,...profiles[type]};
    assert.equal(ctx.aiPlayerIdentity(ctx.owner).type,type);
  }
  const derived=[['sharpshooter',{shot:1.3}],['playmaker',{handle:1.5}],['interior',{height:1.2,speed:.9}],['defender',{block:1.6}],['slasher',{layup:1.4}],['all_rounder',{}]];
  for(const [type,stats] of derived){ctx.owner.profile=stats;assert.equal(ctx.aiPlayerIdentity(ctx.owner).type,type);}
  const choices=[];
  for(const types of [['playmaker','interior'],['playmaker','sharpshooter'],['sharpshooter','sharpshooter']]){
    const team=fixture('balanced',2,types),plan=team.aiGetTeamPlan(team.owner);
    choices.push(team.aiChooseOffensiveAction(team.owner,plan));
    const snapshots=team.matchPlayers.map(p=>JSON.stringify(p.profile));
    team.tick(.7);team.offBall();
    assert.deepEqual(team.matchPlayers.map(p=>JSON.stringify(p.profile)),snapshots);
  }
  assert.deepEqual(choices,['pick_roll','pick_pop','motion']);
  assert.ok(before.includes('"shot":1'));
});

test('chemistry assigns skilled screeners, pop shooters, passing hubs and transition runners',()=>{
  for(const [style,types,expected] of [
    ['pick_roll',['playmaker','interior','sharpshooter'],'0_1'],
    ['pick_pop',['playmaker','interior','sharpshooter'],'0_2'],
    ['give_go',['slasher','interior','playmaker'],'0_2'],
    ['fast_break',['defender','interior','slasher'],'0_2']]){
    const ctx=fixture(style,3,types),plan=ctx.aiGetTeamPlan(ctx.owner);
    assert.equal(plan.actorId,expected,style);
    assert.equal(plan.chemistry.identities.length,3);
  }
  const transition=fixture('balanced',2,['defender','slasher']);
  transition.owner.group.position.set(0,0,3);
  transition.getTeammates(transition.owner)[0].group.position.set(3,0,-3);
  transition.getOpponents(transition.owner).forEach(p=>p.group.position.z=9);
  transition.tick(.7);
  assert.equal(transition.plans().offense[0].action,'fast_break');
});

test('give-and-go retains its passer and permits a return only after a real cut or new advantage',()=>{
  const ctx=fixture('give_go',2,['playmaker','slasher']);ctx.p1.controllerSlot=null;
  ctx.tick(.7);const first=ctx.owner,receiver=ctx.getTeammates(first)[0],plan=ctx.aiGetTeamPlan(first);
  ctx.owner=receiver;ctx.tick(.1);ctx.offBall();
  assert.equal(ctx.aiGetTeamPlan(receiver),plan);
  assert.equal(plan.phase,'cut');assert.equal(first.aiBrain.role,'cut');
  assert.equal(plan.passHistory.length,1);
  const pass={safe:true,value:.72,catchValue:.68,catchSpace:3,rimFeed:false,points:2};
  const shot={value:.5,quality:.5,space:2,distance:6};
  const lane={openness:.3,drive:false,direct:false};
  assert.equal(ctx.aiPassingPurpose(receiver,first,pass,shot,lane,plan,ctx.settings),null,'standing return is refused');
  first.group.position.z-=2;
  assert.equal(ctx.aiPassingPurpose(receiver,first,pass,shot,lane,plan,ctx.settings).reason,'better_shot');
  first.group.position.copy(plan.passHistory[0].fromPosition);
  const openThree={...pass,value:.88,catchValue:.85,points:3};
  assert.equal(ctx.aiPassingPurpose(receiver,first,openThree,shot,lane,plan,ctx.settings).reason,'better_shot','a newly open excellent shot can beat the return restriction');
});

test('passing requires an advantage and rejects a repeated stationary pair despite elapsed possession age',()=>{
  const ctx=fixture('motion',2);const teammate=ctx.getTeammates(ctx.owner)[0],plan=ctx.aiGetTeamPlan(ctx.owner);
  const shot={value:.6,quality:.6,space:2,distance:6},lane={openness:.3,drive:false,direct:false};
  const equal={safe:true,value:.6,catchValue:.6,catchSpace:3,rimFeed:false};
  ctx.owner.aiBrain.possessionAge=99;
  assert.equal(ctx.aiPassingPurpose(ctx.owner,teammate,equal,shot,lane,plan,ctx.settings),null);
  for(let i=0;i<3;i++) plan.passHistory.push({from:i%2?ctx.owner.id:teammate.id,to:i%2?teammate.id:ctx.owner.id,at:i*.1,fromPosition:teammate.group.position.clone()});
  assert.equal(ctx.aiPassingPurpose(ctx.owner,teammate,{...equal,value:.77,catchValue:.76},shot,lane,plan,ctx.settings),null);
  teammate.group.position.x+=2;
  assert.equal(ctx.aiPassingPurpose(ctx.owner,teammate,{...equal,value:.77,catchValue:.76},shot,lane,plan,ctx.settings).reason,'better_shot');
});

test('a queued pass cancels at release when it remains safe but no longer improves the offense',()=>{
  const ctx=fixture('motion',2);const receiver=ctx.getTeammates(ctx.owner)[0],brain=ctx.owner.aiBrain;
  const shot={value:.6,quality:.6,space:2,distance:6};
  ctx.aiAssessShot=()=>shot;ctx.aiFindDriveLane=()=>({openness:.3,drive:false,direct:false});
  ctx.aiAssessPass=()=>({safe:true,value:.6,catchValue:.6,catchSpace:3,rimFeed:false});
  ctx.owner.angle=Math.atan2(receiver.group.position.x-ctx.owner.group.position.x,receiver.group.position.z-ctx.owner.group.position.z);
  brain.passTargetId=receiver.id;brain.passUntil=1;brain.passReason='better_shot';
  ctx.updateAIShotAndPass(ctx.owner,brain,ctx.owner,1/60);
  assert.equal(brain.passTargetId,null);assert.equal(ctx.passCalls.length,0);
  ctx.aiAssessPass=()=>({safe:true,value:.85,catchValue:.82,catchSpace:3,rimFeed:false});
  brain.passTargetId=receiver.id;brain.passUntil=1;
  ctx.updateAIShotAndPass(ctx.owner,brain,ctx.owner,1/60);
  assert.equal(ctx.passCalls.length,1);assert.equal(ctx.aiGetTeamPlan(ctx.owner).passHistory[0].reason,'better_shot');
});

test('a late team possession settles for a playable shot after a catch, while covered looks still create',()=>{
  const ctx=fixture('pick_roll',2),brain=ctx.owner.aiBrain,plan=ctx.aiGetTeamPlan(ctx.owner);
  let shots=0;ctx.handleActionButton=()=>shots++;
  ctx.aiAssessShot=()=>({value:.55,quality:.55,space:1.4,distance:5,inRange:true,closeFinish:false});
  ctx.aiAssessPass=()=>({safe:false});
  ctx.aiFindDriveLane=()=>({openness:.85,drive:true,direct:false,finishSpace:2.1,blocked:false,target:new Vector3(2,0,-7)});
  brain.possessionAge=.6;brain.actionCooldown=0;
  plan.phase='roll';plan.age=1;
  ctx.decideAIHandler(ctx.owner,brain,ctx.settings);
  assert.equal(brain.decision,'drive');assert.equal(shots,0);
  plan.age=15;
  ctx.decideAIHandler(ctx.owner,brain,ctx.settings);
  assert.equal(brain.decision,'shoot');assert.equal(shots,1);
  assert.equal(brain.driveUntil,0);
  ctx.aiAssessShot=()=>({value:.15,quality:.15,space:.7,distance:5,inRange:true,closeFinish:false});
  ctx.decideAIHandler(ctx.owner,brain,ctx.settings);
  assert.equal(shots,1,'team age never authorizes a smothered desperation shot');
  ctx.aiAssessShot=()=>({value:.23,quality:.23,space:1.2,distance:7.8,inRange:false,closeFinish:false});
  plan.age=15;ctx.decideAIHandler(ctx.owner,brain,ctx.settings);assert.equal(shots,1);
  plan.age=17;ctx.decideAIHandler(ctx.owner,brain,ctx.settings);
  assert.equal(shots,2,'after prolonged failed actions, a physically playable marginal look is preferable to an endless reset');
});

test('a safe authored screen turn can create a lane before a complete open finish exists',()=>{
  const ctx=fixture('pick_roll',2),char=ctx.owner,brain=char.aiBrain,plan=ctx.aiGetTeamPlan(char);
  plan.phase='screen';plan.screenTarget.set(0,0,-3);
  ctx.aiAssessShot=()=>({value:.2,quality:.2,space:1.5,distance:5,inRange:true,closeFinish:false});
  ctx.aiAssessPass=()=>({safe:false});
  ctx.aiFindDriveLane=()=>({openness:.35,drive:false,direct:false,finishSpace:1.4,blocked:false,target:new Vector3(5,0,-3),setupTarget:new Vector3(5,0,-3)});
  const toRim=ctx.HOOP_POS.clone().sub(char.group.position).setY(0).normalize();
  const expected={target:new Vector3()};ctx.aiPlayHandlerTarget(char,expected,plan,toRim);
  assert.equal(ctx.aiRouteBlocked(char,expected.target),false);
  ctx.decideAIHandler(char,brain,ctx.settings);
  assert.deepEqual(brain.target,expected.target);
  assert.notEqual(brain.target.x,5,'the generic safe setup cannot discard an unblocked screen turn');
});

test('frontcourt advance gains ground around a set defender without assuming an open rim or crossing a body',()=>{
  const ctx=fixture('pick_roll',2),char=ctx.owner;
  char.group.position.set(0,0,4);
  const defender=ctx.getOpponents(char)[0];defender.group.position.set(0,0,1.6);
  const original=char.group.position.clone();
  const toward=ctx.HOOP_POS.clone().sub(char.group.position).setY(0).normalize();
  const target=ctx.aiAdvanceToFrontcourt(char,toward);
  assert.ok(target);
  assert.ok(ctx.flatDistance(target,ctx.HOOP_POS)<ctx.flatDistance(char.group.position,ctx.HOOP_POS)-1);
  assert.ok(Math.abs(target.x)>1,'ball handler goes around the defender rather than requiring a clear full-court layup');
  assert.equal(ctx.aiRouteBlocked(char,target),false);
  assert.deepEqual(char.group.position,original,'tactics choose only a target and never move or teleport a body');
  char.group.position.set(-2.1,0,ctx.HOOP_POS.z+8.6);char.bodyRadius=.638;defender.bodyRadius=.682;
  const direction=ctx.HOOP_POS.clone().sub(char.group.position).setY(0).normalize();
  defender.group.position.copy(char.group.position).addScaledVector(direction,1.35);
  ctx.getTeammates(char).forEach(player=>player.group.position.set(8,0,10));
  ctx.getOpponents(char).filter(player=>player!==defender).forEach(player=>player.group.position.set(-8,0,10));
  const escape=ctx.aiAdvanceToFrontcourt(char,direction);
  assert.ok(escape,'large bodies retain a lateral escape beyond the comfortable shot range');
  assert.ok(ctx.flatDistance(escape,char.group.position)>.7);
  assert.equal(ctx.aiRouteBlocked(char,escape),false);
});

test('a bobbled pass keeps the team clock and history, while a shot starts a fresh rebound possession',()=>{
  const ctx=fixture('motion',2);ctx.tick(4);const plan=ctx.aiGetTeamPlan(ctx.owner);
  ctx.ballState='loose';ctx.tick(.4);assert.equal(ctx.plans().offense[0],plan);
  ctx.ballState='dribble';ctx.owner=ctx.getTeammates(ctx.owner)[0];ctx.tick(.1);
  assert.equal(ctx.plans().offense[0],plan);assert.ok(plan.age>4);assert.equal(plan.passHistory.length,1);
  ctx.ballState='shot';ctx.tick(.1);assert.equal(ctx.plans().offense[0],undefined);
  ctx.ballState='loose';ctx.tick(.3);ctx.ballState='dribble';ctx.tick(.1);
  assert.notEqual(ctx.plans().offense[0],plan);assert.ok(ctx.plans().offense[0].age<1);
});

test('blocked cuts curl into an open route and failed screens, isolation and post actions reorganize',()=>{
  const cut=fixture('motion',2);cut.tick(.7);cut.offBall();
  const cutter=cut.getTeammates(cut.owner)[0],initial=cutter.aiBrain.target.clone();
  const defender=cut.getOpponents(cut.owner)[0];defender.group.position.copy(cutter.group.position).lerp(initial,.45);
  cut.offBall();
  assert.equal(cutter.aiBrain.role,'cut');assert.equal(cutter.aiBrain.route,'backdoor');
  assert.ok(cut.flatDistance(cutter.aiBrain.target,initial)>1);
  for(const style of ['pick_roll','isolation','post_up']){
    const ctx=fixture(style,2,['playmaker','interior']);ctx.tick(.01);
    const plan=ctx.plans().offense[0];
    if(style==='pick_roll') ctx.tick(3.4);
    else{plan.phase=style==='isolation'?'attack':'post';plan.phaseAge=5;ctx.tick(.25);assert.equal(plan.phase,'reset');ctx.tick(1.5);}
    assert.equal(plan.phase,'setup');assert.ok(plan.actorId);
    assert.ok(['motion','pick_roll','pick_pop','drive_kick','give_go','post_up','isolation'].includes(plan.action));
  }
  const transition=fixture('fast_break',2);transition.tick(.01);transition.tick(1);
  assert.notEqual(transition.plans().offense[0].action,'fast_break','retreated defense ends the transition action');
  const post=fixture('post_up',2,['playmaker','interior']);post.tick(.01);post.tick(4.8);
  assert.notEqual(post.plans().offense[0].action,'post_up','an interior player unable to establish deep position triggers a different action');
});

test('optional calls coordinate AI support, expire into ordinary reads, and leave human teammates alone',()=>{
  for(const size of [2,3]){
    const ctx=fixture('balanced',size,['playmaker','interior','sharpshooter']);
    assert.equal(ctx.aiRequestOffensiveAction(0,'screen'),true);
    let plan=ctx.aiGetTeamPlan(ctx.owner);assert.equal(plan.action,'pick_roll');
    assert.equal(ctx.aiRequestOffensiveAction(0,'cut'),true);ctx.offBall();
    plan=ctx.aiGetTeamPlan(ctx.owner);assert.equal(plan.action,'give_go');assert.equal(plan.phase,'cut');
    assert.equal(ctx.getTeammates(ctx.owner).filter(p=>p.aiBrain.role==='cut').length,1);
    assert.equal(ctx.aiRequestOffensiveAction(1,'cut'),false);
    ctx.tick(6);ctx.tick(2);ctx.tick(2);
    assert.notEqual(plan.phase,'cut');
  }
  const solo=fixture('balanced',1);assert.equal(solo.aiRequestOffensiveAction(0,'screen'),false);
  const coop=fixture('balanced',2);coop.getTeammates(coop.owner)[0].controllerSlot='p2';
  assert.equal(coop.aiRequestOffensiveAction(0,'cut'),false);
});
