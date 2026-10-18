const test=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const path=require('node:path');
const vm=require('node:vm');

class Vector3{
  constructor(x=0,y=0,z=0){this.set(x,y,z);}
  set(x,y,z){this.x=x;this.y=y;this.z=z;return this;}
  setY(y){this.y=y;return this;}
  clone(){return new Vector3(this.x,this.y,this.z);}
  copy(v){return this.set(v.x,v.y,v.z);}
  addScaledVector(v,s){this.x+=v.x*s;this.y+=v.y*s;this.z+=v.z*s;return this;}
  sub(v){return this.addScaledVector(v,-1);}
  length(){return Math.hypot(this.x,this.y,this.z);}
  normalize(){const n=this.length()||1;this.x/=n;this.y/=n;this.z/=n;return this;}
  dot(v){return this.x*v.x+this.y*v.y+this.z*v.z;}
  lerp(v,t){this.x+=(v.x-this.x)*t;this.y+=(v.y-this.y)*t;this.z+=(v.z-this.z)*t;return this;}
}
function fixture(size=3){
  const ctx=vm.createContext({Math,THREE:{Vector3,MathUtils:{clamp:(n,a,b)=>Math.max(a,Math.min(b,n))}},
    HOOP_POS:new Vector3(),teamAITime:1,aiDefensePlans:{},ballState:'dribble',shotCharging:false,
    flatDistance:(a,b)=>Math.hypot(a.x-b.x,a.z-b.z),keepInCourt(){},
    aiIsHuman:p=>!!p.controllerSlot,getForward:a=>new Vector3(Math.sin(a),0,Math.cos(a))});
  const make=(id,teamId,x,z)=>({id,teamId,group:{position:new Vector3(x,0,z)},velocity:new Vector3(),
    angle:0,bodyRadius:.55,profile:{speed:1,width:1,shot:1,block:1},
    aiBrain:{role:'guard',target:new Vector3(),assignedId:null}});
  ctx.handler=make('handler',0,0,6);ctx.handler.controllerSlot='p1';
  ctx.offense=[ctx.handler,make('wing',0,-5,5),make('shooter',0,5,5)].slice(0,size);
  ctx.defense=[make('onball',1,0,4.7),make('helper',1,-3,3),make('denier',1,4,4)].slice(0,size);
  ctx.roster=[...ctx.offense,...ctx.defense];ctx.aiRoster=()=>ctx.roster;
  ctx.getPlayerById=id=>ctx.roster.find(p=>p.id===id);ctx.getBallHandler=()=>ctx.handler;
  ctx.getOpponents=p=>ctx.roster.filter(other=>other.teamId!==p.teamId);
  ctx.settings={defensiveIQ:1,courtVision:1,lookAhead:.4,readError:0};
  for(const file of ['ai-reads.js','ai-coordination.js','ai-defense.js'])
    vm.runInContext(fs.readFileSync(path.join(__dirname,'../js',file),'utf8'),ctx);
  ctx.read=()=>ctx.aiGetCoordinatedDefensePlan(1,ctx.handler,ctx.settings).coverage;
  return ctx;
}

test('shared coverage elects one helper, preserves distinct spots and does not depend on update order',()=>{
  for(const size of [2,3]) for(const reverse of [false,true]){
    const c=fixture(size);c.handler.velocity.set(3,0,-4);
    for(const d of reverse?[...c.defense].reverse():c.defense)c.aiTeamDefense(d,d.aiBrain,c.handler,c.settings);
    const coverage=c.read();assert.equal(new Set(Object.values(coverage.assignments)).size,size);
    assert.ok(c.defense.filter(d=>['rim_help','drop'].includes(d.aiBrain.role)).length<=1);
    for(let i=0;i<size;i++)for(let j=i+1;j<size;j++)
      assert.ok(c.flatDistance(coverage.targets[c.defense[i].id],coverage.targets[c.defense[j].id])>=1.5);
    assert.equal(coverage.primaryId,'onball');
  }
});

test('a planted screen switches two mobile defenders and preserves the exchange on following reads',()=>{
  const c=fixture(2),screener=c.offense[1],partner=c.defense[1];
  screener.aiScreen=true;screener.group.position.set(0,0,5.35);partner.group.position.set(1.1,0,5.4);
  const first=c.read();assert.equal(first.primaryId,partner.id);assert.equal(first.assignments.onball,screener.id);
  c.teamAITime+=.2;assert.equal(c.read().primaryId,partner.id);
  assert.equal(c.read().assignments[partner.id],c.handler.id);
  c.teamAITime+=1.2;assert.equal(c.read().primaryId,partner.id,'one planted screen cannot flip the same assignments back again');
});

test('a slow interior defender drops below a screen while the original guard keeps the ball',()=>{
  const c=fixture(2),screener=c.offense[1],partner=c.defense[1];
  screener.aiScreen=true;screener.group.position.set(0,0,5.35);partner.group.position.set(1.1,0,5.4);
  partner.profile.speed=.8;partner.profile.width=1.25;
  const coverage=c.read();assert.equal(coverage.primaryId,'onball');assert.equal(coverage.roles[partner.id],'drop');
  assert.ok(coverage.targets[partner.id].z<coverage.targets.onball.z);
});

test('a pass after a screen retains exchanged matchups and gives the receiver to their current defender',()=>{
  const c=fixture(2),oldHandler=c.handler,screener=c.offense[1],partner=c.defense[1];
  screener.aiScreen=true;screener.group.position.set(0,0,5.35);partner.group.position.set(1.1,0,5.4);
  assert.equal(c.read().primaryId,partner.id);
  screener.aiScreen=false;c.handler=screener;c.teamAITime+=.2;
  const coverage=c.read();assert.equal(coverage.primaryId,'onball');
  assert.equal(coverage.assignments.onball,screener.id);assert.equal(coverage.assignments.helper,oldHandler.id);
});

test('a human can visibly plant a physical screen without receiving a scripted action',()=>{
  const c=fixture(2),screener=c.offense[1],partner=c.defense[1];screener.controllerSlot='p2';
  screener.group.position.set(0,0,5.35);partner.group.position.set(1.1,0,5.4);
  const position=screener.group.position.clone(),target=screener.aiBrain.target.clone();
  assert.equal(c.read().primaryId,'onball','a momentary stationary body is not immediately declared a screen');
  c.teamAITime+=.2;assert.equal(c.read().primaryId,partner.id,'defense recognizes the visibly held obstruction');
  assert.deepEqual(screener.group.position,position);assert.deepEqual(screener.aiBrain.target,target);
  assert.equal(screener.controllerSlot,'p2');assert.equal(screener.aiScreen,undefined);
  screener.velocity.set(2,0,0);c.teamAITime+=.2;c.read();assert.equal(c.aiDefensePlans[1].screenSwitch,null);
});

test('a contained drive keeps valuable shooters covered and stops helping after the drive ends',()=>{
  const c=fixture();c.offense.slice(1).forEach(p=>p.profile.shot=1.5);c.handler.velocity.set(0,0,-3);
  assert.equal(c.read().helperId,null);
  c.handler.velocity.set(0,0,0);c.teamAITime+=.3;
  assert.equal(c.read().helperId,null);
  assert.ok(Object.values(c.read().roles).every(role=>!['rim_help','double'].includes(role)));
});

test('an already contained distant drive does not pull a defender away from an ordinary wing',()=>{
  const c=fixture();c.handler.group.position.set(0,0,8);c.handler.velocity.set(0,0,-3);
  c.defense[0].group.position.set(0,0,5.6);
  assert.equal(c.read().helperId,null);
});

test('a deliberate sideline trap sends exactly two defenders, rotates the third and then releases',()=>{
  const c=fixture();c.handler.group.position.set(7,0,6);
  c.defense[0].group.position.set(6.4,0,4.7);c.defense[1].group.position.set(3.2,0,4);
  c.defense[2].group.position.set(-2,0,5);c.offense[1].group.position.set(1,0,6);
  c.offense[2].group.position.set(-4,0,5);c.handler.angle=Math.PI/2;
  c.read();c.teamAITime+=1.3;c.aiDefensePlans[1].handlerAge=1.3;
  const coverage=c.read(),trap=c.aiDefensePlans[1].trap;
  assert.ok(trap);assert.equal(coverage.primaryId,'onball');
  assert.equal(Object.values(coverage.roles).filter(role=>role==='double').length,1);
  assert.equal(coverage.roles.denier,'pass_lane');
  assert.ok(c.flatDistance(coverage.targets.onball,coverage.targets.helper)>1.5);
  c.teamAITime+=1.1;assert.equal(c.read().helperId,null);assert.equal(c.aiDefensePlans[1].trap,null);
  assert.ok(c.aiDefensePlans[1].cooldownUntil>c.teamAITime);
});

test('observed outside shooting tightens the next closeout without changing player movement stats',()=>{
  const c=fixture(),normal=c.read().targets.onball.clone();
  const memory=c.aiDefenseMemory(c.aiDefensePlans[1],'handler');memory.shots=4;memory.outsideShots=4;
  c.teamAITime+=.2;const adjusted=c.read().targets.onball;
  assert.ok(c.flatDistance(adjusted,c.handler.group.position)<c.flatDistance(normal,c.handler.group.position));
  assert.equal(c.defense[0].profile.speed,1);
});

test('human defenders retain ownership and are never reassigned as screen or recovery helpers',()=>{
  const c=fixture(2),human=c.defense[1];human.controllerSlot='p2';
  c.offense[1].aiScreen=true;c.offense[1].group.position.set(0,0,5.35);human.group.position.set(1,0,5.4);
  c.handler.velocity.set(0,0,-3);const before=human.aiBrain.target.clone();
  assert.equal(c.aiTeamDefense(human,human.aiBrain,c.handler,c.settings),false);
  assert.deepEqual(human.aiBrain.target,before);assert.equal(c.read().primaryId,'onball');
  assert.equal(c.read().helperId,null);
});

test('tendencies are learned from visible drives and released passes, with bounded reversible shade',()=>{
  const c=fixture();c.read();
  for(let attempt=0;attempt<6;attempt++){
    const plan=c.aiDefensePlans[1];plan.observation=null;c.handler.group.position.set(0,0,8);
    c.handler.velocity.set(3,0,-3);c.aiObserveTeamDefense(c.handler,.1);
    c.handler.group.position.set(1.2,0,6.8);c.aiObserveTeamDefense(c.handler,.25);
  }
  const plan=c.aiDefensePlans[1],memory=plan.tendencies.handler;
  assert.ok(memory.drives>4);assert.ok(c.aiDefenseShade(plan,c.handler,c.settings)>.15);
  assert.ok(c.aiDefenseShade(plan,c.handler,c.settings)<=.42);
  c.ballState='pass';c.passFlight={passerId:'handler',targetId:'wing'};c.aiObserveTeamDefense(null,.1);
  assert.equal(memory.receivers.wing,1);assert.ok(memory.passes>0);
  for(let i=0;i<20;i++){c.aiDefenseEvent(memory,'drives');memory.left++;}
  assert.ok(c.aiDefenseShade(plan,c.handler,c.settings)<0,'countering the old pattern reverses the shade');
  assert.equal(c.aiDefenseShade(plan,c.handler,{defensiveIQ:.6}),0);
});

test('teammate steering yields to human routes and never mutates human input or position',()=>{
  const c=fixture(2),bot=c.defense[1],human=c.defense[0];human.controllerSlot='p2';
  bot.group.position.set(0,0,6);human.group.position.set(0,0,4.8);
  const before=human.group.position.clone(),direction=new Vector3(0,0,-1);
  const speed=c.aiSteerTeammates(bot,bot.aiBrain,direction);
  assert.ok(Math.abs(direction.x)>.1);assert.ok(speed<1);assert.deepEqual(human.group.position,before);
  assert.equal(c.aiMovementPriority(human,human.aiBrain),100);
});
