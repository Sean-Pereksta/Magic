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
  sub(v){this.x-=v.x;this.y-=v.y;this.z-=v.z;return this;}
  addScaledVector(v,s){this.x+=v.x*s;this.y+=v.y*s;this.z+=v.z*s;return this;}
  normalize(){const n=Math.hypot(this.x,this.y,this.z)||1;this.x/=n;this.y/=n;this.z/=n;return this;}
  dot(v){return this.x*v.x+this.y*v.y+this.z*v.z;}
}
const precise={courtVision:1,readError:0,lookAhead:.6};
function fixture(){
  const players=[];
  const ctx=vm.createContext({Math,THREE:{Vector3},HOOP_POS:new Vector3(0,3.28,-10.8),
    getOpponents:char=>players.filter(p=>p.teamId!==char.teamId),
    getTeammates:char=>players.filter(p=>p!==char&&p.teamId===char.teamId),
    keepInCourt:v=>{v.x=Math.max(-8.2,Math.min(8.2,v.x));v.z=Math.max(-12.2,Math.min(12.2,v.z));}});
  vm.runInContext(fs.readFileSync(path.join(__dirname,'../js/ai-reads.js'),'utf8'),ctx);
  const add=(id,teamId,x,z,profile={})=>{
    const p={id,teamId,group:{position:new Vector3(x,0,z)},velocity:new Vector3(),bodyRadius:.55,
      profile:{shot:1,layup:1,block:1,...profile},stamina:1,aiBrain:{readSeed:0}};
    players.push(p);return p;
  };
  return {ctx,add,players};
}

test('a clear running layup grades above an equally open moving jumper; stationary close finishes remain available',()=>{
  const {ctx,add}=fixture(),handler=add('handler',0,0,-7.6);
  handler.velocity.set(0,0,-3.5);
  const layup=ctx.aiAssessShot(handler,precise);
  assert.equal(layup.closeFinish,true);assert.equal(layup.finishOpen,true);assert.ok(layup.quality>.9);
  handler.group.position.z=-4.8;
  const jumper=ctx.aiAssessShot(handler,precise);
  assert.equal(jumper.closeFinish,false);assert.ok(jumper.quality<layup.quality-.2);
  handler.group.position.z=-8.2;handler.velocity.set(0,0,0);
  assert.equal(ctx.aiAssessShot(handler,precise).closeFinish,true);
});

test('paint help can close a finish even when the nearest defender trails the handler',()=>{
  const {ctx,add}=fixture(),handler=add('handler',0,0,-8.0);
  handler.velocity.set(0,0,-3);
  add('trailer',1,0,-5.8);const helper=add('helper',1,0,-10.1);
  const contested=ctx.aiAssessShot(handler,precise);
  assert.equal(contested.finishOpen,false);assert.ok(contested.rimPressure>.6);
  helper.group.position.set(0,0,-10.8);
  assert.equal(ctx.aiAssessShot(handler,precise).finishOpen,false,'a rim anchor at the exact endpoint still contests');
  helper.group.position.set(6,0,-9.0);
  const clear=ctx.aiAssessShot(handler,precise);
  assert.equal(clear.finishOpen,true);assert.ok(clear.quality>contested.quality+.2);
});

test('a body-clear shoulder edge keeps a direct drive open for both sides and different builds',()=>{
  for(const side of [-1,1]) for(const [radius,profile] of [[.5,{archetype:'slasher',layup:1.35}],[.55,{archetype:'sharpshooter',shot:1.4}],[.66,{archetype:'interior',height:1.2,layup:1.3}]]){
    const {ctx,add}=fixture(),handler=add('handler',0,0,-4.8,profile);
    handler.bodyRadius=radius;handler.velocity.set(0,0,-3.2);
    const guard=add('shoulder',1,side*(radius*2+.14),-4.65);
    guard.bodyRadius=radius;guard.velocity.set(0,0,-3.2);
    const before=JSON.stringify(handler.profile);
    const lane=ctx.aiFindDriveLane(handler,precise);
    assert.equal(lane.direct,true);assert.equal(lane.drive,true);assert.equal(lane.angleAdvantage,true);
    assert.ok(lane.openness>.85);assert.equal(lane.blocked,false);
    assert.equal(JSON.stringify(handler.profile),before,'boldness never changes physical player attributes');
  }
});

test('a running finish commits past a trailing shoulder but recognizes front bodies, rotating help and timed blocks',()=>{
  for(const side of [-1,1]){
    const {ctx,add}=fixture(),handler=add('handler',0,0,-7.75);
    handler.velocity.set(0,0,-3.2);
    const guard=add('shoulder',1,side*1.25,-7.6);guard.velocity.set(0,0,-3.2);
    const edge=ctx.aiAssessShot(handler,precise);
    assert.ok(edge.space<1.55,'the nearby body remains observable');
    assert.equal(edge.finishOpen,true,'an already beaten shoulder does not cancel a real gather opportunity');
    const target=new Vector3(0,0,-9.15);
    assert.ok(ctx.aiReadDrivePath(handler,target,target,precise).openness>.8);
    guard.velocity.set(-side*3,0,-3.2);
    assert.equal(ctx.aiAssessShot(handler,precise).finishOpen,false,'visible recovery across the lane closes the angle');
    guard.velocity.set(0,0,-3.2);guard.blockTime=.3;guard.onGround=false;
    assert.equal(ctx.aiAssessShot(handler,precise).finishOpen,false,'a live timed block remains a real contest');
    guard.blockTime=0;guard.onGround=true;guard.group.position.set(0,0,-8.85);guard.velocity.set(0,0,0);
    const front=ctx.aiReadDrivePath(handler,target,target,precise);
    assert.equal(front.blocked,true);assert.equal(ctx.aiAssessShot(handler,precise).finishOpen,false);
    guard.group.position.set(side*1.25,0,-7.6);guard.velocity.set(0,0,-3.2);
    add('rim_help',1,0,-10.0);
    assert.equal(ctx.aiAssessShot(handler,precise).finishOpen,false,'beating one defender does not erase a second rim protector');
    assert.ok(ctx.aiReadDrivePath(handler,target,target,precise).openness<.5);
  }
});

test('drive reads choose a clear direct lane, route around a primary defender, and reject crowded paint',()=>{
  const {ctx,add}=fixture(),handler=add('handler',0,0,-2.8);
  const open=ctx.aiFindDriveLane(handler,precise);
  assert.equal(open.drive,true);assert.equal(open.direct,true);assert.ok(open.openness>.95);
  const primary=add('primary',1,0,-6.1);
  const alternate=ctx.aiFindDriveLane(handler,precise);
  assert.equal(alternate.direct,false);assert.ok(Math.abs(alternate.target.x)>1.5);
  assert.ok(alternate.openness>ctx.aiReadDrivePath(handler,open.target,open.finish,precise).openness);
  primary.group.position.set(6,0,0);add('anchor',1,0,-9.3);add('left',1,-2,-8.8);add('right',1,2,-8.8);
  const closed=ctx.aiFindDriveLane(handler,precise);
  assert.equal(closed.drive,false);assert.ok(closed.openness<.5);
  assert.equal(closed.setupSafe,true);assert.ok(Math.abs(closed.setupTarget.x)>1.5,'closed paint moves to a safe side reset');
  assert.ok(closed.setupTarget.z>=handler.group.position.z-.05,'reset does not continue charging the paint');
  for(const defender of ctx.getOpponents(handler)){
    const gap=ctx.aiReadSegment(defender.group.position,handler.group.position,closed.setupTarget).distance;
    assert.ok(gap>handler.bodyRadius+defender.bodyRadius,'reset corridor clears actual bodies');
  }
});

test('a surrounded handler holds a safe spot instead of selecting a blocked reset route',()=>{
  const {ctx,add}=fixture(),handler=add('handler',0,0,-6.5);
  for(let i=0;i<8;i++) add(`wall_${i}`,1,Math.cos(i*Math.PI/4)*1.3,-6.5+Math.sin(i*Math.PI/4)*1.3);
  const reset=ctx.aiFindSetupSpace(handler,precise);
  assert.equal(reset.safe,false);assert.deepEqual(reset.target,handler.group.position);
});

test('immediate body collisions remain blocked even when perception has a large positional error',()=>{
  const {ctx,add}=fixture(),handler=add('handler',0,0,-6.8),defender=add('body',1,0,-7.8);
  const target=new Vector3(0,0,-9.0),finish=new Vector3(0,0,-9.1);
  const lane=ctx.aiReadDrivePath(handler,target,finish,{courtVision:.1,readError:2,lookAhead:.1});
  assert.equal(lane.blocked,true);assert.equal(lane.openness,0);
  assert.equal(defender.group.position.z,-7.8);
});

test('rim feeds require a real passing window and an unguarded catch under the basket',()=>{
  const {ctx,add}=fixture(),handler=add('handler',0,4,-4.8),receiver=add('roller',0,0,-8.7);
  const open=ctx.aiAssessPass(handler,receiver,precise);
  assert.equal(open.rimFeed,true);assert.equal(open.safe,true);assert.ok(open.value>.9);
  const defender=add('anchor',1,-.7,-9.1);
  const guarded=ctx.aiAssessPass(handler,receiver,precise);
  assert.equal(guarded.safe,false);assert.ok(guarded.value<open.value-.4);
  defender.group.position.set(2,0,-6.75);
  const intercepted=ctx.aiAssessPass(handler,receiver,precise);
  assert.equal(intercepted.safe,false);assert.ok(intercepted.clearance<.4);
});

test('higher vision anticipates a rotating interceptor and values the catch location of a cutter',()=>{
  const {ctx,add}=fixture(),handler=add('handler',0,-4,-5),receiver=add('receiver',0,4,-5);
  const defender=add('rotator',1,0,-3.8);defender.velocity.set(0,0,-4);
  const late=ctx.aiAssessPass(handler,receiver,{courtVision:.15,readError:0,lookAhead:.03});
  const early=ctx.aiAssessPass(handler,receiver,precise);
  assert.equal(late.safe,true);assert.equal(early.safe,false);assert.ok(early.clearance<late.clearance-.7);
  defender.group.position.set(0,0,2);defender.velocity.set(0,0,0);
  receiver.group.position.set(1.5,0,-7.1);receiver.velocity.set(-1,0,-3.5);
  const cutter=ctx.aiAssessPass(handler,receiver,precise);
  assert.ok(cutter.target.z<receiver.group.position.z-.4);assert.equal(cutter.rimFeed,true);
});

test('shot discipline distinguishes a skilled open jumper from contact, poor range, and sideways momentum',()=>{
  const {ctx,add}=fixture(),shooter=add('shooter',0,0,-3.3,{shot:1.55});
  const good=ctx.aiAssessShot(shooter,precise);assert.ok(good.quality>.85);assert.equal(good.inRange,true);
  shooter.profile.shot=.72;const badRange=ctx.aiAssessShot(shooter,precise);
  assert.equal(badRange.inRange,false);assert.ok(badRange.quality<good.quality-.35);
  shooter.profile.shot=1.55;add('contest',1,.6,-4.2);
  const contested=ctx.aiAssessShot(shooter,precise);assert.ok(contested.quality<good.quality-.3);
  shooter.velocity.set(5,0,0);
  assert.ok(ctx.aiAssessShot(shooter,precise).quality<contested.quality-.2);
});

test('a skilled clean three gets a modest point-value advantage while deep and contested threes do not',()=>{
  const {ctx,add}=fixture(),shooter=add('shooter',0,0,-3.3,{shot:1.55});
  const three=ctx.aiAssessShot(shooter,precise);
  assert.equal(three.points,3);assert.equal(three.inRange,true);
  assert.ok(Math.abs(three.value-three.quality-.065)<.00001);
  shooter.group.position.z=-4.8;
  const two=ctx.aiAssessShot(shooter,precise);
  assert.equal(two.points,2);assert.equal(two.value,two.quality);
  assert.ok(three.value>two.value+.06,'comparable clean looks reward the extra point');
  shooter.group.position.z=0;
  const deep=ctx.aiAssessShot(shooter,precise);
  assert.equal(deep.points,3);assert.equal(deep.inRange,false);assert.equal(deep.value,deep.quality);
  shooter.group.position.z=-3.3;add('contest',1,0,-4.7);
  const contested=ctx.aiAssessShot(shooter,precise);
  assert.equal(contested.points,3);assert.equal(contested.value,contested.quality);
  assert.ok(contested.value<two.value-.2);
  ctx.THREE_RADIUS=7.8;
  assert.equal(ctx.aiAssessShot(shooter,precise).points,2,'read uses the real court arc when provided');
});

test('pass values favor an equally clean skilled three, but prefer a sensible midrange look over a poor shooter beyond the arc',()=>{
  const {ctx,add}=fixture(),handler=add('handler',0,0,0);
  const receiver=add('receiver',0,0,-3.55,{shot:1.3});
  const three=ctx.aiAssessPass(handler,receiver,precise);
  receiver.group.position.z=-5;
  const midrange=ctx.aiAssessPass(handler,receiver,precise);
  assert.equal(three.points,3);assert.equal(midrange.points,2);
  assert.equal(three.safe,true);assert.equal(midrange.safe,true);
  assert.ok(three.value>midrange.value+.035,'equal-quality catch opportunities retain the three-point advantage after clamping');
  receiver.profile.shot=.72;receiver.group.position.z=-3.3;
  const poorThree=ctx.aiAssessPass(handler,receiver,precise);
  receiver.group.position.z=-6.3;
  const sensibleTwo=ctx.aiAssessPass(handler,receiver,precise);
  assert.equal(poorThree.catchValue,poorThree.catchQuality);
  assert.ok(sensibleTwo.value>poorThree.value+.3);
});

test('perception errors are stable for a brain, shrink on hard settings, and velocity projection is bounded',()=>{
  const {ctx,add}=fixture(),handler=add('handler',0,0,-2),defender=add('helper',1,1,-8);
  const easy={courtVision:.2,readError:1.7,lookAhead:.1};
  const first=ctx.aiReadPosition(handler,defender,easy),second=ctx.aiReadPosition(handler,defender,easy);
  assert.deepEqual(first,second);
  const hard=ctx.aiReadPosition(handler,defender,{courtVision:.95,readError:.05,lookAhead:.5});
  const gap=v=>Math.hypot(v.x-defender.group.position.x,v.z-defender.group.position.z);
  assert.ok(gap(hard)<gap(first)*.04);
  defender.velocity.set(100,0,0);
  const projected=ctx.aiReadPosition(handler,defender,precise,5);
  assert.ok(projected.x-defender.group.position.x<=4.8);
  assert.deepEqual(ctx.aiReadPosition(handler,defender,precise,5),ctx.aiReadPosition(handler,defender,precise,.6));
});

test('a big skilled finisher recognizes single small-guard contact at the rim without calling it open',()=>{
  const {ctx,add}=fixture();
  const big=add('sky_hook',0,0,-9.2,{height:1.16,width:1.02,layup:1.34,block:1.48});
  const small=add('flash',1,.9,-9.8,{height:.9,width:.84,layup:.82,block:.56});
  const contact=ctx.aiAssessShot(big,precise);
  assert.equal(contact.finishOpen,false,'a size edge never turns contact into an open finish');
  assert.equal(contact.finishFavorable,true);assert.equal(contact.finishDefender,small);
  assert.equal(contact.paintDefenders,1);assert.ok(contact.mismatchAdvantage>.5);
  assert.ok(contact.quality>.65&&contact.quality<.9,'the matchup earns a sensible attempt, not a guaranteed basket');
  small.profile={height:1.16,width:1.02,layup:1.34,block:1.48};
  const equal=ctx.aiAssessShot(big,precise);
  assert.equal(equal.finishFavorable,false);assert.equal(equal.mismatchAdvantage,0);
  assert.ok(equal.quality<contact.quality-.15);
  small.profile={height:1.24,width:1.16,layup:1.22,block:2.05};
  const protector=ctx.aiAssessShot(big,precise);
  assert.equal(protector.finishFavorable,false);assert.ok(protector.quality<contact.quality-.2);
});

test('size-aware rim reads respect well-timed blocks, help, fatigue, and distance',()=>{
  const {ctx,add}=fixture();
  const big=add('big',0,0,-9.2,{height:1.16,width:1.12,layup:1.4});
  const small=add('small',1,.9,-9.8,{height:.9,width:.84,block:.56});
  const available=ctx.aiAssessShot(big,precise);assert.equal(available.finishFavorable,true);
  small.blockTime=.28;small.onGround=false;
  const blocked=ctx.aiAssessShot(big,precise);
  assert.equal(blocked.activeBlockThreat,true);assert.equal(blocked.finishFavorable,false);
  assert.ok(blocked.finishRisk>available.finishRisk+.4);assert.ok(blocked.quality<available.quality-.2);
  small.blockTime=0;small.onGround=true;
  const helper=add('help',1,-.8,-10.1,{height:1.12,width:1.08,block:1.5});
  const doubled=ctx.aiAssessShot(big,precise);
  assert.equal(doubled.paintDefenders,2);assert.equal(doubled.finishFavorable,false);
  assert.ok(doubled.finishRisk>available.finishRisk+.25);
  helper.group.position.set(6,0,-5);big.stamina=.3;
  const tired=ctx.aiAssessShot(big,precise);
  assert.equal(tired.finishFavorable,false);assert.equal(tired.mismatchAdvantage,0);
  big.stamina=1;big.group.position.z=-8.3;
  assert.equal(ctx.aiAssessShot(big,precise).finishFavorable,false,'a 2.5m attempt is not a finish over a guard at the rim');
});

test('rim feeds can target a genuine size seal while actual interceptors and paint help still close the pass',()=>{
  const {ctx,add}=fixture(),passer=add('passer',0,4,-5);
  const receiver=add('center',0,0,-9.2,{height:1.16,width:1.12,layup:1.4});
  const small=add('guard',1,-1.0,-9.6,{height:.9,width:.84,block:.56});
  const seal=ctx.aiAssessPass(passer,receiver,precise);
  assert.equal(seal.finishOpen,false);assert.equal(seal.finishFavorable,true);
  assert.ok(seal.catchSpace<1.55);assert.ok(seal.clearance>.82);assert.equal(seal.safe,true);
  small.group.position.set(.8,0,-8.36);
  const intercepted=ctx.aiAssessPass(passer,receiver,precise);
  assert.ok(intercepted.clearance<.1);assert.equal(intercepted.safe,false,'height cannot excuse a pass through a defender');
  const easy={courtVision:.1,readError:4,lookAhead:.1};
  const noisyTarget=ctx.aiAssessPass(passer,receiver,easy).target;
  small.group.position.set((passer.group.position.x+noisyTarget.x)/2,0,(passer.group.position.z+noisyTarget.z)/2);
  assert.equal(ctx.aiAssessPass(passer,receiver,easy).safe,false,'a noisy matchup read cannot erase a body actually inside its passing corridor');
  small.group.position.set(-1,0,-9.6);add('helper',1,0,-10.4,{height:1.24,width:1.16,block:2.05});
  const help=ctx.aiAssessPass(passer,receiver,precise);
  assert.equal(help.finishFavorable,false);assert.equal(help.safe,false);
});

test('an empty driving corridor stays actionable when a defender is behind the rim, but a rim anchor or rotating body still closes it',()=>{
  const {ctx,add}=fixture(),handler=add('handler',0,0,-2.8);
  const behind=add('behind',1,0,-12.1);
  const available=ctx.aiFindDriveLane(handler,precise);
  assert.equal(available.direct,true);assert.equal(available.clearCorridor,true);
  assert.equal(available.drive,true);assert.ok(available.openness>.95);
  behind.group.position.set(0,0,-10.8);
  const anchor=ctx.aiReadDrivePath(handler,available.target,available.finish,precise);
  assert.equal(anchor.clearCorridor,true);assert.ok(anchor.openness<.62,'the rim anchor remains a real finishing threat');
  behind.group.position.set(6,0,-5);
  const rotator=add('rotator',1,1.8,-7.1);rotator.velocity.set(-3,0,0);
  const rotating=ctx.aiReadDrivePath(handler,available.target,available.finish,precise);
  assert.ok(rotating.openness<.4,'predicted help still closes the route before takeoff');
});
