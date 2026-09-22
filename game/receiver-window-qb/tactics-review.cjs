'use strict';
const fs=require('node:fs'),path=require('node:path');
const file=name=>path.join(__dirname,name);
function edit(name,fn){const original=fs.readFileSync(file(name),'utf8'),next=fn(original);if(next===original)throw Error('No changes in '+name);fs.writeFileSync(file(name),next);}
function swap(s,a,b){if(!s.includes(a))throw Error('Missing anchor: '+a.slice(0,140));return s.replace(a,b);}
edit('game.js',s=>{
  s=swap(s,"if(playState==='call'){updateFormationShift(dt,now);updatePreSnapDefense(dt,now);}","if(playState==='call'){updateFormationShift(dt,now);if(playState==='call')updatePreSnapDefense(dt,now);}");
  s=swap(s,'d.blockEngagement=null;d.blockTime=0;d.blockSlow=1;','d.blockEngagement=null;d.blockPose=0;d.blockTime=0;d.blockSlow=1;');
  s=swap(s,'b.blockPose=Math.max(.01,e.duration-e.elapsed);d.blockTime=b.blockPose;','b.blockPose=Math.max(.01,e.duration-e.elapsed);d.blockTime=b.blockPose;d.blockPose=b.blockPose;');
  s=swap(s,'    // The pair translates together: a weak defender cannot shove through a strong blocker.',`    // Restore shoulder spacing after third-party contact without overriding the strength contest.
    const overlap=Math.max(0,.40*(b.mesh.scale.x+d.mesh.scale.x)+.04-gap),separation=Math.min(overlap,dt*4);
    const resistance=THREE.MathUtils.clamp(.5+e.edge*.4,.15,.85);
    b.mesh.position.addScaledVector(normal,-separation*(1-resistance));d.mesh.position.addScaledVector(normal,separation*resistance);
    // The pair translates together: a weak defender cannot shove through a strong blocker.`);
  return s;
});
edit('gameplay-variety.test.cjs',s=>{
  const start=s.indexOf("test('blocking requires contact, expires and cannot immediately re-lock a defender'");
  const end=s.indexOf("test('rating physique controls",start);
  if(start<0||end<0)throw Error('Legacy block test not found');
  s=s.slice(0,start)+String.raw`test('blocking requires contact, expires and cannot immediately re-lock a defender',()=>{
  const c=game(),q=c.q;c.Math.random=()=>.999;
  q.run("setupPlay(false);ballCarrier=receivers[0];playState='run';receivers.forEach((r,i)=>r.mesh.position.set(i*10,0,0));defenders.forEach(d=>d.mesh.position.set(20,0,-30));receivers[1].mesh.position.set(0,0,-2);receivers[1].heading.set(0,0,-1);receivers[1].mesh.rotation.y=Math.PI;defenders[0].mesh.position.set(0,0,-5);defenders[0].heading.set(0,0,1);updateBlocking(.016)");
  const b=q.state().receivers[1],d=q.state().defenders[0];assert.equal(d.blockTime,0,'no block without contact');
  d.mesh.position.set(0,0,-2.9);q.run('updateBlocking(.016)');
  assert.ok(d.blockTime>0);assert.equal(d.blockEngagement,b.blockEngagement);
  for(let i=0;i<600&&d.blockEngagement;i++)q.run('advanceBlockEngagements(1/120)');
  assert.equal(d.blockTime,0);assert.ok(d.blockCooldown>0);
  q.run('updateBlocking(.016)');assert.equal(d.blockTime,0);
});
`+s.slice(end);
  s=swap(s,'b.mesh.position.set(0,0,0);b.blockPrevious=b.mesh.position.clone();d.mesh.position.set(0,0,-.55);r.mesh.position.set(0,0,.55);', 'b.mesh.scale.x=1;d.mesh.scale.x=1;r.mesh.scale.x=1;b.mesh.position.set(0,0,0);b.blockPrevious=b.mesh.position.clone();d.mesh.position.set(0,0,-.38);r.mesh.position.set(0,0,.38);');
  s+=String.raw`
test('a lead blocker releases a held engagement to receive a pass into his hands',()=>{
  const c=game(),q=c.q;c.Math.random=()=>.999;const {b,d}=arrangeTacticalBlock(q);
  q.run("ballCarrier=null;playState='thrown';ballLive=true;receivers[1].route='Lead';ball.position.copy(receivers[1].mesh.position).add(new THREE.Vector3(0,1.6,.5));ballVel.set(0,0,-10)");
  q.run('updateReceiver(receivers[1],1/120,gameTime)');
  assert.equal(b.blockEngagement,null);assert.equal(d.blockEngagement,null);assert.ok(b.blockCooldown>0);
});
test('a queued audible snap does not wait for every defensive reassignment to finish',()=>{
  const c=game(),q=c.q;c.Math.random=()=>.5;q.run("callPlay(plays.findIndex(p=>p.name==='Bunch Read Screen'))");
  q.state().defenders.forEach(d=>{d.setRead.readyAt=100000;});q.beginCountdown();
  for(let i=0;i<1800&&['call','countdown'].includes(q.state().playState);i++){q.run('gameTime+=1000/120');q.update(1/120,q.run('gameTime'));}
  assert.equal(q.state().playState,'live');assert.ok(q.state().defenders.every(d=>d.setRead.readyAt>q.run('gameTime')));
});
`;
  return s;
});
edit('menu-navigation.test.cjs',s=>{
  s=swap(s,'querySelectorAll:()=>[],addEventListener:',"querySelector:selector=>selector.startsWith('script[src=')?{}:null,querySelectorAll:()=>[],addEventListener:");
  s+=String.raw`
test('screen tuning loader gates play buttons and restores their prior disabled state',()=>{
  for(const event of ['onload','onerror']){
    let script;const buttons={startBtn:{disabled:false},continueBtn:{disabled:true},snapBtn:{disabled:false}};
    const document={body:{dataset:{}},activeElement:null,querySelector:()=>null,querySelectorAll:()=>[],getElementById:id=>buttons[id],createElement:()=>({}),head:{appendChild:s=>{script=s}},addEventListener(){}};
    vm.runInNewContext(fs.readFileSync(__dirname+'/menu-navigation.js','utf8'),{document,getComputedStyle:()=>({visibility:'visible'}),MutationObserver:class{observe(){}}});
    assert.ok(Object.values(buttons).every(b=>b.disabled));assert.equal(script.src,'receiver-window-qb/screen-playability.js');
    script[event]();assert.equal(buttons.startBtn.disabled,false);assert.equal(buttons.continueBtn.disabled,true);assert.equal(buttons.snapBtn.disabled,false);
  }
});
`;
  return s;
});
fs.appendFileSync(file('README.md'),`\n\n### Physical team audibles and strength blocking\n\nChoose another play in the pre-snap playbook to call its entire formation and assignments, including screens and lead blockers. The same receivers and defenders remain on the field. Receivers move into their new set with acceleration and turning limits; selecting Start while they move queues the snap and immediately hides the play menu. Once the offense is set, a team audible uses a short cadence instead of waiting for the defense. Motion plays retain the time needed for their motion paths.\n\nDefensive adjustments use delayed observed spacing, staggered reaction times and the existing coverage shell. Zone defenders shade their landmarks instead of knowing the called play; man defenders physically follow their assignments. Repeated audibles cannot restart an already-pending read. There is no defensive player or rating reroll.\n\nA block requires useful leverage, contact and the blocker facing the opponent. Both players stay in a shared engagement: relative strength controls duration, shedding rate, constrained tackle reach and drive direction. A defender without a strength advantage cannot obtain a random bull-rush through the held blocker. Stronger defenders can shed sooner or drive the pair backward; a stronger blocker can drive forward. Blocks always expire, lose effectiveness with time, release when leverage is lost, and have a re-engagement cooldown. A lead can release the block to catch an incoming pass.\n\nCarrier lane decisions use the same tackle reach as live contact checks, forecast crossing defenders and distinguish a physically protected block side from an exposed shoulder. Sidelines, pursuit, extra tacklers, and the existing retreat limits remain dangerous/authoritative. Existing screen outlet, burst and catching improvements are retained.\n\nValidation: run \`node --test game/receiver-window-qb/*.test.cjs\`. With Playwright installed, run \`node game/receiver-window-qb/tactics-browser.cjs\`; \`QB_CHROMIUM_PATH\` optionally selects the browser executable. This exercises desktop and mobile-touch layouts.\n`);
console.log('Updated sustained-contact fixtures, loader coverage and physical block spacing.');
