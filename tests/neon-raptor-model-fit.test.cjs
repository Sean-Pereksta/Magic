const test=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const path=require('node:path');
const vm=require('node:vm');
const {actorFit,parseGLB,AUTHOR_SCALE,HUMAN_HEIGHT}=require('../game/neon-arena/neon-models.js');
const AI=require('../game/neon-arena/neon-survival-ai.js');

// Off-axis head and ungrounded author bounds exercise the fit's translation as
// well as its scale. Uploaded species use the same named head anchor contract.
function alienModel(authorScale=1){
  return {authorScale,height:2.4*authorScale,bounds:{min:[-.72*authorScale,.08*authorScale,-1.55*authorScale],max:[.72*authorScale,2.48*authorScale,1.45*authorScale]},anchors:{head:[.12*authorScale,1.05*authorScale,.94*authorScale]}};
}
function fitted(point,fit){return point.map((n,i)=>n*fit.scale[i]+fit.offset[i]);}
function close(actual,expected,message){assert.ok(Math.abs(actual-expected)<1e-10,`${message}: ${actual} vs ${expected}`);}

test('ground raptors gain 30 percent in every dimension and keep feet planted',()=>{
  for(const species of ['blue','yellow','red','green','stripe','purple','climber']){
    const model=alienModel(),actor={alien:true,alienType:species,alienScale:species==='climber'?.91:1.12,r:.62};
    const legacy=actorFit(model,actor,1.15),larger=actorFit(model,{...actor,alienVisualScale:1.30},1.15);
    const low=fitted(model.bounds.min,larger),high=fitted(model.bounds.max,larger),oldLow=fitted(model.bounds.min,legacy),oldHigh=fitted(model.bounds.max,legacy);
    close(low[1],0,`${species} soles remain grounded`);
    for(let axis=0;axis<3;axis++)close(high[axis]-low[axis],(oldHigh[axis]-oldLow[axis])*1.30,`${species} axis ${axis} grows uniformly`);
    const oldHead=fitted(model.anchors.head,legacy),head=fitted(model.anchors.head,larger);
    close(head[1],oldHead[1]*1.30,`${species} fitted head rises with body`);
    close(head[0],0,`${species} head remains centered across width`);
    close(head[2],0,`${species} head remains centered in depth`);
  }
});

test('the enlarged head follows the legacy targeting height measured from the feet',()=>{
  const model=alienModel(),actor={alien:true,alienScale:1.24,alienVisualScale:1.30,r:.71,y:3.7},eyeHeight=1.22;
  const head=fitted(model.anchors.head,actorFit(model,actor,eyeHeight));
  const feetY=actor.y-eyeHeight,legacyTargetHeight=Math.max(.30,eyeHeight-.45*actor.alienScale+actor.r*1.10*.62*.18);
  close(feetY+head[1],feetY+legacyTargetHeight*1.30,'head world Y matches scaled target sphere');
  const low=fitted(model.bounds.min,actorFit(model,actor,eyeHeight));
  close(feetY+low[1],feetY,'raised platform support surface is preserved');
});

test('bosses, flyers and specialists retain their existing fit without a size multiplier',()=>{
  for(const species of [...Object.keys(AUTHOR_SCALE),'flyer','rammer','spitter','burrower','commander']){
    const authorScale=AUTHOR_SCALE[species]||1,model=alienModel(authorScale),actor={alien:true,alienType:species,alienScale:authorScale,r:.75};
    const fit=actorFit(model,actor,1.5),explicitDefault=actorFit(model,{...actor,alienVisualScale:1},1.5);
    assert.deepEqual(fit,explicitDefault,`${species} default is unchanged`);
    close(fit.scale[0],1,`${species} author scale is removed exactly once`);
    close(fit.scale[2],1,`${species} depth is unchanged`);
    close(fitted(model.bounds.min,fit)[1],0,`${species} remains grounded`);
    const legacyHeight=Math.max(.30,1.5-.45*actor.alienScale+actor.r*1.10*.62*.18);
    close(fitted(model.anchors.head,fit)[1],legacyHeight,`${species} legacy target alignment is unchanged`);
  }
});

test('invalid visual scales use the legacy fit and human sizes stay independent',()=>{
  const model=alienModel(),actor={alien:true,alienScale:1,r:.6},legacy=actorFit(model,actor,1.15);
  for(const invalid of [0,-1,NaN,Infinity,'1.3',null])assert.deepEqual(actorFit(model,{...actor,alienVisualScale:invalid},1.15),legacy);
  const human={...model,bodyHeight:2.37},normal=actorFit(human,{},1.86,HUMAN_HEIGHT);
  assert.deepEqual(actorFit(human,{alienVisualScale:1.30},1.86,HUMAN_HEIGHT),normal);
});

function production(){
  const source=fs.readFileSync(path.join(__dirname,'../game/neon-arena/neon-arena.js'),'utf8');
  const slice=(start,end)=>{const a=source.indexOf(start),b=source.indexOf(end,a);assert.ok(a>=0&&b>a,`${start} production block exists`);return source.slice(a,b);};
  const constants=['TAU','HUMAN_VISUAL_HEIGHT','RAPTOR_VISUAL_SCALE'].map(name=>{const line=source.match(new RegExp('^const '+name+'=.*;$','m'));assert.ok(line,`${name} production constants exist`);return line[0];}).join('\n');
  const context={window:{NeonSurvivalAI:AI},rand:(lo,hi)=>(lo+hi)/2,WEAPONS:{pistol:{range:90}},lineClear:()=>true};
  vm.createContext(context);
  vm.runInContext(constants+'\n'+slice('function makeAlien(','function spawnAlienAtEdge(')+'\n'+slice('function alienVisualFactor(','function lineClear(')+'\n'+slice('function visibleToPlayer(','function angleDelta(')+'\n'+slice('function angleDelta(','function findMobileAimTarget('),context);
  return context;
}
function aimedRay(origin,point){const delta={x:point.x-origin.x,y:point.y-origin.y,z:point.z-origin.z},length=Math.hypot(delta.x,delta.y,delta.z);return {x:delta.x/length,y:delta.y/length,z:delta.z/length};}
const raptorKinds=['blue','yellow','red','green','stripe','purple','climber'];

test('production factory enlarges exactly the seven ground raptors and preserves simulation scale',()=>{
  const game=production();
  for(const kind of [...raptorKinds,'flyer',...Object.keys(AUTHOR_SCALE),...Object.keys(AI.SPECIES)]){
    const actor=game.makeAlien(kind,50),expected=raptorKinds.includes(kind)?1.30:1;
    close(actor.alienVisualScale,expected,`${kind} receives correct visual multiplier`);
    close(game.alienVisualFactor(actor),expected,`${kind} targeting uses that same multiplier`);
    const simulationScale=AI.speciesSpec(kind,50)?.scale;
    if(simulationScale)close(actor.alienScale,simulationScale*1.5,`${kind} simulation scale remains legacy`);
  }
});

test('production head and torso rays hit the enlarged feet-relative targets from steep tower angles',()=>{
  const game=production(),feet=3.25;
  for(const kind of raptorKinds){
    const actor=game.makeAlien(kind,25);actor.y=feet+1.86;actor.spawnGrace=0;
    const headY=game.alienTargetY(actor,actor.y-.45*actor.alienScale),bodyY=game.alienTargetY(actor,actor.y-.95*actor.alienScale);
    close(headY,feet+(1.86-.45*actor.alienScale)*1.30,`${kind} head scales from support surface`);
    close(bodyY,feet+(1.86-.95*actor.alienScale)*1.30,`${kind} body scales from support surface`);
    const origin={x:5,y:feet+18,z:-7},head={x:actor.x,y:headY,z:actor.z};
    const headHit=game.rayEntity(origin,aimedRay(origin,head),actor,40);
    assert.ok(headHit?.head,`${kind} steep head ray remains a headshot`);
    const wide=actor.r*1.10*1.30,torso={x:actor.x+wide*.8,y:bodyY,z:actor.z},tower={x:torso.x,y:feet+18,z:-3};
    const bodyHit=game.rayEntity(tower,aimedRay(tower,torso),actor,40);
    assert.ok(bodyHit&&!bodyHit.head,`${kind} steep lateral torso ray hits body without crossing head`);
  }
  const actor=game.makeAlien('blue',5);actor.spawnGrace=0;
  const legacySide=actor.r*1.10+.08*actor.alienScale,origin={x:legacySide*1.15,y:game.alienTargetY(actor,actor.y-.95*actor.alienScale),z:-14};
  assert.ok(game.rayEntity(origin,{x:0,y:0,z:1},actor,30),'new outer torso is hittable');
  assert.equal(game.rayEntity(origin,{x:0,y:0,z:1},{...actor,alienVisualScale:1},30),null,'outer torso ray was outside the previous silhouette');
});

test('production mobile aim and visibility sample enlarged head, torso and lower body from the feet',()=>{
  const game=production(),feet=2.5;
  for(const kind of raptorKinds){
    const target=game.makeAlien(kind,20);target.y=feet+1.86;target.spawnGrace=0;
    const samples=[];game.lineClear=(x1,y1,z1,x2,y2,z2)=>{samples.push(y2);return false;};
    assert.equal(game.visibleToPlayer(target,{x:0,y:feet+12.05,z:-5}),false);
    assert.equal(samples.length,3);
    for(let i=0;i<3;i++)close(samples[i],feet+(1.86-[.45,.86,1.34][i]*target.alienScale)*1.30,`${kind} visibility sample ${i}`);
    game.lineClear=()=>true;
    for(const offset of [.42,.86,1.34]){
      const aimY=feet+(1.86-offset*target.alienScale)*1.30,shooter={x:0,y:feet+12.05,z:-5,yaw:0,weapon:'pistol'};
      shooter.pitch=Math.atan2(shooter.y-.05-aimY,5);
      const info=game.mobileAimInfo(shooter,target,.18,.15);
      assert.ok(info,`${kind} mobile lock is available for sample ${offset}`);
      close(info.aimY,aimY,`${kind} mobile sample ${offset} follows enlarged body`);
      const eye={x:shooter.x,y:shooter.y-.05,z:shooter.z},ray=aimedRay(eye,{x:target.x,y:info.aimY,z:target.z});
      assert.ok(game.rayEntity(eye,ray,target,100),`${kind} mobile-directed shot reaches a hittable band`);
    }
  }
});

const realModels=path.resolve(__dirname,'../../original-pack/characters');
test('original uploaded raptor GLBs grow uniformly and their real head anchors remain targetable',{skip:!fs.existsSync(path.join(realModels,'alien_blue.glb'))},()=>{
  const game=production();
  for(const kind of raptorKinds){
    const model=parseGLB(fs.readFileSync(path.join(realModels,`alien_${kind}.glb`)),`characters/alien_${kind}.glb`),actor=game.makeAlien(kind,20);
    const large=actorFit(model,actor,1.86),old=actorFit(model,{...actor,alienVisualScale:1},1.86);
    for(let axis=0;axis<3;axis++)close((model.bounds.max[axis]-model.bounds.min[axis])*large.scale[axis],(model.bounds.max[axis]-model.bounds.min[axis])*old.scale[axis]*1.30,`${kind} real GLB axis ${axis}`);
    close(fitted(model.bounds.min,large)[1],0,`${kind} actual GLB soles stay grounded`);
    const head=fitted(model.anchors.head,large),target=game.alienTargetY(actor,actor.y-.45*actor.alienScale),headRadius=actor.r*1.10*.62*1.30;
    assert.ok(Math.abs(head[1]-target)<headRadius,`${kind} actual head anchor stays inside the enlarged head target`);
    actor.spawnGrace=0;
    const eye={x:0,y:12,z:-4},point={x:actor.x,y:head[1],z:actor.z};
    assert.ok(game.rayEntity(eye,aimedRay(eye,point),actor,30)?.head,`${kind} shot through the rendered head registers a headshot`);
  }
});
