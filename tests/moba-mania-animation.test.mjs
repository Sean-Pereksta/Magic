import test from 'node:test';
import assert from 'node:assert/strict';
import { HEROES, WORLD } from '../game/moba-mania/data.mjs';
import { Renderer } from '../game/moba-mania/renderer.mjs';
import { createEffectBudget } from '../game/moba-mania/combat-visuals.mjs';
import { unitPose, summonPose, drawHitGlint, drawMonsterRocks } from '../game/moba-mania/unit-animation.mjs';

const hero = (extra={}) => ({id:1,kind:'hero',heroId:'dragon',team:0,x:100,y:180,hp:100,maxHp:100,alive:true,r:17,range:50,angle:0,attackAnim:0,attackTimer:1,status:{},...extra});
const action = (extra={}) => ({kind:'attack',startedAt:10,impactAt:10,duration:.4,angle:0,fromX:100,fromY:180,toX:150,toY:180,...extra});
const gameFor = u => ({time:10,player:u,playerId:u.id,units:[u],effects:[],zones:[],canSee:()=>true,pointVisible:()=>true});

function context(){
  const calls=[],stack=[],c={globalAlpha:1,fillStyle:'#000',strokeStyle:'#000',lineWidth:1};
  c.save=()=>stack.push({globalAlpha:c.globalAlpha,fillStyle:c.fillStyle,strokeStyle:c.strokeStyle,lineWidth:c.lineWidth});
  c.restore=()=>{assert(stack.length,'canvas restore must match save');Object.assign(c,stack.pop());};
  for(const method of ['beginPath','closePath','moveTo','lineTo','arc','ellipse','translate','rotate','scale','setTransform','transform','fillRect','strokeRect','clearRect','fill','stroke','setLineDash','rect','clip','drawImage','fillText','strokeText']){
    c[method]=(...args)=>{
      for(const value of args.flat())if(typeof value==='number')assert(Number.isFinite(value),method+' requires finite geometry');
      assert(c.globalAlpha>=0&&c.globalAlpha<=1,'valid opacity');
      calls.push({method,args,alpha:c.globalAlpha,color:c.fillStyle});
    };
  }
  return {c,calls,balanced:()=>stack.length===0&&c.globalAlpha===1};
}

function canvasDocument(run){
  const previous=globalThis.document;let created=0;
  globalThis.document={createElement:()=>{created++;const {c}=context();return {width:1,height:1,getContext:()=>c};}};
  try{return run(()=>created);}finally{if(previous===undefined)delete globalThis.document;else globalThis.document=previous;}
}

test('instant attacks start on the strike and pass through follow-through and recovery without a late windup',()=>{
  const u=hero({visualAction:action()}),game=gameFor(u);
  assert.equal(unitPose(u,game,10).stage,2);
  assert.equal(unitPose(u,game,10.12).stage,3);
  assert.equal(unitPose(u,game,10.3).stage,4);
  assert.equal(unitPose(u,game,10.41).stage,0);
  const legacy=hero({attackAnim:.26});
  assert.equal(unitPose(legacy,game,10).stage,2,'older saves also strike immediately');
});

test('delayed monster and spell poses use real impact deadlines, with rocks released during the warning',()=>{
  const u=hero({visualAction:action({kind:'maw-bite',impactAt:10.75,duration:1.05})}),game=gameFor(u);
  assert.equal(unitPose(u,game,10).stage,1);
  assert.equal(unitPose(u,game,10.74).stage,1);
  assert.equal(unitPose(u,game,10.75).stage,2);
  assert.equal(unitPose(u,game,10.95).stage,4);
  u.visualAction=action({kind:'cast',abilityId:'sky-cleave',impactAt:10.35,duration:.67});
  assert.equal(unitPose(u,game,10.34).stage,1);
  assert.equal(unitPose(u,game,10.35).stage,2);
  assert(unitPose(u,game,10.35).cast);
  u.visualAction=action({kind:'colossus-rock',impactAt:10.8,duration:1.66});
  assert.equal(unitPose(u,game,10.2).stage,1,'the arm holds the rock before release');
  assert.equal(unitPose(u,game,10.3).stage,3,'the arm follows through while its rock flies');
});

test('anticipation only appears for a valid imminent attack, including player orders and vision',()=>{
  const u=hero({attackTimer:.1,targetId:2,isPlayer:true}),target=hero({id:2,team:1,x:145}),game=gameFor(u);
  game.units.push(target);game.attackOrder={targetId:2};
  assert.equal(unitPose(u,game).stage,1);
  game.attackOrder=null;assert.equal(unitPose(u,game).stage,0);
  game.attackHeld=true;assert.equal(unitPose(u,game).stage,1);
  game.canSee=()=>false;assert.equal(unitPose(u,game).stage,0);
  game.canSee=()=>true;target.x=500;assert.equal(unitPose(u,game).stage,0);
  target.x=145;target.team=0;assert.equal(unitPose(u,game).stage,0);
  target.team=1;target.hp=0;assert.equal(unitPose(u,game).stage,0);
  target.hp=100;game.isProtected=()=>true;assert.equal(unitPose(u,game).stage,0);
  game.isProtected=()=>false;u.recallIn=2;assert.equal(unitPose(u,game).stage,0);
  u.recallIn=0;u.status.stun={remaining:1};assert.equal(unitPose(u,game).stage,0);
});

test('all eight heroes have distinct bounded body motion and drawing cannot move collision positions',()=>{
  const signatures=[];
  for(const heroId of Object.keys(HEROES)){
    const u=hero({heroId,visualAction:action(),visualHit:{at:9,angle:.5}}),game=gameFor(u),before=JSON.stringify(u);
    const pose=unitPose(u,game,10);
    signatures.push(JSON.stringify([pose.dx,pose.dy,pose.tilt]));
    for(let i=0;i<60;i++){
      u.visualAction.angle=i*.173;
      const p=unitPose(u,game,10+i*.006);
      assert(p.direction>=0&&p.direction<8&&Number.isInteger(p.direction));
      assert(Math.abs(p.dx)<=9&&Math.abs(p.dy)<=7&&Math.abs(p.tilt)<=.12);
      assert.deepEqual(unitPose(u,game,10+i*.006),p,'repainting is deterministic');
      const still=unitPose(u,game,10+i*.006,true);
      assert.equal(still.dx,0);assert.equal(still.dy,0);assert.equal(still.tilt,0);assert.equal(still.frame,0);
    }
    u.visualAction.angle=0;assert.equal(JSON.stringify(u),before);
  }
  assert.equal(new Set(signatures).size,8);
});

test('summons rise, assemble or leap by species and reduced motion removes large entrance movement',()=>{
  const pose=subtype=>summonPose({subtype,visualSpawn:{at:10,duration:1,subtype}},10.5);
  assert(pose('treant').rise>0);assert.equal(pose('treant').lift,0);
  assert(pose('spirit').assemble);assert.equal(pose('spirit').lift,0);
  assert(pose('wolf').lift>pose('bear').lift);assert(pose('bear').lift>0);
  for(const subtype of ['treant','spirit','wolf','bear']){
    const u={subtype,visualSpawn:{at:10,duration:1,subtype}},before=JSON.stringify(u),reduced=summonPose(u,10.5,true);
    assert.equal(reduced.lift,0);assert.equal(reduced.rise,0);assert(!reduced.assemble);
    assert(reduced.alpha>0&&reduced.alpha<=1);
    assert.equal(summonPose(u,11.1).active,false);assert.equal(summonPose(u,9).active,false);
    assert.equal(JSON.stringify(u),before);
  }
  assert.equal(summonPose({},10).alpha,1,'legacy units are fully visible');
});

test('hit flinches expire quickly and their outline shares the effect budget without leaking canvas state',()=>{
  const u=hero({visualHit:{at:10,angle:Math.PI,power:1}}),game=gameFor(u),before=JSON.stringify(u),budget={remaining:1};
  const {c,calls,balanced}=context();drawHitGlint(c,u,unitPose(u,game,10.04),60,66,budget);
  assert.equal(budget.remaining,0);assert(calls.some(v=>v.method==='fillRect'));assert(balanced());
  const count=calls.length;drawHitGlint(c,u,unitPose(u,game,10.04),60,66,budget);assert.equal(calls.length,count);
  drawHitGlint(c,u,unitPose(u,game,10.2),60,66,{remaining:5});assert.equal(calls.length,count);
  assert.equal(JSON.stringify(u),before);
});

test('Colossus rocks follow existing warning zones, stop at impact, obey fog and consume bounded work',()=>{
  const owner=hero({id:7,kind:'beast',subtype:'colossus',team:-1}),game=gameFor(owner);
  game.player={team:0};game.zones=[{type:'colossus-rock',ownerId:7,x:300,y:180,fromX:100,fromY:180,maxDelay:.8,tickIn:.7}];
  const {c,calls,balanced}=context(),budget={remaining:1};
  assert.equal(drawMonsterRocks(c,game,10,()=>true,budget),0,'rock stays in the raised hand initially');
  game.zones[0].tickIn=.3;const before=JSON.stringify(game);
  assert.equal(drawMonsterRocks(c,game,10.5,()=>true,budget),1);
  assert.equal(budget.remaining,0);assert(balanced());assert.equal(JSON.stringify(game),before);
  assert.equal(drawMonsterRocks(c,game,10.5,()=>true,budget),0);
  const count=calls.length;game.canSee=()=>false;
  assert.equal(drawMonsterRocks(c,game,10.5,()=>true,{remaining:10}),0);
  game.canSee=()=>true;game.pointVisible=()=>false;
  assert.equal(drawMonsterRocks(c,game,10.5,()=>true,{remaining:10}),0);
  game.pointVisible=()=>true;game.zones[0].tickIn=0;
  assert.equal(drawMonsterRocks(c,game,10.8,()=>true,{remaining:10}),0);
  assert.equal(calls.length,count,'hidden or already detonated attacks create no extra rock');
});

test('Assassin afterimages follow actual movement skills, never basic attacks or unmoving casts',()=>canvasDocument(()=>{
  const u=hero({heroId:'ember',visualAction:action({toX:240})}),game=gameFor(u);
  const render=(reduced=false)=>{
    const {c,calls,balanced}=context(),renderer={effectBudget:createEffectBudget(1280,reduced),label(){}};
    Renderer.prototype.unit.call(renderer,c,u,game);assert(balanced());
    return calls.filter(v=>v.method==='drawImage').length;
  };
  assert.equal(render(),1,'basic strike has one body');
  u.visualAction=action({kind:'cast',abilityId:'blade-fan',toX:240});assert.equal(render(),1);
  u.visualAction=action({kind:'cast',abilityId:'cinder-dash',toX:240});assert.equal(render(),3);
  assert.equal(render(true),1,'reduced motion omits dash ghosts');
  game.pointVisible=()=>false;assert.equal(render(),1,'afterimages never reveal an unseen path');
  game.pointVisible=()=>true;u.visualAction.toX=u.visualAction.fromX;assert.equal(render(),1,'blocked movement leaves no false dash');
}));

test('sprite poses reuse quantized canvases and evict older cosmetic variants during a long session',()=>canvasDocument(created=>{
  const {c}=context(),renderer={effectBudget:createEffectBudget(),label(){}},u=hero({heroId:'arc',visualAction:action(),cosmetics:{armorTint:'#010101'}}),game=gameFor(u);
  Renderer.prototype.unit.call(renderer,c,u,game);const first=created();
  for(let i=0;i<30;i++){game.time=10+i*.001;Renderer.prototype.unit.call(renderer,c,u,game);}
  assert.equal(created(),first,'elapsed animation time does not create continuous sprite keys');
  for(let i=0;i<1201;i++){
    u.cosmetics.armorTint='#'+(0x100000+i).toString(16);Renderer.prototype.unit.call(renderer,c,u,game);
  }
  const full=created();Renderer.prototype.unit.call(renderer,c,u,game);assert.equal(created(),full,'newest cached pose is reused');
  u.cosmetics.armorTint='#010101';Renderer.prototype.unit.call(renderer,c,u,game);
  assert.equal(created(),full+1,'old cosmetic canvases are evicted at the fixed cache ceiling');
}));

test('renderer passes visible units into reactive terrain and keeps discovered tower rubble visible',()=>{
  const patch=WORLD.brush[0],u=hero({moving:true,x:patch.x,y:patch.y}),dead=hero({id:2,kind:'tower',team:1,hp:0,alive:false,x:patch.x+50,y:patch.y});
  const game=gameFor(u);game.units.push(dead);game.canSee=(team,v)=>v.alive;game.projectiles=[];game.texts=[];
  const draw=visible=>{
    const {c,calls}=context(),drawn=[];
    const renderer={ctx:c,canvas:{width:1000,height:700},width:1000,height:700,camera:{x:0,y:0,width:1000,height:700},terrain:{},visibleEffects:[],effectBudget:createEffectBudget(),
      follow(){},visible(x,y){return visible&&Number.isFinite(x)&&Number.isFinite(y);},drawWater(){},drawWells(){},drawObjectiveGround(){},drawCombatIndicators(){},unit(c,v){drawn.push(v.id);},projectile(){},effect(){},drawFog(){},drawTelegraphs(){},drawAim(){},unitBars(){},floatText(){},playerMarker(){}};
    Renderer.prototype.draw.call(renderer,game);return {calls,drawn};
  };
  const hidden=draw(false),shown=draw(true);
  assert(shown.calls.length>hidden.calls.length+10,'terrain accents receive a unit object and draw around its real position');
  assert(shown.drawn.includes(dead.id),'visible destroyed enemy towers retain their rubble');
  game.pointVisible=()=>false;assert(!draw(true).drawn.includes(dead.id),'unseen enemy rubble does not reveal a destruction');
});
