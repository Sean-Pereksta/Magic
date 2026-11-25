import test from 'node:test';
import assert from 'node:assert/strict';
import { Game } from '../game/moba-mania/engine.mjs';
import { Renderer } from '../game/moba-mania/renderer.mjs';
import { createEffectBudget, drawCombatBurst, drawProjectileTrail, drawShieldShell, drawSpellAccent, isEffectVisible } from '../game/moba-mania/combat-visuals.mjs';

// Observe the warning countdown without requiring a browser or a native canvas.
function countdowns(game) {
  const arcs=[];
  const context={save(){},restore(){},setLineDash(){},beginPath(){},moveTo(){},lineTo(){},closePath(){},fill(){},stroke(){},arc(x,y,r,start,end){if(r===13)arcs.push((end-start)/(Math.PI*2));}};
  Renderer.prototype.drawTelegraphs.call({visible:()=>true,label(){}},context,game);
  return arcs;
}

test('delayed zone warnings fill with the real countdown and stop after their first impact',()=>{
  for(const [hero,slot] of [['ranger',2],['arc',0]]) {
    const game=new Game({hero,seed:73}),player=game.player;
    game.units=[player];game.campStates=[];game.waveIn=game.beastIn=game.planIn=1e8;
    player.level=8;game.refreshStats(player);player.mana=player.maxMana;game.rebuildGrid();
    assert(game.cast(player,slot,{x:player.x,y:player.y}));
    assert.deepEqual(countdowns(game),[0]);
    game.tickZones(.275);
    assert(Math.abs(countdowns(game)[0]-.5)<1e-9,'the warning reaches half-full halfway to the blast');
    game.tickZones(.3);
    assert.deepEqual(countdowns(game),[],'the initial warning disappears after detonation');
    if(hero==='arc') {
      assert.equal(game.zones.length,1,'storm continues dealing damage after its warning ends');
      game.tickZones(.1);
      assert.deepEqual(countdowns(game),[],'later storm pulses do not restart the initial warning');
    }
  }
});

test('beast shrine labels do not disclose an objective outside allied vision',()=>{
  const game=new Game({hero:'arc',seed:73});
  game.units=[game.player];game.rebuildGrid();
  const beast=game.spawnBeast(0),labels=[];
  const context={save(){},restore(){},setLineDash(){},beginPath(){},moveTo(){},lineTo(){},stroke(){}};
  const draw=()=>{
    labels.length=0;
    Renderer.prototype.drawObjectiveGround.call({visible:()=>true,label(c,text){labels.push(text);}},context,game);
    return labels.includes('CLAIM THE BEAST');
  };
  assert.equal(game.canSee(0,beast),false);
  assert.equal(draw(),false);
  game.player.x=beast.x-100;game.player.y=beast.y;game.rebuildGrid();
  assert.equal(game.canSee(0,beast),true);
  assert.equal(draw(),true);
});

// Record actual drawing primitives, with enough canvas state to catch opacity
// and save/restore mistakes, without relying on browser-specific raster output.
function pixelContext(){
  const calls=[],stack=[],c={globalAlpha:1,fillStyle:'#000000',strokeStyle:'#000000',lineWidth:1};
  c.save=()=>stack.push({globalAlpha:c.globalAlpha,fillStyle:c.fillStyle,strokeStyle:c.strokeStyle,lineWidth:c.lineWidth});
  c.restore=()=>{assert(stack.length,'canvas restore needs a matching save');Object.assign(c,stack.pop());};
  for(const method of ['beginPath','closePath','moveTo','lineTo','arc','ellipse','translate','rotate','scale','setTransform','transform','fillRect','strokeRect','fill','stroke','setLineDash']){
    c[method]=(...args)=>{
      for(const value of args.flat())if(typeof value==='number')assert(Number.isFinite(value),method+' needs finite coordinates');
      assert(Number.isFinite(c.globalAlpha)&&c.globalAlpha>=0&&c.globalAlpha<=1,'drawing opacity stays in range');
      calls.push({method,args,alpha:c.globalAlpha,fill:c.fillStyle,stroke:c.strokeStyle});
    };
  }
  return {c,calls,balanced:()=>stack.length===0&&c.globalAlpha===1};
}

const burst={type:'combat-burst',fx:'hit',theme:'fire',x:120,y:180,r:35,color:'#ff9e5b',life:.25,maxLife:.38,power:1.2,angle:.6,seed:918};

test('pixel effects are deterministic, finite and restore canvas state across their whole lifetime',()=>{
  const render=()=>{
    const context=pixelContext();
    for(const theme of ['steel','fire','arcane','nature','poison','holy','frost','violet','stone']){
      const budget=createEffectBudget(1280);
      for(const fraction of [1,.65,.08])for(const ground of [true,false])drawCombatBurst(context.c,{...burst,theme,life:burst.maxLife*fraction},2,ground,budget);
    }
    assert(context.calls.some(c=>c.method==='fillRect'),'impacts use actual pixel primitives');
    assert(context.balanced(),'effect drawing cannot leak opacity into later sprites or health bars');
    return context.calls;
  };
  assert.deepEqual(render(),render(),'repainting a paused frame must not reshuffle its particles');
});

test('desktop, mobile and reduced-motion effects have bounded particle work',()=>{
  const desktop=createEffectBudget(1280),mobile=createEffectBudget(390),reduced=createEffectBudget(1280,true);
  assert(mobile.limit<desktop.limit,'small screens use fewer decorative particles');
  assert(reduced.limit<desktop.limit,'reduced motion lowers the effect density');
  const exhaust=budget=>{
    const {c,calls,balanced}=pixelContext();let emitted=0;
    for(let i=0;i<150;i++){
      for(const ground of [true,false])emitted+=drawCombatBurst(c,{...burst,seed:i},2,ground,budget);
      assert(budget.remaining>=0,'no event can overdraw the remaining pool');
    }
    assert(Number.isFinite(emitted)&&emitted<=budget.limit,'a long fight cannot exceed the per-frame burst allowance');
    assert(balanced());return calls.length;
  };
  const desktopWork=exhaust(desktop),mobileWork=exhaust(mobile),reducedWork=exhaust(reduced);
  assert(mobileWork<desktopWork);assert(reducedWork<desktopWork);
});

test('shields are transparent, visually distinct by source, and absent without real protection',()=>{
  const unit={id:7,kind:'hero',heroId:'dragon',x:120,y:180,r:17,hp:100,alive:true,shield:90,shieldTime:3,status:{}};
  const signatures=[];
  for(const theme of ['fire','frost','violet','arcane','holy','nature']){
    const {c,calls,balanced}=pixelContext(),budget=createEffectBudget(1280);
    const shielded={...unit,shieldVisual:{theme,startedAt:0,amount:100}};
    assert(drawShieldShell(c,shielded,1,64,84,false,budget));
    assert(drawShieldShell(c,shielded,1,64,84,true,budget));
    const fills=calls.filter(call=>call.method==='fill');
    assert(fills.some(call=>call.alpha>0&&call.alpha<.4),'the shield body remains translucent');
    signatures.push(JSON.stringify(calls));assert(balanced());
  }
  assert.equal(new Set(signatures).size,signatures.length,'different shield sources do not share the same shell design');

  for(const inactive of [{...unit,shield:0,shieldVisual:{theme:'fire'}},{...unit,alive:false},{...unit,hp:0}]){
    const {c,calls}=pixelContext();
    assert.equal(drawShieldShell(c,inactive,1,64,84,true,createEffectBudget()),false);
    assert.equal(calls.length,0,'inactive or dead heroes should not retain a visible bubble');
  }
  const legacy=pixelContext();
  assert(drawShieldShell(legacy.c,unit,1,64,84,true,createEffectBudget()),'old saves without shieldVisual still draw their active shield');
});

test('new unit-bound effects respect brush visibility, fog and visible death feedback',()=>{
  const unit={id:7,alive:true,hp:100,team:1,visible:false},game={units:[unit],pointVisible:()=>true,canSee:(team,u)=>u.visible};
  const impact={...burst,visibilityId:unit.id,visibleTo:[0]};
  assert.equal(isEffectVisible(game,0,impact),false,'a visible ground point must not expose a unit hidden in brush');
  unit.visible=true;assert.equal(isEffectVisible(game,0,impact),true);
  game.pointVisible=()=>false;assert.equal(isEffectVisible(game,0,impact),false,'previously seen impacts disappear under fog');
  game.pointVisible=()=>true;unit.alive=false;unit.visible=false;
  assert.equal(isEffectVisible(game,0,impact),true,'an observed kill keeps its brief death burst');
  assert.equal(isEffectVisible(game,0,{...impact,visibleTo:[1]}),false,'an unseen kill remains hidden');
  game.units=[];assert.equal(isEffectVisible(game,0,impact),true,'a removed corpse keeps only feedback already visible when emitted');
  game.units=[unit];unit.alive=true;
  assert.equal(isEffectVisible(game,0,{...burst,followId:unit.id}),false,'attached spell halos obey the same unit visibility');
});

test('projectile tails and spell accents share bounded decoration work and finite geometry',()=>{
  const {c,calls,balanced}=pixelContext(),budget=createEffectBudget(390);
  for(let i=0;i<80;i++){
    for(const type of ['arrow','bolt','dragon','lance','poison','longshot'])drawProjectileTrail(c,{id:i,type,x:120,y:180,prevX:105,prevY:170,angle:.6,color:'#ff9e5b',theme:'fire',r:8},2,budget);
    for(const type of ['explosion','frost','quake','storm','flame','dash'])for(const ground of [true,false])drawSpellAccent(c,{...burst,type},2,ground,budget);
    assert(budget.remaining>=0&&budget.trailRemaining>=0&&budget.accentRemaining>=0);
  }
  assert(calls.some(call=>call.method==='fillRect'),'projectiles and spells receive decorative pixels');
  const before=calls.length;
  drawProjectileTrail(c,{id:1,type:'dragon',x:120,y:180,angle:0,color:'#ff9e5b'},2,budget);
  drawSpellAccent(c,{...burst,type:'explosion'},2,false,budget);
  assert.equal(calls.length,before,'exhausted decoration pools stop drawing additional particles');
  assert(balanced());
});
