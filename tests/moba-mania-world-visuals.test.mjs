import test from 'node:test';
import assert from 'node:assert/strict';
import { WORLD } from '../game/moba-mania/data.mjs';
import { createEffectBudget } from '../game/moba-mania/combat-visuals.mjs';
import { drawCampScenery, drawReactiveTerrain, drawStructureDamage, drawWorldEffect, drawGoldFlight, isWorldEffect } from '../game/moba-mania/world-visuals.mjs';

function context(){
  const calls=[],stack=[],c={globalAlpha:1,fillStyle:'#000',strokeStyle:'#000',lineWidth:1};
  c.save=()=>stack.push({globalAlpha:c.globalAlpha,fillStyle:c.fillStyle,strokeStyle:c.strokeStyle,lineWidth:c.lineWidth});
  c.restore=()=>{assert(stack.length,'balanced canvas state');Object.assign(c,stack.pop());};
  for(const method of ['beginPath','closePath','moveTo','lineTo','stroke','fill','fillRect','drawImage'])c[method]=(...args)=>{
    for(const n of args)if(typeof n==='number')assert(Number.isFinite(n),method+' finite geometry');
    assert(c.globalAlpha>=0&&c.globalAlpha<=1,'valid opacity');
    calls.push({method,args,alpha:c.globalAlpha,color:method==='stroke'?c.strokeStyle:c.fillStyle});
  };
  return {c,calls,paint:()=>calls.filter(v=>['fill','stroke','fillRect','drawImage'].includes(v.method)),balanced:()=>stack.length===0&&c.globalAlpha===1};
}

const effect=(type,extra={})=>({type,x:160,y:220,r:55,createdAt:10,life:1,maxLife:1,theme:'nature',seed:73,team:0,...extra});

test('camp scenery distinguishes the three habitats and is independent of occupancy or time',()=>{
  const signatures=[];
  for(const type of ['wolf','boar','golem']){
    const first=context(),second=context();
    drawCampScenery(first.c,{x:120,y:180,type});
    drawCampScenery(second.c,{x:120,y:180,type,spawned:false,respawnIn:43});
    assert.deepEqual(first.calls,second.calls,'camp occupancy does not change public scenery');
    assert(first.paint().length>25,'each habitat has authored detail');
    assert(first.balanced());signatures.push(JSON.stringify(first.calls));
  }
  assert.equal(new Set(signatures).size,3,'three different habitats');
});

test('reactive vegetation and footfalls cannot reveal concealed units',()=>{
  const patch=WORLD.brush[0],unit={id:1,alive:true,moving:true,kind:'hero',team:1,x:patch.x,y:patch.y,angle:0};
  const game={player:{team:0},units:[unit],effects:[],canSee:()=>false,pointVisible:()=>true};
  const hidden=context(),budget=createEffectBudget();
  assert.equal(drawReactiveTerrain(hidden.c,game,2,()=>true,budget),0);
  assert.equal(hidden.paint().length,0);assert.equal(budget.remaining,budget.limit);
  game.canSee=()=>true;const visible=context();
  assert(drawReactiveTerrain(visible.c,game,2,()=>true,budget)>0);assert(visible.paint().length>0);
  const reduced=context();assert.equal(drawReactiveTerrain(reduced.c,game,2,()=>true,createEffectBudget(390,true)),0);
  assert.equal(reduced.paint().length,0);assert(visible.balanced());assert(hidden.balanced());
});

test('impact brush responses preserve visibility after a hidden target dies or is removed',()=>{
  const patch=WORLD.brush[0],target={id:9,kind:'minion',team:1,alive:false,hp:0,x:patch.x,y:patch.y};
  const mark=effect('ground-mark',{x:patch.x,y:patch.y,r:50,visibilityId:target.id,visibleTo:[1]});
  const game={player:{team:0},units:[target],effects:[mark],canSee:()=>false,pointVisible:()=>true};
  for(const units of [[target],[]]){
    game.units=units;
    const hidden=context(),budget=createEffectBudget();
    assert.equal(drawReactiveTerrain(hidden.c,game,10.1,()=>true,budget),0);
    assert.equal(hidden.paint().length,0,'concealed deaths do not rustle visible brush');
    assert.equal(budget.remaining,budget.limit);assert(hidden.balanced());
  }
  mark.visibleTo=[0];const seen=context();
  assert(drawReactiveTerrain(seen.c,game,10.1,()=>true,createEffectBudget())>0,'witnessed impacts still disturb brush after removal');
  assert(seen.paint().length>0);assert(seen.balanced());
});

test('terrain responses remain bounded in a crowded visible fight',()=>{
  const units=Array.from({length:500},(_,id)=>({id,kind:'beast',alive:true,moving:true,team:0,x:1600,y:1100,angle:0}));
  const game={player:{team:0},units,effects:[],canSee:()=>true};
  const c=context(),budget=createEffectBudget(390),before=JSON.stringify(game);
  drawReactiveTerrain(c.c,game,15,()=>true,budget);
  assert(budget.remaining>=0);assert.equal(budget.accentRemaining,0);
  assert(c.paint().length<=180,'ambient decorations stop at their allowance');
  assert.equal(JSON.stringify(game),before,'rendering does not alter unit movement');assert(c.balanced());
});

test('visible angel takeoff lifts grass briefly even before normal movement begins',()=>{
  const patch=WORLD.brush[0],unit={id:1,kind:'hero',heroId:'seraph',team:0,alive:true,moving:false,x:patch.x+150,y:patch.y,visualAction:{kind:'cast',abilityId:'wingburst',startedAt:10,fromX:patch.x,fromY:patch.y}};
  const game={player:{team:0},units:[unit],effects:[],canSee:()=>true,pointVisible:()=>true};
  const takeoff=context();assert(drawReactiveTerrain(takeoff.c,game,10.15,()=>true,createEffectBudget())>0);
  assert(takeoff.paint().length>0,'wingburst disturbs its actual takeoff point');
  const after=context();assert.equal(drawReactiveTerrain(after.c,game,10.5,()=>true,createEffectBudget()),0);
  assert.equal(after.paint().length,0,'standing angel does not emit perpetual leaves');
  game.canSee=()=>false;const hidden=context();drawReactiveTerrain(hidden.c,game,10.15,()=>true,createEffectBudget());assert.equal(hidden.paint().length,0);
  game.canSee=()=>true;game.pointVisible=()=>false;const unseenOrigin=context();drawReactiveTerrain(unseenOrigin.c,game,10.15,()=>true,createEffectBudget());assert.equal(unseenOrigin.paint().length,0);
});

test('structure health stages remain readable with no particle budget',()=>{
  for(const kind of ['tower','core']){
    const u={id:1,kind,x:180,y:260,hp:100,maxHp:100,alive:true,team:0};
    const clean=context();drawStructureDamage(clean.c,u,10,{remaining:0,accentRemaining:0});
    assert.equal(clean.paint().length,0,'healthy masonry has no damage overlay');
    u.hp=50;const damaged=context();drawStructureDamage(damaged.c,u,10,{remaining:0,accentRemaining:0});
    u.hp=20;const critical=context();drawStructureDamage(critical.c,u,10,{remaining:0,accentRemaining:0});
    assert(damaged.paint().length>0);assert(critical.paint().length>damaged.paint().length,'critical structure has deeper fractures');
    assert(critical.balanced());assert.equal(u.hp,20);
  }
});

test('destruction advances with presentation time after the simulation freezes',()=>{
  for(const kind of ['tower','core']){
    const e=effect('structure-collapse',{kind,life:1.2,maxLife:1.2}),before=JSON.stringify(e);
    const start=context(),middle=context(),end=context();
    drawWorldEffect(start.c,e,10.05,false,createEffectBudget());
    drawWorldEffect(middle.c,e,10.65,false,createEffectBudget());
    drawWorldEffect(end.c,e,11.3,false,createEffectBudget());
    assert(start.paint().length>0);assert.notDeepEqual(start.calls,middle.calls,'physical chunks move while effect.life is frozen');
    assert.equal(end.paint().length,0,'presentation expires independently');
    assert.equal(JSON.stringify(e),before);assert(start.balanced());assert(middle.balanced());assert(end.balanced());
  }
});

test('spell aftermath stays subdued and is distinct from a live danger zone',()=>{
  const signatures=[];
  for(const mark of ['scorch','frost','quake','slam','poison']){
    const c=context(),e=effect('ground-mark',{mark,maxLife:5,life:5});
    drawWorldEffect(c.c,e,11,true,createEffectBudget());
    assert(c.paint().length>0);assert(c.paint().every(p=>p.alpha<=.35),'harmless residue remains low contrast');
    const expired=context();drawWorldEffect(expired.c,e,15.1,true,createEffectBudget());assert.equal(expired.paint().length,0);
    signatures.push(JSON.stringify(c.calls));assert(c.balanced());
  }
  assert.equal(new Set(signatures).size,5);
});

test('summons, capture and progression have finite deterministic presentations',()=>{
  const cases=[effect('summon-arrive',{subtype:'spirit'}),effect('summon-arrive',{subtype:'treant'}),effect('summon-arrive',{subtype:'wolf'}),effect('summon-arrive',{subtype:'bear'}),effect('summon-depart',{subtype:'treant'}),effect('beast-capture',{subtype:'maw',tier:8}),effect('reward-level',{level:6}),effect('reward-item',{itemId:'sovereign-aegis'})];
  const render=()=>{
    const c=context(),budget=createEffectBudget();let departures=0;
    for(const e of cases)for(const ground of [true,false])assert(drawWorldEffect(c.c,e,10.4,ground,budget,{drawUnit(ctx,event,alpha){assert(event.type==='summon-depart');assert(alpha>0&&alpha<1);departures++;}}));
    assert.equal(departures,1);assert(budget.remaining>=0);assert(c.balanced());return c.calls;
  };
  assert.deepEqual(render(),render(),'paused redraw has stable geometry');
  assert.equal(isWorldEffect({type:'storm'}),false,'ordinary spells retain their existing renderer');
});

test('gold flies toward the actual CSS HUD position and expires without changing rewards',()=>{
  const e=effect('reward-gold',{amount:37,maxLife:1,life:1}),camera={x:0,y:0,width:1000,height:500},viewport={width:640,height:400,cssWidth:1280,cssHeight:800},target={x:1100,y:760};
  const nearStart=context(),nearEnd=context(),ended=context(),before=JSON.stringify(e);
  assert(drawGoldFlight(nearStart.c,e,10.15,camera,viewport,target,createEffectBudget())>0);
  assert(drawGoldFlight(nearEnd.c,e,10.78,camera,viewport,target,createEffectBudget())>0);
  const distance=c=>{const calls=c.paint().filter(v=>v.method==='fillRect'),x=calls.reduce((n,v)=>n+v.args[0],0)/calls.length,y=calls.reduce((n,v)=>n+v.args[1],0)/calls.length;return Math.hypot(x-550,y-380);};
  assert(distance(nearEnd)<distance(nearStart)*.3,'coins arrive at scaled HUD coordinates');
  assert.equal(drawGoldFlight(ended.c,e,11.1,camera,viewport,target,createEffectBudget()),0);
  assert.equal(ended.paint().length,0);assert.equal(JSON.stringify(e),before);assert(nearEnd.balanced());
  assert.equal(drawGoldFlight(ended.c,e,10.5,camera,viewport,{},createEffectBudget()),0);
});
