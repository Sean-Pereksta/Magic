import test from 'node:test';
import assert from 'node:assert/strict';
import {pickEnemyAt} from '../game/moba-mania/input.mjs';
import {HEROES} from '../game/moba-mania/data.mjs';

const hero=(id,heroId='arc',overrides={})=>({id,heroId,kind:'hero',alive:true,team:1,x:300,y:240,r:20,...overrides});
function pick(units,point,{zoom=1,pointerType='mouse',canSee=()=>true,origin={x:37,y:19},camera={x:100,y:80}}={}) {
  const worldToScreen=(x,y)=>({x:origin.x+(x-camera.x)*zoom,y:origin.y+(y-camera.y)*zoom});
  return pickEnemyAt({units,team:0,point:worldToScreen(point.x,point.y),zoom,pointerType,canSee,worldToScreen});
}

test('mouse and touch can select every hero at the face and planted feet through camera transforms',()=>{
  for (const heroId of Object.keys(HEROES)) for (const pointerType of ['mouse','touch']) for (const zoom of [.8,1.6]) {
    const unit=hero(1,heroId);
    assert.equal(pick([unit],{x:unit.x,y:unit.y-44},{pointerType,zoom}),unit,`${heroId} face`);
    assert.equal(pick([unit],{x:unit.x-7,y:unit.y-3},{pointerType,zoom}),unit,`${heroId} feet`);
  }
});

test('visible staffs, hammer, scarf, and wide angel wings remain selectable outside the old body ellipse',()=>{
  const parts=[['arc',27,-48],['bulwark',31,-49],['ember',-31,-36],['seraph',43,-31],['seraph',-43,-31]];
  for (const [heroId,x,y] of parts) for (const pointerType of ['mouse','touch']) {
    const unit=hero(1,heroId);
    assert.equal(pick([unit],{x:unit.x+x,y:unit.y+y},{pointerType,zoom:1.6}),unit,`${heroId} fringe`);
  }
});

test('a small troop body wins over an overlapping angel wing regardless of unit order',()=>{
  const angel=hero(1,'seraph'),troop={...hero(2),kind:'archer',x:angel.x+43,y:angel.y-14,r:12};
  const point={x:troop.x,y:troop.y-17};
  for (const units of [[angel,troop],[troop,angel]]) for (const pointerType of ['mouse','touch']) {
    assert.equal(pick(units,point,{pointerType,zoom:1.6}),troop);
  }
});

test('picking excludes friendly, defeated, hidden, and distant units instead of acquiring them globally',()=>{
  const visible=hero(9),hidden=hero(1),friend=hero(2,'seraph',{team:0}),dead=hero(3,'arc',{alive:false});
  const offscreen=hero(4,'seraph',{x:3000,y:2400}),point={x:visible.x,y:visible.y-31};
  const options={canSee:unit=>unit!==hidden};
  assert.equal(pick([hidden,friend,dead,offscreen,visible],point,options),visible);
  assert.equal(pick([hidden,friend,dead,offscreen],point,options),undefined);
  assert.equal(pick([visible],{x:visible.x+75,y:visible.y-31}),undefined,'empty ground beside a hero stays empty');
});

test('touch forgiveness stays in CSS pixels at both supported zoom extremes',()=>{
  const unit=hero(1);
  for (const zoom of [.8,1.6]) {
    const point={x:unit.x-30-10/zoom,y:unit.y-30};
    assert.equal(pick([unit],point,{zoom,pointerType:'mouse'}),undefined);
    assert.equal(pick([unit],point,{zoom,pointerType:'touch'}),unit);
    assert.equal(pick([unit],{...point,x:unit.x-30-18/zoom},{zoom,pointerType:'touch'}),undefined);
  }
});

test('tower crystals and crystal platforms are selectable without oversized upper corners',()=>{
  const tower={...hero(1),kind:'tower',r:28},core={...hero(2),kind:'core',r:60};
  assert.equal(pick([tower],{x:tower.x,y:tower.y-102}),tower);
  assert.equal(pick([tower],{x:tower.x+29,y:tower.y}),tower);
  assert.equal(pick([core],{x:core.x,y:core.y+60}),core);
  assert.equal(pick([core],{x:core.x,y:core.y-118}),core);
  assert.equal(pick([core],{x:core.x+64,y:core.y-110}),undefined);
});

test('growing river monsters and scaled summons use their rendered sizes',()=>{
  const beast={...hero(1),kind:'beast',r:42,tier:8};
  const summon={...hero(2),kind:'summon',r:20,scale:1.4};
  assert.equal(pick([beast],{x:beast.x+57,y:beast.y-57}),beast);
  assert.equal(pick([summon],{x:summon.x+32,y:summon.y-25}),summon);
  assert.equal(pick([summon],{x:summon.x+60,y:summon.y-25}),undefined);
});
