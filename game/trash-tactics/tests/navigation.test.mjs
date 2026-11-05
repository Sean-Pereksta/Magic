import test from 'node:test';
import assert from 'node:assert/strict';
import {Game} from '../engine.js';

function corridor(){
  const game=new Game();game._think=Infinity;
  game.map.obstacles=[{x:500,y:300,w:100,h:400,type:'book'}];
  game.entities=game.entities.filter(e=>e.type==='desk');
  return game;
}
function checkRoute(game,start,route,target,ignoreId){
  assert(route.length>1,'a detour includes intermediate waypoints');
  let previous=start;
  for(const point of route){assert(game._lineFree(previous.x,previous.y,point.x,point.y,start.r,ignoreId),'every route segment clears terrain and buildings');previous=point;}
  assert(Math.hypot(previous.x-target.x,previous.y-target.y)<.01,'route reaches the actual requested destination');
}
test('cached squad routes invalidate when a building blocks the corridor',()=>{
  const game=corridor(),troop=game._spawn('rock',0,350,500),target={x:750,y:500};
  const before=game._path(troop,target.x,target.y);checkRoute(game,troop,before,target);
  const previousGraph=game._navigation;
  const blocking=before[Math.floor(before.length/2)];game._spawn('cabinet',1,blocking.x,blocking.y);
  const after=game._path(troop,target.x,target.y);checkRoute(game,troop,after,target);
  assert.notEqual(game._navigation,previousGraph,'a new building invalidates cached clearance');
  assert.notDeepEqual(after,before,'the squad takes a new route around the blueprint');
});
test('route caches separate unit footprints, ignored buildings and replaced terrain',()=>{
  const game=corridor(),target={x:750,y:500},troop=game._spawn('rock',0,350,500);
  checkRoute(game,troop,game._path(troop,target.x,target.y),target);
  const twig=game._spawn('twig',0,350,500);
  checkRoute(game,twig,game._path(twig,target.x,target.y),target);
  assert.equal(game._navigation.graphs.size,2,'workers and troops retain their own clearance');
  const desk=game.entities.find(e=>e.type==='desk');
  checkRoute(game,troop,game._path(troop,target.x,target.y,desk.id),target,desk.id);
  assert.equal(game._navigation.graphs.size,3,'ignored collision footprints use separate graphs');
  const oldGraph=game._navigation;game.map.obstacles=[{x:500,y:250,w:100,h:500,type:'book'}];
  checkRoute(game,troop,game._path(troop,target.x,target.y),target);
  assert.notEqual(game._navigation,oldGraph,'replaced terrain invalidates cached routes');
});
