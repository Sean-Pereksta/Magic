// The simulation and its saves own authoritative positions. Canvas sees a
// short-lived view between ticks, while fog checks still use simulation positions.
export function captureMotion(game) {
  return {
    units:new Map(game.units.map(unit=>[unit.id,{x:unit.x,y:unit.y,alive:unit.alive,action:unit.visualAction}])),
    projectiles:new Map(game.projectiles.map(shot=>[shot.id,{x:shot.x,y:shot.y}]))
  };
}

function position(entity, previous, alpha, maximumStep) {
  if(!previous||entity.alive!==previous.alive||entity.x===previous.x&&entity.y===previous.y)return entity;
  const dx=entity.x-previous.x,dy=entity.y-previous.y;
  const action=entity.visualAction;
  const castMove=action?.snapMotion&&previous.action!==action;
  if(castMove||Math.hypot(dx,dy)>maximumStep)return entity;
  return {...entity,x:previous.x+dx*alpha,y:previous.y+dy*alpha};
}

export function renderMotion(game, previous, alpha) {
  if(!previous||game.paused||game.winner!=null)return game;
  const units=game.units.map(unit=>position(unit,previous.units.get(unit.id),alpha,32));
  const lookup=new Map(game.units.map(unit=>[unit.id,unit]));
  return Object.assign(Object.create(game),{
    units,
    projectiles:game.projectiles.map(shot=>position(shot,previous.projectiles.get(shot.id),alpha,120)),
    player:units.find(unit=>unit.id===game.playerId)??game.player,
    canSee:(team,unit)=>game.canSee(team,lookup.get(unit.id)??unit),
    pointVisible:game.pointVisible.bind(game),
    visionSources:game.visionSources.bind(game)
  });
}
