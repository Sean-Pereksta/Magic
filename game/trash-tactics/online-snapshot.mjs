const visualFields = [
  'id','type','team','x','y','hp','maxHp','r','facing','constructing','buildTotal',
  'amount','maxAmount','carrying','paperInvulnerable','attackAnim','wallMask','workState'
];
const playerFields = [
  ...visualFields,'rally','cooldown','abilityCooldown','order','queue','resourceId','collectorId',
  'buildQueue','builderId','cargo','transportedBy','leap','mageCharges','mageRecharge',
  'pendingAbility','clickDamage','aim','cast','casting','crash','divisionCast','mageCasting',
  'roll','shadow','attackTarget'
];

function pick(source, keys) {
  return Object.fromEntries(keys.filter(key => source[key] !== undefined).map(key => [key,source[key]]));
}

function packGrid(grid) {
  const bytes = Buffer.alloc(Math.ceil(grid.length / 8));
  for (let i=0; i<grid.length; i++) if (grid[i]) bytes[i >> 3] |= 1 << (i & 7);
  return bytes.toString('base64');
}

function visualEntity(entity) {
  const view = pick(entity, visualFields);
  view.order = {type:'idle'};
  view.queue = entity.queue?.[0] ? [pick(entity.queue[0], ['remaining','total'])] : [];
  if (entity.cargo) view.cargo = entity.cargo.map(() => null);
  if (entity.leap) view.leap = pick(entity.leap,['remaining','total']);
  if (entity.crash) view.crash = pick(entity.crash,['diving','diveFor','diveTotal']);
  if (entity.roll) view.roll = true;
  if (entity.shadow) view.shadow = true;
  if (entity.aim) view.aim = pick(entity.aim,['remaining','total']);
  return view;
}

export function snapshotFor(game, peer, events = []) {
  const team = peer.team;
  game._ensureVision();
  const seen = (x,y) => Number.isFinite(x) && Number.isFinite(y) && game.canSee(team,x,y);
  const entities = game.entities.filter(e => e.team === team || game.isVisible(e,team))
    .map(e => e.team === team ? pick(e,playerFields) : visualEntity(e));
  const state = {
    type:'state',time:game.time,entities,
    teams:game.teams.map((t,i) => i === team ? pick(t,['trash','upgrades','eliminated','start']) : pick(t,['eliminated','start'])),
    effects:game.effects.filter(e => seen(e.x,e.y)).map(e => {
      if (e.team === team) return e;
      const view = {...e};
      for (const [x,y] of [['toX','toY'],['fromX','fromY'],['targetX','targetY']]) {
        if (x in view && !seen(view[x],view[y])) { delete view[x]; delete view[y]; }
      }
      if (view.points) view.points = view.points.filter(p => seen(p.x,p.y));
      return view;
    }),
    projectiles:game.projectiles.filter(p => seen(p.x,p.y))
      .map(p => p.team === team || seen(p.fromX,p.fromY) ? p : {...p,fromX:p.x,fromY:p.y}),
    forceFields:game.forceFields.filter(f => seen(f.x,f.y)),
    divisions:game.divisions.filter(f => seen(f.x,f.y)),
    events:events.filter(e => e.team === team || (e.target && (e.target.team === team || seen(e.target.x,e.target.y)))),
    field:{
      gateOpen:!!game.map.gate?.open,
      stations:(game.map.stations || []).filter(s => seen(s.x,s.y) || s.owner === team)
        .map(s => pick(s,['id','owner','entityId','captureTeam','captureProgress','cooldownUntil'])),
      conveyors:(game.map.conveyors || []).filter(b => b.owner === team || (game.map.stations || []).some(s => s.beltId === b.id && seen(s.x,s.y)))
        .map(b => pick(b,['id','owner','reversedUntil']))
    }
  };
  if (peer.fogRevision !== game.fog.revision) {
    const fog = game.fog.teams[team];
    state.fog = {visible:packGrid(fog.visible),explored:packGrid(fog.explored),revision:game.fog.revision};
  }
  if (peer.memoryAt === undefined || game.time-peer.memoryAt >= .5) {
    state.memory = game.getRememberedEntities(team).slice(-400);
  }
  return state;
}
