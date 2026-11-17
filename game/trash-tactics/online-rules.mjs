import {TYPES} from './engine.js';

export const ONLINE_VERSION = 1;
export const STEP = 1 / 30;
export const SNAPSHOT_INTERVAL = 1 / 15;
export const RECONNECT_MS = 30_000;
export const LOAD_TIMEOUT_MS = 60_000;
export const OFFICIAL_ORIGIN = 'https://trash-tactics.catnmice.com';
export const SITE_ORIGINS = [OFFICIAL_ORIGIN, 'https://catnmice.com', 'https://www.catnmice.com'];

const abilities = new Set(['snipe','intercept','patrol','crash','forcefield','division','roll','shadowpath']);
const buildings = new Set(['desk','cabinet','printer','cdreader','mouse','workshop','laboratory','wall']);
const production = new Set(['desk','printer','cdreader','workshop','laboratory']);
const orders = new Set(['stop','move','attackMove','attack','gather','build']);
const fail = (reason = 'That command is unavailable.') => ({ok:false, reason});

export function cleanName(value) {
  return typeof value === 'string' ? value.replace(/[^\p{L}\p{N} _.-]/gu, '').trim().slice(0, 24) : '';
}

export function executeOrder(game, team, message) {
  const args = message.args;
  if (!Array.isArray(args) || args.length > 5) return fail();
  const [first, second, third, fourth] = args;
  const owned = id => Number.isSafeInteger(id) && game.getEntity(id)?.team === team;
  const ids = list => Array.isArray(list) && list.length <= 200 ? [...new Set(list.filter(owned))] : [];
  const point = (x, y) => Number.isFinite(x) && Number.isFinite(y) && x >= 0 && y >= 0 && x <= game.width && y <= game.height;
  const seen = id => Number.isSafeInteger(id) && game.isVisible(game.getEntity(id), team);
  const typeKey = type => typeof type === 'string' && Object.hasOwn(TYPES, type);

  switch (message.method) {
    case 'train':
      return owned(first) && typeKey(second) ? game.train(first, second) : fail();
    case 'research':
      return owned(first) && typeKey(second) ? game.research(first, second) : fail();
    case 'build':
      return buildings.has(second) && point(third, fourth)
        ? game.build(ids(first), second, third, fourth, {queue:true}) : fail();
    case 'removeWall':
      return owned(first) ? game.removeWall(first, team) : fail();
    case 'reverseConveyor':
      return owned(first) ? game.reverseConveyor(first, team) : fail();
    case 'loadTransport':
      return game.loadTransport(ids(first), ids(second));
    case 'loadNearest':
      return game.loadNearest(ids(first));
    case 'dropTransports':
      return point(second, third) ? game.dropTransports(ids(first), second, third) : fail();
    case 'ability':
      return point(second, third) ? game.ability(ids(first), second, third) : fail();
    case 'useAbility': {
      const target = third;
      if (!abilities.has(second) || !target || !point(target.x, target.y)) return fail();
      const order = {x:target.x, y:target.y};
      if (seen(target.targetId)) order.targetId = target.targetId;
      if (target.points !== undefined) {
        if (!Array.isArray(target.points) || target.points.length > 240 || !target.points.every(p => p && point(p.x,p.y))) return fail('Use a shorter route.');
        order.points = target.points.map(({x,y}) => ({x,y}));
      }
      return game.useAbility(ids(first), second, order);
    }
    case 'command': {
      const selection = ids(first);
      if (!selection.length || !second || !orders.has(second.type)) return fail();
      const order = {type:second.type};
      if (['move','attackMove'].includes(order.type)) {
        if (!point(second.x,second.y)) return fail();
        Object.assign(order,{x:second.x,y:second.y});
      }
      if (['attack','gather','build'].includes(order.type)) {
        if (!seen(second.targetId)) return fail('That target is out of sight.');
        const target = game.getEntity(second.targetId);
        if (order.type === 'attack' && !selection.some(id => game.canTarget(game.getEntity(id), target))) return fail();
        if (order.type === 'gather' && target.type !== 'trash') return fail();
        if (order.type === 'build' && !owned(target.id)) return fail();
        Object.assign(order,{targetId:target.id,x:target.x,y:target.y});
      }
      return game.command(selection,order);
    }
    case 'rally': {
      if (!point(second,third)) return fail();
      const structures = ids(first).map(id => game.getEntity(id)).filter(e => production.has(e.type));
      if (!structures.length) return fail();
      const targetId = seen(fourth) && game.getEntity(fourth).type === 'trash' ? fourth : null;
      for (const structure of structures) structure.rally = {x:second,y:third,targetId};
      return {ok:true};
    }
    default:
      return fail();
  }
}
