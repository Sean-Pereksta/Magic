// Presentation-only poses. Attack deadlines and collision positions belong to
// the simulation; these helpers only select bounded sprite frames and offsets.
const clamp = (v, a, b) => Math.max(a, Math.min(b, v));
const live = u => u?.alive !== false && u?.hp > 0;
const motion = Object.freeze({
  arc: { back:2, strike:3, crouch:0, tilt:.04 },
  dragon: { back:4, strike:5, crouch:2, tilt:.09 },
  druid: { back:1, strike:2, crouch:3, tilt:.03 },
  angel: { back:1, strike:1, crouch:-2, tilt:.02 },
  ranger: { back:3, strike:2, crouch:1, tilt:.04 },
  bulwark: { back:4, strike:6, crouch:3, tilt:.06 },
  ember: { back:3, strike:6, crouch:3, tilt:.12 },
  seraph: { back:4, strike:5, crouch:1, tilt:.09 },
  rivet: { back:2, strike:3, crouch:2, tilt:.025 },
  brinehook: { back:5, strike:7, crouch:2, tilt:.07 },
  vela: { back:4, strike:-2, crouch:1, tilt:.055 },
  solstice: { back:1, strike:1, crouch:0, tilt:.015 },
});

export function unitPose(unit, game, time = game.time || 0, reducedMotion = false) {
  const action = unit.visualAction, active = action && Number.isFinite(action.startedAt) && time >= action.startedAt && time < action.startedAt + Math.max(.01, action.duration || .32);
  let stage = 0, angle = Number.isFinite(unit.angle) ? unit.angle : 0, cast = false;
  if (active) {
    angle = Number.isFinite(action.angle) ? action.angle : angle;
    cast = action.kind === 'cast';
    const impactAt = Number.isFinite(action.impactAt) ? action.impactAt : action.startedAt;
    if (time < impactAt) {
      // A thrown rock leaves the Colossus's hand before its marked impact.
      const windup = Math.max(.01, impactAt - action.startedAt);
      stage = action.kind === 'colossus-rock' && (time - action.startedAt) / windup >= .28 ? 3 : 1;
    } else {
      const recovery = Math.max(.12, action.startedAt + (action.duration || .32) - impactAt);
      const t = clamp((time - impactAt) / recovery, 0, 1);
      stage = t < .22 ? 2 : t < .6 ? 3 : 4;
    }
  } else if ((unit.attackAnim || 0) > 0) {
    // Old saves can lack visualAction. Their existing attack timer still gives
    // a strike-first recovery instead of replaying a false windup after damage.
    const t = 1 - clamp(unit.attackAnim / .32, 0, 1);
    stage = t < .22 ? 2 : t < .6 ? 3 : 4;
  } else if (!unit.status?.stun && !unit.recallIn && !(unit.magazine?.reloadRemaining>0) && !unit.channel && unit.attackTimer > 0 && unit.attackTimer < .16 && (!unit.isPlayer || game.attackOrder || game.attackHeld)) {
    const target = (game.units || []).find(t => t.id === unit.targetId);
    if (live(target) && target.team !== unit.team && !game.isProtected?.(target) && Math.hypot(target.x - unit.x, target.y - unit.y) <= (unit.range || 40) + (target.r || 12) + (unit.r || 12) * .35 && (unit.team < 0 || !game.canSee || game.canSee(unit.team, target))) {
      stage = 1; angle = Math.atan2(target.y - unit.y, target.x - unit.x);
    }
  }
  const phase = Number.isFinite(unit.id) ? unit.id : 0;
  let frame = reducedMotion ? 0 : unit.moving ? Math.floor(time * (unit.kind === 'hero' ? 9 : 7) + phase) % 4 : Math.floor(time * 1.8 + phase) % 4;
  if(unit.heroId==='vela'&&unit.channel?.kind==='silver-volley'&&unit.channel.endsAt>time&&!unit.status?.stun){
    stage=time<unit.channel.fireAt?1:6;cast=true;angle=unit.channel.angle;
    frame=reducedMotion?0:Math.floor(Math.max(0,time-unit.channel.fireAt)*10)%2;
  }else if(unit.heroId==='vela'&&unit.magazine?.reloadRemaining>0&&!active){
    stage=5;frame=reducedMotion?0:clamp(Math.floor((1-unit.magazine.reloadRemaining/Math.max(.01,unit.magazine.reloadDuration||1.4))*4),0,3);
  }
  const direction = ((Math.round(angle / (Math.PI / 4)) % 8) + 8) % 8;
  const shape = motion[unit.heroId] || {back:2,strike:3,crouch:1,tilt:.04};
  const strength = stage === 1 ? -shape.back : stage === 2 ? shape.strike : stage === 3 ? shape.strike * .55 : stage === 4 ? 1 : 0;
  const hitAge = time - (unit.visualHit?.at ?? -100), hit = hitAge >= 0 && hitAge < .16 ? (1 - hitAge / .16) : 0;
  const hitAngle = Number.isFinite(unit.visualHit?.angle) ? unit.visualHit.angle : 0;
  const idle = !unit.moving && !stage && !unit.status?.stun;
  return {
    stage, frame, direction, angle, cast, active:!!active,
    variant:cast&&['bolt-sentry','shock-mine'].includes(action?.abilityId)?'build':unit.heroId==='vela'?(unit.status?.steadyAim?'aim':'')+(unit.magazine?.rounds%2===1?'offhand':''):'',
    dx: reducedMotion ? 0 : Math.round(Math.cos(angle) * strength + Math.cos(hitAngle) * hit * 3),
    dy: reducedMotion ? 0 : Math.round(Math.sin(angle) * strength * .35 + (stage === 1 || stage === 2 ? shape.crouch : 0) + Math.sin(hitAngle) * hit * 2),
    tilt: reducedMotion ? 0 : (stage === 1 ? -1 : stage === 2 ? 1 : stage === 3 ? .5 : 0) * shape.tilt * (Math.cos(angle) < 0 ? -1 : 1),
    hit, idle, idleFrame: idle && !reducedMotion ? Math.floor((time + phase * .31) * .75) % 4 : 0,
  };
}

export function summonPose(unit, time, reducedMotion = false) {
  const birth = unit.visualSpawn;
  if (!birth || !Number.isFinite(birth.at)) return {active:false,progress:1,alpha:1,lift:0,rise:0};
  const t = clamp((time - birth.at) / Math.max(.1, birth.duration || .65), 0, 1);
  if (t >= 1 || time < birth.at) return {active:false,progress:1,alpha:1,lift:0,rise:0};
  const subtype = birth.subtype || unit.subtype || '';
  const spirit = /spirit/.test(subtype), tree = /treant/.test(subtype);
  return {active:true,progress:t,alpha:reducedMotion ? .6 + .4 * t : spirit ? .15 + .85 * t : .75 + .25 * t,
    lift:reducedMotion || tree || spirit ? 0 : Math.sin(t * Math.PI) * (/wolf/.test(subtype) ? 26 : 15),
    rise:tree && !reducedMotion ? (1 - t) * .7 : 0, assemble:spirit && !reducedMotion};
}

export function drawHitGlint(c, unit, pose, width, height, budget) {
  if (!(pose.hit > 0) || budget && budget.remaining <= 0) return;
  if (budget) budget.remaining--;
  const side = Math.cos(unit.visualHit?.angle || 0) < 0 ? 1 : -1;
  c.save(); c.globalAlpha = pose.hit * .65; c.fillStyle = '#fff4d6';
  const x = Math.round(unit.x + side * Math.min(16, width * .22)), y = Math.round(unit.y - height * .55);
  c.fillRect(x, y, 3, 9); c.fillRect(x - side * 3, y - 3, 6, 3); c.restore();
}

export function drawMonsterRocks(c, game, time, visible, budget) {
  let drawn = 0;
  const team = game.player?.team ?? 0;
  for (const z of game.zones || []) {
    if (z.type !== 'colossus-rock' || !(z.tickIn > 0) || !(z.maxDelay || z.delay)) continue;
    const owner = game.units.find(u => u.id === z.ownerId);
    if (!live(owner) || game.canSee && !game.canSee(team, owner)) continue;
    const duration = z.maxDelay || z.delay, progress = clamp(1 - z.tickIn / duration, 0, 1);
    if (progress < .28) continue;
    const t = (progress - .28) / .72;
    const fromX = Number.isFinite(z.fromX) ? z.fromX : owner.visualAction?.fromX ?? owner.x;
    const fromY = Number.isFinite(z.fromY) ? z.fromY : owner.visualAction?.fromY ?? owner.y;
    const x = fromX + (z.x - fromX) * t, y = fromY - 55 + (z.y - fromY + 55) * t - Math.sin(t * Math.PI) * (budget?.reducedMotion ? 25 : 76);
    if (!visible(x,y) || game.pointVisible && !game.pointVisible(team,x,y)) continue;
    if (budget && budget.remaining < 1) break;
    if (budget) budget.remaining--;
    c.save(); c.fillStyle = '#233b3d'; c.fillRect(Math.round(x - 9),Math.round(y - 8),18,16);
    c.fillStyle = '#8b9d85'; c.fillRect(Math.round(x - 7),Math.round(y - 7),14,12);
    c.fillStyle = '#c0d5a0'; c.fillRect(Math.round(x - 7),Math.round(y - 7),8,4);
    c.fillStyle = '#4e7467'; c.fillRect(Math.round(x + 2),Math.round(y - 2),5,7); c.restore(); drawn++;
  }
  return drawn;
}

