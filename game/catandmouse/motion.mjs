// Presentation coordinates never alias a simulation object or get sent to Firebase.
const clamp = (value, min, max) => Math.max(min, Math.min(max, value));
export const MOTION_PROFILES = Object.freeze({
  mouse: { duration:120, min:55, max:170, hop:1.4 },
  cat: { duration:650, min:100, max:950, hop:1.2 },
  rat: { duration:360, min:130, max:520, hop:2 },
  stinkrat: { duration:580, min:210, max:740, hop:1.6 },
  ox: { duration:730, min:300, max:850, hop:1, heavy:true },
  ratking: { duration:640, min:270, max:780, hop:1.5, heavy:true },
  vulture: { duration:480, min:180, max:620, hop:0.5 },
  termite: { duration:300, min:130, max:420, hop:0.7 },
  flea: { duration:540, min:170, max:720, hop:5 },
  rabbit: { duration:1700, min:160, max:1900, hop:5.5 }
});

export function createMotionTrack(position, now = 0, type = 'rat') {
  const point = { x:position.x, y:position.y };
  return { type, grid:{...point}, previousGrid:{...point}, from:{...point}, target:{...point},
    rendered:{...point}, startedAt:now, receivedAt:now, duration:0, moving:false, facing:position.facing || 'se' };
}

export function sampleMotion(track, now, reducedMotion = false) {
  const t = track.duration ? clamp((now-track.startedAt)/track.duration, 0, 1) : 1;
  const profile = MOTION_PROFILES[track.type] || MOTION_PROFILES.rat;
  // Mostly linear travel; heavy bodies get a little more acceleration.
  const smooth = t*t*(3-2*t);
  const progress = t*(profile.heavy ? 0.55 : 0.84) + smooth*(profile.heavy ? 0.45 : 0.16);
  track.rendered.x = track.from.x + (track.target.x-track.from.x)*progress;
  track.rendered.y = track.from.y + (track.target.y-track.from.y)*progress;
  track.moving = t < 1;
  return { x:track.rendered.x, y:track.rendered.y, moving:track.moving,
    lift:reducedMotion ? 0 : Math.sin(t*Math.PI)*(profile.hop || 0) };
}

export function retargetMotion(track, position, now, { local = false, teleport = false, duration, speed = 1 } = {}) {
  if (!Number.isFinite(position.x) || !Number.isFinite(position.y)) return false;
  track.facing = position.facing || track.facing;
  if (track.grid.x === position.x && track.grid.y === position.y && !teleport) return false;
  const current = sampleMotion(track, now);
  const distance = Math.abs(current.x-position.x) + Math.abs(current.y-position.y);
  const profile = MOTION_PROFILES[track.type] || MOTION_PROFILES.rat;
  const cadence = now-track.receivedAt;
  track.previousGrid = {...track.grid};
  track.grid = {x:position.x, y:position.y};
  track.from = {x:current.x, y:current.y};
  track.target = {...track.grid};
  track.receivedAt = now;
  track.startedAt = now;
  // Teleports/revives don't glide through walls. Smaller corrections catch up
  // promptly instead of accumulating a several-tile presentation backlog.
  if (teleport || distance > 5) {
    track.from = {...track.target};
    track.duration = 0;
  } else if (distance > 1.8) {
    track.duration = local ? 65 : 110;
  } else {
    const observed = cadence >= profile.min && cadence <= profile.max*1.8 ? cadence*0.94 : profile.duration/Math.max(0.85, speed);
    track.duration = local ? 85 : clamp(duration ?? observed, profile.min, profile.max);
  }
  track.moving = track.duration > 0;
  return true;
}

export function sampleProjectile(shot, now, reducedMotion = false) {
  const t = clamp((now-shot.startedAt)/Math.max(1, shot.duration), 0, 1);
  return { x:shot.from.x+(shot.to.x-shot.from.x)*t, y:shot.from.y+(shot.to.y-shot.from.y)*t,
    lift:reducedMotion ? 0 : Math.sin(t*Math.PI)*(shot.arc || 0), finished:t === 1 };
}
