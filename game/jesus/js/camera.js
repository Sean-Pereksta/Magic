import { VIEW } from "./config.js";
import { clamp, lerp } from "./common.js";
export class Camera {
  constructor() { this.x = 0; this.y = 0; this.previousX = 0; this.previousY = 0; this.shake = 0; this.arena = null; this.look = 0; }
  focusArena(arena = null) { this.arena = arena; }
  kick(amount = 6) { this.shake = Math.min(14, Math.max(this.shake, amount)); }
  reset(player, world) {
    this.x = clamp(player.x - VIEW.width * 0.35, 0, Math.max(0, world.width - VIEW.width));
    this.y = clamp(this.verticalTarget(player, world), world.bounds?.top ?? -700, (world.bounds?.bottom ?? 1000) - VIEW.height);
    this.previousX = this.x; this.previousY = this.y; this.look = 0; this.shake = 0;
  }
  verticalTarget(player, world, lookDown = false) {
    const feet = player.y + player.h;
    const fallLook = player.vy > 180 ? Math.min(100, player.vy * .09) : player.vy < -100 ? -30 : 0;
    let target = feet - VIEW.height * .65 + fallLook;
    if ((player.onGround && !world.bossArena) || lookDown) {
      const center = player.x + player.w / 2, ahead = Math.min(420, VIEW.width * .6);
      const direction = player.facing || Math.sign(player.vx) || 1;
      const left = center - (lookDown || direction < 0 ? ahead : 90);
      const right = center + (lookDown || direction > 0 ? ahead : 90);
      const lower = [...(world.platforms || []), ...(world.movers || [])]
        .filter(p => !p.dead && !p.open && !p.hidden && p.kind !== 'wall' && p.kind !== 'gate'
          && p.x + p.w > left && p.x < right && p.y >= feet + 56)
        .sort((a, b) => a.y - b.y)[0];
      if (lower) target = Math.max(target, lower.y + 100 - VIEW.height);
    }
    if (lookDown) target += Math.min(190, VIEW.height * .32);
    if(!lookDown&&!world.bossArena){
      const lift=(world.movers||[]).find(p=>!p.dead&&!p.hidden&&p.id===player.groundId&&(p.axis==="y"||p.motion==="lift"));
      if(lift){
        const upper=(lift.baseY??lift.y)-(lift.range||0);
        target=Math.min(target,Math.max(upper-140,feet-VIEW.height+100));
      }
    }
    // Looking down reveals landings while keeping the player's head in view.
    return Math.min(target, player.y - 64);
  }
  update(player, dt, worldOrWidth, {lookDown = false} = {}) {
    const world = typeof worldOrWidth === "number" ? { width: worldOrWidth } : worldOrWidth;
    this.previousX = this.x; this.previousY = this.y;
    const upcoming=(world.movers||[]).some(p=>p.required&&p.x+p.w>player.x&&p.x<player.x+650);
    const anticipation=upcoming&&player.onGround&&Math.abs(player.vx)<60?(player.facing||1)*95:clamp(player.vx / 430, -1, 1)*125;
    this.look = lerp(this.look, anticipation, 1 - Math.exp(-4 * dt));
    const targetX = player.x + player.w / 2 - VIEW.width * 0.42 + this.look;
    const targetY = this.verticalTarget(player, world, lookDown);
    this.x = lerp(this.x, targetX, 1 - Math.exp(-7.5 * dt));
    const deadZone = lookDown ? 4 : 12;
    if (Math.abs(targetY - this.y) > deadZone) this.y = lerp(this.y, targetY - Math.sign(targetY - this.y) * deadZone, 1 - Math.exp(-6 * dt));
    const left = this.arena?.left ?? 0, right = this.arena?.right ?? world.width;
    this.x = clamp(this.x, left, Math.max(left, right - VIEW.width));
    this.y = clamp(this.y, this.arena?.top ?? world.bounds?.top ?? -700, (this.arena?.bottom ?? world.bounds?.bottom ?? 1000) - VIEW.height);
    this.shake = Math.max(0, this.shake - 24 * dt);
  }
  transform(context, alpha = 1) {
    const sx = this.shake ? (Math.random() - 0.5) * this.shake : 0;
    const sy = this.shake ? (Math.random() - 0.5) * this.shake : 0;
    context.translate(-Math.round(lerp(this.previousX, this.x, alpha)) + sx, -Math.round(lerp(this.previousY, this.y, alpha)) + sy);
  }
}
