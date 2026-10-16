import { VIEW } from "./config.js";
import { clamp, lerp } from "./common.js";
export class Camera {
  constructor() { this.x = 0; this.y = 0; this.previousX = 0; this.previousY = 0; this.shake = 0; this.arena = null; this.look = 0; }
  focusArena(arena = null) { this.arena = arena; }
  kick(amount = 6) { this.shake = Math.min(14, Math.max(this.shake, amount)); }
  reset(player, world) {
    this.x = clamp(player.x - VIEW.width * 0.35, 0, Math.max(0, world.width - VIEW.width));
    this.y = clamp(player.y + player.h - VIEW.height * 0.76, world.bounds?.top ?? -700, (world.bounds?.bottom ?? 1000) - VIEW.height);
    this.previousX = this.x; this.previousY = this.y; this.look = 0; this.shake = 0;
  }
  update(player, dt, worldOrWidth) {
    const world = typeof worldOrWidth === "number" ? { width: worldOrWidth } : worldOrWidth;
    this.previousX = this.x; this.previousY = this.y;
    this.look = lerp(this.look, clamp(player.vx / 430, -1, 1) * 125, 1 - Math.exp(-4 * dt));
    const targetX = player.x + player.w / 2 - VIEW.width * 0.42 + this.look;
    // Keep the landing zone visible during a fall, with a stable grounded horizon.
    const verticalLook = player.vy > 180 ? Math.min(80, player.vy * 0.07) : player.vy < -100 ? -35 : 0;
    const targetY = player.y + player.h - VIEW.height * 0.77 + verticalLook;
    this.x = lerp(this.x, targetX, 1 - Math.exp(-7.5 * dt));
    const deadZone = 18;
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
