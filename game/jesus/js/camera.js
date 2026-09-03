import { VIEW } from "./config.js";
import { clamp, lerp } from "./common.js";

export class Camera {
  constructor() {
    this.x = 0;
    this.y = 0;
    this.shake = 0;
    this.arena = null;
  }

  focusArena(arena = null) {
    this.arena = arena;
  }

  kick(amount = 6) {
    this.shake = Math.max(this.shake, amount);
  }

  update(player, dt, levelWidth) {
    const directionalLook = clamp(player.vx / 430, -1, 1) * 150;
    const targetX = player.x + player.w / 2 - VIEW.width * 0.45 + directionalLook;
    const verticalLook = player.vy < -120 ? -80 : player.vy > 260 ? 52 : 0;
    const targetY = clamp(player.y - VIEW.height * 0.52 + verticalLook, -150, 70);
    const smoothing = 1 - Math.pow(0.001, dt);
    this.x = lerp(this.x, targetX, smoothing);
    this.y = lerp(this.y, targetY, smoothing * 0.7);

    const minX = this.arena ? this.arena.left : 0;
    const maxX = this.arena ? this.arena.right - VIEW.width : levelWidth - VIEW.width;
    this.x = clamp(this.x, minX, Math.max(minX, maxX));
    this.shake = Math.max(0, this.shake - 24 * dt);
  }

  transform(context) {
    const sx = this.shake ? (Math.random() - 0.5) * this.shake : 0;
    const sy = this.shake ? (Math.random() - 0.5) * this.shake : 0;
    context.translate(-Math.round(this.x) + sx, -Math.round(this.y) + sy);
  }
}
