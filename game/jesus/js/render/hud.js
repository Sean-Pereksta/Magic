import { POWER_LABELS } from "../config.js";

export class HUD {
  constructor(root = document) {
    this.region = root.getElementById("regionText");
    this.level = root.getElementById("levelText");
    this.distance = root.getElementById("distanceText");
    this.hearts = root.getElementById("heartsText");
    this.powers = root.getElementById("powerList");
    this.bossHud = root.getElementById("bossHud");
    this.bossName = root.getElementById("bossName");
    this.bossFill = root.getElementById("bossFill");
    this.bossPercent = root.getElementById("bossPercent");
    this.bossPhase = root.getElementById("bossPhase");
    this.banner = root.getElementById("banner");
    this.bannerTimer = 0;
  }

  showBanner(title, subtitle = "", duration = 2) {
    this.banner.innerHTML = `<strong>${title}</strong><span>${subtitle}</span>`;
    this.banner.classList.add("show");
    this.bannerTimer = duration;
  }

  update(game, dt) {
    if (this.bannerTimer > 0) {
      this.bannerTimer -= dt;
      if (this.bannerTimer <= 0) this.banner.classList.remove("show");
    }
    this.region.textContent = game.world?.regionName || game.world?.region || "—";
    this.level.textContent = game.mode === "journey" ? (game.journeyNode?.name || "Journey") : `Depth ${game.depth + 1}`;
    this.distance.textContent = Math.floor(game.score);
    this.hearts.textContent = `${"♥".repeat(game.player.hearts)}${"♡".repeat(Math.max(0, game.player.maxHearts - game.player.hearts))}`;
    this.powers.innerHTML = Object.entries(game.player.powers)
      .filter(([, value]) => Boolean(value))
      .map(([key, value]) => `<span>${POWER_LABELS[key] || key}${typeof value === "number" ? ` ×${value}` : ""}</span>`)
      .join("");

    const boss = game.bossController?.boss;
    const visible = Boolean(boss && !boss.defeatHandled);
    this.bossHud.classList.toggle("visible", visible);
    if (visible) {
      const ratio = Math.max(0, boss.health / boss.maxHealth);
      this.bossName.textContent = boss.definition.name;
      this.bossFill.style.width = `${ratio * 100}%`;
      this.bossPercent.textContent = `${Math.round(ratio * 100)}%`;
      this.bossPhase.textContent = `${boss.phase.name} · ${boss.state}${boss.currentAttack ? ` · ${boss.currentAttack.name}` : ""}`;
    }
  }
}

