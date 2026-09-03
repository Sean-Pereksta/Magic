import { VIEW, PHYSICS } from "./config.js";
import { InputController } from "./input.js";
import { Camera } from "./camera.js";
import { createPlayer, damagePlayer, updateMovers, updatePlayer } from "./physics.js";
import { generateRoom } from "./world/generator.js";
import { REGIONS, regionForDepth } from "./world/regions.js";
import { JOURNEY_BY_ID, JOURNEY_NODES, completeJourneyNode, reachableNodes } from "./world/journey.js";
import { createCheckpoint, restoreCheckpoint } from "./world/checkpoints.js";
import { BossController } from "./bosses/boss-controller.js";
import { bossForRegion } from "./bosses/boss-data.js";
import { updateEnemies } from "./enemies/enemies.js";
import { collectPickups, grantBlessing, useFireRain, useHolyLight } from "./powers/blessings.js";
import { GameStorage } from "./save/storage.js";
import { Renderer } from "./render/renderer.js";
import { HUD } from "./render/hud.js";
import { overlaps } from "./common.js";

function emptyWorld() {
  return {
    id: "menu",
    name: "Galilee Shore",
    region: "galilee",
    regionName: "Galilee",
    width: 1980,
    platforms: [{ id: "menu-ground", x: 0, y: VIEW.floorY, w: 1980, h: 90, kind: "ground", oneWay: false }],
    movers: [], hazards: [], enemies: [], pickups: [], props: [], bossHazards: [],
    routeValid: true,
  };
}

function surfaceAt(world, x) {
  return [...world.platforms, ...world.movers]
    .filter((surface) => x >= surface.x && x <= surface.x + surface.w)
    .sort((a, b) => a.y - b.y)[0] || null;
}

export class JesusRunGame {
  constructor() {
    this.canvas = document.getElementById("gameCanvas");
    this.canvas.width = VIEW.width;
    this.canvas.height = VIEW.height;
    this.storage = new GameStorage();
    this.input = new InputController();
    this.camera = new Camera();
    this.renderer = new Renderer(this.canvas);
    this.hud = new HUD();
    this.player = createPlayer();
    this.world = emptyWorld();
    this.bossController = new BossController((boss) => this.handleBossDefeated(boss));
    this.mode = null;
    this.running = false;
    this.paused = false;
    this.debug = false;
    this.depth = 0;
    this.score = 0;
    this.distanceBank = 0;
    this.difficulty = 1;
    this.time = 0;
    this.fps = 60;
    this.lastFrame = performance.now();
    this.accumulator = 0;
    this.journeySave = this.storage.loadJourney();
    this.journeyNode = null;
    this.checkpointActivated = false;
    this.deathLock = false;
    this.bindUI();
    this.showMainMenu();
    requestAnimationFrame((time) => this.frame(time));
  }

  bindUI() {
    const byId = (id) => document.getElementById(id);
    byId("continueJourneyBtn").addEventListener("click", () => this.startJourney(false));
    byId("newJourneyBtn").addEventListener("click", () => this.startJourney(true));
    byId("endlessBtn").addEventListener("click", () => this.startEndless());
    byId("howBtn").addEventListener("click", () => this.showHowToPlay());
    byId("scoresBtn").addEventListener("click", () => this.showScores());
    byId("fullscreenBtn").addEventListener("click", () => this.toggleFullscreen());
    byId("pauseBtn").addEventListener("click", () => this.togglePause());
    byId("menuBtn").addEventListener("click", () => this.showMainMenu());
    byId("overlayClose").addEventListener("click", () => this.hidePanel());
    byId("gameOverMenu").addEventListener("click", () => this.showMainMenu());
    byId("gameOverRestart").addEventListener("click", () => this.startEndless());
    byId("saveScoreBtn").addEventListener("click", () => this.saveScore());
    this.input.bindMobile(byId("joystick"), byId("stick"), byId("jumpBtn"), byId("lightBtn"), byId("rainBtn"));
  }

  toggleFullscreen() {
    if (!document.fullscreenElement) document.documentElement.requestFullscreen?.();
    else document.exitFullscreen?.();
  }

  togglePause() {
    if (!this.mode || !this.running) return;
    this.paused = !this.paused;
    document.getElementById("pauseShade").classList.toggle("visible", this.paused);
  }

  setGameplayVisible(visible) {
    document.getElementById("gameHud").classList.toggle("hidden", !visible);
    document.getElementById("topControls").classList.toggle("in-game", visible);
    document.getElementById("mobileControls").classList.toggle("active", visible);
  }

  showMainMenu() {
    this.mode = null;
    this.running = false;
    this.paused = false;
    this.bossController.boss = null;
    this.camera.focusArena(null);
    this.world = emptyWorld();
    this.player = createPlayer();
    this.setGameplayVisible(false);
    this.hidePanel();
    document.getElementById("gameOver").classList.remove("visible");
    document.getElementById("journeyMap").classList.remove("visible");
    const menu = document.getElementById("mainMenu");
    menu.classList.add("visible");
    const hasProgress = (this.journeySave.completed || []).length > 0;
    document.getElementById("continueJourneyBtn").hidden = !hasProgress;
  }

  startJourney(fresh) {
    if (fresh && (this.journeySave.completed || []).length && !window.confirm("Start a new Journey and replace the current Journey save?")) return;
    if (fresh) this.journeySave = this.storage.clearJourney();
    this.mode = "journey";
    this.score = (this.journeySave.completed || []).length * 500;
    this.player = createPlayer();
    this.player.maxHearts = this.journeySave.maxHearts || 4;
    for (const power of this.journeySave.permanentPowers || []) grantBlessing(this.player, power);
    document.getElementById("mainMenu").classList.remove("visible");
    this.setGameplayVisible(false);
    this.showJourneyMap();
  }

  showJourneyMap() {
    this.running = false;
    this.bossController.boss = null;
    this.setGameplayVisible(false);
    const overlay = document.getElementById("journeyMap");
    const roads = overlay.querySelector("svg");
    const nodes = overlay.querySelector(".map-nodes");
    roads.innerHTML = "";
    nodes.innerHTML = "";
    const completed = new Set(this.journeySave.completed || []);
    const reachable = new Set(reachableNodes(this.journeySave));

    for (const node of JOURNEY_NODES) {
      for (const nextId of node.next) {
        const next = JOURNEY_BY_ID[nextId];
        const line = document.createElementNS("http://www.w3.org/2000/svg", "line");
        line.setAttribute("x1", `${node.x}%`);
        line.setAttribute("y1", `${node.y}%`);
        line.setAttribute("x2", `${next.x}%`);
        line.setAttribute("y2", `${next.y}%`);
        line.classList.toggle("complete", completed.has(node.id) && completed.has(next.id));
        roads.appendChild(line);
      }
      const button = document.createElement("button");
      button.dataset.node = node.id;
      button.className = `map-node ${completed.has(node.id) ? "complete" : reachable.has(node.id) ? "reachable" : "locked"} ${node.type}`;
      button.style.left = `${node.x}%`;
      button.style.top = `${node.y}%`;
      button.innerHTML = `<b>${completed.has(node.id) ? "✓" : node.type === "boss" ? "♜" : node.type === "trial" ? "✦" : "●"}</b><span>${node.name}</span>`;
      button.disabled = !reachable.has(node.id);
      button.addEventListener("click", () => this.startJourneyNode(node.id));
      nodes.appendChild(button);
    }
    overlay.classList.add("visible");
  }

  startJourneyNode(nodeId, retry = false) {
    const node = JOURNEY_BY_ID[nodeId];
    if (!node) return;
    this.journeyNode = node;
    this.journeySave.currentNode = nodeId;
    this.storage.saveJourney(this.journeySave);
    document.getElementById("journeyMap").classList.remove("visible");
    this.setGameplayVisible(true);
    this.running = true;
    this.paused = false;
    this.deathLock = false;
    this.checkpointActivated = false;
    this.bossController.boss = null;
    const seed = this.journeySave.checkpoint?.nodeId === node.id
      ? this.journeySave.checkpoint.roomSeed
      : [...node.id].reduce((sum, char) => sum + char.charCodeAt(0), 0) * 97;

    if (node.boss) {
      this.startBoss(node.boss, node.difficulty);
    } else {
      this.loadRoom(generateRoom({ region: node.region, seed, journeyDifficulty: node.difficulty }));
      if (retry) restoreCheckpoint(this);
      this.hud.showBanner(node.name.toUpperCase(), `${node.type.toUpperCase()} · DIFFICULTY ${node.difficulty}`);
    }
  }

  startEndless() {
    this.mode = "endless";
    this.running = true;
    this.paused = false;
    this.depth = 0;
    this.score = 0;
    this.distanceBank = 0;
    this.player = createPlayer();
    this.journeyNode = null;
    this.bossController.boss = null;
    document.getElementById("mainMenu").classList.remove("visible");
    document.getElementById("gameOver").classList.remove("visible");
    this.hidePanel();
    this.setGameplayVisible(true);
    this.loadEndlessRoom();
  }

  loadEndlessRoom() {
    const region = regionForDepth(this.depth);
    this.loadRoom(generateRoom({ region, depth: this.depth, seed: Date.now() + this.depth * 1291 }));
    this.hud.showBanner(`DEPTH ${this.depth + 1}`, `${REGIONS[region].name.toUpperCase()} · TIER ${this.world.tier}`);
  }

  loadRoom(room) {
    this.bossController.boss = null;
    this.bossController.arena = null;
    this.world = {
      ...room,
      regionName: REGIONS[room.region]?.name || room.region,
      bossHazards: [],
      props: room.props || [],
    };
    for (const enemy of this.world.enemies) {
      const surface = surfaceAt(this.world, enemy.x + enemy.w / 2);
      if (surface) enemy.y = surface.y - enemy.h;
    }
    for (const pickup of this.world.pickups) {
      const surface = surfaceAt(this.world, pickup.x + pickup.w / 2);
      if (surface) pickup.y = surface.y - 58;
    }
    this.player.x = 76;
    this.player.y = VIEW.floorY - this.player.h;
    this.player.vx = 0;
    this.player.vy = 0;
    this.camera.x = 0;
    this.camera.y = 0;
    this.camera.focusArena(null);
  }

  startBoss(kind, difficulty) {
    this.world = emptyWorld();
    const encounter = this.bossController.start(kind, difficulty, this.world, this.player);
    this.world.regionName = REGIONS[this.world.region]?.name || this.world.region;
    this.world.routeValid = true;
    this.world.tier = difficulty;
    this.world.props = encounter.arena.props || [];
    this.camera.x = 0;
    this.camera.focusArena(encounter.arena);
    this.hud.showBanner(encounter.boss.definition.name, "READ THE TELL · SURVIVE · STRIKE WHEN GOLD");
  }

  finishRoom() {
    if (this.mode === "journey") {
      this.journeySave = completeJourneyNode(this.journeySave, this.journeyNode.id);
      if (this.journeyNode.type === "blessing") this.addPermanentPower("shield");
      if (this.journeyNode.type === "trial") this.addPermanentPower(this.journeyNode.region === "heaven" ? "wings" : "bread");
      this.score += 500;
      this.storage.saveJourney(this.journeySave);
      if (this.journeyNode.id === "heaven") this.showJourneyVictory();
      else this.showJourneyMap();
    } else {
      this.distanceBank += this.world.width;
      this.score += 180 + this.world.tier * 40;
      this.depth++;
      if (this.depth % 4 === 0) this.startBoss(bossForRegion(regionForDepth(this.depth - 1)), Math.min(6, 1 + Math.floor(this.depth / 2)));
      else this.loadEndlessRoom();
    }
  }

  addPermanentPower(power) {
    const powers = new Set(this.journeySave.permanentPowers || []);
    powers.add(power);
    this.journeySave.permanentPowers = [...powers];
    grantBlessing(this.player, power);
  }

  handleBossDefeated(boss) {
    grantBlessing(this.player, boss.definition.reward);
    this.score += 750;
    this.camera.focusArena(null);
    if (this.mode === "journey") {
      this.addPermanentPower(boss.definition.reward);
      this.journeySave = completeJourneyNode(this.journeySave, this.journeyNode.id);
      this.storage.saveJourney(this.journeySave);
      if (this.journeyNode.id === "heaven") this.showJourneyVictory();
      else this.showJourneyMap();
    } else {
      this.depth++;
      this.loadEndlessRoom();
    }
  }

  updateHazards(dt) {
    for (const hazard of this.world.hazards) {
      hazard.timer = (hazard.timer || 0) + dt * (this.world.hazardSpeed || 1);
      if (hazard.type === "fireJet") hazard.active = (hazard.timer % 2.4) > 1.45;
      const hitbox = hazard.type === "fireJet" && hazard.active
        ? { x: hazard.x + 4, y: hazard.y - 100, w: hazard.w - 8, h: hazard.h + 100 }
        : hazard;
      if ((hazard.always || hazard.active) && overlaps(this.player, hitbox)) {
        damagePlayer(this.player, 1, { x: -this.player.facing * 260, y: -480 });
      }
    }
  }

  update(dt) {
    if (!this.running || this.paused || this.deathLock) return;
    this.time += dt;
    if (this.input.consume("f1")) this.debug = !this.debug;
    if (this.input.consume("f2")) this.debugNextRoom();
    if (this.input.consume("f3")) this.debugBoss();
    if (this.input.consume("f4")) this.debugDeepEndless();
    if (this.input.consume("escape") || this.input.consume("p")) this.togglePause();

    updateMovers(this.world, this.time, dt);
    updatePlayer(this.player, this.world, this.input, dt);
    if (!this.bossController.boss) updateEnemies(this.world, this.player, dt);
    this.updateHazards(dt);
    const collected = collectPickups(this.world, this.player);
    if (collected.length) this.hud.showBanner("BLESSING RECEIVED", collected[0].replace(/([A-Z])/g, " $1").toUpperCase(), 1.25);

    if (this.input.consume("q")) useHolyLight(this.player, this.bossController);
    if (this.input.consume("e")) useFireRain(this.player, this.bossController);
    if (this.bossController.boss) this.bossController.update(this.world, this.player, dt, this.camera);

    if (this.mode === "journey" && this.world.checkpointX && !this.checkpointActivated && this.player.x >= this.world.checkpointX) {
      this.checkpointActivated = true;
      const checkpointSurface = surfaceAt(this.world, this.world.checkpointX + 28);
      const checkpointY = (checkpointSurface?.y || VIEW.floorY) - this.player.h;
      createCheckpoint(this, this.world.checkpointX + 28, checkpointY);
      this.hud.showBanner("CHECKPOINT LIT", "Journey progress secured", 1.4);
    }

    if (!this.bossController.boss && this.player.x >= this.world.width - 76) this.finishRoom();
    if (this.player.y > VIEW.height + 150) {
      damagePlayer(this.player, 1);
      if (this.player.hearts > 0) {
        this.player.x = this.player.lastSafe.x;
        this.player.y = this.player.lastSafe.y - 20;
        this.player.vx = 0;
        this.player.vy = 0;
      }
    }
    if (this.player.hearts <= 0) this.handleDeath();

    this.score = Math.max(this.score, this.distanceBank + this.player.x);
    this.camera.update(this.player, dt, this.world.width);
    this.hud.update(this, dt);
  }

  handleDeath() {
    if (this.deathLock) return;
    this.deathLock = true;
    if (this.mode === "journey") {
      this.hud.showBanner("RETURNING TO THE SHRINE", this.journeySave.checkpoint ? "Checkpoint restored" : "Restarting encounter", 1.25);
      setTimeout(() => {
        this.player.hearts = Math.max(2, this.player.maxHearts - 1);
        this.startJourneyNode(this.journeyNode.id, true);
      }, 850);
    } else {
      this.running = false;
      this.showGameOver();
    }
  }

  async showGameOver() {
    const overlay = document.getElementById("gameOver");
    document.getElementById("finalScore").textContent = Math.floor(this.score);
    overlay.classList.add("visible");
    const scores = await this.storage.scores();
    const cutoff = scores.length < 10 ? 0 : scores[scores.length - 1].score;
    document.getElementById("scoreEntry").hidden = this.score < cutoff && scores.length >= 10;
    this.renderScores(scores, document.getElementById("gameOverScores"));
  }

  async saveScore() {
    const input = document.getElementById("scoreName");
    const source = await this.storage.saveScore(input.value.trim() || "Anonymous", this.score);
    document.getElementById("scoreSaveStatus").textContent = `Saved to ${source}.`;
    this.renderScores(await this.storage.scores(), document.getElementById("gameOverScores"));
  }

  async showScores() {
    this.showPanel("ENDLESS HIGH SCORES", "Loading…");
    const scores = await this.storage.scores();
    const list = document.createElement("ol");
    list.className = "score-list";
    this.renderScores(scores, list);
    document.getElementById("overlayBody").replaceChildren(list);
  }

  renderScores(scores, target) {
    target.replaceChildren();
    const rows = scores.length ? scores : [{ name: "No scores yet", score: "—" }];
    for (const row of rows) {
      const item = document.createElement("li");
      const name = document.createElement("span");
      const score = document.createElement("b");
      name.textContent = row.name;
      score.textContent = row.score;
      item.append(name, score);
      target.appendChild(item);
    }
  }

  showHowToPlay() {
    this.showPanel("HOW TO PLAY", `
      <div class="help-grid">
        <section><b>Move</b><p>A/D or arrow keys. Mobile uses the left stick.</p></section>
        <section><b>Jump</b><p>W, Up, or Space. Hold for height. Coyote time and jump buffering are built in.</p></section>
        <section><b>Bosses</b><p>Red warnings are danger. Survive the pattern; strike while the boss glows gold. Stomp, Q Holy Light, or E Fire Rain.</p></section>
        <section><b>Journey</b><p>Choose routes, light midpoint shrines, defeat location-specific bosses, and reach Heaven.</p></section>
        <section><b>Endless</b><p>Death ends the run. Rooms stay physically valid while enemies, modifiers, and bosses intensify.</p></section>
        <section><b>Debug</b><p>F1 HUD · F2 next room · F3 boss · F4 deep Endless test.</p></section>
      </div>`);
  }

  showPanel(title, body) {
    document.getElementById("overlayTitle").textContent = title;
    document.getElementById("overlayBody").innerHTML = body;
    document.getElementById("infoOverlay").classList.add("visible");
  }

  hidePanel() {
    document.getElementById("infoOverlay").classList.remove("visible");
  }

  showJourneyVictory() {
    this.running = false;
    this.showPanel("JOURNEY COMPLETE", `<p class="victory-copy">The final trial is complete. Every route remains available from the Journey map, and your permanent blessings are preserved.</p><button class="primary" id="victoryMapBtn">Return to Journey Map</button>`);
    requestAnimationFrame(() => document.getElementById("victoryMapBtn")?.addEventListener("click", () => {
      this.hidePanel();
      this.showJourneyMap();
    }));
  }

  debugNextRoom() {
    if (!this.mode) return;
    this.bossController.boss = null;
    if (this.mode === "endless") {
      this.depth++;
      this.loadEndlessRoom();
    } else {
      this.loadRoom(generateRoom({ region: this.journeyNode?.region || "galilee", journeyDifficulty: Math.min(6, (this.world.tier || 1) + 1), seed: Date.now() }));
    }
  }

  debugBoss() {
    if (!this.mode) return;
    this.startBoss(bossForRegion(this.world.region), Math.max(1, this.world.tier || 1));
  }

  debugDeepEndless() {
    this.mode = "endless";
    this.depth = 14;
    this.score = 8000;
    this.running = true;
    this.setGameplayVisible(true);
    this.loadEndlessRoom();
  }

  frame(now) {
    const elapsed = Math.min(0.06, Math.max(0, (now - this.lastFrame) / 1000));
    this.lastFrame = now;
    this.fps += ((elapsed > 0 ? 1 / elapsed : 60) - this.fps) * 0.08;
    if (this.running && this.paused && (this.input.consume("escape") || this.input.consume("p"))) {
      this.togglePause();
    }
    if (this.running && !this.paused) this.accumulator += elapsed;
    let steps = 0;
    while (this.accumulator >= PHYSICS.fixedStep && steps < PHYSICS.maxSteps) {
      this.update(PHYSICS.fixedStep);
      this.accumulator -= PHYSICS.fixedStep;
      steps++;
    }
    if (steps) this.input.clearFrame();
    this.renderer.draw(this);
    requestAnimationFrame((time) => this.frame(time));
  }
}

window.jesusRunGame = new JesusRunGame();
