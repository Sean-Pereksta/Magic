import {
  WORLD,
  UNITS,
  clone,
  armySize,
  armyCap,
  clamp,
  newProfile,
  emptyArmy,
  safeArmy,
  addXP,
} from "./data.mjs";
import {
  createWorld,
  createNPCs,
  tickNPCs,
  travel,
  battleArmy,
} from "./world.mjs";
import {
  createBattle,
  tickBattle,
  battleView,
  issueOrder,
  useAbility,
} from "./battle.mjs";
import {
  applyAction,
  applyBattleResult,
  tickEconomy,
  requireThat,
  ownerKey,
} from "./progression.mjs";
function loseGarrison(c, amount) {
  const assigned = safeArmy(c.garrisonArmy),
    unassigned = Math.max(0, c.garrison - armySize(assigned));
  let trackedLoss = Math.max(0, amount - unassigned);
  for (const key of Object.keys(UNITS)) {
    const loss = Math.min(trackedLoss, assigned[key]);
    assigned[key] -= loss;
    trackedLoss -= loss;
  }
  c.garrisonArmy = assigned;
  c.garrison = Math.max(0, c.garrison - amount);
}
export class Realm {
  constructor({ time = 0, settlements, npcs, alliances = [] } = {}) {
    this.world = createWorld();
    if (settlements)
      for (const s of settlements) {
        const target = this.world.settlements.find((t) => t.id === s.id);
        if (target) Object.assign(target, s);
        else if (s.id.startsWith("legacy-"))
          this.world.settlements.push({ ...s });
      }
    this.npcs = npcs || createNPCs(this.world);
    this.time = time;
    this.players = new Map();
    this.battles = new Map();
    this.events = [];
    this.npcClock = 0;
    this.sequence = 0;
    this.alliances = alliances;
  }
  enter(id, profile = newProfile()) {
    const p = profile;
    p.uid = id;
    p.reserve ??= {};
    for (const key of Object.keys(UNITS)) p.reserve[key] ??= 0;
    p.inventory ??= { banners: [], equipped: [] };
    p.explored ??= [];
    p.route ??= [];
    p.routeIndex ??= 0;
    p.readyAt = this.time + 5;
    p.battleCooldown ??= 0;
    const safe = this.world.nav.nearest(p.x, p.y) || { x: 15, y: 44 };
    p.x = safe.x + 0.5;
    p.y = safe.y + 0.5;
    this.players.set(id, p);
    return p;
  }
  move(id, point) {
    const p = this.player(id);
    requireThat(!p.activeBattle, "Your army is in battle.");
    requireThat(
      Number.isFinite(point?.x) &&
        Number.isFinite(point?.y) &&
        point.x >= 0 &&
        point.y >= 0 &&
        point.x < WORLD.width &&
        point.y < WORLD.height,
      "Select a destination inside the realm.",
    );
    p.route = this.world.nav.path(p, point);
    p.routeIndex = 0;
    requireThat(p.route.length > 0, "No safe route reaches that destination.");
    return p.route;
  }
  player(id) {
    const p = this.players.get(id);
    requireThat(p, "Join the realm first.", "NOT_JOINED", 409);
    return p;
  }
  action(id, type, input) {
    const p = this.player(id);
    let message;
    if (type === "caravan") {
      const n = this.npcs.find(
        (n) => n.id === input.target && n.kind === "merchant",
      );
      requireThat(
        !p.activeBattle && n && Math.hypot(p.x - n.x, p.y - n.y) <= 3,
        "Travel closer to the caravan.",
      );
      requireThat(
        p.gold >= 20 && p.supplies < 200,
        "You need 20 gold and room in your supply wagons.",
      );
      p.gold -= 20;
      p.supplies = Math.min(200, p.supplies + 50);
      message = "Traded with the caravan: +50 supplies.";
    } else message = applyAction(p, this.world, type, input);
    if (type === "diplomacy")
      for (const c of this.world.settlements)
        if (c.owner === id && c.faction === input.faction)
          c.ownerReputation = p.relations[input.faction];
    p.revision++;
    this.log(message, id);
    return message;
  }
  log(message, uid = null) {
    this.events.push({ id: ++this.sequence, message, uid, time: this.time });
    if (this.events.length > 60) this.events.shift();
  }
  startBattle(id, kind, targetId) {
    const p = this.player(id);
    requireThat(!p.activeBattle, "Your company is already fighting.");
    requireThat(armySize(p.army) > 0, "Recruit troops before entering battle.");
    let target,
      armyB,
      names,
      defender = null,
      castle = null,
      npc = null;
    if (kind === "npc") {
      npc = this.npcs.find((n) => n.id === targetId);
      requireThat(
        npc && npc.respawn <= this.time && !npc.battleId,
        "This encounter has already moved on.",
      );
      target = npc;
      requireThat(npc.kind !== "merchant", "Caravans are peaceful.");
      armyB = npc.army;
      names = [p.username, npc.name];
    } else if (kind === "siege") {
      castle = this.world.settlements.find((s) => s.id === targetId);
      requireThat(castle?.kind === "castle", "Choose a castle to besiege.");
      requireThat(castle.owner !== id, "This castle belongs to your kingdom.");
      requireThat(!castle.battleId, "This castle is already under siege.");
      target = castle;
      const assigned = safeArmy(castle.garrisonArmy),
        base = Math.max(0, (castle.garrison ?? 24) - armySize(assigned));
      armyB = battleArmy(base, castle.faction);
      for (const key of Object.keys(UNITS)) armyB[key] += assigned[key];
      const owner = this.players.get(castle.owner);
      if (
        owner &&
        !owner.activeBattle &&
        Math.hypot(owner.x - castle.x, owner.y - castle.y) <= 6
      )
        defender = owner;
      names = [p.username, castle.name];
    } else if (kind === "pvp") {
      defender = this.player(targetId);
      requireThat(
        targetId !== id && !defender.activeBattle,
        "This commander is unavailable.",
      );
      requireThat(
        !p.allianceId || p.allianceId !== defender.allianceId,
        "Allied armies cannot attack each other.",
      );
      target = defender;
      armyB = defender.army;
      names = [p.username, defender.username];
    } else requireThat(false, "Choose an encounter.");
    requireThat(
      Math.hypot(p.x - target.x, p.y - target.y) <= 4,
      "Travel closer to engage this army or castle.",
    );
    if (castle?.owner) {
      const alliance = this.alliances.find((a) => a.id === p.allianceId);
      requireThat(
        !alliance?.members.includes(castle.owner),
        "Your alliance protects this castle.",
      );
    }
    const activeTroops = [...this.battles.values()]
      .filter((b) => !b.finished)
      .reduce((n, b) => n + b.units.length, 0);
    requireThat(
      activeTroops + armySize(p.army) + armySize(armyB) <= 4800,
      "The realm is resolving several large battles. Try again shortly.",
      "BATTLE_LIMIT",
      409,
    );
    const battleId = `battle-${Math.floor(this.time * 1000)}-${++this.sequence}-${id.slice(0, 8)}`;
    const b = createBattle({
      id: battleId,
      armyA: p.army,
      armyB,
      seed: (Math.floor(this.time * 1000) + this.sequence * 9931) >>> 0,
      siege: !!castle,
      names,
      morale: [p.morale, defender?.morale || 100],
      banners: [p.inventory.equipped, defender?.inventory?.equipped || []],
      tactics: [p.skills.tactics, defender?.skills?.tactics || 0],
    });
    b.participants = [id, defender?.uid || null];
    b.seatRoles = ["army", castle && defender ? "garrison" : "army"];
    b.encounter = { kind, targetId, owner: castle?.owner || null };
    b.settled = false;
    p.activeBattle = b.id;
    p.route = [];
    if (defender) {
      defender.activeBattle = b.id;
      defender.route = [];
    }
    if (castle) castle.battleId = b.id;
    if (npc) npc.battleId = b.id;
    this.battles.set(b.id, b);
    this.log(`${names[0]} engaged ${names[1]}.`);
    return b;
  }
  command(id, input) {
    const p = this.player(id),
      b = this.battles.get(p.activeBattle);
    requireThat(b, "Your army is not in a battle.");
    const team = b.participants.indexOf(id);
    requireThat(team >= 0, "This is not your battle.", "FORBIDDEN", 403);
    return input.ability
      ? useAbility(b, team, input.ability, input.point)
      : issueOrder(
          b,
          team,
          input.ids,
          input.order,
          input.point,
          input.formation,
        );
  }
  settle(b) {
    if (b.settled || !b.finished) return;
    b.settled = true;
    const reports = [];
    for (let team = 0; team < 2; team++) {
      const id = b.participants[team],
        p = this.players.get(id);
      if (!p) continue;
      if (b.seatRoles?.[team] === "garrison") {
        p.activeBattle = null;
        p.stats.battles++;
        if (b.winner === team) {
          p.stats.wins++;
          p.gold += 30;
          addXP(p, 35);
        }
        reports.push({ id, won: b.winner === team, garrison: true });
      } else reports.push({ id, ...applyBattleResult(p, b, team) });
      p.battleCooldown = this.time + 8;
      p.lastBattle = {
        id: b.id,
        won: b.winner === team,
        winner: b.winner,
        opponent: b.names[1 - team],
        time: this.time,
      };
      p.revision++;
    }
    const attacker = this.players.get(b.participants[0]),
      e = b.encounter;
    if (e.kind === "siege") {
      const c = this.world.settlements.find((s) => s.id === e.targetId);
      if (c?.battleId === b.id) {
        delete c.battleId;
        if (b.winner === 0 && c.owner === e.owner) {
          c.owner = b.participants[0];
          c.ownerName = attacker.username;
          c.ownerReputation = -30;
          c.garrison = 12;
          c.garrisonArmy = {};
          c.level = Math.max(1, c.level);
          attacker.stats.castles++;
          attacker.relations[c.faction] = Math.max(
            -100,
            (attacker.relations[c.faction] || 0) - 30,
          );
          this.log(`${attacker.username} raised their banner over ${c.name}.`);
        } else {
          c.garrisonArmy = emptyArmy();
          for (const u of b.units)
            if (u.team === 1 && u.hp > 0) c.garrisonArmy[u.key]++;
          c.garrison = armySize(c.garrisonArmy);
          if (c.garrison < 5) {
            c.garrisonArmy.recruits += 5 - c.garrison;
            c.garrison = 5;
          }
        }
      }
    }
    if (e.kind === "npc") {
      const n = this.npcs.find((n) => n.id === e.targetId);
      if (n) {
        n.battleId = null;
        n.respawn = this.time + 120;
        n.x = this.world.settlements.find((s) => s.id === n.home)?.x || n.x;
        n.y = this.world.settlements.find((s) => s.id === n.home)?.y || n.y;
      }
    }
    this.log(
      `${b.names[b.winner ?? 0]} ${b.winner == null ? "fought to a standstill" : "won the battle"}.`,
    );
    return reports;
  }
  tick(dt) {
    this.time += dt;
    for (const [id, p] of this.players) {
      p.px = p.x;
      p.py = p.y;
      if (p.activeBattle) continue;
      const speed =
        2.6 *
        (p.inventory.equipped.includes("wayfarer") ? 1.15 : 1) *
        (1 + (p.skills.logistics || 0) * 0.025) *
        (p.supplies > 0 ? 1 : 0.75);
      travel(p, this.world.nav, dt, speed);
      const message = tickEconomy(p, this.world, dt);
      if (message) this.log(message, id);
    }
    this.npcClock += dt;
    if (this.npcClock >= 0.2) {
      for (const n of this.npcs) {
        n.px = n.x;
        n.py = n.y;
        n.updatedAt = this.time;
      }
      tickNPCs(
        this.world,
        this.npcs,
        [...this.players.values()],
        this.npcClock,
        this.time,
      );
      // A neglected holding can fall to a hostile patrol. Strong garrisons repel
      // raids, giving development and returning to defend a concrete purpose.
      for (const c of this.world.settlements) c.underAttack = false;
      for (const n of this.npcs) {
        if (n.kind !== "patrol" || n.battleId || n.respawn > this.time)
          continue;
        const c = this.world.settlements.find(
          (c) =>
            c.kind === "castle" &&
            c.owner &&
            c.faction === n.faction &&
            Math.hypot(c.x - n.x, c.y - n.y) < 2.6,
        );
        if (!c || c.battleId || (c.ownerReputation ?? -30) > -25) continue;
        c.raidPressure = (c.raidPressure || 0) + this.npcClock;
        c.underAttack = true;
        if (c.raidPressure >= 20) {
          c.raidPressure = 0;
          const strength = armySize(n.army);
          if (c.garrison >= strength) {
            n.respawn = this.time + 120;
            c.underAttack = false;
            this.log(`${c.name}'s garrison repelled a faction patrol.`);
          } else {
            loseGarrison(c, 8);
            this.log(
              `${c.name} is under attack. Reinforce its garrison or intercept the patrol.`,
              c.owner,
            );
            if (!c.garrison) {
              this.log(`${c.name} fell back into ${c.faction} hands.`);
              c.owner = null;
              c.ownerName = null;
              c.garrison = 16;
              c.garrisonArmy = {};
              c.underAttack = false;
              n.respawn = this.time + 90;
            }
          }
        }
      }
      this.npcClock = 0;
    }
    for (const b of this.battles.values()) {
      tickBattle(b, dt);
      if (b.finished && !b.settled) this.settle(b);
    }
    for (const [id, b] of this.battles)
      if (
        b.settled &&
        this.time - (b.completedAt ?? (b.completedAt = this.time)) > 90
      )
        this.battles.delete(id);
    // Aggressive outlaws and offended patrols intercept travelers. Never fabricate PvP outcomes locally.
    for (const [id, p] of this.players) {
      if (
        p.activeBattle ||
        p.readyAt > this.time ||
        p.battleCooldown > this.time
      )
        continue;
      const n = this.npcs.find(
        (n) =>
          !n.battleId &&
          n.respawn <= this.time &&
          (n.kind === "bandit" ||
            (n.kind === "patrol" && (p.relations[n.faction] || 0) < -25)) &&
          Math.hypot(n.x - p.x, n.y - p.y) < 0.85,
      );
      if (n && armySize(p.army)) {
        try {
          this.startBattle(id, "npc", n.id);
        } catch (error) {
          if (error.code !== "BATTLE_LIMIT") throw error;
        }
      }
    }
  }
  snapshot(id) {
    const p = this.player(id),
      b =
        this.battles.get(p.activeBattle) ||
        [...this.battles.values()]
          .reverse()
          .find(
            (b) =>
              b.participants.includes(id) &&
              b.settled &&
              b.id === p.lastBattle?.id,
          );
    return {
      time: this.time,
      sequence: this.sequence,
      profile: clone(p),
      settlements: clone(this.world.settlements),
      npcs: this.npcs
        .filter(
          (n) =>
            n.respawn <= this.time && Math.hypot(n.x - p.x, n.y - p.y) < 38,
        )
        .map(({ route, ...n }) => clone(n)),
      others: [...this.players.entries()]
        .filter(
          ([uid, o]) => uid !== id && Math.hypot(o.x - p.x, o.y - p.y) < 38,
        )
        .slice(0, 40)
        .map(([uid, o]) => ({
          id: uid,
          username: o.username,
          x: o.x,
          y: o.y,
          armySize: armySize(o.army),
          activeBattle: o.activeBattle,
          allianceId: o.allianceId || null,
        })),
      events: this.events.filter((e) => !e.uid || e.uid === id).slice(-12),
      battle: b ? battleView(b) : null,
      team: b?.participants.indexOf(id) ?? 0,
    };
  }
  checkpoint() {
    return {
      time: this.time,
      sequence: this.sequence,
      settlements: clone(this.world.settlements),
      npcs: clone(this.npcs),
      alliances: clone(this.alliances),
    };
  }
}
