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
  BIOMES,
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
import {
  createKingdomState, ensureDiplomacy, diplomaticAction, playerHostile,
  playerTreaty, tickKingdoms, recordKingdomBattle, kingdomView, supportPlayerDefense,
} from "./kingdoms.mjs";
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
  constructor({ time = 0, settlements, npcs, alliances = [], kingdoms, seed = WORLD.seed, sequence = 0 } = {}) {
    this.world = createWorld(seed);
    if (settlements)
      for (const s of settlements) {
        const target = this.world.settlements.find((t) => t.id === s.id);
        if (target) Object.assign(target, s);
        else if (s.id.startsWith("legacy-"))
          this.world.settlements.push({ ...s });
      }
    this.npcs = createNPCs(this.world);
    if (Array.isArray(npcs)) for (const saved of npcs) {
      const target = this.npcs.find((n) => n.id === saved.id);
      if (target) Object.assign(target, saved, { army: safeArmy(saved.army) });
    }
    this.world.npcs = this.npcs;
    this.kingdoms = createKingdomState(this.world, kingdoms);
    this.time = time;
    this.world.time = time;
    this.players = new Map();
    this.battles = new Map();
    this.events = [];
    this.npcClock = 0;
    this.sequence = sequence;
    this.alliances = alliances;
  }
  enter(id, profile = newProfile()) {
    const p = profile;
    p.uid = id;
    p.army = safeArmy(p.army);
    p.reserve = safeArmy(p.reserve);
    p.inventory ??= { banners: [], equipped: [] };
    p.explored ??= [];
    p.route ??= [];
    p.routeIndex ??= 0;
    p.readyAt = this.time + 5;
    p.battleCooldown ??= 0;
    ensureDiplomacy(p);
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
    // Elected-host checkpoints can contain profiles from before the expansion.
    // Normalize lazily here as well as on entry so reconnecting old seats work.
    if (Object.keys(UNITS).some((key) => !Number.isFinite(p.army?.[key]))) p.army = safeArmy(p.army);
    if (Object.keys(UNITS).some((key) => !Number.isFinite(p.reserve?.[key]))) p.reserve = safeArmy(p.reserve);
    ensureDiplomacy(p);
    return p;
  }
  action(id, type, input = {}) {
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
    } else if (type === "diplomacy") message = diplomaticAction(p, this.world, input, this.time);
    else message = applyAction(p, this.world, type, input);
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
      npc = null,
      merchant = null,
      ally = null;
    const npcVariant = ["npc", "defensive", "ambush", "rearguard", "reinforcement", "caravanDefense"].includes(kind);
    if (npcVariant) {
      if (kind === "caravanDefense") {
        merchant = this.npcs.find((n) => n.id === targetId && n.kind === "merchant" && !n.battleId && n.respawn <= this.time);
        requireThat(merchant, "Choose an available merchant caravan.");
        npc = this.npcs.filter((n) => n.kind === "bandit" && !n.battleId && n.respawn <= this.time && Math.hypot(n.x - merchant.x, n.y - merchant.y) < 24)
          .sort((a, b) => Math.hypot(a.x - merchant.x, a.y - merchant.y) - Math.hypot(b.x - merchant.x, b.y - merchant.y))[0];
        requireThat(npc, "No raiders currently threaten this caravan.");
      } else npc = this.npcs.find((n) => n.id === targetId);
      requireThat(
        npc && !npc.capturedBy && npc.respawn <= this.time && !npc.battleId,
        "This encounter has already moved on.",
      );
      target = merchant || npc;
      requireThat(npc.kind !== "merchant", "Caravans are peaceful.");
      if (npc.kind === "patrol") requireThat(
        !["pact", "peace", "ceasefire", "militaryAlliance", "defensiveAlliance", "protection", "tribute"].some((t) => playerTreaty(p, npc.faction, t, this.time)),
        "Honor your agreement or break it through diplomacy before attacking.",
      );
      armyB = npc.army;
      names = [p.username, npc.name];
      if (kind === "reinforcement") {
        ally = this.npcs.find((n) => n.lord && !n.battleId && !n.capturedBy && n.respawn <= this.time && n.faction !== npc.faction && Math.hypot(n.x - p.x, n.y - p.y) <= 30 && ["militaryAlliance", "defensiveAlliance", "protection"].some((type) => playerTreaty(p, n.faction, type, this.time)));
        requireThat(ally, "Relief requires an available allied lord within 30 tiles. Negotiate an alliance first.");
        requireThat(armySize(p.army) + armySize(ally.army) <= 600, "Leave room below the 600 soldier field limit for the allied relief force.");
      }
    } else if (kind === "siege" || kind === "raid") {
      castle = this.world.settlements.find((s) => s.id === targetId);
      requireThat((kind === "raid" ? ["village"] : ["castle", "town", "capital"]).includes(castle?.kind), kind === "raid" ? "Choose a village to raid." : "Choose a castle, town, or capital to besiege.");
      requireThat(kind !== "raid" || (castle.raidedUntil || 0) <= this.time, "This village has already been raided and is recovering.");
      requireThat(castle.owner !== id, "This castle belongs to your kingdom.");
      requireThat(!castle.battleId, "This castle is already under siege.");
      if (!castle.owner) requireThat(
        !["pact", "peace", "ceasefire", "militaryAlliance", "defensiveAlliance", "protection", "tribute"].some((t) => playerTreaty(p, castle.currentFaction || castle.faction, t, this.time)),
        "Honor your agreement or break it through diplomacy before besieging this holding.",
      );
      target = castle;
      const assigned = safeArmy(castle.garrisonArmy),
        base = Math.max(0, (kind === "raid" ? Math.max(8, castle.garrison || 0) : castle.garrison ?? 24) - armySize(assigned));
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
      .reduce((n, b) => n + b.units.length + (b.objective?.waves || []).filter(w => !w.delivered).reduce((sum,w) => sum + armySize(w.army), 0), 0);
    requireThat(armySize(p.army) + armySize(armyB) + (ally ? armySize(ally.army) : 0) <= 1200,
      "This field cannot admit another reinforcement company. Choose a smaller force or a field battle.", "BATTLE_LIMIT", 409);
    requireThat(
      activeTroops + armySize(p.army) + armySize(armyB) + (ally ? armySize(ally.army) : 0) <= 4800,
      "The realm is resolving several large battles. Try again shortly.",
      "BATTLE_LIMIT",
      409,
    );
    const hostileFaction = npc?.kind === "patrol" ? npc.faction : castle && !castle.owner ? castle.currentFaction || castle.faction : null;
    if (hostileFaction && !p.diplomacy.wars[hostileFaction])
      this.log(diplomaticAction(p, this.world, { faction: hostileFaction, action: "war" }, this.time), id);
    const terrain = Object.keys(BIOMES)[this.world.biomes?.[Math.floor(target.y) * this.world.width + Math.floor(target.x)]] || "plains";
    const encounterType = kind === "npc" ? (npc.kind === "bandit" && ["forest", "marsh"].includes(terrain) ? "ambush" : "field") : kind === "pvp" ? "field" : kind;
    const objective = kind === "defensive" ? { type: kind, team: 0, x: 16, y: 20, radius: 6, duration: 60 }
      : kind === "rearguard" ? { type: kind, team: 0, delay: 25, duration: 100, required: Math.max(1, Math.ceil(armySize(p.army) * 0.5)) }
      : kind === "reinforcement" ? { type: kind, team: 0, x: 17, y: 20, radius: 7, duration: 60, waves: [{ at: 30, team: 0, army: clone(ally.army), spawn: { x: 5, y: 20 } }] }
      : kind === "caravanDefense" ? { type: kind, team: 0, x: 12, y: 20, targetX: 52, targetY: 20, hp: 360, duration: 120 }
      : kind === "raid" ? { type: kind, team: 0, x: 49, y: 20, radius: 5, hp: 220, duration: 120 } : undefined;
    const battleId = `battle-${Math.floor(this.time * 1000)}-${++this.sequence}-${id.slice(0, 8)}`;
    const b = createBattle({
      id: battleId,
      armyA: p.army,
      armyB,
      seed: (Math.floor(this.time * 1000) + this.sequence * 9931) >>> 0,
      siege: kind === "siege",
      names,
      morale: [p.morale, defender?.morale || 100],
      banners: [p.inventory.equipped, defender?.inventory?.equipped || []],
      tactics: [p.skills.tactics, defender?.skills?.tactics || 0],
      factions: [p.faction || "crown", npc?.faction || castle?.currentFaction || castle?.faction || defender?.faction || "outlaw"],
      commanders: [false, !defender],
      terrain, encounterType, objective,
    });
    b.participants = [id, defender?.uid || null];
    b.seatRoles = ["army", castle && defender ? "garrison" : "army"];
    b.encounter = { kind: npcVariant ? "npc" : kind, variant: encounterType, targetId: npc?.id || targetId, owner: castle?.owner || null, ...(merchant ? { merchantId: merchant.id } : {}), ...(ally ? { allyLord: ally.id } : {}) };
    b.settled = false;
    p.activeBattle = b.id;
    p.route = [];
    if (defender) {
      defender.activeBattle = b.id;
      defender.route = [];
    }
    if (castle) castle.battleId = b.id;
    if (npc) npc.battleId = b.id;
    if (merchant) merchant.battleId = b.id;
    if (ally) ally.battleId = b.id;
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
        n.army = emptyArmy();
        for (const u of b.units) if (u.team === 1 && (u.hp > 0 || u.routed)) n.army[u.key]++;
        if (!armySize(n.army)) n.army.recruits = n.kind === "bandit" ? 5 : 8;
        n.x = this.world.settlements.find((s) => s.id === n.home)?.x || n.x;
        n.y = this.world.settlements.find((s) => s.id === n.home)?.y || n.y;
      }
    }
    if (e.kind === "raid") {
      const s = this.world.settlements.find((s) => s.id === e.targetId);
      if (s?.battleId === b.id) {
        delete s.battleId;
        s.raidedUntil = this.time + 120;
        if (b.winner === 0) {
          const k = this.kingdoms.factions[s.currentFaction || s.faction], gold = Math.min(80, Math.floor(k.wealth)), supplies = Math.min(40, s.supplies ?? 100);
          k.wealth -= gold; attacker.gold += gold; attacker.supplies = Math.min(200, attacker.supplies + supplies);
          s.supplies = Math.max(0, (s.supplies ?? 100) - supplies); s.food = Math.max(0, (s.food ?? 50) - 30); s.damage = Math.min(90, (s.damage || 0) + 40); s.production = Math.max(0.2, (s.production ?? 1) - 0.4); s.recruitStock = Math.max(0, (s.recruitStock ?? 30) - 15);
          this.log(`${attacker.username} raided ${s.name}: ${gold} gold and ${supplies} supplies seized.`);
        }
      }
    }
    if (e.merchantId) {
      const m = this.npcs.find((n) => n.id === e.merchantId);
      if (m?.battleId === b.id) {
        m.battleId = null;
        if (b.winner === 0) { attacker.gold += 60; attacker.renown += 5; attacker.relations[m.faction] = Math.min(100, attacker.relations[m.faction] + 8); this.log(`${m.name} was protected; its grateful merchants paid 60 gold.`, attacker.uid); }
        else { m.respawn = this.time + 180; this.kingdoms.factions[m.faction].wealth = Math.max(0, this.kingdoms.factions[m.faction].wealth - 60); }
      }
    }
    if (e.allyLord) {
      const ally = this.npcs.find((n) => n.id === e.allyLord), wave = b.objective?.waves?.find((w) => w.team === 0);
      if (ally?.battleId === b.id) {
        ally.battleId = null;
        if (wave?.delivered) { ally.army = emptyArmy(); for (const u of b.units) if (u.team === 0 && u.temporary && (u.hp > 0 || u.routed)) ally.army[u.key]++; if (!armySize(ally.army)) { ally.army.recruits = 8; ally.respawn = this.time + 120; } }
      }
    }
    recordKingdomBattle(this, b);
    this.log(
      `${b.names[b.winner ?? 0]} ${b.winner == null ? "fought to a standstill" : "won the battle"}.`,
    );
    return reports;
  }
  tick(dt) {
    this.time += dt;
    this.world.time = this.time;
    for (const [id, p] of this.players) {
      p.px = p.x;
      p.py = p.y;
      if (p.activeBattle) continue;
      const speed =
        2.6 *
        (p.inventory.equipped.includes("wayfarer") ? 1.15 : 1) *
        (1 + (p.skills.logistics || 0) * 0.025) *
        (p.supplies > 0 ? 1 : 0.75);
      const local = this.world.settlements.reduce((best, s) => !best || Math.hypot(s.x - p.x, s.y - p.y) < Math.hypot(best.x - p.x, best.y - p.y) ? s : best, null);
      travel(p, this.world.nav, dt, speed * (local && playerTreaty(p, local.currentFaction || local.faction, "passage", this.time) ? 1.08 : 1));
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
        if (n.kind !== "patrol" || n.battleId || n.capturedBy || n.respawn > this.time)
          continue;
        const c = this.world.settlements.find(
          (c) =>
            c.kind === "castle" &&
            c.owner &&
            c.faction === n.faction &&
            Math.hypot(c.x - n.x, c.y - n.y) < 2.6,
        );
        const owner = c && this.players.get(c.owner);
        if (!c || c.battleId || (c.ownerReputation ?? -30) > -25 || (owner && ["peace", "ceasefire", "pact", "militaryAlliance", "defensiveAlliance", "protection", "tribute"].some((t) => playerTreaty(owner, n.faction, t, this.time)))) continue;
        c.raidPressure = (c.raidPressure || 0) + this.npcClock;
        c.underAttack = true;
        if (owner) supportPlayerDefense(this.world, owner, n.faction, this.time);
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
    tickKingdoms(this, dt);
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
          !n.capturedBy &&
          n.respawn <= this.time &&
          (n.kind === "bandit" ||
            (n.kind === "patrol" && playerHostile(p, this.kingdoms, n.faction, this.time))) &&
          Math.hypot(n.x - p.x, n.y - p.y) < 0.85,
      );
      if (n && armySize(p.army)) {
        try {
          if (n.kind === "patrol") supportPlayerDefense(this.world, p, n.faction, this.time);
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
      kingdoms: kingdomView(this, p),
      settlements: clone(this.world.settlements),
      npcs: this.npcs
        .filter(
          (n) =>
            !n.capturedBy && n.respawn <= this.time && Math.hypot(n.x - p.x, n.y - p.y) < 38,
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
      seed: this.world.seed,
      worldVersion: this.world.version,
      settlements: clone(this.world.settlements),
      npcs: clone(this.npcs),
      alliances: clone(this.alliances),
      kingdoms: clone(this.kingdoms),
    };
  }
}
