import {
  UNITS,
  WORLD,
  FACTIONS,
  ITEMS,
  UNIT_KEYS,
  armySize,
  armyCap,
  addXP,
  clamp,
} from "./data.mjs";
import { createKingdomState, diplomaticAction, playerTreaty, playerHostile } from "./kingdoms.mjs";
import { BIOMES } from "./world.mjs";
export class GameError extends Error {
  constructor(message, code = "INVALID_ACTION", status = 400) {
    super(message);
    this.code = code;
    this.status = status;
  }
}
export const requireThat = (ok, message, code, status) => {
  if (!ok) throw new GameError(message, code, status);
};
export const ownerKey = (p) => p.uid || p.username;
export function nearSettlement(p, world, id) {
  const s = world.settlements.find((s) => s.id === id);
  requireThat(
    s && Math.hypot(p.x - s.x, p.y - s.y) <= 3,
    "Travel closer to this settlement.",
  );
  return s;
}
export function signatureRecruitment(p, world, s, key) {
  const unit = UNITS[key];
  if (!unit?.faction) return { allowed: key === "recruits", reason: "Train shared troops through the promotion roster.", price: unit?.cost || 0 };
  const time = world.time || 0, faction = s.currentFaction || s.faction, owned = s.owner === ownerKey(p), home = unit.faction === faction;
  const agreement = ["trade", "militaryAlliance", "defensiveAlliance", "protection"].some((type) => playerTreaty(p, unit.faction, type, time));
  const mercenary = faction === "suncoast" && ["town", "capital"].includes(s.kind) && agreement;
  const reputation = (p.relations?.[unit.faction] || 0) >= (unit.tier >= 4 ? 35 : 15);
  let reason = "";
  if (!home && !mercenary) reason = `Recruit this unit in its homeland or through a Suncoast mercenary contract.`;
  else if (playerHostile(p, world.kingdoms, unit.faction, time)) reason = "Recruitment is closed while your kingdoms are hostile.";
  else if (!owned && !agreement && !reputation) reason = `Requires ${unit.tier >= 4 ? 35 : 15} relations, a trade agreement, or an alliance.`;
  else if ((s.recruitment || 1) + (s.level || 1) - 1 < unit.tier - 2) reason = "Develop this settlement's recruitment buildings for elite troops.";
  else if (p.level < unit.tier - 1) reason = `Reach commander level ${unit.tier - 1}.`;
  else if ((s.loyalty ?? 100) < 25 || (s.damage || 0) > 65) reason = "Local resistance or raid damage prevents specialty recruitment.";
  return { allowed: !reason, reason, price: Math.ceil(unit.cost * (mercenary ? 1.35 : agreement ? 0.9 : 1)) };
}
export function applyAction(p, world, type, input = {}) {
  requireThat(!p.activeBattle, "Finish the battle first.");
  if (type === "recruit") {
    const s = nearSettlement(p, world, input.settlement), key = input.unit || "recruits", unit = UNITS[key];
    requireThat(unit, "Choose an available troop.");
    const eligibility = signatureRecruitment(p, world, s, key);
    requireThat(eligibility.allowed, eligibility.reason);
    requireThat(s.owner === ownerKey(p) || !playerHostile(p, world.kingdoms, s.currentFaction || s.faction, world.time || 0), "Hostile settlements will not recruit for your army.");
    const n = clamp(Math.floor(input.count) || 1, 1, 10),
      price = n * eligibility.price;
    requireThat((s.recruitStock ?? 30) >= n && (s.damage || 0) < 80, "This settlement needs time to recover its recruits after raids.");
    requireThat(
      armySize(p.army) + n <= armyCap(p),
      "Your field army is full. Train leadership or use your reserve.",
    );
    requireThat(p.gold >= price, "You need more gold.");
    p.gold -= price;
    p.army[key] = (p.army[key] || 0) + n;
    s.recruitStock = (s.recruitStock ?? 30) - n;
    addXP(p, n * 2);
    return `Recruited ${n} ${key === "recruits" ? "soldiers" : unit.name.toLowerCase()}${key === "recruits" ? "" : " troops"}.`;
  }
  if (type === "train") {
    const unit = UNITS[input.unit];
    requireThat(unit?.from, "Choose an available promotion.");
    const n = clamp(Math.floor(input.count) || 1, 1, 10),
      cost = unit.cost * n;
    requireThat(
      p.level >= unit.tier - 1,
      `Reach commander level ${unit.tier - 1} first.`,
    );
    requireThat(
      p.army[unit.from] >= n,
      `You need ${n} ${UNITS[unit.from].name.toLowerCase()} troops.`,
    );
    requireThat(p.gold >= cost, `Training costs ${cost} gold.`);
    p.gold -= cost;
    p.army[unit.from] -= n;
    p.army[input.unit] += n;
    addXP(p, n * 4);
    return `Trained ${n} ${unit.name.toLowerCase()} troops.`;
  }
  if (type === "reserve") {
    requireThat(UNITS[input.unit], "Unknown troop.");
    const from = input.withdraw ? p.reserve : p.army,
      to = input.withdraw ? p.army : p.reserve;
    requireThat(from[input.unit] > 0, "No troops available.");
    requireThat(
      !input.withdraw || armySize(p.army) < armyCap(p),
      "Your field army is full.",
    );
    from[input.unit]--;
    to[input.unit]++;
    return "Army updated.";
  }
  if (type === "supplies") {
    const s = nearSettlement(p, world, input.settlement), faction = s.currentFaction || s.faction, trade = playerTreaty(p, faction, "trade", world.time || 0), price = trade ? 24 : 30;
    requireThat(s.owner === ownerKey(p) || !playerHostile(p, world.kingdoms, faction, world.time || 0), "Hostile settlements cannot supply your company.");
    requireThat((s.supplies ?? 100) >= 20 && !s.battleId, "This settlement's supply route is disrupted.");
    requireThat(p.gold >= price, `Supplies cost ${price} gold.`);
    requireThat(p.supplies < 200, "Your wagons are full.");
    p.gold -= price;
    p.supplies = Math.min(200, p.supplies + 60);
    s.supplies = Math.max(0, (s.supplies ?? 100) - 20);
    p.morale = Math.min(100, p.morale + 10);
    return "Wagons restocked. Morale restored.";
  }
  if (type === "rest") {
    nearSettlement(p, world, input.settlement);
    requireThat(p.gold >= 15, "Rest costs 15 gold.");
    p.gold -= 15;
    p.morale = 100;
    return "Your army is rested and ready.";
  }
  if (type === "buyBanner") {
    nearSettlement(p, world, input.settlement);
    const item = ITEMS[input.item];
    requireThat(item, "Unknown standard.");
    requireThat(
      !p.inventory.banners.includes(input.item),
      "You already own this standard.",
    );
    requireThat(p.gold >= item.cost, `This costs ${item.cost} gold.`);
    p.gold -= item.cost;
    p.inventory.banners.push(input.item);
    return `${item.name} purchased.`;
  }
  if (type === "equip") {
    requireThat(
      ITEMS[input.item] && p.inventory.banners.includes(input.item),
      "You do not own this standard.",
    );
    const i = p.inventory.equipped.indexOf(input.item);
    if (i >= 0) p.inventory.equipped.splice(i, 1);
    else {
      requireThat(
        p.inventory.equipped.length < 3,
        "Equip at most three standards.",
      );
      p.inventory.equipped.push(input.item);
    }
    return "Standards updated.";
  }
  if (type === "skill") {
    requireThat(
      ["leadership", "logistics", "tactics"].includes(input.skill),
      "Unknown skill.",
    );
    requireThat(p.skillPoints > 0, "Earn a commander level for a skill point.");
    requireThat(p.skills[input.skill] < 10, "This skill is mastered.");
    p.skillPoints--;
    p.skills[input.skill]++;
    return "Commander skill improved.";
  }
  if (type === "build") {
    const s = nearSettlement(p, world, input.settlement);
    requireThat(
      s.owner === ownerKey(p),
      "Only your kingdom can develop this castle.",
    );
    requireThat(s.level < 5, "This castle is fully developed.");
    const cost = s.level * 120;
    requireThat(
      p.gold >= cost && p.supplies >= 30,
      `Development requires ${cost} gold and 30 supplies.`,
    );
    p.gold -= cost;
    p.supplies -= 30;
    s.level++;
    s.recruitment = Math.min(5, (s.recruitment || 1) + 1);
    s.prosperity = (s.prosperity || 1) + 1;
    s.garrison += 8;
    return `${s.name} upgraded: stronger walls, garrison, and income.`;
  }
  if (type === "garrison") {
    const s = nearSettlement(p, world, input.settlement);
    requireThat(
      s.owner === ownerKey(p),
      "Only the owner can manage this garrison.",
    );
    requireThat(
      UNITS[input.unit] && p.army[input.unit] > 0,
      "Select an available troop.",
    );
    requireThat(s.garrison < 400, "The garrison is full.");
    p.army[input.unit]--;
    s.garrisonArmy ??= {};
    s.garrisonArmy[input.unit] = (s.garrisonArmy[input.unit] || 0) + 1;
    s.garrison++;
    return "Troop assigned to castle defense.";
  }
  if (type === "diplomacy") {
    if (!world.kingdoms) createKingdomState(world);
    return diplomaticAction(p, world, input, world.time || 0);
  }
  if (type === "contract") {
    const contract = CONTRACTS.find((c) => c.id === input.id);
    requireThat(contract, "Unknown contract.");
    requireThat(
      !p.claimedContracts.includes(contract.id),
      "This contract has already been rewarded.",
    );
    if (!p.contracts.includes(contract.id)) {
      p.contracts.push(contract.id);
      return "Contract accepted. Track it in your journal.";
    }
    requireThat(
      contract.progress(p, world) >= contract.goal,
      "The contract is still in progress.",
    );
    p.claimedContracts.push(contract.id);
    p.gold += contract.gold;
    p.renown += 15;
    p.stats.contracts++;
    addXP(p, 50);
    if (contract.item && !p.inventory.banners.includes(contract.item))
      p.inventory.banners.push(contract.item);
    return `Contract completed: +${contract.gold} gold and +50 XP.`;
  }
  throw new GameError("Unknown action.");
}
export const CONTRACTS = [
  {
    id: "first-blood",
    name: "Roadwarden",
    detail: "Win your first battle.",
    goal: 1,
    gold: 120,
    item: "wayfarer",
    progress: (p) => p.stats.wins,
  },
  {
    id: "veteran",
    name: "A company of renown",
    detail: "Win five battles.",
    goal: 5,
    gold: 300,
    progress: (p) => p.stats.wins,
  },
  {
    id: "explorer",
    name: "Beyond the river",
    detail: "Explore twelve regions of the realm.",
    goal: 12,
    gold: 180,
    progress: (p) => p.explored.length,
  },
  {
    id: "landed",
    name: "A banner on the walls",
    detail: "Conquer your first castle.",
    goal: 1,
    gold: 240,
    progress: (p) => p.stats.castles,
  },
];
export function tickEconomy(p, world, dt) {
  p.playTime = (p.playTime || 0) + dt;
  p.economyClock = (p.economyClock || 0) + dt;
  const region = Math.floor(p.y / 8) * Math.ceil(WORLD.width / 8) + Math.floor(p.x / 8);
  if (!p.explored.includes(region)) p.explored.push(region);
  if (p.economyClock < 120) return null;
  p.economyClock -= 120;
  const owned = world.settlements.filter((s) => s.owner === ownerKey(p)),
    income = Math.floor(owned.reduce((n, s) => n + 24 * s.level * (1 - (s.damage || 0) / 100) * ((s.supplies ?? 100) < 20 ? 0.4 : 1) * ((s.loyalty ?? 100) < 25 ? 0.5 : 1), 0)),
    upkeep = Math.ceil(armySize(p.army) / 30) + owned.reduce((n, s) => n + Math.ceil(Math.max(0, s.garrison - 40) / 20), 0);
  const nearest = world.settlements.reduce((best, s) => !best || Math.hypot(s.x - p.x, s.y - p.y) < Math.hypot(best.x - p.x, best.y - p.y) ? s : best, null),
    hostile = nearest && !nearest.owner && playerHostile(p, world.kingdoms, nearest.currentFaction || nearest.faction, world.time || 0),
    biome = world.biomes?.[Math.floor(p.y) * world.width + Math.floor(p.x)],
    supplyFactor = (hostile ? 1.6 : 1) * (biome === BIOMES.desert ? 1.25 : 1);
  const consumption = Math.max(
    1,
    Math.ceil(
      (armySize(p.army) / 12) * supplyFactor *
        (1 - (p.skills.logistics || 0) * 0.045) *
        (p.inventory.equipped.includes("quartermaster") ? 0.75 : 1),
    ),
  );
  p.gold = Math.max(0, p.gold + income - upkeep);
  p.supplies = Math.max(0, p.supplies - consumption);
  p.morale = clamp(p.morale + (p.supplies ? 2 : -8), 20, 100);
  return `Kingdom income ${income}g · upkeep ${upkeep}g · supplies used ${consumption}.`;
}
export function applyBattleResult(p, battle, team) {
  const survivors = Object.fromEntries(UNIT_KEYS.map((k) => [k, 0]));
  let wounded = 0;
  for (const u of battle.units) {
    if (u.team !== team || u.temporary) continue;
    if (u.hp > 0 || u.routed) survivors[u.key]++;
    else if (u.id % 4 === 0) {
      survivors[u.key]++;
      wounded++;
    }
  }
  p.army = survivors;
  p.activeBattle = null;
  p.stats.battles++;
  const won = battle.winner === team;
  if (won) {
    p.stats.wins++;
    const reward =
      50 + Math.round(battle.units.filter((u) => u.team !== team).length * 2.5);
    p.gold += reward;
    p.renown += 10;
    addXP(
      p,
      35 + battle.units.filter((u) => u.team !== team && u.hp <= 0).length * 3,
    );
    if (!p.achievements.includes("first-victory"))
      p.achievements.push("first-victory");
  } else {
    p.morale = Math.max(25, p.morale - 15);
    addXP(p, 12);
  }
  // A lost company can rebuild immediately rather than waiting for passive income.
  if (armySize(p.army) === 0) {
    p.army.recruits = 8;
    p.gold = Math.max(p.gold, 96);
    p.x = 15.5;
    p.y = 44.5;
  }
  return { won, wounded, survivors: armySize(p.army) };
}
