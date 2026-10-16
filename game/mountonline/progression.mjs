import {
  UNITS,
  ITEMS,
  UNIT_KEYS,
  armySize,
  armyCap,
  addXP,
  clamp,
} from "./data.mjs";
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
export function applyAction(p, world, type, input = {}) {
  requireThat(!p.activeBattle, "Finish the battle first.");
  if (type === "recruit") {
    nearSettlement(p, world, input.settlement);
    const n = clamp(Math.floor(input.count) || 1, 1, 10),
      price = n * UNITS.recruits.cost;
    requireThat(
      armySize(p.army) + n <= armyCap(p),
      "Your field army is full. Train leadership or use your reserve.",
    );
    requireThat(p.gold >= price, "You need more gold.");
    p.gold -= price;
    p.army.recruits += n;
    addXP(p, n * 2);
    return `Recruited ${n} soldiers.`;
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
    nearSettlement(p, world, input.settlement);
    requireThat(p.gold >= 30, "Supplies cost 30 gold.");
    requireThat(p.supplies < 200, "Your wagons are full.");
    p.gold -= 30;
    p.supplies = Math.min(200, p.supplies + 60);
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
    requireThat(
      ["crown", "ember", "vale"].includes(input.faction),
      "Unknown faction.",
    );
    const cost = input.peace ? 120 : 35;
    requireThat(p.gold >= cost, `This diplomatic mission costs ${cost} gold.`);
    p.gold -= cost;
    p.relations[input.faction] = clamp(
      (p.relations[input.faction] || 0) + (input.peace ? 45 : 12),
      -100,
      100,
    );
    return "Your envoy improved relations. Friendly patrols will leave you in peace.";
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
  const region = Math.floor(p.y / 8) * 16 + Math.floor(p.x / 8);
  if (!p.explored.includes(region)) p.explored.push(region);
  if (p.economyClock < 120) return null;
  p.economyClock -= 120;
  const owned = world.settlements.filter((s) => s.owner === ownerKey(p)),
    income = owned.reduce((n, s) => n + 24 * s.level, 0),
    upkeep = Math.ceil(armySize(p.army) / 30);
  const consumption = Math.max(
    1,
    Math.ceil(
      (armySize(p.army) / 12) *
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
    if (u.team !== team) continue;
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
