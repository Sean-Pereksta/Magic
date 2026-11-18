/* Stable late-game tile IDs, recipes and ports. Simulation lives in the game systems. */
(function (root, factory) {
  if (typeof module === 'object' && module.exports) module.exports = factory();
  else root.CircuitRoboticsCatalog = factory();
})(globalThis, function () {
  'use strict';

  const ores = [
    ['COBALT_ORE', 110, 'cobaltOre', 'Cobalt Ore', '#619ada', 2.8, 2],
    ['IRIDIUM_ORE', 111, 'iridiumOre', 'Iridium Ore', '#c57ddb', 4.2, 3],
    ['AETHERIUM_ORE', 112, 'aetheriumOre', 'Aetherium Ore', '#82ffdb', 6.2, 4]
  ];

  const goods = [
    ['cobaltIngot', 'Cobalt Ingot', '▰', '#79ace5'],
    ['iridiumIngot', 'Iridium Ingot', '▰', '#d2a0e5'],
    ['aetheriumIngot', 'Aetherium Ingot', '▰', '#8dffdd'],
    ['precisionBearing', 'Precision Bearing', '◎', '#a5cbc9'],
    ['roboticFrame', 'Robotic Frame', '▧', '#88a6a9'],
    ['servoCore', 'Servo Core', '⊙', '#99dcf3'],
    ['fluxCell', 'Flux Cell', '▬', '#a5df80'],
    ['surveyLens', 'Survey Lens', '◉', '#79e1d2'],
    ['diamondHead', 'Diamond Drill Head', '♢', '#a5eaef'],
    ['superAlloy', 'Superalloy Billet', '▰', '#d1b2ee'],
    ['phaseLens', 'Phase Lens', '◉', '#6fffd4'],
    ['quantumController', 'Quantum Controller', '▣', '#aef4f7'],
    ['railSlug', 'Siege Slug', '▹', '#cb99ed'],
    ['sunspikeCell', 'Sunspike Charge', '◆', '#ffab71'],
    ['flightCell', 'Flight Cell', '▬', '#a2f9dd']
  ];

  const oresToIngots = [
    { key: 'cobaltIngot', cost: { cobaltOre: 3 }, out: { cobaltIngot: 1 } },
    { key: 'iridiumIngot', cost: { iridiumOre: 2, crystal: 2 }, out: { iridiumIngot: 1 } },
    { key: 'aetheriumIngot', cost: { aetheriumOre: 2, crystal: 2 }, out: { aetheriumIngot: 1 } }
  ];

  const foundryRecipes = [
    { key: 'precisionBearing', cost: { steel: 2, cobaltIngot: 1, gear: 1 }, out: { precisionBearing: 2 } },
    { key: 'roboticFrame', cost: { steel: 3, cobaltIngot: 1 }, out: { roboticFrame: 2 } },
    { key: 'servoCore', cost: { cobaltIngot: 1, circuit: 2, precisionBearing: 1 }, out: { servoCore: 2 } },
    { key: 'fluxCell', cost: { cobaltIngot: 1, copperIngot: 2, crystal: 2 }, out: { fluxCell: 6 } },
    { key: 'surveyLens', cost: { crystal: 3, cobaltIngot: 1, circuit: 1 }, out: { surveyLens: 2 } },
    { key: 'diamondHead', cost: { cobaltIngot: 2, iridiumIngot: 1, obsidian: 4 }, out: { diamondHead: 1 } },
    { key: 'superAlloy', cost: { cobaltIngot: 1, iridiumIngot: 1, steel: 3 }, out: { superAlloy: 2 } },
    { key: 'phaseLens', cost: { aetheriumIngot: 1, crystal: 4, advancedComponent: 1 }, out: { phaseLens: 1 } },
    { key: 'quantumController', cost: { iridiumIngot: 1, advancedComponent: 2, circuit: 2 }, out: { quantumController: 1 } },
    { key: 'railSlug', cost: { iridiumIngot: 1, steel: 1, fluxCell: 1 }, out: { railSlug: 4 } },
    { key: 'sunspikeCell', cost: { aetheriumIngot: 1, fluxCell: 2 }, out: { sunspikeCell: 2 } },
    { key: 'flightCell', cost: { fluxCell: 2, cobaltIngot: 1 }, out: { flightCell: 4 } }
  ];

  const benchParts = [
    ['droneChassis', 'Drone Chassis', '◇', { roboticFrame: 1, precisionBearing: 2, servoCore: 1, circuit: 2 }, 'Common airframe for all three drones. Add role hardware at the Workbench.', 4, 'robotics'],
    ['collectionDrone', 'Collection Drone', '◈', { droneChassis: 1, precisionBearing: 1, fluxCell: 2 }, 'Commission at a Reclaimer Dock. The drone retrieves loose supplies and unloads at a Salvage Collection Bin.', 4, 'robotics'],
    ['miningDrone', 'Mining Drone', '◈', { droneChassis: 1, surveyLens: 1, servoCore: 1, steel: 4 }, 'Commission at a Mining Drone Dock. The drone cuts ordinary ore and returns it to an Ore Collection Bin.', 4, 'robotics'],
    ['defenseDrone', 'Defense Drone', '◈', { droneChassis: 1, servoCore: 2, fluxCell: 2, steel: 6 }, 'Commission at a Defense Drone Dock. Keep the dock supplied with turret rounds.', 4, 'robotics'],
    ['flightController', 'Flight Control Module', '▣', { quantumController: 1, phaseLens: 1, circuit: 4 }, 'Controls the Starship flight surfaces and docking approach.', 5, 'starflight'],
    ['plasmaThruster', 'Plasma Thruster', '⬘', { superAlloy: 2, servoCore: 2, aetheriumIngot: 1, obsidian: 6 }, 'Turns stored flight charge into airborne thrust.', 5, 'starflight'],
    ['fluxTank', 'Flux Tank', '▤', { superAlloy: 2, cobaltIngot: 2, fluxCell: 4, steel: 5 }, 'Holds and stabilizes fuel for a Starship.', 5, 'starflight'],
    ['starshipHull', 'Starship Hull', '◇', { superAlloy: 3, roboticFrame: 2, iridiumIngot: 2, aetheriumIngot: 2 }, 'The pressure hull and landing frame for a Starship.', 5, 'starflight'],
    ['starship', 'Starship', '◭', { starshipHull: 1, flightController: 1, plasmaThruster: 2, fluxTank: 1, precisionBearing: 3 }, 'Take to a powered Starship Pad and load Flight Cells before launch.', 5, 'starflight']
  ];

  const port = (side, type, mode, name) => ({ side, type, mode, name });
  const carry = (type, name) => [0, 1, 2, 3].map(side => port(side, type, 'both', name));
  const powerIn = side => port(side, 'power', 'in', 'PWR');
  const enable = port(3, 'signal', 'in', 'EN');
  const itemIn = port(2, 'item', 'in', 'IN');
  const itemOut = port(0, 'item', 'out', 'OUT');
  const driveIn = port(2, 'mechanical', 'in', 'DRIVE');
  const driveOut = port(0, 'mechanical', 'out', 'DRIVE');

  const conductor = capacity => ({ electric: true, capacity, ports: carry('power', 'BUS') });
  const store = (inventoryCapacity, extras = {}) => ({
    kind: 'item', transport: true, inventoryCapacity, retainItems: true,
    ports: [itemIn, itemOut, port(3, 'item', 'in', 'DROP')], ...extras
  });
  const droneDevice = (droneRole, load, ports, extras = {}) => ({
    kind: 'power', electric: true, load, capacity: 65, droneRole, ports, ...extras
  });
  const miner = (load, torque, time, depth, width, miningTier) => ({
    kind: 'item', load, capacity: 65, electric: true, mechanical: true, producer: true, transport: true,
    inventoryCapacity: 32, outputCapacity: 24,
    machine: { load, torque, time, depth, width, miningTier, mining: true },
    ports: [powerIn(1), powerIn(3), driveIn, port(1, 'item', 'out', 'ORE'), enable]
  });
  const plant = (load, time, recipes, torque = 0) => ({
    kind: 'item', load, capacity: 65, electric: true, mechanical: !!torque, producer: true, transport: true,
    inventoryCapacity: 48, outputCapacity: 32, machine: { load, torque, time, recipes },
    ports: [powerIn(1), itemIn, itemOut, ...(torque ? [port(3, 'mechanical', 'in', 'DRIVE')] : []), enable]
  });

  const machines = [
    ['ION_BORE', 120, 'ionBore', 'Ion Bore', '▸', '#80d5f2', { steel: 12, cobaltIngot: 4, servoCore: 2, surveyLens: 1, precisionBearing: 2 },
      'Rear drive and side power cut a narrow fourteen-tile bore, including iridium. Feed ore into a receiver below.', 4, 'robotics', miner(10, 15, .58, 14, 1, 3)],
    ['PLASMA_EXCAVATOR', 121, 'plasmaExcavator', 'Plasma Excavator', '▸', '#f69770', { steel: 16, iridiumIngot: 4, superAlloy: 4, diamondHead: 2, servoCore: 3, quantumController: 1 },
      'A 25-torque plasma drive cuts five lanes ahead and extracts aetherium. Supply 18 power and attach an ore receiver below.', 5, 'aetherEngineering', miner(18, 25, .42, 20, 5, 4)],
    ['RECLAIMER_DOCK', 122, 'reclaimerDock', 'Reclaimer Dock', '⊔', '#8fe3bf', { steel: 7, roboticFrame: 2, servoCore: 1, fluxCell: 3 },
      'Powers and recalls up to three Collection Drones. Place a Salvage Collection Bin nearby for returned supplies.', 4, 'robotics',
      droneDevice('dock', 8, [powerIn(1), powerIn(2), enable], { droneSpecialty: 'collector', droneSlots: 3 })],
    ['DRONE_BEACON', 123, 'droneBeacon', 'Drone Beacon', '◉', '#9ae2ed', { steel: 3, copperIngot: 3, surveyLens: 1, circuit: 2 },
      'A powered waypoint guides drones to a work area and narrows their search.', 4, 'robotics', droneDevice('beacon', 2, [powerIn(1), powerIn(2), enable], { solid: false })],
    ['DRONE_CHARGER', 124, 'droneCharger', 'Drone Charger', '▣', '#b6e789', { steel: 6, cobaltIngot: 2, fluxCell: 3, precisionBearing: 1 },
      'Feeds six power into a charging pad. Drones top off their energy during active routes.', 4, 'robotics', droneDevice('charger', 6, [powerIn(1), powerIn(2), enable])],
    ['DRONE_DEPOT', 125, 'droneDepot', 'Drone Cargo Depot', '▤', '#9fc9af', { steel: 8, copperIngot: 3, roboticFrame: 1 },
      'A 160-item common destination for mining and collection drones. Routes delivered materials through its output.', 4, 'robotics',
      store(160, { droneRole: 'depot' })],
    ['ROBOTICS_FOUNDRY', 126, 'roboticsFoundry', 'Robotics Foundry', '▣', '#db9cef', { steel: 10, cobaltIngot: 5, circuit: 6, advancedComponent: 4 },
      'Turns refined rare metals into bearings, servo cores, drone frames and flight or weapon cells. Connect 14 power.', 4, 'robotics', plant(14, 3.6, foundryRecipes)],
    ['INDUCTION_SMELTER', 127, 'inductionSmelter', 'Induction Smelter', '▥', '#f6ad71', { steel: 12, copperIngot: 8, circuit: 6, advancedComponent: 4, obsidian: 6 },
      'Feeds cobalt, iridium or aetherium ore through a powered crucible. Delivers rare ingots to the front output.', 4, 'metallurgy', plant(11, 3.2, oresToIngots)],
    ['SERVO_MOTOR', 128, 'servoMotor', 'High-Torque Servo Motor', '◎', '#80e8dd', { steel: 8, cobaltIngot: 3, servoCore: 2, precisionBearing: 2 },
      'Turns 10 power into 32 torque. Orient its drive output into a heavy drill or shaft.', 4, 'robotics',
      { kind: 'mechanical', servoMotor: true, driveTorque: 32, driveSpeed: 1.1, electric: true, mechanical: true, capacity: 65, load: 10, ports: [powerIn(2), enable, driveOut] }],
    ['REACTOR_GENERATOR', 129, 'reactorGenerator', 'Compact Reactor Generator', 'ϟ', '#ffa282', { steel: 16, cobaltIngot: 8, iridiumIngot: 2, fluxCell: 4, advancedComponent: 4 },
      'Supplies 42 power through industrial-capacity conductors. Use a dedicated grid or an appropriately rated bus.', 5, 'aetherEngineering',
      { kind: 'power', electric: true, source: 42, capacity: 85, ports: [0, 1, 2, 3].map(side => port(side, 'power', 'out', 'PWR')) }],
    ['MINING_DRONE_DOCK', 150, 'miningDroneDock', 'Mining Drone Dock', '⊔', '#adc8e9', { steel: 10, roboticFrame: 2, servoCore: 2, surveyLens: 2 },
      'Powers and recalls up to two Mining Drones. Place an Ore Collection Bin before mining begins.', 4, 'robotics',
      droneDevice('dock', 10, [powerIn(1), powerIn(2), enable], { droneSpecialty: 'miner', droneSlots: 2 })],
    ['DEFENSE_DRONE_DOCK', 151, 'defenseDroneDock', 'Defense Drone Dock', '⊔', '#e5a38b', { steel: 12, roboticFrame: 2, servoCore: 3, fluxCell: 4 },
      'Deploys up to two Defense Drones. Load turret rounds or pipe them in from a Defense Supply Bin.', 4, 'robotics',
      droneDevice('dock', 12, [powerIn(1), itemIn, enable], { droneSpecialty: 'defense', droneSlots: 2, transport: true, ammo: 'ammo', inventoryCapacity: 96, retainItems: true })],
    ['ORE_COLLECTION_BIN', 152, 'oreCollectionBin', 'Ore Collection Bin', '▤', '#86b9e3', { steel: 6, ironIngot: 4, roboticFrame: 1 },
      'Receives and buffers 128 mined items from Mining Drones. The front port feeds factory logistics.', 4, 'robotics',
      store(128, { droneRole: 'depot', droneCargo: 'ore' })],
    ['SALVAGE_COLLECTION_BIN', 153, 'salvageCollectionBin', 'Salvage Collection Bin', '▤', '#9fe0bd', { steel: 6, copperIngot: 4, roboticFrame: 1 },
      'Receives and buffers 128 recovered items from Collection Drones. The front port feeds factory logistics.', 4, 'robotics',
      store(128, { droneRole: 'depot', droneCargo: 'salvage' })],
    ['DEFENSE_SUPPLY_BIN', 154, 'defenseSupplyBin', 'Defense Supply Bin', '▤', '#de9a73', { steel: 5, ironIngot: 4, roboticFrame: 1 },
      'Holds up to 96 turret rounds. Face the output at the ammunition input on a Defense Drone Dock or turret.', 4, 'robotics',
      store(96, { droneRole: 'depot', droneCargo: 'ammo', ammo: 'ammo' })],
    ['REACTOR_BATTERY', 155, 'reactorBattery', 'Reactor Battery', '▣', '#afdf8c', { steel: 10, cobaltIngot: 4, fluxCell: 5, advancedComponent: 2 },
      'Buffers 480 energy, charging at up to 12 and discharging at up to 22 power per second.', 5, 'aetherEngineering',
      { kind: 'power', battery: true, storage: 480, discharge: 22, charge: 12, ...conductor(85) }],
    ['SERVO_SHAFT', 156, 'servoShaft', 'Servo Shaft', '━', '#b7e7dc', { steel: 2, precisionBearing: 1 },
      'A reinforced shaft that carries rotation between motors, machines, or gearing.', 4, 'robotics',
      { kind: 'mechanical', mechanical: true, solid: false, weight: 2, strength: 16, ports: [driveIn, driveOut] }],
    ['STARSHIP_PAD', 157, 'starshipPad', 'Starship Pad', '▥', '#99f3df', { steel: 10, cobaltIngot: 3, roboticFrame: 2, fluxCell: 4 },
      'Connect 12 power from either side or below and clear two tiles wide above the pad. Ordinary copper wire carries only 10.', 5, 'starflight',
      { kind: 'power', platform: true, electric: true, load: 12, capacity: 65, ports: [powerIn(0), powerIn(1), powerIn(2), enable] }],
    ['CARGO_SILO', 158, 'cargoSilo', 'Cargo Silo', '▤', '#b1b3c9', { steel: 10, ironIngot: 8, roboticFrame: 2 },
      'Stores 240 items and sends them through a compatible front connection. Heavy when used in a moving assembly.', 4, 'robotics', store(240, { weight: 9 })],
    ['ALLOY_RACK', 159, 'alloyRack', 'Alloy Rack', '▤', '#e4c08e', { steel: 4, cobaltIngot: 1, precisionBearing: 1 },
      'A 96-item buffer for small factories. Holds cargo until a front receiver has room.', 4, 'robotics', store(96, { weight: 3 })]
  ];

  const structures = [
    ['COBALT_PLATE', 130, 'cobaltPlate', 'Cobalt Plate', '▥', '#739fd4', { cobaltIngot: 1, steel: 2 }, 4,
      'Stiff, lightweight structural plating for factory walls and moving machines.', { weight: 2, strength: 24, hp: 260 }],
    ['IRIDIUM_SHELL', 131, 'iridiumShell', 'Iridium Shell', '▧', '#c291df', { iridiumIngot: 1, steel: 3 }, 3,
      'High-durability outer wall for exposed installations.', { weight: 5, strength: 50, hp: 620, hard: 2.3 }],
    ['AETHERIUM_PANEL', 132, 'aetheriumPanel', 'Aetherium Panel', '▥', '#71e2c4', { aetheriumIngot: 1, superAlloy: 1 }, 3,
      'Very light, rigid hull plate for moving bridges and linked machinery.', { weight: 1, strength: 54, hp: 460, hard: 2 }],
    ['TEMPERED_GLASS', 133, 'temperedGlass', 'Tempered Glass', '▢', '#90c7d2', { crystal: 2, copperIngot: 1 }, 4,
      'Solid viewing panel for enclosing working machinery.', { weight: 1, strength: 7, hp: 120, hard: .5 }],
    ['REFRACTORY_BRICK', 134, 'refractoryBrick', 'Refractory Brick', '▩', '#ba7864', { obsidian: 3, stone: 4 }, 6,
      'Dense furnace brick with high durability for walls and reactor rooms.', { weight: 6, strength: 36, hp: 340, hard: 1.7 }],
    ['HONEYCOMB_FRAME', 135, 'honeycombFrame', 'Honeycomb Frame', '⬡', '#b9ad84', { steel: 2, cobaltIngot: 1 }, 4,
      'A light frame with a high strength-to-weight ratio for linked gantries.', { weight: 1, strength: 28 }],
    ['TRUSS_BEAM', 136, 'trussBeam', 'Truss Beam', '╳', '#a9b9ab', { steel: 3, ironIngot: 2 }, 4,
      'Reinforced beam for carrying mechanical loads between larger assemblies.', { weight: 3, strength: 37 }],
    ['ELECTRIC_BUS', 137, 'electricBus', 'Industrial Power Bus', '━', '#e6ad66', { cobaltIngot: 1, copperIngot: 3 }, 4,
      'Carries up to 45 power in any direction. A weaker connection still sets the overall grid limit.',
      { kind: 'power', solid: false, weight: 0, ...conductor(45) }],
    ['SUPER_CABLE', 138, 'superCable', 'Superconducting Cable', '━', '#78fbe0', { iridiumIngot: 1, superAlloy: 1, copperIngot: 3 }, 6,
      'Carries up to 85 power, enough for multiple heavy loads on a compatible grid.', { kind: 'power', solid: false, weight: 0, ...conductor(85) }],
    ['FIBER_WIRE', 139, 'fiberWire', 'Fiber Signal Line', '┄', '#d693ea', { copperIngot: 1, crystal: 2, steel: 1 }, 8,
      'A visible bidirectional signal conductor for drone and machine controls.',
      { kind: 'signal', solid: false, weight: 0, signal: true, signalLine: true, ports: carry('signal', 'SIGNAL') }],
    ['ARMORED_PLATFORM', 140, 'armoredPlatform', 'Armored Platform', '═', '#a0bac0', { steel: 3, cobaltIngot: 1 }, 4,
      'A tough walkable surface you can jump through and drop through.', { platform: true, weight: 2, strength: 26, hp: 240 }],
    ['GANTRY_DECK', 141, 'gantryDeck', 'Gantry Deck', '═', '#b5c38e', { steel: 2, ironIngot: 2 }, 4,
      'Light open deck for elevated walkways and moving work platforms.', { platform: true, weight: 1, strength: 20 }],
    ['SERVICE_LADDER', 142, 'serviceLadder', 'Service Ladder', '╫', '#a9caaa', { steel: 2, cobaltIngot: 1 }, 4,
      'Climb a durable maintenance path around powered equipment.', { ladder: true, solid: false, weight: 1, strength: 16 }],
    ['MAGNETIC_RAIL', 143, 'magneticRail', 'Magnetic Guide Rail', '║', '#91c1d7', { steel: 2, cobaltIngot: 1, precisionBearing: 1 }, 4,
      'Lines a lift shaft with a firm guide for linked platforms.', { rail: true, solid: false, weight: 2, strength: 20 }],
    ['BULKHEAD', 144, 'bulkhead', 'Armored Bulkhead', '▦', '#b29ea2', { steel: 5, cobaltIngot: 1, obsidian: 2 }, 2,
      'Takes heavy raid damage and provides a strong fixed wall for machinery.', { weight: 7, strength: 48, hp: 740, hard: 2.6 }],
    ['FLIGHT_DECK', 145, 'flightDeck', 'Flight Deck', '═', '#8fcbc0', { steel: 3, cobaltIngot: 1, crystal: 1 }, 4,
      'A broad jump-through deck for hangars and approach walkways.', { platform: true, weight: 2, strength: 35, hp: 220 }],
    ['ANCHOR_PYLON', 146, 'anchorPylon', 'Anchor Pylon', '╥', '#aab1ad', { steel: 4, precisionBearing: 1 }, 2,
      'A high-strength rope anchor and mount for a permanent gantry.', { anchor: true, weight: 5, strength: 60, hp: 360 }],
    ['BALLAST_CORE', 147, 'ballastCore', 'Ballast Core', '■', '#9e8f9e', { steel: 4, obsidian: 4, ironIngot: 3 }, 1,
      'An 18-mass counterweight. Link it separately so a winch can offset a heavier lift deck.', { counterweight: true, weight: 18, strength: 45 }],
    ['HANGAR_GRATE', 148, 'hangarGrate', 'Hangar Grate', '▧', '#889f9f', { steel: 2, ironIngot: 1 }, 6,
      'An open floor for hangar catwalks. Jump through from below and drop through from above.', { platform: true, weight: 1, strength: 18 }],
    ['ARMORED_SCAFFOLD', 149, 'armoredScaffold', 'Armored Scaffold', '╳', '#a4b489', { steel: 4, cobaltIngot: 1 }, 4,
      'A strong jump-through scaffold. Connect multiple sections into a work deck.', { platform: true, weight: 3, strength: 38, hp: 320 }]
  ];

  function install(T, defs, items, recipes) {
    function tile(symbol, id, key, name, icon, color, flags) {
      T[symbol] = id;
      defs[id] = { kind: 'structure', name, drop: key, symbol, icon, solid: true, hard: 1.1, weight: 4, strength: 14, ...flags };
      items.push({ key, tile: id, name, icon, color, rarity: 'rare' });
    }
    function benchRecipe(key, name, icon, out, cost, desc, chapter, schematic, category) {
      recipes.push({ key, name, icon, out, cost, desc, chapter, schematic, requiresSchematic: true, category });
    }
    for (const [symbol, id, key, name, color, hard, miningTier] of ores) {
      tile(symbol, id, key, name, '◆', color, { kind: 'ore', rareOre: true, hard, miningTier, weight: 6, strength: 12 });
      // The resource is recovered as ore for smelting, so it cannot be placed as fresh terrain.
      Object.assign(items[items.length - 1], { tile: -2, resource: true });
    }
    for (const [key, name, icon, color] of goods) items.push({ key, tile: -2, name, icon, color, resource: true, rarity: 'rare' });
    for (const [key, name, icon, cost, desc, chapter, schematic] of benchParts) {
      items.push({ key, tile: -2, name, icon, color: '#a0e5db', rarity: chapter === 5 ? 'relic' : 'rare' });
      benchRecipe(key, name, icon, 1, cost, desc, chapter, schematic, schematic === 'starflight' ? 'flight' : 'robotics');
    }
    for (const [symbol, id, key, name, icon, color, cost, desc, chapter, schematic, flags] of machines) {
      tile(symbol, id, key, name, icon, color, flags);
      benchRecipe(key, name, icon, 1, cost, desc, chapter, schematic, schematic === 'starflight' ? 'flight' : 'robotics');
    }
    for (const [symbol, id, key, name, icon, color, cost, out, desc, flags] of structures) {
      const exotic = ['iridiumShell', 'aetheriumPanel', 'superCable', 'flightDeck'].includes(key);
      tile(symbol, id, key, name, icon, color, flags);
      benchRecipe(key, name, icon, out, cost, desc, exotic ? 5 : 4, exotic ? 'aetherEngineering' : 'metallurgy', 'structure');
    }
  }

  return { install };
});
