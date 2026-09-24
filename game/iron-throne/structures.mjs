import { BUILDINGS, UNITS } from './data.mjs';
import { buildingLevel, buildingSpec, fortMaximum, wallMaximum } from './economy.mjs';
import { atWar, distance, kingdom, log, random, sizeOf } from './core.mjs';
import { siegeStep } from './warfare.mjs';

export const structuresAt = t => Object.keys(BUILDINGS).filter(type => buildingLevel(t, type) > 0);
export function structureMaximum(t, type) {
  const level = buildingLevel(t, type);
  if (!level) return 0;
  if (type === 'wall') return wallMaximum(t);
  return (['fort','city','town','watchtower'].includes(type) ? 150 + (level - 1) * 90 : 60 + (level - 1) * 30);
}
export function structureHealth(t, type) {
  return type === 'wall' ? t.walls : Math.max(0, structureMaximum(t, type) - (t.structureDamage?.[type] || 0));
}
export function bombardRange(a) {
  return Math.max(0, ...Object.entries(a.units).filter(([, n]) => n > 0).map(([id]) => UNITS[id]?.bombardRange || 0));
}
// Cube interpolation is deterministic on the axial hex map. Mountains block fire.
export function clearShot(s, from, to) {
  const length = distance(from, to);
  for (let i = 1; i < length; i++) {
    const q = from.q + (to.q - from.q) * i / length, r = from.r + (to.r - from.r) * i / length;
    const y = -q - r; let x = Math.round(q), z = Math.round(r), ry = Math.round(y);
    const dx = Math.abs(x - q), dz = Math.abs(z - r), dy = Math.abs(ry - y);
    if (dx > dy && dx > dz) x = -ry - z;
    else if (dz > dy) z = -x - ry;
    const tile = s.tiles[`${x},${z}`];
    if (!tile || tile.terrain === 'mountain') return false;
  }
  return true;
}
export function structureAttackCheck(s, a, t, type, mode = 'attack') {
  if (s.outcome || !a || !sizeOf(a) || !t || !Object.hasOwn(BUILDINGS, type) || !buildingLevel(t, type)) return 'Select an existing structure and an active army.';
  if (!t.owner || !atWar(s, a.owner, t.owner)) return 'You must be at war with the structure’s owner.';
  if (!['attack','bombard'].includes(mode)) return 'Choose Attack or Bombard.';
  if (mode === 'bombard') {
    const range = bombardRange(a), d = distance(s.tiles[a.tile], t);
    if (!range) return 'Bombardment requires rams, catapults, trebuchets or legacy siege engines.';
    if (d > range) return `Outside bombardment range (${range} hexes).`;
    if (!clearShot(s, s.tiles[a.tile], t)) return 'Mountains block this line of fire.';
    if (type === 'wall' && !t.walls || type === 'fort' && !(t.fortIntegrity ?? fortMaximum(t))) return 'These fortifications are breached. Assault the defenders or choose another structure.';
  }
  return null;
}
export function damageStructure(s, a, t, type, mode = 'attack') {
  const error = structureAttackCheck(s, a, t, type, mode);
  if (error || mode === 'attack' && a.tile !== t.id || a.lastStructureTurn === s.turn) return false;
  const defenders=s.armies.filter(e => e.tile === t.id && sizeOf(e) > 0 && atWar(s, a.owner, e.owner));
  const d = distance(s.tiles[a.tile], t);
  // Bombardment weakens protection even with a garrison present. It neither
  // occupies the tile from range nor destroys the underlying fort building.
  if(mode==='bombard'&&['wall','fort'].includes(type)){
    const before=sizeOf(a),defendersBefore=defenders.reduce((n,e)=>n+sizeOf(e),0);
    const integrityBefore=t.walls+(t.fortIntegrity??fortMaximum(t));
    const garrison=defenders.length?{units:Object.fromEntries(Object.keys(UNITS).map(id=>[id,defenders.reduce((n,e)=>n+(e.units[id]||0),0)])),morale:Math.max(...defenders.map(e=>e.morale))}:null;
    const result=siegeStep(s,a,t,garrison,()=>random(s),{range:d,type});
    if(result.surrendered)for(const e of defenders)for(const id of Object.keys(e.units))e.units[id]=0;
    a.lastStructureTurn=s.turn;
    const integrityAfter=t.walls+(t.fortIntegrity??fortMaximum(t));
    s.militaryEvents.push({id:s.nextId++,turn:s.turn,attacker:a.owner,defender:t.owner,attackerArmyId:a.id,tile:t.id,from:a.tile,action:'siege',structure:type,damage:result.damage,before:[before,integrityBefore],after:[sizeOf(a),integrityAfter],breach:integrityAfter===0,
      troopLosses:[before-sizeOf(a),result.surrendered?defendersBefore:0],phases:[{name:'Bombardment',notes:result.notes,loss:[before-sizeOf(a),result.damage]}]});
    log(s,`${kingdom(s,a.owner).name} bombards ${t.name||t.id}: ${result.damage} fortification damage.`,'battle');
    if(structureAttackCheck(s,a,t,type,mode)){a.structureTarget=null;a.target=null;a.path=[];a.order='hold';}
    return true;
  }
  if (defenders.length) return false;
  const engines = Object.entries(a.units).reduce((n, [id, count]) => n + count * (UNITS[id].breach || 0) *
    (mode === 'attack' || (UNITS[id].bombardRange || 0) >= d && UNITS[id].bombardRange > 0 ? 1 : 0), 0);
  const fortified = ['wall','fort','city','town','watchtower'].includes(type);
  const damage = Math.max(1, Math.floor((engines * (fortified ? 2 : 1.5) + (mode === 'attack' ? sizeOf(a) * .35 : 0)) * a.morale));
  const before = structureHealth(t, type), actual = Math.min(before, damage), owner = t.owner;
  const name = buildingSpec(type, buildingLevel(t, type)).name;
  a.lastStructureTurn = s.turn;
  t.structureDamage ||= {};
  if (type === 'wall') t.walls = Math.max(0, before - damage);
  else t.structureDamage[type] = (t.structureDamage[type] || 0) + actual;
  if (type === 'fort') t.fortIntegrity = Math.min(t.fortIntegrity ?? fortMaximum(t), Math.ceil(fortMaximum(t) * structureHealth(t, type) / structureMaximum(t, type)));
  const destroyed = structureHealth(t, type) === 0;
  if (destroyed) {
    delete t.levels[type]; delete t.structureDamage[type];
    if (t.project?.type === type) t.project = null;
    if (type === 'wall') t.walls = 0;
    else if (type === 'road' || BUILDINGS[type].settlement) t[type] = false;
    else if (type === 'city') { t.building = 'town'; t.levels.town = 1; }
    else { t.building = null; if (type === 'town') t.capital = null; }
    if (type === 'fort') { t.fortIntegrity = 0; t.siege = null; }
    a.structureTarget = null; a.target = null; a.path = []; a.order = 'hold';
  }
  s.militaryEvents.push({ id: s.nextId++, turn: s.turn, attacker: a.owner, defender: owner, attackerArmyId: a.id,
    tile: t.id, from: a.tile, action: 'structure', structure: type, damage: actual, destroyed,
    phases: [{ name: destroyed ? 'Structure destroyed' : 'Structure attack', notes: [`${name}: ${actual} damage; ${destroyed ? 'destroyed' : `${structureHealth(t,type)} durability remains`}.`], loss: [0,actual] }] });
  log(s, `${kingdom(s,a.owner).name} ${destroyed ? 'destroyed' : 'damaged'} ${kingdom(s,owner).name}’s ${name} at ${t.name || t.id}${destroyed && type === 'city' ? '; the settlement is reduced to a town' : ''}.`, 'war');
  return true;
}
export function validateStructures(s) {
  for (const t of Object.values(s.tiles)) if (t.structureDamage !== undefined) {
    if (!t.structureDamage || Array.isArray(t.structureDamage) || typeof t.structureDamage !== 'object') throw new Error('Damaged structure durability.');
    for (const [type,n] of Object.entries(t.structureDamage)) if (!Object.hasOwn(BUILDINGS,type) || !buildingLevel(t,type) || !Number.isInteger(n) || n < 0 || n >= structureMaximum(t,type)) throw new Error('Damaged structure durability.');
  }
  for (const a of s.armies) {
    if (a.structureTarget && (!s.tiles[a.target] || !Object.hasOwn(BUILDINGS,a.structureTarget) || !['attack','bombard'].includes(a.order))) throw new Error('Damaged structure orders.');
    if (!['move','attack','retreat','hold','bombard'].includes(a.order) || a.order === 'bombard' && !a.structureTarget) throw new Error('Damaged army orders.');
    if (a.lastStructureTurn !== undefined && (!Number.isInteger(a.lastStructureTurn) || a.lastStructureTurn < 1 || a.lastStructureTurn > s.turn)) throw new Error('Damaged structure orders.');
  }
}
