import '../maps.js';

const definitions = globalThis.TurncraftMaps.definitions;
export const LOBBY_MAP_CATALOG = Object.freeze(definitions.filter(map => (map.capacity || 3) <= 4).map(map => Object.freeze({id: map.id, name: map.name, capacity: map.capacity || 3, width: map.width || 42, height: map.height || 30, category: map.category || '3-player', legacy: !!map.legacy})));
export const LOBBY_MAPS = Object.freeze(LOBBY_MAP_CATALOG.map(map => map.id));
export function validateBattleSettings(value = {}) {
  const result = {mapId: value.mapId || 'classic', gameMode: value.gameMode || 'annihilation', maxPlayers: value.maxPlayers ?? 3, startingResources: 'standard', fillAi: value.fillAi ?? false, aiDifficulty: value.aiDifficulty || 'medium', aiStyle: value.aiStyle || 'balanced'};
  const map = LOBBY_MAP_CATALOG.find(map => map.id === result.mapId);
  if (!map || !['annihilation', 'conquest', 'relic'].includes(result.gameMode) || ![2, 3, 4].includes(result.maxPlayers) || result.maxPlayers > map.capacity || typeof result.fillAi !== 'boolean' || !['easy', 'medium', 'hard', 'pro'].includes(result.aiDifficulty) || !['cautious', 'balanced', 'aggressive'].includes(result.aiStyle)) throw Object.assign(new Error('Choose a compatible battlefield, two to four human seats, and supported AI settings.'), {code: 'INVALID_ARGUMENT'});
  return result;
}
export function battleParticipantCount(settings, humanCount) {
  const config = validateBattleSettings(settings), map = LOBBY_MAP_CATALOG.find(map => map.id === config.mapId);
  if (!Number.isInteger(humanCount) || humanCount < 2 || humanCount > config.maxPlayers) throw Object.assign(new Error('The human commander count does not fit this room.'), {code: 'INVALID_ARGUMENT'});
  return config.fillAi ? map.capacity : humanCount;
}
export function battleSlots(room) {
  const settings = validateBattleSettings(room.settings), humans = room.turncraftSeats.map(uid => ({name: room.turncraftPlayers[uid].name, factionId: room.turncraftPlayers[uid].factionId, ai: false}));
  const count = battleParticipantCount(settings, humans.length);
  while (humans.length < count) humans.push({name: `AI Commander ${humans.length - room.turncraftSeats.length + 1}`, factionId: ['frontier', 'ascendant', 'brood'][humans.length % 3], ai: true, aiDifficulty: settings.aiDifficulty, aiStyle: settings.aiStyle});
  return humans;
}
