import { DIFFICULTIES, MAPS } from '../simulation.mjs';

const includes = (catalog, id) => Array.isArray(catalog)
  ? catalog.some(entry => (typeof entry === 'string' ? entry : entry.id) === id)
  : Object.hasOwn(catalog, id);

function label(value, max) {
  return typeof value === 'string' ? value.replace(/[\u0000-\u001f\u007f]/g, '').trim().slice(0, max) : '';
}

export function readLobbySettings(options) {
  const roomName = label(options?.roomName, 48);
  const hostName = label(options?.hostName, 28);
  const { map, difficulty, capacity } = options ?? {};
  const visibility = options?.visibility === 'private' ? 'invite' : options?.visibility;
  if (!roomName || !hostName || !includes(MAPS, map) || !includes(DIFFICULTIES, difficulty)
    || !['public', 'invite'].includes(visibility) || !Number.isInteger(capacity) || capacity < 1 || capacity > 4) {
    throw new Error('Enter a room and display name, valid map and difficulty, and one to four seats.');
  }
  return { roomName, hostName, map, difficulty, visibility, capacity };
}

export const displayName = value => label(value, 28) || 'Guest';
