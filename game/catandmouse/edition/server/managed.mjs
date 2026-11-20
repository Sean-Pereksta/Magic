import { resolve } from 'node:path';
import { pathToFileURL } from 'node:url';

export async function start({ packRoot, name }) {
  const nativeServer = resolve(packRoot, 'game/catandmouse/edition/server/match-server.mjs');
  const { createMatchServer } = await import(pathToFileURL(nativeServer).href);
  const match = await createMatchServer({
    roomName: name || 'Cat & Mouse', hostName: 'Host', capacity: 4,
    difficulty: 'normal', map: 'flour-mill', visibility: 'public'
  }, { managed: true });
  return {
    origin: match.url,
    getStatus() {
      const lobby = match.getLobby();
      const phase = lobby.phase === 'waiting' || lobby.phase === 'finished' ? lobby.phase : 'playing';
      return { phase, players: lobby.players.length, capacity: lobby.capacity, queued: 0 };
    },
    joinPath: () => '/#room=' + match.id + '&managed=1',
    close: () => match.close()
  };
}
