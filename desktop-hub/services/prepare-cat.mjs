import { writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { pathToFileURL } from 'node:url';

// Generate operator entry points beside the staged game and use its ranked mode directly.
export async function prepareCat(edition) {
  await Promise.all([
    writeFile(join(edition, 'simulation-ranked.mjs'), `export * from './simulation.mjs';
import { createSimulation as run } from './simulation.mjs';
export const createSimulation = options => run({ ...options, startingWave: 1, ranked: true });
`),
    writeFile(join(edition, 'server', 'ranked-match.mjs'), `import { createMatchServer as host } from './match-server.mjs';
export function createMatchServer(options, runtime) {
  if (typeof options.challengeSeed !== 'string' || !options.challengeSeed) throw new TypeError('A ranked seed is required.');
  return host(options, { ...runtime, rankedSeed: options.challengeSeed });
}
`),
  ]);
}

if (process.argv[1] && pathToFileURL(process.argv[1]).href === import.meta.url) await prepareCat(process.argv[2]);
