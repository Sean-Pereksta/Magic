import { resolve } from 'node:path';
import { pathToFileURL } from 'node:url';

export async function start({ packRoot, name, signal, onFailure }) {
  signal?.throwIfAborted();
  const hostModule = resolve(packRoot, 'moba-mania/server/host.mjs');
  const { startMobaHost } = await import(pathToFileURL(hostModule).href);
  return startMobaHost({ packRoot, name, signal, onFailure });
}
