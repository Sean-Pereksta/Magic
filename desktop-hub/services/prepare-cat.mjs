import {readFile,writeFile} from 'node:fs/promises';
import {join} from 'node:path';
import {pathToFileURL} from 'node:url';

// Generate adjacent, operator-only engine entry points. The original local and
// co-op games continue to use their original modules and save formats.
function replaceOnce(source,anchor,replacement) {
  const parts=source.split(anchor);
  if(parts.length!==2)throw new Error('Cat & Mouse source changed near '+anchor.slice(0,60));
  return parts.join(replacement);
}
export async function prepareCat(edition) {
  let simulation=await readFile(join(edition,'simulation.mjs'),'utf8');
  const spawn='pendingSpawns.push({ time:';
  if(simulation.split(spawn).length!==6)throw new Error('Review the wave schedule before updating the ranked engine.');
  simulation=simulation.replaceAll(spawn,'pendingSpawns.push({ wave, time:');
  simulation=replaceOnce(simulation,'const { type, lane } = pendingSpawns.shift();','const { type, lane, wave: spawnWave } = pendingSpawns.shift();');
  simulation=replaceOnce(simulation,'const enemy = {','const enemy = { spawnWave,');
  simulation=replaceOnce(simulation,'get status() { return status; },',`get status(){return status;},
    get rankedScore(){
      const alive=new Set([...enemies.values()].map(e=>e.spawnWave));
      for(const entry of pendingSpawns)alive.add(entry.wave);
      let waves=0;
      for(let w=1;w<=wave;w++){if(!alive.has(w))waves++;}
      return {waves,timeMs:Math.floor(resultAt??time)};
    },`);
  const source=join(edition,'server','match-server.mjs');
  let server=await readFile(source,'utf8');
  server=replaceOnce(server,"from '../simulation.mjs'","from '../simulation-ranked.mjs'");
  server=replaceOnce(server,'matchId, mapId: settings.map, difficulty: settings.difficulty,','matchId, seed: options.challengeSeed, mapId: settings.map, difficulty: settings.difficulty,');
  server=replaceOnce(server,'url: origin, id, hostToken, invite, listingToken, getLobby,',`url: origin, id, hostToken, invite, listingToken, getLobby,
    seatToken:host.token,
    get score(){return phase==='finished'?simulation.rankedScore:null;},`);
  await writeFile(join(edition,'simulation-ranked.mjs'),simulation);
  await writeFile(join(edition,'server','ranked-match.mjs'),server);
}
if(process.argv[1]&&pathToFileURL(process.argv[1]).href===import.meta.url)await prepareCat(process.argv[2]);
