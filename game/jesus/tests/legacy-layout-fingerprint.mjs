import {createHash} from 'node:crypto';

// Only stable gameplay geometry and identities are included. Rendering metadata
// and the implementation's validation report may evolve without moving a save.
export function fingerprintLegacy(world, stream) {
  const chunks=[];
  const fields=['id','x','y','w','h','type','kind','oneWay','baseX','baseY','axis','range','phase','speed','motion','period','switchId','requires','groundId','patrolLeft','patrolRight'];
  for(let index=0;index<world.chunkCount;index++) {
    stream(world,index*world.chunkWidth+100);
    const chunk=world.chunks.find(c=>c.index===index);
    chunks.push({index,moduleId:chunk.moduleId,seed:chunk.seed,rest:chunk.rest,timeOffset:chunk.timeOffset,
      entities:Object.fromEntries(['platforms','movers','hazards','enemies','pickups','switches'].map(field=>[field,chunk[field].map(item=>Object.fromEntries(fields.filter(k=>item[k]!==undefined).map(k=>[k,item[k]])))]))});
  }
  return createHash('sha256').update(JSON.stringify({width:world.width,chunks,checkpoints:world.checkpoints.map(c=>({id:c.id,x:c.x,y:c.y,groundId:c.groundId}))})).digest('hex');
}
