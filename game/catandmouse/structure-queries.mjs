// Support effects use local cells and topology-keyed caches, never a scan of
// every building for each tower. Health/shield changes do not change topology.
export function createStructureQueries({ size, lookup, revision, now=Date.now, disabledTypes, hash }) {
  let seenRevision=-1;
  const stink=new Map(),shields=new Map();
  const stats={cellReads:0,stinkBuilds:0,shieldBuilds:0};
  function refresh(){
    const next=revision();
    if(next!==seenRevision){seenRevision=next;stink.clear();shields.clear();}
  }
  function nearby(x,y,range,{square=false,type=null}={}){
    const found=[];
    for(let ty=Math.max(0,y-range);ty<=Math.min(size-1,y+range);ty++)
      for(let tx=Math.max(0,x-range);tx<=Math.min(size-1,x+range);tx++){
        if(!square && Math.abs(tx-x)+Math.abs(ty-y)>range)continue;
        stats.cellReads++;
        const item=lookup(tx,ty);
        if(item && (!type || item.type===type))found.push(item);
      }
    return found;
  }
  function stinkTarget(rat,windowMs=5000,time=now()){
    if(!rat || (rat.health ?? 0)<=0 || !Number.isFinite(rat.x) || !Number.isFinite(rat.y))return null;
    refresh();
    const id=rat.docId || rat.id || `${rat.x}_${rat.y}`,window=Math.floor(time/windowMs);
    const signature=`${rat.x}|${rat.y}|${window}`;
    let item=stink.get(id);
    if(!item || item.signature!==signature){
      const candidates=nearby(rat.x,rat.y,2).filter(s=>disabledTypes.includes(s.type))
        .sort((a,b)=>(Math.abs(rat.x-a.x)+Math.abs(rat.y-a.y))-(Math.abs(rat.x-b.x)+Math.abs(rat.y-b.y)) || a.key.localeCompare(b.key));
      const key=candidates.length ? candidates[(window+hash(rat.id))%candidates.length].key : null;
      item={signature,key};stats.stinkBuilds++;
      if(stink.size>=16 && !stink.has(id))stink.delete(stink.keys().next().value);
      stink.set(id,item);
    }
    return item.key;
  }
  function shieldSupports(structure){
    refresh();
    const key=`${structure.x}_${structure.y}`;
    if(!shields.has(key)){
      shields.set(key,nearby(structure.x,structure.y,4,{type:'🛡️'}));stats.shieldBuilds++;
    }
    return shields.get(key);
  }
  return {nearby,stinkTarget,shieldSupports,
    clear(){seenRevision=-1;stink.clear();shields.clear();},
    get stats(){return {...stats,stinkCached:stink.size,shieldsCached:shields.size};}};
}
