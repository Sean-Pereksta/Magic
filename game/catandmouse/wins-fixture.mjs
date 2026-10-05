// Minimal optimistic Firestore model for transaction conflicts, retries and
// lost acknowledgements. No production Firebase connection or credentials.
export function memoryStorage(){
  const values=new Map();
  return {getItem:key=>values.get(key) ?? null,setItem:(key,value)=>values.set(key,String(value)),removeItem:key=>values.delete(key)};
}
export function firestoreFixture(initial={'users/Sean':{wins:17}}){
  const records=new Map(Object.entries(initial)),versions=new Map(),reads=[],commits=[],listeners=new Set();
  let sequence=0,offline=false,loseAck=false;
  const clone=value=>value===undefined ? undefined : structuredClone(value);
  const snapshot=ref=>({id:ref.id,exists:()=>records.has(ref.path),data:()=>clone(records.get(ref.path)),metadata:{fromCache:false,hasPendingWrites:false}});
  const sdk={db:{},doc:(_db,...parts)=>({path:parts.join('/'),id:parts.at(-1)}),collection:(_db,...parts)=>({path:parts.join('/')}),
    where:(field,op,value)=>({field,op,value}),limit:count=>({count}),query:(collection,...constraints)=>({...collection,constraints}),
    async getDoc(ref){reads.push(ref.path);if(offline)throw Error('unavailable');return snapshot(ref);},
    async getDocs(q){if(offline)throw Error('unavailable');reads.push(q);
      let rows=[...records].filter(([key])=>key.startsWith(q.path+'/'));
      for(const c of q.constraints || [])if(c.field)rows=rows.filter(([,data])=>data[c.field]===c.value);else if(c.count)rows=rows.slice(0,c.count);
      return {docs:rows.map(([path])=>snapshot({path,id:path.split('/').at(-1)}))};},
    onSnapshot(ref,_options,next,error){const item={ref,next,error};listeners.add(item);return()=>listeners.delete(item);},
    async runTransaction(_db,callback){
      if(offline)throw Error('unavailable');
      for(let attempt=0;attempt<8;attempt++){
        const observed=new Map(),writes=[];
        const tx={async get(ref){if(writes.length)throw Error('read after write');observed.set(ref.path,versions.get(ref.path) || 0);
            const snap=snapshot(ref);await Promise.resolve();return snap;},
          update(ref,data){writes.push({type:'update',ref,data});},set(ref,data){writes.push({type:'set',ref,data});}};
        const result=await callback(tx);
        if([...observed].some(([key,version])=>(versions.get(key) || 0)!==version))continue;
        for(const write of writes){
          if(write.type==='update' && !records.has(write.ref.path))throw Error('missing account');
          records.set(write.ref.path,write.type==='update' ? {...records.get(write.ref.path),...clone(write.data)} : clone(write.data));
          versions.set(write.ref.path,++sequence);
        }
        if(writes.length)commits.push(writes);
        if(loseAck && writes.length){loseAck=false;throw Error('acknowledgement lost');}
        return result;
      }
      throw Error('too much contention');
    }};
  return {sdk,records,reads,commits,listeners,offline(value){offline=value;},loseNextAcknowledgement(){loseAck=true;}};
}
