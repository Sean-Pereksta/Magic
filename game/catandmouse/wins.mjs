// The lobby's account key is users/{username}, not the anonymous Auth UID.
// Keep SDK calls injected so account resolution and transaction retries can
// be exercised without reading or writing a production account.
export function readWinCount(value){
  if(value===undefined || value===null)return 0;
  const number=typeof value==='string' && /^\d+$/.test(value) ? Number(value) : value;
  if(!Number.isSafeInteger(number) || number<0)throw Error('The saved win count is invalid.');
  return number;
}

export async function resolveWinAccount({username,savedUsername='',readExact,findByUsername}){
  const name=String(username || '').trim();
  if(!name || name.includes('/'))throw Error('An account username is required to load wins.');
  const norm=name.toLowerCase();
  const names=[name];
  if(savedUsername.trim().toLowerCase()===norm)names.push(savedUsername.trim());
  names.push(norm);
  for(const candidate of new Set(names)){
    const record=await readExact(candidate);
    if(record)return {id:record.id,wins:readWinCount(record.data.wins)};
  }
  // Compatibility with older records whose ID differs from username. A
  // bounded equality query cannot scan unrelated accounts or silently pick
  // the last of several case-insensitive matches.
  const matches=new Map();
  for(const candidate of new Set([name,norm]))
    for(const record of await findByUsername(candidate))
      if(String(record.data.username || '').trim().toLowerCase()===norm)matches.set(record.id,record);
  if(matches.size!==1)throw Error(matches.size ? 'Several accounts match that name; use the exact account username.' : 'No saved account was found for this username.');
  const record=[...matches.values()][0];
  return {id:record.id,wins:readWinCount(record.data.wins)};
}

export function winReceiptId(accountId,matchId){
  if(!accountId || !matchId)throw Error('A saved account and match ID are required to save a win.');
  return 'catmouse_win_'+encodeURIComponent(JSON.stringify([accountId,matchId]));
}

export async function recordWinTransaction({db,doc,runTransaction,accountId,matchId,gameId,uid,legacyAlreadyAwarded=false,now=Date.now}){
  const accountRef=doc(db,'users',accountId);
  // gameStats documents already have authenticated access in the checked-in
  // rules. Receipts survive lobby cleanup without requiring a new rules path.
  const receiptRef=doc(db,'gameStats',winReceiptId(accountId,matchId));
  return runTransaction(db,async transaction=>{
    const account=await transaction.get(accountRef),receipt=await transaction.get(receiptRef);
    if(!account.exists())throw Error('The saved account no longer exists.');
    const wins=readWinCount(account.data().wins);
    if(receipt.exists())return {wins,awarded:false};
    // Earlier versions wrote this local marker only after Firebase confirmed
    // the increment. Migrate it to a receipt without awarding it a second time.
    const next=legacyAlreadyAwarded ? wins : wins+1;
    if(!Number.isSafeInteger(next))throw Error('The saved win count is too large.');
    if(!legacyAlreadyAwarded)transaction.update(accountRef,{wins:next});
    transaction.set(receiptRef,{kind:'catmouse-win',accountId,matchId,gameId,uid,awardedAt:now(),migratedLegacy:legacyAlreadyAwarded});
    return {wins:next,awarded:!legacyAlreadyAwarded};
  });
}

const pendingKey=accountName=>'catmouse-pending-wins:'+accountName;
export function readPendingWins(storage,accountName){
  try{
    const entries=JSON.parse(storage.getItem(pendingKey(accountName)) || '[]');
    if(!Array.isArray(entries))return [];
    const unique=new Map();
    for(const job of entries)if(job?.accountName===accountName && (job.accountId===null || typeof job.accountId==='string') && typeof job.matchId==='string' && job.matchId &&
      typeof job.gameId==='string' && typeof job.uid==='string')unique.set(job.matchId,job);
    return [...unique.values()];
  }catch{return [];}
}
export function savePendingWins(storage,accountName,entries){
  try{
    if(entries.length)storage.setItem(pendingKey(accountName),JSON.stringify(entries));
    else storage.removeItem(pendingKey(accountName));
    return true;
  }catch{return false;}
}
