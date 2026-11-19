'use strict';
const project='bible-game-246c0';
const apiKey='AIzaSyB7twY7z31ucB6pGA8JC_HrVMZhA8lNaJA';
function parseArena(document,now){
  const f=document?.fields||{};
  const origin=f.origin?.stringValue, code=f.publicRoom?.stringValue, owner=f.ownerUid?.stringValue;
  const count=Number(f.humans?.integerValue),updated=Date.parse(f.lastSignalListedAt?.timestampValue);
  if(f.gameType?.stringValue!=='lastsignal-arena-v1'||!/^[A-Za-z0-9_-]{1,100}$/.test(owner||'')||
    !document.name?.endsWith('/lobbies/lastsignal-'+owner)||!/^https:\/\/[a-z0-9-]+\.trycloudflare\.com$/.test(origin||'')||
    !/^[A-Za-z0-9_-]{24}$/.test(code||'')||!Number.isInteger(count)||count<1||count>=8||
    !Number.isFinite(updated)||updated<=now-90000||updated>now+30000)return null;
  const name=(f.hostName?.stringValue||'Host').replace(/[\u0000-\u001f\u007f]/g,'').slice(0,20);
  const arena=(f.arena?.stringValue||'Arena').replace(/[\u0000-\u001f\u007f]/g,'').slice(0,64);
  return {name,arena,count,invite:origin+'/#'+code,expires:updated+90000};
}
function createArenaReader(fetchImpl=fetch,now=Date.now){
  let token,refresh,uid,validUntil=0,readAt=0,rooms=[],pending;
  async function request(url,body,headers={}){
    const response=await fetchImpl(url,{method:'POST',headers,body,redirect:'error',signal:AbortSignal.timeout(6500)});
    if(!response.ok||Number(response.headers.get('content-length'))>524288)throw Error('The arena directory is unavailable.');
    const reader=response.body.getReader(),decoder=new TextDecoder();let bytes=0,text='';
    try{for(;;){const part=await reader.read();if(part.done)break;bytes+=part.value.byteLength;if(bytes>524288)throw Error('Arena list is too large.');text+=decoder.decode(part.value,{stream:true});}}
    finally{await reader.cancel().catch(()=>{});}
    return JSON.parse(text+decoder.decode());
  }
  async function load(){
    if(!token||now()>validUntil){
      if(refresh){
        const data=await request('https://securetoken.googleapis.com/v1/token?key='+apiKey,new URLSearchParams({grant_type:'refresh_token',refresh_token:refresh}));
        if(data.user_id!==uid)throw Error('The arena directory identity changed.');
        token=data.id_token;refresh=data.refresh_token;
      }else{
        const data=await request('https://identitytoolkit.googleapis.com/v1/accounts:signUp?key='+apiKey,JSON.stringify({returnSecureToken:true}),{'Content-Type':'application/json'});
        uid=data.localId;token=data.idToken;refresh=data.refreshToken;
      }
      if(!token||!refresh)throw Error('Could not sign into the public arena directory.');
      validUntil=now()+50*60000;
    }
    const field={fieldPath:'lastSignalListedAt'};
    const query={structuredQuery:{from:[{collectionId:'lobbies'}],where:{fieldFilter:{field,op:'GREATER_THAN',value:{timestampValue:new Date(now()-90000).toISOString()}}},orderBy:[{field,direction:'DESCENDING'}],limit:50}};
    const raw=await request('https://firestore.googleapis.com/v1/projects/'+project+'/databases/(default)/documents:runQuery',JSON.stringify(query),{Authorization:'Bearer '+token,'Content-Type':'application/json'});
    if(!Array.isArray(raw))throw Error('Invalid public arena list.');
    rooms=raw.slice(0,50).map(r=>parseArena(r.document,now())).filter(Boolean);readAt=now();
    return rooms;
  }
  return function list(){
    if(readAt && now()-readAt<15000)return Promise.resolve(rooms.filter(r=>r.expires>now()));
    if(!pending)pending=load().finally(()=>{pending=null;});
    return pending;
  };
}
module.exports={createArenaReader,parseArena};
