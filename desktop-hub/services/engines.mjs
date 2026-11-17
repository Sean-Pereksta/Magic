import {join} from 'node:path';
import {pathToFileURL} from 'node:url';
import {survivalWinner} from './ledger.mjs';

const moduleAt=(root,path)=>import(pathToFileURL(join(root,path)).href);
async function post(origin,path,body,token) {
  const headers={'Content-Type':'application/json'};
  if(token)headers.Authorization='Bearer '+token;
  const response=await fetch(new URL(path,origin),{method:'POST',headers,body:JSON.stringify(body),signal:AbortSignal.timeout(6000)});
  if(!response.ok)throw new Error('Official match engine did not reserve a seat.');
  return response.json();
}
export function createEngineFactory({bannerlands,catEdition}) {
  return async ({game,players,id})=>{
    if(game==='bannerlands'){
      const [{createMatchServer},versions]=await Promise.all([moduleAt(bannerlands,'source/multiplayer-server.mjs'),moduleAt(bannerlands,'multiplayer/protocol.js')]);
      const match=await createMatchServer({config:{enemies:1,mode:'conquest',houses:[0,1],scale:'standard',difficulty:'standard'},lobby:{maxHumans:2,public:false,hostName:players[0].username,name:'Catnmice ranked 1v1'}});
      try{
        const host=await post(match.url,'/api/host/session',{},match.hostToken);
        const guest=await post(match.url,'/api/join',{v:versions.PROTOCOL_VERSION,gameVersion:versions.GAME_VERSION,assetVersion:versions.ASSET_VERSION,invite:match.invite,name:players[1].username});
        const seats=[host,guest].map((p,i)=>({url:match.url,session:{token:p.token,team:p.team,sessionId:p.sessionId,host:i===0}}));
        return {seats,close:()=>match.close(),started:()=>match.getLobby().phase==='in-progress',outcome:()=>{
          if(match.getLobby().phase!=='in-progress'||match.game.winner===null)return null;
          const winner=match.game.winner;
          if(winner!==0&&winner!==1)return null;
          return {winner};
        }};
      }catch(error){await match.close();throw error;}
    }
    const {createMatchServer}=await moduleAt(catEdition,'server/ranked-match.mjs');
    const rooms=[];
    try {
      for(const player of players)rooms.push(await createMatchServer({hostName:player.username,roomName:'Ranked Survival Challenge',map:'flour-mill',difficulty:'hard',capacity:1,visibility:'invite',challengeSeed:id}));
      return {seats:rooms.map(room=>({url:room.url,room:room.id,token:room.seatToken})),
        started:()=>rooms.every(room=>['running','finished'].includes(room.getLobby().phase)),
        outcome:()=>{const scores=rooms.map(r=>r.score);return scores.every(Boolean)?{scores,winner:survivalWinner(scores)}:null;},
        close:()=>Promise.all(rooms.map(room=>room.close()))};
    }catch(error){await Promise.allSettled(rooms.map(room=>room.close()));throw error;}
  };
}
