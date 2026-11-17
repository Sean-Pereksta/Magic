// Release probe for every published game route. Real matches and account gates
// are exercised separately on signed-in desktop and web clients.
import fs from 'node:fs';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
const here=path.dirname(fileURLToPath(import.meta.url));
const catalog=JSON.parse(fs.readFileSync(path.join(here,'catalog.json'),'utf8'));
const checks=[];
for(const game of catalog.games){
  for(const field of ['route','singleRoute','multiplayerRoute','localEditionRoute','downloadRoute','dmRoute']){
    if(game[field]) checks.push({id:game.id,field,url:new URL(game[field],catalog.site).href});
  }
}
const unique=[...new Set(checks.map(c=>c.url))];
const queue=[...unique], results=new Map;
async function probe(url){
  for(let attempt=0;attempt<3;attempt++){
    try{
      const response=await fetch(url,{method:'HEAD',signal:AbortSignal.timeout(20000)});
      const result={status:response.status,finalUrl:response.url};
      if(response.ok || response.status<500 || attempt===2)return result;
    }catch(error){if(attempt===2)return {error:error.message};}
  }
}
await Promise.all(Array.from({length:5},async()=>{while(queue.length){const url=queue.shift();results.set(url,await probe(url));}}));
const report={source:catalog.source,checkedAt:new Date().toISOString(),entries:catalog.games.length,
  routes:checks.map(row=>({...row,...results.get(row.url)}))};
const failed=report.routes.filter(row=>row.status!==200);
if(process.argv[2])fs.writeFileSync(process.argv[2],JSON.stringify(report,null,2)+'\n');
console.log(report.entries+' game entries, '+checks.length+' route references, '+unique.length+' distinct URLs, '+failed.length+' unsuccessful references');
for(const failure of failed) console.log(failure);
if(failed.length)process.exitCode=1;
