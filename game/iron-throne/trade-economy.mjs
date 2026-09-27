// Shared read-only trade economy. Kept separate from proposal generation so
// living diplomacy can inspect needs without importing the deal evaluator.
import { BUILDINGS, REGIONS, RESOURCES, UNITS } from './data.mjs';
import { buildingLevel, constructionSpec, productionPlan } from './economy.mjs';
import { atWar, economyProjection, kingdom, neighbors, passable, relation, settlements, treaty } from './core.mjs';
import { tradeBlocked } from './living.mjs';

export function tradeInfrastructure(s,owner) {
  const tiles=Object.values(s.tiles).filter(t=>t.owner===owner),outposts=tiles.filter(t=>buildingLevel(t,'tradeOutpost'));
  const level=outposts.reduce((n,t)=>Math.max(n,buildingLevel(t,'tradeOutpost')),0);
  return {outposts,level,capacity:Math.min(12,1+outposts.reduce((n,t)=>n+buildingLevel(t,'tradeOutpost'),0)+tiles.reduce((n,t)=>n+Math.floor(buildingLevel(t,'market')/2)+buildingLevel(t,'merchantGuild'),0)),shipment:12+level*12,bonus:tiles.reduce((n,t)=>n+buildingLevel(t,'merchantGuild')*2,0)};
}
// Read-only route search. Called by rules/inspection, never by map animation.
export function tradeRoute(s,from,to) {
  if(atWar(s,from,to)||tradeBlocked(s,from,to))return {safe:false,status:'War / embargo',path:[]};
  const starts=settlements(s,from),ends=new Set(settlements(s,to).map(t=>t.id));
  const occupied=t=>s.armies.some(a=>a.tile===t.id&&(atWar(s,from,a.owner)||atWar(s,to,a.owner)));
  const ports=starts.filter(t=>buildingLevel(t,'harbor')&&!occupied(t));
  const destination=settlements(s,to).find(t=>buildingLevel(t,'harbor')&&!occupied(t));
  if(ports.length&&destination)return {safe:true,status:'Coastal shipping',path:[ports[0].id,destination.id],capacity:72,fee:0,anchors:[ports[0].id,destination.id]};
  for(const roadsOnly of [true,false]) {
    const queue=starts.filter(t=>!occupied(t)&&(!roadsOnly||t.road)).map(t=>t.id),came=new Map(queue.map(id=>[id,null]));
    for(let head=0;head<queue.length;head++){
      const id=queue[head],t=s.tiles[id];
      if(ends.has(id)){
        const path=[];let cursor=id;while(cursor!==null){path.unshift(cursor);cursor=came.get(cursor);}
        const fromInfra=tradeInfrastructure(s,from),toInfra=tradeInfrastructure(s,to),roadLevel=roadsOnly?Math.min(...path.map(id=>buildingLevel(s.tiles[id],'road'))):0,capacity=Math.min(fromInfra.shipment,toInfra.shipment)+(roadsOnly?12+(roadLevel-1)*12:0);
        return {safe:true,status:roadLevel===3?'Royal Highway':roadsOnly?'Connected road':'Overland caravan',path,capacity,fee:roadsOnly?0:Math.max(0,2-Math.max(fromInfra.level,toInfra.level)),anchors:[]};
      }
      for(const n of neighbors(s,t)){
        if(came.has(n.id)||!passable(n)||roadsOnly&&!n.road||occupied(n))continue;
        if(n.owner&&![from,to].includes(n.owner)&&(tradeBlocked(s,from,n.owner)||atWar(s,from,n.owner)||!treaty(s,from,n.owner,'trade')&&!treaty(s,from,n.owner,'alliance')))continue;
        came.set(n.id,id);queue.push(n.id);
      }
    }
  }
  return {safe:false,status:'Route blocked',path:[]};
}
export function contractCheck(s,from,to,i,{existing=false,anchors=[]}={}) {
  const route=tradeRoute(s,from,to);
  if(!route.safe)return route.status;
  if(anchors.some(a=>!s.tiles[a.tile]||s.tiles[a.tile].owner!==a.owner||buildingLevel(s.tiles[a.tile],a.type)<a.level))return 'A contracted trade outpost or harbor was captured or lost.';
  const capacity=Math.floor(route.capacity*(i.tradeKind==='strategic'?1.5:1));
  if(Math.max(i.giveAmount,i.receiveAmount)>capacity)return `Route capacity is ${capacity} units per shipment; develop trade outposts or roads.`;
  if(!existing)for(const owner of [from,to])if(s.treaties.filter(t=>t.type==='recurring'&&t.expires>s.turn&&t.parties.includes(owner)).length>=tradeInfrastructure(s,owner).capacity)return 'Trade contract capacity is full; develop markets, outposts or a merchant guild.';
  if(i.tradeKind==='strategic'&&relation(s,to,from).trust<30)return 'Strategic supply requires 30 trust.';
  if(i.tradeKind==='preferential'&&!treaty(s,from,to,'alliance')&&relation(s,to,from).trust<45)return 'Preferential trade requires an alliance or 45 trust.';
  if(i.tradeKind==='preferential')route.fee=0;
  if(!existing&&[from,to].some(id=>kingdom(s,id).resources.gold<(i.giveResource==='gold'&&id===from?i.giveAmount:i.receiveResource==='gold'&&id===to?i.receiveAmount:0)+route.fee))return 'Insufficient gold for transport.';
  return null;
}
export function contractAnchors(s,from,to) {
  const result=[];
  for(const owner of [from,to]){
    const best=tradeInfrastructure(s,owner).outposts.sort((a,b)=>buildingLevel(b,'tradeOutpost')-buildingLevel(a,'tradeOutpost'))[0];
    if(best)result.push({tile:best.id,owner,type:'tradeOutpost',level:buildingLevel(best,'tradeOutpost')});
  }
  const route=tradeRoute(s,from,to);
  for(const id of route.anchors||[])result.push({tile:id,owner:s.tiles[id].owner,type:'harbor',level:1});
  return result;
}
export function economicNeeds(s,owner) {
  const k=kingdom(s,owner),{income}=economyProjection(s,owner),goals={food:60,wood:55,stone:45,iron:30,gold:65,horses:8,tools:12,arms:8};
  const plan=k.economicPlan;
  if(plan&&BUILDINGS[plan.type])for(const [r,n] of Object.entries(constructionSpec(s.tiles[plan.tile],plan.type)?.cost||{}))goals[r]=Math.max(goals[r],n);
  const military=REGIONS[owner].troops==='cavalry'?'knight':REGIONS[owner].troops==='archer'?'veteranArcher':'menAtArms';
  if(s.turn>4)for(const [r,n] of Object.entries(UNITS[military].cost))goals[r]=Math.max(goals[r],n*2);
  const committed=Object.fromEntries(RESOURCES.map(r=>[r,0]));
  for(const t of s.treaties.filter(t=>t.type==='recurring'&&t.expires>s.turn&&t.parties.includes(owner))){const payer=t.payer===owner;committed[payer?t.intent.giveResource:t.intent.receiveResource]+=payer?t.intent.giveAmount:t.intent.receiveAmount;committed[payer?t.intent.receiveResource:t.intent.giveResource]-=payer?t.intent.receiveAmount:t.intent.giveAmount;}
  const needs=RESOURCES.map(r=>({resource:r,need:Math.max(0,goals[r]+committed[r]*2-k.resources[r]-income[r]*2),surplus:Math.max(0,k.resources[r]-goals[r]-committed[r]*2),production:income[r],goal:goals[r]}));
  return needs;
}
export function publicEconomy(s,owner) {
  const needs=economicNeeds(s,owner);
  const capital=Object.values(s.tiles).find(t=>t.owner===owner&&t.capital),region=s.worldGeneration?s.regions?.[capital?.region]:REGIONS[owner];
  return {region:region?.name||'Unfounded realm',specialty:region?.description||'Choose a starting region.',imports:needs.filter(n=>n.need>5).map(n=>n.resource),exports:needs.filter(n=>n.surplus>30).map(n=>n.resource)};
}
