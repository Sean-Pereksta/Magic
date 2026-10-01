import { tradeItems, itemTotal } from './trade-package.mjs';
import { blockadeAt } from './naval.mjs';
// Shared read-only trade economy. Kept separate from proposal generation so
// living diplomacy can inspect needs without importing the deal evaluator.
import { BUILDINGS, REGIONS, RESOURCES, RESOURCE_VALUES, UNITS } from './data.mjs';
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
  const ports=starts.filter(t=>buildingLevel(t,'harbor')&&!occupied(t)&&!blockadeAt(s,t.id,t.owner));
  const destination=settlements(s,to).find(t=>buildingLevel(t,'harbor')&&!occupied(t)&&!blockadeAt(s,t.id,t.owner));
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
  if(Math.max(itemTotal(tradeItems(i,'give')),itemTotal(tradeItems(i,'receive')))>capacity)return `Route capacity is ${capacity} units per shipment; develop trade outposts or roads.`;
  if(!existing)for(const owner of [from,to])if(s.treaties.filter(t=>t.type==='recurring'&&t.expires>s.turn&&t.parties.includes(owner)).length>=tradeInfrastructure(s,owner).capacity)return 'Trade contract capacity is full; develop markets, outposts or a merchant guild.';
  if(i.tradeKind==='strategic'&&relation(s,to,from).trust<30)return 'Strategic supply requires 30 trust.';
  if(i.tradeKind==='preferential'&&!treaty(s,from,to,'alliance')&&relation(s,to,from).trust<45)return 'Preferential trade requires an alliance or 45 trust.';
  if(i.tradeKind==='preferential')route.fee=0;
  if(!existing&&[from,to].some(id=>kingdom(s,id).resources.gold<(tradeItems(i,id===from?'give':'receive').find(x=>x.resource==='gold')?.amount||0)+route.fee))return 'Insufficient gold for transport.';
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
  const k=kingdom(s,owner),projection=economyProjection(s,owner),income=projection.income;
  const reserve={food:Math.max(50,(projection.food?.consumption||15)*4),wood:35,stone:25,iron:20,gold:65,horses:6,tools:8,arms:6};
  const spending=Object.fromEntries(RESOURCES.map(r=>[r,0])),obligations={...spending},incoming={...spending};
  const add=cost=>{for(const [r,n] of Object.entries(cost||{}))spending[r]+=n;};
  if(k.economicPlan&&BUILDINGS[k.economicPlan.type]&&!s.tiles[k.economicPlan.tile]?.project)add(constructionSpec(s.tiles[k.economicPlan.tile],k.economicPlan.type)?.cost);
  const war=s.wars.some(w=>w.split(':').includes(owner));
  const military=REGIONS[owner]?.troops==='cavalry'?'lightCavalry':REGIONS[owner]?.troops==='archer'?'archer':'spearman';
  const plans=(s.intrigue?.plans||[]).filter(p=>p.actor===owner&&['Preparing','Committed','Executing'].includes(p.status));
  const troops=[...s.armies,...(s.fleets||[]).flatMap(f=>f.cargo||[])].filter(a=>a.owner===owner).reduce((n,a)=>n+Object.values(a.units).reduce((m,v)=>m+v,0),0);
  const batches=Math.min(3,Math.max(s.turn>4||war?1:0,...plans.map(p=>Math.ceil(Math.max(0,(p.requiredForces||0)-troops)/UNITS[military].count))));
  for(let i=0;i<batches;i++)add(UNITS[military].cost);
  if(k.goal==='EXPAND'&&k.economicPlan?.type!=='town')add(BUILDINGS.town.cost);
  for(const t of s.treaties.filter(t=>t.type==='recurring'&&t.expires>s.turn&&t.parties.includes(owner))){
    const turns=Math.min(3,t.expires-s.turn),payer=t.payer===owner;
    for(const x of tradeItems(t.intent,payer?'give':'receive'))obligations[x.resource]+=x.amount*turns;
    for(const x of tradeItems(t.intent,payer?'receive':'give'))incoming[x.resource]+=x.amount*turns;
  }
  for(const p of s.pledges||[])if(p.debtor===owner&&p.status==='pending'&&!p.delivered&&(p.intent.type==='PROMISE'||p.operationTask==='supply')&&p.intent.giveAmount)obligations[p.intent.giveResource]+=p.intent.giveAmount;
  const tiles=Object.values(s.tiles).filter(t=>t.owner===owner&&t.fog!=='unknown');
  return RESOURCES.map(r=>{
    const sites=tiles.filter(t=>t.resource===r).length,geographicScarcity=['food','wood','stone','iron','horses'].includes(r)&&sites<3;
    const goal=reserve[r]+spending[r]+obligations[r],projected=k.resources[r]+income[r]*3+incoming[r]-obligations[r]-spending[r];
    const need=Math.max(0,reserve[r]-projected),surplus=Math.max(0,Math.min(k.resources[r]-goal,projected-reserve[r]));
    const state=projected<reserve[r]*.25?'Critical':need>0?'Needed':projected<reserve[r]*1.5?'Useful':surplus>reserve[r]*2?'Excess':surplus>reserve[r]*.5?'Surplus':'Comfortable';
    const multiplier=({Critical:3.5,Needed:2.2,Useful:1.35,Comfortable:1,Surplus:.7,Excess:.5}[state])*(geographicScarcity?1.2:1);
    return {resource:r,state,need,surplus,production:income[r],goal,reserve:reserve[r],planned:spending[r],obligations:obligations[r],projected,geographicScarcity,value:RESOURCE_VALUES[r]*multiplier};
  });
}
export const packageValue=(needs,items)=>items.reduce((n,x)=>n+x.amount*needs.find(r=>r.resource===x.resource).value,0);
export function publicEconomy(s,owner) {
  const needs=economicNeeds(s,owner);
  const capital=Object.values(s.tiles).find(t=>t.owner===owner&&t.capital),region=s.worldGeneration?s.regions?.[capital?.region]:REGIONS[owner];
  return {region:region?.name||'Unfounded realm',specialty:region?.description||'Choose a starting region.',imports:needs.filter(n=>n.need>5).map(n=>n.resource),exports:needs.filter(n=>n.surplus>30).map(n=>n.resource)};
}
