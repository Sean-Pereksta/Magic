import { BUILDINGS, REGIONS, RESOURCES, RESOURCE_VALUES, UNITS } from './data.mjs';
import { buildingLevel, constructionSpec, productionPlan } from './economy.mjs';
import { PLAYER, alive, atWar, canAfford, economyProjection, kingdom, neighbors, passable, pay, relation, settlements, treaty } from './core.mjs';
import { appendConversation, economicRelationship, recordTrade, tradeBlocked } from './living.mjs';
import { evaluateDeal } from './diplomacy.mjs';

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
  return {region:REGIONS[owner].name,specialty:REGIONS[owner].description,imports:needs.filter(n=>n.need>5).map(n=>n.resource),exports:needs.filter(n=>n.surplus>30).map(n=>n.resource)};
}
function candidate(s,from,to) {
  const route=tradeRoute(s,from,to);if(!route.safe)return null;
  const a=kingdom(s,from),needs=economicNeeds(s,from),supplies=economicNeeds(s,to);
  let best=null;
  for(const n of needs.filter(n=>n.need>=8)){
    const surplus=supplies.find(x=>x.resource===n.resource).surplus;if(surplus<8)continue;
    for(const give of needs.filter(x=>x.resource!==n.resource&&x.surplus>=12)){
      const amount=Math.floor(Math.min(18,n.need,surplus/2,route.capacity));
      const receive=Math.min(Math.floor(give.surplus/2),Math.floor(amount*RESOURCE_VALUES[n.resource]/RESOURCE_VALUES[give.resource]*.95),route.capacity);
      if(receive<3)continue;
      const dependency=economicRelationship(s,from,to).dependency;
      const score=n.need*Math.min(2,surplus/30)*(1+Math.max(-.8,relation(s,from,to).trust/100))*(route.status==='Connected road'?1.3:1)*(dependency>55?.45:1)*(1+tradeInfrastructure(s,to).level*.18);
      const intent={type:'EXCHANGE',duration:5,giveResource:n.resource,giveAmount:amount,receiveResource:give.resource,receiveAmount:receive,tradeKind:n.resource==='food'&&a.resources.food<30?'emergency':'immediate'};
      // Sustainable supply needs may become recurring proposals. Both stores
      // and forecast production must cover the offered five-turn obligation.
      if(to===PLAYER&&tradeInfrastructure(s,from).level&&tradeInfrastructure(s,to).level&&s.turn%4===0&&give.surplus+give.production*5>=receive*5&&surplus+supplies.find(x=>x.resource===n.resource).production*5>=amount*5&&!contractCheck(s,to,from,{...intent,type:'RECURRING'})) {intent.type='RECURRING';intent.tradeKind='recurring';}
      if(to===PLAYER){let verdict=evaluateDeal(s,from,intent);if(verdict.status==='counter'){Object.assign(intent,verdict.counter);verdict=evaluateDeal(s,from,intent);}if(verdict.status!=='accept')continue;}
      if(!best||score>best.score)best={score,intent,reason:`${a.name} seeks ${n.resource} for ${a.economicPlan?BUILDINGS[a.economicPlan.type].name:'its population and military plans'} and offers surplus ${give.resource}.`,route};
    }
  }
  return best;
}
export function scheduleTrade(s) {
  const c=s.commerce;c.offers=c.offers.filter(o=>o.expires>=s.turn).slice(-12);
  if(c.lastOfferTurn===s.turn)return;
  const candidates=[];
  for(const k of s.kingdoms.filter(k=>k.id!==PLAYER&&alive(s,k.id))){
    const crisis=k.resources.food<25,cooldown=crisis?2:4;
    if(s.turn-(c.cooldowns[k.id]??-9)<cooldown||c.offers.some(o=>o.from===k.id&&o.status==='pending'))continue;
    if(!crisis&&s.turn%2)continue;
    const offer=candidate(s,k.id,PLAYER);if(offer&&offer.score>=18)candidates.push({...offer,from:k.id});
  }
  candidates.sort((a,b)=>b.score-a.score||a.from.localeCompare(b.from));const best=candidates[0];if(!best)return;
  const offer={id:s.nextId++,from:best.from,intent:best.intent,reason:best.reason,created:s.turn,expires:s.turn+3,status:'pending'};
  c.offers.push(offer);c.cooldowns[best.from]=s.turn;c.lastOfferTurn=s.turn;
  appendConversation(s,best.from,'ruler',`${offer.reason} We request ${offer.intent.giveAmount} ${offer.intent.giveResource} for ${offer.intent.receiveAmount} ${offer.intent.receiveResource}${offer.intent.type==='RECURRING'?` each turn for ${offer.intent.duration} turns`:''}.`,{unread:true,kind:'trade-dispatch',proposal:offer.intent});
}
export function aiResourceTrade(s) {
  if(s.turn%3)return;
  for(const k of s.kingdoms.filter(k=>k.id!==PLAYER&&alive(s,k.id))){
    if(s.turn-(s.commerce.aiTrades[k.id]||0)<4)continue;
    const options=s.kingdoms.filter(o=>![PLAYER,k.id].includes(o.id)&&alive(s,o.id)).map(o=>({partner:o.id,offer:candidate(s,k.id,o.id)})).filter(x=>x.offer).sort((a,b)=>b.offer.score-a.offer.score);
    const best=options[0];if(!best)continue;
    const other=kingdom(s,best.partner),i=best.offer.intent;
    if(!canAfford(other,{[i.giveResource]:i.giveAmount})||!canAfford(k,{[i.receiveResource]:i.receiveAmount}))continue;
    pay(other,{[i.giveResource]:i.giveAmount});pay(k,{[i.giveResource]:i.giveAmount},1);pay(k,{[i.receiveResource]:i.receiveAmount});pay(other,{[i.receiveResource]:i.receiveAmount},1);
    recordTrade(s,other.id,k.id,i.giveResource,i.giveAmount,'ai-trade');recordTrade(s,k.id,other.id,i.receiveResource,i.receiveAmount,'ai-trade');s.commerce.aiTrades[k.id]=s.turn;
  }
}
