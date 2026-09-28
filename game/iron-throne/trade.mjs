import { planningView } from './ai-knowledge.mjs';
import { isHumanHouse, isAiHouse } from './house-control.mjs';
import { BUILDINGS, RESOURCES, RESOURCE_VALUES } from './data.mjs';
import { PLAYER, alive, canAfford, kingdom, pay, relation } from './core.mjs';
import { appendConversation, economicRelationship, recordTrade } from './living.mjs';

export { tradeInfrastructure, tradeRoute, contractCheck, contractAnchors, economicNeeds, publicEconomy } from './trade-economy.mjs';
import { tradeInfrastructure, tradeRoute, economicNeeds } from './trade-economy.mjs';

function candidate(s,from,to) {
  if(from===to||!alive(s,to))return null;
  s=planningView(s,from);
  const route=tradeRoute(s,from,to);if(!route.safe)return null;
  const a=kingdom(s,from),needs=economicNeeds(s,from),supplies=RESOURCES.map(resource=>({resource,surplus:36,production:0}));
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
      if(isHumanHouse(s,to)&&tradeInfrastructure(s,from).level&&tradeInfrastructure(s,to).level&&s.turn%4===0&&give.surplus+give.production*5>=receive*5&&surplus+supplies.find(x=>x.resource===n.resource).production*5>=amount*5) {intent.type='RECURRING';intent.tradeKind='recurring';}
      if(!best||score>best.score)best={score,intent,reason:`${a.name} seeks ${n.resource} for ${a.economicPlan?BUILDINGS[a.economicPlan.type].name:'its population and military plans'} and offers surplus ${give.resource}.`,route};
    }
  }
  return best;
}
export function scheduleTrade(s, actorHouseId = PLAYER) {
  const c=s.commerce;c.offers=c.offers.filter(o=>o.expires>=s.turn).slice(s.controllers?-72:-12);
  if((s.controllers ? c.lastOfferByHouse?.[actorHouseId] : c.lastOfferTurn)===s.turn)return;
  const candidates=[];
  for(const k of s.kingdoms.filter(k=>isAiHouse(s,k.id)&&alive(s,k.id))){
    const crisis=k.resources.food<25,cooldown=crisis?2:4;
    if(s.turn-(c.cooldowns[`${actorHouseId}:${k.id}`]??-9)<cooldown||c.offers.some(o=>o.from===k.id&&(o.to||PLAYER)===actorHouseId&&o.status==='pending'))continue;
    if(!crisis&&s.turn%2)continue;
    const offer=candidate(s,k.id,actorHouseId);if(offer&&offer.score>=18)candidates.push({...offer,from:k.id});
  }
  candidates.sort((a,b)=>b.score-a.score||a.from.localeCompare(b.from));const best=candidates[0];if(!best)return;
  const offer={id:s.nextId++,from:best.from,to:actorHouseId,intent:best.intent,reason:best.reason,created:s.turn,expires:s.turn+3,status:'pending'};
  c.offers.push(offer);c.cooldowns[`${actorHouseId}:${best.from}`]=s.turn;c.lastOfferTurn=s.turn;c.lastOfferByHouse||={};c.lastOfferByHouse[actorHouseId]=s.turn;
  appendConversation(s,best.from,'ruler',`${offer.reason} We request ${offer.intent.giveAmount} ${offer.intent.giveResource} for ${offer.intent.receiveAmount} ${offer.intent.receiveResource}${offer.intent.type==='RECURRING'?` each turn for ${offer.intent.duration} turns`:''}.`,{unread:true,kind:'trade-dispatch',proposal:offer.intent,actorHouseId});
}
export function aiResourceTrade(s,onlyOwner=null) {
  if(s.turn%3)return;
  for(const k of s.kingdoms.filter(k=>isAiHouse(s,k.id)&&alive(s,k.id)&&(!onlyOwner||k.id===onlyOwner))){
    if(s.turn-(s.commerce.aiTrades[k.id]||0)<4)continue;
    const options=s.kingdoms.filter(o=>isAiHouse(s,o.id)&&o.id!==k.id&&alive(s,o.id)).map(o=>({partner:o.id,offer:candidate(s,k.id,o.id)})).filter(x=>x.offer).sort((a,b)=>b.offer.score-a.offer.score);
    const best=options[0];if(!best)continue;
    const other=kingdom(s,best.partner),i=best.offer.intent;
    if(economicNeeds(s,other.id).find(n=>n.resource===i.giveResource).surplus<i.giveAmount)continue;
    if(!canAfford(other,{[i.giveResource]:i.giveAmount})||!canAfford(k,{[i.receiveResource]:i.receiveAmount}))continue;
    pay(other,{[i.giveResource]:i.giveAmount});pay(k,{[i.giveResource]:i.giveAmount},1);pay(k,{[i.receiveResource]:i.receiveAmount});pay(other,{[i.receiveResource]:i.receiveAmount},1);
    recordTrade(s,other.id,k.id,i.giveResource,i.giveAmount,'ai-trade');recordTrade(s,k.id,other.id,i.receiveResource,i.receiveAmount,'ai-trade');s.commerce.aiTrades[k.id]=s.turn;
  }
}
