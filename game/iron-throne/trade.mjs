import { planningView } from './ai-knowledge.mjs';
import { isAiHouse } from './house-control.mjs';
import { PLAYER, alive, canAfford, kingdom, pay, relation } from './core.mjs';
import { appendConversation, recordTrade } from './living.mjs';
import { tradeItems, itemCost, itemText, withItems } from './trade-package.mjs';
export { tradeInfrastructure, tradeRoute, contractCheck, contractAnchors, economicNeeds, publicEconomy } from './trade-economy.mjs';
import { tradeRoute, economicNeeds, packageValue, buyerTradeTerms } from './trade-economy.mjs';
import { tradeBriefing, evaluateCommercial } from './trade-negotiation.mjs';
import { RESOURCE_VALUES } from './data.mjs';

function hardOpening(wanted, exports, terms) {
  const desired = terms.established ? Math.floor(Math.min(terms.shipment, wanted.need)) : terms.shipment;
  const quantities = terms.established ? [desired] : [8, 9, 10, 11, 12]
    .sort((a, b) => Math.abs(a - desired) - Math.abs(b - desired) || a - b);
  const unitValue = RESOURCE_VALUES[wanted.resource];
  const ceiling = Math.min(unitValue * .85, terms.maximumValue);
  const choices = exports.slice(0, 3);
  let payments;
  for (const amount of quantities) {
    const minimum = amount * unitValue * .75 * 10;
    const maximum = amount * ceiling * 10;
    const target = amount * unitValue * terms.openingRate * 10;
    const requested = [{ resource: wanted.resource, amount }];
    // Preserve the court's preferred export whenever one whole-resource payment fits.
    for (const choice of choices) {
      const value = Math.round(RESOURCE_VALUES[choice.resource] * 10);
      const low = Math.ceil((minimum - 1e-8) / value);
      const high = Math.min(Math.floor(choice.surplus * .6), Math.floor((maximum + 1e-8) / value));
      if (low <= high) return {
        requested,
        payment: [{ resource: choice.resource, amount: Math.max(low, Math.min(high, Math.round(target / value))) }]
      };
    }
    if (!payments) {
      // Bounded change-making in tenths of reference value: at most 3 * 816
      // entries, independent of treasury size. Reuse it across shipment choices.
      const limit = Math.floor(Math.max(...quantities) * ceiling * 10 + 1e-8);
      payments = new Map([[0, []]]);
      for (const choice of choices) {
        const value = Math.round(RESOURCE_VALUES[choice.resource] * 10);
        const stock = Math.floor(choice.surplus * .6);
        const previousPayments = new Map(payments);
        const used = new Int32Array(limit + 1).fill(-1);
        for (let paid = 0; paid <= limit; paid++) {
          if (previousPayments.has(paid)) {
            used[paid] = 0;
            continue;
          }
          if (paid < value || used[paid - value] < 0 || used[paid - value] >= stock) continue;
          used[paid] = used[paid - value] + 1;
          payments.set(paid, [
            ...previousPayments.get(paid - used[paid] * value),
            { resource: choice.resource, amount: used[paid] }
          ]);
        }
      }
    }
    const prices = [...payments.keys()].filter(value => value >= minimum - 1e-8 && value <= maximum + 1e-8);
    prices.sort((a, b) => Math.abs(a - target) - Math.abs(b - target) || a - b);
    if (prices.length) return { requested, payment: payments.get(prices[0]) };
  }
  return null;
}

export function tradeCandidate(s,from,to) {
  if(from===to||!alive(s,to))return null;
  const known=planningView(s,from),route=tradeRoute(known,from,to);if(!route.safe)return null;
  const ours=economicNeeds(s,from,{tradeReserves:true});
  // Foreign needs are authorized disclosures, never a treasury query by the
  // proposing ruler. Exact availability is checked later by the recipient.
  const foreign=Object.values(known.tiles).filter(t=>t.owner===to&&t.fog==='visible');
  const briefing=tradeBriefing(s,to,from)||{exports:[...new Set(foreign.map(t=>t.resource).filter(Boolean))],imports:[]};
  const hard=s.difficulty==='hard';
  const wanted=ours.filter(n=>n.need>=8&&briefing.exports.includes(n.resource)).sort((a,b)=>b.need*b.value-a.need*a.value).slice(0,hard?1:2);
  const exports=ours.filter(n=>n.surplus>=6&&!wanted.some(w=>w.resource===n.resource)).sort((a,b)=>Number(briefing.imports.includes(b.resource))-Number(briefing.imports.includes(a.resource))||b.surplus-a.surplus);
  if(!wanted.length||!exports.length)return null;
  const bargaining=hard?buyerTradeTerms(s,from,to,wanted.map(n=>({resource:n.resource,amount:1})),ours,known):null;
  let requested,payment;
  if(hard){
    const opening=hardOpening(wanted[0],exports,bargaining);if(!opening)return null;
    ({requested,payment}=opening);
  }else{
    requested=wanted.map(n=>({resource:n.resource,amount:Math.max(1,Math.floor(Math.min(80,n.need)))}));
    payment=[];let budget=packageValue(ours,requested)*.8;
    for(const n of exports.slice(0,3)){
      const amount=Math.min(Math.floor(n.surplus*.6),Math.max(1,Math.ceil(budget/n.value)));
      if(amount<1)continue;
      payment.push({resource:n.resource,amount});budget-=amount*n.value;if(budget<=0)break;
    }
  }
  if(!payment.length)return null;
  // Offer orientation is from the receiving player's point of view.
  const intent=withItems({type:'EXCHANGE',duration:2,tradeKind:wanted.some(n=>n.resource==='food'&&n.state==='Critical')?'emergency':'immediate'},requested,payment);
  const need=wanted[0],reason=need.resource==='food'?`${kingdom(s,from).name} needs food: our net production is ${need.production>=0?'+':''}${need.production} per turn and projected reserves are ${Math.max(0,Math.round(need.projected))}.`:`${kingdom(s,from).name} seeks ${wanted.map(n=>n.resource).join(' and ')} for reserves and planned development; we offer resources left after our own commitments.`;
  return {intent,reason,route,score:wanted.reduce((n,x)=>n+x.need*x.value,0)*(1+Math.max(-.8,relation(s,from,to).trust/100))};
}
export function scheduleTrade(s,actorHouseId=PLAYER) {
  const c=s.commerce;c.offers=c.offers.filter(o=>o.expires>=s.turn).slice(s.controllers?-72:-12);
  if((s.controllers?c.lastOfferByHouse?.[actorHouseId]:c.lastOfferTurn)===s.turn)return;
  const options=[];
  for(const k of s.kingdoms.filter(k=>isAiHouse(s,k.id)&&alive(s,k.id))){
    const need=economicNeeds(s,k.id,{tradeReserves:true}).find(n=>n.resource==='food'),cooldown=need.state==='Critical'?3:5;
    if(s.turn-(c.cooldowns[`${actorHouseId}:${k.id}`]??-9)<cooldown||c.offers.some(o=>o.from===k.id&&(o.to||PLAYER)===actorHouseId&&o.status==='pending'))continue;
    const offer=tradeCandidate(s,k.id,actorHouseId);if(offer)options.push({...offer,from:k.id});
  }
  options.sort((a,b)=>b.score-a.score||a.from.localeCompare(b.from));const best=options[0];if(!best)return;
  const offer={id:s.nextId++,from:best.from,to:actorHouseId,intent:best.intent,reason:best.reason,created:s.turn,expires:s.turn+3,status:'pending'};
  c.offers.push(offer);c.cooldowns[`${actorHouseId}:${best.from}`]=s.turn;c.lastOfferTurn=s.turn;c.lastOfferByHouse||={};c.lastOfferByHouse[actorHouseId]=s.turn;
  appendConversation(s,best.from,'ruler',`${offer.reason} We request ${itemText(tradeItems(offer.intent,'give'))} for ${itemText(tradeItems(offer.intent,'receive'))}.`,{unread:true,kind:'trade-dispatch',proposal:offer.intent,actorHouseId});
}
export function aiResourceTrade(s,onlyOwner=null) {
  if(s.turn%3)return;
  for(const k of s.kingdoms.filter(k=>isAiHouse(s,k.id)&&alive(s,k.id)&&(!onlyOwner||k.id===onlyOwner))){
    if(s.turn-(s.commerce.aiTrades[k.id]||0)<4)continue;
    const options=s.kingdoms.filter(o=>isAiHouse(s,o.id)&&o.id!==k.id&&alive(s,o.id)).map(o=>({partner:o.id,offer:tradeCandidate(s,k.id,o.id)})).filter(x=>x.offer).sort((a,b)=>b.offer.score-a.offer.score);
    for(const best of options){
      const other=kingdom(s,best.partner);let i=best.offer.intent;
      if(s.difficulty==='hard'){
        const sellerOffer=withItems(i,tradeItems(i,'receive'),tradeItems(i,'give'));
        const judge=candidate=>evaluateCommercial(s,other.id,candidate,k.id,null,false);
        const seller=evaluateCommercial(s,other.id,sellerOffer,k.id,judge);
        if(seller.status==='reject')continue;
        const agreed=seller.counter||seller.intent;
        i=withItems(agreed,tradeItems(agreed,'receive'),tradeItems(agreed,'give'));
        if(evaluateCommercial(s,k.id,i,other.id,null,false).status!=='accept')continue;
      }
      const needs=economicNeeds(s,other.id,{tradeReserves:true}),buyerNeeds=economicNeeds(s,k.id,{tradeReserves:true}),give=tradeItems(i,'give'),receive=tradeItems(i,'receive');
      // Recipient judges its own package and protected surplus. Both parties
      // must benefit according to their own values before any resources move.
      if(give.some(x=>needs.find(n=>n.resource===x.resource).surplus<x.amount))continue;
      if(s.difficulty!=='hard'&&packageValue(needs,receive)<packageValue(needs,give)*(1+Math.max(0,other.greed-.65)*.25))continue;
      if(receive.some(x=>buyerNeeds.find(n=>n.resource===x.resource).surplus<x.amount))continue;
      if(!canAfford(other,itemCost(give))||!canAfford(k,itemCost(receive)))continue;
      pay(other,itemCost(give));pay(k,itemCost(receive));pay(k,itemCost(give),1);pay(other,itemCost(receive),1);
      for(const x of give)recordTrade(s,other.id,k.id,x.resource,x.amount,'ai-trade');
      for(const x of receive)recordTrade(s,k.id,other.id,x.resource,x.amount,'ai-trade');
      s.commerce.aiTrades[k.id]=s.turn;s.commerce.aiTrades[other.id]=s.turn;break;
    }
  }
}
