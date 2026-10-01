import { RESOURCES } from './data.mjs';

export const packageTrade = i => ['EXCHANGE','RECURRING'].includes(i?.type);
// Arrays are authoritative; scalar fields remain mirrors of the first row for
// older callers. Duplicate resources merge before affordability or valuation.
export function normalizeItems(items) {
  if (!Array.isArray(items) || !items.length || items.length > RESOURCES.length) return null;
  const totals = new Map();
  for (const item of items) {
    if (!item || typeof item !== 'object' || Array.isArray(item) || Object.keys(item).some(k=>!['resource','amount'].includes(k)) || !RESOURCES.includes(item.resource) || !Number.isInteger(item.amount) || item.amount < 1 || item.amount > 1000) return null;
    const amount = (totals.get(item.resource)||0)+item.amount;
    if (amount > 1000) return null;
    totals.set(item.resource,amount);
  }
  return [...totals].sort(([a],[b])=>RESOURCES.indexOf(a)-RESOURCES.indexOf(b)).map(([resource,amount])=>({resource,amount}));
}
export function tradeItems(i, side) {
  return i[`${side}Items`] ?? (i[`${side}Amount`] ? [{resource:i[`${side}Resource`],amount:i[`${side}Amount`]}] : []);
}
export const itemCost = items => Object.fromEntries(items.map(x=>[x.resource,x.amount]));
export const itemTotal = items => items.reduce((n,x)=>n+x.amount,0);
export const itemText = items => items.map(x=>`${x.amount} ${x.resource}`).join(' + ');
export function withItems(i, giveItems, receiveItems) {
  giveItems=normalizeItems(giveItems)||[];receiveItems=normalizeItems(receiveItems)||[];
  return {...i,giveItems,receiveItems,giveResource:giveItems[0]?.resource||'gold',giveAmount:giveItems[0]?.amount||0,receiveResource:receiveItems[0]?.resource||'food',receiveAmount:receiveItems[0]?.amount||0};
}
export function normalizePackage(i) {
  const give=normalizeItems(tradeItems(i,'give')),receive=normalizeItems(tradeItems(i,'receive'));
  return give && receive ? withItems(i,give,receive) : null;
}
export const packageKey = i => JSON.stringify([tradeItems(i,'give'),tradeItems(i,'receive')]);
export function scalePackage(i, scale) {
  return withItems(i,...['give','receive'].map(side=>tradeItems(i,side).map(x=>({...x,amount:Math.max(1,Math.floor(x.amount*scale))}))));
}
export function migrateTradePackages(s) {
  // Also migrate proposals already stored in court histories and multiplayer.
  const visit = value => {
    if (!value || typeof value!=='object') return;
    if (packageTrade(value)) {
      const canonical=normalizePackage(value);
      if(!canonical)throw new Error('Damaged trade package.');
      Object.assign(value,canonical);
    }
    for(const child of Object.values(value))if(child&&typeof child==='object')visit(child);
  };
  visit(s);
}
