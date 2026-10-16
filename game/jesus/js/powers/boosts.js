export const BOOST_SECONDS = {water:45,wind:30};
export const PICKUP_MESSAGES = {
  water:['Living Water','Jump again in midair · 45 seconds'],
  wind:['Wind Sandals','Run faster · Dash farther and more often · 30 seconds'],
  fireRain:['Fire Rain','3 fire casts · Use blessing'],
  shield:['Shield','Blocks a hit, then recharges'],
  sandals:['Swift Sandals','Faster running for this journey'],
  wings:['Angel Wings','Extra air jump · Hold jump to glide'],
  holyLight:['Holy Light','Light refilled · Use blessing'],
  bread:['Bread','Health restored'],
};
export function cleanBoosts(value) {
  return Object.fromEntries(Object.entries(BOOST_SECONDS).map(([key,max])=>
    [key,Number.isFinite(value?.[key])?Math.max(0,Math.min(max,value[key])):0]));
}
