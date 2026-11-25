/** Deterministic bot-led pacing sample. This is not a human win-rate estimate. */
import {Game} from '../game/moba-mania/engine.mjs';
import {HEROES,DIFFICULTIES} from '../game/moba-mania/data.mjs';
import {ITEM_BY_ID} from '../game/moba-mania/items.mjs';

const args = process.argv.slice(2);
const option = (name,fallback) => args.find(a=>a.startsWith(`--${name}=`))?.split('=')[1] ?? fallback;
const heroes = option('heroes',Object.keys(HEROES).join(',')).split(',').filter(id=>HEROES[id]);
const difficulties = option('difficulties','normal,hard').split(',').filter(id=>DIFFICULTIES[id]);
const minutes = Math.min(30,Math.max(1,Number(option('minutes','18'))||18));
const seed = Number(option('seed','260125')) || 260125;
const results = [];
for (const [index,hero] of heroes.entries()) for (const difficulty of difficulties) {
  const game = new Game({hero,difficulty,seed:seed+index}), player = game.player;
  player.isPlayer=false;player.bot=true;
  const levels=Object.fromEntries([6,10,12,15,18].map(l=>[l,null]));
  let firstUpgrade=null,firstAdvanced=null,alive=0,travel=0,combat=0,maxUnits=game.units.length;
  const started=performance.now();
  while(game.time<minutes*60&&game.winner===null) {
    const prior=game.time;game.update(.25);const dt=game.time-prior;
    if(player.alive){alive+=dt;if(player.moving)travel+=dt;if(game.time-player.lastDamageAt<1||player.attackAnim>0)combat+=dt;}
    for(const level of [6,10,12,15,18])if(levels[level]===null&&player.level>=level)levels[level]=Math.round(game.time);
    if(firstUpgrade===null&&player.items.some(id=>ITEM_BY_ID[id].tier>=2))firstUpgrade=Math.round(game.time);
    if(firstAdvanced===null&&player.items.some(id=>ITEM_BY_ID[id].tier===3))firstAdvanced=Math.round(game.time);
    maxUnits=Math.max(maxUnits,game.units.length);
    if(game.units.some(u=>!Number.isFinite(u.hp)||!Number.isFinite(u.x)||!Number.isFinite(u.y)))throw Error('Nonfinite battle state');
  }
  const result={hero,difficulty,seed:game.seed,finished:game.winner!==null,winner:game.winner,seconds:Math.round(game.time),
    level:player.level,levelTimes:levels,firstUpgrade,firstAdvanced,advancedItems:player.items.filter(id=>ITEM_BY_ID[id].tier===3).length,
    goldEarned:player.goldEarned,kills:player.kills,assists:player.assists,deaths:player.deaths,metrics:player.metrics,
    aliveSeconds:Math.round(alive),movingSeconds:Math.round(travel),combatSeconds:Math.round(combat),maxUnits,
    runtimeSeconds:Number(((performance.now()-started)/1000).toFixed(2))};
  results.push(result);console.log(JSON.stringify(result));
}
if(!results.length)throw Error('Choose at least one known hero and difficulty');
const average=key=>Number((results.reduce((n,r)=>n+r[key],0)/results.length).toFixed(2));
console.log(JSON.stringify({summary:true,sample:'deterministic bot-led matches; travel/combat can overlap',matches:results.length,
  completed:results.filter(r=>r.finished).length,averageSeconds:average('seconds'),averageEndLevel:average('level'),averageAdvancedItems:average('advancedItems')}));
