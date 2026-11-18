(function (root, factory) {
  const geology = factory();
  if (typeof module !== 'undefined' && module.exports) module.exports = geology;
  else root.CircuitRoboticsWorld = geology;
})(globalThis, function () {
  'use strict';

  const deposits = [
    { tile:'COBALT_ORE', total:300, centers:[[38,47],[52,49],[66,52],[81,46],[95,54],[112,49],[128,55],[148,52],[174,54],[195,55]], rx:8, ry:4 },
    { tile:'IRIDIUM_ORE', total:224, centers:[[40,63],[52,60],[73,64],[91,58],[108,64],[129,66],[152,61],[177,65],[188,67],[201,69]], rx:7, ry:4 },
    { tile:'AETHERIUM_ORE', total:102, centers:[[31,76],[42,75],[67,78],[92,76],[116,78],[144,75],[167,78],[189,80],[207,77]], rx:6, ry:3 }
  ];

  const vaults = [
    { key:'metallurgy', name:'Induction metallurgy', x:55, y:48, supplies:{ cobaltOre:6, steel:3 } },
    { key:'robotics', name:'Robotics foundry', x:83, y:57, supplies:{ circuit:3, gear:2 } },
    { key:'aetherEngineering', name:'Aether engineering', x:155, y:72, supplies:{ iridiumOre:5, crystal:4 } },
    { key:'phaseBlade', name:'Phase Blade', x:63, y:68, supplies:{ crystal:5 } },
    { key:'coilLance', name:'Coil Lance', x:107, y:69, supplies:{ copperIngot:6 } },
    { key:'voidPulser', name:'Void Pulser', x:132, y:75, supplies:{ iridiumOre:3 } },
    { key:'siegeRailcannon', name:'Siege Railcannon', x:194, y:72, supplies:{ steel:7 } },
    { key:'sunspikeLaser', name:'Sunspike Laser', x:207, y:79, supplies:{ aetheriumOre:2 } },
    { key:'starflight', name:'Starflight', x:178, y:77, supplies:{ aetheriumOre:2, advancedComponent:2 } }
  ];

  const adjacent = [[1,0],[-1,0],[0,1],[0,-1]];

  function install({ T, get, set, inside, getMeta, hash, stats }) {
    if (stats.roboticsGeology) return;
    const natural = new Set([T.STONE,T.COPPER,T.IRON,T.CRYSTAL,T.OBSIDIAN,T.DIRT]);
    const exposed = (x,y) => adjacent.some(([dx,dy]) => inside(x+dx,y+dy) && get(x+dx,y+dy) === T.AIR);
    const virgin = (x,y) => inside(x,y) && !getMeta(x,y);
    const sites = {};
    for (const vault of vaults) {
      let best = null, bestScore = Infinity;
      for (let dy=-9; dy<=9; dy++) for (let dx=-10; dx<=10; dx++) {
        const x=vault.x+dx, y=vault.y+dy;
        if (!virgin(x,y)) continue;
        const tile=get(x,y);
        if (tile!==T.AIR && !natural.has(tile)) continue;
        const score=Math.hypot(dx,dy)+(tile===T.AIR?0:16)+(exposed(x,y)?0:10);
        if(score<bestScore){ best=[x,y]; bestScore=score; }
      }
      if (!best) continue;
      const [x,y]=best;
      set(x,y,T.STORAGE,{rot:0,contents:{...vault.supplies},discovery:true,schematic:vault.key,siteName:vault.name+' vault'});
      sites[vault.key]={name:vault.name,x,y};
    }
    for (const vein of deposits) {
      const groups=vein.centers.map(([cx,cy]) => {
        const candidates=[];
        for(let dy=-vein.ry;dy<=vein.ry;dy++)for(let dx=-vein.rx;dx<=vein.rx;dx++){
          const x=cx+dx,y=cy+dy;
          if(!virgin(x,y)||!natural.has(get(x,y)))continue;
          const radial=(dx/vein.rx)**2+(dy/vein.ry)**2;
          if(radial>1.1)continue;
          candidates.push({x,y,rank:radial+hash(x,y)*.42-(exposed(x,y)?.33:0)});
        }
        return candidates.sort((a,b)=>a.rank-b.rank);
      });
      let remaining=vein.total;
      for(let rank=0;remaining>0&&groups.some(group=>rank<group.length);rank++){
        for(const group of groups){
          const point=group[rank];
          if(!point||!natural.has(get(point.x,point.y)))continue;
          set(point.x,point.y,T[vein.tile]);
          if(--remaining===0)break;
        }
      }
    }
    stats.roboticsGeology=true;
    stats.roboticsSites=sites;
  }

  return { install };
});
