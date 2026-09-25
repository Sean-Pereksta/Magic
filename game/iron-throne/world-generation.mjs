import { MAP_PROFILES, selectMapProfile } from './map-profiles.mjs';
import { hash, seededRandom, sample, distance, neighbors, passable, tileId } from './world-hex.mjs';
import { planFoundings } from './founding.mjs';

function noise(seed,x,y,scale) {
  x/=scale;y/=scale;const q=Math.floor(x),r=Math.floor(y),smooth=n=>n*n*(3-2*n),a=smooth(x-q),b=smooth(y-r);
  const mix=(v,w,n)=>v+(w-v)*n;
  return mix(mix(sample(seed,q,r),sample(seed,q+1,r),a),mix(sample(seed,q,r+1),sample(seed,q+1,r+1),a),b);
}
function regionName(t,width,height,terrain) {
  const vertical=t.r<height*.34?'Northern':t.r>height*.66?'Southern':'';
  const horizontal=t.q<width*.34?'Western':t.q>width*.66?'Eastern':'';
  const direction=[vertical,horizontal].filter(Boolean).join(' ')||'Central';
  return `${direction} ${{forest:'Woodlands',plains:'Plains',hills:'Highlands',mountain:'Range',coast:'Coast',water:'Waters'}[terrain]}`;
}
export function generateRegions(s,profile,seed) {
  const roll=seededRandom(hash(seed,'regions')),regions=[];
  // Large jittered region cells with broad boundaries. Detail is coherent noise,
  // so exceptions become small clearings/hill groups rather than white-noise hexes.
  for(let r=0;r<Math.ceil(s.height/10);r++)for(let q=0;q<Math.ceil(s.width/10);q++){
    const n=roll(),terrain=n<profile.forest?'forest':n<profile.forest+profile.hills?'hills':'plains';
    const t={q:4+q*10+(roll()-.5)*5,r:4+r*10+(roll()-.5)*5};
    regions.push({...t,id:`region-${r*Math.ceil(s.width/10)+q}`,terrain,name:regionName(t,s.width,s.height,terrain),description:`Predominantly ${terrain}, with natural clearings, foothills and resource pockets.`});
  }
  // Every world has a broad forest, plains and highland region, independent of Houses.
  for(const [i,terrain]of [[0,'forest'],[6,'plains'],[10,'hills']]){regions[i].terrain=terrain;regions[i].name=regionName(regions[i],s.width,s.height,terrain);regions[i].description=`Predominantly ${terrain}, with natural clearings, foothills and resource pockets.`;}
  s.regions=Object.fromEntries(regions.map(r=>[r.id,r]));
  for(let r=0;r<s.height;r++)for(let q=0;q<s.width;q++){
    const t={id:tileId(q,r),q,r,terrain:'plains',resource:null,quality:'normal',owner:null,building:null,road:false,river:false,walls:0,market:false,workshop:false,project:null,capital:null,levels:{}};
    const region=[...regions].sort((a,b)=>distance(t,a)-distance(t,b)||a.id.localeCompare(b.id))[0];
    t.region=region.id;t.biome=region.terrain;
    const detail=noise(hash(seed,'detail'),q,r,2.4);
    const cut={forest:[.14,.22,.74,.91],plains:[.1,.2,.76,.94],hills:[.12,.25,.81,.96]}[region.terrain];
    t.terrain=region.terrain;
    if(detail<cut[1])t.terrain='plains';
    else if(detail>cut[2])t.terrain=region.terrain==='hills'?'forest':'hills';
    else if(region.terrain==='plains'&&detail<.38)t.terrain='forest';
    // Coast depth is sampled smoothly along each edge; never cut a random corridor.
    const coast=profile.coast;
    const west=1+noise(hash(seed,'west'),r,0,8)*coast,east=1+noise(hash(seed,'east'),r,0,8)*coast;
    const north=1+noise(hash(seed,'north'),q,0,9)*coast,south=1+noise(hash(seed,'south'),q,0,9)*coast;
    if(q<west||q>s.width-1-east||r<north||r>s.height-1-south)t.terrain='water';
    if(profile.basin&&Math.hypot((q-s.width/2)*.8,r-s.height/2)<profile.basin)t.terrain='water';
    s.tiles[t.id]=t;
  }
}
export function generateMountainRanges(s,profile,seed) {
  const roll=seededRandom(hash(seed,'ranges')),ranges=[];
  for(let i=0;i<profile.ranges;i++){
    const horizontal=profile.axis==='horizontal',diagonal=profile.axis==='diagonal';
    const length=horizontal?s.width:s.height;
    const center=horizontal?(5+i*6)*s.height/30:profile.ranges===1?s.width*.5:(9+i*19)*s.width/40+(roll()-.5)*3;
    const phase=roll()*Math.PI*2,amplitude=1.5+roll()*2,passOffset=3+Math.floor(roll()*4),points=[];
    for(let n=5;n<length-5;n++){
      const cross=center+Math.sin(n/5+phase)*amplitude+(diagonal?(n-length/2)*.22:0);
      points.push({q:horizontal?n:cross,r:horizontal?cross:n,pass:(n-passOffset+100)%10<2});
    }
    ranges.push({id:`range-${i}`,points,width:profile.ridgeWidth});
  }
  s.mountainRanges=ranges;
  const land=Object.values(s.tiles).filter(t=>t.terrain!=='water');
  for(const range of ranges)for(const t of land){
    // A nearest centerline point determines deliberate passes before any mountain placement.
    const ranked=range.points.map(p=>({p,d:distance(t,p)})).sort((a,b)=>a.d-b.d),nearest=ranked[0];
    if(!nearest)continue;
    const taper=(nearest.p===range.points[0]||nearest.p===range.points.at(-1)) ? .6 : 1;
    const width=range.width*taper+.2*Math.sin(t.r+t.q*.35);
    if(nearest.d<width&&!nearest.p.pass)t.terrain='mountain';
    else if(nearest.d<width+1.3&&t.terrain!=='mountain')t.terrain='hills';
    if(nearest.p.pass&&nearest.d<width){t.terrain='hills';t.mountainPass=true;}
  }
  // Mark coast only after ranges are laid out; preserve wooded/mineral shore ground.
  for(const t of land)if(t.terrain==='plains'&&neighbors(s,t).some(n=>n.terrain==='water'))t.terrain='coast';
}
export function generateRivers(s,seed) {
  // Coast distance supplies an acyclic downstream gradient. Routing never changes terrain.
  const land=Object.values(s.tiles).filter(passable),queue=land.filter(t=>neighbors(s,t).some(n=>n.terrain==='water'));
  const depth=new Map(queue.map(t=>[t.id,0]));
  for(let i=0;i<queue.length;i++)for(const n of neighbors(s,queue[i]))if(passable(n)&&!depth.has(n.id)){depth.set(n.id,depth.get(queue[i].id)+1);queue.push(n);}
  const sources=land.filter(t=>t.terrain==='hills'&&depth.get(t.id)>=4&&neighbors(s,t).some(n=>n.terrain==='mountain')).sort((a,b)=>depth.get(b.id)-depth.get(a.id)||sample(seed,'spring',a.id)-sample(seed,'spring',b.id));
  const used=[];
  for(const source of sources){
    if(used.some(t=>distance(t,source)<7))continue;
    used.push(source);let t=source;
    while(t){
      t.river=true;if(depth.get(t.id)===0)break;
      t=neighbors(s,t).filter(n=>depth.get(n.id)<depth.get(t.id)).sort((a,b)=>(a.terrain==='hills'?1:0)-(b.terrain==='hills'?1:0)||sample(seed,'river',a.id)-sample(seed,'river',b.id))[0];
    }
    if(used.length===Math.round(s.width*s.height/300))break;
  }
}
export function generateResources(s,seed) {
  for(const t of Object.values(s.tiles)){
    const n=sample(seed,'resource',t.id),richness=noise(hash(seed,'quality'),t.q,t.r,5);
    t.resource=t.terrain==='forest'?'wood':t.terrain==='hills'?(n<.49?'iron':'stone'):t.terrain==='plains'?(n<.68?'food':n<.86?'horses':null):t.terrain==='coast'&&n<.38?'horses':null;
    const matches=t.terrain===t.biome;
    t.quality=richness>.74&&matches?'exceptional':richness>.48&&matches?'rich':richness<.18?'poor':'normal';
    if(t.river&&t.terrain==='plains'){t.resource='food';t.quality='rich';}
  }
}
export function validateWorld(s) {
  const tiles=Object.values(s.tiles),land=tiles.filter(passable),counts=Object.fromEntries(['plains','forest','hills','mountain'].map(type=>[type,tiles.filter(t=>t.terrain===type).length]));
  if(land.length<650||counts.plains<130||counts.forest<110||counts.hills<110||counts.mountain<20)return {ok:false,reason:'Insufficient geographic diversity.'};
  if(tiles.filter(t=>t.resource==='iron').length<40||tiles.filter(t=>t.resource==='stone').length<40)return {ok:false,reason:'Insufficient minerals.'};
  for(const [terrain,minimum] of [['forest',45],['plains',55],['hills',40],['mountain',15]]){
    const seen=new Set();let largest=0;
    for(const root of tiles.filter(t=>t.terrain===terrain)){
      if(seen.has(root.id))continue;
      const group=[root];seen.add(root.id);
      for(let i=0;i<group.length;i++)for(const n of neighbors(s,group[i]))if(n.terrain===terrain&&!seen.has(n.id)){seen.add(n.id);group.push(n);}
      largest=Math.max(largest,group.length);
    }
    if(largest<minimum)return {ok:false,reason:`No recognizable ${terrain} region.`};
  }
  const reached=new Set([land[0].id]),queue=[land[0]];
  for(let i=0;i<queue.length;i++)for(const n of neighbors(s,queue[i]))if(passable(n)&&!reached.has(n.id)){reached.add(n.id);queue.push(n);}
  if(reached.size!==land.length)return {ok:false,reason:'Land routes are disconnected.'};
  const plan=planFoundings(s,s.kingdoms.map(h=>h.id));
  return plan?{ok:true,sites:plan.map(p=>p.capital)}:{ok:false,reason:'Separated starting packages do not fit.'};
}
export function generateWorld(s,requested='random') {
  s.mapProfile=selectMapProfile(s.seed,requested);s.preset=s.mapProfile;s.worldGeneration=1;
  const profile=MAP_PROFILES[s.mapProfile];
  for(let attempt=0;attempt<96;attempt++){
    const seed=hash(s.seed,s.mapProfile,'world-v1',attempt);s.tiles={};
    generateRegions(s,profile,seed);generateMountainRanges(s,profile,seed);generateRivers(s,seed);generateResources(s,seed);
    const validation=validateWorld(s);
    if(validation.ok){s.generation={attempt,seed};return s;}
  }
  throw new Error('Unable to generate a viable world for this seed. Try a different map seed.');
}
