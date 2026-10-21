// Deterministic CPU workload; this does not measure browser FPS or mobile GPU time.
import {performance} from 'node:perf_hooks';
import {createRequire} from 'node:module';
const require=createRequire(import.meta.url);
const {NavigationQueue,SpatialHash}=require('./living-performance.js');
const enemies=Array.from({length:1200},(_,i)=>({x:(i*31)%1440,y:(i*43)%1440}));
const hash=new SpatialHash(12); hash.rebuild(enemies);
let candidateChecks=0;
const started=performance.now();
for(let tick=0;tick<300;tick++) for(let i=0;i<240;i++) candidateChecks+=hash.query((i*37)%1440,(i*53)%1440,14).length;
const spatialMs=performance.now()-started;
const nav=new NavigationQueue(1440,1440,(x,y)=>x>=700&&x<800&&y>=700&&y<800);
for(let i=0;i<64;i++) nav.request(710+i%8,710+Math.floor(i/8),'point:790,790:0',(x,y)=>x===790&&y===790);
const sliceTimes=[]; let maxExpanded=0;
while(nav.active||nav.pending.length){ const t=performance.now(); maxExpanded=Math.max(maxExpanded,nav.tick(6000,0));sliceTimes.push(performance.now()-t); }
sliceTimes.sort((a,b)=>a-b);
console.log(JSON.stringify({entities:1200,queries:72000,spatialMs:Number(spatialMs.toFixed(2)),candidateChecks,bruteForceChecks:1200*72000,navigationRequests:64,navigationSlices:sliceTimes.length,maxExpandedPerSlice:maxExpanded,regionalRouteJoins:nav.stats.regionalHits,navigationSliceP95Ms:Number(sliceTimes[Math.floor(sliceTimes.length*0.95)].toFixed(2)),note:'Synthetic Node CPU benchmark; browser/device performance must be profiled separately.'},null,2));
