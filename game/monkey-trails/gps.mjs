// No route is retained. Only the current filter baselines exist in memory.
export const MAX_MPS=4.4704; // Exactly 10 mph.
const RAD=Math.PI/180,EARTH=6371008.8;
export function delta(a,b){
  const dlat=(b.lat-a.lat)*RAD,dlon=((b.lon-a.lon+540)%360-180)*RAD;
  const h=Math.sin(dlat/2)**2+Math.cos(a.lat*RAD)*Math.cos(b.lat*RAD)*Math.sin(dlon/2)**2;
  return {meters:2*EARTH*Math.asin(Math.sqrt(Math.min(1,h))),east:dlon*EARTH*Math.cos((a.lat+b.lat)*RAD/2),north:dlat*EARTH};
}
export class MotionFilter {
  constructor(){this.reset();}
  reset(){this.raw=null;this.anchor=null;this.warm=0;}
  seed(p){this.raw=p;this.anchor=p;this.warm=1;}
  accept(position,now=Date.now(),visible=true){
    const c=position?.coords,t=position?.timestamp;
    const result=(reason,extra={})=>({meters:0,east:0,north:0,reason,mph:null,accuracy:c?.accuracy??null,...extra});
    if(!visible){this.reset();return result('Paused while hidden');}
    if(!c||![c.latitude,c.longitude,c.accuracy,t].every(Number.isFinite)||Math.abs(c.latitude)>90||Math.abs(c.longitude)>180||c.accuracy<0){this.reset();return result('Invalid GPS reading');}
    if(now-t>10000||t-now>2000){this.reset();return result('Waiting for a fresh GPS reading');}
    if(c.accuracy>25){this.reset();return result('Weak GPS · move to open sky (≤25 m accuracy needed)');}
    const speed=typeof c.speed==='number'&&Number.isFinite(c.speed)&&c.speed>=0?c.speed:null;
    const p={lat:c.latitude,lon:c.longitude,accuracy:c.accuracy,t};
    if(speed!==null&&speed>MAX_MPS){this.reset();return result('Above 10 mph · no distance credited',{mph:speed*2.236936292});}
    if(!this.raw){this.seed(p);return result('Acquiring a stable GPS fix');}
    if(t<=this.raw.t)return result('Ignoring repeated GPS reading');
    const dt=(t-this.raw.t)/1000,rawStep=delta(this.raw,p);
    if(dt>15){this.seed(p);return result('GPS gap · reacquiring; gap not counted');}
    // Ignore tiny position noise for this raw-speed check, but never ignore a
    // genuinely fast credited segment or a device-reported speed over the cap.
    const jitter=Math.max(5,Math.min(this.raw.accuracy,p.accuracy)*.7);
    if(rawStep.meters>jitter&&rawStep.meters/dt>MAX_MPS+1e-7){this.reset();return result('GPS jump or excess speed · no distance credited',{mph:rawStep.meters/dt*2.236936292});}
    this.raw=p;
    if(this.warm<2){this.warm++;this.anchor=p;return result('GPS ready · start walking');}
    if(speed!==null&&speed<.4){this.anchor=p;return result('Stationary · filtering GPS drift',{mph:speed*2.236936292});}
    const elapsed=(t-this.anchor.t)/1000,step=delta(this.anchor,p),gate=Math.max(5,(p.accuracy+this.anchor.accuracy)*.6);
    if(elapsed>25){this.anchor=p;return result('Filtering slow GPS drift');}
    if(step.meters<gate)return result('Tracking · smoothing GPS',{mph:speed===null?null:speed*2.236936292});
    const computed=step.meters/elapsed;
    if(computed>MAX_MPS+1e-7){this.reset();return result('Above 10 mph · no distance credited',{mph:computed*2.236936292});}
    if(computed<.55){this.anchor=p;return result('Stationary · filtering GPS drift');}
    this.anchor=p;return result('Walking · distance accepted',{...step,mph:(speed??computed)*2.236936292});
  }
}
export class TrailGPS {
  constructor(onFix,onStatus){this.filter=new MotionFilter();this.onFix=onFix;this.onStatus=onStatus;this.watch=null;this.lock=null;this.active=false;this.generation=0;}
  async start(){
    if(this.active)return;
    if(!globalThis.isSecureContext)throw Error('Open the HTTPS-hosted game. Browser location is not available on this insecure page.');
    if(!navigator.geolocation)throw Error('This browser does not provide geolocation.');
    if(document.hidden)throw Error('Bring the game to the foreground before starting.');
    this.active=true;this.filter.reset();const generation=++this.generation;
    this.onStatus({reason:'Requesting location permission…',accuracy:null,mph:null});
    try{
      this.watch=navigator.geolocation.watchPosition(p=>{
        if(!this.active||generation!==this.generation||document.hidden)return;
        const fix=this.filter.accept(p);this.onStatus(fix);if(fix.meters>0)this.onFix(fix);
      },error=>{
        if(generation!==this.generation)return;
        this.filter.reset();
        const message=error.code===1?'Location denied. Enable location in browser/site settings, then try again.':error.code===2?'Location unavailable. Move to open sky and retry.':'GPS timed out. Waiting for a new fix; no gap will be counted.';
        if(error.code===1)this.stop();this.onStatus({reason:message,accuracy:null,mph:null});
      },{enableHighAccuracy:true,maximumAge:0,timeout:12000});
    }catch(error){this.stop();throw error;}
    // Optional convenience, not background-location permission. Failure is harmless.
    if(navigator.wakeLock)try{const lock=await navigator.wakeLock.request('screen');if(this.active&&generation===this.generation)this.lock=lock;else await lock.release();}catch{}
  }
  stop(){
    this.generation++;this.active=false;
    if(this.watch!==null)navigator.geolocation.clearWatch(this.watch);
    this.watch=null;this.filter.reset();if(this.lock){this.lock.release().catch(()=>{});this.lock=null;}
  }
}
