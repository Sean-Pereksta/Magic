// Foreground, motion-estimated steps. No location, route or sensor history is saved.
export const STEP_METERS=.762; // An estimated 2.5-foot walking step.
const MPH_PER_MPS=2.236936292;
const vector=value=>value&&[value.x,value.y,value.z].every(Number.isFinite)?value:null;
const magnitude=value=>Math.hypot(value.x,value.y,value.z);

export class StepDetector {
  constructor(){this.reset();}
  reset(){this.lastSample=null;this.baseline=null;this.source=null;this.armed=false;this.lastStep=null;this.started=null;this.linearAxis=null;}
  sample(event,now=Date.now()){
    if(!Number.isFinite(now))return {valid:false,step:false};
    const gravity=vector(event?.accelerationIncludingGravity),linear=vector(event?.acceleration);
    // Magnitude is independent of the phone's orientation. Prefer the gravity
    // channel so a full positive/negative stride cycle produces one peak.
    const source=gravity&&magnitude(gravity)>3?'gravity':linear?'linear':null;
    if(!source)return {valid:false,step:false};
    const value=magnitude(source==='gravity'?gravity:linear);
    if(!Number.isFinite(value))return {valid:false,step:false};
    if(this.lastSample!==null&&now<=this.lastSample)return {valid:true,step:false};
    const gap=this.lastSample===null||now-this.lastSample>1000||source!==this.source;
    const dt=gap?0:now-this.lastSample;
    this.lastSample=now;this.source=source;
    if(gap){this.baseline=source==='gravity'?value:0;this.started=now;this.armed=false;this.lastStep=null;this.linearAxis=null;return {valid:true,step:false};}
    // Smooth the gravity baseline over roughly a second, retaining gait peaks.
    if(source==='gravity')this.baseline+=(value-this.baseline)*(1-Math.exp(-dt/1000));
    let signal=value-this.baseline;
    if(source==='linear'){
      // Preserve the sign of each gait cycle. Rectifying linear acceleration
      // with its magnitude would count both the upward and downward peaks.
      // Keep a slowly adapting gait axis, aligning opposite samples to the
      // same axis, so changing the phone's orientation does not flip its sign.
      if(value>=.45){
        const unit={x:linear.x/value,y:linear.y/value,z:linear.z/value};
        if(!this.linearAxis)this.linearAxis=unit;
        else{
          const alignment=unit.x*this.linearAxis.x+unit.y*this.linearAxis.y+unit.z*this.linearAxis.z;
          if(Math.abs(alignment)<.5){
            this.linearAxis=unit;this.started=now;this.armed=false;this.lastStep=null;
            return {valid:true,step:false};
          }
          const sign=alignment<0?-1:1,blend=1-Math.exp(-dt/500),axis={};
          for(const key of ['x','y','z'])axis[key]=this.linearAxis[key]+(unit[key]*sign-this.linearAxis[key])*blend;
          const length=magnitude(axis);this.linearAxis={x:axis.x/length,y:axis.y/length,z:axis.z/length};
        }
      }
      signal=this.linearAxis?linear.x*this.linearAxis.x+linear.y*this.linearAxis.y+linear.z*this.linearAxis.z:0;
    }
    const high=source==='gravity'?.9:1.25,low=source==='gravity'?.15:.45;
    if(signal<low)this.armed=true;
    if(now-this.started<300||!this.armed||signal<high)return {valid:true,step:false};
    this.armed=false;
    if(this.lastStep!==null&&now-this.lastStep<250)return {valid:true,step:false};
    const interval=this.lastStep===null?null:now-this.lastStep;
    this.lastStep=now;
    return {valid:true,step:true,mph:interval&&interval<=2500?STEP_METERS/(interval/1000)*MPH_PER_MPS:null};
  }
}

export class TrailPedometer {
  constructor(onFix,onStatus=()=>{}){
    this.onFix=onFix;this.onStatus=onStatus;this.detector=new StepDetector();
    this.active=false;this.lock=null;this.steps=0;this.direction={east:0,north:1};
    this.generation=0;this.motion=null;this.visibility=null;this.sensorTimer=null;
  }
  setHeading(east,north){
    if(!Number.isFinite(east)||!Number.isFinite(north))return;
    const length=Math.hypot(east,north);if(!Number.isFinite(length)||length<.001)return;
    this.direction={east:east/length,north:north/length};
  }
  status(reason,extra={}){this.onStatus({reason,source:'treadmill',mph:null,accuracy:null,steps:this.steps,...extra});}
  async start({wakeLock=true}={}){
    if(this.active)return;
    if(!globalThis.isSecureContext)throw Error('Open the HTTPS-hosted game to use treadmill motion tracking.');
    const Motion=globalThis.DeviceMotionEvent;
    if(!Motion||typeof globalThis.addEventListener!=='function')throw Error('Motion tracking is unavailable in this browser. Open the game directly in a phone browser with motion sensors.');
    if(document.hidden)throw Error('Bring the game to the foreground before starting treadmill mode.');
    this.active=true;this.steps=0;this.detector.reset();const generation=++this.generation;
    this.status('Starting treadmill motion tracking…');
    try{
      // iOS requires this call during the original Start walking user gesture.
      const permission=typeof Motion.requestPermission==='function'?Motion.requestPermission():null;
      if(permission!==null){
        const result=await permission;
        if(!this.active||generation!==this.generation)return;
        if(result!=='granted')throw Error('Motion access denied. Allow motion sensors in your browser/site settings, then tap Start walking again.');
      }
      if(!this.active||generation!==this.generation)return;
      if(document.hidden)throw Error('Bring the game to the foreground before starting treadmill mode.');
      let received=false;
      this.motion=event=>{
        if(!this.active||generation!==this.generation)return;
        if(document.hidden){this.stop();this.status('Treadmill paused while hidden. Tap Start walking to resume.');return;}
        const sample=this.detector.sample(event);
        if(!sample.valid)return;
        if(!received){received=true;clearTimeout(this.sensorTimer);this.sensorTimer=null;this.status('Treadmill ready · carry your phone as you walk.');}
        if(!sample.step)return;
        this.steps++;
        const fix={meters:STEP_METERS,east:this.direction.east*STEP_METERS,north:this.direction.north*STEP_METERS,mph:sample.mph,steps:this.steps,source:'treadmill',reason:'Treadmill · step accepted'};
        this.status(fix.reason,fix);this.onFix(fix);
      };
      this.visibility=()=>{if(document.hidden&&generation===this.generation){this.stop();this.status('Treadmill paused while hidden. Tap Start walking to resume.');}};
      globalThis.addEventListener('devicemotion',this.motion);
      document.addEventListener('visibilitychange',this.visibility);
      this.status('Treadmill ready · waiting for phone motion.');
      this.sensorTimer=setTimeout(()=>{
        if(!this.active||generation!==this.generation||received)return;
        this.stop();this.status('No motion data received. Allow motion sensors in site settings and keep the game open in a phone browser.');
      },6000);
      if(wakeLock&&navigator.wakeLock)try{
        const lock=await navigator.wakeLock.request('screen');
        if(this.active&&generation===this.generation)this.lock=lock;else await lock.release();
      }catch{}
    }catch(error){
      if(generation!==this.generation)return;
      this.stop();this.status(error.message);throw error;
    }
  }
  stop(){
    this.generation++;this.active=false;clearTimeout(this.sensorTimer);this.sensorTimer=null;
    if(this.motion)globalThis.removeEventListener('devicemotion',this.motion);
    if(this.visibility)document.removeEventListener('visibilitychange',this.visibility);
    this.motion=null;this.visibility=null;this.detector.reset();
    if(this.lock){Promise.resolve(this.lock.release()).catch(()=>{});this.lock=null;}
  }
}
