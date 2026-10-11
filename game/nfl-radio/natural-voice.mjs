let worker=null,context=null,ready=false,loading=null,serial=0,pending=null,loadReject=null;
const listeners=new Set();
let message='Optional free natural voice · download required.';
function status(text){if(text===message)return;message=text;for(const fn of listeners)fn(text);}
export function onNaturalVoiceStatus(fn){listeners.add(fn);fn(message);return ()=>listeners.delete(fn);}
export function unlockNaturalVoice(){
 try{const Context=globalThis.AudioContext||globalThis.webkitAudioContext;if(!Context)return;context??=new Context();void context.resume().catch(()=>{});}catch{}
}
function reset(reason){
 ready=false;worker?.terminate();worker=null;loading=null;
 const reject=loadReject;loadReject=null;reject?.(new Error(reason));
 const job=pending;pending=null;job?.fallback();status(`${reason} Using device voice; you can retry the download.`);
}
export function loadNaturalVoice(){
 unlockNaturalVoice();if(ready)return Promise.resolve();if(loading)return loading;
 if(!context||typeof Worker==='undefined')return Promise.reject(new Error('Natural voice is unavailable in this browser.'));
 status('Downloading natural voice… first download may exceed 100 MB.');
 loading=new Promise((resolve,reject)=>{
  const timer=setTimeout(()=>reset('Voice download timed out.'),180000);
  loadReject=error=>{clearTimeout(timer);reject(error);};
  try{worker=new Worker(new URL('./natural-voice-worker.mjs',import.meta.url),{type:'module'});}catch{reset('Voice worker could not start.');return;}
  worker.onerror=()=>reset('Natural voice could not load.');
  worker.onmessage=({data})=>{
   if(data.type==='progress')status(`Downloading voice file: ${data.progress}% · device voice remains available`);
   if(data.type==='ready'){clearTimeout(timer);loadReject=null;ready=true;status('Natural voice ready · runs on this device.');resolve();}
   if(data.type==='error'){reset('Natural voice encountered an error.');}
   if(data.type==='audio'&&pending?.id===data.id)pending.play(data);
  };
  worker.postMessage({type:'load'});
 });
 // Allow retry after synchronous worker-construction failures as well.
 loading.catch(()=>{loading=null;});return loading;
}
export function tryNaturalSpeech(text,{voice='af_heart',rate=1,onStart,onEnd,onFallback}={}){
 if(!ready||!worker||context?.state!=='running')return null;
 const id=++serial;let source=null,done=false;
 const cleanup=()=>{clearTimeout(timer);if(pending?.id===id)pending=null;if(source){source.onended=null;try{source.stop();}catch{}source.disconnect();source=null;}};
 const fallback=()=>{if(done)return;done=true;cleanup();onFallback?.();};
 const timer=setTimeout(()=>reset('Natural voice was too slow.'),20000);
 pending={id,fallback,play(data){
  if(done)return;clearTimeout(timer);
  try{
   const buffer=context.createBuffer(1,data.samples.length,data.rate);buffer.copyToChannel(data.samples,0);
   source=context.createBufferSource();source.buffer=buffer;source.connect(context.destination);
   source.onended=()=>{if(done)return;done=true;cleanup();onEnd?.();};source.start();onStart?.();
  }catch{fallback();}
 }};
 try{worker.postMessage({type:'speak',id,text,voice,rate});}catch{fallback();}
 return ()=>{done=true;cleanup();};
}
