import {tryNaturalSpeech,unlockNaturalVoice} from './natural-voice.mjs';
import {chooseVoice,englishVoices,readVoiceSettings} from './voice-settings.mjs';
const DEFAULT_RATE=1.06;
const DEVICE_FALLBACK_ERRORS=new Set([
  'voice-unavailable','language-unavailable','synthesis-unavailable','synthesis-failed',
  'network','audio-busy','audio-hardware','speech-start-timeout'
]);

const available=(synth,Utterance)=>!!synth&&typeof Utterance==='function';

export function createBrowserSpeechQueue({
  synth=globalThis.speechSynthesis,
  Utterance=globalThis.SpeechSynthesisUtterance,
  getVoicePreferences=readVoiceSettings,
  renderNatural=tryNaturalSpeech,
  startTimeoutMs=8000,
  minimumSpeechTimeoutMs=45000
}={}){
  const queue=[];
  const keys=new Set();
  const listeners=new Set();
  const failedVoices=new Set();
  let current=null;
  let suspended=false;
  let blocked=false;
  let activated=false;
  let primed=false;
  let watchdog=null;
  const clearWatchdog=()=>{clearTimeout(watchdog);watchdog=null;};
  const arm=(fn,ms)=>{clearWatchdog();watchdog=setTimeout(fn,ms);watchdog.unref?.();};

  const state=()=>({
    available:available(synth,Utterance),
    blocked,
    activated,
    suspended,
    starting:!!current&&!current.started,
    speaking:!!current?.started,
    queued:queue.length,
    current:current?{key:current.key,source:current.source,text:current.text}:null
  });

  const notify=()=>{
    const snapshot=state();
    for(const listener of listeners){
      try{listener(snapshot);}catch{}
    }
  };

  const resume=()=>{
    if(!available(synth,Utterance))return false;
    try{synth.resume?.();}catch{}
    return true;
  };

  const prepareGesture=()=>{
    if(getVoicePreferences().engine==='natural')unlockNaturalVoice();
    if(!resume())return false;
    if(blocked){blocked=false;for(const item of queue)item.retries=0;}
    return true;
  };

  const unlock=()=>{
    if(!prepareGesture())return false;
    drain();
    notify();
    if(activated||primed)return true;
    try{
      if(synth.speaking||synth.pending||current||queue.length)return true;
      const warmup=new Utterance(' ');
      warmup.lang='en-US';
      warmup.rate=10;
      warmup.volume=0;
      const voice=chooseVoice(synth?.getVoices?.()||[],getVoicePreferences().voiceURI);
      if(voice)warmup.voice=voice;
      synth.speak(warmup);
      primed=true;
      return true;
    }catch{return false;}
  };

  const finish=(item,ok,error)=>{
    if(current!==item)return;
    clearWatchdog();
    item.utterance=null;
    current=null;
    item.cancelNatural?.();item.cancelNatural=null;
    if(item.key)keys.delete(item.key);
    try{
      if(ok)item.onEnd?.();
      else item.onError?.(error||'speech-error');
    }catch{}
    notify();
    setTimeout(drain,0);
  };

  function recover(item,error){
    if(current!==item)return;
    clearWatchdog();
    current=null; // Ignore cancellation callbacks from the failed attempt.
    item.cancelNatural?.();item.cancelNatural=null;
    try{synth.cancel?.();}catch{}
    if(DEVICE_FALLBACK_ERRORS.has(error)){
      const failedVoice=item.utterance?.voice?.voiceURI;
      if(failedVoice)failedVoices.add(failedVoice);
      item.useDeviceDefault=true;
    }
    item.utterance=null;
    item.retries=(item.retries||0)+1;
    queue.unshift(item); // Preserve FIFO order and the de-duplication key.
    blocked=error==='not-allowed'||item.retries>1;
    try{item.onError?.(blocked?'speech-blocked':error);}catch{}
    notify();
    if(!blocked)setTimeout(drain,0);
  }

  function drain(){
    if(suspended||blocked||current||!queue.length)return;
    if(!resume()){
      while(queue.length){
        const item=queue.shift();
        if(item.key)keys.delete(item.key);
        try{item.onError?.('speech-unavailable');}catch{}
      }
      notify();
      return;
    }

    let item=null;
    while(queue.length&&!item){
      const candidate=queue.shift();
      if(candidate.shouldPlay&&!candidate.shouldPlay()){
        if(candidate.key)keys.delete(candidate.key);
        try{candidate.onSkip?.();}catch{}
        continue;
      }
      item=candidate;
    }
    if(!item){notify();return;}

    current=item;
    item.started=false;
    notify();

    const preferences=getVoicePreferences();
    if(preferences.engine==='natural'&&!item.forceDevice&&!item.useDeviceDefault){
      let usingNatural=true;
      const fallback=()=>{
        if(current!==item||!usingNatural)return;
        usingNatural=false;
        item.cancelNatural=null;
        speakBrowser(item);
      };
      const cancel=renderNatural(item.getText?.()||item.text,{
        voice:preferences.naturalVoice,rate:item.rate||1,
        onStart:()=>{if(current===item&&usingNatural)markStarted(item,item.text.length*110);},
        onEnd:()=>{if(usingNatural)finish(item,true);},
        onFallback:fallback
      });
      if(!usingNatural||current!==item)return;
      if(cancel){
        item.cancelNatural=cancel;
        if(!item.started)arm(()=>{
          if(current!==item||item.started)return;
          cancel();
          fallback();
        },startTimeoutMs);
        return;
      }
    }
    speakBrowser(item);
  }

  function markStarted(item,estimate){
    activated=true;item.started=true;
    arm(()=>recover(item,'speech-timeout'),Math.max(minimumSpeechTimeoutMs,estimate));
    try{item.onStart?.();}catch{}
    notify();
  }

  function speakBrowser(item){
    try{
      const utterance=new Utterance(item.getText?.()||item.text);
      item.utterance=utterance; // Keep the utterance alive until completion (mobile engines).
      utterance.lang=item.lang||'en-US';
      utterance.rate=Number.isFinite(item.rate)?item.rate:DEFAULT_RATE;
      utterance.pitch=Number.isFinite(item.pitch)?item.pitch:1;
      utterance.volume=Number.isFinite(item.volume)?item.volume:1;
      const voices=(synth.getVoices?.()||[]).filter(voice=>!failedVoices.has(voice.voiceURI));
      const voice=item.useDeviceDefault||item.preferLocal
        ?englishVoices(voices).find(voice=>voice.localService===true)
        :chooseVoice(voices,getVoicePreferences().voiceURI);
      if(voice){utterance.voice=voice;utterance.lang=item.lang||voice.lang;}

      let done=false;
      utterance.onstart=()=>{
        if(done||current!==item||item.utterance!==utterance)return;
        markStarted(item,utterance.text.length*110/(utterance.rate||1));
      };
      utterance.onend=()=>{
        if(done||current!==item||item.utterance!==utterance)return;
        done=true;
        if(item.immediate&&!item.started)recover(item,'speech-start-timeout');
        else finish(item,true);
      };
      utterance.onerror=event=>{
        if(done||current!==item||item.utterance!==utterance)return;
        done=true;
        recover(item,event?.error||'speech-error');
      };
      arm(()=>{if(current===item)recover(item,'speech-start-timeout');},item.immediate?Math.min(startTimeoutMs,3500):startTimeoutMs);
      synth.speak(utterance);
    }catch(error){
      recover(item,error?.message||'speech-error');
    }
  }

  const makeItem=(text,options)=>{
    const normalized=String(text??'').replace(/\s+/g,' ').trim();
    if(!normalized||!available(synth,Utterance))return null;
    return {
      text:normalized,
      getText:options.getText,
      key:options.key?String(options.key):'',
      source:options.source||'app',
      rate:options.rate,
      pitch:options.pitch,
      volume:options.volume,
      lang:options.lang,
      forceDevice:!!options.forceDevice,
      preferLocal:!!options.preferLocal,
      shouldPlay:options.shouldPlay,
      onStart:options.onStart,
      onEnd:options.onEnd,
      onError:options.onError,
      onSkip:options.onSkip
    };
  };
  const enqueue=(text,options={})=>{
    const item=makeItem(text,options);
    if(!item||(item.key&&keys.has(item.key)))return false;
    if(item.key)keys.add(item.key);
    queue.push(item);
    notify();
    drain();
    return true;
  };

  const removePending=predicate=>{
    let removed=0;
    for(let i=queue.length-1;i>=0;i--){
      const item=queue[i];
      if(!predicate||predicate(item)){
        queue.splice(i,1);
        if(item.key)keys.delete(item.key);
        removed++;
      }
    }
    if(!queue.length&&!current)blocked=false;
    if(removed)notify();
    return removed;
  };

  function playNow(text,options={}){
    const item=makeItem(text,options);
    if(!item||!prepareGesture())return false;
    removePending(pending=>pending.immediate||(item.key&&pending.key===item.key));
    if(current){
      const interrupted=current;
      current=null;
      clearWatchdog();
      interrupted.cancelNatural?.();interrupted.cancelNatural=null;
      interrupted.utterance=null;
      try{synth.cancel?.();}catch{}
      if(interrupted.immediate||interrupted.source==='voice-command'||(item.key&&interrupted.key===item.key)){
        if(interrupted.key)keys.delete(interrupted.key);
      }else queue.unshift(interrupted);
      try{interrupted.onError?.('canceled');}catch{}
    }else if(synth.pending||synth.speaking){
      // Clear a legacy silent warm-up before playing the requested clip.
      try{synth.cancel?.();}catch{}
    }
    item.immediate=true;
    if(item.key)keys.add(item.key);
    queue.unshift(item);
    drain();
    notify();
    return true;
  }

  // Browser cancel() does not consistently fire onend/onerror on mobile.
  // Settle our queue explicitly so radio ducking and the next item always recover.
  const cancel=predicate=>{
    const removed=removePending(predicate);
    if(current&&(!predicate||predicate(current))){
      const item=current;
      finish(item,false,'canceled');
      try{synth.cancel?.();}catch{}
      return removed+1;
    }
    return removed;
  };
  const setSuspended=value=>{
    suspended=!!value;
    if(suspended&&current){const item=current;current=null;clearWatchdog();item.cancelNatural?.();item.cancelNatural=null;queue.unshift(item);try{synth.cancel?.();}catch{}try{item.onError?.('microphone-paused');}catch{}notify();}
    if(!suspended)drain();
    notify();
  };

  const subscribe=listener=>{
    listeners.add(listener);
    try{listener(state());}catch{}
    return ()=>listeners.delete(listener);
  };

  return {enqueue,playNow,unlock,resume,removePending,cancel,setSuspended,subscribe,state,available:()=>available(synth,Utterance)};
}

export const browserSpeechQueue=createBrowserSpeechQueue();
export const browserSpeechAvailable=()=>browserSpeechQueue.available();
export const unlockBrowserSpeech=()=>browserSpeechQueue.unlock();
export const resumeBrowserSpeech=()=>browserSpeechQueue.resume();
export const enqueueBrowserSpeech=(text,options)=>browserSpeechQueue.enqueue(text,options);
export const playBrowserSpeechNow=(text,options)=>browserSpeechQueue.playNow(text,options);
export const removeQueuedSpeech=predicate=>browserSpeechQueue.removePending(predicate);
export const getBrowserSpeechQueueState=()=>browserSpeechQueue.state();
export const onBrowserSpeechQueueState=listener=>browserSpeechQueue.subscribe(listener);

export const cancelBrowserSpeech=predicate=>browserSpeechQueue.cancel(predicate);
export const suspendBrowserSpeech=value=>browserSpeechQueue.setSuspended(value);
