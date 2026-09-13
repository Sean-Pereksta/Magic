const DEFAULT_RATE=1.06;

const available=(synth,Utterance)=>!!synth&&typeof Utterance==='function';

function chooseVoice(synth){
  const voices=synth?.getVoices?.()||[];
  return voices.find(v=>v.default&&/^en/i.test(v.lang))
    ||voices.find(v=>/^en/i.test(v.lang))
    ||voices.find(v=>v.default)
    ||null;
}

export function createBrowserSpeechQueue({
  synth=globalThis.speechSynthesis,
  Utterance=globalThis.SpeechSynthesisUtterance
}={}){
  const queue=[];
  const keys=new Set();
  const listeners=new Set();
  let current=null;

  const state=()=>({
    available:available(synth,Utterance),
    speaking:!!current,
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

  const unlock=()=>{
    if(!resume())return false;
    try{
      if(synth.speaking||synth.pending||current||queue.length)return true;
      const warmup=new Utterance(' ');
      warmup.lang='en-US';
      warmup.rate=10;
      warmup.volume=0;
      const voice=chooseVoice(synth);
      if(voice)warmup.voice=voice;
      synth.speak(warmup);
      return true;
    }catch{return false;}
  };

  const finish=(item,ok,error)=>{
    if(current!==item)return;
    current=null;
    if(item.key)keys.delete(item.key);
    try{
      if(ok)item.onEnd?.();
      else item.onError?.(error||'speech-error');
    }catch{}
    notify();
    setTimeout(drain,0);
  };

  function drain(){
    if(current||!queue.length)return;
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
    notify();

    try{
      const utterance=new Utterance(item.text);
      utterance.lang=item.lang||'en-US';
      utterance.rate=Number.isFinite(item.rate)?item.rate:DEFAULT_RATE;
      utterance.pitch=Number.isFinite(item.pitch)?item.pitch:1;
      utterance.volume=Number.isFinite(item.volume)?item.volume:1;
      const voice=chooseVoice(synth);
      if(voice)utterance.voice=voice;

      let done=false;
      utterance.onstart=()=>{
        if(done)return;
        try{item.onStart?.();}catch{}
        notify();
      };
      utterance.onend=()=>{
        if(done)return;
        done=true;
        finish(item,true);
      };
      utterance.onerror=event=>{
        if(done)return;
        done=true;
        finish(item,false,event?.error||'speech-error');
      };
      synth.speak(utterance);
    }catch(error){
      finish(item,false,error?.message||'speech-error');
    }
  }

  const enqueue=(text,options={})=>{
    const normalized=String(text??'').replace(/\s+/g,' ').trim();
    if(!normalized||!available(synth,Utterance))return false;
    const key=options.key?String(options.key):'';
    if(key&&keys.has(key))return false;
    const item={
      text:normalized,
      key,
      source:options.source||'app',
      rate:options.rate,
      pitch:options.pitch,
      volume:options.volume,
      lang:options.lang,
      shouldPlay:options.shouldPlay,
      onStart:options.onStart,
      onEnd:options.onEnd,
      onError:options.onError,
      onSkip:options.onSkip
    };
    if(key)keys.add(key);
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
    if(removed)notify();
    return removed;
  };

  const subscribe=listener=>{
    listeners.add(listener);
    try{listener(state());}catch{}
    return ()=>listeners.delete(listener);
  };

  return {enqueue,unlock,resume,removePending,subscribe,state,available:()=>available(synth,Utterance)};
}

export const browserSpeechQueue=createBrowserSpeechQueue();
export const browserSpeechAvailable=()=>browserSpeechQueue.available();
export const unlockBrowserSpeech=()=>browserSpeechQueue.unlock();
export const resumeBrowserSpeech=()=>browserSpeechQueue.resume();
export const enqueueBrowserSpeech=(text,options)=>browserSpeechQueue.enqueue(text,options);
export const removeQueuedSpeech=predicate=>browserSpeechQueue.removePending(predicate);
export const getBrowserSpeechQueueState=()=>browserSpeechQueue.state();
export const onBrowserSpeechQueueState=listener=>browserSpeechQueue.subscribe(listener);
