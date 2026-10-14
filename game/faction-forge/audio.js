/** Small, asset-independent battle sounds. Audio starts only after a user gesture.
 * Voices, event rates and gain are bounded independently of simulation speed. */
export const AUDIO_LIMITS = Object.freeze({voices:10,windowMs:100,eventsPerWindow:5,kindCooldownMs:65});
const clamp=(n,a,b)=>Math.max(a,Math.min(b,n));
const SOUND = Object.freeze({
  missile:{wave:'sawtooth',from:180,to:55,duration:.22,gain:.10,noise:true},
  explosion:{from:95,to:28,duration:.38,gain:.22,noise:true},
  lightning:{from:1600,to:120,duration:.22,gain:.13,noise:true},
  melee:{wave:'triangle',from:280,to:65,duration:.10,gain:.12},
  bullet:{from:900,to:170,duration:.055,gain:.08,noise:true},
  magic:{wave:'sine',from:380,to:820,duration:.23,gain:.08},
  heal:{wave:'sine',from:520,to:1040,duration:.30,gain:.06},
  swarm:{from:240,to:90,duration:.19,gain:.07,noise:true},
  toxic:{from:330,to:70,duration:.25,gain:.07,noise:true},
  shield:{wave:'sine',from:190,to:430,duration:.17,gain:.07}
});

export function createBattleAudio({contextFactory=()=>{const Audio=globalThis.AudioContext||globalThis.webkitAudioContext;return Audio?new Audio():null;},now=()=>globalThis.performance?.now?.()??Date.now(),enabled=true,volume=.35}={}){
  let context=null,master=null,compressor=null,noise=null,disposed=false,windowAt=-Infinity,windowCount=0;
  const voices=new Set(),lastByKind=new Map();
  let settings={audio:!!enabled,volume:clamp(Number(volume)||0,0,1)};
  function configure(next={}){
    if('audio' in next)settings.audio=!!next.audio;
    if(Number.isFinite(next.volume))settings.volume=clamp(next.volume,0,1);
    if(master&&context)master.gain.setTargetAtTime(settings.audio?settings.volume:0,context.currentTime,.035);
    if(!settings.audio)clear();
    return {...settings};
  }
  async function unlock(){
    if(disposed||!settings.audio)return false;
    try{
      if(!context){
        context=contextFactory();if(!context)return false;
        master=context.createGain();master.gain.value=settings.volume;
        compressor=context.createDynamicsCompressor?.();
        if(compressor){compressor.threshold.value=-18;compressor.knee.value=12;compressor.ratio.value=5;master.connect(compressor);compressor.connect(context.destination);}
        else master.connect(context.destination);
        // Reuse one deterministic noise buffer for all mechanical sounds.
        noise=context.createBuffer(1,Math.ceil(context.sampleRate*.5),context.sampleRate);
        const data=noise.getChannelData(0);let seed=173;
        for(let i=0;i<data.length;i++){seed=(Math.imul(seed,1664525)+1013904223)|0;data[i]=((seed>>>0)/4294967296*2-1);}
      }
      if(context.state==='suspended')await context.resume();
      return context.state==='running';
    }catch{return false;}
  }
  function play(kind,{delay=0,power=1}={}){
    if(disposed||!settings.audio||!settings.volume||context?.state!=='running'||!SOUND[kind])return false;
    const time=now();
    if(time-windowAt>AUDIO_LIMITS.windowMs){windowAt=time;windowCount=0;}
    if(windowCount>=AUDIO_LIMITS.eventsPerWindow||voices.size>=AUDIO_LIMITS.voices||time-(lastByKind.get(kind)??-Infinity)<AUDIO_LIMITS.kindCooldownMs)return false;
    let source=null,filter=null,envelope=null,voice=null;
    try{
      const spec=SOUND[kind],at=context.currentTime+clamp(delay,0,.6),duration=spec.duration;
      source=spec.noise?context.createBufferSource():context.createOscillator();
      if(spec.noise){source.buffer=noise;filter=context.createBiquadFilter();filter.type=kind==='explosion'?'lowpass':'bandpass';filter.Q.value=.7;filter.frequency.setValueAtTime(spec.from,at);filter.frequency.exponentialRampToValueAtTime(spec.to,at+duration);source.connect(filter);}
      else{source.type=spec.wave||'triangle';source.frequency.setValueAtTime(spec.from,at);source.frequency.exponentialRampToValueAtTime(spec.to,at+duration);}
      envelope=context.createGain();envelope.gain.setValueAtTime(.0001,at);envelope.gain.linearRampToValueAtTime(spec.gain*clamp(power,.3,1.4),at+.012);envelope.gain.exponentialRampToValueAtTime(.0001,at+duration);
      (filter||source).connect(envelope);envelope.connect(master);
      voice={source,filter,envelope};voices.add(voice);
      source.onended=()=>{voices.delete(voice);source.disconnect();filter?.disconnect();envelope.disconnect();};
      source.start(at);source.stop(at+duration+.02);windowCount++;lastByKind.set(kind,time);return true;
    }catch{if(voice)voices.delete(voice);try{source?.disconnect();filter?.disconnect();envelope?.disconnect();}catch{}return false;}
  }
  function clear(){
    for(const voice of voices){try{voice.source.onended=null;voice.source.stop();voice.source.disconnect();voice.filter?.disconnect();voice.envelope.disconnect();}catch{}}
    voices.clear();lastByKind.clear();windowCount=0;windowAt=-Infinity;
  }
  function destroy(){clear();disposed=true;try{context?.close()?.catch?.(()=>{});master?.disconnect();compressor?.disconnect();}catch{}context=null;noise=null;}
  return {unlock,play,configure,clear,destroy,get settings(){return {...settings};},get activeVoices(){return voices.size;},get available(){return !!context&&context.state==='running';}};
}
