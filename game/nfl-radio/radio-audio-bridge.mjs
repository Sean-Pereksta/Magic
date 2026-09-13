const PATCH_KEY=Symbol.for('catnmice.nflRadioAudioPlayPatch');

let activeAudio=null;
let duckedAudio=null;
let restoreVolume=1;
let duckRequested=false;
let duckLevel=.18;

const validVolume=value=>Number.isFinite(value)?Math.max(0,Math.min(1,value)):1;

function restoreDuckedAudio(){
  if(!duckedAudio)return;
  try{duckedAudio.volume=validVolume(restoreVolume);}catch{}
  duckedAudio=null;
  restoreVolume=1;
}

function applyDuck(){
  if(!duckRequested){restoreDuckedAudio();return false;}
  const audio=activeAudio;
  if(!audio)return false;
  if(duckedAudio!==audio){
    restoreDuckedAudio();
    duckedAudio=audio;
    restoreVolume=validVolume(audio.volume);
  }
  try{
    audio.volume=Math.min(restoreVolume,duckLevel);
    return true;
  }catch{return false;}
}

export function registerRadioAudio(audio){
  if(!audio)return null;
  activeAudio=audio;
  if(duckRequested)applyDuck();
  return audio;
}

export function setRadioDucked(value,{level=.18}={}){
  duckRequested=!!value;
  duckLevel=validVolume(level);
  return applyDuck();
}

export function getRadioDuckState(){
  return {
    requested:duckRequested,
    active:!!duckedAudio,
    level:duckLevel,
    restoreVolume:duckedAudio?restoreVolume:null,
    hasAudio:!!activeAudio
  };
}

export function installRadioAudioCapture({mediaPrototype=globalThis.HTMLMediaElement?.prototype,AudioElement=globalThis.HTMLAudioElement}={}){
  if(!mediaPrototype||typeof mediaPrototype.play!=='function')return false;
  if(mediaPrototype[PATCH_KEY])return true;
  const originalPlay=mediaPrototype.play;
  Object.defineProperty(mediaPrototype,PATCH_KEY,{value:originalPlay,configurable:true});
  mediaPrototype.play=function(...args){
    const isAudio=typeof AudioElement==='function'?this instanceof AudioElement:true;
    if(isAudio)registerRadioAudio(this);
    return originalPlay.apply(this,args);
  };
  return true;
}

installRadioAudioCapture();
