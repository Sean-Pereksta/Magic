const KEY='nfl-dial:voice';
export function readVoiceSettings(storage=globalThis.localStorage){
  try{const saved=JSON.parse(storage?.getItem(KEY)||'null')||{};return {voiceURI:typeof saved.voiceURI==='string'?saved.voiceURI:'',style:saved.style==='excited'?'excited':'normal'};}
  catch{return {voiceURI:'',style:'normal'};}
}
export function saveVoiceSettings(settings,storage=globalThis.localStorage){try{storage?.setItem(KEY,JSON.stringify(settings));}catch{}}
export function englishVoices(voices=[]){return voices.filter(v=>/^en(?:[-_]|$)/i.test(v.lang)).sort((a,b)=>voiceScore(b)-voiceScore(a)||a.name.localeCompare(b.name));}
function voiceScore(v){return (/natural|neural|premium|enhanced/i.test(v.name)?100:0)+(/google|microsoft/i.test(v.name)?15:0)+(/^en[-_]US$/i.test(v.lang)?10:0)+(v.default?5:0);}
export function chooseVoice(voices=[],preferredURI=''){
  return voices.find(v=>preferredURI&&v.voiceURI===preferredURI)||englishVoices(voices)[0]||voices.find(v=>v.default)||null;
}
export function playVoiceOptions({important=false}={},style='normal'){
  return style==='excited'?{rate:important?1.13:1.1,pitch:important?1.08:1.02}:{rate:important?1.06:1.08,pitch:important?1.03:1};
}
