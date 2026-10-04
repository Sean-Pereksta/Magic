import test from 'node:test';
import assert from 'node:assert/strict';
import {chooseVoice,englishVoices,playVoiceOptions,readVoiceSettings,saveVoiceSettings} from './voice-settings.mjs';
const voices=[{name:'English basic',lang:'en-US',default:true,voiceURI:'basic'},{name:'French Natural',lang:'fr-FR',voiceURI:'fr'},{name:'English Natural',lang:'en-US',voiceURI:'natural'}];
test('automatic voice favors natural English and honors a saved manual selection',()=>{
 assert.equal(chooseVoice(voices).voiceURI,'natural');assert.equal(chooseVoice(voices,'basic').voiceURI,'basic');
 assert.equal(chooseVoice(voices,'missing').voiceURI,'natural');assert.equal(englishVoices(voices).length,2);assert.equal(chooseVoice([]),null);
});
test('voice preference can survive delayed voice discovery and a reload',()=>{
 let value=null;const storage={getItem:()=>value,setItem:(_key,newValue)=>{value=newValue;}};
 saveVoiceSettings({voiceURI:'natural',style:'excited'},storage);assert.deepEqual(readVoiceSettings(storage),{voiceURI:'natural',style:'excited'});
 assert.equal(chooseVoice([],readVoiceSettings(storage).voiceURI),null);assert.equal(chooseVoice(voices,readVoiceSettings(storage).voiceURI).voiceURI,'natural');
});
test('excited calls increase emphasis within restrained speech settings',()=>{
 const normal=playVoiceOptions({important:true},'normal'),excited=playVoiceOptions({important:true},'excited');
 assert.ok(excited.pitch>normal.pitch);assert.ok(excited.rate>normal.rate);assert.ok(excited.rate<=1.15);assert.ok(excited.pitch<=1.1);
});
