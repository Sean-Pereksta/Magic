import test from 'node:test';
import assert from 'node:assert/strict';
import {createBrowserSpeechQueue} from './speech-queue.mjs';

class FakeUtterance{
  constructor(text){this.text=text;}
}
function fakeSynth(){
  const events=[];
  const synth={
    speaking:false,
    pending:false,
    cancelCalls:0,
    getVoices(){return [];},
    resume(){events.push('resume');},
    cancel(){this.cancelCalls++;events.push('cancel');},
    speak(utterance){
      events.push(`queued:${utterance.text}`);
      setTimeout(()=>{
        this.speaking=true;
        events.push(`start:${utterance.text}`);
        utterance.onstart?.();
        setTimeout(()=>{
          this.speaking=false;
          events.push(`end:${utterance.text}`);
          utterance.onend?.();
        },5);
      },0);
    }
  };
  return {synth,events};
}
const sleep=ms=>new Promise(resolve=>setTimeout(resolve,ms));

test('speaks queued items one at a time without canceling current speech',async()=>{
  const {synth,events}=fakeSynth();
  const queue=createBrowserSpeechQueue({synth,Utterance:FakeUtterance});
  queue.enqueue('first',{key:'1',source:'play-by-play'});
  queue.enqueue('second',{key:'2',source:'game-update'});
  await sleep(30);
  assert.deepEqual(events.filter(x=>x.startsWith('start:')||x.startsWith('end:')),[
    'start:first','end:first','start:second','end:second'
  ]);
  assert.equal(synth.cancelCalls,0);
});

test('deduplicates keys and removing pending items never stops the active item',async()=>{
  const {synth,events}=fakeSynth();
  const queue=createBrowserSpeechQueue({synth,Utterance:FakeUtterance});
  assert.equal(queue.enqueue('first',{key:'same',source:'play-by-play'}),true);
  assert.equal(queue.enqueue('duplicate',{key:'same',source:'play-by-play'}),false);
  assert.equal(queue.enqueue('later',{key:'later',source:'game-update'}),true);
  assert.equal(queue.removePending(item=>item.source==='game-update'),1);
  await sleep(20);
  assert.ok(events.includes('start:first'));
  assert.ok(events.includes('end:first'));
  assert.ok(!events.includes('start:later'));
  assert.equal(synth.cancelCalls,0);
});
