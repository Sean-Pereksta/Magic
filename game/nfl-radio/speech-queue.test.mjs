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

test('canceling an active command settles the queue even without browser cancel events',async()=>{
 const spoken=[],ended=[];
 const synth={getVoices:()=>[],resume(){},cancel(){},speak:u=>spoken.push(u)};
 const queue=createBrowserSpeechQueue({synth,Utterance:FakeUtterance});
 queue.enqueue('command',{source:'voice-command',key:'command',onError:error=>ended.push(error)});
 queue.enqueue('live play',{source:'play-by-play',key:'play'});
 queue.cancel(item=>item.source==='voice-command');
 await sleep(5);
 assert.deepEqual(ended,['canceled']);assert.equal(spoken.at(-1).text,'live play');assert.equal(queue.state().current.source,'play-by-play');
 spoken.at(-1).onend();assert.equal(queue.state().speaking,false);
});
test('queued plays reevaluate filters and cannot duck audio after cancellation',async()=>{
 const spoken=[],hooks=[];let allowed=true;
 const synth={getVoices:()=>[],resume(){},cancel(){},speak:u=>spoken.push(u)};
 const queue=createBrowserSpeechQueue({synth,Utterance:FakeUtterance});
 queue.enqueue('first',{key:'first',onStart:()=>hooks.push('start'),onError:()=>hooks.push('restore')});
 queue.enqueue('filtered play',{key:'second',shouldPlay:()=>allowed,onSkip:()=>hooks.push('skip')});
 allowed=false;const canceled=spoken[0];queue.cancel(item=>item.key==='first');canceled.onstart?.();
 await sleep(5);assert.deepEqual(hooks,['restore','skip']);assert.equal(spoken.length,1);assert.equal(queue.state().speaking,false);
});
test('microphone suspension pauses queue draining and restores announcements afterwards',()=>{
 const spoken=[];const synth={getVoices:()=>[],resume(){},cancel(){},speak:u=>spoken.push(u)};
 const queue=createBrowserSpeechQueue({synth,Utterance:FakeUtterance});
 queue.setSuspended(true);queue.enqueue('after microphone',{key:'mic'});assert.equal(spoken.length,0);
 queue.setSuspended(false);assert.equal(spoken[0].text,'after microphone');spoken[0].onend();
});
test('voice choice is resolved when a queued item starts, including late-loaded voices',async()=>{
 const spoken=[];let voices=[],choice='preferred';
 const synth={getVoices:()=>voices,resume(){},speak:u=>spoken.push(u)};
 const queue=createBrowserSpeechQueue({synth,Utterance:FakeUtterance,getVoicePreferences:()=>({voiceURI:choice})});
 queue.enqueue('first',{key:'one'});queue.enqueue('second',{key:'two',rate:1.1,pitch:1.08});
 voices=[{voiceURI:'preferred',name:'Natural',lang:'en-US'}];spoken[0].onend();await sleep(5);
 assert.equal(spoken[1].voice.voiceURI,'preferred');assert.equal(spoken[1].rate,1.1);assert.equal(spoken[1].pitch,1.08);spoken[1].onend();
});

test('a stalled browser retries the head item without losing the FIFO backlog',async()=>{
 const spoken=[];const synth={getVoices:()=>[],resume(){},cancel(){},speak:u=>spoken.push(u)};
 const queue=createBrowserSpeechQueue({synth,Utterance:FakeUtterance,startTimeoutMs:12});
 queue.enqueue('first',{key:'1'});queue.enqueue('second',{key:'2'});
 await sleep(18);
 assert.equal(spoken.length,2);assert.equal(spoken[1].text,'first');
 spoken[0].onend();assert.equal(queue.state().current.key,'1','late old callback cannot finish retry');
 spoken[1].onstart();spoken[1].onend();await sleep(3);
 assert.equal(spoken[2].text,'second');spoken[2].onend();assert.equal(queue.state().queued,0);
});
test('blocked voice retains all calls until a user unlocks speech',async()=>{
 const spoken=[];const synth={getVoices:()=>[],resume(){},cancel(){},speak:u=>spoken.push(u)};
 const queue=createBrowserSpeechQueue({synth,Utterance:FakeUtterance});
 queue.enqueue('first',{key:'1'});queue.enqueue('second',{key:'2'});
 spoken[0].onerror({error:'not-allowed'});
 assert.equal(queue.state().blocked,true);assert.equal(queue.state().queued,2);
 queue.unlock();assert.equal(spoken[1].text,'first');spoken[1].onend();await sleep(3);
 assert.equal(spoken[2].text,'second');spoken[2].onend();assert.equal(queue.state().blocked,false);
});
test('missing end callback recovers and a queued correction is read rather than dropped',async()=>{
 const spoken=[];let text='old description';
 const synth={getVoices:()=>[],resume(){},cancel(){},speak:u=>spoken.push(u)};
 const queue=createBrowserSpeechQueue({synth,Utterance:FakeUtterance,minimumSpeechTimeoutMs:5});
 queue.enqueue('A',{key:'1'});spoken[0].onstart();queue.enqueue('old description',{key:'2',getText:()=>text});text='corrected description';
 await sleep(125);assert.equal(spoken[1].text,'A');spoken[1].onend();await sleep(3);
 assert.equal(spoken[2].text,'corrected description');spoken[2].onend();
});
test('microphone interruption preserves active play ahead of later calls',async()=>{
 const spoken=[];const synth={getVoices:()=>[],resume(){},cancel(){},speak:u=>spoken.push(u)};
 const queue=createBrowserSpeechQueue({synth,Utterance:FakeUtterance});
 queue.enqueue('first',{key:'1'});queue.enqueue('second',{key:'2'});queue.setSuspended(true);
 assert.equal(queue.state().queued,2);queue.setSuspended(false);
 assert.equal(spoken[1].text,'first');spoken[0].onend();assert.equal(queue.state().current.key,'1');
 spoken[1].onend();await sleep(3);assert.equal(spoken[2].text,'second');spoken[2].onend();
});
