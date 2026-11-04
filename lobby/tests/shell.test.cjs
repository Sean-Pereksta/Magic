const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const html = fs.readFileSync(path.join(__dirname, '../../index.html'), 'utf8');

function source(name) {
  const start = html.search(new RegExp('    (?:async )?function ' + name + '\\('));
  assert.ok(start >= 0, `missing ${name}`);
  return html.slice(start, html.indexOf('\n    }', start) + 6);
}
function node() {
  const classes = new Set(), attrs = {};
  return {attrs, classList: {contains: n => classes.has(n), toggle(n,on) {on ? classes.add(n) : classes.delete(n);}},
    setAttribute(n,v) {attrs[n]=v;}, getAttribute(n) {return attrs[n];}, focus() {this.focused=true;}, contains(value) {return value===this.child;}};
}
function chatHarness(username='') {
  const chatContainer=node(), toggleChat=node(), closeChat=node(), msgInput=node(), badge=node();
  const context=vm.createContext({chatContainer,toggleChat,closeChat,msgInput,chatUnreadBadge:badge,
    unreadMessages:3,currentUsername:username,chatFocusReturn:null,frame:{},document:{activeElement:null},syncLobbyUsername(){},syncChatRelay(){},setTimeout(fn){fn();}});
  vm.runInContext(source('setUnreadCount')+'\n'+source('toggleChatPanel'),context);
  return context;
}

test('chat remains keyboard accessible for a guest and returns focus on close',()=>{
  const h=chatHarness();
  h.toggleChatPanel(true);
  assert.equal(h.chatContainer.inert,false);
  assert.equal(h.chatContainer.attrs['aria-hidden'],'false');
  assert.equal(h.toggleChat.attrs['aria-expanded'],'true');
  assert.equal(h.closeChat.focused,true);
  assert.equal(h.chatUnreadBadge.hidden,true);
  assert.equal(h.toggleChat.attrs['aria-label'],'Close Game Hub chat');
  h.chatContainer.child=h.closeChat;h.document.activeElement=h.closeChat;
  h.toggleChatPanel(false);
  assert.equal(h.chatContainer.inert,true);
  assert.equal(h.toggleChat.attrs['aria-expanded'],'false');
  assert.equal(h.toggleChat.focused,true);
});
test('signed in chat focuses composition and a close does not steal unrelated focus',()=>{
  const h=chatHarness('Player');
  h.toggleChatPanel(true);assert.equal(h.msgInput.focused,true);
  h.document.activeElement={};h.toggleChatPanel(false);
  assert.equal(h.toggleChat.focused,undefined);
});
test('chat unread announcement reports the action and total',()=>{
  const h=chatHarness();h.setUnreadCount(123);
  assert.equal(h.chatUnreadBadge.textContent,'99+');
  assert.match(h.toggleChat.attrs['aria-label'],/Open Game Hub chat, 123 unread messages/);
});
test('chat opened from the hub restores focus to the hub relay',()=>{
  const h=chatHarness('Player'),relay={isConnected:true,focus(){this.focused=true;}};
  h.frame.contentDocument={activeElement:relay};h.document.activeElement=h.frame;
  h.toggleChatPanel(true);
  h.chatContainer.child=h.msgInput;h.document.activeElement=h.msgInput;
  h.toggleChatPanel(false);
  assert.equal(relay.focused,true);
  assert.equal(h.toggleChat.focused,undefined);
});
test('hub integration hides the duplicate toolbar only after a working relay exists',()=>{
  let relay=null,created=0;const badge={},actions={prepend(value){relay=value;}};
  const hubDocument={title:'Cat’N Mice',querySelector:()=>actions,getElementById:()=>relay,addEventListener(){},
    createElement(){created++;return {...node(),querySelector:()=>badge,addEventListener(name,callback){this[name]=callback;}};}};
  const frame={contentWindow:{location:{pathname:'/lobby/lobby.html'}},contentDocument:hubDocument};
  const status={},toggleChat=node(),chatContainer=node();toggleChat.setAttribute('aria-label','Open Game Hub chat, 3 unread messages');
  let opened=0;
  const h=vm.createContext({frame,document:{body:{dataset:{}},getElementById:()=>status},toggleChat,chatContainer,unreadMessages:3,toggleChatPanel(){opened++;}});
  vm.runInContext(source('syncChatRelay')+'\n'+source('syncFrameShell'),h);
  h.syncFrameShell();
  assert.equal(h.document.body.dataset.shellMode,'hub');
  assert.equal(relay.attrs['aria-label'],'Open Game Hub chat, 3 unread messages');
  assert.equal(badge.textContent,'3');relay.click();assert.equal(opened,1);
  h.syncFrameShell();assert.equal(created,1,'repeated frame loads must not duplicate Chat');
  frame.contentWindow.location.pathname='/game/apes-together-strong.html';hubDocument.title='Apes Together Strong';
  h.syncFrameShell();assert.equal(h.document.body.dataset.shellMode,'game');assert.equal(frame.title,'Apes Together Strong');
  frame.contentWindow.location.pathname='/lobby/lobby.html';hubDocument.querySelector=()=>null;
  h.syncFrameShell();assert.equal(h.document.body.dataset.shellMode,'game','missing relay target retains the fallback toolbar');
  Object.defineProperty(frame.contentWindow,'location',{get(){throw new Error('cross-origin');}});
  h.syncFrameShell();assert.equal(h.document.body.dataset.shellMode,'game');
});
test('unavailable chat services reject writes while leaving the shell operable',async()=>{
  const h=vm.createContext({queueMicrotask});vm.runInContext(source('offlineChatSdk'),h);
  const offline=h.offlineChatSdk(new Error('Network failed'));
  assert.equal(offline.getAuth().currentUser,null);
  await assert.rejects(offline.addDoc(),/temporarily unavailable/);
  await assert.rejects(offline.signInAnonymously(),/temporarily unavailable/);
  let failure;offline.onSnapshot({},()=>assert.fail('must not emit fabricated messages'),error=>failure=error);
  await new Promise(resolve=>queueMicrotask(resolve));assert.equal(failure.message,'Network failed');
});
test('fullscreen requests still target only the actual content frame',async()=>{
  const calls=[],frame={contentWindow:{}};
  const h=vm.createContext({frame,currentFullscreenElement:()=>null,
    enterFrameFullscreen:async()=>calls.push('enter'),exitFrameFullscreen:async()=>calls.push('exit')});
  vm.runInContext(source('setFrameFullscreen')+'\n'+source('requestGameFullscreenFromMessage'),h);
  await h.requestGameFullscreenFromMessage({source:{},data:{type:'game-fullscreen-request'}});
  assert.deepEqual(calls,[]);
  await h.requestGameFullscreenFromMessage({source:frame.contentWindow,data:{type:'game-fullscreen-request',action:'enter'}});
  assert.deepEqual(calls,['enter']);
  h.currentFullscreenElement=()=>frame;
  await h.requestGameFullscreenFromMessage({source:frame.contentWindow,data:{type:'game-fullscreen',action:'toggle'}});
  assert.deepEqual(calls,['enter','exit']);
});
test('the shared shell keeps one chat control and the existing game permissions',()=>{
  assert.equal((html.match(/id="toggleChat"/g)||[]).length,1);
  assert.match(html,/<button id="toggleChat"[^>]+aria-controls="chatContainer"/);
  assert.match(html,/src="\/lobby\/lobby.html"/);
  assert.match(html,/allow="fullscreen \*; autoplay[^\n]+microphone[^\n]+geolocation 'self'/);
  assert.match(html,/allowfullscreen="true"/);
  assert.match(html,/event\.key==="Escape"&&chatContainer\.classList\.contains\("open"\)/);
});
